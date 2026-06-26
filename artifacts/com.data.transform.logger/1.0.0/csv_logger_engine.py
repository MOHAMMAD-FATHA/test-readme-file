#!/usr/bin/env python3
"""
Greengrass Component: CSV Logger
----------------------------------
Subscribes to the same local MQTT broker as live_state_engine.py and logs
every raw message to a daily-rotating CSV file.

Runs as a completely independent Greengrass component — if live_state_engine
restarts or crashes, this logger keeps running unaffected.

Environment Variables:
    SUB_TOPIC       : MQTT topic to subscribe to (same as live_state_engine)
    CSV_LOG_DIR     : Directory to write daily CSV files
                      (default: /greengrass/v2/oee_engine/logs)
    LOG_QUEUE_SIZE  : Max in-memory queue depth before drops (default: 10000)
    RETAIN_DAYS     : Days of files to keep before auto-delete (default: 7)

Output files:
    {CSV_LOG_DIR}/raw_mqtt_YYYY-MM-DD.csv        <- today (open, uncompressed)
    {CSV_LOG_DIR}/raw_mqtt_YYYY-MM-DD.csv.gz     <- past days (compressed ~90% smaller)

Columns:
    Timestamp_CST, StationID, TagName, Value
    (RawPayload removed — redundant, was 3x file size)

Storage estimate at your message rate (~113 MB/day uncompressed):
    Today (uncompressed) : ~35  MB
    Past days (gzipped)  : ~5   MB/day
    7-day total          : ~75  MB   (vs ~790 MB before)

Threads:
    csv_logger_worker  — all disk writes, daily file rotation
    cleanup_worker     — compresses yesterday's file + deletes files > RETAIN_DAYS
                         runs on startup then every 24 hours automatically
"""

import csv
import gzip
import json
import os
import random
import shutil
import signal
import time
from datetime import datetime, timedelta
from queue import Empty, Full, Queue
from threading import Event, Thread
from zoneinfo import ZoneInfo

import paho.mqtt.client as mqtt_client

# ---------------------------------------------------------------------------
# Configuration (all tunable via Greengrass deployment config / env vars)
# ---------------------------------------------------------------------------
BROKER         = "127.0.0.1"
PORT           = 1883
SUB_TOPIC      = os.getenv("SUB_TOPIC",      "#")
CSV_LOG_DIR    = os.getenv("CSV_LOG_DIR",    "/greengrass/v2/oee_engine/logs")
LOG_QUEUE_SIZE = int(os.getenv("LOG_QUEUE_SIZE", "50000")) # Increase from 10000 to 50000
RETAIN_DAYS    = int(os.getenv("RETAIN_DAYS",    "7"))
CLIENT_ID      = f"csv-logger-{random.randint(0, 99999)}"

# Flush tuning — flush to disk every N rows OR every T seconds
FLUSH_EVERY_N_ROWS  = 5000  # Increased from 50 to 5000
FLUSH_INTERVAL_SECS = 10.0  # Increased from 5.0 to 10.0

# ---------------------------------------------------------------------------
# Timezone — matches live_state_engine.py
# ---------------------------------------------------------------------------
CENTRAL_TZ = ZoneInfo("America/Chicago")

# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------
shutdown_event = Event()
csv_log_queue  = Queue(maxsize=LOG_QUEUE_SIZE)
mqtt_connected = False

# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def now_central() -> datetime:
    return datetime.now(CENTRAL_TZ)

def now_iso() -> str:
    return now_central().isoformat()

def safe_print(*args, **kwargs):
    """Thread-safe timestamped print — Greengrass captures this into component logs."""
    print(f"{now_iso()} -", *args, **kwargs, flush=True)

# ---------------------------------------------------------------------------
# MQTT callbacks  (network thread — must stay non-blocking)
# ---------------------------------------------------------------------------
def on_connect(client, userdata, flags, rc):
    global mqtt_connected
    if rc == 0:
        mqtt_connected = True
        client.subscribe(SUB_TOPIC)
        safe_print(f"[MQTT] Connected. Subscribed to '{SUB_TOPIC}'")
    else:
        safe_print(f"[MQTT] Connection failed rc={rc} — will retry.")

def on_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False
    safe_print(f"[MQTT] Disconnected (rc={rc}). Auto-reconnect active.")

# def on_message(client, userdata, msg):
#     """
#     Runs on the MQTT network thread.
#     MUST stay non-blocking — only decode minimum fields and drop into queue.
#     Any slow operation here will lag or drop the MQTT connection.
#     """

#     try:
#         raw = msg.payload.decode("utf-8", errors="replace")
#         ts  = now_central().strftime("%Y-%m-%d %H:%M:%S")

#         try:
#             payload = json.loads(raw)
#         except json.JSONDecodeError:
#             payload = {}

#         # Extract fields matching live_state_engine payload shape
#         station_id = payload.get("stationID", "")
#         tag_name   = payload.get("tagName",   "")
#         value      = payload.get("details",   "")

#         # No RawPayload column — redundant data, was causing 3x file size
#         row = (ts, station_id, tag_name, str(value))

#         try:
#             csv_log_queue.put_nowait(row)
#         except Full:
#             safe_print("[CSV WARNING] Queue Full! Dropping MQTT message.")

#     except Exception:
#         pass  # Never let a bad payload crash the MQTT thread

def on_message(client, userdata, msg):
    """
    Runs on the MQTT network thread.
    Ultra-lightweight to ensure we never block incoming high-frequency packets.
    """
    try:
        # 1. Decode raw bytes to string instantly
        raw = msg.payload.decode("utf-8", errors="replace")
        
        # 2. Push directly to the queue. Parsing is handled by the worker thread.
        csv_log_queue.put_nowait(raw)
        
    except Full:
        # Warns you in Greengrass logs if Python is actually falling behind
        safe_print("[CSV WARNING] Queue Full! Dropping MQTT message. : ",raw)
    except Exception:
        safe_print("[CSV WARNING] bad payload. : ",raw)  # Prevent any bad packet from crashing the network thread

# ---------------------------------------------------------------------------
# Thread 1: CSV Logger Worker  (all disk I/O lives here)
# ---------------------------------------------------------------------------
# def csv_logger_worker():
#     """
#     Pulls rows from the in-memory queue and writes them to today's CSV file.

#     - Keeps file handle open all day (no open/close overhead per message)
#     - Rotates to a new file at midnight CST automatically
#     - csv.writer safely escapes commas, quotes, newlines inside values
#     - Flushes every FLUSH_EVERY_N_ROWS rows OR every FLUSH_INTERVAL_SECS seconds
#     """
#     safe_print("[CSV LOGGER] Worker thread started.")

#     current_date: str | None = None
#     file_handle              = None
#     writer                   = None
#     rows_since_flush         = 0
#     last_flush_time          = time.monotonic()

#     os.makedirs(CSV_LOG_DIR, exist_ok=True)

#     def open_todays_file(today_str: str):
#         path         = os.path.join(CSV_LOG_DIR, f"raw_mqtt_{today_str}.csv")
#         needs_header = not os.path.exists(path)
#         fh           = open(path, "a", newline="", encoding="utf-8")
#         w            = csv.writer(fh)
#         if needs_header:
#             w.writerow(["Timestamp_CST", "StationID", "TagName", "Value"])
#             fh.flush()
#         safe_print(f"[CSV LOGGER] Opened -> {path}")
#         return fh, w

#     while not shutdown_event.is_set():

#         # Block up to 2 s waiting for a row — allows timers to tick even at low rates
#         try:
#             row = csv_log_queue.get(timeout=2.0)
#         except Empty:
#             # No new data — check if a time-based flush is due
#             if file_handle and (time.monotonic() - last_flush_time) >= FLUSH_INTERVAL_SECS:
#                 # file_handle.flush()
#                 # last_flush_time = time.monotonic()
#                 try:
#                     file_handle.flush()
#                     last_flush_time = time.monotonic()
#                 except Exception as e:
#                     safe_print(f"[CSV LOGGER ERROR] Time-based flush failed: {e}")
#             continue

#         today_str = now_central().strftime("%Y-%m-%d")

#         # ── Daily file rotation at midnight CST ──────────────────────────
#         if today_str != current_date:
#             if file_handle:
#                 # file_handle.flush()
#                 try:
#                     file_handle.flush()
#                 except Exception as e:
#                     safe_print(f"[CSV LOGGER ERROR] Flush before close failed: {e}")
#                 file_handle.close()
#                 safe_print("[CSV LOGGER] Midnight rotation — closed yesterday's file.")

#             file_handle, writer = open_todays_file(today_str)
#             current_date        = today_str
#             rows_since_flush    = 0
#             last_flush_time     = time.monotonic()

#         # ── Write row ────────────────────────────────────────────────────
#         try:
#             writer.writerow(row)
#             rows_since_flush += 1
#         except Exception as e:
#             safe_print(f"[CSV LOGGER ERROR] Write failed: {e}")
#             continue

#         # ── Flush every N rows OR every T seconds ─────────────────────────
#         now = time.monotonic()
#         if rows_since_flush >= FLUSH_EVERY_N_ROWS or (now - last_flush_time) >= FLUSH_INTERVAL_SECS:
#             # file_handle.flush()
#             # rows_since_flush = 0
#             # last_flush_time  = now
#             try:
#                 file_handle.flush()
#                 rows_since_flush = 0
#                 last_flush_time  = now
#             except Exception as e:
#                 safe_print(f"[CSV LOGGER ERROR] Row-based flush failed: {e}")

#     # ── Clean shutdown ────────────────────────────────────────────────────
#     if file_handle:
#         file_handle.flush()
#         file_handle.close()
#         safe_print("[CSV LOGGER] File closed cleanly on shutdown.")

def csv_logger_worker():
    """
    Pulls raw strings from the queue, parses JSON, and logs to a daily CSV file.
    Flushes less aggressively to minimize disk I/O bottlenecks.
    """
    safe_print("[CSV LOGGER] Worker thread started.")

    current_date: str | None = None
    file_handle              = None
    writer                   = None
    rows_since_flush         = 0
    last_flush_time          = time.monotonic()

    os.makedirs(CSV_LOG_DIR, exist_ok=True)

    def open_todays_file(today_str: str):
        path         = os.path.join(CSV_LOG_DIR, f"raw_mqtt_{today_str}.csv")
        needs_header = not os.path.exists(path)
        fh           = open(path, "a", newline="", encoding="utf-8")
        w            = csv.writer(fh)
        if needs_header:
            w.writerow(["Timestamp_CST", "StationID", "TagName", "Value"])
            fh.flush()
        safe_print(f"[CSV LOGGER] Opened -> {path}")
        return fh, w

    while not shutdown_event.is_set():

        try:
            # Block up to 2 seconds waiting for raw message string
            raw_string = csv_log_queue.get(timeout=2.0)
        except Empty:
            # No new data — check if a time-based flush is due
            if file_handle and (time.monotonic() - last_flush_time) >= FLUSH_INTERVAL_SECS:
                try:
                    file_handle.flush()
                    last_flush_time = time.monotonic()
                except Exception as e:
                    safe_print(f"[CSV LOGGER ERROR] Time-based flush failed: {e}")
            continue

        # ── Parse JSON on this worker thread (Moved out of MQTT thread) ──
        ts = now_central().strftime("%Y-%m-%d %H:%M:%S")
        try:
            payload = json.loads(raw_string)
            station_id = payload.get("stationID", "")
            tag_name   = payload.get("tagName", "")
            value      = payload.get("details", "")
            row = (ts, station_id, tag_name, str(value))
        except Exception:
            continue  # Skip corrupt JSON payloads silently

        today_str = now_central().strftime("%Y-%m-%d")

        # ── Daily file rotation at midnight CST ──────────────────────────
        if today_str != current_date:
            if file_handle:
                try:
                    file_handle.flush()
                except Exception as e:
                    safe_print(f"[CSV LOGGER ERROR] Flush before close failed: {e}")
                file_handle.close()
                safe_print("[CSV LOGGER] Midnight rotation — closed yesterday's file.")

            file_handle, writer = open_todays_file(today_str)
            current_date        = today_str
            rows_since_flush    = 0
            last_flush_time     = time.monotonic()

        # ── Write row ────────────────────────────────────────────────────
        try:
            writer.writerow(row)
            rows_since_flush += 1
        except Exception as e:
            safe_print(f"[CSV LOGGER ERROR] Write failed: {e}")
            continue

        # ── Safe, efficient flushing to protect disk performance ─────────
        now = time.monotonic()
        if rows_since_flush >= FLUSH_EVERY_N_ROWS or (now - last_flush_time) >= FLUSH_INTERVAL_SECS:
            try:
                file_handle.flush()
                rows_since_flush = 0
                last_flush_time  = now
            except Exception as e:
                safe_print(f"[CSV LOGGER ERROR] Row-based flush failed: {e}")

    # ── Clean shutdown ────────────────────────────────────────────────────
    if file_handle:
        file_handle.flush()
        file_handle.close()
        safe_print("[CSV LOGGER] File closed cleanly on shutdown.")

# ---------------------------------------------------------------------------
# Thread 2: Cleanup Worker  (compression + deletion)
# ---------------------------------------------------------------------------
def cleanup_worker():
    """
    Runs once on startup, then every 24 hours.

    Step 1 — Compress: any closed .csv file (not today's) gets gzipped.
              raw_mqtt_YYYY-MM-DD.csv  ->  raw_mqtt_YYYY-MM-DD.csv.gz
              Typical saving: 85-92% smaller. Yesterday's 35 MB -> ~4 MB.

    Step 2 — Delete: any .csv.gz (or uncompressed .csv) file whose date is
              older than RETAIN_DAYS gets permanently removed.
              No client action needed ever.
    """
    safe_print(f"[CLEANUP] Started. Retention = {RETAIN_DAYS} days. "
               f"Files older than {RETAIN_DAYS} days will be deleted automatically.")

    def compress_closed_files():
        """Gzip every past day's .csv that has not been compressed yet."""
        today_str = now_central().strftime("%Y-%m-%d")

        try:
            files = sorted(os.listdir(CSV_LOG_DIR))
        except Exception as e:
            safe_print(f"[COMPRESS ERROR] Cannot list dir: {e}")
            return

        for fname in files:
            if not (fname.startswith("raw_mqtt_") and fname.endswith(".csv")):
                continue

            date_str = fname.replace("raw_mqtt_", "").replace(".csv", "")

            # Never compress today's file — it is still open and being written to
            if date_str == today_str:
                continue

            fpath   = os.path.join(CSV_LOG_DIR, fname)
            gz_path = fpath + ".gz"

            # Already compressed — skip
            if os.path.exists(gz_path):
                continue

            try:
                original_mb = os.path.getsize(fpath) / 1_048_576
                with open(fpath, "rb") as f_in:
                    with gzip.open(gz_path, "wb", compresslevel=6) as f_out:
                        shutil.copyfileobj(f_in, f_out)

                compressed_mb = os.path.getsize(gz_path) / 1_048_576
                saving_pct    = ((original_mb - compressed_mb) / original_mb * 100) if original_mb > 0 else 0

                # Only remove original after .gz is confirmed written successfully
                os.remove(fpath)
                safe_print(f"[COMPRESS] {fname} -> {fname}.gz  "
                           f"({original_mb:.1f} MB -> {compressed_mb:.1f} MB, "
                           f"{saving_pct:.0f}% saved)")

            except Exception as e:
                safe_print(f"[COMPRESS ERROR] {fname}: {e}")
                # Remove partial .gz to avoid a corrupt file next time
                if os.path.exists(gz_path):
                    try:
                        os.remove(gz_path)
                    except Exception:
                        pass

    def delete_old_files():
        """Delete .csv.gz files (and any leftover .csv) older than RETAIN_DAYS."""
        cutoff     = now_central() - timedelta(days=RETAIN_DAYS)
        cutoff_str = cutoff.strftime("%Y-%m-%d")

        try:
            files = sorted(os.listdir(CSV_LOG_DIR))
        except Exception as e:
            safe_print(f"[CLEANUP ERROR] Cannot list dir: {e}")
            return

        deleted  = 0
        freed_mb = 0.0

        for fname in files:
            # Match both compressed and uncompressed variants
            if fname.startswith("raw_mqtt_") and fname.endswith(".csv.gz"):
                date_str = fname.replace("raw_mqtt_", "").replace(".csv.gz", "")
            elif fname.startswith("raw_mqtt_") and fname.endswith(".csv"):
                date_str = fname.replace("raw_mqtt_", "").replace(".csv", "")
            else:
                continue

            # Validate it is actually a date string before comparing
            try:
                datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                continue

            # Still within retention window — keep it
            if date_str >= cutoff_str:
                continue

            fpath = os.path.join(CSV_LOG_DIR, fname)
            try:
                size_mb   = os.path.getsize(fpath) / 1_048_576
                os.remove(fpath)
                freed_mb += size_mb
                deleted  += 1
                safe_print(f"[CLEANUP] Deleted {fname} ({size_mb:.1f} MB) "
                           f"— older than {RETAIN_DAYS} days")
            except Exception as e:
                safe_print(f"[CLEANUP ERROR] Could not delete {fname}: {e}")

        if deleted:
            safe_print(f"[CLEANUP] Done — removed {deleted} file(s), freed {freed_mb:.1f} MB.")
        else:
            safe_print(f"[CLEANUP] Done — nothing to delete (cutoff: {cutoff_str}).")

    # ── Main cleanup loop ─────────────────────────────────────────────────
    while not shutdown_event.is_set():
        compress_closed_files()  # Step 1: gzip yesterday and older
        delete_old_files()       # Step 2: delete anything beyond RETAIN_DAYS
        # Sleep 24 hours but wake instantly if shutdown signal is received
        shutdown_event.wait(timeout=86400)

    safe_print("[CLEANUP] Thread stopped.")

# ---------------------------------------------------------------------------
# Signal handlers
# ---------------------------------------------------------------------------
def handle_exit(sig, frame):
    safe_print(f"[SYSTEM] Signal {sig} received — shutting down gracefully.")
    shutdown_event.set()

signal.signal(signal.SIGTERM, handle_exit)
signal.signal(signal.SIGINT,  handle_exit)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    safe_print("[SYSTEM] CSV Logger component starting...")
    safe_print(f"[SYSTEM] Broker={BROKER}:{PORT} | Topic={SUB_TOPIC} | "
               f"Dir={CSV_LOG_DIR} | Retain={RETAIN_DAYS} days")

    # Start background threads before MQTT connects so no messages are lost
    logger_thread  = Thread(target=csv_logger_worker, daemon=True, name="csv_logger")
    cleanup_thread = Thread(target=cleanup_worker,    daemon=True, name="cleanup")
    logger_thread.start()
    cleanup_thread.start()

    # Set up MQTT client
    client = mqtt_client.Client(client_id=CLIENT_ID)
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    # Retry connect loop — broker may not be ready on cold start
    while not shutdown_event.is_set():
        try:
            client.connect(BROKER, PORT, keepalive=60)
            break
        except Exception as e:
            safe_print(f"[MQTT] Cannot connect: {e} — retrying in 5s...")
            shutdown_event.wait(timeout=5)

    client.loop_start()
    safe_print("[SYSTEM] MQTT loop running. Logger active.")

    # Block main thread until shutdown signal
    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    finally:
        shutdown_event.set()
        try:
            client.loop_stop()
            client.disconnect()
        except Exception:
            pass

        logger_thread.join(timeout=10)  # wait for final flush
        cleanup_thread.join(timeout=5)
        safe_print("[SYSTEM] CSV Logger component stopped cleanly.")

if __name__ == "__main__":
    main()