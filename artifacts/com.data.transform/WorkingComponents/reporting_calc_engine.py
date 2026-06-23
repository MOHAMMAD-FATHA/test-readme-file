#!/usr/bin/env python3
"""
Component 3: Calculation & Reporting Engine
Features:
- Publishes individual metrics (Performance, Quality, OEE, Utilization, Shift) 
  alongside the main JSON summaries for dashboarding.
- Atomic DB transactions & QoS 1 MQTT publishing.
"""

import os
import json
import time
import sqlite3
from datetime import datetime, timedelta, time as tm
from threading import Thread, Event, RLock
import paho.mqtt.client as mqtt_client
from zoneinfo import ZoneInfo

CENTRAL_TZ = ZoneInfo("America/Chicago")
DB_PATH = os.getenv("DB_PATH", "/tmp/stations_data.db")
SITEWISE_MODEL_NAME = os.getenv("SITEWISE_MODEL_NAME")
CONFIG_PATH = os.getenv("CONFIG_PATH")
BROKER = "127.0.0.1"
PORT = 1883
PUB_TOPIC = "python/mqtt"

shutdown_event = Event()
db_lock = RLock()
client = mqtt_client.Client(client_id=f'python-mqtt-report-{int(time.time())}')
CONFIG = {}
CONFIG_MTIME = None

def load_config():
    global CONFIG
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r") as f:
                CONFIG = json.load(f)
    except Exception: pass

def safe_print(*args): 
    print(f"{datetime.now(CENTRAL_TZ).isoformat()} -", *args)

# -------------------------------------------------
# QoS 1 Publish with Synchronous ACK
# -------------------------------------------------
def publish_with_ack(payload):
    try: 
        msg_info = client.publish(PUB_TOPIC, payload, qos=1)
        msg_info.wait_for_publish(timeout=10.0)
        if msg_info.is_published():
            safe_print("[MQTT] Publish successful and acknowledged by broker.", "Payload:", payload)
            return True
        else:
            safe_print("[MQTT] Publish timeout. Payload not acknowledged.")
            return False
    except Exception as e: 
        safe_print("[MQTT] publish failed:", e)
        return False

# -------------------------------------------------
# Shift & Boundary Math
# -------------------------------------------------
def get_shift_date_cst(ts_epoch):
    dt = datetime.fromtimestamp(ts_epoch, CENTRAL_TZ)
    if dt.hour < 6: dt = dt.replace(hour=0, minute=0, second=0) - timedelta(days=1)
    return dt.strftime("%Y-%m-%d")

def get_shift(ts_epoch):
    """Calculates the Shift letter (A, B, C) based on the timestamp."""
    dt = datetime.fromtimestamp(ts_epoch, CENTRAL_TZ)
    t = dt.time()
    # Default shift times based on standard 3-shift model (06:00, 14:00, 22:00)
    if tm(6, 0) <= t < tm(14, 0): return "A"
    elif tm(14, 0) <= t < tm(22, 0): return "B"
    else: return "C"

def get_shift_splits(start_ts, end_ts):
    splits = set()
    curr_ts = start_ts
    end_ts = end_ts if end_ts else start_ts 
    
    while curr_ts <= end_ts:
        splits.add(get_shift_date_cst(curr_ts))
        curr_dt = datetime.fromtimestamp(curr_ts, CENTRAL_TZ)
        if curr_dt.hour >= 6:
            next_boundary_dt = (curr_dt + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)
        else:
            next_boundary_dt = curr_dt.replace(hour=6, minute=0, second=0, microsecond=0)
        curr_ts = int(next_boundary_dt.timestamp())
        if start_ts == end_ts: break
        
    return splits

def now_central():
    return datetime.now(CENTRAL_TZ)


def get_shift_net_minutes(station_id, shift_name):
    """Calculates Total Shift Mins - Planned Break Mins based on the config."""
    try:
        station_cfg = CONFIG.get("stations", {}).get(station_id, {})
        shift_group = station_cfg.get("shifts", "2shifts")
        shift_cfg = CONFIG.get("shifts", {}).get(shift_group, {}).get(shift_name, {})
        
        if not shift_cfg: return 480.0
        
        start_t = parse_hhmm(shift_cfg.get("start", "00:00"))
        end_t = parse_hhmm(shift_cfg.get("end", "00:00"))
        
        start_mins = start_t.hour * 60 + start_t.minute
        end_mins = end_t.hour * 60 + end_t.minute
        if end_mins <= start_mins: end_mins += 24 * 60
        total_mins = end_mins - start_mins
        
        break_mins = 0
        for brk in shift_cfg.get("breaks", []):
            bs = parse_hhmm(brk.get("start", "00:00"))
            be = parse_hhmm(brk.get("end", "00:00"))
            bs_m = bs.hour * 60 + bs.minute
            be_m = be.hour * 60 + be.minute
            if be_m <= bs_m: be_m += 24 * 60
            break_mins += (be_m - bs_m)
            
        net_mins = total_mins - break_mins
        return float(net_mins) if net_mins > 0 else 480.0
    except Exception as e:
        safe_print(f"[SHIFT MINS] Error calculating shift net minutes: {e}")
        return 480.0
# -------------------------
# Time utilities
# -------------------------
def parse_hhmm(tstr):
    try:
        parts = tstr.split(":")
        return tm(int(parts[0]), int(parts[1]))
    except Exception:
        return None
    
# -------------------------
# SHIFT & BREAK LOGIC (from config)
# -------------------------
def get_shift_for_station(station_id):
    # now_dt = datetime.now(timezone.utc)
    now_dt = now_central()
    now_time = now_dt.time()

    station_cfg = CONFIG.get("stations", {}).get(station_id, {})
    shift_group = station_cfg.get("shifts", "2shifts")
    shifts_map = CONFIG.get("shifts", {})

    # print(f"[SHIFT-DEBUG] Shift group: {shift_group}")

    try:
        if shift_group not in shifts_map:
            # print("[SHIFT-DEBUG] Shift group missing. Using fallback.")
            shift = "A" if tm(6,0) <= now_time < tm(18,0) else "B"
            # print(f"[SHIFT-DEBUG] Fallback: {shift}")
            return shift

        group = shifts_map[shift_group]

        for sname, win in group.items():
            start_t = parse_hhmm(win["start"])
            end_t   = parse_hhmm(win["end"])

            print(f"\n[SHIFT-DEBUG] Checking shift {sname}: {start_t} â†’ {end_t}")

            # NORMAL SHIFT (does NOT cross midnight)
            if end_t > start_t:

                start_dt = datetime.combine(
                    now_dt.date(),
                    start_t,
                    tzinfo=CENTRAL_TZ
                )

                end_dt = datetime.combine(
                    now_dt.date(),
                    end_t,
                    tzinfo=CENTRAL_TZ
                )


            # OVERNIGHT SHIFT (crosses midnight, e.g., 18:00 â†’ 06:00)
            else:
                print("[SHIFT-DEBUG] Overnight shift")

                # If current time < end â†’ after midnight â†’ shift started yesterday
                if now_time < end_t:

                    start_dt = datetime.combine(
                        now_dt.date() - timedelta(days=1),
                        start_t,
                        tzinfo=CENTRAL_TZ
                    )

                    end_dt = datetime.combine(
                        now_dt.date(),
                        end_t,
                        tzinfo=CENTRAL_TZ
                    )

                else:

                    start_dt = datetime.combine(
                        now_dt.date(),
                        start_t,
                        tzinfo=CENTRAL_TZ
                    )

                    end_dt = datetime.combine(
                        now_dt.date() + timedelta(days=1),
                        end_t,
                        tzinfo=CENTRAL_TZ
                    )


                # print(f"[SHIFT-DEBUG] Start: {start_dt}")
                # print(f"[SHIFT-DEBUG] End:   {end_dt}")

            # FINAL MATCH CHECK
            if start_dt <= now_dt <= end_dt:
                print(f"[SHIFT-DEBUG] MATCH â†’ {sname}")
                return sname
            else:
                print("[SHIFT-DEBUG] No match.")

        # If no match, fall back to the first shift
        fallback_shift = list(group.keys())[0]
        # print(f"[SHIFT-DEBUG] No shift matched â†’ fallback: {fallback_shift}")
        return fallback_shift

    except Exception as e:
        print("[SHIFT] ERROR:", e)
        return "A"
    
# -------------------------------------------------
# Event Loop: Job Summary 
# -------------------------------------------------
def fetch_reporting_triggers():
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT station_id, key, value FROM runtime_state WHERE category='reporting_trigger'")
        rows = cur.fetchall()
        conn.close()
    return rows

def clear_trigger(station_id, key):
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        conn.execute("DELETE FROM runtime_state WHERE station_id=? AND category='reporting_trigger' AND key=?", (station_id, key))
        conn.commit(); conn.close()

def build_job_summary(job_id):
    """Returns (list_of_payloads, OA, QR, OR_job, split_days_set, station_id)"""
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        # cur.execute("""
        # SELECT station_id, start_ts, end_ts, operator_sec, diesetter_sec, downtime_sec, planned_run_mins, 
        #        good_parts, scrap_parts, min_op_start, max_op_end, min_co_start, max_co_end, 
        #        morder, die_number, part_number, spm, plan_qty, shift
        # FROM jobs WHERE job_id=?
        # """, (job_id,))
        # r = cur.fetchone()

        cur.execute("""SELECT 
                    station_id, 
                    start_ts, 
                    end_ts, 
                    COALESCE(operator_sec, 0.0), 
                    COALESCE(diesetter_sec, 0.0), 
                    COALESCE(downtime_sec, 0.0), 
                    COALESCE(planned_run_mins, 0.0), 
                    COALESCE(good_parts, 0), 
                    COALESCE(scrap_parts, 0), 
                    min_op_start, 
                    max_op_end, 
                    min_co_start, 
                    max_co_end, 
                    COALESCE(morder, ''), 
                    COALESCE(die_number, ''), 
                    COALESCE(part_number, ''), 
                    COALESCE(spm, 0.0), 
                    COALESCE(plan_qty, 0), 
                    shift
                FROM jobs WHERE job_id=?""", (job_id,))
        
        r = cur.fetchone()

        if not r: 
            conn.close(); return [], 0, 0, 0, set(), ""

        (station, start_ts, end_ts, op_sec, co_sec, dt_sec, plan_mins, good, scrap, 
         min_op, max_op, min_co, max_co, morder, die, part, spm, plan_qty, shift_letter) = r

        cur.execute("SELECT reason, SUM(duration_sec) FROM downtime_events WHERE job_id=? GROUP BY reason", (job_id,))
        dt_rows = cur.fetchall()
        conn.close()

    # RT = (op_sec or 0) / 60.0
    # CT = (co_sec or 0) / 60.0
    # DT = (dt_sec or 0) / 60.0 
    
    # denom = RT + CT
    # OA = (plan_mins / denom) if denom > 0 else 0.0
    # total = (good or 0) + (scrap or 0)
    # QR = (good / total) if total > 0 else 0.0

    # Force conversion to float to handle potential string/None issues from DB
    RT = (float(op_sec) if op_sec else 0.0) / 60.0
    CT = (float(co_sec) if co_sec else 0.0) / 60.0
    DT = (float(dt_sec) if dt_sec else 0.0) / 60.0
    plan_mins_f = float(plan_mins) if plan_mins else 0.0

    denom = RT + CT
    OA = (plan_mins_f / denom) if denom > 0 else 0.0

    # Do the same for Quality components
    good_f = float(good) if good else 0.0
    scrap_f = float(scrap) if scrap else 0.0
    total = good_f + scrap_f
    QR = (good_f / total) if total > 0 else 0.0

    end_ts_safe = end_ts if end_ts else int(time.time())
    split_days = get_shift_splits(start_ts, end_ts_safe)
    primary_day = get_shift_date_cst(start_ts)

    # --- NEW OR CALCULATION USING CONFIG MINS ---
    net_shift_mins = get_shift_net_minutes(station, shift_letter)
    OR_job = (RT - DT) / net_shift_mins if net_shift_mins > 0 else 0.0

    dt_summary = {}
    for reason, duration_sec in dt_rows:
        r_clean = str(reason).strip() if reason else "Unknown Reason"
        dt_summary[r_clean] = dt_summary.get(r_clean, 0.0) + (duration_sec / 60.0)

    data = {
        "ShiftDateCST": primary_day,
        "Shift": shift_letter, # Fetched directly from the new jobs table column!
        "Asset": station,
        "MOrder": morder, "DieNumber": die, "PartNumber": part, "SPM": spm, "PlanQty": plan_qty,
        "OA": OA, "Quality": QR, "OR": OR_job,
        "OperatorMinutes": RT, "ChangeoverMinutes": CT, "DowntimeMinutes": DT,
        "GoodQty": good, "ScrapQty": scrap,
        "Operator_Start": datetime.fromtimestamp(min_op, CENTRAL_TZ).isoformat() if min_op else None,
        "Operator_End": datetime.fromtimestamp(max_op, CENTRAL_TZ).isoformat() if max_op else None,
        "Changeover_Start": datetime.fromtimestamp(min_co, CENTRAL_TZ).isoformat() if min_co else None,
        "Changeover_End": datetime.fromtimestamp(max_co, CENTRAL_TZ).isoformat() if max_co else None,
    }
    
    data["DowntimeReasons"] = dt_summary

    timestamp_block = {"timeInSeconds": int(end_ts_safe)}
    payloads = []

    payloads.append(json.dumps({
        "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station}/JobSummary",
        "propertyValues": [{"timestamp": timestamp_block, "quality": "GOOD", "value": {"stringValue": json.dumps(data)}}]
    }))

    # -------------------------------------------------------------------------
    # CHANGE THIS BLOCK in build_job_summary:
    # -------------------------------------------------------------------------

    payloads.append(json.dumps({
        "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station}/PerformancePerJob",
        "propertyValues": [{"timestamp": timestamp_block, "quality": "GOOD", "value": {"stringValue": f"{float(OA)}:{shift_letter}"}}]
    }))
    
    payloads.append(json.dumps({
        "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station}/QualityPerJob",
        "propertyValues": [{"timestamp": timestamp_block, "quality": "GOOD", "value": {"stringValue": f"{float(QR)}:{shift_letter}"}}]
    }))
    
    # Note: I noticed your alias is "UtilizationPerDay" in SiteWise but the tag represents the Job. 
    # Ensure the alias exactly matches what is in SiteWise!
    payloads.append(json.dumps({
        "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station}/UtilizationPerDay",
        "propertyValues": [{"timestamp": timestamp_block, "quality": "GOOD", "value": {"stringValue": f"{float(OR_job)}:{shift_letter}"}}]
    }))
    
    return payloads, OA, QR, OR_job, split_days, station

def event_engine():
    safe_print("[REPORTING] Event engine started. Waiting for triggers...")
    while not shutdown_event.is_set():
        try:
            triggers = fetch_reporting_triggers()
            if not triggers:
                shutdown_event.wait(2)
                continue

            safe_print(f"[REPORTING] Found {len(triggers)} triggers in the database! Processing...")

            for station_id, key, val in triggers:
                safe_print(f"[REPORTING] Processing trigger: {key} for {station_id}")
                try: 
                    data = json.loads(val)
                except Exception as e: 
                    safe_print(f"[REPORTING ERROR] Bad JSON: {e}")
                    clear_trigger(station_id, key)
                    continue

                if data.get("type") == "JOB_FINALIZED":
                    payloads, oa, qr, or_val, split_days, stn = build_job_summary(data.get("job_id"))
                    
                    if payloads:
                        safe_print(f"[REPORTING] Generated {len(payloads)} payloads. Publishing...")
                        all_published = True
                        for idx, payload in enumerate(payloads):
                            
                            # --- NEW: Print payload and slow down the publish rate ---
                            safe_print(f"[DEBUG PAYLOAD] {payload}")
                            if not publish_with_ack(payload):
                                all_published = False
                            time.sleep(0.5) # Give SiteWise Edge Gateway time to process!
                            # ---------------------------------------------------------
                                
                        if all_published:
                            with db_lock:
                                conn = sqlite3.connect(DB_PATH)
                                cur = conn.cursor()
                                for day_str in split_days:
                                    cur.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (day_str, stn))
                                    cur.execute("""
                                        UPDATE daily_agg
                                        SET oa_sum = oa_sum + ?,  oa_count = oa_count + 1, 
                                            qr_sum = qr_sum + ?, qr_count = qr_count + 1,
                                            or_sum = or_sum + ?, or_count = or_count + 1
                                        WHERE day=? AND station_id=?
                                    """, (oa, qr, or_val, day_str, stn))
                                    
                                cur.execute("DELETE FROM runtime_state WHERE station_id=? AND category='reporting_trigger' AND key=?", (station_id, key))
                                conn.commit(); conn.close()
                            safe_print(f"[REPORTING] Successfully cleared trigger: {key}\n")
                        else:
                            safe_print(f"[REPORTING WARNING] Publish failed for {key}.\n")
                    else:
                        clear_trigger(station_id, key)
                        
        except Exception as e:
            safe_print(f"[REPORTING] CRITICAL ERROR: {str(e)}")
            shutdown_event.wait(5)
# -------------------------------------------------
# Daily Loop: OEE & Utilization 
# -------------------------------------------------
def get_daily_compute_time():
    try:
        start = CONFIG.get("shifts", {}).get("2shifts", {}).get("A", {}).get("start")
        if start:
            h, m = start.split(":")
            return int(h), int(m)
    except Exception: pass
    return 6, 0

def daily_loop():
    safe_print("[REPORTING] Daily Loop started")
    while not shutdown_event.is_set():
        now = datetime.now(CENTRAL_TZ)
        h, m = get_daily_compute_time()
        
        shift_boundary = datetime.combine(now.date(), tm(h, m), tzinfo=CENTRAL_TZ)
        if now < shift_boundary:
            target_day_dt = now - timedelta(days=2) 
        else:
            target_day_dt = now - timedelta(days=1)
            
        target_day_str = target_day_dt.strftime("%Y-%m-%d")

        run_needed = False
        with db_lock:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT value FROM runtime_state WHERE category='reporting' AND key='last_daily_run'")
            row = cur.fetchone()
            if not row or row[0] != target_day_str: run_needed = True
            conn.close()

        if run_needed:
            safe_print(f"[DAILY] Running Daily Summary for {target_day_str}")
            with db_lock:
                conn = sqlite3.connect(DB_PATH)
                cur = conn.cursor()
                cur.execute("""
                SELECT station_id, operator_sec, downtime_sec, oa_sum, oa_count, qr_sum, qr_count,or_sum, or_count
                FROM daily_agg WHERE day=?
                """, (target_day_str,))
                rows = cur.fetchall()
                conn.close()

            daily_metrics_to_save = []
            all_published = True
            for station, op_sec, dt_sec, oa_sum, oa_count, qr_sum, qr_count, or_sum, or_count in rows:
                avg_OA = (oa_sum / oa_count) if oa_count and oa_count > 0 else 0
                avg_QR = (qr_sum / qr_count) if qr_count and qr_count > 0 else 0
                avg_OR = (or_sum / or_count) if or_count and or_count > 0 else 0

                OEE_daily = avg_OA * avg_QR * avg_OR
                    
                shift = get_shift_for_station(station_id=station)

                daily_metrics_to_save.append((OEE_daily, avg_OA, avg_OR, avg_QR, target_day_str, station))
                
                timestamp_block = {"timeInSeconds": int(time.time())}

                # 1. Main JSON Blob
                payload_json = json.dumps({
                    "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station}/DailySummary",
                    "propertyValues": [{"timestamp": timestamp_block, "quality": "GOOD", "value": {"stringValue": json.dumps({"ShiftDateCST": target_day_str, "OR": avg_OR, "OEE": OEE_daily})}}]
                })
                
                # 2. Individual OEE Metric
                payload_oee = json.dumps({
                    "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station}/OEEPerDay",
                    "propertyValues": [{"timestamp": timestamp_block, "quality": "GOOD", "value": {"stringValue": f"{float(OEE_daily)}:{shift}"}}]
                })

                # 3. Individual Utilization Metric
                payload_or = json.dumps({
                    "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station}/UtilizationPerDay",
                    "propertyValues": [{"timestamp": timestamp_block, "quality": "GOOD", "value": {"stringValue": f"{float(avg_OR)}:{shift}"}}]
                })
                
                # Publish all three for this station
                for p in [payload_json, payload_oee, payload_or]:
                    if not publish_with_ack(p):
                        all_published = False

            if all_published:
                with db_lock:
                    conn = sqlite3.connect(DB_PATH)

                    # --- NEW BATCH UPDATE ---
                    conn.executemany("""
                        UPDATE daily_agg 
                        SET final_oee = ?, final_oa = ?, final_or = ?, final_qr = ?
                        WHERE day = ? AND station_id = ?
                    """, daily_metrics_to_save)
                    # ------------------------
                    
                    conn.execute("""
                        INSERT INTO runtime_state(station_id, category, key, value, updated_ts)
                        VALUES ('GLOBAL', 'reporting', 'last_daily_run', ?, ?)
                        ON CONFLICT(station_id, category, key) DO UPDATE SET value=excluded.value, updated_ts=excluded.updated_ts
                    """, (target_day_str, int(time.time())))
                    
                    thirty_days_ago = "strftime('%s', 'now', '-30 days')"
                    thirty_days_date = "date('now', '-30 days')"
                    conn.execute("DELETE FROM jobs WHERE end_ts IS NOT NULL AND end_ts < strftime('%s', 'now', '-30 days')")
                    conn.execute(f"DELETE FROM downtime_events WHERE end_ts IS NOT NULL AND end_ts < {thirty_days_ago}")
                    conn.execute(f"DELETE FROM daily_agg WHERE day < {thirty_days_date}")
                    conn.commit(); conn.close()
            else:
                safe_print(f"[DAILY] Network error. Will retry {target_day_str} in 60 seconds.")
                shutdown_event.wait(60)
                continue 

        next_run = datetime.combine(now.date(), tm(h, m), tzinfo=CENTRAL_TZ)
        if now >= next_run: next_run += timedelta(days=1)
        wait_sec = max(1, (next_run - now).total_seconds())
        
        safe_print(f"[DAILY] Sleeping until {next_run.isoformat()}")
        shutdown_event.wait(wait_sec)


# -------------------------------------------------
# Shift Monitor Loop (Live Shift Publishing)
# -------------------------------------------------
def shift_monitor_loop():
    safe_print("[SHIFT MONITOR] Started")
    last_shift = {}
    last_shift_date = {}

    while not shutdown_event.is_set():
        now_ts = int(time.time())
        current_date = get_shift_date_cst(now_ts)
        current_shift = get_shift(now_ts)

        # Get list of stations from the JSON config
        stations = CONFIG.get("stations", [])
        
        for station in stations:
            # 1. Did the Shift letter change? (e.g., A -> B)
            if last_shift.get(station) != current_shift:
                payload_shift = json.dumps({
                    "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station}/Shift",
                    "propertyValues": [{"timestamp": {"timeInSeconds": now_ts}, "quality": "GOOD", "value": {"stringValue": current_shift}}]
                })
                if publish_with_ack(payload_shift):
                    last_shift[station] = current_shift
                    safe_print(f"[SHIFT PUBLISH] {station} transitioned to Shift {current_shift}")

            # 2. Did the Shift Date change? (e.g., crossed 6:00 AM)
            if last_shift_date.get(station) != current_date:
                payload_date = json.dumps({
                    "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station}/ShiftDateCST",
                    "propertyValues": [{"timestamp": {"timeInSeconds": now_ts}, "quality": "GOOD", "value": {"stringValue": current_date}}]
                })
                if publish_with_ack(payload_date):
                    last_shift_date[station] = current_date
                    safe_print(f"[SHIFT PUBLISH] {station} transitioned to Date {current_date}")

        # Go to sleep for 60 seconds before checking again
        shutdown_event.wait(60)

def config_reloader():
    """Wakes up every 30 seconds to hot-reload the config JSON if it changed."""
    global CONFIG_MTIME
    last_mtime = None
    while not shutdown_event.is_set():
        try:
            if os.path.exists(CONFIG_PATH):
                current_mtime = os.path.getmtime(CONFIG_PATH)
                if current_mtime != last_mtime:
                    load_config()
                    last_mtime = current_mtime
                    safe_print("[CONFIG] Hot-reloaded configuration file.")
        except Exception as e:
            pass
        shutdown_event.wait(30)

# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    load_config()
    
    # --- Patient MQTT Connection Loop (Fixes Errno 111) ---
    connected = False
    while not connected and not shutdown_event.is_set():
        try:
            client.connect(BROKER, PORT, 60)
            connected = True
            safe_print("[SYSTEM] Successfully connected to local MQTT Broker.")
        except ConnectionRefusedError:
            safe_print("[SYSTEM] MQTT Broker not ready yet. Retrying in 5 seconds...")
            time.sleep(5)

    client.loop_start()

    Thread(target=config_reloader, daemon=True).start()
    Thread(target=event_engine, daemon=True).start()
    Thread(target=daily_loop, daemon=True).start()
    Thread(target=shift_monitor_loop, daemon=True).start()

    try:
        while not shutdown_event.is_set(): 
            time.sleep(1)
    except KeyboardInterrupt:
        safe_print("[SYSTEM] Shutting down Reporting Engine...")
        shutdown_event.set()
        client.loop_stop()
        client.disconnect()
# def main():
#     load_config()
#     client.connect(BROKER, PORT, 60)
#     client.loop_start()

#     # Add this line:
#     Thread(target=config_reloader, daemon=True).start()
#     Thread(target=event_engine, daemon=True).start()
#     Thread(target=daily_loop, daemon=True).start()

#     # ADD THIS LINE HERE:
#     Thread(target=shift_monitor_loop, daemon=True).start()

#     try:
#         while True: time.sleep(1)
#     except KeyboardInterrupt:
#         shutdown_event.set()
#         client.loop_stop()

if __name__ == "__main__":
    main()