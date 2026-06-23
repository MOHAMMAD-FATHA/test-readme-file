#!/usr/bin/env python3
"""
Production-hardened Greengrass component for OEE/job tracking.

Improvements:
 - MQTT publisher thread with retry/backoff
 - Bounded worker queue (backpressure) with safe drop + logging
 - Worker watchdog (auto-restart if crashed/stalled)
 - Input validation for MQTT payloads
 - DB integrity check and backup on startup
 - MQTT reconnect handling
 - Consistent UTC timestamp handling
 - Defensive SQL (whitelist columns)
 - Graceful shutdown of all threads
 - All original business rules unchanged:
     - Job start/end: B_JobInProgressForIgnition
     - OA_job = N_OperationPlannedRunMins / (RT + CT)  using latest absolute value for N_OperationPlannedRunMins # Performance 
     - QR_job = good/(good+scrap) using latest absolute values # Quality
     - OR_daily = (RT - DT) / 1270  (RT/DT in minutes) # Utilization
     - OEE_daily = avg(OA_job) * avg(QR_job) * OR_daily # OEE
 - Publish the status (Active/Paused) and Persist runtime state (machine state/call buttons/downtime reasons) in SQLite for restart-safe recover
"""

import json
import time
import random
import signal
import os
import shutil
import sqlite3
from datetime import datetime, time as tm, timezone, timedelta
from threading import Thread, Lock, Event
from queue import Queue, Empty, Full
import paho.mqtt.client as mqtt_client
from zoneinfo import ZoneInfo


#--------------------------
# Central Time Zone
#--------------------------

CENTRAL_TZ = ZoneInfo("America/Chicago")

# -------------------------
# CONFIGURATION (tweakable)
# -------------------------
CONFIG_PATH = os.getenv('CONFIG_PATH')#"/greengrass/v2/work/com.data.transform/station_config.json"
DB_PATH = os.getenv('DB_PATH')#"/greengrass/v2/work/com.data.transform/stations_data.db"
DB_BACKUP_DIR = os.getenv('DB_BACKUP_DIR')#"/greengrass/v2/work/com.data.transform/db_backups"
SITEWISE_MODEL_NAME =os.getenv('SITEWISE_MODEL_NAME')#dnadct-oa2-dev-mct-model_core

BROKER = "127.0.0.1"
PORT = 1883
SUB_TOPIC = os.getenv("SUB_TOPIC")#"oa/us/dna/dttp/dttp/+/+/+"
PUB_TOPIC = "python/mqtt"
CLIENT_ID = f'python-mqtt-{random.randint(0, 10000)}'
# queues and limits
MESSAGE_QUEUE_MAXSIZE = 5000   # bound for incoming messages to avoid OOM
PUBLISH_QUEUE_MAXSIZE = 2000   # buffered publishes awaiting send or retry
PUBLISH_RETRY_MAX_ATTEMPTS = 5
PUBLISH_RETRY_BASE_DELAY = 1.0  # seconds

# watchdog interval
WORKER_WATCHDOG_INTERVAL = 10   # seconds
WORKER_STALL_TIMEOUT = 30       # seconds without processing considered stalled


# ==========================================================
# STATE (IMPORTANT)
# ==========================================================
GET_IN_FLIGHT = False   # prevents duplicate GETs

# -------------------------
# AWS IoT SHADOW CONFIG
# -------------------------
AWS_THING_NAME  = os.getenv("AWS_IOT_THING_NAME")
AWS_REGION      = os.getenv("AWS_REGION")

AWS_ENDPOINT    = os.getenv('AWS_ENDPOINT')#"a3mw4u4go8765p-ats.iot.us-east-1.amazonaws.com"
AWS_SHADOW_NAME = os.getenv("AWS_SHADOW_NAME")
AWS_PORT = 8883

# shutdown flag and events
shutdown_flag = False
shutdown_event = Event()

# -------------------------
# Global runtime state
# -------------------------
mqtt_connected = False
mqtt_client_instance = None

previous_station_status = {}


CONFIG = {"stations": {}}
CONFIG_MTIME = None

# tags / sets
MACHINE_STATE_TAGS = set([
    "B_JobInProgressForIgnition",
    "B_DowntimeInProgressForIgnition",
    "B_DieSetterInProgressForIgnition",
    "B_OperatorInProgressForIgnition",
    "N_DieSPM",
    "PartsPerMinute"
])

REQUIRED_TAGS = set([
    "B_DowntimeInProgressForIgnition",
    "B_DieSetterInProgressForIgnition",
    "B_OperatorInProgressForIgnition",
    "N_DieSPM",
    "PartsPerMinute",
    "N_OperationPlannedRunMins",
    "N_GoodPartsTotalQty",
    "N_ScrapPartsTotalQty",
    "S_DowntimeReasonOperator",
    "PB_Call_DieSetter","PB_Call_DieShop","PB_Call_Forklift",
    "PB_Call_Maintenance","PB_Call_Material","PB_Call_Quality","PB_Call_Supervisor",
    "B_JobInProgressForIgnition"
])

CALL_BUTTON_TAGS = set([
    "PB_Call_DieSetter","PB_Call_DieShop","PB_Call_Forklift",
    "PB_Call_Maintenance","PB_Call_Material","PB_Call_Quality","PB_Call_Supervisor"
])

call_button_states = {}
last_press_status = {}
station_ids_plan_downtime = []
last_tag_values = {}
downtime_state = {}
total_downtime_by_reason = {}
last_published_state = {}

# queues
message_queue = Queue(maxsize=MESSAGE_QUEUE_MAXSIZE)
publish_queue = Queue(maxsize=PUBLISH_QUEUE_MAXSIZE)

# DB & runtime
db_lock = Lock()
live = {}  # station -> { job_id, flags: {}, flag_start_ts: {}, job_start_counters: {} }

# tracked thread references for watchdog
worker_thread = None
publisher_thread = None
threads_lock = Lock()
last_worker_processed_ts = time.time()


# -------------------------
# UTIL: Time Conversion
# -------------------------

def now_central():
    return datetime.now(CENTRAL_TZ)

def now_iso():
    """ISO string in Central Time (for logs only)"""
    return now_central().isoformat()

def to_central(dt):
    """Convert any datetime (UTC or naive) to Central"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(CENTRAL_TZ)

def central_day_from_epoch(ts_epoch: int) -> str:
    """
    Convert epoch seconds → Central Time calendar day (YYYY-MM-DD)
    """
    return datetime.fromtimestamp(ts_epoch, CENTRAL_TZ).strftime('%Y-%m-%d')

# -------------------------
# UTIL: safe printing
# -------------------------

def safe_print(*args, **kwargs):
    print(f"{now_iso()} -", *args, **kwargs)


def is_station_allowed(station_id: str) -> bool:
    """
    Station must exist in config AND be Active
    """
    if not station_id:
        return False

    station_cfg = CONFIG.get("stations", {}).get(station_id)
    if not station_cfg:
        return False

    return str(station_cfg.get("status", "")).lower() == "active"


# -------------------------
# UTIL: Stations zone and status
# -------------------------

def publish_current_shift(station_id):
    """
    Publishes current shift for the station based on time & config only
    """
    if not is_station_active(station_id):
        return

    shift = get_shift_for_station(station_id)

    payload = json.dumps({
        "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station_id}/Shift",
        "propertyValues": [{
            "timestamp": {"timeInSeconds": int(time.time())},
            "quality": "GOOD",
            "value": {"stringValue": shift}
        }]
    })

    enqueue_publish(PUB_TOPIC, payload)
    safe_print(f"[SHIFT-PUBLISH] {station_id} Shift={shift}")

def is_station_active(station_id):
    st_cfg = CONFIG.get("stations", {}).get(station_id)
    if not st_cfg:
        return False
    return str(st_cfg.get("status", "Paused")).lower() == "active"



def publish_station_metadata(station_id):
    # zone = get_station_zone(station_id)
    status = CONFIG.get("stations", {}).get(station_id, {}).get("status", "Paused")

    ts = int(time.time())

    # ---- Publish STATUS ----
    payload_status = json.dumps({
        "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station_id}/Status",
        "propertyValues": [{
            "timestamp": {"timeInSeconds": ts},
            "quality": "GOOD",
            "value": {"stringValue": status}
        }]
    })
    enqueue_publish(PUB_TOPIC, payload_status)

    safe_print(f"[METADATA] {station_id} status={status}")

# -------------------------
# CONFIG FILE LOADER & VALIDATION
# -------------------------
def validate_config(cfg):
    # basic structural validation - extend as needed
    if not isinstance(cfg, dict):
        return False
    if "stations" in cfg and not isinstance(cfg["stations"], dict):
        return False
    # shifts and breaks optional - if present should be dict/list
    if "shifts" in cfg and not isinstance(cfg["shifts"], dict):
        return False
    if "breaks" in cfg and not isinstance(cfg["breaks"], list):
        return False
    return True

def load_config(path=CONFIG_PATH):
    global CONFIG, station_ids_plan_downtime, CONFIG_MTIME
    try:
        if os.path.exists(path):
            mtime = os.path.getmtime(path)
            if CONFIG_MTIME is None or mtime != CONFIG_MTIME:
                with open(path, "r") as f:
                    cfg = json.load(f)
                if validate_config(cfg):
                    CONFIG = cfg
                    station_ids_plan_downtime = list(CONFIG.get("stations", {}).keys())
                    CONFIG_MTIME = mtime
                    safe_print(f"[CONFIG] Loaded {len(CONFIG.get('stations',{}))} stations from {path}")

                    for station_id in CONFIG.get("stations", {}):
                        publish_station_metadata(station_id)
                        
                    # ----------------------------------------
                    # Station status transition handling
                    # ----------------------------------------
                    for station_id, station_cfg in CONFIG.get("stations", {}).items():
                        new_status = str(station_cfg.get("status", "Paused")).lower()
                        old_status = previous_station_status.get(station_id)

                        # -------- Active → Paused --------
                        if old_status == "active" and new_status == "paused":
                            safe_print(f"[STATUS] {station_id} Active → Paused")

                            shift = get_shift_for_station(station_id)
                            state = f"Planned Downtime:{shift}"

                            publish_machine_state(
                                station_id,
                                state,
                                by="status"
                            )

                        # -------- Paused → Active --------
                        elif old_status == "paused" and new_status == "active":
                            safe_print(f"[STATUS] {station_id} Paused → Active")

                            state = determine_machine_state(
                                station_id,
                                include_planned=True
                            )

                            publish_machine_state(
                                station_id,
                                state,
                                by="status"
                            )

                        previous_station_status[station_id] = new_status
                    

                else:
                    safe_print("[CONFIG] Validation failed - ignoring new config")
            
        else:
            safe_print(f"[CONFIG] No config at {path}; using existing or empty map")
    except Exception as e:
        safe_print("[CONFIG] Error loading config:", e)

def config_reloader():
    while not shutdown_event.is_set():
        try:
            load_config(CONFIG_PATH)
        except Exception as e:
            safe_print("[CONFIG] reload error:", e)
        shutdown_event.wait(timeout=10)



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

            print(f"\n[SHIFT-DEBUG] Checking shift {sname}: {start_t} → {end_t}")

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


            # OVERNIGHT SHIFT (crosses midnight, e.g., 18:00 → 06:00)
            else:
                print("[SHIFT-DEBUG] Overnight shift")

                # If current time < end → after midnight → shift started yesterday
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
                print(f"[SHIFT-DEBUG] MATCH → {sname}")
                return sname
            else:
                print("[SHIFT-DEBUG] No match.")

        # If no match, fall back to the first shift
        fallback_shift = list(group.keys())[0]
        # print(f"[SHIFT-DEBUG] No shift matched → fallback: {fallback_shift}")
        return fallback_shift

    except Exception as e:
        print("[SHIFT] ERROR:", e)
        return "A"


def get_breaks_for_station(station_id):
    """
    Returns list of breaks for the station's current shift.
    """
    station_cfg = CONFIG.get("stations", {}).get(station_id)
    if not station_cfg:
        return []

    shift_group = station_cfg.get("shifts")
    if not shift_group:
        return []

    current_shift = get_shift_for_station(station_id)
    shifts_cfg = CONFIG.get("shifts", {}).get(shift_group, {})

    shift_cfg = shifts_cfg.get(current_shift, {})
    return shift_cfg.get("breaks", [])

def is_break_time_for_station(station_id):
    now = now_central()
    break_list = get_breaks_for_station(station_id)

    if not break_list:
        return False
    for br in break_list:
        start_str = br.get("start"); end_str = br.get("end")
        if not start_str or not end_str:
            continue
        start_t = parse_hhmm(start_str); end_t = parse_hhmm(end_str)
        if not start_t or not end_t:
            continue

        s = datetime.combine(
            now.date(),
            start_t,
            tzinfo=CENTRAL_TZ
        )

        e = datetime.combine(
            now.date(),
            end_t,
            tzinfo=CENTRAL_TZ
        )

        if end_t < start_t:
            e = e + timedelta(days=1)
        if s <= now <= e:
            return True
    return False

# -------------------------
# PUBLISHER (retries + backoff)
# -------------------------
def publisher_thread_fn():
    """
    Publisher thread consumes publish_queue items of form:
    {"topic":..., "payload":..., "qos":0, "retain":False, "attempts":0, "next_try":ts}
    It will attempt publish immediately if next_try <= now, otherwise requeue.
    On failure, increment attempts and reschedule with exponential backoff.
    """
    safe_print("[PUBLISHER] started")
    buffer = []
    while not shutdown_event.is_set():
        try:
            item = None
            # prefer buffered retries (FIFO)
            if buffer:
                item = buffer.pop(0)
                # if next_try not reached push back and sleep small amount
                if item.get("next_try", 0) > time.time():
                    buffer.insert(0, item)
                    shutdown_event.wait(timeout=0.5)
                    continue
            else:
                try:
                    item = publish_queue.get(timeout=0.5)
                except Empty:
                    continue

            if item is None:
                break

            topic = item.get("topic"); payload = item.get("payload")
            qos = item.get("qos", 0); retain = item.get("retain", False)
            attempts = item.get("attempts", 0)

            try:
                # attempt publish
                mqtt_client_instance.publish(topic, payload, qos=qos, retain=retain)
                safe_print(f"[PUBLISHER] published topic={topic}")
            except Exception as e:
                attempts += 1
                if attempts >= PUBLISH_RETRY_MAX_ATTEMPTS:
                    safe_print(f"[PUBLISHER] dropping after attempts={attempts} topic={topic} err={e}")
                else:
                    # schedule retry
                    delay = (2 ** (attempts - 1)) * PUBLISH_RETRY_BASE_DELAY
                    item["attempts"] = attempts
                    item["next_try"] = time.time() + delay
                    buffer.append(item)
                    safe_print(f"[PUBLISHER] publish failed, scheduling retry#{attempts} in {delay:.1f}s for topic={topic}")
        except Exception as e:
            safe_print("[PUBLISHER] error:", e)
    safe_print("[PUBLISHER] exiting")

def enqueue_publish(topic, payload, qos=0, retain=False):
    entry = {"topic": topic, "payload": payload, "qos": qos, "retain": retain, "attempts": 0, "next_try": 0}
    try:
        publish_queue.put(entry, block=False)
    except Full:
        # if publish queue full, drop oldest and insert new (simple strategy)
        try:
            _ = publish_queue.get_nowait()
            publish_queue.put(entry, block=False)
            safe_print("[PUBLISH] publish_queue full: dropped oldest entry to enqueue new")
        except Exception:
            safe_print("[PUBLISH] publish_queue full: dropping publish for topic", topic)

# -------------------------
# PUBLISH HELPERS (wraps enqueue)
# -------------------------
def publish_message(entry):
    """
    Use enqueue_publish rather than mqtt publish directly.
    entry: {"topic":..., "payload":..., "qos":..., "retain":...}
    """
    try:
        enqueue_publish(entry["topic"], entry["payload"], qos=entry.get("qos", 0), retain=entry.get("retain", False))
        safe_print(f"[ENQUEUE-PUBLISH] topic={entry['topic']}")
    except Exception as e:
        safe_print("[ENQUEUE-PUBLISH] error:", e)

def publish_machine_state(station_id, state, by='main'):

    # if not is_station_active(station_id):
    #     return
    station_cfg = CONFIG.get("stations", {}).get(station_id, {})
    status = str(station_cfg.get("status", "Paused")).lower()

    # If station is paused, allow ONLY Idle or Planned Downtime
    if status == "paused":
        if not (state.startswith("PlannedDowntime")):
            safe_print(f"[STATE-BLOCK] {station_id} paused → blocked state {state}")
            return

    if by == 'main':
        last_state = last_published_state.get(station_id, {}).get('state', "")
        if last_state.startswith("Planned"):
            safe_print(f"[STATE-BLOCK] {station_id} is in PlannedDowntime; main skip.")
            return
    if state == "PlannedDowntime" and by != 'break':
        return
    if state == "PlannedDowntime":
        shift = get_shift_for_station(station_id=station_id)
        state = f"Planned Downtime:{shift}"
    payload = json.dumps({
        "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station_id}/MachineStateAndShift",
        "propertyValues": [{
            "timestamp": {"timeInSeconds": int(time.time())},
            "quality":"GOOD",
            "value":{"stringValue": state}
        }]
    })
    try:
        enqueue_publish(PUB_TOPIC, payload)

        last_published_state[station_id] = {"state": state, "by": by, "ts": time.time()}
        safe_print(f"[STATE-PUBLISH] [{by}] {station_id} -> {state}")
    except Exception as e:
        safe_print("[STATE] publish enqueue error:", e)

# -------------------------
# SQLITE DB init & helpers (with integrity check)
# -------------------------
def init_db():
    # ensure backup dir exists
    try:
        os.makedirs(DB_BACKUP_DIR, exist_ok=True)
    except Exception:
        pass

    # If DB exists, run integrity check
    if os.path.exists(DB_PATH):
        try:
            conn = sqlite3.connect(DB_PATH, timeout=30)
            cur = conn.cursor()
            cur.execute("PRAGMA integrity_check;")
            res = cur.fetchone()
            conn.close()
            if res and res[0].upper() != "OK":
                # backup corrupted DB
                ts = now_central().strftime("%Y%m%dT%H%M%SZ")
                backup_name = os.path.join(DB_BACKUP_DIR, f"stations_data_corrupt_{ts}.db")
                shutil.copy2(DB_PATH, backup_name)
                safe_print(f"[DB] Integrity check failed. Backed up DB to {backup_name} and will recreate.")
                # rename original and allow recreation
                os.rename(DB_PATH, DB_PATH + ".corrupt."+ts)
        except Exception as e:
            safe_print("[DB] Integrity check failed with exception:", e)
            # try to move corrupted DB
            try:
                ts = now_central().strftime("%Y%m%dT%H%M%SZ")
                
                os.rename(DB_PATH, DB_PATH + ".corrupt."+ts)
            except Exception:
                pass

    # create DB & tables
    with db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.executescript("""
        CREATE TABLE IF NOT EXISTS jobs (
          job_id INTEGER PRIMARY KEY AUTOINCREMENT,
          station_id TEXT NOT NULL,
          start_ts INTEGER NOT NULL,
          end_ts INTEGER,
          operator_sec REAL DEFAULT 0,
          diesetter_sec REAL DEFAULT 0,
          downtime_sec REAL DEFAULT 0,
          planned_run_mins REAL DEFAULT 0,
          good_parts INTEGER DEFAULT 0,
          scrap_parts INTEGER DEFAULT 0,
          OA REAL,
          QR REAL,
          finalized INTEGER DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS daily_agg (
          day TEXT NOT NULL,
          station_id TEXT NOT NULL,
          operator_sec REAL DEFAULT 0,
          diesetter_sec REAL DEFAULT 0,
          downtime_sec REAL DEFAULT 0,
          planned_run_mins REAL DEFAULT 0,
          good_parts INTEGER DEFAULT 0,
          scrap_parts INTEGER DEFAULT 0,
          oa_sum REAL DEFAULT 0,
          oa_count INTEGER DEFAULT 0,
          qr_sum REAL DEFAULT 0,
          qr_count INTEGER DEFAULT 0,
          OR_scaled REAL,
          OEE REAL,
          PRIMARY KEY(day, station_id)
        );
        """)
        conn.commit(); conn.close()
    safe_print("[DB] Initialized at " + DB_PATH)



# -------------------------
# RUNTIME STATE (DB)
# -------------------------
def init_runtime_state_db():
    with db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("""
        CREATE TABLE IF NOT EXISTS runtime_state (
            station_id TEXT NOT NULL,
            category   TEXT NOT NULL,
            key        TEXT NOT NULL,
            value      TEXT NOT NULL,
            updated_ts INTEGER NOT NULL,
            PRIMARY KEY (station_id, category, key)
        );
        """)
        conn.commit()
        conn.close()


def save_runtime_state(station_id, category, key, value):
    with db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO runtime_state(station_id, category, key, value, updated_ts)
            VALUES (?,?,?,?,?)
            ON CONFLICT(station_id, category, key)
            DO UPDATE SET value=excluded.value, updated_ts=excluded.updated_ts
        """, (station_id, category, key, json.dumps(value), int(time.time())))
        conn.commit()
        conn.close()


def delete_runtime_state(station_id, category, key):
    with db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM runtime_state
            WHERE station_id=? AND category=? AND key=?
        """, (station_id, category, key))
        conn.commit()
        conn.close()


def load_runtime_state():
    data = {}
    with db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cur = conn.cursor()
        cur.execute("SELECT station_id, category, key, value FROM runtime_state")
        for sid, cat, key, val in cur.fetchall():
            try:
                v = json.loads(val)
            except Exception:
                continue
            data.setdefault(sid, {}).setdefault(cat, {})[key] = v
        conn.close()
    return data

# -------------------------
# JOB helpers (unchanged logic)
# -------------------------
def start_job(station_id, ts_epoch):
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("INSERT INTO jobs(station_id, start_ts) VALUES (?, ?)", (station_id, int(ts_epoch)))
        job_id = cur.lastrowid
        # day = datetime.utcfromtimestamp(ts_epoch).strftime('%Y-%m-%d')
        day = central_day_from_epoch(ts_epoch)

        cur.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (day, station_id))
        conn.commit(); conn.close()
    live.setdefault(station_id, {})['job_id'] = job_id
    # set flags start times if flags already true
    st = live.setdefault(station_id, {}); flags = st.setdefault('flags', {}); starts = st.setdefault('flag_start_ts', {})
    for tag in ("B_OperatorInProgressForIgnition","B_DieSetterInProgressForIgnition","B_DowntimeInProgressForIgnition"):
        if flags.get(tag):
            starts[tag] = ts_epoch
    # record job start counters (kept but not used for delta approach)
    st['job_start_counters'] = {
        'N_GoodPartsTotalQty': last_tag_values.get(station_id, {}).get('N_GoodPartsTotalQty'),
        'N_ScrapPartsTotalQty': last_tag_values.get(station_id, {}).get('N_ScrapPartsTotalQty')
    }
    safe_print(f"[JOB] Started job_id={job_id} for {station_id} at {ts_epoch}")
    return job_id

def finalize_job_and_publish(job_id):
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT station_id, operator_sec, diesetter_sec, downtime_sec, planned_run_mins, good_parts, scrap_parts FROM jobs WHERE job_id=?", (job_id,))
        row = cur.fetchone()
        if not row:
            conn.close(); return
        station_id, operator_sec, diesetter_sec, downtime_sec, planned_run_mins, good_parts, scrap_parts = row

        RT = (operator_sec or 0.0) / 60.0
        CT = (diesetter_sec or 0.0) / 60.0
        denom = (RT + CT)
        OA_job = (planned_run_mins / denom) if denom > 0 else 0.0

        total_parts = (good_parts or 0) + (scrap_parts or 0)
        QR_job = (good_parts / total_parts) if total_parts > 0 else 0.0

        cur.execute("UPDATE jobs SET OA=?, QR=?, finalized=1 WHERE job_id=?", (OA_job, QR_job, job_id))
        # day = datetime.utcfromtimestamp(int(time.time())).strftime('%Y-%m-%d')
        day = central_day_from_epoch(int(time.time()))

        cur.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (day, station_id))
        cur.execute("""
            UPDATE daily_agg
            SET 
                oa_sum = oa_sum + ?,
                oa_count = oa_count + 1,
                qr_sum = qr_sum + ?,
                qr_count = qr_count + 1
            WHERE day=? AND station_id=?
        """, (OA_job, QR_job, day, station_id))
        conn.commit(); conn.close()

    # publish OA & QR immediately (enqueue)
    shift = get_shift_for_station(station_id=station_id)
    oa_data = f"{float(OA_job)}:{shift}"
    qr_data = f"{float(QR_job)}:{shift}"
    payload_oa = json.dumps({
        "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station_id}/PerformancePerJob",
        "propertyValues": [{"timestamp":{"timeInSeconds":int(time.time())}, "quality":"GOOD", "value":{"stringValue":oa_data}}]
    })
    payload_qr = json.dumps({
        "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station_id}/QualityPerJob",
        "propertyValues": [{"timestamp":{"timeInSeconds":int(time.time())}, "quality":"GOOD", "value":{"stringValue":qr_data}}]
    })
    enqueue_publish(PUB_TOPIC, payload_oa)
    enqueue_publish(PUB_TOPIC, payload_qr)
    safe_print(f"[PUBLISH-OA_JOB] {station_id} OA={OA_job}")
    safe_print(f"[PUBLISH-QR_JOB] {station_id} QR={QR_job}")

def end_job(station_id, ts_epoch):
    job_id = live.get(station_id, {}).get('job_id')
    if not job_id:
        safe_print(f"[JOB] No active job to end for {station_id}")
        return
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("UPDATE jobs SET end_ts=?, finalized=1 WHERE job_id=?", (int(ts_epoch), job_id))
        conn.commit(); conn.close()
    live[station_id]['job_id'] = None
    safe_print(f"[JOB] Ended job_id={job_id} for {station_id} at {ts_epoch}")
    finalize_job_and_publish(job_id)

def add_seconds_to_job(station_id, col, seconds, ts_epoch):
    job_id = live.get(station_id, {}).get('job_id')
    # day = datetime.utcfromtimestamp(ts_epoch).strftime('%Y-%m-%d')
    day = central_day_from_epoch(ts_epoch)

    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        if job_id:
            if col in ('operator_sec', 'diesetter_sec', 'downtime_sec'):
                cur.execute(f"UPDATE jobs SET {col} = {col} + ? WHERE job_id = ?", (float(seconds), job_id))
        cur.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (day, station_id))
        if col in ('operator_sec', 'diesetter_sec', 'downtime_sec'):
            cur.execute(f"UPDATE daily_agg SET {col} = {col} + ? WHERE day=? AND station_id=?", (float(seconds), day, station_id))
        conn.commit(); conn.close()
    safe_print(f"[DB] +{seconds:.2f}s to {col} for {station_id} (job_id={job_id})")

def add_planned_run_mins(station_id, mins, ts_epoch):
    job_id = live.get(station_id, {}).get('job_id')
    # day = datetime.utcfromtimestamp(ts_epoch).strftime('%Y-%m-%d')
    day = central_day_from_epoch(ts_epoch)

    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        if job_id:
            cur.execute("UPDATE jobs SET planned_run_mins = ? WHERE job_id=?", (float(mins), job_id))
        cur.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (day, station_id))
        cur.execute("UPDATE daily_agg SET planned_run_mins =? WHERE day=? AND station_id=?", (float(mins), day, station_id))
        conn.commit(); conn.close()
    safe_print(f"[DB] +{mins}min planned_run for {station_id} (job_id={job_id})")

# -------------------------
# FLAGS & part-counter processing
# -------------------------
def process_boolean_flag(station_id, tag, raw_val, ts_epoch):
    val = str(raw_val).lower() in ("true","1","yes","y")
    st = live.setdefault(station_id, {})
    flags = st.setdefault('flags', {})
    starts = st.setdefault('flag_start_ts', {})
    prev = flags.get(tag, False)
    if prev == val:
        return
    if val:
        flags[tag] = True
        starts[tag] = ts_epoch
        safe_print(f"[FLAG] {station_id} {tag} TRUE at {ts_epoch}")
        return
    # falling edge
    flags[tag] = False
    start_ts = starts.get(tag)
    if not start_ts:
        safe_print(f"[FLAG] {station_id} {tag} FALLING no start_ts")
        return
    duration = ts_epoch - start_ts
    if tag == "B_OperatorInProgressForIgnition":
        add_seconds_to_job(station_id, 'operator_sec', duration, ts_epoch)
    elif tag == "B_DieSetterInProgressForIgnition":
        add_seconds_to_job(station_id, 'diesetter_sec', duration, ts_epoch)
    elif tag == "B_DowntimeInProgressForIgnition":
        add_seconds_to_job(station_id, 'downtime_sec', duration, ts_epoch)
    starts[tag] = None
    safe_print(f"[FLAG] {station_id} {tag} FALSE at {ts_epoch} dur={duration:.2f}s")

def process_job_flag(station_id, raw_val, ts_epoch):
    val = str(raw_val).lower() in ("true","1","yes","y")
    st = live.setdefault(station_id, {})
    current_job_id = st.get('job_id')
    if val and not current_job_id:
        start_job(station_id, ts_epoch)
        return
    if not val and current_job_id:
        # capture in-flight flags up to now
        for tag in ("B_OperatorInProgressForIgnition","B_DieSetterInProgressForIgnition","B_DowntimeInProgressForIgnition"):
            if st.get('flags',{}).get(tag):
                start_ts = st['flag_start_ts'].get(tag)
                if start_ts:
                    duration = ts_epoch - start_ts
                    if tag == "B_OperatorInProgressForIgnition":
                        add_seconds_to_job(station_id, 'operator_sec', duration, ts_epoch)
                    elif tag == "B_DieSetterInProgressForIgnition":
                        add_seconds_to_job(station_id, 'diesetter_sec', duration, ts_epoch)
                    elif tag == "B_DowntimeInProgressForIgnition":
                        add_seconds_to_job(station_id, 'downtime_sec', duration, ts_epoch)
                st['flag_start_ts'][tag] = None
        end_job(station_id, ts_epoch)
        return

def process_part_counter(station_id, tag, value, ts_epoch):
    """
    Final: NO DELTA logic. Store latest absolute values in job and daily_agg.
    """
    try:
        cur_val = int(value)
    except:
        try:
            cur_val = int(float(value))
        except:
            safe_print(f"[PART] Invalid {value} for {tag}")
            return

    st = live.setdefault(station_id, {})
    job_id = st.get('job_id')
    # day = datetime.utcfromtimestamp(ts_epoch).strftime('%Y-%m-%d')
    day = central_day_from_epoch(ts_epoch)


    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        if job_id:
            if tag == "N_GoodPartsTotalQty":
                cur.execute("UPDATE jobs SET good_parts = ? WHERE job_id = ?", (cur_val, job_id))
            elif tag == "N_ScrapPartsTotalQty":
                cur.execute("UPDATE jobs SET scrap_parts = ? WHERE job_id = ?", (cur_val, job_id))

        cur.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (day, station_id))
        if tag == "N_GoodPartsTotalQty":
            cur.execute("UPDATE daily_agg SET good_parts = ? WHERE day=? AND station_id=?", (cur_val, day, station_id))
        elif tag == "N_ScrapPartsTotalQty":
            cur.execute("UPDATE daily_agg SET scrap_parts = ? WHERE day=? AND station_id=?", (cur_val, day, station_id))

        conn.commit(); conn.close()

    safe_print(f"[PART] {station_id} {tag} latest value stored = {cur_val}")

# -------------------------
# DOWNTIME PUBLISH (unchanged)
# -------------------------
def process_downtime_flag(station_id, flag_value, ts):
    global downtime_state, total_downtime_by_reason
    flag = str(flag_value).lower() == "true"
    state = downtime_state.get(station_id, {"previous_flag": False, "start_time": None, "reason": None})
    reason = last_tag_values.get(station_id, {}).get("S_DowntimeReasonOperator", "Unknown")
    shift = get_shift_for_station(station_id)
    if flag and not state["previous_flag"]:
        state["start_time"] = ts
        state["reason"] = reason
        save_runtime_state(
                station_id,
                "downtime",
                "active",
                {
                    "start_ts": ts.timestamp(),
                    "reason": reason
                }
            )
        safe_print(f"[DOWNTIME START] {station_id} reason={reason} at {ts}")
    elif not flag and state["previous_flag"] and state["start_time"] is not None:
        duration = (ts - state["start_time"]).total_seconds()
        r = state["reason"] or "Unknown"
        safe_print(f"[DOWNTIME END] {station_id} reason={r} dur={duration:.2f}s at {ts}")
        total_downtime_by_reason[r] = total_downtime_by_reason.get(r,0.0) + duration
        event_payload = json.dumps({
            "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station_id}/DowntimeReasons",
            "propertyValues": [{
                "timestamp":{"timeInSeconds":int(ts.timestamp()), "offsetInNanos":0},
                "quality":"GOOD",
                "value":{"stringValue": str({"reason": r, "duration_sec": duration, "Shift": shift})}
            }]
        })
        enqueue_publish(PUB_TOPIC, event_payload)
        delete_runtime_state(station_id, "downtime", "active")
        state["start_time"] = None
        state["reason"] = None
    state["previous_flag"] = flag
    downtime_state[station_id] = state

# -------------------------
# MACHINE STATE ENGINE
# -------------------------
def determine_machine_state(station_id, include_planned=True):
    tags = last_tag_values.get(station_id, {})
    shift = get_shift_for_station(station_id=station_id)
    # if include_planned and is_break_time():
    if include_planned and is_break_time_for_station(station_id=station_id):
        return "PlannedDowntime"
    elif str(tags.get("B_DowntimeInProgressForIgnition", "false")).lower() == "true":
        return f"Unplanned Downtime:{shift}"
    elif str(tags.get("B_DieSetterInProgressForIgnition", "false")).lower() == "true":
        return f"Changeover:{shift}"
    elif str(tags.get("B_OperatorInProgressForIgnition", "false")).lower() == "true":
        try:
            spm = float(tags.get("N_DieSPM", 0) or 0)
        except:
            spm = 0.0
        try:
            target = float(tags.get("PartsPerMinute", 0) or 0)
        except:
            target = 0.0
        if spm >= target:
            return f"Run to rate:{shift}"
        else:
            return f"Run not to rate:{shift}"
    return f"Idle:{shift}"

# -------------------------
# TRANSFORM & CALL BUTTONS
# -------------------------
def publish_press_status(station_id, tag_value):
    shift = get_shift_for_station(station_id)
    tval = str(tag_value).replace("PB_Call_", "")
    formatted = f"{tval}:{shift}"
    payload = json.dumps({
        "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station_id}/CallButton",
        "propertyValues":[{"timestamp":{"timeInSeconds":int(time.time())},"quality":"GOOD","value":{"stringValue":formatted}}]
    })
    enqueue_publish(PUB_TOPIC, payload)
    last_press_status[station_id] = formatted
    save_runtime_state(
            station_id,
            "call_button",
            "last_active",
            {
                "button": formatted,
                "ts": time.time()
            }
        )
    safe_print(f"[CALL] {station_id} -> {formatted}")

def transform_payload(payload):
    station = payload.get("stationID"); tag = payload.get("tagName"); val = payload.get("details")
    shift = get_shift_for_station(station) if station else "A"
    out = val
    try:
        if not isinstance(val, (int,float)):
            s = str(val)
            out = float(s) if s.replace(".", "", 1).isdigit() else s
    except:
        out = val
    entry = {
        "topic": PUB_TOPIC,
        "payload": json.dumps({
            "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station}/{tag}",
            "propertyValues":[{"timestamp":{"timeInSeconds":int(time.time())},"quality":"GOOD","value":{"stringValue": json.dumps({tag: out, "Shift": shift})}}]
        })
    }
    
    safe_print(f"[TRANSFORM] {entry['payload']}")
    return entry

# -------------------------
# INPUT VALIDATION
# -------------------------
def validate_incoming_payload(payload):
    if not isinstance(payload, dict):
        return False, "payload not dict"
    # required fields
    if "stationID" not in payload or not payload["stationID"]:
        return False, "missing stationID"
    if "tagName" not in payload or not payload["tagName"]:
        return False, "missing tagName"
    # details can be any type but must exist
    if "details" not in payload:
        return False, "missing details"
    # timestamp optional - if present should be numeric
    if "timestamp" in payload:
        try:
            float(payload["timestamp"])
        except Exception:
            return False, "invalid timestamp"
    return True, None

# -------------------------
# MESSAGE HANDLER
# -------------------------
def handle_message(payload):
    global last_worker_processed_ts
    valid, err = validate_incoming_payload(payload)
    if not valid:
        safe_print("[HANDLE] invalid payload:", err, payload)
        return
    
    tag = payload.get("tagName"); station = payload.get("stationID"); val = payload.get("details")
    ts_raw = payload.get("timestamp", time.time())

    station = payload.get("stationID")

    # Ignore unknown or inactive stations
    if station not in CONFIG.get("stations", {}):
        safe_print(f"[FILTER] Ignoring {station} (not in config)")
        return

    if not is_station_active(station):
        safe_print(f"[FILTER] Ignoring {station} (status inactive)")
        return

    try:
        # ts = datetime.fromtimestamp(float(ts_raw), timezone.utc)
        ts = to_central(datetime.fromtimestamp(float(ts_raw), timezone.utc))

    except Exception:
        # ts = datetime.now(timezone.utc)
        ts = now_central()

    ts_epoch = int(ts.timestamp())

    safe_print(f"[HANDLE] station={station} tag={tag} val={val}")

    # update last_tag_values for state & downtime reason extraction
    #  # ensure station exists in planned downtime list
    if station not in station_ids_plan_downtime:
        station_ids_plan_downtime.append(station)
        safe_print(f"[CONFIG] Warning: station {station} not in config; added.")    

    last_tag_values.setdefault(station, {})["timestamp"] = ts
    
    tags = last_tag_values.setdefault(station, {})

    job_flag = str(tags.get("B_JobInProgressForIgnition", "false")).lower() == "true"
    op_flag  = str(tags.get("B_OperatorInProgressForIgnition", "false")).lower() == "true"
    dt_flag  = str(tags.get("B_DowntimeInProgressForIgnition", "false")).lower() == "true"
    ds_flag  = str(tags.get("B_DieSetterInProgressForIgnition", "false")).lower() == "true"

    # Downtime active without Job
    if dt_flag and not job_flag:
        safe_print(f"[ANOMALY] {station} Downtime active while Job=false")

    # Operator active without Job
    if op_flag and not job_flag:
        safe_print(f"[ANOMALY] {station} Operator active while Job=false")

    # DieSetter active during Job (unexpected per client)
    if ds_flag and job_flag:
        safe_print(f"[ANOMALY] {station} DieSetter active during Job means Job=True")

    if tag in REQUIRED_TAGS:
            
            # Normalize and convert numbers safely
            sval = str(val).strip()

            # Try to convert numeric-looking values into float
            try:
                # handle ints & floats such as "12", "12.5", " 12.50 "
                num = float(sval)
                last_tag_values.setdefault(station, {})[tag] = num
                if tag in MACHINE_STATE_TAGS:
                    save_runtime_state(
                    station,
                        "state_inputs",
                        tag,
                        {
                            "value": num,
                            "ts": ts_epoch
                        }
                    )
            except ValueError:
                # not a number → store raw
                last_tag_values.setdefault(station, {})[tag] = val
                if tag in MACHINE_STATE_TAGS:
                    save_runtime_state(
                    station,
                        "state_inputs",
                        tag,
                        {
                            "value": val,
                            "ts": ts_epoch
                        }
                    )


    # handle call buttons
    if tag in CALL_BUTTON_TAGS:
        call_button_states.setdefault(station, {k: "false" for k in CALL_BUTTON_TAGS})
        call_button_states[station][tag] = str(val).lower()
        active = [k for k, v in call_button_states[station].items() if v == "true"]
        publish_tag = active[-1] if active else "None"
        publish_press_status(station, publish_tag)

    # downtime flag events
    if tag == "B_DowntimeInProgressForIgnition":
        process_downtime_flag(station, val, ts)

    # machine state publishes
    if tag in MACHINE_STATE_TAGS:
        new_state = determine_machine_state(station, include_planned=False)
        safe_print(f"[STATE] main publish {station} -> {new_state}")
        publish_machine_state(station, new_state, by='main')

    # OEE/job processing
    if tag == "B_JobInProgressForIgnition":
        process_job_flag(station, val, ts_epoch)
    elif tag in ("B_OperatorInProgressForIgnition","B_DieSetterInProgressForIgnition","B_DowntimeInProgressForIgnition"):
        process_boolean_flag(station, tag, val, ts_epoch)
    elif tag == "N_OperationPlannedRunMins":
        try:
            mins = float(val); 
            add_planned_run_mins(station, mins, ts_epoch)
        except Exception as e:
            safe_print("[OEE] invalid planned mins", e)
    elif tag in ("N_GoodPartsTotalQty","N_ScrapPartsTotalQty"):
        process_part_counter(station, tag, val, ts_epoch)

    # At end mark last processed timestamp for watchdog
    last_worker_processed_ts = time.time()

# -------------------------
# WORKER THREAD + WATCHDOG
# -------------------------
def worker_fn():
    safe_print("[WORKER] started")
    while not shutdown_event.is_set():
        try:
            payload = message_queue.get(timeout=1.0)
        except Empty:
            continue
        if payload is None:
            break
        try:
            handle_message(payload)
            entry = transform_payload(payload)
            publish_message(entry)
        except Exception as e:
            safe_print("[WORKER] error handling message:", e)
        finally:
            try:
                message_queue.task_done()
            except Exception:
                pass
    safe_print("[WORKER] exiting")

def worker_watchdog_fn():
    global worker_thread
    safe_print("[WATCHDOG] started")
    while not shutdown_event.is_set():
        shutdown_event.wait(timeout=WORKER_WATCHDOG_INTERVAL)
        # check worker thread alive
        with threads_lock:
            wt = worker_thread
        
        if wt is None:
            continue
        if not wt.is_alive():
            safe_print("[WATCHDOG] worker died → restarting")
            t = Thread(target=worker_fn, daemon=True)
            with threads_lock:
                worker_thread = t
            t.start()
    safe_print("[WATCHDOG] exiting")

# -------------------------
# BREAK MONITOR (unchanged mostly)
# -------------------------
def break_monitor():
    global mqtt_connected
    safe_print("[BREAK] monitor waiting for MQTT...")

    while not mqtt_connected and not shutdown_event.is_set():
        shutdown_event.wait(timeout=1)

    safe_print("[BREAK] monitor active")

    last_break_state = {}  # station_id -> bool

    while not shutdown_event.is_set():
        try:
            for sid in list(station_ids_plan_downtime):
                current = is_break_time_for_station(sid)
                previous = last_break_state.get(sid)

                if previous is None:
                    last_break_state[sid] = current
                    
                    safe_print(f"[BREAK] {sid} transition {previous} → {current}")
                    # -----------------------------------
                    # FORCE INITIAL STATE AFTER RESTART
                    # -----------------------------------
                    if current:
                        publish_machine_state(sid, "PlannedDowntime", by="break")
                    else:
                        new_state = determine_machine_state(sid, include_planned=False)
                        publish_machine_state(sid, new_state, by="break")
                    continue

                if current != previous:
                    safe_print(f"[BREAK] {sid} transition {previous} → {current}")

                    if current:
                        publish_machine_state(sid, "PlannedDowntime", by="break")
                    else:
                        new_state = determine_machine_state(sid, include_planned=False)
                        publish_machine_state(sid, new_state, by="break")

                    last_break_state[sid] = current

        except Exception as e:
            safe_print("[BREAK] Error:", e)

        shutdown_event.wait(timeout=60)

    safe_print("[BREAK] exiting")

# -------------------------
# MQTT callbacks & safe enqueue
# -------------------------
def on_connect(client, userdata, flags, rc):
    global mqtt_connected
    if rc == 0:
        mqtt_connected = True
        try:
            client.subscribe(SUB_TOPIC)
            safe_print(f"[MQTT] Connected & subscribed {SUB_TOPIC}")
        except Exception as e:
            safe_print("[MQTT] subscribe failed:", e)
    else:
        safe_print("[MQTT] Connect failed rc=", rc)

def on_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False
    safe_print("[MQTT] disconnected rc=", rc)

def on_message(client, userdata, msg):
    # try decode & enqueue; drop if queue full
    try:
        payload = json.loads(msg.payload.decode())
    except Exception as e:
        safe_print("[MQTT] on_message decode error:", e)
        return
    
    station = payload.get("stationID")

    if not is_station_allowed(station):
        safe_print(f"[MQTT] Dropping message for paused/unknown station: {station}")
        return
    try:
        message_queue.put(payload, timeout=0.5)
    except Full:
        safe_print("[MQTT] message_queue full - dropping incoming message")

# -------------------------
# DAILY compute first shift start time CST  and cleanup
# -------------------------

def get_daily_compute_time():
    """
    Returns (hour, minute) for daily compute based on
    A-shift start time from 2shifts config.
    Fallback: 06:00
    """
    try:
        shifts = CONFIG.get("shifts", {})
        two_shifts = shifts.get("2shifts", {})
        a_shift = two_shifts.get("A", {})
        start = a_shift.get("start")  # "HH:MM"

        if start:
            h, m = start.split(":")
            return int(h), int(m)

    except Exception as e:
        safe_print("[DAILY] Failed reading shift config, fallback to 06:00", e)

    return 6, 0

def compute_daily_and_publish():
    safe_print("[DAILY] thread started")
    while not shutdown_event.is_set():

        now = now_central()
        hour, minute = get_daily_compute_time()

        safe_print(f"[DAILY] Calculation of OEE, Utilization, Performance and Quality Happens at every {hour}:{minute}  ")
        # Schedule daily compute at 6:00 AM Central
        next_run = datetime.combine(
            now.date(),
            tm(hour,minute),
            tzinfo=CENTRAL_TZ
        )

        # If already past 6 AM today, run tomorrow
        if now >= next_run:
            next_run += timedelta(days=1)

        safe_print(f"[DAILY] Next run scheduled at {next_run.isoformat()}")

        sleep_seconds = (next_run - now).total_seconds()
        shutdown_event.wait(timeout=sleep_seconds)

        if shutdown_event.is_set():
            break

        # Compute for operational day (previous calendar day)
        # day = (next_run - timedelta(days=1)).strftime('%Y-%m-%d')
        day = (next_run - timedelta(days=1)).astimezone(CENTRAL_TZ).strftime('%Y-%m-%d')
        try:
            with db_lock:
                conn = sqlite3.connect(DB_PATH)
                cur = conn.cursor()
                cur.execute("""
                    SELECT station_id, operator_sec, diesetter_sec, downtime_sec, planned_run_mins, good_parts, scrap_parts,
                           oa_sum, oa_count, qr_sum, qr_count
                    FROM daily_agg WHERE day=?""", (day,))
                rows = cur.fetchall()
                for (station_id, operator_sec, diesetter_sec, downtime_sec, planned_run_mins, good_parts, scrap_parts,
                     oa_sum, oa_count, qr_sum, qr_count) in rows:
                    avg_OA = (oa_sum / oa_count) if (oa_count and oa_count > 0) else 0.0
                    avg_QR = (qr_sum / qr_count) if (qr_count and qr_count > 0) else 0.0
                    if avg_OA == 0.0:
                        denom_minutes = (operator_sec + diesetter_sec) / 60.0 if (operator_sec + diesetter_sec) > 0 else 0.0
                        avg_OA = (planned_run_mins / denom_minutes) if denom_minutes > 0 else 0.0
                    if avg_QR == 0.0:
                        avg_QR = (good_parts / (good_parts + scrap_parts)) if (good_parts + scrap_parts) > 0 else 0.0

                    RT = (operator_sec or 0.0) / 60.0
                    DT = (downtime_sec or 0.0) / 60.0
                    RT_DT = (RT - DT) / 60
                    OR_daily = (RT - DT) / 1270.0
                    OEE_daily = avg_OA * avg_QR * OR_daily

                    cur.execute("UPDATE daily_agg SET OR_scaled=?, OEE=? WHERE day=? AND station_id=?", (OR_daily, OEE_daily, day, station_id))

                    # publish OA & QR immediately (enqueue)
                    shift = get_shift_for_station(station_id=station_id)
                    OR_daily = f"{float(OR_daily)}:{shift}"
                    OEE_daily = f"{float(OEE_daily)}:{shift}"

                    payload_or = json.dumps({
                        "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station_id}/UtilizationPerDay",
                        "propertyValues":[{"timestamp":{"timeInSeconds":int(time.time())},"quality":"GOOD","value":{"stringValue":OR_daily}}]
                    })
                    payload_oee = json.dumps({
                        "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station_id}/OEEPerDay",
                        "propertyValues":[{"timestamp":{"timeInSeconds":int(time.time())},"quality":"GOOD","value":{"stringValue":OEE_daily}}]
                    })

                    enqueue_publish(PUB_TOPIC, payload_or)
                    enqueue_publish(PUB_TOPIC, payload_oee)
                    safe_print(f"[PUBLISH-DAILY] {station_id} OR={OR_daily} OEE={OEE_daily}")
                conn.commit()
                # cleanup old jobs
                cur.execute("DELETE FROM jobs WHERE end_ts IS NOT NULL AND end_ts < strftime('%s', 'now', '-30 days')")
                conn.commit(); conn.close()
        except Exception as e:
            safe_print("[DAILY] compute error:", e)
    safe_print("[DAILY] exiting")

# -------------------------
# SIGNALS / SHUTDOWN
# -------------------------
def handle_exit(sig, frame):
    safe_print("[SYSTEM] Received exit signal, shutting down...")
    shutdown_event.set()

signal.signal(signal.SIGTERM, handle_exit)
signal.signal(signal.SIGINT, handle_exit)


def shift_publisher_thread():
    safe_print("[SHIFT] publisher started")

    last_shift = {}  # station_id -> shift

    while not shutdown_event.is_set():
        try:
            for station_id in CONFIG.get("stations", {}):
                shift = get_shift_for_station(station_id)

                if last_shift.get(station_id) != shift:
                    publish_current_shift(station_id)
                    last_shift[station_id] = shift

        except Exception as e:
            safe_print("[SHIFT] error:", e)

        shutdown_event.wait(timeout=60)  # check every minute

    safe_print("[SHIFT] publisher exiting")

# -------------------------
# STARTUP & MAIN
# -------------------------
def main():
    global mqtt_client_instance, worker_thread, publisher_thread

    safe_print("[SYSTEM] Starting component")

    # safe_print(f"ALL ENV Variables : {CONFIG_PATH}, {DB_BACKUP_DIR},{DB_PATH}, {AWS_THING_NAME}, AWS_ENDPOINT}")
    # None, None,None, mct-dev-greengrass-core-dk, None,None.
    load_config(CONFIG_PATH)
    init_db()
    init_runtime_state_db()

    # -------------------------
    # RECOVER RUNTIME STATE
    # -------------------------
    runtime = load_runtime_state()

    for station_id, cats in runtime.items():

        inputs = cats.get("state_inputs", {})
        if inputs:
            for tag, obj in inputs.items():
                last_tag_values.setdefault(station_id, {})[tag] = obj["value"]
            safe_print(f"[RECOVER] state inputs restored for {station_id} :- {last_tag_values}")

        # DOWNTIME
        dt = cats.get("downtime", {}).get("active")
        if dt:
            downtime_state[station_id] = {
                "previous_flag": True,
                # "start_time": datetime.fromtimestamp(dt["start_ts"], timezone.utc),
                "start_time":  to_central(datetime.fromtimestamp(dt["start_ts"], timezone.utc)),
                "reason": dt["reason"]
            }
            safe_print(f"[RECOVER] downtime resumed {station_id} :-  {downtime_state} ")

        # CALL BUTTON
        cb = cats.get("call_button", {}).get("last_active")
        if cb:
            last_press_status[station_id] = cb["button"]
            safe_print(f"[RECOVER] call button {station_id} -> {cb['button']}")

    # recover incomplete jobs into live
    with db_lock:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT job_id, station_id, start_ts, operator_sec, diesetter_sec, downtime_sec, planned_run_mins, good_parts, scrap_parts FROM jobs WHERE finalized=0")
        rows = cur.fetchall()
        for job_id, station_id, start_ts, operator_sec, diesetter_sec, downtime_sec, planned_run_mins, good_parts, scrap_parts in rows:
            st = live.setdefault(station_id, {})
            st['job_id'] = job_id
            st['flags'] = st.get('flags', {})
            st['flag_start_ts'] = st.get('flag_start_ts', {})
            st['job_start_counters'] = {'N_GoodPartsTotalQty': good_parts, 'N_ScrapPartsTotalQty': scrap_parts}
            safe_print(f"[RECOVER] resumed job {job_id} for {station_id}")
        conn.close()

    mqtt_client_instance = mqtt_client.Client(client_id=CLIENT_ID)
    mqtt_client_instance.on_connect = on_connect
    mqtt_client_instance.on_disconnect = on_disconnect
    mqtt_client_instance.on_message = on_message
    try:
        mqtt_client_instance.connect(BROKER, PORT, 60)
    except Exception as e:
        safe_print("[MQTT] initial connect failed:", e)

    # start background threads
    publisher_thread = Thread(target=publisher_thread_fn, daemon=True)
    publisher_thread.start()

    worker_thread = Thread(target=worker_fn, daemon=True)
    worker_thread.start()

    watchdog_thread = Thread(target=worker_watchdog_fn, daemon=True)
    watchdog_thread.start()

    Thread(target=break_monitor, daemon=True).start()
    Thread(target=config_reloader, daemon=True).start()
    Thread(target=compute_daily_and_publish, daemon=True).start()

    Thread(target=shift_publisher_thread, daemon=True).start()



    mqtt_client_instance.loop_start()

    # main wait for shutdown
    try:
        while not shutdown_event.is_set():
            time.sleep(1)
    finally:
        safe_print("[SYSTEM] shutdown initiated")
        # signal threads to stop
        shutdown_event.set()
        # enqueue None to unblock workers if needed
        try:
            message_queue.put_nowait(None)
        except Exception:
            pass
        try:
            publish_queue.put_nowait(None)
        except Exception:
            pass
        # stop MQTT loop
        try:
            mqtt_client_instance.loop_stop()
            mqtt_client_instance.disconnect()
        except Exception:
            pass
        safe_print("[SYSTEM] exited")

if __name__ == "__main__":
    main()