#!/usr/bin/env python3
"""
Component 2: Flat-Row Data Storage & Downtime Engine
Upgraded: Persistent DB Connection, Anti-Wipe Guard Clauses, Memory Leak Protection, 
          and precise simultaneous-start timestamping (with rich logging).
"""

import json
import time
import sqlite3
import os
import random
from datetime import datetime, timezone, timedelta
from threading import Thread, RLock, Event
from queue import Queue, Empty
import paho.mqtt.client as mqtt_client
from zoneinfo import ZoneInfo

CENTRAL_TZ = ZoneInfo("America/Chicago")
DB_PATH = os.getenv('DB_PATH', "/tmp/stations_data.db")
CONFIG_PATH = os.getenv('CONFIG_PATH')
SUB_TOPIC = os.getenv("SUB_TOPIC", "#")
BROKER = "127.0.0.1"
PORT = 1883
CLIENT_ID = f'python-mqtt-storage-{random.randint(0, 10000)}'

# Global State
CONFIG = {}
CONFIG_MTIME = None
shutdown_event = Event()
message_queue = Queue(maxsize=10000)
db_lock = RLock()
DB_CONN = None

live = {}  
last_tag_values = {}

ALLOWED_TAGS = {
    "S_DowntimeReasonOperator", "B_JobInProgressForIgnition",
    "B_OperatorInProgressForIgnition", "B_DieSetterInProgressForIgnition", 
    "B_DowntimeInProgressForIgnition", "N_OperationPlannedRunMins", 
    "N_GoodPartsTotalQty", "N_ScrapPartsTotalQty", "MOrder", 
    "S_DieNumber1", "PartNumber", "N_DieSPM", "Quantity",
    "B_EndOfShiftForIgnition", "N_RemainingPartsQty"  # <--- NEW TAGS
}

# -------------------------
# Utilities & DB Connection
# -------------------------
def now_central(): return datetime.now(CENTRAL_TZ)
def now_iso(): return now_central().isoformat()
def safe_print(*args, **kwargs): print(f"{now_iso()} -", *args, **kwargs)

def get_db():
    global DB_CONN
    if DB_CONN is None:
        DB_CONN = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
        DB_CONN.execute("PRAGMA journal_mode=WAL;")
    return DB_CONN

def init_db():
    conn = get_db()
    with db_lock:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
          job_id INTEGER PRIMARY KEY AUTOINCREMENT, station_id TEXT NOT NULL,
          day TEXT NOT NULL, shift TEXT, 
          start_ts INTEGER NOT NULL, end_ts INTEGER,
          operator_sec REAL DEFAULT 0, diesetter_sec REAL DEFAULT 0, downtime_sec REAL DEFAULT 0,
          planned_run_mins REAL DEFAULT 0, good_parts INTEGER DEFAULT 0, scrap_parts INTEGER DEFAULT 0,
          min_op_start INTEGER, max_op_end INTEGER, min_co_start INTEGER, max_co_end INTEGER,
          morder TEXT, die_number TEXT, part_number TEXT, spm REAL, plan_qty INTEGER, 
          finalized INTEGER DEFAULT 0
        );""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS downtime_events (
          event_id INTEGER PRIMARY KEY AUTOINCREMENT, job_id INTEGER, station_id TEXT NOT NULL,
          reason TEXT, start_ts INTEGER NOT NULL, end_ts INTEGER, duration_sec REAL
        );""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_agg (
          day TEXT NOT NULL, station_id TEXT NOT NULL, operator_sec REAL DEFAULT 0,
          diesetter_sec REAL DEFAULT 0, downtime_sec REAL DEFAULT 0, planned_run_mins REAL DEFAULT 0,
          good_parts INTEGER DEFAULT 0, scrap_parts INTEGER DEFAULT 0, oa_sum REAL DEFAULT 0,
          oa_count INTEGER DEFAULT 0, qr_sum REAL DEFAULT 0, qr_count INTEGER DEFAULT 0,
          or_sum REAL DEFAULT 0, or_count INTEGER DEFAULT 0, -- <--- NEW OR COLUMNS
          final_oee REAL, final_oa REAL, final_or REAL, final_qr REAL,
          PRIMARY KEY(day, station_id)
        );""")
        conn.execute("""
        CREATE TABLE IF NOT EXISTS runtime_state (
            station_id TEXT NOT NULL, category TEXT NOT NULL, key TEXT NOT NULL,
            value TEXT NOT NULL, updated_ts INTEGER NOT NULL,
            PRIMARY KEY (station_id, category, key)
        );""")
        conn.commit()

def load_config(path=CONFIG_PATH):
    global CONFIG, CONFIG_MTIME
    try:
        if os.path.exists(path):
            mtime = os.path.getmtime(path)
            if CONFIG_MTIME is None or mtime != CONFIG_MTIME:
                with open(path, "r") as f: CONFIG = json.load(f)
                CONFIG_MTIME = mtime
                safe_print(f"[CONFIG] Loaded stations from {path}")
    except Exception as e: safe_print("[CONFIG] Error loading config:", e)

def config_reloader():
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
        except Exception: pass
        shutdown_event.wait(30)

def is_station_allowed(station_id: str) -> bool:
    if not station_id: return False
    station_cfg = CONFIG.get("stations", {}).get(station_id)
    return bool(station_cfg) and str(station_cfg.get("status", "")).lower() == "active"

def get_shift_date_cst(ts_epoch):
    dt = datetime.fromtimestamp(ts_epoch, CENTRAL_TZ)
    if dt.hour < 6: dt = dt.replace(hour=0, minute=0, second=0) - timedelta(days=1)
    return dt.strftime("%Y-%m-%d")

def get_shift_splits(start_ts, end_ts):
    splits = {}
    curr_ts = start_ts
    while curr_ts < end_ts:
        curr_day_str = get_shift_date_cst(curr_ts)
        curr_dt = datetime.fromtimestamp(curr_ts, CENTRAL_TZ)
        if curr_dt.hour >= 6:
            next_boundary_dt = (curr_dt + timedelta(days=1)).replace(hour=6, minute=0, second=0, microsecond=0)
        else:
            next_boundary_dt = curr_dt.replace(hour=6, minute=0, second=0, microsecond=0)
        
        next_boundary_ts = int(next_boundary_dt.timestamp())
        chunk_end_ts = min(end_ts, next_boundary_ts)
        splits[curr_day_str] = splits.get(curr_day_str, 0) + (chunk_end_ts - curr_ts)
        curr_ts = chunk_end_ts
    return splits


def parse_hhmm(tstr):
    try:
        parts = tstr.split(":")
        from datetime import time as tm
        return tm(int(parts[0]), int(parts[1]))
    except Exception: return None

def get_shift_for_station(station_id, ts_epoch):
    dt = datetime.fromtimestamp(ts_epoch, CENTRAL_TZ)
    now_time = dt.time()
    station_cfg = CONFIG.get("stations", {}).get(station_id, {})
    shift_group = station_cfg.get("shifts", "2shifts")
    shifts_map = CONFIG.get("shifts", {})

    try:
        if shift_group not in shifts_map:
            from datetime import time as tm
            return "A" if tm(6,0) <= now_time < tm(18,0) else "B"
        group = shifts_map[shift_group]
        for sname, win in group.items():
            start_t = parse_hhmm(win["start"])
            end_t = parse_hhmm(win["end"])
            
            if end_t > start_t:
                start_dt = datetime.combine(dt.date(), start_t, tzinfo=CENTRAL_TZ)
                end_dt = datetime.combine(dt.date(), end_t, tzinfo=CENTRAL_TZ)
            else:
                if now_time < end_t:
                    start_dt = datetime.combine(dt.date() - timedelta(days=1), start_t, tzinfo=CENTRAL_TZ)
                    end_dt = datetime.combine(dt.date(), end_t, tzinfo=CENTRAL_TZ)
                else:
                    start_dt = datetime.combine(dt.date(), start_t, tzinfo=CENTRAL_TZ)
                    end_dt = datetime.combine(dt.date() + timedelta(days=1), end_t, tzinfo=CENTRAL_TZ)
            
            if start_dt <= dt <= end_dt:
                return sname
        return list(group.keys())[0]
    except Exception:
        return "A"
    
def save_runtime_state(station_id, category, key, value):
    conn = get_db()
    with db_lock:
        conn.execute("""
            INSERT INTO runtime_state(station_id, category, key, value, updated_ts) VALUES (?,?,?,?,?)
            ON CONFLICT(station_id, category, key) DO UPDATE SET value=excluded.value, updated_ts=excluded.updated_ts
        """, (station_id, category, key, str(value), int(time.time())))
        conn.commit()

def delete_runtime_state(station_id, category, key):
    conn = get_db()
    with db_lock:
        conn.execute("DELETE FROM runtime_state WHERE station_id=? AND category=? AND key=?", (station_id, category, key))
        conn.commit()

# -------------------------
# Core Job & Session Logic
# -------------------------
def add_duration_to_db(station_id, job_id, tag, start_ts, end_ts):
    duration = end_ts - start_ts
    if duration <= 0 or not job_id: return
    splits = get_shift_splits(start_ts, end_ts)

    col = "operator_sec"
    if tag == "B_DieSetterInProgressForIgnition": col = "diesetter_sec"
    elif tag == "B_DowntimeInProgressForIgnition": col = "downtime_sec"

    conn = get_db()
    with db_lock:
        for day_str, chunk_dur in splits.items():
            conn.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (day_str, station_id))
            conn.execute(f"UPDATE daily_agg SET {col} = {col} + ? WHERE day=? AND station_id=?", (chunk_dur, day_str, station_id))
        
        conn.execute(f"UPDATE jobs SET {col} = {col} + ? WHERE job_id = ?", (duration, job_id))

        if tag == "B_OperatorInProgressForIgnition":
            conn.execute("UPDATE jobs SET max_op_end = ? WHERE job_id = ?", (end_ts, job_id))
        elif tag == "B_DieSetterInProgressForIgnition":
            conn.execute("UPDATE jobs SET max_co_end = ? WHERE job_id = ?", (end_ts, job_id))
        elif tag == "B_DowntimeInProgressForIgnition":
            reason = last_tag_values.get(station_id, {}).get("S_DowntimeReasonOperator", "Unknown Reason")
            conn.execute("""
                INSERT INTO downtime_events(job_id, station_id, reason, start_ts, end_ts, duration_sec) 
                VALUES (?,?,?,?,?,?)
            """, (job_id, station_id, reason, start_ts, end_ts, duration))
        conn.commit()
    safe_print(f"[DB WRITE] {station_id} | Job {job_id} | Added {duration}s to '{col}'. Shifts affected: {list(splits.keys())}")

def _end_job(station_id, job_id, ts_epoch, st):
    active_sessions = st.setdefault('active_sessions', {})
    
    for tag in list(active_sessions.keys()):
        start_ts = active_sessions.pop(tag)
        add_duration_to_db(station_id, job_id, tag, start_ts, ts_epoch)
        delete_runtime_state(station_id, 'active_sessions', tag)
        safe_print(f"[TIMER STOP] {station_id} | {tag} = False. Session closed with Job.")

    conn = get_db()
    with db_lock:
        conn.execute("UPDATE jobs SET end_ts=?, finalized=1 WHERE job_id=?", (ts_epoch, job_id))
        trigger_val = json.dumps({"type": "JOB_FINALIZED", "job_id": job_id})
        conn.execute("""
            INSERT INTO runtime_state(station_id, category, key, value, updated_ts) VALUES (?,'reporting_trigger',?,?,?)
            ON CONFLICT(station_id, category, key) DO UPDATE SET value=excluded.value, updated_ts=excluded.updated_ts
        """, (station_id, f"JOB_END_{job_id}", trigger_val, int(time.time())))
        conn.commit()
        
    st['job_id'] = None
    safe_print(f"[{datetime.now(CENTRAL_TZ).isoformat()}] Ended job_id={job_id} for {station_id}")

def process_job_flag(station_id, val_bool, ts_epoch):
    st = live.setdefault(station_id, {})
    current_job_id = st.get('job_id')

    if val_bool and current_job_id: 
        safe_print(f"[INFO] Ignored duplicate Job=True signal for {station_id}. Job {current_job_id} is already active.")
        return 

    if val_bool:
        if current_job_id:
            safe_print(f"[WARNING] Job {current_job_id} was stuck open on {station_id}. Force-closing.")
            _end_job(station_id, current_job_id, ts_epoch - 1, st)

        day = get_shift_date_cst(ts_epoch)
        cache = last_tag_values.get(station_id, {})
        morder, die, part = cache.get("MOrder"), cache.get("S_DieNumber1"), cache.get("PartNumber")
        spm, plan_qty, planned_mins = cache.get("N_DieSPM", 0.0), cache.get("Quantity", 0), cache.get("N_OperationPlannedRunMins", 0.0)

        shift_letter = get_shift_for_station(station_id, ts_epoch)
        conn = get_db()
        with db_lock:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO jobs(station_id,day,shift, start_ts, morder, die_number, part_number, spm, plan_qty, planned_run_mins) 
                VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (station_id, day, shift_letter, ts_epoch, morder, die, part, spm, plan_qty, planned_mins))
            new_job_id = cur.lastrowid
            conn.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (day, station_id))
            conn.commit()
        
        st['job_id'] = new_job_id
        safe_print(f"[{datetime.now(CENTRAL_TZ).isoformat()}] Started job_id={new_job_id} for {station_id}")

        raw_tags = st.setdefault('raw_tags', {})
        active_sessions = st.setdefault('active_sessions', {})
        for tag in ["B_OperatorInProgressForIgnition", "B_DieSetterInProgressForIgnition", "B_DowntimeInProgressForIgnition"]:
            if raw_tags.get(tag) == True and tag not in active_sessions:
                active_sessions[tag] = ts_epoch
                save_runtime_state(station_id, 'active_sessions', tag, ts_epoch)
                safe_print(f"[TIMER START] {station_id} | Job {new_job_id} | {tag} = True (Simultaneous with Job Start)")
                
                db_col = "min_op_start" if tag == "B_OperatorInProgressForIgnition" else "min_co_start" if tag == "B_DieSetterInProgressForIgnition" else None
                if db_col:
                    with db_lock:
                        conn.execute(f"UPDATE jobs SET {db_col} = COALESCE({db_col}, ?) WHERE job_id = ?", (ts_epoch, new_job_id))
                        conn.commit()
                    safe_print(f"[DB WRITE] {station_id} | Job {new_job_id} | Set {db_col} = {ts_epoch}")

    elif not val_bool and current_job_id:
        _end_job(station_id, current_job_id, ts_epoch, st)

def process_session_flag(station_id, tag, val_bool, ts_epoch):
    st = live.setdefault(station_id, {})
    raw_tags = st.setdefault('raw_tags', {})
    active_sessions = st.setdefault('active_sessions', {})
    current_job_id = st.get('job_id')

    if raw_tags.get(tag) == val_bool: return 
    raw_tags[tag] = val_bool
    save_runtime_state(station_id, 'raw_tags', tag, val_bool)

    if val_bool:
        if current_job_id and tag not in active_sessions:
            active_sessions[tag] = ts_epoch
            save_runtime_state(station_id, 'active_sessions', tag, ts_epoch)
            safe_print(f"[TIMER START] {station_id} | Job {current_job_id} | {tag} = True")
            
            db_col = "min_op_start" if tag == "B_OperatorInProgressForIgnition" else "min_co_start" if tag == "B_DieSetterInProgressForIgnition" else None
            if db_col:
                conn = get_db()
                with db_lock:
                    conn.execute(f"UPDATE jobs SET {db_col} = COALESCE({db_col}, ?) WHERE job_id = ?", (ts_epoch, current_job_id))
                    conn.commit()
                safe_print(f"[DB WRITE] {station_id} | Job {current_job_id} | Set {db_col} = {ts_epoch}")
        elif current_job_id:
            safe_print(f"[WARNING] {station_id} ignored {tag}=True because no Job is active.")
                    
    else:
        if tag in active_sessions:
            start_ts = active_sessions.pop(tag)
            add_duration_to_db(station_id, current_job_id, tag, start_ts, ts_epoch)
            delete_runtime_state(station_id, 'active_sessions', tag)
            safe_print(f"[TIMER STOP] {station_id} | {tag} = False. Session duration calculated.")

def process_absolute_or_snapshot(station_id, tag, value, ts_epoch):
    last_tag_values.setdefault(station_id, {})[tag] = value
    st = live.setdefault(station_id, {})
    job_id = st.get('job_id')
    
    col_map = {
            "N_OperationPlannedRunMins": "planned_run_mins", "N_GoodPartsTotalQty": "good_parts", 
            "N_ScrapPartsTotalQty": "scrap_parts", "MOrder": "morder", "S_DieNumber1": "die_number",
            "PartNumber": "part_number", "N_DieSPM": "spm", "Quantity": "plan_qty",
            "N_RemainingPartsQty": "plan_qty" # <--- ADDED MAPPING
        }
    
    if tag not in col_map or not job_id: return
    col = col_map[tag]

    # --- ANTI-WIPE GUARD CLAUSE ---
    if col in ("morder", "die_number", "part_number") and str(value).strip() == "":
        safe_print(f"[ANTI-WIPE] {station_id} | Job {job_id} | Ignored empty string wipe for {col}")
        return 
        
    # if col in ("plan_qty", "planned_run_mins", "good_parts", "scrap_parts","spm"):
    #     try:
    #         if float(value) == 0.0: 
    #             safe_print(f"[ANTI-WIPE] {station_id} | Job {job_id} | Ignored zero-out wipe for {col}")
    #             return 
    #     except ValueError: pass

    if col in ("plan_qty", "planned_run_mins", "good_parts", "scrap_parts", "spm"):
        try:
            # Only ignore 0.0 if we already have a meaningful value in last_tag_values
            if float(value) in (0.0,-1.0) and last_tag_values.get(station_id, {}).get(tag) not in (None, 0):
                safe_print(f"[ANTI-WIPE] {station_id} | Job {job_id} | Ignored zero-out wipe for {col}")
                return 
        except ValueError: pass


    if value is None or str(value).strip().lower() == "none":
        if tag in ("N_OperationPlannedRunMins", "N_GoodPartsTotalQty", 
                  "N_ScrapPartsTotalQty", "N_DieSPM", "Quantity", "N_RemainingPartsQty"):
            value = 0.0
        else:
            value = "" # Use empty string for text columns like MOrder
    # ------------------------------

    conn = get_db()
    with db_lock:
        conn.execute(f"UPDATE jobs SET {col} = ? WHERE job_id = ?", (value, job_id))
        if col in ("planned_run_mins", "good_parts", "scrap_parts"):
            day = get_shift_date_cst(ts_epoch)
            conn.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (day, station_id))
            conn.execute(f"UPDATE daily_agg SET {col} = ? WHERE day=? AND station_id=?", (value, day, station_id))
        conn.commit()

    safe_print(f"[LIVE UPDATE] {station_id} | Job {job_id} | '{col}' overwritten to absolute value: {value}")

def active_heartbeat_worker():
    """Actively writes a heartbeat every 60 seconds for all active stations so recovery math is always accurate."""
    safe_print("[SYSTEM] Active heartbeat thread started.")
    time.sleep(5)
    # while not shutdown_event.is_set():
    #     now_ts = int(time.time())
    #     stations = CONFIG.get("stations", {})
        
    #     for station_id, cfg in stations.items():
    #         if str(cfg.get("status", "")).lower() == "active":
    #             save_runtime_state(station_id, 'system', 'last_heartbeat', now_ts)
        
    #     # We wait 60 seconds, but doing it via the event allows instant shutdown if needed
    #     shutdown_event.wait(60)

    while not shutdown_event.is_set():
        now_ts = int(time.time())
        try:
            with db_lock:
                for station_id in list(live.keys()):
                    save_runtime_state(station_id, 'system', 'last_heartbeat', now_ts)
        except Exception as e:
            pass # Keep alive
        shutdown_event.wait(60)

# -------------------------
# Worker & MQTT Handling
# -------------------------

def process_shift_end_split(station_id, ts_epoch):
    st = live.setdefault(station_id, {})
    current_job_id = st.get('job_id')

    if not current_job_id: return

    safe_print(f"[SHIFT SPLIT] {station_id} | Shift end triggered. Finalizing segment for job {current_job_id}.")

    # 1. End current job segment (Triggers Reporting Engine for the old shift)
    # NOTE: _end_job automatically closes active downtime. It accurately uses the current 
    # S_DowntimeReasonOperator from last_tag_values, exactly matching live_state_engine's logic!
    _end_job(station_id, current_job_id, ts_epoch, st)

    # 2. Start the new job segment for the new shift
    day = get_shift_date_cst(ts_epoch)
    shift_letter = get_shift_for_station(station_id, ts_epoch)
    cache = last_tag_values.get(station_id, {})
    
    morder = cache.get("MOrder")
    die = cache.get("S_DieNumber1")
    part = cache.get("PartNumber")
    spm = cache.get("N_DieSPM", 0.0)
    planned_mins = cache.get("N_OperationPlannedRunMins", 0.0)
    
    # Capture RemainingPartsQty for the new Shift's Plan Qty
    plan_qty = cache.get("N_RemainingPartsQty", cache.get("Quantity", 0))

    conn = get_db()
    with db_lock:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO jobs(station_id, day, shift, start_ts, morder, die_number, part_number, spm, plan_qty, planned_run_mins) 
            VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (station_id, day, shift_letter, ts_epoch, morder, die, part, spm, plan_qty, planned_mins))
        new_job_id = cur.lastrowid
        conn.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (day, station_id))
        conn.commit()
    
    st['job_id'] = new_job_id
    safe_print(f"[SHIFT SPLIT] {station_id} | Started new continuation job_id={new_job_id} for Shift {shift_letter}")

    # 3. Resume the active timers seamlessly
    raw_tags = st.setdefault('raw_tags', {})
    active_sessions = st.setdefault('active_sessions', {})
    
    for tag in ["B_OperatorInProgressForIgnition", "B_DieSetterInProgressForIgnition", "B_DowntimeInProgressForIgnition"]:
        if raw_tags.get(tag) == True:
            active_sessions[tag] = ts_epoch
            save_runtime_state(station_id, 'active_sessions', tag, ts_epoch)
            safe_print(f"[TIMER RESUME] {station_id} | Job {new_job_id} | {tag} carried over.")
            
            db_col = "min_op_start" if tag == "B_OperatorInProgressForIgnition" else "min_co_start" if tag == "B_DieSetterInProgressForIgnition" else None
            if db_col:
                with db_lock:
                    conn.execute(f"UPDATE jobs SET {db_col} = ? WHERE job_id = ?", (ts_epoch, new_job_id))
                    conn.commit()

def worker_fn():
    safe_print("[SYSTEM] Worker thread started and processing MQTT queue.")
    while not shutdown_event.is_set():
        try: payload = message_queue.get(timeout=1.0)
        except Empty: continue
        
        tag, station, val = payload.get("tagName"), payload.get("stationID"), payload.get("details")
        
        
        if tag not in ALLOWED_TAGS: continue 

        ts_epoch = int(payload.get("timestamp", time.time()))
        
        # 1. Logic Routing
        if tag == "S_DowntimeReasonOperator": 
            reason_str = str(val).strip()
            last_tag_values.setdefault(station, {})[tag] = reason_str
            save_runtime_state(station, 'last_known_reason', 'S_DowntimeReasonOperator', reason_str)
            safe_print(f"[REASON UPDATED] {station} | Operator set downtime reason to: '{reason_str}'")
            
            job_id = live.get(station, {}).get('job_id')
            if job_id:
                conn = get_db()
                with db_lock:
                    conn.execute("""
                        UPDATE downtime_events SET reason = ? WHERE event_id = (
                            SELECT event_id FROM downtime_events WHERE station_id = ? AND job_id = ? ORDER BY event_id DESC LIMIT 1
                        )
                    """, (reason_str, station, job_id))
                    conn.commit()

        elif tag == "B_EndOfShiftForIgnition":
            # if str(val).lower() in ("true", "1", "yes", "y"):
            #     process_shift_end_split(station, ts_epoch)

            is_end_of_shift = str(val).lower() in ("true", "1", "yes", "y")
            st = live.setdefault(station, {})
            raw_tags = st.setdefault('raw_tags', {})
            
            # Only trigger the split if the tag is transitioning from False -> True
            if is_end_of_shift and not raw_tags.get(tag, False):
                process_shift_end_split(station, ts_epoch)
                
            # Always remember the current state so we don't double-fire
            raw_tags[tag] = is_end_of_shift
            save_runtime_state(station, 'raw_tags', tag, is_end_of_shift)
                
        elif tag == "B_JobInProgressForIgnition":
            process_job_flag(station, str(val).lower() in ("true", "1", "yes", "y"), ts_epoch)
        elif tag in ("B_OperatorInProgressForIgnition","B_DieSetterInProgressForIgnition", "B_DowntimeInProgressForIgnition"):
            process_session_flag(station, tag, str(val).lower() in ("true", "1", "yes", "y"), ts_epoch)
        else:
            try:
                if any(x in tag for x in ["Qty", "Mins", "SPM", "Quantity"]): val = float(val)
            except: pass
            process_absolute_or_snapshot(station, tag, val, ts_epoch)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.subscribe(SUB_TOPIC)
        safe_print(f"[SYSTEM] Subscribed to topic: {SUB_TOPIC}")
    else:
        safe_print(f"[SYSTEM] Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        if isinstance(payload, dict) and "stationID" in payload and "tagName" in payload:
            if is_station_allowed(payload["stationID"]):
                message_queue.put(payload, timeout=0.1) 
    except Exception: pass

def recover_active_sessions():
    """Recovers previous state, timers, and jobs from the SQLite database after a reboot."""
    conn = get_db()
    with db_lock:
        now_ts = int(time.time())
        cur = conn.cursor()
        
        cur.execute("SELECT job_id, station_id, start_ts FROM jobs WHERE finalized=0")
        for job_id, station_id, start_ts in cur.fetchall():
            if (now_ts - start_ts) > 259200:
                safe_print(f"[CLEANUP] Job {job_id} for {station_id} is over 24h old. Force-closing.")
                conn.execute("UPDATE jobs SET end_ts=?, finalized=1 WHERE job_id=?", (now_ts, job_id))
                conn.execute("DELETE FROM runtime_state WHERE station_id=? AND category='active_sessions'", (station_id,))
            else:
                live.setdefault(station_id, {})['job_id'] = job_id
                safe_print(f"[RECOVER] Resumed active job_id={job_id} for {station_id}")
                
        cur.execute("SELECT station_id, key, value FROM runtime_state WHERE category='raw_tags'")
        for sid, tag, val_str in cur.fetchall():
            live.setdefault(sid, {}).setdefault('raw_tags', {})[tag] = (val_str == "True")

        heartbeats = {sid: int(hb) for sid, hb in conn.execute("SELECT station_id, value FROM runtime_state WHERE category='system' AND key='last_heartbeat'").fetchall()}

        cur.execute("SELECT station_id, key, value FROM runtime_state WHERE category='active_sessions'")
        for station_id, tag, start_ts_str in cur.fetchall():
            start_ts = int(start_ts_str)
            last_hb = heartbeats.get(station_id, now_ts)
            offline_gap = now_ts - last_hb
            
            if offline_gap > 259200: 
                safe_print(f"[WARNING] {station_id} offline for >4hrs. Purging ghost timer {tag}.")
                job_id = live.get(station_id, {}).get('job_id')
                if job_id:
                    add_duration_to_db(station_id, job_id, tag, start_ts, last_hb)
                    conn.execute("UPDATE jobs SET end_ts=?, finalized=1 WHERE job_id=?", (last_hb, job_id))
                    live[station_id]['job_id'] = None 
                conn.execute("DELETE FROM runtime_state WHERE station_id=? AND category=? AND key=?", (station_id, 'active_sessions', tag))
            else:
                live.setdefault(station_id, {}).setdefault('active_sessions', {})[tag] = start_ts
                safe_print(f"[RECOVER] Resumed active {tag} timer for {station_id} (Offline Gap: {offline_gap}s)")
        
        cur.execute("SELECT station_id, key, value FROM runtime_state WHERE category='last_known_reason'")
        for sid, tag, val_str in cur.fetchall():
            last_tag_values.setdefault(sid, {})[tag] = val_str
            safe_print(f"[RECOVER] Restored downtime reason '{val_str}' for {sid}")
            
        conn.commit()

def main():
    # 1. Initialize Memory & DB First!
    load_config()
    init_db()  
    recover_active_sessions() 

    # 2. Setup MQTT Client
    client = mqtt_client.Client(client_id=CLIENT_ID)
    client.on_connect = on_connect
    client.on_message = on_message
    
    # 3. Patient MQTT Connection Loop (Fixes Errno 111 on Greengrass boot)
    connected = False
    while not connected and not shutdown_event.is_set():
        try:
            client.connect(BROKER, PORT, 60)
            connected = True
            safe_print("[SYSTEM] Successfully connected to local MQTT Broker.")
        except ConnectionRefusedError:
            safe_print("[SYSTEM] MQTT Broker not ready yet. Retrying in 5 seconds...")
            time.sleep(5)

    client.subscribe(SUB_TOPIC)
    
    # 4. Start Background Threads AFTER the DB is initialized and MQTT is connected
    Thread(target=config_reloader, daemon=True).start()
    Thread(target=active_heartbeat_worker, daemon=True).start()
    Thread(target=worker_fn, daemon=True).start()

    # 5. Start MQTT Loop
    client.loop_start()

    # 6. Keep Main Thread Alive safely
    try:
        while not shutdown_event.is_set(): 
            time.sleep(1)
    except KeyboardInterrupt:
        safe_print("[SYSTEM] Shutting down Storage Engine...")
        shutdown_event.set()
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()