#!/usr/bin/env python3
"""
Component 2: Flat-Row Data Storage & Downtime Engine
Upgraded: Persistent DB Connection, Anti-Wipe Guard Clauses, Memory Leak Protection, 
          and precise simultaneous-start timestamping (with rich logging).
"""

import json
from linecache import cache
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
    "N_TotalProductionPartsQty","N_GoodPartsTotalQty", "N_ScrapPartsTotalQty", "MOrder", 
    "S_DieNumber1", "PartNumber", "N_DieSPM", "Quantity",
    "B_EndOfShiftForIgnition", "N_RemainingPartsQty", 'PB_PressState_Fixed',
    "IDBarCode_DS", "IDBarCode_OP","N_StrokeCount"  # <--- NEW TAGS
}

# -------------------------
# Utilities & DB Connection
# -------------------------
def now_central(): return datetime.now(CENTRAL_TZ)
def now_iso(): return now_central().isoformat()
def safe_print(*args, **kwargs): print(f"{now_iso()} -", *args, **kwargs)
def get_cst_str(ts_epoch):
    """Safely converts an epoch integer into a human-readable CST/CDT string."""
    if not ts_epoch: return None
    return datetime.fromtimestamp(ts_epoch, CENTRAL_TZ).strftime('%Y-%m-%d %H:%M:%S')

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
          start_cst TEXT, end_cst TEXT,
          min_op_start_cst TEXT, max_op_end_cst TEXT, min_co_start_cst TEXT, max_co_end_cst TEXT,
          operator_sec REAL DEFAULT 0, diesetter_sec REAL DEFAULT 0, downtime_sec REAL DEFAULT 0,
          planned_run_mins REAL DEFAULT 0, good_parts INTEGER DEFAULT 0, scrap_parts INTEGER DEFAULT 0,total_parts INTEGER DEFAULT 0,
          min_op_start INTEGER, max_op_end INTEGER, min_co_start INTEGER, max_co_end INTEGER,
          morder TEXT, die_number TEXT, part_number TEXT, spm REAL, plan_qty INTEGER, 
          diesetter_id TEXT, operator_id TEXT, -- <--- NEW COLUMNS
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
          good_parts INTEGER DEFAULT 0, scrap_parts INTEGER DEFAULT 0, total_parts INTEGER DEFAULT 0,
          diesetter_id TEXT, operator_id TEXT, -- <--- NEW COLUMNS
          oa_sum REAL DEFAULT 0,
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

def load_persistent_tag_cache():
    """Restores the last_tag_values memory cache from the hard drive after a reboot."""
    conn = get_db()
    with db_lock:
        cur = conn.cursor()
        cur.execute("SELECT station_id, key, value FROM runtime_state WHERE category = 'tag_cache'")
        for st_id, tag, val in cur.fetchall():
            # Try to convert back to float if it's a number, otherwise keep as string
            try:
                parsed_val = float(val) if '.' in val or val.lstrip('-').isdigit() else val
            except ValueError:
                parsed_val = val
            last_tag_values.setdefault(st_id, {})[tag] = parsed_val
    safe_print("[SYSTEM] Persistent tag cache successfully loaded from DB.")


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

def get_factory_day_start_time(station_id):
    """Dynamically reads the config to find the exact start time of the production day."""
    from datetime import time as tm
    default_time = tm(6, 0) # Fallback to 6:00 AM if config is missing
    
    if not station_id or not CONFIG: return default_time
    try:
        station_cfg = CONFIG.get("stations", {}).get(station_id, {})
        shift_group = station_cfg.get("shifts")
        if not shift_group: return default_time
        
        group = CONFIG.get("shifts", {}).get(shift_group, {})
        if not group: return default_time
        
        # Grab the start time of the very first shift listed in the config (e.g., Shift A)
        first_shift_name = list(group.keys())[0]
        start_str = group[first_shift_name].get("start", "06:00")
        parsed = parse_hhmm(start_str)
        return parsed if parsed else default_time
    except Exception:
        return default_time
    

def get_shift_date_cst(station_id, ts_epoch):
    dt = datetime.fromtimestamp(ts_epoch, CENTRAL_TZ)
    day_start = get_factory_day_start_time(station_id)
    
    # --- DYNAMIC BOUNDARY ---
    # If the current time is before the station's configured day start time, 
    # it belongs to the previous factory production day.
    if dt.time() < day_start: 
        dt = dt - timedelta(days=1)
        
    return dt.strftime("%Y-%m-%d")

def get_shift_splits(station_id, start_ts, end_ts):
    splits = {}
    curr_ts = start_ts
    day_start = get_factory_day_start_time(station_id)
    
    while curr_ts < end_ts:
        curr_day_str = get_shift_date_cst(station_id, curr_ts)
        curr_dt = datetime.fromtimestamp(curr_ts, CENTRAL_TZ)
        
        # --- DYNAMIC BOUNDARY ---
        if curr_dt.time() >= day_start:
            # The next boundary is tomorrow at the dynamic start time
            next_boundary_dt = datetime.combine(curr_dt.date() + timedelta(days=1), day_start, tzinfo=CENTRAL_TZ)
        else:
            # The next boundary is today at the dynamic start time
            next_boundary_dt = datetime.combine(curr_dt.date(), day_start, tzinfo=CENTRAL_TZ)
            
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


# ==========================================
# ZOMBIE JOB WATCHDOG & RECOVERY
# ==========================================
last_stroke_counts = {}
last_stroke_times = {}

# def stroke_count_watchdog():
#     """Monitors if machines have stopped producing over the weekend."""
#     safe_print("[WATCHDOG] Stroke Count Watchdog started.")
#     IDLE_TIMEOUT_SEC = 3600  # 60 minutes
    
#     while not shutdown_event.is_set():
#         now_ts = int(time.time())
#         try:
#             with db_lock:
#                 conn = get_db()
#                 cur = conn.cursor()
#                 cur.execute("SELECT station_id, job_id, start_ts FROM jobs WHERE finalized=0")
#                 active_jobs = cur.fetchall()
                
#                 for station_id, job_id, start_ts in active_jobs:
#                     current_stroke = last_tag_values.get(station_id, {}).get("N_StrokeCount", 0)
                    
#                     if current_stroke != last_stroke_counts.get(station_id):
#                         last_stroke_counts[station_id] = current_stroke
#                         last_stroke_times[station_id] = now_ts
#                         continue
                        
#                     time_idle = now_ts - last_stroke_times.get(station_id, now_ts)
                    
#                     if time_idle > IDLE_TIMEOUT_SEC:
#                         safe_print(f"[WATCHDOG] {station_id} idle for {time_idle/60:.1f} mins. Auto-closing weekend job.")
                        
#                         st = live.setdefault(station_id, {})
#                         idle_start_ts = int(last_stroke_times.get(station_id, now_ts))
                        
#                         # Use the last stroke time, UNLESS it's a ghost job, 
#                         # in which case we close it right when it started (Duration = 0).
#                         safe_end_time = max(start_ts, idle_start_ts)
                        
#                         _end_job(station_id, job_id, safe_end_time, st)
                        
#                         st['zombie_closed'] = True
#                         save_runtime_state(station_id, 'system', 'zombie_closed', "True")
#                         last_stroke_times[station_id] = now_ts + 86400 
                
#         except Exception as e:
#             safe_print(f"[WATCHDOG ERROR]: {e}")

#         # Change 600 to 60 to check every minute    
#         shutdown_event.wait(60) # Check every 10 minutes

def stroke_count_watchdog():
    """Monitors if machines have stopped producing over the weekend."""
    safe_print("[WATCHDOG] Stroke Count Watchdog started.")
    IDLE_TIMEOUT_SEC = 3600  # 60 minutes
    
    while not shutdown_event.is_set():
        now_ts = int(time.time())
        try:
            with db_lock:
                conn = get_db()
                cur = conn.cursor()
                cur.execute("SELECT station_id, job_id, start_ts FROM jobs WHERE finalized=0")
                active_jobs = cur.fetchall()
                
                for station_id, job_id, start_ts in active_jobs:
                    # Grab the exact physical PLC timestamp recorded by worker_fn
                    # If script just rebooted and it's empty, safely fallback to now_ts
                    idle_start_ts = last_stroke_times.get(station_id, now_ts)
                    
                    time_idle = now_ts - idle_start_ts
                    
                    # FLAW 1 FIXED: Changed strict > to >= 
                    if time_idle >= IDLE_TIMEOUT_SEC:
                        safe_print(f"[WATCHDOG] {station_id} idle for {time_idle/60:.1f} mins. Auto-closing weekend job.")
                        
                        st = live.setdefault(station_id, {})
                        
                        # Close the job exactly when the last stroke occurred
                        safe_end_time = max(start_ts, idle_start_ts)
                        
                        _end_job(station_id, job_id, safe_end_time, st)
                        
                        st['zombie_closed'] = True
                        save_runtime_state(station_id, 'system', 'zombie_closed', "True")
                        
                        # Push the memory 24 hours into the future so it doesn't repeatedly loop while sleeping
                        last_stroke_times[station_id] = now_ts + 86400 
                
        except Exception as e:
            safe_print(f"[WATCHDOG ERROR]: {e}")
            
        # FLAW 2 FIXED: Wake up every 60 seconds instead of 600 seconds
        shutdown_event.wait(60)
        
def resume_zombie_job_as_split(station_id, ts_epoch, new_operator_id=None):
    """Safely wakes up a weekend zombie job as a continuation split."""
    st = live.setdefault(station_id, {})
    
    conn = get_db()
    with db_lock:
        cur = conn.cursor()
        cur.execute("""
            SELECT morder, die_number, part_number, spm, planned_run_mins, plan_qty, diesetter_id, operator_id
            FROM jobs WHERE station_id = ? ORDER BY job_id DESC LIMIT 1
        """, (station_id,))
        row = cur.fetchone()
        
    db_morder, db_die, db_part, db_spm, db_planned_mins, db_qty, db_ds_id, db_op_id = row if row else ("", "", "", 0.0, 0.0, 0, "", "")

    # ==========================================
    # --- ADDED: The +2 Hour Lookahead Math ---
    # ==========================================
    # Operators might wake the machine up 15 minutes before the shift officially starts.
    # We look 2 hours into the future to guarantee we grab the ONCOMING Shift and Day.
    oncoming_epoch = ts_epoch + 7200
    day = get_shift_date_cst(station_id, oncoming_epoch)
    shift_letter = get_shift_for_station(station_id, oncoming_epoch)

    cache = last_tag_values.get(station_id, {})
    
    morder = cache.get("MOrder") or db_morder or ""
    die = cache.get("S_DieNumber1") or db_die or ""
    part = cache.get("PartNumber") or db_part or ""
    spm = cache.get("N_DieSPM") or db_spm or 0.0
    planned_mins = cache.get("N_OperationPlannedRunMins") or db_planned_mins or 0.0
    ds_id = cache.get("IDBarCode_DS") or db_ds_id or ""
    
    if new_operator_id is not None:
        op_id = new_operator_id
        last_tag_values.setdefault(station_id, {})["IDBarCode_OP"] = new_operator_id 
    else:
        op_id = cache.get("IDBarCode_OP") or db_op_id or ""

    raw_rem = cache.get("N_RemainingPartsQty")
    raw_qty = cache.get("Quantity") or db_qty or 0
    
    try: rem_val = float(raw_rem) if raw_rem is not None else 0.0
    except ValueError: rem_val = 0.0
    try: qty_val = float(raw_qty)
    except ValueError: qty_val = 0.0

    plan_qty = rem_val if rem_val > 0.0 else qty_val

    with db_lock:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO jobs(station_id, day, shift, start_ts,start_cst, morder, die_number, part_number, spm, plan_qty, planned_run_mins,diesetter_id, operator_id) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (station_id, day, shift_letter, ts_epoch, get_cst_str(ts_epoch), morder, die, part, spm, plan_qty, planned_mins, ds_id, op_id))
        new_job_id = cur.lastrowid
        conn.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (day, station_id))
        conn.commit()
    
    st['job_id'] = new_job_id
    st['is_continuation'] = True 
    save_runtime_state(station_id, 'system', 'is_continuation', "True") 

    safe_print(f"[MONDAY SPLIT] {station_id} | Started new continuation job_id={new_job_id} for Shift {shift_letter}")

    raw_tags = st.setdefault('raw_tags', {})
    active_sessions = st.setdefault('active_sessions', {})
    
    for tag in ["B_OperatorInProgressForIgnition", "B_DieSetterInProgressForIgnition", "B_DowntimeInProgressForIgnition"]:
        if raw_tags.get(tag) == True:
            active_sessions[tag] = ts_epoch
            save_runtime_state(station_id, 'active_sessions', tag, ts_epoch)
            
            db_col = "min_op_start" if tag == "B_OperatorInProgressForIgnition" else "min_co_start" if tag == "B_DieSetterInProgressForIgnition" else None
            db_col_cst = "min_op_start_cst" if tag == "B_OperatorInProgressForIgnition" else "min_co_start_cst" if tag == "B_DieSetterInProgressForIgnition" else None
            if db_col:
                with db_lock:
                    conn.execute(f"UPDATE jobs SET {db_col} = ?, {db_col_cst} = ? WHERE job_id = ?", (ts_epoch, get_cst_str(ts_epoch),new_job_id))
                    conn.commit()
# ==========================================

# -------------------------
# Core Job & Session Logic
# -------------------------
def add_duration_to_db(station_id, job_id, tag, start_ts, end_ts):
    duration = end_ts - start_ts
    if duration <= 0 or not job_id: return
    splits = get_shift_splits(station_id, start_ts, end_ts)

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
            conn.execute("UPDATE jobs SET max_op_end = ?, max_op_end_cst = ? WHERE job_id = ?", (end_ts, get_cst_str(end_ts), job_id))
        elif tag == "B_DieSetterInProgressForIgnition":
            conn.execute("UPDATE jobs SET max_co_end = ?, max_co_end_cst = ? WHERE job_id = ?", (end_ts, get_cst_str(end_ts), job_id))
        elif tag == "B_DowntimeInProgressForIgnition":
            reason = last_tag_values.get(station_id, {}).get("S_DowntimeReasonOperator", "Unknown Reason")
            conn.execute("""
                INSERT INTO downtime_events(job_id, station_id, reason, start_ts, start_cst, end_ts, end_cst, duration_sec) 
                VALUES (?,?,?,?,?,?,?,?)
            """, (job_id, station_id, reason, start_ts, get_cst_str(start_ts), end_ts, get_cst_str(end_ts), duration))
        conn.commit()
    safe_print(f"[DB WRITE] {station_id} | Job {job_id} | Added {duration}s to '{col}'. Shifts affected: {list(splits.keys())}")

def _end_job(station_id, job_id, ts_epoch, st):
    active_sessions = st.setdefault('active_sessions', {})
    
    # for tag in list(active_sessions.keys()):
    #     start_ts = active_sessions.pop(tag)
    #     add_duration_to_db(station_id, job_id, tag, start_ts, ts_epoch)
    #     delete_runtime_state(station_id, 'active_sessions', tag)
    #     safe_print(f"[TIMER STOP] {station_id} | {tag} = False. Session closed with Job.")

    for tag in list(active_sessions.keys()):
        start_ts = active_sessions.pop(tag)
        add_duration_to_db(station_id, job_id, tag, start_ts, ts_epoch)
        delete_runtime_state(station_id, 'active_sessions', tag)
        safe_print(f"[TIMER STOP] {station_id} | {tag} = False. Session closed with Job.")
    

    conn = get_db()
    with db_lock:
        # --- DYNAMIC PLANNED MINS RECALCULATION ---
        # Fetch the locked final parts and speed from the database for this specific job
        cur = conn.cursor()
        cur.execute("SELECT total_parts, spm, planned_run_mins, day FROM jobs WHERE job_id = ?", (job_id,))
        row = cur.fetchone()
        
        if row:
            db_total_parts, db_spm, db_plan_mins, db_day = row
            parts_val = float(db_total_parts) if db_total_parts else 0.0
            spm_val = float(db_spm) if db_spm else 0.0
            current_plan_mins = float(db_plan_mins) if db_plan_mins else 0.0
            
            # Prevent division by zero
            if spm_val > 0.0:
                calculated_mins = parts_val / spm_val
            elif spm_val == 0.0 :
                calculated_mins = 0.0
                
            # If the calculated math differs from the PLC value (using a 0.01 tolerance for floating point math)
            if abs(calculated_mins - current_plan_mins) > 0.01:
                # Overwrite the jobs table
                conn.execute("UPDATE jobs SET planned_run_mins = ? WHERE job_id = ?", (calculated_mins, job_id))
                # Overwrite the daily aggregation totals
                conn.execute("UPDATE daily_agg SET planned_run_mins = ? WHERE day = ? AND station_id = ?", (calculated_mins, db_day, station_id))
                
                safe_print(f"[RECALC] {station_id} | Job {job_id} | Corrected Planned Mins from {current_plan_mins:.2f} to {calculated_mins:.2f} (Parts: {parts_val} / SPM: {spm_val})")
        # ------------------------------------------
        trigger_val = json.dumps({"type": "JOB_FINALIZED", "job_id": job_id})
        conn.execute("UPDATE jobs SET end_ts = ?, end_cst = ?, finalized=1 WHERE job_id=?", (ts_epoch, get_cst_str(ts_epoch), job_id))
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

        # day = get_shift_date_cst(ts_epoch)
        day = get_shift_date_cst(station_id, ts_epoch)
        cache = last_tag_values.get(station_id, {})
        morder, die, part = cache.get("MOrder"), cache.get("S_DieNumber1"), cache.get("PartNumber")
        spm, plan_qty, planned_mins = cache.get("N_DieSPM", 0.0), cache.get("Quantity", 0), cache.get("N_OperationPlannedRunMins", 0.0)

        ds_id = cache.get("IDBarCode_DS", "")
        op_id = cache.get("IDBarCode_OP", "")

        shift_letter = get_shift_for_station(station_id, ts_epoch)

        conn = get_db()
        with db_lock:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO jobs(station_id,day,shift, start_ts, start_cst, morder, die_number, part_number, spm, plan_qty, planned_run_mins,diesetter_id, operator_id) 
                VALUES (?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (station_id, day, shift_letter, ts_epoch, get_cst_str(ts_epoch), morder, die, part, spm, plan_qty, planned_mins,ds_id, op_id))
            new_job_id = cur.lastrowid
            conn.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (day, station_id))
            conn.commit()
        
        st['job_id'] = new_job_id

        st['is_continuation'] = False 
        save_runtime_state(station_id, 'system', 'is_continuation', "False") 
        # ---------------------------

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

        # --- THE CACHE BLEED FIX (UPGRADED TO WIPE ALL TAGS) ---
        tags_to_scrub = [
            "MOrder", "S_DieNumber1", "PartNumber", "Quantity", 
            "N_OperationPlannedRunMins", "N_RemainingPartsQty",
            "IDBarCode_DS", "IDBarCode_OP",
            "N_TotalProductionPartsQty", "N_GoodPartsTotalQty", 
            "N_ScrapPartsTotalQty", "N_DieSPM", "S_DowntimeReasonOperator"
        ]
        
        for t in tags_to_scrub:
            if t in last_tag_values.get(station_id, {}):
                # Ensure text tags reset to "", numeric tags reset to 0.0
                cleared_val = "" if any(word in t for word in ["Order", "Number", "ID", "Reason"]) else 0.0
                last_tag_values[station_id][t] = cleared_val
                
                delete_runtime_state(station_id, 'tag_cache', t)
                
        safe_print(f"[CACHE CLEARED] {station_id} | Job officially ended. ALL tags wiped for next totally new job.")
        # -------------------------------------------------------

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
            
            # ==========================================
            # --- NEW: DYNAMIC SHIFT ALIGNMENT LOGIC ---
            # ==========================================
            if tag == "B_OperatorInProgressForIgnition":
                new_day = get_shift_date_cst(station_id, ts_epoch)
                new_shift = get_shift_for_station(station_id, ts_epoch)
                
                conn = get_db()
                with db_lock:
                    # Update the job to reflect the exact moment the Operator logged in
                    conn.execute("UPDATE jobs SET day = ?, shift = ? WHERE job_id = ?", (new_day, new_shift, current_job_id))
                    # Ensure the day row exists in daily_agg
                    conn.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (new_day, station_id))
                    conn.commit()
                    
                safe_print(f"[SHIFT ALIGNMENT] {station_id} | Job {current_job_id} reassigned to Shift {new_shift} for {new_day} based on Operator Start.")
            # ==========================================

            db_col_epoch = "min_op_start" if tag == "B_OperatorInProgressForIgnition" else "min_co_start" if tag == "B_DieSetterInProgressForIgnition" else None
            db_col_cst = "min_op_start_cst" if tag == "B_OperatorInProgressForIgnition" else "min_co_start_cst" if tag == "B_DieSetterInProgressForIgnition" else None
            
            if db_col_epoch:
                conn = get_db()
                with db_lock:
                    conn.execute(f"UPDATE jobs SET {db_col_cst} = COALESCE({db_col_cst}, ?), {db_col_epoch} = COALESCE({db_col_epoch}, ?) WHERE job_id = ?", (get_cst_str(ts_epoch), ts_epoch, current_job_id))
                    conn.commit()
                safe_print(f"[DB WRITE] {station_id} | Job {current_job_id} | Set {db_col_epoch} = {ts_epoch}")
        elif current_job_id:
            safe_print(f"[WARNING] {station_id} ignored {tag}=True because no Job is active.")
                    
    else:
        if tag in active_sessions:
            start_ts = active_sessions.pop(tag)
            add_duration_to_db(station_id, current_job_id, tag, start_ts, ts_epoch)
            delete_runtime_state(station_id, 'active_sessions', tag)
            safe_print(f"[TIMER STOP] {station_id} | {tag} = False. Session duration calculated.")
            
# def process_session_flag(station_id, tag, val_bool, ts_epoch):
#     st = live.setdefault(station_id, {})
#     raw_tags = st.setdefault('raw_tags', {})
#     active_sessions = st.setdefault('active_sessions', {})
#     current_job_id = st.get('job_id')

#     if raw_tags.get(tag) == val_bool: return 
#     raw_tags[tag] = val_bool
#     save_runtime_state(station_id, 'raw_tags', tag, val_bool)

#     if val_bool:
#         if current_job_id and tag not in active_sessions:
#             active_sessions[tag] = ts_epoch
#             save_runtime_state(station_id, 'active_sessions', tag, ts_epoch)
#             safe_print(f"[TIMER START] {station_id} | Job {current_job_id} | {tag} = True")
            
#             db_col_epoch = "min_op_start" if tag == "B_OperatorInProgressForIgnition" else "min_co_start" if tag == "B_DieSetterInProgressForIgnition" else None
#             db_col_cst = "min_op_start_cst" if tag == "B_OperatorInProgressForIgnition" else "min_co_start_cst" if tag == "B_DieSetterInProgressForIgnition" else None
            
#             if db_col_epoch:
#                 conn = get_db()
#                 with db_lock:
#                     conn.execute(f"UPDATE jobs SET {db_col_cst} = COALESCE({db_col_cst}, ?), {db_col_epoch} = COALESCE({db_col_epoch}, ?) WHERE job_id = ?", (get_cst_str(ts_epoch), ts_epoch, current_job_id))
#                     conn.commit()
#                 safe_print(f"[DB WRITE] {station_id} | Job {current_job_id} | Set {db_col_epoch} = {ts_epoch}")
#         elif current_job_id:
#             safe_print(f"[WARNING] {station_id} ignored {tag}=True because no Job is active.")
                    
#     else:
#         if tag in active_sessions:
#             start_ts = active_sessions.pop(tag)
#             add_duration_to_db(station_id, current_job_id, tag, start_ts, ts_epoch)
#             delete_runtime_state(station_id, 'active_sessions', tag)
#             safe_print(f"[TIMER STOP] {station_id} | {tag} = False. Session duration calculated.")


def process_absolute_or_snapshot(station_id, tag, value, ts_epoch):
    
    st = live.setdefault(station_id, {})
    job_id = st.get('job_id')
    # --- GET THE SWITCH STATE ---
    is_cont = st.get('is_continuation', False)
    
    col_map = {
        "N_OperationPlannedRunMins": "planned_run_mins", 
        "N_TotalProductionPartsQty": "total_parts", 
        "N_GoodPartsTotalQty": "good_parts",
        "N_ScrapPartsTotalQty": "scrap_parts", 
        "MOrder": "morder", "S_DieNumber1": "die_number",
        "PartNumber": "part_number", "N_DieSPM": "spm",
        "IDBarCode_DS": "diesetter_id", "IDBarCode_OP": "operator_id" # <--- ADDED HERE
    }

    # --- DYNAMIC ROUTING FOR PLAN QUANTITY ---
    if is_cont:
        col_map["N_RemainingPartsQty"] = "plan_qty" # On shift splits, route Remaining Parts to DB
    else:
        col_map["Quantity"] = "plan_qty"            # On new jobs, route Quantity to DB
    # -----------------------------------------

    
    # ==========================================
    # --- THE STRICT "GREATER THAN" FIREWALL ---
    # ==========================================
    
    # 1. Protect Strings from empty teardown wipes
    if tag in ("MOrder", "S_DieNumber1", "PartNumber","IDBarCode_DS", "IDBarCode_OP"):
        if value is None or str(value).strip() == "":
            return 
            
    # 2. Strict Numeric Rules
    if tag in ("N_OperationPlannedRunMins", "N_TotalProductionPartsQty", "N_GoodPartsTotalQty", "N_ScrapPartsTotalQty", "N_DieSPM", "Quantity", "N_RemainingPartsQty"):
        if value is None or str(value).strip().lower() == "none":
            return
            
        try:
            val_float = float(value)
            
            # # RULE A: Remaining Parts must be strictly > 1 (Blocks 1, 0, and Negatives)
            # if tag == "N_RemainingPartsQty" and val_float <= 1.0:
            #     safe_print(f"[FIREWALL] {station_id} | Blocked {tag} <= 1: {val_float}")
            #     return

            # RULE B: Parts, Quantity, and Planned Mins must be strictly > 0 (Blocks 0 and Negatives permanently)
            if tag in ("N_GoodPartsTotalQty", "N_ScrapPartsTotalQty", "N_TotalProductionPartsQty", "Quantity", "N_OperationPlannedRunMins","N_RemainingPartsQty"):
                if val_float <= 0.0:
                    safe_print(f"[FIREWALL] {station_id} | Blocked {tag} <= 0: {val_float}")
                    return

            # RULE C: SPM (Speed) is the only tag allowed to be 0, but ONLY if the machine is idle
            if tag == "N_DieSPM":
                if val_float < 0.0:
                    return # Block negatives
                if val_float == 0.0 and job_id is not None:
                    return # Block 0 if a job is currently running
                    
        except (ValueError, TypeError):
            return 
    # ==========================================
    
    # If it survives the firewall, update memory cache
    last_tag_values.setdefault(station_id, {})[tag] = value
    
    # Backup the safe tag to the hard drive
    conn = get_db()
    with db_lock:
        conn.execute("""
            INSERT OR REPLACE INTO runtime_state (station_id, category, key, value, updated_ts)
            VALUES (?, 'tag_cache', ?, ?, ?)
        """, (station_id, tag, str(value), ts_epoch))
        conn.commit()
    
    # If no active job or tag isn't mapped to a column, stop here
    if tag not in col_map or not job_id: return
    
    col = col_map[tag]

    # Live Database Updates
    with db_lock:
        conn.execute(f"UPDATE jobs SET {col} = ? WHERE job_id = ?", (value, job_id))
        
        # Keep daily_agg totals in sync
        if col in ("planned_run_mins", "good_parts", "scrap_parts", "total_parts", "diesetter_id", "operator_id"):
            # day = get_shift_date_cst(ts_epoch)
            day = get_shift_date_cst(station_id, ts_epoch)
            conn.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (day, station_id))
            conn.execute(f"UPDATE daily_agg SET {col} =  {col} + ? WHERE day=? AND station_id=?", (value, day, station_id))
            
        conn.commit()

    safe_print(f"[LIVE UPDATE] {station_id} | Job {job_id} | '{col}' safely updated to: {value}")

def active_heartbeat_worker():
    """Actively writes a heartbeat every 60 seconds for all active stations so recovery math is always accurate."""
    safe_print("[SYSTEM] Active heartbeat thread started.")
    time.sleep(5)

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



    # --- REBOOT-PROOF: FETCH TRUTH DIRECTLY FROM THE DATABASE ---
    conn = get_db()
    with db_lock:
        cur = conn.cursor()
        cur.execute("""
            SELECT morder, die_number, part_number, spm, planned_run_mins, plan_qty , diesetter_id, operator_id
            FROM jobs WHERE job_id = ?
        """, (current_job_id,))
        row = cur.fetchone()
        
    if row:
        db_morder, db_die, db_part, db_spm, db_planned_mins, db_qty, db_ds_id, db_op_id = row
    else:
        db_morder, db_die, db_part, db_spm, db_planned_mins, db_qty, db_ds_id, db_op_id = ("", "", "", 0.0, 0.0, 0, "", "")

    # 1. End current job segment (Triggers Reporting Engine for the old shift)
    # NOTE: _end_job automatically closes active downtime. It accurately uses the current 
    # S_DowntimeReasonOperator from last_tag_values, exactly matching live_state_engine's logic!

    _end_job(station_id, current_job_id, ts_epoch, st)

    # 2. Start the new job segment for the new shift
    # Operators might trigger the split 30 mins early or 30 mins late. 
    # To guarantee we get the correct Day and Shift letter for the ONCOMING crew, 
    # we mathematically look 2 hours (+7200s) into the future to grab the text labels!
    # (The actual start_ts for the job remains perfectly exact to the millisecond).
    oncoming_epoch = ts_epoch + 7200
    # day = get_shift_date_cst(oncoming_epoch)
    day = get_shift_date_cst(station_id, oncoming_epoch)
    shift_letter = get_shift_for_station(station_id, oncoming_epoch)
    cache = last_tag_values.get(station_id, {})
    
# --- THE FIX: Use Cache as primary truth, fall back to DB if cache is missing/empty ---
    morder = cache.get("MOrder") or db_morder or ""
    die = cache.get("S_DieNumber1") or db_die or ""
    part = cache.get("PartNumber") or db_part or ""
    spm = cache.get("N_DieSPM") or db_spm or 0.0
    planned_mins = cache.get("N_OperationPlannedRunMins") or db_planned_mins or 0.0

    ds_id = cache.get("IDBarCode_DS") or db_ds_id or ""
    op_id = cache.get("IDBarCode_OP") or db_op_id or ""
    
    # # Capture RemainingPartsQty for the new Shift's Plan Qty
    # plan_qty = cache.get("N_RemainingPartsQty", cache.get("Quantity", 0))


    # --- ANTI-WIPE LOGIC FOR SHIFT SPLIT ---
    raw_rem = cache.get("N_RemainingPartsQty")
    raw_qty = cache.get("Quantity") or db_qty or 0
    
    try: rem_val = float(raw_rem) if raw_rem is not None else 0.0
    except ValueError: rem_val = 0.0
        
    try: qty_val = float(raw_qty)
    except ValueError: qty_val = 0.0

    # If Remaining Parts is valid (> 0), use it. 
    # If it is 0 or negative (like the PLC reset codes), fall back to Quantity!
    if rem_val > 0.0:
        plan_qty = rem_val
        safe_print(f"[SHIFT SPLIT] {station_id} | N_RemainingPartsQty is {rem_val}. Using RemainingPartsQty as plan for new shift.")
    else:
        plan_qty = qty_val
        safe_print(f"[SHIFT SPLIT] {station_id} | N_RemainingPartsQty was {rem_val}. Fell back to Quantity: {qty_val}")    # ---------------------------------------

    conn = get_db()
    with db_lock:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO jobs(station_id, day, shift, start_ts,start_cst, morder, die_number, part_number, spm, plan_qty, planned_run_mins,diesetter_id, operator_id) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (station_id, day, shift_letter, ts_epoch, get_cst_str(ts_epoch), morder, die, part, spm, plan_qty, planned_mins,ds_id, op_id))
        new_job_id = cur.lastrowid
        conn.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (day, station_id))
        conn.commit()
    
    st['job_id'] = new_job_id

    st['is_continuation'] = True 
    save_runtime_state(station_id, 'system', 'is_continuation', "True") 
    # ---------------------------

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
            db_col_cst = "min_op_start_cst" if tag == "B_OperatorInProgressForIgnition" else "min_co_start_cst" if tag == "B_DieSetterInProgressForIgnition" else None
            if db_col:
                with db_lock:
                    conn.execute(f"UPDATE jobs SET {db_col} = ?, {db_col_cst} = ? WHERE job_id = ?", (ts_epoch, get_cst_str(ts_epoch),new_job_id))
                    conn.commit()

def worker_fn():
    safe_print("[SYSTEM] Worker thread started and processing MQTT queue.")
    while not shutdown_event.is_set():
        try: payload = message_queue.get(timeout=1.0)
        except Empty: continue
        try:
            tag, station, val = payload.get("tagName"), payload.get("stationID"), payload.get("details")
            
            
            if tag not in ALLOWED_TAGS: continue 

            ts_epoch = int(payload.get("timestamp", time.time()))
            
            # 1. Logic Routing
            if tag == "S_DowntimeReasonOperator": 
                reason_str = str(val).strip()
                last_tag_values.setdefault(station, {})[tag] = reason_str
                save_runtime_state(station, 'last_known_reason', 'S_DowntimeReasonOperator', reason_str)
                safe_print(f"[REASON UPDATED] {station} | Operator set downtime reason to: '{reason_str}'")
                
                st = live.setdefault(station, {})
                job_id = live.get(station, {}).get('job_id')

                is_currently_down = st.setdefault('raw_tags', {}).get("B_DowntimeInProgressForIgnition", False)

                # ONLY update the DB if the machine is running (Retroactive update).
                # If the machine is currently down, we skip the DB update because the 
                # new reason is safely waiting in memory for the 'DT=False' INSERT!
                if job_id and reason_str and not is_currently_down:
                    conn = get_db()
                    with db_lock:
                        conn.execute("""
                            UPDATE downtime_events SET reason = ? WHERE event_id = (
                                SELECT event_id FROM downtime_events WHERE station_id = ? AND job_id = ? ORDER BY event_id DESC LIMIT 1
                            )
                        """, (reason_str, station, job_id))
                        conn.commit()
            # --- NEW TAG LISTENER ---
            elif tag == "N_StrokeCount":
                # try: val_float = float(val)
                # except: val_float = 0.0
                # last_tag_values.setdefault(station, {})[tag] = val_float
                try: val_float = float(val)
                except: val_float = 0.0
                
                # FLAW 3 FIXED: Capture the exact physical PLC timestamp ONLY when a part is actually made!
                if last_tag_values.get(station, {}).get(tag) != val_float:
                    last_stroke_times[station] = ts_epoch 
                    
                last_tag_values.setdefault(station, {})[tag] = val_float

            # --- UPDATED SHIFT END LOGIC ---
            elif tag == "B_EndOfShiftForIgnition":
                is_end_of_shift = str(val).lower() in ("true", "1", "yes", "y")
                st = live.setdefault(station, {})
                raw_tags = st.setdefault('raw_tags', {})
                raw_tags[tag] = is_end_of_shift
                save_runtime_state(station, 'raw_tags', tag, is_end_of_shift)
                
                if is_end_of_shift:
                    if st.get('zombie_closed', False):
                        safe_print(f"[WEEKDAY WAKEUP] {station} | Late End-of-Shift pressed. Resuming weekend job as a Shift Split.")
                        st['zombie_closed'] = False
                        save_runtime_state(station, 'system', 'zombie_closed', "False")
                        resume_zombie_job_as_split(station, ts_epoch)
                    else:
                        st['shift_split_armed'] = True
                        save_runtime_state(station, 'system', 'shift_split_armed', "True")
                        safe_print(f"[SHIFT SPLIT] {station} | End of Shift screen opened. Armed for Press Stop OR New Operator.")
                        
            # elif tag == "B_EndOfShiftForIgnition":
            #     is_end_of_shift = str(val).lower() in ("true", "1", "yes", "y")
            #     st = live.setdefault(station, {})
            #     raw_tags = st.setdefault('raw_tags', {})
                
            #     # We no longer trigger the split here. We just use this to "Open the Window".
            #     raw_tags[tag] = is_end_of_shift
            #     save_runtime_state(station, 'raw_tags', tag, is_end_of_shift)
                
            #     if is_end_of_shift:
            #         st['shift_split_armed'] = True
            #         save_runtime_state(station, 'system', 'shift_split_armed', "True")
            #         safe_print(f"[SHIFT SPLIT] {station} | End of Shift screen opened. Armed for Press Stop OR New Operator.")

            elif tag == "PB_PressState_Fixed":
                # Convert the incoming value to a boolean
                is_fixed_active = str(val).lower() in ("true", "1", "yes", "y")
                
                st = live.setdefault(station, {})
                raw_tags = st.setdefault('raw_tags', {})
                
                # --- THE NEW TRIGGER: THE PLC CONFIRMATION ---
                # If the PLC drives state to False (0) AND the operator's End of Shift window is open
                if not is_fixed_active and st.get('shift_split_armed', False):
                    safe_print(f"[SHIFT SPLIT] {station} | Press State dropped. Executing armed shift split.")
                    process_shift_end_split(station, ts_epoch)
                    
                    # Instantly close the window internally so operator "Back" button spam cannot trigger a double-split
                    st['shift_split_armed'] = False
                    save_runtime_state(station, 'system', 'shift_split_armed', "False")
            # --- UPDATED OPERATOR ID LOGIC ---
            elif tag == "IDBarCode_OP":
                val_str = str(val).strip()
                st = live.setdefault(station, {})
                current_op = last_tag_values.get(station, {}).get("IDBarCode_OP", "")
                
                if st.get('zombie_closed', False) and val_str and val_str != current_op:
                    safe_print(f"[WEEKDAY WAKEUP] {station} | New Operator {val_str} scanned in. Resuming weekend job as a Shift Split.")
                    st['zombie_closed'] = False
                    save_runtime_state(station, 'system', 'zombie_closed', "False")
                    resume_zombie_job_as_split(station, ts_epoch, new_operator_id=val_str)
                
                elif st.get('shift_split_armed', False) and val_str and val_str != current_op:
                    safe_print(f"[SHIFT SPLIT FALLBACK] {station} | New Operator {val_str} logged in. Force-executing armed shift split.")
                    process_shift_end_split(station, ts_epoch)
                    st['shift_split_armed'] = False
                    save_runtime_state(station, 'system', 'shift_split_armed', "False")
                
                process_absolute_or_snapshot(station, tag, val_str, ts_epoch)
            # elif tag == "IDBarCode_OP":
            #     val_str = str(val).strip()
            #     st = live.setdefault(station, {})
            #     current_op = last_tag_values.get(station, {}).get("IDBarCode_OP", "")
                
            #     # --- TRIGGER 2: THE FALLBACK EXTRA LAYER ---
            #     # If the split was armed, but we missed the Press State drop, 
            #     # the new operator scanning in will force the split!
            #     if st.get('shift_split_armed', False) and val_str and val_str != current_op:
            #         safe_print(f"[SHIFT SPLIT FALLBACK] {station} | New Operator {val_str} logged in. Force-executing armed shift split.")
            #         process_shift_end_split(station, ts_epoch)
                    
            #         # Disarm immediately
            #         st['shift_split_armed'] = False
            #         save_runtime_state(station, 'system', 'shift_split_armed', "False")
                
            #     # Continue to save the new operator ID normally
            #     process_absolute_or_snapshot(station, tag, val_str, ts_epoch)
            elif tag == "B_JobInProgressForIgnition":
                process_job_flag(station, str(val).lower() in ("true", "1", "yes", "y"), ts_epoch)
            elif tag in ("B_OperatorInProgressForIgnition","B_DieSetterInProgressForIgnition", "B_DowntimeInProgressForIgnition"):
                process_session_flag(station, tag, str(val).lower() in ("true", "1", "yes", "y"), ts_epoch)
            else:
                try:
                    if any(x in tag for x in ["Qty", "Mins", "SPM", "Quantity"]): val = float(val)
                except: pass
                process_absolute_or_snapshot(station, tag, val, ts_epoch)
        except Exception as e:
            safe_print(f"[CRITICAL WORKER ERROR] Failed processing tag {payload.get('tagName')} for {payload.get('stationID')}: {str(e)}")

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
        
        cur.execute("SELECT station_id, key, value FROM runtime_state WHERE category='system' AND key IN ('is_continuation', 'shift_split_armed')")
        for sid, key, val_str in cur.fetchall():
            live.setdefault(sid, {})[key] = (val_str == "True")
        # ---------------------------------------------------------
        # --------------------------------------------

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
    # CALL IT IMMEDIATELY AT STARTUP
    load_persistent_tag_cache()
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
    Thread(target=stroke_count_watchdog, daemon=True).start()
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