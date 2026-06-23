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

# -------------------------------------------------
# Shift & Boundary Math (Dynamic from Config)
# -------------------------------------------------
def get_factory_day_start_time(station_id):
    """Dynamically reads the config to find the exact start time of the production day."""
    from datetime import time as tm
    default_time = tm(6, 0)
    
    if not station_id or not CONFIG: return default_time
    try:
        station_cfg = CONFIG.get("stations", {}).get(station_id, {})
        shift_group = station_cfg.get("shifts")
        if not shift_group: return default_time
        
        group = CONFIG.get("shifts", {}).get(shift_group, {})
        if not group: return default_time
        
        first_shift_name = list(group.keys())[0]
        start_str = group[first_shift_name].get("start", "06:00")
        parsed = parse_hhmm(start_str)
        return parsed if parsed else default_time
    except Exception:
        return default_time

def get_shift_date_cst(station_id, ts_epoch):
    dt = datetime.fromtimestamp(ts_epoch, CENTRAL_TZ)
    day_start = get_factory_day_start_time(station_id)
    
    if dt.time() < day_start: 
        dt = dt - timedelta(days=1)
        
    return dt.strftime("%Y-%m-%d")

def get_shift_splits(station_id, start_ts, end_ts):
    splits = set()
    curr_ts = start_ts
    end_ts = end_ts if end_ts else start_ts 
    day_start = get_factory_day_start_time(station_id)

    if start_ts == end_ts:
        splits.add(get_shift_date_cst(station_id, start_ts))
        return splits
    
    while curr_ts < end_ts:
        splits.add(get_shift_date_cst(station_id, curr_ts))
        curr_dt = datetime.fromtimestamp(curr_ts, CENTRAL_TZ)
        
        if curr_dt.time() >= day_start:
            next_boundary_dt = datetime.combine(curr_dt.date() + timedelta(days=1), day_start, tzinfo=CENTRAL_TZ)
        else:
            next_boundary_dt = datetime.combine(curr_dt.date(), day_start, tzinfo=CENTRAL_TZ)
            
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

# -------------------------
# SHIFT & BREAK LOGIC (from config)
# -------------------------
def get_shift_for_station(station_id, ts_epoch=None):
    if ts_epoch:
        dt = datetime.fromtimestamp(ts_epoch, CENTRAL_TZ)
    else:
        dt = now_central()
        
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
    except Exception as e:
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
                    COALESCE(total_parts, 0), -- <--- NEW COLUMN RETRIEVED
                    min_op_start, 
                    max_op_end, 
                    min_co_start, 
                    max_co_end, 
                    COALESCE(morder, ''), 
                    COALESCE(die_number, ''), 
                    COALESCE(part_number, ''), 
                    COALESCE(spm, 0.0), 
                    COALESCE(plan_qty, 0), 
                    shift,
                    COALESCE(diesetter_id, ''),
                    COALESCE(operator_id, ''),
                    day   -- <--- NEW COLUMN
                FROM jobs WHERE job_id=?""", (job_id,))
        
        r = cur.fetchone()

        if not r: 
            conn.close(); return [], 0, 0, 0, set(), ""

        (station, start_ts, end_ts, op_sec, co_sec, dt_sec, plan_mins, good, scrap, total_parts_db,
         min_op, max_op, min_co, max_co, morder, die, part, spm, plan_qty, shift_letter,ds_id, op_id, primary_day) = r

        cur.execute("SELECT reason, SUM(duration_sec) FROM downtime_events WHERE job_id=? GROUP BY reason", (job_id,))
        dt_rows = cur.fetchall()
        conn.close()

    # Force conversion to float to handle potential string/None issues from DB
    RT = (float(op_sec) if op_sec else 0.0) / 60.0
    CT = (float(co_sec) if co_sec else 0.0) / 60.0
    DT = (float(dt_sec) if dt_sec else 0.0) / 60.0
    plan_mins_f = float(plan_mins) if plan_mins else 0.0

    denom = RT + CT
    OA = (plan_mins_f / denom) if denom > 0 else 0.0
    
    # --- NEW QUALITY MATH ---
    # The 'good' variable from the DB actually holds our N_TotalProductionPartsQty now
    total_production_f = float(total_parts_db) if total_parts_db else 0.0
    good_f = float(good) if good else 0.0
    scrap_f = float(scrap) if scrap else 0.0
    
    # Formula: (Total - Scrap) / Total
    QR = (total_production_f - scrap_f) / total_production_f if total_production_f > 0 else 0.0
    
    end_ts_safe = end_ts if end_ts else int(time.time())
    # split_days = get_shift_splits(start_ts, end_ts_safe)
    # primary_day = get_shift_date_cst(start_ts)
    # split_days = get_shift_splits(station, start_ts, end_ts_safe)
    # primary_day = get_shift_date_cst(station, start_ts)

    dt_summary = {}
    for reason, duration_sec in dt_rows:
        r_clean = str(reason).strip() if reason else "Unknown Reason"
        dt_summary[r_clean] = dt_summary.get(r_clean, 0.0) + (duration_sec / 60.0)

    data = {
        "ShiftDateCST": primary_day,
        "Shift": shift_letter, # Fetched directly from the new jobs table column!
        "Asset": station,
        "MOrder": morder, "DieNumber": die, "PartNumber": part, "SPM": spm, "PlanQty": plan_qty,
        "DieSetterID": ds_id,  # <--- NEW MAPPING
        "OperatorID": op_id,   # <--- NEW MAPPING
        "OA": OA, "Quality": QR,
        "OperatorMinutes": RT, "ChangeoverMinutes": CT, "DowntimeMinutes": DT,
        "GoodQty": good_f, "ScrapQty": scrap_f,"TotalProductionQty" : total_production_f, # Include total production for clarity
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
    
    
    return payloads, OA, QR, 0.0, primary_day, station

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
                    payloads, oa, qr, or_val, primary_day, stn = build_job_summary(data.get("job_id"))
                    
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
                                # for day_str in split_days:
                                cur.execute("INSERT OR IGNORE INTO daily_agg(day, station_id) VALUES (?,?)", (primary_day, stn))
                                cur.execute("""
                                    UPDATE daily_agg
                                    SET oa_sum = oa_sum + ?,  oa_count = oa_count + 1, 
                                        qr_sum = qr_sum + ?, qr_count = qr_count + 1,
                                        or_sum = or_sum + ?, or_count = or_count + 1
                                    WHERE day=? AND station_id=?
                                """, (oa, qr, or_val, primary_day, stn))
                                    
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
    safe_print("[REPORTING] Daily Loop started (Strict Daily OR = 1270 mins)")
    
    # Track the last published date in memory per station
    last_run_date = {}

    while not shutdown_event.is_set():
        now_dt = now_central()
        current_date_str = now_dt.strftime("%Y-%m-%d")
        
        stations = CONFIG.get("stations", {})
        
        for station_id in stations.keys():
            # 1. Grab this exact station's start time (e.g., 06:15 or 08:00)
            day_start = get_factory_day_start_time(station_id)
            
            # 2. Add a 60-minutes buffer to allow edge database locks to safely finish and end of shift should happen properly by the operator at HMI
            target_minute = day_start.minute + 60
            target_hour = day_start.hour
            if target_minute >= 60:
                target_hour += 1
                target_minute -= 60
                
            target_time = tm(target_hour, target_minute)
            
            # 3. Check if we have crossed the station's target time
            if now_dt.time() >= target_time:
                
                # --- 3b. THE MEMORY SHIELD ---
                if last_run_date.get(station_id) == current_date_str:
                    continue
                
                # Look back 24h to calculate the summary for the factory day that just ended
                yesterday_ts = int(now_dt.timestamp()) - 86400
                target_day = get_shift_date_cst(station_id, yesterday_ts)
                
                # 4. Check the Database just to be absolutely sure
                run_needed = False
                with db_lock:
                    conn = sqlite3.connect(DB_PATH)
                    cur = conn.cursor()
                    cur.execute("SELECT value FROM runtime_state WHERE station_id=? AND category='reporting' AND key='last_daily_run'", (station_id,))
                    row = cur.fetchone()
                    if not row or row[0] != target_day: 
                        run_needed = True
                    conn.close()

                if run_needed:
                    safe_print(f"[DAILY] Running Daily Summary for {station_id} -> {target_day}")
                    with db_lock:
                        conn = sqlite3.connect(DB_PATH)
                        cur = conn.cursor()
                        
                        # --- NEW: Fetch exact SUMS directly from the jobs table for the day ---
                        cur.execute("""
                        SELECT 
                            SUM(operator_sec), 
                            SUM(diesetter_sec),
                            SUM(planned_run_mins),
                            SUM((operator_sec/60)- planned_run_mins), 
                            SUM(total_parts), 
                            SUM(scrap_parts)
                        FROM jobs WHERE day=? AND station_id=? AND finalized=1
                        """, (target_day, station_id))
                        row = cur.fetchone()
                        conn.close()

                    if row and row[0] is not None:
                        # Extract the daily sums
                        sum_operator_sec = row[0] or 0.0
                        sum_changeover_sec = row[1] or 0.0
                        sum_planned_run_mins = row[2] or 0.0
                        sum_downtime_mins = row[3] or 0.0
                        sum_total_parts = row[4] or 0.0
                        sum_scrap_parts = row[5] or 0.0
                        
                        # Convert seconds to minutes for the math
                        sum_operator_mins = float(sum_operator_sec) / 60.0
                        sum_changeover_mins = float(sum_changeover_sec) / 60.0
                        
                        # ==========================================
                        # CALCULATE AVERAGE OA (Weighted)
                        # ==========================================
                        # Formula: Planned Run Mins / (Operator Mins + Changeover Mins)
                        denom = sum_operator_mins + sum_changeover_mins
                        if denom > 0:
                            avg_OA = sum_planned_run_mins / denom
                        else:
                            avg_OA = 0.0

                        # ==========================================
                        # CALCULATE AVERAGE QR (Weighted)
                        # ==========================================
                        # Formula: (Total Parts - Scrap Parts) / Total Parts
                        if sum_total_parts > 0:
                            avg_QR = (sum_total_parts - sum_scrap_parts) / sum_total_parts
                        else:
                            avg_QR = 1.0  # Default to 100% quality if no parts run
                            
                        # ==========================================
                        # CALCULATE OR (Operating Rate)
                        # ==========================================
                        # Your Formula: (Operator Mins - Downtime Mins) / 1270
                        actual_mins_calc = sum_operator_mins - sum_downtime_mins
                        daily_OR = actual_mins_calc / 1270.0
                        if daily_OR < 0: daily_OR = 0.0

                        # ==========================================
                        # Final OEE
                        # ==========================================
                        OEE_daily = avg_OA * avg_QR * daily_OR
                        
                        shift = get_shift_for_station(station_id, int(now_dt.timestamp()))
                        timestamp_block = {"timeInSeconds": int(time.time())}

                        # Publish to SiteWise
                        payload_json = json.dumps({
                            "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station_id}/DailySummary",
                            "propertyValues": [{"timestamp": timestamp_block, "quality": "GOOD", "value": {"stringValue": json.dumps({"ShiftDateCST": target_day, "OR": daily_OR, "OEE": OEE_daily})}}]
                        })
                        payload_oee = json.dumps({
                            "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station_id}/OEEPerDay",
                            "propertyValues": [{"timestamp": timestamp_block, "quality": "GOOD", "value": {"stringValue": f"{float(OEE_daily)}:{shift}"}}]
                        })
                        payload_or = json.dumps({
                            "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station_id}/UtilizationPerDay",
                            "propertyValues": [{"timestamp": timestamp_block, "quality": "GOOD", "value": {"stringValue": f"{float(daily_OR)}:{shift}"}}]
                        })

                        all_published = True
                        for p in [payload_json, payload_oee, payload_or]:
                            if not publish_with_ack(p):
                                all_published = False

                        if all_published:
                            with db_lock:
                                conn = sqlite3.connect(DB_PATH)
                                # Update daily_agg just for historical record keeping
                                conn.execute("""
                                    UPDATE daily_agg 
                                    SET final_oee = ?, final_oa = ?, final_or = ?, final_qr = ?
                                    WHERE day = ? AND station_id = ?
                                """, (OEE_daily, avg_OA, daily_OR, avg_QR, target_day, station_id))
                                
                                conn.execute("""
                                    INSERT INTO runtime_state(station_id, category, key, value, updated_ts)
                                    VALUES (?, 'reporting', 'last_daily_run', ?, ?)
                                    ON CONFLICT(station_id, category, key) DO UPDATE SET value=excluded.value, updated_ts=excluded.updated_ts
                                """, (station_id, target_day, int(time.time())))
                                conn.commit(); conn.close()
                            
                            # Lock it in memory
                            last_run_date[station_id] = current_date_str
                            safe_print(f"[DAILY] {station_id} Successfully Published for {target_day}")
                    else:
                        # Machine was off all day, skip and mark as done
                        with db_lock:
                            conn = sqlite3.connect(DB_PATH)
                            conn.execute("""
                                INSERT INTO runtime_state(station_id, category, key, value, updated_ts)
                                VALUES (?, 'reporting', 'last_daily_run', ?, ?)
                                ON CONFLICT(station_id, category, key) DO UPDATE SET value=excluded.value, updated_ts=excluded.updated_ts
                            """, (station_id, target_day, int(time.time())))
                            conn.commit(); conn.close()
                        
                        last_run_date[station_id] = current_date_str
                        safe_print(f"[DAILY] {station_id} had no data for {target_day}. Skipped and marked as done.")
                            
        # --- Run Database Maintenance globally once per cycle ---
        try:
            with db_lock:
                conn = sqlite3.connect(DB_PATH)
                thirty_days_ago = "strftime('%s', 'now', '-30 days')"
                thirty_days_date = "date('now', '-30 days')"
                conn.execute(f"DELETE FROM jobs WHERE end_ts IS NOT NULL AND end_ts < {thirty_days_ago}")
                conn.execute(f"DELETE FROM downtime_events WHERE end_ts IS NOT NULL AND end_ts < {thirty_days_ago}")
                conn.execute(f"DELETE FROM daily_agg WHERE day < {thirty_days_date}")
                conn.commit(); conn.close()
        except: pass

        shutdown_event.wait(60)

def shift_monitor_loop():
    safe_print("[SHIFT MONITOR] Started")
    last_shift = {}
    last_shift_date = {}

    while not shutdown_event.is_set():
        now_ts = int(time.time())
        stations = CONFIG.get("stations", [])
        
        for station in stations:
            # We now evaluate the date and shift PER STATION based on its config!
            current_date = get_shift_date_cst(station, now_ts)
            current_shift = get_shift_for_station(station, now_ts)
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

if __name__ == "__main__":
    main()