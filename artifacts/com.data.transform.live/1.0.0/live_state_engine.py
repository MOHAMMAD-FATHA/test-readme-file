#!/usr/bin/env python3
"""
Component 1: Live Data, Machine State, Call Buttons, and Downtime Reasons.
No OEE, Job, or Utilization calculations.
"""

import json
import time
import random
import signal
import os
import sqlite3
from datetime import datetime, time as tm, timezone, timedelta
from threading import Thread, Lock, Event
from queue import Queue, Empty, Full
import paho.mqtt.client as mqtt_client
from zoneinfo import ZoneInfo

# -------------------------
# Central Time Zone
# -------------------------
CENTRAL_TZ = ZoneInfo("America/Chicago")

# -------------------------
# CONFIGURATION
# -------------------------
CONFIG_PATH = os.getenv('CONFIG_PATH')
DB_PATH = os.getenv('DB_PATH')  # DB just for state recovery now
DB_BACKUP_DIR = os.getenv('DB_BACKUP_DIR')
SITEWISE_MODEL_NAME = os.getenv('SITEWISE_MODEL_NAME')

BROKER = "127.0.0.1"
PORT = 1883
SUB_TOPIC = os.getenv("SUB_TOPIC")
PUB_TOPIC = "python/mqtt"
CLIENT_ID = f'python-mqtt-live-{random.randint(0, 10000)}'

# Queues and limits
MESSAGE_QUEUE_MAXSIZE = 5000
PUBLISH_QUEUE_MAXSIZE = 2000
PUBLISH_RETRY_MAX_ATTEMPTS = 5
PUBLISH_RETRY_BASE_DELAY = 1.0

# Watchdog interval
WORKER_WATCHDOG_INTERVAL = 10

# Shutdown flags
shutdown_flag = False
shutdown_event = Event()

# Global runtime state
mqtt_connected = False
mqtt_client_instance = None
previous_station_status = {}
CONFIG = {"stations": {}}
CONFIG_MTIME = None

MACHINE_STATE_TAGS = set([
    "B_JobInProgressForIgnition",
    "B_DowntimeInProgressForIgnition",
    "B_DieSetterInProgressForIgnition",
    "B_OperatorInProgressForIgnition",
    "N_DieSPM",
    "PartsPerMinute"
])

CALL_BUTTON_TAGS = set([
    "PB_Call_DieSetter", "PB_Call_DieShop", "PB_Call_Forklift",
    "PB_Call_Maintenance", "PB_Call_Material", "PB_Call_Quality", "PB_Call_Supervisor"
])

call_button_states = {}
last_press_status = {}
station_ids_plan_downtime = []
last_tag_values = {}
downtime_state = {}
last_published_state = {}

message_queue = Queue(maxsize=MESSAGE_QUEUE_MAXSIZE)
publish_queue = Queue(maxsize=PUBLISH_QUEUE_MAXSIZE)

db_lock = Lock()
worker_thread = None
publisher_thread = None
threads_lock = Lock()
last_worker_processed_ts = time.time()

# -------------------------
# UTIL: Time & Logging
# -------------------------
def now_central():
    return datetime.now(CENTRAL_TZ)

def now_iso():
    return now_central().isoformat()

def to_central(dt):
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(CENTRAL_TZ)

def safe_print(*args, **kwargs):
    print(f"{now_iso()} -", *args, **kwargs)

def get_daily_compute_time():
    try:
        shifts = CONFIG.get("shifts", {})
        three_shifts = shifts.get("3shifts", {})
        a_shift = three_shifts.get("A", {})
        start = a_shift.get("start")
        if start:
            h, m = start.split(":")
            return int(h), int(m)
    except Exception:
        pass
    return 6, 0

def get_shift_date_cst(ts_epoch=None):
    if ts_epoch:
        dt = datetime.fromtimestamp(ts_epoch, CENTRAL_TZ)
    else:
        dt = now_central()
    h, m = get_daily_compute_time()
    shift_start = tm(h, m)
    if dt.time() < shift_start:
        dt = dt - timedelta(days=1)
    return dt.strftime("%Y-%m-%d")

# -------------------------
# SHIFT & BREAK LOGIC
# -------------------------
def parse_hhmm(tstr):
    try:
        parts = tstr.split(":")
        return tm(int(parts[0]), int(parts[1]))
    except Exception:
        return None

def get_shift_for_station(station_id):
    now_dt = now_central()
    now_time = now_dt.time()
    station_cfg = CONFIG.get("stations", {}).get(station_id, {})
    shift_group = station_cfg.get("shifts", "2shifts")
    shifts_map = CONFIG.get("shifts", {})

    try:
        if shift_group not in shifts_map:
            return "A" if tm(6,0) <= now_time < tm(18,0) else "B"
        group = shifts_map[shift_group]
        for sname, win in group.items():
            start_t = parse_hhmm(win["start"])
            end_t = parse_hhmm(win["end"])
            
            if end_t > start_t:
                start_dt = datetime.combine(now_dt.date(), start_t, tzinfo=CENTRAL_TZ)
                end_dt = datetime.combine(now_dt.date(), end_t, tzinfo=CENTRAL_TZ)
            else:
                if now_time < end_t:
                    start_dt = datetime.combine(now_dt.date() - timedelta(days=1), start_t, tzinfo=CENTRAL_TZ)
                    end_dt = datetime.combine(now_dt.date(), end_t, tzinfo=CENTRAL_TZ)
                else:
                    start_dt = datetime.combine(now_dt.date(), start_t, tzinfo=CENTRAL_TZ)
                    end_dt = datetime.combine(now_dt.date() + timedelta(days=1), end_t, tzinfo=CENTRAL_TZ)
            
            if start_dt <= now_dt <= end_dt:
                return sname
        return list(group.keys())[0]
    except Exception:
        return "A"

def get_breaks_for_station(station_id):
    station_cfg = CONFIG.get("stations", {}).get(station_id)
    if not station_cfg: return []
    shift_group = station_cfg.get("shifts")
    if not shift_group: return []
    current_shift = get_shift_for_station(station_id)
    shifts_cfg = CONFIG.get("shifts", {}).get(shift_group, {})
    shift_cfg = shifts_cfg.get(current_shift, {})
    return shift_cfg.get("breaks", [])

def is_break_time_for_station(station_id):
    now = now_central()
    break_list = get_breaks_for_station(station_id)
    if not break_list: return False
    
    for br in break_list:
        start_t = parse_hhmm(br.get("start"))
        end_t = parse_hhmm(br.get("end"))
        if not start_t or not end_t: continue

        s = datetime.combine(now.date(), start_t, tzinfo=CENTRAL_TZ)
        e = datetime.combine(now.date(), end_t, tzinfo=CENTRAL_TZ)
        if end_t < start_t:
            e = e + timedelta(days=1)
        if s <= now <= e:
            return True
    return False

# -------------------------
# CONFIG & STATION HELPERS
# -------------------------
def is_station_active(station_id):
    st_cfg = CONFIG.get("stations", {}).get(station_id)
    if not st_cfg: return False
    return str(st_cfg.get("status", "Paused")).lower() == "active"

def is_station_allowed(station_id: str) -> bool:
    if not station_id: return False
    station_cfg = CONFIG.get("stations", {}).get(station_id)
    return bool(station_cfg) and str(station_cfg.get("status", "")).lower() == "active"

def load_config(path=CONFIG_PATH):
    global CONFIG, station_ids_plan_downtime, CONFIG_MTIME
    try:
        if os.path.exists(path):
            mtime = os.path.getmtime(path)
            if CONFIG_MTIME is None or mtime != CONFIG_MTIME:
                with open(path, "r") as f:
                    cfg = json.load(f)
                CONFIG = cfg
                station_ids_plan_downtime = list(CONFIG.get("stations", {}).keys())
                CONFIG_MTIME = mtime
                safe_print(f"[CONFIG] Loaded stations from {path}")
                
                for station_id, station_cfg in CONFIG.get("stations", {}).items():
                    new_status = str(station_cfg.get("status", "Paused")).lower()
                    old_status = previous_station_status.get(station_id)

                    if old_status == "active" and new_status == "paused":
                        shift = get_shift_for_station(station_id)
                        publish_machine_state(station_id, f"Planned Downtime:{shift}", by="status")
                    elif old_status == "paused" and new_status == "active":
                        state = determine_machine_state(station_id, include_planned=True)
                        publish_machine_state(station_id, state, by="status")
                    previous_station_status[station_id] = new_status
    except Exception as e:
        safe_print("[CONFIG] Error loading config:", e)

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

# -------------------------
# PUBLISHER QUEUE
# -------------------------
def enqueue_publish(topic, payload, qos=0, retain=False):
    entry = {"topic": topic, "payload": payload, "qos": qos, "retain": retain, "attempts": 0, "next_try": 0}
    safe_print(entry)
    try: publish_queue.put(entry, block=False)
    except Full:
        try:
            publish_queue.get_nowait()
            publish_queue.put(entry, block=False)
        except Exception: pass

def publisher_thread_fn():
    buffer = []
    while not shutdown_event.is_set():
        try:
            item = None
            if buffer:
                item = buffer.pop(0)
                if item.get("next_try", 0) > time.time():
                    buffer.insert(0, item)
                    shutdown_event.wait(timeout=0.5)
                    continue
            else:
                try: item = publish_queue.get(timeout=0.5)
                except Empty: continue

            if item is None: break
            topic = item.get("topic"); payload = item.get("payload")
            qos = item.get("qos", 0); retain = item.get("retain", False)
            attempts = item.get("attempts", 0)

            try:
                mqtt_client_instance.publish(topic, payload, qos=qos, retain=retain)
            except Exception as e:
                attempts += 1
                if attempts < PUBLISH_RETRY_MAX_ATTEMPTS:
                    delay = (2 ** (attempts - 1)) * PUBLISH_RETRY_BASE_DELAY
                    item["attempts"] = attempts
                    item["next_try"] = time.time() + delay
                    buffer.append(item)
        except Exception as e:
            safe_print("[PUBLISHER] error:", e)

# -------------------------
# SQLITE DB (Strictly State Recovery)
# -------------------------
def init_runtime_state_db():
    try: os.makedirs(DB_BACKUP_DIR, exist_ok=True)
    except: pass

    with db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
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
        conn.commit(); conn.close()

def delete_runtime_state(station_id, category, key):
    with db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cur = conn.cursor()
        cur.execute("DELETE FROM runtime_state WHERE station_id=? AND category=? AND key=?", 
                    (station_id, category, key))
        conn.commit(); conn.close()

def load_runtime_state():
    data = {}
    with db_lock:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cur = conn.cursor()
        cur.execute("SELECT station_id, category, key, value FROM runtime_state")
        for sid, cat, key, val in cur.fetchall():
            try: v = json.loads(val)
            except Exception: continue
            data.setdefault(sid, {}).setdefault(cat, {})[key] = v
        conn.close()
    return data

# -------------------------
# CORE LOGIC
# -------------------------
def publish_machine_state(station_id, state, by='main'):
    station_cfg = CONFIG.get("stations", {}).get(station_id, {})
    status = str(station_cfg.get("status", "Paused")).lower()
    last_state = last_published_state.get(station_id, {}).get("state")
    
    # FIX 1: Change "PlannedDowntime" to "Planned" to account for the space 
    # in the string passed by load_config()
    if status == "paused" and not state.startswith("Planned"):
        return

    if by == 'main' and last_state and last_state.startswith("Planned"):
        return

    # FIX 2: Removed the strict `and by != 'break': return` block so 
    # break_monitor and status changes can successfully trigger this.
    if state == "PlannedDowntime":
        shift = get_shift_for_station(station_id=station_id)
        state = f"Planned Downtime:{shift}"
    
    if last_state == state: return

    payload = json.dumps({
        "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station_id}/MachineStateAndShift",
        "propertyValues": [{"timestamp": {"timeInSeconds": int(time.time())}, "quality":"GOOD", "value":{"stringValue": state}}]
    })
    enqueue_publish(PUB_TOPIC, payload)
    last_published_state[station_id] = {"state": state, "by": by, "ts": time.time()}
    safe_print(f"[STATE-PUBLISH] [{by}] {station_id} -> {state}")


def determine_machine_state(station_id, include_planned=True):
    tags = last_tag_values.get(station_id, {})
    shift = get_shift_for_station(station_id=station_id)
    
    if include_planned and is_break_time_for_station(station_id=station_id):
        return "PlannedDowntime"
    elif str(tags.get("B_DowntimeInProgressForIgnition", "false")).lower() == "true":
        return f"Unplanned Downtime:{shift}"
    elif str(tags.get("B_DieSetterInProgressForIgnition", "false")).lower() == "true":
        return f"Changeover:{shift}"
    elif str(tags.get("B_OperatorInProgressForIgnition", "false")).lower() == "true":
        try: spm = float(tags.get("N_DieSPM", 0) or 0)
        except: spm = 0.0
        try: target = float(tags.get("PartsPerMinute", 0) or 0)
        except: target = 0.0
        
        if spm >= target: return f"Run to rate:{shift}"
        else: return f"Run not to rate:{shift}"
        
    return f"Idle:{shift}"

def process_downtime_flag(station_id, flag_value, ts):
    global downtime_state
    flag = str(flag_value).lower() == "true"
    state = downtime_state.get(station_id, {"previous_flag": False, "start_time": None, "reason": None})
    shift = get_shift_for_station(station_id)
    
    if flag and not state["previous_flag"]:
        state["start_time"] = ts
        # state["reason"] = reason
        save_runtime_state(station_id, "downtime", "active", {"start_ts": ts.timestamp(), "reason": None})
    elif not flag and state["previous_flag"] and state["start_time"] is not None:
        duration = (ts - state["start_time"]).total_seconds()

        # --- NEW: SAVE THESE FOR LATE REASONS ---
        state["last_end_ts"] = int(ts.timestamp())
        state["last_duration"] = duration

        reason = last_tag_values.get(station_id, {}).get("S_DowntimeReasonOperator", "Unknown")
        if not reason.strip(): reason = "Unknown"
        # r = state["reason"] or "Unknown"
        event_payload = json.dumps({
            "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station_id}/DowntimeReasons",
            "propertyValues": [{
                "timestamp":{"timeInSeconds":int(ts.timestamp()), "offsetInNanos":0},
                "quality":"GOOD",
                "value":{"stringValue": str({"reason": reason, "duration_sec": duration, "Shift": shift})}
            }]
        })
        enqueue_publish(PUB_TOPIC, event_payload)
        delete_runtime_state(station_id, "downtime", "active")
        state["start_time"] = None
        state["reason"] = None
        
    state["previous_flag"] = flag
    downtime_state[station_id] = state

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
    save_runtime_state(station_id, "call_button", "last_active", {"button": formatted, "ts": time.time()})

def transform_payload(payload):
    station = payload.get("stationID"); tag = payload.get("tagName"); val = payload.get("details")
    shift = get_shift_for_station(station) if station else "A"
    out = val
    try:
        if not isinstance(val, (int,float)):
            s = str(val)
            out = float(s) if s.replace(".", "", 1).isdigit() else s
    except: pass
    
    return {
        "topic": PUB_TOPIC,
        "payload": json.dumps({
            "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station}/{tag}",
            "propertyValues":[{"timestamp":{"timeInSeconds":int(time.time())},"quality":"GOOD","value":{"stringValue": json.dumps({tag: out, "Shift": shift})}}]
        })
    }

def handle_message(payload):
    global last_worker_processed_ts
    if not isinstance(payload, dict) or "stationID" not in payload or "tagName" not in payload or "details" not in payload:
        return
    
    tag = payload.get("tagName"); station = payload.get("stationID"); val = payload.get("details")
    ts_raw = payload.get("timestamp", time.time())

    if not is_station_allowed(station): return

    try: ts = to_central(datetime.fromtimestamp(float(ts_raw), timezone.utc))
    except Exception: ts = now_central()

    ts_epoch = int(ts.timestamp())
    
    if station not in station_ids_plan_downtime:
        station_ids_plan_downtime.append(station)

    # Store incoming value internally
    sval = str(val).strip()

    # --- THE FIX: Ignore blank downtime reasons so they don't wipe memory ---
    # if tag == "S_DowntimeReasonOperator" and not sval:
    #     pass # Skip saving the blank string to internal memory
    # else:
    try:
        num = float(sval)
        last_tag_values.setdefault(station, {})[tag] = num
        if tag in MACHINE_STATE_TAGS:
            save_runtime_state(station, "state_inputs", tag, {"value": num, "ts": ts_epoch})
    except ValueError:
        last_tag_values.setdefault(station, {})[tag] = val
        if tag in MACHINE_STATE_TAGS:
            save_runtime_state(station, "state_inputs", tag, {"value": val, "ts": ts_epoch})

    last_tag_values[station]["timestamp"] = ts

    # Forward to SiteWise instantly
    enqueue_publish(PUB_TOPIC, transform_payload(payload)["payload"])

    # Handle Call Buttons
    if tag in CALL_BUTTON_TAGS:
        call_button_states.setdefault(station, {k: "false" for k in CALL_BUTTON_TAGS})
        call_button_states[station][tag] = str(val).lower()
        active = [k for k, v in call_button_states[station].items() if v == "true"]
        publish_tag = active[-1] if active else "None"
        publish_press_status(station, publish_tag)



    # # NEW: Handle Downtime Reason Crash Recovery
    # if tag == "S_DowntimeReasonOperator":
    #     reason_str = str(val).strip()
    #     # last_tag_values.setdefault(station, {})[tag] = reason_str
    #     if reason_str:
    #         save_runtime_state(station, 'last_known_reason', 'S_DowntimeReasonOperator', reason_str)
    #         # We still want it to continue down and be forwarded to SiteWise via transform_payload

    if tag == "S_DowntimeReasonOperator":
        reason_str = str(val).strip()
        if reason_str:
            save_runtime_state(station, 'last_known_reason', 'S_DowntimeReasonOperator', reason_str)
            
            st = downtime_state.get(station, {})
            is_currently_down = st.get("previous_flag", False)
            last_end_ts = st.get("last_end_ts")
            last_dur = st.get("last_duration")
            
            # If the machine is running (not down) AND we have a previous downtime saved in memory...
            if not is_currently_down and last_end_ts and last_dur:
                shift = get_shift_for_station(station)
                
                # We build a payload using the EXACT timestamp of the previous downtime event!
                # SiteWise sees the matching timestamp and gracefully overwrites the "Unknown" reason.
                event_payload = json.dumps({
                    "propertyAlias": f"{SITEWISE_MODEL_NAME}/{station}/DowntimeReasons",
                    "propertyValues": [{
                        "timestamp": {"timeInSeconds": last_end_ts, "offsetInNanos": 0},
                        "quality": "GOOD",
                        "value": {"stringValue": str({"reason": reason_str, "duration_sec": last_dur, "Shift": shift})}
                    }]
                })
                enqueue_publish(PUB_TOPIC, event_payload)
                safe_print(f"[RETROACTIVE UPDATE] {station} | SiteWise reason updated to '{reason_str}'")

    # Handle Downtime
    if tag == "B_DowntimeInProgressForIgnition":
        process_downtime_flag(station, val, ts)

    # Handle Machine State Evaluation
    if tag in MACHINE_STATE_TAGS:
        new_state = determine_machine_state(station, include_planned=False)
        publish_machine_state(station, new_state, by='main')

    last_worker_processed_ts = time.time()

# -------------------------
# THREADS
# -------------------------
def worker_fn():
    while not shutdown_event.is_set():
        try: payload = message_queue.get(timeout=1.0)
        except Empty: continue
        if payload is None: break
        try: handle_message(payload)
        except Exception as e: safe_print("[WORKER] error:", e)

def worker_watchdog_fn():
    global worker_thread
    while not shutdown_event.is_set():
        shutdown_event.wait(timeout=WORKER_WATCHDOG_INTERVAL)
        with threads_lock: wt = worker_thread
        if wt and not wt.is_alive():
            t = Thread(target=worker_fn, daemon=True)
            with threads_lock: worker_thread = t
            t.start()


def break_monitor():
    global mqtt_connected
    while not mqtt_connected and not shutdown_event.is_set(): shutdown_event.wait(timeout=1)
    
    last_break_state = {}
    last_shift_state = {}  # <--- NEW: Track the last known shift
    
    while not shutdown_event.is_set():
        for sid in list(station_ids_plan_downtime):
            current_break = is_break_time_for_station(sid)
            previous_break = last_break_state.get(sid)
            
            current_shift = get_shift_for_station(sid)
            previous_shift = last_shift_state.get(sid)
            
            # Did the break status change? OR did the shift letter change?
            break_changed = (previous_break is not None and current_break != previous_break)
            shift_changed = (previous_shift is not None and current_shift != previous_shift)
            
            # If either changed, force a re-evaluation of the machine state
            if break_changed or shift_changed:
                if current_break: 
                    publish_machine_state(sid, "PlannedDowntime", by="time_monitor")
                else: 
                    publish_machine_state(sid, determine_machine_state(sid, include_planned=False), by="time_monitor")
                    
            last_break_state[sid] = current_break
            last_shift_state[sid] = current_shift
            
        shutdown_event.wait(timeout=30)  # Dropped to 30s to make the shift boundary change snappier
# -------------------------
# MQTT
# -------------------------
def on_connect(client, userdata, flags, rc):
    global mqtt_connected
    if rc == 0:
        mqtt_connected = True
        client.subscribe(SUB_TOPIC)

def on_disconnect(client, userdata, rc):
    global mqtt_connected
    mqtt_connected = False

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        if is_station_allowed(payload.get("stationID")):
            message_queue.put(payload, timeout=0.5)
    except Exception: pass

# -------------------------
# MAIN
# -------------------------
def handle_exit(sig, frame):
    shutdown_event.set()

signal.signal(signal.SIGTERM, handle_exit)
signal.signal(signal.SIGINT, handle_exit)

def main():
    global mqtt_client_instance, worker_thread, publisher_thread

    load_config(CONFIG_PATH)
    init_runtime_state_db()

    # Recover State
    runtime = load_runtime_state()
    for station_id, cats in runtime.items():

        reasons = cats.get("last_known_reason", {})
        if reasons:
            for tag, val_str in reasons.items():
                last_tag_values.setdefault(station_id, {})[tag] = val_str
                safe_print(f"[RECOVER] Restored downtime reason '{val_str}' for {station_id}")
                
        inputs = cats.get("state_inputs", {})
        if inputs:
            for tag, obj in inputs.items():
                last_tag_values.setdefault(station_id, {})[tag] = obj["value"]
        
        dt = cats.get("downtime", {}).get("active")
        if dt:
            downtime_state[station_id] = {
                "previous_flag": True,
                "start_time":  to_central(datetime.fromtimestamp(dt["start_ts"], timezone.utc)),
                "reason": dt["reason"]
            }
            
        cb = cats.get("call_button", {}).get("last_active")
        if cb: last_press_status[station_id] = cb["button"]

    
    mqtt_client_instance = mqtt_client.Client(client_id=CLIENT_ID)
    mqtt_client_instance.on_connect = on_connect
    mqtt_client_instance.on_disconnect = on_disconnect
    mqtt_client_instance.on_message = on_message
    
    try: mqtt_client_instance.connect(BROKER, PORT, 60)
    except Exception: pass

    publisher_thread = Thread(target=publisher_thread_fn, daemon=True)
    publisher_thread.start()
    worker_thread = Thread(target=worker_fn, daemon=True)
    worker_thread.start()
    Thread(target=worker_watchdog_fn, daemon=True).start()
    Thread(target=break_monitor, daemon=True).start()
    Thread(target=config_reloader, daemon=True).start()

    mqtt_client_instance.loop_start()

    try:
        while not shutdown_event.is_set(): time.sleep(1)
    finally:
        shutdown_event.set()
        try: message_queue.put_nowait(None)
        except Exception: pass
        try: mqtt_client_instance.loop_stop(); mqtt_client_instance.disconnect()
        except Exception: pass

if __name__ == "__main__":
    main()