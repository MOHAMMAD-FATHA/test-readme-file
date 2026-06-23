#!/usr/bin/env python3

import os
import json
import time
import ssl
import signal
import tempfile
from paho.mqtt import client as mqtt_client

# ==========================================================
# ENV CONFIG (SAFE DEFAULTS)
# ==========================================================

CONFIG_PATH = os.getenv(
    "CONFIG_PATH",
    "/greengrass/v2/work/oee_shared/station_config.json"
)

AWS_THING_NAME  = os.getenv("AWS_IOT_THING_NAME", "")
AWS_SHADOW_NAME = os.getenv("AWS_SHADOW_NAME", "")
AWS_ENDPOINT    = os.getenv("AWS_ENDPOINT", "")

AWS_PORT = 8883

AWS_CERT = "/greengrass/v2/certs/device.pem.crt"
AWS_KEY  = "/greengrass/v2/certs/private.pem.key"

SHADOW_BASE = f"$aws/things/{AWS_THING_NAME}/shadow/name/{AWS_SHADOW_NAME}"
SHADOW_GET_TOPIC       = f"{SHADOW_BASE}/get"
SHADOW_GET_ACCEPTED    = f"{SHADOW_BASE}/get/accepted"
SHADOW_UPDATE_DELTA    = f"{SHADOW_BASE}/update/delta"
SHADOW_UPDATE_ACCEPTED = f"{SHADOW_BASE}/update/accepted"

# ==========================================================
# GLOBAL STATE
# ==========================================================

GET_IN_FLIGHT = False
INITIAL_GET_DONE = False
LAST_SHADOW_VERSION = None
shutdown_flag = False

# ==========================================================
# LOGGING
# ==========================================================

def log(*args):
    print("[SHADOW-SYNC]", *args, flush=True)

# ==========================================================
# SAFE ATOMIC WRITE (NO FILE CORRUPTION)
# ==========================================================

def atomic_write_json(path, data):

    os.makedirs(os.path.dirname(path), exist_ok=True)

    with tempfile.NamedTemporaryFile(
        mode="w",
        delete=False,
        dir=os.path.dirname(path)
    ) as tmp:

        json.dump(data, tmp, indent=4)
        tmp.flush()
        os.fsync(tmp.fileno())
        temp_name = tmp.name

    os.replace(temp_name, path)

    log("Config atomically written →", path)

# ==========================================================
# SHADOW TRANSFORM (YOUR ORIGINAL LOGIC)
# ==========================================================

def convert_shift_name(template_name: str) -> str:
    name = template_name.strip()
    if name.lower().startswith("shift"):
        number_part = name.lower().replace("shift", "").strip()
        return f"{number_part}shifts"
    return name


def flatten_zone_config_to_station_config(input_data: dict) -> dict:

    zones = input_data.get("zones", {})
    shift_templates = input_data.get("shiftTemplates", {})

    output = {"stations": {}, "shifts": {}}

    for zone in zones.values():
        shift_template_name = zone["shiftTemplate"]
        shifts_key = convert_shift_name(shift_template_name)

        for station_id, station_data in zone["stations"].items():
            output["stations"][station_id] = {
                "shifts": shifts_key,
                "status": station_data["status"]
            }

    for template_name, template_shifts in shift_templates.items():
        shifts_key = convert_shift_name(template_name)
        output["shifts"][shifts_key] = {}

        for shift_name, shift_data in template_shifts.items():
            short_name = shift_name.split("-")[-1].strip()

            output["shifts"][shifts_key][short_name] = {
                "start": shift_data["start"],
                "end": shift_data["end"],
                "breaks": shift_data.get("breaks", [])
            }

    return output


def remove_nulls(obj):
    if isinstance(obj, dict):
        for k in list(obj.keys()):
            if obj[k] is None:
                del obj[k]
            else:
                obj[k] = remove_nulls(obj[k])
    elif isinstance(obj, list):
        obj = [remove_nulls(x) for x in obj if x is not None]
    return obj


def extract_desired_config(state):
    desired = state.get("desired")
    if not desired:
        return None
    IGNORE = {"welcome", "config"}
    return {k: v for k, v in desired.items() if k not in IGNORE}

# ==========================================================
# MQTT CALLBACKS
# ==========================================================

def on_connect(client, userdata, flags, rc, properties=None):

    global INITIAL_GET_DONE

    if rc == 0:
        log("Connected to AWS IoT")

        client.subscribe(SHADOW_GET_ACCEPTED)
        client.subscribe(SHADOW_UPDATE_DELTA)
        client.subscribe(SHADOW_UPDATE_ACCEPTED)

        if not INITIAL_GET_DONE:
            log("Initial GET requested")
            client.publish(SHADOW_GET_TOPIC, "{}")
            INITIAL_GET_DONE = True
    else:
        log("Connection failed rc=", rc)


def on_message(client, userdata, msg):

    global GET_IN_FLIGHT
    global LAST_SHADOW_VERSION

    try:
        data = json.loads(msg.payload.decode())
    except:
        log("Invalid JSON")
        return

    if msg.topic == SHADOW_GET_ACCEPTED:

        GET_IN_FLIGHT = False

        version = data.get("version")
        if version == LAST_SHADOW_VERSION:
            log("Shadow version unchanged → skip write")
            return

        LAST_SHADOW_VERSION = version

        state = data.get("state", {})
        desired = extract_desired_config(state)

        if not desired:
            log("No desired config")
            return

        cfg = flatten_zone_config_to_station_config(desired)
        cfg = remove_nulls(cfg)

        atomic_write_json(CONFIG_PATH, cfg)
        return

    if msg.topic in (SHADOW_UPDATE_DELTA, SHADOW_UPDATE_ACCEPTED):

        if not GET_IN_FLIGHT:
            GET_IN_FLIGHT = True
            log("Delta detected → requesting GET")
            client.publish(SHADOW_GET_TOPIC, "{}")

# ==========================================================
# SIGNAL HANDLING (GREENGRASS SAFE)
# ==========================================================

def handle_exit(sig, frame):
    global shutdown_flag
    shutdown_flag = True
    log("Shutdown signal received")

signal.signal(signal.SIGTERM, handle_exit)
signal.signal(signal.SIGINT, handle_exit)

# ==========================================================
# MAIN
# ==========================================================

def main():

    if not AWS_ENDPOINT or not AWS_THING_NAME:
        log("Missing ENV vars — running but shadow disabled")

    client = mqtt_client.Client(
        client_id=f"shadow-sync-{AWS_THING_NAME}",
        protocol=mqtt_client.MQTTv311
    )

    client.tls_set(
        certfile=AWS_CERT,
        keyfile=AWS_KEY,
        tls_version=ssl.PROTOCOL_TLSv1_2
    )

    client.tls_insecure_set(True)

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(AWS_ENDPOINT, AWS_PORT, keepalive=60)

    client.loop_start()

    log("Shadow updater running...")

    while not shutdown_flag:
        time.sleep(1)

    client.loop_stop()
    client.disconnect()

    log("Shadow updater stopped")


if __name__ == "__main__":
    main()
