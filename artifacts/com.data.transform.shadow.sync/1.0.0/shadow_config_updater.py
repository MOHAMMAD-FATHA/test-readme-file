#!/usr/bin/env python3

import os
import json
import ssl
from paho.mqtt import client as mqtt_client

# -------------------------
# ENV VARIABLES
# -------------------------
CONFIG_PATH = os.getenv("CONFIG_PATH")

AWS_THING_NAME  = os.getenv("AWS_IOT_THING_NAME")
AWS_SHADOW_NAME = os.getenv("AWS_SHADOW_NAME")
AWS_ENDPOINT    = os.getenv("AWS_ENDPOINT")
AWS_PORT = 8883

AWS_CERT = "/greengrass/v2/certs/device.pem.crt"
AWS_KEY  = "/greengrass/v2/certs/private.pem.key"

SHADOW_BASE = f"$aws/things/{AWS_THING_NAME}/shadow/name/{AWS_SHADOW_NAME}"
SHADOW_GET_TOPIC       = f"{SHADOW_BASE}/get"
SHADOW_GET_ACCEPTED    = f"{SHADOW_BASE}/get/accepted"
SHADOW_UPDATE_DELTA    = f"{SHADOW_BASE}/update/delta"
SHADOW_UPDATE_ACCEPTED = f"{SHADOW_BASE}/update/accepted"

GET_IN_FLIGHT = False


# -------------------------
# HELPERS
# -------------------------
def safe_print(*args):
    print("[SHADOW]", *args)


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


def save_shadow_config(cfg):
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=4)
    safe_print("Config updated from shadow")


# -------------------------
# MQTT CALLBACKS
# -------------------------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        safe_print("Connected")
        client.subscribe(SHADOW_GET_ACCEPTED)
        client.subscribe(SHADOW_UPDATE_DELTA)
        client.subscribe(SHADOW_UPDATE_ACCEPTED)
        client.publish(SHADOW_GET_TOPIC, "{}")
    else:
        safe_print("Connect failed", rc)


def on_message(client, userdata, msg):
    global GET_IN_FLIGHT

    try:
        data = json.loads(msg.payload.decode())
    except:
        return

    if msg.topic == SHADOW_GET_ACCEPTED:

        GET_IN_FLIGHT = False

        state = data.get("state", {})
        desired = extract_desired_config(state)

        if not desired:
            return

        cfg = flatten_zone_config_to_station_config(desired)
        cfg = remove_nulls(cfg)
        save_shadow_config(cfg)
        return

    if msg.topic in (SHADOW_UPDATE_DELTA, SHADOW_UPDATE_ACCEPTED):

        if not GET_IN_FLIGHT:
            GET_IN_FLIGHT = True
            safe_print("Delta detected → GET")
            client.publish(SHADOW_GET_TOPIC, "{}")


# -------------------------
# MAIN
# -------------------------
def main():

    client = mqtt_client.Client(client_id=f"shadow-{AWS_THING_NAME}")

    client.tls_set(
        certfile=AWS_CERT,
        keyfile=AWS_KEY,
        tls_version=ssl.PROTOCOL_TLSv1_2
    )

    client.tls_insecure_set(True)

    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(AWS_ENDPOINT, AWS_PORT, keepalive=60)

    client.loop_forever()


if __name__ == "__main__":
    main()
