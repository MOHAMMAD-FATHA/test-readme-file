import os
import json
import logging
import ast
import re
import time
import concurrent.futures
from datetime import datetime, timedelta, timezone, date
from decimal import Decimal
from typing import Any, Dict, Optional, List, Tuple
import requests
from zoneinfo import ZoneInfo
import boto3
from botocore.config import Config

# ---------- Config ----------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

AWS_REGION = os.getenv("AWS_REGION")

ASSET_MODEL_NAME = os.getenv("ASSET_MODEL_NAME")
DATA_TABLE_NAME = os.getenv("DATA_TABLE_NAME")
DAILY_HISTORICAL_TRIGGER = os.getenv("DAILY_HISTORICAL_TRIGGER")
DDB_DAILY_TRIGGER = os.getenv("DDB_DAILY_TRIGGER")


# Increase max workers to handle 65 assets
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "15"))

boto_config = Config(max_pool_connections=150, retries={'max_attempts': 3})

if AWS_REGION:
    SESSION = boto3.session.Session(region_name=AWS_REGION)
else:
    SESSION = boto3.session.Session()

SITEWISE = SESSION.client("iotsitewise", config=boto_config)
DDB = SESSION.resource("dynamodb", config=boto_config)
EVENTS = boto3.client("events", config=boto_config)

DATA_TABLE = DDB.Table(DATA_TABLE_NAME) if DATA_TABLE_NAME else None

# ---------- IoT Shadow Client ----------
IOT = SESSION.client("iot")
IOT_ENDPOINT = IOT.describe_endpoint(
    endpointType="iot:Data-ATS"
)["endpointAddress"]


IOT_DATA = SESSION.client(
    "iot-data",
    endpoint_url=f"https://{IOT_ENDPOINT}",
    config=boto_config
)

THING_NAME = os.environ.get("THING_NAME")
SHADOW_NAME = os.environ.get("SHADOW_NAME")
TIME_ZONE = os.environ.get("TIME_ZONE")
# ---------- Metric prefix maps ----------
METRIC_PREFIXES = {
    "today_util": "Util",
    # "Status":"Status",
    "yesterday_util": "UtilizationPerDay",
    "OEE": "OEEPerDay",
    "downtime_reasons": "DownTimeReasons",
    # "availability": "Availability",
    "quality": "QualityPerJob",
    "performance": "PerformancePerJob",
    "plan_qty": "PlanQTY",
    "part_number": "P#",
    "current_qty": "CurrentQTY",
    "remaining_qty": "RemainingQTY",
    "parts_produced": "TotalPartsProduced",
    "state": "MachineStateAndShift",
    "lost_parts": "TotalLostParts",
    # "actual_oa": "TotalActualOA",
    "remaining_parts": "TotalRemainingParts",
    "call_button": "CallButton",
}
TODAY_SKIP_METRICS = {
    "OEE",
    "utilization",
    "quality",
    "performance",
}
HISTORICAL_METRIC_PREFIXES = {
    "OEE": "OEEPerDay",
    "utilization": "UtilizationPerDay",
    # "availability": "Availability",
    "quality": "QualityPerJob",
    "performance": "PerformancePerJob",
    # "plan_qty": "PlanQTY",
    "parts_produced": "TotalPartsProduced",
    "state": "MachineStateAndShift",
    "downtime_reasons": "DownTimeReasons",
    "lost_parts": "TotalLostParts",
    # "actual_oa": "TotalActualOA",
    "remaining_parts": "TotalRemainingParts",
}
NUMERIC_METRIC_KEYS = {
    "today_util",
    "yesterday_util",
    "OEE",
    "quality",
    "performance",
    "plan_qty",
    "current_qty",
    "remaining_qty",
    "parts_produced",
    "lost_parts",
    "remaining_parts",
}
# Measurements → Alias + MQTT
MEASUREMENT_WITH_ALIAS_AND_MQTT = {
    "MachineStateAndShift",
    "N_OperationPlannedRunMins",
    "DowntimeReasons",
    "Status"
}

# METRICS + Transforms → MQTT ONLY
MQTT_ONLY_PROPERTIES = {
    "RemainingQTY",
    "CurrentQTY",
    "P#",
    "PlanQTY",
    "DownTimeReasons",
    "TotalLostParts",
    "TotalRemainingParts",
    "TotalPartsProduced"
}

PERCENT_LIKE_KEYS = {"yesterday_util", "OEE", "quality", "performance"}

# ---------- caches ----------
_asset_prop_name_cache: Dict[str, Dict[str, str]] = {}

# ================= UTC to CST Helper =================

CENTRAL_TZ = ZoneInfo(TIME_ZONE)

def business_day_bounds(ref_utc: datetime):
    """
    Returns:
      business_date (date)
      window_start_utc (datetime)
      window_end_utc (datetime)

    Business day is defined as:
      Shift-A start (from shadow) → next day Shift-A start
    """

    # Read shadow dynamically
    shadow = get_current_shadow()
    start_hour, start_minute = get_business_start_from_shadow(shadow)

    # Convert reference time to Central time
    local = ref_utc.astimezone(CENTRAL_TZ)

    # Determine business date
    if (
        local.hour < start_hour
        or (local.hour == start_hour and local.minute < start_minute)
    ):
        business_date = local.date() - timedelta(days=1)
    else:
        business_date = local.date()

    #  Build business window in local time
    start_local = datetime(
        business_date.year,
        business_date.month,
        business_date.day,
        start_hour,
        start_minute,
        tzinfo=CENTRAL_TZ,
    )

    end_local = start_local + timedelta(days=1)

    return (
        business_date,
        start_local.astimezone(timezone.utc),
        end_local.astimezone(timezone.utc),
    )

# ---------- helpers ----------
def json_default(o: Any):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, (datetime,)):
        return o.isoformat()
    return str(o)


def response(status_code: int, body: Any) -> Dict[str, Any]:
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "OPTIONS,POST,PUT,GET,DELETE",
            "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
        },
        "body": json.dumps(body, default=json_default),
    }


def safe_parse_str_dict(s: Any) -> Any:
    if not isinstance(s, str):
        return s
    s = s.strip()
    if s == "":
        return s
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, (dict, list)):
            return parsed
    except Exception:
        pass
    try:
        return json.loads(s)
    except Exception:
        pass
    return s


def normalize_sitewise_ts(ts) -> Optional[str]:
    """Normalize SiteWise timestamp shapes to ISO8601 UTC string (Z) or None."""
    try:
        if not ts:
            return None
        if isinstance(ts, dict):
            if "timeInMillis" in ts:
                try:
                    millis = int(ts["timeInMillis"])
                    return (
                        datetime.fromtimestamp(millis / 1000.0, tz=timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z")
                    )
                except Exception:
                    pass
            if "timeInSeconds" in ts:
                try:
                    secs = int(ts["timeInSeconds"])
                    return (
                        datetime.fromtimestamp(secs, tz=timezone.utc)
                        .isoformat()
                        .replace("+00:00", "Z")
                    )
                except Exception:
                    pass
            for v in ts.values():
                res = normalize_sitewise_ts(v)
                if res:
                    return res
            return None
        if isinstance(ts, (int, float)):
            if ts > 1e12:
                return (
                    datetime.fromtimestamp(float(ts) / 1000.0, tz=timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            if ts > 1e9:
                return (
                    datetime.fromtimestamp(float(ts), tz=timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            return None
        if isinstance(ts, str):
            s = ts.strip()
            if re.fullmatch(r"\d+", s):
                return normalize_sitewise_ts(int(s))
            try:
                s2 = s.replace("Z", "+00:00")
                dt = datetime.fromisoformat(s2)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return (
                    dt.astimezone(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            except Exception:
                return None
    except Exception:
        logger.exception("normalize_sitewise_ts failed on %s", ts)
    return None


OKTA_DOMAIN = os.getenv("OKTA_DOMAIN_NAME") # e.g., https://okta-dev.daikincomfort.com
OKTA_API_TOKEN = os.getenv("OKTA_API_TOKEN")
# -------------- OKTA STUFF ----------------------------------------------------------
def okta_headers():
    return {
        "Authorization": f"SSWS {OKTA_API_TOKEN}",
        "Accept": "application/json",
        "Content-Type": "application/json"
    }

def okta_list_users():
    url = f"{OKTA_DOMAIN}/api/v1/users"
    resp = requests.get(url, headers=okta_headers())
    resp.raise_for_status()
    return resp.json()


def okta_get_groups(user_id: str):
    url = f"{OKTA_DOMAIN}/api/v1/users/{user_id}/groups"
    resp = requests.get(url, headers=okta_headers())
    resp.raise_for_status()
    groups = resp.json()
    return {g["profile"]["name"]: g["id"] for g in groups}

def okta_lookup_group_id(group_name: str):
    url = f"{OKTA_DOMAIN}/api/v1/groups?q={group_name}"
    resp = requests.get(url, headers=okta_headers())
    resp.raise_for_status()
    groups = resp.json()
    for g in groups:
        if g["profile"]["name"] == group_name:
            return g["id"]
    return None

def okta_assign_group(user_id: str, group_id: str):
    url = f"{OKTA_DOMAIN}/api/v1/groups/{group_id}/users/{user_id}"
    resp = requests.put(url, headers=okta_headers())
    resp.raise_for_status()
    return True

def okta_remove_group(user_id: str, group_id: str):
    # url = f"{OKTA_DOMAIN}/api/v1/users/{user_id}/groups/{group_id}"
    url = f"{OKTA_DOMAIN}/api/v1/groups/{group_id}/users/{user_id}"
    resp = requests.delete(url, headers=okta_headers())
    resp.raise_for_status()
    return True

# ---------- SiteWise helpers ----------

# --------- UPDATE EVENTBRIDGE TRIGGERS -----------------------
def cst_hhmm_to_utc_cron(hhmm: str, offset_min: int = 0) -> str:
    hour, minute = map(int, hhmm.split(":"))
    local = datetime.now(CENTRAL_TZ).replace(
        hour=hour, minute=minute, second=0, microsecond=0
    ) + timedelta(minutes=offset_min)

    utc = local.astimezone(timezone.utc)

    # cron(M H DOM MON DOW YR)
    return f"cron({utc.minute} {utc.hour} * * ? *)"

def update_eventbridge_triggers(start_time_hhmm: str):

    # Trigger 1 → Shift 2-A start
    cron_1 = cst_hhmm_to_utc_cron(start_time_hhmm, offset_min=0)

    # Trigger 2 → +15 minutes
    cron_2 = cst_hhmm_to_utc_cron(start_time_hhmm, offset_min=15)

    EVENTS.put_rule(
        Name=DAILY_HISTORICAL_TRIGGER,
        ScheduleExpression=cron_1,
        State="ENABLED"
    )

    EVENTS.put_rule(
        Name=DDB_DAILY_TRIGGER,
        ScheduleExpression=cron_2,
        State="ENABLED"
    )
def get_model_id_by_name(model_name: str) -> str:
    paginator = SITEWISE.get_paginator("list_asset_models")
    for page in paginator.paginate():
        for model in page.get("assetModelSummaries", []):
            name = model.get("assetModelName") or model.get("name")
            if name == model_name:
                return model.get("id") or model.get("assetModelId")
    raise Exception(f"Model '{model_name}' not found")


def list_assets_for_model(model_id: str) -> list:
    assets = []
    paginator = SITEWISE.get_paginator("list_assets")
    for page in paginator.paginate(assetModelId=model_id):
        assets.extend(page.get("assetSummaries", []))
    return assets

# ------------------------------------- HELPER FUNCTIONS TO CREATE AN ASSET IN SITEWISE 
def get_zone_working_days(zone: str) -> list[int]:
    key = {
        "AssetId": "ZONE",
        "PropertyId": f"WORKING_DAYS#{zone.lower()}"
    }

    try:
        resp = DATA_TABLE.get_item(Key=key)
        item = resp.get("Item")
        if not item:
            return []
        props = json.loads(item.get("properties", "{}"))
        return props.get("working_days", [])
    except Exception:
        logger.exception("Failed fetching working days for %s", zone)
        return []

# ENABLING ALIAS AND MQTT FOR ASSETS

def enable_alias_and_mqtt_for_asset(asset_id: str, asset_name: str, model_name: str):

    logger.info("Starting alias/MQTT configuration | asset=%s", asset_name)

    prop_map = map_property_name_to_id(asset_id)

    for prop_name, prop_id in prop_map.items():

        alias = f"{model_name}/{asset_name}/{prop_name}"

        # -------------------------------------------------
        # Specific Measurements → Alias + MQTT
        # -------------------------------------------------
        if prop_name in MEASUREMENT_WITH_ALIAS_AND_MQTT:

            try:
                SITEWISE.update_asset_property(
                    assetId=asset_id,
                    propertyId=prop_id,
                    propertyAlias=alias,
                    propertyNotificationState="ENABLED",
                )

                logger.info(
                    "ALIAS + MQTT SET | asset=%s | property=%s",
                    asset_name,
                    prop_name
                )

            except Exception:
                logger.exception(
                    "FAILED ALIAS + MQTT | asset=%s | property=%s",
                    asset_name,
                    prop_name
                )

        # -------------------------------------------------
        #  Transforms + Metrics → MQTT ONLY
        # -------------------------------------------------
        elif prop_name in MQTT_ONLY_PROPERTIES:

            try:
                SITEWISE.update_asset_property(
                    assetId=asset_id,
                    propertyId=prop_id,
                    propertyNotificationState="ENABLED",
                )

                logger.info(
                    "MQTT ENABLED | asset=%s | property=%s",
                    asset_name,
                    prop_name
                )

            except Exception:
                logger.exception(
                    "FAILED MQTT | asset=%s | property=%s",
                    asset_name,
                    prop_name
                )

        # -------------------------------------------------
        #  Everything else → Alias ONLY
        # -------------------------------------------------
        else:

            try:
                SITEWISE.update_asset_property(
                    assetId=asset_id,
                    propertyId=prop_id,
                    propertyAlias=alias,
                )

                logger.info(
                    "ALIAS SET | asset=%s | property=%s",
                    asset_name,
                    prop_name
                )

            except Exception:
                logger.exception(
                    "FAILED ALIAS | asset=%s | property=%s",
                    asset_name,
                    prop_name
                )

    logger.info("Alias/MQTT configuration completed | asset=%s", asset_name)

def wait_for_asset_active(asset_id: str, timeout_sec: int = 60):
    start = time.time()

    while True:
        resp = SITEWISE.describe_asset(assetId=asset_id)
        state = resp.get("assetStatus", {}).get("state")

        logger.info(
            "ASSET STATE | assetId=%s | state=%s",
            asset_id, state
        )

        if state == "ACTIVE":
            return

        if time.time() - start > timeout_sec:
            raise TimeoutError(
                f"Asset {asset_id} did not become ACTIVE in time"
            )

        # time.sleep(4)


def upsert_zone_working_days(zone: str, working_days: list[int]):

    if not re.match(r"^zone_\d+$", zone):
        raise ValueError("zone must be like zone_1")
    working_days = _validate_working_days(working_days)

    item = {
        "AssetId": "ZONE",
        "PropertyId": f"WORKING_DAYS#{zone}",
        "properties": json.dumps({
            "zone": zone,
            "working_days": working_days,
            "timezone": "America/Chicago",
            "updatedAtUTC": datetime.now(timezone.utc).isoformat()
        })
    }

    DATA_TABLE.put_item(Item=item)


def asset_exists_for_model(model_id: str, machine_name: str) -> bool:
    """Return True if an asset with machine_name already exists for the model."""
    try:
        assets = list_assets_for_model(model_id)
        for a in assets:
            name = a.get("name") or a.get("assetName") or ""
            if name == machine_name:
                return True
        return False
    except Exception:
        logger.exception("asset_exists_for_model failed")
        # Be conservative and say it exists in case of failure
        return True

def get_asset_id_by_name(model_id: str, machine_name: str) -> Optional[str]:
    """
    Return the assetId for machine_name under the given model_id, or None if not found.
    """
    try:
        paginator = SITEWISE.get_paginator("list_assets")
        for page in paginator.paginate(assetModelId=model_id):
            for a in page.get("assetSummaries", []):
                name = a.get("name") or a.get("assetName") or ""
                if name == machine_name:
                    return a.get("id") or a.get("assetId")
        return None
    except Exception:
        logger.exception("get_asset_id_by_name failed for %s", machine_name)
        return None

def is_asset_active(asset_id: str) -> bool:
    try:
        SITEWISE.describe_asset(assetId=asset_id)
        return True
    except SITEWISE.exceptions.ResourceNotFoundException:
        return False
    except Exception:
        return False

def cleanup_alias_and_mqtt_before_delete(asset_id: str, asset_name: str):
    """
    Disable MQTT and remove alias for ALL properties before deleting asset.
    Prevents ghost data streams.
    """

    logger.info("Starting cleanup before delete | asset=%s", asset_name)

    prop_map = map_property_name_to_id(asset_id)

    for prop_name, prop_id in prop_map.items():

        try:
            desc = SITEWISE.describe_asset_property(
                assetId=asset_id,
                propertyId=prop_id
            )

            asset_prop = desc.get("assetProperty", {})

            existing_alias = asset_prop.get("alias")
            notification = asset_prop.get("notification", {})
            notif_state = notification.get("state")

            # Only update if something exists
            if existing_alias or notif_state == "ENABLED":

                SITEWISE.update_asset_property(
                    assetId=asset_id,
                    propertyId=prop_id,
                    propertyAlias="",                     # REMOVE alias
                    propertyNotificationState="DISABLED"  # DISABLE MQTT
                )

                logger.info(
                    "CLEANED | asset=%s | property=%s",
                    asset_name,
                    prop_name
                )

        except Exception:
            logger.exception(
                "Cleanup failed | asset=%s | property=%s",
                asset_name,
                prop_name
            )

    logger.info("Cleanup completed | asset=%s", asset_name)

def create_asset_for_machine(model_id: str, machine_name: str, description: Optional[str] = None) -> str:
    """Create a new asset for the given model and return the new assetId."""
    params = {"assetModelId": model_id, "assetName": machine_name}
    if description:
        params["assetDescription"] = description
    try:
        resp = SITEWISE.create_asset(**params)
        return resp.get("assetId")
    except Exception:
        logger.exception("create_asset_for_machine failed")
        raise

def remove_machine_from_targets(
    zone_name: str,
    machine_name: str,
) -> Dict[str, Any]:

    key = {"AssetId": "All", "PropertyId": "Targets"}

    resp = DATA_TABLE.get_item(Key=key)
    item = resp.get("Item")

    if not item:
        return {}

    raw_props = item.get("properties") or item
    current_map = safe_parse_str_dict(raw_props)

    # Normalize zone name to match stored keys
    zone_name_norm = normalize_zone_name(zone_name)

    # Make lookup case-insensitive
    zone_key_match = None
    for z in current_map.keys():
        if z.lower() == zone_name_norm.lower():
            zone_key_match = z
            break

    if not zone_key_match:
        return current_map

    zone_obj = current_map.get(zone_key_match)
    if not zone_obj:
        return current_map

    for metric in ["OA", "Quality", "OEE", "Utilization"]:
        metric_obj = zone_obj.get(metric)

        if not isinstance(metric_obj, dict):
            continue

        # Remove machine
        metric_obj.pop(machine_name, None)

        # Remove Common temporarily
        metric_obj.pop("Common_Zone_Value", None)

        # Recalculate average if machines remain
        machine_values = [
            float(v)
            for v in metric_obj.values()
            if isinstance(v, (int, float))
        ]

        if machine_values:
            metric_obj["Common_Zone_Value"] = round(
                sum(machine_values) / len(machine_values), 2
            )
        else:
            # No machines left → empty object
            metric_obj.clear()

        zone_obj[metric] = metric_obj

    current_map[zone_name] = zone_obj

    new_item = {
        "AssetId": "All",
        "PropertyId": "Targets",
        "properties": json.dumps(current_map, default=json_default),
    }

    DATA_TABLE.put_item(Item=new_item)

    return current_map
def delete_asset_by_name(model_id: str, machine_name: str) -> Dict[str, Any]:
    """
    Find assetId for machine_name under model_id,
    CLEAN alias + MQTT,
    then delete asset.

    Returns:
        {"deleted": True, "assetId": id}
        {"deleted": False, "reason": "..."}
    """

    asset_id = get_asset_id_by_name(model_id, machine_name)

    if not asset_id:
        return {"deleted": False, "reason": "not_found"}

    logger.info("DELETE START | machine=%s | assetId=%s", machine_name, asset_id)

    # -------------------------------------------------
    # STEP 1 — GET ALL PROPERTIES
    # -------------------------------------------------
    try:
        prop_map = map_property_name_to_id(asset_id)
        logger.info("Found %d properties for cleanup", len(prop_map))
    except Exception:
        logger.exception("Failed to list asset properties before delete")
        return {"deleted": False, "reason": "property_list_failed"}

    # -------------------------------------------------
    # STEP 2 — DISABLE MQTT + REMOVE ALIAS
    # -------------------------------------------------
    for prop_name, prop_id in prop_map.items():

        try:
            SITEWISE.update_asset_property(
                assetId=asset_id,
                propertyId=prop_id,
                propertyAlias=None,  # remove alias
                propertyNotificationState="DISABLED",  # disable mqtt
            )

            logger.info(
                "CLEANED | asset=%s | property=%s",
                machine_name,
                prop_name
            )

        except Exception:
            logger.exception(
                "FAILED CLEANUP | asset=%s | property=%s",
                machine_name,
                prop_name
            )

    # # -------------------------------------------------
    # # STEP 3 — SMALL WAIT (IMPORTANT)
    # # gives AWS time to detach streams
    # # -------------------------------------------------
    # time.sleep(2)

    # -------------------------------------------------
    # STEP 4 — DELETE ASSET
    # -------------------------------------------------
    try:
        SITEWISE.delete_asset(assetId=asset_id)

        logger.info(
            "ASSET DELETED SUCCESS | machine=%s | assetId=%s",
            machine_name,
            asset_id
        )

        return {"deleted": True, "assetId": asset_id}

    except SITEWISE.exceptions.ConflictException as e:
        logger.exception("Conflict deleting asset %s (%s)", machine_name, asset_id)
        return {"deleted": False, "reason": "conflict", "error": str(e)}

    except Exception as e:
        logger.exception("Failed to delete SiteWise asset %s (%s)", machine_name, asset_id)
        return {"deleted": False, "reason": "error", "error": str(e)}

def get_business_start_from_shadow(shadow: dict) -> tuple[int, int]:
    """
    Business day starts at Shift-A start time.
    Shift-A start is guaranteed same for Shift 2 & Shift 3.
    Returns (hour, minute).
    """
    shift_templates = shadow.get("shiftTemplates", {})

    for template in shift_templates.values():
        for shift_name, shift_cfg in template.items():
            if shift_name.endswith("-A"):
                h, m = map(int, shift_cfg["start"].split(":"))
                return h, m

    raise ValueError("Shift-A start time not found in shadow")

def get_current_shadow() -> Dict[str, Any]:
    try:
        resp = IOT_DATA.get_thing_shadow(
            thingName=THING_NAME,
            shadowName=SHADOW_NAME
        )
        data = json.loads(resp["payload"].read())
        return data.get("state", {}).get("desired", {})
    except Exception:
        logger.exception("Failed reading shadow")
        return {}

def update_shadow(delta: Dict[str, Any]) -> None:
    payload = {
        "state": {
            "desired": delta
        }
    }
    logger.info("Shadow update payload: %s", payload)

    IOT_DATA.update_thing_shadow(
        thingName=THING_NAME,
        shadowName=SHADOW_NAME,
        payload=json.dumps(payload)
    )
def dispatch_shadow_action(action: str, body: Dict[str, Any]) -> None:
    """
    Allowed actions ONLY.
    remove_station only via DELETE machine API.
    """

    # -------------------------------------------------
    # REMOVE MACHINE (WITH ZONE CLEANUP)
    # -------------------------------------------------
    if action == "remove_machine":

        _require(body, ["zone", "stationId"])
        zone = body["zone"].strip().lower()
        station_id = body["stationId"].strip()

        if not re.match(r"^zone_\d+$", zone):
            raise ValueError("zone must be like zone_1 | zone_2")

        _validate_station_id(station_id)

        # Read current shadow
        shadow = get_current_shadow()
        zones_cfg = shadow.get("zones", {})

        zone_cfg = zones_cfg.get(zone)
        if not zone_cfg:
            logger.warning("Zone %s not found in shadow — skipping", zone)
            return

        stations = zone_cfg.get("stations", {})
        if station_id not in stations:
            logger.warning(
                "Station %s not present in zone %s — skipping",
                station_id, zone
            )
            return

        #  Remove station locally
        remaining_stations = {
            k: v for k, v in stations.items() if k != station_id
        }

        # LAST STATION → REMOVE ZONE
        if not remaining_stations:
            logger.info(
                "Removing last station %s from %s → deleting zone",
                station_id, zone
            )
            update_shadow({
                "zones": {
                    zone: None
                }
            })
        else:
            logger.info(
                "Removing station %s from zone %s",
                station_id, zone
            )
            update_shadow({
                "zones": {
                    zone: {
                        "stations": {
                            station_id: None
                        }
                    }
                }
            })


    # -------------------------------------------------
    # UPSERT MACHINE INTO ITS DERIVED ZONE
    # -------------------------------------------------
    elif action == "upsert_station":

        _require(body, ["zone", "stationId", "status"])

        zone = body["zone"].strip().lower()
        station_id = body["stationId"].strip()

        if not re.match(r"^zone_\d+$", zone):
            raise ValueError("zone must be like zone_1 | zone_2")

        _validate_station_id(station_id)
        _validate_status(body["status"])

        update_shadow({
            "zones": {
                zone: {
                    "stations": {
                        station_id: {
                            "status": body["status"]
                        }
                    }
                }
            }
        })

    # -------------------------------------------------
    # STATUS UPDATE INSIDE ZONE
    # -------------------------------------------------
    elif action == "update_status":

        _require(body, ["zone", "stationId", "status"])

        zone = body["zone"].strip().lower()
        station_id = body["stationId"].strip()

        if not re.match(r"^zone_\d+$", zone):
            raise ValueError("zone must be like zone_1 | zone_2")

        _validate_station_id(station_id)
        _validate_status(body["status"])

        update_shadow({
            "zones": {
                zone: {
                    "stations": {
                        station_id: {
                            "status": body["status"]
                        }
                    }
                }
            }
        })

    # -------------------------------------------------
    # TEMPLATE SWITCH FOR ENTIRE ZONE
    # -------------------------------------------------
    elif action == "move_zone":

        _require(body, ["zone","shifts"])
        zone = body["zone"].strip().lower()

        _validate_shift_type(body["shifts"])

        if not re.match(r"^zone_\d+$", zone):
            raise ValueError("zone must be like zone_1 | zone_2")

        update_shadow({
            "zones":{
                zone:{
                    "shiftTemplate": body["shifts"]
                }
            }
        })


    # -------------------------------------------------
    # SHIFT TEMPLATE TIME — GLOBAL (NO ZONE)
    # -------------------------------------------------
    elif action == "update_shift_config_bulk":

        _require(body, ["shiftType", "shifts"])
        _validate_shift_type(body["shiftType"])

        update_shadow({
            "shiftTemplates": {
                body["shiftType"]: body["shifts"]
            }
        })


    else:
        raise ValueError(f"Unsupported shadow action: {action}")



def _validate_working_days(working_days):
    if not isinstance(working_days, list):
        raise ValueError("working_days must be a list")

    if not working_days:
        raise ValueError("working_days cannot be empty")

    for d in working_days:
        if not isinstance(d, int) or d < 0 or d > 6:
            raise ValueError(
                "working_days must contain integers between 0 and 6"
            )

    # Duplicate validation (important)
    if len(working_days) != len(set(working_days)):
        raise ValueError("working_days must not contain duplicate values")

    # return normalized value
    return sorted(working_days)


def apply_machine_configs(configs: list):
    """
    Apply post-create configs.
    remove_station is NOT allowed here.
    """
    if not isinstance(configs, list):
        return

    for cfg in configs:
        cfg_type = cfg.get("type")
        action = cfg.get("action")
        payload = cfg.get("payload")

        if not cfg_type or not action or not payload:
            logger.warning("Invalid config entry skipped: %s", cfg)
            continue

        if action == "remove_station":
            raise ValueError(
                "remove_station cannot be used here. "
                "Use DELETE machine API."
            )

        if cfg_type == "shadow":
            dispatch_shadow_action(action, payload)
        else:
            logger.warning("Unknown config type ignored: %s", cfg_type)
def _require(body, fields: list):
    missing = [f for f in fields if f not in body or body[f] in (None, "", [])]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

def _validate_station_id(station_id: str):
    if not isinstance(station_id, str) or not re.match(r"^SM_SP\d+", station_id):
        raise ValueError("stationId must be like SM_SP<number>")

def _validate_shift_type(shift_type: str):
    if not isinstance(shift_type, str):
        raise ValueError("shiftType must be a string")

    if not re.match(r"^Shift\s+[1-9]\d*$", shift_type):
        raise ValueError("shiftType must be like 'Shift 2' or 'Shift 3'")


def _validate_shift_code(code: str, shift_type: Optional[str] = None):
    if not isinstance(code, str):
        raise ValueError("shiftCode must be a string")

    if not shift_type:
        raise ValueError("shiftType is required to validate shiftCode")

    m = re.match(r"^Shift\s+(\d+)$", shift_type)
    if not m:
        raise ValueError("Invalid shiftType format")

    shift_num = int(m.group(1))

    # Shift code must match: Shift <n>-<Letter>
    code_match = re.match(rf"^Shift\s+{shift_num}-([A-Z])$", code)
    if not code_match:
        raise ValueError(
            f"shiftCode must be like 'Shift {shift_num}-A'"
        )

    letter = code_match.group(1)

    if shift_num == 2 and letter not in ("A", "B"):
        raise ValueError("Shift 2 supports only A | B")

    if shift_num == 3 and letter not in ("A", "B", "C"):
        raise ValueError("Shift 3 supports only A | B | C")



def _validate_time_hhmm(val: str):
    if not isinstance(val, str) or not re.match(r"^\d{2}:\d{2}$", val):
        raise ValueError("time must be HH:MM format")

def _validate_status(status: str):
    if status not in ("Active", "Paused", "Running", "Idle"):
        raise ValueError("status must be one of Active | Paused | Running | Idle")

def _validate_breaks_bulk(breaks: list, shift_start: str, shift_end: str):
    if not isinstance(breaks, list) or not breaks:
        raise ValueError("breaks must be a non-empty list")

    ss = _time_to_minutes(shift_start)
    se = _time_to_minutes(shift_end)

    overnight_shift = se <= ss
    segments = []

    for b in breaks:
        _require(b, ["start", "end"])
        _validate_time_hhmm(b["start"])
        _validate_time_hhmm(b["end"])

        bs = _time_to_minutes(b["start"])
        be = _time_to_minutes(b["end"])

        # Normalize break for overnight shift
        if overnight_shift:
            if bs < ss:
                bs += 1440
            if be <= ss:
                be += 1440
            shift_end_norm = se + 1440
            shift_start_norm = ss
        else:
            shift_start_norm = ss
            shift_end_norm = se

        if bs < shift_start_norm or be > shift_end_norm:
            raise ValueError(
                f"Break {b['start']}–{b['end']} outside shift time "
                f"{shift_start}–{shift_end}"
            )

        if be <= bs:
            raise ValueError(
                f"Invalid break range {b['start']}–{b['end']}"
            )

        segments.append((b, bs, be))

    # Overlap check
    segments.sort(key=lambda x: x[1])
    for i in range(len(segments) - 1):
        if segments[i][2] > segments[i + 1][1]:
            raise ValueError(
                f"Break overlap between "
                f"{segments[i][0]['start']}–{segments[i][0]['end']} and "
                f"{segments[i+1][0]['start']}–{segments[i+1][0]['end']}"
            )

def _time_to_minutes(t: str) -> int:
    h, m = t.split(":")
    return int(h) * 60 + int(m)

def _validate_no_overlap_shift_time_bulk(shift_type: str, updates: dict):
    segments = []

    for code, cfg in updates.items():
        s = _time_to_minutes(cfg["start"])
        e = _time_to_minutes(cfg["end"])

        if e > s:
            segments.append((code, s, e))
        else:
            segments.append((code, s, 1440))
            segments.append((code, 0, e))

    segments.sort(key=lambda x: x[1])

    for i in range(len(segments) - 1):
        if segments[i][2] > segments[i + 1][1]:
            raise ValueError(
                f"Shift overlap between "
                f"{segments[i][0]} and {segments[i+1][0]}"
            )




def _validate_no_overlapping_breaks(breaks: list):
    """
    Ensures breaks do not overlap.
    Adjacent breaks (end == next start) are allowed.
    """
    if not isinstance(breaks, list):
        raise ValueError("breaks must be a list")

    intervals = []

    for idx, b in enumerate(breaks):
        _require(b, ["start", "end"])
        _validate_time_hhmm(b["start"])
        _validate_time_hhmm(b["end"])

        start = _time_to_minutes(b["start"])
        end = _time_to_minutes(b["end"])

        if start >= end:
            raise ValueError(
                f"Invalid break time at index {idx}: start must be before end"
            )

        intervals.append((start, end))

    # sort by start time
    intervals.sort(key=lambda x: x[0])

    for i in range(1, len(intervals)):
        prev_start, prev_end = intervals[i - 1]
        curr_start, curr_end = intervals[i]

        if curr_start < prev_end:
            raise ValueError(
                "Breaks must not overlap. "
                f"Overlap between {prev_start//60:02d}:{prev_start%60:02d}-"
                f"{prev_end//60:02d}:{prev_end%60:02d} and "
                f"{curr_start//60:02d}:{curr_start%60:02d}-"
                f"{curr_end//60:02d}:{curr_end%60:02d}"
            )

# ------------------------------------- SITEWISE HELPER FUNCTION ENDS HERE -------------------------------------
def map_property_name_to_id(asset_id: str) -> Dict[str, str]:
    if asset_id in _asset_prop_name_cache:
        return _asset_prop_name_cache[asset_id]
    prop_map: Dict[str, str] = {}
    paginator = SITEWISE.get_paginator("list_asset_properties")
    for page in paginator.paginate(assetId=asset_id):
        for p in page.get("assetPropertySummaries", []):
            pid = p.get("id")
            path = p.get("path", [])
            if len(path) > 1 and isinstance(path[1], dict) and path[1].get("name"):
                pname = path[1]["name"]
            else:
                pname = p.get("name") or pid
            if pname and pid:
                prop_map[pname] = pid
    _asset_prop_name_cache[asset_id] = prop_map
    return prop_map


def get_sitewise_latest_value(asset_id: str, property_id: str) -> Optional[Dict[str, Any]]:
    try:
        resp = SITEWISE.get_asset_property_value(
            assetId=asset_id, propertyId=property_id
        )
        pv = resp.get("propertyValue")
        if not pv:
            return None
        val = pv.get("value")
        ts = pv.get("timestamp")
        quality = pv.get("quality")
        primitive = None
        if isinstance(val, dict):
            for key in ("doubleValue", "integerValue", "stringValue", "booleanValue"):
                if key in val:
                    primitive = val[key]
                    break
            if primitive is None and "value" in val and isinstance(val["value"], dict):
                for key in (
                    "doubleValue",
                    "integerValue",
                    "stringValue",
                    "booleanValue",
                ):
                    if key in val["value"]:
                        primitive = val["value"][key]
                        break
        else:
            primitive = val
        return {"raw_value": primitive, "timestamp": ts, "quality": quality}
    except Exception:
        logger.exception("SiteWise get_asset_property_value failed")
        return None


# ---------- SiteWise history fetch ----------
def fetch_history_range(
    asset_id: str, property_id: str, start_dt: datetime, end_dt: datetime
) -> List[dict]:
    """Fetch SiteWise history for [start_dt, end_dt). Returns list of history records."""
    out = []
    try:
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())
        next_token = None
        while True:
            params = {
                "assetId": asset_id,
                "propertyId": property_id,
                "startDate": start_ts,
                "endDate": end_ts,
            }
            if next_token:
                params["nextToken"] = next_token
            resp = SITEWISE.get_asset_property_value_history(**params)
            out.extend(resp.get("assetPropertyValueHistory", []))
            next_token = resp.get("nextToken")
            if not next_token:
                break
    except Exception:
        logger.exception("fetch_history_range failed")
    return out


# Introducing this new function to lookback exponentially for the state start time
def fetch_history_with_lookback(
    asset_id: str,
    property_id: str,
    window_start: datetime,
    window_end: datetime,
    initial_days: int = 1,
    max_days: int = 30,
) -> List[dict]:
    """
    Exponentially expand lookback until we find at least one history record whose timestamp < window_start
    (i.e. the preceding state), OR until max_days reached. Returns combined list of in-window records
    plus the prior record(s) we discovered.
    """
    # first fetch the window itself
    recs = (
        fetch_history_range(asset_id, property_id, window_start, window_end)
        if property_id
        else []
    )

    def any_prior(rows):
        for r in rows:
            tsobj = r.get("timestamp") or {}
            if isinstance(tsobj, dict):
                secs = tsobj.get("timeInSeconds")
                if secs is None and "timeInMillis" in tsobj:
                    try:
                        secs = int(tsobj.get("timeInMillis")) // 1000
                    except Exception:
                        secs = None
            elif isinstance(tsobj, (int, float)):
                secs = int(tsobj)
            else:
                secs = None
            if secs is not None and secs < int(window_start.timestamp()):
                return True
        return False

    if any_prior(recs):
        return recs

    # exponential backoff
    days = initial_days
    while days <= max_days:
        start_try = window_start - timedelta(days=days)
        extra = fetch_history_range(asset_id, property_id, start_try, window_start)
        if extra:
            try:
                extra_sorted = sorted(
                    extra,
                    key=lambda r: int(
                        (
                            r.get("timestamp", {}).get("timeInSeconds")
                            or (
                                int(
                                    r.get("timestamp", {}).get("timeInMillis") or 0
                                )
                                // 1000
                            )
                        )
                    ),
                )
            except Exception:
                extra_sorted = extra
            latest_prior = extra_sorted[-1] if extra_sorted else None
            if latest_prior:
                if not any(
                    (
                        r.get("timestamp", {}).get("timeInSeconds")
                        == latest_prior.get("timestamp", {}).get("timeInSeconds")
                        and (
                            r.get("value", {}).get("stringValue")
                            == latest_prior.get("value", {}).get("stringValue")
                        )
                    )
                    for r in recs
                ):
                    recs.append(latest_prior)
            return recs
        days = days * 2

    return recs


# ---------- Dynamo helpers ----------
def dynamo_get_item(asset_id: str, property_key: str) -> Optional[dict]:
    if DATA_TABLE is None:
        logger.debug("No DATA_TABLE configured")
        return None
    try:
        resp = DATA_TABLE.get_item(Key={"AssetId": asset_id, "PropertyId": property_key})
        item = resp.get("Item")
        if not item:
            return None
        props = item.get("properties") or item.get("Properties") or item
        parsed = safe_parse_str_dict(props)
        return parsed
    except Exception:
        logger.exception(
            "Dynamo get_item failed for %s / %s", asset_id, property_key
        )
        return None
# PARTS DAYWISE HELPER
def get_day_record_for_metric(asset_id: str, base_pid: str, dt: date):
    """
    Reads the daily stored record: <base_pid>_<YYYY-MM-DD>_ABC
    Returns only {date, sum}
    """
    if DATA_TABLE is None:
        return None

    key = f"{base_pid}_{dt.isoformat()}_ABC"

    try:
        resp = DATA_TABLE.get_item(Key={"AssetId": asset_id, "PropertyId": key})
    except Exception:
        logger.exception("Failed fetching daywise metric: %s", key)
        return None

    item = resp.get("Item")
    if not item:
        return None

    props = (
        item.get("properties")
        or item.get("Properties")
        or item
    )
    parsed = safe_parse_str_dict(props)

    if isinstance(parsed, dict):
        return {
            "date": dt.isoformat(),
            "sum": parsed.get("sum") or "0"
        }

    return None

def build_daywise_metric_list(asset_id: str, base_pid: str, days: int):
    today = datetime.now(tz=timezone.utc).date()
    results = []

    for i in range(1, days + 1):
        dt = today - timedelta(days=i)
        rec = get_day_record_for_metric(asset_id, base_pid, dt)

        if rec:
            results.append(rec)
        else:
            # empty placeholder
            results.append({
                "date": dt.isoformat(),
                "sum": "0"
            })

    return results


def get_historical_combined_from_dynamo(
    asset_id: str, base_pid: str, window_days: int
) -> Optional[dict]:
    prop_key = f"{base_pid}_{window_days}_ABC"
    return dynamo_get_item(asset_id, prop_key)


def get_machine_states_history(
    asset_id: str, state_pid: str, days: int
) -> List[dict]:
    results = []
    today = datetime.now(tz=timezone.utc).date()
    for i in range(1, days + 1):
        dt = today - timedelta(days=i)
        date_str = dt.isoformat()
        prop_key = f"{state_pid}_{date_str}_ABC"
        item = dynamo_get_item(asset_id, prop_key)
        if item:
            if isinstance(item, dict) and "date" not in item:
                item["date"] = date_str
            results.append(item)
        else:
            results.append(
                {
                    "date": date_str,
                    "assetName": None,
                    "shiftA": {
                        "segments": [],
                        "state_percentage": {},
                    },
                    "shiftB": {
                        "segments": [],
                        "state_percentage": {},
                    },
                    "shiftC": {
                        "segments": [],
                        "state_percentage": {},
                    }

                }
            )
    return results

# ---------- NEW helper: Derive Zone Value From Machine ----------

def derive_zone_from_machine(machine_name: str) -> Optional[str]:
    """
    Infer zone name from machine, e.g. 'SM_SP102' -> 'Zone_1'.
    Logic: first digit after 'SP' in the name.
    """
    if not machine_name or not isinstance(machine_name, str):
        return None
    try:
        m = re.search(r"SP\s*?(\d)", machine_name)
        if not m:
            return None
        zone_num = m.group(1)
        return f"Zone_{zone_num}"
    except Exception:
        logger.exception("Failed to derive zone from machine '%s'", machine_name)
        return None


# ---------- NEW helper: fetch constants TargetOA  ----------
def get_constant_from_dynamo(asset_id: str, prop_name: str) -> Optional[Any]:
    if DATA_TABLE is None:
        logger.debug("No DATA_TABLE configured - cannot fetch constants")
        return None
    try:
        val = dynamo_get_item(asset_id, prop_name)
        return val
    except Exception:
        logger.exception("Failed to fetch constant %s for asset %s", prop_name, asset_id)
        return None


# ---------- parsing helpers ----------
def parse_metric_value_generic(metric_val: Any) -> Tuple[Optional[float], Optional[str]]:
    if isinstance(metric_val, dict):
        shift = metric_val.get("Shift") or metric_val.get("shift")
        for k, v in metric_val.items():
            if k in ("Shift", "shift"):
                continue
            if isinstance(v, (int, float, Decimal)):
                return float(v), shift
            if isinstance(v, str):
                try:
                    return float(v), shift
                except Exception:
                    continue
        return None, shift
    if isinstance(metric_val, str):
        s = metric_val.strip()
        if s.startswith("{") and s.endswith("}"):
            parsed = safe_parse_str_dict(s)
            if isinstance(parsed, dict):
                return parse_metric_value_generic(parsed)
        if ":" in s:
            left, right = s.split(":", 1)
            left = left.strip()
            try:
                return float(left), right.strip() or None
            except Exception:
                return None, right.strip() or None
        try:
            return float(s), None
        except Exception:
            return None, None
    if isinstance(metric_val, (int, float, Decimal)):
        return float(metric_val), None
    return None, None


def compute_oee_from_values(avail, perf, qual, shift=None):
    if avail is None or perf is None or qual is None:
        return None
    try:
        import math

        if any(math.isnan(v) for v in (avail, perf, qual)):
            return None
    except Exception:
        pass
    prod = avail * perf * qual
    return f"{round(prod, 2)}:{shift}" if shift else round(prod, 2)


# ---------- Parallel fetch worker ----------
def _fetch_metric(args):
    asset_id, metric_key, pid = args
    sv = get_sitewise_latest_value(asset_id, pid)
    if not sv:
        return metric_key, {
            "raw": None,
            "num": None,
            "shift": None,
            "timestamp": None,
        }
    raw_val = sv.get("raw_value")
    num, shift = parse_metric_value_generic(raw_val)
    ts_raw = sv.get("timestamp")
    ts_iso = normalize_sitewise_ts(ts_raw)
    return metric_key, {"raw": raw_val, "num": num, "shift": shift, "timestamp": ts_iso}


# ---------- Utility: build state events from SiteWise history rows ----------
def build_ordered_state_events_from_history(
    records: List[dict],
) -> List[Tuple[datetime, str, str]]:
    out = []
    for r in records:
        tsobj = r.get("timestamp") or {}
        time_seconds = None
        if isinstance(tsobj, dict):
            if "timeInSeconds" in tsobj:
                time_seconds = tsobj.get("timeInSeconds")
            elif "timeInMillis" in tsobj:
                try:
                    time_seconds = int(tsobj.get("timeInMillis")) // 1000
                except Exception:
                    time_seconds = None
        sval = None
        v = r.get("value") or {}
        if isinstance(v, dict):
            sval = v.get("stringValue") or v.get("value") or None
        else:
            sval = v
        if sval is None or time_seconds is None:
            continue
        s = str(sval).strip()
        if ":" not in s:
            continue
        left, shift = s.rsplit(":", 1)
        state = left.strip()
        shift = shift.strip()
        try:
            dt = datetime.fromtimestamp(int(time_seconds), tz=timezone.utc)
        except Exception:
            continue
        out.append((dt, state, shift))
    out.sort(key=lambda x: x[0])
    return out

def compute_live_state_duration(
    asset_id: str,
    state_pid: str,
    current_state_raw: str,
) -> Optional[str]:

    if not current_state_raw or ":" not in current_state_raw:
        return None

    current_state = current_state_raw.rsplit(":", 1)[0].strip()

    # Case 1: Current is PD
    if current_state.replace(" ", "").lower() == "planneddowntime":
        return None

    try:
        now_utc = datetime.now(tz=timezone.utc)

        window_start = now_utc - timedelta(days=1)

        recs = fetch_history_with_lookback(
            asset_id=asset_id,
            property_id=state_pid,
            window_start=window_start,
            window_end=now_utc,
            initial_days=1,
            max_days=30,
        )

        if not recs:
            return None

        events = build_ordered_state_events_from_history(recs)

        if not events:
            return None

        # events.sort(key=lambda x: x[0])
        # events = events[-3:]
        events.sort(key=lambda x: x[0])

        collapsed = []
        for e in events:
            if not collapsed or collapsed[-1][1] != e[1]:
                collapsed.append(e)

        events = collapsed[-3:]        
        # Find latest event index
        last_index = None
        for i in reversed(range(len(events))):
            if events[i][1] == current_state:
                last_index = i
                break

        if last_index is None:
            return None

        current_start_time = events[last_index][0]

        # ---------------------------------------------
        # CHECK IF PREVIOUS STATE IS PD
        # ---------------------------------------------
        if last_index - 1 >= 0:
            prev_state = events[last_index - 1][1]

            if prev_state.replace(" ", "").lower() == "planneddowntime":

                # Find state before PD
                if last_index - 2 >= 0:
                    state_before_pd = events[last_index - 2][1]

                    # ✅ Only subtract if same state
                    if state_before_pd.strip().lower() == current_state.strip().lower():
                    # if state_before_pd == current_state:

                        # Calculate previous state's duration
                        prev_start_time = events[last_index - 2][0]
                        pd_start_time = events[last_index - 1][0]

                        # prev_duration_minutes = int(
                        #     (pd_start_time - prev_start_time).total_seconds() / 60
                        # )

                        # adjusted_start = current_start_time - timedelta(
                        #     minutes=prev_duration_minutes
                        # )
                        prev_duration_seconds = (
                            pd_start_time - prev_start_time
                        ).total_seconds()

                        adjusted_start = current_start_time - timedelta(
                            seconds=prev_duration_seconds
                        )
                        return adjusted_start.isoformat()

        # ---------------------------------------------
        # DEFAULT CASE (no subtraction)
        # ---------------------------------------------
        # duration_minutes = int(
        #     (now_utc - current_start_time).total_seconds() / 60
        # )

        return current_start_time.isoformat()

    except Exception:
        logger.exception("Failed computing live state duration")
        return None

# ---------- Utility: build shift segments clipped to window ----------
def build_segments_from_events(
    events: List[Tuple[datetime, str, str]],
    window_start: datetime,
    window_end: datetime,
) -> List[Tuple[datetime, datetime, str, str]]:
    segments = []
    if not events:
        return segments
    for i in range(len(events)):
        t_i, state_i, shift_i = events[i]
        t_next = events[i + 1][0] if i + 1 < len(events) else window_end
        seg_start = max(t_i, window_start)
        seg_end = min(t_next, window_end)
        if seg_start < seg_end:
            segments.append((seg_start, seg_end, state_i, shift_i))
    return segments


def build_records_for_shift(
    clipped_segments: List[Tuple[datetime, datetime, str, str]],
    requested_shift: str,
) -> List[dict]:
    out = []
    for s, e, state, shift in clipped_segments:
        if requested_shift in ("A", "B","C") and shift != requested_shift:
            continue

        duration_seconds = (e - s).total_seconds()
        duration_minutes = int(duration_seconds // 60)

        out.append(
            {
                "state": state,
                "shift": shift,
                "start_utc": s.astimezone(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "end_utc": e.astimezone(timezone.utc)
                .isoformat()
                .replace("+00:00", "Z"),
                "duration_minutes": duration_minutes,
                "duration_seconds": duration_seconds,
            }
        )
    return out


# ---------- Today processors ----------
def process_state_summary_and_machine_states_today(
    asset_id: str,
    state_pid: str,
    metrics_raw_state_entry: dict,
    window_start: datetime,
    window_end: datetime,
) -> Tuple[dict, List[dict]]:
    recs = (
        fetch_history_with_lookback(asset_id, state_pid, window_start, window_end)
        if state_pid
        else []
    )
    now_dt = window_end

    latest_raw = metrics_raw_state_entry.get("raw") if metrics_raw_state_entry else None
    latest_ts = (
        metrics_raw_state_entry.get("timestamp") if metrics_raw_state_entry else None
    )
    if latest_raw and isinstance(latest_raw, str) and ":" in latest_raw:
        synthetic_ts_sec = None
        if latest_ts:
            try:
                if isinstance(latest_ts, str):
                    dt = datetime.fromisoformat(latest_ts.replace("Z", "+00:00"))
                    synthetic_ts_sec = int(dt.timestamp())
                elif isinstance(latest_ts, (int, float)):
                    synthetic_ts_sec = int(latest_ts)
            except Exception:
                synthetic_ts_sec = None
        if synthetic_ts_sec is None:
            synthetic_ts_sec = int(now_dt.timestamp())
        try:
            synthetic = {
                "value": {"stringValue": latest_raw},
                "timestamp": {"timeInSeconds": int(synthetic_ts_sec)},
            }
            if not any(
                (
                    r.get("timestamp", {}).get("timeInSeconds")
                    == synthetic["timestamp"]["timeInSeconds"]
                    and (
                        r.get("value", {}).get("stringValue")
                        == synthetic["value"]["stringValue"]
                    )
                )
                for r in recs
            ):
                recs.append(synthetic)
        except Exception:
            pass

    events = build_ordered_state_events_from_history(recs)
    if not events:
        day_key = window_start.date().isoformat()
        now_utc = datetime.now(tz=timezone.utc)

        machine_states = [
            {
                "assetName": None,
                "date": day_key,
                "shiftA": {
                    "segments": [],
                    "state_percentage": {},
                },
                "shiftB": {
                    "segments": [],
                    "state_percentage": {},
                },
                "shiftC": {
                    "segments": [],
                    "state_percentage": {},
                },
            }
        ]
        state_summary = {
            "assetName": None,
            "days": "today",
            "generatedAtUTC": now_utc.isoformat().replace("+00:00", "Z"),
            "generatedAtCST":now_utc.astimezone(CENTRAL_TZ).isoformat(),
            "states": [],
        }
        return state_summary, machine_states

    events.sort(key=lambda x: x[0])

    if events and events[0][0] > window_start:
        first_dt, first_state, first_shift = events[0]
        events.insert(0, (window_start, first_state, first_shift))

    segments = build_segments_from_events(events, window_start, window_end)

    recA = build_records_for_shift(segments, "A")
    recB = build_records_for_shift(segments, "B")
    recC = build_records_for_shift(segments, "C")

    total_events_A = sum(1 for e in events if e[2] == "A")
    total_events_B = sum(1 for e in events if e[2] == "B")
    total_events_C = sum(1 for e in events if e[2] == "C")

    dm_A = {"shift": "A", "count": total_events_A}
    dm_B = {"shift": "B", "count": total_events_B}

    total_window_seconds = max(1, int((window_end - window_start).total_seconds()))

    def accumulate_seconds(clipped_segments, requested_shift=None):
        per_state_seconds = {}
        covered_seconds = 0
        for s, e, state, shift in clipped_segments:
            if requested_shift in ("A", "B","C") and shift != requested_shift:
                continue
            secs = int((e - s).total_seconds())
            if secs <= 0:
                continue
            per_state_seconds[state] = per_state_seconds.get(state, 0) + secs
            covered_seconds += secs
        return per_state_seconds, covered_seconds

    state_secs_A, covered_A = accumulate_seconds(segments, "A")
    state_secs_B, covered_B = accumulate_seconds(segments, "B")
    state_secs_C, covered_C = accumulate_seconds(segments, "C")

    state_secs_combined = {}
    total_covered = 0
    for s, e, state, shift in segments:
        secs = int((e - s).total_seconds())
        if secs <= 0:
            continue
        state_secs_combined[state] = state_secs_combined.get(state, 0) + secs
        total_covered += secs

    uncovered = total_window_seconds - total_covered
    if uncovered > 0:
        state_secs_combined["Unknown"] = state_secs_combined.get("Unknown", 0) + uncovered
        total_covered += uncovered

    def build_state_percentage_map(state_secs, total_secs, events_count_for_shift):
        out = {}
        for st, secs in state_secs.items():
            pct = (secs / total_secs) * 100 if total_secs > 0 else 0
            avg = (secs / events_count_for_shift) if events_count_for_shift else 0
            out[st] = {
                "state": st,
                "minutes": int(round(secs / 60.0)),
                "percentage": round(pct, 2),
                "average": avg,
            }
        total_pct = round(sum(v["percentage"] for v in out.values()), 2) if out else 0.0
        if out and total_pct != 100.0:
            max_key = max(out.keys(), key=lambda k: state_secs.get(k, 0))
            out[max_key]["percentage"] = round(
                out[max_key]["percentage"] + (100.0 - total_pct), 2
            )
        return out

    state_pct_A = build_state_percentage_map(state_secs_A, total_window_seconds, total_events_A)
    state_pct_B = build_state_percentage_map(state_secs_B, total_window_seconds, total_events_B)
    state_pct_C = build_state_percentage_map(state_secs_C,total_window_seconds,total_events_C)

    machine_states = [
        {
            "assetName": None,
            "date": window_start.date().isoformat(),
            "shiftA": {
                 "state_percentage": state_pct_A,
            },
            "shiftB": {
                "state_percentage": state_pct_B,
            },
            "shiftC":{
                "state_percentage": state_pct_C,
            }
        }
    ]

    states_items = []
    total_events = total_events_A + total_events_B + total_events_C
    for st, secs in sorted(
        state_secs_combined.items(), key=lambda kv: -kv[1]
    ):
        pct = (secs / total_window_seconds) * 100 if total_window_seconds > 0 else 0
        states_items.append(
            {
                "state": st,
                "minutes": int(round(secs / 60.0)),
                "percentage": round(pct, 2),
                "propertyName": "MachineStates",
                "assetName": None,
                "average": (secs / total_events) if total_events else 0,
            }
        )

    if states_items:
        total_pct = round(sum(si["percentage"] for si in states_items), 2)
        if total_pct != 100.0:
            idx = max(
                range(len(states_items)),
                key=lambda i: state_secs_combined.get(
                    states_items[i]["state"], 0
                ),
            )
            states_items[idx]["percentage"] = round(
                states_items[idx]["percentage"] + (100.0 - total_pct), 2
            )
    now_utc = datetime.now(tz=timezone.utc)


    state_summary = {
        "assetName": None,
        "days": "today",
        "generatedAtUTC": now_utc.isoformat().replace("+00:00", "Z"),
        "generatedAtCST":now_utc.astimezone(CENTRAL_TZ).isoformat(),
        "states": states_items,
    }

    return state_summary, machine_states

def upsert_targets(
    zone_name: str,
    metric: str,
    machine_name: str,
    metric_value: float,
) -> Dict[str, Any]:

    if DATA_TABLE is None:
        raise Exception("DATA_TABLE not configured")

    key = {"AssetId": "All", "PropertyId": "Targets"}

    resp = DATA_TABLE.get_item(Key=key)
    item = resp.get("Item")

    if item:
        raw_props = item.get("properties") or item
        current_map = safe_parse_str_dict(raw_props)
        if not isinstance(current_map, dict):
            current_map = {}
    else:
        current_map = {}

    # ---- Ensure zone exists ----
    zone_obj = current_map.get(zone_name, {})

    # ---- Ensure metric bucket exists ----
    metric_obj = zone_obj.get(metric, {})

    # ---- Update machine value ----
    metric_obj[machine_name] = float(metric_value)

    # ---- Recalculate Common_Zone_Value ----
    machine_values = [
        float(v)
        for k, v in metric_obj.items()
        if k != "Common_Zone_Value"
    ]

    if machine_values:
        metric_obj["Common_Zone_Value"] = round(
            sum(machine_values) / len(machine_values), 2
        )

    zone_obj[metric] = metric_obj
    current_map[zone_name] = zone_obj

    # ---- Write back ----
    new_item = {
        "AssetId": "All",
        "PropertyId": "Targets",
        "properties": json.dumps(current_map, default=json_default),
    }

    DATA_TABLE.put_item(Item=new_item)

    return current_map
def process_downtime_summary_today(
    asset_id: str,
    downtime_pid: str,
    metrics_raw_downtime_entry: dict,
    window_start: datetime,
    window_end: datetime,
) -> Optional[List[dict]]:
    recs = (
        fetch_history_range(asset_id, downtime_pid, window_start, window_end)
        if downtime_pid
        else []
    )
    now_dt = window_end

    latest_raw = (
        metrics_raw_downtime_entry.get("raw") if metrics_raw_downtime_entry else None
    )
    latest_ts = (
        metrics_raw_downtime_entry.get("timestamp") if metrics_raw_downtime_entry else None
    )

    if latest_raw and isinstance(latest_raw, str):
        s = latest_raw.strip()
        synthetic_ts_sec = None
        if latest_ts:
            try:
                if isinstance(latest_ts, str):
                    dt = datetime.fromisoformat(latest_ts.replace("Z", "+00:00"))
                    synthetic_ts_sec = int(dt.timestamp())
                elif isinstance(latest_ts, (int, float)):
                    synthetic_ts_sec = int(latest_ts)
            except Exception:
                synthetic_ts_sec = None
        if synthetic_ts_sec is None:
            synthetic_ts_sec = int(now_dt.timestamp())
        synthetic = {
            "value": {"stringValue": s},
            "timestamp": {"timeInSeconds": synthetic_ts_sec},
        }
        if not any(
            (
                r.get("value", {}).get("stringValue") == s
                and r.get("timestamp", {}).get("timeInSeconds")
                == synthetic_ts_sec
            )
            for r in recs
        ):
            recs.append(synthetic)

    reason_minutes = {}
    total_minutes = 0.0
    reason_counts = {}

    for r in recs:
        sval = None
        v = r.get("value") or {}
        if isinstance(v, dict):
            sval = v.get("stringValue") or v.get("value")
        else:
            sval = v
        if not sval:
            continue
        s = str(sval).strip()
        parsed_reason = None
        minutes = None
        try:
            if ":" in s:
                parts = s.rsplit(":", 2)
                if len(parts) == 3:
                    parsed_reason = parts[0].strip()
                    dur = float(parts[2])
                    minutes = dur / 60.0
            elif s.startswith("{") and s.endswith("}"):
                d = safe_parse_str_dict(s)
                if isinstance(d, dict):
                    parsed_reason = d.get("reason") or d.get("type") or None
                    dur_s = (
                        d.get("duration_sec")
                        or d.get("duration")
                        or d.get("durationSeconds")
                    )
                    if dur_s is not None:
                        minutes = float(dur_s) / 60.0
        except Exception:
            logger.exception("Failed parsing downtime record %s", s)
            continue

        if parsed_reason is None or minutes is None:
            continue
        reason_minutes[parsed_reason] = reason_minutes.get(parsed_reason, 0.0) + minutes
        reason_counts[parsed_reason] = reason_counts.get(parsed_reason, 0) + 1
        total_minutes += minutes

    if total_minutes == 0:
        return []

    results = []
    for reason, mins in sorted(reason_minutes.items(), key=lambda kv: -kv[1]):
        count = reason_counts.get(reason, 1)
        avg = mins / count
        pct = (mins / total_minutes) * 100 if total_minutes > 0 else 0
        results.append(
            {
                "reason": reason,
                "minutes": mins,
                "percentage": pct,
                "average": avg,
            }
        )
    return results

def normalize_zone_name(zone_raw: str) -> str:
    """
    Normalise various zone inputs to a canonical form: 'Zone_<n>'.

    Examples:
      'Zone_1'  -> 'Zone_1'
      'zone_1'  -> 'Zone_1'
      'ZONE_1'  -> 'Zone_1'
      '1'       -> 'Zone_1'
      'zone_2 ' -> 'Zone_2'
    """
    if not zone_raw:
        return zone_raw
    s = str(zone_raw).strip()

    # case-insensitive match for zone_<n>
    m = re.match(r"(?i)zone_(\d+)", s)
    if m:
        return f"Zone_{m.group(1)}"

    # just a plain number -> Zone_<n>
    if s.isdigit():
        return f"Zone_{s}"

    # fallback: return as-is (for weird custom keys)
    return s

# ---------- New helper: convert historical downtime_reasons object -> list ----------
def historical_downtimeresponses_to_list(
    hist_obj: Any,
) -> Optional[List[dict]]:
    if hist_obj is None:
        return None
    if isinstance(hist_obj, list):
        return hist_obj
    try:
        reasons_map = None
        if isinstance(hist_obj, dict):
            reasons_map = (
                hist_obj.get("reasons")
                or hist_obj.get("reasons_map")
                or None
            )
        if not isinstance(reasons_map, dict):
            if isinstance(hist_obj, dict):
                all_values_dict = (
                    all(isinstance(v, dict) for v in hist_obj.values())
                    if hist_obj
                    else False
                )
                if all_values_dict:
                    reasons_map = hist_obj
        if not isinstance(reasons_map, dict):
            return []
        out_list = []
        for rkey, rval in reasons_map.items():
            if isinstance(rval, dict):
                mins = rval.get("minutes") or rval.get("minutes_total") or 0
                pct = rval.get("percentage") or 0
                avg = rval.get("average") or rval.get("avg") or 0
            else:
                try:
                    mins = float(rval)
                    pct = 0
                    avg = 0
                except Exception:
                    mins = 0
                    pct = 0
                    avg = 0
            out_list.append(
                {
                    "reason": rkey,
                    "minutes": mins,
                    "percentage": pct,
                    "average": avg,
                }
            )
        out_list.sort(key=lambda x: -float(x.get("minutes", 0)))
        return out_list
    except Exception:
        logger.exception("historical_downtimeresponses_to_list failed")
        return []


def process_numeric_today(
    records: List[dict], days: int, minutes_window: Optional[int] = None
) -> Dict[str, Any]:
    A_sum = B_sum = C_sum = Decimal("0")
    A_count = B_count = C_count = 0

    for rec in records:
        sval = None
        v = rec.get("value") or {}
        if isinstance(v, dict):
            sval = v.get("stringValue") or v.get("value")
        else:
            sval = v
        if sval is None:
            continue
        s = str(sval).strip()
        try:
            if ":" in s:
                left, shift = s.rsplit(":", 1)
                shift = shift.strip()
                left = left.strip()
                if left.startswith("{") and left.endswith("}"):
                    dd = safe_parse_str_dict(left)
                    if isinstance(dd, dict):
                        found = None
                        for k, vv in dd.items():
                            if isinstance(vv, (int, float, Decimal, str)):
                                try:
                                    num = Decimal(str(vv))
                                    found = num
                                    break
                                except Exception:
                                    continue
                        if found is None:
                            continue
                        num_val = found
                    else:
                        continue
                else:
                    try:
                        num_val = Decimal(left)
                    except Exception:
                        continue
                if shift == "A":
                    A_sum += num_val
                    A_count += 1
                elif shift == "B":
                    B_sum += num_val
                    B_count += 1
                elif shift == "C":
                    C_sum += num_val
                    C_count += 1
            else:
                try:
                    num_val = Decimal(s)
                    A_sum += num_val
                    A_count += 1
                except Exception:
                    continue
        except Exception:
            logger.exception("process_numeric_today parse error for %s", s)
            continue

    combined_sum = A_sum + B_sum + C_sum
    combined_count = A_count + B_count + C_count
    if minutes_window is not None:
        total_minutes = Decimal(minutes_window) if minutes_window > 0 else Decimal(1)
    else:
        total_minutes = Decimal(days * 24 * 60) if days > 0 else Decimal(1)
    combined_percentage = (
        (combined_sum / total_minutes * 100) if total_minutes > 0 else Decimal("0")
    )
    avg = (combined_sum / combined_count) if combined_count else Decimal("0")

    return {
        "sum": str(combined_sum),
        "average": str(avg),
        "percentage": str(combined_percentage),
        "shift_count": combined_count,
    }



def process_single_live_asset(asset, shadow_data, today_flag, hist_days, today_start, today_end, now_dt, parts_daywise, states_daywise):
    zones_cfg = shadow_data.get("zones", {})
    asset_id = asset.get("id")
    asset_name = asset.get("name")
    prop_map = map_property_name_to_id(asset_id)

    metrics_raw = {}
    for key, prefix in METRIC_PREFIXES.items():
        pid = prop_map.get(prefix)
        if pid:
            try:
                sv = get_sitewise_latest_value(asset_id, pid)
                if sv:
                    raw_val = sv.get("raw_value")
                    num, shift = parse_metric_value_generic(raw_val)
                    ts_iso = normalize_sitewise_ts(sv.get("timestamp"))
                    metrics_raw[key] = {"raw": raw_val, "num": num, "shift": shift, "timestamp": ts_iso}
                else:
                    metrics_raw[key] = {"raw": None, "num": None, "shift": None, "timestamp": None}
            except Exception:
                metrics_raw[key] = {"raw": None, "num": None, "shift": None, "timestamp": None}

    for mk in METRIC_PREFIXES.keys():
        if mk not in metrics_raw:
            metrics_raw[mk] = {"raw": None, "num": None, "shift": None, "timestamp": None}

    for key in NUMERIC_METRIC_KEYS:
        if key in metrics_raw:
            raw = metrics_raw[key]["raw"]
            if raw is None: metrics_raw[key]["raw"] = "0.0"
            elif isinstance(raw, str) and (raw.strip().lower() in ("none", "null") or raw.startswith("object")):
                metrics_raw[key]["raw"] = "0.0"

    asset_obj = {"asset_id": asset_id, "asset_name": asset_name}
    
    for k, v in metrics_raw.items():
        if k == "state":
            raw_state = v.get("raw")
            asset_obj[k] = raw_state
            if not raw_state:
                asset_obj["state_timestamp"] = None
                continue
            state_name = str(raw_state).split(":")[0].strip()
            if state_name.replace(" ", "").lower() == "planneddowntime":
                asset_obj["state_timestamp"] = None
            else:
                state_pid = prop_map.get(METRIC_PREFIXES.get("state"))
                asset_obj["state_timestamp"] = compute_live_state_duration(asset_id, state_pid, raw_state)
        else:
            asset_obj[k] = v.get("raw")

    zone_key = derive_zone_from_machine(asset_name)
    asset_obj["Status"] = zones_cfg.get(zone_key.lower(), {}).get("stations", {}).get(asset_name, {}).get("status", "Unknown") if zone_key else "Unknown"

    for k in PERCENT_LIKE_KEYS:
        if k in asset_obj:
            val = asset_obj[k]
            if val is None: pass
            elif isinstance(val, (int, float, Decimal)): asset_obj[k] = float(val) * 100.0
            elif isinstance(val, str) and ":" in val:
                try: asset_obj[k] = f"{round(float(val.split(':')[0]) * 100.0, 2)}:{val.split(':')[1].strip()}"
                except: pass
            else:
                try: asset_obj[k] = round(float(val) * 100.0, 2)
                except: pass

    # ================= TODAY FLAG LOGIC =================
    if today_flag:
        today_obj: Dict[str, Any] = {}
        window_minutes = int((now_dt - today_start).total_seconds() // 60)
        today_obj["window_minutes"] = window_minutes
        now_utc = datetime.now(tz=timezone.utc)
        today_obj["generatedAt"] = {
            "utc": now_utc.isoformat().replace("+00:00", "Z"),
            "cst": now_utc.astimezone(CENTRAL_TZ).isoformat()
        }
        metrics: Dict[str, Any] = {}

        # 1) STATES
        state_pid = prop_map.get(METRIC_PREFIXES.get("state"))
        state_raw_entry = metrics_raw.get("state", {})
        try:
            state_summary, machine_states_list = process_state_summary_and_machine_states_today(
                asset_id, state_pid, state_raw_entry, today_start, min(now_dt, today_end)
            )
            state_summary["assetName"] = asset_name if state_summary.get("assetName") is None else state_summary.get("assetName")
            for st in state_summary.get("states", []): st["assetName"] = asset_name
            for ms in machine_states_list: ms["assetName"] = asset_name
            metrics["state"] = state_summary
            today_obj["machine_states"] = machine_states_list
        except Exception:
            now_utc = datetime.now(tz=timezone.utc)
            metrics["state"] = {
                "assetName": asset_name, "days": "today",
                "generatedAtUTC": now_utc.isoformat().replace("+00:00", "Z"),
                "generatedAtCST": now_utc.astimezone(CENTRAL_TZ).isoformat(),
                "states": [],
            }
            today_obj["machine_states"] = []

        # 2) DOWNTIME REASONS
        downtime_pid = prop_map.get(METRIC_PREFIXES.get("downtime_reasons"))
        downtime_entry = metrics_raw.get("downtime_reasons", {})
        try:
            dr_summary = process_downtime_summary_today(asset_id, downtime_pid, downtime_entry, today_start, min(now_dt, today_end))
            metrics["downtime_reasons"] = dr_summary if dr_summary is not None else []
        except Exception:
            metrics["downtime_reasons"] = []

        # 3) NUMERIC METRICS FOR TODAY
        for metric_key, prefix in HISTORICAL_METRIC_PREFIXES.items():
            if metric_key in ("state", "downtime_reasons") or metric_key in TODAY_SKIP_METRICS: continue
            pid = prop_map.get(prefix)
            if not pid:
                metrics[metric_key] = None
                continue
            try:
                recs = fetch_history_range(asset_id, pid, today_start, min(now_dt, today_end))
                latest_item = metrics_raw.get(metric_key) or {}
                if latest_item and latest_item.get("raw") is not None:
                    sval, ts_iso = latest_item.get("raw"), latest_item.get("timestamp")
                    synthetic_ts = int(datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).timestamp()) if ts_iso else int(now_dt.timestamp())
                    synthetic = {"value": {"stringValue": str(sval)}, "timestamp": {"timeInSeconds": synthetic_ts}}
                    if not any((r.get("value", {}).get("stringValue") == synthetic["value"]["stringValue"] and r.get("timestamp", {}).get("timeInSeconds") == synthetic["timestamp"]["timeInSeconds"]) for r in recs):
                        recs.append(synthetic)
                metrics[metric_key] = process_numeric_today(recs, 1, minutes_window=window_minutes)
            except Exception:
                metrics[metric_key] = None

        # Today util numeric summary
        try:
            util_pid = prop_map.get(METRIC_PREFIXES.get("today_util"))
            if util_pid:
                recs_util = fetch_history_range(asset_id, util_pid, today_start, min(now_dt, today_end))
                latest_util_item = metrics_raw.get("today_util", {})
                if latest_util_item and latest_util_item.get("raw") is not None:
                    sval, ts_iso = latest_util_item.get("raw"), latest_util_item.get("timestamp")
                    synthetic_ts = int(datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).timestamp()) if ts_iso else int(now_dt.timestamp())
                    synthetic = {"value": {"stringValue": str(sval)}, "timestamp": {"timeInSeconds": synthetic_ts}}
                    if not any((r.get("value", {}).get("stringValue") == synthetic["value"]["stringValue"] and r.get("timestamp", {}).get("timeInSeconds") == synthetic["timestamp"]["timeInSeconds"]) for r in recs_util):
                        recs_util.append(synthetic)
                metrics["today_util"] = process_numeric_today(recs_util, 1, minutes_window=window_minutes)
            else:
                metrics["today_util"] = None
        except Exception:
            pass

        # Current qty numeric summary
        try:
            cur_pid = prop_map.get(METRIC_PREFIXES.get("current_qty"))
            if cur_pid:
                recs_cur = fetch_history_range(asset_id, cur_pid, today_start, min(now_dt, today_end))
                latest_cur_item = metrics_raw.get("current_qty", {})
                if latest_cur_item and latest_cur_item.get("raw") is not None:
                    sval, ts_iso = latest_cur_item.get("raw"), latest_cur_item.get("timestamp")
                    synthetic_ts = int(datetime.fromisoformat(ts_iso.replace("Z", "+00:00")).timestamp()) if ts_iso else int(now_dt.timestamp())
                    synthetic = {"value": {"stringValue": str(sval)}, "timestamp": {"timeInSeconds": synthetic_ts}}
                    if not any((r.get("value", {}).get("stringValue") == synthetic["value"]["stringValue"] and r.get("timestamp", {}).get("timeInSeconds") == synthetic["timestamp"]["timeInSeconds"]) for r in recs_cur):
                        recs_cur.append(synthetic)
                metrics["current_qty"] = process_numeric_today(recs_cur, 1, minutes_window=window_minutes)
            else:
                metrics["current_qty"] = None
        except Exception:
            metrics["current_qty"] = None

        def _scale_numeric_summary(summary: Any):
            if not isinstance(summary, dict): return summary
            for k in ("sum", "average"):
                if k in summary and summary[k] is not None:
                    try:
                        v = float(summary[k])
                        summary[k] = str(v * 100.0)
                    except Exception: pass
            return summary

        for k in PERCENT_LIKE_KEYS:
            if k in TODAY_SKIP_METRICS: continue
            if k in metrics and metrics[k] is not None:
                metrics[k] = _scale_numeric_summary(metrics[k])

        today_obj["metrics"] = metrics
        asset_obj["today"] = today_obj

    # ================= HISTORICAL FLAG LOGIC =================
    if hist_days and DATA_TABLE is not None:
        asset_obj.setdefault("historical", {})
        asset_obj["historical"]["window_days"] = hist_days
        hist_metrics: Dict[str, Any] = {}

        for metric_key, prefix in HISTORICAL_METRIC_PREFIXES.items():
            base_pid = prop_map.get(prefix)
            hist_metrics.setdefault(metric_key, {})
            if not base_pid:
                hist_metrics[metric_key] = None
                continue
            combined = get_historical_combined_from_dynamo(asset_id, base_pid, hist_days)

            if metric_key == "downtime_reasons":
                try:
                    dr_list = historical_downtimeresponses_to_list(combined)
                    hist_metrics[metric_key] = dr_list if dr_list is not None else []
                except Exception:
                    hist_metrics[metric_key] = []
            else:
                hist_metrics[metric_key] = combined

        if "state" not in hist_metrics:
            state_prefix = prop_map.get(METRIC_PREFIXES.get("state"))
            hist_metrics["state"] = {f"{hist_days}_ABC": get_historical_combined_from_dynamo(asset_id, state_prefix, hist_days) if state_prefix else None}

        asset_obj["historical"]["metrics"] = hist_metrics

        if states_daywise:
            state_pid = prop_map.get(METRIC_PREFIXES.get("state"))
            if state_pid:
                states_history = get_machine_states_history(asset_id, state_pid, hist_days)
                asset_obj["historical"]["machine_states"] = states_history
            else:
                asset_obj["historical"]["machine_states"] = []
        else:
            asset_obj["historical"]["machine_states"] = []

        if parts_daywise:
            parts_pid = prop_map.get(METRIC_PREFIXES.get("parts_produced"))
            remaining_pid = prop_map.get(METRIC_PREFIXES.get("remaining_parts"))
            lost_pid = prop_map.get(METRIC_PREFIXES.get("lost_parts"))

            asset_obj["historical"]["parts_daywise"] = build_daywise_metric_list(asset_id, parts_pid, hist_days) if parts_pid else []
            asset_obj["historical"]["remaining_daywise"] = build_daywise_metric_list(asset_id, remaining_pid, hist_days) if remaining_pid else []
            asset_obj["historical"]["lost_daywise"] = build_daywise_metric_list(asset_id, lost_pid, hist_days) if lost_pid else []
        else:
            asset_obj["historical"]["parts_daywise"] = []
            asset_obj["historical"]["remaining_daywise"] = []
            asset_obj["historical"]["lost_daywise"] = []

    return asset_name, asset_obj, zone_key

# ---------- Main handler ----------
def lambda_handler(event, context):
    logger.info("Incoming event keys: %s", list(event.keys()))
    # parse qsp
    qsp = {}
    if isinstance(event.get("queryStringParameters"), dict):
        qsp = event.get("queryStringParameters") or {}
    elif isinstance(event.get("queryStringParameters"), str):
        try:
            qsp = json.loads(event["queryStringParameters"])
        except Exception:
            qsp = {}

    machine = qsp.get("machine") or qsp.get("asset") or ""
    hist_flag = (
        qsp.get("historical") or qsp.get("history") or qsp.get("hist")
    )
    hist_days = None
    if hist_flag:
        try:
            hist_days = int(str(hist_flag))
            if hist_days not in (7, 30,180,365):
                logger.info(
                    "Unsupported historical window %s; ignoring", hist_days
                )
                hist_days = None
        except Exception:
            hist_days = None

    def _truthy(x):
        if x is None:
            return False
        if isinstance(x, bool):
            return x
        s = str(x).strip().lower()
        return s in ("1", "true", "yes", "y", "t")

    # DAYWISE STATES FLAG 
    states_daywise_flag_raw = (
        qsp.get("states_daywise")
        or qsp.get("states_day")
        or qsp.get("states_daywise_flag")
    )

    states_daywise = _truthy(states_daywise_flag_raw)

    # # DAYWISE PARTS FLAG
    parts_daywise_flag_raw = (
        qsp.get("parts_daywise")
        or qsp.get("daywise_parts")
        or qsp.get("parts_history")
    )
    parts_daywise = _truthy(parts_daywise_flag_raw)

    # today flag: when true compute today's accumulations from SiteWise history
    today_flag_raw = (
        qsp.get("today") or qsp.get("today_flag") or qsp.get("today_true")
    )
    today_flag = _truthy(today_flag_raw)
    # ------------------------------
    # Method detection (HTTP API safe)
    # ------------------------------
    http_method = (
        event.get("requestContext", {})
        .get("http", {})
        .get("method")
        or event.get("httpMethod")
        or "GET"
    ).upper()
    # OKTA STUFF
    # =====================================================
    # OKTA: LIST USERS
    # =====================================================
    if http_method == "GET" and qsp.get("okta") == "list_users":
        try:
            raw_users = okta_list_users()
            enriched = []

            for u in raw_users:
                uid = u.get("id")
                try:
                    g_map = okta_get_groups(uid)  # returns {groupName: groupId}
                    groups = list(g_map.keys())
                except Exception:
                    groups = []

                enriched.append({
                    **u,
                    "groups": groups
                })

            return response(200, {
                "message": "OKTA users fetched",
                "users": enriched
            })

        except Exception as e:
            logger.exception("OKTA list_users failed")
            return response(500, {"error": str(e)})

    # =====================================================
    # OKTA: UPDATE USER GROUPS ONLY
    # =====================================================
    if http_method == "PUT" and qsp.get("okta") == "update_groups":
        try:
            body = json.loads(event.get("body") or "{}")
        except:
            return response(400, {"error": "Invalid JSON body"})

        user_id = body.get("userId")
        new_groups = body.get("groups", [])

        if not user_id:
            return response(400, {"error": "userId is required"})
        if not isinstance(new_groups, list):
            return response(400, {"error": "groups must be a list"})

        try:
            current = okta_get_groups(user_id)  # {name: id}
            current_names = set(current.keys())
            new_names = set(new_groups)

            to_add = new_names - current_names
            to_remove = current_names - new_names

            # assign
            for g in to_add:
                gid = okta_lookup_group_id(g)
                if gid:
                    okta_assign_group(user_id, gid)

            # remove
            for g in to_remove:
                gid = current[g]
                okta_remove_group(user_id, gid)

            return response(200, {
                "message": "Groups updated",
                "userId": user_id,
                "addedGroups": list(to_add),
                "removedGroups": list(to_remove)
            })

        except Exception as e:
            logger.exception("OKTA update_groups failed")
            return response(500, {"error": str(e)})

    # =====================================================
    # DELETE MACHINE — CLEANUP ZONE WISE
    # =====================================================
    delete_machine_value = qsp.get("delete_machine") or qsp.get("machine")

    if http_method == "DELETE" and delete_machine_value:
        try:
            machine_name = delete_machine_value.strip()

            _validate_station_id(machine_name)

            model_id = get_model_id_by_name(ASSET_MODEL_NAME)

            res = delete_asset_by_name(model_id, machine_name)

            if res.get("deleted"):

                zone_key = derive_zone_from_machine(machine_name)

                #  CLEAN TARGETS TABLE
                if zone_key:
                    try:
                        updated_targets = remove_machine_from_targets(
                            zone_key,
                            machine_name
                        )
                    except Exception:
                        logger.exception("Targets cleanup failed after asset delete")
                        # We log it but DO NOT fail asset deletion
                        updated_targets = None

                    # Shadow cleanup (existing logic)
                    dispatch_shadow_action("remove_machine", {
                        "zone": zone_key.lower(),
                        "stationId": machine_name,
                        "status": "Active"
                    })

                return response(200, {
                    "message": "Asset deleted",
                    "assetName": machine_name,
                    "zone": zone_key,
                    "targetsUpdated": updated_targets
                })

            return response(400, {
                "error": res.get("reason") or "Delete failed"
            })

        except ValueError as ve:
            return response(400, {"error": str(ve)})

        except Exception:
            logger.exception("Unexpected delete error")
            return response(500, {"error": "Internal Server Error"})
    # =====================================================
    #  GET: RETURN FULL CONFIG FILE FOR FE
    # =====================================================
    if http_method == "GET" and _truthy(qsp.get("get_config")):
        try:
            shadow = get_current_shadow()

            return response(200, {
                "message": "Config",
                "data": shadow.get("zones", {}),
                "shiftTemplates": shadow.get("shiftTemplates", {})
            })

        except Exception as e:
            logger.exception("get_config failed")
            return response(500, {"error": "Internal Server Error"})
    # =====================================================
    #  PUT : Add Machine Asset and Upsert Station
    # =====================================================
    if http_method == "PUT" and _truthy(qsp.get("add_machine")):

        try:
            body = json.loads(event.get("body") or "{}")

            _require(body, ["machine", "station"])

            machine = body["machine"].strip()
            station = body["station"]

            _validate_station_id(machine)
            _validate_status(station.get("status"))

            # -------------------------------------------------------
            # 1) DERIVE ZONE FIRST
            # -------------------------------------------------------
            zone_key = derive_zone_from_machine(machine)

            if not zone_key:
                return response(400, {"error": "Could not derive zone from machine"})

            zone_norm = zone_key.lower()

            # -------------------------------------------------------
            # 2) READ SHADOW BEFORE ANY WRITE
            # -------------------------------------------------------
            shadow = get_current_shadow() or {}
            zones_json = shadow.get("zones", {})

            zone_already = zone_norm in zones_json

            # -------------------------------------------------------
            # 3) NEW ZONE → SHIFTS REQUIRED
            # -------------------------------------------------------
            if not zone_already:

                shifts_val = station.get("shifts")

                if not shifts_val:
                    return response(400, {
                        "error": "shifts mandatory when adding first machine to new zone — machine NOT added in SiteWise"
                    })

                _validate_shift_type(shifts_val)

                # ONLY NOW CREATE SITEWISE ✔
                model_id = get_model_id_by_name(ASSET_MODEL_NAME)

                if asset_exists_for_model(model_id, machine):
                    return response(409, {"error": "Asset already exists"})

                asset_id = create_asset_for_machine(model_id, machine)
                # ENABLING ALIAS NAME AND MQTT STATUS WHILE ADDING A MACHINE 
                wait_for_asset_active(asset_id)
                enable_alias_and_mqtt_for_asset(asset_id,machine,ASSET_MODEL_NAME)
                # insert zone + machine with existing action
                dispatch_shadow_action("move_zone", {
                    "zone": zone_norm,
                    "shifts": shifts_val
                })

                # machine added via existing action ONLY
                dispatch_shadow_action("upsert_station", {
                    "zone": zone_norm,
                    "stationId": machine,
                    "status": station["status"]
                })

                return response(201, {
                    "message": "Zone created and machine added successfully",
                    "assetName": machine,
                    "zone": zone_norm,
                    "shiftTemplate": shifts_val
                })

            # -------------------------------------------------------
            # 4) EXISTING ZONE → IGNORE SHIFTS, ADD MACHINE
            # -------------------------------------------------------
            else:

                model_id = get_model_id_by_name(ASSET_MODEL_NAME)

                if asset_exists_for_model(model_id, machine):
                    return response(409, {"error": "Asset already exists"})

                asset_id = create_asset_for_machine(model_id, machine)
                # ENABLING ALIAS NAME AND MQTT STATUS WHILE ADDING A MACHINE 
                wait_for_asset_active(asset_id)
                enable_alias_and_mqtt_for_asset(asset_id,machine,ASSET_MODEL_NAME)

                existing_template = zones_json.get(zone_norm, {}).get("shiftTemplate")

                # machine insertion with existing action ONLY
                dispatch_shadow_action("upsert_station", {
                    "zone": zone_norm,
                    "stationId": machine,
                    "status": station["status"]
                })

                return response(200, {
                    "message": "Zone already exists with shiftTemplate: "
                            + str(existing_template)
                            + ". Machine has been added successfully",
                    "assetName": machine,
                    "zone": zone_norm,
                    "currentShiftTemplate": existing_template,
                    "note": "Incoming shifts ignored as per business rule"
                })

        except ValueError as ve:
            return response(400, {"error": str(ve)})

        except Exception:
            logger.exception("add_machine failed for %s", machine)
            return response(500, {"error": "Internal Server Error"})
    # =====================================================
    #  PUT: UPDATE WORKING DAYS
    # =====================================================
    if http_method == "PUT" and _truthy(qsp.get("update_working_days")):
        try:
            body = json.loads(event.get("body") or "{}")

            zone = body.get("zone")
            working_days = body.get("working_days")

            if zone is None or working_days is None:
                return response(400, {
                    "error": "zone and working_days are required"
                })

            upsert_zone_working_days(zone, working_days)

            return response(200, {
                "message": "Working days updated",
                "zone": zone.lower(),
                "working_days": sorted(set(working_days))
            })

        except ValueError as ve:
            return response(400, {"error": str(ve)})

        except Exception:
            logger.exception("update_working_days failed")
            return response(500, {"error": "Internal Server Error"})

    # =====================================================
    #  PUT: MOVE ZONE (ZONE-WISE SHIFT TEMPLATE)
    # =====================================================
    if http_method == "PUT" and _truthy(qsp.get("move_zone")):
        try:
            body = json.loads(event.get("body") or "{}")

            _require(body, ["zone", "shifts"])
            _validate_shift_type(body["shifts"])

            zone_key = body["zone"].strip().lower()

            dispatch_shadow_action("move_zone", {
                "zone": zone_key,
                "shifts": body["shifts"]
            })

            return response(200, {
                "message": "Zone shift template updated",
                "zone": zone_key,
                "shiftTemplate": body["shifts"]
            })

        except ValueError as ve:
            return response(400, {"error": str(ve)})

        except Exception:
            logger.exception("Zone shift update failed")
            return response(500, {"error": "Internal Server Error"})


    # =====================================================
    #  PUT: UPDATE STATUS
    # =====================================================
    if http_method == "PUT" and _truthy(qsp.get("update_status")):
        try:
            body = json.loads(event.get("body") or "{}")

            _require(body, ["stationId", "status"])
            _validate_station_id(body["stationId"])
            _validate_status(body["status"])

            #  zone-wise lookup
            zone_key = derive_zone_from_machine(body["stationId"])

            dispatch_shadow_action("update_status", {
                "stationId": body["stationId"],
                "status": body["status"],
                "zone": zone_key
            })

            return response(200, {"message": "Status updated"})

        except ValueError as ve:
            return response(400, {"error": str(ve)})

        except Exception:
            logger.exception("Status update failed")
            return response(500, {"error": "Internal Server Error"})
   
    if http_method == "PUT" and _truthy(qsp.get("update_shift_timings")):
        try:
            body = json.loads(event.get("body") or "{}")

            _require(body, ["shiftType", "shifts"])
            _validate_shift_type(body["shiftType"])

            shifts = body["shifts"]
            if not isinstance(shifts, list) or not shifts:
                raise ValueError("shifts must be a non-empty list")

            shadow = get_current_shadow()
            templates = shadow.get("shiftTemplates", {}).get(body["shiftType"], {})

            prepared_updates = {}

            for s in shifts:
                _require(s, ["shiftCode"])
                _validate_shift_code(s["shiftCode"], body["shiftType"])

                existing = templates.get(s["shiftCode"], {})
                if not existing:
                    raise ValueError(f"{s['shiftCode']} not found")

                # Resolve effective shift time
                start = s.get("start", existing.get("start"))
                end = s.get("end", existing.get("end"))

                if not start or not end:
                    raise ValueError(f"start/end missing for {s['shiftCode']}")

                _validate_time_hhmm(start)
                _validate_time_hhmm(end)

                # Break validation (if provided)
                breaks = s.get("breaks")
                if breaks is not None:
                    _validate_breaks_bulk(
                        breaks=breaks,
                        shift_start=start,
                        shift_end=end
                    )

                prepared_updates[s["shiftCode"]] = {
                    "start": start,
                    "end": end,
                    **({"breaks": breaks} if breaks is not None else {})
                }

            # Cross-shift overlap validation (NEW timings)
            _validate_no_overlap_shift_time_bulk(
                body["shiftType"],
                prepared_updates
            )

            # Atomic shadow update
            dispatch_shadow_action(
                "update_shift_config_bulk",
                {
                    "shiftType": body["shiftType"],
                    "shifts": prepared_updates
                }
            )

            # EventBridge logic
            if body["shiftType"] == "Shift 2" and "Shift 2-A" in prepared_updates:
                update_eventbridge_triggers(
                    prepared_updates["Shift 2-A"]["start"]
                )

            return response(200, {
                "message": "Shift configuration updated",
                "shiftType": body["shiftType"],
                "updatedShifts": list(prepared_updates.keys())
            })

        except ValueError as ve:
            return response(400, {
                "error": "ValidationError",
                "message": str(ve)
            })

        except Exception:
            logger.exception("update_shift_config failed")
            return response(500, {
                "error": "InternalServerError"
            })


    update_targets = _truthy(qsp.get("update_targets"))

    if http_method == "PUT" and update_targets:
        try:
            body = json.loads(event.get("body") or "{}")
        except Exception:
            return response(400, {"error": "Invalid JSON body"})

        zone_raw = body.get("zone") or qsp.get("zone")
        metric = body.get("metric") or qsp.get("metric")
        machine = body.get("machine")
        value = body.get("value")

            
        if not all([zone_raw, metric, machine, value]):
            return response(400, {
                "error": "zone, metric, machine and value are required",
                "allowed_metrics": ["OA", "Quality", "OEE", "Utilization"]
            })

        zone = normalize_zone_name(zone_raw)

        if not zone.lower().startswith("zone_"):
            zone = f"Zone_{zone}"

        # VALIDATION STARTS HERE
        _validate_station_id(machine)

        derived_zone = derive_zone_from_machine(machine)

        if not derived_zone:
            return response(400, {"error": "Cannot derive zone from machine name"})

        if derived_zone.lower() != zone.lower():
            return response(400, {
                "error": f"Machine {machine} does not belong to {zone}"
            })
        # VALIDATION ENDS HERE

        model_id = get_model_id_by_name(ASSET_MODEL_NAME)

        asset_id = get_asset_id_by_name(model_id, machine)

        if not asset_id:
            return response(400, {
                "error": f"Machine {machine} does not exist"
            })

        try:
            value_f = float(value)
        except:
            return response(400, {"error": "value must be numeric"})

        try:
            updated = upsert_targets(zone, metric, machine, value_f)
        except Exception as e:
            logger.exception("Targets update failed")
            return response(500, {"error": str(e)})

        return response(200, {
            "message": "Target updated",
            "zone": zone,
            "metric": metric,
            "machine": machine,
            "value": value_f,
            "Targets": updated
        })        

    # --------- normal GET-style behaviour below (unchanged) ---------

    # load assets
    try:
        model_id = get_model_id_by_name(ASSET_MODEL_NAME)
    except Exception as e:
        logger.exception("Failed to get model id")
        return response(500, {"error": str(e)})
    try:
        all_assets = list_assets_for_model(model_id)
        assets = [
            a
            for a in all_assets
            if not machine
            or a.get("name") == machine
            or a.get("assetName") == machine
        ]
        if machine and not assets:
            return response(404, {"error": f"Asset '{machine}' not found"})
    except Exception as e:
        logger.exception("Failed to list assets")
        return response(500, {"error": str(e)})

    out: Dict[str, Any] = {}
    now_dt = datetime.now(tz=timezone.utc)

    business_date, today_start, today_end = business_day_bounds(now_dt)

    # ADD THESE TWO LINES
    # shadow_data = get_current_shadow()
    # zones_cfg = shadow_data.get("zones", {})
    
    today_date = business_date
    yesterday_date = business_date - timedelta(days=1)

    # ---------- fetch constants once from Dynamo under AssetId="All" ----------
    constants_asset_id = "All"
    # target_oa_const = get_constant_from_dynamo(constants_asset_id, "TargetOA")
    all_targets = get_constant_from_dynamo(
        constants_asset_id, "Targets"
    )
    # -----------------------------------------------------------------------

    # ---------- helper to normalize numeric-like raw values (live) ----------
    def _normalize_default_raw(raw):
        if raw is None:
            return "0.0"

        if isinstance(raw, str):
            s = raw.strip().lower()

            # handle junk values
            if s in ("none", "null", "none:null") or s.startswith("object"):
                return "0.0"

        return raw

    # helper: multiply a live value by 100 (preserving ":shift" if present)
    def _scale_percent_like_live(raw):
        if raw is None:
            return None

        # numeric -> simple multiply
        if isinstance(raw, (int, float, Decimal)):
            return float(raw) * 100.0

        if isinstance(raw, str):
            s = raw.strip()
            if ":" in s:
                left, right = s.split(":", 1)
                left = left.strip()
                try:
                    v = float(left)
                    return f"{round(v * 100.0, 2)}:{right.strip()}"
                except Exception:
                    return raw
            # plain numeric string
            try:
                v = float(s)
                return round(v * 100.0, 2)
            except Exception:
                return raw

        return raw

    # helper: multiply numeric-summary dict (today.metrics[...]) by 100 for sum & average
    def _scale_numeric_summary(summary: Any):
        if not isinstance(summary, dict):
            return summary
        for k in ("sum", "average"):
            if k in summary and summary[k] is not None:
                try:
                    v = float(summary[k])
                    summary[k] = str(v * 100.0)
                except Exception:
                    pass
        return summary


    shadow_data = get_current_shadow()
    zones_cfg = shadow_data.get("zones", {})


    # -------------------------------------------------
    # FAN-OUT PARALLEL PROCESSING
    # -------------------------------------------------
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                process_single_live_asset, 
                asset, shadow_data, today_flag, hist_days, 
                today_start, today_end, now_dt, parts_daywise, states_daywise
            ): asset 
            for asset in assets
        }
        
        for future in concurrent.futures.as_completed(futures):
            try:
                asset_name, asset_obj, zone_key = future.result()
                out[asset_name] = asset_obj
            except Exception as e:
                logger.error(f"Failed to process live asset: {e}")

    # group by zone
    zones: Dict[str, Any] = {}
    for asset_name, asset_data in out.items():
        try:
            m = re.search(r"SP[-_]?(\d)", asset_name)
            zone = f"Zone_{m.group(1)}" if m else "Zone_Unknown"
        except Exception:
            zone = "Zone_Unknown"
        zones.setdefault(zone, {})[asset_name] = asset_data

    for zone, machines in zones.items():

        wd = get_zone_working_days(zone.lower())
        zones[zone]["working_days"] = wd if wd is not None else []
    result = {
        "model_name": ASSET_MODEL_NAME,
        "asset_count": len(out),
        "zone_count": len(zones),
        "zones": zones,
        # "TargetOA": target_oa_const if target_oa_const else {},        
        "All_Targets": all_targets if all_targets else {},
    }
    return response(200, {"message": "Data", "data": result})
