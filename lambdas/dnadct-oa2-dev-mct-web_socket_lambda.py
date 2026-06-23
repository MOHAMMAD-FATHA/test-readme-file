"""
Unified Lambda: IoT Rule -> normalize -> SiteWise name lookup -> broadcast via WebSocket

Environment variables required:
- CONNECTIONS_TABLE
- WS_API_ID
- WS_STAGE
Optional:
- AWS_REGION

Permissions required (in addition to Dynamo and APIGW perms):
- iotsitewise:ListAssetProperties
- iotsitewise:DescribeAsset
"""
import os
import json
import ast
import logging
import re
import concurrent.futures
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

# ---------- logging ----------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------- env ----------
AWS_REGION = os.getenv("AWS_REGION")
CONNECTIONS_TABLE = os.getenv("CONNECTIONS_TABLE", "ActiveConnections")
WS_API_ID = os.getenv("WS_API_ID", "k6k1wff816")
WS_STAGE = os.getenv("WS_STAGE", "dev")

# ---------- boto3 session & clients ----------
if AWS_REGION:
    SESSION = boto3.session.Session(region_name=AWS_REGION)
else:
    SESSION = boto3.session.Session()

DDB = SESSION.resource("dynamodb")
SITEWISE = SESSION.client("iotsitewise")
# apigw client created on demand with endpoint_url built from WS_API_ID + region + WS_STAGE

# ---------- in-memory caches ----------
_asset_prop_name_cache: Dict[str, Dict[str, str]] = {}  # assetId -> { propId/short: propName }
_asset_name_cache: Dict[str, str] = {}                  # assetId -> assetName

# ---------- FE mapping ----------
# PROPERTY_NAME_MAP: Dict[str, str] = {
#     "PlanQTY" : "Plan QTY",
#     "CurrentQTY" : "Current QTY",
#     "Util" : "Today util",
#     "MachineStateAndShift": "Machine States",
# }

# ---------- PERCENTAGE properties we must *x100* for FE ----------
# These are the underlying SiteWise property *names*
PERCENTAGE_PROPERTY_NAMES = {
    "OEEPerDay",        # OEE
    "QualityPerJob",    # quality
    "PerformancePerJob"  # performance
}

def _maybe_scale_percentage_value(prop_name: Optional[str], numeric_val: Any) -> Any:
    """
    For specific percentage-like properties, multiply the numeric value by 100
    before sending to FE. If value is not numeric or prop doesn't match, return as-is.
    """
    if numeric_val is None:
        return numeric_val

    if prop_name:
        key = re.sub(r"\s+", "", str(prop_name))
        if key in PERCENTAGE_PROPERTY_NAMES:
            try:
                # only scale if it's numeric
                if isinstance(numeric_val, (int, float)):
                    return numeric_val * 100
            except Exception:
                pass
    return numeric_val

# ---------- helpers ----------
def json_default(o: Any):
    if isinstance(o, (datetime, )):
        return o.isoformat()
    return str(o)

def safe_json_loads(s: Any) -> Any:
    if isinstance(s, (dict, list)):
        return s
    if isinstance(s, (bytes, bytearray)):
        try:
            s = s.decode("utf-8")
        except Exception:
            return {"raw": str(s)}
    if isinstance(s, str):
        s = s.strip()
        try:
            return json.loads(s)
        except Exception:
            return {"raw": s}
    return s

def safe_parse_str_dict(s: Any) -> Any:
    if not isinstance(s, str):
        return s
    s = s.strip()
    if not (s.startswith("{") and s.endswith("}")):
        return s
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    try:
        parsed = json.loads(s.replace("'", '"'))
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return {"raw": s}

def _get_connections_table():
    return DDB.Table(CONNECTIONS_TABLE)

def store_connection(connection_id: str, metadata: Dict[str, Any] = None):
    table = _get_connections_table()
    item = {"connectionId": connection_id, "connectedAt": datetime.utcnow().isoformat()}
    if metadata:
        item.update(metadata)
    try:
        table.put_item(Item=item)
        logger.info(f"[WS] Stored connection {connection_id}")
    except Exception:
        logger.exception(f"[WS] Failed to store connection {connection_id}")

def remove_connection(connection_id: str):
    table = _get_connections_table()
    try:
        table.delete_item(Key={"connectionId": connection_id})
        logger.info(f"[WS] Removed connection {connection_id}")
    except Exception:
        logger.exception(f"[WS] Failed to remove connection {connection_id}")

def list_connections() -> List[str]:
    table = _get_connections_table()
    try:
        resp = table.scan(ProjectionExpression="connectionId")
        items = resp.get("Items", [])
        ids = [i["connectionId"] for i in items if "connectionId" in i]
        while "LastEvaluatedKey" in resp:
            resp = table.scan(ProjectionExpression="connectionId", ExclusiveStartKey=resp["LastEvaluatedKey"])
            items = resp.get("Items", [])
            ids.extend([i["connectionId"] for i in items if "connectionId" in i])
        return ids
    except Exception:
        logger.exception("[WS] Failed to list connections")
        return []

def _get_apigw_client():
    if not WS_API_ID:
        raise Exception("WS_API_ID env var not set")
    region = AWS_REGION or SESSION.region_name
    endpoint_url = f"https://{WS_API_ID}.execute-api.{region}.amazonaws.com/{WS_STAGE}"
    return boto3.client("apigatewaymanagementapi", endpoint_url=endpoint_url)

def send_to_connection(connection_id: str, payload: Dict[str, Any]) -> bool:
    try:
        apigw = _get_apigw_client()
        apigw.post_to_connection(Data=json.dumps(payload, default=json_default).encode("utf-8"), ConnectionId=connection_id)
        return True
    except ClientError as e:
        err_code = e.response.get("Error", {}).get("Code")
        logger.warning(f"[WS] post_to_connection error for {connection_id}: {err_code}")
        if err_code in ("GoneException", "410"):
            try:
                remove_connection(connection_id)
            except Exception:
                logger.exception("[WS] remove_connection failed")
        return False
    except Exception:
        logger.exception(f"[WS] Unexpected error posting to {connection_id}")
        return False

def broadcast(payload: Dict[str, Any]) -> int:
    conns = list_connections()
    sent = 0
    for cid in conns:
        if send_to_connection(cid, payload):
            sent += 1
    logger.info(f"[WS] Broadcasted to {sent}/{len(conns)} connections")
    return sent

# ---------- SiteWise helpers ----------
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

def compute_live_state_timestamp(
    asset_id: str,
    state_pid: str,
    current_state_raw: str,
) -> Optional[str]:

    if not current_state_raw or ":" not in current_state_raw:
        return None

    current_state = current_state_raw.rsplit(":", 1)[0].strip()

    # # If current is PD
    # if current_state.replace(" ", "").lower() == "planneddowntime":
    #     return None

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

        events.sort(key=lambda x: x[0])
        # events = events[-3:]
        # events.sort(key=lambda x: x[0])

        # collapse consecutive same states
        collapsed = []
        for e in events:
            if not collapsed or collapsed[-1][1] != e[1]:
                collapsed.append(e)

        events = collapsed[-3:]

        # Find latest occurrence of current state
        # last_index = None
        # for i in reversed(range(len(events))):
        #     if events[i][1] == current_state:
        #         last_index = i
        #         break
        # Find latest transition INTO current state
        last_index = None
        for i in reversed(range(len(events))):

            if events[i][1] != current_state:
                continue

            # Check if this is a transition into the state
            if i == 0 or events[i-1][1] != current_state:
                last_index = i
                break

        if last_index is None:
            return None

        current_start_time = events[last_index][0]
        # DEBUG LOG
        prev_state_dbg = events[last_index-1][1] if last_index-1 >= 0 else None
        before_pd_dbg = events[last_index-2][1] if last_index-2 >= 0 else None

        logger.info(
            f"[STATE CHECK] asset={asset_id} "
            f"current={current_state} "
            f"prev={prev_state_dbg} "
            f"before_pd={before_pd_dbg} "
            f"current_start={current_start_time}"
        )
        # ---------------------------------------------
        # Check if previous state was PD
        # ---------------------------------------------
        if last_index - 1 >= 0:
            prev_state = events[last_index - 1][1]

            if prev_state.replace(" ", "").lower() == "planneddowntime":

                if last_index - 2 >= 0:
                    state_before_pd = events[last_index - 2][1]

                    #  Subtract ONLY if same state
                    # if state_before_pd == current_state:
                    if state_before_pd.strip().lower() == current_state.strip().lower():
                        prev_start_time = events[last_index - 2][0]
                        pd_start_time = events[last_index - 1][0]

                        # prev_duration_minutes = int(
                        #     (pd_start_time - prev_start_time).total_seconds() / 60
                        # )

                        # adjusted_start = current_start_time - timedelta(
                        #     minutes=prev_duration_minutes
                        # )

                        # return adjusted_start.isoformat().replace("+00:00", "Z")
                        prev_duration_seconds = (
                            pd_start_time - prev_start_time
                        ).total_seconds()

                        adjusted_start = current_start_time - timedelta(
                            seconds=prev_duration_seconds
                        )

                        return adjusted_start.isoformat().replace("+00:00", "Z")
        # ---------------------------------------------
        # Default → return actual start time
        # ---------------------------------------------
        return current_start_time.isoformat().replace("+00:00", "Z")

    except Exception:
        logger.exception("Failed computing live state timestamp")
        return None

def _normalize_property_id(prop_id: Optional[str]) -> Optional[str]:
    if not prop_id or not isinstance(prop_id, str):
        return prop_id
    return prop_id.split("_", 1)[0]

def get_property_name_for_asset(asset_id: str, raw_property_id: str) -> Optional[str]:
    if not asset_id or not raw_property_id:
        return None
    base_pid = _normalize_property_id(raw_property_id)
    asset_cache = _asset_prop_name_cache.get(asset_id)
    if asset_cache:
        if base_pid in asset_cache:
            return asset_cache[base_pid]
        if raw_property_id in asset_cache:
            return asset_cache[raw_property_id]
    try:
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
                if pid:
                    prop_map[pid] = pname
                    short = pid.split("_", 1)[0]
                    prop_map[short] = pname
        _asset_prop_name_cache[asset_id] = prop_map
        return prop_map.get(base_pid) or prop_map.get(raw_property_id)
    except Exception:
        logger.exception("[SITEWISE] list_asset_properties failed")
        return None

def get_asset_name_for_id(asset_id: str) -> Optional[str]:
    if not asset_id:
        return None
    if asset_id in _asset_name_cache:
        return _asset_name_cache[asset_id]
    try:
        resp = SITEWISE.describe_asset(assetId=asset_id)
        asset_name = resp.get("assetName") or resp.get("asset", {}).get("name") or resp.get("name")
        if asset_name:
            _asset_name_cache[asset_id] = asset_name
            return asset_name
    except Exception:
        logger.exception(f"[SITEWISE] describe_asset failed for asset {asset_id}")
    return None

# ---------- timestamp normalization ----------
def normalize_sitewise_ts(ts) -> Optional[str]:
    """
    Normalize SiteWise timestamp shapes to ISO8601 UTC string (Z) or None.
    Accepts dicts like {'timeInMillis':...} / {'timeInSeconds':...}, numeric epoch, or ISO string.
    """
    try:
        if not ts:
            return None
        if isinstance(ts, dict):
            if "timeInMillis" in ts:
                try:
                    millis = int(ts["timeInMillis"])
                    return datetime.fromtimestamp(millis/1000.0, tz=timezone.utc).isoformat().replace("+00:00","Z")
                except Exception:
                    pass
            if "timeInSeconds" in ts:
                try:
                    secs = int(ts["timeInSeconds"])
                    return datetime.fromtimestamp(secs, tz=timezone.utc).isoformat().replace("+00:00","Z")
                except Exception:
                    pass
            # nested shapes
            for v in ts.values():
                res = normalize_sitewise_ts(v)
                if res:
                    return res
            return None
        if isinstance(ts, (int, float)):
            if ts > 1e12:
                return datetime.fromtimestamp(float(ts)/1000.0, tz=timezone.utc).isoformat().replace("+00:00","Z")
            if ts > 1e9:
                return datetime.fromtimestamp(float(ts), tz=timezone.utc).isoformat().replace("+00:00","Z")
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
                return dt.astimezone(timezone.utc).isoformat().replace("+00:00","Z")
            except Exception:
                return None
    except Exception:
        logger.exception("normalize_sitewise_ts failed on %s", ts)
    return None

def _normalize_segment_times(seg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize common segment time keys in-place and return segment dict.
    Looks for keys: start, start_utc, startTime, startTimeUtc, startTimestamp and end variants.
    Converts numeric/string epoch to ISO Z.
    """
    if not isinstance(seg, dict):
        return seg
    # candidate key pairs
    start_keys = ("start", "start_utc", "startTime", "startTimeUtc", "startTimestamp", "startUtc")
    end_keys = ("end", "end_utc", "endTime", "endTimeUtc", "endTimestamp", "endUtc")
    for k in start_keys:
        if k in seg:
            normalized = normalize_sitewise_ts(seg.get(k))
            seg[k] = normalized or seg.get(k)
            break
    for k in end_keys:
        if k in seg:
            normalized = normalize_sitewise_ts(seg.get(k))
            seg[k] = normalized or seg.get(k)
            break
    # also normalize any nested timestamp-like keys
    for key, val in list(seg.items()):
        if isinstance(val, (dict, list)):
            # recurse for nested dicts or lists of dicts
            if isinstance(val, dict):
                seg[key] = _normalize_segment_times(val)
            else:
                # list
                new_list = []
                for item in val:
                    if isinstance(item, dict):
                        new_list.append(_normalize_segment_times(item))
                    else:
                        new_list.append(item)
                seg[key] = new_list
    return seg

# ---------- parsing ----------
def parse_value_and_shift(val: Any) -> Tuple[Any, Optional[str]]:
    if isinstance(val, dict):
        shift = val.get("Shift") or val.get("shift")
        number = None
        for k, v in val.items():
            if k in ("Shift", "shift"):
                continue
            if isinstance(v, (int, float)):
                number = v
                break
        return number, shift
    if isinstance(val, str) and ":" in val:
        try:
            num_s, shift = val.split(":", 1)
            return float(num_s), shift
        except Exception:
            return val, None
    try:
        if isinstance(val, (int, float)):
            return val, None
        if isinstance(val, str):
            if val.strip() == "":
                return None, None
            return float(val), None
    except Exception:
        pass
    return val, None

# ---------- NEW helper: robust timestamp candidate extractor ----------
def _extract_candidate_timestamp(parsed_obj: Any, entry_obj: Any = None) -> Optional[Any]:
    """
    Try many common keys and nested locations for a timestamp.
    Returns the raw candidate (could be dict, int, or string) — caller will normalize.
    """
    if not parsed_obj:
        parsed_obj = {}

    # top-level candidates (try many common variants)
    candidates = [
        "timestamp", "timestampUtc", "timestamp_utc", "time", "timeUtc", "time_utc",
        "timeInMillis", "timeInSeconds", "timestampMs", "ts", "timestampMillis"
    ]
    if isinstance(parsed_obj, dict):
        for k in candidates:
            if k in parsed_obj and parsed_obj.get(k) is not None:
                return parsed_obj.get(k)

        # also check nested shapes (common in IoT rule message wrappers)
        nested_paths = [
            ("value", "timestamp"),
            ("value", "time"),
            ("message", "timestamp"),
            ("message", "time"),
            ("payload", "timestamp"),
            ("payload", "time"),
        ]
        for p in nested_paths:
            cur = parsed_obj
            ok = True
            for key in p:
                if isinstance(cur, dict) and key in cur:
                    cur = cur.get(key)
                else:
                    ok = False
                    break
            if ok and cur is not None:
                return cur

    # if the current entry has its own timestamp (useful when values_list elements carry timestamps)
    if entry_obj and isinstance(entry_obj, dict):
        for k in ("timestamp", "timestampUtc", "time", "timeInMillis", "timeInSeconds", "ts"):
            if k in entry_obj and entry_obj.get(k) is not None:
                return entry_obj.get(k)
        # also common nested timestamp in entry's value
        if "value" in entry_obj and isinstance(entry_obj["value"], dict):
            for k in ("timestamp", "time", "timeInMillis", "timeInSeconds"):
                if k in entry_obj["value"] and entry_obj["value"].get(k) is not None:
                    return entry_obj["value"].get(k)

    return None

# ---------- NEW helper: extract ts from propertyId suffix like "..._1764679904968" ----------
def _extract_ts_from_property_id(prop_id: Optional[str]) -> Optional[str]:
    """
    If propertyId has a trailing numeric suffix (e.g. pid_1764679904968) use it as epoch seconds/ms.
    Returns normalized ISO string or None.
    """
    if not prop_id or not isinstance(prop_id, str):
        return None
    m = re.search(r"_(\d{10,13})$", prop_id)
    if not m:
        return None
    s = m.group(1)
    try:
        ts_int = int(s)
        if len(s) >= 13:
            return normalize_sitewise_ts({"timeInMillis": ts_int})
        else:
            return normalize_sitewise_ts({"timeInSeconds": ts_int})
    except Exception:
        return None

# ---------- core single-record processor ----------
def process_single_record(parsed: Dict[str, Any]):
    """
    Process a single IoT record dict (parsed).
    For each entry in string_value/double_value/value list, broadcast one message.
    Output payload DOES NOT include 'raw' and INCLUDES 'assetName' and normalized timestamp if present.
    """
    if not isinstance(parsed, dict):
        broadcast({"source": "iot_rule", "original": parsed})
        return

    asset_id = parsed.get("AssetId") or parsed.get("assetId") or parsed.get("asset_id")
    property_id = parsed.get("PropertyId") or parsed.get("propertyId") or parsed.get("property_id")

    # extract candidate values:
    string_vals = parsed.get("string_value") or parsed.get("stringValue")
    double_vals = parsed.get("double_value") or parsed.get("doubleValue")
    values_list = None

    if string_vals:
        values_list = string_vals if isinstance(string_vals, list) else [string_vals]
    elif double_vals:
        values_list = double_vals if isinstance(double_vals, list) else [double_vals]
    elif "value" in parsed:
        v = parsed.get("value")
        values_list = v if isinstance(v, list) else [v]
    else:
        values_list = [parsed]

    # pre-fetch assetName (cached)
    asset_name = None
    if asset_id:
        try:
            asset_name = get_asset_name_for_id(asset_id)
        except Exception:
            logger.exception("[SITEWISE] get_asset_name_for_id failed")

    # NOTE: robustly extract a top-level candidate timestamp (may be None)
    top_level_candidate = _extract_candidate_timestamp(parsed, None)
    top_level_ts = normalize_sitewise_ts(top_level_candidate)

    # also prepare fallback from propertyId suffix (if present)
    propid_ts_fallback = _extract_ts_from_property_id(property_id)

    for entry in values_list:
        entry_parsed = safe_parse_str_dict(entry) if isinstance(entry, str) and entry.strip().startswith("{") else entry
        normalized_pid = _normalize_property_id(property_id) if property_id else None

        prop_name = None
        if asset_id and property_id:
            try:
                prop_name = get_property_name_for_asset(asset_id, property_id)
            except Exception:
                logger.exception("[IOT_RULE] get_property_name_for_asset failed")

        # property_alias = PROPERTY_NAME_MAP.get(prop_name, prop_name)

        numeric_val, shift = parse_value_and_shift(entry_parsed)
        # ---- SCALE % metrics for FE ----
        numeric_val = _maybe_scale_percentage_value(prop_name, numeric_val)

        # prefer timestamp found on the entry itself; else fall back to top-level; else propertyId suffix
        entry_candidate = _extract_candidate_timestamp(parsed, entry_parsed)
        entry_ts = normalize_sitewise_ts(entry_candidate) if entry_candidate is not None else None

        # top_ts = entry_ts or top_level_ts or propid_ts_fallback
        top_ts = entry_ts or top_level_ts or propid_ts_fallback

        # -------------------------------
        # APPLY STATE START LOGIC HERE
        # -------------------------------
        if prop_name == "MachineStateAndShift":

            current_state_raw = entry_parsed

            if current_state_raw:

                state_name = str(current_state_raw).split(":")[0].strip()

                if state_name.replace(" ", "").lower() == "planneddowntime":

                    top_ts = None

                else:

                    computed_ts = compute_live_state_timestamp(
                        asset_id=asset_id,
                        state_pid=_normalize_property_id(property_id),
                        current_state_raw=current_state_raw,
                    )

                    if computed_ts:
                        top_ts = computed_ts
        # build final payload
        payload = {
            "source": "iot_rule",
            "assetId": asset_id,
            "assetName": asset_name,
            "propertyId": property_id,
            "normalizedPropertyId": normalized_pid,
            "propertyName": prop_name,
            # "propertyAlias": property_alias,
            "value": numeric_val,
            "shift": shift,
            "timestamp": top_ts,  # include normalized top-level/entry/propertyId timestamp if present
        }

        # If this looks like a MachineStates structure (segments), try to preserve & normalize segments
        if isinstance(entry_parsed, dict) and (prop_name and ("Machine" in prop_name or "State" in prop_name)):
            # attempt to normalize segments if present
            segs = None
            # common keys: segments, value->segments, or entry_parsed itself may be the whole structure
            if "segments" in entry_parsed and isinstance(entry_parsed["segments"], list):
                segs = entry_parsed["segments"]
            elif "value" in entry_parsed and isinstance(entry_parsed["value"], dict) and isinstance(entry_parsed["value"].get("segments"), list):
                segs = entry_parsed["value"]["segments"]
            elif isinstance(entry_parsed.get("value"), list):
                segs = entry_parsed.get("value")
            if segs:
                normalized_segments = []
                for s in segs:
                    if isinstance(s, dict):
                        normalized_segments.append(_normalize_segment_times(s))
                    else:
                        normalized_segments.append(s)
                payload["segments"] = normalized_segments
                # compute latest_transition if any end timestamps present in last segment
                last = normalized_segments[-1] if normalized_segments else None
                latest_end = None
                if isinstance(last, dict):
                    # try to find end key we normalized
                    for key in ("end", "end_utc", "endTime", "endTimeUtc", "endTimestamp", "endUtc"):
                        if key in last and isinstance(last[key], str):
                            latest_end = last[key]
                            break
                payload["latest_transition"] = latest_end

        logger.info(f"[IOT_RULE] Broadcasting: asset={asset_name or asset_id} prop={normalized_pid} name={prop_name} value={numeric_val} shift={shift} ts={top_ts}")
        broadcast(payload)

# ---------- IoT handlers ----------
def handle_iot_rule_records(records: List[Dict[str, Any]]):
    logger.info(f"[IOT_RULE] Received {len(records)} records")
    for rec in records:
        raw_payload = rec.get("message") or rec.get("payload") or rec.get("body") or rec
        parsed = safe_json_loads(raw_payload)
        if isinstance(parsed, dict) and ("AssetId" not in parsed and "assetId" not in parsed) and ("message" in parsed):
            parsed = safe_json_loads(parsed.get("message"))
        process_single_record(parsed)
    return {"statusCode": 200}

# ---------- SiteWise Property Update handler (includes normalized timestamps & state segments) ----------
def handle_sitewise_property_update(event_payload: Dict[str, Any]):
    payload = event_payload.get("payload", {}) or {}
    asset_id = payload.get("assetId")
    property_id = payload.get("propertyId")
    values = payload.get("values", []) or []

    # pre-fetch assetName and prop_name
    asset_name = None
    prop_name = None
    if asset_id:
        try:
            asset_name = get_asset_name_for_id(asset_id)
        except Exception:
            logger.exception("[SITEWISE] get_asset_name_for_id failed")
    if asset_id and property_id:
        try:
            prop_name = get_property_name_for_asset(asset_id, property_id)
        except Exception:
            logger.exception("[SITEWISE] get_property_name_for_asset failed")

    # property_alias = PROPERTY_NAME_MAP.get(prop_name, prop_name)

    # for v in values:
    #     # value_data may be nested
    #     value_data = v.get("value", v)
    #     primitive = None
    #     # If value_data is dict, pick primitive typed field if present
    #     if isinstance(value_data, dict):
    #         for k in ("doubleValue", "integerValue", "stringValue", "booleanValue"):
    #             if k in value_data:
    #                 primitive = value_data[k]
    #                 break
    #         # sometimes 'value' holds a nested dict with segments; keep the dict in that case
    #         if primitive is None and "value" in value_data and isinstance(value_data["value"], dict):
    #             # the nested dict may contain segments or structured state - keep it
    #             primitive = value_data["value"]
    #     else:
    #         primitive = value_data

    #     # parse numeric and shift where applicable
    #     numeric_val, shift = parse_value_and_shift(primitive)
    #     # ---- SCALE % metrics for FE ----
    #     numeric_val = _maybe_scale_percentage_value(prop_name, numeric_val)

    #     # normalize SiteWise-provided timestamp (v may contain nested timestamp shape)
    #     ts_norm = normalize_sitewise_ts(v.get("timestamp") or v.get("timestampUtc") or v.get("time") or v.get("timeInMillis"))

    #     out = {
    #         "source": "sitewise_property_update",
    #         "assetId": asset_id,
    #         "assetName": asset_name,
    #         "propertyId": property_id,
    #         "normalizedPropertyId": _normalize_property_id(property_id),
    #         "propertyName": prop_name,
    #         # "propertyAlias": property_alias,
    #         "value": numeric_val,
    #         "shift": shift,
    #         "timestamp": ts_norm,
    #         "quality": v.get("quality", "UNKNOWN"),
    #     }

    #     # If this is a MachineStates-like property, try to include segments and normalize their times
    #     # is_states = False
    #     # if prop_name and (prop_name == "MachineStateAndShift"):
    #     #     is_states = True
    #     # # if property_alias and property_alias.lower() in ("state", "machine states", "machine state"):
    #     # #     is_states = True

    # # 🚨 ONLY handle MachineStateAndShift here
    #     if prop_name != "MachineStateAndShift":
    #         broadcast(out)
    #         continue

    #         current_state_raw = primitive

    #         # Default safety
    #         out["timestamp"] = None
    #         # out["duration"] = 0

    #         if current_state_raw:

    #             state_name = str(current_state_raw).split(":")[0].strip()

    #             # Planned Downtime → timestamp null, duration 0
    #             if state_name.replace(" ", "").lower() == "planneddowntime":
    #                 out["timestamp"] = None
    #                 # out["duration"] = 0

    #             else:
    #                 state_start_ts = compute_live_state_timestamp(
    #                     asset_id=asset_id,
    #                     state_pid=property_id,
    #                     current_state_raw=current_state_raw,
    #                 )

    #                 # Timestamp now represents state start
    #                 out["timestamp"] = state_start_ts

    #         if isinstance(current_state_raw, dict):
    #             # common shapes: {'segments': [...], ...} or {'shiftA': {...}, 'shiftB': {...}} etc.
    #             # Normalize any segments arrays found at top-level or under shiftA/shiftB
    #             def _norm_container(container):
    #                 if not isinstance(container, dict):
    #                     return container
    #                 # if container has 'segments' list normalize each
    #                 if "segments" in container and isinstance(container["segments"], list):
    #                     new_segs = []
    #                     for s in container["segments"]:
    #                         if isinstance(s, dict):
    #                             new_segs.append(_normalize_segment_times(s))
    #                         else:
    #                             new_segs.append(s)
    #                     container["segments"] = new_segs
    #                 # also attempt to normalize nested shift objects
    #                 for k2, v2 in list(container.items()):
    #                     if isinstance(v2, dict) and ("segments" in v2 or "daily_metrics" in v2):
    #                         container[k2] = _norm_container(v2)
    #                 return container

    #             state_obj = _norm_container(current_state_raw)

    #             # attach normalized state object to out
    #             out["state_object"] = state_obj

    #             # attempt to compute a latest_transition timestamp: look for last segment end in common places
    #             latest = None
    #             # check top-level segments
    #             if isinstance(state_obj.get("segments"), list) and state_obj["segments"]:
    #                 last_seg = state_obj["segments"][-1]
    #                 # check end / end_utc etc.
    #                 for k in ("end", "end_utc", "endTime", "endTimeUtc", "endTimestamp", "endUtc"):
    #                     if last_seg.get(k):
    #                         latest = last_seg.get(k)
    #                         break
    #             # check shiftA/shiftB daily segments
    #             if not latest:
    #                 for shift_key in ("shiftA", "shiftB","shiftC", "ShiftA", "ShiftB","ShiftC"):
    #                     shift_obj = state_obj.get(shift_key)
    #                     if isinstance(shift_obj, dict) and isinstance(shift_obj.get("segments"), list) and shift_obj["segments"]:
    #                         last_seg = shift_obj["segments"][-1]
    #                         for k in ("end", "end_utc", "endTime", "endTimeUtc", "endTimestamp", "endUtc"):
    #                             if last_seg.get(k):
    #                                 latest = last_seg.get(k)
    #                                 break
    #                         if latest:
    #                             break
    #             # fallback: use v.get('timestamp')
    #             if not latest:
    #                 latest = ts_norm

    #             out["latest_transition"] = latest

    #         else:
    #             # primitive not dict; still attach raw primitive as state_value
    #             out["state_value"] = primitive
    #             out["latest_transition"] = ts_norm

    #     # broadcast
    #     logger.info(f"[SITEWISE] Broadcasting: asset={asset_name or asset_id} prop={property_id} name={prop_name} ts={ts_norm}")
    #     broadcast(out)
    for v in values:

        value_data = v.get("value", v)
        primitive = None

        if isinstance(value_data, dict):
            for k in ("doubleValue", "integerValue", "stringValue", "booleanValue"):
                if k in value_data:
                    primitive = value_data[k]
                    break
            if primitive is None and "value" in value_data and isinstance(value_data["value"], dict):
                primitive = value_data["value"]
        else:
            primitive = value_data

        numeric_val, shift = parse_value_and_shift(primitive)
        numeric_val = _maybe_scale_percentage_value(prop_name, numeric_val)

        ts_norm = normalize_sitewise_ts(
            v.get("timestamp") or
            v.get("timestampUtc") or
            v.get("time") or
            v.get("timeInMillis")
        )

        out = {
            "source": "sitewise_property_update",
            "assetId": asset_id,
            "assetName": asset_name,
            "propertyId": property_id,
            "normalizedPropertyId": _normalize_property_id(property_id),
            "propertyName": prop_name,
            "value": numeric_val,
            "shift": shift,
            "timestamp": ts_norm,
            "quality": v.get("quality", "UNKNOWN"),
        }
        # out = {
        #     "source": "sitewise_property_update",
        #     "assetId": asset_id,
        #     "assetName": asset_name,
        #     "propertyId": property_id,
        #     "normalizedPropertyId": _normalize_property_id(property_id),
        #     "propertyName": prop_name,
        #     "value": numeric_val,
        #     "shift": shift,
        #     "timestamp": None,
        #     "quality": v.get("quality", "UNKNOWN"),
        # }
        # ONLY apply custom logic for MachineStateAndShift
        if prop_name == "MachineStateAndShift":

            current_state_raw = primitive
            out["timestamp"] = None

            if current_state_raw:

                state_name = str(current_state_raw).split(":")[0].strip()

                if state_name.replace(" ", "").lower() == "planneddowntime":
                    out["timestamp"] = None

                else:
                    state_start_ts = compute_live_state_timestamp(
                        asset_id=asset_id,
                        state_pid=property_id,
                        current_state_raw=current_state_raw,
                    )

                    out["timestamp"] = state_start_ts

        logger.info(
            f"[SITEWISE] Broadcasting: asset={asset_name or asset_id} "
            f"prop={property_id} name={prop_name} ts={out['timestamp']}"
        )

        broadcast(out)
    return {"statusCode": 200}

def handle_generic_records(records: List[Dict[str, Any]]):
    for rec in records:
        body = rec.get("body") or rec.get("Sns", {}).get("Message") or rec
        parsed = safe_json_loads(body)
        process_single_record(parsed)
    return {"statusCode": 200}

# ---------- WebSocket handlers ----------
def handle_ws_connect(event: Dict[str, Any]):
    conn_id = event["requestContext"].get("connectionId")
    logger.info(f"[WS] $connect {conn_id}")
    try:
        qs = event.get("queryStringParameters") or {}
        metadata = {}
        if isinstance(qs, dict):
            metadata["query"] = qs
        store_connection(conn_id, metadata)
    except Exception:
        logger.exception("[WS] store_connection failed")
    return {"statusCode": 200, "body": "Connected"}

def handle_ws_disconnect(event: Dict[str, Any]):
    conn_id = event["requestContext"].get("connectionId")
    logger.info(f"[WS] $disconnect {conn_id}")
    try:
        remove_connection(conn_id)
    except Exception:
        logger.exception("[WS] remove_connection failed")
    return {"statusCode": 200, "body": "Disconnected"}

def handle_ws_default(event: Dict[str, Any]):
    conn_id = event["requestContext"].get("connectionId")
    body = safe_json_loads(event.get("body"))
    send_to_connection(conn_id, {"echo": body})
    return {"statusCode": 200, "body": "OK"}

# ---------- main handler ----------
def lambda_handler(event: Dict[str, Any], context: Any):
    logger.info(f"[LAMBDA] Incoming event keys: {list(event.keys())}")
    try:
        # WebSocket lifecycle
        if "requestContext" in event and isinstance(event.get("requestContext"), dict) and "routeKey" in event["requestContext"]:
            route = event["requestContext"].get("routeKey")
            if route == "$connect":
                return handle_ws_connect(event)
            if route == "$disconnect":
                return handle_ws_disconnect(event)
            return handle_ws_default(event)

        # IoT Rule 'records'
        if "records" in event and isinstance(event["records"], list):
            return handle_iot_rule_records(event["records"])

        # SiteWise PropertyValueUpdate
        if event.get("type") in ("PropertyValueUpdate", "propertyValueUpdate"):
            return handle_sitewise_property_update(event)

        # SNS / SQS style 'Records'
        if "Records" in event:
            return handle_generic_records(event["Records"])

        # direct single-record detection
        if isinstance(event, dict) and ("AssetId" in event or "assetId" in event or "PropertyId" in event or "propertyId" in event):
            process_single_record(event)
            return {"statusCode": 200}

        # fallback: broadcast full event
        parsed = event
        if isinstance(event, dict) and "body" in event:
            parsed = safe_json_loads(event.get("body"))
        broadcast({"source": "unknown", "original": parsed})
        return {"statusCode": 200}
    except Exception:
        logger.exception("[LAMBDA] Unhandled exception")
        return {"statusCode": 500, "body": "Internal error"}
