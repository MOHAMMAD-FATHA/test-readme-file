# ----------------------------- UPDATED VERSION CODE BELOW ------------------------------------
import json
import boto3
import os
import re
import logging
import concurrent.futures
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import Optional
from boto3.dynamodb.conditions import Key
import csv
from io import StringIO
from botocore.config import Config

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ================= AWS CLIENTS =================
boto_config = Config(max_pool_connections=200, retries={'max_attempts': 5, 'mode': 'adaptive'})
sitewise = boto3.client("iotsitewise", config=boto_config)
ddb = boto3.resource("dynamodb", config=boto_config)
s3 = boto3.client("s3", config=boto_config)

# ================= ENV =================
MODEL_NAME = os.environ.get("ASSET_MODEL_NAME")
TABLE_NAME = os.environ.get("TABLE_NAME") 
PRESS_TABLE_NAME = os.environ["PRESS_TABLE_NAME"]
EXCEL_EXPORT_BUCKET = os.environ["EXCEL_EXPORT_BUCKET"]
PRESS_NUMBER_GSI_INDEX = os.environ["PRESS_NUMBER_GSI_INDEX"]

MACHINE_STATE_PROPERTY_NAME = "MachineStateAndShift"

press_table = ddb.Table(PRESS_TABLE_NAME)

DOWNTIME_COLUMNS = [
    "Machine - Encoder fault", "Machine - Light Curtain fault", "Machine - Lube fault",
    "Machine - Bolster", "Machine - Transfer", "Machine - Air pressure fault",
    "Machine - Conveyor belt", "Machine - De-stacker", "Scrap - Scrap removal",
    "Scrap - Scrap tray missing", "Scrap - Shaker bar", "Scrap - Scrap tray bad",
    "Scrap - Conveyor Jam", "Scrap - Scrap hopper", "Adjustment - Fingers",
    "Adjustment - Scrap tray", "Adjustment - Feeder", "Die Shop - Die protection",
    "Die Shop - Cam punches", "Die Shop - Bad details", "Die Shop - Lifters",
    "Die Shop - Bad Punches", "Quality - Waiting on Auditor", "Quality - Quality issues",
    "Quality - Frequently check", "Automation", "Coil Change", "Container Change", "Operator NTR"
]

CSV_COLUMNS = [
    "Zone", "date_cst", "PRESS_NO", "SHIFT_NO", "Order", "DIE_NO", "Part_No", "Plan_QTY",
    "Plan_REQ_Time", "SPM", "Total_QTY", "Good_QTY", "Scrap_QTY", "Actual_SPM",
    "Start_ChangeOver", "END_Changeover", "Actual_Changeover_min", "Die_Setter_ID",
    "START_Time", "END_Time", "Employee_ID", "PLAN_Time_with_new_QTY", "ACTProduction_Time",
    "DIFF_DT", "OA", "OR", "Quality", "OEE"
] + DOWNTIME_COLUMNS

# =====================================================
# ZONE DERIVATION
# =====================================================
def derive_zone_from_machine(machine_name: str) -> Optional[str]:
    if not machine_name: return None
    m = re.search(r"SP\s*?(\d)", machine_name)
    return f"Zone_{m.group(1)}" if m else None

# =====================================================
# MODEL HELPERS
# =====================================================
def get_model_id_from_name(model_name):
    paginator = sitewise.get_paginator("list_asset_models")
    for page in paginator.paginate():
        for model in page.get("assetModelSummaries", []):
            if model["name"] == model_name:
                return model["id"]
    raise Exception(f"Model {model_name} not found")

def calc_window_days(start_date, end_date):
    s = datetime.fromisoformat(start_date).date()
    e = datetime.fromisoformat(end_date).date()
    return (e - s).days + 1

# =====================================================
# CSV EXPORT FUNCTION
# =====================================================
def export_press_report_csv(assets, start_date, end_date):
    buffer = StringIO()
    writer = csv.writer(buffer)
    BASE_WIDTH, DT_WIDTH = 28, 55

    total_dt_cols = len(DOWNTIME_COLUMNS)
    left_pad = (total_dt_cols // 2)
    right_pad = total_dt_cols - left_pad - 1
    writer.writerow([""] * len(CSV_COLUMNS) + [""] * left_pad + ["==== Downtime Reasons ===="] + [""] * right_pad)

    headers = CSV_COLUMNS + DOWNTIME_COLUMNS
    padded_headers = [h.ljust(DT_WIDTH) if h in DOWNTIME_COLUMNS else h.ljust(BASE_WIDTH) for h in headers]
    writer.writerow(padded_headers)

    for asset in assets:
        press_rows = query_press_rows(asset["name"], start_date, end_date)
        for r in press_rows:
            rec_type = str(r.get("RecordType", "")).strip()
            press_shift = str(r.get("press_shift_time", "")).strip().upper()
            
            if rec_type == "DailyMachineStates" or "DAILY_STATES" in press_shift:
                continue

            row = []
            for col in CSV_COLUMNS:
                val = r.get(col)
                if isinstance(val, Decimal):
                    if col in ["OA", "OR", "Quality", "OEE"]: val = float(val)
                    else: val = int(val)
                if col == "date_cst" and val is not None: val = str(val)
                if col in ["OA", "OR", "Quality", "OEE"] and val is not None:
                    try: val = f"{round(float(val) * 100, 2)}%"
                    except: pass
                if val is not None: val = f"\t{val}"
                row.append(val)

            for col in DOWNTIME_COLUMNS:
                val = r.get(col, 0)
                if isinstance(val, Decimal): val = float(val)
                try: val = int(round(float(val or 0)))
                except: val = 0
                val = f"\t{val}"
                row.append(val)

            writer.writerow(row)

    csv_data = buffer.getvalue()
    key = f"exports/press_report_{start_date}_{end_date}.csv"

    s3.put_object(
        Bucket=EXCEL_EXPORT_BUCKET, Key=key, Body=csv_data,
        ContentType="text/csv", ContentDisposition=f'attachment; filename="press_report_{start_date}_to_{end_date}.csv"'
    )
    url = s3.generate_presigned_url("get_object", Params={"Bucket": EXCEL_EXPORT_BUCKET, "Key": key}, ExpiresIn=180)
    
    return url

# =====================================================
# PRESS TABLE QUERY 
# =====================================================
def query_press_rows(machine_name, start_date, end_date):
    items = []
    last_key = None
    s_date = start_date[:10]
    e_date = end_date[:10]
    
    while True:
        args = {
            "IndexName": PRESS_NUMBER_GSI_INDEX,
            "KeyConditionExpression": (
                Key("PRESS_NO").eq(machine_name) &
                Key("date_cst").between(s_date, e_date)
            )
        }
        if last_key: args["ExclusiveStartKey"] = last_key
        resp = press_table.query(**args)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key: break
    return items

# =====================================================
# CORE METRICS CALCULATIONS (CUSTOM MATH)
# =====================================================
def calculate_core_metrics(job_rows, window_days, asset_name):
    sum_plan_time = 0.0
    sum_act_prod_time = 0.0
    sum_act_changeover = 0.0
    sum_total_qty = 0.0
    sum_scrap_qty = 0.0
    sum_diff_dt = 0.0
    sum_good_qty = 0.0
    sum_plan_qty = 0.0
    sum_plan_req_time = 0.0

    for r in job_rows:
        def safe_float(val):
            try: return float(val) if val is not None else 0.0
            except: return 0.0
            
        sum_plan_time += safe_float(r.get("PLAN_Time_with_new_QTY"))
        sum_act_prod_time += safe_float(r.get("ACTProduction_Time"))
        sum_act_changeover += safe_float(r.get("Actual_Changeover_min"))
        sum_total_qty += safe_float(r.get("Total_QTY"))
        sum_scrap_qty += safe_float(r.get("Scrap_QTY"))
        sum_diff_dt += safe_float(r.get("DIFF_DT"))
        sum_good_qty += safe_float(r.get("Good_QTY"))
        sum_plan_qty += safe_float(r.get("Plan_QTY"))
        sum_plan_req_time += safe_float(r.get("Plan_REQ_Time"))

    oa_denom = sum_act_prod_time + sum_act_changeover
    avg_oa = (sum_plan_time / oa_denom) if oa_denom > 0 else 0.0

    avg_quality = ((sum_total_qty - sum_scrap_qty) / sum_total_qty) if sum_total_qty > 0 else 0.0

    or_denom = 1270.0 * window_days
    avg_or = ((sum_act_prod_time - sum_diff_dt) / or_denom) if or_denom > 0 else 0.0

    avg_oee = avg_oa * avg_quality * avg_or * 100

    now_utc = datetime.now(timezone.utc).isoformat()
    shift_count = len(job_rows)

    def format_metric(name, avg_val, raw_sum):
        return {
            "sum": str(raw_sum),
            "average": str(avg_val),
            "percentage": str(avg_val * 100.0), 
            "shift_count": shift_count,
            "assetName": asset_name,
            "propertyName": name,
            "days": str(window_days),
            "timestampUTC": now_utc
        }

    metrics = {}
    
    metrics["OA"] = format_metric("OA", avg_oa, sum_plan_time)
    metrics["OR"] = format_metric("OR", avg_or, sum_act_prod_time)
    metrics["Quality"] = format_metric("Quality", avg_quality, sum_total_qty)
    metrics["OEE"] = format_metric("OEE", avg_oee, 0.0) 
    
    metrics["performance"] = metrics["OA"]
    metrics["utilization"] = metrics["OR"]
    metrics["quality"] = metrics["Quality"]

    rem_parts = max(0.0, sum_plan_qty - sum_good_qty)
    metrics["parts_produced"] = format_metric("parts_produced", sum_good_qty / shift_count if shift_count else 0, sum_good_qty)
    metrics["lost_parts"] = format_metric("lost_parts", sum_scrap_qty / shift_count if shift_count else 0, sum_scrap_qty)
    metrics["remaining_parts"] = format_metric("remaining_parts", rem_parts / shift_count if shift_count else 0, rem_parts)
    
    metrics["Scrap_Parts"] = format_metric("Scrap_Parts", sum_scrap_qty / shift_count if shift_count else 0, sum_scrap_qty)
    metrics["Good_Parts"] = format_metric("Good_Parts", sum_good_qty / shift_count if shift_count else 0, sum_good_qty)
    metrics["Planned_Time"] = format_metric("Planned_Time", sum_plan_req_time / shift_count if shift_count else 0, sum_plan_req_time)
    metrics["Actual_Run_Time"] = format_metric("Actual_Run_Time", sum_act_prod_time / shift_count if shift_count else 0, sum_act_prod_time)
    metrics["Actual_Changeover_Time"] = format_metric("Actual_Changeover_Time", sum_act_changeover / shift_count if shift_count else 0, sum_act_changeover)
    
    metrics["changeover"] = {
        "sum": shift_count, "assetName": asset_name, "propertyName": "changeover",
        "days": str(window_days), "timestampUTC": now_utc,
    }

    return metrics

# =====================================================
# STATE AGGREGATION (Overall Date Range)
# =====================================================
def aggregate_states(state_rows, asset_name, days):
    state_map = {
        "Run to rate": "State_Run_to_rate_min",
        "Idle": "State_Idle_min",
        "Planned Downtime": "State_Planned_Downtime_min",
        "Unplanned Downtime": "State_Unplanned_Downtime_min",
        "Changeover": "State_Changeover_min"
    }
    
    totals = {k: 0.0 for k in state_map.keys()}
    
    for r in state_rows:
        press_shift = str(r.get("press_shift_time", "")).strip().upper()
        if not (press_shift.endswith("#A") or press_shift.endswith("#B") or press_shift.endswith("#C")):
            continue
            
        for display, col in state_map.items():
            v = r.get(col)
            if v is not None:
                try: totals[display] += float(v)
                except: pass
                
    total_mins = sum(totals.values())
    
    states = []
    for state, mins in totals.items():
        # FIX: Removed zero minute filter to prevent UI crashes
        pct = (mins / total_mins * 100) if total_mins > 0 else 0
        avg = mins / days if days > 0 else 0
        
        states.append({
            "state": state, 
            "minutes": float(round(mins, 4)), 
            "percentage": float(round(pct, 2)),
            "average": float(round(avg, 4)), 
            "assetName": asset_name,
            "propertyName": MACHINE_STATE_PROPERTY_NAME 
        })
        
    return {
        "assetName": asset_name, "days": days,
        "generatedAtUTC": datetime.now(timezone.utc).isoformat(),
        "states": states
    }

# =====================================================
# MACHINE STATES LOGIC (DAY-WISE SHIFT SEPARATION)
# =====================================================
def build_states_daywise(state_rows, asset_name, start_date, end_date):
    state_map = {
        "Run to rate": "State_Run_to_rate_min",
        "Idle": "State_Idle_min",
        "Planned Downtime": "State_Planned_Downtime_min",
        "Unplanned Downtime": "State_Unplanned_Downtime_min",
        "Changeover": "State_Changeover_min"
    }
    
    date_map = {}
    
    s_date = start_date[:10]
    e_date = end_date[:10]
    
    # If the user selects a range (e.g. 01 to 04), we exclude the final day (04)
    # If they select only 1 day (01 to 01), we do NOT exclude it.
    exclude_end_date = (s_date != e_date)
    
    for r in state_rows:
        dt = r.get("date_cst")
        if not dt: continue
        
        # ---> FEATURE: Exclude the last selected date <---
        if exclude_end_date and dt == e_date:
            continue
            
        press_shift = str(r.get("press_shift_time", "")).strip().upper()
        
        if press_shift.endswith("#A"): shift_id = "A"
        elif press_shift.endswith("#B"): shift_id = "B"
        elif press_shift.endswith("#C"): shift_id = "C"
        else: continue 
            
        if dt not in date_map:
            date_map[dt] = {
                "A": {k: 0.0 for k in state_map.keys()},
                "B": {k: 0.0 for k in state_map.keys()},
                "C": {k: 0.0 for k in state_map.keys()}
            }
            
        for display, col in state_map.items():
            v = r.get(col)
            if v is not None:
                try: date_map[dt][shift_id][display] += float(v)
                except: pass
                
    # -----------------------------------------------------
    # EXACT SORT LOGIC
    # -----------------------------------------------------
    sorted_dates = sorted(date_map.keys())
    results = []
    
    for dt_str in sorted_dates:
        day_shifts = date_map[dt_str]
        
        total_day_minutes = sum(sum(day_shifts[skey].values()) for skey in ["A", "B", "C"])
        
        shift_objects = {}
        
        for skey, full_key in [("A", "shiftA"), ("B", "shiftB"), ("C", "shiftC")]:
            shift_totals = day_shifts[skey]
            total_shift_minutes = sum(shift_totals.values())
            
            state_pct = {}
            for state, mins in shift_totals.items():
                # FIX: Removed the `if mins <= 0: continue` line entirely.
                # All 5 states MUST be sent to the frontend or the Javascript UI will crash!
                
                day_pct = (mins / total_day_minutes * 100) if total_day_minutes > 0 else 0.0
                shift_pct = (mins / total_shift_minutes * 100) if total_shift_minutes > 0 else 0.0
                
                state_pct[state] = {
                    "state": state, 
                    "minutes": float(round(mins, 4)), 
                    "percentage": float(day_pct),
                    "shift_percentage": float(shift_pct), 
                    "average": float(round(mins, 4)) 
                }
            
            shift_objects[full_key] = {
                "daily_metrics": {"shift": skey, "count": 1},
                "segments": [],
                "state_percentage": state_pct
            }
            
        results.append({
            "date": dt_str, 
            "assetName": asset_name,
            "shiftA": shift_objects["shiftA"],
            "shiftB": shift_objects["shiftB"],
            "shiftC": shift_objects["shiftC"]
        })

    return results


# =====================================================
# DOWNTIME REASONS LOGIC
# =====================================================
def aggregate_downtime_reasons_from_press_rows(rows):
    reason_minutes = {r: 0 for r in DOWNTIME_COLUMNS}
    reason_counts = {r: 0 for r in DOWNTIME_COLUMNS}
    for row in rows:
        for reason in DOWNTIME_COLUMNS:
            val = row.get(reason)
            if val is None: continue
            try: mins = float(val)
            except: continue
            if mins <= 0: continue
            reason_minutes[reason] += mins
            reason_counts[reason] += 1

    total_minutes = sum(reason_minutes.values())
    results = []
    for reason in DOWNTIME_COLUMNS:
        mins = reason_minutes[reason]
        if mins <= 0: continue
        count = reason_counts[reason] if reason_counts[reason] else 1
        results.append({
            "reason": reason, "minutes": round(mins, 2),
            "percentage": round((mins / total_minutes) * 100, 2) if total_minutes else 0,
            "average": round(mins / count, 2),
        })
    return sorted(results, key=lambda x: -x["minutes"])

# ================= PARALLEL WORKER =================
def process_historical_asset(asset, start_date, end_date, window_days, params, model_id):
    asset_id = asset["id"]
    asset_name = asset["name"]
    zone = derive_zone_from_machine(asset_name) or "UNKNOWN"
    
    press_rows = query_press_rows(asset_name, start_date, end_date)
    
    state_rows = []
    job_rows = []
    for r in press_rows:
        rec_type = str(r.get("RecordType", "")).strip()
        press_shift = str(r.get("press_shift_time", "")).strip().upper()
        
        if rec_type == "DailyMachineStates" or "DAILY_STATES" in press_shift:
            state_rows.append(r)
        else:
            job_rows.append(r)
    
    def truthy(val):
        if val is None: return False
        return str(val).strip().lower() in ("1", "true", "yes", "y")
    
    downtime_reasons_flag = truthy(params.get("downtime_reasons"))
    states_flag = truthy(params.get("states")) or True
    states_daywise = truthy(params.get("states_daywise")) or True 
    
    metrics = {}
    machine_states_daywise = []

    core_metrics = calculate_core_metrics(job_rows, window_days, asset_name)
    metrics.update(core_metrics)
        
    if states_flag: metrics["state"] = aggregate_states(state_rows, asset_name, window_days)
    else: metrics["state"] = None
        
    # Using the updated function with Start/End dates passed in
    if states_daywise: machine_states_daywise = build_states_daywise(state_rows, asset_name, start_date, end_date)
        
    if downtime_reasons_flag: metrics["downtime_reasons"] = aggregate_downtime_reasons_from_press_rows(job_rows)
    else: metrics["downtime_reasons"] = []

    return zone, asset_name, {
        "asset_id": asset_id,
        "historical": {
            "window_days": window_days,
            "metrics": metrics,
            "machine_states": machine_states_daywise
        }
    }

# =====================================================
# LAMBDA HANDLER
# =====================================================
def lambda_handler(event, context):
    def truthy(val):
        if val is None: return False
        return str(val).strip().lower() in ("1", "true", "yes", "y")

    params = event.get("queryStringParameters") or {}
    start_date = params.get("startDate")
    end_date = params.get("endDate")
    download_excel = truthy(params.get("download_excel"))

    if not start_date or not end_date:
        return {"statusCode": 400, "body": json.dumps({"error": "startDate and endDate required"})}

    window_days = calc_window_days(start_date, end_date)
    model_id = get_model_id_from_name(MODEL_NAME)
    
    machine_name = (params.get("machine_name") or params.get("machine") or params.get("asset"))
    if machine_name:
        if not isinstance(machine_name, str):
            return {"statusCode": 400, "body": json.dumps({"error": "machine_name must be a string"})}
        machine_name = machine_name.strip()
        if not re.match(r"^SM_SP\d+", machine_name):
            return {"statusCode": 400, "body": json.dumps({"error": "Invalid machine_name format. Expected like SM_SP102"})}

    all_assets = []
    sitewise_paginator = sitewise.get_paginator("list_assets")
    for page in sitewise_paginator.paginate(assetModelId=model_id):
        all_assets.extend(page.get("assetSummaries", []))

    if machine_name:
        assets = [a for a in all_assets if a.get("name") == machine_name]
        if not assets:
            return {"statusCode": 404, "body": json.dumps({"error": f"Machine '{machine_name}' not found"})}
    else:
        assets = all_assets

    states_daywise = truthy(params.get("states_daywise"))
    if states_daywise and window_days > 30:
        return {"statusCode": 400, "body": json.dumps({"error": "Date range cannot exceed 30 days for the flag states_daywise."})}

    # -------------------------------------------------
    # EXPORT MODE
    # -------------------------------------------------
    if download_excel:
        url = export_press_report_csv(assets, start_date, end_date)
        return {
            "statusCode": 200, 
            "headers": {
                "Content-Type": "application/json", 
                "Access-Control-Allow-Origin": "*"
            }, 
            "body": json.dumps({"download_url": url})
        }

    # -------------------------------------------------
    # NORMAL MODE (PARALLELIZED)
    # -------------------------------------------------
    zones = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
        futures = {
            executor.submit(process_historical_asset, asset, start_date, end_date, window_days, params, model_id): asset
            for asset in assets
        }
        for future in concurrent.futures.as_completed(futures):
            try:
                zone, asset_name, asset_data = future.result()
                zones.setdefault(zone, {})[asset_name] = asset_data
            except Exception as e:
                logger.error(f"Failed to process historical asset: {e}")

    response = {
        "message": "Data",
        "data": {"model_name": MODEL_NAME, "asset_count": len(assets), "zone_count": len(zones), "zones": zones},
    }

    return {"statusCode": 200, "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"}, "body": json.dumps(response, default=str)}
