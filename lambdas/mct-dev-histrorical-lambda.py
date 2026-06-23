
import os
import json
import boto3
import datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

# Initialize AWS clients outside the handler for faster warm starts
sitewise = boto3.client('iotsitewise')
dynamodb = boto3.resource('dynamodb')
iot_client = boto3.client('iot-data')

# Configuration
TABLE_NAME = os.environ.get('DYNAMODB_TABLE', 'DailyPressSummaries')
MODEL_NAME = os.environ.get('MODEL_NAME')
table = dynamodb.Table(TABLE_NAME)
CENTRAL_TZ = ZoneInfo("America/Chicago")

# ---> SET YOUR IOT THING NAME HERE <---
THING_NAME = os.environ.get('IOT_THING_NAME', 'mct-dev-greengrass-core-dk') 

# =========================================================================
# HELPER FUNCTIONS
# =========================================================================

def safe_decimal(val):
    if val is None or val == "": return Decimal('0')
    try: return Decimal(str(round(float(val), 4)))
    except: return Decimal('0')

def parse_state_string(raw_str):
    if not raw_str: return "Unknown"
    if raw_str.startswith('{'):
        try: return json.loads(raw_str).get('MachineStateAndShift', 'Unknown').split(':')[0].strip()
        except: return "Unknown"
    return raw_str.split(':')[0].strip()

def iso_to_epoch(iso_str):
    if not iso_str or iso_str == 'UnknownTime': return None
    try: return int(datetime.datetime.fromisoformat(iso_str).timestamp())
    except: return None

# =========================================================================
# DYNAMIC CONFIGURATION & SHADOW LOGIC
# =========================================================================


def fetch_shadow_config():
    """Parses the nested Zones, Stations, and Shift Templates from the Shadow."""
    try:
        # ---> ADDED shadowName='config' HERE <---
        response = iot_client.get_thing_shadow(
            thingName=THING_NAME,
            shadowName='config' 
        )
        payload = json.loads(response['payload'].read())
        
        # Check 'reported' first, fallback to 'desired'
        state_data = payload.get('state', {}).get('reported') or payload.get('state', {}).get('desired', {})        
        zones = state_data.get('zones', {})
        templates = state_data.get('shiftTemplates', {})
        
        active_machines = []
        machine_shift_ranges = {} # Maps machine -> its specific shift timings
        
        for zone_name, zone_data in zones.items():
            template_name = zone_data.get('shiftTemplate')
            stations = zone_data.get('stations', {})
            
            # Extract the raw shifts for this zone's template (e.g., "Shift 3")
            raw_shifts = templates.get(template_name, {})
            processed_shifts = {}
            
            for shift_full_name, times in raw_shifts.items():
                # Extract "A", "B", "C" from names like "Shift 3-A"
                shift_letter = shift_full_name.split('-')[-1] if '-' in shift_full_name else shift_full_name
                
                start_h, start_m = map(int, times['start'].split(':'))
                end_h, end_m = map(int, times['end'].split(':'))
                
                processed_shifts[shift_letter] = {
                    'start_min': (start_h * 60) + start_m,
                    'end_min': (end_h * 60) + end_m
                }
            
            # Map the processed shifts to the active stations in this zone
            for machine_id, machine_data in stations.items():
                if str(machine_data.get('status', '')).lower() == 'active':
                    active_machines.append(machine_id)
                    machine_shift_ranges[machine_id] = processed_shifts
                    
        print(f"[CONFIG] Fetched {len(active_machines)} active machines from Shadow.")
        return active_machines, machine_shift_ranges

    except Exception as e:
        print(f"[ERROR] Failed to fetch Shadow for {THING_NAME}: {e}")
        return [], {}

def detect_correct_shift(start_time_str, reported_shift, dynamic_shift_ranges):
    if not start_time_str or start_time_str == 'UnknownTime' or not dynamic_shift_ranges: 
        return reported_shift
    try:
        time_part = start_time_str.split('T')[1]
        total_mins = (int(time_part.split(':')[0]) * 60) + int(time_part.split(':')[1])
        
        for shift_letter, limits in dynamic_shift_ranges.items():
            s_min = limits['start_min']
            e_min = limits['end_min']
            
            if s_min < e_min:
                if s_min <= total_mins < e_min: return shift_letter
            else:
                if total_mins >= s_min or total_mins < e_min: return shift_letter
                
        return reported_shift
    except Exception: 
        return reported_shift

# =========================================================================
# SITEWISE HISTORY METRICS
# =========================================================================

def get_state_history(machine, start, end, base_path):
    states = []
    alias = f"{base_path}/{machine}/MachineStateAndShift"
    try:
        lookback = start - datetime.timedelta(days=2)
        paginator = sitewise.get_paginator('get_asset_property_value_history')
        for page in paginator.paginate(propertyAlias=alias, startDate=lookback, endDate=end):
            for entry in page.get('assetPropertyValueHistory', []):
                val = parse_state_string(entry['value'].get('stringValue'))
                ts = entry['timestamp']['timeInSeconds']
                states.append((ts, val))
    except Exception as e: 
        pass
    states.sort(key=lambda x: x[0])
    return states

def calculate_state_metrics(states_list, window_start_ts, window_end_ts):
    if not window_start_ts or not window_end_ts or window_start_ts >= window_end_ts: return {}
    total_window_seconds = window_end_ts - window_start_ts
    current_state = None  
    
    for ts, st in states_list:
        if ts <= window_start_ts: current_state = st

    calc_start = window_start_ts
    if current_state is None and states_list:
        first_ts = states_list[0][0]
        if first_ts < window_end_ts:
            calc_start = max(window_start_ts, first_ts)
            current_state = states_list[0][1]

    if current_state is None:
        return {"State_Unknown_min": safe_decimal(total_window_seconds / 60.0)}

    timeline = [(calc_start, current_state)]
    for ts, st in states_list:
        if calc_start < ts < window_end_ts: timeline.append((ts, st))

    durations = {}
    for i in range(len(timeline)):
        ts, state = timeline[i]
        next_ts = timeline[i+1][0] if (i+1) < len(timeline) else window_end_ts
        durations[state] = durations.get(state, 0) + (next_ts - ts)

    final_metrics = {}
    tracked_seconds = sum(durations.values())
    gap_seconds = total_window_seconds - tracked_seconds

    for state, seconds in durations.items():
        if seconds > 0:
            final_metrics[f"State_{state.replace(' ', '_')}_min"] = safe_decimal(seconds / 60.0)
            
    if gap_seconds > 0:
        final_metrics["State_Unknown_min"] = safe_decimal(gap_seconds / 60.0)
            
    return final_metrics

# =========================================================================
# MAIN HANDLER
# =========================================================================

def lambda_handler(event, context):
    end_time = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    start_time = end_time - datetime.timedelta(days=1)
    
    # 1. FETCH DYNAMIC CONFIG FROM IOT SHADOW
    machines, machine_shift_ranges = fetch_shadow_config()
    
    if not machines:
        return {"statusCode": 500, "body": "No active machines found in IoT Shadow."}

    print(f"Time Window: {start_time} to {end_time}")
    
    base_model_path = MODEL_NAME
    records_processed = 0
    daily_metrics_lookup = {}
    
    # 2. PASS 1: Fetch Daily Summaries
    for machine in machines:
        alias = f"{base_model_path}/{machine}/DailySummary"
        next_token = None
        while True:
            try:
                kwargs = {'propertyAlias': alias, 'startDate': start_time, 'endDate': end_time}
                if next_token: kwargs['nextToken'] = next_token
                response = sitewise.get_asset_property_value_history(**kwargs)
                
                for entry in response.get('assetPropertyValueHistory', []):
                    raw_json_str = entry.get('value', {}).get('stringValue')
                    if not raw_json_str: continue
                    
                    data = json.loads(raw_json_str)
                    date_cst = data.get('ShiftDateCST', 'UnknownDate')
                    lookup_key = f"{machine}#{date_cst}"
                    if lookup_key not in daily_metrics_lookup:
                        daily_metrics_lookup[lookup_key] = {
                            'oee': safe_decimal(data.get('OEE', 0)),
                            'or_utilization': safe_decimal(data.get('OR', 0))
                        }
                next_token = response.get('nextToken')
                if not next_token: break 
            except Exception as e:
                pass
                break 

    # 3. PASS 2: Fetch Job Summaries & Batch Write to DynamoDB
    with table.batch_writer(overwrite_by_pkeys=['date_cst', 'press_shift_time']) as batch:
        for machine in machines:
            
            # Grab this specific machine's shift ranges from the Shadow dict
            dynamic_shift_ranges = machine_shift_ranges.get(machine, {})
            
            state_history = get_state_history(machine, start_time, end_time, base_model_path)
            alias = f"{base_model_path}/{machine}/JobSummary"
            next_token = None
            
            while True:
                try:
                    kwargs = {'propertyAlias': alias, 'startDate': start_time, 'endDate': end_time}
                    if next_token: kwargs['nextToken'] = next_token
                    response = sitewise.get_asset_property_value_history(**kwargs)
                    
                    for entry in response.get('assetPropertyValueHistory', []):
                        raw_json_str = entry.get('value', {}).get('stringValue')
                        if not raw_json_str: continue
                        
                        data = json.loads(raw_json_str)
                        date_cst = data.get('ShiftDateCST', 'UnknownDate')
                        asset_name = data.get('Asset', machine)
                        reported_shift = data.get('Shift', 'UnknownShift')
                        start_time_str = data.get('Operator_Start') or data.get('Changeover_Start', 'UnknownTime')
                        
                        # ---> DYNAMIC SHIFT DETECTION <---
                        actual_shift = detect_correct_shift(start_time_str, reported_shift, dynamic_shift_ranges)
                        
                        plan_qty = float(data.get('PlanQty', 0))
                        spm = float(data.get('SPM', 0))
                        good_qty = float(data.get('GoodQty', 0))
                        scrap_qty = float(data.get('ScrapQty', 0))
                        act_prod_time = float(data.get('OperatorMinutes', 0))
                        total_qty = float(data.get('TotalProductionQty', 0))

                        if good_qty == 0 and scrap_qty > 0 and total_qty > 0:
                            good_qty = total_qty - scrap_qty
                        
                        plan_req_time = (plan_qty / spm) if spm > 0 else 0
                        plan_time_new_qty = (total_qty / spm) if spm > 0 else 0
                        diff_dt = act_prod_time - plan_time_new_qty
                        
                        zone_val = asset_name[5:6] if len(asset_name) >= 6 else ""
                        lookup_key = f"{asset_name}#{date_cst}"
                        daily_data = daily_metrics_lookup.get(lookup_key, {})
                        
                        item = {
                            'date_cst': date_cst,
                            'press_shift_time': f"{asset_name}#{reported_shift}#{start_time_str}",
                            'Zone': zone_val,
                            'Date': date_cst,
                            'PRESS_NO': asset_name,
                            'SHIFT_NO': reported_shift,
                            'Order': data.get('MOrder', ''),
                            'DIE_NO': data.get('DieNumber', ''),
                            'Part_No': data.get('PartNumber', ''),
                            'Plan_QTY': safe_decimal(plan_qty),
                            'Plan_REQ_Time': safe_decimal(plan_req_time),
                            'SPM': safe_decimal(spm),
                            'Total_QTY': safe_decimal(total_qty),
                            'Good_QTY': safe_decimal(good_qty),
                            'Scrap_QTY': safe_decimal(scrap_qty),
                            'Actual_SPM': safe_decimal(spm),
                            'Start_ChangeOver': data.get('Changeover_Start', ''),
                            'END_Changeover': data.get('Changeover_End', ''),
                            'Actual_Changeover_min': safe_decimal(data.get('ChangeoverMinutes', 0)),
                            'Die_Setter_ID': data.get('DieSetterID',''),
                            'START_Time': data.get('Operator_Start', ''),
                            'END_Time': data.get('Operator_End', ''),
                            'Employee_ID': data.get('OperatorID',''),
                            'PLAN_Time_with_new_QTY': safe_decimal(plan_time_new_qty),
                            'ACTProduction_Time': safe_decimal(act_prod_time),
                            'DIFF_DT': safe_decimal(diff_dt),
                            'OA': safe_decimal(data.get('OA', 0)),
                            'OR': daily_data.get('or_utilization', Decimal('0')),
                            'Quality': safe_decimal(data.get('Quality', 0)),
                            'OEE': daily_data.get('oee', Decimal('0')),
                            'RecordType': 'JobSummary' 
                        }
                        
                        downtime_reasons = data.get('DowntimeReasons', {})
                        for reason_key, duration_val in downtime_reasons.items():
                            item[reason_key] = safe_decimal(duration_val)
                            
                        batch.put_item(Item=item)
                        records_processed += 1
                        
                    next_token = response.get('nextToken')
                    if not next_token: break 
                except Exception as e:
                    pass
                    break 
            
            # 4. CREATE DAILY MACHINE STATE ROWS SHIFT-WISE
            target_date = start_time.astimezone(CENTRAL_TZ).date()
            target_date_str = target_date.strftime("%Y-%m-%d")
            
            if not dynamic_shift_ranges:
                # Fallback: if no shadow config, dump it all into a single 'ALL_DAY' row
                window_start = int(start_time.timestamp())
                window_end = int(end_time.timestamp())
                daily_state_results = calculate_state_metrics(state_history, window_start, window_end)
                
                daily_state_item = {
                    'date_cst': target_date_str,
                    'Date': target_date_str,
                    'PRESS_NO': machine,
                    'Zone': machine[5:6] if len(machine) >= 6 else "",
                    'press_shift_time': f"{machine}#DAILY_STATES", 
                    'SHIFT_NO': "ALL_DAY",
                    'RecordType': 'DailyMachineStates' 
                }
                daily_state_item.update(daily_state_results)
                batch.put_item(Item=daily_state_item)
                records_processed += 1
            else:
                # Loop through each shift ("A", "B", "C") defined in the shadow
                for shift_letter, limits in dynamic_shift_ranges.items():
                    start_m = limits['start_min']
                    end_m = limits['end_min']

                    sh = start_m // 60
                    sm = start_m % 60
                    eh = end_m // 60
                    em = end_m % 60

                    # Convert the start time into a CST datetime object
                    shift_start_dt = datetime.datetime(
                        target_date.year, target_date.month, target_date.day,
                        sh, sm, tzinfo=CENTRAL_TZ
                    )

                    # Convert the end time into a CST datetime object
                    shift_end_dt = datetime.datetime(
                        target_date.year, target_date.month, target_date.day,
                        eh, em, tzinfo=CENTRAL_TZ
                    )

                    # Handle overnight shifts (e.g. 23:00 to 07:00) by pushing the end time to the next calendar day
                    if end_m <= start_m:
                        shift_end_dt += datetime.timedelta(days=1)

                    # Convert back to standard UTC timestamp to slice the SiteWise state history
                    shift_start_ts = int(shift_start_dt.timestamp())
                    shift_end_ts = int(shift_end_dt.timestamp())

                    # Calculate durations exclusively for this shift's slice
                    shift_state_results = calculate_state_metrics(state_history, shift_start_ts, shift_end_ts)

                    shift_state_item = {
                        'date_cst': target_date_str,
                        'Date': target_date_str,
                        'PRESS_NO': machine,
                        'Zone': machine[5:6] if len(machine) >= 6 else "",
                        'press_shift_time': f"{machine}#DAILY_STATES#{shift_letter}", # Distinct row per shift
                        'SHIFT_NO': shift_letter,
                        'RecordType': 'DailyMachineStates' 
                    }
                    
                    shift_state_item.update(shift_state_results)
                    batch.put_item(Item=shift_state_item)
                    records_processed += 1

    return {
        'statusCode': 200,
        'body': f"Success! Fetched data dynamically from Shadow and batch-processed {records_processed} records into DynamoDB."
    }
