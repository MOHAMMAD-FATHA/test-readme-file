# ==============================================================================
#"AWS_ENDPOINT": "a3mw4u4go8765p-ats.iot.us-east-1.amazonaws.com",
# "SITEWISE_MODEL_NAME": "dnadct-oa2-dev-mct-model_core"
# ==============================================================================


# ==============================================================================
# FILE: deploy_components.sh
#
# DESCRIPTION:
# This script dynamically packages, registers, and deploys custom AWS Greengrass
# V2 Python components (Live, Storage, Reporting, Shadow Sync, Logger) to an 
# Edge device. It also supports injecting configuration updates to an existing 
# EMQX broker deployment.
#
# HOW IT WORKS:
# 1. Parses command-line flags to determine which custom components to update.
# 2. Creates new Greengrass component versions inline (via AWS CLI) using the 
#    provided S3 artifact URIs and minified JSON recipes.
# 3. Fetches the active deployment currently running on the Core Device.
# 4. Modifies the existing deployment payload by merging in the newly created 
#    component versions and the EMQX port/SSL configuration (if requested via -e).
# 5. Triggers a new OTA (Over-The-Air) deployment to apply the updates seamlessly
#    without disrupting other unselected components on the edge device.
# ==============================================================================

#!/usr/bin/env bash
set -euo pipefail

# =====================================================
# INPUT ARGUMENT PARSING
# =====================================================
usage() {
  echo "Usage: $0 [-l <live_ver>] [-s <storage_ver>] [-r <reporting_ver>] [-w <shadow_ver>] [-c <logger_ver>] [-e] -m <model> -t <topic> -b <bucket> [-n <shadow_name>] [-d <retain_days>]"
  echo ""
  echo "Component Flags (Choose at least one, or use -e):"
  echo "  -l  <version> : Deploys 'com.data.transform.live'"
  echo "  -s  <version> : Deploys 'com.data.transform.storage'"
  echo "  -r  <version> : Deploys 'com.data.transform.reporting'"
  echo "  -w  <version> : Deploys 'com.data.transform.shadow.sync'"
  echo "  -c  <version> : Deploys 'com.data.transform.logger'"
  echo "  -e            : Updates the existing EMQX broker configuration"
  echo ""
  echo "Required Configuration Flags:"
  echo "  -m  <model>   : AWS SiteWise Model Name"
  echo "  -t  <topic>   : MQTT Sub Topic"
  echo "  -b  <bucket>  : S3 Bucket Name holding the artifacts"
  echo ""
  echo "Conditional Flags:"
  echo "  -n  <name>    : AWS Shadow Name (REQUIRED if using -w)"
  echo "  -d  <days>    : Log Retention in Days (REQUIRED if using -c)"
  echo ""
  echo "Example (Deploy All + EMQX update):"
  echo "  $0 -l 1.0.3 -s 2.1.0 -r 1.1.0 -w 1.0.1 -c 1.0.0 -e -m dnadct-oa2-dev-mct-model_core -t \"oa/us/dna/dttp/dttp/+/+/+\" -b greengrass-core-bucket -n \"config\" -d \"7\""
  exit 1
}

LIVE_VERSION=""
STORAGE_VERSION=""
REPORTING_VERSION=""
SHADOW_VERSION=""
LOGGER_VERSION=""
UPDATE_EMQX=false
SHADOW_NAME=""
RETAIN_DAYS=""

# Parse command-line flags
while getopts "l:s:r:w:c:m:t:b:n:d:eh" opt; do
  case "$opt" in
    l) LIVE_VERSION="$OPTARG" ;;
    s) STORAGE_VERSION="$OPTARG" ;;
    r) REPORTING_VERSION="$OPTARG" ;;
    w) SHADOW_VERSION="$OPTARG" ;;
    c) LOGGER_VERSION="$OPTARG" ;;
    e) UPDATE_EMQX=true ;;
    m) SITEWISE_MODEL_NAME="$OPTARG" ;;
    t) SUB_TOPIC="$OPTARG" ;;
    b) S3_BUCKET_NAME="$OPTARG" ;;
    n) SHADOW_NAME="$OPTARG" ;;
    d) RETAIN_DAYS="$OPTARG" ;;
    h) usage ;;
    *) usage ;;
  esac
done

# Ensure mandatory global variables were provided
if [ -z "${SITEWISE_MODEL_NAME:-}" ] || [ -z "${SUB_TOPIC:-}" ] || [ -z "${S3_BUCKET_NAME:-}" ]; then
  echo "ERROR: Missing required global arguments (-m, -t, -b)."
  usage
fi

# Ensure at least one action is selected
if [ -z "$LIVE_VERSION" ] && [ -z "$STORAGE_VERSION" ] && [ -z "$REPORTING_VERSION" ] && [ -z "$SHADOW_VERSION" ] && [ -z "$LOGGER_VERSION" ] && [ "$UPDATE_EMQX" = false ]; then
  echo "ERROR: You must specify at least one action to perform (-l, -s, -r, -w, -c, or -e)."
  usage
fi

# Ensure shadow name is provided if shadow sync component is selected
if [ -n "$SHADOW_VERSION" ] && [ -z "$SHADOW_NAME" ]; then
  echo "ERROR: You must provide a shadow name (-n) when deploying the shadow sync component (-w)."
  usage
fi

# Ensure retain days are provided if logger component is selected
if [ -n "$LOGGER_VERSION" ] && [ -z "$RETAIN_DAYS" ]; then
  echo "ERROR: You must provide retain days (-d) when deploying the logger component (-c)."
  usage
fi

# =====================================================
# CORE CONFIGURATION
# =====================================================
GG_CONFIG="/greengrass/v2/config/effectiveConfig.yaml"
REGION=$(grep 'awsRegion:' "$GG_CONFIG" | awk '{print $2}' | tr -d '"')
CORE_THING_NAME=$(grep -m1 'thingName:' "$GG_CONFIG" | awk -F': ' '{print $2}' | tr -d '"')
CORE_ARN=$(aws iot describe-thing --thing-name "$CORE_THING_NAME" --region "$REGION" --query thingArn --output text)
AWS_ENDPOINT=$(aws iot describe-endpoint --query 'endpointAddress' --output text)

echo "=========================================="
echo "Target Core : $CORE_THING_NAME ($REGION)"
echo "Topic       : $SUB_TOPIC"
echo "Model       : $SITEWISE_MODEL_NAME"
if [ -n "$SHADOW_NAME" ]; then echo "Shadow Name : $SHADOW_NAME"; fi
if [ -n "$RETAIN_DAYS" ]; then echo "Log Retain  : $RETAIN_DAYS days"; fi
if [ "$UPDATE_EMQX" = true ]; then echo "EMQX Update : ENABLED"; fi
echo "=========================================="

# Initialize a temporary JSON file to hold our dynamic custom component list
echo "{}" > /tmp/new_components.json

# =====================================================
# 1. GENERATE & CREATE CUSTOM COMPONENTS
# =====================================================

# --- LIVE COMPONENT ---
if [ -n "$LIVE_VERSION" ]; then
  COMP_NAME="com.data.transform.live"
  URI="s3://${S3_BUCKET_NAME}/artifacts/${COMP_NAME}/${LIVE_VERSION}/live_state_engine.py"
  RECIPE="/tmp/${COMP_NAME}_recipe.json"
  echo "Processing $COMP_NAME v$LIVE_VERSION..."

cat <<EOF > "$RECIPE"
{
  "RecipeFormatVersion": "2020-01-25",
  "ComponentName": "${COMP_NAME}",
  "ComponentVersion": "${LIVE_VERSION}",
  "ComponentType": "aws.greengrass.generic",
  "Manifests": [{ "Platform": { "os": "linux" }, "Lifecycle": { "Install": { "Script": "mkdir -p /greengrass/v2/oee_engine/data && chown -R ggc_user:ggc_group /greengrass/v2/oee_engine && chmod -R 775 /greengrass/v2/oee_engine/data ", "RequiresPrivilege": true }, "Run": "python3 -u {artifacts:path}/live_state_engine.py", "Setenv": { "CONFIG_PATH": "/greengrass/v2/oee_engine/config/station_config.json", "DB_PATH": "/greengrass/v2/oee_engine/data/stations_data.db", "SUB_TOPIC": "${SUB_TOPIC}","SITEWISE_MODEL_NAME": "${SITEWISE_MODEL_NAME}" } }, "Artifacts": [{ "Uri": "${URI}", "Unarchive": "NONE", "Permission": { "Read": "OWNER", "Execute": "NONE" } }] }]
}
EOF
  CREATE_OUT=$(aws greengrassv2 create-component-version --inline-recipe fileb://"$RECIPE" --region "$REGION" 2>&1) || STATUS=$?
  if [ "${STATUS:-0}" -ne 0 ] && ! echo "$CREATE_OUT" | grep -q "ConflictException"; then echo "ERROR: Failed to create $COMP_NAME" && exit 1; fi
  jq --arg comp "$COMP_NAME" --arg ver "$LIVE_VERSION" '. + { ($comp): { "componentVersion": $ver } }' "/tmp/new_components.json" > "/tmp/tmp_comps.json" && mv "/tmp/tmp_comps.json" "/tmp/new_components.json"
fi

# --- STORAGE COMPONENT ---
if [ -n "$STORAGE_VERSION" ]; then
  COMP_NAME="com.data.transform.storage"
  URI="s3://${S3_BUCKET_NAME}/artifacts/${COMP_NAME}/${STORAGE_VERSION}/data_storage_engine.py"
  RECIPE="/tmp/${COMP_NAME}_recipe.json"
  echo "Processing $COMP_NAME v$STORAGE_VERSION..."

cat <<EOF > "$RECIPE"
{
  "RecipeFormatVersion": "2020-01-25",
  "ComponentName": "${COMP_NAME}",
  "ComponentVersion": "${STORAGE_VERSION}",
  "ComponentType": "aws.greengrass.generic",
  "Manifests": [{ "Platform": { "os": "linux" }, "Lifecycle": { "Install": { "Script": "mkdir -p /greengrass/v2/oee_engine/data && chown -R ggc_user:ggc_group /greengrass/v2/oee_engine && chmod -R 775 /greengrass/v2/oee_engine/data ", "RequiresPrivilege": true }, "Run": "python3 -u {artifacts:path}/data_storage_engine.py", "Setenv": { "CONFIG_PATH": "/greengrass/v2/oee_engine/config/station_config.json", "DB_PATH": "/greengrass/v2/oee_engine/data/stations_data.db", "SUB_TOPIC": "${SUB_TOPIC}" } }, "Artifacts": [{ "Uri": "${URI}", "Unarchive": "NONE", "Permission": { "Read": "OWNER", "Execute": "NONE" } }] }]
}
EOF
  CREATE_OUT=$(aws greengrassv2 create-component-version --inline-recipe fileb://"$RECIPE" --region "$REGION" 2>&1) || STATUS=$?
  if [ "${STATUS:-0}" -ne 0 ] && ! echo "$CREATE_OUT" | grep -q "ConflictException"; then echo "ERROR: Failed to create $COMP_NAME" && exit 1; fi
  jq --arg comp "$COMP_NAME" --arg ver "$STORAGE_VERSION" '. + { ($comp): { "componentVersion": $ver } }' "/tmp/new_components.json" > "/tmp/tmp_comps.json" && mv "/tmp/tmp_comps.json" "/tmp/new_components.json"
fi

# --- REPORTING COMPONENT ---
if [ -n "$REPORTING_VERSION" ]; then
  COMP_NAME="com.data.transform.reporting"
  URI="s3://${S3_BUCKET_NAME}/artifacts/${COMP_NAME}/${REPORTING_VERSION}/reporting_calc_engine.py"
  RECIPE="/tmp/${COMP_NAME}_recipe.json"
  echo "Processing $COMP_NAME v$REPORTING_VERSION..."

cat <<EOF > "$RECIPE"
{
  "RecipeFormatVersion": "2020-01-25",
  "ComponentName": "${COMP_NAME}",
  "ComponentVersion": "${REPORTING_VERSION}",
  "ComponentType": "aws.greengrass.generic",
  "ComponentConfiguration": { "DefaultConfiguration": { "accessControl": { "aws.greengrass.ipc.mqttproxy": { "${COMP_NAME}:mqttproxy:1": { "policyDescription": "Allows access to Publish to IoT Core.", "operations": ["aws.greengrass#PublishToIoTCore"], "resources": ["*"] } } } } },
  "Manifests": [{ "Platform": { "os": "linux" }, "Lifecycle": { "Install": { "Script": "mkdir -p /greengrass/v2/oee_engine/data && chown -R ggc_user:ggc_group /greengrass/v2/oee_engine && chmod -R 775 /greengrass/v2/oee_engine/data ", "RequiresPrivilege": true }, "Run": "python3 -u {artifacts:path}/reporting_calc_engine.py", "Setenv": { "CONFIG_PATH": "/greengrass/v2/oee_engine/config/station_config.json", "DB_PATH": "/greengrass/v2/oee_engine/data/stations_data.db", "SITEWISE_MODEL_NAME": "${SITEWISE_MODEL_NAME}" } }, "Artifacts": [{ "Uri": "${URI}", "Unarchive": "NONE", "Permission": { "Read": "OWNER", "Execute": "NONE" } }] }]
}
EOF
  CREATE_OUT=$(aws greengrassv2 create-component-version --inline-recipe fileb://"$RECIPE" --region "$REGION" 2>&1) || STATUS=$?
  if [ "${STATUS:-0}" -ne 0 ] && ! echo "$CREATE_OUT" | grep -q "ConflictException"; then echo "ERROR: Failed to create $COMP_NAME" && exit 1; fi
  jq --arg comp "$COMP_NAME" --arg ver "$REPORTING_VERSION" '. + { ($comp): { "componentVersion": $ver } }' "/tmp/new_components.json" > "/tmp/tmp_comps.json" && mv "/tmp/tmp_comps.json" "/tmp/new_components.json"
fi

# --- SHADOW SYNC COMPONENT ---
if [ -n "$SHADOW_VERSION" ]; then
  COMP_NAME="com.data.transform.shadow.sync"
  URI="s3://${S3_BUCKET_NAME}/artifacts/${COMP_NAME}/${SHADOW_VERSION}/shadow_config_updater.py"
  RECIPE="/tmp/${COMP_NAME}_recipe.json"
  echo "Processing $COMP_NAME v$SHADOW_VERSION..."

cat <<EOF > "$RECIPE"
{
  "RecipeFormatVersion": "2020-01-25",
  "ComponentName": "${COMP_NAME}",
  "ComponentVersion": "${SHADOW_VERSION}",
  "ComponentType": "aws.greengrass.generic",
  "ComponentConfiguration": { "DefaultConfiguration": { "accessControl": { "aws.greengrass.ipc.mqttproxy": { "${COMP_NAME}:mqttproxy:1": { "operations": ["aws.greengrass#PublishToIoTCore", "aws.greengrass#SubscribeToIoTCore"], "resources": ["*"] } }, "aws.greengrass.ipc.pubsub": { "${COMP_NAME}:pubsub:1": { "operations": ["aws.greengrass#PublishToTopic", "aws.greengrass#SubscribeToTopic"], "resources": ["*"] } } } } },
  "Manifests": [{ "Platform": { "os": "linux" }, "Lifecycle": { "Install": { "Script": "chown -R ggc_user:ggc_group /greengrass/v2/certs && chmod -R 775 /greengrass/v2/certs", "RequiresPrivilege": true }, "Run": "python3 -u {artifacts:path}/shadow_config_updater.py", "Setenv": { "AWS_ENDPOINT": "${AWS_ENDPOINT}", "AWS_SHADOW_NAME": "${SHADOW_NAME}", "CONFIG_PATH": "/greengrass/v2/oee_engine/config/station_config.json" } }, "Artifacts": [{ "Uri": "${URI}", "Unarchive": "NONE", "Permission": { "Read": "OWNER", "Execute": "NONE" } }] }]
}
EOF
  CREATE_OUT=$(aws greengrassv2 create-component-version --inline-recipe fileb://"$RECIPE" --region "$REGION" 2>&1) || STATUS=$?
  if [ "${STATUS:-0}" -ne 0 ] && ! echo "$CREATE_OUT" | grep -q "ConflictException"; then echo "ERROR: Failed to create $COMP_NAME" && exit 1; fi
  jq --arg comp "$COMP_NAME" --arg ver "$SHADOW_VERSION" '. + { ($comp): { "componentVersion": $ver } }' "/tmp/new_components.json" > "/tmp/tmp_comps.json" && mv "/tmp/tmp_comps.json" "/tmp/new_components.json"
fi

# --- CSV LOGGER COMPONENT ---
if [ -n "$LOGGER_VERSION" ]; then
  COMP_NAME="com.data.transform.logger"
  URI="s3://${S3_BUCKET_NAME}/artifacts/${COMP_NAME}/${LOGGER_VERSION}/csv_logger_engine.py"
  RECIPE="/tmp/${COMP_NAME}_recipe.json"
  echo "Processing $COMP_NAME v$LOGGER_VERSION..."

cat <<EOF > "$RECIPE"
{
  "RecipeFormatVersion": "2020-01-25",
  "ComponentName": "${COMP_NAME}",
  "ComponentVersion": "${LOGGER_VERSION}",
  "ComponentType": "aws.greengrass.generic",
  "ComponentConfiguration": { "DefaultConfiguration": { "accessControl": { "aws.greengrass.ipc.mqttproxy": { "${COMP_NAME}:mqttproxy:1": { "operations": ["aws.greengrass#PublishToIoTCore"], "resources": ["*"] } } } } },
  "Manifests": [{ "Platform": { "os": "linux" }, "Lifecycle": { "Install": { "Script": "mkdir -p /greengrass/v2/oee_engine/logs && chown -R ggc_user:ggc_group /greengrass/v2/oee_engine && chmod -R 775 /greengrass/v2/oee_engine/logs ", "RequiresPrivilege": true }, "Run": "python3 -u {artifacts:path}/csv_logger_engine.py", "Setenv": { "CSV_LOG_DIR": "/greengrass/v2/oee_engine/logs", "SUB_TOPIC": "${SUB_TOPIC}", "RETAIN_DAYS": "${RETAIN_DAYS}" } }, "Artifacts": [{ "Uri": "${URI}", "Unarchive": "NONE", "Permission": { "Read": "OWNER", "Execute": "NONE" } }] }]
}
EOF
  CREATE_OUT=$(aws greengrassv2 create-component-version --inline-recipe fileb://"$RECIPE" --region "$REGION" 2>&1) || STATUS=$?
  if [ "${STATUS:-0}" -ne 0 ] && ! echo "$CREATE_OUT" | grep -q "ConflictException"; then echo "ERROR: Failed to create $COMP_NAME" && exit 1; fi
  jq --arg comp "$COMP_NAME" --arg ver "$LOGGER_VERSION" '. + { ($comp): { "componentVersion": $ver } }' "/tmp/new_components.json" > "/tmp/tmp_comps.json" && mv "/tmp/tmp_comps.json" "/tmp/new_components.json"
fi

# =====================================================
# 2. FETCH CURRENT DEPLOYMENT
# =====================================================
echo "Fetching current effective deployment..."
DEPLOYMENT_INFO=$(aws greengrassv2 list-effective-deployments \
  --core-device-thing-name "$CORE_THING_NAME" \
  --region "$REGION" \
  --query 'effectiveDeployments[0]' --output json)

DEPLOYMENT_ID=$(echo "$DEPLOYMENT_INFO" | jq -r '.deploymentId')
DEPLOYMENT_NAME=$(echo "$DEPLOYMENT_INFO" | jq -r '.deploymentName')

aws greengrassv2 get-deployment \
  --deployment-id "$DEPLOYMENT_ID" \
  --region "$REGION" > "/tmp/current-deployment.json"

# =====================================================
# 3. INJECT EMQX CONFIGURATION (IF REQUESTED)
# =====================================================
if [ "$UPDATE_EMQX" = true ]; then
  echo "Injecting EMQX configuration update..."
  
  EMQX_COMPONENT=$(jq -r '.components | keys[] | select(test("EMQX"))' "/tmp/current-deployment.json")
  
  if [ -n "$EMQX_COMPONENT" ]; then
    EMQX_VERSION=$(jq -r ".components[\"$EMQX_COMPONENT\"].componentVersion" "/tmp/current-deployment.json")
    
    cat <<EOF > "/tmp/emqx-merge.json"
{
  "emqxConfig": { "authorization": { "no_match": "allow" }, "listeners": { "tcp": { "default": { "enabled": true, "enable_authn": false } }, "ssl": { "default": { "bind": 443, "enabled": true, "enable_authn": true, "ssl_options": { "verify": "verify_none", "fail_if_no_peer_cert": false } } } } },
  "authMode": "bypass",
  "dockerOptions": "-p 443:443 -p 127.0.0.1:1883:1883",
  "requiresPrivilege": true
}
EOF
    MERGE_STRING=$(jq -c . "/tmp/emqx-merge.json")

    jq --arg comp "$EMQX_COMPONENT" \
       --arg ver "$EMQX_VERSION" \
       --arg merge "$MERGE_STRING" \
       '.components[$comp] |= (. // {} | .componentVersion = $ver | .configurationUpdate = {merge: $merge})' \
       "/tmp/current-deployment.json" > "/tmp/tmp_deploy.json" && mv "/tmp/tmp_deploy.json" "/tmp/current-deployment.json"
    echo "EMQX Config injected successfully."
  else
    echo "WARNING: EMQX component not found in current deployment. Skipping EMQX update."
  fi
fi


# =====================================================
# 4. INJECT CUSTOM COMPONENTS & TRIGGER DEPLOYMENT
# =====================================================
echo "Extracting and merging components into deployment payload..."

# Use + instead of += to extract the .components object and drop the root metadata
jq --slurpfile new_comps "/tmp/new_components.json" \
   '.components + $new_comps[0]' \
   "/tmp/current-deployment.json" > "/tmp/components-payload.json"

echo "Triggering deployment..."
aws greengrassv2 create-deployment \
  --target-arn "$CORE_ARN" \
  --deployment-name "${DEPLOYMENT_NAME}-dynamic-update" \
  --components file://"/tmp/components-payload.json" \
  --region "$REGION"

# Cleanup
rm -f /tmp/*_recipe.json "/tmp/current-deployment.json" "/tmp/components-payload.json" "/tmp/new_components.json" "/tmp/tmp_comps.json" "/tmp/emqx-merge.json" 2>/dev/null || true

echo "Deployment triggered successfully!"

# =====================================================
# # 4. INJECT CUSTOM COMPONENTS & TRIGGER DEPLOYMENT
# # =====================================================
# echo "Merging selected components into deployment payload..."
# jq --slurpfile new_comps "/tmp/new_components.json" \
#    '.components += $new_comps[0]' \
#    "/tmp/current-deployment.json" > "/tmp/components-updated.json"

# echo "Triggering deployment..."
# aws greengrassv2 create-deployment \
#   --target-arn "$CORE_ARN" \
#   --deployment-name "${DEPLOYMENT_NAME}-dynamic-update" \
#   --components file://"/tmp/components-updated.json" \
#   --region "$REGION"

# # Cleanup
# rm -f /tmp/*_recipe.json "/tmp/current-deployment.json" "/tmp/components-updated.json" "/tmp/new_components.json" "/tmp/tmp_comps.json" "/tmp/emqx-merge.json" 2>/dev/null || true

# echo "Deployment triggered successfully!"