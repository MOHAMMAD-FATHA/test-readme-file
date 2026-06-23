# ==============================================================================
# FILE: install_greengrass.sh
#
# DESCRIPTION:
# This is the master provisioning and infrastructure setup script for the edge 
# device. It installs prerequisites, provisions the AWS IoT Things, configures 
# AWS Greengrass V2, and sets up the local client device environment for MQTT.
#
# HOW IT WORKS:
# 1. Installs system prerequisites (Java, Python, AWS CLI, Docker) and handles 
#    the sudo-rs swap if necessary.
# 2. Provisions the Core IoT Thing in AWS, generates/attaches certificates, 
#    and applies the required IoT policies.
# 3. Downloads, configures, and installs the AWS Greengrass Nucleus software.
# 4. Initializes the AWS IoT Device Shadow ("config") with a default JSON state.
# 5. Provisions a local Client IoT Thing, generates its certificates, and 
#    associates it with the Greengrass Core.
# 6. Updates the Core's local connectivity info and attempts to extract the 
#    local MQTT broker (EMQX) CA certificate for client testing.
# ==============================================================================

#!/usr/bin/env bash
set -euo pipefail


# =====================================================
# USAGE
# =====================================================
usage() {
  echo "Usage: $0 --region <region> --thing-name <core_name> --policy-name <policy> --role-alias <alias> --client-thing-name <client_name> --broker-host <ip> --sub-topic <topic>"
  echo ""
  echo "Example:"
  echo "  $0 --region us-east-2 \\"
  echo "     --thing-name mct-dev-greengrass-core \\"
  echo "     --policy-name mct-dev-iot-thing-policy \\"
  echo "     --role-alias GreengrassTESCertificatePolicydnadct-mct-greengrass-edge-device-role-alias \\"
  # echo "     --broker-host 192.168.1.100 \\"
  echo "     --sub-topic \"oa/us/dna/dttp/dttp/+/+/+\""
  exit 1
}

# =====================================================
# PARSE ARGUMENTS
# =====================================================
while [[ $# -gt 0 ]]; do
  case "$1" in
    --region) REGION="$2"; shift 2 ;;
    --thing-name) THING_NAME="$2"; shift 2 ;;
    --policy-name) POLICY_NAME="$2"; shift 2 ;;
    --role-alias) ROLE_ALIAS="$2"; shift 2 ;;
    # --client-thing-name) CLIENT_THING_NAME="$2"; shift 2 ;;
    # --broker-host) BROKER_HOST="$2"; shift 2 ;;
    --sub-topic) SUB_TOPIC="$2"; shift 2 ;;
    *) echo "[ERROR] Unknown option: $1"; usage ;;
  esac
done

if [[ -z "${REGION:-}" || -z "${THING_NAME:-}" || -z "${POLICY_NAME:-}" || -z "${ROLE_ALIAS:-}" || -z "${SUB_TOPIC:-}" ]]; then
  echo "[ERROR] Missing required arguments"
  usage
fi


# ==========================================================
# CONFIGURATION
# ==========================================================
GG_VERSION="2.13.0"
INSTALL_DIR="/greengrass/v2"
GG_USER="ggc_user"
GG_GROUP="ggc_group"
CERTS_DIR="$INSTALL_DIR/certs"
CONFIG_DIR="$INSTALL_DIR/config"

BROKER_PORT=443
# CLIENT_CERT_DIR="./client_device_certs"
CLIENT_POLICY_NAME="mct-dev-client-device-policy"
SHADOW_NAME="config"

echo "=========================================="
echo "Region       : $REGION"
echo "Core Thing   : $THING_NAME"
# echo "Client Thing : $CLIENT_THING_NAME"
# echo "Broker Host  : $BROKER_HOST:$BROKER_PORT"
echo "=========================================="

# ==========================================================
# 1. PREREQUISITES
# ==========================================================
echo "[1/10] Installing prerequisites..."
apt-get update -y
apt-get install -y openjdk-11-jdk python3 python3-pip curl jq zip unzip python3-paho-mqtt python3-tz


if ! command -v aws >/dev/null 2>&1; then
  echo 'Installing aws cli'
  snap install aws-cli --classic
fi

STATE_FILE="/tmp/sudo_rs_was_installed"
if dpkg -l | grep -qw "sudo-rs"; then
    echo "Detected sudo-rs. Swapping with classic sudo..."
    touch "$STATE_FILE"
    apt-get update -y && apt-get install -y sudo && apt-get remove -y sudo-rs
else
    rm -f "$STATE_FILE"
fi

if ! command -v docker >/dev/null 2>&1; then
    echo "Installing Docker..."
    apt-get update -y && apt-get install -y apt-transport-https ca-certificates curl software-properties-common
    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
    chmod a+r /etc/apt/keyrings/docker.asc
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | tee /etc/apt/sources.list.d/docker.list > /dev/null
    apt-get update -y && apt-get install -y docker-ce docker-ce-cli containerd.io
fi

# ==========================================================
# 2. PROVISION CORE THING & CERTS
# ==========================================================
echo "[2/10] Creating Core IoT Thing: $THING_NAME..."
aws iot create-thing --thing-name "$THING_NAME" --region "$REGION" || true

CERT_OUTPUT=$(aws iot create-keys-and-certificate --set-as-active --region "$REGION")
CERT_ARN=$(echo "$CERT_OUTPUT" | jq -r .certificateArn)

mkdir -p "$CERTS_DIR"
echo "$CERT_OUTPUT" | jq -r .certificatePem > "$CERTS_DIR/device.pem.crt"
echo "$CERT_OUTPUT" | jq -r .keyPair.PrivateKey > "$CERTS_DIR/private.pem.key"
chmod 600 "$CERTS_DIR/private.pem.key"
curl -s https://www.amazontrust.com/repository/AmazonRootCA1.pem -o "$CERTS_DIR/AmazonRootCA1.pem"

aws iot attach-thing-principal --thing-name "$THING_NAME" --principal "$CERT_ARN" --region "$REGION"
aws iot attach-policy --policy-name "$POLICY_NAME" --target "$CERT_ARN" --region "$REGION"

# ==========================================================
# 3. INSTALL GREENGRASS CORE
# ==========================================================
echo "[3/10] Installing Greengrass Core..."
IOT_DATA_ENDPOINT=$(aws iot describe-endpoint --endpoint-type iot:Data-ATS --region "$REGION" --query endpointAddress --output text)
IOT_CRED_ENDPOINT=$(aws iot describe-endpoint --endpoint-type iot:CredentialProvider --region "$REGION" --query endpointAddress --output text)

mkdir -p "$CONFIG_DIR"
cat <<EOF > "$CONFIG_DIR/config.yaml"
system:
  certificateFilePath: "${CERTS_DIR}/device.pem.crt"
  privateKeyPath: "${CERTS_DIR}/private.pem.key"
  rootCaPath: "${CERTS_DIR}/AmazonRootCA1.pem"
  rootpath: "${INSTALL_DIR}"
  thingName: "${THING_NAME}"
services:
  aws.greengrass.Nucleus:
    componentType: "NUCLEUS"
    version: "${GG_VERSION}"
    configuration:
      awsRegion: "${REGION}"
      iotRoleAlias: "${ROLE_ALIAS}"
      iotDataEndpoint: "${IOT_DATA_ENDPOINT}"
      iotCredEndpoint: "${IOT_CRED_ENDPOINT}"
      mqtt: { port: 443 }
      greengrassDataPlanePort: 443
      interpolateComponentConfiguration: true
EOF

id -u $GG_USER >/dev/null 2>&1 || useradd --system --create-home $GG_USER
getent group $GG_GROUP >/dev/null 2>&1 || groupadd --system $GG_GROUP

mkdir -p /tmp/gg-install && cd /tmp/gg-install
curl -s https://d2s8p88vqu9w66.cloudfront.net/releases/greengrass-${GG_VERSION}.zip -o greengrass.zip
unzip -q greengrass.zip

java -Droot="$INSTALL_DIR" -Dlog.store=FILE -jar lib/Greengrass.jar \
  --init-config "$CONFIG_DIR/config.yaml" --component-default-user "$GG_USER:$GG_GROUP" --setup-system-service true

systemctl daemon-reload && systemctl enable greengrass.service && systemctl restart greengrass.service

mkdir -p /var/greengrass/streams && chown -R $GG_USER:$GG_GROUP /var/greengrass/streams && chmod 700 /var/greengrass/streams

# ==========================================================
# 4. INITIALIZE DEVICE SHADOW
# ==========================================================
echo "[4/10] Initializing Configuration Shadow..."
cat <<EOF > /tmp/shadow_payload.json
{ "state": { "desired": { "zones": {}, "shiftTemplates": { "Shift 2": {}, "Shift 3": {} } } } }
EOF

aws iot-data update-thing-shadow --thing-name "$THING_NAME" --shadow-name "$SHADOW_NAME" \
  --cli-binary-format raw-in-base64-out --payload file:///tmp/shadow_payload.json --region "$REGION" "/tmp/shadow_response.json"
rm -f /tmp/shadow_payload.json

# # ==========================================================
# # 5. PROVISION CLIENT DEVICE
# # ==========================================================
# echo "[5/10] Creating Client IoT Thing: $CLIENT_THING_NAME..."
# aws iot create-thing --thing-name "$CLIENT_THING_NAME" --region "$REGION" || true

# echo "[6/10] Generating Client Certificates..."
# CLIENT_CERT_OUTPUT=$(aws iot create-keys-and-certificate --set-as-active --region "$REGION")
# CLIENT_CERT_ARN=$(echo "$CLIENT_CERT_OUTPUT" | jq -r .certificateArn)

# mkdir -p "$CLIENT_CERT_DIR"
# echo "$CLIENT_CERT_OUTPUT" | jq -r .certificatePem > "$CLIENT_CERT_DIR/device.pem.crt"
# echo "$CLIENT_CERT_OUTPUT" | jq -r .keyPair.PrivateKey > "$CLIENT_CERT_DIR/private.pem.key"
# chmod 600 "$CLIENT_CERT_DIR/private.pem.key"
# curl -s https://www.amazontrust.com/repository/AmazonRootCA1.pem -o "$CLIENT_CERT_DIR/AmazonRootCA1.pem"

# aws iot attach-thing-principal --thing-name "$CLIENT_THING_NAME" --principal "$CLIENT_CERT_ARN" --region "$REGION"

# echo "[7/10] Ensuring Client IoT Policy exists..."
# if ! aws iot get-policy --policy-name "$CLIENT_POLICY_NAME" --region "$REGION" >/dev/null 2>&1; then
#   cat <<EOF > /tmp/client-device-policy.json
# { "Version": "2012-10-17", "Statement": [ { "Effect": "Allow", "Action": [ "iot:Connect", "iot:Publish", "iot:Subscribe", "iot:Receive" ], "Resource": "*" } ] }
# EOF
#   aws iot create-policy --policy-name "$CLIENT_POLICY_NAME" --policy-document file:///tmp/client-device-policy.json --region "$REGION"
# fi

# aws iot attach-policy --policy-name "$CLIENT_POLICY_NAME" --target "$CLIENT_CERT_ARN" --region "$REGION"

# # ==========================================================
# # 6. ASSOCIATE CLIENT & UPDATE CONNECTIVITY
# # ==========================================================
# echo "[8/10] Associating client device with Core..."
# aws greengrassv2 batch-associate-client-device-with-core-device \
#   --core-device-thing-name "$THING_NAME" --entries "[{\"thingName\":\"$CLIENT_THING_NAME\"}]" --region "$REGION"

# echo "[9/10] Updating Core Connectivity Info..."
# aws greengrassv2 update-connectivity-info \
#   --thing-name "$THING_NAME" --connectivity-info "[{\"id\":\"local-mqtt\",\"hostAddress\":\"$BROKER_HOST\",\"portNumber\":$BROKER_PORT}]" --region "$REGION"

# # ==========================================================
# # 7. EXTRACT BROKER CA CERTIFICATE
# # ==========================================================
# echo "[10/10] Extracting CA certificate from $BROKER_HOST:$BROKER_PORT..."
# # Wait a moment to ensure broker is listening if deployed alongside Nucleus
# sleep 10 

# openssl s_client -showcerts -connect ${BROKER_HOST}:${BROKER_PORT} </dev/null 2>/dev/null \
# | awk '/BEGIN CERTIFICATE/ {n++} n==2 {print} n==2 && /END CERTIFICATE/ {exit}' > "$CLIENT_CERT_DIR/RootCA.pem" || true

# if [ -s "$CLIENT_CERT_DIR/RootCA.pem" ]; then
#   echo "CA certificate successfully saved at: $CLIENT_CERT_DIR/RootCA.pem"
# else
#   echo "WARNING: Failed to extract CA certificate. The broker may not be running yet."
# fi


# ==========================================================
# 8. DEPLOY FOUNDATIONAL GATEWAY COMPONENTS
# ==========================================================
echo "[11/11] Deploying foundational gateway components (Auth, Bridge, CLI)..."

DEPLOYMENT_FILE="/tmp/gateway-foundation-deployment.json"
THING_ARN=$(aws iot describe-thing --thing-name "$THING_NAME" --region "$REGION" --query thingArn --output text)
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

# We use the variables already established earlier in the install script!
cat <<EOF > "$DEPLOYMENT_FILE"
{
  "targetArn": "$THING_ARN",
  "deploymentName": "GatewayGreengrassCoreDevice-deployment",
  "components": {
    "aws.greengrass.Nucleus": {
      "componentVersion": "${GG_VERSION}",
      "configurationUpdate": {
        "merge": "{\"iotRoleAlias\":\"$ROLE_ALIAS\",\"iotDataEndpoint\":\"$IOT_DATA_ENDPOINT\",\"iotCredEndpoint\":\"$IOT_CRED_ENDPOINT\",\"mqtt\":{\"port\":443},\"greengrassDataPlanePort\":443,\"interpolateComponentConfiguration\":true}"
      }
    },
    "aws.greengrass.Cli": {
      "componentVersion": "2.15.0"
    },
    "aws.greengrass.clientdevices.Auth": {
      "componentVersion": "2.5.4",
      "configurationUpdate": {
        "merge": "{\"deviceGroups\":{\"formatVersion\":\"2021-03-05\",\"definitions\":{\"MyPermissiveDeviceGroup\":{\"selectionRule\":\"thingName: *\",\"policyName\":\"MyPermissivePolicy\"}},\"policies\":{\"MyPermissivePolicy\":{\"AllowAll\":{\"statementDescription\":\"Allow client devices to perform all actions.\",\"operations\":[\"*\"],\"resources\":[\"*\"]}}}}}"
      }
    },
    "aws.greengrass.clientdevices.mqtt.Bridge": {
      "componentVersion": "2.3.2",
      "configurationUpdate": {
        "merge": "{\"mqttTopicMapping\":{\"MQTTFetcDataEvent\":{\"topic\":\"$SUB_TOPIC\",\"source\":\"LocalMqtt\",\"target\":\"IotCore\"},\"toLocal\":{\"topic\":\"*\",\"source\":\"IotCore\",\"target\":\"LocalMqtt\"}},\"brokerUri\":\"ssl://localhost:443\"}"
      }
    }
  }
}
EOF

aws greengrassv2 create-deployment \
  --region "$REGION" \
  --target-arn "arn:aws:iot:$REGION:$AWS_ACCOUNT_ID:thing/$THING_NAME" \
  --deployment-name "GatewayGreengrassCoreDevice-deployment" \
  --cli-input-json file://"$DEPLOYMENT_FILE"

rm -f "$DEPLOYMENT_FILE"
echo "Foundational gateway deployment triggered successfully!"


# ==========================================================
# FINISH
# ==========================================================
echo "======================================================"
echo "Provisioning Complete!"
echo "Core Device   : $THING_NAME"
# echo "Client Device : $CLIENT_THING_NAME"
# echo "Client Certs  : $(realpath $CLIENT_CERT_DIR)"
echo "Core Logs     : sudo tail -f /greengrass/v2/logs/greengrass.log"
echo "======================================================"