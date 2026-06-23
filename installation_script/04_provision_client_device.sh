#!/usr/bin/env bash
set -e

# ==========================================================
# USAGE
# ==========================================================
usage() {
  echo "Usage: $0 --client-thing-name NAME --broker-host IP_ADDRESS"
  echo ""
  echo "Mandatory flags:"
  echo "  --client-thing-name NAME"
  echo "  --broker-host IP_ADDRESS"
  exit 1
}


# ==========================================================
# PARSE ARGUMENTS
# ==========================================================
while [[ $# -gt 0 ]]; do
  case $1 in
    --client-thing-name) CLIENT_THING_NAME="$2"; shift 2;;
    --broker-host) BROKER_HOST="$2"; shift 2;;
    *) echo "[ERROR] Unknown option: $1"; usage;;
  esac
done

# ==========================================================
# VALIDATE MANDATORY ARGUMENTS
# ==========================================================
if [[ -z "${CLIENT_THING_NAME:-}" || -z "${BROKER_HOST:-}" ]]; then
  echo "[ERROR] Missing mandatory arguments!"
  usage
fi


# ==========================================================
# CONFIG
# ==========================================================
CONFIG="/greengrass/v2/config/effectiveConfig.yaml"

REGION=$(grep 'awsRegion:' "$CONFIG" | awk '{print $2}' | tr -d '"')
CORE_THING_NAME=$(grep 'thingName:' "$CONFIG" | awk '{print $2}' | tr -d '"')

# MQTT broker endpoint to register in Greengrass
BROKER_PORT=443
CLIENT_CERT_DIR="./client_device_certs"

echo "Region       : $REGION"
echo "Core Thing   : $CORE_THING_NAME"
echo "Client Thing : $CLIENT_THING_NAME"

# ==========================================================
# CHECK IF CLIENT THING EXISTS
# ==========================================================
if ! aws iot describe-thing \
    --thing-name "$CORE_THING_NAME" \
    --region "$REGION" >/dev/null 2>&1; then
  echo "Thing '$CLIENT_THING_NAME' does not exist in AWS IoT."
  echo "Create it first using:"
  echo "aws iot create-thing --thing-name $CLIENT_THING_NAME --region $REGION"
  exit 1
fi

# ==========================================================
# GET CERT ARN ATTACHED TO CLIENT THING
# ==========================================================
THING_ARN=$(aws iot list-thing-principals \
  --thing-name "$CORE_THING_NAME" \
  --region "$REGION" \
  --query 'principals[0]' \
  --output text)

echo "Cert ARN: $THING_ARN"

# ==========================================================
# GET POLICY NAME FROM CERT
# ==========================================================
POLICIES=$(aws iot list-attached-policies \
  --target "$THING_ARN" \
  --region "$REGION")

CLIENT_POLICY_NAME=$(echo "$POLICIES" | jq -r '.policies[0].policyName')

echo "Client policy name: $CLIENT_POLICY_NAME"


# ==========================================================
# 1. CREATE CLIENT THING
# ==========================================================
echo "[1/7] Creating IoT Thing for client device..."
aws iot create-thing \
  --thing-name "$CLIENT_THING_NAME" \
  --region "$REGION" >/dev/null 2>&1 || echo "Thing already exists"

# ==========================================================
# 2. CREATE CERT FOR CLIENT DEVICE
# ==========================================================
echo "[2/7] Creating certificate..."
CERT_OUTPUT=$(aws iot create-keys-and-certificate \
  --set-as-active \
  --region "$REGION")

CERT_ARN=$(echo "$CERT_OUTPUT" | jq -r .certificateArn)
CERT_PEM=$(echo "$CERT_OUTPUT" | jq -r .certificatePem)
PRIV_KEY=$(echo "$CERT_OUTPUT" | jq -r .keyPair.PrivateKey)

mkdir -p "$CLIENT_CERT_DIR"

echo "$CERT_PEM"  > "$CLIENT_CERT_DIR/device.pem.crt"
echo "$PRIV_KEY" > "$CLIENT_CERT_DIR/private.pem.key"
chmod 600 "$CLIENT_CERT_DIR/private.pem.key"

curl -s https://www.amazontrust.com/repository/AmazonRootCA1.pem \
  -o "$CLIENT_CERT_DIR/AmazonRootCA1.pem"

# ==========================================================
# 3. ATTACH CERT TO CLIENT THING
# ==========================================================
echo "[3/7] Attaching cert to client Thing..."
aws iot attach-thing-principal \
  --thing-name "$CLIENT_THING_NAME" \
  --principal "$CERT_ARN" \
  --region "$REGION"

# ==========================================================
# 4. ENSURE IOT POLICY
# ==========================================================
echo "[4/7] Ensuring IoT policy exists..."

# POLICY_EXISTS=$(aws iot list-policies \
#   --region "$REGION" \
#   --query "policies[?policyName=='$CLIENT_POLICY_NAME'] | length(@)" \
#   --output text)

if aws iot get-policy \
    --policy-name "$CLIENT_POLICY_NAME" \
    --region "$REGION" \
    >/dev/null 2>&1; then
  echo "IoT policy already exists: $CLIENT_POLICY_NAME"

else
  echo "IoT policy does not exists: $CLIENT_POLICY_NAME"
  cat <<EOF > /tmp/client-device-policy.json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "iot:Connect",
        "iot:Publish",
        "iot:Subscribe",
        "iot:Receive"
      ],
      "Resource": "*"
    }
  ]
}
EOF

  aws iot create-policy \
    --policy-name "$CLIENT_POLICY_NAME" \
    --policy-document file:///tmp/client-device-policy.json \
    --region "$REGION"
fi

echo "[4.5/7] Attaching policy..."
aws iot attach-policy \
  --policy-name "$CLIENT_POLICY_NAME" \
  --target "$CERT_ARN" \
  --region "$REGION"

# ==========================================================
# 5. ASSOCIATE CLIENT DEVICE WITH CORE DEVICE
# ==========================================================
echo "[5/7] Associating client device with Greengrass Core..."

aws greengrassv2 batch-associate-client-device-with-core-device \
  --core-device-thing-name "$CORE_THING_NAME" \
  --entries "[{\"thingName\":\"$CLIENT_THING_NAME\"}]" \
  --region "$REGION"

# ==========================================================
# 6. UPDATE MQTT BROKER ENDPOINT (MANAGED ENDPOINTS)
# ==========================================================
echo "[6/7] Updating connectivity info..."

aws greengrassv2 update-connectivity-info \
  --thing-name "$CORE_THING_NAME" \
  --connectivity-info "[
    {
      \"id\": \"local-mqtt\",
      \"hostAddress\": \"$BROKER_HOST\",
      \"portNumber\": $BROKER_PORT
    }
  ]" \
  --region "$REGION"

# ==========================================================
# 7. VERIFY
# ==========================================================
echo "[7/7] Verifying setup..."

aws greengrassv2 list-client-devices-associated-with-core-device \
  --core-device-thing-name "$CORE_THING_NAME" \
  --region "$REGION"

aws greengrassv2 get-connectivity-info \
  --thing-name "$CORE_THING_NAME" \
  --region "$REGION"



# ==========================================================
# EXTRACT CA CERTIFICATE FROM SERVER (2nd CERT)
# ==========================================================
echo "Extracting CA certificate from $BROKER_HOST:$BROKER_PORT ..."

mkdir -p "$(dirname ""$CLIENT_CERT_DIR"")"

openssl s_client -showcerts -connect ${BROKER_HOST}:${BROKER_PORT} </dev/null 2>/dev/null \
| awk '
/BEGIN CERTIFICATE/ {n++}
n==2 {print}
n==2 && /END CERTIFICATE/ {exit}
' > "$CLIENT_CERT_DIR/RootCA.pem"

if [ ! -s "$CLIENT_CERT_DIR/RootCA.pem" ]; then
  echo "Failed to extract CA certificate"
  exit 1
fi

echo "CA certificate saved at: $CLIENT_CERT_DIR/RootCA.pem"


# Optional: verify the cert
openssl x509 -in "$CLIENT_CERT_DIR/RootCA.pem" -text -noout >/dev/null && \
echo "CA certificate verified successfully"



# # ==========================================================
# # Reinstall sudo-rs if it was present before
# # ==========================================================
# # Define the location of the temporary flag file
# STATE_FILE="/tmp/sudo_rs_was_installed"

# # Check if the flag file exists
# if [ -f "$STATE_FILE" ]; then
#     echo "State file found. Restoring sudo-rs to its original state..."
    
#     # Reinstall sudo-rs and remove classic sudo
#     apt-get update -y
#     apt-get install -y sudo-rs
#    # Bypass Ubuntu's safety check to remove classic sudo
#     export SUDO_FORCE_REMOVE=yes
#     apt-get remove -y sudo
    
#     # Delete the flag file so it doesn't trigger accidentally in the future
#     rm -f "$STATE_FILE"
    
#     echo "Successfully restored sudo-rs."
# else
#     echo "State file not found. sudo-rs was not present initially, skipping restoration."
# fi


echo ""
echo "Client device provisioned successfully!"
echo "Client Thing     : $CLIENT_THING_NAME"
echo "Certs stored at  : $CLIENT_CERT_DIR"
echo "Associated Core  : $CORE_THING_NAME"
echo "MQTT Broker      : $BROKER_HOST:$BROKER_PORT"
