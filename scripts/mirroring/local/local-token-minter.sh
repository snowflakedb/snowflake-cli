#!/usr/bin/env bash
set -e

_SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
ENV_FILE="${_SCRIPT_DIR}/.env"
if [ -f ${ENV_FILE} ]; then
  set -a
  source ${ENV_FILE}
  set +a
else
  echo "Error: .env file not found!"
  exit 1
fi

if [ -z "$MIRRORING_APP_ID" ]; then
  echo "Error: Missing required variables \$MIRRORING_APP_ID"
  exit 1
fi
if [ -z "$MIRRORING_APP_INSTALLATION_ID" ]; then
  echo "Error: Missing required variables \$MIRRORING_APP_INSTALLATION_ID"
  exit 1
fi
if [ -z "$MIRRORING_APP_PRIVATE_KEY" ]; then
  echo "Error: Missing required variables \$MIRRORING_APP_PRIVATE_KEY"
  exit 1
fi

base64url() {
  openssl base64 -e | tr -d '=' | tr '/+' '_-' | tr -d '\n'
}

echo "Generating JWT..." >&2

header=$(echo -n '{"typ":"JWT","alg":"RS256"}' | base64url)

now=$(date +%s)
iat=$((now - 60))    # 1 minute ago to handle potential clock drift
exp=$((now + 600))   # Expires in 10 minutes
payload=$(echo -n "{\"iat\":${iat},\"exp\":${exp},\"iss\":\"${MIRRORING_APP_ID}\"}" | base64url)

header_payload="${header}.${payload}"

# printf '%b' expands the '\n' characters back into actual newlines securely in-memory
signature=$(echo -n "${header_payload}" | openssl dgst -sha256 -sign <(printf '%b' "$MIRRORING_APP_PRIVATE_KEY") | base64url)
JWT="${header_payload}.${signature}"

echo "Exchanging JWT for Installation Access Token via GitHub CLI..." >&2

MINTED_TOKEN=$(echo '{"repositories":["snowflake-cli"]}' | gh api \
  --method POST \
  -H "Authorization: Bearer $JWT" \
  -H "Accept: application/vnd.github+json" \
  --input - \
  "/app/installations/${MIRRORING_APP_INSTALLATION_ID}/access_tokens" \
  --jq '.token')

echo $MINTED_TOKEN
