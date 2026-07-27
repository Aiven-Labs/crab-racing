#!/usr/bin/env bash
# Fetches Kafka and Valkey credentials from Aiven and stores them in fnox.toml.
# Run this after rotating keys or to bootstrap a fresh checkout.
set -euo pipefail

PROJECT="jay-miller"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(dirname "$SCRIPT_DIR")"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# Shell alias won't expand in scripts; wrap it as a function instead.
avn() { uvx --from aiven-client avn "$@"; }

pem_inline() {
  # Collapse a PEM file to a single line with literal \n so server.js readPem() works.
  local content
  content="$(cat "$1")"
  [[ -n "$content" ]] || { echo "ERROR: $1 is empty" >&2; exit 1; }
  printf '%s' "$content" | tr '\n' '|' | sed 's/|/\\n/g'
}

fnox_set() {
  local key="$1" value="$2"
  [[ -n "$value" ]] || { echo "ERROR: value for $key is empty" >&2; exit 1; }
  printf '%s\n' "$value" | fnox set --config "$REPO_ROOT/fnox.toml" "$key"
  echo "  set $key"
}

echo "Fetching Kafka certs…"
avn service user-creds-download crab-race-kafka \
  --project "$PROJECT" --username avnadmin -d "$TMP_DIR/kafka"

echo "Fetching project CA (Valkey)…"
avn project ca-get --project "$PROJECT" --target-filepath "$TMP_DIR/valkey-ca.pem"

echo "Fetching Valkey URI…"
VALKEY_URI="$(avn service get crab-race-valkey --project "$PROJECT" --json \
  | python3 -c "import json,sys; print(json.load(sys.stdin)['service_uri'])")"
[[ -n "$VALKEY_URI" ]] || { echo "ERROR: could not fetch Valkey URI" >&2; exit 1; }

echo "Fetching Kafka broker…"
KAFKA_BROKER="$(avn service get crab-race-kafka --project "$PROJECT" --json \
  | python3 -c "
import json,sys
p = json.load(sys.stdin)['service_uri_params']
print(p['host'] + ':' + p['port'])
")"
[[ -n "$KAFKA_BROKER" ]] || { echo "ERROR: could not fetch Kafka broker" >&2; exit 1; }

echo "Storing secrets in fnox…"
fnox_set KAFKA_BROKER              "$KAFKA_BROKER"
fnox_set KAFKA_CA_CERT             "$(pem_inline "$TMP_DIR/kafka/ca.pem")"
fnox_set KAFKA_ACCESS_CERT         "$(pem_inline "$TMP_DIR/kafka/service.cert")"
fnox_set KAFKA_ACCESS_KEY          "$(pem_inline "$TMP_DIR/kafka/service.key")"
fnox_set VALKEY_URI                "$VALKEY_URI"
fnox_set VALKEY_CA_CERT            "$(pem_inline "$TMP_DIR/valkey-ca.pem")"
fnox_set PORT                      "3000"
fnox_set KAFKA_SECURITY_PROTOCOL   "SSL"

echo ""
echo "Done. Set PUBLIC_WS_URL once the new App Platform service is created:"
echo "  fnox set PUBLIC_WS_URL <wss://your-service-3000.aiven.app>"
