#!/usr/bin/env bash
#
# setup-prometheus.sh — provision Prometheus basic-auth credentials for the
# Aiven "crab-race-valkey" → Prometheus metrics integration.
#
# What it does, every run:
#   1. Generates a fresh random basic-auth username + password.
#   2. Rotates them onto the Aiven Prometheus integration endpoint.
#   3. Stores them (encrypted) in fnox so nothing sensitive lands in shell
#      history, the repo, or CI logs.
#   4. Resolves the Valkey scrape target (host/port) + downloads the project CA.
#   5. Renders prometheus/prometheus.yml from prometheus/prometheus.yml.tmpl,
#      injecting the credentials via ${...} substitution at render time.
#
# The generated prometheus.yml and CA (prometheus/ca.pem) are gitignored — they
# are build artifacts, regenerated on demand. Re-run this whenever you need to
# rotate credentials or stand the stack up on a new machine.
#
# Requires: avn (Aiven CLI, authenticated), fnox, python3, openssl.
# Usage:    ./scripts/setup-prometheus.sh
set -euo pipefail

# ── Config (override via env) ───────────────────────────────────────────────
PROJECT="${AIVEN_PROJECT:-jay-miller}"
VALKEY_SERVICE="${AIVEN_VALKEY_SERVICE:-crab-race-valkey}"
ENDPOINT_NAME="${AIVEN_PROMETHEUS_ENDPOINT:-crab-race-prometheus}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROM_DIR="$REPO_ROOT/prometheus"
TMPL="$PROM_DIR/prometheus.yml.tmpl"
OUT="$PROM_DIR/prometheus.yml"
CA_OUT="$PROM_DIR/ca.pem"

# The Aiven CLI may be a real binary on PATH or (as in this dev setup) only
# reachable via uvx. Resolve it once into $AVN.
if command -v avn >/dev/null 2>&1; then
  AVN=(avn)
elif command -v uvx >/dev/null 2>&1; then
  AVN=(uvx --from aiven-client avn)
else
  echo "ERROR: neither 'avn' nor 'uvx' found on PATH" >&2; exit 1
fi

for bin in fnox python3 openssl; do
  command -v "$bin" >/dev/null 2>&1 || { echo "ERROR: '$bin' not found on PATH" >&2; exit 1; }
done

# ── 1. Generate fresh random credentials ────────────────────────────────────
# Username must match Aiven's ^[a-z0-9\-@_]{5,32}$ ; password 8-256 chars.
USERNAME="cprom$(openssl rand -hex 6)"                       # e.g. cprom1a2b3c4d5e6f
PASSWORD="$(openssl rand -base64 24 | tr -d '/+=' | cut -c1-24)"
echo "Generated new Prometheus basic-auth credentials (username: $USERNAME)"

# ── 2. Rotate them onto the Aiven Prometheus endpoint ───────────────────────
# The endpoint JSON gives us both its id and the scrape port (client_port).
read -r ENDPOINT_ID SCRAPE_PORT < <("${AVN[@]}" service integration-endpoint-list --project "$PROJECT" --json \
  | python3 -c "import sys,json;e=next(e for e in json.load(sys.stdin) if e['endpoint_name']=='$ENDPOINT_NAME');print(e['endpoint_id'],e['endpoint_config']['client_port'])")

"${AVN[@]}" service integration-endpoint-update --project "$PROJECT" "$ENDPOINT_ID" \
  --user-config-json "{\"basic_auth_username\":\"$USERNAME\",\"basic_auth_password\":\"$PASSWORD\"}" >/dev/null
echo "Rotated credentials on Aiven endpoint '$ENDPOINT_NAME'"

# ── 3. Store in fnox (encrypted, gitignored fnox.toml) ──────────────────────
# NOTE: use the positional-value form, not stdin. `... | fnox set KEY` writes the
# entry but silently drops the encrypted value (fnox 1.25.1), leaving an
# unreadable secret. The arg form persists correctly.
fnox set PROMETHEUS_BASIC_AUTH_USERNAME "$USERNAME" -d "crab-race-valkey Prometheus scrape username"
fnox set PROMETHEUS_BASIC_AUTH_PASSWORD "$PASSWORD" -d "crab-race-valkey Prometheus scrape password"
echo "Stored credentials in fnox"

# ── 4. Resolve scrape host + download project CA ────────────────────────────
# Port already came from the endpoint above; the host lives on the service's
# prometheus component (only present once the integration is active).
SCRAPE_HOST="$("${AVN[@]}" service get --project "$PROJECT" "$VALKEY_SERVICE" --json \
  | python3 -c "import sys,json;print(next(c['host'] for c in json.load(sys.stdin)['components'] if c['component']=='prometheus'))")"
echo "Scrape target: $SCRAPE_HOST:$SCRAPE_PORT"

"${AVN[@]}" project ca-get --project "$PROJECT" --target-filepath "$CA_OUT"
echo "Wrote project CA to $CA_OUT"

# ── 5. Render prometheus.yml from template ──────────────────────────────────
PROMETHEUS_BASIC_AUTH_USERNAME="$USERNAME" \
PROMETHEUS_BASIC_AUTH_PASSWORD="$PASSWORD" \
PROMETHEUS_SCRAPE_HOST="$SCRAPE_HOST" \
PROMETHEUS_SCRAPE_PORT="$SCRAPE_PORT" \
  python3 - "$TMPL" "$OUT" <<'PY'
import os, sys, string
tmpl, out = sys.argv[1], sys.argv[2]
with open(tmpl) as f:
    rendered = string.Template(f.read()).safe_substitute(os.environ)
with open(out, "w") as f:
    f.write(rendered)
PY
echo "Rendered $OUT"
echo
echo "Done. Start the stack with:  docker compose up -d prometheus"
