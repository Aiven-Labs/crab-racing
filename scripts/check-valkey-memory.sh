#!/usr/bin/env bash
#
# check-valkey-memory.sh — poll Prometheus for the crab-race-valkey low-memory
# condition and notify Signal (via Apprise) when it trips. Replaces Alertmanager:
# this server's Apprise field-remap doesn't work, but a plain `body=` POST does,
# so we query Prometheus ourselves and send a formed message.
#
#   Prometheus /api/v1/query  ->  (this script)  ->  Apprise /notify  ->  Signal
#
# Run it on an interval (cron every 5m, or the /loop skill). It debounces with a
# state file so a persistent low-memory condition notifies once per RENOTIFY
# window instead of every run, and sends a single "recovered" note when it clears.
#
# Secrets: SIGNAL_PHONE_NUMBER comes from fnox — never printed, never committed.
# Requires: fnox, curl, python3. Usage: ./scripts/check-valkey-memory.sh
set -euo pipefail

# Prometheus has no published port (see docker-compose.yml), so by default we
# query it from inside its own container. Set PROM_URL to hit a reachable
# Prometheus over HTTP instead (e.g. one running elsewhere).
PROM_URL="${PROM_URL:-}"
APPRISE_BASE="${APPRISE_BASE:-https://apprise.kjaymiller.dev}"
THRESHOLD="${THRESHOLD:-75}"                 # notify when available% <= this
RENOTIFY_SECS="${RENOTIFY_SECS:-10800}"      # re-notify at most every 3h while firing
STATE_FILE="${STATE_FILE:-/tmp/crab-valkey-mem.state}"
QUERY='mem_available_percent{job="aivenmetrics"}'

prom_query() {  # $1 = PromQL; prints the raw JSON response
  if [ -n "$PROM_URL" ]; then
    curl -s -m 10 --get "$PROM_URL/api/v1/query" --data-urlencode "query=$1"
  else
    local q
    q="$(python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.argv[1]))' "$1")"
    docker compose -f "$(dirname "$0")/../docker-compose.yml" exec -T prometheus \
      wget -q -T 10 -O - "http://localhost:9090/api/v1/query?query=$q"
  fi
}

for bin in fnox curl python3; do
  command -v "$bin" >/dev/null 2>&1 || { echo "ERROR: '$bin' not found on PATH" >&2; exit 1; }
done

# ── Query Prometheus for the current available-memory percentage ─────────────
avail="$(prom_query "$QUERY" \
  | python3 -c "import sys,json;r=json.load(sys.stdin)['data']['result'];print(r[0]['value'][1] if r else '')")"

if [ -z "$avail" ]; then
  echo "No data for $QUERY (is Prometheus up and scraping?)" >&2
  exit 1
fi

# Integer comparison via python (bash can't do floats).
firing="$(python3 -c "print('1' if float('$avail') <= float('$THRESHOLD') else '0')")"
now="$(python3 -c 'import time;print(int(time.time()))')"

# ── Read previous state (firing flag + last-notified epoch) ──────────────────
prev_firing=0; last_notified=0
if [ -f "$STATE_FILE" ]; then
  read -r prev_firing last_notified < "$STATE_FILE" 2>/dev/null || true
fi

notify() {  # $1=title  $2=body  $3=type(info|warning|success)
  local sig
  sig="$(SIGNAL_PHONE_NUMBER="$(fnox get SIGNAL_PHONE_NUMBER)" python3 -c \
    'import os;p=os.environ["SIGNAL_PHONE_NUMBER"].strip();print(f"signal://signal-cli:8080/{p}/{p}")')"
  curl -s -m 20 -X POST "$APPRISE_BASE/notify" \
    --data-urlencode "urls=$sig" \
    --data-urlencode "title=$1" \
    --data-urlencode "body=$2" \
    --data-urlencode "type=${3:-warning}" \
    -o /dev/null -w '%{http_code}'
}

msg=""
if [ "$firing" = "1" ]; then
  # Fire if newly-firing OR the renotify window has elapsed.
  if [ "$prev_firing" != "1" ] || [ $(( now - last_notified )) -ge "$RENOTIFY_SECS" ]; then
    pct="$(python3 -c "print(f'{float(\"$avail\"):.1f}')")"
    code="$(notify "crab-race-valkey low memory" \
      "Available memory is ${pct}% (threshold ${THRESHOLD}%). Check key growth or plan size." \
      warning)"
    echo "FIRING: ${pct}% available -> Signal notify HTTP $code"
    printf '%s %s\n' 1 "$now" > "$STATE_FILE"
  else
    echo "FIRING but within renotify window; staying quiet ($avail% available)"
    printf '%s %s\n' 1 "$last_notified" > "$STATE_FILE"
  fi
else
  # Recovered: send one "resolved" note if we were previously firing.
  if [ "$prev_firing" = "1" ]; then
    pct="$(python3 -c "print(f'{float(\"$avail\"):.1f}')")"
    code="$(notify "crab-race-valkey recovered" \
      "Available memory back to ${pct}% (above ${THRESHOLD}% threshold)." success)"
    echo "RECOVERED: ${pct}% available -> Signal notify HTTP $code"
  else
    echo "OK: ${avail}% available (>${THRESHOLD}%)"
  fi
  printf '%s %s\n' 0 0 > "$STATE_FILE"
fi
