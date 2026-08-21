#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
RUNTIME="$ROOT/e2e/manual-thread/runtime"
BIN="$RUNTIME/bin/moltmesh-daemon"
CALC_CAP="a2a:v1:cap:calculator"
TEXT_CAP="a2a:v1:cap:text-generation"

agent_home() { printf '%s/%s/%s/home' "$RUNTIME" "$1" "$2"; }
agent_data() { printf '%s/.moltmesh' "$(agent_home "$1" "$2")"; }

mm() {
  local node=$1 agent=$2; shift 2
  HOME=$(agent_home "$node" "$agent") "$BIN" --json "$@" --data-dir "$(agent_data "$node" "$agent")"
}

identity_did() { mm "$1" "$2" get-identity | jq -r '.data.did'; }

wait_health() {
  local node=$1 agent=$2 deadline=$((SECONDS + 30))
  until mm "$node" "$agent" health >/dev/null 2>&1; do
    (( SECONDS < deadline )) || { echo "timeout waiting for $node/$agent" >&2; return 1; }
    sleep 0.2
  done
}

discover() {
  local node=$1 agent=$2 capability=$3 own_did=$4 output=$5 deadline=$((SECONDS + 60))
  mkdir -p "$(dirname "$output")"
  while (( SECONDS < deadline )); do
    local result
    result=$(mm "$node" "$agent" find-agents --capability "$capability" --limit 20 2>/dev/null || true)
    if jq -e --arg own "$own_did" '.data | map(select(.did != $own)) | length > 0' >/dev/null 2>&1 <<<"$result"; then
      jq --arg own "$own_did" '.data | map(select(.did != $own))[0]' <<<"$result" >"$output"
      return 0
    fi
    sleep 0.5
  done
  echo "discovery timeout for $capability from $node/$agent" >&2
  return 1
}

wait_thread() {
  local node=$1 agent=$2 thread_id=$3 deadline=$((SECONDS + 30))
  until mm "$node" "$agent" get-thread --id "$thread_id" >/dev/null 2>&1; do
    (( SECONDS < deadline )) || return 1
    sleep 0.25
  done
}

