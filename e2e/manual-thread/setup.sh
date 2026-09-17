#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }
mkdir -p "$RUNTIME/bin"
env GOCACHE="${GOCACHE:-/tmp/p2p_a2a-go-cache}" go build -o "$BIN" "$ROOT/cmd/moltmesh"

init_agent() {
  local node=$1 agent=$2 name=$3 caps=$4 port=$5 grpc=$6
  local home data
  home=$(agent_home "$node" "$agent"); data=$(agent_data "$node" "$agent")
  mkdir -p "$home"
  if [[ ! -f "$data/identity.json" ]]; then
    HOME="$home" "$BIN" init --data-dir "$data" --name "$name" --capabilities "$caps" --port "$port" --grpc-addr "$grpc" >/dev/null
  fi
}

# Two independent identities/daemons on simulated host node-a, one on node-b.
init_agent node-a textgen deterministic-textgen "$TEXT_CAP" 43101 127.0.0.1:53101
init_agent node-a observer deterministic-observer "a2a:v1:cap:observer" 43102 127.0.0.1:53102
init_agent node-b calculator deterministic-calculator "$CALC_CAP" 43201 127.0.0.1:53201
init_agent node-c recovery deterministic-recovery "a2a:v1:cap:thread-reader" 43301 127.0.0.1:53301
# Claude Code sits on the mesh as its own agent: own identity, own card,
# discoverable by capability like any other participant.
init_agent node-a claude claude-agent "$CLAUDE_CAP" 43103 127.0.0.1:53103
init_agent infrastructure bootstrap deterministic-bootstrap "a2a:v1:cap:bootstrap" 43000 127.0.0.1:53000

echo "Persistent identities and isolated HOME directories are ready under $RUNTIME"
