#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
# A failed/interrupted prior run can leave a daemon alive after its pidfile was
# removed.  Stop harness-owned processes before resetting their databases; a
# live daemon against deleted state makes the next run appear healthy while it
# is actually serving stale identities and streams.
"$(dirname "$0")/stop.sh"
"$(dirname "$0")/setup.sh"

# Each invocation is a fresh scenario. Keep the deterministic identity and
# configuration, but discard state left by a previous successful/failed run;
# otherwise old durable outbox retries re-invite old threads and make the
# replication assertion nondeterministic.
reset_agent_state() {
  local node=$1 agent=$2 data
  data=$(agent_data "$node" "$agent")
  rm -f "$data"/{inbox,outbox,tasks,threads,networks}.db \
    "$data"/{inbox,outbox,tasks,threads,networks}.db-wal \
    "$data"/{inbox,outbox,tasks,threads,networks}.db-shm \
    "$data"/manual.log "$data"/grpc-addr "$data"/daemon.pid "$data"/manual.pid
  rm -rf "$data/blocks"
}
for spec in "node-a textgen" "node-a observer" "node-b calculator" "node-c recovery" "node-a claude" "infrastructure bootstrap"; do
  read -r node agent <<<"$spec"
  reset_agent_state "$node" "$agent"
done

start_agent() {
  local node=$1 agent=$2 home data pidfile
  home=$(agent_home "$node" "$agent"); data=$(agent_data "$node" "$agent"); pidfile="$data/manual.pid"
  if [[ -f "$pidfile" ]] && kill -0 "$(<"$pidfile")" 2>/dev/null; then return; fi
  HOME="$home" "$BIN" start --config "$data/moltbook.toml" >"$data/manual.log" 2>&1 &
  echo $! >"$pidfile"
  wait_health "$node" "$agent"
}

start_agent infrastructure bootstrap
bootstrap_addr=$(mm infrastructure bootstrap get-identity | jq -r '(.data // .).multiaddrs[] | select(startswith("/ip4/127.0.0.1/tcp/"))' | head -n 1)

set_bootstrap() {
  local node=$1 agent=$2 cfg tmp
  cfg="$(agent_data "$node" "$agent")/moltbook.toml"; tmp="$cfg.tmp"
  awk -v addr="$bootstrap_addr" '
    /^ipfs_bootstrap[[:space:]]*=/ || /^bootstrap_peers[[:space:]]*=/ { next }
    /^port[[:space:]]*=/ { print; print "ipfs_bootstrap = false"; print "bootstrap_peers = [\"" addr "\"]"; next }
    { print }
  ' "$cfg" >"$tmp"
  mv "$tmp" "$cfg"
}
set_bootstrap node-a textgen
set_bootstrap node-a observer
set_bootstrap node-b calculator
set_bootstrap node-c recovery
set_bootstrap node-a claude

start_agent node-a textgen
start_agent node-a observer
start_agent node-b calculator
start_agent node-a claude
echo "Four independent daemon identities are running; they know only the neutral bootstrap service, not one another."
