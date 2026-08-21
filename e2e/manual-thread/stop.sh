#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

stop_pid() {
  local pid=$1
  kill "$pid" 2>/dev/null || true
  for _ in {1..30}; do kill -0 "$pid" 2>/dev/null || return 0; sleep 0.1; done
  # The daemon owns libp2p listeners.  Do not start a replacement while a
  # previous harness daemon still owns its fixed port.
  kill -KILL "$pid" 2>/dev/null || true
}

for spec in "node-a textgen" "node-a observer" "node-b calculator" "node-c recovery" "infrastructure bootstrap"; do
  read -r node agent <<<"$spec"
  data=$(agent_data "$node" "$agent")
  pidfile="$data/manual.pid"
  if [[ -f "$pidfile" ]]; then
    pid=$(<"$pidfile")
    stop_pid "$pid"
    rm -f "$pidfile"
  fi

  # pidfiles are deliberately cleared by a fresh-start reset.  Still find a
  # daemon that is explicitly using this harness configuration, so an
  # interrupted run cannot survive and race the next scenario.
  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    stop_pid "$pid"
  done < <(pgrep -f "$data/moltbook.toml" 2>/dev/null || true)
done
