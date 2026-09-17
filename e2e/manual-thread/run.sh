#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
trap '"$(dirname "$0")/stop.sh"' EXIT
log() { printf '[manual-thread] %s\n' "$*"; }

log "starting isolated daemon network"
"$(dirname "$0")/start.sh"

text_did=$(identity_did node-a textgen)
observer_did=$(identity_did node-a observer)
calc_did=$(identity_did node-b calculator)

# Every identity discovers independently and persists what it learned.
log "discovering calculator and text-generation capabilities"
discover node-a textgen "$CALC_CAP" "$text_did" "$(agent_home node-a textgen)/discovered/calculator.json"
discover node-a observer "$TEXT_CAP" "$observer_did" "$(agent_home node-a observer)/discovered/textgen.json"
discover node-a observer "$CALC_CAP" "$observer_did" "$(agent_home node-a observer)/discovered/calculator.json"

thread_json=$(mm node-a textgen create-thread --f 0 --with-recovery)
thread_id=$(jq -r '(.data // .) | (.thread // .) | .id' <<<"$thread_json")
recovery_secret=$(jq -r '(.data // .) | .recovery_handle.recovery_secret' <<<"$thread_json")
[[ "$thread_id" != "null" && "$recovery_secret" != "null" ]] || { echo "missing recovery handle" >&2; exit 1; }
printf '%s\n' "$thread_id" >"$RUNTIME/thread-id"
mm node-a textgen append-entry --thread-id "$thread_id" --kind message --payload "thread-online" >/dev/null
log "created encrypted recoverable thread $thread_id"

# Late additions are committed observers, then invited through the durable outbox.
mm node-a textgen add-thread-replica --thread-id "$thread_id" --did "$calc_did" >/dev/null
mm node-a textgen add-thread-replica --thread-id "$thread_id" --did "$observer_did" >/dev/null
wait_thread node-b calculator "$thread_id"
wait_thread node-a observer "$thread_id"
log "calculator and observer joined the committed thread"
# The wake confirms the actors subscribed; give GossipSub a short mesh
# formation window before committing the task lifecycle block.  A late node
# still has the capability-gated archive recovery path, but this scenario is
# specifically asserting live observer replication.
sleep 3

"$(dirname "$0")/calculator-agent.sh" >"$RUNTIME/calculator-result" &
calculator_pid=$!
log "submitting calculator task"
task_json=$(mm node-a textgen create-task --to "$calc_did" --skill "$CALC_CAP" --thread-id "$thread_id" --input "add 2 + 2.")
task_id=$(jq -r '(.data // .).id' <<<"$task_json")
wait "$calculator_pid"

deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
  task=$(mm node-a textgen get-task --id "$task_id")
  [[ $(jq -r '(.data // .).status' <<<"$task") == "3" ]] && break
  sleep 0.25
done
[[ $(jq -r '(.data // .).status' <<<"$task") == "3" ]] || { echo "task did not complete" >&2; exit 1; }
answer=$(jq -r '(.data // .).output_artifacts[0].inline | @base64d' <<<"$task")
[[ "$answer" == "4" ]] || { echo "wrong result: $answer" >&2; exit 1; }
log "task $task_id completed with answer $answer"

# Subscription is durable: start it after completion and receive replay.
events="$RUNTIME/task-events.jsonl"
# Start the client directly so $! is the actual CLI process.  Backgrounding
# the mm shell function only kills its wrapper and can leave the subscriber
# alive to print an EOF when the harness later stops the daemon.
HOME="$(agent_home node-a textgen)" "$BIN" --json subscribe-task-events --id "$task_id" \
  --data-dir "$(agent_data node-a textgen)" >"$events" 2>"$RUNTIME/task-events.err" & subscriber=$!
sleep 1; kill "$subscriber" 2>/dev/null || true; wait "$subscriber" 2>/dev/null || true
jq -se -e 'map(select(.data == "NA==" or .data?.data == "NA==")) | length > 0' "$events" >/dev/null
log "verified durable task-event replay"

# The terminal task result is also committed to the thread and replicated.
deadline=$((SECONDS + 60))
for spec in "node-a textgen" "node-a observer" "node-b calculator"; do
  read -r node agent <<<"$spec"
  until entries=$(mm "$node" "$agent" get-thread-entries --id "$thread_id") && jq -se -e 'map(.data // .) | flatten | map(select(.entry.kind == "task_result")) | length > 0' >/dev/null <<<"$entries"; do
    (( SECONDS < deadline )) || { echo "thread replication timeout for $node/$agent" >&2; exit 1; }
    sleep 0.5
  done
done
log "verified terminal result replication on all three members"

# Original agent loops have exited. A brand-new identity knows only the saved
# thread ID; storage daemons remain online to serve content-addressed blocks.
home=$(agent_home node-c recovery); data=$(agent_data node-c recovery)
HOME="$home" "$BIN" start --config "$data/moltbook.toml" >"$data/manual.log" 2>&1 & echo $! >"$data/manual.pid"
wait_health node-c recovery
# A brand-new node has to find providers for the thread's blocks through the
# DHT and fetch them over Bitswap. Provider records take time to become
# discoverable from a routing table this node only just built, so 60s failed
# roughly two runs in five on an otherwise healthy network.
deadline=$((SECONDS + 180))
until mm node-c recovery recover-thread --id "$thread_id" --secret-base64 "$recovery_secret" >/dev/null 2>&1; do
  (( SECONDS < deadline )) || { echo "cold recovery timeout after 180s" >&2; exit 1; }
  sleep 0.5
done
recovered=$(mm node-c recovery get-thread-entries --id "$thread_id")
jq -se -e 'map(.data // .) | flatten | map(select(.entry.kind == "task_result")) | length > 0' >/dev/null <<<"$recovered"
log "verified cold recovery with the capability secret"

echo "PASS thread=$thread_id task=$task_id answer=$answer"
