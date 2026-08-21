#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"

self=$(identity_did node-b calculator)
discover node-b calculator "$TEXT_CAP" "$self" "$(agent_home node-b calculator)/discovered/textgen.json"

deadline=$((SECONDS + 90))
while (( SECONDS < deadline )); do
  inbox=$(mm node-b calculator get-inbox --unread --decode --limit 50)
  item=$(jq -c '.data[] | select(.message.kind == 2)' <<<"$inbox" | head -n 1 || true)
  if [[ -n "$item" ]]; then
    message_id=$(jq -r '.message.id' <<<"$item")
    from_did=$(jq -r '.message.from_did' <<<"$item")
    task_id=$(jq -r '.message.task_id' <<<"$item")
    thread_id=$(jq -r '.message.thread_id' <<<"$item")
    question=$(jq -r '.decoded.input_artifacts[0].inline | @base64d' <<<"$item")
    [[ "$question" == "add 2 + 2." ]] || { echo "unexpected task input: $question" >&2; exit 1; }
    answer=$((2 + 2))
    mm node-b calculator send-task-result --to "$from_did" --task-id "$task_id" --thread-id "$thread_id" --result "$answer" >/dev/null
    mm node-b calculator ack-message --id "$message_id" >/dev/null
    echo "$answer"
    exit 0
  fi
  sleep 0.25
done
echo "calculator timed out waiting for a task" >&2
exit 1

