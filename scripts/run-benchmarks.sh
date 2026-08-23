#!/usr/bin/env bash
# Run the moltmesh benchmark suite and collect results as CSV.
#
#   ./scripts/run-benchmarks.sh                  # full suite
#   ./scripts/run-benchmarks.sh raft_3node       # one scenario
#   BENCH_VERBOSE=1 ./scripts/run-benchmarks.sh  # full daemon logs
#
# Each scenario runs in its own `go test` process.
#
# That is deliberate. Running several multi-node scenarios in one process is
# not reliable: a scenario that passes on its own stops committing entirely
# when an earlier scenario ran first, even though every scenario builds fresh
# daemons on fresh ports. libp2p connections still establish, so the failure
# is not at the transport layer; the symptom is that GossipSub carries no
# consensus traffic, which is consistent with go-libp2p's process-wide
# resource manager not having released the previous hosts' stream budget yet.
# That is a property of packing many hosts into one test binary, not of the
# daemon, which runs a single host per process. Isolating each scenario
# removes the interference and also stops GC and scheduler pressure from one
# scenario biasing the next one's latency figures.
#
# Results land in bench/results/ (or $BENCH_RESULTS_DIR):
#
#   environment.csv                    what the numbers were produced on
#   thread_commit_latency.csv          percentiles per scenario
#   thread_commit_latency_raw.csv      every observation, for re-plotting
#   commit_latency_vs_epoch.csv        epoch sweep summary
#   thread_throughput.csv              sustained commit rate
#
# The benchmarks sit behind the `bench` build tag so `go test ./...` stays
# fast. A non-zero exit means at least one scenario did not converge; that is
# a reportable result, and the scenarios that did converge still write CSVs.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export BENCH_GIT_COMMIT="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
git diff --quiet 2>/dev/null || export BENCH_GIT_COMMIT="${BENCH_GIT_COMMIT}-dirty"

OUT="${BENCH_RESULTS_DIR:-$ROOT/bench/results}"
WORK="$OUT/.parts"
rm -rf "$OUT"
mkdir -p "$WORK"

# test-name : subtest-id
SCENARIOS=(
  "TestThreadCommitLatency:raft_1node"
  "TestThreadCommitLatency:raft_3node"
  "TestThreadCommitLatency:tendermint_1node"
  "TestThreadCommitLatency:tendermint_4node"
  "TestCommitLatencyVsEpoch:epoch_20ms"
  "TestCommitLatencyVsEpoch:epoch_50ms"
  "TestCommitLatencyVsEpoch:epoch_100ms"
  "TestCommitLatencyVsEpoch:epoch_200ms"
  "TestCommitLatencyVsEpoch:epoch_500ms"
  "TestThreadThroughput:raft_1node"
  "TestThreadThroughput:raft_3node"
)

if [[ $# -gt 0 ]]; then
  filtered=()
  for s in "${SCENARIOS[@]}"; do
    [[ "${s#*:}" == *"$1"* || "${s%%:*}" == *"$1"* ]] && filtered+=("$s")
  done
  SCENARIOS=("${filtered[@]}")
fi

echo "commit:  ${BENCH_GIT_COMMIT}"
echo "results: ${OUT}"
echo "running ${#SCENARIOS[@]} scenario(s), one process each"
echo

failed=0
for spec in "${SCENARIOS[@]}"; do
  test_name="${spec%%:*}"
  subtest="${spec#*:}"
  part="$WORK/${test_name}__${subtest}"
  mkdir -p "$part"

  printf '  %-28s %-18s ' "$test_name" "$subtest"
  if BENCH_RESULTS_DIR="$part" go test -tags=bench ./bench/ \
        -run "${test_name}/${subtest}\$" -v -timeout 20m > "$part/log.txt" 2>&1; then
    grep -hoE '(p50=[^ ]+ +p95=[^ ]+)|([0-9.]+ entries/sec)' "$part/log.txt" | head -1
  else
    echo "DID NOT CONVERGE"
    failed=1
  fi
done

# Merge each CSV kind across scenario directories, keeping one header.
python3 - "$WORK" "$OUT" <<'PY'
import csv, glob, os, sys
work, out = sys.argv[1], sys.argv[2]
for name in ["thread_commit_latency.csv", "thread_commit_latency_raw.csv",
             "commit_latency_vs_epoch.csv", "commit_latency_vs_epoch_raw.csv",
             "thread_throughput.csv", "environment.csv"]:
    parts = sorted(glob.glob(os.path.join(work, "*", name)))
    if not parts:
        continue
    rows, header = [], None
    for p in parts:
        with open(p) as f:
            r = list(csv.reader(f))
        if not r:
            continue
        header = header or r[0]
        rows.extend(r[1:])
    if header is None:
        continue
    # environment.csv is identical per run; keep one copy.
    if name == "environment.csv":
        seen, uniq = set(), []
        for row in rows:
            if row and row[0] not in seen:
                seen.add(row[0]); uniq.append(row)
        rows = uniq
    with open(os.path.join(out, name), "w", newline="") as f:
        w = csv.writer(f); w.writerow(header); w.writerows(rows)
    print(f"  merged {name} ({len(rows)} rows)")
PY

echo
if [[ $failed -eq 0 ]]; then
  echo "all scenarios converged"
else
  echo "at least one scenario did not converge; see ${WORK}/*/log.txt"
fi
exit $failed
