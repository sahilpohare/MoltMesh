//go:build bench

package bench

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// Sample sizes. Latency runs are sequential, so each sample costs at least
// one consensus round; these are sized to finish in minutes, not hours, while
// still giving a p99 that is not a single observation.
const (
	latencyWarmup  = 10
	latencySamples = 200
	throughputSecs = 5
)

// TestThreadCommitLatency measures the time from AppendEntry returning to the
// entry arriving on a SubscribeThread stream: the interval during which
// consensus actually happens.
//
// Measuring via the subscription rather than polling GetThreadEntries matters.
// Polling quantises every observation to the poll interval, which would put a
// floor under the reported latency and make the single-node fast path
// (expected to be sub-millisecond) indistinguishable from a 50 ms poll.
//
// Four scenarios isolate two independent variables, consensus algorithm and
// cluster size, so the cost of each can be attributed separately.
func TestThreadCommitLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("latency benchmark; skipped under -short")
	}
	writeEnvironment(t)

	// id is slash-free so `go test -run` can select a single scenario:
	// a "/" inside a subtest name is parsed as another nesting level.
	scenarios := []struct {
		id      string
		label   string
		backend string
		nodes   int
		f       int32
	}{
		{"raft_1node", "raft/1-node (f=0 fast path)", "raft", 1, 0},
		{"raft_3node", "raft/3-node (f=1, CFT)", "raft", 3, 1},
		{"tendermint_1node", "tendermint/1-node", "tendermint", 1, 0},
		{"tendermint_4node", "tendermint/4-node (f=1, BFT)", "tendermint", 4, 1},
	}

	// Cluster sizes differ by backend because the failure models differ:
	// Raft is crash-fault-tolerant and needs 2f+1 voters, Tendermint is
	// Byzantine-fault-tolerant and needs 3f+1. Comparing raft/3 against
	// tendermint/4 is therefore the honest comparison, since each is the
	// smallest cluster that tolerates one failure under its own model.
	var sums []summary
	raw := map[string][]sample{}

	for _, sc := range scenarios {
		t.Run(sc.id, func(t *testing.T) {
			samples := measureCommitLatency(t, sc.backend, sc.nodes, sc.f)
			s := summarize(sc.label, samples)
			s.log(t)
			sums = append(sums, s)
			raw[sc.label] = samples
		})
	}

	writeLatencyCSV(t, "thread_commit_latency.csv", sums)
	writeRawCSV(t, "thread_commit_latency_raw.csv", raw)
}

// measureCommitLatency runs the default configuration: 100 ms epoch, the
// standard sample count.
func measureCommitLatency(t *testing.T, backend string, nodes int, f int32) []sample {
	t.Helper()
	return measureCommitLatencyN(t, backend, nodes, f, 100, latencySamples)
}

func measureCommitLatencyN(t *testing.T, backend string, nodes int, f int32, epochMs int64, samples int) []sample {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	ds := make([]*daemon, nodes)
	for i := range ds {
		ds[i] = newDaemon(t, ctx, quietLogger())
	}
	connectAll(t, ds)

	replicas := make([]string, nodes)
	for i, d := range ds {
		replicas[i] = d.id.DID
	}

	leader := ds[0]
	th, err := leader.client.CreateThread(ctx, &pb.CreateThreadRequest{
		ReplicaDids: replicas,
		F:           f,
		EpochMs:     epochMs,
		Metadata:    map[string]string{"backend": backend},
	})
	if err != nil {
		t.Fatalf("CreateThread(%s, n=%d): %v", backend, nodes, err)
	}
	startThreadOnPeers(t, ds, th)

	// Subscribe before appending anything, so no commit can be missed.
	stream, err := leader.client.SubscribeThread(ctx, &pb.SubscribeThreadRequest{ThreadId: th.Id})
	if err != nil {
		t.Fatalf("SubscribeThread: %v", err)
	}

	// Correlate each commit with the append that produced it by payload, not
	// by arrival order. A block can carry several entries, so the subscription
	// emits more events than there were appends and a positional 1:1 pairing
	// silently matches append N against entry N-1, which shows up as negative
	// latency. Sends are sequential, so at most one payload is outstanding.
	var mu sync.Mutex
	awaiting := map[string]time.Time{}
	matched := make(chan time.Duration, latencyWarmup+samples+64)
	streamErr := make(chan error, 1)

	go func() {
		for {
			e, err := stream.Recv()
			if err != nil {
				streamErr <- err
				return
			}
			now := time.Now()
			key := string(e.Entry.Payload)
			mu.Lock()
			sentAt, ok := awaiting[key]
			if ok {
				delete(awaiting, key)
			}
			mu.Unlock()
			if ok {
				matched <- now.Sub(sentAt)
			}
		}
	}()

	// Establish that the cluster actually commits before timing anything.
	// A fixed sleep is not enough: leader election and GossipSub mesh
	// formation for a brand-new topic both take a variable amount of time,
	// and under the load of several in-process daemons that variance is large
	// enough that a multi-voter scenario intermittently had not elected a
	// leader by the time the first measured append went out. Probing until a
	// commit is observed makes the start of measurement a fact rather than an
	// assumption, and removes the flakiness that produced.
	writer := awaitFirstCommit(t, ds, th.Id, awaiting, &mu, matched, streamErr)
	if writer == nil {
		t.Fatalf("cluster never committed a probe entry (%s, n=%d)", backend, nodes)
	}

	out := make([]sample, 0, samples)
	for i := 0; i < latencyWarmup+samples; i++ {
		payload := fmt.Sprintf("entry-%d", i)
		mu.Lock()
		awaiting[payload] = time.Now()
		mu.Unlock()

		if _, err := writer.client.AppendEntry(ctx, &pb.AppendEntryRequest{
			ThreadId: th.Id,
			Payload:  []byte(payload),
			Kind:     "message",
		}); err != nil {
			t.Fatalf("AppendEntry %d: %v", i, err)
		}

		select {
		case d := <-matched:
			if i >= latencyWarmup { // discard warmup: first commits pay one-off setup
				out = append(out, sample{d: d})
			}
		case err := <-streamErr:
			t.Fatalf("thread stream closed after %d entries: %v", i, err)
		case <-time.After(30 * time.Second):
			t.Fatalf("no commit within 30s at entry %d (%s, n=%d)", i, backend, nodes)
		}
	}
	return out
}

// awaitFirstCommit finds a replica whose appends actually commit, and returns
// it. Returns nil if no replica commits within the readiness budget.
//
// Rotating across replicas is required, not defensive. AppendEntry queues the
// entry locally and RaftBackend.proposePending proposes it only if this node
// is the current Raft leader; a follower keeps the entry in its own pending
// table indefinitely, with no forwarding to the leader and no error returned
// to the caller. Appending to a fixed replica therefore succeeds or hangs
// depending purely on who won the election, which is what made the
// multi-voter scenarios look intermittently broken.
func awaitFirstCommit(
	t *testing.T,
	ds []*daemon,
	threadID string,
	awaiting map[string]time.Time,
	mu *sync.Mutex,
	matched <-chan time.Duration,
	streamErr <-chan error,
) *daemon {
	t.Helper()
	deadline := time.Now().Add(readinessBudget)
	for attempt := 0; time.Now().Before(deadline); attempt++ {
		candidate := ds[attempt%len(ds)]
		payload := fmt.Sprintf("probe-%d", attempt)
		mu.Lock()
		awaiting[payload] = time.Now()
		mu.Unlock()

		if _, err := candidate.client.AppendEntry(context.Background(), &pb.AppendEntryRequest{
			ThreadId: threadID,
			Payload:  []byte(payload),
			Kind:     "message",
		}); err != nil {
			t.Logf("readiness probe %d: append failed: %v", attempt, err)
		}

		select {
		case <-matched:
			return candidate
		case err := <-streamErr:
			t.Logf("readiness probe: stream closed: %v", err)
			return nil
		case <-time.After(probeInterval):
			// Not yet live; the probe payload stays in `awaiting` harmlessly.
		}
	}
	return nil
}

// readinessBudget bounds how long a cluster may take to elect a leader and
// commit its first entry. probeInterval is how often a probe is retried.
const (
	readinessBudget = 60 * time.Second
	probeInterval   = 2 * time.Second
)

// TestCommitLatencyVsEpoch sweeps the per-thread epoch and measures commit
// latency at each setting, holding everything else fixed (single node, f=0,
// Raft, same payload).
//
// The purpose is to attribute latency to a cause. If commit latency is
// dominated by consensus work it should be roughly flat as the epoch changes;
// if it is dominated by waiting for the next epoch tick it should track the
// epoch approximately linearly. The distinction matters because the two
// imply completely different optimisations, and because the project's README
// documents "~150 ms (one Raft heartbeat)" and "single-node (f=0):
// sub-millisecond, no network round-trip" without stating an epoch, which is
// only meaningful if latency is epoch-independent.
func TestCommitLatencyVsEpoch(t *testing.T) {
	if testing.Short() {
		t.Skip("epoch sweep; skipped under -short")
	}

	const sweepSamples = 60
	epochs := []int64{20, 50, 100, 200, 500}

	var sums []summary
	raw := map[string][]sample{}

	for _, epoch := range epochs {
		label := fmt.Sprintf("raft/1-node epoch=%dms", epoch)
		t.Run(fmt.Sprintf("epoch_%dms", epoch), func(t *testing.T) {
			samples := measureCommitLatencyN(t, "raft", 1, 0, epoch, sweepSamples)
			s := summarize(label, samples)
			s.log(t)
			// Ratio of observed median to the configured epoch: ~1.0 means the
			// epoch tick, not consensus, sets the latency.
			t.Logf("    p50/epoch ratio = %.2f", float64(s.P50)/float64(time.Duration(epoch)*time.Millisecond))
			sums = append(sums, s)
			raw[label] = samples
		})
	}

	writeLatencyCSV(t, "commit_latency_vs_epoch.csv", sums)
	writeRawCSV(t, "commit_latency_vs_epoch_raw.csv", raw)
}

// TestThreadThroughput measures sustained committed entries per second with
// appends issued back-to-back rather than one-at-a-time, which is what lets
// the backend batch multiple entries into a block.
//
// This is a deliberately different regime from TestThreadCommitLatency: that
// test measures how long one entry takes when nothing else is in flight, this
// one measures how many entries per second the pipeline sustains. Reporting
// only one of the two would misrepresent the system.
func TestThreadThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("throughput benchmark; skipped under -short")
	}

	scenarios := []struct {
		id      string
		label   string
		backend string
		nodes   int
		f       int32
	}{
		{"raft_1node", "raft/1-node (f=0 fast path)", "raft", 1, 0},
		{"raft_3node", "raft/3-node (f=1, CFT)", "raft", 3, 1},
	}

	type row struct {
		label     string
		committed int
		elapsed   time.Duration
	}
	var rows []row

	for _, sc := range scenarios {
		t.Run(sc.id, func(t *testing.T) {
			n, elapsed := measureThroughput(t, sc.backend, sc.nodes, sc.f)
			if n == 0 {
				t.Errorf("%s committed nothing in %v: a zero rate is a harness or convergence failure, not a measurement", sc.label, elapsed)
			}
			rows = append(rows, row{sc.label, n, elapsed})
			t.Logf("%-34s committed=%-6d in %-8v => %.1f entries/sec",
				sc.label, n, elapsed.Round(time.Millisecond),
				float64(n)/elapsed.Seconds())
		})
	}

	// Throughput is a rate, not a distribution, so it gets its own schema.
	dir := resultsDir()
	_ = dir
	sums := make([]summary, 0, len(rows))
	for _, r := range rows {
		perEntry := time.Duration(0)
		if r.committed > 0 {
			perEntry = r.elapsed / time.Duration(r.committed)
		}
		sums = append(sums, summary{
			Label: fmt.Sprintf("%s (%.1f entries/sec)", r.label, float64(r.committed)/r.elapsed.Seconds()),
			N:     r.committed,
			Mean:  perEntry,
		})
	}
	writeLatencyCSV(t, "thread_throughput.csv", sums)
}

func measureThroughput(t *testing.T, backend string, nodes int, f int32) (int, time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	ds := make([]*daemon, nodes)
	for i := range ds {
		ds[i] = newDaemon(t, ctx, quietLogger())
	}
	connectAll(t, ds)

	replicas := make([]string, nodes)
	for i, d := range ds {
		replicas[i] = d.id.DID
	}
	leader := ds[0]
	th, err := leader.client.CreateThread(ctx, &pb.CreateThreadRequest{
		ReplicaDids: replicas, F: f, EpochMs: 100,
		Metadata: map[string]string{"backend": backend},
	})
	if err != nil {
		t.Fatalf("CreateThread: %v", err)
	}
	startThreadOnPeers(t, ds, th)

	stream, err := leader.client.SubscribeThread(ctx, &pb.SubscribeThreadRequest{ThreadId: th.Id})
	if err != nil {
		t.Fatalf("SubscribeThread: %v", err)
	}

	committed := make(chan struct{}, 1<<16)
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
			committed <- struct{}{}
		}
	}()

	// Find a replica whose appends commit before opening the measurement
	// window. Only the Raft leader proposes appended entries (see
	// awaitFirstCommit), so load aimed at a follower would register as zero
	// throughput regardless of how fast the cluster actually is.
	writer := ds[0]
	readyBy := time.Now().Add(readinessBudget)
	live := false
	for attempt := 0; time.Now().Before(readyBy) && !live; attempt++ {
		candidate := ds[attempt%len(ds)]
		if _, err := candidate.client.AppendEntry(ctx, &pb.AppendEntryRequest{
			ThreadId: th.Id,
			Payload:  []byte(fmt.Sprintf("tp-probe-%d", attempt)),
			Kind:     "message",
		}); err != nil {
			t.Logf("throughput probe %d: %v", attempt, err)
		}
		select {
		case <-committed:
			writer, live = candidate, true
		case <-time.After(probeInterval):
		}
	}
	if !live {
		t.Fatalf("cluster never committed within %v (%s, n=%d)", readinessBudget, backend, nodes)
	}

	// Offer load with a bounded number of concurrent appenders rather than a
	// single unthrottled loop. AppendEntry returns as soon as the entry is
	// queued, so an uncapped loop issues calls far faster than consensus can
	// drain them and, with several in-process daemons sharing the machine,
	// starves the consensus goroutines outright: the three-node case measured
	// zero commits in five seconds that way while the same cluster committed
	// in about 1 ms under the latency test. Concurrency here is offered load,
	// not a tuning knob.
	const appenders = 8
	appendCtx, stopAppends := context.WithCancel(ctx)
	defer stopAppends()
	for a := 0; a < appenders; a++ {
		go func(worker int) {
			for i := 0; ; i++ {
				if appendCtx.Err() != nil {
					return
				}
				_, _ = writer.client.AppendEntry(appendCtx, &pb.AppendEntryRequest{
					ThreadId: th.Id,
					Payload:  []byte(fmt.Sprintf("tp-%d-%d", worker, i)),
					Kind:     "message",
				})
			}
		}(a)
	}

	// Count only commits observed inside the window, so ramp-up before the
	// window and drain after it are both excluded.
	time.Sleep(500 * time.Millisecond) // let the rate settle after first commit
	drain(committed)

	start := time.Now()
	deadline := time.After(throughputSecs * time.Second)
	count := 0
loop:
	for {
		select {
		case <-committed:
			count++
		case <-deadline:
			break loop
		}
	}
	elapsed := time.Since(start)
	stopAppends()
	return count, elapsed
}

func drain(ch chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
