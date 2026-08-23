//go:build bench

// Package bench measures the runtime characteristics of a moltmesh network:
// thread commit latency, thread throughput, cross-daemon task delegation
// latency, and daemon startup cost against persisted thread count.
//
// Every measurement runs against real daemons: real libp2p hosts over TCP,
// real GossipSub with StrictSign, real Raft or Tendermint consensus, real
// SQLite persistence, and a real gRPC server per node. Nothing here is
// mocked or stubbed. The only concessions to running in one process are
// that hosts listen on 127.0.0.1 and storage is in-memory SQLite, both of
// which are stated as threats to validity in the dissertation rather than
// hidden: they remove disk and physical-network latency from the numbers,
// so results are a lower bound on real-world latency, not an estimate of it.
//
// Results are written as CSV so they can be re-analysed independently of
// this code. Run everything with scripts/run-benchmarks.sh.
package bench

import (
	"context"
	"encoding/csv"
	"fmt"
	"math"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ipfs/boxo/blockstore"
	datastore "github.com/ipfs/go-datastore"
	dssync "github.com/ipfs/go-datastore/sync"

	"github.com/sahilpohare/p2p-a2a/daemon/deliver"
	"github.com/sahilpohare/p2p-a2a/daemon/gossip"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	"github.com/sahilpohare/p2p-a2a/daemon/inbox"
	"github.com/sahilpohare/p2p-a2a/daemon/node"
	"github.com/sahilpohare/p2p-a2a/daemon/outbox"
	"github.com/sahilpohare/p2p-a2a/daemon/rpc"
	"github.com/sahilpohare/p2p-a2a/daemon/tasks"
	"github.com/sahilpohare/p2p-a2a/daemon/thread"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"

	_ "github.com/mattn/go-sqlite3"
)

// ─── daemon harness ───────────────────────────────────────────────────────────

type daemon struct {
	id     *identity.Identity
	client pb.A2ANodeClient
	dlv    *deliver.Deliverer
	tm     *thread.Manager
	addrs  []string
}

// newDaemon starts one complete in-process node. It deliberately uses
// thread.NewManager (the Engine path) rather than thread.NewActorManager,
// because the actor path constructs a Raft backend unconditionally and never
// reads Metadata["backend"]. Benchmarking Raft against Tendermint is only
// possible through the Engine path today; that asymmetry is itself a result
// reported in the dissertation.
func newDaemon(tb testing.TB, ctx context.Context, log *zap.Logger) *daemon {
	tb.Helper()

	id, err := identity.Generate()
	if err != nil {
		tb.Fatalf("identity.Generate: %v", err)
	}

	h, err := libp2p.New(
		libp2p.Identity(id.LibP2PKey),
		libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"),
	)
	if err != nil {
		tb.Fatalf("libp2p.New: %v", err)
	}
	tb.Cleanup(func() { h.Close() })

	ps, err := pubsub.NewGossipSub(ctx, h, pubsub.WithMessageSignaturePolicy(pubsub.StrictSign))
	if err != nil {
		tb.Fatalf("pubsub.NewGossipSub: %v", err)
	}

	addrs := make([]string, len(h.Addrs()))
	for i, a := range h.Addrs() {
		addrs[i] = fmt.Sprintf("%s/p2p/%s", a, h.ID())
	}

	ib, err := inbox.New(":memory:")
	if err != nil {
		tb.Fatalf("inbox.New: %v", err)
	}
	tb.Cleanup(func() { ib.Close() })

	ts, err := tasks.New(":memory:")
	if err != nil {
		tb.Fatalf("tasks.New: %v", err)
	}
	tb.Cleanup(func() { ts.Close() })

	gm := gossip.New(ps, log)
	dlv := deliver.New(h, nil, ib, nil, nil, log)

	bs := blockstore.NewBlockstore(dssync.MutexWrap(datastore.NewMapDatastore()))
	n := &node.Node{Host: h, Blockstore: bs}

	ob, err := outbox.New(":memory:", dlv.DeliverFunc(), log)
	if err != nil {
		tb.Fatalf("outbox.New: %v", err)
	}
	tb.Cleanup(func() { ob.Close() })
	go ob.Run(ctx)

	threadStore, err := thread.NewStore(":memory:")
	if err != nil {
		tb.Fatalf("thread.NewStore: %v", err)
	}
	tb.Cleanup(func() { threadStore.Close() })
	tm := thread.NewManager(ctx, threadStore, id, ps, log)

	srv := rpc.New(id, ib, ob, ts, nil, gm, dlv, tm, nil, nil, nil, n, addrs, nil, log)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("net.Listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterA2ANodeServer(grpcSrv, srv)
	go grpcSrv.Serve(lis) //nolint:errcheck
	tb.Cleanup(func() { grpcSrv.Stop() })

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		tb.Fatalf("grpc.NewClient: %v", err)
	}
	tb.Cleanup(func() { conn.Close() })

	return &daemon{id: id, client: pb.NewA2ANodeClient(conn), dlv: dlv, tm: tm, addrs: addrs}
}

// connectAll wires every daemon to every other one at the libp2p layer and
// waits for GossipSub meshes to form. Consensus traffic rides GossipSub, so
// benchmarking before the mesh has settled measures mesh formation rather
// than consensus.
func connectAll(tb testing.TB, ds []*daemon) {
	tb.Helper()
	for i, a := range ds {
		for j, b := range ds {
			if i == j {
				continue
			}
			info, err := peer.AddrInfoFromString(b.addrs[0])
			if err != nil {
				tb.Fatalf("parse addr: %v", err)
			}
			if err := a.dlv.Host().Connect(context.Background(), *info); err != nil {
				tb.Fatalf("connect: %v", err)
			}
		}
	}
	if len(ds) > 1 {
		time.Sleep(meshSettleDelay)
	}
}

const meshSettleDelay = 1500 * time.Millisecond

// startThreadOnPeers boots the thread's consensus engine on every replica
// other than the creator.
//
// CreateThread only starts an engine on the node that served the call. In a
// deployed network the remaining replicas learn about the thread from a
// THREAD_INVITE delivered through the outbox, which needs a registry to
// resolve recipient DIDs; this harness runs without one, so the invite step
// is performed directly instead. The metadata is carried over verbatim
// because it holds both the backend selector and the creator's signature
// over the descriptor.
//
// Without this, a multi-voter thread is created but never reaches quorum:
// the creator is the only node with an engine, so no other replica can vote
// and nothing ever commits.
func startThreadOnPeers(tb testing.TB, ds []*daemon, th *pb.Thread) {
	tb.Helper()
	for _, d := range ds[1:] {
		peerCopy := &pb.Thread{
			Id:          th.Id,
			CreatorDid:  th.CreatorDid,
			ReplicaDids: th.ReplicaDids,
			N:           th.N,
			F:           th.F,
			EpochMs:     th.EpochMs,
			CreatedAt:   th.CreatedAt,
			Metadata:    th.Metadata,
		}
		if err := d.tm.Start(peerCopy); err != nil {
			tb.Fatalf("start thread on replica: %v", err)
		}
	}
	if len(ds) > 1 {
		time.Sleep(electionSettleDelay)
	}
}

// electionSettleDelay gives a multi-voter cluster time to elect a leader
// before the first measured append. Without it the first sample would
// include election time rather than commit time.
const electionSettleDelay = 2 * time.Second

// ─── statistics ───────────────────────────────────────────────────────────────

// sample is one timed observation.
type sample struct {
	d time.Duration
}

// summary reports the distribution of a set of latency observations.
// Percentiles are reported rather than means alone because commit latency is
// bounded below by a consensus round and has a long right tail: a mean hides
// both facts.
type summary struct {
	Label  string
	N      int
	Min    time.Duration
	P50    time.Duration
	P90    time.Duration
	P95    time.Duration
	P99    time.Duration
	Max    time.Duration
	Mean   time.Duration
	StdDev time.Duration
}

func summarize(label string, samples []sample) summary {
	if len(samples) == 0 {
		return summary{Label: label}
	}
	ds := make([]time.Duration, len(samples))
	for i, s := range samples {
		ds[i] = s.d
	}
	sort.Slice(ds, func(i, j int) bool { return ds[i] < ds[j] })

	var total time.Duration
	for _, d := range ds {
		total += d
	}
	mean := total / time.Duration(len(ds))

	var sumSq float64
	for _, d := range ds {
		diff := float64(d - mean)
		sumSq += diff * diff
	}
	stddev := time.Duration(math.Sqrt(sumSq / float64(len(ds))))

	return summary{
		Label: label, N: len(ds),
		Min:  ds[0],
		P50:  pct(ds, 50),
		P90:  pct(ds, 90),
		P95:  pct(ds, 95),
		P99:  pct(ds, 99),
		Max:  ds[len(ds)-1],
		Mean: mean, StdDev: stddev,
	}
}

// pct returns the p-th percentile using nearest-rank, which needs no
// interpolation and is stable for the small sample sizes used here.
func pct(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	rank := int(math.Ceil(float64(p) / 100 * float64(len(sorted))))
	if rank < 1 {
		rank = 1
	}
	if rank > len(sorted) {
		rank = len(sorted)
	}
	return sorted[rank-1]
}

func (s summary) log(tb testing.TB) {
	tb.Helper()
	tb.Logf("%-34s n=%-4d p50=%-9v p95=%-9v p99=%-9v max=%-9v mean=%v sd=%v",
		s.Label, s.N, rnd(s.P50), rnd(s.P95), rnd(s.P99), rnd(s.Max), rnd(s.Mean), rnd(s.StdDev))
}

// rnd trims sub-microsecond noise so printed values stay readable.
func rnd(d time.Duration) time.Duration { return d.Round(time.Microsecond) }

// ─── result recording ─────────────────────────────────────────────────────────

// resultsDir is where CSVs land. Overridable so a CI job can collect them.
func resultsDir() string {
	if d := os.Getenv("BENCH_RESULTS_DIR"); d != "" {
		return d
	}
	return "results"
}

// writeLatencyCSV appends one row per summary, creating the file with a
// header on first write. Raw per-sample values go to a sibling file so the
// distribution can be re-plotted without re-running the experiment.
func writeLatencyCSV(tb testing.TB, name string, sums []summary) {
	tb.Helper()
	dir := resultsDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		tb.Fatalf("mkdir results: %v", err)
	}
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()
	if err := w.Write([]string{"scenario", "n", "min_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms", "mean_ms", "stddev_ms"}); err != nil {
		tb.Fatalf("write header: %v", err)
	}
	for _, s := range sums {
		row := []string{
			s.Label, strconv.Itoa(s.N),
			ms(s.Min), ms(s.P50), ms(s.P90), ms(s.P95), ms(s.P99), ms(s.Max), ms(s.Mean), ms(s.StdDev),
		}
		if err := w.Write(row); err != nil {
			tb.Fatalf("write row: %v", err)
		}
	}
	tb.Logf("wrote %s", path)
}

// writeRawCSV records every observation, so percentiles can be recomputed and
// distributions plotted (boxplots, violin plots) without trusting this code's
// own summary arithmetic.
func writeRawCSV(tb testing.TB, name string, byScenario map[string][]sample) {
	tb.Helper()
	dir := resultsDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		tb.Fatalf("mkdir results: %v", err)
	}
	path := filepath.Join(dir, name)
	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"scenario", "iteration", "latency_ms"})
	scenarios := make([]string, 0, len(byScenario))
	for k := range byScenario {
		scenarios = append(scenarios, k)
	}
	sort.Strings(scenarios)
	for _, sc := range scenarios {
		for i, s := range byScenario[sc] {
			_ = w.Write([]string{sc, strconv.Itoa(i), ms(s.d)})
		}
	}
	tb.Logf("wrote %s", path)
}

func ms(d time.Duration) string {
	return strconv.FormatFloat(float64(d)/float64(time.Millisecond), 'f', 4, 64)
}

// writeEnvironment records what the numbers were produced on. Without this a
// latency figure is not reproducible, only repeatable by whoever ran it.
func writeEnvironment(tb testing.TB) {
	tb.Helper()
	dir := resultsDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		tb.Fatalf("mkdir results: %v", err)
	}
	path := filepath.Join(dir, "environment.csv")
	f, err := os.Create(path)
	if err != nil {
		tb.Fatalf("create %s: %v", path, err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{"key", "value"})
	for _, kv := range [][2]string{
		{"go_version", runtime.Version()},
		{"goos", runtime.GOOS},
		{"goarch", runtime.GOARCH},
		{"num_cpu", strconv.Itoa(runtime.NumCPU())},
		{"git_commit", os.Getenv("BENCH_GIT_COMMIT")},
		{"captured_at_utc", time.Now().UTC().Format(time.RFC3339)},
		{"storage", "in-memory SQLite"},
		{"transport", "libp2p TCP over loopback"},
	} {
		_ = w.Write([]string{kv[0], kv[1]})
	}
	tb.Logf("wrote %s", path)
}

// quietLogger silences daemons during measurement. Set BENCH_VERBOSE=1 to get
// full daemon logs when diagnosing a scenario that will not commit.
func quietLogger() *zap.Logger {
	if os.Getenv("BENCH_VERBOSE") == "1" {
		l, _ := zap.NewDevelopment()
		return l
	}
	return zap.NewNop()
}
