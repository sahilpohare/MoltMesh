package thread

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tochemey/goakt/v4/actor"
	"go.uber.org/zap"

	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// raftTickInterval mirrors thread.raftTickMs (100ms); kept local since that
// constant is unexported.
const raftTickInterval = 100 * time.Millisecond

// Threads are virtualized at the application layer: only recently active
// threads have a PID and Raft ticker. Durable SQLite state is the source of
// truth and PreStart reconstructs the actor when it is needed again.
const threadPassivateAfter = 5 * time.Minute

// TickMsg drives one raft clock tick. Sent on a Schedule from PreStart.
type TickMsg struct{}

// ActivateMsg records application activity and ensures a newly activated
// thread gets an immediate chance to propose durable pending entries.
type ActivateMsg struct{}

// ProposeVoterChangeMsg asks the actor to start a Raft ConfChangeV2. The
// response contains a completion channel; Receive must not wait on it because
// subsequent ticks are what commit the change.
type ProposeVoterChangeMsg struct {
	DID string
	Add bool
}
type VoterChangeReply struct {
	Done <-chan error
	Err  error
}

// QueryThreadMsg asks the actor for its current thread + committed height.
// Used by external readers (GetThread/GetThreadEntries callers) via Ask.
type QueryThreadMsg struct{}

// QueryThreadReply is the response to QueryThreadMsg.
type QueryThreadReply struct {
	Thread          *pb.Thread
	CommittedHeight int64
}

// injectFaultMsg forces a panic inside Receive. It exists to exercise the
// crash-isolation contract this actor is supervised under (ADR-0015): a
// panic from processing one bad message — e.g. a Byzantine proposer's
// ConsensusMsg that passes signature checks but trips an invariant deeper in
// handleReady/commitBlock/applyCommittedBlock — must restart the actor with
// state intact, not leave it dead. Not reachable from any real caller; only
// this package's tests send it.
type injectFaultMsg struct{}

// ThreadActor drives a single thread's RaftBackend under GoAkt supervision.
// It is a live consensus participant (WithLongLived — never passivated),
// not an on-demand resource.
//
// State: wraps the existing RaftBackend (daemon/thread/raft.go); it reuses
// that type's fields/logic rather than reimplementing the etcd/raft driving.
type ThreadActor struct {
	thread  *pb.Thread
	id      *identity.Identity
	store   *Store
	log     *zap.Logger
	backend *RaftBackend
	bcast   func(*pb.ConsensusMsg)
	// publish sends a ConsensusMsg to remote replicas over GossipSub. Set by
	// the caller (ThreadSupervisor.Spawn) at construction; nil is a valid,
	// tested configuration (single-node/f=0 threads with no remote replicas).
	publish func(*pb.ConsensusMsg)
	// durability publishes committed blocks to Bitswap/DHT so the thread
	// stays fetchable after every replica goes offline — see publisher.go.
	// A nil *Publisher (the zero value of this field) is valid: PublishBlock
	// no-ops on a nil receiver.
	durability *Publisher
	// schedRef is written by ThreadSupervisor.Spawn on the spawning goroutine
	// and read by PostStop on GoAkt's shutdown goroutine. Those are different
	// goroutines, so the access has to be atomic; a plain string field here
	// is a data race the -race detector flags on passivation.
	schedRef atomic.Pointer[string]

	mu          sync.Mutex
	subscribers []chan *pb.ThreadEntryWithPos
	// committedHeight is loaded during lifecycle initialization and advanced
	// with each commit, so QueryThreadMsg never has to perform SQLite I/O in
	// Receive.
	committedHeight int64
	lastActivity    time.Time
	passivateAfter  time.Duration
	onStop          func()
	metricsActive   bool
}

// newThreadActor constructs a ThreadActor. The RaftBackend/raft.Node itself
// is built in PreStart, not here, per GoAkt's actor lifecycle: PreStart is
// where state recovery happens, not the constructor. publish is called for
// every outbound ConsensusMsg so remote replicas receive it over GossipSub;
// pass nil for threads with no remote replicas. durability may be nil to
// disable Bitswap/DHT publishing (e.g. unit tests with no node/DHT set up).
func newThreadActor(th *pb.Thread, id *identity.Identity, store *Store, log *zap.Logger, publish func(*pb.ConsensusMsg), durability *Publisher) (*ThreadActor, error) {
	return &ThreadActor{
		thread:     th,
		id:         id,
		store:      store,
		log:        log.Named("thread-actor").With(zap.String("thread", th.Id)),
		publish:    publish,
		durability: durability,
	}, nil
}

// Subscribe returns a channel that receives committed entries in order,
// mirroring thread.Engine.Subscribe. The caller must call Unsubscribe when
// done to avoid a goroutine/channel leak.
func (a *ThreadActor) Subscribe() <-chan *pb.ThreadEntryWithPos {
	ch := make(chan *pb.ThreadEntryWithPos, 256)
	a.mu.Lock()
	a.subscribers = append(a.subscribers, ch)
	a.mu.Unlock()
	return ch
}

// Unsubscribe closes and removes a previously subscribed channel.
func (a *ThreadActor) Unsubscribe(ch <-chan *pb.ThreadEntryWithPos) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for i, s := range a.subscribers {
		if s == ch {
			a.subscribers = append(a.subscribers[:i], a.subscribers[i+1:]...)
			close(s)
			return
		}
	}
}

// handleCommit fans out committed block entries to subscribers and, if
// durability publishing is configured, pushes the block to Bitswap/DHT.
// Passed to NewRaftBackend as onCommit; called from inside
// handleReady/commitBlock (i.e. from within Pump), so it must not block —
// the subscriber fan-out only reads the block argument and writes to a's
// own subscriber slice, guarded by a's own mutex, matching
// thread.Engine.handleCommit's approach; the durability publish does
// network I/O (Bitswap store + DHT PutValue), so it runs in its own
// actor-backed durability worker, so this callback never performs network I/O.
func (a *ThreadActor) handleCommit(block *pb.ThreadBlock) {
	a.mu.Lock()
	a.committedHeight = block.Height
	for i, entry := range block.Entries {
		ep := &pb.ThreadEntryWithPos{
			Height:    block.Height,
			Index:     int32(i),
			Entry:     entry,
			BlockHash: block.BlockHash,
		}
		for _, sub := range a.subscribers {
			select {
			case sub <- ep:
			default:
				a.log.Warn("subscriber channel full, dropping entry",
					zap.Int64("height", block.Height),
					zap.Int("index", i),
					zap.String("thread", block.ThreadId),
				)
			}
		}
	}
	a.mu.Unlock()

	if a.durability != nil {
		a.durability.PublishBlock(context.Background(), a.thread, block)
	}
}

var _ actor.Actor = (*ThreadActor)(nil)

// PreStart loads the thread + committed height and constructs/restarts the
// raft.Node via the existing newRaftBackend logic (ported, not rewritten).
//
// The recurring TickMsg schedule (replacing raft.go's hand-rolled
// time.NewTicker) is set up by ThreadSupervisor.Spawn after this returns,
// once a *PID is available — scheduling against a PID looked up by name from
// inside PreStart would race the actor's own registration.
func (a *ThreadActor) PreStart(ctx *actor.Context) error {
	// Re-read the descriptor: a.thread is whatever was captured when the actor
	// was first created, and a promotion raises N in the store. The raft voter
	// set is bootstrapped from the first N ReplicaDids at construction, so
	// starting from the stale copy brings the node back up with the old voter
	// set and its writes never reach a quorum.
	if current, err := a.store.GetThread(a.thread.Id); err == nil && current != nil {
		a.thread = current
	}
	backend, err := newRaftBackend(a.thread, a.id, a.store, a.log, a.handleCommit)
	if err != nil {
		return err
	}
	a.backend = backend
	height, err := a.store.GetCommittedHeight(a.thread.Id)
	if err != nil {
		return err
	}
	a.committedHeight = height
	a.lastActivity = time.Now()
	appactors.Metrics.ThreadActivated()
	a.metricsActive = true
	if a.passivateAfter <= 0 {
		a.passivateAfter = threadPassivateAfter
	}
	a.bcast = func(msg *pb.ConsensusMsg) {
		// Local delivery: enqueue onto the backend's own inbound channel
		// rather than calling StepInbound synchronously. This mirrors
		// GossipBridge.BroadcastFunc's "GossipSub does not echo to
		// publisher" local-delivery step, which goes through Engine.Deliver
		// (buffered channel), not a direct Step call. It matters here for a
		// reason gossip's async goroutine hid: TryReady calls handleReady,
		// which calls this broadcast while still inside etcd/raft's
		// Ready/Advance cycle for the current batch — recursing straight
		// into node.Step() from there (as an earlier version of this code
		// did) violates raft's Ready protocol (no Step before Advance) and
		// stalled single-node leader election indefinitely. Queuing here
		// and draining inboundCh from a later Receive call keeps Step calls
		// outside any in-flight Ready/Advance pair.
		a.backend.Deliver(msg)

		// Remote fan-out over GossipSub, mirroring
		// GossipBridge.BroadcastFunc's async publish (never block the
		// caller waiting for mesh peers). publish is nil for threads with
		// no remote replicas (e.g. the f=0/single-node tests).
		if a.publish != nil {
			a.publish(msg)
		}
	}

	a.log.Info("thread actor: started", zap.String("id", a.thread.Id))
	return nil
}

// setScheduleRef records the recurring-tick schedule reference so PostStop
// can cancel it. Called by ThreadSupervisor.Spawn once the PID exists.
func (a *ThreadActor) setScheduleRef(ref string) { a.schedRef.Store(&ref) }

// Receive replaces raft.go's blocking select with message-typed dispatch.
// Per GoAkt's hard framework rule, Receive must never block.
func (a *ThreadActor) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case TickMsg:
		if time.Since(a.lastActivity) >= a.passivateAfter {
			ctx.Shutdown()
			return
		}
		a.backend.Tick(ctx.Context())
		// Pump drains both the queued-inbound and Ready channels in
		// alternation, non-blocking throughout (each step is a select with
		// a default case), required because Receive must never block. A
		// single Tick can chain several raft hops (e.g. campaign → vote
		// request queued via bcast → vote response → leader Ready) that a
		// single TryReady call would only advance one hop per call.
		a.backend.Pump(ctx.Context(), a.bcast)

	case *pb.ConsensusMsg:
		a.lastActivity = time.Now()
		a.backend.StepInbound(ctx.Context(), msg)
		a.backend.Pump(ctx.Context(), a.bcast)

	case ActivateMsg:
		a.lastActivity = time.Now()
		a.backend.Pump(ctx.Context(), a.bcast)

	case ProposeVoterChangeMsg:
		done, err := a.backend.BeginVoterChange(ctx.Context(), msg.DID, msg.Add)
		ctx.Response(&VoterChangeReply{Done: done, Err: err})

	case injectFaultMsg:
		panic("actors: injected fault")

	case QueryThreadMsg:
		a.mu.Lock()
		height := a.committedHeight
		a.mu.Unlock()
		ctx.Response(&QueryThreadReply{Thread: a.thread, CommittedHeight: height})

	default:
		ctx.Unhandled()
	}
}

// PostStop stops the raft node, closes subscriber channels, and flushes any
// pending persistence.
func (a *ThreadActor) PostStop(ctx *actor.Context) error {
	if ref := a.schedRef.Load(); ref != nil && *ref != "" {
		_ = ctx.ActorSystem().CancelSchedule(*ref)
	}
	if a.backend != nil {
		started := time.Now()
		if err := a.backend.Snapshot(); err != nil {
			a.log.Error("thread actor: save raft snapshot", zap.Error(err))
		} else {
			appactors.Metrics.RaftSnapshot(time.Since(started))
		}
		a.backend.Stop()
	}
	if err := a.store.SaveCheckpoint(a.thread.Id, ThreadCheckpoint{
		CommittedHeight: a.committedHeight,
		PassivatedAt:    time.Now().UnixMilli(),
	}); err != nil {
		a.log.Error("thread actor: save checkpoint", zap.Error(err))
	}
	a.mu.Lock()
	for _, s := range a.subscribers {
		close(s)
	}
	a.subscribers = nil
	a.mu.Unlock()
	if a.onStop != nil {
		a.onStop()
	}
	if a.metricsActive {
		appactors.Metrics.ThreadPassivated()
		a.metricsActive = false
	}
	return nil
}
