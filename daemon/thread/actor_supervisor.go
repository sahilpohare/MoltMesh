package thread

import (
	"context"
	"fmt"
	"sync"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/tochemey/goakt/v4/actor"
	goakterrors "github.com/tochemey/goakt/v4/errors"
	"github.com/tochemey/goakt/v4/supervisor"
	"go.uber.org/zap"

	"github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// crash-budget numbers per ADR-0008: 3 crashes / 60s window.
const (
	threadSupervisorMaxRetries = uint32(3)
	threadSupervisorWindow     = 60 * time.Second
)

// ThreadSupervisor spawns and owns ThreadActor children under the daemon's
// ActorSystem. On budget-exceeded, GoAkt suspends the child (queryable via
// pid.IsSuspended, revivable via pid.Reinstate) rather than killing it
// permanently — a deliberate divergence from ADR-0008's "mark failed, do not
// thrash" wording; see ADR-0015.
type ThreadSupervisor struct {
	system        *actors.System
	store         *Store
	id            *identity.Identity
	ps            *pubsub.PubSub // nil disables GossipSub fan-out (e.g. unit tests)
	durability    *Publisher     // nil disables Bitswap/DHT durability publishing (e.g. unit tests)
	log           *zap.Logger
	hierarchy     *actors.Hierarchy
	supervisorPID *actor.PID
	ctx           context.Context // lifecycle context — bridge goroutines live as long as this

	sup            *supervisor.Supervisor
	passivateAfter time.Duration

	mu      sync.Mutex
	pids    sync.Map // threadID -> *actor.PID
	cancels sync.Map // threadID -> context.CancelFunc
}

func (s *ThreadSupervisor) UseHierarchy(h *actors.Hierarchy) error {
	pid, err := h.Spawn(s.ctx, "threads", &actors.ContainerActor{}, actor.WithLongLived())
	if err != nil {
		return err
	}
	s.hierarchy, s.supervisorPID = h, pid
	return nil
}

// NewThreadSupervisor creates a ThreadSupervisor bound to the given
// ActorSystem. ctx is the daemon lifecycle context — GossipSub bridge
// goroutines run until it is cancelled, mirroring thread.Manager's ctx. ps
// may be nil to disable GossipSub fan-out entirely (used by tests that only
// exercise single-node/f=0 threads). durability may be nil to disable
// Bitswap/DHT durability publishing (see publisher.go).
func NewThreadSupervisor(ctx context.Context, system *actors.System, store *Store, id *identity.Identity, ps *pubsub.PubSub, durability *Publisher, log *zap.Logger) *ThreadSupervisor {
	sup := supervisor.NewSupervisor(
		supervisor.WithStrategy(supervisor.OneForOneStrategy),
		supervisor.WithRetry(threadSupervisorMaxRetries, threadSupervisorWindow),
		supervisor.WithExponentialBackoff(100*time.Millisecond, 5*time.Second, threadSupervisorWindow),
		// A panic inside Receive (e.g. malformed ConsensusMsg from a Byzantine
		// proposer that passed signature checks) should restart the actor, not
		// permanently stop it — GoAkt's own default for PanicError is Stop.
		supervisor.WithDirective(&goakterrors.PanicError{}, supervisor.RestartDirective),
	)

	return &ThreadSupervisor{
		system:         system,
		store:          store,
		id:             id,
		ps:             ps,
		durability:     durability,
		log:            log,
		ctx:            ctx,
		sup:            sup,
		passivateAfter: threadPassivateAfter,
	}
}

// Spawn starts a ThreadActor for the given thread, supervised per s.sup.
// Safe to call multiple times — if already spawned, returns the existing PID.
func (s *ThreadSupervisor) Spawn(ctx context.Context, th *pb.Thread) (*actor.PID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if value, ok := s.pids.Load(th.Id); ok {
		pid := value.(*actor.PID)
		if pid.IsRunning() {
			return pid, nil
		}
		s.pids.Delete(th.Id)
		if value, ok := s.cancels.LoadAndDelete(th.Id); ok {
			value.(context.CancelFunc)()
		}
	}

	tctx, cancel := context.WithCancel(s.ctx)
	var pid *actor.PID

	var bridge *actorGossipBridge
	var publish func(*pb.ConsensusMsg)
	if s.ps != nil {
		var err error
		bridge, err = newActorGossipBridge(s.ctx, s.ps, s.system.ActorSystem(), s.hierarchy, s.supervisorPID, th, s.log)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("actors: gossip bridge for thread %s: %w", th.Id, err)
		}
		publish = bridge.PublishFunc()
	}

	ta, err := newThreadActor(th, s.id, s.store, s.log, publish, s.durability)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("actors: new thread actor %s: %w", th.Id, err)
	}
	ta.passivateAfter = s.passivateAfter
	if s.hierarchy != nil {
		pid, err = s.hierarchy.SpawnUnder(ctx, s.supervisorPID, threadActorName(th.Id), ta, actor.WithSupervisor(s.sup))
	} else {
		pid, err = s.system.ActorSystem().Spawn(ctx, threadActorName(th.Id), ta, actor.WithSupervisor(s.sup))
	}
	if err != nil {
		cancel()
		return nil, fmt.Errorf("actors: spawn thread actor %s: %w", th.Id, err)
	}
	ta.onStop = func() {
		cancel()
		if current, ok := s.pids.Load(th.Id); ok && current == pid {
			s.pids.Delete(th.Id)
			s.cancels.Delete(th.Id)
		}
	}

	schedRef := "tick-" + th.Id
	if err := s.system.ActorSystem().Schedule(ctx, TickMsg{}, pid, raftTickInterval,
		actor.WithReference(schedRef),
	); err != nil {
		_ = s.system.ActorSystem().Kill(ctx, pid.Name())
		cancel()
		return nil, fmt.Errorf("actors: schedule tick for thread actor %s: %w", th.Id, err)
	}
	ta.setScheduleRef(schedRef)

	if bridge != nil {
		go bridge.Run(tctx, pid)
	}

	s.pids.Store(th.Id, pid)
	s.cancels.Store(th.Id, cancel)
	return pid, nil
}

// PID returns the running ThreadActor PID for a thread, or nil.
func (s *ThreadSupervisor) PID(threadID string) *actor.PID {
	value, ok := s.pids.Load(threadID)
	if !ok {
		return nil
	}
	pid := value.(*actor.PID)
	if pid == nil || pid.IsRunning() {
		return pid
	}
	s.pids.Delete(threadID)
	if value, ok := s.cancels.LoadAndDelete(threadID); ok {
		value.(context.CancelFunc)()
	}
	return nil
}

// Stop stops the ThreadActor for a thread, if running, and its GossipSub
// bridge goroutine.
func (s *ThreadSupervisor) Stop(ctx context.Context, threadID string) error {
	value, ok := s.pids.LoadAndDelete(threadID)
	if !ok {
		return nil
	}
	pid := value.(*actor.PID)
	if value, ok := s.cancels.LoadAndDelete(threadID); ok {
		value.(context.CancelFunc)()
	}
	return s.system.ActorSystem().Kill(ctx, pid.Name())
}

func threadActorName(threadID string) string {
	return "thread-" + threadID
}
