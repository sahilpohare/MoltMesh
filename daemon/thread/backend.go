package thread

import (
	"context"
	"errors"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// BackendKind selects the consensus algorithm for a thread.
type BackendKind string

const (
	BackendTendermint BackendKind = "tendermint"
	BackendRaft       BackendKind = "raft"
)

// Backend is the consensus algorithm interface.
// Each thread has one Backend instance.
type Backend interface {
	// Run starts the consensus loop. Blocks until ctx is cancelled.
	// broadcast sends a ConsensusMsg to all peers (via GossipSub).
	Run(ctx context.Context, broadcast func(*pb.ConsensusMsg))

	// Deliver feeds an inbound ConsensusMsg from a peer into the backend.
	Deliver(msg *pb.ConsensusMsg)

	// Subscribe returns a channel that receives committed entries in order.
	Subscribe() <-chan *pb.ThreadEntryWithPos

	// Unsubscribe closes and removes a subscriber channel.
	Unsubscribe(ch <-chan *pb.ThreadEntryWithPos)
}

// ActorBackend is the consensus surface ThreadActor drives. Unlike Backend,
// nothing here blocks or owns a goroutine: the actor owns the loop and steps
// the backend synchronously from its mailbox, once per TickMsg (100ms) and
// once per inbound message. Every method must return promptly.
//
// This is the polymorphic seam. A backend that reaches consensus through a
// shared database rather than through peers (an in-org Postgres backend, for
// instance) has no consensus messages at all: its StepInbound is a no-op,
// Pump inserts pending entries and reads back the committed sequence, and
// Tick polls for rows committed by others. Nothing here assumes votes,
// leaders, or a replicated log.
type ActorBackend interface {
	// Tick advances the backend's clock: expire timeouts, propose pending
	// entries if this node may, and so on.
	Tick(ctx context.Context)
	// StepInbound feeds one inbound ConsensusMsg synchronously.
	StepInbound(ctx context.Context, msg *pb.ConsensusMsg)
	// Pump drains queued inbound messages and any internal work until quiet,
	// bounded so a message storm cannot starve the actor's mailbox. Committed
	// blocks surface through the CommitCallback the backend was built with.
	Pump(ctx context.Context, broadcast func(*pb.ConsensusMsg))
	// Deliver enqueues an inbound message from a transport goroutine. It must
	// never step the backend itself; Pump does that from the actor.
	Deliver(msg *pb.ConsensusMsg)
	Stop()
}

// VoterChanger is implemented by backends whose membership is a voter set
// (Raft). A backend without one, such as a database-ordered backend, simply
// does not implement it and ProposeVoterChangeMsg is answered with an error.
type VoterChanger interface {
	BeginVoterChange(ctx context.Context, did string, add bool) (<-chan error, error)
}

// Snapshotter is implemented by backends with a log worth compacting on
// passivation. Optional for the same reason as VoterChanger.
type Snapshotter interface {
	Snapshot() error
}

// ErrNoVoterSet is returned when a voter change is requested of a backend
// that has no voter set.
var ErrNoVoterSet = errors.New("thread: backend has no voter set")
