package thread

import (
	"context"

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
