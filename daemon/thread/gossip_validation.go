package thread

import (
	"context"
	"fmt"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

const maxConsensusMessageSize = 1 << 20

// maxInFlightPublishesPerThread bounds how many consensus messages one thread
// may have in flight on GossipSub at once.
//
// This budget is deliberately per thread. It used to be a single package-level
// channel of 128 slots shared by every thread on every daemon in the process,
// which coupled unrelated threads to each other: a burst on one thread, or a
// torn-down thread whose publish goroutines were still waiting out their five
// second timeout, could exhaust the shared budget and make every other thread
// silently drop consensus messages. Dropped votes and appends do not surface
// as errors, they just prevent a quorum forming, so the visible symptom was a
// cluster that never elected a leader. That is also why the failure only
// appeared once several threads ran in one process. ADR-0015 targets nodes
// holding hundreds of thousands of threads, where one shared 128-slot budget
// across all of them would drop consensus traffic continuously.
//
// A single thread only ever needs a few concurrent publishes (roughly one per
// peer per tick), so a small per-thread budget still bounds memory while
// removing the cross-thread coupling.
const maxInFlightPublishesPerThread = 32

// newPublishSlots returns a per-thread publish budget. Each GossipBridge and
// actorGossipBridge owns one.
func newPublishSlots() chan struct{} {
	return make(chan struct{}, maxInFlightPublishesPerThread)
}

// joinConsensusTopic joins a thread's GossipSub consensus topic and installs
// its replica-set validator — the identical setup step both GossipBridge
// (Engine path, gossip.go) and actorGossipBridge (GoAkt actor path,
// actor_gossip_bridge.go) perform in their constructors before starting a
// receive loop.
func joinConsensusTopic(ps *pubsub.PubSub, th *pb.Thread) (*pubsub.Topic, error) {
	topicName := ConsensusTopic(th.Id)
	t, err := ps.Join(topicName)
	if err != nil {
		return nil, fmt.Errorf("join thread topic %q: %w", topicName, err)
	}
	if err := registerConsensusValidator(ps, th); err != nil {
		_ = t.Close()
		return nil, fmt.Errorf("register thread validator %q: %w", topicName, err)
	}
	return t, nil
}

// decodeConsensusMsg unmarshals a raw GossipSub payload and reports whether
// it belongs to threadID — the identical unmarshal-and-filter step both
// GossipBridge.Run and actorGossipBridge.Run apply to every inbound message
// before delivering it (to the Engine or the ThreadActor mailbox,
// respectively). A nil, nil return means "valid message, not for this
// thread" — not an error, just skip it.
func decodeConsensusMsg(threadID string, data []byte) (*pb.ConsensusMsg, error) {
	var cm pb.ConsensusMsg
	if err := proto.Unmarshal(data, &cm); err != nil {
		return nil, err
	}
	if cm.ThreadId != threadID {
		return nil, nil
	}
	return &cm, nil
}

func registerConsensusValidator(ps *pubsub.PubSub, th *pb.Thread) error {
	replicas := make(map[string]struct{}, len(th.ReplicaDids))
	for _, did := range th.ReplicaDids {
		replicas[did] = struct{}{}
	}
	return ps.RegisterTopicValidator(ConsensusTopic(th.Id), func(_ context.Context, _ peer.ID, msg *pubsub.Message) bool {
		if msg == nil || len(msg.Data) == 0 || len(msg.Data) > maxConsensusMessageSize {
			return false
		}
		author := msg.GetFrom()
		pub, err := author.ExtractPublicKey()
		if err != nil {
			return false
		}
		raw, err := pub.Raw()
		if err != nil {
			return false
		}
		if _, ok := replicas[identity.DIDFromPubBytes(raw)]; !ok {
			return false
		}
		var cm pb.ConsensusMsg
		return proto.Unmarshal(msg.Data, &cm) == nil && cm.ThreadId == th.Id && cm.Payload != nil
	}, pubsub.WithValidatorConcurrency(4), pubsub.WithValidatorTimeout(2*time.Second))
}
