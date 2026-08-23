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

var consensusPublishSlots = make(chan struct{}, 128)

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
