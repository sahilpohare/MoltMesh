package thread

import (
	"context"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"

	"github.com/sahilpohare/p2p-a2a/daemon/identity"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

const maxConsensusMessageSize = 1 << 20

var consensusPublishSlots = make(chan struct{}, 128)

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
