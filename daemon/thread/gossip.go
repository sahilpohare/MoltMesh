package thread

import (
	"context"
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/daemon/identity"
)

// ConsensusTopic returns the GossipSub topic name for a thread's consensus messages.
func ConsensusTopic(threadID string) string {
	return fmt.Sprintf("a2a/threads/%s/consensus", threadID)
}

// GossipBridge connects a thread Engine to GossipSub.
// It subscribes to the thread's consensus topic and feeds messages into the
// engine, and exposes a BroadcastFunc for the engine to publish messages out.
type GossipBridge struct {
	ps       *pubsub.PubSub
	engine   *Engine
	topic    *pubsub.Topic
	id       *identity.Identity
	log      *zap.Logger
	threadID string
}

// NewGossipBridge creates a GossipBridge but does not start it.
// Call Run(ctx) to begin subscribing.
func NewGossipBridge(
	ps *pubsub.PubSub,
	engine *Engine,
	threadID string,
	id *identity.Identity,
	log *zap.Logger,
) (*GossipBridge, error) {
	topicName := ConsensusTopic(threadID)
	t, err := ps.Join(topicName)
	if err != nil {
		return nil, fmt.Errorf("join thread topic %q: %w", topicName, err)
	}
	return &GossipBridge{
		ps:       ps,
		engine:   engine,
		topic:    t,
		id:       id,
		log:      log,
		threadID: threadID,
	}, nil
}

// BroadcastFunc returns a function suitable for passing to Engine.Run.
// It delivers the message to the local engine immediately (GossipSub does not
// echo messages back to the publisher) and asynchronously publishes to GossipSub
// for remote validators. The async publish prevents deadlocks when Publish blocks
// waiting for mesh peers.
func (g *GossipBridge) BroadcastFunc() func(*pb.ConsensusMsg) {
	return func(msg *pb.ConsensusMsg) {
		// Deliver locally first.
		g.engine.Deliver(msg)

		// Publish to remote peers asynchronously so we never block the caller.
		data, err := proto.Marshal(msg)
		if err != nil {
			g.log.Warn("thread: marshal consensus msg", zap.Error(err))
			return
		}
		go func() {
			if err := g.topic.Publish(context.Background(), data); err != nil {
				g.log.Warn("thread: publish consensus msg",
					zap.String("thread", g.threadID),
					zap.Error(err),
				)
			}
		}()
	}
}

// Run subscribes to GossipSub and delivers messages to the engine.
// It starts the engine's consensus loop in a separate goroutine.
// Blocks until ctx is cancelled.
func (g *GossipBridge) Run(ctx context.Context) {
	sub, err := g.topic.Subscribe()
	if err != nil {
		g.log.Error("thread: subscribe gossipsub topic",
			zap.String("thread", g.threadID),
			zap.Error(err),
		)
		return
	}
	defer sub.Cancel()

	broadcast := g.BroadcastFunc()

	// Start the engine loop in the background.
	go g.engine.Run(ctx, broadcast)

	// Forward incoming GossipSub messages to the engine.
	for {
		gmsg, err := sub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			g.log.Warn("thread: gossipsub recv",
				zap.String("thread", g.threadID),
				zap.Error(err),
			)
			continue
		}
		var cm pb.ConsensusMsg
		if err := proto.Unmarshal(gmsg.Data, &cm); err != nil {
			g.log.Warn("thread: unmarshal consensus msg", zap.Error(err))
			continue
		}
		// Filter out our own messages (engine handles self-messages via inboundCh
		// only when it explicitly sends to itself).
		if cm.ThreadId != g.threadID {
			continue
		}
		g.engine.Deliver(&cm)
	}
}
