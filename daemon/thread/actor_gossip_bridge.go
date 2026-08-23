package thread

import (
	"context"
	"fmt"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	goakt "github.com/tochemey/goakt/v4/actor"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// actorGossipBridge connects a ThreadActor to GossipSub — the actor-native
// counterpart to GossipBridge (daemon/thread/gossip.go), which is tightly
// coupled to *Engine and starts its own per-thread goroutine running
// Engine.Run. This bridge does neither: publishing is handled by
// ThreadActor.publish (called from inside the actor's Receive via bcast, see
// actor.go), and the only goroutine here is the GossipSub receive loop,
// which every subscription needs regardless of transport — it feeds inbound
// messages into the actor's mailbox via NoSender().Tell, the GoAkt-documented
// idiom for delivering messages from outside the actor system (see
// docs/references/goakt-llms-full.txt, "Messaging from outside").
type actorGossipBridge struct {
	ps       *pubsub.PubSub
	system   goakt.ActorSystem
	topic    *pubsub.Topic
	log      *zap.Logger
	threadID string
	publish  *appactors.Executor
}

// newActorGossipBridge joins the thread's consensus topic. Call Run(ctx) to
// start the receive loop.
func newActorGossipBridge(ctx context.Context, ps *pubsub.PubSub, system goakt.ActorSystem, h *appactors.Hierarchy, parent *goakt.PID, th *pb.Thread, log *zap.Logger) (*actorGossipBridge, error) {
	t, err := joinConsensusTopic(ps, th)
	if err != nil {
		return nil, err
	}
	g := &actorGossipBridge{ps: ps, system: system, topic: t, log: log, threadID: th.Id}
	if h != nil && parent != nil {
		g.publish, err = appactors.NewExecutorUnder(ctx, h, parent, "thread-gossip-"+th.Id, goakt.WithLongLived())
		if err != nil {
			_ = ps.UnregisterTopicValidator(ConsensusTopic(th.Id))
			_ = t.Close()
			return nil, fmt.Errorf("spawn gossip publisher: %w", err)
		}
	}
	return g, nil
}

// PublishFunc returns a function that publishes a ConsensusMsg to remote
// replicas over GossipSub, asynchronously so the caller (ThreadActor's
// Receive, via bcast) never blocks waiting for mesh peers — mirrors
// thread.GossipBridge.BroadcastFunc's async publish.
func (g *actorGossipBridge) PublishFunc() func(*pb.ConsensusMsg) {
	return func(msg *pb.ConsensusMsg) {
		data, err := proto.Marshal(msg)
		if err != nil {
			g.log.Warn("actors: marshal consensus msg", zap.Error(err))
			return
		}
		work := func() (any, error) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return nil, g.topic.Publish(ctx, data)
		}
		if g.publish != nil {
			if err := g.publish.Cast(context.Background(), work); err != nil {
				g.log.Warn("actors: enqueue consensus msg", zap.String("thread", g.threadID), zap.Error(err))
			}
			return
		}
		// Tests may construct the supervisor without the daemon hierarchy. Keep
		// that compatibility path bounded; production always uses the executor.
		select {
		case consensusPublishSlots <- struct{}{}:
			go func() { defer func() { <-consensusPublishSlots }(); _, _ = work() }()
		default:
			g.log.Warn("actors: publish queue full", zap.String("thread", g.threadID))
		}
	}
}

// Run subscribes to GossipSub and Tells inbound messages to the ThreadActor
// PID. Blocks until ctx is cancelled; run it in its own goroutine.
func (g *actorGossipBridge) Run(ctx context.Context, pid *goakt.PID) {
	if g.publish != nil {
		defer func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = g.system.Kill(stopCtx, g.publish.PID().Name())
		}()
	}
	defer g.ps.UnregisterTopicValidator(ConsensusTopic(g.threadID)) //nolint:errcheck
	defer g.topic.Close()                                           //nolint:errcheck
	sub, err := g.topic.Subscribe()
	if err != nil {
		g.log.Error("actors: subscribe gossipsub topic",
			zap.String("thread", g.threadID),
			zap.Error(err),
		)
		return
	}
	defer sub.Cancel()

	for {
		gmsg, err := sub.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			g.log.Warn("actors: gossipsub recv",
				zap.String("thread", g.threadID),
				zap.Error(err),
			)
			continue
		}
		cm, err := decodeConsensusMsg(g.threadID, gmsg.Data)
		if err != nil {
			g.log.Warn("actors: unmarshal consensus msg", zap.Error(err))
			continue
		}
		if cm == nil {
			continue
		}
		if err := g.system.NoSender().Tell(ctx, pid, cm); err != nil {
			g.log.Debug("actors: tell consensus msg", zap.Error(err))
		}
	}
}
