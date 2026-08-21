// Package actors hosts the daemon's single GoAkt ActorSystem. Domain-specific
// actors and supervisors (e.g. daemon/thread's ThreadActor/ThreadSupervisor)
// live in their own domain packages and import System from here, rather than
// living in this package — this package stays generic scaffolding shared by
// every actor in the process, not a home for any one domain's actors.
//
// No Discovery/clustering config is used here: one ActorSystem per daemon
// process, single-node. Cross-daemon communication stays on the existing
// libp2p/DHT/GossipSub transport, unchanged — GoAkt is purely an in-process
// concurrency/supervision primitive in this project, not a clustering layer.
package actors

import (
	"context"
	"fmt"
	"time"

	"github.com/tochemey/goakt/v4/actor"
	golog "github.com/tochemey/goakt/v4/log"
	"github.com/tochemey/goakt/v4/supervisor"
	"go.uber.org/zap"
)

// System wraps a single GoAkt ActorSystem for the daemon process.
type System struct {
	actorSystem actor.ActorSystem
	log         *zap.Logger
}

// NewSystem creates and starts the daemon's ActorSystem.
func NewSystem(ctx context.Context, log *zap.Logger) (*System, error) {
	defaultSupervisor := supervisor.NewSupervisor(
		supervisor.WithStrategy(supervisor.OneForOneStrategy),
		supervisor.WithRetry(3, time.Minute),
		supervisor.WithExponentialBackoff(100*time.Millisecond, 5*time.Second, time.Minute),
		supervisor.WithAnyErrorDirective(supervisor.RestartDirective),
	)
	as, err := actor.NewActorSystem(
		"moltmesh",
		actor.WithLogger(golog.NewZapFrom(log)),
		actor.WithDefaultSupervisor(defaultSupervisor),
	)
	if err != nil {
		return nil, fmt.Errorf("actors: new actor system: %w", err)
	}
	if err := as.Start(ctx); err != nil {
		return nil, fmt.Errorf("actors: start actor system: %w", err)
	}
	return &System{actorSystem: as, log: log}, nil
}

// ActorSystem returns the underlying GoAkt ActorSystem.
func (s *System) ActorSystem() actor.ActorSystem { return s.actorSystem }

// Stop shuts down the actor system. Safe to call once during daemon shutdown.
func (s *System) Stop(ctx context.Context) error {
	return s.actorSystem.Stop(ctx)
}
