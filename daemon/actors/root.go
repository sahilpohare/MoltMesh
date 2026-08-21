package actors

import (
	"context"
	"fmt"
	"sync"

	"github.com/tochemey/goakt/v4/actor"
)

// RootActor is the daemon's supervision root. Domain supervisors are spawned
// as its children, so stopping or inspecting the root covers the complete
// application actor tree rather than a collection of unrelated root actors.
type RootActor struct{}

func (*RootActor) PreStart(*actor.Context) error     { return nil }
func (*RootActor) PostStop(*actor.Context) error     { return nil }
func (*RootActor) Receive(ctx *actor.ReceiveContext) { ctx.Unhandled() }

var _ actor.Actor = (*RootActor)(nil)

// ContainerActor is a state-free supervision node for a domain subtree.
type ContainerActor struct{ RootActor }

// Hierarchy owns the root PID and serializes child creation by logical name.
type Hierarchy struct {
	system   actor.ActorSystem
	root     *actor.PID
	mu       sync.Mutex
	children map[string]*actor.PID
}

func (s *System) NewHierarchy(ctx context.Context) (*Hierarchy, error) {
	root, err := s.actorSystem.Spawn(ctx, "daemon-supervisor", &RootActor{}, actor.WithLongLived())
	if err != nil {
		return nil, fmt.Errorf("actors: spawn daemon supervisor: %w", err)
	}
	return &Hierarchy{system: s.actorSystem, root: root, children: make(map[string]*actor.PID)}, nil
}

func (h *Hierarchy) Spawn(ctx context.Context, name string, child actor.Actor, opts ...actor.SpawnOption) (*actor.PID, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if pid := h.children[name]; pid != nil {
		if pid.IsRunning() {
			return pid, nil
		}
		delete(h.children, name)
	}
	pid, err := h.root.SpawnChild(ctx, name, child, opts...)
	if err != nil {
		return nil, fmt.Errorf("actors: spawn child %s: %w", name, err)
	}
	h.children[name] = pid
	return pid, nil
}

func (h *Hierarchy) Root() *actor.PID { return h.root }

func (h *Hierarchy) SpawnUnder(ctx context.Context, parent *actor.PID, name string, child actor.Actor, opts ...actor.SpawnOption) (*actor.PID, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	key := parent.Name() + "/" + name
	if pid := h.children[key]; pid != nil {
		if pid.IsRunning() {
			return pid, nil
		}
		delete(h.children, key)
	}
	pid, err := parent.SpawnChild(ctx, name, child, opts...)
	if err != nil {
		return nil, fmt.Errorf("actors: spawn child %s: %w", key, err)
	}
	h.children[key] = pid
	return pid, nil
}
