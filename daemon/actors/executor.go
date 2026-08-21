package actors

import (
	"context"
	"fmt"
	"time"

	"github.com/tochemey/goakt/v4/actor"
)

// Result is returned by a serial service actor operation.
type Result struct {
	Value any
	Err   error
}

type operation struct {
	work  func() (any, error)
	reply chan Result
}

type completed struct {
	op     operation
	result Result
}

// SerialActor owns the ordering of a blocking service. Work is executed with
// PipeTo, never on a dispatcher worker, and only one operation is in flight.
type SerialActor struct {
	queue []operation
	busy  bool
}

func (*SerialActor) PreStart(*actor.Context) error { return nil }
func (*SerialActor) PostStop(*actor.Context) error { return nil }
func (a *SerialActor) Receive(ctx *actor.ReceiveContext) {
	switch msg := ctx.Message().(type) {
	case operation:
		a.queue = append(a.queue, msg)
		a.start(ctx)
	case completed:
		a.busy = false
		if msg.op.reply != nil {
			msg.op.reply <- msg.result
		}
		a.start(ctx)
	default:
		ctx.Unhandled()
	}
}

func (e *Executor) Cast(ctx context.Context, work func() (any, error)) error {
	return e.system.NoSender().Tell(ctx, e.pid, operation{work: work})
}

func (a *SerialActor) start(ctx *actor.ReceiveContext) {
	if a.busy || len(a.queue) == 0 {
		return
	}
	op := a.queue[0]
	a.queue = a.queue[1:]
	a.busy = true
	ctx.PipeTo(ctx.Self(), func() (any, error) {
		value, err := op.work()
		return completed{op: op, result: Result{Value: value, Err: err}}, nil
	})
}

var _ actor.Actor = (*SerialActor)(nil)

// Executor is the external façade for one supervised SerialActor.
type Executor struct {
	system actor.ActorSystem
	pid    *actor.PID
}

func NewExecutor(ctx context.Context, h *Hierarchy, name string, opts ...actor.SpawnOption) (*Executor, error) {
	pid, err := h.Spawn(ctx, name, &SerialActor{}, opts...)
	if err != nil {
		return nil, err
	}
	return &Executor{system: h.system, pid: pid}, nil
}

func NewExecutorUnder(ctx context.Context, h *Hierarchy, parent *actor.PID, name string, opts ...actor.SpawnOption) (*Executor, error) {
	pid, err := h.SpawnUnder(ctx, parent, name, &SerialActor{}, opts...)
	if err != nil {
		return nil, err
	}
	return &Executor{system: h.system, pid: pid}, nil
}

func (e *Executor) Call(ctx context.Context, work func() (any, error)) (any, error) {
	if e == nil || e.pid == nil {
		return nil, fmt.Errorf("actor executor is not started")
	}
	reply := make(chan Result, 1)
	if err := e.system.NoSender().Tell(ctx, e.pid, operation{work: work, reply: reply}); err != nil {
		return nil, err
	}
	select {
	case result := <-reply:
		return result.Value, result.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (e *Executor) PID() *actor.PID { return e.pid }

func (e *Executor) Stop(ctx context.Context) error {
	if e == nil || e.pid == nil || !e.pid.IsRunning() {
		return nil
	}
	return e.system.Kill(ctx, e.pid.Name())
}

func (e *Executor) Schedule(ctx context.Context, reference string, interval time.Duration, work func() (any, error)) error {
	return e.system.Schedule(ctx, operation{work: work}, e.pid, interval, actor.WithReference(reference))
}
