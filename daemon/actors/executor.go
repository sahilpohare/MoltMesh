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

// NewExecutor spawns the one, persistent SerialActor for a whole domain
// (registry, inbox, outbox, tasks, ...) — it must outlive GoAkt's default
// 2-minute passivation timeout, since nothing ever re-spawns it on demand
// the way durable Thread actors do. Per-entity actors that genuinely should
// passivate when idle (a specific task or peer connection) go through
// NewExecutorUnder instead, which does not add this option.
func NewExecutor(ctx context.Context, h *Hierarchy, name string, opts ...actor.SpawnOption) (*Executor, error) {
	pid, err := h.Spawn(ctx, name, &SerialActor{}, append(opts, actor.WithLongLived())...)
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

// EnableSerialActor is the shape every domain package's EnableActor method
// reduces to: spawn the one persistent SerialActor for this domain, hand it
// to the caller via assign (so it can be stored on the domain's own struct),
// and optionally register periodic work on it via schedule. Domains with
// nothing to run on a timer (inbox, webhook, networks, gossip) pass a nil
// schedule; domains that republish or flush periodically (registry, names,
// outbox) pass one. This is the one place that pattern is written down —
// every EnableActor body used to hand-copy it.
func EnableSerialActor(ctx context.Context, h *Hierarchy, name string, assign func(*Executor), schedule func(*Executor) error) error {
	exec, err := NewExecutor(ctx, h, name)
	if err != nil {
		return err
	}
	assign(exec)
	if schedule == nil {
		return nil
	}
	return schedule(exec)
}

// Dispatch, DispatchErr, and DispatchVoid are the three shapes every domain
// package's public methods repeated by hand at every call site: "if this
// actor is running, route the work through it (context.Background(), since
// these are synchronous SDK-facing calls with no caller deadline to
// propagate); otherwise just call it directly" — the same fallback NewExecutor
// itself documents for when no actor system is wired up at all (tests,
// mostly). exec may be nil, matching Executor.Call's own nil-receiver check.

// Dispatch runs work through exec if present, returning its (value, error).
func Dispatch[T any](exec *Executor, work func() (T, error)) (T, error) {
	if exec == nil {
		return work()
	}
	value, err := exec.Call(context.Background(), func() (any, error) { return work() })
	if err != nil {
		var zero T
		return zero, err
	}
	return value.(T), nil
}

// DispatchErr runs work through exec if present, returning only its error.
func DispatchErr(exec *Executor, work func() error) error {
	if exec == nil {
		return work()
	}
	_, err := exec.Call(context.Background(), func() (any, error) { return nil, work() })
	return err
}

// DispatchVoid runs work through exec if present and waits for it to finish,
// discarding any error — for fire-and-forget cleanup calls (e.g. unsubscribe)
// that have never had an error path their callers could act on.
func DispatchVoid(exec *Executor, work func()) {
	if exec == nil {
		work()
		return
	}
	_, _ = exec.Call(context.Background(), func() (any, error) { work(); return nil, nil })
}

// DispatchCast is DispatchVoid's non-blocking sibling: it queues work on
// exec via Cast and returns immediately rather than waiting for it to run,
// for callers (webhook delivery, thread block publishing) that only need
// ordering against the actor's other queued work, not a synchronous result.
func DispatchCast(exec *Executor, work func()) {
	if exec == nil {
		work()
		return
	}
	_ = exec.Cast(context.Background(), func() (any, error) { work(); return nil, nil })
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
