package deliver

import (
	"context"
	"sync"
	"testing"

	"github.com/libp2p/go-libp2p"
	"go.uber.org/zap"

	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/daemon/inbox"
)

// A per-peer executor is passivated when its last stream releases it:
// releaseExecutor drops the map entry, unlocks, then kills the actor by name.
// The next stream from that peer must still get a working executor, even if
// it arrives while that shutdown is in flight. When it does not, the receive
// handler resets the stream without writing an ack byte, the sender logs
// "read ack: EOF", and the message waits for the outbox retry schedule
// instead of being handled now.
func TestExecutorReacquireAfterRelease(t *testing.T) {
	ctx := context.Background()
	h, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"))
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()
	ib, err := inbox.New(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer ib.Close()
	d := New(h, nil, ib, nil, nil, zap.NewNop())
	actorSystem, err := appactors.NewSystem(ctx, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer actorSystem.Stop(ctx)
	hierarchy, err := actorSystem.NewHierarchy(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.EnableActor(hierarchy); err != nil {
		t.Fatal(err)
	}
	pid := h.ID()
	// The daemon releases on one stream's goroutine while the next stream
	// from the same peer acquires on another, so overlap them deliberately.
	for i := 0; i < 200; i++ {
		e1, err := d.executor(ctx, pid)
		if err != nil {
			t.Fatalf("iter %d: first acquire: %v", i, err)
		}
		var (
			e2     *appactors.Executor
			acqErr error
			wg     sync.WaitGroup
			start  = make(chan struct{})
		)
		wg.Add(2)
		go func() { defer wg.Done(); <-start; d.releaseExecutor(pid, e1) }()
		go func() { defer wg.Done(); <-start; e2, acqErr = d.executor(ctx, pid) }()
		close(start)
		wg.Wait()
		if acqErr != nil {
			t.Fatalf("iter %d: acquire concurrent with passivation: %v", i, acqErr)
		}
		ran := false
		if err := appactors.DispatchErr(e2, func() error { ran = true; return nil }); err != nil {
			t.Fatalf("iter %d: dispatch on concurrently acquired executor: %v", i, err)
		}
		if !ran {
			t.Fatalf("iter %d: work did not run", i)
		}
		d.releaseExecutor(pid, e2)
	}

}
