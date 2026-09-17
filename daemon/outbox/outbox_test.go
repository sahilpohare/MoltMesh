package outbox

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

func newTestOutbox(t *testing.T, deliver DeliverFunc) *Outbox {
	t.Helper()
	log, _ := zap.NewDevelopment()
	ob, err := New(":memory:", deliver, log)
	if err != nil {
		t.Fatalf("New outbox: %v", err)
	}
	t.Cleanup(func() { ob.Close() })
	return ob
}

func makeMsg(id, to string) *pb.Message {
	return &pb.Message{Id: id, ToDid: to, Kind: pb.MessageKind_MESSAGE_KIND_TEXT}
}

func TestEnqueueAndList(t *testing.T) {
	ob := newTestOutbox(t, func(_ context.Context, _ *pb.Message) error {
		return nil
	})

	if err := ob.Enqueue(makeMsg("msg-1", "did:key:zA")); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	msgs, err := ob.List("pending", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].Id != "msg-1" {
		t.Errorf("unexpected list: %+v", msgs)
	}
}

func TestEnqueue_Idempotent(t *testing.T) {
	ob := newTestOutbox(t, func(_ context.Context, _ *pb.Message) error { return nil })
	msg := makeMsg("dup", "did:key:zA")
	ob.Enqueue(msg)
	ob.Enqueue(msg) // second enqueue same ID — INSERT OR IGNORE

	msgs, _ := ob.List("pending", 0)
	if len(msgs) != 1 {
		t.Errorf("expected 1 after duplicate enqueue, got %d", len(msgs))
	}
}

func TestMarkDelivered(t *testing.T) {
	ob := newTestOutbox(t, func(_ context.Context, _ *pb.Message) error { return nil })
	ob.Enqueue(makeMsg("msg-x", "did:key:zA"))
	if err := ob.MarkDelivered("msg-x"); err != nil {
		t.Fatal(err)
	}
	delivered, _ := ob.List("delivered", 0)
	if len(delivered) != 1 {
		t.Errorf("expected 1 delivered, got %d", len(delivered))
	}
	pending, _ := ob.List("pending", 0)
	if len(pending) != 0 {
		t.Errorf("expected 0 pending after deliver, got %d", len(pending))
	}
}

func TestFlush_SuccessfulDelivery(t *testing.T) {
	var delivered atomic.Int32
	ob := newTestOutbox(t, func(_ context.Context, msg *pb.Message) error {
		delivered.Add(1)
		return nil
	})

	ob.Enqueue(makeMsg("m1", "did:key:zA"))
	ob.Enqueue(makeMsg("m2", "did:key:zB"))

	ob.flush(context.Background())

	if delivered.Load() != 2 {
		t.Errorf("expected 2 deliveries, got %d", delivered.Load())
	}

	pending, _ := ob.List("pending", 0)
	if len(pending) != 0 {
		t.Errorf("expected 0 pending after flush, got %d", len(pending))
	}
}

func TestFlush_FailedDelivery_Retries(t *testing.T) {
	var attempts atomic.Int32
	ob := newTestOutbox(t, func(_ context.Context, _ *pb.Message) error {
		attempts.Add(1)
		return context.DeadlineExceeded
	})

	ob.Enqueue(makeMsg("m-fail", "did:key:zA"))

	// first flush — should attempt delivery, fail, stay pending
	ob.flush(context.Background())
	if attempts.Load() != 1 {
		t.Errorf("expected 1 attempt, got %d", attempts.Load())
	}

	// backoff: last_attempt is set, so second flush within retryBaseDelay won't retry
	ob.flush(context.Background())
	if attempts.Load() != 1 {
		t.Errorf("expected still 1 attempt due to backoff, got %d", attempts.Load())
	}

	pending, _ := ob.List("pending", 0)
	if len(pending) != 1 {
		t.Errorf("expected message to remain pending, got %d", len(pending))
	}
}

func TestFlush_MaxAttempts_MarkedFailed(t *testing.T) {
	ob := newTestOutbox(t, func(_ context.Context, _ *pb.Message) error {
		return context.DeadlineExceeded
	})

	ob.Enqueue(makeMsg("m-max", "did:key:zA"))

	// force attempts to maxAttempts-1 directly in DB
	ob.db.Exec(`UPDATE outbox SET attempts = ?, last_attempt = 0 WHERE id = 'm-max'`, maxAttempts-1)

	ob.flush(context.Background())

	failed, _ := ob.List("failed", 0)
	if len(failed) != 1 {
		t.Errorf("expected 1 failed, got %d", len(failed))
	}
}

func TestFlush_ExpiredMessages(t *testing.T) {
	ob := newTestOutbox(t, func(_ context.Context, _ *pb.Message) error { return nil })
	ob.Enqueue(makeMsg("m-expire", "did:key:zA"))

	// set expires_at to past
	ob.db.Exec(`UPDATE outbox SET expires_at = ? WHERE id = 'm-expire'`,
		time.Now().Add(-1*time.Hour).UnixMilli())

	ob.flush(context.Background())

	expired, _ := ob.List("expired", 0)
	if len(expired) != 1 {
		t.Errorf("expected 1 expired, got %d", len(expired))
	}
}

func TestThreadWakeNeverExpiresOrHitsAttemptCeiling(t *testing.T) {
	ob := newTestOutbox(t, func(_ context.Context, _ *pb.Message) error { return context.DeadlineExceeded })
	msg := makeMsg("wake", "did:key:zA")
	msg.Kind = pb.MessageKind_MESSAGE_KIND_THREAD_INVITE
	msg.ThreadId = "thread-1"
	if err := ob.Enqueue(msg); err != nil {
		t.Fatal(err)
	}
	ob.db.Exec(`UPDATE outbox SET attempts = ?, last_attempt = 0, expires_at = 0 WHERE id = ?`, maxAttempts-1, msg.Id)
	ob.flush(context.Background())
	pending, err := ob.List("pending", 0)
	if err != nil || len(pending) != 1 {
		t.Fatalf("durable wake did not remain pending: len=%d err=%v", len(pending), err)
	}
}

func TestMigrationAddsDurableColumn(t *testing.T) {
	path := t.TempDir() + "/outbox.db"
	ob, err := New(path, func(context.Context, *pb.Message) error { return nil }, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	msg := makeMsg("legacy-wake", "did:key:zA")
	msg.Kind = pb.MessageKind_MESSAGE_KIND_THREAD_INVITE
	if err := ob.Enqueue(msg); err != nil {
		t.Fatal(err)
	}
	if _, err := ob.db.Exec(`UPDATE outbox SET status = 'failed' WHERE id = ?`, msg.Id); err != nil {
		t.Fatal(err)
	}
	if _, err := ob.db.Exec(`ALTER TABLE outbox DROP COLUMN durable`); err != nil {
		t.Fatal(err)
	}
	ob.Close()
	ob, err = New(path, func(context.Context, *pb.Message) error { return nil }, zap.NewNop())
	if err != nil {
		t.Fatalf("reopen legacy outbox: %v", err)
	}
	defer ob.Close()
	var durable int
	var status string
	if err := ob.db.QueryRow(`SELECT durable, status FROM outbox WHERE id = ?`, msg.Id).Scan(&durable, &status); err != nil {
		t.Fatal(err)
	}
	if durable != 1 || status != "pending" {
		t.Fatalf("legacy wake was not recovered: durable=%d status=%s", durable, status)
	}
}

func TestListLimit(t *testing.T) {
	ob := newTestOutbox(t, func(_ context.Context, _ *pb.Message) error { return nil })
	for i := 0; i < 5; i++ {
		ob.Enqueue(makeMsg(string(rune('a'+i)), "did:key:zA"))
	}
	msgs, _ := ob.List("pending", 3)
	if len(msgs) != 3 {
		t.Errorf("expected 3 with limit, got %d", len(msgs))
	}
}

func TestOwnerNamespacesDoNotLeakOutgoingOperations(t *testing.T) {
	ob := newTestOutbox(t, func(_ context.Context, _ *pb.Message) error { return nil })
	if err := ob.EnqueueForOwner("did:key:zAgentA", makeMsg("a", "did:key:zRemote")); err != nil {
		t.Fatal(err)
	}
	if err := ob.EnqueueForOwner("did:key:zAgentB", makeMsg("b", "did:key:zRemote")); err != nil {
		t.Fatal(err)
	}
	a, err := ob.ListForOwner("did:key:zAgentA", "pending", 0)
	if err != nil || len(a) != 1 || a[0].Id != "a" {
		t.Fatalf("agent A outbox = %#v, %v", a, err)
	}
	b, err := ob.ListForOwner("did:key:zAgentB", "pending", 0)
	if err != nil || len(b) != 1 || b[0].Id != "b" {
		t.Fatalf("agent B outbox = %#v, %v", b, err)
	}
}

// Enqueue must not wait on delivery. flush runs on the outbox actor and makes
// a blocking network call per pending message, so routing the insert through
// that same actor made EnqueueForOwner wait behind every in-flight attempt:
// CreateTask, which only needs a local INSERT, took as long as the DHT took to
// give up on unreachable recipients, and exceeded a 30s RPC deadline on CI.
func TestEnqueueDoesNotBlockOnSlowDelivery(t *testing.T) {
	releaseDelivery := make(chan struct{})
	deliveryStarted := make(chan struct{}, 1)
	ob := newTestOutbox(t, func(ctx context.Context, msg *pb.Message) error {
		select {
		case deliveryStarted <- struct{}{}:
		default:
		}
		<-releaseDelivery // a peer that never answers
		return nil
	})
	defer close(releaseDelivery)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sys, err := appactors.NewSystem(ctx, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer sys.Stop(context.Background())
	h, err := sys.NewHierarchy(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := ob.EnableActor(ctx, h); err != nil {
		t.Fatal(err)
	}

	// First message wedges the actor inside deliver.
	if err := ob.Enqueue(&pb.Message{Id: "stuck", FromDid: "did:key:zA", ToDid: "did:key:zUnreachable"}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-deliveryStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("delivery never started; test cannot exercise the contention")
	}

	// A second enqueue must still return promptly.
	done := make(chan error, 1)
	go func() { done <- ob.Enqueue(&pb.Message{Id: "second", FromDid: "did:key:zA", ToDid: "did:key:zB"}) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("second enqueue: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("enqueue blocked behind an in-flight delivery")
	}
}
