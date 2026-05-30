package outbox

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"

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
