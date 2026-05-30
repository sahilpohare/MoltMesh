package inbox

import (
	"testing"
	"time"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

func newTestInbox(t *testing.T) *Inbox {
	t.Helper()
	ib, err := New(":memory:")
	if err != nil {
		t.Fatalf("New inbox: %v", err)
	}
	t.Cleanup(func() { ib.Close() })
	return ib
}

func makeMsg(id, from, thread, task string) *pb.Message {
	return &pb.Message{
		Id:       id,
		FromDid:  from,
		ThreadId: thread,
		TaskId:   task,
		Kind:     pb.MessageKind_MESSAGE_KIND_TEXT,
	}
}

func TestPutAndGet(t *testing.T) {
	ib := newTestInbox(t)

	msg := makeMsg("msg-1", "did:key:zA", "thread-1", "")
	if err := ib.Put(msg); err != nil {
		t.Fatalf("Put: %v", err)
	}

	msgs, err := ib.Get("", "", false, 0, 0)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Id != "msg-1" {
		t.Errorf("message ID mismatch: got %q", msgs[0].Id)
	}
}

func TestPut_Idempotent(t *testing.T) {
	ib := newTestInbox(t)
	msg := makeMsg("msg-dup", "did:key:zA", "thread-1", "")
	if err := ib.Put(msg); err != nil {
		t.Fatal(err)
	}
	// second put of same ID must be a no-op (INSERT OR IGNORE)
	if err := ib.Put(msg); err != nil {
		t.Fatalf("second Put: %v", err)
	}
	msgs, _ := ib.Get("", "", false, 0, 0)
	if len(msgs) != 1 {
		t.Errorf("expected 1 message after duplicate put, got %d", len(msgs))
	}
}

func TestGetByThread(t *testing.T) {
	ib := newTestInbox(t)
	ib.Put(makeMsg("a", "did:key:z1", "thread-A", ""))
	ib.Put(makeMsg("b", "did:key:z1", "thread-B", ""))
	ib.Put(makeMsg("c", "did:key:z1", "thread-A", ""))

	msgs, err := ib.Get("thread-A", "", false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Errorf("expected 2 messages in thread-A, got %d", len(msgs))
	}
}

func TestGetByTask(t *testing.T) {
	ib := newTestInbox(t)
	ib.Put(makeMsg("a", "did:key:z1", "", "task-1"))
	ib.Put(makeMsg("b", "did:key:z1", "", "task-2"))

	msgs, err := ib.Get("", "task-1", false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 || msgs[0].TaskId != "task-1" {
		t.Errorf("unexpected result: %+v", msgs)
	}
}

func TestGetLimit(t *testing.T) {
	ib := newTestInbox(t)
	for i := 0; i < 10; i++ {
		ib.Put(makeMsg(string(rune('a'+i)), "did:key:z1", "t", ""))
	}
	msgs, _ := ib.Get("", "", false, 3, 0)
	if len(msgs) != 3 {
		t.Errorf("expected 3 with limit, got %d", len(msgs))
	}
}

func TestAckAndUnreadFilter(t *testing.T) {
	ib := newTestInbox(t)
	ib.Put(makeMsg("msg-read", "did:key:z1", "", ""))
	ib.Put(makeMsg("msg-unread", "did:key:z1", "", ""))

	if err := ib.Ack("msg-read"); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	unread, _ := ib.Get("", "", true, 0, 0)
	if len(unread) != 1 {
		t.Errorf("expected 1 unread, got %d", len(unread))
	}
	if unread[0].Id != "msg-unread" {
		t.Errorf("wrong unread message: %q", unread[0].Id)
	}
}

func TestGetSince(t *testing.T) {
	ib := newTestInbox(t)

	// put a message, record time, put another
	ib.Put(makeMsg("old", "did:key:z1", "", ""))

	// get timestamp after first message; sleep to ensure "new" gets a later ms
	var ts int64
	ib.db.QueryRow(`SELECT received_at FROM inbox WHERE id = 'old'`).Scan(&ts)
	time.Sleep(2 * time.Millisecond)

	ib.Put(makeMsg("new", "did:key:z1", "", ""))

	msgs, _ := ib.Get("", "", false, 0, ts)
	if len(msgs) != 1 || msgs[0].Id != "new" {
		t.Errorf("since filter failed: got %+v", msgs)
	}
}
