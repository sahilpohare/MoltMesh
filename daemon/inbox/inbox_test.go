package inbox

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/sqlite"
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

func TestMigratesLegacyInboxBeforeOwnerIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE inbox (id TEXT PRIMARY KEY, from_did TEXT NOT NULL, thread_id TEXT NOT NULL DEFAULT '', task_id TEXT NOT NULL DEFAULT '', payload BLOB NOT NULL, received_at INTEGER NOT NULL, read_at INTEGER)`)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	ib, err := New(path)
	if err != nil {
		t.Fatalf("legacy migration: %v", err)
	}
	defer ib.Close()
	var owner string
	if err := ib.db.QueryRow(`SELECT owner_did FROM inbox LIMIT 1`).Scan(&owner); err != sql.ErrNoRows {
		t.Fatalf("owner column query = %v", err)
	}
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
	if err := ib.PutForOwner("", msg); err != nil {
		t.Fatalf("Put: %v", err)
	}

	msgs, err := ib.GetForOwner("", "", "", false, 0, 0)
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
	if err := ib.PutForOwner("", msg); err != nil {
		t.Fatal(err)
	}
	// second put of same ID must be a no-op (INSERT OR IGNORE)
	if err := ib.PutForOwner("", msg); err != nil {
		t.Fatalf("second Put: %v", err)
	}
	msgs, _ := ib.GetForOwner("", "", "", false, 0, 0)
	if len(msgs) != 1 {
		t.Errorf("expected 1 message after duplicate put, got %d", len(msgs))
	}
}

func TestGetByThread(t *testing.T) {
	ib := newTestInbox(t)
	ib.PutForOwner("", makeMsg("a", "did:key:z1", "thread-A", ""))
	ib.PutForOwner("", makeMsg("b", "did:key:z1", "thread-B", ""))
	ib.PutForOwner("", makeMsg("c", "did:key:z1", "thread-A", ""))

	msgs, err := ib.GetForOwner("", "thread-A", "", false, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Errorf("expected 2 messages in thread-A, got %d", len(msgs))
	}
}

func TestGetByTask(t *testing.T) {
	ib := newTestInbox(t)
	ib.PutForOwner("", makeMsg("a", "did:key:z1", "", "task-1"))
	ib.PutForOwner("", makeMsg("b", "did:key:z1", "", "task-2"))

	msgs, err := ib.GetForOwner("", "", "task-1", false, 0, 0)
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
		ib.PutForOwner("", makeMsg(string(rune('a'+i)), "did:key:z1", "t", ""))
	}
	msgs, _ := ib.GetForOwner("", "", "", false, 3, 0)
	if len(msgs) != 3 {
		t.Errorf("expected 3 with limit, got %d", len(msgs))
	}
}

func TestAckAndUnreadFilter(t *testing.T) {
	ib := newTestInbox(t)
	ib.PutForOwner("", makeMsg("msg-read", "did:key:z1", "", ""))
	ib.PutForOwner("", makeMsg("msg-unread", "did:key:z1", "", ""))

	if err := ib.AckForOwner("", "msg-read"); err != nil {
		t.Fatalf("Ack: %v", err)
	}

	unread, _ := ib.GetForOwner("", "", "", true, 0, 0)
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
	ib.PutForOwner("", makeMsg("old", "did:key:z1", "", ""))

	// get timestamp after first message; sleep to ensure "new" gets a later ms
	var ts int64
	ib.db.QueryRow(`SELECT received_at FROM inbox WHERE id = 'old'`).Scan(&ts)
	time.Sleep(2 * time.Millisecond)

	ib.PutForOwner("", makeMsg("new", "did:key:z1", "", ""))

	msgs, _ := ib.GetForOwner("", "", "", false, 0, ts)
	if len(msgs) != 1 || msgs[0].Id != "new" {
		t.Errorf("since filter failed: got %+v", msgs)
	}
}

func TestOwnerNamespacesDoNotLeakMessagesOrAcknowledgements(t *testing.T) {
	ib := newTestInbox(t)
	if err := ib.PutForOwner("did:key:zAgentA", makeMsg("a", "did:key:zRemote", "", "")); err != nil {
		t.Fatal(err)
	}
	if err := ib.PutForOwner("did:key:zAgentB", makeMsg("b", "did:key:zRemote", "", "")); err != nil {
		t.Fatal(err)
	}
	a, err := ib.GetForOwner("did:key:zAgentA", "", "", false, 0, 0)
	if err != nil || len(a) != 1 || a[0].Id != "a" {
		t.Fatalf("agent A inbox = %#v, %v", a, err)
	}
	b, err := ib.GetForOwner("did:key:zAgentB", "", "", false, 0, 0)
	if err != nil || len(b) != 1 || b[0].Id != "b" {
		t.Fatalf("agent B inbox = %#v, %v", b, err)
	}
	if err := ib.AckForOwner("did:key:zAgentA", "b"); err != nil {
		t.Fatal(err)
	}
	unread, err := ib.GetForOwner("did:key:zAgentB", "", "", true, 0, 0)
	if err != nil || len(unread) != 1 || unread[0].Id != "b" {
		t.Fatalf("cross-owner acknowledgement leaked: %#v, %v", unread, err)
	}
}
