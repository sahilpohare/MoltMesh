package inbox

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/pkg/sqlite"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"google.golang.org/protobuf/proto"
)

// Inbox is a persistent SQLite-backed message queue with live push.
type Inbox struct {
	db   *sql.DB
	mu   sync.Mutex
	subs []subscriber
	exec *appactors.Executor
}

type subscriber struct {
	ownerDID string
	ch       chan *pb.Message
}

// EnableActor moves all inbox persistence and subscriber mutation behind a
// supervised serial actor while preserving the existing Inbox API.
func (b *Inbox) EnableActor(ctx context.Context, h *appactors.Hierarchy) error {
	return appactors.EnableSerialActor(ctx, h, "inbox", func(e *appactors.Executor) { b.exec = e }, nil)
}

// New opens (or creates) the inbox database at the given path.
func New(path string) (*Inbox, error) {
	db, err := sqlite.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open inbox db: %w", err)
	}
	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("migrate inbox: %w", err)
	}
	return &Inbox{db: db}, nil
}

// PutForOwner durably assigns an incoming message to its local SDK-agent
// namespace and notifies live subscribers. The DB write is synchronous (so
// we never ACK before persisting), but subscriber fan-out is async to
// unblock the network stream. Callers that don't need per-owner scoping
// (a single-agent daemon) can just pass msg.ToDid, its natural owner.
func (b *Inbox) PutForOwner(ownerDID string, msg *pb.Message) error {
	return appactors.DispatchErr(b.exec, func() error { return b.put(ownerDID, msg) })
}

func (b *Inbox) put(ownerDID string, msg *pb.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	_, err = b.db.Exec(`
		INSERT OR IGNORE INTO inbox (id, owner_did, from_did, thread_id, task_id, payload, received_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`,
		msg.Id, ownerDID, msg.FromDid, msg.ThreadId, msg.TaskId, data, time.Now().UnixMilli(),
	)
	if err != nil {
		return err
	}
	b.notify(ownerDID, msg)
	return nil
}

// SubscribeForOwner registers a channel to receive new messages as they
// arrive, scoped to ownerDID ("" for all of them). The caller must call
// Unsubscribe when done to avoid a goroutine leak.
func (b *Inbox) SubscribeForOwner(ownerDID string) chan *pb.Message {
	ch, _ := appactors.Dispatch(b.exec, func() (chan *pb.Message, error) { return b.subscribe(ownerDID), nil })
	return ch
}

func (b *Inbox) subscribe(ownerDID string) chan *pb.Message {
	ch := make(chan *pb.Message, 64)
	b.mu.Lock()
	b.subs = append(b.subs, subscriber{ownerDID: ownerDID, ch: ch})
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes and closes a previously subscribed channel.
func (b *Inbox) Unsubscribe(ch chan *pb.Message) {
	appactors.DispatchVoid(b.exec, func() { b.unsubscribe(ch) })
}

func (b *Inbox) unsubscribe(ch chan *pb.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, s := range b.subs {
		if s.ch == ch {
			b.subs = append(b.subs[:i], b.subs[i+1:]...)
			close(ch)
			return
		}
	}
}

func (b *Inbox) notify(ownerDID string, msg *pb.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, sub := range b.subs {
		if sub.ownerDID != "" && sub.ownerDID != ownerDID {
			continue
		}
		select {
		case sub.ch <- msg:
		default: // subscriber too slow — drop rather than block
		}
	}
}

// GetForOwner retrieves messages matching the query, scoped to ownerDID
// ("" for all of them).
func (b *Inbox) GetForOwner(ownerDID, threadID, taskID string, unreadOnly bool, limit int, since int64) ([]*pb.Message, error) {
	return appactors.Dispatch(b.exec, func() ([]*pb.Message, error) {
		return b.get(ownerDID, threadID, taskID, unreadOnly, limit, since)
	})
}

func (b *Inbox) get(ownerDID, threadID, taskID string, unreadOnly bool, limit int, since int64) ([]*pb.Message, error) {
	q := `SELECT payload FROM inbox WHERE 1=1`
	args := []any{}
	if ownerDID != "" {
		q += ` AND owner_did = ?`
		args = append(args, ownerDID)
	}

	if threadID != "" {
		q += ` AND thread_id = ?`
		args = append(args, threadID)
	}
	if taskID != "" {
		q += ` AND task_id = ?`
		args = append(args, taskID)
	}
	if unreadOnly {
		q += ` AND read_at IS NULL`
	}
	if since > 0 {
		q += ` AND received_at > ?`
		args = append(args, since)
	}
	if limit > 0 {
		q += fmt.Sprintf(` LIMIT %d`, limit)
	}

	rows, err := b.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return sqlite.ScanProtos(rows, func() *pb.Message { return &pb.Message{} })
}

// AckForOwner marks a message as read, scoped to ownerDID ("" for any owner).
func (b *Inbox) AckForOwner(ownerDID, messageID string) error {
	return appactors.DispatchErr(b.exec, func() error { return b.ack(ownerDID, messageID) })
}

func (b *Inbox) ack(ownerDID, messageID string) error {
	q := `UPDATE inbox SET read_at = ? WHERE id = ?`
	args := []any{time.Now().UnixMilli(), messageID}
	if ownerDID != "" {
		q += ` AND owner_did = ?`
		args = append(args, ownerDID)
	}
	_, err := b.db.Exec(q, args...)
	return err
}

// Close closes the database.
func (b *Inbox) Close() error {
	return b.db.Close()
}

// ─── migrations ──────────────────────────────────────────────────────────────

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS inbox (
			id          TEXT PRIMARY KEY,
			owner_did   TEXT NOT NULL DEFAULT '',
			from_did    TEXT NOT NULL,
			thread_id   TEXT NOT NULL DEFAULT '',
			task_id     TEXT NOT NULL DEFAULT '',
			payload     BLOB NOT NULL,
			received_at INTEGER NOT NULL,
			read_at     INTEGER
		);
	`)
	if err != nil {
		return err
	}
	// An old inbox table already exists without owner_did. Add the column
	// before creating its index; otherwise SQLite rejects daemon restart before
	// the ALTER has an opportunity to migrate persistent state.
	_, err = db.Exec(`ALTER TABLE inbox ADD COLUMN owner_did TEXT NOT NULL DEFAULT ''`)
	if err != nil && !containsColumnError(err) {
		return err
	}
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_inbox_thread ON inbox(thread_id);
		CREATE INDEX IF NOT EXISTS idx_inbox_task   ON inbox(task_id);
		CREATE INDEX IF NOT EXISTS idx_inbox_time   ON inbox(received_at);
		CREATE INDEX IF NOT EXISTS idx_inbox_owner  ON inbox(owner_did, received_at);
	`)
	if err != nil {
		return err
	}
	return nil
}

func containsColumnError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "duplicate column name")
}
