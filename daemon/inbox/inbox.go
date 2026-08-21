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
	exec, err := appactors.NewExecutor(ctx, h, "inbox")
	if err != nil {
		return err
	}
	b.exec = exec
	return nil
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

// Put stores an incoming message and notifies live subscribers.
// The DB write is synchronous (so we never ACK before persisting),
// but subscriber fan-out is async to unblock the network stream.
func (b *Inbox) Put(msg *pb.Message) error {
	return b.PutForOwner(msg.ToDid, msg)
}

// PutForOwner durably assigns a message to its local SDK-agent namespace.
// The legacy Put path derives this from Message.ToDid.
func (b *Inbox) PutForOwner(ownerDID string, msg *pb.Message) error {
	if b.exec != nil {
		_, err := b.exec.Call(context.Background(), func() (any, error) { return nil, b.put(ownerDID, msg) })
		return err
	}
	return b.put(ownerDID, msg)
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

// Subscribe registers a channel to receive new messages as they arrive.
// The caller must call Unsubscribe when done to avoid a goroutine leak.
func (b *Inbox) Subscribe() chan *pb.Message {
	return b.SubscribeForOwner("")
}

// SubscribeForOwner only receives messages belonging to ownerDID. An empty
// owner preserves the legacy all-inbox subscription behavior.
func (b *Inbox) SubscribeForOwner(ownerDID string) chan *pb.Message {
	if b.exec != nil {
		value, err := b.exec.Call(context.Background(), func() (any, error) { return b.subscribe(ownerDID), nil })
		if err == nil {
			return value.(chan *pb.Message)
		}
		return nil
	}
	return b.subscribe(ownerDID)
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
	if b.exec != nil {
		_, _ = b.exec.Call(context.Background(), func() (any, error) { b.unsubscribe(ch); return nil, nil })
		return
	}
	b.unsubscribe(ch)
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

// Get retrieves messages matching the query.
func (b *Inbox) Get(threadID, taskID string, unreadOnly bool, limit int, since int64) ([]*pb.Message, error) {
	return b.GetForOwner("", threadID, taskID, unreadOnly, limit, since)
}

func (b *Inbox) GetForOwner(ownerDID, threadID, taskID string, unreadOnly bool, limit int, since int64) ([]*pb.Message, error) {
	if b.exec != nil {
		value, err := b.exec.Call(context.Background(), func() (any, error) { return b.get(ownerDID, threadID, taskID, unreadOnly, limit, since) })
		if err != nil {
			return nil, err
		}
		return value.([]*pb.Message), nil
	}
	return b.get(ownerDID, threadID, taskID, unreadOnly, limit, since)
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

// Ack marks a message as read.
func (b *Inbox) Ack(messageID string) error {
	return b.AckForOwner("", messageID)
}

func (b *Inbox) AckForOwner(ownerDID, messageID string) error {
	if b.exec != nil {
		_, err := b.exec.Call(context.Background(), func() (any, error) { return nil, b.ack(ownerDID, messageID) })
		return err
	}
	return b.ack(ownerDID, messageID)
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
