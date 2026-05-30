package inbox

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/sahilpohare/p2p-a2a/pkg/sqlite"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"google.golang.org/protobuf/proto"
)

// Inbox is a persistent SQLite-backed message queue with live push.
type Inbox struct {
	db   *sql.DB
	mu   sync.Mutex
	subs []chan<- *pb.Message
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
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	_, err = b.db.Exec(`
		INSERT OR IGNORE INTO inbox (id, from_did, thread_id, task_id, payload, received_at)
		VALUES (?, ?, ?, ?, ?, ?)`,
		msg.Id, msg.FromDid, msg.ThreadId, msg.TaskId, data, time.Now().UnixMilli(),
	)
	if err != nil {
		return err
	}
	go b.notify(msg)
	return nil
}

// Subscribe registers a channel to receive new messages as they arrive.
// The caller must call Unsubscribe when done to avoid a goroutine leak.
func (b *Inbox) Subscribe() chan *pb.Message {
	ch := make(chan *pb.Message, 64)
	b.mu.Lock()
	b.subs = append(b.subs, ch)
	b.mu.Unlock()
	return ch
}

// Unsubscribe removes and closes a previously subscribed channel.
func (b *Inbox) Unsubscribe(ch chan *pb.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, s := range b.subs {
		if s == ch {
			b.subs = append(b.subs[:i], b.subs[i+1:]...)
			close(ch)
			return
		}
	}
}

func (b *Inbox) notify(msg *pb.Message) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subs {
		select {
		case ch <- msg:
		default: // subscriber too slow — drop rather than block
		}
	}
}

// Get retrieves messages matching the query.
func (b *Inbox) Get(threadID, taskID string, unreadOnly bool, limit int, since int64) ([]*pb.Message, error) {
	q := `SELECT payload FROM inbox WHERE 1=1`
	args := []any{}

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
	_, err := b.db.Exec(`UPDATE inbox SET read_at = ? WHERE id = ?`,
		time.Now().UnixMilli(), messageID)
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
			from_did    TEXT NOT NULL,
			thread_id   TEXT NOT NULL DEFAULT '',
			task_id     TEXT NOT NULL DEFAULT '',
			payload     BLOB NOT NULL,
			received_at INTEGER NOT NULL,
			read_at     INTEGER
		);
		CREATE INDEX IF NOT EXISTS idx_inbox_thread ON inbox(thread_id);
		CREATE INDEX IF NOT EXISTS idx_inbox_task   ON inbox(task_id);
		CREATE INDEX IF NOT EXISTS idx_inbox_time   ON inbox(received_at);
	`)
	return err
}
