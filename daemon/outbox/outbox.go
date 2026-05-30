package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

const (
	defaultTTL     = 72 * time.Hour
	maxAttempts    = 10
	retryBaseDelay = 5 * time.Second
)

// DeliverFunc is called by the retry loop to attempt message delivery.
type DeliverFunc func(ctx context.Context, msg *pb.Message) error

// Outbox is a persistent SQLite-backed outgoing message queue with retry.
type Outbox struct {
	db      *sql.DB
	deliver DeliverFunc
	log     *zap.Logger
}

// New opens (or creates) the outbox database at the given path.
func New(path string, deliver DeliverFunc, log *zap.Logger) (*Outbox, error) {
	db, err := sql.Open("sqlite3", path+"?_journal=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open outbox db: %w", err)
	}
	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("migrate outbox: %w", err)
	}
	return &Outbox{db: db, deliver: deliver, log: log}, nil
}

// Enqueue adds a message to the outbox for delivery.
func (o *Outbox) Enqueue(msg *pb.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	expiresAt := time.Now().Add(defaultTTL).UnixMilli()
	_, err = o.db.Exec(`
		INSERT OR IGNORE INTO outbox (id, to_did, thread_id, task_id, payload, created_at, expires_at, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')`,
		msg.Id, msg.ToDid, msg.ThreadId, msg.TaskId, data,
		time.Now().UnixMilli(), expiresAt,
	)
	return err
}

// MarkDelivered marks a message as delivered.
func (o *Outbox) MarkDelivered(messageID string) error {
	_, err := o.db.Exec(`UPDATE outbox SET status = 'delivered' WHERE id = ?`, messageID)
	return err
}

// List returns outbox messages by status.
func (o *Outbox) List(status string, limit int) ([]*pb.Message, error) {
	q := `SELECT payload FROM outbox WHERE status = ?`
	if limit > 0 {
		q += fmt.Sprintf(` LIMIT %d`, limit)
	}
	rows, err := o.db.Query(q, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*pb.Message
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var msg pb.Message
		if err := proto.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		msgs = append(msgs, &msg)
	}
	return msgs, rows.Err()
}

// Run starts the retry loop. Blocks until ctx is cancelled.
func (o *Outbox) Run(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			o.flush(ctx)
		}
	}
}

// Close closes the database.
func (o *Outbox) Close() error {
	return o.db.Close()
}

// ─── internal ────────────────────────────────────────────────────────────────

func (o *Outbox) flush(ctx context.Context) {
	now := time.Now().UnixMilli()

	// expire old messages
	if _, err := o.db.Exec(`UPDATE outbox SET status = 'expired' WHERE status = 'pending' AND expires_at < ?`, now); err != nil {
		o.log.Warn("outbox expire", zap.Error(err))
	}

	// fetch pending messages
	rows, err := o.db.Query(`
		SELECT id, payload, attempts FROM outbox
		WHERE status = 'pending' AND (last_attempt IS NULL OR last_attempt < ?)
		LIMIT 50`,
		time.Now().Add(-retryBaseDelay).UnixMilli(),
	)
	if err != nil {
		o.log.Warn("outbox query", zap.Error(err))
		return
	}
	defer rows.Close()

	type pending struct {
		id       string
		payload  []byte
		attempts int
	}
	var items []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.payload, &p.attempts); err != nil {
			continue
		}
		items = append(items, p)
	}
	rows.Close()

	for _, item := range items {
		var msg pb.Message
		if err := proto.Unmarshal(item.payload, &msg); err != nil {
			o.log.Warn("outbox unmarshal", zap.String("id", item.id), zap.Error(err))
			continue
		}

		err := o.deliver(ctx, &msg)

		attempts := item.attempts + 1
		if err == nil {
			o.db.Exec(`UPDATE outbox SET status = 'delivered', attempts = ?, last_attempt = ? WHERE id = ?`,
				attempts, now, item.id)
			o.log.Debug("outbox delivered", zap.String("id", item.id))
		} else {
			status := "pending"
			if attempts >= maxAttempts {
				status = "failed"
				o.log.Warn("outbox max attempts", zap.String("id", item.id))
			}
			o.db.Exec(`UPDATE outbox SET status = ?, attempts = ?, last_attempt = ? WHERE id = ?`,
				status, attempts, now, item.id)
		}
	}
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS outbox (
			id           TEXT PRIMARY KEY,
			to_did       TEXT NOT NULL,
			thread_id    TEXT NOT NULL DEFAULT '',
			task_id      TEXT NOT NULL DEFAULT '',
			payload      BLOB NOT NULL,
			created_at   INTEGER NOT NULL,
			expires_at   INTEGER NOT NULL,
			attempts     INTEGER DEFAULT 0,
			last_attempt INTEGER,
			status       TEXT DEFAULT 'pending'
		);
		CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status);
		CREATE INDEX IF NOT EXISTS idx_outbox_to     ON outbox(to_did);
	`)
	return err
}
