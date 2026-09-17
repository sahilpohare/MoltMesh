package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/pkg/sqlite"
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
	flushCh chan struct{} // signals an immediate flush on new message enqueue
	exec    *appactors.Executor
}

func (o *Outbox) EnableActor(ctx context.Context, h *appactors.Hierarchy) error {
	return appactors.EnableSerialActor(ctx, h, "outbox", func(e *appactors.Executor) { o.exec = e }, func(e *appactors.Executor) error {
		return e.Schedule(ctx, "outbox-flush", 10*time.Second, func() (any, error) { o.flush(ctx); return nil, nil })
	})
}

// New opens (or creates) the outbox database at the given path.
func New(path string, deliver DeliverFunc, log *zap.Logger) (*Outbox, error) {
	db, err := sqlite.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open outbox db: %w", err)
	}
	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("migrate outbox: %w", err)
	}
	return &Outbox{db: db, deliver: deliver, log: log, flushCh: make(chan struct{}, 1)}, nil
}

// Enqueue adds a message to the outbox for delivery.
func (o *Outbox) Enqueue(msg *pb.Message) error {
	return o.EnqueueForOwner(msg.FromDid, msg)
}

// EnqueueForOwner persists an outgoing operation in the SDK-agent namespace
// that accepted it. Delivery itself remains daemon-to-daemon and is therefore
// intentionally independent of the local owner.
func (o *Outbox) EnqueueForOwner(ownerDID string, msg *pb.Message) error {
	// The insert runs directly rather than through o.exec. The actor
	// serializes flush, which makes a blocking network call per pending
	// message, so a Call here waits behind every in-flight delivery attempt:
	// an RPC that only needed a local INSERT took as long as the DHT took to
	// give up on unreachable recipients. Serialization is not lost, since the
	// SQLite pool is capped at one connection, and the statement is a single
	// INSERT OR IGNORE that touches nothing the actor owns.
	if err := o.enqueue(ownerDID, msg); err != nil {
		return err
	}
	// Ask for a flush without waiting for it.
	if o.exec != nil {
		_ = o.exec.Cast(context.Background(), func() (any, error) { o.flush(context.Background()); return nil, nil })
	}
	return nil
}

func (o *Outbox) enqueue(ownerDID string, msg *pb.Message) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	durable := durableKind(msg.Kind)
	expiresAt := time.Now().Add(defaultTTL).UnixMilli()
	_, err = o.db.Exec(`
		INSERT OR IGNORE INTO outbox (id, owner_did, to_did, thread_id, task_id, payload, created_at, expires_at, status, durable)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)`,
		msg.Id, ownerDID, msg.ToDid, msg.ThreadId, msg.TaskId, data,
		time.Now().UnixMilli(), expiresAt, durable,
	)
	if err != nil {
		return err
	}
	select {
	case o.flushCh <- struct{}{}:
	default: // flush already queued, nothing to do
	}
	return nil
}

// MarkDelivered marks a message as delivered.
func (o *Outbox) MarkDelivered(messageID string) error {
	return appactors.DispatchErr(o.exec, func() error { return o.markDelivered(messageID) })
}
func (o *Outbox) markDelivered(messageID string) error {
	_, err := o.db.Exec(`UPDATE outbox SET status = 'delivered' WHERE id = ?`, messageID)
	return err
}

// List returns outbox messages by status.
func (o *Outbox) List(status string, limit int) ([]*pb.Message, error) {
	return o.ListForOwner("", status, limit)
}

func (o *Outbox) ListForOwner(ownerDID, status string, limit int) ([]*pb.Message, error) {
	return appactors.Dispatch(o.exec, func() ([]*pb.Message, error) { return o.list(ownerDID, status, limit) })
}

func (o *Outbox) list(ownerDID, status string, limit int) ([]*pb.Message, error) {
	q := `SELECT payload FROM outbox WHERE status = ?`
	args := []any{status}
	// Keep the legacy failed query useful while exposing the more precise
	// dead-letter terminal state to newer callers.
	if status == "failed" {
		q = `SELECT payload FROM outbox WHERE status IN ('failed', 'dead_letter')`
		args = nil
	}
	if ownerDID != "" {
		q += ` AND owner_did = ?`
		args = append(args, ownerDID)
	}
	if limit > 0 {
		q += fmt.Sprintf(` LIMIT %d`, limit)
	}
	rows, err := o.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return sqlite.ScanProtos(rows, func() *pb.Message { return &pb.Message{} })
}

// Run starts the delivery loop. Blocks until ctx is cancelled.
// First delivery attempt fires immediately on Enqueue via flushCh.
// The ticker handles retries for failed messages.
func (o *Outbox) Run(ctx context.Context) {
	if o.exec != nil {
		<-ctx.Done()
		return
	}
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-o.flushCh:
			o.flush(ctx)
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
	if _, err := o.db.Exec(`UPDATE outbox SET status = 'expired' WHERE status = 'pending' AND durable = 0 AND expires_at < ?`, now); err != nil {
		o.log.Warn("outbox expire", zap.Error(err))
	}
	// A process may die after claiming an item. Processing is a lease, not a
	// terminal state: reclaim expired claims before selecting the next batch.
	if _, err := o.db.Exec(`UPDATE outbox SET status = 'pending' WHERE status = 'processing' AND last_attempt < ?`, now-int64(time.Minute/time.Millisecond)); err != nil {
		o.log.Warn("outbox reclaim expired processing lease", zap.Error(err))
	}

	// Atomically claim pending messages by transitioning to 'processing'.
	// This prevents overlapping flushes from picking the same batch.
	rows, err := o.db.Query(`
		UPDATE outbox SET status = 'processing', last_attempt = ?
		WHERE id IN (
			SELECT id FROM outbox
			WHERE status = 'pending' AND (last_attempt IS NULL OR last_attempt < ?)
			LIMIT 50
		) RETURNING id, payload, attempts, durable`,
		now, time.Now().Add(-retryBaseDelay).UnixMilli(),
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
		durable  bool
	}
	var items []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.id, &p.payload, &p.attempts, &p.durable); err != nil {
			continue
		}
		items = append(items, p)
	}
	rows.Close()

	for _, item := range items {
		var msg pb.Message
		if err := proto.Unmarshal(item.payload, &msg); err != nil {
			o.log.Warn("outbox unmarshal", zap.String("id", item.id), zap.Error(err))
			if _, updateErr := o.db.Exec(`UPDATE outbox SET status = 'failed', attempts = attempts + 1, last_attempt = ? WHERE id = ?`, time.Now().UnixMilli(), item.id); updateErr != nil {
				o.log.Warn("outbox quarantine corrupt message", zap.String("id", item.id), zap.Error(updateErr))
			}
			continue
		}

		err := o.deliver(ctx, &msg)

		attempts := item.attempts + 1
		finishNow := time.Now().UnixMilli()
		if err == nil {
			if _, updateErr := o.db.Exec(`UPDATE outbox SET status = 'delivered', attempts = ?, last_attempt = ? WHERE id = ?`,
				attempts, finishNow, item.id); updateErr != nil {
				o.log.Warn("outbox mark delivered", zap.String("id", item.id), zap.Error(updateErr))
			}
			o.log.Debug("outbox delivered", zap.String("id", item.id))
		} else {
			status := "pending"
			if !item.durable && attempts >= maxAttempts {
				status = "dead_letter"
				o.log.Warn("outbox max attempts", zap.String("id", item.id), zap.Error(err))
			} else {
				o.log.Warn("outbox delivery failed", zap.String("id", item.id), zap.Int("attempts", attempts), zap.Error(err))
			}
			if _, updateErr := o.db.Exec(`UPDATE outbox SET status = ?, attempts = ?, last_attempt = ? WHERE id = ?`,
				status, attempts, finishNow, item.id); updateErr != nil {
				o.log.Warn("outbox update status", zap.String("id", item.id), zap.Error(updateErr))
			}
		}
	}
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS outbox (
			id           TEXT PRIMARY KEY,
			owner_did    TEXT NOT NULL DEFAULT '',
			to_did       TEXT NOT NULL,
			thread_id    TEXT NOT NULL DEFAULT '',
			task_id      TEXT NOT NULL DEFAULT '',
			payload      BLOB NOT NULL,
			created_at   INTEGER NOT NULL,
			expires_at   INTEGER NOT NULL,
			attempts     INTEGER DEFAULT 0,
			last_attempt INTEGER,
			status       TEXT DEFAULT 'pending',
			durable      INTEGER NOT NULL DEFAULT 0
		);
	`)
	if err != nil {
		return err
	}
	// Migration for databases created before durable wake messages.
	rows, err := db.Query(`PRAGMA table_info(outbox)`)
	if err != nil {
		return err
	}
	hasDurable := false
	for rows.Next() {
		var cid, notNull, pk int
		var name, typ string
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &pk); err != nil {
			rows.Close()
			return err
		}
		if name == "durable" {
			hasDurable = true
		}
	}
	rows.Close()
	if !hasDurable {
		if _, err = db.Exec(`ALTER TABLE outbox ADD COLUMN durable INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
	}
	if _, err = db.Exec(`ALTER TABLE outbox ADD COLUMN owner_did TEXT NOT NULL DEFAULT ''`); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
		return err
	}
	if _, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status);
		CREATE INDEX IF NOT EXISTS idx_outbox_to     ON outbox(to_did);
		CREATE INDEX IF NOT EXISTS idx_outbox_owner  ON outbox(owner_did, status);
	`); err != nil {
		return err
	}
	// Any row left in processing was leased by a process that exited. It has
	// never been acknowledged as delivered, so returning it to pending is safe.
	if _, err = db.Exec(`UPDATE outbox SET status = 'pending' WHERE status = 'processing'`); err != nil {
		return err
	}
	// Backfill wake records created before the durable column existed. The
	// protobuf payload is the authoritative message kind, so this remains safe
	// across schema versions and does not rely on an absent SQL kind column.
	legacy, err := db.Query(`SELECT id, payload FROM outbox WHERE durable = 0`)
	if err != nil {
		return err
	}
	var wakeIDs []string
	for legacy.Next() {
		var id string
		var payload []byte
		if err := legacy.Scan(&id, &payload); err != nil {
			legacy.Close()
			return err
		}
		var msg pb.Message
		if proto.Unmarshal(payload, &msg) == nil && durableKind(msg.Kind) {
			wakeIDs = append(wakeIDs, id)
		}
	}
	if err := legacy.Close(); err != nil {
		return err
	}
	for _, id := range wakeIDs {
		if _, err := db.Exec(`UPDATE outbox SET durable = 1, status = CASE WHEN status IN ('expired', 'failed') THEN 'pending' ELSE status END WHERE id = ?`, id); err != nil {
			return err
		}
	}
	return nil
}

func durableKind(kind pb.MessageKind) bool {
	switch kind {
	case pb.MessageKind_MESSAGE_KIND_THREAD_INVITE,
		pb.MessageKind_MESSAGE_KIND_TASK_REQUEST,
		pb.MessageKind_MESSAGE_KIND_TASK_RESULT,
		pb.MessageKind_MESSAGE_KIND_TASK_CANCEL:
		return true
	default:
		return false
	}
}
