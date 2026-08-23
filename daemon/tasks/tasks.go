package tasks

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	appactors "github.com/sahilpohare/p2p-a2a/daemon/actors"
	"github.com/sahilpohare/p2p-a2a/pkg/assert"
	"github.com/sahilpohare/p2p-a2a/pkg/sqlite"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// knownStatuses is every pb.TaskStatus value the FSM and storage layer know
// how to handle. Used to assert that values read back out of the database
// (or passed in from the wire) are within the domain the code was written
// against — anything else means either a schema/proto drift or storage
// corruption, not a caller mistake.
var knownStatuses = map[pb.TaskStatus]bool{
	pb.TaskStatus_TASK_STATUS_SUBMITTED: true,
	pb.TaskStatus_TASK_STATUS_WORKING:   true,
	pb.TaskStatus_TASK_STATUS_COMPLETED: true,
	pb.TaskStatus_TASK_STATUS_FAILED:    true,
	pb.TaskStatus_TASK_STATUS_CANCELLED: true,
}

// ErrInvalidTask is returned when Create is called with data that violates
// a Task invariant (e.g. no skill, self-delegation).
var ErrInvalidTask = errors.New("invalid task")

// ErrInvalidTransition is returned when a status update would move a task
// through a transition its current status does not allow (e.g. completing
// a task that was never started, or updating a task already in a terminal
// state).
var ErrInvalidTransition = errors.New("invalid task status transition")
var ErrLeaseConflict = errors.New("task lease conflict")
var ErrLeaseExpired = errors.New("task lease expired")
var ErrAttemptsExhausted = errors.New("task attempts exhausted")

// validTransitions encodes the Task lifecycle FSM: submitted -> working ->
// {completed, failed}, with cancellation possible from either non-terminal
// state. Terminal states (completed, failed, cancelled) accept no further
// transitions.
var validTransitions = map[pb.TaskStatus]map[pb.TaskStatus]bool{
	pb.TaskStatus_TASK_STATUS_SUBMITTED: {
		pb.TaskStatus_TASK_STATUS_WORKING:   true,
		pb.TaskStatus_TASK_STATUS_CANCELLED: true,
		pb.TaskStatus_TASK_STATUS_FAILED:    true,
	},
	pb.TaskStatus_TASK_STATUS_WORKING: {
		pb.TaskStatus_TASK_STATUS_COMPLETED: true,
		pb.TaskStatus_TASK_STATUS_FAILED:    true,
		pb.TaskStatus_TASK_STATUS_CANCELLED: true,
	},
}

func canTransition(from, to pb.TaskStatus) bool {
	// Both sides must be statuses the FSM was designed against — an unknown
	// value here means a proto enum drift or DB corruption, not a caller
	// mistake, so it's an assertion, not a returned error.
	assert.That(knownStatuses[from], "canTransition: unrecognized from-status %v", from)
	assert.That(knownStatuses[to], "canTransition: unrecognized to-status %v", to)
	return validTransitions[from][to]
}

// Store is a persistent SQLite-backed task store.
type Store struct {
	db         *sql.DB
	exec       *appactors.Executor
	hierarchy  *appactors.Hierarchy
	actorMu    sync.Mutex
	taskActors map[string]*appactors.Executor
}

func (s *Store) EnableActor(ctx context.Context, h *appactors.Hierarchy) error {
	exec, err := appactors.NewExecutor(ctx, h, "tasks")
	if err != nil {
		return err
	}
	s.exec = exec
	s.hierarchy = h
	s.taskActors = make(map[string]*appactors.Executor)
	return nil
}

func (s *Store) taskExecutor(id string) (*appactors.Executor, error) {
	s.actorMu.Lock()
	defer s.actorMu.Unlock()
	if exec := s.taskActors[id]; exec != nil {
		if exec.PID().IsRunning() {
			return exec, nil
		}
		delete(s.taskActors, id)
	}
	exec, err := appactors.NewExecutorUnder(context.Background(), s.hierarchy, s.exec.PID(), "task-"+id)
	if err != nil {
		return nil, err
	}
	s.taskActors[id] = exec
	appactors.Metrics.TaskActivated()
	return exec, nil
}

func (s *Store) releaseTaskActor(id string, exec *appactors.Executor) {
	s.actorMu.Lock()
	released := false
	if s.taskActors[id] == exec {
		delete(s.taskActors, id)
		released = true
	}
	s.actorMu.Unlock()
	if !released {
		return
	}
	_ = exec.Stop(context.Background())
	appactors.Metrics.TaskPassivated()
}

func (s *Store) ActiveTaskActors() int {
	s.actorMu.Lock()
	defer s.actorMu.Unlock()
	return len(s.taskActors)
}

func terminal(status pb.TaskStatus) bool {
	return status == pb.TaskStatus_TASK_STATUS_COMPLETED || status == pb.TaskStatus_TASK_STATUS_FAILED || status == pb.TaskStatus_TASK_STATUS_CANCELLED
}

// New opens (or creates) the task store at the given path.
func New(path string) (*Store, error) {
	db, err := sqlite.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open tasks db: %w", err)
	}
	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("migrate tasks: %w", err)
	}
	return &Store{db: db}, nil
}

// Create inserts a new task and returns it with a generated ID.
//
// Enforces: skill must be specified, and a task cannot be delegated to its
// own initiator (self-delegation defeats the purpose of delegation and
// papers over callers that forgot to resolve a real assignee DID).
func (s *Store) Create(initiator, assignee, threadID, skill string, inputArtifacts []*pb.Artifact, meta map[string]string) (*pb.Task, error) {
	return s.CreateIdempotent(initiator, assignee, threadID, skill, inputArtifacts, meta, "")
}

// CreateIdempotent returns a prior accepted task for the same initiator/key.
func (s *Store) CreateIdempotent(initiator, assignee, threadID, skill string, inputArtifacts []*pb.Artifact, meta map[string]string, key string) (*pb.Task, error) {
	if key != "" {
		var id string
		err := s.db.QueryRow(`SELECT id FROM tasks WHERE initiator=? AND idempotency_key=?`, initiator, key).Scan(&id)
		if err == nil {
			return s.Get(id)
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
	}
	return s.createDispatched("", initiator, assignee, threadID, skill, inputArtifacts, meta, key)
}

// CreateFromRemote materializes a task on the assignee's own daemon after
// receiving a TASK_REQUEST message from the initiator's daemon
// (Server.HandleIncoming) — without it, a task delegated across daemons is
// never recorded on the assignee's side, so SubscribeTasks/ClaimTask have
// nothing to find and the task sits in SUBMITTED forever. Unlike
// CreateIdempotent, id is supplied by the caller and must match the
// initiator's own record of the same task; returning the existing row for
// an id already present makes a redelivered TASK_REQUEST (the outbox
// retries until acked) safe to apply twice.
func (s *Store) CreateFromRemote(id, initiator, assignee, threadID, skill string, inputArtifacts []*pb.Artifact, meta map[string]string) (*pb.Task, error) {
	if id == "" {
		return nil, fmt.Errorf("%w: task id is required", ErrInvalidTask)
	}
	if existing, err := s.Get(id); err == nil {
		return existing, nil
	}
	return s.createDispatched(id, initiator, assignee, threadID, skill, inputArtifacts, meta, "")
}

// createDispatched routes create through the tasks domain actor when one is
// running (see daemon/actors), keeping every insert serialized the same way
// regardless of which of the two public constructors above triggered it.
func (s *Store) createDispatched(id, initiator, assignee, threadID, skill string, inputArtifacts []*pb.Artifact, meta map[string]string, key string) (*pb.Task, error) {
	task, err := appactors.Dispatch(s.exec, func() (*pb.Task, error) {
		return s.create(id, initiator, assignee, threadID, skill, inputArtifacts, meta, key)
	})
	if err != nil {
		return nil, err
	}
	if s.exec != nil {
		if _, err := s.taskExecutor(task.Id); err != nil {
			return nil, err
		}
	}
	return task, nil
}

func (s *Store) create(id, initiator, assignee, threadID, skill string, inputArtifacts []*pb.Artifact, meta map[string]string, key string) (*pb.Task, error) {
	if skill == "" {
		return nil, fmt.Errorf("%w: skill is required", ErrInvalidTask)
	}
	if initiator != "" && initiator == assignee {
		return nil, fmt.Errorf("%w: cannot delegate a task to its own initiator", ErrInvalidTask)
	}
	if id == "" {
		id = uuid.New().String()
	}
	now := time.Now().UnixMilli()

	artifactsJSON, err := json.Marshal(inputArtifacts)
	if err != nil {
		return nil, err
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}

	_, err = s.db.Exec(`
		INSERT INTO tasks (id, initiator, assignee, thread_id, skill, status, input_artifacts, metadata, created_at, updated_at, idempotency_key)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, initiator, assignee, threadID, skill,
		int(pb.TaskStatus_TASK_STATUS_SUBMITTED),
		artifactsJSON, metaJSON, now, now, key,
	)
	if err != nil {
		// The unique index is the authority for an idempotency key. A
		// preflight lookup is useful for the usual retry path, but it cannot
		// close a concurrent-create race. Resolve that race to the task the
		// first writer created instead of leaking a uniqueness error to an SDK
		// retry.
		if key != "" {
			var existing string
			if lookupErr := s.db.QueryRow(`SELECT id FROM tasks WHERE initiator=? AND idempotency_key=?`, initiator, key).Scan(&existing); lookupErr == nil {
				return s.get(existing)
			}
		}
		return nil, err
	}
	if _, err := s.db.Exec(`INSERT INTO task_deliveries (task_id, available_at) VALUES (?, ?)`, id, now); err != nil {
		return nil, err
	}

	task, err := s.get(id)
	if err != nil {
		return nil, err
	}
	assert.That(task.Id == id, "Create: returned task ID %q does not match inserted ID %q", task.Id, id)
	assert.That(task.Status == pb.TaskStatus_TASK_STATUS_SUBMITTED,
		"Create: new task %s has status %v, want SUBMITTED", id, task.Status)
	assert.That(task.Assignee == assignee, "Create: task %s assignee %q does not match requested %q", id, task.Assignee, assignee)
	return task, nil
}

// Get retrieves a task by ID.
func (s *Store) Get(id string) (*pb.Task, error) {
	if s.exec == nil {
		return s.get(id)
	}
	exec, err := s.taskExecutor(id)
	if err != nil {
		return nil, err
	}
	task, err := appactors.Dispatch(exec, func() (*pb.Task, error) { return s.get(id) })
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			s.releaseTaskActor(id, exec)
		}
		return nil, err
	}
	if terminal(task.Status) {
		s.releaseTaskActor(id, exec)
	}
	return task, nil
}
func (s *Store) get(id string) (*pb.Task, error) {
	row := s.db.QueryRow(`
		SELECT id, initiator, assignee, thread_id, skill, status,
		       input_artifacts, output_artifacts, metadata, created_at, updated_at, error
		FROM tasks WHERE id = ?`, id)
	return scanTask(row)
}

// UpdateStatus transitions a task to a new status, enforcing the Task
// lifecycle FSM: the transition is only applied if canTransition allows it
// from the task's current status. The read-check-write happens inside a
// single transaction so two concurrent callers (e.g. a worker completing a
// task while a coordinator cancels it) can't race past the guard — the
// loser gets ErrInvalidTransition instead of silently clobbering state.
func (s *Store) UpdateStatus(id string, status pb.TaskStatus, errMsg string, outputArtifacts []*pb.Artifact) (*pb.Task, error) {
	if s.exec == nil {
		return s.updateStatus(id, status, errMsg, outputArtifacts)
	}
	exec, err := s.taskExecutor(id)
	if err != nil {
		return nil, err
	}
	task, err := appactors.Dispatch(exec, func() (*pb.Task, error) { return s.updateStatus(id, status, errMsg, outputArtifacts) })
	if err != nil {
		return nil, err
	}
	if terminal(task.Status) {
		s.releaseTaskActor(id, exec)
	}
	return task, nil
}
func (s *Store) updateStatus(id string, status pb.TaskStatus, errMsg string, outputArtifacts []*pb.Artifact) (*pb.Task, error) {
	now := time.Now().UnixMilli()

	outputJSON, err := json.Marshal(outputArtifacts)
	if err != nil {
		return nil, err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck

	var current int
	if err := tx.QueryRow(`SELECT status FROM tasks WHERE id = ?`, id).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("task %s: %w", id, sql.ErrNoRows)
		}
		return nil, err
	}
	currentStatus := pb.TaskStatus(current)

	if !canTransition(currentStatus, status) {
		return nil, fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, currentStatus, status)
	}

	if _, err := tx.Exec(`
		UPDATE tasks SET status = ?, error = ?, output_artifacts = ?, updated_at = ?
		WHERE id = ?`,
		int(status), errMsg, outputJSON, now, id,
	); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	task, err := s.get(id)
	if err != nil {
		return nil, err
	}
	assert.That(task.Status == status, "UpdateStatus: task %s has status %v after committing transition to %v", id, task.Status, status)
	assert.That(task.UpdatedAt >= now, "UpdateStatus: task %s UpdatedAt %d did not advance to >= %d", id, task.UpdatedAt, now)
	return task, nil
}

// StartWork transitions a task from submitted to working.
func (s *Store) StartWork(id string) (*pb.Task, error) {
	task, err := s.UpdateStatus(id, pb.TaskStatus_TASK_STATUS_WORKING, "", nil)
	if err == nil {
		assert.That(task.Status == pb.TaskStatus_TASK_STATUS_WORKING, "StartWork: postcondition violated for task %s", id)
	}
	return task, err
}

// Complete transitions a task to completed, attaching output artifacts.
func (s *Store) Complete(id string, outputArtifacts []*pb.Artifact) (*pb.Task, error) {
	task, err := s.UpdateStatus(id, pb.TaskStatus_TASK_STATUS_COMPLETED, "", outputArtifacts)
	if err == nil {
		assert.That(task.Status == pb.TaskStatus_TASK_STATUS_COMPLETED, "Complete: postcondition violated for task %s", id)
	}
	return task, err
}

// Fail transitions a task to failed, recording the error.
func (s *Store) Fail(id string, errMsg string) (*pb.Task, error) {
	task, err := s.UpdateStatus(id, pb.TaskStatus_TASK_STATUS_FAILED, errMsg, nil)
	if err == nil {
		assert.That(task.Status == pb.TaskStatus_TASK_STATUS_FAILED, "Fail: postcondition violated for task %s", id)
		assert.That(task.Error == errMsg, "Fail: task %s error %q does not match requested %q", id, task.Error, errMsg)
	}
	return task, err
}

// Cancel transitions a task to cancelled.
func (s *Store) Cancel(id string) (*pb.Task, error) {
	task, err := s.UpdateStatus(id, pb.TaskStatus_TASK_STATUS_CANCELLED, "", nil)
	if err == nil {
		assert.That(task.Status == pb.TaskStatus_TASK_STATUS_CANCELLED, "Cancel: postcondition violated for task %s", id)
	}
	return task, err
}

// Claim atomically assigns a submitted task to its authenticated assignee.
// A repeat claim by the same worker returns the still-active lease.
func (s *Store) Claim(id, worker string, lease time.Duration) (*pb.TaskLease, error) {
	if lease <= 0 {
		lease = 30 * time.Second
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var assignee string
	var status int
	var token sql.NullString
	var owner sql.NullString
	var expires sql.NullInt64
	var attempt int
	var maxAttempts int
	var deadline sql.NullInt64
	if err := tx.QueryRow(`SELECT assignee,status,lease_token,lease_owner,lease_expires_at,attempt,max_attempts,deadline_at FROM tasks WHERE id=?`, id).Scan(&assignee, &status, &token, &owner, &expires, &attempt, &maxAttempts, &deadline); err != nil {
		return nil, err
	}
	if assignee != worker {
		return nil, fmt.Errorf("%w: worker is not task assignee", ErrLeaseConflict)
	}
	now := time.Now().UnixMilli()
	if deadline.Valid && deadline.Int64 > 0 && now >= deadline.Int64 {
		if _, err := tx.Exec(`UPDATE tasks SET status=?, error=?, updated_at=? WHERE id=? AND status IN (?,?)`, int(pb.TaskStatus_TASK_STATUS_FAILED), "task deadline exceeded", now, id, int(pb.TaskStatus_TASK_STATUS_SUBMITTED), int(pb.TaskStatus_TASK_STATUS_WORKING)); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, ErrAttemptsExhausted
	}
	if pb.TaskStatus(status) == pb.TaskStatus_TASK_STATUS_WORKING && owner.String == worker && expires.Valid && expires.Int64 > now {
		return &pb.TaskLease{TaskId: id, LeaseToken: token.String, ExpiresAtUnixMs: expires.Int64, Attempt: uint32(attempt)}, tx.Commit()
	}
	if pb.TaskStatus(status) != pb.TaskStatus_TASK_STATUS_SUBMITTED && !(pb.TaskStatus(status) == pb.TaskStatus_TASK_STATUS_WORKING && expires.Valid && expires.Int64 <= now) {
		return nil, ErrLeaseConflict
	}
	if maxAttempts > 0 && attempt >= maxAttempts {
		if _, err := tx.Exec(`UPDATE tasks SET status=?, error=?, lease_token='', lease_owner='', lease_expires_at=0, updated_at=? WHERE id=?`, int(pb.TaskStatus_TASK_STATUS_FAILED), "maximum task attempts exhausted", now, id); err != nil {
			return nil, err
		}
		if err := tx.Commit(); err != nil {
			return nil, err
		}
		return nil, ErrAttemptsExhausted
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return nil, err
	}
	leaseToken := base64.RawURLEncoding.EncodeToString(b)
	until := now + lease.Milliseconds()
	if _, err := tx.Exec(`UPDATE tasks SET status=?,lease_token=?,lease_owner=?,lease_expires_at=?,attempt=attempt+1,updated_at=? WHERE id=?`, int(pb.TaskStatus_TASK_STATUS_WORKING), leaseToken, worker, until, now, id); err != nil {
		return nil, err
	}
	attempt++
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &pb.TaskLease{TaskId: id, LeaseToken: leaseToken, ExpiresAtUnixMs: until, Attempt: uint32(attempt)}, nil
}

// SetMaxAttempts records the caller's task retry budget. Zero preserves the
// default budget, while a positive value replaces it before work is claimed.
func (s *Store) SetMaxAttempts(id string, max int) error {
	if max == 0 {
		return nil
	}
	if max < 1 {
		return fmt.Errorf("max attempts must be positive")
	}
	_, err := s.db.Exec(`UPDATE tasks SET max_attempts=? WHERE id=?`, max, id)
	return err
}

func (s *Store) SetTimeout(id string, timeout time.Duration) error {
	if timeout <= 0 {
		return nil
	}
	_, err := s.db.Exec(`UPDATE tasks SET deadline_at=? WHERE id=?`, time.Now().Add(timeout).UnixMilli(), id)
	return err
}

func (s *Store) RenewLease(id, worker, leaseToken string, lease time.Duration) (*pb.TaskLease, error) {
	if lease <= 0 {
		lease = 30 * time.Second
	}
	now := time.Now().UnixMilli()
	until := now + lease.Milliseconds()
	r, err := s.db.Exec(`UPDATE tasks SET lease_expires_at=?,updated_at=? WHERE id=? AND status=? AND lease_owner=? AND lease_token=? AND lease_expires_at>?`, until, now, id, int(pb.TaskStatus_TASK_STATUS_WORKING), worker, leaseToken, now)
	if err != nil {
		return nil, err
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		return nil, ErrLeaseExpired
	}
	var attempt int
	_ = s.db.QueryRow(`SELECT attempt FROM tasks WHERE id=?`, id).Scan(&attempt)
	return &pb.TaskLease{TaskId: id, LeaseToken: leaseToken, ExpiresAtUnixMs: until, Attempt: uint32(attempt)}, nil
}

func (s *Store) FinishLease(id, worker, leaseToken string, status pb.TaskStatus, errMsg string, outputs []*pb.Artifact) (*pb.Task, error) {
	if status != pb.TaskStatus_TASK_STATUS_COMPLETED && status != pb.TaskStatus_TASK_STATUS_FAILED {
		return nil, ErrInvalidTransition
	}
	now := time.Now().UnixMilli()
	out, err := json.Marshal(outputs)
	if err != nil {
		return nil, err
	}
	r, err := s.db.Exec(`UPDATE tasks SET status=?,error=?,output_artifacts=?,lease_token='',lease_owner='',lease_expires_at=0,updated_at=? WHERE id=? AND status=? AND lease_owner=? AND lease_token=? AND lease_expires_at>?`, int(status), errMsg, out, now, id, int(pb.TaskStatus_TASK_STATUS_WORKING), worker, leaseToken, now)
	if err != nil {
		return nil, err
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		return nil, ErrLeaseExpired
	}
	return s.get(id)
}

// ListDeliveries returns the latest durable delivery for each task that is
// available to worker. Sequence is a database-assigned cursor, so callers can
// reconnect with AfterSequence without losing or renumbering work.
//
// An expired lease is returned to SUBMITTED before the query. This is done in
// the store, rather than in a stream-local timer, so expiry survives daemon and
// SDK restarts. Exhausted and timed-out tasks are terminally failed instead.
func (s *Store) ListDeliveries(worker string, skills []string, after uint64, limit int) ([]*pb.TaskDelivery, error) {
	if worker == "" {
		return nil, ErrLeaseConflict
	}
	if err := s.requeueExpired(); err != nil {
		return nil, err
	}
	args := []any{worker, int(pb.TaskStatus_TASK_STATUS_SUBMITTED), after}
	query := `SELECT d.sequence, t.id, t.initiator, t.assignee, t.thread_id, t.skill, t.status,
				t.input_artifacts, t.output_artifacts, t.metadata, t.created_at, t.updated_at, t.error
		FROM task_deliveries d JOIN tasks t ON t.id=d.task_id
		WHERE t.assignee=? AND t.status=? AND d.sequence>?
		  AND d.sequence=(SELECT MAX(latest.sequence) FROM task_deliveries latest WHERE latest.task_id=t.id)`
	if len(skills) > 0 {
		placeholders := make([]string, len(skills))
		for i, skill := range skills {
			placeholders[i] = "?"
			args = append(args, skill)
		}
		query += ` AND t.skill IN (` + strings.Join(placeholders, ",") + `)`
	}
	query += ` ORDER BY d.sequence`
	if limit > 0 {
		query += fmt.Sprintf(` LIMIT %d`, limit)
	}
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var deliveries []*pb.TaskDelivery
	for rows.Next() {
		var sequence uint64
		var task pb.Task
		if err := scanTaskWithSequence(rows, &sequence, &task); err != nil {
			return nil, err
		}
		deliveries = append(deliveries, &pb.TaskDelivery{Task: &task, Sequence: sequence})
	}
	return deliveries, rows.Err()
}

func (s *Store) requeueExpired() error {
	now := time.Now().UnixMilli()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	rows, err := tx.Query(`SELECT id, attempt, max_attempts, deadline_at FROM tasks WHERE status=? AND lease_expires_at>0 AND lease_expires_at<=?`, int(pb.TaskStatus_TASK_STATUS_WORKING), now)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var attempt, maxAttempts int
		var deadline sql.NullInt64
		if err := rows.Scan(&id, &attempt, &maxAttempts, &deadline); err != nil {
			return err
		}
		if (deadline.Valid && deadline.Int64 > 0 && now >= deadline.Int64) || (maxAttempts > 0 && attempt >= maxAttempts) {
			if _, err := tx.Exec(`UPDATE tasks SET status=?, error=?, lease_token='', lease_owner='', lease_expires_at=0, updated_at=? WHERE id=?`, int(pb.TaskStatus_TASK_STATUS_FAILED), "task lease expired after retry budget", now, id); err != nil {
				return err
			}
			continue
		}
		if _, err := tx.Exec(`UPDATE tasks SET status=?, lease_token='', lease_owner='', lease_expires_at=0, updated_at=? WHERE id=?`, int(pb.TaskStatus_TASK_STATUS_SUBMITTED), now, id); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO task_deliveries (task_id, available_at) VALUES (?, ?)`, id, now); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return tx.Commit()
}

// List returns tasks filtered by initiator or assignee DID.
func (s *Store) List(did string, limit int) ([]*pb.Task, error) {
	return appactors.Dispatch(s.exec, func() ([]*pb.Task, error) { return s.list(did, limit) })
}

// SaveEvent durably records a task event. Duplicate sequence numbers are
// idempotent, allowing direct-message retries without duplicate replay.
func (s *Store) SaveEvent(event *pb.TaskEvent) error {
	if event == nil || event.TaskId == "" {
		return fmt.Errorf("task event requires task_id")
	}
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT OR IGNORE INTO task_events (task_id, seq, event, emitted_at) VALUES (?, ?, ?, ?)`, event.TaskId, event.Seq, data, event.EmittedAt)
	return err
}

func (s *Store) ListEvents(taskID string, afterSeq int64) ([]*pb.TaskEvent, error) {
	rows, err := s.db.Query(`SELECT event FROM task_events WHERE task_id = ? AND seq > ? ORDER BY seq`, taskID, afterSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []*pb.TaskEvent
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		var event pb.TaskEvent
		if err := json.Unmarshal(data, &event); err != nil {
			return nil, err
		}
		events = append(events, &event)
	}
	return events, rows.Err()
}
func (s *Store) list(did string, limit int) ([]*pb.Task, error) {
	q := `SELECT id, initiator, assignee, thread_id, skill, status,
		         input_artifacts, output_artifacts, metadata, created_at, updated_at, error
		  FROM tasks WHERE initiator = ? OR assignee = ? ORDER BY created_at DESC`
	if limit > 0 {
		q += fmt.Sprintf(` LIMIT %d`, limit)
	}
	rows, err := s.db.Query(q, did, did)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*pb.Task
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, t)
	}
	return tasks, rows.Err()
}

// Close closes the database.
func (s *Store) Close() error {
	return s.db.Close()
}

// ─── internal ────────────────────────────────────────────────────────────────

type scanner interface {
	Scan(dest ...any) error
}

func scanTask(row scanner) (*pb.Task, error) {
	var (
		t          pb.Task
		status     int
		inputJSON  []byte
		outputJSON []byte
		metaJSON   []byte
		errMsg     sql.NullString
	)
	err := row.Scan(
		&t.Id, &t.Initiator, &t.Assignee, &t.ThreadId, &t.Skill,
		&status, &inputJSON, &outputJSON, &metaJSON,
		&t.CreatedAt, &t.UpdatedAt, &errMsg,
	)
	if err != nil {
		return nil, err
	}

	t.Status = pb.TaskStatus(status)
	assert.That(knownStatuses[t.Status], "scanTask: task %s has unrecognized status %d in storage", t.Id, status)
	if errMsg.Valid {
		t.Error = errMsg.String
	}

	if err := json.Unmarshal(inputJSON, &t.InputArtifacts); err != nil && string(inputJSON) != "null" {
		return nil, fmt.Errorf("unmarshal input artifacts: %w", err)
	}
	if len(outputJSON) > 0 && string(outputJSON) != "null" {
		if err := json.Unmarshal(outputJSON, &t.OutputArtifacts); err != nil {
			return nil, fmt.Errorf("unmarshal output artifacts: %w", err)
		}
	}
	if len(metaJSON) > 0 && string(metaJSON) != "null" {
		if err := json.Unmarshal(metaJSON, &t.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}

	return &t, nil
}

func scanTaskWithSequence(row scanner, sequence *uint64, task *pb.Task) error {
	var (
		status     int
		inputJSON  []byte
		outputJSON []byte
		metaJSON   []byte
		errMsg     sql.NullString
	)
	if err := row.Scan(sequence, &task.Id, &task.Initiator, &task.Assignee, &task.ThreadId, &task.Skill,
		&status, &inputJSON, &outputJSON, &metaJSON, &task.CreatedAt, &task.UpdatedAt, &errMsg); err != nil {
		return err
	}
	task.Status = pb.TaskStatus(status)
	if !knownStatuses[task.Status] {
		return fmt.Errorf("task %s has unrecognized status %d in storage", task.Id, status)
	}
	if errMsg.Valid {
		task.Error = errMsg.String
	}
	if err := json.Unmarshal(inputJSON, &task.InputArtifacts); err != nil && string(inputJSON) != "null" {
		return fmt.Errorf("unmarshal input artifacts: %w", err)
	}
	if len(outputJSON) > 0 && string(outputJSON) != "null" {
		if err := json.Unmarshal(outputJSON, &task.OutputArtifacts); err != nil {
			return fmt.Errorf("unmarshal output artifacts: %w", err)
		}
	}
	if len(metaJSON) > 0 && string(metaJSON) != "null" {
		if err := json.Unmarshal(metaJSON, &task.Metadata); err != nil {
			return fmt.Errorf("unmarshal metadata: %w", err)
		}
	}
	return nil
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS tasks (
			id               TEXT PRIMARY KEY,
			initiator        TEXT NOT NULL,
			assignee         TEXT NOT NULL,
			thread_id        TEXT NOT NULL DEFAULT '',
			skill            TEXT NOT NULL,
			status           INTEGER NOT NULL DEFAULT 1,
			input_artifacts  BLOB,
			output_artifacts BLOB,
			metadata         BLOB,
			created_at       INTEGER NOT NULL,
			updated_at       INTEGER NOT NULL,
			error            TEXT,
			idempotency_key  TEXT NOT NULL DEFAULT '',
			max_attempts     INTEGER NOT NULL DEFAULT 3,
			deadline_at      INTEGER
		);
		CREATE INDEX IF NOT EXISTS idx_tasks_initiator ON tasks(initiator);
		CREATE INDEX IF NOT EXISTS idx_tasks_assignee  ON tasks(assignee);
		CREATE INDEX IF NOT EXISTS idx_tasks_status    ON tasks(status);
		CREATE INDEX IF NOT EXISTS idx_tasks_thread    ON tasks(thread_id);
		CREATE TABLE IF NOT EXISTS task_events (
			task_id    TEXT NOT NULL,
			seq        INTEGER NOT NULL,
			event      BLOB NOT NULL,
			emitted_at INTEGER NOT NULL,
			PRIMARY KEY (task_id, seq)
		);
		CREATE INDEX IF NOT EXISTS idx_task_events_task ON task_events(task_id, seq);
		CREATE TABLE IF NOT EXISTS task_deliveries (
			sequence     INTEGER PRIMARY KEY AUTOINCREMENT,
			task_id      TEXT NOT NULL,
			available_at INTEGER NOT NULL,
			FOREIGN KEY (task_id) REFERENCES tasks(id)
		);
		CREATE INDEX IF NOT EXISTS idx_task_deliveries_task_sequence ON task_deliveries(task_id, sequence);
	`)
	if err != nil {
		return err
	}
	for _, q := range []string{
		`ALTER TABLE tasks ADD COLUMN lease_token TEXT`, `ALTER TABLE tasks ADD COLUMN lease_owner TEXT`, `ALTER TABLE tasks ADD COLUMN lease_expires_at INTEGER`, `ALTER TABLE tasks ADD COLUMN attempt INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE tasks ADD COLUMN idempotency_key TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE tasks ADD COLUMN max_attempts INTEGER NOT NULL DEFAULT 3`,
		`ALTER TABLE tasks ADD COLUMN deadline_at INTEGER`,
	} {
		if _, e := db.Exec(q); e != nil && !strings.Contains(e.Error(), "duplicate column") {
			return e
		}
	}
	if _, e := db.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_idempotency ON tasks(initiator, idempotency_key) WHERE idempotency_key <> ''`); e != nil {
		return e
	}
	return nil
}
