package tasks

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
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
	db *sql.DB
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
	if skill == "" {
		return nil, fmt.Errorf("%w: skill is required", ErrInvalidTask)
	}
	if initiator != "" && initiator == assignee {
		return nil, fmt.Errorf("%w: cannot delegate a task to its own initiator", ErrInvalidTask)
	}

	id := uuid.New().String()
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
		INSERT INTO tasks (id, initiator, assignee, thread_id, skill, status, input_artifacts, metadata, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, initiator, assignee, threadID, skill,
		int(pb.TaskStatus_TASK_STATUS_SUBMITTED),
		artifactsJSON, metaJSON, now, now,
	)
	if err != nil {
		return nil, err
	}

	task, err := s.Get(id)
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

	task, err := s.Get(id)
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

// List returns tasks filtered by initiator or assignee DID.
func (s *Store) List(did string, limit int) ([]*pb.Task, error) {
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
		t               pb.Task
		status          int
		inputJSON       []byte
		outputJSON      []byte
		metaJSON        []byte
		errMsg          sql.NullString
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
			error            TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_tasks_initiator ON tasks(initiator);
		CREATE INDEX IF NOT EXISTS idx_tasks_assignee  ON tasks(assignee);
		CREATE INDEX IF NOT EXISTS idx_tasks_status    ON tasks(status);
		CREATE INDEX IF NOT EXISTS idx_tasks_thread    ON tasks(thread_id);
	`)
	return err
}
