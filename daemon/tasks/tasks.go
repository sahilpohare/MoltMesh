package tasks

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

// Store is a persistent SQLite-backed task store.
type Store struct {
	db *sql.DB
}

// New opens (or creates) the task store at the given path.
func New(path string) (*Store, error) {
	db, err := sql.Open("sqlite3", path+"?_journal=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("open tasks db: %w", err)
	}
	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("migrate tasks: %w", err)
	}
	return &Store{db: db}, nil
}

// Create inserts a new task and returns it with a generated ID.
func (s *Store) Create(initiator, assignee, threadID, skill string, inputArtifacts []*pb.Artifact, meta map[string]string) (*pb.Task, error) {
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

	return s.Get(id)
}

// Get retrieves a task by ID.
func (s *Store) Get(id string) (*pb.Task, error) {
	row := s.db.QueryRow(`
		SELECT id, initiator, assignee, thread_id, skill, status,
		       input_artifacts, output_artifacts, metadata, created_at, updated_at, error
		FROM tasks WHERE id = ?`, id)
	return scanTask(row)
}

// UpdateStatus transitions a task to a new status.
func (s *Store) UpdateStatus(id string, status pb.TaskStatus, errMsg string, outputArtifacts []*pb.Artifact) (*pb.Task, error) {
	now := time.Now().UnixMilli()

	outputJSON, err := json.Marshal(outputArtifacts)
	if err != nil {
		return nil, err
	}

	_, err = s.db.Exec(`
		UPDATE tasks SET status = ?, error = ?, output_artifacts = ?, updated_at = ?
		WHERE id = ?`,
		int(status), errMsg, outputJSON, now, id,
	)
	if err != nil {
		return nil, err
	}
	return s.Get(id)
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
