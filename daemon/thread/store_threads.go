package thread

import (
	"database/sql"
	"encoding/json"
	"fmt"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

func (s *Store) SaveThread(t *pb.Thread) error {
	replicas, _ := json.Marshal(t.ReplicaDids)
	meta, _ := json.Marshal(t.Metadata)
	_, err := s.db.Exec(`
		INSERT OR REPLACE INTO threads
		  (id, creator_did, replica_dids, n, f, epoch_ms, created_at, metadata)
		VALUES (?,?,?,?,?,?,?,?)`,
		t.Id, t.CreatorDid, string(replicas),
		t.N, t.F, t.EpochMs, t.CreatedAt, string(meta),
	)
	return err
}

func (s *Store) ListThreads() ([]*pb.Thread, error) {
	rows, err := s.db.Query(`
		SELECT id, creator_did, replica_dids, n, f, epoch_ms, created_at, metadata
		FROM threads`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*pb.Thread
	for rows.Next() {
		var t pb.Thread
		var replicasJSON, metaJSON string
		if err := rows.Scan(&t.Id, &t.CreatorDid, &replicasJSON,
			&t.N, &t.F, &t.EpochMs, &t.CreatedAt, &metaJSON); err != nil {
			return nil, err
		}
		json.Unmarshal([]byte(replicasJSON), &t.ReplicaDids) //nolint:errcheck
		json.Unmarshal([]byte(metaJSON), &t.Metadata)        //nolint:errcheck
		out = append(out, &t)
	}
	return out, rows.Err()
}

func (s *Store) GetThread(id string) (*pb.Thread, error) {
	row := s.db.QueryRow(`
		SELECT id, creator_did, replica_dids, n, f, epoch_ms, created_at, metadata
		FROM threads WHERE id = ?`, id)
	return scanThread(row)
}

func scanThread(row *sql.Row) (*pb.Thread, error) {
	var t pb.Thread
	var replicasJSON, metaJSON string
	err := row.Scan(&t.Id, &t.CreatorDid, &replicasJSON,
		&t.N, &t.F, &t.EpochMs, &t.CreatedAt, &metaJSON)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("thread not found")
	}
	if err != nil {
		return nil, err
	}
	json.Unmarshal([]byte(replicasJSON), &t.ReplicaDids) //nolint:errcheck
	json.Unmarshal([]byte(metaJSON), &t.Metadata)        //nolint:errcheck
	return &t, nil
}

// ─── Blocks ───────────────────────────────────────────────────────────────────
