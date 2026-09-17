package thread

import (
	"fmt"

	"database/sql"
	"encoding/json"
	"errors"
	"github.com/sahilpohare/p2p-a2a/pkg/sqlite"
	"time"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func (s *Store) SaveVote(v *pb.Vote) error {
	_, err := s.db.Exec(`
		INSERT OR IGNORE INTO thread_votes
		  (thread_id, height, round, vote_type, voter_did, block_hash, signature, received_at)
		VALUES (?,?,?,?,?,?,?,?)`,
		v.ThreadId, v.Height, v.Round, int32(v.Type),
		v.VoterDid, v.BlockHash, v.Signature, time.Now().UnixMilli(),
	)
	return err
}

// GetVotes returns all votes for (thread, height, round, type).
func (s *Store) GetVotes(threadID string, height int64, round int32, vtype pb.VoteType) ([]*pb.Vote, error) {
	rows, err := s.db.Query(`
		SELECT thread_id, height, round, vote_type, voter_did, block_hash, signature
		FROM thread_votes
		WHERE thread_id = ? AND height = ? AND round = ? AND vote_type = ?`,
		threadID, height, round, int32(vtype))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var votes []*pb.Vote
	for rows.Next() {
		var v pb.Vote
		var vt int32
		if err := rows.Scan(&v.ThreadId, &v.Height, &v.Round, &vt,
			&v.VoterDid, &v.BlockHash, &v.Signature); err != nil {
			return nil, err
		}
		v.Type = pb.VoteType(vt)
		votes = append(votes, &v)
	}
	return votes, rows.Err()
}

// ─── Pending entries ──────────────────────────────────────────────────────────

// SaveRaftReady persists the unstable Raft log and HardState before the node
// sends messages, applies commits, or calls Advance.
func (s *Store) SaveRaftReady(threadID string, hardState raftpb.HardState, entries []raftpb.Entry) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if !raft.IsEmptyHardState(hardState) {
		data, err := hardState.Marshal()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO raft_hard_state (thread_id, state) VALUES (?, ?)`, threadID, data); err != nil {
			return err
		}
	}
	for i := range entries {
		data, err := entries[i].Marshal()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT OR REPLACE INTO raft_entries (thread_id, log_index, term, entry) VALUES (?, ?, ?, ?)`,
			threadID, entries[i].Index, entries[i].Term, data); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) LoadRaftState(threadID string) (raftpb.Snapshot, raftpb.HardState, []raftpb.Entry, error) {
	var snapshot raftpb.Snapshot
	var snapshotData []byte
	if err := s.db.QueryRow(`SELECT snapshot FROM raft_snapshots WHERE thread_id = ?`, threadID).Scan(&snapshotData); err == nil {
		if err := snapshot.Unmarshal(snapshotData); err != nil {
			return snapshot, raftpb.HardState{}, nil, err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return snapshot, raftpb.HardState{}, nil, err
	}
	var hardState raftpb.HardState
	var hardData []byte
	if err := s.db.QueryRow(`SELECT state FROM raft_hard_state WHERE thread_id = ?`, threadID).Scan(&hardData); err == nil {
		if err := hardState.Unmarshal(hardData); err != nil {
			return snapshot, hardState, nil, err
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return snapshot, hardState, nil, err
	}
	rows, err := s.db.Query(`SELECT entry FROM raft_entries WHERE thread_id = ? AND log_index > ? ORDER BY log_index`, threadID, snapshot.Metadata.Index)
	if err != nil {
		return snapshot, hardState, nil, err
	}
	defer rows.Close()
	var entries []raftpb.Entry
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return snapshot, hardState, nil, err
		}
		var entry raftpb.Entry
		if err := entry.Unmarshal(data); err != nil {
			return snapshot, hardState, nil, err
		}
		entries = append(entries, entry)
	}
	return snapshot, hardState, entries, rows.Err()
}

func (s *Store) SaveRaftSnapshot(threadID string, snapshot raftpb.Snapshot) error {
	data, err := snapshot.Marshal()
	if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.Exec(`INSERT OR REPLACE INTO raft_snapshots (thread_id, snapshot, updated_at) VALUES (?, ?, ?)`,
		threadID, data, time.Now().UnixMilli()); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM raft_entries WHERE thread_id = ? AND log_index <= ?`, threadID, snapshot.Metadata.Index); err != nil {
		return err
	}
	return tx.Commit()
}

// ─── Consensus state ──────────────────────────────────────────────────────────

type ConsensusState struct {
	Height      int64
	Round       int32
	Step        string // propose | prevote | precommit | commit
	LockedRound int32
	LockedHash  string
	ValidRound  int32
	ValidHash   string
}

// ThreadCheckpoint is the durable activation record for a passivated thread.
// Consensus data itself lives in thread_blocks/consensus_state; this record
// makes lifecycle state inspectable without keeping an actor resident.
type ThreadCheckpoint struct {
	CommittedHeight int64 `json:"committed_height"`
	PassivatedAt    int64 `json:"passivated_at"`
}

func (s *Store) SaveCheckpoint(threadID string, checkpoint ThreadCheckpoint) error {
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`
		INSERT INTO thread_checkpoints (thread_id, checkpoint_json, updated_at)
		VALUES (?, ?, ?)
		ON CONFLICT(thread_id) DO UPDATE SET
			checkpoint_json = excluded.checkpoint_json,
			updated_at = excluded.updated_at`,
		threadID, string(data), time.Now().UnixMilli())
	return err
}

func (s *Store) LoadCheckpoint(threadID string) (ThreadCheckpoint, error) {
	var data string
	err := s.db.QueryRow(`SELECT checkpoint_json FROM thread_checkpoints WHERE thread_id = ?`, threadID).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return ThreadCheckpoint{}, nil
	}
	if err != nil {
		return ThreadCheckpoint{}, err
	}
	var checkpoint ThreadCheckpoint
	if err := json.Unmarshal([]byte(data), &checkpoint); err != nil {
		return ThreadCheckpoint{}, err
	}
	return checkpoint, nil
}

// Store is the SQLite-backed persistence layer for threads and consensus state.
type Store struct {
	db *sql.DB
}

func NewStore(path string) (*Store, error) {
	db, err := sqlite.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open thread db: %w", err)
	}
	if err := migrate(db); err != nil {
		return nil, fmt.Errorf("migrate thread db: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// ─── Threads ──────────────────────────────────────────────────────────────────

func (s *Store) SaveConsensusState(threadID string, cs ConsensusState) error {
	data, _ := json.Marshal(cs)
	_, err := s.db.Exec(`
		INSERT OR REPLACE INTO consensus_state (thread_id, state_json)
		VALUES (?,?)`, threadID, string(data))
	return err
}

func (s *Store) LoadConsensusState(threadID string) (ConsensusState, error) {
	var stateJSON string
	err := s.db.QueryRow(`SELECT state_json FROM consensus_state WHERE thread_id = ?`,
		threadID).Scan(&stateJSON)
	if err == sql.ErrNoRows {
		return ConsensusState{Height: 1, Round: 0, Step: stepPropose, LockedRound: -1, ValidRound: -1}, nil
	}
	if err != nil {
		return ConsensusState{}, err
	}
	var cs ConsensusState
	json.Unmarshal([]byte(stateJSON), &cs) //nolint:errcheck
	return cs, nil
}
