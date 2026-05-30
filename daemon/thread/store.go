package thread

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"google.golang.org/protobuf/proto"
)

// Store is the SQLite-backed persistence layer for threads and consensus state.
type Store struct {
	db *sql.DB
}

func NewStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite3", path+"?_journal=WAL&_busy_timeout=5000")
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

func (s *Store) SaveBlock(b *pb.ThreadBlock) error {
	entriesJSON, err := json.Marshal(marshalEntries(b.Entries))
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`
		INSERT OR REPLACE INTO thread_blocks
		  (thread_id, height, round, parent_hash, entries, proposer_did,
		   proposer_sig, block_hash, committed_at)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		b.ThreadId, b.Height, b.Round, b.ParentHash,
		string(entriesJSON), b.ProposerDid, b.ProposerSig,
		b.BlockHash, b.CommittedAt,
	)
	return err
}

func (s *Store) GetBlock(threadID string, height int64) (*pb.ThreadBlock, error) {
	row := s.db.QueryRow(`
		SELECT thread_id, height, round, parent_hash, entries, proposer_did,
		       proposer_sig, block_hash, committed_at
		FROM thread_blocks WHERE thread_id = ? AND height = ? AND committed_at > 0`,
		threadID, height)
	return scanBlock(row)
}

func (s *Store) GetCommittedHeight(threadID string) (int64, error) {
	var h sql.NullInt64
	err := s.db.QueryRow(`
		SELECT MAX(height) FROM thread_blocks
		WHERE thread_id = ? AND committed_at > 0`, threadID).Scan(&h)
	if err != nil {
		return 0, err
	}
	if !h.Valid {
		return 0, nil
	}
	return h.Int64, nil
}

func (s *Store) GetBlocksSince(threadID string, sinceHeight int64, limit int) ([]*pb.ThreadBlock, error) {
	q := `SELECT thread_id, height, round, parent_hash, entries, proposer_did,
	             proposer_sig, block_hash, committed_at
	      FROM thread_blocks
	      WHERE thread_id = ? AND height > ? AND committed_at > 0
	      ORDER BY height ASC`
	args := []any{threadID, sinceHeight}
	if limit > 0 {
		q += fmt.Sprintf(" LIMIT %d", limit)
	}
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var blocks []*pb.ThreadBlock
	for rows.Next() {
		b, err := scanBlock(wrapRow(rows))
		if err != nil {
			return nil, err
		}
		blocks = append(blocks, b)
	}
	return blocks, rows.Err()
}

func scanBlock(row scanner) (*pb.ThreadBlock, error) {
	var b pb.ThreadBlock
	var entriesJSON string
	err := row.Scan(&b.ThreadId, &b.Height, &b.Round, &b.ParentHash,
		&entriesJSON, &b.ProposerDid, &b.ProposerSig, &b.BlockHash, &b.CommittedAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("block not found")
	}
	if err != nil {
		return nil, err
	}
	b.Entries, _ = unmarshalEntries(entriesJSON)
	return &b, nil
}

// ─── Votes ────────────────────────────────────────────────────────────────────

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

func (s *Store) EnqueueEntry(threadID string, entry *pb.ThreadEntry) error {
	data, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`
		INSERT INTO pending_entries (thread_id, payload, submitted_at)
		VALUES (?,?,?)`,
		threadID, data, time.Now().UnixMilli(),
	)
	return err
}

func (s *Store) DequeuePendingEntries(threadID string, limit int) ([]*pb.ThreadEntry, error) {
	rows, err := s.db.Query(`
		SELECT id, payload FROM pending_entries
		WHERE thread_id = ?
		ORDER BY submitted_at ASC LIMIT ?`,
		threadID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []int64
	var entries []*pb.ThreadEntry
	for rows.Next() {
		var id int64
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			return nil, err
		}
		var e pb.ThreadEntry
		if err := proto.Unmarshal(data, &e); err != nil {
			continue
		}
		ids = append(ids, id)
		entries = append(entries, &e)
	}
	rows.Close()
	// delete dequeued
	for _, id := range ids {
		s.db.Exec(`DELETE FROM pending_entries WHERE id = ?`, id) //nolint:errcheck
	}
	return entries, nil
}

// ─── Consensus state ──────────────────────────────────────────────────────────

type ConsensusState struct {
	Height    int64
	Round     int32
	Step      string // propose | prevote | precommit | commit
	LockedRound int32
	LockedHash  string
	ValidRound  int32
	ValidHash   string
}

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

// ─── migrations ───────────────────────────────────────────────────────────────

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS threads (
		id          TEXT PRIMARY KEY,
		creator_did TEXT NOT NULL,
		replica_dids TEXT NOT NULL DEFAULT '[]',
		n           INTEGER NOT NULL DEFAULT 4,
		f           INTEGER NOT NULL DEFAULT 1,
		epoch_ms    INTEGER NOT NULL DEFAULT 1000,
		created_at  INTEGER NOT NULL,
		metadata    TEXT NOT NULL DEFAULT '{}'
	);
	CREATE TABLE IF NOT EXISTS thread_blocks (
		thread_id    TEXT NOT NULL,
		height       INTEGER NOT NULL,
		round        INTEGER NOT NULL,
		parent_hash  TEXT NOT NULL DEFAULT '',
		entries      TEXT NOT NULL DEFAULT '[]',
		proposer_did TEXT NOT NULL,
		proposer_sig TEXT NOT NULL,
		block_hash   TEXT NOT NULL,
		committed_at INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY (thread_id, height)
	);
	CREATE INDEX IF NOT EXISTS idx_blocks_height ON thread_blocks(thread_id, height);
	CREATE TABLE IF NOT EXISTS thread_votes (
		thread_id  TEXT NOT NULL,
		height     INTEGER NOT NULL,
		round      INTEGER NOT NULL,
		vote_type  INTEGER NOT NULL,
		voter_did  TEXT NOT NULL,
		block_hash TEXT NOT NULL DEFAULT '',
		signature  TEXT NOT NULL,
		received_at INTEGER NOT NULL,
		PRIMARY KEY (thread_id, height, round, vote_type, voter_did)
	);
	CREATE TABLE IF NOT EXISTS pending_entries (
		id          INTEGER PRIMARY KEY AUTOINCREMENT,
		thread_id   TEXT NOT NULL,
		payload     BLOB NOT NULL,
		submitted_at INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_pending_thread ON pending_entries(thread_id, submitted_at);
	CREATE TABLE IF NOT EXISTS consensus_state (
		thread_id  TEXT PRIMARY KEY,
		state_json TEXT NOT NULL
	);
	`)
	return err
}

// ─── helpers ──────────────────────────────────────────────────────────────────

type scanner interface {
	Scan(dest ...any) error
}

type rowWrapper struct{ rows *sql.Rows }

func wrapRow(rows *sql.Rows) scanner { return &rowWrapper{rows} }

func (r *rowWrapper) Scan(dest ...any) error { return r.rows.Scan(dest...) }

type entryJSON struct {
	AuthorDid   string `json:"author_did"`
	Payload     []byte `json:"payload"`
	Kind        string `json:"kind"`
	AuthorSig   string `json:"author_sig"`
	SubmittedAt int64  `json:"submitted_at"`
}

func marshalEntries(entries []*pb.ThreadEntry) []entryJSON {
	out := make([]entryJSON, 0, len(entries))
	for _, e := range entries {
		out = append(out, entryJSON{
			AuthorDid:   e.AuthorDid,
			Payload:     e.Payload,
			Kind:        e.Kind,
			AuthorSig:   e.AuthorSig,
			SubmittedAt: e.SubmittedAt,
		})
	}
	return out
}

func unmarshalEntries(raw string) ([]*pb.ThreadEntry, error) {
	var ejs []entryJSON
	if err := json.Unmarshal([]byte(raw), &ejs); err != nil {
		return nil, err
	}
	out := make([]*pb.ThreadEntry, 0, len(ejs))
	for _, ej := range ejs {
		out = append(out, &pb.ThreadEntry{
			AuthorDid:   ej.AuthorDid,
			Payload:     ej.Payload,
			Kind:        ej.Kind,
			AuthorSig:   ej.AuthorSig,
			SubmittedAt: ej.SubmittedAt,
		})
	}
	return out, nil
}
