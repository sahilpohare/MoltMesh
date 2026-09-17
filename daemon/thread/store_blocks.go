package thread

import (
	"database/sql"
	"encoding/json"
	"fmt"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

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

// ImportHistory atomically publishes a verified recovery result. Callers must
// verify descriptor and chain before this method; if any block cannot be
// written, neither descriptor nor partial history becomes visible.
func (s *Store) ImportHistory(t *pb.Thread, blocks []*pb.ThreadBlock) error {
	if t == nil || t.Id == "" {
		return fmt.Errorf("invalid recovery thread")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	replicas, _ := json.Marshal(t.ReplicaDids)
	meta, _ := json.Marshal(t.Metadata)
	if _, err = tx.Exec(`INSERT OR REPLACE INTO threads(id,creator_did,replica_dids,n,f,epoch_ms,created_at,metadata) VALUES(?,?,?,?,?,?,?,?)`, t.Id, t.CreatorDid, string(replicas), t.N, t.F, t.EpochMs, t.CreatedAt, string(meta)); err != nil {
		return err
	}
	for _, b := range blocks {
		entries, e := json.Marshal(marshalEntries(b.Entries))
		if e != nil {
			return e
		}
		if _, e = tx.Exec(`INSERT OR REPLACE INTO thread_blocks(thread_id,height,round,parent_hash,entries,proposer_did,proposer_sig,block_hash,committed_at) VALUES(?,?,?,?,?,?,?,?,?)`, b.ThreadId, b.Height, b.Round, b.ParentHash, string(entries), b.ProposerDid, b.ProposerSig, b.BlockHash, b.CommittedAt); e != nil {
			return e
		}
	}
	return tx.Commit()
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

// CommittedHead is the locally verified history point used to bind an
// observer catch-up attestation to one immutable chain state.
func (s *Store) CommittedHead(threadID string) (int64, string, error) {
	height, err := s.GetCommittedHeight(threadID)
	if err != nil || height == 0 {
		return height, "", err
	}
	block, err := s.GetBlock(threadID, height)
	if err != nil {
		return 0, "", err
	}
	return height, block.BlockHash, nil
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
