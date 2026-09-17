package thread

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"google.golang.org/protobuf/proto"
)

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

type PendingBatch struct {
	IDs     []int64
	Entries []*pb.ThreadEntry
}

// ClaimPendingEntries reserves durable inputs without deleting them. Claims
// survive concurrent ticks but are reset when an actor reconstructs after a
// crash; committed blocks delete their claims and inputs atomically.
func (s *Store) ClaimPendingEntries(threadID string, limit int) (PendingBatch, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return PendingBatch{}, err
	}
	defer tx.Rollback() //nolint:errcheck
	rows, err := tx.Query(`
		SELECT p.id, p.payload FROM pending_entries p
		WHERE p.thread_id = ? AND NOT EXISTS (
			SELECT 1 FROM pending_entry_claims c WHERE c.pending_id = p.id
		)
		ORDER BY p.submitted_at ASC LIMIT ?`, threadID, limit)
	if err != nil {
		return PendingBatch{}, err
	}
	var batch PendingBatch
	for rows.Next() {
		var id int64
		var data []byte
		if err := rows.Scan(&id, &data); err != nil {
			rows.Close()
			return PendingBatch{}, err
		}
		var entry pb.ThreadEntry
		if err := proto.Unmarshal(data, &entry); err != nil {
			continue
		}
		batch.IDs = append(batch.IDs, id)
		batch.Entries = append(batch.Entries, &entry)
	}
	if err := rows.Close(); err != nil {
		return PendingBatch{}, err
	}
	for _, id := range batch.IDs {
		if _, err := tx.Exec(`INSERT INTO pending_entry_claims (pending_id, claimed_at) VALUES (?, ?)`, id, time.Now().UnixMilli()); err != nil {
			return PendingBatch{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return PendingBatch{}, err
	}
	return batch, nil
}

func (s *Store) ReleasePendingClaims(threadID string) error {
	_, err := s.db.Exec(`DELETE FROM pending_entry_claims WHERE pending_id IN (SELECT id FROM pending_entries WHERE thread_id = ?)`, threadID)
	return err
}

func (s *Store) ReleasePendingClaimIDs(ids []int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	for _, id := range ids {
		if _, err := tx.Exec(`DELETE FROM pending_entry_claims WHERE pending_id = ?`, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) PendingEntryCount(threadID string) (int, error) {
	var count int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM pending_entries WHERE thread_id = ?`, threadID).Scan(&count)
	return count, err
}

// AppendBlockAndAckPending allocates the next height, links the block to its
// parent, and deletes the write-ahead inputs, all in one transaction.
//
// Height allocation must be inside that transaction. Reading MAX(height)
// beforehand and inserting afterwards let two commits on the same thread read
// the same height and build two different blocks for it; the table is keyed by
// (thread_id, height), so the second INSERT OR REPLACE silently overwrote the
// first and a committed block was lost from the chain.
//
// fill receives the allocated height and parent hash and returns the finished
// block, so the block hash is computed over the values actually written.
func (s *Store) AppendBlockAndAckPending(threadID string, pendingIDs []int64, fill func(height int64, parentHash string) *pb.ThreadBlock) (*pb.ThreadBlock, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck

	var maxHeight sql.NullInt64
	if err := tx.QueryRow(`
		SELECT MAX(height) FROM thread_blocks
		WHERE thread_id = ? AND committed_at > 0`, threadID).Scan(&maxHeight); err != nil {
		return nil, err
	}
	height := int64(1)
	parentHash := ""
	if maxHeight.Valid {
		height = maxHeight.Int64 + 1
		if err := tx.QueryRow(`
			SELECT block_hash FROM thread_blocks
			WHERE thread_id = ? AND height = ?`, threadID, maxHeight.Int64).Scan(&parentHash); err != nil && err != sql.ErrNoRows {
			return nil, err
		}
	}

	b := fill(height, parentHash)
	if b == nil {
		return nil, fmt.Errorf("thread %s: no block produced for height %d", threadID, height)
	}
	entriesJSON, err := json.Marshal(marshalEntries(b.Entries))
	if err != nil {
		return nil, err
	}
	// Plain INSERT: (thread_id, height) is the primary key, so a height that
	// already holds a block is a bug rather than something to overwrite.
	if _, err := tx.Exec(`
		INSERT INTO thread_blocks
		  (thread_id, height, round, parent_hash, entries, proposer_did,
		   proposer_sig, block_hash, committed_at)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		b.ThreadId, b.Height, b.Round, b.ParentHash, string(entriesJSON),
		b.ProposerDid, b.ProposerSig, b.BlockHash, b.CommittedAt); err != nil {
		return nil, err
	}
	for _, id := range pendingIDs {
		if _, err := tx.Exec(`DELETE FROM pending_entries WHERE id = ?`, id); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(`DELETE FROM pending_entry_claims WHERE pending_id = ?`, id); err != nil {
			return nil, err
		}
	}
	return b, tx.Commit()
}

// SaveBlockAndAckPending is the durability boundary: a committed block and
// deletion of its write-ahead inputs happen in one SQLite transaction.
func (s *Store) SaveBlockAndAckPending(b *pb.ThreadBlock, pendingIDs []int64) error {
	entriesJSON, err := json.Marshal(marshalEntries(b.Entries))
	if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.Exec(`
		INSERT OR REPLACE INTO thread_blocks
		  (thread_id, height, round, parent_hash, entries, proposer_did,
		   proposer_sig, block_hash, committed_at)
		VALUES (?,?,?,?,?,?,?,?,?)`,
		b.ThreadId, b.Height, b.Round, b.ParentHash, string(entriesJSON),
		b.ProposerDid, b.ProposerSig, b.BlockHash, b.CommittedAt); err != nil {
		return err
	}
	for _, id := range pendingIDs {
		if _, err := tx.Exec(`DELETE FROM pending_entries WHERE id = ?`, id); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM pending_entry_claims WHERE pending_id = ?`, id); err != nil {
			return err
		}
	}
	return tx.Commit()
}
