package thread

import (
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

func (s *Store) SaveArchiveAcknowledgement(ack *pb.ArchiveAcknowledgement) error {
	if err := VerifyArchiveAcknowledgement(ack); err != nil {
		return fmt.Errorf("verify archive acknowledgement: %w", err)
	}
	_, err := s.db.Exec(`INSERT OR REPLACE INTO archive_acknowledgements(thread_id,provider_did,block_hash,acknowledged_at,signature) VALUES(?,?,?,?,?)`, ack.ThreadId, ack.ProviderDid, ack.BlockHash, ack.AcknowledgedAtUnixMs, ack.Signature)
	return err
}

func (s *Store) ArchiveAcknowledgements(threadID, blockHash string) ([]*pb.ArchiveAcknowledgement, error) {
	rows, err := s.db.Query(`SELECT thread_id,provider_did,block_hash,acknowledged_at,signature FROM archive_acknowledgements WHERE thread_id=? AND block_hash=?`, threadID, blockHash)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*pb.ArchiveAcknowledgement
	for rows.Next() {
		a := &pb.ArchiveAcknowledgement{}
		if err := rows.Scan(&a.ThreadId, &a.ProviderDid, &a.BlockHash, &a.AcknowledgedAtUnixMs, &a.Signature); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// HasArchiveAcknowledgementQuorum reports whether distinct persisted provider
// receipts meet a configured archive durability policy for one immutable block.
func (s *Store) HasArchiveAcknowledgementQuorum(threadID, blockHash string, required int) (bool, error) {
	if threadID == "" || blockHash == "" || required <= 0 {
		return false, fmt.Errorf("invalid archive acknowledgement policy")
	}
	var count int
	if err := s.db.QueryRow(`SELECT COUNT(DISTINCT provider_did) FROM archive_acknowledgements WHERE thread_id=? AND block_hash=?`, threadID, blockHash).Scan(&count); err != nil {
		return false, err
	}
	return count >= required, nil
}

// SaveArchiveProvider caches a signed provider advertisement. Expired records
// are never retained as eligible replication targets.
func (s *Store) SaveArchiveProvider(record *pb.ArchiveProviderRecord) error {
	if record == nil || record.ProviderDid == "" || record.NodePeerId == "" || record.ExpiresAtUnixMs <= time.Now().UnixMilli() || len(record.Signature) == 0 {
		return fmt.Errorf("invalid archive provider record")
	}
	addrs, err := json.Marshal(record.Multiaddrs)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`INSERT OR REPLACE INTO archive_providers(provider_did,node_peer_id,multiaddrs,expires_at,signature,updated_at) VALUES(?,?,?,?,?,?)`,
		record.ProviderDid, record.NodePeerId, string(addrs), record.ExpiresAtUnixMs, record.Signature, time.Now().UnixMilli())
	return err
}

// ArchiveProviders returns only currently valid provider advertisements.
func (s *Store) ArchiveProviders(limit int) ([]*pb.ArchiveProviderRecord, error) {
	query := `SELECT provider_did,node_peer_id,multiaddrs,expires_at,signature FROM archive_providers WHERE expires_at>? ORDER BY expires_at DESC`
	args := []any{time.Now().UnixMilli()}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	rows, err := s.db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*pb.ArchiveProviderRecord
	for rows.Next() {
		record := &pb.ArchiveProviderRecord{}
		var addrs string
		if err := rows.Scan(&record.ProviderDid, &record.NodePeerId, &addrs, &record.ExpiresAtUnixMs, &record.Signature); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(addrs), &record.Multiaddrs); err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

// PruneArchiveProviders removes expired advertisements and returns how many
// rows were discarded. Stored blocks are deliberately unaffected by this GC.
func (s *Store) PruneArchiveProviders(now int64) (int64, error) {
	result, err := s.db.Exec(`DELETE FROM archive_providers WHERE expires_at<=?`, now)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// ArchiveDiagnostic records an integrity or conflict observation for an
// archive provider. It intentionally contains no plaintext or key material.
type ArchiveDiagnostic struct {
	ThreadID    string
	ProviderDID string
	Kind        string
	Detail      string
	ObservedAt  int64
}

func (s *Store) RecordArchiveDiagnostic(d ArchiveDiagnostic) error {
	if d.ThreadID == "" || d.ProviderDID == "" || d.Kind == "" {
		return fmt.Errorf("invalid archive diagnostic")
	}
	if d.ObservedAt == 0 {
		d.ObservedAt = time.Now().UnixMilli()
	}
	_, err := s.db.Exec(`INSERT INTO archive_diagnostics(thread_id,provider_did,kind,detail,observed_at) VALUES(?,?,?,?,?)`, d.ThreadID, d.ProviderDID, d.Kind, d.Detail, d.ObservedAt)
	return err
}
