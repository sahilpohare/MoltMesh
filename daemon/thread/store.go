package thread

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
	"github.com/sahilpohare/p2p-a2a/pkg/sqlite"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

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

func (s *Store) SaveMember(threadID string, member *pb.ThreadMember) error {
	if threadID == "" || member == nil || member.Did == "" || member.Role == pb.ThreadMemberRole_THREAD_MEMBER_ROLE_UNSPECIFIED {
		return fmt.Errorf("invalid thread member")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
	if _, err := tx.Exec(`INSERT OR REPLACE INTO thread_members(thread_id,did,role,joined_epoch) VALUES(?,?,?,?)`, threadID, member.Did, int(member.Role), member.JoinedEpoch); err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT OR IGNORE INTO thread_membership_epochs(thread_id,membership_epoch) VALUES(?,1)`, threadID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) MembershipEpoch(threadID string) (uint64, error) {
	var epoch uint64
	err := s.db.QueryRow(`SELECT membership_epoch FROM thread_membership_epochs WHERE thread_id=?`, threadID).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		return 1, nil
	}
	return epoch, err
}

func (s *Store) AdvanceMembershipEpoch(threadID string) (uint64, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	var epoch uint64
	err = tx.QueryRow(`SELECT membership_epoch FROM thread_membership_epochs WHERE thread_id=?`, threadID).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		epoch = 1
		if _, err = tx.Exec(`INSERT INTO thread_membership_epochs(thread_id,membership_epoch) VALUES(?,?)`, threadID, epoch); err != nil {
			return 0, err
		}
	} else if err != nil {
		return 0, err
	} else {
		epoch++
		if _, err = tx.Exec(`UPDATE thread_membership_epochs SET membership_epoch=? WHERE thread_id=?`, epoch, threadID); err != nil {
			return 0, err
		}
	}
	return epoch, tx.Commit()
}

// advanceMembershipEpochTx must be called in the same transaction as the
// member mutation. A membership change is not visible without its new epoch.
func advanceMembershipEpochTx(tx *sql.Tx, threadID string) (uint64, error) {
	var epoch uint64
	err := tx.QueryRow(`SELECT membership_epoch FROM thread_membership_epochs WHERE thread_id=?`, threadID).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		// Threads created before membership epochs were introduced start at 1.
		epoch = 1
		if _, err := tx.Exec(`INSERT INTO thread_membership_epochs(thread_id,membership_epoch) VALUES(?,?)`, threadID, epoch); err != nil {
			return 0, err
		}
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	epoch++
	if _, err := tx.Exec(`UPDATE thread_membership_epochs SET membership_epoch=? WHERE thread_id=?`, epoch, threadID); err != nil {
		return 0, err
	}
	return epoch, nil
}

func (s *Store) ListMembers(threadID string) ([]*pb.ThreadMember, error) {
	rows, err := s.db.Query(`SELECT did,role,joined_epoch FROM thread_members WHERE thread_id=? ORDER BY did`, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*pb.ThreadMember
	for rows.Next() {
		m := &pb.ThreadMember{}
		var role int
		if err := rows.Scan(&m.Did, &role, &m.JoinedEpoch); err != nil {
			return nil, err
		}
		m.Role = pb.ThreadMemberRole(role)
		out = append(out, m)
	}
	return out, rows.Err()
}

func (s *Store) RemoveMember(threadID, did string) error {
	_, err := s.RemoveMemberWithEpoch(threadID, did)
	return err
}

// RemoveMemberWithEpoch removes a member and advances the membership epoch in
// one durable transaction. It refuses to remove the last voting member.
func (s *Store) RemoveMemberWithEpoch(threadID, did string) (uint64, error) {
	if threadID == "" || did == "" {
		return 0, fmt.Errorf("invalid thread member")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	var role int
	if err := tx.QueryRow(`SELECT role FROM thread_members WHERE thread_id=? AND did=?`, threadID, did).Scan(&role); err != nil {
		return 0, err
	}
	if pb.ThreadMemberRole(role) == pb.ThreadMemberRole_THREAD_MEMBER_ROLE_VOTER || pb.ThreadMemberRole(role) == pb.ThreadMemberRole_THREAD_MEMBER_ROLE_ADMIN {
		var voters int
		if err := tx.QueryRow(`SELECT COUNT(*) FROM thread_members WHERE thread_id=? AND role IN (?,?)`, threadID, int(pb.ThreadMemberRole_THREAD_MEMBER_ROLE_VOTER), int(pb.ThreadMemberRole_THREAD_MEMBER_ROLE_ADMIN)).Scan(&voters); err != nil {
			return 0, err
		}
		if voters <= 1 {
			return 0, fmt.Errorf("cannot remove last voter")
		}
	}
	if _, err := tx.Exec(`DELETE FROM thread_members WHERE thread_id=? AND did=?`, threadID, did); err != nil {
		return 0, err
	}
	epoch, err := advanceMembershipEpochTx(tx, threadID)
	if err != nil {
		return 0, err
	}
	return epoch, tx.Commit()
}

func (s *Store) PromoteMember(threadID, did string) (*pb.ThreadMember, error) {
	member, _, err := s.PromoteMemberWithEpoch(threadID, did)
	return member, err
}

// PromoteMemberWithEpoch promotes an observer after its history catch-up has
// been verified by the caller, atomically advancing the membership epoch.
func (s *Store) PromoteMemberWithEpoch(threadID, did string) (*pb.ThreadMember, uint64, error) {
	if threadID == "" || did == "" {
		return nil, 0, fmt.Errorf("invalid thread member")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback() //nolint:errcheck
	var role int
	var epoch uint64
	if err := tx.QueryRow(`SELECT role,joined_epoch FROM thread_members WHERE thread_id=? AND did=?`, threadID, did).Scan(&role, &epoch); err != nil {
		return nil, 0, err
	}
	if pb.ThreadMemberRole(role) != pb.ThreadMemberRole_THREAD_MEMBER_ROLE_OBSERVER {
		return nil, 0, fmt.Errorf("only observers may be promoted")
	}
	if _, err := tx.Exec(`UPDATE thread_members SET role=? WHERE thread_id=? AND did=?`, int(pb.ThreadMemberRole_THREAD_MEMBER_ROLE_VOTER), threadID, did); err != nil {
		return nil, 0, err
	}
	membershipEpoch, err := advanceMembershipEpochTx(tx, threadID)
	if err != nil {
		return nil, 0, err
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, err
	}
	return &pb.ThreadMember{Did: did, Role: pb.ThreadMemberRole_THREAD_MEMBER_ROLE_VOTER, JoinedEpoch: epoch}, membershipEpoch, nil
}

func (s *Store) SaveInvite(threadID, invitee string, role pb.ThreadMemberRole, nonce []byte, expiresAt int64) error {
	if threadID == "" || invitee == "" || role == pb.ThreadMemberRole_THREAD_MEMBER_ROLE_UNSPECIFIED || len(nonce) == 0 || expiresAt <= time.Now().UnixMilli() {
		return fmt.Errorf("invalid thread invite")
	}
	_, err := s.db.Exec(`INSERT OR REPLACE INTO thread_invites(thread_id,invitee_did,role,nonce,expires_at,accepted_at) VALUES(?,?,?,?,?,0)`, threadID, invitee, int(role), nonce, expiresAt)
	return err
}

func (s *Store) AcceptInvite(threadID, invitee string, nonce []byte) (*pb.ThreadMember, error) {
	member, _, err := s.AcceptInviteWithEpoch(threadID, invitee, nonce)
	return member, err
}

// AcceptInviteWithEpoch consumes a signed-invite record exactly once and
// makes the observer visible at the new membership epoch.
func (s *Store) AcceptInviteWithEpoch(threadID, invitee string, nonce []byte) (*pb.ThreadMember, uint64, error) {
	if threadID == "" || invitee == "" || len(nonce) == 0 {
		return nil, 0, fmt.Errorf("invalid thread invite")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()
	var role int
	var expires, accepted int64
	if err := tx.QueryRow(`SELECT role,expires_at,accepted_at FROM thread_invites WHERE thread_id=? AND invitee_did=? AND nonce=?`, threadID, invitee, nonce).Scan(&role, &expires, &accepted); err != nil {
		return nil, 0, err
	}
	if accepted != 0 || expires <= time.Now().UnixMilli() {
		return nil, 0, fmt.Errorf("thread invite expired or already accepted")
	}
	membershipEpoch, err := advanceMembershipEpochTx(tx, threadID)
	if err != nil {
		return nil, 0, err
	}
	member := &pb.ThreadMember{Did: invitee, Role: pb.ThreadMemberRole(role), JoinedEpoch: membershipEpoch}
	if _, err := tx.Exec(`INSERT OR REPLACE INTO thread_members(thread_id,did,role,joined_epoch) VALUES(?,?,?,?)`, threadID, member.Did, int(member.Role), member.JoinedEpoch); err != nil {
		return nil, 0, err
	}
	if _, err := tx.Exec(`UPDATE thread_invites SET accepted_at=? WHERE thread_id=? AND invitee_did=? AND nonce=? AND accepted_at=0`, time.Now().UnixMilli(), threadID, invitee, nonce); err != nil {
		return nil, 0, err
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, err
	}
	return member, membershipEpoch, nil
}

// SaveKeyEnvelope persists an opaque per-recipient epoch-key envelope. The
// daemon deliberately never decrypts it; only the SDK recipient (or recovery
// capability holder) possesses the material needed to unwrap its ciphertext.
func (s *Store) SaveKeyEnvelope(envelope *pb.ThreadKeyEnvelope) error {
	if envelope == nil || envelope.ThreadId == "" || envelope.RecipientDid == "" || envelope.EncryptionEpoch == 0 || len(envelope.Ciphertext) == 0 {
		return fmt.Errorf("invalid thread key envelope")
	}
	if len(envelope.EphemeralPublicKey) != 32 || len(envelope.Nonce) != 24 || len(envelope.Ciphertext) > 64*1024 {
		return fmt.Errorf("invalid thread key envelope size")
	}
	_, err := s.db.Exec(`INSERT OR REPLACE INTO thread_key_envelopes
		(thread_id, encryption_epoch, recipient_did, ephemeral_public_key, nonce, ciphertext, recovery_envelope)
		VALUES (?,?,?,?,?,?,?)`, envelope.ThreadId, envelope.EncryptionEpoch, envelope.RecipientDid,
		envelope.EphemeralPublicKey, envelope.Nonce, envelope.Ciphertext, envelope.RecoveryEnvelope)
	return err
}

func (s *Store) KeyEnvelopes(threadID string, epoch uint64, recipientDID string) ([]*pb.ThreadKeyEnvelope, error) {
	rows, err := s.db.Query(`SELECT thread_id,encryption_epoch,recipient_did,ephemeral_public_key,nonce,ciphertext,recovery_envelope
		FROM thread_key_envelopes WHERE thread_id=? AND encryption_epoch=? AND recipient_did=?`, threadID, epoch, recipientDID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*pb.ThreadKeyEnvelope
	for rows.Next() {
		e := &pb.ThreadKeyEnvelope{}
		if err := rows.Scan(&e.ThreadId, &e.EncryptionEpoch, &e.RecipientDid, &e.EphemeralPublicKey, &e.Nonce, &e.Ciphertext, &e.RecoveryEnvelope); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// SaveRecoveryKeyEnvelope persists an opaque key copy addressed to the
// recovery capability. It is deliberately isolated from ordinary recipient
// envelopes so a member session cannot enumerate or retrieve it.
func (s *Store) SaveRecoveryKeyEnvelope(envelope *pb.ThreadKeyEnvelope) error {
	if envelope == nil || !envelope.RecoveryEnvelope {
		return fmt.Errorf("invalid recovery key envelope")
	}
	if envelope.ThreadId == "" || envelope.EncryptionEpoch == 0 || len(envelope.EphemeralPublicKey) != 32 || len(envelope.Nonce) != 24 || len(envelope.Ciphertext) == 0 || len(envelope.Ciphertext) > 64*1024 {
		return fmt.Errorf("invalid recovery key envelope")
	}
	_, err := s.db.Exec(`INSERT OR REPLACE INTO thread_recovery_key_envelopes
		(thread_id,encryption_epoch,ephemeral_public_key,nonce,ciphertext)
		VALUES (?,?,?,?,?)`, envelope.ThreadId, envelope.EncryptionEpoch, envelope.EphemeralPublicKey, envelope.Nonce, envelope.Ciphertext)
	return err
}

func (s *Store) RecoveryKeyEnvelopes(threadID string) ([]*pb.ThreadKeyEnvelope, error) {
	rows, err := s.db.Query(`SELECT thread_id,encryption_epoch,ephemeral_public_key,nonce,ciphertext
		FROM thread_recovery_key_envelopes WHERE thread_id=? ORDER BY encryption_epoch`, threadID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*pb.ThreadKeyEnvelope
	for rows.Next() {
		envelope := &pb.ThreadKeyEnvelope{RecoveryEnvelope: true}
		if err := rows.Scan(&envelope.ThreadId, &envelope.EncryptionEpoch, &envelope.EphemeralPublicKey, &envelope.Nonce, &envelope.Ciphertext); err != nil {
			return nil, err
		}
		out = append(out, envelope)
	}
	return out, rows.Err()
}

// SaveRecoveryCapability stores only a SHA-256 digest of a recovery secret.
func (s *Store) SaveRecoveryCapability(threadID string, secret []byte) error {
	if threadID == "" || len(secret) != 32 {
		return fmt.Errorf("invalid recovery capability")
	}
	digest := sha256.Sum256(secret)
	_, err := s.db.Exec(`INSERT OR REPLACE INTO thread_recovery_capabilities(thread_id, secret_hash, created_at) VALUES(?,?,?)`, threadID, digest[:], time.Now().UnixMilli())
	return err
}

func (s *Store) AuthorizeRecoveryCapability(threadID string, secret []byte) (bool, error) {
	if threadID == "" || len(secret) != 32 {
		return false, nil
	}
	var expected []byte
	if err := s.db.QueryRow(`SELECT secret_hash FROM thread_recovery_capabilities WHERE thread_id=?`, threadID).Scan(&expected); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	digest := sha256.Sum256(secret)
	return len(expected) == len(digest) && string(expected) == string(digest[:]), nil
}

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
	CREATE TABLE IF NOT EXISTS thread_key_envelopes (
		thread_id TEXT NOT NULL,
		encryption_epoch INTEGER NOT NULL,
		recipient_did TEXT NOT NULL,
		ephemeral_public_key BLOB NOT NULL,
		nonce BLOB NOT NULL,
		ciphertext BLOB NOT NULL,
		recovery_envelope INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY(thread_id, encryption_epoch, recipient_did)
	);
	CREATE TABLE IF NOT EXISTS thread_recovery_key_envelopes (
		thread_id TEXT NOT NULL,
		encryption_epoch INTEGER NOT NULL,
		ephemeral_public_key BLOB NOT NULL,
		nonce BLOB NOT NULL,
		ciphertext BLOB NOT NULL,
		PRIMARY KEY(thread_id, encryption_epoch)
	);
	CREATE TABLE IF NOT EXISTS thread_members (
		thread_id TEXT NOT NULL,
		did TEXT NOT NULL,
		role INTEGER NOT NULL,
		joined_epoch INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY(thread_id,did)
	);
	CREATE TABLE IF NOT EXISTS thread_membership_epochs (thread_id TEXT PRIMARY KEY, membership_epoch INTEGER NOT NULL);
	CREATE TABLE IF NOT EXISTS archive_acknowledgements (
		thread_id TEXT NOT NULL, provider_did TEXT NOT NULL, block_hash TEXT NOT NULL,
		acknowledged_at INTEGER NOT NULL, signature BLOB NOT NULL,
		PRIMARY KEY(thread_id,provider_did,block_hash)
	);
	CREATE TABLE IF NOT EXISTS archive_providers (
		provider_did TEXT PRIMARY KEY, node_peer_id TEXT NOT NULL, multiaddrs TEXT NOT NULL,
		expires_at INTEGER NOT NULL, signature BLOB NOT NULL, updated_at INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_archive_providers_expiry ON archive_providers(expires_at);
	CREATE TABLE IF NOT EXISTS archive_diagnostics (
		id INTEGER PRIMARY KEY AUTOINCREMENT, thread_id TEXT NOT NULL, provider_did TEXT NOT NULL,
		kind TEXT NOT NULL, detail TEXT NOT NULL, observed_at INTEGER NOT NULL
	);
	CREATE TABLE IF NOT EXISTS thread_invites (
		thread_id TEXT NOT NULL, invitee_did TEXT NOT NULL, role INTEGER NOT NULL,
		nonce BLOB NOT NULL, expires_at INTEGER NOT NULL, accepted_at INTEGER NOT NULL DEFAULT 0,
		PRIMARY KEY(thread_id,invitee_did,nonce)
	);
	CREATE TABLE IF NOT EXISTS thread_recovery_capabilities (
		thread_id TEXT PRIMARY KEY,
		secret_hash BLOB NOT NULL,
		created_at INTEGER NOT NULL
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
	CREATE TABLE IF NOT EXISTS pending_entry_claims (
		pending_id INTEGER PRIMARY KEY,
		claimed_at INTEGER NOT NULL,
		FOREIGN KEY(pending_id) REFERENCES pending_entries(id) ON DELETE CASCADE
	);
	CREATE TABLE IF NOT EXISTS consensus_state (
		thread_id  TEXT PRIMARY KEY,
		state_json TEXT NOT NULL
	);
	CREATE TABLE IF NOT EXISTS thread_checkpoints (
		thread_id       TEXT PRIMARY KEY,
		checkpoint_json TEXT NOT NULL,
		updated_at      INTEGER NOT NULL,
		FOREIGN KEY(thread_id) REFERENCES threads(id) ON DELETE CASCADE
	);
	CREATE TABLE IF NOT EXISTS raft_hard_state (
		thread_id TEXT PRIMARY KEY,
		state BLOB NOT NULL
	);
	CREATE TABLE IF NOT EXISTS raft_entries (
		thread_id TEXT NOT NULL,
		log_index INTEGER NOT NULL,
		term INTEGER NOT NULL,
		entry BLOB NOT NULL,
		PRIMARY KEY (thread_id, log_index)
	);
	CREATE TABLE IF NOT EXISTS raft_snapshots (
		thread_id TEXT PRIMARY KEY,
		snapshot BLOB NOT NULL,
		updated_at INTEGER NOT NULL
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
