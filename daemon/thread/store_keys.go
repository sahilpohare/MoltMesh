package thread

import (
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"time"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

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
