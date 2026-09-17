package thread

import (
	"database/sql"
	"encoding/json"

	pb "github.com/sahilpohare/p2p-a2a/gen/a2a/v1"
)

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
