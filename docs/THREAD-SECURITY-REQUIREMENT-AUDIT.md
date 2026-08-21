# Thread security and durability requirement audit

This is a current-state audit, not a completion claim. Status is based on
source and focused test evidence as of this revision.

| Requirement | Status | Current evidence / gap |
|---|---|---|
| Signed invitations bound to authenticated SDK identity | Partial | `InviteThreadMember` verifies a deterministic signed invitation and `AcceptThreadInvite` checks the session DID. End-to-end cross-daemon coverage is still absent. |
| Promotion and leave handlers | Partial | RPC and SDK methods exist; member records and epochs change atomically. Promotion now requires a fresh observer-DID-signed attestation matching the creator's current committed height/head hash, and voter promotion/removal waits for the corresponding committed Raft `ConfChangeV2` before updating the member table. A multi-daemon proof-exchange test remains required. |
| Membership epochs | Implemented locally | SQLite transaction tests cover acceptance, promotion, and removal epoch progression. Epoch state is not yet committed into the replicated log. |
| Raft joint consensus | Partial | Raft voter transitions carry correlation tokens and return only after their `ConfChangeV2` is committed/applied; the actor path has a single-node observer-promotion test. A multi-node joint-consensus/catch-up test remains required. |
| Epoch key envelopes | Partial | X25519/HKDF/XChaCha wrapping primitives, durable storage, authenticated put/fetch RPCs, and Python SDK wrap/unwrap/cache/rotation helpers exist. Envelope publication is restricted to the current committed epoch; Python promotion/removal now rotate automatically using that epoch. Invite acceptance still requires the creator to receive the committed change, and TypeScript crypto parity is missing. |
| End-to-end encrypted thread entries | Partial | Ciphertext header signing and daemon validation exist; the Python SDK now builds, signs, encrypts, and decrypts v2 entries from cached epoch keys. The daemon rejects ciphertext from non-members or a stale membership/encryption epoch. Legacy plaintext append remains accepted, and no multi-daemon encrypted restart test proves confidentiality/recovery. |
| Archive provider discovery and replication | Partial | Committed blocks advertise a DHT provider rendezvous; recovery and a periodic archive worker discover and connect candidates, fetch over Bitswap, independently verify the full chain, retain verified blocks, re-advertise, and create signed local receipts. Receipts propagate on a per-thread pubsub channel and are signature-checked before storage. A configured receipt quorum is still not enforced before a durability-sensitive action succeeds. |
| Archive integrity/conflict diagnostics | Partial | The archive worker refuses an invalid descriptor or chain before retention, and a diagnostic persistence table exists. Provider-attributed corruption/conflict reporting is not yet wired because DHT/Bitswap fetches do not identify a single serving DID. |
| Archive retention | Partial | Expired provider records are pruned. Retention of blocks/envelopes based on contracts, pins, and policy is not implemented. |
| Recovery capability | Partial | Handle secrets are hashed at rest and required by `RecoverThreadWithHandle`; bare-ID recovery rejects, and both CLIs invoke the capability-gated path. A creator-signed descriptor now carries only the SHA-256 commitment, letting a fresh daemon validate the holder's secret before it imports history. Recovery-scoped X25519 envelopes are separately stored, capability-gated on retrieval, and unwrap into the Python SDK key cache. They are replicated as opaque ciphertext to subscribed archive workers, and the Python SDK exposes idempotent history-then-key recovery stages. Offline archival discovery/replay of envelopes and a full encrypted recovery test are still missing. |
| SDK membership APIs/bindings | Implemented | Python/TypeScript invitation, accept, promotion, removal, leave, and envelope transport methods are present; generated bindings were refreshed. |
| Full encrypted restart/recovery E2E | Missing | Only focused unit/package checks currently pass. A multi-daemon fault-injection suite remains required. |

## Completion gate

Do not mark the thread objective complete until every row is implemented and
covered by a multi-daemon test that includes encrypted entries, membership
changes, restart, archive loss/corruption, and capability-gated recovery.
