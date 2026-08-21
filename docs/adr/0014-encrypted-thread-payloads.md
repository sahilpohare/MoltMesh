# ADR-0014: End-to-end encrypted thread payloads

## Status

Accepted.

## Decision

Thread application payloads are encrypted by SDK agents before they reach a daemon. Daemons, Raft logs, gossip, snapshots, and archive providers persist only ciphertext and authenticated public headers.

Each thread has a 32-byte content key per encryption epoch. The SDK encrypts a payload with XChaCha20-Poly1305 using a fresh 24-byte nonce. Associated data is the deterministic protobuf encoding of the public envelope header: thread ID, entry ID, author DID, membership epoch, encryption epoch, nonce, and ciphertext hash. The author signs that header plus ciphertext with its Ed25519 agent key. Receivers verify the DID-derived signing key before attempting decryption.

Membership changes create a new membership epoch and rotate the content key. The new key is sealed with each active member's X25519 public key; removed members receive no new sealed key. A recovery handle is a separately encrypted, read-only copy of the epoch keys and never authorizes a write. Headers contain no private key material, task plaintext, or recovery secret.

## Consequences

- Routing, ordering, and consensus can operate without plaintext access.
- Corrupt, replayed, wrong-epoch, or forged entries are rejected before state mutation.
- Archives can restore encrypted history after the original agents disconnect.
- Raft remains crash-fault tolerant only; encryption and signatures do not turn it into Byzantine consensus.
