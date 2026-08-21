# ADR-0017: Versioned Thread Key Envelopes and Capability Recovery

**Status**: Accepted and implemented
**Date**: 2026-08-19
**Amends**: ADR-0014 and the recovery/confidentiality portions of ADR-0016

## Context

ADR-0014 chose encrypted thread payloads, but did not define how a member
obtains a thread key, how a key changes when membership changes, or how a
fresh identity can recover encrypted history without making a public thread ID
a decryption credential. ADR-0016 consequently described recovery as
read-only history retrieval and incorrectly characterized a thread ID as a
bearer capability.

The implementation now needs portable recovery and key rotation while archive
providers and consensus nodes continue to store only ciphertext.

## Decision

Each thread entry carries a versioned public encryption envelope. Application
payloads are encrypted with an epoch-specific 32-byte content key using
XChaCha20-Poly1305. The public header is bound as associated data and signed
by the entry author; it includes the thread, entry, author, membership epoch,
encryption epoch, nonce, and ciphertext hash.

For every active member and encryption epoch, the content key is sealed into a
signed `ThreadKeyEnvelope` using that member’s X25519 public key. Membership
changes rotate the encryption epoch. New members receive only the new key;
removed members receive no envelope for later epochs.

`CreateThread --with-recovery` returns a random recovery secret once. The
thread descriptor stores only its SHA-256 commitment. Recovery APIs require
the secret, verify it against that commitment, resolve creator-signed recovery
envelopes through DHT/Bitswap, and install the resulting history as read-only.
A thread ID alone can discover no usable keys and grants neither decryption nor
write authority.

## Consequences

- Raft logs, snapshots, GossipSub, and archive storage contain authenticated
  ciphertext plus public routing headers, not application plaintext.
- Key-envelope signatures and descriptor signatures are verified before a key
  or recovered history is accepted.
- Recovery handles must be treated as high-value secrets. Loss of the secret
  makes encrypted recovery unavailable; possession does not grant membership,
  consensus, or append rights.
- SDKs expose create-with-recovery, envelope handling, and staged/full
  recovery APIs rather than asking applications to construct crypto metadata.

