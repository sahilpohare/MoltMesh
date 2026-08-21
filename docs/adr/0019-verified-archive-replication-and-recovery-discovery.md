# ADR-0019: Verified Archive Replication and Recovery Discovery

**Status**: Accepted and implemented
**Date**: 2026-08-19
**Amends**: ADR-0013 and ADR-0016

## Context

Local content-addressed storage and live replicas are insufficient when thread
members are offline or restarted. Recovery must find both encrypted blocks and
their epoch key envelopes without trusting an arbitrary storage provider.

## Decision

Archive providers replicate committed, hash-linked thread blocks and signed
recovery-key envelopes. They validate the descriptor, block chain, entry
signatures, and creator signature on a recovery envelope before persisting it.
For each retained block, a provider emits a signed archive acknowledgement.

Committed block heads and recovery-envelope manifests are advertised under
thread-scoped DHT records. A recovering client resolves those records, fetches
content-addressed bytes over Bitswap, verifies every hash and signature, and
only then saves the block or envelope locally. Invalid CIDs, broken chains,
wrong-thread records, and corrupt/forged envelopes are rejected rather than
being retried as valid data.

Acknowledgements are persisted locally and published on a per-thread archive
receipt topic. They are evidence of a specific provider retaining a specific
block; they are not a substitute for verifying the retrieved content.

## Consequences

- Availability can outlive an individual member while archive providers learn
  no plaintext or recovery secret.
- Discovery is content-addressed and authenticated; a DHT record is a hint,
  not trusted state.
- Applications may evaluate persisted acknowledgement receipts for their own
  retention policy. The current protocol records receipts but does not block a
  thread commit on a configurable archive quorum.
- If no replica or archive provider is online, content remains temporarily
  unavailable; cryptographic verification cannot create an unavailable copy.

