# ADR-0018: Explicit Membership Lifecycle and Raft Joint Consensus

**Status**: Accepted and implemented
**Date**: 2026-08-19
**Amends**: ADR-0016

## Context

Late observers in ADR-0016 intentionally did not affect the voting quorum.
That was safe for passive replication but left no authenticated lifecycle for
invitation, catch-up, promotion to a voter, removal, or matching encryption-key
rotation to the resulting membership.

## Decision

Thread membership is an explicit, creator-authorized lifecycle:

1. A creator-signed descriptor names the proposed member and membership epoch.
2. The recipient verifies the descriptor and authenticated libp2p peer/DID
   binding before accepting the invite.
3. A replica first joins as an observer and proves it has caught up to the
   creator’s committed head with a signed catch-up attestation.
4. Promotion to voter or removal is committed through Raft `ConfChangeV2`
   joint consensus and is not considered complete until applied.
5. The committed membership epoch drives the next encryption-key envelope
   rotation under ADR-0017.

The descriptor remains creator-signed and durable invites are delivered through
the persistent outbox. Replaying an invite is idempotent; unsigned, mutated,
wrong-epoch, or uncaught-up promotion requests are rejected.

## Consequences

- A newly invited peer cannot silently become a voter.
- Quorum transitions remain safe while old and new configurations overlap.
- Observer replication can be deployed without changing the Raft quorum, while
  promotion and removal have explicit committed semantics.
- Membership state, consensus configuration, and encryption access evolve
  together but remain separately auditable.

