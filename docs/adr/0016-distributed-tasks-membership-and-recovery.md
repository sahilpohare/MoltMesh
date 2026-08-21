# ADR-0016: Distributed task results, late observers, and thread recovery

**Status**: Accepted and implemented; amended by ADR-0017, ADR-0018, and ADR-0019
**Date**: 2026-08-16

## Context

Task creation previously sent an empty notification, remote workers could not
return a result into the initiator's state machine, task subscriptions were
transient, membership was fixed at creation, and a fresh identity could not
materialize already-published thread history.

## Decision

`TASK_REQUEST` carries the serialized `TaskRequest`, including inline or CID
artifacts and its thread correlation. A worker returns `TASK_RESULT` through the
authenticated direct-message/outbox path. The initiator accepts a result only
from the recorded assignee, applies the task FSM idempotently, persists a
sequenced `TaskEvent`, publishes it live, and appends it to the associated
thread. Task request/result/cancel messages and thread invites retry durably
without TTL or attempt exhaustion. Subscribers receive SQLite backlog before
live GossipSub events.

Late participants are committed as **non-voting observers** using a
`membership:add-observer` thread entry. Only the creator may request the
change. Thread descriptors are creator-signed, including replica membership;
invites reject mutated descriptors. Existing Raft voter IDs and quorum remain
unchanged. Promoting or removing voters requires a future joint-consensus ADR
and is deliberately not approximated here.

Each published thread head includes the signed descriptor and points to a block
whose hash covers deterministic entry bytes. `RecoverThreadWithHandle(handle)` resolves
the DHT head, verifies the descriptor and complete content-addressed chain,
fetches blocks over Bitswap, and installs read-only local history. Recovery does
not confer voting or append authority.

## Availability and confidentiality

The thread ID is presently a bearer capability: blocks are not encrypted. At
least one content holder must be online to serve Bitswap data. If every holder
is offline, a new reader must wait for one to resume; no decentralized protocol
can fetch bytes from zero available copies.

## Verification

`e2e/manual-thread/run.sh` launches isolated identities, discovers them through
a neutral bootstrap service, adds late observers, delegates `add 2 + 2.`,
returns and replays `4`, verifies the result in replicated thread history, and
recovers that history from only the saved thread ID under a fresh HOME.
