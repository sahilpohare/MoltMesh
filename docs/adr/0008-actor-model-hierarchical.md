# ADR-0008: Hierarchical Actor Model for Agent/Task Isolation

**Status**: Accepted
**Date**: 2026-05-30
**Amended**: 2026-08-16 by ADR-0015

## Context

The daemon manages concurrent tasks, threads, and streams. Failure in one task must not affect others. Two models debated:

- **Flat**: all tasks report to a single supervisor
- **Hierarchical**: tasks are child actors of a TaskSupervisor, which is a child of AgentActor

The PM argued flat is simpler for adoption. The architect argued hierarchical is necessary for correctness under adversarial conditions (remote agents will send malformed messages).

## Decision

**Hierarchical actor model.** The production daemon has one standalone GoAkt
system and one root. High-cardinality children are virtualized and exist only
while active:

```
DaemonSupervisor (root)
├── RegistryActor / NameRegistryActor
├── GossipActor
├── InboxActor / OutboxActor
├── WebhookActor / NetworkStoreActor
├── DeliverySupervisor
│   └── PeerActor[peer-id]...
├── TaskSupervisor
│   └── TaskActor[task-id]...
├── ThreadSupervisor
│   ├── ThreadActor[active thread-id]...
│   └── ThreadGossipActor[active thread-id]...
└── ThreadDurabilityActor
```

Actors own mutable application state and ordering. Normal service SQLite, DHT,
GossipSub and libp2p operations are dispatched with `ReceiveContext.PipeTo`.
Raft Ready persistence is synchronous to preserve its required
Ready/persist/Advance ordering. gRPC handlers, libp2p stream handlers and
subscription loops remain transport adapters. Pure configuration, protobuf and
validation code is deliberately not actorized because it owns no concurrent
state.

Thread actors are durable virtual entities, not permanent processes. Daemon
startup does not enumerate stored threads. Active threads passivate after a
configurable idle interval (five minutes by default), persist an etcd/raft
snapshot, and reconstruct on demand. Appends
durably wake remote replicas through idempotent `THREAD_INVITE` messages in the
outbox. See ADR-0015.

Task actors are created lazily and evicted when their durable task reaches a
terminal state. Peer actors are reference-counted for concurrent stream work
and removed as soon as the last send/receive operation completes. Thus none of
the three high-cardinality domains retains one actor per historical entity.

The original diagram placed a `StreamActor` and `StorageActor` below every
task. The implementation instead isolates streams per authenticated peer and
serializes each task's storage operations in its `TaskActor`. This avoids
creating duplicate stream ownership for tasks sharing a peer and preserves the
same failure boundary.

## Restart Strategy

- **TaskSupervisor**: `one-for-one` — one task crashes, only that task restarts.
- **All actors**: one-for-one restart, at most 3 restarts in 60 seconds, with
  100ms-to-5s exponential backoff. GoAkt suspends an actor when its budget is
  exhausted, preventing restart thrash.
- **AgentActor children**: `one-for-one` — RegistryActor crash does not affect TaskSupervisor.

## Rationale

- Remote agents will send malformed, oversized, or adversarial messages. Tasks must be crash-isolated.
- A TaskActor crash (e.g., stream timeout, bad artifact) must not affect RegistryActor (agent's DHT presence) or other running tasks.
- Hierarchical supervision is the standard approach in mature actor systems (Erlang/OTP, Akka). The complexity is in the framework, not the application code.
- GoAkt supplies mailbox ordering, supervision and restart budgets. It runs in
  standalone mode; libp2p remains the only cross-daemon transport and trust
  boundary.

## Consequences

- Actor residency and recurring ticks scale with active work, not total
  persisted history.
- Blocking adapters are lifecycle-bound to the daemon context. Normal service
  I/O uses `PipeTo`; Raft Ready persistence is the documented crash-consistency
  exception.
- Budget exhaustion suspends the affected actor. It does not invent a
  `TaskFailed` wire event: the current protocol has no such message kind.
- Flat actor model may be revisited if goroutine overhead becomes measurable at >10k concurrent tasks (future ADR).
