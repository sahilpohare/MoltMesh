# ADR-0008: Hierarchical Actor Model for Agent/Task Isolation

**Status**: Accepted
**Date**: 2026-05-30

## Context

The daemon manages concurrent tasks, threads, and streams. Failure in one task must not affect others. Two models debated:

- **Flat**: all tasks report to a single supervisor
- **Hierarchical**: tasks are child actors of a TaskSupervisor, which is a child of AgentActor

The PM argued flat is simpler for adoption. The architect argued hierarchical is necessary for correctness under adversarial conditions (remote agents will send malformed messages).

## Decision

**Hierarchical actor model.** Two levels in v1:

```
AgentActor (root)
├── RegistryActor       — DHT publish/resolve, Agent Card management
├── GossipActor         — GossipSub topic subscriptions/publications
├── InboxActor          — SQLite inbox, message dispatch
├── OutboxActor         — SQLite outbox, retry loop
└── TaskSupervisor
    ├── TaskActor[id-1]
    │   ├── StreamActor     — bidirectional libp2p stream
    │   └── StorageActor    — local thread + artifact writes
    └── TaskActor[id-2]
        └── ...
```

## Restart Strategy

- **TaskSupervisor**: `one-for-one` — one task crashes, only that task restarts.
- **TaskActor crash budget**: if a TaskActor crashes >3 times in 60 seconds, mark the task as `failed` and emit `TaskFailed` event to remote agent via outbox. Do not thrash.
- **AgentActor children**: `one-for-one` — RegistryActor crash does not affect TaskSupervisor.

## Rationale

- Remote agents will send malformed, oversized, or adversarial messages. Tasks must be crash-isolated.
- A TaskActor crash (e.g., stream timeout, bad artifact) must not affect RegistryActor (agent's DHT presence) or other running tasks.
- Hierarchical supervision is the standard approach in mature actor systems (Erlang/OTP, Akka). The complexity is in the framework, not the application code.
- Go goroutines + channels implement actors naturally. No external actor framework needed.

## Consequences

- Each TaskActor is a goroutine tree. Goroutine leak on crash must be prevented — use context cancellation propagated from TaskSupervisor.
- `TaskFailed` is a protocol-level message type — remote agents receive it via outbox delivery when a task crashes beyond its budget.
- Flat actor model may be revisited if goroutine overhead becomes measurable at >10k concurrent tasks (future ADR).
