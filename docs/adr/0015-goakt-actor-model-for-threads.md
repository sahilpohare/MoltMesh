# ADR-0015: Durable, Virtualized GoAkt Actors

**Status**: Accepted and implemented
**Date**: 2026-08-15
**Amended**: 2026-08-16

## Context

ADR-0008 selected a hierarchical actor model. The initial GoAkt implementation
treated every thread as a permanently resident actor with a 100 ms Raft tick.
That cannot support nodes holding hundreds of thousands of threads: startup,
PID count, GossipSub subscriptions and scheduled ticks would all scale with
historical thread count instead of current work.

Threads and entire agents may be offline for long periods. Thread identity and
consensus history must therefore be durable independently of actor lifetime,
and an actor must be reconstructible rather than treated as the source of
truth.

## Decision

Use one standalone GoAkt actor system per daemon, without GoAkt remoting or
clustering. libp2p remains the authenticated cross-agent transport.

Stateful daemon services live below one `DaemonSupervisor`. High-cardinality
entities are lazy children of domain supervisors; threads additionally
passivate because their persisted population is expected to be much larger
than the concurrently active set.

```text
DaemonSupervisor
├── RegistryActor / NameRegistryActor / GossipActor
├── InboxActor / OutboxActor / WebhookActor / NetworkStoreActor
├── DeliverySupervisor
│   └── PeerActor[peer]...
├── TaskSupervisor
│   └── TaskActor[task]...
├── ThreadSupervisor
│   ├── ThreadActor[active thread]...
│   └── ThreadGossipActor[active thread]...
└── ThreadDurabilityActor
```

### Thread activation

Daemon startup is O(1) in persisted thread count. `StartAll` does not enumerate
or spawn stored threads. A thread actor is activated when a thread is created or
invited, a local entry is appended, or a caller subscribes to the live thread.

Appending is write-ahead: the entry is committed to `pending_entries` before
activation. If activation fails, the work remains durable for the next resume.
Raft proposals claim inputs without deleting them; saving the committed block
and acknowledging its claimed input IDs is one SQLite transaction. Stale claims
are released during reconstruction, so pausing or crashing between proposal and
commit cannot lose an acknowledged append.

### Passivation

An active thread with no application or consensus activity for the configured
`daemon.thread_passivation_seconds` interval (300 seconds by default)
gracefully stops. Raft ticks do not count as useful activity. `PostStop` cancels
the tick, stops Raft, writes a `thread_checkpoints` record, closes subscriptions,
and cancels the per-thread GossipSub bridge and publisher actor. The supervisor
removes stopped PIDs from concurrent registries. Later access creates a fresh
PID for the same logical thread.

### Durable resume and snapshots

SQLite, not the PID, is the source of truth:

- `threads` stores identity, replicas and configuration;
- `thread_blocks` stores the hash-linked committed state;
- `consensus_state` stores persisted Raft hard state;
- `pending_entries` is the durable write-ahead input queue; and
- `thread_checkpoints` stores committed height and passivation time;
- `raft_hard_state` and `raft_entries` store exact etcd/raft Ready state; and
- `raft_snapshots` stores compact restart points.

Every Ready batch persists its HardState and unstable entries before messages
are sent, commits are applied, or `Advance` is called. Passivation creates and
persists an etcd/raft snapshot at the last applied index, then compacts the
in-memory and SQLite Raft logs through that index. `ThreadActor.PreStart`
restores snapshot, HardState and post-snapshot entries in bounded work relative
to uncompacted activity, not the full thread history.
If a Ready write fails, the actor retains that exact batch and retries it before
ticking or accepting another Ready; it never calls `Advance` on unpersisted
state.

`thread_blocks` is deliberately not compacted with the Raft log: it is the
canonical, independently verifiable application history. Legacy databases
without exact Raft tables use block reconstruction once and begin filling the
new exact persistence format on subsequent Ready batches.

### Agent pause and wake-up

Stopping the daemon safely pauses all actors because required state is persisted
before acknowledgement. Restarting does not reactivate every thread.

A dormant replica is not subscribed to its per-thread GossipSub topic.
Therefore each accepted append places an idempotent `THREAD_INVITE` for every
other replica in the persistent outbox. The existing invite carries the full
thread descriptor, is authenticated by the libp2p peer/DID binding, and invokes
`InviteReceived`, which activates a dormant actor before acknowledging delivery.
Reusing this direct message avoids both a new wire kind and one always-live
GossipSub subscription per dormant thread. Outbox retries make wake delivery
survive either agent being offline indefinitely: unlike ordinary messages,
wakes have neither a TTL nor a maximum-attempt terminal state. Migration scans
legacy protobuf payloads, marks existing invites durable, and revives invites
that an older binary had expired or failed.

### Other high-cardinality actors

Task actors are lazy and are stopped when tasks become completed, failed, or
cancelled; SQLite remains authoritative and a later read may create a short
lived actor. Peer actors exist only around active libp2p stream operations.
Reference counting preserves per-peer serialization for overlapping streams
while allowing immediate eviction when the last operation finishes.

### Operations and scale verification

`actors.Metrics.Snapshot()` exposes active thread/task/peer gauges plus thread
activation/passivation, snapshot count and cumulative duration, Raft Ready
persistence failures, and durable wake enqueue counters. It is intentionally
telemetry-backend-neutral so a daemon embedding can export the snapshot through
its chosen metrics system.

The normal suite covers snapshot/resume, legacy schema migration, durable wake
recovery, passivation, and task/peer eviction. The opt-in capacity check inserts
100,000 dormant thread records and verifies startup does not enumerate or spawn
them:

```sh
MOLTMESH_SOAK=1 go test ./daemon/thread -run TestSoakHundredThousandDormantThreads
```

## Supervision and Byzantine input

The daemon default and thread policy are one-for-one restart, at most three
restarts in 60 seconds, with 100 ms to 5 second exponential backoff. Budget
exhaustion suspends only the failing actor.

Supervision is a final containment boundary, not validation. libp2p peer/DID
binding, recipient checks, size limits, thread membership, authorship and
signatures are validated before consensus dispatch.

## Actor boundary

Actors own mutable concurrent state and ordering. Pure protobuf, validation,
identity, configuration and formatting code is not actorized. gRPC handlers,
libp2p stream handlers and blocking subscription iterators are adapters.

Generic service actors use `ReceiveContext.PipeTo` for SQLite, DHT, webhook and
network I/O. Raft Ready persistence is the deliberate exception: it remains
synchronous to preserve etcd/raft's Ready/persist/Advance crash-consistency
contract. Slow GossipSub, Bitswap and DHT publication runs in separate actors.

## Consequences

- Memory, PID and tick cost scale with active threads, not stored threads.
- Thread actors reconstruct through one durable recovery path.
- Generic actor messages contain local closures and cannot cross libp2p.
- Legacy `thread.Manager` remains for compatibility tests; shipped entrypoints
  use `ActorManager`.
- The protobuf has no separate `ThreadWake` kind; the already-idempotent
  `THREAD_INVITE` is the documented wake operation.

## Alternatives considered

**Permanently live thread actors:** rejected because idle resource cost grows
with total historical thread count.

**GoAkt grains or clustering:** rejected because clustering assumes a trusted
administered cluster, while agents are untrusted libp2p peers. Application-level
activation provides durable identity without framework clustering.

**Arbitrary asynchronous Raft persistence:** rejected because `Advance` must
occur only after Ready state is durable. A goroutine without an acknowledgement
state machine would resume quickly but incorrectly after a crash.
