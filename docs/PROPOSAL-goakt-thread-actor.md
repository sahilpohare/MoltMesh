# Proposal: ThreadActor on GoAkt (actor-model branch)

> **Historical:** implemented and superseded by ADR-0015's durable,
> daemon-wide actor decision. Scope statements below describe the original
> thread-only proposal, not the current production architecture.
> In particular, ADR-0015 now defines lazy activation, passivation, exact Raft
> Ready persistence, snapshots, and durable remote wake-up.

Status: implemented (sequencing steps 1-4 complete; see ADR-0015). Branch: `actor-model`.

## Context

`daemon/thread`'s Raft backend (`raft.go`) already drives `etcd/raft`
correctly — a single goroutine `select`-multiplexing a tick channel,
`node.Ready()`, and inbound consensus messages. Blocks are hash-linked
(`parent_hash`), content-hashed (`block_hash = sha256(canonical(block))`),
and signed (`proposer_sig`) — the cryptographic/consensus core of a real
small-validator-set blockchain already exists and works.

What's missing, independent of any actor-model work:
- No independent verification path (nothing recomputes hashes/checks sigs
  on read, only at commit time).
- Only replicas in `replica_dids` ever see the chain — it doesn't survive
  or stay discoverable after the parties involved go offline ("stays on
  the network").
- `AppendEntry`'s returned `EntryId` is fabricated, not tied to a real
  committed height.

These are explicitly **out of scope for this proposal** — they're a
separate, GoAkt-independent piece of work (content-addressing blocks to
Bitswap, a DHT head-pointer, a standalone `VerifyChain` function).

## What GoAkt adds here specifically

Not: clustering (rejected — GoAkt's discovery providers assume a trusted,
owned/operated node set with no authn/authz; this project's actual trust
boundary is untrusted external peer daemons, already handled by
libp2p/DHT/GossipSub, a different layer entirely).

Is:
1. **Crash isolation with restart, not crash-to-dead.** A panic in
   `handleReady`/`commitBlock`/`applyCommittedBlock` (e.g. malformed input
   from a Byzantine proposer that passed signature checks but hits some
   other edge case) currently kills the thread's goroutine outright.
   GoAkt's supervisor gives `recover()` + restart + a crash-budget
   ("suspend after N crashes in a window") for free.
2. **Architectural uniformity.** Today: `raft.go` uses a raw `select`,
   `gossip.go` uses pubsub callbacks, `inbox.go` uses a manually
   mutex-guarded subscriber-channel slice, `outbox.go` uses a
   ticker+flushCh. Four different concurrency idioms for the same
   underlying shape (receive a message, mutate own state, maybe emit
   something). GoAkt collapses these to one (`Tell`/`Ask`/`Receive`).
3. **Scheduler** replaces four hand-rolled `time.NewTicker` loops
   (`outbox.go:94`, `registry.go:144`, `names.go:176`, `raft.go:192`) with
   `ScheduleOnce`/`Schedule`/`ScheduleWithCron`, and can add a
   currently-nonexistent task/thread timeout.
4. **Dead-letter stream** (built into GoAkt core) is most of "reliable
   send" for a future mailbox-native Outbox redesign — a message that
   fails delivery auto-routes to a subscribable dead-letter stream,
   instead of hand-rolled retry/backoff bookkeeping in `outbox.go`.

## Why ThreadActor first (not TaskActor)

The Thread — a Raft/BFT-verified, hash-chained, signed log shared by a
fixed replica set — is the actual core of the "delegate work, observed by
concerned actors" model this project is aiming at (see conversation:
publish capability → discover candidates → local scheduling/selection by
the requester, no network-wide broker → propose → on accept, spin up a
thread → work happens as thread entries → anyone invited can observe).
Task delegation is a Thread use case, not a parallel subsystem. TaskActor,
PeerActor (untrusted-input isolation at `deliver.go`), and the
mailbox-native Inbox/Outbox redesign are real follow-on work, sequenced
after this because ThreadActor is the highest-value, highest-risk piece
and should be proven out first.

## Design

### Root wiring (new)
- `daemon/actors/system.go` — one `ActorSystem` per daemon process.
  `NewActorSystem("moltmesh", opts...)`, `Start(ctx)` in boot, `Stop(ctx)`
  on shutdown. **No `Discovery`/clustering config** — single-node per
  process; cross-daemon communication stays on the existing
  libp2p/DHT/GossipSub transport, unchanged.
- `daemon/actors/thread_supervisor.go` — spawns/owns `ThreadActor`
  children. `WithSupervisor(supervisor.NewSupervisor(
  WithStrategy(OneForOneStrategy), WithRetry(3, 60*time.Second),
  WithExponentialBackoff(...)))` — matches ADR-0008's crash-budget numbers
  (3 crashes / 60s). Note: GoAkt's actual behavior on budget-exceeded is
  **suspend** (queryable via `pid.IsSuspended()`, revivable via
  `Reinstate()`), not permanent kill — a deliberate divergence from
  ADR-0008's "mark failed, do not thrash" wording, worth documenting in
  ADR-0015 as an improvement (recoverable via explicit action rather than
  a dead end) rather than silently reconciling the two.

### ThreadActor (new)
`daemon/actors/thread_actor.go`:
- **State**: wraps the existing `RaftBackend` (`daemon/thread/raft.go`) —
  reuse its fields/logic, do not reimplement the `etcd/raft` driving.
- **`PreStart(ctx)`**: load thread + committed height from
  `daemon/thread/store.go` (existing, unchanged), construct/restart the
  `raft.Node` (existing `newRaftBackend` logic, ported not rewritten).
- **`Receive(ctx)`**: replaces `raft.go:191-218`'s blocking `select` with
  message-typed dispatch:
  - `TickMsg` → `node.Tick()`, then a **non-blocking** check on Ready
    (`select { case rd := <-node.Ready(): ...; default: }` — a
    well-known etcd/raft idiom, not a hack; required because GoAkt's
    `Receive` must never block, a hard framework rule), then
    `proposePending`.
  - `*pb.ConsensusMsg` (inbound, from `deliver`/`gossip`) → `node.Step(...)`.
  - `QueryThreadMsg` → `ctx.Response(...)` for external reads
    (`GetThread`/`GetThreadEntries` callers).
- **`WithLongLived()`** — never passivate; it's a live consensus
  participant, not an on-demand resource (unlike a future `TaskActor`,
  which fits GoAkt's Grain model — activate on demand, auto-passivate
  when idle — much better than a plain long-lived Actor).
- **`PostStop`**: `node.Stop()`, flush any pending persistence.
- On commit (inside `handleReady`, reused as-is): keep existing
  `SaveBlock`/`commitBlock` calls unchanged. **No durability/DHT-publish
  work in this slice** — that's the separately-scoped "stays on the
  network" piece described in Context above.

### Scheduler migration
Replace `raft.go`'s `time.NewTicker(raftTickMs)` with
`system.Schedule(TickMsg{}, threadActorPID, tickInterval)`, cancelled via
the returned reference in `PostStop`.

### Wiring into existing code
- `daemon/thread/manager.go` (existing `Manager`) gets a thin adapter:
  instead of directly running `RaftBackend.Run(ctx, broadcast)` in a
  goroutine, it spawns a `ThreadActor` via `ThreadSupervisor` and routes
  `Deliver(msg)` calls as `Tell`/`Ask` into the actor.
- `daemon/rpc/server.go`'s `AppendEntry`/`GetThread`/`GetThreadEntries`
  unchanged in signature — internally now go through the actor via `Ask`
  instead of calling `Manager` methods that touch shared state directly.

### Tests
- Port `thread_test.go`'s existing Raft-path coverage to exercise the
  actor (spawn, `Ask` a query, `Tell` a tick, assert committed height
  advances).
- New: crash-injection test — force a panic inside `Receive` (e.g.
  malformed `ConsensusMsg`), assert the actor restarts per the supervisor
  policy and thread state is intact afterward. This is the concrete,
  testable version of the adversarial-input isolation claim.

### Docs
ADR-0015 (supersedes ADR-0008's "no framework needed" position with the
actual decision): GoAkt adopted as an informed engineering choice, not
hand-rolled — rationale: avoids re-deriving a hard concurrency primitive
(the project's own hand-rolled Raft glue took 4 stabilization commits;
supervision has similar sharp edges), keeps effort on the actual A2A
protocol contribution rather than the actor framework itself. Clustering
considered and explicitly rejected (wrong trust model for untrusted
external peers). Written for a dissertation context — the comparative
framing (conventional bolted-on subsystems vs. actor-native, evaluated
head-to-head) is itself the argument, not just an implementation note.

## Sequencing

1. Root `ActorSystem` + supervisor scaffolding. Build + test green, zero
   behavior change.
2. `ThreadActor` implementing the Raft path only, wired in behind the
   existing `Manager` interface so `rpc.Server` doesn't change.
3. Crash-injection test proving isolation.
4. ADR-0015.

Explicitly deferred to later slices: Tendermint backend actor variant
(candidate for `Become`/`UnBecome`-based phase modeling — propose →
prevote → precommit → commit — replacing `ConsensusState.Step`'s
stringly-typed field in `store.go`), TaskActor (as a Grain), PeerActor
(untrusted-input isolation at `deliver.go`'s stream handler and
`gossip.go`'s topic handlers), mailbox-native Inbox/Outbox redesign
(inbox = durable mailbox replay, outbox = scheduler-driven retry +
GoAkt's built-in dead-letter stream), thread durability/DHT-publish/
`VerifyChain` (GoAkt-independent, could be done in parallel by a
different work stream).

## Local reputation (design note, not yet scoped into a slice)

Capability-based routing literature (see: arXiv 2503.07686, "Adaptive
routing protocols for... AI multi-agent systems") proposes a multi-factor
scoring function (task complexity vs. capability, priority vs.
availability, latency, load, reliability) for ranking candidate agents,
but assumes centralized computation and has no empirical evaluation. This
project's model — no network-wide broker, each requester locally ranks
its own `FindAgents` candidates — is a legitimate, decentralized
instantiation of that idea, with one addition: today's `AgentCard` has no
reputation signal (any peer can self-declare any capability with zero
track record). A local reputation score derived from each agent's own
observed Thread history (completion rate, timeout rate for past
delegations to a given DID) is a fully decentralized signal requiring no
new subsystem — it's a query over data ThreadActor already produces.
Worth a dissertation section on its own; not required for the
ThreadActor slice itself.
