# ADR-0010: Switchable Thread Consensus Backends (Raft + Tendermint)

**Status:** Accepted
**Date:** 2024-01

## Context

Threads are ordered, replicated logs shared between a fixed set of agent validators. The core design question is: what consensus algorithm should they use?

Two relevant algorithms exist:

**Raft (CFT — Crash Fault Tolerant)**
- Handles crash failures only. If validators are adversarial, Raft provides no safety guarantee.
- Simple leader-based model: one leader, all others followers.
- Quorum: N/2 + 1 (majority).
- Low latency: one round-trip per block (heartbeat drives batching).
- Single-node fast path: f=0, N=1 — leader immediately, no network, ~0ms overhead.

**Tendermint (BFT — Byzantine Fault Tolerant)**
- Handles up to f Byzantine (adversarial) validators in a set of N = 3f+1.
- Two-phase commit: prevote → precommit. Each phase requires 2f+1 votes.
- PoL (Proof of Lock) prevents safety violations across view changes.
- Higher latency: two round-trips (two phases of broadcast + collection).
- Correct under network partitions (safety always holds; liveness requires 2f+1 online).

The agents using threads will have different trust models:

- **Cooperative agents** (same operator, same organization, same team) — crash faults are the realistic concern. Raft is appropriate.
- **Multi-party agreements** (agents from different organizations, automated escrow, audit trails) — Byzantine faults are realistic. Tendermint is appropriate.

A single hard-coded algorithm would either over-engineer cooperative cases (Tendermint's 2x latency penalty for simple use cases) or under-engineer adversarial cases (Raft's safety collapse under a lying validator).

## Decision

Implement **both Raft and Tendermint** as pluggable backends behind a common `Backend` interface. Selection is per-thread via `thread.Metadata["backend"]`.

```go
type Backend interface {
    Run(ctx context.Context, broadcast func(*pb.ConsensusMsg))
    Deliver(msg *pb.ConsensusMsg)
    Subscribe() <-chan *pb.ThreadEntryWithPos
    Unsubscribe(ch <-chan *pb.ThreadEntryWithPos)
}
```

An `Engine` wrapper owns subscriber fan-out and the commit callback, decoupled from backend selection:

```go
func NewEngine(thread, id, store, log, kind BackendKind, onCommit) (*Engine, error)
```

A `GossipBridge` connects any engine to GossipSub topic `a2a/threads/{id}/consensus`, routing `ConsensusMsg` protobufs in both directions.

Default backend: `"raft"`.

## Consequences

**Good:**
- Cooperative use cases get Raft's low latency (~150ms commit).
- Multi-party use cases get Tendermint's BFT safety when needed.
- Single-node threads (f=0) are trivially fast — one validator, no network messages.
- New backends (e.g. HotStuff, PBFT) can be added without touching gRPC or GossipSub wiring.
- Proto `ConsensusMsg` uses `oneof payload` — Raft and Tendermint messages share the same wire envelope.

**Trade-offs:**
- Operators must choose the right backend. Wrong choice (Raft in adversarial setting) is unsafe.
- Two implementations to maintain.
- `ConsensusMsg` proto grows as new backends add message types.

## Alternatives Considered

**Single algorithm (Raft only)**
Simpler implementation. Rejected: provides no safety for adversarial multi-party threads.

**Single algorithm (Tendermint only)**
Safe in all cases. Rejected: 2x latency penalty for the common cooperative case; 3f+1 replica requirement is unnecessarily expensive for trusted agents.

**IPLD chains (no consensus)**
Hash-linked blocks give verifiability but not agreement — two validators can produce conflicting forks. Rejected as the primary mechanism: no liveness or safety guarantee. The current block `parent_hash` field is IPLD-compatible and could be used for content retrieval in a future version.

**etcd/raft library**
Would provide a battle-tested Raft. Rejected to keep the dependency tree minimal and to maintain full control over the consensus loop, which needs to integrate tightly with GossipSub's broadcast semantics.

## Implementation Notes

- Raft state (term, votedFor) is persisted via `store.SaveConsensusState` / `store.LoadConsensusState`.
- Tendermint state (height, round, step, proposal, locks) is persisted similarly.
- `BroadcastFunc` delivers to self first, then publishes to GossipSub asynchronously (goroutine) to prevent deadlock when `topic.Publish` blocks while the engine holds its mutex.
- Engine goroutines use the **daemon lifecycle context** (`Manager.ctx`), not the gRPC request context — which is cancelled as soon as the RPC returns.

## Performance Numbers

Single-thread, single machine, Raft backend:

| Metric | Value |
|---|---|
| Commit latency | ~150 ms (one heartbeat interval) |
| Block capacity | 64 entries/block |
| Throughput | ~400 entries/sec per thread |
| Single-node latency | <1 ms (no network, leader immediate) |

Multiple threads scale linearly. For sub-millisecond event delivery, use GossipSub task events directly.
