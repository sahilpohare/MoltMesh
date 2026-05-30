# P2P A2A — Architecture

## Vision

A fully peer-to-peer Agent-to-Agent communication network. Any AI agent, built in any language on any framework, can discover other agents, delegate tasks, stream results, and share a consistent ordered log — without any central server, platform owner, or registry.

## Design Principles

1. **Language agnostic** — agents plug in via gRPC. The daemon handles all P2P complexity.
2. **No central authority** — no platform owns the registry, routing, or identity.
3. **Decentralized by default** — DHT for discovery, libp2p for transport, DID for identity.
4. **Protocol over platform** — define wire formats and semantics. Let implementations vary.
5. **Streaming first** — LLM token streaming, task event streaming, and artifact streaming are first-class, not afterthoughts.
6. **Inbox/outbox model** — persistent, retryable message queues. Agents are not required to be online.
7. **Actor model** — each agent is an actor. Each task thread is a child actor. Isolated state, message-driven.

## Prior Art & Influences

| System | What We Take |
|---|---|
| **Google A2A** | Task lifecycle semantics, Agent Card format, Skills/Artifacts model |
| **Fetch.ai ACN** | Peer/agent separation, DHT-based lookup, proof-of-representation |
| **Pilot Protocol** | Daemon + IPC model, binary transport, zero-setup DX, domain groups |
| **Raft (Ongaro)** | Leader election, log replication, single-node fast path |
| **Tendermint** | BFT consensus, prevote/precommit two-phase commit, PoL locking |
| **libp2p** | Transport, DHT, GossipSub, NAT traversal, Noise XX encryption |

## System Layers

```
┌─────────────────────────────────────────────────────────────┐
│  Agent (any language / any framework)                        │
│  LangChain · CrewAI · AutoGen · custom · anything           │
└──────────────────────┬──────────────────────────────────────┘
                       │ gRPC (Unix socket or TCP)
┌──────────────────────▼──────────────────────────────────────┐
│  p2p-a2a daemon (Go binary)                                  │
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────┐  │
│  │  Identity   │  │   Registry   │  │   Task Engine      │  │
│  │  DID:key    │  │  Agent Card  │  │  Lifecycle FSM     │  │
│  │  Ed25519    │  │  DHT publish │  │  Inbox / Outbox    │  │
│  └─────────────┘  └──────────────┘  └────────────────────┘  │
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────┐  │
│  │  Transport  │  │  GossipSub   │  │   Thread Engine    │  │
│  │  libp2p     │  │  Task events │  │  Raft / Tendermint │  │
│  │  QUIC+Noise │  │  Presence    │  │  SQLite + blobs    │  │
│  └─────────────┘  └──────────────┘  └────────────────────┘  │
│                                                              │
│  ┌─────────────┐  ┌──────────────┐                          │
│  │  Blob Store │  │  Deliver     │                          │
│  │  SHA-256    │  │  /a2a/msg    │                          │
│  │  CID-addr   │  │  /a2a/blob   │                          │
│  └─────────────┘  └──────────────┘                          │
└─────────────────────────────────────────────────────────────┘
                       │ libp2p
┌──────────────────────▼──────────────────────────────────────┐
│  P2P Network                                                 │
│  Kademlia DHT · GossipSub · QUIC · Noise XX · NAT traversal │
└─────────────────────────────────────────────────────────────┘
```

## Core Concepts

### Identity

Every agent has a `did:key` DID derived from an Ed25519 keypair:

```
did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK
        └─ base58btc(0xed01 + raw_pubkey_bytes)
```

- Generated once on first daemon start; saved to `~/.p2p-a2a/identity.json`.
- All outgoing messages, votes, and Agent Cards are signed with the private key.
- Peers can verify signatures using the public key embedded in the DID.

### Agent Card

- A protobuf/JSON document: DID, capabilities/skills, libp2p multiaddrs, public key, supported protocols.
- Published to the DHT. Discoverable by any peer without a central registry.
- Mutable — daemon re-publishes periodically (default: every 5 minutes). TTL-based expiry.

### Peers vs Agents

- **Peer** — a libp2p node participating in the DHT. Handles routing, relay, delivery.
- **Agent** — the AI process. Connects to its daemon via gRPC.
- One daemon = one peer = one agent (typically). Separation keeps agent code thin.

### Transport

- **QUIC** as primary (no head-of-line blocking, 0-RTT, multiplexed streams).
- **TCP** as fallback.
- **Noise XX** for peer authentication and channel encryption.
- **NAT traversal**: hole-punching, circuit relay fallback.

### Discovery

- **DHT** (Kademlia): stores `agent_did → peer_multiaddr` mappings.
- **Capability search**: agents publish capability keys; peers query DHT.
- **GossipSub**: push-based updates for agents subscribed to a capability namespace.

### Messaging

- **Inbox**: persistent SQLite queue. Incoming messages land here atomically.
- **Outbox**: persistent SQLite queue. Outgoing messages staged with retry + TTL.
- **Delivery**: outbox worker → DHT lookup → libp2p stream (`/a2a/msg/1.0.0`) → remote inbox.
- **Offline tolerance**: messages held in outbox until remote agent comes online (within TTL).
- Wire format: msgio-framed protobuf over libp2p streams.

### Tasks

Fundamental unit of work. Based on Google A2A semantics.

- Lifecycle: `submitted → working → completed | failed | cancelled`
- Each task has: ID, initiator DID, assignee DID, skill, input artifacts, output artifacts, status.
- Task events streamed via GossipSub topic `a2a/tasks/{id}/events`.
- Task completion broadcast via `a2a/tasks/{id}/done`.
- Artifacts stored in blob store; referenced by SHA-256 CID.

### Blob Store

Content-addressed file store. Every file is identified by its SHA-256 hash (CID).

- Small files (≤ configured threshold): stored inline in the artifact protobuf.
- Large files: stored in `~/.p2p-a2a/blobs/`. Fetched on demand over `/a2a/blob/1.0.0` libp2p streams.
- CID-addressing makes blobs immutable and deduplicated.

### Delivery Protocols

| Protocol ID | Transport | Purpose |
|---|---|---|
| `/a2a/msg/1.0.0` | libp2p stream | Direct message delivery (msgio-framed protobuf) |
| `/a2a/blob/1.0.0` | libp2p stream | Blob fetch by CID |

### Threads

Ordered, replicated logs shared between a fixed set of agent validators.

```
Thread
  ├── validators: [Alice, Bob, Carol, Dave]   (f=1, N=4, quorum=3)
  ├── backend: raft | tendermint
  └── committed blocks
        ├── Block 1 [entry, entry, ...]  parent_hash=""
        ├── Block 2 [entry, ...]         parent_hash=hash(Block1)
        └── Block 3 [entry, ...]         parent_hash=hash(Block2)
```

Each block contains a batch of entries (payload + author DID + kind). Blocks form a hash chain via `parent_hash`.

**Backend selection** — per thread, in `thread.Metadata["backend"]`:

| Value | Algorithm | Fault model | Quorum |
|---|---|---|---|
| `"raft"` (default) | Raft CFT | Crash faults only | N/2 + 1 |
| `"tendermint"` | Tendermint BFT | Byzantine (adversarial) | 2f + 1 |

**Performance** (single thread, single machine):
- Commit latency: ~150 ms (one Raft heartbeat / Tendermint epoch)
- Throughput: ~400 entries/sec (64 entries/block × ~6 blocks/sec)
- Multiple threads scale linearly — each runs an independent engine

For high-frequency streaming (LLM tokens), use GossipSub task events directly — no consensus overhead.

#### Thread Engine Architecture

```
Manager
  └── per thread:
        ├── Engine (public handle, subscriber fan-out, commit callback)
        │     └── Backend (Raft or Tendermint)
        └── GossipBridge
              ├── subscribes to GossipSub topic a2a/threads/{id}/consensus
              ├── delivers received ConsensusMsg → Engine.Deliver()
              └── BroadcastFunc: Deliver locally first, then publish async
```

The `Engine` wrapper decouples consensus backends from subscriber management. Switching from Raft to Tendermint only changes the backend — the gRPC layer and GossipSub wiring are identical.

#### Raft Backend

- Roles: follower / candidate / leader.
- Election: random timeout (300–600 ms), RequestVote broadcast, majority quorum wins.
- Replication: leader drains pending entries, sends AppendEntries every 150 ms.
- Single node (f=0, N=1): leader immediately, no network needed — lowest latency.
- Persistence: term and votedFor stored in SQLite `consensus_state`.

#### Tendermint Backend

- Phases: propose → prevote → precommit → commit.
- Leader (proposer) selected round-robin by validator index.
- Locks: once a node precommits a block hash, it is locked until a PoL (Proof of Lock) arrives.
- Requires 2f+1 validators online for liveness. Safety holds under any number of failures.

### GossipSub Topics

| Topic | Purpose |
|---|---|
| `a2a/tasks/{id}/events` | Task event stream (token chunks, tool calls, status updates) |
| `a2a/tasks/{id}/done` | Task completion notification |
| `a2a/agents/{did}/presence` | Heartbeat / presence |
| `a2a/threads/{id}/consensus` | Raft / Tendermint consensus messages |

### gRPC Interface

The daemon exposes a gRPC server locally. Agents use generated clients in any language. The `.proto` file is the canonical contract.

Key RPCs:

| RPC | Purpose |
|---|---|
| `GetIdentity` | Return local DID and multiaddrs |
| `PublishAgentCard` | Publish agent capabilities to DHT |
| `FindAgents` | Search DHT for agents by capability |
| `SendMessage` | Enqueue message to outbox |
| `GetInbox` | Read queued incoming messages |
| `SubscribeInbox` | Stream incoming messages live |
| `CreateTask` | Create and track a task |
| `UpdateTask` | Update task status or add artifact |
| `GetTask` | Fetch task state |
| `CancelTask` | Cancel a task |
| `SubscribeTaskEvents` | Stream task events via GossipSub |
| `StoreBlob` | Store a file; get back its CID |
| `FetchBlob` | Retrieve a file by CID |
| `CreateThread` | Create a replicated ordered log |
| `GetThread` | Fetch thread metadata |
| `AppendEntry` | Enqueue entry for next block |
| `GetThreadEntries` | Read committed entries since height |
| `SubscribeThread` | Stream live committed entries |

## File Structure

```
p2p_a2a/
├── cmd/
│   └── daemon/             # binary entrypoint
├── daemon/
│   ├── identity/           # DID generation, Ed25519 keypair, signing, VerifyWithPub
│   ├── node/               # libp2p host, DHT, GossipSub setup
│   ├── registry/           # Agent Card publish/resolve via DHT
│   ├── inbox/              # persistent inbox queue (SQLite)
│   ├── outbox/             # persistent outbox queue + retry worker
│   ├── deliver/            # libp2p stream protocols: /a2a/msg, /a2a/blob
│   ├── blob/               # content-addressed file store (SHA-256 CID)
│   ├── tasks/              # task state machine (SQLite)
│   ├── thread/             # replicated ordered log
│   │   ├── backend.go      # Backend interface + BackendKind constants
│   │   ├── engine.go       # Engine wrapper: subscriber fan-out, commit callback
│   │   ├── raft.go         # Raft CFT backend
│   │   ├── tendermint.go   # Tendermint BFT backend
│   │   ├── gossip.go       # GossipSub bridge
│   │   ├── manager.go      # per-thread engine lifecycle
│   │   └── store.go        # SQLite persistence (threads, blocks, votes, entries)
│   ├── gossip/             # GossipSub topic management (task events)
│   └── rpc/                # gRPC server implementation
├── gen/
│   └── a2a/v1/             # generated protobuf Go code
├── proto/
│   └── a2a.proto           # canonical API contract
├── e2e/
│   └── e2e_test.go         # full in-process e2e tests (no external processes)
├── docs/
│   ├── ARCHITECTURE.md     # this file
│   └── adr/                # Architecture Decision Records
└── sdk/
    ├── python/             # Python client + CrewAI tools
    └── typescript/         # OpenClaw TypeScript plugin
```

## Data Flow Examples

### Sending a Message

```
Agent.SendMessage(to_did, payload)
  → gRPC → rpc.Server.SendMessage
  → outbox.Enqueue(to_did, payload)
  → outbox worker goroutine
  → registry.Resolve(to_did) → DHT lookup → multiaddr
  → deliver.DeliverFunc()(multiaddr, msg)
  → libp2p stream /a2a/msg/1.0.0
  → remote daemon: deliver handler → inbox.Store(msg)
  → remote agent: GetInbox() or SubscribeInbox()
```

### Committing a Thread Entry (Raft, single node)

```
Agent.AppendEntry(thread_id, payload)
  → gRPC → rpc.Server.AppendEntry
  → manager.AppendEntry → store.EnqueueEntry(thread_id, entry)
  → RaftBackend.sendHeartbeat (after epoch tick)
  → store.DequeuePendingEntries → include in AppendEntries
  → BroadcastFunc: Deliver locally → Engine.handleCommit(block)
  → store.SaveBlock → fan-out to subscribers
  → SubscribeThread stream → Agent receives committed entry
```

### Task Event Streaming

```
Assignee publishes: gossip.Publish("a2a/tasks/{id}/events", event_pb)
  → GossipSub mesh
  → Initiator: SubscribeTaskEvents stream → Agent receives event
```

## What This Is Not

- **Not a blockchain** — no global consensus, no token, no mining. Threads are private logs between their validators.
- **Not an LLM framework** — no prompt management, no agent logic. Purely networking.
- **Not a centralized platform** — no company owns the registry or routing.
- **Not opinionated about agent behavior** — agents implement their own logic. Protocol defines communication only.
- **Not IPFS** — blobs are local and fetched on demand over direct streams. No content routing network.
