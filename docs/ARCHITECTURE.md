# OpenMolt Network — Architecture

## Vision

A fully peer-to-peer Agent-to-Agent communication network. Any AI agent, built in any language on any framework, can discover other agents, delegate tasks, stream results, and share a consistent ordered log — without any central server, platform owner, or registry.

## Thread recovery security

Thread recovery is capability-based. A bare thread ID locates public metadata
only and must not yield plaintext history. Readable recovery requires a saved
recovery capability and at least one surviving encrypted local or archive copy.
If every persisted copy is destroyed, recovery is impossible.

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
| **ActivityPub / XMPP** | Inbox/outbox model, federation without central servers |

## System Layers

```
┌─────────────────────────────────────────────────────────────┐
│  Agent (any language / any framework)                        │
│  LangChain · CrewAI · AutoGen · custom · anything           │
└──────────────────────┬──────────────────────────────────────┘
                       │ gRPC (Unix socket or TCP)
                       │ service A2ANode (single service, all RPCs)
┌──────────────────────▼──────────────────────────────────────┐
│  moltmesh-daemon (Go binary)                                 │
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────┐  │
│  │  Identity   │  │   Registry   │  │   Task Engine      │  │
│  │  DID:key    │  │  Agent Card  │  │  Lifecycle FSM     │  │
│  │  Ed25519    │  │  DHT + sign  │  │  Inbox / Outbox    │  │
│  └─────────────┘  └──────────────┘  └────────────────────┘  │
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────┐  │
│  │  Transport  │  │  GossipSub   │  │   Thread Engine    │  │
│  │  libp2p     │  │  Task events │  │  Raft / Tendermint │  │
│  │  QUIC+Noise │  │  Pub/Sub     │  │  SQLite + blobs    │  │
│  └─────────────┘  └──────────────┘  └────────────────────┘  │
│                                                              │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────┐  │
│  │  Blockstore │  │  Deliver     │  │  Networks          │  │
│  │  CIDv1 blks │  │  /a2a/msg    │  │  Named groups      │  │
│  │  via Bitswap│  │  + Bitswap   │  │  SQLite + gossip   │  │
│  └─────────────┘  └──────────────┘  └────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────┐                           │
│  │  Webhook Dispatcher          │                           │
│  │  HTTP POST · retry · secret  │                           │
│  └──────────────────────────────┘                           │
└─────────────────────────────────────────────────────────────┘
                       │ libp2p
┌──────────────────────▼──────────────────────────────────────┐
│  P2P Network                                                 │
│  Kademlia DHT · GossipSub · QUIC · Noise XX · NAT traversal │
└─────────────────────────────────────────────────────────────┘
                       │ outbound HTTP (optional)
┌──────────────────────▼──────────────────────────────────────┐
│  Your webhook receiver                                       │
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
- **Signed** — `Publish` computes a canonical JSON representation, signs with the agent's Ed25519 private key, and embeds the signature. `Resolve` verifies the signature against the public key encoded in the DID — spoofing is cryptographically impossible.

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
- **Outbox**: persistent SQLite queue. Ordinary messages have bounded retry and
  a 72-hour TTL. Idempotent thread wake/invite messages retry until delivery and
  are recovered from legacy databases without an expiry ceiling.
- **Delivery**: outbox worker → DHT lookup → libp2p stream (`/a2a/msg/1.0.0`) → remote inbox.
- **Offline tolerance**: ordinary messages are held until their TTL; durable
  thread wake messages remain queued while an agent is paused for any duration.
- **Live push**: `SubscribeInbox` now streams in real-time. When a message arrives it is first committed to SQLite, then fan-out notifies all active `SubscribeInbox` streams without polling.
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
- Large files: stored as CIDv1 blocks in the IPFS flatfs blockstore under `<data-dir>/blocks/`. Fetched on demand over Bitswap; any peer holding the block can serve it.
- CID-addressing makes blobs immutable and deduplicated.

### Delivery Protocols

| Protocol ID | Transport | Purpose |
|---|---|---|
| `/a2a/msg/1.0.0` | libp2p stream | Direct message delivery (msgio-framed protobuf) |
| Bitswap | libp2p (boxo) | Block fetch by CIDv1; the DHT locates providers |

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

### Pub/Sub

Raw topic-based messaging via GossipSub, exposed over gRPC.

- `Publish(topic, payload)` — broadcast bytes to any GossipSub topic.
- `SubscribeTopic(topic)` — server-streaming RPC; yields messages as they arrive.
- Topics are arbitrary strings; built-in system topics are prefixed `a2a/`.
- Backed by the same GossipSub mesh as task events and thread consensus.

### Webhooks

HTTP push delivery for external processes that cannot maintain a gRPC stream.

- Configure with `set-webhook <url> [--secret <s>]`.
- Events POSTed as JSON `{"kind":"…","timestamp":…,"data":{…}}`.
- Headers: `X-MoltMesh-Event` (kind), `X-MoltMesh-Secret` (shared secret when set).
- Delivery: best-effort, up to 3 retries with exponential back-off (500 ms base).
- Fired on: incoming messages (`message`), task status changes (`task_event`), network broadcasts (`pubsub`).

### Networks (Groups)

Named, persistent groups of agents with multicast broadcast.

- **Membership** stored in SQLite (`networks.db`). Survives daemon restarts.
- **Broadcast** is backed by GossipSub topic `a2a/networks/{id}/broadcast`.
- Creating a network automatically makes the creator a member.
- `JoinNetwork` / `LeaveNetwork` are idempotent.
- `SubscribeNetwork` streams broadcasts from the network's GossipSub topic.

### GossipSub Topics

| Topic | Purpose |
|---|---|
| `a2a/tasks/{id}/events` | Task event stream (token chunks, tool calls, status updates) |
| `a2a/tasks/{id}/done` | Task completion notification |
| `a2a/agents/{did}/presence` | Heartbeat / presence |
| `a2a/threads/{id}/consensus` | Raft / Tendermint consensus messages |
| `a2a/networks/{id}/broadcast` | Network group broadcast |
| `<user-defined>` | Application pub/sub (arbitrary topics) |

### gRPC Interface

The daemon exposes a single `service A2ANode` defined in `proto/a2a.proto`. All RPCs share the same protobuf wire encoding. Generated clients are available for Go and Python; the TypeScript SDK uses `@grpc/proto-loader` at runtime.

**Identity & Registry:**

| RPC | Purpose |
|---|---|
| `GetIdentity` | Return local DID and multiaddrs |
| `PublishAgentCard` | Publish agent capabilities to DHT (signed) |
| `GetAgentCard` | Resolve and verify an agent card by DID |
| `FindAgents` | Search DHT for agents by capability |
| `ClaimName` | Claim a human-readable name (DHT, 24h TTL) |
| `ResolveName` | Resolve a name to a DID |

**Messaging:**

| RPC | Purpose |
|---|---|
| `SendMessage` | Enqueue message to outbox |
| `GetInbox` | Read queued incoming messages |
| `SubscribeInbox` | Stream incoming messages live (real-time push) |
| `AckMessage` | Mark message as read |
| `GetOutbox` | Read outgoing queue |

**Tasks:**

| RPC | Purpose |
|---|---|
| `CreateTask` | Create and track a task |
| `UpdateTask` | Update task status or add artifact |
| `GetTask` | Fetch task state |
| `CancelTask` | Cancel a task |
| `PublishTaskEvent` | Emit a task event via GossipSub |
| `SubscribeTaskEvents` | Stream task events via GossipSub |

**Blobs & Threads:**

| RPC | Purpose |
|---|---|
| `SendFile` | Store a file; get back its SHA-256 CID |
| `FetchFile` | Retrieve a file by CID (local or remote, server-streaming) |
| `CreateThread` | Create a replicated ordered log |
| `GetThread` | Fetch thread metadata |
| `AppendEntry` | Enqueue entry for next block |
| `GetThreadEntries` | Read committed entries since height |
| `SubscribeThread` | Stream live committed entries |

**Diagnostics:**

| RPC | Purpose |
|---|---|
| `Ping` | Measure round-trip latency to a DID |
| `Health` | Return version, uptime, DID, peer count |
| `ListPeers` | Enumerate connected libp2p peers |

**Pub/Sub, Webhooks & Networks:**

| RPC | Purpose |
|---|---|
| `Publish` | Publish bytes to a GossipSub topic |
| `SubscribeTopic` | Stream messages from a GossipSub topic |
| `SetWebhook` | Configure webhook URL and secret |
| `ClearWebhook` | Disable webhook delivery |
| `GetWebhook` | Return current webhook URL |
| `CreateNetwork` | Create a named agent group |
| `JoinNetwork` | Join an existing network |
| `LeaveNetwork` | Leave a network |
| `ListNetworks` | List networks the local agent belongs to |
| `NetworkMembers` | List members of a network |
| `BroadcastNetwork` | Multicast to a network via GossipSub |
| `SubscribeNetwork` | Stream broadcasts from a network |

## File Structure

```
p2p_a2a/
├── cmd/
│   └── moltmesh/           # the one binary: daemon, CLI, and TUI
│       ├── main.go, daemon.go          # entrypoint, daemon lifecycle, config
│       ├── client.go                   # shared flags + connect() every command uses
│       ├── registry.go, messaging.go, tasks.go, files.go, threads.go,
│       │   diag.go, pubsub.go, network.go, names.go, init.go   # CLI, one file per domain
│       ├── commands.go                 # --json envelope helpers shared by all of them
│       └── tui.go, completion.go       # interactive TUI, shell completion
├── daemon/
│   ├── actors/             # GoAkt root supervisor and per-domain actor helpers
│   ├── identity/           # did:key generation, Ed25519 signing and verification
│   ├── node/               # libp2p host, Kademlia DHT, GossipSub, Bitswap blockstore
│   ├── registry/           # Agent Card publish/resolve/verify over the DHT
│   ├── names/              # human-readable name claims and resolution
│   ├── session/            # short-lived authenticated SDK sessions
│   ├── inbox/              # persistent inbox (SQLite) + live subscriber fan-out
│   ├── outbox/             # persistent outbox + retry worker (the outbox pattern)
│   ├── deliver/            # libp2p stream protocol /a2a/msg, sender DID verification
│   ├── tasks/              # task state machine (SQLite)
│   ├── thread/             # replicated hash-chained log
│   │   ├── backend.go      # ActorBackend + optional VoterChanger/Snapshotter
│   │   ├── raft.go         # Raft CFT backend
│   │   ├── tendermint.go   # Tendermint BFT backend
│   │   ├── actor.go, actor_manager.go, actor_supervisor.go,
│   │   │   actor_gossip_bridge.go       # ThreadActor and its supervision
│   │   ├── store.go        # Store, schema migration, shared codec
│   │   ├── store_{threads,membership,blocks,pending,keys,consensus,archive}.go
│   │   ├── archive.go, archive_worker.go, publisher.go   # durability: Bitswap + DHT
│   │   ├── verify.go, descriptor.go, gossip_validation.go
│   │   └── manager.go, engine.go, gossip.go   # legacy goroutine path (tests only)
│   ├── gossip/             # GossipSub topic management
│   ├── network/            # named agent groups, membership, broadcast
│   ├── webhook/            # HTTP event delivery with SSRF protection
│   └── rpc/                # gRPC server, one file per domain
│       ├── server.go       # Server struct, New, auth helpers
│       ├── server_{session,registry,tasks,messaging,files,threads}.go
│       └── ext.go, diag.go, version.go
├── gen/
│   └── a2a/v1/             # generated Go stubs
├── pkg/
│   ├── a2avalidator/       # DHT /a2a/ namespace record validator
│   ├── capability/         # capability ID namespace (a2a:v1:cap:<name>)
│   ├── config/             # moltbook.toml
│   ├── did/                # DID parsing and formatting
│   ├── format/             # human-readable output
│   ├── p2putil/, sqlite/, threadcrypto/, assert/
├── proto/
│   └── a2a.proto           # canonical API contract (single service, ADR-0012)
├── e2e/
│   ├── e2e_test.go         # in-process end-to-end tests
│   └── manual-thread/      # multi-daemon demo: discovery, delegation, recovery
├── docs/
│   ├── ARCHITECTURE.md     # this file
│   └── adr/                # Architecture Decision Records
└── sdk/
    ├── python/             # Python client + CrewAI tools
    └── typescript/         # OpenClaw plugin (also the Claude Code MCP bridge), AI SDK tools
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
  → remote daemon: deliver handler → inbox.Put(msg) → notify(msg)
  → fan-out: all active SubscribeInbox streams receive msg immediately
  → remote agent: GetInbox() or SubscribeInbox() (live push, no polling)
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
Assignee publishes: gossip.PublishTaskEvent("a2a/tasks/{id}/events", event_pb)
  → GossipSub mesh
  → Initiator: SubscribeTaskEvents stream → Agent receives event
```

### Network Broadcast

```
Agent.BroadcastNetwork(network_id, payload)
  → gRPC → rpc.Server.BroadcastNetwork
  → network.Store.IsMember check (SQLite)
  → network.Manager.Broadcast(network_id, payload)
  → gossip.Publish("a2a/networks/{id}/broadcast", payload)
  → GossipSub mesh → all SubscribeNetwork streams receive payload
  → webhook.Send("pubsub", {network_id, payload})  [async, if configured]
```

### Webhook Event Delivery

```
Event occurs (message received / task updated / network broadcast)
  → rpc.Server fires webhook.Dispatcher.Send(kind, data)
  → goroutine: json.Marshal → HTTP POST to configured URL
  → Headers: X-MoltMesh-Event, X-MoltMesh-Secret
  → On non-2xx: retry up to 3× with exponential back-off (500ms base)
  → On exhaustion: log error and drop (best-effort, not durable)
```

## What This Is Not

- **Not a blockchain** — no global consensus, no token, no mining. Threads are private logs between their validators.
- **Not an LLM framework** — no prompt management, no agent logic. Purely networking.
- **Not a centralized platform** — no company owns the registry or routing.
- **Not opinionated about agent behavior** — agents implement their own logic. Protocol defines communication only.
- **Not IPFS** — blobs are local and fetched on demand over direct streams. No content routing network.
