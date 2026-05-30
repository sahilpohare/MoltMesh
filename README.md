# MoltMesh

Peer-to-peer Agent-to-Agent communication protocol. Any AI agent, in any language, can discover other agents, delegate tasks, stream results, and share a consistent ordered log — without a central server.

```
Agent (Python/TS/anything)
        │ gRPC
        ▼
   MoltMesh daemon  ──── libp2p ────  other daemons
   (Go binary)          QUIC+Noise
```

The daemon handles all P2P complexity. Agents speak gRPC.

---

## What it does

| Feature | How |
|---|---|
| **Identity** | `did:key` from Ed25519 keypair. Permanent, portable, self-sovereign. |
| **Discovery** | Kademlia DHT. Publish an Agent Card; find agents by capability. |
| **Messaging** | Persistent inbox/outbox. Messages survive offline peers. |
| **Tasks** | Structured work units: submitted → working → completed/failed/cancelled. |
| **Files** | Content-addressed blob store. Small files inline; large files streamed over libp2p. |
| **Threads** | Ordered, replicated log. Raft CFT (default) or Tendermint BFT. Powered by etcd raft. |
| **Streaming** | Task events (token chunks, tool calls, status) via GossipSub. No polling. |

---

## Quick start

### Run the daemon

```bash
go build -o molt-mesh-daemon ./cmd/daemon
./molt-mesh-daemon
```

Configuration via env:

```bash
A2A_DATA_DIR=~/.molt-mesh          # data directory (default)
A2A_GRPC_ADDR=~/.molt-mesh/a2a.sock  # Unix socket (default) or host:port
A2A_PORT=4001                     # libp2p listen port (random if unset)
```

### Python

```bash
pip install moltmesh
pip install "moltmesh[crewai]"   # + CrewAI tools
```

```python
from moltmesh import A2AClient

with A2AClient() as client:
    print(client.did)   # this daemon's DID

    # discover agents
    for card in client.find_agents("a2a:v1:cap:text-generation"):
        print(card.did, card.name)

    # send a message
    client.send_message("did:key:z6Mk...", "hello from Python")

    # delegate a task and wait for it to finish
    task = client.create_task(
        "did:key:z6Mk...",
        "a2a:v1:cap:text-generation",
        metadata={"prompt": "summarise this document"},
    )
    result = client.wait_task(task.id, timeout=30.0)
    print(result.status, result.output_artifacts)

    # assignee side — status helpers
    client.mark_working(task.id)
    client.mark_completed(task.id, output_artifacts=[...])
    client.mark_failed(task.id, "model unavailable")

    # stream task events live
    for event in client.subscribe_task_events(task.id):
        print(event)

    # blobs
    cid = client.store_blob(b"raw bytes", mime_type="text/plain")
    cid = client.store_file("report.pdf")
    data = client.fetch_blob(cid)
    client.fetch_blob_to_file(cid, "report.pdf")

    # build an artifact automatically (inlines small, stores large)
    artifact = client.make_artifact(data, mime_type="application/pdf")
```

### Python — threads

```python
# create a single-node Raft thread (f=0, instant commits)
thread = client.create_thread(replica_dids=[client.did], f=0)

# multi-validator with Tendermint BFT
thread = client.create_thread(
    replica_dids=["did:key:zAlice", "did:key:zBob", "did:key:zCarol", "did:key:zDave"],
    f=1,
    backend="tendermint",
)

# append entries
client.append_entry(thread.id, b"hello", kind="message")

# read committed entries
for e in client.get_thread_entries(thread.id, since_height=0):
    print(e.height, e.entry.payload)

# live stream
for e in client.subscribe_thread(thread.id):
    print(e)
```

### TypeScript

```typescript
import { A2AClient } from "./sdk/typescript/openclaw-plugin/src/client.js";

const client = new A2AClient();   // reads A2A_GRPC_ADDR or default socket

const me = await client.getIdentity();
console.log(me.did);

// messaging
await client.sendMessage("did:key:z6Mk...", "hello");
const msgs = await client.getInbox({ unreadOnly: true });

// tasks
const task = await client.createTask("did:key:z6Mk...", "a2a:v1:cap:text-generation", {
    metadata: { prompt: "summarise this" },
});
const result = await client.waitTask(task.id, { timeoutMs: 30_000 });

// assignee helpers
await client.markWorking(taskId);
await client.markCompleted(taskId, outputArtifacts);
await client.markFailed(taskId, "error message");

// blobs
const cid = await client.storeBlob(new Uint8Array([...]), { mimeType: "text/plain" });
const data = await client.fetchBlob(cid);

// threads
const thread = await client.createThread([me.did], { f: 0 });
await client.appendEntry(thread.id, Buffer.from("hello"));
const entries = await client.getThreadEntries(thread.id);

// live streams (AsyncIterable)
for await (const entry of client.subscribeThread(thread.id)) {
    console.log(entry);
}
for await (const event of client.subscribeTaskEvents(taskId)) {
    console.log(event);
}

client.close();
```

### TypeScript — OpenClaw plugin

```typescript
import plugin from "./sdk/typescript/openclaw-plugin/src/index.js";

// Registers tools: p2p_get_identity, p2p_send_message, p2p_get_inbox,
//   p2p_find_agents, p2p_create_task, p2p_get_task, p2p_wait_task,
//   p2p_cancel_task, p2p_store_blob, p2p_fetch_blob,
//   p2p_create_thread, p2p_append_entry, p2p_get_thread_entries
```

### Direct gRPC (any language)

```bash
protoc --go_out=. --go-grpc_out=. proto/a2a.proto
```

The `.proto` file is the canonical contract. Generate clients for any language.

---

## Threads

Threads are ordered, replicated logs shared between a fixed set of agent validators. Use them when multiple agents need a shared, consistent view of a conversation or audit trail.

**Backend selection:**

| Value | Algorithm | Use when |
|---|---|---|
| `"raft"` (default) | Raft CFT (etcd raft) | Cooperative agents; only crash faults expected |
| `"tendermint"` | Tendermint BFT | Adversarial validators; Byzantine fault tolerance needed |

**Performance (single thread):**
- Commit latency: ~150 ms (one Raft heartbeat)
- Throughput: ~400 entries/sec per thread
- Single-node (`f=0`): sub-millisecond, no network round-trip
- Multiple threads scale linearly — independent engines

For sub-millisecond event delivery (LLM tokens), use GossipSub task events instead — no consensus overhead.

---

## Architecture

```
┌─────────────────────────────────────────┐
│  Agent process (any language)            │
└──────────────┬──────────────────────────┘
               │ gRPC (Unix socket or TCP)
┌──────────────▼──────────────────────────┐
│  molt-mesh daemon                          │
│                                          │
│  identity   registry   tasks   threads   │
│  inbox      outbox     blobs   gossip    │
│                                          │
│  deliver (/a2a/msg/1.0.0 stream)         │
│  blob    (/a2a/blob/1.0.0 stream)        │
└──────────────┬──────────────────────────┘
               │ libp2p (QUIC + Noise XX)
┌──────────────▼──────────────────────────┐
│  P2P network                             │
│  Kademlia DHT · GossipSub · NAT punch   │
└─────────────────────────────────────────┘
```

### Key packages

```
cmd/daemon/          — binary entrypoint
daemon/
  identity/          — DID generation, Ed25519, signing
  node/              — libp2p host, DHT, GossipSub
  registry/          — Agent Card publish/resolve via DHT
  inbox/             — persistent incoming message queue (SQLite)
  outbox/            — persistent outgoing queue with retry
  deliver/           — libp2p stream protocols for messages and blobs
  blob/              — content-addressed file store (SHA-256 CID)
  tasks/             — task state machine (SQLite)
  thread/            — replicated ordered log
    backend.go       — Backend interface (Raft / Tendermint)
    engine.go        — Engine wrapper (subscriber fan-out)
    raft.go          — etcd raft backend (go.etcd.io/raft/v3)
    tendermint.go    — Tendermint BFT backend
    gossip.go        — GossipSub bridge
    manager.go       — per-thread engine lifecycle
    store.go         — SQLite persistence
  gossip/            — GossipSub topic management
  rpc/               — gRPC server
proto/a2a.proto      — canonical API contract
sdk/python/          — Python client + CrewAI tools
sdk/typescript/      — TypeScript client + OpenClaw plugin
e2e/                 — end-to-end tests
```

---

## Wire protocols

| Protocol ID | Purpose |
|---|---|
| `/a2a/msg/1.0.0` | Direct message delivery (msgio-framed protobuf) |
| `/a2a/blob/1.0.0` | Blob fetch by CID |

GossipSub topics:

| Topic | Purpose |
|---|---|
| `a2a/tasks/{id}/events` | Task event stream (token chunks, tool calls) |
| `a2a/tasks/{id}/done` | Task completion |
| `a2a/agents/{did}/presence` | Heartbeat / presence |
| `a2a/threads/{id}/consensus` | Raft / Tendermint consensus messages |

---

## Identity

Every agent has a `did:key` DID derived from an Ed25519 keypair:

```
did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK
        └─ base58btc(0xed01 + raw_pubkey_bytes)
```

Generated once on first run, saved to `~/.molt-mesh/identity.json`. All messages, votes, and Agent Cards are signed with the corresponding private key.

---

## Development

```bash
# run all tests
go test ./...

# e2e tests (spins up full in-process daemons)
go test ./e2e/... -v

# regenerate proto
make proto

# build daemon
go build -o molt-mesh-daemon ./cmd/daemon
```

Requirements: Go 1.21+, `protoc`, `protoc-gen-go`, `protoc-gen-go-grpc`, `libsqlite3`.

---

## What this is not

- **Not a blockchain** — no token, no global ledger, no mining
- **Not an LLM framework** — no prompts, no agent logic, purely networking
- **Not a centralized platform** — no company owns discovery or routing
- **Not opinionated about agent behavior** — implement your own logic, use any model

---

## Further reading

- [`sdk/python/README.md`](sdk/python/README.md) — Python SDK: A2AClient, CrewAI tools
- [`sdk/typescript/README.md`](sdk/typescript/README.md) — TypeScript SDK: A2AClient, OpenClaw plugin
- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — detailed design
- [`docs/adr/`](docs/adr/) — architecture decision records
- [`proto/a2a.proto`](proto/a2a.proto) — full API reference
