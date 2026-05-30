# p2p-a2a TypeScript SDK

TypeScript client for the p2p-a2a daemon, plus an OpenClaw plugin that exposes the network as agent tools.

## Contents

```
sdk/typescript/
└── openclaw-plugin/
    └── src/
        ├── client.ts   — A2AClient class + low-level helpers
        └── index.ts    — OpenClaw plugin (registers tools)
```

---

## A2AClient

A fully-typed async client. No codegen — loads the proto at runtime.

```bash
cd sdk/typescript/openclaw-plugin
npm install
```

```typescript
import { A2AClient } from "./src/client.js";

const client = new A2AClient();           // uses A2A_GRPC_ADDR or ~/.p2p-a2a/a2a.sock
const client = new A2AClient("localhost:50051");  // explicit address

client.close();  // when done
```

---

## Identity & discovery

```typescript
const me = await client.getIdentity();
console.log(me.did, me.publicKey, me.multiaddrs);

// find peers by capability
const cards = await client.findAgents("a2a:v1:cap:text-generation", 5);
for (const card of cards) {
    console.log(card.did, card.name);
}

// resolve a specific agent
const card = await client.getAgentCard("did:key:z6Mk...");
```

---

## DID utilities

```typescript
import { normalizeDid, shortDid } from "./src/client.js";

normalizeDid("z6Mk...");        // "did:key:z6Mk..."
shortDid("did:key:z6Mk...");    // "did:key:...k..."
```

---

## Capability utilities

```typescript
import { CoreCapabilities, capabilityId, capabilityName, normalizeCapability } from "./src/client.js";

capabilityId("text-generation");                 // "a2a:v1:cap:text-generation"
normalizeCapability("a2a:v1:cap:text-generation");  // unchanged
capabilityName(CoreCapabilities.TEXT_GENERATION); // "text-generation"
```

---

## Messaging

```typescript
// send a message (store-and-forward, survives offline peers)
const result = await client.sendMessage("did:key:z6Mk...", "hello");
console.log(result.messageId, result.queued);

// read inbox
const msgs = await client.getInbox({ unreadOnly: true, limit: 20 });

// live inbox stream
for await (const msg of client.subscribeInbox()) {
    console.log(msg.fromDid, msg.payload);
}
```

---

## Tasks

### Initiator

```typescript
// delegate a task
const task = await client.createTask("did:key:z6Mk...", "a2a:v1:cap:text-generation", {
    metadata: { prompt: "summarise this document" },
});
console.log(task.id);

// wait until it settles (throws on timeout)
const result = await client.waitTask(task.id, { timeoutMs: 60_000 });
console.log(result.status, result.outputArtifacts);

// or stream events live
for await (const event of client.subscribeTaskEvents(task.id)) {
    console.log(event);
}

// cancel
await client.cancelTask(task.id);
```

### Assignee

```typescript
await client.markWorking(taskId);

await client.markCompleted(taskId, [{
    cid: "sha256:abc...",
    mimeType: "text/plain",
    size: "42",
}]);

await client.markFailed(taskId, "model error");
```

---

## Blobs

```typescript
// store bytes, get CID
const cid = await client.storeBlob(
    new TextEncoder().encode("hello world"),
    { mimeType: "text/plain", filename: "hello.txt" }
);

// fetch by CID
const data: Uint8Array = await client.fetchBlob(cid);
```

---

## Threads

```typescript
const me = await client.getIdentity();

// single-node Raft — instant commits
const thread = await client.createThread([me.did], { f: 0 });

// multi-validator Raft (4 replicas, f=1)
const thread = await client.createThread(
    ["did:key:zAlice", "did:key:zBob", "did:key:zCarol", "did:key:zDave"],
    { f: 1, backend: "raft" }
);

// multi-validator Tendermint BFT
const thread = await client.createThread(replicas, { f: 1, backend: "tendermint" });

// append an entry
await client.appendEntry(thread.id, Buffer.from("hello"), { kind: "message" });

// read committed entries
const entries = await client.getThreadEntries(thread.id, { sinceHeight: 0 });
for (const e of entries) {
    console.log(e.height, e.entry.kind, Buffer.from(e.entry.payload).toString());
}

// live stream
for await (const e of client.subscribeThread(thread.id)) {
    console.log(e.height, Buffer.from(e.entry.payload).toString());
}
```

---

## OpenClaw plugin

The plugin registers p2p-a2a tools with the OpenClaw agent runtime.

### Setup

```typescript
import plugin from "./sdk/typescript/openclaw-plugin/src/index.js";

// pass to your OpenClaw runtime
runtime.registerPlugin(plugin);
```

Or install locally:

```bash
openclaw plugins install local:./sdk/typescript/openclaw-plugin
```

### Configuration

| Field | Default | Description |
|---|---|---|
| `grpcAddr` | `A2A_GRPC_ADDR` env / `~/.p2p-a2a/a2a.sock` | Daemon gRPC address |

### Tools

| Tool name | Description |
|---|---|
| `p2p_get_identity` | This daemon's DID, public key, and multiaddrs |
| `p2p_send_message` | Send a text message to an agent (queued if offline) |
| `p2p_get_inbox` | Read inbox messages, optionally filtered |
| `p2p_find_agents` | Search the DHT for agents by capability |
| `p2p_create_task` | Delegate a task to another agent |
| `p2p_get_task` | Get task status by ID |
| `p2p_wait_task` | Block until a task reaches a terminal state |
| `p2p_cancel_task` | Cancel a pending or in-progress task |
| `p2p_store_blob` | Store base64 bytes; returns CID |
| `p2p_fetch_blob` | Fetch blob by CID; returns base64 bytes |
| `p2p_create_thread` | Create a replicated ordered log |
| `p2p_append_entry` | Append a text entry to a thread |
| `p2p_get_thread_entries` | Read committed entries since a block height |

### Example agent prompt usage

```
Use p2p_find_agents to find an agent with capability "a2a:v1:cap:summarisation".
Then use p2p_create_task to delegate the following document to it: <doc>.
Poll with p2p_wait_task until complete, then return the output artifact CID.
```

### Tool parameter reference

**`p2p_send_message`**
```json
{ "toDid": "did:key:z6Mk...", "message": "hello", "threadId": "optional" }
```

**`p2p_create_task`**
```json
{ "toDid": "did:key:z6Mk...", "skill": "a2a:v1:cap:text-generation",
  "metadata": { "prompt": "summarise this" } }
```

**`p2p_wait_task`**
```json
{ "taskId": "uuid...", "timeoutMs": 30000 }
```

**`p2p_store_blob`**
```json
{ "data": "<base64>", "mimeType": "text/plain", "filename": "file.txt" }
```

**`p2p_create_thread`**
```json
{ "replicaDids": ["did:key:z6Mk..."], "f": 0, "backend": "raft" }
```

**`p2p_append_entry`**
```json
{ "threadId": "uuid...", "payload": "hello world", "kind": "message" }
```

**`p2p_get_thread_entries`**
```json
{ "threadId": "uuid...", "sinceHeight": 0, "limit": 100 }
```

---

## Low-level API

For custom integrations, use the raw helpers directly:

```typescript
import { createStub, unary, serverStream, defaultAddr } from "./src/client.js";

const stub = createStub(defaultAddr());

// unary call
const identity = await unary(stub, "getIdentity", {});

// server-streaming call (collects all items)
const cards = await serverStream(stub, "findAgents", { capability: "...", limit: 5 });
```
