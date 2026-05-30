# ADR-0011: Store-and-Forward Offline Delivery via Persistent Outbox

**Status:** Accepted
**Date:** 2024-01

## Context

In a P2P network, agents are not always online simultaneously. A naive delivery model — open a stream, send, close — fails silently when the recipient is offline. This is particularly problematic for:

- **Task delegation**: initiator sends a task to an offline assignee.
- **Thread entries**: validator sends a consensus message to a temporarily partitioned peer.
- **Notifications**: agent sends a result to a requester that has disconnected.

Options for handling offline recipients:

1. **Fire and forget** — drop the message if the peer is unreachable. Simple but lossy.
2. **Rendezvous nodes** — a third party holds messages for offline peers (e.g. Signal mailbox servers). Introduces centralization.
3. **DHT store** — write messages to the DHT keyed by recipient DID. DHT values are small and ephemeral; not suitable for arbitrary payloads.
4. **Persistent local outbox with retry** — sender queues messages locally, retries delivery with backoff until the recipient comes online (within a TTL). No third party required.

## Decision

Use a **persistent local outbox** backed by SQLite, with an async retry worker.

### Outbox Schema

```sql
CREATE TABLE outbox (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    to_did     TEXT NOT NULL,
    payload    BLOB NOT NULL,
    created_at INTEGER NOT NULL,
    attempt    INTEGER NOT NULL DEFAULT 0,
    next_retry INTEGER NOT NULL DEFAULT 0,
    status     TEXT NOT NULL DEFAULT 'pending'
);
```

### Delivery Worker

```
outbox worker (goroutine)
  loop every N seconds:
    SELECT messages WHERE status='pending' AND next_retry <= now()
    for each message:
      resolve(to_did) → DHT lookup → multiaddr
      dial libp2p peer → open /a2a/msg/1.0.0 stream
      if success: mark delivered
      if fail:    increment attempt, set next_retry (exponential backoff), cap at TTL
      if attempt > max or now > ttl: mark failed
```

### Backoff

- Attempt 1: retry after 5s
- Attempt 2: retry after 15s
- Attempt 3: retry after 60s
- Attempt 4+: retry after 5m
- Max TTL: configurable, default 24h

### Delivery Function

The outbox takes a `DeliverFunc` at construction time:

```go
type DeliverFunc func(toMultiaddr string, msg *pb.Message) error
```

This is provided by the `deliver` package (`Deliverer.DeliverFunc()`), which opens a libp2p stream and writes a msgio-framed protobuf. The outbox is not coupled to libp2p directly.

## Consequences

**Good:**
- Messages survive offline recipients with no third party.
- Delivery is durable — daemon restarts do not lose queued messages.
- Clean separation: outbox knows nothing about libp2p; deliver knows nothing about queuing.
- Easy to test: inject a mock DeliverFunc.

**Trade-offs:**
- Messages accumulate on the sender's disk until delivered or TTL expires.
- DHT lookup is required on each retry attempt (peer multiaddr may change).
- No end-to-end acknowledgement — delivery is best-effort within TTL. If the recipient's daemon crashes immediately after receiving, the message is lost.
- Large payloads should use the blob protocol; the outbox is for small protobuf messages.

## Alternatives Considered

**Fire and forget**
Rejected: unacceptable for task delegation — the entire task would be lost silently.

**Rendezvous nodes**
Third-party nodes hold messages for offline peers. Rejected for v1: introduces centralization and requires a separate infrastructure component. May be revisited in v2 for cross-NAT scenarios where the sender also goes offline before the recipient reconnects.

**DHT store (PubSub offline buffer)**
DHT values are limited in size and TTL. GossipSub does not buffer for offline peers. Rejected.

**libp2p AutoRelay + persistent connection**
AutoRelay keeps a circuit open to a relay node. Useful for NAT traversal but does not solve the message persistence problem.

## Interaction with Other Components

- **Inbox**: recipient daemon writes arriving messages to its local SQLite inbox. The sender's outbox and the recipient's inbox together form the store-and-forward pair.
- **Thread consensus**: consensus messages (Raft/Tendermint) are NOT sent via the outbox — they go through GossipSub for low latency. The outbox is for application-level messages only.
- **Blobs**: large file payloads are stored in the blob store; the outbox message carries only the CID reference. Blob fetch is on-demand over `/a2a/blob/1.0.0`.
