# ADR-0005: Persistent Inbox/Outbox via SQLite

**Status**: Accepted
**Date**: 2026-05-30

## Context

Agents are not always online. Messages sent to offline agents must be held for delivery. Agents must not lose outgoing messages on daemon crash. Options considered:

1. In-memory queues (lost on crash)
2. Redis / external message broker (new dependency, centralized)
3. SQLite WAL (embedded, crash-safe, zero dependency)

## Decision

Use **SQLite with WAL mode** for both inbox and outbox queues. One SQLite database per agent, stored in the daemon's data directory.

## Rationale

- **Crash-safe**: SQLite WAL mode provides durability. Messages survive daemon restarts.
- **Zero dependency**: no external broker. The daemon is a single binary.
- **Offline delivery**: outbox holds messages with TTL. Retry loop attempts delivery when remote agent comes online (detected via DHT presence or GossipSub heartbeat).
- **Actor model fit**: inbox is processed sequentially per thread — SQLite's single-writer model matches this naturally.
- **Inspectable**: SQLite databases are human-readable with standard tools. Useful for debugging.

## Schema (Inbox)

```sql
CREATE TABLE inbox (
    id          TEXT PRIMARY KEY,   -- message CID (content-addressed)
    from_did    TEXT NOT NULL,
    thread_id   TEXT NOT NULL,
    task_id     TEXT,
    payload     BLOB NOT NULL,       -- encrypted, serialized protobuf
    received_at INTEGER NOT NULL,
    read_at     INTEGER,
    INDEX idx_thread (thread_id),
    INDEX idx_task (task_id)
);
```

## Schema (Outbox)

```sql
CREATE TABLE outbox (
    id           TEXT PRIMARY KEY,  -- message CID
    to_did       TEXT NOT NULL,
    thread_id    TEXT,
    task_id      TEXT,
    payload      BLOB NOT NULL,
    created_at   INTEGER NOT NULL,
    expires_at   INTEGER NOT NULL,  -- TTL
    attempts     INTEGER DEFAULT 0,
    last_attempt INTEGER,
    status       TEXT DEFAULT 'pending'  -- pending | delivered | failed | expired
);
```

## Consequences

- SQLite is single-process. Two daemon instances cannot share the same database file.
- For high-throughput agents (>10k messages/day), SQLite may become a bottleneck. Migration path: replace with embedded key-value store (bbolt, pebble) per queue item.
- TTL on outbox items bounds storage growth. Default TTL: 72 hours. Configurable.
