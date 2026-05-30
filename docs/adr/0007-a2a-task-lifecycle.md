# ADR-0007: Task Lifecycle Based on Google A2A Semantics

**Status**: Accepted
**Date**: 2026-05-30

## Context

Agents need a shared model for what a "task" is — its states, transitions, and artifacts. Options considered:

1. Custom task model from scratch
2. Adopt Google A2A protocol task semantics
3. Minimal request/response only

## Decision

Adopt **Google A2A task lifecycle semantics** as the task model, adapted for P2P transport.

## Task Lifecycle

```
         ┌─────────┐
    ─────►│submitted│
         └────┬────┘
              │ assignee accepts
         ┌────▼────┐
    ┌────│ working │────┐
    │    └────┬────┘    │
    │         │         │
    │    ┌────▼─────┐   │
    │    │completed │   │
    │    └──────────┘   │
    │                   │
    │    ┌──────────┐   │
    └───►│  failed  │◄──┘
         └──────────┘
              │
         ┌────▼─────┐
         │cancelled │  (by initiator, any time before completed)
         └──────────┘
```

## Task Object

```protobuf
message Task {
  string id           = 1;  // UUID v4
  string initiator    = 2;  // DID of requesting agent
  string assignee     = 3;  // DID of executing agent
  string thread_id    = 4;  // conversation thread this task belongs to
  string skill        = 5;  // capability being invoked (namespaced)
  TaskStatus status   = 6;
  repeated Artifact input_artifacts  = 7;
  repeated Artifact output_artifacts = 8;
  int64 created_at    = 9;
  int64 updated_at    = 10;
  string error        = 11; // populated on failed
  map<string,string> metadata = 12;
}

enum TaskStatus {
  SUBMITTED  = 0;
  WORKING    = 1;
  COMPLETED  = 2;
  FAILED     = 3;
  CANCELLED  = 4;
}
```

## Artifacts

Artifacts are content-addressed blobs (files, JSON, text, binary). Each artifact has:
- `cid`: SHA256 content ID (IPLD-compatible)
- `mime_type`: content type
- `size`: byte size
- `inline`: small artifacts inlined as bytes; large artifacts referenced by CID and fetched separately

## Rationale

- Google A2A is becoming an industry standard (150+ organizations, Linux Foundation).
- Adopting the same task semantics means future interoperability with centralized A2A implementations.
- The task model is transport-agnostic — same semantics work over libp2p streams as over HTTP.
- Artifacts as content-addressed CIDs enables future IPFS pinning without changing the task schema.

## Consequences

- Tasks are the unit of work — not raw messages. Agents that want fire-and-forget messaging use tasks with immediate completion.
- Task IDs must be globally unique (UUID v4). Collision probability negligible at scale.
- Task state is owned by the assignee daemon. Initiator tracks state via GossipSub events.
- No distributed consensus on task state — assignee is the authority. Disputes are out of scope for v1.
