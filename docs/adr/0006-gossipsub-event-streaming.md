# ADR-0006: GossipSub for Event Streaming and Presence

**Status**: Accepted
**Date**: 2026-05-30

## Context

Task execution produces a stream of events (token chunks, tool calls, status updates, completion signals). Multiple agents may want to observe the same task. Options considered:

1. Point-to-point streams (only the initiator sees events)
2. GossipSub pub/sub (any subscriber sees events)
3. Custom fan-out at the daemon level

## Decision

Use **libp2p GossipSub** for:
- Task event streaming (observable by any subscribed agent)
- Task completion broadcasts
- Agent presence/heartbeat
- Capability advertisement push

Use **point-to-point libp2p streams** for:
- Task delegation (initiator → assignee)
- Artifact transfer (large binary payloads)
- Direct agent-to-agent messaging

## Topic Naming Convention

```
a2a/tasks/{task_id}/events      — streaming task progress
a2a/tasks/{task_id}/done        — task completion + result artifact CID
a2a/agents/{did}/presence       — heartbeat (published every 30s)
a2a/capabilities/{namespace}    — capability advertisement push
```

## Rationale

- GossipSub is native to libp2p — no additional infrastructure.
- Fan-out is handled by the gossip mesh — daemon doesn't need to manage subscriber lists.
- Multiple agents can observe the same task stream without coordination (useful for audit, monitoring, orchestration).
- Presence via GossipSub enables outbox retry: when an agent's presence is detected, the outbox flushes pending messages.
- Capability push via GossipSub enables reactive discovery — agents subscribe to a namespace topic and receive new capability advertisements without polling DHT.

## Message Format on Topics

All GossipSub messages are serialized protobuf, signed with the publishing agent's Ed25519 key. Receivers verify the signature before processing.

## Consequences

- GossipSub topics are public within the network. Task event topics expose task IDs. Agents requiring private task execution must use point-to-point streams only and not publish to GossipSub.
- Topic proliferation: one topic per task_id. Tasks must be cleaned up (topic unsubscription) after completion + grace period.
- GossipSub has eventual delivery — not suitable for at-most-once semantics. Task completion signals should be idempotent.
