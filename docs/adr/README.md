# Architecture Decision Records

| ADR | Title | Status |
|-----|-------|--------|
| [0001](./0001-language-agnostic-daemon-grpc.md) | Language-Agnostic Daemon with gRPC Interface | Accepted |
| [0002](./0002-did-key-identity.md) | DID:key for Agent Identity | Accepted |
| [0003](./0003-libp2p-no-ipfs-v1.md) | Plain libp2p — No IPFS Dependency in v1 | Accepted |
| [0004](./0004-quic-transport.md) | QUIC as Primary Transport | Accepted |
| [0005](./0005-inbox-outbox-sqlite.md) | Persistent Inbox/Outbox via SQLite | Accepted |
| [0006](./0006-gossipsub-event-streaming.md) | GossipSub for Event Streaming and Presence | Accepted |
| [0007](./0007-a2a-task-lifecycle.md) | Task Lifecycle Based on Google A2A Semantics | Accepted |
| [0008](./0008-actor-model-hierarchical.md) | Hierarchical Actor Model for Agent/Task Isolation | Accepted |
| [0009](./0009-capability-schema-two-tier.md) | Two-Tier Capability Schema | Accepted |
| [0010](./0010-thread-consensus-switchable-backends.md) | Switchable Thread Consensus Backends (Raft + Tendermint) | Accepted |
| [0011](./0011-store-and-forward-offline-delivery.md) | Store-and-Forward Offline Delivery via Persistent Outbox | Accepted |
| [0012](./0012-proto-as-canonical-standard.md) | proto/a2a.proto as the Single Canonical Standard | Accepted |
| [0013](./0013-content-addressed-blob-store.md) | Content-Addressed Blob Store with Always-Persist Semantics | Accepted |
| [0014](./0014-encrypted-thread-payloads.md) | End-to-end encrypted thread payloads | Accepted |
| [0015](./0015-goakt-actor-model-for-threads.md) | Durable, Virtualized GoAkt Actors | Accepted and implemented |
| [0016](./0016-distributed-tasks-membership-and-recovery.md) | Distributed Tasks, Late Observers, and Thread Recovery | Accepted and implemented |
| [0017](./0017-versioned-thread-key-envelopes-and-capability-recovery.md) | Versioned Thread Key Envelopes and Capability Recovery | Accepted and implemented |
| [0018](./0018-explicit-membership-lifecycle-and-raft-joint-consensus.md) | Explicit Membership Lifecycle and Raft Joint Consensus | Accepted and implemented |
| [0019](./0019-verified-archive-replication-and-recovery-discovery.md) | Verified Archive Replication and Recovery Discovery | Accepted and implemented |
| [0020](./0020-sdk-agent-session-authentication.md) | SDK Agent Session Authentication, Separate from Daemon Identity | Accepted and implemented |
| [0021](./0021-sybil-resistant-name-claims-via-dht-quorum-reads.md) | Sybil/Eclipse-Resistant Name Claims via DHT Quorum Reads | Accepted and implemented |
| [0022](./0022-lease-based-task-claiming-for-sdk-workers.md) | Lease-Based Task Claiming and Bounded Retry for SDK Workers | Accepted and implemented |
| [0023](./0023-terminal-ui-for-daemon-inspection.md) | A Terminal UI for Daemon Inspection | Accepted and implemented |
| [0024](./0024-canonical-daemon-entrypoint.md) | `cmd/moltmesh` as the Canonical Daemon Entrypoint | Accepted |

## Open Questions (Future ADRs)

- Trust and delegation model (capability attenuation, confused deputy)
- DID key rotation under active sessions
- IPFS/Ceramic integration for thread persistence (v2)
- Economic primitives (cost expression, quota, receipts)
- Sybil resistance and reputation model
