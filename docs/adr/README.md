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

## Open Questions (Future ADRs)

- ADR-0012: Thread encryption model (per-thread key derivation, Signal ratchet vs ECIES)
- ADR-0013: Trust and delegation model (capability attenuation, confused deputy)
- ADR-0014: DID key rotation under active sessions
- ADR-0015: IPFS/Ceramic integration for thread persistence (v2)
- ADR-0016: Economic primitives (cost expression, quota, receipts)
- ADR-0017: Sybil resistance and reputation model
