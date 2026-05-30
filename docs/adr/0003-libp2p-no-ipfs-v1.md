# ADR-0003: Plain libp2p — No IPFS Dependency in v1

**Status**: Accepted
**Date**: 2026-05-30

## Context

IPFS is built on libp2p and provides content-addressed storage, pinning, Merkle DAGs, and garbage collection. Initial design considered using IPFS (via Kubo daemon or Boxo library) for:
- Agent Card storage (IPNS)
- Thread history persistence
- Artifact storage

## Decision

**v1 uses plain libp2p only.** No IPFS, no Kubo, no Boxo dependency.

Thread and artifact storage uses local SQLite with SHA256-based content IDs (IPLD-compatible CID format) that can be migrated to IPFS in v2 without breaking the wire format.

## Rationale

- IPFS adds: a running daemon OR a heavy embedded library, garbage collection complexity, pinning infrastructure, and Bitswap protocol — none of which are needed for v1.
- libp2p alone provides everything needed: Kademlia DHT, QUIC transport, GossipSub, NAT traversal, Noise XX.
- IPFS is an application built on libp2p, not a transport primitive.
- Keeping IPFS out of v1 reduces the dependency surface and deployment complexity dramatically.
- Content-addressed CIDs (SHA256 multihash) are IPFS-compatible by design — v2 can pin to IPFS without changing the data model.

## Consequences

- Thread history is local-only in v1. Not globally retrievable if the originating agent is offline.
- Agent Cards are stored in DHT only (ephemeral, TTL-based). No permanent IPFS backup in v1.
- v2 migration path: add optional IPFS pinning for thread archives and Agent Cards. Agents opt in.
- IPFS becomes a persistence/portability layer, not a hard dependency.
