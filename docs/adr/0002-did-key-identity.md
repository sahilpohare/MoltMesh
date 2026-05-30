# ADR-0002: DID:key for Agent Identity

**Status**: Accepted
**Date**: 2026-05-30

## Context

Agents need a globally unique, verifiable identity that works without a central authority. Options considered:

1. Public key hash (like Fetch.ai ACN — address = hash(pubkey))
2. Virtual address assigned by a registry (like Pilot Protocol — `N:NNNN.HHHH.LLLL`)
3. W3C DID (Decentralized Identifier)

## Decision

Use **`did:key`** — a W3C DID method where the DID is derived directly from an Ed25519 public key. No registry required.

Format: `did:key:z6Mk...` (base58btc-encoded multicodec-prefixed public key)

## Rationale

- **Self-sovereign**: no issuer, no registry, no company can revoke it.
- **Portable**: the same DID works across any infrastructure, any peer, any restart.
- **Verifiable**: anyone can derive the public key from the DID and verify signatures.
- **W3C standard**: interoperable with existing DID ecosystems (Ceramic, Verifiable Credentials, etc).
- **Ceramic-native**: Ceramic Network uses DIDs as stream controllers — future IPFS/Ceramic integration is natural.
- **Simple**: `did:key` requires no DID Document resolution network. The key IS the document.

## Consequences

- Key rotation breaks the DID. Agents that rotate keys get a new identity. This is a known `did:key` limitation.
- For key rotation support in future: migrate to `did:peer` or `did:web` — reserved for ADR-0002a.
- All messages, Agent Cards, and artifacts must be signed with the agent's Ed25519 private key.
- Identity = keypair. Private key must be stored securely by the daemon (encrypted at rest).
