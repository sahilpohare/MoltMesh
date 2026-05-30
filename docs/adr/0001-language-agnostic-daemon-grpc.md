# ADR-0001: Language-Agnostic Daemon with gRPC Interface

**Status**: Accepted
**Date**: 2026-05-30

## Context

The protocol must be usable by AI agents built in any language or framework (Python, TypeScript, Rust, Java, etc). The core networking layer (libp2p, DHT, encryption) is complex and should not be reimplemented per language.

Three approaches considered:
1. Native library per language (reimplement core in each)
2. Single daemon + language-specific SDKs via IPC
3. HTTP/REST local API

## Decision

Ship a single Go daemon binary. Agents communicate with it via **gRPC over a Unix socket** (local) or TCP (remote/container). Auto-generate SDKs for each target language from a single `.proto` file.

## Rationale

- **Pilot Protocol** proved this model works at 230k agents. Single static binary, zero setup.
- gRPC gives streaming primitives natively (server-streaming, bidirectional streaming) — essential for task event streaming.
- One `.proto` file → typed clients in every language via `protoc` codegen. No manual SDK maintenance.
- Go is the reference implementation language for libp2p (go-libp2p powers IPFS, Filecoin, Ethereum).
- Unix socket for local IPC: zero network overhead, no port conflicts, OS-level access control.

## Consequences

- Agents must have the daemon running locally (or accessible via TCP).
- Proto file is the canonical contract — breaking changes require version bump.
- Daemon process lifecycle must be managed (systemd, Docker, etc).
- Generated SDKs (`sdk/python`, `sdk/typescript`) are thin wrappers — all logic lives in the daemon.
