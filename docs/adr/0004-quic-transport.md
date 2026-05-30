# ADR-0004: QUIC as Primary Transport

**Status**: Accepted
**Date**: 2026-05-30

## Context

libp2p supports multiple transports: TCP, WebSocket, QUIC, WebTransport. Choice of transport affects latency, streaming behavior, and NAT traversal.

Pilot Protocol chose UDP with a custom reliable streaming layer specifically to avoid TCP head-of-line blocking.

## Decision

Use **QUIC** as the primary transport via `go-libp2p`'s built-in QUIC support. TCP as fallback.

## Rationale

- **No head-of-line blocking**: QUIC multiplexes streams over UDP. A dropped packet blocks only the affected stream, not all streams on the connection. Critical for concurrent task streaming.
- **0-RTT connection establishment**: QUIC supports 0-RTT for known peers — reduces latency on reconnection.
- **Built-in encryption**: QUIC uses TLS 1.3 internally. Combined with Noise XX at the libp2p layer, channels are double-encrypted (acceptable overhead given the security model).
- **NAT traversal**: QUIC over UDP works better with many NAT configurations than TCP.
- **go-libp2p native**: QUIC is first-class in go-libp2p, not a third-party plugin.
- Pilot proved UDP-based transport outperforms TCP for agent workloads (12s vs 51s query resolution).

## Consequences

- Some firewalls block UDP. TCP fallback must be configured and tested.
- QUIC + Noise XX = double encryption. Minor CPU overhead, acceptable.
- WebSocket transport may be needed for browser-based agents (future ADR).
