# ADR-0013: Content-Addressed Blob Store with Always-Persist Semantics

**Status**: Accepted
**Date**: 2026-05-31

## Context

The daemon needs to store arbitrary binary data (files, artifacts, model outputs) and make it retrievable by content hash. The key design questions are:

1. **CID format** — how are blobs identified?
2. **Small-file handling** — should small blobs be inlined in responses or written to disk?
3. **Fetch semantics** — how does a client retrieve a blob it didn't store locally?

An earlier implementation inlined small blobs (≤64 KB) in the `Artifact.inline` field and skipped writing them to disk. The `FetchFile` RPC read from disk only. This created a silent correctness bug: `SendFile` returned a CID for a small blob, but a subsequent `FetchFile` with that CID failed with "blob not found locally" because nothing was ever written to disk.

## Decision

### CID format

`sha256:<hex>` — the hex-encoded SHA-256 hash of the raw bytes, prefixed with `sha256:`. Example:

```
sha256:e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
```

Strict validation on read and write: must begin with `sha256:`, followed by exactly 64 lowercase hex characters. Invalid CIDs are rejected at the API boundary.

### Always-persist semantics

`Put` always writes to disk, regardless of size. For small blobs, `Artifact.inline` is still populated in the response (for callers that want the data immediately without a round-trip), but the on-disk copy is always written. `Get` always reads from disk.

```
Put(data)
  cid = sha256(data)
  write_to_disk(cid, data)          ← always
  if len(data) <= 64KB:
      artifact.inline = data        ← convenience copy in response
  else:
      artifact.uri = "blob://" + cid
  return artifact

Get(cid)
  return read_from_disk(cid)        ← always works after Put
```

Writes are atomic: data is written to a temp file in the same directory, then renamed. This prevents partially-written blobs from being read on crash.

### Remote fetch

`FetchFile` checks the local store first. If not found and a `from_did` is provided, it resolves the peer's Agent Card from the DHT, fetches the blob over the `/a2a/blob/1.0.0` libp2p protocol, verifies the CID matches (content-addressed integrity check), caches locally, and streams back in chunks.

### Streaming chunk size

`FetchFile` streams data in 32 KB chunks. This keeps individual gRPC messages within the default HTTP/2 flow-control window (65535 bytes), ensuring compatibility with gRPC client implementations that do not send WINDOW_UPDATE frames aggressively (observed with `@grpc/grpc-js` in bun's HTTP/2 runtime).

## Rationale

- **Always-persist eliminates the consistency gap.** A CID returned by `SendFile` is always fetchable by `FetchFile` without any race condition or size-dependent special case.
- **SHA-256 CIDs are IPLD-compatible.** Future integration with IPFS/Filecoin content routing requires no CID format change.
- **Atomic writes prevent corruption.** The temp-then-rename pattern is standard for content-addressed stores. Same-directory temp file ensures the rename is atomic on POSIX (same filesystem).
- **32 KB chunks balance throughput and flow-control.** Large chunks (256 KB) trigger HTTP/2 flow-control stalls in some clients. 32 KB stays safely below the 64 KB default window.
- **Inline field preserved for small blobs.** Callers that receive an `Artifact` directly from `SendFile` can read `inline` without an extra `FetchFile` round-trip.

## Consequences

- Small blobs use slightly more disk I/O than before (one `write + fsync` per small blob). For workloads with many tiny blobs this is measurable but acceptable; the alternative (lost data) is not.
- The `inline` field in `Artifact` is a response convenience only — callers must not assume it means the blob is not on disk.
- Remote fetch requires the peer to have a published Agent Card with reachable multiaddrs. Blobs stored by a peer that has never called `PublishAgentCard` cannot be fetched remotely.
- `FetchFile` with no `from_did` on a missing CID returns an error immediately. This is intentional — no silent fallback to remote fetch without an explicit peer target.

## Alternatives Considered

**Keep inline-only for small blobs, fix `FetchFile` to check inline store**
Would require maintaining a second in-memory or SQLite map from CID → inline bytes. More complex, no durability across daemon restarts. Rejected.

**Use IPFS CIDv1 (multihash + multicodec)**
More expressive but adds complexity and a dependency. The `sha256:` prefix is sufficient for v1 and is mechanically convertible to CIDv1 when needed. Deferred to a future ADR on IPFS integration.

**256 KB chunk size**
Higher throughput for large blobs. Breaks streaming with `@grpc/grpc-js` on bun's HTTP/2 runtime (RST_STREAM FLOW_CONTROL_ERROR). Rejected.
