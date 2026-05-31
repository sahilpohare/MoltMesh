# ADR-0012: proto/a2a.proto as the Single Canonical Standard

**Status**: Accepted
**Date**: 2026-05-31

## Context

Early in development, the daemon grew three separate gRPC service definitions:

- `A2ANode` — core messaging, tasks, blobs, threads (in `proto/a2a.proto`)
- `Diag` — health, ping, list-peers (hand-written Go, `gen/a2a/v1/diag.go`)
- `Ext` — pub/sub, webhooks, networks, names (hand-written Go, `gen/a2a/v1/extensions.go`)

The Diag and Ext services were written directly as Go structs with only `json` tags — no protobuf field numbers, no `.proto` definitions. Their gRPC server descriptors used the same service name `"a2a.v1.A2ANode"`, causing a registration panic on startup that required a custom `RegisterAll` shim to merge them.

SDK clients (Python, TypeScript) could not use generated stubs for these methods because no proto definitions existed. The Python SDK shipped a separate `ext_stub.py` that serialized requests as JSON and called the server as if it were a REST API. The server, using the standard gRPC protobuf codec, decoded nothing; all calls failed silently or returned garbage.

## Decision

**`proto/a2a.proto` is the one and only source of truth for the entire protocol.**

All RPCs — identity, registry, messaging, tasks, blobs, threads, diagnostics, pub/sub, webhooks, networks, names — live in a single `service A2ANode`. All request/response message types are defined in the same file. Nothing is hand-written outside it.

Steps taken:
1. Added all missing RPCs and message types to `proto/a2a.proto` (Ping, Health, ListPeers, Publish, SubscribeTopic, SetWebhook, ClearWebhook, GetWebhook, CreateNetwork, JoinNetwork, LeaveNetwork, ListNetworks, NetworkMembers, BroadcastNetwork, SubscribeNetwork, ClaimName, ResolveName — and their request/response messages).
2. Regenerated Go stubs via `protoc --go_out --go-grpc_out`.
3. Deleted `gen/a2a/v1/diag.go`, `gen/a2a/v1/extensions.go`, `gen/a2a/v1/register_all.go`.
4. Regenerated Python stubs via `grpc_tools.protoc`. Fixed package import path (`from moltmesh.proto import a2a_pb2`).
5. Deleted `sdk/python/moltmesh/ext_stub.py`.
6. Updated all SDK clients (`client.py`, `client.ts`) to call the generated stub methods with proper protobuf types.

## Rationale

- **One source, one codec.** All methods share the same protobuf wire encoding. No JSON islands inside a gRPC service.
- **Generated SDKs are correct by construction.** Running `protoc` produces type-safe stubs in every language. No stub can drift from the server.
- **Single service registration.** gRPC's server panics on duplicate service names. One service = no workaround needed.
- **Cross-language contract.** Any language with a `protoc` plugin can generate a fully functional client without any hand-written code.
- **Testability.** Integration tests can be written against the generated types. All 42 Python and 34 TypeScript integration tests now pass against a live daemon with no mocks.

## Consequences

- **Proto file must be updated before any new RPC is added.** The workflow is: edit `.proto` → run `make proto` → update server handler → update SDK clients.
- **Regeneration is a required step after any proto change.** Both Go and Python stubs must be regenerated; Python stubs require a post-processing fix for the package import path (`from moltmesh.proto import a2a_pb2` not the bare `import a2a_pb2` that `protoc` emits).
- **Breaking changes require a version bump.** Field removals, type changes, and RPC renames are breaking. Use new field numbers; do not reuse deleted ones.
- The TypeScript SDK uses `@grpc/proto-loader` at runtime — it reads the `.proto` file directly rather than pre-generating stubs, so TypeScript picks up proto changes automatically without a codegen step.

## Protocol Regeneration

```bash
# Go
protoc \
  --go_out=gen/a2a/v1 --go_opt=paths=source_relative \
  --go-grpc_out=gen/a2a/v1 --go-grpc_opt=paths=source_relative \
  proto/a2a.proto

# Python (run from sdk/python/)
python -m grpc_tools.protoc \
  -I ../../proto \
  --python_out=moltmesh/proto \
  --grpc_python_out=moltmesh/proto \
  ../../proto/a2a.proto
# then fix the import in a2a_pb2_grpc.py:
# "import a2a_pb2" → "from moltmesh.proto import a2a_pb2 as a2a__pb2"
```
