# IPFS Rebase Plan

**Goal:** Rebase the daemon onto a native IPFS host. The existing libp2p host is extended with
Bitswap + flatfs blockstore so it participates in the global IPFS network. The custom blob
transport protocol (`/a2a/blob/1.0.0`) is deleted and replaced by Bitswap. CIDs migrate from
`sha256:<hex>` to CIDv1 (`bafy...`). Breaking changes are accepted — the project is pre-launch.

**Prerequisite before public launch (not in this plan):** blob encryption at rest.

---

## Ticket Index

| # | Title | Layer | Depends on |
|---|-------|-------|------------|
| [T-01](#t-01-ipfs-host--blockstore-in-nodenode) | IPFS host + blockstore in `node/node` | Infra | — |
| [T-02](#t-02-bitswap-replaces-custom-blob-transport) | Bitswap replaces custom blob transport | Blob | T-01 |
| [T-03](#t-03-rewrite-daemonblob-as-blockstore-adapter) | Rewrite `daemon/blob` as blockstore adapter | Blob | T-01 |
| [T-04](#t-04-cidv1-in-proto--rpc-layer) | CIDv1 in proto + RPC layer | Protocol | T-02, T-03 |
| [T-05](#t-05-dht-validator-for-a2a-namespace) | DHT validator for `/a2a/` namespace | Registry | T-01 |
| [T-06](#t-06-update-sdks-python--typescript) | Update SDKs — Python + TypeScript | SDKs | T-04 |
| [T-07](#t-07-update-e2e--unit-tests) | Update e2e + unit tests | Testing | T-02–T-06 |

---

## T-01: IPFS host + blockstore in `node/node`

**Layer:** `daemon/node/node.go`

### What changes

1. **DHT prefix:** `dht.ProtocolPrefix("/a2a")` → remove (use default `/ipfs/kad/1.0.0`).
   The node joins the global IPFS Kademlia DHT.

2. **Flatfs datastore:** open a durable on-disk flatfs at `<dataDir>/blocks/` using
   `github.com/ipfs/go-ds-flatfs`. This is the backing store for the Bitswap blockstore.

3. **Blockstore:** wrap flatfs with `boxo/blockstore.NewBlockstore(ds)`. Add to `Node` struct.

4. **Bitswap:** construct `bitswap.New(ctx, bsnet.NewFromIpfsHost(host), dht, blockstore)`.
   The DHT already implements `routing.ContentDiscovery` — pass it directly. Add to `Node` struct.

5. **Node struct additions:**
   ```go
   type Node struct {
       Host       host.Host
       DHT        *dht.IpfsDHT
       PubSub     *pubsub.PubSub
       Identity   *identity.Identity
       Blockstore blockstore.Blockstore   // NEW
       Bitswap    *bitswap.Bitswap        // NEW
       log        *zap.Logger
   }
   ```

6. **`Node.Close()`:** add `n.Bitswap.Close()` before DHT close.

### New imports
```
github.com/ipfs/boxo/bitswap
github.com/ipfs/boxo/bitswap/network/bsnet
github.com/ipfs/boxo/blockstore
github.com/ipfs/go-ds-flatfs
github.com/ipfs/go-datastore
```

### go.mod changes
- `github.com/ipfs/go-ds-flatfs` promoted to direct dep (already `go get`'d)
- `github.com/ipfs/boxo` promoted to direct dep

### Acceptance criteria
- `go build ./daemon/node/...` passes
- Node starts, connects to IPFS bootstrap peers, DHT bootstrap succeeds
- `n.Blockstore` and `n.Bitswap` are non-nil after `node.New()`

---

## T-02: Bitswap replaces custom blob transport

**Layer:** `daemon/deliver/blob.go`, `daemon/deliver/deliver.go`, `cmd/daemon/main.go`

### What changes

1. **Delete** `daemon/deliver/blob.go` entirely.
   - Removes `BlobProtocol = "/a2a/blob/1.0.0"`
   - Removes `RegisterBlobHandler()`
   - Removes `FetchBlob()`

2. **`Deliverer` struct** — remove the blob handler registration from `New()` and all
   call sites. `Deliverer` handles only message delivery going forward.

3. **`cmd/daemon/main.go`** — remove:
   ```go
   dlv.RegisterBlobHandler(bs)   // DELETE
   ```

4. **`rpc/server.go` `FetchFile` handler** — currently calls `dlv.FetchBlob(ctx, peerID, cid)`.
   Replace with:
   ```go
   block, err := n.Bitswap.GetBlock(ctx, parseCIDv1(req.Cid))
   ```
   Streaming to gRPC client stays the same (chunked writes), just the source changes.

5. **`rpc/server.go` `SendFile` handler** — currently calls `bs.Put(data, name, mimeType)`.
   Replace with blockstore put:
   ```go
   blk := blocks.NewBlock(data)
   n.Blockstore.Put(ctx, blk)
   // notify Bitswap so peers can pull it
   n.Bitswap.NotifyNewBlocks(ctx, blk)
   ```
   Return CIDv1 string from `blk.Cid().String()`.

### Acceptance criteria
- `daemon/deliver/blob.go` is deleted
- `go build ./...` passes
- `SendFile` stores block in Bitswap blockstore
- `FetchFile` retrieves block via Bitswap (works from remote peer)

---

## T-03: Rewrite `daemon/blob` as blockstore adapter

**Layer:** `daemon/blob/blob.go`

### What changes

The `daemon/blob` package currently implements a custom flat-file store with `sha256:<hex>` CIDs.
It is replaced by a thin adapter that wraps `n.Blockstore` and speaks the same interface the RPC
layer uses.

**Option A (preferred):** delete `daemon/blob` package entirely. All callers (`rpc/server.go`,
`main.go`) switch directly to `blockstore.Blockstore` + `blocks.Block`. The `daemon/blob` package
ceases to exist.

**Option B:** keep `daemon/blob` as a shim over `blockstore.Blockstore` for backward compatibility
with test helpers. Implement `Put`, `Get`, `Has` in terms of blockstore ops, replacing CID logic.

Recommendation: **Option A**. The package is small and all callers are internal.

### Changes in call sites
- `main.go`: remove `blob.New(...)`, remove `bs` variable, pass `n.Blockstore` directly to `rpc.New()`
- `rpc/server.go`: replace `*blob.Store` param with `blockstore.Blockstore`
- `rpc.New()` signature: `bs *blob.Store` → `bs blockstore.Blockstore`
- All tests using `blob.New()` switch to `blockstore.NewBlockstore(datastore.NewMapDatastore())`

### Acceptance criteria
- `daemon/blob/` is either deleted or is a pure pass-through with no custom CID logic
- No remaining references to `sha256:` prefix format in non-test production code
- `go build ./...` passes

---

## T-04: CIDv1 in proto + RPC layer

**Layer:** `proto/a2a.proto`, `gen/a2a/v1/`, `daemon/rpc/server.go`

### What changes

1. **`proto/a2a.proto` — `Artifact` message:**
   ```protobuf
   message Artifact {
     string cid       = 1; // CIDv1 (bafy... base32 multihash)
     bytes  inline    = 2; // inline data for small blobs (≤ 64KB)
     string uri       = 3; // blob://bafy... for large blobs
     string name      = 4;
     string mime_type = 5;
     int64  size      = 6;
   }
   ```
   Comment updated — format changes from `sha256:<hex>` to CIDv1. No field number changes.

2. **`FetchFileRequest`:**
   ```protobuf
   message FetchFileRequest {
     string cid      = 1; // CIDv1 (bafy...)
     string from_did = 2; // hint: which peer to fetch from (optional)
   }
   ```

3. **Regenerate stubs:**
   ```bash
   protoc -I proto \
     --go_out=gen/a2a/v1 --go_opt=paths=source_relative \
     --go-grpc_out=gen/a2a/v1 --go-grpc_opt=paths=source_relative \
     proto/a2a.proto
   ```
   Python stubs regenerated + import path fix applied.

4. **`rpc/server.go`** — add `parseCIDv1(s string) (cid.Cid, error)` helper.
   All CID string parsing goes through this; returns proper error on bad format.

5. **`daemon/registry/registry.go`** — `capabilityCID()` already produces CIDv1. No change needed.
   `dhtKey()` uses `/a2a/agents/<did>` — unaffected by CID format change.

6. **`daemon/thread/tendermint.go`** — `block_hash` field uses `sha256(canonical(block))` encoded
   as hex. This is internal to consensus, not an IPFS CID. **No change.**

### Acceptance criteria
- Proto regenerated, `go build ./...` passes
- `SendFile` returns `Artifact.Cid` as CIDv1 string (`bafy...`)
- `FetchFile` accepts CIDv1 string and retrieves block
- Invalid CID returns gRPC `InvalidArgument`

---

## T-05: DHT validator for `/a2a/` namespace

**Layer:** `daemon/registry/registry.go`, `daemon/node/node.go`

### Problem

Agent Cards are stored in the IPFS DHT under `/a2a/agents/<did>`. IPFS DHT peers run validators
on `PutValue` records. Non-A2A peers have no validator for `/a2a/` — they reject or silently drop
puts. Result: agent card publishing fails in a mixed IPFS+A2A network.

### What changes

1. **Implement `record.Validator`** for the `/a2a/` namespace:
   ```go
   // pkg/a2avalidator/validator.go
   type AgentCardValidator struct{}

   func (v AgentCardValidator) Validate(key string, value []byte) error {
       // key format: /a2a/agents/<did>
       // value: JSON-encoded AgentCard with Ed25519 signature
       // verify signature using DID embedded public key
   }

   func (v AgentCardValidator) Select(key string, vals [][]byte) (int, error) {
       // prefer most recently published (highest published_at)
   }
   ```

2. **Register validator on DHT init** in `node/node.go`:
   ```go
   dht.New(ctx, h,
       dht.Mode(dht.ModeAutoServer),
       dht.Validator(record.NamespacedValidator{
           "a2a": a2avalidator.AgentCardValidator{},
           "pk":  record.PublicKeyValidator{}, // keep IPFS default
       }),
   )
   ```

3. Non-A2A IPFS peers that don't know the `/a2a/` validator will reject puts — this is
   acceptable. A2A peers will validate and store correctly. The DHT still routes the keys
   to the closest peers, most of which in a real A2A deployment will be A2A nodes.

### Acceptance criteria
- `go build ./...` passes
- `Publish(card)` succeeds when DHT has a mix of A2A-aware and non-A2A peers
- `Resolve(did)` returns valid card after publish
- Invalid signatures are rejected by the validator

---

## T-06: Update SDKs — Python + TypeScript

**Layer:** `sdk/python/`, `sdk/typescript/`

### What changes

**Both SDKs:** CID format change. Any code that constructs, validates, or displays CIDs must
stop assuming `sha256:<hex>` format.

**Python (`sdk/python/moltmesh/client.py`):**
- `send_file()` return value: `artifact.cid` is now `bafy...` format
- `fetch_file(cid=...)` callers pass CIDv1 strings
- Remove any `sha256:` prefix stripping/validation helpers

**TypeScript (`sdk/typescript/openclaw-plugin/src/client.ts`):**
- Same — `sendFile()` returns CIDv1, `fetchFile(cid)` accepts CIDv1
- Remove any `sha256:` format assumptions

**Tests:**
- Python: update all `assert artifact.cid.startswith("sha256:")` → `assert artifact.cid.startswith("bafy")`
- TypeScript: same pattern

**Proto stubs:** regenerate Python stubs after T-04 proto change:
```bash
cd sdk/python
python -m grpc_tools.protoc -I ../../proto \
  --python_out=moltmesh/proto --grpc_python_out=moltmesh/proto \
  ../../proto/a2a.proto
# fix import: "import a2a_pb2" → "from moltmesh.proto import a2a_pb2 as a2a__pb2"
```

### Acceptance criteria
- All Python integration tests pass (`pytest sdk/python/tests/`)
- All TypeScript integration tests pass (`bun test sdk/typescript/`)
- No `sha256:` assertions remain in SDK tests

---

## T-07: Update e2e + unit tests

**Layer:** `e2e/e2e_test.go`, `daemon/*/`*`_test.go`

### What changes

1. **`e2e/e2e_test.go`** — `TestE2E_SendFile` and `TestE2E_FetchFile`:
   - Replace `blob.New()` setup with `blockstore.NewBlockstore(datastore.NewMapDatastore())`
   - Assert CIDs start with `bafy` not `sha256:`
   - `FetchFile` test uses Bitswap path — requires two nodes with connected hosts

2. **`daemon/blob/blob_test.go`** — if `daemon/blob` package is deleted (T-03 Option A),
   delete this file. If kept as adapter, update all CID format assertions.

3. **`daemon/deliver/deliver_test.go`** — `TestFetchBlob_*` tests become obsolete when
   `FetchBlob` is deleted. Delete these tests. Add Bitswap integration test if needed.

4. **`daemon/rpc/server_test.go`** — update `SendFile`/`FetchFile` test cases for CIDv1 format.

5. **New: `daemon/node/node_test.go`** — test that:
   - Node starts with non-nil `Blockstore` and `Bitswap`
   - Put a block, retrieve it via `Bitswap.GetBlock()`
   - Two nodes connected can exchange a block via Bitswap

### Acceptance criteria
- `go test ./...` passes
- No tests reference `sha256:` CID format in assertions
- Bitswap block exchange verified in at least one integration test

---

## Execution order

```
T-01  →  T-02  →  T-03  →  T-04  →  T-05
                                ↓
                              T-06
                                ↓
                              T-07
```

T-01 through T-03 can be done as a single commit or sequentially — they are all Go-only changes.
T-04 requires proto regeneration and must precede T-06.
T-05 is independent of T-04 and can run in parallel.
T-07 is the final integration pass.

---

## Files deleted at end of rebase

| File | Reason |
|---|---|
| `daemon/deliver/blob.go` | Replaced by Bitswap (T-02) |
| `daemon/blob/blob.go` | Replaced by blockstore adapter (T-03, Option A) |
| `daemon/blob/blob_test.go` | Package deleted (T-07) |

## Files added at end of rebase

| File | Reason |
|---|---|
| `pkg/a2avalidator/validator.go` | DHT validator for `/a2a/` namespace (T-05) |

## go.mod: deps promoted from indirect to direct

```
github.com/ipfs/boxo
github.com/ipfs/go-ds-flatfs
github.com/ipfs/go-block-format
```
