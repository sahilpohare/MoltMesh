# Manual three-agent thread lifecycle

This harness runs three independent identities across two simulated hosts:
`node-a/textgen`, `node-a/observer`, and `node-b/calculator`. Each has an
isolated persistent `HOME`; no DID, libp2p peer ID, or multiaddress is passed
between agents. Discovery happens through advertised capabilities.
A neutral local bootstrap daemon supplies the DHT rendezvous point; agents
know its address but never receive another agent's DID, peer ID, or address.

Requirements: Go and `jq`.

```sh
./e2e/manual-thread/run.sh
```

The run proves thread commit/replication, consensus-recorded late observer
addition, task input delivery on that thread, real calculator execution,
authenticated result return, durable event replay, thread result commitment,
and read-only recovery by a fresh identity using the capability emitted at
thread creation. The harness keeps that secret only in its temporary runtime
directory; a public thread ID is not sufficient for recovery.

Identity directories persist between runs, making DIDs deterministic after the
first setup. Remove `e2e/manual-thread/runtime` manually to generate new ones.

“All agents left” means the agent loops exited. At least one daemon holding the
content must remain online while the new reader fetches it via Bitswap. If every
copy-holder is offline, retrieval is physically impossible; restarting any
original daemon restores availability from its durable blockstore.
