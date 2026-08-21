# ADR-0021: Sybil/Eclipse-Resistant Human-Readable Names via DHT Quorum Reads

**Status**: Accepted and implemented
**Date**: 2026-08-21

## Context

A `did:key` (ADR-0002) is exactly as memorable as a base58-encoded public key, which is to say not at all. Agents and their operators need a human-readable handle — `swift-falcon`, `bright-harbor-relay` — that resolves to a DID, without introducing a registry a company could own or gate.

The obvious implementation is a DHT record: `PutValue("/a2a/names/<name>", signedClaim)`, resolved with `GetValue`. That alone has a real weakness. Kademlia's `GetValue` (and this project's own `Registry.Resolve` for Agent Cards, ADR-0002/0009) is satisfied by the first responding peer close to the key. An attacker who controls, or has eclipsed, a majority of the closest-K peers to a name's hash can simply not return the real claim, or return a stale one, and a single-peer read has no way to notice. `docs/adr/README.md`'s own "Open Questions" list names general "Sybil resistance and reputation model" as unaddressed future work for the network as a whole — but a working, narrower answer to exactly that problem, scoped to name claims, was implemented in `daemon/names/names.go` without ever being written up as its own decision.

## Decision

Implement name claiming as a DHT record with two specific defenses baked into how it's read and written, both already present in the shipped code:

**Quorum reads, not single-peer reads.** `Resolve` does not call `GetValue`; it calls `SearchValue`, which queries multiple DHT peers near the key and returns a channel of all responses. `searchBest` collects every response, discards anything that doesn't parse or doesn't verify (see below), and returns the most-recently-published *valid* claim. A single lying or eclipsed peer can't suppress the real record as long as at least one honest peer among the closest-K responds — the same reasoning IPFS's own `SearchValue` quorum semantics are built for, applied here specifically to defend name resolution rather than left as a generic library feature nobody exercises.

**Signed, self-verifying records.** A `Claim` carries `Name`, `DID`, `PublishedAt`, `ExpiresAt`, and a base64 Ed25519 `Signature` over the canonical JSON of the other four fields. `verifyClaim` extracts the public key directly from the claim's own DID (`identity.PubKeyFromDID`) and checks the signature against it — the same "the key is embedded in the identifier itself" property `did:key` gives Agent Cards (ADR-0002) and the DHT-level Agent Card validator (Chapter 8 of the project's architecture dissertation) gives capability records. A forged claim requires forging a signature, not just writing a value to a key.

**Consent-gated conflict resolution.** `Claim` performs the same quorum read before writing: if any live (`ExpiresAt` in the future), validly-signed claim for the name already exists under a *different* DID, the write is rejected outright. A name cannot be taken from its current holder by racing a write — the current holder's claim must lapse (24-hour TTL, renewed every 20 hours by an actor-scheduled republish job) before anyone else's claim can succeed.

Names are normalized (lowercased, punctuation stripped, words hyphen-joined) and validated against a fixed pattern (`^[a-z][a-z0-9]*(-[a-z][a-z0-9]*)*$`, max 64 characters) before any DHT operation, so `Resolve("Swift Falcon")` and `Resolve("swift-falcon")` are guaranteed to hit the same key.

## Rationale

- **Byzantine-resistant *reads* are a cheaper first step than Byzantine-resistant *writes*.** This is explicitly not full BFT name assignment — the package's own doc comment says as much, and points to anchoring claims in a consensus thread (Raft/Tendermint, ADR-0010) as "the correct next step" for linear-history, equivocation-proof naming. Quorum reads plus signed records give real, immediate protection against a *minority* Sybil/eclipse attacker at the cost of one extra network round-trip, without requiring every name claim to go through full thread consensus.
- **Signature verification at read time, not just at write time, matters.** Because any peer can technically return any bytes it wants under a `SearchValue` query, `searchBest` treats every response as untrusted input and re-verifies it — the same "don't trust the network, verify the artifact" discipline applied at the DHT layer for Agent Cards and thread archive blocks (ADR-0019).
- **This is a real, working, partial answer to a question the project's roadmap still lists as fully open**, and documenting it here means a future engineer building the general Sybil-resistance/reputation model doesn't have to rediscover that `daemon/names` already solved the narrower "can someone steal my agent's name" instance of the problem.

## Consequences

- Name resolution costs more than a single DHT `GetValue` — `SearchValue`'s quorum fan-out means `Resolve` waits on multiple peer responses rather than the first one. This is the deliberate trade for eclipse resistance and is not currently configurable.
- Protection is probabilistic, bounded by "attacker does not control a majority of the closest-K peers to the name's key" — an attacker who *does* achieve that majority can still eclipse a name claim. This is a known, stated limitation, not a gap discovered by this ADR.
- A name's holder can let it lapse (deliberately or by the daemon going offline for more than the TTL and republish window) and lose it to another claimant; there is no reservation or grace-period mechanism beyond the 24-hour TTL / 20-hour republish cadence.
- Future work escalating this to full BFT name assignment (thread-anchored claims) should amend this ADR rather than silently replace the DHT-quorum mechanism, so the project's own documented history of "this is intentionally the cheaper interim answer" isn't lost the way ADR-0003's blob-store reversal was.
