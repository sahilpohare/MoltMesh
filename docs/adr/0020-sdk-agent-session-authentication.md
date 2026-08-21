# ADR-0020: SDK Agent Session Authentication, Separate from Daemon Identity

**Status**: Accepted and implemented
**Date**: 2026-08-21

## Context

A single daemon process can serve more than one SDK-side agent identity — for example, a worker pool where several logical agents share one running `moltmesh-daemon`. Several RPCs (`ClaimTask`, `RenewTaskLease` — see ADR-0022) need to know *which* SDK agent is calling, not just that some authenticated client is calling.

The daemon already has a cryptographic identity: its own `did:key`, generated once from an Ed25519 keypair (ADR-0002) and reused as its libp2p peer ID. It would be tempting to let that same key stand in for whichever SDK agent is currently connected. That would be a mistake. The daemon's libp2p identity is a *network* identity — it identifies the peer on the mesh, is embedded in every Agent Card the daemon publishes, and is long-lived by design. An SDK agent's signing identity needs to be independently issued, short-lived, and revocable without touching the daemon's own network presence. Collapsing the two would mean a compromised SDK process could act with the daemon's full peer identity, and would mean every SDK agent sharing a daemon would be indistinguishable from every other one on the wire.

This need was implemented directly in `daemon/session/session.go` without ever being written up. The package's own doc comment states the principle plainly: "a daemon's libp2p identity must never become an SDK agent's signing identity." This ADR is the write-up that should have accompanied that code.

## Decision

Implement a short-lived, challenge/response session layer, entirely independent of `daemon/identity`:

1. **`Begin(agent)`** — the SDK agent supplies its own `did:key` and Ed25519 signing public key (plus an X25519 encryption public key, used elsewhere for thread payload encryption per ADR-0014). The daemon validates that the supplied public key actually derives the claimed DID, then issues a single-use, 32-byte random nonce as an `AgentChallenge`, valid for `DefaultChallengeTTL` (60 seconds).
2. **`Complete(req)`** — the SDK agent signs a canonical payload (`"moltmesh-agent-session-v1\x00" + nodeID + "\x00" + challengeID + "\x00" + nonce + "\x00" + expiry + "\x00" + did`) with its Ed25519 private key and returns the signature. The daemon verifies the signature against the public key it validated in step 1, consumes the challenge (single-use even on failure), and issues an opaque 32-byte random session token, valid for `DefaultSessionTTL` (15 minutes).
3. Every subsequent authenticated RPC presents the token; the daemon looks it up, confirms it hasn't expired, and resolves it to the calling agent's DID (`Authenticate`) or full identity material (`Identity`).

Tokens are never stored in plaintext server-side — only their SHA-256 hash is retained in the in-memory `sessions` map, so a heap dump or log line can't leak a usable credential after the fact. Challenges and sessions are pruned lazily on every access (`pruneLocked`), so there is no background sweep to forget to run.

## Rationale

- **Identity separation is a security boundary, not a convenience.** The daemon's libp2p/DID identity authenticates the *daemon* to the rest of the P2P network (Chapter 4/8 of the project's architecture dissertation). An SDK agent's session identity authenticates *that specific agent process* to *its own daemon*. Neither should be able to impersonate the other.
- **Challenge/response, not bearer-key-on-the-wire.** The SDK never sends its private key or a long-lived credential to the daemon; it proves possession of the private key once, per session, over a nonce that can't be replayed (single-use, TTL-bounded).
- **Hashed-at-rest tokens.** Session tokens are capability-bearing strings; storing only their hash means a read of daemon memory doesn't hand out working credentials, mirroring the same "never persist the secret itself" discipline used for thread recovery secrets (ADR-0017).
- **`MultipleAgents()` exists because this daemon-serves-many-agents case is real.** When more than one distinct SDK identity is attached simultaneously, legacy unscoped calls (RPCs written before session scoping existed) become ambiguous about which agent they apply to, and must be rejected rather than silently guessing.

## Consequences

- SDK clients must complete the challenge/response handshake before calling any session-scoped RPC (currently `ClaimTask`/`RenewTaskLease`, ADR-0022); other RPCs remain daemon-identity-scoped.
- A 15-minute session TTL means long-running SDK workers must re-authenticate periodically; no refresh-token mechanism exists yet — a worker's own retry/backoff logic on `Complete` failure is expected to re-run `Begin`.
- Session state is in-memory only and does not survive a daemon restart, by design — this is intentionally not a durability boundary the way the outbox (ADR-0005) or thread state (ADR-0010) is; a restarted daemon simply requires every SDK agent to re-authenticate.
- This is a security-relevant decision that existed only as a package doc comment until this ADR. Future changes to session lifetime, revocation, or the signed payload format should amend this document rather than repeat the previous silent-drift pattern this project's own architecture audit (`docs/DISSERTATION.md`, Chapter 12) catalogues elsewhere.
