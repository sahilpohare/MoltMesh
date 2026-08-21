# ADR-0022: Lease-Based Task Claiming and Bounded Retry for SDK Workers

**Status**: Accepted and implemented
**Date**: 2026-08-21

## Context

ADR-0007 defines the A2A task lifecycle (`submitted → working → completed | failed | cancelled`) and states plainly that the *assignee* daemon is sole authority over task state — a push model where the initiator names an assignee DID and that assignee's daemon owns every subsequent transition. That model says nothing about what happens *inside* the assignee's own process once a task lands there: an SDK-side worker still has to pick the task up, do the work, and — if it crashes mid-task — the daemon needs a way to know the work didn't finish and let it be retried, instead of a task silently sitting in `WORKING` forever with no process actually working on it.

`daemon/tasks/tasks.go` implements exactly this, via `Claim` and `RenewLease`, gated behind the SDK session authentication layer (ADR-0020): `ClaimTask`/`RenewTaskLease` resolve the caller's DID via `s.agentDID(ctx)` before touching task state. This is not a second, competing way to *assign* a task — `Claim` explicitly rejects any caller whose DID doesn't match the task's already-recorded `assignee` (`ErrLeaseConflict`, "worker is not task assignee"). It is a crash-recovery and retry layer for the single assignee ADR-0007 already names, and it was built without ever being connected back to ADR-0007 in writing, leaving three schema columns (`lease_token`, `lease_owner`, `lease_expires_at`) and a retry vocabulary (`attempt`, `max_attempts`, `deadline_at`) undocumented in any ADR.

## Decision

Layer a lease on top of the `WORKING` state, scoped to the task's existing assignee:

- **`Claim(id, worker, lease)`** — the assignee's SDK process calls this to actually start work. If the task is `SUBMITTED`, or `WORKING` with an *expired* lease, the daemon atomically transitions it to `WORKING`, issues a fresh random 32-byte lease token, records the caller as `lease_owner`, sets `lease_expires_at = now + lease` (default 30s), and increments `attempt`. A repeat call by the same worker while its own lease is still live is idempotent — it returns the existing lease rather than erroring or double-counting an attempt.
- **`RenewLease(id, worker, token, lease)`** — a long-running worker extends its own lease before it expires, proving it's still alive without needing to finish the task yet.
- **Lease expiry is the retry trigger.** If a worker crashes or hangs without renewing, `lease_expires_at` passes; the *next* `Claim` call for that task (from the same assignee — a supervisor process restarting a crashed worker, for instance) is accepted again, `attempt` increments again, and work resumes.
- **`max_attempts`** (default 3, overridable via `SetMaxAttempts`) bounds the retries: once exhausted, the task is force-transitioned to `FAILED` with `"maximum task attempts exhausted"` and further claims return `ErrAttemptsExhausted`.
- **`deadline_at`** (optional, via `SetTimeout`) is an independent hard ceiling: if the deadline has passed, `Claim` fails the task outright (`"task deadline exceeded"`) regardless of remaining attempts.

All of this is a single atomic SQL transaction per call (`tx.Begin()` / `tx.Commit()`), so a crash between "read current lease state" and "write new lease state" can't leave the row in an inconsistent state.

## Rationale

- **This is a reliability layer, not a distribution mechanism.** The task-distribution decision — who gets assigned what — is entirely ADR-0007's; this ADR only covers what a single already-assigned worker does to claim, hold, renew, and retry its own work. Framing it any other way (as this project's own architecture dissertation initially did, describing it as "a competitive pull model where a pool of workers race to claim a task") overstates what the code does — `assignee != worker` is rejected outright, so there is no pool of competing workers here, only a single named assignee's own crash-recovery path.
- **Bounded retry needs to live somewhere, and the task row is the natural place.** An SDK worker crashing mid-task is a normal failure mode, not an edge case, for any long-running agentic task (a model call that hangs, a container that OOMs). Without a lease, a crashed worker leaves a task stuck in `WORKING` with nothing driving it forward; ADR-0007's FSM has no timeout concept of its own.
- **Gating claims behind session authentication (ADR-0020) is what makes "worker is not task assignee" enforceable at all.** Without a verified caller DID, `Claim` would have no way to check the one invariant that keeps this a reliability mechanism instead of an open free-for-all.

## Consequences

- The `tasks` table now carries lease/retry columns (`lease_token`, `lease_owner`, `lease_expires_at`, `attempt`, `max_attempts`, `deadline_at`) added via `ALTER TABLE` after the table's original creation; any future schema migration tooling needs to account for these as part of the canonical task schema, not as an unexplained addition.
- A worker that never calls `Claim` at all (i.e. relies purely on `UpdateStatus` the way ADR-0007 originally describes) does not get retry-on-crash behavior — leasing is opt-in per assignee, not mandatory for every task.
- `max_attempts` and `deadline_at` interact: a task can fail from either exhausting attempts or exceeding its deadline, whichever comes first, and callers should check the returned error (`ErrAttemptsExhausted` vs. the deadline-exceeded failure) rather than assuming one implies the other.
- This ADR should be read alongside ADR-0007 and ADR-0016 (thread-anchored task results) as the three-part answer to "how does work actually move between agents in this system" — assignment (0007), thread-scoped result observation (0016), and single-assignee crash recovery (this ADR) — closing the documentation gap the project's own architecture dissertation (Chapter 12) flagged as three coexisting, only-partially-documented mechanisms.
