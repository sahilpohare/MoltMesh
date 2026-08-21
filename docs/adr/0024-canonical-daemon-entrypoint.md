# ADR-0024: `cmd/moltmesh` as the Canonical Daemon Entrypoint

**Status**: Accepted
**Date**: 2026-08-21

## Context

`cmd/daemon/main.go` (1,047 lines) and `cmd/moltmesh/daemon.go` (537 lines, part of the larger `cmd/moltmesh` CLI) both construct and start a full `moltmesh-daemon` process: identity load, libp2p host, registry, inbox/outbox, tasks, gossip, actor hierarchy, gRPC server on a Unix socket and/or TCP listener. A diff between the two, after stripping comments, is over 500 lines — this is not a thin wrapper calling into a shared `daemon.Run(...)` function twice; it is two separately-maintained copies of the daemon's own bootstrap sequence. ADR-0001 established the language-agnostic daemon-plus-gRPC shape this bootstrap logic implements, but never anticipated — and no later ADR ever recorded — that the bootstrap sequence itself would end up written twice.

The Git history explains *how* this happened, even though no ADR recorded *that* it happened. The project's early commits build and ship `cmd/daemon` as the daemon binary. Later commits — "Rename binary to moltmesh and add cross-build targets," "Support multiple binaries in Makefile," "Output binaries to bin and add build-all" — introduce `cmd/moltmesh` as a unified CLI (identity, diagnostics, pub/sub, webhooks, networks, name claims, and daemon management all as subcommands of one binary) and rebrand the project's public name from `p2p-a2a`/`daemon` to MoltMesh/OpenMolt Network. `cmd/moltmesh`'s `daemon.go` needed the same bootstrap logic `cmd/daemon/main.go` already had, and rather than factoring it into a shared package at rename time, it was copied and adapted.

## Decision

`cmd/moltmesh` is the canonical, supported daemon entrypoint going forward. `cmd/daemon` is retained only for backward compatibility with existing scripts, documentation, and muscle memory built around the pre-rebrand binary name, and should not receive new daemon-bootstrap functionality independently of `cmd/moltmesh`.

Concretely:

- New daemon-startup features (new flags, new subsystems wired into the bootstrap sequence, changes to listener setup) are implemented in `cmd/moltmesh/daemon.go` first.
- `cmd/daemon/main.go` is treated as a compatibility shim and should be brought to parity opportunistically, not left to silently drift further out of sync with `cmd/moltmesh`'s bootstrap logic than it already has.
- The `Makefile`'s `BINARIES := moltmesh daemon tui` list continues to build both, so existing consumers of the `daemon` binary name are not broken by this decision — this ADR is about which copy of the *logic* is authoritative, not about removing the `daemon` binary outright.

## Rationale

- **A rebrand is exactly the moment shared logic should get factored out, and exactly the moment teams are least likely to do it**, because the immediate goal is "ship the new binary name," not "refactor the bootstrap path." Naming the canonical copy now, rather than leaving both as equally-authoritative, stops the drift from getting worse while a proper factor-out (see Consequences) is scheduled.
- **`cmd/moltmesh` is the actively-developed surface.** It is where the CLI's other subcommands (`identity`, `health`, `ping`, `peers`, `publish`, `network *`, `name *`, `format *`) already live, it is the name the project's own README, `moltbook.toml` documentation, and public branding all point to, and it is the binary the `tui` subcommand (ADR-0023) is wired into. Naming `cmd/daemon` canonical instead would mean every new daemon-facing feature has to be built against the *less*-actively-maintained copy first.
- **This mirrors the same decision this project already made once, implicitly, for the TUI** (ADR-0023): rather than pretend the fork doesn't exist, name which copy is authoritative and schedule the real fix.

## Consequences

- Anyone currently scripting against the `daemon` binary continues to work unmodified; this ADR changes maintenance priority, not the build output or CLI surface of either binary today.
- Until the bootstrap logic is actually unified, the same >500-line diff this ADR documents will keep needing manual attention: a bug fixed in `cmd/moltmesh/daemon.go`'s bootstrap sequence is not automatically fixed in `cmd/daemon/main.go`, and vice versa. This ADR does not resolve that; it names which side to trust when the two disagree.
- **Follow-up work**: extract the shared bootstrap sequence (identity load → libp2p host → registry/inbox/outbox/tasks/gossip wiring → actor hierarchy → gRPC listener setup) into a single function in a shared package (e.g. `daemon/bootstrap` or reusing `daemon/node` plus a new orchestration package), with both `cmd/daemon/main.go` and `cmd/moltmesh/daemon.go` reduced to flag-parsing plus a call into it. At that point `cmd/daemon` becomes a true thin wrapper and this ADR's "which copy is canonical" question stops mattering, because there will only be one copy.
- This decision, and the diff it responds to, is the second instance in this project's history of two forked implementations of the same feature going undocumented until an architecture audit surfaced them (the first being the TUI, ADR-0023). Both should be read as the same underlying lesson: a rename, rebrand, or new entrypoint is the moment to factor out shared logic, not copy it, and if copying happens anyway under time pressure, it should be recorded as a decision — with a named canonical side and a stated follow-up — rather than left for the next engineer to discover by diffing two files that look suspiciously alike.
