
# Durable SDK Agents and Distributed Threads — Implementation Handoff

**Target file:** `docs/plans/SDK-DURABLE-THREADS-HANDOFF.md`

## 1. Goal and Required Invariants

Build a production-capable libp2p agent network where multiple SDK-controlled agents can share a daemon, discover one another without preconfigured DIDs or PIDs, collaborate through durable threads, delegate real work, subscribe to results, and recover encrypted history after every original agent disconnects.

The completed system must guarantee:

- Agents are independent SDK clients; they are not embedded daemon actors.
- Each agent owns its signing, encryption, and recovery material under its own scoped `HOME`.
- One daemon can concurrently serve multiple isolated SDK agent identities.
- Agent-to-agent references use DIDs and advertised capabilities only. PIDs must never enter discovery, protocol messages, persisted peer records, or tests.
- Daemons provide transport, discovery, durable routing, consensus, encrypted storage, archival replication, and actor lifecycle management.
- Thread payloads are end-to-end encrypted. Daemons and archive providers store ciphertext.
- A recovery capability can reconstruct readable history without an original agent being online.
- A recovery capability grants read access only; writing still requires current thread membership.
- Raft provides crash-fault tolerance. Documentation must not claim Byzantine consensus.
- Forged, malformed, replayed, unauthorized, and cryptographically invalid traffic is rejected before state mutation.
- Hundreds of thousands of dormant threads do not imply hundreds of thousands of goroutines. Dormant thread actors must be snapshotted and passivated.
- Durable operations are idempotent and resumable across SDK, daemon, and network restarts.

## 2. Architecture and Public Interfaces

### 2.1 SDK-owned agent identities

Each SDK agent stores this material below its own `HOME`:

- Ed25519 signing key and DID.
- X25519 encryption key.
- Recovery handles explicitly saved by the application.
- SDK session state and discovery cache.
- Optional local worker checkpoints.

Add an authenticated SDK session protocol:

```protobuf
message AgentIdentity {
  string did = 1;
  bytes signing_public_key = 2;
  bytes encryption_public_key = 3;
}

message BeginAgentSessionRequest {
  AgentIdentity identity = 1;
}

message AgentChallenge {
  string challenge_id = 1;
  bytes nonce = 2;
  int64 expires_at_unix_ms = 3;
}

message CompleteAgentSessionRequest {
  string challenge_id = 1;
  bytes signature = 2;
  AgentCard card = 3;
}

message AgentSession {
  string token = 1;
  string agent_did = 2;
  int64 expires_at_unix_ms = 3;
}
```

Add RPCs:

```protobuf
rpc BeginAgentSession(BeginAgentSessionRequest) returns (AgentChallenge);
rpc CompleteAgentSession(CompleteAgentSessionRequest) returns (AgentSession);
rpc CloseAgentSession(Empty) returns (Empty);
rpc GetAgentIdentity(Empty) returns (AgentIdentity);
rpc GetNodeIdentity(Empty) returns (NodeIdentity);
```

All agent-scoped RPCs require the session token in gRPC authorization metadata. The SDK must attach it automatically. Health and explicitly daemon-administration RPCs remain node-scoped.

Rules:

- A challenge is single-use and expires quickly.
- The signed challenge binds nonce, daemon node ID, agent DID, and expiry.
- Session tokens are opaque, random, short-lived, hashed at rest, and revocable.
- Reconnecting repeats the challenge flow.
- Omitting the token is allowed temporarily only in legacy single-agent mode.
- If multiple local agents exist, an unscoped request fails instead of choosing an identity.
- Every database row belonging to an agent includes `owner_did` or an equivalent namespace key.
- Inbox, outbox, task, card, webhook, subscription, and discovery data must never leak across local agents.

Update Python and TypeScript SDKs with:

- `AgentIdentity.load_or_create(home=...)`
- `A2AClient(..., identity=...)`
- automatic session establishment and renewal
- `client.agent_did` and `client.node_peer_id`
- scoped reconnect and stream resumption
- redaction of session tokens and recovery secrets from logs and exceptions

### 2.2 Actor hierarchy and lifecycle

Use actors inside the daemon for daemon responsibilities, not for external SDK agent business logic.

Required supervision hierarchy:

```text
RootSupervisor
├── NodeActor
├── RegistryActor
├── DeliverySupervisor
│   └── OutboxShardActor[]
├── AgentSessionSupervisor
│   └── AgentSessionActor[]       ephemeral, authenticated SDK connections
├── TaskSupervisor
│   └── TaskShardActor[]
├── ThreadSupervisor
│   └── ThreadShardActor[]
└── ArchiveSupervisor
    └── ArchiveReplicationActor[]
```

Implementation rules:

- Route thread operations by stable hash into a bounded number of shard actors.
- Instantiate an individual thread actor only while that thread is active.
- Default `max_active_thread_actors` to 1,000 per daemon and make it configurable.
- Passivate a thread actor after five minutes without commands, subscribers, consensus work, or pending delivery.
- Snapshot before passivation and after every 1,000 committed entries.
- Rehydrate from the latest versioned snapshot plus subsequent committed log entries.
- A thread has one actor-owned mutation path. RPC, gossip, Raft, recovery, and task-result callbacks submit commands to that actor instead of writing its store directly.
- Mailbox commands carry stable command IDs. Persist the applied-command ID with the resulting state so replay cannot duplicate mutations.
- Supervision restarts actors from durable state using bounded exponential backoff.
- Streams and network callbacks must never block actor mailboxes.
- Actor shutdown drains accepted commands or records them durably for replay.
- Dormant threads consume database rows and blocks, not goroutines.

Version snapshots explicitly:

```go
type ThreadSnapshot struct {
    Version            uint32
    ThreadID           string
    CommittedIndex     uint64
    MembershipEpoch    uint64
    EncryptionEpoch    uint64
    DescriptorHash     []byte
    HeadBlockHash      []byte
    AppliedCommandIDs  []string
    BackendState       []byte
}
```

Provide migrations for every persisted schema and snapshot version. An unknown future snapshot version must fail safely without overwriting it.

### 2.3 Encrypted thread model

Create ADR-0014 as the canonical encryption specification.

For every thread:

- Generate a random 256-bit thread data key.
- Encrypt entry payloads with XChaCha20-Poly1305.
- Sign the immutable entry header and ciphertext using the author’s Ed25519 key.
- Include thread ID, sequence, previous block hash, author DID, entry kind, membership epoch, encryption epoch, and nonce in authenticated data.
- Hash the canonical encrypted block, never a decoded or mutable representation.
- Verify authorization, signature, epoch, previous hash, and AEAD integrity before applying an entry.
- Rotate the data key whenever membership changes.
- Wrap each epoch key separately for every current member’s X25519 public key.
- Wrap each epoch key for the recovery capability using a key derived with HKDF-SHA-256.
- Do not distribute the recovery secret to ordinary members.
- A removed member may retain history it was previously authorized to read but receives no later epoch keys.

Replace a bare recovery ID with a structured bearer capability:

```protobuf
message ThreadRecoveryHandle {
  string thread_id = 1;
  bytes recovery_secret = 2;
  uint32 version = 3;
}

message RecoverThreadRequest {
  ThreadRecoveryHandle handle = 1;
  bool subscribe = 2;
}

message RecoverThreadResponse {
  Thread thread = 1;
  ThreadAccess access = 2; // READ_ONLY until admitted as a member
}
```

`CreateThread` returns a `CreateThreadResponse` containing the thread and recovery handle. SDKs serialize the handle in a versioned, checksummed form suitable for secret storage.

Security rules:

- Never print, log, publish through DHT, or include the recovery secret in telemetry.
- Treat possession of the complete handle as read authority.
- A bare thread ID discovers public metadata and archive providers but cannot decrypt history.
- Recovery never silently grants write membership.
- Compromising a recovery handle compromises all archived epochs; document rotation as creating a new thread and migrating state.
- Protocol parsing uses bounded sizes before allocating or decrypting.

### 2.4 Membership and consensus

Replace normal `membership:add-observer` payload conventions with explicit membership commands and APIs:

```protobuf
enum ThreadMemberRole {
  THREAD_MEMBER_ROLE_UNSPECIFIED = 0;
  THREAD_MEMBER_ROLE_OBSERVER = 1;
  THREAD_MEMBER_ROLE_VOTER = 2;
  THREAD_MEMBER_ROLE_ADMIN = 3;
}

rpc InviteThreadMember(InviteThreadMemberRequest) returns (ThreadMembershipChange);
rpc AcceptThreadInvite(AcceptThreadInviteRequest) returns (Thread);
rpc PromoteThreadMember(PromoteThreadMemberRequest) returns (ThreadMembershipChange);
rpc RemoveThreadMember(RemoveThreadMemberRequest) returns (ThreadMembershipChange);
rpc LeaveThread(LeaveThreadRequest) returns (ThreadMembershipChange);
rpc ListThreadMembers(ThreadID) returns (ThreadMembers);
```

Membership behavior:

1. An admin creates a signed invitation bound to the thread, invitee DID, role, expiry, and nonce.
2. The invitee independently discovers/connects and accepts through its SDK.
3. The new member starts as an observer.
4. It retrieves and verifies the descriptor, committed chain, snapshot, and its encrypted epoch-key envelope.
5. Promotion to voter is permitted only after catch-up reaches the current committed index.
6. Voter additions and removals use Raft `ConfChangeV2` joint consensus.
7. Key rotation commits after the membership change and before new application entries.
8. Removal revokes outstanding invites and subscriptions for that membership epoch.
9. A member may leave; removing the last voter is rejected.
10. Loss of quorum makes a Raft thread read-only until quorum returns.

Consensus claims:

- Raft supports crash/unavailability tolerance with `2f+1` voters for `f` failures.
- Rename ambiguous Byzantine-oriented configuration fields where compatibility permits; otherwise document their CFT meaning.
- Disable or mark the current Tendermint backend experimental unless it passes a separately specified true-BFT conformance suite.
- Production defaults to Raft.
- Cryptographic validation protects against hostile inputs but does not make malicious Raft voters safe.
- Invalid voters must be removable through the normal joint-consensus path once a valid quorum exists.

Upgrade the committed block format to a versioned v2 canonical encoding. Retain a v1 decoder for existing persisted threads, but all new writes use v2. Include the encoding version in hashes and signatures to prevent cross-version ambiguity.

### 2.5 Durable discovery and connection semantics

Agent discovery remains capability-driven:

- SDK agents publish signed `AgentCard` records through their authenticated daemon sessions.
- Cards bind agent DID, encryption key, capabilities, sequence number, expiry, and reachable daemon peer IDs.
- Daemons verify the card’s agent signature before local storage or DHT publication.
- `FindAgents` merges locally attached agents and DHT results through the same interface.
- A local match must not bypass signature, expiry, capability, or authorization checks.
- Discovery results contain DIDs, cards, node peer IDs, and multiaddrs—never process IDs.
- Persist discovered agents by owner DID with expiry and last successful connection metadata.
- Reject stale card sequence numbers and conflicting cards with invalid signatures.
- SDKs may exclude their own DID and previously failed results.
- Connections are established daemon-to-daemon using libp2p; SDK agents address one another by DID.

The test topology may provide bootstrap node multiaddrs. It must not provide target agent DIDs, PIDs, thread membership, or hard-coded cross-agent socket knowledge.

### 2.6 Durable task execution through SDK workers

The daemon does not execute calculator or text-generation business logic. SDK agents register workers and receive durable leased tasks.

Add APIs:

```protobuf
message WorkerSubscription {
  repeated string skills = 1;
  uint64 after_sequence = 2;
}

message TaskLease {
  string task_id = 1;
  string lease_token = 2;
  int64 expires_at_unix_ms = 3;
  uint32 attempt = 4;
}

rpc SubscribeTasks(WorkerSubscription) returns (stream TaskDelivery);
rpc ClaimTask(ClaimTaskRequest) returns (TaskLease);
rpc RenewTaskLease(RenewTaskLeaseRequest) returns (TaskLease);
rpc CompleteTask(CompleteTaskRequest) returns (Task);
rpc FailTask(FailTaskRequest) returns (Task);
rpc SubscribeTaskEvents(SubscribeTaskEventsRequest) returns (stream TaskEvent);
```

Task rules:

- `CreateTask` accepts an idempotency key, target DID, capability, optional thread ID, typed input artifacts, timeout, and maximum attempts.
- Only the target agent can claim a task.
- A claim atomically transitions `SUBMITTED` to `WORKING` and creates a lease.
- Duplicate claims return the existing active lease to the same authenticated worker or fail with a conflict.
- Lease expiry returns the task to a deliverable state until attempts are exhausted.
- Only the lease holder may complete or fail a working task.
- Terminal transitions are immutable and idempotent.
- Cancellation wins only before a terminal result commits.
- Every transition is a signed, ordered task event.
- Task event subscriptions accept a durable sequence cursor and replay persisted events before following live events.
- Completing a threaded task appends an encrypted `task_result` entry to the thread and creates a direct durable notification for the initiator.
- The task becomes `COMPLETED` only after the result is durably stored and, when applicable, committed to the thread.
- Outbox retry must not produce duplicate task events or thread entries.
- Large results use content-addressed artifacts; the task result stores verified hashes and metadata.

SDK worker API:

```python
worker = client.worker(
    skills=["a2a:v1:cap:calculator"],
    concurrency=4,
    lease_seconds=30,
)

@worker.handle("a2a:v1:cap:calculator")
def calculate(task):
    ...
```

Provide equivalent TypeScript APIs. The helper manages subscription cursors, claims, lease renewal, graceful shutdown, idempotent completion, and reconnect. Applications remain responsible for capability-specific execution.

### 2.7 Archival replication and recovery

Separate active voting replicas from archival storage providers.

- Voting replicas participate in Raft.
- Observer replicas follow the committed thread without voting.
- Archive providers store encrypted snapshots, blocks, descriptors, membership records, key envelopes, and signed head records.
- Default archival replication factor is three provider nodes, configurable per thread.
- In a two-daemon test, explicitly request two archive providers.
- A committed write is considered archive-durable only after the configured archive acknowledgement policy is met.
- Publish signed provider records and head references through the DHT with expiry and periodic refresh.
- Archive providers verify block hashes and signatures without decrypting payloads.
- Recovery queries multiple providers, validates every candidate chain, chooses the highest valid committed head, and fetches missing blocks through content addressing.
- Recovery imports into a staging namespace and atomically publishes the thread locally only after complete validation.
- Interrupted recovery resumes from verified blocks.
- Conflicting or corrupt providers are ignored and recorded in diagnostics.
- Archive garbage collection retains data while an unexpired provider contract, local membership, recovery pin, or configured retention policy exists.

Recovery scenarios:

- Original SDK agents offline, daemons online.
- Original daemon restarted from its disk.
- Fresh daemon discovers remote archive providers.
- Fresh SDK agent possesses only its identity plus the recovery handle.
- Bare thread ID without the capability returns metadata but no plaintext.
- If every physical holder and backup is destroyed, recovery is impossible; documentation must state this explicitly.

### 2.8 Outbox, storage, and operational hardening

Complete durable outbox administration:

- idempotency key and payload hash on every outbound operation
- lease owner and lease expiry
- bounded exponential backoff with jitter
- maximum attempts and dead-letter state
- explicit retry, cancel, inspect, and purge APIs
- retention-based garbage collection for acknowledged and terminal records
- per-owner namespaces
- queue depth, oldest age, attempt count, delivery latency, and dead-letter metrics
- clean restart recovery for records left in an in-flight state

Storage rules:

- Enable SQLite WAL, busy timeout, foreign keys, and bounded transactions.
- Serialize migrations and back up before destructive version changes.
- Use monotonic per-stream sequence numbers for resumable subscriptions.
- Never acknowledge network or SDK delivery before the corresponding durable commit.
- Validate IDs, lengths, enum values, and ownership at RPC boundaries.
- Add context cancellation and bounded timeouts to DHT, Bitswap, gossip, and peer operations.
- Enforce maximum message, block, artifact, card, and snapshot sizes.

Repository cleanup:

- Establish `gen/a2a/v1` as the only committed Go protobuf output.
- Remove duplicate generated import trees after confirming no consumers.
- Remove committed binaries, daemon homes, caches, sockets, databases, and runtime artifacts.
- Add ignore rules for all generated runtime state.
- Make protobuf generation deterministic and expose one documented generation command.
- Do not overwrite unrelated user changes while cleaning the existing dirty worktree.

## 3. Delivery Order and Agent Ownership

### Phase 0 — Baseline and protocol freeze

**Owner: Integration agent**

- Record the current test/build baseline and dirty-worktree inventory.
- Create the implementation branch and handoff checklist.
- Freeze canonical protobuf package paths and generated-code procedure.
- Allocate exclusive ownership of `proto/a2a.proto` and generated bindings to the protocol agent.
- Require each workstream to rebase on the protocol commit instead of independently editing generated files.

### Phase 1 — Protocol and SDK identity foundation

**Protocol agent**

- Implement v2 protobuf messages, membership APIs, session APIs, worker APIs, recovery handles, sequence cursors, and compatibility annotations.
- Generate canonical Go, Python, and TypeScript bindings.
- Add v1 decoding compatibility and clear deprecation comments.

**SDK identity agent**

- Implement SDK-owned signing and X25519 identities.
- Implement challenge authentication, automatic session metadata, renewal, secret redaction, and per-HOME persistence.
- Update examples so SDK agents, not daemons, own agent identity.

Phase exit criteria:

- Two SDK identities with separate homes can authenticate concurrently to one daemon.
- Their inboxes, cards, tasks, and subscriptions are isolated.
- Restarting an SDK preserves its DID; moving to another daemon does not change it.

### Phase 2 — Multi-tenancy and actor durability

**Daemon runtime agent**

- Refactor singleton identity dependencies out of RPC, registry, inbox, outbox, delivery, task, webhook, and thread entry paths.
- Introduce authenticated request context carrying owner DID.
- Implement bounded actor shards, activation, snapshots, passivation, supervision, and recovery.
- Remove direct state mutations that bypass the owning actor.
- Add actor and queue diagnostics.

**Storage agent**

- Add owner namespaces, cursor tables, applied-command IDs, versioned snapshots, leases, dead letters, archival pins, and migrations.
- Implement crash-safe transaction boundaries and restart recovery.
- Add legacy data migration with backups and idempotent reruns.

Phase exit criteria:

- A daemon hosts multiple authenticated agents without data leakage.
- Restarting during accepted commands produces no loss or duplication.
- Creating 100,000 dormant threads does not create 100,000 live actors or goroutines.

### Phase 3 — Thread security, membership, and consensus

**Thread security agent**

- Implement encrypted v2 thread blocks, canonical signing, member key envelopes, recovery envelopes, epoch rotation, and SDK encryption/decryption.
- Ensure daemon thread logic operates on authenticated ciphertext.
- Add tamper, replay, stale-epoch, wrong-key, and secret-redaction protections.

**Consensus agent**

- Implement observer catch-up and Raft `ConfChangeV2` joint-consensus promotion/removal.
- Enforce role authorization and quorum behavior.
- Correct recovery of consensus state from snapshots and logs.
- Remove unsupported Byzantine claims and feature-gate experimental consensus backends.

Phase exit criteria:

- A newly invited observer catches up, is promoted, and participates after joint consensus.
- A removed member cannot decrypt later epochs or submit accepted entries.
- Quorum loss makes the thread read-only and recovery restores progress without divergent commits.

### Phase 4 — Discovery, tasks, and archives

**Discovery/delivery agent**

- Implement signed multi-agent cards, local-plus-DHT discovery, durable routing, outbox leases, dead letters, retry controls, and connection diagnostics.
- Ensure local co-resident agents follow the same discovery contract as remote agents.

**Task agent**

- Implement durable task delivery, claims, leases, retries, terminal-state rules, event cursors, threaded results, and Python/TypeScript worker helpers.
- Replace shell inbox polling as the primary execution contract while retaining shell examples using SDK/CLI commands.

**Archive/recovery agent**

- Implement archive provider discovery, encrypted replication, acknowledgement policy, resumable fetching, staging validation, recovery handles, and retention.
- Test recovery through both local persisted state and remote archive providers.

Phase exit criteria:

- A text agent finds a calculator solely through capability discovery.
- The calculator claims and computes the task through its SDK.
- The signed result is committed to the thread and delivered to a resumable subscriber.
- A fresh SDK agent can read the encrypted thread using the recovery handle after all original SDK agents exit.

### Phase 5 — Integration, compatibility, and documentation

**Integration agent**

- Resolve cross-workstream migrations and interface changes.
- Run formatters, generators, complete automated suites, race checks, and manual scripts.
- Remove deprecated paths only after SDK examples and compatibility tests pass.
- Confirm repository status contains only intended source and documentation changes.

Agents must not concurrently edit shared protocol/generated files. Every phase must land with passing focused tests before the next dependent phase starts.

## 4. Verification and Acceptance Tests

### 4.1 Required manual two-daemon scenario

Provide `e2e/manual-thread/` scripts using ordinary Bash orchestration and SDK agent programs:

```text
node-a daemon
├── text-agent SDK process, HOME=node-a/text-agent
└── observer-agent SDK process, HOME=node-a/observer-agent

node-b daemon
└── calculator-agent SDK process, HOME=node-b/calculator-agent
```

The scenario must:

1. Start two daemons with isolated daemon homes and deterministic test node keys.
2. Start three SDK agents with independent homes and deterministic fixture-only agent keys.
3. Give agents only their local daemon address, bootstrap peer address, and desired capability.
4. Publish signed agent cards.
5. Have each agent discover relevant peers independently and persist discovered DIDs.
6. Assert scripts never exchange or inspect application PIDs for protocol coordination.
7. Have the text agent create a thread and save its recovery handle.
8. Add the observer and calculator through invite, catch-up, and promotion flows.
9. Ask the text agent: `add 2 + 2.`
10. Have it discover the calculator capability, delegate a threaded task, and subscribe from a stored cursor.
11. Have the calculator SDK worker claim the task, compute `4`, and complete it.
12. Verify the text agent receives the computed result through both task events and the committed thread entry.
13. Restart one SDK agent during delivery and prove cursor-based resumption produces exactly one logical result.
14. Restart one daemon and prove outbox, actors, membership, subscriptions, and thread state resume.
15. Remove/leave all three original SDK agents while archive daemons retain ciphertext.
16. Start a new SDK identity with only a daemon address and the stored recovery handle.
17. Recover and decrypt the complete ordered thread as read-only.
18. Prove the same recovery fails with a bare thread ID or modified secret.
19. Print DIDs, thread ID, task ID, and final answer, while redacting keys, tokens, and the recovery secret.

Scripts must include `setup.sh`, `start.sh`, `run.sh`, `stop.sh`, and a README containing equivalent manual commands. Cleanup must target only explicitly created fixture directories.

### 4.2 Automated test matrix

Run and require:

- `go test ./...`
- `go test -race ./...`
- Python SDK unit and integration tests
- TypeScript SDK unit and integration tests
- protobuf regeneration followed by a clean diff check
- manual Bash scenario
- repeated E2E execution to detect nondeterministic delivery failures

Coverage scenarios:

- session challenge replay, expiry, wrong signature, and cross-agent token use
- two local agents sharing a daemon without inbox/task/thread leakage
- DHT discovery with stale, forged, duplicate, and conflicting cards
- concurrent thread appends and subscriber reconnect from every cursor boundary
- actor crash before commit, after commit, during snapshot, and during passivation
- snapshot corruption and unsupported snapshot versions
- duplicate commands and duplicate network deliveries
- observer catch-up, voter promotion, voter removal, voluntary leave, and quorum loss
- membership key rotation and removed-member access denial
- encrypted block tampering, wrong associated data, replay, and oversized ciphertext
- task duplicate claim, lease expiry, worker crash, retry exhaustion, cancellation race, and duplicate completion
- result commit failure followed by retry without duplicate thread entries
- outbox restart recovery, dead-letter transition, manual retry, cancellation, and garbage collection
- archive provider loss, corrupt provider, conflicting stale head, partial download, and resumable recovery
- recovery with correct capability, wrong capability, bare ID, and already imported blocks
- 100,000 dormant thread records with active actor count bounded by configuration
- libp2p disconnect/reconnect, delayed discovery, reordered gossip, duplicate pubsub messages, and unavailable peers

### 4.3 Adversarial/CFT tests

The production claim is signed crash-fault-tolerant consensus, not Byzantine consensus. Test:

- forged author and proposer signatures
- unauthorized membership proposals
- altered ciphertext and block hashes
- replayed signed commands
- equivocation attempts from non-voters
- stale membership/encryption epochs
- invalid Raft transport sender identity
- malformed consensus payloads and allocation bombs
- compromised or malicious archive providers returning corrupt/stale data
- a crashed or unavailable minority of Raft voters

Do not add a test claiming continued safety with malicious quorum voters. Document that requirement as needing a separate BFT architecture.

## 5. Documentation and Definition of Done

Update:

- ADR-0001: daemon is a multi-tenant SDK transport/runtime
- ADR-0002: SDK-owned Ed25519 and X25519 keys
- ADR-0005: per-agent durable namespaces and queue leases
- ADR-0007: claimed/leased task lifecycle and threaded results
- ADR-0008: complete daemon supervision hierarchy
- ADR-0010: Raft CFT guarantees and unsupported BFT claims
- ADR-0011: durable cursor, retry, and dead-letter semantics
- ADR-0014: thread encryption and capability recovery
- ADR-0015: sharded, snapshottable, passivated thread actors
- ADR-0016: membership, archival replicas, recovery, and SDK worker delegation
- architecture guide, root README, SDK READMEs, manual E2E README, and operational troubleshooting guide

Each ADR must distinguish:

- accepted behavior
- security and failure assumptions
- persistence boundaries
- compatibility/migration behavior
- rejected alternatives
- operational limits

The implementation is complete only when:

- All required suites and the repeatable manual scenario pass.
- Three independent SDK agents operate across two daemons with two agents sharing one daemon.
- No agent begins with another agent’s DID, PID, or socket.
- Discovery, connection, membership, delegation, calculation, result subscription, restart, leave, and recovery are demonstrated.
- Thread actors are durable, resumable, snapshottable, and bounded in memory.
- Encrypted history is unreadable to daemons and to holders of only a bare thread ID.
- Recovery works with the saved capability while all original SDK agents are offline.
- CFT and Byzantine guarantees are described accurately.
- Generated code has one canonical location.
- No runtime binaries, caches, keys, databases, sockets, or agent homes are committed.
- There are no known deferred correctness items hidden behind TODOs; explicitly experimental features are disabled by default and documented.

## Assumptions and Defaults

- Agents connect through Python or TypeScript SDKs over local gRPC to a daemon.
- Agent private keys remain SDK-owned.
- The recovery handle is a bearer secret and must be stored like a private key.
- New recovery sessions are read-only until admitted by current membership.
- Raft is the only production consensus backend.
- Default voters use `2f+1`; Byzantine voter tolerance is out of scope.
- Default archive replication factor is three, reduced explicitly for smaller test topologies.
- Default active thread actor limit is 1,000.
- Default passivation timeout is five minutes.
- Default snapshot interval is 1,000 committed entries.
- Daemons may know bootstrap node addresses, but agents discover one another exclusively through signed capability records.
- Recovery requires at least one surviving persisted or remote archive provider; no protocol can recover data after every physical copy is destroyed.
