/**
 * MoltMesh gRPC client.
 *
 * Low-level: createStub() + unary() / serverStream()
 * High-level: A2AClient class
 *
 * Usage:
 *   import { A2AClient } from "./client.js";
 *   const client = new A2AClient();
 *   const me = await client.getIdentity();
 *   const task = await client.createTask("did:key:z6Mk...", "a2a:v1:cap:text-generation");
 *   await client.waitTask(task.id);
 */

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { fileURLToPath } from "url";
import { dirname, resolve } from "path";
import { createHash, randomBytes, randomUUID } from "crypto";
import { AgentIdentity } from "./identity.js";

export { AgentIdentity } from "./identity.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const PROTO_PATH = resolve(__dirname, "../../../../proto/a2a.proto");

let protoDescriptor: grpc.GrpcObject | null = null;

function loadProto(): grpc.GrpcObject {
  if (protoDescriptor) return protoDescriptor;
  const packageDef = protoLoader.loadSync(PROTO_PATH, {
    keepCase: false,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  protoDescriptor = grpc.loadPackageDefinition(packageDef);
  return protoDescriptor;
}

export type GrpcClient = grpc.Client & Record<string, (...args: unknown[]) => grpc.ClientReadableStream<unknown> | void>;

const SESSION = Symbol("moltmeshSession");
type SessionState = { ready: Promise<void>; metadata?: grpc.Metadata; expiresAt?: number; refreshing?: boolean; refresh?: () => Promise<void> };
type SessionStub = GrpcClient & { [SESSION]?: SessionState };

export function createStub(addr: string): GrpcClient {
  const descriptor = loadProto();
  const A2ANode = ((descriptor as Record<string, unknown>)["a2a"] as Record<string, unknown>)["v1"] as Record<string, unknown>;
  return new (A2ANode["A2ANode"] as grpc.ServiceClientConstructor)(addr, grpc.credentials.createInsecure()) as GrpcClient;
}

export function defaultAddr(): string {
  const env = process.env["A2A_GRPC_ADDR"];
  if (env) return env;
  const home = process.env["HOME"] ?? "/root";
  return `unix://${home}/.moltmesh/a2a.sock`;
}

/** Wrap a gRPC unary call in a Promise. */
export function unary<Req, Res>(stub: GrpcClient, method: string, req: Req): Promise<Res> {
  return sessionReady(stub).then(() => new Promise((resolve, reject) => {
    const callback = (err: Error | null, res: Res) => { if (err) reject(err); else resolve(res); };
    const metadata = (stub as SessionStub)[SESSION]?.metadata;
    // Must be called as stub[method](...), not hoisted into a local first —
    // grpc-js's generated methods read internal state off `this`, and a bare
    // function reference (`const invoke = stub[method]`) loses that binding.
    if (metadata) (stub[method] as (req: Req, m: grpc.Metadata, cb: (err: Error | null, res: Res) => void) => void)(req, metadata, callback);
    else (stub[method] as (req: Req, cb: (err: Error | null, res: Res) => void) => void)(req, callback);
  }));
}

/** Collect all items from a server-streaming call into an array. */
export function serverStream<Req, Res>(stub: GrpcClient, method: string, req: Req): Promise<Res[]> {
  return sessionReady(stub).then(() => new Promise((resolve, reject) => {
    const items: Res[] = [];
    const metadata = (stub as SessionStub)[SESSION]?.metadata;
    const call = (metadata ? stub[method](req, metadata) : stub[method](req)) as grpc.ClientReadableStream<Res>;
    call.on("data", (item: Res) => items.push(item));
    call.on("end", () => resolve(items));
    call.on("error", reject);
  }));
}

async function sessionReady(stub: GrpcClient): Promise<void> {
  const state = (stub as SessionStub)[SESSION];
  if (!state) return;
  await state.ready;
  if (state.refresh && !state.refreshing && (state.expiresAt ?? Infinity) <= Date.now() + 30_000) {
    state.refreshing = true;
    state.ready = state.refresh().finally(() => { state.refreshing = false; });
  }
  await state.ready;
}

async function* sessionStream<T>(stub: GrpcClient, method: string, request: Obj): AsyncIterable<T> {
  await sessionReady(stub);
  const metadata = (stub as SessionStub)[SESSION]?.metadata;
  const call = (metadata ? stub[method](request, metadata) : stub[method](request)) as grpc.ClientReadableStream<T>;
  yield* toAsyncIterable(call);
}

function sessionPayload(nodeID: string, challengeID: string, nonce: Uint8Array, expiresAtUnixMs: string, did: string): Uint8Array {
  return Buffer.from(`moltmesh-agent-session-v1\0${nodeID}\0${challengeID}\0${Buffer.from(nonce).toString("base64url")}\0${expiresAtUnixMs}\0${did}`);
}

// ── types ─────────────────────────────────────────────────────────────────────

export type Obj = Record<string, unknown>;

export interface Identity { did: string; publicKey: string; multiaddrs: string[] }
export interface Skill { id: string; name?: string; description?: string; tags?: string[] }
export interface AgentCard {
  did: string;
  name: string;
  description?: string;
  skills?: Skill[];
  multiaddrs?: string[];
  publicKey?: string;
  publishedAt?: string;
  expiresAt?: string;
  metadata?: Record<string, string>;
  encryptionPublicKey?: Uint8Array;
  nodePeerId?: string;
  sequence?: string;
}
export interface Task { id: string; status: string; skill: string; assignee: string; error?: string; outputArtifacts?: Artifact[] }
export interface Artifact { cid: string; data: Uint8Array; mimeType: string; filename: string; size: string }
export interface Thread { id: string; creatorDid: string; replicaDids: string[]; n: number; f: number }
export interface ThreadEntry { height: string; index: number; entry: { authorDid: string; payload: Uint8Array; kind: string }; blockHash: string }
export interface HealthInfo { version: string; did: string; peerCount: number; uptimeSecs: number }
export interface PeerInfo { peerId: string; multiaddrs: string[]; did: string }
export interface PingResult { did: string; latencyMs: number; reachable: boolean; error: string }
export interface TopicMessage { topic: string; payload: Uint8Array; emittedAt: string }
export interface NetworkInfo { id: string; name: string; creatorDid: string; createdAt: string }
export interface NetworkMember { did: string; joinedAt: string }
export interface BroadcastMessage { networkId: string; payload: Uint8Array; emittedAt: string }
export type CapabilityId = string;
export type CapabilityTag = string;

// ── A2AClient ─────────────────────────────────────────────────────────────────

/**
 * High-level async client for the MoltMesh daemon.
 *
 * All methods return Promises. Streaming methods return AsyncIterable.
 */
export class A2AClient {
  private stub: GrpcClient;
  private bootstrapStub: GrpcClient;
  private readonly identity?: AgentIdentity;
  private nodePeerId = "";

  constructor(addr?: string, opts: { identity?: AgentIdentity } = {}) {
    const target = addr ?? defaultAddr();
    this.stub = createStub(target);
    this.bootstrapStub = opts.identity ? createStub(target) : this.stub;
    this.identity = opts.identity;
    if (opts.identity) {
      const state: SessionState = { ready: Promise.resolve() };
      state.refresh = () => this.establishSession(opts.identity!, state);
      state.refreshing = true;
      state.ready = state.refresh().finally(() => { state.refreshing = false; });
      (this.stub as SessionStub)[SESSION] = state;
    }
  }

  close(): void {
    this.stub.close();
    if (this.bootstrapStub !== this.stub) this.bootstrapStub.close();
  }

  private async establishSession(identity: AgentIdentity, state: SessionState): Promise<void> {
    const node = await unary<Obj, Obj>(this.bootstrapStub, "getNodeIdentity", {});
    this.nodePeerId = String(node["peerId"] ?? "");
    const challenge = await unary<Obj, Obj>(this.bootstrapStub, "beginAgentSession", {
      identity: { did: identity.did, signingPublicKey: identity.signingPublicKey, encryptionPublicKey: identity.encryptionPublicKey },
    });
    const expires = String(challenge["expiresAtUnixMs"] ?? "");
    const nonce = challenge["nonce"] as Uint8Array;
    const payload = sessionPayload(String(node["nodeId"]), String(challenge["challengeId"]), nonce, expires, identity.did);
    const created = await unary<Obj, Obj>(this.bootstrapStub, "completeAgentSession", {
      challengeId: challenge["challengeId"], signature: identity.sign(payload),
    });
    const metadata = new grpc.Metadata();
    metadata.set("authorization", `Bearer ${String(created["token"])}`);
    state.metadata = metadata;
    state.expiresAt = Number(created["expiresAtUnixMs"] ?? 0);
  }

  // ── identity ──────────────────────────────────────────────────────────────

  getIdentity(): Promise<Identity> {
    return unary(this.stub, "getIdentity", {});
  }

  /** Return the SDK identity authenticated for this client session. */
  getAgentIdentity(): Promise<Identity> {
    return unary(this.stub, "getAgentIdentity", {});
  }

  getNodeIdentity(): Promise<{ nodeId: string; peerId: string; multiaddrs: string[] }> {
    return unary(this.stub, "getNodeIdentity", {});
  }

  // ── registry ──────────────────────────────────────────────────────────────

  findAgents(capability: string, limit = 10): Promise<AgentCard[]> {
    return serverStream(this.stub, "findAgents", { capability, limit });
  }

  getAgentCard(did: string): Promise<AgentCard> {
    return unary(this.stub, "getAgentCard", { did });
  }

  /**
   * Publish this agent's card to the DHT, advertising `skills` so other
   * agents can discover it via findAgents(). `did`/`publicKey`/timestamps/
   * signature are all filled in server-side (daemon/registry.Registry.Publish)
   * and any values passed here for those fields are overwritten — only
   * `name`, `description`, `skills`, `multiaddrs`, and `metadata` matter.
   * Callers that want the card to be reachable over libp2p (not just
   * discoverable) should set `multiaddrs` from getIdentity().
   */
  async publishAgentCard(card: {
    name: string;
    description?: string;
    skills: Skill[];
    multiaddrs?: string[];
    metadata?: Record<string, string>;
  }): Promise<{ success: boolean; error?: string }> {
    await sessionReady(this.stub);
    const unsigned = {
      name: card.name,
      description: card.description ?? "",
      skills: card.skills,
      multiaddrs: card.multiaddrs ?? [],
      metadata: card.metadata ?? {},
    };
    const signed = this.identity ? this.identity.signAgentCard(unsigned, this.nodePeerId) : unsigned;
    return unary(this.stub, "publishAgentCard", signed);
  }

  // ── messaging ─────────────────────────────────────────────────────────────

  sendMessage(toDid: string, text: string, opts: { threadId?: string; taskId?: string } = {}): Promise<{ messageId: string; queued: boolean }> {
    return unary(this.stub, "sendMessage", {
      toDid,
      threadId: opts.threadId ?? "",
      taskId: opts.taskId ?? "",
      kind: "MESSAGE_KIND_TEXT",
      payload: Buffer.from(JSON.stringify({ text })),
    });
  }

  getInbox(opts: { threadId?: string; taskId?: string; unreadOnly?: boolean; limit?: number } = {}): Promise<Obj[]> {
    return serverStream(this.stub, "getInbox", {
      threadId: opts.threadId ?? "",
      taskId: opts.taskId ?? "",
      unreadOnly: opts.unreadOnly ?? false,
      limit: opts.limit ?? 50,
    });
  }

  subscribeInbox(opts: { threadId?: string; taskId?: string } = {}): AsyncIterable<Obj> {
    return sessionStream(this.stub, "subscribeInbox", {
      threadId: opts.threadId ?? "",
      taskId: opts.taskId ?? "",
    });
  }

  /** Mark an inbox message read. getInbox({unreadOnly:true}) skips it after. */
  async ackMessage(messageId: string): Promise<void> {
    await unary(this.stub, "ackMessage", { messageId });
  }

  // ── tasks ─────────────────────────────────────────────────────────────────

  createTask(
    toDid: string,
    skill: string,
    opts: { threadId?: string; metadata?: Record<string, string>; idempotencyKey?: string; timeoutMs?: number; maxAttempts?: number } = {},
  ): Promise<Task> {
    return unary(this.stub, "createTask", {
      toDid,
      idempotencyKey: opts.idempotencyKey ?? "",
      timeoutMs: opts.timeoutMs ?? 0,
      maxAttempts: opts.maxAttempts ?? 0,
      task: { skill, threadId: opts.threadId ?? "", metadata: opts.metadata ?? {} },
    });
  }

  getTask(taskId: string): Promise<Task> {
    return unary(this.stub, "getTask", { id: taskId });
  }

  async waitTask(taskId: string, opts: { pollMs?: number; timeoutMs?: number } = {}): Promise<Task> {
    const { pollMs = 500, timeoutMs = 60_000 } = opts;
    const terminal = new Set(["TASK_STATUS_COMPLETED", "TASK_STATUS_FAILED", "TASK_STATUS_CANCELLED"]);
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const task = await this.getTask(taskId);
      if (terminal.has(task.status)) return task;
      if (Date.now() >= deadline) throw new Error(`task ${taskId} timed out after ${timeoutMs}ms`);
      await sleep(pollMs);
    }
  }

  markWorking(taskId: string): Promise<Task> {
    return unary(this.stub, "updateTask", { taskId, status: "TASK_STATUS_WORKING" });
  }

  markCompleted(taskId: string, outputArtifacts: Artifact[] = []): Promise<Task> {
    return unary(this.stub, "updateTask", { taskId, status: "TASK_STATUS_COMPLETED", outputArtifacts });
  }

  markFailed(taskId: string, error: string): Promise<Task> {
    return unary(this.stub, "updateTask", { taskId, status: "TASK_STATUS_FAILED", error });
  }

  cancelTask(taskId: string): Promise<Task> {
    return unary(this.stub, "cancelTask", { id: taskId });
  }

  subscribeTaskEvents(taskId: string, afterSequence = 0): AsyncIterable<Obj> {
    return sessionStream(this.stub, "subscribeTaskEvents", { id: taskId, afterSequence });
  }

  sendTaskResult(
    toDid: string,
    taskId: string,
    opts: { threadId?: string; status?: "TASK_STATUS_COMPLETED" | "TASK_STATUS_FAILED"; outputArtifacts?: Artifact[]; error?: string; data?: Uint8Array } = {},
  ): Promise<{ messageId: string; queued: boolean }> {
    return unary(this.stub, "sendTaskResult", {
      toDid,
      threadId: opts.threadId ?? "",
      result: {
        taskId,
        status: opts.status ?? "TASK_STATUS_COMPLETED",
        outputArtifacts: opts.outputArtifacts ?? [],
        error: opts.error ?? "",
        data: opts.data ?? new Uint8Array(),
      },
    });
  }

  subscribeTasks(skills: string[], afterSequence = 0): AsyncIterable<Obj> {
    return sessionStream(this.stub, "subscribeTasks", { skills, afterSequence });
  }

  claimTask(taskId: string, leaseSeconds = 30): Promise<Obj> {
    return unary(this.stub, "claimTask", { taskId, leaseSeconds });
  }

  renewTaskLease(taskId: string, leaseToken: string, leaseSeconds = 30): Promise<Obj> {
    return unary(this.stub, "renewTaskLease", { taskId, leaseToken, leaseSeconds });
  }

  completeTaskLease(taskId: string, leaseToken: string, outputArtifacts: Artifact[] = [], data = new Uint8Array()): Promise<Task> {
    return unary(this.stub, "completeTask", { taskId, leaseToken, outputArtifacts, data });
  }

  failTaskLease(taskId: string, leaseToken: string, error: string): Promise<Task> {
    return unary(this.stub, "failTask", { taskId, leaseToken, error });
  }

  /** Create a resumable SDK worker for capability-specific application code. */
  worker(skills: string[], opts: { concurrency?: number; leaseSeconds?: number } = {}): DurableWorker {
    return new DurableWorker(this, skills, opts);
  }

  // ── blobs ─────────────────────────────────────────────────────────────────

  async storeBlob(data: Uint8Array, opts: { mimeType?: string; filename?: string } = {}): Promise<string> {
    const result: Obj = await unary(this.stub, "sendFile", {
      data,
      mimeType: opts.mimeType ?? "application/octet-stream",
      name: opts.filename ?? "",
    });
    return result["cid"] as string;
  }

  async fetchBlob(cid: string): Promise<Uint8Array> {
    const chunks = await serverStream<Obj, Obj>(this.stub, "fetchFile", { cid });
    const parts = chunks.map(c => c["data"] as Uint8Array).filter(Boolean);
    if (parts.length === 0) return new Uint8Array(0);
    const total = parts.reduce((n, p) => n + p.byteLength, 0);
    const out = new Uint8Array(total);
    let off = 0;
    for (const p of parts) { out.set(p, off); off += p.byteLength; }
    return out;
  }

  // ── threads ───────────────────────────────────────────────────────────────

  createThread(
    replicaDids: string[],
    opts: { f?: number; epochMs?: number; backend?: "raft" | "tendermint" } = {},
  ): Promise<Thread> {
    const f = opts.f ?? 0;
    const epochMs = opts.epochMs ?? 200;
    const metadata = { backend: opts.backend ?? "raft" };
    if (this.identity) {
      const creatorDid = this.identity.did;
      const descriptor = {
        id: randomUUID(), creatorDid,
        replicaDids: [...new Set([creatorDid, ...replicaDids])],
        n: f === 0 ? 1 : 3 * f + 1, f, epochMs,
        createdAt: String(Date.now()), metadata,
      };
      return unary(this.stub, "createThread", {
        replicaDids: descriptor.replicaDids, f, epochMs, metadata,
        threadId: descriptor.id, creatorDid, createdAt: descriptor.createdAt,
        creatorSignature: this.identity.signThreadDescriptor(descriptor),
      });
    }
    return unary(this.stub, "createThread", {
      replicaDids,
      f, epochMs, metadata,
    });
  }

  getThread(threadId: string): Promise<Thread> {
    return unary(this.stub, "getThread", { id: threadId });
  }

  createThreadWithRecovery(replicaDids: string[], opts: { f?: number; epochMs?: number; backend?: "raft" | "tendermint" } = {}): Promise<Obj> {
    const f = opts.f ?? 0, epochMs = opts.epochMs ?? 200;
    const recoverySecret = randomBytes(32);
    // Metadata map keys are not protobuf field names: this exact wire key is
    // covered by the creator descriptor signature and verified by Go.
    const metadata = { backend: opts.backend ?? "raft", recovery_capability_sha256: createHash("sha256").update(recoverySecret).digest("hex") };
    if (!this.identity) return unary(this.stub, "createThreadWithRecovery", { replicaDids, f, epochMs, metadata, recoverySecret });
    const creatorDid = this.identity.did, createdAt = String(Date.now());
    const descriptor = { id: randomUUID(), creatorDid, replicaDids: [...new Set([creatorDid, ...replicaDids])], n: f === 0 ? 1 : 3*f+1, f, epochMs, createdAt, metadata };
    return unary(this.stub, "createThreadWithRecovery", { ...descriptor, creatorSignature: this.identity.signThreadDescriptor(descriptor), recoverySecret });
  }

  appendEntry(threadId: string, payload: Uint8Array, opts: { kind?: string } = {}): Promise<void> {
    return unary(this.stub, "appendEntry", {
      threadId,
      payload,
      kind: opts.kind ?? "message",
    });
  }

  /** Submit an SDK-prepared v2 ciphertext entry; plaintext is never sent. */
  appendEncryptedEntry(threadId: string, entry: Obj): Promise<void> {
    return unary(this.stub, "appendEntry", { threadId, encryptedEntry: entry });
  }

  /** Publish an opaque epoch key for a current thread member. */
  putThreadKeyEnvelope(envelope: Obj): Promise<void> {
    if (envelope["recoveryEnvelope"]) {
      if (!this.identity) throw new Error("recovery envelope publication requires an SDK identity");
      envelope["authorSignature"] = this.identity.signRecoveryKeyEnvelope(envelope);
    }
    return unary(this.stub, "putThreadKeyEnvelope", envelope);
  }

  /** Fetch envelopes addressed to the authenticated SDK identity. */
  async getThreadKeyEnvelopes(threadId: string, encryptionEpoch: number): Promise<Obj[]> {
    const result: Obj = await unary(this.stub, "getThreadKeyEnvelopes", { threadId, encryptionEpoch });
    return (result["envelopes"] as Obj[] | undefined) ?? [];
  }

  /** Fetch capability-scoped recovery envelopes; callers retain the secret. */
  async getRecoveryThreadKeyEnvelopes(handle: Obj): Promise<Obj[]> {
    const result: Obj = await unary(this.stub, "getRecoveryThreadKeyEnvelopes", { handle });
    return (result["envelopes"] as Obj[] | undefined) ?? [];
  }

  getThreadEntries(threadId: string, opts: { sinceHeight?: number; limit?: number } = {}): Promise<ThreadEntry[]> {
    return serverStream(this.stub, "getThreadEntries", {
      threadId,
      sinceHeight: opts.sinceHeight ?? 0,
      limit: opts.limit ?? 0,
    });
  }

  subscribeThread(threadId: string): AsyncIterable<ThreadEntry> {
    return sessionStream(this.stub, "subscribeThread", { threadId });
  }

  /** Add a late participant as a non-voting observer. Quorum is unchanged. */
  addThreadObserver(threadId: string, did: string): Promise<Thread> {
    return unary(this.stub, "addThreadReplica", { threadId, replicaDid: did });
  }

  async listThreadMembers(threadId: string): Promise<Obj[]> {
    const result: Obj = await unary(this.stub, "listThreadMembers", { id: threadId });
    return (result["members"] as Obj[] | undefined) ?? [];
  }

  async createThreadCatchupProof(threadId: string): Promise<Uint8Array> {
    if (!this.identity) throw new Error("catchup proofs require an SDK identity");
    const state: Obj = await unary(this.stub, "getThreadCatchupState", { id: threadId });
    if (state["threadId"] !== threadId || state["observerDid"] !== this.identity.did) throw new Error("daemon catchup state is not bound to this SDK identity/thread");
    const proof: Obj = { threadId, observerDid: this.identity.did, committedHeight: state["committedHeight"], headBlockHash: state["headBlockHash"], issuedAtUnixMs: String(Date.now()) };
    proof["signature"] = this.identity.signThreadCatchupProof(proof);
    return this.identity.encodeThreadCatchupProof(proof);
  }

  inviteThreadMember(threadId: string, inviteeDid: string, expiresAtUnixMs: number, nonce: Uint8Array): Promise<Obj> {
    if (!this.identity) throw new Error("signed invitations require an SDK identity");
    const invitation = { threadId, inviterDid: this.identity.did, inviteeDid, role: "THREAD_MEMBER_ROLE_OBSERVER", expiresAtUnixMs: String(expiresAtUnixMs), nonce };
    return unary(this.stub, "inviteThreadMember", { ...invitation, signature: this.identity.signThreadInvitation(invitation) });
  }
  acceptThreadInvite(threadId: string, invite: Uint8Array): Promise<Thread> { return unary(this.stub, "acceptThreadInvite", { threadId, invite }); }
  promoteThreadMember(threadId: string, memberDid: string, catchupProof: Uint8Array): Promise<Obj> {
    if (catchupProof.length === 0) throw new Error("signed catchup proof is required for promotion");
    return unary(this.stub, "promoteThreadMember", { threadId, memberDid, catchupProof });
  }
  removeThreadMember(threadId: string, memberDid: string): Promise<Obj> { return unary(this.stub, "removeThreadMember", { threadId, memberDid }); }
  leaveThread(threadId: string): Promise<Obj> { return unary(this.stub, "leaveThread", { id: threadId }); }

  /** Recover and cryptographically verify a read-only thread from the network. */
  recoverThread(threadId: string): Promise<Thread> {
    void threadId;
    return Promise.reject(new Error("bare-ID recovery is retired; use recoverThreadWithHandle"));
  }

  recoverThreadWithHandle(handle: { threadId: string; recoverySecret: Uint8Array; version?: number }, subscribe = false): Promise<Obj> {
    return unary(this.stub, "recoverThreadWithHandle", { handle: { ...handle, version: handle.version ?? 1 }, subscribe });
  }

  // ── diagnostics ───────────────────────────────────────────────────────────

  health(): Promise<HealthInfo> {
    return unary(this.stub, "health", {});
  }

  ping(did = ""): Promise<PingResult> {
    return unary(this.stub, "ping", { targetDid: did });
  }

  async listPeers(): Promise<PeerInfo[]> {
    const r: Obj = await unary(this.stub, "listPeers", {});
    return (r["peers"] as PeerInfo[]) ?? [];
  }

  async connectPeer(did: string): Promise<{ peerId: string; multiaddrs: string[]; alreadyConnected: boolean }> {
    const r: Obj = await unary(this.stub, "connectPeer", { did });
    return {
      peerId: r["peerId"] as string,
      multiaddrs: (r["multiaddrs"] as string[]) ?? [],
      alreadyConnected: r["alreadyConnected"] as boolean,
    };
  }

  async disconnectPeer(did: string): Promise<void> {
    await unary(this.stub, "disconnectPeer", { did });
  }

  // ── pub/sub ───────────────────────────────────────────────────────────────

  async publish(topic: string, payload: string | Uint8Array): Promise<void> {
    const data = typeof payload === "string" ? Buffer.from(payload) : payload;
    await unary(this.stub, "publish", { topic, payload: data });
  }

  subscribeTopic(topic: string): AsyncIterable<TopicMessage> {
    return sessionStream(this.stub, "subscribeTopic", { topic });
  }

  // ── webhooks ──────────────────────────────────────────────────────────────

  async setWebhook(url: string, secret = ""): Promise<string> {
    const r: Obj = await unary(this.stub, "setWebhook", { url, secret });
    return (r["url"] as string) ?? url;
  }

  async clearWebhook(): Promise<void> {
    await unary(this.stub, "clearWebhook", {});
  }

  async getWebhook(): Promise<string> {
    const r: Obj = await unary(this.stub, "getWebhook", {});
    return (r["url"] as string) ?? "";
  }

  // ── networks ──────────────────────────────────────────────────────────────

  createNetwork(name: string): Promise<NetworkInfo> {
    return unary(this.stub, "createNetwork", { name });
  }

  joinNetwork(networkId: string): Promise<NetworkInfo> {
    return unary(this.stub, "joinNetwork", { networkId });
  }

  async leaveNetwork(networkId: string): Promise<void> {
    await unary(this.stub, "leaveNetwork", { networkId });
  }

  async listNetworks(): Promise<NetworkInfo[]> {
    const r: Obj = await unary(this.stub, "listNetworks", {});
    return (r["networks"] as NetworkInfo[]) ?? [];
  }

  async networkMembers(networkId: string): Promise<NetworkMember[]> {
    const r: Obj = await unary(this.stub, "networkMembers", { networkId });
    return (r["members"] as NetworkMember[]) ?? [];
  }

  async broadcastNetwork(networkId: string, payload: string | Uint8Array): Promise<void> {
    const data = typeof payload === "string" ? Buffer.from(payload) : payload;
    await unary(this.stub, "broadcastNetwork", { networkId, payload: data });
  }

  subscribeNetwork(networkId: string): AsyncIterable<BroadcastMessage> {
    return sessionStream(this.stub, "subscribeNetwork", { networkId });
  }
}

export type TaskHandler = (task: Obj) => Promise<Artifact[] | void> | Artifact[] | void;

/**
 * A durable leased worker. The cursor advances before handler invocation; a
 * crashed handler is retried by the daemon after its lease expires under a new
 * delivery cursor. `run` reconnects until its AbortSignal is aborted.
 */
export class DurableWorker {
  private readonly handlers = new Map<string, TaskHandler>();
  private cursor = 0;
  private readonly concurrency: number;
  private readonly leaseSeconds: number;

  constructor(private readonly client: A2AClient, private readonly skills: string[], opts: { concurrency?: number; leaseSeconds?: number } = {}) {
    this.concurrency = Math.max(1, opts.concurrency ?? 1);
    this.leaseSeconds = Math.max(3, opts.leaseSeconds ?? 30);
  }

  handle(skill: string, handler: TaskHandler): this {
    this.handlers.set(skill, handler);
    return this;
  }

  afterSequence(): number { return this.cursor; }

  async run(signal?: AbortSignal): Promise<void> {
    const inFlight = new Set<Promise<void>>();
    while (!signal?.aborted) {
      try {
        for await (const raw of this.client.subscribeTasks(this.skills, this.cursor)) {
          if (signal?.aborted) break;
          const delivery = raw as Obj;
          const sequence = Number(delivery["sequence"] ?? 0);
          this.cursor = Math.max(this.cursor, sequence);
          const task = delivery["task"] as Obj | undefined;
          const handler = task ? this.handlers.get(String(task["skill"] ?? "")) : undefined;
          if (!task || !handler) continue;
          while (inFlight.size >= this.concurrency) await Promise.race(inFlight);
          const run = this.process(task, handler, signal).finally(() => inFlight.delete(run));
          inFlight.add(run);
        }
      } catch {
        // A stream/session/transport interruption is normal; preserve cursor
        // and reconnect after a short backoff.
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
    await Promise.allSettled(inFlight);
  }

  private async process(task: Obj, handler: TaskHandler, signal?: AbortSignal): Promise<void> {
    const taskID = String(task["id"] ?? "");
    if (!taskID) return;
    let lease: Obj;
    try {
      lease = await this.client.claimTask(taskID, this.leaseSeconds);
    } catch {
      return; // a replayed delivery may already be held by another worker
    }
    const token = String(lease["leaseToken"] ?? "");
    if (!token) return;
    let stopped = false;
    const timer = setInterval(() => {
      if (!stopped && !signal?.aborted) void this.client.renewTaskLease(taskID, token, this.leaseSeconds).catch(() => { stopped = true; });
    }, Math.max(1000, Math.floor(this.leaseSeconds * 500)));
    try {
      const result = await handler(task);
      await this.client.completeTaskLease(taskID, token, result ?? []);
    } catch (error) {
      try { await this.client.failTaskLease(taskID, token, error instanceof Error ? error.message : String(error)); } catch { /* lease may have expired */ }
    } finally {
      stopped = true;
      clearInterval(timer);
    }
  }
}

// ── helpers ───────────────────────────────────────────────────────────────────

const DID_KEY_PREFIX = "did:key:";
const DID_KEY_MB_PREFIX = "did:key:z";

export function normalizeDid(did: string): string {
  if (!did) return did;
  if (did.startsWith("did:")) {
    if (did.startsWith(DID_KEY_PREFIX) && !did.startsWith(DID_KEY_MB_PREFIX)) {
      return DID_KEY_MB_PREFIX + did.slice(DID_KEY_PREFIX.length);
    }
    return did;
  }
  if (did.startsWith("z")) return `${DID_KEY_PREFIX}${did}`;
  return `${DID_KEY_MB_PREFIX}${did}`;
}

export function shortDid(did: string, opts: { head?: number; tail?: number } = {}): string {
  const head = opts.head ?? 8;
  const tail = opts.tail ?? 4;
  if (head < 0 || tail < 0) throw new Error("head and tail must be >= 0");
  const full = normalizeDid(did);
  if (full.length <= head + tail + 3) return full;
  return `${full.slice(0, head)}...${full.slice(-tail)}`;
}

export const CORE_CAPABILITY_PREFIX = "a2a:v1:cap:";

export const CoreCapabilities = {
  TEXT_GENERATION: `${CORE_CAPABILITY_PREFIX}text-generation`,
  CODE_EXECUTION: `${CORE_CAPABILITY_PREFIX}code-execution`,
  WEB_RETRIEVAL: `${CORE_CAPABILITY_PREFIX}web-retrieval`,
  IMAGE_GENERATION: `${CORE_CAPABILITY_PREFIX}image-generation`,
  DATA_ANALYSIS: `${CORE_CAPABILITY_PREFIX}data-analysis`,
  FILE_PROCESSING: `${CORE_CAPABILITY_PREFIX}file-processing`,
  TOOL_USE: `${CORE_CAPABILITY_PREFIX}tool-use`,
  EMBEDDING: `${CORE_CAPABILITY_PREFIX}embedding`,
  SPEECH_TO_TEXT: `${CORE_CAPABILITY_PREFIX}speech-to-text`,
  TEXT_TO_SPEECH: `${CORE_CAPABILITY_PREFIX}text-to-speech`,
} as const;

export type CoreCapability = typeof CoreCapabilities[keyof typeof CoreCapabilities];

export function capabilityId(name: string, opts: { version?: string } = {}): string {
  if (!name) return name;
  if (name.includes(":")) return name;
  const version = opts.version ?? "v1";
  return `a2a:${version}:cap:${name}`;
}

export function normalizeCapability(capability: string, opts: { version?: string } = {}): string {
  return capabilityId(capability, opts);
}

export function capabilityName(capability: string): string {
  if (!capability) return capability;
  const marker = ":cap:";
  const idx = capability.indexOf(marker);
  if (idx === -1) return capability;
  return capability.slice(idx + marker.length);
}

export function isCoreCapability(capability: string, opts: { version?: string } = {}): boolean {
  if (!capability) return false;
  const version = opts.version ?? "v1";
  return capability.startsWith(`a2a:${version}:cap:`);
}

function sleep(ms: number): Promise<void> {
  return new Promise(r => setTimeout(r, ms));
}

function toAsyncIterable<T>(stream: grpc.ClientReadableStream<T>): AsyncIterable<T> {
  return {
    [Symbol.asyncIterator]() {
      const queue: T[] = [];
      let done = false;
      let error: Error | null = null;
      let waiter: ((v: IteratorResult<T>) => void) | null = null;

      stream.on("data", (item: T) => {
        if (waiter) { const w = waiter; waiter = null; w({ value: item, done: false }); }
        else queue.push(item);
      });
      stream.on("end", () => {
        done = true;
        if (waiter) { const w = waiter; waiter = null; w({ value: undefined as unknown as T, done: true }); }
      });
      let rejecter: ((err: Error) => void) | null = null;
      stream.on("error", (err: Error) => {
        error = err;
        // A stream error must surface as a rejection, not a silent `done:
        // true` — resolving it as a clean end-of-stream is indistinguishable
        // from the server simply having nothing more to say, which is what
        // let an immediate auth failure (no session established) masquerade
        // as an empty subscription and made DurableWorker.run() reconnect in
        // a zero-backoff loop instead of ever reaching its own retry delay.
        if (rejecter) { const r = rejecter; rejecter = null; waiter = null; r(err); }
        else if (waiter) { waiter = null; }
      });

      return {
        next(): Promise<IteratorResult<T>> {
          if (queue.length > 0) return Promise.resolve({ value: queue.shift()!, done: false });
          if (error) return Promise.reject(error);
          if (done) return Promise.resolve({ value: undefined as unknown as T, done: true });
          return new Promise((resolve, reject) => { waiter = resolve; rejecter = reject; });
        },
        // Defining return() makes this a well-behaved async iterator: a `for
        // await` loop that exits early (break, return, or an uncaught throw
        // in the loop body) calls it automatically, so the underlying gRPC
        // stream is cancelled instead of being left open server-side for the
        // lifetime of the process — otherwise every early-exited subscriber
        // (e.g. a browser client disconnecting from an SSE relay) leaks one
        // open stream per connection.
        return(value?: T): Promise<IteratorResult<T>> {
          stream.cancel();
          return Promise.resolve({ value: value as T, done: true });
        },
      };
    },
  };
}
