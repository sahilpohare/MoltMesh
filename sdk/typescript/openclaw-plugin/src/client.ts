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
  return new Promise((resolve, reject) => {
    (stub[method] as (req: Req, cb: (err: Error | null, res: Res) => void) => void)(
      req,
      (err, res) => { if (err) reject(err); else resolve(res); },
    );
  });
}

/** Collect all items from a server-streaming call into an array. */
export function serverStream<Req, Res>(stub: GrpcClient, method: string, req: Req): Promise<Res[]> {
  return new Promise((resolve, reject) => {
    const items: Res[] = [];
    const call = stub[method](req) as grpc.ClientReadableStream<Res>;
    call.on("data", (item: Res) => items.push(item));
    call.on("end", () => resolve(items));
    call.on("error", reject);
  });
}

// ── types ─────────────────────────────────────────────────────────────────────

export type Obj = Record<string, unknown>;

export interface Identity { did: string; publicKey: string; multiaddrs: string[] }
export interface AgentCard { did: string; name: string; capabilities: string[] }
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

  constructor(addr?: string) {
    this.stub = createStub(addr ?? defaultAddr());
  }

  close(): void {
    this.stub.close();
  }

  // ── identity ──────────────────────────────────────────────────────────────

  getIdentity(): Promise<Identity> {
    return unary(this.stub, "getIdentity", {});
  }

  // ── registry ──────────────────────────────────────────────────────────────

  findAgents(capability: string, limit = 10): Promise<AgentCard[]> {
    return serverStream(this.stub, "findAgents", { capability, limit });
  }

  getAgentCard(did: string): Promise<AgentCard> {
    return unary(this.stub, "getAgentCard", { did });
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
    return toAsyncIterable(this.stub["subscribeInbox"]({
      threadId: opts.threadId ?? "",
      taskId: opts.taskId ?? "",
    }) as grpc.ClientReadableStream<Obj>);
  }

  // ── tasks ─────────────────────────────────────────────────────────────────

  createTask(
    toDid: string,
    skill: string,
    opts: { threadId?: string; metadata?: Record<string, string> } = {},
  ): Promise<Task> {
    return unary(this.stub, "createTask", {
      toDid,
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

  subscribeTaskEvents(taskId: string): AsyncIterable<Obj> {
    return toAsyncIterable(this.stub["subscribeTaskEvents"]({ id: taskId }) as grpc.ClientReadableStream<Obj>);
  }

  // ── blobs ─────────────────────────────────────────────────────────────────

  async storeBlob(data: Uint8Array, opts: { mimeType?: string; filename?: string } = {}): Promise<string> {
    const result: Obj = await unary(this.stub, "storeBlob", {
      data,
      mimeType: opts.mimeType ?? "application/octet-stream",
      filename: opts.filename ?? "",
    });
    return result["cid"] as string;
  }

  async fetchBlob(cid: string): Promise<Uint8Array> {
    const result: Obj = await unary(this.stub, "fetchBlob", { cid });
    return result["data"] as Uint8Array;
  }

  // ── threads ───────────────────────────────────────────────────────────────

  createThread(
    replicaDids: string[],
    opts: { f?: number; epochMs?: number; backend?: "raft" | "tendermint" } = {},
  ): Promise<Thread> {
    return unary(this.stub, "createThread", {
      replicaDids,
      f: opts.f ?? 0,
      epochMs: opts.epochMs ?? 200,
      metadata: { backend: opts.backend ?? "raft" },
    });
  }

  getThread(threadId: string): Promise<Thread> {
    return unary(this.stub, "getThread", { id: threadId });
  }

  appendEntry(threadId: string, payload: Uint8Array, opts: { kind?: string; authorDid?: string } = {}): Promise<void> {
    return unary(this.stub, "appendEntry", {
      threadId,
      entry: { payload, kind: opts.kind ?? "message", authorDid: opts.authorDid ?? "" },
    });
  }

  getThreadEntries(threadId: string, opts: { sinceHeight?: number; limit?: number } = {}): Promise<ThreadEntry[]> {
    return serverStream(this.stub, "getThreadEntries", {
      threadId,
      sinceHeight: opts.sinceHeight ?? 0,
      limit: opts.limit ?? 0,
    });
  }

  subscribeThread(threadId: string): AsyncIterable<ThreadEntry> {
    return toAsyncIterable(this.stub["subscribeThread"]({ threadId }) as grpc.ClientReadableStream<ThreadEntry>);
  }

  // ── diagnostics ───────────────────────────────────────────────────────────

  health(): Promise<HealthInfo> {
    return unary(this.stub, "health", {});
  }

  ping(did = ""): Promise<PingResult> {
    return unary(this.stub, "ping", { did });
  }

  listPeers(): Promise<PeerInfo[]> {
    return serverStream(this.stub, "listPeers", {});
  }

  // ── pub/sub ───────────────────────────────────────────────────────────────

  async publish(topic: string, payload: string | Uint8Array): Promise<void> {
    const data = typeof payload === "string" ? Buffer.from(payload) : payload;
    await unary(this.stub, "publish", { topic, payload: data });
  }

  subscribeTopic(topic: string): AsyncIterable<TopicMessage> {
    return toAsyncIterable(this.stub["subscribeTopic"]({ topic }) as grpc.ClientReadableStream<TopicMessage>);
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

  listNetworks(): Promise<NetworkInfo[]> {
    return serverStream(this.stub, "listNetworks", {});
  }

  networkMembers(networkId: string): Promise<NetworkMember[]> {
    return serverStream(this.stub, "networkMembers", { networkId });
  }

  async broadcastNetwork(networkId: string, payload: string | Uint8Array): Promise<void> {
    const data = typeof payload === "string" ? Buffer.from(payload) : payload;
    await unary(this.stub, "broadcastNetwork", { networkId, payload: data });
  }

  subscribeNetwork(networkId: string): AsyncIterable<BroadcastMessage> {
    return toAsyncIterable(this.stub["subscribeNetwork"]({ networkId }) as grpc.ClientReadableStream<BroadcastMessage>);
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
      stream.on("error", (err: Error) => {
        error = err;
        if (waiter) { const w = waiter; waiter = null; w({ value: undefined as unknown as T, done: true }); }
      });

      return {
        next(): Promise<IteratorResult<T>> {
          if (queue.length > 0) return Promise.resolve({ value: queue.shift()!, done: false });
          if (done) return Promise.resolve({ value: undefined as unknown as T, done: true });
          if (error) return Promise.reject(error);
          return new Promise(resolve => { waiter = resolve; });
        },
      };
    },
  };
}
