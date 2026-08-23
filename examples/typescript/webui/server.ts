/**
 * webui/server.ts — a web front end for the OpenMolt Network, built on the
 * TypeScript SDK (A2AClient in sdk/typescript/openclaw-plugin/src/client.ts).
 *
 * This talks to ONE daemon — its own daemon, e.g. Agent A from the
 * discover-and-call demo (07_master_agent.ts is the callee, running on a
 * separate daemon). The browser never touches gRPC directly; this server
 * is the same kind of caller Claude Code's moltmesh MCP plugin is, just
 * with a browser instead of a chat window in front of it.
 *
 * Run:
 *   AGENT_ADDR=127.0.0.1:15701 npx tsx webui/server.ts
 *   open http://localhost:4600
 */
import express from "express";
import { readFileSync, writeFileSync, existsSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { randomBytes } from "crypto";
import { A2AClient, type ThreadEntry } from "../../../sdk/typescript/openclaw-plugin/src/client.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const AGENT_ADDR = process.env["AGENT_ADDR"] ?? "";
const PORT = Number(process.env["PORT"] ?? 4600);

const client = new A2AClient(AGENT_ADDR);

const app = express();
app.use(express.json());
app.use(express.static(join(__dirname, "public")));

/** Wrap a handler so a thrown/rejected SDK error becomes a 502 with its text
 * rather than an unhandled rejection that hangs the request. */
function route(handler: (req: express.Request, res: express.Response) => Promise<unknown>) {
  return async (req: express.Request, res: express.Response) => {
    try {
      const body = await handler(req, res);
      if (!res.headersSent) res.json(body ?? { ok: true });
    } catch (err) {
      if (!res.headersSent) res.status(502).json({ error: String(err) });
    }
  };
}

// ── node: identity, health, advertising ─────────────────────────────────────

app.get("/api/identity", route(async () => client.getIdentity()));
app.get("/api/health", route(async () => client.health()));

/** Advertising: publish this daemon's own signed Agent Card to the DHT.
 * Until a daemon does this, other peers cannot resolve its DID — which also
 * means they cannot route task results back to it. */
app.post("/api/advertise", route(async (req) => {
  const { name, description, skills } = req.body as { name?: string; description?: string; skills?: string[] };
  if (!name) throw new Error("name is required");
  const me = await client.getIdentity();
  const result = await client.publishAgentCard({
    name,
    description: description ?? "",
    skills: (skills ?? []).filter(Boolean).map((id) => ({ id, name: id.split(":").pop() ?? id })),
    multiaddrs: (me as { multiaddrs?: string[] }).multiaddrs ?? [],
  });
  // publishAgentCard resolves with {success,error} rather than throwing, so a
  // failure here would otherwise look like success to the browser.
  if (!result.success) throw new Error(result.error ?? "publish failed");
  return result;
}));

// ── peers: list, dial, hang up, ping ────────────────────────────────────────

app.get("/api/peers", route(async () => client.listPeers()));

/** Dial a peer by DID: resolves its Agent Card from the DHT for multiaddrs,
 * then opens a libp2p connection. */
app.post("/api/peers/connect", route(async (req) => {
  const { did } = req.body as { did?: string };
  if (!did) throw new Error("did is required");
  return client.connectPeer(did);
}));

app.post("/api/peers/disconnect", route(async (req) => {
  const { did } = req.body as { did?: string };
  if (!did) throw new Error("did is required");
  await client.disconnectPeer(did);
  return { ok: true };
}));

app.post("/api/peers/ping", route(async (req) => {
  const { did } = req.body as { did?: string };
  return client.ping(did ?? "");
}));

// ── agent discovery + task delegation ───────────────────────────────────────

app.get("/api/agents", route(async (req) => {
  const capability = String(req.query["capability"] ?? "a2a:v1:cap:text-generation");
  return client.findAgents(capability, 20);
}));

app.post("/api/delegate", route(async (req) => {
  const { did, capability, prompt, threadId } = req.body as { did?: string; capability?: string; prompt?: string; threadId?: string };
  if (!did || !capability) throw new Error("did and capability are required");
  const task = await client.createTask(did, capability, {
    threadId: threadId ?? "",
    metadata: prompt ? { prompt } : {},
  });
  return { taskId: task.id };
}));

/** Server-Sent Events: push task status transitions live instead of polling. */
app.get("/api/tasks/:id/stream", async (req, res) => {
  const taskId = req.params["id"]!;
  const send = openSSE(res);

  let lastStatus = "";
  const timer = setInterval(async () => {
    try {
      const task = await client.getTask(taskId);
      if (task.status !== lastStatus) {
        lastStatus = task.status;
        send("status", { status: task.status });
      }
      if (task.status === "TASK_STATUS_COMPLETED" || task.status === "TASK_STATUS_FAILED" || task.status === "TASK_STATUS_CANCELLED") {
        if (task.error) {
          send("error", { error: task.error });
        } else {
          const artifact = task.outputArtifacts?.[0];
          let text = "";
          if (artifact) {
            const bytes = artifact.data?.length ? artifact.data : await client.fetchBlob(artifact.cid);
            text = Buffer.from(bytes).toString("utf8");
          }
          send("done", { text });
        }
        clearInterval(timer);
        res.end();
      }
    } catch (err) {
      send("error", { error: String(err) });
      clearInterval(timer);
      res.end();
    }
  }, 500);

  req.on("close", () => clearInterval(timer));
});

// ── direct messaging ────────────────────────────────────────────────────────

app.get("/api/inbox", route(async (req) => {
  const unreadOnly = req.query["unread"] === "1";
  const msgs = await client.getInbox({ unreadOnly, limit: 100 });
  return msgs.map(decodeInboxMessage);
}));

app.post("/api/messages", route(async (req) => {
  const { to, text, threadId } = req.body as { to?: string; text?: string; threadId?: string };
  if (!to || !text) throw new Error("to and text are required");
  return client.sendMessage(to, text, { threadId: threadId ?? "" });
}));

app.post("/api/inbox/:id/ack", route(async (req) => {
  await client.ackMessage(req.params["id"]!);
  return { ok: true };
}));

/** Live inbox feed. Breaking the loop on client disconnect cancels the
 * underlying gRPC stream via the iterator's return(). */
app.get("/api/inbox/stream", async (req, res) => {
  const send = openSSE(res);
  let closed = false;
  req.on("close", () => { closed = true; });
  try {
    for await (const msg of client.subscribeInbox()) {
      if (closed) break;
      send("message", decodeInboxMessage(msg));
    }
  } catch (err) {
    if (!closed) send("error", { error: String(err) });
  } finally {
    res.end();
  }
});

// ── threads: browse, read, chat, membership ─────────────────────────────────
//
// A thread is a replicated, ordered log (see daemon/thread): every entry
// appended to it is committed by consensus and delivered to every subscriber
// watching the same thread ID — including subscribers on other daemons. That
// is the "fan out to all listeners" a chat room needs, and it comes from the
// thread primitive rather than this server tracking its own client list.

/** The daemon exposes GetThread but no ListThreads RPC, so "browse threads"
 * is backed by a local index of thread IDs this UI has created or connected
 * to, persisted so it survives a restart. This is deliberately a client-side
 * view, not a daemon-wide enumeration: a thread created by another client on
 * the same daemon will not appear until someone connects to it here by ID.
 * Exposing thread.Store.ListThreads (which already exists) as an RPC is the
 * proper fix and would let this drop entirely. */
const INDEX_PATH = join(__dirname, ".known-threads.json");
function knownThreadIds(): string[] {
  if (!existsSync(INDEX_PATH)) return [];
  try {
    return JSON.parse(readFileSync(INDEX_PATH, "utf8")) as string[];
  } catch {
    return [];
  }
}
function rememberThread(id: string) {
  const ids = knownThreadIds();
  if (!ids.includes(id)) writeFileSync(INDEX_PATH, JSON.stringify([...ids, id], null, 2));
}
function forgetThread(id: string) {
  writeFileSync(INDEX_PATH, JSON.stringify(knownThreadIds().filter((t) => t !== id), null, 2));
}

/** describeThread reports the consensus backend alongside the descriptor.
 * `backend` is what the thread's own metadata asks for, but the daemon
 * currently runs the GoAkt actor path (thread.NewActorManager), whose
 * ThreadActor hardcodes newRaftBackend and never reads that metadata — so
 * `effectiveBackend` is what actually runs. They differ whenever a thread
 * was created asking for tendermint. */
function describeThread(t: Record<string, unknown>) {
  const metadata = (t["metadata"] ?? {}) as Record<string, string>;
  return {
    ...t,
    backend: metadata["backend"] ?? "raft",
    effectiveBackend: "raft",
  };
}

app.get("/api/threads", route(async () => {
  const results = await Promise.all(knownThreadIds().map(async (id) => {
    try {
      return describeThread((await client.getThread(id)) as unknown as Record<string, unknown>);
    } catch {
      return { id, unavailable: true };
    }
  }));
  return results;
}));

app.post("/api/threads", route(async (req) => {
  const { replicaDids, backend } = req.body as { replicaDids?: string[]; backend?: "raft" | "tendermint" };
  const thread = await client.createThread((replicaDids ?? []).filter(Boolean), { backend: backend ?? "raft" });
  rememberThread(thread.id);
  return describeThread(thread as unknown as Record<string, unknown>);
}));

app.get("/api/threads/:id", route(async (req) => {
  const thread = await client.getThread(req.params["id"]!);
  rememberThread(thread.id);
  return describeThread(thread as unknown as Record<string, unknown>);
}));

app.delete("/api/threads/:id", route(async (req) => {
  forgetThread(req.params["id"]!);
  return { ok: true };
}));

/** Thread history — what a client reads on first connecting to a thread. */
app.get("/api/threads/:id/entries", route(async (req) => {
  const entries = await client.getThreadEntries(req.params["id"]!, { limit: 500 });
  return entries.map(decodeEntry);
}));

/** Append a chat message. Every subscriber of this thread — this browser tab
 * included, via its own /stream connection — receives it once it commits. */
app.post("/api/threads/:id/messages", route(async (req) => {
  const { text } = req.body as { text?: string };
  if (!text) throw new Error("text is required");
  await client.appendEntry(req.params["id"]!, Buffer.from(text, "utf8"), { kind: "message" });
  return { ok: true };
}));

/** Assign a task scoped to this thread. */
app.post("/api/threads/:id/tasks", route(async (req) => {
  const { to, skill, prompt } = req.body as { to?: string; skill?: string; prompt?: string };
  if (!to || !skill) throw new Error("to and skill are required");
  const task = await client.createTask(to, skill, {
    threadId: req.params["id"]!,
    metadata: prompt ? { prompt } : {},
  });
  return { taskId: task.id };
}));

// membership management
app.get("/api/threads/:id/members", route(async (req) => client.listThreadMembers(req.params["id"]!)));

app.post("/api/threads/:id/members/invite", route(async (req) => {
  const { did, ttlMinutes } = req.body as { did?: string; ttlMinutes?: number };
  if (!did) throw new Error("did is required");
  const expiresAt = Date.now() + (ttlMinutes ?? 60) * 60_000;
  return client.inviteThreadMember(req.params["id"]!, did, expiresAt, randomBytes(16));
}));

app.post("/api/threads/:id/members/observer", route(async (req) => {
  const { did } = req.body as { did?: string };
  if (!did) throw new Error("did is required");
  return describeThread((await client.addThreadObserver(req.params["id"]!, did)) as unknown as Record<string, unknown>);
}));

app.post("/api/threads/:id/members/promote", route(async (req) => {
  const { did } = req.body as { did?: string };
  if (!did) throw new Error("did is required");
  // Promotion to voter requires proof the observer has caught up to the
  // committed head; the daemon builds and verifies it, we just fetch it.
  const proof = await client.createThreadCatchupProof(req.params["id"]!);
  return client.promoteThreadMember(req.params["id"]!, did, proof);
}));

app.post("/api/threads/:id/members/remove", route(async (req) => {
  const { did } = req.body as { did?: string };
  if (!did) throw new Error("did is required");
  return client.removeThreadMember(req.params["id"]!, did);
}));

app.post("/api/threads/:id/leave", route(async (req) => {
  const result = await client.leaveThread(req.params["id"]!);
  forgetThread(req.params["id"]!);
  return result;
}));

/** Live thread feed: relays every newly committed entry as an SSE event. */
app.get("/api/threads/:id/stream", async (req, res) => {
  const threadId = req.params["id"]!;
  const send = openSSE(res);
  let closed = false;
  req.on("close", () => { closed = true; });
  try {
    for await (const entry of client.subscribeThread(threadId)) {
      if (closed) break;
      send("entry", decodeEntry(entry));
    }
  } catch (err) {
    if (!closed) send("error", { error: String(err) });
  } finally {
    res.end();
  }
});

// ── helpers ─────────────────────────────────────────────────────────────────

function openSSE(res: express.Response) {
  res.set({ "Content-Type": "text/event-stream", "Cache-Control": "no-cache", Connection: "keep-alive" });
  res.flushHeaders();
  return (event: string, data: unknown) => res.write(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`);
}

/** decodeEntry turns a raw ThreadEntry into what the chat UI renders. */
function decodeEntry(e: ThreadEntry) {
  return {
    height: e.height,
    authorDid: e.entry.authorDid,
    kind: e.entry.kind,
    text: Buffer.from(e.entry.payload).toString("utf8"),
    blockHash: e.blockHash,
  };
}

/** Renders one inbox message for display. Only MESSAGE_KIND_TEXT carries a
 * JSON {text} payload (see A2AClient.sendMessage) — every other kind carries
 * a marshalled protobuf (TaskResult, Task, Thread, ...), so decoding those as
 * UTF-8 emits binary garbage into the UI. Summarize them from the envelope's
 * own correlation fields instead, which are already plain strings. */
function decodeInboxMessage(m: Record<string, unknown>) {
  const kind = String(m["kind"] ?? "");
  const raw = Buffer.from((m["payload"] as Uint8Array) ?? new Uint8Array());
  let text: string;
  if (kind === "MESSAGE_KIND_TEXT" || kind === "") {
    text = raw.toString("utf8");
    try {
      const parsed = JSON.parse(text) as { text?: string };
      if (typeof parsed.text === "string") text = parsed.text;
    } catch {
      // not JSON — fall through and show the raw text
    }
  } else {
    const label = kind.replace("MESSAGE_KIND_", "").toLowerCase().replace(/_/g, " ");
    const ref = m["taskId"] || m["threadId"];
    text = `[${label}]${ref ? ` ${String(ref).slice(0, 8)}` : ""} · ${raw.byteLength} bytes`;
  }
  return {
    id: m["id"],
    fromDid: m["fromDid"],
    toDid: m["toDid"],
    threadId: m["threadId"],
    taskId: m["taskId"],
    kind: m["kind"],
    sentAt: m["sentAt"],
    text,
  };
}

app.listen(PORT, () => {
  console.log(`[webui] OpenMolt Network web UI on http://localhost:${PORT}`);
  console.log(`[webui] talking to daemon at ${AGENT_ADDR || "(default socket)"}`);
});
