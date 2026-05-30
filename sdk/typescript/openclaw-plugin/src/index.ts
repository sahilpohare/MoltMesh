/**
 * openclaw-plugin-moltmesh
 *
 * OpenClaw plugin that exposes MoltMesh daemon capabilities as agent tools:
 *   - p2p_get_identity      — get this daemon's DID and multiaddrs
 *   - p2p_send_message      — send a text message to another agent
 *   - p2p_get_inbox         — read messages from the inbox
 *   - p2p_create_task       — delegate a task to another agent
 *   - p2p_get_task          — poll task status by ID
 *   - p2p_cancel_task       — cancel an in-progress task
 *   - p2p_find_agents       — discover agents by capability
 *
 * Configuration (set in OpenClaw plugin config UI):
 *   grpcAddr  — daemon gRPC address (default: env A2A_GRPC_ADDR or unix socket)
 *
 * Install:
 *   openclaw plugins install local:/path/to/openclaw-plugin-moltmesh
 */

import { Type } from "@sinclair/typebox";
import { A2AClient, defaultAddr, type Task, type Obj, type NetworkInfo } from "./client.js";

// ── plugin config schema ──────────────────────────────────────────────────────

const ConfigSchema = Type.Object({
  grpcAddr: Type.String({
    default: "",
    description: "MoltMesh daemon gRPC address. Leave empty to use A2A_GRPC_ADDR env var or the default Unix socket.",
  }),
});

// ── client cache — one per addr ───────────────────────────────────────────────

const clients = new Map<string, A2AClient>();

function getClient(config: { grpcAddr?: string }): A2AClient {
  const addr = config.grpcAddr || defaultAddr();
  if (!clients.has(addr)) clients.set(addr, new A2AClient(addr));
  return clients.get(addr)!;
}

// ── formatting helpers ────────────────────────────────────────────────────────

function fmtNetwork(net: NetworkInfo): string {
  return `${net.id.slice(0, 8)}  ${net.name}  (creator: ${net.creatorDid.slice(0, 16)}...)`;
}

function fmtTask(task: Task): string {
  const lines = [
    `Task ${task.id}`,
    `  Status:   ${task.status}`,
    `  Skill:    ${task.skill}`,
    `  Assignee: ${task.assignee}`,
  ];
  if (task.error) lines.push(`  Error:    ${task.error}`);
  for (const a of task.outputArtifacts ?? []) {
    lines.push(`  Artifact: ${a.cid || "(inline)"}  ${a.mimeType}  ${a.size} bytes`);
  }
  return lines.join("\n");
}

function text(t: string) { return { content: [{ type: "text", text: t }] }; }

// ── plugin ────────────────────────────────────────────────────────────────────

export default {
  id: "moltmesh",
  name: "moltmesh",
  description: "MoltMesh network — messages, tasks, threads, blobs, agent discovery.",
  configSchema: ConfigSchema,

  register(api: { registerTool: (tool: unknown) => void }) {
    const C = (cfg: { grpcAddr?: string }) => getClient(cfg);

    // ── identity ──────────────────────────────────────────────────────────

    api.registerTool({
      name: "p2p_get_identity",
      description: "Return this daemon's DID, public key, and libp2p multiaddrs.",
      parameters: Type.Object({}),
      async execute(_: string, _p: unknown, cfg: { grpcAddr?: string }) {
        const id = await C(cfg).getIdentity();
        return text(JSON.stringify(id, null, 2));
      },
    });

    // ── messaging ─────────────────────────────────────────────────────────

    api.registerTool({
      name: "p2p_send_message",
      description: "Send a text message to another agent. Queued if peer is offline.",
      parameters: Type.Object({
        toDid:    Type.String({ description: "Recipient DID" }),
        message:  Type.String({ description: "Text to send" }),
        threadId: Type.Optional(Type.String({ description: "Thread ID" })),
      }),
      async execute(_: string, p: { toDid: string; message: string; threadId?: string }, cfg: { grpcAddr?: string }) {
        const r = await C(cfg).sendMessage(p.toDid, p.message, { threadId: p.threadId });
        const queued = (r as Obj)["queued"] ? " (queued)" : "";
        return text(`Sent. ID: ${(r as Obj)["messageId"]}${queued}`);
      },
    });

    api.registerTool({
      name: "p2p_get_inbox",
      description: "Fetch messages from inbox. Filter by thread/task/unread.",
      parameters: Type.Object({
        threadId:  Type.Optional(Type.String()),
        taskId:    Type.Optional(Type.String()),
        unreadOnly: Type.Optional(Type.Boolean()),
        limit:     Type.Optional(Type.Number()),
      }),
      async execute(_: string, p: { threadId?: string; taskId?: string; unreadOnly?: boolean; limit?: number }, cfg: { grpcAddr?: string }) {
        const msgs = await C(cfg).getInbox(p);
        if (!msgs.length) return text("Inbox empty.");
        const lines = [`${msgs.length} message(s):`];
        for (const m of msgs as Obj[]) {
          lines.push(`  [${String(m["id"]).slice(0, 8)}] from=${m["fromDid"]}  kind=${m["kind"]}`);
        }
        return text(lines.join("\n"));
      },
    });

    // ── agents ────────────────────────────────────────────────────────────

    api.registerTool({
      name: "p2p_find_agents",
      description: "Find agents by capability ID. Returns DIDs and names.",
      parameters: Type.Object({
        capability: Type.String({ description: "e.g. a2a:v1:cap:text-generation" }),
        limit: Type.Optional(Type.Number()),
      }),
      async execute(_: string, p: { capability: string; limit?: number }, cfg: { grpcAddr?: string }) {
        const cards = await C(cfg).findAgents(p.capability, p.limit ?? 5);
        if (!cards.length) return text(`No agents for: ${p.capability}`);
        return text([
          `${cards.length} agent(s) for '${p.capability}':`,
          ...cards.map(c => `  ${c.did}  (${c.name || "unnamed"})`),
        ].join("\n"));
      },
    });

    // ── tasks ─────────────────────────────────────────────────────────────

    api.registerTool({
      name: "p2p_create_task",
      description: "Delegate a task to another agent. Returns task ID and status.",
      parameters: Type.Object({
        toDid:    Type.String({ description: "Assignee DID" }),
        skill:    Type.String({ description: "Capability ID (e.g. a2a:v1:cap:text-generation)" }),
        threadId: Type.Optional(Type.String()),
        metadata: Type.Optional(Type.Record(Type.String(), Type.String())),
      }),
      async execute(_: string, p: { toDid: string; skill: string; threadId?: string; metadata?: Record<string, string> }, cfg: { grpcAddr?: string }) {
        const task = await C(cfg).createTask(p.toDid, p.skill, { threadId: p.threadId, metadata: p.metadata });
        return text(fmtTask(task));
      },
    });

    api.registerTool({
      name: "p2p_get_task",
      description: "Get task status by ID.",
      parameters: Type.Object({ taskId: Type.String() }),
      async execute(_: string, p: { taskId: string }, cfg: { grpcAddr?: string }) {
        return text(fmtTask(await C(cfg).getTask(p.taskId)));
      },
    });

    api.registerTool({
      name: "p2p_wait_task",
      description: "Block until a task completes, fails, or is cancelled. Returns final state.",
      parameters: Type.Object({
        taskId:    Type.String(),
        timeoutMs: Type.Optional(Type.Number({ description: "Timeout in ms (default 60000)" })),
      }),
      async execute(_: string, p: { taskId: string; timeoutMs?: number }, cfg: { grpcAddr?: string }) {
        const task = await C(cfg).waitTask(p.taskId, { timeoutMs: p.timeoutMs });
        return text(fmtTask(task));
      },
    });

    api.registerTool({
      name: "p2p_cancel_task",
      description: "Cancel a pending or in-progress task.",
      parameters: Type.Object({ taskId: Type.String() }),
      async execute(_: string, p: { taskId: string }, cfg: { grpcAddr?: string }) {
        const task = await C(cfg).cancelTask(p.taskId);
        return text(`Task ${task.id} cancelled (${task.status})`);
      },
    });

    // ── blobs ─────────────────────────────────────────────────────────────

    api.registerTool({
      name: "p2p_store_blob",
      description: "Store bytes in the blob store. Returns CID (SHA-256).",
      parameters: Type.Object({
        data:     Type.String({ description: "Base64-encoded bytes to store" }),
        mimeType: Type.Optional(Type.String()),
        filename: Type.Optional(Type.String()),
      }),
      async execute(_: string, p: { data: string; mimeType?: string; filename?: string }, cfg: { grpcAddr?: string }) {
        const cid = await C(cfg).storeBlob(Buffer.from(p.data, "base64"), {
          mimeType: p.mimeType,
          filename: p.filename,
        });
        return text(`CID: ${cid}`);
      },
    });

    api.registerTool({
      name: "p2p_fetch_blob",
      description: "Fetch a blob by CID. Returns base64-encoded bytes.",
      parameters: Type.Object({ cid: Type.String() }),
      async execute(_: string, p: { cid: string }, cfg: { grpcAddr?: string }) {
        const data = await C(cfg).fetchBlob(p.cid);
        return text(Buffer.from(data).toString("base64"));
      },
    });

    // ── threads ───────────────────────────────────────────────────────────

    api.registerTool({
      name: "p2p_create_thread",
      description: "Create a replicated ordered log. f=0 = single-node Raft (fastest).",
      parameters: Type.Object({
        replicaDids: Type.Array(Type.String(), { description: "Validator DIDs (include your own)" }),
        f:           Type.Optional(Type.Number({ description: "Fault tolerance (default 0)" })),
        backend:     Type.Optional(Type.String({ description: "'raft' (default) or 'tendermint'" })),
      }),
      async execute(_: string, p: { replicaDids: string[]; f?: number; backend?: "raft" | "tendermint" }, cfg: { grpcAddr?: string }) {
        const thread = await C(cfg).createThread(p.replicaDids, { f: p.f, backend: p.backend });
        return text(`Thread created.\n  ID: ${thread.id}\n  N: ${thread.n}  F: ${thread.f}\n  Replicas: ${thread.replicaDids.join(", ")}`);
      },
    });

    api.registerTool({
      name: "p2p_append_entry",
      description: "Append an entry to a thread. Committed asynchronously by consensus.",
      parameters: Type.Object({
        threadId: Type.String(),
        payload:  Type.String({ description: "UTF-8 text payload" }),
        kind:     Type.Optional(Type.String({ description: "Entry kind tag (default 'message')" })),
      }),
      async execute(_: string, p: { threadId: string; payload: string; kind?: string }, cfg: { grpcAddr?: string }) {
        await C(cfg).appendEntry(p.threadId, Buffer.from(p.payload), { kind: p.kind });
        return text("Entry enqueued.");
      },
    });

    api.registerTool({
      name: "p2p_get_thread_entries",
      description: "Read committed entries from a thread since a given block height.",
      parameters: Type.Object({
        threadId:    Type.String(),
        sinceHeight: Type.Optional(Type.Number({ description: "Return entries after this height (default 0 = all)" })),
        limit:       Type.Optional(Type.Number()),
      }),
      async execute(_: string, p: { threadId: string; sinceHeight?: number; limit?: number }, cfg: { grpcAddr?: string }) {
        const entries = await C(cfg).getThreadEntries(p.threadId, { sinceHeight: p.sinceHeight, limit: p.limit });
        if (!entries.length) return text("No committed entries.");
        const lines = entries.map(e =>
          `  [h=${e.height} i=${e.index}] ${e.entry.kind}: ${Buffer.from(e.entry.payload).toString("utf8").slice(0, 80)}`
        );
        return text(`${entries.length} entry/entries:\n${lines.join("\n")}`);
      },
    });

    // ── diagnostics ───────────────────────────────────────────────────────

    api.registerTool({
      name: "p2p_health",
      description: "Return the daemon's health: version, DID, peer count, uptime.",
      parameters: Type.Object({}),
      async execute(_: string, _p: unknown, cfg: { grpcAddr?: string }) {
        const h = await C(cfg).health();
        return text(
          `Daemon health:\n  Version: ${h.version}\n  DID:     ${h.did}\n  Peers:   ${h.peerCount}\n  Uptime:  ${h.uptimeSecs.toFixed(1)}s`
        );
      },
    });

    api.registerTool({
      name: "p2p_ping",
      description: "Measure round-trip latency to a peer by DID. Empty DID = loopback.",
      parameters: Type.Object({
        did: Type.Optional(Type.String({ description: "Peer DID (empty = loopback)" })),
      }),
      async execute(_: string, p: { did?: string }, cfg: { grpcAddr?: string }) {
        const r = await C(cfg).ping(p.did ?? "");
        if (!r.reachable) return text(`Peer ${p.did || "loopback"} unreachable: ${r.error}`);
        return text(`Ping ${r.did || "loopback"}: ${r.latencyMs.toFixed(1)}ms`);
      },
    });

    // ── pub/sub ───────────────────────────────────────────────────────────

    api.registerTool({
      name: "p2p_publish",
      description: "Publish a UTF-8 message to a GossipSub topic on the mesh.",
      parameters: Type.Object({
        topic:   Type.String({ description: "GossipSub topic name" }),
        payload: Type.String({ description: "UTF-8 text payload" }),
      }),
      async execute(_: string, p: { topic: string; payload: string }, cfg: { grpcAddr?: string }) {
        await C(cfg).publish(p.topic, p.payload);
        return text(`Published to topic '${p.topic}'.`);
      },
    });

    // ── webhooks ──────────────────────────────────────────────────────────

    api.registerTool({
      name: "p2p_set_webhook",
      description: "Configure a webhook URL to receive daemon events via HTTP POST.",
      parameters: Type.Object({
        url:    Type.String({ description: "HTTP endpoint URL" }),
        secret: Type.Optional(Type.String({ description: "Shared secret for X-MoltMesh-Secret header" })),
      }),
      async execute(_: string, p: { url: string; secret?: string }, cfg: { grpcAddr?: string }) {
        const configured = await C(cfg).setWebhook(p.url, p.secret ?? "");
        return text(`Webhook configured: ${configured}`);
      },
    });

    api.registerTool({
      name: "p2p_get_webhook",
      description: "Return the currently configured webhook URL.",
      parameters: Type.Object({}),
      async execute(_: string, _p: unknown, cfg: { grpcAddr?: string }) {
        const url = await C(cfg).getWebhook();
        return text(url ? `Webhook: ${url}` : "No webhook configured.");
      },
    });

    api.registerTool({
      name: "p2p_clear_webhook",
      description: "Remove the configured webhook.",
      parameters: Type.Object({}),
      async execute(_: string, _p: unknown, cfg: { grpcAddr?: string }) {
        await C(cfg).clearWebhook();
        return text("Webhook cleared.");
      },
    });

    // ── networks ──────────────────────────────────────────────────────────

    api.registerTool({
      name: "p2p_network_create",
      description: "Create a named agent group. The creator is automatically a member.",
      parameters: Type.Object({
        name: Type.String({ description: "Human-readable network name" }),
      }),
      async execute(_: string, p: { name: string }, cfg: { grpcAddr?: string }) {
        const net = await C(cfg).createNetwork(p.name);
        return text(`Network created.\n  ID:   ${net.id}\n  Name: ${net.name}`);
      },
    });

    api.registerTool({
      name: "p2p_network_join",
      description: "Join an existing named network by its ID.",
      parameters: Type.Object({
        networkId: Type.String({ description: "Network UUID" }),
      }),
      async execute(_: string, p: { networkId: string }, cfg: { grpcAddr?: string }) {
        const net = await C(cfg).joinNetwork(p.networkId);
        return text(`Joined network '${net.name}' (${net.id}).`);
      },
    });

    api.registerTool({
      name: "p2p_network_leave",
      description: "Leave a network.",
      parameters: Type.Object({
        networkId: Type.String({ description: "Network UUID" }),
      }),
      async execute(_: string, p: { networkId: string }, cfg: { grpcAddr?: string }) {
        await C(cfg).leaveNetwork(p.networkId);
        return text(`Left network ${p.networkId}.`);
      },
    });

    api.registerTool({
      name: "p2p_network_list",
      description: "List all networks this agent belongs to.",
      parameters: Type.Object({}),
      async execute(_: string, _p: unknown, cfg: { grpcAddr?: string }) {
        const nets = await C(cfg).listNetworks();
        if (!nets.length) return text("Not a member of any network.");
        return text([`${nets.length} network(s):`, ...nets.map(n => `  ${fmtNetwork(n)}`)].join("\n"));
      },
    });

    api.registerTool({
      name: "p2p_network_broadcast",
      description: "Broadcast a UTF-8 message to all members of a network via GossipSub.",
      parameters: Type.Object({
        networkId: Type.String({ description: "Network UUID" }),
        payload:   Type.String({ description: "UTF-8 text to broadcast" }),
      }),
      async execute(_: string, p: { networkId: string; payload: string }, cfg: { grpcAddr?: string }) {
        await C(cfg).broadcastNetwork(p.networkId, p.payload);
        return text(`Broadcast sent to network ${p.networkId}.`);
      },
    });
  },
};
