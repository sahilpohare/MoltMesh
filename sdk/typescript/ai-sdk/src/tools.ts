/**
 * moltmesh-ai-sdk/tools — Vercel AI SDK tool() wrappers for the MoltMesh daemon.
 *
 * Usage:
 *   import { createMoltMeshTools } from "moltmesh-ai-sdk";
 *   import { generateText } from "ai";
 *
 *   const tools = createMoltMeshTools();        // uses A2A_GRPC_ADDR or default socket
 *   // or: createMoltMeshTools({ addr: "localhost:50051" })
 *
 *   const { text } = await generateText({
 *     model: ...,
 *     tools,
 *     prompt: "Find agents that do text-generation and send one a message.",
 *   });
 */

import { tool } from "ai";
import { z } from "zod";
import { A2AClient } from "./client.js";

export interface MoltMeshToolsOptions {
  /** gRPC address of the daemon. Defaults to A2A_GRPC_ADDR env var or the default Unix socket. */
  addr?: string;
  /** Pre-constructed A2AClient (useful for testing). */
  client?: A2AClient;
}

/**
 * Returns a record of AI SDK tools keyed by tool name.
 * Pass the result directly to `generateText`, `streamText`, etc. as `tools`.
 */
export function createMoltMeshTools(opts: MoltMeshToolsOptions = {}) {
  const client = opts.client ?? new A2AClient(opts.addr);

  // ── identity ──────────────────────────────────────────────────────────────

  const p2p_get_identity = tool({
    description: "Return this daemon's DID, public key, and libp2p multiaddrs.",
    parameters: z.object({}),
    execute: async () => {
      const id = await client.getIdentity();
      return { did: id.did, publicKey: id.publicKey, multiaddrs: id.multiaddrs };
    },
  });

  // ── registry ──────────────────────────────────────────────────────────────

  const p2p_find_agents = tool({
    description: "Search the p2p network for agents that advertise a given capability. Returns a list of matching agent DIDs and names.",
    parameters: z.object({
      capability: z.string().describe("Capability ID, e.g. a2a:v1:cap:text-generation"),
      limit: z.number().optional().describe("Max results (default 5)"),
    }),
    execute: async ({ capability, limit }) => {
      const cards = await client.findAgents(capability, limit ?? 5);
      return cards.map(c => ({ did: c.did, name: c.name, capabilities: c.capabilities }));
    },
  });

  // ── messaging ─────────────────────────────────────────────────────────────

  const p2p_send_message = tool({
    description: "Send a text message to another agent identified by their DID. Queued if the peer is offline.",
    parameters: z.object({
      toDid:    z.string().describe("Recipient agent DID"),
      message:  z.string().describe("Text content to send"),
      threadId: z.string().optional().describe("Optional thread ID"),
    }),
    execute: async ({ toDid, message, threadId }) => {
      const r = await client.sendMessage(toDid, message, { threadId }) as Record<string, unknown>;
      return {
        messageId: r["messageId"] as string,
        queued: r["queued"] as boolean,
      };
    },
  });

  const p2p_get_inbox = tool({
    description: "Retrieve messages from this agent's inbox. Optionally filter by thread, task, or unread status.",
    parameters: z.object({
      threadId:   z.string().optional(),
      taskId:     z.string().optional(),
      unreadOnly: z.boolean().optional().describe("Return only unread messages"),
      limit:      z.number().optional().describe("Max messages (default 20)"),
    }),
    execute: async (p) => {
      const msgs = await client.getInbox({ ...p, limit: p.limit ?? 20 }) as Record<string, unknown>[];
      return msgs.map(m => ({
        id: m["id"] as string,
        fromDid: m["fromDid"] as string,
        kind: m["kind"] as string,
      }));
    },
  });

  // ── tasks ─────────────────────────────────────────────────────────────────

  const p2p_create_task = tool({
    description: "Delegate a task to another agent. Specify the assignee DID and the required capability. Returns the task ID and initial status.",
    parameters: z.object({
      toDid:    z.string().describe("Assignee agent DID"),
      skill:    z.string().describe("Capability ID, e.g. a2a:v1:cap:text-generation"),
      threadId: z.string().optional(),
      metadata: z.record(z.string()).optional().describe("Key-value metadata"),
    }),
    execute: async ({ toDid, skill, threadId, metadata }) => {
      const task = await client.createTask(toDid, skill, { threadId, metadata });
      return { id: task.id, status: task.status, skill: task.skill, assignee: task.assignee };
    },
  });

  const p2p_get_task = tool({
    description: "Retrieve the current status and details of a task by ID.",
    parameters: z.object({
      taskId: z.string().describe("Task ID to retrieve"),
    }),
    execute: async ({ taskId }) => {
      const task = await client.getTask(taskId);
      return {
        id: task.id,
        status: task.status,
        skill: task.skill,
        assignee: task.assignee,
        error: task.error,
        outputArtifacts: task.outputArtifacts?.map(a => ({
          cid: a.cid,
          mimeType: a.mimeType,
          size: a.size,
        })),
      };
    },
  });

  const p2p_wait_task = tool({
    description: "Block until a task completes, fails, or is cancelled. Returns the final task state.",
    parameters: z.object({
      taskId:    z.string(),
      timeoutMs: z.number().optional().describe("Timeout in milliseconds (default 60000)"),
    }),
    execute: async ({ taskId, timeoutMs }) => {
      const task = await client.waitTask(taskId, { timeoutMs });
      return {
        id: task.id,
        status: task.status,
        skill: task.skill,
        assignee: task.assignee,
        error: task.error,
      };
    },
  });

  const p2p_cancel_task = tool({
    description: "Cancel a pending or in-progress task.",
    parameters: z.object({
      taskId: z.string(),
    }),
    execute: async ({ taskId }) => {
      const task = await client.cancelTask(taskId);
      return { id: task.id, status: task.status };
    },
  });

  // ── diagnostics ───────────────────────────────────────────────────────────

  const p2p_health = tool({
    description: "Return the daemon's health: version, DID, connected peer count, and uptime.",
    parameters: z.object({}),
    execute: async () => {
      const h = await client.health();
      return { version: h.version, did: h.did, peerCount: h.peerCount, uptimeSecs: h.uptimeSecs };
    },
  });

  const p2p_ping = tool({
    description: "Measure round-trip latency to a peer by DID. Leave did empty for a loopback ping.",
    parameters: z.object({
      did: z.string().optional().describe("Peer DID (empty = loopback)"),
    }),
    execute: async ({ did }) => {
      const r = await client.ping(did ?? "");
      return { did: r.did, reachable: r.reachable, latencyMs: r.latencyMs, error: r.error };
    },
  });

  // ── pub/sub ───────────────────────────────────────────────────────────────

  const p2p_publish = tool({
    description: "Publish a UTF-8 message to a GossipSub topic on the mesh.",
    parameters: z.object({
      topic:   z.string().describe("GossipSub topic name"),
      payload: z.string().describe("UTF-8 text payload"),
    }),
    execute: async ({ topic, payload }) => {
      await client.publish(topic, payload);
      return { topic, published: true };
    },
  });

  // ── webhooks ──────────────────────────────────────────────────────────────

  const p2p_set_webhook = tool({
    description: "Configure a webhook URL so external processes receive daemon events via HTTP POST.",
    parameters: z.object({
      url:    z.string().describe("HTTP endpoint to receive events"),
      secret: z.string().optional().describe("Shared secret sent in X-MoltMesh-Secret header"),
    }),
    execute: async ({ url, secret }) => {
      const configured = await client.setWebhook(url, secret ?? "");
      return { url: configured };
    },
  });

  const p2p_get_webhook = tool({
    description: "Return the currently configured webhook URL.",
    parameters: z.object({}),
    execute: async () => {
      const url = await client.getWebhook();
      return { url: url || null };
    },
  });

  const p2p_clear_webhook = tool({
    description: "Remove the configured webhook.",
    parameters: z.object({}),
    execute: async () => {
      await client.clearWebhook();
      return { cleared: true };
    },
  });

  // ── networks ──────────────────────────────────────────────────────────────

  const p2p_network_create = tool({
    description: "Create a named agent group. The creator is automatically a member. Returns the network ID.",
    parameters: z.object({
      name: z.string().describe("Human-readable network name"),
    }),
    execute: async ({ name }) => {
      const net = await client.createNetwork(name);
      return { id: net.id, name: net.name, creatorDid: net.creatorDid };
    },
  });

  const p2p_network_join = tool({
    description: "Join an existing network by its ID.",
    parameters: z.object({
      networkId: z.string().describe("Network UUID"),
    }),
    execute: async ({ networkId }) => {
      const net = await client.joinNetwork(networkId);
      return { id: net.id, name: net.name };
    },
  });

  const p2p_network_leave = tool({
    description: "Leave a network.",
    parameters: z.object({
      networkId: z.string().describe("Network UUID"),
    }),
    execute: async ({ networkId }) => {
      await client.leaveNetwork(networkId);
      return { left: networkId };
    },
  });

  const p2p_network_list = tool({
    description: "List all networks this agent belongs to.",
    parameters: z.object({}),
    execute: async () => {
      const nets = await client.listNetworks();
      return nets.map(n => ({ id: n.id, name: n.name }));
    },
  });

  const p2p_network_broadcast = tool({
    description: "Broadcast a message to all members of a network via GossipSub.",
    parameters: z.object({
      networkId: z.string().describe("Network UUID"),
      payload:   z.string().describe("UTF-8 text to broadcast"),
    }),
    execute: async ({ networkId, payload }) => {
      await client.broadcastNetwork(networkId, payload);
      return { networkId, sent: true };
    },
  });

  return {
    p2p_get_identity,
    p2p_find_agents,
    p2p_send_message,
    p2p_get_inbox,
    p2p_create_task,
    p2p_get_task,
    p2p_wait_task,
    p2p_cancel_task,
    p2p_health,
    p2p_ping,
    p2p_publish,
    p2p_set_webhook,
    p2p_get_webhook,
    p2p_clear_webhook,
    p2p_network_create,
    p2p_network_join,
    p2p_network_leave,
    p2p_network_list,
    p2p_network_broadcast,
  } as const;
}

export type MoltMeshTools = ReturnType<typeof createMoltMeshTools>;
