/**
 * MCP bridge used by the MoltMesh Claude Code plugin.
 *
 * It deliberately communicates over stdio: Claude Code owns the process and
 * the only network connection is the local MoltMesh gRPC client.
 */
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import { A2AClient, defaultAddr, type Obj, type Task } from "./client.js";

const client = new A2AClient(process.env["A2A_GRPC_ADDR"] || defaultAddr());
const server = new McpServer({ name: "moltmesh", version: "0.1.0" });

function json(value: unknown) {
  return { content: [{ type: "text" as const, text: JSON.stringify(value, null, 2) }] };
}

function taskSummary(task: Task) {
  return { id: task.id, status: task.status, skill: task.skill, assignee: task.assignee, error: task.error ?? "" };
}

server.tool("moltmesh_identity", "Get the local MoltMesh daemon identity and reachable addresses.", {}, async () => json(await client.getIdentity()));

server.tool("moltmesh_find_agents", "Find MoltMesh agents advertising a capability.", {
  capability: z.string().min(1).describe("Capability ID, for example a2a:v1:cap:text-generation"),
  limit: z.number().int().min(1).max(100).optional().describe("Maximum results; default 5"),
}, async ({ capability, limit }) => json(await client.findAgents(capability, limit ?? 5)));

server.tool("moltmesh_send_message", "Send a text message to a MoltMesh agent. Offline delivery is queued.", {
  toDid: z.string().min(1).describe("Recipient DID"),
  message: z.string().min(1).describe("Message text"),
  threadId: z.string().optional().describe("Optional thread ID"),
}, async ({ toDid, message, threadId }) => json(await client.sendMessage(toDid, message, { threadId })));

server.tool("moltmesh_get_inbox", "Read recent messages received by this daemon.", {
  threadId: z.string().optional(),
  taskId: z.string().optional(),
  unreadOnly: z.boolean().optional(),
  limit: z.number().int().min(1).max(100).optional(),
}, async args => json(await client.getInbox(args)));

server.tool("moltmesh_create_task", "Delegate a task to another MoltMesh agent.", {
  toDid: z.string().min(1).describe("Assignee DID"),
  skill: z.string().min(1).describe("Capability required for the task"),
  threadId: z.string().optional(),
  metadata: z.record(z.string()).optional().describe("Small string metadata for the assignee"),
}, async ({ toDid, skill, threadId, metadata }) => json(taskSummary(await client.createTask(toDid, skill, { threadId, metadata }))));

server.tool("moltmesh_get_task", "Get the current state of a delegated task.", {
  taskId: z.string().min(1),
}, async ({ taskId }) => json(taskSummary(await client.getTask(taskId))));

server.tool("moltmesh_wait_task", "Wait for a task to complete, fail, or be cancelled.", {
  taskId: z.string().min(1),
  timeoutMs: z.number().int().min(1_000).max(300_000).optional().describe("Wait limit; default 60 seconds"),
}, async ({ taskId, timeoutMs }) => json(taskSummary(await client.waitTask(taskId, { timeoutMs }))));

server.tool("moltmesh_create_thread", "Create a replicated MoltMesh thread.", {
  replicaDids: z.array(z.string().min(1)).describe("Replica DIDs"),
  faultTolerance: z.number().int().min(0).max(10).optional().describe("Fault tolerance f; default 0"),
}, async ({ replicaDids, faultTolerance }) => json(await client.createThread(replicaDids, { f: faultTolerance })));

server.tool("moltmesh_append_thread_entry", "Append UTF-8 text to a thread.", {
  threadId: z.string().min(1),
  text: z.string(),
  kind: z.string().optional().describe("Application entry kind; default message"),
}, async ({ threadId, text, kind }) => {
  await client.appendEntry(threadId, Buffer.from(text), { kind });
  return json({ appended: true, threadId });
});

server.tool("moltmesh_read_thread", "Read committed thread entries from a height.", {
  threadId: z.string().min(1),
  sinceHeight: z.number().int().min(0).optional(),
  limit: z.number().int().min(1).max(1_000).optional(),
}, async ({ threadId, sinceHeight, limit }) => json((await client.getThreadEntries(threadId, { sinceHeight, limit })).map(entry => ({
  ...entry,
  entry: { ...entry.entry, payload: Buffer.from(entry.entry.payload).toString("utf8") },
}))));

server.tool("moltmesh_health", "Return daemon health and peer connectivity diagnostics.", {}, async () => {
  const [health, peers] = await Promise.all([client.health(), client.listPeers()]);
  return json({ health, peers });
});

const transport = new StdioServerTransport();
await server.connect(transport);

async function close() {
  client.close();
  await server.close();
}
process.once("SIGINT", () => { void close(); });
process.once("SIGTERM", () => { void close(); });
