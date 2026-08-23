/**
 * 07_master_agent.ts — A Groq-powered agent that advertises a skill on the
 * MoltMesh network and does real work when someone delegates a task to it.
 *
 * This is the "master agent" half of a discover-and-call demo: it publishes
 * a signed Agent Card advertising a2a:v1:cap:text-generation, then sits on
 * a durable, leased task subscription (DurableWorker) waiting for work. Any
 * peer on the network — including Claude Code, via the moltmesh MCP plugin
 * (sdk/typescript/openclaw-plugin) — can find it with FindAgents and hand
 * it a task with CreateTask.
 *
 * The skill itself calls the real Groq API, so a delegated task actually
 * gets a model-generated answer back, not a canned response.
 *
 * Run (own terminal, own daemon, key never touches the coordinating agent):
 *
 *   AGENT_ADDR=127.0.0.1:15702 \
 *   GROQ_API_KEY=gsk_... \
 *   npx tsx 07_master_agent.ts
 *
 * Start the daemon it points at first:
 *
 *   ./bin/moltmesh start --data-dir ~/.moltmesh-master --grpc-addr 127.0.0.1:15702 --port 15712
 */
import Groq from "groq-sdk";
import { A2AClient, type Artifact, AgentIdentity } from "../../sdk/typescript/openclaw-plugin/src/client.js";

const AGENT_ADDR = process.env["AGENT_ADDR"] ?? "";
const SKILL = process.env["SKILL"] ?? "a2a:v1:cap:text-generation";
const AGENT_NAME = process.env["AGENT_NAME"] ?? "master-agent";
const MODEL = process.env["GROQ_MODEL"] ?? "openai/gpt-oss-120b";

if (!process.env["GROQ_API_KEY"]) {
  console.error("GROQ_API_KEY is required — export it in this shell, never paste it into a chat.");
  process.exit(1);
}

const groq = new Groq();

async function main() {
  // Session-scoped RPCs (subscribeTasks, claimTask, ...) require an SDK-side
  // AgentIdentity distinct from the daemon's own libp2p identity (ADR-0020) —
  // without one, A2AClient never attaches session metadata, so the daemon
  // rejects SubscribeTasks and DurableWorker spins reconnecting forever.
  const identity = AgentIdentity.loadOrCreate();
  const client = new A2AClient(AGENT_ADDR, { identity });
  const me = await client.getIdentity();
  console.log(`[master-agent] DID: ${me.did}`);

  await client.publishAgentCard({
    name: AGENT_NAME,
    description: "Groq-powered text-generation worker, discoverable over MoltMesh",
    skills: [{ id: SKILL, name: "text-generation" }],
    multiaddrs: (me as { multiaddrs?: string[] }).multiaddrs ?? [],
  });
  console.log(`[master-agent] Advertised ${SKILL} on the DHT — waiting for tasks...`);

  const worker = client.worker([SKILL], { concurrency: 1, leaseSeconds: 60 });

  worker.handle(SKILL, async (task) => {
    const meta = (task["metadata"] ?? {}) as Record<string, string>;
    const prompt = meta["prompt"] ?? "Say hello and explain what you are in one sentence.";
    const shortId = String(task["id"]).slice(0, 8);
    console.log(`[master-agent] task ${shortId} — prompt: ${prompt}`);

    const response = await groq.chat.completions.create({
      model: MODEL,
      max_tokens: 512,
      messages: [{ role: "user", content: prompt }],
    });
    const text = response.choices[0]?.message?.content ?? "";

    console.log(`[master-agent] task ${shortId} — done (${text.length} chars)`);

    const data = Buffer.from(text, "utf8");
    const cid = await client.storeBlob(data, { mimeType: "text/plain", filename: "response.txt" });
    const artifact: Artifact = { cid, data, mimeType: "text/plain", filename: "response.txt", size: String(data.byteLength) };
    return [artifact];
  });

  const controller = new AbortController();
  process.on("SIGINT", () => controller.abort());
  await worker.run(controller.signal);
}

main().catch((err) => {
  console.error("[master-agent] fatal:", err);
  process.exit(1);
});
