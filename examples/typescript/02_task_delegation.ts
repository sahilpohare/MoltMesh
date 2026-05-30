/**
 * 02_task_delegation.ts — Create a task and wait for it to complete.
 *
 * Run:
 *   AGENT_B_ADDR=unix://$HOME/.moltmesh/agent_b.sock npx tsx 02_task_delegation.ts
 */
import { A2AClient } from "../../sdk/typescript/openclaw-plugin/src/client.js";

const AGENT_A_ADDR = process.env["AGENT_A_ADDR"] ?? "";
const AGENT_B_ADDR = process.env["AGENT_B_ADDR"] ?? `unix://${process.env["HOME"]}/.moltmesh/agent_b.sock`;
const SKILL        = process.env["SKILL"]        ?? "a2a:v1:cap:text-generation";

async function main() {
  const coordinator = new A2AClient(AGENT_A_ADDR);
  const worker      = new A2AClient(AGENT_B_ADDR);

  const workerDid = (await worker.getIdentity()).did;
  console.log(`Worker DID: ${workerDid}`);

  const task = await coordinator.createTask(workerDid, SKILL, {
    metadata: { prompt: "Summarise MoltMesh in one sentence." },
  });
  console.log(`Task created: ${task.id}  status=${task.status}`);

  const final = await coordinator.waitTask(task.id, { timeoutMs: 30_000 });
  console.log(`\nTask finished: ${final.status}`);
  if (final.error) console.log(`  Error: ${final.error}`);
  for (const a of final.outputArtifacts ?? []) {
    console.log(`  Artifact: ${a.cid} (${a.mimeType}, ${a.size} bytes)`);
  }

  coordinator.close();
  worker.close();
}

main().catch(console.error);
