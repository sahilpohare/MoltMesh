/**
 * 01_basic_messaging.ts — Send a message and read the inbox.
 *
 * Two daemons required (or use one with loopback):
 *   moltmesh-daemon &
 *   A2A_GRPC_ADDR=unix://$HOME/.moltmesh/agent_b.sock moltmesh-daemon &
 *
 * Run:
 *   npx tsx 01_basic_messaging.ts
 */
import { A2AClient } from "../../sdk/typescript/openclaw-plugin/src/client.js";

const AGENT_A_ADDR = process.env["AGENT_A_ADDR"] ?? "";
const AGENT_B_ADDR = process.env["AGENT_B_ADDR"] ?? `unix://${process.env["HOME"]}/.moltmesh/agent_b.sock`;

async function main() {
  const a = new A2AClient(AGENT_A_ADDR);
  const b = new A2AClient(AGENT_B_ADDR);

  const idB = await b.getIdentity();
  console.log(`Agent B DID: ${idB.did}`);

  const result = await a.sendMessage(idB.did, "Hello from agent A!");
  console.log(`Sent message ID: ${(result as Record<string,unknown>)["messageId"]}  queued=${(result as Record<string,unknown>)["queued"]}`);

  const msgs = await b.getInbox({ limit: 5 });
  console.log(`\nAgent B inbox (${msgs.length} message(s)):`);
  for (const m of msgs as Record<string, unknown>[]) {
    console.log(`  [${String(m["id"]).slice(0, 8)}] from=${m["fromDid"]}  kind=${m["kind"]}`);
  }

  a.close();
  b.close();
}

main().catch(console.error);
