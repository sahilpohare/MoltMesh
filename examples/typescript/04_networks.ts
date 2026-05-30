/**
 * 04_networks.ts — Create a network, join it from a second agent, broadcast.
 *
 * Run:
 *   AGENT_B_ADDR=unix://$HOME/.moltmesh/agent_b.sock npx tsx 04_networks.ts
 */
import { A2AClient } from "../../sdk/typescript/openclaw-plugin/src/client.js";

const AGENT_A_ADDR = process.env["AGENT_A_ADDR"] ?? "";
const AGENT_B_ADDR = process.env["AGENT_B_ADDR"] ?? `unix://${process.env["HOME"]}/.moltmesh/agent_b.sock`;

async function listenOne(client: A2AClient, networkId: string): Promise<void> {
  console.log(`[agent_b] subscribing to network ${networkId.slice(0, 8)}...`);
  for await (const msg of client.subscribeNetwork(networkId)) {
    console.log(`[agent_b] broadcast: ${Buffer.from(msg.payload).toString("utf8")}`);
    break;
  }
}

async function main() {
  const a = new A2AClient(AGENT_A_ADDR);
  const b = new A2AClient(AGENT_B_ADDR);

  const net = await a.createNetwork("demo-group");
  console.log(`Network created: ${net.id}  name=${net.name}`);

  await b.joinNetwork(net.id);
  console.log(`Agent B joined network ${net.id.slice(0, 8)}`);

  const listenPromise = listenOne(b, net.id);

  await a.broadcastNetwork(net.id, "Hello network!");
  console.log(`[agent_a] broadcast sent`);

  await listenPromise;

  const netsB = await b.listNetworks();
  console.log(`\nAgent B networks: ${netsB.map(n => n.name).join(", ")}`);

  a.close();
  b.close();
}

main().catch(console.error);
