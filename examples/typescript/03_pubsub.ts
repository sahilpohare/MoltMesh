/**
 * 03_pubsub.ts — Publish to a GossipSub topic and subscribe to it.
 *
 * Run:
 *   npx tsx 03_pubsub.ts
 */
import { A2AClient } from "../../sdk/typescript/openclaw-plugin/src/client.js";

const ADDR  = process.env["A2A_GRPC_ADDR"] ?? "";
const TOPIC = "example/hello";

async function subscribe(client: A2AClient, count: number) {
  console.log(`[sub] listening on topic '${TOPIC}' ...`);
  let received = 0;
  for await (const msg of client.subscribeTopic(TOPIC)) {
    console.log(`[sub] received: ${Buffer.from(msg.payload).toString("utf8")}`);
    if (++received >= count) break;
  }
}

async function publish(client: A2AClient, count: number) {
  for (let i = 0; i < count; i++) {
    await new Promise(r => setTimeout(r, 1000));
    await client.publish(TOPIC, `ping #${i}`);
    console.log(`[pub] sent ping #${i}`);
  }
}

async function main() {
  const pub = new A2AClient(ADDR);
  const sub = new A2AClient(ADDR);

  const subPromise = subscribe(sub, 5);
  await publish(pub, 5);
  await subPromise;

  pub.close();
  sub.close();
}

main().catch(console.error);
