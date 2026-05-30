/**
 * 05_diagnostics.ts — Health check, peer list, and ping.
 *
 * Run:
 *   npx tsx 05_diagnostics.ts
 */
import { A2AClient } from "../../sdk/typescript/openclaw-plugin/src/client.js";

const ADDR = process.env["A2A_GRPC_ADDR"] ?? "";

async function main() {
  const client = new A2AClient(ADDR);

  const health = await client.health();
  console.log("Daemon health:");
  console.log(`  Version: ${health.version}`);
  console.log(`  DID:     ${health.did}`);
  console.log(`  Peers:   ${health.peerCount}`);
  console.log(`  Uptime:  ${health.uptimeSecs.toFixed(1)}s`);

  const peers = await client.listPeers();
  if (peers.length) {
    console.log(`\nKnown peers (${peers.length}):`);
    for (const p of peers) {
      console.log(`  ${p.peerId.slice(0, 12)}...  did=${p.did || "(none)"}`);
    }

    const first = peers[0];
    const ping = await client.ping(first.did || "");
    if (ping.reachable) {
      console.log(`\nPing ${first.did || first.peerId}: ${ping.latencyMs.toFixed(1)}ms`);
    } else {
      console.log(`\nPeer unreachable: ${ping.error}`);
    }
  } else {
    console.log("\nNo peers connected.");
    const loopback = await client.ping();
    console.log(`Loopback ping: ${loopback.latencyMs.toFixed(1)}ms`);
  }

  client.close();
}

main().catch(console.error);
