/**
 * moltmesh-ai-sdk — Vercel AI SDK integration for the MoltMesh p2p agent network.
 *
 * Quick start:
 *
 *   import { createMoltMeshTools } from "moltmesh-ai-sdk";
 *   import { generateText } from "ai";
 *   import { openai } from "@ai-sdk/openai"; // or any provider
 *
 *   const tools = createMoltMeshTools();
 *
 *   const { text } = await generateText({
 *     model: openai("gpt-4o"),
 *     tools,
 *     maxSteps: 10,
 *     prompt: "Find agents that do text-generation and send the first one a message.",
 *   });
 *
 * Available tools:
 *   p2p_get_identity      — get this daemon's DID and multiaddrs
 *   p2p_find_agents       — discover agents by capability
 *   p2p_send_message      — send a message to another agent
 *   p2p_get_inbox         — read inbox messages
 *   p2p_create_task       — delegate a task to another agent
 *   p2p_get_task          — poll task status
 *   p2p_wait_task         — block until task completes
 *   p2p_cancel_task       — cancel a task
 *   p2p_health            — daemon health (version, DID, peers, uptime)
 *   p2p_ping              — measure round-trip latency to a peer
 *   p2p_publish           — publish to a GossipSub topic
 *   p2p_set_webhook       — configure a webhook URL
 *   p2p_get_webhook       — get current webhook URL
 *   p2p_clear_webhook     — remove the webhook
 *   p2p_network_create    — create an agent group (network)
 *   p2p_network_join      — join an existing network
 *   p2p_network_leave     — leave a network
 *   p2p_network_list      — list all networks this agent belongs to
 *   p2p_network_broadcast — broadcast a message to all network members
 */

export { createMoltMeshTools } from "./tools.js";
export type { MoltMeshTools, MoltMeshToolsOptions } from "./tools.js";

export {
  A2AClient,
  defaultAddr,
  normalizeDid,
  shortDid,
  capabilityId,
  capabilityName,
  normalizeCapability,
  isCoreCapability,
  CORE_CAPABILITY_PREFIX,
  CoreCapabilities,
} from "./client.js";

export type {
  Identity,
  AgentCard,
  Task,
  Artifact,
  Thread,
  ThreadEntry,
  HealthInfo,
  PeerInfo,
  PingResult,
  TopicMessage,
  NetworkInfo,
  NetworkMember,
  BroadcastMessage,
  CapabilityId,
  CapabilityTag,
  CoreCapability,
} from "./client.js";
