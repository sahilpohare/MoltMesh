// Re-export A2AClient from the shared implementation.
// We vendor a copy here so moltmesh-ai-sdk has no workspace dependency,
// keeping it installable standalone with `npm install moltmesh-ai-sdk`.
export {
  A2AClient,
  defaultAddr,
  createStub,
  unary,
  serverStream,
  normalizeDid,
  shortDid,
  capabilityId,
  capabilityName,
  normalizeCapability,
  isCoreCapability,
  CORE_CAPABILITY_PREFIX,
  CoreCapabilities,
} from "../../openclaw-plugin/src/client.js";

export type {
  GrpcClient,
  Obj,
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
} from "../../openclaw-plugin/src/client.js";
