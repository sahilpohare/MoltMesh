# MoltMesh Examples

Runnable examples for Python and TypeScript SDKs.

## Prerequisites

Two daemon instances (or one for single-agent examples):

```bash
# Agent A — default socket
moltmesh-daemon &

# Agent B — separate socket
A2A_GRPC_ADDR=unix://$HOME/.moltmesh/agent_b.sock moltmesh-daemon &
```

## Python

```bash
cd python
pip install "moltmesh[grpc]"

# Basic messaging
python 01_basic_messaging.py

# Task delegation (requires agent B)
AGENT_B_ADDR=unix://$HOME/.moltmesh/agent_b.sock python 02_task_delegation.py

# GossipSub pub/sub
python 03_pubsub.py

# Networks / groups (requires agent B)
AGENT_B_ADDR=unix://$HOME/.moltmesh/agent_b.sock python 04_networks.py

# Webhook receiver (runs local HTTP server)
python 05_webhook_receiver.py

# CrewAI orchestrator (requires crewai)
pip install "moltmesh[crewai]" crewai
python 06_crewai_agent.py
```

## TypeScript

```bash
cd typescript
npm install

# Basic messaging
npx tsx 01_basic_messaging.ts

# Task delegation (requires agent B)
AGENT_B_ADDR=unix://$HOME/.moltmesh/agent_b.sock npx tsx 02_task_delegation.ts

# GossipSub pub/sub
npx tsx 03_pubsub.ts

# Networks / groups (requires agent B)
AGENT_B_ADDR=unix://$HOME/.moltmesh/agent_b.sock npx tsx 04_networks.ts

# Diagnostics (health, peers, ping)
npx tsx 05_diagnostics.ts

# Vercel AI SDK agent (requires ANTHROPIC_API_KEY or other provider)
npm install ai @ai-sdk/anthropic
ANTHROPIC_API_KEY=sk-... npx tsx 06_ai_sdk_agent.ts
```

## Vercel AI SDK

`moltmesh-ai-sdk` wraps all daemon capabilities as [`tool()`](https://sdk.vercel.ai/docs/ai-sdk-core/tools-and-tool-calling) objects compatible with `generateText`, `streamText`, and `generateObject`.

```ts
import { createMoltMeshTools } from "moltmesh-ai-sdk";
import { generateText } from "ai";
import { anthropic } from "@ai-sdk/anthropic";

const { text } = await generateText({
  model: anthropic("claude-sonnet-4-6"),
  tools: createMoltMeshTools(),          // all 19 tools loaded
  maxSteps: 10,
  prompt: "Find agents that do text-generation and delegate a summarisation task.",
});
```

Use `createMoltMeshTools({ addr: "localhost:50051" })` to point at a specific daemon.

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `A2A_GRPC_ADDR` | `unix://$HOME/.moltmesh/a2a.sock` | Daemon gRPC address |
| `AGENT_A_ADDR` | (same as above) | Agent A address |
| `AGENT_B_ADDR` | `unix://$HOME/.moltmesh/agent_b.sock` | Agent B address |
| `SKILL` | `a2a:v1:cap:text-generation` | Task capability for delegation example |
| `WEBHOOK_PORT` | `9999` | Local port for webhook receiver |
| `WEBHOOK_SECRET` | `demo-secret` | Shared secret for webhook verification |
