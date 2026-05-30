/**
 * 06_ai_sdk_agent.ts — Vercel AI SDK agent using MoltMesh tools.
 *
 * Install:
 *   npm install ai @ai-sdk/anthropic zod
 *   npm install moltmesh-ai-sdk   # or use local path
 *
 * Run:
 *   ANTHROPIC_API_KEY=sk-... npx tsx 06_ai_sdk_agent.ts
 */
import { generateText } from "ai";
import { anthropic } from "@ai-sdk/anthropic";
import { createMoltMeshTools } from "../../sdk/typescript/ai-sdk/src/index.js";

async function main() {
  const tools = createMoltMeshTools();

  const { text, steps } = await generateText({
    model: anthropic("claude-sonnet-4-6"),
    tools,
    maxSteps: 10,
    system:
      "You are an agent on the MoltMesh peer-to-peer network. " +
      "Use the available tools to complete the user's request.",
    prompt:
      "1. Check the daemon health. " +
      "2. Find any agents that offer text-generation capability. " +
      "3. Create a network called 'ai-sdk-demo'. " +
      "4. List all networks you belong to. " +
      "Report your findings.",
  });

  console.log("=== STEPS ===");
  for (const step of steps) {
    for (const call of step.toolCalls ?? []) {
      console.log(`  [tool] ${call.toolName}`, JSON.stringify(call.args).slice(0, 120));
    }
    for (const result of step.toolResults ?? []) {
      console.log(`  [result]`, JSON.stringify(result.result).slice(0, 120));
    }
  }

  console.log("\n=== FINAL RESPONSE ===");
  console.log(text);
}

main().catch(console.error);
