#!/usr/bin/env node
// Keep the plugin source portable: marketplace installs receive package
// dependencies but do not run npm lifecycle scripts. Build only when needed,
// then replace this bootstrap process with the actual stdio MCP server.
import { existsSync } from "node:fs";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const server = resolve(root, "dist/claude-code-server.js");

if (!existsSync(server)) {
  const npm = process.platform === "win32" ? "npm.cmd" : "npm";
  const result = spawnSync(npm, ["run", "build"], { cwd: root, stdio: "inherit" });
  if (result.status !== 0 || !existsSync(server)) {
    throw new Error("MoltMesh Claude Code plugin could not build its MCP server");
  }
}

await import(server);
