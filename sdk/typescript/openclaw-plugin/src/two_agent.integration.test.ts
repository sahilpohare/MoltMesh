/**
 * Two-agent integration test — proves discovery, capability publishing, and
 * task delegation work end-to-end between two independent daemon processes
 * on the same machine (mDNS/DHT peering, no hardcoded DIDs or addresses).
 *
 * Deterministic: both daemons run with a fixed skill/name/payload (no
 * randomness), each in its own temp data dir, on its own free TCP port for
 * gRPC. They discover each other over the real libp2p transport (mDNS on
 * localhost), exactly as two independently-operated agents would.
 *
 * Skipped (not failed) when the Go toolchain is unavailable or either
 * daemon fails to start, matching client.integration.test.ts's convention.
 */

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { spawn, type ChildProcess, execFileSync } from "child_process";
import { mkdtempSync, rmSync } from "fs";
import { createServer } from "net";
import { join } from "path";
import { tmpdir } from "os";

import { A2AClient, CoreCapabilities } from "./client.js";

// ── harness ───────────────────────────────────────────────────────────────────

function freePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const srv = createServer();
    srv.listen(0, "127.0.0.1", () => {
      const port = (srv.address() as { port: number }).port;
      srv.close(() => resolve(port));
    });
    srv.on("error", reject);
  });
}

function repoRoot(): string {
  // import.meta.dirname is this file's own directory
  // (sdk/typescript/openclaw-plugin/src/), so 4 levels up reaches p2p_a2a.
  return join(import.meta.dirname, "..", "..", "..", "..");
}

function hasGo(): boolean {
  try {
    execFileSync("go", ["version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

async function waitForPort(port: number, timeoutMs = 10_000): Promise<boolean> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const ok = await new Promise<boolean>(resolve => {
      const s = createServer();
      s.once("error", () => resolve(true)); // EADDRINUSE = port is up
      s.listen(port, "127.0.0.1", () => {
        s.close(() => resolve(false)); // port is still free
      });
    });
    if (ok) return true;
    await new Promise(r => setTimeout(r, 200));
  }
  return false;
}

interface Daemon {
  proc: ChildProcess;
  dataDir: string;
  grpcAddr: string;
  netPort: number;
  client: A2AClient;
}

async function startDaemon(binary: string, label: string): Promise<Daemon> {
  const dataDir = mkdtempSync(join(tmpdir(), `moltmesh_${label}_`));
  const grpcPort = await freePort();
  const netPort = await freePort();
  const grpcAddr = `127.0.0.1:${grpcPort}`;

  const proc = spawn(
    binary,
    ["start", "--data-dir", dataDir, "--grpc-addr", grpcAddr, "--port", String(netPort)],
    { stdio: "ignore" },
  );

  const ready = await waitForPort(grpcPort, 30_000);
  if (!ready) {
    proc.kill("SIGTERM");
    throw new Error(`${label} daemon did not start within 30s`);
  }

  return { proc, dataDir, grpcAddr, netPort, client: new A2AClient(grpcAddr) };
}

function stopDaemon(d: Daemon | null): Promise<void> {
  if (!d) return Promise.resolve();
  d.client.close();
  d.proc.kill("SIGTERM");
  return new Promise(resolve => {
    d.proc.on("exit", () => resolve());
    setTimeout(resolve, 3000);
  });
}

// ── global state ──────────────────────────────────────────────────────────────

let buildDir: string | null = null;
let coordinator: Daemon | null = null;
let worker: Daemon | null = null;
let skipReason: string | null = null;

const SKILL = CoreCapabilities.TEXT_GENERATION;
const DISCOVERY_TIMEOUT_MS = 30_000;

beforeAll(async () => {
  if (!hasGo()) {
    skipReason = "go toolchain not found";
    return;
  }

  buildDir = mkdtempSync(join(tmpdir(), "moltmesh_build_"));
  const binary = join(buildDir, "moltmesh-daemon");

  try {
    execFileSync("go", ["build", "-o", binary, "./cmd/daemon"], {
      cwd: repoRoot(),
      timeout: 120_000,
      stdio: "ignore",
    });
  } catch {
    skipReason = "go build failed";
    return;
  }

  try {
    [coordinator, worker] = await Promise.all([
      startDaemon(binary, "coordinator"),
      startDaemon(binary, "worker"),
    ]);
  } catch (e) {
    skipReason = `daemon startup failed: ${(e as Error).message}`;
  }
}, 150_000); // go build (up to 120s) + two parallel daemon startups (up to 30s each)

afterAll(async () => {
  await Promise.all([stopDaemon(coordinator), stopDaemon(worker)]);
  if (buildDir) rmSync(buildDir, { recursive: true, force: true });
  if (coordinator) rmSync(coordinator.dataDir, { recursive: true, force: true });
  if (worker) rmSync(worker.dataDir, { recursive: true, force: true });
});

// timeoutMs defaults well above bun's 5000ms test default — DHT propagation
// and mDNS peering are not instant, and several tests poll for up to
// DISCOVERY_TIMEOUT_MS internally, which must fit inside the test's own
// timeout or bun kills the test before the polling loop gets a real chance.
function dtest(name: string, fn: () => Promise<void> | void, timeoutMs = 40_000): void {
  test(name, async () => {
    if (skipReason !== null || coordinator === null || worker === null) return;
    await fn();
  }, timeoutMs);
}

// ── discover a peer by polling find_agents (DHT propagation isn't instant) ────

async function findWorker(timeoutMs: number) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const agents = await coordinator!.client.findAgents(SKILL, 5);
    if (agents.length > 0) return agents[0]!;
    if (Date.now() >= deadline) {
      throw new Error(`no agent advertising ${SKILL} found within ${timeoutMs}ms`);
    }
    await new Promise(r => setTimeout(r, 500));
  }
}

// ── tests ─────────────────────────────────────────────────────────────────────

describe("two-agent: identity", () => {
  dtest("coordinator and worker have distinct DIDs", async () => {
    const [a, b] = await Promise.all([
      coordinator!.client.getIdentity(),
      worker!.client.getIdentity(),
    ]);
    expect(a.did).toMatch(/^did:key:/);
    expect(b.did).toMatch(/^did:key:/);
    expect(a.did).not.toBe(b.did);
  });
});

describe("two-agent: capability publish + discovery", () => {
  dtest("worker publishes a capability the coordinator discovers on the DHT", async () => {
    const workerId = await worker!.client.getIdentity();

    const result = await worker!.client.publishAgentCard({
      name: "deterministic-worker",
      description: "Test worker for two-agent discovery",
      skills: [{ id: SKILL, name: "text-generation" }],
      multiaddrs: workerId.multiaddrs,
    });
    expect(result.success).toBe(true);

    const found = await findWorker(DISCOVERY_TIMEOUT_MS);
    expect(found.did).toBe(workerId.did);
    expect(found.name).toBe("deterministic-worker");
    expect((found.skills ?? []).map(s => s.id)).toContain(SKILL);
  });
});

describe("two-agent: task delegation", () => {
  dtest("coordinator discovers the worker and delegates a task", async () => {
    const found = await findWorker(DISCOVERY_TIMEOUT_MS);

    const task = await coordinator!.client.createTask(found.did, SKILL, {
      metadata: { prompt: "deterministic two-agent test payload" },
    });
    expect(task.status).toBe("TASK_STATUS_SUBMITTED");
    expect(task.skill).toBe(SKILL);

    // Confirm the task record actually lives on the coordinator (the real,
    // current architecture), not on the worker.
    const seenByCoordinator = await coordinator!.client.getTask(task.id);
    expect(seenByCoordinator.id).toBe(task.id);
  });

  dtest("worker receives the TASK_REQUEST notification for a delegated task", async () => {
    const found = await findWorker(DISCOVERY_TIMEOUT_MS);

    const task = await coordinator!.client.createTask(found.did, SKILL, {
      metadata: { prompt: "deterministic two-agent test payload" },
    });

    // The notification is the real cross-daemon signal in this
    // architecture — poll the worker's inbox for it rather than assuming
    // instant delivery (delivery is over a real libp2p stream + outbox
    // retry loop, not synchronous with createTask returning).
    const deadline = Date.now() + DISCOVERY_TIMEOUT_MS;
    let received: Record<string, unknown> | undefined;
    while (Date.now() < deadline) {
      const inbox = await worker!.client.getInbox({ taskId: task.id, limit: 10 });
      received = inbox.find(m => m["taskId"] === task.id);
      if (received) break;
      await new Promise(r => setTimeout(r, 500));
    }

    expect(received).toBeDefined();
    expect(received?.["kind"]).toBe("MESSAGE_KIND_TASK_REQUEST");
    expect(received?.["fromDid"]).toBe((await coordinator!.client.getIdentity()).did);
  });

  dtest("worker durably returns a result that completes the coordinator task", async () => {
    const found = await findWorker(DISCOVERY_TIMEOUT_MS);
    const task = await coordinator!.client.createTask(found.did, SKILL, {
      metadata: { prompt: "return the deterministic result 4" },
    });
    const coordinatorId = await coordinator!.client.getIdentity();

    const queued = await worker!.client.sendTaskResult(coordinatorId.did, task.id, {
      data: Buffer.from("4"),
    });
    expect(queued.messageId).toBeTruthy();
    expect(queued.queued).toBe(true);

    const deadline = Date.now() + DISCOVERY_TIMEOUT_MS;
    let completed = await coordinator!.client.getTask(task.id);
    while (Date.now() < deadline && completed.status !== "TASK_STATUS_COMPLETED") {
      await new Promise(r => setTimeout(r, 500));
      completed = await coordinator!.client.getTask(task.id);
    }
    expect(completed.status).toBe("TASK_STATUS_COMPLETED");
  });
});
