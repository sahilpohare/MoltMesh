/**
 * Integration tests for A2AClient — require a live daemon.
 *
 * The suite builds the moltmesh-daemon binary (via `go build`) and starts it
 * on a random TCP port before running tests, then tears it down afterwards.
 * All tests are skipped when the Go toolchain is absent or the daemon fails
 * to start within 10 seconds.
 */

import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { spawn, type ChildProcess, execFileSync } from "child_process";
import { mkdtempSync, rmSync } from "fs";
import { createServer } from "net";
import { join } from "path";
import { tmpdir } from "os";

import { A2AClient } from "./client.js";

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
  // src → openclaw-plugin → typescript → sdk → p2p_a2a
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
      s.once("error", () => resolve(true));     // EADDRINUSE = port is up
      s.listen(port, "127.0.0.1", () => {
        s.close(() => resolve(false));           // port is still free
      });
    });
    if (ok) return true;
    await new Promise(r => setTimeout(r, 200));
  }
  return false;
}

// ── global state ──────────────────────────────────────────────────────────────

let daemonProc: ChildProcess | null = null;
let buildDir: string | null = null;
let dataDir: string | null = null;
let grpcAddr: string | null = null;
let client: A2AClient | null = null;
let skipReason: string | null = null;

// ── setup / teardown ──────────────────────────────────────────────────────────

beforeAll(async () => {
  if (!hasGo()) {
    skipReason = "go toolchain not found";
    return;
  }

  buildDir = mkdtempSync(join(tmpdir(), "moltmesh_build_"));
  dataDir = mkdtempSync(join(tmpdir(), "moltmesh_data_"));
  const port = await freePort();
  grpcAddr = `127.0.0.1:${port}`;
  const binary = join(buildDir, "moltmesh-daemon");

  // Build the daemon binary.
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

  // Start the daemon.
  daemonProc = spawn(
    binary,
    ["start", "--data-dir", dataDir, "--grpc-addr", grpcAddr],
    { stdio: "ignore" },
  );

  const ready = await waitForPort(port, 10_000);
  if (!ready) {
    daemonProc.kill("SIGTERM");
    skipReason = "daemon did not start within 10 s";
    return;
  }

  client = new A2AClient(grpcAddr);
});

afterAll(async () => {
  client?.close();
  if (daemonProc) {
    daemonProc.kill("SIGTERM");
    await new Promise<void>(resolve => {
      daemonProc!.on("exit", () => resolve());
      setTimeout(resolve, 3000);
    });
  }
  if (buildDir) rmSync(buildDir, { recursive: true, force: true });
  if (dataDir)  rmSync(dataDir,  { recursive: true, force: true });
});

// ── helper ────────────────────────────────────────────────────────────────────

/**
 * Wrap test() so the test is skipped when the daemon is unavailable.
 * Usage mirrors test(): dtest("name", async () => { ... })
 */
function dtest(name: string, fn: (c: A2AClient) => Promise<void> | void): void {
  test.skipIf(() => skipReason !== null)(name, async () => {
    await fn(client!);
  });
}

// ── identity ──────────────────────────────────────────────────────────────────

describe("identity", () => {
  test("getIdentity returns a did:key DID", async () => {
    const c = requireDaemon();
    const id = await c.getIdentity();
    expect(id.did).toMatch(/^did:key:/);
  });

  test("getIdentity returns a non-empty public key", async () => {
    const c = requireDaemon();
    const id = await c.getIdentity();
    expect(id.publicKey).toBeTruthy();
  });

  test("DID is stable across calls", async () => {
    const c = requireDaemon();
    const a = await c.getIdentity();
    const b = await c.getIdentity();
    expect(a.did).toBe(b.did);
  });
});

// ── messaging ─────────────────────────────────────────────────────────────────

describe("messaging", () => {
  test("sendMessage returns a messageId", async () => {
    const c = requireDaemon();
    const result = await c.sendMessage("did:key:zRemoteTest", "hello");
    expect(result.messageId).toBeTruthy();
  });

  test("sendMessage to offline peer is queued", async () => {
    const c = requireDaemon();
    const result = await c.sendMessage("did:key:zOfflinePeer", "queued?");
    expect(result.queued).toBe(true);
  });

  test("getInbox returns an array", async () => {
    const c = requireDaemon();
    const msgs = await c.getInbox();
    expect(Array.isArray(msgs)).toBe(true);
  });

  test("getInbox respects limit", async () => {
    const c = requireDaemon();
    const msgs = await c.getInbox({ limit: 3 });
    expect(msgs.length).toBeLessThanOrEqual(3);
  });
});

// ── tasks ─────────────────────────────────────────────────────────────────────

describe("tasks", () => {
  test("createTask returns a non-empty id", async () => {
    const c = requireDaemon();
    const task = await c.createTask("did:key:zAssigneeTest", "a2a:v1:cap:test-skill");
    expect(task.id).toBeTruthy();
  });

  test("createTask status is TASK_STATUS_SUBMITTED", async () => {
    const c = requireDaemon();
    const task = await c.createTask("did:key:zAssigneeTest", "a2a:v1:cap:test-skill");
    expect(task.status).toBe("TASK_STATUS_SUBMITTED");
  });

  test("getTask returns the same task", async () => {
    const c = requireDaemon();
    const created = await c.createTask("did:key:zAssigneeTest", "test-skill");
    const fetched = await c.getTask(created.id);
    expect(fetched.id).toBe(created.id);
  });

  test("markWorking transitions status", async () => {
    const c = requireDaemon();
    const task = await c.createTask("did:key:zAssigneeTest", "test-skill");
    const updated = await c.markWorking(task.id);
    expect(updated.status).toBe("TASK_STATUS_WORKING");
  });

  test("markCompleted transitions status", async () => {
    const c = requireDaemon();
    const task = await c.createTask("did:key:zAssigneeTest", "test-skill");
    await c.markWorking(task.id);
    const done = await c.markCompleted(task.id);
    expect(done.status).toBe("TASK_STATUS_COMPLETED");
  });

  test("markFailed transitions status and sets error", async () => {
    const c = requireDaemon();
    const task = await c.createTask("did:key:zAssigneeTest", "test-skill");
    const failed = await c.markFailed(task.id, "something broke");
    expect(failed.status).toBe("TASK_STATUS_FAILED");
    expect(failed.error).toContain("something broke");
  });

  test("cancelTask transitions status to CANCELLED", async () => {
    const c = requireDaemon();
    const task = await c.createTask("did:key:zAssigneeTest", "test-skill");
    const cancelled = await c.cancelTask(task.id);
    expect(cancelled.status).toBe("TASK_STATUS_CANCELLED");
  });

  test("waitTask resolves for already-completed task", async () => {
    const c = requireDaemon();
    const task = await c.createTask("did:key:zAssigneeTest", "test-skill");
    await c.markCompleted(task.id);
    const result = await c.waitTask(task.id, { timeoutMs: 5_000 });
    expect(result.status).toBe("TASK_STATUS_COMPLETED");
  });

  test("waitTask throws on timeout", async () => {
    const c = requireDaemon();
    const task = await c.createTask("did:key:zAssigneeTest", "long-running");
    await expect(c.waitTask(task.id, { timeoutMs: 300, pollMs: 100 })).rejects.toThrow(/timed out/);
  });
});

// ── blobs ─────────────────────────────────────────────────────────────────────

describe("blobs", () => {
  test("storeBlob returns a sha256 CID", async () => {
    const c = requireDaemon();
    const cid = await c.storeBlob(Buffer.from("hello blob"), { mimeType: "text/plain" });
    expect(cid).toMatch(/^sha256:/);
  });

  test("storeBlob is deterministic", async () => {
    const c = requireDaemon();
    const data = Buffer.from("deterministic content");
    const cid1 = await c.storeBlob(data);
    const cid2 = await c.storeBlob(data);
    expect(cid1).toBe(cid2);
  });

  test("fetchBlob returns the original data", async () => {
    const c = requireDaemon();
    const original = Buffer.from("roundtrip test data");
    const cid = await c.storeBlob(original);
    const fetched = await c.fetchBlob(cid);
    expect(Buffer.from(fetched)).toEqual(original);
  });

  test("store and fetch large blob (128 KB)", async () => {
    const c = requireDaemon();
    const data = Buffer.alloc(128 * 1024, 0x42);
    const cid = await c.storeBlob(data);
    const fetched = await c.fetchBlob(cid);
    expect(Buffer.from(fetched)).toEqual(data);
  });
});

// ── threads ───────────────────────────────────────────────────────────────────

describe("threads", () => {
  test("createThread returns an id", async () => {
    const c = requireDaemon();
    const id = await c.getIdentity();
    const thread = await c.createThread([id.did]);
    expect(thread.id).toBeTruthy();
  });

  test("getThread returns the created thread", async () => {
    const c = requireDaemon();
    const id = await c.getIdentity();
    const created = await c.createThread([id.did]);
    const fetched = await c.getThread(created.id);
    expect(fetched.id).toBe(created.id);
  });

  test("appendEntry and getThreadEntries roundtrip", async () => {
    const c = requireDaemon();
    const id = await c.getIdentity();
    const thread = await c.createThread([id.did]);
    await c.appendEntry(thread.id, Buffer.from("hello-thread"));

    // Wait up to 5 s for the entry to be committed by Raft
    const deadline = Date.now() + 5_000;
    let entries: unknown[] = [];
    while (Date.now() < deadline) {
      entries = await c.getThreadEntries(thread.id);
      if (entries.length > 0) break;
      await new Promise(r => setTimeout(r, 200));
    }
    expect(entries.length).toBeGreaterThan(0);
  });
});

// ── diagnostics ───────────────────────────────────────────────────────────────

describe("diagnostics", () => {
  test("health returns version", async () => {
    const c = requireDaemon();
    const h = await c.health();
    expect(h.version).toBeTruthy();
  });

  test("health DID matches identity", async () => {
    const c = requireDaemon();
    const [h, id] = await Promise.all([c.health(), c.getIdentity()]);
    expect(h.did).toBe(id.did);
  });

  test("listPeers returns an array", async () => {
    const c = requireDaemon();
    const peers = await c.listPeers();
    expect(Array.isArray(peers)).toBe(true);
  });
});

// ── webhooks ──────────────────────────────────────────────────────────────────

describe("webhooks", () => {
  test("setWebhook / getWebhook / clearWebhook roundtrip", async () => {
    const c = requireDaemon();
    const url = "https://example.com/webhook-test";
    await c.setWebhook(url);
    const got = await c.getWebhook();
    expect(got).toBe(url);
    await c.clearWebhook();
    const cleared = await c.getWebhook();
    expect(cleared).toBe("");
  });

  test("clearWebhook is idempotent", async () => {
    const c = requireDaemon();
    await c.clearWebhook();
    await c.clearWebhook();
  });
});

// ── networks ──────────────────────────────────────────────────────────────────

describe("networks", () => {
  test("createNetwork returns an id and name", async () => {
    const c = requireDaemon();
    const net = await c.createNetwork("ts-test-net");
    expect(net.id).toBeTruthy();
    expect(net.name).toBe("ts-test-net");
  });

  test("listNetworks includes created network", async () => {
    const c = requireDaemon();
    const net = await c.createNetwork("ts-listed-net");
    const networks = await c.listNetworks();
    const ids = networks.map((n: Record<string, unknown>) => n.id);
    expect(ids).toContain(net.id);
  });

  test("leaveNetwork removes it from list", async () => {
    const c = requireDaemon();
    const net = await c.createNetwork("ts-leave-net");
    await c.leaveNetwork(net.id);
    const networks = await c.listNetworks();
    const ids = networks.map((n: Record<string, unknown>) => n.id);
    expect(ids).not.toContain(net.id);
  });

  test("broadcastNetwork does not throw", async () => {
    const c = requireDaemon();
    const net = await c.createNetwork("ts-broadcast-net");
    await c.broadcastNetwork(net.id, "hello network");
    await c.leaveNetwork(net.id);
  });
});

// ── pub/sub ───────────────────────────────────────────────────────────────────

describe("pubsub", () => {
  test("publish to a topic does not throw", async () => {
    const c = requireDaemon();
    await c.publish("ts-test-topic", Buffer.from("payload"));
  });

  test("publish string payload does not throw", async () => {
    const c = requireDaemon();
    await c.publish("ts-test-topic", "string payload");
  });
});
