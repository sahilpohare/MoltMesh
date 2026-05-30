import { describe, test, expect } from "bun:test";
import {
  normalizeDid,
  shortDid,
  capabilityId,
  capabilityName,
  normalizeCapability,
  isCoreCapability,
  CoreCapabilities,
  CORE_CAPABILITY_PREFIX,
  defaultAddr,
  A2AClient,
} from "./client.js";

// ── DID helpers ───────────────────────────────────────────────────────────────

describe("normalizeDid", () => {
  test("empty string passthrough", () => {
    expect(normalizeDid("")).toBe("");
  });

  test("already normalized", () => {
    const did = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    expect(normalizeDid(did)).toBe(did);
  });

  test("did:key without multibase z prefix gets z added", () => {
    const did = "did:key:6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    expect(normalizeDid(did)).toBe("did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK");
  });

  test("bare z-prefixed key", () => {
    const key = "z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    expect(normalizeDid(key)).toBe("did:key:" + key);
  });

  test("bare base58 key gets did:key:z prefix", () => {
    const key = "6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";
    expect(normalizeDid(key)).toBe("did:key:z" + key);
  });

  test("non-key DID passthrough", () => {
    const did = "did:web:example.com";
    expect(normalizeDid(did)).toBe(did);
  });
});

describe("shortDid", () => {
  const longDid = "did:key:z6MkhaXgBZDvotDkL5257faiztiGiC2QtKLGpbnnEGta2doK";

  test("truncates long DID", () => {
    const result = shortDid(longDid);
    expect(result).toContain("...");
    expect(result.length).toBeLessThan(longDid.length);
  });

  test("preserves head and tail", () => {
    const result = shortDid(longDid, { head: 12, tail: 6 });
    expect(result.slice(0, 12)).toBe(longDid.slice(0, 12));
    expect(result.slice(-6)).toBe(longDid.slice(-6));
  });

  test("short DID not truncated", () => {
    const did = "did:key:z6Mk";
    expect(shortDid(did, { head: 40, tail: 10 })).toBe(did);
  });

  test("negative head throws", () => {
    expect(() => shortDid(longDid, { head: -1 })).toThrow();
  });

  test("negative tail throws", () => {
    expect(() => shortDid(longDid, { tail: -1 })).toThrow();
  });
});

// ── Capability helpers ────────────────────────────────────────────────────────

describe("capabilityId", () => {
  test("bare name gets namespaced", () => {
    expect(capabilityId("text-generation")).toBe("a2a:v1:cap:text-generation");
  });

  test("already namespaced passthrough", () => {
    expect(capabilityId("acme:cap:legal")).toBe("acme:cap:legal");
  });

  test("empty string passthrough", () => {
    expect(capabilityId("")).toBe("");
  });
});

describe("capabilityName", () => {
  test("strips core prefix", () => {
    expect(capabilityName("a2a:v1:cap:text-generation")).toBe("text-generation");
  });

  test("strips custom namespace", () => {
    expect(capabilityName("acme:cap:legal")).toBe("legal");
  });

  test("no cap marker passthrough", () => {
    expect(capabilityName("plain")).toBe("plain");
  });

  test("empty string passthrough", () => {
    expect(capabilityName("")).toBe("");
  });
});

describe("isCoreCapability", () => {
  test("core capability returns true", () => {
    expect(isCoreCapability("a2a:v1:cap:text-generation")).toBe(true);
  });

  test("non-core returns false", () => {
    expect(isCoreCapability("acme:v1:cap:custom")).toBe(false);
  });

  test("empty returns false", () => {
    expect(isCoreCapability("")).toBe(false);
  });
});

describe("normalizeCapability", () => {
  test("alias for capabilityId", () => {
    expect(normalizeCapability("embedding")).toBe(capabilityId("embedding"));
  });
});

describe("CoreCapabilities", () => {
  test("all values have core prefix", () => {
    for (const cap of Object.values(CoreCapabilities)) {
      expect(cap.startsWith(CORE_CAPABILITY_PREFIX)).toBe(true);
    }
  });

  test("known values", () => {
    expect(CoreCapabilities.TEXT_GENERATION).toBe("a2a:v1:cap:text-generation");
    expect(CoreCapabilities.EMBEDDING).toBe("a2a:v1:cap:embedding");
  });

  test("all values are distinct", () => {
    const values = Object.values(CoreCapabilities);
    expect(new Set(values).size).toBe(values.length);
  });
});

// ── defaultAddr ───────────────────────────────────────────────────────────────

describe("defaultAddr", () => {
  test("uses A2A_GRPC_ADDR env when set", () => {
    process.env["A2A_GRPC_ADDR"] = "localhost:9999";
    expect(defaultAddr()).toBe("localhost:9999");
    delete process.env["A2A_GRPC_ADDR"];
  });

  test("falls back to unix socket path", () => {
    delete process.env["A2A_GRPC_ADDR"];
    const addr = defaultAddr();
    expect(addr.startsWith("unix://")).toBe(true);
    expect(addr).toContain("a2a.sock");
  });
});

// ── A2AClient ─────────────────────────────────────────────────────────────────

describe("A2AClient", () => {
  test("constructs without throwing", () => {
    // gRPC channels are lazy — connecting to invalid addr doesn't throw here
    expect(() => new A2AClient("localhost:0")).not.toThrow();
  });

  test("close is idempotent", () => {
    const client = new A2AClient("localhost:0");
    expect(() => {
      client.close();
      client.close();
    }).not.toThrow();
  });
});
