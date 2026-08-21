import { createPrivateKey, createPublicKey, generateKeyPairSync, sign, type KeyObject } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync, chmodSync } from "fs";
import { join } from "path";
import { fileURLToPath } from "url";
import protobuf from "protobufjs";

const B58 = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
const PROTO_PATH = join(fileURLToPath(new URL("../../../../proto/a2a.proto", import.meta.url)));
const agentCardType = protobuf.loadSync(PROTO_PATH).lookupType("a2a.v1.AgentCard");
const threadType = protobuf.loadSync(PROTO_PATH).lookupType("a2a.v1.Thread");
const invitationType = protobuf.loadSync(PROTO_PATH).lookupType("a2a.v1.ThreadInvitation");
const catchupProofType = protobuf.loadSync(PROTO_PATH).lookupType("a2a.v1.ThreadCatchupProof");
const keyEnvelopeType = protobuf.loadSync(PROTO_PATH).lookupType("a2a.v1.ThreadKeyEnvelope");

function base58(data: Uint8Array): string {
  let value = BigInt(`0x${Buffer.from(data).toString("hex") || "0"}`);
  let result = "";
  while (value > 0n) {
    const r = Number(value % 58n);
    result = B58[r]! + result;
    value /= 58n;
  }
  for (const byte of data) {
    if (byte !== 0) break;
    result = "1" + result;
  }
  return result || "1";
}

function rawPublic(key: KeyObject): Uint8Array {
  const jwk = key.export({ format: "jwk" }) as { x?: string };
  if (!jwk.x) throw new Error("key does not expose a raw public value");
  return Buffer.from(jwk.x, "base64url");
}

/** SDK-owned Ed25519 and X25519 identity, stored under one agent HOME. */
export class AgentIdentity {
  private constructor(
    private readonly signingPrivateKey: KeyObject,
    private readonly encryptionPrivateKey: KeyObject,
  ) {}

  static loadOrCreate(home = join(process.env["HOME"] ?? ".", ".moltmesh-agent")): AgentIdentity {
    const path = join(home, "identity.json");
    if (existsSync(path)) {
      const stored = JSON.parse(readFileSync(path, "utf8")) as { signing_private_key: string; encryption_private_key: string };
      return new AgentIdentity(
        createPrivateKey(stored.signing_private_key),
        createPrivateKey(stored.encryption_private_key),
      );
    }
    mkdirSync(home, { recursive: true, mode: 0o700 });
    const signing = generateKeyPairSync("ed25519");
    const encryption = generateKeyPairSync("x25519");
    const identity = new AgentIdentity(signing.privateKey, encryption.privateKey);
    writeFileSync(path, `${JSON.stringify({
      version: 1,
      signing_private_key: signing.privateKey.export({ format: "pem", type: "pkcs8" }),
      encryption_private_key: encryption.privateKey.export({ format: "pem", type: "pkcs8" }),
    })}\n`, { mode: 0o600 });
    chmodSync(path, 0o600);
    return identity;
  }

  get signingPublicKey(): Uint8Array { return rawPublic(createPublicKey(this.signingPrivateKey)); }
  get encryptionPublicKey(): Uint8Array { return rawPublic(createPublicKey(this.encryptionPrivateKey)); }
  get did(): string { return `did:key:z${base58(Buffer.concat([Buffer.from([0xed, 0x01]), Buffer.from(this.signingPublicKey)]))}`; }
  sign(payload: Uint8Array): Uint8Array { return sign(null, payload, this.signingPrivateKey); }

  /** Fill the identity-bound fields and sign deterministic AgentCard bytes. */
  signAgentCard(card: Record<string, unknown>, nodePeerId = ""): Record<string, unknown> {
    const now = String(Date.now());
    const metadata = Object.fromEntries(Object.entries((card["metadata"] as Record<string, string> | undefined) ?? {}).sort(([a], [b]) => a.localeCompare(b)));
    const signed = {
      ...card,
      did: this.did,
      publicKey: Buffer.from(this.signingPublicKey).toString("base64"),
      encryptionPublicKey: this.encryptionPublicKey,
      nodePeerId,
      publishedAt: now,
      expiresAt: String(Date.now() + 60 * 60 * 1000),
      signature: "",
      metadata,
    };
    const err = agentCardType.verify(signed);
    if (err) throw new Error(`invalid agent card: ${err}`);
    const canonical = agentCardType.encode(agentCardType.create(signed)).finish();
    return { ...signed, signature: Buffer.from(this.sign(canonical)).toString("base64") };
  }

  /** Sign a canonical Thread descriptor without its creator_signature field. */
  signThreadDescriptor(thread: Record<string, unknown>): Uint8Array {
    const err = threadType.verify(thread);
    if (err) throw new Error(`invalid thread descriptor: ${err}`);
    return this.sign(threadType.encode(threadType.create(thread)).finish());
  }
  signThreadInvitation(invitation: Record<string, unknown>): Uint8Array {
    const err = invitationType.verify(invitation); if (err) throw new Error(`invalid thread invitation: ${err}`);
    return this.sign(invitationType.encode(invitationType.create(invitation)).finish());
  }
  signThreadCatchupProof(proof: Record<string, unknown>): Uint8Array {
    const err = catchupProofType.verify(proof); if (err) throw new Error(`invalid catchup proof: ${err}`);
    return this.sign(catchupProofType.encode(catchupProofType.create(proof)).finish());
  }
  encodeThreadCatchupProof(proof: Record<string, unknown>): Uint8Array {
    const err = catchupProofType.verify(proof); if (err) throw new Error(`invalid catchup proof: ${err}`);
    return catchupProofType.encode(catchupProofType.create(proof)).finish();
  }
  signRecoveryKeyEnvelope(envelope: Record<string, unknown>): Uint8Array {
    const unsigned = { ...envelope, authorSignature: new Uint8Array() };
    const err = keyEnvelopeType.verify(unsigned); if (err) throw new Error(`invalid recovery envelope: ${err}`);
    return this.sign(keyEnvelopeType.encode(keyEnvelopeType.create(unsigned)).finish());
  }
}
