"""SDK-owned signing and encryption identities.

Daemon node keys deliberately never appear here.  An application can reuse an
AgentIdentity with another daemon without changing its DID or recovery keys.
"""

from __future__ import annotations

import base64
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519, x25519

from moltmesh.proto import a2a_pb2 as pb


_B58 = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def _base58(data: bytes) -> str:
    value = int.from_bytes(data, "big")
    out = bytearray()
    while value:
        value, remainder = divmod(value, 58)
        out.append(_B58[remainder])
    return (b"1" * (len(data) - len(data.lstrip(b"\0"))) + bytes(reversed(out or b""))).decode()


def _did_from_signing_key(public_key: bytes) -> str:
    # did:key / multicodec Ed25519 prefix (0xed01) encoded as base58btc.
    return "did:key:z" + _base58(b"\xed\x01" + public_key)


@dataclass(frozen=True)
class AgentIdentity:
    """An SDK agent's persistent Ed25519/X25519 key material."""

    signing_key: ed25519.Ed25519PrivateKey
    encryption_key: x25519.X25519PrivateKey

    @property
    def did(self) -> str:
        return _did_from_signing_key(self.signing_public_key)

    @property
    def signing_public_key(self) -> bytes:
        return self.signing_key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)

    @property
    def encryption_public_key(self) -> bytes:
        return self.encryption_key.public_key().public_bytes(serialization.Encoding.Raw, serialization.PublicFormat.Raw)

    @property
    def encryption_private_key_bytes(self) -> bytes:
        """Raw X25519 private material for SDK-only envelope unwrap operations."""
        return self.encryption_key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption())

    def sign(self, payload: bytes) -> bytes:
        return self.signing_key.sign(payload)

    def sign_agent_card(
        self, card: pb.AgentCard, *, node_peer_id: str = "", ttl_seconds: int = 3600
    ) -> pb.AgentCard:
        """Populate and sign an AgentCard using the canonical protobuf bytes."""
        now = int(time.time() * 1000)
        card.did = self.did
        card.public_key = base64.b64encode(self.signing_public_key).decode()
        card.encryption_public_key = self.encryption_public_key
        card.node_peer_id = node_peer_id
        card.published_at = now
        card.expires_at = now + ttl_seconds * 1000
        card.signature = ""
        card.signature = base64.b64encode(self.sign(card.SerializeToString(deterministic=True))).decode()
        return card

    @classmethod
    def load_or_create(cls, home: str | Path | None = None) -> "AgentIdentity":
        root = Path(home) if home is not None else Path(os.environ.get("HOME", "~")).expanduser() / ".moltmesh-agent"
        path = root / "identity.json"
        if path.exists():
            data = json.loads(path.read_text())
            return cls(
                ed25519.Ed25519PrivateKey.from_private_bytes(base64.b64decode(data["signing_private_key"])),
                x25519.X25519PrivateKey.from_private_bytes(base64.b64decode(data["encryption_private_key"])),
            )
        root.mkdir(mode=0o700, parents=True, exist_ok=True)
        identity = cls(ed25519.Ed25519PrivateKey.generate(), x25519.X25519PrivateKey.generate())
        payload = {
            "version": 1,
            "signing_private_key": base64.b64encode(identity.signing_key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption())).decode(),
            "encryption_private_key": base64.b64encode(identity.encryption_key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption())).decode(),
        }
        path.write_text(json.dumps(payload, sort_keys=True) + "\n")
        path.chmod(0o600)
        return identity
