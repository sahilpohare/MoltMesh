"""SDK-side XChaCha20 thread-entry envelopes compatible with pkg/threadcrypto."""

from __future__ import annotations

import struct
import hashlib
import hmac
from dataclasses import dataclass

from nacl.bindings import (
    crypto_aead_xchacha20poly1305_ietf_NPUBBYTES,
    crypto_aead_xchacha20poly1305_ietf_KEYBYTES,
    crypto_aead_xchacha20poly1305_ietf_decrypt,
    crypto_aead_xchacha20poly1305_ietf_encrypt,
    crypto_scalarmult,
    crypto_scalarmult_base,
)
from nacl.utils import random

from moltmesh.identity import AgentIdentity
from moltmesh.proto import a2a_pb2 as pb


def epoch_key_context(thread_id: str, epoch: int) -> bytes:
    return b"moltmesh-thread-epoch-key-v1\0" + thread_id.encode() + b"\0" + str(epoch).encode()


def _hkdf_sha256(ikm: bytes, info: bytes, length: int = 32) -> bytes:
    prk = hmac.new(b"\0" * 32, ikm, hashlib.sha256).digest()
    out, previous = b"", b""
    counter = 1
    while len(out) < length:
        previous = hmac.new(prk, previous + info + bytes([counter]), hashlib.sha256).digest()
        out += previous
        counter += 1
    return out[:length]


def unwrap_epoch_key(identity: AgentIdentity, envelope: pb.ThreadKeyEnvelope) -> bytes:
    """Unwrap a recipient-addressed X25519/HKDF epoch key envelope."""
    if envelope.recipient_did != identity.did:
        raise ValueError("envelope is addressed to another identity")
    if len(envelope.ephemeral_public_key) != 32 or len(envelope.nonce) != crypto_aead_xchacha20poly1305_ietf_NPUBBYTES:
        raise ValueError("invalid epoch key envelope")
    context = epoch_key_context(envelope.thread_id, envelope.encryption_epoch)
    wrap_key = _hkdf_sha256(crypto_scalarmult(identity.encryption_private_key_bytes, envelope.ephemeral_public_key), context)
    key = crypto_aead_xchacha20poly1305_ietf_decrypt(envelope.ciphertext, context, envelope.nonce, wrap_key)
    if len(key) != crypto_aead_xchacha20poly1305_ietf_KEYBYTES:
        raise ValueError("invalid unwrapped epoch key")
    return key


def wrap_epoch_key(*, thread_id: str, encryption_epoch: int, recipient_did: str,
                   recipient_public_key: bytes, epoch_key: bytes) -> pb.ThreadKeyEnvelope:
    """Create an opaque X25519/HKDF envelope for one member's epoch key."""
    if len(recipient_public_key) != 32 or len(epoch_key) != crypto_aead_xchacha20poly1305_ietf_KEYBYTES:
        raise ValueError("recipient public key and epoch key must be 32 bytes")
    ephemeral_private = random(32)
    context = epoch_key_context(thread_id, encryption_epoch)
    wrap_key = _hkdf_sha256(crypto_scalarmult(ephemeral_private, recipient_public_key), context)
    nonce = random(crypto_aead_xchacha20poly1305_ietf_NPUBBYTES)
    return pb.ThreadKeyEnvelope(thread_id=thread_id, encryption_epoch=encryption_epoch, recipient_did=recipient_did,
                                ephemeral_public_key=crypto_scalarmult_base(ephemeral_private), nonce=nonce,
                                ciphertext=crypto_aead_xchacha20poly1305_ietf_encrypt(epoch_key, context, nonce, wrap_key))


def wrap_recovery_epoch_key(*, thread_id: str, encryption_epoch: int,
                            recovery_secret: bytes, epoch_key: bytes) -> pb.ThreadKeyEnvelope:
    """Wrap an epoch key to the X25519 key represented by a recovery secret."""
    if len(recovery_secret) != 32 or len(epoch_key) != crypto_aead_xchacha20poly1305_ietf_KEYBYTES:
        raise ValueError("recovery secret and epoch key must be 32 bytes")
    ephemeral_private = random(32)
    context = epoch_key_context(thread_id, encryption_epoch)
    wrap_key = _hkdf_sha256(crypto_scalarmult(ephemeral_private, crypto_scalarmult_base(recovery_secret)), context)
    nonce = random(crypto_aead_xchacha20poly1305_ietf_NPUBBYTES)
    return pb.ThreadKeyEnvelope(thread_id=thread_id, encryption_epoch=encryption_epoch,
                                ephemeral_public_key=crypto_scalarmult_base(ephemeral_private), nonce=nonce,
                                ciphertext=crypto_aead_xchacha20poly1305_ietf_encrypt(epoch_key, context, nonce, wrap_key),
                                recovery_envelope=True)


def unwrap_recovery_epoch_key(recovery_secret: bytes, envelope: pb.ThreadKeyEnvelope) -> bytes:
    """Unwrap a capability-addressed recovery envelope without an SDK DID."""
    if len(recovery_secret) != 32 or not envelope.recovery_envelope:
        raise ValueError("invalid recovery key envelope")
    if len(envelope.ephemeral_public_key) != 32 or len(envelope.nonce) != crypto_aead_xchacha20poly1305_ietf_NPUBBYTES:
        raise ValueError("invalid recovery key envelope")
    context = epoch_key_context(envelope.thread_id, envelope.encryption_epoch)
    wrap_key = _hkdf_sha256(crypto_scalarmult(recovery_secret, envelope.ephemeral_public_key), context)
    key = crypto_aead_xchacha20poly1305_ietf_decrypt(envelope.ciphertext, context, envelope.nonce, wrap_key)
    if len(key) != crypto_aead_xchacha20poly1305_ietf_KEYBYTES:
        raise ValueError("invalid unwrapped recovery epoch key")
    return key


@dataclass(frozen=True)
class EntryHeader:
    thread_id: str
    sequence: int
    previous_block_hash: bytes
    author_did: str
    kind: str
    membership_epoch: int
    encryption_epoch: int
    nonce: bytes

    def canonical(self) -> bytes:
        if len(self.nonce) != crypto_aead_xchacha20poly1305_ietf_NPUBBYTES:
            raise ValueError("thread nonce must be 24 bytes")
        def blob(value: bytes) -> bytes:
            return struct.pack(">I", len(value)) + value
        return b"".join((
            blob(self.thread_id.encode()), struct.pack(">Q", self.sequence),
            blob(self.previous_block_hash), blob(self.author_did.encode()), blob(self.kind.encode()),
            struct.pack(">Q", self.membership_epoch), struct.pack(">Q", self.encryption_epoch), blob(self.nonce),
        ))


def encrypt_entry(identity: AgentIdentity, key: bytes, *, thread_id: str, sequence: int,
                  previous_block_hash: bytes = b"", kind: str = "message",
                  membership_epoch: int, encryption_epoch: int, plaintext: bytes) -> pb.ThreadEntry:
    """Encrypt and DID-sign one v2 entry; daemon receives ciphertext only."""
    if len(key) != crypto_aead_xchacha20poly1305_ietf_KEYBYTES:
        raise ValueError("thread key must be 32 bytes")
    header = EntryHeader(thread_id, sequence, previous_block_hash, identity.did, kind, membership_epoch, encryption_epoch,
                         random(crypto_aead_xchacha20poly1305_ietf_NPUBBYTES))
    aad = header.canonical()
    ciphertext = crypto_aead_xchacha20poly1305_ietf_encrypt(plaintext, aad, header.nonce, key)
    return pb.ThreadEntry(author_did=identity.did, payload=ciphertext, kind=kind, encoding_version=2,
                          sequence=sequence, previous_block_hash=previous_block_hash,
                          membership_epoch=membership_epoch, encryption_epoch=encryption_epoch,
                          nonce=header.nonce, author_signature=identity.sign(aad + ciphertext))


def decrypt_entry_for_thread(key: bytes, thread_id: str, entry: pb.ThreadEntry) -> bytes:
    if len(key) != crypto_aead_xchacha20poly1305_ietf_KEYBYTES:
        raise ValueError("thread key must be 32 bytes")
    header = EntryHeader(thread_id, entry.sequence, entry.previous_block_hash, entry.author_did, entry.kind,
                         entry.membership_epoch, entry.encryption_epoch, entry.nonce)
    return crypto_aead_xchacha20poly1305_ietf_decrypt(entry.payload, header.canonical(), entry.nonce, key)
