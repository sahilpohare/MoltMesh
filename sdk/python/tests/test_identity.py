import pytest

from moltmesh.identity import AgentIdentity
from moltmesh.threadcrypto import decrypt_entry_for_thread, encrypt_entry, epoch_key_context, _hkdf_sha256, unwrap_epoch_key, unwrap_recovery_epoch_key, wrap_epoch_key, wrap_recovery_epoch_key


def test_load_or_create_is_stable_and_scoped(tmp_path):
    first = AgentIdentity.load_or_create(tmp_path / "agent-a")
    second = AgentIdentity.load_or_create(tmp_path / "agent-a")
    other = AgentIdentity.load_or_create(tmp_path / "agent-b")

    assert first.did == second.did
    assert first.did != other.did
    assert len(first.signing_public_key) == 32
    assert len(first.encryption_public_key) == 32
    assert (tmp_path / "agent-a" / "identity.json").stat().st_mode & 0o777 == 0o600


def test_signed_agent_card_binds_did_and_canonical_payload(tmp_path):
    from moltmesh.proto import a2a_pb2 as pb

    identity = AgentIdentity.load_or_create(tmp_path / "agent")
    card = identity.sign_agent_card(pb.AgentCard(name="calculator", skills=[pb.Skill(id="a2a:v1:cap:calculator")]))
    assert card.did == identity.did
    assert card.signature
    assert card.expires_at > card.published_at


def test_thread_entry_xchacha_envelope_round_trip(tmp_path):
    identity = AgentIdentity.load_or_create(tmp_path / "agent")
    key = b"k" * 32
    entry = encrypt_entry(identity, key, thread_id="thread-1", sequence=7,
                          membership_epoch=2, encryption_epoch=2, plaintext=b"secret")
    assert entry.encoding_version == 2
    assert entry.author_signature
    assert decrypt_entry_for_thread(key, "thread-1", entry) == b"secret"
    with pytest.raises(Exception):
        decrypt_entry_for_thread(key, "wrong-thread", entry)


def test_epoch_key_envelope_unwraps_for_recipient(tmp_path):
    from nacl.bindings import crypto_aead_xchacha20poly1305_ietf_encrypt, crypto_scalarmult, crypto_scalarmult_base
    from nacl.utils import random
    from moltmesh.proto import a2a_pb2 as pb

    recipient = AgentIdentity.load_or_create(tmp_path / "recipient")
    ephemeral_private = random(32)
    context = epoch_key_context("thread-1", 2)
    wrapping_key = _hkdf_sha256(crypto_scalarmult(ephemeral_private, recipient.encryption_public_key), context)
    nonce = random(24)
    envelope = pb.ThreadKeyEnvelope(thread_id="thread-1", encryption_epoch=2, recipient_did=recipient.did,
                                    ephemeral_public_key=crypto_scalarmult_base(ephemeral_private), nonce=nonce,
                                    ciphertext=crypto_aead_xchacha20poly1305_ietf_encrypt(b"k" * 32, context, nonce, wrapping_key))
    assert unwrap_epoch_key(recipient, envelope) == b"k" * 32


def test_epoch_key_envelope_wrap_helper_round_trip(tmp_path):
    recipient = AgentIdentity.load_or_create(tmp_path / "recipient")
    envelope = wrap_epoch_key(thread_id="thread-1", encryption_epoch=3, recipient_did=recipient.did,
                              recipient_public_key=recipient.encryption_public_key, epoch_key=b"e" * 32)
    assert unwrap_epoch_key(recipient, envelope) == b"e" * 32


def test_recovery_key_envelope_wrap_unwrap():
    recovery_secret = b"r" * 32
    epoch_key = b"k" * 32
    envelope = wrap_recovery_epoch_key(thread_id="thread-1", encryption_epoch=2,
                                       recovery_secret=recovery_secret, epoch_key=epoch_key)
    assert envelope.recovery_envelope
    assert not envelope.recipient_did
    assert unwrap_recovery_epoch_key(recovery_secret, envelope) == epoch_key
    with pytest.raises(Exception):
        unwrap_recovery_epoch_key(b"x" * 32, envelope)
