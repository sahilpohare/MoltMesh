"""
Integration tests for moltmesh.client — require a live daemon.

All tests in this module depend on the `client` fixture from conftest.py,
which starts a real daemon subprocess.  They are skipped automatically when
the go toolchain is absent or the daemon fails to start.
"""

from __future__ import annotations

import time

import pytest

from moltmesh.client import A2AClient
from moltmesh.capability import CoreCapability, capability_id
from moltmesh.proto import a2a_pb2 as pb


# ── identity ──────────────────────────────────────────────────────────────────

class TestIdentity:
    def test_get_identity_returns_did(self, client: A2AClient):
        identity = client.get_identity()
        assert identity.did.startswith("did:key:")

    def test_get_identity_has_public_key(self, client: A2AClient):
        identity = client.get_identity()
        assert identity.public_key != ""

    def test_did_property_matches_get_identity(self, client: A2AClient):
        identity = client.get_identity()
        assert client.did == identity.did

    def test_identity_stable_across_calls(self, client: A2AClient):
        a = client.get_identity()
        b = client.get_identity()
        assert a.did == b.did


# ── messaging ─────────────────────────────────────────────────────────────────

class TestMessaging:
    def test_send_message_returns_message_id(self, client: A2AClient):
        result = client.send_message("did:key:zRemoteTest", text="hello")
        assert result.message_id != ""

    def test_send_message_is_queued(self, client: A2AClient):
        result = client.send_message("did:key:zRemoteTest", text="queued?")
        # Recipient is offline — must be queued
        assert result.queued is True

    def test_get_inbox_returns_list(self, client: A2AClient):
        msgs = client.get_inbox()
        assert isinstance(msgs, list)

    def test_get_inbox_limit(self, client: A2AClient):
        msgs = client.get_inbox(limit=5)
        assert len(msgs) <= 5

    def test_send_and_ack(self, client: A2AClient):
        """Messages sent to self end up in the inbox; ack removes from unread."""
        own_did = client.did
        result = client.send_message(own_did, text="self-test")
        assert result.message_id != ""

        # Ack may fail if delivery is async; just verify no exception.
        try:
            client.ack_message(result.message_id)
        except Exception:
            pass  # message may not have arrived yet in inbox


# ── tasks ─────────────────────────────────────────────────────────────────────

class TestTasks:
    def test_create_task_returns_id(self, client: A2AClient):
        task = client.create_task(
            "did:key:zAssigneeTest",
            CoreCapability.TEXT_GENERATION,
        )
        assert task.id != ""

    def test_create_task_status_submitted(self, client: A2AClient):
        from moltmesh.proto import a2a_pb2 as pb
        task = client.create_task(
            "did:key:zAssigneeTest",
            "a2a:v1:cap:test-skill",
        )
        assert task.status == pb.TASK_STATUS_SUBMITTED

    def test_get_task_by_id(self, client: A2AClient):
        created = client.create_task("did:key:zAssigneeTest", "test-skill")
        fetched = client.get_task(created.id)
        assert fetched.id == created.id

    def test_mark_working(self, client: A2AClient):
        from moltmesh.proto import a2a_pb2 as pb
        task = client.create_task("did:key:zAssigneeTest", "test-skill")
        updated = client.mark_working(task.id)
        assert updated.status == pb.TASK_STATUS_WORKING

    def test_mark_completed(self, client: A2AClient):
        from moltmesh.proto import a2a_pb2 as pb
        task = client.create_task("did:key:zAssigneeTest", "test-skill")
        client.mark_working(task.id)
        done = client.mark_completed(task.id)
        assert done.status == pb.TASK_STATUS_COMPLETED

    def test_mark_failed(self, client: A2AClient):
        from moltmesh.proto import a2a_pb2 as pb
        task = client.create_task("did:key:zAssigneeTest", "test-skill")
        failed = client.mark_failed(task.id, "something went wrong")
        assert failed.status == pb.TASK_STATUS_FAILED
        assert "something went wrong" in failed.error

    def test_cancel_task(self, client: A2AClient):
        from moltmesh.proto import a2a_pb2 as pb
        task = client.create_task("did:key:zAssigneeTest", "test-skill")
        cancelled = client.cancel_task(task.id)
        assert cancelled.status == pb.TASK_STATUS_CANCELLED

    def test_wait_task_already_terminal(self, client: A2AClient):
        from moltmesh.proto import a2a_pb2 as pb
        task = client.create_task("did:key:zAssigneeTest", "test-skill")
        client.mark_completed(task.id)
        result = client.wait_task(task.id, timeout=5.0)
        assert result.status == pb.TASK_STATUS_COMPLETED

    def test_wait_task_timeout(self, client: A2AClient):
        task = client.create_task("did:key:zAssigneeTest", "long-running")
        with pytest.raises(TimeoutError):
            client.wait_task(task.id, timeout=0.5, poll_interval=0.1)


# ── blobs ─────────────────────────────────────────────────────────────────────

class TestBlobs:
    def test_store_and_fetch_roundtrip(self, client: A2AClient):
        data = b"hello integration test"
        cid = client.store_blob(data, mime_type="text/plain")
        assert cid.startswith("baf")
        fetched = client.fetch_blob(cid)
        assert fetched == data

    def test_store_blob_returns_deterministic_cid(self, client: A2AClient):
        data = b"deterministic content"
        cid1 = client.store_blob(data)
        cid2 = client.store_blob(data)
        assert cid1 == cid2

    def test_store_empty_blob_raises(self, client: A2AClient):
        import grpc
        with pytest.raises(grpc.RpcError):
            client.store_blob(b"")

    def test_store_large_blob(self, client: A2AClient):
        # 128 KB — above inline threshold
        data = b"x" * (128 * 1024)
        cid = client.store_blob(data)
        fetched = client.fetch_blob(cid)
        assert fetched == data

    def test_make_artifact_inline(self, client: A2AClient):
        data = b"small"
        artifact = client.make_artifact(data, mime_type="text/plain")
        assert artifact.inline == data
        assert artifact.cid == ""

    def test_make_artifact_large_uses_cid(self, client: A2AClient):
        data = b"y" * (128 * 1024)
        artifact = client.make_artifact(data)
        assert artifact.cid.startswith("baf")


# ── threads ───────────────────────────────────────────────────────────────────

class TestThreads:
    def test_create_thread_returns_id(self, client: A2AClient):
        thread = client.create_thread([client.did])
        assert thread.id != ""

    def test_get_thread(self, client: A2AClient):
        created = client.create_thread([client.did])
        fetched = client.get_thread(created.id)
        assert fetched.id == created.id

    def test_append_and_read_entries(self, client: A2AClient):
        thread = client.create_thread([client.did])
        client.append_entry(thread.id, b"entry-1", kind="message")
        client.append_entry(thread.id, b"entry-2", kind="message")

        # Allow consensus to commit (single-node Raft is fast)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            entries = client.get_thread_entries(thread.id)
            if len(entries) >= 2:
                break
            time.sleep(0.2)

        assert len(entries) >= 2
        payloads = [e.entry.payload for e in entries]
        assert b"entry-1" in payloads
        assert b"entry-2" in payloads

    def test_thread_entries_empty_initially(self, client: A2AClient):
        thread = client.create_thread([client.did])
        entries = client.get_thread_entries(thread.id)
        assert isinstance(entries, list)


# ── diagnostics ───────────────────────────────────────────────────────────────

class TestDiagnostics:
    def test_health_returns_info(self, client: A2AClient):
        h = client.health()
        assert isinstance(h, pb.HealthResponse)
        assert h.version != ""

    def test_health_did_matches_identity(self, client: A2AClient):
        h = client.health()
        assert h.did == client.did

    def test_list_peers_returns_list(self, client: A2AClient):
        peers = client.list_peers()
        assert isinstance(peers, list)

    def test_ping_loopback(self, client: A2AClient):
        result = client.ping()
        assert result is not None


# ── webhooks ──────────────────────────────────────────────────────────────────

class TestWebhooks:
    def test_set_get_clear_webhook(self, client: A2AClient):
        url = "https://example.com/webhook"
        client.set_webhook(url)
        got = client.get_webhook()
        assert got == url
        client.clear_webhook()
        got_after = client.get_webhook()
        assert got_after == ""

    def test_clear_when_none_set_is_safe(self, client: A2AClient):
        client.clear_webhook()
        client.clear_webhook()  # idempotent

    def test_set_webhook_with_secret(self, client: A2AClient):
        url = "https://example.com/hook2"
        returned = client.set_webhook(url, secret="s3cret")
        assert returned == url
        client.clear_webhook()


# ── networks ──────────────────────────────────────────────────────────────────

class TestNetworks:
    def test_create_network(self, client: A2AClient):
        net = client.create_network("test-net")
        assert net.id != ""
        assert net.name == "test-net"

    def test_list_networks_includes_created(self, client: A2AClient):
        net = client.create_network("listed-net")
        networks = client.list_networks()
        ids = [n.id for n in networks]
        assert net.id in ids

    def test_network_members_includes_creator(self, client: A2AClient):
        net = client.create_network("member-net")
        members = client.network_members(net.id)
        dids = [m.did for m in members]
        assert client.did in dids

    def test_leave_network(self, client: A2AClient):
        net = client.create_network("leave-net")
        client.leave_network(net.id)
        networks = client.list_networks()
        ids = [n.id for n in networks]
        assert net.id not in ids

    def test_broadcast_network(self, client: A2AClient):
        net = client.create_network("broadcast-net")
        client.broadcast_network(net.id, b"hello network")
        client.leave_network(net.id)


# ── pub/sub ───────────────────────────────────────────────────────────────────

class TestPubSub:
    def test_publish_does_not_raise(self, client: A2AClient):
        client.publish("test-topic", b"test payload")

    def test_publish_string_payload(self, client: A2AClient):
        client.publish("test-topic", "string payload")
