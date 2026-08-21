"""Tests for moltmesh.client — unit tests (no daemon required)."""

import os
from unittest.mock import MagicMock, patch

import pytest

from moltmesh.client import A2AClient, _default_addr
from moltmesh.proto import a2a_pb2 as pb


class TestDefaultAddr:
    def test_env_override(self):
        with patch.dict(os.environ, {"A2A_GRPC_ADDR": "localhost:9999"}):
            assert _default_addr() == "localhost:9999"

    def test_default_unix_socket(self):
        with patch.dict(os.environ, {}, clear=True):
            # Remove A2A_GRPC_ADDR if set
            os.environ.pop("A2A_GRPC_ADDR", None)
            addr = _default_addr()
            assert addr.startswith("unix://")
            assert "a2a.sock" in addr


class TestClientLifecycle:
    def test_not_connected_raises(self):
        client = A2AClient("localhost:0")
        with pytest.raises(RuntimeError, match="not connected"):
            _ = client.stub

    def test_not_connected_diag_raises(self):
        client = A2AClient("localhost:0")
        with pytest.raises(RuntimeError, match="not connected"):
            _ = client.diag

    def test_not_connected_ext_raises(self):
        client = A2AClient("localhost:0")
        with pytest.raises(RuntimeError, match="not connected"):
            _ = client.ext

    def test_context_manager_connect_close(self):
        # Connect to an invalid address — connect() itself doesn't fail
        # because gRPC channels are lazy. We just verify the lifecycle.
        with A2AClient("localhost:0") as client:
            assert client._stub is not None
            assert client._channel is not None
        # After exit, everything should be None
        assert client._stub is None
        assert client._channel is None

    def test_close_idempotent(self):
        client = A2AClient("localhost:0")
        client.connect()
        client.close()
        client.close()  # should not raise


class TestStatusConstants:
    def test_constants_exist(self):
        assert A2AClient.STATUS_SUBMITTED is not None
        assert A2AClient.STATUS_WORKING is not None
        assert A2AClient.STATUS_COMPLETED is not None
        assert A2AClient.STATUS_FAILED is not None
        assert A2AClient.STATUS_CANCELLED is not None

    def test_constants_are_distinct(self):
        statuses = {
            A2AClient.STATUS_SUBMITTED,
            A2AClient.STATUS_WORKING,
            A2AClient.STATUS_COMPLETED,
            A2AClient.STATUS_FAILED,
            A2AClient.STATUS_CANCELLED,
        }
        assert len(statuses) == 5


class TestDurableThreadHandoff:
    def setup_method(self):
        self.client = A2AClient("localhost:0")
        self.client._stub = MagicMock()

    def test_send_task_result_serializes_terminal_result(self):
        self.client.send_task_result(
            "did:key:initiator", "task-1", thread_id="thread-1", data=b"4"
        )

        request = self.client.stub.SendTaskResult.call_args.args[0]
        assert request.to_did == "did:key:initiator"
        assert request.thread_id == "thread-1"
        assert request.result.task_id == "task-1"
        assert request.result.status == self.client.STATUS_COMPLETED
        assert request.result.data == b"4"

    def test_observer_uses_durable_thread_rpc_and_bare_recovery_is_retired(self):
        self.client.add_thread_observer("thread-1", "did:key:observer")
        with pytest.raises(RuntimeError, match="bare-ID recovery is retired"):
            self.client.recover_thread("thread-1")

        observer = self.client.stub.AddThreadReplica.call_args.args[0]
        assert (observer.thread_id, observer.replica_did) == ("thread-1", "did:key:observer")
        self.client.stub.RecoverThread.assert_not_called()

    def test_thread_key_envelopes_use_authenticated_transport_rpcs(self):
        envelope = pb.ThreadKeyEnvelope(
            thread_id="thread-1", encryption_epoch=2, recipient_did="did:key:zRecipient",
            ephemeral_public_key=b"x" * 32, nonce=b"n" * 24, ciphertext=b"ciphertext",
        )
        self.client.put_thread_key_envelope(envelope)
        self.client.stub.GetThreadKeyEnvelopes.return_value.envelopes = [envelope]
        assert self.client.get_thread_key_envelopes("thread-1", 2) == [envelope]
        assert self.client.stub.PutThreadKeyEnvelope.call_args.args[0] == envelope
        query = self.client.stub.GetThreadKeyEnvelopes.call_args.args[0]
        assert (query.thread_id, query.encryption_epoch) == ("thread-1", 2)

    @pytest.mark.parametrize(
        ("method", "rpc", "did"),
        [
            ("promote_thread_member", "PromoteThreadMember", "did:key:zObserver"),
            ("remove_thread_member", "RemoveThreadMember", "did:key:zFormerMember"),
        ],
    )
    def test_creator_membership_change_rotates_to_committed_epoch(self, method, rpc, did):
        self.client._identity = MagicMock()
        getattr(self.client.stub, rpc).return_value = pb.ThreadMembershipChange(
            thread_id="thread-1", membership_epoch=7
        )
        self.client.rotate_thread_epoch_key = MagicMock()

        kwargs = {"catchup_proof": b"proof"} if method == "promote_thread_member" else {}
        change = getattr(self.client, method)("thread-1", did, **kwargs)

        assert change.membership_epoch == 7
        self.client.rotate_thread_epoch_key.assert_called_once_with("thread-1", 7)

    def test_sdk_creates_identity_bound_catchup_proof(self):
        from moltmesh.identity import AgentIdentity

        self.client._identity = AgentIdentity.load_or_create("/tmp/moltmesh-catchup-proof-test")
        self.client.stub.GetThreadCatchupState.return_value = pb.ThreadCatchupState(
            thread_id="thread-1", observer_did=self.client._identity.did,
            committed_height=5, head_block_hash="head",
        )
        proof = pb.ThreadCatchupProof()
        proof.ParseFromString(self.client.create_thread_catchup_proof("thread-1"))
        assert (proof.thread_id, proof.observer_did, proof.committed_height, proof.head_block_hash) == ("thread-1", self.client._identity.did, 5, "head")
        assert proof.signature

    def test_full_recovery_stages_history_before_epoch_keys(self):
        handle = pb.ThreadRecoveryHandle(thread_id="thread-1", recovery_secret=b"s" * 32, version=1)
        response = pb.RecoverThreadResponse(access=pb.THREAD_ACCESS_READ_ONLY)
        self.client.recover_thread_with_handle = MagicMock(return_value=response)
        self.client.install_recovery_epoch_keys = MagicMock(return_value=[2, 3])

        recovered, epochs = self.client.recover_thread_fully(handle, subscribe=True)

        assert recovered == response and epochs == [2, 3]
        self.client.recover_thread_with_handle.assert_called_once_with(handle, subscribe=True)
        self.client.install_recovery_epoch_keys.assert_called_once_with(handle)
