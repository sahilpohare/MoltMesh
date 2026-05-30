"""Tests for moltmesh.client — unit tests (no daemon required)."""

import os
from unittest.mock import patch

import pytest

from moltmesh.client import A2AClient, _default_addr


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
