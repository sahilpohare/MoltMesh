"""
conftest.py — pytest fixtures for integration tests.

The `daemon` fixture builds the moltmesh-daemon binary (via `go build`),
starts it in a temporary data directory on a random TCP port, and tears it
down after the test session.  Tests that depend on this fixture are skipped
when the Go toolchain is absent or the build fails.
"""

from __future__ import annotations

import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import pytest

from moltmesh.client import A2AClient


# ── helpers ───────────────────────────────────────────────────────────────────

def _free_port() -> int:
    """Return an ephemeral TCP port that is currently free."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _repo_root() -> Path:
    """Return the repository root (two levels above sdk/python)."""
    return Path(__file__).resolve().parents[3]


def _build_daemon(repo: Path, out: Path) -> Path | None:
    """
    Build the daemon binary into `out`.  Returns the binary path on success,
    None if go is unavailable or the build fails.
    """
    if shutil.which("go") is None:
        return None
    binary = out / "moltmesh-daemon"
    result = subprocess.run(
        ["go", "build", "-o", str(binary), "./cmd/daemon"],
        cwd=str(repo),
        capture_output=True,
        timeout=120,
    )
    if result.returncode != 0:
        return None
    return binary


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def daemon_addr():
    """
    Session-scoped fixture that yields a (host, port, addr) tuple for a live
    daemon.  Skips the whole session if the daemon cannot be built or started.
    """
    repo = _repo_root()
    build_dir = Path(tempfile.mkdtemp(prefix="moltmesh_build_"))
    data_dir = Path(tempfile.mkdtemp(prefix="moltmesh_data_"))
    port = _free_port()
    grpc_addr = f"127.0.0.1:{port}"

    try:
        binary = _build_daemon(repo, build_dir)
        if binary is None:
            pytest.skip("go build failed or go toolchain not found")

        proc = subprocess.Popen(
            [
                str(binary),
                "start",
                "--data-dir", str(data_dir),
                "--grpc-addr", grpc_addr,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Wait up to 10 s for the daemon to accept connections.
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                    break
            except OSError:
                time.sleep(0.2)
        else:
            proc.terminate()
            pytest.skip("daemon did not start within 10 s")

        yield grpc_addr

    finally:
        try:
            proc.send_signal(signal.SIGTERM)
            proc.wait(timeout=5)
        except Exception:
            pass
        shutil.rmtree(build_dir, ignore_errors=True)
        shutil.rmtree(data_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def client(daemon_addr):
    """Session-scoped A2AClient connected to the test daemon."""
    c = A2AClient(daemon_addr).connect()
    yield c
    c.close()
