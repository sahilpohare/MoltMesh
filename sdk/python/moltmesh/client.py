"""
moltmesh.client — gRPC client for the p2p-a2a daemon.

Usage:
    from moltmesh import A2AClient

    with A2AClient() as client:
        me = client.get_identity()
        print(me.did)

        # send a message
        client.send_message("did:key:z6Mk...", text="hello")

        # delegate a task and wait for it
        task = client.create_task("did:key:z6Mk...", "a2a:v1:cap:text-generation",
                                   metadata={"prompt": "summarise this"})
        result = client.wait_task(task.id)

        # replicated thread
        thread = client.create_thread(replica_dids=[me.did], f=0)
        client.append_entry(thread.id, b"hello", kind="message")
        for entry in client.get_thread_entries(thread.id):
            print(entry.entry.payload)

        # blobs
        cid = client.store_blob(b"raw bytes", mime_type="text/plain")
        data = client.fetch_blob(cid)
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Iterator

import grpc

from moltmesh.proto import a2a_pb2 as pb
from moltmesh.proto import a2a_pb2_grpc as rpc


def _default_addr() -> str:
    env = os.environ.get("A2A_GRPC_ADDR", "")
    if env:
        return env
    sock = Path.home() / ".p2p-a2a" / "a2a.sock"
    return f"unix://{sock}"


class A2AClient:
    """
    Synchronous gRPC client for the p2p-a2a daemon.

    Use as a context manager or call connect()/close() manually.
    """

    # Task status constants — no need to import pb directly
    STATUS_SUBMITTED = pb.TASK_STATUS_SUBMITTED
    STATUS_WORKING = pb.TASK_STATUS_WORKING
    STATUS_COMPLETED = pb.TASK_STATUS_COMPLETED
    STATUS_FAILED = pb.TASK_STATUS_FAILED
    STATUS_CANCELLED = pb.TASK_STATUS_CANCELLED

    def __init__(self, addr: str | None = None) -> None:
        self._addr = addr or _default_addr()
        self._channel: grpc.Channel | None = None
        self._stub: rpc.A2ANodeStub | None = None

    # ── lifecycle ────────────────────────────────────────────────────────────

    def connect(self) -> "A2AClient":
        self._channel = grpc.insecure_channel(self._addr)
        self._stub = rpc.A2ANodeStub(self._channel)
        return self

    def close(self) -> None:
        if self._channel:
            self._channel.close()
            self._channel = None
            self._stub = None

    def __enter__(self) -> "A2AClient":
        return self.connect()

    def __exit__(self, *_) -> None:
        self.close()

    @property
    def stub(self) -> rpc.A2ANodeStub:
        if self._stub is None:
            raise RuntimeError(
                "not connected — use 'with A2AClient() as c' or call connect()"
            )
        return self._stub

    # ── identity ─────────────────────────────────────────────────────────────

    def get_identity(self) -> pb.AgentIdentity:
        """Return this daemon's DID, public key, and multiaddrs."""
        return self.stub.GetIdentity(pb.Empty())

    @property
    def did(self) -> str:
        """Shortcut: this daemon's DID string."""
        return self.get_identity().did

    # ── registry ─────────────────────────────────────────────────────────────

    def publish_agent_card(self, card: pb.AgentCard) -> pb.PublishResult:
        """Publish an AgentCard to the DHT."""
        return self.stub.PublishAgentCard(card)

    def get_agent_card(self, did: str) -> pb.AgentCard:
        """Resolve an AgentCard by DID."""
        return self.stub.GetAgentCard(pb.AgentIdentityRequest(did=did))

    def find_agents(self, capability: str, limit: int = 10) -> list[pb.AgentCard]:
        """Search the DHT for agents advertising a capability."""
        return list(
            self.stub.FindAgents(pb.CapabilityQuery(capability=capability, limit=limit))
        )

    # ── messaging ─────────────────────────────────────────────────────────────

    def send_message(
        self,
        to_did: str,
        text: str,
        *,
        thread_id: str = "",
        task_id: str = "",
    ) -> pb.SendResult:
        """Send a plain-text message to another agent."""
        payload = pb.TextMessage(text=text).SerializeToString()
        return self.stub.SendMessage(
            pb.Message(
                to_did=to_did,
                thread_id=thread_id,
                task_id=task_id,
                kind=pb.MESSAGE_KIND_TEXT,
                payload=payload,
            )
        )

    def get_inbox(
        self,
        *,
        thread_id: str = "",
        task_id: str = "",
        unread_only: bool = False,
        limit: int = 50,
        since: int = 0,
    ) -> list[pb.Message]:
        """Fetch messages from the inbox."""
        return list(
            self.stub.GetInbox(
                pb.InboxQuery(
                    thread_id=thread_id,
                    task_id=task_id,
                    unread_only=unread_only,
                    limit=limit,
                    since=since,
                )
            )
        )

    def ack_message(self, message_id: str) -> None:
        """Mark a message as read."""
        self.stub.AckMessage(pb.AckRequest(message_id=message_id))

    def subscribe_inbox(
        self,
        *,
        thread_id: str = "",
        task_id: str = "",
    ) -> Iterator[pb.Message]:
        """Stream incoming messages as they arrive."""
        return self.stub.SubscribeInbox(
            pb.SubscribeRequest(
                thread_id=thread_id,
                task_id=task_id,
            )
        )

    # ── tasks ─────────────────────────────────────────────────────────────────

    def create_task(
        self,
        to_did: str,
        skill: str,
        *,
        thread_id: str = "",
        input_artifacts: list[pb.Artifact] | None = None,
        metadata: dict[str, str] | None = None,
    ) -> pb.Task:
        """Delegate a task to another agent."""
        return self.stub.CreateTask(
            pb.CreateTaskRequest(
                to_did=to_did,
                task=pb.TaskRequest(
                    skill=skill,
                    thread_id=thread_id,
                    input_artifacts=input_artifacts or [],
                    metadata=metadata or {},
                ),
            )
        )

    def get_task(self, task_id: str) -> pb.Task:
        """Fetch current task state."""
        return self.stub.GetTask(pb.TaskID(id=task_id))

    def wait_task(
        self,
        task_id: str,
        *,
        poll_interval: float = 0.5,
        timeout: float = 60.0,
    ) -> pb.Task:
        """
        Block until a task reaches a terminal state (completed/failed/cancelled).
        Raises TimeoutError if it doesn't settle within `timeout` seconds.
        """
        terminal = {
            pb.TASK_STATUS_COMPLETED,
            pb.TASK_STATUS_FAILED,
            pb.TASK_STATUS_CANCELLED,
        }
        deadline = time.monotonic() + timeout
        while True:
            task = self.get_task(task_id)
            if task.status in terminal:
                return task
            if time.monotonic() >= deadline:
                raise TimeoutError(f"task {task_id} did not complete within {timeout}s")
            time.sleep(poll_interval)

    def mark_working(self, task_id: str) -> pb.Task:
        """Signal that this agent has started working on a task."""
        return self._update_task(task_id, pb.TASK_STATUS_WORKING)

    def mark_completed(
        self,
        task_id: str,
        *,
        output_artifacts: list[pb.Artifact] | None = None,
    ) -> pb.Task:
        """Mark a task as successfully completed."""
        return self._update_task(
            task_id, pb.TASK_STATUS_COMPLETED, output_artifacts=output_artifacts
        )

    def mark_failed(self, task_id: str, error: str) -> pb.Task:
        """Mark a task as failed with an error message."""
        return self._update_task(task_id, pb.TASK_STATUS_FAILED, error=error)

    def cancel_task(self, task_id: str) -> pb.Task:
        """Cancel a task."""
        return self.stub.CancelTask(pb.TaskID(id=task_id))

    def subscribe_task_events(self, task_id: str) -> Iterator[pb.TaskEvent]:
        """Stream task events (token chunks, tool calls, status changes)."""
        return self.stub.SubscribeTaskEvents(pb.TaskID(id=task_id))

    def _update_task(
        self,
        task_id: str,
        status: int,
        *,
        error: str = "",
        output_artifacts: list[pb.Artifact] | None = None,
    ) -> pb.Task:
        return self.stub.UpdateTask(
            pb.TaskStatusUpdate(
                task_id=task_id,
                status=status,
                error=error,
                output_artifacts=output_artifacts or [],
            )
        )

    # ── blobs ─────────────────────────────────────────────────────────────────

    def store_blob(
        self,
        data: bytes,
        *,
        mime_type: str = "application/octet-stream",
        filename: str = "",
    ) -> str:
        """
        Store bytes in the blob store. Returns the CID (SHA-256 hex).

        For files on disk, use store_file() instead.
        """
        result = self.stub.StoreBlob(
            pb.BlobData(
                data=data,
                mime_type=mime_type,
                filename=filename,
            )
        )
        return result.cid

    def store_file(self, path: str | Path, *, mime_type: str = "") -> str:
        """
        Store a file from disk. Returns its CID.
        MIME type is guessed from the extension if not provided.
        """
        path = Path(path)
        if not mime_type:
            import mimetypes

            mime_type = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        return self.store_blob(
            path.read_bytes(), mime_type=mime_type, filename=path.name
        )

    def fetch_blob(self, cid: str) -> bytes:
        """Fetch a blob by CID. Returns raw bytes."""
        result = self.stub.FetchBlob(pb.BlobRequest(cid=cid))
        return result.data

    def fetch_blob_to_file(self, cid: str, dest: str | Path) -> Path:
        """Fetch a blob and write it to `dest`. Returns the path."""
        dest = Path(dest)
        dest.write_bytes(self.fetch_blob(cid))
        return dest

    # ── threads ───────────────────────────────────────────────────────────────

    def create_thread(
        self,
        replica_dids: list[str],
        *,
        f: int = 0,
        epoch_ms: int = 200,
        backend: str = "raft",
    ) -> pb.Thread:
        """
        Create a replicated ordered log.

        Args:
            replica_dids: DIDs of all validator nodes (must include your own DID).
            f:            Number of tolerated faults. f=0 → single-node Raft (fast).
                          f≥1 with backend="tendermint" → Byzantine fault tolerance.
            epoch_ms:     Tick interval in milliseconds.
            backend:      "raft" (default, CFT) or "tendermint" (BFT).
        """
        return self.stub.CreateThread(
            pb.CreateThreadRequest(
                replica_dids=replica_dids,
                f=f,
                epoch_ms=epoch_ms,
                metadata={"backend": backend},
            )
        )

    def get_thread(self, thread_id: str) -> pb.Thread:
        """Fetch thread metadata."""
        return self.stub.GetThread(pb.ThreadID(id=thread_id))

    def append_entry(
        self,
        thread_id: str,
        payload: bytes,
        *,
        kind: str = "message",
        author_did: str = "",
    ) -> None:
        """
        Enqueue an entry for the next committed block on this thread.
        The entry is committed asynchronously by the consensus engine.
        """
        self.stub.AppendEntry(
            pb.AppendEntryRequest(
                thread_id=thread_id,
                entry=pb.ThreadEntry(
                    author_did=author_did,
                    payload=payload,
                    kind=kind,
                ),
            )
        )

    def get_thread_entries(
        self,
        thread_id: str,
        *,
        since_height: int = 0,
        limit: int = 0,
    ) -> list[pb.ThreadEntryWithPos]:
        """
        Return committed entries after `since_height`.
        limit=0 means no cap.
        """
        return list(
            self.stub.GetThreadEntries(
                pb.GetThreadEntriesRequest(
                    thread_id=thread_id,
                    since_height=since_height,
                    limit=limit,
                )
            )
        )

    def subscribe_thread(self, thread_id: str) -> Iterator[pb.ThreadEntryWithPos]:
        """Stream committed entries live as they are appended."""
        return self.stub.SubscribeThread(pb.SubscribeThreadRequest(thread_id=thread_id))

    # ── artifact helpers ──────────────────────────────────────────────────────

    def make_artifact(
        self,
        data: bytes,
        *,
        mime_type: str = "application/octet-stream",
        filename: str = "",
        inline_threshold: int = 65536,
    ) -> pb.Artifact:
        """
        Build an Artifact, storing large payloads in the blob store automatically.

        Files ≤ inline_threshold bytes are inlined; larger ones are stored and
        referenced by CID.
        """
        if len(data) <= inline_threshold:
            return pb.Artifact(
                data=data, mime_type=mime_type, filename=filename, size=len(data)
            )
        cid = self.store_blob(data, mime_type=mime_type, filename=filename)
        return pb.Artifact(
            cid=cid, mime_type=mime_type, filename=filename, size=len(data)
        )
