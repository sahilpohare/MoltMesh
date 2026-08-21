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
import base64
import hashlib
import threading
import uuid
from collections import namedtuple
from pathlib import Path
from typing import Iterator

import grpc

from moltmesh.proto import a2a_pb2 as pb
from moltmesh.proto import a2a_pb2_grpc as rpc
from moltmesh.identity import AgentIdentity
from moltmesh.threadcrypto import decrypt_entry_for_thread, encrypt_entry, unwrap_epoch_key, unwrap_recovery_epoch_key, wrap_epoch_key, wrap_recovery_epoch_key


class _ClientCallDetails(
    namedtuple("_ClientCallDetails", "method timeout metadata credentials wait_for_ready compression"),
    grpc.ClientCallDetails,
):
    pass


class _SessionInterceptor(grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor):
    def __init__(self, token_provider) -> None:
        self._token_provider = token_provider

    def _details(self, details: grpc.ClientCallDetails) -> _ClientCallDetails:
        metadata = list(details.metadata or ())
        metadata.append(("authorization", f"Bearer {self._token_provider()}"))
        return _ClientCallDetails(details.method, details.timeout, metadata, details.credentials, details.wait_for_ready, details.compression)

    def intercept_unary_unary(self, continuation, client_call_details, request):
        return continuation(self._details(client_call_details), request)

    def intercept_unary_stream(self, continuation, client_call_details, request):
        return continuation(self._details(client_call_details), request)


def _default_addr() -> str:
    env = os.environ.get("A2A_GRPC_ADDR", "")
    if env:
        return env
    sock = Path.home() / ".moltmesh" / "a2a.sock"
    return f"unix://{sock}"


def _session_payload(node_id: str, challenge_id: str, nonce: bytes, expires_at_unix_ms: int, did: str) -> bytes:
    """Canonical bytes signed for Begin/CompleteAgentSession."""
    nonce_b64 = base64.urlsafe_b64encode(nonce).rstrip(b"=").decode()
    return f"moltmesh-agent-session-v1\0{node_id}\0{challenge_id}\0{nonce_b64}\0{expires_at_unix_ms}\0{did}".encode()


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

    def __init__(self, addr: str | None = None, *, identity: AgentIdentity | None = None) -> None:
        self._addr = addr or _default_addr()
        self._identity = identity
        self._channel: grpc.Channel | None = None
        self._raw_stub: rpc.A2ANodeStub | None = None
        self._stub: rpc.A2ANodeStub | None = None
        self._session_token: str | None = None
        self._agent_did = ""
        self._node_peer_id = ""
        self._session_expires_at = 0
        self._session_lock = threading.RLock()
        self._thread_epoch_keys: dict[tuple[str, int], bytes] = {}

    # ── lifecycle ────────────────────────────────────────────────────────────

    def connect(self) -> "A2AClient":
        self._channel = grpc.insecure_channel(self._addr)
        raw_stub = rpc.A2ANodeStub(self._channel)
        self._raw_stub = raw_stub
        if self._identity is not None:
            self._renew_session(force=True)
            self._channel = grpc.intercept_channel(self._channel, _SessionInterceptor(self._session_token_for_request))
            self._stub = rpc.A2ANodeStub(self._channel)
        else:
            self._stub = raw_stub
        return self

    def close(self) -> None:
        if self._channel:
            if self._stub is not None and self._session_token is not None:
                try:
                    self._stub.CloseAgentSession(pb.Empty())
                except grpc.RpcError:
                    pass
            self._channel.close()
            self._channel = None
            self._stub = None
            self._raw_stub = None
            self._session_token = None
            self._session_expires_at = 0

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

    @property
    def diag(self) -> rpc.A2ANodeStub:
        """Compatibility alias for diagnostic RPC access on the node stub."""
        return self.stub

    @property
    def ext(self) -> rpc.A2ANodeStub:
        """Compatibility alias for extension RPC access on the node stub."""
        return self.stub

    def _session_token_for_request(self) -> str:
        """Return a valid short-lived token, renewing shortly before expiry."""
        self._renew_session()
        if not self._session_token:
            raise RuntimeError("agent session is unavailable")
        return self._session_token

    def _renew_session(self, *, force: bool = False) -> None:
        if self._identity is None:
            return
        # Keep a small margin so an RPC/stream setup cannot race session expiry.
        if not force and self._session_token and time.time() < self._session_expires_at - 30:
            return
        with self._session_lock:
            if not force and self._session_token and time.time() < self._session_expires_at - 30:
                return
            if self._raw_stub is None:
                raise RuntimeError("not connected")
            node = self._raw_stub.GetNodeIdentity(pb.Empty())
            challenge = self._raw_stub.BeginAgentSession(
                pb.BeginAgentSessionRequest(identity=pb.AgentIdentity(
                    did=self._identity.did,
                    signing_public_key=self._identity.signing_public_key,
                    encryption_public_key=self._identity.encryption_public_key,
                ))
            )
            signed = _session_payload(node.node_id, challenge.challenge_id, challenge.nonce, challenge.expires_at_unix_ms, self._identity.did)
            created = self._raw_stub.CompleteAgentSession(pb.CompleteAgentSessionRequest(
                challenge_id=challenge.challenge_id,
                signature=self._identity.sign(signed),
            ))
            self._session_token = created.token
            self._session_expires_at = created.expires_at_unix_ms / 1000
            self._agent_did = created.agent_did
            self._node_peer_id = node.peer_id

    # ── identity ─────────────────────────────────────────────────────────────

    def get_identity(self) -> pb.AgentIdentity:
        """Return this daemon's DID, public key, and multiaddrs."""
        return self.stub.GetIdentity(pb.Empty())

    @property
    def did(self) -> str:
        """Shortcut: this daemon's DID string."""
        return self.get_identity().did

    @property
    def agent_did(self) -> str:
        """Authenticated SDK agent DID, empty in legacy daemon-identity mode."""
        return self._agent_did

    @property
    def node_peer_id(self) -> str:
        """Connected daemon's libp2p peer ID, if the daemon exposes one."""
        return self._node_peer_id

    # ── registry ─────────────────────────────────────────────────────────────

    def publish_agent_card(self, card: pb.AgentCard) -> pb.PublishResult:
        """Publish an AgentCard to the DHT."""
        if self._identity is not None:
            self._identity.sign_agent_card(card, node_peer_id=self._node_peer_id)
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
        idempotency_key: str = "",
        timeout_ms: int = 0,
        max_attempts: int = 0,
    ) -> pb.Task:
        """Delegate a task to another agent."""
        return self.stub.CreateTask(
            pb.CreateTaskRequest(
                to_did=to_did,
                idempotency_key=idempotency_key,
                timeout_ms=timeout_ms,
                max_attempts=max_attempts,
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
        # Preserve the historical convenience helper while the daemon keeps a
        # strict submitted -> working -> terminal state machine. New workers
        # should use complete_task_lease instead.
        if self.get_task(task_id).status == pb.TASK_STATUS_SUBMITTED:
            self.mark_working(task_id)
        return self._update_task(
            task_id, pb.TASK_STATUS_COMPLETED, output_artifacts=output_artifacts
        )

    def mark_failed(self, task_id: str, error: str) -> pb.Task:
        """Mark a task as failed with an error message."""
        return self._update_task(task_id, pb.TASK_STATUS_FAILED, error=error)

    def cancel_task(self, task_id: str) -> pb.Task:
        """Cancel a task."""
        return self.stub.CancelTask(pb.TaskID(id=task_id))

    def subscribe_task_events(self, task_id: str, after_sequence: int = 0) -> Iterator[pb.TaskEvent]:
        """Stream task events (token chunks, tool calls, status changes)."""
        return self.stub.SubscribeTaskEvents(pb.TaskID(id=task_id, after_sequence=after_sequence))

    def send_task_result(
        self,
        to_did: str,
        task_id: str,
        *,
        thread_id: str = "",
        status: int = pb.TASK_STATUS_COMPLETED,
        output_artifacts: list[pb.Artifact] | None = None,
        error: str = "",
        data: bytes = b"",
    ) -> pb.SendResult:
        """Durably return a terminal task result to its initiator.

        The daemon queues delivery while the initiator is offline and applies a
        received result idempotently.  ``status`` must be COMPLETED or FAILED.
        """
        return self.stub.SendTaskResult(
            pb.SendTaskResultRequest(
                to_did=to_did,
                thread_id=thread_id,
                result=pb.TaskResult(
                    task_id=task_id,
                    status=status,
                    output_artifacts=output_artifacts or [],
                    error=error,
                    data=data,
                ),
            )
        )

    # ── durable SDK worker leases ───────────────────────────────────────────

    def subscribe_tasks(self, skills: list[str], after_sequence: int = 0) -> Iterator[pb.TaskDelivery]:
        """Replay and follow work deliverable to this authenticated agent."""
        return self.stub.SubscribeTasks(pb.WorkerSubscription(skills=skills, after_sequence=after_sequence))

    def claim_task(self, task_id: str, lease_seconds: int = 30) -> pb.TaskLease:
        return self.stub.ClaimTask(pb.ClaimTaskRequest(task_id=task_id, lease_seconds=lease_seconds))

    def renew_task_lease(self, task_id: str, lease_token: str, lease_seconds: int = 30) -> pb.TaskLease:
        return self.stub.RenewTaskLease(pb.RenewTaskLeaseRequest(task_id=task_id, lease_token=lease_token, lease_seconds=lease_seconds))

    def complete_task_lease(self, task_id: str, lease_token: str, output_artifacts: list[pb.Artifact] | None = None, data: bytes = b"") -> pb.Task:
        return self.stub.CompleteTask(pb.CompleteTaskRequest(task_id=task_id, lease_token=lease_token, output_artifacts=output_artifacts or [], data=data))

    def fail_task_lease(self, task_id: str, lease_token: str, error: str) -> pb.Task:
        return self.stub.FailTask(pb.FailTaskRequest(task_id=task_id, lease_token=lease_token, error=error))

    def worker(self, skills: list[str], *, concurrency: int = 1, lease_seconds: int = 30, checkpoint_path: str | None = None):
        """Create a handler-driven SDK worker; application code supplies handlers."""
        from moltmesh.worker import Worker
        return Worker(self, skills, concurrency=concurrency, lease_seconds=lease_seconds, checkpoint_path=checkpoint_path)

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
        result = self.stub.SendFile(
            pb.SendFileRequest(
                data=data,
                mime_type=mime_type,
                name=filename,
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
        chunks = self.stub.FetchFile(pb.FetchFileRequest(cid=cid))
        return b"".join(chunk.data for chunk in chunks)

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
        metadata = {"backend": backend}
        if self._identity is None:
            return self.stub.CreateThread(pb.CreateThreadRequest(replica_dids=replica_dids, f=f, epoch_ms=epoch_ms, metadata=metadata))
        # An attached SDK agent, not its daemon, owns the descriptor signature.
        replicas = list(dict.fromkeys([self._identity.did, *replica_dids]))
        created_at = int(time.time() * 1000)
        descriptor = pb.Thread(
            id=str(uuid.uuid4()), creator_did=self._identity.did,
            replica_dids=replicas, n=1 if f == 0 else 3 * f + 1,
            f=f, epoch_ms=epoch_ms or 1000, created_at=created_at, metadata=metadata,
        )
        signature = self._identity.sign(descriptor.SerializeToString(deterministic=True))
        return self.stub.CreateThread(pb.CreateThreadRequest(
            replica_dids=replicas, f=f, epoch_ms=descriptor.epoch_ms, metadata=metadata,
            thread_id=descriptor.id, creator_did=descriptor.creator_did,
            creator_signature=signature, created_at=created_at,
        ))

    def get_thread(self, thread_id: str) -> pb.Thread:
        """Fetch thread metadata."""
        return self.stub.GetThread(pb.ThreadID(id=thread_id))

    def create_thread_with_recovery(self, replica_dids: list[str], *, f: int = 0, epoch_ms: int = 200, backend: str = "raft") -> pb.CreateThreadResponse:
        """Create a thread and return its one-time recovery capability."""
        recovery_secret = os.urandom(32)
        metadata = {"backend": backend, "recovery_capability_sha256": hashlib.sha256(recovery_secret).hexdigest()}
        if self._identity is None:
            # The daemon may use this secret directly; returning it from the
            # response keeps the caller on one explicit capability path.
            return self.stub.CreateThreadWithRecovery(pb.CreateThreadRequest(replica_dids=replica_dids, f=f, epoch_ms=epoch_ms, metadata=metadata, recovery_secret=recovery_secret))
        replicas = list(dict.fromkeys([self._identity.did, *replica_dids]))
        created_at = int(time.time() * 1000)
        descriptor = pb.Thread(id=str(uuid.uuid4()), creator_did=self._identity.did, replica_dids=replicas, n=1 if f == 0 else 3*f+1, f=f, epoch_ms=epoch_ms or 1000, created_at=created_at, metadata=metadata)
        return self.stub.CreateThreadWithRecovery(pb.CreateThreadRequest(replica_dids=replicas, f=f, epoch_ms=descriptor.epoch_ms, metadata=descriptor.metadata, thread_id=descriptor.id, creator_did=descriptor.creator_did, creator_signature=self._identity.sign(descriptor.SerializeToString(deterministic=True)), created_at=created_at, recovery_secret=recovery_secret))

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
                payload=payload,
                kind=kind,
            )
        )

    def append_encrypted_entry(self, thread_id: str, entry: pb.ThreadEntry) -> None:
        """Submit an SDK-prepared v2 ciphertext entry.

        The daemon verifies the DID-bound signature over the canonical header
        and ciphertext but never receives the epoch data key.
        """
        self.stub.AppendEntry(pb.AppendEntryRequest(thread_id=thread_id, encrypted_entry=entry))

    def set_thread_epoch_key(self, thread_id: str, encryption_epoch: int, key: bytes) -> None:
        """Install a 32-byte SDK-owned epoch key for automatic entry crypto."""
        if not thread_id or encryption_epoch <= 0 or len(key) != 32:
            raise ValueError("thread_id, positive epoch, and a 32-byte key are required")
        self._thread_epoch_keys[(thread_id, encryption_epoch)] = bytes(key)

    def append_encrypted(self, thread_id: str, plaintext: bytes, *, sequence: int,
                         membership_epoch: int, encryption_epoch: int,
                         previous_block_hash: bytes = b"", kind: str = "message") -> None:
        """Encrypt, sign, and append using the cached SDK epoch key."""
        if self._identity is None:
            raise RuntimeError("encrypted entries require an SDK identity")
        key = self._thread_epoch_keys.get((thread_id, encryption_epoch))
        if key is None:
            raise KeyError(f"no epoch key for thread {thread_id!r}, epoch {encryption_epoch}")
        self.append_encrypted_entry(thread_id, encrypt_entry(
            self._identity, key, thread_id=thread_id, sequence=sequence,
            previous_block_hash=previous_block_hash, kind=kind,
            membership_epoch=membership_epoch, encryption_epoch=encryption_epoch,
            plaintext=plaintext,
        ))

    def decrypt_thread_entry(self, thread_id: str, entry: pb.ThreadEntry) -> bytes:
        """Decrypt a v2 entry with the matching cached epoch key."""
        key = self._thread_epoch_keys.get((thread_id, entry.encryption_epoch))
        if key is None:
            raise KeyError(f"no epoch key for thread {thread_id!r}, epoch {entry.encryption_epoch}")
        return decrypt_entry_for_thread(key, thread_id, entry)

    def put_thread_key_envelope(self, envelope: pb.ThreadKeyEnvelope) -> None:
        """Publish an opaque epoch-key envelope as the authenticated creator."""
        if envelope.recovery_envelope:
            if self._identity is None:
                raise RuntimeError("recovery envelope publication requires an SDK identity")
            envelope.author_signature = b""
            envelope.author_signature = self._identity.sign(envelope.SerializeToString(deterministic=True))
        self.stub.PutThreadKeyEnvelope(envelope)

    def publish_recovery_epoch_key(self, handle: pb.ThreadRecoveryHandle, encryption_epoch: int, epoch_key: bytes) -> None:
        """Store an opaque recovery-capability copy of one SDK epoch key."""
        if handle.version != 1 or len(handle.recovery_secret) != 32:
            raise ValueError("valid recovery handle required")
        self.put_thread_key_envelope(wrap_recovery_epoch_key(thread_id=handle.thread_id, encryption_epoch=encryption_epoch,
                                                              recovery_secret=handle.recovery_secret, epoch_key=epoch_key))

    def get_thread_key_envelopes(self, thread_id: str, encryption_epoch: int) -> list[pb.ThreadKeyEnvelope]:
        """Fetch only envelopes addressed to this SDK session identity."""
        result = self.stub.GetThreadKeyEnvelopes(
            pb.ThreadKeyEnvelopeQuery(thread_id=thread_id, encryption_epoch=encryption_epoch)
        )
        return list(result.envelopes)

    def install_thread_key_envelope(self, envelope: pb.ThreadKeyEnvelope) -> None:
        """Unwrap a fetched envelope with this SDK identity and cache its epoch key."""
        if self._identity is None:
            raise RuntimeError("thread key envelopes require an SDK identity")
        self.set_thread_epoch_key(envelope.thread_id, envelope.encryption_epoch, unwrap_epoch_key(self._identity, envelope))

    def rotate_thread_epoch_key(self, thread_id: str, encryption_epoch: int, epoch_key: bytes | None = None) -> bytes:
        """Generate/cache an epoch key and publish an envelope for each member.

        Member encryption keys are read from signed agent cards. The daemon sees
        only opaque envelopes; the creator retains the plaintext epoch key.
        """
        if self._identity is None:
            raise RuntimeError("key rotation requires an SDK identity")
        key = os.urandom(32) if epoch_key is None else bytes(epoch_key)
        if len(key) != 32:
            raise ValueError("epoch key must be 32 bytes")
        members = self.list_thread_members(thread_id)
        for member in members:
            card = self.get_agent_card(member.did)
            if len(card.encryption_public_key) != 32:
                raise ValueError(f"member {member.did!r} has no X25519 encryption key")
            self.put_thread_key_envelope(wrap_epoch_key(thread_id=thread_id, encryption_epoch=encryption_epoch,
                                                         recipient_did=member.did, recipient_public_key=card.encryption_public_key,
                                                         epoch_key=key))
        self.set_thread_epoch_key(thread_id, encryption_epoch, key)
        return key

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

    def add_thread_observer(self, thread_id: str, did: str) -> pb.Thread:
        """Add ``did`` as a non-voting observer of a thread.

        Observer membership is committed into thread history.  It grants
        replication/read access but does not alter the Raft voter set or quorum.
        """
        return self.stub.AddThreadReplica(
            pb.ThreadReplicaRequest(thread_id=thread_id, replica_did=did)
        )

    def list_thread_members(self, thread_id: str) -> list[pb.ThreadMember]:
        """Return the durable explicit membership view for a thread."""
        return list(self.stub.ListThreadMembers(pb.ThreadID(id=thread_id)).members)

    def accept_thread_invite(self, thread_id: str, signed_invitation: bytes) -> pb.Thread:
        return self.stub.AcceptThreadInvite(pb.AcceptThreadInviteRequest(thread_id=thread_id, invite=signed_invitation))

    def invite_thread_member(self, thread_id: str, invitee_did: str, *, expires_at_unix_ms: int, nonce: bytes) -> pb.ThreadMembershipChange:
        if self._identity is None:
            raise RuntimeError("signed invitations require an SDK identity")
        invitation = pb.ThreadInvitation(thread_id=thread_id, inviter_did=self._identity.did, invitee_did=invitee_did, role=pb.THREAD_MEMBER_ROLE_OBSERVER, expires_at_unix_ms=expires_at_unix_ms, nonce=nonce)
        signature = self._identity.sign(invitation.SerializeToString(deterministic=True))
        return self.stub.InviteThreadMember(pb.InviteThreadMemberRequest(thread_id=thread_id, invitee_did=invitee_did, role=pb.THREAD_MEMBER_ROLE_OBSERVER, expires_at_unix_ms=expires_at_unix_ms, nonce=nonce, signature=signature))

    def create_thread_catchup_proof(self, thread_id: str) -> bytes:
        """Sign this SDK member's current locally committed thread head."""
        if self._identity is None:
            raise RuntimeError("catchup proofs require an SDK identity")
        state = self.stub.GetThreadCatchupState(pb.ThreadID(id=thread_id))
        if state.observer_did != self._identity.did or state.thread_id != thread_id:
            raise RuntimeError("daemon catchup state is not bound to this SDK identity/thread")
        proof = pb.ThreadCatchupProof(thread_id=thread_id, observer_did=self._identity.did,
                                      committed_height=state.committed_height, head_block_hash=state.head_block_hash,
                                      issued_at_unix_ms=int(time.time() * 1000))
        proof.signature = self._identity.sign(proof.SerializeToString(deterministic=True))
        return proof.SerializeToString()

    def promote_thread_member(self, thread_id: str, did: str, *, catchup_proof: bytes, rotate_epoch_key: bool = True) -> pb.ThreadMembershipChange:
        """Promote a caught-up observer and rotate the creator-owned epoch key.

        The membership epoch returned by the daemon is the encryption epoch.
        Rotation occurs only after the committed membership mutation succeeds;
        callers can disable it only for an explicitly managed key ceremony.
        """
        if not catchup_proof:
            raise ValueError("signed catchup proof is required for promotion")
        change = self.stub.PromoteThreadMember(pb.PromoteThreadMemberRequest(thread_id=thread_id, member_did=did, catchup_proof=catchup_proof))
        if rotate_epoch_key:
            if self._identity is None:
                raise RuntimeError("automatic key rotation requires an SDK identity")
            if change.membership_epoch <= 0:
                raise RuntimeError("daemon did not return a committed membership epoch")
            self.rotate_thread_epoch_key(thread_id, int(change.membership_epoch))
        return change

    def remove_thread_member(self, thread_id: str, did: str, *, rotate_epoch_key: bool = True) -> pb.ThreadMembershipChange:
        """Remove a member and rotate the epoch key for the remaining members."""
        change = self.stub.RemoveThreadMember(pb.RemoveThreadMemberRequest(thread_id=thread_id, member_did=did))
        if rotate_epoch_key:
            if self._identity is None:
                raise RuntimeError("automatic key rotation requires an SDK identity")
            if change.membership_epoch <= 0:
                raise RuntimeError("daemon did not return a committed membership epoch")
            self.rotate_thread_epoch_key(thread_id, int(change.membership_epoch))
        return change

    def leave_thread(self, thread_id: str) -> pb.ThreadMembershipChange:
        return self.stub.LeaveThread(pb.ThreadID(id=thread_id))

    def recover_thread(self, thread_id: str) -> pb.Thread:
        """Deprecated: a public thread ID is not a recovery capability."""
        del thread_id
        raise RuntimeError("bare-ID recovery is retired; use recover_thread_with_handle()")

    def recover_thread_with_handle(self, handle: pb.ThreadRecoveryHandle, *, subscribe: bool = False) -> pb.RecoverThreadResponse:
        """Recover a thread using a complete read-only bearer capability."""
        return self.stub.RecoverThreadWithHandle(pb.RecoverThreadRequest(handle=handle, subscribe=subscribe))

    def install_recovery_epoch_keys(self, handle: pb.ThreadRecoveryHandle) -> list[int]:
        """Fetch capability-scoped envelopes and install their epoch keys locally."""
        if handle.version != 1 or len(handle.recovery_secret) != 32:
            raise ValueError("valid recovery handle required")
        result = self.stub.GetRecoveryThreadKeyEnvelopes(pb.RecoverThreadRequest(handle=handle))
        installed: list[int] = []
        for envelope in result.envelopes:
            if envelope.thread_id != handle.thread_id:
                raise ValueError("recovery envelope thread does not match handle")
            self.set_thread_epoch_key(envelope.thread_id, envelope.encryption_epoch,
                                      unwrap_recovery_epoch_key(handle.recovery_secret, envelope))
            installed.append(envelope.encryption_epoch)
        return installed

    def recover_thread_fully(self, handle: pb.ThreadRecoveryHandle, *, subscribe: bool = False) -> tuple[pb.RecoverThreadResponse, list[int]]:
        """Recover verified history, then install capability-scoped epoch keys.

        Both stages are idempotent, so a caller can safely resume after an
        interruption without converting recovery access into membership.
        """
        recovered = self.recover_thread_with_handle(handle, subscribe=subscribe)
        return recovered, self.install_recovery_epoch_keys(handle)

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
                inline=data, mime_type=mime_type, name=filename, size=len(data)
            )
        cid = self.store_blob(data, mime_type=mime_type, filename=filename)
        return pb.Artifact(
            cid=cid, mime_type=mime_type, name=filename, size=len(data)
        )

    # ── diagnostics ───────────────────────────────────────────────────────────

    def health(self) -> pb.HealthResponse:
        """Return daemon version, DID, peer count, and uptime."""
        return self.stub.Health(pb.Empty())

    def ping(self, did: str = "") -> pb.PingResponse:
        """
        Measure round-trip latency to a peer by DID.
        Omit did to ping the local daemon (loopback).
        """
        return self.stub.Ping(pb.PingRequest(target_did=did))

    def list_peers(self) -> list[pb.PeerInfo]:
        """Return all currently connected libp2p peers."""
        return list(self.stub.ListPeers(pb.Empty()).peers)

    def connect_peer(self, did: str) -> pb.ConnectPeerResponse:
        """Resolve a DID via DHT and connect the local daemon to that peer."""
        return self.stub.ConnectPeer(pb.ConnectPeerRequest(did=did))

    def disconnect_peer(self, did: str) -> None:
        """Disconnect from the peer identified by DID."""
        self.stub.DisconnectPeer(pb.ConnectPeerRequest(did=did))

    # ── pub/sub ───────────────────────────────────────────────────────────────

    def publish(self, topic: str, payload: bytes | str) -> None:
        """Publish a message to a GossipSub topic."""
        if isinstance(payload, str):
            payload = payload.encode()
        self.stub.Publish(pb.PublishRequest(topic=topic, payload=payload))

    def subscribe_topic(self, topic: str) -> Iterator[pb.TopicMessage]:
        """Stream messages from a GossipSub topic."""
        return self.stub.SubscribeTopic(pb.SubscribeTopicRequest(topic=topic))

    # ── webhooks ──────────────────────────────────────────────────────────────

    def set_webhook(self, url: str, secret: str = "") -> str:
        """Configure webhook URL. Returns the configured URL."""
        r = self.stub.SetWebhook(pb.SetWebhookRequest(url=url, secret=secret))
        return r.url

    def clear_webhook(self) -> None:
        """Remove webhook configuration."""
        self.stub.ClearWebhook(pb.Empty())

    def get_webhook(self) -> str:
        """Return the currently configured webhook URL (empty if none)."""
        r = self.stub.GetWebhook(pb.Empty())
        return r.url

    # ── networks ──────────────────────────────────────────────────────────────

    def create_network(self, name: str) -> pb.NetworkInfo:
        """Create a named agent group. Creator is automatically a member."""
        return self.stub.CreateNetwork(pb.CreateNetworkRequest(name=name))

    def join_network(self, network_id: str) -> pb.NetworkInfo:
        """Join an existing network by ID."""
        return self.stub.JoinNetwork(pb.JoinNetworkRequest(network_id=network_id))

    def leave_network(self, network_id: str) -> None:
        """Leave a network."""
        self.stub.LeaveNetwork(pb.NetworkIDRequest(network_id=network_id))

    def list_networks(self) -> list[pb.NetworkInfo]:
        """List all networks this agent belongs to."""
        return list(self.stub.ListNetworks(pb.Empty()).networks)

    def network_members(self, network_id: str) -> list[pb.NetworkMember]:
        """Return members of a network."""
        return list(
            self.stub.NetworkMembers(pb.NetworkIDRequest(network_id=network_id)).members
        )

    def broadcast_network(self, network_id: str, payload: bytes | str) -> None:
        """Multicast a message to all members of a network."""
        if isinstance(payload, str):
            payload = payload.encode()
        self.stub.BroadcastNetwork(
            pb.BroadcastRequest(network_id=network_id, payload=payload)
        )

    def subscribe_network(self, network_id: str) -> Iterator[pb.BroadcastMessage]:
        """Stream broadcasts from a network."""
        return self.stub.SubscribeNetwork(pb.NetworkIDRequest(network_id=network_id))

    # ── names ─────────────────────────────────────────────────────────────────

    def claim_name(self, name: str) -> pb.NameClaimResponse:
        """Claim a human-readable name for this agent."""
        return self.stub.ClaimName(pb.ClaimNameRequest(name=name))

    def resolve_name(self, name: str) -> str:
        """Resolve a name to a DID. Returns empty string if not found."""
        return self.stub.ResolveName(pb.ResolveNameRequest(name=name)).did
