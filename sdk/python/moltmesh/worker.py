"""Handler-driven durable SDK worker built on resumable task leases."""
from __future__ import annotations

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable

from moltmesh.proto import a2a_pb2 as pb


class Worker:
    """Consume durable deliveries without treating stream receipt as an ack.

    A delivery cursor is persisted before handler execution. If the process
    crashes after that point the task is still recoverable: its lease expires,
    the daemon issues a later delivery cursor, and this worker resumes it.
    """

    def __init__(
        self,
        client,
        skills: list[str],
        *,
        concurrency: int = 1,
        lease_seconds: int = 30,
        checkpoint_path: str | Path | None = None,
    ):
        self.client, self.skills = client, skills
        self.concurrency, self.lease_seconds = max(1, concurrency), max(3, lease_seconds)
        self.handlers: dict[str, Callable] = {}
        self.checkpoint_path = Path(checkpoint_path) if checkpoint_path else None

    def handle(self, skill: str):
        def register(fn: Callable):
            self.handlers[skill] = fn
            return fn

        return register

    def cursor(self) -> int:
        if self.checkpoint_path is None or not self.checkpoint_path.exists():
            return 0
        try:
            return max(0, int(json.loads(self.checkpoint_path.read_text()).get("after_sequence", 0)))
        except (OSError, ValueError, json.JSONDecodeError):
            # A corrupt local checkpoint must not make an agent silently skip
            # work; restart from zero and rely on ClaimTask idempotency.
            return 0

    def _save_cursor(self, cursor: int) -> None:
        if self.checkpoint_path is None:
            return
        self.checkpoint_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        temporary = self.checkpoint_path.with_suffix(self.checkpoint_path.suffix + ".tmp")
        temporary.write_text(json.dumps({"version": 1, "after_sequence": cursor}) + "\n")
        temporary.chmod(0o600)
        os.replace(temporary, self.checkpoint_path)

    def run_forever(self, stop_event: threading.Event | None = None, *, retry_delay: float = 1.0) -> None:
        """Follow work until ``stop_event`` is set, reconnecting on stream errors."""
        stop_event = stop_event or threading.Event()
        cursor = self.cursor()
        slots = threading.BoundedSemaphore(self.concurrency)
        with ThreadPoolExecutor(max_workers=self.concurrency) as pool:
            while not stop_event.is_set():
                try:
                    for delivery in self.client.subscribe_tasks(self.skills, cursor):
                        if stop_event.is_set():
                            return
                        cursor = max(cursor, delivery.sequence)
                        self._save_cursor(cursor)
                        handler = self.handlers.get(delivery.task.skill)
                        if handler is None:
                            continue
                        slots.acquire()
                        future = pool.submit(self._run, delivery.task, handler, stop_event)
                        future.add_done_callback(lambda _: slots.release())
                except Exception:
                    if stop_event.wait(retry_delay):
                        return

    def run_once(self, after_sequence: int = 0) -> int:
        """Compatibility wrapper which processes the first available delivery.

        Long-running applications should use :meth:`run_forever`; the daemon
        now intentionally keeps its durable subscription open.
        """
        cursor = max(after_sequence, self.cursor())
        for delivery in self.client.subscribe_tasks(self.skills, cursor):
            cursor = max(cursor, delivery.sequence)
            self._save_cursor(cursor)
            handler = self.handlers.get(delivery.task.skill)
            if handler:
                self._run(delivery.task, handler, threading.Event())
            return cursor
        return cursor

    def _run(self, task: pb.Task, handler: Callable, stop_event: threading.Event) -> None:
        try:
            lease = self.client.claim_task(task.id, self.lease_seconds)
        except Exception:
            # Another worker may have claimed a duplicated/replayed delivery.
            return
        done = threading.Event()

        def renew() -> None:
            while not done.wait(max(1, self.lease_seconds // 2)) and not stop_event.is_set():
                try:
                    self.client.renew_task_lease(task.id, lease.lease_token, self.lease_seconds)
                except Exception:
                    # Completion will surface lease loss; do not keep a noisy
                    # renewal loop alive after that point.
                    return

        renewer = threading.Thread(target=renew, name=f"moltmesh-lease-{task.id[:8]}", daemon=True)
        renewer.start()
        try:
            result = handler(task)
            artifacts = result if isinstance(result, list) else []
            self.client.complete_task_lease(task.id, lease.lease_token, artifacts)
        except Exception as exc:
            try:
                self.client.fail_task_lease(task.id, lease.lease_token, str(exc))
            except Exception:
                pass
        finally:
            done.set()
            renewer.join(timeout=1)
