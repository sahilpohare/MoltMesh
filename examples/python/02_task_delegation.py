"""
02_task_delegation.py — Discover a worker via the DHT and delegate a task to it.

Two daemons must be running, on different data dirs / ports, connected to
the same network (they'll find each other via mDNS or the DHT). Start the
worker first:

    A2A_GRPC_ADDR=unix://$HOME/.moltmesh/agent_b.sock python 02_task_worker.py

Then run this as the coordinator:

    python 02_task_delegation.py

This does NOT hardcode the worker's DID — it looks the worker up on the DHT
by capability via find_agents(), the same way an agent would discover a
stranger's daemon on the network.
"""
import os
import time

from moltmesh import A2AClient
from moltmesh.proto import a2a_pb2 as pb

AGENT_A_ADDR = os.getenv("AGENT_A_ADDR", "")
SKILL        = os.getenv("SKILL", "a2a:v1:cap:text-transform")
DISCOVERY_TIMEOUT = float(os.getenv("DISCOVERY_TIMEOUT", "15"))


def find_worker(coordinator: A2AClient, skill: str, timeout: float) -> pb.AgentCard:
    """Poll the DHT for an agent advertising `skill`. DHT propagation isn't
    instant, so retry for a bit rather than failing on the first empty result."""
    deadline = time.monotonic() + timeout
    while True:
        agents = coordinator.find_agents(skill, limit=5)
        if agents:
            return agents[0]
        if time.monotonic() >= deadline:
            raise TimeoutError(
                f"no agent advertising {skill!r} found on the DHT within {timeout}s "
                "(is 02_task_worker.py running?)"
            )
        time.sleep(1)


def main():
    coordinator = A2AClient(AGENT_A_ADDR).connect()
    print(f"Coordinator DID: {coordinator.did}")

    print(f"Searching DHT for an agent advertising {SKILL!r}...")
    worker_card = find_worker(coordinator, SKILL, DISCOVERY_TIMEOUT)
    print(f"Found worker: {worker_card.name} ({worker_card.did})")

    task = coordinator.create_task(
        worker_card.did, SKILL, metadata={"text": "hello from the coordinator"}
    )
    print(f"Task created: {task.id}  status={pb.TaskStatus.Name(task.status)}")

    task = coordinator.wait_task(task.id, timeout=30)

    print(f"\nTask finished: {pb.TaskStatus.Name(task.status)}")
    if task.error:
        print(f"  Error: {task.error}")
    for a in task.output_artifacts:
        value = a.inline.decode() if a.inline else f"<blob {a.cid}>"
        print(f"  Artifact: {a.name} ({a.mime_type}, {a.size} bytes) = {value!r}")

    coordinator.close()


if __name__ == "__main__":
    main()
