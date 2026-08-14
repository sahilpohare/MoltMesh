"""
02_task_worker.py — Advertise a capability, listen for delegated tasks, execute them.

Run this first, on its own daemon:

    A2A_GRPC_ADDR=unix://$HOME/.moltmesh/agent_b.sock python 02_task_worker.py

It publishes an AgentCard advertising SKILL to the DHT, then blocks on
SubscribeInbox waiting for MESSAGE_KIND_TASK_REQUEST notifications — no
polling. For each task it fetches the full Task, does the work, and marks
it completed (or failed) via the SDK.

Pair with 02_task_delegation.py, which finds this worker via find_agents()
and delegates a task to it.
"""
import os

from moltmesh import A2AClient
from moltmesh.proto import a2a_pb2 as pb

AGENT_ADDR = os.getenv("A2A_GRPC_ADDR", "")
SKILL      = os.getenv("SKILL", "a2a:v1:cap:text-transform")


def handle_task(client: A2AClient, task_id: str) -> None:
    task = client.get_task(task_id)
    print(f"Task received: {task.id}  skill={task.skill}  from={task.initiator}")

    client.mark_working(task.id)

    text = task.metadata.get("text", "")
    try:
        result = text.upper()
    except Exception as e:  # pragma: no cover — toy handler, kept defensive on purpose
        client.mark_failed(task.id, str(e))
        print(f"  Task failed: {e}")
        return

    artifact = client.make_artifact(
        result.encode(), mime_type="text/plain", filename="result.txt"
    )
    client.mark_completed(task.id, output_artifacts=[artifact])
    print(f"  Task completed: {result!r}")


def main():
    worker = A2AClient(AGENT_ADDR).connect()

    print(f"Worker DID: {worker.did}")

    card = pb.AgentCard(
        did=worker.did,
        name="uppercase-worker",
        description="Demo worker that uppercases text",
        skills=[pb.Skill(id=SKILL, name="text-transform")],
    )
    worker.publish_agent_card(card)
    print(f"Advertised capability {SKILL!r} on the DHT")

    print("Listening for tasks (Ctrl+C to stop)...")
    for msg in worker.subscribe_inbox():
        if msg.kind == pb.MESSAGE_KIND_TASK_REQUEST:
            handle_task(worker, msg.task_id)


if __name__ == "__main__":
    main()
