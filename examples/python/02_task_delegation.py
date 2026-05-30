"""
02_task_delegation.py — Delegate a task and poll for completion.

Two daemons must be running. The "worker" agent must handle incoming tasks.
This example shows the coordinator side: create task, poll until done.

Run:
    AGENT_B_ADDR=unix://$HOME/.moltmesh/agent_b.sock python 02_task_delegation.py
"""
import os
import time
from moltmesh import A2AClient, STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED
from moltmesh.proto import a2a_pb2 as pb

AGENT_A_ADDR = os.getenv("AGENT_A_ADDR", "")
AGENT_B_ADDR = os.getenv("AGENT_B_ADDR", f"unix://{os.path.expanduser('~')}/.moltmesh/agent_b.sock")
SKILL        = os.getenv("SKILL", "a2a:v1:cap:text-generation")

TERMINAL = {STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELLED}

def main():
    coordinator = A2AClient(AGENT_A_ADDR).connect()
    worker      = A2AClient(AGENT_B_ADDR).connect()

    worker_did = worker.get_identity().did
    print(f"Worker DID: {worker_did}")

    task = coordinator.create_task(worker_did, SKILL, metadata={"prompt": "Summarise MoltMesh in one sentence."})
    print(f"Task created: {task.id}  status={pb.TaskStatus.Name(task.status)}")

    # Poll until terminal
    while task.status not in TERMINAL:
        time.sleep(1)
        task = coordinator.get_task(task.id)
        print(f"  Polling... status={pb.TaskStatus.Name(task.status)}")

    print(f"\nTask finished: {pb.TaskStatus.Name(task.status)}")
    if task.error:
        print(f"  Error: {task.error}")
    for a in task.output_artifacts:
        print(f"  Artifact: {a.cid} ({a.mime_type}, {a.size} bytes)")

    coordinator.close()
    worker.close()

if __name__ == "__main__":
    main()
