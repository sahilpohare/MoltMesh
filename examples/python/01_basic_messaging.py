"""
01_basic_messaging.py — Send a message and read it from the inbox.

Prerequisites:
    pip install "moltmesh[grpc]"
    moltmesh-daemon &          # agent A (default socket)
    A2A_GRPC_ADDR=unix://$HOME/.moltmesh/agent_b.sock moltmesh-daemon &   # agent B

Run:
    python 01_basic_messaging.py
"""
import os
import sys
from moltmesh import A2AClient

AGENT_A_ADDR = os.getenv("AGENT_A_ADDR", "")   # default socket
AGENT_B_ADDR = os.getenv("AGENT_B_ADDR", f"unix://{os.path.expanduser('~')}/.moltmesh/agent_b.sock")

def main():
    a = A2AClient(AGENT_A_ADDR).connect()
    b = A2AClient(AGENT_B_ADDR).connect()

    id_b = b.get_identity()
    print(f"Agent B DID: {id_b.did}")

    result = a.send_message(id_b.did, text="Hello from agent A!")
    print(f"Sent message ID: {result.message_id}  queued={result.queued}")

    msgs = b.get_inbox(limit=5)
    print(f"\nAgent B inbox ({len(msgs)} message(s)):")
    for m in msgs:
        print(f"  [{m.id[:8]}] from={m.from_did}  kind={m.kind}")

    a.close()
    b.close()

if __name__ == "__main__":
    main()
