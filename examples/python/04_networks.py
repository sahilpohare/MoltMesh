"""
04_networks.py — Create a group, have a second agent join, and broadcast.

Run:
    AGENT_B_ADDR=unix://$HOME/.moltmesh/agent_b.sock python 04_networks.py
"""
import os
import threading
from moltmesh import A2AClient

AGENT_A_ADDR = os.getenv("AGENT_A_ADDR", "")
AGENT_B_ADDR = os.getenv("AGENT_B_ADDR", f"unix://{os.path.expanduser('~')}/.moltmesh/agent_b.sock")

def listen(client: A2AClient, network_id: str):
    print(f"[agent_b] subscribing to network {network_id[:8]}...")
    for msg in client.subscribe_network(network_id):
        print(f"[agent_b] broadcast: {msg.payload.decode()}")
        break  # receive one message then exit

def main():
    a = A2AClient(AGENT_A_ADDR).connect()
    b = A2AClient(AGENT_B_ADDR).connect()

    net = a.create_network("demo-group")
    print(f"Network created: {net.id}  name={net.name}")

    b.join_network(net.id)
    print(f"Agent B joined network {net.id[:8]}")

    t = threading.Thread(target=listen, args=(b, net.id), daemon=True)
    t.start()

    a.broadcast_network(net.id, "Hello network!")
    print(f"[agent_a] broadcast sent")

    t.join(timeout=5)

    nets_b = b.list_networks()
    print(f"\nAgent B networks: {[n.name for n in nets_b]}")

    a.close()
    b.close()

if __name__ == "__main__":
    main()
