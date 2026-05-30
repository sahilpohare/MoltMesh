"""
03_pubsub.py — Publish to a GossipSub topic and subscribe to it.

Two threads: one publishes every second; the other subscribes and prints.

Run:
    python 03_pubsub.py
"""
import os
import threading
import time
from moltmesh import A2AClient

ADDR  = os.getenv("A2A_GRPC_ADDR", "")
TOPIC = "example/hello"

def publisher(client: A2AClient):
    for i in range(5):
        time.sleep(1)
        client.publish(TOPIC, f"ping #{i}")
        print(f"[pub] sent ping #{i}")

def subscriber(client: A2AClient):
    print(f"[sub] listening on topic '{TOPIC}' ...")
    count = 0
    for msg in client.subscribe_topic(TOPIC):
        print(f"[sub] received: {msg.payload.decode()}")
        count += 1
        if count >= 5:
            break

def main():
    pub = A2AClient(ADDR).connect()
    sub = A2AClient(ADDR).connect()

    t_sub = threading.Thread(target=subscriber, args=(sub,), daemon=True)
    t_sub.start()

    publisher(pub)
    t_sub.join(timeout=10)

    pub.close()
    sub.close()

if __name__ == "__main__":
    main()
