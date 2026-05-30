"""
05_webhook_receiver.py — Configure a webhook and run a local HTTP server to receive events.

Start the listener first, then configure the daemon to post to it.

Run:
    python 05_webhook_receiver.py
"""
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from moltmesh import A2AClient

ADDR   = os.getenv("A2A_GRPC_ADDR", "")
PORT   = int(os.getenv("WEBHOOK_PORT", "9999"))
SECRET = os.getenv("WEBHOOK_SECRET", "demo-secret")


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body   = self.rfile.read(length)
        event  = self.headers.get("X-MoltMesh-Event", "unknown")
        secret = self.headers.get("X-MoltMesh-Secret", "")
        print(f"\n[webhook] event={event}  secret_ok={secret == SECRET}")
        try:
            print(json.dumps(json.loads(body), indent=2))
        except Exception:
            print(body.decode(errors="replace"))
        self.send_response(200)
        self.end_headers()

    def log_message(self, *_):
        pass  # suppress default access log


def main():
    server = HTTPServer(("0.0.0.0", PORT), Handler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    print(f"Listening for webhooks on http://localhost:{PORT}")

    client = A2AClient(ADDR).connect()
    url = f"http://localhost:{PORT}/events"
    configured = client.set_webhook(url, SECRET)
    print(f"Daemon webhook configured: {configured}")

    print("Press Ctrl-C to stop.\n")
    try:
        while True:
            pass
    except KeyboardInterrupt:
        pass
    finally:
        client.clear_webhook()
        print("Webhook cleared.")
        client.close()

if __name__ == "__main__":
    main()
