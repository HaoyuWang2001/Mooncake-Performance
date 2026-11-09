#!/usr/bin/env python3
"""In-memory fake of the etcd v3 gRPC gateway for local e2e / pytest.

Implements POST /v3/kv/put, /v3/kv/range and /v3/kv/deleterange with the
same base64-JSON contract as the real gateway (see scripts/bench_etcd.py),
backed by an in-memory dict. GET /health answers 200 for probes.

Usage: python3 mock/etcd_server.py [port]   (default 23790; port 0 = pick one)
Serves until killed. Point global.json etcd_endpoints at it, e.g.
"etcd://127.0.0.1:23790".
"""
import base64
import json
import os
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_LOCK = threading.Lock()
_KV = {}


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _send(self, obj, code=200):
        body = json.dumps(obj).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/health":
            self._send({"health": "true"})
        else:
            self._send({"error": "not found"}, 404)

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        req = json.loads(self.rfile.read(n).decode("utf-8") or "{}")

        def key(name):
            return base64.b64decode(req[name]).decode("utf-8") \
                if name in req else ""

        if self.path == "/v3/kv/put":
            with _LOCK:
                _KV[key("key")] = key("value")
            self._send({})
        elif self.path == "/v3/kv/range":
            k = key("key")
            end = req.get("range_end") and \
                base64.b64decode(req["range_end"]).decode("utf-8") or ""
            with _LOCK:
                items = sorted(_KV.items())
            if end == "\x00" and not k:
                sel = items                       # whole keyspace
            elif not end:
                sel = [(a, b) for a, b in items if a >= k]  # open-ended
            else:
                sel = [(a, b) for a, b in items if k <= a < end]
            self._send({
                "count": str(len(sel)),
                "kvs": [{"key": base64.b64encode(a.encode()).decode(),
                         "value": base64.b64encode(b.encode()).decode()}
                        for a, b in sel],
            })
        elif self.path == "/v3/kv/deleterange":
            k = key("key")
            end = req.get("range_end") and \
                base64.b64decode(req["range_end"]).decode("utf-8") or ""
            with _LOCK:
                if end == "\x00" and not k:
                    sel = [a for a in _KV]
                elif not end:
                    sel = [a for a in _KV if a >= k]
                else:
                    sel = [a for a in _KV if k <= a < end]
                for a in sel:
                    _KV.pop(a, None)
            self._send({"deleted": str(len(sel))})
        else:
            self._send({"error": "not found"}, 404)


def serve(port=23790):
    srv = ThreadingHTTPServer(("127.0.0.1", port), _Handler)
    return srv


def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 23790
    srv = serve(port)
    real = srv.server_address[1]
    print("[mock-etcd] listening on 127.0.0.1:{} (pid {})".format(
        real, os.getpid()), flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
