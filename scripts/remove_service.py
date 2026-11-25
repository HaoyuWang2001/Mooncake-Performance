#!/usr/bin/env python3
"""Long-lived remove service (k8s remove pod).

The per-stage remove-keys path pays one Mooncake store client cold start
(~40s with ub/tent) per invocation. This service is created once per run
(pre-warmed while the benchmark measurement window runs) and keeps ONE
initialized store client, serving remove requests over loopback HTTP:

    GET  /health   -> 200 only after store.setup() finished
    POST /remove   -> {"scope": "full"|"key-prefix",
                       "key_prefix": "...", "scenario": "...",
                       "key_count": N, "max_passes": 32, "pass_delay": 1.0}
                      -> {"ok": true, "scope": ..., "cleared": N,
                          "passes": N, "elapsed_s": x, "error": null}

The drain loop and the key-prefix classification are imported from
mooncake_benchmark_client.py so the CLI and this service share ONE
implementation. Removal calls are serialized (one store client, one lock).

Launched by the orchestrator inside a tiny Deployment that mounts the run's
scripts ConfigMap; env.sh + env.mooncake.sh are sourced by the launcher and
provide SETUP_* (per-pod POD_IP late-bound by env.sh).
"""
import argparse
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
import mooncake_benchmark_client as mc   # noqa: E402

_LOCK = threading.Lock()
_STATE = {"store": None, "ready_at": None}


def _env(name, default=None):
    val = os.environ.get(name)
    return val if val not in (None, "") else default


def _store_args():
    ns = argparse.Namespace()
    ns.fabric_ip = (_env("SETUP_FABRIC_IP") or _env("POD_IP") or "").strip()
    ns.master_type = _env("SETUP_MASTER_TYPE", "")
    ns.etcd_endpoints = _env("SETUP_ETCD_ENDPOINTS", "")
    ns.master_addr = _env("SETUP_MASTER_ADDR", "")
    ns.metadata_server = _env("SETUP_METADATA_SERVER", "")
    ns.protocol = _env("SETUP_PROTOCOL", "tcp")
    ns.ub_device = _env("SETUP_UB_DEVICE", "")
    ns.tenant_id = _env("SETUP_TENANT_ID", "default")
    ns.global_segment_size = 0              # pure client: never mount a segment
    ns.local_buffer_size = int(_env("SETUP_LOCAL_BUFFER_SIZE",
                                    str(16 * 1024 * 1024)))
    return ns


def _init_store():
    t0 = time.time()
    args = _store_args()
    store = mc.make_store(args)
    _STATE["store"] = store
    _STATE["ready_at"] = time.time()
    mc.log("[remove-service] store ready ({:.1f}s)".format(time.time() - t0))


def _handle_remove(payload):
    store = _STATE["store"]
    scope = payload.get("scope") or "full"
    t0 = time.time()
    if scope == "full":
        max_passes = int(payload.get("max_passes") or 32)
        pass_delay = float(payload.get("pass_delay") or 1.0)
        passes, total = mc._remove_all_until_drained(
            store, max(1, max_passes), max(0.0, pass_delay))
        if passes < 0 and total < 0:
            return {"ok": False, "scope": scope, "error":
                    "remove_all returned {}".format(total),
                    "cleared": 0, "passes": 0,
                    "elapsed_s": round(time.time() - t0, 3)}
        if passes < 0:
            return {"ok": False, "scope": scope, "cleared": total,
                    "passes": -passes, "error":
                    "store not drained after {} passes".format(-passes),
                    "elapsed_s": round(time.time() - t0, 3)}
        return {"ok": True, "scope": scope, "cleared": total,
                "passes": passes, "error": None,
                "elapsed_s": round(time.time() - t0, 3)}
    keys = mc.build_keys(
        payload.get("key_prefix") or _env("BENCH_KEY_PREFIX", "mc_bench"),
        payload.get("scenario") or _env("BENCH_SCENARIO", ""),
        int(payload.get("key_count") or _env("BENCH_KEY_COUNT", "1000")))
    res = mc.remove_keys_key_prefix(store, keys)
    elapsed = round(time.time() - t0, 3)
    if "error" in res:
        return {"ok": False, "scope": scope, "error": res["error"],
                "cleared": 0, "passes": 1, "elapsed_s": elapsed}
    ok = res["failed"] == 0
    return {"ok": ok, "scope": scope, "cleared": res["removed"],
            "passes": 1, "elapsed_s": elapsed,
            "not_found": res["not_found"], "failed": res["failed"],
            "error": None if ok else
            "key-prefix remove failed for {} keys".format(res["failed"])}


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *a):   # keep pod logs compact
        mc.log("[remove-service] " + (fmt % a))

    def _send(self, code, body=None):
        data = json.dumps(body or {}, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/health":
            ready = _STATE["store"] is not None
            self._send(200 if ready else 503,
                       {"ok": ready,
                        "uptime_s": round(time.time() - _STATE["ready_at"], 1)
                        if ready else None})
        else:
            self._send(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        if self.path == "/shutdown":
            self._send(200, {"ok": True})
            threading.Thread(target=self.server.shutdown, daemon=True).start()
            return
        if self.path != "/remove":
            self._send(404, {"ok": False, "error": "not found"})
            return
        if _STATE["store"] is None:
            self._send(503, {"ok": False, "error": "store not ready"})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            payload = json.loads(raw.decode("utf-8") or "{}")
        except (ValueError, OSError) as exc:
            self._send(400, {"ok": False, "error": "bad json: {}".format(exc)})
            return
        with _LOCK:
            result = _handle_remove(payload)
        mc.log("[remove-service] remove scope={} ok={} cleared={} passes={} "
               "({}s)".format(result.get("scope"), result.get("ok"),
                              result.get("cleared"), result.get("passes"),
                              result.get("elapsed_s")))
        self._send(200 if result.get("ok") else 500, result)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Mooncake remove service")
    ap.add_argument("--port", type=int, default=9401)
    args = ap.parse_args(argv)
    try:
        _init_store()
    except SystemExit:
        raise
    except Exception as exc:
        mc.log("[remove-service] store setup failed: {}".format(exc))
        return 1
    server = ThreadingHTTPServer(("127.0.0.1", args.port), _Handler)
    mc.log("[remove-service] listening on 127.0.0.1:{}".format(args.port))
    server.serve_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
