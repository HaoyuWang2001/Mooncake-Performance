"""Minimal etcd v3 KV client over the gRPC gateway (stdlib only).

Shared by the orchestrator (benchmark.py) and the benchmark client
(mooncake_benchmark_client.py) for the pipeline rendezvous: reader
self-registration, writer discovery, HTTP-trigger address lookup and
run-prefix cleanup.

No third-party dependencies: urllib + base64 + json against the etcd
v3 gRPC gateway (POST /v3/kv/put | range | deleterange) on the plain
HTTP client port. Endpoint format follows global.json etcd_endpoints:

    "etcd://host:2379;host2:2379"

(semicolon- or comma-separated; bare "host:port" and "http://..." are
also accepted). Multiple endpoints are tried in order; the first one
that answers wins.
"""
import base64
import json
import urllib.request


def endpoint_urls(etcd_endpoints):
    """Parse an endpoints string into a list of gateway base URLs."""
    if not etcd_endpoints:
        return []
    parts = [p.strip() for p in str(etcd_endpoints).replace(",", ";").split(";")
             if p.strip()]
    urls = []
    for p in parts:
        if p.startswith("http://") or p.startswith("https://"):
            urls.append(p.rstrip("/"))
            continue
        if p.startswith("etcd://"):
            p = p[len("etcd://"):]
        urls.append("http://" + p.rstrip("/"))
    return urls


def _b64(s):
    return base64.b64encode(s.encode("utf-8")).decode("ascii")


def _unb64(s):
    return base64.b64decode(s).decode("utf-8")


def _prefix_end(prefix):
    """range_end for a prefix scan (etcd semantics: last byte +1).

    Empty prefix maps to range_end="\\x00" (whole keyspace); an all-0xFF
    prefix maps to "" (open-ended), both per etcd's rules."""
    if not prefix:
        return "\x00"
    b = bytearray(prefix.encode("utf-8"))
    for i in range(len(b) - 1, -1, -1):
        if b[i] < 0xFF:
            b[i] += 1
            return bytes(b[:i + 1]).decode("utf-8", "replace")
    return ""


def _request(urls, path, payload, timeout):
    body = json.dumps(payload).encode("utf-8")
    last_err = None
    for base in urls:
        req = urllib.request.Request(
            base + path, data=body,
            headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8") or "{}")
        except Exception as exc:
            last_err = exc
    raise RuntimeError("etcd gateway {} failed on {}: {}".format(
        path, ",".join(urls), last_err))


def _urls(etcd_endpoints):
    urls = endpoint_urls(etcd_endpoints)
    if not urls:
        raise RuntimeError("empty etcd endpoints")
    return urls


def kv_put(etcd_endpoints, key, value, timeout=5):
    """PUT a string value under a string key."""
    _request(_urls(etcd_endpoints), "/v3/kv/put",
             {"key": _b64(key), "value": _b64(value)}, timeout)
    return True


def kv_range(etcd_endpoints, prefix, timeout=5):
    """Range-scan a key prefix; returns {key: value}."""
    out = _request(_urls(etcd_endpoints), "/v3/kv/range",
                   {"key": _b64(prefix), "range_end": _b64(_prefix_end(prefix))},
                   timeout)
    kvs = out.get("kvs") or []
    return {_unb64(kv["key"]): _unb64(kv["value"]) for kv in kvs}


def kv_delete_prefix(etcd_endpoints, prefix, timeout=5):
    """Delete a whole key prefix; returns the number of deleted keys."""
    out = _request(_urls(etcd_endpoints), "/v3/kv/deleterange",
                   {"key": _b64(prefix), "range_end": _b64(_prefix_end(prefix))},
                   timeout)
    return int(out.get("deleted", 0) or 0)
