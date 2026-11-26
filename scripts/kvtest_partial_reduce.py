#!/usr/bin/env python3
"""Controller-side kvtest PARTIAL reduce (stdlib only).

The pods already ran the map (stats + csvstats products, with REQ digests).
This driver merges everything EXCEPT the digest sketches: counts, averages,
histograms, time buckets, tiers, trace stats and the per-instance CSV block.
Tail percentiles (p99/p999/p9999) are deferred to the orchestrator's
``report.finalize_digests`` — the controller must not need datasketches.

Job JSON::

    {"dir": "<cdir with the pod products>", "out": "<partial.json.gz>",
     "ds": "8KB", "meta": {...reduce meta sans 'started'...},
     "instances": [{"iid": 0, "node": "...", "pod": "...", "role": "writer",
                    "size": "8KB", "stats": "<base>.stats.json.gz",
                    "csvstats": "<base>.csvstats.json.gz",
                    "trace": "<base>.trace.json.gz"|null}]}

Output: the partial reduced data with ``_digest_pending`` (de-duplicated
digest list + per-scope indexes) — pulled back by the orchestrator.
"""
import argparse
import gzip
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
import logstats as ls   # noqa: E402
import report as rp     # noqa: E402


def _load(path):
    if not os.path.isfile(path):
        raise RuntimeError("partial reduce: product not found: " + path)
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rb") as f:
        return json.loads(f.read().decode("utf-8"))


def _bytes_of(path):
    try:
        return os.path.getsize(path)
    except OSError:
        return 0


def _instances(stats_products, csv_products, metas):
    """Per-process CSV block: formal counts/bytes/avg/min/max (csvstats) +
    role-op percentiles/thresholds (stats product, already computed in pod)."""
    out = []
    for st, cs, m in zip(stats_products, csv_products, metas):
        cs = cs or {}
        role = (m.get("role") or "").strip().lower()
        ops = st.get("ops") or {}
        lib = ls.KVTEST_WRITE_OPS if role == "writer" else ls.KVTEST_READ_OPS
        op = next((o for o in ops if o in lib), None)
        opd = ops.get(op) or {}
        cs_ops = cs.get("ops") or {}
        c_ops = cs_ops.get(op) or {}

        def _num(d, key):
            try:
                return float(d.get(key) or 0.0)
            except (TypeError, ValueError):
                return 0.0

        # notify: prefer the FORMAL sample rows (warmup already excluded); the
        # summary block (which includes the warmup region) is the fallback.
        ns = cs_ops.get("notify_rpc") or cs_ops.get("notify_http")
        if ns:
            notify = {"submitted": int(ns.get("total") or 0),
                      "ok": int(ns.get("success") or 0),
                      "fail": int(ns.get("failure") or 0),
                      "avg_ms": _num(ns, "avg_ms"),
                      "max_ms": _num(ns, "max_ms")}
        else:
            notify = dict(cs.get("notify") or {})
        queue = cs.get("queue") or {}
        try:
            elapsed = float(cs.get("elapsed_seconds") or 0.0)
        except (TypeError, ValueError):
            elapsed = 0.0
        try:
            warm = float(cs.get("warmup_s") or 0.0)
        except (TypeError, ValueError):
            warm = 0.0
        out.append({
            "iid": cs.get("instance_id"),
            "node": m.get("node") or "",
            "pod": m.get("pod") or "",
            "role": role,
            "op": op,
            "elapsed_seconds": elapsed,
            "warmup_s": warm,
            "wall_s": (max(0.0, elapsed - warm) if warm > 0 else elapsed),
            "verify_failures": (int(cs.get("verify_failures") or 0)
                                if role == "reader" else None),
            "meta_bytes": int(cs.get("meta_bytes") or 0),
            "total": int(c_ops.get("total") or opd.get("count") or 0),
            "success": int(c_ops.get("success") or opd.get("ok") or 0),
            "failure": int(c_ops.get("failure") or opd.get("fail") or 0),
            "bytes": int(c_ops.get("bytes") or 0),
            "avg_ms": _num(c_ops, "avg_ms"),
            "min_ms": _num(c_ops, "min_ms"),
            "max_ms": _num(c_ops, "max_ms"),
            "pcts": {k: opd.get(k) for k in (
                "p50_ms", "p90_ms", "p95_ms", "p99_ms", "p999_ms",
                "p9999_ms")},
            "thresholds": {k: opd.get(k) for k in opd
                           if k.startswith(">") and k.endswith("us")},
            "notify": notify or None,
            "queue": ({"avg_ms": _num(queue, "avg_ms"),
                       "max_ms": _num(queue, "max_ms")}
                      if queue else None),
        })
    return out


def run(job):
    d = job["dir"]
    out_path = job["out"]
    stats_products, csv_products, metas, trace_products = [], [], [], []
    for inst in job.get("instances") or []:
        stats_path = os.path.join(d, inst["stats"])
        stats = _load(stats_path)
        if not (stats.get("digest") or {}).get("global"):
            raise RuntimeError(
                "partial reduce: product missing digest: " + stats_path)
        stats["_path"] = stats_path
        stats["_bytes"] = _bytes_of(stats_path)
        stats_products.append(stats)
        metas.append(stats.get("meta") or {})
        csv_path = os.path.join(d, inst["csvstats"])
        csv_products.append(_load(csv_path))
        if inst.get("trace"):
            trace_path = os.path.join(d, inst["trace"])
            tr = _load(trace_path)
            tr["_path"] = trace_path
            tr["_bytes"] = _bytes_of(trace_path)
            trace_products.append(tr)
    if not stats_products:
        raise RuntimeError("partial reduce: no instances")
    meta = dict(job.get("meta") or {})
    # started = earliest data timestamp (same rule as the former controller job)
    t0 = None
    for st in stats_products:
        m = st.get("meta") or {}
        if m.get("ts_from") is not None:
            t0 = m["ts_from"] if t0 is None else min(t0, m["ts_from"])
    import time as _t
    meta.update({
        "started": (_t.strftime("%Y-%m-%d %H:%M:%S", _t.localtime(t0))
                    if t0 is not None else _t.strftime("%Y-%m-%d %H:%M:%S")),
        "unit": job.get("ds"), "unit_size": job.get("ds"),
        "clients": len(stats_products),
        "early_stop": bool(meta.get("early_stop")),
    })
    data = rp.reduce_scenario(stats_products, meta, monitors=[],
                              traces=trace_products, defer_digests=True,
                              logsite=False)
    data["instances"] = _instances(stats_products, csv_products, metas)
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    tmp = out_path + ".tmp"
    with gzip.open(tmp, "wb") as f:
        f.write(json.dumps(data, ensure_ascii=False,
                           separators=(",", ":")).encode("utf-8"))
    os.replace(tmp, out_path)
    print("[partial] {} -> {}".format(job.get("ds"), out_path), flush=True)
    return out_path


def main(argv=None):
    ap = argparse.ArgumentParser(description="kvtest partial reduce (stdlib)")
    ap.add_argument("--job", required=True)
    args = ap.parse_args(argv)
    with open(args.job, "r", encoding="utf-8") as f:
        job = json.load(f)
    run(job)
    return 0


if __name__ == "__main__":
    sys.exit(main())
