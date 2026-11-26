#!/usr/bin/env python3
"""Controller-side kvtest PARTIAL reduce (stdlib only, digests deferred)."""
import importlib.util
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import logstats as ls   # noqa: E402

START = 1763596800.0


def _load_reducer():
    path = os.path.join(ROOT, "scripts", "kvtest_partial_reduce.py")
    spec = importlib.util.spec_from_file_location("kvtest_partial_reduce", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _instance(d, iid, role, lats, op, ds="8MB", verify=0, qw=None):
    base = "case.i{}_p9101.kvtest_{}".format(iid, ds)
    samples = os.path.join(d, base + ".samples.csv")
    with open(samples, "w", encoding="utf-8") as f:
        f.write("elapsed_us,instance_id,operation,request_index,"
                "latency_ms,status,bytes\n")
        for i, lat in enumerate(lats):
            f.write("{},{},{},{},{:.6f},OK,{}\n".format(
                int(i * 200000), iid, op, i, lat, 1 << 20))
    summary = os.path.join(d, base + ".summary.json")
    ops = {op: {"total": len(lats), "success": len(lats),
                "failure": 0, "bytes": len(lats) * (1 << 20)}}
    if qw:
        ops["notify_queue_wait"] = qw
    with open(summary, "w", encoding="utf-8") as f:
        json.dump({"instance_id": iid, "elapsed_seconds": 10.0,
                   "verify_failures": verify, "operations": ops}, f)
    inst_dir = os.path.join(d, "proc_i{}".format(iid))
    os.makedirs(inst_dir, exist_ok=True)
    with open(os.path.join(inst_dir, "keep.log"), "w") as f:
        f.write("x" * 64)
    stats_doc, csvstats_doc = ls.kvtest_map_instance(
        samples, summary, role, node="10.0.0.1", pod="pod-a", size=ds,
        start_epoch=START, digest_k=24, remote_path=base + ".stats.json.gz",
        iid=iid, instance_dir=inst_dir)
    stats = os.path.join(d, base + ".stats.json")
    csvstats = os.path.join(d, base + ".csvstats.json")
    for path, doc in ((stats, stats_doc), (csvstats, csvstats_doc)):
        with open(path, "w", encoding="utf-8") as f:
            json.dump(doc, f)
    return {"iid": iid, "node": "10.0.0.1", "pod": "pod-a", "role": role,
            "size": ds, "stats": base + ".stats.json",
            "csvstats": base + ".csvstats.json", "trace": None}


def _job(d, instances, out=None):
    return {"dir": d,
            "out": out or os.path.join(d, "partial.json.gz"),
            "ds": "8MB",
            "meta": {"config": "cfg", "mode": "pipeline",
                     "engine": "kvtest", "unit": "8MB"},
            "instances": instances}


def test_partial_reduce_defers_tails_and_keeps_instances(tmp_path,
                                                         fake_sketch):
    d = str(tmp_path)
    insts = [
        _instance(d, 0, "writer", [1.0, 2.0, 3.0], "upsert_from"),
        _instance(d, 1, "writer", [4.0, 5.0, 6.0], "upsert_from"),
        _instance(d, 2, "reader", [7.0, 8.0], "get_buffer", verify=3,
                  qw={"total": 2, "success": 2, "failure": 0,
                      "avg_ms": 0.5, "max_ms": 1.0}),
    ]
    mod = _load_reducer()
    out = mod.run(_job(d, insts))
    with open(out, "rb") as f:
        import gzip
        data = json.loads(gzip.open(f, "rb").read().decode("utf-8"))
    blk = data["sizes"]["8MB"]
    w = blk["role_summaries"]["writer"]
    assert w["pct_method"] == "pending"
    assert w["p99_ms"] is None and w["p999_ms"] is None
    assert w["max_ms"] == 6.0 and w["p50_ms"] is not None
    pend = data["_digest_pending"]
    assert pend["k"] == 24
    # one digest per product, per scope (no duplicates per quantile call)
    assert len(pend["scopes"]["8MB"]["all"]) == 3
    assert len(pend["scopes"]["8MB"]["writer"]) == 2
    assert len(pend["scopes"]["8MB"]["reader"]) == 1
    assert "logsite" not in data
    # instances block: formal window stats, notify/queue, per-process bytes
    by_iid = {it["iid"]: it for it in data["instances"]}
    assert by_iid[0]["role"] == "writer"
    assert by_iid[0]["wall_s"] == 10.0
    assert by_iid[0]["total"] == 3 and by_iid[0]["success"] == 3
    assert by_iid[0]["bytes"] == 3 * (1 << 20)
    assert by_iid[0]["pcts"]["p99_ms"] is not None   # pod map has digests
    assert by_iid[2]["verify_failures"] == 3
    assert by_iid[2]["queue"]["avg_ms"] == 0.5
    assert by_iid[0]["meta_bytes"] > 0


def test_partial_reduce_rejects_product_without_digest(tmp_path,
                                                       fake_sketch):
    d = str(tmp_path)
    inst = _instance(d, 0, "writer", [1.0, 2.0], "upsert_from")
    p = os.path.join(d, inst["stats"])
    with open(p, "r", encoding="utf-8") as f:
        doc = json.load(f)
    doc.pop("digest", None)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(doc, f)
    mod = _load_reducer()
    with pytest.raises(RuntimeError) as ei:
        mod.run(_job(d, [inst]))
    assert "digest" in str(ei.value)
