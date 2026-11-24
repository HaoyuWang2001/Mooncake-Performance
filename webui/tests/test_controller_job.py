#!/usr/bin/env python3
"""controller map+reduce driver 端到端：同一 job 代码路径本地验证。"""
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import logstats as ls   # noqa: E402


def _write_samples(d, iid, lats, op="upsert_from"):
    path = os.path.join(d, "cfg.i{}_p9101.kvtest_8MB.samples.csv".format(iid))
    with open(path, "w", encoding="utf-8") as f:
        f.write("elapsed_us,instance_id,operation,request_index,"
                "latency_ms,status,bytes\n")
        for i, lat in enumerate(lats):
            f.write("{},{},{},{},{:.6f},OK,{}\n".format(
                1_000_000, iid, op, i, lat, 1 << 20))
    return path


def test_controller_job_end_to_end(tmp_path):
    d = str(tmp_path / "data")
    os.makedirs(d)
    inst = []
    for iid, lats in ((1, [1.0, 2.0, 3.0]), (2, [4.0, 5.0, 6.0])):
        samples = _write_samples(d, iid, lats)
        base = samples[:-len(".samples.csv")]
        with open(base + ".summary.json", "w", encoding="utf-8") as f:
            json.dump({"instance_id": iid, "elapsed_seconds": 10.0,
                       "verify_failures": 0,
                       "operations": {"upsert_from": {
                           "total": len(lats), "success": len(lats),
                           "failure": 0, "bytes": len(lats) * (1 << 20)}}}, f)
        inst.append({"samples": os.path.basename(samples),
                     "summary": os.path.basename(base + ".summary.json"),
                     "trace": None, "node": "n{}".format(iid), "pod": "",
                     "ds": "8MB", "start_epoch": 1000.0,
                     "extra_bytes": 0,
                     "writer_ops": ["upsert_from"],
                     "reader_ops": ["get_buffer"]})
    job = {"dir": d, "out_dir": os.path.join(d, "reduced"),
           "meta_base": {"config": "cfg", "mode": "pipeline",
                         "engine": "kvtest"},
           "instances": inst}
    job_path = os.path.join(str(tmp_path), "job.json")
    with open(job_path, "w", encoding="utf-8") as f:
        json.dump(job, f)
    out = subprocess.run(
        [sys.executable,
         os.path.join(ROOT, "scripts", "kvtest_controller_job.py"),
         "--job", job_path], capture_output=True, text=True)
    assert out.returncode == 0, out.stdout + out.stderr
    red = os.path.join(d, "reduced", "8MB.reduced.json")
    data = json.load(open(red))
    assert os.path.isfile(os.path.join(d, "reduced", "8MB.meta.json"))
    w = data["sizes"]["8MB"]["role_summaries"]["writer"]
    pooled = sorted([1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
    # exact pooled percentiles, order-statistic convention
    assert w["p50_ms"] == round(ls._exact_percentile(pooled, 0.50), 4)
    assert w["p99_ms"] == round(ls._exact_percentile(pooled, 0.99), 4)
    assert w["max_ms"] == 6.0 and w["avg_ms"] == round(sum(pooled) / 6, 4)
    # reader role has no rows in this scenario -> absent from summaries
    assert "reader" not in data["sizes"]["8MB"]["role_summaries"]


def test_controller_job_missing_samples_fails(tmp_path):
    d = str(tmp_path / "data")
    os.makedirs(d)
    job = {"dir": d, "out_dir": os.path.join(d, "reduced"),
           "meta_base": {}, "instances": []}
    job_path = os.path.join(str(tmp_path), "job.json")
    with open(job_path, "w", encoding="utf-8") as f:
        json.dump(job, f)
    out = subprocess.run(
        [sys.executable,
         os.path.join(ROOT, "scripts", "kvtest_controller_job.py"),
         "--job", job_path], capture_output=True, text=True)
    assert out.returncode != 0        # no products -> non-zero, fallback fires
