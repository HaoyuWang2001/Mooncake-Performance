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


def test_controller_job_uses_node_site_remote_path(tmp_path):
    """报表索引必须是节点侧真实路径：job 用 site_dir，不泄漏 controller 暂存目录。"""
    d = str(tmp_path / "data")
    os.makedirs(d)
    samples = _write_samples(d, 1, [1.0, 2.0])
    base = samples[:-len(".samples.csv")]
    with open(base + ".summary.json", "w", encoding="utf-8") as f:
        json.dump({"instance_id": 1, "elapsed_seconds": 10.0,
                   "verify_failures": 0,
                   "operations": {"upsert_from": {
                       "total": 2, "success": 2, "failure": 0,
                       "bytes": 2}}}, f)
    site = "/var/lib/mooncake-bench/20261126-1141/pod-a"
    inst = [{"samples": os.path.basename(samples),
             "summary": os.path.basename(base + ".summary.json"),
             "trace": None, "site_dir": site,
             "node": "10.0.0.1", "pod": "pod-a", "ds": "8MB",
             "start_epoch": 1000.0, "extra_bytes": 0,
             "writer_ops": ["upsert_from"], "reader_ops": ["get_buffer"]}]
    job = {"dir": d, "out_dir": os.path.join(d, "reduced"),
           "meta_base": {"config": "cfg"}, "instances": inst}
    job_path = os.path.join(str(tmp_path), "job.json")
    with open(job_path, "w", encoding="utf-8") as f:
        json.dump(job, f)
    out = subprocess.run(
        [sys.executable,
         os.path.join(ROOT, "scripts", "kvtest_controller_job.py"),
         "--job", job_path], capture_output=True, text=True)
    assert out.returncode == 0, out.stdout + out.stderr
    data = json.load(open(os.path.join(d, "reduced",
                                       "8MB.reduced.json")))
    src = [s for s in data["sources"] if s.get("role") == "writer"]
    assert src
    assert src[0]["remote_path"] == site + "/" + os.path.basename(samples)
    assert src[0]["source"] == os.path.basename(base) + ".writer.samples.csv"
    assert "k8s-ws" not in json.dumps(data)


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


def test_controller_cp_uses_explicit_file_dest(monkeypatch):
    """真机回归：kubectl cp 的 dest 必须是显式文件路径。

    传裸目录时 kubectl 把目录当目标文件 -> "is a directory"（scp_pull 的
    远端分支正是用 stage/basename 才正常的）。
    """
    import benchmark as bm
    calls = []

    def fake_run(controller, argv, check=True, timeout=None):
        calls.append(list(argv))
        return "", 0

    monkeypatch.setattr(bm, "controller_run", fake_run)
    monkeypatch.setattr(bm, "controller_mkdir", lambda c, p: None)
    cluster = {"_controller": {"host": "ctl"}}
    node = {"_controller": {"host": "ctl"}, "_ns": "ns1", "_pod": "pod-a"}
    groups = [(node, ["/logs/a/summary.json",
                      "/logs/a/x.samples.csv.gz"], "kvtest i1")]
    bm._controller_cp_groups(cluster, groups, "/data/run1")
    assert len(calls) == 2
    for argv, src in zip(calls, ("summary.json", "x.samples.csv.gz")):
        assert argv[:3] == ["-n", "ns1", "cp"]
        assert argv[3].startswith("pod-a:/logs/a/")
        # explicit file destination — never the bare dir
        assert argv[4] == "/data/run1/" + src


def test_controller_cp_failure_raises(monkeypatch):
    import benchmark as bm
    monkeypatch.setattr(bm, "controller_run",
                        lambda c, argv, check=True, timeout=None:
                        ("error: open /x: is a directory", 1))
    monkeypatch.setattr(bm, "controller_mkdir", lambda c, p: None)
    node = {"_controller": {}, "_ns": "ns", "_pod": "p"}
    try:
        bm._controller_cp_groups(
            {"_controller": {"host": "ctl"}}, [(node, ["/f"], "l")], "/d")
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "kubectl cp failed" in str(exc)
