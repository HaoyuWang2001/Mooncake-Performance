#!/usr/bin/env python3
"""kvtest transfer contract: per-process dirs, strict trace/gzip, no samples."""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import benchmark as bm

BASE = "case.i0_9301.kvtest_8MB"


def test_instance_dir_is_per_process_under_the_pod():
    k8s = {"ip": "10.42.0.5", "_pod": "pod-a"}
    ssh = {"ip": "10.0.0.9"}
    assert bm._kvtest_pod_dir("/logs/ts", k8s) == "/logs/ts/pod-a"
    assert bm._kvtest_instance_dir("/logs/ts", k8s, "writer", 0, 9101) == \
        "/logs/ts/pod-a/writer_i0_p9101"
    # same pod, two processes -> two strictly separated directories
    assert bm._kvtest_instance_dir("/logs/ts", k8s, "reader", 0, 9201) == \
        "/logs/ts/pod-a/reader_i0_p9201"
    # ssh mode has no pod segment
    assert bm._kvtest_pod_dir("/logs/ts", ssh) == "/logs/ts"
    assert bm._kvtest_instance_dir(
        "/logs/ts", ssh, "writer", 3, 9101) == "/logs/ts/writer_i3_p9101"


def test_gzip_is_strict(monkeypatch):
    node = {"ip": "10.42.0.5", "_pod": "pod-a"}
    monkeypatch.setattr(bm, "get_node", lambda c, name: node)
    monkeypatch.setattr(bm, "ssh_check_output",
                        lambda *a, **k: "gzip: no such file")
    assert bm._kvtest_gzip_remote_files(
        {}, "pod-a", ["/x/a.stats.json"]) == ["/x/a.stats.json.gz"]

    def fail(*a, **k):
        raise RuntimeError("ssh down")

    monkeypatch.setattr(bm, "ssh_check_output", fail)
    try:
        bm._kvtest_gzip_remote_files({}, "pod-a", ["/x/a.stats.json"])
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "gzip failed" in str(exc)


def test_remote_trace_map_uses_process_base_and_strict_glog(monkeypatch):
    node = {"ip": "10.42.0.5", "_pod": "pod-a", "host_ip": "192.168.137.11",
            "kind": "k8s-pod"}
    cluster = {"nodes": {"pod-a": node}}
    monkeypatch.setattr(bm, "get_node", lambda c, name: node)
    captured = {}
    segs = ["/logs/ts/pod-a/writer_i0_p9101/case.glog.d/x.INFO.1"]

    def fake_inputs(cluster_, node_, glog_dir, legacy, dry=False,
                    allow_legacy=True):
        captured.update({"glog_dir": glog_dir, "legacy": legacy,
                         "allow_legacy": allow_legacy})
        return list(segs), "/node/site"

    def fake_map(cluster_, node_, glogs, remote_scripts, globals_cfg, **kw):
        captured.update({"glogs": glogs, "node_name": kw.get("node_name"),
                         "role": kw.get("role"), "size": kw.get("size"),
                         "out_path": kw.get("out_path"),
                         "remote_path": kw.get("remote_path")})
        return [kw["out_path"]]

    monkeypatch.setattr(bm, "_resolve_glog_inputs", fake_inputs)
    monkeypatch.setattr(bm, "run_tracestats_map", fake_map)
    entry = {"node": "pod-a", "_role": "writer", "_ds": "8MB",
             "_log_base": "/logs/ts/pod-a/writer_i0_p9101/case"}
    out = bm._kvtest_remote_trace_map(
        cluster, entry, "/logs/ts", "/scripts", {}, best_effort=False)
    assert out == ["/logs/ts/pod-a/writer_i0_p9101/case.trace.json"]
    assert captured["glog_dir"] == \
        "/logs/ts/pod-a/writer_i0_p9101/case.glog.d"
    # kvtest never falls back to the legacy stderr capture
    assert captured["allow_legacy"] is False
    assert captured["role"] == "writer" and captured["size"] == "8MB"
    assert captured["out_path"].endswith(".trace.json")
    assert bm._node_display(cluster, node, "pod-a") == "192.168.137.11"

    # strict: no .INFO.* segments -> hard error, no partial trace
    monkeypatch.setattr(bm, "_resolve_glog_inputs",
                        lambda *a, **k: ([], ""))
    try:
        bm._kvtest_remote_trace_map(
            cluster, entry, "/logs/ts", "/scripts", {}, best_effort=False)
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "glog" in str(exc)


def test_report_context_k8s_and_ssh(monkeypatch):
    monkeypatch.setattr(bm, "k8s_settings", lambda cluster: {
        "host_log_root": "/var/lib/mooncake-bench", "namespace": "mc-ns"})
    ctx = bm._kvtest_report_context({}, "20261121-1244", "/logs/ts", True)
    assert ctx == {"run_id": "20261121-1244", "remote_log_dir": "/logs/ts",
                   "host_log_root": "/var/lib/mooncake-bench",
                   "k8s_namespace": "mc-ns"}
    ctx = bm._kvtest_report_context({}, "20261121-1244", "/logs/ts", False)
    assert ctx == {"run_id": "20261121-1244", "remote_log_dir": "/logs/ts"}
