#!/usr/bin/env python3
"""kvtest 拉取策略测试：stats-only（原始 log/glog 不传输）+ 每 pod 独立目录
+ 现场索引上下文（节点 IP / host 日志目录）。
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import benchmark as bm

BASE = "case.i0_9301.kvtest_8MB"


def test_pod_layout_k8s_vs_ssh():
    k8s = {"ip": "10.42.0.5", "_pod": "pod-a"}
    ssh = {"ip": "10.0.0.9"}
    assert bm._kvtest_pod_dir("/logs/ts", k8s) == "/logs/ts/pod-a"
    assert bm._kvtest_pod_base("/logs/ts", BASE, k8s) == \
        "/logs/ts/pod-a/" + BASE
    # ssh 模式保持平铺（无 pod 概念）
    assert bm._kvtest_pod_dir("/logs/ts", ssh) == "/logs/ts"
    assert bm._kvtest_pod_base("/logs/ts", BASE, ssh) == "/logs/ts/" + BASE


def test_flatten_targets_pod_dir_for_k8s(monkeypatch):
    commands = []
    node = {"ip": "10.42.0.5", "_pod": "pod-a"}
    monkeypatch.setattr(bm, "get_node", lambda cluster, name: node)

    def fake_ssh(ip, n, command, quiet=False):
        commands.append(command)
        return "summary=OK samples=OK"

    monkeypatch.setattr(bm, "ssh_check_output", fake_ssh)
    entry = {"node": "pod-a", "_base": BASE, "_out_dir": "/logs/ts/pod-a/kvtest_out/case.i0"}
    ok, _status = bm._kvtest_flatten_artifacts(
        {"nodes": {}}, entry, "/logs/ts")
    assert ok
    assert "/logs/ts/pod-a/" + BASE + ".summary.json" in commands[0]
    assert "/logs/ts/pod-a/" + BASE + ".samples.csv" in commands[0]


def test_remote_trace_map_uses_ip_display_and_pod(monkeypatch):
    node = {"ip": "10.42.0.5", "_pod": "pod-a", "host_ip": "192.168.137.11",
            "kind": "k8s-pod"}
    cluster = {"nodes": {"pod-a": node}}
    monkeypatch.setattr(bm, "get_node", lambda c, name: node)
    captured = {}

    def fake_map(cluster_, node_, glogs, remote_scripts, globals_cfg, **kw):
        captured.update({"glogs": glogs, "node_name": kw.get("node_name"),
                         "role": kw.get("role"), "size": kw.get("size")})
        return [glogs[0] + ".trace.json"]

    monkeypatch.setattr(bm, "run_tracestats_map", fake_map)
    entry = {"node": "pod-a", "_base": BASE, "_ds": "8MB", "_role": "dual"}
    out = bm._kvtest_remote_trace_map(
        cluster, entry, "/logs/ts", "/scripts", {}, best_effort=False)
    assert out == ["/logs/ts/pod-a/" + BASE + ".log.glog.trace.json"]
    assert captured["glogs"] == ["/logs/ts/pod-a/" + BASE + ".log.glog"]
    # dual 实例不传 role（一个 glog 同时含 writer+reader）
    assert captured["role"] is None
    assert captured["size"] == "8MB"
    # 节点显示名解析为宿主 IP（run_tracestats_map 内部同样走 _node_display）
    assert bm._node_display(cluster, node, "pod-a") == "192.168.137.11"


def test_report_context_k8s_and_ssh(monkeypatch):
    monkeypatch.setattr(bm, "k8s_settings", lambda cluster: {
        "host_log_root": "/var/lib/mooncake-bench", "namespace": "mc-ns"})
    ctx = bm._kvtest_report_context({}, "20261121-1244", "/logs/ts", True)
    assert ctx == {"run_id": "20261121-1244", "remote_log_dir": "/logs/ts",
                   "host_log_root": "/var/lib/mooncake-bench",
                   "k8s_namespace": "mc-ns"}
    ctx = bm._kvtest_report_context({}, "20261121-1244", "/logs/ts", False)
    assert ctx == {"run_id": "20261121-1244", "remote_log_dir": "/logs/ts"}


def test_small_log_bytes_parses_remote_sum(monkeypatch):
    node = {"ip": "10.42.0.5", "_pod": "pod-a"}
    monkeypatch.setattr(bm, "get_node", lambda c, name: node)
    monkeypatch.setattr(bm, "ssh_check_output",
                        lambda ip, n, cmd, quiet=False: " 4096")
    entry = {"node": "pod-a", "_base": BASE}
    assert bm._kvtest_small_log_bytes({}, entry, "/logs/ts") == 4096


def _write_instance(logs, iid=0, port=9301, ds="8MB"):
    """最小 kvtest 实例：samples.csv + summary.json（writer/reader 各一行）。"""
    base = bm._kvtest_artifact_base("kvtest_demo", iid, port, ds)
    rows = [
        {"elapsed_us": 0, "instance_id": iid, "operation": "upsert_from",
         "request_index": 0, "latency_ms": 1.0, "status": "OK", "bytes": 8388608},
        {"elapsed_us": 100000, "instance_id": iid, "operation": "get_buffer",
         "request_index": 1, "latency_ms": 2.0, "status": "OK", "bytes": 8388608},
    ]
    with open(os.path.join(logs, base + ".samples.csv"), "w",
              encoding="utf-8", newline="") as f:
        w = __import__("csv").DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    with open(os.path.join(logs, base + ".summary.json"), "w",
              encoding="utf-8") as f:
        json.dump({"instance_id": iid, "elapsed_seconds": 1.0,
                   "verify_failures": 0,
                   "operations": {"upsert_from": {"requests": 1},
                                  "get_buffer": {"requests": 1}}}, f)
    return base


def test_map_stats_ip_pod_remote_path_and_trace_bytes(tmp_path):
    logs = str(tmp_path / "logs")
    os.makedirs(logs)
    base = _write_instance(logs)
    # 现场 trace 产物（glog 真实字节）—— 原始 glog 不拉回
    with open(os.path.join(logs, base + ".log.glog.trace.json"), "w",
              encoding="utf-8") as f:
        json.dump({"meta": {"bytes": 123456789}}, f)
    # 侧车：节点显示名=IP、pod 名、远端日志根
    bm._kvtest_write_report_meta(
        logs, {"_kvtest_iid_nodes": {"0": "192.168.137.11"},
               "_kvtest_iid_pods": {"0": "pod-a"},
               "_kvtest_start_epochs": {"8MB": 1763596800.0}},
        {"run_id": "20261121-1244", "remote_log_dir": "/logs/ts"})
    cfg = {"_kvtest_extra_bytes": {base: 4096}}
    mapped = bm._kvtest_map_stats(logs, cfg)
    assert mapped
    writer = json.load(open(
        [p for p in mapped if ".writer.stats.json" in p][0], encoding="utf-8"))
    assert writer["meta"]["node"] == "192.168.137.11"
    assert writer["meta"]["pod"] == "pod-a"
    assert writer["meta"]["remote_path"] == \
        "/logs/ts/pod-a/" + base + ".samples.csv"
    # stats-only：日志量 = trace meta.bytes（glog）+ 远端小文件字节
    assert writer["meta"]["bytes"] == 123456789 + 4096
    # samples 自身体积单列保留
    assert writer["meta"]["samples_bytes"] > 0
