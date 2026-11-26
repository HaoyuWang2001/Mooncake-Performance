#!/usr/bin/env python3
"""现场日志实测大小（du -sb）：节点采集 → report/site_sizes.json → 报表。

覆盖：k8s(kubectl exec)/ssh 两条采集通道、失败静默回退、sidecar 不被成功清理
删除、现场日志表与 V-7 数据量卡消费 du 值、无 du 时回退旧口径。
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import benchmark as bm                     # noqa: E402
import report as rp                        # noqa: E402
from test_report import _site_stats        # noqa: E402


def _k8s_cluster():
    return {"_controller": object(),
            "nodes": {"n1": {"ip": "10.0.0.1", "host_ip": "192.168.1.10",
                             "_pod": "pod-a", "kind": "k8s-pod"}}}


def test_site_du_snapshot_k8s_writes_report_sidecar(monkeypatch, tmp_path):
    seen = {}

    def fake_run(c, argv, check=False, timeout=None):
        seen["argv"] = argv
        out = ("123\t/workspace/mp/logs/20260101-0101\n"
               "100\t/workspace/mp/logs/20260101-0101/pod-a\n"
               "23\t/workspace/mp/logs/20260101-0101/profiling/cfg\n")
        return out, 0

    monkeypatch.setattr(bm, "controller_run", fake_run)
    monkeypatch.setattr(bm, "k8s_settings", lambda c: {
        "host_log_root": "/var/lib/mb", "pod_workspace": "/workspace/mp",
        "namespace": "ns"})
    monkeypatch.setattr(bm, "get_node", lambda c, n: c["nodes"][n])
    snap = bm._site_du_snapshot(_k8s_cluster(), "20260101-0101", "cfg",
                                ["n1"], str(tmp_path), "/unused")
    assert "exec" in seen["argv"] and "du -sb" in seen["argv"][-1]
    path = tmp_path / "logs" / "cfg" / "report" / "site_sizes.json"
    saved = json.loads(open(path, encoding="utf-8").read())
    node = saved["nodes"]["192.168.1.10"]
    # pod 内路径已换算为节点侧 hostPath 孪生路径
    assert node["total"] == 123
    assert node["root"] == "/var/lib/mb/20260101-0101"
    assert node["dirs"]["/var/lib/mb/20260101-0101/pod-a"] == 100
    assert node["dirs"]["/var/lib/mb/20260101-0101/profiling/cfg"] == 23
    assert snap and snap["nodes"]


def test_site_du_snapshot_ssh_and_failure(monkeypatch, tmp_path):
    cluster = {"nodes": {"n1": {"ip": "10.0.0.1"}}}
    monkeypatch.setattr(bm, "get_node", lambda c, n: c["nodes"][n])
    monkeypatch.setattr(bm, "k8s_settings", lambda c: (_ for _ in ()).throw(
        RuntimeError("ssh mode")))
    monkeypatch.setattr(bm, "ssh_check_output", lambda ip, n, cmd,
                        quiet=True: (
        "50\t/remote/logs\n48\t/remote/logs/profiling/cfg\n"))
    snap = bm._site_du_snapshot(cluster, "ts", "cfg", ["n1"], str(tmp_path),
                                "/remote/logs")
    assert snap["nodes"]["10.0.0.1"]["total"] == 50
    # du 失败/不可用：静默返回 None，不写 sidecar
    monkeypatch.setattr(bm, "ssh_check_output", lambda *a, **k: "")
    assert bm._site_du_snapshot(cluster, "ts", "cfg2", ["n1"],
                                str(tmp_path), "/remote/logs") is None
    assert not (tmp_path / "logs" / "cfg2" / "report" /
                "site_sizes.json").exists()


def test_site_sizes_survive_intermediate_cleanup(tmp_path):
    d = tmp_path / "logs" / "cfg"
    (d / "report").mkdir(parents=True)
    (d / "report" / "site_sizes.json").write_text("{}", encoding="utf-8")
    (d / "a.stats.json").write_text("{}", encoding="utf-8")
    bm._cleanup_config_intermediates(str(d))
    assert (d / "report" / "site_sizes.json").is_file()
    assert not (d / "a.stats.json").exists()


def test_logsite_and_volume_use_du_sizes(tmp_path):
    root, ts = "/var/lib/mb", "20260101-0101"
    base = root + "/" + ts
    a = _site_stats(tmp_path, "a.writer", "writer", "10.0.0.1", "pod-a", base)
    b = _site_stats(tmp_path, "b.reader", "reader", "10.0.0.1", "pod-a", base)
    sites = {"10.0.0.1": {"pod": "mc-mon-1",
                          "remote_dir": base + "/profiling/cfg",
                          "files": 2, "bytes": 100}}
    sizes = {"nodes": {"10.0.0.1": {"total": 900, "root": base,
                                    "dirs": {base: 900,
                                             base + "/pod-a": 800,
                                             base + "/profiling/cfg": 90}}}}
    meta = {"config": "cfg", "mode": "pipeline",
            "host_log_root": root, "run_id": ts}
    data = rp.reduce_scenario(rp.load([a, b]), meta, monitor_sites=sites,
                              site_sizes=sizes)
    rows = data["logsite"]
    node = [r for r in rows if r["tier"] == "node"][0]
    pod = [r for r in rows if r["tier"] == "pod"][0]
    mon = [r for r in rows if r["tier"] == "monitor"][0]
    assert pod["bytes"] == 800                     # du 实测（非源头记录）
    assert mon["bytes"] == 90
    assert node["bytes"] == 890                    # pods + profiling
    blk = list(data["sizes"].values())[0]
    site = blk["volume"]["site"]
    assert site["total"] == 890
    assert site["nodes"][0]["pods"] == 800
    assert site["nodes"][0]["profiling"] == 90
    # 无 du 数据 → 回退源头记录口径（已知产物合计）
    d2 = rp.reduce_scenario(rp.load([a, b]), meta, monitor_sites=sites)
    pod2 = [r for r in d2["logsite"] if r["tier"] == "pod"][0]
    expect = sum(int(s["bytes"]) for s in d2["sources"] if s["pod"] == "pod-a")
    assert pod2["bytes"] == expect
    assert "site" not in list(d2["sizes"].values())[0]["volume"]


def test_volume_renders_du_rows(tmp_path):
    a = _site_stats(tmp_path, "a.writer", "writer", "10.0.0.1", "pod-a",
                    "/var/lib/mb/20260101-0101")
    sizes = {"nodes": {"10.0.0.1": {"total": 900,
                                    "root": "/var/lib/mb/20260101-0101",
                                    "dirs": {"/var/lib/mb/20260101-0101/pod-a":
                                             800}}}}
    data = rp.reduce_scenario(rp.load([a]),
                              {"config": "cfg", "mode": "pipeline",
                               "host_log_root": "/var/lib/mb",
                               "run_id": "20260101-0101"},
                              site_sizes=sizes)
    html, _dj = rp.render_scenario(
        data, str(tmp_path / "r"),
        os.path.join(ROOT, "webui", "web", "vendor", "echarts.min.js"))
    src = open(html, encoding="utf-8").read()
    assert "留存" in src and "du -sb 实测" in src
