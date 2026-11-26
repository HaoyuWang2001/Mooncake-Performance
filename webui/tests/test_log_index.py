#!/usr/bin/env python3
"""日志目录契约：节点侧路径索引（site dir）+ MC_LOG_DIR + remove 服务日志。

报表索引必须是节点上真实存在的路径，且由产物生成时（collect / job / map）
写入——不做渲染侧改写。目标是「ssh <节点IP> → cd <目录> → 看到全部日志」。
"""
import json
import os
import sys
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import benchmark as bm  # noqa: E402

K8S = {"k8s": {"pod_workspace": "/workspace/mp",
               "host_log_root": "/var/lib/mooncake-bench",
               "namespace": "ns", "image": "img",
               "controller_workspace": "/cws"}}


def test_pod_path_to_site_maps_mount():
    p = "/workspace/mp/logs/20261126-1141/pod-a/x.log"
    assert bm._pod_path_to_site(K8S, p) == (
        "/var/lib/mooncake-bench/20261126-1141/pod-a/x.log")


def test_pod_path_to_site_passthrough():
    # ssh mode (no k8s block) / outside the log mount / relative -> unchanged
    assert bm._pod_path_to_site({"nodes": {}}, "/data/logs/x.log") == (
        "/data/logs/x.log")
    assert bm._pod_path_to_site(K8S, "/workspace/mp/data/x.log") == (
        "/workspace/mp/data/x.log")
    assert bm._pod_path_to_site(K8S, "relative/x.log") == "relative/x.log"


def test_site_dirs_and_remote_names():
    cfg = {"_kvtest_iid_pods": {"0": "pod-a", "1": "pod-b"}}
    dirs = bm._kvtest_site_dirs(
        K8S, "/workspace/mp/logs/20261126-1141", cfg)
    assert dirs == {
        "0": "/var/lib/mooncake-bench/20261126-1141/pod-a",
        "1": "/var/lib/mooncake-bench/20261126-1141/pod-b"}
    names = bm._kvtest_remote_names([(
        None,
        ["/w/logs/ts/pod-a/a.samples.csv.gz",
         "/w/logs/ts/pod-a/a.summary.json"],
        "kvtest i0")])
    assert names == {"a.samples.csv": "a.samples.csv.gz",
                     "a.summary.json": "a.summary.json"}


def test_meta_base_merges_context():
    args = SimpleNamespace(cluster="c.json", global_cfg="g.json",
                           profile="p.json")
    meta = bm._kvtest_report_meta_base({}, args, "cfg", "8KB", context={
        "run_id": "20261126-1141",
        "host_log_root": "/var/lib/mooncake-bench",
        "remote_log_dir": "/workspace/mp/logs/20261126-1141",
        "k8s_namespace": "ns"})
    assert meta["run_id"] == "20261126-1141"
    assert meta["host_log_root"] == "/var/lib/mooncake-bench"
    assert meta["remote_log_dir"] == "/workspace/mp/logs/20261126-1141"
    assert meta["k8s_namespace"] == "ns"


def test_local_map_uses_node_site_path(tmp_path, monkeypatch):
    d = str(tmp_path)
    name = "kv1.i0_p9301.kvtest_8KB.samples.csv"
    with open(os.path.join(d, name), "w", encoding="utf-8") as f:
        f.write("h\n")
    with open(os.path.join(d, "kvtest_report_meta.json"), "w",
              encoding="utf-8") as f:
        json.dump({"iid_nodes": {"0": "10.0.0.1"},
                   "iid_pods": {"0": "pod-a"},
                   "start_epochs": {"8KB": 1000.0},
                   "remote_log_dir": "/workspace/mp/logs/ts",
                   "site_dirs": {"0": "/var/lib/mooncake-bench/ts/pod-a"},
                   "remote_names": {name: name + ".gz"}}, f)

    def _compute(*a, **k):
        return {"meta": {"rows": 1, "bytes": 1,
                         "node": k.get("node"), "pod": k.get("pod"),
                         "role": k.get("role"), "size": k.get("size"),
                         "source": k.get("source_name"),
                         "remote_path": k.get("remote_path")},
                "global": {}}

    fake = SimpleNamespace(kvtest_summary_payload=lambda p, ops: None,
                           compute_stats=_compute)
    monkeypatch.setattr(bm, "_logstats_module", lambda: fake)
    written = bm._kvtest_map_stats(d, {})
    assert written
    doc = json.load(open(written[0], encoding="utf-8"))
    assert doc["meta"]["remote_path"] == (
        "/var/lib/mooncake-bench/ts/pod-a/" + name + ".gz")


def test_runners_set_mc_log_dir():
    for runner in ("run_kvtest.sh", "run_benchmark_pipeline.sh",
                   "run_benchmark_basic.sh"):
        src = open(os.path.join(ROOT, "scripts", runner),
                   encoding="utf-8").read()
        assert ('export MC_LOG_DIR="${MC_LOG_DIR:-$(dirname "$LOG_FILE")}"'
                in src), runner
    for runner in ("run_benchmark_pipeline.sh", "run_benchmark_basic.sh"):
        src = open(os.path.join(ROOT, "scripts", runner),
                   encoding="utf-8").read()
        assert "MC_LOG_DIR=%s" in src, runner


def test_remove_service_log_lands_in_pod_site_dir(monkeypatch):
    monkeypatch.setattr(bm, "k8s_settings", lambda c: {
        "pod_workspace": "/ws", "namespace": "ns",
        "controller_workspace": "/cws"})
    monkeypatch.setattr(bm, "_render_remove_deployment_yaml",
                        lambda cluster, ts, cfg: ("mc-x-remove", "lbl", "yaml"))
    monkeypatch.setattr(bm, "controller_put", lambda *a, **k: None)

    def _run(c, argv, check=True, timeout=None):
        if "get" in argv:
            return "pod-x", 0
        return "", 0

    monkeypatch.setattr(bm, "controller_run", _run)
    monkeypatch.setattr(bm, "wait_k8s_deployment_ready",
                        lambda *a, **k: None)
    monkeypatch.setattr(bm, "_k8s_pod_ip", lambda *a, **k: "10.0.0.9")
    cmds = []
    monkeypatch.setattr(bm, "ssh_check_output",
                        lambda ip, node, cmd, **k: cmds.append(cmd) or "200")
    cluster = {"_controller": {"ip": "1.2.3.4"}}
    assert bm._remove_service_ensure(cluster, {}, "20261126-1141", "cfg")
    launch = next(c for c in cmds if "remove_service.py" in c)
    assert "/ws/logs/20261126-1141/pod-x/remove_service.log" in launch


def test_report_index_is_the_logsite_table():
    """现场日志表 = 唯一现场索引：路径列给出节点侧目录（ssh+cd 即达），
    旧「节点与登录现场」/「其它产物索引」不得回潮。"""
    src = open(os.path.join(ROOT, "scripts", "report.py"),
               encoding="utf-8").read()
    assert "logsite-table" in src and "日志属性" in src and "日志路径" in src
    assert "节点与登录现场" not in src
    assert "其它产物索引" not in src
