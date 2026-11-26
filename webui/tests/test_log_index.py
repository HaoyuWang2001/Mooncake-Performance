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


def test_logsite_rows_follow_process_dirs():
    """logsite 由产物 meta.remote_path 生成：pod 行=pod 目录（进程子目录
    聚合），字节=meta.bytes 之和；监控行始终存在。"""
    from types import SimpleNamespace
    import report as rp
    meta = {"run_id": "20261126-1141",
            "host_log_root": "/var/lib/mooncake-bench"}
    base = "/var/lib/mooncake-bench/20261126-1141/pod-a"
    sources = [
        {"node": "10.0.0.1", "pod": "pod-a", "role": "writer",
         "source": "case.writer.stats.json.gz",
         "remote_path": base + "/writer_i0_p9101/case.writer.stats.json.gz",
         "bytes": 1000},
        {"node": "10.0.0.1", "pod": "pod-a", "role": "reader",
         "source": "case.reader.stats.json.gz",
         "remote_path": base + "/reader_i4_p9201/case.reader.stats.json.gz",
         "bytes": 2000},
    ]
    rows = rp.build_logsite(meta, sources, {})
    pod = next(r for r in rows if r["tier"] == "pod")
    assert pod["dir"] == base
    assert pod["bytes"] == 3000
    assert pod["files"] == 2
    assert set(pod["role"].split("、")) == {"writer", "reader"}
    node = next(r for r in rows if r["tier"] == "node")
    assert node["dir"] == "/var/lib/mooncake-bench/20261126-1141"
    assert any(r["tier"] == "monitor" for r in rows)
    assert all("du" not in str(r) for r in rows)


def test_runners_set_mc_log_dir():
    """每进程一个 <dir>/<client>.glog.d（glog 全量落文件 + 轮转隔离）。"""
    pat = ('export MC_LOG_DIR="${MC_LOG_DIR:-$(dirname "$LOG_FILE")/'
           '$(basename "$LOG_FILE" .log).glog.d}"')
    for runner in ("run_kvtest.sh", "run_benchmark_pipeline.sh",
                   "run_benchmark_basic.sh"):
        src = open(os.path.join(ROOT, "scripts", runner),
                   encoding="utf-8").read()
        assert pat in src, runner
        assert 'mkdir -p "$MC_LOG_DIR"' in src, runner
    for runner in ("run_benchmark_pipeline.sh", "run_benchmark_basic.sh"):
        src = open(os.path.join(ROOT, "scripts", runner),
                   encoding="utf-8").read()
        assert "MC_LOG_DIR=%s" in src, runner


def test_kvtest_runner_drops_stderr_glog():
    """.log.glog 彻底删除：stderr 并入叙事 .log，glog 只在 MC_LOG_DIR。"""
    src = open(os.path.join(ROOT, "scripts", "run_kvtest.sh"),
               encoding="utf-8").read()
    assert '"$LOG_FILE" 2>&1' in src
    assert '2> "$LOG_FILE.glog"' not in src
    # basic 模式每 client 独立 MC_LOG_DIR
    basic = open(os.path.join(ROOT, "scripts", "run_benchmark_basic.sh"),
                 encoding="utf-8").read()
    assert 'MC_LOG_DIR="$LOG_DIR/client.$i.glog.d"' in basic


def test_resolve_glog_inputs_info_then_legacy(monkeypatch):
    """MC_LOG_DIR 的 *.INFO.*（regular files, 轮转多段）优先；否则旧 .glog。"""
    monkeypatch.setattr(bm, "k8s_settings", lambda c: {
        "pod_workspace": "/ws", "host_log_root": "/var/lib/mooncake-bench"})
    node = {"ip": "10.0.0.1"}

    info = ("/ws/logs/ts/pod-a/x.glog.d/prog.host.root.log.INFO.20261126-134505.45\n"
            "/ws/logs/ts/pod-a/x.glog.d/prog.host.root.log.INFO.20261126-135505.45")
    monkeypatch.setattr(bm, "ssh_check_output", lambda *a, **k: info)
    files, site = bm._resolve_glog_inputs(
        K8S, node, "/ws/logs/ts/pod-a/x.glog.d", "/ws/logs/ts/pod-a/x.log.glog")
    assert [os.path.basename(f) for f in files] == [
        "prog.host.root.log.INFO.20261126-134505.45",
        "prog.host.root.log.INFO.20261126-135505.45"]
    assert site == "/var/lib/mooncake-bench/ts/pod-a/x.glog.d"

    legacy = "/ws/logs/ts/pod-a/x.log.glog"
    monkeypatch.setattr(bm, "ssh_check_output", lambda *a, **k: legacy)
    # python 引擎保留旧回退；kvtest（allow_legacy=False）严格不回落
    files, site = bm._resolve_glog_inputs(
        K8S, node, "/ws/logs/ts/pod-a/x.glog.d", legacy)
    assert files == [legacy]
    assert site == "/var/lib/mooncake-bench/ts/pod-a/x.log.glog"
    assert bm._resolve_glog_inputs(
        K8S, node, "/ws/logs/ts/pod-a/x.glog.d", legacy,
        allow_legacy=False) == ([], "")

    monkeypatch.setattr(bm, "ssh_check_output", lambda *a, **k: "")
    assert bm._resolve_glog_inputs(
        K8S, node, "/ws/logs/ts/pod-a/x.glog.d", legacy) == ([], "")


def test_kvtest_config_declares_key_prefixes():
    """key 前缀由编排机下发：warmup 与测量必须不同前缀；并行角色分域。"""
    cfg = {"scenario": "kvtest", "protocol": "tcp", "data_size": ["8KB"],
           "batch_size": 1, "duration": 30, "warmup": {"duration": 5},
           "writer": {"operation": "put", "count_per_node": 1},
           "reader": {"operation": "get_buffer", "count_per_node": 1},
           "kvtest": {}}
    session = {"SETUP_MASTER_ADDR": "10.0.0.1:9003"}
    plan = {"writer_threads": 1, "reader_threads": 1,
            "notify_threads": 1, "notify_count": 1}
    entry = {"iid": 0, "port": 9301, "_out_dir": "/ws/kvtest_out/x",
             "_cpu_affinity": "", "_qps": 20}
    inst = [{"host": "10.0.0.1", "port": 9301, "iid": 0, "role": "writer"}]
    for ns, kp, wp in ((None, "mooncake_kvtest_", "mooncake_kvtest_warmup_"),
                       ("rank", "mooncake_kvtest_rank_",
                        "mooncake_kvtest_warmup_rank_"),
                       ("recall", "mooncake_kvtest_recall_",
                        "mooncake_kvtest_warmup_recall_")):
        c = bm.build_kvtest_instance_config(
            cfg, {}, session, plan, entry, "writer", "8KB", 100, inst,
            key_namespace=ns)
        assert c["key_prefix"] == kp
        assert c["warmup_key_prefix"] == wp
        assert c["warmup_key_prefix"] != c["key_prefix"]


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
