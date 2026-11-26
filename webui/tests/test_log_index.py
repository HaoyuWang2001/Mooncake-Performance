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
    files, site = bm._resolve_glog_inputs(
        K8S, node, "/ws/logs/ts/pod-a/x.glog.d", legacy)
    assert files == [legacy]
    assert site == "/var/lib/mooncake-bench/ts/pod-a/x.log.glog"

    monkeypatch.setattr(bm, "ssh_check_output", lambda *a, **k: "")
    assert bm._resolve_glog_inputs(
        K8S, node, "/ws/logs/ts/pod-a/x.glog.d", legacy) == ([], "")


def test_trace_bytes_accepts_new_and_legacy_names(tmp_path):
    logs = tmp_path
    (logs / "x.trace.json").write_text(
        json.dumps({"meta": {"bytes": 123}}), encoding="utf-8")
    (logs / "y.log.glog.trace.json").write_text(
        json.dumps({"meta": {"bytes": 456}}), encoding="utf-8")
    assert bm._kvtest_trace_bytes(str(logs), "x") == 123
    assert bm._kvtest_trace_bytes(str(logs), "y") == 456


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


def test_report_index_points_at_directory():
    src = open(os.path.join(ROOT, "scripts", "report.py"),
               encoding="utf-8").read()
    assert "cd '+dir" in src
    assert "ssh <节点IP> 后 cd 该目录即可看到全部日志" in src
    assert ".samples.csv(.gz)" in src
