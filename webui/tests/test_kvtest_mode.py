"""kvtest scenario tests: plan/iid/qps-split/instance-config/CSV summary.

The C++ mooncake_kvtest binary records summary.json operation keys exactly
as the config strings we ship (metric_name is the pre-normalization name),
so the aggregation matches on "upsert_from"/"get_buffer"/"notify_rpc"/
"notify_queue_wait".
"""
import argparse
import csv
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import benchmark as bm

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _kv_cfg(**over):
    cfg = {
        "scenario": "kvtest", "work_mode": "smoke", "protocol": "ub",
        "data_size": ["8MB"], "duration": 30, "base_qps": 105,
        "qps_multiple": 1, "batch_size": 1, "notify_count": 10,
        "client": {"count_per_node": 2, "port_base": 9301},
        "writer": {"operation": "upsert_from", "num_threads": 1,
                   "notify_threads": 10},
        "reader": {"operation": "get_buffer", "num_threads": 4},
    }
    cfg.update(over)
    return cfg


# ------------------------------------------------------------------ planning

def test_kvtest_plan_dual_iids_follow_client_ids():
    plan = bm.kvtest_plan(_kv_cfg(), node_count=2)
    assert plan["dual"] is True
    assert len(plan["clients"]) == 4                 # 2 per node x 2 nodes
    assert [c["iid"] for c in plan["clients"]] == [c["id"] for c in plan["clients"]]
    assert plan["writer_threads"] == 1


def test_kvtest_plan_separate_iids_are_global():
    cfg = _kv_cfg()
    del cfg["client"]
    cfg["writer"] = {"operation": "upsert_from", "count_per_node": 2,
                     "num_threads": 2, "notify_threads": 8}
    cfg["reader"] = {"operation": "get_buffer", "count_per_node": 3,
                     "num_threads": 4}
    plan = bm.kvtest_plan(cfg, node_count=2)
    assert not plan.get("dual")
    assert len(plan["writers"]) == 4 and len(plan["readers"]) == 6
    # writers 0..W-1, readers CONTINUE at W (keys/sender ids are namespaced
    # by instance_id, so writer and reader ids must never collide)
    assert [w["iid"] for w in plan["writers"]] == [0, 1, 2, 3]
    assert [r["iid"] for r in plan["readers"]] == list(range(4, 10))
    assert plan["writer_threads"] == 2


def test_kvtest_plan_split_pairs_processes_in_client_pods():
    cfg = _kv_cfg()
    cfg["client"]["process_mode"] = "split"
    cfg["writer"]["port_base"] = 9101
    cfg["reader"]["port_base"] = 9201
    plan = bm.kvtest_plan(cfg, node_count=2)

    assert not plan["dual"] and plan["split_pod"] is True
    assert plan["process_mode"] == "split"
    assert len(plan["clients"]) == 4
    assert len(plan["writers"]) == len(plan["readers"]) == 4
    assert [w["iid"] for w in plan["writers"]] == [0, 1, 2, 3]
    assert [r["iid"] for r in plan["readers"]] == [4, 5, 6, 7]
    # SSH uses per-node port_base+local; k8s rebases each process to the same
    # two in-pod ports after the client pods are materialized.
    assert [w["port"] for w in plan["writers"]] == [9101, 9102, 9101, 9102]
    assert [r["port"] for r in plan["readers"]] == [9201, 9202, 9201, 9202]
    assert bm._pipeline_k8s_roles(plan) == (
        ("client", plan["clients"], 9101),)

    for index, client in enumerate(plan["clients"]):
        client["node"] = "pod-{}".format(index)
    bm._kvtest_bind_split_pods(plan)
    assert [w["node"] for w in plan["writers"]] == [
        "pod-0", "pod-1", "pod-2", "pod-3"]
    assert [r["node"] for r in plan["readers"]] == [
        "pod-0", "pod-1", "pod-2", "pod-3"]
    assert all(w["port"] == 9101 for w in plan["writers"])
    assert all(r["port"] == 9201 for r in plan["readers"])


def test_kvtest_plan_rejects_invalid_split_shape():
    cfg = _kv_cfg()
    cfg["client"]["process_mode"] = "invalid"
    try:
        bm.kvtest_plan(cfg, node_count=2)
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "process_mode" in str(exc)

    cfg["client"]["process_mode"] = "split"
    cfg["writer"]["port_base"] = 9101
    cfg["reader"]["port_base"] = 9101
    try:
        bm.kvtest_plan(cfg, node_count=2)
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "different" in str(exc)


def test_kvtest_split_qps_matches_binary_rate_gate():
    # 105 across 2 instances: 53 + 52 (the binary's own thread split)
    assert [bm._kvtest_split_qps(105, 2, i) for i in range(2)] == [53, 52]
    # exact division and single instance
    assert [bm._kvtest_split_qps(104, 2, i) for i in range(2)] == [52, 52]
    assert bm._kvtest_split_qps(60, 1, 0) == 60


def test_kvtest_warmup_duration_and_no_warmup_override():
    cfg = {"warmup": {"duration": 5}}
    assert bm._kvtest_warmup_duration(
        cfg, argparse.Namespace(no_warmup=False)) == 5.0
    assert bm._kvtest_warmup_duration(
        cfg, argparse.Namespace(no_warmup=True)) == 0.0
    try:
        bm._kvtest_warmup_duration(
            {"warmup": {"duration": -1}},
            argparse.Namespace(no_warmup=False))
        raise AssertionError("negative warmup should fail")
    except RuntimeError as exc:
        assert "warmup.duration" in str(exc)

    inherited = bm.load_scenario_config(os.path.join(
        ROOT, "configs", "kvtest", "smoke", "kvtest_smoke.json"))
    assert inherited["warmup"]["duration"] == 5


def test_kvtest_single_window_no_warmup_control_cycle():
    """warmup 不再单独成阶段：单窗口 duration+warmup，前 warmup 秒由 MP 侧
    数据面按 cutoff 剔除；/pause /drain /reset /arm /resume 循环整体移除。"""
    source = open(os.path.join(ROOT, "benchmark.py"), encoding="utf-8").read()
    for call in ('_control("/pause"', '_control("/drain"',
                 '_control("/reset"', '_control("/arm"',
                 '_control("/resume"'):
        assert call not in source, call
    # 单窗口 = warmup + duration
    assert "_total_window = float(warmup_duration) + float(duration)" in source
    assert '_wait_window(_total_window, "measurement")' in source


# ------------------------------------------------- per-instance config build

def test_build_kvtest_instance_config_rpc_notify():
    plan = bm.kvtest_plan(_kv_cfg(), node_count=1)
    entry = plan["clients"][0]
    entry["_qps"] = 53
    entry["_out_dir"] = "/remote/kvtest_out/i0"
    entry["_lhost_port"] = 12345
    entry["_cpu_affinity"] = "0-3"
    all_instances = [{"iid": c["iid"], "port": c["port"], "role": "dual",
                      "host": "10.1.{}.{}".format(1 + c["id"] // 2,
                                                   c["id"])}
                     for c in plan["clients"]]
    inst = bm.build_kvtest_instance_config(
        _kv_cfg(), {"local_buffer_size": 8589934592,
                    "tenant_id": "default"},
        {"SETUP_MASTER_ADDR": "10.0.0.10:50051",
         "SETUP_METADATA_SERVER": ""},
        plan, entry, "dual", "8MB", 256, all_instances)
    assert inst["role"] == "dual" and inst["instance_id"] == entry["iid"]
    assert inst["pipeline"] == ["upsert_from"]          # native op name
    assert inst["notify_pipeline"] == ["get_buffer"]
    assert inst["target_qps"] == 53
    assert inst["num_threads"] == 1
    assert inst["num_total_threads"] == 1 + plan["reader_threads"] == 5
    assert inst["notify_count"] == 10 and inst["notify_threads"] == 10
    assert inst["notify_rpc_port"] == entry["port"] + 10000
    assert inst["notify_rpc_connections_per_peer"] == 4
    assert inst["notify_rpc_reuse_limit"] == 4
    assert inst["key_pool_size"] == 256
    assert inst["start_paused"] is True
    # late-bound placeholders (filled by run_kvtest.sh per pod)
    assert inst["store"]["rdma_devices"] == "__UB_DEVICE__"
    assert inst["store"]["local_hostname"] == "__FABRIC_IP__:12345"
    # store block mirrors make_store() in http mode
    assert inst["store"]["metadata_server"] == \
        "http://10.0.0.10:50051/metadata"
    assert inst["store"]["master_server_address"] == "10.0.0.10:50051"
    # nodes view: the whole cluster as kvtest peers
    assert [n["instance_id"] for n in inst["nodes"]] == [0, 1]
    assert all(n["role"] == "dual" for n in inst["nodes"])
    assert all(n["notify_rpc_port"] == n["port"] + 10000
               for n in inst["nodes"])


def test_build_kvtest_instance_config_etcd_master():
    plan = bm.kvtest_plan(_kv_cfg(), node_count=1)
    entry = plan["clients"][0]
    entry["_qps"] = 52
    entry["_out_dir"] = "/remote/kvtest_out/i0"
    entry["_lhost_port"] = 12346
    inst = bm.build_kvtest_instance_config(
        _kv_cfg(), {"master_type": "etcd"},
        {"SETUP_ETCD_ENDPOINTS": "http://10.0.0.10:2379"},
        plan, entry, "dual", "8MB", 128, [])
    assert inst["store"]["metadata_server"] == "http://10.0.0.10:2379"
    assert inst["store"]["master_server_address"] == "http://10.0.0.10:2379"


def test_build_kvtest_split_pair_keeps_four_plus_twelve_budget():
    cfg = _kv_cfg()
    cfg["client"]["process_mode"] = "split"
    cfg["writer"].update({"num_threads": 4, "port_base": 9101})
    cfg["reader"].update({"num_threads": 12, "port_base": 9201})
    plan = bm.kvtest_plan(cfg, node_count=6)
    session = {"SETUP_MASTER_ADDR": "10.0.0.10:50051",
               "SETUP_METADATA_SERVER": ""}
    built = []
    for role, entry in (("writer", plan["writers"][0]),
                        ("reader", plan["readers"][0])):
        entry["_qps"] = 53 if role == "writer" else 0
        entry["_out_dir"] = "/remote/kvtest_out/{}".format(role)
        entry["_lhost_port"] = 12345 if role == "writer" else 12346
        built.append(bm.build_kvtest_instance_config(
            cfg, {"local_buffer_size": "1GB"}, session, plan, entry, role,
            "8MB", 256, []))

    writer, reader = built
    assert writer["role"] == "writer" and reader["role"] == "reader"
    assert writer["target_qps"] == 53 and reader["target_qps"] == 0
    assert writer["num_threads"] == reader["num_threads"] == 4
    assert writer["num_total_threads"] == reader["num_total_threads"] == 16


def test_kvtest_store_targets_require_endpoints():
    try:
        bm._kvtest_store_targets({"master_type": "etcd"}, {})
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "etcd_endpoints" in str(exc)


def test_kvtest_key_pool_zero_and_auto_have_distinct_semantics():
    plan = bm.kvtest_plan(_kv_cfg(), node_count=2)
    zero = _kv_cfg(kvtest={"key_pool_size": 0})
    assert bm._kvtest_key_pool(zero, {}, plan, 4, 8 << 20, "8MB") == 0
    auto = _kv_cfg(kvtest={"key_pool_size": "auto"})
    pool = bm._kvtest_key_pool(
        auto, {"store_total_size": "20GB"}, plan, 4, 8 << 20, "8MB")
    assert pool > 0


def test_kvtest_operation_maps_cover_documented_native_names():
    assert bm.KVTEST_WRITER_OPS["upsert"] == "upsert"
    assert bm.KVTEST_WRITER_OPS["batch_upsert_from"] == \
        "batch_upsert_from"
    assert bm.KVTEST_WRITER_OPS["remove"] == "remove"
    assert bm.KVTEST_READER_OPS["batch_get_buffer"] == "batch_get_buffer"
    assert bm.KVTEST_READER_OPS["exist"] == "exist"


def test_kvtest_runner_replaces_both_runtime_placeholders():
    runner = open(os.path.join(ROOT, "scripts", "run_kvtest.sh"),
                  encoding="utf-8").read()
    assert "s/__FABRIC_IP__/${fabric_escaped}/g" in runner
    assert "s/__UB_DEVICE__/${device_escaped}/g" in runner


def test_kvtest_runner_binds_cpu_and_memory_to_requested_numa_node():
    runner = open(os.path.join(ROOT, "scripts", "run_kvtest.sh"),
                  encoding="utf-8").read()
    assert "KVTEST_NUMA_NODE" in runner
    assert '"--cpunodebind=$KVTEST_NUMA_NODE"' in runner
    assert '"--membind=$KVTEST_NUMA_NODE"' in runner


def test_rank_smoke_numa_process_mapping():
    cfg = bm.load_scenario_config(_smoke_path("kvtest_rank_smoke.json"))
    plan = bm.kvtest_plan(cfg, node_count=1)
    assert [bm._kvtest_numa_node(cfg, entry, "writer")
            for entry in plan["writers"]] == [0, 2]
    assert [bm._kvtest_numa_node(cfg, entry, "reader")
            for entry in plan["readers"]] == [1, 3]


def test_recall_smoke_numa_process_mapping():
    cfg = bm.load_scenario_config(_smoke_path("kvtest_recall_smoke.json"))
    plan = bm.kvtest_plan(cfg, node_count=1)
    assert [bm._kvtest_numa_node(cfg, entry, "writer")
            for entry in plan["writers"]] == [0, 0, 0, 2, 2, 2]
    assert [bm._kvtest_numa_node(cfg, entry, "reader")
            for entry in plan["readers"]] == [1, 1, 1, 3, 3, 3]


def test_kvtest_artifact_flatten_includes_request_samples(monkeypatch):
    commands = []
    monkeypatch.setattr(
        bm, "get_node", lambda cluster, name: {"ip": "10.0.0.1"})

    def fake_ssh(ip, node, command, quiet=False):
        commands.append(command)
        return "summary=OK samples=OK"

    monkeypatch.setattr(bm, "ssh_check_output", fake_ssh)
    entry = {"node": "n1", "_base": "case.i0", "_out_dir": "/out/i0"}
    ok, status = bm._kvtest_flatten_artifacts(
        {"nodes": {}}, entry, "/remote/logs")
    assert ok and status.endswith("samples=OK")
    assert "latency_samples.csv" in commands[0]
    assert "/remote/logs/case.i0.samples.csv" in commands[0]
    assert "latency_timeseries.csv" not in commands[0]


def test_cleanup_removes_intermediates_and_keeps_report(tmp_path):
    """Per-config cleanup: everything but report/ and profiling/ goes (raw
    logs/glog/samples/summary/timeseries/PNG are all intermediates; --png
    runs skip the whole-run cleanup via --pull-raw-logs)."""
    nested = tmp_path / "logs" / "case"
    nested.mkdir(parents=True)
    disposable = [
        nested / "case.node_i0_p9101.kvtest_8MB.summary.json",
        nested / "case.node_i0_p9101.kvtest_8MB.samples.csv",
        nested / "case.node_i0_p9101.kvtest_8MB.timeseries.csv",
        nested / "case.node_i0_p9101.kvtest_8MB.log",
        nested / "case.node_i0_p9101.kvtest_8MB.log.glog",
        nested / "case.node_i0_p9101.kvtest_8MB.upsert.latency.png",
        nested / "case.node_i0_p9101.kvtest_8MB.log.stats.json",
    ]
    for path in disposable:
        path.write_text("x", encoding="utf-8")
    unit = nested / "report" / "8MB"
    unit.mkdir(parents=True)
    (unit / "report.html").write_text("<html></html>", encoding="utf-8")
    (unit / "data.js").write_text("x", encoding="utf-8")
    (nested / "profiling").mkdir()
    (nested / "profiling" / "mem.host.csv").write_text("x", encoding="utf-8")

    assert bm._cleanup_config_intermediates(str(nested)) == 7
    assert all(not path.exists() for path in disposable)
    assert (unit / "report.html").is_file()
    assert (nested / "profiling" / "mem.host.csv").is_file()


def test_kvtest_artifact_base_is_compact_and_omits_node_name():
    cfg_name = "very_long_kvtest_configuration_name_" * 3
    pod_name = "mc-run-reader-7c9d8f6d4b-abcde"
    base = bm._kvtest_artifact_base(cfg_name, 123, 9201, "1024MB")
    assert len(base) < 50
    assert cfg_name not in base
    assert pod_name not in base
    assert base.endswith(".i123_p9201.kvtest_1024MB")


# ------------------------------------------------------ binary_local push

def test_kvtest_deploy_binary_local(tmp_path, monkeypatch):
    cluster = {"nodes": {"n1": {"ip": "10.0.0.1"}, "n2": {"ip": "10.0.0.2"}}}
    binary = tmp_path / "mooncake_kvtest"
    binary.write_bytes(b"ELF")
    calls = []

    def fake_run_ssh(host, node, cmd, dry_run=False):
        calls.append(("ssh", node["ip"], cmd))

    def fake_scp_push(host, node, files, path, dry_run=False):
        calls.append(("scp", node["ip"], files[0], path))

    monkeypatch.setattr(bm, "run_ssh", fake_run_ssh)
    monkeypatch.setattr(bm, "scp_push", fake_scp_push)

    # unset -> no push, no remote path
    assert bm._kvtest_deploy_binary(
        cluster, ["n1"], "", "/ws/scripts", None, False, False) == ""
    assert calls == []

    # missing local file -> hard error before any node is touched
    try:
        bm._kvtest_deploy_binary(
            cluster, ["n1"], str(tmp_path / "nope"), "/ws/scripts",
            None, False, False)
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "binary_local" in str(exc)
    assert calls == []

    # ssh mode: lands in the writable scripts dir
    path = bm._kvtest_deploy_binary(
        cluster, ["n1", "n2"], str(binary), "/ws/scripts", None, False, False)
    assert path == "/ws/scripts/mooncake_kvtest"
    assert len(calls) == 6            # mkdir + scp + chmod per node
    assert calls[0] == ("ssh", "10.0.0.1", "mkdir -p /ws/scripts")
    assert calls[1] == ("scp", "10.0.0.1", str(binary), "/ws/scripts")
    assert calls[2][2].endswith("chmod +x /ws/scripts/mooncake_kvtest")
    assert calls[3] == ("ssh", "10.0.0.2", "mkdir -p /ws/scripts")
    assert calls[4] == ("scp", "10.0.0.2", str(binary), "/ws/scripts")

    # k8s mode: scripts/ is a read-only ConfigMap mount -> kvtest-bin/
    # under the writable pod workspace instead
    calls.clear()
    path = bm._kvtest_deploy_binary(
        cluster, ["n1"], str(binary), "/pod/ws/scripts", "/pod/ws", True, False)
    assert path == "/pod/ws/kvtest-bin/mooncake_kvtest"
    assert ("ssh", "10.0.0.1", "mkdir -p /pod/ws/kvtest-bin") in calls
    assert ("scp", "10.0.0.1", str(binary), "/pod/ws/kvtest-bin") in calls


def test_kvtest_deploy_binary_local_includes_sibling_libraries(
        tmp_path, monkeypatch):
    cluster = {"nodes": {"n1": {"ip": "10.0.0.1"}}}
    binary_dir = tmp_path / "outputs" / "bin"
    library_dir = tmp_path / "outputs" / "lib"
    binary_dir.mkdir(parents=True)
    library_dir.mkdir(parents=True)
    binary = binary_dir / "mooncake_kvtest"
    binary.write_bytes(b"ELF")
    asio = library_dir / "libasio.so"
    asio.write_bytes(b"SO")
    calls = []

    monkeypatch.setattr(
        bm, "run_ssh",
        lambda host, node, cmd, dry_run=False:
            calls.append(("ssh", node["ip"], cmd)))
    monkeypatch.setattr(
        bm, "scp_push",
        lambda host, node, files, path, dry_run=False:
            calls.append(("scp", node["ip"], list(files), path)))

    path = bm._kvtest_deploy_binary(
        cluster, ["n1"], str(binary), "/ws/scripts", None, False, False)
    assert path == "/ws/scripts/mooncake_kvtest"
    assert ("ssh", "10.0.0.1", "mkdir -p /ws/scripts/lib") in calls
    assert ("scp", "10.0.0.1", [str(asio)], "/ws/scripts/lib") in calls


# ------------------------------------------------------ CSV summary rows

def _summary(iid, up_total, get_total, verify):
    mb = 8 * 1024 * 1024
    return {
        "instance_id": iid,
        "elapsed_seconds": 30.0,
        "verify_failures": verify,
        "operations": {
            "upsert_from": {"count": up_total, "avg_ms": 1.0, "min_ms": 0.1,
                            "p50_ms": 0.8, "p90_ms": 1.5, "p99_ms": 3.0,
                            "p999_ms": 5.0, "p9999_ms": 7.0, "max_ms": 9.0,
                            "total": up_total, "success": up_total,
                            "failure": 0, "bytes": up_total * mb},
            "get_buffer": {"count": get_total, "avg_ms": 2.0, "min_ms": 0.2,
                           "p50_ms": 1.6, "p90_ms": 3.0, "p99_ms": 6.0,
                           "p999_ms": 9.0, "p9999_ms": 11.0, "max_ms": 13.0,
                           "total": get_total, "success": get_total,
                           "failure": 0, "bytes": get_total * mb},
            "notify_rpc": {"count": up_total * 10, "avg_ms": 1.5,
                            "max_ms": 9.0, "total": up_total * 10,
                            "success": up_total * 10 - 10, "failure": 10,
                            "bytes": 0},
            "notify_queue_wait": {"count": get_total, "avg_ms": 0.2,
                                  "max_ms": 1.1, "total": get_total,
                                  "success": get_total, "failure": 0,
                                  "bytes": 0},
        },
    }


def test_kvtest_csv_rows_from_dual_summaries(tmp_path):
    local_logs = tmp_path / "logs"
    local_logs.mkdir()
    # dual instances emit BOTH the writer op and the reader op
    cases = [("nodeA", 0, 9301, 1500, 1490, 2),
             ("nodeB", 1, 9302, 1300, 1290, 1)]
    for node, iid, port, up, get, verify in cases:
        fn = "kvtest_csv.{}_i{}_p{}.kvtest_8MB.summary.json".format(
            node, iid, port)
        summary = _summary(iid, up, get, verify)
        if iid == 1:
            summary["operations"]["upsert_from"]["avg_ms"] = 3.0
            summary["operations"]["upsert_from"]["max_ms"] = 19.0
        (local_logs / fn).write_text(json.dumps(summary), encoding="utf-8")
    cfg = _kv_cfg()
    # run-time records the auto pool + real client counts
    cfg["_kvtest_key_pool"] = {"8MB": 256}
    cfg["_writer_count"] = 2
    cfg["_reader_count"] = 2
    cfg_path = os.path.join(str(tmp_path), "kvtest_csv.json")
    rows = bm.write_scenario_csv(
        str(local_logs), cfg, "ts", cfg_path, cluster=None,
        mooncake_store_nodes=[], result_dir=str(tmp_path))
    # order: writer i0, writer i1, writer TOTAL, reader i0, reader i1,
    # reader TOTAL
    assert [(r["role"], r["instance"]) for r in rows] == [
        ("writer", "i0"), ("writer", "i1"), ("writer", "TOTAL"),
        ("reader", "i0"), ("reader", "i1"), ("reader", "TOTAL")]
    w0, w1, wT, r0, r1, rT = rows
    # per-instance writer row
    assert w0["operation"] == "upsert_from" and w0["total"] == 1500
    assert w0["total_bytes"] == 1500 * 8 * 1024 * 1024
    assert w0["qps"] == 50.0                     # 1500 / 30s
    assert w0["notify_submitted"] == 15000 and w0["notify_fail"] == 10
    assert w0["key_pool_size"] == 256
    assert w0["num_threads"] == 1 and w0["notify_threads"] == 10
    assert w0["target_qps"] == 105
    # 无原始样本且无 controller reduce 产物：计数/阈值求和与 min/max/加权均值
    # 仍精确（均值加权 = 全样本均值），分位数一律置空（N/A）——绝不使用
    # “分位数加权平均”这类近似。
    assert wT["total"] == 2800 and wT["success"] == 2800
    assert wT["notify_submitted"] == 28000 and wT["notify_fail"] == 20
    assert wT["notify_avg_ms"] == 1.5 and wT["notify_max_ms"] == 9.0
    assert wT["avg_ms"] == round((1500 * 1.0 + 1300 * 3.0) / 2800, 4)
    for _k in ("p50_ms", "p90_ms", "p95_ms", "p99_ms", "p999_ms",
               "p9999_ms"):
        assert wT[_k] == ""                     # N/A，绝不近似
    assert wT["max_ms"] == 19.0                  # true max, never weighted
    assert wT["throughput_mb_s"] == round(2800 * 8 / 30, 2)   # MB per second
    # reader rows: queue wait + verify failures (read-path verification)
    assert r0["operation"] == "get_buffer" and r0["verify_failures"] == 2
    assert r0["queue_wait_avg_ms"] == 0.2 and r0["queue_wait_max_ms"] == 1.1
    assert rT["total"] == 2780 and rT["verify_failures"] == 3
    assert rT["queue_wait_avg_ms"] == 0.2
    # per-config csv written with the kvtest header
    with open(os.path.join(str(tmp_path), "kvtest_csv.csv"),
              encoding="utf-8") as f:
        assert next(csv.reader(f)) == bm.KVTEST_CSV_HEADER
    # rows carry exactly the header keys the detail csv expects
    for r in rows:
        assert set(r.keys()) <= set(bm.KVTEST_CSV_HEADER)


def test_kvtest_qps_is_measured_success_qps(tmp_path):
    local_logs = tmp_path / "logs"
    local_logs.mkdir()
    summary = _summary(0, 100, 0, 0)
    summary["operations"]["upsert_from"].update(
        {"total": 100, "success": 90, "failure": 10})
    (local_logs / "kvtest_csv.nodeA_i0_p9301.kvtest_8MB.summary.json") \
        .write_text(json.dumps(summary), encoding="utf-8")
    rows = bm.write_scenario_csv(
        str(local_logs), _kv_cfg(), "ts",
        os.path.join(str(tmp_path), "kvtest_csv.json"), cluster=None,
        mooncake_store_nodes=[], result_dir=str(tmp_path))
    writer, total = [r for r in rows if r["role"] == "writer"]
    assert writer["qps"] == 3.0  # 90 successful / 30s
    assert total["qps"] == 3.0


def test_kvtest_total_latency_recomputed_from_merged_ok_samples(tmp_path):
    local_logs = tmp_path / "logs"
    local_logs.mkdir()
    cases = [("nodeA", 0, 9301, [1.0, 2.0]),
             ("nodeB", 1, 9302, [100.0])]
    for node, iid, port, values in cases:
        base = bm._kvtest_artifact_base("kvtest_csv", iid, port, "8MB")
        summary = _summary(iid, len(values), 0, 0)
        summary["operations"].pop("get_buffer")
        summary["operations"].pop("notify_queue_wait")
        summary["operations"]["notify_rpc"].update(
            {"total": 0, "success": 0, "failure": 0, "count": 0})
        # Deliberately wrong summary latency fields: raw OK samples must win.
        summary["operations"]["upsert_from"].update(
            {"avg_ms": -1, "p50_ms": -1, "p90_ms": -1,
             "p99_ms": -1, "p999_ms": -1, "p9999_ms": -1,
             "max_ms": -1})
        (local_logs / (base + ".summary.json")).write_text(
            json.dumps(summary), encoding="utf-8")
        rows = [
            "elapsed_us,instance_id,operation,request_index,latency_ms,status,bytes"
        ]
        for index, value in enumerate(values):
            rows.append("{}, {},upsert_from,{},{},OK,8388608".format(
                index + 1, iid, index, value).replace(", ", ","))
        # Failed requests are retained in the raw file but excluded from all
        # latency distribution statistics.
        rows.append("99,{},upsert_from,99,9999,ERR,0".format(iid))
        (local_logs / (base + ".samples.csv")).write_text(
            "\n".join(rows) + "\n", encoding="utf-8")

    cfg = _kv_cfg()
    cfg["_kvtest_iid_nodes"] = {"0": "nodeA", "1": "nodeB"}
    result = bm.write_scenario_csv(
        str(local_logs), cfg, "ts",
        os.path.join(str(tmp_path), "kvtest_csv.json"), cluster=None,
        mooncake_store_nodes=[], result_dir=str(tmp_path))
    w0, w1, total = [r for r in result if r["role"] == "writer"]
    assert w0["avg_ms"] == 1.5 and w0["max_ms"] == 2.0
    assert w1["avg_ms"] == 100.0 and w1["max_ms"] == 100.0
    assert total["avg_ms"] == 34.3333
    assert total["min_ms"] == 1.0
    assert total["max_ms"] == 100.0
    # 统一口径：TOTAL 分位 = 实例行同源合并（直方图插值；本环境无 sketch）。
    # 小样本（3 条）下尾部按桶内秩插值，单调且不超过真实 max。
    assert 1.0 <= total["p50_ms"] <= 2.5
    assert (total["p50_ms"] <= total["p95_ms"] <= total["p99_ms"]
            <= total["p999_ms"] <= total["p9999_ms"] <= total["max_ms"])
    # Strictly greater: the 2ms sample is not part of >2000us.
    assert total[">2000us"] == 1
    assert total[">20000us"] == 1


def test_kvtest_csv_skips_bad_summary_json(tmp_path, capsys):
    local_logs = tmp_path / "logs"
    local_logs.mkdir()
    (local_logs / "kvtest_csv.nodeA_i0_p9301.kvtest_8MB.summary.json") \
        .write_text("{not json", encoding="utf-8")
    good = _summary(1, 100, 100, 0)
    (local_logs / "kvtest_csv.nodeB_i1_p9302.kvtest_8MB.summary.json") \
        .write_text(json.dumps(good), encoding="utf-8")
    cfg = _kv_cfg()
    cfg["_kvtest_key_pool"] = {"8MB": 256}
    rows = bm.write_scenario_csv(
        str(local_logs), cfg, "ts", os.path.join(str(tmp_path), "kvtest_csv.json"),
        cluster=None, mooncake_store_nodes=[], result_dir=str(tmp_path))
    # only the healthy instance produced rows (grouped: writers + TOTAL,
    # then readers + TOTAL)
    assert [(r["role"], r["instance"]) for r in rows] == [
        ("writer", "i1"), ("writer", "TOTAL"),
        ("reader", "i1"), ("reader", "TOTAL")]
    assert "(skipped)" in capsys.readouterr().out


# -------------------------------------------------- rank+recall parallel

def _smoke_path(name):
    return os.path.join(ROOT, "configs", "kvtest", "smoke", name)


def test_kvtest_parallel_smoke_partition():
    # the rank_recall launcher (and the WebUI pre-launch validation)
    # partitions by work_mode: the two kvtest smoke files mirror
    # rank_smoke/recall_smoke and must land in the expected groups while
    # still inheriting scenario=kvtest
    from scripts.run_rank_recall import partition_configs
    groups = partition_configs(
        [], [_smoke_path("kvtest_rank_smoke.json"),
             _smoke_path("kvtest_recall_smoke.json")])
    assert [os.path.basename(p) for p in groups["rank"]] == \
        ["kvtest_rank_smoke.json"]
    assert [os.path.basename(p) for p in groups["recall"]] == \
        ["kvtest_recall_smoke.json"]
    for p in groups["rank"] + groups["recall"]:
        assert bm.load_scenario_config(p)["scenario"] == "kvtest"


def test_kvtest_parallel_smoke_plan_shapes():
    # Keep the original pod counts (rank 2/node, recall 6/node), but run one
    # writer plus one reader process inside each pod.
    rank = bm.load_scenario_config(_smoke_path("kvtest_rank_smoke.json"))
    recall = bm.load_scenario_config(_smoke_path("kvtest_recall_smoke.json"))
    assert rank["work_mode"] == "rank" and recall["work_mode"] == "recall"
    plan = bm.kvtest_plan(rank, node_count=6)
    assert plan["split_pod"] and len(plan["clients"]) == 12
    assert len(plan["writers"]) == len(plan["readers"]) == 12
    assert plan["notify_count"] == 10                    # <= 12 readers
    assert plan["per_node_qps"] == 105 and plan["total_qps"] == 105 * 6
    assert plan["writer_threads"] == 4
    assert plan["reader_threads"] == 12
    assert plan["writer_port_base"] == 9101
    assert plan["reader_port_base"] == 9201
    assert rank["kvtest"]["local_port_base"] == 12345
    plan = bm.kvtest_plan(recall, node_count=6)
    assert plan["split_pod"] and len(plan["clients"]) == 36
    assert len(plan["writers"]) == len(plan["readers"]) == 36
    assert plan["notify_count"] == 1
    assert plan["per_node_qps"] == 35 and plan["total_qps"] == 35 * 6
    assert plan["writer_threads"] == 4
    assert plan["reader_threads"] == 12
    assert plan["writer_port_base"] == 9301
    assert plan["reader_port_base"] == 9401
    assert recall["kvtest"]["local_port_base"] == 13345


# -------------------------------------------------- loader / webui wiring

def test_kvtest_loader_inherits_fragment():
    # the shipped smoke scenario sits under configs/kvtest/ and must pick up
    # the category root defaults (scenario/protocol/duration/kvtest block)
    path = os.path.join(ROOT, "configs", "kvtest", "smoke", "kvtest_smoke.json")
    if not os.path.isfile(path):        # tolerate trimmed checkouts
        import pytest
        pytest.skip("configs/kvtest/smoke/kvtest_smoke.json not present")
    merged = bm.load_scenario_config(path)
    assert merged["scenario"] == "kvtest"
    # smoke overrides key_pool_size to 256 over the root default "auto"
    assert merged["kvtest"]["key_pool_size"] == 256
    assert merged["kvtest"]["notify_queue_max"] == 65536  # YuanRong baseline
    plan = bm.kvtest_plan(merged, node_count=4)
    assert plan["split_pod"] is True
    assert len(plan["clients"]) == 4
    assert len(plan["writers"]) == len(plan["readers"]) == 4
    assert plan["notify_count"] == 1


def test_webui_validate_and_inventory():
    from webui.backend.routers import configs as cfg_router
    # valid kvtest content passes the kvtest_plan shape validation
    smoke = json.load(open(os.path.join(
        ROOT, "configs", "kvtest", "smoke", "kvtest_smoke.json"),
        encoding="utf-8"))
    errs = cfg_router.validate_config_content(
        os.path.join(bm.CONFIGS_DIR, "kvtest", "smoke", "zz-test.json"),
        json.dumps(smoke))
    assert errs == []
    # protocol outside tcp/rdma/ub is rejected
    bad = dict(smoke, protocol="xxx")
    errs = cfg_router.validate_config_content(
        os.path.join(bm.CONFIGS_DIR, "kvtest", "smoke", "zz-test.json"),
        json.dumps(bad))
    assert any("protocol" in e for e in errs)
    # the config inventory exposes the kvtest category tree
    inv = cfg_router.api_configs(None, {})
    assert "kvtest" in inv["categories"]
    assert inv["categories"]["kvtest"]["config_count"] >= 2
