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

def test_kvtest_plan_split_processes_from_client_pods():
    plan = bm.kvtest_plan(_kv_cfg(), node_count=2)
    # a client block ALWAYS means split: one writer + one reader process per
    # client pod; the legacy dual process is gone.
    assert plan["dual"] is False and plan["split_pod"] is True
    assert "clients" in plan and len(plan["clients"]) == 4
    assert len(plan["writers"]) == 4 and len(plan["readers"]) == 4
    assert [w["iid"] for w in plan["writers"]] == [0, 1, 2, 3]
    assert [r["iid"] for r in plan["readers"]] == [4, 5, 6, 7]
    assert [w["pair_index"] for w in plan["writers"]] == [0, 1, 2, 3]
    assert [r["pair_index"] for r in plan["readers"]] == [0, 1, 2, 3]
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


def test_kvtest_plan_ignores_removed_process_mode():
    cfg = _kv_cfg()
    cfg["client"]["process_mode"] = "invalid"      # removed knob: WARN only
    plan = bm.kvtest_plan(cfg, node_count=2)
    assert plan["split_pod"] is True and plan["process_mode"] == "split"

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
    entry = plan["writers"][0]
    entry["_qps"] = 53
    entry["_out_dir"] = "/remote/kvtest_out/i0"
    entry["_lhost_port"] = 12345
    entry["_cpu_affinity"] = "0-3"
    all_instances = (
        [{"iid": c["iid"], "port": c["port"], "role": "writer",
          "host": "10.1.0.{}".format(c["local"])}
         for c in plan["writers"]]
        + [{"iid": c["iid"], "port": c["port"], "role": "reader",
            "host": "10.1.0.{}".format(c["local"])}
           for c in plan["readers"]])
    inst = bm.build_kvtest_instance_config(
        _kv_cfg(), {"local_buffer_size": 8589934592,
                    "tenant_id": "default"},
        {"SETUP_MASTER_ADDR": "10.0.0.10:50051",
         "SETUP_METADATA_SERVER": ""},
        plan, entry, "writer", "8MB", 256, all_instances)
    assert inst["role"] == "writer" and inst["instance_id"] == entry["iid"]
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
    # nodes view: the whole cluster as kvtest peers (writer + reader pairs)
    assert [n["instance_id"] for n in inst["nodes"]] == [0, 1, 2, 3]
    assert [n["role"] for n in inst["nodes"]] == [
        "writer", "writer", "reader", "reader"]
    assert all(n["notify_rpc_port"] == n["port"] + 10000
               for n in inst["nodes"])


def test_build_kvtest_instance_config_etcd_master():
    plan = bm.kvtest_plan(_kv_cfg(), node_count=1)
    entry = plan["writers"][0]
    entry["_qps"] = 52
    entry["_out_dir"] = "/remote/kvtest_out/i0"
    entry["_lhost_port"] = 12346
    inst = bm.build_kvtest_instance_config(
        _kv_cfg(), {"master_type": "etcd"},
        {"SETUP_ETCD_ENDPOINTS": "http://10.0.0.10:2379"},
        plan, entry, "writer", "8MB", 128, [])
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


def test_kvtest_pod_map_runs_in_process_dir(monkeypatch):
    commands = []
    monkeypatch.setattr(
        bm, "get_node", lambda cluster, name: {"ip": "10.0.0.1",
                                               "_pod": "pod-a"})

    def fake_ssh(ip, node, command, quiet=False):
        commands.append(command)
        return "OK" if "test -s" in command else "[logstats] ok"

    monkeypatch.setattr(bm, "ssh_check_output", fake_ssh)
    entry = {"node": "n1", "iid": 3, "_role": "writer", "_ds": "8MB",
             "_log_base": "/pod/writer_i3_p9101/case",
             "_out_dir": "/pod/writer_i3_p9101/case_out",
             "_inst_dir": "/pod/writer_i3_p9101"}
    bm._kvtest_pod_map({"nodes": {}}, entry, "/scripts", {},
                       start_epoch=1000.0, warmup_s=2.0, digest_k=24)
    assert "--kvtest-map-instance" in commands[0]
    assert "--instance-dir /pod/writer_i3_p9101" in commands[0]
    assert "--warmup-s 2.0" in commands[0]
    assert "--digest-k 24" in commands[0]
    assert "--out-stats /pod/writer_i3_p9101/case.stats.json" in commands[0]
    # strict: a missing product is an error (no fallback map)
    monkeypatch.setattr(bm, "ssh_check_output",
                        lambda *a, **k: "MISSING")
    try:
        bm._kvtest_pod_map({"nodes": {}}, entry, "/scripts", {},
                           start_epoch=1000.0)
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "products missing" in str(exc)


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
