"""kvtest scenario tests: plan/iid/qps-split/instance-config/CSV summary.

The C++ mooncake_kvtest binary records summary.json operation keys exactly
as the config strings we ship (metric_name is the pre-normalization name),
so the aggregation matches on "upsert_from"/"get_buffer"/"notify_http"/
"notify_queue_wait".
"""
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


def test_kvtest_split_qps_matches_binary_rate_gate():
    # 105 across 2 instances: 53 + 52 (the binary's own thread split)
    assert [bm._kvtest_split_qps(105, 2, i) for i in range(2)] == [53, 52]
    # exact division and single instance
    assert [bm._kvtest_split_qps(104, 2, i) for i in range(2)] == [52, 52]
    assert bm._kvtest_split_qps(60, 1, 0) == 60


# ------------------------------------------------- per-instance config build

def test_build_kvtest_instance_config_http_master():
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
    assert inst["key_pool_size"] == 256
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


def test_kvtest_store_targets_require_endpoints():
    try:
        bm._kvtest_store_targets({"master_type": "etcd"}, {})
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "etcd_endpoints" in str(exc)


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


# ------------------------------------------------------ CSV summary rows

def _summary(iid, up_total, get_total, verify):
    mb = 8 * 1024 * 1024
    return {
        "instance_id": iid,
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
            "notify_http": {"count": up_total * 10, "avg_ms": 1.5,
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
        (local_logs / fn).write_text(
            json.dumps(_summary(iid, up, get, verify)), encoding="utf-8")
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
    # TOTAL rows: counts sum exactly, percentiles average across instances
    assert wT["total"] == 2800 and wT["success"] == 2800
    assert wT["notify_submitted"] == 28000 and wT["notify_fail"] == 20
    assert wT["notify_avg_ms"] == 1.5 and wT["notify_max_ms"] == 9.0
    assert wT["p50_ms"] == 0.8                   # identical inputs -> same
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
    # smoke overrides key_pool_size to 256 over the root default 0
    assert merged["kvtest"]["key_pool_size"] == 256
    assert merged["kvtest"]["notify_queue_max"] == 8192   # from _configs.json


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
