"""pipeline etcd-rendezvous orchestration tests (plan/barrier/loader/wipe)."""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                                "scripts"))

import importlib.util

_SPEC = importlib.util.spec_from_file_location(
    "mock_etcd_server",
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                 "mock", "etcd_server.py"))
_ME = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_ME)

import benchmark as bm
import bench_etcd as et

import pytest


def test_pipeline_plan_cluster_oriented():
    cfg = {"writer": {"count_per_node": 2}, "reader": {"count_per_node": 3},
           "notify_count": 4}
    plan = bm.pipeline_plan(cfg, node_count=3)
    assert len(plan["writers"]) == 6          # 2 per node x 3 nodes
    assert len(plan["readers"]) == 9          # 3 per node x 3 nodes
    # global gids/indices in node order
    assert [w["gid"] for w in plan["writers"]] == [1, 2, 3, 4, 5, 6]
    assert [r["index"] for r in plan["readers"]] == list(range(9))
    # per-node port numbering (base defaults 9101/9201)
    assert [w["port"] for w in plan["writers"]] == [9101, 9102] * 3
    assert [r["port"] for r in plan["readers"]] == [9201, 9202, 9203] * 3
    # slots: process i of node j -> slot j
    assert plan["writers"][5]["slot"] == 2 and plan["writers"][5]["local"] == 1


def test_pipeline_plan_port_bases_configurable():
    cfg = {"writer": {"count_per_node": 2, "port_base": 7001},
           "reader": {"count_per_node": 1, "port_base": 8001}}
    plan = bm.pipeline_plan(cfg, node_count=1)
    assert [w["port"] for w in plan["writers"]] == [7001, 7002]
    assert [r["port"] for r in plan["readers"]] == [8001]
    assert plan["writer_port_base"] == 7001
    assert plan["reader_port_base"] == 8001


def test_pipeline_plan_qps_per_node_semantics():
    # qps is PER-NODE: total = base x multiple x N; each client of a node
    # shares that node's rate equally
    cfg = {"base_qps": 105, "qps_multiple": 2,
           "writer": {"count_per_node": 1}, "reader": {"count_per_node": 1}}
    plan = bm.pipeline_plan(cfg, node_count=2)
    assert plan["per_node_qps"] == 210
    assert plan["total_qps"] == 420          # 210 per node x 2 nodes
    assert plan["per_writer_qps"] == 210     # 1 writer per node -> whole rate
    cfg = {"base_qps": 105, "qps_multiple": 1,
           "writer": {"count_per_node": 2}, "reader": {"count_per_node": 1}}
    plan = bm.pipeline_plan(cfg, node_count=3)
    assert plan["total_qps"] == 315
    assert plan["per_writer_qps"] == 52.5    # 105 per node / 2 writers


def test_pipeline_plan_dual_client_mode():
    cfg = {"base_qps": 105, "qps_multiple": 1, "notify_count": 10,
           "client": {"count_per_node": 2, "port_base": 9301},
           "writer": {"operation": "put_batch", "notify_threads": 10},
           "reader": {"operation": "get", "num_threads": 1}}
    plan = bm.pipeline_plan(cfg, node_count=6)
    assert plan["dual"] is True
    assert len(plan["clients"]) == 12          # 2 per node x 6 nodes
    assert [c["id"] for c in plan["clients"]] == list(range(12))
    # per-node port numbering; k8s overrides all to port_base at
    # materialization time (every pod has its own netns)
    assert [c["port"] for c in plan["clients"]] == [9301, 9302] * 6
    assert plan["per_node_qps"] == 105
    assert plan["per_writer_qps"] == 52.5      # 105 / 2 clients per node
    assert plan["writers"] is plan["clients"] and plan["readers"] is plan["clients"]


def test_loader_strips_client_nodes(tmp_path, capsys):
    cfg_file = tmp_path / "p.json"
    cfg_file.write_text(json.dumps({
        "scenario": "pipeline", "duration": 10,
        "writer": {"count_per_node": 1, "client_nodes": ["a"]},
        "reader": {"count_per_node": 1, "client_nodes": ["b"]},
    }), encoding="utf-8")
    merged = bm.load_scenario_config(str(cfg_file))
    assert "client_nodes" not in merged["writer"]
    assert "client_nodes" not in merged["reader"]
    assert "client_nodes is deprecated" in capsys.readouterr().out


def test_pipeline_requires_etcd_endpoints():
    try:
        bm._pipeline_etcd_endpoints({"etcd_endpoints": ""})
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "etcd_endpoints" in str(exc)
    assert bm._pipeline_etcd_endpoints(
        {"etcd_endpoints": "etcd://a:2379"}) == "etcd://a:2379"


@pytest.fixture(scope="module")
def endpoints():
    srv = _ME.serve(0)
    import threading
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    yield "etcd://127.0.0.1:{}".format(srv.server_address[1])
    srv.shutdown()


def test_pipeline_barrier_success_and_timeout(endpoints, monkeypatch):
    et.kv_put(endpoints, "mcbench/t1/readers/0", "a")
    et.kv_put(endpoints, "mcbench/t1/readers/1", "b")
    kvs = bm._pipeline_barrier(endpoints, "mcbench/t1/readers/", 2, 5,
                               "readers", "t1")
    assert len(kvs) == 2
    # timeout path: expect 5, only 2 present; shrink poll so the test is fast
    monkeypatch.setattr(bm.time, "sleep", lambda s: None)
    try:
        bm._pipeline_barrier(endpoints, "mcbench/t1/readers/", 5, 0.01,
                             "readers", "t1")
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "2/5 readers" in str(exc)


def test_pipeline_wipe_etcd(endpoints):
    et.kv_put(endpoints, "mcbench/x/readers/0", "a")
    et.kv_put(endpoints, "mcbench/y/writers/1", "b")
    bm._pipeline_wipe_etcd({"etcd_endpoints": endpoints})
    assert et.kv_range(endpoints, "mcbench/") == {}
    # no etcd configured -> silent no-op
    bm._pipeline_wipe_etcd({"etcd_endpoints": ""})


def test_run_pipeline_rejects_without_nodes(monkeypatch):
    # ssh cluster with an empty node set -> clear error before any work
    cfg = {"scenario": "pipeline", "duration": 5, "writer": {}, "reader": {}}
    args = type("A", (), {"dry_run": True, "no_warmup": False})()
    cluster = {"nodes": {}}
    glb = {"etcd_endpoints": "etcd://127.0.0.1:1", "numa_bind": False}
    monkeypatch.setattr(bm, "_k8s_pipeline_nodes",
                        lambda c, d=False: [])
    monkeypatch.setattr(bm, "apply_cli_overrides", lambda cfg, args: None)
    try:
        bm.run_pipeline_scenario(cluster, glb, dict(cfg), "ts", args)
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "non-empty cluster node set" in str(exc)
