"""Regression coverage for the kvtest profiling lifecycle port from main."""
import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
import benchmark as bm


def test_kvtest_profiling_lifecycle():
    # Adapted from main's lifecycle guard: both triggers and every exit path.
    src = inspect.getsource(bm.run_kvtest_scenario)
    assert 'matrix = getattr(args, "_matrix", None)' in src
    assert src.count("prof_core.start_profiling(") == 2
    assert 'matrix.get("trigger", "whole_run") == "whole_run"' in src
    assert src.index("prof_core.start_profiling(") < src.index('if plan.get("dual"):', src.index("lhost_counter = {}"))
    stop = src.split("except StopRequested:", 1)[1]
    assert "prof_core.stop_and_collect(" in stop.split("except Exception:", 1)[0]
    failure = stop.split("except Exception:", 1)[1]
    assert failure.index("prof_core.stop_and_collect(") < failure.index("reap_state_entries(cluster)")
    assert src.count("prof_core.stop_and_collect(") == 3


def test_spdiag_restart_and_show_are_collected_per_pod(monkeypatch, tmp_path):
    core = bm.prof_core
    commands, pulls = [], []
    node = {"ip": "10.0.0.1"}
    cluster = {"nodes": {"bench-pod": node}}
    matrix = {"nodes": "auto", "tools": {
        "spdiag": {"enabled": True, "bin": "spdiag", "sudo": False}}}

    def run(ip, target, cmd, quiet=True):
        commands.append(cmd)
        return "OK" if "command -v" in cmd else ""

    monkeypatch.setattr(core, "_run", run)
    monkeypatch.setattr(core, "scp_pull", lambda ip, target, src, dst:
                        pulls.append((src, dst)))
    monkeypatch.setattr(core, "spdiag_tree_mod", None)
    monkeypatch.setattr(bm, "state_remove", lambda rows: None)
    session = core.start_profiling(
        cluster, matrix, ["bench-pod"], "kvtest_rank_smoke", "/remote/profiling")
    assert session["tools"] == {"bench-pod": {"spdiag": None}}
    core.stop_and_collect(cluster, session, str(tmp_path))
    assert "spdiag restart" in commands
    assert "spdiag show > /remote/profiling/spdiag.show.txt" in commands
    assert pulls == [("/remote/profiling/spdiag.show.txt",
                      os.path.join(str(tmp_path), "bench-pod"))]


def test_service_discovery_by_namespace_and_role_without_mutating_workers(monkeypatch):
    cluster = {"_controller": {}, "nodes": {"bench": {"ip": "1"}}}
    calls = []

    def run(controller, args, check=False):
        import json
        calls.append(args)
        assert args[2:5] == ["get", "pods", "-l"]
        pods = ["master-0"] if args[5] == "role=master" else ["store-0", "store-1"]
        items = [{"metadata": {"name": pod}, "status": {
            "podIP": "10.0.0.1", "phase": "Running",
            "conditions": [{"type": "Ready", "status": "True"}]}}
            for pod in pods]
        # Pending, unready and terminating pods must not be selected.
        items += [
            {"metadata": {"name": "pending"}, "status": {"phase": "Pending"}},
            {"metadata": {"name": "unready"}, "status": {
                "phase": "Running", "podIP": "10.0.0.2"}},
            {"metadata": {"name": "terminating", "deletionTimestamp": "now"},
             "status": {"phase": "Running", "podIP": "10.0.0.3",
                        "conditions": [{"type": "Ready", "status": "True"}]}}]
        return json.dumps({"items": items}), 0

    monkeypatch.setattr(bm, "controller_run", run)
    nodes = bm._spdiag_service_nodes(
        cluster, {"master_addr": "mc-master.prod.svc.cluster.local:50051"},
        {"tools": {"spdiag": {"enabled": True}}})
    assert len(nodes) == 3
    assert {n["_pod"] for n in nodes.values()} == {"master-0", "store-0", "store-1"}
    assert all(n["_ns"] == "prod" for n in nodes.values())
    assert set(cluster["nodes"]) == {"bench"}
    assert calls[1][5] == "role=store"
    # etcd mode has no RPC master_addr; discover from the metrics Service DNS.
    etcd_nodes = bm._spdiag_service_nodes(
        cluster, {"master_type": "etcd", "master_addr": "",
                  "master_metrics_url": "http://mc-master.prod.svc.cluster.local:17702/metrics"},
        {"tools": {"spdiag": {"enabled": True}}})
    assert set(etcd_nodes) == set(nodes)
    # Explicit namespace works even when no master DNS is configured.
    explicit_nodes = bm._spdiag_service_nodes(
        cluster, {}, {"tools": {"spdiag": {"enabled": True, "namespace": "rank-prod"}}})
    assert len(explicit_nodes) == 3
    assert all(n["_ns"] == "rank-prod" for n in explicit_nodes.values())
    # rank+recall children each read their own global.json and share one profile.
    profile = {"tools": {"spdiag": {"enabled": True}}}
    for namespace in ("mooncake-rank", "mooncake-recall"):
        targets = bm._spdiag_service_nodes(
            cluster, {"master_type": "etcd", "master_addr": "",
                      "master_metrics_url":
                          "http://mooncake-master.{}.svc.cluster.local:17702/metrics".format(namespace)},
            profile)
        assert len(targets) == 3
        assert all(n["_ns"] == namespace for n in targets.values())
    assert "namespace" not in profile["tools"]["spdiag"]


def test_service_only_spdiag_is_collected_without_worker_registration(monkeypatch, tmp_path):
    core = bm.prof_core
    cluster = {"nodes": {"bench": {"ip": "client"}}}
    commands, pulls = [], []

    def run(ip, target, cmd, quiet=True):
        commands.append((ip, cmd))
        return "OK" if "command -v" in cmd else ""

    monkeypatch.setattr(core, "_run", run)
    monkeypatch.setattr(core, "scp_pull", lambda ip, target, src, dst:
                        pulls.append((ip, dst)))
    monkeypatch.setattr(core, "spdiag_tree_mod", None)
    monkeypatch.setattr(bm, "state_remove", lambda rows: None)
    session = core.start_profiling(
        cluster, {"tools": {"spdiag": {"enabled": True, "bin": "spdiag"},
                            "perf": {"enabled": True, "bin": "perf"}}},
        [], "kvtest", "/remote", spdiag_nodes={
            "service-prod-master-0": {"ip": "master"},
            "service-prod-store-0": {"ip": "store"}})
    core.stop_and_collect(cluster, session, str(tmp_path))
    assert len(pulls) == 2
    assert {ip for ip, _ in pulls} == {"master", "store"}
    assert all("perf" not in cmd for _, cmd in commands)
    assert sum(cmd == "spdiag restart" for _, cmd in commands) == 2
    assert set(cluster["nodes"]) == {"bench"}


def test_ssh_services_resolve_master_fabric_ip_and_store():
    cluster = {"nodes": {"master": {"ip": "m", "fabric_ip": "192.168.1.1"},
                         "store": {"ip": "s"}, "bench": {"ip": "b"}}}
    nodes = bm._spdiag_service_nodes(
        cluster, {"master_addr": "192.168.1.1:50051", "mooncake_store_nodes": ["store"]},
        {"tools": {"spdiag": {"enabled": True}}})
    assert set(nodes) == {"master", "store"}
