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


def test_service_discovery_all_endpoints_without_mutating_workers(monkeypatch):
    cluster = {"_controller": {}, "nodes": {"bench": {"ip": "1"}}}
    calls = []

    def run(controller, args, check=False):
        import json
        calls.append(args)
        pods = ["master-0"] if args[4] == "mc-master" else ["store-0", "store-1"]
        return json.dumps({"subsets": [{"addresses": [
            {"ip": "10.0.0.1", "targetRef": {"kind": "Pod", "name": pod}}
            for pod in pods]}]}), 0

    monkeypatch.setattr(bm, "controller_run", run)
    nodes = bm._spdiag_service_nodes(
        cluster, {"master_addr": "mc-master.prod.svc.cluster.local:50051"},
        {"tools": {"spdiag": {"enabled": True}}})
    assert len(nodes) == 3
    assert {n["_pod"] for n in nodes.values()} == {"master-0", "store-0", "store-1"}
    assert all(n["_ns"] == "prod" for n in nodes.values())
    assert set(cluster["nodes"]) == {"bench"}
    assert calls[1][4] == "mooncake-store"


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
