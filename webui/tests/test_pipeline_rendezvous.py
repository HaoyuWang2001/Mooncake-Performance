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
    kvs = bm._pipeline_barrier(None, endpoints, "mcbench/t1/readers/", 2, 5,
                               "readers", "t1")
    assert len(kvs) == 2
    # timeout path: expect 5, only 2 present; shrink poll so the test is fast
    monkeypatch.setattr(bm.time, "sleep", lambda s: None)
    try:
        bm._pipeline_barrier(None, endpoints, "mcbench/t1/readers/", 5, 0.01,
                             "readers", "t1")
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "2/5 readers" in str(exc)


def test_pipeline_wipe_etcd(endpoints):
    et.kv_put(endpoints, "mcbench/x/readers/0", "a")
    et.kv_put(endpoints, "mcbench/y/writers/1", "b")
    bm._pipeline_wipe_etcd(None, {"etcd_endpoints": endpoints})
    assert et.kv_range(endpoints, "mcbench/") == {}
    # no etcd configured -> silent no-op
    bm._pipeline_wipe_etcd(None, {"etcd_endpoints": ""})


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


def test_k8s_pipeline_nodes_jsonpath_has_no_raw_newline(monkeypatch):
    """kubectl must receive a LITERAL backslash-n in the jsonpath; a raw
    newline makes real kubectl fail with 'unterminated quoted string'."""
    captured = {}

    def fake_run(controller, args, check=True, timeout=None):
        captured["argv"] = list(args)
        return "node-a True\nnode-b True", 0

    monkeypatch.setattr(bm, "controller_run", fake_run)
    nodes = bm._k8s_pipeline_nodes({"_controller": {"ip": "1.2.3.4", "_ctx": ""}})
    assert nodes == ["node-a", "node-b"]
    jp = captured["argv"][captured["argv"].index("-o") + 1]
    assert "\\n" in jp, "jsonpath must carry a literal backslash-n"
    assert "\n" not in jp, "jsonpath must not carry a raw newline"


def test_etcd_range_via_controller_proxy(endpoints):
    """k8s mode: orchestrator-side etcd access goes through the controller
    (curl), never directly from the orchestrator host."""
    cluster = {"_controller": {"ip": "127.0.0.1", "_local": True,
                               "_ctx": ""}}
    et.kv_put(endpoints, "mcbench/px/readers/0", "a")
    et.kv_put(endpoints, "mcbench/px/readers/1", "b")
    # route through the controller-proxy layer (local controller -> curl)
    kvs = bm._etcd_range(cluster, endpoints, "mcbench/px/readers/")
    assert kvs == {"mcbench/px/readers/0": "a", "mcbench/px/readers/1": "b"}
    n = bm._etcd_delete_prefix(cluster, endpoints, "mcbench/px/")
    assert n == 2
    assert bm._etcd_range(cluster, endpoints, "mcbench/px/") == {}


def test_pipeline_k8s_roles_shape():
    """k8s deployment role list shape: each item must be an unpackable
    (role, entries, port_base) 3-tuple — dual yields ONE 'client' entry,
    separate yields reader+writer. Locks the regression where the dual
    branch returned the bare tuple and the for-loop unpacked its elements
    ('too many values to unpack (expected 3)')."""
    dual = bm.pipeline_plan(
        {"client": {"count_per_node": 2, "port_base": 9301},
         "writer": {}, "reader": {}}, node_count=2)
    roles = bm._pipeline_k8s_roles(dual)
    items = list(roles)
    assert len(items) == 1
    role, entries, base = items[0]           # must unpack cleanly
    assert role == "client" and base == 9301 and len(entries) == 4
    assert all(isinstance(i, tuple) and len(i) == 3 for i in items)

    sep = bm.pipeline_plan(
        {"writer": {"count_per_node": 1}, "reader": {"count_per_node": 2}},
        node_count=2)
    items = list(bm._pipeline_k8s_roles(sep))
    assert len(items) == 2
    assert [i[0] for i in items] == ["reader", "writer"]
    assert items[0][2] == 9201 and items[1][2] == 9101
    assert len(items[0][1]) == 4 and len(items[1][1]) == 2
    assert all(isinstance(i, tuple) and len(i) == 3 for i in items)


def test_render_deployment_yaml_structure():
    """Deployment template field placement: topologySpreadConstraints is a
    POD-spec field (must sit inside template.spec, 6-space indent — a real
    k8s server rejected it at Deployment.spec level) and the pod template
    must use restartPolicy: Always (Deployments reject Never)."""
    cluster = {
        "k8s": {"namespace": "mc-test", "image": "local/test:1",
                "controller_workspace": "/tmp/cw", "pod_workspace": "/tmp/pw"},
        "_controller": {"ip": "127.0.0.1", "_ctx": ""},
    }
    deploy, run_label, text = bm._render_deployment_yaml(
        cluster, "20261109-x", "pipeline", "client", 4)
    lines = text.split("\n")
    # never at Deployment.spec level (2-space, before '  template:')
    for ln in lines[:lines.index("  template:")]:
        assert not ln.startswith("  topologySpreadConstraints"), (
            "topologySpreadConstraints must not sit at Deployment.spec level")
    # must exist inside template.spec (6-space indent)
    assert "      topologySpreadConstraints:" in lines, (
        "topologySpreadConstraints missing from template.spec")
    # Deployment pod template requires restartPolicy Always
    assert "      restartPolicy: Always" in lines, (
        "Deployment pod template restartPolicy must be Always")
    assert "{{MC_" not in text
    assert "replicas: 4" in text and "role: client" in text


def test_pipeline_http_call_always_loops_back_via_node(monkeypatch):
    """Pipeline HTTP calls (/start, /shutdown) must curl 127.0.0.1 ON THE
    NODE ITSELF — the transport (ssh / kubectl exec) carries the command.
    The orchestrator host must never be assumed to reach pod IPs directly:
    a real cluster sat outside the pod network and every direct probe
    timed out."""
    calls = {}

    def fake_ssh(ip, node, cmd, dry_run=False, quiet=False):
        calls["cmd"] = cmd
        calls["ip"] = ip
        return "200"

    monkeypatch.setattr(bm, "ssh_check_output", fake_ssh)
    # a k8s pod node entry: is_k8s_node -> dispatches via kubectl exec
    pod_node = {"kind": "k8s-pod", "_pod": "mc-x-client-0", "_ns": "ns",
                "ip": "192.168.83.76", "_controller": {}}
    assert bm._pipeline_http_call(pod_node, 9301, "/start") is True
    assert "127.0.0.1:9301/start" in calls["cmd"]
    assert "192.168.83.76" not in calls["cmd"], (
        "must not target the pod IP from the orchestrator")
    # ssh node: same loopback command over ssh
    ssh_node = {"ip": "10.1.1.5"}
    assert bm._pipeline_http_call(ssh_node, 9201, "/shutdown") is True
    assert "127.0.0.1:9201/shutdown" in calls["cmd"]
    # non-200 -> False
    monkeypatch.setattr(bm, "ssh_check_output",
                        lambda ip, node, cmd, dry_run=False, quiet=False: "503")
    assert bm._pipeline_http_call(ssh_node, 9201, "/shutdown") is False


def test_marker_probe_shell_split():
    """The pid-liveness marker probe must be shell-split so the probing
    process's own /proc cmdline never matches itself on a recycled pid."""
    probe = bm._marker_probe("run_benchmark_pipeline.sh")
    assert '""' in probe
    # bash concatenates the "" away -> same fixed string for grep -F
    assert probe.replace('""', "") == "run_benchmark_pipeline.sh"


def test_wait_process_exit_marker_poll_cmd(monkeypatch):
    captured = {}

    def fake_ssh(ip, node, cmd, dry_run=False, quiet=False):
        captured["cmd"] = cmd
        return "DEAD"

    monkeypatch.setattr(bm, "ssh_check_output", fake_ssh)
    cluster = {"nodes": {"n1": {"ip": "10.0.0.1"}}}
    ok = bm.wait_process_exit(cluster, "n1", "59", "dual c1", 2,
                              marker=bm._PIPELINE_PID_MARKER)
    assert ok is True
    assert "/proc/59/cmdline" in captured["cmd"]
    assert '""' in captured["cmd"], "marker must be split to avoid self-match"
    assert captured["cmd"].count("run_benchmark_pipeline.sh") == 0, (
        "raw marker must not appear (only the split form)")
    # without marker: plain kill -0 poll
    bm.wait_process_exit(cluster, "n1", "59", "x", 2)
    assert "/proc/" not in captured["cmd"]


def test_check_exitrc(monkeypatch):
    def mk(ret):
        def fake_ssh(ip, node, cmd, dry_run=False, quiet=False):
            return ret
        return fake_ssh
    cluster = {"nodes": {"n1": {"ip": "10.0.0.1"}}}
    monkeypatch.setattr(bm, "ssh_check_output", mk("0"))
    bm._check_exitrc(cluster, "n1", "/tmp/x.log.exitrc", "dual c0")  # ok
    monkeypatch.setattr(bm, "ssh_check_output", mk(""))
    bm._check_exitrc(cluster, "n1", "/tmp/x.log.exitrc", "dual c0")  # no file
    monkeypatch.setattr(bm, "ssh_check_output", mk("1"))
    try:
        bm._check_exitrc(cluster, "n1", "/tmp/x.log.exitrc", "dual c0")
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "rc=1" in str(exc) and "x.log.exitrc" in str(exc)


def test_parse_result_file_dual_mixed_log(tmp_path):
    """Dual logs mix writer (no qw field) and reader (qw field) RESULT rows:
    qws is a SUBSEQUENCE of lats and must carry its own time axis."""
    pytest.importorskip("matplotlib")
    import importlib.util as _ilu
    _spec = _ilu.spec_from_file_location(
        "plot_latency",
        os.path.join(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))),
            "scripts", "plot_latency.py"))
    pl = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(pl)
    lines = []
    # 3 writer rows (7 fields with ts) + 2 reader rows (8 fields with qw)
    for i in range(3):
        lines.append("RESULT\t10:00:0{}.000000\tpipeline_write\tk{}\t"
                     "0.5\tOK\t1".format(i, i))
    lines.append("RESULT\t10:00:03.000000\tpipeline_read\tk0\t0.7\tOK\t0.9\t1")
    lines.append("RESULT\t10:00:04.000000\tpipeline_read\tk1\t0.8\tOK\t0.4\t1")
    lines.append("RESULT\t10:00:05.000000\tpipeline_write\tk9\t0.6\tERR:-200\t1")
    f = tmp_path / "dual.log"
    f.write_text("\n".join(lines) + "\n", encoding="utf-8")
    lats, qws, times, counts, qw_times = pl.parse_result_file(str(f))
    assert len(lats) == 5 and len(times) == 5          # OK rows only (3w+2r)
    assert len(qws) == 2 and len(qw_times) == 2        # reader rows only
    assert qw_times == sorted(qw_times)
    assert counts.get("ERR:-200") == 1
    # the queue-wait pair is internally consistent (the old bug passed the
    # 4-element times with the 2-element qws -> np.interp crash)
    assert len(qw_times) == len(qws)


def test_wait_k8s_deployment_ready_success(monkeypatch):
    c = {"_controller": {"ip": "127.0.0.1", "_ctx": ""},
         "k8s": {"namespace": "mc-test", "image": "i", "template": "t",
                 "controller_workspace": "/cw", "pod_workspace": "/pw"}}

    def fake(controller, args, check=True, timeout=None):
        a = " ".join(str(x) for x in args)
        if "availableReplicas" in a:
            return "4", 0
        if "spec.replicas" in a:
            return "4", 0
        return "", 0

    monkeypatch.setattr(bm, "controller_run", fake)
    bm.wait_k8s_deployment_ready(c, "d", "run1", timeout=5, poll=0)


def test_wait_k8s_deployment_ready_fails_fast_on_pod_fatal(monkeypatch):
    c = {"_controller": {"ip": "127.0.0.1", "_ctx": ""},
         "k8s": {"namespace": "mc-test", "image": "i", "template": "t",
                 "controller_workspace": "/cw", "pod_workspace": "/pw"}}

    def fake(controller, args, check=True, timeout=None):
        a = " ".join(str(x) for x in args)
        if "availableReplicas" in a:
            return "2", 0
        if "spec.replicas" in a:
            return "4", 0
        if a.startswith("-n mc-test get pods"):
            return "mc-p0 Running Error waiting\\nmc-p1 Pending \\n", 0
        if "get events" in a:
            return "10s Warning FailedScheduling pod/mc-p1 0/2 nodes available", 0
        if "describe" in a:
            return "Name: d", 0
        return "", 0

    diag = {"n": 0}
    monkeypatch.setattr(bm, "controller_run", fake)
    monkeypatch.setattr(bm, "_k8s_deployment_diag",
                        lambda *a, **k: diag.__setitem__("n", diag["n"] + 1))
    start = __import__("time").time()
    try:
        bm.wait_k8s_deployment_ready(c, "d", "run1", timeout=60, poll=0)
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "failed to start" in str(exc)
        assert "Error" in str(exc) or "FailedScheduling" in str(exc)
    assert __import__("time").time() - start < 5, "must fail fast, not wait 60s"
    assert diag["n"] == 1, "evidence dump must run exactly once"


def test_wait_k8s_deployment_ready_fails_fast_on_events_only(monkeypatch):
    c = {"_controller": {"ip": "127.0.0.1", "_ctx": ""},
         "k8s": {"namespace": "mc-test", "image": "i", "template": "t",
                 "controller_workspace": "/cw", "pod_workspace": "/pw"}}

    def fake(controller, args, check=True, timeout=None):
        a = " ".join(str(x) for x in args)
        if "availableReplicas" in a:
            return "0", 0
        if "spec.replicas" in a:
            return "1", 0
        if a.startswith("-n mc-test get pods"):
            return "mc-p0 Pending \\n", 0
        if "get events" in a:
            return "1s Warning ErrImageNeverPull pod/mc-p0 image never pulled", 0
        if "describe" in a:
            return "Name: d", 0
        return "", 0

    monkeypatch.setattr(bm, "controller_run", fake)
    monkeypatch.setattr(bm, "_k8s_deployment_diag", lambda *a, **k: None)
    try:
        bm.wait_k8s_deployment_ready(c, "d", "run1", timeout=60, poll=0)
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "ErrImageNeverPull" in str(exc)


def test_wait_k8s_deployment_ready_timeout_dumps_evidence(monkeypatch):
    c = {"_controller": {"ip": "127.0.0.1", "_ctx": ""},
         "k8s": {"namespace": "mc-test", "image": "i", "template": "t",
                 "controller_workspace": "/cw", "pod_workspace": "/pw"}}

    def fake(controller, args, check=True, timeout=None):
        a = " ".join(str(x) for x in args)
        if "availableReplicas" in a:
            return "2", 0
        if "spec.replicas" in a:
            return "4", 0
        if "get events" in a:
            return "REASON MESSAGE", 0
        if a.startswith("-n mc-test get pods"):
            return "mc-p0 Running \\n", 0
        if "describe" in a:
            return "Name: d", 0
        return "", 0

    diag = {"n": 0}
    monkeypatch.setattr(bm, "controller_run", fake)
    monkeypatch.setattr(bm, "_k8s_deployment_diag",
                        lambda *a, **k: diag.__setitem__("n", diag["n"] + 1))
    try:
        bm.wait_k8s_deployment_ready(c, "d", "run1", timeout=0, poll=0)
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "not ready in 0s" in str(exc) and "2/4" in str(exc)
    assert diag["n"] == 1
