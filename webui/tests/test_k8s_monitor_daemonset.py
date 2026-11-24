#!/usr/bin/env python3
"""Node-level monitor DaemonSet (k8s): template render, per-host pseudo-nodes,
target selection, and teardown. No real cluster required."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import benchmark as bm


def _cluster():
    c = {
        "controller": {"ip": "127.0.0.1", "ssh_user": "root", "ssh_port": 22,
                       "ssh_key": "", "passwd": "", "kubectl": "kubectl",
                       "kubeconfig": ""},
        "k8s": {"namespace": "mc-test", "kubecontext": "",
                "image": "local/test:1",
                "controller_workspace": "/tmp/cw",
                "pod_workspace": "/tmp/pw",
                "host_log_root": "/var/lib/mcbench"},
    }
    c["_controller"] = bm.build_controller(c)
    return c


def _involved(c):
    c["nodes"] = {
        "bench-a": {"kind": "k8s-pod", "_pod": "bench-a", "_ns": "mc-test",
                    "host_ip": "192.168.1.2"},
        "bench-b": {"kind": "k8s-pod", "_pod": "bench-b", "_ns": "mc-test",
                    "host_ip": "192.168.1.3"},
    }
    return ["bench-a", "bench-b"]


def _monitor_matrix(enabled=True):
    return {"tools": {"mem": {"enabled": enabled},
                      "urma": {"enabled": enabled}},
            "trigger": "whole_run"}


def _fake_cluster_run(pods):
    """kubectl fake: apply -> ok, ds ready -> 2 2, pod list -> `pods`."""
    calls = []

    def fake(controller, args, check=True, timeout=None):
        calls.append(list(args))
        argv = " ".join(str(a) for a in args)
        if "apply" in args:
            return "", 0
        if "get" in args and "daemonset" in args:
            return "{} {}".format(len(pods), len(pods)), 0
        if "get" in args and "pods" in args and "-l" in args:
            return "\n".join(
                "{}|{}|{}|{}|{}".format(*p) for p in pods), 0
        if "delete" in args and "daemonset" in args:
            return "", 0
        if "events" in args:
            return "REASON  MESSAGE  COUNT", 0
        raise AssertionError("unexpected kubectl call: " + argv)

    return calls, fake


def test_render_monitor_daemonset_template():
    c = _cluster()
    name, run_label, text = bm._render_monitor_daemonset_yaml(
        c, "20261104-1530", "kv1")
    assert name == "mc-20261104-1530-kv1-monitor"
    assert run_label == "20261104-1530-kv1"
    assert "{{MC_" not in text
    assert "kind: DaemonSet" in text
    assert "name: mc-20261104-1530-kv1-monitor" in text
    assert "namespace: mc-test" in text
    assert "hostPID: true" in text
    assert "privileged: true" in text
    assert "operator: Exists" in text
    # shared hostPath log root + identical in-pod mount as the benchmark pods
    assert "path: /var/lib/mcbench/20261104-1530" in text
    assert "mountPath: /tmp/pw/logs/20261104-1530" in text
    assert "mountPath: /host" in text


def test_ensure_registers_one_pseudo_node_per_involved_host(monkeypatch):
    c = _cluster()
    involved = _involved(c)
    pods = [
        ("mc-mon-a", "Running", "10.0.0.11", "192.168.1.2", "node-a"),
        ("mc-mon-b", "Running", "10.0.0.12", "192.168.1.3", "node-b"),
        # uninvolved host: must never be profiled for this run
        ("mc-mon-c", "Running", "10.0.0.13", "192.168.9.9", "node-c"),
        # not Running yet: skipped
        ("mc-mon-d", "Pending", "", "192.168.1.2", "node-a"),
    ]
    calls, fake = _fake_cluster_run(pods)
    puts = []
    monkeypatch.setattr(bm, "controller_run", fake)
    monkeypatch.setattr(bm, "controller_put",
                        lambda ctl, files, remote: puts.append(remote))
    nodes = bm.ensure_k8s_monitor_daemonset(
        c, "20261104-1530", "kv1", involved)
    assert nodes == ["mc-mon-a", "mc-mon-b"]
    assert len(puts) == 1
    assert sum(1 for a in calls if "apply" in a) == 1
    for pod, host_ip, node_name in (("mc-mon-a", "192.168.1.2", "node-a"),
                                    ("mc-mon-b", "192.168.1.3", "node-b")):
        n = c["nodes"][pod]
        assert n["kind"] == "k8s-pod"
        assert n["_monitor"] is True
        assert n["_role"] == "monitor"
        assert n["_node_name"] == node_name
        assert n["host_ip"] == host_ip
        assert n["_host_exec"] == "nsenter -t 1 -m -n --"
        assert n["_controller"] is c["_controller"]
        assert n["_run_label"] == "20261104-1530-kv1"
    assert c["_k8s_monitor_ds"] == ["mc-20261104-1530-kv1-monitor"]
    # cached: a second call must not re-apply the DaemonSet
    again = bm.ensure_k8s_monitor_daemonset(
        c, "20261104-1530", "kv1", involved)
    assert again == nodes
    assert sum(1 for a in calls if "apply" in a) == 1


def test_ensure_host_exec_configurable(monkeypatch):
    c = _cluster()
    c["k8s"]["monitor_host_exec"] = "chroot /host"
    involved = _involved(c)
    pods = [("mc-mon-a", "Running", "10.0.0.11", "192.168.1.2", "node-a")]
    _calls, fake = _fake_cluster_run(pods)
    monkeypatch.setattr(bm, "controller_run", fake)
    monkeypatch.setattr(bm, "controller_put", lambda *a, **k: None)
    bm.ensure_k8s_monitor_daemonset(c, "20261104-1530", "chroot", involved)
    assert c["nodes"]["mc-mon-a"]["_host_exec"] == "chroot /host"

    # empty override = pod-only tools (mem works, urma only if mdcli is in
    # the image): the pseudo-node carries no wrapper
    c2 = _cluster()
    c2["k8s"]["monitor_host_exec"] = ""
    _involved(c2)
    monkeypatch.setattr(bm, "controller_run", fake)
    bm.ensure_k8s_monitor_daemonset(c2, "20261104-1530", "plain", involved)
    assert c2["nodes"]["mc-mon-a"]["_host_exec"] == ""


def test_ensure_dry_run_touches_nothing(monkeypatch):
    c = _cluster()
    involved = _involved(c)

    def boom(*a, **k):
        raise AssertionError("dry-run must not call kubectl")

    monkeypatch.setattr(bm, "controller_run", boom)
    assert bm.ensure_k8s_monitor_daemonset(
        c, "20261104-1530", "dry", involved, dry_run=True) == involved
    assert not any((v or {}).get("_monitor")
                   for v in c["nodes"].values())


def test_delete_monitor_daemonset_drops_pseudo_nodes(monkeypatch):
    c = _cluster()
    c["nodes"] = {"mc-mon-a": {"kind": "k8s-pod", "_monitor": True,
                               "host_ip": "192.168.1.2"}}
    c["_k8s_monitor_ds"] = ["mc-x-monitor"]
    calls = []

    def fake(controller, args, check=True, timeout=None):
        calls.append(list(args))
        return "", 0

    monkeypatch.setattr(bm, "controller_run", fake)
    bm._delete_monitor_daemonset(c)
    assert any(a[:3] == ["-n", "mc-test", "delete"] and "daemonset" in a
               for a in calls)
    assert "mc-mon-a" not in c["nodes"]
    assert "_k8s_monitor_ds" not in c


def test_wait_monitor_daemonset_ready(monkeypatch):
    c = _cluster()

    def ready(controller, args, check=True, timeout=None):
        return "2 2", 0

    monkeypatch.setattr(bm, "controller_run", ready)
    bm._wait_monitor_daemonset_ready(c, "mc-x-monitor", "mc-test",
                                     timeout=5, poll=0.0)

    def stuck(controller, args, check=True, timeout=None):
        argv = " ".join(str(a) for a in args)
        if "daemonset" in args:
            return "0 2", 0
        if "pods" in args:
            return "mc-x-monitor-1 0/1 Pending node-a", 0
        if "events" in args:
            return "REASON  MESSAGE  COUNT", 0
        raise AssertionError("unexpected: " + argv)

    monkeypatch.setattr(bm, "controller_run", stuck)
    try:
        bm._wait_monitor_daemonset_ready(c, "mc-x-monitor", "mc-test",
                                         timeout=0, poll=0.0)
        raise AssertionError("should have raised")
    except RuntimeError as exc:
        assert "not ready" in str(exc)


def test_profiling_targets_ssh_unchanged():
    c = _cluster()
    involved = _involved(c)
    c.pop("_controller")
    assert bm._profiling_targets(c, _monitor_matrix(), involved, "ts", "kv1") \
        == involved


def test_profiling_targets_skips_when_no_monitor_tools(monkeypatch):
    c = _cluster()
    involved = _involved(c)

    def boom(*a, **k):
        raise AssertionError("DaemonSet must not be created without monitors")

    monkeypatch.setattr(bm, "ensure_k8s_monitor_daemonset", boom)
    matrix = {"tools": {"mem": {"enabled": False},
                        "urma": {"enabled": False}},
              "trigger": "whole_run"}
    assert bm._profiling_targets(c, matrix, involved, "ts", "kv1") == involved
    assert bm._profiling_targets(c, None, involved, "ts", "kv1") == involved


def test_profiling_targets_uses_daemonset(monkeypatch):
    c = _cluster()
    involved = _involved(c)
    monkeypatch.setattr(bm, "ensure_k8s_monitor_daemonset",
                        lambda cl, ts, cfg, inv, dry=False: ["mc-mon-a"])
    assert bm._profiling_targets(c, _monitor_matrix(), involved, "ts", "kv1") \
        == ["mc-mon-a"]


def test_profiling_targets_falls_back_to_pods(monkeypatch):
    c = _cluster()
    involved = _involved(c)

    def fail(*a, **k):
        raise RuntimeError("no RBAC for daemonsets")

    monkeypatch.setattr(bm, "ensure_k8s_monitor_daemonset", fail)
    assert bm._profiling_targets(c, _monitor_matrix(), involved, "ts", "kv1") \
        == involved


def test_stop_profiling_collects_then_tears_down(monkeypatch):
    c = _cluster()
    stopped, torn = [], []

    class FakeProf:
        @staticmethod
        def stop_and_collect(cluster, sess, local_dir):
            stopped.append((cluster, sess, local_dir))

    monkeypatch.setattr(bm, "prof_core", FakeProf)
    monkeypatch.setattr(bm, "_delete_monitor_daemonset",
                        lambda cl, dry_run=False: torn.append(dry_run))
    bm._stop_profiling(c, {"sess": 1}, "/tmp/results/ts", "kv1", dry=False)
    assert stopped and stopped[0][1] == {"sess": 1}
    assert stopped[0][2].replace(os.sep, "/").endswith(
        "results/ts/logs/kv1/profiling")
    assert torn == [False]

    # a session that never started must still tear the DaemonSet down
    stopped.clear()
    bm._stop_profiling(c, None, "/tmp/results/ts", "kv1", dry=False)
    assert stopped == []
    assert torn == [False, False]


def test_monitor_node_probes_container_mem_and_host_urma(monkeypatch):
    """Real-cluster regression: the host wrapper must gate ONLY host tools.

    A monitor pseudo-node carrying `_host_exec` made the mem probe run
    `nsenter ... command -v bash`, which fails on images without nsenter and
    silently disabled node memory monitoring. mem runs in-container (hostPID +
    host /proc//sys), so only urma's mdcli goes through the wrapper."""
    pc = bm.prof_core
    assert pc is not None
    calls = []

    def fake_run(ip, node, cmd, quiet=True):
        calls.append(cmd)
        if "command -v" in cmd:
            return "OK"
        if "nohup" in cmd:
            return "4242"
        return ""

    monkeypatch.setattr(pc, "_run", fake_run)
    monkeypatch.setattr(pc, "scp_push", lambda *a, **k: None)
    monkeypatch.setattr(bm, "state_append", lambda rows: None)
    node = {"kind": "k8s-pod", "_pod": "mon-a", "_monitor": True,
            "_host_exec": "nsenter -t 1 -m -n --", "host_ip": "10.1.1.1",
            "ip": "10.42.0.9"}
    cluster = {"_controller": {"ip": "10.0.0.1", "_ctx": ""},
               "nodes": {"mon-a": node}}
    matrix = {"tools": {
        "mem": {"enabled": True, "bin": "bash", "interval_s": 1,
                "proc_pattern": "mooncake_kvtest"},
        "urma": {"enabled": True, "bin": "mdcli", "host_tool": True,
                 "slot": 2, "ubpus": [0, 1], "interval_s": 1}},
        "nodes": "auto", "trigger": "whole_run",
        "outliers": {"threshold_ms": 1.0, "window_ms": 200, "auto": True}}
    sess = pc.start_profiling(cluster, matrix, ["mon-a"], "cfg", "/tmp/out")
    assert sess and sess["tools"]["mon-a"].get("mem") == "4242"
    mem_probe = next(c for c in calls if "command -v bash" in c)
    urma_probe = next(c for c in calls if "command -v mdcli" in c)
    assert "nsenter" not in mem_probe, "mem probes the container, not the node"
    assert "nsenter" in urma_probe, "urma probes the NODE mdcli"
    mem_launch = next(c for c in calls if "mem_monitor.sh" in c)
    urma_launch = next(c for c in calls if "urma_monitor.sh" in c)
    assert "MDCLI_BIN" not in mem_launch
    assert "MDCLI_BIN=" in urma_launch
    assert "nsenter -t 1 -m -n -- mdcli" in urma_launch


if __name__ == "__main__":
    import inspect
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            if len(inspect.signature(fn).parameters) == 0:
                fn()
    print("PASS test_k8s_monitor_daemonset")
