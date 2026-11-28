#!/usr/bin/env python3
"""Unified failure reporting: one [FAIL] line with scenario + stage + reason,
then the full pod table and (case 2) the per-pod raw-log directories. No log
extraction, no describe/event dumps, no per-pod debug hints.

Case 1 = pods never became ready (failing stage starts with "deploy"):
show the inspect commands only. Case 2 = a stage failed after the pods were
up: also show where the raw logs stay. A failing remove-keys stage must not
retry the store remove-all (it would waste another full RPC timeout).

Pinpoint (exc carries .failing, e.g. the kvtest health gate's
InstanceGateError): the failure already names its instances, so the full
dumps are REPLACED by a per-instance classification — pod down (not
Running / restarted / gone) prints the full describe for that pod only;
pod up + process not ready prints exactly one raw-logs line with the
instance's per-process directory. No namespace-wide pod table, no other
pod mentioned. The gate error itself must keep the legacy message text
and carry the pending entries.
"""
import os
import sys
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import benchmark as bm  # noqa: E402
import utils  # noqa: E402
from webui.backend import runmgr  # noqa: E402


def _cluster():
    return {
        "k8s": {"namespace": "ns", "image": "img",
                "controller_workspace": "/tmp/cw",
                "pod_workspace": "/tmp/pw",
                "host_log_root": "/var/lib/mooncake-bench"},
        "_controller": {"ip": "127.0.0.1", "_ctx": ""},
        "nodes": {
            "pod-a": {"kind": "k8s-pod", "_pod": "pod-a", "ip": "10.0.0.1",
                      "host_ip": "141.62.32.39"},
            "pod-b": {"kind": "k8s-pod", "_pod": "pod-b", "ip": "10.0.0.2",
                      "host_ip": "141.62.32.15"},
        },
    }


def _fake_controller(monkeypatch):
    def _run(controller, args, check=False):
        if "pods" in args and "get" in args:
            if "-o" in args and "wide" in args:
                return ("NAME  READY\npod-a 1/1\npod-b 1/1\n", 0)
            return ("pod/pod-a\npod/pod-b\n", 0)
        return ("", 0)
    monkeypatch.setattr(bm, "controller_run", _run)


def _fake_controller_states(monkeypatch, wide_table, describes=None,
                            fail_get=False):
    """Controller fake for the pinpoint path: `get pods -o wide` returns the
    given table (or raises when fail_get), `describe pod X` returns the
    canned text."""
    describes = describes or {}

    def _run(controller, args, check=False):
        if "describe" in args:
            return (describes.get(args[-1], ""), 0)
        if fail_get:
            raise RuntimeError("kubectl unreachable")
        if "pods" in args and "get" in args and "wide" in args:
            return (wide_table, 0)
        return ("", 0)

    monkeypatch.setattr(bm, "controller_run", _run)


def _gate_error(failing):
    return bm.InstanceGateError(
        "kvtest health gate timed out after 120s: {}/96 instances not "
        "healthy: {}".format(len(failing),
                             ", ".join("i{} on {}".format(e["iid"],
                                                          e["node"])
                                       for e in failing[:8])),
        failing)


def test_report_class2_stage_failure(monkeypatch):
    lines = []
    monkeypatch.setattr(bm, "log", lambda msg: lines.append(msg))
    _fake_controller(monkeypatch)
    bm._scenario_failure_report(
        _cluster(), "cfg", "remove-keys (pre-run, 8MB)",
        RuntimeError("stage boom"), ["pod-a", "pod-b"],
        "/remote/logs", "20261123-1959", True, class1=False)
    text = "\n".join(lines)
    assert lines[0] == ("[FAIL] scenario cfg failed at stage "
                        "'remove-keys (pre-run, 8MB)': stage boom")
    assert "[k8s] pods (ns=ns):" in text
    assert "pod-a 1/1" in text
    assert "[FAIL] raw logs: ssh 141.62.32.39 -> " \
           "/var/lib/mooncake-bench/20261123-1959/pod-a/" in text
    assert "[FAIL] raw logs: ssh 141.62.32.15 -> " \
           "/var/lib/mooncake-bench/20261123-1959/pod-b/" in text
    assert "get events" not in text
    assert "describe pod" not in text
    assert "[k8s][DEBUG]" not in text


def test_report_class1_pods_never_ready(monkeypatch):
    lines = []
    monkeypatch.setattr(bm, "log", lambda msg: lines.append(msg))
    _fake_controller(monkeypatch)
    bm._scenario_failure_report(
        _cluster(), "cfg", "deploy client pods",
        RuntimeError("pods failed to start (reason=ImagePullBackOff)"),
        ["pod-a", "pod-b"], "/remote/logs", "20261123-1959", True,
        class1=True)
    text = "\n".join(lines)
    assert "failed at stage 'deploy client pods'" in text
    assert "ImagePullBackOff" in text
    assert "[k8s] pods (ns=ns):" in text
    assert "describe pod pod-a" in text
    assert "get events --sort-by=.lastTimestamp" in text
    assert "raw logs:" not in text


def test_report_ssh_raw_dirs(monkeypatch):
    lines = []
    monkeypatch.setattr(bm, "log", lambda msg: lines.append(msg))
    cluster = {"nodes": {"n1": {"ip": "192.168.1.10"}}}
    bm._scenario_failure_report(
        cluster, "cfg", "benchmark (8MB, c1 t1)", RuntimeError("boom"),
        ["n1"], "/remote/logs", "ts", False, class1=False)
    assert lines[-1] == "[FAIL] raw logs: ssh 192.168.1.10 -> /remote/logs/"


# ---- pinpoint path: exc carries .failing (health gate timeout) ----

_WIDE_RUNNING = ("NAME READY STATUS RESTARTS AGE IP NODE\n"
                 "pod-a 1/1 Running 0 2m 10.0.0.1 n1\n"
                 "pod-b 1/1 Running 0 2m 10.0.0.2 n1\n")


def test_report_gate_pinpoint_running_pod_process_dir(monkeypatch):
    """Pod up + process not ready -> exactly one raw-logs line with the
    instance's per-process directory; no pod table, no describe, no other
    pod mentioned."""
    lines = []
    monkeypatch.setattr(bm, "log", lambda msg: lines.append(msg))
    _fake_controller_states(monkeypatch, _WIDE_RUNNING)
    bm._scenario_failure_report(
        _cluster(), "cfg", "health gate (instances, 3.5MB)",
        _gate_error([{"node": "pod-a", "iid": 11, "port": 9301,
                      "_role": "writer"}]),
        ["pod-a", "pod-b"], "/remote/logs", "20261128-1033", True,
        class1=False)
    text = "\n".join(lines)
    assert lines[0].startswith("[FAIL] scenario cfg failed at stage")
    assert ("[FAIL] i11 (writer) on pod-a: pod Running（RESTARTS=0），"
            "进程未就绪，原始日志目录如下" in text)
    assert ("[FAIL] raw logs: ssh 141.62.32.39 -> "
            "/var/lib/mooncake-bench/20261128-1033/pod-a/writer_i11_p9301/"
            in text)
    assert "pods (ns=" not in text      # no namespace-wide dump
    assert "describe" not in text       # pod is up: no describe
    assert "pod-b" not in text          # healthy pods stay invisible


@pytest.mark.parametrize("status,restarts", [
    ("Pending", 0), ("CrashLoopBackOff", 3)])
def test_report_gate_pinpoint_pod_down_describes_only_that_pod(
        monkeypatch, status, restarts):
    """Pod not Running (or restarted) -> full describe for THAT pod only;
    no raw-logs line, no pod table, no other pod mentioned."""
    lines = []
    monkeypatch.setattr(bm, "log", lambda msg: lines.append(msg))
    wide = ("NAME READY STATUS RESTARTS AGE IP NODE\n"
            "pod-a 1/1 {s} {r} 2m 10.0.0.1 n1\n"
            "pod-b 1/1 Running 0 2m 10.0.0.2 n1\n").format(s=status,
                                                            r=restarts)
    _fake_controller_states(
        monkeypatch, wide,
        describes={"pod-a": "Name: pod-a\nEvents:\n  Warning "
                            "FailedScheduling 0/1 nodes available"})
    bm._scenario_failure_report(
        _cluster(), "cfg", "health gate (instances, 3.5MB)",
        _gate_error([{"node": "pod-a", "iid": 11, "port": 9301,
                      "_role": "writer"}]),
        ["pod-a", "pod-b"], "/remote/logs", "20261128-1033", True,
        class1=False)
    text = "\n".join(lines)
    assert ("[FAIL] i11 (writer) on pod-a: pod 未就绪（STATUS={}，"
            "RESTARTS={}），describe 输出如下".format(status, restarts)
            in text)
    assert "[k8s] describe pod pod-a:" in text
    assert "FailedScheduling" in text     # full describe body printed
    assert "raw logs" not in text
    assert "pods (ns=" not in text
    assert "pod-b" not in text


def test_report_gate_pinpoint_pod_missing_from_listing(monkeypatch):
    """Failing pod absent from a successful listing (deleted/never created)
    -> classified pod-down + describe attempt."""
    lines = []
    monkeypatch.setattr(bm, "log", lambda msg: lines.append(msg))
    _fake_controller_states(
        monkeypatch,
        "NAME READY STATUS RESTARTS AGE IP NODE\n"
        "pod-b 1/1 Running 0 2m 10.0.0.2 n1\n",
        describes={"pod-a": "Error from server (NotFound)"})
    bm._scenario_failure_report(
        _cluster(), "cfg", "health gate (instances, 3.5MB)",
        _gate_error([{"node": "pod-a", "iid": 11, "port": 9301,
                      "_role": "writer"}]),
        ["pod-a", "pod-b"], "/remote/logs", "ts", True, class1=False)
    text = "\n".join(lines)
    assert "[FAIL] i11 (writer) on pod-a: pod 未就绪（不在 pod 列表中）" in text
    assert "[k8s] describe pod pod-a:" in text


def test_report_gate_pinpoint_shared_pod_describe_once(monkeypatch):
    """Split mode: two failing instances share one pod (describe once, two
    distinct process dirs); a third instance on a crashing pod gets the
    describe path and NO raw-logs line."""
    lines = []
    monkeypatch.setattr(bm, "log", lambda msg: lines.append(msg))
    wide = ("NAME READY STATUS RESTARTS AGE IP NODE\n"
            "pod-a 1/1 Running 0 2m 10.0.0.1 n1\n"
            "pod-b 1/1 CrashLoopBackOff 2 2m 10.0.0.2 n1\n")
    _fake_controller_states(monkeypatch, wide, describes={"pod-b": "boom"})
    bm._scenario_failure_report(
        _cluster(), "cfg", "health gate (instances, 3.5MB)",
        _gate_error([
            {"node": "pod-a", "iid": 11, "port": 9301, "_role": "writer"},
            {"node": "pod-a", "iid": 59, "port": 9401, "_role": "reader"},
            {"node": "pod-b", "iid": 12, "port": 9401, "_role": "reader"}]),
        ["pod-a", "pod-b"], "/remote/logs", "ts", True, class1=False)
    text = "\n".join(lines)
    assert ("raw logs: ssh 141.62.32.39 -> "
            "/var/lib/mooncake-bench/ts/pod-a/writer_i11_p9301/" in text)
    assert ("raw logs: ssh 141.62.32.39 -> "
            "/var/lib/mooncake-bench/ts/pod-a/reader_i59_p9401/" in text)
    assert text.count("[k8s] describe pod pod-b:") == 1
    assert "pod-b 1/1 CrashLoopBackOff" not in text   # no table dump
    raw_b = [ln for ln in lines if "raw logs" in ln and "pod-b" in ln]
    assert raw_b == []


def test_report_gate_pinpoint_ssh_flat_dir(monkeypatch):
    """ssh mode: no pods to classify — one flat per-instance dir line."""
    lines = []
    monkeypatch.setattr(bm, "log", lambda msg: lines.append(msg))
    cluster = {"nodes": {"n1": {"ip": "192.168.1.10"}}}
    bm._scenario_failure_report(
        cluster, "cfg", "health gate (writers, 8MB)",
        _gate_error([{"node": "n1", "iid": 11, "port": 9306,
                      "_role": "writer"}]),
        ["n1"], "/remote/logs", "ts", False, class1=False)
    text = "\n".join(lines)
    assert "[FAIL] i11 (writer) on n1: 进程未就绪" in text
    assert lines[-1] == ("[FAIL] raw logs: ssh 192.168.1.10 -> "
                         "/remote/logs/writer_i11_p9306/")
    assert "pods (ns=" not in text
    assert "describe" not in text


def test_report_gate_pinpoint_pod_query_failure_falls_back(monkeypatch):
    """`kubectl get pods` itself failing -> WARN + process-not-ready dirs
    (pods were launched; the gate ran after they existed)."""
    lines = []
    monkeypatch.setattr(bm, "log", lambda msg: lines.append(msg))
    _fake_controller_states(monkeypatch, "", fail_get=True)
    bm._scenario_failure_report(
        _cluster(), "cfg", "health gate (instances, 3.5MB)",
        _gate_error([{"node": "pod-a", "iid": 11, "port": 9301,
                      "_role": "writer"}]),
        ["pod-a", "pod-b"], "/remote/logs", "ts", True, class1=False)
    text = "\n".join(lines)
    assert "[WARN] pod 状态查询失败" in text
    assert "[FAIL] i11 (writer) on pod-a: pod 状态未知（查询失败），进程未就绪" in text
    assert "raw logs: ssh 141.62.32.39 -> " in text
    assert "describe pod" not in text
    assert "pods (ns=" not in text


def test_health_gate_error_carries_failing_entries(monkeypatch):
    """The gate raises InstanceGateError with the legacy message text and
    the pending entries attached."""
    monkeypatch.setattr(bm, "log", lambda msg: None)
    monkeypatch.setattr(bm, "_pipeline_http_call",
                        lambda *a, **k: False)
    cluster = {"nodes": {"n1": {"ip": "10.0.0.1"}}}
    entries = [{"node": "n1", "iid": 1, "port": 9301, "_role": "writer"},
               {"node": "n1", "iid": 2, "port": 9302, "_role": "writer"}]
    with pytest.raises(bm.InstanceGateError) as ei:
        bm._kvtest_health_gate(cluster, entries, 0, "instances")
    assert str(ei.value) == ("kvtest health gate timed out after 0s: "
                             "2/2 instances not healthy: "
                             "i1 on n1, i2 on n1")
    assert ei.value.failing == entries


def test_kvtest_fail_cleanup_skips_remove_all_for_remove_keys(monkeypatch):
    monkeypatch.setattr(bm, "log", lambda msg: None)
    calls = []
    monkeypatch.setattr(bm, "run_stage", lambda *a, **k: calls.append(k))
    bm.fail_cleanup_kvtest(
        _cluster(), [], ["pod-a"], "/r/logs", "cfg", "/tmp/local",
        remove_host="10.0.0.1", remove_node="pod-a",
        remove_runner="/r/run.sh", failed_stage="remove-keys (pre-run, 8MB)")
    assert calls == []


def test_kvtest_fail_cleanup_labels_other_remove_all(monkeypatch):
    monkeypatch.setattr(bm, "log", lambda msg: None)
    monkeypatch.setattr(bm, "_verify_store_clean", lambda *a, **k: None)
    calls = []
    monkeypatch.setattr(bm, "run_stage",
                        lambda *a, **k: calls.append(k))
    bm.fail_cleanup_kvtest(
        _cluster(), [], ["pod-a"], "/r/logs", "cfg", "/tmp/local",
        remove_host="10.0.0.1", remove_node="pod-a",
        remove_runner="/r/run.sh", failed_stage="measurement (8MB)")
    assert len(calls) == 1
    assert calls[0]["label"] == "cfg (fail-cleanup remove-all)"


def test_pipeline_fail_cleanup_never_pulls_logs(monkeypatch):
    monkeypatch.setattr(bm, "log", lambda msg: None)
    calls = []
    monkeypatch.setattr(bm, "pull_logs", lambda *a, **k: calls.append("pull"))
    monkeypatch.setattr(bm, "run_stage", lambda *a, **k: calls.append("stage"))
    bm.fail_cleanup_pipeline(
        _cluster(), {"readers": [], "writers": []}, [], [], ["pod-a"],
        "/r/logs", "cfg", "/tmp/local",
        remove_host="10.0.0.1", remove_node="pod-a", remove_runner="/r",
        failed_stage="remove-keys (pre-run, 8MB)")
    assert calls == []


def test_pipeline_fail_cleanup_remove_all_kept_for_other_stages(monkeypatch):
    monkeypatch.setattr(bm, "log", lambda msg: None)
    monkeypatch.setattr(bm, "_verify_store_clean", lambda *a, **k: None)
    labels = []
    monkeypatch.setattr(bm, "pull_logs", lambda *a, **k: None)
    monkeypatch.setattr(bm, "run_stage",
                        lambda *a, **k: labels.append(k.get("label")))
    bm.fail_cleanup_pipeline(
        _cluster(), {"readers": [], "writers": []}, [], [], ["pod-a"],
        "/r/logs", "cfg", "/tmp/local",
        remove_host="10.0.0.1", remove_node="pod-a", remove_runner="/r",
        failed_stage="measurement (8MB)")
    assert labels == ["cfg (fail-cleanup remove-all)"]


def test_utf8_stdio_env_helper():
    env = runmgr._utf8_stdio_env({})
    assert env["PYTHONUTF8"] == "1"
    assert env["PYTHONIOENCODING"] == "utf-8"


class _FakeStream(object):
    def __init__(self, tty):
        self._tty = tty
        self.kw = None

    def isatty(self):
        return self._tty

    def reconfigure(self, **kw):
        self.kw = kw


def test_ensure_utf8_stdio_non_tty(monkeypatch):
    out, err = _FakeStream(False), _FakeStream(False)
    monkeypatch.setattr(utils.sys, "stdout", out)
    monkeypatch.setattr(utils.sys, "stderr", err)
    utils.ensure_utf8_stdio()
    assert out.kw == {"encoding": "utf-8", "errors": "replace"}
    assert err.kw == {"encoding": "utf-8", "errors": "replace"}


def test_ensure_utf8_stdio_leaves_tty(monkeypatch):
    out, err = _FakeStream(True), _FakeStream(True)
    monkeypatch.setattr(utils.sys, "stdout", out)
    monkeypatch.setattr(utils.sys, "stderr", err)
    utils.ensure_utf8_stdio()
    assert out.kw is None and err.kw is None
