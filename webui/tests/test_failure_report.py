#!/usr/bin/env python3
"""Unified failure reporting: one [FAIL] line with scenario + stage + reason,
then the full pod table and (case 2) the per-pod raw-log directories. No log
extraction, no describe/event dumps, no per-pod debug hints.

Case 1 = pods never became ready (failing stage starts with "deploy"):
show the inspect commands only. Case 2 = a stage failed after the pods were
up: also show where the raw logs stay. A failing remove-keys stage must not
retry the store remove-all (it would waste another full RPC timeout).
"""
import os
import sys
from types import SimpleNamespace

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
