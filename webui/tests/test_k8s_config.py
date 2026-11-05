#!/usr/bin/env python3
"""k8s config/contract regression tests (no real cluster required)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import benchmark as bm
import utils as u


def _cluster():
    return {
        "controller": {"ip": "127.0.0.1", "ssh_user": "root", "ssh_port": 22,
                       "ssh_key": "", "passwd": "", "kubectl": "kubectl",
                       "kubeconfig": ""},
        "k8s": {"namespace": "mc-test", "kubecontext": "",
                "image": "local/test:1",
                "template": "configs/_k8s/templates/benchmark-pod.yaml.tpl",
                "controller_workspace": "/tmp/cw", "pod_workspace": "/tmp/pw"},
    }


def test_remote_paths_use_posix_separators():
    # Windows orchestrators must not leak '\\' into controller/pod paths
    assert u._remote_path("/tmp/a\\b\\c") == "/tmp/a/b/c"
    assert u._remote_path("/tmp/Mooncake-Performance/pods/x.yaml") == \
        "/tmp/Mooncake-Performance/pods/x.yaml"


def test_parse_size_bytes():
    assert bm.parse_size_bytes(123) == 123
    assert bm.parse_size_bytes("1GB") == 1024 ** 3
    assert bm.parse_size_bytes("512KB") == 512 * 1024
    assert bm.parse_size_bytes("1.5MB") == int(1.5 * 1024 ** 2)
    try:
        bm.parse_size_bytes("0")
    except RuntimeError:
        pass
    else:
        raise AssertionError("zero size must fail")


def test_controller_local_and_names():
    c = _cluster()
    ctrl = bm.build_controller(c)
    assert ctrl["_local"] is True
    assert bm.k8s_pod_name("20261104-1530", "put") == "mc-20261104-1530-put"
    assert bm.k8s_run_label("20261104-1530", "put") == "20261104-1530-put"
    assert len(bm.k8s_pod_name("20261104-1530", "x" * 100)) <= 63


def test_render_pod_template():
    c = _cluster()
    pod, run_label, text = bm._render_pod_yaml(c, "20261104-1530", "put")
    assert pod == "mc-20261104-1530-put"
    assert "{{MC_" not in text
    assert "mountPath: /tmp/pw/scripts" in text
    assert "mountPath: /tmp/pw\n" in text or "mountPath: /tmp/pw" in text
    assert "name: mc-20261104-1530-put" in text
    assert "namespace: mc-test" in text


def _session():
    return {"SETUP_MASTER_TYPE": "http", "SETUP_UB_DEVICE": "",
            "ORCH_VENV": "", "SETUP_LOCAL_BUFFER_SIZE": str(1024 ** 3),
            "SETUP_TENANT_ID": "default", "SETUP_RANDOM_SEED": "42",
            "BENCH_REMOTE_DATA_DIR": "/tmp/pw/data"}


def test_ub_env_auto_vs_explicit():
    params = {"BENCH_SCENARIO": "basic", "SETUP_PROTOCOL": "ub"}
    auto = bm.build_k8s_env_content(params, _session())
    assert "urma_admin show" in auto
    sess = _session()
    sess["SETUP_UB_DEVICE"] = "bonding_dev_3"
    explicit = bm.build_k8s_env_content(params, sess)
    assert "urma_admin show" not in explicit
    assert 'SETUP_UB_DEVICE="bonding_dev_3"' in explicit


def test_resolve_k8s_svc(monkeypatch):
    c = _cluster()
    c["_controller"] = bm.build_controller(c)
    diag = []

    def fake_ok(controller, args, check=True, timeout=None):
        if "svc" in args:
            return ("10.109.138.197", 0)
        return ("192.168.49.60", 0)

    monkeypatch.setattr(bm, "controller_run", fake_ok)
    monkeypatch.setattr(bm, "controller_diagnose",
                        lambda ctrl: diag.append(1) or "diag")
    assert bm._resolve_k8s_svc("mooncake-etcd", "mooncake", "",
                               c["_controller"]) == "10.109.138.197"

    def fake_headless(controller, args, check=True, timeout=None):
        if "svc" in args:
            return ("None", 0)
        return ("192.168.49.60", 0)

    monkeypatch.setattr(bm, "controller_run", fake_headless)
    assert bm._resolve_k8s_svc("mooncake-etcd", "mooncake", "",
                               c["_controller"]) == "192.168.49.60"

    def fake_warn(controller, args, check=True, timeout=None):
        return ("warning: deprecated\n10.0.0.9", 0)

    monkeypatch.setattr(bm, "controller_run", fake_warn)
    assert bm._resolve_k8s_svc("mooncake-etcd", "mooncake", "",
                               c["_controller"]) == "10.0.0.9"

    def fake_fail(controller, args, check=True, timeout=None):
        return ("command not found", 127)

    monkeypatch.setattr(bm, "controller_run", fake_fail)
    diag.clear()
    assert bm._resolve_k8s_svc("mooncake-etcd", "mooncake", "",
                               c["_controller"]) is None
    assert diag, "controller diagnostics should be logged on failure"


def test_resolve_host_service_fqdn(monkeypatch):
    c = _cluster()
    calls = []

    def fake_svc(svc, ns, ctx, controller=None):
        calls.append((svc, ns))
        return "10.109.138.197"

    monkeypatch.setattr(bm, "_resolve_k8s_svc", fake_svc)
    assert bm._resolve_host(
        "mooncake-etcd.mooncake.svc.cluster.local", c) == "10.109.138.197"
    assert calls == [("mooncake-etcd", "mooncake")]


def test_ssh_env_ub_auto_vs_explicit():
    params = {"BENCH_SCENARIO": "basic", "SETUP_PROTOCOL": "ub"}
    sess = _session()
    auto = bm.build_env_content(params, sess, "10.0.0.1")
    assert "urma_admin show" in auto
    sess["SETUP_UB_DEVICE"] = "bonding_dev_2"
    explicit = bm.build_env_content(params, sess, "10.0.0.1")
    assert "urma_admin show" not in explicit
    assert 'SETUP_UB_DEVICE="bonding_dev_2"' in explicit


def test_namespace_lifecycle(monkeypatch, tmp_path):
    c = _cluster()
    c["_controller"] = bm.build_controller(c)
    calls = []
    state = {"exists": False}

    def fake_run(controller, args, check=True, timeout=None):
        calls.append(list(args))
        if args[0] == "get" and "jsonpath" in args[-1]:
            return ("mcbench", 0)
        if args[0] == "get":
            return ("", 0 if state["exists"] else 1)
        if args[0] == "create" and args[1] == "namespace":
            state["exists"] = True
        if args[0] == "delete" and args[1] == "namespace":
            state["exists"] = False
        return ("", 0)

    monkeypatch.setattr(bm, "STATE_FILE", str(tmp_path / ".state"))
    monkeypatch.setattr(bm, "controller_run", fake_run)
    bm.ensure_k8s_namespace(c, "20261104-1530")
    assert any(a[:2] == ["create", "namespace"] for a in calls)
    assert any("k8s-namespace" in line for line in open(bm.STATE_FILE))
    bm.cleanup_k8s_namespace(c)
    assert not state["exists"]
    assert open(bm.STATE_FILE).read().strip() == ""


def test_reap_stale_namespace(monkeypatch, tmp_path):
    c = _cluster()
    c["_controller"] = bm.build_controller(c)
    calls = []

    def fake_run(controller, args, check=True, timeout=None):
        calls.append(list(args))
        if args[0] == "get" and "jsonpath" in args[-1]:
            return ("mcbench", 0)
        if args[0] == "get":
            return ("", 0)
        return ("", 0)

    monkeypatch.setattr(bm, "STATE_FILE", str(tmp_path / ".state"))
    monkeypatch.setattr(bm, "controller_run", fake_run)
    with open(bm.STATE_FILE, "w", encoding="utf-8") as f:
        f.write('{"type":"k8s-namespace","controller":"127.0.0.1",'
                '"context":"","namespace":"mc-test","ts":"old"}\n')
    bm.reap_k8s_namespaces(c)
    assert any(a[:2] == ["delete", "namespace"] for a in calls)
    assert open(bm.STATE_FILE).read().strip() == ""


if __name__ == "__main__":
    import inspect
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            sig = inspect.signature(fn)
            if len(sig.parameters) == 0:
                fn()
    print("PASS test_k8s_config")


def test_k8s_prerun_skip_when_absent(tmp_path, monkeypatch):
    """No script file -> k8s_prerun silently returns (no error, no log)."""
    c = _cluster()
    c["_controller"] = bm.build_controller(c)
    monkeypatch.setattr(bm, "K8S_PRERUN", str(tmp_path / "nope.sh"))
    bm.k8s_prerun(c, dry_run=False)  # should not raise


def test_k8s_prerun_failure_aborts(tmp_path, monkeypatch):
    """Script returns non-zero -> RuntimeError."""
    c = _cluster()
    c["_controller"] = bm.build_controller(c)
    script = tmp_path / "k8s-prerun.sh"
    script.write_text("#!/bin/bash\nexit 1\n")
    monkeypatch.setattr(bm, "K8S_PRERUN", str(script))
    monkeypatch.setattr(bm, "controller_put", lambda *a, **k: None)
    monkeypatch.setattr(bm, "controller_exec", lambda c, cmd: ("fail", 1))
    try:
        bm.k8s_prerun(c, dry_run=False)
        raise AssertionError("should have raised")
    except RuntimeError as exc:
        assert "pre-run" in str(exc)


def test_k8s_prerun_success(tmp_path, monkeypatch):
    """Script returns 0 -> no exception."""
    c = _cluster()
    c["_controller"] = bm.build_controller(c)
    script = tmp_path / "k8s-prerun.sh"
    script.write_text("#!/bin/bash\nexit 0\n")
    monkeypatch.setattr(bm, "K8S_PRERUN", str(script))
    monkeypatch.setattr(bm, "controller_put", lambda *a, **k: None)
    monkeypatch.setattr(bm, "controller_exec", lambda c, cmd: ("ok", 0))
    bm.k8s_prerun(c, dry_run=False)  # should not raise
