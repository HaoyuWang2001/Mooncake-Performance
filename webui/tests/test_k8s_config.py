#!/usr/bin/env python3
"""k8s config/contract regression tests (no real cluster required)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import benchmark as bm


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
