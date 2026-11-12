#!/usr/bin/env python3
"""③ k8s hostPath 日志落盘：模板渲染、per-pod 子目录、拉取 glob。"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

import benchmark as bm   # noqa: E402


CLUSTER = {
    "controller": {"ip": "10.0.0.1"},
    "k8s": {"namespace": "mooncake-bench", "image": "img:1",
            "controller_workspace": "/tmp/cw", "pod_workspace": "/tmp/pw",
            "template": os.path.join(
                ROOT, "configs", "_k8s", "templates", "benchmark-pod.yaml.tpl")},
}


def test_host_log_dir_default_and_override():
    assert bm.host_log_dir(CLUSTER, "20261112-1000") == \
        "/var/lib/mooncake-bench/20261112-1000"
    c2 = {"k8s": dict(CLUSTER["k8s"], host_log_root="/data/mclogs")}
    assert bm.host_log_dir(c2, "t1") == "/data/mclogs/t1"
    # trailing slash normalized
    c3 = {"k8s": dict(CLUSTER["k8s"], host_log_root="/data/mclogs/")}
    assert bm.host_log_dir(c3, "t1") == "/data/mclogs/t1"


def test_pod_base_ssh_vs_k8s():
    base = "/remote/logs/20261112-1000/rank_smoke.benchmark_c1_t1_8MB"
    # ssh node: no _pod -> flat layout unchanged
    assert bm._pod_base(base, {"ip": "1.2.3.4"}) == base
    assert bm._pod_base(base, None) == base
    # k8s pod: pod subdirectory inserted before the basename
    pod = {"ip": "1.2.3.4", "_pod": "mc-x-client-abc"}
    assert bm._pod_base(base, pod) == \
        "/remote/logs/20261112-1000/mc-x-client-abc/" \
        "rank_smoke.benchmark_c1_t1_8MB"


def test_pod_template_renders_hostpath(tmp_path):
    pod, run_label, text = bm._render_pod_yaml(
        CLUSTER, "20261112-1000", "rank_smoke")
    assert "hostPath" in text and "DirectoryOrCreate" in text
    assert "/var/lib/mooncake-bench/20261112-1000" in text
    assert "/tmp/pw/logs/20261112-1000" in text
    assert "{{MC_" not in text


def test_deployment_template_renders_hostpath(tmp_path):
    cl = dict(CLUSTER)
    cl["k8s"] = dict(CLUSTER["k8s"])
    cl["k8s"]["deployment_template"] = os.path.join(
        ROOT, "configs", "_k8s", "templates", "benchmark-deployment.yaml.tpl")
    deploy, run_label, text = bm._render_deployment_yaml(
        cl, "20261112-1000", "rank_smoke", "client", 32)
    assert "hostPath" in text and "DirectoryOrCreate" in text
    assert "/var/lib/mooncake-bench/20261112-1000" in text
    assert "/tmp/pw/logs/20261112-1000" in text
    assert "{{MC_" not in text


def test_run_stage_pods_cope_log_path(monkeypatch, tmp_path):
    """run_stage 的日志路径：k8s 节点带 <pod>/ 子目录，ssh 保持平铺。"""
    commands = []

    def fake_run_ssh_progress(host, node, command, label=None, dry_run=False,
                              **kw):
        commands.append((command, dry_run))

    monkeypatch.setattr(bm, "run_ssh_with_progress", fake_run_ssh_progress)
    monkeypatch.setattr(bm, "_check_stop", lambda: None)
    k8s_node = {"ip": "1.2.3.4", "_pod": "mc-pod-1"}
    bm.run_stage("1.2.3.4", k8s_node, "runner.sh", "remove-keys",
                 "/tmp/pw/logs/20261112-1000", "rank_smoke",
                 extra="--scope full", dry_run=False)
    cmd = commands[0][0]
    assert ("/tmp/pw/logs/20261112-1000/mc-pod-1/"
            "rank_smoke.remove-keys.log") in cmd

    commands.clear()
    ssh_node = {"ip": "1.2.3.4"}
    bm.run_stage("1.2.3.4", ssh_node, "runner.sh", "remove-keys",
                 "/remote/logs/20261112-1000", "rank_smoke",
                 extra="--scope full", dry_run=False)
    cmd = commands[0][0]
    assert ("/remote/logs/20261112-1000/"
            "rank_smoke.remove-keys.log") in cmd
    assert "mc-pod-1" not in cmd


def test_pull_logs_k8s_pod_glob(monkeypatch, tmp_path):
    """pull_logs：k8s 节点的 glob 指向 <pod>/ 子目录。"""
    globs = []

    def fake_pull(host, node, remote_glob, local, dry_run=False):
        globs.append(remote_glob)

    monkeypatch.setattr(bm, "scp_pull", fake_pull)
    monkeypatch.setattr(bm, "get_node", lambda c, n: {"ip": "1.2.3.4",
                                                      "_pod": "mc-pod-9"})
    bm.pull_logs({}, ["n1"], "/tmp/pw/logs/20261112-1000", "rank_smoke",
                 str(tmp_path), False)
    assert globs == ["/tmp/pw/logs/20261112-1000/mc-pod-9/rank_smoke.*"]

    globs.clear()
    monkeypatch.setattr(bm, "get_node", lambda c, n: {"ip": "1.2.3.4"})
    bm.pull_logs({}, ["n1"], "/remote/logs/20261112-1000", "rank_smoke",
                 str(tmp_path), False)
    assert globs == ["/remote/logs/20261112-1000/rank_smoke.*"]


def test_cleanup_mentions_host_log_root():
    src = open(os.path.join(ROOT, "benchmark.py"), encoding="utf-8").read()
    assert "宿主机日志留存于各节点" in src
    assert "CLEAN-UP 不删除" in src
