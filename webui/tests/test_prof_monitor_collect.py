#!/usr/bin/env python3
"""prof monitor 采集时序 + controller 数据面 import 面守卫。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import benchmark as bm  # noqa: E402
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.dirname(os.path.abspath(__file__)))), "profiling"))
import prof_core  # noqa: E402


def test_controller_get_file_is_imported():
    """真机回归：_kvtest_controller_collect 依赖 controller_get_file；
    utils 里定义而 benchmark 未导入时会以
    "name 'controller_get_file' is not defined" 让 controller 路径崩掉。"""
    assert callable(bm.controller_get_file)


def test_monitor_collect_waits_for_stable_before_pull(monkeypatch, tmp_path):
    order = []

    def fake_run(ip, node, cmd, quiet=False):
        if "kill -INT" in cmd:
            order.append(("kill", cmd.split()[-3] if False else "pid"))
        return ""

    def fake_wait(ip, node, out_file, sudo="", **kw):
        order.append(("stable", os.path.basename(out_file)))

    def fake_pidgone(ip, node, pid, sudo="", **kw):
        order.append(("pidgone", "pid"))

    def fake_pull(ip, node, remote_glob, local_dir, dry_run=False):
        order.append(("pull", os.path.basename(remote_glob)))

    monkeypatch.setattr(prof_core, "_run", fake_run)
    monkeypatch.setattr(prof_core, "_wait_artifact_stable", fake_wait)
    monkeypatch.setattr(prof_core, "_wait_pid_gone", fake_pidgone)
    monkeypatch.setattr(prof_core, "scp_pull", fake_pull)
    monkeypatch.setattr(prof_core, "get_node",
                        lambda cluster, name: {"ip": "1.2.3.4"})
    tool = next(iter(prof_core.MONITOR_ARTIFACTS))
    files = prof_core.MONITOR_ARTIFACTS[tool]
    session = {"tools": {"n1": {tool: "123"}},
               "matrix_tools": {tool: {}},
               "remote_out_dir": "/remote/out",
               "monitor": True}
    prof_core.stop_and_collect(None, session, str(tmp_path))
    kinds = [k for k, _n in order]
    pos = {}
    for i, (k, n) in enumerate(order):
        pos.setdefault((k, n), i)
    # kill first, then ONE stability wait per artifact, then the pulls —
    # never a pull before its artifact stabilized (that causes the live-CSV
    # "remote > local" size-mismatch WARNs)
    assert kinds[0] == "kill"
    # the sampler must be dead before any pull (the old size-stability-only
    # wait left it alive ~70s per pod and raced the copy)
    assert kinds.index("kill") < kinds.index("pidgone")
    assert kinds.index("pidgone") < kinds.index("stable")
    assert kinds.index("pidgone") < kinds.index("pull")
    for f in files:
        assert ("pull", f) in pos, "no pull for {}".format(f)
        assert pos[("pidgone", "pid")] < pos[("pull", f)]


def _patch_collect(monkeypatch, nodes, display=None):
    """Fake the collect primitives; returns the set of local dir names pulled."""
    import benchmark as bm
    pulled = set()

    def fake_pull(ip, node, remote, local_dir, dry_run=False):
        pulled.add(os.path.basename(local_dir))

    monkeypatch.setattr(prof_core, "_run", lambda *a, **k: "")
    monkeypatch.setattr(prof_core, "_wait_pid_gone", lambda *a, **k: None)
    monkeypatch.setattr(prof_core, "_wait_artifact_stable",
                        lambda *a, **k: None)
    monkeypatch.setattr(prof_core, "scp_pull", fake_pull)
    monkeypatch.setattr(prof_core, "get_node", lambda c, name: nodes[name])
    monkeypatch.setattr(bm, "state_remove", lambda *a, **k: None)
    if display is not None:
        monkeypatch.setattr(bm, "_node_display",
                            lambda cluster, n, name: display)
    return pulled


def test_stop_and_collect_uses_node_ip_dirs(tmp_path, monkeypatch):
    """Profiling 产物目录 = 节点显示名（k8s 宿主 IP / ssh 管理 IP）。"""
    nodes = {"n1": {"ip": "10.0.0.1"}, "n2": {"ip": "10.0.0.2"}}
    pulled = _patch_collect(monkeypatch, nodes)
    session = {"tools": {"n1": {"mem": "11"}, "n2": {"mem": "22"}},
               "matrix_tools": {"mem": {}}, "remote_out_dir": "/remote/out"}
    prof_core.stop_and_collect({"nodes": nodes}, session, str(tmp_path))
    assert pulled == {"10.0.0.1", "10.0.0.2"}


def test_stop_and_collect_display_collision_falls_back(tmp_path, monkeypatch):
    """显示名冲突（异常配置）时回退原名，绝不互相覆盖。"""
    nodes = {"n1": {"ip": "10.0.0.9"}, "n2": {"ip": "10.0.0.9"}}
    pulled = _patch_collect(monkeypatch, nodes, display="dup-host")
    session = {"tools": {"n1": {"mem": "11"}, "n2": {"mem": "22"}},
               "matrix_tools": {"mem": {}}, "remote_out_dir": "/remote/out"}
    prof_core.stop_and_collect({"nodes": nodes}, session, str(tmp_path))
    assert len(pulled) == 2, pulled
    assert "dup-host" in pulled


def test_spdiag_show_uses_per_pod_cfg_dir(tmp_path, monkeypatch):
    """k8s: spdiag show 落到 <log>/<pod>/profiling/<cfg>/spdiag.show.txt
    （同节点多 pod 不互相覆盖）；ssh 目标保持原路径；收集名不变。"""
    import benchmark as bm
    shows, pulls = {}, {}

    def fake_run(ip, node, cmd, quiet=True):
        if " show > " in cmd:
            shows[node.get("_pod") or "ssh"] = cmd
        return ""

    def fake_pull(ip, node, remote, local_dir, dry_run=False):
        pulls[node.get("_pod") or "ssh"] = remote

    nodes = {"pod-a": {"ip": "10.0.0.1", "_pod": "pod-a", "_monitor": False},
             "host-b": {"ip": "10.0.0.2"}}
    monkeypatch.setattr(prof_core, "_run", fake_run)
    monkeypatch.setattr(prof_core, "scp_pull", fake_pull)
    monkeypatch.setattr(prof_core, "get_node", lambda c, name: nodes[name])
    monkeypatch.setattr(bm, "state_remove", lambda *a, **k: None)
    session = {"tools": {"pod-a": {"spdiag": None},
                         "host-b": {"spdiag": None}},
               "matrix_tools": {"spdiag": {"bin": "spdiag", "sudo": False}},
               "remote_out_dir": "/pw/logs/ts/profiling/cfg1",
               "cfg": "cfg1"}
    prof_core.stop_and_collect({"nodes": nodes}, session, str(tmp_path))
    assert "/pw/logs/ts/pod-a/profiling/cfg1/spdiag.show.txt" in \
        shows["pod-a"], shows
    assert pulls["pod-a"].endswith(
        "/pw/logs/ts/pod-a/profiling/cfg1/spdiag.show.txt")
    assert "/pw/logs/ts/profiling/cfg1/spdiag.show.txt" in shows["ssh"], shows
    assert "/pod-a/" not in shows["ssh"]
    assert pulls["ssh"].endswith(
        "/pw/logs/ts/profiling/cfg1/spdiag.show.txt")


def test_collect_console_fold_one_summary_line(tmp_path, capsys, monkeypatch):
    """通用聚合规则落到 profile collect：console 只有 1 条结构化汇总
    （mem.*×N, monitor.config.json×N），零逐文件 collected 行；
    benchmark.log 保留全部原始行 + 汇总行。"""
    import utils
    nodes = {"n1": {"ip": "10.0.0.1"}, "n2": {"ip": "10.0.0.2"}}
    pulled = _patch_collect(monkeypatch, nodes)
    logf = tmp_path / "benchmark.log"
    utils.set_detail_log(str(logf))
    try:
        session = {"tools": {"n1": {"mem": "11"}, "n2": {"mem": "22"}},
                   "matrix_tools": {"mem": {}}, "remote_out_dir": "/remote/out"}
        prof_core.stop_and_collect({"nodes": nodes}, session, str(tmp_path))
        assert pulled == {"10.0.0.1", "10.0.0.2"}
        out = capsys.readouterr().out
        # one summary line, zero per-file lines
        collect_lines = [l for l in out.splitlines() if "] collected" in l]
        per_file = [l for l in out.splitlines()
                    if "collected n" in l and "files from" not in l]
        assert len(collect_lines) == 1, out
        assert not per_file, out
        import re
        assert re.search(r"\[profile\] collected 8 files from 2 nodes: "
                         r"mem\.\*×2, monitor\.config\.json×2", out), out
        # benchmark.log keeps all 8 raw lines verbatim + the summary digest
        text = logf.read_text(encoding="utf-8")
        for node in ("n1", "n2"):
            for f in ("mem.host.csv", "mem.proc.csv", "mem.err.log",
                      "monitor.config.json"):
                assert "collected {}/{}".format(node, f) in text, \
                    "benchmark.log lost a raw line: {}/{}".format(node, f)
        assert "collected 8 files from 2 nodes" in text
    finally:
        utils.close_detail_log()


def test_collect_failure_warn_stays_on_console(tmp_path, capsys, monkeypatch):
    """单个文件拉取失败：[WARN] 完整立即上屏（绝不折叠），汇总行退化为
    逐文件计数并带错误计数。"""
    import utils
    nodes = {"n1": {"ip": "10.0.0.1"}, "n2": {"ip": "10.0.0.2"}}

    def fake_pull(ip, node, remote, local_dir, dry_run=False):
        fname = os.path.basename(remote)
        if ip == "10.0.0.2" and fname == "mem.proc.csv":
            raise RuntimeError("scp size mismatch")
        pulled.add(os.path.basename(local_dir))

    pulled = set()
    monkeypatch.setattr(prof_core, "_run", lambda *a, **k: "")
    monkeypatch.setattr(prof_core, "_wait_pid_gone", lambda *a, **k: None)
    monkeypatch.setattr(prof_core, "_wait_artifact_stable",
                        lambda *a, **k: None)
    monkeypatch.setattr(prof_core, "scp_pull", fake_pull)
    monkeypatch.setattr(prof_core, "get_node", lambda c, name: nodes[name])
    import benchmark as bm
    monkeypatch.setattr(bm, "state_remove", lambda *a, **k: None)
    logf = tmp_path / "benchmark.log"
    utils.set_detail_log(str(logf))
    try:
        session = {"tools": {"n1": {"mem": "11"}, "n2": {"mem": "22"}},
                   "matrix_tools": {"mem": {}}, "remote_out_dir": "/remote/out"}
        prof_core.stop_and_collect({"nodes": nodes}, session, str(tmp_path))
        out = capsys.readouterr().out
        assert "[WARN] [profile] collect n2/mem.proc.csv: scp size mismatch" in out
        import re
        m = re.search(r"\[profile\] collected 7 files from 2 nodes: "
                      r"mem\.\*: (.+), monitor\.config\.json×2"
                      r"; 1 collect error \(see \[WARN\] above\)", out)
        assert m, out
        assert "mem.host.csv×2, mem.proc.csv×1, mem.err.log×2" == m.group(1)
        # the failed line is in benchmark.log too (with its WARN twin)
        text = logf.read_text(encoding="utf-8")
        assert "collected n2/mem.proc.csv" not in text  # failed pull: no raw line
        assert "[WARN] [profile] collect n2/mem.proc.csv" in text
    finally:
        utils.close_detail_log()


def test_start_profiling_injects_epoch_base(monkeypatch, tmp_path):
    """kvtest 传入 epoch_base 时启动命令带 EPOCH_BASE；python 不传则不带。"""
    import benchmark as bm
    cmds = []

    def fake_run(ip, node, cmd, quiet=True):
        cmds.append(cmd)
        return "OK" if ("command -v" in cmd or "test -x" in cmd) else "123"

    monkeypatch.setattr(prof_core, "_run", fake_run)
    monkeypatch.setattr(prof_core, "scp_push", lambda *a, **k: None)
    monkeypatch.setattr(bm, "state_append", lambda *a, **k: None)
    nodes = {"n1": {"ip": "10.0.0.1"}}
    monkeypatch.setattr(prof_core, "get_node", lambda c, n: nodes[n])
    matrix = {"nodes": "auto", "trigger": "whole_run",
              "tools": {"mem": {"enabled": True, "interval_s": 1,
                                "bin": "bash", "sudo": False,
                                "proc_pattern": "p"},
                        "urma": {"enabled": False}}}
    prof_core.start_profiling({"nodes": nodes}, matrix, ["n1"], "cfg",
                              "/remote/out", epoch_base=1700000000)
    launched = [c for c in cmds
                if "nohup" in c and "mem_monitor.sh" in c]
    assert launched and "EPOCH_BASE=1700000000" in launched[0], launched
    cmds.clear()
    prof_core.start_profiling({"nodes": nodes}, matrix, ["n1"], "cfg",
                              "/remote/out")
    launched = [c for c in cmds
                if "nohup" in c and "mem_monitor.sh" in c]
    assert launched and "EPOCH_BASE" not in launched[0], launched
