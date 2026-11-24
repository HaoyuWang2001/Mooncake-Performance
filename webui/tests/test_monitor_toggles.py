#!/usr/bin/env python3
"""WebUI 监控开关（内存 / URMA 端口）→ argv → profile 矩阵覆盖。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import benchmark as bm  # noqa: E402
from webui.backend.routers import runs as runs_mod  # noqa: E402


def _build(flags, mode="ssh", launch_mode="single"):
    body = {"selector": {"files": ["some/case.json"]},
            "flags": dict(flags, mode=mode, launch_mode=launch_mode)}
    # keep the test independent of which lab configs exist on the host
    orig_isfile, orig_isdir = runs_mod.os.path.isfile, runs_mod.os.path.isdir
    runs_mod.os.path.isfile = lambda p: True
    runs_mod.os.path.isdir = lambda p: True
    import scripts.run_rank_recall as rr
    orig_part = rr.partition_configs
    rr.partition_configs = lambda dirs, files: ({}, {})
    try:
        return runs_mod._build_run_argv(body)
    finally:
        runs_mod.os.path.isfile = orig_isfile
        runs_mod.os.path.isdir = orig_isdir
        rr.partition_configs = orig_part


def test_webui_sends_both_monitor_flags():
    argv = _build({"monitor_mem": True, "monitor_urma": False})
    assert "--monitor-mem" in argv and argv[argv.index("--monitor-mem") + 1] == "on"
    assert "--monitor-urma" in argv and argv[argv.index("--monitor-urma") + 1] == "off"


def test_webui_dual_mode_forwards_monitors():
    argv = _build({"monitor_mem": False, "monitor_urma": True,
                   "rank_cluster": "rc.json", "rank_global": "rg.json",
                   "recall_cluster": "cc.json", "recall_global": "cg.json",
                   "profile": "p.json"},
                  mode="k8s", launch_mode="rank_recall")
    assert argv[1].endswith("run_rank_recall.py")
    assert argv[argv.index("--monitor-mem") + 1] == "off"
    assert argv[argv.index("--monitor-urma") + 1] == "on"


def test_direct_cli_without_flags_keeps_profile():
    argv = _build({})
    assert "--monitor-mem" not in argv and "--monitor-urma" not in argv


def _matrix(mem=True, urma=False):
    return {"tools": {"mem": {"enabled": mem},
                      "urma": {"enabled": urma},
                      "perf": {"enabled": False}},
            "trigger": "whole_run"}


class _Args:
    def __init__(self, mem=None, urma=None):
        self.monitor_mem = mem
        self.monitor_urma = urma


def test_override_turns_both_off_disables_profiling():
    assert bm._apply_monitor_overrides(_matrix(), _Args("off", "off")) is None


def test_override_can_swap_mem_for_urma():
    m = bm._apply_monitor_overrides(_matrix(mem=True, urma=False),
                                    _Args("off", "on"))
    assert m is not None
    assert m["tools"]["mem"]["enabled"] is False
    assert m["tools"]["urma"]["enabled"] is True


def test_override_none_leaves_matrix_untouched():
    m = bm._apply_monitor_overrides(_matrix(mem=True), _Args())
    assert m["tools"]["mem"]["enabled"] is True
    assert m["tools"]["urma"]["enabled"] is False


def test_override_on_without_tools_stays_off():
    assert bm._apply_monitor_overrides(None, _Args("on", None)) is None
