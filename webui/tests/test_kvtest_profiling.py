#!/usr/bin/env python3
"""kvtest profiling lifecycle (G1): process-name retarget + wiring contract.

The full scenario needs a live cluster, so the wiring is asserted structurally
against run_kvtest_scenario's source (same style as the other contract tests in
this suite); the two pure helpers are tested directly."""
import inspect
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import benchmark as bm


def test_monitor_proc_pattern_retargeted_to_kvtest_binary():
    matrix = {"tools": {"mem": {"enabled": True,
                                "proc_pattern": "mooncake_benchmark_client"},
                        "urma": {"enabled": False}}}
    out = bm._kvtest_monitor_matrix(matrix, "/opt/bin/mooncake_kvtest")
    assert out is not matrix, "must copy: args._matrix is shared across configs"
    assert out["tools"]["mem"]["proc_pattern"] == "mooncake_kvtest"
    # the shared matrix stays untouched for a later basic/pipeline config
    assert matrix["tools"]["mem"]["proc_pattern"] == "mooncake_benchmark_client"


def test_monitor_proc_pattern_explicit_wins():
    matrix = {"tools": {"mem": {"enabled": True,
                                "proc_pattern": "my_custom_client"}}}
    out = bm._kvtest_monitor_matrix(matrix, "mooncake_kvtest")
    assert out["tools"]["mem"]["proc_pattern"] == "my_custom_client"

    empty = {"tools": {"mem": {"enabled": True, "proc_pattern": ""}}}
    out = bm._kvtest_monitor_matrix(empty, "mooncake_kvtest")
    assert out["tools"]["mem"]["proc_pattern"] == "mooncake_kvtest"


def test_monitor_proc_pattern_no_matrix_no_mem():
    assert bm._kvtest_monitor_matrix(None, "mooncake_kvtest") is None
    matrix = {"tools": {"urma": {"enabled": True}}}
    out = bm._kvtest_monitor_matrix(matrix, "mooncake_kvtest")
    assert "mem" not in out["tools"]


def test_kvtest_scenario_wires_profiling_lifecycle():
    src = inspect.getsource(bm.run_kvtest_scenario)
    # session state + profile matrix are read at the top
    assert "matrix = getattr(args, \"_matrix\", None)" in src
    assert "profile_sess = None" in src
    # whole_run + per_stage starts, both targeting node-level monitors
    assert src.count("prof_core.start_profiling(") == 2
    assert src.count("_profiling_targets(") == 2
    assert 'matrix.get("trigger", "whole_run") == "whole_run"' in src
    # success, STOP and failure paths all stop + collect
    assert src.count("_stop_profiling(") == 3
    stop_seg = src.split("except StopRequested:", 1)[1].split("raise", 1)[0]
    assert "_stop_profiling(" in stop_seg
    fail_seg = src.rsplit("except Exception as exc:", 1)[1]
    assert fail_seg.index("_stop_profiling(") < \
        fail_seg.index("reap_state_entries(cluster)")


def test_kvtest_scenario_retargets_monitors_before_starting():
    src = inspect.getsource(bm.run_kvtest_scenario)
    assert "_kvtest_monitor_matrix(" in src
    assert src.index("_kvtest_monitor_matrix(") < \
        src.index("prof_core.start_profiling(")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            if len(inspect.signature(fn).parameters) == 0:
                fn()
    print("PASS test_kvtest_profiling")
