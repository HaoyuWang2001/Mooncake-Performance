#!/usr/bin/env python3
"""本地绘图（plot_latency.py / benchmark.py auto-plot）的回归测试。

绘图在编排器本机执行；OpenBLAS 按线程数预分配缓冲区，在 Windows 提交
内存紧张/32 位 Python 下会以 "Memory allocation still failed" 崩溃，
因此绘图进程必须限制 BLAS/OpenMP 线程数。
"""
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


def _load_plot_module():
    pytest.importorskip("matplotlib")
    import importlib.util

    spec = importlib.util.spec_from_file_location(
        "plot_latency_for_test", os.path.join(ROOT, "scripts", "plot_latency.py"))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_plot_script_caps_blas_threads_before_numpy():
    """线程上限必须在 import numpy/matplotlib 之前设置（numpy 导入即分配）。"""
    src = _read("scripts/plot_latency.py")
    for var in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS",
                "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        assert var in src
    assert "setdefault" in src
    cap_pos = src.index("OPENBLAS_NUM_THREADS")
    numpy_pos = src.index("import numpy")
    pyplot_pos = src.index("import matplotlib.pyplot")
    assert cap_pos < numpy_pos, "BLAS thread caps must precede import numpy"
    assert cap_pos < pyplot_pos, "BLAS thread caps must precede matplotlib"


def test_auto_plot_sets_env_and_runs_locally():
    """benchmark.py 自动绘图：本机 sys.executable 子进程 + 线程上限 env。"""
    src = _read("benchmark.py")
    assert "plotting locally on this machine" in src
    for var in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS",
                "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        assert var in src
    assert "env=plot_env" in src
    assert "[sys.executable, plot_script, \"--log\", log_dir]" in src


def test_plot_file_stem_removes_pipeline_pod_but_keeps_unique_client_id():
    pl = _load_plot_module()
    pod = "mc-20261112-1429-rank-recall-recall-recall-smoke-client-c52d4w2"
    base = "recall_smoke.benchmark_c1_t1_3.5MB"

    assert pl._plot_file_stem(
        f"{base}.dual_{pod}_c0_p9301.reader"
    ) == f"{base}.dual_c0_p9301.reader"
    assert pl._plot_file_stem(
        f"{base}.dual_{pod}_c1_p9301.writer"
    ) == f"{base}.dual_c1_p9301.writer"
    assert pl._plot_file_stem(
        f"{base}.reader_{pod}_r2_p9201"
    ) == f"{base}.reader_r2_p9201"
    assert pl._plot_file_stem(
        f"{base}.writer_{pod}_w3"
    ) == f"{base}.writer_w3"
    assert pl._plot_file_stem("basic.benchmark_c1_t1_8MB") == \
        "basic.benchmark_c1_t1_8MB"


def test_plot_file_stem_avoids_regression_windows_max_path():
    pl = _load_plot_module()
    out_dir = (r"D:\Benchmark\CodeHub\Mooncake-Performance-main\Mooncake-Performance"
               r"\outputs\20261112-1429-rank-recall\recall\logs\recall_smoke")
    old_stem = (
        "recall_smoke.benchmark_c1_t1_3.5MB.dual_"
        "mc-20261112-1429-rank-recall-recall-recall-smoke-client-c52d4w2_"
        "c0_p9301.reader"
    )
    old_path = os.path.join(out_dir, old_stem + ".queuewait.png")
    new_path = os.path.join(
        out_dir, pl._plot_file_stem(old_stem) + ".queuewait.png")

    # Windows joins with "\\" (the incident path is exactly 260 chars);
    # POSIX CI adds one "/" after the drive-like prefix and sees 261.
    assert len(old_path) >= 260
    assert len(new_path) < 260
    assert "client-c52d4w2" not in new_path
