#!/usr/bin/env python3
"""本地绘图（plot_latency.py / benchmark.py auto-plot）的回归测试。

绘图在编排器本机执行；OpenBLAS 按线程数预分配缓冲区，在 Windows 提交
内存紧张/32 位 Python 下会以 "Memory allocation still failed" 崩溃，
因此绘图进程必须限制 BLAS/OpenMP 线程数。
"""
import os

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))


def _read(rel):
    with open(os.path.join(ROOT, rel), encoding="utf-8") as f:
        return f.read()


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
