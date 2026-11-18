#!/usr/bin/env python3
"""本地绘图（plot_latency.py / benchmark.py auto-plot）的回归测试。

绘图在编排器本机执行；OpenBLAS 按线程数预分配缓冲区，在 Windows 提交
内存紧张/32 位 Python 下会以 "Memory allocation still failed" 崩溃，
因此绘图进程必须限制 BLAS/OpenMP 线程数。
"""
import os
from types import SimpleNamespace

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


def test_kvtest_samples_generate_one_request_per_point_png(tmp_path):
    pl = _load_plot_module()
    src = _read("scripts/plot_latency.py")
    assert "outputs.extend(_emit_plots(" in src
    assert 'time_axis="elapsed"' in src
    path = tmp_path / "kvtest_smoke.node_i0_p9101.kvtest_8MB.samples.csv"
    path.write_text(
        "elapsed_us,instance_id,operation,request_index,latency_ms,status,bytes\n"
        "1000,0,upsert_from,0,0.360000,OK,8388608\n"
        "2000,0,upsert_from,1,0.410000,OK,8388608\n"
        "3000,0,upsert_from,2,0.900000,ERR,0\n"
        "1500,0,notify_queue_wait,0,0.007000,OK,0\n"
        "2500,0,notify_queue_wait,1,0.009000,OK,0\n",
        encoding="utf-8")

    groups = pl.parse_kvtest_samples(str(path))
    assert [p["request_index"] for p in groups["upsert_from"]] == [0, 1, 2]
    assert groups["upsert_from"][2]["status"] == "ERR"
    outputs = pl.process_kvtest_samples(
        str(path), SimpleNamespace(out=None, type="both", outlier=None))
    assert sorted(os.path.basename(p) for p in outputs) == [
        "kvtest_smoke.node_i0_p9101.kvtest_8MB."
        "notify_queue_wait.queuewait.png",
        "kvtest_smoke.node_i0_p9101.kvtest_8MB.upsert_from.latency.png",
    ]
    assert all(os.path.getsize(p) > 0 for p in outputs)
    window = tmp_path / "kvtest_smoke.node_i0_p9101.kvtest_8MB.timeseries.csv"
    window.write_text("elapsed_ms,operation\n", encoding="utf-8")
    inputs = pl.collect_inputs(str(tmp_path))
    assert str(path) in inputs
    assert str(window) not in inputs


def test_plot_series_exposes_percentiles_in_legend(tmp_path, monkeypatch):
    pl = _load_plot_module()
    original_close = pl.plt.close
    monkeypatch.setattr(pl.plt, "close", lambda fig: None)
    try:
        # 10,000 samples make p99/p999/p9999 statistically eligible under
        # the same gating rules used by the Python RESULT plots.
        values = [0.1 + (i % 100) / 1000.0 for i in range(10000)]
        output = tmp_path / "percentiles.png"
        pl.plot_series(values, None, "stats", str(output), 10,
                       include_outliers=True)
        labels = [line.get_label() for line in pl.plt.gcf().axes[0].lines]
        assert any(label.startswith("p99=") for label in labels)
        assert any(label.startswith("p999=") for label in labels)
        assert any(label.startswith("p9999=") for label in labels)
    finally:
        original_close("all")
