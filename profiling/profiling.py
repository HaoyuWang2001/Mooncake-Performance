#!/usr/bin/env python3
"""profiling.py — 手动异常值追踪 CLI（离线，对已完成的 run 目录操作）。

用法：
  python profiling/profiling.py --trace outputs/<ts> [--at HH:MM:SS.micro]
      [--threshold-ms 1.0] [--window-ms 200]

不传 --at 时：按 --threshold-ms 提取全部异常值并输出清单 + 每个异常的
时间窗切片（切片源：outputs/<ts>/profiling/ 下已收集的 bpftrace/spdiag
文本 + 同窗口的 RESULT 邻近样本）。跨节点切片受 NTP 偏差影响。
perf.data 的精确切片在节点侧用 `perf script -i perf.data --time` 执行。
"""
import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import prof_core  # noqa: E402


def _fmt_slices(slices):
    out = []
    for fname, lines in slices.items():
        out.append("----- {} ({} lines) -----".format(fname, len(lines)))
        out.extend(lines[:40])
        if len(lines) > 40:
            out.append("... ({} more)".format(len(lines) - 40))
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(description="Manual outlier tracing over a finished run")
    parser.add_argument("--trace", required=True, help="run dir, e.g. outputs/20261022-1012")
    parser.add_argument("--at", default=None, help="center time HH:MM:SS.micro (manual mode)")
    parser.add_argument("--threshold-ms", type=float, default=1.0)
    parser.add_argument("--window-ms", type=float, default=200.0)
    args = parser.parse_args(argv)

    run_dir = args.trace
    logs_dir = os.path.join(run_dir, "logs")
    prof_dir = os.path.join(run_dir, "profiling")
    if not os.path.isdir(logs_dir):
        sys.exit("[ERROR] logs dir not found: {}".format(logs_dir))

    outliers = prof_core.extract_outliers(logs_dir, args.threshold_ms)
    print("outliers (> {}ms): {}".format(args.threshold_ms, len(outliers)))
    for o in outliers:
        print("  {}  lat={:.3f}ms  key={}  status={}  [{}]".format(
            o["ts"], o["lat_ms"], o["key"], o["status"], o["file"]))

    if args.at:
        try:
            center = prof_core._ts_to_seconds(args.at)
        except ValueError:
            sys.exit("[ERROR] bad --at {!r} (use HH:MM:SS.micro)".format(args.at))
        windows = [("manual@{}".format(args.at), center)]
    elif outliers:
        windows = [(o["ts"], o["seconds"]) for o in outliers[:10]]  # top-10
    else:
        return

    if not os.path.isdir(prof_dir):
        print("(no profiling artifacts collected under {}/profiling)".format(run_dir))
        return
    report = []
    for label, center in windows:
        report.append("\n================ outlier {} (window ±{}ms) ================".format(
            label, args.window_ms))
        slices = prof_core.slice_window(prof_dir, center, args.window_ms)
        if slices:
            report.extend(_fmt_slices(slices))
        else:
            report.append("(no profiling text lines in window)")
        report.append("hint: perf.data precise slicing (node-side): "
                      "perf script -i perf.data --time <start>,<end>")
    print("\n".join(report))


if __name__ == "__main__":
    main()
