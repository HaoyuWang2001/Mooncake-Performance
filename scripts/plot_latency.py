#!/usr/bin/env python3
"""plot_latency.py — 从 benchmark 客户端日志绘制时延统计图。

RESULT 行三种格式（tab 分隔，首字段为 HH:MM:SS.micro 本地墙钟时间戳）：
  basic/mixed : RESULT  <ts>  <op>  <key>  <lat_ms>  <status>                       (6 字段)
  pipeline_w  : RESULT  <ts>  pipeline_write  <key>  <lat_ms>  <status>  <kc>       (7 字段)
  pipeline_r  : RESULT  <ts>  pipeline_read  <key>  <lat_ms>  <status>  <qw>  <kc>  (8 字段)
旧格式（无时间戳，op 直接跟在 RESULT 后）同样支持——自动回退为 request#i 横轴。

仅 OK 样本进入主序列。pipeline_read 日志额外绘制 queue_wait（第 6 数据列）统计图。
存在离群值时每个统计量出两幅图：原名图（包含离群值，红 x 标注）+ .trimmed.png
（剔除离群值，分位线按剔除后数据重算）。

用法：
  python plot_latency.py --log <日志文件|目录> [--out DIR] [--outlier MS] [--type both]
    --log      单个日志文件，或目录（递归扫描其中含 RESULT 行的 *.log，逐文件出图）
    --out      输出目录，缺省 = 日志所在目录（即 outputs/<ts>/logs/<配置名>/）
    --outlier  离群阈值 ms；缺省自动 = p99 × 10 取整（p99 不会被尾部离群值自身抬高）
    --type     both（缺省）| latency | queue-wait

输出：<日志主干>.latency.png[.trimmed] / <日志主干>.queuewait.png[.trimmed]（dpi 150）。
横轴：有时间戳 → 下轴时间线（HH:MM:SS）+ 顶轴 request#i；无时间戳 → request#i。
benchmark.py 主流程结束后会自动以 --log <本次 run 的 logs/> 调用本脚本
（--no-plot 关闭）。依赖：matplotlib（pip install matplotlib）。
"""
import argparse
import os
import re
import sys
from datetime import datetime, timedelta

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
except ImportError:
    sys.exit("[ERROR] matplotlib is required: pip install matplotlib")

STAT_HLINES_ALWAYS = [("avg", "tab:blue"), ("max", "tab:red")]
# gated lines: only drawn when the sample count makes the percentile
# meaningful (a percentile needs at least its reciprocal sample count)
STAT_HLINES_GATED = [("p99", "tab:orange", 100), ("p999", "tab:green", 1000),
                     ("p9999", "tab:purple", 10000)]
REF_LINES = [(1.0, "1ms"), (2.0, "2ms")]
TS_PREFIX = re.compile(r"^\d{2}:\d{2}:\d{2}")


def percentile(sorted_vals, q):
    if not sorted_vals:
        return 0.0
    return sorted_vals[int(round(q * (len(sorted_vals) - 1)))]


def ts_to_seconds(ts):
    """'HH:MM:SS.micro' -> seconds since midnight (float)."""
    h, m, rest = ts.split(":")
    return int(h) * 3600 + int(m) * 60 + float(rest)


def parse_result_file(path):
    """Return (lats, qws, times, counts) from one log file.

    lats/qws are OK-only sample lists; times is the matching wall-clock list
    in seconds-since-midnight (midnight wraparound folded forward), or None
    when the log predates timestamps. status_counts maps status -> line count.
    """
    lats, qws, times, counts = [], [], [], {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line.startswith("RESULT\t"):
                continue
            parts = line.rstrip("\n").split("\t")
            off = 1 if len(parts) > 1 and TS_PREFIX.match(parts[1]) else 0
            if len(parts) < 5 + off:
                continue
            op = parts[1 + off]
            if op == "pipeline_notify":
                continue  # notify 失败诊断行，无时延样本
            status = parts[4 + off]
            counts[status] = counts.get(status, 0) + 1
            if status != "OK":
                continue
            try:
                lats.append(float(parts[3 + off]))
            except (ValueError, IndexError):
                continue
            if off:
                times.append(ts_to_seconds(parts[1]))
            if len(parts) > 6 + off and op == "pipeline_read":
                try:
                    qws.append(float(parts[5 + off]))
                except ValueError:
                    pass
    if lats and times and len(times) != len(lats):
        times = None  # 混合行（部分带 ts）不可靠 → 回退 index 轴
    if times:
        # 多线程 flush 序非严格单调：按时间排序；跨午夜回绕折叠为单调
        order = sorted(range(len(lats)), key=lambda i: times[i])
        lats = [lats[i] for i in order]
        if qws and len(qws) == len(lats):
            qws = [qws[i] for i in order]
        folded = []
        prev = None
        for t in sorted(times):
            if prev is not None and t < prev:
                t += 86400
            folded.append(t)
            prev = t
        times = folded
    else:
        times = None
    return lats, qws, times, counts


def _fmt_hms(seconds, base):
    """Relative seconds -> absolute HH:MM:SS string (base = run start t0)."""
    t = datetime(2000, 1, 1) + timedelta(seconds=base + seconds)
    return t.strftime("%H:%M:%S")


def plot_series(vals, times, title, out_png, outlier_ms, include_outliers,
                ylabel="Latency (ms)"):
    """Dot-line plot with stat hlines, absolute reference lines and outliers.

    times: wall-clock seconds per sample (None -> request#i axis). When
    include_outliers is False the sample list must already be trimmed.
    """
    n = len(vals)
    fig, ax = plt.subplots(figsize=(12, 6))
    if times:
        t0 = times[0]
        xs = [t - t0 for t in times]  # 相对秒（底部刻度再渲染回绝对 HH:MM:SS）
    else:
        t0 = None
        xs = list(range(n))
    if include_outliers:
        main = [(x, v) for x, v in zip(xs, vals) if v <= outlier_ms]
        outliers = [(x, v) for x, v in zip(xs, vals) if v > outlier_ms]
    else:
        main = list(zip(xs, vals))
        outliers = []
    if main:
        ax.plot([x for x, _ in main], [v for _, v in main],
                linewidth=0.5, markersize=2, marker=".", color="tab:gray",
                label=f"samples (n={len(main)})")
    if outliers:
        ax.scatter([x for x, _ in outliers], [v for _, v in outliers],
                   s=28, marker="x", color="red", zorder=5,
                   label=f"outliers ({len(outliers)}, >{outlier_ms:g}ms)")
    stat_vals = sorted(vals)
    computed = {"avg": sum(stat_vals) / n, "max": stat_vals[-1],
                "p99": percentile(stat_vals, 0.99),
                "p999": percentile(stat_vals, 0.999),
                "p9999": percentile(stat_vals, 0.9999)}
    for name, color in STAT_HLINES_ALWAYS:
        v = computed[name]
        ax.axhline(v, linestyle="--", linewidth=1.2, color=color)
        ax.annotate(f"{name}={v:.4f}", xy=(0.998, v), xycoords=("axes fraction", "data"),
                    ha="right", va="bottom", fontsize=9, color=color)
    for name, color, min_n in STAT_HLINES_GATED:
        if n < min_n:
            continue  # 样本量不达标：分位数无意义，不加线
        v = computed[name]
        ax.axhline(v, linestyle="--", linewidth=1.2, color=color)
        ax.annotate(f"{name}={v:.4f}", xy=(0.998, v), xycoords=("axes fraction", "data"),
                    ha="right", va="bottom", fontsize=9, color=color)
    for v, label in REF_LINES:
        if max(vals) >= v:
            ax.axhline(v, linestyle="-.", linewidth=1.0, color="gray", alpha=0.8)
            ax.annotate(label, xy=(0.003, v), xycoords=("axes fraction", "data"),
                        ha="left", va="bottom", fontsize=8, color="gray")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(loc="upper left", fontsize=9)
    if times:
        ax.set_xlabel("time (HH:MM:SS)")
        ax.xaxis.set_major_formatter(
            plt.FuncFormatter(lambda x, _: _fmt_hms(x, t0)))
        # 顶部次轴：同一数据点的 request 序号（时间→序号插值，向量化）
        sec = ax.secondary_xaxis(
            "top", functions=(
                lambda t, _xt=np.array(xs), _xi=np.arange(len(vals)):
                np.interp(t, _xt, _xi),
                lambda i, _xt=np.array(xs), _xi=np.arange(len(vals)):
                np.interp(i, _xi, _xt)))
        sec.set_xlabel("request #i")
    else:
        ax.set_xlabel("request #i")
    ax.set_xlim(min(xs) if times else 0, max(xs))
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def _emit_plots(stem, kind, vals, times, out_dir, outlier_ms, note):
    """Write the latency/queue-wait figure(s): full view always; trimmed view
    additionally when outliers exist (stats recomputed on trimmed data)."""
    suffix = "latency" if kind == "latency" else "queuewait"
    ylabel = "Latency (ms)" if kind == "latency" else "Queue wait (ms)"
    base_title = f"{stem}  ({note})"
    trimmed = [v for v in vals if v <= outlier_ms]
    has_out = len(trimmed) != len(vals)
    png = os.path.join(out_dir, f"{stem}.{suffix}.png")
    plot_series(vals, times, base_title, png, outlier_ms, include_outliers=True,
                ylabel=ylabel)
    print(f"  -> {png}")
    if has_out:
        png = os.path.join(out_dir, f"{stem}.{suffix}.trimmed.png")
        plot_series(trimmed, _trim_times(times, vals, outlier_ms),
                    f"{stem}  ({note}, outliers removed)", png, outlier_ms,
                    include_outliers=False, ylabel=ylabel)
        print(f"  -> {png}  (trimmed, {len(vals) - len(trimmed)} outliers excluded)")


def _trim_times(times, vals, threshold):
    if not times:
        return None
    return [t for t, v in zip(times, vals) if v <= threshold]


def process_file(path, args):
    lats, qws, times, counts = parse_result_file(path)
    out_dir = args.out or os.path.dirname(os.path.abspath(path))
    stem = os.path.splitext(os.path.basename(path))[0]
    status_note = "  ".join(f"{k}:{v}" for k, v in sorted(counts.items())) or "no RESULT"
    print(f"[{os.path.basename(path)}] RESULT status: {status_note}, OK-samples: {len(lats)}"
          + (f", queue_wait: {len(qws)}" if qws else "")
          + ("" if times else "  [no timestamps -> request#i axis]"))
    if not lats:
        print("  skip (no OK samples)")
        return
    if not args.outlier:
        # auto = p99 x 10：p99 位次在尾部之外，不会被离群值自身抬高
        # （p999 在小样本时会落在离群点上导致阈值自污染）
        threshold = max(round(percentile(sorted(lats), 0.99) * 10), 1)
    else:
        threshold = args.outlier
    print(f"  outlier threshold: {threshold}ms "
          f"({'auto = p99 x 10' if not args.outlier else 'explicit'})")
    if args.type in ("both", "latency"):
        _emit_plots(stem, "latency", lats, times, out_dir, threshold,
                    f"{len(lats)} OK samples")
    if qws and args.type in ("both", "queue-wait"):
        # queue_wait 阈值独立按自身分布计算，避免被时延离群值牵连
        qw_threshold = threshold if args.outlier else \
            max(round(percentile(sorted(qws), 0.99) * 10), 1)
        _emit_plots(stem, "queue-wait", qws, times, out_dir, qw_threshold,
                    f"{len(qws)} samples")


def collect_logs(path):
    if os.path.isfile(path):
        return [path]
    found = []
    for root, _dirs, files in os.walk(path):
        for f in sorted(files):
            if f.endswith(".log"):
                found.append(os.path.join(root, f))
    return found


def main(argv=None):
    parser = argparse.ArgumentParser(description="Plot latency stats from RESULT log lines")
    parser.add_argument("--log", required=True,
                        help="log file or directory (recursive *.log scan)")
    parser.add_argument("--out", default=None,
                        help="output dir; default = each log's own directory")
    parser.add_argument("--outlier", type=float, default=None,
                        help="outlier threshold in ms; default auto = p99 x 10")
    parser.add_argument("--type", choices=["both", "latency", "queue-wait"],
                        default="both")
    args = parser.parse_args(argv)
    if not os.path.exists(args.log):
        sys.exit(f"[ERROR] log path not found: {args.log}")
    logs = collect_logs(args.log)
    if not logs:
        sys.exit(f"[ERROR] no *.log under {args.log}")
    for path in logs:
        process_file(path, args)
    print("done")


if __name__ == "__main__":
    main()
