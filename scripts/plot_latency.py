#!/usr/bin/env python3
"""plot_latency.py — 从 benchmark 日志/kvtest 时序 CSV 绘制时延统计图。

RESULT 行三种格式（tab 分隔，首字段为 HH:MM:SS.micro 本地墙钟时间戳）：
  basic/mixed : RESULT  <ts>  <op>  <key>  <lat_ms>  <status>                       (6 字段)
  pipeline_w  : RESULT  <ts>  pipeline_write  <key>  <lat_ms>  <status>  <kc>       (7 字段)
  pipeline_r  : RESULT  <ts>  pipeline_read  <key>  <lat_ms>  <status>  <qw>  <kc>  (8 字段)
旧格式（无时间戳，op 直接跟在 RESULT 后）同样支持——自动回退为 request#i 横轴。

仅 OK 样本进入主序列。pipeline_read 日志额外绘制 queue_wait（第 6 数据列）统计图。
存在离群值时每个统计量出两幅图：原名图（包含离群值，红 x 标注）+ .trimmed.png
（剔除离群值，分位线按剔除后数据重算）。

kvtest C++ 客户端把逐请求样本批量落到 ``*.kvtest_*.samples.csv``，脚本按
operation 绘制一请求一点的 ``.latency.png`` / ``.queuewait.png``。
自动读取同目录 kvtest_report_meta.json 的 report 区间：全程 OK 请求都绘制，
区间外点变浅，avg/P99/P999/P9999/Pmax 仅按区间内 OK 样本计算。
有统计区间时不生成剔除离群值的 trimmed 图，避免改变统计样本或遗漏启动长尾。

用法：
  python plot_latency.py --log <日志文件|目录> [--out DIR] [--outlier MS] [--type both]
    --log      单个日志/kvtest CSV，或目录（递归扫描 *.log 与
               *.samples.csv，逐文件出图）
    --out      输出目录，缺省 = 日志所在目录（即 outputs/<ts>/logs/<配置名>/）
    --outlier  离群阈值 ms；缺省自动 = p99 × 10 取整（p99 不会被尾部离群值自身抬高）
    --type     both（缺省）| latency | queue-wait

输出：<日志主干>.latency.png[.trimmed] / <日志主干>.queuewait.png[.trimmed]（dpi 150）。
pipeline 日志主干中的 Pod 名不写入图片文件名，仅保留全局唯一的 client/role
标识，避免 Windows MAX_PATH 导致出图失败。
横轴：有时间戳 → 下轴时间线（HH:MM:SS）+ 顶轴 request#i；无时间戳 → request#i。
benchmark.py 主流程结束后会自动以 --log <本次 run 的 logs/> 调用本脚本
（--no-plot 关闭）。依赖：matplotlib（pip install matplotlib）。
"""
import argparse
import csv
import json
import math
import os
import re
import sys
from datetime import datetime, timedelta

# OpenBLAS pre-allocates one buffer per thread when numpy is imported; on
# Windows that can abort with "OpenBLAS error: Memory allocation still
# failed" under a tight commit limit / 32-bit Python. Plotting needs no
# parallel BLAS, so default to one thread (must run BEFORE importing numpy;
# setdefault keeps an explicit user override).
for _var in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS",
             "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
    os.environ.setdefault(_var, "1")

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
_PIPELINE_POD_STEM_PATTERNS = (
    # dual:   ...dual_<pod>_c0_p9301.reader
    re.compile(r"^(?P<prefix>.*\.dual)_.+(?P<suffix>_c\d+_p\d+\.(?:reader|writer))$"),
    # split reader: ...reader_<pod>_r0_p9201
    re.compile(r"^(?P<prefix>.*\.reader)_.+(?P<suffix>_r\d+_p\d+)$"),
    # split writer: ...writer_<pod>_w0
    re.compile(r"^(?P<prefix>.*\.writer)_.+(?P<suffix>_w\d+)$"),
)
KVTEST_SAMPLES_SUFFIX = ".samples.csv"


def percentile(sorted_vals, q):
    if not sorted_vals:
        return 0.0
    return sorted_vals[int(round(q * (len(sorted_vals) - 1)))]


def _plot_file_stem(log_stem):
    """Drop the pipeline Pod/node segment from generated image names.

    Client/reader/writer ids are global within one scenario, so keeping the
    suffix (cN/rN/wN, port and role) preserves uniqueness without carrying a
    potentially very long Kubernetes Pod name into the Windows output path.
    Non-pipeline/basic log names are returned unchanged.
    """
    for pattern in _PIPELINE_POD_STEM_PATTERNS:
        match = pattern.match(log_stem)
        if match:
            return match.group("prefix") + match.group("suffix")
    return log_stem


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
    lats, qws, times, counts, qw_times = [], [], [], {}, []
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
                    if off:
                        qw_times.append(ts_to_seconds(parts[1]))
                except ValueError:
                    pass
    if lats and times and len(times) != len(lats):
        times = None  # 混合行（部分带 ts）不可靠 → 回退 index 轴
    if qws and qw_times and len(qw_times) != len(qws):
        qw_times = None
    if times:
        # 多线程 flush 序非严格单调：按时间排序；跨午夜回绕折叠为单调
        order = sorted(range(len(lats)), key=lambda i: times[i])
        lats = [lats[i] for i in order]
        if qws and len(qws) == len(lats):
            # 纯 reader 日志：行集相同，沿用同一排序
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
    # queue-wait 独立时间轴：dual 日志把 writer（无 qw 字段）与 reader
    # （带 qw）行混在一个文件里，qws 是 lats 的子序列——必须按 reader
    # 行自身的时间排序，不能借用全量 times（长度错配曾使次轴
    # np.interp 直接崩溃）
    if qw_times:
        qorder = sorted(range(len(qws)), key=lambda i: qw_times[i])
        qws = [qws[i] for i in qorder]
        qfolded = []
        prev = None
        for t in (qw_times[i] for i in qorder):
            if prev is not None and t < prev:
                t += 86400
            qfolded.append(t)
            prev = t
        qw_times = qfolded
    return lats, qws, times, counts, qw_times


def _fmt_hms(seconds, base):
    """Relative seconds -> absolute HH:MM:SS string (base = run start t0)."""
    t = datetime(2000, 1, 1) + timedelta(seconds=base + seconds)
    return t.strftime("%H:%M:%S")


def plot_series(vals, times, title, out_png, outlier_ms, include_outliers,
                ylabel="Latency (ms)", time_axis="wall", report_window=None):
    """Dot-line plot with stat hlines, absolute reference lines and outliers.

    times: wall-clock seconds per sample (None -> request#i axis). When
    include_outliers is False the sample list must already be trimmed.
    """
    if times and len(times) != len(vals):
        # defensive: a mismatched time axis would misalign every sample
        # (and crash the request#i secondary axis) -> fall back to index
        times = None
    n = len(vals)
    fig, ax = plt.subplots(figsize=(12, 6))
    if times and time_axis == "wall":
        t0 = times[0]
        xs = [t - t0 for t in times]  # 相对秒（底部刻度再渲染回绝对 HH:MM:SS）
    elif times:
        t0 = 0.0
        xs = list(times)  # kvtest: already seconds elapsed since /start
    else:
        t0 = None
        xs = list(range(n))
    selected = [True] * n if report_window is None else [
        report_window[0] <= x and (report_window[1] is None or x < report_window[1])
        for x in xs]
    # Mask each category independently. No line connects a gap in the window.
    for inside, alpha, label in ((False, 0.16, "outside stats window"),
                                 (True, 0.85, "stats samples")):
        main = [(x, v) for x, v, keep in zip(xs, vals, selected)
                if keep == inside and (not include_outliers or v <= outlier_ms)]
        outliers = [(x, v) for x, v, keep in zip(xs, vals, selected)
                    if keep == inside and include_outliers and v > outlier_ms]
        if main and report_window is None:
            ax.plot([x for x, _ in main], [v for _, v in main],
                    linewidth=0.5, markersize=2, marker=".", color="tab:gray",
                    label=f"samples (n={len(main)})")
        elif main:
            ax.scatter([x for x, _ in main], [v for _, v in main],
                       s=4, marker=".", color="tab:gray", alpha=alpha,
                       label=f"{label} (n={len(main)})", rasterized=True)
        if outliers:
            ax.scatter([x for x, _ in outliers], [v for _, v in outliers],
                       s=28, marker="x", color="red", alpha=alpha, zorder=5,
                       label=f"{label} outliers ({len(outliers)}, >{outlier_ms:g}ms)",
                       rasterized=True)
    stat_vals = sorted(v for v, keep in zip(vals, selected) if keep)
    stat_n = len(stat_vals)
    if stat_n:
        computed = {"avg": sum(stat_vals) / stat_n, "max": stat_vals[-1],
                    "p99": percentile(stat_vals, 0.99),
                    "p999": percentile(stat_vals, 0.999),
                    "p9999": percentile(stat_vals, 0.9999)}
        lines = list(STAT_HLINES_ALWAYS) + [
            (name, color) for name, color, min_n in STAT_HLINES_GATED
            if report_window is not None or stat_n >= min_n]
        for name, color in lines:
            v = computed[name]
            display = "Pmax" if name == "max" and report_window is not None else name
            ax.axhline(v, linestyle="--", linewidth=1.2, color=color,
                       label=f"{display}={v:.4f}ms")
            # The legend carries exact values. Avoid overlapping labels when
            # several tail percentiles coincide or startup spikes stretch y.
            if report_window is None:
                ax.annotate(f"{display}={v:.4f}", xy=(0.998, v),
                            xycoords=("axes fraction", "data"),
                            ha="right", va="bottom", fontsize=9, color=color)
    else:
        ax.text(0.5, 0.95, "No OK samples in stats window", transform=ax.transAxes,
                ha="center", va="top")
    if report_window is not None:
        start, end = report_window
        ax.axvline(start, color="steelblue", linewidth=0.8, alpha=0.5)
        if end is not None:
            ax.axvline(end, color="steelblue", linewidth=0.8, alpha=0.5)
    for v, label in REF_LINES:
        if max(vals) >= v:
            ax.axhline(v, linestyle="-.", linewidth=1.0, color="gray", alpha=0.8)
            ax.annotate(label, xy=(0.003, v), xycoords=("axes fraction", "data"),
                        ha="left", va="bottom", fontsize=8, color="gray")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(loc="upper left", fontsize=9)
    if times and time_axis == "wall":
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
    elif times:
        ax.set_xlabel("elapsed time (s)")
        sec = ax.secondary_xaxis(
            "top", functions=(
                lambda t, _xt=np.array(xs), _xi=np.arange(len(vals)):
                np.interp(t, _xt, _xi),
                lambda i, _xt=np.array(xs), _xi=np.arange(len(vals)):
                np.interp(i, _xi, _xt)))
        sec.set_xlabel("request #i")
        if report_window is not None and len(xs) > 1:
            # Place ticks uniformly in time, avoiding clustered request-index
            # ticks when sparse samples surround a long steady-state window.
            sec.set_xticks(np.interp(np.linspace(min(xs), max(xs), 5),
                                      np.array(xs), np.arange(len(vals))))
            sec.xaxis.set_major_formatter(plt.FuncFormatter(
                lambda value, _: f"{value:.2g}" if n < 10 else f"{value:,.0f}"))
    else:
        ax.set_xlabel("request #i")
    if len(xs) == 1:
        ax.set_xlim(xs[0] - 0.5, xs[0] + 0.5)
    else:
        ax.set_xlim(min(xs) if times else 0, max(xs))
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def _emit_plots(stem, file_stem, kind, vals, times, out_dir, outlier_ms, note,
                time_axis="wall", report_window=None):
    """Write the latency/queue-wait figure(s): full view always; trimmed view
    additionally when outliers exist (stats recomputed on trimmed data)."""
    suffix = "latency" if kind == "latency" else "queuewait"
    ylabel = "Latency (ms)" if kind == "latency" else "Queue wait (ms)"
    base_title = f"{stem}  ({note})"
    trimmed = [v for v in vals if v <= outlier_ms]
    has_out = len(trimmed) != len(vals)
    png = os.path.join(out_dir, f"{file_stem}.{suffix}.png")
    plot_series(vals, times, base_title, png, outlier_ms, include_outliers=True,
                ylabel=ylabel, time_axis=time_axis, report_window=report_window)
    outputs = [png]
    print(f"  -> {png}")
    if has_out and report_window is None:
        png = os.path.join(out_dir, f"{file_stem}.{suffix}.trimmed.png")
        plot_series(trimmed, _trim_times(times, vals, outlier_ms),
                    f"{stem}  ({note}, outliers removed)", png, outlier_ms,
                    include_outliers=False, ylabel=ylabel,
                    time_axis=time_axis)
        print(f"  -> {png}  (trimmed, {len(vals) - len(trimmed)} outliers excluded)")
        outputs.append(png)
    return outputs


def _trim_times(times, vals, threshold):
    if not times:
        return None
    return [t for t, v in zip(times, vals) if v <= threshold]


def process_file(path, args):
    lats, qws, times, counts, qw_times = parse_result_file(path)
    out_dir = args.out or os.path.dirname(os.path.abspath(path))
    stem = os.path.splitext(os.path.basename(path))[0]
    file_stem = _plot_file_stem(stem)
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
        _emit_plots(stem, file_stem, "latency", lats, times, out_dir, threshold,
                    f"{len(lats)} OK samples")
    if qws and args.type in ("both", "queue-wait"):
        # queue_wait 阈值独立按自身分布计算，避免被时延离群值牵连；
        # 时间轴用 qw 自己的 reader 行时间（dual 混合日志下与全量
        # times 长度不同）
        qw_threshold = threshold if args.outlier else \
            max(round(percentile(sorted(qws), 0.99) * 10), 1)
        _emit_plots(stem, file_stem, "queue-wait", qws, qw_times, out_dir, qw_threshold,
                    f"{len(qws)} samples")


def parse_kvtest_samples(path):
    """Return one row per kvtest operation call, grouped by operation."""
    groups = {}
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.DictReader(f)
        required = {"elapsed_us", "instance_id", "operation", "request_index",
                    "latency_ms", "status", "bytes"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            return groups
        for row in reader:
            operation = (row.get("operation") or "").strip()
            if not operation:
                continue
            try:
                point = {
                    "elapsed_us": int(row["elapsed_us"]),
                    "request_index": int(row["request_index"]),
                    "latency_ms": float(row["latency_ms"]),
                    "status": (row["status"] or "").strip(),
                    "bytes": int(row["bytes"]),
                }
            except (KeyError, TypeError, ValueError):
                continue
            groups.setdefault(operation, []).append(point)
    for points in groups.values():
        points.sort(key=lambda p: (p["elapsed_us"], p["request_index"]))
    return groups


def _report_window(path, args):
    meta_path = os.path.join(os.path.dirname(os.path.abspath(path)),
                             "kvtest_report_meta.json")
    try:
        with open(meta_path, encoding="utf-8") as stream:
            meta = json.load(stream)
    except FileNotFoundError:
        meta = {}
    report = meta.get("report") or {}
    start = report.get("start_s", 0)
    end = report.get("end_s")
    if end is None:
        end = meta.get("duration")
    start_override = getattr(args, "report_start_s", None)
    end_override = getattr(args, "report_end_s", None)
    if not report and start_override is None and end_override is None:
        return None
    start = float(start_override if start_override is not None else start)
    end = float(end_override if end_override is not None else end) if (
        end_override is not None or end is not None) else None
    if (not math.isfinite(start) or start < 0 or
            (end is not None and (not math.isfinite(end) or end <= start))):
        raise ValueError("plot report window requires 0 <= start_s < end_s")
    return start, end


def process_kvtest_samples(path, args):
    groups = parse_kvtest_samples(path)
    report_window = _report_window(path, args)
    if not groups:
        print("[{}] skip (no valid kvtest request samples)".format(
            os.path.basename(path)))
        return []
    outputs = []
    name = os.path.basename(path)
    base = name[:-len(KVTEST_SAMPLES_SUFFIX)] \
        if name.endswith(KVTEST_SAMPLES_SUFFIX) else os.path.splitext(name)[0]
    out_dir = args.out or os.path.dirname(os.path.abspath(path))
    os.makedirs(out_dir, exist_ok=True)
    for operation in sorted(groups):
        is_queue_wait = _kvtest_is_queue_wait(operation)
        if args.type == "latency" and is_queue_wait:
            continue
        if args.type == "queue-wait" and not is_queue_wait:
            continue
        points = groups[operation]
        successful = [p for p in points if p["status"] == "OK"]
        vals = [p["latency_ms"] for p in successful]
        times = [p["elapsed_us"] / 1_000_000.0 for p in successful]
        failed = len(points) - len(successful)
        if not vals:
            print("[{}] {} skip (no OK request samples)".format(
                name, operation))
            continue
        stat_vals = [v for v, t in zip(vals, times) if report_window is None or
                     (t >= report_window[0] and
                      (report_window[1] is None or t < report_window[1]))]
        threshold = args.outlier or max(
            round(percentile(sorted(stat_vals or vals), 0.99) * 10), 1)
        print("[{}] {}: OK={}, failed={}, outlier threshold={}ms".format(
            name, operation, len(vals), failed, threshold))
        kind = "queue-wait" if is_queue_wait else "latency"
        file_stem = "{}.{}".format(base, _safe_plot_component(operation))
        title = "{} — {}".format(base, operation)
        if report_window is not None:
            title += " | stats [{:g}, {}) s, n={}".format(
                report_window[0], "end" if report_window[1] is None else
                "{:g}".format(report_window[1]), len(stat_vals))
        outputs.extend(_emit_plots(
            title, file_stem, kind, vals, times, out_dir, threshold,
            "{} OK samples  ERR:{}".format(len(vals), failed),
            time_axis="elapsed", report_window=report_window))
    return outputs


def _safe_plot_component(value):
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._") or "operation"


def _kvtest_is_queue_wait(operation):
    return operation == "notify_queue_wait" or operation.endswith("queue_wait")


def collect_inputs(path):
    if os.path.isfile(path):
        return [path]
    found = []
    for root, _dirs, files in os.walk(path):
        for f in sorted(files):
            if f.endswith(".log") or f.endswith(KVTEST_SAMPLES_SUFFIX):
                found.append(os.path.join(root, f))
    return found


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Plot latency stats from RESULT logs or kvtest samples CSV")
    parser.add_argument("--log", required=True,
                        help="file or directory (recursive *.log and "
                             "*.samples.csv scan)")
    parser.add_argument("--out", default=None,
                        help="output dir; default = each log's own directory")
    parser.add_argument("--report-start-s", type=float, default=None)
    parser.add_argument("--report-end-s", type=float, default=None)
    parser.add_argument("--outlier", type=float, default=None,
                        help="outlier threshold in ms; default auto = p99 x 10")
    parser.add_argument("--type", choices=["both", "latency", "queue-wait"],
                        default="both")
    args = parser.parse_args(argv)
    if not os.path.exists(args.log):
        sys.exit(f"[ERROR] log path not found: {args.log}")
    inputs = collect_inputs(args.log)
    if not inputs:
        sys.exit(f"[ERROR] no *.log or *.samples.csv under {args.log}")
    for path in inputs:
        if path.endswith(KVTEST_SAMPLES_SUFFIX):
            process_kvtest_samples(path, args)
        else:
            process_file(path, args)
    print("done")


if __name__ == "__main__":
    main()
