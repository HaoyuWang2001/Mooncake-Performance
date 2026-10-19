#!/usr/bin/env python3
"""plot_latency.py — 从 benchmark 客户端日志绘制时延统计图。

RESULT 行三种格式（tab 分隔，与 mooncake_benchmark_client.py 输出一致）：
  basic/mixed : RESULT  <op>  <key>  <lat_ms>  <status>                      (5 字段)
  pipeline_w  : RESULT  pipeline_write  <key>  <lat_ms>  <status>  <kc>    (6 字段)
  pipeline_r  : RESULT  pipeline_read  <key>  <lat_ms>  <status>  <qw>  <kc> (7 字段)

仅 OK 样本进入主序列。pipeline_read 日志额外绘制 queue_wait（第 6 列）统计图。

用法：
  python plot_latency.py --log <日志文件|目录> [--out DIR] [--outlier MS] [--type both]
    --log      单个日志文件，或目录（递归扫描其中含 RESULT 行的 *.log，逐文件出图）
    --out      输出目录，缺省 = 日志所在目录（即 outputs/<ts>/logs/<配置名>/）
    --outlier  离群阈值 ms；缺省自动 = p99 × 10 取整（p99 不会被尾部离群值自身抬高）
    --type     both（缺省，read 日志出时延+queue_wait 双图）| latency | queue-wait

输出：<日志主干>.latency.png / <日志主干>.queuewait.png（dpi 150）。
benchmark.py 主流程结束后会自动以 --log <本次 run 的 logs/> 调用本脚本
（--no-plot 关闭）。依赖：matplotlib（pip install matplotlib）。
"""
import argparse
import os
import sys

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    sys.exit("[ERROR] matplotlib is required: pip install matplotlib")

STAT_HLINES = [("avg", "tab:blue"), ("p99", "tab:orange"), ("max", "tab:red")]
REF_LINES = [(1.0, "1ms"), (2.0, "2ms")]


def percentile(sorted_vals, q):
    if not sorted_vals:
        return 0.0
    return sorted_vals[int(round(q * (len(sorted_vals) - 1)))]


def parse_result_file(path):
    """Return (latencies, queue_waits, status_counts) from one log file.

    latencies/queue_waits are OK-only sample lists; status_counts maps
    status -> line count across all RESULT rows.
    """
    lats, qws, counts = [], [], {}
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            if not line.startswith("RESULT\t"):
                continue
            parts = line.rstrip("\n").split("\t")
            op = parts[1] if len(parts) > 1 else ""
            if op == "pipeline_notify":
                continue  # notify 失败诊断行，无时延样本
            status = parts[4] if len(parts) > 4 else "ERR"
            counts[status] = counts.get(status, 0) + 1
            if status != "OK":
                continue
            try:
                lats.append(float(parts[3]))
            except (ValueError, IndexError):
                continue
            if len(parts) > 6 and op == "pipeline_read":
                try:
                    qws.append(float(parts[5]))
                except ValueError:
                    pass
    return lats, qws, counts


def plot_series(vals, title, out_png, outlier_ms, ylabel="Latency (ms)"):
    """Dot-line plot with stat hlines, absolute reference lines and outliers."""
    n = len(vals)
    fig, ax = plt.subplots(figsize=(12, 6))
    idx = list(range(n))
    main = [(i, v) for i, v in zip(idx, vals) if v <= outlier_ms]
    outliers = [(i, v) for i, v in zip(idx, vals) if v > outlier_ms]
    if main:
        ax.plot([i for i, _ in main], [v for _, v in main],
                linewidth=0.5, markersize=2, marker=".", color="tab:gray",
                label=f"samples (n={len(main)})")
    if outliers:
        ax.scatter([i for i, _ in outliers], [v for _, v in outliers],
                   s=28, marker="x", color="red", zorder=5,
                   label=f"outliers ({len(outliers)}, >{outlier_ms:g}ms)")
    svals = sorted(vals)
    for name, color in STAT_HLINES:
        v = {"avg": sum(svals) / n, "p99": percentile(svals, 0.99),
             "max": svals[-1]}[name]
        ax.axhline(v, linestyle="--", linewidth=1.2, color=color)
        ax.annotate(f"{name}={v:.4f}", xy=(0.998, v), xycoords=("axes fraction", "data"),
                    ha="right", va="bottom", fontsize=9, color=color)
    for v, label in REF_LINES:
        if max(vals) >= v:
            ax.axhline(v, linestyle="-.", linewidth=1.0, color="gray", alpha=0.8)
            ax.annotate(label, xy=(0.003, v), xycoords=("axes fraction", "data"),
                        ha="left", va="bottom", fontsize=8, color="gray")
    ax.set_xlabel("request #i")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.legend(loc="upper left", fontsize=9)
    ax.set_xlim(0, max(n, 1))
    fig.tight_layout()
    fig.savefig(out_png, dpi=150)
    plt.close(fig)


def process_file(path, args):
    lats, qws, counts = parse_result_file(path)
    out_dir = args.out or os.path.dirname(os.path.abspath(path))
    stem = os.path.splitext(os.path.basename(path))[0]
    status_note = "  ".join(f"{k}:{v}" for k, v in sorted(counts.items())) or "no RESULT"
    print(f"[{os.path.basename(path)}] RESULT status: {status_note}, OK-samples: {len(lats)}"
          + (f", queue_wait: {len(qws)}" if qws else ""))
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
        png = os.path.join(out_dir, f"{stem}.latency.png")
        plot_series(lats, f"{stem}  ({len(lats)} OK samples)", png, threshold)
        print(f"  -> {png}")
    if qws and args.type in ("both", "queue-wait"):
        # queue_wait 阈值独立按自身分布计算，避免被时延离群值牵连
        if not args.outlier:
            qw_threshold = max(round(percentile(sorted(qws), 0.99) * 10), 1)
        else:
            qw_threshold = args.outlier
        png = os.path.join(out_dir, f"{stem}.queuewait.png")
        plot_series(qws, f"{stem}  queue_wait ({len(qws)} samples)", png,
                    qw_threshold, ylabel="Queue wait (ms)")
        print(f"  -> {png}")


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
    produced = 0
    for path in logs:
        before = produced
        process_file(path, args)
    print("done")


if __name__ == "__main__":
    main()
