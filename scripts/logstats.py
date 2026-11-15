#!/usr/bin/env python3
"""logstats.py — 客户端日志的就地统计（map 阶段，纯 stdlib 单文件）。

把一份客户端日志（.writer.log / .reader.log / basic 合并日志）压缩成
<log>.stats.json，编排器只回传统计产物（reduce 由此合并出集群报表），
原始日志留在节点上按需查看。覆盖 basic 与 pipeline 全部行格式：

  RESULT  <ts>  <op>  <key>  <lat_ms>  <status>                       (basic)
  RESULT  <ts>  pipeline_write  <key>  <lat_ms>  <status>  <kc>       (writer)
  RESULT  <ts>  pipeline_read  <key>  <lat_ms>  <status>  <qw>  <kc>  (reader)
  ts: "YYYY-MM-DD HH:MM:SS.micro"（新，带日期）或 "HH:MM:SS.micro"（旧）

产物结构（JSON）：
  meta      {source, bytes, rows, ts_from, ts_to, wall_s, format}
  global    {total, ok, fail, key_total, min/avg/p50/p95/p99/p999/p9999/max_ms,
             qw_avg/qw_max_ms, exact}
  histogram 对数定梯（FIXED 阶梯，跨客户端可无损相加 -> 集群分位数）
  ops       每个 op 名的计数与错误数
  seconds   per-1s 桶（count/ok/fail/avg/max/qw_avg/qw_max + 稀疏直方图）
  minutes   由 1s 桶精确聚合
  errors    状态码直方图 + 首错误 ±context 行原文摘录
  slowest   Top-N 慢操作（时刻/key/lat/qw/status）
  anomalies 简单异常窗口（错误突发 / 时延突增）

用法：
  python3 logstats.py --log <file> [--out <file>] [--top-n 100] [--context 20]
"""
import argparse
import heapq
import json
import os
import re
import sys
import time

TS_PREFIX = re.compile(r"^(?:\d{4}-\d{2}-\d{2} )?\d{2}:\d{2}:\d{2}")
RESULT_PREFIX = "RESULT\t"
SUMMARY_PREFIX = "SUMMARY\t"
META_PREFIX = "META\t"

# 对数定梯：0.001ms 起、ratio 1.2、80 档（到 2160ms）。所有客户端共用
# 同一阶梯 -> 直方图按 bin 相加即可无损合并，集群分位数由此还原。
BIN_EDGES = [0.001 * (1.2 ** i) for i in range(81)]
BIN_EDGES[0] = 0.0

# C++ SDK trace 的阶段时延单位是 MICROSECOND：必须用 µs 阶梯（0.001µs 起、
# ratio 1.2、120 档到 ~3.4e6µs=3.4s），否则 >2.16ms 的请求全落末桶、分位被
# 钳在 2.4ms 附近（max 仍精确，造成 p99 < max 的荒谬结果）。
BIN_EDGES_US = [0.001 * (1.2 ** i) for i in range(121)]
BIN_EDGES_US[0] = 0.0
EXACT_LIMIT = 5_000_000        # 样本数上限内用精确分位数，超出用直方图
EXCERPT_CAP = 16 * 1024        # 错误摘录字节上限


def _ts_to_epoch(ts):
    """'YYYY-MM-DD HH:MM:SS.micro' -> local epoch float.

    The timestamp is node LOCAL wall clock (mktime, same basis as
    tracestats._start_epoch — timegm would shift every bucket by the UTC
    offset and disagree with the C++ trace timeline). Legacy 'HH:MM:SS.micro'
    -> None (undated: time buckets skipped, global stats still complete)."""
    if " " not in ts:
        return None
    date_part, hms = ts.strip().split(" ", 1)
    frac = 0.0
    if "." in hms:
        hms, _, micro = hms.partition(".")
        try:
            frac = float("0." + micro)
        except ValueError:
            frac = 0.0
    try:
        y, mo, d = (int(x) for x in date_part.split("-"))
        h, m, s = (int(x) for x in hms.split(":"))
        return time.mktime((y, mo, d, h, m, s, 0, 0, -1)) + frac
    except (ValueError, OverflowError):
        return None


def iter_rows(path):
    """Yield dicts {ts, op, key, lat, status, qw, kc, lineno} for RESULT rows."""
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for lineno, line in enumerate(f, 1):
            if not line.startswith(RESULT_PREFIX):
                continue
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 5:
                continue
            # legacy rows start with the op name (no timestamp at all);
            # dated/dateless-time rows keep the timestamp at parts[1]
            off = 1 if TS_PREFIX.match(parts[1]) else 0
            if len(parts) < 5 + off:
                continue
            op = parts[1 + off]
            ts = parts[1] if off else ""
            if op == "pipeline_notify":
                # notify 诊断行：无时延样本，错误计数交给 errors 流
                yield {"ts": ts, "op": op, "key": parts[2 + off] if len(parts) > 2 + off else "",
                       "lat": None, "status": parts[4 + off] if len(parts) > 4 + off else "",
                       "qw": None, "kc": 0, "lineno": lineno}
                continue
            try:
                lat = float(parts[3 + off])
            except ValueError:
                continue
            status = parts[4 + off]
            qw = None
            kc = 1
            try:
                if op == "pipeline_write" and len(parts) > 5 + off:
                    kc = int(parts[5 + off]) or 1
                elif op == "pipeline_read":
                    if len(parts) > 6 + off:
                        qw = float(parts[5 + off])
                    if len(parts) > 6 + off:
                        kc = int(parts[6 + off]) or 1
            except ValueError:
                pass
            yield {"ts": ts, "op": op,
                   "key": parts[2 + off], "lat": lat, "status": status,
                   "qw": qw, "kc": kc, "lineno": lineno}


def bin_of(lat, edges=None):
    """Histogram bin index for a value (ms by default; pass edges for µs)."""
    edges = BIN_EDGES if edges is None else edges
    lo, hi = 0, len(edges) - 1
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if lat >= edges[mid]:
            lo = mid
        else:
            hi = mid - 1
    return lo


def hist_percentile(hist, q, edges=None):
    """Percentile from a (sparse) histogram dict {bin: count}.

    Bin keys may be ints (in-memory) or numeric strings (merged from the
    persisted stats JSON) — normalized here so the reduce stage can merge
    straight from disk. `edges` must be the ladder used to build the bins
    (ms BIN_EDGES by default; BIN_EDGES_US for C++ trace values)."""
    edges = BIN_EDGES if edges is None else edges
    hist = {int(k): v for k, v in hist.items()}
    total = sum(hist.values())
    if not total:
        return 0.0
    target = q * (total - 1)
    acc = 0
    for b in sorted(hist):
        c = hist[b]
        if acc + c > target:
            # linear interpolation inside the bin
            edge_lo = edges[b]
            edge_hi = edges[b + 1] if b + 1 < len(edges) else edge_lo * 1.2
            frac = (target - acc) / c if c else 0.0
            return edge_lo + (edge_hi - edge_lo) * frac
        acc += c
    return edges[max(hist)]


def _exact_percentile(sorted_vals, q):
    if not sorted_vals:
        return 0.0
    return sorted_vals[int(round(q * (len(sorted_vals) - 1)))]


def _read_context(path, center_line, context):
    """Raw excerpt lines [center-context, center+context] (1-based)."""
    lo = max(1, center_line - context)
    hi = center_line + context
    out = []
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for i, line in enumerate(f, 1):
            if i > hi:
                break
            if i >= lo:
                out.append(line.rstrip("\n"))
    text = "\n".join(out)
    return text[:EXCERPT_CAP]


def _scan_summaries(path, cap=128, meta_cap=2):
    """SUMMARY/META lines of one log (JSON payloads).

    SUMMARYs are what downstream needs (bytes / wall / max_depth): every
    client of a merged basic log writes one, so they get their own generous
    budget. META lines are diagnostics only — they must NOT consume the
    SUMMARY quota (a writer log alone prints 5-6 of them)."""
    out = []
    n_sum = n_meta = 0
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                if line.startswith(SUMMARY_PREFIX):
                    if n_sum >= cap:
                        continue
                    try:
                        out.append(("summary",
                                    json.loads(line[len(SUMMARY_PREFIX):])))
                        n_sum += 1
                    except ValueError:
                        continue
                elif line.startswith(META_PREFIX):
                    if n_meta >= meta_cap:
                        continue
                    try:
                        out.append(("meta",
                                    json.loads(line[len(META_PREFIX):])))
                        n_meta += 1
                    except ValueError:
                        continue
    except OSError:
        pass
    return out


def compute_stats(path, top_n=100, context=20, node=None, pod=None,
                  role=None, remote_path=None, size=None):
    rows = 0
    total = ok = fail = 0
    key_total = key_ok = key_fail = 0
    lats = []                       # OK-only (exact percentile window)
    lat_min = lat_max = None        # exact extremes, kept past the window
    exact_ok = True                 # still holding every OK sample?
    lat_sum = 0.0
    qw_sum = 0.0
    qw_n = 0
    qw_max = 0.0
    hist = {}
    ops = {}
    errors = {}
    first_error_line = None
    first_error_status = None
    slowest = []                    # min-heap of (lat, seq, row), size<=top_n
    slow_seq = 0
    seconds = {}                    # epoch-second -> bucket dict
    ts_from = ts_to = None

    for row in iter_rows(path):
        rows += 1
        op = row["op"]
        kc = row["kc"]
        status = row["status"]
        okrow = status == "OK"
        total += 1
        key_total += kc
        opd = ops.setdefault(op, {"count": 0, "ok": 0, "fail": 0, "keys": 0,
                                   "_lats": [], "_h": {}, "_sum": 0.0,
                                   "_min": None, "_max": None, "_exact": True})
        opd["count"] += 1
        opd["keys"] += kc
        if okrow:
            opd["ok"] += 1
            ob = bin_of(row["lat"])
            opd["_h"][ob] = opd["_h"].get(ob, 0) + 1
            opd["_sum"] += row["lat"]
            opd["_min"] = (row["lat"] if opd["_min"] is None
                           else min(opd["_min"], row["lat"]))
            opd["_max"] = (row["lat"] if opd["_max"] is None
                           else max(opd["_max"], row["lat"]))
            if len(opd["_lats"]) < EXACT_LIMIT:
                if opd["_exact"]:
                    opd["_lats"].append(row["lat"])
            elif opd["_exact"]:
                opd["_exact"] = False
                opd["_lats"] = []          # free the window, keep histogram
        else:
            opd["fail"] += 1
        ep = _ts_to_epoch(row["ts"])
        if ep is not None:
            if ts_from is None or ep < ts_from:
                ts_from = ep
            if ts_to is None or ep > ts_to:
                ts_to = ep

        if not okrow:
            fail += 1
            key_fail += kc
            errors[status] = errors.get(status, 0) + 1
            if first_error_line is None:
                first_error_line = row["lineno"]
                first_error_status = status
            if row["lat"] is not None:
                slow_seq += 1
                if len(slowest) < top_n:
                    heapq.heappush(slowest, (row["lat"], slow_seq, row))
                elif row["lat"] > slowest[0][0]:
                    heapq.heapreplace(slowest, (row["lat"], slow_seq, row))
            continue

        ok += 1
        key_ok += kc
        lat = row["lat"]
        lat_sum += lat
        lat_min = lat if lat_min is None else min(lat_min, lat)
        lat_max = lat if lat_max is None else max(lat_max, lat)
        if len(lats) < EXACT_LIMIT:
            if exact_ok:
                lats.append(lat)
        elif exact_ok:
            exact_ok = False
            lats = []                      # free the window, keep histogram
        b = bin_of(lat)
        hist[b] = hist.get(b, 0) + 1
        qw = row["qw"]
        if qw is not None:
            qw_sum += qw
            qw_n += 1
            qw_max = max(qw_max, qw)
        # top-N slowest (OK rows too; errors already added above)
        slow_seq += 1
        if len(slowest) < top_n:
            heapq.heappush(slowest, (lat, slow_seq, row))
        elif lat > slowest[0][0]:
            heapq.heapreplace(slowest, (lat, slow_seq, row))
        if ep is not None:
            sec = int(ep)
            buck = seconds.get(sec)
            if buck is None:
                buck = seconds[sec] = {"c": 0, "ok": 0, "fail": 0,
                                       "sum": 0.0, "max": 0.0,
                                       "qsum": 0.0, "qmax": 0.0, "h": {}}
            buck["c"] += 1
            buck["ok"] += 1
            buck["sum"] += lat
            buck["max"] = max(buck["max"], lat)
            hb = bin_of(lat)
            buck["h"][hb] = buck["h"].get(hb, 0) + 1
            if qw is not None:
                buck["qsum"] += qw
                buck["qmax"] = max(buck["qmax"], qw)

    # errors land in their (epoch) bucket too (for anomaly windows)
    # second pass avoided: errors were few in normal runs; add them now by
    # re-scanning ONLY when errors exist
    if fail:
        for row in iter_rows(path):
            if row["status"] == "OK" or row["lat"] is None:
                continue
            ep = _ts_to_epoch(row["ts"])
            if ep is None:
                continue
            sec = int(ep)
            buck = seconds.get(sec)
            if buck is None:
                buck = seconds[sec] = {"c": 0, "ok": 0, "fail": 0,
                                       "sum": 0.0, "max": 0.0,
                                       "qsum": 0.0, "qmax": 0.0, "h": {}}
            buck["c"] += 1
            buck["fail"] += 1

    # top-N slowest overall (descending)
    top = []
    for lat, _seq, row in sorted(slowest, key=lambda x: -x[0]):
        top.append({"ts": row["ts"], "op": row["op"], "key": row["key"],
                    "lat_ms": round(row["lat"], 6),
                    "status": row["status"],
                    "qw_ms": (round(row["qw"], 6) if row["qw"] is not None else None)})

    # exact or histogram percentiles (min/max stay exact either way)
    exact = exact_ok and ok > 0
    if exact and lats:
        lats.sort()
        def pct(q):
            return round(_exact_percentile(lats, q), 6)
    else:
        def pct(q):
            return round(hist_percentile(hist, q), 6)

    # per-op finalized stats (counts + latency percentiles per op name)
    ops_out = {}
    for op, opd in ops.items():
        if opd["_exact"] and opd["_lats"]:
            ls = sorted(opd["_lats"])
            def opct(q, _ls=ls):
                return round(_exact_percentile(_ls, q), 6)
        else:
            def opct(q, _h=opd["_h"]):
                return round(hist_percentile(_h, q), 6)
        opmin = round(opd["_min"], 6) if opd["_min"] is not None else 0.0
        opmax = round(opd["_max"], 6) if opd["_max"] is not None else 0.0
        ops_out[op] = {
            "count": opd["count"], "ok": opd["ok"], "fail": opd["fail"],
            "keys": opd["keys"],
            "avg_ms": round(opd["_sum"] / opd["ok"], 6) if opd["ok"] else 0.0,
            "min_ms": opmin, "max_ms": opmax,
            "p50_ms": opct(0.50), "p95_ms": opct(0.95), "p99_ms": opct(0.99),
            "p999_ms": opct(0.999), "p9999_ms": opct(0.9999),
        }

    # minute buckets: exact merge of the 1s buckets
    minutes = {}
    for sec in sorted(seconds):
        buck = seconds[sec]
        minute = sec // 60
        mb = minutes.get(minute)
        if mb is None:
            mb = minutes[minute] = {"c": 0, "ok": 0, "fail": 0, "sum": 0.0,
                                    "max": 0.0, "qsum": 0.0, "qmax": 0.0, "h": {}}
        for k in ("c", "ok", "fail"):
            mb[k] += buck[k]
        mb["sum"] += buck["sum"]
        mb["max"] = max(mb["max"], buck["max"])
        mb["qsum"] += buck["qsum"]
        mb["qmax"] = max(mb["qmax"], buck["qmax"])
        for b, n in buck["h"].items():
            mb["h"][b] = mb["h"].get(b, 0) + n

    def buckets_to_list(bmap, base):
        # bucket keys are epoch-seconds (1s) or epoch-minutes (1min); the
        # emitted t is ALWAYS epoch-seconds so every consumer shares one unit
        out = []
        for t in sorted(bmap):
            b = bmap[t]
            out.append([
                t * base, b["c"], b["ok"], b["fail"],
                round(b["sum"], 3), round(b["max"], 6),
                round(b["qsum"], 3), round(b["qmax"], 6),
                b["h"]])
        return out

    # simple anomaly windows: error burst or latency spike per second
    anomalies = []
    avg_ms = (lat_sum / ok) if ok else 0.0
    for sec in sorted(seconds):
        b = seconds[sec]
        if b["fail"] > 0 and b["c"] and b["fail"] >= max(1, 0.05 * b["c"]):
            anomalies.append({"t": sec, "kind": "error_burst",
                              "fail": b["fail"], "count": b["c"]})
        elif b["ok"] >= 10 and avg_ms > 0 and (b["sum"] / b["ok"]) > 3 * avg_ms:
            anomalies.append({"t": sec, "kind": "latency_spike",
                              "avg_ms": round(b["sum"] / b["ok"], 3),
                              "count": b["c"]})

    stats = {
        "meta": {
            "source": os.path.basename(path),
            "bytes": os.path.getsize(path),
            "node": node,
            "pod": pod,
            "role": role,
            "remote_path": remote_path or path,
            "size": size,
            "rows": rows,
            "ts_from": ts_from, "ts_to": ts_to,
            "wall_s": round(ts_to - ts_from, 3) if ts_from and ts_to else None,
            "dated": ts_from is not None,
            # time-bucket t unit; "s" = epoch seconds (absent in old products,
            # whose minutes used epoch minutes -> reduce multiplies by 60)
            "bucket_unit": "s",
        },
        "global": {
            "total": total, "ok": ok, "fail": fail,
            "key_total": key_total, "key_ok": key_ok, "key_fail": key_fail,
            "avg_ms": round(avg_ms, 6) if ok else 0.0,
            "min_ms": round(lat_min, 6) if lat_min is not None else 0.0,
            "max_ms": round(lat_max, 6) if lat_max is not None else 0.0,
            "p50_ms": pct(0.50), "p95_ms": pct(0.95), "p99_ms": pct(0.99),
            "p999_ms": pct(0.999), "p9999_ms": pct(0.9999),
            "qw_avg_ms": round(qw_sum / qw_n, 6) if qw_n else None,
            "qw_max_ms": round(qw_max, 6) if qw_n else None,
            "qw_n": qw_n,
            "exact": exact,
        },
        "histogram": {str(b): n for b, n in sorted(hist.items())},
        "ops": ops_out,
        "summaries": _scan_summaries(path),
        "seconds": buckets_to_list(seconds, 1),
        "minutes": buckets_to_list(minutes, 60),
        "errors": errors,
        "error_excerpt": (_read_context(path, first_error_line, context)
                          if first_error_line is not None else None),
        "error_excerpt_status": first_error_status,
        "slowest": top,
        "anomalies": anomalies,
    }
    return stats


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Compute compact stats JSON from one client log (map stage)")
    parser.add_argument("--log", required=True, help="client log file")
    parser.add_argument("--out", default=None,
                        help="output path (default: <log>.stats.json)")
    parser.add_argument("--top-n", type=int, default=100)
    parser.add_argument("--context", type=int, default=20)
    # provenance stamped by the orchestrator's map stage (used by the report's
    # per-node/per-pod breakdown and the on-site log location index)
    parser.add_argument("--node", default=None)
    parser.add_argument("--pod", default=None)
    parser.add_argument("--role", default=None)
    parser.add_argument("--remote-path", default=None)
    parser.add_argument("--size", default=None)
    args = parser.parse_args(argv)
    out = args.out or (args.log + ".stats.json")
    stats = compute_stats(args.log, top_n=args.top_n, context=args.context,
                          node=args.node, pod=args.pod, role=args.role,
                          remote_path=args.remote_path, size=args.size)
    tmp = out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, out)
    g = stats["global"]
    print("[logstats] {} rows={} ok={} fail={} p99={:.3f}ms -> {}".format(
        os.path.basename(args.log), stats["meta"]["rows"], g["ok"], g["fail"],
        g["p99_ms"], out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
