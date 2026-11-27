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

# 对数定梯：0.001ms 起、ratio 1.2。所有客户端共用同一阶梯 -> 直方图按 bin
# 相加即可无损合并，集群分位数由此还原。
# v1（历史产物，读取旧 stats.json 用）：81 档到 2160ms
BIN_EDGES = [0.001 * (1.2 ** i) for i in range(81)]
BIN_EDGES[0] = 0.0
# v2（当前产物）：同一 ratio、108 档到 ~250s——多秒级停顿（超时/存储耗尽/
# GC）不再全挤进末桶，p99/p999 保持真实量级。products 带 meta.hist_version。
BIN_EDGES_V2 = [0.001 * (1.2 ** i) for i in range(108)]
BIN_EDGES_V2[0] = 0.0
HIST_VERSION = 2
EDGES_BY_VERSION = {1: BIN_EDGES, 2: BIN_EDGES_V2}

# C++ SDK trace 的阶段时延单位是 MICROSECOND：必须用 µs 阶梯（0.001µs 起、
# ratio 1.2、120 档到 ~3.4e6µs=3.4s），否则 >2.16ms 的请求全落末桶、分位被
# 钳在 2.4ms 附近（max 仍精确，造成 p99 < max 的荒谬结果）。
BIN_EDGES_US = [0.001 * (1.2 ** i) for i in range(121)]
BIN_EDGES_US[0] = 0.0
# 分位口径（单一管线，见 scripts/quantiles.py）：q <= QUANTILE_HIST_MAX_Q
# 用合并直方图插值；之上的尾部用 REQ sketch（无 sketch 时回退直方图）。
QUANTILE_HIST_MAX_Q = 0.95

try:                            # 第三方库可缺失（旧镜像/纯 python 环境）
    import quantiles as _qt
except Exception:               # pragma: no cover
    _qt = None


def have_sketch():
    """True when the third-party sketch library is importable right now."""
    return _qt is not None and _qt.available()

def digest_quantile(items, q, k=None):
    """Merge serialized REQ sketches and query one quantile (strict).

    Used by the orchestrator finalize stage only; the controller defers the
    tails. Missing library or an empty merge is an ERROR (no fallback)."""
    if _qt is None or not _qt.available():
        raise RuntimeError(
            "digest_quantile: datasketches unavailable (finalize runs on the "
            "orchestrator and requires the library)")
    if not items:
        raise RuntimeError("digest_quantile: empty digest list for q={}"
                           .format(q))
    v = _qt.quantile_from(items, q, k=k)
    if v is None:
        raise RuntimeError("digest_quantile: sketch merge failed (q={})"
                           .format(q))
    return float(v)


def unified_pct(hist, digest_items=None, q=0.5, edges=None, gmax=None):
    """One percentile under the single policy.

    hist: merged bin histogram {bin: count}; digest_items: serialized REQ
    sketches (base64 list). q above QUANTILE_HIST_MAX_Q prefers the sketch;
    everything else (or sketch unavailable) uses histogram interpolation,
    never above the true maximum."""
    edges = BIN_EDGES_V2 if edges is None else edges
    if q > QUANTILE_HIST_MAX_Q and digest_items and _qt is not None:
        v = _qt.quantile_from(digest_items, q)
        if v is not None:
            return float(v) if gmax is None else min(float(v), gmax)
    if not hist:
        return None
    v = hist_percentile(hist, q, edges)
    return float(v) if gmax is None else min(float(v), gmax)
# 成功请求的时延阈值计数（微秒 -> CSV 的 ">Nus" 列）。与
# benchmark.LATENCY_THRESHOLD_US 保持一致：stats-first 与原始日志两条
# CSV 路径输出同一套列，阈值口径都是「严格大于」。
LATENCY_THRESHOLDS_US = (2000, 5000, 6000, 7000, 8000, 9000, 10000, 20000)
LATENCY_THRESHOLD_COLUMNS = [">{}us".format(v)
                             for v in LATENCY_THRESHOLDS_US]
# 离群体系（S5）：离群值 = 单请求 > 该客户端 p99 × 10（需足够样本）；
# 离群样本从 1s 桶的标量统计（sum/max/队列等待）中剔除；
# 计数（c/ok/fail）与直方图保持全量（QPS 与时延分布口径不变）。
OUTLIER_FACTOR = 10.0
OUTLIER_MIN_SAMPLES = 1000     # 样本不足时 p99 不稳定 -> 不做离群判定
LIST_CAP = 200                 # 异常请求 / 离群值清单每客户端上限
# 时间尺度收集规则（与 report.py / tracestats.py 一致）：
#   尺度 s 回传 ⇔ 桶数 span/s ∈ [MIN_TIER_BUCKETS, MAX_TIER_BUCKETS]
TIER_STEPS = (1, 10, 60)
MIN_TIER_BUCKETS = 59
MAX_TIER_BUCKETS = 8192
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
    last = len(edges) - 1
    acc = 0
    for b in sorted(hist):
        c = hist[b]
        if acc + c > target:
            # linear interpolation inside the bin (bin index clamped: a product
            # with an unexpected ladder must not crash the report)
            b = min(b, last)
            edge_lo = edges[b]
            edge_hi = edges[b + 1] if b + 1 < len(edges) else edge_lo * 1.2
            frac = (target - acc) / c if c else 0.0
            return edge_lo + (edge_hi - edge_lo) * frac
        acc += c
    return edges[min(max(hist), last)]


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


def _row_epoch(row):
    """Absolute epoch of a row: explicit field wins, else parse the ts string.

    kvtest map rows carry `epoch` because their ts is synthesized on the
    ORCHESTRATOR (host timezone), while node-side map products carry node
    wall-clock ts that must be parsed with the node's timezone."""
    ep = row.get("epoch")
    if ep is not None:
        return ep
    ts = row.get("ts") or ""
    return _ts_to_epoch(ts) if ts else None


def compute_stats(path, top_n=100, context=20, node=None, pod=None,
                  role=None, remote_path=None, size=None, rows_factory=None,
                  summaries=None, source_name=None, extra_meta=None,
                  warmup_cutoff=None, digest_k=None,
                  row_sink=None, op_filter=None):
    """Build one client's stats product (the map stage).

    rows_factory: optional callable(path) -> row iterator; defaults to the
        RESULT-line reader. The kvtest map passes ``iter_kvtest_rows`` so a
        request-sample CSV produces the exact same product as a client log.
    summaries: pre-built SUMMARY/META payload list (kvtest has no such lines;
        the orchestrator synthesizes them from summary.json).
    source_name: display/provenance name written to meta.source.
    extra_meta: additional meta keys (e.g. {"format": "kvtest_samples"}).
    warmup_cutoff: epoch seconds; rows BEFORE it are the warmup region — they
        feed ONLY the time-bucket charts (drawn, with a boundary line) and are
        excluded from every formal statistic (totals/op stats/histograms/
        slowest/errors). None keeps the legacy single-phase behaviour.
    digest_k: REQ sketch k for the percentile digests (None disables the
        sketch — tails then fall back to histogram interpolation).
    row_sink: optional callback(row, in_warmup, okrow) invoked for EVERY row
        (before op filtering/warmup skipping) so a caller can collect a
        side product in the same single pass (kvtest csvstats).
    op_filter: when set, only these ops enter this product's aggregates;
        other ops are delivered to row_sink and skipped (notify ops).
    """
    row_source = rows_factory or iter_rows
    top_n = max(1, int(top_n or 0))   # a 0-capacity heap indexes slowest[0]
    rows = 0
    total = ok = fail = 0
    key_total = key_ok = key_fail = 0
    lat_min = lat_max = None        # exact extremes
    _dk = (_qt.resolve_k(digest_k)
           if (_qt is not None and digest_k is not None) else None)
    digest_all = _qt.make(_dk) if _dk is not None else None
    op_digests = {}                 # op -> sketch
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
    threshold_counts = dict.fromkeys(LATENCY_THRESHOLDS_US, 0)
    abnormal = []                   # 异常请求（未完成/失败）清单，<=LIST_CAP
    seconds = {}                    # epoch-second -> bucket dict
    ts_from = ts_to = None
    # warmup region bookkeeping: chart domain = ts_from/to (all rows),
    # formal domain = formal_ts_from/to (rows at/after the cutoff)
    warmup_rows = 0
    warmup_fail = 0
    formal_ts_from = formal_ts_to = None

    for row in row_source(path):
        rows += 1
        op = row["op"]
        kc = row["kc"]
        status = row["status"]
        okrow = status == "OK"
        ep = _row_epoch(row)
        in_warmup = (warmup_cutoff is not None and ep is not None
                     and ep < float(warmup_cutoff))
        if row_sink is not None:
            row_sink(row, in_warmup, okrow)
        if op_filter is not None and op not in op_filter:
            continue
        if in_warmup:
            # warmup 区：只进图表时间桶（图上绘制），不进任何正式统计。
            # 失败行交给下方 errors 二次扫描统一入桶，避免重复计数。
            warmup_rows += 1
            if ts_from is None or ep < ts_from:
                ts_from = ep
            if ts_to is None or ep > ts_to:
                ts_to = ep
            if okrow and row["lat"] is not None:
                sec = int(ep)
                buck = seconds.get(sec)
                if buck is None:
                    buck = seconds[sec] = {"c": 0, "ok": 0, "fail": 0,
                                           "sum": 0.0, "max": 0.0,
                                           "qsum": 0.0, "qmax": 0.0, "h": {}}
                lat = row["lat"]
                buck["c"] += 1
                buck["ok"] += 1
                buck["sum"] += lat
                buck["max"] = max(buck["max"], lat)
                hb = bin_of(lat, BIN_EDGES_V2)
                buck["h"][hb] = buck["h"].get(hb, 0) + 1
                qw = row["qw"]
                if qw is not None:
                    buck["qsum"] += qw
                    buck["qmax"] = max(buck["qmax"], qw)
            else:
                warmup_fail += 1
            continue
        total += 1
        key_total += kc
        opd = ops.setdefault(op, {"count": 0, "ok": 0, "fail": 0, "keys": 0,
                                   "_keys_ok": 0, "_keys_fail": 0,
                                   "_h": {}, "_sum": 0.0,
                                   "_min": None, "_max": None,
                                   "_th": dict.fromkeys(
                                       LATENCY_THRESHOLDS_US, 0)})
        opd["count"] += 1
        opd["keys"] += kc
        if okrow:
            opd["ok"] += 1
            opd["_keys_ok"] += kc
            for threshold in LATENCY_THRESHOLDS_US:
                if row["lat"] > threshold / 1000.0:
                    opd["_th"][threshold] += 1
            ob = bin_of(row["lat"], BIN_EDGES_V2)
            opd["_h"][ob] = opd["_h"].get(ob, 0) + 1
            opd["_sum"] += row["lat"]
            opd["_min"] = (row["lat"] if opd["_min"] is None
                           else min(opd["_min"], row["lat"]))
            opd["_max"] = (row["lat"] if opd["_max"] is None
                           else max(opd["_max"], row["lat"]))
            if digest_all is not None:
                _qt.update(digest_all, row["lat"])
                _sk = op_digests.get(op)
                if _sk is None:
                    _sk = op_digests[op] = _qt.make(_dk)
                _qt.update(_sk, row["lat"])
        else:
            opd["fail"] += 1
            opd["_keys_fail"] += kc
        if ep is not None:
            if ts_from is None or ep < ts_from:
                ts_from = ep
            if ts_to is None or ep > ts_to:
                ts_to = ep
            if formal_ts_from is None or ep < formal_ts_from:
                formal_ts_from = ep
            if formal_ts_to is None or ep > formal_ts_to:
                formal_ts_to = ep

        if not okrow:
            fail += 1
            key_fail += kc
            errors[status] = errors.get(status, 0) + 1
            if first_error_line is None:
                first_error_line = row["lineno"]
                first_error_status = status
            if len(abnormal) < LIST_CAP:
                abnormal.append({
                    "ts": row["ts"], "op": row["op"], "key": row["key"],
                    "lat_ms": (round(row["lat"], 6)
                               if row["lat"] is not None else None),
                    "status": status, "ts_epoch": row.get("epoch")})
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
        for threshold in LATENCY_THRESHOLDS_US:
            if lat > threshold / 1000.0:
                threshold_counts[threshold] += 1
        lat_sum += lat
        lat_min = lat if lat_min is None else min(lat_min, lat)
        lat_max = lat if lat_max is None else max(lat_max, lat)
        b = bin_of(lat, BIN_EDGES_V2)
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
            hb = bin_of(lat, BIN_EDGES_V2)
            buck["h"][hb] = buck["h"].get(hb, 0) + 1
            if qw is not None:
                buck["qsum"] += qw
                buck["qmax"] = max(buck["qmax"], qw)

    # errors land in their (epoch) bucket too (for anomaly windows)
    # second pass avoided: errors were few in normal runs; add them now by
    # re-scanning ONLY when errors exist
    if fail or warmup_fail:
        for row in row_source(path):
            if row["status"] == "OK" or row["lat"] is None:
                continue
            ep = _row_epoch(row)
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
                    "ts_epoch": row.get("epoch"),
                    "qw_ms": (round(row["qw"], 6) if row["qw"] is not None else None)})

    # 统一分位口径：hist <= p95，尾部 REQ sketch（无 sketch 回退直方图）；
    # min/max 始终精确
    _digest_b64 = _qt.serialize(digest_all) if digest_all is not None else None
    _digest_items = [_digest_b64] if _digest_b64 else None

    def pct(q):
        v = unified_pct(hist, _digest_items, q, BIN_EDGES_V2, gmax=lat_max)
        return round(v, 6) if v is not None else 0.0

    # per-op finalized stats (counts + latency percentiles per op name)
    ops_out = {}
    _op_digest_b64 = {}
    for _op, _sk in op_digests.items():
        if _sk is not None:
            _b = _qt.serialize(_sk)
            if _b:
                _op_digest_b64[_op] = _b
    for op, opd in ops.items():
        _items = ([_op_digest_b64[op]] if op in _op_digest_b64 else None)

        def opct(q, _h=opd["_h"], _items=_items, _mx=opd["_max"]):
            v = unified_pct(_h, _items, q, BIN_EDGES_V2, gmax=_mx)
            return round(v, 6) if v is not None else 0.0
        opmin = round(opd["_min"], 6) if opd["_min"] is not None else 0.0
        opmax = round(opd["_max"], 6) if opd["_max"] is not None else 0.0
        ops_out[op] = {
            "count": opd["count"], "ok": opd["ok"], "fail": opd["fail"],
            "keys": opd["keys"], "keys_ok": opd["_keys_ok"],
            "keys_fail": opd["_keys_fail"],
            # per-op histogram: CSV's stats-first path must be able to filter
            # by op (mixed put/get runs) without falling back to raw logs
            "hist": {str(b): n for b, n in sorted(opd["_h"].items())},
            "avg_ms": round(opd["_sum"] / opd["ok"], 6) if opd["ok"] else 0.0,
            "min_ms": opmin, "max_ms": opmax,
            "p50_ms": opct(0.50), "p90_ms": opct(0.90), "p95_ms": opct(0.95),
            "p99_ms": opct(0.99),
            "p999_ms": opct(0.999), "p9999_ms": opct(0.9999),
        }
        for column, threshold in zip(LATENCY_THRESHOLD_COLUMNS,
                                     LATENCY_THRESHOLDS_US):
            ops_out[op][column] = opd["_th"][threshold]

    # ---- 离群体系：第二遍扫描（阈值需在全局 p99 之后才知道） ----
    outliers = []
    outlier_thr = None
    if ok >= OUTLIER_MIN_SAMPLES:
        try:
            _p99 = pct(0.99)
        except Exception:
            _p99 = 0.0
        if _p99 and _p99 > 0:
            outlier_thr = _p99 * OUTLIER_FACTOR
            excl = {}       # sec -> [sum_excl, qsum_excl]
            mx = {}         # sec -> [max_non_outlier, qmax_non_outlier]
            for row in row_source(path):
                if row["status"] != "OK" or row["lat"] is None:
                    continue
                lat = row["lat"]
                ep = _row_epoch(row)
                if lat > outlier_thr:
                    if len(outliers) < LIST_CAP:
                        outliers.append({
                            "ts": row["ts"], "op": row["op"],
                            "key": row["key"],
                            "lat_ms": round(lat, 6), "status": "OK",
                            "ts_epoch": row.get("epoch")})
                    if ep is not None:
                        e = excl.setdefault(int(ep), [0.0, 0.0])
                        e[0] += lat
                        if row["qw"] is not None:
                            e[1] += row["qw"]
                    continue
                if ep is None:
                    continue
                s2 = int(ep)
                m = mx.get(s2)
                qw = row["qw"]
                if m is None:
                    mx[s2] = [lat, (qw if qw is not None else 0.0)]
                else:
                    if lat > m[0]:
                        m[0] = lat
                    if qw is not None and qw > m[1]:
                        m[1] = qw
            # 只改标量统计；c/ok/fail 与直方图保持全量
            for s2, buck in seconds.items():
                e = excl.get(s2)
                if e:
                    buck["sum"] = max(0.0, buck["sum"] - e[0])
                    buck["qsum"] = max(0.0, buck["qsum"] - e[1])
                m = mx.get(s2)
                buck["max"] = m[0] if m else 0.0
                buck["qmax"] = m[1] if m else 0.0

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

    def _acc_into(b, o):
        b["c"] += o["c"]
        b["ok"] += o["ok"]
        b["fail"] += o["fail"]
        b["sum"] += o["sum"]
        b["max"] = max(b["max"], o["max"])
        b["qsum"] += o["qsum"]
        b["qmax"] = max(b["qmax"], o["qmax"])
        for hk, hn in (o["h"] or {}).items():
            b["h"][hk] = b["h"].get(hk, 0) + hn

    # 收集尺度：最细的、桶数落在 [59, 8192] 的时间尺度；无达标尺度时回退 1s。
    emit_step = 1
    if ts_from is not None and ts_to is not None:
        _span = max(0.0, float(ts_to) - float(ts_from))
        for cand in TIER_STEPS:
            if MIN_TIER_BUCKETS <= _span / cand <= MAX_TIER_BUCKETS:
                emit_step = cand
                break
    if emit_step == 1:
        emit_buckets = seconds
    else:
        emit_buckets = {}
        for t in sorted(seconds):
            k = t // emit_step * emit_step
            b = emit_buckets.get(k)
            if b is None:
                b = emit_buckets[k] = {
                    "c": 0, "ok": 0, "fail": 0, "sum": 0.0, "max": 0.0,
                    "qsum": 0.0, "qmax": 0.0, "h": {}}
            _acc_into(b, seconds[t])

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
            "source": source_name or os.path.basename(path),
            "bytes": os.path.getsize(path),
            "node": node,
            "pod": pod,
            "role": role,
            "remote_path": remote_path or path,
            "size": size,
            "rows": rows,
            "ts_from": ts_from, "ts_to": ts_to,
            # 图表域（含 warmup）vs 正式统计域；cutoff 非空时两者不同
            "warmup_cutoff": (float(warmup_cutoff)
                              if warmup_cutoff is not None else None),
            "warmup_rows": warmup_rows,
            "warmup_fail": warmup_fail,
            "formal_ts_from": formal_ts_from,
            "formal_ts_to": formal_ts_to,
            "wall_s": round(ts_to - ts_from, 3) if ts_from and ts_to else None,
            "dated": ts_from is not None,
            # time-bucket t unit; "s" = epoch seconds (absent in old products,
            # whose minutes used epoch minutes -> reduce multiplies by 60)
            "bucket_unit": "s",
            # 本产物 seconds 行的桶步长（1/10/60）：map 按收集规则选最细达标尺度
            "bucket_step": emit_step,
            "outlier_threshold_ms": (round(outlier_thr, 6)
                                     if outlier_thr else None),
            # histogram ladder of this product (1 = legacy 81-bin ms ladder)
            "hist_version": HIST_VERSION,
        },
        "global": {
            "total": total, "ok": ok, "fail": fail,
            "key_total": key_total, "key_ok": key_ok, "key_fail": key_fail,
            "avg_ms": round(avg_ms, 6) if ok else 0.0,
            "min_ms": round(lat_min, 6) if lat_min is not None else 0.0,
            "max_ms": round(lat_max, 6) if lat_max is not None else 0.0,
            "p50_ms": pct(0.50), "p90_ms": pct(0.90), "p95_ms": pct(0.95),
            "p99_ms": pct(0.99),
            "p999_ms": pct(0.999), "p9999_ms": pct(0.9999),
            **{column: threshold_counts[threshold]
               for column, threshold in zip(LATENCY_THRESHOLD_COLUMNS,
                                            LATENCY_THRESHOLDS_US)},
            "qw_avg_ms": round(qw_sum / qw_n, 6) if qw_n else None,
            "qw_max_ms": round(qw_max, 6) if qw_n else None,
            "qw_n": qw_n,
        },
        "histogram": {str(b): n for b, n in sorted(hist.items())},
        "ops": ops_out,
        "summaries": (summaries if summaries is not None
                      else _scan_summaries(path)),
        "seconds": buckets_to_list(emit_buckets, 1),
        "errors": errors,
        "error_excerpt": (_read_context(path, first_error_line, context)
                          if first_error_line is not None else None),
        "error_excerpt_status": first_error_status,
        "slowest": top,
        "anomalies": anomalies,
        "abnormal": abnormal,
        "outliers": outliers,
    }
    if _digest_b64 or _op_digest_b64:
        stats["digest"] = {
            "fmt": _qt.FMT, "k": _dk,
            "global": _digest_b64,
            "ops": _op_digest_b64,
        }
    if extra_meta:
        stats["meta"].update(extra_meta)
    return stats


# ---------------------------------------------------------------------------
# kvtest map: request-sample CSV -> the same stats product as a client log
# ---------------------------------------------------------------------------
# Operation names in the kvtest summary/samples are the scenario config's
# native metric names (benchmark.KVTEST_WRITER_OPS maps the aliases onto them).
KVTEST_WRITE_OPS = frozenset(
    ("upsert", "upsert_from", "batch_upsert_from", "remove"))
KVTEST_READ_OPS = frozenset(
    ("get", "get_buffer", "batch_get_buffer", "exist"))


def kvtest_instance_ops_stats(path, ops, warmup_sec=None, digest_k=None,
                              start_epoch=None):
    """Streaming per-op FORMAL stats for one kvtest request-sample CSV.

    One pass, bounded memory: per op it returns exact counts/bytes/avg/min/max
    plus the shared percentile policy (hist interpolation <= p95, REQ sketch
    tails) — the SAME helper the report reduce uses, so CSV and report can
    never drift.  ``warmup_sec`` skips rows whose relative elapsed is inside
    the warmup region (single-window mode); ``digest_k`` enables the sketch.
    The per-op histogram/digest are returned so callers can merge scopes
    (e.g. one CSV TOTAL row) with identical semantics."""
    qt = _qt
    k = (qt.resolve_k(digest_k)
         if (qt is not None and digest_k is not None) else None)
    acc = {}
    for row in iter_kvtest_rows(path, start_epoch, ops):
        el = row.get("elapsed")
        if warmup_sec and el is not None and el < float(warmup_sec):
            continue
        op = row["op"]
        st = acc.get(op)
        if st is None:
            st = acc[op] = {"total": 0, "success": 0, "failure": 0,
                            "bytes": 0, "lat_sum": 0.0, "min": None,
                            "max": None, "hist": {},
                            "th": dict.fromkeys(LATENCY_THRESHOLDS_US, 0),
                            "sketch": (qt.make(k) if k is not None else None)}
        st["total"] += 1
        lat = row.get("lat")
        if row.get("status") == "OK" and lat is not None:
            st["success"] += 1
            st["bytes"] += int(row.get("bytes") or 0)
            st["lat_sum"] += lat
            st["min"] = lat if st["min"] is None else min(st["min"], lat)
            st["max"] = lat if st["max"] is None else max(st["max"], lat)
            b = bin_of(lat, BIN_EDGES_V2)
            st["hist"][b] = st["hist"].get(b, 0) + 1
            for threshold in LATENCY_THRESHOLDS_US:
                if lat > threshold / 1000.0:
                    st["th"][threshold] += 1
            if st["sketch"] is not None:
                qt.update(st["sketch"], lat)
        else:
            st["failure"] += 1
    out = {}
    for op, st in acc.items():
        b64 = (qt.serialize(st["sketch"])
               if (qt is not None and st["sketch"] is not None) else None)
        items = [b64] if b64 else None
        ok = st["success"]
        res = {
            "total": st["total"], "success": ok,
            "failure": st["failure"], "bytes": st["bytes"],
            "avg_ms": round(st["lat_sum"] / ok, 6) if ok else 0.0,
            "min_ms": round(st["min"], 6) if st["min"] is not None else 0.0,
            "max_ms": round(st["max"], 6) if st["max"] is not None else 0.0,
            # merge inputs for scope-level (TOTAL) percentiles
            "hist": {str(b): n for b, n in sorted(st["hist"].items())},
            **{column: st["th"][threshold]
               for column, threshold in zip(LATENCY_THRESHOLD_COLUMNS,
                                            LATENCY_THRESHOLDS_US)},
            "digest": b64,
        }
        for q, key in ((0.50, "p50_ms"), (0.90, "p90_ms"),
                       (0.95, "p95_ms"), (0.99, "p99_ms"),
                       (0.999, "p999_ms"), (0.9999, "p9999_ms")):
            v = unified_pct(st["hist"], items, q, gmax=st["max"])
            res[key] = round(v, 6) if v is not None else 0.0
        out[op] = res
    return out


def _kvtest_summary_doc(path):
    """Parse one C++ summary.json (None when missing/corrupt)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            doc = json.load(f)
        return doc if isinstance(doc, dict) else None
    except (OSError, ValueError):
        return None


def _apply_summary_metrics(stats, summary_doc):
    """Surface C++ aggregates the request samples cannot carry.

    value-verification failures and the notify queue wait exist only in
    summary.json (the samples CSV has no such columns)."""
    if not summary_doc:
        return
    try:
        verify = int(summary_doc.get("verify_failures") or 0)
    except (TypeError, ValueError):
        verify = 0
    if verify:
        stats["global"]["verify_failures"] = verify
    qw = (summary_doc.get("operations") or {}).get("notify_queue_wait") or {}
    try:
        avg = float(qw.get("avg_ms") or 0.0)
        mx = float(qw.get("max_ms") or 0.0)
        n = int(qw.get("total") or qw.get("count") or 0)
    except (TypeError, ValueError):
        return
    if n > 0 and (avg > 0 or mx > 0):
        stats["global"]["qw_avg_ms"] = round(avg, 6)
        stats["global"]["qw_max_ms"] = round(mx, 6)
        stats["global"]["qw_n"] = n


def _dir_apparent_bytes(path):
    """Apparent size of every entry under ``path`` (du -sb semantics).

    Computed inside the pod before the map writes anything, so it is the raw
    footprint of THIS process's directory (meta.bytes), never mixing roles.
    Uses lstat on purpose: glog_compat leaves convenience symlinks in
    ``<base>.glog.d/`` (``<prog>.INFO`` → the rotated segment). Following them
    with stat() counted every segment twice (~2× the real footprint); du -sb
    counts the link itself. Missing directory -> 0."""
    total = 0
    try:
        for root, _dirs, files in os.walk(str(path)):
            for name in files:
                try:
                    total += os.lstat(os.path.join(root, name)).st_size
                except OSError:
                    continue
    except OSError:
        return 0
    return total


def kvtest_map_instance(samples, summary_path, role, node=None, pod=None,
                        size=None, start_epoch=None, warmup_s=None,
                        digest_k=None, remote_path=None, iid=None,
                        instance_dir=None, out_stats=None, out_csvstats=None,
                        top_n=100):
    """Single-pass in-pod map for ONE kvtest process (split topology).

    Produces BOTH products from one read of latency_samples.csv:
      * the role stats product (hist/seconds/slowest/outliers/digest), and
      * the csvstats product (per-op FORMAL counts/avg/min/max for the
        scenario CSV; no digests — the CSV has only avg/max for notify/queue
        and instance-level percentiles come from the role product).
    summary.json is read in place on the same node for wall/verify/queue-wait
    (never transported). meta.bytes = apparent size of the instance directory
    captured BEFORE any map output is written. Missing samples/summary is an
    error — there is no fallback path."""
    if not os.path.isfile(samples):
        raise RuntimeError("kvtest map: samples not found: " + str(samples))
    if not os.path.isfile(summary_path):
        raise RuntimeError("kvtest map: summary not found: "
                           + str(summary_path))
    doc = _kvtest_summary_doc(summary_path)
    if doc is None:
        raise RuntimeError("kvtest map: summary unreadable: "
                           + str(summary_path))
    role = (role or "").strip().lower()
    role_ops = KVTEST_WRITE_OPS if role == "writer" else KVTEST_READ_OPS
    warm = float(warmup_s or 0)
    cutoff = ((float(start_epoch) + warm)
              if (start_epoch and warm > 0) else None)
    pod_bytes = _dir_apparent_bytes(instance_dir) if instance_dir else 0
    csv_ops = {}

    def _sink(row, in_warmup, okrow):
        if in_warmup:
            return                      # 正式窗口口径（与报表严格一致）
        st = csv_ops.get(row["op"])
        if st is None:
            st = csv_ops[row["op"]] = {"total": 0, "success": 0,
                                       "failure": 0, "bytes": 0,
                                       "lat_sum": 0.0, "min": None,
                                       "max": None}
        st["total"] += 1
        lat = row.get("lat")
        if okrow and lat is not None:
            st["success"] += 1
            st["bytes"] += int(row.get("bytes") or 0)
            st["lat_sum"] += lat
            st["min"] = lat if st["min"] is None else min(st["min"], lat)
            st["max"] = lat if st["max"] is None else max(st["max"], lat)
        else:
            st["failure"] += 1

    payload = kvtest_summary_payload(summary_path, role_ops)
    summaries = [("summary", payload)] if payload is not None else []
    stats = compute_stats(
        samples, top_n=top_n, node=node, pod=pod, role=role,
        remote_path=remote_path, size=size,
        rows_factory=lambda p: iter_kvtest_rows(p, start_epoch, None),
        summaries=summaries, source_name=os.path.basename(samples),
        digest_k=digest_k, warmup_cutoff=cutoff,
        op_filter=role_ops, row_sink=_sink,
        extra_meta={"format": "kvtest_samples", "scenario": "kvtest"})
    stats["meta"]["samples_bytes"] = os.path.getsize(samples)
    stats["meta"]["bytes"] = int(pod_bytes)
    # 单一大小口径：该字节数 = 进程现场目录 apparent size（已含 glog/样本），
    # 报表的「原始合计」不再叠加 trace 的 glog_bytes。
    stats["meta"]["size_mode"] = "dir"
    _apply_summary_metrics(stats, doc)
    ops_out = {}
    for op, st in csv_ops.items():
        ok = st["success"]
        ops_out[op] = {
            "total": st["total"], "success": ok,
            "failure": st["failure"], "bytes": st["bytes"],
            "avg_ms": round(st["lat_sum"] / ok, 6) if ok else 0.0,
            "min_ms": round(st["min"], 6) if st["min"] is not None else 0.0,
            "max_ms": round(st["max"], 6) if st["max"] is not None else 0.0,
        }
    try:
        elapsed = float(doc.get("elapsed_seconds") or 0.0)
    except (TypeError, ValueError):
        elapsed = 0.0
    # summary.json 只读一遍：notify/queue 指标只有它带（请求样本里没有），
    # 随 csvstats 产出一并回传，编排机无需再接触 summary。
    _sops = doc.get("operations") or {}
    _nop = ("notify_rpc" if "notify_rpc" in _sops else
            ("notify_http" if "notify_http" in _sops else None))
    _nt = (_sops.get(_nop) or {}) if _nop else {}
    _qw = _sops.get("notify_queue_wait") or {}

    def _num(d, key):
        try:
            return float(d.get(key) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    csvstats = {"instance_id": iid, "node": node, "pod": pod, "role": role,
                "size": size, "elapsed_seconds": elapsed,
                "warmup_s": warm, "meta_bytes": int(pod_bytes),
                "verify_failures": int(doc.get("verify_failures") or 0),
                "notify": ({"submitted": int(_nt.get("total") or 0),
                            "ok": int(_nt.get("success") or 0),
                            "fail": int(_nt.get("failure") or 0),
                            "avg_ms": _num(_nt, "avg_ms"),
                            "max_ms": _num(_nt, "max_ms")}
                           if _nt else None),
                "queue": ({"avg_ms": _num(_qw, "avg_ms"),
                           "max_ms": _num(_qw, "max_ms"),
                           "n": int(_qw.get("total") or _qw.get("count") or 0)}
                          if _qw else None),
                "ops": ops_out}
    return stats, csvstats


def _epoch_to_ts(epoch):
    """Absolute local wall clock as 'YYYY-MM-DD HH:MM:SS.micro' (log format)."""
    whole = int(epoch)
    frac = int(round((float(epoch) - whole) * 1e6))
    if frac >= 1000000:
        whole += 1
        frac -= 1000000
    return "{}.{:06d}".format(
        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(whole)), frac)


def iter_kvtest_rows(path, start_epoch, ops=None):
    """Yield RESULT-shaped rows from one kvtest request-sample CSV.

    Columns: elapsed_us,instance_id,operation,request_index,latency_ms,
    status,bytes. ``elapsed_us`` is relative to the C++ measurement window
    start (the orchestrator /start), so ``start_epoch`` rebuilds the same
    absolute timeline the Python client logs carry; time buckets, slowest
    lists and the report's time axis then share one clock.
    """
    import csv
    with open(path, "r", encoding="utf-8", errors="replace",
              newline="") as f:
        reader = csv.DictReader(f)
        for lineno, row in enumerate(reader, 2):
            op = (row.get("operation") or "").strip()
            if not op or (ops is not None and op not in ops):
                continue
            status = (row.get("status") or "").strip()
            try:
                lat = float(row.get("latency_ms"))
            except (TypeError, ValueError):
                lat = None
            if lat is None and status == "OK":
                continue        # an OK sample without a latency cannot be binned
            try:
                elapsed = float(row.get("elapsed_us") or 0.0) / 1e6
            except (TypeError, ValueError):
                elapsed = 0.0
            epoch = (float(start_epoch) + elapsed) if start_epoch else None
            ts = _epoch_to_ts(epoch) if epoch is not None else ""
            index = str(row.get("request_index") or "").strip()
            try:
                nbytes = int(float(row.get("bytes") or 0))
            except (TypeError, ValueError):
                nbytes = 0
            yield {"ts": ts, "op": op,
                   "key": ("req#" + index) if index else "",
                   "lat": lat, "status": status, "qw": None,
                   "kc": 1, "lineno": lineno, "epoch": epoch,
                   "bytes": nbytes, "elapsed": elapsed}


def kvtest_summary_payload(summary_path, ops=None):
    """Synthesize the SUMMARY payload the reduce stage expects, from summary.json.

    Only the consumed fields are kept (wall_s / bytes / max_depth).
    ``ops`` restricts the byte total to one role, so a dual instance's writer
    and reader products do not both claim the whole traffic.
    """
    try:
        with open(summary_path, "r", encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, ValueError):
        return None
    operations = doc.get("operations") or {}
    total_bytes = 0
    for name, payload in operations.items():
        if ops is not None and name not in ops:
            continue
        try:
            total_bytes += int((payload or {}).get("bytes", 0) or 0)
        except (TypeError, ValueError):
            pass
    try:
        wall = float(doc.get("elapsed_seconds") or 0.0)
    except (TypeError, ValueError):
        wall = 0.0
    return {"mode": "kvtest_summary", "wall_s": wall, "bytes": total_bytes,
            "max_depth": 0,
            "verify_failures": doc.get("verify_failures"),
            "operations": sorted(operations)}


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Compute compact stats JSON from one client log (map stage)")
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--log", help="client log file")
    src.add_argument("--kvtest-map-instance", action="store_true",
                     help="kvtest: single-pass in-pod map over "
                          "latency_samples.csv (stats + csvstats)")
    parser.add_argument("--kvtest-samples", default=None,
                        help="kvtest map: latency_samples.csv path")
    parser.add_argument("--out", default=None,
                        help="output path (default: <input>.stats.json)")
    parser.add_argument("--out-stats", default=None,
                        help="kvtest map: stats product output path")
    parser.add_argument("--out-csvstats", default=None,
                        help="kvtest map: csvstats product output path")
    parser.add_argument("--instance-dir", default=None,
                        help="kvtest map: per-process artifact dir "
                             "(meta.bytes = its apparent size, captured first)")
    parser.add_argument("--iid", type=int, default=None,
                        help="kvtest map: process instance id")
    parser.add_argument("--warmup-s", type=float, default=None,
                        help="kvtest map: warmup seconds (rows before the "
                             "cutoff stay chart-only / are excluded)")
    parser.add_argument("--start-epoch", type=float, default=None,
                        help="kvtest: epoch seconds of the /start trigger "
                             "(origin of elapsed_us)")
    parser.add_argument("--summary", default=None,
                        help="kvtest map: per-instance summary.json "
                             "(read in place for wall/verify/queue-wait)")
    parser.add_argument("--source-name", default=None,
                        help="meta.source override (default: input basename)")
    parser.add_argument("--digest-k", type=int, default=None,
                        help="REQ sketch k for percentile digests "
                             "(omit to disable the sketch)")
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
    if args.kvtest_map_instance:
        missing = [flag for flag, val in (
            ("--kvtest-samples", args.kvtest_samples),
            ("--summary", args.summary), ("--role", args.role),
            ("--instance-dir", args.instance_dir),
            ("--out-stats", args.out_stats),
            ("--out-csvstats", args.out_csvstats),
            ("--start-epoch", args.start_epoch)) if not val]
        if missing:
            parser.error("--kvtest-map-instance requires: "
                         + ", ".join(missing))
        stats, csvstats = kvtest_map_instance(
            args.kvtest_samples, args.summary, args.role,
            node=args.node, pod=args.pod, size=args.size,
            start_epoch=args.start_epoch, warmup_s=args.warmup_s,
            digest_k=args.digest_k, remote_path=args.remote_path,
            iid=args.iid, instance_dir=args.instance_dir,
            out_stats=args.out_stats, out_csvstats=args.out_csvstats,
            top_n=args.top_n)
        for path, doc in ((args.out_stats, stats),
                          (args.out_csvstats, csvstats)):
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False,
                          separators=(",", ":"))
            os.replace(tmp, path)
        g = stats["global"]
        print("[logstats] map-instance {} i{} rows={} ok={} fail={} "
              "p99={:.3f}ms bytes={} -> stats, csvstats".format(
                  args.role, args.iid, stats["meta"]["rows"], g["ok"],
                  g["fail"], g["p99_ms"], stats["meta"]["bytes"]))
        return 0
    src_path = args.log
    stats = compute_stats(src_path, top_n=args.top_n,
                          context=args.context, node=args.node,
                          pod=args.pod, role=args.role,
                          remote_path=args.remote_path, size=args.size,
                          source_name=args.source_name,
                          digest_k=args.digest_k)
    out = args.out or (src_path + ".stats.json")
    tmp = out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, out)
    g = stats["global"]
    print("[logstats] {} rows={} ok={} fail={} p99={:.3f}ms -> {}".format(
        os.path.basename(src_path), stats["meta"]["rows"], g["ok"], g["fail"],
        g["p99_ms"], out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
