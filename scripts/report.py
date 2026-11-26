#!/usr/bin/env python3
"""report.py — 报表 reduce + 渲染（纯 stdlib；ECharts 由调用方内联）。

map 阶段（节点/pod 内 logstats.py）产出每个客户端日志的 <log>.stats.json；
本模块把统计产物合并成**可独立打开的 HTML 报表**（无需 WebUI）：

  outputs/<ts>/logs/<cfg>/report/report.html   场景完整报表（自包含）
  outputs/<ts>/logs/<cfg>/report/data.js       全量数据（可选，深查懒加载）
  outputs/<ts>/report/overview.html            run 总览（场景摘要 + 链接）

设计要点：
  - 粒度按时长自适应：≤1 天嵌入 1s 桶；>1 天嵌入 1min（data.js 保留 1s）；
  - 分位数由「合并后的稀疏直方图」还原（跨客户端无损相加），不是平均各端 p99；
  - 报表内可悬停查看每个桶的全部统计量（替代逐点散点），Top-N 慢点表保留
    单点级证据；小 run（原始日志已拉回）另叠加逐点散点层；
  - 现场索引：每节点/每 pod 的宿主机路径、远程路径、日志体积与登录命令。
"""
import argparse
import csv
import calendar
import html
import math
import json
import os
import re
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
import logstats as ls   # noqa: E402  (histogram ladder + percentile shared)

EMBED_SECONDS_MAX = 86400        # ≤1 天：1s 桶内嵌；否则内嵌 1min
SCATTER_MAX_POINTS = 200000      # per-client raw scatter cap (small runs)
TOPK_DEFAULT = 20
TRACE_TOP_N = 100            # 慢点追踪 Top-N（后续可调 1000：map 侧 --top-n 同步）
TRACE_EMBED_POINTS = 192     # 节点/实例阶段桶嵌入抽稀点数（总计不抽稀）
TRACE_EMBED_ROWS = 60000     # 节点/实例阶段行嵌入预算（超出留 data.js）


# ---------------------------------------------------------------------------
# reduce
# ---------------------------------------------------------------------------

def load(paths):
    out = []
    for p in paths:
        st = None
        try:
            with open(p, "r", encoding="utf-8") as f:
                st = json.load(f)
        except (OSError, ValueError):
            st = None
        if st:
            st["_path"] = p
            try:
                st["_bytes"] = os.path.getsize(p)
            except OSError:
                st["_bytes"] = 0
            out.append(st)
    return out


def _role_of(st, fallback="all"):
    return (st.get("meta") or {}).get("role") or fallback


def _client_name(source):
    """Display name of a map product: kvtest ones end in .samples.csv."""
    return os.path.basename(str(source or "")).replace(
        ".log", "").replace(".samples.csv", "")


def _row_ts_epoch(row):
    """Absolute epoch of a listed request: the map may already know it.

    Node-side map products only carry the node wall-clock ts; kvtest products
    are mapped on the orchestrator and ship an explicit ts_epoch, which must
    win (re-deriving it with the UTC assumption would shift it)."""
    ep = row.get("ts_epoch")
    if ep is not None:
        return ep
    return _node_ts_epoch(row.get("ts"))


def _hist_edges(stats):
    """Ladder for these map products (v2 unless any legacy v1 file is mixed in).

    Legacy stats.json carries no meta.hist_version and was binned with the
    81-bin 2160ms ladder; new products use the extended 250s ladder."""
    versions = {(st.get("meta") or {}).get("hist_version") for st in stats}
    if versions and versions <= {2}:
        return ls.BIN_EDGES_V2
    return ls.BIN_EDGES


def _minutes_rows(st):
    """Minute-bucket rows with t normalized to epoch seconds.

    New products carry meta.bucket_unit="s"; older ones stored epoch MINUTES
    in the minute buckets (report.py used to plot them as seconds -> 1970)."""
    rows = st.get("minutes") or []
    if (st.get("meta") or {}).get("bucket_unit") == "s":
        return rows
    out = []
    for row in rows:
        r = list(row)
        r[0] = int(r[0]) * 60
        out.append(r)
    return out


def _merge_buckets(bucket_lists):
    """Merge [t,c,ok,fail,sum,max,qsum,qmax,hist] bucket rows across clients."""
    merged = {}
    for buckets in bucket_lists:
        for row in buckets or []:
            t = int(row[0])
            b = merged.get(t)
            if b is None:
                b = merged[t] = _new_bucket()
            _acc_bucket(b, row)
    return merged


# ---------------------------------------------------------------------------
# 分桶引擎（段2）：时间阶梯 + 采样数量分桶
#   成熟方案（Thanos/Grafana）的关键是「每序列点数封顶」+「分级预聚合」，
#   序列数则靠预算分配控制；横轴永远是真时间轴。
# ---------------------------------------------------------------------------
# 分桶体系（S1）：map 只产出时间片桶（1s/10s/1min），收集规则：
#   尺度 s 可回传 ⇔ 桶数 span/s ∈ [MIN_TIER_BUCKETS, MAX_TIER_BUCKETS]
# 自适应 = 仅基于可用时间尺度做「折叠」，使展示点数尽量 2048。
TIER_STEPS = [1, 10, 60]
TIER_LABELS = {1: "1 秒", 10: "10 秒", 60: "1 分钟"}
COUNT_KEY = "cnt"
COUNT_LABEL = "自适应"
MIN_TIER_BUCKETS = 59         # 下限 60；用 59 判定避免 60 边界清空
MAX_TIER_BUCKETS = 8192       # 上限：更粗尺度足够时舍弃过细尺度
ADAPTIVE_MIN_BUCKETS = 2048   # 自适应目标下界（尽量贴近）
ADAPTIVE_MAX_BUCKETS = 4096   # 不折叠时允许的最大桶数
EMBED_POINTS_MAX = 2048       # 每序列嵌入点数上限（Grafana/uPlot 同量级）
EMBED_ROW_BUDGET = 150000     # report.html 内嵌点行总预算
DATAJS_AGG_MAX = 100000       # data.js 单个聚合序列点数上限
DATAJS_ROW_BUDGET = 2000000   # data.js 点行总预算
EMBED_CLIENTS_TOPK = 20


def _new_bucket():
    return [0, 0, 0, 0.0, 0.0, 0.0, 0.0, {}]


def _acc_bucket(b, row):
    """Accumulate one bucket row into b=[c,ok,fail,sum,max,qsum,qmax,hist]."""
    b[0] += int(row[1])
    b[1] += int(row[2])
    b[2] += int(row[3])
    b[3] += float(row[4])
    b[4] = max(b[4], float(row[5]))
    b[5] += float(row[6])
    b[6] = max(b[6], float(row[7]))
    for k, n in (row[8] or {}).items():
        b[7][int(k)] = b[7].get(int(k), 0) + int(n)


def _merge_bucket(b, o):
    """Merge bucket into bucket (both are [c,ok,fail,sum,max,qsum,qmax,hist])."""
    b[0] += o[0]
    b[1] += o[1]
    b[2] += o[2]
    b[3] += o[3]
    b[4] = max(b[4], o[4])
    b[5] += o[5]
    b[6] = max(b[6], o[6])
    for k, n in (o[7] or {}).items():
        b[7][int(k)] = b[7].get(int(k), 0) + int(n)
    return b


def _tier_rows(rows, step):
    """Aggregate 1s bucket rows into time buckets of `step` seconds."""
    out = {}
    for row in rows or []:
        k = (int(row[0]) // step) * step
        b = out.get(k)
        if b is None:
            b = out[k] = _new_bucket()
        _acc_bucket(b, row)
    return out


def _available_scales(span, base_step=1):
    """可用的时间尺度：从产物最细步长起，桶数 ∈ [59, 8192]。

    短 run 没有任何尺度达标时回退到最细步长（否则无图可画）。"""
    span = float(span or 0)
    base_step = int(base_step or 1)
    out = []
    for s in TIER_STEPS:
        if s < base_step:
            continue
        if span > 0 and MIN_TIER_BUCKETS <= span / s <= MAX_TIER_BUCKETS:
            out.append(s)
    return out or [base_step]


def _adaptive_step(span, scales):
    """自适应折叠：仅用时间桶，选使展示点数尽量贴近 2048 的方案。

    每个可用尺度产生一个候选：
      n <= 4096         -> 原样（点数 = n）
      n >  4096         -> 选整数 k 使 n/k ∈ [2048,4096] 且最接近 2048
    所有候选中取 |点数-2048| 最小者；并列时取更细尺度（遍历顺序）。
    """
    span = float(span or 0)
    best = None                       # (distance, step)
    for s in scales:
        if span <= 0:
            continue
        n = int(math.ceil(span / float(s)))
        if n <= 0:
            continue
        if n <= ADAPTIVE_MAX_BUCKETS:
            cand = (abs(n - ADAPTIVE_MIN_BUCKETS), s)
        else:
            kbest, mbest = None, None
            for k in range(2, 65):
                m = n / float(k)
                if m < ADAPTIVE_MIN_BUCKETS:
                    break
                if m <= ADAPTIVE_MAX_BUCKETS:
                    if (kbest is None
                            or abs(m - ADAPTIVE_MIN_BUCKETS)
                            < abs(n / float(kbest) - ADAPTIVE_MIN_BUCKETS)):
                        kbest, mbest = k, m
            if kbest is None:
                kbest = max(2, int(math.ceil(n / float(ADAPTIVE_MAX_BUCKETS))))
                mbest = n / float(kbest)
            cand = (abs(int(round(mbest)) - ADAPTIVE_MIN_BUCKETS),
                    s * kbest)
        if best is None or cand[0] < best[0]:
            best = cand
    return best[1] if best else (scales[0] if scales else 1)


def _adaptive_hint(step, scales, span):
    """按钮 tooltip：折叠方案与近似点数。"""
    for s in reversed(scales):
        if step % s == 0:
            k = step // s
            n = int(math.ceil(float(span) / step)) if span else 0
            plan = "{} 秒".format(s) if k <= 1 else "{} 秒 × {}".format(s, k)
            return "{} ≈ {} 点".format(plan, n)
    return "自适应"


def _tier_policy(span, base_step=1):
    """(offered scales, embed step) for a run span in seconds."""
    span = float(span or 0)
    scales = _available_scales(span, base_step)
    embed = next((s for s in scales if span / s <= EMBED_POINTS_MAX),
                 scales[-1])
    return scales, embed


def _series_columns(merged, step, edges=None):
    """Columnar series for the chart (compact JSON, fixed key order).

    Per-bucket percentiles come from the merged per-second histograms
    (single policy: histogram interpolation — the same center path the
    summaries use; the chain has no raw-sample pool any more)."""
    edges = ls.BIN_EDGES if edges is None else edges
    out = {"t": [], "c": [], "ok": [], "fail": [], "avg": [], "p50": [],
           "p99": [], "p999": [], "p9999": [], "min": [], "max": [],
           "qw": [], "qwmax": []}
    prev_t = None
    for t in sorted(merged):
        c, ok, fail, s, mx, qs, qm, h = merged[t]
        out["t"].append(int(t))
        out["c"].append(int(c))
        out["ok"].append(int(ok))
        out["fail"].append(int(fail))
        out["avg"].append(round(s / ok, 4) if ok else 0.0)
        out["p50"].append(round(ls.hist_percentile(h, 0.50, edges), 4)
                          if h else 0.0)
        out["p99"].append(round(ls.hist_percentile(h, 0.99, edges), 4)
                          if h else 0.0)
        out["p999"].append(round(ls.hist_percentile(h, 0.999, edges), 4)
                           if h else 0.0)
        out["p9999"].append(round(ls.hist_percentile(h, 0.9999, edges), 4)
                            if h else 0.0)
        # min：直方图最低非空分箱的下界（对数分箱 -> 近似最小值）
        mn = 0.0
        if h:
            try:
                mn = float(edges[min(int(k) for k in h)])
            except (ValueError, IndexError):
                mn = 0.0
        out["min"].append(round(mn, 4))
        out["max"].append(round(mx, 4))
        # logstats accumulates queue wait on successful rows only -> divide
        # by ok (not by every row, which understated it by the failure rate)
        out["qw"].append(round(qs / ok, 4) if ok else 0.0)
        out["qwmax"].append(round(qm, 4))
        prev_t = t
    return out


def _summary_of(stats):
    """Cluster summary from merged per-instance globals.

    Percentiles follow the single pipeline policy (ls.unified_pct): center
    from the merged histogram, tails from the merged REQ sketches."""
    combos = combos_of(stats)
    total = ok = fail = ok_rows = 0
    lat_sum = 0.0
    qw_sum = 0.0
    qw_n = 0
    qw_max = 0.0
    total_bytes = 0
    max_depth = 0
    verify_failures = 0
    ts_from = ts_to = None
    fts_from = fts_to = None
    has_cutoff = False
    hist = {}
    _digest_items = []
    _pct_k = None
    wall = 0.0
    gmax = gmin = None
    for st in stats:
        g = st.get("global") or {}
        meta = st.get("meta") or {}
        if g.get("max_ms") is not None:
            gmax = float(g["max_ms"]) if gmax is None else max(gmax,
                                                               float(g["max_ms"]))
        if g.get("min_ms") is not None:
            gmin = float(g["min_ms"]) if gmin is None else min(gmin,
                                                               float(g["min_ms"]))
        total += g.get("key_total") or 0
        ok += g.get("key_ok") or 0
        fail += g.get("key_fail") or 0
        verify_failures += int(g.get("verify_failures") or 0)
        rows_ok = g.get("ok") or 0
        ok_rows += rows_ok
        lat_sum += (g.get("avg_ms") or 0.0) * rows_ok
        if g.get("qw_avg_ms") is not None:
            n = g.get("qw_n")
            n = rows_ok if n is None else n
            qw_sum += g["qw_avg_ms"] * n
            qw_n += n
            qw_max = max(qw_max, g.get("qw_max_ms") or 0.0)
        if meta.get("ts_from") is not None:
            ts_from = meta["ts_from"] if ts_from is None else min(
                ts_from, meta["ts_from"])
        if meta.get("ts_to") is not None:
            ts_to = meta["ts_to"] if ts_to is None else max(
                ts_to, meta["ts_to"])
        if meta.get("warmup_cutoff") is not None:
            has_cutoff = True
        if meta.get("formal_ts_from") is not None:
            fts_from = meta["formal_ts_from"] if fts_from is None else min(
                fts_from, meta["formal_ts_from"])
        if meta.get("formal_ts_to") is not None:
            fts_to = meta["formal_ts_to"] if fts_to is None else max(
                fts_to, meta["formal_ts_to"])
        for b, n in (st.get("histogram") or {}).items():
            hist[int(b)] = hist.get(int(b), 0) + n
        _dg = (st.get("digest") or {})
        if _dg.get("global"):
            _digest_items.append(_dg["global"])
            if _pct_k is None and _dg.get("k"):
                _pct_k = _dg.get("k")
        for kind, payload in st.get("summaries") or []:
            if kind != "summary":
                continue
            max_depth = max(max_depth, int(payload.get("max_depth", 0) or 0))
            total_bytes += int(payload.get("bytes", 0) or 0)
            wall = max(wall, float(payload.get("wall_s", 0) or 0))
    span = (ts_to - ts_from) if (ts_from is not None and ts_to is not None) else 0.0
    fspan = (fts_to - fts_from) if (fts_from is not None
                                    and fts_to is not None) else 0.0
    if has_cutoff and fspan > 0:
        # warmup 并入窗口：正式时长 = 正式域跨度（C++ elapsed_seconds 含
        # warmup 区，不能直接用于 qps/吞吐口径）
        wall = fspan
    else:
        wall = wall or span

    edges = _hist_edges(stats)

    def pct(q, key=None):
        # Single policy (scripts/quantiles.py): center <= p95 from the merged
        # histogram, tails from the merged REQ sketches; never above the true
        # maximum.  N/A only when neither source exists.
        v = ls.unified_pct(hist, _digest_items, q, edges, gmax=gmax)
        return None if v is None else round(v, 4)

    _pct_method = ("hist+digest" if (_digest_items and ls.have_sketch())
                   else "hist")
    return {
        "pct_method": _pct_method, "pct_k": _pct_k,
        "total": total, "ok": ok, "fail": fail,
        "fail_rate": (fail / total) if total else 0.0,
        "hist_bins": [[int(k), int(v)] for k, v in sorted(hist.items())],
        "bin_edges": [round(float(x), 3) for x in edges],
        "avg_ms": round(lat_sum / ok_rows, 4) if ok_rows else 0.0,
        "min_ms": round(gmin, 4) if gmin is not None else 0.0,
        "p50_ms": pct(0.50, "p50_ms"),
        "p90_ms": pct(0.90, "p90_ms"),
        "p95_ms": pct(0.95, "p95_ms"),
        "p99_ms": pct(0.99, "p99_ms"),
        "p999_ms": pct(0.999, "p999_ms"),
        "p9999_ms": pct(0.9999, "p9999_ms"),
        "max_ms": round(gmax, 4) if gmax is not None else 0.0,
        "qw_avg_ms": round(qw_sum / qw_n, 4) if qw_n else None,
        "qw_max_ms": round(qw_max, 4) if qw_n else None,
        "max_depth": max_depth,
        "verify_failures": verify_failures,
        "wall_s": round(wall, 3),
        "qps": round(total / wall, 2) if wall > 0 else 0.0,
        # None when the logs carry no timestamps (legacy) -> the card shows
        # "-" instead of Infinity (JS must not divide by a zero wall)
        "bytes": total_bytes,
        "throughput_mb_s": (round(total_bytes / wall / 1048576.0, 6)
                            if wall > 0 else None),
        "clients": len(stats),
        "combos": combos,
        "ts_from": ts_from, "ts_to": ts_to,
    }


def _client_entry(st, role):
    g = st.get("global") or {}
    meta = st.get("meta") or {}
    return {
        "name": _client_name(meta.get("source")),
        "node": meta.get("node") or "",
        "pod": meta.get("pod") or "",
        "role": role,
        "source": meta.get("source") or "",
        "remote_path": meta.get("remote_path") or "",
        "bytes": meta.get("bytes") or 0,
        "map_bytes": st.get("_bytes") or 0,
        "total": g.get("key_total") or 0,
        "ok": g.get("key_ok") or 0,
        "fail": g.get("key_fail") or 0,
        "avg_ms": g.get("avg_ms") or 0.0,
        "p50_ms": g.get("p50_ms") or 0.0,
        "p95_ms": g.get("p95_ms") or 0.0,
        "p99_ms": g.get("p99_ms") or 0.0,
        "p999_ms": g.get("p999_ms") or 0.0,
        "max_ms": g.get("max_ms") or 0.0,
        "qw_avg_ms": g.get("qw_avg_ms"),
        "qw_max_ms": g.get("qw_max_ms"),
        "max_depth": next((int(p.get("max_depth", 0) or 0)
                           for k, p in st.get("summaries") or []
                           if k == "summary"), 0),
        "verify_failures": int(g.get("verify_failures") or 0),
        "slowest": st.get("slowest") or [],
        "errors": st.get("errors") or {},
    }


def _role_label(role):
    return {"all": "总体（全部）", "writer": "writer", "reader": "reader"}.get(
        role, role)


def _obj_seconds(stats_list):
    """All 1s bucket rows of a group of clients (merged later)."""
    out = []
    for st in stats_list:
        out.extend(st.get("seconds") or [])
    return out


def _samples(rows):
    return sum(int(r[1]) for r in rows or [])


# ---------------------------------------------------------------------------
# Exact pooled percentiles (kvtest). The histogram merge reconstructs
# cross-instance percentiles from log-ladder counts; kvtest map products
# instead self-describe their raw per-request CSV (meta.format =
# "kvtest_samples" and product _path == "<samples>.<role>.stats.json"), so
# every merged percentile of a kvtest scope is recomputed from the POOLED
# RAW SAMPLES — a plain sort, an absolute true value. A scope whose samples
# are unavailable (older binary / file deleted) degrades its percentiles to
# null ("N/A"), NEVER to a histogram approximation.
# ---------------------------------------------------------------------------

def _obj_tiers(rows, edges, offered, min_step, adaptive_step):
    """Build the tier dict for one chart object.

    min_step: finest time tier to materialise (= embed tier for nodes,
    data.js allowance for roles). adaptive_step: 自适应档的折叠步长（秒）。
    Per-bucket percentiles use the merged per-second histograms."""
    tiers = {}
    for s in offered:
        if s < min_step:
            continue
        tiers[str(s)] = _series_columns(_tier_rows(rows, s), s, edges)
    if adaptive_step:
        tiers[COUNT_KEY] = _series_columns(
            _tier_rows(rows, adaptive_step), adaptive_step, edges)
    return tiers


def _tier_rowcount(tiers):
    return sum(len((cols or {}).get("t") or []) for cols in (tiers or {}).values())


_COMBO_RE = re.compile(r"benchmark_c(\d+)_t(\d+)")


def combos_of(stats):
    """参数组（num_clients x num_threads）-> 有数据的 stats 文件数。

    benchmark.py 把每个 (num_clients, num_threads) 组合写成一个独立的客户端
    日志 `<name>.benchmark_c{C}_t{T}_{size}.log`，因此文件名本身就是参数组的
    权威来源。多组参数（sweep）时，报表顶部若按「单组」口径展示 QPS/客户端数
    会误导（如 c1t1+c2t4+c4t8 合并后写「客户端 2」），这里把组合列出来供
    前端做选择器与口径标注。解析失败返回空列表（旧版/自定义命名）。
    """
    out = {}
    for st in stats:
        src = str((st.get("meta") or {}).get("source") or "")
        m = _COMBO_RE.search(src)
        if not m:
            continue
        key = (int(m.group(1)), int(m.group(2)))
        out[key] = out.get(key, 0) + 1
    return [{"c": c, "t": t, "files": n,
             "label": "{} 客户端 × {} 线程".format(c, t)}
            for (c, t), n in sorted(out.items())]


def _node_ts_epoch(ts):
    """节点日志钟面时间 'YYYY-MM-DD HH:MM:SS.micro' -> epoch 秒。

    客户端把 glog/benchmark 行写成**节点本地钟面时间**（本 lab 的 openEuler
    节点时区是 UTC，日志里是 03:21 而宿主 WSL 本地是 11:21）。报表侧
    ts_from/seconds 桶都用 `time.mktime`（宿主本地时区）解析同一批字符串，
    两者互相抵消才得到正确 epoch；但 slowest[].ts 的字符串被直接送到
    浏览器后，`tsEpoch()` 又按浏览器本地时区解析一遍，就会整体偏 8 小时。

    这里用 UTC 解释节点串：对 UTC 节点正确；对与宿主同时区的节点，epoch
    与 mktime 结果也一致（同为墙钟 -> 绝对时刻的换算）。无法解析返回 None。

    注意：本函数只修 slowest 的绝对时刻；主图/慢点图 x 轴用的 seconds 桶
    由 map 阶段在**节点上**计算（与节点时区一致）。若 stats 是在宿主本地
    重算的（宿主时区 != 节点时区），两者会有偏差——此时以 slowest 的
    ts_epoch 为准更可靠，或统一用节点侧 map 产物。
    """
    if not ts:
        return None
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})"
                 r"(?:\.(\d+))?", str(ts))
    if not m:
        return None
    frac = float("0." + m.group(7)) if m.group(7) else 0.0
    try:
        y, mo, d, h, mi, s = (int(m.group(i)) for i in range(1, 7))
        return calendar.timegm((y, mo, d, h, mi, s, 0, 0, 0)) + frac
    except (ValueError, OverflowError):
        return None


def reduce_size(stats, meta):
    """One data-size block: summary / chart objects (tiered series) / clients."""
    role_stats = {}
    node_stats = {}
    for st in stats:
        role_stats.setdefault(_role_of(st), []).append(st)
        node_stats.setdefault((st.get("meta") or {}).get("node") or "?",
                              []).append(st)
    clients = [_client_entry(st, _role_of(st)) for st in stats]
    errors = {}
    for st in stats:
        for k, n in (st.get("errors") or {}).items():
            errors[k] = errors.get(k, 0) + int(n)
    slowest = []
    for st in stats:
        _m = st.get("meta") or {}
        _node = _m.get("node") or ""
        _role = _m.get("role") or ""
        _client = _client_name(_m.get("source"))
        for row in (st.get("slowest") or []):
            row = dict(row)
            row["node"] = _node
            row["role"] = _role
            row["client"] = _client
            # 归一绝对时刻：map 阶段在节点上跑，节点时区可能与报表宿主
            # 不同（本 lab 节点是 UTC，WSL 宿主是 CST）。raw `ts` 是节点
            # 本地钟面时间，直接在前端用本地时区解析会整体偏移。这里按
            # 节点写日志时的时区（UTC）换算成 epoch，前端只用 ts_epoch，
            # 与 seconds 桶（同为绝对时刻）口径一致。
            row["ts_epoch"] = _row_ts_epoch(row)
            slowest.append(row)
    slowest.sort(key=lambda r: -float(r.get("lat_ms") or 0))
    slowest = slowest[:100]
    # 三清单：异常请求（未完成/失败）与离群值（>p99×10），逐客户端合并
    outliers, abnormal = [], []
    for st in stats:
        m = st.get("meta") or {}
        node = m.get("node") or ""
        role = m.get("role") or ""
        client = _client_name(m.get("source"))
        for row in (st.get("outliers") or []):
            r = dict(row)
            r["ts_epoch"] = _row_ts_epoch(r)
            r["node"], r["role"], r["client"] = node, role, client
            outliers.append(r)
        for row in (st.get("abnormal") or []):
            r = dict(row)
            r["ts_epoch"] = _row_ts_epoch(r)
            r["node"], r["role"], r["client"] = node, role, client
            abnormal.append(r)
    outliers.sort(key=lambda r: r.get("ts_epoch") or 0)
    abnormal.sort(key=lambda r: r.get("ts_epoch") or 0)
    anomalies = []
    seen = set()
    for st in stats:
        _m = st.get("meta") or {}
        _node = _m.get("node") or ""
        _role = _m.get("role") or ""
        _cl = _client_name(_m.get("source"))
        for a in st.get("anomalies") or []:
            key = (a.get("t"), a.get("kind"), _node)
            if key in seen:
                continue
            seen.add(key)
            a = dict(a)
            a.setdefault("node", _node)
            a.setdefault("role", _role)
            a.setdefault("client", _cl)
            anomalies.append(a)
    anomalies.sort(key=lambda a: a.get("t") or 0)
    excerpt = None
    excerpt_status = None
    best = -1
    for st in stats:
        n = sum(int(x) for x in (st.get("errors") or {}).values())
        if n > best and st.get("error_excerpt"):
            best = n
            excerpt = st["error_excerpt"]
            excerpt_status = st.get("error_excerpt_status")
    summary = _summary_of(stats)
    _cuts = [float((st.get("meta") or {}).get("warmup_cutoff"))
             for st in stats
             if (st.get("meta") or {}).get("warmup_cutoff") is not None]
    warmup_cutoff = min(_cuts) if _cuts else None
    span = 0.0
    if summary.get("ts_from") is not None and summary.get("ts_to") is not None:
        span = float(summary["ts_to"]) - float(summary["ts_from"])
    # 产物最细步长（map 侧按收集规则决定；旧产物无该字段 = 1s）
    base_step = max(int((st.get("meta") or {}).get("bucket_step") or 1)
                    for st in stats)
    offered, embed = _tier_policy(span, base_step)
    adaptive = _adaptive_step(span, offered)
    edges = _hist_edges(stats)
    # roles may carry the finer tiers in data.js when affordable
    fine_roles = offered[0]
    if span > 0:
        fine_roles = next((s for s in offered
                           if span / s <= DATAJS_AGG_MAX), offered[-1])
    objects = {}
    used = 0
    # 总体（全部）：跨角色的整体聚合。客户端都有 reader/writer 角色时
    # 也要有这一条 —— 对象树的顶层与「总体 vs 节点 vs 客户端」对比
    # 都依赖它（否则总体曲线根本不存在）。
    all_rows = _obj_seconds(stats)
    all_tiers = _obj_tiers(all_rows, edges, offered, fine_roles, adaptive)
    used += _tier_rowcount(all_tiers)
    # basic 是「单场景 sweep」，顶层对象就是所有节点/实例的平均 —— 叫「总览」；
    # pipeline 保留「总体（全部）」（其对象树还有 writer/reader 角色层）。
    all_label = ("总览" if (meta.get("scenario") == "basic"
                            or meta.get("mode") == "basic")
                 else _role_label("all"))
    objects["all"] = {"label": all_label, "kind": "role",
                      "tiers": all_tiers}
    for role, group in sorted(role_stats.items()):
        if role == "all":
            continue          # 与「总体（全部）」是同一条，不重复建
        rows = _obj_seconds(group)
        tiers = _obj_tiers(rows, edges, offered, fine_roles, adaptive)
        used += _tier_rowcount(tiers)
        objects[role] = {"label": _role_label(role), "kind": "role",
                         "tiers": tiers}
    # nodes: embed-tier upward only (bounded regardless of node count)
    # pipeline：同一节点的 writer/reader 分开建对象（角色单选下互不混用）
    is_pipe = _scenario_of(meta) == "pipeline"
    for node, group in sorted(node_stats.items()):
        if is_pipe:
            by_role = {}
            for st in group:
                by_role.setdefault(_role_of(st), []).append(st)
            for role, gg in sorted(by_role.items()):
                rows = _obj_seconds(gg)
                tiers = _obj_tiers(rows, edges, offered, embed, adaptive)
                used += _tier_rowcount(tiers)
                node_bytes = sum(int((st.get("meta") or {}).get("bytes") or 0)
                                 for st in gg)
                objects["node:{}:{}".format(node, role)] = {
                    "label": node, "kind": "node", "node": node,
                    "role": role, "bytes": node_bytes, "tiers": tiers}
        else:
            rows = _obj_seconds(group)
            tiers = _obj_tiers(rows, edges, offered, embed, adaptive)
            used += _tier_rowcount(tiers)
            node_bytes = sum(int((st.get("meta") or {}).get("bytes") or 0)
                             for st in group)
            objects["node:" + node] = {"label": node, "kind": "node",
                                       "node": node, "bytes": node_bytes,
                                       "tiers": tiers}
    # clients: 与角色/节点同一套时间尺度 + 自适应
    for st, c in zip(stats, clients):
        rows = st.get("seconds") or []
        tiers = _obj_tiers(rows, edges, offered, fine_roles, adaptive)
        objects["client:" + c["name"]] = {
            "label": c["name"], "kind": "client", "node": c["node"],
            "role": c["role"], "p99_ms": c["p99_ms"], "tiers": tiers}
    # pipeline：writer/reader 各自一套摘要（段1 分角色展示；basic 为空）
    role_summaries = {}
    for role, group in role_stats.items():
        if role in ("writer", "reader"):
            role_summaries[role] = _summary_of(group)
    tiers_meta = [{"key": str(s), "label": TIER_LABELS[s], "step": s}
                  for s in offered]
    tiers_meta.append({"key": COUNT_KEY, "label": COUNT_LABEL,
                       "step": adaptive,
                       "hint": _adaptive_hint(adaptive, offered, span)})
    embed_tiers = [str(s) for s in offered if s >= embed] + [COUNT_KEY]
    return {
        "summary": summary,
        "span": round(span, 1),
        # 统计口径（报告顶部注记 + 前端说明）：hist+digest / hist
        "pct_method": summary.get("pct_method"),
        "pct_k": summary.get("pct_k"),
        # warmup 结束点（epoch，图表域）：前端在该位置画一条竖线
        "warmup_cutoff": warmup_cutoff,
        "tiers": tiers_meta,
        "role_summaries": role_summaries,
        "embed_tier": embed,
        "embed_step": embed,       # compat alias
        "embed_tiers": embed_tiers,
        "objects": objects,
        "clients": clients,
        "errors": errors,
        "error_excerpt": excerpt,
        "error_excerpt_status": excerpt_status,
        "slowest": slowest,
        "anomalies": anomalies,
        "outliers": outliers,
        "abnormal": abnormal,
    }


def _sources(stats, traces=None):
    out = []
    for st in stats:
        meta = st.get("meta") or {}
        out.append({"node": meta.get("node") or "?",
                    "pod": meta.get("pod") or "",
                    "role": meta.get("role") or "",
                    "source": meta.get("source") or "",
                    "remote_path": meta.get("remote_path") or "",
                    "bytes": int(meta.get("bytes") or 0),
                    "kind": "log"})
    for st in traces or []:
        meta = st.get("meta") or {}
        out.append({"node": meta.get("node") or "?",
                    "pod": meta.get("pod") or "",
                    "role": meta.get("role") or "",
                    "source": meta.get("source") or "",
                    "remote_path": meta.get("remote_path") or "",
                    "bytes": int(meta.get("bytes") or 0),
                    "kind": "glog"})
    out.sort(key=lambda s: (s["node"], s["pod"], s["source"]))
    nodes = {}
    for s in out:
        agg = nodes.setdefault(s["node"], {"node": s["node"], "bytes": 0,
                                           "_pods": set()})
        agg["bytes"] += s["bytes"]
        # client count = distinct LOG sources (one per client); k8s pods use
        # the pod name, ssh falls back to the log basename. glog entries only
        # contribute bytes (they are the same clients).
        if s.get("kind") == "log":
            agg["_pods"].add(s["pod"] or s["source"])
    node_list = [{"node": n["node"], "bytes": n["bytes"],
                  "pods": len(n["_pods"])} for n in nodes.values()]
    return out, sorted(node_list, key=lambda n: -n["bytes"])


# ---------------------------------------------------------------------------
# C++ SDK trace reduce (breakdown[api] main lines; one file per client glog)
# ---------------------------------------------------------------------------

TRACE_META_KEYS = ("lines", "trace_lines", "slice_lines", "parse_errors")


def _merge_field(a, b):
    if a is None:
        if not b:
            return None
        # deep-copy hist: the returned dict is merged into by the NEXT file,
        # and a shallow copy would write straight back into the loaded stats
        out = dict(b)
        out["hist"] = dict(b.get("hist") or {})
        return out
    out = a
    out["n"] = (out.get("n") or 0) + (b.get("n") or 0)
    out["neg"] = (out.get("neg") or 0) + (b.get("neg") or 0)
    out["sum"] = (out.get("sum") or 0.0) + (b.get("sum") or 0.0)
    out["sumsq"] = (out.get("sumsq") or 0.0) + (b.get("sumsq") or 0.0)
    for k in ("min", "max"):
        v = b.get(k)
        if v is not None:
            out[k] = v if out.get(k) is None else (
                min(out[k], v) if k == "min" else max(out[k], v))
    h = out.setdefault("hist", {})
    for bin_, n in (b.get("hist") or {}).items():
        h[str(bin_)] = h.get(str(bin_), 0) + int(n)
    return out


def _field_pcts(fd, edges=None, hist_ok=True):
    """Field stats -> report row. `edges` is the ladder the bins were built
    with (default ms); trace values are µs. When a legacy trace product is
    present (no meta.bins) histogram percentiles are left None instead of
    being silently misread."""
    hist = {int(k): v for k, v in (fd.get("hist") or {}).items()}
    n = fd.get("n") or 0

    def pct(q):
        if not hist_ok or not hist:
            return None
        return round(ls.hist_percentile(hist, q, edges), 4)

    neg = fd.get("neg") or 0
    avg_n = n - neg          # -1 sentinel samples carry no duration
    avg = (fd.get("sum") or 0.0) / avg_n if avg_n else 0.0
    sigma = None
    sumsq = fd.get("sumsq")
    if sumsq is not None and avg_n:
        var = sumsq / avg_n - avg * avg
        sigma = round(var ** 0.5, 4) if var > 0 else 0.0
    return {
        "n": n, "neg": neg, "avg_n": avg_n, "avg": round(avg, 4),
        "sigma": sigma,
        "min": fd.get("min"), "max": fd.get("max"),
        "p50": pct(0.50), "p95": pct(0.95),
        "p99": pct(0.99), "p999": pct(0.999),
        # p9999 needs >= 10k samples to mean anything; the JS side hides it
        # below that threshold (the histogram ladder is coarse up there).
        "p9999": pct(0.9999),
    }


def _merge_value_counts(a, b, cap=24):
    out = dict(a or {})
    for k, v in (b or {}).items():
        out[str(k)] = out.get(str(k), 0) + int(v)
    items = sorted(out.items(), key=lambda kv: -kv[1])
    if len(items) <= cap:
        return dict(items)
    keep = dict(items[:cap])
    keep["other"] = sum(v for _k, v in items[cap:])
    return keep


def _merge_bucket_rows(dst, rows):
    """dst: {t: [n, ok, fail, sum, max, 0, 0, hist, neg]} <- trace bucket rows.

    neg (index 8) counts -1 sentinel samples: they are part of n but carry no
    duration, so the bucket average divides by n-neg."""
    for r in rows or []:
        t = int(r[0])
        b = dst.get(t)
        if b is None:
            b = dst[t] = [0, 0, 0, 0.0, 0.0, 0.0, 0.0, {}, 0]
        b[0] += int(r[1])
        b[1] += int(r[2])
        b[2] += int(r[3])
        b[3] += float(r[4])
        b[4] = max(b[4], float(r[5]))
        b[5] += float(r[6])
        b[6] = max(b[6], float(r[7]))
        for k, n in (r[8] or {}).items():
            b[7][int(k)] = b[7].get(int(k), 0) + int(n)
        if len(r) > 9:
            b[8] += int(r[9] or 0)
    return dst



def _merge_trace_stat(groups, st, tops_by_group=None):
    """Merge one trace stats file into `groups` (aggregate or per-object).

    tops_by_group=None 时不收集慢请求明细（按对象合并时不需要，节省内存）；
    否则按写/读组分别收集，供 reduce 端「先按组过滤、再各组 Top-K」。"""
    m = st.get("meta") or {}
    for gname, g in (st.get("groups") or {}).items():
        gg = groups.setdefault(gname, {"ops": {}})
        for op, o in (g.get("ops") or {}).items():
            dst = gg["ops"].setdefault(op, {
                "count": 0, "ok": 0, "fail": 0, "status": {},
                "fields": {}, "_buckets": {"seconds": {}, "minutes": {}},
                "_fmin": {}, "ftier": 0,
                "size_values": {}, "replicas": {}, "slice_count": {},
                "residual": None, "top": []})
            dst["count"] += o.get("count") or 0
            dst["ok"] += o.get("ok") or 0
            dst["fail"] += o.get("fail") or 0
            for k, n in (o.get("status") or {}).items():
                dst["status"][k] = dst["status"].get(k, 0) + int(n)
            for fk, fd in (o.get("fields") or {}).items():
                dst["fields"][fk] = _merge_field(dst["fields"].get(fk), fd)
            _merge_bucket_rows(dst["_buckets"]["seconds"], o.get("seconds"))
            _merge_bucket_rows(dst["_buckets"]["minutes"], o.get("minutes"))
            for vk in ("size_values", "replicas", "slice_count"):
                dst[vk] = _merge_value_counts(dst[vk], o.get(vk))
            if o.get("residual"):
                dst["residual"] = _merge_field(dst["residual"], o["residual"])
            for fname, frows in (o.get("fminutes") or {}).items():
                fb = dst["_fmin"].setdefault(fname, {})
                for r in frows or []:
                    t = int(r[0])
                    cur = fb.get(t)
                    if cur is None:
                        fb[t] = [int(r[1]), float(r[2]) * int(r[1]),
                                 float(r[3])]
                    else:
                        cur[0] += int(r[1])
                        cur[1] += float(r[2]) * int(r[1])
                        cur[2] = max(cur[2], float(r[3]))
            _ft = int(o.get("ftier") or 60)
            dst["ftier"] = min(dst.get("ftier") or _ft, _ft)
            if tops_by_group is not None:
                for _e in (o.get("top") or []):
                    _e = dict(_e)
                    _e["node"] = m.get("node") or ""
                    _e["pod"] = m.get("pod") or ""
                    _e["client"] = m.get("source") or ""
                    tops_by_group.setdefault(gname, []).append(_e)


def _finalize_trace_groups(groups, edges, hist_ok):
    """Convert merged groups' internals to the on-disk/JS payload shape."""
    for _gname, g in groups.items():
        for _op, o in g["ops"].items():
            o["fields"] = {k: _field_pcts(fd, edges, hist_ok)
                           for k, fd in o["fields"].items()}
            o["residual"] = (_field_pcts(o["residual"], edges, hist_ok)
                             if o.get("residual") else None)
            fmin = {}
            for fname, fb in (o.pop("_fmin") or {}).items():
                rows = []
                for t in sorted(fb):
                    n, ssum, mx = fb[t]
                    rows.append([int(t), int(n),
                                 round(ssum / n, 3) if n else 0.0,
                                 round(mx, 3)])
                fmin[fname] = rows
            o["fminutes"] = fmin
            o["ftier"] = int(o.get("ftier") or 60)
            buckets = o.pop("_buckets")
            for bk in ("seconds", "minutes"):
                bmap = buckets[bk]
                rows = []
                for t, b in sorted(bmap.items()):
                    n, okc, failc, ssum, mx = b[0], b[1], b[2], b[3], b[4]
                    hb = b[7]
                    valid = n - (b[8] if len(b) > 8 else 0)
                    rows.append([int(t), n, okc, failc,
                                 round(ssum / valid, 3) if valid > 0 else 0.0,
                                 round(mx, 6),
                                 (round(ls.hist_percentile(hb, 0.50, edges), 4)
                                  if hist_ok and hb else None),
                                 (round(ls.hist_percentile(hb, 0.99, edges), 4)
                                  if hist_ok and hb else None)])
                o[bk] = rows

def _coarsen_fmin(rows, max_points):
    """[t,n,avg,max] 抽稀：按 k 合并桶（n 加权平均，max 取最大）。"""
    n = len(rows or [])
    if n <= max_points:
        return rows, 1
    k = (n + max_points - 1) // max_points
    agg = {}
    for r in rows:
        tk = int(r[0]) // k * k
        w = int(r[1])
        a = agg.get(tk)
        if a is None:
            agg[tk] = [tk, w, float(r[2]) * w, float(r[3])]
        else:
            a[1] += w
            a[2] += float(r[2]) * w
            a[3] = max(a[3], float(r[3]))
    out = []
    for tk in sorted(agg):
        a = agg[tk]
        out.append([tk, a[1], round(a[2] / a[1], 3) if a[1] else 0.0,
                    round(a[3], 3)])
    return out, k


def _coarsen_sec(rows, max_points):
    """[t,n,ok,fail,avg,max,p50,p99] 抽稀（avg 按 n 加权，max 取最大，
    p50/p99 取简单平均——仅提示用途）。"""
    n = len(rows or [])
    if n <= max_points:
        return rows, 1
    k = (n + max_points - 1) // max_points
    agg = {}
    for r in rows:
        tk = int(r[0]) // k * k
        w = int(r[1])
        a = agg.get(tk)
        if a is None:
            agg[tk] = [tk, w, int(r[2]), int(r[3]), float(r[4]) * w,
                       float(r[5]), r[6], r[7]]
        else:
            a[1] += w
            a[2] += int(r[2])
            a[3] += int(r[3])
            a[4] += float(r[4]) * w
            a[5] = max(a[5], float(r[5]))
            if r[6] is not None:
                a[6] = r[6] if a[6] is None else (a[6] + r[6]) / 2.0
            if r[7] is not None:
                a[7] = r[7] if a[7] is None else (a[7] + r[7]) / 2.0
    out = []
    for tk in sorted(agg):
        a = agg[tk]
        out.append([tk, a[1], a[2], a[3],
                    round(a[4] / a[1], 3) if a[1] else 0.0,
                    round(a[5], 6), a[6], a[7]])
    return out, k


def _copy_trace_obj(obj):
    """浅拷贝对象的两层结构（groups -> ops），供嵌入裁剪后安全修改。"""
    new = dict(obj)
    groups = {}
    for gname, g in (obj.get("groups") or {}).items():
        groups[gname] = {"ops": {op: dict(p)
                                 for op, p in (g.get("ops") or {}).items()}}
    new["groups"] = groups
    return new


def _trim_trace_embed(tr, max_points=TRACE_EMBED_POINTS,
                      budget=TRACE_EMBED_ROWS):
    """HTML 内嵌裁剪：总计对象全嵌；节点/实例抽稀到 max_points 并按行预算
    裁剪（超出的对象只留在 data.js，前端在 Full 模式合并）。"""
    objs = tr.get("objects") or {}
    order = sorted(objs, key=lambda k: (
        0 if objs[k].get("kind") == "role" else
        (1 if objs[k].get("kind") == "node" else 2),
        -int(objs[k].get("count") or 0)))
    out = {}
    used = 0
    for k in order:
        c = _copy_trace_obj(objs[k])
        if c.get("kind") != "role":
            rows = 0
            for g in c["groups"].values():
                for op in g["ops"].values():
                    fm = {}
                    k_once = 1
                    for fn, rs in (op.get("fminutes") or {}).items():
                        rs2, kk = _coarsen_fmin(rs, max_points)
                        fm[fn] = rs2
                        k_once = max(k_once, kk)
                        rows += len(rs2)
                    op["fminutes"] = fm
                    if op.get("seconds"):
                        rs2, kk = _coarsen_sec(op["seconds"], max_points)
                        op["seconds"] = rs2
                        k_once = max(k_once, kk)
                        rows += len(rs2)
                    # ftier 必须每个 op 只乘一次（曾写在逐字段循环里 → k^字段数，
                    # 600s run 的 5 字段 = 4^5 = 1024s → 只剩 1 桶 → 曲线空白）
                    if k_once > 1:
                        op["ftier"] = int(op.get("ftier") or 1) * k_once
            if used + rows > budget:
                continue
            used += rows
        out[k] = c
    new = dict(tr)
    new["objects"] = out
    new["embed_objects"] = len(out)
    new["object_total"] = len(objs)
    return new


def reduce_trace_size(trace_list):
    """Merge per-client trace stats into one data-size block."""
    groups = {}
    objects = {}
    clients = []
    tops_by_group = {}
    metad = {"files": 0, "lines": 0, "trace_lines": 0, "slice_lines": 0,
             "parse_errors": 0, "sample_rates": {}, "bins": "us-v1",
             "markers": False, "raw_bytes": 0, "product_bytes": 0,
             "phase_counts": {"setup": 0, "warmup": 0, "measured": 0,
                              "teardown": 0}}
    for st in trace_list:
        m = st.get("meta") or {}
        metad["files"] += 1
        if m.get("bins") != "us-v1":
            metad["bins"] = "legacy"     # histogram unit unknown -> no pcts
        if m.get("markers"):
            metad["markers"] = True       # any marked file enables the note
        for pk, pn in (m.get("phase_counts") or {}).items():
            metad["phase_counts"][pk] = metad["phase_counts"].get(pk, 0) + pn
        for k in TRACE_META_KEYS:
            metad[k] += m.get(k) or 0
        if m.get("sample_rate") is not None:
            key = str(m["sample_rate"])
            metad["sample_rates"][key] = metad["sample_rates"].get(key, 0) + 1
        metad["raw_bytes"] = metad.get("raw_bytes", 0) + int(
            m.get("bytes") or 0)
        metad["product_bytes"] = metad.get("product_bytes", 0) + int(
            st.get("_bytes") or 0)
        cl = {"name": m.get("source") or "", "node": m.get("node") or "",
              "pod": m.get("pod") or "", "role": m.get("role") or "",
              "map_bytes": st.get("_bytes") or 0,
              "raw_bytes": m.get("bytes") or 0,
              "ops": {}}
        for g in (st.get("groups") or {}).values():
            for op, o in (g.get("ops") or {}).items():
                cl["ops"][op] = cl["ops"].get(op, 0) + (o.get("count") or 0)
        _merge_trace_stat(groups, st, tops_by_group)
        # ---- 按对象（总计/节点/实例）保留阶段桶：SDK 全链路的对象树用 ----
        node = m.get("node") or ""
        pod = m.get("pod") or m.get("source") or ""
        for gname, g in (st.get("groups") or {}).items():
            cnt = sum(int((oo or {}).get("count") or 0)
                      for oo in (g.get("ops") or {}).values())
            tgts = [("role:" + gname, "role", "总计", "", gname)]
            if node:
                tgts.append(("node:%s:%s" % (node, gname), "node",
                             node, node, gname))
            if pod:
                tgts.append(("client:%s:%s" % (pod, gname), "client",
                             pod, node, gname))
            for key, kind, label, nd, role in tgts:
                tobj = objects.get(key)
                if tobj is None:
                    tobj = objects[key] = {
                        "kind": kind, "label": label, "node": nd,
                        "role": role, "groups": {}, "count": 0}
                tobj["count"] += cnt
                _merge_trace_stat(tobj["groups"],
                                  {"meta": m, "groups": {gname: g}}, None)
        clients.append(cl)
    trace_edges = ls.BIN_EDGES_US if metad["bins"] == "us-v1" else None
    trace_hist_ok = metad["bins"] == "us-v1"
    _finalize_trace_groups(groups, trace_edges, trace_hist_ok)
    for tobj in objects.values():
        _finalize_trace_groups(tobj["groups"], trace_edges, trace_hist_ok)
    # 慢请求时刻归一：start_time 是节点本地钟面时间 -> epoch（前端时间轴用）
    # 每组先过滤（该组自身的慢请求集合）再各自 Top-K —— 图与表恰有 K 条。
    for _gname, _lst in tops_by_group.items():
        for _e in _lst:
            _e["ts_epoch"] = _node_ts_epoch(_e.get("start_time"))
        _lst.sort(key=lambda e: -float(e.get("total_us") or 0))
        groups.setdefault(_gname, {"ops": {}})["top"] = _lst[:TRACE_TOP_N]
    return {"meta": metad, "groups": groups,
            "clients": clients, "objects": objects,
            "object_total": len(objects)}


def _volume_block(blk, traces):
    """每个 size 的数据量账本：输入 → 行数速率 → 原始(分文件) → 单实例 map 后
    → reduce 回传 → 输出（输出大小由 render_scenario 回填）。

    glog **参与 map-reduce**（tracestats → trace.json）；ylt（coro_rpc
    easylog）当前**不采集/不处理**，只作为“原始数据”存在，绝不进入 map 或
    reduce 统计（V-7/DR-40）。原始 glog 字节来自 map 产物 meta.bytes（实测），
    map 产物字节来自文件大小（实测）。"""
    sm = blk.get("summary") or {}
    tr = blk.get("trace") or {}
    tm = tr.get("meta") or {}
    clients = blk.get("clients") or []
    wall = float(sm.get("wall_s") or 0)
    span = float(blk.get("span") or wall or 0)
    ops = int(sm.get("total") or 0)
    if not ops:
        ops = int(sm.get("ok") or 0) + int(sm.get("fail") or 0)
    trace_lines = int(tm.get("trace_lines") or 0)
    glog_raw = int(tm.get("raw_bytes") or 0)
    py_bytes = sum(int(c.get("bytes") or 0) for c in clients)
    w_bytes = sum(int(c.get("bytes") or 0) for c in clients
                  if (c.get("role") or "") == "writer")
    r_bytes = sum(int(c.get("bytes") or 0) for c in clients
                  if (c.get("role") or "") == "reader")
    stats_map = sum(int(c.get("map_bytes") or 0) for c in clients)
    trace_map = int(tm.get("product_bytes") or 0)
    n = max(1, len(clients))
    n_trace = max(1, len(traces or []))
    nodes = len({c.get("node") for c in clients if c.get("node")})

    def rate(v):
        return round(v / span, 1) if span else None

    out = {
        "inputs": {
            "wall_s": round(wall, 3), "span_s": round(span, 3),
            "clients": len(clients), "nodes": nodes or None,
            "writer_clients": sum(1 for c in clients
                                  if (c.get("role") or "") == "writer"),
            "reader_clients": sum(1 for c in clients
                                  if (c.get("role") or "") == "reader"),
            "sample_rate": tm.get("sample_rate"),
            "ops": ops, "trace_lines": trace_lines,
        },
        "rates": {
            "rows_per_s": rate(ops),
            "trace_rows_per_s": rate(trace_lines),
        },
        "raw": {
            "writer_bytes": w_bytes, "reader_bytes": r_bytes,
            "other_python_bytes": max(0, py_bytes - w_bytes - r_bytes),
            "glog_bytes": glog_raw,
            "ylt_bytes": None,          # 未采集（暂不处理）
            "total_bytes": py_bytes + glog_raw,
        },
        "map": {
            "stats_bytes": stats_map, "trace_bytes": trace_map,
            "stats_per_client": round(stats_map / n),
            "trace_per_client": round(trace_map / n_trace),
            "trace_files": len(traces or []),
            "bucket_cap": MAX_TIER_BUCKETS,
            "hist_bins": len(ls.BIN_EDGES_US) - 1,
            "top_n": TRACE_TOP_N, "list_cap": ls.LIST_CAP,
            "trace_top_n": TRACE_TOP_N,
        },
        "reduce": {"pulled_bytes": stats_map + trace_map},
        "output": {},
        "ratio": {},
    }
    raw = out["raw"]["total_bytes"]
    pulled = out["reduce"]["pulled_bytes"]
    out["ratio"]["raw_to_pulled"] = (round(raw / pulled, 1)
                                     if pulled else None)
    return out


def _scenario_of(meta):
    """Report format: basic | pipeline (legacy name "scenario").

    Old artifacts carry mode="kvtest" (engine written into the format slot):
    treat them as pipeline, the shape kvtest has always produced."""
    m = meta or {}
    sc = m.get("scenario")
    if sc in ("basic", "pipeline"):
        return sc
    mode = m.get("mode")
    if mode in ("basic", "pipeline"):
        return mode
    if mode == "kvtest" or m.get("engine") == "kvtest":
        return "pipeline"
    return "basic"


def _engine_of(meta):
    m = meta or {}
    eng = m.get("engine")
    if eng in ("python", "kvtest"):
        return eng
    return "kvtest" if m.get("mode") == "kvtest" else "python"


def reduce_scenario(stats, meta, monitors=None, traces=None,
                    monitor_sites=None, site_sizes=None):
    """Full scenario report data: stats grouped by data-size (caller passes a
    flat list where possible; the size label is taken from meta["size"] when
    present, else 'all'). `monitors` (from reduce_monitors) is scenario-level
    and independent of data-size; `monitor_sites` (from reduce_monitor_sites)
    feeds the 现场日志 table's per-node monitor row."""
    by_size = {}
    for st in stats:
        size = (st.get("meta") or {}).get("size") or meta.get("size") or "all"
        by_size.setdefault(size, []).append(st)
    sizes = {}
    for size, group in by_size.items():
        sizes[str(size)] = reduce_size(group, meta)
    trace_list = traces or []
    by_size_trace = {}
    for st in trace_list:
        tsize = str((st.get("meta") or {}).get("size")
                    or meta.get("size") or "all")
        by_size_trace.setdefault(tsize, []).append(st)
    for size, group in by_size_trace.items():
        blk = sizes.get(size)
        if blk is None:
            blk = sizes[size] = {"summary": None, "embed_step": 60,
                                 "series": {}, "clients": [], "errors": {},
                                 "error_excerpt": None,
                                 "error_excerpt_status": None,
                                 "slowest": [], "anomalies": []}
        blk["trace"] = reduce_trace_size(group)
    for size, blk in sizes.items():
        if blk.get("summary"):
            blk["volume"] = _volume_block(
                blk, [t for t in trace_list
                      if str((t.get("meta") or {}).get("size")
                             or meta.get("size") or "all") == size])
    sources, nodes = _sources(stats, trace_list)
    du_ledger = _site_du_ledger(meta, sources, monitor_sites, site_sizes)
    for size, blk in sizes.items():
        if blk.get("volume") is not None and du_ledger:
            blk["volume"]["site"] = {
                "nodes": [{"node": n, "pods": v["pods"],
                           "profiling": v["profiling"], "total": v["total"]}
                          for n, v in sorted(du_ledger.items())],
                "total": sum(v["total"] for v in du_ledger.values())}
    return {"meta": meta, "sizes": sizes, "sources": sources, "nodes": nodes,
            "monitors": monitors or {},
            "logsite": build_logsite(meta, sources, monitor_sites,
                                     site_sizes)}


# ---------------------------------------------------------------------------
# 日志现场（现场日志表）：节点行 + pod 行 + 监控行；路径一律节点侧真实目录
# ---------------------------------------------------------------------------

MONITOR_ROW_ROLE = "监控"


def reduce_monitor_sites(local_log_dir):
    """Per-node monitor artifact sites for the 现场日志 table.

    Returns {node_label: {"pod": <monitor pod>, "remote_dir": <node-side
    profiling dir>, "files": N, "bytes": B}} keyed by the node display name
    (monitor.config.json node_ip, else the artifact dir name)."""
    sites = {}
    try:
        for root, _dirs, files in os.walk(local_log_dir):
            fset = set(files)
            if not (fset & {MEM_HOST, MEM_PROC, URMA_FLITS, MONITOR_CONFIG,
                            "mem.err.log", "urma.err.log"}):
                continue
            rcfg = {}
            if MONITOR_CONFIG in fset:
                try:
                    with open(os.path.join(root, MONITOR_CONFIG),
                              "r", encoding="utf-8") as f:
                        rcfg = json.load(f) or {}
                except (OSError, ValueError):
                    rcfg = {}
            disp = str(rcfg.get("node_ip") or os.path.basename(root))
            nbytes = 0
            for fn in files:
                try:
                    nbytes += os.path.getsize(os.path.join(root, fn))
                except OSError:
                    pass
            sites[disp] = {"pod": str(rcfg.get("pod") or ""),
                           "remote_dir": str(rcfg.get("remote_dir") or ""),
                           "files": len(files), "bytes": nbytes}
    except Exception:
        pass
    return sites


def _site_mount_twin(meta):
    """旧产物兜底：pod 内路径 → 节点侧孪生路径（挂载逆变换）。"""
    meta = meta or {}
    root = str(meta.get("host_log_root") or "").rstrip("/")
    run_id = str(meta.get("run_id") or "")
    base = (root + "/" + run_id) if (root and run_id) else ""
    remote_root = str(meta.get("remote_log_dir") or "").rstrip("/")

    def twin(p):
        p = str(p or "")
        if (base and remote_root
                and (p == remote_root or p.startswith(remote_root + "/"))):
            return base + p[len(remote_root):]
        return p
    return twin, base


def _site_du_ledger(meta, sources, monitor_sites, site_sizes):
    """per-node du 台账：pods / profiling / total（无 du 数据时返回 {}）。

    k8s：pod 目录逐项匹配；ssh 扁平目录（pod 行目录==根目录）用根 total，
    profiling 作为其子集扣除，避免重复计数。"""
    twin, base = _site_mount_twin(meta)
    du_nodes = (site_sizes or {}).get("nodes") or {}
    if not du_nodes:
        return {}
    pod_dirs = {}
    for src in sources or []:
        node = str(src.get("node") or "?")
        pod = str(src.get("pod") or "")
        rpath = twin(src.get("remote_path") or "")
        d = (base + ("/" + pod if pod else "")) if base else (
            os.path.dirname(rpath) if rpath else "")
        if d:
            pod_dirs.setdefault(node, set()).add(d.rstrip("/"))
    mons = monitor_sites or {}
    out = {}
    for node, du in du_nodes.items():
        dirs = du.get("dirs") or {}
        scopes = sorted(pod_dirs.get(node) or [])
        pod_bytes = sum(int(dirs.get(d, 0)) for d in scopes)
        mdir = twin(((mons.get(node) or {}).get("remote_dir")) or "")
        prof = int(dirs.get(mdir.rstrip("/"), 0)) if mdir else 0
        root_path = str(du.get("root") or "").rstrip("/")
        if root_path and scopes and all(d == root_path for d in scopes):
            total = int(du.get("total") or 0)
            pod_bytes = max(0, total - prof) if prof else total
        else:
            total = pod_bytes + prof
        out[node] = {"pods": pod_bytes, "profiling": prof, "total": total}
    return out


def build_logsite(meta, sources, monitor_sites=None, site_sizes=None):
    """现场日志表行：每节点一个节点行（聚合）+ 若干 pod 行 + 恰好一个监控行。

    - 节点行：files/bytes = 该节点全部已知产物合计；dir = 节点日志根目录
      （meta.host_log_root/run_id，缺省取同节点 pod 目录公共父级）
    - pod 行：role = 该 pod 日志的角色集合；files/bytes 去重后合计
    - 监控行：有监控产物时带监控 pod 与 profiling 目录；未开监控时
      pod/bytes/dir 为空（渲染为 -），但该行始终存在
    - 结构性不变量：任何 source 的 remote_path 必须落在其 pod 行的目录下；
      落不进的强制以自身 dirname 成行（永不静默丢产物）。"""
    meta = meta or {}
    _twin, base = _site_mount_twin(meta)
    du_nodes = (site_sizes or {}).get("nodes") or {}
    du_ledger = _site_du_ledger(meta, sources, monitor_sites, site_sizes)

    pods = {}
    extras = {}
    for src in sources or []:
        node = str(src.get("node") or "?")
        pod = str(src.get("pod") or "")
        rpath = _twin(src.get("remote_path") or "")
        name = str(src.get("source") or "")
        row_dir = (base + ("/" + pod if pod else "")) if base else (
            os.path.dirname(rpath) if rpath else "")
        inside = (not row_dir) or (not rpath) or (
            rpath == row_dir or rpath.startswith(row_dir.rstrip("/") + "/"))
        key = (node, pod, row_dir) if inside else (
            node, os.path.dirname(rpath) if rpath else "")
        tgt = pods if inside else extras
        row = tgt.get(key)
        if row is None:
            row = tgt[key] = {"node": node,
                              "pod": pod if inside else (pod or "-"),
                              "dir": row_dir if inside else key[1],
                              "roles": set(), "files": set(), "bytes": 0}
        if src.get("role"):
            row["roles"].add(str(src["role"]))
        if name and name not in row["files"]:
            row["files"].add(name)
            row["bytes"] += int(src.get("bytes") or 0)
    by_node = {}
    for row in list(pods.values()) + list(extras.values()):
        by_node.setdefault(row["node"], []).append(row)
    sites = monitor_sites or {}
    for node in sites:
        by_node.setdefault(node, [])
    rows = []
    for node in sorted(by_node):
        items = sorted(by_node[node],
                       key=lambda r: (str(r.get("pod") or ""), r["dir"]))
        site = sites.get(node) or {}
        site_bytes = int(site.get("bytes") or 0)
        site_files = int(site.get("files") or 0)
        node_dir = base
        if not node_dir:
            dirs = [r["dir"] for r in items if r["dir"]]
            if dirs:
                try:
                    node_dir = os.path.commonpath(dirs)
                except ValueError:
                    node_dir = os.path.dirname(dirs[0])
        mon_dir = _twin(site.get("remote_dir") or "")
        if not node_dir and mon_dir:
            node_dir = os.path.dirname(os.path.dirname(mon_dir.rstrip("/")))
        du = du_nodes.get(node) or {}
        du_dirs = du.get("dirs") or {}
        led = du_ledger.get(node)
        node_bytes = (led["total"] if led else
                      sum(r["bytes"] for r in items) + site_bytes)
        rows.append({"tier": "node", "node": node, "role": "", "pod": "",
                     "files": sum(len(r["files"]) for r in items) + site_files,
                     "bytes": node_bytes, "dir": node_dir})
        for r in items:
            dv = du_dirs.get(str(r["dir"]).rstrip("/"))
            rows.append({"tier": "pod", "node": node, "pod": r["pod"],
                         "role": "、".join(sorted(r["roles"])),
                         "files": len(r["files"]),
                         "bytes": int(dv) if dv is not None else r["bytes"],
                         "dir": r["dir"]})
        mv = du_dirs.get(mon_dir.rstrip("/")) if mon_dir else None
        rows.append({"tier": "monitor", "node": node,
                     "role": MONITOR_ROW_ROLE,
                     "pod": str(site.get("pod") or ""),
                     "files": site_files,
                     "bytes": int(mv) if mv is not None else site_bytes,
                     "dir": mon_dir})
    return rows


# ---------------------------------------------------------------------------
# node monitors (mem / urma) — CSVs written by the profiling sampler scripts
# ---------------------------------------------------------------------------

MEM_HOST = "mem.host.csv"
MEM_PROC = "mem.proc.csv"
URMA_FLITS = "urma.flits.csv"
MONITOR_CONFIG = "monitor.config.json"

_MON_LEAK_PCT = 30.0
_MON_LEAK_MB = 200.0
_MON_FLIT_BYTES = 20
MON_EMBED_MAX_POINTS = 2000


def _read_csv_rows(path):
    try:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            return list(csv.reader(f))
    except OSError:
        return []


def _to_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _to_int(x):
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return None


def _parse_mem_host(path, node, t0=None, t1=None, flags=None):
    rows = _read_csv_rows(path)
    if len(rows) < 2 or len(rows[0]) < 2:
        return None
    cols = [c.strip() for c in rows[0][1:]]

    def _collect(lo, hi):
        t, series = [], {c: [] for c in cols}
        for r in rows[1:]:
            ts = _to_float(r[0]) if r else None
            if ts is None:
                continue
            if lo is not None and ts < lo:
                continue
            if hi is not None and ts > hi:
                continue
            t.append(int(ts))
            for i, c in enumerate(cols):
                series[c].append(_to_float(r[i + 1]) if i + 1 < len(r) else None)
        return t, series

    t, series = _collect(t0, t1)
    if not t and (t0 is not None or t1 is not None):
        # 窗口内无任何采样、但文件有行：节点/编排器时钟偏差等——回退全量，
        # 绝不能因为时间窗对不齐就把监控图整张丢掉。
        t, series = _collect(None, None)
        if t and flags is not None:
            flags["window_misaligned"] = True
    if not t:
        return None
    return {"node": node, "t": t, "columns": cols, "series": series}


def _parse_mem_proc(path, node, leak_pct, leak_mb, t0=None, t1=None,
                    flags=None):
    rows = _read_csv_rows(path)
    if len(rows) < 2:
        return []
    idx = {name.strip(): i for i, name in enumerate(rows[0])}
    if any(n not in idx for n in ("ts", "pid", "rss_kb")):
        return []

    def _collect(lo, hi):
        groups = {}
        for r in rows[1:]:
            def cell(name):
                i = idx[name]
                return r[i] if i < len(r) else None
            ts, pid = _to_int(cell("ts")), _to_int(cell("pid"))
            rss = _to_float(cell("rss_kb"))
            if ts is None or pid is None:
                continue
            if (lo is not None and ts < lo) or (hi is not None and ts > hi):
                continue
            key = (node, pid)
            g = groups.get(key)
            if g is None:
                g = groups[key] = {"node": node, "pid": pid,
                                   "comm": (cell("comm") or "").strip(),
                                   "t": [], "rss_mb": [], "pss_mb": []}
            g["t"].append(ts)
            g["rss_mb"].append(round(rss / 1024.0, 2)
                              if rss is not None else None)
            pss = _to_float(cell("pss_kb"))
            g["pss_mb"].append(round(pss / 1024.0, 2)
                               if pss is not None else None)
        return groups

    groups = _collect(t0, t1)
    if not groups and (t0 is not None or t1 is not None):
        groups = _collect(None, None)
        if groups and flags is not None:
            flags["window_misaligned"] = True
    out = []
    for g in groups.values():
        vals = [v for v in g["rss_mb"] if v is not None]
        if not vals:
            continue
        first, last = vals[0], vals[-1]
        growth = last - first
        growth_pct = (growth / first * 100.0) if first > 0 else 0.0
        g.update({"rss_first_mb": round(first, 2), "rss_last_mb": round(last, 2),
                  "rss_peak_mb": round(max(vals), 2),
                  "rss_growth_mb": round(growth, 2),
                  "rss_growth_pct": round(growth_pct, 1),
                  "leak_suspect": bool(growth_pct >= leak_pct
                                       and growth >= leak_mb)})
        out.append(g)
    out.sort(key=lambda x: (x["node"], -x.get("rss_growth_mb", 0)))
    return out


def _parse_urma(path, node, flit_bytes, t0=None, t1=None, flags=None):
    rows = _read_csv_rows(path)
    if len(rows) < 2:
        return []
    idx = {name.strip(): i for i, name in enumerate(rows[0])}
    if any(n not in idx for n in ("ts_ns", "ubpu", "port", "rx_flits",
                                  "tx_flits")):
        return []

    def _collect(lo, hi):
        groups = {}
        for r in rows[1:]:
            def cell(name):
                i = idx[name]
                return r[i] if i < len(r) else None
            ts, ubpu, port = (_to_int(cell("ts_ns")), _to_int(cell("ubpu")),
                              _to_int(cell("port")))
            rx, tx = _to_int(cell("rx_flits")), _to_int(cell("tx_flits"))
            if None in (ts, ubpu, port, rx, tx):
                continue
            sec = ts / 1e9
            if (lo is not None and sec < lo) or (hi is not None and sec > hi):
                continue
            groups.setdefault((ubpu, port), []).append((ts, rx, tx))
        return groups

    groups = _collect(t0, t1)
    if not groups and (t0 is not None or t1 is not None):
        groups = _collect(None, None)
        if groups and flags is not None:
            flags["window_misaligned"] = True
    if flags is not None:
        # 窗口内样本数：>0 但 <2 属「样本不足」（不打扰），0 才是采集失败
        flags["urma_samples"] = sum(len(v) for v in groups.values())
    gib = float(1024 ** 3)
    out = []
    for (ubpu, port), samples in groups.items():
        samples.sort(key=lambda s: s[0])
        t, rxr, txr = [], [], []
        prev = None
        resync = False        # 回绕/复位的下一拍跳过（否则出现假尖峰）
        for ts, rx, tx in samples:
            if prev is not None:
                dt_ns = ts - prev[0]
                drx, dtx = rx - prev[1], tx - prev[2]
                if dt_ns > 0 and drx >= 0 and dtx >= 0 and not resync:
                    sec = dt_ns / 1e9
                    t.append(round(ts / 1e9, 3))
                    rxr.append(round(drx * flit_bytes / sec / gib, 4))
                    txr.append(round(dtx * flit_bytes / sec / gib, 4))
                resync = (drx < 0 or dtx < 0)
            prev = (ts, rx, tx)
        if not t:
            continue
        out.append({"node": node, "ubpu": ubpu, "port": port, "t": t,
                    "rx": rxr, "tx": txr,
                    "rx_avg": round(sum(rxr) / len(rxr), 3),
                    "rx_max": max(rxr),
                    "tx_avg": round(sum(txr) / len(txr), 3),
                    "tx_max": max(txr)})
    out.sort(key=lambda x: (x["node"], x["ubpu"], x["port"]))
    return out


def reduce_monitors(local_log_dir, t0=None, t1=None):
    """Scan <local_log_dir>/**/profiling/<node>/ for monitor CSVs.

    t0/t1（epoch 秒，可选）把采样行裁到该时间窗内——单元报表（一个
    size×combo）只带自己的时间窗，避免同一场景的监控曲线在每份报表里
    重复且混入其它单元的数据。summary 在裁剪后的数据上重算。

    Returns {config, mem_host, mem_proc, urma, summary, notes}；当存在监控
    产物却没有任何可用序列时返回 {notes: [...]}（报表渲染占位卡列出原因，
    不再整卡静默消失）；完全没有 profiling 产物（监控未开/旧 run）时返回 {}。
    Never raises."""
    mem_host, mem_proc, urma, cfg = [], [], [], {}
    notes = []
    flags = {}
    seen_host = set()     # 同主机多 pod 的宿主内存只保留一份

    def _first_line(path, limit=160):
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        return line[:limit]
        except OSError:
            pass
        return ""

    try:
        for root, _dirs, files in os.walk(local_log_dir):
            fset = set(files)
            if not (fset & {MEM_HOST, MEM_PROC, URMA_FLITS, MONITOR_CONFIG,
                            "mem.err.log", "urma.err.log"}):
                continue
            node = os.path.basename(root)
            rcfg = {}
            if MONITOR_CONFIG in fset:
                try:
                    with open(os.path.join(root, MONITOR_CONFIG),
                              "r", encoding="utf-8") as f:
                        rcfg = json.load(f) or {}
                except (OSError, ValueError):
                    rcfg = {}
                if not cfg:
                    cfg = rcfg
            # 节点显示名：产物目录名已是节点 IP（新 run），旧产物靠
            # monitor.config.json 的 node_ip 覆盖（k8s=宿主 IP / ssh=管理 IP）
            disp = str(rcfg.get("node_ip") or node)
            mem_cfg = rcfg.get("mem") or cfg.get("mem") or {}
            leak_pct = float(mem_cfg.get("leak_growth_pct") or _MON_LEAK_PCT)
            leak_mb = float(mem_cfg.get("leak_min_mb") or _MON_LEAK_MB)
            flit_bytes = int((rcfg.get("urma") or cfg.get("urma") or {})
                             .get("bytes_per_flit") or _MON_FLIT_BYTES)
            if MEM_HOST in fset and disp not in seen_host:
                seen_host.add(disp)
                h = _parse_mem_host(os.path.join(root, MEM_HOST), disp,
                                    t0=t0, t1=t1, flags=flags)
                if h:
                    mem_host.append(h)
                else:
                    notes.append("{}：mem.host.csv 无有效数据行".format(disp))
            if MEM_PROC in fset:
                got = _parse_mem_proc(
                    os.path.join(root, MEM_PROC), disp, leak_pct, leak_mb,
                    t0=t0, t1=t1, flags=flags)
                if got:
                    mem_proc.extend(got)
                else:
                    notes.append("{}：mem.proc.csv 无有效数据行".format(disp))
            urma_ok = False
            urma_samples = 0
            if URMA_FLITS in fset:
                flags.pop("urma_samples", None)
                got = _parse_urma(
                    os.path.join(root, URMA_FLITS), disp, flit_bytes,
                    t0=t0, t1=t1, flags=flags)
                urma_samples = int(flags.pop("urma_samples", 0) or 0)
                if got:
                    urma.extend(got)
                    urma_ok = True
            if (not urma_ok and not urma_samples
                    and ("urma.err.log" in fset or URMA_FLITS in fset)):
                reason = (_first_line(os.path.join(root, "urma.err.log"))
                          if "urma.err.log" in fset else "")
                notes.append(
                    "{}：URMA 采集失败 — {}".format(disp, reason)
                    if reason else "{}：URMA 无有效样本".format(disp))
            if (MEM_HOST not in fset and "mem.err.log" in fset
                    and not any(h["node"] == disp for h in mem_host)):
                reason = _first_line(os.path.join(root, "mem.err.log"))
                notes.append("{}：内存采集失败 — {}".format(disp, reason)
                             if reason else
                             "{}：内存采集无产物".format(disp))
    except Exception:
        pass
    if flags.get("window_misaligned"):
        notes.append("监控时间窗与测量窗无交叠（疑似节点/编排器时钟偏差），"
                     "已回退全量采样")
    if not (mem_host or mem_proc or urma):
        return {"notes": notes} if notes else {}
    summary = {"peak_mem_mb": {}, "worst_proc": None, "peak_urma": None}
    for h in mem_host:
        used = [c for c in h["columns"] if "used_mb" in c]
        peak = 0.0
        for i in range(len(h["t"])):
            s = sum((h["series"][c][i] or 0) for c in used)
            peak = max(peak, s)
        summary["peak_mem_mb"][h["node"]] = round(peak, 1)
    if mem_proc:
        growth = sorted(mem_proc, key=lambda p: -(p.get("rss_growth_mb") or 0))
        summary["proc_growth"] = [{
            "label": "{}/{} {}".format(p["node"], p["pid"],
                                       p.get("comm") or "").strip(),
            "growth_mb": p.get("rss_growth_mb"),
            "growth_pct": p.get("rss_growth_pct"),
            "leak_suspect": p.get("leak_suspect")}
            for p in growth[:12]]
        worst = max(mem_proc, key=lambda p: p.get("rss_growth_mb") or 0)
        summary["worst_proc"] = {
            "label": "{}/{} {}".format(worst["node"], worst["pid"],
                                       worst.get("comm") or "").strip(),
            "growth_mb": worst.get("rss_growth_mb"),
            "growth_pct": worst.get("rss_growth_pct"),
            "leak_suspect": worst.get("leak_suspect")}
    if urma:
        best = max(urma, key=lambda u: max(u.get("rx_max") or 0, u.get("tx_max") or 0))
        summary["peak_urma"] = {
            "label": "{} ubpu={} port={}".format(best["node"], best["ubpu"],
                                                 best["port"]),
            "value": round(max(best.get("rx_max") or 0, best.get("tx_max") or 0), 3)}
    out = {"config": cfg, "mem_host": mem_host, "mem_proc": mem_proc,
           "urma": urma, "summary": summary}
    if notes:
        out["notes"] = notes
    return out


def _downsample(t, arrays, max_points=MON_EMBED_MAX_POINTS):
    """Bucket-average the series down to <= max_points (embedded report size
    bound); data.js keeps the full-resolution series."""
    n = len(t)
    if n <= max_points or max_points <= 0:
        return t, arrays
    step = (n + max_points - 1) // max_points
    out_t = []
    out_arrays = [[] for _ in arrays]
    for i in range(0, n, step):
        j = min(i + step, n)
        out_t.append(t[(i + j) // 2])
        for k, arr in enumerate(arrays):
            vals = [arr[x] for x in range(i, j) if arr[x] is not None]
            out_arrays[k].append(round(sum(vals) / len(vals), 3) if vals else None)
    return out_t, out_arrays


def compact_monitors(mons):
    """Downsampled monitor payload embedded into report.html (full series stay
    in data.js)."""
    if not mons:
        return {}
    out = {"summary": mons.get("summary") or {},
           "notes": mons.get("notes") or [],
           "mem_host": [], "mem_proc": [], "urma": []}
    for h in mons.get("mem_host") or []:
        cols = h.get("columns") or []
        t, arrs = _downsample(h["t"], [h["series"][c] for c in cols])
        out["mem_host"].append({"node": h["node"], "t": t, "columns": cols,
                                "series": {c: arrs[i] for i, c in enumerate(cols)}})
    for p in mons.get("mem_proc") or []:
        t, arrs = _downsample(p["t"], [p["rss_mb"], p["pss_mb"]])
        q = dict(p)
        q["t"], q["rss_mb"], q["pss_mb"] = t, arrs[0], arrs[1]
        out["mem_proc"].append(q)
    for u in mons.get("urma") or []:
        t, arrs = _downsample(u["t"], [u["rx"], u["tx"]])
        q = dict(u)
        q["t"], q["rx"], q["tx"] = t, arrs[0], arrs[1]
        out["urma"].append(q)
    return out



# ---------------------------------------------------------------------------
# render (self-contained HTML + optional data.js)
# ---------------------------------------------------------------------------

def _read_echarts(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()
    except OSError:
        return ""


VENDOR_LIEFLAT = os.path.join(os.path.dirname(_HERE), "webui", "web",
                               "vendor", "lieflat")


def _lieflat_tokens_js():
    """lieflat-charts 视觉 token（vendored，PolyForm NC；见同目录 NOTICE.md）。
    report.html 自包含：两个 token 文件整体内联在 app JS 之前。"""
    parts = []
    for name in ("mono-tokens.js", "color-presets.js"):
        text = _read_echarts(os.path.join(VENDOR_LIEFLAT, name))
        if text:
            parts.append(text)
    return "\n".join(parts)


_PAGE_CSS = """
:root{color-scheme:light dark;
--paper:#F0F0EE;--ink:#1F1E1C;--mut:rgba(31,30,28,.60);--lab:rgba(31,30,28,.72);
--faint:rgba(31,30,28,.72);--grid:rgba(31,30,28,.16);--quiet:rgba(31,30,28,.15);
--track:rgba(31,30,28,.12);--hero:#F5572F;--term-bg:#1C1C1A;--term-fg:#F0EFEB}
@media (prefers-color-scheme:dark){:root:not([data-theme="light"]){--paper:#1C1C1A;--ink:#F0EFEB;
--mut:rgba(240,240,238,.55);--lab:rgba(240,240,238,.72);--faint:rgba(240,240,238,.72);
--grid:#2E2D29;--quiet:rgba(240,240,238,.15);--track:rgba(240,240,238,.12)}}
:root[data-theme="dark"]{--paper:#1C1C1A;--ink:#F0EFEB;
--mut:rgba(240,240,238,.55);--lab:rgba(240,240,238,.72);--faint:rgba(240,240,238,.72);
--grid:#2E2D29;--quiet:rgba(240,240,238,.15);--track:rgba(240,240,238,.12)}
:root[data-theme="light"]{--paper:#F0F0EE;--ink:#1F1E1C;
--mut:rgba(31,30,28,.60);--lab:rgba(31,30,28,.72);--faint:rgba(31,30,28,.72);
--grid:rgba(31,30,28,.16);--quiet:rgba(31,30,28,.15);--track:rgba(31,30,28,.12)}
.theme-btn{position:absolute;top:36px;right:28px;border:1px solid var(--grid);
  background:transparent;color:var(--ink);border-radius:99px;padding:2px 10px;
  font:inherit;font-size:12.5px;cursor:pointer;line-height:1.5}
.theme-btn:hover{border-color:var(--faint)}
*{box-sizing:border-box}
body{margin:0;background:var(--paper);color:var(--ink);
font:14px/1.62 'Inter','SF Pro Text',system-ui,'Segoe UI','PingFang SC',
'Microsoft YaHei',sans-serif;-webkit-font-smoothing:antialiased}
.wrap{max-width:1400px;margin:0 auto;padding:38px 28px 76px}
h1{font-size:19px;font-weight:700;letter-spacing:-.02em;margin:0 0 3px}
h2{font-size:16.5px;font-weight:700;letter-spacing:-.02em;margin:0 0 2px}
.sub{color:var(--mut);font-size:13px;margin-bottom:14px}
.card{background:var(--paper);border:1px solid var(--grid);border-radius:24px;padding:26px 26px 20px;
margin:0}
.card + .card,.wrap > .card:not(:first-child){margin-top:14px}
.note{color:var(--mut);font-size:13px;line-height:1.75;margin-top:8px}
.ulinks{display:flex;flex-wrap:wrap;gap:6px 16px;margin-top:10px}
.ulinks a{color:var(--ink);font-size:12px;text-decoration:none;
border-bottom:1px solid var(--grid);padding-bottom:1px}
.ulinks a:hover{border-bottom-color:var(--ink)}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(132px,1fr));
gap:18px 22px}
.kv{padding:2px 0}
.kv .k{color:var(--mut);font-size:12px;font-weight:500;letter-spacing:.06em;
text-transform:uppercase;margin-bottom:2px}
.kv .v{font-size:24px;font-weight:800;letter-spacing:-.02em;
font-variant-numeric:tabular-nums;line-height:1.25}
.kv .u{font-size:12px;font-weight:500;letter-spacing:.02em;color:var(--mut)}
.kv.fail .v{color:var(--hero)}
#perfcards .kv .v,#tcards .kv .v,#rolecards .kv .v{font-weight:500;font-size:18px}
/* 指标名按小写显示（avg/p99/min/max…，与曲线一致），不套大写 */
#perfcards .kv .k,#tcards .kv .k,#rolecards .kv .k{text-transform:none}
#perfcards .kv.strong .k,#tcards .kv.strong .k,
#rolecards .kv.strong .k{color:var(--ink)}
#perfcards .kv.strong .v,#tcards .kv.strong .v,
#rolecards .kv.strong .v{font-weight:800;font-size:24px}
.gridbreak{grid-column:1/-1;height:0;margin:0;padding:0}
#unitmeta{gap:10px 22px;margin-bottom:8px}
#rolecards .rblock{margin-bottom:4px}
#rolecards .rblock .sec{margin-top:16px}
#unitmeta .kv .v{font-size:20px}
.toolbar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;
margin-bottom:10px}
.tl-label{color:var(--faint);font-size:12px;font-weight:600;
letter-spacing:.08em;text-transform:uppercase}
.sec{font-size:13px;font-weight:600;letter-spacing:.1em;color:var(--faint);
margin:20px 0 10px;padding-bottom:6px;border-bottom:1px solid var(--grid);
text-transform:uppercase}
.sec:first-of-type{margin-top:4px}
button{font:inherit;font-size:12.5px;padding:3px 11px;border-radius:99px;
border:1px solid var(--grid);background:transparent;color:var(--ink);
cursor:pointer;transition:background .15s,border-color .15s}
button:hover{border-color:var(--faint)}
button.on{background:var(--ink);border-color:var(--ink);color:var(--paper)}
button:disabled{opacity:.35;cursor:not-allowed}
select{font:inherit;font-size:12.5px;padding:3px 8px;border-radius:99px;
border:1px solid var(--grid);background:transparent;color:var(--ink);
max-width:280px}
.chips{display:inline-flex;flex-wrap:wrap;gap:5px;align-items:center}
.chip{display:inline-flex;align-items:center;gap:2px;padding:3px 5px 3px 11px;
border:1px solid var(--grid);border-radius:99px;font-size:12.5px;
background:transparent;max-width:220px;overflow:hidden;
text-overflow:ellipsis;white-space:nowrap}
.chip .chip-x{border:none;background:transparent;color:var(--mut);
cursor:pointer;font-size:12px;padding:0 5px}
.chip .chip-x:hover{color:var(--hero)}

#latToolbar,#tLatToolbar{flex-wrap:nowrap;overflow-x:auto;scrollbar-width:none}
#latToolbar::-webkit-scrollbar,#tLatToolbar::-webkit-scrollbar{display:none}
#latToolbar>*,#tLatToolbar>*{flex:0 0 auto;margin:0}   /* 清掉嵌套 .toolbar 的 margin-bottom（否则内容整体上移、与前两个按钮错行） */
#latToolbar .toolbar,#tLatToolbar .toolbar{flex-wrap:nowrap}
.logsite-table{width:max-content;min-width:100%}
.logsite-table th,.logsite-table td{white-space:nowrap}
.logsite-table tr.node-row td{font-weight:700}
.params{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));
gap:0 26px}
.prow{display:flex;gap:10px;padding:6px 0;border-bottom:1px solid var(--grid);
font-size:13px;min-width:0}
.prow .pk{flex:0 0 132px;color:var(--mut);font-size:12.5px;letter-spacing:.02em}
.prow .pv{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;
white-space:nowrap;font-family:'Cascadia Code',Consolas,monospace;
font-variant-numeric:tabular-nums}
.chart{width:100%;height:420px}.chart.sm{height:250px}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{border-bottom:1px solid var(--grid);padding:6px 10px;text-align:left;
white-space:nowrap}
th{color:var(--faint);font-weight:600;font-size:12px;letter-spacing:.08em;
text-transform:uppercase;position:sticky;top:0;background:var(--paper);
z-index:1}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}
td.mono{font-size:12px}
tbody tr:hover td{background:var(--quiet)}
.wrapx{overflow:auto;max-height:420px;border-top:1px solid var(--grid)}
pre.term{background:var(--term-bg);color:var(--term-fg);border-radius:16px;
padding:14px 16px;font:12.5px/1.6 'Cascadia Code',Consolas,monospace;
overflow:auto;max-height:360px;white-space:pre-wrap;word-break:break-all}
.tag{display:inline-block;font-size:12px;font-weight:600;padding:2px 9px;
border-radius:99px;border:1px solid var(--grid);color:var(--mut);
letter-spacing:.06em;vertical-align:middle}
.tag.hero{color:var(--hero);border-color:var(--hero)}
.mono{font-family:'Cascadia Code',Consolas,monospace}
.path{color:var(--mut);font-size:12.5px}
.anml li{margin:5px 0;font-size:13px}
a{color:inherit}
/* 时延分布（F14）/慢点（G15） */
/* 分级时延图（Lupi 编辑语法） */
.levels{display:flex;flex-direction:column;gap:8px;margin-top:4px}
/* 分级时延：块宽 = 占父项百分比；列宽用 minmax(可读下限, Nfr)，
   极小项按最小可读宽度展开并允许整行横向滚动（不截断文字）。
   块无任何边框；聚焦只靠块的形态（更大、字更大）表达。 */
.lvl-wrap{margin-bottom:2px}
.lvl-row{display:grid;gap:4px;align-items:start;overflow-x:auto;padding-bottom:2px}
.lvl-row.root{grid-template-columns:minmax(0,1fr)}
.lvl-blk{display:flex;flex-direction:column;justify-content:center;gap:1px;
padding:7px 11px;border-radius:8px;border:0;
font-size:12px;font-weight:600;min-width:0;overflow:hidden;line-height:1.3}
.lvl-blk:hover{filter:brightness(1.07)}
.lvl-blk.focus{padding:13px 16px;font-size:13px}
.lvl-blk.focus .lvl-val{font-size:13px}
.lvl-blk.compressed{padding:5px 6px}
.lvl-blk.compressed .lvl-name,.lvl-blk.compressed .lvl-val{visibility:hidden}
.lvl-name{font-weight:700;white-space:nowrap;overflow:hidden;
text-overflow:ellipsis;min-width:0}
.lvl-val{font-variant-numeric:tabular-nums;opacity:.85;white-space:nowrap;
overflow:hidden;text-overflow:ellipsis;min-width:0}
.lvl-children{align-self:start;overflow-x:auto;padding-top:5px}
.lvl-note{color:var(--mut);font-size:12.5px;margin:2px 0 6px}
/* 分级时延曲线：与端到端同构——plain toolbar + 两个标准 objtree-strip 并排
   （仅布局，无底色/无新容器视觉；窄屏回退单栏） */
.trace-cols{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);
gap:12px;margin-bottom:10px}
.trace-cols .objtree-strip{margin-bottom:0}
@media (max-width:1000px){.trace-cols{grid-template-columns:1fr}}
.blockobj-row{margin-bottom:6px}
.blockobj-row .tl-label,.blockobj-row #lvlMetric{align-items:center}
#lvlMetric.toolbar{margin-bottom:0;flex-wrap:nowrap;align-items:center}
#lvlMetric button{vertical-align:middle;line-height:1}
.objdrop-wrap{position:relative;display:inline-block}
.objdrop-btn{font-size:12px;padding:2px 10px;border-radius:4px;
border:1px solid var(--grid);background:transparent;color:var(--ink);
cursor:pointer;max-width:240px;overflow:hidden;text-overflow:ellipsis;
white-space:nowrap}
.objdrop-menu{position:absolute;z-index:30;top:calc(100% + 4px);left:0;
min-width:240px;max-width:420px;max-height:320px;overflow:auto;
background:var(--paper);border:1px solid var(--grid);border-radius:4px;
padding:4px}
.objdrop-row{display:flex;align-items:center;gap:6px;padding:3px 6px;
cursor:pointer;font-size:12px;border-radius:3px}
.objdrop-row:hover{background:var(--quiet)}
.objdrop-row.on{background:var(--track);font-weight:700}
.objdrop-row .dot{display:inline-block;width:8px;height:8px;border-radius:99px;
flex:0 0 auto}
.objdrop-name{white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
min-width:0;flex:1 1 auto}
.objdrop-tag{font-size:12px;color:var(--mut)}
.node-row td{background:var(--quiet);font-weight:700}
tr.volume-key td{font-weight:700}
td.logcell{max-width:220px;overflow:hidden;text-overflow:ellipsis;
white-space:nowrap}
.node-row .node-name{color:var(--ink)}
.client-row td:first-child{padding-left:14px}
.sbrk{padding:10px 0 4px;width:min(100%,1250px)}
.sbrk .lvl-row{margin-bottom:6px}
/* 慢点追踪表格展开：去 total、单行压缩、横向滚动 */
.sbrk.compact{padding:4px 0 2px;width:100%}
.sbrk.compact .lvl-row{margin-bottom:3px}
.sbrk.compact .lvl-blk{flex-direction:row;align-items:baseline;gap:5px;
padding:2px 6px;border-radius:3px}
.sbrk.compact .lvl-name,.sbrk.compact .lvl-val{font-size:12px;
font-weight:600;opacity:.95}
.sbrk.compact .lvl-note{font-size:12px;margin:2px 0}
.focus-bar{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:8px}
.focus-empty{color:var(--mut);font-size:12.5px}
/* 阶段树（F5 Tick Rows 语法） */
.ttree{border-top:1px solid var(--grid);padding:2px 0}
.tnode{display:flex;align-items:center;gap:9px;padding:3px 0;
line-height:1.75;user-select:none;border-bottom:1px solid var(--grid)}
.tnode:hover{background:var(--quiet)}
.tarrow{width:14px;flex:none;color:var(--faint);cursor:pointer}
.tname{font-size:13px;padding:1px 6px;border-radius:4px;cursor:pointer;
white-space:nowrap}
.tname:hover{background:var(--quiet)}
.tname.sel{font-weight:700}
.tbar{width:88px;height:7px;flex:none;border-radius:99px;
background:var(--track);overflow:hidden}
.tfill{display:block;height:7px;border-radius:99px;min-width:2px;
transition:width .3s ease-out}
.tpct{font-size:12px;color:var(--mut);min-width:36px;text-align:right;
font-variant-numeric:tabular-nums}
.tval{font-size:12.5px;font-weight:600;color:var(--mut);
font-variant-numeric:tabular-nums}
/* 状态分布（F5 横条） */
.statusbars{display:flex;flex-direction:column;gap:6px;margin-top:6px}
.sbrow{display:flex;align-items:center;gap:10px}
.sbname{flex:0 0 168px;font-size:12.5px;overflow:hidden;
text-overflow:ellipsis;white-space:nowrap}
.sbtrack{flex:1;height:8px;border-radius:99px;background:var(--track);
overflow:hidden}
.sbfill{display:block;height:8px;border-radius:99px;min-width:2px}
.sbval{flex:0 0 auto;font-size:12.5px;font-weight:700;
font-variant-numeric:tabular-nums}
/* RSS 增幅正负条（G10 diverging） */
.grow{display:flex;flex-direction:column;gap:4px;margin:14px 0 2px}
.grow-row{display:flex;align-items:center;gap:10px}
.grow-name{flex:0 0 190px;font-size:12px;overflow:hidden;
text-overflow:ellipsis;white-space:nowrap;color:var(--mut)}
.grow-track{position:relative;flex:1;height:9px}
.grow-track:before{content:"";position:absolute;left:50%;top:-2px;
width:1px;height:13px;background:var(--faint)}
.grow-bar{position:absolute;top:0;height:9px;border-radius:99px;
background:var(--faint)}
.grow-val{flex:0 0 96px;font-size:12px;text-align:right;
font-variant-numeric:tabular-nums}
/* ---- 时延曲线：对象选择横条（曲线上方） ---- */
/* 注意：不能用 align-items:center —— 内容超过 max-height 时会向上下
   均分溢出，顶部溢出区在滚动容器里不可达（展开节点后「总体与角色」
   会被顶出且滚不回来）。必须 flex-start 让内容从顶部开始排。 */
.objtree-strip{display:flex;align-items:flex-start;gap:12px;
border:1px solid var(--grid);border-radius:14px;padding:6px 10px;
margin-bottom:14px;max-height:300px;overflow:auto}
.objtree-body{flex:1;min-width:0;display:flex;flex-direction:column;gap:2px}
.objtree-count{color:var(--mut);font-size:12px;white-space:nowrap}
.tree-group{display:flex;flex-wrap:wrap;align-items:center;gap:4px 6px;
padding:3px 8px}
.tree-group+.tree-group{border-top:1px solid var(--grid)}
.node-group{flex-direction:column;align-items:stretch}
.node-group-head{display:flex;align-items:center;gap:10px}
.node-group-acts{display:flex;align-items:center;gap:6px;padding:1px 0 3px 2px}
.tree-list{display:flex;flex-direction:column;gap:2px;flex:1;min-width:0;
padding:2px 0}
.tree-group-title{font-size:12px;font-weight:700;letter-spacing:.06em;
color:var(--lab);text-transform:uppercase}
.chart-hint{color:var(--mut);font-size:12.5px;margin-top:6px;letter-spacing:.01em}
.slow-ctl{display:flex;flex-direction:column;gap:4px;margin-bottom:8px}
.slow-ctl .row{display:flex;flex-wrap:wrap;gap:6px;align-items:center}
.slow-sum{display:flex;flex-wrap:wrap;gap:6px 14px;margin-bottom:6px}
.slow-chip{display:inline-flex;align-items:center;gap:6px;font-size:12px;
color:var(--mut);font-variant-numeric:tabular-nums}
.slow-chip .dot{width:8px;height:8px;border-radius:99px;flex:0 0 auto}
.chart.qps{height:190px;margin-top:14px}
.tree-row{display:flex;align-items:center;gap:6px;padding:2px 4px;border-radius:6px}
.tree-row:hover{background:var(--quiet)}
.tree-row.on{background:var(--quiet)}
.tree-check{display:flex;align-items:center;gap:8px;min-width:0;
background:none;border:none;padding:3px 2px;cursor:pointer;color:inherit;
text-align:left;font:inherit}
.tree-check:hover .tree-name{text-decoration:underline}
.tree-box{flex:none;width:15px;height:15px;border-radius:4px;
border:1px solid var(--faint);display:inline-flex;align-items:center;
justify-content:center;font-size:12px;line-height:1;color:var(--paper)}
.tree-row.on .tree-box{background:var(--ink);border-color:var(--ink)}
.tree-name{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;
white-space:nowrap;font-size:13px}
.tree-note{color:var(--mut);font-size:12px;white-space:nowrap}
.tree-caret{flex:none;width:16px;border:none;background:transparent;
color:var(--faint);cursor:pointer;font-size:12px;padding:0}
.tree-empty{color:var(--mut);font-size:12.5px;padding:6px 8px}
button.mini{font-size:12px;padding:1px 8px;border-radius:99px;line-height:1.6}
/* ---- 时延轴：左侧缩放滑块（倍率）+ 细刻度轨（窗口平移） ---- */
.chart-frame{display:grid;grid-template-columns:16px 16px minmax(0,1fr);gap:8px;
align-items:stretch}
.chart-canvas{position:relative;min-width:0}
.zslider{position:relative;display:flex;flex-direction:column}
.zslider-track{position:relative;flex:1;width:100%;min-height:150px;
background:var(--track);border-radius:8px}
.zslider-fill{position:absolute;left:0;right:0;bottom:0;height:0;
background:var(--quiet);border-radius:8px;pointer-events:none}
.zslider-handle{position:absolute;left:50%;width:18px;height:13px;
transform:translateX(-50%);background:var(--ink);border-radius:99px;
cursor:ns-resize}
.zslider-cap{position:absolute;left:20px;display:none;
transform:translateY(-50%);font-size:12px;font-variant-numeric:tabular-nums;
color:var(--ink);background:var(--paper);border-radius:4px;padding:1px 5px;
white-space:nowrap;pointer-events:none;box-shadow:0 0 0 1px var(--grid);z-index:5}
.zslider.dragging .zslider-cap{display:block}
.yscale{display:flex;flex-direction:column}
.yscale-rail{position:relative;flex:1;width:100%;min-height:150px;
background:var(--track);border-radius:7px}
.yscale-win{position:absolute;left:0;right:0;top:0;height:100%;
background:var(--quiet);border-radius:6px;
cursor:grab}
/* ---- 底部时间轴平移条（仅平移；左右边距与主图 grid 一致） ---- */
.xpan{position:absolute;left:56px;right:104px;bottom:6px;height:12px;
background:var(--track);border-radius:99px;z-index:2}
#tXpan{left:104px}   /* 阶段曲线 grid.left=104 */
.xpan-win{position:absolute;top:0;height:100%;left:0;width:100%;
background:var(--quiet);border-radius:99px;
cursor:grab}
/* ---- 时延曲线选择（对象树下方，统一控制指标显隐） ---- */
.lat-sel{display:flex;align-items:center;gap:10px;flex-wrap:wrap;
margin-bottom:16px}
.lat-sel-btns{display:flex;gap:6px;flex-wrap:wrap}
.lat-sel-btns button{font-size:12px;padding:2px 10px;border-radius:99px;
border:1px solid var(--grid);background:transparent;color:var(--ink);
cursor:pointer}
.lat-sel-btns button.off{opacity:.35;text-decoration:line-through}
.lat-sel>button.off{opacity:.35}
.lat-sel-btns button.rect{border-radius:4px;font-weight:600;padding:2px 9px}
.lat-sel-btns button .dot{display:inline-block;width:8px;height:8px;
border-radius:99px;margin-right:5px;vertical-align:middle}
/* ---- 端点标识：细线指向端点，标识纵向排开 ---- */
.endlabels{position:absolute;inset:0;pointer-events:none;overflow:visible}
.endlabel{pointer-events:auto;cursor:pointer}
.endlabel{position:absolute;font-size:12px;font-weight:700;white-space:nowrap;
transform:translateY(-50%)}
.endlabel.fixed{max-width:96px;overflow:hidden;text-overflow:ellipsis}
.mc-label-tip{pointer-events:none;max-width:360px;white-space:normal;
word-break:break-all;font-size:12px;z-index:45;line-height:1.5}
.mc-tip-box{position:absolute;z-index:40;pointer-events:auto;
background:var(--paper);color:var(--ink);border:1px solid var(--grid);
border-radius:4px;padding:8px 10px;font-size:13px;line-height:1.55;
font-variant-numeric:tabular-nums;max-height:46vh;overflow-y:auto}
.endleader{position:absolute;height:1px;transform-origin:0 50%;opacity:.85}
/* ---- 阶段树：左树右详情 ---- */
.stage-split{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);
gap:20px;align-items:start}
.tdetail{border-left:1px solid var(--grid);padding-left:18px;min-width:0}
.tdetail-title{font-size:13.5px;font-weight:700;margin:0 0 8px}
.tdetail-sub{font-size:12.5px;font-weight:600;color:var(--lab);margin:14px 0 6px}
.tnode.sel{background:var(--quiet)}
.empty-tip{border:1px dashed var(--grid);border-radius:12px;padding:14px 16px;
color:var(--mut);font-size:13px;margin-bottom:8px}
@media (max-width:1100px){
.stage-split{grid-template-columns:1fr}
.tdetail{border-left:none;padding-left:0;border-top:1px solid var(--grid);
padding-top:14px}
}
@media (prefers-reduced-motion: reduce){ .tfill{transition:none} }
""" 

_APP_JS = r"""
const D = window.__MC_REPORT__ || {};
const FULL = window.__MC_FULL__ || null;   // loaded from data.js when present
function fmtDur(s){s=Math.max(0,Math.round(s));const h=Math.floor(s/3600),
m=Math.floor(s%3600/60);return (h?h+'h':'')+(m?m+'m':'')+(s%60)+'s';}
function fmtBytes(n){n=Number(n)||0;if(n<1024)return n+' B';if(n<1048576)
return (n/1024).toFixed(1)+' KB';if(n<1073741824)return (n/1048576).toFixed(1)+' MB';
return (n/1073741824).toFixed(2)+' GB';}
function hhmmss(t){const d=new Date(t*1000);const p=n=>String(n).padStart(2,'0');
return p(d.getHours())+':'+p(d.getMinutes())+':'+p(d.getSeconds());}
function fullts(t){const d=new Date(t*1000);const p=n=>String(n).padStart(2,'0');
return d.getFullYear()+'-'+p(d.getMonth()+1)+'-'+p(d.getDate())+' '+hhmmss(t);}
function el(tag,cls,text){const e=document.createElement(tag);if(cls)e.className=cls;
if(text!==undefined)e.textContent=text;return e;}
function kv(k,v,u){const w=el('div','kv');w.appendChild(el('div','k',k));
const vv=el('div','v',v);if(u)vv.appendChild(el('span','u',' '+u));
w.appendChild(vv);return w;}
function kvStrong(k,v,u){const w=kv(k,v,u);w.className='kv strong';return w;}
function gridBreak(){return el('div','gridbreak');}
const S = D.sizes || {}; let curSize = Object.keys(S)[0]||'';
let curTier = '';            // 当前粒度 key（"1"/"10"/…/"cnt"）
let selObjs = ['all'];       // 选中的曲线对象（总体/角色/node:*/client:*）
/* pipeline 对象树状态：角色单选（默认 reader）+ 总计开关（默认开） */
let curRole = 'reader';
let totalOn = true;
let selByRole = {};          /* 各角色记住自己的节点/实例选择（切换不丢弃） */
function isPipeMode(){
  const m=D.meta||{};
  if(m.scenario==='basic'||m.scenario==='pipeline') return m.scenario==='pipeline';
  const md=String(m.mode||'');   /* 旧产物：kvtest 即 pipeline 形态 */
  return md==='pipeline'||md==='kvtest';
}
function isBasicMode(){
  const m=D.meta||{};
  if(m.scenario==='basic'||m.scenario==='pipeline') return m.scenario==='basic';
  return String(m.mode||'')==='basic';
}
function Z(){return S[curSize]||null;}
function objAll(){
  const z=Z(); const e=(z&&z.objects)||{};
  const f=(FULL&&FULL.sizes&&FULL.sizes[curSize]&&FULL.sizes[curSize].objects)||{};
  const out={}; Object.keys(e).forEach(k=>{out[k]=e[k];});
  Object.keys(f).forEach(k=>{out[k]=f[k];});
  return out;
}
function tierList(){const z=Z();return (z&&z.tiers)||[];}
function colsOf(key,tier){const o=objAll()[key];return o&&o.tiers?o.tiers[tier]:null;}
function objLabel(key){const o=objAll()[key];return (o&&o.label)||key;}
function defaultTier(){
  const z=Z(); if(!z) return 'cnt';
  const emb=z.embed_tiers||[];
  // 自适应（按样本数）是默认档：样本不足 2048 时逐样本成桶 = 全量绘制
  if(emb.indexOf('cnt')>=0) return 'cnt';
  const time=emb.filter(function(t){return t!=='cnt';});
  return time.length?time[0]:(emb.indexOf('cnt')>=0?'cnt':'');
}
function currentTier(){return curTier||defaultTier();}
function tierStep(tier){
  const m=(tierList()||[]).filter(function(t){
    return String(t.key)===String(tier);})[0];
  if(m&&m.step) return Number(m.step);
  return tier==='cnt'?1:(Number(tier)||1);
}
function colQps(col,tier){
  const step=tierStep(tier);
  return col.c.map(function(c,i){
    return Math.round(c/((col.span&&col.span[i])||step)*100)/100;});}
/* ---- lieflat Wire 主题（token 来自 vendored mono-tokens/color-presets） ---- */
/* R-SHELL-4：主题三态（自动/亮/暗）。'auto' 跟随系统 prefers-color-scheme；
   手动选择持久化到 localStorage（file:// 下不可用时退化为会话内生效），
   并通过 <html data-theme> 让 CSS token 翻转——图表 token（LF_DARK）在
   切换时整体重算并整页重渲染。 */
const _THEME_KEY = 'mc_report_theme';
function _sysDark(){
  return !!(window.matchMedia &&
    window.matchMedia('(prefers-color-scheme: dark)').matches);
}
function _storedTheme(){
  try{ return window.localStorage.getItem(_THEME_KEY)||''; }catch(e){ return ''; }
}
function themePref(){ return _storedTheme()||'auto'; }
function applyTheme(pref, silent){
  const dark = (pref==='auto') ? _sysDark() : (pref==='dark');
  try{
    const root=document.documentElement;
    root.setAttribute('data-theme', dark?'dark':'light');
  }catch(e){}
  if(!silent){
    try{ window.localStorage.setItem(_THEME_KEY, pref); }catch(e){}
  }
  const btn=document.getElementById('themeToggle');
  if(btn){ btn.setAttribute('aria-pressed', pref==='auto'?'false':'true');
    btn.title='当前：'+(pref==='auto'?'自动（跟随系统）':(pref==='dark'?'暗色':'亮色'))+'，点击切换';
    const t=btn.querySelector('.theme-icon'); if(t)t.textContent=dark?'◐':'◑'; }
  return dark;
}
const LF_DARK = applyTheme(themePref(), true);
window.__MC_THEME__ = { pref: themePref(), dark: LF_DARK };
const LF_W = (window.PRESETS && window.PRESETS.WIRE) || null;
const LF_M = window.MONO || null;
const LF_LADDER_L = (LF_W && LF_W.SER) || ['#22211F','#8F8E86','#C0BFB7',
  'rgba(34,33,31,.52)','#DBDAD3','rgba(34,33,31,.3)'];
const LF_LADDER_D = (LF_M && LF_M.DARK && LF_M.DARK.ladder) ||
  ['#F0EFEB','#DCDAD2','#C9C7BD','#B3B0A4','#8F8E88','#6A6963','#4A4944'];
const HERO = (LF_W && LF_W.HERO) || '#F5572F';
const C = LF_DARK ? {
  fg:(LF_M&&LF_M.DARK&&LF_M.DARK.ink)||'#F0EFEB',
  mut:'rgba(240,240,238,.55)',
  faint:(LF_M&&LF_M.DARK&&LF_M.DARK.faint)||'#55554F',
  grid:(LF_M&&LF_M.DARK&&LF_M.DARK.grid)||'#2E2D29',
  quiet:'rgba(240,240,238,.15)', track:'rgba(240,240,238,.12)',
  ladder:LF_LADDER_D, hero:HERO,
  blocks:['#F0EFEB','#B3B0A4','#8F8E88','#55554F']
} : {
  fg:(LF_W&&LF_W.TXT)||'#1F1E1C', mut:(LF_W&&LF_W.MUT)||'rgba(31,30,28,.60)',
  faint:(LF_W&&LF_W.FAINT)||'rgba(31,30,28,.32)',
  grid:(LF_W&&LF_W.GRID)||'rgba(31,30,28,.16)',
  quiet:(LF_W&&LF_W.QUIET)||'rgba(31,30,28,.15)',
  track:(LF_W&&LF_W.TRACK)||'rgba(31,30,28,.12)',
  ladder:LF_LADDER_L, hero:HERO,
  blocks:['#1F1E1C','#6E6D66','#8F8E86','#C0BFB7']
};
function lfColor(i){const L=C.ladder||[];return L[i%Math.max(1,L.length)]||C.fg;}
const LFTIP = LF_DARK
  ? ({backgroundColor:'#F0EFEB',padding:[10,14],
      textStyle:{color:'#1C1C1A'},borderWidth:0,
      _src:(LF_M&&LF_M.tipDark)})
  : ({backgroundColor:'#1C1C1A',padding:[10,14],
      textStyle:{color:'#F0EFEB'},borderWidth:0,
      _src:(LF_M&&LF_M.tipLight)});
const LFFONT="'Inter','SF Pro Text',system-ui,'Segoe UI','PingFang SC','Microsoft YaHei',sans-serif";
function lfTooltip(extra){
  /* 所有图统一：**原生悬浮** —— 竖线 + 各曲线交点 + 内容框同时出现/消失，
     不做点击钉住（复杂状态机已拒绝，见 DR-42）；showDelay 避免快速扫过弹框，
     enterable 允许鼠标移入框内滚动/复制。 */
  const o={trigger:'axis',confine:true,
    triggerOn:'mousemove',showContent:true,
    hideDelay:250,enterable:true,className:'mc-tip',
    extraCssText:'max-height:46vh;overflow-y:auto',
    backgroundColor:LFTIP.backgroundColor,borderWidth:0,
    padding:LFTIP.padding,
    textStyle:Object.assign({fontFamily:LFFONT,fontSize:13},
      LFTIP.textStyle||{})};
  if(extra)Object.keys(extra).forEach(function(k){o[k]=extra[k];});
  return o;
}
/* 图表辅助绑定：仅保留「标签悬浮聚焦」的清理钩子（悬浮/悬出画布即恢复全部），
   与 tooltip 无关（tooltip 走 ECharts 原生悬浮）。 */
function bindHoverGuards(ch){
  if(!ch||!ch.on||!ch.getZr||ch.__mcGuardsBound) return;
  ch.__mcGuardsBound=true;
  const zr=ch.getZr();
  zr.on('mousemove',function(){ mcClearFocus(ch); });
  zr.on('globalout',function(){
    mcClearFocus(ch);
    /* 移出画布必隐藏 tooltip（showDelay 竞态曾导致框残留、并挡住右侧标签） */
    try{ ch.dispatchAction({type:'hideTip'}); }catch(e){}
  });
  /* 兜底：指针移动到任何「标签之外」的元素即结束悬浮聚焦
     （修复长截断标签向右移出后鼠标事件不再命中导致聚焦残留） */
  try{
    document.addEventListener('mousemove',function(ev){
      if(!ch.__mcFocus) return;
      const box=ch.__mcLabelBox;
      let inside=false;
      try{
        inside=!!(box&&box.contains&&ev&&ev.target&&box.contains(ev.target));
      }catch(e){}
      if(!inside) mcClearFocus(ch);
    });
  }catch(e){}
}
/* ---- x 轴刻度：双图共用显式范围/间隔（上下对齐）+ 跨度自适应格式 ----
   主图与吞吐面板传同一个 ext -> min/max/interval 完全一致，且两图
   grid 左右边距相同 -> 同一时刻在两张图上像素级垂直对齐。
   间隔按「绘图区宽度」限流：窄页面自动减少刻度数，标签绝不挤压
   （72px/刻度的安全宽度，数量上限 12、下限 3）。
   格式规则：<2 分钟 HH:MM:SS；2 分钟~48 小时 HH:MM（每天首个刻度带
   MM-DD 日期）；>48 小时一律 MM-DD HH:MM。不用 0.1s 精度（数据是 1s 桶）。 */
/* 每张时间轴图各持一份量程状态（曾用全局单例，多图互相覆盖导致
   端点标签/刻度格式错乱）。key: main / qps / slow / stage / <mon chart id> */
const X_ZOOMS={};
function zoomState(key){
  if(!X_ZOOMS[key])
    X_ZOOMS[key]={min:0,max:0,iv:1000,spanMs:1000,fullSpan:1000};
  return X_ZOOMS[key];
}
/* 缩放后按可见比例刷新用于刻度格式的 span；挂在该图的 dataZoom 回调里 */
function syncZoomSpan(ch,key){
  try{
    const opt=ch.getOption();
    const dz=(opt.dataZoom||[])[0]||{};
    const zs=zoomState(key);
    if(dz.start!=null&&zs.fullSpan)
      zs.spanMs=Math.max(1000,zs.fullSpan*(dz.end-dz.start)/100);
  }catch(e){}
}
const TICK_STEPS=[1000,2000,5000,10000,15000,30000,60000,120000,300000,
  600000,900000,1800000,3600000,7200000,10800000,21600000,43200000,
  86400000,172800000];
function plotWidthOf(ch,grid){
  const w=(ch&&ch.getWidth&&ch.getWidth())||900;
  return Math.max(200,w-(grid?(grid.left+grid.right):160));
}
function tickIntervalFor(extentMs,plotW){
  const maxTicks=Math.max(3,Math.min(12,Math.floor((plotW||900)/72)));
  const target=extentMs/Math.max(1,maxTicks);
  for(let i=0;i<TICK_STEPS.length;i++){
    if(TICK_STEPS[i]>=target) return TICK_STEPS[i];
  }
  return TICK_STEPS[TICK_STEPS.length-1];
}
function xExtent(tMinS,tMaxS,plotW){
  const spanMs=Math.max(1000,(tMaxS-tMinS)*1000);
  const iv=tickIntervalFor(spanMs,plotW);
  const minX=Math.floor((tMinS*1000)/iv)*iv;
  const maxX=Math.ceil((tMaxS*1000)/iv)*iv;
  return {min:minX,max:maxX,iv:iv,
    spanMs:Math.max(1000,maxX-minX)};
}
function fmtTick(v,zs){
  const span=zs.spanMs;
  const d=new Date(v);
  const p2=function(n){return String(n).padStart(2,'0');};
  const hm=p2(d.getHours())+':'+p2(d.getMinutes());
  const hms=hm+':'+p2(d.getSeconds());
  const md=(d.getMonth()+1)+'-'+p2(d.getDate());
  let firstOfDay=false;
  if(zs.iv){
    const prev=new Date(v-zs.iv);
    firstOfDay=(d.toDateString()!==prev.toDateString());
  }
  if(span<2*60*1000) return (firstOfDay?md+' ':'')+hms;
  if(span<48*3600*1000) return (firstOfDay?md+' ':'')+hm;
  return md+' '+hm;
}
function lfAxisTime(ext,key){
  const zs=zoomState(key||'main');
  if(ext){
    zs.min=ext.min; zs.max=ext.max; zs.iv=ext.iv;
    zs.spanMs=ext.spanMs; zs.fullSpan=ext.spanMs;
  }
  /* 用值轴（毫秒时间戳）：vendored ECharts 的 time 轴无视 interval/splitNumber
     （实测恒出 ~15 根刻度导致窄页挤压），值轴严格遵守 min/max/interval。 */
  return {type:'value',min:(ext?ext.min:null),max:(ext?ext.max:null),
    interval:(ext?ext.iv:null),
    axisLine:{show:false},axisTick:{show:false},splitLine:{show:false},
    axisLabel:{color:C.mut,fontSize:12,fontWeight:600,fontFamily:LFFONT,
      showMinLabel:true,showMaxLabel:true,
      formatter:(ext?function(v){return fmtTick(v,zs);}
                    :function(v){return hhmmss(v/1000);})}};
}
function lfAxisValue(extra){
  const o={type:'value',axisLine:{show:false},axisTick:{show:false},
    axisLabel:{color:C.mut,fontSize:12,fontWeight:600,fontFamily:LFFONT},
    nameTextStyle:{color:C.mut,fontSize:12,fontFamily:LFFONT},
    splitLine:{lineStyle:{color:C.grid,width:1}}};
  if(extra)Object.keys(extra).forEach(function(k){o[k]=extra[k];});
  return o;
}
function axisCommon(){
  return {animation:false,
    textStyle:{fontFamily:LFFONT,color:C.fg,fontSize:12},
    tooltip:lfTooltip(),
    legend:{show:false},
    grid:{left:46,right:20,top:30,bottom:50},
    xAxis:lfAxisTime(null,'main'),
    dataZoom:zoomModel('main')};
}
/* ---- 缩放 / 平移模型 --------------------------------------------------
   滚轮 = 只缩时间轴（时延轴不动）；左右拖 = 平移时间轴；上下拖 = 平移时延轴
   （x/y 平移互斥，一次拖动只作用一个轴）；时延轴的缩放**只**走左侧刻度轨
   （自绘，永不倒转）。filterMode:'none' —— 缩放只改轴范围，不删数据点。 */
function zoomModel(mode,filter){
  if(mode==='xonly')
    return [{type:'inside',xAxisIndex:[0],zoomOnMouseWheel:true,
      moveOnMouseWheel:false,moveOnMouseMove:false,
      filterMode:filter||'none'}];
  return [
    {type:'inside',xAxisIndex:[0],zoomOnMouseWheel:true,
      moveOnMouseWheel:false,moveOnMouseMove:false,filterMode:'none'},
    {type:'inside',yAxisIndex:[0],zoomOnMouseWheel:false,
      moveOnMouseWheel:false,moveOnMouseMove:false,filterMode:'none'}];
}
/* 时延曲线配色：p99 = 强调橙（唯一焦点），其余为高对比中性色实线
   （不再用 30% 淡灰 / 虚线，避免"看不清"）。 */
/* 全灰阶（不用品牌橙）：avg/p99/p9999 = 深灰（原 avg 色，重点三项），
   其余 = 稍浅的灰（原 p999 色）。两个主题各取当前值为准。 */
const LAT_COLORS = LF_DARK
  ? {avg:'#B3B0A4',p99:'#B3B0A4',p9999:'#B3B0A4',
     p50:'#8F8E88',p999:'#8F8E88',max:'#8F8E88',min:'#8F8E88',
     qw:'#B3B0A4'}
  : {avg:'#6E6D66',p99:'#6E6D66',p9999:'#6E6D66',
     p50:'#8F8E86',p999:'#8F8E86',max:'#8F8E86',min:'#8F8E86',
     qw:'#6E6D66'};
/* 线宽（S4b）：avg 1.0，其余 0.8；emphasis +0.2；所有对象等宽（不再首对象加粗） */
const LAT_LINES=[['avg','avg',1.0],['p50','p50',0.8],['p99','p99',0.8],
  ['p999','p999',0.8],['p9999','p9999',0.8],['min','min',0.8],
  ['max','max',0.8],['qw','队列等待',0.8]];
/* 全局统计横线（仅端到端时延图）：正式窗口的 role_summaries 值，图表生成
   时即确定，不随所选 pod/node/实例改变；读写各自一套（curRole）。
   默认只展示 avg / p99 / p9999 三条。 */
const GLOB_LINES=[['avg','avg'],['p50','p50'],['p95','p95'],
  ['p99','p99'],['p999','p999'],['p9999','p9999']];
const globShown={avg:true,p50:false,p95:false,p99:true,p999:false,p9999:true};
const MAIN_GRID={left:56,right:104,top:44,bottom:58};
const QPS_GRID={left:56,right:104,top:48,bottom:34};
function latColor(k){return LAT_COLORS[k]||C.fg;}
function objPalette(){
  /* 多对象对比的保底灰阶调色板（节点色系见下） */
  return LF_DARK
    ? ['#F0EFEB','#DCDAD2','#8F8E88','#B3B0A4','#6A6963','#4A4944']
    : ['#22211F','#3F3E3B','#8F8E86','#6E6D66','#A5A49C','#55554F'];
}
/* 每个节点一个色系；其实例用同色系色阶；总计用灰阶。
   色系按节点名排序固定分配，保证跨选择稳定。 */
const NODE_HUES_L=['#3B6FB6','#3E7D4F','#6B5BA7','#A8702A',
                   '#2F7F7C','#A64B5B'];
const NODE_HUES_D=['#7EA7E8','#7CBF8C','#B3A5E8','#E0A968',
                   '#6FC5C0','#E08C9B'];
function nodeNamesSorted(){
  const all=objAll(); const set={};
  Object.keys(all).forEach(function(k){
    const o=all[k];
    if(o.kind==='node'&&o.node) set[o.node]=1;
  });
  return Object.keys(set).sort();
}
function nodeHue(name){
  const names=nodeNamesSorted();
  const pal=LF_DARK?NODE_HUES_D:NODE_HUES_L;
  const i=Math.max(0,names.indexOf(String(name)));
  return pal[i%pal.length];
}
function instanceColor(nodeName,clientKey){
  const hue=nodeHue(nodeName||'');
  const all=objAll();
  const sib=Object.keys(all).filter(function(k){
    const o=all[k];
    return o.kind==='client'&&o.node===nodeName;
  }).sort();
  const i=Math.max(0,sib.indexOf(clientKey));
  const t=Math.min(0.55,0.12+i*0.14);
  return mixHex(hue,LF_DARK?'#000000':'#FFFFFF',t);
}
/* 对象颜色：role(总计)->null（按指标取灰阶）；node->色系；client->同系色阶 */
function objLineColor(key){
  const o=objAll()[key]||{};
  if(!o||o.kind==='role') return null;
  if(o.kind==='node') return nodeHue(o.node||o.label);
  return instanceColor(o.node,key);
}
/* ---- 离群值剔除（阈值 = 10 x 整体 p99，与 plot_latency.py 的 PNG 版一致）
   语义：命中桶**不丢弃**，而是把该桶的取值截顶到阈值 —— 这样 y 轴不再被
   偶发离群值抬到天上，同时数据点不消失（早前版本整桶置空，一个 1s 桶
   里有 100 个样本时命中概率极高，曲线会缺一大半 = 明显 bug）。        */
/* 离群体系（S5）：离群值 = 单请求 > 客户端 p99×10，由 map 阶段判定并落盘；
   桶的标量统计已剔除离群样本（avg/max 不被拉高），直方图/计数保持全量。
   这里只负责把离群值作为**独立散点**叠加到曲线图上（可显示/隐藏）。 */
let outlierOn=true;             /* true = 显示离群点（默认） */
function outliersFor(key){
  const z=Z(); if(!z) return [];
  const o=objAll()[key]||{};
  const rows=z.outliers||[];
  return rows.filter(function(r){
    if(r.ts_epoch==null||r.lat_ms==null) return false;
    if(o.kind==='role') return key==='all'||r.role===key;
    if(o.kind==='node')
      return r.node===o.node&&(!o.role||r.role===o.role);
    return r.client===String(key).replace(/^client:/,'');
  });
}
function outlierScatter(key,name,color){
  const pts=outliersFor(key).map(function(r){
    return [Number(r.ts_epoch)*1000, Number(r.lat_ms)];
  });
  if(!pts.length) return null;
  return {name:name,type:'scatter',symbolSize:6,clip:true,z:9,
    itemStyle:{color:color||C.hero},
    emphasis:{itemStyle:{borderColor:C.ink,borderWidth:1}},
    /* 旗杆：从底部到离群点画细虚线，上端正好停在点上（否则全高竖线
       看不出离群值的位置） */
    markLine:{silent:true,symbol:'none',
      lineStyle:{color:color||C.hero,width:1,type:'dashed',opacity:0.5},
      label:{show:false},
      data:pts.map(function(p){
        return [{coord:[p[0],0]},{coord:[p[0],p[1]]}];})},
    data:pts};
}
function latLineSeries(col,x,meta,key){
  /* 颜色：节点=节点色系、实例=实例色阶（单/多对象一致）；总计/总览=灰阶 */
  const objColor=key?objLineColor(key):null;
  return LAT_LINES.filter(function(L){return latShown[L[0]];}).map(function(L){
    const k=L[0], color=objColor||latColor(k), arr=col[k]||[];
    const data=arr.map(function(v,i){return [x[i],v==null?0:v];});
    if(meta) meta.push({name:L[1],color:color,data:data,seriesIndex:meta.length});
    return {name:L[1],type:'line',showSymbol:false,clip:true,
      lineStyle:{width:L[2],color:color}, itemStyle:{color:color},
      z:(k==='p99')?6:3,
      data:data};
  });
}
/* 全局参考横线：取当前角色（写/读）的 role_summaries——正式窗口统计，与
   所选对象无关；basic（无 role_summaries）或值缺失则不画。虚线 + 同指标
   颜色 = 「同色同指标、虚线=全局常量」。不进入端点标签（mainLabelMeta）。 */
function globRoleSummary(){
  const z=Z(); if(!z) return null;
  const rs=z.role_summaries||{};
  if(rs[curRole]) return rs[curRole];
  return rs.writer||rs.reader||null;
}
function globalRefLines(x0,x1){
  if(x0==null||x1==null||x1<x0) return [];
  const rr=globRoleSummary(); if(!rr) return [];
  const out=[];
  GLOB_LINES.forEach(function(L){
    const k=L[0];
    if(!globShown[k]) return;
    const v=rr[k+'_ms'];
    if(v==null||!isFinite(Number(v))) return;
    const val=Number(v);
    out.push({name:'全局'+L[1],type:'line',showSymbol:false,clip:true,
      lineStyle:{width:1,type:'dashed',color:latColor(k),opacity:0.9},
      itemStyle:{color:latColor(k)}, z:2,
      data:[[x0,val],[x1,val]]});
  });
  return out;
}
function markLines(){
  const z=Z(); const data=[];
  if(z&&z.anomalies&&z.anomalies.length)
    z.anomalies.slice(0,50).forEach(function(a){
      data.push({xAxis:a.t*1000,
        lineStyle:{color:C.hero,width:1,opacity:0.15},
        label:{show:false}});});
  /* warmup 结束点：前 warmup 秒已并入测量窗口（图上绘制）但不计入正式
     统计；这里标一条竖线给出边界。 */
  if(z&&z.warmup_cutoff!=null&&isFinite(Number(z.warmup_cutoff))){
    data.push({xAxis:Number(z.warmup_cutoff)*1000,
      lineStyle:{color:C.fg,width:1,type:'dashed',opacity:0.75},
      label:{show:true,position:'insideEndTop',formatter:'warmup 结束',
             color:C.mut}});
  }
  return data.length?{symbol:'none',silent:true,data:data}:null;
}
/* 无数据窗口（两次运行/两次参数组之间）：单参数 run 与多 combo sweep 都会
   出现，此前图上就是一段空白、看不出是「没跑」还是「丢了数据」。找出
   相邻桶间隔 > gapSec 的区间，返回 markArea 用的 [[start,end],...]。 */
function dataGaps(tArr,gapSec){
  const out=[];
  if(!tArr||tArr.length<2) return out;
  for(let i=1;i<tArr.length;i++){
    const d=tArr[i]-tArr[i-1];
    if(d>gapSec){
      const label=(d>=120)?Math.round(d/60)+' 分钟无数据':d+' 秒无数据';
      out.push([{xAxis:tArr[i-1]*1000,name:label},
                {xAxis:tArr[i]*1000}]);
    }
  }
  return out;
}
/* 间隔判定阈值：以该序列自身的**常见桶宽**为基准，超过 3 倍才当作
   「无数据窗口」。这样 1s 桶的连续曲线不会被抖动误判，而 combo 之间
   十几秒/几分钟的空档（或运行中途断流）会稳定命中。 */
function gapThreshold(tArr){
  if(!tArr||tArr.length<3) return Infinity;
  const d=[];
  for(let i=1;i<tArr.length;i++) d.push(tArr[i]-tArr[i-1]);
  d.sort(function(a,b){return a-b;});
  const med=d[Math.floor(d.length/2)]||1;
  return Math.max(med*3,3);
}
/* 把无数据窗口画成浅色竖带 + 顶部小字（与离群标注区分：这里用极浅轨道色） */
function gapMarkArea(tArr,gapSec){
  const gaps=dataGaps(tArr,gapSec);
  if(!gaps.length) return null;
  /* 只给第一处标注文字，其余仅保留浅色带（避免多条文字重叠刷屏） */
  const data=gaps.map(function(g,i){
    if(i===0) return g;
    const start={xAxis:g[0].xAxis, label:{show:false}};
    return [start, g[1]];
  });
  return {silent:true,
    itemStyle:{color:LF_DARK?'rgba(240,240,238,.055)':'rgba(31,30,28,.045)'},
    label:{show:true,position:'insideTop',color:C.faint,fontSize:12,
      fontWeight:600,formatter:function(p){return p.name||'';}},
    data:data};
}
function fmtMs(v){
  v=Number(v)||0;
  if(v>=1000) return (v/1000).toFixed(2)+'s';
  if(v>=10) return v.toFixed(1)+'ms';
  return v.toFixed(2)+'ms';
}
let chart=null,qpsChart=null;
let mainLabelMeta=[];   /* 端点标识用：{name,color,data} */
/* 单步渲染隔离：任何一张图/一个区块抛错只丢自己，绝不整页中断 */
function safe(name,fn){
  try{ fn(); }
  catch(e){
    try{ console.error('[report] render '+name+' failed: '+(e&&e.message||e)); }
    catch(_e){}
  }
}
const latShown={p99:true,p50:true,avg:true,p999:true,max:true,
  min:true,p9999:true,qw:false};   /* 队列等待默认不展示 */
function renderLatSel(){
  const box=document.getElementById('latSel'); if(!box) return;
  box.innerHTML='';
  /* 操作按钮（矩形，与指标 pill 区分）固定在指标之前 */
  const allb=el('button','mini rect','全选');
  allb.title='显示全部指标（含队列等待）';
  allb.onclick=function(){
    LAT_LINES.forEach(function(L){latShown[L[0]]=true;});
    renderLatSel(); buildChart();
  };
  const clrb=el('button','mini rect','清空');
  clrb.title='关闭全部指标（保留 avg 防止空图）';
  clrb.onclick=function(){
    LAT_LINES.forEach(function(L){latShown[L[0]]=false;});
    latShown.avg=true;
    renderLatSel(); buildChart();
  };
  box.appendChild(allb); box.appendChild(clrb);
  LAT_LINES.forEach(function(L){
    const k=L[0];
    const b=el('button',latShown[k]?'':'off');
    const dot=el('span','dot'); dot.style.background=latColor(k);
    b.appendChild(dot);
    b.appendChild(el('span',null,L[1]));
    b.title=latShown[k]?('点击隐藏 '+L[1]):('点击显示 '+L[1]);
    b.onclick=function(){
      latShown[k]=!latShown[k];
      renderLatSel(); buildChart();
    };
    box.appendChild(b);
  });
  renderGlobSel();
}
/* 全局横线显隐（仅端到端时延图；默认 avg/p99/p9999） */
function renderGlobSel(){
  const box=document.getElementById('globSel'); if(!box) return;
  box.innerHTML='';
  GLOB_LINES.forEach(function(L){
    const k=L[0];
    const b=el('button',globShown[k]?'':'off');
    const dot=el('span','dot'); dot.style.background=latColor(k);
    b.appendChild(dot);
    b.appendChild(el('span',null,'全局'+L[1]));
    b.title=globShown[k]?('点击隐藏全局'+L[1]+'横线'):
                         ('点击显示全局'+L[1]+'横线');
    b.onclick=function(){
      globShown[k]=!globShown[k];
      renderGlobSel(); buildChart();
    };
    box.appendChild(b);
  });
}
function ensureSel(){
  const all=objAll();
  if(!Object.keys(all).length) return;
  selObjs=selObjs.filter(function(k){return !!all[k];});
  if(isPipeMode()){
    /* 早停只剩单一角色时自动切过去，避免空树空图 */
    if(!all[curRole]&&(all.writer||all.reader))
      curRole=all.writer?'writer':'reader';
    /* 只保留：当前角色聚合（总计开启时）+ 当前角色的节点/实例 */
    selObjs=selObjs.filter(function(k){
      const o=all[k]; if(!o) return false;
      if(o.kind==='role') return k===curRole&&totalOn;
      return o.role===curRole;
    });
    if(totalOn&&selObjs.indexOf(curRole)<0) selObjs.push(curRole);
    if(!totalOn) selObjs=selObjs.filter(function(k){return k!==curRole;});
    return;                     /* 允许空选择（图表显示空态） */
  }
  if(!selObjs.length){
    if(all['all']){selObjs=['all'];}
    else{
      const roles=Object.keys(all).filter(function(k){
        return all[k].kind==='role';});
      selObjs=roles.length?[roles[0]]:[Object.keys(all)[0]];
    }
  }
}
function emptyOpt(){const o=axisCommon();o.yAxis={type:'value'};o.series=[];return o;}
function buildChart(){
  const box=document.getElementById('mainchart'); if(!box) return;
  if(!chart) chart=echarts.init(box);
  bindHoverGuards(chart);
  const z=Z();
  if(!z){chart.clear();return;}
  ensureSel();
  const tier=currentTier();
  const opt=selObjs.length>1?multiOpt(tier):richOpt(tier,selObjs[0]);
  /* 全局统计横线：仅端到端时延图；x 域取首条「线」曲线的首尾 */
  if(opt.series&&opt.series.length){
    const _ln=opt.series.filter(function(ss){return ss.type==='line';})[0];
    const _d0=(_ln&&_ln.data)||[];
    if(_d0.length){
      const gl=globalRefLines(_d0[0][0],_d0[_d0.length-1][0]);
      if(gl.length) opt.series=opt.series.concat(gl);
    }
  }
  if(opt.series&&opt.series.length&&!opt.series[0].markLine){
    const ml=markLines(null,null,null);      // 异常窗口竖线
    if(ml) opt.series[0].markLine=ml;
  }
  chart.setOption(opt,true);
  safe('qps-chart',buildQpsChart);
  safe('x-pan',function(){syncXPan();});
  safe('y-scale',function(){
    yScaleState.domain=yDomainOf();
    setZoomP(yScaleState.p);
  });
  safe('end-labels',function(){layoutEndLabels(chart);});
}
function richOpt(tier,key){
  const col=colsOf(key,tier);
  if(!col||!col.t.length) return emptyOpt();
  const qps=colQps(col,tier);
  const x=col.t.map(function(t){return t*1000;});
  const o=axisCommon();
  o.grid=MAIN_GRID;
  o.tooltip=lfTooltip({trigger:'axis',show:true});
  o.tooltip.formatter=function(ps){
    const i=ps[0].dataIndex; const t=col.t[i];
    const rows=[['时间',fullts(t)],['请求数',col.c[i]],['成功',col.ok[i]],
      ['失败',col.fail[i]],['QPS',qps[i]],['平均(ms)',col.avg[i]],
      ['p50(ms)',col.p50[i]],['p99(ms)',col.p99[i]],['p999(ms)',col.p999[i]],
      ['最大(ms)',col.max[i]],['队列等待均(ms)',col.qw[i]],
      ['队列等待峰(ms)',col.qwmax[i]]];
    const _rr=globRoleSummary()||{};
    GLOB_LINES.forEach(function(L){
      if(!globShown[L[0]]) return;
      const _v=_rr[L[0]+'_ms'];
      if(_v==null||!isFinite(Number(_v))) return;
      rows.push(['全局 '+L[1]+'(ms)', Math.round(Number(_v)*10000)/10000]);
    });
    return '<b>'+fullts(t)+'</b><br>'+
      rows.map(function(r){return r[0]+'：'+r[1];}).join('<br>');
  };
  o.yAxis=[lfAxisValue({name:'ms',scale:true,nameLocation:'end',nameGap:14})];
  mainLabelMeta=[];
  o.series=latLineSeries(col,x,mainLabelMeta,key);
  if(outlierOn){
    const os=outlierScatter(key,'离群点',C.hero);
    if(os) o.series.push(os);
  }
  if(o.series.length){
    o.series[0].markLine=markLines();
    /* 无数据窗口（多 combo 之间的间隔 / 运行中途断流）画成浅带 */
    var ga=gapMarkArea(col.t,gapThreshold(col.t));
    if(ga) o.series[0].markArea=ga;
  }
  o.xAxis=lfAxisTime(xExtent(col.t[0],col.t[col.t.length-1],
    plotWidthOf(chart,MAIN_GRID)),'main');
  // 单对象：顶部图例删除（显隐统一在「时延曲线选择」区）
  o.legend={show:false};
  return o;
}
function multiOpt(tier){
  const o=axisCommon();
  o.grid=MAIN_GRID;
  o.tooltip=lfTooltip({trigger:'axis',show:true});
  /* 多对象 tooltip：时间 + 各对象各指标（带对象颜色圆点），不再落回
     ECharts 默认的原始毫秒时间戳 */
  o.tooltip.formatter=function(ps){
    if(!ps||!ps.length) return '';
    const v0=(ps[0].value&&ps[0].value[0]);
    const t=(v0!=null)?v0/1000:(ps[0].axisValue||0);
    const rows=ps.filter(function(p){
      return p.componentSubType==='line'||p.seriesType==='line';
    }).map(function(p){
      const v=(p.value&&p.value[1]!=null)?p.value[1]:null;
      return (p.marker||'')+p.seriesName+'：'+
        (v==null?'-':(Math.round(v*1000)/1000)+' ms');
    });
    return '<b>'+fullts(t)+'</b>'+(rows.length?'<br>'+rows.join('<br>'):'');
  };
  o.yAxis=[lfAxisValue({name:'ms',scale:true,
    nameLocation:'end',nameGap:14})];
  o.series=[];
  mainLabelMeta=[];
  let minT=null,maxT=null;
  selObjs.forEach(function(key,idx){
    const t=colsOf(key,tier)?tier:'cnt';
    const col=colsOf(key,t);
    if(!col||!col.t.length) return;
    const x=col.t.map(function(v){return v*1000;});
    const nm=objLabel(key)+(t!==tier?'（自适应）':'');
    const objColor=objLineColor(key);   /* null = role(总计) -> 按指标灰阶 */
    const oo=objAll()[key]||{};
    const kind=oo.kind||'client';
    const zbase=(kind==='role')?4:((kind==='node')?5:6);
    // 多对象：每个对象一整套指标曲线；node 用色系、实例用同系色阶、总计用灰阶
    LAT_LINES.forEach(function(L){
      const k=L[0];
      if(k==='p9999'&&(col.c||[]).some(function(n){return n<10000;})) return;
      if(!latShown[k]) return;
      const color=objColor||latColor(k);
      const v=col[k]||[];
      const data=v.map(function(val,i){return [x[i],val==null?0:val];});
      const seriesName=nm+' '+L[1];
      o.series.push({name:seriesName,type:'line',showSymbol:false,clip:true,
        lineStyle:{width:L[2],color:color},
        itemStyle:{color:color},
        z:(k==='p99')?(zbase+2):zbase,
        data:data});
    });
    if(outlierOn){
      const os=outlierScatter(key,nm+' 离群点',
        objColor||latColor('p99'));
      if(os) o.series.push(os);
    }
    if(col.t.length){
      const t0=col.t[0], t1=col.t[col.t.length-1];
      if(minT===null||t0<minT)minT=t0;
      if(maxT===null||t1>maxT)maxT=t1;
    }
    // 端点标签：用主指标（p99）代表该对象
    const vv=col.p99||[];
    const d99=vv.map(function(val,i){return [x[i],val==null?0:val];});
    const p99Color=objColor||latColor('p99');
    const p99Idx=LAT_LINES.filter(function(L){return latShown[L[0]];}).findIndex(function(L){return L[0]==='p99';});
    mainLabelMeta.push({name:nm,seriesName:nm+' p99',color:p99Color,data:d99,
      seriesIndex:idx*LAT_LINES.filter(function(L){return latShown[L[0]];}).length+p99Idx});
  });
  /* 图例只用于区分对象颜色：点击不得隐藏指标曲线（固定显示） */
  o.legend={show:true,top:0,type:'scroll',icon:'roundRect',itemWidth:14,
    itemHeight:3,itemGap:16,selectedMode:false,
    textStyle:{color:C.mut,fontSize:12,fontFamily:LFFONT}};
  if(minT!==null) o.xAxis=lfAxisTime(xExtent(minT,maxT,
    plotWidthOf(chart,MAIN_GRID)),'main');
  /* 多对象同图：无数据窗口挂在第一条曲线上（浅带，不抢指标线的视觉） */
  if(minT!==null&&o.series.length){
    var gx=[];
    selObjs.forEach(function(key){
      var t=colsOf(key,tier)?tier:'cnt';
      var cc=colsOf(key,t);
      if(cc&&cc.t&&cc.t.length>1) gx=gx.concat(cc.t);
    });
    if(gx.length>1){
      gx.sort(function(a,b){return a-b;});
      var gm=gapMarkArea(gx,gapThreshold(gx));
      if(gm) o.series[0].markArea=gm;
    }
  }
  return o;
}
/* ---- 端点标识：细线指向端点，标识纵向排开（避免完全重叠） ---- */
/* ---- 端点标识：固定锚定在绘图区右缘（不随缩放漂移） ----
   标识列位置恒定；引线从「当前窗口内最后一个可见点」斜向连到标识。
   标识按 y 排开（最小间距 19px），出界自动回推。 ---- */
function xWindowMs(ch,key){
  ch=ch||chart; key=key||'main';
  let s=0,e=100;
  try{
    const opt=ch.getOption();
    const dz=(opt.dataZoom||[])[0]||{};
    if(dz.start!=null)s=dz.start;
    if(dz.end!=null)e=dz.end;
  }catch(err){}
  const zs=zoomState(key);
  const ext=(zs.max-zs.min)||1;
  return [zs.min+ext*s/100, zs.min+ext*e/100];
}
/* 端点标签布局：标签纵向排开且不得越过绘图区上下沿。
   py = 引线指向曲线的那一端（y 轴缩放后 convertToPixel 可能给出绘图区外
   坐标），ly = 标签所在行 —— 两者都钳制在 [top,bot]。 */
function endLabelLayout(items, top, bot){
  if(!items||!items.length) return items||[];
  const n=items.length, range=bot-top;
  /* 目标间距 19px；标签过多/绘图区过矮时自适应压缩到放得下为止 */
  let MIN=n>1?Math.min(19,range/(n-1)):19;
  if(MIN<8)MIN=8;
  items.forEach(function(it){it.py=Math.max(top,Math.min(bot,it.py));});
  items.sort(function(a,b){return a.py-b.py;});
  /* 正向链：自上而下保间距 */
  items.forEach(function(it,i){
    it.ly=(i===0)?it.py:Math.max(it.py,items[i-1].ly+MIN);
  });
  if(items[n-1].ly>bot){
    /* 反向链：以 bot 为锚向上收 —— 贴底聚集的整簇被拉回界内，
       间距不破坏（旧的「整体上移+首项回移」会压塌间距，标签堆叠） */
    items[n-1].ly=Math.min(items[n-1].py,bot);
    for(let i=n-2;i>=0;i--)
      items[i].ly=Math.min(items[i].py,items[i+1].ly-MIN);
    if(items[0].ly<top){
      /* 顶底同时受限：自 top 起用当前值重压一遍（保持有序与间距） */
      items[0].ly=Math.max(items[0].py,top);
      for(let i=1;i<n;i++)
        items[i].ly=Math.max(items[i].ly,items[i-1].ly+MIN);
    }
  }
  /* 最终安全钳制（极端标签数下仍可能放不下，退化由间距测试兜底） */
  items.forEach(function(it){it.ly=Math.max(top,Math.min(bot,it.ly));});
  return items;
}
function layoutEndLabels(ch){
  const ov=document.getElementById('endlabels');
  if(!ov) return;
  ov.innerHTML='';
  ch.__mcLabelBox=ov;
  /* 多对象：固定槽位对象名标签（不绑任何曲线、不画引线） */
  if(selObjs.length>1){ layoutFixedLabels(ch,ov); return; }
  if(!ch||!ch.convertToPixel) return;          /* 桩环境跳过 */
  const w=xWindowMs(ch,'main');
  const anchorX=ch.getWidth()-MAIN_GRID.right+8;   /* 固定锚点 */
  const H=ch.getHeight?ch.getHeight():400;
  const top=MAIN_GRID.top+8, bot=H-MAIN_GRID.bottom-8;
  const items=[];
  mainLabelMeta.forEach(function(m,idx){
    let last=null;
    const data=m.data||[];
    for(let i=data.length-1;i>=0;i--){
      const d=data[i];
      if(d&&d[1]!=null&&d[0]<=w[1]&&d[0]>=w[0]){last=d;break;}
    }
    if(!last) return;
    let p=null;
    try{ p=ch.convertToPixel({seriesIndex:(m.seriesIndex!=null?m.seriesIndex:idx)},[last[0],last[1]]); }
    catch(e){ p=null; }
    if(!p||isNaN(p[0])||isNaN(p[1])) return;
    // 引线两端都限制在绘图区内（x/y 轴放大后数据点可能在窗口外，
    // 标签与引线端点都不得越过图片上/下沿）
    const plotL=MAIN_GRID.left, plotR=ch.getWidth()-MAIN_GRID.right;
    items.push({px:Math.max(plotL,Math.min(plotR,p[0])),
      py:Math.max(top,Math.min(bot,p[1])),ly:0,
      name:m.name,color:m.color});
  });
  if(!items.length) return;
  endLabelLayout(items,top,bot);
  items.forEach(function(it){
    /* 单对象标签聚焦：与多对象/分级曲线同一机制（setOption 压暗其他指标，
       不依赖 emphasis —— DR-42 已删除线序列 emphasis） */
    const dx=anchorX-it.px, dy=it.ly-it.py;
    const len=Math.max(1,Math.sqrt(dx*dx+dy*dy));
    const line=el('div','endleader');
    line.style.left=it.px+'px'; line.style.top=it.py+'px';
    line.style.width=len+'px'; line.style.height='1px';
    line.style.background=it.color;
    line.style.transformOrigin='0 50%';
    line.style.transform='rotate('+Math.atan2(dy,dx)+'rad)';
    ov.appendChild(line);
    const lab=el('div','endlabel',it.name);
    lab.style.left=anchorX+'px'; lab.style.top=it.ly+'px';
    lab.style.color=it.color;
    lab.title=it.name+'：悬浮聚焦该指标曲线';
    lab.onmouseenter=function(){mcFocus(ch,[it.name]);};
    lab.onmouseleave=function(){mcClearFocus(ch);};
    ov.appendChild(lab);
  });
}

/* 多对象右侧标签：总计最上，其余按对象树顺序（节点 → 其下实例）平铺；
   固定槽位、不画引线、不依赖任何曲线的端点（某条线消失不影响标签）；
   超长名称截断显示开头，悬浮时浏览器 title 展示全名；
   悬浮标签压暗其他对象、保留该对象整簇（不用 highlight 动作，避免
   emphasis.focus 的 blur 互相覆盖导致只亮一条）。 */
function fixedLabelText(key){
  const o=objAll()[key]||{};
  if(o.kind==='role'&&isPipeMode()&&key===curRole) return '总计';
  return String(objLabel(key));
}
function fixedLabelOrder(){
  const m=objTreeModel();
  const sel={}; selObjs.forEach(function(k){sel[k]=1;});
  const out=[];
  const roles=selObjs.filter(function(k){
    const o=objAll()[k]; return o&&o.kind==='role';});
  const ord={'all':0,'writer':1,'reader':2};
  roles.sort(function(a,b){
    return (ord[a]===undefined?9:ord[a])-(ord[b]===undefined?9:ord[b]);});
  roles.forEach(function(k){out.push(k);});
  m.nodes.forEach(function(nk){
    if(sel[nk]) out.push(nk);
    const nn=String((m.all[nk]&&m.all[nk].node)||nk.replace(/^node:/,''));
    (m.byNode[nn]||[]).forEach(function(ck){ if(sel[ck]) out.push(ck); });
  });
  selObjs.forEach(function(k){ if(out.indexOf(k)<0) out.push(k); });
  return out;
}
/* ---- 标签聚焦（两图共用）：悬浮预览，指针移入画布或离开标签即恢复全部。
   注意：分级图每次 setOption 会触发 'finished' 重建标签，hover 中的元素被
   销毁导致 mouseleave 丢失（曾表现为「聚焦卡死」）——因此画布 mousemove
   也会清除预览，保证一定能恢复。 */
function mcFocusInfo(ch){ return (ch&&ch.__mcFocus)||null; }
function mcSeriesHit(nm,f){
  if(!f||!f.prefixes||!f.prefixes.length) return true;
  for(let i=0;i<f.prefixes.length;i++){
    const p=f.prefixes[i];
    if(!p) continue;
    if(nm===p||nm.indexOf(p+' ')===0||nm.indexOf(p+'（')===0) return true;
  }
  return false;
}
function mcApplyFocus(ch){
  let sers=[];
  try{ sers=(ch.getOption().series)||[]; }catch(e){ return; }
  const f=mcFocusInfo(ch);
  const partials=sers.map(function(sr){
    const v=mcSeriesHit(String(sr.name||''),f)?1:0.15;
    return {lineStyle:{opacity:v},itemStyle:{opacity:v}};
  });
  try{ ch.setOption({series:partials}); }catch(e){}
  ch.__mcDimActive=!!f;
}
function mcMarkLabels(ch){
  const box=ch&&ch.__mcLabelBox; if(!box) return;
  Array.prototype.slice.call(box.children||[]).forEach(function(c){
    if(String(c.className||'').indexOf('endlabel')<0) return;
    c.className=String(c.className||'').replace(/\s*locked/g,'');
  });
}
/* 长名截断时，悬浮标签用详情框展示全名（替代原生 title） */
function mcHideLabelTip(ch){
  if(ch&&ch.__mcLabelTipEl){
    try{ ch.__mcLabelTipEl.parentNode.removeChild(ch.__mcLabelTipEl); }
    catch(e){}
    ch.__mcLabelTipEl=null;
  }
}
function mcShowLabelTip(ch,lab){
  const box=ch&&ch.__mcLabelBox; if(!box||!lab) return;
  const full=lab.getAttribute&&lab.getAttribute('data-full');
  if(!full) return;
  /* 桩环境没有 scrollWidth → 视为可能截断（测试可覆盖） */
  const trunc=(typeof lab.scrollWidth!=='number')||
    (lab.scrollWidth>lab.clientWidth+1);
  if(!trunc) return;
  mcHideLabelTip(ch);
  const el=document.createElement('div');
  el.className='mc-tip-box mc-label-tip';
  el.textContent=full;
  el.style.left='0px'; el.style.top='0px';
  box.appendChild(el);
  ch.__mcLabelTipEl=el;
  const bw=el.offsetWidth||240, bh=el.offsetHeight||24;
  const lx=(parseFloat(lab.style.left)||0)-bw-8;
  const ly=(parseFloat(lab.style.top)||0)-bh/2;
  const H=(ch.getHeight?ch.getHeight():400);
  el.style.left=Math.max(4,lx)+'px';
  el.style.top=Math.max(4,Math.min(ly,H-bh-4))+'px';
}
function mcFocus(ch,prefixes){
  if(!ch) return;
  if(!prefixes||!prefixes.length) ch.__mcFocus=null;
  else ch.__mcFocus={prefixes:prefixes.slice()};
  mcApplyFocus(ch); mcMarkLabels(ch);
}
function mcClearFocus(ch){
  if(!ch) return;
  mcHideLabelTip(ch);
  if(!ch.__mcFocus&&!ch.__mcDimActive) return;
  ch.__mcFocus=null;
  mcApplyFocus(ch); mcMarkLabels(ch);
}
function mcBindLabel(ch,lab,prefix){
  lab.setAttribute('data-prefix',prefix);
  lab.onmouseenter=function(){ mcFocus(ch,[prefix]); mcShowLabelTip(ch,lab); };
  lab.onmouseleave=function(){ mcClearFocus(ch); };
}
function mcAnchorY(ch,srIndex,last){
  let py=null;
  try{
    const p=ch.convertToPixel({seriesIndex:srIndex},[last[0],last[1]]);
    if(p&&isFinite(p[1])) py=p[1];
  }catch(e){}
  return py;
}
function mcLastVisible(data,w){
  let last=null;
  for(let i=(data||[]).length-1;i>=0;i--){
    const d=data[i];
    if(!d||d[1]==null) continue;
    if(w&&(d[0]<w[0]||d[0]>w[1])) continue;
    last=d; break;
  }
  if(!last){
    for(let i=(data||[]).length-1;i>=0;i--){
      const d=data[i];
      if(d&&d[1]!=null){ last=d; break; }
    }
  }
  return last;
}
/* 锚定排布：标签贴近各自曲线在当前窗口内的末端；endLabelLayout 防重叠；
   单序列不出标签；prefixOf 决定聚焦前缀（对象标签按对象名前缀匹配序列）。 */
function mcRenderLabels(ch,ov,items,grid){
  if(!ov) return;
  ov.innerHTML='';
  ch.__mcLabelBox=ov; ch.__mcGrid=grid;
  const H=(ch&&ch.getHeight)?ch.getHeight():400;
  const top=grid.top+8, bot=H-grid.bottom-8;
  if(items.length<2){
    if(ch.__mcFocus||ch.__mcDimActive) mcClearFocus(ch);
    return;
  }
  endLabelLayout(items,top,bot);
  items.forEach(function(it){
    const lab=el('div','endlabel fixed',it.name);
    lab.setAttribute('data-name',it.name);
    lab.setAttribute('data-full',it.name);
    lab.style.left=(ch.getWidth()-grid.right+8)+'px';
    lab.style.top=it.ly+'px';
    lab.style.color=it.color;
    mcBindLabel(ch,lab,it.prefix||it.name);
    ov.appendChild(lab);
  });
  mcMarkLabels(ch);
}
function focusObjectSeries(prefix){
  let sers=[];
  try{ sers=(chart.getOption().series)||[]; }catch(e){ return; }
  const partials=sers.map(function(sr){
    const nm=String(sr.name||'');
    const hit=!prefix||nm===prefix||nm.indexOf(prefix+' ')===0;
    const v=hit?1:0.15;
    return {lineStyle:{opacity:v},itemStyle:{opacity:v}};
  });
  try{ chart.setOption({series:partials}); }catch(e){}
}
function layoutFixedLabels(ch,ov){
  if(!ch||!ov) return;
  const meta=(mainLabelMeta||[]).slice();
  const keys=fixedLabelOrder();
  const w=xWindowMs(ch,'main');
  const H=ch.getHeight?ch.getHeight():400;
  const top=MAIN_GRID.top+8, bot=H-MAIN_GRID.bottom-8;
  let mx=-Infinity,mn=Infinity;
  meta.forEach(function(m){
    (m.data||[]).forEach(function(d){
      if(d&&d[1]!=null){ if(d[1]>mx)mx=d[1]; if(d[1]<mn)mn=d[1]; }
    });
  });
  if(!isFinite(mx)){mx=1;mn=0;}
  if(mx===mn)mx=mn+1;
  const items=[];
  keys.forEach(function(k){
    const pref=String(objLabel(k));
    let m=null;
    for(let q=0;q<meta.length;q++){
      const mn=String(meta[q].name||'');
      if(mn===pref||mn.indexOf(pref+'（')===0){m=meta[q];break;}
    }
    if(!m) return;
    const last=mcLastVisible(m.data||[],w);
    if(!last) return;
    let py=mcAnchorY(ch,(m.seriesIndex!=null?m.seriesIndex:0),last);
    if(py==null) py=top+((mx-last[1])/(mx-mn))*(bot-top);
    items.push({py:py,name:fixedLabelText(k),
      color:m.color||objLineColor(k)||latColor('p99'),prefix:pref});
  });
  mcRenderLabels(ch,ov,items,MAIN_GRID);
}

/* ---- 时延轴：左侧缩放滑块（倍率 1x..Y_ZOOM_MAX）+ 细刻度轨（窗口平移） ---- */
const Y_ZOOM_MAX = 10;                    /* 最大放大倍率（可限制：改这一个数） */
const yScaleState = {domain:[0,1], p:0};  /* p: 滑块位置 0(=1x)..1(=最大) */
function yDomainOf(){
  let mx=-Infinity,mn=Infinity;
  (mainLabelMeta||[]).forEach(function(m){
    (m.data||[]).forEach(function(d){
      if(d&&d[1]!=null){
        if(d[1]>mx)mx=d[1];
        if(d[1]<mn)mn=d[1];}});
  });
  if(!isFinite(mx)||!isFinite(mn)){mx=1;mn=0;}
  if(mx===mn)mx=mn+1;
  if(mn>0)mn=0;
  return [mn,mx];
}
function fmtFactor(F){
  return (F>=9.95?Math.round(F):F.toFixed(1))+'x';
}
function yWindowFromChart(){
  const d=yScaleState.domain, span=(d[1]-d[0])||1;
  let s=0,e=100;
  try{
    const opt=chart.getOption();
    const dz=(opt.dataZoom||[])[1]||{};
    if(dz.start!=null)s=dz.start;
    if(dz.end!=null)e=dz.end;
  }catch(err){}
  return [d[0]+s/100*span, d[0]+e/100*span];
}
function applyYWindow(lo,hi){
  const d=yScaleState.domain, span=(d[1]-d[0])||1;
  const minSpan=span*0.01;
  if(hi-lo<minSpan){                 /* 绝不倒转 */
    const mid=(lo+hi)/2;
    lo=mid-minSpan/2; hi=mid+minSpan/2;
  }
  lo=Math.max(d[0],Math.min(lo,d[1]-minSpan));
  hi=Math.min(d[1],Math.max(hi,d[0]+minSpan));
  if(hi<=lo)hi=Math.min(d[1],lo+minSpan);
  try{
    chart.dispatchAction({type:'dataZoom',dataZoomIndex:1,
      start:(lo-d[0])/span*100, end:(hi-d[0])/span*100});
  }catch(e){}
  syncYScale();
}
/* 滑块：p∈[0,1] -> 倍率 F=Y_ZOOM_MAX^p；放大锚点=当前窗口下边缘 */
function setZoomP(p){
  yScaleState.p=Math.max(0,Math.min(1,p));
  const d=yScaleState.domain, span0=(d[1]-d[0])||1;
  const F=Math.pow(Y_ZOOM_MAX,yScaleState.p);
  const w=yWindowFromChart();
  let lo=Math.max(d[0], w[0]);
  let hi=lo+span0/F;
  if(hi>d[1]){
    hi=d[1];
    lo=Math.max(d[0], hi-span0/F);
  }
  applyYWindow(lo,hi);
}
function syncYScale(){
  const rail=document.getElementById('yscaleRail');
  const win=document.getElementById('yscaleWin');
  const track=document.getElementById('zsliderTrack');
  const fill=document.getElementById('zsliderFill');
  const handle=document.getElementById('zsliderHandle');
  const cap=document.getElementById('zsliderCap');
  if(!rail||!win) return;
  const d=yScaleState.domain, span=(d[1]-d[0])||1;
  const w=yWindowFromChart();
  const topPct=Math.max(0,Math.min(100,(d[1]-w[1])/span*100));
  const botPct=Math.max(0,Math.min(100,(d[1]-w[0])/span*100));
  win.style.top=topPct+'%';
  win.style.height=Math.max(1.5,botPct-topPct)+'%';
  /* 把手行程 = 轨高-把手高：两端都不越界 */
  const th=(track&&track.clientHeight)||150;
  const travel=Math.max(1,th-13);
  const hx=(1-yScaleState.p)*travel;          /* 把手中心（自轨顶） */
  if(handle) handle.style.top=hx+'px';
  if(fill) fill.style.height=Math.max(0,th-hx-6.5)+'px';
  if(cap){
    cap.style.top=(hx+6.5)+'px';
    cap.textContent=fmtFactor(Math.pow(Y_ZOOM_MAX,yScaleState.p));
  }
}
function bindYScale(){
  const track=document.getElementById('zsliderTrack');
  const win=document.getElementById('yscaleWin');
  const rail=document.getElementById('yscaleRail');
  /* 滑块：改倍率必须拖把手；单击轨道不跳位 */
  const handle=document.getElementById('zsliderHandle');
  if(handle&&track){
    handle.onmousedown=function(ev){
      if(ev&&ev.preventDefault)ev.preventDefault();
      const z=document.getElementById('zslider');
      if(z){
        if(z.classList) z.classList.add('dragging');
        else z.className=(z.className+' dragging').trim();
      }
      const r=track.getBoundingClientRect();
      const set=function(y){
        setZoomP((r.bottom-y)/Math.max(1,r.height));
      };
      function mv(e){set(e.clientY);}
      function up(){
        if(z){
          if(z.classList) z.classList.remove('dragging');
          else z.className=String(z.className||'').replace(' dragging','').trim();
        }
        document.removeEventListener('mousemove',mv);
        document.removeEventListener('mouseup',up);
      }
      document.addEventListener('mousemove',mv);
      document.addEventListener('mouseup',up);
    };
  }
  /* 细轨窗口：平移 */
  if(win&&rail){
    win.onmousedown=function(ev){
      if(ev&&ev.preventDefault)ev.preventDefault();
      const r=rail.getBoundingClientRect();
      const d=yScaleState.domain;
      const w0=yWindowFromChart();
      const y0=ev.clientY;
      const span=(d[1]-d[0])||1;
      /* 1:1：鼠标像素 = 轨像素 = 数据域位移；端点硬限，到顶/到底即停 */
      const dvMin=d[0]-w0[0], dvMax=d[1]-w0[1];
      function mv(e){
        const raw=-(e.clientY-y0)/Math.max(1,r.height)*span;
        const dv=Math.max(dvMin,Math.min(dvMax,raw));
        applyYWindow(w0[0]+dv,w0[1]+dv);
      }
      function up(){
        document.removeEventListener('mousemove',mv);
        document.removeEventListener('mouseup',up);
      }
      document.addEventListener('mousemove',mv);
      document.addEventListener('mouseup',up);
    };
  }
}

/* ---- 底部时间轴横条：仅平移（缩放只走滚轮）；1:1 跟手，端点硬限 ---- */
function syncXPan(){
  const win=document.getElementById('xpanWin');
  if(!win) return;
  let s=0,e=100;
  try{
    const opt=chart.getOption();
    const dz=(opt.dataZoom||[])[0]||{};
    if(dz.start!=null)s=dz.start;
    if(dz.end!=null)e=dz.end;
  }catch(err){}
  win.style.left=s+'%';
  win.style.width=Math.max(1,e-s)+'%';
}
function bindXPan(){
  const strip=document.getElementById('xpan');
  const win=document.getElementById('xpanWin');
  if(!strip||!win) return;
  win.onmousedown=function(ev){
    if(ev&&ev.preventDefault)ev.preventDefault();
    const r=strip.getBoundingClientRect();
    let s0=0,e0=100;
    try{
      const opt=chart.getOption();
      const dz=(opt.dataZoom||[])[0]||{};
      if(dz.start!=null)s0=dz.start;
      if(dz.end!=null)e0=dz.end;
    }catch(err){}
    const zs=zoomState('main');
    const ext=(zs.max-zs.min)||1;
    const w0s=zs.min+ext*s0/100, w0e=zs.min+ext*e0/100;
    const x0=ev.clientX;
    const dvMin=zs.min-w0s, dvMax=zs.max-w0e;
    function mv(e){
      const raw=(e.clientX-x0)/Math.max(1,r.width)*ext;
      const dv=Math.max(dvMin,Math.min(dvMax,raw));
      try{
        chart.dispatchAction({type:'dataZoom',dataZoomIndex:0,
          start:(w0s+dv-zs.min)/ext*100,
          end:(w0e+dv-zs.min)/ext*100});
      }catch(err){}
      syncXPan();
    }
    function up(){
      document.removeEventListener('mousemove',mv);
      document.removeEventListener('mouseup',up);
    }
    document.addEventListener('mousemove',mv);
    document.addEventListener('mouseup',up);
  };
}

/* ---- QPS / 失败柱（共享时间轴；QPS 每桶一根柱，失败为独立计数轴红柱） ---- */
function qpsOpt(tier){
  const o=axisCommon();
  o.grid=QPS_GRID;
  /* 一个时间点只用一根竖直虚线；原生悬浮详情框列出该桶**全部对象**
     的 QPS+失败（槽位偏移会破坏 ECharts 按序列匹配，故用 formatter 兜底：
     按轴值找最近时间桶，直接从 qpsEntries 渲染全对象）。 */
  o.tooltip=lfTooltip({trigger:'axis',
    axisPointer:{type:'line',lineStyle:{type:'dashed',width:1,color:C.faint}}});
  o.tooltip.formatter=function(ps){
    if(!qpsChart||!qpsChart.__mcBuildTipAt||!qpsEntries.length) return '';
    let v=null;
    if(ps&&ps.length){
      v=(ps[0].value&&ps[0].value[0]);
      if(v==null) v=ps[0].axisValue;
    }
    const tArr=qpsEntries[0].t;
    let bi=0,best=Infinity;
    tArr.forEach(function(t,i){
      const d=Math.abs(t*1000-(Number(v)||0));
      if(d<best){best=d;bi=i;}
    });
    return qpsChart.__mcBuildTipAt(bi);
  };
  o.xAxis=lfAxisTime(null,'main');
  o.yAxis=[lfAxisValue({name:'QPS',scale:true,nameLocation:'end',nameGap:16}),
           lfAxisValue({name:'失败',position:'right',splitLine:{show:false},
             nameLocation:'end',nameGap:16})];
  /* filterMode 必须为 none：x 轴缩放时保留全部点，y 轴量程不随窗口重算 */
  o.dataZoom=zoomModel('xonly','none');
  o.series=[];
  const keys=selObjs.length?selObjs:(isPipeMode()?[]:['all']);
  /* QPS 柱与端到端曲线同配色：总计灰阶 / 节点色系 / 实例色阶；
     失败柱保持强调色不变 */
  const qcolors=LF_DARK?['#8F8E88','#B3B0A4','#6A6963','#DCDAD2']
                      :['#8F8E86','#6E6D66','#A5A49C','#4A4944'];
  /* 先收集有数据的对象（决定并排槽位），再建 series */
  const entries=[];
  let minT=null,maxT=null,stepMs=null;
  keys.forEach(function(key,idx){
    const t=colsOf(key,tier)?tier:'cnt';
    const col=colsOf(key,t);
    if(!col||!col.t.length) return;
    entries.push({key:key,col:col,t:t,qps:colQps(col,t),
      nm:objLabel(key),qc:objLineColor(key)||qcolors[idx%qcolors.length]});
    for(let i=1;i<col.t.length;i++){
      const d=(col.t[i]-col.t[i-1])*1000;
      if(d>0&&(stepMs===null||d<stepMs)) stepMs=d;
    }
    const t0=col.t[0], t1=col.t[col.t.length-1];
    if(minT===null||t0<minT)minT=t0;
    if(maxT===null||t1>maxT)maxT=t1;
  });
  if(!stepMs) stepMs=1000;
  const n=entries.length;
  /* 同一时间点的多条线**紧贴成一小簇**：每对象 1px 线 + 1px 间隙（定宽像素），
     不随桶宽铺开；偏移按当前窗口跨度换算成毫秒，缩放时由
     refreshQpsSlots() 重算（保证任何缩放级别下簇都保持紧凑）。 */
  const plotW=plotWidthOf(qpsChart,QPS_GRID);
  const w0=xWindowMs(qpsChart,'main');
  const spanWin=Math.max(1000,(w0&&w0.length===2)?(w0[1]-w0[0]):0);
  const barW=n>1?1:7;
  const slotPx=n>1?2:0;                 /* 线宽 1 + 间隙 1 */
  const slot=n>1?slotPx*spanWin/plotW:0;
  const failW=n>1?barW:3;
  const rad=n>1?0:[99,99,0,0];
  qpsEntries=entries.map(function(e){return {nm:e.nm,qps:e.qps,
    fail:(e.col.fail||[]),t:e.col.t,color:e.qc};});
  entries.forEach(function(e,i){
    const x=e.col.t.map(function(v){return v*1000;});
    const off=n>1?((i-(n-1)/2)*slot):0;
    o.series.push({name:e.nm+' QPS',type:'bar',yAxisIndex:0,clip:true,
      barMaxWidth:barW,emphasis:{disabled:true},
      itemStyle:{color:e.qc,borderRadius:rad},
      z:3, data:x.map(function(v,j){return [v+off,e.qps[j]];})});
    /* 失败柱：与所属对象的 QPS 细柱同槽位对齐（位置即对象），保持强调橙 */
    o.series.push({name:e.nm+' 失败',type:'bar',yAxisIndex:1,clip:true,
      barMaxWidth:failW,emphasis:{disabled:true},
      itemStyle:{color:C.hero,borderRadius:rad},z:4,
      data:x.map(function(v,j){return [v+off,(e.col.fail&&e.col.fail[j])||0];})});
  });
  /* 图例只列对象（QPS 色）；失败统一橙色，按槽位对齐辨识归属，不逐个列 */
  o.legend={top:0,type:'scroll',icon:'roundRect',itemWidth:14,itemHeight:3,
    itemGap:16,selectedMode:false,
    data:entries.map(function(e){return e.nm+' QPS';}),
    textStyle:{color:C.mut,fontSize:12,fontFamily:LFFONT}};
  if(minT!==null) o.xAxis=lfAxisTime(xExtent(minT,maxT,
    plotWidthOf(qpsChart,QPS_GRID)),'main');
  /* 无数据窗口：QPS 为 0 的空白段看不出是「没跑」还是「挂了」，标注出来 */
  if(o.series.length){
    var qall=[];
    entries.forEach(function(e){ qall=qall.concat(e.col.t); });
    qall.sort(function(a,b){return a-b;});
    var qg=gapMarkArea(qall,gapThreshold(qall));
    if(qg) o.series[0].markArea=qg;
  }
  return o;
}
let qpsEntries=[];      /* 最近一次 QPS 渲染的对象数据（缩放重算槽位用） */
function refreshQpsSlots(){
  /* 缩放/平移后按当前窗口重算簇内偏移（1px 线 + 1px 间隙），merge 更新
     序列数据，不重置 dataZoom。 */
  if(!qpsChart||!qpsEntries.length) return;
  const n=qpsEntries.length;
  const plotW=plotWidthOf(qpsChart,QPS_GRID);
  const w=xWindowMs(qpsChart,'main');
  const spanWin=Math.max(1000,(w&&w.length===2)?(w[1]-w[0]):0);
  const slot=n>1?2*spanWin/plotW:0;
  const partials=[];
  qpsEntries.forEach(function(e,i){
    const off=n>1?((i-(n-1)/2)*slot):0;
    const x=e.t.map(function(v){return v*1000+off;});
    partials.push({data:x.map(function(v,j){return [v,e.qps[j]];})});
    partials.push({data:x.map(function(v,j){return [v,e.fail[j]||0];})});
  });
  try{ qpsChart.setOption({series:partials}); }catch(err){}
}
function totalFails(tier){
  let sum=0;
  const keys=selObjs.length?selObjs:(isPipeMode()?[]:['all']);
  keys.forEach(function(key){
    const t=colsOf(key,tier)?tier:'cnt';
    const col=colsOf(key,t);
    if(!col||!col.fail) return;
    for(let i=0;i<col.fail.length;i++) sum+=(col.fail[i]||0);
  });
  return sum;
}
function buildQpsChart(){
  const box=document.getElementById('qpschart'); if(!box) return;
  if(!qpsChart) qpsChart=echarts.init(box);
  bindHoverGuards(qpsChart);
  if(!qpsChart.__slotBound){
    qpsChart.__slotBound=true;
    try{ qpsChart.on('dataZoom',refreshQpsSlots); }catch(e){}
  }
  /* 点击详情框（自绘）：按像素换算到最近时间桶，列出全部对象 QPS+失败 */
  qpsChart.__mcBuildTipAt=function(bi){
    if(!qpsEntries.length) return '';
    const t0=qpsEntries[0].t[bi];
    if(t0==null) return '';
    const rows=qpsEntries.map(function(e){
      const q=e.qps[bi], f=e.fail[bi]||0;
      const dot='<span class="dot" style="display:inline-block;width:8px;'+
        'height:8px;border-radius:99px;margin-right:5px;background:'+
        (e.color||C.fg)+'"></span>';
      return dot+e.nm+'：QPS '+(q==null?'-':Math.round(q*100)/100)+
        ' · 失败 '+f;
    });
    return '<b>'+fullts(t0)+'</b><br>'+rows.join('<br>');
  };
  const z=Z(); if(!z){qpsChart.clear();return;}
  const tier=currentTier();
  qpsChart.setOption(qpsOpt(tier),true);
  const note=document.getElementById('qpsNote');
  if(note){
    const tf=totalFails(tier);
    note.textContent=tf>0
      ? ('失败合计 '+tf+' 次 —— 细红柱为失败数（右侧独立计数轴，与 QPS 不同量纲）。')
      : '本次测试无失败。';
  }
}
/* ---- 拖拽平移（x/y 互斥） ---- */
function panZoom(ch,idx,delta,extent){
  try{
    const opt=ch.getOption();
    const dz=(opt.dataZoom||[])[idx]||{};
    let s=dz.start,e=dz.end;
    if(s==null||e==null) return;
    const span=Math.max(0.001,e-s);
    let ns=s-(delta/extent)*span;
    ns=Math.max(0,Math.min(100-span,ns));
    ch.dispatchAction({type:'dataZoom',dataZoomIndex:idx,
      start:ns,end:ns+span});
  }catch(err){}
}
function bindInteract(ch,grid){
  if(!ch) return;
  let zr=null;
  try{ zr=ch.getZr&&ch.getZr(); }catch(e){ zr=null; }
  if(!zr||!zr.on) return;
  let drag=null;
  zr.on('mousedown',function(e){
    const g=grid||MAIN_GRID;
    const w=ch.getWidth()||0,h=ch.getHeight()||0;
    const ph=Math.max(1,h-g.top-g.bottom), pw=Math.max(1,w-g.left-g.right);
    drag={x:e.offsetX,y:e.offsetY};
  });
  zr.on('mousemove',function(e){
    if(!drag) return;
    const dx=e.offsetX-drag.x, dy=e.offsetY-drag.y;
    if(Math.abs(dx)<4&&Math.abs(dy)<4) return;
    const g=grid||MAIN_GRID;
    const w=ch.getWidth()||0,h=ch.getHeight()||0;
    const pw=Math.max(1,w-g.left-g.right), ph=Math.max(1,h-g.top-g.bottom);
    if(Math.abs(dx)>=Math.abs(dy)) panZoom(ch,0,dx,pw);
    else panZoom(ch,1,dy,ph);
    drag={x:e.offsetX,y:e.offsetY};
  });
  zr.on('mouseup',function(){drag=null;});
  zr.on('globalout',function(){drag=null;});
}
let zoomLock=false;
function linkZoom(a,b){
  if(!a||!b) return;
  function push(src,dst){
    try{
      src.on('datazoom',function(){
        if(zoomLock) return;
        zoomLock=true;
        try{
          const opt=src.getOption();
          const dz=(opt.dataZoom||[])[0]||{};
          if(dz.start==null) return;
          dst.dispatchAction({type:'dataZoom',dataZoomIndex:0,
            start:dz.start,end:dz.end});
        }catch(e){}
        zoomLock=false;
      });
    }catch(e){}
  }
  push(a,b); push(b,a);
}
function resetViews(){
  [chart,qpsChart].forEach(function(c){
    if(!c) return;
    try{
      c.dispatchAction({type:'dataZoom',dataZoomIndex:0,start:0,end:100});
      c.dispatchAction({type:'dataZoom',dataZoomIndex:1,start:0,end:100});
    }catch(e){}
  });
  safe('y-scale',function(){
    yScaleState.domain=yDomainOf();
    setZoomP(0);
  });
}

/* 参数组（combo）筛选：sweep 时把每条曲线限定到某几组；默认「全部」。
   选择结果直接反映到对象树可见集合与明细表，避免顶部合并口径被误读成
   单组数值。 */
let comboFilter=null;           /* null = 全部 */
function combosCur(s){
  const z=Z();
  return ((z&&z.summary&&z.summary.combos)||[]);
}
function renderComboBar(){
  const bar=document.getElementById('comboBar');
  const box=document.getElementById('comboBtns');
  if(!bar||!box) return;
  const z=Z();
  const cs=combosCur(z);
  bar.style.display=cs.length>1?'':'none';
  box.innerHTML='';
  if(cs.length<2){comboFilter=null;return;}
  function btn(label,val){
    const on=(comboFilter===val);
    const b=el('button',on?'on':'',label);
    b.onclick=function(){
      comboFilter=val;
      /* 切组后若选中了被隐藏的实例，回落到「总体」，避免空图 */
      if(comboFilter){
        const keep=selObjs.filter(function(k){
          return k.indexOf('client:')!==0||clientInCombo(k);});
        selObjs=keep.length?keep:['all'];
      }
      renderComboBar(); renderObjTree(); renderControls();
      renderSummary(); renderSlowStrip();
      buildChart(); buildQpsChart();
    };
    box.appendChild(b);
  }
  btn('全部 ('+cs.length+')',null);
  cs.forEach(function(c){btn('c'+c.c+'×t'+c.t,'c'+c.c+'_t'+c.t);});
}
/* 客户端名是否属于当前选中的参数组（名字形如 benchmark_c4_t8） */
function clientInCombo(name){
  if(!comboFilter) return true;
  const m=String(name||'').match(/benchmark_c(\d+)_t(\d+)/);
  if(!m) return true;                     /* 解析不了的不隐藏，避免丢数据 */
  return ('c'+m[1]+'_t'+m[2])===comboFilter;
}
function perfCardsInto(box,s){
  /* 性能统计两行：第一行（吞吐 / avg / p99 / p9999）加粗强调，
     第二行（p50 / p95 / p999 / min / max + 队列等待）常规字重。
     详略得当：视觉重心只落在第一行。 */
  box.appendChild(kvStrong('吞吐', s.throughput_mb_s==null?'-':
    Number(s.throughput_mb_s).toFixed(1), 'MB/s'));
  box.appendChild(kvStrong('avg', s.avg_ms, 'ms'));
  box.appendChild(kvStrong('p99', s.p99_ms, 'ms'));
  box.appendChild(kvStrong('p9999', s.p9999_ms, 'ms'));
  box.appendChild(gridBreak());
  box.appendChild(kv('p50', s.p50_ms, 'ms'));
  box.appendChild(kv('p95', s.p95_ms, 'ms'));
  box.appendChild(kv('p999', s.p999_ms, 'ms'));
  box.appendChild(kv('min', s.min_ms, 'ms'));
  box.appendChild(kv('max', s.max_ms, 'ms'));
}
function infoCardsInto(box,s){
  /* 第一行：总数/成功/失败/失败率/QPS；第二行：队列相关项（若有） */
  box.appendChild(kv('总数', s.total));
  box.appendChild(kv('成功', s.ok));
  box.appendChild(kv('失败', s.fail));
  box.appendChild(kv('失败率', ((s.fail_rate||0)*100).toFixed(2), '%'));
  box.appendChild(kv('QPS', s.qps));
  const hasQw=(s.qw_avg_ms!=null||s.qw_max_ms!=null||s.max_depth);
  if(hasQw){
    box.appendChild(gridBreak());
    if(s.qw_avg_ms!=null)box.appendChild(kv('队列等待均', s.qw_avg_ms, 'ms'));
    if(s.qw_max_ms!=null)box.appendChild(kv('队列等待峰', s.qw_max_ms, 'ms'));
    if(s.max_depth)box.appendChild(kv('最大队列深度', s.max_depth));
  }
  // kvtest: read-path value verification failures (correctness signal)
  const vf=Number(s.verify_failures)||0;
  if(vf>0){
    box.appendChild(gridBreak());
    const w=kv('值校验失败', vf);
    w.className='kv fail';
    box.appendChild(w);
  }
}
function renderSummary(){
  const z=Z(); if(!z) return;
  const s=z.summary;
  const pg=document.getElementById('perfcards');
  const ug=document.getElementById('unitmeta');
  const rc=document.getElementById('rolecards');
  const pbox=document.getElementById('params');
  const secP=document.getElementById('secPerf');
  if(!pg||!pbox) return;
  pg.innerHTML=''; pbox.innerHTML='';
  if(ug) ug.innerHTML='';
  if(rc){rc.innerHTML=''; rc.style.display='none';}
  /* 报告顶部口径注记：哪些统计值用哪种方式获取（受控近似，如实标注） */
  const pn=document.getElementById('pctNote');
  if(pn){
    const _pm=z.pct_method||'hist'; const _pk=z.pct_k;
    pn.style.display='';
    pn.textContent=(_pm==='hist+digest')
      ? ('统计口径：count/avg/min/max 精确；p50/p90/p95 = 合并直方图插值'
         +'（≈1-2%）；p99/p999/p9999 = REQ sketch（k='+(_pk||'?')
         +'，尾部 ≤0.6%）')
      : ('统计口径：count/avg/min/max 精确；p50/p90/p95 = 合并直方图插值'
         +'（≈1-2%）；p99/p999/p9999 = 直方图插值（本环境未启用 REQ sketch）');
  }
  renderParams(pbox);
  if(!s){
    if(secP)secP.style.display='';
    pg.style.display='';
    pg.appendChild(el('div','note',
      '该 data_size 只有 C++ trace 统计（无 logstats 数据），下方仍可查看全链路区。'));
    if(ug) ug.style.display='none';
    return;
  }
  if(ug) ug.style.display='';
  const pmeta={};
  ((D.meta&&D.meta.params)||[]).forEach(function(p){
    if(p)pmeta[String(p[0])]=p[1];
  });
  const um=D.meta||{};
  const isPipe=isPipeMode();
  const infoSize=um.unit_size||pmeta['数据大小']||curSize||'-';
  const infoClients=(um.unit_clients!=null&&um.unit_clients!=='')
    ?um.unit_clients:(pmeta['客户端数']!=null?pmeta['客户端数']:s.clients);
  const infoThreads=(um.unit_threads!=null&&um.unit_threads!=='')
    ?um.unit_threads:(pmeta['线程数']!=null?pmeta['线程数']:'-');
  const nodesN=Number(pmeta['节点数'])||0;
  const cpnN=Number(pmeta['client.count_per_node'])||0;
  const notifyN=Number(pmeta['notify_count'])||0;
  const storeN=Number(pmeta['store_count_per_node'])||0;
  const rs=(z.role_summaries)||{};
  const roles=Object.keys(rs);
  if(isPipe&&roles.length){
    /* pipeline：writer/reader 各自完整一套（性能统计两行 + 第三行统计项） */
    if(secP)secP.style.display='none';
    pg.style.display='none';
    if(rc){
      rc.style.display='';
      [['writer','写路径（writer）'],['reader','读路径（reader）']]
        .forEach(function(x){
          const rr=rs[x[0]]; if(!rr) return;
          const blk=el('div','rblock');
          blk.appendChild(el('h3','sec',x[1]));
          const p2=el('div','grid'); perfCardsInto(p2,rr); blk.appendChild(p2);
          const i2=el('div','grid'); infoCardsInto(i2,rr); blk.appendChild(i2);
          rc.appendChild(blk);
        });
    }
  }else{
    if(secP)secP.style.display='';
    pg.style.display='';
    perfCardsInto(pg,s);
    /* 第三行：总数/成功/失败/失败率/QPS（+ 队列行/校验） */
    pg.appendChild(gridBreak());
    infoCardsInto(pg,s);
    /* 多参数组（sweep）时顶部为合并口径，明确标注 */
    const cs=(s.combos||[]);
    if(cs.length>1){
      const note=el('div','note');
      note.textContent='本次为多参数组 sweep：性能统计为 '+cs.length+
        ' 组合并口径（'+cs.map(function(x){return 'c'+x.c+'×t'+x.t;}).join('、')+
        '），单组数值见下方明细表与各对象曲线。';
      pg.appendChild(note);
    }
  }
  /* 单元信息行：紧随参数配置标题、位于参数表格上方 */
  if(ug){
    if(isPipe&&cpnN&&nodesN){
      ug.appendChild(kv('数据大小', infoSize));
      /* 部署模式固定放左侧第二个位置 */
      ug.appendChild(kv('部署模式',
        cpnN+'w'+(notifyN?notifyN:'-')+'r'));
      ug.appendChild(kv('客户端数', cpnN+'×'+nodesN+' nodes'));
      if(storeN) ug.appendChild(kv('store 数量', storeN+'×'+nodesN+' nodes'));
      ug.appendChild(kv('时长', s.wall_s>0?fmtDur(s.wall_s):'-'));
    }else{
      ug.appendChild(kv('数据大小', infoSize));
      ug.appendChild(kv('客户端数', infoClients));
      ug.appendChild(kv('线程数', infoThreads));
      ug.appendChild(kv('时长', s.wall_s>0?fmtDur(s.wall_s):'-'));
    }
  }
}
function renderParams(box){
  const ps=(D.meta&&D.meta.params)||[];
  if(!ps.length){
    box.appendChild(el('div','note','无参数信息（旧报表或未注入）'));
    return;
  }
  /* 段1 已给出的值不在参数配置里重复 */
  const HIDE={'数据大小':1,'客户端数':1,'线程数':1,'客户端总数':1,
              '采集时长':1,'开始时间':1,
              /* pipeline：这些值已在单元信息行/分角色区块给出 */
              'client.count_per_node':1,'store_count_per_node':1,
              'notify_count':1};
  ps.forEach(function(p){
    if(p&&HIDE[String(p[0])])return;
    const row=el('div','prow');
    row.appendChild(el('span','pk',String(p[0])));
    const v=el('span','pv');
    v.textContent=(p[1]==null||p[1]==='')?'-':String(p[1]);
    v.title=v.textContent;
    row.appendChild(v);
    box.appendChild(row);
  });
}
let histChart=null, slowChart=null;
function binIndexFor(edges,v){
  for(let i=0;i<edges.length-1;i++){ if(v<=edges[i+1]) return i; }
  return null;
}
function fmtUsRange(lo,hi){
  const f=function(v){return v>=1000?((v/1000).toFixed(v>=10000?0:1)+'ms')
    :(Math.round(v)+'µs');};
  return f(lo)+'–'+f(hi);
}
function renderHist(){
  const z=Z(); const card=document.getElementById('histcard'); if(!card) return;
  const sm=(z&&z.summary)||{};
  const bins=sm.hist_bins||[];
  // logstats 直方图阶梯单位是 ms；统一换算成 µs 再格式化/定位标注线
  const edges=(sm.bin_edges||[]).map(function(v){return v*1000;});
  if(!bins.length||edges.length<2){card.style.display='none';return;}
  card.style.display='';
  const box=document.getElementById('histchart');
  if(!histChart) histChart=echarts.init(box);
  bindHoverGuards(histChart);
  const labels=[], counts=[];
  bins.forEach(function(x){
    const lo=edges[x[0]]!=null?edges[x[0]]:0;
    const hi=(edges[x[0]+1]!=null?edges[x[0]+1]:lo);
    labels.push(fmtUsRange(lo,hi)); counts.push(x[1]);
  });
  const o=axisCommon();
  o.tooltip=lfTooltip();
  o.grid={left:104,right:104,top:30,bottom:44};
  o.xAxis={type:'category',data:labels,axisLine:{show:false},
    axisTick:{show:false},splitLine:{show:false},
    axisLabel:{color:C.mut,fontSize:12,fontWeight:600,fontFamily:LFFONT,
      interval:Math.max(1,Math.floor(labels.length/9)),rotate:0}};
  o.yAxis=[lfAxisValue({name:'样本'})];
  o.dataZoom=[];
  o.series=[{name:'样本数',type:'bar',data:counts,barWidth:'58%',
    itemStyle:{color:lfColor(0),borderRadius:[99,99,0,0]}}];
  const p50=(sm.p50_ms||0)*1000, p99=(sm.p99_ms||0)*1000;
  const p999=(sm.p999_ms||0)*1000, p9999=(sm.p9999_ms||0)*1000;
  const avg=(sm.avg_ms||0)*1000;
  // 标注线按「阶梯下标 -> 非零桶位置」映射；labels 只包含非零桶，
  // 直接 labels[阶梯下标] 会得到 undefined（真实 ECharts 会抛
  // "reading 'coord'"，曾杀死整页后续渲染）
  function binPos(ladderIdx){
    if(ladderIdx==null) return null;
    for(let i=0;i<bins.length;i++){ if(bins[i][0]===ladderIdx) return i; }
    return null;
  }
  /* 配色：所有标注（文字 + 竖直虚线）统一用 p50 的灰色样式（全灰阶）。 */
  const INK_MID=LF_DARK?'#9B9A93':'#8F8E86';
  const mk=[];
  function mkLine(value,label){
    const pos=binPos(binIndexFor(edges,value));
    if(pos==null) return;
    mk.push({pos:pos,xAxis:labels[pos],label:label});
  }
  mkLine(avg,'avg');
  mkLine(p50,'p50');
  mkLine(p99,'p99');
  mkLine(p999,'p999');
  if((sm.total||0)>=10000) mkLine(p9999,'p9999');
  /* 标签防重叠：分位数常挤在相邻桶里，全部 position:'end' 会叠成一坨。
     水平一律居中（grid 左右各留 104px，不会压画布边界）。碰撞按**像素
     距离**判定：相邻桶的两个标签若水平会重叠，就把后者抬到更高一层
     （distance 递增）——只看「同一分箱」会让相邻桶的 p999/p9999 粘在
     一起（曾出现 "p999p9999" 连写）。 */
  mk.sort(function(a,b){return a.pos-b.pos;});
  const plotW=Math.max(200,(box.clientWidth||1000)-(o.grid.left+o.grid.right));
  const binW=plotW/Math.max(1,bins.length);
  const labelW=function(t){return String(t).length*8+12;};
  const placed=[];
  let maxLevel=0;
  mk.forEach(function(m){
    const w=labelW(m.label);
    let level=0;
    for(;;){
      const clash=placed.some(function(p){
        return p.level===level &&
          Math.abs((p.pos-m.pos)*binW)<(p.w+w)/2+8;
      });
      if(!clash) break;
      level++;
    }
    placed.push({pos:m.pos,w:w,level:level});
    if(level>maxLevel) maxLevel=level;
    m.mk={xAxis:m.xAxis,
      lineStyle:{color:INK_MID,width:1.2,type:'dashed'},
      label:{formatter:m.label,color:INK_MID,
        fontSize:12,fontWeight:600,position:'end',
        distance:6+level*14,align:'center'}};
  });
  if(mk.length) o.series[0].markLine={symbol:'none',silent:true,
    data:mk.map(function(m){return m.mk;})};
  /* 同箱多指标上下罗列时（多个分位数落在同一区间），最上层标签在
     position:'end' 之外还要再上移 level*14px。顶部留白必须按最上层
     层数动态加高，否则最上面的文字会被图表上沿裁掉。 */
  if(mk.length&&maxLevel>0)
    o.grid={left:o.grid.left,right:o.grid.right,
      top:30+maxLevel*14,bottom:o.grid.bottom};
  histChart.setOption(o,true);
}
function tsEpoch(ts){
  if(!ts) return null;
  const m=String(ts).match(
    /(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?/);
  if(!m) return null;
  return new Date(+m[1],+m[2]-1,+m[3],+m[4],+m[5],+m[6],
    m[7]?Math.round(Number('0.'+m[7])*1000):0).getTime()/1000;
}
/* 行内绝对时刻：优先用后端归一好的 ts_epoch（不受浏览器时区影响）；
   老报表没有该字段时回退到按本地钟面解析 raw ts。 */
function rowEpoch(r){
  if(!r) return null;
  const e=Number(r.ts_epoch);
  return (r.ts_epoch!=null&&isFinite(e))?e:tsEpoch(r.ts);
}
/* ---------------- 慢点捕捉（独立控件，与主图解耦） ----------------
   叠加语义：总计（灰）= 当前角色全部慢点；勾选节点/实例 = 同位置高亮叠加；
   总计不自动关闭（仅手动开关）。角色单选（pipeline，默认 reader）。 */
let slowRole='reader';
let slowTotal=true;
let slowSel=[];        /* 选中高亮的节点/实例对象键（独立于主图选择） */
let slowOpen={};       /* 节点展开状态 */
function slowRows(){
  const z=Z(); if(!z) return [];
  const rows=(z.slowest||[]).slice();
  if(!isPipeMode()) return rows;
  return rows.filter(function(r){return r.role===slowRole;});
}
function slowMatchesKey(r,key){
  const k=String(key);
  if(k.indexOf('client:')===0) return ('client:'+(r.client||''))===k;
  const m=k.match(/^node:([^:]+)(?::(.+))?$/);
  if(!m) return false;
  if((r.node||'未归属节点')!==m[1]) return false;
  return !m[2]||r.role===m[2];
}
function slowLabelOf(key){
  const o=objAll()[key]||{};
  if(o.kind==='node') return '节点 '+(o.node||o.label);
  return String(key).replace(/^client:/,'');
}
function slowToggle(key){
  const i=slowSel.indexOf(key);
  if(i>=0) slowSel.splice(i,1); else slowSel.push(key);
  renderSlowStrip();
}
function slowAllKeys(kind){
  const out=[]; const all=objAll();
  Object.keys(all).forEach(function(k){
    const o=all[k];
    if(kind&&o.kind!==kind) return;
    if(o.kind!=='node'&&o.kind!=='client') return;
    if(isPipeMode()&&o.role!==slowRole) return;
    out.push(k);
  });
  return out;
}
function slowColorOf(key){
  const o=objAll()[key]||{};
  if(o.kind==='node') return nodeHue(o.node||o.label);
  return instanceColor(o.node,key);
}
function renderSlowControls(){
  const box=document.getElementById('slowctl'); if(!box) return;
  box.innerHTML='';
  const z=Z(); if(!z) return;
  const row1=el('div','row');
  if(isPipeMode()){
    row1.appendChild(el('span','tl-label','角色'));
    ['reader','writer'].forEach(function(r){
      const b=el('button','role-btn'+(slowRole===r?' on':''),r);
      b.title='慢点捕捉按角色过滤（独立于主图）';
      b.onclick=function(){
        if(slowRole===r) return;
        slowRole=r; slowSel=[]; renderSlowStrip();
      };
      row1.appendChild(b);
    });
  }
  const tb=el('button','total-btn'+(slowTotal?' on':''),'总计');
  tb.title='总计 = 当前角色全部慢点（灰色背景点）';
  tb.onclick=function(){slowTotal=!slowTotal;renderSlowStrip();};
  row1.appendChild(tb);
  box.appendChild(row1);
  const row2=el('div','row');
  row2.appendChild(el('span','tl-label','节点与实例'));
  function sbtn(label,title,fn){
    const b=el('button','mini',label);
    b.title=title; b.onclick=fn; return b;
  }
  function slowKindAll(kind){
    const ks=slowAllKeys(kind);
    return ks.length>0&&ks.every(function(k){return slowSel.indexOf(k)>=0;});
  }
  function slowKindSet(kind,on){
    const ks=slowAllKeys(kind);
    if(on){
      ks.forEach(function(k){if(slowSel.indexOf(k)<0)slowSel.push(k);});
    }else{
      slowSel=slowSel.filter(function(k){return ks.indexOf(k)<0;});
    }
    renderSlowStrip();
  }
  const sndAll=slowKindAll('node'), sclAll=slowKindAll('client');
  row2.appendChild(sbtn(sndAll?'清空节点':'全选节点',
    sndAll?'取消全部节点慢点高亮':'高亮当前范围内全部节点的慢点',
    function(){slowKindSet('node',!sndAll);}));
  row2.appendChild(sbtn(sclAll?'清空实例':'全选实例',
    sclAll?'取消全部实例慢点高亮':'高亮当前范围内全部实例的慢点',
    function(){slowKindSet('client',!sclAll);}));
  row2.appendChild(sbtn('全部清空','清空节点与实例高亮，并自动显示总计',
    function(){slowSel=[];slowTotal=true;renderSlowStrip();}));
  box.appendChild(row2);
  const nodes={};
  (z.clients||[]).forEach(function(c){
    if(isPipeMode()&&c.role!==slowRole) return;
    const n=c.node||'未归属节点';
    (nodes[n]=nodes[n]||[]).push(c.name);
  });
  Object.keys(nodes).sort().forEach(function(n){
    const row=el('div','row');
    const open=!!slowOpen[n];
    const caret=el('button','tree-caret',open?'▾':'▸');
    caret.title=open?'收起该节点实例':'展开该节点实例';
    caret.onclick=function(){slowOpen[n]=!slowOpen[n];renderSlowStrip();};
    const nodeKey=isPipeMode()?('node:'+n+':'+slowRole):('node:'+n);
    const nb=el('button','slow-chip-btn'+(slowSel.indexOf(nodeKey)>=0?' on':''),n);
    nb.title='高亮该节点的全部慢点';
    nb.onclick=function(){slowToggle(nodeKey);};
    row.appendChild(caret);
    row.appendChild(nb);
    if(open){
      nodes[n].sort().forEach(function(name){
        const ck='client:'+name;
        const cb=el('button','slow-chip-btn'+
          (slowSel.indexOf(ck)>=0?' on':''),name.replace(/^.*\.benchmark_/,''));
        cb.title=name;
        cb.onclick=function(){slowToggle(ck);};
        row.appendChild(cb);
      });
    }
    box.appendChild(row);
  });
}
function renderSlowStrip(){
  const z=Z(); const card=document.getElementById('slowcard'); if(!card) return;
  renderSlowControls();
  const rows=slowRows();
  if(!rows.length){card.style.display='none';return;}
  card.style.display='';
  const box=document.getElementById('slowchart');
  if(!slowChart) slowChart=echarts.init(box);
  bindHoverGuards(slowChart);
  const pts=rows.filter(function(r){return rowEpoch(r)!=null;});
  const shape=(isPipeMode()&&slowRole==='writer')?'rect':'circle';
  const series=[];
  if(slowTotal){
    series.push({name:'总计（'+slowRole+'）',type:'scatter',symbol:shape,
      symbolSize:7,clip:true,z:3,
      itemStyle:{color:C.mut,opacity:.55},
      data:pts.map(function(r){
        return [rowEpoch(r)*1000,Number(r.lat_ms)||0,r];})});
  }
  slowSel.forEach(function(key){
    const pr=pts.filter(function(r){return slowMatchesKey(r,key);});
    if(!pr.length) return;
    series.push({name:slowLabelOf(key),type:'scatter',symbol:shape,
      symbolSize:9,clip:true,z:6,
      itemStyle:{color:slowColorOf(key),borderColor:C.paper,borderWidth:1},
      data:pr.map(function(r){
        return [rowEpoch(r)*1000,Number(r.lat_ms)||0,r];})});
  });
  /* 摘要条：每节点慢点次数 + 该节点 p99（当前角色；不受勾选影响） */
  const sum=document.getElementById('slowsum');
  if(sum){
    sum.innerHTML='';
    const byNode={};
    pts.forEach(function(r){
      const n=r.node||'未归属节点';
      (byNode[n]=byNode[n]||[]).push(r);
    });
    const p99={};
    (z.clients||[]).forEach(function(c){
      if(isPipeMode()&&c.role!==slowRole) return;
      const n=c.node||'未归属节点';
      p99[n]=Math.max(p99[n]||0,Number(c.p99_ms)||0);
    });
    Object.keys(byNode).sort().forEach(function(n){
      const chip=el('span','slow-chip');
      const dot=el('span','dot'); dot.style.background=nodeHue(n);
      chip.appendChild(dot);
      chip.appendChild(el('span',null,n+' '+byNode[n].length+' 次'));
      if(p99[n]) chip.appendChild(el('span',null,'p99 '+fmtMs(p99[n])));
      sum.appendChild(chip);
    });
  }
  if(!series.length){try{slowChart.clear();}catch(e){} return;}
  const o=axisCommon();
  o.grid={left:104,right:104,top:30,bottom:44};
  o.tooltip=lfTooltip({trigger:'item',formatter:function(p){
    const r=p.data&&p.data[2];
    if(!r) return '';
    /* R-SLOW-2：时刻走归一 epoch（rowEpoch 优先 ts_epoch），不得读行上
       不存在的 r.t（曾致 tooltip 时刻显示 undefined） */
    return fullts(rowEpoch(r))+'<br>'+String(r.key||'')+
      '<br>'+(r.role?('角色 '+r.role+' · 节点 '+(r.node||'-')+'<br>'):'')+
      '时延 '+Number(r.lat).toFixed(2)+' ms · '+(r.status||'');
  }});
  o.yAxis=[lfAxisValue({name:'ms',scale:true,nameLocation:'end',nameGap:14})];
  /* x 轴与端到端曲线同窗口（run 的 ts_from..ts_to）；缩放独立 */
  o.dataZoom=zoomModel('xonly');
  const sm=z.summary||{};
  let x0=sm.ts_from, x1=sm.ts_to;
  if(x0==null||x1==null){
    const ts=pts.map(function(r){return rowEpoch(r);});
    x0=Math.min.apply(null,ts); x1=Math.max.apply(null,ts);
  }
  o.xAxis=lfAxisTime(xExtent(x0,x1,plotWidthOf(slowChart,
    {left:56,right:104})),'slow');
  o.series=series;
  slowChart.setOption(o,true);
}
/* 数据量与优化：按实测大小/请求数还原各层级，并给出优化倍数。
   glog 参与 map-reduce；ylt 不采集（仅占节点磁盘）。 */
function fmtNum(n){
  if(n==null||n==='') return '-';
  const v=Number(n);
  return isFinite(v)?v.toLocaleString('en-US'):String(n);
}
function renderVolume(){
  const box=document.getElementById('volume'); if(!box) return;
  const note=document.getElementById('volumeNote');
  const z=Z(); const v=z&&z.volume;
  const out=(D.meta&&D.meta.volume_out)||{};
  box.innerHTML='';
  if(!v){
    if(note) note.textContent='本产物不含数据量统计（旧产物）：用 --rebuild-report 重新生成后可见。';
    return;
  }
  if(note) note.textContent='各层级数据量按实测文件大小与请求计数还原；glog 参与 map-reduce，ylt 暂不处理（仅占节点磁盘）。';
  const inp=v.inputs||{},rt=v.rates||{},raw=v.raw||{},mp=v.map||{};
  const red=v.reduce||{};
  const t=el('table'); const thead=el('thead'); const hr=el('tr');
  ['层级','项目','数值','计算口径'].forEach(function(h,i){
    hr.appendChild(el('th',i===2?'n':'',h));});
  thead.appendChild(hr); t.appendChild(thead);
  const tb=el('tbody');
  function row(level,item,val,formula,strong){
    const tr=el('tr');
    tr.appendChild(el('td','',level));
    tr.appendChild(el('td','',item));
    tr.appendChild(el('td','n',val));
    tr.appendChild(el('td','',formula||''));
    if(strong) tr.className='volume-key';
    tb.appendChild(tr);
  }
  function b(level,item,bytes,formula,strong){
    row(level,item,bytes==null?'-':fmtBytes(bytes),formula,strong);
  }
  row('输入','运行时长',String(inp.wall_s)+' s','统计窗口 '+inp.span_s+' s');
  row('输入','节点数',fmtNum(inp.nodes),'');
  row('输入','实例数',fmtNum(inp.clients),
    '写 '+fmtNum(inp.writer_clients)+' · 读 '+fmtNum(inp.reader_clients));
  row('输入','trace 采样率',
    inp.sample_rate==null?'-':String(inp.sample_rate),'');
  row('行数','总请求行',fmtNum(inp.ops),
    '行/秒 ≈ 总请求 ÷ 时长 = '+fmtNum(rt.rows_per_s)+' 行/s');
  row('行数','trace 行',fmtNum(inp.trace_lines),
    '行/秒 ≈ trace 行 ÷ 时长 = '+fmtNum(rt.trace_rows_per_s)+' 行/s');
  b('原始','writer.log',raw.writer_bytes,'实测（map 前原始文件）');
  b('原始','reader.log',raw.reader_bytes,'实测（map 前原始文件）');
  b('原始','glog（C++ SDK）',raw.glog_bytes,
    '实测；参与 map-reduce（tracestats → trace.json）');
  row('原始','ylt（coro_rpc easylog）','未采集',
    '暂不处理，仅占节点磁盘（轮转上限 50MB×3）');
  b('原始','合计',raw.total_bytes,'writer + reader + glog（不含 ylt）',true);
  b('map 后','stats.json · 单实例均值',mp.stats_per_client,
    '时间桶 ≤ '+fmtNum(mp.bucket_cap)+'/档 + 直方图 '+mp.hist_bins+
    ' 箱 + 慢点 Top-'+mp.top_n+' + 异常/离群清单 ≤'+mp.list_cap+'/实例');
  b('map 后','trace.json · 单实例均值',mp.trace_per_client,
    '阶段秒桶 + 慢点 Top-'+mp.trace_top_n+'（含 L1/L2）');
  b('reduce','回传统计总量',red.pulled_bytes,
    'stats '+fmtBytes(mp.stats_bytes)+' + trace '+fmtBytes(mp.trace_bytes)+
    '（原始日志留节点）',true);
  b('输出','data.js',out.data_js,'全量明细（深查用）');
  b('输出','report.html',out.report_html,'自包含单文件（内嵌当前档位）');
  const sd=v.site||{}; const sdn=sd.nodes||[];
  if(sdn.length){
    sdn.forEach(function(x){
      const formula=(x.profiling!=null&&x.pods!=null)
        ?('pod 目录 '+fmtBytes(x.pods)+' + profiling '+fmtBytes(x.profiling)+
          '（du -sb 实测）')
        :'现场目录（du -sb 实测，含全部运行时文件）';
      b('留存',String(x.node),x.total,formula);
    });
    b('留存','合计',sd.total,
      'du -sb 实测（节点侧真实字节；含 .glog.d / summary / exitrc / '+
      'remove 服务日志等全部现场文件）',true);
  }
  const rawT=Number(raw.total_bytes)||0;
  const pulled=Number(red.pulled_bytes)||0;
  const repTotal=(Number(out.data_js)||0)+(Number(out.report_html)||0);
  if(pulled) row('对比','原始 → 回传',
    (rawT/pulled).toFixed(1)+'×',
    fmtBytes(rawT)+' ÷ '+fmtBytes(pulled)+
    '：map 只回传统计产物，原始日志留在节点',true);
  if(repTotal) row('对比','原始 → 报表',
    (rawT/repTotal).toFixed(1)+'×',
    fmtBytes(rawT)+' ÷ ('+fmtBytes(out.data_js)+' + '+
    fmtBytes(out.report_html)+')',true);
  t.appendChild(tb);
  const wrapx=el('div','wrapx'); wrapx.appendChild(t); box.appendChild(wrapx);
}
/* 日志现场：唯一一张「现场日志」表（节点行 + pod 行 + 监控行）。
   展平不折叠；全表 nowrap + .wrapx 水平滑动。节点列只在节点行填写。 */
let errOpen={};          /* 错误与异常：节点级收放（默认折叠实例） */
function renderErrors(){
  const z=Z(); if(!z) return;
  const wrap=document.getElementById('errors');
  wrap.innerHTML='';
  function stCodes(map){
    const ks=Object.keys(map||{});
    if(!ks.length) return '-';
    return ks.sort(function(a,b){return (map[b]||0)-(map[a]||0);})
      .slice(0,3).map(function(k){return k+'×'+map[k];}).join(' / ');
  }
  const cs=(z.clients||[]).slice();
  if(!cs.length){
    wrap.appendChild(el('div','note','无实例数据'));
  }else{
    /* 角色分组（写在前、读在后；basic 无角色则单块） */
    const roles=[];
    cs.forEach(function(c){
      const r=c.role||'';
      if(roles.indexOf(r)<0) roles.push(r);
    });
    const ro={writer:0,reader:1};
    roles.sort(function(a,b){return (ro[a]===undefined?9:ro[a])-
      (ro[b]===undefined?9:ro[b]);});
    roles.forEach(function(role){
      const list=cs.filter(function(c){return (c.role||'')===role;});
      if(!list.length) return;
      if(roles.length>1){
        wrap.appendChild(el('h3','sec',
          role==='writer'?'写路径（writer）':
          (role==='reader'?'读路径（reader）':role)));
      }
      const nodes={};
      list.forEach(function(c){
        const n=c.node||'未归属节点';
        (nodes[n]=nodes[n]||[]).push(c);
      });
      const names=Object.keys(nodes).sort();
      const t=el('table'); const thead=el('thead'); const hr=el('tr');
      ['节点','客户端','总数','失败','状态码'].forEach(function(h,i){
        hr.appendChild(el('th',(i===2||i===3)?'n':'',h));});
      thead.appendChild(hr); t.appendChild(thead);
      const tb=el('tbody');
      names.forEach(function(n){
        const ls=nodes[n].sort(function(a,b){
          return String(a.name).localeCompare(String(b.name));});
        const tot=ls.reduce((a,c)=>a+(Number(c.total)||0),0);
        const fail=ls.reduce((a,c)=>a+(Number(c.fail)||0),0);
        const agg={};
        ls.forEach(function(c){
          Object.keys(c.errors||{}).forEach(function(k){
            agg[k]=(agg[k]||0)+Number(c.errors[k]||0);});
        });
        const okey=role+'|'+n;
        const open=errOpen[okey]===true;   /* 默认折叠：只显示节点汇总行 */
        const trn=el('tr','node-row');
        const tdn=el('td','node-name');
        const tg=el('button','mini err-toggle',open?'▾':'▸');
        tg.title=open?'收起该节点实例':'展开该节点实例';
        tg.onclick=function(){errOpen[okey]=!open;renderErrors();};
        tdn.appendChild(tg);
        tdn.appendChild(el('span',null,' '+n));
        trn.appendChild(tdn);
        trn.appendChild(el('td','','（'+ls.length+' 实例）'));
        trn.appendChild(el('td','n',String(tot)));
        trn.appendChild(el('td','n',String(fail)));
        trn.appendChild(el('td','mono',stCodes(agg)));
        tb.appendChild(trn);
        if(open){
          ls.forEach(function(c){
            const tr=el('tr','client-row');
            tr.appendChild(el('td','', ''));
            tr.appendChild(el('td','mono',c.name));
            tr.appendChild(el('td','n',String(c.total)));
            tr.appendChild(el('td','n',String(c.fail)));
            tr.appendChild(el('td','mono',stCodes(c.errors)));
            tb.appendChild(tr);
          });
        }
      });
      t.appendChild(tb);
      const wrapx=el('div','wrapx'); wrapx.appendChild(t); wrap.appendChild(wrapx);
    });
  }
  if(z.error_excerpt){
    const p=el('pre','term'); p.textContent=z.error_excerpt;
    wrap.appendChild(el('div','note','首错误现场（±20 行，状态 '+
      (z.error_excerpt_status||'?')+'）：'));
    wrap.appendChild(p);
  }
  if(z.anomalies&&z.anomalies.length){
    const ul=el('ul','anml');
    z.anomalies.slice(0,100).forEach(function(a){
      const who=[a.role,a.node,a.client].filter(Boolean).join(' · ');
      ul.appendChild(el('li',null, fullts(a.t)+'  '+
        (a.kind==='error_burst'
          ?('错误突发：'+a.fail+'/'+a.count+' 失败')
          :('时延突增：均值 '+a.avg_ms+'ms / '+a.count+' 次'))+
        (who?'（'+who+'）':'')));
    });
    wrap.appendChild(el('div','note','异常窗口（主图上以强调色竖线标注）：'));
    wrap.appendChild(ul);
  }
}

function renderLogSite(){
  const box=document.getElementById('logsite'); if(!box) return;
  box.innerHTML='';
  const rows=(D.logsite&&D.logsite.length)?D.logsite:
    (((typeof FULL!=='undefined')&&FULL&&FULL.logsite)||[]);
  if(!rows.length){box.appendChild(el('div','note','无日志产物'));return;}
  const t=el('table','logsite-table');
  const thead=el('thead'); const hr=el('tr');
  ['节点','角色','pod','日志属性','日志路径'].forEach(function(h){
    hr.appendChild(el('th',null,h));});
  thead.appendChild(hr); t.appendChild(thead);
  const tb=el('tbody');
  rows.forEach(function(r){
    const nodeRow=r.tier==='node';
    const tr=el('tr', nodeRow?'node-row':(
      r.tier==='monitor'?'monitor-row':'pod-row'));
    tr.appendChild(el('td','mono', nodeRow?String(r.node||'-'):''));
    tr.appendChild(el('td','', nodeRow?'':String(r.role||'')));
    tr.appendChild(el('td','mono', nodeRow?'':String(r.pod||'-')));
    let attr='-';
    if(nodeRow){
      attr=String(Number(r.files)||0)+' 个子项 · '+fmtBytes(r.bytes||0);
    }else if(r.tier==='monitor'&&!(r.files||r.bytes||r.dir)){
      attr='-';                       /* 未开监控：pod/属性/路径全为 - */
    }else if(r.bytes||r.files){
      attr=fmtBytes(r.bytes||0);
    }
    tr.appendChild(el('td','n',attr));
    tr.appendChild(el('td','mono', r.dir?String(r.dir):'-'));
    tb.appendChild(tr);
  });
  t.appendChild(tb);
  const wrapx=el('div','wrapx'); wrapx.appendChild(t);
  box.appendChild(wrapx);
}

/* ---- 对象树：总体 -> 节点 -> 客户端，每行一个复选框 -------------- */
let treeOpen={};
function objTreeModel(noRoleFilter){
  const all=objAll();
  const roles=[],nodes=[],clients=[];
  Object.keys(all).forEach(function(k){
    const o=all[k]; const kind=o.kind||'client';
    if(kind==='role') roles.push(k);
    else if(kind==='node') nodes.push(k);
    else clients.push(k);
  });
  /* pipeline：节点/实例按当前角色过滤（阶段树 noRoleFilter=true 保持原样） */
  if(isPipeMode()&&!noRoleFilter){
    const keep=nodes.filter(function(k){return all[k].role===curRole;});
    nodes.length=0; keep.forEach(function(k){nodes.push(k);});
    const kc=clients.filter(function(k){return all[k].role===curRole;});
    clients.length=0; kc.forEach(function(k){clients.push(k);});
  }
  /* 参数组筛选：只保留当前 combo 的客户端（对象键是 'client:<日志名>'） */
  const _cf=clients.filter(function(k){return clientInCombo(k);});
  clients.length=0; _cf.forEach(function(k){clients.push(k);});
  const ord={'all':0,'writer':1,'reader':2};
  roles.sort(function(a,b){
    return (ord[a]===undefined?9:ord[a])-(ord[b]===undefined?9:ord[b]);});
  nodes.sort(function(a,b){
    return String(objLabel(a)).localeCompare(String(objLabel(b)));});
  clients.sort(function(a,b){
    return String(objLabel(a)).localeCompare(String(objLabel(b)));});
  const byNode={};
  clients.forEach(function(k){
    const n=(all[k].node||'未归属节点');
    (byNode[n]=byNode[n]||[]).push(k);
  });
  return {all:all,roles:roles,nodes:nodes,clients:clients,byNode:byNode};
}
function toggleObj(key){
  if(!key) return;
  const i=selObjs.indexOf(key);
  if(i>=0) selObjs.splice(i,1); else selObjs.push(key);
  ensureSel();
  renderObjTree(); renderControls(); buildChart();
}
function setMany(keys,on){
  keys.forEach(function(k){
    const i=selObjs.indexOf(k);
    if(on&&i<0) selObjs.push(k);
    if(!on&&i>=0) selObjs.splice(i,1);
  });
  ensureSel();
  renderObjTree(); renderControls(); buildChart();
}
function miniBtn(label,keys,title,on){
  const b=el('button','mini',label);
  b.title=title;
  b.onclick=function(){setMany(keys,on);};
  return b;
}
function treeGroupStrip(title,keys){
  const g=el('div','tree-group');
  g.appendChild(el('span','tree-group-title',title));
  g.appendChild(miniBtn('全选',keys,'选中本级全部对象',true));
  g.appendChild(miniBtn('清空',keys,'取消本级全部对象',false));
  return g;
}
function treeRow(key,label,depth,opts){
  opts=opts||{};
  const on=selObjs.indexOf(key)>=0;
  const row=el('div','tree-row'+(on?' on':''));
  row.style.paddingLeft=(depth>1?36:0)+'px';
  if(opts.lead) row.appendChild(opts.lead);
  const b=el('button','tree-check');
  b.setAttribute('data-key',key);
  b.appendChild(el('span','tree-box',on?'✓':''));
  b.appendChild(el('span','tree-name',label));
  b.title=label;
  b.onclick=function(){toggleObj(key);};
  row.appendChild(b);
  (opts.minis||[]).forEach(function(m){row.appendChild(m);});
  if(opts.note) row.appendChild(el('span','tree-note',opts.note));
  return row;
}
function renderObjTree(){
  const box=document.getElementById('objTree'); if(!box) return;
  ensureSel();
  box.innerHTML='';
  const m=objTreeModel();
  const isBasic=isBasicMode();
  if(isBasic){
    /* basic：顶层就是「总览」（全部节点/实例的平均），只此一行、无角色分组 */
    box.appendChild(treeRow('all',objLabel('all'),1));
  }else if(isPipeMode()){
    /* pipeline：角色单选（互斥必选，默认 reader）+ 总计开关（默认开） */
    const g1=el('div','tree-group role-group');
    g1.appendChild(el('span','tree-group-title','角色'));
    ['reader','writer'].forEach(function(r){
      if(!m.all[r]) return;
      const b=el('button','role-btn'+(curRole===r?' on':''),r);
      b.title='切换查看角色（互斥）：当前 '+r;
      b.onclick=function(){
        if(curRole===r) return;
        /* 切换角色不丢弃节点/实例选择：按角色记忆，切回自动恢复 */
        selByRole[curRole]=selObjs.filter(function(k){
          const o=objAll()[k];
          return o&&o.kind!=='role';
        });
        curRole=r;
        const keep=(selByRole[curRole]||[]).slice();
        selObjs=keep.concat(totalOn?[curRole]:[]);
        renderAll();
      };
      g1.appendChild(b);
    });
    box.appendChild(g1);
    /* 总计：独立一行（只有选中/不选两种状态） */
    const g2t=el('div','tree-group total-group');
    g2t.appendChild(el('span','tree-group-title','总计'));
    const tb=el('button','total-btn'+(totalOn?' on':''),'总计');
    tb.title=totalOn?'当前角色全部实例的平均（点击隐藏）':
      '显示当前角色全部实例的平均';
    tb.onclick=function(){totalOn=!totalOn;renderAll();};
    g2t.appendChild(tb);
    box.appendChild(g2t);
  }else{
    const g1=treeGroupStrip('总体与角色',m.roles);
    m.roles.forEach(function(k){
      g1.appendChild(treeRow(k,objLabel(k),1));
    });
    box.appendChild(g1);
  }
  /* 单元报表（单 size×combo）里只有一个实例时，节点/实例与「总览」
     完全等价，隐藏该组避免三重冗余。多实例场景保持原样。 */
  if(m.clients.length<=1&&m.nodes.length<=1) return;
  const g2=el('div','tree-group node-group');
  const head=el('div','node-group-head');
  head.appendChild(el('span','tree-group-title','节点与实例'));
  head.appendChild(el('span','objtree-count','已选 '+selObjs.length+' 项'));
  g2.appendChild(head);
  const acts=el('div','node-group-acts');
  /* 全选/清空节点、全选/清空实例（按当前是否已全选动态切换）、全部清空 */
  function kindKeys(kind){
    const all=objAll(); const keys=[];
    Object.keys(all).forEach(function(k){
      const o=all[k];
      if(o.kind!==kind) return;
      if(isPipeMode()&&o.role!==curRole) return;
      keys.push(k);
    });
    return keys;
  }
  function selKind(kind,on){setMany(kindKeys(kind),on);}
  function kindAll(kind){
    const keys=kindKeys(kind);
    return keys.length>0&&keys.every(function(k){
      return selObjs.indexOf(k)>=0;});
  }
  function clearAll(){
    selObjs=selObjs.filter(function(k){
      const o=objAll()[k];
      return !!o&&o.kind==='role';
    });
    /* 全部清空后自动开启总计/总览，避免落到空图 */
    if(isPipeMode()){
      totalOn=true;
      if(selObjs.indexOf(curRole)<0) selObjs.push(curRole);
    }else if(objAll()['all']&&selObjs.indexOf('all')<0){
      selObjs.push('all');
    }
    renderAll();
  }
  function actBtn(label,title,fn){
    const b=el('button','mini',label);
    b.title=title; b.onclick=fn;
    return b;
  }
  const ndAll=kindAll('node'), clAll=kindAll('client');
  acts.appendChild(actBtn(ndAll?'清空节点':'全选节点',
    ndAll?'取消当前范围内全部节点的选择':'选中当前范围内全部节点',
    function(){selKind('node',!ndAll);}));
  acts.appendChild(actBtn(clAll?'清空实例':'全选实例',
    clAll?'取消当前范围内全部实例的选择':'选中当前范围内全部实例',
    function(){selKind('client',!clAll);}));
  acts.appendChild(actBtn('全部清空','清空节点与实例，并自动开启总计',
    clearAll));
  g2.appendChild(acts);
  const list=el('div','tree-list');
  m.nodes.forEach(function(k){
    const nodeName=String((m.all[k]&&m.all[k].node)||k.replace(/^node:/,''));
    const kids=m.byNode[nodeName]||[];
    const open=!!treeOpen[k];
    let caret=null;
    if(kids.length){
      caret=el('button','tree-caret',open?'▾':'▸');
      caret.title=open?'收起该节点的客户端':'展开该节点的客户端';
      caret.onclick=function(){treeOpen[k]=!treeOpen[k];renderObjTree();};
    }
    list.appendChild(treeRow(k,objLabel(k),1,{
      lead:caret,
      minis:kids.length?[
        miniBtn('全选',kids,'选中该节点下的全部实例',true),
        miniBtn('清空',kids,'取消该节点下的全部实例',false)]:[],
      note:kids.length?(kids.length+' 实例'):''}));
    if(open&&kids.length){
      kids.forEach(function(ck){
        list.appendChild(treeRow(ck,objLabel(ck),2));
      });
    }
  });
  if(!m.nodes.length){
    list.appendChild(el('div','tree-empty','该数据大小没有节点聚合对象'));
  }
  g2.appendChild(list);
  box.appendChild(g2);
}

function renderControls(){
  const box=document.getElementById('tierBtns'); if(!box) return;
  ensureSel();
  box.innerHTML='';
  const active=currentTier();
  /* 自适应是默认档：按钮固定放最左 */
  const tlist=tierList().slice().sort(function(a,b){
    if(a.key==='cnt') return -1;
    if(b.key==='cnt') return 1;
    return 0;
  });
  tlist.forEach(function(t){
    const ok=selObjs.every(function(k){return !!colsOf(k,t.key);});
    const b=el('button', (t.key===active&&ok)?'on':'', t.label);
    b.disabled=!ok;
    if(!ok) b.title='所选对象无该粒度';
    else if(t.key==='cnt'&&t.hint) b.title='自适应：'+t.hint;
    b.onclick=function(){curTier=t.key;renderControls();buildChart();};
    box.appendChild(b);
  });
  const ob=document.getElementById('outlierBtn');
  if(ob){
    const z=Z(); const hasOut=!!(z&&(z.outliers||[]).length);
    /* 默认选中（显示离群点）；点击后变灰 = 不显示 */
    ob.className=outlierOn?'on':'off';
    ob.textContent='离群点';
    ob.title=hasOut
      ?('离群点 = 单请求 > 客户端 p99×10（共 '+(z.outliers||[]).length+
         ' 个）；桶统计已剔除，此处仅控制散点显隐（默认选中，点击变灰即不显示）')
      :'本次没有离群请求（单请求 > 客户端 p99×10）';
    ob.disabled=!hasOut;
  }
}
function renderThemeToggle(){
  const btn=document.getElementById('themeToggle'); if(!btn) return;
  btn.onclick=function(){
    const cur=themePref();
    const next=cur==='auto'?'dark':(cur==='dark'?'light':'auto');
    applyTheme(next);
    renderAll();               /* token 翻转后整页重渲染 */
  };
}
function renderAll(){
  const z=Z(); if(!z) return;
  const ss=document.getElementById('sizeSel');
  if(ss) ss.style.display=Object.keys(S).length>1?'inline-block':'none';
  const sl=document.getElementById('sizeSelLabel');
  if(sl) sl.style.display=Object.keys(S).length>1?'inline-block':'none';
  safe('summary',renderSummary);
  safe('combo-bar',function(){
    renderComboBar();
    /* 切参数组会隐藏不在该组的客户端：若它们正被选中，回落「总体」以免空图 */
    if(comboFilter){
      const keep=selObjs.filter(function(k){
        return k.indexOf('client:')!==0||clientInCombo(k);});
      selObjs=keep.length?keep:['all'];
    }
  });
  safe('object-tree',function(){
    renderObjTree(); renderControls();});
  safe('lat-sel',function(){renderLatSel(); renderGlobSel();});
  safe('latency-chart',buildChart);
  safe('histogram',renderHist);
  safe('slow-strip',renderSlowStrip);
  safe('logsite',renderLogSite);
  safe('volume',renderVolume);
  safe('errors',renderErrors);
  safe('trace',renderTrace);
  safe('theme-toggle',renderThemeToggle);
}
let monCharts={};
function _monChart(id, series, yname){
  const box=document.getElementById(id); if(!box||!series.length) return false;
  let c=monCharts[id]; if(!c) c=monCharts[id]=echarts.init(box);
  bindHoverGuards(c);
  const o=axisCommon();
  o.grid={left:104,right:104,top:26,bottom:42};
  o.yAxis=[lfAxisValue({name:yname,scale:true,nameLocation:'end',nameGap:14})];
  o.dataZoom=zoomModel('xonly');
  // x 轴显式范围（value 轴默认从 0 开始会把数据挤到最右端）
  let t0=null,t1=null;
  series.forEach(function(sr){
    (sr.data||[]).forEach(function(d){
      const t=d[0]/1000;
      if(t0===null||t<t0)t0=t;
      if(t1===null||t>t1)t1=t;});
  });
  if(t0!==null) o.xAxis=lfAxisTime(xExtent(t0,t1,
    plotWidthOf(c,{left:104,right:104})),id);
  o.series=series;
  c.setOption(o, true);
  return true;
}

function renderMonitors(){
  const M=D.monitors||{}; const card=document.getElementById('monitorcard');
  if(!card) return;
  const notes=M.notes||[];
  const has=(M.mem_host&&M.mem_host.length)||(M.mem_proc&&M.mem_proc.length)||
    (M.urma&&M.urma.length);
  /* 没有可用序列但有产物/失败原因：渲染占位卡列出原因，不静默消失 */
  if(!has&&!notes.length){card.style.display='none';return;}
  card.style.display='';
  const g=document.getElementById('moncards'); g.innerHTML='';
  if(notes.length){
    const nb=el('div','note');
    notes.forEach(function(s){nb.appendChild(el('div',null,'• '+s));});
    g.appendChild(nb);
  }
  if(!has){
    const suf=document.getElementById('monTitleSuffix');
    if(suf) suf.textContent='';
    ['growthTitle','growth','memchart','procchart','urmachart','monNote']
      .forEach(function(id){
        const b=document.getElementById(id); if(b) b.style.display='none';});
    return;
  }
  const sm=M.summary||{};
  /* R-MON-4 部分缺失：只有 mem 无 urma（或反之）时，标题/图注同步去掉
     对应分句、空图占位隐藏——不留下对不存在内容的说明与空洞。 */
  const hasUrma=!!(M.urma&&M.urma.length);
  const hasMem=!!((M.mem_host&&M.mem_host.length)||
    (M.mem_proc&&M.mem_proc.length));
  const suf=document.getElementById('monTitleSuffix');
  if(suf) suf.textContent=hasMem&&hasUrma?'（内存 / URMA 端口）':
    (hasUrma?'（URMA 端口）':(hasMem?'（内存）':''));
  const unote=document.getElementById('monUrmaNote');
  if(unote&&!hasUrma) unote.textContent='';
  const ubox=document.getElementById('urmachart');
  if(ubox) ubox.style.display=hasUrma?'':'none';
  if(sm.peak_mem_mb) Object.keys(sm.peak_mem_mb).forEach(function(n){
    g.appendChild(kv('峰值内存 · '+n, sm.peak_mem_mb[n], 'MB'));});
  if(sm.worst_proc){
    const w=sm.worst_proc;
    const k=kv('RSS 最大增幅', w.label||'-', '');
    if(w.leak_suspect){
      const t=el('span','tag hero',' 疑似泄露');
      k.querySelector('.v').appendChild(t);
    }
    k.appendChild(el('div','note',
      (w.growth_mb==null?'-':w.growth_mb+' MB')+' / '+
      (w.growth_pct==null?'-':w.growth_pct+'%')));
    g.appendChild(k);
  }
  if(sm.peak_urma&&sm.peak_urma.value!=null)
    g.appendChild(kv('端口峰值带宽', sm.peak_urma.label||'-',
      sm.peak_urma.value.toFixed(3)+' GiB/s'));
  renderGrowth(sm.proc_growth||[]);
  const hs=[]; let hi=0;
  (M.mem_host||[]).forEach(function(h){
    h.columns.forEach(function(c){
      hs.push({name:h.node+'/'+c,type:'line',showSymbol:false,
        lineStyle:{width:1.4,color:lfColor(hi++)},
        data:h.t.map(function(t,j){return [t*1000,h.series[c][j]];})});
    });});
  safe('mem-chart',function(){_monChart('memchart', hs, 'MB');});
  const ps=(M.mem_proc||[]).slice(0,8).map(function(p,i){
    return {name:p.node+'/'+p.pid+(p.comm?' '+p.comm:''),type:'line',
      showSymbol:false,lineStyle:{width:1.4,color:lfColor(i)},
      data:p.t.map(function(t,j){return [t*1000,p.rss_mb[j]];})};});
  safe('proc-chart',function(){_monChart('procchart', ps, 'RSS MB');});
  const us=[]; let ui=0;
  (M.urma||[]).forEach(function(u){
    const tag=u.node+' ubpu='+u.ubpu+' port='+u.port;
    us.push({name:tag+' RX',type:'line',showSymbol:false,
      lineStyle:{width:1.5,color:lfColor(ui)},
      data:u.t.map(function(t,i){return [t*1000,u.rx[i]];})});
    us.push({name:tag+' TX',type:'line',showSymbol:false,
      lineStyle:{width:1.1,color:lfColor(ui+1),type:'dashed'},
      data:u.t.map(function(t,i){return [t*1000,u.tx[i]];})});
    ui+=2;});
  safe('urma-chart',function(){_monChart('urmachart', us, 'GiB/s');});
}
function renderGrowth(rows){
  const box=document.getElementById('growth'); if(!box) return;
  box.innerHTML='';
  if(!rows.length){box.style.display='none';return;}
  box.style.display='';
  const max=Math.max.apply(null, rows.map(function(r){
    return Math.abs(r.growth_mb||0);}).concat([1]));
  rows.slice(0,10).forEach(function(r){
    const row=el('div','grow-row');
    row.appendChild(el('span','grow-name mono',r.label||''));
    const mid=el('span','grow-track');
    const bar=el('span','grow-bar');
    const w=Math.max(1,Math.abs(r.growth_mb||0)/max*50);
    bar.style.width=w+'%';
    if((r.growth_mb||0)>=0) bar.style.left='50%'; else bar.style.right='50%';
    if(r.leak_suspect) bar.style.background=C.hero;
    mid.appendChild(bar); row.appendChild(mid);
    row.appendChild(el('span','grow-val',
      ((r.growth_mb||0)>0?'+':'')+(r.growth_mb||0)+' MB'));
    box.appendChild(row);
  });
}

function mount(){
  const sizeSel=document.getElementById('sizeSel');
  if(sizeSel){
    Object.keys(S).forEach(function(k){
      const o=el('option',null,k); o.value=k; sizeSel.appendChild(o);});
    sizeSel.value=curSize;
    sizeSel.onchange=function(){curSize=sizeSel.value;selObjs=['all'];
      curTier=''; renderAll();};
  }
  const ob=document.getElementById('outlierBtn');
  if(ob) ob.onclick=function(){
    outlierOn=!outlierOn; renderControls(); buildChart();};
  const rb=document.getElementById('resetBtn');
  if(rb) rb.onclick=function(){resetViews();};
  renderAll(); safe('monitors',renderMonitors);
  safe('chart-interactions',function(){
    bindInteract(chart,MAIN_GRID);
    linkZoom(chart,qpsChart);
  });
  safe('legend',function(){
    if(chart&&chart.on){
      chart.on('legendselectchanged',function(ev){
        if(selObjs.length<=1) return;
        const nm=ev.name||'';
        // 多对象时 series 名 = 对象名 + 指标，图例点击切换该对象整套
        const hit=selObjs.find(function(k){
          return nm.indexOf(objLabel(k))===0;});
        if(hit) toggleObj(hit);
      });
    }
  });
  safe('x-pan',function(){bindXPan();});
  const trb=document.getElementById('tResetBtn');
  if(trb) trb.onclick=function(){
    try{
      tChart.dispatchAction({type:'dataZoom',dataZoomIndex:0,start:0,end:100});
      tChart.dispatchAction({type:'dataZoom',dataZoomIndex:1,start:0,end:100});
    }catch(e){}
    safe('stage-yscale',function(){
      tYState.domain=tYDomainOf(); tSetZoomP(0);});
  };
  safe('y-scale',function(){
    bindYScale();
    if(chart){
      try{
        chart.on('finished',function(){
          safe('end-labels',function(){layoutEndLabels(chart);});});
        chart.on('dataZoom',function(){
          safe('x-ticks',function(){syncZoomSpan(chart,'main');});
          safe('x-pan',function(){syncXPan();});
          safe('end-labels',function(){layoutEndLabels(chart);});
          safe('y-scale',function(){syncYScale();});});
      }catch(e){}
    }
  });
  window.addEventListener('resize',function(){
    if(chart)chart.resize();
    if(qpsChart)qpsChart.resize();
    if(tChart)tChart.resize();
    if(tdetailChart)tdetailChart.resize();
    Object.keys(monCharts).forEach(function(k){monCharts[k].resize();});
    safe('x-ticks',function(){
      try{
        const zs=zoomState('main');
        if(chart&&zs.max>zs.min){
          const iv=tickIntervalFor(zs.max-zs.min,
            plotWidthOf(chart,MAIN_GRID));
          if(iv!==zs.iv){
            zs.iv=iv;
            chart.setOption({xAxis:{interval:iv}});
          }
        }
      }catch(e){}
    });
    safe('x-pan',function(){syncXPan();});
    safe('y-scale',function(){syncYScale();});
  });
}
/* ---------------- SDK 全链路（C++ trace）：分级时延 / 阶段曲线 / 阶段树 ---------------- */
const TREAD=[["total_us",0,""],["query_us",1,"total_us"],
 ["select_us",1,"total_us"],["local_endpoints_us",2,"select_us"],
 ["select_replica_us",2,"select_us"],["alloc_us",1,"total_us"],
 ["read_us",1,"total_us"],["read_a_us",2,"read_us"],
 ["read_transfer_window_us",2,"read_us"],["read_b_us",2,"read_us"],
 ["offload_rpc_us",2,"read_us"],["transfer_data_us",2,"read_us"],
 ["release_buffer_us",2,"read_us"],["read_overhead_us",2,"read_us"],
 ["residual",1,"total_us"]];
const TWRITE=[["total_us",0,""],["prep_total_us",1,"total_us"],
 ["prep_buf_alloc_us",2,"prep_total_us"],["prep_memcpy_us",2,"prep_total_us"],
 ["prep_split_us",2,"prep_total_us"],["checksum_us",1,"total_us"],
 ["prepare_us",1,"total_us"],["query_us",1,"total_us"],
 ["put_start_us",1,"total_us"],["memwrite_total_us",1,"total_us"],
 ["memwrite_submit_us",2,"memwrite_total_us"],
 ["memwrite_queue_us",2,"memwrite_total_us"],
 ["memwrite_urma_us",2,"memwrite_total_us"],
 ["memwrite_teardown_us",2,"memwrite_total_us"],
 ["admit_us",1,"total_us"],["queue_prep_us",1,"total_us"],
 ["wire_span_us",1,"total_us"],["propagate_us",1,"total_us"],
 ["put_end_us",1,"total_us"],["finalize_us",1,"total_us"],
 ["cache_invalidate_us",1,"total_us"],["revoke_us",1,"total_us"],
 ["residual",1,"total_us"]];
function tTree(g){return g==='write'?TWRITE:TREAD;}
function traceSizes(){return Object.keys(S).filter(function(k){
  return S[k]&&S[k].trace;});}
let tGroup='',tOp='',tChart=null;
let tLevelMetric='avg',tStageSel=['total_us'];
let tCurveMetric='avg';          /* 分级时延曲线指标：avg / max */
let tObjSel=[];                  /* 曲线对象多选（trace objects，当前 group 作用域） */
let blockMenuOpen=false, blockMenuBound=false;   /* 块图对象弹层状态 */
let tObjInit=null;               /* 当前读写组是否已初始化默认选择 */
let tBlockObj='';                /* 分级时延块图对象（单选） */
let tStageTier='cnt';            /* 粒度：1 秒/10 秒/1 分钟/自适应(默认) */
const T_METRIC_OPTS=[['min','最小'],['avg','平均'],['p50','p50'],['p99','p99'],
  ['p999','p999'],['p9999','p9999'],['max','最大']];
const tExpand={};
let tFocus={};        /* 分级时延焦点集合（可多选） */
let tExpandAll=false; /* 展开全部（不聚焦时） */
let tSel='total_us';  /* 阶段树当前选中节点 */
const T_METRICS=[['min','最小'],['avg','平均'],['p50','p50'],['p99','p99'],
  ['p999','p999'],['p9999','p9999'],['max','最大']];
function tMetricName(k){
  const m=T_METRICS.find(function(x){return x[0]===k;});
  return m?m[1]:k;}
function tMetricOf(st,metric){
  if(!st) return null;
  if(metric==='p9999'&&(st.n||0)<10000) return null;
  const v=st[metric];
  return (v==null)?null:v;
}
function traceOf(){
  const keys=traceSizes(); if(!keys.length) return null;
  /* 数据大小由最上方主选择器统一控制（trace 卡内不再有独立 size 选择器） */
  if(curSize&&S[curSize]&&S[curSize].trace) return S[curSize].trace;
  return S[keys[0]].trace;
}
function tGroupsList(){const t=traceOf();return t?Object.keys(t.groups||{}):[];}
function tPick(){
  const t=traceOf(); if(!t) return null;
  const gs=tGroupsList(); if(!gs.length) return null;
  if(tGroup===''||gs.indexOf(tGroup)<0) tGroup=gs[0];
  const ops=Object.keys((t.groups[tGroup]||{}).ops||{});
  if(!ops.length) return null;
  if(tOp===''||ops.indexOf(tOp)<0) tOp=ops[0];
  return {g:tGroup,op:tOp,data:t.groups[tGroup].ops[tOp]};
}
/* ---- trace 对象模型（总计/节点/实例，当前读写组作用域） ---- */
const STAGE_PALETTE_L=['#3B6FB6','#C77C2B','#3E7D4F','#A64B5B','#6B5BA7',
  '#2F7F7C','#8C6D31','#A04B87','#4F6B2F','#B05A2E'];
const STAGE_PALETTE_D=['#7EA7E8','#E0A968','#7CBF8C','#E08C9B','#B3A5E8',
  '#6FC5C0','#D8B36A','#DFA3CF','#A9C98A','#E6996E'];
function stageColor(i){
  const p=LF_DARK?STAGE_PALETTE_D:STAGE_PALETTE_L;
  return p[Math.abs(Number(i)||0)%p.length];
}
function tObjAll(){
  const z=Z();
  const e=(z&&z.trace&&z.trace.objects)||{};
  const f=(FULL&&FULL.sizes&&FULL.sizes[curSize]&&
           FULL.sizes[curSize].trace&&FULL.sizes[curSize].trace.objects)||{};
  const out={};
  Object.keys(e).forEach(function(k){out[k]=e[k];});
  Object.keys(f).forEach(function(k){out[k]=f[k];});
  return out;
}
function tRoleKey(){return 'role:'+tGroup;}
function tObjKeys(){
  const all=tObjAll(); const g=tGroup;
  const roles=[],nodes=[],clients=[];
  Object.keys(all).forEach(function(k){
    const o=all[k]; if(!o||o.role!==g) return;
    if(o.kind==='role') roles.push(k);
    else if(o.kind==='node') nodes.push(k);
    else clients.push(k);
  });
  nodes.sort(function(a,b){
    return String(all[a].node||'').localeCompare(String(all[b].node||''));
  });
  clients.sort(function(a,b){
    const na=String(all[a].node||''), nb=String(all[b].node||'');
    if(na!==nb) return na.localeCompare(nb);
    return String(all[a].label||'').localeCompare(String(all[b].label||''));
  });
  return roles.concat(nodes, clients);
}
function traceObjLabel(key){
  const o=tObjAll()[key]||{};
  if(o.kind==='role') return '总计';
  return String(o.label||key);
}
function traceNodeColor(name){
  const all=tObjAll(); const names=[];
  Object.keys(all).forEach(function(k){
    const o=all[k];
    if(o&&o.role===tGroup&&o.kind==='node'&&names.indexOf(o.node)<0)
      names.push(o.node);
  });
  names.sort();
  const hues=LF_DARK?NODE_HUES_D:NODE_HUES_L;
  const i=Math.max(0,names.indexOf(String(name)));
  return hues[i%hues.length];
}
function traceObjColor(key){
  const o=tObjAll()[key]; if(!o) return C.fg;
  if(o.kind==='role') return latColor('p99');          /* 总计=灰阶 */
  if(o.kind==='node') return traceNodeColor(o.node);
  const base=traceNodeColor(o.node||'');               /* 实例=节点色阶 */
  const nm=String(o.label||key); let h=0;
  for(let i=0;i<nm.length;i++)h=(h*31+nm.charCodeAt(i))>>>0;
  return mixHex(base, LF_DARK?'#101010':'#FFFFFF', 0.18+0.14*(h%4));
}
function tObjPick(key){
  const o=tObjAll()[key]; if(!o) return null;
  const g=(o.groups||{})[tGroup]; if(!g) return null;
  return (g.ops||{})[tOp]||null;
}
function tStageName(k){
  return k==='total_us'?'total':(k==='residual'?'残差':k);
}
function tStageKeys(){
  const cur=tPick(); if(!cur) return ['total_us'];
  const keys=['total_us'];
  tTree(cur.g).forEach(function(x){
    if(x[1]===1&&keys.indexOf(x[0])<0)keys.push(x[0]);});
  if(cur.data&&cur.data.residual) keys.push('residual');
  return keys;
}
function ensureTraceSel(){
  /* 每组首次进入给默认选择（总计 + total）；此后允许空选（清空不再回填） */
  if(tGroup&&tObjInit!==tGroup){
    tObjInit=tGroup;
    tObjSel=[tRoleKey()];
    tStageSel=['total_us'];
  }
  const keys=tObjKeys();
  if(!keys.length) return;
  tObjSel=tObjSel.filter(function(k){return keys.indexOf(k)>=0;});
  if(keys.indexOf(tBlockObj)<0) tBlockObj=keys[0];
  const stages=tStageKeys();
  tStageSel=tStageSel.filter(function(k){return stages.indexOf(k)>=0;});
  /* 对象与阶段可自由组合（不再互相收敛） */
}
/* 通用：按序列名集合压暗/恢复（用于曲线标签聚焦） */
function focusSeriesOn(ch,names){
  let sers=[];
  try{ sers=(ch.getOption().series)||[]; }catch(e){ return; }
  const partials=sers.map(function(sr){
    const hit=!names||names.indexOf(String(sr.name||''))>=0;
    const v=hit?1:0.15;
    return {lineStyle:{opacity:v},itemStyle:{opacity:v}};
  });
  try{ ch.setOption({series:partials}); }catch(e){}
}
function tStat(key,cur){
  if(key==='residual') return cur.data.residual||{};
  return (cur.data.fields||{})[key]||{};
}
function tParentKey(key,cur){
  const it=tTree(cur.g).find(function(x){return x[0]===key;});
  return (it&&it[2])||'total_us';
}
function tChildrenOf(key,cur){
  return tTree(cur.g).filter(function(x){return x[1]>0&&x[2]===key;})
    .map(function(x){return {key:x[0],
      label:(x[0]==='residual'?'残差':x[0]), st:tStat(x[0],cur)};});
}
function fmtUs(v){v=Number(v)||0;
  return v>=1000?(v/1000).toFixed(2)+' ms':v.toFixed(1)+' µs';}
/* 色深 = 占父项百分比（连续插值）：占比越大，颜色越深 */
function hex2rgb(h){
  h=String(h||'').replace('#','');
  if(h.length===3) h=h[0]+h[0]+h[1]+h[1]+h[2]+h[2];
  const v=parseInt(h,16);
  if(isNaN(v)) return [128,128,128];
  return [(v>>16)&255,(v>>8)&255,v&255];
}
function rgb2hex(c){
  return '#'+c.map(function(v){
    v=Math.max(0,Math.min(255,Math.round(v)));
    return (v<16?'0':'')+v.toString(16);}).join('');
}
function mixHex(a,b,t){
  const A=hex2rgb(a),B=hex2rgb(b);
  return rgb2hex([0,1,2].map(function(i){return A[i]+(B[i]-A[i])*t;}));
}
function lumOf(hex){
  const c=hex2rgb(hex);
  return (0.299*c[0]+0.587*c[1]+0.114*c[2])/255;
}
function lvlStyle(share,isResidual){
  const L=C.blocks||[];
  if(!L.length) return {background:C.faint,color:C.fg};
  if(isResidual&&share>0.05) return {background:C.hero,color:'#FFFFFF'};
  const s=Math.max(0,Math.min(1,share||0));
  const pos=(1-s)*(L.length-1);
  const i=Math.floor(pos), f=pos-i;
  const bg=mixHex(L[i],L[Math.min(L.length-1,i+1)],f);
  return {background:bg,color:(lumOf(bg)>0.55)?'#1F1E1C':'#FFFFFF'};
}
function renderLevelMetric(){
  const box=document.getElementById('lvlMetric'); if(!box) return;
  box.innerHTML='';
  T_METRICS.forEach(function(m){
    const b=el('button',m[0]===tLevelMetric?'on':'',m[1]);
    b.title=(m[0]==='p9999')
      ? 'p9999 需要至少 10000 个样本，否则显示为空'
      : ('按 '+m[1]+' 计算块宽、占父百分比与色深');
    b.onclick=function(){tLevelMetric=m[0];renderTrace();};
    box.appendChild(b);
  });
}
function focusLit(cur){
  const lit={};
  Object.keys(tFocus).forEach(function(k){
    if(!tFocus[k]) return;
    lit[k]=true;
    let p=tParentKey(k,cur);
    while(p&&p!==k&&!lit[p]){lit[p]=true;p=tParentKey(p,cur);}
    (function mark(key){
      tChildrenOf(key,cur).forEach(function(c){
        if(lit[c.key]) return;
        lit[c.key]=true; mark(c.key);
      });
    })(k);
  });
  return lit;
}
function toggleFocus(key){
  if(tExpandAll) tExpandAll=false;
  tFocus[key]=!tFocus[key];
  const cur=tPick(); if(!cur) return;
  renderLevels(cur);
}
function focusBar(cur){
  const bar=el('div','focus-bar');
  const exp=el('button','mini'+(tExpandAll?' on':''),'展开全部');
  exp.title='不聚焦，展开全部子阶段（每一级另起一行）';
  exp.onclick=function(){tFocus={};tExpandAll=true;renderLevels(cur);};
  bar.appendChild(exp);
  const clr=el('button','mini','收起全部');
  clr.title='取消全部聚焦与展开';
  clr.onclick=function(){tFocus={};tExpandAll=false;renderLevels(cur);};
  bar.appendChild(clr);
  bar.appendChild(el('span','tl-label','焦点'));
  const keys=Object.keys(tFocus).filter(function(k){return tFocus[k];});
  if(!keys.length){
    bar.appendChild(el('span','focus-empty',tExpandAll
      ? '已展开全部子阶段（点击任一块可切换为聚焦）'
      : '未聚焦 —— 点击任一阶段块即可聚焦，其下一级在下一行展开'));
    return bar;
  }
  keys.forEach(function(k){
    const chip=el('span','chip',k);
    const x=el('button','chip-x','×'); x.title='取消聚焦';
    x.onclick=function(){tFocus[k]=false;renderLevels(cur);};
    chip.appendChild(x); bar.appendChild(chip);
  });
  return bar;
}
function levelTip(b){
  const st=b.x.st||{};
  const nm=b.residual?'残差':b.x.label;
  const effN=(st.avg_n!=null)?st.avg_n:st.n;
  const tip=[nm,'样本 '+(effN==null?'-':effN)];
  ['min','avg','p50','p95','p99','p999','p9999','max','sigma','neg'].forEach(
    function(k){ if(st[k]!=null) tip.push(k+'：'+st[k]+' µs'); });
  tip.push('占父项：'+(b.share*100).toFixed(1)+'%');
  if(b.residual) tip.push('残差 = 父项 − 各子项之和（未展开的部分）');
  else tip.push('点击聚焦：下一级在下一行展开，其余块横向压缩');
  return tip.join('\n');
}
/* 块：两行文字（名称 / 值 + 占父%），无任何边框；聚焦靠块的形态（更大）。 */
function lvlBlock(b,cur,lit,depth){
  const blk=el('div','lvl-blk'+(b.focused?' focus':'')+
    (b.compressed?' compressed':''));
  const st=lvlStyle(b.share,b.residual);
  blk.style.background=st.background; blk.style.color=st.color;
  blk.appendChild(el('div','lvl-name',b.x.label));
  blk.appendChild(el('div','lvl-val',
    fmtUs(b.x.v)+'  '+(b.share*100).toFixed(1)+'%'));
  blk.title=levelTip(b);
  const hasKids=!b.residual&&tChildrenOf(b.x.key,cur).length>0;
  if(hasKids){
    blk.style.cursor='pointer';
    blk.title=blk.title+'\n点击聚焦展开下一级';
    blk.onclick=function(){toggleFocus(b.x.key);};
  }else{
    blk.style.cursor='default';
  }
  return blk;
}
/* 一行 = 同一父下的一组子项：列宽 = 占父百分比（minmax(可读下限, Nfr)）。
   聚焦时该列横向扩展、其余列压缩（压缩块文字隐藏）；子级另起一行。 */
/* 12px 字号文字宽度估算：CJK/全角按双宽（12.4px），其余 6.9px。
   用于 0% 块「尽可能窄」= 文字宽 + 标准 padding（.lvl-blk 水平 padding 22px）。 */
function textW12(t){
  let w=0; const s=String(t);
  for(let i=0;i<s.length;i++)
    w+=(s.charCodeAt(i)>0x2E80)?12.4:6.9;
  return w;
}
/* 块内是两行堆叠（名称 / 值+占比）：块宽只需容纳最宽的一行，而不是把
   两行拼成一串测宽（此前按拼接长度算，0% 块与 minW 都虚高约 2 倍，
   整行被撑出横向滚动、行尾残差被裁半）。 */
function lvlLineW(label,valTxt){
  return Math.ceil(Math.max(textW12(label),textW12(valTxt||'')));
}
function lvlZeroW(label,valTxt){return lvlLineW(label,valTxt)+26;}
function lvlBlockMinW(label,valTxt){
  return Math.max(96,lvlLineW(label,valTxt)+34);
}
function appendLevelRow(rowEl,items,parentStat,parentVal,cur,lit,depth){
  const metric=tLevelMetric;
  const vals=items.map(function(it){
    const v=tMetricOf(it.st,metric);
    return {key:it.key,label:it.label,st:it.st,v:(v==null?0:v),has:v!=null};
  });
  const sum=vals.reduce(function(a,b){return a+b.v;},0);
  const parentV=parentVal||0;
  let scale=1,over=false;
  if(parentV>0&&sum>parentV){scale=parentV/sum;over=true;}
  const used=vals.reduce(function(a,b){return a+b.v*scale;},0);
  const rest=parentV>0?Math.max(0,parentV-used):0;
  const hasResidual=vals.some(function(x){return x.key==='residual';});
  const hasRest=!hasResidual&&(rest>0.0001||over);
  const anyFocus=vals.some(function(x){return !!tFocus[x.key];});
  const cols=[],blocks=[];
  vals.forEach(function(x){
    const share=parentV>0?(x.v/parentV):0;
    const w=share*100*scale;
    const valTxt=fmtUs(x.v)+'  '+(share*100).toFixed(1)+'%';
    const minW=lvlBlockMinW(x.label,valTxt);
    const zeroW=lvlZeroW(x.label,valTxt); /* 0% 块：文字宽 + 标准 padding，尽可能窄 */
    const focused=!!tFocus[x.key];
    let col;
    if(focused) col='minmax('+minW+'px, 9fr)';
    else if(anyFocus) col='minmax(0px, 0.4fr)';
    else if(w<=0) col=zeroW+'px';
    else col='minmax('+minW+'px,'+w.toFixed(3)+'fr)';
    cols.push(col);
    blocks.push({x:x,share:share,w:w,focused:focused,
      compressed:anyFocus&&!focused});
  });
  if(hasRest){
    const share=parentV>0?(rest/parentV):0;
    const resW=lvlBlockMinW('残差',
      fmtUs(rest)+'  '+(share*100).toFixed(1)+'%');
    cols.push(anyFocus?'minmax(0px, 0.4fr)'
      :'minmax('+resW+'px,'+(share*100).toFixed(3)+'fr)');
    blocks.push({x:{key:'__residual__',label:'残差',
      st:tStat('residual',cur),v:rest},
      share:share,w:share*100,residual:true,compressed:anyFocus});
  }
  rowEl.style.gridTemplateColumns=cols.join(' ');
  blocks.forEach(function(b,i){
    rowEl.appendChild(lvlBlock(b,cur,lit,depth));
    const kids=b.residual?[]:tChildrenOf(b.x.key,cur);
    if(!kids.length) return;
    if(!(b.focused||tExpandAll)) return;
    const holder=el('div','lvl-children');
    holder.style.gridColumn=(i+1);
    holder.style.gridRow='2';
    const sub=el('div','lvl-row');
    appendLevelRow(sub,kids,b.x.st,b.x.v,cur,lit,depth+1);
    holder.appendChild(sub);
    rowEl.appendChild(holder);
  });
  const notes=[];
  if(over) notes.push('子项合计 '+(sum/parentV*100).toFixed(1)+
    '%，超出父项 —— 已按比例压缩填满父项宽度。');
  const tiny=blocks.some(function(b){return b.share>0&&b.share<0.08;});
  if(tiny) notes.push('本行含占比极小的项：已按最小可读宽度展开（整行可横向滚动），'+
    '块内百分比与颜色仍按真实占比计算。');
  notes.forEach(function(t){
    if(rowEl.parentNode) rowEl.parentNode.appendChild(el('div','lvl-note',t));
  });
}
function renderLevels(cur){
  const box=document.getElementById('levels'); if(!box) return;
  box.innerHTML='';
  const total=tStat('total_us',cur);
  const tv=tMetricOf(total,tLevelMetric);
  box.appendChild(focusBar(cur));
  if(tv==null){
    box.appendChild(el('div','note','当前指标（'+tMetricName(tLevelMetric)+
      '）没有可用数据：样本量不足 10000 或该阶段没有样本。'));
    return;
  }
  const lit=focusLit(cur);
  const rw=el('div','lvl-wrap');
  const rrow=el('div','lvl-row root');
  rrow.style.gridTemplateColumns='minmax(0,1fr)';
  rrow.appendChild(lvlBlock({x:{key:'total_us',label:'total',st:total,v:tv},
    share:1},cur,lit,0));
  rw.appendChild(rrow);
  box.appendChild(rw);
  const l1=tChildrenOf('total_us',cur);
  if(l1.length){
    const wrap=el('div','lvl-wrap');
    const row=el('div','lvl-row');
    appendLevelRow(row,l1,total,tv,cur,lit,1);
    wrap.appendChild(row);
    box.appendChild(wrap);
  }
}

/* ---- 阶段曲线：与时延曲线同一套交互（独立状态） ---- */
const tYState={domain:[0,1],p:0};
let tLabelMeta=[];   /* 端点标签数据 {name,color,data} */
function tYDomainOf(){
  let mx=-Infinity,mn=Infinity;
  (tLabelMeta||[]).forEach(function(m){
    (m.data||[]).forEach(function(d){
      if(d&&d[1]!=null){
        if(d[1]>mx)mx=d[1];
        if(d[1]<mn)mn=d[1];}});
  });
  if(!isFinite(mx)||!isFinite(mn)){mx=1;mn=0;}
  if(mx===mn)mx=mn+1;
  if(mn>0)mn=0;
  return [mn,mx];
}
function tYWindowFromChart(){
  const d=tYState.domain, span=(d[1]-d[0])||1;
  let s=0,e=100;
  try{
    const opt=tChart.getOption();
    const dz=(opt.dataZoom||[])[1]||{};
    if(dz.start!=null)s=dz.start;
    if(dz.end!=null)e=dz.end;
  }catch(err){}
  return [d[0]+s/100*span, d[0]+e/100*span];
}
function tApplyYWindow(lo,hi){
  const d=tYState.domain, span=(d[1]-d[0])||1;
  const minSpan=span*0.01;
  if(hi-lo<minSpan){
    const mid=(lo+hi)/2;
    lo=mid-minSpan/2; hi=mid+minSpan/2;
  }
  lo=Math.max(d[0],Math.min(lo,d[1]-minSpan));
  hi=Math.min(d[1],Math.max(hi,d[0]+minSpan));
  if(hi<=lo)hi=Math.min(d[1],lo+minSpan);
  try{
    tChart.dispatchAction({type:'dataZoom',dataZoomIndex:1,
      start:(lo-d[0])/span*100, end:(hi-d[0])/span*100});
  }catch(e){}
  tSyncYScale();
}
function tSetZoomP(p){
  tYState.p=Math.max(0,Math.min(1,p));
  const d=tYState.domain, span0=(d[1]-d[0])||1;
  const F=Math.pow(Y_ZOOM_MAX,tYState.p);
  const w=tYWindowFromChart();
  let lo=Math.max(d[0], w[0]);
  let hi=lo+span0/F;
  if(hi>d[1]){
    hi=d[1];
    lo=Math.max(d[0], hi-span0/F);
  }
  tApplyYWindow(lo,hi);
}
function tSyncYScale(){
  const rail=document.getElementById('tYscaleRail');
  const win=document.getElementById('tYscaleWin');
  const track=document.getElementById('tZsliderTrack');
  const fill=document.getElementById('tZsliderFill');
  const handle=document.getElementById('tZsliderHandle');
  const cap=document.getElementById('tZsliderCap');
  if(!rail||!win) return;
  const d=tYState.domain, span=(d[1]-d[0])||1;
  const w=tYWindowFromChart();
  const topPct=Math.max(0,Math.min(100,(d[1]-w[1])/span*100));
  const botPct=Math.max(0,Math.min(100,(d[1]-w[0])/span*100));
  win.style.top=topPct+'%';
  win.style.height=Math.max(1.5,botPct-topPct)+'%';
  const th=(track&&track.clientHeight)||150;
  const travel=Math.max(1,th-13);
  const hx=(1-tYState.p)*travel;
  if(handle) handle.style.top=hx+'px';
  if(fill) fill.style.height=Math.max(0,th-hx-6.5)+'px';
  if(cap){
    cap.style.top=(hx+6.5)+'px';
    cap.textContent=fmtFactor(Math.pow(Y_ZOOM_MAX,tYState.p));
  }
}
function tBindYScale(){
  const track=document.getElementById('tZsliderTrack');
  const handle=document.getElementById('tZsliderHandle');
  const win=document.getElementById('tYscaleWin');
  const rail=document.getElementById('tYscaleRail');
  if(handle&&track){
    handle.onmousedown=function(ev){
      if(ev&&ev.preventDefault)ev.preventDefault();
      const z=document.getElementById('tZslider');
      if(z){
        if(z.classList) z.classList.add('dragging');
        else z.className=(z.className+' dragging').trim();
      }
      const r=track.getBoundingClientRect();
      const set=function(y){tSetZoomP((r.bottom-y)/Math.max(1,r.height));};
      function mv(e){set(e.clientY);}
      function up(){
        if(z){
          if(z.classList) z.classList.remove('dragging');
          else z.className=String(z.className||'').replace(' dragging','').trim();
        }
        document.removeEventListener('mousemove',mv);
        document.removeEventListener('mouseup',up);
      }
      document.addEventListener('mousemove',mv);
      document.addEventListener('mouseup',up);
    };
  }
  if(win&&rail){
    win.onmousedown=function(ev){
      if(ev&&ev.preventDefault)ev.preventDefault();
      const r=rail.getBoundingClientRect();
      const d=tYState.domain;
      const w0=tYWindowFromChart();
      const y0=ev.clientY;
      const span=(d[1]-d[0])||1;
      const dvMin=d[0]-w0[0], dvMax=d[1]-w0[1];
      function mv(e){
        const raw=-(e.clientY-y0)/Math.max(1,r.height)*span;
        const dv=Math.max(dvMin,Math.min(dvMax,raw));
        tApplyYWindow(w0[0]+dv,w0[1]+dv);
      }
      function up(){
        document.removeEventListener('mousemove',mv);
        document.removeEventListener('mouseup',up);
      }
      document.addEventListener('mousemove',mv);
      document.addEventListener('mouseup',up);
    };
  }
}
/* 端点标签：固定右缘 + 引线 + hover 聚焦 */
function tXWindowMs(){
  let s=0,e=100;
  try{
    const opt=tChart.getOption();
    const dz=(opt.dataZoom||[])[0]||{};
    if(dz.start!=null)s=dz.start;
    if(dz.end!=null)e=dz.end;
  }catch(err){}
  const zs=zoomState('stage');
  const ext=(zs.max-zs.min)||1;
  return [zs.min+ext*s/100, zs.min+ext*e/100];
}
function tLayoutEndLabels(ch){
  const ov=document.getElementById('tEndlabels');
  if(!ov) return;
  let sers=[];
  try{ sers=(ch.getOption().series)||[]; }catch(e){ sers=[]; }
  const w=tXWindowMs();
  const H=ch.getHeight?ch.getHeight():400;
  const top=STAGE_GRID.top+8, bot=H-STAGE_GRID.bottom-8;
  let mx=-Infinity,mn=Infinity;
  sers.forEach(function(sr){
    (sr.data||[]).forEach(function(d){
      if(d&&d[1]!=null){ if(d[1]>mx)mx=d[1]; if(d[1]<mn)mn=d[1]; }
    });
  });
  if(!isFinite(mx)){mx=1;mn=0;}
  if(mx===mn)mx=mn+1;
  const items=[];
  sers.forEach(function(sr,i){
    const last=mcLastVisible(sr.data||[],w);
    if(!last) return;
    let py=mcAnchorY(ch,i,last);
    if(py==null) py=top+((mx-last[1])/(mx-mn))*(bot-top);
    items.push({py:py,name:String(sr.name||''),
      color:((sr.lineStyle||{}).color)||C.fg});
  });
  mcRenderLabels(ch,ov,items,STAGE_GRID);
}
/* 底部平移条（仅平移 x） */
function tSyncXPan(){
  const win=document.getElementById('tXpanWin');
  if(!win) return;
  let s=0,e=100;
  try{
    const opt=tChart.getOption();
    const dz=(opt.dataZoom||[])[0]||{};
    if(dz.start!=null)s=dz.start;
    if(dz.end!=null)e=dz.end;
  }catch(err){}
  win.style.left=s+'%';
  win.style.width=Math.max(1,e-s)+'%';
}
function tBindXPan(){
  const strip=document.getElementById('tXpan');
  const win=document.getElementById('tXpanWin');
  if(!strip||!win) return;
  win.onmousedown=function(ev){
    if(ev&&ev.preventDefault)ev.preventDefault();
    const r=strip.getBoundingClientRect();
    let s0=0,e0=100;
    try{
      const opt=tChart.getOption();
      const dz=(opt.dataZoom||[])[0]||{};
      if(dz.start!=null)s0=dz.start;
      if(dz.end!=null)e0=dz.end;
    }catch(err){}
    const zs=zoomState('stage');
    const ext=(zs.max-zs.min)||1;
    const w0s=zs.min+ext*s0/100, w0e=zs.min+ext*e0/100;
    const x0=ev.clientX;
    const dvMin=zs.min-w0s, dvMax=zs.max-w0e;
    function mv(e){
      const raw=(e.clientX-x0)/Math.max(1,r.width)*ext;
      const dv=Math.max(dvMin,Math.min(dvMax,raw));
      try{
        tChart.dispatchAction({type:'dataZoom',dataZoomIndex:0,
          start:(w0s+dv-zs.min)/ext*100,
          end:(w0e+dv-zs.min)/ext*100});
      }catch(err){}
      tSyncXPan();
    }
    function up(){
      document.removeEventListener('mousemove',mv);
      document.removeEventListener('mouseup',up);
    }
    document.addEventListener('mousemove',mv);
    document.addEventListener('mouseup',up);
  };
}
/* tooltip 延迟 + 邻域触发 */
function totalSeriesData(cur,tier,metric){
  /* total 曲线：优先用 1s 秒桶现算目标档位（短 run 在 1s/10s
     档才有足够点数）；旧产物只有 minutes（1 分钟桶）时退回分桶聚合，
     此时低于 60s 的档位与 60s 档等价，不会出现空图。 */
  const src=(cur.data.seconds&&cur.data.seconds.length)?
    cur.data.seconds:(cur.data.minutes||[]);
  const agg={};
  src.forEach(function(r){
    const k=Math.floor(r[0]/tier)*tier;
    const a=agg[k]||(agg[k]={t:k,ok:0,sum:0,max:0});
    a.ok+=r[2]||0;
    a.sum+=(r[4]||0)*(r[2]||0);
    a.max=Math.max(a.max,r[5]||0);
  });
  return Object.keys(agg).map(Number).sort(function(x,y){return x-y;})
    .map(function(k){
      const a=agg[k];
      return [a.t*1000, metric==='max'?a.max:(a.ok?a.sum/a.ok:0)];});
}
const STAGE_GRID={left:104,right:104,top:30,bottom:50};
let tChartBound=false;
/* 分桶/自适应策略（与服务端 report.py 完全一致）：
   时间尺度 s 可用 ⇔ span/s ∈ [59, 8192]；自适应仅折叠时间桶，
   使展示点数尽量贴近 2048。 */
const TIER_MIN_BUCKETS=59, TIER_MAX_BUCKETS=8192;
const ADAPTIVE_MIN_BUCKETS=2048, ADAPTIVE_MAX_BUCKETS=4096;
function tierScalesFor(span,minStep){
  const out=[];
  [1,10,60].forEach(function(s){
    if(s<(minStep||1)) return;
    if(span>0&&span/s>=TIER_MIN_BUCKETS&&span/s<=TIER_MAX_BUCKETS)
      out.push(s);
  });
  return out.length?out:[Math.max(1,minStep||1)];
}
function adaptiveStepFor(span,scales){
  scales=(scales&&scales.length)?scales:[1];
  span=Number(span)||0;
  let best=null;
  scales.forEach(function(s){
    if(span<=0) return;
    const n=Math.ceil(span/s);
    let cand;
    if(n<=ADAPTIVE_MAX_BUCKETS){
      cand={d:Math.abs(n-ADAPTIVE_MIN_BUCKETS),step:s};
    }else{
      let kb=null,mb=null;
      for(let k=2;k<=64;k++){
        const m=n/k;
        if(m<ADAPTIVE_MIN_BUCKETS) break;
        if(m<=ADAPTIVE_MAX_BUCKETS&&
           (kb===null||Math.abs(m-ADAPTIVE_MIN_BUCKETS)<
            Math.abs(n/kb-ADAPTIVE_MIN_BUCKETS))){kb=k;mb=m;}
      }
      if(kb===null){kb=Math.max(2,Math.ceil(n/ADAPTIVE_MAX_BUCKETS));mb=n/kb;}
      cand={d:Math.abs(Math.round(mb)-ADAPTIVE_MIN_BUCKETS),step:s*kb};
    }
    if(!best||cand.d<best.d) best=cand;
  });
  return best?best.step:(scales[0]||1);
}
function tierHintFor(step,scales,span){
  for(let i=(scales||[]).length-1;i>=0;i--){
    const s=scales[i];
    if(step%s===0){
      const k=step/s, n=span?Math.ceil(span/step):0;
      return (k<=1?(s+' 秒'):(s+' 秒 × '+k))+' ≈ '+n+' 点';
    }
  }
  return '自适应';
}
/* 阶段时间桶的数据跨度（秒）：fminutes 全部字段的 min..max；无数据返回 null。 */
function tFmSpan(cur){
  const fm=(cur&&cur.data&&cur.data.fminutes)||{};
  let lo=null,hi=null;
  Object.keys(fm).forEach(function(k){
    (fm[k]||[]).forEach(function(r){
      if(lo===null||r[0]<lo)lo=r[0];
      if(hi===null||r[0]>hi)hi=r[0];});
  });
  return (lo!=null&&hi!=null)?Math.max(1,hi-lo+1):null;
}
function buildStageChart(cur){
  const box=document.getElementById('tchart'); if(!box) return;
  const cap=document.getElementById('tchartEmpty');
  ensureTraceSel();
  const objs=tObjSel.slice();
  const stages=tStageSel.slice();
  const multiObj=objs.length>1;
  const multiStage=!multiObj&&stages.length>1;
  const base=tObjPick(objs[0]||tRoleKey())||(cur&&cur.data)||{};
  const ftierMin=Number(base.ftier)||1;
  let tier;
  if(tStageTier==='auto'||tStageTier==='cnt'){
    const span=tFmSpan({data:base})||0;
    tier=adaptiveStepFor(span,tierScalesFor(span,ftierMin));
  }else{
    tier=Number(tStageTier)||60;
  }
  tier=Math.max(tier,ftierMin);
  const series=[]; let t0=null,t1=null;
  tLabelMeta=[];
  function addSeries(nm,color,data,wide,dash){
    if(!data||!data.length) return;
    const w=wide?1.0:0.8;
    tLabelMeta.push({name:nm,color:color,data:data});
    series.push({name:nm,type:'line',showSymbol:false,clip:true,
      lineStyle:{width:w,color:color,type:dash||'solid'},
      itemStyle:{color:color},z:3,
      data:data});
    data.forEach(function(d){
      const t=d[0]/1000;
      if(t0===null||t<t0)t0=t;
      if(t1===null||t>t1)t1=t;
    });
  }
  const stageOrder=tStageKeys();
  /* 对象与阶段可任意组合（不做互斥收敛）：
     - 单对象：颜色=阶段调色板（可叠加任意多阶段）
     - 多对象：颜色=对象；阶段>1 时用线型（实/虚/点）区分阶段 */
  const DASHES=['solid','dashed','dotted'];
  function dashFor(i){return DASHES[i%DASHES.length];}
  if(objs.length&&stages.length){
    if(objs.length===1){
      const k=objs[0];
      const op=tObjPick(k);
      const isRole=(k===tRoleKey());
      stages.forEach(function(stage){
        const nm=(stages.length>1||isRole)?tStageName(stage):
          (traceObjLabel(k)+' · '+tStageName(stage));
        addSeries(nm,stageColor(stageOrder.indexOf(stage)),
          tStageSeriesData(op,stage,tier,tCurveMetric),
          stage==='total_us');
      });
    }else{
      objs.forEach(function(k){
        const op=tObjPick(k);
        stages.forEach(function(stage,si){
          const nm=stages.length>1
            ?(traceObjLabel(k)+' · '+tStageName(stage))
            :traceObjLabel(k);
          addSeries(nm,traceObjColor(k),
            tStageSeriesData(op,stage,tier,tCurveMetric),
            stage==='total_us',
            stages.length>1?dashFor(si):'solid');
        });
      });
    }
  }
  const hasData=series.length>0;
  if(cap){
    cap.style.display=hasData?'none':'';
    if(!hasData){
      if(!objs.length&&!stages.length)
        cap.textContent='未选择对象（总计 / 节点 / 实例）与阶段：在下方控台勾选后查看。';
      else if(!objs.length)
        cap.textContent='未选择对象（总计 / 节点 / 实例）：在左侧对象树勾选后查看。';
      else if(!stages.length)
        cap.textContent='未选择阶段：在右侧阶段树勾选（或点「全选」）后查看。';
      else if(!tObjKeys().length)
        cap.textContent='该数据大小没有 trace 对象数据（节点/实例阶段桶）。';
      else
        cap.textContent='所选对象暂无可用的阶段时间桶（旧产物或该对象未内嵌；完整数据见 data.js）。';
    }
  }
  if(!tChart) tChart=echarts.init(box);
  bindHoverGuards(tChart);
  if(!hasData){
    try{tChart.clear();}catch(e){}
    safe('stage-endlabels',function(){tLayoutEndLabels(tChart);});
    return;
  }
  const o=axisCommon();
  o.grid=STAGE_GRID;
  o.tooltip=lfTooltip({trigger:'axis'});
  /* trace 域的 warmup 结束线（meta.warmup_cutoff 由 tracestats 计算） */
  if(series.length){
    const _tm=((cur&&cur.data&&cur.data.meta)||(traceOf()||{}).meta||{});
    const _wc=_tm.warmup_cutoff;
    if(_wc!=null&&isFinite(Number(_wc))){
      series[0].markLine={symbol:'none',silent:true,
        data:[{xAxis:Number(_wc)*1000,
          lineStyle:{color:C.fg,width:1,type:'dashed',opacity:0.75},
          label:{show:true,position:'insideEndTop',formatter:'warmup 结束',
                 color:C.mut}}]};
    }
  }
  o.tooltip.valueFormatter=function(v){return fmtUs(v);};
  o.tooltip.formatter=function(ps){
    if(!ps||!ps.length) return '';
    const v0=(ps[0].value&&ps[0].value[0]);
    const t=(v0!=null)?v0/1000:(ps[0].axisValue||0);
    const rows=ps.map(function(p){
      const v=(p.value&&p.value[1]!=null)?p.value[1]:null;
      return (p.marker||'')+p.seriesName+'：'+(v==null?'-':fmtUs(v));
    });
    return '<b>'+fullts(t)+'</b><br>'+rows.join('<br>');
  };
  o.yAxis=[lfAxisValue({name:'µs',scale:true,nameLocation:'end',nameGap:14})];
  o.dataZoom=zoomModel('main');
  if(t0!==null) o.xAxis=lfAxisTime(xExtent(t0,t1,
    plotWidthOf(tChart,STAGE_GRID)),'stage');
  o.series=series;
  tChart.setOption(o,true);
  safe('stage-yscale',function(){
    tYState.domain=tYDomainOf();
    tSetZoomP(tYState.p);
  });
  safe('stage-endlabels',function(){tLayoutEndLabels(tChart);});
  safe('stage-xpan',function(){tSyncXPan();});
  if(!tChartBound){tChartBound=true;safe('stage-interactions',function(){
    bindInteract(tChart,STAGE_GRID);
    tBindYScale(); tBindXPan();
    try{
      tChart.on('finished',function(){
        safe('stage-endlabels',function(){tLayoutEndLabels(tChart);});});
      tChart.on('dataZoom',function(){
        safe('stage-endlabels',function(){tLayoutEndLabels(tChart);});
        safe('stage-xpan',function(){tSyncXPan();});
        safe('stage-yscale',function(){tSyncYScale();});
        safe('stage-x-ticks',function(){syncZoomSpan(tChart,'stage');});});
    }catch(e){}
  });}
}
/* 阶段序列：total 用秒桶（[t,n,ok,fail,avg,max,...]），阶段用 fminutes
   的 [t,n,avg,max]；按档位聚合（avg 按样本数加权，max 取最大）。 */
function tAggFm(rows,tier,metric){
  const acc={};
  (rows||[]).forEach(function(r){
    const k=Math.floor(r[0]/tier)*tier;
    const a=acc[k]||(acc[k]={t:k,n:0,sum:0,max:0});
    const n=Number(r[1])||0;
    a.n+=n;
    a.sum+=(Number(r[2])||0)*n;
    a.max=Math.max(a.max,Number(r[3])||0);
  });
  return Object.keys(acc).map(Number).sort(function(x,y){return x-y;})
    .map(function(k){
      const a=acc[k];
      return [a.t*1000, metric==='max'?a.max:(a.n?a.sum/a.n:0)];
    });
}
function tAggTotal(op,tier,metric){
  const acc={};
  ((op&&op.seconds)||[]).forEach(function(r){
    const k=Math.floor(r[0]/tier)*tier;
    const a=acc[k]||(acc[k]={t:k,n:0,sum:0,max:0});
    const n=Number(r[1])||0;
    a.n+=n;
    a.sum+=(Number(r[4])||0)*n;
    a.max=Math.max(a.max,Number(r[5])||0);
  });
  return Object.keys(acc).map(Number).sort(function(x,y){return x-y;})
    .map(function(k){
      const a=acc[k];
      return [a.t*1000, metric==='max'?a.max:(a.n?a.sum/a.n:0)];
    });
}
function tStageSeriesData(op,stage,tier,metric){
  if(!op) return [];
  if(stage==='total_us') return tAggTotal(op,tier,metric);
  return tAggFm(((op.fminutes||{})[stage])||[],tier,metric);
}
function stageObjToggle(key){
  if(tObjKeys().indexOf(key)<0) return;
  /* 对象 = 自由多选 checkbox（与阶段选择互不影响；允许空） */
  const i=tObjSel.indexOf(key);
  if(i>=0) tObjSel.splice(i,1); else tObjSel.push(key);
  renderStageObjTree(); renderTrace();
}
function stageObjSetMany(keys,on){
  keys.forEach(function(k){
    if(tObjKeys().indexOf(k)<0) return;
    const i=tObjSel.indexOf(k);
    if(on&&i<0) tObjSel.push(k);
    if(!on&&i>=0) tObjSel.splice(i,1);
  });
  renderStageObjTree(); renderTrace();
}
function stageTreeModel(){
  const all=tObjAll(); const g=tGroup;
  const roles=[],nodes=[],clients=[];
  Object.keys(all).forEach(function(k){
    const o=all[k]; if(!o||o.role!==g) return;
    if(o.kind==='role') roles.push(k);
    else if(o.kind==='node') nodes.push(k);
    else clients.push(k);
  });
  nodes.sort(function(a,b){
    return String(all[a].node||'').localeCompare(String(all[b].node||''));});
  clients.sort(function(a,b){
    const na=String(all[a].node||''),nb=String(all[b].node||'');
    if(na!==nb) return na.localeCompare(nb);
    return String(all[a].label||'').localeCompare(String(all[b].label||''));});
  const byNode={};
  clients.forEach(function(k){
    const n=all[k].node||'未归属节点';
    (byNode[n]=byNode[n]||[]).push(k);
  });
  return {all:all,roles:roles,nodes:nodes,clients:clients,byNode:byNode};
}
function renderStageObjTree(){
  const box=document.getElementById('stageObjTree'); if(!box) return;
  ensureTraceSel();
  box.innerHTML='';
  const m=stageTreeModel();
  function row(key,label,depth,lead,minis,note){
    const on=tObjSel.indexOf(key)>=0;
    const r=el('div','tree-row'+(on?' on':''));
    r.style.paddingLeft=(depth>1?36:0)+'px';
    if(lead) r.appendChild(lead);
    const b=el('button','tree-check');
    b.setAttribute('data-key',key);
    b.appendChild(el('span','tree-box',on?'✓':''));
    b.appendChild(el('span','tree-name',label));
    b.onclick=function(){stageObjToggle(key);};
    r.appendChild(b);
    (minis||[]).forEach(function(x){r.appendChild(x);});
    if(note) r.appendChild(el('span','tree-note',note));
    return r;
  }
  function mini(label,keys,on){
    const b=el('button','mini',label);
    b.onclick=function(){stageObjSetMany(keys,on);};
    return b;
  }
  /* 总计：当前读写组的全部实例聚合（角色由顶部总控决定，这里不再有角色行） */
  if(m.roles.length){
    const g1=el('div','tree-group');
    g1.appendChild(el('span','tree-group-title','总计'));
    m.roles.forEach(function(k){
      g1.appendChild(row(k,traceObjLabel(k),1));
    });
    box.appendChild(g1);
  }
  if(!m.nodes.length&&!m.clients.length) return;
  const g2=el('div','tree-group node-group');
  const head=el('div','node-group-head');
  head.appendChild(el('span','tree-group-title','节点与实例'));
  head.appendChild(el('span','objtree-count','已选 '+tObjSel.length+' 项'));
  g2.appendChild(head);
  function kindKeys(kind){
    return m.all?Object.keys(m.all).filter(function(k){
      const o=m.all[k];
      return o&&o.role===tGroup&&o.kind===kind;}).sort():[];
  }
  function kindAll(kind){
    const ks=kindKeys(kind);
    return ks.length>0&&ks.every(function(k){
      return tObjSel.indexOf(k)>=0;});
  }
  function clearAll(){
    /* 允许空选：真正的全部清空（图表显示空态，不回填总计） */
    tObjSel=[];
    renderStageObjTree(); renderTrace();
  }
  function actBtn(label,title,fn){
    const b=el('button','mini',label);
    b.title=title; b.onclick=fn; return b;
  }
  const ndAll=kindAll('node'), clAll=kindAll('client');
  const acts=el('div','node-group-acts');
  acts.appendChild(actBtn(ndAll?'清空节点':'全选节点',
    ndAll?'取消全部节点选择':'选中当前范围内全部节点',
    function(){stageObjSetMany(kindKeys('node'),!ndAll);}));
  acts.appendChild(actBtn(clAll?'清空实例':'全选实例',
    clAll?'取消全部实例选择':'选中当前范围内全部实例',
    function(){stageObjSetMany(kindKeys('client'),!clAll);}));
  acts.appendChild(actBtn('全部清空',
    '清空全部对象（总计 / 节点 / 实例；允许空选，图表显示空态）',
    clearAll));
  g2.appendChild(acts);
  const list=el('div','tree-list');
  m.nodes.forEach(function(k){
    const nodeName=String((m.all[k]&&m.all[k].node)||k.replace(/^node:/,''));
    const kids=m.byNode[nodeName]||[];
    const open=!!treeOpen['stage:'+k];
    let caret=null;
    if(kids.length){
      caret=el('button','tree-caret',open?'▾':'▸');
      caret.onclick=function(){treeOpen['stage:'+k]=!treeOpen['stage:'+k];
        renderStageObjTree();};
    }
    list.appendChild(row(k,traceObjLabel(k),1,caret,kids.length?[
      mini('全选',kids,true),mini('清空',kids,false)]:[],
      kids.length?(kids.length+' 实例'):''));
    if(open&&kids.length) kids.forEach(function(ck){
      list.appendChild(row(ck,traceObjLabel(ck),2));
    });
  });
  if(!m.nodes.length){
    list.appendChild(el('div','tree-empty','该读写组没有节点聚合对象'));
  }
  g2.appendChild(list);
  box.appendChild(g2);
}
function renderStageBtns(cur){
  const stb=document.getElementById('stageTierBtns');
  if(stb){
    stb.innerHTML='';
    const base=(cur&&cur.data)||{};
    const span=tFmSpan({data:base})||0;
    const ftierMin=Number(base.ftier)||1;
    const scales=tierScalesFor(span||0,ftierMin);
    const ac=el('button',tStageTier==='cnt'?'on':'','自适应');
    ac.title='自适应按 30~120 桶自动选档';
    ac.onclick=function(){tStageTier='cnt';renderStageBtns(cur);
      buildStageChart(cur);};
    stb.appendChild(ac);
    [[1,'1 秒'],[10,'10 秒'],[60,'1 分钟']].forEach(function(s){
      if(scales.indexOf(s[0])<0) return;
      const b=el('button',tStageTier===String(s[0])?'on':'',s[1]);
      b.title='时间桶 '+s[0]+' 秒（约 '+Math.max(1,Math.floor(span/s[0]))+' 桶）';
      b.onclick=function(){tStageTier=String(s[0]);renderStageBtns(cur);
        buildStageChart(cur);};
      stb.appendChild(b);
    });
  }
}
let tStageOpen={};   /* 阶段树 L2 展开状态 */
function toggleStageKey(k,cur){
  /* 阶段 = 自由多选 checkbox（与对象选择互不影响；允许空） */
  const i=tStageSel.indexOf(k);
  if(i>=0) tStageSel.splice(i,1); else tStageSel.push(k);
  renderStageObjTree(); renderStageTree(cur); buildStageChart(cur);
}
function stageSelectableKeys(cur){
  const g=(cur&&cur.g)||tGroup;
  const ks=['total_us'];
  tTree(g).forEach(function(x){
    if(x[1]===1&&ks.indexOf(x[0])<0)ks.push(x[0]);});
  return ks;
}
function renderStageTree(cur){
  const box=document.getElementById('stageTree'); if(!box) return;
  box.innerHTML='';
  const g=(cur&&cur.g)||tGroup;
  const tree=tTree(g);
  const order=stageSelectableKeys(cur);
  const head=el('div','node-group-head');
  head.appendChild(el('span','tree-group-title','阶段'));
  head.appendChild(el('span','objtree-count','已选 '+tStageSel.length+' 项'));
  box.appendChild(head);
  const acts=el('div','node-group-acts');
  const allb=el('button','mini','全选');
  allb.title='选中全部可选阶段（total / L1 / 残差）';
  allb.onclick=function(){
    tStageSel=order.slice();
    renderStageObjTree(); renderStageTree(cur); buildStageChart(cur);
  };
  const clrb=el('button','mini','清空');
  clrb.title='清空全部阶段（允许空选，图表显示空态）';
  clrb.onclick=function(){
    tStageSel=[];
    renderStageTree(cur); buildStageChart(cur);
  };
  acts.appendChild(allb); acts.appendChild(clrb);
  box.appendChild(acts);
  const list=el('div','tree-list');
  function stageRow(key,level){
    const on=tStageSel.indexOf(key)>=0;
    const r=el('div','tree-row'+(on?' on':''));
    r.style.paddingLeft=(level*18)+'px';
    const kids=(level===1)?tree.filter(function(x){
      return x[1]===2&&x[2]===key;}):[];
    if(kids.length){
      const caret=el('button','tree-caret',tStageOpen[key]?'▾':'▸');
      caret.title=tStageOpen[key]?'收起二级子阶段':'展开二级子阶段';
      caret.onclick=function(){tStageOpen[key]=!tStageOpen[key];
        renderStageTree(cur);};
      r.appendChild(caret);
    }
    const b=el('button','tree-check');
    b.setAttribute('data-key',key);
    if(level===2){
      /* L2 无时间桶（map 侧只回传 L1+残差），显示但禁用 */
      b.disabled=true;
      b.title='L2 子阶段没有时间桶（map 只回传 L1+残差），不能加入曲线；仅用于块图展开';
    }else{
      b.onclick=function(){toggleStageKey(key,cur);};
      b.title=(key==='total_us')?'端到端 total':
        (key==='residual'?'残差 = total − L1 之和':'点击加入/移出阶段曲线对比');
    }
    b.appendChild(el('span','tree-box',on?'✓':''));
    if(level>0){
      const dot=el('span','dot');
      dot.style.background=stageColor(order.indexOf(key));
      b.appendChild(dot);
    }
    b.appendChild(el('span','tree-name',
      level===0?'total':tStageName(key)));
    r.appendChild(b);
    if(level===2) r.appendChild(el('span','tree-note','无时间桶'));
    return r;
  }
  tree.forEach(function(x){
    const level=x[1];
    if(level===2) return;             /* L2 随父行展开渲染 */
    list.appendChild(stageRow(x[0],level));
    if(level===1&&tStageOpen[x[0]]){
      tree.filter(function(y){return y[1]===2&&y[2]===x[0];})
        .forEach(function(y){list.appendChild(stageRow(y[0],2));});
    }
  });
  box.appendChild(list);
}
function renderCurveMetric(){
  const box=document.getElementById('tCurveMetric'); if(!box) return;
  box.innerHTML='';
  [['avg','均值'],['max','最大']].forEach(function(m){
    const b=el('button',tCurveMetric===m[0]?'on':'',m[1]);
    b.title='阶段桶的 '+m[1]+'（map 侧仅保存均值/最大）';
    b.onclick=function(){tCurveMetric=m[0];renderCurveMetric();
      buildStageChart(tPick());};
    box.appendChild(b);
  });
}
function renderTree(cur){
  const box=document.getElementById('ttree'); if(!box) return;
  box.innerHTML='';
  const tree=tTree(cur.g);
  function addRow(key,level,st,label){
    const row=el('div','tnode'+(tSel===key?' sel':''));
    row.style.paddingLeft=(level*22)+'px';
    const hasKids=level===1&&tree.some(function(x){
      return x[1]===2&&x[2]===key;});
    const arrow=el('span','tarrow',hasKids?(tExpand[key]?'▾':'▸'):'');
    if(hasKids) arrow.onclick=function(){
      tExpand[key]=!tExpand[key]; renderTree(cur);};
    row.appendChild(arrow);
    const nm=el('span','tname',label);
    nm.title='点击在右侧查看该阶段的指标套件';
    nm.onclick=function(){tSel=key;renderTree(cur);renderTreeDetail(cur);};
    row.appendChild(nm);
    const pk=(level===0)?'total_us':tParentKey(key,cur);
    const pm=tStat(pk,cur);
    const share=(pm&&pm.avg>0&&st&&st.avg!=null)?st.avg/pm.avg:0;
    const bar=el('span','tbar');
    const fill=el('span','tfill');
    fill.style.width=Math.max(2,Math.min(100,Math.round(share*100)))+'%';
    fill.style.background=lvlStyle(share,key==='residual').background;
    bar.appendChild(fill); row.appendChild(bar);
    row.appendChild(el('span','tpct',share>0?Math.round(share*100)+'%':''));
    const effN=(st&&st.avg_n!=null)?st.avg_n:(st&&st.n);
    row.appendChild(el('span','tval',(effN!=null)?
      (Number(st.avg)||0).toFixed(1)+' µs ('+effN+')':'—'));
    box.appendChild(row);
  }
  addRow('total_us',0,tStat('total_us',cur),'total');
  tree.filter(function(x){return x[1]===1;}).forEach(function(it){
    addRow(it[0],1,tStat(it[0],cur),
      it[0]==='residual'?'残差（total − 阶段和）':it[0]);
    if(tExpand[it[0]]){
      tree.filter(function(x){return x[1]===2&&x[2]===it[0];})
        .forEach(function(ch){addRow(ch[0],2,tStat(ch[0],cur),ch[0]);});
    }
  });
}
let tdetailChart=null;
function renderTreeDetail(cur){
  const box=document.getElementById('tdetail'); if(!box) return;
  box.innerHTML='';
  if(tdetailChart){try{tdetailChart.dispose&&tdetailChart.dispose();}catch(e){}
    tdetailChart=null;}
  const st=tStat(tSel,cur);
  const total=tStat('total_us',cur);
  const label=(tSel==='residual')?'残差':tSel;
  box.appendChild(el('h4','tdetail-title',label+' · 指标套件'));
  if(!st||st.n==null){
    box.appendChild(el('div','note','该阶段无样本'));
    return;
  }
  const g=el('div','grid');
  [['样本数',st.n],['负值(-1) 样本',st.neg],['最小',st.min,'µs'],
   ['平均',st.avg,'µs'],['p50',st.p50,'µs'],['p95',st.p95,'µs'],
   ['p99',st.p99,'µs'],['p999',st.p999,'µs'],
   ['p9999',(st.n>=10000?st.p9999:null),'µs'],
   ['最大',st.max,'µs'],['标准差 σ',st.sigma,'µs']].forEach(function(x){
    if(x[1]==null) return;
    g.appendChild(kv(x[0],x[1],x[2]||''));
  });
  if(total&&total.avg>0&&st.avg!=null)
    g.appendChild(kv('占端到端',(st.avg/total.avg*100).toFixed(1),'%'));
  const pm=tStat(tParentKey(tSel,cur),cur);
  if(pm&&pm.avg>0&&st.avg!=null)
    g.appendChild(kv('占父阶段',(st.avg/pm.avg*100).toFixed(1),'%'));
  box.appendChild(g);
  if(st.hist){
    box.appendChild(el('h4','tdetail-sub','时延分布（对数分箱）'));
    const div=el('div','chart sm');
    box.appendChild(div);
    const keys=Object.keys(st.hist).map(Number)
      .sort(function(a,b){return a-b;});
    tdetailChart=echarts.init(div);
    bindHoverGuards(tdetailChart);
    tdetailChart.setOption({animation:false,
      textStyle:{fontFamily:LFFONT,color:C.fg,fontSize:12},
      tooltip:{trigger:'axis'},grid:{left:56,right:16,top:16,bottom:40},
      xAxis:{type:'category',data:keys,
        axisLabel:{color:C.mut,fontSize:12,fontFamily:LFFONT}},
      yAxis:{type:'value',axisLabel:{color:C.mut,fontSize:12,fontFamily:LFFONT},
        splitLine:{lineStyle:{color:C.grid,width:1}}},
      series:[{type:'bar',data:keys.map(function(k){return st.hist[k];}),
        itemStyle:{color:C.hero,opacity:0.75}}]});
  }
  const hasCurve=(tSel==='total_us'||tSel==='residual'||
    !!(cur.data.fminutes||{})[tSel]);
  if(hasCurve){
    const b=el('button','','加入阶段曲线对比');
    b.style.marginTop='10px';
    b.onclick=function(){
      if(tStageSel.indexOf(tSel)<0) tStageSel.push(tSel);
      renderStageBtns(cur); buildStageChart(cur);
    };
    box.appendChild(b);
  }
}
function stageBreakdownRow(r,opts){
  opts=opts||{};
  /* 单请求「完整」分级时延：
     - 第一行 = 树序全部 L1（含 0/缺失，便于判断其他部分是否正常）+ 残差
     - 其后：每个「有二级子区间且值>0」的 L1 各一行二级展开（含未归因）；
       按占比降序排列（可能有多个主要原因同时展开）
     - 每块 tooltip 附全局均值与倍率（本次值 vs 全局 avg）
     - 压缩：小号灰标签 + 块行横向滚动（.lvl-row）+ 悬浮细节（tooltip） */
  const wrap=el('div','sbrk'+(opts.compact?' compact':''));
  const total=Number(r.total_us)||0;
  const stages=r.stages||{};
  function stAvg(cur,k){
    const st=(k==='residual')?cur.data.residual
      :((cur.data.fields||{})[k]);
    return (st&&st.n)?st.avg:null;
  }
  function blockRow(items,parentV,cur){
    const row=el('div','lvl-row');
    const cols=[];
    items.forEach(function(it){
      const share=parentV>0?(it.v/parentV):0;
      const valTxt=fmtUs(it.v)+'  '+(share*100).toFixed(1)+'%';
      const zeroW=lvlZeroW(it.label,valTxt);
      const minW=lvlBlockMinW(it.label,valTxt);
      cols.push(it.v<=0?zeroW+'px'
        :'minmax('+minW+'px,'+Math.max(share*100,0.001).toFixed(3)+'fr)');
      const st=lvlStyle(share,it.key==='residual');
      const b=el('div','lvl-blk');
      b.style.background=st.background; b.style.color=st.color;
      b.appendChild(el('div','lvl-name',it.label));
      b.appendChild(el('div','lvl-val',
        fmtUs(it.v)+'  '+(share*100).toFixed(1)+'%'));
      const ga=stAvg(cur,it.key);
      let tip=it.label+'：'+fmtUs(it.v)+
        '（占端到端 '+(share*100).toFixed(1)+'%）';
      if(ga!=null&&ga>0)
        tip+='\n全局均值 '+fmtUs(ga)+'（×'+(it.v/ga).toFixed(2)+'）';
      b.title=tip;
      b.style.cursor='default';
      row.appendChild(b);
    });
    row.style.gridTemplateColumns=cols.join(' ');
    return row;
  }
  function reqGroup(){
    /* top 列表是跨组/跨 op 的：按该请求自己的 op 反查所属组，
       不能用当前 tGroup（否则拿错阶段树） */
    const t=traceOf();
    if(t&&r.op){
      for(const g in (t.groups||{})){
        if((((t.groups[g]||{}).ops)||{})[r.op]) return g;
      }
    }
    const pick=tPick();
    return pick?pick.g:(tGroup||'read');
  }
  function render(){
    wrap.innerHTML='';
    const g=reqGroup();
    const t=traceOf();
    const data=((((t||{}).groups||{})[g]||{}).ops||{})[r.op]||{};
    const cur={g:g,op:r.op,data:data};
    const tree=tTree(g);
    const items=tree.filter(function(x){
      return x[1]===1&&x[0]!=='residual';}).map(function(x){
        return {key:x[0],label:x[0],v:Number(stages[x[0]])||0};
      });
    const ssum=items.reduce(function(a,b){return a+b.v;},0);
    items.push({key:'residual',label:'残差',v:Math.max(0,total-ssum)});
    if(!opts.noTotal) wrap.appendChild(blockRow(items,total,cur));
    /* 全部「有二级子区间且有值」的 L1：每个都展开二级（不止一个主要原因） */
    const causes=items.filter(function(it){
      return it.key!=='residual'&&it.v>0&&
        tree.some(function(y){return y[1]===2&&y[2]===it.key;});})
      .sort(function(a,b){return b.v-a.v;});
    causes.forEach(function(c){
      const fv=c.v;
      const kids=tree.filter(function(x){
        return x[1]===2&&x[2]===c.key;}).map(function(x){
          return {key:x[0],label:x[0],v:Number(stages[x[0]])||0};});
      const ks=kids.reduce(function(a,b){return a+b.v;},0);
      if(fv-ks>0.0001)
        kids.push({key:'__unattributed__',label:'未归因',v:fv-ks});
      wrap.appendChild(el('div','lvl-note',
        '二级：'+c.key+'（占端到端 '+
        (total>0?(fv/total*100).toFixed(1):'0.0')+'%）'));
      wrap.appendChild(blockRow(kids,fv>0?fv:1,cur));
    });
  }
  render();
  return wrap;
}
let tStackChart=null, tStackLegendBound=false, tStackLegendLock=false;
let tStackSolo=null;   /* 当前被 solo 的阶段名（null=全部显示） */
function tTop(g){
  /* 慢点追踪：每组各自的 Top-K（reduce 端先按组过滤再截断） */
  const t=traceOf(); if(!t) return [];
  const gg=(t.groups||{})[g];
  return (gg&&gg.top)||[];
}
function renderTraceStack(){
  /* 慢点追踪：柱按端到端时延降序排列（x 标签=时延值，便于看出
     1000ms/500ms 等聚集簇）；柱高=端到端时延，自下而上=一级阶段；
     每个阶段一种颜色；图例点击=solo（只留该阶段），再点还原全部。 */
  const box=document.getElementById('tstackchart'); if(!box) return;
  const t=traceOf(); if(!t){box.style.display='none';return;}
  const g=tGroup;
  const rows=tTop(g).filter(function(r){
    return Number(r.total_us)>0;
  }).sort(function(a,b){return Number(b.total_us)-Number(a.total_us);});
  if(!rows.length){box.style.display='none';return;}
  box.style.display='';
  if(!tStackChart) tStackChart=echarts.init(box);
  bindHoverGuards(tStackChart);
  const tree=tTree(g).filter(function(x){return x[1]===1;});
  const keys=tree.map(function(x){return x[0];});
  function l1sum(r){
    const st=r.stages||{};
    return keys.reduce(function(a,k){
      return k==='residual'?a:a+(Number(st[k])||0);},0);
  }
  function valOf(r,k){
    return k==='residual'
      ?Math.max(0,(Number(r.total_us)||0)-l1sum(r))
      :(Number((r.stages||{})[k])||0);
  }
  const names=keys.map(function(k){
    return k==='residual'?'残差':k;});
  const cats=rows.map(function(r){return fmtUs(r.total_us);});
  const o=axisCommon();
  o.grid={left:64,right:24,top:38,bottom:36};
  o.legend={top:0,type:'scroll',icon:'roundRect',itemWidth:14,itemHeight:3,
    itemGap:14,selectedMode:true,
    textStyle:{color:C.mut,fontSize:12,fontFamily:LFFONT}};
  o.tooltip=lfTooltip({trigger:'axis',axisPointer:{type:'shadow'},
    formatter:function(ps){
      if(!ps||!ps.length) return '';
      const r=rows[ps[0].dataIndex]; if(!r) return '';
      const wl=(r.ts_epoch!=null)?fullts(r.ts_epoch)
        :String(r.start_time||'');
      const lines=[wl+' · '+String(r.op||'')+
        '<br>端到端 '+fmtUs(r.total_us)+
        ((r.node||r.pod)?('<br>'+(r.node||'')+' · '+
          (r.pod||r.client||'')):'')];
      ps.forEach(function(p){
        const v=Number(p.value)||0;
        if(v>0) lines.push(p.marker+String(p.seriesName)+' '+fmtUs(v));
      });
      return lines.join('<br>');
    }});
  o.xAxis={type:'category',data:cats,axisLine:{show:false},
    axisTick:{show:false},splitLine:{show:false},
    axisLabel:{color:C.mut,fontSize:12,fontFamily:LFFONT,
      interval:Math.max(0,Math.floor(cats.length/12)),rotate:0}};
  o.yAxis=[lfAxisValue({name:'µs',scale:true,nameLocation:'end',nameGap:12})];
  o.dataZoom=[];
  o.series=keys.map(function(k,i){
    return {name:(k==='residual'?'残差':k),type:'bar',stack:'stage',
      barMaxWidth:12,itemStyle:{color:stageColor(i)},
      data:rows.map(function(r){return valOf(r,k);})};
  });
  tStackChart.setOption(o,true);
  if(!tStackLegendBound){
    tStackLegendBound=true;
    try{
      tStackChart.on('legendselectchanged',function(ev){
        if(tStackLegendLock) return;
        const all=names.slice();
        const clicked=String((ev&&ev.name)||'');
        if(!clicked) return;
        const target={};
        if(tStackSolo===clicked){
          /* 再次点击同一图例：恢复全部阶段 */
          all.forEach(function(n){target[n]=true;});
          tStackSolo=null;
        }else{
          /* 点击其他图例：只保留该阶段 */
          all.forEach(function(n){target[n]=(n===clicked);});
          tStackSolo=clicked;
        }
        tStackLegendLock=true;
        try{ tStackChart.setOption({legend:{selected:target}}); }catch(e){}
        tStackLegendLock=false;
      });
    }catch(e){}
  }
}
function renderTraceTop(cur){
  const tb=document.querySelector('#ttop tbody'); if(!tb) return;
  tb.innerHTML='';
  const top=tTop(tGroup).filter(function(r){
    return Number(r.total_us)>0;
  });
  top.forEach(function(r){
    const tr=el('tr');
    tr.style.cursor='pointer';
    tr.title='点击展开/收起该请求的全链路时延及占比';
    const cells=[r.start_time,r.trace_id,r.key,r.op,r.status,
      r.node||'-',r.pod||r.client||'-',r.size,r.total_us,
      Object.keys(r.stages||{}).slice(0,3).join(' / ')];
    cells.forEach(function(v,i){
      tr.appendChild(el('td',(i===7||i===8)?'n mono':'mono',
        v==null?'-':String(v)));});
    let open=false, detail=null;
    tr.onclick=function(){
      open=!open;
      if(open){
        if(!detail){
          detail=el('tr');
          const td=el('td'); td.colSpan=10;
          td.appendChild(stageBreakdownRow(r,{noTotal:true,compact:true}));
          detail.appendChild(td);
        }
        tr.parentNode.insertBefore(detail,tr.nextSibling);
      }else if(detail&&detail.parentNode){
        detail.parentNode.removeChild(detail);
      }
    };
    tb.appendChild(tr);
  });
  if(!top.length){
    const tr=el('tr'); const td=el('td'); td.colSpan=10;
    td.textContent='无数据'; tr.appendChild(td); tb.appendChild(tr);
  }
}
function renderTraceStatus(cur){
  const box=document.getElementById('terrs'); if(!box) return;
  box.innerHTML='';
  const st=(cur&&cur.data&&cur.data.status)||{};
  const es=Object.entries(st).sort(function(a,b){return b[1]-a[1];});
  if(!es.length){box.appendChild(el('div','note','无数据'));return;}
  const max=Math.max.apply(null,es.map(function(x){return x[1];}).concat([1]));
  const wrap=el('div','statusbars');
  es.forEach(function(x){
    const row=el('div','sbrow');
    row.appendChild(el('span','sbname mono',x[0]));
    const track=el('span','sbtrack');
    const fill=el('span','sbfill');
    fill.style.width=Math.max(1.5,x[1]/max*100)+'%';
    fill.style.background=(x[0]==='ok'||x[0]==='')?lfColor(1):C.hero;
    track.appendChild(fill); row.appendChild(track);
    row.appendChild(el('span','sbval',String(x[1])));
    wrap.appendChild(row);
  });
  box.appendChild(wrap);
}
function renderBlockObjMenu(){
  /* 分级时延块图对象：树状弹层（总计 / 节点（下挂实例）/ 实例），单选即定并收起 */
  const btn=document.getElementById('tBlockObj');
  const menu=document.getElementById('tBlockObjMenu');
  if(!btn||!menu) return;
  ensureTraceSel();
  btn.textContent=traceObjLabel(tBlockObj)+' ▾';
  btn.title='选择分级时延块图的对象（树状：总计 → 节点 → 实例）';
  menu.innerHTML='';
  const keys=tObjKeys();
  btn.style.display=keys.length>1?'':'none';
  const all=tObjAll();
  const clientsByNode={};
  keys.forEach(function(k){
    const o=all[k]||{};
    if(o.kind==='client'){
      const n=o.node||'';
      (clientsByNode[n]=clientsByNode[n]||[]).push(k);
    }
  });
  function optRow(key,depth){
    const o=all[key]||{};
    const on=(key===tBlockObj);
    const r=el('div','objdrop-row'+(on?' on':''));
    r.style.paddingLeft=(8+depth*16)+'px';
    r.setAttribute('data-key',key);
    const dot=el('span','dot');
    dot.style.background=traceObjColor(key);
    r.appendChild(dot);
    const nm=el('span','objdrop-name',traceObjLabel(key));
    nm.title=String(traceObjLabel(key));
    r.appendChild(nm);
    if(o.kind==='role') r.appendChild(el('span','objdrop-tag','总计'));
    r.onclick=function(){
      tBlockObj=key;
      blockMenuOpen=false;
      renderTrace();
    };
    return r;
  }
  keys.forEach(function(k){
    const o=all[k]||{};
    if(o.kind==='client') return;
    menu.appendChild(optRow(k,0));
    if(o.kind==='node'){
      (clientsByNode[o.node||'']||[]).forEach(function(ck){
        menu.appendChild(optRow(ck,1));
      });
    }
  });
  menu.style.display=blockMenuOpen?'':'none';
  btn.onclick=function(){
    blockMenuOpen=!blockMenuOpen;
    menu.style.display=blockMenuOpen?'':'none';
  };
  if(!blockMenuBound){
    blockMenuBound=true;
    try{
      document.addEventListener('click',function(ev){
        if(!blockMenuOpen||!ev) return;
        let inside=false;
        function walk(n){
          if(!n||inside) return;
          if(n===ev.target){inside=true;return;}
          Array.prototype.slice.call(n.children||[]).forEach(walk);
        }
        walk(menu); walk(btn);
        if(!inside){
          blockMenuOpen=false;
          const m=document.getElementById('tBlockObjMenu');
          if(m)m.style.display='none';
        }
      });
      document.addEventListener('keydown',function(ev){
        if(ev&&(ev.key==='Escape'||ev.keyCode===27)){
          blockMenuOpen=false;
          const m=document.getElementById('tBlockObjMenu');
          if(m)m.style.display='none';
        }
      });
    }catch(e){}
  }
}
function renderTrace(){
  const card=document.getElementById('tracecard'); if(!card) return;
  const keys=traceSizes();
  if(!keys.length){card.style.display='none';return;}
  card.style.display='';
  const t=traceOf();
  const m=(t&&t.meta)||{};
  const rates=Object.keys(m.sample_rates||{});
  let phaseTxt;
  if(m.markers||(m.warmup_s!=null&&Number(m.warmup_s)>0)){
    const pc=m.phase_counts||{};
    phaseTxt=' · 已按阶段剔除：预热 '+(pc.warmup||0)+
      ' / 启动 '+(pc.setup||0)+' / 收尾 '+(pc.teardown||0)+
      '（计入统计 '+(pc.measured||0)+'）';
  }else{
    phaseTxt=' · 无阶段标记（旧客户端），样本未分阶段';
  }
  document.getElementById('tmeta').textContent=
    'trace 文件 '+m.files+' · 解析行 '+m.trace_lines+
    ' · slice 行（仅计数）'+m.slice_lines+' · 解析失败 '+m.parse_errors+
    (rates.length?(' · 采样率 '+rates.join('/')):'')+phaseTxt;
  const gbox=document.getElementById('tgroups'); gbox.innerHTML='';
  tGroupsList().forEach(function(g){
    const b=el('button',g===tGroup?'on':'',
      g==='read'?'读路径':(g==='write'?'写路径':'其它'));
    b.title='SDK trace 的请求方向（读=get 类 / 写=put 类）';
    b.onclick=function(){tGroup=g;tOp='';renderTrace();};
    gbox.appendChild(b);});
  const sel=document.getElementById('topSel');
  const ops=Object.keys((t.groups[tGroup]||{}).ops||{});
  sel.style.display=ops.length>1?'inline-block':'none';
  if(ops.length>1){
    sel.innerHTML='';
    ops.forEach(function(o){const op=el('option',null,o);op.value=o;
      sel.appendChild(op);});
    sel.value=tOp||ops[0];
    sel.onchange=function(){tOp=sel.value;renderTrace();};
  }
  const cur=tPick(); if(!cur) return;
  ensureTraceSel();
  renderBlockObjMenu();
  const bcur=tObjPick(tBlockObj)||cur.data;
  safe('trace-levels',function(){
    renderLevelMetric();
    renderLevels({g:cur.g,op:cur.op,data:bcur});});
  safe('trace-stage-chart',function(){renderStageObjTree();renderStageBtns(cur);
    renderStageTree(cur);renderCurveMetric();buildStageChart(cur);});
  safe('trace-tree',function(){renderTree(cur);renderTreeDetail(cur);});
  safe('trace-top',function(){renderTraceTop(cur);});
  safe('trace-stack',function(){renderTraceStack();});
  safe('trace-status',function(){renderTraceStatus(cur);});
}

window.__MC_SEL=function(){return selObjs.slice();};
window.__MC_TSEL=function(){return tObjSel.slice();};
window.__MC_SSEL=function(){return tStageSel.slice();};
window.__MC_FOCUS=function(){
  return {main:!!(chart&&chart.__mcDimActive),
    stage:!!(tChart&&tChart.__mcDimActive)};
};
window.__MC_FULL_READY__=function(){renderAll();};
document.addEventListener('DOMContentLoaded',mount);
"""


# Runtime link adapter: pages stay standalone (file:// relative links work)
# but when served through the WebUI (/api/file?path=<rel>&mode=raw) every
# relative href resolves against /api/file — breaking ../logs/... links and
# the data.js script. The adapter rewrites them to /api/file URLs.
_PAGE_ADAPTER_JS = """
(function(){
  var base=(window.__MC_REL__||'');
  if(location.protocol==='file:'||!base) return;
  function api(rel){
    var parts=(base+'/'+rel).split('/');
    var out=[];
    for(var i=0;i<parts.length;i++){
      var sg=parts[i];
      if(sg==='..') out.pop();
      else if(sg!=='.'&&sg!=='') out.push(sg);
    }
    return '/api/file?path='+encodeURIComponent(out.join('/'))+'&mode=raw';
  }
  document.querySelectorAll('a[href^="../"]').forEach(function(a){
    /* 传完整相对路径：api() 内的 ".." 弹层负责逐级上行解析。
       曾经 slice(3) 砍掉开头的 ../，导致 overview 的场景报表链接
       解析成 outputs/<ts>/report/logs/... 而 404。 */
    a.href=api(a.getAttribute('href'));
  });
  if(window.__MC_DATA_REL__&&!window.__MC_FULL__){
    var u=api(window.__MC_DATA_REL__);
    fetch(u).then(function(r){return r.ok?r.text():null;})
      .then(function(t){
        if(!t) return;
        (0,eval)(t);
        window.__MC_FULL_READY__&&window.__MC_FULL_READY__();
      }).catch(function(){});
  }
})();
"""


def _page_html(title, subtitle, body_html, data_json, echarts_js, data_js_path,
               full_loader, rel_base=None, data_rel=None, theme_js=""):
    # JSON is embedded as a JS literal; escape the closing-script sequence.
    data_literal = json.dumps(data_json, ensure_ascii=False,
                              separators=(",", ":")).replace("</", "<\\/")
    # echarts/theme（vendored JS）里的字符串可能含 </script>，会提前关闭
    # 标签导致后续内容显示为文本（html 末尾出现 lieflat 注释的事故）
    echarts_js = (echarts_js or "").replace("</", r"<\/")
    theme_js = (theme_js or "").replace("</", r"<\/")
    loader = ""
    if full_loader and data_js_path:
        loader = ('<script src="{}" onerror="window.__mcDataMissing=1">'
                  '</script>'.format(html.escape(str(data_js_path), quote=True)))
    rel_vars = ""
    if rel_base:
        rel_vars = ('<script>window.__MC_REL__ = {}'
                    .format(json.dumps(rel_base)))
        if data_rel:
            rel_vars += (';window.__MC_DATA_REL__ = {}'
                         .format(json.dumps(data_rel)))
        rel_vars += ';</script>\n'
    adapter = '<script>{}</script>\n'.format(_PAGE_ADAPTER_JS.strip())
    return """<!DOCTYPE html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title><style>{css}</style></head><body>
<div class="wrap">
<h1>{title}</h1>
<button id="themeToggle" class="theme-btn" type="button"
  aria-pressed="false" title="当前：自动（跟随系统），点击切换"><span
  class="theme-icon">◑</span></button>
<div class="sub">{subtitle}</div>
{body}
</div>
<script>{echarts}</script>
<script>{theme}</script>
<script>window.__MC_REPORT__ = {data};</script>
{rel_vars}{loader}{adapter}
<script>{app}</script>
<script>if(window.__MC_FULL__){{}}/* placeholder for consistency */</script>
</body></html>
""".format(title=html.escape(str(title), quote=True),
           subtitle=html.escape(str(subtitle), quote=True), body=body_html,
           css=_PAGE_CSS, echarts=echarts_js, data=data_literal,
           theme=theme_js, rel_vars=rel_vars, loader=loader, adapter=adapter,
           app=_APP_JS)


# ---------------------------------------------------------------------------
# 页面 body 模板：basic / pipeline 两套独立模板 + 共享片段
#   需求：docs/spec/report/REQUIREMENTS.md（做什么）
#   规范：.opencode/skills/report-design/SKILL.md（UI 怎么做）
#   JS 单份共享（_APP_JS 内按 D.meta.mode 分支）；body 按 meta.mode 二选一。
#   新增区块：先抽 _SEC_<NAME>，再在两个模板中显式引用。
# ---------------------------------------------------------------------------
_SEC_SUMMARY = """
<div class="card">
<h2>测试概览</h2>
<div class="toolbar">
<label class="tl-label" id="sizeSelLabel" for="sizeSel">数据大小</label>
<select id="sizeSel"></select>
</div>
<div class="note" id="pctNote" style="display:none"></div>
<h3 class="sec" id="secPerf">性能统计</h3>
<div id="perfcards" class="grid"></div>
<div id="rolecards" style="display:none"></div>
<h3 class="sec">参数配置</h3>
<div id="unitmeta" class="grid"></div>
<div id="params" class="params"></div>
</div>
"""
_SEC_LATENCY = """<div class="card"><h2>端到端时延曲线</h2>
<div class="toolbar" id="comboBar" style="display:none">
<span class="tl-label">参数组</span>
<span id="comboBtns" class="toolbar"></span>
</div>
<div class="toolbar" id="latToolbar">
<span id="tierBtns" class="toolbar"></span>
</div>
<div class="objtree-strip">
<div class="objtree-body" id="objTree"></div>
</div>
<div class="lat-sel" id="latSelWrap">
<button id="resetBtn" title="恢复初始缩放范围">重置视图</button>
<button id="outlierBtn">离群点</button>
<span class="tl-label">时延曲线选择</span>
<span id="latSel" class="lat-sel-btns"></span>
<span class="tl-label" title="仅端到端时延图：当前角色（写/读）的全局统计值，不随所选对象变化">全局横线</span>
<span id="globSel" class="lat-sel-btns"></span>
</div>
<div class="chart-frame">
<div class="zslider" id="zslider">
<div class="zslider-track" id="zsliderTrack" title="拖动缩放时延轴：最下=1x 完整视图，最上=最大倍率（放大锚点为窗口下边缘）">
<div class="zslider-fill" id="zsliderFill"></div>
<div class="zslider-handle" id="zsliderHandle"></div>
</div>
<div class="zslider-cap" id="zsliderCap">1.0x</div>
</div>
<div class="yscale" id="yscale">
<div class="yscale-rail" id="yscaleRail" title="时延轴窗口指示：拖动平移时延轴（与图上纵向拖动等效）">
<div class="yscale-win" id="yscaleWin" title="拖动平移时延轴"></div>
</div>
</div>
<div class="chart-canvas">
<div id="mainchart" class="chart" role="img" aria-label="端到端时延曲线图"></div>
<div id="endlabels" class="endlabels"></div>
<div id="xpan" class="xpan" title="拖动平移时间轴（缩放请用滚轮）">
<div id="xpanWin" class="xpan-win" title="拖动平移时间轴"></div>
</div>
</div>
</div>
<div id="qpschart" class="chart qps" role="img" aria-label="QPS 与失败副图"></div>
<div class="note" id="qpsNote"></div>
<div class="note">浅色竖带 = 无数据窗口（仅首处标注时长，其余同色带同样含义）；
红色竖线 = 异常窗口（错误突发/时延突增，非离群点）；离群点为散点 + 旗杆虚线（无可命中离群时按钮置灰）</div>
<div class="chart-hint">滚轮 = 缩放时间轴 · 左右拖动 = 平移x · 上下拖动 = 平移y ·
左侧刻度轨：拖动调倍率（最下 1x）</div>
</div>
"""
_SEC_HIST = """<div class="card" id="histcard" style="display:none"><h2>时延分布</h2>
<div class="sub">p9999 需样本 ≥ 10000</div>
<div id="histchart" class="chart sm" role="img" aria-label="时延分布直方图"></div></div>
"""
_SEC_SLOW = """<div class="card" id="slowcard" style="display:none"><h2>慢点捕捉</h2>
<div class="sub">每条 = 一次慢操作（Top-100）；总计 = 该角色全部慢点（灰），
勾选节点/实例后高亮叠加</div>
<div id="slowctl" class="slow-ctl"></div>
<div id="slowsum" class="slow-sum"></div>
<div id="slowchart" class="chart sm" role="img" aria-label="慢点捕捉条带图"></div>
<div class="note">时间轴与端到端时延曲线同窗口（缩放独立）；摘要条 = 各节点慢点次数
与该节点自身 p99</div></div>
"""
_SEC_TRACE = """<div class="card" id="tracecard" style="display:none">
<h2>SDK 全链路（C++ trace）</h2>
<div class="sub" id="tmeta"></div>
<div class="toolbar"><span id="tgroups" class="toolbar"></span>
<select id="topSel" style="display:none"></select></div>
<h3 class="sec" title="块宽 = 占父项百分比；点击块聚焦，下一级在新行展开">分级时延块图（trace）</h3>
<div class="toolbar blockobj-row"><span class="tl-label">对象</span>
<span class="objdrop-wrap"><button id="tBlockObj" class="objdrop-btn" type="button">总计 ▾</button>
<div id="tBlockObjMenu" class="objdrop-menu" style="display:none"></div></span></div>
<div class="toolbar blockobj-row"><span class="tl-label">指标</span>
<span id="lvlMetric" class="toolbar"></span></div>
<div id="levels" class="levels"></div>
<h3 class="sec" title="阶段 × 指标 × 对象：单对象可多阶段，单阶段可多对象">分级时延曲线（trace）</h3>
<div class="toolbar" id="tLatToolbar">
<span id="stageTierBtns" class="toolbar"></span>
<span class="tl-label" style="margin-left:16px">指标</span>
<span id="tCurveMetric" class="toolbar"></span>
<button id="tResetBtn" title="恢复缩放窗口">重置视图</button>
</div>
<div class="trace-cols">
<div class="objtree-strip">
<div class="objtree-body" id="stageObjTree"></div>
</div>
<div class="objtree-strip">
<div class="objtree-body" id="stageTree"></div>
</div>
</div>
<div id="tchartEmpty" class="empty-tip" style="display:none"></div>
<div class="chart-frame">
<div class="zslider" id="tZslider">
<div class="zslider-track" id="tZsliderTrack" title="拖动缩放时延轴：最下=1x 完整视图，最上=最大倍率">
<div class="zslider-fill" id="tZsliderFill"></div>
<div class="zslider-handle" id="tZsliderHandle"></div>
</div>
<div class="zslider-cap" id="tZsliderCap">1.0x</div>
</div>
<div class="yscale" id="tYscale">
<div class="yscale-rail" id="tYscaleRail" title="时延轴窗口指示：拖动平移时延轴">
<div class="yscale-win" id="tYscaleWin" title="拖动平移时延轴"></div>
</div>
</div>
<div class="chart-canvas">
<div id="tchart" class="chart" role="img" aria-label="分级时延曲线图"></div>
<div id="tEndlabels" class="endlabels"></div>
<div id="tXpan" class="xpan" title="拖动平移时间轴（缩放请用滚轮）">
<div id="tXpanWin" class="xpan-win" title="拖动平移时间轴"></div>
</div>
</div>
</div>
<div class="note">分级时延曲线是「阶段 × 指标 × 对象」三重维度：单对象可多阶段对比，
单阶段可多对象对比（多选一侧时另一侧自动收敛为单选）；指标为均值/最大
（map 侧阶段桶只保存这两项）。时间桶档位按跨度提供；节点/实例阶段数据
在预算内嵌，完整数据见 data.js。</div>
<h3 class="sec" style="display:none" title="点击节点，右侧显示该阶段指标套件">阶段树</h3>
<div class="stage-split" style="display:none">
<div class="ttree" id="ttree"></div>
<div class="tdetail" id="tdetail"></div>
</div>
<h3 class="sec" title="按端到端时延降序（Top-N），点击行展开该请求的阶段构成">慢点追踪（trace）</h3>
<div class="sub">柱按端到端时延从最慢到较慢排列（横轴标签=时延值，可看出聚集簇）；
柱高=端到端时延，自下而上=一级阶段；每个阶段一种颜色；点击图例只保留该阶段，
再点还原全部。</div>
<div id="tstackchart" class="chart" style="height:210px" role="img" aria-label="慢请求阶段构成堆叠直方图"></div>
<div class="wrapx"><table id="ttop"><thead><tr>
<th>start_time</th><th>trace_id</th><th>key</th><th>op</th><th>状态</th>
<th>节点</th><th>实例</th>
<th class="n">size</th><th class="n">total (µs)</th><th>主要阶段</th>
</tr></thead><tbody></tbody></table></div>
<div class="note">说明：① 慢点表 Top-N（当前 100）按端到端时延降序；② 采样门控——主行仅在采样命中或失败时出现；③ put/upsert 的
memwrite_* 与 slice 明细仅代表最后一个副本的传输（memwrite_total_us 为全部副本合计）；
③ `-1` 表示该阶段拿不到时间戳（不参与时延统计）；④ slice 明细行 P1 仅计数。</div>
</div>
<pre class="term" style="display:none"></pre>
"""

_SEC_ERRORS = """<div class="card"><h2>错误与异常</h2><div id="errors"></div>
<h3 class="sec">状态分布</h3><div id="terrs"></div></div>
"""
_SEC_CLIENTSITE = """<div class="card" id="clientsite"><h2>日志现场</h2>
<div id="logsite"></div></div>
"""
_SEC_VOLUME = """<div class="card" id="volumecard"><h2>数据量与优化</h2>
<div class="sub" id="volumeNote"></div>
<div id="volume"></div></div>
"""
_SEC_FOOTER = """<div class="note" id="pipelineNote">
<b>数据文件</b>：report.html（自包含，数据内嵌，可离线打开）+ data.js（全量，
深查用；经 WebUI 打开时自动加载）。<br>
<b>生成流程（map → reduce）</b>：测试结束后每节点本地运行 map（logstats 解析
RESULT 日志、tracestats 解析 C++ trace），把 1 秒级聚合（计数/时延和/最大/
直方图/离群与慢点清单/阶段秒桶）写入节点产物；编排机回拉后由 report.py
reduce 按对象（总体/角色/节点/实例）与档位（1s/10s/1min/自适应）合并，
生成 report.html + data.js。<br>
<b>数据正确性</b>：总计 = 各节点同一时间桶按请求数加权平均（avg = Σ时延/Σ请求数，
max 取各节点最大）；分位数在 map 侧按直方图归并后计算；时延直方图为全量样本合并；
离群值（单请求 &gt; 客户端 p99×10）不计入桶的 avg/max，但计入计数与直方图。
跨节点时间轴以各节点日志的绝对时刻对齐（时钟偏差问题见后续批次）。</div>
"""
_MONITOR_CARD = """
<div class="card" id="monitorcard"><h2>节点监控<span id="monTitleSuffix">（内存 / URMA 端口）</span></h2>
<div id="moncards" class="grid"></div>
<h3 class="sec" id="growthTitle">进程 RSS 增幅</h3>
<div id="growth" class="grow" style="display:none"></div>
<div id="memchart" class="chart" role="img" aria-label="节点宿主内存图"></div>
<div id="procchart" class="chart" role="img" aria-label="进程 RSS 内存图"></div>
<div id="urmachart" class="chart" role="img" aria-label="URMA 端口流量图"></div>
<div class="note" id="monNote">整机内存来自 numastat（NUMA 分列），进程内存来自
/proc/&lt;pid&gt;/smaps_rollup；<span id="monUrmaNote">URMA 端口为 mdcli flit 计数器差分（GiB/s，1024 进制）。</span>
时间轴与上方延迟图同源，可对照异常窗口；进程 RSS 持续增长会被标记「疑似泄露」。</div></div>
"""

_BODY_BASIC = "<!-- tpl:basic -->\n" + "".join([
    _SEC_SUMMARY, _SEC_LATENCY, _SEC_HIST, _SEC_SLOW,
    _SEC_TRACE, _SEC_ERRORS, _SEC_CLIENTSITE, "<!-- monitor -->",
    _SEC_VOLUME, _SEC_FOOTER])
_BODY_PIPELINE = "<!-- tpl:pipeline -->\n" + "".join([
    _SEC_SUMMARY, _SEC_LATENCY, _SEC_HIST, _SEC_SLOW,
    _SEC_TRACE, _SEC_ERRORS, _SEC_CLIENTSITE, "<!-- monitor -->",
    _SEC_VOLUME, _SEC_FOOTER])



def render_scenario(data, out_dir, echarts_path, topk=TOPK_DEFAULT,
                     rel_base=None):
    """Write report.html (self-contained) + data.js (full per-client data)."""
    os.makedirs(out_dir, exist_ok=True)
    meta = data.get("meta") or {}
    size_keys = list((data.get("sizes") or {}).keys())
    # compact copy for the HTML: only top-K clients carry minute series
    embedded = {"meta": meta, "sizes": {}, "nodes": data.get("nodes") or [],
                "sources": data.get("sources") or [],
                "logsite": data.get("logsite") or [],
                "monitors": compact_monitors(data.get("monitors") or {})}
    for size, z in (data.get("sizes") or {}).items():
        clients = sorted(z.get("clients") or [],
                         key=lambda c: -float(c.get("p99_ms") or 0))
        keep = clients[:topk]
        slim = dict(z)
        if z.get("trace"):
            # SDK 区的按对象阶段数据：总计全嵌，节点/实例抽稀+预算
            slim["trace"] = _trim_trace_embed(z["trace"])
        slim["clients"] = keep
        # 段2：对象预算裁剪（角色全嵌 -> 节点按日志量 -> Top 客户端），
        # 单点行预算 EMBED_ROW_BUDGET；其余对象留在 data.js
        emb = set(z.get("embed_tiers") or [])
        objs = z.get("objects") or {}
        budget = EMBED_ROW_BUDGET
        emb_objs = {}

        def _take(key):
            nonlocal budget
            o = objs.get(key)
            if not o:
                return False
            tiers = {k: v for k, v in (o.get("tiers") or {}).items()
                     if k in emb}
            rows = _tier_rowcount(tiers)
            if not rows or rows > budget:
                return False
            budget -= rows
            slim_o = {"label": o.get("label"), "kind": o.get("kind"),
                      "tiers": tiers}
            for k in ("node", "bytes", "p99_ms", "role"):
                if k in o:
                    slim_o[k] = o[k]
            emb_objs[key] = slim_o
            return True

        for key in ("all", "writer", "reader"):
            _take(key)
        nodes = sorted((k for k in objs if k.startswith("node:")),
                       key=lambda k: -(objs[k].get("bytes") or 0))
        for k in nodes:
            _take(k)
        for c in keep:
            _take("client:" + c["name"])
        slim["objects"] = emb_objs
        slim["object_total"] = len(objs)
        embedded["sizes"][size] = slim
    meta_bits = []
    if meta.get("started"):
        meta_bits.append("测试开始于 " + str(meta["started"]))
    meta_bits.append("报告生成于 " + time.strftime("%Y-%m-%d %H:%M:%S"))
    if meta.get("protocol"):
        meta_bits.append("协议 " + str(meta["protocol"]))
    meta_bits.append("引擎 " + _engine_of(meta))
    if meta.get("early_stop"):
        meta_bits.append("⚠ 提前停止/异常，数据不完整")
    subtitle = " · ".join(meta_bits)
    is_pipe = _scenario_of(meta) == "pipeline"
    body = _BODY_PIPELINE if is_pipe else _BODY_BASIC
    mon = data.get("monitors") or {}
    body = body.replace("<!-- monitor -->",
                        _MONITOR_CARD
                        if (mon.get("mem_host") or mon.get("mem_proc")
                            or mon.get("urma") or mon.get("notes")) else "")
    # R-SHELL-11：title/h1 携带场景名与单元身份（meta.unit，如「c4×t8 · 128KB」）；
    # 报告生成时间已由副标题承载，不再占据页面最高层级（多标签页对照时
    # 同名不可区分的问题由此修复——pipeline 单元目录仅 size 一级时尤甚）。
    _ident = " · ".join(x for x in (
        str(meta.get("config") or ""),
        str(meta.get("unit") or "")) if x)
    _base_name = "Pipeline 场景报表" if is_pipe else "场景报表"
    page_title = (_base_name + " · " + _ident) if _ident else _base_name
    # 数据量卡：输出文件大小要在页面内显示 —— 先产 data.js（首轮测长），
    # 再两遍渲染 report.html（大小写回页面）
    meta["volume_out"] = {"data_js": 0, "report_html": 0}
    _prefix = "window.__MC_FULL__ = "
    full_literal = ""
    for _ in range(6):
        full_literal = json.dumps(data, ensure_ascii=False,
                                  separators=(",", ":")).replace("</", "<\\/")
        _n = len((_prefix + full_literal + ";\n").encode("utf-8"))
        if _n == meta["volume_out"]["data_js"]:
            break
        meta["volume_out"]["data_js"] = _n
    echarts_js = _read_echarts(echarts_path)
    html = _page_html(page_title,
                      subtitle, body, embedded, echarts_js,
                      "data.js", True, rel_base=rel_base,
                      data_rel="data.js", theme_js=_lieflat_tokens_js())
    for _ in range(4):
        _h = len(html.encode("utf-8"))
        if _h == meta["volume_out"]["report_html"]:
            break
        meta["volume_out"]["report_html"] = _h
        html = _page_html(page_title,
                          subtitle, body, embedded, echarts_js,
                          "data.js", True, rel_base=rel_base,
                          data_rel="data.js", theme_js=_lieflat_tokens_js())
    html_path = os.path.join(out_dir, "report.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    full_js = os.path.join(out_dir, "data.js")
    with open(full_js, "w", encoding="utf-8") as f:
        f.write(_prefix + full_literal + ";\n")
    return html_path, full_js


def render_overview(run_meta, scenarios, out_dir, rel_base=None):
    """run 级总览：**每配置一卡**（跨单元聚合口径）+ 卡内单元报表链接列表
    + 节点日志量。scenarios 的每一项是「单元」（一个 size×combo），
    sc["unit"] 是单元标签（如 "c4×t8 · 128KB"）。"""
    os.makedirs(out_dir, exist_ok=True)
    groups = {}
    order = []
    for sc in scenarios:
        key = (sc.get("meta") or {}).get("config") or "scenario"
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(sc)
    cards = []
    for cfg_name in order:
        units = groups[cfg_name]
        m = units[0].get("meta") or {}
        # 跨单元聚合：ok/fail/bytes/total 求和，时长求和（单元顺序执行），
        # 分位数取最差单元（并注明单元数）。
        blocks = [z.get("summary")
                  for u in units
                  for z in (u.get("sizes") or {}).values()
                  if z and z.get("summary")]
        if blocks:
            s = {
                "ok": sum(int(b.get("ok") or 0) for b in blocks),
                "fail": sum(int(b.get("fail") or 0) for b in blocks),
                "total": sum(int(b.get("total") or 0) for b in blocks),
                "bytes": sum(int(b.get("bytes") or 0) for b in blocks),
                "wall_s": sum(float(b.get("wall_s") or 0) for b in blocks),
            }
            s["fail_rate"] = (s["fail"] / s["total"]) if s["total"] else 0.0
        else:
            s = {}
        # 监控摘要跨单元聚合（每个单元只带自己时间窗的数据）；
        # 峰值内存逐 pod 名单过长，不写入 overview（见 DR-43）
        worst_proc, peak_urma = None, None
        for u in units:
            ms = (u.get("monitors") or {}).get("summary") or {}
            wp = ms.get("worst_proc")
            if wp and (worst_proc is None or
                       (wp.get("growth_mb") or 0) >
                       (worst_proc.get("growth_mb") or 0)):
                worst_proc = wp
            pu = ms.get("peak_urma")
            if pu and pu.get("value") is not None and (
                    peak_urma is None or pu["value"] > peak_urma["value"]):
                peak_urma = pu
        bits = []
        if worst_proc and worst_proc.get("leak_suspect"):
            bits.append('<span class="tag warn">RSS 疑似泄露 {} ({} MB / {}%)'
                        "</span>".format(worst_proc.get("label") or "",
                                         worst_proc.get("growth_mb"),
                                         worst_proc.get("growth_pct")))
        if peak_urma:
            bits.append("端口峰值 {} GiB/s".format(peak_urma["value"]))
        monline = ('<div class="note">监控：' + " · ".join(bits) +
                   "</div>") if bits else ""
        # 单元链接列表（一个 size×combo 一份报表）
        ulinks = []
        for u in units:
            rel = u.get("report_rel") or ""
            lab = u.get("unit") or ", ".join(
                str(k) for k in (u.get("sizes") or {})) or "报表"
            ulinks.append(
                '<a href="{}" target="_blank" rel="noopener">{}</a>'.format(
                    html.escape(str(rel), quote=True),
                    html.escape(str(lab), quote=True)))
        link_block = ('<div class="ulinks">' + "".join(ulinks) +
                      "</div>") if ulinks else ""
        sizes = sorted({k for u in units
                        for k in (u.get("sizes") or {}) if k != "all"})
        name = html.escape(str(cfg_name))
        mode = html.escape(str(m.get("mode") or ""))
        cards.append("""
<div class="card"><h2>{name}</h2>
<div class="sub">{mode} · {proto} · {sizes} · 时长 {dur} {early}</div>
<div class="grid">
<div class="kv"><div class="k">成功</div><div class="v">{ok}</div></div>
<div class="kv"><div class="k">失败</div><div class="v">{fail}</div></div>
<div class="kv"><div class="k">失败率</div><div class="v">{fr}<span class="u"> %</span></div></div>
</div>
{monline}
{linkblock}
</div>""".format(
            name=name, mode=mode,
            proto=html.escape(str(m.get("protocol") or "-")),
            sizes=html.escape(", ".join(sizes) or "-", quote=True),
            dur=_fmt_dur(s.get("wall_s") or 0),
            early='<span class="tag warn">提前停止/异常</span>'
            if m.get("early_stop") else "",
            ok=s.get("ok", "-"), fail=s.get("fail", "-"),
            fr=round((s.get("fail_rate") or 0) * 100, 2),
            linkblock=link_block, monline=monline))
    nodes = run_meta.get("nodes") or []
    node_rows = "".join(
        "<tr><td class='mono'>{n}</td><td class='n'>{p}</td>"
        "<td class='n'>{b}</td></tr>".format(
            n=html.escape(str(n.get("node")), quote=True), p=n.get("pods", ""),
            b=_fmt_bytes(n.get("bytes")))
        for n in nodes)
    body = "".join(cards)
    if node_rows:
        _root = str(run_meta.get("host_log_root") or "").rstrip("/")
        _run = str(run_meta.get("run_id") or run_meta.get("run") or "")
        _rdir = str(run_meta.get("remote_log_dir") or "").rstrip("/")
        _ns = html.escape(str(run_meta.get("k8s_namespace") or ""), quote=True)
        if _root and _run:
            _where = html.escape("{}/{}/<pod>/".format(_root, _run),
                                 quote=True)
            _note = ("现场日志（原始 log/glog/ylt 不拉回，每 pod 一目录，"
                     "同目录含 .log/.ylt.log/.exitrc 与 MC_LOG_DIR 的 "
                     "<base>.glog.d/（glog 分段）；"
                     "CLEAN-UP 不删除）：{}。登录节点 ssh &lt;节点IP&gt;；"
                     "进入 pod：kubectl exec -it &lt;pod&gt; -n {} -- bash"
                     .format(_where, _ns or "&lt;ns&gt;"))
        elif _rdir:
            _where = html.escape(_rdir + "/", quote=True)
            _note = ("远端现场日志（原始 log/glog/ylt 不拉回）：{}。"
                     "登录节点 ssh &lt;节点IP&gt; 查看".format(_where))
        else:
            _note = ("宿主机日志：&lt;host_log_root&gt;/&lt;run_id&gt;/&lt;pod&gt;/"
                     "（CLEAN-UP 不删除）。")
        body += ("<div class='card'><h2>节点日志量</h2><div class='wrapx'>"
                 "<table><thead><tr><th>节点</th><th class='n'>客户端数</th>"
                 "<th class='n'>日志量</th></tr></thead><tbody>" + node_rows +
                 "</tbody></table></div><div class='note'>" + _note +
                 "</div></div>")
    subtitle = " · ".join(filter(None, [
        run_meta.get("run"), run_meta.get("mode"),
        ("开始 " + run_meta["started"]) if run_meta.get("started") else "",
        "生成于 " + time.strftime("%Y-%m-%d %H:%M:%S")]))
    page = _page_html("运行总览", subtitle, body, {"meta": run_meta},
                      "", None, False, rel_base=rel_base,
                      theme_js=_lieflat_tokens_js())
    path = os.path.join(out_dir, "overview.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(page)
    return path


def _fmt_dur(s):
    s = max(0, int(round(float(s or 0))))
    h, m = divmod(s, 3600)
    m, sec = divmod(m, 60)
    return ("{}h".format(h) if h else "") + \
        ("{}m".format(m) if m else "") + "{}s".format(sec)


def _fmt_bytes(n):
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return "{:.1f} {}".format(n, unit) if unit != "B" else "{:.0f} B".format(n)
        n /= 1024.0
    return "{:.1f} TB".format(n)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Build HTML reports from logstats JSONs")
    ap.add_argument("--stats", nargs="+", required=False)
    ap.add_argument("--out", required=False)
    ap.add_argument("--echarts", default=os.path.join(
        os.path.dirname(_HERE), "webui", "web", "vendor", "echarts.min.js"))
    ap.add_argument("--config", default="")
    ap.add_argument("--mode", default="")
    ap.add_argument("--topk", type=int, default=TOPK_DEFAULT)
    ap.add_argument("--reduce-only", action="store_true",
                    help="stop after reduce_scenario and write the reduced "
                         "data JSON to --out-reduced (run this stage next "
                         "to the map products; the renderer runs wherever "
                         "the orchestrator lives)")
    ap.add_argument("--out-reduced", default=None,
                    help="--reduce-only output path (required with it)")
    ap.add_argument("--render-only", action="store_true",
                    help="render report.html/data.js from a reduced data "
                         "JSON produced by --reduce-only (no stats needed)")
    ap.add_argument("--reduced", default=None,
                    help="reduced data JSON to render (--render-only)")
    args = ap.parse_args(argv)

    if args.render_only:
        if not args.reduced or not args.out:
            sys.exit("[ERROR] --render-only needs --reduced and --out")
        with open(args.reduced, "r", encoding="utf-8") as f:
            data = json.load(f)
        html, datajs = render_scenario(data, args.out, args.echarts,
                                       topk=args.topk)
        print("[report] {} (+ {})".format(html, os.path.basename(datajs)))
        return 0

    if not args.stats or not args.out:
        sys.exit("[ERROR] need --stats and --out (or --render-only)")
    stats = load(args.stats)
    if not stats:
        sys.exit("[ERROR] no readable stats files")
    data = reduce_scenario(stats, {"config": args.config, "mode": args.mode})
    if args.reduce_only:
        if not args.out_reduced:
            sys.exit("[ERROR] --reduce-only needs --out-reduced")
        tmp = args.out_reduced + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, args.out_reduced)
        print("[report] reduced -> {}".format(args.out_reduced))
        return 0
    html, datajs = render_scenario(data, args.out, args.echarts,
                                   topk=args.topk)
    print("[report] {} (+ {})".format(html, os.path.basename(datajs)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
