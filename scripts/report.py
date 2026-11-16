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
import html
import json
import os
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
import logstats as ls   # noqa: E402  (histogram ladder + percentile shared)

EMBED_SECONDS_MAX = 86400        # ≤1 天：1s 桶内嵌；否则内嵌 1min
SCATTER_MAX_POINTS = 200000      # per-client raw scatter cap (small runs)
TOPK_DEFAULT = 20


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
            out.append(st)
    return out


def _role_of(st, fallback="all"):
    return (st.get("meta") or {}).get("role") or fallback


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
TIER_STEPS = [1, 10, 60, 600, 1800, 3600, 10800, 21600, 86400]
TIER_LABELS = {1: "1 秒", 10: "10 秒", 60: "1 分钟", 600: "10 分钟",
               1800: "30 分钟", 3600: "1 小时", 10800: "3 小时",
               21600: "6 小时", 86400: "1 天"}
COUNT_KEY = "cnt"
COUNT_LABEL = "自适应（按样本数）"
MIN_TIER_BUCKETS = 8          # 少于 8 个桶的粒度没有意义，不提供
EMBED_POINTS_MAX = 2048       # 每序列嵌入点数上限（Grafana/uPlot 同量级）
EMBED_ROW_BUDGET = 150000     # report.html 内嵌点行总预算
DATAJS_AGG_MAX = 100000       # data.js 单个聚合序列点数上限
DATAJS_ROW_BUDGET = 2000000   # data.js 点行总预算
CLIENT_MIN_POINTS = 128
CLIENT_MAX_POINTS = 2048
COUNT_BUCKETS_MAX = 1024
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


def _count_target(total):
    """Adaptive sample-count bucketing: ~8 samples/bucket, 64..1024 buckets."""
    try:
        t = int(total / 8)
    except (TypeError, ValueError):
        t = 0
    return max(64, min(COUNT_BUCKETS_MAX, t or 64))


def _count_rows(rows, target, target_points=None):
    """Equal-sample buckets: accumulate 1s rows until ~total/target samples,
    key each bucket by its LAST sample time (横轴仍是时间轴)."""
    pts = target_points or target
    ordered = sorted(rows or [], key=lambda r: int(r[0]))
    total = sum(int(r[1]) for r in ordered)
    if not ordered or total <= 0 or pts <= 0:
        return {}
    per = max(1, int(round(total / float(pts))))
    out = {}
    acc = None
    for row in ordered:
        if acc is None:
            acc = _new_bucket()
        _acc_bucket(acc, row)
        if acc[0] >= per:
            out[int(row[0])] = acc
            acc = None
    if acc is not None and acc[0] > 0:
        out[int(ordered[-1][0])] = acc
    return out


def _tier_policy(span):
    """(offered steps, embed step) for a run span in seconds.

    Offered tiers need >= MIN_TIER_BUCKETS buckets; the embed tier is the
    finest one with <= EMBED_POINTS_MAX buckets (coarser tiers are cheap and
    embedded too). 30s runs offer 1s only; 24h runs never offer 1s/10s."""
    span = float(span or 0)
    if span <= 0:
        return [1, 60], 1
    offered = [s for s in TIER_STEPS if span / s >= MIN_TIER_BUCKETS]
    if not offered:
        offered = [1]
    embed = next((s for s in offered if span / s <= EMBED_POINTS_MAX),
                 offered[-1])
    return offered, embed


def _series_columns(merged, step, edges=None, with_span=False):
    """Columnar series for the chart (compact JSON, fixed key order).
    `with_span` adds per-bucket spans (needed for sample-count buckets)."""
    edges = ls.BIN_EDGES if edges is None else edges
    out = {"t": [], "c": [], "ok": [], "fail": [], "avg": [], "p50": [],
           "p99": [], "p999": [], "max": [], "qw": [], "qwmax": []}
    if with_span:
        out["span"] = []
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
        out["max"].append(round(mx, 4))
        # logstats accumulates queue wait on successful rows only -> divide
        # by ok (not by every row, which understated it by the failure rate)
        out["qw"].append(round(qs / ok, 4) if ok else 0.0)
        out["qwmax"].append(round(qm, 4))
        if with_span:
            span = (t - prev_t) if prev_t is not None and t > prev_t else None
            out["span"].append(int(span) if span else None)
        prev_t = t
    return out


def _summary_of(stats):
    """Cluster summary from merged per-instance globals."""
    total = ok = fail = ok_rows = 0
    lat_sum = 0.0
    qw_sum = 0.0
    qw_n = 0
    qw_max = 0.0
    total_bytes = 0
    max_depth = 0
    ts_from = ts_to = None
    hist = {}
    wall = 0.0
    gmax = gmin = None
    single = None
    for st in stats:
        g = st.get("global") or {}
        meta = st.get("meta") or {}
        if g.get("max_ms") is not None:
            gmax = float(g["max_ms"]) if gmax is None else max(gmax,
                                                               float(g["max_ms"]))
        if g.get("min_ms") is not None:
            gmin = float(g["min_ms"]) if gmin is None else min(gmin,
                                                               float(g["min_ms"]))
        if len(stats) == 1:
            single = g
        total += g.get("key_total") or 0
        ok += g.get("key_ok") or 0
        fail += g.get("key_fail") or 0
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
        for b, n in (st.get("histogram") or {}).items():
            hist[int(b)] = hist.get(int(b), 0) + n
        for kind, payload in st.get("summaries") or []:
            if kind != "summary":
                continue
            max_depth = max(max_depth, int(payload.get("max_depth", 0) or 0))
            total_bytes += int(payload.get("bytes", 0) or 0)
            wall = max(wall, float(payload.get("wall_s", 0) or 0))
    span = (ts_to - ts_from) if (ts_from is not None and ts_to is not None) else 0.0
    wall = wall or span

    edges = _hist_edges(stats)

    def pct(q, key):
        # single client: the map product already holds EXACT percentiles (the
        # CSV uses the same values); otherwise reconstruct from the merged
        # histogram and never report more than the true maximum.
        if single is not None and single.get("exact") and single.get(key) is not None:
            return round(float(single[key]), 4)
        if not hist:
            return 0.0
        v = ls.hist_percentile(hist, q, edges)
        return round(min(v, gmax) if gmax is not None else v, 4)

    return {
        "total": total, "ok": ok, "fail": fail,
        "fail_rate": (fail / total) if total else 0.0,
        "avg_ms": round(lat_sum / ok_rows, 4) if ok_rows else 0.0,
        "min_ms": round(gmin, 4) if gmin is not None else 0.0,
        "p50_ms": pct(0.50, "p50_ms"),
        "p95_ms": pct(0.95, "p95_ms"),
        "p99_ms": pct(0.99, "p99_ms"),
        "p999_ms": pct(0.999, "p999_ms"),
        "p9999_ms": pct(0.9999, "p9999_ms"),
        "max_ms": round(gmax, 4) if gmax is not None else 0.0,
        "qw_avg_ms": round(qw_sum / qw_n, 4) if qw_n else None,
        "qw_max_ms": round(qw_max, 4) if qw_n else None,
        "max_depth": max_depth,
        "wall_s": round(wall, 3),
        "qps": round(total / wall, 2) if wall > 0 else 0.0,
        # None when the logs carry no timestamps (legacy) -> the card shows
        # "-" instead of Infinity (JS must not divide by a zero wall)
        "bytes": total_bytes,
        "throughput_mb_s": (round(total_bytes / wall / 1048576.0, 6)
                            if wall > 0 else None),
        "clients": len(stats),
        "ts_from": ts_from, "ts_to": ts_to,
    }


def _client_entry(st, role):
    g = st.get("global") or {}
    meta = st.get("meta") or {}
    return {
        "name": os.path.basename(str(meta.get("source") or "")).replace(
            ".log", ""),
        "node": meta.get("node") or "",
        "pod": meta.get("pod") or "",
        "role": role,
        "source": meta.get("source") or "",
        "remote_path": meta.get("remote_path") or "",
        "bytes": meta.get("bytes") or 0,
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


def _obj_tiers(rows, edges, offered, min_step, count_points):
    """Build the tier dict for one chart object.

    min_step: finest time tier to materialise (= embed tier for nodes,
    data.js allowance for roles). count_points: bucket count for the
    sample-count tier (0 disables it)."""
    tiers = {}
    for s in offered:
        if s < min_step:
            continue
        tiers[str(s)] = _series_columns(_tier_rows(rows, s), s, edges)
    if count_points:
        tiers[COUNT_KEY] = _series_columns(
            _count_rows(rows, count_points), 1, edges, with_span=True)
    return tiers


def _tier_rowcount(tiers):
    return sum(len((cols or {}).get("t") or []) for cols in (tiers or {}).values())


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
        for row in (st.get("slowest") or []):
            slowest.append(row)
    slowest.sort(key=lambda r: -float(r.get("lat_ms") or 0))
    slowest = slowest[:100]
    anomalies = []
    seen = set()
    for st in stats:
        for a in st.get("anomalies") or []:
            key = (a.get("t"), a.get("kind"))
            if key in seen:
                continue
            seen.add(key)
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
    span = 0.0
    if summary.get("ts_from") is not None and summary.get("ts_to") is not None:
        span = float(summary["ts_to"]) - float(summary["ts_from"])
    offered, embed = _tier_policy(span)
    edges = _hist_edges(stats)
    # roles may carry the finer tiers in data.js when affordable
    fine_roles = offered[0]
    if span > 0:
        fine_roles = next((s for s in offered
                           if span / s <= DATAJS_AGG_MAX), offered[-1])
    objects = {}
    used = 0
    for role, group in sorted(role_stats.items()):
        rows = _obj_seconds(group)
        tiers = _obj_tiers(rows, edges, offered, fine_roles,
                           _count_target(_samples(rows)))
        used += _tier_rowcount(tiers)
        objects[role] = {"label": _role_label(role), "kind": "role",
                         "tiers": tiers}
    # nodes: embed-tier upward only (bounded regardless of node count)
    for node, group in sorted(node_stats.items()):
        rows = _obj_seconds(group)
        tiers = _obj_tiers(rows, edges, offered, embed,
                           _count_target(_samples(rows)))
        used += _tier_rowcount(tiers)
        node_bytes = sum(int((st.get("meta") or {}).get("bytes") or 0)
                         for st in group)
        objects["node:" + node] = {"label": node, "kind": "node",
                                   "node": node, "bytes": node_bytes,
                                   "tiers": tiers}
    # clients: per-client point budget split from what is left
    n_clients = len(stats)
    per_client = CLIENT_MAX_POINTS
    if n_clients:
        per_client = int(max(CLIENT_MIN_POINTS,
                             min(CLIENT_MAX_POINTS,
                                 (DATAJS_ROW_BUDGET - used) / n_clients)))
    for st, c in zip(stats, clients):
        rows = st.get("seconds") or []
        tiers = {COUNT_KEY: _series_columns(
            _count_rows(rows, per_client), 1, edges, with_span=True)}
        objects["client:" + c["name"]] = {
            "label": c["name"], "kind": "client", "node": c["node"],
            "p99_ms": c["p99_ms"], "tiers": tiers}
    tiers_meta = [{"key": str(s), "label": TIER_LABELS[s]} for s in offered]
    tiers_meta.append({"key": COUNT_KEY, "label": COUNT_LABEL})
    embed_tiers = [str(s) for s in offered if s >= embed] + [COUNT_KEY]
    return {
        "summary": summary,
        "span": round(span, 1),
        "tiers": tiers_meta,
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


def reduce_trace_size(trace_list):
    """Merge per-client trace stats into one data-size block."""
    groups = {}
    clients = []
    tops = []
    metad = {"files": 0, "lines": 0, "trace_lines": 0, "slice_lines": 0,
             "parse_errors": 0, "sample_rates": {}, "bins": "us-v1",
             "markers": False,
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
        cl = {"name": m.get("source") or "", "node": m.get("node") or "",
              "pod": m.get("pod") or "", "role": m.get("role") or "",
              "ops": {}}
        for gname, g in (st.get("groups") or {}).items():
            gg = groups.setdefault(gname, {"ops": {}})
            for op, o in (g.get("ops") or {}).items():
                cl["ops"][op] = cl["ops"].get(op, 0) + (o.get("count") or 0)
                dst = gg["ops"].setdefault(op, {
                    "count": 0, "ok": 0, "fail": 0, "status": {},
                    "fields": {}, "_buckets": {"seconds": {}, "minutes": {}},
                    "_fmin": {}, "ftier": 60,
                    "size_values": {}, "replicas": {}, "slice_count": {},
                    "residual": None, "top": []})
                dst["count"] += o.get("count") or 0
                dst["ok"] += o.get("ok") or 0
                dst["fail"] += o.get("fail") or 0
                for k, n in (o.get("status") or {}).items():
                    dst["status"][k] = dst["status"].get(k, 0) + int(n)
                for fk, fd in (o.get("fields") or {}).items():
                    dst["fields"][fk] = _merge_field(dst["fields"].get(fk), fd)
                _merge_bucket_rows(dst["_buckets"]["seconds"],
                                   o.get("seconds"))
                _merge_bucket_rows(dst["_buckets"]["minutes"],
                                   o.get("minutes"))
                for vk in ("size_values", "replicas", "slice_count"):
                    dst[vk] = _merge_value_counts(dst[vk], o.get(vk))
                if o.get("residual"):
                    dst["residual"] = _merge_field(dst["residual"],
                                                   o["residual"])
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
                dst["ftier"] = max(dst.get("ftier") or 0,
                                   int(o.get("ftier") or 60))
                tops.extend(o.get("top") or [])
        clients.append(cl)
    trace_edges = ls.BIN_EDGES_US if metad["bins"] == "us-v1" else None
    trace_hist_ok = metad["bins"] == "us-v1"
    for _gname, g in groups.items():
        for _op, o in g["ops"].items():
            o["fields"] = {k: _field_pcts(fd, trace_edges, trace_hist_ok)
                           for k, fd in o["fields"].items()}
            o["residual"] = (_field_pcts(o["residual"], trace_edges,
                                         trace_hist_ok)
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
            buckets = o.pop("_buckets")
            for bk in ("seconds", "minutes"):
                bmap = buckets[bk]
                rows = []
                for t, b in sorted(bmap.items()):
                    n, okc, failc, ssum, mx = b[0], b[1], b[2], b[3], b[4]
                    hb = b[7]
                    valid = n - (b[8] if len(b) > 8 else 0)
                    rows.append([
                        int(t), n, okc, failc,
                        round(ssum / valid, 3) if valid > 0 else 0.0,
                        round(mx, 6),
                        (round(ls.hist_percentile(hb, 0.50, trace_edges), 4)
                         if trace_hist_ok and hb else None),
                        (round(ls.hist_percentile(hb, 0.99, trace_edges), 4)
                         if trace_hist_ok and hb else None)])
                o[bk] = rows
    tops.sort(key=lambda e: -float(e.get("total_us") or 0))
    return {"meta": metad, "groups": groups, "top": tops[:50],
            "clients": clients}


def reduce_scenario(stats, meta, monitors=None, traces=None):
    """Full scenario report data: stats grouped by data-size (caller passes a
    flat list where possible; the size label is taken from meta["size"] when
    present, else 'all'). `monitors` (from reduce_monitors) is scenario-level
    and independent of data-size."""
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
    sources, nodes = _sources(stats, trace_list)
    return {"meta": meta, "sizes": sizes, "sources": sources, "nodes": nodes,
            "monitors": monitors or {}}


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


def _parse_mem_host(path, node):
    rows = _read_csv_rows(path)
    if len(rows) < 2 or len(rows[0]) < 2:
        return None
    cols = [c.strip() for c in rows[0][1:]]
    t, series = [], {c: [] for c in cols}
    for r in rows[1:]:
        ts = _to_float(r[0]) if r else None
        if ts is None:
            continue
        t.append(int(ts))
        for i, c in enumerate(cols):
            series[c].append(_to_float(r[i + 1]) if i + 1 < len(r) else None)
    if not t:
        return None
    return {"node": node, "t": t, "columns": cols, "series": series}


def _parse_mem_proc(path, node, leak_pct, leak_mb):
    rows = _read_csv_rows(path)
    if len(rows) < 2:
        return []
    idx = {name.strip(): i for i, name in enumerate(rows[0])}
    if any(n not in idx for n in ("ts", "pid", "rss_kb")):
        return []
    groups = {}
    for r in rows[1:]:
        def cell(name):
            i = idx[name]
            return r[i] if i < len(r) else None
        ts, pid = _to_int(cell("ts")), _to_int(cell("pid"))
        rss = _to_float(cell("rss_kb"))
        if ts is None or pid is None:
            continue
        key = (node, pid)
        g = groups.get(key)
        if g is None:
            g = groups[key] = {"node": node, "pid": pid,
                               "comm": (cell("comm") or "").strip(),
                               "t": [], "rss_mb": [], "pss_mb": []}
        g["t"].append(ts)
        g["rss_mb"].append(round(rss / 1024.0, 2) if rss is not None else None)
        pss = _to_float(cell("pss_kb"))
        g["pss_mb"].append(round(pss / 1024.0, 2) if pss is not None else None)
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
                  "leak_suspect": bool(growth_pct >= leak_pct and growth >= leak_mb)})
        out.append(g)
    out.sort(key=lambda x: (x["node"], -x.get("rss_growth_mb", 0)))
    return out


def _parse_urma(path, node, flit_bytes):
    rows = _read_csv_rows(path)
    if len(rows) < 2:
        return []
    idx = {name.strip(): i for i, name in enumerate(rows[0])}
    if any(n not in idx for n in ("ts_ns", "ubpu", "port", "rx_flits", "tx_flits")):
        return []
    groups = {}
    for r in rows[1:]:
        def cell(name):
            i = idx[name]
            return r[i] if i < len(r) else None
        ts, ubpu, port = _to_int(cell("ts_ns")), _to_int(cell("ubpu")), _to_int(cell("port"))
        rx, tx = _to_int(cell("rx_flits")), _to_int(cell("tx_flits"))
        if None in (ts, ubpu, port, rx, tx):
            continue
        groups.setdefault((ubpu, port), []).append((ts, rx, tx))
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
                    "rx_avg": round(sum(rxr) / len(rxr), 3), "rx_max": max(rxr),
                    "tx_avg": round(sum(txr) / len(txr), 3), "tx_max": max(txr)})
    out.sort(key=lambda x: (x["node"], x["ubpu"], x["port"]))
    return out


def reduce_monitors(local_log_dir):
    """Scan <local_log_dir>/**/profiling/<node>/ for monitor CSVs.

    Returns {config, mem_host, mem_proc, urma, summary} or {} when no monitor
    artifact exists (old runs / monitoring disabled). Never raises."""
    mem_host, mem_proc, urma, cfg = [], [], [], {}
    try:
        for root, _dirs, files in os.walk(local_log_dir):
            fset = set(files)
            if not (fset & {MEM_HOST, MEM_PROC, URMA_FLITS}):
                continue
            node = os.path.basename(root)
            if not cfg and MONITOR_CONFIG in fset:
                try:
                    with open(os.path.join(root, MONITOR_CONFIG),
                              "r", encoding="utf-8") as f:
                        cfg = json.load(f) or {}
                except (OSError, ValueError):
                    cfg = {}
            mem_cfg = cfg.get("mem") or {}
            leak_pct = float(mem_cfg.get("leak_growth_pct") or _MON_LEAK_PCT)
            leak_mb = float(mem_cfg.get("leak_min_mb") or _MON_LEAK_MB)
            flit_bytes = int((cfg.get("urma") or {}).get("bytes_per_flit")
                             or _MON_FLIT_BYTES)
            if MEM_HOST in fset:
                h = _parse_mem_host(os.path.join(root, MEM_HOST), node)
                if h:
                    mem_host.append(h)
            if MEM_PROC in fset:
                mem_proc.extend(_parse_mem_proc(
                    os.path.join(root, MEM_PROC), node, leak_pct, leak_mb))
            if URMA_FLITS in fset:
                urma.extend(_parse_urma(
                    os.path.join(root, URMA_FLITS), node, flit_bytes))
    except Exception:
        pass
    if not (mem_host or mem_proc or urma):
        return {}
    summary = {"peak_mem_mb": {}, "worst_proc": None, "peak_urma": None}
    for h in mem_host:
        used = [c for c in h["columns"] if "used_mb" in c]
        peak = 0.0
        for i in range(len(h["t"])):
            s = sum((h["series"][c][i] or 0) for c in used)
            peak = max(peak, s)
        summary["peak_mem_mb"][h["node"]] = round(peak, 1)
    if mem_proc:
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
    return {"config": cfg, "mem_host": mem_host, "mem_proc": mem_proc,
            "urma": urma, "summary": summary}


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


_PAGE_CSS = """
:root{color-scheme:light dark;--bg:#f7f8fa;--panel:#fff;--fg:#1f2329;--mut:#6b7785;
--line:#e3e6eb;--pri:#0e7490;--ok:#2f9e44;--warn:#e8a33d;--err:#d64545;
--term-bg:#12171b;--term-fg:#d6dde3}
@media (prefers-color-scheme:dark){:root{--bg:#101418;--panel:#161b21;--fg:#e6e9ed;
--mut:#8b97a3;--line:#252c34;--pri:#27b3c9;--ok:#4cc38a;--warn:#f0b35a;--err:#ff7b7b}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);
font:14px/1.6 system-ui,'Segoe UI','PingFang SC','Microsoft YaHei',sans-serif}
.wrap{max-width:1400px;margin:0 auto;padding:18px 20px 60px}
h1{font-size:19px;margin:0 0 2px}h2{font-size:15px;margin:0 0 8px}
.sub{color:var(--mut);font-size:12.5px;margin-bottom:14px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:6px;
padding:14px 16px;margin:12px 0}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(150px,1fr));gap:10px}
.kv{background:var(--bg);border:1px solid var(--line);border-radius:5px;padding:8px 10px}
.kv .k{color:var(--mut);font-size:11.5px}.kv .v{font-size:17px;font-weight:600;
font-variant-numeric:tabular-nums}.kv .u{font-size:11.5px;color:var(--mut);font-weight:400}
.toolbar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:8px}
.tl-label{color:var(--mut);font-size:12px}
.sec{font-size:13px;font-weight:600;color:var(--mut);margin:14px 0 8px;
padding-bottom:5px;border-bottom:1px solid var(--line)}
.sec:first-of-type{margin-top:2px}
.params{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));
gap:2px 22px}
.prow{display:flex;gap:10px;padding:4px 0;border-bottom:1px dashed var(--line);
font-size:12.5px;min-width:0}
.prow .pk{flex:0 0 130px;color:var(--mut)}
.prow .pv{flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;
white-space:nowrap;font-family:'Cascadia Code',Consolas,monospace}
.chips{display:inline-flex;flex-wrap:wrap;gap:4px;align-items:center}
.chip{display:inline-flex;align-items:center;gap:2px;padding:2px 4px 2px 8px;
border:1px solid var(--line);border-radius:12px;font-size:12px;background:var(--bg);
max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.chip .chip-x{border:none;background:transparent;color:var(--mut);cursor:pointer;
font-size:12px;padding:0 4px;border-radius:50%}
.chip .chip-x:hover{color:var(--err)}
.tb-sep{width:1px;height:18px;background:var(--line);display:inline-block}
button:disabled{opacity:.4;cursor:not-allowed}
/* 分级时延图（图1）：每层一行，块宽 sqrt 压缩 + 最小 2% */
.levels{display:flex;flex-direction:column;gap:6px}
.lvl-row{display:flex;gap:4px;align-items:stretch;min-height:30px}
.lvl-blk{display:flex;align-items:center;padding:0 8px;border-radius:3px;
color:#fff;font-size:11.5px;white-space:nowrap;overflow:hidden;
text-overflow:ellipsis;cursor:default;min-width:18px}
.lvl-blk:hover{outline:2px solid var(--fg);outline-offset:-2px}
/* 阶段树（图3，参照 LinQuickRec Call Tree） */
.ttree{border:1px solid var(--line);border-radius:5px;padding:6px 10px}
.tnode{display:flex;align-items:center;gap:8px;padding:2px 0;
line-height:1.8;user-select:none}
.tarrow{width:14px;flex:none;color:var(--mut);cursor:pointer}
.tname{font-size:13px;padding:1px 6px;border-radius:4px;cursor:pointer;
white-space:nowrap}
.tname:hover{background:var(--bg)}
.tname.sel{background:var(--bg);color:var(--pri);font-weight:600}
.tbar{width:80px;height:8px;flex:none;border-radius:2px;background:var(--line);
overflow:hidden}
.tfill{display:block;height:8px;border-radius:2px;min-width:2px;
transition:width .3s ease-out}
.tpct{font-size:11.5px;color:var(--mut);min-width:38px;text-align:right;
font-variant-numeric:tabular-nums}
.tval{font-size:12px;color:var(--mut);font-variant-numeric:tabular-nums}
/* 指标套件弹窗（纯 DOM，无 EP 依赖） */
.modal-mask{position:fixed;inset:0;z-index:2000;background:rgba(0,0,0,.45);
display:flex;align-items:center;justify-content:center;padding:24px}
.modal{background:var(--panel);border:1px solid var(--line);border-radius:8px;
width:min(720px,94vw);max-height:86vh;overflow:auto;
box-shadow:0 12px 40px rgba(0,0,0,.35)}
.modal-head{display:flex;align-items:center;justify-content:space-between;
padding:12px 16px;border-bottom:1px solid var(--line);position:sticky;top:0;
background:var(--panel);z-index:1}
.modal-head button{border:none;background:transparent;cursor:pointer;
font-size:14px;color:var(--mut);padding:2px 6px;border-radius:4px}
.modal-head button:hover{color:var(--fg);background:var(--bg)}
.modal-body{padding:12px 16px 20px}
@media (prefers-reduced-motion: reduce){ .tfill{transition:none} }
button{font:inherit;font-size:12.5px;padding:4px 10px;border:1px solid var(--line);
background:var(--panel);color:var(--fg);border-radius:4px;cursor:pointer}
button.on{background:var(--pri);border-color:var(--pri);color:#fff}
select{font:inherit;font-size:12.5px;padding:4px 6px;border:1px solid var(--line);
background:var(--panel);color:var(--fg);border-radius:4px;max-width:280px}
.chart{width:100%;height:420px}.chart.sm{height:260px}
table{border-collapse:collapse;width:100%;font-size:12.5px}
th,td{border-bottom:1px solid var(--line);padding:5px 8px;text-align:left;
white-space:nowrap}
th{color:var(--mut);font-weight:500;position:sticky;top:0;background:var(--panel)}
td.n,th.n{text-align:right;font-variant-numeric:tabular-nums}
tbody tr:hover{background:var(--bg)}
.wrapx{overflow:auto;max-height:420px;border:1px solid var(--line);border-radius:5px}
pre.term{background:var(--term-bg);color:var(--term-fg);border-radius:5px;
padding:10px 12px;font:12.5px/1.5 'Cascadia Code',Consolas,monospace;
overflow:auto;max-height:360px;white-space:pre-wrap;word-break:break-all}
.tag{display:inline-block;font-size:11.5px;padding:1px 7px;border-radius:3px;
border:1px solid var(--line);color:var(--mut)}
.tag.ok{color:var(--ok);border-color:var(--ok)}.tag.err{color:var(--err);
border-color:var(--err)}.tag.warn{color:var(--warn);border-color:var(--warn)}
.mono{font-family:'Cascadia Code',Consolas,monospace}
.path{color:var(--mut);font-size:12px}
.anml li{margin:3px 0;font-size:12.5px}
a{color:var(--pri)}
.note{color:var(--mut);font-size:12px;margin-top:6px}
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
const S = D.sizes || {}; let curSize = Object.keys(S)[0]||'';
let curTier = '';            // 当前粒度 key（"1"/"10"/…/"cnt"）
let selObjs = ['all'];       // 选中的曲线对象（总体/角色/node:*/client:*）
let curMetric = 'p99';       // 多对象对比时的指标
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
  const time=emb.filter(function(t){return t!=='cnt';});
  return time.length?time[0]:(emb.indexOf('cnt')>=0?'cnt':'');
}
function currentTier(){return curTier||defaultTier();}
function tierStep(tier){return tier==='cnt'?1:(Number(tier)||1);}
function colQps(col,tier){
  const step=tierStep(tier);
  return col.c.map(function(c,i){
    return Math.round(c/((col.span&&col.span[i])||step)*100)/100;});}
const C = (window.matchMedia &&
  window.matchMedia('(prefers-color-scheme: dark)').matches)
  ? {fg:'#e6e9ed',mut:'#8b97a3',line:'#252c34',pri:'#27b3c9',
     err:'#ff7b7b',ok:'#4cc38a',warn:'#f0b35a'}
  : {fg:'#1f2329',mut:'#6b7785',line:'#e3e6eb',pri:'#0e7490',
     err:'#d64545',ok:'#2f9e44',warn:'#e8a33d'};
const METRICS=[['avg','平均'],['p50','p50'],['p99','p99'],['p999','p999'],
  ['max','最大'],['qps','QPS'],['fail','失败'],['qw','队列等待']];
function metricLabel(k){const m=METRICS.find(function(x){return x[0]===k;});
  return m?m[1]:k;}
function axisCommon(){
  return {animation:false,textStyle:{color:C.fg,fontSize:12},
    tooltip:{trigger:'axis',confine:true},
    legend:{top:0,type:'scroll',textStyle:{color:C.fg}},
    grid:{left:56,right:20,top:36,bottom:64},
    xAxis:{type:'time',axisLabel:{color:C.mut,
      formatter:function(v){return hhmmss(v/1000);}},
      axisLine:{lineStyle:{color:C.line}}},
    dataZoom:[{type:'inside'},{type:'slider',height:18,bottom:8}]};
}
let chart=null;
function ensureSel(){
  const all=objAll();
  if(!Object.keys(all).length) return;
  selObjs=selObjs.filter(function(k){return !!all[k];});
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
  const z=Z();
  if(!z){chart.clear();return;}
  ensureSel();
  const tier=currentTier();
  const opt=selObjs.length>1?multiOpt(tier):richOpt(tier,selObjs[0]);
  if(z.anomalies&&z.anomalies.length&&opt.series&&opt.series.length){
    opt.series[0].markLine={symbol:'none',silent:true,
      data:z.anomalies.slice(0,50).map(function(a){return {xAxis:a.t*1000,
        lineStyle:{color:C.warn},label:{show:false}};})};
  }
  chart.setOption(opt,true);
}
function richOpt(tier,key){
  const col=colsOf(key,tier);
  if(!col||!col.t.length) return emptyOpt();
  const step=tierStep(tier);
  const qps=colQps(col,tier);
  const x=col.t.map(function(t){return t*1000;});
  const o=axisCommon();
  o.tooltip.formatter=function(ps){
    const i=ps[0].dataIndex; const t=col.t[i];
    const rows=[['时间',fullts(t)],['请求数',col.c[i]],['成功',col.ok[i]],
      ['失败',col.fail[i]],['QPS',qps[i]],['平均(ms)',col.avg[i]],
      ['p50(ms)',col.p50[i]],['p99(ms)',col.p99[i]],['p999(ms)',col.p999[i]],
      ['最大(ms)',col.max[i]],['队列等待均(ms)',col.qw[i]],
      ['队列等待峰(ms)',col.qwmax[i]]];
    return '<b>'+fullts(t)+'</b><br>'+
      rows.map(function(r){return r[0]+'：'+r[1];}).join('<br>');
  };
  o.yAxis=[{type:'value',name:'ms',scale:true,axisLabel:{color:C.mut},
            splitLine:{lineStyle:{color:C.line}}},
           {type:'value',name:'QPS',position:'right',splitLine:{show:false},
            axisLabel:{color:C.mut}}];
  o.series=[
    {name:'QPS',type:'bar',yAxisIndex:1,data:x.map(function(v,i){return [v,qps[i]];}),
     itemStyle:{color:C.pri,opacity:0.55},barMaxWidth:8,large:true},
    {name:'失败',type:'bar',yAxisIndex:1,
     data:x.map(function(v,i){return [v,col.fail[i]];}),
     itemStyle:{color:C.err,opacity:0.75},barMaxWidth:8,large:true},
    {name:'平均',type:'line',data:x.map(function(v,i){return [v,col.avg[i]];}),
     showSymbol:false,lineStyle:{width:1,type:'dashed'}},
    {name:'p50',type:'line',data:x.map(function(v,i){return [v,col.p50[i]];}),
     showSymbol:false,lineStyle:{width:1.5}},
    {name:'p99',type:'line',data:x.map(function(v,i){return [v,col.p99[i]];}),
     showSymbol:false,lineStyle:{width:1.8}},
    {name:'p999',type:'line',data:x.map(function(v,i){return [v,col.p999[i]];}),
     showSymbol:false,lineStyle:{width:1.5}},
    {name:'最大',type:'line',data:x.map(function(v,i){return [v,col.max[i]];}),
     showSymbol:false,lineStyle:{width:1,type:'dotted'}}];
  return o;
}
function multiOpt(tier){
  const o=axisCommon();
  o.yAxis=[{type:'value',name:metricLabel(curMetric),scale:true,
            axisLabel:{color:C.mut},splitLine:{lineStyle:{color:C.line}}}];
  o.series=[];
  selObjs.forEach(function(key){
    const t=colsOf(key,tier)?tier:'cnt';
    const col=colsOf(key,t);
    if(!col||!col.t.length) return;
    const qps=(curMetric==='qps')?colQps(col,t):null;
    const nm=objLabel(key)+(t!==tier?'（自适应）':'');
    o.series.push({name:nm,type:'line',showSymbol:false,
      data:col.t.map(function(tv,i){
        let v;
        if(curMetric==='qps') v=qps[i];
        else if(curMetric==='fail') v=col.fail[i];
        else v=(col[curMetric]==null?0:col[curMetric][i]);
        return [tv*1000,v];}),
      lineStyle:{width:1.8}});
  });
  return o;
}
function renderSummary(){
  const z=Z(); if(!z) return;
  const s=z.summary;
  const pg=document.getElementById('perfcards');
  const ig=document.getElementById('infocards');
  const pbox=document.getElementById('params');
  if(!pg||!ig||!pbox) return;
  pg.innerHTML=''; ig.innerHTML=''; pbox.innerHTML='';
  renderParams(pbox);
  if(!s){
    pg.appendChild(el('div','note',
      '该 data_size 只有 C++ trace 统计（无 logstats 数据），下方仍可查看全链路区。'));
    ig.style.display='none';
    return;
  }
  ig.style.display='';
  const target=D.meta&&D.meta.qps_target?Number(D.meta.qps_target):0;
  // 性能统计：时延/吞吐
  pg.appendChild(kv('QPS', s.qps));
  if(target)pg.appendChild(kv('QPS 达成率', (s.qps/target*100).toFixed(1), '%'));
  pg.appendChild(kv('平均时延', s.avg_ms, 'ms'));
  pg.appendChild(kv('p50', s.p50_ms, 'ms'));
  pg.appendChild(kv('p95', s.p95_ms, 'ms'));
  pg.appendChild(kv('p99', s.p99_ms, 'ms'));
  pg.appendChild(kv('p999', s.p999_ms, 'ms'));
  pg.appendChild(kv('p9999', s.p9999_ms, 'ms'));
  pg.appendChild(kv('最大', s.max_ms, 'ms'));
  if(s.qw_avg_ms!=null)pg.appendChild(kv('队列等待均', s.qw_avg_ms, 'ms'));
  if(s.qw_max_ms!=null)pg.appendChild(kv('队列等待峰', s.qw_max_ms, 'ms'));
  if(s.max_depth)pg.appendChild(kv('最大队列深度', s.max_depth));
  pg.appendChild(kv('吞吐', s.throughput_mb_s==null?'-':
    Number(s.throughput_mb_s).toFixed(1), 'MB/s'));
  // 信息展示：数量/规模
  ig.appendChild(kv('总数', s.total));
  ig.appendChild(kv('成功', s.ok));
  ig.appendChild(kv('失败', s.fail));
  ig.appendChild(kv('失败率', (s.fail_rate*100).toFixed(2), '%'));
  ig.appendChild(kv('时长', s.wall_s>0?fmtDur(s.wall_s):'-'));
  ig.appendChild(kv('客户端', s.clients));
}
function renderParams(box){
  const ps=(D.meta&&D.meta.params)||[];
  if(!ps.length){
    box.appendChild(el('div','note','无参数信息（旧报表或未注入）'));
    return;
  }
  ps.forEach(function(p){
    const row=el('div','prow');
    row.appendChild(el('span','pk',String(p[0])));
    const v=el('span','pv');
    v.textContent=(p[1]==null||p[1]==='')?'-':String(p[1]);
    v.title=v.textContent;
    row.appendChild(v);
    box.appendChild(row);
  });
}
function renderTable(){
  const z=Z(); if(!z) return;
  const note=document.getElementById('datanote');
  if(note&&window.__mcDataMissing)note.style.display='';
  const tb=document.querySelector('#ctable tbody'); tb.innerHTML='';
  const clients=(FULL&&FULL.sizes&&FULL.sizes[curSize]&&
    FULL.sizes[curSize].clients)||z.clients||[];
  clients.slice().sort((a,b)=>(b.p99_ms-a.p99_ms)).forEach(c=>{
    const tr=el('tr');
    [c.name,c.role,c.node,c.total,c.ok,c.fail,
     c.avg_ms,c.p50_ms,c.p99_ms,c.p999_ms,c.max_ms,
     c.qw_avg_ms==null?'-':c.qw_avg_ms,fmtBytes(c.bytes)].forEach((v,i)=>{
      const td=el('td', (i>=3?'n':''), String(v));
      tr.appendChild(td);
    });
    tr.title='点击加入时延曲线对比（对象选择器）';
    tr.style.cursor='pointer';
    tr.onclick=()=>addObj("client:"+c.name);
    tb.appendChild(tr);
  });
}
function renderErrors(){
  const z=Z(); if(!z) return;
  const wrap=document.getElementById('errors');
  wrap.innerHTML='';
  const entries=Object.entries(z.errors||{});
  if(!entries.length){wrap.appendChild(el('div','note','无错误'));}
  else{
    const t=el('table'); const thead=el('thead');
    const hr=el('tr'); hr.appendChild(el('th',null,'状态码'));
    hr.appendChild(el('th','n','次数')); thead.appendChild(hr); t.appendChild(thead);
    const tb=el('tbody');
    entries.sort((a,b)=>b[1]-a[1]).forEach(([k,n])=>{
      const tr=el('tr'); tr.appendChild(el('td','mono',k));
      tr.appendChild(el('td','n',String(n))); tb.appendChild(tr);
    });
    t.appendChild(tb); wrap.appendChild(t);
  }
  if(z.error_excerpt){
    const p=el('pre','term'); p.textContent=z.error_excerpt;
    wrap.appendChild(el('div','note','首错误现场（±20 行，状态 '+
      (z.error_excerpt_status||'?')+'）：'));
    wrap.appendChild(p);
  }
  if(z.slowest&&z.slowest.length){
    const t=el('table'); const thead=el('thead'); const hr=el('tr');
    ['时间','操作','key','时延(ms)','队列等待(ms)','状态'].forEach((h,i)=>
      hr.appendChild(el('th', i>=3&&i<=4?'n':'', h)));
    thead.appendChild(hr); t.appendChild(thead);
    const tb=el('tbody');
    z.slowest.forEach(r=>{
      const tr=el('tr');
      [r.ts,r.op,r.key,r.lat_ms,(r.qw_ms==null?'-':r.qw_ms),r.status].forEach(
        (v,i)=>tr.appendChild(el('td', (i>=3&&i<=4?'n mono':'mono'), String(v))));
      tb.appendChild(tr);
    });
    t.appendChild(tb);
    const box=document.createElement('div'); box.className='wrapx'; box.appendChild(t);
    wrap.appendChild(el('div','note','Top-'+(z.slowest.length)+' 慢操作（单点级证据）：'));
    wrap.appendChild(box);
  }
  if(z.anomalies&&z.anomalies.length){
    const ul=el('ul','anml');
    z.anomalies.slice(0,100).forEach(a=>{
      ul.appendChild(el('li',null, fullts(a.t)+'  '+
        (a.kind==='error_burst'?('错误突发：'+a.fail+'/'+a.count+' 失败'):
         ('时延突增：均值 '+a.avg_ms+'ms / '+a.count+' 次'))));
    });
    wrap.appendChild(el('div','note','异常窗口（主图上以黄色竖线标注）：'));
    wrap.appendChild(ul);
  }
}
function renderSources(){
  const box=document.getElementById('sources'); box.innerHTML='';
  const nodes=D.nodes||[]; const srcs=D.sources||[];
  if(nodes.length){
    const t=el('table'); const thead=el('thead'); const hr=el('tr');
    ['节点','客户端数','日志量'].forEach((h,i)=>hr.appendChild(el('th',i?'n':'',h)));
    thead.appendChild(hr); t.appendChild(thead); const tb=el('tbody');
    nodes.forEach(n=>{const tr=el('tr');
      tr.appendChild(el('td','mono',n.node));
      tr.appendChild(el('td','n',String(n.pods)));
      tr.appendChild(el('td','n',fmtBytes(n.bytes))); tb.appendChild(tr);});
    t.appendChild(tb); box.appendChild(t); box.appendChild(el('div','note',
      '宿主机日志：<host_log_root>/<run_id>/<pod>/（CLEAN-UP 不删除）。'+
      '登录节点 ssh <node>；进入 pod：kubectl exec -it <pod> -n <ns> -- bash'));
  }
  if(srcs.length){
    const t=el('table'); const thead=el('thead'); const hr=el('tr');
    ['节点','pod','角色','文件','远程路径','大小'].forEach((h,i)=>
      hr.appendChild(el('th', i===5?'n':'', h)));
    thead.appendChild(hr); t.appendChild(thead); const tb=el('tbody');
    srcs.forEach(s=>{const tr=el('tr');
      [s.node,s.pod,s.role,s.source,s.remote_path,fmtBytes(s.bytes)].forEach(
        (v,i)=>tr.appendChild(el('td', (i===5?'n mono':'mono'), String(v||'-'))));
      tb.appendChild(tr);});
    t.appendChild(tb); const wrapx=el('div','wrapx'); wrapx.appendChild(t);
    box.appendChild(el('div','note','日志现场索引（按需登录节点/进入 pod 查看）：'));
    box.appendChild(wrapx);
  }
}
function addObj(key){
  if(!key) return;
  if(selObjs.indexOf(key)<0) selObjs.push(key);
  renderObjChips(); renderObjPicker(); renderControls(); buildChart();
}
function delObj(key){
  selObjs=selObjs.filter(function(x){return x!==key;});
  if(!selObjs.length) selObjs=['all'];
  renderObjChips(); renderObjPicker(); renderControls(); buildChart();
}
function renderObjPicker(){
  const sel=document.getElementById('objAdd'); if(!sel) return;
  const all=objAll();
  const groups={role:[],node:[],client:[]};
  Object.keys(all).forEach(function(k){
    const kind=all[k].kind||'client';
    (groups[kind]||groups.client).push(k);
  });
  sel.innerHTML='';
  function addGroup(label,keys){
    const vis=keys.filter(function(k){return selObjs.indexOf(k)<0;});
    if(!vis.length) return;
    const g=document.createElement('optgroup'); g.label=label;
    vis.sort(function(x,y){
      return String(objLabel(x)).localeCompare(String(objLabel(y)));})
      .forEach(function(k){
        const op=el('option',null,objLabel(k)); op.value=k; g.appendChild(op);});
    sel.appendChild(g);
  }
  addGroup('总体',groups.role);
  addGroup('节点',groups.node);
  addGroup('客户端',groups.client);
  sel.onchange=function(){
    const v=sel.value; sel.value=''; if(v) addObj(v);};
}
function renderObjChips(){
  const box=document.getElementById('objChips'); if(!box) return;
  ensureSel();
  box.innerHTML='';
  selObjs.forEach(function(k){
    const chip=el('span','chip',objLabel(k));
    const x=el('button','chip-x','×'); x.title='移除该对象';
    x.onclick=function(){delObj(k);};
    chip.appendChild(x); box.appendChild(chip);
  });
}
function renderControls(){
  const box=document.getElementById('tierBtns'); if(!box) return;
  ensureSel();
  box.innerHTML='';
  const active=currentTier();
  tierList().forEach(function(t){
    const ok=selObjs.every(function(k){return !!colsOf(k,t.key);});
    const b=el('button', (t.key===active&&ok)?'on':'', t.label);
    b.disabled=!ok;
    if(!ok) b.title='所选对象无该粒度（客户端曲线固定为自适应分桶）';
    b.onclick=function(){curTier=t.key;renderControls();buildChart();};
    box.appendChild(b);
  });
  const mb=document.getElementById('metricBtns');
  if(mb){
    mb.innerHTML='';
    if(selObjs.length>1){
      METRICS.forEach(function(m){
        const b=el('button', m[0]===curMetric?'on':'', m[1]);
        b.onclick=function(){curMetric=m[0];renderControls();buildChart();};
        mb.appendChild(b);
      });
    }
  }
}
function renderAll(){
  const z=Z(); if(!z) return;
  const ss=document.getElementById('sizeSel');
  if(ss) ss.style.display=Object.keys(S).length>1?'inline-block':'none';
  renderSummary(); renderObjPicker(); renderObjChips(); renderControls();
  buildChart(); renderTable(); renderErrors();
  renderTrace();
  if(!window.__srcDone){renderSources();window.__srcDone=true;}
}
let monCharts={};
function _monChart(id, series, yname){
  const box=document.getElementById(id); if(!box||!series.length) return false;
  let c=monCharts[id]; if(!c) c=monCharts[id]=echarts.init(box);
  c.setOption({animation:false,textStyle:{color:C.fg,fontSize:12},
    tooltip:{trigger:'axis',confine:true},
    legend:{top:0,type:'scroll',textStyle:{color:C.fg}},
    grid:{left:56,right:20,top:36,bottom:56},
    xAxis:{type:'time',axisLabel:{color:C.mut,formatter:v=>hhmmss(v/1000)},
      axisLine:{lineStyle:{color:C.line}}},
    yAxis:{type:'value',name:yname,scale:true,axisLabel:{color:C.mut},
      splitLine:{lineStyle:{color:C.line}}},
    dataZoom:[{type:'inside'},{type:'slider',height:16,bottom:6}],
    series:series}, true);
  return true;
}
function renderMonitors(){
  const M=D.monitors||{}; const card=document.getElementById('monitorcard');
  if(!card) return;
  const has=(M.mem_host&&M.mem_host.length)||(M.mem_proc&&M.mem_proc.length)||
    (M.urma&&M.urma.length);
  if(!has){card.style.display='none';return;}
  card.style.display='';
  const g=document.getElementById('moncards'); g.innerHTML='';
  const sm=M.summary||{};
  if(sm.peak_mem_mb) Object.keys(sm.peak_mem_mb).forEach(n=>
    g.appendChild(kv('峰值内存 '+n, sm.peak_mem_mb[n], 'MB')));
  if(sm.worst_proc){
    const w=sm.worst_proc;
    const k=kv('RSS 最大增幅', w.label||'-', '');
    if(w.leak_suspect){
      const t=el('span','tag warn',' 疑似泄露');
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
  const hs=[];
  (M.mem_host||[]).forEach(h=>h.columns.forEach(c=>hs.push({
    name:h.node+'/'+c,type:'line',showSymbol:false,
    data:h.t.map((t,i)=>[t*1000,h.series[c][i]])})));
  _monChart('memchart', hs, 'MB');
  const ps=(M.mem_proc||[]).map(p=>({
    name:p.node+'/'+p.pid+(p.comm?' '+p.comm:''),type:'line',showSymbol:false,
    data:p.t.map((t,i)=>[t*1000,p.rss_mb[i]])}));
  _monChart('procchart', ps, 'RSS MB');
  const us=[];
  (M.urma||[]).forEach(u=>{
    const tag=u.node+' ubpu='+u.ubpu+' port='+u.port;
    us.push({name:tag+' RX',type:'line',showSymbol:false,
      data:u.t.map((t,i)=>[t*1000,u.rx[i]])});
    us.push({name:tag+' TX',type:'line',showSymbol:false,
      data:u.t.map((t,i)=>[t*1000,u.tx[i]])});
  });
  _monChart('urmachart', us, 'GiB/s');
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
  renderAll(); renderMonitors();
  window.addEventListener('resize',function(){
    if(chart)chart.resize();
    Object.keys(monCharts).forEach(function(k){monCharts[k].resize();});
  });
}
/* ---------------- SDK 全链路（C++ trace）：分级时延 / 阶段曲线 / 阶段树 ---------------- */
const TREAD=[["total_us",0,""],["query_total_us",1,"total_us"],
 ["query_route_us",2,"query_total_us"],["query_rpc_us",2,"query_total_us"],
 ["select_us",1,"total_us"],["buf_alloc_us",1,"total_us"],
 ["memread_total_us",1,"total_us"],["read_submit_us",2,"memread_total_us"],
 ["read_queue_us",2,"memread_total_us"],["read_urma_us",2,"memread_total_us"],
 ["read_teardown_us",2,"memread_total_us"],["residual",1,"total_us"]];
const TWRITE=[["total_us",0,""],["prep_total_us",1,"total_us"],
 ["prep_buf_alloc_us",2,"prep_total_us"],["prep_memcpy_us",2,"prep_total_us"],
 ["prep_split_us",2,"prep_total_us"],["checksum_us",1,"total_us"],
 ["prepare_us",1,"total_us"],["query_us",1,"total_us"],
 ["put_start_us",1,"total_us"],["memwrite_total_us",1,"total_us"],
 ["memwrite_submit_us",2,"memwrite_total_us"],
 ["memwrite_queue_us",2,"memwrite_total_us"],
 ["memwrite_urma_us",2,"memwrite_total_us"],
 ["memwrite_teardown_us",2,"memwrite_total_us"],
 ["put_end_us",1,"total_us"],["finalize_us",1,"total_us"],
 ["cache_invalidate_us",1,"total_us"],["revoke_us",1,"total_us"],
 ["residual",1,"total_us"]];
function tTree(g){return g==='write'?TWRITE:TREAD;}
function traceSizes(){return Object.keys(S).filter(function(k){
  return S[k]&&S[k].trace;});}
let tSize='',tGroup='',tOp='',tChart=null;
let tLevelMetric='avg',tStageSel=['total_us'],tStageMetric='avg';
const tExpand={};
function traceOf(){
  const keys=traceSizes(); if(!keys.length) return null;
  if(tSize&&S[tSize]&&S[tSize].trace) return S[tSize].trace;
  tSize=keys[0]; return S[tSize].trace;
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
function tStat(key,cur){
  if(key==='residual') return cur.data.residual||{};
  return (cur.data.fields||{})[key]||{};
}
function tParentKey(key,cur){
  const it=tTree(cur.g).find(function(x){return x[0]===key;});
  return (it&&it[2])||'total_us';
}
function tLevelMetricOf(st){return st?(st[tLevelMetric]||0):0;}
function fmtUs(v){v=Number(v)||0;
  return v>=1000?(v/1000).toFixed(2)+' ms':v.toFixed(1)+' µs';}
function lvlColor(share){
  const x=Math.max(0,Math.min(1,share));
  return 'hsl('+Math.round(200-200*x)+',62%,44%)';
}
function renderLevelMetric(){
  const box=document.getElementById('lvlMetric'); if(!box) return;
  box.innerHTML='';
  [['avg','平均'],['p50','p50'],['p99','p99']].forEach(function(m){
    const b=el('button',m[0]===tLevelMetric?'on':'',m[1]);
    b.onclick=function(){tLevelMetric=m[0];renderTrace();};
    box.appendChild(b);
  });
}
function renderLevels(cur){
  const box=document.getElementById('levels'); if(!box) return;
  box.innerHTML='';
  const tree=tTree(cur.g);
  const total=tStat('total_us',cur);
  [0,1,2].forEach(function(level){
    const items=tree.filter(function(x){return x[1]===level;});
    if(!items.length) return;
    const vals=items.map(function(it){
      return {key:it[0],parent:it[2],st:tStat(it[0],cur)};});
    const maxv=Math.max.apply(null,vals.map(function(v){
      return tLevelMetricOf(v.st);}).concat([0.001]));
    const row=el('div','lvl-row');
    vals.forEach(function(v){
      const m=tLevelMetricOf(v.st);
      const pm=tLevelMetricOf(tStat(v.parent||'total_us',cur));
      const share=pm>0?m/pm:0;
      const b=el('div','lvl-blk');
      b.style.width=Math.max(2,Math.round(Math.sqrt(m/maxv)*100))+'%';
      b.style.background=lvlColor(share);
      const nm=(v.key==='residual')?'残差':v.key;
      b.textContent=nm+' '+fmtUs(m);
      const tip=[nm,'样本 '+(v.st.n==null?'-':v.st.n)];
      ['avg','p50','p95','p99','p999','max','sigma','neg'].forEach(function(k){
        if(v.st&&v.st[k]!=null) tip.push(k+'：'+v.st[k]+' µs');});
      if(total&&total.avg>0&&v.st&&v.st.avg!=null)
        tip.push('占端到端：'+(v.st.avg/total.avg*100).toFixed(1)+'%');
      b.title=tip.join('\n');
      row.appendChild(b);
    });
    box.appendChild(row);
  });
}
function totalSeriesData(cur,tier,metric){
  const rows=cur.data.minutes||[];
  const agg={};
  rows.forEach(function(r){
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
function buildStageChart(cur){
  const box=document.getElementById('tchart'); if(!box) return;
  if(!tChart) tChart=echarts.init(box);
  const tier=cur.data.ftier||60;
  const fmin=cur.data.fminutes||{};
  const series=[];
  tStageSel.forEach(function(key){
    if(key==='total_us'){
      series.push({name:'total（端到端）',type:'line',showSymbol:false,
        data:totalSeriesData(cur,tier,tStageMetric),
        lineStyle:{width:2}});
      return;
    }
    const rows=fmin[key]||[];
    if(!rows.length) return;
    series.push({name:(key==='residual'?'残差':key),
      type:'line',showSymbol:false,
      data:rows.map(function(r){
        return [r[0]*1000, tStageMetric==='max'?r[3]:r[2]];}),
      lineStyle:{width:1.6}});
  });
  tChart.setOption({animation:false,textStyle:{color:C.fg,fontSize:12},
    tooltip:{trigger:'axis',confine:true,
      valueFormatter:function(v){return fmtUs(v);}},
    legend:{top:0,type:'scroll',textStyle:{color:C.fg}},
    grid:{left:60,right:20,top:36,bottom:56},
    xAxis:{type:'time',axisLabel:{color:C.mut,
      formatter:function(v){return hhmmss(v/1000);}},
      axisLine:{lineStyle:{color:C.line}}},
    yAxis:{type:'value',name:'µs',scale:true,axisLabel:{color:C.mut},
      splitLine:{lineStyle:{color:C.line}}},
    dataZoom:[{type:'inside'},{type:'slider',height:16,bottom:6}],
    series:series},true);
}
function renderStageBtns(cur){
  const box=document.getElementById('stageBtns'); if(!box) return;
  box.innerHTML='';
  const tree=tTree(cur.g);
  const list=['total_us'].concat(tree.filter(function(x){
    return x[1]===1;}).map(function(x){return x[0];}));
  list.forEach(function(k){
    const on=tStageSel.indexOf(k)>=0;
    const b=el('button',on?'on':'',k==='residual'?'残差':k);
    b.onclick=function(){
      const i=tStageSel.indexOf(k);
      if(i>=0)tStageSel.splice(i,1); else tStageSel.push(k);
      if(!tStageSel.length) tStageSel=['total_us'];
      renderStageBtns(cur); buildStageChart(cur);
    };
    box.appendChild(b);
  });
  const mb=document.getElementById('fmetricBtns'); mb.innerHTML='';
  [['avg','平均'],['max','最大']].forEach(function(m){
    const b=el('button',m[0]===tStageMetric?'on':'',m[1]);
    b.onclick=function(){tStageMetric=m[0];renderStageBtns(cur);
      buildStageChart(cur);};
    mb.appendChild(b);
  });
  const tb=document.getElementById('ftierBtns'); tb.innerHTML='';
  const tier=cur.data.ftier||60;
  tb.appendChild(el('span','tag','粒度：'+(tier>=600?'10 分钟':'1 分钟')));
}
function renderTree(cur){
  const box=document.getElementById('ttree'); if(!box) return;
  box.innerHTML='';
  const tree=tTree(cur.g);
  function addRow(key,level,st,label){
    const row=el('div','tnode');
    row.style.paddingLeft=(level*22)+'px';
    const hasKids=level===1&&tree.some(function(x){
      return x[1]===2&&x[2]===key;});
    const arrow=el('span','tarrow',hasKids?(tExpand[key]?'▾':'▸'):'');
    if(hasKids) arrow.onclick=function(){
      tExpand[key]=!tExpand[key]; renderTree(cur);};
    row.appendChild(arrow);
    const nm=el('span','tname',label);
    nm.title='点击查看指标套件';
    nm.onclick=function(){openNodeModal(cur,key,label);};
    row.appendChild(nm);
    const pk=(level===0)?'total_us':tParentKey(key,cur);
    const pm=tStat(pk,cur);
    const share=(pm&&pm.avg>0&&st&&st.avg!=null)?st.avg/pm.avg:0;
    const bar=el('span','tbar');
    const fill=el('span','tfill');
    fill.style.width=Math.max(2,Math.min(100,Math.round(share*100)))+'%';
    fill.style.background=lvlColor(share);
    bar.appendChild(fill); row.appendChild(bar);
    row.appendChild(el('span','tpct',share>0?Math.round(share*100)+'%':''));
    row.appendChild(el('span','tval',(st&&st.n!=null)?
      (Number(st.avg)||0).toFixed(1)+' µs ('+st.n+')':'—'));
    box.appendChild(row);
  }
  addRow('total_us',0,tStat('total_us',cur),'total（端到端）');
  tree.filter(function(x){return x[1]===1;}).forEach(function(it){
    addRow(it[0],1,tStat(it[0],cur),
      it[0]==='residual'?'残差（total − 阶段和）':it[0]);
    if(tExpand[it[0]]){
      tree.filter(function(x){return x[1]===2&&x[2]===it[0];})
        .forEach(function(ch){addRow(ch[0],2,tStat(ch[0],cur),ch[0]);});
    }
  });
}
let modalChart=null;
function openNodeModal(cur,key,label){
  const st=tStat(key,cur);
  const total=tStat('total_us',cur);
  document.getElementById('modalTitle').textContent=
    (key==='residual'?'残差':key)+' · 指标套件';
  const body=document.getElementById('modalBody'); body.innerHTML='';
  if(!st||st.n==null){
    body.appendChild(el('div','note','该阶段无样本'));
  }else{
    const g=el('div','grid');
    [['样本数',st.n],['负值(-1) 样本',st.neg],['平均',st.avg,'µs'],
     ['p50',st.p50,'µs'],['p95',st.p95,'µs'],['p99',st.p99,'µs'],
     ['p999',st.p999,'µs'],['最大',st.max,'µs'],['最小',st.min,'µs'],
     ['标准差 σ',st.sigma,'µs']].forEach(function(x){
      if(x[1]==null) return;
      g.appendChild(kv(x[0],x[1],x[2]||''));
    });
    if(total&&total.avg>0&&st.avg!=null)
      g.appendChild(kv('占端到端',(st.avg/total.avg*100).toFixed(1),'%'));
    const pm=tStat(tParentKey(key,cur),cur);
    if(pm&&pm.avg>0&&st.avg!=null)
      g.appendChild(kv('占父阶段',(st.avg/pm.avg*100).toFixed(1),'%'));
    body.appendChild(g);
    if(st.hist){
      body.appendChild(el('h3','sec','时延分布（对数分箱）'));
      const div=el('div','chart sm');
      body.appendChild(div);
      const keys=Object.keys(st.hist).map(Number)
        .sort(function(a,b){return a-b;});
      modalChart=echarts.init(div);
      modalChart.setOption({animation:false,textStyle:{color:C.fg,fontSize:12},
        tooltip:{trigger:'axis'},grid:{left:56,right:16,top:16,bottom:40},
        xAxis:{type:'category',data:keys,axisLabel:{color:C.mut}},
        yAxis:{type:'value',axisLabel:{color:C.mut},
          splitLine:{lineStyle:{color:C.line}}},
        series:[{type:'bar',data:keys.map(function(k){return st.hist[k];}),
          itemStyle:{color:C.pri,opacity:0.7}}]});
    }
  }
  const hasCurve=(key==='total_us'||key==='residual'||
    !!(cur.data.fminutes||{})[key]);
  if(hasCurve){
    const b=el('button','','查看该阶段时间曲线');
    b.style.marginTop='10px';
    b.onclick=function(){
      if(tStageSel.indexOf(key)<0) tStageSel.push(key);
      closeNodeModal(); renderStageBtns(cur); buildStageChart(cur);
    };
    body.appendChild(b);
  }
  document.getElementById('nodeModal').style.display='flex';
}
function closeNodeModal(){
  document.getElementById('nodeModal').style.display='none';
  if(modalChart){modalChart.dispose&&modalChart.dispose(); modalChart=null;}
}
function renderTraceTop(cur){
  const tb=document.querySelector('#ttop tbody'); if(!tb) return;
  tb.innerHTML='';
  const top=(traceOf()||{}).top||[];
  top.forEach(function(r){
    const tr=el('tr');
    [r.start_time,r.trace_id,r.key,r.op,r.status,r.size,r.total_us,
     Object.keys(r.stages||{}).slice(0,3).join(' / ')].forEach(
      function(v,i){
        tr.appendChild(el('td',
          (i===5||i===6)?'n mono':'mono', v==null?'-':String(v)));});
    tb.appendChild(tr);
  });
  if(!top.length){
    const tr=el('tr'); const td=el('td'); td.colSpan=8;
    td.textContent='无数据'; tr.appendChild(td); tb.appendChild(tr);
  }
}
function renderTraceStatus(cur){
  const box=document.getElementById('terrs'); box.innerHTML='';
  const st=(cur&&cur.data&&cur.data.status)||{};
  const es=Object.entries(st).sort(function(a,b){return b[1]-a[1];});
  if(!es.length){box.appendChild(el('div','note','无数据'));return;}
  const t=el('table'); const thead=el('thead'); const hr=el('tr');
  hr.appendChild(el('th',null,'状态')); hr.appendChild(el('th','n','次数'));
  thead.appendChild(hr); t.appendChild(thead);
  const tbody=el('tbody');
  es.forEach(function(x){
    const tr=el('tr'); tr.appendChild(el('td','mono',x[0]));
    tr.appendChild(el('td','n',String(x[1]))); tbody.appendChild(tr);});
  t.appendChild(tbody); box.appendChild(t);
}
function renderTrace(){
  const card=document.getElementById('tracecard'); if(!card) return;
  const keys=traceSizes();
  if(!keys.length){card.style.display='none';return;}
  card.style.display='';
  const tsel=document.getElementById('tsizeSel');
  if(tsel){
    if(!tSize||keys.indexOf(tSize)<0) tSize=keys[0];
    tsel.innerHTML='';
    keys.forEach(function(k){const o=el('option',null,k);o.value=k;
      tsel.appendChild(o);});
    tsel.value=tSize;
    tsel.onchange=function(){
      tSize=tsel.value; tGroup=''; tOp=''; tStageSel=['total_us'];
      renderTrace();};
  }
  const t=traceOf();
  const m=(t&&t.meta)||{};
  const rates=Object.keys(m.sample_rates||{});
  let phaseTxt;
  if(m.markers){
    const pc=m.phase_counts||{};
    phaseTxt=' · 已按阶段标记剔除：预热 '+(pc.warmup||0)+
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
      g==='read'?'读':(g==='write'?'写':'其它'));
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
  const d=cur.data; const f=d.fields||{}; const tot=f.total_us||{};
  const cards=document.getElementById('tcards'); cards.innerHTML='';
  cards.appendChild(kv('样本数',d.count));
  cards.appendChild(kv('失败',d.fail));
  if(tot.avg!=null)cards.appendChild(kv('平均',tot.avg,'µs'));
  if(tot.p50!=null)cards.appendChild(kv('p50',tot.p50,'µs'));
  if(tot.p95!=null)cards.appendChild(kv('p95',tot.p95,'µs'));
  if(tot.p99!=null)cards.appendChild(kv('p99',tot.p99,'µs'));
  if(tot.p999!=null)cards.appendChild(kv('p999',tot.p999,'µs'));
  if(tot.max!=null)cards.appendChild(kv('最大',tot.max,'µs'));
  renderLevelMetric(); renderLevels(cur);
  renderStageBtns(cur); buildStageChart(cur);
  renderTree(cur);
  renderTraceTop(cur);
  renderTraceStatus(cur);
}
document.addEventListener('DOMContentLoaded',function(){
  const mc=document.getElementById('modalClose');
  if(mc) mc.onclick=closeNodeModal;
  const nm=document.getElementById('nodeModal');
  if(nm) nm.onclick=function(e){if(e.target===nm)closeNodeModal();};
});
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
    a.href=api(a.getAttribute('href').slice(3));
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
               full_loader, rel_base=None, data_rel=None):
    # JSON is embedded as a JS literal; escape the closing-script sequence.
    data_literal = json.dumps(data_json, ensure_ascii=False,
                              separators=(",", ":")).replace("</", "<\\/")
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
<h1>{title}</h1><div class="sub">{subtitle}</div>
{body}
</div>
<script>{echarts}</script>
<script>window.__MC_REPORT__ = {data};</script>
{rel_vars}{loader}{adapter}
<script>{app}</script>
<script>if(window.__MC_FULL__){{}}/* placeholder for consistency */</script>
</body></html>
""".format(title=html.escape(str(title), quote=True),
           subtitle=html.escape(str(subtitle), quote=True), body=body_html,
           css=_PAGE_CSS, echarts=echarts_js, data=data_literal,
           rel_vars=rel_vars, loader=loader, adapter=adapter, app=_APP_JS)


def render_scenario(data, out_dir, echarts_path, topk=TOPK_DEFAULT,
                     rel_base=None):
    """Write report.html (self-contained) + data.js (full per-client data)."""
    os.makedirs(out_dir, exist_ok=True)
    meta = data.get("meta") or {}
    size_keys = list((data.get("sizes") or {}).keys())
    # compact copy for the HTML: only top-K clients carry minute series
    embedded = {"meta": meta, "sizes": {}, "nodes": data.get("nodes") or [],
                "sources": data.get("sources") or [],
                "monitors": compact_monitors(data.get("monitors") or {})}
    for size, z in (data.get("sizes") or {}).items():
        clients = sorted(z.get("clients") or [],
                         key=lambda c: -float(c.get("p99_ms") or 0))
        keep = clients[:topk]
        slim = dict(z)
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
            for k in ("node", "bytes", "p99_ms"):
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
    if meta.get("config"):
        meta_bits.append("配置 " + str(meta["config"]))
    if meta.get("mode"):
        meta_bits.append("模式 " + str(meta["mode"]))
    if meta.get("launch_mode"):
        meta_bits.append("启动方式 " + str(meta["launch_mode"]))
    if meta.get("clients"):
        meta_bits.append("客户端 " + str(meta["clients"]))
    if meta.get("qps_target"):
        meta_bits.append("目标 QPS " + str(meta["qps_target"]))
    if meta.get("protocol"):
        meta_bits.append("协议 " + str(meta["protocol"]))
    if meta.get("started"):
        meta_bits.append("开始 " + str(meta["started"]))
    if meta.get("early_stop"):
        meta_bits.append("⚠ 提前停止/异常，数据不完整")
    subtitle = " · ".join(meta_bits) + \
        " · 生成于 " + time.strftime("%Y-%m-%d %H:%M:%S")
    body = """
<div class="card">
<div class="toolbar">
<label class="tl-label" for="sizeSel">数据大小</label>
<select id="sizeSel"></select>
</div>
<h3 class="sec">性能统计</h3>
<div id="perfcards" class="grid"></div>
<h3 class="sec">信息展示</h3>
<div id="infocards" class="grid"></div>
<h3 class="sec">参数配置</h3>
<div id="params" class="params"></div>
</div>
<div class="card"><h2>时延曲线</h2>
<div class="toolbar">
<label class="tl-label" for="objAdd">对象</label>
<select id="objAdd"></select>
<span id="objChips" class="chips"></span>
<span class="tb-sep"></span>
<span id="tierBtns" class="toolbar"></span>
<span id="metricBtns" class="toolbar"></span>
</div>
<div id="mainchart" class="chart"></div>
<div class="note">对象可选：总体（含 writer/reader 角色）、节点聚合、单个客户端，可多选叠加对比
（多选时用指标按钮切换显示指标）。粒度：时间分桶（1 秒~1 天，按运行时长自动裁剪可用档）
与「自适应（按样本数）」双策略，横轴恒为时间轴；客户端曲线固定为自适应分桶。
悬停查看每个桶的全部统计量，滚轮/拖动缩放。点击客户端表行可快速加入对比。</div></div>
<div class="card" id="tracecard" style="display:none">
<h2>SDK 全链路（C++ trace）</h2>
<div class="sub" id="tmeta"></div>
<div class="toolbar"><span id="tgroups" class="toolbar"></span>
<select id="topSel" style="display:none"></select>
<span class="tb-sep"></span>
<label class="tl-label" for="tsizeSel">数据大小</label>
<select id="tsizeSel"></select></div>
<div id="tcards" class="grid"></div>
<h3 class="sec">分级时延（鼠标悬停看该块全部统计）</h3>
<div class="toolbar"><span class="tl-label">指标</span>
<span id="lvlMetric" class="toolbar"></span></div>
<div id="levels" class="levels"></div>
<h3 class="sec">阶段曲线（可多选阶段同图对比）</h3>
<div class="toolbar"><span id="stageBtns" class="toolbar"></span>
<span class="tb-sep"></span><span id="fmetricBtns" class="toolbar"></span>
<span class="tb-sep"></span><span id="ftierBtns"></span></div>
<div id="tchart" class="chart"></div>
<div class="note">阶段曲线的时间桶为 1 分钟（超过 1440 桶自动降为 10 分钟），
平均/最大值来自 SDK 字段统计；端到端曲线由 1s 桶聚合到同一档位。</div>
<h3 class="sec">阶段树（点击节点查看指标套件）</h3>
<div class="ttree" id="ttree"></div>
<h3 class="sec">最慢请求（Top-50）</h3>
<div class="wrapx"><table id="ttop"><thead><tr>
<th>start_time</th><th>trace_id</th><th>key</th><th>op</th><th>状态</th>
<th class="n">size</th><th class="n">total (µs)</th><th>主要阶段</th>
</tr></thead><tbody></tbody></table></div>
<h3 class="sec">状态分布</h3>
<div id="terrs"></div>
<div class="note">说明：① 采样门控——主行仅在采样命中或失败时出现；② put/upsert 的
memwrite_* 与 slice 明细仅代表最后一个副本的传输（memwrite_total_us 为全部副本合计）；
③ `-1` 表示该阶段拿不到时间戳（不参与时延统计）；④ slice 明细行 P1 仅计数。</div>
</div>
<div class="modal-mask" id="nodeModal" style="display:none">
 <div class="modal">
  <div class="modal-head"><b id="modalTitle"></b>
   <button id="modalClose" title="关闭" aria-label="关闭">✕</button></div>
  <div class="modal-body" id="modalBody"></div>
 </div>
</div>
<pre class="term" style="display:none"></pre>
<div class="card"><h2>客户端（点击行叠加曲线）</h2><div class="wrapx">
<table id="ctable"><thead><tr>
<th>客户端</th><th>角色</th><th>节点</th><th class="n">总数</th><th class="n">成功</th>
<th class="n">失败</th><th class="n">平均</th><th class="n">p50</th><th class="n">p99</th>
<th class="n">p999</th><th class="n">最大</th><th class="n">队列等待均</th><th class="n">日志</th>
</tr></thead><tbody></tbody></table></div>
<div class="note" id="datanote" style="display:none">全量数据 data.js 未加载（例如通过
WebUI/HTTP 直接打开）：客户端表仅显示 Top-20，点击行只能叠加已内嵌的客户端。
把 data.js 与 report.html 放在同一目录后重新打开即可查看全量。</div></div>
<div class="card"><h2>错误与异常</h2><div id="errors"></div></div>
<div class="card"><h2>现场索引</h2><div id="sources"></div></div>
<div class="note">数据文件：report.html（自包含）+ data.js（全量，深查用）。
由 logstats（map）+ report.py（reduce）生成。</div>
""".format(gran="1 秒 / 1 分钟")
    mon = data.get("monitors") or {}
    if mon.get("mem_host") or mon.get("mem_proc") or mon.get("urma"):
        body += """
<div class="card" id="monitorcard"><h2>节点监控（内存 / URMA 端口）</h2>
<div id="moncards" class="grid"></div>
<div id="memchart" class="chart"></div>
<div id="procchart" class="chart"></div>
<div id="urmachart" class="chart"></div>
<div class="note">整机内存来自 numastat（NUMA 分列），进程内存来自
/proc/&lt;pid&gt;/smaps_rollup；URMA 端口为 mdcli flit 计数器差分（GiB/s，1024 进制）。
时间轴与上方延迟图同源，可对照异常窗口；进程 RSS 持续增长会被标记「疑似泄露」。</div></div>
"""
    echarts_js = _read_echarts(echarts_path)
    html = _page_html("场景报表", subtitle, body, embedded, echarts_js,
                      "data.js", True, rel_base=rel_base,
                      data_rel="data.js")
    html_path = os.path.join(out_dir, "report.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    full_js = os.path.join(out_dir, "data.js")
    full_literal = json.dumps(data, ensure_ascii=False,
                              separators=(",", ":")).replace("</", "<\\/")
    with open(full_js, "w", encoding="utf-8") as f:
        f.write("window.__MC_FULL__ = " + full_literal + ";\n")
    return html_path, full_js


def render_overview(run_meta, scenarios, out_dir, rel_base=None):
    """run 级总览：各场景摘要卡 + 链接 + 现场索引 + 节点日志量。"""
    os.makedirs(out_dir, exist_ok=True)
    cards = []
    for sc in scenarios:
        m = sc.get("meta") or {}
        # aggregate EVERY data_size of the scenario: ok/fail/bytes/total sum,
        # wall sums (sizes run sequentially) -> qps from the sums;
        # percentiles take the worst size across the sweep (labelled below).
        blocks = [z.get("summary") for z in (sc.get("sizes") or {}).values()
                  if z and z.get("summary")]
        if blocks:
            s = {
                "ok": sum(int(b.get("ok") or 0) for b in blocks),
                "fail": sum(int(b.get("fail") or 0) for b in blocks),
                "total": sum(int(b.get("total") or 0) for b in blocks),
                "bytes": sum(int(b.get("bytes") or 0) for b in blocks),
                "wall_s": sum(float(b.get("wall_s") or 0) for b in blocks),
                "p50_ms": max((b.get("p50_ms") or 0) for b in blocks),
                "p99_ms": max((b.get("p99_ms") or 0) for b in blocks),
                "p999_ms": max((b.get("p999_ms") or 0) for b in blocks),
                "clients": len({c.get("name") for z in sc["sizes"].values()
                                for c in (z.get("clients") or [])}),
            }
            s["qps"] = (round(s["total"] / s["wall_s"], 2)
                        if s["wall_s"] > 0 else 0.0)
            s["fail_rate"] = (s["fail"] / s["total"]) if s["total"] else 0.0
            if len(blocks) > 1:
                s["pct_note"] = "跨 {} 个 data_size 取最差值".format(len(blocks))
        else:
            s = {}
        link = sc.get("report_rel") or ""
        ms = (sc.get("monitors") or {}).get("summary") or {}
        bits = []
        pm = ms.get("peak_mem_mb") or {}
        if pm:
            bits.append("峰值内存 " + ", ".join(
                "{}={} MB".format(k, v) for k, v in pm.items()))
        wp = ms.get("worst_proc")
        if wp and wp.get("leak_suspect"):
            bits.append('<span class="tag warn">RSS 疑似泄露 {} ({} MB / {}%)'
                        "</span>".format(wp.get("label") or "",
                                         wp.get("growth_mb"),
                                         wp.get("growth_pct")))
        pu = ms.get("peak_urma")
        if pu and pu.get("value") is not None:
            bits.append("端口峰值 {} GiB/s".format(pu["value"]))
        monline = ('<div class="note">监控：' + " · ".join(bits) +
                   "</div>") if bits else ""
        name = html.escape(str(m.get("config") or "scenario"))
        mode = html.escape(str(m.get("mode") or ""))
        cards.append("""
<div class="card"><h2>{name}</h2>
<div class="sub">{mode} · {proto} · {sizes} · {clients} 客户端 · 时长 {dur} · 目标 QPS {qt} {early}</div>
<div class="grid">
<div class="kv"><div class="k">成功</div><div class="v">{ok}</div></div>
<div class="kv"><div class="k">失败</div><div class="v">{fail}</div></div>
<div class="kv"><div class="k">QPS</div><div class="v">{qps}</div></div>
<div class="kv"><div class="k">p50</div><div class="v">{p50}<span class="u"> ms</span></div></div>
<div class="kv"><div class="k">p99</div><div class="v">{p99}<span class="u"> ms</span></div></div>
<div class="kv"><div class="k">p999</div><div class="v">{p999}<span class="u"> ms</span></div></div>
<div class="kv"><div class="k">失败率</div><div class="v">{fr}<span class="u"> %</span></div></div>
</div>
{pctnote}
{monline}
<div style="margin-top:10px"><a href="{link}">打开场景完整报表 →</a></div>
</div>""".format(
            name=name, mode=mode,
            proto=html.escape(str(m.get("protocol") or "-")),
            sizes=html.escape(", ".join(
                str(k) for k in (sc.get("sizes") or {}) if k != "all") or "-",
                quote=True),
            qt=m.get("qps_target") or "-",
            clients=m.get("clients") or "-",
            dur=_fmt_dur(s.get("wall_s") or 0),
            early='<span class="tag warn">提前停止/异常</span>'
            if m.get("early_stop") else "",
            ok=s.get("ok", "-"), fail=s.get("fail", "-"),
            qps=s.get("qps", "-"), p50=s.get("p50_ms", "-"),
            p99=s.get("p99_ms", "-"), p999=s.get("p999_ms", "-"),
            fr=round((s.get("fail_rate") or 0) * 100, 2),
            pctnote=('<div class="note">' + s["pct_note"] +
                     '；QPS 按各 size 时长之和计</div>')
            if s.get("pct_note") else "",
            link=html.escape(str(link), quote=True),
            monline=monline))
    nodes = run_meta.get("nodes") or []
    node_rows = "".join(
        "<tr><td class='mono'>{n}</td><td class='n'>{p}</td>"
        "<td class='n'>{b}</td></tr>".format(
            n=html.escape(str(n.get("node")), quote=True), p=n.get("pods", ""),
            b=_fmt_bytes(n.get("bytes")))
        for n in nodes)
    body = "".join(cards)
    if node_rows:
        body += ("<div class='card'><h2>节点日志量</h2><div class='wrapx'>"
                 "<table><thead><tr><th>节点</th><th class='n'>客户端数</th>"
                 "<th class='n'>日志量</th></tr></thead><tbody>" + node_rows +
                 "</tbody></table></div><div class='note'>宿主机日志："
                 "&lt;host_log_root&gt;/&lt;run_id&gt;/&lt;pod&gt;/"
                 "（CLEAN-UP 不删除）。</div></div>")
    subtitle = " · ".join(filter(None, [
        run_meta.get("run"), run_meta.get("mode"),
        ("开始 " + run_meta["started"]) if run_meta.get("started") else "",
        "生成于 " + time.strftime("%Y-%m-%d %H:%M:%S")]))
    page = _page_html("运行总览", subtitle, body, {"meta": run_meta},
                      "", None, False, rel_base=rel_base)
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
    ap.add_argument("--stats", nargs="+", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--echarts", default=os.path.join(
        os.path.dirname(_HERE), "webui", "web", "vendor", "echarts.min.js"))
    ap.add_argument("--config", default="")
    ap.add_argument("--mode", default="")
    ap.add_argument("--topk", type=int, default=TOPK_DEFAULT)
    args = ap.parse_args(argv)
    stats = load(args.stats)
    if not stats:
        sys.exit("[ERROR] no readable stats files")
    data = reduce_scenario(stats, {"config": args.config, "mode": args.mode})
    html, datajs = render_scenario(data, args.out, args.echarts,
                                   topk=args.topk)
    print("[report] {} (+ {})".format(html, os.path.basename(datajs)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
