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


def _merge_buckets(bucket_lists):
    """Merge [t,c,ok,fail,sum,max,qsum,qmax,hist] bucket rows across clients."""
    merged = {}
    for buckets in bucket_lists:
        for row in buckets or []:
            t = int(row[0])
            b = merged.get(t)
            if b is None:
                b = merged[t] = [0, 0, 0, 0.0, 0.0, 0.0, 0.0, {}]
            b[0] += int(row[1])
            b[1] += int(row[2])
            b[2] += int(row[3])
            b[3] += float(row[4])
            b[4] = max(b[4], float(row[5]))
            b[5] += float(row[6])
            b[6] = max(b[6], float(row[7]))
            for k, n in (row[8] or {}).items():
                b[7][int(k)] = b[7].get(int(k), 0) + int(n)
    return merged


def _series_columns(merged, step):
    """Columnar series for the chart (compact JSON, fixed key order)."""
    out = {"t": [], "c": [], "ok": [], "fail": [], "avg": [], "p50": [],
           "p99": [], "p999": [], "max": [], "qw": [], "qwmax": []}
    for t in sorted(merged):
        c, ok, fail, s, mx, qs, qm, h = merged[t]
        out["t"].append(int(t))
        out["c"].append(int(c))
        out["ok"].append(int(ok))
        out["fail"].append(int(fail))
        out["avg"].append(round(s / ok, 4) if ok else 0.0)
        out["p50"].append(round(ls.hist_percentile(h, 0.50), 4) if h else 0.0)
        out["p99"].append(round(ls.hist_percentile(h, 0.99), 4) if h else 0.0)
        out["p999"].append(round(ls.hist_percentile(h, 0.999), 4) if h else 0.0)
        out["max"].append(round(mx, 4))
        out["qw"].append(round(qs / c, 4) if c else 0.0)
        out["qwmax"].append(round(qm, 4))
    return out


def _summary_of(stats):
    """Cluster summary from merged per-instance globals."""
    total = ok = fail = ok_rows = 0
    lat_sum = 0.0
    qw_sum = 0.0
    qw_n = 0
    qw_max = 0.0
    max_depth = 0
    ts_from = ts_to = None
    hist = {}
    wall = 0.0
    for st in stats:
        g = st.get("global") or {}
        meta = st.get("meta") or {}
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
            wall = max(wall, float(payload.get("wall_s", 0) or 0))
    span = (ts_to - ts_from) if (ts_from is not None and ts_to is not None) else 0.0
    wall = wall or span
    return {
        "total": total, "ok": ok, "fail": fail,
        "fail_rate": (fail / total) if total else 0.0,
        "avg_ms": round(lat_sum / ok_rows, 4) if ok_rows else 0.0,
        "p50_ms": round(ls.hist_percentile(hist, 0.50), 4) if hist else 0.0,
        "p95_ms": round(ls.hist_percentile(hist, 0.95), 4) if hist else 0.0,
        "p99_ms": round(ls.hist_percentile(hist, 0.99), 4) if hist else 0.0,
        "p999_ms": round(ls.hist_percentile(hist, 0.999), 4) if hist else 0.0,
        "p9999_ms": round(ls.hist_percentile(hist, 0.9999), 4) if hist else 0.0,
        "max_ms": round(ls.hist_percentile(hist, 1.0), 4) if hist else 0.0,
        "qw_avg_ms": round(qw_sum / qw_n, 4) if qw_n else None,
        "qw_max_ms": round(qw_max, 4) if qw_n else None,
        "max_depth": max_depth,
        "wall_s": round(wall, 3),
        "qps": round(total / wall, 2) if wall > 0 else 0.0,
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
        "minutes": _series_columns(
            _merge_buckets([st.get("minutes") or []]), 60),
        "slowest": st.get("slowest") or [],
        "errors": st.get("errors") or {},
    }


def _role_series(stats):
    """{role: {step1, step60}} cluster series merged per role."""
    by_role = {}
    for st in stats:
        role = _role_of(st)
        by_role.setdefault(role, []).append(st)
    out = {}
    for role, group in by_role.items():
        s1 = _merge_buckets([st.get("seconds") or [] for st in group])
        s60 = _merge_buckets([st.get("minutes") or [] for st in group])
        out[role] = {"step1": _series_columns(s1, 1),
                     "step60": _series_columns(s60, 60)}
    return out


def reduce_size(stats, meta):
    """One data-size block: summary / per-role series / clients / errors."""
    role_series = _role_series(stats)
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
    embed_step = 1
    max_seconds = max((len((role_series[r]["step1"]["t"]))
                       for r in role_series), default=0)
    if max_seconds > EMBED_SECONDS_MAX:
        embed_step = 60
    return {
        "summary": _summary_of(stats),
        "embed_step": embed_step,
        "series": role_series,
        "clients": clients,
        "errors": errors,
        "error_excerpt": excerpt,
        "error_excerpt_status": excerpt_status,
        "slowest": slowest,
        "anomalies": anomalies,
    }


def _sources(stats):
    out = []
    nodes = {}
    for st in stats:
        meta = st.get("meta") or {}
        node = meta.get("node") or "?"
        b = int(meta.get("bytes") or 0)
        out.append({"node": node, "pod": meta.get("pod") or "",
                    "role": meta.get("role") or "",
                    "source": meta.get("source") or "",
                    "remote_path": meta.get("remote_path") or "",
                    "bytes": b})
        agg = nodes.setdefault(node, {"node": node, "bytes": 0, "pods": 0})
        agg["bytes"] += b
        agg["pods"] += 1
    out.sort(key=lambda s: (s["node"], s["pod"], s["source"]))
    return out, sorted(nodes.values(), key=lambda n: -n["bytes"])


def reduce_scenario(stats, meta, monitors=None):
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
    sources, nodes = _sources(stats)
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
        for ts, rx, tx in samples:
            if prev is not None:
                dt_ns = ts - prev[0]
                drx, dtx = rx - prev[1], tx - prev[2]
                if dt_ns > 0 and drx >= 0 and dtx >= 0:
                    sec = dt_ns / 1e9
                    t.append(round(ts / 1e9, 3))
                    rxr.append(round(drx * flit_bytes / sec / gib, 4))
                    txr.append(round(dtx * flit_bytes / sec / gib, 4))
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
let curRole = 'all'; let curStep = 0;
function Z(){return S[curSize]||null;}
function rolesOf(z){return Object.keys((z&&z.series)||{});}
function rolesOrder(z){const r=rolesOf(z);const o=['all','writer','reader'];
return o.filter(x=>r.includes(x)).concat(r.filter(x=>!o.includes(x)));}
let chart=null;
function seriesFor(step){
  const z=Z(); if(!z) return null;
  const src=z.series[curRole]||z.series[rolesOf(z)[0]]||{};
  return src[step===1?'step1':'step60']||{t:[],c:[],ok:[],fail:[],avg:[],p50:[],p99:[],p999:[],max:[],qw:[],qwmax:[]};
}
function buildChart(){
  const box=document.getElementById('mainchart');
  if(!chart) chart=echarts.init(box);
  const col=seriesFor(curStep);
  if(!col||!col.t.length){chart.clear();return;}
  const step = curStep===1?1:60;
  const qps = col.c.map((c,i)=>Math.round(c/step*100)/100);
  const x = col.t.map(t=>t*1000);
  const opt={
    animation:false, textStyle:{fontSize:12},
    tooltip:{trigger:'axis', confine:true, formatter:function(ps){
      const i=ps[0].dataIndex; const t=col.t[i];
      const rows=[['时间',fullts(t)],['请求数',col.c[i]],['成功',col.ok[i]],
        ['失败',col.fail[i]],['QPS',qps[i]],['平均(ms)',col.avg[i]],
        ['p50(ms)',col.p50[i]],['p99(ms)',col.p99[i]],['p999(ms)',col.p999[i]],
        ['最大(ms)',col.max[i]],['队列等待均(ms)',col.qw[i]],['队列等待峰(ms)',col.qwmax[i]]];
      return '<b>'+fullts(t)+'</b><br>'+rows.map(r=>r[0]+'：'+r[1]).join('<br>');
    }},
    legend:{top:0,type:'scroll'},
    grid:{left:56,right:20,top:36,bottom:64},
    xAxis:{type:'time',axisLabel:{formatter:v=>hhmmss(v/1000)}},
    yAxis:[{type:'value',name:'ms',scale:true},
           {type:'value',name:'QPS',position:'right',splitLine:{show:false}}],
    dataZoom:[{type:'inside'},{type:'slider',height:18,bottom:8}],
    series:[
      {name:'QPS',type:'bar',yAxisIndex:1,data:x.map((v,i)=>[v,qps[i]]),
       itemStyle:{color:'rgba(14,116,144,.35)'},barMaxWidth:8,large:true},
      {name:'失败',type:'bar',yAxisIndex:1,data:x.map((v,i)=>[v,col.fail[i]]),
       itemStyle:{color:'rgba(214,69,69,.55)'},barMaxWidth:8,large:true},
      {name:'平均',type:'line',data:x.map((v,i)=>[v,col.avg[i]]),
       showSymbol:false,lineStyle:{width:1,type:'dashed'}},
      {name:'p50',type:'line',data:x.map((v,i)=>[v,col.p50[i]]),
       showSymbol:false,lineStyle:{width:1.5}},
      {name:'p99',type:'line',data:x.map((v,i)=>[v,col.p99[i]]),
       showSymbol:false,lineStyle:{width:1.8}},
      {name:'p999',type:'line',data:x.map((v,i)=>[v,col.p999[i]]),
       showSymbol:false,lineStyle:{width:1.5}},
      {name:'最大',type:'line',data:x.map((v,i)=>[v,col.max[i]]),
       showSymbol:false,lineStyle:{width:1,type:'dotted'}}
    ]
  };
  const z=Z();
  if(z && z.anomalies && z.anomalies.length){
    opt.series[0].markLine={symbol:'none',silent:true,data:z.anomalies.slice(0,50)
      .map(a=>({xAxis:a.t*1000,lineStyle:{color:'rgba(232,163,61,.6)'},
                label:{show:false}}))};
  }
  chart.setOption(opt,true);
}
function renderSummary(){
  const z=Z(); if(!z) return;
  const s=z.summary; const g=document.getElementById('cards');
  g.innerHTML='';
  const target=D.meta&&D.meta.qps_target?Number(D.meta.qps_target):0;
  g.appendChild(kv('成功', s.ok));
  g.appendChild(kv('失败', s.fail));
  g.appendChild(kv('失败率', (s.fail_rate*100).toFixed(2), '%'));
  g.appendChild(kv('QPS', s.qps));
  if(target)g.appendChild(kv('QPS 达成率', (s.qps/target*100).toFixed(1), '%（目标 '+target+'）'));
  g.appendChild(kv('平均时延', s.avg_ms, 'ms'));
  g.appendChild(kv('p50', s.p50_ms, 'ms'));
  g.appendChild(kv('p95', s.p95_ms, 'ms'));
  g.appendChild(kv('p99', s.p99_ms, 'ms'));
  g.appendChild(kv('p999', s.p999_ms, 'ms'));
  g.appendChild(kv('p9999', s.p9999_ms, 'ms'));
  g.appendChild(kv('最大', s.max_ms, 'ms'));
  if(s.qw_avg_ms!=null)g.appendChild(kv('队列等待均', s.qw_avg_ms, 'ms'));
  if(s.qw_max_ms!=null)g.appendChild(kv('队列等待峰', s.qw_max_ms, 'ms'));
  if(s.max_depth)g.appendChild(kv('最大队列深度', s.max_depth));
  g.appendChild(kv('吞吐', (s.total/s.wall_s/1048576).toFixed(1), 'MB/s'));
  g.appendChild(kv('时长', fmtDur(s.wall_s)));
  g.appendChild(kv('客户端', s.clients));
}
function renderTable(){
  const z=Z(); if(!z) return;
  const tb=document.querySelector('#ctable tbody'); tb.innerHTML='';
  const clients=(FULL&&FULL.sizes&&FULL.sizes[curSize]&&
    FULL.sizes[curSize].clients)||z.clients;
  clients.slice().sort((a,b)=>(b.p99_ms-a.p99_ms)).forEach(c=>{
    const tr=el('tr');
    [c.name,c.role,c.node,c.total,c.ok,c.fail,
     c.avg_ms,c.p50_ms,c.p99_ms,c.p999_ms,c.max_ms,
     c.qw_avg_ms==null?'-':c.qw_avg_ms,fmtBytes(c.bytes)].forEach((v,i)=>{
      const td=el('td', (i>=3?'n':''), String(v));
      tr.appendChild(td);
    });
    tr.title='点击叠加该客户端分钟曲线';
    tr.style.cursor='pointer';
    tr.onclick=()=>overlayClient(c.name);
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
function overlayClient(name){
  const z=Z(); if(!z||!chart) return;
  const all=((FULL&&FULL.sizes&&FULL.sizes[curSize]&&
    FULL.sizes[curSize].clients)||z.clients||[]);
  const c=all.find(x=>x.name===name); if(!c) return;
  const col=c.minutes||{t:[],p50:[],p99:[],max:[],c:[],qw:[]};
  const x=col.t.map(t=>t*1000);
  chart.setOption({series:[
    {name:name+' p99',type:'line',data:x.map((v,i)=>[v,col.p99[i]]),
     showSymbol:false,lineStyle:{width:2,type:'dashed'}},
    {name:name+' max',type:'line',data:x.map((v,i)=>[v,col.max[i]]),
     showSymbol:false,lineStyle:{width:1,type:'dotted'}}
  ]});
}
function renderAll(){
  const z=Z(); if(!z) return;
  document.getElementById('sizeSel').style.display =
    Object.keys(S).length>1?'inline-block':'none';
  renderSummary(); buildChart(); renderTable(); renderErrors();
  if(!window.__srcDone){renderSources();window.__srcDone=true;}
}
let monCharts={};
function _monChart(id, series, yname){
  const box=document.getElementById(id); if(!box||!series.length) return false;
  let c=monCharts[id]; if(!c) c=monCharts[id]=echarts.init(box);
  c.setOption({animation:false,textStyle:{fontSize:12},
    tooltip:{trigger:'axis',confine:true},
    legend:{top:0,type:'scroll'},
    grid:{left:56,right:20,top:36,bottom:56},
    xAxis:{type:'time',axisLabel:{formatter:v=>hhmmss(v/1000)}},
    yAxis:{type:'value',name:yname,scale:true},
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
    Object.keys(S).forEach(k=>{const o=el('option',null,k);o.value=k;sizeSel.appendChild(o);});
    sizeSel.value=curSize;
    sizeSel.onchange=()=>{curSize=sizeSel.value;curRole='all';curStep=0;
      renderRoleButtons(); renderAll();};
  }
  renderRoleButtons(); renderAll(); renderMonitors();
  window.addEventListener('resize',()=>{
    if(chart)chart.resize();
    Object.keys(monCharts).forEach(k=>monCharts[k].resize());
  });
}
function renderRoleButtons(){
  const box=document.getElementById('roles'); box.innerHTML='';
  const z=Z(); if(!z) return;
  rolesOrder(z).forEach(r=>{
    const b=el('button', r===curRole?'on':'', r==='all'?'全部':r);
    b.onclick=()=>{curRole=r;renderRoleButtons();buildChart();};
    box.appendChild(b);
  });
  const steps=z.embed_step===1?[[1,'1 秒粒度'],[60,'1 分钟粒度']]:[[60,'1 分钟粒度']];
  const box2=document.getElementById('steps');
  box2.innerHTML='';
  steps.forEach(([v,label])=>{
    const b=el('button', (v===1&&curStep===0)||(v===60&&curStep===1)?'on':'', label);
    b.onclick=()=>{curStep=v===1?0:1;renderRoleButtons();buildChart();};
    box2.appendChild(b);
  });
}
document.addEventListener('DOMContentLoaded',mount);
"""


def _page_html(title, subtitle, body_html, data_json, echarts_js, data_js_path,
               full_loader):
    # JSON is embedded as a JS literal; escape the closing-script sequence.
    data_literal = json.dumps(data_json, ensure_ascii=False,
                              separators=(",", ":")).replace("</", "<\\/")
    loader = ""
    if full_loader:
        loader = ('<script src="{}" onerror="window.__mcDataMissing=1">'
                  '</script>'.format(full_loader))
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
{loader}
<script>{app}</script>
</body></html>
""".format(title=title, subtitle=subtitle, body=body_html,
           css=_PAGE_CSS, echarts=echarts_js, data=data_literal,
           loader=loader, app=_APP_JS)


def render_scenario(data, out_dir, echarts_path, topk=TOPK_DEFAULT):
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
        keep = [c for c in clients[:topk]]
        slim = dict(z)
        slim["clients"] = keep
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
<div class="card"><div class="toolbar">
<select id="sizeSel"></select>
<span id="roles" class="toolbar"></span>
<span id="steps" class="toolbar"></span>
</div>
<div id="cards" class="grid"></div></div>
<div class="card"><h2>时间线</h2>
<div id="mainchart" class="chart"></div>
<div class="note">悬停查看每个桶的全部统计量；滚轮/拖动缩放；点击下方客户端行可叠加该客户端的分钟曲线。
粒度：{gran}。大范围以上采用聚合桶（非逐点），Top 慢点表保留单点级证据。</div></div>
<pre class="term" style="display:none"></pre>
<div class="card"><h2>客户端（点击行叠加曲线）</h2><div class="wrapx">
<table id="ctable"><thead><tr>
<th>客户端</th><th>角色</th><th>节点</th><th class="n">总数</th><th class="n">成功</th>
<th class="n">失败</th><th class="n">平均</th><th class="n">p50</th><th class="n">p99</th>
<th class="n">p999</th><th class="n">最大</th><th class="n">队列等待均</th><th class="n">日志</th>
</tr></thead><tbody></tbody></table></div></div>
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
                      "data.js", True)
    html_path = os.path.join(out_dir, "report.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)
    full_js = os.path.join(out_dir, "data.js")
    full_literal = json.dumps(data, ensure_ascii=False,
                              separators=(",", ":")).replace("</", "<\\/")
    with open(full_js, "w", encoding="utf-8") as f:
        f.write("window.__MC_FULL__ = " + full_literal + ";\n")
    return html_path, full_js


def render_overview(run_meta, scenarios, out_dir):
    """run 级总览：各场景摘要卡 + 链接 + 现场索引 + 节点日志量。"""
    os.makedirs(out_dir, exist_ok=True)
    cards = []
    for sc in scenarios:
        m = sc.get("meta") or {}
        z = next(iter((sc.get("sizes") or {}).values()), {})
        s = (z or {}).get("summary") or {}
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
        cards.append("""
<div class="card"><h2>{name}</h2>
<div class="sub">{mode} · {clients} 客户端 · 时长 {dur} {early}</div>
<div class="grid">
<div class="kv"><div class="k">成功</div><div class="v">{ok}</div></div>
<div class="kv"><div class="k">失败</div><div class="v">{fail}</div></div>
<div class="kv"><div class="k">QPS</div><div class="v">{qps}</div></div>
<div class="kv"><div class="k">p50</div><div class="v">{p50}<span class="u"> ms</span></div></div>
<div class="kv"><div class="k">p99</div><div class="v">{p99}<span class="u"> ms</span></div></div>
<div class="kv"><div class="k">p999</div><div class="v">{p999}<span class="u"> ms</span></div></div>
<div class="kv"><div class="k">失败率</div><div class="v">{fr}<span class="u"> %</span></div></div>
</div>
{monline}
<div style="margin-top:10px"><a href="{link}">打开场景完整报表 →</a></div>
</div>""".format(
            name=m.get("config") or "scenario", mode=m.get("mode") or "",
            clients=m.get("clients") or "-",
            dur=_fmt_dur(s.get("wall_s") or 0),
            early='<span class="tag warn">提前停止/异常</span>'
            if m.get("early_stop") else "",
            ok=s.get("ok", "-"), fail=s.get("fail", "-"),
            qps=s.get("qps", "-"), p50=s.get("p50_ms", "-"),
            p99=s.get("p99_ms", "-"), p999=s.get("p999_ms", "-"),
            fr=round((s.get("fail_rate") or 0) * 100, 2), link=link,
            monline=monline))
    nodes = run_meta.get("nodes") or []
    node_rows = "".join(
        "<tr><td class='mono'>{n}</td><td class='n'>{p}</td>"
        "<td class='n'>{b}</td></tr>".format(
            n=n.get("node"), p=n.get("pods", ""), b=_fmt_bytes(n.get("bytes")))
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
    html = _page_html("运行总览", subtitle, body, {"meta": run_meta},
                      "", None, False)
    path = os.path.join(out_dir, "overview.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
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
