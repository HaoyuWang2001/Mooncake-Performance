#!/usr/bin/env python3
"""tracestats.py — Mooncake SDK trace 行（breakdown[api]）就地统计（map 阶段）。

SDK 把高频 trace 打到 glog（`<glog 前缀> trace_id[...] breakdown[op] k[v] ...`），
本脚本流式解析主行（breakdown[<op>]，不含 `.slice` 明细，P1 只计数），产出
<glog>.trace.json：按方向（read/write/other）与 op 聚合每请求端到端时延与
各阶段时延（树状），供 report.py 跨客户端合并渲染。

字段格式：`name[value]` 全局括号感知匹配（value 可含空格 : . @ / -，不含 ]）；
数值字段按 int/float 解析，`-1` 为"该阶段拿不到时间戳"的哨兵（不参与时延统计，
仅计 neg）；start_time 为本地日期时间（含空格），转 epoch 用于时间桶。

阶段树（数据自检用，report 按此展示）：
  读: total ≈ query_total(route+rpc) + select + buf_alloc + memread_total
            (read_submit + read_queue + read_urma + read_teardown)
  写: total ≈ prep_total(buf_alloc+memcpy+split) + checksum + prepare + query
            + put_start + memwrite_total(submit+queue+urma+teardown)
            + put_end + finalize + cache_invalidate + revoke
  辅助（不参与加和）: slice_count, *_urma_min/avg_us, replicas, memcpy_us

采样门控（报表需标注）：主行仅在采样命中或失败时出现；样本数 ≠ 请求总数。

用法：
  python3 tracestats.py --log <glog> [--out <file>] [--node N] [--pod P]
                        [--role R] [--size S] [--sample-rate 1.0] [--top-n 50]
"""
import argparse
import heapq
import json
import os
import re
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)
import logstats as ls   # noqa: E402  (bin ladder + histogram percentile shared)

PAIR_RE = re.compile(r"(\w+)\[([^\]]*)\]")
STRING_FIELDS = {"trace_id", "breakdown", "key", "status", "type",
                 "remote_endpoint", "local_endpoint", "src", "dst"}
META_FIELDS = {"start_time", "slice_idx", "offset", "bytes"}

READ_OPS = {"get", "get_batch", "get_into", "get_buffer", "batch_get"}
WRITE_OPS = {"put", "put_batch", "put_from", "upsert", "upsert_from",
             "batch_put"}

READ_TOP = ("query_total_us", "select_us", "buf_alloc_us", "memread_total_us")
WRITE_TOP = ("prep_total_us", "checksum_us", "prepare_us", "query_us",
             "put_start_us", "memwrite_total_us", "put_end_us", "finalize_us",
             "cache_invalidate_us", "revoke_us")

# 段3：阶段曲线（图2）采样字段 = L1 顶层 + total + residual。
# L2 子阶段（query_route/read_urma/...）不再逐时间采样（体积随字段数线性膨胀，
# 512 客户端不可控）；L2 细粒度分析在阶段树/指标套件里提供全局统计。
CHART_FIELDS = set(READ_TOP) | set(WRITE_TOP) | {"residual"}
FIELD_MIN_SECONDS = 60        # 阶段时间桶初档：1 分钟
FIELD_MAX_BUCKETS = 1440      # 单字段桶数上限（超出自动降档到 10 分钟）

TOP_N = 50


def _num(value):
    """name[value] value -> int/float/None (non-numeric)."""
    v = (value or "").strip()
    if not v:
        return None
    try:
        if re.fullmatch(r"[-+]?\d+", v):
            return int(v)
        return float(v)
    except ValueError:
        return None


def _start_epoch(value):
    """'YYYY-MM-DD HH:MM:SS[.micro]' (node local wall clock) -> epoch float.

    Keeps the fractional part so the value matches logstats._ts_to_epoch
    exactly (the two timelines must share one basis)."""
    text = (value or "").strip()
    if " " not in text:
        return None
    date_part, hms = text.split(" ", 1)
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


def _truncated(line):
    """True when a trace line was cut mid-field (glog caps a message at
    ~30KB; a long trace line loses its tail and would otherwise be parsed as
    a valid shorter request)."""
    text = line.rstrip("\n")
    # the glog prefix itself ends with "...:123] " -> count brackets only in
    # the payload that starts at the trace marker
    i = text.find("trace_id[")
    if i < 0:
        i = text.find("breakdown[")
    if i < 0:
        return False
    seg = text[i:]
    return seg.count("[") != seg.count("]") or not seg.endswith("]")


def _group_of(pairs):
    """read / write / other for one parsed line.

    Field presence is authoritative (it implies which stage tree applies);
    an early-exit request may be missing the telling field, so the op name
    is the fallback — otherwise a failed put silently lands in 'other' and
    loses its residual."""
    bd = pairs.get("breakdown", "")
    if "memread_total_us" in pairs:
        return "read"
    if "memwrite_total_us" in pairs:
        return "write"
    if bd in READ_OPS:
        return "read"
    if bd in WRITE_OPS:
        return "write"
    return "other"


def parse_line(line):
    """One glog line -> (pairs, trace_start_epoch) or None when not a trace
    line. Bracket-aware: values may contain spaces (start_time)."""
    if "breakdown[" not in line:
        return None
    i = line.find("trace_id[")
    if i < 0:
        i = line.find("breakdown[")
    pairs = dict(PAIR_RE.findall(line[i:]))
    if "breakdown" not in pairs:
        return None
    return pairs


PHASE_MARK_RE = re.compile(
    r"^MC_PHASE (warmup|measured) (start|end) (\d+\.\d+)")


def parse_phase_markers(path):
    """Collect MC_PHASE markers from one glog: {('warmup','start'): t, ...}.
    Written by the client to stderr (same fd2 -> glog stream)."""
    marks = {}
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                m = PHASE_MARK_RE.match(line.strip())
                if m:
                    key = (m.group(1), m.group(2))
                    if key not in marks:          # first occurrence wins
                        marks[key] = float(m.group(3))
    except OSError:
        pass
    return marks


def _phase_of(t, marks):
    """Classify one trace timestamp into setup / warmup / measured / teardown
    by the marker windows. No markers at all (legacy clients) -> None (the
    caller treats every row as measured to keep the old behaviour)."""
    if not marks:
        return None
    w0 = marks.get(("warmup", "start"))
    m0 = marks.get(("measured", "start"))
    m1 = marks.get(("measured", "end"))
    if w0 is not None and t < w0:
        return "setup"
    if w0 is not None and m0 is not None and w0 <= t < m0:
        return "warmup"
    if w0 is None and m0 is not None and t < m0:
        return "setup"            # warmup disabled: pre-measured = setup
    if m0 is not None and (m1 is None or t <= m1):
        return "measured"
    if m1 is not None and t > m1:
        return "teardown"
    # markers exist but this row predates all of them
    return "setup"


def _bucket_new():
    return {"n": 0, "ok": 0, "fail": 0, "sum": 0.0, "max": 0.0, "h": {},
            "neg": 0}


def _bucket_add(b, total_us, ok):
    """Count one request into a time bucket. total_us < 0 is the SDK's
    "no timestamp" sentinel: it counts as neg (n) but never as duration."""
    b["n"] += 1
    b["ok"] += 1 if ok else 0
    b["fail"] += 0 if ok else 1
    if total_us is not None and total_us >= 0:
        b["sum"] += total_us
        b["max"] = max(b["max"], total_us)
        hb = ls.bin_of(total_us, ls.BIN_EDGES_US)
        b["h"][hb] = b["h"].get(hb, 0) + 1
    else:
        b["neg"] += 1


def _fmb_add(bmap, field, mkey, value):
    """Add one stage sample into its per-minute bucket [n, sum, max]."""
    fb = bmap.setdefault(field, {})
    b = fb.get(mkey)
    if b is None:
        fb[mkey] = [1, value, value]
    else:
        b[0] += 1
        b[1] += value
        if value > b[2]:
            b[2] = value


def _fmb_rows(fbuckets):
    """{min_epoch: [n,sum,max]} -> [[t, n, avg, max], ...] with an automatic
    coarsening step (10 min) when the bucket count would exceed the cap."""
    if not fbuckets:
        return [], FIELD_MIN_SECONDS
    step = FIELD_MIN_SECONDS
    if len(fbuckets) > FIELD_MAX_BUCKETS:
        step = FIELD_MIN_SECONDS * 10
        agg = {}
        for t, b in fbuckets.items():
            k = int(t) // step * step
            a = agg.get(k)
            if a is None:
                agg[k] = [b[0], b[1], b[2]]
            else:
                a[0] += b[0]
                a[1] += b[1]
                a[2] = max(a[2], b[2])
        fbuckets = agg
    rows = []
    for t in sorted(fbuckets):
        n, ssum, mx = fbuckets[t]
        rows.append([int(t), int(n), round(ssum / n, 3) if n else 0.0,
                     round(mx, 3)])
    return rows, step


def _bucket_rows(bmap, base=1):
    """bucket rows; t is emitted as epoch-seconds (base=60 for minute keys)."""
    out = []
    for t in sorted(bmap):
        b = bmap[t]
        out.append([t * base, b["n"], b["ok"], b["fail"], round(b["sum"], 3),
                    round(b["max"], 6), 0.0, 0.0, b["h"], b["neg"]])
    return out


def _value_hist(value_counts, cap=24):
    """Small-cardinality value histogram (size / replicas / slice_count)."""
    items = sorted(value_counts.items(), key=lambda kv: -kv[1])
    if len(items) <= cap:
        return {str(k): v for k, v in items}
    keep = dict(items[:cap])
    keep["other"] = sum(v for _k, v in items[cap:])
    return {str(k): v for k, v in keep.items()}


def compute_trace_stats(path, node=None, pod=None, role=None, size=None,
                        sample_rate=None, top_n=TOP_N):
    top_n = max(1, int(top_n or 0))
    ops = {}                       # (group, op) -> aggregate dict
    lines_total = lines_trace = lines_slice = parse_errors = 0
    ts_from = ts_to = None

    def op_state(group, op):
        key = (group, op)
        st = ops.get(key)
        if st is None:
            st = ops[key] = {
                "group": group, "op": op,
                "count": 0, "ok": 0, "fail": 0, "status": {},
                "fields": {},          # field -> {n, sum, sumsq, min, max, neg, hist}
                "field_minutes": {},   # chart field -> {min_epoch: [n, sum, max]}
                "seconds": {}, "minutes": {},
                "size_values": {}, "replicas": {}, "slice_count": {},
                "top": [], "_top_seq": 0,
            }
        return st

    # phase markers first (one pass): classification windows for the C++
    # trace rows. Legacy clients (no markers) -> every row is measured.
    marks = parse_phase_markers(path)
    phase_counts = {"setup": 0, "warmup": 0, "measured": 0, "teardown": 0}

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            lines_total += 1
            if "breakdown[" in line and _truncated(line):
                parse_errors += 1
                continue
            pairs = parse_line(line)
            if pairs is None:
                continue
            bd = pairs.get("breakdown", "")
            if bd.endswith(".slice"):
                lines_slice += 1
                continue           # P1: slice 明细仅计数（后续补充）
            lines_trace += 1
            status = pairs.get("status", "")
            ok = status == "ok"
            group = _group_of(pairs)
            # phase bucket: only MEASURED rows feed the op aggregates; the
            # rest (setup / warmup prefill+reads / teardown) are counted and
            # dropped — the report footnote shows the excluded sample counts.
            ep = _start_epoch(pairs.get("start_time", ""))
            if ep is None:
                ep = 0.0
            phase = _phase_of(ep, marks)
            if phase is None:
                phase = "measured"      # legacy: no markers in this glog
            phase_counts[phase] += 1
            if phase != "measured":
                continue
            st = op_state(group, bd)
            st["count"] += 1
            if ok:
                st["ok"] += 1
            else:
                st["fail"] += 1
            st["status"][status] = st["status"].get(status, 0) + 1

            ep_pre = _start_epoch(pairs.get("start_time", ""))
            # numeric field statistics (histogram + sum/min/max; -1 counts as neg)
            parsed = {}
            for k, v in pairs.items():
                if k in STRING_FIELDS or k in META_FIELDS or k == "trace_id":
                    continue
                if not k.endswith("_us"):
                    continue
                num = _num(v)
                if num is None:
                    continue
                parsed[k] = num
                fd = st["fields"].setdefault(k, {"n": 0, "sum": 0.0,
                                                 "sumsq": 0.0,
                                                 "min": None, "max": None,
                                                 "neg": 0, "hist": {}})
                fd["n"] += 1
                if num < 0:
                    fd["neg"] += 1
                    continue
                fd["sum"] += num
                fd["sumsq"] += num * num
                fd["min"] = num if fd["min"] is None else min(fd["min"], num)
                fd["max"] = num if fd["max"] is None else max(fd["max"], num)
                hb = ls.bin_of(num, ls.BIN_EDGES_US)
                fd["hist"][hb] = fd["hist"].get(hb, 0) + 1

            total = parsed.get("total_us")
            # 阶段分钟桶（图2）：L1 顶层 + residual（端到端减去 L1 之和）
            if ep_pre is not None:
                mkey = int(ep_pre) // FIELD_MIN_SECONDS * FIELD_MIN_SECONDS
                tops = READ_TOP if group == "read" else (
                    WRITE_TOP if group == "write" else ())
                stage_sum = 0.0
                for cf in tops:
                    cv = parsed.get(cf)
                    if cv is not None and cv >= 0:
                        stage_sum += cv
                        _fmb_add(st["field_minutes"], cf, mkey, cv)
                if group in ("read", "write") and total is not None \
                        and total >= 0:
                    _fmb_add(st["field_minutes"], "residual", mkey,
                             total - stage_sum)
            # time buckets from start_time
            if ep is not None:
                ts_from = ep if ts_from is None else min(ts_from, ep)
                ts_to = ep if ts_to is None else max(ts_to, ep)
                sec = int(ep)
                _bucket_add(st["seconds"].setdefault(sec, _bucket_new()),
                            total, ok)
                _bucket_add(st["minutes"].setdefault(sec // 60, _bucket_new()),
                            total, ok)

            # small-cardinality distributions
            size_v = pairs.get("size")
            if size_v not in (None, ""):
                st["size_values"][size_v] = st["size_values"].get(size_v, 0) + 1
            rep = pairs.get("replicas", pairs.get("replica_count"))
            if rep not in (None, ""):
                st["replicas"][rep] = st["replicas"].get(rep, 0) + 1
            sc = pairs.get("slice_count")
            if sc not in (None, ""):
                st["slice_count"][sc] = st["slice_count"].get(sc, 0) + 1

            # top-N slow requests (+ dominant stages for the report table);
            # total_us=-1 (no timestamp) has no duration -> never a "slowest"
            if total is not None and total >= 0:
                st["_top_seq"] += 1
                stage_tree = READ_TOP if group == "read" else WRITE_TOP
                stages = {f: parsed[f] for f in stage_tree if f in parsed
                          and parsed[f] > 0}
                cand = (total, st["_top_seq"])
                entry = {"trace_id": pairs.get("trace_id", ""),
                         "key": pairs.get("key", ""),
                         "op": bd, "status": status,
                         "size": pairs.get("size", ""),
                         "start_time": pairs.get("start_time", ""),
                         "total_us": total, "stages": stages}
                if len(st["top"]) < top_n:
                    heapq.heappush(st["top"], (cand[0], cand[1], entry))
                elif cand[0] > st["top"][0][0]:
                    heapq.heapreplace(st["top"], (cand[0], cand[1], entry))

    # finalize: residual histogram + top list + fields cleanup
    groups = {}
    for (group, op), st in ops.items():
        top_fields = READ_TOP if group == "read" else (
            WRITE_TOP if group == "write" else ())
        fields = {}
        for k, fd in st["fields"].items():
            fields[k] = {"n": fd["n"], "neg": fd["neg"],
                         "sum": round(fd["sum"], 3),
                         "sumsq": round(fd.get("sumsq", 0.0), 3),
                         "min": fd["min"], "max": fd["max"],
                         "hist": {str(b): n for b, n in sorted(fd["hist"].items())}}
        # residual = total_us - sum(top-level stage values), per request
        res_hist = {}
        res_n = 0
        res_sum = 0.0
        # residual needs per-request values; recompute is impossible without a
        # second pass — use median-of-fields approximation? No: keep it exact
        # by accumulating during the scan below (second pass over the file).
        g = groups.setdefault(group, {"ops": {}})
        g["ops"][op] = {
            "count": st["count"], "ok": st["ok"], "fail": st["fail"],
            "status": st["status"], "fields": fields,
            "seconds": _bucket_rows(st["seconds"]),
            "minutes": _bucket_rows(st["minutes"], 60),
            "size_values": _value_hist(st["size_values"]),
            "replicas": _value_hist(st["replicas"]),
            "slice_count": _value_hist(st["slice_count"]),
            "top": [e for _t, _s, e in sorted(st["top"], key=lambda x: -x[0])],
        }
        # 段3：阶段时间桶（图2）；同一 op 内统一档位
        fmin = {}
        tier = FIELD_MIN_SECONDS
        for fname, fb in (st.get("field_minutes") or {}).items():
            rows, tier = _fmb_rows(fb)
            fmin[fname] = rows
        if fmin:
            g["ops"][op]["fminutes"] = fmin
            g["ops"][op]["ftier"] = tier

    # second pass: residual histogram per (group, op) — exact, cheap (local
    # disk, sampled lines); only when trace lines exist
    if lines_trace:
        res = {}
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                pairs = parse_line(line)
                if pairs is None:
                    continue
                bd = pairs.get("breakdown", "")
                if bd.endswith(".slice"):
                    continue
                group = _group_of(pairs)
                if group == "read":
                    tops = READ_TOP
                elif group == "write":
                    tops = WRITE_TOP
                else:
                    continue
                total = _num(pairs.get("total_us", ""))
                if total is None or total < 0:
                    continue
                stage_sum = 0.0
                for f in tops:
                    n = _num(pairs.get(f, ""))
                    if n is not None and n > 0:
                        stage_sum += n
                r = total - stage_sum
                key = (group, bd)
                rd = res.setdefault(key, {"n": 0, "sum": 0.0, "min": None,
                                          "max": None, "hist": {}})
                rd["n"] += 1
                rd["sum"] += r
                rd["min"] = r if rd["min"] is None else min(rd["min"], r)
                rd["max"] = r if rd["max"] is None else max(rd["max"], r)
                rb = ls.bin_of(max(r, 0.0), ls.BIN_EDGES_US)
                rd["hist"][rb] = rd["hist"].get(rb, 0) + 1
        for (group, op), rd in res.items():
            g = groups.get(group, {}).get("ops", {}).get(op)
            if g is None:
                continue
            g["residual"] = {
                "n": rd["n"], "sum": round(rd["sum"], 3),
                "min": rd["min"], "max": rd["max"],
                "hist": {str(b): n for b, n in sorted(rd["hist"].items())},
            }

    meta = {
        "source": os.path.basename(path),
        "bytes": os.path.getsize(path),
        "node": node, "pod": pod, "role": role, "size": size,
        "sample_rate": sample_rate,
        "lines": lines_total,
        "trace_lines": lines_trace,
        "slice_lines": lines_slice,
        "parse_errors": parse_errors,
        "ts_from": ts_from, "ts_to": ts_to,
        "dated": ts_from is not None,
        "bucket_unit": "s",
        # histogram ladder this product was built with; report refuses to
        # interpret legacy files (which used the ms ladder for µs values)
        "bins": "us-v1",
        # phase classification (MC_PHASE markers from the client):
        # `markers` False = legacy glog, every row was counted as measured.
        "markers": bool(marks),
        "phase_counts": phase_counts,
        "phases": { "{}_{}".format(ph, act): t for (ph, act), t in marks.items() },
    }
    return {"meta": meta, "groups": groups}


def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Compute compact stats from Mooncake SDK trace lines (map)")
    ap.add_argument("--log", required=True)
    ap.add_argument("--out", default=None)
    ap.add_argument("--node", default=None)
    ap.add_argument("--pod", default=None)
    ap.add_argument("--role", default=None)
    ap.add_argument("--size", default=None)
    ap.add_argument("--sample-rate", type=float, default=None)
    ap.add_argument("--top-n", type=int, default=TOP_N)
    args = ap.parse_args(argv)
    out = args.out or (args.log + ".trace.json")
    stats = compute_trace_stats(args.log, node=args.node, pod=args.pod,
                                role=args.role, size=args.size,
                                sample_rate=args.sample_rate,
                                top_n=args.top_n)
    tmp = out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, out)
    m = stats["meta"]
    print("[tracestats] {} trace_lines={} slice_lines={} ops={} -> {}".format(
        os.path.basename(args.log), m["trace_lines"], m["slice_lines"],
        sum(len(g["ops"]) for g in stats["groups"].values()), out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
