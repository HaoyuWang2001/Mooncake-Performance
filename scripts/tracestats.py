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

READ_TOP = ("query_total_us", "select_us", "buf_alloc_us", "memread_total_us")
WRITE_TOP = ("prep_total_us", "checksum_us", "prepare_us", "query_us",
             "put_start_us", "memwrite_total_us", "put_end_us", "finalize_us",
             "cache_invalidate_us", "revoke_us")

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
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return time.mktime(time.strptime(value.strip(), fmt))
        except (ValueError, OverflowError):
            continue
    return None


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


def _bucket_new():
    return {"n": 0, "ok": 0, "fail": 0, "sum": 0.0, "max": 0.0, "h": {}}


def _bucket_add(b, total_us, ok):
    b["n"] += 1
    b["ok"] += 1 if ok else 0
    b["fail"] += 0 if ok else 1
    if total_us is not None and total_us >= 0:
        b["sum"] += total_us
        b["max"] = max(b["max"], total_us)
        hb = ls.bin_of(total_us)
        b["h"][hb] = b["h"].get(hb, 0) + 1


def _bucket_rows(bmap):
    out = []
    for t in sorted(bmap):
        b = bmap[t]
        out.append([t, b["n"], b["ok"], b["fail"], round(b["sum"], 3),
                    round(b["max"], 6), 0.0, 0.0, b["h"]])
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
                "fields": {},          # field -> {n, sum, min, max, neg, hist}
                "seconds": {}, "minutes": {},
                "size_values": {}, "replicas": {}, "slice_count": {},
                "top": [], "_top_seq": 0,
            }
        return st

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            lines_total += 1
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
            if "memread_total_us" in pairs:
                group = "read"
            elif "memwrite_total_us" in pairs:
                group = "write"
            else:
                group = "other"
            st = op_state(group, bd)
            st["count"] += 1
            if ok:
                st["ok"] += 1
            else:
                st["fail"] += 1
            st["status"][status] = st["status"].get(status, 0) + 1

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
                                                 "min": None, "max": None,
                                                 "neg": 0, "hist": {}})
                fd["n"] += 1
                if num < 0:
                    fd["neg"] += 1
                    continue
                fd["sum"] += num
                fd["min"] = num if fd["min"] is None else min(fd["min"], num)
                fd["max"] = num if fd["max"] is None else max(fd["max"], num)
                hb = ls.bin_of(num)
                fd["hist"][hb] = fd["hist"].get(hb, 0) + 1

            total = parsed.get("total_us")
            # time buckets from start_time
            ep = _start_epoch(pairs.get("start_time", ""))
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

            # top-N slow requests (+ dominant stages for the report table)
            if total is not None:
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
                         "sum": round(fd["sum"], 3), "min": fd["min"],
                         "max": fd["max"],
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
            "minutes": _bucket_rows(st["minutes"]),
            "size_values": _value_hist(st["size_values"]),
            "replicas": _value_hist(st["replicas"]),
            "slice_count": _value_hist(st["slice_count"]),
            "top": [e for _t, _s, e in sorted(st["top"], key=lambda x: -x[0])],
        }

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
                if "memread_total_us" in pairs:
                    group, tops = "read", READ_TOP
                elif "memwrite_total_us" in pairs:
                    group, tops = "write", WRITE_TOP
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
                rb = ls.bin_of(max(r, 0.0))
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
