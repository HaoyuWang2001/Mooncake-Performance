#!/usr/bin/env python3
"""tracestats.py — Mooncake SDK trace 行（breakdown[api]）就地统计（map 阶段）。

SDK 把高频 trace 打到 glog（`<glog 前缀> trace_id[...] breakdown[op] k[v] ...`），
本脚本流式解析主行（breakdown[<op>]，不含 `.slice` 明细，P1 只计数），产出
<glog>.trace.json：按方向（read/write/other）与 op 聚合每请求端到端时延与
各阶段时延（树状），供 report.py 跨客户端合并渲染。

字段格式：`name[value]` 全局括号感知匹配（value 可含空格 : . @ / -，不含 ]）；
数值字段按 int/float 解析，`-1` 为"该阶段拿不到时间戳"的哨兵（不参与时延统计，
仅计 neg）；start_time 为本地日期时间（含空格），转 epoch 用于时间桶。

阶段树（数据自检用，report 按此展示；对齐 Mooncake 源码统一 breakdown[op] 体系，
见 Mooncake 仓库 docs/yh/log-reference.md §14；TE/TENT 双引擎）：
  读 get_buffer（breakdown[get_buffer]；旧 get_breakdown 行仍兼容）:
    total ≈ query_total + select(local_endpoints+select_replica) + buf_alloc + read
            (query_total ≈ query_route + query_rpc + query_retry + glue)
            (read ≈ read_a + read_transfer_window + read_b；classic: read == memread_total)
  读 get_into（breakdown[get_into]）:
    total ≈ query + select(local_endpoints+select_replica) + read
            (read = offload_rpc + transfer_data + release_buffer + read_overhead)
  读 batch_get / batch_get_into（breakdown[batch_get|batch_get_into]）:
    total ≈ query + prep(逐 key 选择/分配汇总) + read
            (query 同样带 query_route/query_rpc/query_retry 下钻)
  写 put/put_from/upsert/upsert_from（breakdown[op]，engine[tent|classic]）:
    total ≈ prep_total(buf_alloc+memcpy+split) + checksum + prepare
            + query(upsert，带 query_route/rpc/retry 下钻) | put_start(put)
            + <传输段> + put_end(put) | finalize(upsert) + cache_invalidate + revoke
    <传输段> classic: memwrite_total(submit+queue+urma+teardown)
             tent:    admit + queue_prep + wire_span + propagate
  写 batch_put / batch_put_into（breakdown[op]）：put 族字段集（缺省字段跳过）
  remove / batchRemove：薄行（remove_rpc_us/total_us/status），归「其它」组
  辅助（不参与加和）: slice_count, replicas, bytes, engine 分布
  字段名归一：query_total_us→query_us、buf_alloc_us→alloc_us（报表树沿用规范名）

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
# 成功状态归一：C++ 读路径历史上把成功写成 status[read_ok]（写路径为
# status[ok]），严格 status=="ok" 会把全部读请求误判为失败。识别集合里的
# 成功变体统一按 ok 归档（旧镜像/旧产物的兼容；生产端应写 ok）。
SUCCESS_STATUSES = frozenset({"ok", "read_ok"})

STRING_FIELDS = {"trace_id", "breakdown", "key", "status", "type",
                 "remote_endpoint", "local_endpoint", "src", "dst",
                 "engine"}
META_FIELDS = {"start_time", "slice_idx", "offset", "bytes"}

READ_OPS = {"get", "get_batch", "get_into", "get_buffer", "batch_get",
            "batch_get_into", "batch_get_buffer", "batch_get_into_buffer"}
WRITE_OPS = {"put", "put_batch", "put_from", "upsert", "upsert_from",
             "batch_put", "batch_put_into", "batch_upsert_from",
             "batch_put_from"}

# 统一 breakdown[get_buffer] 的字段改名（log-reference §14.5/§14.9）：
# 报表阶段树沿用既有规范名 query_us/alloc_us，这里做一次等价归一。
FIELD_ALIASES = {"query_total_us": "query_us", "buf_alloc_us": "alloc_us"}

# 读侧（get_breakdown / breakdown[get_into]）：query+select+alloc+read 四个 L1；
# select 拆 local_endpoints/select_replica，read 拆 A/B 分段（get_buffer）或
# offload 三段+overhead（get_into）。
READ_TOP = ("query_us", "select_us", "alloc_us", "prep_us", "read_us")
# L2 子阶段：慢请求展开明细要回答「慢的一块里哪个子区间慢」——top 条目
# 除 L1 外一并保留这些字段（与 report.py 的 TREAD/TWRITE 树一致）。
READ_SUB = ("local_endpoints_us", "select_replica_us",
            "read_a_us", "read_transfer_window_us", "read_b_us",
            "offload_rpc_us", "transfer_data_us", "release_buffer_us",
            "read_overhead_us",
            "query_route_us", "query_rpc_us", "query_retry_us")
# 写侧公共段 + 双引擎传输段：classic 的 memwrite_*（字段名不变）与 TENT 四相位
# admit/queue_prep/wire_span/propagate 并列；逐行缺省字段跳过，残差各自成立。
WRITE_TOP = ("prep_total_us", "checksum_us", "prepare_us", "query_us",
             "put_start_us",
             "memwrite_total_us",
             "admit_us", "queue_prep_us", "wire_span_us", "propagate_us",
             "put_end_us", "finalize_us",
             "cache_invalidate_us", "revoke_us")
WRITE_SUB = ("prep_buf_alloc_us", "prep_memcpy_us", "prep_split_us",
             "memwrite_submit_us", "memwrite_queue_us",
             "memwrite_urma_us", "memwrite_teardown_us",
             "query_route_us", "query_rpc_us", "query_retry_us")

# 段3：阶段曲线（图2）采样字段 = L1 顶层 + total + residual。
# L2 子阶段（query_route/read_urma/...）不再逐时间采样（体积随字段数线性膨胀，
# 512 客户端不可控）；L2 细粒度分析在阶段树/指标套件里提供全局统计。
CHART_FIELDS = set(READ_TOP) | set(WRITE_TOP) | {"residual"}
FIELD_BASE_SECONDS = 1        # 采集粒度：按 1 秒累加
# 时间尺度收集规则（与 logstats.py / report.py 一致）：
#   尺度 s 回传 ⇔ 桶数 span/s ∈ [MIN_TIER_BUCKETS, MAX_TIER_BUCKETS]
TIER_STEPS = (1, 10, 60)
MIN_TIER_BUCKETS = 59
MAX_TIER_BUCKETS = 8192

TOP_N = 100


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
        i = text.find("get_breakdown")
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
    if ("read_a_us" in pairs or "read_transfer_window_us" in pairs
            or "offload_rpc_us" in pairs):
        return "read"
    if ("admit_us" in pairs or "wire_span_us" in pairs
            or "memwrite_total_us" in pairs):
        return "write"
    if bd in READ_OPS:
        return "read"
    if bd in WRITE_OPS:
        return "write"
    return "other"


def parse_line(line):
    """One glog line -> pairs or None when not a trace line.

    Two families (both bracket-pair `name[value]` payloads):
      - `breakdown[<op>] ...` (put family, get_into, batch ops, .slice/.slice_stats)
      - `get_breakdown key[...] ...` (get_buffer; normalized to
        breakdown="get_buffer" so the rest of the pipeline is unchanged)"""
    is_get = " get_breakdown " in line
    if not is_get and "breakdown[" not in line:
        return None
    i = line.find("trace_id[")
    if i < 0:
        i = line.find("get_breakdown") if is_get else line.find("breakdown[")
    pairs = dict(PAIR_RE.findall(line[i:]))
    if is_get:
        if "key" not in pairs:
            return None
        pairs["breakdown"] = "get_buffer"
    elif "breakdown" not in pairs:
        return None
    for _old, _new in FIELD_ALIASES.items():
        if _old in pairs and _new not in pairs:
            pairs[_new] = pairs.pop(_old)
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


def _resolve_ep_shift(path, marks):
    """宿主重算时的 epoch 基准校准（秒）。

    节点钟面串用 mktime（宿主本地时区）解析。map 在节点上跑时两边一致；
    但 --rebuild-report / 拉原始日志的本地重算跑在宿主上，宿主时区与
    节点（本项目约定 UTC，见 report._node_ts_epoch）不同时，mktime 会
    整体偏移整数个时区，导致所有行落不进 MC_PHASE 的 measured 窗口
    （曾出现 ops=0、trace 段空白）。UTC 宿主（含节点侧）直接返回 0；
    非 UTC 宿主用 marker 的客户端 epoch 自校准（15 分钟步进 + 跨文件抽样）。
    """
    if not marks:
        return 0.0
    m0 = marks.get(("measured", "start"))
    m1 = marks.get(("measured", "end"))
    if m0 is None:
        return 0.0
    if m1 is None:
        m1 = m0 + 3600.0
    off = -time.timezone
    if time.daylight and time.localtime(m0).tm_isdst > 0:
        off = -time.altzone
    if off == 0:
        return 0.0                       # UTC 宿主：无需校准
    try:
        size = os.path.getsize(path)
    except OSError:
        size = 0
    stride = max(1, size // (400 * 300))   # 约 400 个跨文件均匀样本
    samples = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for i, line in enumerate(f):
                if stride > 1 and i % stride:
                    continue
                pairs = parse_line(line)
                if not pairs:
                    continue
                bd = pairs.get("breakdown", "")
                if not bd or bd.endswith(".slice"):
                    continue
                if _num(pairs.get("total_us", "")) is None:
                    continue
                e = _start_epoch(pairs.get("start_time", ""))
                if e is not None:
                    samples.append(e)
                if len(samples) >= 400:
                    break
    except OSError:
        return off
    best_o, best_hits = off, 0
    for k in range(-56, 57):              # ±14 小时，15 分钟步进
        o = k * 900.0
        hits = sum(1 for e in samples if m0 <= e + o <= m1)
        if hits > best_hits:
            best_hits, best_o = hits, o
    return best_o


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


def _emit_step(span):
    """收集规则：最细的、桶数落在 [59, 8192] 的时间尺度；无达标回退 1s。"""
    span = float(span or 0)
    for cand in TIER_STEPS:
        if span > 0 and MIN_TIER_BUCKETS <= span / cand <= MAX_TIER_BUCKETS:
            return cand
    return FIELD_BASE_SECONDS


def _fmb_rows(fbuckets, step=1):
    """{epoch_s: [n,sum,max]} -> [[t, n, avg, max], ...] at the emit step.

    采集粒度是 1 秒；按 step（1/10/60）聚合，step 由该 op 的时间跨度按
    收集规则决定（与 logstats / 端到端曲线同一策略）。"""
    if not fbuckets:
        return []
    step = int(step or 1)
    if step > 1:
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
    return rows


def _bucket_rows(bmap, base=1, step=1):
    """bucket rows; t is emitted as epoch-seconds (base=60 for minute keys).

    step>1 aggregates consecutive base buckets onto the step grid (收集规则
    选出最细达标尺度时的产物)。"""
    src = bmap
    if int(step or 1) > 1:
        src = {}
        for t in sorted(bmap):
            k = t // step * step
            b = src.get(k)
            o = bmap[t]
            if b is None:
                b = src[k] = {"n": o["n"], "ok": o["ok"], "fail": o["fail"],
                              "sum": o["sum"], "max": o["max"],
                              "neg": o.get("neg", 0),
                              "h": dict(o.get("h") or {})}
            else:
                b["n"] += o["n"]
                b["ok"] += o["ok"]
                b["fail"] += o["fail"]
                b["sum"] += o["sum"]
                b["max"] = max(b["max"], o["max"])
                b["neg"] += o.get("neg", 0)
                for hk, hn in (o.get("h") or {}).items():
                    b["h"][hk] = b["h"].get(hk, 0) + hn
    out = []
    for t in sorted(src):
        b = src[t]
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
                        sample_rate=None, top_n=TOP_N, remote_path=None,
                        warmup_s=None):
    """One compact trace product from one or more glog segments.

    ``path`` may be a single file or a list. MC_LOG_DIR splits glog into
    <prog>.<host>.<user>.log.INFO.<date>-<time>.<pid> and glog rotates at
    max_log_size, so a long run produces several segments (each possibly
    GBs). All segments are scanned into the SAME aggregates: one product,
    exact stats, no per-file product sprawl."""
    paths = ([path] if isinstance(path, (str, os.PathLike))
             else [p for p in (path or [])])
    paths = [str(p) for p in paths if p]
    if not paths:
        raise ValueError("compute_trace_stats: no glog input")
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
                "field_minutes": {},   # chart field -> {epoch_s: [n, sum, max]}
                "seconds": {}, "minutes": {},
                "size_values": {}, "replicas": {}, "slice_count": {},
                "engine": {},
                "top": [], "_top_seq": 0,
            }
        return st

    # phase markers first (one pass per segment): marker timestamps are
    # absolute epochs, so the union across rotated segments is exact. The
    # epoch-shift calibration needs one segment that carries markers.
    marks = {}
    _mark_src = None
    for _p in paths:
        _pm = parse_phase_markers(_p)
        if _pm and _mark_src is None:
            _mark_src = _p
        for _k, _t in _pm.items():
            if _k not in marks:
                marks[_k] = _t
    ep_shift = (_resolve_ep_shift(_mark_src, marks)
                if _mark_src is not None else 0.0)
    phase_counts = {"setup": 0, "warmup": 0, "measured": 0, "teardown": 0}
    _warmup_until = None

    def _lines_multi():
        for _p in paths:
            try:
                with open(_p, "r", encoding="utf-8", errors="replace") as f:
                    for _ln in f:
                        yield _ln
            except OSError:
                continue

    for line in _lines_multi():
            lines_total += 1
            if ("breakdown[" in line or " get_breakdown " in line) \
                    and _truncated(line):
                parse_errors += 1
                continue
            pairs = parse_line(line)
            if pairs is None:
                continue
            bd = pairs.get("breakdown", "")
            if bd.endswith(".slice") or bd.endswith(".slice_stats"):
                lines_slice += 1
                continue           # P1: slice 明细/统计行仅计数（后续补充）
            lines_trace += 1
            eng = pairs.get("engine", "")
            status = pairs.get("status", "")
            ok = status in SUCCESS_STATUSES
            if ok:
                status = "ok"          # canonical label (read_ok -> ok)
            group = _group_of(pairs)
            # phase bucket: only MEASURED rows feed the op aggregates; the
            # rest (setup / warmup prefill+reads / teardown) are counted and
            # dropped — the report footnote shows the excluded sample counts.
            ep = _start_epoch(pairs.get("start_time", ""))
            if ep is None:
                ep = 0.0
            else:
                ep += ep_shift
            if warmup_s is not None and float(warmup_s or 0) > 0:
                # 单窗口模式：前 warmup_s 秒为 warmup 区（不计入正式统计）。
                # trace 的 start_time 是容器钟面时间，与编排机 epoch 不同域，
                # 因此以首条 trace 行做相对锚点（分段按文件名排序=时间顺序）。
                if _warmup_until is None and ep:
                    _warmup_until = ep + float(warmup_s)
                if _warmup_until is not None and ep and ep < _warmup_until:
                    phase = "warmup"
                else:
                    phase = _phase_of(ep, marks)
            else:
                phase = _phase_of(ep, marks)
            if phase is None:
                phase = "measured"      # legacy: no markers in this glog
            phase_counts[phase] += 1
            if phase != "measured":
                continue
            st = op_state(group, bd)
            if eng:
                st["engine"][eng] = st["engine"].get(eng, 0) + 1
            st["count"] += 1
            if ok:
                st["ok"] += 1
            else:
                st["fail"] += 1
            st["status"][status] = st["status"].get(status, 0) + 1

            ep_pre = _start_epoch(pairs.get("start_time", ""))
            if ep_pre is not None:
                ep_pre += ep_shift
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
                mkey = int(ep_pre) // FIELD_BASE_SECONDS * FIELD_BASE_SECONDS
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
            # total_us=-1 (no timestamp) has no duration -> never a "slowest".
            # stages 同时保留 L1 与 L2（展开行要下钻到子区间）。
            if total is not None and total >= 0:
                st["_top_seq"] += 1
                stage_tree = ((READ_TOP + READ_SUB) if group == "read"
                              else (WRITE_TOP + WRITE_SUB))
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
        # 该 op 的采集步长：由 seconds 桶的时间跨度按收集规则决定；
        # fminutes（阶段字段）与 seconds（端到端）统一使用这一档位。
        _lo = min(st["seconds"]) if st["seconds"] else None
        _hi = max(st["seconds"]) if st["seconds"] else None
        _span = (_hi - _lo + 1) if _lo is not None else 0
        op_step = _emit_step(_span)
        g["ops"][op] = {
            "count": st["count"], "ok": st["ok"], "fail": st["fail"],
            "status": st["status"], "fields": fields,
            "seconds": _bucket_rows(st["seconds"], 1, op_step),
            "minutes": _bucket_rows(st["minutes"], 60, op_step),
            "size_values": _value_hist(st["size_values"]),
            "replicas": _value_hist(st["replicas"]),
            "slice_count": _value_hist(st["slice_count"]),
            "engine": dict(st.get("engine") or {}),
            "top": [e for _t, _s, e in sorted(st["top"], key=lambda x: -x[0])],
        }
        # 段3：阶段时间桶（图2）；同一 op 内统一档位（= op_step）
        fmin = {}
        for fname, fb in (st.get("field_minutes") or {}).items():
            fmin[fname] = _fmb_rows(fb, op_step)
        if fmin:
            g["ops"][op]["fminutes"] = fmin
            g["ops"][op]["ftier"] = op_step

    # second pass: residual histogram per (group, op) — exact, cheap (local
    # disk, sampled lines); only when trace lines exist
    if lines_trace:
        res = {}
        if True:
            for line in _lines_multi():
                pairs = parse_line(line)
                if pairs is None:
                    continue
                bd = pairs.get("breakdown", "")
                if bd.endswith(".slice") or bd.endswith(".slice_stats"):
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

    _is_multi = len(paths) > 1
    meta = {
        # single file: file basename (report keys on it); rotated set: the
        # segment DIRECTORY name so clients[] keeps one row per process
        "source": (os.path.basename(os.path.dirname(paths[0]))
                   if _is_multi else os.path.basename(paths[0])),
        "bytes": sum(os.path.getsize(_p) for _p in paths
                     if os.path.isfile(_p)),
        # node-side twin of the glog (the path a report reader can ssh+cat);
        # the orchestrator passes it explicitly, older callers stay empty
        "remote_path": remote_path or "",
        "node": node, "pod": pod, "role": role, "size": size,
        "sample_rate": sample_rate,
        # 单窗口 warmup：warmup_s 为配置值；warmup_cutoff 是解析出的
        # trace 钟面绝对边界（首条 trace 行 + warmup_s），图表竖线用
        "warmup_s": (float(warmup_s) if warmup_s is not None else None),
        "warmup_cutoff": _warmup_until,
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
    ap.add_argument("--log", required=True, action="append",
                    help="glog file (repeatable: rotated segments are "
                         "merged into one product)")
    ap.add_argument("--out", default=None)
    ap.add_argument("--node", default=None)
    ap.add_argument("--pod", default=None)
    ap.add_argument("--role", default=None)
    ap.add_argument("--size", default=None)
    ap.add_argument("--sample-rate", type=float, default=None)
    ap.add_argument("--remote-path", default=None,
                    help="node-side path of the source glog (report index)")
    ap.add_argument("--warmup-s", type=float, default=None,
                    help="single-window warmup seconds: rows in the first "
                         "warmup_s (anchored at the first trace row) are "
                         "counted as phase=warmup and excluded from stats")
    ap.add_argument("--top-n", type=int, default=TOP_N)
    args = ap.parse_args(argv)
    logs = list(args.log or [])
    out = args.out or (logs[0] + ".trace.json")
    stats = compute_trace_stats(logs, node=args.node, pod=args.pod,
                                role=args.role, size=args.size,
                                sample_rate=args.sample_rate,
                                top_n=args.top_n,
                                remote_path=args.remote_path,
                                warmup_s=args.warmup_s)
    tmp = out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, separators=(",", ":"))
    os.replace(tmp, out)
    m = stats["meta"]
    _src = (os.path.basename(logs[0]) if len(logs) == 1
            else "{}/ ({} segments)".format(
                os.path.basename(os.path.dirname(logs[0])), len(logs)))
    print("[tracestats] {} trace_lines={} slice_lines={} ops={} -> {}".format(
        _src, m["trace_lines"], m["slice_lines"],
        sum(len(g["ops"]) for g in stats["groups"].values()), out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
