#!/usr/bin/env python3
"""Controller-side kvtest map+reduce driver.

Runs ON the k8s controller, next to the per-run data directory the pods'
`kubectl cp` landed in. Consumes one job JSON written by the orchestrator:

    {"dir": "<per-run data dir>", "out_dir": "<dir for reduced/meta>",
     "meta_base": {...reduce meta sans 'started'...}, "topk": 20,
     "instances": [{"samples": "<base>.samples.csv[.gz]",
                    "summary": "<base>.summary.json",
                    "trace": "<base>.trace.json[.gz] (legacy .log.glog.trace.json also accepted)",
                    "site_dir": "<node-side per-pod log dir, optional>",
                    "node": "...", "pod": "...", "ds": "8MB",
                    "start_epoch": 1234.5, "extra_bytes": 0,
                    "writer_ops": [...], "reader_ops": [...]}]}

For every instance it performs the SAME logstats map the orchestrator runs
locally (identical product schema incl. the REQ percentile digests), then ONE
reduce per data-size unit, writing <out_dir>/<unit>.reduced.json +
<unit>.meta.json and a per-instance FORMAL stats block (`instances`) that the
scenario CSV consumes — same percentile policy as the report.
The orchestrator pulls those, renders HTML locally and assembles the
CSV/overview — raw samples never leave the cluster and the controller never
renders. Any error exits non-zero -> the orchestrator falls back to the
local pull+map+reduce path (pods still hold everything; no data loss).
"""
import gzip
import json
import os
import re
import sys
import time

_HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _HERE)
import logstats as ls   # noqa: E402
import report as rp     # noqa: E402

_DS_RE = re.compile(r"\.kvtest_(?P<ds>[^./]+)\.")


def _maybe_gunzip(path):
    if not path.endswith(".gz"):
        return path
    out = path[:-len(".gz")]
    if not os.path.isfile(out):
        with gzip.open(path, "rb") as src, open(out, "wb") as dst:
            while True:
                chunk = src.read(1 << 20)
                if not chunk:
                    break
                dst.write(chunk)
    return out


def _trace_bytes(path):
    """meta.bytes of one trace product (the glog volume it distilled)."""
    p = _maybe_gunzip(path)
    try:
        with open(p, "r", encoding="utf-8") as f:
            doc = json.load(f)
        return int((doc.get("meta") or {}).get("bytes") or 0)
    except (OSError, ValueError, TypeError):
        return 0


def _map_one(d, inst, digest_k=None):
    """Both role products of one instance — the orchestrator-local
    _kvtest_map_stats semantics, verbatim."""
    samples = _maybe_gunzip(os.path.join(d, inst["samples"]))
    summary = os.path.join(d, inst["summary"])
    start = inst.get("start_epoch")
    products = []
    for role, ops in (("writer", inst.get("writer_ops") or []),
                      ("reader", inst.get("reader_ops") or [])):
        ops = frozenset(ops)
        payload = ls.kvtest_summary_payload(summary, ops)
        # Remote path must be the real NODE-side file (the orchestrator sends
        # site_dir = <host_log_root>/<run_id>/<pod>), never the controller's
        # staging copy (deleted right after the job). source_name stays a
        # basename exactly like the orchestrator-local map so the report's
        # source column/keys match in both data planes.
        site_dir = (inst.get("site_dir") or "").rstrip("/")
        remote_path = (site_dir + "/" + os.path.basename(samples)
                       if site_dir else samples)
        stem = os.path.basename(samples)
        for _suf in (".samples.csv.gz", ".samples.csv"):
            if stem.endswith(_suf):
                stem = stem[:-len(_suf)]
                break
        stats = ls.compute_stats(
            samples, top_n=100, context=20,
            node=inst.get("node") or "", pod=inst.get("pod") or "",
            role=role, remote_path=remote_path, size=inst.get("ds"),
            warmup_cutoff=inst.get("warmup_until"), digest_k=digest_k,
            rows_factory=lambda p, _ops=ops, _start=start:
                ls.iter_kvtest_rows(p, _start, _ops),
            summaries=[("summary", payload)] if payload is not None else [],
            source_name="{}.{}.samples.csv".format(stem, role),
            extra_meta={"format": "kvtest_samples", "scenario": "kvtest"})
        if not stats["meta"]["rows"]:
            continue
        if role == "reader":
            # verify_failures / notify queue wait exist only in summary.json
            try:
                with open(summary, "r", encoding="utf-8") as f:
                    doc = json.load(f)
            except (OSError, ValueError):
                doc = None
            _apply_summary_metrics(stats, doc)
        stats["meta"]["samples_bytes"] = stats["meta"]["bytes"]
        raw = inst.get("extra_bytes") or 0
        tb = _trace_bytes(os.path.join(d, inst["trace"])) \
            if inst.get("trace") else 0
        if tb or raw:
            stats["meta"]["bytes"] = tb + raw
        out = "{}.{}.stats.json".format(samples, role)
        tmp = out + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(stats, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp, out)
        products.append((role, out, samples, inst))
    return products


def _apply_summary_metrics(stats, summary_doc):
    """Verbatim port of benchmark._kvtest_apply_summary_metrics."""
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


def main(argv=None):
    argv = argv if argv is not None else sys.argv[1:]
    if len(argv) != 2 or argv[0] != "--job":
        sys.exit("usage: kvtest_controller_job.py --job <job.json>")
    with open(argv[1], "r", encoding="utf-8") as f:
        job = json.load(f)
    d = job["dir"]
    out_dir = job["out_dir"]
    os.makedirs(out_dir, exist_ok=True)
    by_unit = {}
    trace_by_unit = {}
    for inst in job["instances"]:
        for role, product, samples, src in _map_one(
                d, inst, job.get("digest_k")):
            m = _DS_RE.search(os.path.basename(product))
            ds = (m.group("ds") if m else src.get("ds") or "all")
            by_unit.setdefault(ds, []).append(product)
            if src.get("trace"):
                trace_by_unit.setdefault(ds, []).append(
                    _maybe_gunzip(os.path.join(d, src["trace"])))
    written = []
    for ds, products in sorted(by_unit.items()):
        loaded = rp.load(products)
        if not loaded:
            continue
        t0 = t1 = None
        for st in loaded:
            m = st.get("meta") or {}
            if m.get("ts_from") is not None:
                t0 = m["ts_from"] if t0 is None else min(t0, m["ts_from"])
            if m.get("ts_to") is not None:
                t1 = m["ts_to"] if t1 is None else max(t1, m["ts_to"])
        import time as _t
        started = (_t.strftime("%Y-%m-%d %H:%M:%S", _t.localtime(t0))
                   if t0 is not None
                   else _t.strftime("%Y-%m-%d %H:%M:%S"))
        meta = dict(job.get("meta_base") or {})
        meta.update({"started": started, "unit": ds, "unit_size": ds,
                     "clients": len(loaded), "early_stop":
                     bool(meta.get("early_stop"))})
        data = rp.reduce_scenario(
            loaded, meta, monitors=[],
            traces=rp.load(trace_by_unit.get(ds, [])))
        # CSV 专用 per-instance 正式统计（post-warmup 计数/均值/极值 +
        # 统一分位口径）；与报表 reduce 消费同一 logstats 助手，口径严格一致。
        _inst_out = []
        _warm = None
        for inst in job["instances"]:
            su, se = inst.get("warmup_until"), inst.get("start_epoch")
            if su is not None and se is not None and float(su) > float(se):
                _warm = float(su) - float(se)
            if inst.get("iid") is None:
                continue
            try:
                _samples = _maybe_gunzip(os.path.join(d, inst["samples"]))
                _ops = (set(inst.get("writer_ops") or [])
                        | set(inst.get("reader_ops") or [])
                        | {"notify_rpc", "notify_http",
                           "notify_queue_wait"})
                _stats = ls.kvtest_instance_ops_stats(
                    _samples, _ops, warmup_sec=_warm,
                    digest_k=job.get("digest_k"),
                    start_epoch=inst.get("start_epoch"))
            except Exception as exc:
                print("[job] WARN instance stats i{}: {}".format(
                    inst.get("iid"), exc), flush=True)
                _stats = {}
            _inst_out.append({"iid": inst["iid"],
                              "node": inst.get("node") or "",
                              "pod": inst.get("pod") or "",
                              "ops": _stats})
        if _inst_out:
            data["instances"] = _inst_out
        red = os.path.join(out_dir, "{}.reduced.json".format(ds))
        with open(red + ".tmp", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(red + ".tmp", red)
        with open(os.path.join(out_dir, "{}.meta.json".format(ds)), "w",
                  encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=1)
        print("[job] {} -> {}".format(ds, red), flush=True)
        written.append(red)
    if not written:
        sys.exit("[job] no products mapped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
