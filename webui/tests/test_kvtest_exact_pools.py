#!/usr/bin/env python3
"""kvtest 精确池化：合并分位数 = 全样本池化排序取值（绝对真值），缺样本 = N/A。"""
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import logstats as ls   # noqa: E402
import report as rp     # noqa: E402


def _mk_product(tmpdir, iid, lats_per_sec, op="upsert_from", role="writer",
                missing=False, start=1000.0):
    """Write a fake samples.csv + its .stats.json product; return the st."""
    samples = os.path.join(tmpdir, "cfg.i{}_p9101.kvtest_8MB.samples.csv"
                           .format(iid))
    with open(samples, "w", encoding="utf-8") as f:
        f.write("elapsed_us,instance_id,operation,request_index,"
                "latency_ms,status,bytes\n")
        for sec, lats in lats_per_sec.items():
            for i, lat in enumerate(lats):
                f.write("{},{},{},{},{:.6f},OK,{}\n".format(
                    int(sec) * 1000000, iid, op, i, lat, 1 << 20))
    st_path = "{}.{}.stats.json".format(samples, role)
    ok = sum(len(v) for v in lats_per_sec.values())
    all_lats = [x for v in lats_per_sec.values() for x in v]
    st = {
        "_path": st_path,
        "meta": {"format": "kvtest_samples", "role": role,
                 "source": "cfg.i{}".format(iid), "ts_from": start,
                 "node": "n{}".format(iid), "bucket_step": 1},
        "global": {"key_total": ok, "key_ok": ok, "key_fail": 0, "ok": ok,
                   "avg_ms": round(sum(all_lats) / ok, 4) if ok else 0.0,
                   "min_ms": min(all_lats) if all_lats else 0.0,
                   "max_ms": max(all_lats) if all_lats else 0.0},
        "histogram": {},
        "summaries": [],
        "seconds": [[int(start) + int(sec), ok, len(lats_per_sec[sec]), 0,
                     sum(lats_per_sec[sec]), max(lats_per_sec[sec]), 0, 0,
                     {}] for sec in lats_per_sec],
        "ops": {op: {"count": ok}},
    }
    if missing:
        os.remove(samples)
    return st


def test_pools_merge_and_exact_pct(tmp_path):
    st1 = _mk_product(str(tmp_path), 1, {0: [1.0, 2.0, 3.0]})
    st2 = _mk_product(str(tmp_path), 2, {0: [4.0, 5.0]})
    pools = rp._kvtest_scope_pools([st1, st2])
    assert pools["missing"] == []
    assert pools["lats"] == [1.0, 2.0, 3.0, 4.0, 5.0]
    assert pools["buckets"] == {1000: [1.0, 2.0, 3.0, 4.0, 5.0]}
    summary = rp._summary_of([st1, st2], pools)
    # exact: pooled sorted [1,2,3,4,5], p50 = index round(.5*4)=2 -> 3.0
    assert summary["p50_ms"] == 3.0
    assert summary["min_ms"] == 1.0 or summary["min_ms"] == 0.1  # gmin from globals
    # single-exact/histogram path must NOT have been used
    assert summary["p99_ms"] == round(ls._exact_percentile(
        pools["lats"], 0.99), 4)


def test_missing_samples_degrades_to_na(tmp_path):
    st1 = _mk_product(str(tmp_path), 1, {0: [1.0, 2.0]})
    st2 = _mk_product(str(tmp_path), 2, {0: [3.0]}, missing=True)
    pools = rp._kvtest_scope_pools([st1, st2])
    assert len(pools["missing"]) == 1
    summary = rp._summary_of([st1, st2], pools)
    for k in ("p50_ms", "p95_ms", "p99_ms", "p999_ms", "p9999_ms"):
        assert summary[k] is None          # N/A, never an approximation
    assert summary["avg_ms"] == 2.0        # avg/min/max stay exact
    merged = {1000: (3, 3, 0, 6.0, 3.0, 0.0, 0.0, {})}
    cols = rp._series_columns(merged, 1, pools=pools)
    assert cols["p99"] == [None] and cols["min"] == [None]
    assert cols["avg"] == [2.0] and cols["max"] == [3.0]


def test_series_columns_exact_per_second(tmp_path):
    st1 = _mk_product(str(tmp_path), 1, {0: [1.0, 2.0], 1: [10.0]})
    pools = rp._kvtest_scope_pools([st1])
    merged = {1000: (2, 2, 0, 3.0, 2.0, 0, 0, {}),
              1001: (1, 1, 0, 10.0, 10.0, 0, 0, {})}
    cols = rp._series_columns(merged, 1, pools=pools)
    # order-statistic convention (same as the map's exact percentiles):
    # [1.0, 2.0] -> p50 = index round(.5*(n-1)) = 0 -> 1.0
    assert cols["p50"] == [1.0, 10.0]
    assert cols["min"] == [1.0, 10.0]
    assert cols["max"] == [2.0, 10.0]


def test_no_kvtest_products_keeps_legacy(tmp_path):
    st = _mk_product(str(tmp_path), 1, {0: [1.0]})
    st["meta"]["format"] = "python_log"           # python-engine product
    assert rp._kvtest_scope_pools([st]) is None
    # _summary_of without pools keeps the histogram path (zero-fallback)
    summary = rp._summary_of([st])
    assert summary["p99_ms"] == 0.0


def test_reduce_render_cli_roundtrip(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(rp, "reduce_scenario",
                        lambda stats, meta: {"hello": 1})
    reduced = os.path.join(str(tmp_path), "reduced.json")
    monkeypatch.setattr(rp, "load", lambda paths: [{"fake": 1}])
    rc = rp.main(["--stats", "ignored", "--out", "x", "--config", "c",
                  "--mode", "kvtest", "--reduce-only",
                  "--out-reduced", reduced])
    assert rc == 0 and json.load(open(reduced)) == {"hello": 1}
    monkeypatch.setattr(rp, "render_scenario",
                        lambda data, out, echarts, topk=10: ("/a.html",
                                                             "/a/data.js"))
    out = os.path.join(str(tmp_path), "out")
    rc = rp.main(["--render-only", "--reduced", reduced, "--out", out])
    assert rc == 0
    assert "(+ data.js)" in capsys.readouterr().out
