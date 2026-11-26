#!/usr/bin/env python3
"""kvtest 统一分位口径：中心=合并直方图插值，尾部=REQ sketch（无库时同为直方图）。

精确原始样本池化（pools）已退役——单一路径便于维护（见 scripts/quantiles.py）。
"""
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


def test_summary_uses_hist_interpolation(tmp_path):
    st1 = _mk_product(str(tmp_path), 1, {0: [1.0, 2.0, 3.0]})
    st2 = _mk_product(str(tmp_path), 2, {0: [4.0, 5.0]})
    # 产物 histogram 为空 -> 中心/尾部都无源 -> N/A（不再报 0）
    s0 = rp._summary_of([st1, st2])
    assert s0["p50_ms"] is None and s0["p99_ms"] is None
    assert s0["avg_ms"] == 3.0             # avg 精确（加权等价于全样本均值）
    # 带直方图的产物：分位来自合并直方图插值，且不超过真实 max
    for st in (st1, st2):
        st["histogram"] = {str(ls.bin_of(v, ls.BIN_EDGES_V2)): 1
                           for v in ((1.0, 2.0, 3.0) if st is st1
                                     else (4.0, 5.0))}
    out = rp._summary_of([st1, st2])
    assert out["p50_ms"] is not None and out["p99_ms"] is not None
    assert out["p50_ms"] <= 5.0 and out["p99_ms"] <= 5.0
    assert out["min_ms"] == 1.0


def test_series_columns_use_bucket_histograms(tmp_path):
    merged = {1000: (2, 2, 0, 3.0, 2.0, 0, 0,
                     {str(ls.bin_of(1.0, ls.BIN_EDGES)): 1,
                      str(ls.bin_of(2.0, ls.BIN_EDGES)): 1}),
              1001: (1, 1, 0, 10.0, 10.0, 0, 0,
                     {str(ls.bin_of(10.0, ls.BIN_EDGES)): 1})}
    cols = rp._series_columns(merged, 1, ls.BIN_EDGES)
    assert cols["avg"] == [1.5, 10.0] and cols["max"] == [2.0, 10.0]
    # 桶级分位 = 桶内直方图插值（近似，非精确池化）
    assert cols["p50"][0] > 0 and abs(cols["p50"][1] - 10.0) < 2.0
    assert cols["min"][0] > 0


def test_python_engine_products_keep_hist_path(tmp_path):
    st = _mk_product(str(tmp_path), 1, {0: [1.0]})
    st["meta"]["format"] = "python_log"           # 无 digest 的旧/引擎产物
    st["histogram"] = {str(ls.bin_of(1.0, ls.BIN_EDGES_V2)): 1}
    summary = rp._summary_of([st])
    assert summary["p99_ms"] is not None
    assert summary["pct_method"] == "hist"


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
