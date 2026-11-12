#!/usr/bin/env python3
"""logstats（map 阶段统计器）单测：行格式、分位数、直方图合并、桶、摘录。"""
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import logstats as ls   # noqa: E402


def _write(tmp_path, text, name="x.writer.log"):
    p = tmp_path / name
    p.write_text(text, encoding="utf-8")
    return str(p)


def _mk_lines(n, lat=1.0, ts_fmt="{:02d}:{:02d}:{:02d}.{:06d}"):
    out = []
    for i in range(n):
        t = ts_fmt.format(10, 0, i // 1000, (i % 1000) * 1000)
        out.append("RESULT\t{}\tpipeline_write\tk{}\t{:.6f}\tOK\t1\n".format(t, i, lat))
    return "".join(out)


def test_new_dated_format_and_basic_fields(tmp_path):
    text = (
        "RESULT\t2026-11-12 10:00:01.000000\tput\tk1\t1.000000\tOK\n"
        "RESULT\t2026-11-12 10:00:02.000000\tput\tk2\t2.000000\tOK\n"
        "RESULT\t2026-11-12 10:00:03.000000\tput\tk3\t3.000000\tOK\n"
        "RESULT\t2026-11-12 10:00:04.000000\tput\tk4\t4.000000\tOK\n"
        "RESULT\t2026-11-12 10:00:05.000000\tput\tk5\t100.000000\tERR:-200\n"
    )
    st = ls.compute_stats(_write(tmp_path, text))
    g = st["global"]
    assert g["total"] == 5 and g["ok"] == 4 and g["fail"] == 1
    assert g["p50_ms"] == 3.0 and g["max_ms"] == 4.0  # nearest-rank (client _percentile)
    assert g["exact"] is True
    assert st["errors"] == {"ERR:-200": 1}
    assert st["ops"]["put"]["fail"] == 1
    assert st["slowest"][0]["key"] == "k5" and st["slowest"][0]["status"] == "ERR:-200"
    assert st["error_excerpt"] and "k5" in st["error_excerpt"]
    assert st["meta"]["dated"] is True and st["meta"]["wall_s"] == 4.0
    # 4 per-second buckets (epoch-second keyed)
    # 4 OK seconds + 1 error second (errors land in buckets too)
    assert len(st["seconds"]) == 5
    assert st["minutes"][0][1] == 5  # minute bucket total count


def test_pipeline_reader_rows_qw_and_kc(tmp_path):
    text = (
        "RESULT\t2026-11-12 10:00:01.000000\tpipeline_read\tk1\t2.000000\tOK\t0.500000\t4\n"
        "RESULT\t2026-11-12 10:00:02.000000\tpipeline_read\tk2\t3.000000\tOK\t1.500000\t4\n"
        "RESULT\t2026-11-12 10:00:02.500000\tpipeline_warmup_read\tk3\t9.000000\tSKIP\t0.000000\t4\n"
    )
    st = ls.compute_stats(_write(tmp_path, text))
    g = st["global"]
    # key-based counting: kc=4 per row
    assert g["ok"] == 2 and g["key_ok"] == 8
    # warmup SKIP rows count as non-OK diagnostics
    assert g["fail"] == 1 and st["errors"].get("SKIP") == 1
    assert g["qw_avg_ms"] == 1.0 and g["qw_max_ms"] == 1.5
    assert st["ops"]["pipeline_read"]["keys"] == 8


def test_legacy_undated_rows_still_counted(tmp_path):
    text = (
        "RESULT\t10:00:01.000000\tput\tk1\t1.000000\tOK\n"
        "RESULT\t10:00:02.000000\tput\tk2\t2.000000\tOK\n"
    )
    st = ls.compute_stats(_write(tmp_path, text))
    g = st["global"]
    assert g["ok"] == 2 and g["p50_ms"] == 1.0  # nearest-rank
    assert st["meta"]["dated"] is False
    assert st["seconds"] == []        # no time buckets without dates


def test_histogram_merge_cluster_percentile(tmp_path):
    """两个客户端的直方图按 bin 相加 -> 合并分位数还原集群分布。"""
    def spread_lines(lo, hi, n, name):
        out = []
        for i in range(n):
            lat = lo + (hi - lo) * i / (n - 1)
            out.append("RESULT\t2026-11-12 10:00:{:02d}.{:06d}\tput\tk{}\t{:.6f}\tOK\n".format(
                i % 60, i * 1000, i, lat))
        return _write(tmp_path, "".join(out), name)

    a = ls.compute_stats(spread_lines(1.0, 3.0, 200, "a.log"))
    b = ls.compute_stats(spread_lines(2.0, 4.0, 200, "b.log"))
    merged = {}
    for st in (a, b):
        for b_, n in st["histogram"].items():
            merged[b_] = merged.get(b_, 0) + n
    p50 = ls.hist_percentile(merged, 0.5)
    p99 = ls.hist_percentile(merged, 0.99)
    # exact merged median 2.3 / p99 3.9; ratio-1.2 ladder error ~10-20%
    assert 2.3 * 0.8 <= p50 <= 2.3 * 1.2, p50
    assert 3.9 * 0.8 <= p99 <= 3.9 * 1.2, p99


def test_notify_rows_not_latency_samples(tmp_path):
    text = (
        "RESULT\t2026-11-12 10:00:01.000000\tpipeline_notify\tk1\t0\tERR:timeout target=n1:9201\n"
        "RESULT\t2026-11-12 10:00:02.000000\tpipeline_write\tk2\t1.000000\tOK\t1\n"
    )
    st = ls.compute_stats(_write(tmp_path, text))
    g = st["global"]
    assert g["ok"] == 1
    assert st["errors"].get("ERR:timeout target=n1:9201") == 1
    assert "pipeline_notify" in st["ops"]


def test_anomaly_windows(tmp_path):
    lines = []
    for i in range(50):
        t = "2026-11-12 10:00:{:02d}.{:06d}".format(i % 60, i * 1000)
        lines.append("RESULT\t{}\tput\tk{}\t1.000000\tOK\n".format(t, i))
    # error burst at second 10
    for i in range(5):
        lines.append("RESULT\t2026-11-12 10:00:10.500000\tput\te{}\t1.0\tERR:-200\n".format(i))
    st = ls.compute_stats(_write(tmp_path, "".join(lines)))
    kinds = [a["kind"] for a in st["anomalies"]]
    assert "error_burst" in kinds
    err_window = [a for a in st["anomalies"] if a["kind"] == "error_burst"][0]
    assert err_window["fail"] == 5


def test_cli_writes_stats_json(tmp_path):
    p = _write(tmp_path, _mk_lines(10))
    out = str(tmp_path / "out.json")
    r = subprocess.run(
        [sys.executable, os.path.join(ROOT, "scripts", "logstats.py"),
         "--log", p, "--out", out],
        capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    d = json.load(open(out, encoding="utf-8"))
    assert d["global"]["ok"] == 10
    assert r"[logstats]" in r.stdout


def test_topn_is_bounded(tmp_path):
    lines = []
    for i in range(1000):
        t = "2026-11-12 10:{:02d}:{:02d}.{:06d}".format(i // 60 // 60 % 60, i // 60 % 60, i % 60, 0)
        lines.append("RESULT\t{}\tput\tk{}\t{:.6f}\tOK\n".format(t, i, i / 1000.0))
    st = ls.compute_stats(_write(tmp_path, "".join(lines)), top_n=10)
    assert len(st["slowest"]) == 10
    assert st["slowest"][0]["lat_ms"] >= st["slowest"][-1]["lat_ms"]
    assert st["slowest"][0]["lat_ms"] == 0.999


def test_summaries_and_per_op_stats(tmp_path):
    text = (
        "META\t{\"mode\": \"pipeline_dual\", \"client_id\": 0}\n"
        "RESULT\t2026-11-12 10:00:01.000000\tput\tk1\t1.000000\tOK\n"
        "RESULT\t2026-11-12 10:00:02.000000\tput\tk2\t2.000000\tOK\n"
        "RESULT\t2026-11-12 10:00:03.000000\tput\tk3\t3.000000\tOK\n"
        "RESULT\t2026-11-12 10:00:04.000000\tget\tk4\t8.000000\tOK\n"
        "RESULT\t2026-11-12 10:00:05.000000\tget\tk5\t9.000000\tOK\n"
        "SUMMARY\t{\"mode\": \"benchmark\", \"total\": 5, \"ok\": 5, \"fail\": 0, "
        "\"wall_s\": 5.0, \"qps\": 1.0, \"max_depth\": 7}\n"
    )
    st = ls.compute_stats(_write(tmp_path, text))
    # summaries captured (META + SUMMARY)
    kinds = [k for k, _ in st["summaries"]]
    assert "meta" in kinds and "summary" in kinds
    summary = [v for k, v in st["summaries"] if k == "summary"][0]
    assert summary["wall_s"] == 5.0 and summary["max_depth"] == 7
    # per-op latency stats
    put = st["ops"]["put"]
    assert put["ok"] == 3 and put["p50_ms"] == 2.0 and put["max_ms"] == 3.0
    get = st["ops"]["get"]
    assert get["ok"] == 2 and get["avg_ms"] == 8.5 and get["min_ms"] == 8.0
