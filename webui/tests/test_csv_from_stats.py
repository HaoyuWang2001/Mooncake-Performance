#!/usr/bin/env python3
"""④-c CSV 数据源切到 logstats：parse_benchmark_log stats 优先 + 发现 + 出表。"""
import json
import os
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import benchmark as bm   # noqa: E402
import logstats as ls    # noqa: E402

WRITER_ROWS = "".join(
    "RESULT\t2026-11-12 10:00:{:02d}.{:06d}\tpipeline_write\tk{}\t{:.6f}\tOK\t10\n".format(
        10 + i // 10, i % 10, i, 1.0 + i * 0.1)
    for i in range(20))
READER_ROWS = "".join(
    "RESULT\t2026-11-12 10:00:{:02d}.{:06d}\tpipeline_read\tk{}\t{:.6f}\tOK\t0.5\t10\n".format(
        10 + i // 10, i % 10, i, 2.0 + i * 0.1)
    for i in range(20))
SUMMARY_W = json.dumps({"mode": "pipeline_writer", "wall_s": 10.0,
                        "bytes": 20 * 10 * (8 << 20)})
SUMMARY_R = json.dumps({"mode": "pipeline_reader", "wall_s": 10.0,
                        "bytes": 20 * 10 * (8 << 20), "max_depth": 5})


def _mk_stats(dirpath, base, text, summary):
    p = os.path.join(dirpath, base + ".log")
    with open(p, "w", encoding="utf-8") as f:
        f.write("META\t{}\n".format(json.dumps(
            {"mode": "pipeline_dual", "start_ts": 1762953600.0})))
        f.write(text)
        f.write("SUMMARY\t{}\n".format(summary))
    st = ls.compute_stats(p)
    with open(p + ".stats.json", "w", encoding="utf-8") as f:
        json.dump(st, f, ensure_ascii=False, separators=(",", ":"))
    os.remove(p)                      # stats-only pull: no raw log locally
    return p + ".stats.json"


def test_parse_benchmark_log_prefers_stats(tmp_path):
    sp = _mk_stats(str(tmp_path), "x.writer", WRITER_ROWS, SUMMARY_W)
    agg = bm.parse_benchmark_log(sp)
    # key-based: 20 rows x kc=10
    assert agg["ok"] == 200 and agg["total"] == 200 and agg["fail"] == 0
    assert agg["wall_s"] == 10.0
    assert agg["max_depth"] == 0            # writer summary has none
    # raw log sibling absent -> still resolves through .stats.json
    assert agg["p99_ms"] > 0
    reader = _mk_stats(str(tmp_path), "x.reader", READER_ROWS, SUMMARY_R)
    agg_r = bm.parse_benchmark_log(reader)
    assert agg_r["ok"] == 200 and agg_r["max_depth"] == 5
    assert agg_r["queue_wait_avg_ms"] == 0.5


def test_merge_across_instances(tmp_path):
    a = _mk_stats(str(tmp_path), "a.writer", WRITER_ROWS, SUMMARY_W)
    b = _mk_stats(str(tmp_path), "b.writer", WRITER_ROWS, SUMMARY_W)
    merged = bm.parse_benchmark_log([a, b])
    assert merged["ok"] == 400 and merged["total"] == 400
    assert merged["lat_sum_ms"] > 0


def test_glob_log_sources_prefers_stats(tmp_path):
    raw = tmp_path / "cfg.a.writer_n1_w1.log"
    raw.write_text("x", encoding="utf-8")
    st = tmp_path / "cfg.a.writer_n1_w1.log.stats.json"
    st.write_text("{}", encoding="utf-8")
    empty = tmp_path / "cfg.a.writer_n2_w2.log.stats.json"
    empty.write_text("{}", encoding="utf-8")
    got = bm._glob_log_sources(str(tmp_path), "cfg.a.writer_*.log")
    names = sorted(os.path.basename(p) for p in got)
    assert names == ["cfg.a.writer_n1_w1.log.stats.json",
                     "cfg.a.writer_n2_w2.log.stats.json"][::-1] or \
        names == ["cfg.a.writer_n1_w1.log.stats.json",
                  "cfg.a.writer_n2_w2.log.stats.json"]


def test_write_scenario_csv_from_stats_only(tmp_path, monkeypatch):
    """stats-only 拉取（本地无原始日志）也能出 pipeline CSV。"""
    monkeypatch.setattr(bm, "RESULTS_DIR", str(tmp_path / "results"))
    os.makedirs(str(tmp_path / "results" / "20261112-1000"))
    logdir = tmp_path / "logs" / "rank_smoke"
    logdir.mkdir(parents=True)
    stem = "rank_smoke.benchmark_c1_t1_8MB.dual_node1_c0_p9301"
    _mk_stats(str(logdir), stem + ".writer", WRITER_ROWS, SUMMARY_W)
    _mk_stats(str(logdir), stem + ".reader", READER_ROWS, SUMMARY_R)
    cfg = {
        "scenario": "pipeline", "protocol": "ub", "work_mode": "rank",
        "data_size": ["8MB"], "batch_size": 1, "duration": 0,
        "client": {"count_per_node": 1},
        "_writer_count": 1, "_reader_count": 1,
        "_key_space": {"8MB": 1000},
    }
    rows = bm.write_scenario_csv(
        str(logdir), cfg, "20261112-1000", "rank_smoke.json")
    roles = [(r["role"], r["instance"]) for r in rows]
    assert ("writer", "c0") in roles and ("reader", "c0") in roles
    assert ("writer", "TOTAL") in roles and ("reader", "TOTAL") in roles
    w = [r for r in rows if r["role"] == "writer" and r["instance"] == "c0"][0]
    assert w["total"] == 200 and w["success"] == 200
    assert w["client_count"] == 1
    assert "queue_wait_avg_ms" not in w   # writer rows carry no qw
    r = [r for r in rows if r["role"] == "reader" and r["instance"] == "c0"][0]
    assert r["queue_wait_avg_ms"] == 0.5
    assert r["max_depth"] == 5
