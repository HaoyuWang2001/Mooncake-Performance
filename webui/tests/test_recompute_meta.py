#!/usr/bin/env python3
"""本地重算（失败/STOP/--rebuild-report）必须从文件名/目录回填
size/role/node，否则所有 data_size 并入 "all"、角色页签消失。"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

import benchmark as bm   # noqa: E402

ROWS = "".join(
    "RESULT\t2026-11-15 14:00:{:02d}.{:06d}\tpipeline_write\tk{}\t1.0\tOK\t4\n"
    .format(i, i, i) for i in range(12))


def test_derive_log_meta_from_names(tmp_path):
    cases = {
        "cfg.benchmark_c1_t1_8MB.log": dict(size="8MB", role=None, node=None),
        "cfg.benchmark_c1_t8_8MB.dual_n1_c0_p9301.reader.log":
            dict(size="8MB", role="reader", node="n1"),
        "cfg.benchmark_c1_t8_128KB.writer_n2_w0.log":
            dict(size="128KB", role="writer", node="n2"),
        "cfg.benchmark_c1_t1_64KB.glog": dict(size="64KB", role=None),
    }
    for name, want in cases.items():
        got = bm._derive_log_meta(str(tmp_path / name), str(tmp_path))
        for k, v in want.items():
            assert got[k] == v, (name, k, got)


def test_collect_local_stats_keeps_size_role_node(tmp_path):
    log = tmp_path / "cfg.benchmark_c1_t8_8MB.dual_n1_c0_p9301.writer.log"
    log.write_text(ROWS, encoding="utf-8")
    stats = bm._collect_local_stats(str(tmp_path))
    assert stats and stats[0].endswith(".stats.json")
    st = json.load(open(stats[0], encoding="utf-8"))
    assert st["meta"]["size"] == "8MB"
    assert st["meta"]["role"] == "writer"
    assert st["meta"]["node"] == "n1"
    assert st["meta"]["recomputed"] is True
    assert st["global"]["total"] == 12


def test_collect_local_traces_keeps_size_role(tmp_path):
    glog = tmp_path / "cfg.benchmark_c1_t8_8MB.dual_n1_c0_p9301.glog"
    glog.write_text(
        "I20261112 10:23:41.000000 1 t.cc:1] trace_id[1] breakdown[put] "
        "key[k] status[ok] size[1048576] replicas[2] "
        "start_time[2026-11-12 10:23:41.000000] prep_total_us[54] "
        "total_us[54]\n", encoding="utf-8")
    traces = bm._collect_local_traces(str(tmp_path))
    assert traces
    st = json.load(open(traces[0], encoding="utf-8"))
    assert st["meta"]["size"] == "8MB"
    assert st["meta"]["recomputed"] is True
