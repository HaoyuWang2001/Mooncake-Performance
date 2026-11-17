#!/usr/bin/env python3
"""pipeline CSV schema：writer_count/reader_count 已合并为 client_count。"""
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

import benchmark as bm

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def test_pipeline_header_uses_client_count():
    h = bm.PIPELINE_CSV_HEADER
    assert "client_count" in h
    assert "writer_count" not in h
    assert "reader_count" not in h
    # role 列区分写/读，client_count 紧跟 warmup_duration 之后
    assert h.index("client_count") == h.index("warmup_duration") + 1


def test_basic_header_has_no_client_count():
    assert "client_count" not in bm.BASIC_CSV_HEADER


def test_all_result_schemas_use_requested_latency_summary_order():
    for header in (bm.BASIC_CSV_HEADER, bm.PIPELINE_CSV_HEADER,
                   bm.KVTEST_CSV_HEADER):
        start = header.index("total")
        assert header[start:start + 4] == [
            "total", "fail", "fail_rate", "qps"]
        assert header[start + 4:start + 4 + len(bm.LATENCY_SUMMARY_COLUMNS)] \
            == bm.LATENCY_SUMMARY_COLUMNS


def test_summary_qps_and_latency_thresholds_use_success_samples(tmp_path):
    path = tmp_path / "result.log"
    lines = [
        "META\t" + json.dumps({"mode": "benchmark", "start_ts": 100.0}),
        "RESULT\t10:00:00.000000\tput\tk0\t1.0\tOK",
        "RESULT\t10:00:01.000000\tput\tk1\t2.0\tOK",
        "RESULT\t10:00:02.000000\tput\tk2\t21.0\tOK",
        "RESULT\t10:00:03.000000\tput\tk3\t999.0\tERR",
        "SUMMARY\t" + json.dumps({"wall_s": 10.0, "bytes": 3}),
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    stats = bm.parse_benchmark_log(str(path))
    assert stats["total"] == 4 and stats["ok"] == 3 and stats["fail"] == 1
    assert stats["qps"] == 0.3                 # successful requests / 10s
    assert stats["fail_rate"] == 25.0
    assert stats["min_ms"] == 1.0 and stats["max_ms"] == 21.0
    assert stats["p50_ms"] == 2.0 and stats["p95_ms"] == 21.0
    assert stats[">2000us"] == 1              # strict >; 2ms is excluded
    assert stats[">20000us"] == 1


def test_client_count_uses_runtime_totals():
    """client_count 取运行期记录的真实集群总数（不是 node_count=1 的 plan）。"""
    src = open(os.path.join(ROOT, "benchmark.py"), encoding="utf-8").read()
    assert 'cfg["_writer_count"] = len(plan["writers"])' in src
    assert 'cfg["_reader_count"] = len(plan["readers"])' in src
    assert 'role_key = "_writer_count" if role == "writer" else "_reader_count"' in src
    assert 'client_count = int(cfg[role_key])' in src


def test_client_count_numeric_in_pipeline_rows():
    """运行期有真实计数时 client_count 必须是整数，不能是 'N*1' 字符串。"""
    src = open(os.path.join(ROOT, "benchmark.py"), encoding="utf-8").read()
    assert '"client_count": client_count' in src
    assert "client_count = int(cfg[role_key])" in src
