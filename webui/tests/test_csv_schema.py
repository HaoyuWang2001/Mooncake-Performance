#!/usr/bin/env python3
"""pipeline CSV schema：writer_count/reader_count 已合并为 client_count。"""
import os
import sys

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
