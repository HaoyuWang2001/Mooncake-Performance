#!/usr/bin/env python3
"""tracestats（C++ SDK trace 主行）单测——fixtures 严格取自 SDK 日志格式样例。

P1 范围：breakdown[<op>] 主行（slice 明细行仅计数）；方向 read/write；
阶段树加和/残差；-1 哨兵；失败早退字段为 0；OBJECT_ALREADY_EXISTS 幂等。
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import logstats as ls    # noqa: E402
import tracestats as ts  # noqa: E402

GLOG = "I20261112 10:23:41.000000 12345 transfer_engine.cc:88] "

READ_OK = (GLOG + "trace_id[18446744073] breakdown[get_buffer] key[bench/obj_000123] "
           "status[ok] size[262144] type[memory_remote] replica_count[2] "
           "remote_endpoint[10.20.1.9:50051] local_endpoint[10.20.1.7:50051] "
           "start_time[2026-11-12 10:23:41.882310] query_total_us[50] "
           "query_route_us[12] query_rpc_us[38] select_us[4] buf_alloc_us[6] "
           "memread_total_us[206] read_submit_us[16] read_queue_us[12] "
           "read_urma_us[166] read_teardown_us[12] slice_count[4] "
           "read_urma_min_us[140] read_urma_avg_us[154] memcpy_us[0] total_us[266]")

READ_MISS = (GLOG + "trace_id[18446744073] breakdown[get_buffer] key[bench/missing_9] "
             "status[OBJECT_NOT_FOUND] size[0] type[unknown] replica_count[0] "
             "remote_endpoint[-] local_endpoint[10.20.1.7:50051] "
             "start_time[2026-11-12 10:23:42.015004] query_total_us[37] "
             "query_route_us[0] query_rpc_us[37] select_us[0] buf_alloc_us[0] "
             "memread_total_us[0] read_submit_us[0] read_queue_us[0] read_urma_us[0] "
             "read_teardown_us[0] slice_count[0] read_urma_min_us[0] "
             "read_urma_avg_us[0] memcpy_us[0] total_us[37]")

WRITE_OK = (GLOG + "trace_id[18446744073] breakdown[put] key[bench/obj_000123] "
            "status[ok] size[1048576] replicas[2] start_time[2026-11-12 10:23:43.110220] "
            "prep_total_us[54] prep_buf_alloc_us[9] prep_memcpy_us[42] prep_split_us[3] "
            "checksum_us[31] prepare_us[4] query_us[0] put_start_us[58] "
            "memwrite_total_us[188] memwrite_submit_us[11] memwrite_queue_us[14] "
            "memwrite_urma_us[152] memwrite_teardown_us[11] slice_count[2] "
            "memwrite_urma_min_us[148] memwrite_urma_avg_us[150] put_end_us[66] "
            "finalize_us[0] cache_invalidate_us[0] revoke_us[0] total_us[401]")

WRITE_FAIL = (GLOG + "trace_id[18446744073] breakdown[put] key[bench/obj_000124] "
              "status[TRANSFER_FAIL] size[1048576] replicas[2] "
              "start_time[2026-11-12 10:23:44.002981] prep_total_us[55] "
              "prep_buf_alloc_us[9] prep_memcpy_us[43] prep_split_us[3] "
              "checksum_us[30] prepare_us[5] query_us[0] put_start_us[63] "
              "memwrite_total_us[40012] memwrite_submit_us[9] "
              "memwrite_queue_us[40000] memwrite_urma_us[0] memwrite_teardown_us[0] "
              "slice_count[1] memwrite_urma_min_us[-1] memwrite_urma_avg_us[0] "
              "put_end_us[0] finalize_us[0] cache_invalidate_us[0] revoke_us[0] "
              "total_us[40170]")

UPSERT_OK = (GLOG + "trace_id[18446744073] breakdown[upsert] key[bench/obj_000123] "
             "status[ok] size[262144] replicas[2] start_time[2026-11-12 10:23:45.223417] "
             "prep_total_us[52] prep_buf_alloc_us[8] prep_memcpy_us[41] prep_split_us[3] "
             "checksum_us[29] prepare_us[4] query_us[57] put_start_us[0] "
             "memwrite_total_us[191] memwrite_submit_us[12] memwrite_queue_us[13] "
             "memwrite_urma_us[154] memwrite_teardown_us[12] slice_count[1] "
             "memwrite_urma_min_us[154] memwrite_urma_avg_us[154] put_end_us[0] "
             "finalize_us[63] cache_invalidate_us[2] revoke_us[0] total_us[398]")

SLICE_LINES = (
    GLOG + "trace_id[18446744073] breakdown[get_buffer.slice] key[bench/obj_000123] "
    "slice_idx[0/4] offset[0] bytes[65536] src[10.20.1.7:50051] "
    "dst[10.20.1.9:50051] slice_queue_us[12] slice_urma_us[140] status[ok]\n"
    + GLOG + "trace_id[18446744073] breakdown[put.slice] key[bench/obj_000123] "
    "slice_idx[0/2] offset[0] bytes[524288] src[10.20.1.7:50051] "
    "dst[10.20.1.9:50051] slice_queue_us[14] slice_urma_us[148] status[ok]\n")

ALREADY_EXISTS = (GLOG + "trace_id[18446744073] breakdown[put] key[bench/dup] "
                  "status[ok] size[1048576] replicas[0] "
                  "start_time[2026-11-12 10:23:46.000000] prep_total_us[10] "
                  "prep_buf_alloc_us[10] prep_memcpy_us[0] prep_split_us[0] "
                  "checksum_us[5] prepare_us[1] query_us[0] put_start_us[20] "
                  "memwrite_total_us[0] memwrite_submit_us[0] memwrite_queue_us[0] "
                  "memwrite_urma_us[0] memwrite_teardown_us[0] slice_count[0] "
                  "memwrite_urma_min_us[-1] memwrite_urma_avg_us[0] put_end_us[0] "
                  "finalize_us[0] cache_invalidate_us[0] revoke_us[0] total_us[36]")


def _write(tmp_path, name, lines):
    p = tmp_path / name
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(p)


def _field_percentile(fd, q):
    return ls.hist_percentile({int(k): v for k, v in fd["hist"].items()}, q)


def test_parse_pairs_with_spaces_and_glog_prefix():
    p = ts.parse_line(READ_OK)
    assert p["breakdown"] == "get_buffer"
    assert p["start_time"] == "2026-11-12 10:23:41.882310"
    assert p["total_us"] == "266" and p["type"] == "memory_remote"
    assert ts.parse_line("plain log line without trace") is None


def test_read_tree_and_direction(tmp_path):
    st = ts.compute_trace_stats(_write(tmp_path, "a.glog", [READ_OK, READ_MISS]))
    m = st["meta"]
    assert m["trace_lines"] == 2 and m["slice_lines"] == 0
    ops = st["groups"]["read"]["ops"]
    op = ops["get_buffer"]
    assert op["count"] == 2 and op["ok"] == 1 and op["fail"] == 1
    assert op["status"] == {"ok": 1, "OBJECT_NOT_FOUND": 1}
    # total_us statistics exclude nothing here (miss total 37 is a valid value)
    assert op["fields"]["total_us"]["n"] == 2
    assert op["fields"]["total_us"]["max"] == 266
    # tree sums for the OK sample
    f = op["fields"]
    assert (f["query_route_us"]["max"] + f["query_rpc_us"]["max"]) == \
        f["query_total_us"]["max"]
    assert (f["read_submit_us"]["max"] + f["read_queue_us"]["max"] +
            f["read_urma_us"]["max"] + f["read_teardown_us"]["max"]) == \
        f["memread_total_us"]["max"]
    # residual: both samples total == stage sum -> ~0
    assert op["residual"]["n"] == 2
    assert abs(op["residual"]["max"]) <= 1
    # size distribution
    assert op["size_values"] == {"262144": 1, "0": 1}
    assert op["replicas"] == {"2": 1, "0": 1}
    assert op["slice_count"] == {"4": 1, "0": 1}


def test_write_ops_and_failure_early_exit(tmp_path):
    st = ts.compute_trace_stats(
        _write(tmp_path, "b.glog", [WRITE_OK, WRITE_FAIL, UPSERT_OK,
                                    ALREADY_EXISTS]))
    ops = st["groups"]["write"]["ops"]
    assert set(ops) == {"put", "upsert"}
    put = ops["put"]
    assert put["count"] == 3 and put["ok"] == 2 and put["fail"] == 1
    assert put["status"]["TRANSFER_FAIL"] == 1
    # -1 sentinel: counted as neg, excluded from min/max/sum
    mm = put["fields"]["memwrite_urma_min_us"]
    # TRANSFER_FAIL and ALREADY_EXISTS both report -1 (TIMESTAMP unavailable)
    assert mm["neg"] == 2
    assert mm["min"] == 148 and mm["max"] == 148
    # failure early-exit fields are 0, not negative
    assert put["fields"]["put_end_us"]["neg"] == 0
    # upsert: query_us is the UpsertStart RPC, put_start always 0
    up = ops["upsert"]
    assert up["fields"]["query_us"]["max"] == 57
    assert up["fields"]["put_start_us"]["max"] == 0
    assert up["fields"]["finalize_us"]["max"] == 63
    # tree sums (ok put sample)
    f = up["fields"]
    assert (f["prep_buf_alloc_us"]["max"] + f["prep_memcpy_us"]["max"] +
            f["prep_split_us"]["max"]) == f["prep_total_us"]["max"]
    # residual handles the -1/zero rows too (values clamped at >= 0 for bins)
    assert put["residual"]["n"] == 3
    # OBJECT_ALREADY_EXISTS idempotent put: ok but replicas/memwrite are 0
    assert "0" in put["replicas"]
    assert put["fields"]["memwrite_total_us"]["min"] == 0


def test_slice_lines_counted_not_aggregated(tmp_path):
    st = ts.compute_trace_stats(
        _write(tmp_path, "c.glog", [READ_OK, SLICE_LINES]))
    m = st["meta"]
    assert m["slice_lines"] == 2 and m["trace_lines"] == 1
    # no slice op leaked into the aggregates
    assert "get_buffer.slice" not in st["groups"].get("read", {}).get("ops", {})
    assert "put.slice" not in st["groups"].get("write", {}).get("ops", {})


def test_time_buckets_and_topn(tmp_path):
    lines = [READ_OK, READ_MISS]
    st = ts.compute_trace_stats(_write(tmp_path, "d.glog", lines))
    op = st["groups"]["read"]["ops"]["get_buffer"]
    # both samples are within 10:23:41/42 -> event-time seconds buckets
    assert len(op["seconds"]) >= 1
    rows = op["seconds"]
    assert sum(r[1] for r in rows) == 2
    assert op["top"][0]["total_us"] >= op["top"][-1]["total_us"]
    assert op["top"][0]["trace_id"] == "18446744073"
    assert "stages" in op["top"][0]


def test_cli_writes_trace_json(tmp_path):
    import subprocess
    p = _write(tmp_path, "e.glog", [READ_OK, WRITE_OK])
    out = str(tmp_path / "out.json")
    r = subprocess.run(
        [sys.executable, os.path.join(ROOT, "scripts", "tracestats.py"),
         "--log", p, "--out", out, "--size", "8MB", "--sample-rate", "1.0"],
        capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    d = json.load(open(out, encoding="utf-8"))
    assert d["meta"]["size"] == "8MB" and d["meta"]["sample_rate"] == 1.0
    assert set(d["groups"]) == {"read", "write"}
    assert "[tracestats]" in r.stdout
