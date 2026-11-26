#!/usr/bin/env python3
"""tracestats（C++ SDK trace 主行）单测——fixtures 严格取自 SDK 日志格式样例。

覆盖当前打点体系（对齐 Mooncake 源码 docs/yh/log-reference.md §14，统一 breakdown[op]，
TE/TENT 双引擎）：
- 读：breakdown[get_buffer]（新字段 query_total_us/buf_alloc_us 归一为 query_us/alloc_us）、
  get_into、batch_get、batch_get_into（query 三段下钻 / prep_us L1；旧 get_breakdown 仍兼容）
- 写：put 家族 breakdown[op]：engine[classic] memwrite_* / engine[tent] 四相位；
  batch_put / batch_put_into；TENT+TCP 诚实零值
- remove / batchRemove 薄行归「其它」组（remove_rpc_us 仍入字段统计）
- slice / slice_stats 明细行仅计数；方向 read/write；阶段树加和/残差；
  -1 哨兵；失败早退字段为 0；engine 分布
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import logstats as ls    # noqa: E402
import tracestats as ts  # noqa: E402

GLOG = "I20261112 10:23:41.000000 12345 transfer_engine.cc:88] "

READ_OK = (GLOG + "trace_id[18446744073] get_breakdown key[bench/obj_000123] "
           "start_time[2026-11-25 10:23:41.882310] query_us[50] select_us[18] "
           "local_endpoints_us[12] select_replica_us[6] alloc_us[5] read_us[196] "
           "total_us[269] read_a_us[58] read_b_us[12] "
           "read_transfer_window_us[126] read_ab_valid[1] "
           "read_transfer_count[1] type[memory_remote] replica_count[2] "
           "selected_type[memory_remote] local_endpoint[10.20.1.7:50051] "
           "remote_endpoint[10.20.1.9:50051] status[ok]")

READ_MISS = (GLOG + "trace_id[18446744073] get_breakdown key[bench/missing_9] "
             "start_time[2026-11-25 10:23:42.015004] query_us[37] select_us[0] "
             "local_endpoints_us[0] select_replica_us[0] alloc_us[0] read_us[0] "
             "total_us[37] read_a_us[-1] read_b_us[0] "
             "read_transfer_window_us[-1] read_ab_valid[0] "
             "read_transfer_count[0] type[unknown] replica_count[0] "
             "selected_type[unknown] local_endpoint[10.20.1.7:50051] "
             "remote_endpoint[-] status[OBJECT_NOT_FOUND]")

# get_into：read 拆 offload_rpc/transfer_data/release_buffer/read_overhead
GET_INTO_OK = (GLOG + "trace_id[18446744073] breakdown[get_into] "
               "key[bench/obj_000123] start_time[2026-11-25 10:23:44.000000] "
               "query_us[40] select_us[10] local_endpoints_us[6] "
               "select_replica_us[4] read_us[120] offload_rpc_us[20] "
               "transfer_data_us[80] release_buffer_us[10] "
               "read_overhead_us[10] total_us[170] type[memory_remote] "
               "mode[full] replica_count[2] selected_type[memory_remote] "
               "local_endpoint[10.20.1.7:50051] "
               "remote_endpoint[10.20.1.9:50051] status[ok]")

WRITE_OK = (GLOG + "trace_id[18446744073] breakdown[put] key[bench/obj_000123] "
            "status[ok] size[1048576] replicas[2] "
            "start_time[2026-11-25 10:23:43.110220] prep_total_us[54] "
            "prep_buf_alloc_us[9] prep_memcpy_us[42] prep_split_us[3] "
            "checksum_us[31] prepare_us[4] query_us[0] put_start_us[58] "
            "engine[classic] memwrite_total_us[188] memwrite_submit_us[11] "
            "memwrite_queue_us[14] memwrite_urma_us[152] "
            "memwrite_teardown_us[11] slice_count[2] memwrite_urma_min_us[148] "
            "memwrite_urma_avg_us[150] put_end_us[66] finalize_us[0] "
            "cache_invalidate_us[0] revoke_us[0] total_us[401]")

WRITE_FAIL = (GLOG + "trace_id[18446744073] breakdown[put] key[bench/obj_000124] "
              "status[TRANSFER_FAIL] size[1048576] replicas[2] "
              "start_time[2026-11-25 10:23:44.002981] prep_total_us[55] "
              "prep_buf_alloc_us[9] prep_memcpy_us[43] prep_split_us[3] "
              "checksum_us[30] prepare_us[5] query_us[0] put_start_us[63] "
              "engine[classic] memwrite_total_us[40012] memwrite_submit_us[9] "
              "memwrite_queue_us[40000] memwrite_urma_us[0] "
              "memwrite_teardown_us[0] slice_count[1] memwrite_urma_min_us[-1] "
              "memwrite_urma_avg_us[0] put_end_us[0] finalize_us[0] "
              "cache_invalidate_us[0] revoke_us[0] total_us[40170]")

UPSERT_OK = (GLOG + "trace_id[18446744073] breakdown[upsert] key[bench/obj_000123] "
             "status[ok] size[262144] replicas[2] "
             "start_time[2026-11-25 10:23:45.223417] prep_total_us[52] "
             "prep_buf_alloc_us[8] prep_memcpy_us[41] prep_split_us[3] "
             "checksum_us[29] prepare_us[4] query_us[57] put_start_us[0] "
             "engine[classic] memwrite_total_us[191] memwrite_submit_us[12] "
             "memwrite_queue_us[13] memwrite_urma_us[154] "
             "memwrite_teardown_us[12] slice_count[1] "
             "memwrite_urma_min_us[154] memwrite_urma_avg_us[154] "
             "put_end_us[0] finalize_us[63] cache_invalidate_us[2] revoke_us[0] "
             "total_us[398]")

# TENT 引擎写主行（Mooncake 文档示例；四相位加和 = 458，残差 0）
TENT_UPSERT = (GLOG + "trace_id[18446744073] breakdown[upsert] "
               "key[mooncake_kvtest_0_0] status[ok] size[131072] replicas[2] "
               "start_time[2026-11-25 10:23:41.882310] prep_total_us[52] "
               "prep_buf_alloc_us[8] prep_memcpy_us[41] prep_split_us[3] "
               "checksum_us[29] prepare_us[4] query_us[57] put_start_us[0] "
               "engine[tent] admit_us[10] queue_prep_us[90] wire_span_us[150] "
               "propagate_us[4] slice_count[2] bytes[131072] put_end_us[0] "
               "finalize_us[60] cache_invalidate_us[2] revoke_us[0] "
               "total_us[458]")

SLICE_LINES = (
    GLOG + "trace_id[18446744073] breakdown[upsert.slice] key[bench/obj] "
    "engine[tent] slice_idx[1/2] offset[65536] bytes[65536] "
    "src[10.20.1.7:50051] dst[seg2/dev0] slice_queue_us[14] "
    "prep_route_us[12] prep_path_us[5] prep_seg_us[9] post_call_us[4] "
    "wire_us[160] attempt[1] defer[0] status[ok]\n"
    + GLOG + "trace_id[18446744073] breakdown[upsert.slice_stats] "
    "key[bench/obj] engine[tent] status[ok] slices[2] bytes[131072] "
    "slice_queue_us_max[14] prep_route_us_sum[21] prep_route_us_max[12] "
    "prep_path_us_sum[9] prep_path_us_max[5] prep_seg_us_sum[16] "
    "prep_seg_us_max[9] post_call_us_sum[7] post_call_us_max[4] "
    "wire_us_p50[148] wire_us_p95[158] wire_us_max[160] wire_us_sum[308] "
    "defer_total[0] attempt_total[2] slow_count[0] slowest_idx[1] "
    "slowest_offset[65536] slowest_wire_us[160]\n")

ALREADY_EXISTS = (GLOG + "trace_id[18446744073] breakdown[put] key[bench/dup] "
                  "status[ok] size[1048576] replicas[0] "
                  "start_time[2026-11-25 10:23:46.000000] prep_total_us[10] "
                  "prep_buf_alloc_us[10] prep_memcpy_us[0] prep_split_us[0] "
                  "checksum_us[5] prepare_us[1] query_us[0] put_start_us[20] "
                  "engine[classic] memwrite_total_us[0] memwrite_submit_us[0] "
                  "memwrite_queue_us[0] memwrite_urma_us[0] "
                  "memwrite_teardown_us[0] slice_count[0] "
                  "memwrite_urma_min_us[-1] memwrite_urma_avg_us[0] "
                  "put_end_us[0] finalize_us[0] cache_invalidate_us[0] "
                  "revoke_us[0] total_us[36]")


def _write(tmp_path, name, lines):
    p = tmp_path / name
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return str(p)


def _field_percentile(fd, q):
    return ls.hist_percentile({int(k): v for k, v in fd["hist"].items()}, q)


def test_parse_pairs_with_spaces_and_glog_prefix():
    p = ts.parse_line(READ_OK)
    # get_breakdown 行归一化为 breakdown="get_buffer"（下游零改动）
    assert p["breakdown"] == "get_buffer"
    assert p["start_time"] == "2026-11-25 10:23:41.882310"
    assert p["total_us"] == "269" and p["type"] == "memory_remote"
    assert ts.parse_line("plain log line without trace") is None


def test_read_ok_status_counts_as_success(tmp_path):
    """读路径成功状态历史写作 status[read_ok]：必须识别为成功并归一到 ok，
    否则全部读请求会被误判为失败（错误与异常的状态分布按写/读各一条）。"""
    line = READ_OK.replace("status[ok]", "status[read_ok]")
    st = ts.compute_trace_stats(_write(tmp_path, "ro.glog", [line]))
    op = st["groups"]["read"]["ops"]["get_buffer"]
    assert op["ok"] == 1 and op["fail"] == 0
    assert op["status"] == {"ok": 1}


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
    assert op["fields"]["total_us"]["max"] == 269
    # tree sums for the OK sample（get_breakdown 新树）
    f = op["fields"]
    assert (f["query_us"]["max"] + f["select_us"]["max"] +
            f["alloc_us"]["max"] + f["read_us"]["max"]) == \
        f["total_us"]["max"]
    assert (f["local_endpoints_us"]["max"] +
            f["select_replica_us"]["max"]) == f["select_us"]["max"]
    assert (f["read_a_us"]["max"] + f["read_transfer_window_us"]["max"] +
            f["read_b_us"]["max"]) == f["read_us"]["max"]
    # residual: both samples total == stage sum -> ~0
    assert op["residual"]["n"] == 2
    assert abs(op["residual"]["max"]) <= 1
    # get_breakdown 无 size/slice_count 字段；replica_count 归一为 replicas
    assert op["size_values"] == {}
    assert op["replicas"] == {"2": 1, "0": 1}
    assert op["slice_count"] == {}


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
    # 引擎分布：这些 fixture 全是 classic
    assert put["engine"] == {"classic": 3}
    assert up["engine"] == {"classic": 1}


def test_slice_lines_counted_not_aggregated(tmp_path):
    st = ts.compute_trace_stats(
        _write(tmp_path, "c.glog", [READ_OK, SLICE_LINES]))
    m = st["meta"]
    assert m["slice_lines"] == 2 and m["trace_lines"] == 1
    # slice_stats 行同样仅计数（不产生 "upsert.slice_stats" 假 op）
    st2 = ts.compute_trace_stats(
        _write(tmp_path, "c2.glog", [TENT_UPSERT, SLICE_LINES]))
    m2 = st2["meta"]
    assert m2["slice_lines"] == 2 and m2["trace_lines"] == 1
    ops = st2["groups"]["write"]["ops"]
    assert set(ops) == {"upsert"}
    assert ops["upsert"]["count"] == 1


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
    # 展开明细需要 L1+L2：top 条目必须保留二级子阶段
    st0 = op["top"][0]["stages"]
    for k in ("query_us", "select_us", "alloc_us", "read_us"):
        assert k in st0
    for k in ("local_endpoints_us", "select_replica_us", "read_a_us",
              "read_transfer_window_us", "read_b_us"):
        assert k in st0, (k, st0)


# ---- 统一 breakdown[op]（log-reference §14.8/§14.9）：新增接口与字段 ----

READ_UNIFIED = (
    GLOG + "trace_id[18446744073] breakdown[get_buffer] "
    "key[bench/obj_000123] status[ok] size[1048576] type[memory_remote] "
    "replica_count[2] remote_endpoint[10.20.1.9:50051] "
    "local_endpoint[10.20.1.7:50051] start_time[2026-11-27 10:23:41.882310] "
    "query_total_us[57] query_route_us[5] query_rpc_us[50] "
    "query_retry_us[0] query_retry_count[0] select_us[18] "
    "local_endpoints_us[12] select_replica_us[6] buf_alloc_us[5] "
    "engine[classic] memread_total_us[189] read_submit_us[8] "
    "read_queue_us[14] read_urma_us[160] read_teardown_us[7] "
    "slice_count[2] read_urma_min_us[148] read_urma_avg_us[150] "
    "read_us[189] read_a_us[58] read_b_us[5] "
    "read_transfer_window_us[126] read_ab_valid[1] "
    "read_transfer_count[1] memcpy_us[0] total_us[269]")

BATCH_GET = (
    GLOG + "trace_id[18446744073] breakdown[batch_get] num_keys[8] "
    "start_time[2026-11-27 10:23:42.000000] query_us[30] "
    "query_route_us[4] query_rpc_us[24] query_retry_us[0] prep_us[50] "
    "read_us[120] total_us[200] batch_get_ops[8] ssd_offload_ops[0] "
    "success[8] replicas[2] status[ok]")

BATCH_GET_INTO = (
    GLOG + "trace_id[18446744073] breakdown[batch_get_into] num_keys[6] "
    "start_time[2026-11-27 10:23:43.000000] query_us[30] "
    "query_route_us[4] query_rpc_us[24] query_retry_us[0] prep_us[40] "
    "read_us[130] total_us[200] direct_ops[0] mem_ops[6] nof_ops[0] "
    "disk_ops[0] ssd_offload_ops[0] success[6] replicas[2] status[ok]")

BATCH_PUT = (
    GLOG + "trace_id[18446744073] breakdown[batch_put] num_keys[4] "
    "start_time[2026-11-27 10:23:44.000000] total_us[500] status[ok]")

REMOVE_THIN = (
    GLOG + "trace_id[18446744073] breakdown[remove] key[bench/obj] "
    "start_time[2026-11-27 10:23:45.000000] remove_rpc_us[30] "
    "total_us[30] status[ok]")

BATCH_REMOVE_THIN = (
    GLOG + "trace_id[18446744073] breakdown[batchRemove] num_keys[3] "
    "start_time[2026-11-27 10:23:46.000000] remove_rpc_us[40] "
    "total_us[40] status[ok]")

# TENT + TCP：引擎身份仍是 tent，但 rail 不暴露任务级计时 → 四相位诚实零值
TENT_TCP_ZERO = (
    GLOG + "trace_id[18446744073] breakdown[upsert] key[bench/obj] "
    "status[ok] size[131072] replicas[2] "
    "start_time[2026-11-27 10:23:47.000000] prep_total_us[52] "
    "prep_buf_alloc_us[8] prep_memcpy_us[41] prep_split_us[3] "
    "checksum_us[29] prepare_us[4] query_us[57] put_start_us[0] "
    "engine[tent] admit_us[0] queue_prep_us[0] wire_span_us[0] "
    "propagate_us[0] slice_count[0] bytes[0] put_end_us[0] "
    "finalize_us[60] cache_invalidate_us[2] revoke_us[0] total_us[458]")


def test_unified_get_buffer_field_alias_and_tree(tmp_path):
    """breakdown[get_buffer] 新字段：query_total_us/buf_alloc_us 归一为
    query_us/alloc_us，query 三段下钻与 A/B 分段保留，残差为 0。"""
    st = ts.compute_trace_stats(_write(tmp_path, "u.glog", [READ_UNIFIED]))
    op = st["groups"]["read"]["ops"]["get_buffer"]
    f = op["fields"]
    assert op["count"] == 1 and op["ok"] == 1
    assert f["query_us"]["max"] == 57          # query_total_us → query_us
    assert f["alloc_us"]["max"] == 5           # buf_alloc_us → alloc_us
    assert f["select_us"]["max"] == 18 and f["read_us"]["max"] == 189
    assert f["query_route_us"]["max"] == 5
    assert f["query_rpc_us"]["max"] == 50
    assert f["query_retry_us"]["max"] == 0
    assert f["read_a_us"]["max"] == 58 and f["read_transfer_window_us"]["max"] == 126
    assert "memread_total_us" in f             # classic 传输段仍入字段统计
    assert op["residual"]["n"] == 1 and op["residual"]["max"] == 0
    assert op["engine"] == {"classic": 1}


def test_get_into_query_drill_fields(tmp_path):
    """get_into 的 query 三段下钻（L2）登记且不破坏残差。"""
    line = (GET_INTO_OK.replace("query_us[40]",
                                "query_us[40] query_route_us[5] "
                                "query_rpc_us[30] query_retry_us[0]"))
    st = ts.compute_trace_stats(_write(tmp_path, "into3.glog", [line]))
    op = st["groups"]["read"]["ops"]["get_into"]
    assert op["fields"]["query_route_us"]["max"] == 5
    assert op["fields"]["query_rpc_us"]["max"] == 30
    assert op["residual"]["max"] == 0


def test_batch_read_interfaces_prep_and_query_drill(tmp_path):
    st = ts.compute_trace_stats(
        _write(tmp_path, "batch.glog", [BATCH_GET, BATCH_GET_INTO]))
    read = st["groups"]["read"]["ops"]
    for name, prep in (("batch_get", 50), ("batch_get_into", 40)):
        op = read[name]
        assert op["count"] == 1
        assert op["fields"]["prep_us"]["max"] == prep
        assert op["fields"]["query_route_us"]["max"] == 4
        assert op["residual"]["n"] == 1 and op["residual"]["max"] == 0


def test_batch_put_write_group_and_remove_other(tmp_path):
    st = ts.compute_trace_stats(
        _write(tmp_path, "mix2.glog",
               [BATCH_PUT, REMOVE_THIN, BATCH_REMOVE_THIN]))
    assert st["groups"]["write"]["ops"]["batch_put"]["count"] == 1
    other = st["groups"]["other"]["ops"]
    assert other["remove"]["fields"]["remove_rpc_us"]["max"] == 30
    assert other["batchRemove"]["fields"]["remove_rpc_us"]["max"] == 40
    assert "residual" not in other["remove"]   # 其它组不做阶段残差


def test_tent_tcp_honest_zero_phases(tmp_path):
    """TENT + TCP：engine 仍为 tent，四相位 0（诚实零值）；残差吸收真实传输。"""
    st = ts.compute_trace_stats(_write(tmp_path, "zt.glog", [TENT_TCP_ZERO]))
    op = st["groups"]["write"]["ops"]["upsert"]
    assert op["engine"] == {"tent": 1}
    assert op["fields"]["admit_us"]["max"] == 0
    assert "memwrite_total_us" not in op["fields"]
    # 52+29+4+57+60+2 = 204；458 − 204 = 254
    assert op["residual"]["n"] == 1 and op["residual"]["max"] == 254


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


def test_minute_buckets_are_epoch_seconds(tmp_path):
    """trace 的 minutes 桶 t 也必须是 epoch 秒（与 logstats 一致）。"""
    read2 = READ_OK.replace("10:23:41.882310", "10:24:01.500000")
    p = _write(tmp_path, "tr.glog", [READ_OK, read2])
    st = ts.compute_trace_stats(p)
    assert st["meta"]["bucket_unit"] == "s"
    op = st["groups"]["read"]["ops"]["get_buffer"]
    assert all(r[0] % 60 == 0 for r in op["minutes"])
    sec_minutes = {t - t % 60 for t in (r[0] for r in op["seconds"])}
    assert {r[0] for r in op["minutes"]} == sec_minutes
    assert len(op["minutes"]) == 2


def test_top_entry_keeps_write_l2_fields(tmp_path):
    st = ts.compute_trace_stats(_write(tmp_path, "w2.glog", [WRITE_OK]))
    top = st["groups"]["write"]["ops"]["put"]["top"][0]["stages"]
    for k in ("prep_buf_alloc_us", "prep_memcpy_us", "prep_split_us",
              "memwrite_urma_us", "memwrite_queue_us"):
        assert k in top, (k, top)


def test_negative_total_not_in_top_and_bucket_counts_neg(tmp_path):
    """total_us=-1 的样本不应进 Top-50，桶里只计 neg 不计时延。"""
    bad = WRITE_OK.replace("total_us[401]", "total_us[-1]")
    p = _write(tmp_path, "neg.glog", [WRITE_OK, bad])
    st = ts.compute_trace_stats(p)
    op = st["groups"]["write"]["ops"]["put"]
    assert [e["total_us"] for e in op["top"]] == [401]
    assert len(op["seconds"]) == 1
    assert op["seconds"][0][1] == 2      # 两个样本都计入 n
    assert op["seconds"][0][9] == 1      # 其中 1 个是 neg
    assert op["seconds"][0][4] == 401.0  # avg 只按有效样本


def test_direction_falls_back_to_op_name(tmp_path):
    """早退/字段缺失的 put 行（无 memwrite_total_us）不能落进 other。"""
    trunc = (GLOG + "trace_id[9] breakdown[put] key[bench/x] status[TRANSFER_FAIL] "
             "size[1048576] replicas[2] start_time[2026-11-12 10:23:47.000000] "
             "prep_total_us[54] put_start_us[58] put_end_us[66] total_us[124]")
    p = _write(tmp_path, "dir.glog", [trunc])
    st = ts.compute_trace_stats(p)
    assert "put" in st["groups"]["write"]["ops"]
    op = st["groups"]["write"]["ops"]["put"]
    assert op["count"] == 1 and op["fail"] == 1
    assert op["residual"]["n"] == 1          # 第二遍扫描同样归类为 write
    assert "put" not in (st["groups"].get("other") or {}).get("ops", {})


def test_truncated_line_counts_parse_errors(tmp_path):
    """glog 单行上限会截断超长 trace 行：必须计 parse_errors 而不是静默半解析。"""
    cut = WRITE_OK[:WRITE_OK.index("total_us[401]") + len("total_us[40")]
    p = _write(tmp_path, "cut.glog", [WRITE_OK, cut])
    st = ts.compute_trace_stats(p)
    m = st["meta"]
    assert m["parse_errors"] == 1
    assert m["trace_lines"] == 1
    assert st["groups"]["write"]["ops"]["put"]["count"] == 1


def test_top_n_zero_does_not_crash(tmp_path):
    p = _write(tmp_path, "t0.glog", [WRITE_OK])
    st = ts.compute_trace_stats(p, top_n=0)
    assert len(st["groups"]["write"]["ops"]["put"]["top"]) == 1


# ---------------------------------------------------------------------------
# phase classification (MC_PHASE markers): setup / warmup / measured / teardown
# ---------------------------------------------------------------------------

GLOGP = "I20261115 09:27:33.000000 1 te.cc:1] "


def _trace(t, op, total=100):
    return (GLOGP + "trace_id[1] breakdown[{}] key[k] status[ok] size[1] "
            "start_time[{}] total_us[{}] query_us[{}]"
            .format(op, t, total, total))


def _mark(phase, action, t):
    return "MC_PHASE {} {} {:.6f}".format(phase, action, t)


def test_phase_markers_four_buckets(tmp_path):
    base = ts._start_epoch("2026-11-15 09:27:33.000000")
    lines = [
        _trace("2026-11-15 09:27:33.100000", "put", 10),       # setup
        _mark("warmup", "start", base + 0.3),
        _trace("2026-11-15 09:27:33.500000", "put", 20),        # warmup
        _trace("2026-11-15 09:27:34.000000", "put", 30),        # warmup
        _mark("measured", "start", base + 1.5),
        _trace("2026-11-15 09:27:35.000000", "put", 40),        # measured
        _mark("measured", "end", base + 3.0),
        _trace("2026-11-15 09:27:37.000000", "put", 50),        # teardown
    ]
    st = ts.compute_trace_stats(_write(tmp_path, "p.glog", lines))
    m = st["meta"]
    assert m["markers"] is True
    assert m["phase_counts"] == {"setup": 1, "warmup": 2, "measured": 1,
                                 "teardown": 1}
    # only the measured row is aggregated
    op = st["groups"]["write"]["ops"]["put"]
    assert op["count"] == 1
    assert op["fields"]["total_us"]["max"] == 40
    assert op["seconds"] and op["seconds"][0][1] == 1


def test_phase_no_markers_legacy_all_measured(tmp_path):
    lines = [
        _trace("2026-11-15 09:27:33.100000", "put", 10),
        _trace("2026-11-15 09:27:35.000000", "put", 40),
    ]
    st = ts.compute_trace_stats(_write(tmp_path, "l.glog", lines))
    m = st["meta"]
    assert m["markers"] is False
    assert m["phase_counts"]["measured"] == 2
    assert st["groups"]["write"]["ops"]["put"]["count"] == 2


def test_phase_warmup_disabled(tmp_path):
    base = ts._start_epoch("2026-11-15 09:27:33.000000")
    lines = [
        _trace("2026-11-15 09:27:32.000000", "put", 5),     # setup (before measured)
        _mark("measured", "start", base),
        _trace("2026-11-15 09:27:35.000000", "put", 40),
        _mark("measured", "end", base + 3.0),
    ]
    st = ts.compute_trace_stats(_write(tmp_path, "nw.glog", lines))
    assert st["meta"]["phase_counts"] == {"setup": 1, "warmup": 0,
                                          "measured": 1, "teardown": 0}
    assert st["groups"]["write"]["ops"]["put"]["count"] == 1


def test_phase_of_window_math():
    marks = {("warmup", "start"): 100.0, ("measured", "start"): 200.0,
             ("measured", "end"): 300.0}
    assert ts._phase_of(50, marks) == "setup"
    assert ts._phase_of(100, marks) == "warmup"
    assert ts._phase_of(199.9, marks) == "warmup"
    assert ts._phase_of(200, marks) == "measured"
    assert ts._phase_of(300, marks) == "measured"
    assert ts._phase_of(301, marks) == "teardown"
    assert ts._phase_of(1, {}) is None          # legacy -> measured by caller


# ---------------------------------------------------------------------------
# 段3：阶段时间桶（图2）+ σ（sumsq）
# ---------------------------------------------------------------------------

def test_field_minutes_l1_and_residual(tmp_path):
    p = tmp_path / "f.glog"
    p.write_text(READ_OK + "\n" + WRITE_OK + "\n", encoding="utf-8")
    st = ts.compute_trace_stats(str(p))
    read = st["groups"]["read"]["ops"]["get_buffer"]
    fm = read["fminutes"]
    # L1 顶层 + residual；L2 不逐时间采样（体积控制）
    assert set(fm) == {"query_us", "select_us", "alloc_us", "read_us",
                       "residual"}
    assert "read_a_us" not in fm
    rows = fm["query_us"]
    assert len(rows) == 1
    t, n, avg, mx = rows[0]
    assert n == 1 and avg == 50 and mx == 50
    # 阶段桶按 1 秒采集（旧版写死 1 分钟，短 run 只剩 2 个点、曲线退化成
    # 一条直线）；降档由 _fmb_rows 按桶数决定，短 run 保持 1 秒
    assert read["ftier"] == 1
    # residual = 269 - (50+18+5+196) = 0
    assert fm["residual"][0][2] == 0
    # σ 需要的平方和已累计
    assert read["fields"]["total_us"]["sumsq"] == 269 ** 2


def test_emit_step_collection_rule():
    """收集规则：最细的、桶数 ∈ [59, 8192] 的时间尺度；无达标回退 1s。"""
    assert ts._emit_step(30) == 1
    assert ts._emit_step(600) == 1
    assert ts._emit_step(3600) == 1
    assert ts._emit_step(10800) == 10      # 1s 超 8192
    assert ts._emit_step(90000) == 60      # 1s/10s 超上限
    assert ts._emit_step(86400 * 5) == 60  # 60s=7200 达标


def test_fmb_rows_aggregate_at_step():
    fb = {i: [3, 30.0, 40.0] for i in range(31)}
    rows = ts._fmb_rows(fb, 10)
    assert len(rows) == 4                  # 0-9,10-19,20-29,30
    assert rows[0] == [0, 30, 10.0, 40.0]   # sum=30 / n=30
    assert sum(r[1] for r in rows) == 93


def test_field_buckets_keep_seconds_when_short():
    """短 run（几十秒）保留秒级桶。"""
    fb = {1000 + i: [3, 30.0, 40.0] for i in range(31)}
    step = ts._emit_step(31)
    rows = ts._fmb_rows(fb, step)
    assert step == 1 and len(rows) == 31
    assert [r[0] for r in rows] == list(range(1000, 1031))


def test_field_buckets_coarsen_when_dense():
    """1s 桶数超过 8192 -> 10s 档，总量不丢。"""
    fb = {i: [1, 5.0, 9.0] for i in range(9000)}
    step = ts._emit_step(9000)
    rows = ts._fmb_rows(fb, step)
    assert step == 10 and len(rows) == 900
    assert sum(r[1] for r in rows) == 9000




# ---------------------------------------------------------------------------
# TE / TENT 双引擎适配（当前打点体系）
# ---------------------------------------------------------------------------

def test_tent_engine_tree_and_distribution(tmp_path):
    st = ts.compute_trace_stats(_write(tmp_path, "tent.glog", [TENT_UPSERT]))
    op = st["groups"]["write"]["ops"]["upsert"]
    assert op["count"] == 1 and op["ok"] == 1
    f = op["fields"]
    # TENT 四相位 + 公共段
    for k in ("admit_us", "queue_prep_us", "wire_span_us", "propagate_us"):
        assert f[k]["max"] >= 0, (k, f.get(k))
    assert f["finalize_us"]["max"] == 60
    # 四相位加和 ≈ transfer；残差 = total − Σ(公共段 + 四相位) = 0
    assert op["residual"]["n"] == 1 and op["residual"]["max"] == 0
    assert op["engine"] == {"tent": 1}
    # classic 专属字段不出现
    assert "memwrite_total_us" not in f


def test_engine_mixed_distribution(tmp_path):
    st = ts.compute_trace_stats(
        _write(tmp_path, "mix.glog", [UPSERT_OK, TENT_UPSERT]))
    op = st["groups"]["write"]["ops"]["upsert"]
    assert op["count"] == 2
    assert op["engine"] == {"classic": 1, "tent": 1}
    # 两行字段各自统计，互不污染
    assert "memwrite_total_us" in op["fields"]
    assert "admit_us" in op["fields"]


def test_get_into_offload_tree(tmp_path):
    st = ts.compute_trace_stats(_write(tmp_path, "into.glog", [GET_INTO_OK]))
    op = st["groups"]["read"]["ops"]["get_into"]
    assert op["count"] == 1
    f = op["fields"]
    assert (f["query_us"]["max"] + f["select_us"]["max"] +
            f["read_us"]["max"]) == f["total_us"]["max"]
    assert (f["offload_rpc_us"]["max"] + f["transfer_data_us"]["max"] +
            f["release_buffer_us"]["max"] +
            f["read_overhead_us"]["max"]) == f["read_us"]["max"]
    assert op["residual"]["n"] == 1 and op["residual"]["max"] == 0


def test_rotated_segments_merge_into_one_product(tmp_path):
    """glog 按 max_log_size 轮转：多段 INFO 文件必须合并成一份统计。

    MC_LOG_DIR 模式下 glog 全量写文件（名含时间戳+pid），长跑/高流量会切
    段。合并口径：计数/字段/直方图相加，ts 取 min/max，top 取并集重截，
    meta.source=分段目录名、bytes=各段之和。"""
    (tmp_path / "s1").mkdir()
    (tmp_path / "s2").mkdir()
    seg1 = _write(tmp_path / "s1", "prog.host.root.log.INFO.20261126-130000.45",
                  [READ_OK, WRITE_OK])
    seg2 = _write(tmp_path / "s2", "prog.host.root.log.INFO.20261126-140000.45",
                  [READ_OK, READ_OK])
    one = ts.compute_trace_stats(seg1)
    st = ts.compute_trace_stats([seg1, seg2])
    m = st["meta"]
    assert m["trace_lines"] == one["meta"]["trace_lines"] + 2
    assert m["lines"] == one["meta"]["lines"] + 2
    assert m["bytes"] == (os.path.getsize(seg1) + os.path.getsize(seg2))
    # 单文件时 source=文件名；多段时 source=分段目录名（clients 里一行）
    assert m["source"] == os.path.basename(os.path.dirname(seg1))
    read = st["groups"]["read"]["ops"]["get_buffer"]
    assert read["count"] == 3
    # ts 覆盖两段（min/max）
    assert m["ts_from"] is not None and m["ts_to"] >= m["ts_from"]
    # top 并集后仍按 total_us 降序、不超过 top_n
    assert len(read["top"]) <= ts.TOP_N
    totals = [e["total_us"] for e in read["top"]]
    assert totals == sorted(totals, reverse=True)


def test_cli_accepts_rotated_logs(tmp_path):
    import subprocess
    (tmp_path / "r1").mkdir()
    (tmp_path / "r2").mkdir()
    p1 = _write(tmp_path / "r1", "a.INFO.20261126-130000.45", [READ_OK])
    p2 = _write(tmp_path / "r2", "a.INFO.20261126-140000.45", [WRITE_OK])
    out = str(tmp_path / "rot.json")
    r = subprocess.run(
        [sys.executable, os.path.join(ROOT, "scripts", "tracestats.py"),
         "--log", p1, "--log", p2, "--out", out],
        capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
    d = json.load(open(out, encoding="utf-8"))
    assert set(d["groups"]) == {"read", "write"}
    assert "segments" in r.stdout
