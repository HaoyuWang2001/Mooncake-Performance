#!/usr/bin/env python3
"""trace_trees（configs/_trace_trees/<op>.json）单测。

树是阶段树的单一事实源：每个 op 的树必须与 Mooncake 源码真实发射格式
一致。测试用「真实发射格式的合成行 → 跑 tracestats → 断言归组/L1/L2/
残差与树一致」——树本身就是测试期望的生成器（改树 → 测试自动跟随）。
"""
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import trace_trees as tt      # noqa: E402
import tracestats as ts       # noqa: E402

GLOG = "I20261125 10:23:41.882310 1 te.cc:1] "

# ---- 真实发射格式的合成行（逐字段对照 real_client.cpp / breakdown_format.h）----

GET_BUFFER_CLASSIC = (
    GLOG + "trace_id[1] breakdown[get_buffer] key[bench/o] status[ok] "
    "size[1048576] type[memory_remote] replica_count[2] "
    "remote_endpoint[10.20.1.9:50051] local_endpoint[10.20.1.7:50051] "
    "start_time[2026-11-25 10:23:41.882310] "
    "query_total_us[57] query_route_us[5] query_rpc_us[50] "
    "query_retry_us[0] query_retry_count[0] "
    "select_us[18] local_endpoints_us[12] select_replica_us[6] "
    "buf_alloc_us[5] engine[classic] memread_total_us[196] "
    "read_submit_us[8] read_queue_us[14] read_urma_us[160] "
    "read_teardown_us[14] slice_count[2] read_urma_min_us[148] "
    "read_urma_avg_us[150] read_us[196] read_a_us[58] read_b_us[12] "
    "read_transfer_window_us[126] read_ab_valid[1] read_transfer_count[1] "
    "memcpy_us[0] total_us[269]")

GET_BUFFER_TENT = (
    GLOG + "trace_id[2] breakdown[get_buffer] key[bench/o] status[ok] "
    "size[1048576] type[memory_remote] replica_count[2] "
    "start_time[2026-11-25 10:23:41.882310] "
    "query_total_us[57] query_route_us[5] query_rpc_us[50] "
    "query_retry_us[0] select_us[18] local_endpoints_us[12] "
    "select_replica_us[6] buf_alloc_us[5] engine[tent] admit_us[10] "
    "queue_prep_us[90] wire_span_us[60] propagate_us[4] slice_count[2] "
    "bytes[1048576] read_us[164] total_us[250]")

GET_INTO = (
    GLOG + "trace_id[3] breakdown[get_into] key[bench/o] "
    "start_time[2026-11-25 10:23:44.000000] query_us[40] "
    "query_route_us[5] query_rpc_us[30] query_retry_us[0] select_us[10] "
    "local_endpoints_us[6] select_replica_us[4] read_us[120] "
    "offload_rpc_us[20] transfer_data_us[80] release_buffer_us[10] "
    "read_overhead_us[10] total_us[170] type[memory_remote] mode[full] "
    "replica_count[2] selected_type[memory_remote] "
    "local_endpoint[10.20.1.7:50051] remote_endpoint[10.20.1.9:50051] "
    "status[ok]")

BATCH_GET = (
    GLOG + "trace_id[4] breakdown[batch_get] num_keys[8] "
    "start_time[2026-11-25 10:23:42.000000] query_us[30] "
    "query_route_us[4] query_rpc_us[24] query_retry_us[0] prep_us[50] "
    "read_us[120] total_us[200] batch_get_ops[8] ssd_offload_ops[0] "
    "success[8] replicas[2] status[ok]")

PUT_CLASSIC = (
    GLOG + "trace_id[5] breakdown[put] key[bench/o] status[ok] "
    "size[1048576] replicas[2] start_time[2026-11-25 10:23:43.110220] "
    "prep_total_us[54] prep_buf_alloc_us[9] prep_memcpy_us[42] "
    "prep_split_us[3] checksum_us[31] prepare_us[4] query_us[0] "
    "put_start_us[58] engine[classic] memwrite_total_us[188] "
    "memwrite_submit_us[11] memwrite_queue_us[14] memwrite_urma_us[152] "
    "memwrite_teardown_us[11] slice_count[2] memwrite_urma_min_us[148] "
    "memwrite_urma_avg_us[150] put_end_us[66] finalize_us[0] "
    "cache_invalidate_us[0] revoke_us[0] total_us[401]")

UPSERT_TENT = (
    GLOG + "trace_id[6] breakdown[upsert] key[bench/o] status[ok] "
    "size[131072] replicas[2] start_time[2026-11-25 10:23:41.882310] "
    "prep_total_us[52] prep_buf_alloc_us[8] prep_memcpy_us[41] "
    "prep_split_us[3] checksum_us[29] prepare_us[4] query_us[57] "
    "put_start_us[0] engine[tent] admit_us[10] queue_prep_us[90] "
    "wire_span_us[150] propagate_us[4] slice_count[2] bytes[131072] "
    "put_end_us[0] finalize_us[60] cache_invalidate_us[2] revoke_us[0] "
    "total_us[458]")

BATCH_PUT = (
    GLOG + "trace_id[7] breakdown[batch_put] num_keys[4] "
    "start_time[2026-11-25 10:23:44.000000] prep_us[52] write_us[400] "
    "total_us[452] size[524288] success[4] status[ok]")

REMOVE = (
    GLOG + "trace_id[8] breakdown[remove] key[bench/o] status[ok] "
    "force[0] start_time[2026-11-25 10:23:45.000000] remove_rpc_us[30] "
    "total_us[30]")

LEGACY_GET_BREAKDOWN = (
    GLOG + "trace_id[9] get_breakdown key[bench/o] "
    "start_time[2026-11-25 10:23:41.882310] query_us[50] select_us[18] "
    "local_endpoints_us[12] select_replica_us[6] alloc_us[5] read_us[196] "
    "total_us[269] read_a_us[58] read_b_us[12] "
    "read_transfer_window_us[126] read_ab_valid[1] "
    "read_transfer_count[1] type[memory_remote] replica_count[2] "
    "status[read_ok]")

ALL_LINES = [GET_BUFFER_CLASSIC, GET_BUFFER_TENT, GET_INTO, BATCH_GET,
             PUT_CLASSIC, UPSERT_TENT, BATCH_PUT, REMOVE,
             LEGACY_GET_BREAKDOWN]


def _compute(tmp_path, lines):
    p = tmp_path / "t.glog"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return ts.compute_trace_stats(str(p))


# ---- P0：加载与静态校验 ----

def test_all_trees_load_and_validate():
    trees = tt.load_trace_trees()
    # 13 棵树 = 12 个真实接口 + batch_upsert_from（put 族薄行）；
    # 「get」不是独立树：breakdown[get] 经 get_buffer.legacy_ops 归一
    for op in ("get_buffer", "get_into", "batch_get", "batch_get_into",
               "put", "put_from", "upsert", "upsert_from",
               "batch_put", "batch_put_into", "batch_upsert_from",
               "remove", "batchRemove"):
        assert op in trees, op
    assert "get" not in trees          # 旧别名归一，不再有独立树
    assert trees["batch_put"]["group"] == "write"
    assert trees["remove"]["group"] == "other"
    assert trees["get_buffer"]["group"] == "read"


def test_schema_version_enforced():
    import json
    # 临时目录里放一个坏树：目录缺失 -> 硬失败（不静默回退）
    try:
        tt.load_trace_trees("/nonexistent/xx")
        assert False, "must raise"
    except tt.TreeError:
        pass


def test_missing_dir_is_hard_failure(tmp_path):
    try:
        tt.load_trace_trees(str(tmp_path / "nope"))
        assert False, "must raise"
    except tt.TreeError:
        pass


def test_bad_tree_raises(tmp_path):
    d = tmp_path / "trees"
    d.mkdir()
    (d / "bad.json").write_text(
        json.dumps({"schema": "mp-trace-tree/1", "op": "bad", "group": "read",
                    "total_field": "total_us",
                    "stages": [{"field": "dup_us"},
                               {"field": "dup_us"}]}),
        encoding="utf-8")
    try:
        tt.load_trace_trees(str(d))
        assert False, "duplicate field must raise"
    except tt.TreeError:
        pass


def test_stage_rows_shape():
    rows = tt.stage_rows_of(tt.load_trace_trees())["get_buffer"]
    # [field, level, parent, view]；L1 parent=total_us；views 行带 view key
    by_field = {r[0]: r for r in rows}
    assert by_field["total_us"] == ["total_us", 0, "", ""]
    assert by_field["query_total_us"][1] == 1
    assert by_field["query_total_us"][2] == "total_us"
    assert by_field["query_route_us"][1] == 2
    assert by_field["query_route_us"][2] == "query_total_us"
    assert by_field["read_submit_us"] == \
        ["read_submit_us", 2, "read_us", "classic"]
    assert by_field["admit_us"][3] == "tent"
    assert by_field["read_a_us"][3] == "ab"


# ---- P1：行匹配 / 归组 / 阶段集合与树一致 ----

def test_every_line_matches_its_tree(tmp_path):
    st = _compute(tmp_path, ALL_LINES)
    groups = st["groups"]
    assert sorted(groups["read"]["ops"]) == \
        ["batch_get", "get_buffer", "get_into"]
    assert sorted(groups["write"]["ops"]) == \
        ["batch_put", "put", "upsert"]
    assert sorted(groups["other"]["ops"]) == ["remove"]
    assert st["meta"]["unknown_ops"] == {}


def test_group_comes_from_tree_not_field_sniffing():
    # remove 带有 total_us 字段但必须归 other（树口径，非字段探测）
    p = ts.parse_line(REMOVE)
    assert ts._group_of(p) == "other"
    # 未知 op 落 other 并计数
    p2 = ts.parse_line(GLOG + "trace_id[99] breakdown[zzz] total_us[1] "
                      "start_time[2026-11-25 10:23:41.000000]")
    assert ts._group_of(p2) == "other"


def test_unknown_op_counted_in_meta(tmp_path):
    st = _compute(tmp_path, [PUT_CLASSIC,
                             GLOG + "trace_id[99] breakdown[zzz] key[k] "
                             "status[ok] total_us[5] "
                             "start_time[2026-11-25 10:23:41.000000]"])
    assert st["meta"]["unknown_ops"] == {"zzz": 1}
    # 未知 op 不进 read/write 组
    assert "zzz" not in st["groups"].get("write", {}).get("ops", {})


def test_l1_set_matches_tree_per_op():
    # fminutes 的字段集合 = 树 L1（chart:true）+ residual
    trees = tt.load_trace_trees()
    cf = tt.chart_fields_of(trees)
    assert "query_total_us" in cf["get_buffer"][0]
    assert "buf_alloc_us" in cf["get_buffer"][0]
    assert "query_us" not in cf["get_buffer"][0]     # 旧归一名不再存在
    assert "alloc_us" not in cf["get_buffer"][0]
    assert "prep_us" not in cf["get_buffer"][0]      # batch 专属
    assert cf["batch_put"][0] == ("prep_us", "write_us")   # 薄行两段
    # put 树不含 query/finalize（upsert 专属）；upsert 不含 put_start/put_end
    assert "query_us" not in cf["put"][0]
    assert "finalize_us" not in cf["put"][0]
    assert "put_start_us" not in cf["upsert"][0]
    # switch 两侧都在 L1 并集（classic + tent 混跑兼容）
    assert "memwrite_total_us" in cf["put"][0]
    assert "admit_us" in cf["upsert"][0]
    # residual 不在 L1（独立通道）
    assert "residual" not in cf["get_buffer"][0]
    # L2：query 三段 + 视图子段
    assert "query_route_us" in cf["get_buffer"][1]
    assert "read_submit_us" in cf["get_buffer"][1]   # classic 视图
    assert "read_a_us" in cf["get_buffer"][1]        # ab 视图


def test_residual_per_op_tree(tmp_path):
    st = _compute(tmp_path, [GET_BUFFER_CLASSIC, PUT_CLASSIC, UPSERT_TENT,
                             BATCH_PUT])
    # classic get_buffer: 269-(57+18+5+196) = -7
    gb = st["groups"]["read"]["ops"]["get_buffer"]
    assert abs(gb["residual"]["sum"] - (-7)) < 0.01
    # put: 401-(54+31+4+0+58+188+66+0+0+0) = 0
    put = st["groups"]["write"]["ops"]["put"]
    assert abs(put["residual"]["sum"]) < 0.01
    # upsert tent: 458-(52+29+4+57+10+90+150+4+60+2+0) = 0
    up = st["groups"]["write"]["ops"]["upsert"]
    assert abs(up["residual"]["sum"]) < 0.01
    # batch_put 薄行：452-(52+400) = 0（修正了按 put 族解析时的 残差=total）
    bp = st["groups"]["write"]["ops"]["batch_put"]
    assert abs(bp["residual"]["sum"]) < 0.01


def test_classic_read_transfer_segment_visible(tmp_path):
    """读路径传输段（classic 四段 / tent 四相位）必须在字段统计中出现——
    旧树完全没有它们（读树无 memread_*/四相位）。"""
    st = _compute(tmp_path, [GET_BUFFER_CLASSIC, GET_BUFFER_TENT])
    gb = st["groups"]["read"]["ops"]["get_buffer"]
    f = gb["fields"]
    for k in ("read_submit_us", "read_queue_us", "read_urma_us",
              "read_teardown_us",           # classic 视图
              "admit_us", "queue_prep_us", "wire_span_us", "propagate_us",
              "read_a_us", "read_transfer_window_us", "read_b_us"):
        assert k in f, k
    assert gb["engine"] == {"classic": 1, "tent": 1}


def test_legacy_get_breakdown_maps_to_native_names(tmp_path):
    st = _compute(tmp_path, [LEGACY_GET_BREAKDOWN])
    gb = st["groups"]["read"]["ops"]["get_buffer"]
    f = gb["fields"]
    # 旧名 query_us/alloc_us → 日志原生名 query_total_us/buf_alloc_us
    assert f["query_total_us"]["max"] == 50
    assert f["buf_alloc_us"]["max"] == 5
    assert "query_us" not in f and "alloc_us" not in f
    # read_ok 仍按成功归档
    assert gb["ok"] == 1 and gb["status"] == {"ok": 1}


def test_legacy_bd_alias_maps_to_target_tree():
    # 旧别名 op（get/get_batch/batch_get_buffer/...）→ get_buffer 树 op
    p = ts.parse_line(GLOG + "trace_id[1] breakdown[get] key[k] "
                      "query_total_us[5] total_us[9]")
    assert p["breakdown"] == "get_buffer"
    assert ts._group_of(p) == "read"
    p2 = ts.parse_line(GLOG + "trace_id[2] breakdown[get_batch] key[k] "
                       "query_total_us[5] total_us[9]")
    assert p2["breakdown"] == "get_buffer"   # legacy_ops 归一
    assert ts._group_of(p2) == "read"


def test_top_stages_follow_tree(tmp_path):
    st = _compute(tmp_path, [GET_BUFFER_CLASSIC])
    top = st["groups"]["read"]["ops"]["get_buffer"]["top"][0]
    s = top["stages"]
    # L1 + L2（含视图子段）都在 Top-N 展开集合
    for k in ("query_total_us", "select_us", "buf_alloc_us", "read_us",
              "query_route_us", "read_submit_us", "read_a_us"):
        assert k in s, k
    # put 树专属字段不串树
    assert "prep_total_us" not in s
    assert "put_start_us" not in s


def test_fields_stats_skip_tree_meta_fields(tmp_path):
    """树 meta_fields（slice_count/replica_count/*_min/avg_us 等）不进
    时延字段统计。"""
    st = _compute(tmp_path, [PUT_CLASSIC])
    f = st["groups"]["write"]["ops"]["put"]["fields"]
    assert "memwrite_urma_min_us" not in f
    assert "memwrite_urma_avg_us" not in f
    assert "checksum_us" in f
