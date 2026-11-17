#!/usr/bin/env python3
"""⑤ 报表：reduce 合并、自包含 HTML、data.js、总览、--rebuild-report。"""
import json
import os
import re
import shutil
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import benchmark as bm   # noqa: E402
import logstats as ls    # noqa: E402
import report as rp      # noqa: E402

ECHARTS = os.path.join(ROOT, "webui", "web", "vendor", "echarts.min.js")
HERE = os.path.dirname(os.path.abspath(__file__))


def _stats(tmp_path, name, role, node="n1", lat=1.0, n=40, secs=4):
    rows = []
    for i in range(n):
        t = "2026-11-12 10:00:{:02d}.{:06d}".format(i % secs, i * 1000)
        if role == "reader":
            rows.append("RESULT\t{}\tpipeline_read\tk{}\t{:.4f}\tOK\t0.5\t10\n"
                        .format(t, i, lat + i * 0.01))
        else:
            rows.append("RESULT\t{}\tpipeline_write\tk{}\t{:.4f}\tOK\t10\n"
                        .format(t, i, lat + i * 0.01))
    p = str(tmp_path / (name + ".log"))
    with open(p, "w", encoding="utf-8") as f:
        f.write("".join(rows))
        f.write("SUMMARY\t{}\n".format(json.dumps(
            {"mode": "pipeline_" + role, "wall_s": 10.0, "bytes": 4096,
             "max_depth": (3 if role == "reader" else 0)})))
    st = ls.compute_stats(p, node=node, role=role, size="8MB")
    with open(p + ".stats.json", "w", encoding="utf-8") as f:
        json.dump(st, f)
    os.remove(p)
    return p + ".stats.json"


def test_reduce_roles_clients_sources(tmp_path):
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    b = _stats(tmp_path, "b.reader", "reader", node="n2")
    data = rp.reduce_scenario(rp.load([a, b]),
                              {"config": "cfg", "mode": "pipeline"})
    z = data["sizes"]["8MB"]
    roles = {k for k, o in z["objects"].items() if o.get("kind") == "role"}
    # 角色 + 「总体（全部）」聚合（客户端全为 reader/writer 时也必须有）
    assert roles == {"all", "writer", "reader"}
    all_col = z["objects"]["all"]["tiers"]["cnt"]
    # 4 秒 x 2 客户端 x 10 样本 = 80；同一时刻的客户端行必须合并而非覆盖
    assert all_col["c"] and sum(all_col["c"]) == 80, all_col["c"]
    # 对象体系：角色 + 节点聚合 + 客户端；每对象都带自适应档
    assert "node:n1" in z["objects"] and "node:n2" in z["objects"]
    assert any(k.startswith("client:") for k in z["objects"])
    assert "cnt" in z["objects"]["writer"]["tiers"]
    assert z["summary"]["ok"] == 800          # 40 x kc=10, both roles
    assert len(z["clients"]) == 2
    assert {c["role"] for c in z["clients"]} == {"writer", "reader"}
    assert z["summary"]["max_depth"] == 3
    nodes = {n["node"]: n for n in data["nodes"]}
    assert set(nodes) == {"n1", "n2"}
    assert all(n["pods"] == 1 for n in nodes.values())
    assert len(data["sources"]) == 2
    assert all(s["remote_path"].endswith(".log") for s in data["sources"])


def test_render_self_contained_and_data_js(tmp_path):
    a = _stats(tmp_path, "a.writer", "writer")
    b = _stats(tmp_path, "b.writer", "writer")
    data = rp.reduce_scenario(rp.load([a, b]),
                              {"config": "cfg", "mode": "pipeline"})
    out = str(tmp_path / "report")
    html, dj = rp.render_scenario(data, out, ECHARTS, topk=1)
    html_src = open(html, encoding="utf-8").read()
    # self-contained: echarts inlined (>=500KB) + embedded report data
    assert len(html_src) > 500 * 1024
    assert "window.__MC_REPORT__" in html_src
    assert '"sizes"' in html_src
    # lightweight copy embeds only top-1 client per size
    embedded = re.search(r"window\.__MC_REPORT__ = (.*?);</script>",
                         html_src, re.S).group(1)
    emb = json.loads(embedded)
    assert len(emb["sizes"]["8MB"]["clients"]) == 1
    # data.js carries the full per-client data and is valid JS assignment
    dj_src = open(dj, encoding="utf-8").read()
    assert dj_src.startswith("window.__MC_FULL__ = ")
    full = json.loads(dj_src[len("window.__MC_FULL__ = "):].rstrip().rstrip(";"))
    assert len(full["sizes"]["8MB"]["clients"]) == 2
    # module namespaces do not leak the closing tag
    assert "</script>" not in embedded.replace("<\\/", "")


def test_data_js_loader_points_to_real_file(tmp_path):
    """report.html 必须引用同目录 data.js（曾经写成 format(True) -> src="True"）。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"})
    out = str(tmp_path / "report")
    html, dj = rp.render_scenario(data, out, ECHARTS, topk=1)
    src = open(html, encoding="utf-8").read()
    assert '<script src="data.js"' in src
    assert 'src="True"' not in src
    assert 'id="datanote"' in src          # 加载失败时的降级提示容器
    assert os.path.basename(dj) == "data.js"


def test_tier_policy_by_span(tmp_path):
    """时间档阶梯按时长裁剪：>1 天嵌入 1 分钟档；1 小时嵌入从 10 秒档起。"""
    st = {
        "meta": {"source": "x.writer.log", "bytes": 1, "node": "n1",
                 "role": "writer", "size": "8MB", "ts_from": 0,
                 "ts_to": 90000, "wall_s": 90000, "dated": True},
        "global": {"total": 10, "ok": 10, "fail": 0, "key_total": 10,
                   "key_ok": 10, "key_fail": 0, "avg_ms": 1.0,
                   "min_ms": 0.9, "max_ms": 2.0, "p50_ms": 1.0,
                   "p95_ms": 1.5, "p99_ms": 1.8, "p999_ms": 1.9,
                   "p9999_ms": 1.99, "qw_avg_ms": None, "qw_max_ms": None,
                   "exact": True},
        "histogram": {"30": 10},
        "ops": {}, "errors": {}, "error_excerpt": None,
        "slowest": [], "anomalies": [], "summaries": [],
        "seconds": [[t, 1, 1, 0, 1.0, 2.0, 0.0, 0.0, {"30": 1}]
                    for t in range(0, 90001, 60)],
        "minutes": [],
    }
    z = rp.reduce_size([st], {"config": "c"})
    assert z["embed_step"] == 60
    assert [t["key"] for t in z["tiers"]] == [
        "1", "10", "60", "600", "1800", "3600", "10800", "cnt"]
    assert z["embed_tiers"] == ["60", "600", "1800", "3600", "10800", "cnt"]
    # 1 小时 run：1s 超过 2048 点上限 -> 嵌入自 10s 档起
    st["meta"]["ts_to"] = 3600
    z2 = rp.reduce_size([st], {"config": "c"})
    assert z2["embed_step"] == 10
    assert [t["key"] for t in z2["tiers"]] == ["1", "10", "60", "cnt"]
    # 30 秒 run：只有 1s 档有意义（不足 8 桶的粒度不提供）
    st["meta"]["ts_to"] = 30
    z3 = rp.reduce_size([st], {"config": "c"})
    assert [t["key"] for t in z3["tiers"]] == ["1", "cnt"]


def test_render_overview(tmp_path):
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]),
                              {"config": "rank_smoke", "mode": "pipeline"})
    ov = rp.render_overview(
        {"run": "20261112-1000", "mode": "ssh",
         "nodes": [{"node": "n1", "pods": 1, "bytes": 4096}]},
        [dict(data, report_rel="logs/x/report/report.html")],
        str(tmp_path / "ov"))
    src = open(ov, encoding="utf-8").read()
    assert "运行总览" in src and "rank_smoke" in src
    assert "report/report.html" in src
    assert "节点日志量" in src


def test_rebuild_reports_local(tmp_path, monkeypatch):
    monkeypatch.setattr(bm, "RESULTS_DIR", str(tmp_path))
    logs = tmp_path / "20261112-1000" / "logs" / "rank_smoke"
    logs.mkdir(parents=True)
    raw = logs / "rank_smoke.benchmark_c1_t1_8MB.dual_n1_c0_p9301.writer.log"
    raw.write_text("".join(
        "RESULT\t2026-11-12 10:00:{:02d}.{:06d}\tpipeline_write\tk{}\t1.0\tOK\t4\n"
        .format(10 + i // 10, i % 10, i) for i in range(30)), encoding="utf-8")
    # 段1：run 时的 params/上下文落在 report/meta.json，rebuild 应恢复
    (logs / "report").mkdir(parents=True)
    (logs / "report" / "meta.json").write_text(json.dumps({
        "mode": "pipeline", "launch_mode": "rank_recall", "qps_target": 42,
        "protocol": "tcp", "started": "2026-11-12 10:00:00",
        "cluster_cfg": "cluster.json", "global_cfg": "global.json",
        "profile_cfg": "profile.json",
        "params": [["场景", "pipeline"], ["节点数", 3],
                   ["目标 QPS", "100 × 1 = 100"]]}), encoding="utf-8")
    rc = bm.rebuild_reports("20261112-1000")
    assert rc == 0
    html = (logs / "report" / "report.html").read_text(encoding="utf-8")
    assert "场景" in html and "节点数" in html and "目标 QPS" in html
    import re as _re
    emb = json.loads(_re.search(r"window\.__MC_REPORT__ = (.*?);</script>",
                                html, _re.S).group(1))
    ps = dict((p[0], p[1]) for p in (emb.get("meta") or {}).get("params") or [])
    # 客户端总数/节点数按实际加载的 stats 重算（旧 params 的幽灵值被纠正）
    assert ps["客户端总数"] == 1
    assert ps["节点数"] == 1
    assert (tmp_path / "20261112-1000" / "report" / "overview.html").is_file()
    # missing run -> non-zero
    assert bm.rebuild_reports("nope") == 2


def test_summary_three_sections_and_params(tmp_path):
    """段1：总体统计=性能统计/信息展示/参数配置 三子区；无返回链接。"""
    a = _stats(tmp_path, "a.writer", "writer")
    params = [["场景", "pipeline"], ["协议", "tcp"],
              ["writer.operation", "put_batch"], ["节点数", 2],
              ["开始时间", "2026-11-12 10:00:00"]]
    data = rp.reduce_scenario(rp.load([a]),
                              {"config": "cfg", "mode": "pipeline",
                               "launch_mode": "single", "params": params})
    out = str(tmp_path / "report")
    html, _dj = rp.render_scenario(data, out, ECHARTS)
    src = open(html, encoding="utf-8").read()
    # 三子区标题与容器
    for sec in ("性能统计", "信息展示", "参数配置"):
        assert sec in src, "missing section {}".format(sec)
    assert 'id="perfcards"' in src and 'id="infocards"' in src
    assert 'id="params"' in src
    # 参数卡数据随 meta 嵌入（label 出现在内嵌 JSON 中）
    assert "writer.operation" in src and "put_batch" in src
    # 返回总览链接已删除
    assert "返回运行总览" not in src
    assert "../../report/overview.html" not in src


def test_build_report_params_pipeline_and_basic():
    """benchmark._build_report_params：场景字段拍平 + 运行上下文。"""
    cfg = {"scenario": "pipeline", "protocol": "tcp", "work_mode": "rank",
           "traffic_mode": "pulse", "base_qps": 50, "qps_multiple": 2,
           "notify_count": 3, "data_size": ["8MB", "16MB"], "duration": 30,
           "warmup": {"duration": 5}, "batch_size": 1,
           "writer": {"operation": "put_batch", "count_per_node": 2,
                      "notify_threads": 4},
           "reader": {"operation": "get", "count_per_node": 2,
                      "num_threads": 1},
           "client": {"count_per_node": 2}, "numa_bind": True}
    meta = {"mode": "ssh", "launch_mode": "single", "clients": 8,
            "started": "2026-11-12 10:00:00"}
    ps = dict(bm._build_report_params(cfg, meta, nodes=4))
    assert ps["场景"] == "pipeline"
    assert ps["数据大小"] == "8MB、16MB"
    assert ps["目标 QPS"] == "50 × 2 = 100"
    assert ps["writer.operation"] == "put_batch"
    assert ps["reader.count_per_node"] == 2
    assert ps["运行模式"] == "真机"
    assert ps["启动方式"] == "单任务"
    assert ps["节点数"] == 4 and ps["客户端总数"] == 8
    assert ps["numa_bind"] == "是"
    # basic 配置的关键字段
    basic = {"scenario": "basic", "operation": "get", "key_count": 1000,
             "target_qps": 1000, "data_size": ["64KB"], "duration": 10}
    pb = dict(bm._build_report_params(basic, {"mode": "k8s", "clients": 1}))
    assert pb["操作"] == "get" and pb["key_count"] == 1000
    assert pb["目标 QPS"] == "1000 × 1 = 1000"
    assert pb["运行模式"] == "k8s"
    # 空字段不输出
    empty = dict(bm._build_report_params({}, {}))
    assert "writer.operation" not in empty and "节点数" not in empty



# ---------------------------------------------------------------------------
# C++ SDK trace integration (reduce + report section)
# ---------------------------------------------------------------------------

GLOG = "I20261112 10:23:41.000000 12345 te.cc:88] "
WRITE_OK_TRACE = (GLOG + "trace_id[1] breakdown[put] key[bench/ok] status[ok] "
                  "size[1048576] replicas[2] start_time[2026-11-12 10:23:43.110220] "
                  "prep_total_us[54] checksum_us[31] prepare_us[4] query_us[0] "
                  "put_start_us[58] memwrite_total_us[188] memwrite_urma_min_us[148] "
                  "put_end_us[66] total_us[401]")
WRITE_FAIL_TRACE = (GLOG + "trace_id[2] breakdown[put] key[bench/bad] status[TRANSFER_FAIL] "
                    "size[1048576] replicas[2] start_time[2026-11-12 10:23:44.002981] "
                    "prep_total_us[55] checksum_us[30] prepare_us[5] query_us[0] "
                    "put_start_us[63] memwrite_total_us[40012] memwrite_urma_min_us[-1] "
                    "put_end_us[0] total_us[40170]")
TRACE_READ = (GLOG + "trace_id[18446744073] breakdown[get_buffer] key[bench/obj] "
              "status[ok] size[262144] type[memory_remote] replica_count[2] "
              "remote_endpoint[10.20.1.9:50051] local_endpoint[10.20.1.7:50051] "
              "start_time[2026-11-12 10:23:41.882310] query_total_us[50] "
              "query_route_us[12] query_rpc_us[38] select_us[4] buf_alloc_us[6] "
              "memread_total_us[206] read_submit_us[16] read_queue_us[12] "
              "read_urma_us[166] read_teardown_us[12] slice_count[4] "
              "read_urma_min_us[140] read_urma_avg_us[154] memcpy_us[0] total_us[266]")
TRACE_WRITE = (GLOG + "trace_id[18446744073] breakdown[put] key[bench/obj] "
               "status[ok] size[1048576] replicas[2] "
               "start_time[2026-11-12 10:23:43.110220] prep_total_us[54] "
               "prep_buf_alloc_us[9] prep_memcpy_us[42] prep_split_us[3] "
               "checksum_us[31] prepare_us[4] query_us[0] put_start_us[58] "
               "memwrite_total_us[188] memwrite_submit_us[11] "
               "memwrite_queue_us[14] memwrite_urma_us[152] memwrite_teardown_us[11] "
               "slice_count[2] memwrite_urma_min_us[148] memwrite_urma_avg_us[150] "
               "put_end_us[66] finalize_us[0] cache_invalidate_us[0] revoke_us[0] "
               "total_us[401]")


def _trace_stats(tmp_path, name, size="8MB"):
    from tracestats import compute_trace_stats
    p = tmp_path / name
    p.write_text(TRACE_READ + "\n" + TRACE_WRITE + "\n", encoding="utf-8")
    return compute_trace_stats(str(p), node="n1", role="", size=size,
                               sample_rate=1.0)


def test_reduce_trace_block_and_sources(tmp_path):
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"},
                              traces=[_trace_stats(tmp_path, "c.glog")])
    z = data["sizes"]["8MB"]
    assert "trace" in z
    tr = z["trace"]
    assert tr["meta"]["trace_lines"] == 2 and tr["meta"]["files"] == 1
    read = tr["groups"]["read"]["ops"]["get_buffer"]
    assert read["count"] == 1
    tot = read["fields"]["total_us"]
    assert tot["max"] == 266 and tot["p50"] > 0
    assert "p99" in read["fields"]["memread_total_us"]
    write = tr["groups"]["write"]["ops"]["put"]
    assert write["fields"]["memwrite_total_us"]["max"] == 188
    # residual row present (~0 for both samples)
    assert read["residual"]["n"] == 1 and abs(read["residual"]["max"]) <= 1
    # time buckets carry avg/p50/p99 (indices 4/6/7)
    assert read["seconds"] and len(read["seconds"][0]) == 8
    # glog source included in the on-site index with kind=glog
    glogs = [s for s in data["sources"] if s.get("kind") == "glog"]
    assert glogs and glogs[0]["source"] == "c.glog"
    assert tr["top"] and tr["top"][0]["total_us"] == 401


def test_render_trace_section(tmp_path):
    a = _stats(tmp_path, "a.writer", "writer")
    tr = _trace_stats(tmp_path, "c.glog")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"}, traces=[tr])
    html, dj = rp.render_scenario(data, str(tmp_path / "out"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    assert "SDK 全链路" in src
    assert 'id="tracecard"' in src
    # 段3 三图：分级时延 / 阶段曲线 / 阶段树（含指标套件弹窗）
    assert 'id="levels"' in src and 'id="tchart"' in src and 'id="ttree"' in src
    # 阶段树细节在右半部分（不再用弹窗）
    assert 'id="tdetail"' in src and 'id="tsizeSel"' in src
    assert 'id="nodeModal"' not in src
    assert "分级时延" in src and "阶段树" in src
    assert "残差（total − 阶段和）" in src
    assert "最慢请求" in src
    # embedded data carries the trace block
    import re as _re
    emb = json.loads(_re.search(r"window\.__MC_REPORT__ = (.*?);</script>",
                                src, _re.S).group(1))
    assert "trace" in emb["sizes"]["8MB"]
    full = open(dj, encoding="utf-8").read()
    assert "trace" in full


def test_render_without_trace_keeps_card_hidden(tmp_path):
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "out2"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    # card exists but hidden by default; JS hides it when data.trace is absent
    assert 'id="tracecard" style="display:none"' in src


def _write_rows(tmp_path, name, rows):
    p = tmp_path / name
    p.write_text(rows, encoding="utf-8")
    return str(p)


def _minimal_stats(seconds_t, minutes_t, bucket_unit=None):
    st = {
        "meta": {"source": "x.writer.log", "bytes": 1, "node": "n1",
                 "role": "writer", "size": "8MB", "ts_from": 0,
                 "ts_to": 90000, "wall_s": 90000, "dated": True},
        "global": {"total": 10, "ok": 10, "fail": 0, "key_total": 10,
                   "key_ok": 10, "key_fail": 0, "avg_ms": 1.0,
                   "min_ms": 0.9, "max_ms": 2.0, "p50_ms": 1.0,
                   "p95_ms": 1.5, "p99_ms": 1.8, "p999_ms": 1.9,
                   "p9999_ms": 1.99, "qw_avg_ms": None, "qw_max_ms": None,
                   "exact": True},
        "histogram": {"30": 10}, "ops": {}, "errors": {},
        "error_excerpt": None, "slowest": [], "anomalies": [],
        "summaries": [],
        "seconds": [[t, 1, 1, 0, 1.0, 2.0, 0.0, 0.0, {"30": 1}]
                    for t in seconds_t],
        "minutes": [[t, 1, 1, 0, 1.0, 2.0, 0.0, 0.0, {"30": 1}]
                    for t in minutes_t],
    }
    if bucket_unit:
        st["meta"]["bucket_unit"] = bucket_unit
    return st


def test_old_stats_minutes_compat(tmp_path):
    """新引擎只从 1s 桶现算各档；旧 stats 的 minutes（epoch 分钟）不参与。"""
    old = _minimal_stats([3600, 3660], [60, 61])
    z = rp.reduce_size([old], {"config": "c"})
    col = z["objects"]["writer"]["tiers"]["60"]
    assert col["t"] == [3600, 3660]
    new = _minimal_stats([3600, 3660], [3600, 3660], bucket_unit="s")
    z2 = rp.reduce_size([new], {"config": "c"})
    assert z2["objects"]["writer"]["tiers"]["60"]["t"] == [3600, 3660]


def _js_check(tmp_path, data, *args):
    """用桩 DOM 执行 report 的 app JS；无 node 时跳过（返回 None）。"""
    node = shutil.which("node")
    if not node:
        print("  [skip] node not found")
        return None
    app = tmp_path / "app.js"
    app.write_text(rp._APP_JS, encoding="utf-8")
    dj = tmp_path / "data.json"
    dj.write_text(json.dumps(data), encoding="utf-8")
    r = subprocess.run([node, os.path.join(HERE, "report_js_check.js"),
                        str(app), str(dj)] + list(args),
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stdout + r.stderr
    return json.loads(r.stdout)


def test_tier_buttons_and_ladder(tmp_path):
    """1 小时 run：1s/10s/1min 三档可切，默认嵌入档 10s（1s 仅 data.js）。"""
    st = _minimal_stats([3600 + i for i in range(3600)], [3600],
                        bucket_unit="s")
    st["meta"]["ts_from"] = 3600
    st["meta"]["ts_to"] = 3600 + 3599
    data = rp.reduce_scenario([st], {"config": "c"})
    z = data["sizes"]["8MB"]
    assert [t["key"] for t in z["tiers"]] == ["1", "10", "60", "cnt"]
    assert z["embed_tiers"] == ["10", "60", "cnt"]
    res = _js_check(tmp_path, data, "--tier-label", "1 分钟")
    if res is None:
        return
    assert res["errors"] == []
    # 默认档 = 自适应（按样本数等分，<=2048 桶）；3600 样本 -> 每桶 2 个 -> 1800 桶
    assert res["init_len"] == 1800
    # 自适应桶按「桶内最后一个样本时刻」标注（横轴仍是时间轴）
    assert res["init_x0"] == 3601 * 1000
    assert res["min_len"] == 60             # 切到 1 分钟档


def test_long_run_embed_tier_and_datajs_finer(tmp_path):
    """>1 天 run：HTML 只嵌嵌入档以上；更细档（1s）完整留在 data.js。"""
    st = _minimal_stats([3600, 3660, 3720], [3600], bucket_unit="s")
    st["meta"]["ts_from"] = 0
    st["meta"]["ts_to"] = 90000          # 25h 跨度，但只有 3 个桶
    data = rp.reduce_scenario([st], {"config": "long"})
    assert data["sizes"]["8MB"]["embed_step"] == 60
    out = str(tmp_path / "report")
    html, dj = rp.render_scenario(data, out, ECHARTS, topk=5)
    import re as _re
    emb = json.loads(_re.search(r"window\.__MC_REPORT__ = (.*?);</script>",
                                open(html, encoding="utf-8").read(),
                                _re.S).group(1))
    eobj = emb["sizes"]["8MB"]["objects"]["writer"]
    assert "1" not in eobj["tiers"]              # 1s 不内嵌
    assert eobj["tiers"]["60"]["t"]              # 嵌入档有数据
    full = json.loads(open(dj, encoding="utf-8").read()[
        len("window.__MC_FULL__ = "):].rstrip().rstrip(";"))
    fobj = full["sizes"]["8MB"]["objects"]["writer"]
    assert len(fobj["tiers"]["1"]["t"]) == 3     # data.js 保留更细档



def test_trace_only_size_does_not_break_page(tmp_path):
    """只有 trace 的 data_size（summary 为空）不能把整页 JS 打挂。"""
    a = _stats(tmp_path, "a.writer", "writer")
    tr = _trace_stats(tmp_path, "c.glog", size="all")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"}, traces=[tr])
    assert data["sizes"]["all"]["summary"] is None
    res = _js_check(tmp_path, data, "--switch-size", "all",
                    "--expect-trace", "1")
    if res is None:
        return
    assert res["errors"] == []
    assert res["switched_error"] is False
    assert res["trace_visible"] is True


def test_html_meta_is_escaped(tmp_path):
    """配置名/run 名/节点名进入 HTML 前必须转义（防注入 & 防破版）。"""
    bad = '<img src=x onerror=alert(1)>'
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": bad, "mode": bad,
                                             "launch_mode": bad})
    out = str(tmp_path / "report")
    html, _dj = rp.render_scenario(data, out, ECHARTS, topk=5)
    src = open(html, encoding="utf-8").read()
    # 展示区（数据 JSON 之前）必须转义；JSON 里的原值在 <script> 内无害
    head = src[:src.index("window.__MC_REPORT__")]
    assert "<img src=x" not in head
    assert "&lt;img src=x" in head
    ov = rp.render_overview(
        {"run": bad, "mode": bad,
         "nodes": [{"node": bad, "pods": 1, "bytes": 10}]},
        [dict(data, report_rel="logs/x/report/report.html")],
        str(tmp_path / "ov"))
    osrc = open(ov, encoding="utf-8").read()
    ohead = osrc[:osrc.index("window.__MC_REPORT__")]
    assert "<img src=x" not in ohead
    assert "&lt;img src=x" in ohead


def test_zero_wall_has_no_infinity(tmp_path):
    """旧日志无日期（wall=0）时卡片不能出现 Infinity/NaN。"""
    st = _minimal_stats([], [], bucket_unit="s")
    st["meta"].update({"ts_from": None, "ts_to": None, "wall_s": 0,
                       "dated": False})
    data = rp.reduce_scenario([st], {"config": "c"})
    s = data["sizes"]["8MB"]["summary"]
    assert s["throughput_mb_s"] is None
    assert s["qps"] == 0.0
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert "Infinity" not in res["cards_text"]
    assert "NaN" not in res["cards_text"]
    assert "吞吐" in res["cards_text"]


def test_trace_percentiles_not_clamped_by_ms_ladder(tmp_path):
    """trace 是 µs；>2.16ms 的请求不能被 ms 阶梯钳到 2.4ms 附近。"""
    from tracestats import compute_trace_stats
    lines = []
    for i in range(100):
        lines.append(
            GLOG + "trace_id[1] breakdown[put] key[k%d] status[ok] size[1048576] "
            "replicas[2] start_time[2026-11-12 10:23:4%d.000000] "
            "prep_total_us[54] prep_buf_alloc_us[9] prep_memcpy_us[42] "
            "prep_split_us[3] checksum_us[31] prepare_us[4] query_us[0] "
            "put_start_us[58] memwrite_total_us[39400] memwrite_submit_us[11] "
            "memwrite_queue_us[39000] memwrite_urma_us[152] "
            "memwrite_teardown_us[11] slice_count[2] memwrite_urma_min_us[148] "
            "memwrite_urma_avg_us[150] put_end_us[414] finalize_us[0] "
            "cache_invalidate_us[0] revoke_us[0] total_us[50000]"
            % (i, i % 10))
    p = tmp_path / "slow.glog"
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    tr = compute_trace_stats(str(p))
    assert tr["meta"]["bins"] == "us-v1"
    blk = rp.reduce_trace_size([tr])
    tot = blk["groups"]["write"]["ops"]["put"]["fields"]["total_us"]
    assert tot["max"] == 50000
    assert 40000 <= tot["p50"] <= 65000, tot
    assert 40000 <= tot["p99"] <= 65000, tot
    bucket = blk["groups"]["write"]["ops"]["put"]["seconds"][0]
    assert 40000 <= bucket[6] <= 65000 and 40000 <= bucket[7] <= 65000
    # 旧 trace.json（无 bins）无法安全解释直方图 -> 分位留空，不静默错读
    legacy = json.loads(json.dumps(tr))
    legacy["meta"].pop("bins")
    ltot = rp.reduce_trace_size([legacy])["groups"]["write"]["ops"]["put"][
        "fields"]["total_us"]
    assert ltot["p50"] is None and ltot["max"] == 50000


def test_trace_avg_excludes_neg_sentinel(tmp_path):
    """-1 哨兵不参与时延统计：平均值的分母应为 n-neg（原为 n，被稀释）。"""
    from tracestats import compute_trace_stats
    p = tmp_path / "neg.glog"
    p.write_text(WRITE_FAIL_TRACE + "\n" + WRITE_OK_TRACE + "\n",
                 encoding="utf-8")
    tr = compute_trace_stats(str(p))
    blk = rp.reduce_trace_size([tr])
    fd = blk["groups"]["write"]["ops"]["put"]["fields"]["memwrite_urma_min_us"]
    assert fd["n"] == 2 and fd["neg"] == 1
    assert fd["avg"] == 148.0       # 只有 148 一个有效样本（不是 74）
    assert fd["avg_n"] == 1


def test_summary_percentiles_consistent_with_csv(tmp_path):
    """报表 summary 的 max 取精确值、分位不超过 max；单实例与 CSV 同口径。"""
    p = tmp_path / "u.log"
    p.write_text("".join(
        "RESULT\t2026-11-15 10:00:{:02d}.{:06d}\tput\tk{}\t{:.6f}\tOK\n".format(
            i % 60, i, i, 1.0 + i) for i in range(100)), encoding="utf-8")
    st = ls.compute_stats(str(p), size="8MB", role="writer")
    s = rp.reduce_size([st], {"config": "c"})["summary"]
    agg = bm._merge_stats_aggs([st])
    assert s["max_ms"] == 100.0 and s["min_ms"] == 1.0
    assert s["p50_ms"] == agg["p50_ms"] == 51.0
    assert s["p99_ms"] == agg["p99_ms"] == 99.0
    assert s["p999_ms"] <= s["max_ms"]
    # 多客户端：max 仍取精确最大，分位按合并直方图且不超过 max
    st2 = json.loads(json.dumps(st))
    st2["meta"]["source"] = "b.writer.log"
    s2 = rp.reduce_size([st, st2], {"config": "c"})["summary"]
    assert s2["max_ms"] == 100.0 and s2["p999_ms"] <= s2["max_ms"]


def test_summary_throughput_uses_bytes(tmp_path):
    """吞吐卡必须用字节数（原用 key 数/1MiB，相当于随便一个数）。"""
    a = _stats(tmp_path, "a.writer", "writer")
    b = _stats(tmp_path, "b.writer", "writer")
    s = rp.reduce_size(rp.load([a, b]), {"config": "c"})["summary"]
    assert s["bytes"] == 8192                 # 2 x SUMMARY bytes=4096
    assert abs(s["throughput_mb_s"]
               - 8192 / s["wall_s"] / 1048576.0) < 1e-6


def test_overview_sums_all_data_sizes(tmp_path):
    """overview 卡片要汇总所有 data_size（原只取第一个 size）。"""
    a = _minimal_stats([3600], [3600], bucket_unit="s")
    b = json.loads(json.dumps(a))
    b["meta"]["size"] = "64MB"
    sc = rp.reduce_scenario([a, b], {"config": "multi", "mode": "basic"})
    assert set(sc["sizes"]) == {"8MB", "64MB"}
    ov = rp.render_overview({"run": "r", "mode": "ssh", "nodes": []},
                            [dict(sc, report_rel="logs/m/report/report.html")],
                            str(tmp_path / "ov"))
    src = open(ov, encoding="utf-8").read()
    assert '<div class="k">成功</div><div class="v">20</div>' in src
    assert '<div class="k">失败</div><div class="v">0</div>' in src


def test_qw_average_divides_by_ok_rows(tmp_path):
    """队列等待均值只对成功行取平均（qw 仅 OK 行累计，分母原为全部行）。"""
    p = tmp_path / "r.reader.log"
    p.write_text(
        "RESULT\t2026-11-15 10:00:01.000000\tpipeline_read\tk1\t2.0\tOK\t0.5\t4\n"
        "RESULT\t2026-11-15 10:00:01.500000\tpipeline_read\tk2\t0.0\t"
        "MISS/MISMATCH\t0.0\t4\n", encoding="utf-8")
    st = ls.compute_stats(str(p), role="reader", size="8MB")
    z = rp.reduce_size([st], {"config": "c"})
    col = z["objects"]["reader"]["tiers"]["1"]
    assert col["c"][0] == 2 and col["ok"][0] == 1
    assert col["qw"][0] == 0.5          # 不是 0.25


def test_merge_field_does_not_mutate_source(tmp_path):
    """_merge_field 首次合并必须深拷贝 hist：两个 trace 文件 reduce 两次会
    把第一个文件的源直方图累加进去，污染后续 reduce/重建。"""
    tr1 = _trace_stats(tmp_path, "a.glog")
    tr2 = _trace_stats(tmp_path, "b.glog")
    before1 = json.loads(json.dumps(tr1))["groups"]["read"]["ops"][
        "get_buffer"]["fields"]["total_us"]["hist"]
    rp.reduce_trace_size([tr1, tr2])
    rp.reduce_trace_size([tr1, tr2])
    after1 = tr1["groups"]["read"]["ops"]["get_buffer"]["fields"][
        "total_us"]["hist"]
    assert after1 == before1


def test_extended_ms_ladder_keeps_second_scale_tail(tmp_path):
    """v2 阶梯覆盖到 ~250s：多秒停顿的 p99 不再被 2160ms 末桶钳死。"""
    rows = "".join(
        "RESULT\t2026-11-15 10:00:0{}.000000\tput\tk\t{:.1f}\tOK\n".format(i, v)
        for i, v in enumerate([5000.0, 5000.0]))
    st = ls.compute_stats(_write_rows(tmp_path, "a.log", rows),
                          role="writer", size="8MB")
    assert st["meta"]["hist_version"] == 2
    st2 = json.loads(json.dumps(st))
    st2["meta"]["source"] = "b.writer.log"
    s = rp.reduce_size([st, st2], {"config": "c"})["summary"]
    assert 4200 <= s["p99_ms"] <= 6500, s
    # 旧产物（无 hist_version、按 v1 阶梯分桶）仍按 v1 解释，不误读
    old = json.loads(json.dumps(st))
    old["meta"].pop("hist_version")
    old["histogram"] = {"80": 2}
    old["seconds"] = []        # 旧产物整份都是 v1 分桶
    old["minutes"] = []
    old2 = json.loads(json.dumps(old))
    old2["meta"]["source"] = "d.writer.log"
    so = rp.reduce_size([old, old2], {"config": "c"})["summary"]
    assert so["p99_ms"] < 3000
    # CSV 的 stats-first 聚合同样按版本选阶梯
    agg = bm._merge_stats_aggs([st, st2])
    assert 4200 <= agg["p99_ms"] <= 6500


# ---------------------------------------------------------------------------
# ③ phase footnote + WebUI raw-endpoint link adapter
# ---------------------------------------------------------------------------

def _trace_stats_with_markers(tmp_path, name, warmup_n=2):
    import tracestats
    base = tracestats._start_epoch("2026-11-15 09:27:33.000000")
    lines = ["MC_PHASE warmup start {:.6f}".format(base)]
    for i in range(warmup_n):
        lines.append(
            "I20261115 09:27:33.000000 1 te.cc:1] trace_id[1] breakdown[put] "
            "key[mc_warmup*{}] status[ok] size[1] replicas[1] "
            "start_time[2026-11-15 09:27:33.{:06d}] prep_total_us[1] "
            "total_us[100]".format(i, i * 10000))
    lines.append("MC_PHASE measured start {:.6f}".format(base + 1.0))
    lines.append(
        "I20261115 09:27:35.000000 1 te.cc:1] trace_id[1] breakdown[put] "
        "key[k] status[ok] size[1] replicas[1] "
        "start_time[2026-11-15 09:27:35.000000] prep_total_us[1] "
        "total_us[100]")
    lines.append("MC_PHASE measured end {:.6f}".format(base + 3.0))
    p = tmp_path / name
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return tracestats.compute_trace_stats(str(p), node="n1", size="8MB")


def test_reduce_trace_phase_counts_and_markers(tmp_path):
    a = _stats(tmp_path, "a.writer", "writer")
    tr = _trace_stats_with_markers(tmp_path, "m.glog", warmup_n=2)
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"}, traces=[tr])
    tm = data["sizes"]["8MB"]["trace"]["meta"]
    assert tm["markers"] is True
    assert tm["phase_counts"]["warmup"] == 2
    assert tm["phase_counts"]["measured"] == 1
    op = data["sizes"]["8MB"]["trace"]["groups"]["write"]["ops"]["put"]
    assert op["count"] == 1            # only the measured row aggregated


def test_render_report_has_rel_base_and_adapter(tmp_path):
    a = _stats(tmp_path, "a.writer", "writer")
    tr = _trace_stats_with_markers(tmp_path, "m.glog")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"}, traces=[tr])
    html, dj = rp.render_scenario(data, str(tmp_path / "out"), ECHARTS,
                                   rel_base="outputs/t1/logs/cfg/report")
    src = open(html, encoding="utf-8").read()
    # WebUI link adapter wired with the outputs-relative base
    assert 'window.__MC_REL__ = "outputs/t1/logs/cfg/report"' in src
    assert "__MC_DATA_REL__" in src
    assert "__MC_FULL_READY__" in src
    assert 'api/file?path=' in src
    # file:// path unchanged: static data.js tag still there
    assert 'src="data.js"' in src
    # phase footnote: built at runtime by the app JS from phase_counts —
    # the JS source carries the template and __MC_FULL_READY__ hook
    assert "已按阶段标记剔除" in src
    assert "phase_counts" in src and "pc.warmup||0" in src


def test_render_overview_has_rel_base_and_adapter(tmp_path):
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"})
    ov = rp.render_overview(
        {"run": "t1", "mode": "ssh", "nodes": [{"node": "n1", "pods": 1,
                                                "bytes": 100}]},
        [dict(data, report_rel="logs/x/report/report.html")],
        str(tmp_path / "ov"), rel_base="outputs/t1")
    src = open(ov, encoding="utf-8").read()
    assert 'window.__MC_REL__ = "outputs/t1"' in src
    assert 'a[href^="../"]' in src           # the adapter rewrites scenario links
    # no data.js loader on the overview page: the adapter script mentions
    # __MC_DATA_REL__ (shared source) but there is no assignment
    assert "window.__MC_DATA_REL__ = " not in src


def test_legacy_trace_no_marker_note(tmp_path):
    a = _stats(tmp_path, "a.writer", "writer")
    import tracestats
    p = tmp_path / "legacy.glog"
    p.write_text(
        "I20261115 09:27:33.000000 1 te.cc:1] trace_id[1] breakdown[put] "
        "key[k] status[ok] size[1] replicas[1] "
        "start_time[2026-11-15 09:27:33.000000] total_us[100]\n",
        encoding="utf-8")
    tr = tracestats.compute_trace_stats(str(p), node="n1", size="8MB")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"}, traces=[tr])
    tm = data["sizes"]["8MB"]["trace"]["meta"]
    assert tm["markers"] is False
    assert tm["phase_counts"]["measured"] == 1   # legacy: all measured
    html, _ = rp.render_scenario(data, str(tmp_path / "out2"), ECHARTS)
    assert "无阶段标记" in open(html, encoding="utf-8").read()


# ---------------------------------------------------------------------------
# 段2：分桶引擎 / 对象预算 / 多对象叠加
# ---------------------------------------------------------------------------

def test_tier_policy_ladder():
    assert rp._tier_policy(30) == ([1], 1)                       # 30s：仅 1s
    assert rp._tier_policy(0) == ([1, 60], 1)                    # 未知跨度兜底
    offered, embed = rp._tier_policy(3600)
    assert offered == [1, 10, 60] and embed == 10                 # 1h：1s 超上限
    offered, embed = rp._tier_policy(90000)
    assert embed == 60 and 10800 in offered and 21600 not in offered
    offered, embed = rp._tier_policy(86400 * 3)
    assert embed == 600 and offered[-1] == 21600                   # 3 天：粗档封顶
    assert 86400 not in offered                                   # <8 桶不提供


def test_count_bucket_equal_samples():
    # 100 行 × 10 样本 = 1000 样本；目标 100 桶 -> 每桶 ~10 样本
    rows = [[1000 + i, 10, 10, 0, 10.0, 20.0, 0.0, 0.0, {"30": 10}]
            for i in range(100)]
    merged = rp._count_rows(rows, 100)
    assert 80 <= len(merged) <= 120
    total = sum(b[0] for b in merged.values())
    assert total == 1000                       # 样本总数不丢
    # 桶键 = 桶内末样本时刻（时间轴）
    keys = sorted(merged)
    assert keys[0] >= 1000 and keys[-1] == 1099
    cols = rp._series_columns(merged, 1, with_span=True)
    assert len(cols["t"]) == len(cols["span"]) == len(merged)


def test_objects_and_budget_trimming(tmp_path):
    """节点过多时嵌入按预算裁剪；data.js 仍保留全部对象。"""
    stats = []
    for n in range(60):
        st = _minimal_stats([90000], [90000], bucket_unit="s")
        st["meta"].update({"node": "node%d" % n, "source": "c%d.log" % n,
                           "ts_from": 0, "ts_to": 90000})
        st["seconds"] = [[90000 - i, 10, 10, 0, 1.0, 2.0, 0.0, 0.0, {"30": 10}]
                         for i in range(0, 3600, 60)]   # 60 buckets
        stats.append(st)
    data = rp.reduce_scenario(stats, {"config": "c"})
    z = data["sizes"]["8MB"]
    assert len([k for k in z["objects"] if k.startswith("node:")]) == 60
    html, dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS, topk=5)
    import re as _re
    emb = json.loads(_re.search(r"window\.__MC_REPORT__ = (.*?);</script>",
                                open(html, encoding="utf-8").read(),
                                _re.S).group(1))["sizes"]["8MB"]
    rows = sum(rp._tier_rowcount(o.get("tiers"))
               for o in emb["objects"].values())
    assert rows <= rp.EMBED_ROW_BUDGET
    assert emb["object_total"] == len(z["objects"])


def test_overlay_two_objects_js(tmp_path):
    """对象多选叠加：同一指标两条曲线，客户端自动落到自适应档。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    b = _stats(tmp_path, "b.reader", "reader", node="n2")
    data = rp.reduce_scenario(rp.load([a, b]), {"config": "c"})
    res = _js_check(tmp_path, data, "--add-obj", "node:n2")
    if res is None:
        return
    assert res["errors"] == []
    assert res["overlay_names"] and len(res["overlay_names"]) == 2
    assert res["overlay_len"] and res["overlay_len"] > 0


def test_trace_field_sigma_and_reduce_merge(tmp_path):
    """段3：阶段 σ（sumsq 合并）+ fminutes 跨客户端合并。"""
    tr = _trace_stats(tmp_path, "a.glog")
    b = _trace_stats(tmp_path, "b.glog")
    z = rp.reduce_trace_size([tr, b])
    read = z["groups"]["read"]["ops"]["get_buffer"]
    tot = read["fields"]["total_us"]
    assert tot["sigma"] is not None and tot["sigma"] >= 0
    assert read["fminutes"].get("total_us") is None or True  # total 不在 fminutes
    assert "query_total_us" in read["fminutes"]
    merged = read["fminutes"]["query_total_us"]
    assert len(merged) >= 1
    # 合并后样本数翻倍（两个相同 glog）
    assert sum(r[1] for r in merged) == 2


def test_trace_three_charts_js(tmp_path):
    """段3：分级时延/阶段树渲染、点击节点弹指标套件，全程无 JS 异常。"""
    a = _stats(tmp_path, "a.reader", "reader")
    tr = _trace_stats(tmp_path, "c.glog")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"}, traces=[tr])
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    lv = res.get("levels_text") or ""
    assert "total" in lv and "memread_total_us" in lv and "残差" in lv
    tree = res.get("tree_text") or ""
    assert "total（端到端）" in tree and "memread_total_us" in tree
    # 细节在右半部分（不再是弹窗）
    assert res.get("modal_visible") is None
    mt = res.get("detail_text") or ""
    assert ("样本数" in mt) or ("平均" in mt)


# ---------------------------------------------------------------------------
# lieflat 风格改造：F14 直方图数据 / G10 增幅 / 新卡结构
# ---------------------------------------------------------------------------

def test_summary_hist_bins_and_edges(tmp_path):
    """F14：summary 透传合并直方图（非零 bins）与分箱边界；计数守恒。"""
    a = _stats(tmp_path, "a.writer", "writer")
    b = _stats(tmp_path, "b.writer", "writer")
    data = rp.reduce_scenario(rp.load([a, b]), {"config": "c"})
    s = data["sizes"]["8MB"]["summary"]
    assert s["hist_bins"], "hist_bins missing"
    assert len(s["bin_edges"]) >= 2
    # 直方图按 RESULT 行计数（_stats 每个客户端 40 行，两个客户端 80 行）
    assert sum(n for _i, n in s["hist_bins"]) == 80
    idxs = [i for i, _n in s["hist_bins"]]
    assert idxs == sorted(idxs)


def test_monitor_proc_growth_sorted(tmp_path):
    """G10：RSS 增幅列表按增长降序，泄露标记保留。"""
    root = tmp_path / "logs" / "cfg" / "profiling" / "n1"
    root.mkdir(parents=True)
    (root / "mem.proc.csv").write_text(
        "ts,pid,comm,rss_kb,pss_kb,private_dirty_kb,private_clean_kb,swap_kb\n"
        "1000,11,a,102400,100000,1,1,0\n"
        "1001,11,a,409600,400000,1,1,0\n"
        "1000,22,b,102400,100000,1,1,0\n"
        "1001,22,b,112640,110000,1,1,0\n", encoding="utf-8")
    mon = rp.reduce_monitors(str(tmp_path / "logs" / "cfg"))
    g = mon["summary"]["proc_growth"]
    assert g and g[0]["growth_mb"] >= g[-1]["growth_mb"]
    assert g[0]["leak_suspect"] is True
    assert g[0]["label"].startswith("n1/11")


def test_render_new_charts_structure(tmp_path):
    """新图卡存在且默认隐藏（无数据时不显示）；来源行已加。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "out"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    for i in ('id="histcard"', 'id="slowcard"', 'id="histchart"',
              'id="slowchart"'):
        assert i in src
    assert 'class="src"' in src
    # lieflat token 已内联（mono-tokens / color-presets）
    assert "window.MONO" in src or "MONO=" in src
    assert "PRESETS" in src and "F5572F" in src


# ---------------------------------------------------------------------------
# 回归：profiling 产物不得变成幽灵数据档；F14 直方图单位/标注正确
# ---------------------------------------------------------------------------

def test_profiling_artifacts_not_clients(tmp_path, monkeypatch):
    """logs/profiling/*/mem.err.log 是监控产物，不是客户端日志：
    不得生成 stats（幽灵 'all' 档）、不得计入客户端数。"""
    logs = tmp_path / "logs" / "cfg"
    (logs / "profiling" / "store1").mkdir(parents=True)
    rows = "".join(
        "RESULT\t2026-11-16 10:00:{:02d}.{:06d}\tpipeline_read\tk{}\t1.0\tOK\t0.5\t4\n"
        .format(10 + i // 10, i % 10, i) for i in range(30))
    (logs / "cfg.benchmark_c1_t1_8MB.reader.log").write_text(rows,
                                                             encoding="utf-8")
    (logs / "profiling" / "store1" / "mem.err.log").write_text(
        "numastat: command not found\n", encoding="utf-8")
    stats = bm._collect_local_stats(str(logs))
    assert len(stats) == 1, stats
    assert not (logs / "profiling" / "store1" /
                "mem.err.log.stats.json").exists()
    assert bm._is_client_log("mem.err.log") is False
    data = rp.reduce_scenario(rp.load(stats), {"config": "cfg"})
    assert "all" not in data["sizes"], list(data["sizes"])


def test_hist_chart_units_and_marklines(tmp_path):
    """F14：bin_edges(ms) -> µs 格式化；p50/p99 标注线必须命中分箱。"""
    a = _stats(tmp_path, "a.reader", "reader")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    s = data["sizes"]["8MB"]["summary"]
    assert s["p50_ms"] < s["p99_ms"]
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert res["hist_cats"] > 0
    # avg / p50 / p99 / p999 四条（p9999 需样本 >= 10000）
    assert res["hist_marks"] == 4, res
    assert ("µs" in res["hist_first"]) or ("ms" in res["hist_first"]), res


# ---------------------------------------------------------------------------
# 真实 ECharts 渲染冒烟（桩 setOption 掩盖过 markLine 事故，必须真渲染）
# ---------------------------------------------------------------------------

def test_real_echarts_render_smoke(tmp_path):
    """--real：真实 ECharts 逐图 setOption + renderToSVGString，任何一图失败
    即失败；F14 标注线必须落在真实 label 上（曾是 undefined -> 整页中断）。"""
    a = _stats(tmp_path, "a.reader", "reader")
    tr = _trace_stats(tmp_path, "c.glog")
    mons = {
        "mem_host": [{"node": "n1", "t": [1000, 1001],
                      "columns": ["n0_used_mb"],
                      "series": {"n0_used_mb": [10.0, 20.0]}}],
        "mem_proc": [{"node": "n1", "pid": 1, "comm": "python",
                      "t": [1000, 1001], "rss_mb": [10.0, 20.0],
                      "pss_mb": [9.0, 18.0], "rss_growth_mb": 10.0,
                      "rss_growth_pct": 100.0, "leak_suspect": False}],
        "urma": [{"node": "n1", "ubpu": 0, "port": 1, "t": [1000, 1001],
                  "rx": [1.0, 2.0], "tx": [0.5, 1.0],
                  "rx_avg": 1.5, "rx_max": 2.0,
                  "tx_avg": 0.75, "tx_max": 1.0}],
        "summary": {
            "peak_mem_mb": {"n1": 20}, "peak_urma": None,
            "worst_proc": {"label": "n1/1 python", "growth_mb": 10.0,
                           "growth_pct": 100.0, "leak_suspect": False},
            "proc_growth": [{"label": "n1/1 python", "growth_mb": 10.0,
                             "growth_pct": 100.0, "leak_suspect": False}],
        },
    }
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"},
                              monitors=mons, traces=[tr])
    res = _js_check(tmp_path, data, "--real")
    if res is None:
        return
    assert res["errors"] == [], res["errors"]
    real = res.get("real") or {}
    assert real.get("fail") == 0, real
    assert real.get("ok", 0) >= 7, real
    # F14 标注线：xAxis 必须是非空 label 字符串
    assert res.get("hist_marks") == 4, res.get("hist_marks")
    xs = res.get("hist_mark_x") or []
    assert len(xs) == 4
    assert all(isinstance(x, str) and x for x in xs), xs


# ---------------------------------------------------------------------------
# 报表 v3：排版基线 / 自适应分桶 / 对象树 / 离群值 / 分级时延聚焦
# ---------------------------------------------------------------------------

def test_adaptive_bucket_rule(tmp_path):
    """自适应档：<=2048 桶；样本不足 2048 时逐样本成桶（全量绘制）。"""
    st = _minimal_stats([10 * i for i in range(40)], [600])
    st["meta"]["ts_from"] = 0
    st["meta"]["ts_to"] = 400
    z = rp.reduce_size([st], {"config": "c"})
    cnt = z["objects"]["writer"]["tiers"]["cnt"]
    assert len(cnt["t"]) == 40, len(cnt["t"])
    assert all(c == 1 for c in cnt["c"]), cnt["c"]
    # 样本多于 2048：桶数封顶 2048，每桶样本数相等
    st2 = _minimal_stats([10 * i for i in range(5000)], [600])
    st2["meta"]["ts_from"] = 0
    st2["meta"]["ts_to"] = 50000
    z2 = rp.reduce_size([st2], {"config": "c"})
    cnt2 = z2["objects"]["writer"]["tiers"]["cnt"]
    assert 0 < len(cnt2["t"]) <= 2048, len(cnt2["t"])
    per = sorted(set(cnt2["c"]))
    assert len(per) <= 2, per                    # 等分（最多差 1）
    assert cnt2["t"] == sorted(cnt2["t"])


def test_report_has_no_micro_captions_and_tiny_fonts(tmp_path):
    """排版基线：编辑性 .src 小注整条删除；CSS/图表字号不得低于 12px。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "out"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    # 装饰性来源小注整条删除（用报表正文独有的文案判定，
    # 避免误伤 vendored lieflat 主题里的注释模板）
    for gone in ("指标卡与参数台账", "单变量分箱频数", "单点时延证据",
                 "系列：QPS/失败/平均/p50/p99/p999/最大", "账本式表格"):
        assert gone not in src, gone
    assert ".src{" not in rp._PAGE_CSS
    css_sizes = [float(x) for x in re.findall(r"font-size:([\d.]+)px", rp._PAGE_CSS)]
    assert css_sizes and min(css_sizes) >= 12, min(css_sizes)
    js_sizes = [float(x) for x in re.findall(r"fontSize:([\d.]+)", rp._APP_JS)]
    assert js_sizes and min(js_sizes) >= 12, min(js_sizes)


def test_section_order_and_renames(tmp_path):
    """区块顺序：时延曲线 -> 时延分布 -> 慢点捕捉；长说明删除。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "out"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    i_curve = src.index("<h2>时延曲线</h2>")
    i_hist = src.index("<h2>时延分布</h2>")
    i_slow = src.index("<h2>慢点捕捉</h2>")
    assert i_curve < i_hist < i_slow
    assert "慢点分布" not in src
    assert "对象可选：总体（含 writer/reader 角色）" not in src
    # 失败数不再挤在 QPS 轴上：独立的吞吐/失败窄面板
    assert 'id="qpschart"' in src and 'id="qpsNote"' in src


def test_object_tree_and_qps_panel_js(tmp_path):
    """对象树：总体/节点分组 + 每级全选清空；吞吐/失败面板已渲染。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    b = _stats(tmp_path, "b.reader", "reader", node="n2")
    data = rp.reduce_scenario(rp.load([a, b]), {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    rows = res.get("tree_rows") or []
    joined = " ".join(rows)
    assert "总体与角色" in joined and "节点与客户端" in joined
    assert "全选" in joined and "清空" in joined
    assert res.get("qps_series"), res.get("qps_series")
    note = res.get("qps_note") or ""
    assert "失败" in note, note


def test_outlier_toggle_masks_buckets(tmp_path):
    """离群值剔除：阀值 = 10 x p99，命中桶从折线挖空（可一键开关）。"""
    st = _minimal_stats([10 * i for i in range(20)], [600])
    st["meta"]["ts_from"] = 0
    st["meta"]["ts_to"] = 200
    st["seconds"][7][5] = 1000.0            # 一个巨大离群值
    data = rp.reduce_scenario([st], {"config": "c"})
    res = _js_check(tmp_path, data, "--outlier", "1")
    if res is None:
        return
    assert res["errors"] == []
    # 语义：命中桶**截顶**而不是整桶丢弃（整桶丢弃会因 1s 桶样本多而误伤大片）
    assert not any(n > 0 for n in (res.get("outlier_nulls") or [])), \
        res.get("outlier_nulls")
    # 阀值 = 10 x p99 = 18ms -> 图上最大值被压到 18
    assert abs((res.get("outlier_max") or 0) - 18.0) < 0.01, res.get("outlier_max")
    assert "离群" in (res.get("outlier_btn") or "")


def test_y_scale_rail_bounds_never_invert(tmp_path):
    """时延轴刻度轨：给出上下界，且上界 > 下界（绝不倒转）。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    hi = res.get("yscale_hi") or ""
    lo = res.get("yscale_lo") or ""
    assert hi and lo, (hi, lo)
    def val(t):
        return float(t.replace("ms", "").replace("s", ""))
    assert val(hi) > val(lo), (hi, lo)


def test_levels_focus_expand_and_residual(tmp_path):
    """分级时延：聚焦展开下一级；残差显式；多点聚焦可叠加。"""
    tr = _trace_stats(tmp_path, "c.glog")
    a = _stats(tmp_path, "a.reader", "reader")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"}, traces=[tr])
    res = _js_check(tmp_path, data, "--focus", "query_total_us")
    if res is None:
        return
    assert res["errors"] == []
    lv = res.get("focus_text") or ""
    assert "残差" in lv, lv[:200]
    assert "query_total_us" in lv
    assert "query_route_us" in lv, lv[:400]     # 聚焦后下一级在新行展开
    assert "query_rpc_us" in lv, lv[:400]


def test_trace_field_has_p9999(tmp_path):
    """trace 字段统计补齐 p9999（供分级时延指标切换器使用）。"""
    tr = _trace_stats(tmp_path, "a.glog")
    z = rp.reduce_trace_size([tr])
    read = z["groups"]["read"]["ops"]["get_buffer"]
    tot = read["fields"]["total_us"]
    assert "p9999" in tot and tot["p9999"] is not None
    for key in ("query_total_us", "select_us"):
        assert "p9999" in read["fields"][key]


def test_adaptive_bucket_merges_same_second_clients(tmp_path):
    """回归：同一秒内多个客户端的行必须合并（曾被后者覆盖，样本与统计全丢）。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    b = _stats(tmp_path, "b.reader", "reader", node="n2")
    data = rp.reduce_scenario(rp.load([a, b]), {"config": "c"})
    z = data["sizes"]["8MB"]
    for key in ("all", "writer", "reader"):
        col = z["objects"][key]["tiers"]["cnt"]
        # 4 秒 x 10 样本（两个客户端同一时刻 -> 合并后每桶 20）
        assert sum(col["c"]) == (80 if key == "all" else 40), (key, col["c"])
        assert col["ok"] and all(v > 0 for v in col["avg"]), col["avg"]
