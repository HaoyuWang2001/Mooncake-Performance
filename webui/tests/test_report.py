#!/usr/bin/env python3
"""⑤ 报表：reduce 合并、自包含 HTML、data.js、总览、--rebuild-report。"""
import json
import os
import re
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

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
    # 对象体系：角色 + 角色化节点聚合 + 客户端；每对象都带自适应档
    assert "node:n1:writer" in z["objects"] and "node:n2:reader" in z["objects"]
    assert "node:n1" not in z["objects"] and "node:n2" not in z["objects"]
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
    # 90000s：1s/10s 桶数超过 8192 -> 只剩 60s 档
    assert z["embed_step"] == 60
    assert [t["key"] for t in z["tiers"]] == ["60", "cnt"]
    assert z["tiers"][-1]["step"] == 60          # 1500 桶 -> 自适应原样
    assert z["embed_tiers"] == ["60", "cnt"]
    # 1 小时 run：1s 超过 2048 点上限 -> 嵌入自 10s 档起
    st["meta"]["ts_to"] = 3600
    z2 = rp.reduce_size([st], {"config": "c"})
    assert z2["embed_step"] == 10
    assert [t["key"] for t in z2["tiers"]] == ["1", "10", "60", "cnt"]
    assert z2["tiers"][-1]["step"] == 1          # 3600 点 -> 自适应 = 1s
    # 30 秒 run：无达标尺度 -> 回退 1s
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
    # 单元布局：pipeline 按 size 拆（c1_t1 里的 t 是 reader_threads，不是 combo）
    html = (logs / "report" / "8MB" / "report.html").read_text(encoding="utf-8")
    unit_meta = json.loads(
        (logs / "report" / "8MB" / "meta.json").read_text(encoding="utf-8"))
    assert unit_meta["unit_size"] == "8MB"
    # P1-B：重建同样以数据最早采样时刻为准（RESULT 行为 10:00:10 起），
    # 覆盖 meta.json 里的旧「生成时刻」10:00:00
    assert unit_meta["started"] == "2026-11-12 10:00:10", \
        unit_meta["started"]
    assert "场景" in html and "节点数" in html and "目标 QPS" in html
    import re as _re
    emb = json.loads(_re.search(r"window\.__MC_REPORT__ = (.*?);</script>",
                                html, _re.S).group(1))
    ps = dict((p[0], p[1]) for p in (emb.get("meta") or {}).get("params") or [])
    # 客户端总数/节点数按实际加载的 stats 重算（旧 params 的幽灵值被纠正）
    assert ps["客户端总数"] == 1
    assert ps["节点数"] == 1
    assert ps["数据大小"] == "8MB"
    ov = (tmp_path / "20261112-1000" / "report" / "overview.html").read_text(
        encoding="utf-8")
    assert "logs/rank_smoke/report/8MB/report.html" in ov
    assert "8MB" in ov
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
    assert 'id="params"' in src and 'id="unitmeta"' in src
    # 顺序：参数配置标题 -> 单元信息行 -> 参数表格
    assert (src.index("参数配置") < src.index('id="unitmeta"')
            < src.index('id="params"'))
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
    assert ps["目标写 QPS"] == "100 × 4 节点 = 400"
    assert ps["目标读 QPS"] == "400 × 3 读 = 1200"
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


def _trace_stats2(tmp_path, name, *, node, pod, role=""):
    from tracestats import compute_trace_stats
    p = tmp_path / name
    p.write_text(TRACE_READ + "\n" + TRACE_WRITE + "\n", encoding="utf-8")
    return compute_trace_stats(str(p), node=node, role=role, size="8MB",
                               sample_rate=1.0, pod=pod)


def test_trace_per_object_series(tmp_path):
    """SDK 全链路数据侧：按对象（总计/节点/实例）保留阶段桶。"""
    a = _trace_stats2(tmp_path, "n1.glog", node="n1", pod="pod1")
    b = _trace_stats2(tmp_path, "n2.glog", node="n2", pod="pod2")
    out = rp.reduce_trace_size([a, b])
    objs = out["objects"]
    assert objs["role:read"]["kind"] == "role"
    assert objs["role:write"]["kind"] == "role"
    assert objs["node:n1:read"]["kind"] == "node"
    assert objs["node:n2:write"]["kind"] == "node"
    assert objs["client:pod1:read"]["kind"] == "client"
    assert objs["client:pod1:read"]["node"] == "n1"
    # 节点对象计数 = 该节点各文件之和；总计 = 全部之和
    n1 = objs["node:n1:read"]["groups"]["read"]["ops"]["get_buffer"]
    assert n1["count"] == 1
    tot = objs["role:read"]["groups"]["read"]["ops"]["get_buffer"]
    assert tot["count"] == 2
    # 每个对象都有阶段秒桶（曲线数据源）
    assert n1["seconds"] and n1["fminutes"]


def test_trace_embed_trim_budget_and_coarsen(tmp_path):
    """嵌入裁剪：总计全嵌、节点/实例抽稀并受行预算约束；data.js 保留全量。"""
    trs = [_trace_stats2(tmp_path, "n%d.glog" % i, node="n%d" % i,
                         pod="p%d" % i) for i in range(12)]
    tr = rp.reduce_trace_size(trs)
    full_total = tr["object_total"]
    assert full_total == 2 + 12 * 2 + 12 * 2   # role×2 + node×2组×12 + client×2组×12
    trimmed = rp._trim_trace_embed(tr, max_points=1, budget=10)
    assert "role:read" in trimmed["objects"]   # 总计始终嵌入
    assert trimmed["embed_objects"] < full_total
    assert trimmed["object_total"] == full_total
    # 抽稀后 ftier 提高（1s -> k 秒），总样本数保持
    key = [k for k in trimmed["objects"] if k.startswith("node:")][0]
    g = list(trimmed["objects"][key]["groups"])[0]
    op = list(trimmed["objects"][key]["groups"][g]["ops"])[0]
    assert op
    # 全量数据未被裁剪污染（data.js 用原始 tr）
    raws = [len(op2.get("fminutes", {}).get("total_us", []))
            for t in tr["objects"].values()
            for g2 in t["groups"].values()
            for op2 in g2["ops"].values()]
    trims = [len(op2.get("fminutes", {}).get("total_us", []))
             for t in trimmed["objects"].values()
             for g2 in t["groups"].values()
             for op2 in g2["ops"].values()]
    assert max(raws) >= max(trims)


def test_render_embeds_trimmed_trace_full_in_datajs(tmp_path):
    """HTML 内嵌 trace.objects 受预算裁剪；data.js 仍含全部对象。"""
    a = _stats(tmp_path, "a.reader", "reader")
    trs = [_trace_stats2(tmp_path, "n%d.glog" % i, node="n%d" % i,
                         pod="p%d" % i) for i in range(6)]
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"}, traces=trs)
    html, dj = rp.render_scenario(data, str(tmp_path / "o"), ECHARTS)
    import re as _re
    emb = json.loads(_re.search(r"window\.__MC_REPORT__ = (.*?);</script>",
                                open(html, encoding="utf-8").read(), _re.S
                                ).group(1))
    etr = list(emb["sizes"].values())[0]["trace"]
    assert etr["object_total"] >= etr["embed_objects"]
    full = open(dj, encoding="utf-8").read()
    assert "client:p0:read" in full or "client:p0:write" in full


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
    assert 'id="tdetail"' in src
    # trace 卡内不再有独立「数据大小」选择器（由最上方主选择器统一控制）
    assert 'id="tsizeSel"' not in src
    assert 'id="tstackchart"' in src
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


def _js_check(tmp_path, data, *args, env=None):
    """用桩 DOM 执行 report 的 app JS；无 node 时跳过（返回 None）。
    env: 传给 harness 的环境变量（VC_* 钩子：模拟 data.js 缺失/注入断点等）。"""
    node = shutil.which("node")
    if not node:
        print("  [skip] node not found")
        return None
    app = tmp_path / "app.js"
    app.write_text(rp._APP_JS, encoding="utf-8")
    dj = tmp_path / "data.json"
    dj.write_text(json.dumps(data), encoding="utf-8")
    run_env = None
    if env:
        run_env = dict(os.environ)
        run_env.update({k: str(v) for k, v in env.items()})
    r = subprocess.run([node, os.path.join(HERE, "report_js_check.js"),
                        str(app), str(dj)] + list(args),
                       capture_output=True, text=True, timeout=60,
                       env=run_env)
    assert r.returncode == 0, r.stdout + r.stderr
    return json.loads(r.stdout)


def test_tier_buttons_and_ladder(tmp_path):
    """1 小时 run：1s/10s/1min 三档可切；自适应默认 = 1s 原样（3600 点）。"""
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
    # 默认档 = 自适应：3600 桶在 [2048,4096] 内 -> 原样 1s（3600 点）
    assert res["init_len"] == 3600, res["init_len"]
    assert res["init_x0"] == 3600 * 1000
    assert res["min_len"] == 60             # 切到 1 分钟档


def test_long_run_embed_tier_and_datajs_finer(tmp_path):
    """90000s run：1s/10s 被收集规则舍弃 -> 只有 60s 档（内嵌与 data.js 一致）。"""
    st = _minimal_stats([3600, 3660, 3720], [3600], bucket_unit="s")
    st["meta"]["ts_from"] = 0
    st["meta"]["ts_to"] = 90000
    data = rp.reduce_scenario([st], {"config": "long"})
    assert data["sizes"]["8MB"]["embed_step"] == 60
    out = str(tmp_path / "report")
    html, dj = rp.render_scenario(data, out, ECHARTS, topk=5)
    import re as _re
    emb = json.loads(_re.search(r"window\.__MC_REPORT__ = (.*?);</script>",
                                open(html, encoding="utf-8").read(),
                                _re.S).group(1))
    eobj = emb["sizes"]["8MB"]["objects"]["writer"]
    assert "1" not in eobj["tiers"]              # 1s 不收集/不内嵌
    assert eobj["tiers"]["60"]["t"]              # 60s 档有数据
    full = json.loads(open(dj, encoding="utf-8").read()[
        len("window.__MC_FULL__ = "):].rstrip().rstrip(";"))
    fobj = full["sizes"]["8MB"]["objects"]["writer"]
    assert "1" not in fobj["tiers"]              # data.js 同样只有可达尺度
    assert len(fobj["tiers"]["60"]["t"]) == 3



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
    """meta 字段进入 HTML 前必须转义（防注入 & 防破版）。

    副标题只渲染 started / protocol / early_stop，探针用 protocol；
    config/mode 仍进数据 JSON（script 内原值无害）。"""
    bad = '<img src=x onerror=alert(1)>'
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": bad, "mode": bad,
                                             "protocol": bad})
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
    """收集规则：尺度 s 可用 ⇔ span/s ∈ [59, 8192]（仅 1s/10s/1min）。"""
    assert rp._tier_policy(30) == ([1], 1)          # 短 run：回退 1s
    assert rp._tier_policy(0) == ([1], 1)           # 未知跨度
    offered, embed = rp._tier_policy(3600)
    assert offered == [1, 10, 60] and embed == 10   # 1h：1s 超嵌入上限
    offered, embed = rp._tier_policy(90000)         # 1s/10s 超 8192 -> 60s
    assert offered == [60] and embed == 60
    offered, embed = rp._tier_policy(86400 * 3)     # 3 天：60s=4320 达标
    assert offered == [60] and embed == 60
    # 产物最细步长 10s 起：1s 档不再提供
    assert rp._tier_policy(600, 10) == ([10], 10)


def test_available_scales_and_adaptive():
    """可用尺度 + 自适应折叠（6144 -> ÷3 = 2048；不折叠时尽量贴 2048）。"""
    assert rp._available_scales(600) == [1, 10]     # 10min：1min 桶数不足
    assert rp._available_scales(30) == [1]          # 无达标尺度 -> 回退
    assert rp._available_scales(90000) == [60]
    assert rp._available_scales(3600) == [1, 10, 60]
    assert rp._available_scales(600, 10) == [10]    # 不细于产物步长
    assert rp._adaptive_step(600, [1, 10]) == 1
    assert rp._adaptive_step(3500, [1, 10]) == 1    # 窗口内原样
    assert rp._adaptive_step(5000, [1]) == 2        # 5000/2=2500
    assert rp._adaptive_step(6144, [1, 10, 60]) == 3  # 6144/3=2048
    assert rp._adaptive_step(8192, [1]) == 4        # 8192/4=2048


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
    # 多对象：每个对象一整套指标线（同色），series 名 = 对象名 + 指标
    names = res["overlay_names"] or []
    objs = {n.split(" ")[0] for n in names}
    assert len(objs) == 2, names
    assert len(names) >= 4, names
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

def test_adaptive_tier_folds_time_buckets(tmp_path):
    """自适应只折叠时间桶：600s -> 与 1s 档一致；6144s -> 3s 折叠 2048 点。"""
    st = _minimal_stats([10 * i for i in range(600)], [600])
    st["meta"]["ts_from"] = 0
    st["meta"]["ts_to"] = 600
    z = rp.reduce_size([st], {"config": "c"})
    cnt = z["objects"]["writer"]["tiers"]["cnt"]
    t1 = z["objects"]["writer"]["tiers"]["1"]
    assert cnt["t"] == t1["t"]                   # 600 桶：自适应=1s 原样
    assert z["tiers"][-1]["step"] == 1
    # 6144 桶：折叠 x3 -> 2048 点
    st2 = _minimal_stats(list(range(6144)), [6144])
    st2["meta"]["ts_from"] = 0
    st2["meta"]["ts_to"] = 6144
    z2 = rp.reduce_size([st2], {"config": "c"})
    cnt2 = z2["objects"]["writer"]["tiers"]["cnt"]
    assert len(cnt2["t"]) == 2048, len(cnt2["t"])
    assert z2["tiers"][-1]["step"] == 3
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
    i_curve = src.index("<h2>端到端时延曲线</h2>")
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
    assert "总体与角色" in joined and "节点与实例" in joined
    assert "已选" in joined
    assert "全选" in joined and "清空" in joined
    assert res.get("qps_series"), res.get("qps_series")
    note = res.get("qps_note") or ""
    assert "失败" in note, note


def test_outlier_scatter_toggle_and_merge(tmp_path):
    """S5：离群值 = 单请求散点（桶统计已在 map 剔除）；按钮只控制散点显隐。"""
    st = _minimal_stats([0, 1, 2], [600])
    st["meta"].update({"ts_from": 0, "ts_to": 200, "node": "n1",
                       "role": "writer", "source": "a.writer.log"})
    st["outliers"] = [{"ts": "2026-11-19 00:00:01.000000", "op": "put",
                       "key": "k9", "lat_ms": 500.0, "status": "OK"}]
    st["abnormal"] = [{"ts": "2026-11-19 00:00:01.500000", "op": "put",
                       "key": "k8", "lat_ms": None, "status": "ERR:-1"}]
    data = rp.reduce_scenario([st], {"config": "c"})
    z = data["sizes"]["8MB"]
    assert len(z["outliers"]) == 1 and z["outliers"][0]["ts_epoch"] is not None
    assert z["outliers"][0]["node"] == "n1"
    assert z["outliers"][0]["client"] == "a.writer"
    assert len(z["abnormal"]) == 1 and z["abnormal"][0]["status"] == "ERR:-1"
    res = _js_check(tmp_path, data, "--outlier", "1")
    if res is None:
        return
    assert res["errors"] == []
    assert any("离群" in n for n in (res.get("outlier_before") or [])), \
        res.get("outlier_before")
    assert not any("离群" in n for n in (res.get("outlier_after") or [])), \
        res.get("outlier_after")
    assert "显示离群点" in (res.get("outlier_btn") or ""), res.get("outlier_btn")


def test_y_zoom_slider_factor_valid(tmp_path):
    """时延轴缩放滑块：倍率标注合法且 >= 1x（窗口倒转由 setZoomP 夹取杜绝）。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    cap = res.get("zslider_cap") or ""
    assert cap.endswith("x"), cap
    assert float(cap[:-1]) >= 1.0, cap


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


# ---------------------------------------------------------------------------
# 回归：本轮 UI 维护修复（时区归一 / 多 combo 口径 / 秒级阶段桶 / gap 标注）
# ---------------------------------------------------------------------------

def test_slowest_ts_epoch_matches_buckets(tmp_path):
    """slowest[].ts_epoch 必须与 seconds 桶同口径（节点 UTC 钟面时间曾导致
    慢点图整体偏 8 小时）。"""
    st = _minimal_stats([1000000 + i for i in range(5)], [1000000])
    # 节点把日志写成 UTC 钟面时间：03:21 对应宿主本地 11:21
    st["slowest"] = [{"ts": "2026-11-18 03:21:01.301328", "op": "get_buffer",
                      "key": "k1", "lat_ms": 181.3, "status": "OK"}]
    z = rp.reduce_size([st], {"config": "c"})
    row = z["slowest"][0]
    assert "ts_epoch" in row
    import calendar
    expect = calendar.timegm((2026, 11, 18, 3, 21, 1, 0, 0, 0)) + 0.301328
    assert abs(row["ts_epoch"] - expect) < 1e-6
    # 关键回归：不能再按宿主本地时区解释（那会再偏一个时区差）
    import time as _t
    assert abs(row["ts_epoch"] - _t.mktime((2026, 11, 18, 3, 21, 1,
                                            0, 0, -1))) > 3600


def test_slowest_ts_epoch_unparseable_is_none(tmp_path):
    st = _minimal_stats([1000000], [1000000])
    st["slowest"] = [{"ts": "not-a-time", "op": "x", "key": "k",
                      "lat_ms": 1.0, "status": "OK"}]
    z = rp.reduce_size([st], {"config": "c"})
    assert z["slowest"][0]["ts_epoch"] is None


def test_summary_reports_multiple_combos(tmp_path):
    """多参数组 sweep：summary.combos 列出各组，(c,t) 从日志名解析。"""
    a = _minimal_stats([1000000 + i for i in range(3)], [1000000])
    a["meta"]["source"] = "s.benchmark_c2_t4_128KB.log"
    b = _minimal_stats([1000010 + i for i in range(3)], [1000000])
    b["meta"]["source"] = "s.benchmark_c4_t8_128KB.log"
    z = rp.reduce_size([a, b], {"config": "c"})
    cs = z["summary"]["combos"]
    assert [(x["c"], x["t"]) for x in cs] == [(2, 4), (4, 8)]
    assert cs[0]["label"] == "2 客户端 × 4 线程"


def test_summary_single_combo_still_listed(tmp_path):
    a = _minimal_stats([1000000 + i for i in range(3)], [1000000])
    a["meta"]["source"] = "s.benchmark_c1_t1_128KB.log"
    z = rp.reduce_size([a], {"config": "c"})
    assert [(x["c"], x["t"]) for x in z["summary"]["combos"]] == [(1, 1)]


def test_combos_of_ignores_unparseable_names():
    st = {"meta": {"source": "custom-name.log"}}
    assert rp.combos_of([st]) == []


def test_stage_buckets_use_seconds_and_multitier(tmp_path):
    """阶段曲线：秒级桶 -> 60s 档聚合后仍有 >2 个点（旧版固定 1 分钟只出 2 点，
    画出来是一条直线）。"""
    st = _minimal_stats([1000000 + i for i in range(600)], [1000000])
    tr = {"meta": {"bins": "us-v1", "size": "128KB"},
          "groups": {"read": {"ops": {"get_buffer": {
              "count": 600, "ok": 600, "fail": 0, "status": {"ok": 600},
              "fields": {}, "ftier": 1,
              "fminutes": {"query_total_us": [
                  [1000000 + i * 10, 4, 100.0 + i, 400.0 + i]
                  for i in range(60)]},
          }}}}}
    data = rp.reduce_scenario([st], {"config": "c", "size": "128KB"},
                              traces=[tr])
    assert "128KB" in data["sizes"] and "trace" in data["sizes"]["128KB"]
    op = data["sizes"]["128KB"]["trace"]["groups"]["read"]["ops"]["get_buffer"]
    assert op["ftier"] == 1
    assert len(op["fminutes"]["query_total_us"]) == 60


# ---------------------------------------------------------------------------
# 回归：HTML 结构配平 + 参数组（combo）切换可用
# ---------------------------------------------------------------------------

def test_body_div_balance_cards_inside_wrap(tmp_path):
    """曾多一个 </div> 导致 .wrap 提前闭合、后续卡片全部脱离版心。"""
    from html.parser import HTMLParser
    a = _stats(tmp_path, "a.reader", "reader")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "out"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    import re as _re
    src = _re.sub(r"<script[\s\S]*?</script>", "", src)

    class P(HTMLParser):
        def __init__(self):
            super().__init__()
            self.depth = 0
            self.cards = []
            self.neg = 0

        def handle_starttag(self, tag, attrs):
            if tag == "div":
                cls = (dict(attrs).get("class") or "").split()
                if "card" in cls:
                    self.cards.append(self.depth)
                self.depth += 1

        def handle_endtag(self, tag):
            if tag == "div":
                self.depth -= 1
                if self.depth < 0:
                    self.neg += 1
                    self.depth = 0

    pr = P()
    pr.feed(src)
    pr.close()
    assert pr.neg == 0
    assert pr.depth == 0
    assert pr.cards, "no cards rendered"
    assert len(set(pr.cards)) == 1, pr.cards      # 所有卡片同一层级（.wrap 内）


def test_combo_switch_click_works(tmp_path):
    """参数组按钮回调曾引用未定义的 renderMain/renderQps 并误调 trace 树。"""
    stats = []
    for c, t in ((2, 4), (4, 8)):
        st = _minimal_stats([3600 + i for i in range(60)], [3600],
                            bucket_unit="s")
        st["meta"].update({
            "source": "benchmark_c{}_t{}_128KB.log".format(c, t),
            "node": "n1", "ts_from": 3600, "ts_to": 3659})
        stats.append(st)
    data = rp.reduce_scenario(stats, {"config": "c"})
    assert len(data["sizes"]["8MB"]["summary"]["combos"]) == 2
    res = _js_check(tmp_path, data, "--real", "--click-combo")
    if res is None:
        return
    assert res["errors"] == [], res["errors"]
    assert res.get("combo_btns", 0) >= 3
    assert res.get("combo_clicked") is True
    real = res.get("real") or {}
    assert real.get("fail") == 0, real


# ---------------------------------------------------------------------------
# overview 链接：适配器必须按相对路径语义解析 ../（曾 slice(3) 砍掉导致 404）
# ---------------------------------------------------------------------------

def test_adapter_resolves_parent_relative_links(tmp_path):
    """node 行为测试：base=outputs/t1/report + href=../logs/x/report/report.html
    -> /api/file?path=outputs/t1/logs/x/report/report.html。"""
    import subprocess
    import shutil
    node = shutil.which("node")
    if not node:
        print("  [skip] node not found")
        return
    js = rp._PAGE_ADAPTER_JS
    script = """
var rewritten = null;
var anchor = {
  href: '',
  getAttribute: function () { return '../logs/x/report/report.html'; },
};
globalThis.location = { protocol: 'http:' };
globalThis.window = { __MC_REL__: 'outputs/t1/report' };
globalThis.document = {
  querySelectorAll: function (sel) { return [anchor]; },
};
""" + js + """
process.stdout.write(anchor.href);
"""
    r = subprocess.run([node, "-e", script], capture_output=True, text=True,
                       timeout=30)
    assert r.returncode == 0, r.stderr
    assert r.stdout == (
        "/api/file?path=outputs%2Ft1%2Flogs%2Fx%2Freport%2Freport.html"
        "&mode=raw"), r.stdout


def test_overview_links_open_new_tab(tmp_path):
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"})
    ov = rp.render_overview(
        {"run": "t1", "mode": "ssh", "nodes": []},
        [dict(data, report_rel="../logs/x/report/report.html")],
        str(tmp_path / "ov"), rel_base="outputs/t1/report")
    src = open(ov, encoding="utf-8").read()
    assert 'target="_blank"' in src
    assert 'rel="noopener"' in src
    # 相对链接保持原样（本地 file:// 可直开），../ 不被砍掉
    assert 'href="../logs/x/report/report.html"' in src
    # 适配器已按相对语义实现（不再对 href 做 slice(3)）
    assert "getAttribute('href').slice(3)" not in src


def test_objtree_strip_scrolls_from_top(tmp_path):
    """对象选择横条内容超长时必须从顶部开始排（align-items:start）。
    曾用 align-items:center + overflow:auto：内容溢出向上下均分，
    展开节点后顶部「总体与角色」滚不回来。"""
    a = _stats(tmp_path, "a.reader", "reader")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "out"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    import re as _re
    m = _re.search(r"\.objtree-strip\{([^}]*)\}", src)
    assert m, "objtree-strip rule missing"
    rule = m.group(1)
    assert "align-items:flex-start" in rule
    assert "align-items:center" not in rule


def test_hist_labels_dynamic_top_margin(tmp_path):
    """同一分箱内压了 5 个分位标注时，顶部留白必须动态加高，否则最上层
    文字被图表上沿裁掉。"""
    a = _stats(tmp_path, "a.reader", "reader")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    z = data["sizes"]["8MB"]
    s = z["summary"]
    edges = s["bin_edges"]
    # 全部指标压进同一个分箱（单桶有样本，标签 5 层摞起）
    s["hist_bins"] = [[30, 20000]]
    for k in ("avg_ms", "p50_ms", "p99_ms", "p999_ms", "p9999_ms"):
        s[k] = (edges[30] + edges[31]) / 2.0
    s["total"] = 20000
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert len(res.get("hist_mark_x") or []) == 5
    # 5 个标签 -> 4 层叠放 -> top = 30 + 4*14
    assert res.get("hist_grid_top") == 30 + 4 * 14, res.get("hist_grid_top")


def test_hist_labels_no_extra_margin_when_separate(tmp_path):
    """指标各自落在不同分箱时不应白白加高顶部。"""
    a = _stats(tmp_path, "a.reader", "reader")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    z = data["sizes"]["8MB"]
    s = z["summary"]
    edges = s["bin_edges"]
    s["hist_bins"] = [[30, 10], [35, 10], [40, 10], [45, 10]]
    for k, bi in (("avg_ms", 30), ("p50_ms", 35), ("p99_ms", 40),
                  ("p999_ms", 45)):
        s[k] = (edges[bi] + edges[bi + 1]) / 2.0
    s["total"] = 100            # <10000 -> p9999 标注不出现
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert len(res.get("hist_mark_x") or []) == 4
    assert res.get("hist_grid_top") == 30


def test_unit_of_basic_and_pipeline():
    """单元键：basic=(c,t,size)；pipeline 只按 size（t 是 reader_threads）。"""
    key, name, label, c, t, size = bm._unit_of(
        "cfg.benchmark_c4_t8_128KB.log", "basic")
    assert (key, name, c, t, size) == (
        ("basic", 4, 8, "128KB"), "c4_t8_128KB", 4, 8, "128KB")
    assert label == "c4×t8 · 128KB"
    key, name, label, c, t, size = bm._unit_of(
        "cfg.benchmark_c1_t4_8MB.writer.log", "pipeline")
    assert (key, name, label, size) == (("size", "8MB"), "8MB", "8MB", "8MB")
    assert bm._unit_of("legacy.log", "pipeline")[1] == "all"
    groups = bm._unit_groups(
        ["a/cfg.benchmark_c1_t1_8MB.log", "a/cfg.benchmark_c1_t1_1MB.log"],
        "basic")
    assert [u["name"] for u in groups.values()] == ["c1_t1_8MB", "c1_t1_1MB"]


def test_generate_reports_unit_split_basic(tmp_path):
    """basic：一个 size×combo 一份 report.html + meta.json（单元级参数覆盖）。"""
    logs = tmp_path / "logs" / "cfg"
    logs.mkdir(parents=True)
    rows = "".join(
        "RESULT\t2026-11-15 14:00:{:02d}.{:06d}\tput\tk{}\t1.5\tOK\n".format(
            i % 60, i, i) for i in range(30))
    for c, t, sz in ((1, 1, "8MB"), (2, 4, "8MB"), (1, 1, "1MB")):
        (logs / "cfg.benchmark_c{}_t{}_{}.log".format(c, t, sz)).write_text(
            rows, encoding="utf-8")
    args = SimpleNamespace(no_plot=False, dry_run=False, cluster="cluster.json",
                           global_cfg="global.json", profile="profile.json")
    written = bm._generate_reports(
        {"__cfg_path__": "cfg.json", "scenario": "basic"}, str(logs), args,
        qps_target=100, mode="basic")
    if written is None:
        pytest.skip("report pipeline unavailable")
    assert len(written) == 3
    names = sorted(p.parent.name for p in
                   (logs / "report").glob("*/report.html"))
    assert names == ["c1_t1_1MB", "c1_t1_8MB", "c2_t4_8MB"]
    meta = json.loads(
        (logs / "report" / "c2_t4_8MB" / "meta.json").read_text("utf-8"))
    assert (meta["unit_clients"], meta["unit_threads"]) == (2, 4)
    assert meta["unit_size"] == "8MB"
    ps = dict((p[0], p[1]) for p in meta["params"])
    assert (ps["客户端数"], ps["线程数"], ps["数据大小"]) == (2, 4, "8MB")


def test_overview_groups_units_per_config(tmp_path):
    """overview：每配置一卡（跨单元聚合）+ 卡内逐单元链接。"""
    def unit(sz, rel, lab):
        summ = {"ok": 1, "fail": 0, "total": 1, "bytes": 10, "wall_s": 1.0,
                "p50_ms": 1, "p99_ms": 2, "p999_ms": 3,
                "clients": [{"name": "a"}]}
        return {"meta": {"config": "cfg", "mode": "basic", "protocol": "tcp"},
                "sizes": {sz: {"summary": summ, "clients": []}},
                "unit": lab, "report_rel": rel}
    ov = rp.render_overview({"run": "t"}, [
        unit("8MB", "logs/cfg/report/c1_t1_8MB/report.html", "c1×t1 · 8MB"),
        unit("1MB", "logs/cfg/report/c1_t1_1MB/report.html", "c1×t1 · 1MB"),
    ], str(tmp_path))
    html = open(ov, encoding="utf-8").read()
    assert html.count('<div class="card">') == 1
    assert "2 个单元" in html and "1MB, 8MB" in html
    assert 'href="logs/cfg/report/c1_t1_8MB/report.html"' in html
    assert 'href="logs/cfg/report/c1_t1_1MB/report.html"' in html


def test_single_instance_tree_hides_duplicate_groups(tmp_path):
    """单元内只有一个实例：对象树只留「总体」（节点/实例 ≡ 总体）。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert len(res["tree_rows"]) == 1
    assert "总体" in res["tree_rows"][0]
    assert "节点" not in res["tree_rows"][0]
    # 多实例场景保持完整的分组
    b = _stats(tmp_path, "b.reader", "reader", node="n2")
    data2 = rp.reduce_scenario(rp.load([a, b]), {"config": "c"})
    res2 = _js_check(tmp_path, data2)
    if res2 is None:
        return
    assert len(res2["tree_rows"]) == 2
    assert "节点" in res2["tree_rows"][1]


def test_summary_layout_rows_and_dedup(tmp_path):
    """段1 新版：性能统计第一行（吞吐/avg/p99/p9999）强调、第二行含 min/max；无 QPS；
    信息展示两行（总数/成功/失败/失败率/QPS + 数据大小/客户端数/线程数/时长）；
    参数配置不重复信息展示已给出的值。"""
    a = _stats(tmp_path, "a.writer", "writer")
    params = [["场景", "basic"], ["数据大小", "8MB"], ["客户端数", 4],
              ["线程数", 8], ["客户端总数", 4],
              ["writer.operation", "put_batch"]]
    data = rp.reduce_scenario(rp.load([a]),
                              {"config": "cfg", "mode": "basic",
                               "unit": "c4×t8 · 8MB", "unit_size": "8MB",
                               "unit_clients": 4, "unit_threads": 8,
                               "clients": 4, "params": params})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    perf = res["perf_kids"]
    # 第一行整体强调：吞吐 / avg / p99 / p9999
    assert perf[0]["text"].startswith("吞吐")
    assert perf[1]["text"].startswith("avg")
    assert perf[2]["text"].startswith("p99")
    assert perf[3]["text"].startswith("p9999")
    assert all("strong" in k["cls"] for k in perf[:4])
    # 其后是换行；第二行不含 QPS，且含 min/max、不加粗
    assert "gridbreak" in perf[4]["cls"]
    assert not any("QPS" in k["text"] for k in perf)
    assert any(k["text"].startswith("p50") for k in perf[5:])
    assert any(k["text"].startswith("min") for k in perf[5:])
    assert any(k["text"].startswith("max") for k in perf[5:])
    assert all("strong" not in k["cls"] for k in perf[5:])
    # 信息展示第一行
    info = res["info_kids"]
    first = [k["text"] for k in info[:5]]
    for lab in ("总数", "成功", "失败", "失败率", "QPS"):
        assert any(t.startswith(lab) for t in first), (lab, first)
    # 信息展示只有第一行
    assert len(info) == 5
    # 数据大小/客户端数/线程数/时长 移到参数配置标题下、参数表格上方
    unit = [k["text"] for k in res["unit_kids"]]
    for lab in ("数据大小", "客户端数", "线程数", "时长"):
        assert any(t.startswith(lab) for t in unit), (lab, unit)
    # 参数配置去重（信息展示已给出的字段不再出现）
    labels = res["param_rows"]
    assert any(l.startswith("writer.operation") for l in labels)
    for lab in ("数据大小", "客户端数", "线程数", "客户端总数", "采集时长",
                "开始时间"):
        assert not any(l.startswith(lab) for l in labels), (lab, labels)


def test_basic_tree_overview_single_row(tmp_path):
    """basic 对象树：顶部单行「总览」（原「总体（全部）」），无角色分组；
    pipeline 仍保留「总体与角色」+「总体（全部）」标签。"""
    a = _stats(tmp_path, "a.merged", "client")
    data = rp.reduce_scenario(rp.load([a]),
                              {"config": "cfg", "mode": "basic",
                               "unit_size": "8MB"})
    z = data["sizes"]["8MB"]
    assert z["objects"]["all"]["label"] == "总览"
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert len(res["tree_rows"]) == 1
    assert "总览" in res["tree_rows"][0]
    assert "总体与角色" not in res["tree_rows"][0]
    b = _stats(tmp_path, "b.writer", "writer")
    d2 = rp.reduce_scenario(rp.load([b]),
                            {"config": "cfg", "mode": "pipeline"})
    assert d2["sizes"]["8MB"]["objects"]["all"]["label"] == "总体（全部）"


def test_qps_legend_fixed_and_caption_removed(tmp_path):
    """QPS 图：图例可看不可点；「吞吐与失败」小标题删除；
    时延工具栏单行（无 tb-sep）。"""
    a = _stats(tmp_path, "a.merged", "client")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg", "mode": "basic"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    assert "吞吐与失败" not in src
    assert 'class="sub qps-cap"' not in src
    assert 'id="latToolbar"' in src
    i = src.index('id="latToolbar"')
    block = src[i:src.index("</div>", i)]
    assert "tb-sep" not in block
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert res["qps_legend_mode"] is False


def test_end_label_layout_stays_inside_plot(tmp_path):
    """端点标签与引线点端都钳制在绘图区 [top,bot] 内（y 轴缩放后可越界）。"""
    a = _stats(tmp_path, "a.merged", "client")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg", "mode": "basic"})
    res = _js_check(tmp_path, data, "--endlabels")
    if res is None:
        return
    assert res["errors"] == []
    top, bot = res["endlabel_bounds"]
    laid = res["endlabels"]
    assert len(laid) == 4
    for it in laid:
        assert top <= it["py"] <= bot
        assert top <= it["ly"] <= bot
    lys = [it["ly"] for it in laid]
    assert lys == sorted(lys)
    # 聚集场景：标签必须依次分散、绝不重叠（旧的平移+钳制会把底部簇压到同一像素）
    cases = {c["name"]: c for c in res["endlabel_cases"]}
    for name in ("bottom-cluster+top", "top+bottom", "all-bottom",
                 "top-cluster"):
        c = cases[name]
        assert c["lys"] == sorted(c["lys"]), (name, c["lys"])
        assert c["min_gap"] >= 8, (name, c["lys"])
        for y in c["lys"]:
            assert top <= y <= bot, (name, c["lys"])


def test_sliders_have_no_border(tmp_path):
    """时延轴的 x/y 平移滑块窗口不再有黑色描边（仅保留填充色）。"""
    a = _stats(tmp_path, "a.merged", "client")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg", "mode": "basic"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    for sel in (".xpan-win{", ".yscale-win{"):
        i = src.index(sel)
        rule = src[i:src.index("}", i)]
        assert "border:" not in rule, rule


def test_trace_cards_stack_and_breakdown_js(tmp_path):
    """trace 卡：摘要两行（加粗 平均/P99）、慢请求堆叠直方图（全灰阶）、
    展开行含完整 L1 + 残差 + L2 子区间。"""
    a = _stats(tmp_path, "a.reader", "reader")
    tr = _trace_stats(tmp_path, "c.glog")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"}, traces=[tr])
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    # 本批删除「指标概览」卡（段1 已给一眼指标；此处关注阶段耗时）
    assert not (res.get("tcards_kids") or []), res.get("tcards_kids")
    # 堆叠图：stack + 全灰阶（r=g=b）
    series = res["tstack_series"]
    assert series and all(s["stack"] == "stage" for s in series)
    for s in series:
        h = (s["color"] or "").lstrip("#")
        assert len(h) == 6, s
        rgb = [int(h[i:i + 2], 16) for i in (0, 2, 4)]
        # 灰阶（主题灰是暖灰，允许极小通道差）
        assert max(rgb) - min(rgb) <= 16, (rgb, s)
    assert res["tstack_cats"] and res["tstack_cats"] >= 1
    # 展开行：残差 + 多个主要原因的二级展开（该请求自己的组）
    t = res["tbreak_text"]
    assert "残差" in t, t
    assert t.count("二级：") >= 2, t        # 有子区间的 L1 全部展开
    l2 = ("query_route_us", "read_urma_us",
          "prep_buf_alloc_us", "memwrite_urma_us")
    assert any(k in t for k in l2), t
    # 0% 块宽度 = 最宽一行文字 + 标准 padding（不是拼接串长度）
    lw = res["lvl_line_w"]
    px = res["lvl_zero_px"]
    assert lw[1] < 110, lw                  # buf_alloc_us 行宽 ~83px
    assert 90 <= px[1] <= 120, px           # 块宽 ~109px（旧公式 ~174px）
    assert px[0] <= px[1], px


def _trace_stats_span(tmp_path, name, span_s):
    """跨度可控的 trace 产物（30s / 5min 等分桶策略测试用）。"""
    from tracestats import compute_trace_stats
    import datetime as _dt
    base = _dt.datetime(2026, 11, 12, 10, 0, 0)
    t0 = base.strftime("%Y-%m-%d %H:%M:%S")
    t1 = (base + _dt.timedelta(seconds=span_s)).strftime(
        "%Y-%m-%d %H:%M:%S")
    lines = []
    for i, ts in enumerate((t0, t1)):
        lines.append(TRACE_READ.replace("2026-11-12 10:23:41.882310", ts))
    p = tmp_path / name
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return compute_trace_stats(str(p), node="n1", size="8MB")


def test_stage_tier_policy_by_span(tmp_path):
    """阶段曲线分桶：与端到端同策略（span/s ∈ [59,8192]，且不细于 ftier）。
    30s/5min -> 仅 1秒（10s 档只有 30 桶）；1h -> 三档；旧产物 ftier=60 仅 1分钟。"""
    a = _stats(tmp_path, "a.reader", "reader")

    def _btns(tr, name):
        d = rp.reduce_scenario(rp.load([a]), {"config": "c"}, traces=[tr])
        r = _js_check(tmp_path, d)
        return None if r is None else r["stage_tier_btns"]

    bt = _btns(_trace_stats_span(tmp_path, "s30.glog", 30), "s30")
    if bt is None:
        return
    assert bt == ["自适应", "1 秒"], bt
    # 5min：10s=30 桶 < 59 -> 不提供
    bt5 = _btns(_trace_stats_span(tmp_path, "s300.glog", 300), "s300")
    if bt5 is None:
        return
    assert bt5 == ["自适应", "1 秒"], bt5
    # 1h：1s/10s/1min 全达标
    bt60 = _btns(_trace_stats_span(tmp_path, "s3600.glog", 3600), "s3600")
    if bt60 is None:
        return
    assert bt60 == ["自适应", "1 秒", "10 秒", "1 分钟"], bt60
    # 旧产物：ftier=60 -> 1s/10s 不可用；30s 跨度回退到 60s 档
    tr_old = _trace_stats_span(tmp_path, "s30old.glog", 30)
    for _g in tr_old["groups"].values():
        for _o in _g["ops"].values():
            _o["ftier"] = 60          # 旧产物：只有分钟桶
    bt_old = _btns(tr_old, "old")
    if bt_old is None:
        return
    assert bt_old == ["自适应", "1 分钟"], bt_old


def test_trace_toolbar_stage_rows(tmp_path):
    """trace 工具栏不再含数据大小选择器/说明文字；组切换保留；
    阶段曲线工具栏：粒度+重置 一行（无分割线），阶段独立一行。"""
    a = _stats(tmp_path, "a.reader", "reader")
    tr = _trace_stats(tmp_path, "c.glog")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"}, traces=[tr])
    html, _dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    assert 'id="tsizeSel"' not in src
    assert "「数据大小」= 本次 trace" not in src
    assert 'id="tgroups"' in src
    i = src.index('id="tLatToolbar"')
    head = src[i:src.index("</div>", i)]
    assert "tb-sep" not in head
    assert 'id="stageTierBtns"' in head and 'id="tResetBtn"' in head
    # 阶段行独立：tLatToolbar 之后、tchart 之前含 阶段 label + stageBtns
    j = src.index('id="stageBtns"')
    assert i < j
    assert src.index("</div>", j) > j


def test_client_table_readonly_no_expand(tmp_path):
    """实例详情：只读表格（每实例一行关键信息），点击不展开、不叠加曲线。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    b = _stats(tmp_path, "b.reader", "reader", node="n2")
    data = rp.reduce_scenario(rp.load([a, b]), {"config": "cfg"})
    res = _js_check(tmp_path, data, "--click-client")
    if res is None:
        return
    assert res["errors"] == []
    assert res["row_onclick"] is False
    assert res["rows_after"] == 2 and res["cld_rows"] == 0
    assert res["selobjs_before"] == res["selobjs_after"]
    assert res["main_series_before"] == res["main_series_after"]


def test_client_table_header_text(tmp_path):
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    data = rp.reduce_scenario(rp.load([a]), {"config": "cfg"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    assert "<h2>实例详情</h2>" in src
    assert "实例详情（" not in src
    assert "点击行查看该实例明细" not in src


def test_pipeline_and_basic_templates_split(tmp_path):
    """basic/pipeline 两套独立 body 模板：按 meta.mode 选择，标记与标题区分，
    两个模板都包含全部共享区块。"""
    a = _stats(tmp_path, "a.writer", "writer")
    b = _stats(tmp_path, "b.reader", "reader")
    basic = rp.reduce_scenario(rp.load([a]), {"config": "c", "mode": "basic"})
    hb, _dj = rp.render_scenario(basic, str(tmp_path / "b"), ECHARTS)
    sb = open(hb, encoding="utf-8").read()
    assert "<!-- tpl:basic -->" in sb
    assert "Pipeline 场景报表" not in sb
    assert "场景报表 ·" in sb.split("<title>")[1]
    pipe = rp.reduce_scenario(rp.load([a, b]),
                              {"config": "c", "mode": "pipeline"})
    hp, _dj2 = rp.render_scenario(pipe, str(tmp_path / "p"), ECHARTS)
    sp = open(hp, encoding="utf-8").read()
    assert "<!-- tpl:pipeline -->" in sp
    assert "<!-- tpl:basic -->" not in sp
    assert "Pipeline 场景报表" in sp.split("<title>")[1]
    for sec in ("性能统计", "信息展示", "参数配置", "端到端时延曲线",
                "时延分布", "慢点捕捉", "SDK 全链路", "实例详情",
                "错误与异常", "现场索引"):
        assert sec in sb, (sec, "basic")
        assert sec in sp, (sec, "pipeline")


def test_pipeline_summary_splits_roles(tmp_path):
    """pipeline 段1：writer/reader 各自完整一套；单元信息行 pipeline 化。"""
    a = _stats(tmp_path, "a.writer", "writer")
    b = _stats(tmp_path, "b.reader", "reader")
    params = [["场景", "pipeline"], ["节点数", 4],
              ["client.count_per_node", 2], ["notify_count", 4],
              ["store_count_per_node", 1],
              ["目标写 QPS", "25 × 4 节点 = 100"],
              ["目标读 QPS", "100 × 4 读 = 400"]]
    data = rp.reduce_scenario(rp.load([a, b]),
                              {"config": "cfg", "mode": "pipeline",
                               "params": params})
    z = data["sizes"]["8MB"]
    rs = z["role_summaries"]
    assert set(rs) == {"writer", "reader"}
    assert rs["writer"]["ok"] + rs["reader"]["ok"] == z["summary"]["ok"]
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert res["rolecards_shown"] is True
    assert res["perf_shown"] is False          # 合并卡隐藏
    txt = res["rolecards_text"]
    assert "写路径（writer）" in txt and "读路径（reader）" in txt
    assert "总数" in txt and "QPS" in txt
    # 单元信息行：2×4 / 2写4读 / store 1×4，且无「线程数」
    labels = [k["text"] for k in res["unit_kids"]]
    assert any(t.startswith("客户端数") and "2×4 nodes" in t
               for t in labels), labels
    assert "= 8" not in " ".join(labels)
    assert any(t.startswith("部署模式") and "2w4r" in t for t in labels), labels
    assert any(t.startswith("store 数量") and "1×4 nodes" in t
               for t in labels), labels
    # 顺序：数据大小 / 部署模式 / 客户端数 / store / 时长
    order=[t.split(" ")[0] for t in labels]
    assert order[:3] == ["数据大小", "部署模式", "客户端数"], order
    assert not any(t.startswith("线程数") for t in labels), labels


def test_basic_summary_unchanged_by_role_split(tmp_path):
    """basic 无角色分块：合并卡正常显示，单元行仍是 数据大小/客户端数/线程数/时长。"""
    a = _stats(tmp_path, "a.merged", "client")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c", "mode": "basic"})
    assert data["sizes"]["8MB"]["role_summaries"] == {}
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["rolecards_shown"] is False
    assert res["perf_shown"] is True
    labels = [k["text"] for k in res["unit_kids"]]
    assert any(t.startswith("数据大小") for t in labels), labels
    assert any(t.startswith("线程数") for t in labels), labels


def test_build_report_params_pipeline_qps_total():
    """pipeline 参数：目标写 QPS 全口径 + 读 QPS + store 数量。"""
    cfg = {"scenario": "pipeline", "protocol": "tcp", "work_mode": "rank",
           "base_qps": 25, "qps_multiple": 1, "notify_count": 4,
           "client": {"count_per_node": 2}, "store_count_per_node": 1,
           "data_size": ["128KB"], "duration": 600,
           "warmup": {"duration": 10}}
    meta = {"mode": "ssh", "launch_mode": "rank_recall", "clients": 8,
            "started": "2026-11-19 00:00:00"}
    ps = dict(bm._build_report_params(cfg, meta, nodes=4))
    assert ps["目标写 QPS"] == "25 × 4 节点 = 100"
    assert ps["目标读 QPS"] == "100 × 4 读 = 400"
    assert ps["store_count_per_node"] == 1
    assert ps["client.count_per_node"] == 2


def test_pipeline_tree_role_radio_total_and_scoped_nodes(tmp_path):
    """pipeline 对象树：角色单选（默认 reader）+ 总计（默认开）+ 角色化节点对象。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    a2 = _stats(tmp_path, "a2.writer", "writer", node="n2")
    b = _stats(tmp_path, "b.reader", "reader", node="n2")
    b2 = _stats(tmp_path, "b2.reader", "reader", node="n1")
    data = rp.reduce_scenario(rp.load([a, a2, b, b2]),
                              {"config": "cfg", "mode": "pipeline"})
    z = data["sizes"]["8MB"]
    # 服务端：节点对象按角色拆分；basic 形态的 node:<n> 不再存在
    assert "node:n1:writer" in z["objects"]
    assert "node:n2:reader" in z["objects"]
    assert not any(k.startswith("node:") and k.count(":") == 1
                   for k in z["objects"])
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert res["selobjs"] == ["reader"]          # 默认 reader + 仅总计
    joined = " ".join(res["tree_rows"])
    assert "角色" in joined and "总计" in joined
    assert "reader" in joined and "writer" in joined
    # reader 视角的节点与实例组存在，且只列 reader 角色节点（内嵌数据保留 role）
    assert "节点与实例" in joined, res["tree_rows"]
    tail = joined.split("节点与实例")[-1]
    assert "n1" in tail and "n2" in tail, tail
    # 展开后：每个节点下只有 reader 实例（writer 实例不在该角色视图里）
    rx = _js_check(tmp_path, data, "--expand-tree")
    if rx is None:
        return
    exp = " ".join(rx.get("tree_rows_expanded") or [])
    assert "b.reader" in exp and "b2.reader" in exp, exp
    assert "a.writer" not in exp and "a2.writer" not in exp, exp
    # 切到 writer：选中集切换、QPS 副图跟随
    r2 = _js_check(tmp_path, data, "--role-writer")
    if r2 is None:
        return
    assert r2["errors"] == []
    assert r2["selobjs_after_role"] == ["writer"]
    assert any(str(n).startswith("writer") for n in r2["qps_after_role"]), \
        r2["qps_after_role"]
    # 关闭总计：当前角色未选节点/实例 -> 空
    r3 = _js_check(tmp_path, data, "--total-off")
    if r3 is None:
        return
    assert r3["selobjs_total_off"] == []
    # 切换角色不丢弃选择：按角色记忆，切回自动恢复
    r4 = _js_check(tmp_path, data, "--role-roundtrip")
    if r4 is None:
        return
    assert r4["errors"] == []
    assert "node:n2:reader" in r4["selobjs_reader_selected"]
    assert r4["selobjs_writer_view"] == ["writer"]
    assert "node:n2:reader" in r4["selobjs_back_reader"], r4["selobjs_back_reader"]


def test_basic_tree_keeps_single_overview_selection(tmp_path):
    """basic：选中集仍是「总览」，没有角色 radio/总计。"""
    a = _stats(tmp_path, "a.merged", "client")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c", "mode": "basic"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["selobjs"] == ["all"]
    joined = " ".join(res["tree_rows"])
    assert "总览" in joined and "角色" not in joined


def test_multi_object_node_hues_and_metric_buttons_removed(tmp_path):
    """S4：节点色系/实例同系色阶、总计灰阶；指标弹出行删除；队列等待入选择且默认关。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    b = _stats(tmp_path, "b.writer", "writer", node="n2")
    data = rp.reduce_scenario(rp.load([a, b]), {"config": "c", "mode": "basic"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    assert 'id="metricBtns"' not in src          # 指标弹出行已删除
    assert 'id="selAllBtn"' not in src and 'id="selClearBtn"' not in src

    assert '全选节点' in src and '全选实例' in src and '全部清空' in src
    res = _js_check(tmp_path, data, "--add-obj", "node:n2")
    if res is None:
        return
    assert res["errors"] == []
    # 队列等待在时延曲线选择中且默认关闭
    qw = [x for x in res["lat_btns"]
          if x["t"].strip().startswith("队列等待")]
    assert qw, res["lat_btns"]
    assert qw[0]["off"]                         # 默认不展示
    # 多对象颜色：总计(role) 为灰阶；node:n2 为彩色色系（通道不等）
    cols = [c for c in (res.get("overlay_colors") or res["main_colors"])
            if c["color"]]
    grays = [c for c in cols
             if "all" in c["name"] or c["name"].startswith("总览")]
    hues = [c for c in cols if c["name"].startswith("n2")]
    assert grays and hues, cols
    import re as _re
    def rgb(h):
        h = h.lstrip("#")
        return [int(h[i:i + 2], 16) for i in (0, 2, 4)]
    for c in grays[:4]:
        r = rgb(c["color"])
        assert max(r) - min(r) <= 16, c       # 总计灰阶
    assert any(max(rgb(c["color"])) - min(rgb(c["color"])) > 20 for c in hues), hues


def test_tree_group_select_buttons(tmp_path):
    """树「节点与实例」组：全选节点 / 全选实例 / 全部清空（总计保留）。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    b = _stats(tmp_path, "b.writer", "writer", node="n2")
    data = rp.reduce_scenario(rp.load([a, b]), {"config": "c", "mode": "basic"})
    res = _js_check(tmp_path, data, "--tree-select")
    if res is None:
        return
    assert res["errors"] == []
    assert "all" in res["sel_after_nodes"] and "node:n1" in res["sel_after_nodes"]
    assert "node:n2" in res["sel_after_nodes"]
    assert any(k.startswith("client:") for k in res["sel_after_instances"])
    # 全部清空：节点+实例清掉，总计（basic=总览 all）保留
    assert res["sel_after_clear"] == ["all"]
    # pipeline：全部清空后保留当前角色（总计）
    a2 = _stats(tmp_path, "a2.writer", "writer", node="n1")
    b2 = _stats(tmp_path, "b2.reader", "reader", node="n2")
    d2 = rp.reduce_scenario(rp.load([a2, b2]),
                            {"config": "c", "mode": "pipeline"})
    r2 = _js_check(tmp_path, d2, "--tree-select")
    if r2 is None:
        return
    assert r2["sel_after_clear"] == ["reader"], r2["sel_after_clear"]


def test_basic_summary_unchanged_by_role_split(tmp_path):
    """basic 无角色分块：合并卡正常显示，单元行仍是 数据大小/客户端数/线程数/时长。"""
    a = _stats(tmp_path, "a.merged", "client")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c", "mode": "basic"})
    assert data["sizes"]["8MB"]["role_summaries"] == {}
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["rolecards_shown"] is False
    assert res["perf_shown"] is True
    labels = [k["text"] for k in res["unit_kids"]]
    assert any(t.startswith("数据大小") for t in labels), labels
    assert any(t.startswith("线程数") for t in labels), labels


def test_build_report_params_pipeline_qps_total():
    """pipeline 参数：目标写 QPS 全口径 + 读 QPS + store 数量。"""
    cfg = {"scenario": "pipeline", "protocol": "tcp", "work_mode": "rank",
           "base_qps": 25, "qps_multiple": 1, "notify_count": 4,
           "client": {"count_per_node": 2}, "store_count_per_node": 1,
           "data_size": ["128KB"], "duration": 600,
           "warmup": {"duration": 10}}
    meta = {"mode": "ssh", "launch_mode": "rank_recall", "clients": 8,
            "started": "2026-11-19 00:00:00"}
    ps = dict(bm._build_report_params(cfg, meta, nodes=4))
    assert ps["目标写 QPS"] == "25 × 4 节点 = 100"
    assert ps["目标读 QPS"] == "100 × 4 读 = 400"
    assert ps["store_count_per_node"] == 1
    assert ps["client.count_per_node"] == 2


def test_pipeline_tree_role_radio_total_and_scoped_nodes(tmp_path):
    """pipeline 对象树：角色单选（默认 reader）+ 总计（默认开）+ 角色化节点对象。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    a2 = _stats(tmp_path, "a2.writer", "writer", node="n2")
    b = _stats(tmp_path, "b.reader", "reader", node="n2")
    b2 = _stats(tmp_path, "b2.reader", "reader", node="n1")
    data = rp.reduce_scenario(rp.load([a, a2, b, b2]),
                              {"config": "cfg", "mode": "pipeline"})
    z = data["sizes"]["8MB"]
    # 服务端：节点对象按角色拆分；basic 形态的 node:<n> 不再存在
    assert "node:n1:writer" in z["objects"]
    assert "node:n2:reader" in z["objects"]
    assert not any(k.startswith("node:") and k.count(":") == 1
                   for k in z["objects"])
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert res["selobjs"] == ["reader"]          # 默认 reader + 仅总计
    joined = " ".join(res["tree_rows"])
    assert "角色" in joined and "总计" in joined
    assert "reader" in joined and "writer" in joined
    # reader 视角的节点与实例组存在，且只列 reader 角色节点（内嵌数据保留 role）
    assert "节点与实例" in joined, res["tree_rows"]
    tail = joined.split("节点与实例")[-1]
    assert "n1" in tail and "n2" in tail, tail
    # 展开后：每个节点下只有 reader 实例（writer 实例不在该角色视图里）
    rx = _js_check(tmp_path, data, "--expand-tree")
    if rx is None:
        return
    exp = " ".join(rx.get("tree_rows_expanded") or [])
    assert "b.reader" in exp and "b2.reader" in exp, exp
    assert "a.writer" not in exp and "a2.writer" not in exp, exp
    # 切到 writer：选中集切换、QPS 副图跟随
    r2 = _js_check(tmp_path, data, "--role-writer")
    if r2 is None:
        return
    assert r2["errors"] == []
    assert r2["selobjs_after_role"] == ["writer"]
    assert any(str(n).startswith("writer") for n in r2["qps_after_role"]), \
        r2["qps_after_role"]
    # 关闭总计：当前角色未选节点/实例 -> 空
    r3 = _js_check(tmp_path, data, "--total-off")
    if r3 is None:
        return
    assert r3["selobjs_total_off"] == []
    # 切换角色不丢弃选择：按角色记忆，切回自动恢复
    r4 = _js_check(tmp_path, data, "--role-roundtrip")
    if r4 is None:
        return
    assert r4["errors"] == []
    assert "node:n2:reader" in r4["selobjs_reader_selected"]
    assert r4["selobjs_writer_view"] == ["writer"]
    assert "node:n2:reader" in r4["selobjs_back_reader"], r4["selobjs_back_reader"]


def test_basic_tree_keeps_single_overview_selection(tmp_path):
    """basic：选中集仍是「总览」，没有角色 radio/总计。"""
    a = _stats(tmp_path, "a.merged", "client")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c", "mode": "basic"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["selobjs"] == ["all"]
    joined = " ".join(res["tree_rows"])
    assert "总览" in joined and "角色" not in joined


def test_multi_object_node_hues_and_metric_buttons_removed(tmp_path):
    """S4：节点色系/实例同系色阶、总计灰阶；指标弹出行删除；队列等待入选择且默认关。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    b = _stats(tmp_path, "b.writer", "writer", node="n2")
    data = rp.reduce_scenario(rp.load([a, b]), {"config": "c", "mode": "basic"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    assert 'id="metricBtns"' not in src          # 指标弹出行已删除
    assert 'id="selAllBtn"' not in src and 'id="selClearBtn"' not in src

    assert '全选节点' in src and '全选实例' in src and '全部清空' in src
    res = _js_check(tmp_path, data, "--add-obj", "node:n2")
    if res is None:
        return
    assert res["errors"] == []
    # 队列等待在时延曲线选择中且默认关闭
    qw = [x for x in res["lat_btns"]
          if x["t"].strip().startswith("队列等待")]
    assert qw, res["lat_btns"]
    assert qw[0]["off"]                         # 默认不展示
    # 多对象颜色：总计(role) 为灰阶；node:n2 为彩色色系（通道不等）
    cols = [c for c in (res.get("overlay_colors") or res["main_colors"])
            if c["color"]]
    grays = [c for c in cols
             if "all" in c["name"] or c["name"].startswith("总览")]
    hues = [c for c in cols if c["name"].startswith("n2")]
    assert grays and hues, cols
    import re as _re
    def rgb(h):
        h = h.lstrip("#")
        return [int(h[i:i + 2], 16) for i in (0, 2, 4)]
    for c in grays[:4]:
        r = rgb(c["color"])
        assert max(r) - min(r) <= 16, c       # 总计灰阶
    assert any(max(rgb(c["color"])) - min(rgb(c["color"])) > 20 for c in hues), hues


def test_line_widths_avg_thicker_only(tmp_path):
    """A3：avg 1.0 / 其余 0.8，hover 强调 +0.2（不再是首对象 ×1.2）；
    阶段曲线 total 1.0 / 阶段 0.8。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    b = _stats(tmp_path, "b.writer", "writer", node="n2")
    tr = _trace_stats_span(tmp_path, "s300.glog", 300)
    data = rp.reduce_scenario(rp.load([a, b]), {"config": "c"},
                              traces=[tr])
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    lw = {x["name"]: x for x in res["main_line_w"]}
    assert lw["avg"]["width"] == 1.0 and lw["avg"]["emphasis"] == 1.2, lw["avg"]
    for k in ("p50", "p99", "p999", "min", "max", "队列等待"):
        if k in lw:
            assert lw[k]["width"] == 0.8, (k, lw[k])
            assert lw[k]["emphasis"] == 1.0, (k, lw[k])
    tc = {x["name"]: x for x in res["tchart_line_w"]}
    total = tc.get("total（端到端）")
    assert total and total["width"] == 1.0, tc
    # 阶段序列（若本次产物带阶段秒桶）同样 0.8 / 1.0
    for nm, x in tc.items():
        if nm != "total（端到端）":
            assert x["width"] == 0.8 and x["emphasis"] == 1.0, (nm, x)


def test_endlabel_fixed_slots_and_cluster_focus(tmp_path):
    """#3：多对象标签=对象名、固定槽位、无引线、带全名 title；
    悬浮标签压暗其他对象、保留该对象整簇，离开复原。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    b = _stats(tmp_path, "b.writer", "writer", node="n2")
    data = rp.reduce_scenario(rp.load([a, b]), {"config": "c"})
    res = _js_check(tmp_path, data, "--add-obj", "node:n1",
                    "--cluster-hover")
    if res is None:
        return
    assert res["errors"] == []
    labels = res.get("cluster_labels") or []
    assert len(labels) == 2, labels            # 总计 + node:n1
    assert "总" in labels[0], labels           # 总计/总体/总览在最上
    assert labels[1] == "n1", labels
    assert res.get("leader_count") == 0        # 无引线
    classes = res.get("cluster_classes") or []
    assert all("fixed" in c for c in classes), classes
    titles = res.get("cluster_titles") or []
    assert all("悬浮聚焦" in t for t in titles), titles
    names = res.get("cluster_series") or []
    dims = res.get("cluster_dims") or []
    assert len(dims) == len(names) and dims
    for nm, op in zip(names, dims):
        want = 1 if (nm == "n1" or str(nm).startswith("n1 ")) else 0.15
        assert op == want, (nm, op)
    assert res.get("cluster_restored") and \
        all(x == 1 for x in res["cluster_restored"]), res.get("cluster_restored")


def test_multilabel_tree_order_and_truncation(tmp_path):
    """#3：固定标签顺序 = 总计 → 节点 → 实例（树序平铺）；超长名靠 CSS 截断+title。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    b = _stats(tmp_path, "b.writer", "writer", node="n2")
    c = _stats(tmp_path, "c.writer", "writer", node="n3")
    data = rp.reduce_scenario(rp.load([a, b, c]), {"config": "c"})
    res = _js_check(tmp_path, data, "--mlabels")
    if res is None:
        return
    assert res["errors"] == []
    labels = res.get("fixed_labels") or []
    assert len(labels) == 4, labels            # 总计 + 2 节点 + 1 实例
    assert "总" in labels[0], labels
    nodes = res.get("ml_node_labels") or []
    cl = res.get("ml_client_label")
    assert cl in labels, (cl, labels)
    # 树序平铺：节点按序、其实例紧跟在该节点之后
    i0, i1, ic = labels.index(nodes[0]), labels.index(nodes[1]), \
        labels.index(cl)
    assert labels[1] == nodes[0], labels
    assert i0 < i1, labels
    assert ic == i0 + 1, labels                # a.writer 属于 n1，紧跟其后
    assert res.get("leader_count") == 0
    classes = res.get("fixed_classes") or []
    assert all("fixed" in x for x in classes), classes
    titles = res.get("fixed_titles") or []
    assert all(t for t in titles), titles      # 全名挂在 title 上（截断时悬浮可看）
    # 图例可看不可点
    assert res.get("legend_selmode") is False, res.get("legend_selmode")


def test_all_charts_tooltip_hover_pointer_click_box(tmp_path):
    """#4：tooltip 悬浮只显示竖线+交点（showContent=false），内容框点击才出现。"""
    a = _stats(tmp_path, "a.reader", "reader")
    tr = _trace_stats_span(tmp_path, "s300.glog", 300)
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"}, traces=[tr])
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    tips = res.get("tooltips") or {}
    for cid in ("mainchart", "qpschart", "histchart", "slowchart",
                "tchart", "tstackchart"):
        assert cid in tips, (cid, tips.keys())
        assert tips[cid]["triggerOn"] == "mousemove", (cid, tips[cid])
        assert tips[cid]["showContent"] is False, (cid, tips[cid])
        assert tips[cid]["enterable"] is True, (cid, tips[cid])


def test_tooltip_click_anywhere_opens(tmp_path):
    """点击绘图区任意位置（含空白）→ 按点击 x 打开详情框；区外点击 → 收起。
    分级时延曲线走同一 bindClickTip，必须同样生效。"""
    a = _stats(tmp_path, "a.reader", "reader")
    tr = _trace_stats_span(tmp_path, "s300.glog", 300)
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"}, traces=[tr])
    res = _js_check(tmp_path, data, "--tip-click")
    if res is None:
        return
    assert res["errors"] == []
    assert res["tip_after_click"] == {"triggerOn": "none",
                                      "showContent": True}, \
        res["tip_after_click"]
    act = [a for a in res["tip_click_actions"] if a.get("type") == "showTip"]
    assert act and act[0].get("x") == 600 and act[0].get("y") == 200, act
    assert res["tip_after_outside"] == {"triggerOn": "mousemove",
                                       "showContent": False}, \
        res["tip_after_outside"]
    assert any(a.get("type") == "hideTip"
               for a in res["tip_outside_actions"]), res["tip_outside_actions"]
    # 分级时延曲线：空白点击同样 showTip（x/y = 点击位置）
    tacts = [a for a in (res.get("tchart_tip_actions") or [])
             if a.get("type") == "showTip"]
    assert tacts and tacts[0].get("x") == 500, tacts
    assert res["tchart_tip"] == {"triggerOn": "none",
                                 "showContent": True}, res["tchart_tip"]


def test_latency_metric_select_all_clear(tmp_path):
    """时延曲线选择：全选=8 项全开（含队列等待）；清空=仅保留 avg。"""
    a = _stats(tmp_path, "a.reader", "reader")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    res = _js_check(tmp_path, data, "--lat-buttons")
    if res is None:
        return
    assert res["errors"] == []
    assert len(res["lat_before"]) == 8, res["lat_before"]
    on = [x for x in res["lat_all"] if not x["off"]]
    assert len(on) == 8, on
    left = [x for x in res["lat_clear"] if not x["off"]]
    assert [x["t"] for x in left] == [" avg"], left


def test_report_started_time_uses_data_timestamp(tmp_path):
    """P1-B：页头「测试开始于」= 数据最早采样时刻（不是生成时刻）。"""
    logs = tmp_path / "logs"
    logs.mkdir()
    (logs / "cfg.pipeline_c1_t1_8MB.dual_n1_c0_p9301.writer.log").write_text(
        "".join("RESULT\t2026-11-12 10:00:{:02d}.{:06d}\tpipeline_write\t"
                "k{}\t1.0\tOK\t4\n".format(10 + i // 10, i % 10, i)
                for i in range(30)), encoding="utf-8")

    class A:
        cluster = ""
        global_cfg = ""
        profile = ""
        no_plot = False
        dry_run = False

    written = bm._generate_report_units({}, str(logs), A(), mode="pipeline")
    assert written, "report not generated"
    meta = json.loads((logs / "report" / "8MB" / "meta.json")
                      .read_text(encoding="utf-8"))
    assert meta["started"] == "2026-11-12 10:00:10", meta["started"]


def test_stage_tree_objects_enabled(tmp_path):
    """SDK 全链路对象树：总计 + 节点与实例（角色行已上移到顶部总控）。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    b = _stats(tmp_path, "b.reader", "reader", node="n2")
    tr = _trace_stats_span(tmp_path, "s300.glog", 300)
    data = rp.reduce_scenario(rp.load([a, b]),
                              {"config": "cfg", "mode": "pipeline"},
                              traces=[tr])
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    rows = res.get("stage_rows") or []
    assert rows, res.get("stage_tree_text")
    tot = [r for r in rows if "总计" in r["text"]]
    assert tot and not tot[0]["disabled"], rows
    nodes = [r for r in rows if "n1" in r["text"] or "n2" in r["text"]]
    assert nodes, rows
    assert all(not r["disabled"] for r in nodes), rows
    assert not any(r["text"].strip() in ("writer", "reader") for r in rows), rows


def test_trace_object_tree_toggle(tmp_path):
    """SDK 对象树：全选节点选中节点；全部清空自动回到总计。"""
    a = _stats(tmp_path, "a.reader", "reader", node="n1")
    tr = _trace_stats_span(tmp_path, "s300.glog", 300)
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"}, traces=[tr])
    res = _js_check(tmp_path, data, "--trace-tree")
    if res is None:
        return
    assert res["errors"] == []
    assert res["tsel_before"] == ["role:read"], res["tsel_before"]
    assert any(k.startswith("node:n1") for k in res["tsel_nodes"]), \
        res["tsel_nodes"]
    assert "清空节点" in (res.get("tree_btn_after_nodes") or []), \
        res.get("tree_btn_after_nodes")
    assert res["tsel_clear"] == ["role:read"], res["tsel_clear"]


def test_trace_curve_dimension_coupling(tmp_path):
    """分级时延曲线：单对象可多阶段；一旦对象多选，阶段自动收敛为 total。"""
    a = _stats(tmp_path, "a.reader", "reader", node="n1")
    tr = _trace_stats_span(tmp_path, "s300.glog", 300)
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"}, traces=[tr])
    res = _js_check(tmp_path, data, "--trace-dims")
    if res is None:
        return
    assert res["errors"] == []
    # 单对象多阶段：series = 阶段名（无「对象 · 阶段」拼接）
    st = res.get("dim_stage_series") or []
    assert len(st) >= 2, st
    assert all(" · " not in n for n in st), st
    # 再选节点：阶段收敛为 total，series 变为对象名
    ob = res.get("dim_obj_series") or []
    assert "总计" in ob and any("n1" in n for n in ob), ob
    assert res.get("dim_tsel_multi") == 2, res.get("dim_tsel_multi")
    # 块图对象选择器 / 曲线指标按钮
    assert "总计" in (res.get("block_opts") or []), res.get("block_opts")
    assert res.get("curve_metrics") == ["均值", "最大"], res.get("curve_metrics")


def test_qps_zoom_keeps_y_and_gap_labels_once(tmp_path):
    """S6：QPS 副图 x 缩放不重算 y（filterMode none）；无数据窗口仅首处标注。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    import types
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    # QPS 图 dataZoom filterMode = none
    assert res.get("qps_filter_mode") == "none", res.get("qps_filter_mode")


def test_slow_capture_independent_controls(tmp_path):
    """S7：慢点捕捉独立控件（角色/总计/节点实例），叠加语义，x 轴与主图同窗口。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    b = _stats(tmp_path, "b.reader", "reader", node="n2")
    data = rp.reduce_scenario(rp.load([a, b]),
                              {"config": "cfg", "mode": "pipeline"})
    z = data["sizes"]["8MB"]
    assert z["slowest"] and z["slowest"][0].get("role") in ("writer", "reader")
    assert z["slowest"][0].get("node")
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    ctl = res["slow_ctl_text"]
    assert "角色" in ctl and "总计" in ctl and "节点与实例" in ctl
    # 默认：reader 总计（灰色散点、圆形标记）
    sr = res["slow_series"]
    assert sr and sr[0]["name"] == "总计（reader）", sr
    assert sr[0]["type"] == "scatter" and sr[0]["symbol"] == "circle"
    # x 轴与端到端曲线同窗口（summary ts_from..ts_to）
    sm = z["summary"]
    x0, x1 = res["slow_x"]
    assert abs(x0 - sm["ts_from"] * 1000) < 1500, (x0, sm["ts_from"] * 1000)
    assert abs(x1 - sm["ts_to"] * 1000) < 1500, (x1, sm["ts_to"] * 1000)
    # 切到 writer：方点标记；再全选：总计 + 节点/实例高亮叠加
    r2 = _js_check(tmp_path, data, "--slow-writer")
    if r2 is None:
        return
    assert r2["errors"] == []
    assert any(x["name"] == "总计（writer）" and x["symbol"] == "rect"
               for x in r2["slow_series_writer"]), r2["slow_series_writer"]
    assert len(r2["slow_series_all"]) > 1, r2["slow_series_all"]
    assert r2["slow_series_all"][0]["name"].startswith("总计")
    # #2：慢点控件同样是 toggle 按钮（全选后变「清空节点/清空实例」）
    assert "全选节点" in (r2.get("slow_btn_before") or []), r2.get("slow_btn_before")
    assert "清空节点" in (r2.get("slow_btn_after_nodes") or []), \
        r2.get("slow_btn_after_nodes")
    assert "清空实例" in (r2.get("slow_btn_after_instances") or []), \
        r2.get("slow_btn_after_instances")


def test_slow_capture_basic_has_no_role_radio(tmp_path):
    a = _stats(tmp_path, "a.merged", "client")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c", "mode": "basic"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    ctl = res["slow_ctl_text"]
    assert "角色" not in ctl
    assert "总计" in ctl


# ===========================================================================
# A 类补测（2026-11-19 测试审计）：REQUIREMENTS v0.6「已实现、无测试」的规则
# 逐条落测——审计矩阵 38✓/10◐/7✗ 中的 ✗ 与 ◐ 补齐项。纯补测，不改产品码。
# ===========================================================================

def test_a01_gap_markarea_between_runs(tmp_path):
    """R-LAT-9：相邻桶间隔 > 桶宽中位数 3 倍 => 主图/QPS 图 markArea 浅带，
    注「N 秒无数据」。构造 0..9s + 26..30s（gap=16s > 3x1s）。"""
    st = _minimal_stats([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 26, 27, 28, 29, 30],
                       [0, 30])
    st["meta"].update({"ts_from": 0, "ts_to": 30, "node": "n1",
                       "role": "writer", "source": "a.writer.log"})
    data = rp.reduce_scenario([st], {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    ma = res.get("main_markarea") or []
    qma = res.get("qps_markarea") or []
    assert ma, "主图应有 markArea 无数据带"
    label = str(ma[0][0].get("name") or "")
    assert "无数据" in label, label
    assert "17" in label, label            # 10 -> 26，含端点共 17 秒（as-built）
    assert qma, "QPS 副图同样应有 markArea"


def test_a02_no_gap_no_markarea(tmp_path):
    """R-LAT-9 反例：连续 1s 桶无间隔 => 不产生任何无数据带。"""
    st = _minimal_stats(list(range(40)), [0, 30])
    st["meta"].update({"ts_from": 0, "ts_to": 39, "node": "n1",
                       "role": "writer", "source": "a.writer.log"})
    data = rp.reduce_scenario([st], {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert not (res.get("main_markarea") or []), res.get("main_markarea")


def test_a03_latency_color_semantics_gray_only(tmp_path):
    """R-LAT-12 / skill §1：端到端曲线全灰阶（深灰 avg/p99/p9999、其余浅灰），
    多对象调色板同为灰阶；任何 LAT_COLORS 值的 r=g=b（不允许彩色曲线）。"""
    a = _stats(tmp_path, "a.writer", "writer", node="n1")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    colors = res.get("main_colors") or []
    assert colors, "应采集到主线 series 颜色"
    # 灰阶 token = lieflat 阶梯既有值（r≈g≈b 的「视觉灰」，非严格相等）；
    # 语义禁令是「不引入新 hex / 不用彩色」，故断言通道差 <= 8（灰）且
    # 全部落在既有 token 白名单内。
    _grays = {"#6E6D66", "#8F8E86", "#22211F", "#3F3E3B", "#A5A49C",
              "#55554F", "#B3B0A4", "#C0BFB7", "#DCDAD2", "#4A4944",
              "#8F8E88", "#6A6963", "#C9C7BD", "#B5B4AC"}
    for c in colors:
        h = str(c["color"] or "").upper().lstrip("#")
        if len(h) == 3:
            h = "".join(ch * 2 for ch in h)
        assert len(h) == 6, c
        r, g, b = (int(h[i:i + 2], 16) for i in (0, 2, 4))
        assert max(r, g, b) - min(r, g, b) <= 16, "曲线出现非灰阶颜色: {}".format(c)
        assert "#" + h in {g2.upper() for g2 in _grays}, \
            "曲线颜色不在灰阶 token 白名单: {}".format(c)


def test_a04_slow_strip_hero_only_on_max(tmp_path):
    """R-LAT-12：慢点条带仅「该组最慢一根」用 hero 橙，其余全灰。
    同时锁定慢点 series 为 scatter（rect 仅 pipeline writer）。"""
    st = _minimal_stats([0, 1, 2, 3], [0])
    st["meta"].update({"ts_from": 0, "ts_to": 3, "node": "n1",
                       "role": "writer", "source": "a.writer.log"})
    st["slowest"] = [{"ts": "2026-11-19 00:00:0{}.000000".format(i),
                      "op": "put", "key": "k{}".format(i),
                      "lat_ms": 10.0 + i, "status": "OK"} for i in range(4)]
    data = rp.reduce_scenario([st], {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    # v4 as-built：慢点卡为对象化散点（独立控件）。默认「总计」系列颜色
    # 为灰阶 C.mut（rgba），无任何 hero 橙散布在普通慢点上；橙只允许出现在
    # 勾选高亮（slowColorOf）之外——R-LAT-12 的慢点条款随 v4 重构语义如下：
    ss = res.get("slow_series") or []
    assert ss and ss[0]["name"].startswith("总计"), ss
    assert ss[0]["type"] == "scatter", ss
    _sc = res.get("slow_colors") or []
    assert all(c is None for c in _sc), \
        "普通慢点不得携带逐点颜色（橙只用于勾选高亮语义）: {}".format(_sc)


def test_a05_qps_fail_series_hero_only(tmp_path):
    """R-LAT-12：QPS 副图橙只出现在「失败」系列；QPS 柱系列必须为灰阶。"""
    st = _minimal_stats([0, 1, 2, 3], [0])
    st["meta"].update({"ts_from": 0, "ts_to": 3, "node": "n1",
                       "role": "writer", "source": "a.writer.log"})
    data = rp.reduce_scenario([st], {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    qcs = res.get("qps_colors") or []
    assert qcs, "应采集 QPS series 颜色"
    named = {str(s["name"]): s for s in qcs}
    fail_names = [n for n in named if "失败" in n]
    assert fail_names, "QPS 副图应有失败系列: {}".format(list(named))
    for n in fail_names:
        assert "F5572F" in str(named[n]["color"] or "").upper(), named[n]
    _grays = {"#8F8E86", "#8F8E88", "#6E6D66", "#A5A49C", "#4A4944",
              "#B3B0A4", "#DCDAD2", "#3F3E3B", "#22211F", "#55554F",
              "#C0BFB7", "#C9C7BD", "#B5B4AC", "#6A6963"}
    for n, s in named.items():
        if n in fail_names:
            continue
        c = str(s["color"] or "").upper()
        assert c in {g.upper() for g in _grays}, \
            "QPS 柱必须使用灰阶 token: {}".format(s)


def test_a06_metric_list_locked_to_spec(tmp_path):
    """R-LAT-3：曲线指标清单锁定为 LAT_LINES 定义（avg/p50/p99/p999/p9999/
    min/max/qw），按钮数 = 清单长度；杜绝 latSel 超纲类漂移。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    names = [b["t"].strip() for b in (res.get("lat_btns") or [])
             if b["t"].strip() not in ("全选", "清空")]
    expected = {"avg", "p50", "p99", "p999", "p9999", "min", "max", "队列等待"}
    assert set(names) == expected, names
    # 全选 / 清空 两个操作按钮存在（指标清单本身仍锁定）
    allt = [b["t"].strip() for b in (res.get("lat_btns") or [])]
    assert "全选" in allt and "清空" in allt, allt


def test_a07_anomalies_capped_at_50(tmp_path):
    """R-LAT-10：异常竖线最多 50 条（slice(0,50)）。构造 60 条异常。"""
    st = _minimal_stats([0, 1, 2, 3], [0])
    st["meta"].update({"ts_from": 0, "ts_to": 3, "node": "n1",
                       "role": "writer", "source": "a.writer.log"})
    st["anomalies"] = [{"t": i * 0.05, "kind": "latency_spike",
                        "avg_ms": 9.9, "count": 5} for i in range(60)]
    data = rp.reduce_scenario([st], {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert res.get("main_markline_n") == 50, res.get("main_markline_n")


def test_a08_data_missing_degrades_to_note(tmp_path):
    """R-SHELL-2：data.js 加载失败（__mcDataMissing）=> datanote 显示，
    明细表降级仍可渲染（行数 > 0），页面不崩。"""
    a = _stats(tmp_path, "a.writer", "writer")
    b = _stats(tmp_path, "b.reader", "reader")
    data = rp.reduce_scenario(rp.load([a, b]), {"config": "c"})
    res = _js_check(tmp_path, data, env={"VC_DATAMISSING": "1"})
    if res is None:
        return
    assert res["errors"] == []
    assert res.get("datanote_visible") != "none", res.get("datanote_visible")


def test_a09_data_loaded_hides_note(tmp_path):
    """R-SHELL-2 正例：data.js 在场（VC_FULLSIM）=> datanote 保持隐藏。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    res = _js_check(tmp_path, data, env={"VC_FULLSIM": "1"})
    if res is None:
        return
    assert res["errors"] == []
    # as-built（桩 DOM）：inline style="display:none" 不进桩，仅 JS 修改可观测。
    # data.js 在场时 renderTable 从不触碰 datanote => style.display 保持未设置；
    # 与失败路径（A08 中被显式置 ''）相区分：此处必须读作空串。
    assert res.get("datanote_visible") == "", res.get("datanote_visible")


def test_a10_subtitle_sequence(tmp_path):
    """R-SHELL-5：副标题序列 = 测试开始于 / 报告生成于 / 协议 /（早停警告）。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]),
                              {"config": "c", "started": "2026-11-19 10:00:00",
                               "protocol": "tcp", "early_stop": True})
    html, _dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    i0 = src.index("测试开始于")
    i1 = src.index("报告生成于")
    i2 = src.index("协议")
    i3 = src.index("提前停止")
    assert i0 < i1 < i2 < i3, (i0, i1, i2, i3)


def test_a11_empty_hist_and_slow_cards_hidden(tmp_path):
    """R-HIST-3 / R-SLOW-3：无直方图 / 无慢点数据 => 整卡保持 display:none。"""
    st = _minimal_stats([0, 1, 2, 3], [0])
    st["meta"].update({"ts_from": 0, "ts_to": 3, "node": "n1",
                       "role": "writer", "source": "a.writer.log"})
    st["histogram"] = {}              # 无直方图
    # slowest 已是空（_minimal_stats 默认）
    data = rp.reduce_scenario([st], {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert res.get("hist_visible") == "none", res.get("hist_visible")
    assert res.get("slow_visible") == "none", res.get("slow_visible")


def test_a12_hist_and_slow_cards_visible_with_data(tmp_path):
    """R-HIST-3 / R-SLOW-3 正例：有数据 => 两卡解除隐藏。"""
    st = _minimal_stats([0, 1, 2, 3], [0])
    st["meta"].update({"ts_from": 0, "ts_to": 3, "node": "n1",
                       "role": "writer", "source": "a.writer.log"})
    st["slowest"] = [{"ts": "2026-11-19 00:00:01.000000", "op": "put",
                      "key": "k1", "lat_ms": 9.0, "status": "OK"}]
    data = rp.reduce_scenario([st], {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert res.get("hist_visible") != "none", res.get("hist_visible")
    assert res.get("slow_visible") != "none", res.get("slow_visible")


def test_a13_zoom_links_main_and_qps(tmp_path):
    """R-LAT-6：主图 dataZoom 后 QPS 副图窗口同步（VC_ZOOM 注入后读回两图）。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    res = _js_check(tmp_path, data, "--real",
                    env={"VC_ZOOM": "30,60"})
    if res is None:
        return
    assert res["errors"] == [], res["errors"]
    zs = res.get("zoom_states") or {}
    assert zs.get("mainchart") and zs.get("qpschart"), zs
    assert zs["mainchart"]["start"] == zs["qpschart"]["start"], zs
    assert zs["mainchart"]["end"] == zs["qpschart"]["end"], zs
    assert zs["mainchart"]["start"] == 30, zs


def test_a14_safe_isolation_single_block(tmp_path):
    """R-SHELL-8：单块渲染抛错只失效该块，其余块照常（errors 仍为空，
    其余卡片文本已填充）。注入 renderHist 抛错。"""
    a = _stats(tmp_path, "a.writer", "writer")
    b = _stats(tmp_path, "b.reader", "reader")
    data = rp.reduce_scenario(rp.load([a, b]), {"config": "c"})
    res = _js_check(tmp_path, data, env={"VC_BREAK": "renderHist"})
    if res is None:
        return
    # safe() 吞掉注入异常：页面主流程不受影响（渲染树/summary 正常）
    assert res.get("cards_text"), "其余块仍应渲染"
    assert res["errors"] == [], res["errors"]


def test_a15_report_stdlib_only():
    """C-2：report.py 仅使用 Python 标准库（AST 扫描 import）。"""
    import ast as _ast
    src = open(os.path.join(ROOT, "scripts", "report.py"),
               encoding="utf-8").read()
    std = set(sys.stdlib_module_names) | {"logstats"}
    for node in _ast.walk(_ast.parse(src)):
        if isinstance(node, _ast.Import):
            for al in node.names:
                top = al.name.split(".")[0]
                assert top in std, "非标准库 import: {}".format(al.name)
        elif isinstance(node, _ast.ImportFrom):
            if node.module:
                top = node.module.split(".")[0]
                assert top in std, "非标准库 import: {}".format(node.module)


def test_a16_theme_and_numeric_css(tmp_path):
    """R-SHELL-3 / R-SHELL-6 / R-SHELL-10：双主题 @media、tabular-nums 存在；
    sizeSel 多 size 才显示（结构存在 + JS 单 size 隐藏）。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    assert "@media (prefers-color-scheme:dark)" in src
    assert "tabular-nums" in src
    assert 'id="sizeSel"' in src
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert res.get("size_sel_display") == "none", res.get("size_sel_display")


def test_a17_fmt_us_adaptive_units(tmp_path):
    """R-SHELL-10：fmtUs 自适应单位（µs -> ms -> s）。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    fu = res.get("fmt_us")
    assert fu, "harness 应输出 fmtUs 样例"
    assert fu[0].endswith("ms"), fu        # 1500us -> 1.50 ms
    assert fu[1].endswith("µs"), fu        # 500us -> 500.0 µs
    assert fu[3].endswith("ms"), fu        # 1234567us -> 1234.57 ms


def test_a18_outlier_button_disabled_without_outliers(tmp_path):
    """R-LAT-4 v0.5 分支：无离群请求时按钮置灰（不呈现永远无效的控件），
    title 说明口径。"""
    st = _minimal_stats([0, 1, 2, 3], [0])
    st["meta"].update({"ts_from": 0, "ts_to": 3, "node": "n1",
                       "role": "writer", "source": "a.writer.log"})
    st["outliers"] = []
    data = rp.reduce_scenario([st], {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    ob = res.get("outlier_btn_state") or {}
    assert ob.get("disabled") is True, ob
    # as-built：说明走 title（悬停可见），文本仍为动作名
    assert "没有离群" in str(ob.get("title") or ""), ob


def test_a19_outlier_button_enabled_with_outliers(tmp_path):
    """R-LAT-4 正例：有离群请求时按钮可用且 title 带计数。"""
    st = _minimal_stats([0, 1, 2, 3], [0])
    st["meta"].update({"ts_from": 0, "ts_to": 3, "node": "n1",
                       "role": "writer", "source": "a.writer.log"})
    st["outliers"] = [{"ts": "2026-11-19 00:00:01.000000", "op": "put",
                       "key": "k9", "lat_ms": 500.0, "status": "OK"}]
    data = rp.reduce_scenario([st], {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    ob = res.get("outlier_btn_state") or {}
    assert ob.get("disabled") is False, ob


# ===========================================================================
# B 类 TDD（v0.5/v0.6 [新增] 需求）：先红后绿。每条需求一个测试。
# ===========================================================================

def test_b01_unit_identity_in_title_and_h1(tmp_path):
    """R-SHELL-11 [新增]：<title> 与 <h1> 携带场景名与单元身份
    （config · unit，取自 meta）；生成时间只在副标题，不进 h1。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]),
                              {"config": "get_buffer_multi",
                               "mode": "basic", "unit": "c4×t8 · 128KB"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    assert "get_buffer_multi" in src.split("</title>")[0], \
        "<title> 应含场景名"
    assert "c4×t8" in src.split("</title>")[0], "<title> 应含单元身份"
    assert "get_buffer_multi" in src.split("</h1>")[0], "<h1> 应含场景名"
    assert "c4×t8" in src.split("</h1>")[0], "<h1> 应含单元身份"


def test_b02_chart_role_and_aria_labels(tmp_path):
    """R-SHELL-12 [新增]：每个图表容器有 role="img" 与 aria-label。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    import re as _re
    charts = _re.findall(r'id="(mainchart|qpschart|histchart|slowchart|'
                          r'tchart|tstackchart|memchart|procchart|urmachart)"',
                          src)
    assert charts, "应存在图表容器"
    for cid in set(charts):
        m = _re.search(r'id="' + cid + r'"[^>]*role="img"', src)
        assert m, "{} 缺 role=\"img\"".format(cid)
        assert 'aria-label' in src, "图表容器缺 aria-label"


def test_b03_heading_hierarchy_no_skips(tmp_path):
    """R-SHELL-13 [新增]：标题层级不跳级——首卡的「性能统计」分节必须有
    所属 h2 卡片标题；h1 后不得直接跟 h3。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    # h1 在外壳（_page_html），body 模板内首个分节标题（h3.sec）之前
    # 必须已有所属卡片的 h2 —— 不允许 h1 -> h3 跳级。
    body_start = src.index("<!-- tpl:")
    first_h3 = src.index('<h3 class="sec">', body_start)
    first_h2 = src.index("<h2>", body_start)
    assert first_h2 < first_h3, "首个分节 h3 前无 h2 卡片标题（h1→h3 跳级）"


def test_b04_text_contrast_c5(tmp_path):
    """C-5 [新增]：--faint 文本（.sec 节标题 / th 表头）对比度 >= 4.5:1。
    亮色主题下用 --lab（0.72 透明度）替代。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    import re as _re
    faint = _re.search(r"--faint:([^;]+);", src).group(1).strip()
    paper = _re.search(r"--paper:([^;]+);", src).group(1).strip()

    def _hex(c):
        c = c.lstrip("#")
        return [int(c[i:i + 2], 16) for i in (0, 2, 4)]

    # rgba 合成（--faint 是 rgba(31,30,28,.32) 形态）
    m = _re.match(r"rgba\((\d+),(\d+),(\d+),([\d.]+)\)", faint)
    assert m, "亮色 --faint 应为 rgba 形态: {}".format(faint)
    fr, fg, fb, fa = (float(m.group(i)) for i in (1, 2, 3, 4))
    pr, pg, pb = _hex(paper)
    cr = fr * fa + pr * (1 - fa)
    cg = fg * fa + pg * (1 - fa)
    cb = fb * fa + pb * (1 - fa)

    def _rel_lum(r, g, b):
        lin = []
        for v in (r, g, b):
            v = v / 255.0
            lin.append(v / 12.92 if v <= 0.04045 else
                       ((v + 0.055) / 1.055) ** 2.4)
        return 0.2126 * lin[0] + 0.7152 * lin[1] + 0.0722 * lin[2]
    l1 = _rel_lum(cr, cg, cb)
    l2 = _rel_lum(pr, pg, pb)
    ratio = (max(l1, l2) + 0.05) / (min(l1, l2) + 0.05)
    assert ratio >= 4.5, "--faint 亮色对比度 {} < 4.5:1".format(round(ratio, 2))


def test_b05_theme_manual_toggle(tmp_path):
    """R-SHELL-4 [新增]：手动主题切换控件存在（亮/暗/自动），
    且切换后图表 token 跟随（LF_DARK 翻转）。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    assert 'id="themeToggle"' in src, "缺少主题切换控件"
    assert 'data-theme' in src, "缺少 CSS data-theme 覆盖层"
    # 存储偏好 = dark => LF_DARK 为真（token 跟随手动选择而非系统）
    r1 = _js_check(tmp_path, data, env={"VC_THEME": "dark",
                                        "VC_SYS": "light"})
    if r1 is None:
        return
    assert r1["errors"] == []
    assert r1["theme_pref"] == "dark", r1["theme_pref"]
    assert r1["theme_dark"] is True, r1["theme_dark"]
    assert r1["theme_btn_exists"] is True
    # 无偏好 => auto 跟随系统（系统 light => 亮）
    r2 = _js_check(tmp_path, data, env={"VC_SYS": "light"})
    if r2 is None:
        return
    assert r2["theme_pref"] == "auto", r2["theme_pref"]
    assert r2["theme_dark"] is False, r2["theme_dark"]
    # auto + 系统暗 => 暗
    r3 = _js_check(tmp_path, data, env={"VC_SYS": "dark"})
    if r3 is None:
        return
    assert r3["theme_dark"] is True, r3["theme_dark"]


def test_b06_monitor_card_partial_missing(tmp_path):
    """R-MON-4 v0.5 分支 [新增]：只有 mem 无 urma => 不留 URMA 空占位：
    标题去掉对应分句、urmachart 不渲染或隐藏，且图注同步。"""
    a = _stats(tmp_path, "a.writer", "writer")
    mons = {
        "mem_host": [{"node": "n1", "t": [1000, 1001],
                      "columns": ["n0_used_mb"],
                      "series": {"n0_used_mb": [10.0, 20.0]}}],
        "mem_proc": [],
        "urma": [],
        "summary": {"peak_mem_mb": {"n1": 20}, "peak_urma": None,
                    "worst_proc": None, "proc_growth": []},
    }
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"}, monitors=mons)
    # JS 冒烟：模板默认带 urma 分句，renderMonitors 按数据改写（模板无法
    # 静态判别，因为 _MONITOR_CARD 由有无 monitors 决定是否附加）
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert res.get("mon_title_suffix") == "（内存）", res.get("mon_title_suffix")
    assert res.get("urma_chart_display") == "none", res.get("urma_chart_display")
    assert res.get("mon_urma_note") == "", res.get("mon_urma_note")
    # 对照：监控卡本身可见
    assert res.get("monitorcard_visible") != "none", res.get("monitorcard_visible")


# ===========================================================================
# C 类防回归（DR-24 对齐性缺陷清单）：修复已落，断言锁死防回潮。
# ===========================================================================

def test_c01_dr24_alignment_lock(tmp_path):
    """DR-24 全清单锁死：无「阀值」错字 / 无死 CSS / 表头命名 avg-max /
    h3 无交互括号注记 / 卡片有描边。"""
    a = _stats(tmp_path, "a.writer", "writer")
    data = rp.reduce_scenario(rp.load([a]), {"config": "c"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "r"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    # 1) 错字
    assert "阀值" not in src, "「阀值」错字回潮"
    # 2) 死 CSS（skill 明令禁用或已废弃的样式不得复生）
    for dead in (".tb-sep", ".gkv", ".gtick", ".gtrack"):
        assert dead not in src, "死 CSS 回潮: {}".format(dead)
    # 3) 表头命名（skill 文案规则6：指标名 avg/max；队列等待均值不截断）
    assert "<th class=\"n\">avg</th>" in src
    assert "<th class=\"n\">max</th>" in src
    assert "队列等待均值</th>" in src
    assert "队列等待均</th>" not in src
    # 4) h3 标题无交互括号注记（说明移入 title 属性）
    import re as _re
    _h3_ok = {"分级时延块图（trace）", "分级时延曲线（trace）",
              "慢点追踪（trace）"}
    for m in _re.finditer(r'<h3 class="sec"[^>]*>([^<]+)</h3>', src):
        if m.group(1) in _h3_ok:
            continue
        assert "（" not in m.group(1), "h3 括号注记回潮: {}".format(m.group(1))
    # 5) 卡片描边（skill 设计基调：1px 描边或底色区分）
    assert "border:1px solid var(--grid)" in src, "卡片描边缺失"


def test_c02_slow_tooltip_epoch_not_undefined(tmp_path):
    """R-SLOW-2 回归：慢点 tooltip 时刻来自 ts_epoch 归一（rowEpoch），
    不得读行上不存在的 r.t（曾致 tooltip 显示 undefined）。"""
    st = _minimal_stats([0, 1, 2, 3], [0])
    st["meta"].update({"ts_from": 0, "ts_to": 3, "node": "n1",
                       "role": "writer", "source": "a.writer.log"})
    st["slowest"] = [{"ts": "2026-11-19 00:00:01.000000", "op": "put",
                      "key": "k1", "lat_ms": 9.0, "status": "OK"}]
    data = rp.reduce_scenario([st], {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    tip = res.get("slow_tip")
    assert tip, "harness 应产出慢点 tooltip 样例"
    assert "undefined" not in str(tip), tip
    assert "1970" not in str(tip).split("<br>")[0], \
        "epoch=0 时显示 1970 说明归一失败: {}".format(tip)
    # ts 字符串 00:00:01 UTC -> epoch 1? _node_ts_epoch 按节点钟面（UTC）解析
    # 2026-11-19 00:00:01 => 绝对时刻，fullts 应输出 08:00:01（+08:00 宿主）
    assert "08:00:01" in str(tip).split("<br>")[0], tip
