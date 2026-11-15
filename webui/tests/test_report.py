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
    assert set(z["series"]) == {"writer", "reader"}
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


def test_embed_step_switch(tmp_path):
    """>1 天 1s 桶 -> 内嵌 1 分钟粒度。"""
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
                    for t in range(0, 90001, 1)],
        "minutes": [[t, 1, 1, 0, 1.0, 2.0, 0.0, 0.0, {"30": 1}]
                    for t in range(0, 90001, 60)],
    }
    z = rp.reduce_size([st], {"config": "c"})
    assert z["embed_step"] == 60
    st["meta"]["ts_to"] = 3600
    st["seconds"] = st["seconds"][:3600]
    z2 = rp.reduce_size([st], {"config": "c"})
    assert z2["embed_step"] == 1


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
    rc = bm.rebuild_reports("20261112-1000")
    assert rc == 0
    assert (logs / "report" / "report.html").is_file()
    assert (tmp_path / "20261112-1000" / "report" / "overview.html").is_file()
    # missing run -> non-zero
    assert bm.rebuild_reports("nope") == 2


# ---------------------------------------------------------------------------
# C++ SDK trace integration (reduce + report section)
# ---------------------------------------------------------------------------

GLOG = "I20261112 10:23:41.000000 12345 te.cc:88] "
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
    assert 'id="tracecard"' in src and 'id="tstack"' in src and 'id="tchart"' in src
    assert "阶段分解" in src and "残差（total − 阶段和）" in src
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
    """无 bucket_unit 的旧 stats（minutes.t 是 epoch 分钟）合并时需 ×60 兼容。"""
    old = _minimal_stats([3600, 3660], [60, 61])
    z = rp.reduce_size([old], {"config": "c"})
    assert z["series"]["writer"]["step60"]["t"] == [3600, 3660]
    new = _minimal_stats([3600, 3660], [3600, 3660], bucket_unit="s")
    z2 = rp.reduce_size([new], {"config": "c"})
    assert z2["series"]["writer"]["step60"]["t"] == [3600, 3660]


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


def test_granularity_buttons_match_series(tmp_path):
    """默认应画 1s 系列；点「1 分钟粒度」才切到分钟系列（曾整体颠倒）。"""
    st = _minimal_stats([3600 + i for i in range(60)], [3600], bucket_unit="s")
    data = rp.reduce_scenario([st], {"config": "c"})
    res = _js_check(tmp_path, data)
    if res is None:
        return
    assert res["errors"] == []
    assert res["init_len"] == 60          # 默认 1 秒粒度
    assert res["init_x0"] == 3600 * 1000  # x 轴用 epoch 毫秒
    assert res["min_len"] == 1            # 切到 1 分钟粒度


def test_long_run_embeds_minutes_only(tmp_path):
    """>1 天的 run：HTML 内嵌只留 1min（1s 仍完整留在 data.js）。"""
    st = _minimal_stats([3600, 3660, 3720], [3600], bucket_unit="s")
    st["meta"]["ts_from"] = 0
    st["meta"]["ts_to"] = 90000          # 25h 跨度，但只有 3 个桶
    data = rp.reduce_scenario([st], {"config": "long"})
    assert data["sizes"]["8MB"]["embed_step"] == 60
    out = str(tmp_path / "report")
    html, dj = rp.render_scenario(data, out, ECHARTS, topk=5)
    import re as _re
    emb = json.loads(_re.search(r"window\.__MC_REPORT__ = (.*?);</script>",
                                open(html, encoding="utf-8").read(), _re.S).group(1))
    series = emb["sizes"]["8MB"]["series"]["writer"]
    assert series["step1"]["t"] == []            # 1s 不内嵌
    assert len(series["step60"]["t"]) == 1
    full = json.loads(open(dj, encoding="utf-8").read()[
        len("window.__MC_FULL__ = "):].rstrip().rstrip(";"))
    assert len(full["sizes"]["8MB"]["series"]["writer"]["step1"]["t"]) == 3


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
