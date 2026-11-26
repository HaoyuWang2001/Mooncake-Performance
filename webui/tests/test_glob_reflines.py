#!/usr/bin/env python3
"""端到端时延图的全局统计横线（R-LAT-9）。

六条水平参考线 = 当前角色 role_summaries 的正式窗口全局值；默认仅
avg/p99/p9999 可见；同指标颜色 + 虚线；图例在「全局横线」按钮组；仅
端到端图绘制（分级时延不绘制——同构原则的显式例外）。
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import report as rp  # noqa: E402


def _src():
    return open(os.path.join(ROOT, "scripts", "report.py"),
                encoding="utf-8").read()


def test_glob_lines_defined_with_expected_defaults():
    src = _src()
    assert "const GLOB_LINES=" in src
    for k in ("avg", "p50", "p95", "p99", "p999", "p9999"):
        assert "['{}','{}']".format(k, k) in src
    # 默认仅 avg / p99 / p9999
    assert ("const globShown={avg:true,p50:false,p95:false,p99:true,"
            "p999:false,p9999:true};") in src


def test_global_ref_series_builder():
    src = _src()
    assert "function globalRefLines(x0,x1)" in src
    # 同指标颜色 + 虚线 + 沉于分桶曲线下方（z:2）+ 不产生端点标签
    assert "lineStyle:{width:1,type:'dashed',color:latColor(k),opacity:0.9}" in src
    assert "itemStyle:{color:latColor(k)}, z:2" in src
    assert "name:'全局'+L[1]" in src
    # 读取当前角色的 role_summaries（写/读分开）
    assert "function globRoleSummary()" in src
    assert "rs[curRole]" in src
    assert "k+'_ms'" in src


def test_legend_group_and_default_toggle():
    src = _src()
    assert "function renderGlobSel()" in src
    assert 'id="globSel"' in src
    assert "<span class=\"tl-label\" title=\"仅端到端时延图" in src
    assert "globShown[k]=!globShown[k]" in src


def test_mounted_on_main_chart_only():
    src = _src()
    # 挂载点：buildChart（端到端图），且取首条「线」曲线的 x 域
    assert "const gl=globalRefLines(_d0[0][0],_d0[_d0.length-1][0]);" in src
    # 分级时延（阶段图）不挂载全局横线
    stage = src[src.index("function buildStageChart"):]
    stage = stage[:stage.index("function ", 10)]
    assert "globalRefLines" not in stage


def test_tooltip_shows_visible_global_rows():
    src = _src()
    assert "rows.push(['全局 '+L[1]+'(ms)'" in src
    assert "if(!globShown[L[0]]) return;" in src


def test_skill_and_requirements_document_exception():
    skill = open(os.path.join(ROOT, ".opencode", "skills", "report-design",
                              "SKILL.md"), encoding="utf-8").read()
    assert "全局统计横线仅存在于端到端时延图" in skill
    req = open(os.path.join(ROOT, "docs", "spec", "report",
                            "REQUIREMENTS.md"), encoding="utf-8").read()
    assert "R-LAT-20" in req and "全局统计横线" in req
    assert "DR-52" in req


def test_global_line_values_equal_role_summary(tmp_path):
    """横线取值 = 当前角色 role_summaries 的全局值（不随对象变化）。"""
    import json
    from types import SimpleNamespace

    def _stats(name, role, node, lat):
        st = {
            "meta": {"source": name + ".log", "role": role, "node": node,
                     "size": "8MB", "ts_from": 1000.0, "ts_to": 1030.0,
                     "bucket_unit": "s", "rows": 100, "bytes": 0,
                     "bucket_step": 1, "hist_version": 2},
            "global": {"total": 100, "ok": 100, "fail": 0, "avg_ms": lat,
                       "min_ms": lat, "max_ms": lat * 2, "p50_ms": lat,
                       "p90_ms": lat, "p95_ms": lat * 1.1,
                       "p99_ms": lat * 1.2, "p999_ms": lat * 1.5,
                       "p9999_ms": lat * 1.9, "ok_rows": 100},
            "histogram": {"1": 100}, "ops": {}, "summaries": [
                ("summary", {"wall_s": 30.0, "bytes": 0, "max_depth": 0})],
            "seconds": [[1000, 100, 100, 0, lat * 100, 2 * lat, 0, 0,
                         {"1": 100}, 0],
                        [1010, 100, 100, 0, lat * 100, 2 * lat, 0, 0,
                         {"1": 100}, 0]],
            "errors": {}, "slowest": [], "anomalies": [], "outliers": [],
            "abnormal": [],
        }
        return st

    logs = tmp_path / "logs"
    logs.mkdir()
    data = rp.reduce_scenario(
        [_stats("a", "writer", "n1", 1.0), _stats("b", "reader", "n2", 2.0)],
        {"config": "c"})
    sys.path.insert(0, str(tmp_path))
    import test_report as tr
    res = tr._js_check(tmp_path, data, "--add-obj", "node:n2")
    if res is None:
        return
    gl = {g["name"]: g for g in res.get("globals") or []}
    # 默认三条：avg / p99 / p9999
    assert set(gl) == {"全局avg", "全局p99", "全局p9999"}, gl
    # curRole 默认 reader -> 取值来自 reader 的 role_summaries
    rr = data["sizes"]["8MB"]["role_summaries"]["reader"]
    assert abs(gl["全局avg"]["y"] - rr["avg_ms"]) < 1e-9
    assert abs(gl["全局p99"]["y"] - rr["p99_ms"]) < 1e-9
    assert abs(gl["全局p9999"]["y"] - rr["p9999_ms"]) < 1e-9
    # 水平线：两端同值、虚线、横跨当前 x 域
    for g in gl.values():
        assert g["dash"] and g["x1"] > g["x0"]
