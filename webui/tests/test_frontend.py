#!/usr/bin/env python3
"""前端回归测试：防止 app.js 引用未定义的组件常量导致整页白屏。"""
import os
import re
import shutil
import subprocess

HERE = os.path.dirname(os.path.abspath(__file__))
JS_DIR = os.path.join(os.path.dirname(HERE), "web", "js")


def _js_files():
    for root, _dirs, files in os.walk(JS_DIR):
        for f in files:
            if f.endswith(".js"):
                yield os.path.join(root, f)


def test_component_identifiers_declared():
    """app.js 里 app.component("<name>", Ident) 的 Ident 必须在某个 JS 中声明。"""
    with open(os.path.join(JS_DIR, "app.js"), encoding="utf-8") as f:
        app_src = f.read()
    idents = re.findall(r'app\.component\(\s*"[^"]+"\s*,\s*([A-Za-z_$][\w$]*)\s*\)',
                        app_src)
    assert idents, "no app.component(name, ident) found in app.js"
    all_src = "\n".join(open(p, encoding="utf-8").read() for p in _js_files())
    missing = [i for i in idents
               if not re.search(r"(?:const|let|var|function)\s+" + re.escape(i) + r"\b",
                                all_src)]
    assert not missing, "undefined component identifiers: {}".format(missing)


def test_frontend_boot_check():
    """Node VM 顺序执行脚本，捕获顶层 ReferenceError / 未定义组件注册。"""
    node = shutil.which("node")
    if not node:
        print("  [skip] node not found")
        return
    harness = os.path.join(HERE, "frontend_boot_check.js")
    r = subprocess.run([node, harness], capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, (r.stdout + r.stderr)


def test_config_tree_checkbox_cascades():
    """目录复选框勾选/取消必须级联整棵子树，而不是只改自身。"""
    src = open(os.path.join(JS_DIR, "views", "perf-view.js"), encoding="utf-8").read()
    start = src.index("function _perfSetSubtree(")
    end = src.index("}", src.index("node.checked = v;", start)) + 1
    body = src[start:end]
    assert "node.checked = v;" in body
    assert "(node.children || []).forEach" in body
    assert 'setSubtree: (v) => _perfSetSubtree(props.node, v)' in src
    # 目录复选框绑定级联处理器；不能再直接写 node.checked = v
    assert '@update:model-value="setSubtree"' in src


def test_log_wrap_uses_switch():
    """自动换行是开关型控件（el-switch + 文字），不是独立按钮（实例详情页）。"""
    src = open(os.path.join(JS_DIR, "views", "instance-view.js"),
               encoding="utf-8").read()
    assert '<el-switch v-model="logWrap"' in src
    assert "log-wrap-label" in src
    assert ">自动换行</el-button>" not in src


def test_perf_page_is_instance_management():
    """性能测试页：运行框只剩「创建实例」+ CLEAN-UP，下方是实例卡片列表。"""
    src = open(os.path.join(JS_DIR, "views", "perf-view.js"),
               encoding="utf-8").read()
    assert "创建实例" in src
    assert "清理远端" in src and '@click="cleanup"' in src
    assert ">RUN</el-button>" not in src
    assert "当前 RUN" not in src
    assert 'v-for="it in instances"' in src
    assert "runInstance(it)" in src and "stopInstance(it)" in src
    assert "deleteInstance(it)" in src and "openInstance(it)" in src
    assert "/api/instances" in src and "/run" in src and "/stop" in src


def test_instance_detail_tabs_and_split_log():
    """实例详情页：浏览器式页签 + 组合实例 rank/recall 上下分屏 + 跳转结果。"""
    src = open(os.path.join(JS_DIR, "views", "instance-view.js"),
               encoding="utf-8").read()
    assert "inst-tabs" in src and "closeTab" in src
    assert 'stream="rank"' in src and 'stream="recall"' in src
    assert 'stream="all"' in src and "合并日志" in src
    assert "跳转至结果详情" in src
    # nested ts_dir -> top-level parent output (results view lists rank/recall)
    assert "return rel.split(\"/\")[0];" in src


def test_csv_header_single_line_width():
    """CSV 预览列宽必须按表头长度估算（否则长表头被压成两行）。"""
    src = open(os.path.join(JS_DIR, "views", "results-view.js"),
               encoding="utf-8").read()
    assert "function _csvColWidth(" in src
    assert ':min-width="csvColWidth(h)"' in src
    assert "csvColWidth: _csvColWidth" in src


def test_missing_file_falls_back_to_detail_csv():
    """切换 run 时无同名文件 -> 自动展示 benchmark_detail.csv + toast。"""
    src = open(os.path.join(JS_DIR, "views", "results-view.js"),
               encoding="utf-8").read()
    assert "function _defaultDetail(files)" in src
    assert 'f === "rank/benchmark_detail.csv"' in src
    assert "已自动展示 benchmark_detail.csv" in src


def test_reports_in_results_and_instance_detail():
    """HTML 报表：结果页「报表」分组 + 新标签打开；实例详情「查看报表」。"""
    results = open(os.path.join(JS_DIR, "views", "results-view.js"),
                   encoding="utf-8").read()
    inst = open(os.path.join(JS_DIR, "views", "instance-view.js"),
                encoding="utf-8").read()
    assert 'base.endsWith(".html")' in results
    assert '"报表"' in results
    assert "openReport" in results and '"_blank"' in results
    assert "在新标签页打开报表" in results
    assert "查看报表" in inst
    assert "report/overview.html" in inst
    assert "report\\/report\\.html" in inst   # regex form in openReport


def test_shared_status_module():
    """状态中文化、相对时间与危险确认统一在 status.js。"""
    src = open(os.path.join(JS_DIR, "status.js"), encoding="utf-8").read()
    for key in ("running", "done", "failed", "stopped", "created"):
        assert key in src
    assert "运行中" in src and "已完成" in src and "未运行" in src
    assert "function statusText" in src and "function statusType" in src
    assert "function fmtTimeRel" in src and "function confirmDanger" in src
    assert "confirmButtonClass: \"el-button--danger\"" in src


if __name__ == "__main__":
    test_component_identifiers_declared()
    test_frontend_boot_check()
    print("PASS test_frontend")
