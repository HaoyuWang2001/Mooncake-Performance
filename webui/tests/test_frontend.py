#!/usr/bin/env python3
"""前端回归测试：防止 app.js 引用未定义的组件常量导致整页白屏。"""
import json
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


def test_instance_log_fullscreen_dialog():
    """实例日志：行内「全屏」按钮 -> fullscreen 弹窗，内含 SD 日志终端那套
    （工具栏 + 搜索 + 自动滚动 + 换行），关闭即销毁轮询。"""
    src = open(os.path.join(JS_DIR, "views", "instance-view.js"),
               encoding="utf-8").read()
    # 行内入口：图标按钮（仅图标 -> aria-label）+ 未运行禁用的解释 tooltip
    assert 'aria-label="全屏查看日志"' in src
    assert "该实例尚未运行，无日志可全屏查看" in src
    # EP 原生全屏弹窗，关闭释放组件（停止轮询）
    assert " fullscreen destroy-on-close append-to-body" in src
    assert 'class="ilog-fs-dialog"' in src
    # 同一组件复用：弹窗内是 fs 形态的 instance-log-pane
    assert "fs: { type: Boolean, default: false }" in src
    assert '<instance-log-pane v-if="fsOpen" fs :rid="rid" :stream="stream"' in src
    # SD 日志终端功能集：清屏 / 下载 / 自动滚动 / 仅显示匹配行 / 首个 / 最后一个
    for t in ("清屏", "下载", "自动滚动", "仅显示匹配行", "首个", "最后一个"):
        assert t in src, "missing fullscreen log control {}".format(t)
    assert "autoScroll" in src and "filterOnly" in src
    assert "function firstMatch" in src and "function lastMatch" in src

    css = open(os.path.join(os.path.dirname(JS_DIR), "style.css"),
               encoding="utf-8").read()
    assert ".ilog-fs-dialog .el-dialog__body" in css
    assert ".ilog-fs-dialog .ilog-view" in css


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


def test_results_run_list_collapse_and_width():
    """统计结果左列：可收起/展开 + 可拖宽（持久化）+ 完整文件名 tooltip。"""
    results = open(os.path.join(JS_DIR, "views", "results-view.js"),
                   encoding="utf-8").read()
    app = open(os.path.join(JS_DIR, "app.js"), encoding="utf-8").read()
    assert "function toggleList" in results and "toggleList" in results
    assert "listOpen ? 'Fold' : 'Expand'" in results
    assert "mcweb-results-list-open" in results
    assert '<hdrag-handle v-show="listOpen" v-model="listW"' in results
    assert 'persist="mcweb-results-list-w"' in results
    assert "const listW = Vue.ref(" in results
    assert ':title="r.ts"' in results
    # 水平拖拽手柄组件在 app.js 定义并注册
    assert "const HDragHandle = {" in app
    assert 'app.component("hdrag-handle", HDragHandle)' in app


def test_results_run_more_menu():
    """运行结果行尾「…」菜单（重建报表/改名/删除），不再 hover 直出三按钮。"""
    results = open(os.path.join(JS_DIR, "views", "results-view.js"),
                   encoding="utf-8").read()
    assert 'aria-label="更多操作"' in results
    assert "MoreFilled" in results
    assert 'popper-class="ri-more-menu"' in results
    assert "function onRunCommand" in results
    for cmd in ('command="rebuild"', 'command="rename"', 'command="delete"'):
        assert cmd in results, "missing menu command {}".format(cmd)
    assert "ri-actions" not in results            # hover 三按钮已移除
    css = open(os.path.join(os.path.dirname(JS_DIR), "style.css"),
               encoding="utf-8").read()
    assert ".run-item:hover .ri-actions" not in css
    assert ".ri-more-menu .danger-item" in css    # 删除项危险色（teleport 后全局类）


def test_results_ms_bar_compact_no_wrap():
    """多选条：已选计数 12px 等宽数字，整行不换行。"""
    results = open(os.path.join(JS_DIR, "views", "results-view.js"),
                   encoding="utf-8").read()
    assert '<span class="hint mc-num">已选 {{ selected.length }}</span>' in results
    css = open(os.path.join(os.path.dirname(JS_DIR), "style.css"),
               encoding="utf-8").read()
    m = re.search(r"\.ms-bar \{[^}]*\}", css)
    assert m, ".ms-bar rule missing"
    assert "flex-wrap: nowrap" in m.group(0)
    assert ".ms-bar .hint { font-size: 12px" in css
    assert ".ms-bar .spacer { flex: 1; min-width: 0; }" in css


def test_perf_param_panel_from_selected_configs():
    """性能测试：选中 cluster/global/profile 后直接展示关键启动参数。"""
    src = open(os.path.join(JS_DIR, "views", "perf-view.js"),
               encoding="utf-8").read()
    assert "启动参数" in src
    assert "paramGroups" in src and "paramDocs" in src
    assert "function loadParamDocs" in src and "function _loadParamDoc" in src
    assert "function _perfEffectiveProto" in src
    assert "el-descriptions" in src and 'class="rb-params"' in src
    # 刷新配置按钮清缓存后重载
    assert "function refreshConfigs" in src and "@click=\"refreshConfigs\"" in src


def test_perf_form_persisted_across_refresh_and_restart():
    """性能测试页：mode/启动方式/选项/勾选配置 存 localStorage，刷新与重启后恢复。"""
    src = open(os.path.join(JS_DIR, "views", "perf-view.js"),
               encoding="utf-8").read()
    assert 'const FORM_KEY = "mcweb-perf-form"' in src
    assert "function _loadPerfForm" in src and "function _savePerfForm" in src
    assert "function _scheduleSave" in src
    assert "_selRestored" in src and "_savedSel" in src
    assert "let servedDefaults = !!_savedForm;" in src


def test_log_surfaces_scroll_horizontally():
    """不自动换行时长行必须能横向滚动（结果页 / 实例日志 / 全屏弹窗）。"""
    css = open(os.path.join(os.path.dirname(JS_DIR), "style.css"),
               encoding="utf-8").read()
    m = re.search(r"\.pane-body \{[^}]*\}", css)
    assert m and "overflow: auto;" in m.group(0), \
        "results pane-body must scroll both axes"
    assert ".ilog-view { height: 300px; min-height: 220px; overflow: auto;" in css
    # 不换行日志：行按内容宽度撑开（content-visibility 关闭），横向滚动可靠
    assert ".logview.log-nowrap > div" in css
    assert "width: max-content" in css
    # JSON 不再自建滚动容器（横向滚动条始终在 pane-body 边缘可达）
    m = re.search(r"\.pane-body \.json \{[^}]*\}", css)
    assert m and "overflow: auto" not in m.group(0)
    results = open(os.path.join(JS_DIR, "views", "results-view.js"),
                   encoding="utf-8").read()
    # 默认换行（与实例详情页一致），关闭后才依赖横向滚动
    assert 'localStorage.getItem("mcweb-wrap") !== "0"' in results


def test_instances_card_keeps_min_height():
    """实例卡片保底 280px：空态→1 个实例时不坍缩。"""
    css = open(os.path.join(os.path.dirname(JS_DIR), "style.css"),
               encoding="utf-8").read()
    assert ".inst-empty, .inst-list { min-height: 280px;" in css


def test_config_chain_dialog():
    """配置维护：打开配置展示各级 _configs.json + 生效配置；_configs.json 需显式编辑。"""
    src = open(os.path.join(JS_DIR, "views", "config-view.js"),
               encoding="utf-8").read()
    assert "/api/config/chain" in src
    assert "配置继承链" in src and "生效" in src
    assert "function _loadChain" in src and "function editChainLevel" in src
    assert "function backToRoot" in src and "editingFragment" in src
    assert "cfg-chain-pre" in src and "cfg-dlg-body" in src
    # 未保存修改时禁止切换编辑目标（disabled 有 tooltip 解释）
    assert ':disabled="dirty"' in src
    assert "先保存或取消当前修改，再切换编辑目标" in src
    # 近全屏弹窗（保留 24px 边距）
    assert 'width="96%" top="24px"' in src
    css = open(os.path.join(os.path.dirname(JS_DIR), "style.css"),
               encoding="utf-8").read()
    for cls in (".cfg-dlg-body", ".cfg-chain", ".cfg-chain-item.self",
                ".cfg-chain-pre", ".cfg-editor-pane"):
        assert cls in css, "missing style {}".format(cls)
    assert ".cfg-dlg {" in css and "height: calc(100vh - 48px)" in css
    assert ".cfg-editor-pane .editor .el-textarea__inner" in css


def test_instance_reconnect_after_webui_restart():
    """实例详情：重连运行（reattach）+ 日志断线续接（resync / 连接断开提示）。"""
    src = open(os.path.join(JS_DIR, "views", "instance-view.js"),
               encoding="utf-8").read()
    assert "重连运行" in src and "/reattach" in src
    assert "function reconnectRun" in src and "function autoReattach" in src
    assert ":resync=\"syncTick\"" in src and ":resync=\"resync\"" in src
    assert "function reconnect()" in src and "connLost" in src
    assert "连接断开" in src
    assert 'Vue.watch(() => props.live' in src      # 状态回到 running 自动续接
    css = open(os.path.join(os.path.dirname(JS_DIR), "style.css"),
               encoding="utf-8").read()
    assert ".ilog-conn" in css


def test_instance_report_button_opens_overview():
    """查看报表：单任务直开本 run 的 overview.html；rank+recall 直开父级
    聚合总览（内含两侧全部单元报表链接），旧 run 回退 rank 侧。"""
    src = open(os.path.join(JS_DIR, "views", "instance-view.js"),
               encoding="utf-8").read()
    assert "function _reportLoc(" in src and "function _pickReport(" in src
    assert "function openCombinedReport(" in src
    assert '@click="openCombinedReport">查看报表' in src
    assert '@click="openReport">查看报表' in src   # 单任务仍走直开
    assert "report/overview.html" in src
    assert "openRoleReport" not in src


def test_report_picker_pure_logic():
    """_reportLoc/_pickReport 纯逻辑（root/base 拆分 + 角色隔离 + 单任务兜底）。"""
    node = shutil.which("node")
    if not node:
        print("  [skip] node not found")
        return
    path = os.path.join(JS_DIR, "views", "instance-view.js")
    script = r"""
const fs = require("fs"), vm = require("vm");
const ctx = { Vue: {}, console };
vm.createContext(ctx);
vm.runInContext(fs.readFileSync(process.argv[1], "utf8") +
  ";globalThis.L=_reportLoc;globalThis.P=_pickReport;", ctx);
const L = ctx.L, P = ctx.P;
const files = [
  "rank/report/overview.html", "rank/report/report.html",
  "recall/report/overview.html", "report/overview.html",
];
const out = {
  loc_parent: L("outputs/20260101-0000"),
  loc_child: L("outputs/20260101-0000/rank"),
  single: P(files, "", false),
  rank: P(files, "rank/", true),
  recall: P(files, "recall/", true),
  strict_missing: P(["rank/report/overview.html"], "recall/", true),
  fallback_report: P(["report/report.html"], "", false),
  none: P([], "", false),
};
console.log(JSON.stringify(out));
"""
    r = subprocess.run([node, "-e", script, path],
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stdout + r.stderr
    out = json.loads(r.stdout)
    assert out["loc_parent"] == {"root": "20260101-0000", "base": ""}
    assert out["loc_child"] == {"root": "20260101-0000", "base": "rank"}
    assert out["single"] == "report/overview.html"
    assert out["rank"] == "rank/report/overview.html"
    assert out["recall"] == "recall/report/overview.html"
    assert out["strict_missing"] == ""
    assert out["fallback_report"] == "report/report.html"
    assert out["none"] == ""


def test_instance_snapshot_labels():
    """实例卡片/详情显示配置快照数量，说明之后改配置不影响。"""
    perf = open(os.path.join(JS_DIR, "views", "perf-view.js"),
                encoding="utf-8").read()
    inst = open(os.path.join(JS_DIR, "views", "instance-view.js"),
                encoding="utf-8").read()
    assert "快照 {{ it.snapshot.files }}" in perf
    assert "之后修改配置文件不影响该实例" in perf
    assert "detail.instance.snapshot" in inst
    assert "配置快照 {{ detail.instance.snapshot.files }}" in inst
    assert "fmtTimeAbs" in inst


def test_instance_snapshot_viewer_ui():
    """实例详情「查看快照」：清单分组 + 实时对比标签 + 内容查看；卡片标签可直达。"""
    inst = open(os.path.join(JS_DIR, "views", "instance-view.js"),
                encoding="utf-8").read()
    assert "查看快照" in inst
    assert "/snapshot" in inst and "/snapshot/file" in inst
    assert "snapGroups" in inst and "openSnapshotById" in inst
    assert "selectSnapFile" in inst and "copySnapshot" in inst
    assert "已变化" in inst and "实时已删除" in inst
    assert "自动换行" in inst
    perf = open(os.path.join(JS_DIR, "views", "perf-view.js"),
                encoding="utf-8").read()
    assert "openSnapshot(it)" in perf and "store.snapshotTarget" in perf
    app = open(os.path.join(JS_DIR, "app.js"), encoding="utf-8").read()
    assert "snapshotTarget" in app
    css = open(os.path.join(os.path.dirname(JS_DIR), "style.css"),
               encoding="utf-8").read()
    for cls in (".snap-list", ".snap-item.active", ".snap-pre", ".snap-path",
                ".snap-sel"):
        assert cls in css, "missing style {}".format(cls)


def test_shared_status_module():
    """状态中文化、相对时间与危险确认统一在 status.js。"""
    src = open(os.path.join(JS_DIR, "status.js"), encoding="utf-8").read()
    for key in ("running", "done", "failed", "stopped", "created"):
        assert key in src
    assert "运行中" in src and "已完成" in src and "未运行" in src
    assert "function statusText" in src and "function statusType" in src
    assert "function fmtTimeRel" in src and "function confirmDanger" in src
    assert "confirmButtonClass: \"el-button--danger\"" in src


def test_nodes_view_registered_and_has_sd_form():
    """集群节点页：菜单项 + 视图注册 + SD 集群节点页的关键交互。"""
    app = open(os.path.join(JS_DIR, "app.js"), encoding="utf-8").read()
    assert 'index="nodes"' in app and "<span>集群节点</span>" in app
    assert 'nodes: "nodes-view"' in app
    assert 'app.component("nodes-view", NodesView)' in app
    assert '"nodes"' in app            # VIEW_KEYS 持久化
    html = open(os.path.join(os.path.dirname(JS_DIR), "index.html"),
                encoding="utf-8").read()
    assert "views/nodes-view.js" in html

    src = open(os.path.join(JS_DIR, "views", "nodes-view.js"),
               encoding="utf-8").read()
    # 与 Services-Deployment 集群节点页同形的交互
    assert "全部测试连接" in src and "端口查询" in src
    assert "监听端口" in src and "多节点占用对照" in src
    assert "/api/nodes" in src and "/test" in src and "/ports" in src
    # 服务探测展示（master / SP 进程与应有端口）
    assert "mooncake 服务" in src and "应有端口" in src
    assert "el-empty" in src


def test_global_polling_and_sidebar_badges():
    """统一 API 操作显示：全局轮询实例/运行 + 侧边栏徽标。"""
    app = open(os.path.join(JS_DIR, "app.js"), encoding="utf-8").read()
    assert "store.instances" in app and "store.runs" in app
    assert "refreshInstances" in app and "refreshRuns" in app
    assert "setInterval(tick, 3000)" in app
    assert 'class="menu-badge"' in app
    assert "instTotal" in app and "instRunning" in app and "schedState" in app
    # 性能测试页不再自行轮询实例（共享 store 接替）
    perf = open(os.path.join(JS_DIR, "views", "perf-view.js"),
                encoding="utf-8").read()
    assert "store.instances" in perf and "store.runs" in perf
    assert "setInterval(loadInstances" not in perf
    assert "busy[it.id]" in perf


if __name__ == "__main__":
    test_component_identifiers_declared()
    test_frontend_boot_check()
    print("PASS test_frontend")
