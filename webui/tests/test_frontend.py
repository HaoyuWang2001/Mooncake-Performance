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
    """自动换行是开关型控件（el-switch + 文字），不是独立按钮。"""
    src = open(os.path.join(JS_DIR, "views", "perf-view.js"), encoding="utf-8").read()
    assert '<el-switch v-model="logWrap"' in src
    assert 'log-wrap-label' in src
    assert '>自动换行</el-button>' not in src


if __name__ == "__main__":
    test_component_identifiers_declared()
    test_frontend_boot_check()
    print("PASS test_frontend")
