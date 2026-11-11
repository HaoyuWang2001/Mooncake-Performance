#!/usr/bin/env python3
"""webui-design 规范 lint：token 化、字号阶梯、动效/无障碍、状态中文化。

这些是静态断言，保证改动不会悄悄退回散落 hex / 魔法字号 / 英文状态。
"""
import os
import re

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
WEB = os.path.join(ROOT, "webui", "web")
CSS = os.path.join(WEB, "style.css")
JS_DIR = os.path.join(WEB, "js")

HEX = re.compile(r"#[0-9a-fA-F]{3,8}\b")
RGBA = re.compile(r"\brgba?\(")
FONT_SIZE = re.compile(r"font-size:\s*([0-9.]+)px")
FONT_SHORT = re.compile(r"font:\s*([0-9.]+)px/")
ALLOWED_SIZES = {"11", "12", "13", "14", "16", "20", "24"}
FORBIDDEN_SIZES = {"12.5", "13.5", "15"}


def _read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def _is_token_line(line):
    return line.strip().startswith("--")


def test_style_has_no_stray_colors():
    """hex/rgba 只允许出现在 :root token 定义行。"""
    for i, line in enumerate(_read(CSS).splitlines(), 1):
        if _is_token_line(line):
            continue
        assert not HEX.search(line), "un-tokenized hex at style.css:{}: {}".format(i, line)
        assert not RGBA.search(line), "un-tokenized rgba at style.css:{}: {}".format(i, line)


def test_font_size_scale():
    src = _read(CSS)
    sizes = FONT_SIZE.findall(src) + FONT_SHORT.findall(src)
    assert sizes, "no font sizes found"
    for size in sizes:
        assert size not in FORBIDDEN_SIZES, "forbidden font size {}px".format(size)
        assert size in ALLOWED_SIZES, "off-scale font size {}px".format(size)


def test_primary_override_specificity():
    """vendored EP 样式运行时注入，主色覆盖必须用 :root:root 高特异性。"""
    src = _read(CSS)
    assert ":root:root {" in src
    assert ":root:root.dark {" in src
    assert "--el-color-primary: #0e7490" in src
    assert "--el-color-primary: #27b3c9" in src


def test_dark_terminal_tokens():
    src = _read(CSS)
    assert "--mc-term-bg" in src and "--mc-term-fg" in src
    assert "background: var(--mc-term-bg)" in src
    assert ".logview > div" in src and "content-visibility: auto" in src


def test_motion_and_a11y_rules():
    src = _read(CSS)
    assert "prefers-reduced-motion: reduce" in src
    assert "prefers-reduced-motion: no-preference" in src
    assert "transform: scale(0.97)" in src
    assert ":focus-visible" in src
    assert "transition: all" not in src


def test_utility_classes():
    src = _read(CSS)
    for cls in (".mc-page-head", ".mc-num", ".mc-mono", ".mc-muted",
                ".mc-label", ".mc-toolbar"):
        assert cls in src, "missing utility {}".format(cls)


def test_status_module_chinese_and_danger_confirm():
    src = _read(os.path.join(JS_DIR, "status.js"))
    for zh in ("运行中", "停止中", "已完成", "失败", "已停止", "未运行"):
        assert zh in src, "missing status text {}".format(zh)
    assert "function statusText" in src and "function statusType" in src
    assert "function schedulerText" in src
    assert "function fmtTimeRel" in src
    assert "function confirmDanger" in src
    assert 'confirmButtonClass: "el-button--danger"' in src


def test_views_use_design_patterns():
    perf = _read(os.path.join(JS_DIR, "views", "perf-view.js"))
    inst = _read(os.path.join(JS_DIR, "views", "instance-view.js"))
    sched = _read(os.path.join(JS_DIR, "views", "schedule-view.js"))
    results = _read(os.path.join(JS_DIR, "views", "results-view.js"))
    # 状态中文化（不再裸露英文 status）
    assert "statusText(it.status)" in perf
    assert "statusText(detail.instance.status)" in inst
    # 空状态可操作
    assert "el-empty" in perf and "el-empty" in inst and "el-empty" in sched
    # 实例详情用 el-tabs + el-descriptions
    assert 'type="card" closable' in inst
    assert "el-descriptions" in inst
    # 数字列 tabular-nums
    assert "mc-num" in perf and "mc-num" in inst and "csvIsNumeric" in results
    # 危险确认统一 confirmDanger
    assert "confirmDanger(" in perf and "confirmDanger(" in inst
    # 非破坏性的「停止调度」不得用 danger
    assert 'type="warning" plain size="small" @click="stop"' in sched


def test_no_raw_english_run_status_in_templates():
    """视图模板中不再直接输出 it.status / scope.row.status 原文。"""
    for name in ("perf-view.js", "instance-view.js"):
        src = _read(os.path.join(JS_DIR, "views", name))
        assert "{{ it.status }}" not in src
        assert "{{ scope.row.status }}" not in src
