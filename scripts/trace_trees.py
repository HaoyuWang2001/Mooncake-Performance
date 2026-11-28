#!/usr/bin/env python3
"""trace_trees.py — configs/_trace_trees/*.json 的加载与校验（单一事实源）。

每 op 一份阶段树（schema mp-trace-tree/1）。本模块是唯一的读取入口：
tracestats（map）与 report（reduce/渲染）都从这里 import，树本身不再散落
为代码常量。字段名 = 日志原生名（零翻译）。

树是静态 schema 知识，不进任何 trace 产物 JSON；目录缺失或校验失败一律
硬失败（ValueError）——静默回退到错误树是本仓库读/写路径 trace 解析
全错的根因。
"""
import json
import os

_HERE = os.path.dirname(os.path.abspath(__file__))


def _tree_dir_candidates(scripts_dir):
    """树目录候选（按优先级）：
    1. 源仓库布局 <root>/configs/_trace_trees（scripts/ 的同级目录）；
    2. 平铺布局 scripts/ 本身 —— benchmark 经 configmap/ssh 把树与脚本
       平铺下发到 pod（configmap 键不允许子目录），以 _schema.json 作
       平铺标记（loader 本就跳过 ``_`` 前缀文件，不会把它当树加载）。"""
    root = os.path.dirname(os.path.abspath(scripts_dir))
    return [os.path.join(root, "configs", "_trace_trees"),
            os.path.abspath(scripts_dir)]


def _resolve_tree_dir(scripts_dir=None):
    """选第一个带 _schema.json 的候选；都没有则返回首选路径，
    交由 load_trace_trees 硬失败（缺失不静默）。"""
    scripts_dir = os.path.abspath(scripts_dir or _HERE)
    for cand in _tree_dir_candidates(scripts_dir):
        if os.path.isfile(os.path.join(cand, "_schema.json")):
            return cand
    return _tree_dir_candidates(scripts_dir)[0]


TREE_DIR = _resolve_tree_dir()

# 全局成功状态（各 op 树可用 ok_statuses 追加自身变体，如 read_ok）
GLOBAL_OK_STATUSES = frozenset({"ok"})

_FIELD_RE = None  # lazy: re at module import cost is fine, keep simple below


class TreeError(ValueError):
    """树 JSON 静态校验失败（含缺失/结构/引用错误）。"""


def _iter_stage_nodes(stages):
    """Yield (node, level, parent_field, view) for a stage list.

    view: None = 主视图；否则为 views 的 key（并列分解，互不加和）。
    switch 的 case 节点及其子树以 case 名（classic/tent）作为 view ——
    渲染端据此给「仅某引擎会出现」的阶段打徽标。"""
    import re
    for node in stages or []:
        if "switch" in node:
            for case_name, case_nodes in (node.get("cases") or {}).items():
                for sub, level, parent, view in _iter_stage_nodes(case_nodes):
                    yield (sub, level, parent, view or case_name)
            continue
        field = node["field"]
        level = 1 if node.get("chart") else 2
        yield (node, level, None, None)
        for ch in node.get("children") or []:
            yield (ch, 2, field, None)
        for vkey, vw in (node.get("views") or {}).items():
            for ch in vw.get("children") or []:
                yield (ch, 2, field, vkey)


def _validate_tree(tree):
    """One tree dict -> raises TreeError on any structural problem."""
    op = tree.get("op")
    if not op or not isinstance(op, str):
        raise TreeError("tree without op")
    if tree.get("group") not in ("read", "write", "other"):
        raise TreeError("%s: bad group %r" % (op, tree.get("group")))
    if tree.get("total_field") != "total_us":
        raise TreeError("%s: total_field must be total_us" % op)
    stages = tree.get("stages")
    if not isinstance(stages, list) or not stages:
        raise TreeError("%s: empty stages" % op)
    seen_fields = set()
    for node, level, parent, view in _iter_stage_nodes(stages):
        field = node.get("field")
        if not field or not field.endswith("_us"):
            raise TreeError("%s: bad field %r" % (op, field))
        if field in seen_fields:
            raise TreeError("%s: duplicate field %r" % (op, field))
        seen_fields.add(field)
        if parent is not None and parent not in seen_fields:
            raise TreeError("%s: field %r references parent %r not yet seen"
                            % (op, field, parent))
    # views 子节点一律 L2（不得带 chart）；switch case 节点不受此限——
    # 其 view=case 名，允许 L1（put 族传输段按引擎切换 L1 形状）
    for node in stages:
        for vw in (node.get("views") or {}).values():
            for ch in vw.get("children") or []:
                if ch.get("chart"):
                    raise TreeError("%s: views child %r must not be chart "
                                    "(views are L2 sub-decompositions)"
                                    % (op, ch.get("field")))
    for node in stages:
        if "switch" in node:
            if node.get("switch") != "engine":
                raise TreeError("%s: switch only supports 'engine'" % op)
            cases = node.get("cases") or {}
            if not cases:
                raise TreeError("%s: empty switch cases" % op)
            for case_name, case_nodes in cases.items():
                if not case_nodes:
                    raise TreeError("%s: empty switch case %r"
                                    % (op, case_name))
                for n in case_nodes:
                    if "field" not in n:
                        raise TreeError("%s: switch case %r node without field"
                                        % (op, case_name))
    ok = tree.get("ok_statuses") or []
    if not isinstance(ok, list):
        raise TreeError("%s: ok_statuses must be a list" % op)
    return tree


def load_trace_trees(tree_dir=TREE_DIR):
    """Load + validate every <op>.json in tree_dir.

    Returns {op: tree_dict}（extends 已展开、marker 索引已建好）。目录缺失
    或任一文件非法 -> TreeError（硬失败，不回退）。"""
    if not os.path.isdir(tree_dir):
        raise TreeError("trace tree dir not found: %s" % tree_dir)
    trees = {}
    for fn in sorted(os.listdir(tree_dir)):
        if not fn.endswith(".json") or fn.startswith("_"):
            continue
        path = os.path.join(tree_dir, fn)
        with open(path, "r", encoding="utf-8") as f:
            tree = json.load(f)
        base = tree.get("extends")
        if base:
            if base not in trees:
                # extends 仅支持单层：被继承者必须按文件名序已加载
                raise TreeError("%s: extends %s not loadable (single level "
                                "only, ordered by filename)" % (fn, base))
            merged = {k: v for k, v in trees[base].items()
                      if k not in ("op", "line")}
            merged.update({k: v for k, v in tree.items()
                           if k not in ("schema", "extends")})
            tree = merged
        if tree.get("schema") != "mp-trace-tree/1":
            raise TreeError("%s: schema must be mp-trace-tree/1" % fn)
        _validate_tree(tree)
        trees[tree["op"]] = tree
    if not trees:
        raise TreeError("no trace trees in %s" % tree_dir)
    return trees


def marker_index(trees):
    """{marker_literal: op} — 主行 marker（breakdown[<op>] 形式）。"""
    idx = {}
    for op, tree in trees.items():
        for m in (tree.get("line") or {}).get("markers") or []:
            idx[m] = op
    return idx


def legacy_index(trees):
    """{legacy_marker: {op, require, field_aliases, legacy_ops}} — 旧行兼容。"""
    idx = {}
    for op, tree in trees.items():
        leg = (tree.get("line") or {}).get("legacy") or {}
        m = leg.get("marker")
        if m:
            idx[m] = {"op": op, "require": leg.get("require") or [],
                      "field_aliases": leg.get("field_aliases") or {},
                      "legacy_ops": leg.get("legacy_ops") or []}
    return idx


def ok_statuses_of(trees):
    """{op: frozenset(ok 状态集合)}（含全局 'ok'）。"""
    out = {}
    for op, tree in trees.items():
        out[op] = frozenset(GLOBAL_OK_STATUSES
                            | set(tree.get("ok_statuses") or []))
    return out


def stage_rows_of(trees):
    """{op: [rows]} — 扁平化渲染行 [field, level, parent, view]。

    level: 0=total_us 1=chart L1 2=L2；parent: L1 的父是 total_us（与
    JS 端 tParentKey 兜底一致），L2 的父是其 stage 字段；view: ''=主视图，
    否则为 views key 或 switch case 名（并列分解/引擎互斥段，互不加和，
    渲染端以徽标区分）。switch 两个 case 的节点并列展开（classic/tent
    混跑时缺字段的行自然为空——残差按「出现的 L1」计算）。"""
    out = {}
    for op, tree in trees.items():
        rows = [["total_us", 0, "", ""]]
        for node, level, parent, view in _iter_stage_nodes(tree["stages"]):
            rows.append([node["field"], level, parent or "total_us",
                         view or ""])
        rows.append(["residual", 1, "total_us", ""])
        out[op] = rows
    return out


def chart_fields_of(trees):
    """{op: (L1 chart 字段元组, L2 子字段元组)} — map 侧 fminutes/TopN 用。

    residual 不在树行里出现（由 stage_rows_of 单独追加为渲染行），也不进
    L1 元组——map 侧残差走独立通道（field_minutes['residual']）。"""
    out = {}
    rows_map = stage_rows_of(trees)
    for op, rows in rows_map.items():
        top = tuple(r[0] for r in rows if r[1] == 1 and r[0] != "residual")
        sub = tuple(r[0] for r in rows if r[1] == 2)
        out[op] = (top, sub)
    return out


def meta_fields_of(trees):
    """{op: set(不参与时延统计的行内字段)} — STRING/META 类合并口径。"""
    out = {}
    for op, tree in trees.items():
        out[op] = set(tree.get("meta_fields") or [])
    return out


if __name__ == "__main__":
    import sys
    try:
        t = load_trace_trees()
    except TreeError as e:
        print("[trace_trees] FAIL: %s" % e, file=sys.stderr)
        sys.exit(1)
    rows_map = stage_rows_of(t)
    print("[trace_trees] OK %d trees: %s" % (
        len(t), ", ".join("%s(%d rows)" % (op, len(rows_map[op]))
                          for op in sorted(t))))
    sys.exit(0)
