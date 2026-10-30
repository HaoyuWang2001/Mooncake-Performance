#!/usr/bin/env python3
"""spdiag_tree — spdiag 输出的打点树挂载与文本树生成。

两种用法：
  1) benchmark 运行结束由 prof_core.stop_and_collect 自动调用：
     遍历 configs/_spdiag_trees/*.json，根点 (point, module) 在本次收集的
     spdiag.show.txt 中命中才生成，输出 spdiag_tree.<interface>.txt。
  2) 手动 CLI（离线重生成）：
     python profiling/spdiag_tree.py --spdiag spdiag.show.txt \
         --tree configs/_spdiag_trees/get_buffer.json [-o out.txt]

树定义 JSON：root{point,module,kind,children[]}，kind=root|phase|nested|leaf，
order=同父内执行顺序，glue_note=父子打点间的未计量开销说明。

残差对账：residual = parent.avg − Σ(直接子节点 avg)（父与全部子节点均有数据才
计算；部分 nodata 时跳过，避免把缺数据误报成残差）。ALERT 条件：
|r| > 1µs 且 > 2%·parent.avg（打点缺失或嵌套关系变更的信号）。

输出文本格式（webui 的 js/spdiag.js 按此解析）：
  - 头部 `#` 注释行（interface/node/source/generated/规则/invariants）
  - 节点行：`KIND [order/total] <point> <module> ticks=.. avg=..us p999=.. p9999=.. max=.. | nodata`
  - 树形符号：每层前缀 "│   " 或 "    "（4 字符一组），连接符 "├─ "/"└─ "，ROOT 无前缀
  - GLUE/RESIDUAL 行作为所属节点块内最后一行（解析时归属深度-1 的节点）
  - 尾部 UNMATCHED 行：本次输出中未被树挂载的打点
"""
import argparse
import json
import os
import sys
import time

RESIDUAL_MIN_NS = 1000          # |r| > 1µs ...
RESIDUAL_REL = 0.02             # ... 且 > 2% of parent avg -> ALERT


# ---------------------------------------------------------------------------
# spdiag show 表解析（16 列定界文本）
# ---------------------------------------------------------------------------

def _num(tok):
    tok = tok.replace(",", "")
    if tok in ("N/A", "", "-"):
        return None
    try:
        return int(tok)
    except ValueError:
        return None


def parse_spdiag_table(text):
    """-> list of row dicts. Columns: # Program Module Point Lvl Ticks Good
    Bad Not Total Avg Min Max P99 P999 P9999 (values in ns, N/A -> None)."""
    rows = []
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#") or set(s) <= set("- "):
            continue
        toks = s.split()
        if len(toks) != 16 or not toks[0].isdigit():
            continue
        try:
            rows.append({
                "program": toks[1], "module": toks[2], "point": toks[3],
                "lvl": int(toks[4]),
                "ticks": int(toks[5]), "good": int(toks[6]),
                "bad": int(toks[7]), "not": int(toks[8]),
                "total": _num(toks[9]), "avg": _num(toks[10]),
                "min": _num(toks[11]), "max": _num(toks[12]),
                "p99": _num(toks[13]), "p999": _num(toks[14]),
                "p9999": _num(toks[15]),
            })
        except ValueError:
            continue
    return rows


# ---------------------------------------------------------------------------
# 挂载 / 残差
# ---------------------------------------------------------------------------

def _tree_keys(node, acc):
    acc.append((node["point"], node.get("module")))
    for c in node.get("children") or []:
        _tree_keys(c, acc)
    return acc


def mount_tree(tree, rows):
    """按 (point, module) 将表格行挂为节点 data（未命中 -> None）。"""
    index = {(r["point"], r["module"]): r for r in rows}

    def walk(node):
        n = dict(node)
        n["data"] = index.get((node["point"], node.get("module")))
        n["children"] = [walk(c) for c in node.get("children") or []]
        return n
    return walk(tree["root"])


def compute_residuals(node, out):
    """对每个「自身有数据且全部直接子节点有数据」的节点计算
    residual = avg − Σ(child avg)；附加到节点 residual 字段。"""
    kids = node["children"]
    node["residual"] = None
    if node["data"] and kids and all(c["data"] for c in kids):
        child_sum = sum(c["data"]["avg"] for c in kids)
        r = node["data"]["avg"] - child_sum
        alert = abs(r) > RESIDUAL_MIN_NS and abs(r) > RESIDUAL_REL * node["data"]["avg"]
        node["residual"] = {"r": r, "parent_avg": node["data"]["avg"], "alert": alert}
        out.append(node["residual"])
    for c in kids:
        compute_residuals(c, out)
    return out


# ---------------------------------------------------------------------------
# 文本渲染（树形符号）
# ---------------------------------------------------------------------------

def _fmt_ns(v):
    if v is None:
        return "N/A"
    if abs(v) >= 1_000_000:
        return "{:.2f}ms".format(v / 1e6)
    if abs(v) >= 1_000:
        return "{:.2f}us".format(v / 1e3)
    return "{}ns".format(int(v))


def _stats_field(d):
    if not d:
        return "nodata"
    return "ticks={} avg={} p999={} p9999={} max={}".format(
        d["ticks"], _fmt_ns(d["avg"]), _fmt_ns(d["p999"]),
        _fmt_ns(d["p9999"]), _fmt_ns(d["max"]))


def render_text(tree_json, root, unmatched_rows, node_name, source_name):
    """-> 完整文本树（含头部注释 / 节点 / GLUE / RESIDUAL / UNMATCHED）。"""
    lines = []
    lines.append("# spdiag-tree v1")
    lines.append("# interface: {}".format(tree_json.get("interface", "?")))
    lines.append("# node: {}".format(node_name))
    lines.append("# source: {}".format(source_name))
    lines.append("# generated: {}".format(time.strftime("%Y-%m-%d %H:%M:%S")))
    lines.append("# residual rule: |r| > {}us AND > {}% of parent avg -> ALERT".format(
        RESIDUAL_MIN_NS // 1000, int(RESIDUAL_REL * 100)))
    for inv in tree_json.get("invariants", []):
        lines.append("# invariant: {}".format(inv))

    def rec(node, prefix, connector):
        total_sib = node.get("_sib_total")
        order_part = ""
        if node.get("order") is not None and total_sib:
            order_part = " {}/{}".format(node["order"], total_sib)
        lines.append("{}{}{}{} {} {} {}".format(
            prefix, connector, node["kind"].upper(), order_part,
            node["point"], node.get("module", ""),
            _stats_field(node["data"])))
        kids = node["children"]
        kid_prefix = prefix + ("" if connector == "" else
                               ("    " if connector == "└─ " else "│   "))
        for i, c in enumerate(kids):
            c["_sib_total"] = len(kids)
            rec(c, kid_prefix, "└─ " if i == len(kids) - 1 else "├─ ")
        if node.get("glue_note"):
            lines.append("{}└─ GLUE {}".format(kid_prefix, node["glue_note"]))
        if node.get("residual"):
            r = node["residual"]
            lines.append("{}└─ RESIDUAL {} r={} parent={} [{}]".format(
                kid_prefix, node["point"], _fmt_ns(r["r"]),
                _fmt_ns(r["parent_avg"]), "ALERT" if r["alert"] else "OK"))

    rec(root, "", "")
    for r in unmatched_rows:
        lines.append("UNMATCHED {} ({})".format(r["point"], r["module"]))
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# 集成入口（prof_core 调用）
# ---------------------------------------------------------------------------

def generate_trees(spdiag_path, trees_dir, out_dir, node_name):
    """对 trees_dir 下每棵根点命中的树生成 spdiag_tree.<interface>.txt，
    写入 out_dir。返回生成的文件名列表。"""
    if not os.path.isfile(spdiag_path) or not os.path.isdir(trees_dir):
        return []
    with open(spdiag_path, "r", encoding="utf-8", errors="replace") as f:
        rows = parse_spdiag_table(f.read())
    if not rows:
        return []
    index = {(r["point"], r["module"]): r for r in rows}
    written = []
    for name in sorted(os.listdir(trees_dir)):
        if not name.endswith(".json"):
            continue
        try:
            with open(os.path.join(trees_dir, name), "r", encoding="utf-8") as f:
                tree_json = json.load(f)
        except ValueError:
            continue
        root = tree_json.get("root") or {}
        if (root.get("point"), root.get("module")) not in index:
            continue  # 本接口不在本次输出中
        mounted = mount_tree(tree_json, rows)
        compute_residuals(mounted, [])
        keys = set(_tree_keys(root, []))
        unmatched = [r for r in rows if (r["point"], r["module"]) not in keys]
        interface = tree_json.get("interface") or name[:-5]
        text = render_text(tree_json, mounted, unmatched, node_name,
                           os.path.basename(spdiag_path))
        out_path = os.path.join(out_dir, "spdiag_tree.{}.txt".format(interface))
        with open(out_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        written.append(os.path.basename(out_path))
    return written


def main(argv=None):
    parser = argparse.ArgumentParser(description="spdiag text-tree generator")
    parser.add_argument("--spdiag", required=True, help="spdiag show 输出文件")
    parser.add_argument("--tree", required=True, help="打点树定义 json")
    parser.add_argument("--node", default="-", help="节点名（写入头部注释）")
    parser.add_argument("-o", "--output", default=None, help="输出文件（缺省打印 stdout）")
    args = parser.parse_args(argv)
    with open(args.spdiag, "r", encoding="utf-8", errors="replace") as f:
        rows = parse_spdiag_table(f.read())
    with open(args.tree, "r", encoding="utf-8") as f:
        tree_json = json.load(f)
    mounted = mount_tree(tree_json, rows)
    compute_residuals(mounted, [])
    keys = set(_tree_keys(tree_json["root"], []))
    unmatched = [r for r in rows if (r["point"], r["module"]) not in keys]
    text = render_text(tree_json, mounted, unmatched, args.node,
                       os.path.basename(args.spdiag))
    if args.output:
        with open(args.output, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        print("written: {}".format(args.output))
    else:
        sys.stdout.write(text)


if __name__ == "__main__":
    main()
