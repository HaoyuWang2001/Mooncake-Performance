#!/usr/bin/env python3
"""按改动文件选择相关模块的测试（registry 驱动）。

用法：
  python3 scripts/test_scope.py --since HEAD            # 工作区改动 -> 测试清单
  python3 scripts/test_scope.py --since origin/main    # 分支增量
  python3 scripts/test_scope.py --module report        # 指定模块
  python3 scripts/test_scope.py --all                  # 全部登记测试
  python3 scripts/test_scope.py --since HEAD --expr    # 输出 -m 表达式

退出码：0=有匹配；2=无匹配（调用方可据此判断是否需要额外人工选择）。
"""
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REG = os.path.join(ROOT, "webui", "tests", "registry.json")


def _registry():
    with open(REG, "r", encoding="utf-8") as f:
        return json.load(f)


def _module_of(path, module_paths):
    path = path.replace(os.sep, "/")
    best, best_len = None, -1
    for prefix, mod in module_paths:
        prefix = str(prefix).replace(os.sep, "/")
        if path == prefix or path.startswith(prefix.rstrip("/") + "/"):
            if len(prefix) > best_len:
                best, best_len = mod, len(prefix)
    return best


def _changed(since):
    out = subprocess.run(["git", "-C", ROOT, "diff", "--name-only", since],
                         capture_output=True, text=True).stdout
    untracked = subprocess.run(
        ["git", "-C", ROOT, "ls-files", "--others", "--exclude-standard"],
        capture_output=True, text=True).stdout
    return [p for p in (out + "\n" + untracked).splitlines() if p.strip()]


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="HEAD")
    ap.add_argument("--module", action="append", default=None)
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--expr", action="store_true",
                    help="输出 -m 表达式而不是测试节点")
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args(argv)
    reg = _registry()
    rules = reg.get("rules") or []
    if args.all:
        mods = list(reg.get("modules") or [])
    elif args.module:
        mods = list(args.module)
    else:
        mods = sorted({m for m in (_module_of(p, reg.get("module_paths") or [])
                                   for p in _changed(args.since)) if m})
    if not mods:
        if not args.quiet:
            print("[scope] 无模块匹配（改动是否为文档/无关文件？可 --module 指定）",
                  file=sys.stderr)
        return 2
    tests = []
    for r in rules:
        if r.get("module") in mods:
            tests.extend(r.get("unit") or [])
    tests = sorted(set(tests))
    if not args.quiet:
        print("[scope] modules: " + ", ".join(mods), file=sys.stderr)
        print("[scope] tests: {}".format(len(tests)), file=sys.stderr)
    if args.expr:
        print(" or ".join(mods))
    else:
        print(" ".join(tests))
    return 0 if tests else 2


if __name__ == "__main__":
    sys.exit(main())
