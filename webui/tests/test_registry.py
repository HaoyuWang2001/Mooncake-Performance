#!/usr/bin/env python3
"""registry.json 双向完整性（测试体系唯一规则源）。

- 每条规则可解析：id 合法且唯一、desc 非空、module 在册、unit/e2e 是列表；
- 规则引用的每个 unit 测试真实存在；
- 规则引用的每个 e2e 检查 id 存在于 e2e 契约（Batch 2 落地，缺文件则要求为空）；
- **每个测试函数必须归属至少一条规则**（孤儿测试红灯：要么登记，要么删除）。
"""
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

_HERE = os.path.dirname(os.path.abspath(__file__))
REG = os.path.join(_HERE, "registry.json")
ID_RE = re.compile(r"^C\d{1,2}\.(V|I|D|B)\d{1,2}$|^E-\d+$")


def _registry():
    with open(REG, "r", encoding="utf-8") as f:
        return json.load(f)


def _all_test_names():
    names = set()
    for p in os.listdir(_HERE):
        if not p.startswith("test_") or not p.endswith(".py"):
            continue
        src = open(os.path.join(_HERE, p), encoding="utf-8").read()
        names |= set(re.findall(r"^def (test_[a-z0-9_]+)\(", src, re.M))
    return names


def _e2e_ids():
    checks = os.path.join(_HERE, "e2e", "checks.py")
    if not os.path.isfile(checks):
        return set()
    src = open(checks, encoding="utf-8").read()
    return set(re.findall(r"^def (check_[a-z0-9_]+)\(", src, re.M))


def test_registry_shape_and_references():
    reg = _registry()
    assert reg.get("version") == 1
    assert set(reg.get("modules")) == {
        "report", "benchmark", "kvtest", "webui", "profiling", "utils"}
    rules = reg.get("rules") or []
    assert len(rules) >= 200, len(rules)          # 报表 C-* 种子 + E-*
    ids = [r["id"] for r in rules]
    assert len(ids) == len(set(ids)), "规则 id 重复"
    names = _all_test_names()
    e2e = _e2e_ids()
    for r in rules:
        assert ID_RE.fullmatch(r["id"]), r["id"]
        assert (r.get("desc") or "").strip(), r["id"]
        assert r.get("module") in set(reg.get("modules")), r["id"]
        assert isinstance(r.get("unit"), list), r["id"]
        assert isinstance(r.get("e2e"), list), r["id"]
        for t in r["unit"]:
            assert t in names, (r["id"], t)
        for e in r["e2e"]:
            assert e in e2e, (r["id"], e)


def test_every_test_belongs_to_a_rule():
    reg = _registry()
    covered = set()
    for r in reg.get("rules") or []:
        covered.update(r.get("unit") or [])
    orphan = sorted(n for n in _all_test_names() if n not in covered)
    assert not orphan, "孤儿测试（未登记任何规则）：\n  " + "\n  ".join(orphan)


def test_module_paths_cover_sources():
    reg = _registry()
    paths = reg.get("module_paths") or []
    assert paths, "module_paths 为空"
    for prefix, mod in paths:
        assert mod in set(reg.get("modules")), (prefix, mod)
        assert _prefix_exists(prefix), prefix


def _prefix_exists(prefix):
    prefix = prefix.replace(os.sep, "/").rstrip("/")
    head = prefix.split("/")[0]
    base = os.path.join(ROOT, head)
    if prefix.endswith(".py") or prefix.endswith(".sh"):
        return os.path.isfile(os.path.join(ROOT, prefix)) or os.path.isdir(base)
    return os.path.isdir(base)
