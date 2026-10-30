#!/usr/bin/env python3
"""路径白名单与常量测试（python webui/tests/test_paths.py 或 pytest）。"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from webui.backend.common import real_under
from webui.backend.paths import CONF_ROOTS, READ_ROOTS, RUNS_DIR, STATIC_DIR


def test_real_under_accepts_valid():
    assert real_under(os.path.join(RUNS_DIR, "x.log"), READ_ROOTS)
    assert real_under(os.path.realpath(READ_ROOTS[0]), READ_ROOTS)  # 根本身


def test_real_under_rejects_traversal():
    bad = os.path.join(RUNS_DIR, "..", "..", "benchmark.py")
    assert not real_under(bad, READ_ROOTS)
    assert not real_under(os.path.join(os.path.dirname(STATIC_DIR), "API.md"), READ_ROOTS)


def test_read_roots_exclude_configs():
    assert not real_under(os.path.join(os.path.dirname(READ_ROOTS[0]), "configs"), READ_ROOTS)


def test_runs_dir_under_data():
    assert os.path.basename(os.path.dirname(RUNS_DIR)) == "data"
    assert os.path.isdir(RUNS_DIR)


def test_conf_roots():
    assert any(os.path.basename(r) == "configs" for r in CONF_ROOTS)
    assert any(os.path.basename(r) == "schedule" for r in CONF_ROOTS)


if __name__ == "__main__":
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("PASS", name)
            except AssertionError as exc:
                fails += 1
                print("FAIL", name, exc)
    sys.exit(1 if fails else 0)
