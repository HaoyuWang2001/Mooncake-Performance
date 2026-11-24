#!/usr/bin/env python3
"""kvtest split 启动次序：默认合并启动+合并健康门；reader_first_launch 回退两段式。

start_paused 下 writer 直到 /start 才开始连接/发送，"reader 先于 writer"
的约束等价于"所有健康门在 /start 前通过"——合并门因此成立。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import benchmark as bm  # noqa: E402


def _plan():
    return {
        "dual": False,
        "readers": [{"iid": 0}, {"iid": 1}],
        "writers": [{"iid": 2}, {"iid": 3}],
    }


def _recorders():
    calls = {"prepare": [], "fire": [], "gate": []}
    prepare = lambda role, entries: calls["prepare"].append(
        (role, tuple(e["iid"] for e in entries)))
    fire = lambda entries: calls["fire"].append(
        tuple(e["iid"] for e in entries))
    gate = lambda entries, what: calls["gate"].append(
        (what, tuple(e["iid"] for e in entries)))
    return calls, prepare, fire, gate


def test_merged_launch_order_default():
    calls, prepare, fire, gate = _recorders()
    bm._kvtest_launch_split_roles(_plan(), False, prepare, fire, gate)
    # both roles prepared back-to-back, then ONE batch fire, then ONE gate.
    # prepare() carries the SINGULAR role (reader|writer): _prepare_instance
    # forwards it to _kvtest_numa_node, which rejects anything else.
    assert calls["prepare"] == [("reader", (0, 1)), ("writer", (2, 3))]
    for role, _entries in calls["prepare"]:
        assert role in ("reader", "writer")
    assert calls["fire"] == [(0, 1, 2, 3)]
    assert calls["gate"] == [("instances", (0, 1, 2, 3))]


def test_staggered_launch_order_fallback():
    calls, prepare, fire, gate = _recorders()
    bm._kvtest_launch_split_roles(_plan(), True, prepare, fire, gate)
    assert calls["prepare"] == [("reader", (0, 1)), ("writer", (2, 3))]
    for role, _entries in calls["prepare"]:
        assert role in ("reader", "writer")
    assert calls["fire"] == [(0, 1), (2, 3)]
    assert calls["gate"] == [("readers", (0, 1)), ("writers", (2, 3))]


def test_split_launch_order_knob():
    assert bm._kvtest_split_launch_order({}) is False
    assert bm._kvtest_split_launch_order({"kvtest": {}}) is False
    assert bm._kvtest_split_launch_order(
        {"kvtest": {"reader_first_launch": True}}) is True
    assert bm._kvtest_split_launch_order(
        {"kvtest": {"reader_first_launch": False}}) is False


def test_split_launch_with_numa_bindings_passes_singular_roles():
    """真机回归：配置了 kvtest.numa_bindings 时，合并门仍必须给
    _prepare_instance 传单数 role——否则 _kvtest_numa_node 直接抛
    "only supports split writer/reader roles"（launch readerss 事故）。"""
    seen = []
    bm._kvtest_launch_split_roles(
        _plan(), False,
        lambda role, entries: seen.append((role, list(entries))),
        lambda entries: None, lambda entries, what: None)
    cfg = {"kvtest": {"numa_bindings": [{"writer": 0, "reader": 1}]}}
    assert [r for r, _e in seen] == ["reader", "writer"]
    for role, entries in seen:
        for e in entries:
            # would raise RuntimeError("only supports split writer/reader
            # roles") if the plural form leaked through
            bm._kvtest_numa_node(cfg, dict(e, local=0), role)
