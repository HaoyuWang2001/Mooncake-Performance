#!/usr/bin/env python3
"""pipeline client SUMMARY：wall_s / QPS 使用实际测量窗口，而不是配置 duration。

此前 writer/reader 的 SUMMARY 写死 `wall_s = duration`（配置值）——
早停/超时截断时报表时长与 QPS 全部虚报（实测 5min 报成 10min）。
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import mooncake_benchmark_client as client  # noqa: E402


def test_measured_wall_prefers_notify_span():
    f = client._measured_wall_from_notifies
    # 实测跨度优先于配置值
    assert f(100.0, 160.0, 999.0) == 60.0
    # 无 measured notify -> 回退配置窗口
    assert f(0.0, 0.0, 20) == 20.0
    # 退化输入：只有起点 / 起点等于终点 -> 回退
    assert f(0.0, 100.0, 20) == 20.0
    assert f(5.0, 5.0, 20) == 20.0
    # 空/非法 fallback
    assert f(0.0, 0.0, None) == 0.0
    assert f(0.0, 0.0, "bad") == 0.0
