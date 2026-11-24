#!/usr/bin/env python3
"""Log structure v2: phase dividers, heartbeats, tag/label conventions.

Contract covered:
  1) log_phase prints a blank line + untimestamped divider (level 1 / 2) to
     the console and the full detail log;
  2) no log() message embeds a second "[benchmark] " prefix (the utils logger
     already adds one);
  3) _Heartbeat respects heartbeat_interval_s (0=off), prints the configured
     Chinese progress line with remaining/ETA when a total is known, and an
     elapsed-only line otherwise;
  4) DNS resolutions are cached per run (repeat -> log_detail, not console);
  5) stage progress labels carry an engine tag ("[kvtest] remove-keys") when
     tag= is passed, and fall back to the legacy prefix otherwise;
  6) runner scripts print [client-env] only for benchmark/pipeline modes and
     no longer emit the duplicate [INFO] SETUP_UB_DEVICE echo.
"""
import os
import sys
import time
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import benchmark as bm  # noqa: E402
import utils  # noqa: E402


# ------------------------------------------------------------ dividers

def test_log_phase_level1_and_level2(tmp_path, capsys):
    detail = tmp_path / "detail.log"
    utils.set_detail_log(str(detail))
    try:
        utils.log_phase("场景 demo (engine=kvtest, scenario=pipeline)")
        utils.log_phase("资源创建", level=2)
    finally:
        utils.close_detail_log()
    out = capsys.readouterr().out
    assert "\n──── 场景 demo (engine=kvtest, scenario=pipeline) ────" in out
    assert "\n  ── 资源创建 ──" in out
    text = detail.read_text(encoding="utf-8")
    assert "──── 场景 demo (engine=kvtest, scenario=pipeline) ────" in text
    assert "  ── 资源创建 ──" in text


def test_no_double_benchmark_prefix_in_source():
    """No log()/log_detail() message text may start with "[benchmark] "."""
    src = (ROOT + "/benchmark.py")
    with open(src, encoding="utf-8") as f:
        text = f.read()
    import re
    bad = re.findall(r'log(?:_detail)?\("\[benchmark\] ', text)
    assert not bad, "embedded [benchmark] prefix in {} place(s)".format(len(bad))


# ------------------------------------------------------------ heartbeat

def test_heartbeat_disabled():
    logs = []
    old = bm.log
    bm.log = lambda m: logs.append(m)
    try:
        hb = bm._Heartbeat("kvtest", 600, 0)
        hb.tick()
    finally:
        bm.log = old
    assert logs == []


def test_heartbeat_with_remaining_and_eta():
    logs = []
    old = bm.log
    bm.log = lambda m: logs.append(m)
    try:
        hb = bm._Heartbeat("kvtest", 60, 1)
        hb.t0 = time.time() - 1.2   # force the first tick
        hb.last = hb.t0
        hb.tick()
    finally:
        bm.log = old
    assert len(logs) == 1
    import re
    assert re.match(
        r"^\[kvtest\] 测试已进行 00:00:01，剩余 00:00:5\d，预计结束于 "
        r"\d{2}:\d{2}:\d{2}$", logs[0]), logs[0]


def test_heartbeat_elapsed_only_without_total():
    logs = []
    old = bm.log
    bm.log = lambda m: logs.append(m)
    try:
        hb = bm._Heartbeat("basic", None, 1)
        hb.t0 = time.time() - 1.2
        hb.last = hb.t0
        hb.tick()
    finally:
        bm.log = old
    assert logs == ["[basic] 测试已进行 00:00:01"]


def test_heartbeat_interval_resolution():
    assert bm._heartbeat_interval({}) == 60
    assert bm._heartbeat_interval({"heartbeat_interval_s": 120}) == 120
    assert bm._heartbeat_interval({"heartbeat_interval_s": 0}) == 0
    assert bm._heartbeat_interval({"heartbeat_interval_s": "bad"}) == 60


# ------------------------------------------------------------ dns cache

def test_dns_cache_dedupes_console_log(monkeypatch):
    calls = {"log": [], "detail": []}
    monkeypatch.setattr(bm, "log", lambda m: calls["log"].append(m))
    monkeypatch.setattr(bm, "log_detail",
                        lambda m: calls["detail"].append(m))
    monkeypatch.setattr(bm.socket, "getaddrinfo",
                        lambda *a, **k: [(0, 0, 0, "", ("10.1.2.3", 0))])
    cluster = {"nodes": {}}
    bm._resolve_host("svc.example.test", cluster)
    bm._resolve_host("svc.example.test", cluster)
    assert len([m for m in calls["log"] if "dns:" in m]) == 1
    assert any("cached" in m for m in calls["detail"])


# ------------------------------------------------------ stage tag labels

def test_remove_keys_stage_label_tag(monkeypatch):
    seen = []
    monkeypatch.setattr(bm, "run_stage",
                        lambda *a, **k: seen.append(k.get("label")))
    monkeypatch.setattr(bm, "_remove_keys_precheck", lambda *a, **k: True)
    cluster = {"nodes": {}}
    bm._remove_keys_stage(cluster, {}, "h", {}, "runner", "/r", "cfg",
                          "cfg", "full", "pre-run", tag="kvtest")
    assert seen == []            # precheck skipped -> no stage
    bm._remove_keys_stage(cluster, {}, "h", {}, "runner", "/r", "cfg",
                          "cfg", "full", "pre-run", dry=True, tag="kvtest")
    assert seen == ["[kvtest] remove-keys"]


# ------------------------------------------------------ runner scripts

def test_client_env_only_for_benchmark_modes():
    pipe = os.path.join(ROOT, "scripts", "run_benchmark_pipeline.sh")
    basic = os.path.join(ROOT, "scripts", "run_benchmark_basic.sh")
    ptxt = open(pipe, encoding="utf-8").read()
    btxt = open(basic, encoding="utf-8").read()
    assert 'if [ "$MODE" = "pipeline" ]' in ptxt
    assert ptxt.count('print_client_env "mode=$MODE"') == 1
    assert 'if [ "$MODE" = "benchmark" ]' in btxt


def test_env_template_has_no_info_ub_echo():
    lines = bm._ub_probe_lines()
    joined = "\n".join(lines)
    assert "[INFO] SETUP_UB_DEVICE" not in joined
    assert "[ERROR]" in joined          # failure branches kept
