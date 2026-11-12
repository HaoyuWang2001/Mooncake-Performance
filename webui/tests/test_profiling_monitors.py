#!/usr/bin/env python3
"""Node monitors (mem / urma): sampler config, reduce, downsample, report card."""
import importlib.util
import json
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import report as rp   # noqa: E402

_SPEC = importlib.util.spec_from_file_location(
    "prof_core", os.path.join(ROOT, "profiling", "prof_core.py"))
pc = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(pc)

ECHARTS = os.path.join(ROOT, "webui", "web", "vendor", "echarts.min.js")


def _min_stats():
    return {
        "meta": {"source": "x.writer.log", "bytes": 1, "node": "n1",
                 "role": "writer", "size": "8MB", "ts_from": 0,
                 "ts_to": 3, "wall_s": 3, "dated": True},
        "global": {"total": 4, "ok": 4, "fail": 0, "key_total": 4,
                   "key_ok": 4, "key_fail": 0, "avg_ms": 1.0, "min_ms": 0.9,
                   "max_ms": 2.0, "p50_ms": 1.0, "p95_ms": 1.5,
                   "p99_ms": 1.8, "p999_ms": 1.9, "p9999_ms": 1.99,
                   "qw_avg_ms": None, "qw_max_ms": None, "exact": True},
        "histogram": {"30": 4}, "ops": {}, "errors": {}, "error_excerpt": None,
        "slowest": [], "anomalies": [], "summaries": [],
        "seconds": [[t, 1, 1, 0, 1.0, 2.0, 0.0, 0.0, {"30": 1}]
                    for t in range(4)],
        "minutes": [[0, 4, 4, 0, 1.0, 2.0, 0.0, 0.0, {"30": 4}]],
    }


def _write_monitors(tmp_path):
    root = tmp_path / "logs" / "cfg" / "profiling" / "n1"
    root.mkdir(parents=True)
    (root / "monitor.config.json").write_text(json.dumps({
        "mem": {"leak_growth_pct": 30, "leak_min_mb": 200},
        "urma": {"bytes_per_flit": 20}}), encoding="utf-8")
    (root / "mem.host.csv").write_text(
        "ts,n0_used_mb,n1_used_mb,mem_available_mb\n"
        "1000,100,200,500\n"
        "1001,150,250,450\n", encoding="utf-8")
    # pid 111 grows 100 -> 400 MB (300MB / +300%) => leak suspect;
    # pid 222 grows 100 -> 110 MB (10MB / +10%) => not suspect
    (root / "mem.proc.csv").write_text(
        "ts,pid,comm,rss_kb,pss_kb,private_dirty_kb,private_clean_kb,swap_kb\n"
        "1000,111,python,102400,100000,50000,10000,0\n"
        "1001,111,python,409600,400000,200000,20000,0\n"
        "1000,222,python,102400,100000,50000,10000,0\n"
        "1001,222,python,112640,110000,50000,10000,0\n", encoding="utf-8")
    # 1 GiB/s RX (20 B/flit), 0.5 GiB/s TX, then a counter wrap sample (skipped)
    rx = 53687091          # *20 / 1024^3 ~= 1.0 GiB over 1s
    (root / "urma.flits.csv").write_text(
        "ts_ns,ubpu,port,rx_flits,tx_flits\n"
        "1000000000000,0,1,0,0\n"
        "1001000000000,0,1,{}, {}\n"
        "1002000000000,0,1,10,10\n".format(rx, rx // 2), encoding="utf-8")
    return tmp_path / "logs" / "cfg"


def test_load_matrix_monitor_defaults(tmp_path):
    p = tmp_path / "profile.json"
    p.write_text(json.dumps({"tools": {}}), encoding="utf-8")
    m = pc.load_matrix(str(p))
    assert m is not None                       # mem is on by default
    assert m["tools"]["mem"]["enabled"] is True
    assert m["tools"]["mem"]["proc_pattern"] == "mooncake_benchmark_client"
    assert m["tools"]["urma"]["enabled"] is False
    # explicit disable turns the whole matrix off
    p.write_text(json.dumps({"tools": {"mem": {"enabled": False}}}),
                 encoding="utf-8")
    assert pc.load_matrix(str(p)) is None
    # urma opt-in with overrides
    p.write_text(json.dumps({"tools": {"urma": {"enabled": True, "slot": 3,
                                                "ubpus": [2]}}}), encoding="utf-8")
    m = pc.load_matrix(str(p))
    assert m["tools"]["urma"]["enabled"] is True
    assert m["tools"]["urma"]["slot"] == 3
    assert m["tools"]["mem"]["enabled"] is True


def test_reduce_monitors_series_and_summary(tmp_path):
    log_dir = _write_monitors(tmp_path)
    mon = rp.reduce_monitors(str(log_dir))
    assert len(mon["mem_host"]) == 1
    h = mon["mem_host"][0]
    assert h["node"] == "n1"
    assert h["columns"] == ["n0_used_mb", "n1_used_mb", "mem_available_mb"]
    assert h["series"]["n0_used_mb"] == [100.0, 150.0]
    procs = {p["pid"]: p for p in mon["mem_proc"]}
    assert procs[111]["leak_suspect"] is True
    assert procs[111]["rss_growth_mb"] == 300.0
    assert procs[222]["leak_suspect"] is False
    assert len(mon["urma"]) == 1
    u = mon["urma"][0]
    assert (u["ubpu"], u["port"]) == (0, 1)
    # wrap sample (rx drops) is skipped -> 2 rate points max, first one 1 GiB/s
    assert u["rx"][0] == 1.0
    assert u["tx"][0] == 0.5
    sm = mon["summary"]
    assert sm["peak_mem_mb"]["n1"] == 400.0     # max(100+200, 150+250)
    assert sm["worst_proc"]["label"].startswith("n1/111")
    assert sm["worst_proc"]["leak_suspect"] is True
    assert sm["peak_urma"]["value"] == 1.0


def test_reduce_monitors_absent_is_empty(tmp_path):
    assert rp.reduce_monitors(str(tmp_path)) == {}


def test_compact_monitors_downsamples(tmp_path):
    log_dir = _write_monitors(tmp_path)
    mon = rp.reduce_monitors(str(log_dir))
    # grow the host series past the embed cap (3 cols -> need > 2000 rows)
    h = mon["mem_host"][0]
    h["t"] = list(range(6000))
    h["series"] = {c: [float(i) for i in range(6000)] for c in h["columns"]}
    comp = rp.compact_monitors(mon)
    ch = comp["mem_host"][0]
    assert 0 < len(ch["t"]) <= rp.MON_EMBED_MAX_POINTS
    assert len(ch["t"]) == len(ch["series"][ch["columns"][0]])
    # summary is preserved verbatim
    assert comp["summary"]["worst_proc"]["leak_suspect"] is True


def test_render_scenario_has_monitor_card(tmp_path):
    log_dir = _write_monitors(tmp_path)
    mon = rp.reduce_monitors(str(log_dir))
    data = rp.reduce_scenario([_min_stats()],
                              {"config": "cfg", "mode": "pipeline"},
                              monitors=mon)
    html, dj = rp.render_scenario(data, str(tmp_path / "report"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    assert "节点监控（内存 / URMA 端口）" in src
    assert 'id="monitorcard"' in src and 'id="memchart"' in src
    assert "renderMonitors" in src
    dj_src = open(dj, encoding="utf-8").read()
    assert '"monitors"' in dj_src and '"urma"' in dj_src


def test_render_scenario_without_monitors_hides_card(tmp_path):
    data = rp.reduce_scenario([_min_stats()],
                              {"config": "cfg", "mode": "pipeline"})
    html, _dj = rp.render_scenario(data, str(tmp_path / "report"), ECHARTS)
    src = open(html, encoding="utf-8").read()
    assert 'id="monitorcard"' not in src


def test_mem_monitor_script_smoke(tmp_path):
    """The sampler records rows for a matching process (real /proc read)."""
    script = os.path.join(ROOT, "profiling", "scripts", "mem_monitor.sh")
    marker = "zz_mc_proc_{}".format(os.getpid())
    proc = subprocess.Popen(["bash", "-c",
                             "exec -a {} sleep 30".format(marker)])
    try:
        env = dict(os.environ, OUT_DIR=str(tmp_path), INTERVAL_S="1",
                   PROC_PATTERN=marker)
        subprocess.run(["timeout", "3", "bash", script], env=env, check=False)
        csv_txt = (tmp_path / "mem.proc.csv").read_text(encoding="utf-8")
        assert csv_txt.splitlines()[0].startswith("ts,pid,comm,rss_kb")
        rows = [l for l in csv_txt.splitlines()[1:] if l.strip()]
        assert rows, "sampler produced no process rows"
        assert str(proc.pid) in csv_txt
    finally:
        proc.kill()
        proc.wait()


def test_overview_shows_monitor_summary(tmp_path):
    log_dir = _write_monitors(tmp_path)
    mon = rp.reduce_monitors(str(log_dir))
    data = rp.reduce_scenario([_min_stats()],
                              {"config": "cfg", "mode": "pipeline"},
                              monitors=mon)
    ov = rp.render_overview(
        {"run": "20261112-1000", "nodes": []},
        [dict(data, report_rel="logs/cfg/report/report.html")],
        str(tmp_path / "ov"))
    src = open(ov, encoding="utf-8").read()
    assert "RSS 疑似泄露" in src
    assert "端口峰值" in src and "峰值内存" in src

