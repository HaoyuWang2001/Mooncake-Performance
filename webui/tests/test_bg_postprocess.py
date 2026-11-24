#!/usr/bin/env python3
"""Per-config post-processing is dispatched to a background worker so CSV +
report map/reduce overlap the finished config's teardown and the next config's
deployment; the run-level overview renders before the global teardown and the
PNG plots run in parallel with it.

Contract covered here:
  1) _bg_submit/_bg_join keep submission order, collect results and surface
     exceptions as WARN-only failures (never fail the run);
  2) _post_process_config does CSV -> report (-> outliers) and returns the
     (category, rows) pair used by the run-level detail CSV;
  3) the cluster snapshot handed to the worker is isolated from the live
     cluster["nodes"] the next k8s scenario keeps appending to;
  4) the STOP path joins pending background work BEFORE rendering the partial
     overview;
  5) intermediates are removed by default only after a verified report; the
     whole run is kept behind --keep-intermediates / MC_KEEP_INTERMEDIATES=1,
     --pull-raw-logs / --png, --no-report, or any failed config.
"""
import os
import sys
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import benchmark as bm  # noqa: E402


@pytest.fixture(autouse=True)
def _drain_bg():
    bm._bg_join(10)
    yield
    bm._bg_join(10)


# --------------------------------------------------------------- executor

def test_bg_join_keeps_order_and_collects_errors():
    bm._bg_submit("first", lambda: 1)
    bm._bg_submit("second", lambda x: x * 2, 21)

    def _boom():
        raise ValueError("boom")

    bm._bg_submit("third", _boom)
    out = bm._bg_join(10)
    assert [name for name, *_ in out] == ["first", "second", "third"]
    assert out[0][1] is True and out[0][2] == 1
    assert out[1][1] is True and out[1][2] == 42
    assert out[2][1] is False and isinstance(out[2][3], ValueError)


def test_cluster_snapshot_isolates_live_nodes():
    cluster = {"nodes": {"a": {"ip": "1.1.1.1"}}, "k8s": {"namespace": "ns"}}
    snap = bm._cluster_snapshot(cluster)
    # the next k8s scenario keeps appending pod entries to the live dict
    cluster["nodes"]["b"] = {"ip": "2.2.2.2"}
    assert set(snap["nodes"]) == {"a"}
    # iterating the snapshot is safe (the live dict changed size)
    assert [n["ip"] for n in snap["nodes"].values()] == ["1.1.1.1"]
    assert snap["k8s"] is cluster["k8s"]


# ----------------------------------------------------- per-config worker

def test_post_process_csv_then_report(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        bm, "write_scenario_csv",
        lambda log_dir, cfg, ts, cfg_path, cluster, store_nodes,
        result_dir=None: calls.append(("csv", log_dir)) or
        [{"instance": "TOTAL"}, {"instance": "w0"}])
    monkeypatch.setattr(
        bm, "_generate_reports",
        lambda cfg, log_dir, args, **kw: calls.append(("report", kw)) or
        [str(tmp_path / "report.html")])

    cfg = {"__cfg_path__": "configs/pipeline/demo/x.json"}
    args = SimpleNamespace(_matrix=None)
    scenario, rows = bm._post_process_config(
        cfg, "x", "pipeline", "ts", str(tmp_path),
        {"nodes": {"n1": {"ip": "1.1.1.1"}}}, ["n1"],
        {"nodes": 1, "qps_target": 105, "launch_mode": "single",
         "mode": "pipeline"}, args)
    assert scenario == "pipeline"
    assert rows == [{"instance": "TOTAL"}, {"instance": "w0"}]
    assert [c[0] for c in calls] == ["csv", "report"]
    assert calls[0][1] == os.path.join(str(tmp_path), "logs", "x")
    assert calls[1][1]["mode"] == "pipeline"


def test_post_process_kvtest_uses_map_reduce_chain(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(
        bm, "write_scenario_csv",
        lambda *a, **k: calls.append("csv") or [{"instance": "TOTAL"}])
    monkeypatch.setattr(
        bm, "_kvtest_generate_reports",
        lambda cfg, log_dir, args, early_stop=False:
        calls.append(("kvtest-report", log_dir)) or [])
    monkeypatch.setattr(
        bm, "_generate_reports",
        lambda *a, **k: calls.append("pipeline-report") or [])

    scenario, rows = bm._post_process_config(
        {"__cfg_path__": "configs/kvtest/demo/x.json"}, "x", "kvtest",
        "ts", str(tmp_path), {"nodes": {}}, [], {},
        SimpleNamespace(_matrix=None))
    assert scenario == "kvtest"
    assert rows == [{"instance": "TOTAL"}]
    assert calls == ["csv", ("kvtest-report",
                             os.path.join(str(tmp_path), "logs", "x"))]


# --------------------------------------------------------------- STOP path

def _stop_ctx(tmp_path):
    logdir = tmp_path / "ts" / "logs" / "cfg"
    logdir.mkdir(parents=True)
    (logdir / "cfg.benchmark_c1_t1_8MB.log").write_text(
        "RESULT\t2026-11-15 14:00:00.000000\tput\tk0\t1.5\tOK\n",
        encoding="utf-8")
    return {
        "cluster": {"nodes": []},
        "args": SimpleNamespace(no_report=False, png=False,
                                pull_raw_logs=False,
                                keep_intermediates=False,
                                dry_run=False, mode="ssh"),
        "report": {
            "cfg": {"__cfg_path__": "cfg.json", "scenario": "basic"},
            "cfg_name": "cfg",
            "result_dir": str(tmp_path / "ts"),
            "local_log_dir": str(logdir),
            "remote_log_dir": "/remote/logs",
            "involved_nodes": ["n1"],
            "mode": "basic",
        },
    }


def test_stop_reports_joins_background_before_overview(tmp_path, monkeypatch):
    order = []
    monkeypatch.setattr(bm, "_bg_join",
                        lambda timeout_s=None: order.append("join") or [])
    monkeypatch.setattr(bm, "pull_logs", lambda *a, **k: order.append("pull"))
    monkeypatch.setattr(
        bm, "_generate_reports",
        lambda *a, **k: order.append("report") or {"scenario": "basic"})
    monkeypatch.setattr(
        bm, "_render_run_overview",
        lambda *a, **k: order.append("overview") or "overview.html")
    monkeypatch.setattr(bm, "_STOP_CTX", _stop_ctx(tmp_path))
    bm._stop_reports()
    assert order[0] == "join", "STOP must settle background reports first"
    assert "report" in order and order[-1] == "overview"


# ------------------------------------------- main(): report/scenario overlap

def test_main_report_overlaps_next_scenario(tmp_path, monkeypatch):
    """The report for config A must run while scenario B is already running:
    B blocks until A's report sets the event, so a serialized (old) order
    would deadlock the test instead of passing."""
    import threading

    started = threading.Event()
    calls = {"reports": [], "overview": 0, "plots": 0}
    results_root = tmp_path / "results"
    results_root.mkdir()
    ts_dir = results_root / "run1"

    monkeypatch.setattr(bm, "RESULTS_DIR", str(results_root))
    monkeypatch.setattr(bm, "set_detail_log", lambda path: None)
    monkeypatch.setattr(bm, "discover_store_capacity", lambda *a, **k: None)
    monkeypatch.setattr(bm, "_pipeline_wipe_etcd", lambda *a, **k: None)
    monkeypatch.setattr(bm, "reap_state_entries", lambda *a, **k: None)
    monkeypatch.setattr(bm, "prof_core", None)
    bm.clear_stop()

    monkeypatch.setattr(bm, "load_json", lambda path: (
        {"nodes": {"n1": {"ip": "1.1.1.1"}}}
        if path.endswith("cluster.json") else
        {"master_type": "http", "master_addr": "1.1.1.1:50051",
         "etcd_endpoints": ""}))

    def _cfg(path):
        return {"scenario": "basic", "__cfg_path__": path}

    monkeypatch.setattr(bm, "load_scenario_config", _cfg)

    def _fake_scenario(cluster, globals_cfg, cfg, ts, args):
        name = cfg["__cfg_path__"].rsplit("/", 1)[-1]
        if name == "a.json":
            # --png + samples make the run-level PNG job fire
            logs = ts_dir / "logs" / "a"
            logs.mkdir(parents=True, exist_ok=True)
            (logs / "kv1.i0_p9101.kvtest_8MB.samples.csv").write_text(
                "operation,status\n", encoding="utf-8")
        if name == "b.json":
            assert started.wait(10), (
                "a.json report did not overlap scenario b (serialized)")
        return {}

    monkeypatch.setattr(bm, "run_basic_scenario", _fake_scenario)
    monkeypatch.setattr(
        bm, "write_scenario_csv",
        lambda log_dir, cfg, ts, cfg_path, cluster, store_nodes,
        result_dir=None: [])

    def _fake_report(cfg, log_dir, args, **kw):
        calls["reports"].append(os.path.basename(cfg["__cfg_path__"]))
        if cfg["__cfg_path__"].endswith("a.json"):
            started.set()
        return ["r.html"]

    monkeypatch.setattr(bm, "_generate_reports", _fake_report)
    monkeypatch.setattr(bm, "_render_run_overview",
                        lambda *a, **k: calls.__setitem__(
                            "overview", calls["overview"] + 1) or "ov.html")
    monkeypatch.setattr(bm, "_run_legacy_plots",
                        lambda log_dir: calls.__setitem__(
                            "plots", calls["plots"] + 1) or True)
    monkeypatch.setattr(bm, "_REPORT_ACC", [{"data": {}, "report_html": "x"}])

    args = SimpleNamespace(
        rebuild_report=None, stop_file=None, verbose=False,
        cluster=os.path.join(ROOT, "configs/_ssh/cluster.json"),
        global_cfg=os.path.join(ROOT, "configs/_ssh/global.json"),
        profile=None, mode="ssh", dry_run=False,
        result_dir=str(ts_dir), no_report=False, png=True,
        pull_raw_logs=True, keep_intermediates=False,
        clean_up=False, _matrix=None)
    bm.main(args, ["configs/basic/a.json", "configs/basic/b.json"])

    assert calls["reports"] == ["a.json", "b.json"]
    assert calls["overview"] == 1
    assert calls["plots"] == 1
    assert (ts_dir / "benchmark_detail.csv").is_file()
    assert (ts_dir / "logs" / "a" /
            "kv1.i0_p9101.kvtest_8MB.samples.csv").is_file()


# ------------------------------------------------- kvtest intermediates

def _mk_run(tmp_path, verify=True):
    ts = tmp_path / "ts"
    d = ts / "logs" / "cfg"
    d.mkdir(parents=True)
    (d / "kv1.i0_p9101.kvtest_8MB.samples.csv").write_text(
        "operation,status\n", encoding="utf-8")
    (d / "kv1.i0_p9101.kvtest_8MB.summary.json").write_text(
        "{}\n", encoding="utf-8")
    if verify:
        unit = d / "report" / "8MB"
        unit.mkdir(parents=True)
        (unit / "report.html").write_text("<html></html>\n")
        (unit / "data.js").write_text("window.__MC_FULL__={};\n")
    return ts, d


def _clean_args(**kw):
    base = dict(no_report=False, png=False, pull_raw_logs=False,
                keep_intermediates=False)
    base.update(kw)
    return SimpleNamespace(**base)


def test_cleanup_default_removes_after_verified_report(tmp_path, monkeypatch):
    monkeypatch.delenv("MC_KEEP_INTERMEDIATES", raising=False)
    ts, d = _mk_run(tmp_path)
    monkeypatch.setattr(bm, "_verify_config_reports", lambda log_dir: (True, "1 unit"))
    removed = bm._cleanup_run_intermediates(str(ts), [], _clean_args())
    assert removed == 2
    assert not (d / "kv1.i0_p9101.kvtest_8MB.samples.csv").exists()
    assert not (d / "kv1.i0_p9101.kvtest_8MB.summary.json").exists()
    assert (d / "report" / "8MB" / "report.html").is_file()


def test_cleanup_keeps_when_verify_fails(tmp_path, monkeypatch):
    monkeypatch.delenv("MC_KEEP_INTERMEDIATES", raising=False)
    ts, d = _mk_run(tmp_path, verify=False)
    assert bm._cleanup_run_intermediates(str(ts), [], _clean_args()) == 0
    assert (d / "kv1.i0_p9101.kvtest_8MB.samples.csv").is_file()


def test_cleanup_keeps_failed_configs(tmp_path, monkeypatch):
    monkeypatch.delenv("MC_KEEP_INTERMEDIATES", raising=False)
    ts, d = _mk_run(tmp_path)
    monkeypatch.setattr(bm, "_verify_config_reports", lambda log_dir: (True, "1 unit"))
    assert bm._cleanup_run_intermediates(
        str(ts), ["cfg"], _clean_args()) == 0
    assert (d / "kv1.i0_p9101.kvtest_8MB.samples.csv").is_file()


@pytest.mark.parametrize("args_kw,env", [
    (dict(keep_intermediates=True), None),
    (dict(), "1"),
    (dict(png=True, pull_raw_logs=True), None),
    (dict(pull_raw_logs=True), None),
    (dict(no_report=True), None),
])
def test_cleanup_escape_hatches(tmp_path, monkeypatch, args_kw, env):
    if env is None:
        monkeypatch.delenv("MC_KEEP_INTERMEDIATES", raising=False)
    else:
        monkeypatch.setenv("MC_KEEP_INTERMEDIATES", env)
    ts, d = _mk_run(tmp_path)
    monkeypatch.setattr(bm, "_verify_config_reports", lambda log_dir: (True, "1 unit"))
    assert bm._cleanup_run_intermediates(
        str(ts), [], _clean_args(**args_kw)) == 0
    assert (d / "kv1.i0_p9101.kvtest_8MB.samples.csv").is_file()
