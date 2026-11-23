#!/usr/bin/env python3
"""--no-plot / --no-report 两个独立开关的契约：
  1) CLI：两个参数互不隐含（parse_arguments）；
  2) 报表门控只认 no_report（场景 / kvtest / STOP 三条路径）；
  3) WebUI 重建报表端点（POST /api/runs/rebuild-report）的入参校验与 argv。
"""
import os
import sys
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import benchmark as bm                                     # noqa: E402
from webui.backend.common import ApiError                  # noqa: E402
from webui.backend.routers import runs as runs_router      # noqa: E402

_CFG = ["-f", "mock/smoke/put.json", "--ssh",
        "--cluster", "mock/cluster.json",
        "--global", "configs/_ssh/global.json"]


def _parse(extra=()):
    args, _cfg_paths = bm.parse_arguments(_CFG + list(extra))
    return args


# --------------------------------------------------------------- CLI 契约

def test_flags_default_off():
    args = _parse()
    assert args.no_plot is False
    assert args.no_report is False


def test_no_plot_does_not_imply_no_report():
    args = _parse(["--no-plot"])
    assert args.no_plot is True
    assert args.no_report is False


def test_no_report_does_not_imply_no_plot():
    args = _parse(["--no-report"])
    assert args.no_report is True
    assert args.no_plot is False


def test_both_flags_stay_independent():
    args = _parse(["--no-plot", "--no-report"])
    assert args.no_plot is True and args.no_report is True


# ------------------------------------------------------------ 报表门控

def _report_args(**kw):
    base = dict(no_plot=False, no_report=False, dry_run=False, png=False)
    base.update(kw)
    return SimpleNamespace(**base)


def test_no_report_skips_unit_reports(monkeypatch):
    calls = []
    monkeypatch.setattr(bm, "_generate_report_units",
                        lambda *a, **k: calls.append("units") or ["r.html"])
    assert bm._generate_reports({}, "/tmp/none", _report_args(no_report=True)) is None
    assert calls == []
    assert bm._generate_reports({}, "/tmp/none", _report_args()) == ["r.html"]
    assert calls == ["units"]


def test_no_plot_still_generates_reports(monkeypatch):
    calls = []
    monkeypatch.setattr(bm, "_generate_report_units",
                        lambda *a, **k: calls.append("units") or ["r.html"])
    assert bm._generate_reports({}, "/tmp/none", _report_args(no_plot=True)) == ["r.html"]
    assert calls == ["units"]


def test_dry_run_still_skips_reports(monkeypatch):
    calls = []
    monkeypatch.setattr(bm, "_generate_report_units",
                        lambda *a, **k: calls.append("units"))
    assert bm._generate_reports({}, "/tmp/none", _report_args(dry_run=True)) is None
    assert calls == []


def test_kvtest_reports_gated_by_no_report(monkeypatch):
    calls = []
    monkeypatch.setattr(bm, "_kvtest_map_stats",
                        lambda *a, **k: calls.append("map"))
    monkeypatch.setattr(bm, "_generate_reports",
                        lambda *a, **k: calls.append("reduce"))
    cfg = {}
    assert bm._kvtest_generate_reports(cfg, "/tmp/none",
                                       _report_args(no_report=True)) is None
    assert calls == []
    bm._kvtest_generate_reports(cfg, "/tmp/none", _report_args(no_plot=True))
    assert calls == ["map", "reduce"]


def _stop_args(**kw):
    bm._STOP_CTX.clear()
    bm._STOP_CTX["args"] = _report_args(**kw)
    bm._STOP_CTX["cluster"] = {"_nodes": []}
    bm._STOP_CTX["report"] = {"mode": "pipeline"}


def test_stop_report_gated_by_no_report(monkeypatch, tmp_path):
    monkeypatch.delenv("MC_STOP_REPORT", raising=False)
    touched = []
    monkeypatch.setattr(bm, "_generate_reports",
                        lambda *a, **k: touched.append("reduce"))
    _stop_args(no_report=True)
    bm._stop_reports()
    assert touched == []
    bm._STOP_CTX.clear()


def test_stop_report_not_gated_by_no_plot(monkeypatch):
    monkeypatch.delenv("MC_STOP_REPORT", raising=False)
    touched = []
    monkeypatch.setattr(bm, "clear_stop", lambda: None)
    monkeypatch.setattr(bm, "_generate_reports",
                        lambda *a, **k: touched.append("reduce") or ["r"])
    monkeypatch.setattr(bm, "_render_run_overview", lambda *a, **k: "ov.html")
    _stop_args(no_plot=True)
    bm._STOP_CTX["args"].mode = "ssh"
    bm._STOP_CTX["cluster"] = {"node": "n1"}
    bm._STOP_CTX["report"] = {"mode": "basic", "skip_pull": True, "cfg": {},
                              "local_log_dir": "/tmp/none",
                              "result_dir": "/tmp/none", "involved_nodes": [],
                              "remote_log_dir": "", "cfg_name": "x"}
    bm._stop_reports()
    assert touched == ["reduce"]
    bm._STOP_CTX.clear()


# ------------------------------------------------- 重建报表端点（入参校验）

@pytest.fixture
def outputs(tmp_path, monkeypatch):
    monkeypatch.setattr(runs_router, "OUTPUTS_DIR", str(tmp_path))
    return tmp_path


def test_rebuild_argv_for_existing_run(outputs):
    (outputs / "20261112-1804" / "logs").mkdir(parents=True)
    argv = runs_router._build_rebuild_argv({"ts": "20261112-1804"})
    assert argv[1] == runs_router.runmgr.BENCH
    assert argv[-2:] == ["--rebuild-report", "20261112-1804"]


def test_rebuild_rejects_bad_ts(outputs):
    for bad in ("", "   ", ".", "..", "a/b", "a\\b", "../20261112-1804"):
        with pytest.raises(ApiError) as exc:
            runs_router._build_rebuild_argv({"ts": bad})
        assert exc.value.code == 400, bad


def test_rebuild_missing_run_is_404(outputs):
    with pytest.raises(ApiError) as exc:
        runs_router._build_rebuild_argv({"ts": "20990101-0000"})
    assert exc.value.code == 404


def test_rebuild_without_logs_is_400(outputs):
    (outputs / "20261112-1804").mkdir()
    with pytest.raises(ApiError) as exc:
        runs_router._build_rebuild_argv({"ts": "20261112-1804"})
    assert exc.value.code == 400


def test_rebuild_refuses_run_still_writing(outputs, monkeypatch):
    (outputs / "20261112-1804" / "logs").mkdir(parents=True)
    monkeypatch.setattr(runs_router, "_active_run_for_ts",
                        lambda ts: "r20261112-1804-001")
    with pytest.raises(ApiError) as exc:
        runs_router._build_rebuild_argv({"ts": "20261112-1804"})
    assert exc.value.code == 409


# --------------------------------------------------- WebUI 参数转发

def test_webui_forwards_both_flags():
    body = {"selector": {"files": ["mock/smoke/put.json"]},
            "flags": {"mode": "ssh",
                      "cluster": "mock/cluster.json",
                      "global": "mock/zz-global-test.json",
                      "profile": "mock/zz-global-test.json",
                      "no_plot": True, "no_report": True}}
    argv = runs_router._build_run_argv(body)
    assert "--no-plot" in argv and "--no-report" in argv


def test_webui_omits_disabled_flags():
    body = {"selector": {"files": ["mock/smoke/put.json"]},
            "flags": {"mode": "ssh",
                      "cluster": "mock/cluster.json",
                      "global": "mock/zz-global-test.json",
                      "profile": "mock/zz-global-test.json"}}
    argv = runs_router._build_run_argv(body)
    assert "--no-plot" not in argv and "--no-report" not in argv


def test_rank_recall_forwards_no_report():
    from scripts import run_rank_recall as rr
    args = SimpleNamespace(
        mode="k8s", rank_cluster="mock/cluster.json",
        rank_global="mock/zz-global-test.json",
        recall_cluster="mock/cluster.json",
        recall_global="mock/zz-global-test.json",
        profile="mock/zz-global-test.json", dry_run=False, no_warmup=False,
        no_plot=False, no_report=True, verbose=False, protocol=None,
        stop_file=None)
    argv = rr._child_argv(args, "rank", ["mock/smoke/put.json"], "parent",
                          "/tmp/ready")
    assert "--no-report" in argv and "--no-plot" not in argv
