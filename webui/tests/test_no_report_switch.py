#!/usr/bin/env python3
"""绘图（--png）与报表（--no-report）开关契约：
  1) CLI：绘图默认关闭、仅 --png 开启，且 --png 必须与 --pull-raw-logs 同开；
     报表开关与绘图互不隐含；
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
    assert args.png is False
    assert args.pull_raw_logs is False
    assert args.keep_intermediates is False
    assert args.no_report is False


def test_png_requires_pull_raw_logs(capsys):
    with pytest.raises(SystemExit) as exc:
        _parse(["--png"])
    assert exc.value.code == 2
    assert "开启 png 必须开启 pull-raw-logs" in capsys.readouterr().err
    args = _parse(["--png", "--pull-raw-logs"])
    assert args.png is True and args.pull_raw_logs is True


def test_png_does_not_imply_no_report():
    args = _parse(["--png", "--pull-raw-logs"])
    assert args.png is True and args.no_report is False


def test_no_report_does_not_touch_plotting():
    args = _parse(["--no-report"])
    assert args.no_report is True
    assert args.png is False
    args = _parse(["--no-report", "--png", "--pull-raw-logs",
                   "--keep-intermediates"])
    assert args.no_report is True and args.png is True
    assert args.keep_intermediates is True


# ------------------------------------------------------------ 报表门控

def _report_args(**kw):
    base = dict(no_report=False, dry_run=False, png=False,
                pull_raw_logs=False, keep_intermediates=False)
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


def test_plot_switch_still_generates_reports(monkeypatch):
    calls = []
    monkeypatch.setattr(bm, "_generate_report_units",
                        lambda *a, **k: calls.append("units") or ["r.html"])
    args = _report_args(png=True, pull_raw_logs=True)
    assert bm._generate_reports({}, "/tmp/none", args) == ["r.html"]
    assert calls == ["units"]


def test_dry_run_still_skips_reports(monkeypatch):
    calls = []
    monkeypatch.setattr(bm, "_generate_report_units",
                        lambda *a, **k: calls.append("units"))
    assert bm._generate_reports({}, "/tmp/none", _report_args(dry_run=True)) is None
    assert calls == []


def test_kvtest_reports_gated_by_no_report(monkeypatch):
    calls = []
    monkeypatch.setattr(bm, "_kvtest_finalize_stage",
                        lambda *a, **k: calls.append("finalize") or ["r"])
    monkeypatch.setattr(bm, "_kvtest_render_stage",
                        lambda *a, **k: calls.append("render"))
    cfg = {}
    assert bm._kvtest_generate_reports(cfg, "/tmp/none",
                                       _report_args(no_report=True)) is None
    assert calls == []
    bm._kvtest_generate_reports(cfg, "/tmp/none",
                                _report_args(png=True, pull_raw_logs=True))
    assert calls == ["finalize", "render"]


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


def test_stop_report_not_gated_by_plot_switch(monkeypatch):
    monkeypatch.delenv("MC_STOP_REPORT", raising=False)
    touched = []
    monkeypatch.setattr(bm, "clear_stop", lambda: None)
    monkeypatch.setattr(bm, "_generate_reports",
                        lambda *a, **k: touched.append("reduce") or ["r"])
    monkeypatch.setattr(bm, "_render_run_overview", lambda *a, **k: "ov.html")
    _stop_args(png=True, pull_raw_logs=True)
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

def test_webui_forwards_plot_and_report_flags():
    body = {"selector": {"files": ["mock/smoke/put.json"]},
            "flags": {"mode": "ssh",
                      "cluster": "mock/cluster.json",
                      "global": "mock/zz-global-test.json",
                      "profile": "mock/zz-global-test.json",
                      "png": True, "pull_raw_logs": True,
                      "keep_intermediates": True, "no_report": True}}
    argv = runs_router._build_run_argv(body)
    for flag in ("--png", "--pull-raw-logs", "--keep-intermediates",
                 "--no-report"):
        assert flag in argv, flag


def test_webui_png_requires_pull_raw_logs():
    body = {"selector": {"files": ["mock/smoke/put.json"]},
            "flags": {"mode": "ssh",
                      "cluster": "mock/cluster.json",
                      "global": "mock/zz-global-test.json",
                      "profile": "mock/zz-global-test.json",
                      "png": True}}
    with pytest.raises(ApiError) as exc:
        runs_router._build_run_argv(body)
    assert exc.value.code == 400
    assert "pull-raw-logs" in str(exc.value)


def test_webui_omits_disabled_flags():
    body = {"selector": {"files": ["mock/smoke/put.json"]},
            "flags": {"mode": "ssh",
                      "cluster": "mock/cluster.json",
                      "global": "mock/zz-global-test.json",
                      "profile": "mock/zz-global-test.json"}}
    argv = runs_router._build_run_argv(body)
    for flag in ("--no-plot", "--png", "--pull-raw-logs",
                 "--keep-intermediates", "--no-report"):
        assert flag not in argv, flag


def test_rank_recall_forwards_no_report_and_plot_flags():
    from scripts import run_rank_recall as rr
    args = SimpleNamespace(
        mode="k8s", rank_cluster="mock/cluster.json",
        rank_global="mock/zz-global-test.json",
        recall_cluster="mock/cluster.json",
        recall_global="mock/zz-global-test.json",
        profile="mock/zz-global-test.json", dry_run=False, no_warmup=False,
        png=True, pull_raw_logs=True, keep_intermediates=True,
        no_report=True, verbose=False, protocol=None, stop_file=None)
    argv = rr._child_argv(args, "rank", ["mock/smoke/put.json"], "parent",
                          "/tmp/ready")
    for flag in ("--no-report", "--png", "--pull-raw-logs",
                 "--keep-intermediates"):
        assert flag in argv, flag
    assert "--no-plot" not in argv
