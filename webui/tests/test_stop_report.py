#!/usr/bin/env python3
"""STOP 路径也要产出部分报表（StopRequested 是 BaseException，场景的
except Exception 接不住，此前停止后没有任何 report.html/overview.html）。"""
import os
import sys
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

import benchmark as bm   # noqa: E402


ROWS = "".join(
    "RESULT\t2026-11-15 14:00:{:02d}.{:06d}\tput\tk{}\t1.5\tOK\n".format(
        i % 60, i, i) for i in range(30))


def _mk_run(tmp_path, ts="20261115-1400", cfg_name="cfg"):
    logdir = tmp_path / ts / "logs" / cfg_name
    logdir.mkdir(parents=True)
    (logdir / "cfg.benchmark_c1_t1_8MB.log").write_text(ROWS, encoding="utf-8")
    return tmp_path / ts, logdir


def _ctx(tmp_path, ts, logdir, cfg_name="cfg"):
    return {
        "cfg": {"__cfg_path__": "cfg.json", "scenario": "basic"},
        "cfg_name": cfg_name,
        "result_dir": str(tmp_path / ts),
        "local_log_dir": str(logdir),
        "remote_log_dir": "/remote/logs",
        "involved_nodes": ["n1"],
        "mode": "basic",
    }


def test_stop_reports_partial(tmp_path, monkeypatch):
    ts, logdir = _mk_run(tmp_path)
    pulled = []
    monkeypatch.setattr(bm, "pull_logs",
                        lambda *a, **k: pulled.append(a[:4]))
    monkeypatch.setattr(bm, "_REPORT_ACC", [])
    monkeypatch.setattr(bm, "_STOP_CTX", {
        "cluster": {"nodes": []}, "args": SimpleNamespace(
            no_plot=False, dry_run=False, mode="ssh"),
        "report": _ctx(tmp_path, ts, logdir)})
    bm._stop_reports()
    assert pulled, "stop 前必须先把远端日志拉回来"
    assert (logdir / "report" / "report.html").is_file()
    assert (tmp_path / ts / "report" / "overview.html").is_file()


def test_stop_reports_can_be_disabled(tmp_path, monkeypatch):
    ts, logdir = _mk_run(tmp_path)
    monkeypatch.setattr(bm, "pull_logs", lambda *a, **k: None)
    monkeypatch.setattr(bm, "_REPORT_ACC", [])
    monkeypatch.setattr(bm, "_STOP_CTX", {
        "cluster": {"nodes": []}, "args": SimpleNamespace(
            no_plot=False, dry_run=False, mode="ssh"),
        "report": _ctx(tmp_path, ts, logdir)})
    monkeypatch.setenv("MC_STOP_REPORT", "0")
    bm._stop_reports()
    assert not (logdir / "report" / "report.html").exists()
