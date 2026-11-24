#!/usr/bin/env python3
"""FAIL 路径不能把原始异常换成 UnboundLocalError（total_target_qps 在
try 内赋值，首个 stage 失败时 FAIL 处理器引用未绑定变量）。"""
import os
import sys
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

import benchmark as bm   # noqa: E402


def test_fail_path_keeps_original_error(tmp_path, monkeypatch):
    cluster = bm.load_json(os.path.join(ROOT, "mock", "cluster.json"))
    globals_cfg = bm.load_json(os.path.join(ROOT, "configs", "global.json"))
    cfg = bm.load_json(os.path.join(ROOT, "mock", "smoke", "put.json"))
    cfg["__cfg_path__"] = "mock/smoke/put.json"
    args = SimpleNamespace(
        no_warmup=True, dry_run=False, pull_raw_logs=False,
        png=False, mode="ssh", _result_dir_path=str(tmp_path), profile=None,
        clean_up=False, _matrix=None)
    for name in ("ensure_pods", "deploy_scripts", "ensure_configmap",
                 "_scenario_failure_report", "reap_state_entries"):
        monkeypatch.setattr(bm, name, lambda *a, **k: None)
    monkeypatch.setattr(bm, "_generate_reports", lambda *a, **k: None)

    def boom(*a, **k):
        raise RuntimeError("stage boom")
    monkeypatch.setattr(bm, "run_stage", boom)
    with pytest.raises(RuntimeError, match="stage boom"):
        bm.run_basic_scenario(cluster, globals_cfg, cfg, "20261115-9999", args)
    # the scenario reported the stage-aware FAIL line itself -> main must not
    # print the generic duplicate
    assert cfg["_fail_reported"] is True
