#!/usr/bin/env python3
"""run 级 remove 服务：HTTP 处理语义 + _remove_keys_stage 分支 + 清理。"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import benchmark as bm  # noqa: E402
import remove_service as rs  # noqa: E402


class FakeStore:
    def __init__(self, rets=None, exist=None, status=None):
        self.rets = list(rets or [])
        self.exist = exist
        self.status = status

    def remove_all(self, force=False):
        return self.rets.pop(0) if self.rets else 0

    def batch_is_exist(self, keys):
        return self.exist

    def batch_remove(self, keys, force=False):
        return self.status


def test_service_full_scope_converges(monkeypatch):
    rs._STATE["store"] = FakeStore([3, 2, 0])
    res = rs._handle_remove({"scope": "full", "max_passes": 5,
                             "pass_delay": 0})
    assert res["ok"] is True
    assert res["cleared"] == 5 and res["passes"] == 3


def test_service_full_scope_rpc_error():
    rs._STATE["store"] = FakeStore([-7])
    res = rs._handle_remove({"scope": "full", "max_passes": 3,
                             "pass_delay": 0})
    assert res["ok"] is False and "remove_all returned -7" in res["error"]


def test_service_full_scope_max_passes():
    rs._STATE["store"] = FakeStore([1, 1, 1])
    res = rs._handle_remove({"scope": "full", "max_passes": 2,
                             "pass_delay": 0})
    assert res["ok"] is False and res["passes"] == 2


def test_service_key_prefix_classification(monkeypatch):
    monkeypatch.setenv("BENCH_KEY_PREFIX", "mc_bench")
    monkeypatch.setenv("BENCH_SCENARIO", "s")
    monkeypatch.setenv("BENCH_KEY_COUNT", "3")
    rs._STATE["store"] = FakeStore(exist=[1, 0, 1], status=[0, None, -1])
    res = rs._handle_remove({"scope": "key-prefix"})
    assert res["ok"] is False          # one key failed
    assert res["cleared"] == 1         # one removed
    assert res["not_found"] == 1 and res["failed"] == 1


def _stage_args(monkeypatch, service_result):
    calls = {"run": [], "wait": [], "svc": []}
    monkeypatch.setattr(bm, "_remove_keys_precheck", lambda *a: False)
    monkeypatch.setattr(
        bm, "_remove_service_call",
        lambda *a: calls["svc"].append(a) or service_result)
    monkeypatch.setattr(bm, "run_stage",
                        lambda *a, **k: calls["run"].append((a, k)))
    monkeypatch.setattr(bm, "_wait_store_released",
                        lambda *a: calls["wait"].append(a))
    return calls


def test_stage_uses_service_full_then_waits(monkeypatch):
    calls = _stage_args(monkeypatch, {"ok": True})
    ran = bm._remove_keys_stage(
        {"_controller": {}}, {"pipeline": {}}, "h", "n", "r", "/log",
        "cfg", "cfg", "full", "config end")
    assert ran is True
    assert calls["run"] == [] and calls["svc"]
    assert len(calls["wait"]) == 1


def test_stage_key_prefix_service_skips_wait(monkeypatch):
    calls = _stage_args(monkeypatch, {"ok": True})
    bm._remove_keys_stage(
        {"_controller": {}}, {"pipeline": {}}, "h", "n", "r", "/log",
        "cfg", "cfg", "key-prefix", "config end")
    assert calls["run"] == [] and calls["wait"] == []


def test_stage_falls_back_to_legacy(monkeypatch):
    calls = _stage_args(monkeypatch, None)
    bm._remove_keys_stage(
        {"_controller": {}}, {"pipeline": {}}, "h", "n", "r", "/log",
        "cfg", "cfg", "full", "pre-run")
    assert calls["run"] and calls["svc"]
    assert len(calls["wait"]) == 1


def test_stage_precheck_skip_uses_nothing(monkeypatch):
    calls = {"run": [], "svc": []}
    monkeypatch.setattr(bm, "_remove_keys_precheck", lambda *a: True)
    monkeypatch.setattr(bm, "_remove_service_call",
                        lambda *a: calls["svc"].append(a) or {"ok": True})
    monkeypatch.setattr(bm, "run_stage",
                        lambda *a, **k: calls["run"].append(a))
    ran = bm._remove_keys_stage(
        {"_controller": {}}, {"pipeline": {}}, "h", "n", "r", "/log",
        "cfg", "cfg", "full", "pre-run")
    assert ran is False
    assert calls["run"] == [] and calls["svc"] == []


def test_prewarm_skips_short_window(monkeypatch):
    calls = []
    monkeypatch.setattr(bm, "_remove_service_enabled", lambda *a: True)
    monkeypatch.setattr(bm, "_remove_service_ensure",
                        lambda *a, **k: calls.append(a) or True)
    cluster = {"_controller": {"host": "c"}}
    bm._remove_service_prewarm_async(cluster, {}, "ts", "cfg", False,
                                     window_s=30)
    assert calls == [] and "_remove_svc" not in cluster


def test_prewarm_starts_long_window(monkeypatch):
    calls = []

    def _ensure(cluster, *a, **k):
        calls.append(cluster)
        cluster["_remove_svc"]["ok"] = True
        return True

    monkeypatch.setattr(bm, "_remove_service_enabled", lambda *a: True)
    monkeypatch.setattr(bm, "_remove_service_ensure", _ensure)
    cluster = {"_controller": {"host": "c"}}
    bm._remove_service_prewarm_async(cluster, {}, "ts", "cfg", False,
                                     window_s=600)
    assert cluster["_remove_svc"]["event"].wait(5)
    assert calls and cluster["_remove_svc"]["ok"] is True


def test_cleanup_is_idempotent(monkeypatch):
    calls = []
    monkeypatch.setattr(
        bm, "controller_run",
        lambda c, argv, check=True, timeout=None:
        (calls.append(list(argv)), ("", 0))[1])
    monkeypatch.setattr(bm, "k8s_settings",
                        lambda cluster: {"namespace": "ns1"})
    bm._remove_service_cleanup({"_controller": {"host": "c"}})
    assert calls == []                      # no state -> no-op
    bm._remove_service_cleanup(
        {"_controller": {"host": "c"},
         "_remove_svc": {"deploy": "mc-x-remove"}})
    assert calls and calls[0][:4] == ["-n", "ns1", "delete", "deployment"]
