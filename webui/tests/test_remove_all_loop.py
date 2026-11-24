#!/usr/bin/env python3
"""remove_all 循环收敛：单次 remove_all 只入队放行窗口内的对象。

master 的 oplog（--oplog_batch_max_entries，默认 1024）在背压下会静默跳过
溢出对象（不重试、不报错），因此 "成功" 只代表本轮入队数；客户端必须重复
调用直到某轮 cleared==0，才能保证 key 空间真正收敛。
"""
import argparse
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPTS = os.path.join(ROOT, "scripts")
sys.path.insert(0, SCRIPTS)

import mooncake_benchmark_client as client  # noqa: E402


class FakeStore:
    def __init__(self, returns):
        self._returns = list(returns)
        self.calls = 0

    def remove_all(self, force=False):
        self.calls += 1
        if not self._returns:
            return 0
        return self._returns.pop(0)


def _settings(**kw):
    args = argparse.Namespace()
    for k, v in kw.items():
        setattr(args, k, v)
    return args


def test_converges_after_backpressure_passes(monkeypatch):
    sleeps = []
    monkeypatch.setattr(client.time, "sleep", lambda s: sleeps.append(s))
    store = FakeStore([5000, 300, 0])
    passes, total = client._remove_all_until_drained(store, 32, 1.0)
    assert (passes, total) == (3, 5300)
    assert store.calls == 3
    assert sleeps == [1.0, 1.0]          # no sleep after the converged pass


def test_empty_store_single_pass():
    store = FakeStore([0])
    assert client._remove_all_until_drained(store, 32, 1.0) == (1, 0)
    assert store.calls == 1


def test_rpc_failure_propagates():
    store = FakeStore([-7])
    assert client._remove_all_until_drained(store, 32, 1.0) == (-1, -7)
    assert store.calls == 1


def test_max_passes_guard(monkeypatch):
    monkeypatch.setattr(client.time, "sleep", lambda s: None)
    store = FakeStore([1000] * 10)
    passes, total = client._remove_all_until_drained(store, 3, 0)
    assert passes == -3 and total == 3000
    assert store.calls == 3


def test_legacy_single_pass_accepts_nonzero(monkeypatch):
    monkeypatch.delenv("MC_REMOVE_ALL_MAX_PASSES", raising=False)
    store = FakeStore([700])
    passes, total = client._remove_all_checked(
        store, _settings(remove_all_max_passes=1,
                         remove_all_pass_delay=0), "remove-keys full")
    assert (passes, total) == (1, 700)
    assert store.calls == 1


def test_checked_exits_when_not_drained(monkeypatch, capsys):
    monkeypatch.setattr(client.time, "sleep", lambda s: None)
    store = FakeStore([10, 10])
    with pytest.raises(SystemExit):
        client._remove_all_checked(
            store, _settings(remove_all_max_passes=2,
                             remove_all_pass_delay=0), "remove-keys full")
    err = capsys.readouterr().out
    assert "not drained after 2 passes" in err
    assert "oplog_batch_max_entries" in err


def test_checked_exits_on_rpc_failure(capsys):
    store = FakeStore([-5])
    with pytest.raises(SystemExit):
        client._remove_all_checked(
            store, _settings(remove_all_max_passes=4,
                             remove_all_pass_delay=0), "remove-keys full")
    assert "remove_all returned -5" in capsys.readouterr().out


def test_settings_env_fallback_and_overrides(monkeypatch):
    monkeypatch.setenv("MC_REMOVE_ALL_MAX_PASSES", "5")
    monkeypatch.setenv("MC_REMOVE_ALL_PASS_DELAY", "0.25")
    assert client._remove_all_settings(_settings()) == (5, 0.25)
    # explicit args win over env
    assert client._remove_all_settings(
        _settings(remove_all_max_passes=9, remove_all_pass_delay=2)) == (9, 2.0)
    # invalid values fall back to defaults
    monkeypatch.setenv("MC_REMOVE_ALL_MAX_PASSES", "abc")
    monkeypatch.setenv("MC_REMOVE_ALL_PASS_DELAY", "xyz")
    assert client._remove_all_settings(_settings()) == (32, 1.0)


def test_prepare_store_drains_first(monkeypatch, capsys):
    calls = []

    def fake_checked(store, args, what):
        calls.append(what)
        return 2, 100

    monkeypatch.setattr(client, "_remove_all_checked", fake_checked)
    monkeypatch.setattr(client, "load_value", lambda *a: b"v")
    monkeypatch.setattr(client, "build_keys", lambda *a: ["k1"])
    monkeypatch.setattr(client, "emit", lambda *a: None)

    class RunStore(FakeStore):
        def batch_is_exist(self, keys):
            return [0] * len(keys)

        def put(self, k, v, config=None):
            return 0

    store = RunStore([])
    client.cmd_prepare_store(
        _settings(scenario="s", remote_data_dir="/tmp/x",
                  key_prefix="p", key_count=1), store)
    assert calls == ["prepare-store remove-all"]
    out = capsys.readouterr().out
    assert "prepare-store remove-all done (cleared=100, passes=2)" in out
