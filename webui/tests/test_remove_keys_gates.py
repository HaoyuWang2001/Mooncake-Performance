"""remove-keys gates: pre-check skip + oplog release wait."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import benchmark as bm


def _snap(allocated, total=1000000):
    return {
        "allocated": float(allocated),
        "total": float(total),
        "pct": (float(allocated) / float(total)) if total else 0.0,
        "seg_alloc": [float(allocated)],
        "seg_cap": [float(total)],
        "source": "test",
    }


def test_remove_skip_knob_defaults_on():
    assert bm._remove_skip_when_clean({}) is True
    assert bm._remove_skip_when_clean(
        {"pipeline": {"remove_skip_when_clean": False}}) is False


def test_remove_release_timeout_reader():
    assert bm._remove_release_timeout_s({}) == 120
    assert bm._remove_release_timeout_s(
        {"pipeline": {"remove_release_timeout_s": 0}}) == 0
    assert bm._remove_release_timeout_s(
        {"pipeline": {"remove_release_timeout_s": "45"}}) == 45
    assert bm._remove_release_timeout_s(
        {"pipeline": {"remove_release_timeout_s": "abc"}}) == 120
    assert bm._remove_release_timeout_s(
        {"pipeline": {"remove_release_timeout_s": -3}}) == 0


def test_store_snapshot_clean_uses_tolerance():
    assert bm._store_snapshot_is_clean(_snap(0)) is True
    # 0.5% allocated is within the 1% tolerance -> clean
    assert bm._store_snapshot_is_clean(_snap(5000)) is True
    # 2% is dirty
    assert bm._store_snapshot_is_clean(_snap(20000)) is False
    # unavailable probe is never clean
    assert bm._store_snapshot_is_clean(None) is False


def test_precheck_skips_when_clean(monkeypatch, capsys):
    monkeypatch.setattr(bm, "_store_usage_snapshot", lambda *a, **k: _snap(0))
    dropped = []
    monkeypatch.setattr(bm, "state_remove_cfg",
                        lambda name: dropped.append(name))
    assert bm._remove_keys_precheck(None, {}, "cfg", "start") is True
    assert dropped == ["cfg"]
    out = capsys.readouterr().out
    assert "store already clean" in out and "skipping remove-keys" in out


def test_precheck_runs_when_dirty(monkeypatch):
    monkeypatch.setattr(bm, "_store_usage_snapshot",
                        lambda *a, **k: _snap(500000))
    dropped = []
    monkeypatch.setattr(bm, "state_remove_cfg",
                        lambda name: dropped.append(name))
    assert bm._remove_keys_precheck(None, {}, "cfg", "start") is False
    assert dropped == []


def test_precheck_failsafe_when_probe_unavailable(monkeypatch):
    monkeypatch.setattr(bm, "_store_usage_snapshot", lambda *a, **k: None)
    assert bm._remove_keys_precheck(None, {}, "cfg", "start") is False


def test_precheck_disabled_by_knob(monkeypatch):
    monkeypatch.setattr(bm, "_store_usage_snapshot", lambda *a, **k: _snap(0))
    assert bm._remove_keys_precheck(
        None, {"pipeline": {"remove_skip_when_clean": False}},
        "cfg", "start") is False


def test_wait_degenerates_when_disabled(monkeypatch):
    verified = []

    def fake_verify(cluster, globals_cfg, cfg_name, when):
        verified.append(when)
        return "snap"

    monkeypatch.setattr(bm, "_verify_store_clean", fake_verify)
    assert bm._wait_store_released(
        None, {"pipeline": {"remove_release_timeout_s": 0}},
        "cfg", "start") == "snap"
    assert verified == ["start"]


def test_wait_polls_until_released(monkeypatch):
    seq = [_snap(80000), _snap(40000), _snap(0)]
    probes = []

    def fake_snap(*a, **k):
        probes.append(1)
        return seq.pop(0) if seq else _snap(0)

    monkeypatch.setattr(bm, "_store_usage_snapshot", fake_snap)
    monkeypatch.setattr(bm.time, "sleep", lambda s: None)
    verified = []

    def fake_verify(cluster, globals_cfg, cfg_name, when):
        verified.append(when)
        return "snap"

    monkeypatch.setattr(bm, "_verify_store_clean", fake_verify)
    assert bm._wait_store_released(None, {}, "cfg", "start") == "snap"
    # initial probe + at least two loop probes
    assert len(probes) >= 3
    assert verified == ["start"]


def test_wait_timeout_honours_require_clean_store(monkeypatch):
    monkeypatch.setattr(bm, "_store_usage_snapshot",
                        lambda *a, **k: _snap(80000))
    clock = {"t": 1000.0}
    monkeypatch.setattr(bm.time, "time", lambda: clock["t"])
    monkeypatch.setattr(
        bm.time, "sleep",
        lambda s: clock.__setitem__("t", clock["t"] + s))
    try:
        bm._wait_store_released(
            None, {"pipeline": {"remove_release_timeout_s": 5,
                                "require_clean_store": True}},
            "cfg", "start")
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "NOT clean" in str(exc)


def test_wait_timeout_warns_by_default(monkeypatch, capsys):
    monkeypatch.setattr(bm, "_store_usage_snapshot",
                        lambda *a, **k: _snap(80000))
    clock = {"t": 1000.0}
    monkeypatch.setattr(bm.time, "time", lambda: clock["t"])
    monkeypatch.setattr(
        bm.time, "sleep",
        lambda s: clock.__setitem__("t", clock["t"] + s))
    snap = bm._wait_store_released(
        None, {"pipeline": {"remove_release_timeout_s": 3}}, "cfg", "start")
    assert snap is not None
    out = capsys.readouterr().out
    assert "not fully empty" in out and "[WARN]" in out


def _stage_calls(monkeypatch):
    calls = {"run": [], "wait": [], "precheck": []}
    monkeypatch.setattr(
        bm, "run_stage",
        lambda *a, **k: calls["run"].append((a, k)))
    monkeypatch.setattr(
        bm, "_wait_store_released",
        lambda *a: calls["wait"].append(a))
    return calls


def test_stage_skips_when_precheck_true(monkeypatch):
    calls = _stage_calls(monkeypatch)
    monkeypatch.setattr(bm, "_remove_keys_precheck",
                        lambda *a: calls["precheck"].append(a) or True)
    ran = bm._remove_keys_stage(
        None, {"pipeline": {}}, "h", None, "runner", "/r", "cfg", "cfg",
        "full", "pre-run", dry=False)
    assert ran is False
    assert calls["run"] == [] and calls["wait"] == []


def test_stage_executes_full_then_waits(monkeypatch):
    calls = _stage_calls(monkeypatch)
    monkeypatch.setattr(bm, "_remove_keys_precheck",
                        lambda *a: calls["precheck"].append(a) or False)
    ran = bm._remove_keys_stage(
        None, {"pipeline": {}}, "h", "node", "runner", "/r", "cfg",
        "remove-keys-end", "full", "config end", dry=False,
        pull_to="/local", node_name="pod-1", label="L")
    assert ran is True
    (args, kwargs), = calls["run"]
    assert args[2:6] == ("runner", "remove-keys", "/r", "remove-keys-end")
    assert kwargs["extra"] == "--scope full"
    assert kwargs["pull_to"] == "/local"
    assert kwargs["cfg_name"] == "cfg"
    assert kwargs["node_name"] == "pod-1"
    assert kwargs["label"] == "L"
    assert kwargs["dry_run"] is False
    assert calls["wait"], "full scope must wait for oplog release"


def test_stage_key_prefix_never_waits(monkeypatch):
    calls = _stage_calls(monkeypatch)
    monkeypatch.setattr(bm, "_remove_keys_precheck",
                        lambda *a: calls["precheck"].append(a) or False)
    ran = bm._remove_keys_stage(
        None, {"pipeline": {}}, "h", "node", "runner", "/r", "cfg", "cfg",
        "key-prefix", "config end", dry=False)
    assert ran is True
    assert calls["run"] and calls["wait"] == []
    # pre-check still applies to key-prefix
    assert calls["precheck"]


def test_stage_dry_runs_without_gates(monkeypatch):
    calls = _stage_calls(monkeypatch)
    monkeypatch.setattr(bm, "_remove_keys_precheck",
                        lambda *a: calls["precheck"].append(a) or True)
    ran = bm._remove_keys_stage(
        None, {}, "h", "node", "runner", "/r", "cfg", "cfg", "full",
        "pre-run", dry=True, pull_to="/local")
    assert ran is True
    assert calls["precheck"] == [] and calls["wait"] == []
    (args, kwargs), = calls["run"]
    assert kwargs["dry_run"] is True


def test_stage_without_globals_is_ungated(monkeypatch):
    calls = _stage_calls(monkeypatch)
    monkeypatch.setattr(bm, "_remove_keys_precheck",
                        lambda *a: calls["precheck"].append(a) or True)
    ran = bm._remove_keys_stage(
        None, None, "h", "node", "runner", "/r", "cfg", "cfg", "full",
        "fail cleanup", dry=False)
    assert ran is True
    assert calls["precheck"] == [] and calls["wait"] == []
    assert calls["run"]
