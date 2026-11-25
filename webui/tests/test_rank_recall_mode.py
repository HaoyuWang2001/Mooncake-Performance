import io
import json
import os
import shutil
import sys
import threading
import time
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import benchmark as bm
from benchmark import scenario_paths
from scripts.run_rank_recall import (_child_argv, _child_env, _pump,
                                     _render_parent_overview,
                                     partition_configs)
from webui.backend.routers.runs import _build_run_argv
from webui.backend.routers.outputs import api_outputs
from webui.backend.common import ApiError


def test_partition_uses_inherited_work_mode():
    groups = partition_configs(["configs/pipeline/demo"], [])
    assert [os.path.basename(p) for p in groups["rank"]] == ["simple_rank.json"]
    assert [os.path.basename(p) for p in groups["recall"]] == ["simple_recall.json"]


def test_children_have_isolated_namespace_ledgers():
    rank_env = _child_env("rank")
    recall_env = _child_env("recall")
    rank = rank_env["MC_BENCH_STATE_FILE"]
    recall = recall_env["MC_BENCH_STATE_FILE"]
    assert rank.endswith(".state.rank")
    assert recall.endswith(".state.recall")
    assert rank != recall
    assert rank_env["PYTHONUTF8"] == "1"
    assert rank_env["PYTHONIOENCODING"] == "utf-8"


def test_children_derive_ledger_from_parent_run(monkeypatch):
    """WebUI-managed combined run: child ledgers keyed off the run's state
    file so two concurrent combined runs never share a ledger."""
    monkeypatch.setenv("MC_BENCH_STATE_FILE", "/tmp/runs/r1.state")
    rank = _child_env("rank")["MC_BENCH_STATE_FILE"]
    recall = _child_env("recall")["MC_BENCH_STATE_FILE"]
    assert rank == "/tmp/runs/r1.state.rank"
    assert recall == "/tmp/runs/r1.state.recall"


def test_children_share_ready_dir_with_distinct_roles(tmp_path):
    args = SimpleNamespace(
        rank_cluster="rank-cluster.json", rank_global="rank-global.json",
        recall_cluster="recall-cluster.json", recall_global="recall-global.json",
        profile="profile.json", dry_run=False, no_warmup=False,
        verbose=False, protocol=None, stop_file=None)
    ready_dir = str(tmp_path / "ready")

    rank = _child_argv(args, "rank", ["rank.json"], "parent", ready_dir)
    recall = _child_argv(args, "recall", ["recall.json"], "parent", ready_dir)

    assert rank[rank.index("--parallel-ready-dir") + 1] == ready_dir
    assert recall[recall.index("--parallel-ready-dir") + 1] == ready_dir
    assert rank[rank.index("--parallel-ready-role") + 1] == "rank"
    assert recall[recall.index("--parallel-ready-role") + 1] == "recall"
    assert "--k8s" in rank and "--k8s" in recall


def test_children_support_bare_metal_ssh_mode(tmp_path):
    args = SimpleNamespace(
        mode="ssh",
        rank_cluster="rank-cluster.json", rank_global="rank-global.json",
        recall_cluster="recall-cluster.json",
        recall_global="recall-global.json", profile="profile.json",
        dry_run=False, no_warmup=False, verbose=False,
        protocol=None, stop_file=None)
    argv = _child_argv(args, "rank", ["rank.json"], "parent",
                       str(tmp_path / "ready"))
    assert "--ssh" in argv
    assert "--k8s" not in argv


def test_parallel_start_barrier_waits_for_peer(tmp_path, monkeypatch):
    monkeypatch.setattr(bm, "_check_stop", lambda: None)
    args = SimpleNamespace(parallel_ready_dir=str(tmp_path),
                           parallel_ready_role="rank")
    errors = []

    def run_barrier():
        try:
            bm._parallel_start_barrier(args)
        except Exception as exc:
            errors.append(exc)

    thread = threading.Thread(target=run_barrier)
    thread.start()
    deadline = time.time() + 2
    while not (tmp_path / "rank.ready").exists() and time.time() < deadline:
        time.sleep(0.01)
    assert (tmp_path / "rank.ready").exists()
    assert thread.is_alive(), "rank must wait until recall publishes readiness"

    (tmp_path / "recall.ready").write_text("peer\n", encoding="utf-8")
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert not errors
    assert args._parallel_ready_done is True


def test_log_pump_survives_character_not_encodable_by_console():
    class Proc:
        stdout = io.StringIO("bad replacement: \ufffd\n")

    raw = io.BytesIO()
    console = io.TextIOWrapper(raw, encoding="gbk", errors="strict")
    _pump("rank", Proc(), console)
    console.flush()

    assert raw.getvalue().decode("gbk") == \
        "[rank] bad replacement: \\ufffd\n"


def test_parallel_local_logs_stay_in_nested_child_result_dir():
    child_result = os.path.join(ROOT, "outputs", "parent-rank-recall", "rank")
    paths = scenario_paths(
        {"_pod_workspace": "/tmp/Mooncake-Performance"},
        "parent-rank-recall-rank", "rank_smoke", child_result)

    assert paths[2] == \
        "/tmp/Mooncake-Performance/logs/parent-rank-recall-rank"
    assert paths[-1] == os.path.join(child_result, "logs", "rank_smoke")


def test_dual_webui_command():
    body = {
        "selector": {"dirs": ["configs/pipeline/demo"]},
        "flags": {
            "mode": "k8s",
            "launch_mode": "rank_recall",
            "rank_cluster": "configs/_k8s/cluster.example.json",
            "rank_global": "configs/_k8s/global.example.json",
            "recall_cluster": "configs/_k8s/cluster.example.json",
            "recall_global": "configs/_k8s/global.example.json",
            "profile": "configs/_k8s/profile.example.json",
            "dry_run": True,
        },
    }
    argv = _build_run_argv(body)
    assert argv[1].endswith(os.path.join("scripts", "run_rank_recall.py"))
    assert "--k8s" in argv
    assert "--rank-cluster" in argv and "--recall-global" in argv
    assert "--dry-run" in argv


def test_dual_selection_requires_both_work_modes():
    body = {
        "selector": {"files": ["configs/pipeline/demo/simple_rank.json"]},
        "flags": {"mode": "k8s", "launch_mode": "rank_recall"},
    }
    try:
        _build_run_argv(body)
    except ApiError as exc:
        assert exc.code == 400
        assert "no recall test config selected" in exc.message
    else:
        raise AssertionError("rank-only parallel selection should be rejected")


def test_nested_dual_results_are_discoverable():
    name = "zz-rank-recall-test-{}".format(int(time.time() * 1000))
    root = os.path.join(ROOT, "outputs", name)
    rank = os.path.join(root, "rank")
    os.makedirs(rank)
    try:
        with open(os.path.join(rank, "benchmark_detail.csv"), "w") as fh:
            fh.write("a\n1\n")
        item = next(x for x in api_outputs(None, None)["outputs"]
                    if x["ts"] == name)
        assert item["has_detail"] is True
        assert item["csv_count"] == 1
    finally:
        shutil.rmtree(root, ignore_errors=True)


# ---------------------------------------------------------------------------
# 严格模式：恰好 1 rank + 1 recall，warmup/duration 一致
# ---------------------------------------------------------------------------
def _fake_cfgs(monkeypatch, specs):
    """Map config basename -> cfg dict for partition_configs tests."""
    def _load(path, configs_root=None):
        return specs[os.path.basename(path)]

    monkeypatch.setattr(bm, "load_scenario_config", _load)


def _cfg(mode, duration=600, warmup=5):
    return {"work_mode": mode, "duration": duration,
            "warmup": {"duration": warmup}}


def test_partition_rejects_two_configs_per_side(tmp_path, monkeypatch):
    names = ("r1.json", "r2.json", "c1.json")
    files = []
    for n in names:
        p = tmp_path / n
        p.write_text("{}", encoding="utf-8")
        files.append(str(p))
    _fake_cfgs(monkeypatch, {"r1.json": _cfg("rank"),
                             "r2.json": _cfg("rank"),
                             "c1.json": _cfg("recall")})
    with pytest.raises(ValueError, match="仅支持一份"):
        partition_configs([], files)


def test_partition_rejects_duration_mismatch(tmp_path, monkeypatch):
    r, c = tmp_path / "r.json", tmp_path / "c.json"
    r.write_text("{}", encoding="utf-8")
    c.write_text("{}", encoding="utf-8")
    _fake_cfgs(monkeypatch, {"r.json": _cfg("rank", duration=600),
                             "c.json": _cfg("recall", duration=601)})
    with pytest.raises(ValueError, match="warmup/duration 完全一致"):
        partition_configs([], [str(r), str(c)])


def test_partition_rejects_warmup_mismatch(tmp_path, monkeypatch):
    r, c = tmp_path / "r.json", tmp_path / "c.json"
    r.write_text("{}", encoding="utf-8")
    c.write_text("{}", encoding="utf-8")
    _fake_cfgs(monkeypatch, {"r.json": _cfg("rank", warmup=5),
                             "c.json": _cfg("recall", warmup=10)})
    with pytest.raises(ValueError, match="warmup/duration 完全一致"):
        partition_configs([], [str(r), str(c)])


def test_partition_accepts_equal_times(tmp_path, monkeypatch):
    r, c = tmp_path / "r.json", tmp_path / "c.json"
    r.write_text("{}", encoding="utf-8")
    c.write_text("{}", encoding="utf-8")
    _fake_cfgs(monkeypatch, {"r.json": _cfg("rank", duration=600, warmup=5),
                             "c.json": _cfg("recall", duration=600.0,
                                            warmup=5.0)})
    groups = partition_configs([], [str(r), str(c)])
    assert len(groups["rank"]) == 1 and len(groups["recall"]) == 1


# ---------------------------------------------------------------------------
# 子 run 持久化瘦身单元清单
# ---------------------------------------------------------------------------
def _unit_acc(tmp_path, name="u1", cfg="cfg_rank", ns="ns-rank", size="8KB"):
    html = (tmp_path / "ts" / "logs" / cfg / "report" / name / "report.html")
    html.parent.mkdir(parents=True, exist_ok=True)
    html.write_text("x", encoding="utf-8")
    data = {"meta": {"config": cfg, "mode": "pipeline",
                     "started": "2026-11-25 16:14:59", "k8s_namespace": ns},
            "sizes": {size: {"summary": {"ok": 5, "fail": 0},
                             "hist_bins": [[1, 2]], "bin_edges": [1.0, 2.0]}},
            "monitors": {"summary": {"peak_mem_mb": {"nodeA": 100}},
                         "mem_host": [{"t": 1}]},
            "nodes": [{"node": "nodeA", "pods": 1, "bytes": 3}]}
    return {"data": data, "report_html": str(html), "config": cfg,
            "unit": "c1 · " + size}


class _FakeRP:
    def __init__(self):
        self.calls = []

    def render_overview(self, run_meta, scenarios, out_dir, rel_base=None):
        self.calls.append((run_meta, scenarios, out_dir))
        return os.path.join(out_dir, "overview.html")


def test_render_run_overview_writes_slim_units(tmp_path, monkeypatch):
    acc = _unit_acc(tmp_path)
    monkeypatch.setattr(bm, "_REPORT_ACC", [acc])
    rp = _FakeRP()
    monkeypatch.setattr(bm, "_report_module", lambda: rp)
    bm._render_run_overview(str(tmp_path / "ts"), "pipeline")
    path = tmp_path / "ts" / "report" / "overview_units.json"
    doc = json.loads(path.read_text(encoding="utf-8"))
    unit = doc["units"][0]
    assert unit["report_html"] == acc["report_html"]
    assert unit["unit"] == "c1 · 8KB"
    assert unit["sc"]["sizes"]["8KB"] == {"summary": {"ok": 5, "fail": 0}}
    assert unit["sc"]["monitors"] == {
        "summary": {"peak_mem_mb": {"nodeA": 100}}}
    assert rp.calls and rp.calls[0][1][0]["report_rel"].startswith("..")


# ---------------------------------------------------------------------------
# 父级聚合 overview
# ---------------------------------------------------------------------------
def _write_child(parent, role, units):
    d = parent / role / "report"
    d.mkdir(parents=True, exist_ok=True)
    (d / "overview_units.json").write_text(
        json.dumps({"run": role, "units": units}), encoding="utf-8")


def _parent_unit(parent, role, name, cfg, ns):
    html = parent / role / "logs" / cfg / "report" / name / "report.html"
    html.parent.mkdir(parents=True, exist_ok=True)
    html.write_text("x", encoding="utf-8")
    return {"report_html": str(html), "unit": name,
            "sc": {"meta": {"config": cfg, "started": "2026-11-25 16:15:00",
                            "host_log_root": "/host/" + role,
                            "k8s_namespace": ns},
                   "sizes": {"8KB": {"summary": {"ok": 1}}},
                   "nodes": [{"node": "nodeA", "pods": 1,
                              "bytes": 10}]}}


def test_parent_overview_merges_both_children(tmp_path, monkeypatch):
    parent = tmp_path / "out" / "20261125-1614-rank-recall"
    _write_child(parent, "rank", [_parent_unit(
        parent, "rank", "u1", "cfg_rank", "ns-rank")])
    _write_child(parent, "recall", [_parent_unit(
        parent, "recall", "u1", "cfg_recall", "ns-recall")])
    rp = _FakeRP()
    monkeypatch.setattr(bm, "_report_module", lambda: rp)
    ov = _render_parent_overview(str(parent))
    assert ov and ov.endswith("overview.html")
    run_meta, scenarios, out_dir = rp.calls[0]
    assert out_dir == os.path.join(str(parent), "report")
    assert run_meta["mode"] == "rank+recall"
    assert run_meta["k8s_namespace"] == "ns-rank / ns-recall"
    assert [sc["meta"]["config"] for sc in scenarios] == [
        "cfg_rank", "cfg_recall"]
    assert scenarios[0]["report_rel"] == (
        "../rank/logs/cfg_rank/report/u1/report.html")
    assert scenarios[1]["report_rel"] == (
        "../recall/logs/cfg_recall/report/u1/report.html")


def test_parent_overview_tolerates_one_missing_side(tmp_path, monkeypatch):
    parent = tmp_path / "out" / "p1"
    _write_child(parent, "rank", [_parent_unit(
        parent, "rank", "u1", "cfg_rank", "ns-rank")])
    rp = _FakeRP()
    monkeypatch.setattr(bm, "_report_module", lambda: rp)
    _render_parent_overview(str(parent))
    assert len(rp.calls) == 1
    assert len(rp.calls[0][1]) == 1


def test_parent_overview_skips_when_no_children(tmp_path, monkeypatch):
    parent = tmp_path / "out" / "p2"
    (parent / "rank").mkdir(parents=True)
    rp = _FakeRP()
    monkeypatch.setattr(bm, "_report_module", lambda: rp)
    assert _render_parent_overview(str(parent)) is None
    assert rp.calls == []


# ---------------------------------------------------------------------------
# k8s-prerun 并发上传竞态：唯一名上传 + 原子改名
# ---------------------------------------------------------------------------
def test_k8s_prerun_uploads_unique_name_then_renames(tmp_path, monkeypatch):
    script = tmp_path / "k8s-prerun.sh"
    script.write_text("#!/bin/bash\necho hi\n", encoding="utf-8")
    monkeypatch.setattr(bm, "K8S_PRERUN", str(script))
    monkeypatch.setattr(bm, "k8s_settings",
                        lambda cluster: {"controller_workspace": "/ws"})
    monkeypatch.setattr(bm, "controller_desc", lambda c: "ctl")
    puts = []
    monkeypatch.setattr(bm, "controller_put",
                        lambda c, files, rd: puts.append(
                            (list(files), rd)))
    execs = []

    def _exec(c, cmd):
        execs.append(cmd)
        return "", 0

    monkeypatch.setattr(bm, "controller_exec", _exec)
    bm.k8s_prerun({"_controller": {"ip": "1.2.3.4"}})
    assert puts and puts[0][1] == "/ws"
    uploaded = os.path.basename(puts[0][0][0])
    assert uploaded.startswith("k8s-prerun.sh.")
    assert uploaded != "k8s-prerun.sh"
    assert execs[0].startswith("mv -f ")
    assert "/ws/k8s-prerun.sh" in execs[0]
    assert execs[1].startswith("bash ")


def test_controller_put_retries_size_mismatch(tmp_path, monkeypatch):
    import utils as u
    f = tmp_path / "a.sh"
    f.write_text("x" * 10, encoding="utf-8")
    state = {"put": 0, "open": 0}

    class _SFTP:
        def put(self, local, remote):
            state["put"] += 1
            if state["put"] == 1:
                raise IOError("size mismatch in put!  0 != 10")

        def close(self):
            pass

    class _Client:
        def open_sftp(self):
            state["open"] += 1
            return _SFTP()

    monkeypatch.setattr(u, "controller_is_local", lambda c: False)
    monkeypatch.setattr(u, "controller_mkdir", lambda c, p: None)
    monkeypatch.setattr(u, "_ssh_client", lambda c: _Client())
    u.controller_put({"ip": "1.2.3.4"}, [str(f)], "/ws")
    assert state["put"] == 2 and state["open"] == 2


def test_controller_put_propagates_other_errors(tmp_path, monkeypatch):
    import utils as u
    f = tmp_path / "a.sh"
    f.write_text("x", encoding="utf-8")

    class _SFTP:
        def put(self, local, remote):
            raise IOError("permission denied")

        def close(self):
            pass

    class _Client:
        def open_sftp(self):
            return _SFTP()

    monkeypatch.setattr(u, "controller_is_local", lambda c: False)
    monkeypatch.setattr(u, "controller_mkdir", lambda c, p: None)
    monkeypatch.setattr(u, "_ssh_client", lambda c: _Client())
    with pytest.raises(IOError, match="permission denied"):
        u.controller_put({"ip": "1.2.3.4"}, [str(f)], "/ws")
