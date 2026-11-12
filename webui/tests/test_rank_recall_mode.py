import io
import os
import shutil
import sys
import threading
import time
from types import SimpleNamespace

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import benchmark as bm
from benchmark import scenario_paths
from scripts.run_rank_recall import (_child_argv, _child_env, _pump,
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


def test_children_share_ready_dir_with_distinct_roles(tmp_path):
    args = SimpleNamespace(
        rank_cluster="rank-cluster.json", rank_global="rank-global.json",
        recall_cluster="recall-cluster.json", recall_global="recall-global.json",
        profile="profile.json", dry_run=False, no_warmup=False,
        no_plot=False, verbose=False, protocol=None, stop_file=None)
    ready_dir = str(tmp_path / "ready")

    rank = _child_argv(args, "rank", ["rank.json"], "parent", ready_dir)
    recall = _child_argv(args, "recall", ["recall.json"], "parent", ready_dir)

    assert rank[rank.index("--parallel-ready-dir") + 1] == ready_dir
    assert recall[recall.index("--parallel-ready-dir") + 1] == ready_dir
    assert rank[rank.index("--parallel-ready-role") + 1] == "rank"
    assert recall[recall.index("--parallel-ready-role") + 1] == "recall"


def test_parallel_pods_ready_barrier_waits_for_peer(tmp_path, monkeypatch):
    monkeypatch.setattr(bm, "_check_stop", lambda: None)
    args = SimpleNamespace(parallel_ready_dir=str(tmp_path),
                           parallel_ready_role="rank")
    errors = []

    def run_barrier():
        try:
            bm._parallel_pods_ready_barrier(args)
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
