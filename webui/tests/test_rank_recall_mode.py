import io
import os
import shutil
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from scripts.run_rank_recall import _child_env, _pump, partition_configs
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


def test_log_pump_survives_character_not_encodable_by_console():
    class Proc:
        stdout = io.StringIO("bad replacement: \ufffd\n")

    raw = io.BytesIO()
    console = io.TextIOWrapper(raw, encoding="gbk", errors="strict")
    _pump("rank", Proc(), console)
    console.flush()

    assert raw.getvalue().decode("gbk") == \
        "[rank] bad replacement: \\ufffd\n"


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
