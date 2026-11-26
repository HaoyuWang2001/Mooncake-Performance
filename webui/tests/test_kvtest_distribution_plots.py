import csv
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import benchmark as bm
from scripts.kvtest_distribution_plots import prepare, render, distribution_curve, exact_stats


def stats(values):
    s = exact_stats(values)
    return {**s, "total": len(values), "failure": 0, "latency_sum_ms": sum(values),
            "digest": {"unit": "ms", "centroids": [[v, 1] for v in sorted(values)]}}


def artifact(folder, iid, values, raw=False, role="writer", mode="pipeline"):
    base = f"demo.node_i{iid}_p9101.kvtest_8MB"
    doc = {"statistics_format": "tdigest-v1", "mode": mode, "role": role,
           "instance_id": iid, "record_raw_samples": raw,
           "operations": {"get_buffer": stats(values)}}
    (folder / (base + ".summary.json")).write_text(json.dumps(doc), encoding="utf-8")
    if raw:
        with (folder / (base + ".samples.csv")).open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["elapsed_us", "instance_id", "operation", "request_index", "latency_ms", "status", "bytes"])
            for i, v in enumerate(values):
                writer.writerow([i * 1000000, iid, "get_buffer", i, v, "OK", 10])
    return doc


def test_raw_statistics_are_additional_and_do_not_replace_digest(tmp_path):
    artifact(tmp_path, 1, [1, 2], raw=True)
    artifact(tmp_path, 2, [100], raw=True)
    data = prepare(tmp_path, {"work_mode": "rank"})
    group = data["groups"][0]
    assert group["total"]["avg_ms"] == 103 / 3
    raw = json.loads((tmp_path / "kvtest_raw_statistics.json").read_text())
    assert raw["groups"][0]["total"]["p99_ms"] == 100
    assert raw["groups"][0]["total"]["max_ms"] == 100
    assert raw["groups"][0]["total"][">20000us"] == 1
    assert raw["groups"][0]["complete"]
    assert (tmp_path / "kvtest_distributions.json").exists()


def test_digest_only_and_roles_remain_separate(tmp_path):
    artifact(tmp_path, 1, [1, 2], role="writer")
    artifact(tmp_path, 2, [100], role="reader")
    data = prepare(tmp_path, {"work_mode": "recall"})
    assert {g["role"] for g in data["groups"]} == {"reader", "writer"}
    assert not (tmp_path / "kvtest_raw_statistics.json").exists()


def test_window_applies_to_raw_and_digest(tmp_path):
    doc = artifact(tmp_path, 1, [100, 3, 5, 100], raw=True)
    doc["report_window"] = {"start_s": 1, "end_s": 3,
                             "operations": {"get_buffer": stats([3, 5])}}
    path = next(tmp_path.glob("*.summary.json"))
    path.write_text(json.dumps(doc))
    data = prepare(tmp_path, {"report": {"start_s": 1, "end_s": 3}})
    raw = json.loads((tmp_path / "kvtest_raw_statistics.json").read_text())
    assert data["groups"][0]["total"]["avg_ms"] == 4
    assert raw["groups"][0]["total"]["avg_ms"] == 4


def test_raw_switch_validation_and_passthrough():
    assert not bm._kvtest_record_raw_samples({})
    assert bm._kvtest_record_raw_samples({"kvtest": {"record_raw_samples": True}})
    with pytest.raises(ValueError):
        bm._kvtest_record_raw_samples({"kvtest": {"record_raw_samples": "false"}})


@pytest.mark.parametrize("enabled", [False, True])
def test_config_builders_forward_switch(enabled):
    cfg = {"protocol": "ub", "duration": 10, "notify_count": 1,
           "client": {"count_per_node": 1, "process_mode": "split"},
           "kvtest": {"record_raw_samples": enabled}}
    entry = {"iid": 1, "port": 9101, "_out_dir": "/out"}
    plan = bm.kvtest_plan(cfg, 2)
    pipeline = bm.build_kvtest_instance_config(
        cfg, {"master_type": "etcd"}, {"SETUP_ETCD_ENDPOINTS": "etcd://master:2379"},
        plan, entry, "writer", "8MB", 256, [])
    basic = bm.build_kvtest_basic_instance_config(
        cfg, {}, "etcd://master:2379", "master:50051", entry, "8MB", "get", 1, 1, 0, 100)
    assert pipeline["record_raw_samples"] is enabled
    assert basic["record_raw_samples"] is enabled


def test_auto_plot_route_generates_digest_and_raw_figures(tmp_path):
    pytest.importorskip("matplotlib")
    import os
    import subprocess
    import sys
    artifact(tmp_path, 1, [1, 2, 10], raw=True)
    prepare(tmp_path)
    env = dict(os.environ, OPENBLAS_NUM_THREADS="1", OMP_NUM_THREADS="1")
    script = Path(bm.ROOT) / "scripts" / "plot_latency.py"
    proc = subprocess.run([sys.executable, str(script), "--log", str(tmp_path)],
                          capture_output=True, text=True, env=env, timeout=60)
    assert proc.returncode == 0, proc.stderr
    assert len(list((tmp_path / "digest_plots").glob("*.png"))) == 3
    assert list(tmp_path.glob("*.latency.png"))
    stats_file = next(tmp_path.glob("*.raw_stats.json"))
    assert json.loads(stats_file.read_text())["operations"]["get_buffer"]["max_ms"] == 10


def test_basic_plot_group_is_interface(tmp_path):
    artifact(tmp_path, 1, [1], mode="basic", role="client")
    assert prepare(tmp_path)["groups"][0]["role"] == "interface"


def test_render_all_three_digest_figures_and_curve_monotonicity(tmp_path):
    pytest.importorskip("matplotlib")
    artifact(tmp_path, 1, [1, 2, 10])
    artifact(tmp_path, 2, [2, 3, 100])
    data = prepare(tmp_path, {"work_mode": "rank"})
    xs, qs = distribution_curve(data["groups"][0]["total"])
    assert xs == sorted(xs) and qs == sorted(qs)
    assert qs[-1] > .99999
    outputs = render(tmp_path, data)
    assert {Path(p).suffix for p in outputs} == {".png"}
    assert len(outputs) == 3
    assert all(Path(p).stat().st_size > 1000 for p in outputs)
