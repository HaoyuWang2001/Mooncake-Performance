import json
import math

import pytest
import benchmark as bm
from scripts.kvtest_digest import compress, merge_operations, quantile, THRESHOLDS


def operation(value, count):
    return {"total": count, "success": count, "failure": 0,
            "bytes": count * 100, "avg_ms": value, "busy_ms": value * count,
            "latency_sum_ms": value * count, "min_ms": value, "max_ms": value,
            "digest": {"unit": "ms", "centroids": [[value, count]]},
            **{f">{us}us": count if value * 1000 > us else 0 for us in THRESHOLDS}}


def test_merge_uses_centroid_weights_and_exact_moments():
    result = merge_operations([operation(1, 10000), operation(100, 1000)])
    assert result["p99_ms"] == result["p9999_ms"] == 100
    assert result["avg_ms"] == 110000 / 11000
    assert result["min_ms"] == 1 and result["max_ms"] == 100
    assert result[">2000us"] == result[">20000us"] == 1000
    assert sum(w for _, w in result["digest"]["centroids"]) == 11000


def test_compression_is_bounded_and_preserves_tail():
    centroids = []
    for block in range(30):
        centroids = compress(centroids + [(float(i + block * 8000), 1) for i in range(8000)])
    assert len(centroids) < 2000
    assert sum(w for _, w in centroids) == 240000
    assert abs(quantile(centroids, .9999) - 239976) < 20


def test_invalid_or_mixed_digest_is_rejected():
    bad = operation(1, 10)
    bad["success"] = 11
    with pytest.raises(ValueError, match="weight"):
        merge_operations([bad])
    with pytest.raises(ValueError, match="legacy"):
        merge_operations([operation(1, 10), {"success": 5}])


def test_precollected_report_window(tmp_path):
    path = tmp_path / "case.summary.json"
    selected = {"start_s": 120.0, "end_s": 720.0,
                "elapsed_seconds": 600, "operations": {"get_buffer": operation(3, 5)}}
    path.write_text(json.dumps({"statistics_format": "tdigest-v1",
                                "report_window": selected}), encoding="utf-8")
    cfg = {"duration": 780, "report": {"start_s": 120, "end_s": 720}}
    assert bm._kvtest_window_summary(str(path), cfg) == selected
    cfg["report"]["start_s"] = 121
    with pytest.raises(ValueError, match="not collected"):
        bm._kvtest_window_summary(str(path), cfg)


@pytest.mark.parametrize("work_mode", ["rank", "recall"])
def test_pipeline_csv_merges_pods_and_separates_writer_reader(tmp_path, work_mode):
    cfg = {"scenario": "kvtest", "work_mode": work_mode, "protocol": "ub",
           "duration": 30, "data_size": ["8MB"], "notify_count": 1,
           "client": {"count_per_node": 2},
           "writer": {"operation": "upsert_from"},
           "reader": {"operation": "get_buffer"}}
    for i, (v, n) in enumerate([(1, 10000), (100, 1000)]):
        st = {"statistics_format": "tdigest-v1", "elapsed_seconds": 30,
              "operations": {"upsert_from": operation(v, n),
                             "get_buffer": operation(7, n)}}
        (tmp_path / f"demo.node{i}_i{i}_p9101.kvtest_8MB.summary.json").write_text(json.dumps(st))
    rows = bm.write_scenario_csv(str(tmp_path), cfg, "run",
                                 str(tmp_path / "demo.json"), result_dir=str(tmp_path))
    writer = next(r for r in rows if r["instance"] == "TOTAL" and r["role"] == "writer")
    reader = next(r for r in rows if r["instance"] == "TOTAL" and r["role"] == "reader")
    assert writer["p99_ms"] == 100
    assert reader["p99_ms"] == 7
    assert writer[">20000us"] == 1000 and reader[">20000us"] == 0


def test_single_interface_csv_merges_digest(tmp_path):
    cfg = {"scenario": "kvtest_basic", "operation": "get", "protocol": "ub",
           "duration": 30, "data_size": ["8MB"], "client_node": "nodeA",
           "params": [{"num_clients": 2, "num_threads": 1}]}
    for i, (v, n) in enumerate([(1, 10000), (100, 1000)]):
        base = bm.kvtest_basic_base("single", "nodeA", i + 1, 9401 + i, "8MB")
        (tmp_path / (base + ".summary.json")).write_text(json.dumps({
            "statistics_format": "tdigest-v1", "elapsed_seconds": 30,
            "operations": {"get": operation(v, n)}}))
    rows = bm.write_kvtest_basic_csv(str(tmp_path), cfg, "run",
                                    str(tmp_path / "single.json"), result_dir=str(tmp_path))
    assert len(rows) == 1
    assert rows[0]["p99_ms"] == 100
    assert rows[0]["p9999_ms"] == 100
    assert rows[0][">20000us"] == 1000
