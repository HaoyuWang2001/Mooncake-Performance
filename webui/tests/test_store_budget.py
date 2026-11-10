"""store_total_size key-space budget tests (basic check + pipeline budget)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import benchmark as bm


def test_store_total_bytes_parsing():
    assert bm._store_total_bytes({}) is None
    assert bm._store_total_bytes({"store_total_size": ""}) is None
    assert bm._store_total_bytes({"store_total_size": 0}) is None
    assert bm._store_total_bytes({"store_total_size": "20GB"}) == 20 * 1024 ** 3
    assert bm._store_total_bytes({"store_total_size": 1073741824}) == 1024 ** 3


def test_basic_store_check():
    # unset store -> no-op
    bm._basic_store_check({}, {"data_size": ["8MB"], "key_count": 100000})
    # fits
    bm._basic_store_check({"store_total_size": "1GB"},
                          {"data_size": ["8MB"], "key_count": 100})
    # exceeds (100 x 8MB = 800MB >= 512MB)
    try:
        bm._basic_store_check({"store_total_size": "500MB"},
                              {"data_size": ["8MB"], "key_count": 100})
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "store_total_size" in str(exc) and "800" in str(exc)
    # one bad size in the sweep fails the whole config
    try:
        bm._basic_store_check({"store_total_size": "1GB"},
                              {"data_size": ["64KB", "8MB"], "key_count": 1000})
        raise AssertionError("should raise")
    except RuntimeError:
        pass


def test_pipeline_key_budget_unset_store():
    # infinite store: passthrough; unset key_count stays unbounded (0)
    assert bm._pipeline_key_budget({}, 4, 8 << 20, 0, 1, "8MB") == (0, False)
    assert bm._pipeline_key_budget({}, 4, 8 << 20, 500, 1, "8MB") == (500, False)


def test_pipeline_key_budget_explicit_strict():
    glb = {"store_total_size": "1GB"}
    # 4 writers x 200 keys x 1MB = 800MB < 1GB -> ok
    assert bm._pipeline_key_budget(glb, 4, 1 << 20, 200, 1, "1MB") == (200, False)
    # 4 x 300 x 1MB = 1200MB >= 1GB -> fail
    try:
        bm._pipeline_key_budget(glb, 4, 1 << 20, 300, 1, "1MB")
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "1200" in str(exc) or "1GB" not in str(exc)
    # strictness: exact fit (need == store) also fails ("must be greater")
    try:
        bm._pipeline_key_budget({"store_total_size": 1 << 30}, 4, 1 << 20, 256, 1, "1MB")
        raise AssertionError("exact fit must fail")
    except RuntimeError:
        pass


def test_pipeline_key_budget_auto():
    # 20GB store, 4 writers, 8MB data:
    # int(20GiB*0.9) // (8MiB*4) = 576
    kc, auto = bm._pipeline_key_budget({"store_total_size": "20GB"},
                                       4, 8 << 20, 0, 1, "8MB")
    assert auto is True and kc == 576
    # explicit key_count still wins over auto when it fits
    kc, auto = bm._pipeline_key_budget({"store_total_size": "20GB"},
                                       4, 8 << 20, 100, 1, "8MB")
    assert auto is False and kc == 100
    # auto floor below batch_size -> store too small
    try:
        bm._pipeline_key_budget({"store_total_size": "32MB"},
                                4, 8 << 20, 0, 1, "8MB")
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "too small" in str(exc)


def test_parse_prometheus_metrics():
    text = """# HELP segment_total_capacity_bytes total
# TYPE segment_total_capacity_bytes gauge
segment_total_capacity_bytes 2.147483648e+10
segment_allocated_bytes{pool="a"} 1073741824
master_total_capacity_bytes 21474836480
"""
    v = bm._parse_prometheus_metrics(text)
    assert v["segment_total_capacity_bytes"] == 21474836480.0
    assert v["segment_allocated_bytes"] == 1073741824.0   # labels stripped
    assert "HELP" not in v


def test_master_metrics_url():
    assert bm._master_metrics_url({}) == ""
    assert bm._master_metrics_url(
        {"master_addr": "10.0.0.7:50051"}) == "http://10.0.0.7:17702/metrics"
    assert bm._master_metrics_url(
        {"master_addr": "10.0.0.7:50051", "master_metrics_port": 19000}) == \
        "http://10.0.0.7:19000/metrics"
    assert bm._master_metrics_url(
        {"master_addr": "10.0.0.7:50051",
         "master_metrics_url": "http://m:17702/metrics"}) == \
        "http://m:17702/metrics"


def test_store_total_precedence():
    # explicit wins over auto
    g = {"store_total_size": "5GB", "_store_total_auto": 999 * 1024 ** 3}
    assert bm._store_total_bytes(g) == 5 * 1024 ** 3
    # auto used when unset
    g = {"_store_total_auto": 20 * 1024 ** 3}
    assert bm._store_total_bytes(g) == 20 * 1024 ** 3
    # nothing -> None (infinite)
    assert bm._store_total_bytes({}) is None


def test_discover_store_capacity_auto(monkeypatch):
    text = ("segment_total_capacity_bytes 21474836480\n"
            "segment_allocated_bytes 1073741824\n")
    monkeypatch.setattr(bm, "_http_get_text", lambda url, timeout=5: text)
    g = {"master_addr": "10.0.0.7:50051"}
    total = bm.discover_store_capacity(None, g)
    assert total == 21474836480
    assert g["_store_total_auto"] == 21474836480
    # budget now uses the auto value and reports the auto formula
    kc, auto = bm._pipeline_key_budget(g, 4, 8 << 20, 0, 1, "8MB")
    assert auto is True and kc == 576
    # explicit config still wins after discovery
    g["store_total_size"] = "1GB"
    assert bm._store_total_bytes(g) == 1024 ** 3


def test_discover_store_capacity_failure_is_warn_only(monkeypatch, capsys):
    monkeypatch.setattr(bm, "_http_get_text", lambda url, timeout=5: None)
    g = {"master_addr": "10.0.0.7:50051"}
    assert bm.discover_store_capacity(None, g) is None
    assert "_store_total_auto" not in g
    assert "auto-discovery failed" in capsys.readouterr().out
    # unset -> infinite behaviour preserved
    assert bm._store_total_bytes(g) is None
