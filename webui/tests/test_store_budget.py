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


def test_pipeline_key_budget_requires_store():
    """No store source -> HARD error (pipeline sizes its write space purely
    from the store capacity; an unbounded run exhausts the store)."""
    try:
        bm._pipeline_key_budget({}, 4, 8 << 20, 1, "8MB")
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "store_total_size" in str(exc)
        assert "master_metrics" in str(exc)


def test_pipeline_key_budget_formula():
    # 20GB store, 4 writers, 8MB data: int(20GiB*0.9) // (8MiB*4) = 576
    assert bm._pipeline_key_budget({"store_total_size": "20GB"},
                                   4, 8 << 20, 1, "8MB") == 576
    # auto-discovered total works the same way
    assert bm._pipeline_key_budget({"_store_total_auto": 20 * 1024 ** 3},
                                   4, 8 << 20, 1, "8MB") == 576
    # 20GB / 32 writers / 8MB = 72 (the 16-node rank_smoke shape)
    assert bm._pipeline_key_budget({"store_total_size": "20GB"},
                                   32, 8 << 20, 1, "8MB") == 72


def test_pipeline_key_budget_too_small():
    try:
        bm._pipeline_key_budget({"store_total_size": "32MB"},
                                4, 8 << 20, 1, "8MB")
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
    # budget now uses the auto value
    assert bm._pipeline_key_budget(g, 4, 8 << 20, 1, "8MB") == 576
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


def test_discover_skip_is_explicit_warn(monkeypatch, capsys):
    """No metrics source configured -> loud WARN (a silent skip hid a real
    store-exhaustion incident)."""
    g = {}   # no master_addr / master_metrics_url / store_total_size
    assert bm.discover_store_capacity(None, g) is None
    out = capsys.readouterr().out
    assert "auto-discovery skipped" in out and "will FAIL" in out
