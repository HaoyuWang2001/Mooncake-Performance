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
    # probed (B) alone works the same way
    assert bm._pipeline_key_budget({"_store_total_auto": 20 * 1024 ** 3},
                                   4, 8 << 20, 1, "8MB") == 576
    # 20GB / 32 writers / 8MB = 72 (the 16-node rank_smoke shape)
    assert bm._pipeline_key_budget({"store_total_size": "20GB"},
                                   32, 8 << 20, 1, "8MB") == 72
    # min rule: A=20GB + B=100GB -> cap from 20GB
    assert bm._pipeline_key_budget(
        {"store_total_size": "20GB", "_store_total_auto": 100 * 1024 ** 3},
        4, 8 << 20, 1, "8MB") == 576
    # min rule: A=100GB + B=20GB -> cap from B (20GB)
    assert bm._pipeline_key_budget(
        {"store_total_size": "100GB", "_store_total_auto": 20 * 1024 ** 3},
        4, 8 << 20, 1, "8MB") == 576
    # min rule: A=10GB + B=20GB -> cap from 10GB (part of the cluster)
    assert bm._pipeline_key_budget(
        {"store_total_size": "10GB", "_store_total_auto": 20 * 1024 ** 3},
        4, 8 << 20, 1, "8MB") == 288


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


def test_store_capacity_resolution_min_rule():
    """A=configured, B=probed: effective = min(A, B); single source stands;
    neither -> None. Source labels drive the startup log line."""
    # A only
    a, b, eff, src = bm._store_capacity_resolution({"store_total_size": "5GB"})
    assert (a, b, eff, src) == (5 * 1024 ** 3, None, 5 * 1024 ** 3,
                                "configured")
    # B only
    a, b, eff, src = bm._store_capacity_resolution(
        {"_store_total_auto": 20 * 1024 ** 3})
    assert (a, b, eff, src) == (None, 20 * 1024 ** 3, 20 * 1024 ** 3, "probed")
    # both: A < B -> A   ("use part of the cluster")
    a, b, eff, src = bm._store_capacity_resolution(
        {"store_total_size": "5GB", "_store_total_auto": 20 * 1024 ** 3})
    assert eff == 5 * 1024 ** 3 and src == "min(configured,probed)"
    # both: A > B -> B   ("use the whole cluster capacity, capped by reality")
    a, b, eff, src = bm._store_capacity_resolution(
        {"store_total_size": "50GB", "_store_total_auto": 20 * 1024 ** 3})
    assert eff == 20 * 1024 ** 3 and src == "min(configured,probed)"
    # neither
    assert bm._store_capacity_resolution({}) == (None, None, None, "none")
    assert bm._store_total_bytes({}) is None


def test_discover_store_capacity_auto(monkeypatch):
    text = ("segment_total_capacity_bytes 21474836480\n"
            "segment_allocated_bytes 1073741824\n")
    monkeypatch.setattr(bm, "_http_get_text", lambda url, timeout=5: text)
    g = {"master_addr": "10.0.0.7:50051"}
    total = bm.discover_store_capacity(None, g)
    assert total == 21474836480
    assert g["_store_total_auto"] == 21474836480
    # budget now uses the probed value
    assert bm._pipeline_key_budget(g, 4, 8 << 20, 1, "8MB") == 576
    # small configured budget caps the probed capacity (min rule)
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
