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
