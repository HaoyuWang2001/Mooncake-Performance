"""bench_etcd (etcd gRPC gateway KV client) unit tests against mock/etcd_server."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                                "scripts"))

import bench_etcd as et
import importlib.util

_SPEC = importlib.util.spec_from_file_location(
    "mock_etcd_server",
    os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                 "mock", "etcd_server.py"))
_ME = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_ME)

import pytest


@pytest.fixture(scope="module")
def endpoints():
    srv = _ME.serve(0)
    import threading
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    eps = "etcd://127.0.0.1:{}".format(srv.server_address[1])
    yield eps
    srv.shutdown()


def test_endpoint_urls_parsing():
    assert et.endpoint_urls("etcd://a:2379;b:2379") == \
        ["http://a:2379", "http://b:2379"]
    assert et.endpoint_urls("a:2379") == ["http://a:2379"]
    assert et.endpoint_urls("http://a:2379/,http://b") == \
        ["http://a:2379", "http://b"]
    assert et.endpoint_urls("") == []
    assert et.endpoint_urls(None) == []


def test_prefix_end():
    # prefixes are ASCII in practice; the all-0xFF branch is unreachable
    assert et._prefix_end("mcbench/run1/") == "mcbench/run10"
    assert et._prefix_end("mcbench/r") == "mcbench/s"
    assert et._prefix_end("") == "\x00"


def test_put_range_roundtrip(endpoints):
    et.kv_put(endpoints, "mcbench/r1/readers/1", "a")
    et.kv_put(endpoints, "mcbench/r1/readers/2", "b")
    et.kv_put(endpoints, "mcbench/r1/writers/1", "c")
    et.kv_put(endpoints, "mcbench/r2/readers/1", "d")
    got = et.kv_range(endpoints, "mcbench/r1/")
    assert got == {"mcbench/r1/readers/1": "a",
                   "mcbench/r1/readers/2": "b",
                   "mcbench/r1/writers/1": "c"}
    assert et.kv_range(endpoints, "mcbench/r1/readers/") == \
        {"mcbench/r1/readers/1": "a", "mcbench/r1/readers/2": "b"}


def test_delete_prefix(endpoints):
    n = et.kv_delete_prefix(endpoints, "mcbench/r1/")
    assert n == 3
    assert et.kv_range(endpoints, "mcbench/r1/") == {}
    assert et.kv_range(endpoints, "mcbench/r2/") == \
        {"mcbench/r2/readers/1": "d"}
    assert et.kv_delete_prefix(endpoints, "mcbench/r1/") == 0


def test_endpoint_failover():
    # first endpoint dead (port 1), second = live mock
    srv = _ME.serve(0)
    import threading
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    eps = "etcd://127.0.0.1:1;127.0.0.1:{}".format(srv.server_address[1])
    assert et.kv_put(eps, "mcbench/fo/x", "1")
    assert et.kv_range(eps, "mcbench/fo/") == {"mcbench/fo/x": "1"}
    srv.shutdown()


def test_empty_endpoints_raises():
    try:
        et.kv_put("", "k", "v")
        raise AssertionError("should raise")
    except RuntimeError:
        pass
    try:
        et.kv_range("", "k")
        raise AssertionError("should raise")
    except RuntimeError:
        pass
    try:
        et.kv_delete_prefix("", "k")
        raise AssertionError("should raise")
    except RuntimeError:
        pass
