#!/usr/bin/env python3
"""upsert_from benchmark registration and prefill semantics."""
import ctypes
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPTS = os.path.join(ROOT, "scripts")
sys.path.insert(0, ROOT)
sys.path.insert(0, SCRIPTS)

import mooncake_benchmark_client as client


class _Store:
    def __init__(self):
        self.buffers = {}
        self.values = {}
        self.upsert_calls = 0

    def register_buffer(self, ptr, size):
        self.buffers[ptr] = size
        return 0

    def unregister_buffer(self, ptr):
        self.buffers.pop(ptr, None)
        return 0

    def put_from(self, key, ptr, size):
        if ptr not in self.buffers:
            return -1
        self.values[key] = ctypes.string_at(ptr, size)
        return 0

    def upsert_from(self, key, ptr, size):
        if key not in self.values or ptr not in self.buffers:
            return -1
        self.upsert_calls += 1
        self.values[key] = ctypes.string_at(ptr, size)
        return 0


def test_upsert_from_prefills_then_overwrites():
    store = _Store()
    ctx = client._BufferCtx(4096)
    assert ctx.setup(store)
    try:
        value = bytes(range(64)) * 4
        keys = ["key-0", "key-1"]
        client._prefill_upsert_keys(store, keys, value, ctx, "test")
        assert store.values["key-0"] != value

        latency, status = client._run_iface(
            store, *client.PUT_IFACE_DEFS["upsert_from"],
            ["key-0"], value, ctx)
        assert latency >= 0
        assert status == "OK"
        assert store.upsert_calls == 1
        assert store.values["key-0"] == value
    finally:
        ctx.teardown(store)


def test_upsert_prefill_key_selection():
    keys = ["k0", "k1", "k2", "k3"]
    assert client._upsert_prefill_keys("upsert_from", keys, 0.5) == keys
    assert client._upsert_prefill_keys(
        "upsert_from/get", keys, 0.5) == keys[:2]
    assert client._upsert_prefill_keys("put_from", keys, 0.5) == []


def test_mixed_upsert_prefill_covers_per_thread_quotas():
    # measured mixed phase splits the chunk across threads; each thread's
    # put quota is the head of ITS OWN sub-range, so the prefill union must
    # cover those per-thread heads (not just the whole-chunk head)
    chunk = ["k{}".format(i) for i in range(10)]
    assert client._split_even(chunk, 3) == [
        ["k0", "k1", "k2"], ["k3", "k4", "k5"],
        ["k6", "k7", "k8", "k9"]]
    union = client._mixed_upsert_prefill_keys("upsert_from/get", chunk, 3, 0.5)
    assert union == ["k0", "k3", "k6", "k7"]
    # whole-chunk head (warmup view) would leave k6/k7 un-prefilled
    head = client._upsert_prefill_keys("upsert_from/get", chunk, 0.5)
    assert {"k6", "k7"} - set(head)
    # non-upsert mixed put ifaces need no prefill
    assert client._mixed_upsert_prefill_keys("put/get", chunk, 3, 0.5) == []


def test_orchestrator_registry_parity():
    import benchmark as bm
    # pipeline writer validation whitelist accepts upsert_from
    assert "upsert_from" in bm.PIPELINE_PUT_IFACES
    # every orchestrator-approved put iface resolves to a client handler
    for iface in bm.PIPELINE_PUT_IFACES:
        assert iface in client.PUT_IFACE_DEFS, iface
    # upsert_from is zero-copy: needs the registered-buffer path
    assert "upsert_from" in client.ZERO_COPY_PUT_IFACES
    # basic iface tuples stay in sync with the dispatch dict
    assert set(client.PUT_IFACES) == set(client.PUT_IFACE_DEFS)
