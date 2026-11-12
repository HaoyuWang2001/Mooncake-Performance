#!/usr/bin/env python3
"""Remote test process for the Mooncake performance benchmark.
Runs on the server node that hosts the test client (single node, multi client).
Usage:
  python3 mooncake_benchmark_client.py --mode generate-data|remove-keys|prepare-store|benchmark|pipeline   
      [--scope key-prefix|full] [--num-clients N --client-id ID --num-threads T]   
      [--warmup true|false] [other params...]
Output (stdout, captured by run_benchmark_*.sh into per-client log files):
  META\t                       run metadata
  RESULT\t\t\t\t   one line per measured op
  SUMMARY\t                    per-client summary
"""
import argparse
import copy
import gc
import json
import os
import random
import re
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

# bench_etcd lives next to this file on every node (pushed together with
# the runner scripts); pipeline rendezvous uses it over the etcd gateway.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench_etcd
VALIDATE_BYTES = 64
def parse_size(size_str):
    """Parse 8MB / 1GB / 512KB style strings into byte counts."""
    if isinstance(size_str, (int, float)):
        return int(size_str)
    s = str(size_str).strip().upper()
    units = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4}
    for unit in sorted(units, key=len, reverse=True):
        if s.endswith(unit):
            return int(float(s[: -len(unit)].strip()) * units[unit])
    return int(float(s))
def build_keys(prefix, scenario, count):
    return ["{}*{}*{:08d}".format(prefix, scenario, i) for i in range(count)]
def make_value(data_size, seed):
    rng = random.Random(seed)
    return rng.randbytes(data_size)
def scenario_data_dir(remote_data_dir, scenario):
    return os.path.join(remote_data_dir, scenario)
def value_path(remote_data_dir, scenario):
    return os.path.join(scenario_data_dir(remote_data_dir, scenario), "value.bin")
def load_value(remote_data_dir, scenario):
    path = value_path(remote_data_dir, scenario)
    if not os.path.exists(path):
        raise SystemExit("[ERROR] data not generated yet, run --mode generate-data first")
    with open(path, "rb") as f:
        return f.read()
def make_store(args):
    from mooncake.store import MooncakeDistributedStore
    local_hostname = args.fabric_ip
    master_type = args.master_type or ""
    if master_type not in ("http", "etcd"):
        print("[ERROR] master_type not specified: pass --master-type http|etcd "
              "or set SETUP_MASTER_TYPE")
        sys.exit(1)
    if master_type == "etcd":
        # etcd mode: the same etcd URI serves as both metadata_server and
        # master_server_addr; master_addr/metadata_server (if set) are dormant
        if not args.etcd_endpoints:
            print("[ERROR] master_type=etcd requires --etcd-endpoints "
                  "(e.g. etcd://ip:2379;ip:2379)")
            sys.exit(1)
        metadata_server = args.etcd_endpoints
        master_addr = args.etcd_endpoints
    else:
        if not args.master_addr:
            print("[ERROR] master_type=http requires --master-addr")
            sys.exit(1)
        metadata_server = args.metadata_server or "http://{}/metadata".format(args.master_addr)
        master_addr = args.master_addr
    mode = master_type
    log("connecting to mooncake master (mode={}, metadata_server={})".format(mode, metadata_server))
    store = MooncakeDistributedStore()
    ret = store.setup(
        local_hostname=local_hostname,
        metadata_server=metadata_server,
        global_segment_size=args.global_segment_size,
        local_buffer_size=args.local_buffer_size,
        protocol=args.protocol,
        rdma_devices=args.ub_device,
        master_server_addr=master_addr,
        tenant_id=args.tenant_id,
    )
    if ret != 0:
        print("[ERROR] store.setup failed, ret={}".format(ret))
        sys.exit(1)
    log("store.setup done (ret=0)")
    return store
def emit(kind, payload):
    if isinstance(payload, dict):
        payload = json.dumps(payload)
    sys.stdout.write("{}\t{}\n".format(kind, payload))
    sys.stdout.flush()

def log(msg):
    """Timestamped free-text progress line (ignored by the orchestrator parser
    which only reads RESULT/META/SUMMARY prefixes). Flushed immediately so the
    output file is live while the process runs or hangs."""
    sys.stdout.write("[{}] {}\n".format(time.strftime("%H:%M:%S"), msg))
    sys.stdout.flush()

def unbuffer_stdio():
    """Make stdout/stderr line-buffered even when redirected to a file, so
    progress lines appear in the log during execution (not only at exit)."""
    try:
        sys.stdout.reconfigure(line_buffering=True)
        sys.stderr.reconfigure(line_buffering=True)
    except Exception:
        pass

# Environment snapshot printed once at process start (per client), before
# argparse / SDK import. <unset> keeps missing values explicit.
_ENV_LOG_KEYS = ("POD_IP", "NODE_NAME", "LD_LIBRARY_PATH", "SETUP_FABRIC_IP",
                 "MC_FABRIC_IP", "MC_TCP_BIND_ADDRESS", "SETUP_UB_DEVICE",
                 "MC_URMA_TRANS_MODE", "SETUP_PROTOCOL")


def env_snapshot():
    return " ".join("{}={}".format(k, os.environ.get(k, "<unset>"))
                    for k in _ENV_LOG_KEYS)


# --- NUMA / CPU affinity binding ------------------------------------------

def parse_cpu_list(spec):
    """Parse a CPU list spec ('0-83,90' or '0-19') into a set of cpu ids.
    Raises ValueError on malformed input."""
    cpus = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            lo, hi = int(lo), int(hi)
            if lo < 0 or hi < lo:
                raise ValueError("invalid cpu range: {}".format(part))
            cpus.update(range(lo, hi + 1))
        else:
            cpus.add(int(part))
    if not cpus:
        raise ValueError("empty cpu list: {!r}".format(spec))
    return cpus


def parse_lscpu_numa(text, node):
    """Extract the CPU list of a NUMA node from lscpu output ('NUMA nodeN
    CPU(s): 0-83' lines). Returns None if the node is absent."""
    for line in text.splitlines():
        m = re.match(r"NUMA\s+node(\d+)\s+CPU\(s\):\s*(.+)", line.strip())
        if m and int(m.group(1)) == node:
            return m.group(2).strip()
    return None


def numa_node_cpu_list(node):
    """CPU list string for NUMA node N: /sys/devices/system/node/nodeN/cpulist
    first, lscpu parsing as fallback. Returns None when unavailable."""
    sysfs = "/sys/devices/system/node/node{}/cpulist".format(node)
    if os.path.exists(sysfs):
        with open(sysfs) as f:
            return f.read().strip()
    try:
        out = subprocess.check_output(["lscpu"], text=True,
                                      stderr=subprocess.DEVNULL)
    except Exception:
        return None
    return parse_lscpu_numa(out, node)


def apply_cpu_binding(args):
    """Bind this process via os.sched_setaffinity. Three modes:

    1. --cpu-bind CPULIST      — explicit cpu list (highest precedence)
    2. --numa-node N + --cpu-shard S [--shard-index J]
                                — NUMA-aware sharding: take the J-th
                                  consecutive S-core slice of node N's
                                  allowed pool, wrapping around when the
                                  pool cannot give every client a disjoint
                                  slice (clients then reuse earlier slices)
    3. --numa-node N           — all allowed cores of node N

    The exclude set (BENCH_EXCLUDE_CPUS from cluster.json, per-node) is
    subtracted from EVERY mode — reserved cores (for the system / other
    users) are never bound. An empty result is a hard error.
    Binding is applied before any store work so all threads inherit it."""
    exclude = set()
    excl_spec = os.environ.get("BENCH_EXCLUDE_CPUS", "")
    if excl_spec:
        try:
            exclude = parse_cpu_list(excl_spec)
        except ValueError as exc:
            sys.exit("[ERROR] bad BENCH_EXCLUDE_CPUS {!r}: {}".format(excl_spec, exc))
    shard = int(getattr(args, "cpu_shard", 0) or 0)
    shard_index = int(getattr(args, "shard_index", 0) or 0)

    if args.cpu_bind:
        try:
            cpus = parse_cpu_list(args.cpu_bind)
        except ValueError as exc:
            sys.exit("[ERROR] invalid --cpu-bind {!r}: {}".format(args.cpu_bind, exc))
        cpus -= exclude
        source = "cpu_bind={}".format(args.cpu_bind)
    elif args.numa_node is not None and args.numa_node >= 0:
        spec = numa_node_cpu_list(args.numa_node)
        if spec is None:
            sys.exit("[ERROR] --numa-node {}: NUMA topology not available on this node".format(
                args.numa_node))
        try:
            pool = sorted(parse_cpu_list(spec) - exclude)
        except ValueError as exc:
            sys.exit("[ERROR] bad cpu list for NUMA node {} ({}): {}".format(
                args.numa_node, spec, exc))
        if not pool:
            sys.exit("[ERROR] NUMA node {} has no cores left after excluding {!r}".format(
                args.numa_node, excl_spec))
        if shard > 0:
            if shard > len(pool):
                sys.exit("[ERROR] --cpu-shard {} > available cores {} on node {}".format(
                    shard, len(pool), args.numa_node))
            # slice J wraps around the pool when it cannot hold all clients
            start = (shard_index * shard) % len(pool)
            end = start + shard
            if end <= len(pool):
                cpus = set(pool[start:end])
            else:
                cpus = set(pool[start:] + pool[:end - len(pool)])
            source = "numa_node={} cpu_shard={} shard_index={}".format(
                args.numa_node, shard, shard_index)
        else:
            cpus = set(pool)
            source = "numa_node={}".format(args.numa_node)
    else:
        return
    if not cpus:
        sys.exit("[ERROR] cpu binding result is empty (exclude set {!r} removed everything)".format(
            excl_spec))
    try:
        os.sched_setaffinity(0, sorted(cpus))
    except OSError as exc:
        sys.exit("[ERROR] sched_setaffinity failed ({}): {}".format(source, exc))
    bound = sorted(os.sched_getaffinity(0))
    log("cpu binding: {} bound_cpus={} ({} cpus)".format(source, _fmt_cpu_list(bound), len(bound)))
    emit("META", {"mode": "cpu_binding", "source": source,
                  "numa_node": args.numa_node, "shard": shard,
                  "shard_index": shard_index if shard else None,
                  "excluded": _fmt_cpu_list(sorted(exclude)) if exclude else "",
                  "bound_cpus": _fmt_cpu_list(bound), "count": len(bound)})


def _fmt_cpu_list(cpus):
    """Render a sorted cpu set compactly (e.g. {0..9, 12} -> '0-9,12')."""
    cpus = sorted(cpus)
    if not cpus:
        return ""
    parts = []
    start = prev = cpus[0]
    for c in cpus[1:]:
        if c == prev + 1:
            prev = c
            continue
        parts.append("{}".format(start) if start == prev else "{}-{}".format(start, prev))
        start = prev = c
    parts.append("{}".format(start) if start == prev else "{}-{}".format(start, prev))
    return ",".join(parts)

# --------------------------------------------------------------------------

# modes

# --------------------------------------------------------------------------

def cmd_generate_data(args):
    log("generate-data start (scenario={}, data_size={}, key_count={})".format(
        args.scenario, args.data_size, args.key_count))
    data_dir = scenario_data_dir(args.remote_data_dir, args.scenario)
    os.makedirs(data_dir, exist_ok=True)
    vpath = value_path(args.remote_data_dir, args.scenario)
    kpath = os.path.join(data_dir, "keys.txt")
    data_size = parse_size(args.data_size)
    value = make_value(data_size, args.data_seed)
    with open(vpath, "wb") as f:
        f.write(value)
    keys = build_keys(args.key_prefix, args.scenario, args.key_count)
    with open(kpath, "w", encoding="utf-8") as f:
        for k in keys:
            f.write(k + "\n")
    log("generate-data done (value={} bytes, keys={})".format(data_size, len(keys)))
    emit("META", {"mode": "generate-data", "scenario": args.scenario,
                  "key_count": len(keys), "data_size_bytes": data_size,
                  "value_file": vpath, "keys_file": kpath, "seed": args.data_seed})
def cmd_remove_keys(args, store):
    log("remove-keys start (scope={}, scenario={})".format(args.scope, args.scenario))
    keys = build_keys(args.key_prefix, args.scenario, args.key_count)
    scope = args.scope
    if scope == "full":
        ret = store.remove_all(force=True)
        if ret < 0:
            print("[ERROR] remove-keys full failed: remove_all returned {}".format(ret))
            sys.exit(1)
        log("remove-keys done (scope=full, cleared={})".format(max(ret, 0)))
        emit("META", {"mode": "remove-keys", "scope": "full", "cleared": max(ret, 0)})
        return
    existence = store.batch_is_exist(keys)
    statuses = store.batch_remove(keys, force=True)
    if len(existence) != len(keys) or len(statuses) != len(keys):
        print("[ERROR] remove-keys failed: batch_is_exist/batch_remove returned unexpected result size")
        sys.exit(1)
    removed = 0
    not_found = 0
    failed = 0
    failed_samples = []
    for k, ex, st in zip(keys, existence, statuses):
        if ex == 0:
            not_found += 1
        elif ex == 1 and st == 0:
            removed += 1
        else:
            failed += 1
            if len(failed_samples) < 20:
                failed_samples.append("{} (is_exist={}, remove={})".format(k, ex, st))
    emit("META", {"mode": "remove-keys", "scope": "key-prefix",
                  "keys_attempted": len(keys), "keys_removed": removed,
                  "keys_not_found": not_found, "keys_failed": failed})
    log("remove-keys done (scope=key-prefix, removed={}, not_found={}, failed={})".format(
        removed, not_found, failed))
    if failed_samples:
        print("[ERROR] remove-keys failed for {} keys, samples:".format(failed))
        for s in failed_samples:
            print("  " + s)
    if failed > 0:
        sys.exit(1)
def cmd_prepare_store(args, store):
    log("prepare-store start (scenario={})".format(args.scenario))
    # Force release all segment space before preparing new data
    store.remove_all(force=True)
    value = load_value(args.remote_data_dir, args.scenario)
    keys = build_keys(args.key_prefix, args.scenario, args.key_count)
    existence = store.batch_is_exist(keys)
    if len(existence) != len(keys):
        print("[ERROR] prepare-store failed: batch_is_exist returned unexpected result size")
        sys.exit(1)
    missing = [k for k, ex in zip(keys, existence) if ex == 0]
    existing = len(keys) - len(missing)
    written = 0
    for key in missing:
        ret = store.put(key, value)
        if ret != 0:
            print("[ERROR] prepare-store failed: put key={} returned {}".format(
                key, ret))
            sys.exit(1)
        written += 1
        if written % 100 == 0 or written == len(missing):
            log("prepare-store progress ({}/{})".format(
                written, len(missing)))
    emit("META", {"mode": "prepare-store", "scenario": args.scenario,
                  "total": len(keys), "existing": existing, "written": written})
    log("prepare-store done (total={}, existing={}, written={})".format(
        len(keys), existing, written))

# --------------------------------------------------------------------------

# operations

# --------------------------------------------------------------------------

# Interface registry: basic + pipeline share these call semantics.
# A put/get interface name resolves to a handler; batch handlers operate on a
# whole key group in one call (bytes_per_op = batch_size * data_size).
PUT_IFACES = ("put", "put_batch", "put_from", "upsert_from")
GET_IFACES = ("get", "get_batch", "get_into", "get_buffer")
PUT_BATCH_IFACES = ("put_batch",)
GET_BATCH_IFACES = ("get_batch",)
ZERO_COPY_PUT_IFACES = ("put_from", "upsert_from")


class _BufferCtx:
    """Page-aligned buffer registered with the store for the zero-copy
    interfaces (put_from / get_into).

    Why mmap: URMA-based store.register_buffer() requires the registered
    region to be page-aligned (base AND length). A plain ctypes heap array is
    only 16-byte aligned (e.g. 0xfff9351fd010 ends in 0x010), and
    urma_register_seg rejects it — the real cluster failed every zero-copy
    scenario with urma_endpoint.cpp "Failed to register segment ...
    Success [0]" (the errno print is bogus, it is a driver return failure).
    mmap anonymous mapping guarantees a page-aligned base; the length is
    rounded up to a page multiple as well. mooncake's own internal
    allocation is aligned the same way (ub_allocator / staging pool 4096).
    """

    def __init__(self, size):
        import ctypes
        import mmap
        self.ctypes = ctypes
        page = mmap.PAGESIZE
        self.size = max(page, ((int(size) + page - 1) // page) * page)
        self._mm = mmap.mmap(-1, self.size)
        self.ptr = ctypes.addressof(ctypes.c_char.from_buffer(self._mm))
        self.registered = False

    def setup(self, store):
        try:
            self.registered = store.register_buffer(self.ptr, self.size) == 0
        except Exception:
            self.registered = False
        return self.registered

    def teardown(self, store):
        if self.registered:
            try:
                store.unregister_buffer(self.ptr)
            except Exception:
                pass
            self.registered = False
        try:
            self._mm.close()
        except Exception:
            pass

    def write_value(self, value):
        self.ctypes.memmove(self.ptr, value, len(value))

    def read_bytes(self, n):
        return self.ctypes.string_at(self.ptr, n)


class _NoCall(Exception):
    """Raised by make_call when an interface call cannot be produced at all
    (e.g. a required registered buffer is missing). No store invocation is
    attempted, so there is no timed region."""


def now_hms():
    """Local wall-clock timestamp for RESULT rows (HH:MM:SS.microseconds).

    Localization aid only — latency values come from the timed region; wall
    clock may skew across nodes and is never used for measurement.
    """
    t = time.time()
    return time.strftime("%H:%M:%S", time.localtime(t)) + ".{:06d}".format(
        int((t % 1) * 1e6))


def _run_iface(store, make_call, check, keys, value, ctx):
    """Run one interface operation with the timed region containing ONLY the
    store interface call.

    - make_call(store, keys, value, ctx): untimed argument preparation that
      returns a callable performing the single store invocation (e.g. building
      the batch values list, filling the zero-copy buffer).
    - check(raw, keys, value, ctx): untimed result validation / status
      derivation (OK / MISS / MISMATCH / ERR).

    Latency therefore measures the raw interface invocation duration only.
    """
    try:
        call = make_call(store, keys, value, ctx)
    except _NoCall as exc:
        return 0.0, "ERR:{}".format(exc)
    t0 = time.perf_counter()
    try:
        raw = call()
    except Exception as exc:
        lat_ms = (time.perf_counter() - t0) * 1000.0
        return lat_ms, "ERR:{}".format(type(exc).__name__)
    lat_ms = (time.perf_counter() - t0) * 1000.0
    return lat_ms, check(raw, keys, value, ctx)


# --- put family -----------------------------------------------------------

def _put_make_call(store, keys, value, ctx):
    key, val = keys[0], value
    return lambda: store.put(key, val)


def _put_batch_make_call(store, keys, value, ctx):
    values = [value] * len(keys)          # untimed argument preparation
    return lambda: store.put_batch(keys, values)


def _put_from_make_call(store, keys, value, ctx):
    if ctx is None:
        raise _NoCall("no-buffer")
    ctx.write_value(value)                # untimed buffer prep (full value)
    key, ptr, size = keys[0], ctx.ptr, len(value)
    return lambda: store.put_from(key, ptr, size)


def _upsert_from_make_call(store, keys, value, ctx):
    if ctx is None:
        raise _NoCall("no-buffer")
    ctx.write_value(value)                # untimed buffer prep (full value)
    key, ptr, size = keys[0], ctx.ptr, len(value)
    return lambda: store.upsert_from(key, ptr, size)


def _put_check(raw, keys, value, ctx):
    return "OK" if raw == 0 else "ERR:{}".format(raw)


def _split_even(chunk, parts):
    """Even split of a key chunk into `parts` contiguous sub-ranges — the
    exact per-thread ranges the measured basic phase hands to _run_thread."""
    n = max(1, parts)
    return [chunk[t * len(chunk) // n:(t + 1) * len(chunk) // n]
            for t in range(n)]


# --- get family -----------------------------------------------------------

def _get_make_call(store, keys, value, ctx):
    key = keys[0]
    return lambda: store.get(key)


def _get_check(raw, keys, value, ctx):
    if not raw:
        return "MISS"
    if len(raw) != len(value) or raw[:VALIDATE_BYTES] != value[:VALIDATE_BYTES]:
        return "MISMATCH"
    return "OK"


def _get_batch_make_call(store, keys, value, ctx):
    return lambda: store.get_batch(keys)


def _get_batch_check(raw, keys, value, ctx):
    if len(raw) != len(keys):
        return "ERR:batch-size"
    for d in raw:
        if not d:
            return "MISS"
        if len(d) != len(value) or d[:VALIDATE_BYTES] != value[:VALIDATE_BYTES]:
            return "MISMATCH"
    return "OK"


def _get_into_make_call(store, keys, value, ctx):
    if ctx is None:
        raise _NoCall("no-buffer")
    key, ptr, size = keys[0], ctx.ptr, ctx.size
    return lambda: store.get_into(key, ptr, size)


def _get_into_check(raw, keys, value, ctx):
    if raw < 0:
        return "ERR:{}".format(raw)
    if raw != len(value):
        return "MISMATCH"
    if ctx.read_bytes(VALIDATE_BYTES) != value[:VALIDATE_BYTES]:
        return "MISMATCH"
    return "OK"


def _get_buffer_make_call(store, keys, value, ctx):
    key = keys[0]
    return lambda: store.get_buffer(key)


def _get_buffer_check(raw, keys, value, ctx):
    import ctypes
    h = raw
    if h is None:
        return "MISS"
    try:
        size = h.size()
        ptr = h.ptr()
    except Exception as exc:
        return "ERR:{}".format(type(exc).__name__)
    if size != len(value):
        return "MISMATCH"
    if ctypes.string_at(ptr, VALIDATE_BYTES) != value[:VALIDATE_BYTES]:
        return "MISMATCH"
    return "OK"


PUT_IFACE_DEFS = {
    "put": (_put_make_call, _put_check),
    "put_batch": (_put_batch_make_call, _put_check),
    "put_from": (_put_from_make_call, _put_check),
    "upsert_from": (_upsert_from_make_call, _put_check),
}
GET_IFACE_DEFS = {
    "get": (_get_make_call, _get_check),
    "get_batch": (_get_batch_make_call, _get_batch_check),
    "get_into": (_get_into_make_call, _get_into_check),
    "get_buffer": (_get_buffer_make_call, _get_buffer_check),
}
def _run_thread(args, store, keys, value, results, deadline=None, ctx=None):
    op = args.operation
    batch_size = max(1, args.batch_size)
    put_ratio = args.put_ratio
    local = []
    def keys_iter():
        if deadline is None:
            for k in keys:
                yield k
        else:
            while time.time() < deadline:
                for k in keys:
                    if time.time() >= deadline:
                        return
                    yield k
    def groups_iter():
        if deadline is None:
            for j in range(0, len(keys), batch_size):
                yield keys[j:j + batch_size]
        else:
            while time.time() < deadline:
                for j in range(0, len(keys), batch_size):
                    if time.time() >= deadline:
                        return
                    yield keys[j:j + batch_size]
    # basic-mode pacing: if target_qps > 0, even-rate throttle across all
    # client processes x threads (the orchestrator passes the per-client
    # share; each thread then spaces ops by num_threads/share). Absolute
    # schedule (self-correcting: an op that overruns its slot is followed
    # immediately, so the long-run average stays exact). target_qps <= 0
    # means free-run (current behaviour: count-based max-speed hammering).
    per_client_qps = args.target_qps or 0.0
    pacer_interval = (args.num_threads / per_client_qps) if per_client_qps > 0 else 0.0
    pacer_next = time.time() + pacer_interval
    def pace():
        nonlocal pacer_next
        if pacer_interval > 0:
            pacer_next += pacer_interval
            time.sleep(max(0.0, pacer_next - time.time()))
    if "/" in op:
        # mixed: operation = "<put_iface>/<get_iface>"; put/get alternate,
        # starting with put; each get reads a key the put side has written
        put_iface, get_iface = op.split("/", 1)
        if put_iface not in PUT_IFACE_DEFS or get_iface not in GET_IFACE_DEFS:
            raise SystemExit("[ERROR] unsupported mixed operation {}: "
                             "put_iface={}, get_iface={}".format(op, put_iface, get_iface))
        put_count = int(len(keys) * put_ratio)
        put_keys = keys[:put_count]
        get_keys = keys[put_count:]
        n_put = len(put_keys)
        n_get = len(get_keys)
        n_put_units = (n_put + batch_size - 1) // batch_size if n_put else 0
        n_get_units = (n_get + batch_size - 1) // batch_size if n_get else 0
        n_units = max(n_put_units, n_get_units)

        def run_mixed_unit(i):
            # one put/get alternation unit (put group i, then its paired get)
            if i < n_put_units:
                group = put_keys[i * batch_size:(i + 1) * batch_size]
                lat, status = _run_iface(store, *PUT_IFACE_DEFS[put_iface], group, value, ctx)
                local.append(("put", group[0], lat, status, now_hms()))
                pace()
            if i < n_get_units:
                base = i * batch_size
                gkeys = [put_keys[(base + k) % n_put] for k in range(batch_size)] if n_put else \
                        get_keys[base:base + batch_size]
                gkeys = gkeys[:n_get - base]
                if gkeys:
                    lat, status = _run_iface(store, *GET_IFACE_DEFS[get_iface], gkeys, value, ctx)
                    local.append(("get", gkeys[0], lat, status, now_hms()))
                    pace()

        if deadline is None:
            for i in range(n_units):
                run_mixed_unit(i)
        else:
            # duration mode: put/get cycles. When the put side has written
            # all of its keys (full pass completed), it clears them and the
            # next cycle rewrites — the remove is maintenance and is NOT
            # timed (no RESULT line), only the put/get calls are measured.
            # Each thread removes only its own sub-chunk keys, so parallel
            # threads and client processes never interfere.
            cycles = 0
            while time.time() < deadline:
                cycles += 1
                for i in range(n_units):
                    if time.time() >= deadline:
                        break
                    run_mixed_unit(i)
                else:
                    if put_iface != "upsert_from":
                        store.batch_remove(put_keys, force=True)
                        log("mixed cycle {}: removed {} put keys (untimed maintenance)".format(
                            cycles, len(put_keys)))
    elif op in PUT_IFACE_DEFS:
        if op in PUT_BATCH_IFACES:
            if deadline is None:
                for group in groups_iter():
                    lat, status = _run_iface(store, *PUT_IFACE_DEFS[op], group, value, ctx)
                    local.append((op, group[0], lat, status, now_hms()))
                    pace()
            else:
                # duration mode: full-key passes; after each completed pass
                # the thread clears its own keys (untimed maintenance — no
                # RESULT line) and the next cycle rewrites them. Threads and
                # client processes own disjoint key ranges, so the remove
                # never interferes with concurrent writers.
                cycles = 0
                while time.time() < deadline:
                    cycles += 1
                    for j in range(0, len(keys), batch_size):
                        if time.time() >= deadline:
                            break
                        group = keys[j:j + batch_size]
                        lat, status = _run_iface(store, *PUT_IFACE_DEFS[op], group, value, ctx)
                        local.append((op, group[0], lat, status, now_hms()))
                        pace()
                    else:
                        store.batch_remove(keys, force=True)
                        log("put_batch cycle {}: removed {} keys (untimed maintenance)".format(
                            cycles, len(keys)))
        else:
            if deadline is None:
                for k in keys_iter():
                    lat, status = _run_iface(store, *PUT_IFACE_DEFS[op], [k], value, ctx)
                    local.append((op, k, lat, status, now_hms()))
                    pace()
            else:
                # duration mode: full-key passes with untimed self-clear
                # between cycles (see the put_batch branch above)
                cycles = 0
                while time.time() < deadline:
                    cycles += 1
                    for k in keys:
                        if time.time() >= deadline:
                            break
                        lat, status = _run_iface(store, *PUT_IFACE_DEFS[op], [k], value, ctx)
                        local.append((op, k, lat, status, now_hms()))
                        pace()
                    else:
                        if op != "upsert_from":
                            store.batch_remove(keys, force=True)
                            log("put cycle {}: removed {} keys (untimed maintenance)".format(
                                cycles, len(keys)))
    elif op in GET_IFACE_DEFS:
        if op in GET_BATCH_IFACES:
            for group in groups_iter():
                lat, status = _run_iface(store, *GET_IFACE_DEFS[op], group, value, ctx)
                local.append((op, group[0], lat, status, now_hms()))
                pace()
        else:
            for k in keys_iter():
                lat, status = _run_iface(store, *GET_IFACE_DEFS[op], [k], value, ctx)
                local.append((op, k, lat, status, now_hms()))
                pace()
    else:
        raise SystemExit("[ERROR] unsupported operation: {}".format(op))
    results.extend(local)
def run_warmup(args, store, value, ctx=None):
    """Duration-based warmup at full scale.

    Uses an INDEPENDENT key prefix (mc_warmup*, removed afterwards) at the
    SAME key_count and data_size as the measured phase, and loops the real
    operation until the warmup duration expires (same cycle semantics as
    duration stress: get loops reads, ordinary put/mixed writes use full
    passes with self-clear, and upsert_from repeatedly overwrites its
    existing keys). Pure get-family operations pre-fill their warmup keys
    outside the measured region; ordinary put remains create-only.
    Warmup runs unpaced (target_qps ignored) and its samples are discarded.
    """
    duration = float(getattr(args, "warmup_duration", 0) or 0)
    if duration <= 0:
        return
    prefix = "mc_warmup*{}*{}".format(args.scenario, os.getpid())
    keys = ["{}*{:08d}".format(prefix, i) for i in range(args.key_count)]
    op = args.operation
    put_iface = get_iface = None
    if "/" in op:
        put_iface, get_iface = op.split("/", 1)
    pure_get = op in GET_IFACE_DEFS or (get_iface is not None and put_iface is None)
    if pure_get:
        for key in keys:
            ret = store.put(key, value)
            if ret != 0:
                raise SystemExit(
                    "[ERROR] warmup prefill put key={} returned {}".format(
                        key, ret))
    log("warmup start (duration={}s, keys={}, op={})".format(duration, len(keys), op))
    wargs = copy.copy(args)
    wargs.target_qps = 0  # warmup free-runs; pacing is for measured phase
    results = []
    _run_thread(wargs, store, keys, value, results, deadline=time.time() + duration,
                ctx=ctx)
    store.batch_remove(keys, force=True)
    log("warmup done (ops={})".format(len(results)))
def _percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    idx = int(round(p * (len(sorted_vals) - 1)))
    return sorted_vals[idx]
def cmd_benchmark(args, store):
    value = load_value(args.remote_data_dir, args.scenario)
    keys = build_keys(args.key_prefix, args.scenario, args.key_count)
    data_size = len(value)
    op = args.operation
    duration = args.duration
    put_iface = get_iface = None
    if "/" in op:
        put_iface, get_iface = op.split("/", 1)
    if not duration:
        # duration mode: get-family loops over prefilled keys; put-family and
        # mixed run full-key passes with untimed self-clear between cycles
        # (see _run_thread). All basic operations support it.
        duration = None
    # zero-copy interfaces need a registered page-aligned buffer. get_buffer
    # does NOT: it validates through the BufferHandle's own pointer, so it
    # must not register anything (an unnecessary registration is both waste
    # and an extra failure point).
    ctx = None
    need_zero = (op in ZERO_COPY_PUT_IFACES + ("get_into",)
                 or put_iface in ZERO_COPY_PUT_IFACES
                 or get_iface in ("get_into",))
    if need_zero:
        ctx = _BufferCtx(data_size)
        if not ctx.setup(store):
            print("[ERROR] register_buffer failed, cannot use zero-copy interfaces")
            sys.exit(1)
    if args.warmup == "true":
        run_warmup(args, store, value, ctx)
    end_idx = args.key_end if args.key_end is not None else len(keys)
    chunk = keys[args.key_start:end_idx]
    # Keep CPython's automatic cyclic-GC pauses out of per-operation latency.
    # Reference counting remains active while cyclic GC is disabled.
    gc_was_enabled = gc.isenabled()
    if gc_was_enabled:
        gc.collect()
        gc.disable()
        log("python cyclic GC disabled for measured benchmark")
    try:
        deadline = None
        if duration:
            deadline = time.time() + duration
        emit("META", {"mode": "benchmark", "scenario": args.scenario,
                      "operation": op, "protocol": args.protocol,
                      "client_id": args.client_id,
                      "key_start": args.key_start, "key_end": end_idx,
                      "num_threads": args.num_threads, "warmup": bool(args.warmup == "true"),
                      "chunk_keys": len(chunk), "target_qps": args.target_qps or 0,
                      "start_ts": time.time()})
        log("benchmark start (op={}, chunk_keys={}, threads={}, duration={}, target_qps={})".format(
            op, len(chunk), args.num_threads, duration or 0, args.target_qps or 0))
        results = []
        wall_t0 = time.perf_counter()
        if chunk:
            per_thread = _split_even(chunk, args.num_threads)
            with ThreadPoolExecutor(max_workers=args.num_threads) as pool:
                futures = [pool.submit(_run_thread, args, store, sub, value, results, deadline, ctx)
                           for sub in per_thread]
                for f in futures:
                    f.result()
        wall_s = time.perf_counter() - wall_t0
    finally:
        try:
            if ctx is not None:
                ctx.teardown(store)
        finally:
            if gc_was_enabled:
                gc.enable()
                log("python cyclic GC restored after measured benchmark")
    batch_size = max(1, args.batch_size)
    if op in PUT_BATCH_IFACES or op in GET_BATCH_IFACES:
        bytes_per_op = batch_size * data_size
    else:
        bytes_per_op = data_size
    ok_count = sum(1 for r in results if r[3] == "OK")
    fail_count = len(results) - ok_count
    total = len(results)
    # throughput/bytes count successful operations only (failed ops moved no
    # data); latency statistics also count successful samples only
    if put_iface and get_iface:
        put_bp = batch_size if put_iface in PUT_BATCH_IFACES else 1
        get_bp = batch_size if get_iface in GET_BATCH_IFACES else 1
        ok_put = sum(1 for r in results if r[0] == "put" and r[3] == "OK")
        ok_get = sum(1 for r in results if r[0] == "get" and r[3] == "OK")
        total_bytes = ok_put * put_bp * data_size + ok_get * get_bp * data_size
    else:
        total_bytes = ok_count * bytes_per_op
    lat_ok = [r[2] for r in results if r[3] == "OK"]
    latencies = sorted(lat_ok)
    summary = {
        "mode": "benchmark", "scenario": args.scenario, "operation": op,
        "protocol": args.protocol, "client_id": args.client_id,
        "key_start": args.key_start, "key_end": end_idx,
        "num_threads": args.num_threads,
        "total": total, "bytes": total_bytes, "wall_s": wall_s,
        "ok": ok_count, "fail": fail_count,
        "qps": (total / wall_s) if wall_s > 0 else 0.0,
        "fail_rate": (fail_count / total) if total else 0.0,
        "min_ms": _percentile(latencies, 0.0),
        "max_ms": _percentile(latencies, 1.0),
        "avg_ms": (sum(lat_ok) / len(lat_ok)) if lat_ok else 0.0,
        "p99_ms": _percentile(latencies, 0.99),
        "p999_ms": _percentile(latencies, 0.999),
        "p9999_ms": _percentile(latencies, 0.9999),
        "data_size": data_size,
        "key_count": len(keys),
        "target_qps": args.target_qps or 0,
    }
    if op in PUT_BATCH_IFACES or op in GET_BATCH_IFACES:
        summary["batch_size"] = batch_size
    if put_iface and get_iface:
        summary["put_ratio"] = args.put_ratio
        summary["put_total"] = sum(1 for r in results if r[0] == "put")
        summary["get_total"] = sum(1 for r in results if r[0] == "get")
    if duration:
        summary["duration"] = duration
    emit("SUMMARY", summary)
    log("benchmark done (total={}, ok={}, fail={}, wall_s={:.3f})".format(
        total, ok_count, fail_count, wall_s))
    for opname, k, lat, status, ts in results:
        sys.stdout.write("RESULT\t{}\t{}\t{}\t{:.6f}\t{}\n".format(
            ts, opname, k, lat, status))
    sys.stdout.flush()

# --------------------------------------------------------------------------

# pipeline (精排/召排) mode — independent process writer/reader via HTTP notify

# --------------------------------------------------------------------------

import http.server
import socketserver
import urllib.parse
import threading
from queue import Queue


def _self_host():
    """Routable host for etcd registration: POD_IP (k8s downward API)
    wins, then SETUP_FABRIC_IP (ssh high-perf NIC)."""
    return (os.environ.get("POD_IP")
            or os.environ.get("SETUP_FABRIC_IP") or "").strip()


def _self_node():
    return os.environ.get("NODE_NAME") or socket.gethostname()


def _rendezvous_prefix(args):
    return "mcbench/{}/".format(args.run_id)
def _discover_readers(args, expected, who):
    """Pull the FULL reader list from etcd (readers self-registered) and
    block until `expected` entries exist. Shared by --role writer and
    --role both; replaces the old manually injected --peers argv."""
    # ---- rendezvous: pull the FULL reader list from etcd (readers were
    # started first and self-registered). This replaces the old manually
    # injected --peers argv. Registration happens only after the list is
    # complete, so a registered writer == fully prepared writer. ----
    timeout_s = float(getattr(args, "rendezvous_timeout", 120) or 120)
    rdz_prefix = _rendezvous_prefix(args)
    readers = []
    t0 = time.time()
    while True:
        kvs = bench_etcd.kv_range(args.etcd_endpoints, rdz_prefix + "readers/")
        readers = []
        for _k, v in sorted(kvs.items()):
            try:
                r = json.loads(v)
                r.setdefault("role", "reader")
                readers.append(r)
            except ValueError:
                pass
        if len(readers) >= expected:
            break
        if time.time() - t0 > timeout_s:
            raise SystemExit(
                "[ERROR] {}: only {}/{} readers registered in etcd "
                "after {}s (prefix={})".format(
                    who, len(readers), expected,
                    int(timeout_s), rdz_prefix))
        time.sleep(0.5)
    return readers


def _normalize_local_readers(readers):
    """Auto-resolve reader hosts that live on this machine to 127.0.0.1."""
    # Auto-resolve reader host to 127.0.0.1 if they are on the same machine
    local_ips = set()
    try:
        for info in socket.getaddrinfo(socket.gethostname(), None):
            local_ips.add(info[4][0])
        local_ips.add("127.0.0.1")
        local_ips.add("localhost")
    except Exception:
        local_ips = {"127.0.0.1", "localhost"}
    _norm_readers = []
    for r in readers:
        rh = r["host"]
        try:
            resolved = set()
            for info in socket.getaddrinfo(rh, None):
                resolved.add(info[4][0])
        except Exception:
            resolved = set()
        if resolved and resolved.issubset(local_ips) and local_ips.issuperset(resolved):
            r = dict(r)
            r["host"] = "127.0.0.1"
        elif rh in local_ips:
            r = dict(r)
            r["host"] = "127.0.0.1"
        _norm_readers.append(r)
    readers = _norm_readers
    return readers


def _writer_run_phases(args, store, readers, do_warmup=True, exclude_id=None):
    """Writer write+notify phases (warmup -> measured -> SUMMARY).

    Shared by --role writer and the dual-role (--role both) client. readers
    is the FULL normalized reader list; the notify sampling pool excludes
    exclude_id when set (a dual-role client never notifies its own
    reader)."""
    value = load_value(args.remote_data_dir, args.scenario)
    batch_keys = max(1, args.batch_size)
    prefix = args.key_prefix
    scenario = args.scenario
    writer_id = args.client_id
    writer_op = args.operation or "put_batch"
    if writer_op not in PUT_IFACE_DEFS:
        raise SystemExit("[ERROR] unsupported writer operation: {}".format(writer_op))
    effective_writer_op = (
        "put" if writer_op == "put_batch" and batch_keys == 1 else writer_op
    )
    if effective_writer_op != writer_op:
        log("pipeline writer batch_size=1: using put instead of put_batch")
    ctx = None
    if effective_writer_op in ZERO_COPY_PUT_IFACES:
        ctx = _BufferCtx(len(value))
        if not ctx.setup(store):
            raise SystemExit("[ERROR] writer register_buffer failed")
    duration = float(args.duration)
    warmup_duration = float(getattr(args, "warmup_duration", 0) or 0)
    target_qps = float(args.target_qps) if args.target_qps > 0 else 0
    traffic_mode = args.traffic_mode
    if exclude_id is not None:
        # dual role: never notify our own reader
        pool = [r for r in readers if r.get("id") != exclude_id]
    else:
        pool = list(readers)
    notify_count = int(getattr(args, "notify_count", 0) or 0)
    notify_threads = max(1, int(getattr(args, "notify_threads", 1) or 1))
    notify_async = bool(getattr(args, "notify_async", False))
    notify_max_inflight = max(
        1, int(getattr(args, "notify_max_inflight", 256) or 256))
    # Original datasystem semantics:
    #   balanced = even pacing at target_qps (interval = 1/qps between batches)
    #   pulse    = per 1s window, fire target_qps batches back-to-back (burst),
    #              then idle until the window closes. Average QPS still = target_qps.
    balanced = str(traffic_mode).lower() == "balanced"
    interval = (1.0 / target_qps) if (balanced and target_qps > 0) else 0.0
    emit("META", {"mode": "pipeline_writer", "scenario": scenario,
                  "work_mode": args.work_mode or "recall", "protocol": args.protocol,
                  "writer_id": writer_id, "target_qps": target_qps,
                  "traffic_mode": traffic_mode, "batch_size": batch_keys,
                  "duration": duration, "warmup_duration": warmup_duration,
                  "start_ts": time.time(),
                  "num_readers": len(pool), "notify_count": notify_count,
                  "notify_threads": notify_threads,
                  "notify_async": notify_async,
                  "notify_max_inflight": notify_max_inflight,
                  "writer_count": args.writer_count, "reader_count": args.reader_count})
    import urllib.request
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    notify_pool = ThreadPoolExecutor(max_workers=notify_threads)
    pending_notifies = set()
    notify_stats = {
        "submitted": 0, "ok": 0, "fail": 0, "inflight_peak": 0,
        "backpressure_ms": 0.0, "drain_ms": 0.0,
        "elapsed_ms_sum": 0.0, "elapsed_ms_max": 0.0,
    }
    seq = [0]
    # optional per-writer key-space cap (BENCH_KEY_COUNT): the sequence wraps at
    # key_cap; wrapped non-upsert batches remove their keys first
    # (delete-before-write) so puts always create fresh objects — bounding
    # store usage at writers x key_count x data_size. upsert_from instead
    # overwrites the wrapped keys (its own semantics). 0 = unbounded.
    key_cap = int(getattr(args, "key_count", 0) or 0)
    if key_cap and key_cap < batch_keys:
        raise SystemExit("[ERROR] key_count {} < batch_size {}".format(key_cap, batch_keys))
    wrapped_logged = [False]
    insufficient_streak = [0]
    # measured-phase stats (warmup writes stay out of the metrics)
    stats = {"keys_ok": 0, "keys_fail": 0, "batches_ok": 0, "batches_fail": 0, "lats": []}
    last_flush = [time.time()]

    def _notify(r, ks, ts, wf):
        query = "&".join("key=" + k for k in ks)
        url = "http://{}:{}/notify?{}&writer_id={}&write_ts={}&warmup={}".format(
            r["host"], r["port"], query, writer_id, ts, wf)
        started = time.perf_counter()
        try:
            response = opener.open(url, timeout=0.5)
            response.close()
            ok, error = True, ""
        except Exception as exc:
            ok, error = False, str(exc)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        if not ok:
            sys.stdout.write(
                "RESULT\t{}\tpipeline_notify\t{}\t0\tERR:{} "
                "target={}:{} elapsed_ms={:.3f} warmup={}\n".format(
                    now_hms(), ks[0], error, r["host"], r["port"],
                    elapsed_ms, wf))
        return wf == "1", ok, elapsed_ms

    def _record_notify(future):
        try:
            is_warmup, ok, elapsed_ms = future.result()
        except Exception as exc:
            # _notify catches transport errors itself; this protects the writer
            # from an unexpected worker exception without losing the denominator.
            is_warmup, ok, elapsed_ms = False, False, 0.0
            sys.stdout.write(
                "RESULT\t{}\tpipeline_notify\t<worker>\t0\tERR:{}\n".format(
                    now_hms(), exc))
        if is_warmup:
            return
        notify_stats["ok" if ok else "fail"] += 1
        notify_stats["elapsed_ms_sum"] += elapsed_ms
        notify_stats["elapsed_ms_max"] = max(
            notify_stats["elapsed_ms_max"], elapsed_ms)

    def _reap_ready_notifies():
        done = [future for future in pending_notifies if future.done()]
        for future in done:
            pending_notifies.remove(future)
            _record_notify(future)

    def _wait_for_notify_slot(measured):
        if not pending_notifies:
            return
        started = time.perf_counter()
        done, _ = wait(pending_notifies, return_when=FIRST_COMPLETED)
        if measured:
            notify_stats["backpressure_ms"] += \
                (time.perf_counter() - started) * 1000.0
        for future in done:
            pending_notifies.remove(future)
            _record_notify(future)

    def _drain_notifies(measured):
        if not pending_notifies:
            return
        started = time.perf_counter()
        done, _ = wait(pending_notifies)
        for future in done:
            _record_notify(future)
        pending_notifies.clear()
        if measured:
            notify_stats["drain_ms"] += \
                (time.perf_counter() - started) * 1000.0

    def _dispatch_notifies(selected, keys, write_ts, wflag, is_warmup):
        if not selected:
            return
        if notify_async:
            _reap_ready_notifies()
            for reader in selected:
                while len(pending_notifies) >= notify_max_inflight:
                    _wait_for_notify_slot(not is_warmup)
                pending_notifies.add(notify_pool.submit(
                    _notify, reader, keys, write_ts, wflag))
                if not is_warmup:
                    notify_stats["submitted"] += 1
                    notify_stats["inflight_peak"] = max(
                        notify_stats["inflight_peak"], len(pending_notifies))
            _reap_ready_notifies()
            return

        # Legacy closed-loop mode: one batch fans out concurrently, but the
        # next write waits for every selected reader.
        futures = [notify_pool.submit(
            _notify, reader, keys, write_ts, wflag) for reader in selected]
        if not is_warmup:
            notify_stats["submitted"] += len(futures)
            notify_stats["inflight_peak"] = max(
                notify_stats["inflight_peak"], len(futures))
        for future in futures:
            _record_notify(future)

    def _run_phase(phase_deadline, is_warmup):
        next_t = time.time() + interval
        burst_remaining = 0
        burst_budget = 0.0
        window_start = 0.0
        while time.time() < phase_deadline:
            if not balanced and target_qps > 0:
                if burst_remaining <= 0:
                    # fractional per-writer rates (e.g. 52.5) are accumulated
                    # across windows so the long-run average stays exact
                    burst_budget += target_qps
                    burst_remaining = int(burst_budget)
                    burst_budget -= burst_remaining
                    window_start = time.time()
            # key-space wrap: base stays within [0, key_cap); from the second
            # cycle (seq >= key_cap) every batch is delete-before-write so
            # the timed put always creates fresh objects
            base = seq[0] % key_cap if key_cap > 0 else seq[0]
            if key_cap > 0 and base + batch_keys > key_cap:
                base = key_cap - batch_keys
            delete_first = (effective_writer_op != "upsert_from" and
                            key_cap > 0 and seq[0] >= key_cap)
            keys = ["{}*{}*w{}*{:08d}".format(prefix, scenario, writer_id, base + i)
                    for i in range(batch_keys)]
            if delete_first:
                if not wrapped_logged[0]:
                    log("writer w{} key space wrapped at {} keys: delete-before-write mode".format(
                        writer_id, key_cap))
                    wrapped_logged[0] = True
                store.batch_remove(keys, force=True)  # untimed maintenance
            seq[0] += batch_keys
            # interface dispatch: put_batch = one call per whole batch;
            # put/put_from/upsert_from = one timed call per key
            # (key_count=1 each)
            write_samples = []
            if effective_writer_op in PUT_BATCH_IFACES:
                lat_ms, status = _run_iface(
                    store, *PUT_IFACE_DEFS[effective_writer_op], keys, value, ctx
                )
                write_samples.append((keys[0], lat_ms, status, batch_keys))
            else:
                for k in keys:
                    lat_ms, status = _run_iface(
                        store,
                        *PUT_IFACE_DEFS[effective_writer_op],
                        [k],
                        value,
                        ctx
                    )
                    write_samples.append((k, lat_ms, status, 1))
            batch_ok = all(s == "OK" for _, _, s, _ in write_samples)
            # fail-fast: storage exhaustion (put rc=-200 insufficient space)
            # otherwise degrades into a full-MISS garbage run — abort early
            # with an actionable message instead
            if not batch_ok and all("ERR:-200" in s for _, _, s, _ in write_samples):
                insufficient_streak[0] += 1
            else:
                insufficient_streak[0] = 0
            if insufficient_streak[0] >= 10 and not is_warmup:
                raise RuntimeError(
                    "writer w{}: store storage exhausted ({} consecutive batches rc=-200 "
                    "insufficient space); set pipeline key_count to cap the key space "
                    "(writers x key_count x data_size must fit the store)".format(
                        writer_id, insufficient_streak[0]))
            if not is_warmup:
                for k, lat_ms, status, kc in write_samples:
                    if status == "OK":
                        stats["keys_ok"] += kc
                        stats["batches_ok"] += 1
                        stats["lats"].append(lat_ms)
                    else:
                        stats["keys_fail"] += kc
                        stats["batches_fail"] += 1
                    sys.stdout.write("RESULT\t{}\tpipeline_write\t{}\t{:.6f}\t{}\t{}\n".format(
                        now_hms(), k, lat_ms, status, kc))
            if batch_ok:
                # Randomly select notify_count readers and deliver the whole
                # batch in a single multi-key request per reader (bypass proxy;
                # write_ts is retained for diagnostics only — reader latency is
                # measured locally on the reader node with time.perf_counter();
                # warmup flag lets readers warm up without recording).
                n_sel = min(notify_count, len(pool)) if notify_count > 0 else len(pool)
                selected = random.sample(pool, n_sel) if pool else []
                write_ts = time.time()
                wflag = "1" if is_warmup else "0"
                _dispatch_notifies(selected, keys, write_ts, wflag, is_warmup)
            if not is_warmup:
                now = time.time()
                if now - last_flush[0] >= 1.0:
                    sys.stdout.flush()
                    last_flush[0] = now
            if balanced and target_qps > 0:
                next_t += interval
                time.sleep(max(0.0, next_t - time.time()))
            elif target_qps > 0:
                burst_remaining -= 1
                if burst_remaining <= 0:
                    sleep_t = window_start + 1.0 - time.time()
                    if sleep_t > 0:
                        time.sleep(sleep_t)

    try:
        if do_warmup and warmup_duration > 0:
            log("pipeline writer warmup start (duration={}s, target_qps={})".format(
                warmup_duration, target_qps))
            emit("META", {"mode": "pipeline_warmup_start", "writer_id": writer_id,
                          "duration": warmup_duration, "ts": time.time()})
            _run_phase(time.time() + warmup_duration, True)
            # Do not let warmup notifications leak into the measured window.
            _drain_notifies(False)
            emit("META", {"mode": "pipeline_warmup_end", "writer_id": writer_id,
                          "ts": time.time()})
            log("pipeline writer warmup done")
        log("pipeline writer measured start (duration={}s)".format(duration))
        _run_phase(time.time() + duration, False)
        # Preserve delivery semantics: all accepted notifications finish before
        # the writer summary/client shutdown, but this drain is outside 30s.
        _drain_notifies(True)
    finally:
        _drain_notifies(False)
        notify_pool.shutdown(wait=True)
    log("pipeline writer measured done (keys_ok={}, keys_fail={})".format(
        stats["keys_ok"], stats["keys_fail"]))
    sys.stdout.flush()
    if ctx is not None:
        ctx.teardown(store)
    # Writer performance summary (measured phase only; keys/duration based).
    # Keeps the writer log self-contained: META + SUMMARY pair like the
    # reader/basic clients, so the orchestrator aggregation is uniform.
    keys_total = stats["keys_ok"] + stats["keys_fail"]
    lats = stats["lats"]
    ok_bytes = stats["keys_ok"] * len(value)
    busy_s = (sum(lats) / 1000.0) if lats else 0.0
    emit("SUMMARY", {
        "mode": "pipeline_writer_summary", "scenario": scenario,
        "work_mode": args.work_mode or "recall", "protocol": args.protocol,
        "writer_id": writer_id, "target_qps": target_qps,
        "traffic_mode": traffic_mode, "batch_size": batch_keys,
        "duration": duration, "warmup_duration": warmup_duration,
        "total": keys_total, "bytes": ok_bytes,
        "wall_s": duration, "ok": stats["keys_ok"], "fail": stats["keys_fail"],
        "qps": (keys_total / duration) if duration > 0 else 0.0,
        "fail_rate": (stats["keys_fail"] / keys_total) if keys_total else 0.0,
        "notify_async": notify_async,
        "notify_max_inflight": notify_max_inflight,
        "notify_submitted": notify_stats["submitted"],
        "notify_ok": notify_stats["ok"],
        "notify_fail": notify_stats["fail"],
        "notify_inflight_peak": notify_stats["inflight_peak"],
        "notify_backpressure_ms": notify_stats["backpressure_ms"],
        "notify_drain_ms": notify_stats["drain_ms"],
        "notify_avg_ms": ((notify_stats["elapsed_ms_sum"] /
                           (notify_stats["ok"] + notify_stats["fail"]))
                          if (notify_stats["ok"] + notify_stats["fail"]) else 0.0),
        "notify_max_ms": notify_stats["elapsed_ms_max"],
        # interface throughput = successful bytes / sum of successful
        # put_batch call durations (busy time, not wall clock)
        "throughput_mb_s": (ok_bytes / busy_s / 1024.0 ** 2) if busy_s > 0 else 0.0,
        "throughput_gb_s": (ok_bytes / busy_s / 1024.0 ** 3) if busy_s > 0 else 0.0,
        "min_ms": _percentile(sorted(lats), 0.0),
        "max_ms": _percentile(sorted(lats), 1.0),
        "avg_ms": (sum(lats) / len(lats)) if lats else 0.0,
        "p99_ms": _percentile(sorted(lats), 0.99),
        "p999_ms": _percentile(sorted(lats), 0.999),
        "p9999_ms": _percentile(sorted(lats), 0.9999),
        "data_size": len(value),
    })
    sys.stdout.flush()


def _pipeline_writer(args, store, do_warmup=True):
    """Writer process: discover readers from etcd, register the /start
    address, wait for the orchestrator trigger, then run the write+notify
    phases (see _writer_run_phases; warmup uses the same deployment shape
    to activate writer->store, writer->reader and reader->store paths)."""
    writer_id = args.client_id
    timeout_s = float(getattr(args, "rendezvous_timeout", 120) or 120)
    rdz_prefix = _rendezvous_prefix(args)
    readers = _discover_readers(args, int(args.reader_count or 0),
                                "writer {}".format(writer_id))
    readers = _normalize_local_readers(readers)
    # ---- writer HTTP /start endpoint: the orchestrator triggers the run
    # here after seeing ALL writers registered in etcd ----
    started = threading.Event()

    class _WriterStartHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self._handle()

        def do_POST(self):
            self._handle()

        def _handle(self):
            # NOTE: no urllib here — the enclosing function imports
            # urllib.request LATER, which would shadow the closure binding
            path = self.path.split("?")[0]
            if path == "/start":
                started.set()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"STARTED")
            elif path == "/health":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"HEALTHY")
            else:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"UNKNOWN:" + self.path.encode())

        def log_message(self, fmt, *args):
            pass

    socketserver.ThreadingTCPServer.allow_reuse_address = True
    try:
        wserver = socketserver.ThreadingTCPServer(
            ("0.0.0.0", args.listen_port), _WriterStartHandler)
    except OSError as exc:
        raise SystemExit("[ERROR] writer failed to bind port {}: {}".format(
            args.listen_port, exc))
    wserver.daemon_threads = True
    threading.Thread(target=wserver.serve_forever, daemon=True).start()
    wreg = {"host": _self_host(), "port": args.listen_port,
            "id": writer_id}
    bench_etcd.kv_put(args.etcd_endpoints,
                      rdz_prefix + "writers/{}".format(writer_id),
                      json.dumps(wreg))
    emit("META", {"mode": "pipeline_writer_registered", "writer_id": writer_id,
                  "reg": wreg, "ts": time.time()})
    log("pipeline writer registered (gid={}, http_port={}); waiting for "
        "/start trigger (timeout={}s)".format(writer_id, args.listen_port,
                                              int(timeout_s)))
    if not started.wait(timeout=timeout_s):
        wserver.shutdown()
        raise SystemExit("[ERROR] writer {}: no /start trigger within {}s"
                         "".format(writer_id, int(timeout_s)))
    wserver.shutdown()
    emit("META", {"mode": "pipeline_writer_started", "writer_id": writer_id,
                  "ts": time.time()})
    _writer_run_phases(args, store, readers, do_warmup)


class _PipelineNotifyHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler for reader: receives write notifications from writers.

    Dual-role clients additionally arm start_event so the SAME port also
    serves the writer-side /start trigger."""
    # Class-level shared state set by _pipeline_reader / _pipeline_dual
    queue = None
    deadline = 0.0
    exit_grace = 5.0
    start_event = None  # armed by _pipeline_dual; None = pure reader
    # backlog diagnostics: exact queue depth (lock-protected) + observed max
    qdepth = 0
    qmax = 0
    qlock = None
    def do_GET(self):
        self._handle()
    def do_POST(self):
        self._handle()
    def _handle(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)
        if parsed.path == "/notify":
            keys = params.get("key", [])
            writer_id = params.get("writer_id", ["0"])[0]
            write_ts = float(params.get("write_ts", ["0"])[0])
            is_warmup = params.get("warmup", ["0"])[0] == "1"
            # one queue item per notify batch (the writer's whole batch); the
            # queue depth unit therefore counts pending notify batches
            if time.time() < _PipelineNotifyHandler.deadline + _PipelineNotifyHandler.exit_grace:
                _PipelineNotifyHandler.queue.put(
                    (keys, int(writer_id), write_ts, is_warmup, time.perf_counter()))
                with _PipelineNotifyHandler.qlock:
                    _PipelineNotifyHandler.qdepth += 1
                    if _PipelineNotifyHandler.qdepth > _PipelineNotifyHandler.qmax:
                        _PipelineNotifyHandler.qmax = _PipelineNotifyHandler.qdepth
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"OK")
        elif parsed.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"HEALTHY")
        elif parsed.path == "/start" and \
                _PipelineNotifyHandler.start_event is not None:
            _PipelineNotifyHandler.start_event.set()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"STARTED")
        elif parsed.path == "/shutdown":
            _PipelineNotifyHandler.deadline = -1.0
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"SHUTDOWN")
        else:
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"UNKNOWN:" + self.path.encode())
    def log_message(self, format, *args):
        pass  # suppress HTTP server logs
    do_GET = do_POST
def _reader_worker(args, store, queue, deadline, results, ctx=None):
    """Reader worker thread: dequeue a notify batch, run the optional exist
    pre-checks, then execute the configured get interface (reader.operation).

    Latency only wraps the store get-family interface call (time.perf_counter(),
    reader-local monotonic clock); exist pre-checks and value validation are
    not part of the timed region. queue_wait (notify arrival -> batch dequeue)
    and queue depth (in notify batches) are tracked as backlog diagnostics.

    Warmup items (warmup=1 flag from the notify) are still processed — the
    actual store reads warm up the reader->store path — but are tagged so
    they stay out of the measured metrics.
    """
    pipeline = args.notify_pipeline if isinstance(args.notify_pipeline, list) else ["get"]
    reader_op = args.operation or "get"
    if reader_op not in GET_IFACE_DEFS:
        raise SystemExit("[ERROR] unsupported reader operation: {}".format(reader_op))
    value = load_value(args.remote_data_dir, args.scenario)
    batch_mode = reader_op in GET_BATCH_IFACES
    local = []
    while time.time() < deadline:
        try:
            item = queue.get(timeout=1.0)
        except:
            continue
        if item is None:
            break
        keys, writer_id, write_ts, is_warmup, arrival_mono = item
        with _PipelineNotifyHandler.qlock:
            _PipelineNotifyHandler.qdepth -= 1
        queue_wait_ms = (time.perf_counter() - arrival_mono) * 1000.0
        # exist pre-check (untimed) for every key of the batch
        if pipeline:
            ok = True
            for step in pipeline:
                step = step.strip()
                if step == "exist":
                    for k in keys:
                        if store.is_exist(k) <= 0:
                            ok = False
                            break
                # "get"/"getBuffer" step names are ignored: the data interface
                # is fully decided by reader.operation
            if not ok:
                if is_warmup:
                    local.append(("pipeline_warmup_read", keys[0], 0.0, "SKIP", queue_wait_ms, len(keys), now_hms()))
                    continue
                local.append(("pipeline_read", keys[0], 0.0, "MISS/MISMATCH", queue_wait_ms, len(keys), now_hms()))
                continue
        if batch_mode:
            lat_ms, status = _run_iface(store, *GET_IFACE_DEFS[reader_op], keys, value, ctx)
            if is_warmup:
                local.append(("pipeline_warmup_read", keys[0], lat_ms, "SKIP", queue_wait_ms, len(keys), now_hms()))
                continue
            local.append(("pipeline_read", keys[0], lat_ms, status, queue_wait_ms, len(keys), now_hms()))
        else:
            for k in keys:
                lat_ms, status = _run_iface(store, *GET_IFACE_DEFS[reader_op], [k], value, ctx)
                if is_warmup:
                    local.append(("pipeline_warmup_read", k, lat_ms, "SKIP", queue_wait_ms, 1, now_hms()))
                    continue
                local.append(("pipeline_read", k, lat_ms, status, queue_wait_ms, 1, now_hms()))
    results.extend(local)
def cmd_pipeline(args, store):
    """Run pipeline (精排/召排) — dispatches to writer or reader based on --role.

    Rendezvous (etcd, prefix mcbench/<run-id>/): readers self-register after
    their HTTP server binds; writers pull the full reader list from etcd,
    register their own /start address, then wait for the orchestrator's
    HTTP trigger before entering warmup + measured phases."""
    role = args.role
    if not args.run_id or not args.etcd_endpoints:
        raise SystemExit("[ERROR] pipeline requires --run-id and "
                         "--etcd-endpoints (BENCH_RUN_ID / "
                         "SETUP_ETCD_ENDPOINTS via env.sh)")
    if role == "writer":
        if args.listen_port is None:
            raise SystemExit("[ERROR] pipeline writer requires --listen-port "
                             "(HTTP /start trigger port)")
        # warmup is a short same-deployment run handled inside the writer
        _pipeline_writer(args, store, do_warmup=(args.warmup == "true"))
    elif role == "reader":
        if args.listen_port is None:
            raise SystemExit("[ERROR] pipeline reader requires --listen-port")
        _pipeline_reader(args, store)
    elif role == "both":
        if args.listen_port is None:
            raise SystemExit("[ERROR] pipeline dual role requires "
                             "--listen-port (shared /notify + /start port)")
        _pipeline_dual(args, store)
    else:
        raise SystemExit("[ERROR] pipeline mode requires --role "
                         "writer|reader|both")

# --------------------------------------------------------------------------

def _pipeline_reader(args, store):
    """Reader process: HTTP server to receive notify, worker threads to read store."""
    lifetime = float(args.timeout)
    # measurement window = configured duration (BENCH_DURATION); fall back to
    # lifetime only if duration was not provided
    meas_duration = float(args.duration or 0) or lifetime
    exit_grace = 5.0
    num_threads = max(1, args.num_threads)
    my_port = args.listen_port
    reader_op = args.operation or "get"
    if reader_op not in GET_IFACE_DEFS:
        raise SystemExit("[ERROR] unsupported reader operation: {}".format(reader_op))
    ctx = None
    # only get_into needs a registered local buffer; get_buffer validates via
    # the handle's own pointer and must not register anything
    if reader_op in ("get_into",):
        ctx = _BufferCtx(parse_size(args.data_size))
        if not ctx.setup(store):
            raise SystemExit("[ERROR] reader register_buffer failed")
    emit("META", {"mode": "pipeline_reader", "scenario": args.scenario,
                  "work_mode": args.work_mode or "recall", "protocol": args.protocol,
                  "client_id": args.client_id, "reader_count": args.reader_count,
                  "listen_port": my_port, "num_threads": num_threads,
                  "notify_pipeline": args.notify_pipeline,
                  "duration": lifetime, "measure_window": meas_duration,
                  "start_ts": time.time()})
    queue = Queue(maxsize=max(8192, num_threads * 1024))
    deadline = time.time() + lifetime
    results = []
    # Set class-level shared state for the HTTP handler
    _PipelineNotifyHandler.queue = queue
    _PipelineNotifyHandler.deadline = deadline
    _PipelineNotifyHandler.exit_grace = exit_grace
    _PipelineNotifyHandler.qdepth = 0
    _PipelineNotifyHandler.qmax = 0
    _PipelineNotifyHandler.qlock = threading.Lock()
    # Start HTTP server in a daemon thread BEFORE starting workers so a bind
    # failure fails fast (workers would otherwise block the exception for the
    # whole reader lifetime)
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    try:
        server = socketserver.ThreadingTCPServer(("0.0.0.0", my_port), _PipelineNotifyHandler)
    except OSError as exc:
        raise SystemExit("[ERROR] reader failed to bind port {}: {}".format(my_port, exc))
    server.daemon_threads = True
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    emit("META", {"mode": "pipeline_reader_ready", "port": my_port, "ts": time.time()})
    log("pipeline reader ready (port={}, operation={}, threads={})".format(
        my_port, reader_op, num_threads))
    # ---- rendezvous: self-register into etcd so writers (and the
    # orchestrator's shutdown pass) can discover this reader ----
    rreg = {"host": _self_host(), "port": my_port, "node": _self_node(),
            "id": args.client_id}
    if not rreg["host"]:
        raise SystemExit("[ERROR] reader {}: no routable host for etcd "
                         "registration (POD_IP/SETUP_FABRIC_IP unset)"
                         "".format(args.client_id))
    bench_etcd.kv_put(args.etcd_endpoints,
                      _rendezvous_prefix(args) + "readers/{}".format(args.client_id),
                      json.dumps(rreg))
    emit("META", {"mode": "pipeline_reader_registered", "client_id": args.client_id,
                  "reg": rreg, "ts": time.time()})
    # Start worker threads
    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        worker_futures = [pool.submit(_reader_worker, args, store, queue, deadline + exit_grace, results, ctx)
                         for _ in range(num_threads)]
        # Wait for deadline + grace, waking periodically to check shutdown
        remaining = deadline + exit_grace - time.time()
        while remaining > 0 and _PipelineNotifyHandler.deadline > 0:
            time.sleep(min(1.0, remaining))
            remaining = deadline + exit_grace - time.time()
        # Signal workers to stop
        for _ in range(num_threads):
            queue.put(None)
        for f in worker_futures:
            f.result()
        server.shutdown()
    log("pipeline reader done (items={})".format(len(results)))
    if ctx is not None:
        ctx.teardown(store)
    _emit_reader_summary(args, results, num_threads, meas_duration)

# --------------------------------------------------------------------------

def _emit_reader_summary(args, results, num_threads, meas_duration, out=None):
    """Flush the reader-side SUMMARY + RESULT rows (shared by the pure
    reader process and the dual-role client).

    out: optional writable stream. The dual-role client writes reader output
    to a SEPARATE log (MC_READER_LOG) so writer and reader samples never
    share a file — logs and latency/queue-wait plots stay role-separated."""
    def _w(text):
        stream = out if out is not None else sys.stdout
        stream.write(text)
        stream.flush()
    data_size = parse_size(args.data_size)
    # split off warmup items: processed (store reads warmed up) but not
    # part of the measured metrics; counts are key-based (r[5] = key_count)
    warmup_total = sum(r[5] for r in results if r[0] == "pipeline_warmup_read")
    measured = [r for r in results if r[0] != "pipeline_warmup_read"]
    ok_keys = sum(r[5] for r in measured if r[3] == "OK")
    fail_keys = sum(r[5] for r in measured if r[3] != "OK")
    total = ok_keys + fail_keys
    # bytes/throughput count successful reads only
    total_bytes = ok_keys * data_size
    lat_ok = [r[2] for r in measured if r[3] == "OK"]
    latencies = sorted(lat_ok)
    busy_s = (sum(lat_ok) / 1000.0) if lat_ok else 0.0
    queue_waits = [r[4] for r in results]
    summary = {
        "mode": "pipeline_reader", "scenario": args.scenario,
        "work_mode": args.work_mode or "recall", "protocol": args.protocol,
        "client_id": args.client_id, "reader_count": args.reader_count,
        "num_threads": num_threads,
        "total": total, "bytes": total_bytes, "wall_s": meas_duration,
        "warmup_total": warmup_total,
        "ok": ok_keys, "fail": fail_keys,
        "qps": (total / meas_duration) if meas_duration > 0 else 0.0,
        "fail_rate": (fail_keys / total) if total else 0.0,
        # interface throughput = successful bytes / sum of successful store.get
        # call durations (busy time, not wall clock)
        "throughput_mb_s": (total_bytes / busy_s / 1024.0 ** 2) if busy_s > 0 else 0.0,
        "throughput_gb_s": (total_bytes / busy_s / 1024.0 ** 3) if busy_s > 0 else 0.0,
        "min_ms": _percentile(latencies, 0.0),
        "max_ms": _percentile(latencies, 1.0),
        "avg_ms": (sum(lat_ok) / len(lat_ok)) if lat_ok else 0.0,
        "p99_ms": _percentile(latencies, 0.99),
        "p999_ms": _percentile(latencies, 0.999),
        "p9999_ms": _percentile(latencies, 0.9999),
        "queue_wait_avg_ms": (sum(queue_waits) / len(queue_waits)) if queue_waits else 0.0,
        "queue_wait_max_ms": max(queue_waits) if queue_waits else 0.0,
        "max_depth": _PipelineNotifyHandler.qmax,
        "data_size": data_size,
    }
    if out is not None:
        _w("SUMMARY\t{}\n".format(json.dumps(summary)))
    else:
        emit("SUMMARY", summary)
    for opname, k, lat, status, qw, kc, ts in measured:
        _w("RESULT\t{}\t{}\t{}\t{:.6f}\t{}\t{:.6f}\t{}\n".format(
            ts, opname, k, lat, status, qw, kc))


def _pipeline_dual(args, store):
    """Dual-role pipeline client (--role both): ONE process hosting BOTH
    roles behind ONE HTTP port — /notify + /health + /shutdown for the
    reader side, /start for the writer side.

    Sequence: bind + register the reader entry -> discover the full reader
    list from etcd -> register the writer entry -> wait for the
    orchestrator's /start trigger -> run the writer phases (warmup ->
    measured, notify sampling EXCLUDES our own reader) in a thread while
    the main thread serves the reader queue -> /shutdown flushes both
    summaries and exits."""
    my_id = args.client_id
    my_port = args.listen_port
    num_threads = max(1, args.num_threads)
    writer_op = (args.writer_operation or args.operation or "put_batch")
    reader_op = (args.reader_operation or args.operation or "get")
    if writer_op not in PUT_IFACE_DEFS:
        raise SystemExit("[ERROR] unsupported writer operation: {}".format(writer_op))
    if reader_op not in GET_IFACE_DEFS:
        raise SystemExit("[ERROR] unsupported reader operation: {}".format(reader_op))
    # dual argv carries BOTH operations; stash them where the shared phase
    # functions expect them (args.operation stays reader-side default)
    args_wr = copy.copy(args)
    args_wr.operation = writer_op
    lifetime = float(args.timeout)
    meas_duration = float(args.duration or 0) or lifetime
    exit_grace = 5.0
    timeout_s = float(getattr(args, "rendezvous_timeout", 120) or 120)
    rdz_prefix = _rendezvous_prefix(args)
    rctx = None
    if reader_op in ("get_into",):
        rctx = _BufferCtx(parse_size(args.data_size))
        if not rctx.setup(store):
            raise SystemExit("[ERROR] dual client register_buffer failed")
    emit("META", {"mode": "pipeline_dual", "scenario": args.scenario,
                  "work_mode": args.work_mode or "recall", "protocol": args.protocol,
                  "client_id": my_id, "listen_port": my_port,
                  "writer_operation": writer_op, "reader_operation": reader_op,
                  "num_threads": num_threads, "duration": lifetime,
                  "measure_window": meas_duration, "start_ts": time.time()})
    queue = Queue(maxsize=max(8192, num_threads * 1024))
    deadline = time.time() + lifetime
    results = []
    started = threading.Event()
    _PipelineNotifyHandler.queue = queue
    _PipelineNotifyHandler.deadline = deadline
    _PipelineNotifyHandler.exit_grace = exit_grace
    _PipelineNotifyHandler.qdepth = 0
    _PipelineNotifyHandler.qmax = 0
    _PipelineNotifyHandler.qlock = threading.Lock()
    _PipelineNotifyHandler.start_event = started
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    try:
        server = socketserver.ThreadingTCPServer(("0.0.0.0", my_port),
                                                 _PipelineNotifyHandler)
    except OSError as exc:
        raise SystemExit("[ERROR] dual client failed to bind port {}: {}"
                         "".format(my_port, exc))
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, daemon=True).start()
    # rendezvous: register reader, discover peers, register writer
    rreg = {"host": _self_host(), "port": my_port, "node": _self_node(),
            "id": my_id}
    if not rreg["host"]:
        raise SystemExit("[ERROR] dual {}: no routable host for etcd "
                         "registration (POD_IP/SETUP_FABRIC_IP unset)"
                         "".format(my_id))
    bench_etcd.kv_put(args.etcd_endpoints,
                      rdz_prefix + "readers/{}".format(my_id), json.dumps(rreg))
    readers = _discover_readers(args, int(args.reader_count or 0),
                                "dual {}".format(my_id))
    readers = _normalize_local_readers(readers)
    wreg = {"host": _self_host(), "port": my_port, "id": my_id}
    bench_etcd.kv_put(args.etcd_endpoints,
                      rdz_prefix + "writers/{}".format(my_id), json.dumps(wreg))
    emit("META", {"mode": "pipeline_dual_registered", "client_id": my_id,
                  "reg": wreg, "ts": time.time()})
    log("pipeline dual client registered (id={}, http_port={}); waiting for "
        "/start trigger (timeout={}s)".format(my_id, my_port, int(timeout_s)))
    if not started.wait(timeout=timeout_s):
        server.shutdown()
        raise SystemExit("[ERROR] dual {}: no /start trigger within {}s"
                         "".format(my_id, int(timeout_s)))
    emit("META", {"mode": "pipeline_dual_started", "client_id": my_id,
                  "ts": time.time()})
    # writer phases in a thread (exceptions captured; reader loop watches)
    writer_error = []
    def _wthread():
        try:
            _writer_run_phases(args_wr, store, readers,
                               do_warmup=(args.warmup == "true"),
                               exclude_id=my_id)
            # Keep every reader alive until all peers have drained their
            # asynchronous sends; the controller gates /shutdown on this KV.
            bench_etcd.kv_put(args.etcd_endpoints,
                              rdz_prefix + "writers_done/{}".format(my_id), "1")
        except BaseException as exc:
            import traceback
            traceback.print_exc()
            writer_error.append(exc)
    wthread = threading.Thread(target=_wthread, daemon=True)
    wthread.start()
    # reader side: worker pool + wait for /shutdown (or writer failure)
    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        worker_futures = [
            pool.submit(_reader_worker, args, store, queue,
                        deadline + exit_grace, results, rctx)
            for _ in range(num_threads)]
        remaining = deadline + exit_grace - time.time()
        while remaining > 0 and _PipelineNotifyHandler.deadline > 0 \
                and not writer_error:
            time.sleep(min(1.0, remaining))
            remaining = deadline + exit_grace - time.time()
        if writer_error:
            log("[ERROR] dual {}: writer phase failed: {}".format(
                my_id, writer_error[0]))
            _PipelineNotifyHandler.deadline = -1.0
        for _ in range(num_threads):
            queue.put(None)
        for f in worker_futures:
            f.result()
        server.shutdown()
    wthread.join(timeout=60)
    log("pipeline dual client done (reader items={})".format(len(results)))
    if rctx is not None:
        rctx.teardown(store)
    # reader output → its own log (writer rows stay on stdout / main log)
    reader_log = (os.environ.get("MC_READER_LOG") or "").strip()
    out_stream = None
    if reader_log:
        try:
            out_stream = open(reader_log, "a", encoding="utf-8", buffering=1)
        except OSError:
            log("[WARN] cannot open MC_READER_LOG {}; reader output -> stdout"
                "".format(reader_log))
            out_stream = None
    try:
        _emit_reader_summary(args, results, num_threads, meas_duration,
                             out=out_stream)
    finally:
        if out_stream is not None:
            out_stream.close()
    if writer_error:
        raise SystemExit("[ERROR] dual {}: writer phase failed: {}"
                         "".format(my_id, writer_error[0]))


# --------------------------------------------------------------------------

def _resolve_env(args):
    """After parsing CLI args, fall back to env.sh vars for any unset params."""
    if args.protocol is None:
        args.protocol = os.environ.get("SETUP_PROTOCOL", "tcp")
    if args.scenario is None:
        args.scenario = os.environ.get("BENCH_SCENARIO", None)
    if args.work_mode is None:
        args.work_mode = os.environ.get("BENCH_WORK_MODE", None)
    if args.operation is None:
        args.operation = os.environ.get("BENCH_OPERATION", None)
    if args.key_prefix is None:
        args.key_prefix = os.environ.get("BENCH_KEY_PREFIX", "mc_bench")
    if args.key_count is None:
        args.key_count = int(os.environ.get("BENCH_KEY_COUNT", "1000"))
    if args.data_size is None:
        args.data_size = os.environ.get("BENCH_DATA_SIZE", "8MB")
    if args.batch_size is None:
        args.batch_size = int(os.environ.get("BENCH_BATCH_SIZE", "1"))
    if args.put_ratio is None:
        args.put_ratio = float(os.environ.get("BENCH_PUT_RATIO", "0.5"))
    if args.duration is None:
        dur = os.environ.get("BENCH_DURATION", None)
        if dur is not None:
            args.duration = float(dur)
        else:
            args.duration = 0.0
    if args.warmup_duration is None:
        args.warmup_duration = float(os.environ.get("BENCH_WARMUP_DURATION", "0"))
    if args.master_addr is None:
        args.master_addr = os.environ.get("SETUP_MASTER_ADDR", None)
    if args.metadata_server is None:
        args.metadata_server = os.environ.get("SETUP_METADATA_SERVER", None)
    if args.etcd_endpoints is None:
        args.etcd_endpoints = os.environ.get("SETUP_ETCD_ENDPOINTS", None)
    if args.master_type is None:
        args.master_type = os.environ.get("SETUP_MASTER_TYPE", None)
    if args.fabric_ip is None:
        args.fabric_ip = os.environ.get("SETUP_FABRIC_IP", None)
    if args.ub_device is None or args.ub_device == "":
        args.ub_device = os.environ.get("SETUP_UB_DEVICE", "")
    if args.remote_data_dir is None:
        args.remote_data_dir = os.environ.get("BENCH_REMOTE_DATA_DIR", "/tmp/mooncake_bench/data")
    if args.local_buffer_size is None:
        args.local_buffer_size = parse_size(
            os.environ.get("SETUP_LOCAL_BUFFER_SIZE", "1073741824"))
    if args.global_segment_size is None:
        args.global_segment_size = int(os.environ.get("MC_GLOBAL_SEGMENT_SIZE", "0"))
    if args.tenant_id is None:
        args.tenant_id = os.environ.get("SETUP_TENANT_ID", "default")
    if args.data_seed is None:
        args.data_seed = int(os.environ.get("SETUP_RANDOM_SEED", "42"))
    if args.writer_count is None:
        args.writer_count = int(os.environ.get("BENCH_WRITER_COUNT", "1"))
    if args.reader_count is None:
        args.reader_count = int(os.environ.get("BENCH_READER_COUNT", "1"))
    if args.target_qps is None:
        args.target_qps = float(os.environ.get("BENCH_TARGET_QPS", "0"))
    if args.notify_count is None:
        args.notify_count = int(os.environ.get("BENCH_NOTIFY_COUNT", "1"))
    if args.notify_threads is None:
        args.notify_threads = int(os.environ.get("BENCH_NOTIFY_THREADS", "1"))
    if args.notify_async is None:
        args.notify_async = os.environ.get("BENCH_NOTIFY_ASYNC", "false")
    if isinstance(args.notify_async, str):
        raw = args.notify_async.strip().lower()
        if raw not in ("true", "false", "1", "0", "yes", "no", "on", "off"):
            raise ValueError("BENCH_NOTIFY_ASYNC must be true or false")
        args.notify_async = raw in ("true", "1", "yes", "on")
    if args.notify_max_inflight is None:
        args.notify_max_inflight = int(os.environ.get(
            "BENCH_NOTIFY_MAX_INFLIGHT", "256"))
    if args.notify_max_inflight < 1:
        raise ValueError("notify_max_inflight must be >= 1")
    if args.notify_pipeline is None:
        raw = os.environ.get("BENCH_NOTIFY_PIPELINE", "get")
        args.notify_pipeline = [s.strip() for s in raw.split(",")]
    if args.traffic_mode is None:
        args.traffic_mode = os.environ.get("BENCH_TRAFFIC_MODE", "pulse")
    if args.timeout is None:
        args.timeout = int(os.environ.get("BENCH_TIMEOUT", "300"))
    if args.run_id is None:
        args.run_id = os.environ.get("BENCH_RUN_ID", None)
    if args.writer_operation is None:
        args.writer_operation = os.environ.get("BENCH_WRITER_OPERATION", None)
    if args.reader_operation is None:
        args.reader_operation = os.environ.get("BENCH_READER_OPERATION", None)
    if args.rendezvous_timeout is None:
        args.rendezvous_timeout = int(
            os.environ.get("BENCH_RENDEZVOUS_TIMEOUT", "120"))
def main():
    unbuffer_stdio()
    log("[client-env] pid={} argv={} {}".format(
        os.getpid(), " ".join(sys.argv[1:]), env_snapshot()))
    parser = argparse.ArgumentParser(description="Mooncake benchmark client")
    parser.add_argument("--mode", required=True, choices=["generate-data", "remove-keys", "prepare-store", "benchmark", "pipeline"])
    parser.add_argument("--scope", default="key-prefix", choices=["key-prefix", "full"])
    parser.add_argument("--client-id", type=int, default=1,
                        help="process instance id within this test run "
                             "(basic: 1..num-clients; pipeline writer: global writer id; "
                             "pipeline reader: reader index)")
    parser.add_argument("--key-start", type=int, default=0,
                        help="first key index this process handles (basic benchmark)")
    parser.add_argument("--key-end", type=int, default=None,
                        help="one-past-last key index this process handles "
                             "(basic benchmark; default = key_count, full range)")
    parser.add_argument("--num-threads", type=int, default=1,
                        help="worker threads: basic benchmark concurrency / "
                             "pipeline reader queue consumers")
    parser.add_argument("--warmup", default="false", choices=["true", "false"])
    parser.add_argument("--scenario", default=None)
    parser.add_argument("--work-mode", default=None,
                        help="business type (rank/recall) for pipeline; informational")
    parser.add_argument("--operation", default=None)
    parser.add_argument("--writer-operation", default=None,
                        help="pipeline dual role: writer-side operation "
                             "(env BENCH_WRITER_OPERATION; falls back to "
                             "--operation then put_batch)")
    parser.add_argument("--reader-operation", default=None,
                        help="pipeline dual role: reader-side operation "
                             "(env BENCH_READER_OPERATION; falls back to "
                             "--operation then get)")
    parser.add_argument("--protocol", default=None)
    parser.add_argument("--key-prefix", default=None)
    parser.add_argument("--key-count", type=int, default=None)
    parser.add_argument("--data-size", default=None)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--put-ratio", type=float, default=None)
    parser.add_argument("--duration", type=float, default=None)
    parser.add_argument("--timeout", type=int, default=None,
                        help="pipeline reader lifetime in seconds")
    parser.add_argument("--warmup-duration", type=float, default=None)
    parser.add_argument("--writer-count", type=int, default=None)
    parser.add_argument("--reader-count", type=int, default=None)
    parser.add_argument("--target-qps", type=float, default=None)
    parser.add_argument("--notify-threads", type=int, default=None)
    parser.add_argument("--notify-async", choices=["true", "false"], default=None,
                        help="submit notifications without waiting per write")
    parser.add_argument("--notify-max-inflight", type=int, default=None,
                        help="maximum queued plus running HTTP notifications (default 256)")
    parser.add_argument("--notify-count", type=int, default=None)
    parser.add_argument("--notify-pipeline", default=None)
    parser.add_argument("--traffic-mode", default=None)
    parser.add_argument("--role", default=None,
                        choices=["writer", "reader", "both"])
    parser.add_argument("--listen-port", type=int, default=None,
                        help="pipeline HTTP port (reader: /notify server; "
                             "writer: /start trigger server; required for both)")
    parser.add_argument("--run-id", default=None,
                        help="pipeline rendezvous run id (<ts>-<cfg>); scopes the "
                             "etcd registration prefix (env BENCH_RUN_ID)")
    parser.add_argument("--rendezvous-timeout", type=int, default=None,
                        help="seconds to wait for etcd rendezvous barriers "
                             "(reader discovery / writer /start); env "
                             "BENCH_RENDEZVOUS_TIMEOUT, default 120")
    parser.add_argument("--master-addr", default=None)
    parser.add_argument("--metadata-server", default=None)
    parser.add_argument("--etcd-endpoints", default=None,
                        help="connect to mooncake master via etcd: "
                             "etcd://ip:2379;ip:2379;... (used when --master-type=etcd)")
    parser.add_argument("--master-type", default=None, choices=["http", "etcd"],
                        help="how to connect to the mooncake master (required): "
                             "http (master_addr + metadata_server) or etcd "
                             "(etcd_endpoints)")
    parser.add_argument("--fabric-ip", default=None)
    parser.add_argument("--local-buffer-size", type=int, default=None)
    parser.add_argument("--global-segment-size", type=int, default=None)
    parser.add_argument("--tenant-id", default=None)
    parser.add_argument("--ub-device", default=None,
                        help="ub protocol device name (passed to setup rdma_devices; "
                             "empty for tcp/rdma)")
    parser.add_argument("--data-seed", type=int, default=None)
    parser.add_argument("--remote-data-dir", default=None)
    parser.add_argument("--numa-node", type=int, default=None,
                        help="bind this process to a NUMA node (cpu affinity); "
                             "the node's cpu list is probed locally (sysfs/lscpu)")
    parser.add_argument("--cpu-bind", default=None,
                        help="bind this process to a direct cpu list (e.g. 0-83,90); "
                             "takes precedence over --numa-node")
    parser.add_argument("--cpu-shard", type=int, default=None,
                        help="cores per client (NUMA-aware sharding): take the "
                             "shard-index-th consecutive slice of --numa-node's "
                             "allowed pool; 0/unset = whole node")
    parser.add_argument("--shard-index", type=int, default=0,
                        help="slice ordinal within the NUMA node for --cpu-shard "
                             "(assigned by the runner/orchestrator)")
    args = parser.parse_args()
    # Resolve env.sh fallbacks
    _resolve_env(args)
    # Bind to NUMA node / cpu list before any store work so all measurement
    # threads inherit the affinity
    apply_cpu_binding(args)
    # Parse special args
    if args.notify_pipeline is not None and isinstance(args.notify_pipeline, str):
        args.notify_pipeline = [s.strip() for s in args.notify_pipeline.split(",")]
    elif args.notify_pipeline is None:
        args.notify_pipeline = ["get"]
    args.traffic_mode = str(args.traffic_mode)
    mode = args.mode
    if mode == "generate-data":
        cmd_generate_data(args)
        return
    store = make_store(args)
    try:
        if mode == "remove-keys":
            cmd_remove_keys(args, store)
        elif mode == "prepare-store":
            cmd_prepare_store(args, store)
        elif mode == "benchmark":
            cmd_benchmark(args, store)
        elif mode == "pipeline":
            cmd_pipeline(args, store)
        else:
            raise SystemExit("[ERROR] unknown mode: {}".format(mode))
    finally:
        store.close()
if __name__ == "__main__":
    # ──────────────────────────────────────────────────────────────────────
    # fast-exit wrapper（规避 mooncake store.so 退出阶段的 double-free）
    #
    # 背景/原因：mooncake 的 store.so 在 Python 解释器 shutdown 阶段存在
    # 内存管理 bug——所有业务调用（setup / remove_all / close）都已成功返回
    # success 之后，libc exit() 逆序执行 __cxa_atexit 注册的 C++/Go 静态析构
    # 链（store.so 的 pybind11 对象、libetcd_wrapper.so 的 Go runtime 退出钩子、
    # urma/ub 上下文等跨 .so 双重所有权资源）时，glibc 检测到 double free：
    #   "double free or corruption (fasttop)" → abort() → SIGABRT
    # 进程退出码被污染为 134（128+6），使本已成功的运行（rc 应为 0）被
    # run_benchmark_*.sh 当作失败返回给编排器，进而误判 scenario FAIL。
    #
    # 规避原理：os._exit() 直接发起 exit 系统调用，内核立刻回收进程全部
    # 资源，用户态清理链（atexit 处理器、C++/Go 静态析构、Python 终结化）
    # 一行都不执行——触发崩溃的代码根本没有机会运行。一次性 CLI 进程
    # 不需要这些清理，这是 CI 工具的标准做法。唯一代价是 os._exit 不会
    # 自动 flush stdio，所以必须在调用前手动 flush（见下）。
    #
    # 若 Mooncake 上游修复该 bug：设置环境变量 BENCH_NORMAL_TEARDOWN=1 即可
    # 恢复完整的解释器退出流程（含 teardown），无需改代码、无需重新部署；
    # 或在代码审查时删除本 __main__ 块内 os._exit 分支、恢复 sys.exit(code)。
    # ──────────────────────────────────────────────────────────────────────
    code = 0
    try:
        main()
    except SystemExit as e:
        # 统一收口代码内所有 sys.exit(...)/raise SystemExit(...) 的退出：
        # 业务退出码语义完全不变，仅从此单一出口离开。
        if isinstance(e.code, int):
            code = e.code                 # sys.exit(N) / argparse 的 SystemExit(2) 原样透传
        elif e.code is None:
            code = 0                      # sys.exit() 裸调用 → 0
        else:
            # SystemExit("消息") 的消息默认由解释器打印到 stderr；捕获后
            # 必须自己打印，否则错误信息会静默丢失。
            sys.stderr.write("{}\n".format(e.code))
            code = 1
    except BaseException:
        # 意外异常：还原解释器默认行为（打印 traceback 后再退出），
        # 只是退出码不再会被 teardown 崩溃污染。
        import traceback
        traceback.print_exc()
        code = 1
    try:
        sys.stdout.flush()
        sys.stderr.flush()
    except Exception:
        # os._exit 不刷 stdio 缓冲，这里手动兜底保全 RESULT/SUMMARY 输出；
        # 刷盘失败（如 broken pipe）也不改变退出码，保码优先。
        pass
    if os.environ.get("BENCH_NORMAL_TEARDOWN") != "1":
        os._exit(code)
    sys.exit(code)
