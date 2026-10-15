#!/usr/bin/env python3
"""Remote test process for the Mooncake performance benchmark.
Runs on the server node that hosts the test client (single node, multi client).
Usage:
  python3 mooncake_benchmark_client.py --mode prepare-data|clean|prepare-store|benchmark|pipeline   
      [--scope key-prefix|full] [--num-clients N --client-id ID --num-threads T]   
      [--warmup true|false] [other params...]
Output (stdout, captured by run_benchmark.sh into per-client log files):
  META\t                       run metadata
  RESULT\t\t\t\t   one line per measured op
  SUMMARY\t                    per-client summary
"""
import argparse
import json
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
VALIDATE_BYTES = 64
PREPARE_BATCH_KEYS = 100
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
        raise SystemExit("[ERROR] data not prepared yet, run --mode prepare-data first")
    with open(path, "rb") as f:
        return f.read()
def make_store(args):
    from mooncake.store import MooncakeDistributedStore
    local_hostname = args.fabric_ip
    master_type = args.master_type or ""
    if master_type not in ("http", "etcd"):
        print("[ERROR] master_type not specified: pass --master-type http|etcd "
              "or set MC_MASTER_TYPE")
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

# --------------------------------------------------------------------------

# modes

# --------------------------------------------------------------------------

def cmd_prepare_data(args):
    log("prepare-data start (scenario={}, data_size={}, key_count={})".format(
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
    log("prepare-data done (value={} bytes, keys={})".format(data_size, len(keys)))
    emit("META", {"mode": "prepare-data", "scenario": args.scenario,
                  "key_count": len(keys), "data_size_bytes": data_size,
                  "value_file": vpath, "keys_file": kpath, "seed": args.data_seed})
def cmd_clean(args, store):
    log("clean start (scope={}, scenario={})".format(args.scope, args.scenario))
    keys = build_keys(args.key_prefix, args.scenario, args.key_count)
    scope = args.scope
    if scope == "full":
        ret = store.remove_all(force=True)
        if ret < 0:
            print("[ERROR] clean full failed: remove_all returned {}".format(ret))
            sys.exit(1)
        log("clean done (scope=full, cleared={})".format(max(ret, 0)))
        emit("META", {"mode": "clean", "scope": "full", "cleared": max(ret, 0)})
        return
    existence = store.batch_is_exist(keys)
    statuses = store.batch_remove(keys, force=True)
    if len(existence) != len(keys) or len(statuses) != len(keys):
        print("[ERROR] clean failed: batch_is_exist/batch_remove returned unexpected result size")
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
    emit("META", {"mode": "clean", "scope": "key-prefix",
                  "keys_attempted": len(keys), "keys_removed": removed,
                  "keys_not_found": not_found, "keys_failed": failed})
    log("clean done (scope=key-prefix, removed={}, not_found={}, failed={})".format(
        removed, not_found, failed))
    if failed_samples:
        print("[ERROR] clean failed for {} keys, samples:".format(failed))
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
    for i in range(0, len(missing), PREPARE_BATCH_KEYS):
        chunk = missing[i:i + PREPARE_BATCH_KEYS]
        ret = store.put_batch(chunk, [value] * len(chunk))
        if ret != 0:
            print("[ERROR] prepare-store failed: put_batch returned {}".format(ret))
            sys.exit(1)
        written += len(chunk)
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
PUT_IFACES = ("put", "put_batch", "put_from")
GET_IFACES = ("get", "get_batch", "get_into", "get_buffer")
PUT_BATCH_IFACES = ("put_batch",)
GET_BATCH_IFACES = ("get_batch",)


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


def _put_check(raw, keys, value, ctx):
    return "OK" if raw == 0 else "ERR:{}".format(raw)


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
        for i in range(max(n_put_units, n_get_units)):
            if i < n_put_units:
                group = put_keys[i * batch_size:(i + 1) * batch_size]
                lat, status = _run_iface(store, *PUT_IFACE_DEFS[put_iface], group, value, ctx)
                local.append(("put", group[0], lat, status))
                pace()
            if i < n_get_units:
                base = i * batch_size
                gkeys = [put_keys[(base + k) % n_put] for k in range(batch_size)] if n_put else \
                        get_keys[base:base + batch_size]
                gkeys = gkeys[:n_get - base]
                if gkeys:
                    lat, status = _run_iface(store, *GET_IFACE_DEFS[get_iface], gkeys, value, ctx)
                    local.append(("get", gkeys[0], lat, status))
                    pace()
    elif op in PUT_IFACE_DEFS:
        if op in PUT_BATCH_IFACES:
            for group in groups_iter():
                lat, status = _run_iface(store, *PUT_IFACE_DEFS[op], group, value, ctx)
                local.append((op, group[0], lat, status))
                pace()
        else:
            for k in keys_iter():
                lat, status = _run_iface(store, *PUT_IFACE_DEFS[op], [k], value, ctx)
                local.append((op, k, lat, status))
                pace()
    elif op in GET_IFACE_DEFS:
        if op in GET_BATCH_IFACES:
            for group in groups_iter():
                lat, status = _run_iface(store, *GET_IFACE_DEFS[op], group, value, ctx)
                local.append((op, group[0], lat, status))
                pace()
        else:
            for k in keys_iter():
                lat, status = _run_iface(store, *GET_IFACE_DEFS[op], [k], value, ctx)
                local.append((op, k, lat, status))
                pace()
    else:
        raise SystemExit("[ERROR] unsupported operation: {}".format(op))
    results.extend(local)
def run_warmup(args, store):
    count = args.warmup_key_count
    size = parse_size(args.warmup_data_size or "1MB")
    warm_value = make_value(size, args.data_seed)
    prefix = "mc_warmup*{}*{}".format(args.scenario, os.getpid())
    keys = ["{}*{:08d}".format(prefix, i) for i in range(count)]
    for k in keys:
        ret = store.put(k, warm_value)
        if ret != 0:
            print("[WARN] warmup put failed for {}: {}".format(k, ret))
    for k in keys:
        store.get(k)
    store.batch_remove(keys, force=True)
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
    if duration:
        if op not in GET_IFACES:
            print("[ERROR] duration is only supported for get-family interfaces "
                  "(get/get_batch/get_into/get_buffer), got operation={}".format(op))
            sys.exit(1)
    else:
        duration = None
    # zero-copy interfaces need a registered page-aligned buffer. get_buffer
    # does NOT: it validates through the BufferHandle's own pointer, so it
    # must not register anything (an unnecessary registration is both waste
    # and an extra failure point).
    ctx = None
    need_zero = (op in ("put_from", "get_into")
                 or put_iface in ("put_from",)
                 or get_iface in ("get_into",))
    if need_zero:
        ctx = _BufferCtx(data_size)
        if not ctx.setup(store):
            print("[ERROR] register_buffer failed, cannot use zero-copy interfaces")
            sys.exit(1)
    if args.warmup == "true":
        run_warmup(args, store)
    end_idx = args.key_end if args.key_end is not None else len(keys)
    chunk = keys[args.key_start:end_idx]
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
    try:
        if chunk:
            per_thread = []
            for t in range(args.num_threads):
                s = t * len(chunk) // args.num_threads
                e = (t + 1) * len(chunk) // args.num_threads
                per_thread.append(chunk[s:e])
            with ThreadPoolExecutor(max_workers=args.num_threads) as pool:
                futures = [pool.submit(_run_thread, args, store, sub, value, results, deadline, ctx)
                           for sub in per_thread]
                for f in futures:
                    f.result()
    finally:
        if ctx is not None:
            ctx.teardown(store)
    wall_s = time.perf_counter() - wall_t0
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
    for opname, k, lat, status in results:
        sys.stdout.write("RESULT\t{}\t{}\t{:.6f}\t{}\n".format(opname, k, lat, status))
    sys.stdout.flush()

# --------------------------------------------------------------------------

# pipeline (精排/召排) mode — independent process writer/reader via HTTP notify

# --------------------------------------------------------------------------

import http.server
import socketserver
import urllib.parse
import threading
from queue import Queue
def _pipeline_writer(args, store, do_warmup=True):
    """Writer process: write keys at target_qps, notify a random subset of readers.

    Optionally runs a warmup phase first: the SAME deployment shape (same
    pacing, same notify path) for warmup_duration seconds to activate the
    writer->store, writer->reader and reader->store connections. Warmup
    writes carry a warmup=1 flag so readers process them without recording.
    """
    value = load_value(args.remote_data_dir, args.scenario)
    batch_keys = max(1, args.batch_size)
    prefix = args.key_prefix
    scenario = args.scenario
    writer_id = args.client_id
    writer_op = args.operation or "put_batch"
    if writer_op not in PUT_IFACE_DEFS:
        raise SystemExit("[ERROR] unsupported writer operation: {}".format(writer_op))
    ctx = None
    if writer_op == "put_from":
        ctx = _BufferCtx(len(value))
        if not ctx.setup(store):
            raise SystemExit("[ERROR] writer register_buffer failed")
    duration = float(args.duration)
    warmup_duration = float(getattr(args, "warmup_duration", 0) or 0)
    target_qps = float(args.target_qps) if args.target_qps > 0 else 0
    traffic_mode = args.traffic_mode
    peers = json.loads(args.peers) if isinstance(args.peers, str) else args.peers
    readers = [p for p in peers if p.get("role") == "reader"]
    # Auto-resolve reader host to 127.0.0.1 if they are on the same machine
    import socket
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
    notify_count = int(getattr(args, "notify_count", 0) or 0)
    notify_threads = max(1, int(getattr(args, "notify_threads", 1) or 1))
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
                  "num_readers": len(readers), "notify_count": notify_count,
                  "notify_threads": notify_threads,
                  "writer_count": args.writer_count, "reader_count": args.reader_count})
    import urllib.request
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    from concurrent.futures import ThreadPoolExecutor
    notify_pool = ThreadPoolExecutor(max_workers=notify_threads)
    seq = [0]
    # measured-phase stats (warmup writes stay out of the metrics)
    stats = {"keys_ok": 0, "keys_fail": 0, "batches_ok": 0, "batches_fail": 0, "lats": []}
    last_flush = [time.time()]

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
            keys = ["{}*{}*w{}*{:08d}".format(prefix, scenario, writer_id, seq[0] + i)
                    for i in range(batch_keys)]
            seq[0] += batch_keys
            # interface dispatch: put_batch = one call per whole batch;
            # put/put_from = one timed call per key (key_count=1 each)
            write_samples = []
            if writer_op in PUT_BATCH_IFACES:
                lat_ms, status = _run_iface(store, *PUT_IFACE_DEFS[writer_op], keys, value, ctx)
                write_samples.append((keys[0], lat_ms, status, batch_keys))
            else:
                for k in keys:
                    lat_ms, status = _run_iface(store, *PUT_IFACE_DEFS[writer_op], [k], value, ctx)
                    write_samples.append((k, lat_ms, status, 1))
            batch_ok = all(s == "OK" for _, _, s, _ in write_samples)
            if not is_warmup:
                for k, lat_ms, status, kc in write_samples:
                    if status == "OK":
                        stats["keys_ok"] += kc
                        stats["batches_ok"] += 1
                        stats["lats"].append(lat_ms)
                    else:
                        stats["keys_fail"] += kc
                        stats["batches_fail"] += 1
                    sys.stdout.write("RESULT\tpipeline_write\t{}\t{:.6f}\t{}\t{}\n".format(
                        k, lat_ms, status, kc))
            if batch_ok:
                # Randomly select notify_count readers and deliver the whole
                # batch in a single multi-key request per reader (bypass proxy;
                # write_ts is retained for diagnostics only — reader latency is
                # measured locally on the reader node with time.perf_counter();
                # warmup flag lets readers warm up without recording).
                n_sel = min(notify_count, len(readers)) if notify_count > 0 else len(readers)
                selected = random.sample(readers, n_sel) if readers else []
                write_ts = time.time()
                wflag = "1" if is_warmup else "0"
                def _notify(r, ks=keys, ts=write_ts, wf=wflag):
                    query = "&".join("key=" + k for k in ks)
                    url = "http://{}:{}/notify?{}&writer_id={}&write_ts={}&warmup={}".format(
                        r["host"], r["port"], query, writer_id, ts, wf)
                    try:
                        opener.open(url, timeout=0.5)
                    except Exception as exc:
                        sys.stdout.write("RESULT\tpipeline_notify\t{}\t0\tERR:{} target={}:{}\n".format(
                            ks[0], exc, r["host"], r["port"]))
                # blocking submit = natural backpressure; pacing accounts for
                # the slowest in-flight notify instead of the sequential sum
                futures = [notify_pool.submit(_notify, r) for r in selected]
                for f in futures:
                    f.result()
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

    if do_warmup and warmup_duration > 0:
        log("pipeline writer warmup start (duration={}s, target_qps={})".format(
            warmup_duration, target_qps))
        emit("META", {"mode": "pipeline_warmup_start", "writer_id": writer_id,
                      "duration": warmup_duration, "ts": time.time()})
        _run_phase(time.time() + warmup_duration, True)
        emit("META", {"mode": "pipeline_warmup_end", "writer_id": writer_id,
                      "ts": time.time()})
        log("pipeline writer warmup done")
    log("pipeline writer measured start (duration={}s)".format(duration))
    _run_phase(time.time() + duration, False)
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
        # interface throughput = successful bytes / sum of successful
        # put_batch call durations (busy time, not wall clock)
        "throughput_mb_s": (ok_bytes / busy_s / 1024.0 ** 2) if busy_s > 0 else 0.0,
        "throughput_gb_s": (ok_bytes / busy_s / 1024.0 ** 3) if busy_s > 0 else 0.0,
        "min_ms": _percentile(sorted(lats), 0.0),
        "max_ms": _percentile(sorted(lats), 1.0),
        "avg_ms": (sum(lats) / len(lats)) if lats else 0.0,
        "p99_ms": _percentile(sorted(lats), 0.99),
        "p999_ms": _percentile(sorted(lats), 0.999),
        "data_size": len(value),
    })
    sys.stdout.flush()
class _PipelineNotifyHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler for reader: receives write notifications from writers."""
    # Class-level shared state set by _pipeline_reader
    queue = None
    deadline = 0.0
    exit_grace = 5.0
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
                    local.append(("pipeline_warmup_read", keys[0], 0.0, "SKIP", queue_wait_ms, len(keys)))
                    continue
                local.append(("pipeline_read", keys[0], 0.0, "MISS/MISMATCH", queue_wait_ms, len(keys)))
                continue
        if batch_mode:
            lat_ms, status = _run_iface(store, *GET_IFACE_DEFS[reader_op], keys, value, ctx)
            if is_warmup:
                local.append(("pipeline_warmup_read", keys[0], lat_ms, "SKIP", queue_wait_ms, len(keys)))
                continue
            local.append(("pipeline_read", keys[0], lat_ms, status, queue_wait_ms, len(keys)))
        else:
            for k in keys:
                lat_ms, status = _run_iface(store, *GET_IFACE_DEFS[reader_op], [k], value, ctx)
                if is_warmup:
                    local.append(("pipeline_warmup_read", k, lat_ms, "SKIP", queue_wait_ms, 1))
                    continue
                local.append(("pipeline_read", k, lat_ms, status, queue_wait_ms, 1))
    results.extend(local)
def cmd_pipeline(args, store):
    """Run pipeline (精排/召排) — dispatches to writer or reader based on --role."""
    role = args.role
    if role == "writer":
        if not args.peers:
            raise SystemExit("[ERROR] pipeline writer requires --peers "
                             "(reader host/port list)")
        try:
            peer_list = json.loads(args.peers) if isinstance(args.peers, str) else args.peers
        except ValueError:
            raise SystemExit("[ERROR] --peers is not valid JSON: {}".format(args.peers))
        readers = [p for p in peer_list if p.get("role") == "reader"]
        if not readers:
            raise SystemExit("[ERROR] --peers contains no reader entries")
        # warmup is a short same-deployment run handled inside the writer
        _pipeline_writer(args, store, do_warmup=(args.warmup == "true"))
    elif role == "reader":
        if args.listen_port is None:
            raise SystemExit("[ERROR] pipeline reader requires --listen-port")
        _pipeline_reader(args, store)
    else:
        raise SystemExit("[ERROR] pipeline mode requires --role writer|reader")

# --------------------------------------------------------------------------

def _pipeline_reader(args, store):
    """Reader process: HTTP server to receive notify, worker threads to read store."""
    lifetime = float(args.timeout)
    # measurement window = configured duration (MC_DURATION); fall back to
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
        "queue_wait_avg_ms": (sum(queue_waits) / len(queue_waits)) if queue_waits else 0.0,
        "queue_wait_max_ms": max(queue_waits) if queue_waits else 0.0,
        "max_depth": _PipelineNotifyHandler.qmax,
        "data_size": data_size,
    }
    emit("SUMMARY", summary)
    for opname, k, lat, status, qw, kc in measured:
        sys.stdout.write("RESULT\t{}\t{}\t{:.6f}\t{}\t{:.6f}\t{}\n".format(
            opname, k, lat, status, qw, kc))
    sys.stdout.flush()

# --------------------------------------------------------------------------

def _resolve_env(args):
    """After parsing CLI args, fall back to env.sh vars for any unset params."""
    if args.protocol is None:
        args.protocol = os.environ.get("MC_PROTOCOL", "tcp")
    if args.scenario is None:
        args.scenario = os.environ.get("MC_SCENARIO", None)
    if args.work_mode is None:
        args.work_mode = os.environ.get("MC_WORK_MODE", None)
    if args.operation is None:
        args.operation = os.environ.get("MC_OPERATION", None)
    if args.key_prefix is None:
        args.key_prefix = os.environ.get("MC_KEY_PREFIX", "mc_bench")
    if args.key_count is None:
        args.key_count = int(os.environ.get("MC_KEY_COUNT", "1000"))
    if args.data_size is None:
        args.data_size = os.environ.get("MC_DATA_SIZE", "8MB")
    if args.batch_size is None:
        args.batch_size = int(os.environ.get("MC_BATCH_SIZE", "1"))
    if args.put_ratio is None:
        args.put_ratio = float(os.environ.get("MC_PUT_RATIO", "0.5"))
    if args.duration is None:
        dur = os.environ.get("MC_DURATION", None)
        if dur is not None:
            args.duration = float(dur)
        else:
            args.duration = 0.0
    if args.warmup_key_count is None:
        args.warmup_key_count = int(os.environ.get("MC_WARMUP_KEY_COUNT", "100"))
    if args.warmup_data_size is None:
        args.warmup_data_size = os.environ.get("MC_WARMUP_DATA_SIZE", None)
    if args.warmup_duration is None:
        args.warmup_duration = float(os.environ.get("MC_WARMUP_DURATION", "0"))
    if args.master_addr is None:
        args.master_addr = os.environ.get("MC_MASTER_ADDR", None)
    if args.metadata_server is None:
        args.metadata_server = os.environ.get("MC_METADATA_SERVER", None)
    if args.etcd_endpoints is None:
        args.etcd_endpoints = os.environ.get("MC_ETCD_ENDPOINTS", None)
    if args.master_type is None:
        args.master_type = os.environ.get("MC_MASTER_TYPE", None)
    if args.fabric_ip is None:
        args.fabric_ip = os.environ.get("MC_FABRIC_IP", None)
    if args.ub_device is None or args.ub_device == "":
        args.ub_device = os.environ.get("MC_UB_DEVICE", "")
    if args.remote_data_dir is None:
        args.remote_data_dir = os.environ.get("MC_REMOTE_DATA_DIR", "/tmp/mooncake_bench/data")
    if args.local_buffer_size is None:
        args.local_buffer_size = int(os.environ.get("MC_LOCAL_BUFFER_SIZE", "1073741824"))
    if args.global_segment_size is None:
        args.global_segment_size = int(os.environ.get("MC_GLOBAL_SEGMENT_SIZE", "0"))
    if args.tenant_id is None:
        args.tenant_id = os.environ.get("MC_TENANT_ID", "default")
    if args.data_seed is None:
        args.data_seed = int(os.environ.get("MC_RANDOM_SEED", "42"))
    if args.writer_count is None:
        args.writer_count = int(os.environ.get("MC_WRITER_COUNT", "1"))
    if args.reader_count is None:
        args.reader_count = int(os.environ.get("MC_READER_COUNT", "1"))
    if args.target_qps is None:
        args.target_qps = float(os.environ.get("MC_TARGET_QPS", "0"))
    if args.notify_count is None:
        args.notify_count = int(os.environ.get("MC_NOTIFY_COUNT", "1"))
    if args.notify_threads is None:
        args.notify_threads = int(os.environ.get("MC_NOTIFY_THREADS", "1"))
    if args.notify_pipeline is None:
        raw = os.environ.get("MC_NOTIFY_PIPELINE", "get")
        args.notify_pipeline = [s.strip() for s in raw.split(",")]
    if args.traffic_mode is None:
        args.traffic_mode = os.environ.get("MC_TRAFFIC_MODE", "pulse")
    if args.timeout is None:
        args.timeout = int(os.environ.get("MC_TIMEOUT", "300"))
def main():
    unbuffer_stdio()
    parser = argparse.ArgumentParser(description="Mooncake benchmark client")
    parser.add_argument("--mode", required=True, choices=["prepare-data", "clean", "prepare-store", "benchmark", "pipeline"])
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
    parser.add_argument("--protocol", default=None)
    parser.add_argument("--key-prefix", default=None)
    parser.add_argument("--key-count", type=int, default=None)
    parser.add_argument("--data-size", default=None)
    parser.add_argument("--batch-size", type=int, default=None)
    parser.add_argument("--put-ratio", type=float, default=None)
    parser.add_argument("--duration", type=float, default=None)
    parser.add_argument("--timeout", type=int, default=None,
                        help="pipeline reader lifetime in seconds")
    parser.add_argument("--warmup-key-count", type=int, default=None)
    parser.add_argument("--warmup-data-size", default=None)
    parser.add_argument("--warmup-duration", type=float, default=None)
    parser.add_argument("--writer-count", type=int, default=None)
    parser.add_argument("--reader-count", type=int, default=None)
    parser.add_argument("--target-qps", type=float, default=None)
    parser.add_argument("--notify-threads", type=int, default=None)
    parser.add_argument("--notify-count", type=int, default=None)
    parser.add_argument("--notify-pipeline", default=None)
    parser.add_argument("--traffic-mode", default=None)
    parser.add_argument("--role", default=None, choices=["writer", "reader"])
    parser.add_argument("--listen-port", type=int, default=None,
                        help="pipeline reader HTTP notify port (required for reader)")
    parser.add_argument("--peers", default=None,
                        help="pipeline writer reader list JSON (required for writer)")
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
    args = parser.parse_args()
    # Resolve env.sh fallbacks
    _resolve_env(args)
    # Resolve peers from JSON string
    if isinstance(args.peers, str):
        args.peers = json.loads(args.peers)
    # Parse special args
    if args.notify_pipeline is not None and isinstance(args.notify_pipeline, str):
        args.notify_pipeline = [s.strip() for s in args.notify_pipeline.split(",")]
    elif args.notify_pipeline is None:
        args.notify_pipeline = ["get"]
    args.traffic_mode = str(args.traffic_mode)
    mode = args.mode
    if mode == "prepare-data":
        cmd_prepare_data(args)
        return
    store = make_store(args)
    try:
        if mode == "clean":
            cmd_clean(args, store)
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
    # run_benchmark.sh 当作失败返回给编排器，进而误判 scenario FAIL。
    #
    # 规避原理：os._exit() 直接发起 exit 系统调用，内核立刻回收进程全部
    # 资源，用户态清理链（atexit 处理器、C++/Go 静态析构、Python 终结化）
    # 一行都不执行——触发崩溃的代码根本没有机会运行。一次性 CLI 进程
    # 不需要这些清理，这是 CI 工具的标准做法。唯一代价是 os._exit 不会
    # 自动 flush stdio，所以必须在调用前手动 flush（见下）。
    #
    # 若 Mooncake 上游修复该 bug：设置环境变量 MC_NORMAL_TEARDOWN=1 即可
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
    if os.environ.get("MC_NORMAL_TEARDOWN") != "1":
        os._exit(code)
    sys.exit(code)