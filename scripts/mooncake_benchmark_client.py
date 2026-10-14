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
    metadata_server = args.metadata_server or "http://{}/metadata".format(args.master_addr)
    store = MooncakeDistributedStore()
    ret = store.setup(
        local_hostname=local_hostname,
        metadata_server=metadata_server,
        global_segment_size=args.global_segment_size,
        local_buffer_size=args.local_buffer_size,
        protocol=args.protocol,
        rdma_devices=args.ub_device,
        master_server_addr=args.master_addr,
        tenant_id=args.tenant_id,
    )
    if ret != 0:
        print("[ERROR] store.setup failed, ret={}".format(ret))
        sys.exit(1)
    return store
def emit(kind, payload):
    if isinstance(payload, dict):
        payload = json.dumps(payload)
    sys.stdout.write("{}\t{}\n".format(kind, payload))
    sys.stdout.flush()

# --------------------------------------------------------------------------

# modes

# --------------------------------------------------------------------------

def cmd_prepare_data(args):
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
    emit("META", {"mode": "prepare-data", "scenario": args.scenario,
                  "key_count": len(keys), "data_size_bytes": data_size,
                  "value_file": vpath, "keys_file": kpath, "seed": args.data_seed})
def cmd_clean(args, store):
    keys = build_keys(args.key_prefix, args.scenario, args.key_count)
    scope = args.scope
    if scope == "full":
        ret = store.remove_all(force=True)
        if ret < 0:
            print("[ERROR] clean full failed: remove_all returned {}".format(ret))
            sys.exit(1)
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
    if failed_samples:
        print("[ERROR] clean failed for {} keys, samples:".format(failed))
        for s in failed_samples:
            print("  " + s)
    if failed > 0:
        sys.exit(1)
def cmd_prepare_store(args, store):
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

# --------------------------------------------------------------------------

# operations

# --------------------------------------------------------------------------

def _put(store, key, value):
    ret = store.put(key, value)
    return "OK" if ret == 0 else "ERR:{}".format(ret)
def _get(store, key, value):
    data = store.get(key)
    if not data:
        return "MISS"
    if len(data) != len(value) or data[:VALIDATE_BYTES] != value[:VALIDATE_BYTES]:
        return "MISMATCH"
    return "OK"
def _timed(fn):
    t0 = time.perf_counter()
    try:
        status = fn()
    except Exception as exc:
        status = "ERR:{}".format(type(exc).__name__)
    lat_ms = (time.perf_counter() - t0) * 1000.0
    return lat_ms, status
def _run_thread(args, store, keys, value, results, deadline=None):
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
    if op == "put":
        for k in keys_iter():
            lat, status = _timed(lambda k=k: _put(store, k, value))
            local.append(("put", k, lat, status))
    elif op == "get":
        for k in keys_iter():
            lat, status = _timed(lambda k=k: _get(store, k, value))
            local.append(("get", k, lat, status))
    elif op == "put_batch":
        for group in groups_iter():
            values = [value] * len(group)
            def do_batch_put(g=group, v=values):
                ret = store.put_batch(g, v)
                return "OK" if ret == 0 else "ERR:{}".format(ret)
            lat, status = _timed(do_batch_put)
            local.append(("put_batch", group[0], lat, status))
    elif op == "get_batch":
        for group in groups_iter():
            def do_batch_get(g=group):
                data_list = store.get_batch(g)
                if len(data_list) != len(g):
                    return "ERR:batch-size"
                for d in data_list:
                    if not d:
                        return "MISS"
                    if len(d) != len(value) or d[:VALIDATE_BYTES] != value[:VALIDATE_BYTES]:
                        return "MISMATCH"
                return "OK"
            lat, status = _timed(do_batch_get)
            local.append(("get_batch", group[0], lat, status))
    elif op == "mixed" or "/" in op and "put" in op and "get" in op:
        put_count = int(len(keys) * put_ratio)
        # First, put all the put keys
        put_keys = keys[:put_count]
        for k in put_keys:
            lat, status = _timed(lambda k=k: _put(store, k, value))
            local.append(("put", k, lat, status))
        # Then, get from the same key pool, cycling if needed
        get_keys = keys[put_count:]
        for i, k in enumerate(get_keys):
            gk = put_keys[i % len(put_keys)] if put_keys else k
            lat, status = _timed(lambda k=gk: _get(store, k, value))
            local.append(("get", gk, lat, status))
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
    if duration:
        if op not in ("get", "get_batch"):
            print("[ERROR] duration is only supported for get/get_batch, "
                  "got operation={}".format(op))
            sys.exit(1)
    else:
        duration = None
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
                  "chunk_keys": len(chunk), "start_ts": time.time()})
    results = []
    wall_t0 = time.perf_counter()
    if chunk:
        per_thread = []
        for t in range(args.num_threads):
            s = t * len(chunk) // args.num_threads
            e = (t + 1) * len(chunk) // args.num_threads
            per_thread.append(chunk[s:e])
        with ThreadPoolExecutor(max_workers=args.num_threads) as pool:
            futures = [pool.submit(_run_thread, args, store, sub, value, results, deadline)
                       for sub in per_thread]
            for f in futures:
                f.result()
    wall_s = time.perf_counter() - wall_t0
    batch_size = max(1, args.batch_size)
    if op in ("put_batch", "get_batch"):
        bytes_per_op = batch_size * data_size
    else:
        bytes_per_op = data_size
    total_bytes = len(results) * bytes_per_op
    ok_count = sum(1 for r in results if r[3] == "OK")
    fail_count = len(results) - ok_count
    total = len(results)
    latencies = sorted(r[2] for r in results)
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
        "avg_ms": (sum(r[2] for r in results) / total) if total else 0.0,
        "p99_ms": _percentile(latencies, 0.99),
        "p999_ms": _percentile(latencies, 0.999),
        "data_size": data_size,
        "key_count": len(keys),
    }
    if op in ("put_batch", "get_batch"):
        summary["batch_size"] = batch_size
    if op == "mixed" or ("/" in op and "put" in op and "get" in op):
        summary["put_ratio"] = args.put_ratio
    if duration:
        summary["duration"] = duration
    emit("SUMMARY", summary)
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
            t0 = time.perf_counter()
            ret = store.put_batch(keys, [value] * len(keys))
            lat_ms = (time.perf_counter() - t0) * 1000.0
            if ret < 0:
                if not is_warmup:
                    sys.stdout.write("RESULT\tpipeline_write\t{}\t{:.6f}\tERR:{}\n".format(keys[0], lat_ms, ret))
            else:
                if not is_warmup:
                    sys.stdout.write("RESULT\tpipeline_write\t{}\t{:.6f}\tOK\n".format(keys[0], lat_ms))
                # Randomly select notify_count readers and deliver the whole
                # batch in a single multi-key request per reader (bypass proxy;
                # write_ts is wall-clock so reader latency is comparable across
                # nodes; warmup flag lets readers warm up without recording).
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
        emit("META", {"mode": "pipeline_warmup_start", "writer_id": writer_id,
                      "duration": warmup_duration, "ts": time.time()})
        _run_phase(time.time() + warmup_duration, True)
        emit("META", {"mode": "pipeline_warmup_end", "writer_id": writer_id,
                      "ts": time.time()})
    _run_phase(time.time() + duration, False)
    sys.stdout.flush()
class _PipelineNotifyHandler(http.server.BaseHTTPRequestHandler):
    """HTTP handler for reader: receives write notifications from writers."""
    # Class-level shared state set by _pipeline_reader
    queue = None
    deadline = 0.0
    exit_grace = 5.0
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
            for k in keys:
                if time.time() < _PipelineNotifyHandler.deadline + _PipelineNotifyHandler.exit_grace:
                    _PipelineNotifyHandler.queue.put((k, int(writer_id), write_ts, is_warmup))
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
def _reader_worker(args, store, queue, deadline, results):
    """Reader worker thread: dequeue keys, execute notify_pipeline, record latency.

    Warmup items (warmup=1 flag from the notify) are still processed — the
    actual store reads warm up the reader->store path — but are tagged so
    they stay out of the measured metrics.
    """
    pipeline = args.notify_pipeline if isinstance(args.notify_pipeline, list) else ["get"]
    value = load_value(args.remote_data_dir, args.scenario)
    local = []
    while time.time() < deadline:
        try:
            item = queue.get(timeout=1.0)
        except:
            continue
        if item is None:
            break
        key, writer_id, write_ts, is_warmup = item
        ok = True
        for step in pipeline:
            step = step.strip()
            if step == "exist":
                ex = store.is_exist(key)
                if ex <= 0:
                    ok = False
                    break
            elif step == "get" or step == "getBuffer":
                data = store.get(key)
                if not data:
                    ok = False
                    break
                if len(data) != len(value) or data[:VALIDATE_BYTES] != value[:VALIDATE_BYTES]:
                    ok = False
                    break
            else:
                pass
        if is_warmup:
            local.append(("pipeline_warmup_read", key, 0.0, "SKIP"))
            continue
        lat_ms = (time.time() - write_ts) * 1000.0
        if ok:
            local.append(("pipeline_read", key, lat_ms, "OK"))
        else:
            local.append(("pipeline_read", key, lat_ms, "MISS/MISMATCH"))
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
    duration = float(args.timeout)
    exit_grace = 5.0
    num_threads = max(1, args.num_threads)
    my_port = args.listen_port
    my_port = args.listen_port
    emit("META", {"mode": "pipeline_reader", "scenario": args.scenario,
                  "work_mode": args.work_mode or "recall", "protocol": args.protocol,
                  "client_id": args.client_id, "reader_count": args.reader_count,
                  "listen_port": my_port, "num_threads": num_threads,
                  "notify_pipeline": args.notify_pipeline,
                  "duration": duration, "start_ts": time.time()})
    queue = Queue(maxsize=max(8192, num_threads * 1024))
    deadline = time.time() + duration
    results = []
    # Set class-level shared state for the HTTP handler
    _PipelineNotifyHandler.queue = queue
    _PipelineNotifyHandler.deadline = deadline
    _PipelineNotifyHandler.exit_grace = exit_grace
    # Start worker threads
    with ThreadPoolExecutor(max_workers=num_threads) as pool:
        worker_futures = [pool.submit(_reader_worker, args, store, queue, deadline + exit_grace, results)
                         for _ in range(num_threads)]
        # Start HTTP server in a daemon thread — set allow_reuse_address
        # BEFORE instantiation so server_bind() picks it up
        socketserver.ThreadingTCPServer.allow_reuse_address = True
        server = socketserver.ThreadingTCPServer(("0.0.0.0", my_port), _PipelineNotifyHandler)
        server.daemon_threads = True
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        emit("META", {"mode": "pipeline_reader_ready", "port": my_port, "ts": time.time()})
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
    wall_s = duration
    data_size = parse_size(args.data_size)
    # split off warmup items: processed (store reads warmed up) but not
    # part of the measured metrics
    warmup_total = sum(1 for r in results if r[0] == "pipeline_warmup_read")
    measured = [r for r in results if r[0] != "pipeline_warmup_read"]
    total_bytes = len(measured) * data_size
    ok_count = sum(1 for r in measured if r[3] == "OK")
    fail_count = len(measured) - ok_count
    total = len(measured)
    latencies = sorted(r[2] for r in measured)
    summary = {
        "mode": "pipeline_reader", "scenario": args.scenario,
        "work_mode": args.work_mode or "recall", "protocol": args.protocol,
        "client_id": args.client_id, "reader_count": args.reader_count,
        "num_threads": num_threads,
        "total": total, "bytes": total_bytes, "wall_s": wall_s,
        "warmup_total": warmup_total,
        "ok": ok_count, "fail": fail_count,
        "qps": (total / wall_s) if wall_s > 0 else 0.0,
        "fail_rate": (fail_count / total) if total else 0.0,
        "min_ms": _percentile(latencies, 0.0),
        "max_ms": _percentile(latencies, 1.0),
        "avg_ms": (sum(r[2] for r in measured) / total) if total else 0.0,
        "p99_ms": _percentile(latencies, 0.99),
        "p999_ms": _percentile(latencies, 0.999),
        "data_size": data_size,
    }
    emit("SUMMARY", summary)
    for opname, k, lat, status in measured:
        sys.stdout.write("RESULT\t{}\t{}\t{:.6f}\t{}\n".format(opname, k, lat, status))
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
        args.target_qps = float(os.environ.get("MC_TARGET_QPS", "200"))
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
    main()