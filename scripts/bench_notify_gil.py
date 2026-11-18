#!/usr/bin/env python3
"""Micro-benchmark for the pipeline notify hot path — no cluster required.

The production pipeline writer builds a notify URL per (batch, reader) and
sends it over a pooled HTTP connection, while the reader side runs an HTTP
server that parses the query, enqueues the batch and replies.  Both ends are
pure Python, so the measured notify rate is bounded by GIL scheduling as much
as by the network.  This script exercises exactly those two code paths
locally (127.0.0.1) so the cost of a change can be compared before/after
without touching a real cluster.

It is a measurement tool, not a test: it prints a report and exits non-zero
only when the run itself failed (e.g. the reader server could not bind).

Usage:
    python3 scripts/bench_notify_gil.py --server legacy --threads 8
    python3 scripts/bench_notify_gil.py --server fast   --threads 8
    python3 scripts/bench_notify_gil.py --switch-interval 0.0005 \
        --server fast --threads 8 --qps 20000 --duration 10
"""
import argparse
import http.server
import os
import sys
import threading
import time
import urllib.parse
from queue import Empty, Queue

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
if HERE not in sys.path:
    sys.path.insert(0, HERE)

import mooncake_benchmark_client as client  # noqa: E402


def _parse_args():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--server", choices=("legacy", "fast"), default="legacy",
                    help="reader HTTP implementation under test")
    ap.add_argument("--threads", type=int, default=4,
                    help="writer notify worker threads")
    ap.add_argument("--qps", type=float, default=0,
                    help="target notify batches/s (0 = as fast as possible)")
    ap.add_argument("--duration", type=float, default=5.0)
    ap.add_argument("--readers", type=int, default=1,
                    help="distinct reader endpoints to rotate over")
    ap.add_argument("--notify-count", type=int, default=1,
                    help="readers notified per batch (round-robin)")
    ap.add_argument("--keys", type=int, default=1,
                    help="keys per notify batch")
    ap.add_argument("--queue-size", type=int, default=1024)
    ap.add_argument("--switch-interval", type=float, default=0.0,
                    help="call sys.setswitchinterval() before measuring "
                         "(0 = leave the interpreter default)")
    ap.add_argument("--drain-delay-us", type=float, default=0.0,
                    help="simulated per-notify reader work before dequeue")
    return ap.parse_args()


def _start_reader(legacy, drain_delay_us):
    """Bind a reader server on an ephemeral port and drain its queue.

    Returns (server, port, stop_event, drain_thread)."""
    queue = Queue(maxsize=1 << 20)
    client._PipelineNotifyHandler.queue = queue
    client._PipelineNotifyHandler.deadline = time.time() + 3600.0
    client._PipelineNotifyHandler.exit_grace = 5.0
    client._PipelineNotifyHandler.start_event = None
    client._PipelineNotifyHandler.qdepth = 0
    client._PipelineNotifyHandler.qmax = 0
    client._PipelineNotifyHandler.qlock = threading.Lock()

    if legacy:
        server = http.server.ThreadingHTTPServer(
            ("127.0.0.1", 0), client._PipelineNotifyHandler)
        server.daemon_threads = True
    else:
        server = client._FastNotifyServer(
            ("127.0.0.1", 0), client._FastNotifyHandler)
        server.daemon_threads = True
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    stop = threading.Event()
    delay = max(0.0, drain_delay_us) / 1e6

    def drain():
        while not stop.is_set():
            try:
                queue.get(timeout=0.2)
            except Empty:
                continue
            if delay:
                time.sleep(delay)

    thread = threading.Thread(target=drain, daemon=True)
    thread.start()
    return server, port, stop, thread


def main():
    args = _parse_args()
    if args.switch_interval > 0:
        old = sys.getswitchinterval()
        sys.setswitchinterval(args.switch_interval)
        print("switch interval: {} -> {}".format(old, args.switch_interval))

    server, port, stop, drain = _start_reader(args.server == "legacy",
                                              args.drain_delay_us)
    readers = [{"host": "127.0.0.1", "port": port} for _ in range(args.readers)]
    keys = ["key-{}".format(i) for i in range(max(1, args.keys))]
    pool = client._NotifyHTTPPool()
    results = []

    def send(reader, batch_keys, write_ts, wflag):
        query = urllib.parse.urlencode(
            [("key", k) for k in batch_keys] + [("writer_id", "0"),
                                                ("write_ts", write_ts),
                                                ("warmup", wflag)])
        started = time.perf_counter()
        try:
            pool.send(reader, "/notify?" + query)
            ok = True
        except Exception:
            ok = False
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        results.append((ok, elapsed_ms))
        return (wflag == "1", ok, elapsed_ms)

    sender = client._NotifySender(send, max(1, args.threads),
                                  max(1, args.queue_size))
    submitted = 0
    n_sel = max(1, min(args.notify_count, len(readers)))
    interval = (1.0 / args.qps) if args.qps > 0 else 0.0
    next_t = time.perf_counter()
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    deadline = wall_start + args.duration
    try:
        while time.perf_counter() < deadline:
            selected = [readers[(submitted + i) % len(readers)]
                        for i in range(n_sel)]
            for reader in selected:
                sender.submit(reader, keys, str(time.time()), "0")
            submitted += 1
            if interval:
                next_t += interval
                sleep_t = next_t - time.perf_counter()
                if sleep_t > 0:
                    time.sleep(sleep_t)
        sender.drain()
        wall = time.perf_counter() - wall_start
        cpu = time.process_time() - cpu_start
    finally:
        sender.close()
        pool.close()
        stop.set()
        server.shutdown()

    ok = sum(1 for r in results if r[0])
    fail = len(results) - ok
    sent = ok + fail
    lat = sorted(r[1] for r in results)
    stats = sender.stats

    def pct(q):
        if not lat:
            return 0.0
        return lat[min(len(lat) - 1, int(round(q * (len(lat) - 1))))]

    print("\nserver={} threads={} readers={} notify_count={} keys={}".format(
        args.server, args.threads, len(readers), n_sel, len(keys)))
    print("batches submitted : {}".format(submitted))
    print("notifies sent     : {} ok={} fail={} ({:.1f}/s)".format(
        sent, ok, fail, sent / wall if wall else 0.0))
    print("notify latency ms : avg={:.3f} p50={:.3f} p99={:.3f} max={:.3f}".format(
        (sum(lat) / len(lat)) if lat else 0.0, pct(0.50), pct(0.99),
        lat[-1] if lat else 0.0))
    print("queue wait ms     : avg={:.3f} max={:.3f} peak={}".format(
        stats["queue_wait_ms_sum"] / max(1, sent), stats["queue_wait_ms_max"],
        stats["queue_peak"]))
    print("connections       : created={} reused={}".format(
        pool.created, pool.reused))
    print("wall/cpu          : {:.3f}s / {:.3f}s  (cpu/wall={:.2f})".format(
        wall, cpu, cpu / wall if wall else 0.0))
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
