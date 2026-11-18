"""Writer notifications: asynchronous progress, bounded queue and drain."""
import argparse
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import benchmark as bm
import mooncake_benchmark_client as client


def _args(**overrides):
    values = dict(remote_data_dir="unused", scenario="test", batch_size=1,
                  key_prefix="test", client_id=0, operation="put", duration=0.06,
                  warmup_duration=0, target_qps=200, traffic_mode="balanced",
                  notify_count=1, notify_threads=2, notify_async=True,
                  notify_max_inflight=4, key_count=0, writer_count=1,
                  reader_count=1, work_mode="rank", protocol="tcp")
    values.update(overrides)
    return argparse.Namespace(**values)


def _run(monkeypatch, args, open_notify, write=None):
    emitted = []
    monkeypatch.setattr(client, "load_value", lambda *a: b"value")
    monkeypatch.setattr(client, "emit", lambda tag, data: emitted.append((tag, data)))
    monkeypatch.setattr(client, "_run_iface", write or (lambda *a: (0.1, "OK")))
    monkeypatch.setattr(client._NotifyHTTPPool, "send",
                        lambda self, reader, path: open_notify(path, timeout=0.5))
    client._writer_run_phases(args, object(), [{"host": "reader", "port": 9301}])
    return next(data for tag, data in emitted if tag == "SUMMARY")


@pytest.mark.parametrize("asynchronous", [False, True])
def test_writer_progress_while_notify_pending(monkeypatch, asynchronous):
    second_write = threading.Event()
    calls = []
    def write(*a):
        calls.append(1)
        if len(calls) >= 2:
            second_write.set()
        return 0.1, "OK"
    class Response:
        def close(self):
            pass
    def open_notify(*a, **kw):
        second_write.wait(0.15)
        return Response()
    summary = _run(monkeypatch, _args(notify_async=asynchronous), open_notify, write)
    assert (len(calls) > 1) == asynchronous
    assert summary["notify_submitted"] == summary["notify_ok"]
    assert summary["notify_fail"] == 0


def test_bounded_queue_and_failures_drained(monkeypatch):
    def open_notify(*a, **kw):
        time.sleep(0.04)
        raise TimeoutError("simulated timeout")
    summary = _run(monkeypatch, _args(notify_max_inflight=2, target_qps=10000),
                   open_notify)
    assert summary["notify_inflight_peak"] == 2
    assert summary["notify_queue_peak"] <= 2
    assert summary["notify_backpressure_ms"] > 0
    assert summary["notify_drain_ms"] > 0
    assert summary["notify_submitted"] == summary["notify_fail"] > 0
    assert summary["notify_ok"] == 0
    assert summary["notify_queue_wait_avg_ms"] > 0


def test_warmup_drains_before_measurement(monkeypatch):
    pending_warmup = set()
    measured = []
    overlap = []
    lock = threading.Lock()
    def open_notify(url, **kw):
        warmup = "warmup=1" in url
        with lock:
            if warmup:
                pending_warmup.add(url)
            else:
                measured.append(True)
                overlap.append(bool(pending_warmup))
        time.sleep(0.02)
        with lock:
            pending_warmup.discard(url)
        raise TimeoutError("simulated timeout")
    summary = _run(monkeypatch, _args(warmup_duration=0.04), open_notify)
    assert measured
    assert not any(overlap)
    assert summary["notify_submitted"] == len(measured)
    assert summary["notify_fail"] == len(measured)


def test_notify_plan_validation():
    for cfg in ({"writer": {}}, {"client": {"count_per_node": 2}, "writer": {}}):
        assert bm.pipeline_plan(cfg)["notify_async"] is False
        cfg["writer"].update(notify_async=True, notify_max_inflight=17)
        plan = bm.pipeline_plan(cfg)
        assert plan["notify_async"] is True
        assert plan["notify_max_inflight"] == 17
    with pytest.raises(RuntimeError, match="notify_max_inflight"):
        bm.pipeline_plan({"writer": {"notify_max_inflight": 0}})
    with pytest.raises(RuntimeError, match="JSON boolean"):
        bm.pipeline_plan({"writer": {"notify_async": "false"}})
    with pytest.raises(RuntimeError, match="notify_queue_size"):
        bm.pipeline_plan({"writer": {"notify_queue_size": 0}})
    assert bm.pipeline_plan({"writer": {"notify_queue_size": 8192}})["notify_queue_size"] == 8192


def test_notify_summary_aggregation(tmp_path):
    paths = []
    for i, count in enumerate((2, 6)):
        path = tmp_path / (str(i) + ".log")
        path.write_text("SUMMARY\t" + json.dumps(dict(
            mode="pipeline_writer_summary", notify_submitted=count,
            notify_ok=count - 1, notify_fail=1, notify_avg_ms=10 + 10 * i,
            notify_inflight_peak=count, notify_max_ms=30 + i,
            notify_backpressure_ms=5, notify_drain_ms=7, notify_queue_peak=count,
            notify_queue_wait_avg_ms=20 + 10 * i, notify_connections_created=2,
            notify_connections_reused=count)) + "\n", encoding="utf-8")
        paths.append(path)
    agg = bm.parse_benchmark_log(paths, op_filter="pipeline_write")
    assert agg["notify_submitted"] == 8
    assert agg["notify_ok"] == 6 and agg["notify_fail"] == 2
    assert agg["notify_avg_ms"] == 17.5
    assert agg["notify_inflight_peak"] == 6
    assert agg["notify_backpressure_ms"] == 10
    assert agg["notify_queue_peak"] == 6
    assert agg["notify_queue_wait_avg_ms"] == 27.5
    assert agg["notify_connections_created"] == 4
    assert agg["notify_connections_reused"] == 8
    assert bm.parse_benchmark_log(paths, op_filter="pipeline_read")["notify_submitted"] == 0


def test_notify_env_resolution(monkeypatch):
    class Args(argparse.Namespace):
        def __getattr__(self, name):
            return None
    monkeypatch.setenv("BENCH_NOTIFY_ASYNC", "true")
    monkeypatch.setenv("BENCH_NOTIFY_MAX_INFLIGHT", "19")
    args = Args()
    client._resolve_env(args)
    assert args.notify_async is True and args.notify_max_inflight == 19
    assert args.notify_queue_size == 19
    args = Args(notify_async="false", notify_max_inflight=7)
    client._resolve_env(args)
    assert args.notify_async is False and args.notify_max_inflight == 7
    monkeypatch.setenv("BENCH_NOTIFY_QUEUE_SIZE", "8192")
    args = Args()
    client._resolve_env(args)
    assert args.notify_queue_size == 8192
    with pytest.raises(ValueError, match="notify_max_inflight"):
        client._resolve_env(Args(notify_max_inflight=0))


def test_async_client_lifetime_includes_both_drains():
    cfg = dict(duration=30, warmup={"duration": 5},
               writer={"notify_async": True, "notify_threads": 10,
                       "notify_max_inflight": 256})
    assert bm._pipeline_client_timeout(cfg, 120) == 362
    cfg["writer"]["notify_queue_size"] = 8192
    assert bm._pipeline_client_timeout(cfg, 120) == 1156


def test_notify_metrics_exported_to_csv(tmp_path):
    log = tmp_path / "test.benchmark_c1_t1_8MB.dual_worker_c0_p9301.writer.log"
    log.write_text('SUMMARY\t' + json.dumps(dict(
        mode="pipeline_writer_summary", notify_submitted=3, notify_ok=2,
        notify_fail=1, notify_avg_ms=12, notify_drain_ms=9)) + '\n', encoding="utf-8")
    cfg = dict(scenario="pipeline", protocol="tcp", data_size=["8MB"],
               duration=30, client={"count_per_node": 1},
               writer={"operation": "put", "notify_async": True},
               reader={"operation": "get"})
    rows = bm.write_scenario_csv(str(tmp_path), cfg, "ts", "test.json",
                                 result_dir=str(tmp_path))
    writer = next(row for row in rows if row["role"] == "writer")
    assert writer["notify_async"] is True
    assert writer["notify_submitted"] == 3
    assert writer["notify_ok"] == 2 and writer["notify_fail"] == 1
    assert writer["notify_drain_ms"] == 9


@pytest.fixture
def notify_server(monkeypatch):
    handler = client._PipelineNotifyHandler
    queue = Queue(maxsize=8192)
    for key, value in dict(queue=queue, deadline=time.time() + 60,
                           qlock=threading.Lock(), qdepth=0, qmax=0,
                           start_event=threading.Event()).items():
        monkeypatch.setattr(handler, key, value)
    server = client._PipelineNotifyServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield {"host": "127.0.0.1", "port": server.server_address[1]}, queue
    server.shutdown()
    server.server_close()
    thread.join()


def test_http_keepalive_reuses_connection_and_frames_all_replies(notify_server):
    reader, queue = notify_server
    pool = client._NotifyHTTPPool()
    try:
        for i in range(20):
            pool.send(reader, "/notify?key=k{}&writer_id=0".format(i))
        for path in ("/health", "/start", "/unknown", "/shutdown"):
            pool.send(reader, path)
        assert queue.qsize() == 20
        assert pool.created == 1 and pool.reused == 23
    finally:
        pool.close()


def test_http_pool_concurrent_exclusive_checkout(notify_server):
    reader, queue = notify_server
    pool = client._NotifyHTTPPool()
    try:
        with ThreadPoolExecutor(max_workers=10) as workers:
            futures = [workers.submit(pool.send, reader,
                       "/notify?key=k{}&writer_id=0".format(i)) for i in range(200)]
            for future in futures:
                future.result()
        assert queue.qsize() == 200
        assert pool.reused > 0
        assert len(pool.idle[(reader["host"], reader["port"])]) <= 2
    finally:
        pool.close()


def test_failed_connection_discarded_without_retry(notify_server, monkeypatch):
    reader, queue = notify_server
    reply = client._PipelineNotifyHandler._reply
    def disconnect_first(self, body):
        if "key=first" in self.path:
            self.close_connection = True  # received and enqueued, but no reply
            return
        reply(self, body)
    monkeypatch.setattr(client._PipelineNotifyHandler, "_reply", disconnect_first)
    pool = client._NotifyHTTPPool()
    try:
        with pytest.raises(Exception):
            pool.send(reader, "/notify?key=first&writer_id=0")
        pool.send(reader, "/notify?key=second&writer_id=0")
        assert queue.qsize() == 2  # no duplicate from an automatic retry
        assert pool.created == 2
    finally:
        pool.close()


def test_large_send_queue_keeps_writer_progressing(monkeypatch):
    def slow_notify(*a, **kw):
        time.sleep(0.02)
    summary = _run(monkeypatch, _args(notify_queue_size=8192, target_qps=1000), slow_notify)
    assert summary["notify_submitted"] > 10
    assert summary["notify_queue_peak"] > 0
    assert summary["notify_backpressure_ms"] == 0
    assert summary["notify_submitted"] == summary["notify_ok"]
