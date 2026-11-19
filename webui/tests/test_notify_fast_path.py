"""Notify hot path: fast reader HTTP implementation + raw-socket sender.

Covers the pieces that replaced `http.client` / `BaseHTTPRequestHandler` in
the pipeline notify path: query parsing must stay byte-for-byte equivalent to
urllib.parse.parse_qs, every endpoint keeps its reply/state semantics, and
the pooled raw-socket sender must reuse connections, surface non-200 replies
and never hand back a broken connection.
"""
import os
import socket
import sys
import threading
import time
import urllib.parse

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import mooncake_benchmark_client as client  # noqa: E402


# ------------------------------------------------------------------ parsing

@pytest.mark.parametrize("query", [
    "",
    "key=a",
    "key=a&key=b",
    "key=one&writer_id=3&write_ts=1.5&warmup=1",
    "key=a%2Fb%20c",
    "key=a+b",
    "key=%E4%B8%AD%E6%96%87",
    "a=1&a=2&b=3",
    "blank=&key=x",
    "novalue&key=x",
    "key=a&&key=b",
    "key=a=b",
])
def test_query_parser_matches_urllib(query):
    assert client._parse_notify_query(query) == urllib.parse.parse_qs(query)


# ------------------------------------------------------------------- server

class _StateSnapshot(object):
    """Save/restore the reader-side class state around a test."""

    ATTRS = ("queue", "deadline", "exit_grace", "start_event",
             "qdepth", "qmax", "qlock")

    def __init__(self):
        handler = client._PipelineNotifyHandler
        self.saved = {name: getattr(handler, name) for name in self.ATTRS}
        self.handler = handler

    def restore(self):
        for name, value in self.saved.items():
            setattr(self.handler, name, value)


@pytest.fixture
def reader(request):
    snapshot = _StateSnapshot()
    queue = client.Queue(maxsize=256)
    handler = snapshot.handler
    handler.queue = queue
    handler.deadline = time.time() + 60.0
    handler.exit_grace = 5.0
    handler.start_event = threading.Event()
    handler.qdepth = 0
    handler.qmax = 0
    handler.qlock = threading.Lock()
    server = client._FastNotifyServer(("127.0.0.1", 0),
                                      client._FastNotifyHandler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    state = {"queue": queue, "port": server.server_address[1],
             "handler": handler, "server": server}

    def teardown():
        server.shutdown()
        server.server_close()
        snapshot.restore()

    request.addfinalizer(teardown)
    return state


def _send(sock, path):
    sock.sendall(client._notify_request_bytes(
        ("127.0.0.1", sock.getpeername()[1]), path))


def _request(sock, path):
    _send(sock, path)
    return client._read_notify_reply(sock)


def _reply_body(sock, path):
    _send(sock, path)
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = sock.recv(4096)
        assert chunk
        buf += chunk
    head, _, body = buf.partition(b"\r\n\r\n")
    length = 0
    for line in head.split(b"\r\n")[1:]:
        name, _, value = line.partition(b":")
        if name.strip().lower() == b"content-length":
            length = int(value.strip())
    while len(body) < length:
        body += sock.recv(4096)
    return body


def _connect(port):
    return socket.create_connection(("127.0.0.1", port), timeout=5.0)


def test_health_endpoint(reader):
    sock = _connect(reader["port"])
    try:
        status, _ = _request(sock, "/health")
        assert status == 200
    finally:
        sock.close()


def test_notify_enqueues_batch_and_tracks_depth(reader):
    sock = _connect(reader["port"])
    try:
        _request(sock, "/notify?key=a&key=b&writer_id=7&write_ts=12.5&warmup=0")
    finally:
        sock.close()
    keys, writer_id, write_ts, is_warmup, arrival = reader["queue"].get_nowait()
    assert keys == ["a", "b"]
    assert writer_id == 7
    assert write_ts == 12.5
    assert is_warmup is False
    assert arrival <= time.perf_counter()
    assert reader["handler"].qdepth == 1
    assert reader["handler"].qmax == 1


def test_warmup_flag_and_phase_marker(reader):
    sock = _connect(reader["port"])
    try:
        _request(sock, "/notify?key=k&writer_id=0&write_ts=1&warmup=1")
    finally:
        sock.close()
    assert reader["queue"].get_nowait()[3] is True


def test_start_sets_event(reader):
    sock = _connect(reader["port"])
    try:
        _request(sock, "/start")
    finally:
        sock.close()
    assert reader["handler"].start_event.is_set()


def test_shutdown_closes_window(reader):
    sock = _connect(reader["port"])
    try:
        _request(sock, "/shutdown")
    finally:
        sock.close()
    assert reader["handler"].deadline == -1.0


def test_unknown_path_is_echoed(reader):
    sock = _connect(reader["port"])
    try:
        assert _reply_body(sock, "/nope?x=1") == b"UNKNOWN:/nope?x=1"
    finally:
        sock.close()
    assert reader["queue"].empty()


def test_keep_alive_serves_two_requests_on_one_connection(reader):
    sock = _connect(reader["port"])
    try:
        assert _request(sock, "/health")[0] == 200
        assert _request(sock, "/notify?key=a&writer_id=0&write_ts=1")[0] == 200
    finally:
        sock.close()
    assert reader["queue"].qsize() == 1


def test_connection_close_header_ends_connection(reader):
    sock = _connect(reader["port"])
    try:
        sock.sendall(b"GET /health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        status, keep_alive = client._read_notify_reply(sock)
        assert status == 200
        assert keep_alive is False
    finally:
        sock.close()


# -------------------------------------------------------------------- pool

def test_pool_reuses_connection(reader):
    pool = client._NotifyHTTPPool()
    try:
        reader_spec = {"host": "127.0.0.1", "port": reader["port"]}
        pool.send(reader_spec, "/notify?key=a&writer_id=0&write_ts=1")
        pool.send(reader_spec, "/notify?key=b&writer_id=0&write_ts=1")
        assert pool.created == 1
        assert pool.reused == 1
        assert reader["queue"].qsize() == 2
    finally:
        pool.close()


def test_pool_raises_on_non_200(reader):
    class _Always500(object):
        def __init__(self, addr):
            self.sock = socket.socket()
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.sock.bind(addr)
            self.sock.listen(8)
            self.addr = self.sock.getsockname()
            threading.Thread(target=self._serve, daemon=True).start()

        def _serve(self):
            while True:
                conn, _ = self.sock.accept()
                conn.recv(65536)
                conn.sendall(b"HTTP/1.1 503 nope\r\nContent-Length: 0\r\n\r\n")
                conn.close()

    stub = _Always500(("127.0.0.1", 0))
    pool = client._NotifyHTTPPool()
    try:
        with pytest.raises(RuntimeError):
            pool.send({"host": "127.0.0.1", "port": stub.addr[1]}, "/notify")
        assert pool.created == 1 and pool.reused == 0
    finally:
        pool.close()
        stub.sock.close()


def test_pool_surfaces_closed_connection(reader):
    class _Drop(object):
        def __init__(self):
            self.sock = socket.socket()
            self.sock.bind(("127.0.0.1", 0))
            self.sock.listen(8)
            self.port = self.sock.getsockname()[1]
            threading.Thread(target=self._serve, daemon=True).start()

        def _serve(self):
            while True:
                conn, _ = self.sock.accept()
                conn.close()

    stub = _Drop()
    pool = client._NotifyHTTPPool()
    try:
        with pytest.raises(OSError):
            pool.send({"host": "127.0.0.1", "port": stub.port}, "/notify")
    finally:
        pool.close()
        stub.sock.close()


# --------------------------------------------------------------- selection

def test_server_impl_defaults_to_fast(monkeypatch):
    monkeypatch.delenv("MC_NOTIFY_HTTP", raising=False)
    assert client._notify_server_impl() == (
        client._FastNotifyServer, client._FastNotifyHandler)


def test_server_impl_legacy_rollback(monkeypatch):
    monkeypatch.setenv("MC_NOTIFY_HTTP", "legacy")
    assert client._notify_server_impl() == (
        client._PipelineNotifyServer, client._PipelineNotifyHandler)


# ------------------------------------------------------------------- tuner

def test_gil_switch_interval_env(monkeypatch):
    old = sys.getswitchinterval()
    saved = list(client._GIL_SWITCH_INTERVAL)
    try:
        monkeypatch.setenv("BENCH_GIL_SWITCH_INTERVAL", "0.001")
        client._apply_gil_switch_interval()
        assert sys.getswitchinterval() == pytest.approx(0.001)
        assert client._GIL_SWITCH_INTERVAL[0] == pytest.approx(0.001)
    finally:
        sys.setswitchinterval(old)
        client._GIL_SWITCH_INTERVAL[:] = saved


def test_gil_switch_interval_unset_leaves_default(monkeypatch):
    old = sys.getswitchinterval()
    monkeypatch.delenv("BENCH_GIL_SWITCH_INTERVAL", raising=False)
    client._apply_gil_switch_interval()
    assert sys.getswitchinterval() == old


def test_gil_switch_interval_rejects_bad_values(monkeypatch):
    for bad in ("nope", "0", "-1"):
        monkeypatch.setenv("BENCH_GIL_SWITCH_INTERVAL", bad)
        with pytest.raises(SystemExit):
            client._apply_gil_switch_interval()
