"""Fake mooncake.store for local end-to-end testing.

Implements the MooncakeDistributedStore interface used by
mooncake_benchmark_client.py with a simple file-per-key backing store so the
full benchmark flow (orchestrator -> ssh -> run_benchmark.sh -> client) runs
for real on a single machine without a Mooncake deployment.

Data directory: MOCK_STORE_DIR env var, default /tmp/mooncake_mock_store.
See mock/README.md.
"""
import ctypes
import hashlib
import os


class _MockBufferHandle:
    """Fake BufferHandle: keeps a copy of the value in a ctypes buffer and
    exposes ptr()/size() like the real zero-copy handle."""

    def __init__(self, data):
        self._buf = (ctypes.c_ubyte * max(1, len(data)))()
        ctypes.memmove(self._buf, data, len(data))
        self._size = len(data)

    def ptr(self):
        return ctypes.addressof(self._buf)

    def size(self):
        return self._size

    def __len__(self):
        return self._size


class MooncakeDistributedStore:
    _dir = os.environ.get("MOCK_STORE_DIR", "/tmp/mooncake_mock_store")

    def __init__(self):
        os.makedirs(self._dir, exist_ok=True)
        self._buffers = {}

    def _kp(self, k):
        return os.path.join(self._dir, hashlib.md5(k.encode()).hexdigest())

    def setup(self, **kw):
        return 0

    def put(self, k, v, config=None):
        p = self._kp(k)
        tmp = p + ".tmp"
        with open(tmp, "wb") as f:
            f.write(v)
        os.replace(tmp, p)
        return 0

    def get(self, k):
        try:
            with open(self._kp(k), "rb") as f:
                return f.read()
        except FileNotFoundError:
            return b""

    def put_batch(self, keys, values, config=None):
        for k, v in zip(keys, values):
            self.put(k, v)
        return 0

    def get_batch(self, keys):
        return [self.get(k) for k in keys]

    def register_buffer(self, ptr, size):
        self._buffers[ptr] = size
        return 0

    def unregister_buffer(self, ptr):
        self._buffers.pop(ptr, None)
        return 0

    def put_from(self, k, ptr, size, config=None):
        try:
            size = min(size, self._buffers[ptr])
            data = ctypes.string_at(ptr, size)
            self.put(k, data)
            return 0
        except KeyError:
            return -1

    def get_into(self, k, ptr, size):
        try:
            size = min(size, self._buffers[ptr])
        except KeyError:
            return -1
        data = self.get(k)
        if not data:
            return -1
        if size < len(data):
            return -1
        ctypes.memmove(ptr, data, len(data))
        return len(data)

    def get_buffer(self, k):
        data = self.get(k)
        if not data:
            return None
        return _MockBufferHandle(data)

    def remove_all(self, force=False):
        for name in os.listdir(self._dir):
            try:
                os.remove(os.path.join(self._dir, name))
            except FileNotFoundError:
                pass
        return 0

    def batch_remove(self, keys, force=False):
        for k in keys:
            try:
                os.remove(self._kp(k))
            except FileNotFoundError:
                pass
        return [0] * len(keys)

    def batch_is_exist(self, keys):
        return [1 if os.path.exists(self._kp(k)) else 0 for k in keys]

    def is_exist(self, k):
        return 1 if os.path.exists(self._kp(k)) else 0

    def close(self):
        return 0
