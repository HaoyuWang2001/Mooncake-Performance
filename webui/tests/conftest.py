"""pytest bootstrap: keep loopback traffic out of any HTTP proxy.

The test suite starts real local servers (WebUI backend, mock etcd gateway)
on 127.0.0.1:<random port> and talks to them through urllib. Python's urllib
only honours literal host entries in no_proxy (no "127.*" wildcards), so on
machines whose proxy environment uses that syntax (common in WSL) every
loopback request would be sent to the proxy and fail with 502 / empty
bodies. Prepending the loopback names here makes the suite independent of
the surrounding shell/IDE environment.
"""
import os

_LOOPBACK = ("127.0.0.1", "localhost")
for _key in ("no_proxy", "NO_PROXY"):
    _entries = [e for e in os.environ.get(_key, "").split(",") if e]
    _missing = [h for h in _LOOPBACK if h not in _entries]
    if _missing:
        os.environ[_key] = ",".join(_missing + _entries)

# ---------------------------------------------------------------------------
# Shared test doubles
# ---------------------------------------------------------------------------
import base64 as _b64
import json as _json
import math as _math
import sys as _sys

import pytest as _pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
for _p in (_ROOT, os.path.join(_ROOT, "scripts")):
    if _p not in _sys.path:
        _sys.path.insert(0, _p)


class _StubSketch(object):
    def __init__(self, vals):
        self.vals = list(vals or [])

    def update(self, value):
        self.vals.append(float(value))

    def merge(self, other):
        self.vals.extend(other.vals)

    def serialize(self):
        return _json.dumps(self.vals).encode("ascii")


class FakeSketch(object):
    """Datasketches stand-in for tests (exact nearest-rank quantiles).

    The real REQ sketch is a tail approximation; a plain sorted list keeps
    the two-stage pipeline (pod map -> partial -> finalize) fully testable
    on machines without the datasketches wheel installed."""

    FMT = "fake-sketch"

    def available(self):
        return True

    def resolve_k(self, value):
        try:
            return max(4, min(512, int(value)))
        except (TypeError, ValueError):
            return 24

    def make(self, k=None):
        return _StubSketch([])

    def update(self, sketch, value):
        if sketch is not None and value is not None:
            sketch.update(float(value))

    def serialize(self, sketch):
        if sketch is None:
            return None
        return _b64.b64encode(sketch.serialize()).decode("ascii")

    def _decode(self, data):
        return _StubSketch(_json.loads(_b64.b64decode(data.encode("ascii"))))

    def merge(self, items, k=None):
        out = _StubSketch([])
        for item in items or []:
            if item:
                out.vals.extend(self._decode(item).vals)
        return out

    def quantile(self, sketch, q):
        if sketch is None or not sketch.vals:
            return None
        vals = sorted(sketch.vals)
        idx = min(len(vals) - 1, max(0, int(_math.ceil(q * len(vals))) - 1))
        return vals[idx]

    def quantile_from(self, items, q, k=None):
        return self.quantile(self.merge(items), q)

    def relative_error(self, sketch):
        return 0.1

    def is_tail(self, q):
        return float(q) > 0.95


@_pytest.fixture
def fake_sketch(monkeypatch):
    import logstats as _ls
    monkeypatch.setattr(_ls, "_qt", FakeSketch())
    yield FakeSketch()
