"""REQ quantile sketch wrapper (Apache DataSketches) with graceful degradation.

Percentile policy (single code path for the whole pipeline):

    count / avg / min / max  -> exact (sum / extremes)
    p50 / p90 / p95          -> merged histogram interpolation (us-v1 bins)
    p99 / p999 / p9999       -> merged REQ sketch (Relative Error Quantiles)

REQ is tail-focused by design: relative error is bounded and tight at extreme
ranks, which is exactly where a fixed-bin histogram is weak.  The sketch is
produced in the map stage (one per op + one per product), serialized into the
stats product as base64, and merged again at reduce time for any scope.

The third-party library may be absent (old images / plain python envs) — every
entry point degrades to None so the callers fall back to histogram
interpolation with a WARN, never a hard failure.
"""
import base64
import binascii

FMT = "datasketches-req-v1"
# Default k.  Phase-0 microbenchmark (scripts/quantiles_bench.py, 5M lognormal
# + heavy-tail samples) measured the tail errors; k=24 keeps p99/p999/p9999
# well under a percent while the serialized sketch stays a few KB.
DEFAULT_K = 24
_EDGE_Q = 0.95          # <= this rank: histogram interpolation


def _lib():
    try:
        import datasketches  # noqa: F401
        return datasketches
    except Exception:
        return None


def available():
    return _lib() is not None


def resolve_k(value):
    """None/invalid -> DEFAULT_K; explicit values are clamped to [4, 512]."""
    try:
        k = int(value)
    except (TypeError, ValueError):
        return DEFAULT_K
    return max(4, min(512, k))


def make(k=None):
    """A fresh REQ sketch, or None when the library is unavailable."""
    ds = _lib()
    if ds is None:
        return None
    try:
        return ds.req_floats_sketch(k=resolve_k(k))
    except Exception:
        return None


def update(sketch, value):
    if sketch is None or value is None:
        return
    try:
        sketch.update(float(value))
    except Exception:
        pass


def serialize(sketch):
    """base64 text of the compact sketch, or None."""
    if sketch is None:
        return None
    try:
        return base64.b64encode(sketch.serialize()).decode("ascii")
    except Exception:
        return None


def _decode(data):
    try:
        return base64.b64decode(data.encode("ascii"), validate=True)
    except (AttributeError, ValueError, binascii.Error):
        return None


def merge(items, k=None):
    """Merge any number of serialized sketches (base64) into one sketch.

    Returns None when the library is unavailable, every item is unreadable,
    or nothing was merged."""
    ds = _lib()
    if ds is None or not items:
        return None
    out = None
    for item in items:
        if not item:
            continue
        raw = _decode(item)
        if raw is None:
            continue
        try:
            part = ds.req_floats_sketch.deserialize(raw)
        except Exception:
            continue
        if out is None:
            out = part
        else:
            try:
                out.merge(part)
            except Exception:
                pass
    return out


def quantile(sketch, q):
    """Approximate quantile of the sketch, or None."""
    if sketch is None:
        return None
    try:
        v = float(sketch.get_quantile(float(q)))
        return v if v == v else None      # NaN guard
    except Exception:
        return None


def quantile_from(items, q, k=None):
    """Merge serialized sketches then query one quantile."""
    return quantile(merge(items, k=k), q)


def relative_error(sketch):
    """The sketch's own relative standard-error estimate (REQ get_RSE)."""
    if sketch is None:
        return None
    try:
        return float(sketch.get_RSE())
    except Exception:
        return None


def is_tail(q):
    """Ranks above this use the sketch instead of the histogram."""
    return float(q) > _EDGE_Q
