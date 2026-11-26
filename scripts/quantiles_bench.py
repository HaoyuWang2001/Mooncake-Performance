#!/usr/bin/env python3
"""Phase-0 microbenchmark for the REQ sketch percentile policy.

Runs OUTSIDE the benchmark pipeline (needs `pip install datasketches`); used to
pick the default k and to quantify the errors the report footnote states:

    - REQ sketch (k candidates) tail error at p99 / p999 / p9999
    - merged-histogram interpolation error at p50 / p95 (the center path)
    - insert / merge throughput at realistic sample counts

Usage:  python3 scripts/quantiles_bench.py [--size N] [--sizes 1e6,5e6]
        [--k 12,24,48] [--seed S]
"""
import argparse
import math
import os
import random
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import logstats as ls          # noqa: E402
import quantiles as qs         # noqa: E402

QS = (0.50, 0.95, 0.99, 0.999, 0.9999)


def _sample(n, seed, heavy_tail=True):
    """Lognormal latency-ish distribution with an occasional tail cluster."""
    rng = random.Random(seed)
    out = []
    for _ in range(n):
        v = rng.lognormvariate(0.0, 0.6)
        if heavy_tail and rng.random() < 0.01:
            v *= 40.0          # 1% slow cluster: stresses the extreme tails
        out.append(v)
    return out


def _truth(values, q):
    s = sorted(values)
    idx = min(len(s) - 1, max(0, int(math.ceil(q * len(s))) - 1))
    return s[idx]


def _hist(values):
    hist = {}
    for v in values:
        b = ls.bin_of(v, ls.BIN_EDGES_V2)
        hist[b] = hist.get(b, 0) + 1
    return hist


def _err(est, truth):
    if est is None or truth == 0:
        return None
    return 100.0 * abs(est - truth) / truth


def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1e6,5e6")
    ap.add_argument("--k", default="12,24,48")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--no-tail", action="store_true",
                    help="pure lognormal without the 1%% slow cluster")
    ap.add_argument("--merge-shards", type=int, default=16)
    args = ap.parse_args(argv)
    if not qs.available():
        print("ERROR: datasketches not installed", file=sys.stderr)
        return 2
    sizes = [int(float(x)) for x in args.sizes.split(",") if x.strip()]
    ks = [int(x) for x in args.k.split(",") if x.strip()]
    heavy = not args.no_tail
    print("datasketches REQ bench: sizes={} k={} heavy_tail={}".format(
        sizes, ks, heavy))
    for n in sizes:
        t0 = time.time()
        values = _sample(n, args.seed, heavy)
        gen_s = time.time() - t0
        truth = {q: _truth(values, q) for q in QS}
        t0 = time.time()
        hist = _hist(values)
        hist_s = time.time() - t0
        print("\n== n={:,} (gen {:.1f}s, hist {:.1f}s)".format(
            n, gen_s, hist_s))
        print("  truth: " + " ".join(
            "p{:.0f}={:.3f}".format(q * 100, truth[q]) for q in QS))
        print("  hist : " + " ".join(
            "p{:.0f}={:.3f}({:+.1f}%)".format(
                q * 100, ls.hist_percentile(hist, q, ls.BIN_EDGES_V2),
                _err(ls.hist_percentile(hist, q, ls.BIN_EDGES_V2),
                     truth[q]) or 0.0) for q in QS))
        for k in ks:
            t0 = time.time()
            sk = qs.make(k)
            for v in values:
                sk.update(v)
            build_s = time.time() - t0
            # merge path: shard the same samples, merge the shards
            shards = []
            per = max(1, n // args.merge_shards)
            for i in range(0, n, per):
                sh = qs.make(k)
                for v in values[i:i + per]:
                    sh.update(v)
                shards.append(qs.serialize(sh))
            t0 = time.time()
            merged = qs.merge(shards, k=k)
            merge_s = time.time() - t0
            cols = []
            for q in QS:
                e = _err(qs.quantile(sk, q), truth[q])
                m = _err(qs.quantile(merged, q), truth[q])
                cols.append("p{:.0f}={:+.2f}%/{:+.2f}%".format(
                    q * 100, e if e is not None else float("nan"),
                    m if m is not None else float("nan")))
            print("  k={:<3} build={:.1f}s ({:.0f}k/s) merge={:.2f}s "
                  "ser={}B rse={:.4f}".format(
                      k, build_s, n / build_s / 1000.0, merge_s,
                      len(qs.serialize(sk) or ""),
                      qs.relative_error(sk) or -1.0))
            print("       err(single/merged): " + " ".join(cols))
    return 0


if __name__ == "__main__":
    sys.exit(main())
