"""Request-sample window aggregation; raw artifacts are never modified."""

def _exact_percentile(sorted_vals, q):
    if not sorted_vals:
        return 0.0
    return sorted_vals[int(round(q * (len(sorted_vals) - 1)))]


def window_summary(samples_path, summary, start, end):
    """Recompute window aggregates without modifying raw samples/summary."""
    import csv
    observed = summary.get("elapsed_seconds")
    end = min(end, float(observed)) if observed is not None else end
    wall = max(0.0, end - start)
    result = {"elapsed_seconds": wall, "operations": {}}
    values = {}
    with open(samples_path, newline="", encoding="utf-8") as stream:
        for row in csv.DictReader(stream):
            elapsed = float(row["elapsed_us"]) / 1e6
            if not start <= elapsed < end:
                continue
            op = row["operation"]
            st = result["operations"].setdefault(op, {
                "total": 0, "success": 0, "failure": 0, "bytes": 0})
            st["total"] += 1
            if row["status"] == "OK":
                st["success"] += 1
                st["bytes"] += int(row.get("bytes") or 0)
                values.setdefault(op, []).append(float(row["latency_ms"]))
            else:
                st["failure"] += 1
    for op, st in result["operations"].items():
        vals = sorted(values.get(op, []))
        st["avg_ms"] = sum(vals) / len(vals) if vals else 0
        st["max_ms"] = vals[-1] if vals else 0
        for name, q in (("p50_ms", .5), ("p90_ms", .9), ("p95_ms", .95),
                        ("p99_ms", .99), ("p999_ms", .999), ("p9999_ms", .9999)):
            st[name] = _exact_percentile(vals, q) if vals else 0
    # Full-run-only counters (verification, queue peaks, drops) cannot be
    # attributed to a window and must not leak into the filtered report.
    return result
