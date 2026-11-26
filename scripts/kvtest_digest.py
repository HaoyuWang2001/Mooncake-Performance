"""Merge exported t-digest centroids; exact moments and threshold counts."""
import math

THRESHOLDS = (2000, 5000, 6000, 7000, 8000, 9000, 10000, 20000)
QUANTILES = {"p50_ms": .5, "p90_ms": .9, "p95_ms": .95,
             "p99_ms": .99, "p999_ms": .999, "p9999_ms": .9999}


def compress(centroids, compression=1000):
    ordered = sorted(centroids)
    if not ordered:
        return []
    total = sum(w for _, w in ordered)
    def scale(q):
        return compression * math.asin(2 * min(1, max(0, q)) - 1) / (2 * math.pi)
    result = []
    before = 0
    mean, weight = ordered[0]
    for m, w in ordered[1:]:
        if scale((before + weight + w) / total) - scale(before / total) <= 1:
            mean += (m - mean) * w / (weight + w)
            weight += w
        else:
            result.append((mean, weight))
            before += weight
            mean, weight = m, w
    result.append((mean, weight))
    return result


def quantile(centroids, q):
    if not centroids:
        return 0.0
    target = q * sum(w for _, w in centroids)
    previous = centroids[0][1] / 2
    if target <= previous:
        return centroids[0][0]
    for i in range(1, len(centroids)):
        center = previous + (centroids[i - 1][1] + centroids[i][1]) / 2
        if target <= center:
            f = (target - previous) / (center - previous)
            return centroids[i - 1][0] + f * (centroids[i][0] - centroids[i - 1][0])
        previous = center
    return centroids[-1][0]


def merge_operations(operations):
    """Return None only for legacy summaries. Reject corrupt/mixed digests."""
    operations = list(operations)
    if not any("digest" in st for st in operations):
        return None
    centroids = []
    for st in operations:
        n = int(st.get("success", 0))
        if "digest" not in st:
            if n:
                raise ValueError("cannot merge legacy and t-digest statistics")
            continue
        digest = st["digest"]
        if digest.get("unit") != "ms":
            raise ValueError("digest unit must be ms")
        points = digest.get("centroids", [])
        for mean, weight in points:
            if not math.isfinite(mean) or not math.isfinite(weight) or weight <= 0:
                raise ValueError("invalid digest centroid")
        if not math.isclose(sum(w for _, w in points), n, abs_tol=1e-6):
            raise ValueError("digest weight does not match success count")
        centroids.extend(points)
    merged = compress(centroids)
    count = sum(int(st.get("success", 0)) for st in operations)
    total_ms = sum(float(st.get("latency_sum_ms", st.get("busy_ms", 0)))
                   for st in operations)
    active = [st for st in operations if st.get("success", 0)]
    result = {"avg_ms": total_ms / count if count else 0,
              "min_ms": min((st["min_ms"] for st in active), default=0),
              "max_ms": max((st["max_ms"] for st in active), default=0),
              "latency_sum_ms": total_ms,
              "digest": {"unit": "ms", "compression": 1000,
                         "centroids": merged}}
    result.update({name: quantile(merged, q) for name, q in QUANTILES.items()})
    result.update({f">{us}us": sum(int(st.get(f">{us}us", 0)) for st in operations)
                   for us in THRESHOLDS})
    return result
