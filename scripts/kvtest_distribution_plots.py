"""Digest Pod comparisons and CDF/CCDF; optional exact raw statistics.

Data preparation uses only the standard library. Matplotlib is imported only
when rendering, so --no-plot still produces both statistical reports.
"""
import csv
import json
import re
from pathlib import Path

try:
    from scripts.kvtest_digest import merge_operations, quantile, THRESHOLDS, QUANTILES
except ModuleNotFoundError:
    from kvtest_digest import merge_operations, quantile, THRESHOLDS, QUANTILES

DATA_FILE = "kvtest_distributions.json"


def exact_stats(values):
    values = sorted(values)
    count = len(values)
    result = {"success": count, "avg_ms": sum(values) / count if count else 0,
              "min_ms": values[0] if count else 0,
              "max_ms": values[-1] if count else 0}
    result.update({name: values[round(q * (count - 1))] if count else 0
                   for name, q in QUANTILES.items()})
    result.update({f">{us}us": sum(v * 1000 > us for v in values) for us in THRESHOLDS})
    return result


def prepare(folder, cfg=None):
    folder = Path(folder)
    meta_path = folder / "kvtest_report_meta.json"
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}
    cfg = cfg or meta
    report = cfg.get("report") or {}
    start, end = float(report.get("start_s", 0)), report.get("end_s")
    selected = bool(start or end is not None)
    end = float(end if end is not None else cfg.get("duration", 0)) if selected else None
    groups = {}
    raw_groups = {}
    for path in sorted(folder.glob("*.summary.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        if doc.get("statistics_format") != "tdigest-v1":
            continue
        source = doc
        if selected:
            source = doc.get("report_window") or {}
            if (source.get("start_s"), source.get("end_s")) != (start, end):
                raise ValueError(f"{path.name}: requested digest window was not collected")
        match = re.search(r"\.kvtest_(.+)\.summary\.json$", path.name)
        basic_size = re.search(r"_([^_]+)\.summary\.json$", path.name)
        size = (match.group(1) if match else basic_size.group(1) if basic_size
                else str(doc.get("data_size", "unknown")))
        instance = str(doc.get("instance_id", path.stem))
        pod = (cfg.get("_kvtest_iid_pods") or meta.get("iid_pods") or {}).get(instance)
        node = (cfg.get("_kvtest_iid_nodes") or meta.get("iid_nodes") or {}).get(instance)
        label = f"i{instance}" + (f" {pod or node}" if pod or node else "")
        raw = {}
        sample_file = path.with_name(path.name[:-len(".summary.json")] + ".samples.csv")
        if doc.get("record_raw_samples"):
            if not sample_file.exists():
                raise ValueError(f"raw recording enabled but {sample_file.name} is missing")
            with sample_file.open(encoding="utf-8", newline="") as stream:
                for row in csv.DictReader(stream):
                    elapsed = float(row["elapsed_us"]) / 1e6
                    if row["status"] == "OK" and (not selected or start <= elapsed < end):
                        raw.setdefault(row["operation"], []).append(float(row["latency_ms"]))
        for operation, st in (source.get("operations") or {}).items():
            if "digest" not in st:
                continue
            role = doc.get("role", "client")
            if doc.get("mode") == "basic" or cfg.get("scenario") == "kvtest_basic":
                role = "interface"
            elif role in ("dual", "client"):
                reader = meta.get("reader_operation", (cfg.get("reader") or {}).get("operation", "get_buffer"))
                role = "reader" if operation in (reader, "notify_queue_wait") else "writer"
            key = (size, role, operation)
            group = groups.setdefault(key, {"data_size": size, "role": role,
                                            "operation": operation, "pods": []})
            group["pods"].append({"instance": instance, "label": label, "statistics": st})
            if doc.get("record_raw_samples"):
                values = raw.get(operation, [])
                if len(values) != int(st.get("success", 0)):
                    raise ValueError(f"{path.name}: incomplete raw samples for {operation}")
                raw_group = raw_groups.setdefault(key, {"pods": [], "values": []})
                raw_group["pods"].append({"instance": instance, "label": label,
                                          "statistics": exact_stats(values)})
                raw_group["values"].extend(values)
    result = {"format": "kvtest-distributions-v1", "work_mode": cfg.get("work_mode", meta.get("work_mode", "")),
              "report_window": [start, end] if selected else None,
              "groups": []}
    raw_result = {"method": "exact-nearest-index", "report_window": result["report_window"], "groups": []}
    for key, group in sorted(groups.items()):
        group["total"] = merge_operations(p["statistics"] for p in group["pods"])
        group["total"]["success"] = sum(p["statistics"].get("success", 0) for p in group["pods"])
        result["groups"].append(group)
        if key in raw_groups:
            raw = raw_groups[key]
            raw_result["groups"].append({"data_size": key[0], "role": key[1], "operation": key[2],
                                          "pods": raw["pods"], "total": exact_stats(raw["values"]),
                                          "complete": len(raw["pods"]) == len(group["pods"])})
    if result["groups"]:
        (folder / DATA_FILE).write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        if raw_result["groups"]:
            (folder / "kvtest_raw_statistics.json").write_text(
                json.dumps(raw_result, indent=2, ensure_ascii=False), encoding="utf-8")
            columns = ["work_mode", "data_size", "role", "operation", "instance", "label",
                       "success", "min_ms", "avg_ms", *QUANTILES, "max_ms",
                       *[f">{us}us" for us in THRESHOLDS]]
            with (folder / "kvtest_raw_statistics.csv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=columns)
                writer.writeheader()
                for group in raw_result["groups"]:
                    for pod in group["pods"] + [{"instance": "TOTAL", "label": "TOTAL", "statistics": group["total"]}]:
                        writer.writerow({"work_mode": result["work_mode"],
                                         **{k: group[k] for k in ("data_size", "role", "operation")},
                                         "instance": pod["instance"], "label": pod["label"],
                                         **pod["statistics"]})
    return result


def distribution_curve(statistics):
    """Quantile-parametrized approximate CDF, with an explicit logarithmic tail."""
    points = statistics["digest"]["centroids"]
    if not points:
        return [], []
    qs = sorted(set([i / 200 for i in range(200)] +
                    [1 - 10 ** (-i / 20) for i in range(1, 161)]))
    xs = [max(statistics["min_ms"], min(statistics["max_ms"], quantile(points, q))) for q in qs]
    return xs, qs


def render(folder, data=None, out=None):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    folder = Path(folder)
    data = data or prepare(folder)
    out = Path(out) if out else folder / "digest_plots"
    out.mkdir(parents=True, exist_ok=True)
    outputs = []
    for index, group in enumerate(data["groups"]):
        title = " / ".join(str(v) for v in (data.get("work_mode", ""), group["data_size"], group["role"], group["operation"]) if v)
        stem = re.sub(r"[^A-Za-z0-9_.-]", "_", f"{index:03d}.{group['role']}.{group['operation']}.{group['data_size']}")
        pods = group["pods"]
        fig, axes = plt.subplots(4, 1, figsize=(max(10, min(30, len(pods) * .35)), 12), sharex=True)
        for ax, (key, label) in zip(axes, [("avg_ms", "Avg"), ("p99_ms", "P99"), ("p9999_ms", "P99.99"), ("max_ms", "Max")]):
            ax.bar(range(len(pods)), [p["statistics"].get(key, 0) for p in pods])
            ax.axhline(group["total"].get(key, 0), color="red", linestyle="--", label="Merged TOTAL")
            ax.set_ylabel(label + " (ms)")
            ax.grid(axis="y", alpha=.2)
        axes[0].legend()
        axes[-1].set_xticks(range(len(pods)), [p["label"][:36] for p in pods], rotation=70, ha="right")
        fig.suptitle(title + " | digest Pod comparison")
        fig.tight_layout(rect=(0, 0, 1, .97))
        target = out / (stem + ".pods.png")
        fig.savefig(target, dpi=150)
        plt.close(fig)
        outputs.append(str(target))
        for tail in (False, True):
            fig, ax = plt.subplots(figsize=(10, 6))
            for pod in pods:
                if not pod["statistics"].get("success", 0):
                    continue
                xs, qs = distribution_curve(pod["statistics"])
                ax.plot(xs, [1 - q for q in qs] if tail else qs,
                        alpha=.45, linewidth=.8, label=pod["label"] if len(pods) <= 12 else None)
            xs, qs = distribution_curve(group["total"])
            ax.plot(xs, [1 - q for q in qs] if tail else qs, color="black", linewidth=2, label="Merged TOTAL")
            if tail:
                ax.set_yscale("log")
                ax.set_ylim(1e-8, 1)
            else:
                ax.set_ylim(0, 1)
            ax.set_xlabel("Latency (ms)")
            ax.set_ylabel("P(latency > x)" if tail else "P(latency <= x)")
            ax.set_title(title + (" | CCDF (t-digest approximation)" if tail else " | CDF (t-digest approximation)"))
            ax.grid(alpha=.2)
            ax.legend(fontsize="small")
            fig.tight_layout()
            target = out / (stem + (".ccdf.png" if tail else ".cdf.png"))
            fig.savefig(target, dpi=150)
            plt.close(fig)
            outputs.append(str(target))
    return outputs
