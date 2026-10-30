#!/usr/bin/env python3
"""outputs router：/api/outputs（结果列表/tree）、/api/file、/api/csv。"""
import csv
import os
import time

from ..common import ApiError, jpath, real_under
from ..paths import MAX_TEXT_CHUNK, READ_ROOTS, ROOT, TS_RE

ROUTES = []

CONTENT_TYPES = {".png": "image/png", ".jpg": "image/jpeg", ".gif": "image/gif",
                 ".html": "text/html", ".md": "text/plain", ".log": "text/plain"}


def api_outputs(handler, query):
    out_dir = os.path.join(ROOT, "outputs")
    items = []
    if os.path.isdir(out_dir):
        for ts in os.listdir(out_dir):
            d = os.path.join(out_dir, ts)
            if not os.path.isdir(d):
                continue
            csvs = [f for f in os.listdir(d) if f.endswith(".csv")]
            items.append({
                "ts": ts,
                "mtime": time.strftime("%Y-%m-%d %H:%M:%S",
                                       time.localtime(os.path.getmtime(d))),
                "has_detail": "benchmark_detail.csv" in csvs,
                "csv_count": len(csvs),
            })
    items.sort(key=lambda x: x["ts"], reverse=True)
    return {"outputs": items}


def api_output_tree(handler, query, ts):
    if not TS_RE.match(ts):
        raise ApiError(400, "bad ts")
    d = os.path.join(ROOT, "outputs", ts)
    if not os.path.isdir(d):
        raise ApiError(404, "no such output run: {}".format(ts))
    files = []
    for root, _dirs, fs in os.walk(d):
        for f in fs:
            files.append(os.path.relpath(os.path.join(root, f), d).replace("\\", "/"))
    files.sort()
    return {"ts": ts, "files": files}


def _guarded_file(query):
    path = jpath(query, "path")
    full = os.path.join(ROOT, path)
    if not real_under(full, READ_ROOTS):
        raise ApiError(403, "path outside allowed roots "
                            "(outputs/, webui/data/runs/): {}".format(path))
    return path, full


def api_file(handler, query):
    path, full = _guarded_file(query)
    mode = (query.get("mode", ["text"])[0] or "text").lower()
    if not os.path.isfile(full):
        raise ApiError(404, "file not found: {}".format(path))
    if mode == "raw":
        ext = os.path.splitext(full)[1].lower()
        handler._send_raw(open(full, "rb").read(),
                          CONTENT_TYPES.get(ext, "application/octet-stream"))
        return None
    try:
        offset = int(query.get("offset", ["0"])[0])
    except ValueError:
        offset = 0
    size = os.path.getsize(full)
    tail = query.get("tail", [None])[0]
    if tail:
        try:
            n = int(tail)
            with open(full, "rb") as f:
                if size > n:
                    f.seek(size - n)
                data = f.read()
            return {"content": data.decode("utf-8", errors="replace"),
                    "offset": size, "size": size}
        except ValueError:
            pass
    offset = max(0, min(offset, size))
    with open(full, "rb") as f:
        f.seek(offset)
        chunk = f.read(MAX_TEXT_CHUNK)
    return {"content": chunk.decode("utf-8", errors="replace"),
            "offset": offset + len(chunk), "size": size,
            "eof": offset + len(chunk) >= size}


def api_csv(handler, query):
    path, full = _guarded_file(query)
    if not os.path.isfile(full):
        raise ApiError(404, "file not found: {}".format(path))
    try:
        max_rows = int(query.get("max_rows", ["5000"])[0])
    except ValueError:
        max_rows = 5000
    rows = []
    with open(full, "r", encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f)
        headers = next(reader, [])
        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            rows.append(row)
    truncated = sum(1 for _ in open(full, "r", encoding="utf-8",
                                    errors="replace")) - 1 > max_rows
    return {"headers": headers, "rows": rows, "truncated": truncated}


ROUTES += [
    ("GET", r"^/api/outputs$", lambda h, q, m=None: api_outputs(h, q)),
    ("GET", r"^/api/outputs/(?P<ts>[^/]+)/tree$", api_output_tree),
    ("GET", r"^/api/file$", api_file),
    ("GET", r"^/api/csv$", api_csv),
]
