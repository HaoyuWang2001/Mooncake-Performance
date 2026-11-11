#!/usr/bin/env python3
"""outputs router：/api/outputs（结果列表/tree）、/api/file、/api/csv。"""
import csv
import json
import os
import shutil
import time

from ..common import ApiError, jpath, read_json_body, real_under
from ..paths import (MAX_TEXT_CHUNK, OUTPUTS_DIR, OUTPUTS_TRASH_DIR,
                     READ_ROOTS, ROOT, RUNS_DIR, TS_RE)

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


def _output_dir(ts):
    """校验并返回 outputs/<ts> 目录（防穿越 / 越界）。"""
    if not ts or ts in (".", "..") or ts.startswith(".") or not TS_RE.match(ts):
        raise ApiError(400, "bad ts")
    d = os.path.join(OUTPUTS_DIR, ts)
    if not real_under(d, [OUTPUTS_DIR]):
        raise ApiError(403, "path outside outputs/")
    if not os.path.isdir(d):
        raise ApiError(404, "no such output run: {}".format(ts))
    return d


def _move_to_trash(src, ts):
    """把结果目录移入回收站（webui/backups/outputs-trash/），返回相对路径。"""
    os.makedirs(OUTPUTS_TRASH_DIR, exist_ok=True)
    dst = os.path.join(OUTPUTS_TRASH_DIR, ts)
    if os.path.exists(dst):
        base = dst + "." + time.strftime("%Y%m%d-%H%M%S")
        dst = base
        n = 1
        while os.path.exists(dst):
            dst = "{}-{}".format(base, n)
            n += 1
    shutil.move(src, dst)
    return os.path.relpath(dst, ROOT).replace("\\", "/")


def _update_run_ts_dir(old_ts, new_ts):
    """同步 run 历史中的 ts_dir 引用（new_ts=None 表示结果已删除）。"""
    old_rel = "outputs/" + old_ts
    new_rel = ("outputs/" + new_ts) if new_ts else None
    if not os.path.isdir(RUNS_DIR):
        return
    for name in os.listdir(RUNS_DIR):
        if not name.endswith(".json"):
            continue
        fp = os.path.join(RUNS_DIR, name)
        try:
            with open(fp, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except (OSError, ValueError):
            continue
        if meta.get("ts_dir") != old_rel:
            continue
        meta["ts_dir"] = new_rel
        try:
            tmp = fp + ".webui-tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=1)
            os.replace(tmp, fp)
        except OSError:
            pass


def api_output_delete(handler, query, ts):
    d = _output_dir(ts)
    trash = _move_to_trash(d, ts)
    _update_run_ts_dir(ts, None)
    return {"ok": True, "ts": ts, "trash": trash}


def api_outputs_batch_delete(handler, query):
    body = read_json_body(handler)
    names = body.get("names") or []
    if not isinstance(names, list) or not names:
        raise ApiError(400, "names must be a non-empty array")
    deleted, failed = [], []
    for ts in names:
        try:
            d = _output_dir(ts)
            _move_to_trash(d, ts)
            _update_run_ts_dir(ts, None)
            deleted.append(ts)
        except ApiError as exc:
            failed.append({"ts": ts, "error": exc.message})
        except OSError as exc:
            failed.append({"ts": ts, "error": str(exc)})
    return {"deleted": deleted, "failed": failed}


def api_output_rename(handler, query, ts):
    src = _output_dir(ts)
    body = read_json_body(handler)
    name = (body.get("name") or "").strip()
    if (not name or name in (".", "..") or name.startswith(".")
            or not TS_RE.match(name)):
        raise ApiError(400, "bad new name (allowed: letters, digits, . _ -)")
    if name == ts:
        return {"ok": True, "ts": ts, "renamed": False}
    dst = os.path.join(OUTPUTS_DIR, name)
    if not real_under(dst, [OUTPUTS_DIR]):
        raise ApiError(403, "path outside outputs/")
    if os.path.exists(dst):
        raise ApiError(409, "target already exists: {}".format(name))
    try:
        os.rename(src, dst)
    except OSError as exc:
        raise ApiError(400, "rename failed: {}".format(exc))
    _update_run_ts_dir(ts, name)
    return {"ok": True, "ts": name, "from": ts, "renamed": True}


def _guarded_file(query):
    path = jpath(query, "path")
    full = os.path.join(ROOT, path)
    if not real_under(full, READ_ROOTS):
        raise ApiError(403, "path outside allowed roots "
                            "(outputs/, webui/data/runs/): {}".format(path))
    return path, full


def _read_text_page(full, offset):
    """Read one MAX_TEXT_CHUNK page at `offset`; returns the paged payload
    {content, offset, size, eof} consumed by the WebUI pager.

    A UTF-8 character split at the chunk seam is completed by reading up to
    4 extra bytes, so concatenating pages reproduces the file exactly (no
    replacement chars, no skipped bytes) for any chunk size."""
    size = os.path.getsize(full)
    offset = max(0, min(offset, size))
    with open(full, "rb") as f:
        f.seek(offset)
        chunk = f.read(MAX_TEXT_CHUNK)
        base = len(chunk)
        more = (offset + base) < size
        if more and chunk:
            chunk += f.read(4)   # enough to finish any split UTF-8 char
    if more and chunk:
        # consume at least the whole page; tolerate the trailing extra bytes
        # starting a new character (they are re-read on the next page)
        for cut in range(len(chunk), base - 1, -1):
            try:
                text = chunk[:cut].decode("utf-8")
                new_off = offset + cut
                return {"content": text, "offset": new_off, "size": size,
                        "eof": new_off >= size}
            except UnicodeDecodeError:
                continue
    new_off = offset + len(chunk)
    return {"content": chunk.decode("utf-8", errors="replace"),
            "offset": new_off, "size": size, "eof": new_off >= size}


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
    return _read_text_page(full, offset)



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
    ("POST", r"^/api/outputs/delete$",
     lambda h, q, m=None: api_outputs_batch_delete(h, q)),
    ("GET", r"^/api/outputs/(?P<ts>[^/]+)/tree$", api_output_tree),
    ("POST", r"^/api/outputs/(?P<ts>[^/]+)/rename$", api_output_rename),
    ("DELETE", r"^/api/outputs/(?P<ts>[^/]+)$", api_output_delete),
    ("GET", r"^/api/file$", api_file),
    ("GET", r"^/api/csv$", api_csv),
]
