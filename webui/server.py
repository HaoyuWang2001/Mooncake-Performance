#!/usr/bin/env python3
"""webui/server.py — Mooncake-Performance 的本地 Web 界面与 HTTP API。

零第三方依赖（stdlib http.server），双平台（Windows / Linux）。
启动：
    python webui/server.py [--port 8080] [--bind 127.0.0.1]

完整 API 文档见 webui/API.md（也可 GET /api/help 获取，前端 API 页渲染）。
安全模型：本工具面向内网编排机，无鉴权；文件访问限定在白名单根目录
（outputs/、webui/runs/ 可读文件；configs/、schedule/ 可读写 JSON）。
"""
import argparse
import csv
import json
import os
import re
import shutil
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

WEBUI_DIR = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(WEBUI_DIR)
sys.path.insert(0, ROOT)

import runmgr  # noqa: E402
import benchmark as bm  # noqa: E402

MGR = runmgr.RunManager()
BACKUP_DIR = os.path.join(WEBUI_DIR, "backups")
READ_ROOTS = [os.path.join(ROOT, "outputs"), runmgr.RUNS_DIR]
CONF_ROOTS = [os.path.join(ROOT, "configs"), os.path.join(ROOT, "schedule")]
SCHED_DIR = os.path.join(ROOT, "schedule")
STATIC_DIR = os.path.join(WEBUI_DIR, "static")
MAX_TEXT_CHUNK = 512 * 1024
TS_RE = re.compile(r"^[0-9A-Za-z._-]+$")


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _real_under(path, roots):
    rp = os.path.realpath(path)
    for r in roots:
        rr = os.path.realpath(r)
        if rp == rr or rp.startswith(rr + os.sep):
            return True
    return False


def _jpath(query, key, required=True):
    val = query.get(key, [None])[0]
    if not val and required:
        raise ApiError(400, "missing query parameter: {}".format(key))
    return val


def _read_json_body(handler):
    try:
        n = int(handler.headers.get("Content-Length", 0))
        raw = handler.rfile.read(n) if n else b""
        return json.loads(raw.decode("utf-8")) if raw else {}
    except (ValueError, UnicodeDecodeError) as exc:
        raise ApiError(400, "bad JSON body: {}".format(exc))


def _backup(path):
    rel = os.path.relpath(path, ROOT).replace("\\", "/").replace("/", "__")
    dst = os.path.join(BACKUP_DIR, rel + "." + time.strftime("%Y%m%d-%H%M%S"))
    os.makedirs(BACKUP_DIR, exist_ok=True)
    shutil.copy2(path, dst)
    return os.path.relpath(dst, ROOT)


def _atomic_write(path, content):
    tmp = path + ".webui-tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)
    os.replace(tmp, path)


class ApiError(Exception):
    def __init__(self, code, message):
        Exception.__init__(self, message)
        self.code = code
        self.message = message


# ---------------------------------------------------------------------------
# config validation (reuses the repo's own loaders/validators)
# ---------------------------------------------------------------------------

def _merge_chain(cfg_path):
    """Replicate load_scenario_config's _configs.json inheritance for new
    content that has not been written yet (returns merged dict)."""
    cfg = json.loads(open(cfg_path, "r", encoding="utf-8").read()) \
        if os.path.isfile(cfg_path) else {}
    return cfg


def validate_config_content(path, content):
    """-> list of error strings (empty = valid). JSON syntax + semantic
    checks via the repo's own validators (data_size_list / pipeline_plan)."""
    errors = []
    try:
        cfg = json.loads(content)
    except ValueError as exc:
        return ["JSON syntax error: {}".format(exc)]
    if not isinstance(cfg, dict):
        return ["top level must be a JSON object"]
    base = os.path.basename(path)
    if base.startswith("_"):
        # shared-defaults fragment: syntax is all we can check
        return errors
    rp = os.path.realpath(path)
    scenario_dir = None
    for cat in ("basic", "pipeline"):
        cdir = os.path.realpath(os.path.join(bm.CONFIGS_DIR, cat))
        if rp == cdir or rp.startswith(cdir + os.sep):
            scenario_dir = cat
            break
    if scenario_dir is None:
        return errors  # cluster/global/matrix/external: syntax only
    # inheritance: _configs.json chain merged under the new content
    merged = {}
    cfg_dir = os.path.dirname(rp)
    chain = [os.path.realpath(os.path.join(bm.CONFIGS_DIR, scenario_dir))]
    rel = os.path.relpath(cfg_dir, chain[0])
    if rel != ".":
        acc = chain[0]
        for part in rel.split(os.sep):
            acc = os.path.join(acc, part)
            chain.append(acc)
    for level in chain:
        frag = os.path.join(level, "_configs.json")
        if os.path.isfile(frag):
            try:
                bm._deep_merge(merged, json.load(open(frag, "r", encoding="utf-8")))
            except ValueError:
                pass
    bm._deep_merge(merged, cfg)
    # required-field checks (same validators the orchestrator uses)
    try:
        bm.data_size_list(merged)
    except (RuntimeError, KeyError) as exc:
        errors.append(str(exc))
    if scenario_dir == "basic":
        params = merged.get("params")
        if not isinstance(params, list) or not params:
            errors.append("params must be a non-empty array of "
                          "{num_clients, num_threads} objects")
    else:
        try:
            bm.pipeline_plan(merged)
        except (RuntimeError, KeyError, ValueError) as exc:
            errors.append("pipeline plan invalid: {}".format(exc))
    if merged.get("protocol") not in (None, "tcp", "rdma", "ub"):
        errors.append("protocol must be one of tcp/rdma/ub")
    return errors


def validate_plan_content(content):
    """-> list of error strings, using run_schedule's own loaders."""
    errors = []
    tmp = os.path.join(BACKUP_DIR, "plan-validate.tmp.json")
    os.makedirs(BACKUP_DIR, exist_ok=True)
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(content)
        rs = runmgr.load_run_schedule()
        try:
            plan = rs.load_plan(tmp)
        except SystemExit as exc:
            return [str(exc)]
        if plan["tasks"] and rs.parse_trigger(plan["tasks"][0], 1) is None:
            errors.append("the FIRST task must carry date+time")
        if plan["on_fail"] not in ("continue", "abort"):
            errors.append("on_fail must be continue|abort")
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass
    return errors


# ---------------------------------------------------------------------------
# api implementations
# ---------------------------------------------------------------------------

def api_status(handler, query):
    return {"version": "1.0", "platform": runmgr.IS_WIN and "windows" or "posix",
            "python": sys.version.split()[0], "root": ROOT,
            "paths": {"outputs": "outputs", "runs": "webui/runs"},
            **MGR.status()}


def api_help(handler, query):
    path = os.path.join(WEBUI_DIR, "API.md")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return {"markdown": f.read()}
    except OSError:
        return {"markdown": "API.md not found"}


def _config_tree(base_dir, rel_prefix):
    """Recursive config tree node: {name, path, dirs[], files[]}.
    '_'-prefixed fragments are excluded (inheritance fragments, not
    runnable); demo/ is a plain directory like any other."""
    node = {"name": rel_prefix.rstrip("/").split("/")[-1],
            "path": rel_prefix, "dirs": [], "files": []}
    try:
        entries = sorted(os.listdir(base_dir))
    except OSError:
        return node
    for f in entries:
        fp = os.path.join(base_dir, f)
        if os.path.isdir(fp):
            if f.startswith(".") or f == "__pycache__":
                continue
            node["dirs"].append(_config_tree(fp, rel_prefix + f + "/"))
        elif f.endswith(".json") and not f.startswith("_"):
            node["files"].append(rel_prefix + f)
    return node


def _tree_count(node):
    return len(node["files"]) + sum(_tree_count(d) for d in node["dirs"])


def api_configs(handler, query):
    out = {"categories": {}, "special": {}, "cluster_candidates": []}
    for cat in ("basic", "pipeline"):
        cdir = os.path.join(bm.CONFIGS_DIR, cat)
        tree = _config_tree(cdir, "configs/" + cat + "/") if os.path.isdir(cdir) else None
        out["categories"][cat] = {
            "tree": tree,
            "config_count": _tree_count(tree) if tree else 0,
        }
    for name in ("cluster.json", "global.json", "matrix.json"):
        p = os.path.join(bm.CONFIGS_DIR, name)
        out["special"][name[:-5]] = {
            "path": "configs/" + name,
            "present": os.path.isfile(p),
        }
    for cand in ("configs/cluster.json", "configs/cluster.example.json",
                 "mock/cluster.json"):
        if os.path.isfile(os.path.join(ROOT, cand)):
            out["cluster_candidates"].append(cand)
    return out


def api_config_get(handler, query):
    path = _jpath(query, "path")
    full = os.path.join(ROOT, path)
    if not _real_under(full, CONF_ROOTS) or not os.path.isfile(full):
        raise ApiError(404, "config not found (allowed roots: configs/, schedule/): {}".format(path))
    with open(full, "r", encoding="utf-8", errors="replace") as f:
        return {"path": path, "content": f.read()}


def api_config_put(handler, query):
    body = _read_json_body(handler)
    path = body.get("path", "")
    content = body.get("content", "")
    full = os.path.join(ROOT, path)
    if not _real_under(full, CONF_ROOTS):
        raise ApiError(400, "path outside allowed roots (configs/, schedule/): {}".format(path))
    if not path.endswith(".json"):
        raise ApiError(400, "only .json files are editable")
    errors = validate_config_content(full, content)
    if errors:
        raise ApiError(422, "; ".join(errors))
    backup = None
    if os.path.isfile(full):
        backup = _backup(full)
    _atomic_write(full, content)
    return {"ok": True, "path": path, "backup": backup}


def api_config_validate(handler, query):
    body = _read_json_body(handler)
    path = body.get("path", "")
    full = os.path.join(ROOT, path)
    if not _real_under(full, CONF_ROOTS):
        raise ApiError(400, "path outside allowed roots: {}".format(path))
    return {"ok_path": os.path.isfile(full), "errors": validate_config_content(full, body.get("content", ""))}


def _build_run_argv(body):
    sel = body.get("selector") or {}
    flags = body.get("flags") or {}
    typ = sel.get("type", "file")
    path = sel.get("path", "")
    full = os.path.join(ROOT, path)
    argv = []
    if typ == "file":
        if not os.path.isfile(full):
            raise ApiError(400, "config file not found: {}".format(path))
        argv += ["-f", path]
    elif typ == "dir":
        # --config-dir always scans recursively (demo/ included,
        # _-prefixed fragments skipped)
        if not os.path.isdir(full):
            raise ApiError(400, "config dir not found: {}".format(path))
        argv += ["--config-dir", path]
    else:
        raise ApiError(400, "selector.type must be file or dir")
    cluster = flags.get("cluster") or "configs/cluster.json"
    if not os.path.isfile(os.path.join(ROOT, cluster)):
        raise ApiError(400, "cluster config not found: {}".format(cluster))
    global_cfg = flags.get("global") or "configs/global.json"
    if not os.path.isfile(os.path.join(ROOT, global_cfg)):
        raise ApiError(400, "global config not found: {}".format(global_cfg))
    argv += ["--cluster", cluster, "--global", global_cfg]
    for flag in ("dry_run", "no_warmup", "no_plot"):
        if flags.get(flag):
            argv.append("--" + flag.replace("_", "-"))
    protocol = flags.get("protocol")
    if protocol:
        if protocol not in ("tcp", "rdma", "ub"):
            raise ApiError(400, "protocol must be tcp|rdma|ub")
        argv += ["--protocol", protocol]
    profile = flags.get("profile")
    if profile and profile != "auto":
        if not os.path.isfile(os.path.join(ROOT, profile)):
            raise ApiError(400, "profile matrix not found: {}".format(profile))
        argv += ["--profile", profile]
    return [sys.executable, runmgr.BENCH] + argv


def api_run_post(handler, query):
    body = _read_json_body(handler)
    argv = _build_run_argv(body)
    warnings = []
    sch = MGR.scheduler
    if sch is not None and sch.state in ("waiting", "waiting_slot"):
        warnings.append(
            "scheduler active (plan {}, next task @ {}) - its task may "
            "collide with this run".format(
                os.path.basename(sch.plan_path), sch.next_trigger))
    try:
        run = MGR.start_benchmark(argv, meta={"request": body})
    except runmgr.SlotBusy as exc:
        raise ApiError(409, exc.reason)
    return {"run": run.to_dict(), "warnings": warnings}


def api_runs(handler, query):
    return {"runs": [r.to_dict() for r in MGR.history[:60]]}


def api_run_get(handler, query, rid):
    run = MGR.get_run(rid)
    if run is None:
        raise ApiError(404, "no such run: {}".format(rid))
    return run.to_dict()


def api_run_log(handler, query, rid):
    run = MGR.get_run(rid)
    if run is None:
        raise ApiError(404, "no such run: {}".format(rid))
    try:
        offset = int(query.get("offset", ["0"])[0])
    except ValueError:
        offset = 0
    if not os.path.isfile(run.log_path):
        return {"offset": 0, "size": 0, "content": "", "eof": True}
    size = os.path.getsize(run.log_path)
    offset = max(0, min(offset, size))
    with open(run.log_path, "rb") as f:
        f.seek(offset)
        chunk = f.read(MAX_TEXT_CHUNK)
    new_off = offset + len(chunk)
    return {"offset": new_off, "size": size,
            "content": chunk.decode("utf-8", errors="replace"),
            "eof": new_off >= size}


def api_run_stop(handler, query, rid):
    run, err = MGR.stop_run(rid)
    if err:
        raise ApiError(404, err)
    return {"ok": True, "run": run.to_dict()}


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
                "mtime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(os.path.getmtime(d))),
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
    path = _jpath(query, "path")
    full = os.path.join(ROOT, path)
    if not _real_under(full, READ_ROOTS):
        raise ApiError(403, "path outside allowed roots (outputs/, webui/runs/): {}".format(path))
    return path, full


CONTENT_TYPES = {".png": "image/png", ".jpg": "image/jpeg", ".gif": "image/gif",
                 ".html": "text/html", ".md": "text/plain", ".log": "text/plain"}


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
    truncated = sum(1 for _ in open(full, "r", encoding="utf-8", errors="replace")) - 1 > max_rows
    return {"headers": headers, "rows": rows, "truncated": truncated}


def api_schedule_plans(handler, query):
    plans = []
    if os.path.isdir(SCHED_DIR):
        for f in sorted(os.listdir(SCHED_DIR)):
            if not f.endswith(".json"):
                continue
            p = os.path.join(SCHED_DIR, f)
            entry = {"name": f, "example": f == "plan.example.json"}
            try:
                rs = runmgr.load_run_schedule()
                plan = rs.load_plan(p)
                entry["tasks"] = len(plan["tasks"])
                trig = rs.parse_trigger(plan["tasks"][0], 1)
                entry["first_task"] = trig.strftime("%Y-%m-%d %H:%M:%S") if trig else None
                entry["valid"] = True
            except SystemExit as exc:
                entry["valid"] = False
                entry["error"] = str(exc)
            except Exception as exc:
                entry["valid"] = False
                entry["error"] = str(exc)
            plans.append(entry)
    return {"plans": plans}


def api_schedule_plan_get(handler, query, name):
    if not name.endswith(".json") or "/" in name or "\\" in name:
        raise ApiError(400, "bad plan name")
    full = os.path.join(SCHED_DIR, name)
    if not os.path.isfile(full):
        raise ApiError(404, "no such plan: {}".format(name))
    with open(full, "r", encoding="utf-8", errors="replace") as f:
        return {"name": name, "content": f.read()}


def api_schedule_plan_put(handler, query, name):
    if not name.endswith(".json") or "/" in name or "\\" in name:
        raise ApiError(400, "bad plan name")
    if name == "plan.example.json":
        raise ApiError(400, "the example plan is read-only")
    body = _read_json_body(handler)
    content = body.get("content", "")
    errors = validate_plan_content(content)
    if errors:
        raise ApiError(422, "; ".join(errors))
    full = os.path.join(SCHED_DIR, name)
    backup = _backup(full) if os.path.isfile(full) else None
    _atomic_write(full, content)
    return {"ok": True, "name": name, "backup": backup}


def api_schedule_start(handler, query):
    body = _read_json_body(handler)
    name = body.get("plan", "")
    if not name.endswith(".json"):
        raise ApiError(400, "plan must be a .json file name under schedule/")
    full = os.path.join(SCHED_DIR, name)
    if not os.path.isfile(full):
        raise ApiError(404, "no such plan: {}".format(name))
    try:
        sch = MGR.start_scheduler(full)
    except runmgr.SlotBusy as exc:
        raise ApiError(409, exc.reason)
    except SystemExit as exc:
        raise ApiError(422, str(exc))
    return {"scheduler": sch.to_dict()}


def api_schedule_stop(handler, query):
    sch, err = MGR.stop_scheduler()
    if err:
        raise ApiError(409, err)
    return {"ok": True, "scheduler": sch.to_dict()}


def api_schedule_status(handler, query):
    return {"scheduler": MGR.scheduler.to_dict() if MGR.scheduler else None}


# ---------------------------------------------------------------------------
# http plumbing
# ---------------------------------------------------------------------------

ROUTES = [
    ("GET", r"^/api/status$", lambda h, q, m=None: api_status(h, q)),
    ("GET", r"^/api/help$", lambda h, q, m=None: api_help(h, q)),
    ("GET", r"^/api/configs$", lambda h, q, m=None: api_configs(h, q)),
    ("GET", r"^/api/config$", api_config_get),
    ("PUT", r"^/api/config$", lambda h, q, m=None: api_config_put(h, q)),
    ("POST", r"^/api/config/validate$", lambda h, q, m=None: api_config_validate(h, q)),
    ("POST", r"^/api/run$", lambda h, q, m=None: api_run_post(h, q)),
    ("GET", r"^/api/runs$", lambda h, q, m=None: api_runs(h, q)),
    ("GET", r"^/api/runs/(?P<rid>[^/]+)$", api_run_get),
    ("GET", r"^/api/runs/(?P<rid>[^/]+)/log$", api_run_log),
    ("POST", r"^/api/runs/(?P<rid>[^/]+)/stop$", api_run_stop),
    ("GET", r"^/api/outputs$", lambda h, q, m=None: api_outputs(h, q)),
    ("GET", r"^/api/outputs/(?P<ts>[^/]+)/tree$", api_output_tree),
    ("GET", r"^/api/file$", api_file),
    ("GET", r"^/api/csv$", api_csv),
    ("GET", r"^/api/schedule/plans$", lambda h, q, m=None: api_schedule_plans(h, q)),
    ("GET", r"^/api/schedule/plan/(?P<name>[^/]+)$", api_schedule_plan_get),
    ("PUT", r"^/api/schedule/plan/(?P<name>[^/]+)$", api_schedule_plan_put),
    ("POST", r"^/api/schedule/start$", lambda h, q, m=None: api_schedule_start(h, q)),
    ("POST", r"^/api/schedule/stop$", lambda h, q, m=None: api_schedule_stop(h, q)),
    ("GET", r"^/api/schedule/status$", lambda h, q, m=None: api_schedule_status(h, q)),
]


class Handler(BaseHTTPRequestHandler):
    server_version = "MooncakeWebUI/1.0"

    def log_message(self, fmt, *args):
        sys.stderr.write("[webui] {} {}\n".format(
            self.address_string(), fmt % args))

    # -- responses ----------------------------------------------------------

    def _send_json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_raw(self, data, ctype):
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _send_static(self, name):
        # normalize: only relative subpaths of webui/static/ are served
        name = (name or "").replace("\\", "/").lstrip("/")
        full = os.path.realpath(os.path.join(STATIC_DIR, name))
        inside = full.startswith(os.path.realpath(STATIC_DIR) + os.sep)
        if not inside or not os.path.isfile(full):
            self._send_json({"error": "not found: " + name}, 404)
            return
        ext = os.path.splitext(full)[1].lower()
        ctype = {".html": "text/html; charset=utf-8",
                 ".js": "application/javascript; charset=utf-8",
                 ".mjs": "application/javascript; charset=utf-8",
                 ".css": "text/css; charset=utf-8",
                 ".json": "application/json; charset=utf-8",
                 ".map": "application/json; charset=utf-8",
                 ".svg": "image/svg+xml",
                 ".png": "image/png",
                 ".woff2": "font/woff2"}.get(ext, "application/octet-stream")
        with open(full, "rb") as f:
            self._send_raw(f.read(), ctype)

    # -- dispatch -----------------------------------------------------------

    def _dispatch(self, method):
        parsed = self.path.split("?", 1)
        path = parsed[0]
        query = {}
        if len(parsed) > 1:
            for kv in parsed[1].split("&"):
                if "=" in kv:
                    k, v = kv.split("=", 1)
                    from urllib.parse import unquote_plus
                    query.setdefault(unquote_plus(k), []).append(unquote_plus(v))
        if path == "/" or path == "/index.html":
            if method == "GET":
                return self._send_static("index.html")
        if path.startswith("/static/"):
            if method == "GET":
                return self._send_static(path[len("/static/"):])
        if path.startswith("/api/"):
            for m, pattern, fn in ROUTES:
                if m != method:
                    continue
                match = re.match(pattern, path)
                if match:
                    try:
                        result = fn(self, query, **match.groupdict())
                    except ApiError as exc:
                        return self._send_json({"error": exc.message}, exc.code)
                    except runmgr.SlotBusy as exc:
                        return self._send_json({"error": exc.reason}, 409)
                    except Exception as exc:  # never crash the server
                        return self._send_json({"error": "internal: {}".format(exc)}, 500)
                    if result is not None:
                        return self._send_json(result)
                    return
            return self._send_json({"error": "no such api: {} {}".format(method, path)}, 404)
        self._send_json({"error": "not found"}, 404)

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def do_PUT(self):
        self._dispatch("PUT")


def main():
    parser = argparse.ArgumentParser(description="Mooncake-Performance WebUI")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--bind", default="127.0.0.1",
                        help="bind address (use 0.0.0.0 to expose on LAN)")
    args = parser.parse_args()
    httpd = ThreadingHTTPServer((args.bind, args.port), Handler)
    print("[webui] serving on http://{}:{}/  (root: {})".format(
        args.bind, args.port, ROOT), flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("[webui] stopped")


if __name__ == "__main__":
    main()
