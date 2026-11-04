#!/usr/bin/env python3
"""runs router：/api/run（发起）、/api/runs/{id}（状态/日志/停止）。"""
import os
import sys

from .. import runmgr
from ..common import ApiError, read_json_body
from ..paths import MAX_TEXT_CHUNK, ROOT

ROUTES = []


def _build_run_argv(body):
    sel = body.get("selector") or {}
    flags = body.get("flags") or {}
    if sel.get("type") or sel.get("path"):
        raise ApiError(400, "legacy {type,path} selector removed; "
                            "use {\"dirs\": [...], \"files\": [...]}")
    dirs = sel.get("dirs") or []
    files = sel.get("files") or []
    if not isinstance(dirs, list) or not isinstance(files, list):
        raise ApiError(400, "selector.dirs/files must be arrays of paths")
    if not dirs and not files:
        raise ApiError(400, "selector needs at least one dir or file")
    argv = []
    # config-dir entries run first, config files follow (benchmark.py order)
    for d in dirs:
        if not os.path.isdir(os.path.join(ROOT, d)):
            raise ApiError(400, "config dir not found: {}".format(d))
    for f in files:
        if not os.path.isfile(os.path.join(ROOT, f)):
            raise ApiError(400, "config file not found: {}".format(f))
    if dirs:
        argv += ["--config-dir", ",".join(dirs)]
    if files:
        argv += ["-f", ",".join(files)]
    # mode: "ssh" (default) or "k8s" -> adds --ssh / --k8s to the benchmark argv
    mode = flags.get("mode") or "ssh"
    if mode not in ("ssh", "k8s"):
        raise ApiError(400, "flags.mode must be 'ssh' or 'k8s'")
    mode_dir = "configs/_k8s" if mode == "k8s" else "configs/_ssh"
    cluster = flags.get("cluster") or (mode_dir + "/cluster.json")
    global_cfg = flags.get("global") or (mode_dir + "/global.json")
    profile = flags.get("profile") or (mode_dir + "/profile.json")
    for name, path in (("cluster", cluster), ("global", global_cfg),
                       ("profile", profile)):
        if not os.path.isfile(os.path.join(ROOT, path)):
            example = mode_dir + "/" + name + ".example.json"
            target = mode_dir + "/" + name + ".json"
            raise ApiError(
                400, "{} config not found: {}. copy {} to {} and edit it "
                     "first".format(name, path, example, target))
    argv += ["--" + mode, "--cluster", cluster, "--global", global_cfg,
             "--profile", profile]
    for flag in ("dry_run", "no_warmup", "no_plot"):
        if flags.get(flag):
            argv.append("--" + flag.replace("_", "-"))
    protocol = flags.get("protocol")
    if protocol:
        if protocol not in ("tcp", "rdma", "ub"):
            raise ApiError(400, "protocol must be tcp|rdma|ub")
        argv += ["--protocol", protocol]
    return [sys.executable, runmgr.BENCH] + argv


def _build_cleanup_argv(body):
    flags = body.get("flags") or {}
    mode = flags.get("mode") or "ssh"
    if mode not in ("ssh", "k8s"):
        raise ApiError(400, "flags.mode must be 'ssh' or 'k8s'")
    mode_dir = "configs/_k8s" if mode == "k8s" else "configs/_ssh"
    cluster = flags.get("cluster") or (mode_dir + "/cluster.json")
    global_cfg = flags.get("global") or (mode_dir + "/global.json")
    for name, path in (("cluster", cluster), ("global", global_cfg)):
        if not os.path.isfile(os.path.join(ROOT, path)):
            example = mode_dir + "/" + name + ".example.json"
            target = mode_dir + "/" + name + ".json"
            raise ApiError(
                400, "{} config not found: {}. copy {} to {} and edit it "
                     "first".format(name, path, example, target))
    # standalone clean-up does not need profile/scenario configs
    argv = ["--" + mode, "--cluster", cluster, "--global", global_cfg,
            "--clean-up"]
    return [sys.executable, runmgr.BENCH] + argv


def api_cleanup_post(handler, query):
    body = read_json_body(handler)
    argv = _build_cleanup_argv(body)
    warnings = []
    sch = runmgr.MGR.scheduler
    if sch is not None and sch.state in ("waiting", "waiting_slot"):
        warnings.append(
            "scheduler active (plan {}, next task @ {}) - its task may "
            "collide with this cleanup".format(
                os.path.basename(sch.plan_path), sch.next_trigger))
    run = runmgr.MGR.start_benchmark(argv, meta={"request": body,
                                                 "action": "cleanup"})
    return {"run": run.to_dict(), "warnings": warnings}


def api_run_post(handler, query):
    body = read_json_body(handler)
    argv = _build_run_argv(body)
    warnings = []
    sch = runmgr.MGR.scheduler
    if sch is not None and sch.state in ("waiting", "waiting_slot"):
        warnings.append(
            "scheduler active (plan {}, next task @ {}) - its task may "
            "collide with this run".format(
                os.path.basename(sch.plan_path), sch.next_trigger))
    try:
        run = runmgr.MGR.start_benchmark(argv, meta={"request": body})
    except runmgr.SlotBusy as exc:
        raise ApiError(409, exc.reason)
    return {"run": run.to_dict(), "warnings": warnings}


def api_runs(handler, query):
    return {"runs": [r.to_dict() for r in runmgr.MGR.history[:60]]}


def api_run_get(handler, query, rid):
    run = runmgr.MGR.get_run(rid)
    if run is None:
        raise ApiError(404, "no such run: {}".format(rid))
    return run.to_dict()


def api_run_log(handler, query, rid):
    run = runmgr.MGR.get_run(rid)
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
    run, err = runmgr.MGR.stop_run(rid)
    if err:
        raise ApiError(404, err)
    return {"ok": True, "run": run.to_dict()}


ROUTES += [
    ("POST", r"^/api/run$", lambda h, q, m=None: api_run_post(h, q)),
    ("POST", r"^/api/cleanup$", lambda h, q, m=None: api_cleanup_post(h, q)),
    ("GET", r"^/api/runs$", lambda h, q, m=None: api_runs(h, q)),
    ("GET", r"^/api/runs/(?P<rid>[^/]+)$", api_run_get),
    ("GET", r"^/api/runs/(?P<rid>[^/]+)/log$", api_run_log),
    ("POST", r"^/api/runs/(?P<rid>[^/]+)/stop$", api_run_stop),
]
