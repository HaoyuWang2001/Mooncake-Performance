#!/usr/bin/env python3
"""runs router：/api/run（发起）、/api/runs/{id}（状态/日志/停止）。"""
import os
import sys

from .. import conflicts, runmgr
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
    launch_mode = flags.get("launch_mode") or "single"
    if launch_mode not in ("single", "rank_recall"):
        raise ApiError(400, "flags.launch_mode must be 'single' or 'rank_recall'")
    # mode: "ssh" (default) or "k8s" -> adds --ssh / --k8s to the benchmark argv
    mode = flags.get("mode") or "ssh"
    if mode not in ("ssh", "k8s"):
        raise ApiError(400, "flags.mode must be 'ssh' or 'k8s'")
    mode_dir = "configs/_k8s" if mode == "k8s" else "configs/_ssh"
    if launch_mode == "rank_recall":
        if mode != "k8s":
            raise ApiError(400, "rank_recall launch mode currently requires k8s")
        # Fail before occupying the benchmark slot when the selection does not
        # actually contain one inherited rank config and one recall config.
        try:
            from scripts.run_rank_recall import partition_configs
            partition_configs([os.path.join(ROOT, d) for d in dirs],
                              [os.path.join(ROOT, f) for f in files])
        except Exception as exc:
            raise ApiError(400, "invalid rank_recall selection: {}".format(exc))
        profile = flags.get("profile") or (mode_dir + "/profile.json")
        pairs = (
            ("rank_cluster", flags.get("rank_cluster")),
            ("rank_global", flags.get("rank_global")),
            ("recall_cluster", flags.get("recall_cluster")),
            ("recall_global", flags.get("recall_global")),
            ("profile", profile),
        )
        for name, path in pairs:
            if not path or not os.path.isfile(os.path.join(ROOT, path)):
                raise ApiError(400, "{} config not found: {}".format(
                    name, path or "<empty>"))
        dual_argv = [sys.executable,
                     os.path.join(ROOT, "scripts", "run_rank_recall.py")]
        dual_argv += argv
        dual_argv += ["--rank-cluster", flags["rank_cluster"],
                      "--rank-global", flags["rank_global"],
                      "--recall-cluster", flags["recall_cluster"],
                      "--recall-global", flags["recall_global"],
                      "--profile", profile]
        for flag in ("dry_run", "no_warmup", "no_plot"):
            if flags.get(flag):
                dual_argv.append("--" + flag.replace("_", "-"))
        protocol = flags.get("protocol")
        if protocol:
            if protocol not in ("tcp", "rdma", "ub"):
                raise ApiError(400, "protocol must be tcp|rdma|ub")
            dual_argv += ["--protocol", protocol]
        if flags.get("verbose"):
            dual_argv.append("--verbose")
        return dual_argv
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
    if flags.get("verbose"):
        argv.append("--verbose")
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
    if flags.get("verbose"):
        argv.append("--verbose")
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
    identities = conflicts.run_identities(ROOT, body.get("flags") or {})
    try:
        run = runmgr.MGR.start_benchmark(
            argv, meta={"request": body, "action": "cleanup"},
            identities=identities)
    except runmgr.SlotBusy as exc:
        raise ApiError(409, exc.reason)
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
    identities = conflicts.run_identities(ROOT, body.get("flags") or {})
    try:
        run = runmgr.MGR.start_benchmark(argv, meta={"request": body},
                                         identities=identities)
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


def _read_log_page(path, offset, stream):
    """One page of a run log, optionally filtered to one combined-run stream.

    stream=all  -> raw text page (legacy behaviour)
    stream=rank|recall -> only lines carrying the "[rank] "/"[recall] "
        prefix written by scripts/run_rank_recall.py; the prefix is stripped.

    Pages always end on a line boundary (a trailing partial line is left for
    the next page) so filtered pagination never drops or duplicates lines.
    """
    size = os.path.getsize(path)
    offset = max(0, min(offset, size))
    with open(path, "rb") as f:
        f.seek(offset)
        buf = f.read(MAX_TEXT_CHUNK)
        # a page must contain at least one whole line (unless EOF): extend
        # when the chunk has no newline at all (very long line / tiny chunk)
        while (offset + len(buf) < size and b"\n" not in buf
               and len(buf) < 4 * 1024 * 1024):
            more = f.read(MAX_TEXT_CHUNK)
            if not more:
                break
            buf += more
    consumed = len(buf)
    if offset + consumed < size:
        nl = buf.rfind(b"\n")
        if nl >= 0:
            buf = buf[:nl + 1]
            consumed = len(buf)
    eof = offset + consumed >= size
    text = buf.decode("utf-8", errors="replace")
    if stream in ("rank", "recall"):
        if not eof and b"\n" not in buf:
            # pathological >4MB single line: never emit a truncated prefixed
            # line (would be lost/duplicated); skip this page's bytes
            text = ""
        else:
            prefix = "[{}] ".format(stream)
            kept = [ln[len(prefix):] for ln in text.splitlines()
                    if ln.startswith(prefix)]
            text = ("\n".join(kept) + "\n") if kept else ""
    return {"offset": offset + consumed, "size": size,
            "content": text, "eof": eof}


def api_run_log(handler, query, rid):
    run = runmgr.MGR.get_run(rid)
    if run is None:
        raise ApiError(404, "no such run: {}".format(rid))
    stream = (query.get("stream", ["all"])[0] or "all").lower()
    if stream not in ("all", "rank", "recall"):
        raise ApiError(400, "stream must be all|rank|recall")
    try:
        offset = int(query.get("offset", ["0"])[0])
    except ValueError:
        offset = 0
    if not os.path.isfile(run.log_path):
        return {"offset": 0, "size": 0, "content": "", "eof": True}
    return _read_log_page(run.log_path, offset, stream)


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
