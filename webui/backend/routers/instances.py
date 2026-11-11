#!/usr/bin/env python3
"""instances router：实例的创建/列表/详情/运行/停止/改名/删除。

创建实例只做表单快照（selector + flags），不占用运行槽；运行产生的 run 因
meta.instance 归属该实例，日志/停止/并发守卫沿用 runs 的机制。
"""
import os

from .. import conflicts, instances, runmgr
from ..common import ApiError, read_json_body
from ..paths import ROOT
from .runs import _build_run_argv

ROUTES = []


def _active_run(iid):
    for r in list(runmgr.MGR.active.values()):
        if (r.meta or {}).get("instance") == iid:
            return r
    return None


def _summary(inst):
    iid = inst.get("id")
    active = _active_run(iid)
    latest = active.to_dict() if active else instances.latest_run(iid)
    status = (latest or {}).get("status") or "created"
    req = inst.get("request") or {}
    flags = req.get("flags") or {}
    sel = req.get("selector") or {}
    return {
        "id": iid,
        "name": inst.get("name") or iid,
        "created": inst.get("created"),
        "launch_mode": flags.get("launch_mode") or "single",
        "mode": flags.get("mode") or "ssh",
        "status": status,
        "stopping": bool((latest or {}).get("stopping")),
        "last_run": ({k: latest.get(k) for k in
                      ("id", "status", "rc", "t0", "t1", "elapsed",
                       "ts_dir", "stopping", "cmd")}
                     if latest else None),
        "flags": flags,
        "selection": {"dirs": sel.get("dirs") or [],
                      "files": sel.get("files") or []},
    }


def _load(iid):
    inst = instances.load(iid)
    if inst is None:
        raise ApiError(404, "no such instance: {}".format(iid))
    return inst


def api_instances(handler, query):
    return {"instances": [_summary(i) for i in instances.list_all()]}


def api_instance_create(handler, query):
    body = read_json_body(handler)
    # Reuse the run-argv builder for full validation (config existence,
    # selector shape, launch_mode rules) WITHOUT spawning anything.
    _build_run_argv(body)
    request = {"selector": body.get("selector") or {},
               "flags": body.get("flags") or {}}
    inst = instances.create(request, body.get("name"))
    return {"instance": _summary(inst)}


def api_instance_get(handler, query, iid):
    inst = _load(iid)
    active = _active_run(iid)
    runs = instances.runs_for(iid)
    if active is not None:
        latest = active.to_dict()
        runs = [r for r in runs if r.get("id") != latest["id"]]
        runs.insert(0, {
            "id": latest["id"], "status": latest["status"],
            "rc": latest.get("rc"), "t0": latest.get("t0"),
            "t1": latest.get("t1"), "elapsed": latest.get("elapsed"),
            "ts_dir": latest.get("ts_dir"), "stopping": latest.get("stopping"),
            "cmd": latest.get("cmd"),
            "meta": latest.get("meta")})
    return {"instance": _summary(inst), "runs": runs}


def api_instance_run(handler, query, iid):
    inst = _load(iid)
    if _active_run(iid) is not None:
        raise ApiError(409, "实例 {} 正在运行，先停止再运行".format(
            inst.get("name") or iid))
    request = inst.get("request") or {}
    argv = _build_run_argv(request)
    identities = conflicts.run_identities(ROOT, request.get("flags") or {})
    warnings = []
    sch = runmgr.MGR.scheduler
    if sch is not None and sch.state in ("waiting", "waiting_slot"):
        warnings.append(
            "scheduler active (plan {}, next task @ {}) - its task may "
            "collide with this run".format(
                os.path.basename(sch.plan_path), sch.next_trigger))
    try:
        run = runmgr.MGR.start_benchmark(
            argv, meta={"request": request, "instance": iid,
                        "instance_name": inst.get("name") or iid},
            identities=identities)
    except runmgr.SlotBusy as exc:
        raise ApiError(409, exc.reason)
    return {"run": run.to_dict(), "warnings": warnings}


def api_instance_stop(handler, query, iid):
    _load(iid)
    run = _active_run(iid)
    if run is None:
        raise ApiError(409, "实例未在运行")
    run, err = runmgr.MGR.stop_run(run.id)
    if err:
        raise ApiError(404, err)
    return {"ok": True, "run": run.to_dict()}


def api_instance_delete(handler, query, iid):
    _load(iid)
    if _active_run(iid) is not None:
        raise ApiError(409, "实例正在运行，先停止再删除")
    if not instances.delete(iid):
        raise ApiError(404, "no such instance: {}".format(iid))
    return {"ok": True, "deleted": iid}


def api_instance_rename(handler, query, iid):
    _load(iid)
    body = read_json_body(handler)
    inst = instances.rename(iid, body.get("name"))
    if inst is None:
        raise ApiError(404, "no such instance: {}".format(iid))
    return {"instance": _summary(inst)}


ROUTES += [
    ("GET", r"^/api/instances$", api_instances),
    ("POST", r"^/api/instances$", api_instance_create),
    ("GET", r"^/api/instances/(?P<iid>[^/]+)$", api_instance_get),
    ("DELETE", r"^/api/instances/(?P<iid>[^/]+)$", api_instance_delete),
    ("POST", r"^/api/instances/(?P<iid>[^/]+)/run$", api_instance_run),
    ("POST", r"^/api/instances/(?P<iid>[^/]+)/stop$", api_instance_stop),
    ("POST", r"^/api/instances/(?P<iid>[^/]+)/rename$", api_instance_rename),
]
