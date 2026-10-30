#!/usr/bin/env python3
"""schedule router：/api/schedule/*（计划管理与 scheduler 启停/状态）。"""
import os

from .. import runmgr
from ..common import ApiError, atomic_write, backup_file, read_json_body
from ..paths import SCHED_DIR

ROUTES = []


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
    from .configs import validate_plan_content
    if not name.endswith(".json") or "/" in name or "\\" in name:
        raise ApiError(400, "bad plan name")
    if name == "plan.example.json":
        raise ApiError(400, "the example plan is read-only")
    body = read_json_body(handler)
    content = body.get("content", "")
    errors = validate_plan_content(content)
    if errors:
        raise ApiError(422, "; ".join(errors))
    full = os.path.join(SCHED_DIR, name)
    backup = backup_file(full) if os.path.isfile(full) else None
    atomic_write(full, content)
    return {"ok": True, "name": name, "backup": backup}


def api_schedule_start(handler, query):
    body = read_json_body(handler)
    name = body.get("plan", "")
    if not name.endswith(".json"):
        raise ApiError(400, "plan must be a .json file name under schedule/")
    full = os.path.join(SCHED_DIR, name)
    if not os.path.isfile(full):
        raise ApiError(404, "no such plan: {}".format(name))
    try:
        sch = runmgr.MGR.start_scheduler(full)
    except runmgr.SlotBusy as exc:
        raise ApiError(409, exc.reason)
    except SystemExit as exc:
        raise ApiError(422, str(exc))
    return {"scheduler": sch.to_dict()}


def api_schedule_stop(handler, query):
    sch, err = runmgr.MGR.stop_scheduler()
    if err:
        raise ApiError(409, err)
    return {"ok": True, "scheduler": sch.to_dict()}


def api_schedule_status(handler, query):
    return {"scheduler": runmgr.MGR.scheduler.to_dict() if runmgr.MGR.scheduler else None}


ROUTES += [
    ("GET", r"^/api/schedule/plans$", lambda h, q, m=None: api_schedule_plans(h, q)),
    ("GET", r"^/api/schedule/plan/(?P<name>[^/]+)$", api_schedule_plan_get),
    ("PUT", r"^/api/schedule/plan/(?P<name>[^/]+)$", api_schedule_plan_put),
    ("POST", r"^/api/schedule/start$", lambda h, q, m=None: api_schedule_start(h, q)),
    ("POST", r"^/api/schedule/stop$", lambda h, q, m=None: api_schedule_stop(h, q)),
    ("GET", r"^/api/schedule/status$", lambda h, q, m=None: api_schedule_status(h, q)),
]
