#!/usr/bin/env python3
"""instances router：实例的创建/列表/详情/运行/停止/改名/删除。

创建实例只做表单快照（selector + flags），不占用运行槽；运行产生的 run 因
meta.instance 归属该实例，日志/停止/并发守卫沿用 runs 的机制。
"""
import hashlib
import json
import os
import shutil

from .. import conflicts, inst_snapshot, instances, runmgr
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
        "snapshot": ({"files": inst["snapshot"].get("files"),
                      "created": inst["snapshot"].get("created")}
                     if inst.get("snapshot") else None),
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
    name = body.get("name")
    iid = instances.new_id()
    # 创建瞬间冻结全部配置（含 k8s 模板）；之后改配置不影响该实例
    try:
        snap = inst_snapshot.build(iid, name, request)
    except inst_snapshot.SnapshotError as exc:
        raise ApiError(400, str(exc))
    except OSError as exc:
        raise ApiError(500, "创建配置快照失败: {}".format(exc))
    try:
        inst = instances.create(request, name, iid=iid, snapshot=snap)
    except Exception:
        shutil.rmtree(inst_snapshot.snapshot_dir(iid), ignore_errors=True)
        raise
    return {"instance": _summary(inst)}


SNAPSHOT_GROUP_ORDER = ["部署配置", "场景配置", "继承碎片", "k8s 模板",
                        "元数据"]


def _snapshot_root(inst):
    snap = inst.get("snapshot") or {}
    if not snap or not snap.get("dir"):
        raise ApiError(404, "该实例没有配置快照（旧实例）")
    root = snap["dir"]
    return snap, (root if os.path.isabs(root) else os.path.join(ROOT, root))


def _sha256_path(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _snapshot_group_sets(manifest):
    """从清单推导分组键：部署配置=flags 指向的配置；模板=manifest.templates。"""
    flags = ((manifest.get("source_request") or {}).get("flags") or {})
    dep = set()
    for key in ("cluster", "global", "profile", "rank_cluster",
                "rank_global", "recall_cluster", "recall_global"):
        raw = flags.get(key)
        if not raw:
            continue
        full = raw if os.path.isabs(raw) else os.path.join(ROOT, raw)
        dep.add(os.path.relpath(full, ROOT).replace(os.sep, "/"))
    tpl = set()
    for item in manifest.get("templates") or []:
        for key in ("source", "snapshot"):
            raw = item.get(key)
            if not raw:
                continue
            full = raw if os.path.isabs(raw) else os.path.join(ROOT, raw)
            tpl.add(os.path.relpath(full, ROOT).replace(os.sep, "/"))
    return dep, tpl


def _snapshot_group(path, dep, tpl):
    if path in ("snapshot.json", "instance.json"):
        return "元数据"
    if os.path.basename(path) == "_configs.json":
        return "继承碎片"
    if path in dep:
        return "部署配置"
    if path in tpl or path.endswith(".tpl"):
        return "k8s 模板"
    return "场景配置"


def _snapshot_live_status(path, sha):
    """快照文件 vs 当前磁盘（同路径）的状态：same/changed/missing。"""
    if not sha:
        return None
    live = os.path.join(ROOT, path)
    if not os.path.isfile(live):
        return "missing"
    try:
        return "same" if _sha256_path(live) == sha else "changed"
    except OSError:
        return "missing"


def api_instance_snapshot(handler, query, iid):
    """GET /api/instances/{iid}/snapshot：配置快照清单（含与实时配置对比）。"""
    inst = _load(iid)
    snap, root = _snapshot_root(inst)
    mpath = os.path.join(root, "snapshot.json")
    try:
        with open(mpath, "r", encoding="utf-8") as f:
            manifest = json.load(f)
    except (OSError, ValueError):
        raise ApiError(404, "快照清单缺失或损坏：{}".format(mpath))
    dep, tpl = _snapshot_group_sets(manifest)
    files = []
    for item in manifest.get("files") or []:
        path = item.get("path") or ""
        if not path:
            continue
        sha = item.get("sha256") or ""
        files.append({
            "path": path,
            "group": _snapshot_group(path, dep, tpl),
            "bytes": int(item.get("bytes") or 0),
            "sha256": sha,
            "live": _snapshot_live_status(path, sha),
        })
    # 元数据（清单 / 创建时实例记录）也可查看
    for name in ("snapshot.json", "instance.json"):
        fp = os.path.join(root, name)
        if os.path.isfile(fp):
            files.append({"path": name, "group": "元数据",
                          "bytes": os.path.getsize(fp), "sha256": "",
                          "live": None})
    files.sort(key=lambda x: (
        SNAPSHOT_GROUP_ORDER.index(x["group"])
        if x["group"] in SNAPSHOT_GROUP_ORDER else 99, x["path"]))
    return {"iid": iid, "name": inst.get("name") or iid,
            "created": snap.get("created"), "files": files}


def api_instance_snapshot_file(handler, query, iid):
    """GET /api/instances/{iid}/snapshot/file?path=...：读快照文件原文。"""
    inst = _load(iid)
    _snap, root = _snapshot_root(inst)
    rel = (query.get("path", [""])[0] or "").strip()
    if not rel:
        raise ApiError(400, "missing query parameter: path")
    root_real = os.path.realpath(root)
    full = None
    for cand in (os.path.join(root, "files", rel), os.path.join(root, rel)):
        rp = os.path.realpath(cand)
        if not (rp == root_real or rp.startswith(root_real + os.sep)):
            raise ApiError(403, "path outside snapshot: {}".format(rel))
        if os.path.isfile(rp):
            full = rp
            break
    if full is None:
        raise ApiError(404, "snapshot file not found: {}".format(rel))
    try:
        with open(full, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except OSError as exc:
        raise ApiError(500, "read snapshot file failed: {}".format(exc))
    return {"path": rel, "content": content, "bytes": os.path.getsize(full)}


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
    original = inst.get("request") or {}
    snap = inst.get("snapshot") or {}
    # 运行时只用创建瞬间冻结的配置（旧实例无快照 -> 沿用实时路径）
    request = snap.get("request") or original
    configs_root = snap.get("configs_root") or None
    argv = _build_run_argv(request, configs_root=configs_root)
    identities = conflicts.run_identities(ROOT, request.get("flags") or {})
    env_extra = None
    if snap:
        env_extra = {
            "MC_CONFIGS_INHERIT_ROOT": (
                configs_root if os.path.isabs(configs_root)
                else os.path.join(ROOT, configs_root)),
            "MC_INSTANCE_SNAPSHOT": (
                snap["dir"] if os.path.isabs(snap["dir"])
                else os.path.join(ROOT, snap["dir"])),
        }
    warnings = []
    sch = runmgr.MGR.scheduler
    if sch is not None and sch.state in ("waiting", "waiting_slot"):
        warnings.append(
            "scheduler active (plan {}, next task @ {}) - its task may "
            "collide with this run".format(
                os.path.basename(sch.plan_path), sch.next_trigger))
    try:
        run = runmgr.MGR.start_benchmark(
            argv, meta={"request": original, "instance": iid,
                        "instance_name": inst.get("name") or iid,
                        "snapshot": ({"dir": snap.get("dir"),
                                      "files": snap.get("files")}
                                     if snap else None)},
            identities=identities, env_extra=env_extra)
    except runmgr.SlotBusy as exc:
        raise ApiError(409, exc.reason)
    return {"run": run.to_dict(), "warnings": warnings}


def api_instance_stop(handler, query, iid):
    _load(iid)
    run = _active_run(iid)
    if run is None:
        # webui 重启后遗留的孤儿 benchmark：active 只在内存里，重启即丢，
        # 但持久化 run 记录仍是 running（例如 k8s 失败后 hold 等 STOP）。
        # 回退到 stop_orphan：写 stop 文件即可让 hold/清理循环退出并删
        # namespace（能定位 pid 时再补发信号）。
        latest = instances.latest_run(iid)
        if latest and latest.get("status") == "running":
            run, err = runmgr.MGR.stop_orphan(latest.get("id"))
            if err:
                raise ApiError(404, err)
            return {"ok": True,
                    "run": run.to_dict() if run is not None else latest}
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


def api_instance_reattach(handler, query, iid):
    """重新吸附仍在运行的实例（webui 重启 / 网络中断后的「重连」）。

    后端按落盘 pid / --stop-file 扫描确认进程仍存活后，把 run 修回 running
    并重新纳入 active（恢复并发守卫）；找不到进程则不改状态。
    """
    _load(iid)
    active = _active_run(iid)
    if active is not None:
        return {"ok": True, "reattached": True, "status": active.status,
                "run": active.to_dict()}
    latest = instances.latest_run(iid)
    if latest is None:
        raise ApiError(409, "实例还没有运行记录")
    run, err = runmgr.MGR.reattach(latest.get("id"))
    if err:
        raise ApiError(404, err)
    ok = bool(run and run.status == "running")
    return {"ok": True, "reattached": ok,
            "status": run.status if run else latest.get("status"),
            "run": run.to_dict() if run else latest}


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
    ("GET", r"^/api/instances/(?P<iid>[^/]+)/snapshot$", api_instance_snapshot),
    ("GET", r"^/api/instances/(?P<iid>[^/]+)/snapshot/file$",
     api_instance_snapshot_file),
    ("DELETE", r"^/api/instances/(?P<iid>[^/]+)$", api_instance_delete),
    ("POST", r"^/api/instances/(?P<iid>[^/]+)/run$", api_instance_run),
    ("POST", r"^/api/instances/(?P<iid>[^/]+)/stop$", api_instance_stop),
    ("POST", r"^/api/instances/(?P<iid>[^/]+)/reattach$", api_instance_reattach),
    ("POST", r"^/api/instances/(?P<iid>[^/]+)/rename$", api_instance_rename),
]
