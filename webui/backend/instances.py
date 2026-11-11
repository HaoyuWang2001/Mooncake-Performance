#!/usr/bin/env python3
"""instances — 实例定义持久化（创建/列表/改名/删除）。

实例 = 一份表单快照（selector + flags）构成的可复用测试定义；创建本身不运行。
每次「运行」产生一个 runmgr Run（meta.instance 指回实例），沿用 run 的
历史/日志/停止/并发守卫机制。
"""
import json
import os
import re
import shutil
import threading
import time

from .paths import INSTANCES_DIR, INSTANCES_TRASH_DIR, RUNS_DIR

ID_RE = re.compile(r"^i[0-9A-Za-z._-]+$")
_seq_lock = threading.Lock()
_seq = [0]


def _new_id():
    with _seq_lock:
        _seq[0] += 1
        return "i{}-{:03d}".format(time.strftime("%Y%m%d-%H%M%S"), _seq[0])


def _path(iid):
    if not iid or not ID_RE.match(iid):
        return None
    return os.path.join(INSTANCES_DIR, iid + ".json")


def _normalize_name(name, fallback):
    name = (name or "").strip()
    if len(name) > 120:
        name = name[:120]
    return name or fallback


def load(iid):
    path = _path(iid)
    if not path or not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
    except (OSError, ValueError):
        return None
    d.setdefault("id", iid)
    d.setdefault("request", {})
    return d


def list_all():
    try:
        names = os.listdir(INSTANCES_DIR)
    except OSError:
        names = []
    out = []
    for n in names:
        if not n.endswith(".json"):
            continue
        inst = load(n[:-5])
        if inst:
            out.append(inst)
    out.sort(key=lambda d: (d.get("created") or 0, d.get("id") or ""),
             reverse=True)
    return out


def create(request, name=None):
    iid = _new_id()
    inst = {
        "id": iid,
        "name": _normalize_name(name, iid),
        "created": time.time(),
        "request": request or {},
    }
    save(inst)
    return inst


def save(inst):
    path = _path(inst.get("id"))
    if not path:
        raise ValueError("bad instance id: {!r}".format(inst.get("id")))
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(inst, f, ensure_ascii=False, indent=1)
    os.replace(tmp, path)


def rename(iid, name):
    inst = load(iid)
    if inst is None:
        return None
    inst["name"] = _normalize_name(name, inst.get("name") or iid)
    save(inst)
    return inst


def delete(iid):
    """Move the instance record to the trash (runs/logs/outputs are kept)."""
    path = _path(iid)
    if not path or not os.path.isfile(path):
        return False
    os.makedirs(INSTANCES_TRASH_DIR, exist_ok=True)
    dst = os.path.join(INSTANCES_TRASH_DIR, "{}-{}.json".format(
        time.strftime("%Y%m%d-%H%M%S"), iid))
    shutil.move(path, dst)
    return True


def runs_for(iid):
    """All run records of one instance (newest first), read from RUNS_DIR.

    Scans the persisted run json files (runmgr.history is capped in memory),
    so per-instance history survives restarts and old runs."""
    out = []
    try:
        names = os.listdir(RUNS_DIR)
    except OSError:
        names = []
    for n in names:
        if not n.endswith(".json"):
            continue
        try:
            with open(os.path.join(RUNS_DIR, n), "r", encoding="utf-8") as f:
                d = json.load(f)
        except (OSError, ValueError):
            continue
        if ((d.get("meta") or {}).get("instance") == iid):
            out.append(d)
    out.sort(key=lambda d: d.get("id") or "", reverse=True)
    return out


def latest_run(iid):
    runs = runs_for(iid)
    return runs[0] if runs else None
