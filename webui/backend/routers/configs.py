#!/usr/bin/env python3
"""configs router：/api/configs、/api/config（读/写/校验）。

写入前校验复用编排器自身的 loader/validator（benchmark.data_size_list /
pipeline_plan / _configs.json 继承合并），保存自动备份到 webui/backups/。"""
import json
import os

import benchmark as bm

from .. import runmgr
from ..common import (ApiError, atomic_write, backup_file, jpath,
                      read_json_body, real_under)
from ..paths import BACKUP_DIR, CONF_ROOTS, ROOT

ROUTES = []


# ------------------------------------------------------------- 校验（复用编排器）

def validate_config_content(path, content):
    """-> list of error strings (empty = valid). JSON 语法 + 语义校验。"""
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


# ---------------------------------------------------------------- API 实现

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
    """Config inventory: `general` = every configs/*.json except *.example.json
    templates; `categories` = a config tree for EVERY non-underscore
    subdirectory of configs/."""
    out = {"general": [], "categories": {}, "cluster_candidates": [],
           "global_candidates": [], "matrix_candidates": []}
    if not os.path.isdir(bm.CONFIGS_DIR):
        return out
    for f in sorted(os.listdir(bm.CONFIGS_DIR)):
        fp = os.path.join(bm.CONFIGS_DIR, f)
        if os.path.isfile(fp) and f.endswith(".json") and not f.endswith(".example.json"):
            out["general"].append("configs/" + f)
    for name in sorted(os.listdir(bm.CONFIGS_DIR)):
        dpath = os.path.join(bm.CONFIGS_DIR, name)
        if not os.path.isdir(dpath) or name.startswith("_") or name.startswith("."):
            continue
        tree = _config_tree(dpath, "configs/" + name + "/")
        out["categories"][name] = {"tree": tree, "config_count": _tree_count(tree)}
    for p in out["general"]:
        base = os.path.basename(p)
        if base.startswith("cluster"):
            out["cluster_candidates"].append(p)
        elif base.startswith("global"):
            out["global_candidates"].append(p)
        elif base.startswith("matrix"):
            out["matrix_candidates"].append(p)
    return out


def api_config_get(handler, query):
    path = jpath(query, "path")
    full = os.path.join(ROOT, path)
    if not real_under(full, CONF_ROOTS) or not os.path.isfile(full):
        raise ApiError(404, "config not found (allowed roots: configs/, schedule/): {}".format(path))
    with open(full, "r", encoding="utf-8", errors="replace") as f:
        return {"path": path, "content": f.read()}


def api_config_put(handler, query):
    body = read_json_body(handler)
    path = body.get("path", "")
    content = body.get("content", "")
    full = os.path.join(ROOT, path)
    if not real_under(full, CONF_ROOTS):
        raise ApiError(400, "path outside allowed roots (configs/, schedule/): {}".format(path))
    if not path.endswith(".json"):
        raise ApiError(400, "only .json files are editable")
    errors = validate_config_content(full, content)
    if errors:
        raise ApiError(422, "; ".join(errors))
    backup = None
    if os.path.isfile(full):
        backup = backup_file(full)
    atomic_write(full, content)
    return {"ok": True, "path": path, "backup": backup}


def api_config_validate(handler, query):
    body = read_json_body(handler)
    path = body.get("path", "")
    full = os.path.join(ROOT, path)
    if not real_under(full, CONF_ROOTS):
        raise ApiError(400, "path outside allowed roots: {}".format(path))
    return {"ok_path": os.path.isfile(full),
            "errors": validate_config_content(full, body.get("content", ""))}


ROUTES += [
    ("GET", r"^/api/configs$", lambda h, q, m=None: api_configs(h, q)),
    ("GET", r"^/api/config$", api_config_get),
    ("PUT", r"^/api/config$", lambda h, q, m=None: api_config_put(h, q)),
    ("POST", r"^/api/config/validate$", lambda h, q, m=None: api_config_validate(h, q)),
]
