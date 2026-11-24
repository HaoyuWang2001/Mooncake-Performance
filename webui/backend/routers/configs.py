#!/usr/bin/env python3
"""configs router：/api/configs、/api/config（读/写/校验）。

写入前校验复用编排器自身的 loader/validator（benchmark.data_size_list /
pipeline_plan / _configs.json 继承合并）；校验通过后**原子写回、不保留备份**。"""
import json
import os
import tempfile

import benchmark as bm

from .. import runmgr
from ..common import (ApiError, atomic_write, jpath, read_json_body,
                      real_under)
from ..paths import CONF_ROOTS, ROOT

ROUTES = []


# ------------------------------------------------------------- 校验（复用编排器）

INHERIT_CATS = ("basic", "pipeline", "kvtest")


def _inheritance_chain(path):
    """-> (scenario 类别 | None, [各级目录（场景根 -> 文件所在目录）])。

    与 validate_config_content 的继承语义一致：basic/pipeline/kvtest 参与
    `_configs.json` 继承；其它（cluster/global/profile、_ssh/_k8s 等）不继承。
    """
    rp = os.path.realpath(path)
    for cat in INHERIT_CATS:
        cdir = os.path.realpath(os.path.join(bm.CONFIGS_DIR, cat))
        if rp == cdir or rp.startswith(cdir + os.sep):
            chain = [cdir]
            rel = os.path.relpath(os.path.dirname(rp), cdir)
            if rel != ".":
                acc = cdir
                for part in rel.split(os.sep):
                    acc = os.path.join(acc, part)
                    chain.append(acc)
            return cat, chain
    return None, []


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
    scenario_dir, chain = _inheritance_chain(path)
    if scenario_dir is None:
        return errors  # cluster/global/profile/external: syntax only
    # inheritance: _configs.json chain merged under the new content
    merged = {}
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
        try:
            bm.basic_k8s_numa_node(merged)
        except RuntimeError as exc:
            errors.append(str(exc))
    elif scenario_dir == "kvtest":
        if merged.get("scenario") == "kvtest_basic":
            op = merged.get("operation")
            if not op:
                errors.append("kvtest_basic requires operation")
            else:
                put_iface, get_iface, mixed = bm.kvtest_basic_split_operation(
                    merged)
                if mixed:
                    if put_iface not in bm.KVTEST_WRITER_OPS:
                        errors.append(
                            "kvtest_basic unsupported put interface: "
                            "{}".format(put_iface))
                    if get_iface not in bm.KVTEST_READER_OPS:
                        errors.append(
                            "kvtest_basic unsupported get interface: "
                            "{}".format(get_iface))
                elif (put_iface not in bm.KVTEST_WRITER_OPS and
                      get_iface not in bm.KVTEST_READER_OPS):
                    errors.append(
                        "kvtest_basic unsupported operation: {}".format(op))
            params = merged.get("params")
            if not isinstance(params, list) or not params:
                errors.append("params must be a non-empty array of "
                              "{num_clients, num_threads} objects")
        else:
            try:
                bm.kvtest_plan(merged, 1)  # node_count=1: shape validation only
            except (RuntimeError, KeyError, ValueError) as exc:
                errors.append("kvtest plan invalid: {}".format(exc))
    else:
        try:
            bm.pipeline_plan(merged, 1)  # node_count=1: shape validation only
        except (RuntimeError, KeyError, ValueError) as exc:
            errors.append("pipeline plan invalid: {}".format(exc))
    if merged.get("protocol") not in (None, "tcp", "rdma", "ub"):
        errors.append("protocol must be one of tcp/rdma/ub")
    return errors


def validate_plan_content(content):
    """-> list of error strings, using run_schedule's own loaders."""
    errors = []
    fd, tmp = tempfile.mkstemp(prefix="plan-validate-", suffix=".json")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
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
    """Config inventory: `general` = every deployment json (configs root +
    _ssh/ + _k8s/ dirs) except *.example.json templates; `categories` = a
    config tree for EVERY non-underscore subdirectory of configs/."""
    out = {"general": [], "categories": {}, "cluster_candidates": [],
           "global_candidates": [], "profile_candidates": []}
    if not os.path.isdir(bm.CONFIGS_DIR):
        return out
    general_dirs = ["", "_ssh", "_k8s"]
    for sub in general_dirs:
        dpath = os.path.join(bm.CONFIGS_DIR, sub)
        if not os.path.isdir(dpath):
            continue
        prefix = ("configs/" + sub) if sub else "configs"
        for f in sorted(os.listdir(dpath)):
            fp = os.path.join(dpath, f)
            if os.path.isfile(fp) and f.endswith(".json") \
                    and not f.endswith(".example.json"):
                out["general"].append(prefix.rstrip("/") + "/" + f)
    for name in sorted(os.listdir(bm.CONFIGS_DIR)):
        dpath = os.path.join(bm.CONFIGS_DIR, name)
        if not os.path.isdir(dpath) or name.startswith("_") or name.startswith("."):
            continue
        tree = _config_tree(dpath, "configs/" + name + "/")
        out["categories"][name] = {"tree": tree, "config_count": _tree_count(tree)}
    for p in out["general"]:
        base = os.path.basename(p)
        # tag the owning mode dir (_ssh / _k8s / root) so the run box can
        # filter candidates by mode
        if "/_ssh/" in p:
            owner = "_ssh"
        elif "/_k8s/" in p:
            owner = "_k8s"
        else:
            owner = "root"
        tagged = {"path": p, "owner": owner}
        if base.startswith("cluster"):
            out["cluster_candidates"].append(tagged)
        elif base.startswith("global"):
            out["global_candidates"].append(tagged)
        elif base.startswith("profile"):
            out["profile_candidates"].append(tagged)
    return out


def api_config_chain(handler, query):
    """GET /api/config/chain?path=...

    返回该配置的完整继承链（场景根 -> 文件所在目录各级 `_configs.json`，
    缺失级也列出）+ 文件自身 + 合并后的生效配置。供配置维护弹窗一次性展示。
    """
    path = jpath(query, "path")
    full = os.path.join(ROOT, path)
    if not real_under(full, CONF_ROOTS) or not os.path.isfile(full):
        raise ApiError(404, "config not found: {}".format(path))
    cat, dirs = _inheritance_chain(full)
    levels = []
    merged = {}
    for d in dirs:
        frag = os.path.join(d, "_configs.json")
        rel = os.path.relpath(frag, ROOT).replace("\\", "/")
        item = {"kind": "defaults", "path": rel, "exists": False,
                "content": "", "bytes": 0}
        if os.path.isfile(frag):
            try:
                with open(frag, "r", encoding="utf-8") as f:
                    content = f.read()
                doc = json.loads(content)
            except (OSError, ValueError) as exc:
                item.update({"exists": True,
                             "error": "解析失败: {}".format(exc)})
                levels.append(item)
                continue
            item.update({"exists": True, "content": content,
                         "bytes": len(content.encode("utf-8"))})
            if isinstance(doc, dict):
                bm._deep_merge(merged, doc)
        levels.append(item)
    try:
        with open(full, "r", encoding="utf-8", errors="replace") as f:
            self_content = f.read()
    except OSError as exc:
        raise ApiError(500, "read config failed: {}".format(exc))
    levels.append({"kind": "self", "path": path, "exists": True,
                   "content": self_content,
                   "bytes": len(self_content.encode("utf-8"))})
    effective = ""
    if cat is not None:
        try:
            doc = json.loads(self_content)
            if isinstance(doc, dict):
                bm._deep_merge(merged, doc)
        except ValueError:
            pass
        if merged:
            effective = json.dumps(merged, ensure_ascii=False, indent=2)
    return {"path": path, "inherits": cat is not None,
            "category": cat or "", "levels": levels,
            "effective": effective}


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
    atomic_write(full, content)
    return {"ok": True, "path": path}


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
    ("GET", r"^/api/config/chain$", api_config_chain),
    ("GET", r"^/api/config$", api_config_get),
    ("PUT", r"^/api/config$", lambda h, q, m=None: api_config_put(h, q)),
    ("POST", r"^/api/config/validate$", lambda h, q, m=None: api_config_validate(h, q)),
]
