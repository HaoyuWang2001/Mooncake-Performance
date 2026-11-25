#!/usr/bin/env python3
"""inst_snapshot — 实例创建的配置快照（创建瞬间冻结，后续运行只用快照）。

冻结范围（全部为“本次运行会读取”的文件）：
  - 生效的 cluster/global/profile（rank_recall：rank/recall 的 cluster+global + profile）
  - selector 目录递归展开的场景配置（与 benchmark --config-dir 同规则：跳过 `_` 前缀）
  - 每个场景配置从 configs/<basic|pipeline|kvtest>/ 到其目录的每一级 _configs.json
  - k8s 部署模板（k8s.template / deployment_template / monitor_template，含缺省值）；
    快照版 cluster.json 会把这三个键改写为快照内路径，避免运行时回落到实时模板

落盘（原子：临时目录 -> rename）：
  webui/data/instances/<iid>/instance.json   # 创建时实例记录
  webui/data/instances/<iid>/snapshot.json   # 清单（来源/sha256/展开结果/快照 request）
  webui/data/instances/<iid>/files/configs/...  # 镜像仓库相对路径

运行：router 使用 snapshot["request"]（所有配置路径指向快照），并设置
MC_CONFIGS_INHERIT_ROOT（benchmark 的 _configs.json 继承根）与
MC_INSTANCE_SNAPSHOT（每次 run 的审计快照来源）。
"""
import hashlib
import json
import os
import shutil
import tempfile
import time

import benchmark as bm

from .paths import INSTANCES_DIR, ROOT

SNAPSHOT_VERSION = 1
MODE_DIRS = {"ssh": "configs/_ssh", "k8s": "configs/_k8s"}
INHERIT_CATS = ("basic", "pipeline", "kvtest")
K8S_TEMPLATE_KEYS = ("template", "deployment_template", "monitor_template")


class SnapshotError(Exception):
    """快照构建失败（router 转 400）。"""


def snapshot_dir(iid):
    return os.path.join(INSTANCES_DIR, iid)


def _as_ref(path):
    """仓库内 -> 相对 ROOT 的 POSIX 路径；仓库外（仅测试场景）-> 绝对路径。"""
    full = path if os.path.isabs(path) else os.path.join(ROOT, path)
    full = os.path.abspath(full)
    root = os.path.abspath(ROOT)
    if full == root or full.startswith(root + os.sep):
        return os.path.relpath(full, root).replace(os.sep, "/")
    return full


def _require_inside_root(path, what):
    full = path if os.path.isabs(path) else os.path.join(ROOT, path)
    full = os.path.realpath(full)
    root = os.path.realpath(ROOT)
    if not full.startswith(root + os.sep):
        raise SnapshotError("{} 必须在仓库内：{}".format(what, path))
    return full


def resolve_effective_flags(flags):
    """补全缺省 mode/launch_mode/cluster/global/profile（不修改入参）。"""
    out = dict(flags or {})
    mode = out.get("mode") or "ssh"
    if mode not in ("ssh", "k8s"):
        raise SnapshotError("flags.mode 必须是 'ssh' 或 'k8s'")
    launch = out.get("launch_mode") or "single"
    if launch not in ("single", "rank_recall"):
        raise SnapshotError("flags.launch_mode 必须是 'single' 或 'rank_recall'")
    if launch == "rank_recall" and mode != "k8s":
        raise SnapshotError("rank_recall 启动方式目前只支持 k8s")
    mode_dir = MODE_DIRS[mode]
    out["mode"] = mode
    out["launch_mode"] = launch
    if launch == "rank_recall":
        for key in ("rank_cluster", "rank_global",
                    "recall_cluster", "recall_global"):
            if not out.get(key):
                raise SnapshotError("缺少 {} 配置".format(key))
    else:
        if not out.get("cluster"):
            out["cluster"] = mode_dir + "/cluster.json"
        if not out.get("global"):
            out["global"] = mode_dir + "/global.json"
    if not out.get("profile"):
        out["profile"] = mode_dir + "/profile.json"
    return out


def _deployment_paths(flags):
    if flags["launch_mode"] == "rank_recall":
        return [flags[k] for k in ("rank_cluster", "rank_global",
                                   "recall_cluster", "recall_global",
                                   "profile")]
    return [flags["cluster"], flags["global"], flags["profile"]]


def _scenario_files(selector):
    """selector -> 场景配置文件绝对路径（目录递归展开，规则与 --config-dir 一致）。"""
    out = []
    for d in selector.get("dirs") or []:
        full = _require_inside_root(d, "配置目录")
        if not os.path.isdir(full):
            raise SnapshotError("config dir not found: {}".format(d))
        found = bm.collect_config_files(full)
        if not found:
            raise SnapshotError("配置目录下没有可运行配置：{}".format(d))
        out.extend(found)
    for f in selector.get("files") or []:
        full = _require_inside_root(f, "配置文件")
        if not os.path.isfile(full):
            raise SnapshotError("config file not found: {}".format(f))
        if os.path.basename(full).startswith("_"):
            raise SnapshotError("{} 是继承碎片，不能作为运行配置".format(f))
        out.append(full)
    return out


def _chain_fragments(paths):
    """场景配置 -> 其继承链上的每一级 _configs.json（存在才收）。"""
    out = []
    for path in paths:
        cfg_dir = os.path.dirname(os.path.realpath(path))
        for cat in INHERIT_CATS:
            cat_dir = os.path.realpath(os.path.join(bm.CONFIGS_DIR, cat))
            if cfg_dir == cat_dir or cfg_dir.startswith(cat_dir + os.sep):
                chain = [cat_dir]
                rel = os.path.relpath(cfg_dir, cat_dir)
                if rel != ".":
                    acc = cat_dir
                    for part in rel.split(os.sep):
                        acc = os.path.join(acc, part)
                        chain.append(acc)
                for level in chain:
                    frag = os.path.join(level, "_configs.json")
                    if os.path.isfile(frag):
                        out.append(frag)
                break
    return out


def _template_sources(cluster_doc):
    """cluster.json -> {key: 模板绝对路径}（含缺省模板）；非 k8s 集群返回 {}。"""
    if not (isinstance(cluster_doc.get("k8s"), dict)
            or cluster_doc.get("controller")):
        return {}
    k8s = cluster_doc.get("k8s") or {}
    default_dir = os.path.join(bm.CONFIGS_DIR, "_k8s", "templates")
    out = {}
    for key, fname in zip(K8S_TEMPLATE_KEYS,
                          ("benchmark-pod.yaml.tpl",
                           "benchmark-deployment.yaml.tpl",
                           "monitor-daemonset.yaml.tpl")):
        raw = k8s.get(key) or os.path.join(default_dir, fname)
        full = raw if os.path.isabs(raw) else os.path.join(ROOT, raw)
        if not os.path.isfile(full):
            raise SnapshotError("k8s 模板不存在（{}）：{}".format(key, raw))
        out[key] = os.path.realpath(full)
    return out


def _sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _copy_file(src, dst):
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copy2(src, dst)


def build(iid, name, request):
    """为实例 iid 创建配置快照（原子）。返回快照元数据（写入实例 record）。

    request = {"selector": {...}, "flags": {...}}（实例创建时提交的原样内容）。
    失败抛 SnapshotError / OSError；调用方负责在实例落盘失败时清理目录。
    """
    selector = request.get("selector") or {}
    flags = resolve_effective_flags(request.get("flags"))
    dep_paths = _deployment_paths(flags)
    scenario_paths = _scenario_files(selector)
    fragment_paths = _chain_fragments(scenario_paths)

    final_dir = snapshot_dir(iid)
    if os.path.exists(final_dir):
        raise SnapshotError("快照目录已存在：{}".format(iid))
    os.makedirs(INSTANCES_DIR, exist_ok=True)
    tmp_dir = tempfile.mkdtemp(prefix=".snapshot-" + iid + "-",
                              dir=INSTANCES_DIR)
    created = time.time()
    try:
        files_dir = os.path.join(tmp_dir, "files")
        # 快照 request / cluster 改写必须指向“最终目录”（tmp 会被 rename）
        files_final = os.path.join(final_dir, "files")
        copied = []          # (abs source, rel-in-snapshot)
        seen = set()

        def copy_one(src, what):
            src = os.path.realpath(src)
            if not os.path.isfile(src):
                raise SnapshotError("{} 不存在：{}".format(what, _as_ref(src)))
            _require_inside_root(src, what)   # 源文件必须在仓库内
            rel = _as_ref(src)
            if rel in seen:
                return
            seen.add(rel)
            dst = os.path.join(files_dir, rel)
            _copy_file(src, dst)
            copied.append((src, rel))

        for p in dep_paths:
            copy_one(p, "部署配置")
        for p in scenario_paths:
            copy_one(p, "场景配置")
        for p in fragment_paths:
            copy_one(p, "继承碎片")

        # k8s 模板冻结 + 快照版 cluster.json 改写（把三个模板键指向快照内路径）
        templates = []
        for p in dep_paths:
            full = os.path.join(ROOT, p) if not os.path.isabs(p) else p
            try:
                with open(full, "r", encoding="utf-8") as f:
                    doc = json.load(f)
            except (OSError, ValueError):
                continue
            if not isinstance(doc, dict):
                continue
            tsrc = _template_sources(doc)
            if not tsrc:
                continue
            cluster_rel = _as_ref(full)
            snap_cluster = os.path.join(files_dir, cluster_rel)
            for key, tpath in tsrc.items():
                copy_one(tpath, "k8s 模板")
                snap_tpl = _as_ref(os.path.join(files_final, _as_ref(tpath)))
                templates.append({"cluster": cluster_rel, "key": key,
                                  "source": _as_ref(tpath),
                                  "snapshot": snap_tpl})
            k8s = doc.setdefault("k8s", {})
            for key, tpath in tsrc.items():
                # 指向快照副本（否则 benchmark 会回落到实时模板）
                k8s[key] = _as_ref(os.path.join(files_final, _as_ref(tpath)))
            # 用改写后的 cluster 覆盖快照副本（源文件保持不动）
            with open(snap_cluster, "w", encoding="utf-8") as f:
                json.dump(doc, f, ensure_ascii=False, indent=1)

        # 快照专用 request：所有路径指向 files/ 内副本
        def snap_ref(raw):
            rel = _as_ref(raw if os.path.isabs(raw) else os.path.join(ROOT, raw))
            return _as_ref(os.path.join(files_final, rel))

        snap_flags = dict(flags)
        for key in ("cluster", "global", "profile",
                    "rank_cluster", "rank_global",
                    "recall_cluster", "recall_global"):
            if snap_flags.get(key):
                snap_flags[key] = snap_ref(snap_flags[key])
        snap_selector = {
            "dirs": [snap_ref(d) for d in (selector.get("dirs") or [])],
            "files": [snap_ref(f) for f in (selector.get("files") or [])],
        }

        manifest = {
            "version": SNAPSHOT_VERSION,
            "instance": iid,
            "name": name or iid,
            "created": created,
            "source_request": {"selector": selector, "flags": flags},
            "files": [{"path": rel, "sha256": _sha256(src),
                       "bytes": os.path.getsize(src)}
                      for src, rel in copied],
            "templates": templates,
            "request": {"selector": snap_selector, "flags": snap_flags},
        }
        with open(os.path.join(tmp_dir, "snapshot.json"), "w",
                  encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=1)
        with open(os.path.join(tmp_dir, "instance.json"), "w",
                  encoding="utf-8") as f:
            json.dump({"id": iid, "name": name or iid, "created": created,
                       "request": request}, f, ensure_ascii=False, indent=1)

        os.replace(tmp_dir, final_dir)
        tmp_dir = None
    finally:
        if tmp_dir and os.path.isdir(tmp_dir):
            shutil.rmtree(tmp_dir, ignore_errors=True)

    dir_ref = _as_ref(final_dir)
    meta = {
        "dir": dir_ref,
        "configs_root": _as_ref(os.path.join(final_dir, "files", "configs")),
        "manifest": _as_ref(os.path.join(final_dir, "snapshot.json")),
        "files": len(copied),
        "created": created,
        "request": manifest["request"],
    }
    return meta
