#!/usr/bin/env python3
"""conflicts — 并发实例的共享资源身份键与冲突判定（纯函数，便于单测）。

两个实例只要任一身份键相交就不能同时运行。共享资源按层拆开：

  k8s     "<controller.ip>|<kubecontext>|<namespace>"
          同 namespace 的两个 benchmark 会争同一批 pod/Deployment（启动即失败）。
  ssh     frozenset(<节点管理网 IP>)
          同节点的两个 benchmark 会争端口段（9101/9201/9301+）、
          <remote_data_dir>/<scenario>/value.bin 与节点算力。
  etcd    frozenset(<etcd endpoint host:port>)
          benchmark 启动会清空整棵 mcbench/ 前缀，同 etcd 并行会摧毁
          另一实例的 reader 注册 → rendezvous 超时。
  master  "<host:port>"（master_addr 归一化）
  metrics "<host>"（master_metrics_url 的 host）
          同 master = 同 store：两个实例各自按 0.9×total 预算写入 →
          store 被打爆（-200/NO_AVAILABLE_HANDLE），remove-keys 互删数据。

不同集群 + 不同栈（etcd/master）才能真正并行；rank+recall 组合实例取两个
子任务身份键的并集参与判定。
"""
import json
import os
import re

KIND_K8S = "k8s"
KIND_SSH = "ssh"
KIND_ETCD = "etcd"
KIND_MASTER = "master"
KIND_METRICS = "metrics"


def _load(root, rel):
    if not rel:
        return None
    path = rel if os.path.isabs(rel) else os.path.join(root, rel)
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _norm_hostport(value):
    """Normalize "host:port" / "scheme://host:port/path" -> "host:port"."""
    v = (value or "").strip().lower()
    if not v:
        return ""
    if "://" in v:
        v = v.split("://", 1)[1]
    v = v.split("/", 1)[0]
    return v


def _etcd_set(value):
    """Parse an etcd endpoint spec into a set of normalized host:port.

    Accepted forms: "etcd://a:2379;b:2379", "a:2379,b:2379"."""
    v = (value or "").strip()
    if "://" in v:
        v = v.split("://", 1)[1]
    raw = re.split(r"[;,]", v)
    return frozenset(_norm_hostport(x) for x in raw if _norm_hostport(x))


def cluster_identities(root, cluster_rel):
    """Identity keys contributed by one cluster.json."""
    out = {}
    cluster = _load(root, cluster_rel)
    if not isinstance(cluster, dict):
        # Unparsable/missing config: fall back to the file identity so two
        # runs on the same file are still treated as colliding.
        if cluster_rel:
            out["cluster_file"] = os.path.abspath(
                cluster_rel if os.path.isabs(cluster_rel)
                else os.path.join(root, cluster_rel)).lower()
        return out
    if cluster.get("controller"):
        c = cluster.get("controller") or {}
        k8s = cluster.get("k8s") or {}
        if c.get("ip"):
            out[KIND_K8S] = "{}|{}|{}".format(
                str(c.get("ip")).strip().lower(),
                str(k8s.get("kubecontext") or "").strip().lower(),
                str(k8s.get("namespace") or "").strip().lower())
    nodes = cluster.get("nodes") or {}
    ips = set()
    if isinstance(nodes, dict):
        for n in nodes.values():
            if isinstance(n, dict) and n.get("ip"):
                ips.add(str(n["ip"]).strip().lower())
    if ips:
        out[KIND_SSH] = frozenset(ips)
    return out


def global_identities(root, global_rel):
    """Identity keys contributed by one global.json (data plane)."""
    out = {}
    glob = _load(root, global_rel)
    if not isinstance(glob, dict):
        if global_rel:
            out["global_file"] = os.path.abspath(
                global_rel if os.path.isabs(global_rel)
                else os.path.join(root, global_rel)).lower()
        return out
    eps = _etcd_set(glob.get("etcd_endpoints"))
    if eps:
        out[KIND_ETCD] = eps
    master = _norm_hostport(glob.get("master_addr"))
    if master:
        out[KIND_MASTER] = master
    metrics = _norm_hostport(glob.get("master_metrics_url"))
    if metrics:
        out[KIND_METRICS] = metrics.split(":", 1)[0]
    return out


def run_identities(root, flags):
    """Identity dicts for one instance's request flags.

    Returns a LIST (one dict per sub-task): single mode -> 1 entry,
    rank_recall -> rank + recall entries. A candidate conflicts when ANY of
    its dicts intersects ANY active run's dicts."""
    flags = flags or {}
    launch = flags.get("launch_mode") or "single"
    if launch == "rank_recall":
        pairs = [("rank_cluster", "rank_global"),
                 ("recall_cluster", "recall_global")]
    else:
        pairs = [("cluster", "global")]
    out = []
    for ckey, gkey in pairs:
        ident = {}
        ident.update(cluster_identities(root, flags.get(ckey)))
        ident.update(global_identities(root, flags.get(gkey)))
        out.append(ident)
    return out


def _intersects(kind, a, b):
    if isinstance(a, frozenset) and isinstance(b, frozenset):
        return bool(a & b)
    return a == b


def find_conflict(candidate, active):
    """Find the first shared identity key between candidate and active runs.

    candidate: list[dict] identity dicts (one per sub-task)
    active: {rid: (label, list[dict])}
    Returns (kind, value, rid, label) or None."""
    for ident in candidate or []:
        for kind, value in ident.items():
            for rid, (label, others) in active.items():
                for other in others or []:
                    if kind in other and _intersects(kind, value, other[kind]):
                        return kind, value, rid, label
    return None


def describe(kind, value):
    """Human-readable shared resource for 409 messages."""
    names = {
        KIND_K8S: "k8s namespace（controller|context|namespace）",
        KIND_SSH: "SSH 节点集合",
        KIND_ETCD: "etcd 端点",
        KIND_MASTER: "master 地址（store）",
        KIND_METRICS: "master metrics（store）",
        "cluster_file": "cluster 配置文件",
        "global_file": "global 配置文件",
    }
    if isinstance(value, frozenset):
        shown = ", ".join(sorted(value)[:3])
        if len(value) > 3:
            shown += ", ..."
    else:
        shown = str(value)
    return "{}: {}".format(names.get(kind, kind), shown)
