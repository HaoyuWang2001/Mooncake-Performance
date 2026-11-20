#!/usr/bin/env python3
"""nodes router：集群节点（cluster.json）+ SSH 实测探测。

GET /api/nodes               节点表 + global.json 角色/应有端口合并
GET /api/nodes/{name}/test   SSH 连通 + 环境 + mooncake 进程 + 应有端口监听
GET /api/nodes/{name}/ports  节点监听端口（ss -tlnp 降级链）

只读探测：脚本仅执行 uname / ss / ps，且节点名必须来自所选 cluster 配置，
避免该端点被用作任意 SSH 中继。SSH 传输复用编排器的 utils（paramiko，连接缓存）。
"""
import json
import os
import re
from datetime import datetime, timezone
from urllib.parse import urlparse

from ..common import ApiError, jpath, real_under
from ..paths import CONF_ROOTS, ROOT

ROUTES = []

DEFAULT_CLUSTER = "configs/cluster.json"
DEFAULT_GLOBAL = "configs/global.json"

# 一次 SSH 返回全部探测段落；段头 @@NAME@@ 独占一行。
_PROBE_SCRIPT = r"""
echo "@@UNAME@@"; uname -s -r 2>/dev/null
echo "@@ARCH@@"; uname -m 2>/dev/null
echo "@@SS@@"
if command -v ss >/dev/null 2>&1; then
  sudo -n ss -tlnp 2>/dev/null || ss -tlnp 2>/dev/null || ss -tln 2>/dev/null
elif command -v netstat >/dev/null 2>&1; then
  sudo -n netstat -tlnp 2>/dev/null || netstat -tlnp 2>/dev/null || netstat -tln 2>/dev/null
fi
echo "@@PS@@"; ps -eo pid=,user=,comm= 2>/dev/null
echo "@@PROCS@@"; ps -eo pid=,args= 2>/dev/null \
  | grep -Ei 'mooncake_master|mooncake_client|mooncake-store|mooncake_store|mooncake_kvtest' \
  | grep -v 'grep -Ei' | head -40
exit 0
"""


# ---------------------------------------------------------------- 配置加载

def _load_json(path, label):
    full = os.path.join(ROOT, path)
    if not real_under(full, CONF_ROOTS):
        raise ApiError(400, "{} 路径越界（允许 configs/、schedule/）：{}".format(label, path))
    if not os.path.isfile(full):
        raise ApiError(404, "{} 不存在：{}".format(label, path))
    try:
        with open(full, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except (OSError, ValueError) as exc:
        raise ApiError(422, "{} 解析失败：{}".format(label, exc))
    if not isinstance(cfg, dict):
        raise ApiError(422, "{} 顶层必须是 JSON 对象".format(label))
    return cfg


def _context(query):
    cpath = jpath(query, "cluster", required=False) or DEFAULT_CLUSTER
    gpath = jpath(query, "global", required=False) or DEFAULT_GLOBAL
    cluster = _load_json(cpath, "cluster")
    glob = {}
    if os.path.isfile(os.path.join(ROOT, gpath)):
        glob = _load_json(gpath, "global")
    return cpath, cluster, gpath, glob


def _cluster_nodes(cluster):
    """cluster.json 的 nodes 映射 -> 稳定顺序的节点列表（k8s 型配置无 nodes）。"""
    nodes = cluster.get("nodes")
    if not isinstance(nodes, dict):
        return []
    out = []
    for name, spec in nodes.items():
        if not isinstance(spec, dict):
            continue
        out.append({
            "name": name,
            "ip": spec.get("ip") or "",
            "fabric_ip": spec.get("fabric_ip") or "",
            "ssh_user": spec.get("ssh_user") or "root",
            "ssh_port": int(spec.get("ssh_port") or 22),
            "ssh_key": spec.get("ssh_key") or "",
        })
    return out


def _controller_node(cluster):
    """从 cluster.controller 合成 controller 节点（k8s 控制面）。"""
    ctrl = cluster.get("controller")
    if not isinstance(ctrl, dict) or not ctrl.get("ip"):
        return None
    return {
        "name": "controller",
        "ip": ctrl.get("ip") or "",
        "fabric_ip": "",
        "ssh_user": ctrl.get("ssh_user") or "root",
        "ssh_port": int(ctrl.get("ssh_port") or 22),
        "ssh_key": ctrl.get("ssh_key") or "",
        "is_controller": True,
    }


def _all_nodes(cluster):
    """SSH 节点 +（若有）controller 节点。"""
    nodes = _cluster_nodes(cluster)
    cnode = _controller_node(cluster)
    if cnode is not None:
        nodes = nodes + [cnode]
    return nodes


# ------------------------------------------------------- global.json 角色/端口

def _host_port(text):
    """'host:port' -> (host, port|None)。"""
    text = str(text or "").strip()
    if not text:
        return "", None
    host, _, port = text.rpartition(":")
    if not host:
        return text, None
    try:
        return host, int(port)
    except ValueError:
        return text, None


def _url_endpoint(text):
    """'http://h:p/x' / 'h:p' -> (host, port|None)。"""
    text = str(text or "").strip()
    if not text:
        return "", None
    if "://" not in text:
        return _host_port(text)
    u = urlparse(text)
    return u.hostname or "", u.port


def _role_map(nodes, glob):
    store_names = set(glob.get("mooncake_store_nodes") or [])
    master_host, _ = _host_port(glob.get("master_addr"))
    md_host, _ = _url_endpoint(glob.get("metadata_server"))
    etcd_hosts = set()
    for ep in str(glob.get("etcd_endpoints") or "").split(","):
        h, _ = _url_endpoint(ep)
        if h:
            etcd_hosts.add(h)
    out = {}
    for n in nodes:
        roles = []
        if n["name"] in store_names:
            roles.append("store")
        if master_host and master_host in (n["ip"], n["fabric_ip"]):
            roles.append("master")
        if md_host and md_host in (n["ip"], n["fabric_ip"]):
            roles.append("metadata")
        if etcd_hosts and etcd_hosts & {n["ip"], n["fabric_ip"]}:
            roles.append("etcd")
        out[n["name"]] = roles or ["client"]
    return out


def _expected_ports(node, glob):
    """该节点按 global.json 应有暴露的端口（用于 test 端点核对监听）。"""
    addrs = {node["ip"], node["fabric_ip"]}
    out = []

    def add(purpose, host, port):
        if port and host in addrs:
            item = {"purpose": purpose, "port": int(port)}
            if item not in out:
                out.append(item)

    host, port = _host_port(glob.get("master_addr"))
    add("master RPC", host, port)
    host, port = _url_endpoint(glob.get("metadata_server"))
    add("元数据 HTTP", host, port)
    for ep in str(glob.get("etcd_endpoints") or "").split(","):
        host, port = _url_endpoint(ep)
        if host:
            add("etcd", host, port or 2379)
    return out


# ---------------------------------------------------------------- 探测输出解析

def _parse_sections(text):
    sections = {}
    cur = None
    for line in (text or "").split("\n"):
        m = re.match(r"^@@([A-Z]+)@@$", line.strip())
        if m:
            cur = m.group(1)
            sections.setdefault(cur, [])
            continue
        if cur:
            sections[cur].append(line)
    return {k: "\n".join(v).strip() for k, v in sections.items()}


def _split_addr(addr):
    addr = (addr or "").strip()
    if ":" not in addr:
        return addr, None
    host, _, port = addr.rpartition(":")
    host = host.strip("[]") or "0.0.0.0"
    try:
        return host, int(port)
    except ValueError:
        return host, None


def _parse_ports(text):
    """ss -tln[p] / netstat -tln[p] -> [{proto,state,local_addr,port,users}]。"""
    out = []
    for raw in (text or "").split("\n"):
        parts = raw.strip().split()
        if parts and parts[0] == "LISTEN" and len(parts) >= 5:
            # ss: LISTEN 0 4096 0.0.0.0:22 0.0.0.0:* users:(("sshd",pid=1,fd=3))
            proto, state, local = "tcp", "LISTEN", parts[3]
            tail = raw
        elif parts and parts[0] in ("tcp", "tcp6") and len(parts) >= 6 \
                and parts[5] == "LISTEN":
            # netstat: tcp 0 0 0.0.0.0:22 0.0.0.0:* LISTEN 1/sshd
            proto, state, local = parts[0], "LISTEN", parts[3]
            tail = raw
        else:
            continue
        host, port = _split_addr(local)
        users = [{"process": m.group(1), "pid": int(m.group(2))}
                 for m in re.finditer(r'\("([^"]+)",pid=(\d+)', tail)]
        if not users and parts[0] in ("tcp", "tcp6") and len(parts) >= 7:
            m = re.match(r"^(\d+)/(.+)$", parts[6])
            if m:
                users = [{"process": m.group(2), "pid": int(m.group(1))}]
        out.append({"proto": proto, "state": state, "local_addr": host,
                    "port": port, "users": users})
    return out


def _parse_ps(text):
    """ps -eo pid=,user=,comm= -> {pid: {user, comm}}。"""
    out = {}
    for raw in (text or "").split("\n"):
        parts = raw.split(None, 2)
        if len(parts) < 3 or not parts[0].isdigit():
            continue
        out[int(parts[0])] = {"user": parts[1], "comm": parts[2].strip()}
    return out


def _parse_procs(text):
    """ps -eo pid=,args=（已按 mooncake 过滤）-> [{pid, cmd}]。"""
    out = []
    for raw in (text or "").split("\n"):
        parts = raw.split(None, 1)
        if len(parts) < 2 or not parts[0].isdigit():
            continue
        out.append({"pid": int(parts[0]), "cmd": parts[1].strip()})
    return out


def _proc_kind(cmd):
    low = (cmd or "").lower()
    if "mooncake_benchmark_client" in low or "mooncake_kvtest" in low:
        return "benchmark"
    if "mooncake_master" in low:
        return "master"
    if "mooncake_client" in low or "mooncake-store" in low or "mooncake_store" in low:
        return "store"
    return "other"


# ------------------------------------------------------ k8s 跨命名空间套件探测

_MOON_RE = re.compile(r"mooncake|kvtest|store|master|bench", re.I)

# 在 controller 上执行的只读脚本：列出全部命名空间的 pod（JSON）。
_K8S_SCRIPT_TMPL = """\
{export}
echo "@@K8S@@"
{kubectl} get pods --all-namespaces -o json 2>/dev/null
exit 0
"""


def _human_age(iso):
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        secs = (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return ""
    if secs < 60:
        return "{}s".format(int(secs))
    if secs < 3600:
        return "{}m".format(int(secs // 60))
    if secs < 86400:
        return "{}h".format(int(secs // 3600))
    return "{}d".format(int(secs // 86400))


def _parse_k8s_pods(text):
    """kubectl get pods -A -o json -> (pods, namespaces)；仅保留 mooncake 相关。"""
    if not text or not text.strip():
        return [], []
    try:
        data = json.loads(text)
    except (ValueError, TypeError):
        return [], []
    items = (data.get("items") or []) if isinstance(data, dict) else []
    pods = []
    nss = set()
    for it in items:
        if not isinstance(it, dict):
            continue
        md = it.get("metadata", {}) or {}
        ns = md.get("namespace", "") or ""
        name = md.get("name", "") or ""
        if not (_MOON_RE.search(ns) or _MOON_RE.search(name)):
            continue
        st = it.get("status", {}) or {}
        phase = st.get("phase", "") or ""
        cstatuses = st.get("containerStatuses") or []
        restarts = 0
        images = []
        for cs in cstatuses:
            restarts += int(cs.get("restartCount", 0) or 0)
            if cs.get("image"):
                images.append(cs["image"])
        spec = it.get("spec", {}) or {}
        node_name = spec.get("nodeName", "") or ""
        pods.append({
            "namespace": ns, "name": name, "status": phase,
            "node": node_name, "age": _human_age(st.get("startTime", "")),
            "restarts": restarts, "images": images,
        })
        nss.add(ns)
    pods.sort(key=lambda p: (p["namespace"], p["name"]))
    return pods, sorted(nss)


def api_node_k8s(handler, query, name):
    cpath, cluster, gpath, glob = _context(query)
    node = _find_node(cluster, name)
    if node is None:
        raise ApiError(404, "cluster {} 中没有节点 {}".format(cpath, name))
    if not node.get("is_controller"):
        raise ApiError(400, "节点 {} 不是 controller，无法查询 k8s 套件".format(name))
    ctrl = cluster.get("controller") or {}
    kubectl = ctrl.get("kubectl") or "kubectl"
    kubeconfig = ctrl.get("kubeconfig") or ""
    export = ('export KUBECONFIG="{}"\n'.format(kubeconfig)) if kubeconfig else ""
    script = _K8S_SCRIPT_TMPL.format(export=export, kubectl=kubectl)
    ok, text = _ssh_probe(name, node, script)
    if not ok:
        return {"ok": False, "detail": text, "current_namespace": "",
                "pods": [], "namespaces": []}
    sec = _parse_sections(text)
    pods, nss = _parse_k8s_pods(sec.get("K8S", ""))
    return {"ok": True, "detail": "",
            "current_namespace": (cluster.get("k8s", {}) or {}).get("namespace", "") or "",
            "pods": pods, "namespaces": nss}


# ---------------------------------------------------------------- SSH 探测

def _short(exc, limit=600):
    return " ".join(str(exc or "").split())[:limit]


def _ssh_probe(name, node, script=None):
    """在节点上执行只读组合脚本 -> (ok, text|detail)。"""
    try:
        from utils import ssh_check_output
    except ImportError as exc:                      # pragma: no cover - 环境缺失
        raise ApiError(503, "SSH 探测需要编排器 utils/paramiko：{}".format(exc))
    try:
        text = ssh_check_output(name, node, script or _PROBE_SCRIPT, quiet=True, timeout=20)
        return True, text
    except RuntimeError as exc:
        msg = str(exc)
        if "paramiko" in msg:
            raise ApiError(503, "SSH 探测需要 paramiko：pip install paramiko")
        return False, _short(msg)
    except Exception as exc:                        # paramiko 超时等
        return False, _short(exc)


def _find_node(cluster, name):
    for n in _all_nodes(cluster):
        if n["name"] == name:
            return n
    return None


# ---------------------------------------------------------------- API 实现

def api_nodes(handler, query):
    cpath, cluster, gpath, glob = _context(query)
    nodes = _all_nodes(cluster)
    roles = _role_map(nodes, glob)
    out = []
    for n in nodes:
        item = dict(n)
        if n.get("is_controller"):
            item["roles"] = ["controller"]
            item["expected_ports"] = []
        else:
            item["roles"] = roles.get(n["name"], [])
            item["expected_ports"] = _expected_ports(n, glob)
        out.append(item)
    if _cluster_nodes(cluster):
        mode = "ssh"
    elif cluster.get("controller") or "k8s" in cluster:
        mode = "k8s"
    else:
        mode = "unknown"
    return {"cluster": cpath, "global": gpath, "mode": mode,
            "venv": cluster.get("venv") or "", "nodes": out}


def api_node_test(handler, query, name):
    cpath, cluster, gpath, glob = _context(query)
    node = _find_node(cluster, name)
    if node is None:
        raise ApiError(404, "cluster {} 中没有节点 {}".format(cpath, name))
    expected = _expected_ports(node, glob)
    ok, text = _ssh_probe(name, node)
    if not ok:
        return {"ok": False, "detail": text, "kernel": None, "arch": None,
                "processes": [], "ports": []}
    sec = _parse_sections(text)
    listening = {p["port"] for p in _parse_ports(sec.get("SS", ""))
                 if p.get("state") == "LISTEN"}
    checks = [{"purpose": e["purpose"], "port": e["port"],
               "listening": e["port"] in listening} for e in expected]
    procs = [{"pid": p["pid"], "cmd": p["cmd"], "kind": _proc_kind(p["cmd"])}
             for p in _parse_procs(sec.get("PROCS", ""))]
    return {"ok": True, "detail": "", "kernel": sec.get("UNAME", ""),
            "arch": sec.get("ARCH", ""), "processes": procs, "ports": checks}


def api_node_ports(handler, query, name):
    cpath, cluster, gpath, glob = _context(query)
    node = _find_node(cluster, name)
    if node is None:
        raise ApiError(404, "cluster {} 中没有节点 {}".format(cpath, name))
    ok, text = _ssh_probe(name, node)
    if not ok:
        return {"ok": False, "detail": text, "ports": []}
    sec = _parse_sections(text)
    ps_meta = _parse_ps(sec.get("PS", ""))
    ports = _parse_ports(sec.get("SS", ""))
    warning = None
    for p in ports:
        procs = []
        for u in p.pop("users", []):
            meta = ps_meta.get(u["pid"]) or {}
            procs.append({"pid": u["pid"], "process": u["process"],
                          "user": meta.get("user") or ""})
        p["processes"] = procs
    if ports and not any(p["processes"] for p in ports):
        warning = "无法读取进程信息（可能需要 sudo 权限），仅显示端口"
    return {"ok": True, "ports": ports, "warning": warning}


ROUTES += [
    ("GET", r"^/api/nodes$", lambda h, q, m=None: api_nodes(h, q)),
    ("GET", r"^/api/nodes/(?P<name>[^/]+)/test$", api_node_test),
    ("GET", r"^/api/nodes/(?P<name>[^/]+)/ports$", api_node_ports),
    ("GET", r"^/api/nodes/(?P<name>[^/]+)/k8s$", api_node_k8s),
]
