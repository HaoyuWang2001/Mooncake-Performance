#!/usr/bin/env python3
"""Mooncake benchmark orchestrator (runs locally).

Reads cluster/global/scenario configs, generates env.sh, drives the remote
test stages over ssh/scp, pulls logs back and aggregates CSV metrics.

Structure:
  - parse_arguments(): CLI parsing + validation + config path collection
  - main(args, cfg_paths): orchestration loop, dispatches per config to
    run_basic_scenario() or run_pipeline_scenario() based on cfg["scenario"]
  - utils.py: generic helpers (ssh/scp execution, json/size parsing)
"""
import argparse
import csv
import glob
import ipaddress
import json
import os
import posixpath
import re
import atexit
import shutil
import signal
import shlex
import socket
import subprocess
import sys
import threading
import time
import urllib.parse

from utils import (StopRequested, _drain_chan, _ssh_client, _ssh_drop,
                   clear_stop, controller_desc, controller_diagnose,
                   controller_exec, controller_mkdir, controller_put,
                   controller_remove,
                   controller_run, controller_stream, get_node, is_k8s_node,
                   kubectl_exec_line, kubectl_prefix, load_json, log,
                   log_detail, parse_data_size, percentile,
                   request_stop, run_ssh, scp_pull, scp_push, set_stop_file,
                   set_verbose, ssh_check_output, stop_requested,
                   verbose_enabled)

# profiling matrix core (optional tools: spdiag / perf / bpftrace)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "profiling"))
try:
    import prof_core
except ImportError:  # profiling dir missing -> feature silently off
    prof_core = None

# pipeline rendezvous KV (etcd v3 gRPC gateway, stdlib only); lives in
# scripts/ so both this orchestrator and the remote client use one impl
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"))
import bench_etcd

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIGS_DIR = os.path.join(ROOT, "configs")
K8S_PRERUN = os.path.join(ROOT, "k8s-deployment", "scripts", "k8s-prerun.sh")

# --- cooperative stop context (WebUI stop file / Ctrl+C) -------------------
_STOP_CTX = {}
_STOP_SIGNALS = 0


def _install_stop_handlers():
    def _handler(signum, _frame):
        global _STOP_SIGNALS
        _STOP_SIGNALS += 1
        if _STOP_SIGNALS >= 2:
            log("[STOP] second signal -> force exit (cleanup skipped)")
            os._exit(130)
        log("[STOP] signal {}: entering cleanup "
            "(press again to force)".format(signum))
        request_stop()
    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, _handler)
        except Exception:
            pass


def _check_stop():
    if stop_requested():
        raise StopRequested()
SCRIPTS_DIR = os.path.join(ROOT, "scripts")
RESULTS_DIR = os.path.join(ROOT, "outputs")
# RESULT rows whose first field is a wall-clock timestamp (HH:MM:SS.micro);
# legacy rows start with the op name directly (see parse_benchmark_log)
TS_PREFIX = re.compile(r"^\d{2}:\d{2}:\d{2}")
# store interface families (mirror scripts/mooncake_benchmark_client.py so the
# orchestrator can validate operation values and choose staging behaviour)
PIPELINE_PUT_IFACES = ("put", "put_batch", "put_from")
PIPELINE_GET_IFACES = ("get", "get_batch", "get_into", "get_buffer")
PIPELINE_PUT_BATCH_IFACES = ("put_batch",)
PIPELINE_GET_BATCH_IFACES = ("get_batch",)
BASIC_CSV_HEADER = [
    "scenario", "work_mode", "operation", "node", "topology", "protocol",
    "num_clients", "num_threads",
    "data_size", "key_count", "batch_size", "put_ratio", "duration", "warmup_duration",
    "target_qps",
    "total", "total_bytes", "wall_s",
    "throughput_mb_s", "throughput_gb_s",
    "avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms",
    "p9999_ms",
    "ok", "fail", "qps", "fail_rate",
]
PIPELINE_CSV_HEADER = [
    "config_name",
    "scenario", "work_mode", "operation", "role", "node", "instance", "topology", "protocol",
    "num_threads", "notify_threads",
    "data_size", "key_count", "batch_size", "duration", "warmup_duration",
    "writer_count", "reader_count", "notify_count", "store_count",
    "base_qps", "qps_multiple", "target_qps", "notify_pipeline", "traffic_mode",
    "total", "success", "fail", "fail_rate",
    "total_bytes", "wall_s", "qps",
    "throughput_mb_s", "throughput_gb_s",
    "avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms",
    "p9999_ms",
    "queue_wait_avg_ms", "queue_wait_max_ms", "max_depth",
]


def collapse_config_name(rows):
    """Fill config_name only on the first row of each group (previous-row
    comparison), so repeated values stay blank for a grouped display."""
    out = []
    prev = None
    for r in rows:
        r2 = dict(r)
        name = r2.get("config_name", "")
        r2["config_name"] = name if name and name != prev else ""
        if name:
            prev = name
        out.append(r2)
    return out


# --------------------------------------------------------------------------
# domain planning
# --------------------------------------------------------------------------

def remote_log_path(remote_log_dir, scenario, name):
    return "{}/{}.{}.log".format(remote_log_dir, scenario, name)


def data_size_list(cfg):
    """Validate and return the scenario's data_size entries.

    ``data_size`` must be a non-empty array of size strings (e.g.
    ``["8MB"]`` or ``["128KB", "1MB", "4MB", "8MB"]``). Both basic and
    pipeline scenarios sweep over the entries: one full run per size.
    """
    raw = cfg.get("data_size")
    if not isinstance(raw, list) or not raw:
        raise RuntimeError(
            'data_size must be a non-empty array, e.g. ["8MB"] '
            'or ["128KB", "1MB", "4MB", "8MB"] (got {})'.format(raw))
    return [str(s) for s in raw]


def build_runs(cfg):
    """Resolve the basic run matrix.

    The matrix is the cross product: every params (num_clients,
    num_threads) combo runs once per data_size, ordered outer-by-size so
    the remote value.bin rebuild happens once per size group.

    Returns a list of (num_clients, num_threads, data_size) tuples.
    """
    sizes = data_size_list(cfg)
    params = cfg["params"]
    combos = [(int(p["num_clients"]), int(p["num_threads"])) for p in params]
    if not combos:
        raise RuntimeError("params resolved to an empty run matrix")
    return [(c, t, ds) for ds in sizes for (c, t) in combos]


def _store_total_bytes(globals_cfg):
    """global.json store_total_size -> bytes, or None when unset (infinite
    store, legacy unconstrained behaviour)."""
    v = globals_cfg.get("store_total_size")
    if v in (None, "", 0):
        return None
    return parse_size_bytes(v)


def _basic_store_check(globals_cfg, cfg):
    """basic mode: total key space (key_count x data_size, shared across
    clients) must be strictly smaller than store_total_size when that is
    configured; checked per sweep size."""
    store = _store_total_bytes(globals_cfg)
    if store is None:
        return
    kc = int(cfg.get("key_count", 1000) or 1000)
    for ds in data_size_list(cfg):
        ds_bytes = parse_data_size(ds)
        if kc * ds_bytes >= store:
            raise RuntimeError(
                "basic key space {} keys x {}B ({}) = {}B >= "
                "store_total_size {}B; reduce key_count/data_size or raise "
                "store_total_size".format(kc, ds_bytes, ds,
                                          kc * ds_bytes, store))


def _pipeline_key_budget(globals_cfg, writer_count, ds_bytes, cfg_key_count,
                         batch_size, ds_label):
    """Resolve the PER-WRITER key-space cap against store_total_size.

    Returns (per_writer_key_count, auto):
    - store unset (infinite): explicit key_count passes through; 0/absent
      stays 0 = unbounded (legacy behaviour)
    - explicit key_count: writers x key_count x data_size must be STRICTLY
      smaller than the store, else the scenario fails fast
    - key_count unset: auto cap = floor(store x 0.9 / data_size / writers)
      (0.9 = object-metadata + in-flight-batch headroom) and each writer
      wraps inside its cap with untimed delete-before-write (method a)."""
    store = _store_total_bytes(globals_cfg)
    if store is None:
        return int(cfg_key_count or 0), False
    if cfg_key_count:
        need = writer_count * cfg_key_count * ds_bytes
        if need >= store:
            raise RuntimeError(
                "pipeline key space {} writers x {} keys x {}B ({}) = {}B "
                ">= store_total_size {}B; reduce key_count/data_size or "
                "raise store_total_size".format(
                    writer_count, cfg_key_count, ds_bytes, ds_label, need,
                    store))
        return int(cfg_key_count), False
    usable = int(store * 0.9)
    per = usable // (ds_bytes * writer_count) if ds_bytes and writer_count else 0
    if per < max(1, int(batch_size or 1)):
        raise RuntimeError(
            "pipeline auto key space too small: store_total_size {}B x 0.9 "
            "/ ({}B x {} writers) = {} keys < batch_size {}; the store is "
            "too small for this data size".format(
                store, ds_bytes, writer_count, per, int(batch_size or 1)))
    return per, True


def pipeline_plan(cfg, node_count=1):
    """Derive the writer/reader process plan from a pipeline config.

    Cluster-oriented: both roles start count_per_node processes on EVERY
    node of the cluster (N = cluster.json nodes on ssh / Ready k8s nodes),
    so:
      writer total = writer.count_per_node * N
      reader pool = reader.count_per_node * N
    ("1w*xr" is simply writer.count_per_node=1 + notify_count=x.)

    Port bases are configurable per role — writer.port_base (default 9101,
    the writer HTTP /start port) and reader.port_base (default 9201, the
    reader HTTP /notify port). Ports number PER NODE: the i-th process of
    a node gets port_base + i (k8s pods all use port_base itself — every
    pod has its own network namespace).

    Total write QPS = base_qps * qps_multiple, split evenly across all
    writers. Writers get global gids 1..W and readers global indices 0..R-1
    in node order. The plan carries the node SLOT; the caller materializes
    concrete node names (ssh cluster nodes / k8s pod names) into
    entry["node"]. The reader pool must cover notify_count.

    The legacy writer/reader client_nodes lists are gone — the benchmark
    faces the whole cluster, not per-role node assignments.
    """
    writer_cfg = cfg.get("writer", {}) or {}
    reader_cfg = cfg.get("reader", {}) or {}
    client_cfg = cfg.get("client", {}) or {}
    w_cpn = int(writer_cfg.get("count_per_node", 1))
    r_cpn = int(reader_cfg.get("count_per_node", 1))
    w_base = int(writer_cfg.get("port_base", 9101))
    r_base = int(reader_cfg.get("port_base", 9201))
    # ---- qps is PER-NODE: base_qps (x qps_multiple) is the rate ONE node
    # sustains; every client of that node shares it equally. Cluster total
    # = per_node x N. ----
    base_qps = float(cfg.get("base_qps", 0))
    qps_multiple = float(cfg.get("qps_multiple", 1))
    per_node_qps = base_qps * qps_multiple
    total_qps = per_node_qps * node_count
    if client_cfg:
        # ---- dual-role deployment (rank/recall minimal shape): one process
        # per entry hosting BOTH writer and reader behind one HTTP port.
        # "client" block presence selects this mode. ----
        c_cpn = int(client_cfg.get("count_per_node", 1))
        c_base = int(client_cfg.get("port_base", 9301))
        clients = []
        for slot in range(node_count):
            for local in range(c_cpn):
                clients.append({"id": len(clients), "slot": slot,
                                "local": local, "port": c_base + local})
        per_client_qps = (per_node_qps / c_cpn) if c_cpn else per_node_qps
        return {
            "dual": True,
            "clients": clients,
            # aliases so the generic ledger/cleanup/CSV paths keep working
            "writers": clients,
            "readers": clients,
            "total_qps": total_qps,
            "per_node_qps": per_node_qps,
            "per_writer_qps": per_client_qps,
            "notify_count": int(cfg.get("notify_count", 1)),
            "notify_threads": int(writer_cfg.get("notify_threads", 1)),
            "reader_threads": int(reader_cfg.get("num_threads", 1)),
            "writer_port_base": c_base,
            "reader_port_base": c_base,
        }
    writers = []
    readers = []
    for slot in range(node_count):
        for local in range(w_cpn):
            writers.append({"gid": len(writers) + 1, "slot": slot,
                            "local": local, "port": w_base + local})
        for local in range(r_cpn):
            readers.append({"index": len(readers), "slot": slot,
                            "local": local, "port": r_base + local})
    per_writer_qps = (per_node_qps / w_cpn) if w_cpn else per_node_qps
    return {
        "dual": False,
        "writers": writers,
        "readers": readers,
        "total_qps": total_qps,
        "per_node_qps": per_node_qps,
        "per_writer_qps": per_writer_qps,
        "notify_count": int(cfg.get("notify_count", 1)),
        "notify_threads": int(writer_cfg.get("notify_threads", 1)),
        "reader_threads": int(reader_cfg.get("num_threads", 1)),
        "writer_port_base": w_base,
        "reader_port_base": r_base,
    }


# --------------------------------------------------------------------------
# shared orchestration infrastructure (both scenario families)
# --------------------------------------------------------------------------

def apply_cli_overrides(cfg, args):
    # --protocol has the highest priority and overrides the JSON config value
    if getattr(args, "protocol", None):
        cfg["protocol"] = args.protocol


def scenario_paths(globals_cfg, ts, cfg_name):
    pod_ws = globals_cfg.get("_pod_workspace")
    if pod_ws:
        # k8s: paths inside the pod (workspace/scripts, workspace/logs/<ts>)
        remote_dir = pod_ws
        remote_scripts = pod_ws + "/scripts"
        remote_log_dir = "{}/logs/{}".format(pod_ws, ts)
    else:
        remote_dir = globals_cfg["remote_dir"]
        remote_scripts = remote_dir + "/scripts"
        remote_log_dir = "{}/{}/logs".format(globals_cfg["remote_results_dir"], ts)
    remote_runner_basic = remote_scripts + "/run_benchmark_basic.sh"
    remote_runner_pipeline = remote_scripts + "/run_benchmark_pipeline.sh"
    local_log_dir = os.path.join(RESULTS_DIR, ts, "logs", cfg_name)
    return (remote_dir, remote_scripts, remote_log_dir,
            remote_runner_basic, remote_runner_pipeline, local_log_dir)


def remote_profiling_dir(remote_log_dir, cfg_name):
    """POSIX join for the remote per-config profiling dir. os.path.join MUST
    NOT be used on remote paths: running the orchestrator on Windows inserts
    a backslash, which bash escapes into a literal 'logsprofiling' while scp
    (SFTP) looks for a literal backslash name. The <cfg_name> subdirectory
    isolates each config's profiling artifacts so multi-config runs never
    overwrite each other."""
    return "{}/profiling/{}".format(remote_log_dir, cfg_name)


def resolve_venv(cluster, globals_cfg):
    """Resolve the server-side Python interpreter for the benchmark client.

    Preferred form: cluster.json carries named venvs and global.json selects
    one by name:
      cluster.json: "venvs": {"mooncake-benchmark": "/…/bin/python", …}
      global.json:  "venv": "mooncake-benchmark"
    Empty name with exactly one entry auto-selects it; an unknown name fails
    loudly listing the available ones. Legacy form (a bare string
    cluster.json "venv": "/…/bin/python") is still accepted unchanged.
    """
    venvs = cluster.get("venvs") or {}
    name = str(globals_cfg.get("venv", "") or "")
    if isinstance(venvs, dict) and venvs:
        if name:
            if name not in venvs:
                raise RuntimeError(
                    "global.json venv={!r} not found in cluster.json venvs; "
                    "available: {}".format(name, ", ".join(sorted(venvs))))
            return venvs[name]
        if len(venvs) == 1:
            return next(iter(venvs.values()))
        raise RuntimeError(
            "global.json venv is empty but cluster.json venvs has {} entries; "
            "pick one of: {}".format(len(venvs), ", ".join(sorted(venvs))))
    legacy = cluster.get("venv", "")
    if legacy:
        return legacy  # legacy bare-string form, kept for old cluster.json
    return ""


def parse_size_bytes(value):
    """Parse an int/float byte count or strings like 512KB / 1GB (1024 base)."""
    if isinstance(value, bool):
        raise RuntimeError("invalid size (bool): {!r}".format(value))
    if isinstance(value, (int, float)):
        n = int(value)
    else:
        raw = str(value).strip().upper()
        units = {"B": 1, "KB": 1024, "MB": 1024 ** 2,
                 "GB": 1024 ** 3, "TB": 1024 ** 4}
        n = None
        for unit in sorted(units, key=len, reverse=True):
            if raw.endswith(unit):
                try:
                    n = int(float(raw[:-len(unit)].strip()) * units[unit])
                except ValueError:
                    pass
                break
        if n is None:
            try:
                n = int(float(raw))
            except ValueError:
                raise RuntimeError("invalid size string: {!r}".format(value))
    if n <= 0:
        raise RuntimeError("size must be positive: {!r}".format(value))
    return n


def build_cluster_session(cluster, globals_cfg, remote_dir):
    master_type = globals_cfg.get("master_type", "") or ""
    ub_device = str(globals_cfg.get("protocol", {}).get("ub", {}).get("device", "") or "")
    if ub_device.strip().lower() == "auto":
        ub_device = ""
    session = {
        "SETUP_MASTER_TYPE": master_type,
        "SETUP_UB_DEVICE": ub_device,
        "ORCH_VENV": resolve_venv(cluster, globals_cfg),
        "SETUP_LOCAL_BUFFER_SIZE": str(parse_size_bytes(
            globals_cfg.get("local_buffer_size", 8589934592))),
        "SETUP_TENANT_ID": globals_cfg.get("tenant_id", "default"),
        "SETUP_RANDOM_SEED": str(globals_cfg.get("random_seed", 42)),
        "BENCH_REMOTE_DATA_DIR": globals_cfg.get("remote_data_dir", remote_dir + "/data"),
        # NOTE: MC_GLOBAL_SEGMENT_SIZE, MC_YLT_LOG_LEVEL, MC_YLT_LOG_MAX_FILE_SIZE
        # and MC_YLT_LOG_MAX_FILES are intentionally NOT generated here. Their
        # ownership moved to scripts/env.mooncake.sh (hand-editable tuning
        # panel, sourced by the runner scripts after env.sh) — see that file.
        # Same for MC_HIFREQ_LOG_SAMPLE_RATE (moved earlier).
    }
    if master_type == "etcd":
        # etcd mode: only the etcd URI is exported; the client passes it to
        # both setup params. master_addr/metadata_server (if set) are dormant.
        session["SETUP_ETCD_ENDPOINTS"] = _resolve_k8s_addr(
            globals_cfg.get("etcd_endpoints", ""), cluster)
    else:
        # http mode: only rpc master + http metadata are exported
        session["SETUP_MASTER_ADDR"] = _resolve_k8s_addr(
            globals_cfg.get("master_addr", ""), cluster)
        session["SETUP_METADATA_SERVER"] = _resolve_k8s_addr(
            globals_cfg.get("metadata_server", ""), cluster)
        # the SAME etcd (when configured) still serves the pipeline
        # rendezvous (reader registration / writer discovery) — the client
        # consumes it independently of the master connection mode
        if (globals_cfg.get("etcd_endpoints", "") or "").strip():
            session["SETUP_ETCD_ENDPOINTS"] = _resolve_k8s_addr(
                globals_cfg.get("etcd_endpoints", ""), cluster)
    return session


# --------------------------------------------------------------------------
# k8s support: node enrichment, DNS->IP resolution, pod lifecycle
# --------------------------------------------------------------------------

def enrich_k8s_nodes(cluster):
    """Merge cluster-level k8s settings (namespace/kubecontext) into every
    k8s-pod node (_ns/_ctx), so the utils transports need no cluster arg."""
    k8s = cluster.get("k8s") or {}
    ns_default = k8s.get("namespace", "default")
    ctx_default = k8s.get("kubecontext", "")
    for node in (cluster.get("nodes") or {}).values():
        if node.get("kind") == "k8s-pod":
            node["_ns"] = node.get("namespace") or ns_default
            node["_ctx"] = node.get("kubecontext") or ctx_default


def _first_ip(text):
    """Extract the first valid IP from kubectl output (may contain warnings)."""
    for cand in re.findall(r"[0-9A-Fa-f:.]+", text or ""):
        cand = cand.strip(".")
        try:
            ipaddress.ip_address(cand)
            return cand
        except ValueError:
            continue
    return None


def _resolve_k8s_svc(svc, ns, ctx, controller=None):
    """Resolve a k8s Service DNS name to its ClusterIP via kubectl (works
    even when the orchestrator cannot reach cluster DNS). Headless services
    (no ClusterIP) fall back to the first endpoint IP. -> ip or None."""
    def _get(kind, args):
        if controller is not None:
            out, rc = controller_run(controller, ["-n", ns] + args,
                                     check=False)
        else:
            base = ["kubectl"]
            if ctx:
                base += ["--context", ctx]
            try:
                r = subprocess.run(base + ["-n", ns] + args,
                                   capture_output=True, text=True, timeout=15)
                out = (r.stdout or "") + (r.stderr or "")
                rc = r.returncode
            except Exception as exc:
                out, rc = str(exc), 1
        if rc != 0:
            log("[WARN] k8s dns: kubectl -n {} get {} {} failed (rc={}): {}"
                "".format(ns, kind, svc, rc, out.strip()[:400]))
            return None
        if out.strip() == "None":
            return "None"
        ip = _first_ip(out)
        if not ip and out.strip():
            log("[WARN] k8s dns: kubectl -n {} get {} {} returned non-IP "
                "output: {}".format(ns, kind, svc, out.strip()[:400]))
        return ip

    out = _get("svc", ["get", "svc", svc, "-o",
                       "jsonpath={.spec.clusterIP}"])
    if out and out != "None":
        return out
    out = _get("endpoints", ["get", "endpoints", svc, "-o",
                             "jsonpath={.subsets[0].addresses[0].ip}"])
    if out and out != "None":
        return out
    if controller is not None:
        try:
            log("[k8s] controller diagnostics:\n" +
                controller_diagnose(controller))
        except Exception as exc:
            log("[WARN] k8s controller diagnostics failed: {}".format(exc))
    return None

def _resolve_host(host, cluster):
    """Resolve one hostname to an IP. Order: already an IP -> kubectl svc
    (for k8s Service DNS forms) -> getaddrinfo -> None (keep original)."""
    if not host:
        return None
    try:
        ipaddress.ip_address(host)
        return host          # already an IP
    except ValueError:
        pass
    # k8s Service DNS forms: <svc>.<ns>.svc.cluster.local / <svc>.<ns>.svc /
    # <svc>.<ns> (2-part names are tried as svc.ns first, then DNS)
    parts = host.split(".")
    svc = ns = None
    # <svc>.<ns>.svc.cluster.local (standard k8s service FQDN)
    if len(parts) >= 4 and parts[-1] == "local" \
            and parts[-2] == "cluster" and parts[-3] == "svc":
        svc, ns = parts[0], parts[1]
    # <svc>.<ns>.svc
    elif len(parts) >= 3 and parts[-1] == "svc":
        svc, ns = parts[0], parts[1]
    # <svc>.<ns>
    elif len(parts) == 2:
        svc, ns = parts[0], parts[1]
    if svc:
        k8s = cluster.get("k8s") or {}
        ip = _resolve_k8s_svc(svc, ns, k8s.get("kubecontext", ""),
                              cluster.get("_controller"))
        if ip:
            log("k8s dns: {} -> {} (kubectl svc {})".format(host, ip, svc))
            return ip
    try:
        infos = socket.getaddrinfo(host, None, socket.AF_INET)
        ip = infos[0][4][0]
        log("k8s dns: {} -> {} (resolver)".format(host, ip))
        return ip
    except Exception:
        return None


def _resolve_k8s_addr(value, cluster):
    """Resolve every hostname inside an address value to an IP. Accepted
    forms: bare host / host:port / http://host[:port]/path / comma-separated
    etcd endpoint list. Unresolvable parts are kept as-is with a [WARN]."""
    if not value:
        return value

    def resolve_one(addr):
        if "://" in addr:
            p = urllib.parse.urlsplit(addr)
            host, port = p.hostname, p.port
            ip = _resolve_host(host, cluster)
            if not ip:
                log("[WARN] k8s dns: cannot resolve {}, kept as-is".format(host))
                return addr
            netloc = ip if port is None else "{}:{}".format(ip, port)
            return urllib.parse.urlunsplit((p.scheme, netloc, p.path, p.query, p.fragment))
        host, _, port = addr.rpartition(":")
        if not host:          # bare host (no port)
            ip = _resolve_host(addr, cluster)
            if not ip:
                log("[WARN] k8s dns: cannot resolve {}, kept as-is".format(addr))
                return addr
            return ip
        ip = _resolve_host(host, cluster)
        if not ip:
            log("[WARN] k8s dns: cannot resolve {}, kept as-is".format(host))
            return addr
        return "{}:{}".format(ip, port)

    return ",".join(resolve_one(a.strip()) for a in str(value).split(","))


def http_probe(url, method="GET", timeout=5):
    """Direct HTTP probe from the orchestrator (k8s pods are directly
    reachable). Returns the body text; raises on connection/HTTP errors."""
    import urllib.request
    req = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", "replace")


def _validate_master_mode(globals_cfg):
    """master_type selects how the client connects to the mooncake master.
    Required field: 'http' (master_addr + metadata_server) or 'etcd'
    (etcd_endpoints). Both address sets may coexist in the config; only the
    selected one takes effect."""
    master_type = globals_cfg.get("master_type", "") or ""
    if not master_type:
        raise RuntimeError(
            "global config requires 'master_type' = \"http\" or \"etcd\" "
            "to select the master connection mode (configs/global.json)")
    if master_type not in ("http", "etcd"):
        raise RuntimeError(
            "invalid master_type {!r}: must be \"http\" or \"etcd\"".format(master_type))
    if master_type == "etcd":
        if not (globals_cfg.get("etcd_endpoints", "") or ""):
            raise RuntimeError(
                "master_type=etcd requires non-empty 'etcd_endpoints' "
                "(e.g. \"etcd://ip:2379;ip:2379\") in configs/global.json")
    else:
        if not (globals_cfg.get("master_addr", "") or ""):
            raise RuntimeError(
                "master_type=http requires non-empty 'master_addr' "
                "in configs/global.json")


def resolve_store_node_ips(cluster, store_node_names):
    """Resolve global.json 'mooncake_store_nodes' (node NAMES from
    cluster.json) into the set of their management IPs. Unknown names fail
    loudly: a store node that cluster.json does not define is a config error.
    """
    ips = set()
    for name in store_node_names or []:
        n = get_node(cluster, name)
        # k8s store nodes outside this config's involved set have no pod
        # (no "ip") — topology is informational only, fall back to the pod
        # name / node name so detection stays best-effort instead of a crash
        ips.add(n.get("ip") or n.get("_pod") or name)
    return ips


# --------------------------------------------------------------------------
# k8s: controller + namespace/workspace lifecycle
# --------------------------------------------------------------------------

def build_controller(cluster):
    c = dict(cluster.get("controller") or {})
    if not c.get("ip"):
        raise RuntimeError("cluster.json controller.ip is required for --k8s")
    k8s = cluster.get("k8s") or {}
    c["_local"] = (c["ip"] in ("127.0.0.1", "localhost", "::1")
                   or bool(c.get("local")))
    c["_kubectl"] = c.get("kubectl") or "kubectl"
    c["_ctx"] = k8s.get("kubecontext", "") or ""
    return c


def k8s_settings(cluster):
    k8s = cluster.get("k8s") or {}
    if not k8s.get("namespace"):
        raise RuntimeError("cluster.json k8s.namespace is required for --k8s")
    if not k8s.get("image"):
        raise RuntimeError("cluster.json k8s.image is required for --k8s")
    for key in ("controller_workspace", "pod_workspace"):
        if not k8s.get(key):
            raise RuntimeError(
                "cluster.json k8s.{} is required for --k8s".format(key))
    return k8s


def k8s_run_label(ts, cfg_name):
    return re.sub(r"[^a-zA-Z0-9._-]", "-", "{}-{}".format(ts, cfg_name))


def k8s_pod_name(ts, cfg_name):
    name = "mc-{}-{}".format(ts, cfg_name).lower()
    name = re.sub(r"[^a-z0-9-]", "-", name).strip("-")
    return name[:63].strip("-")


def k8s_workspace_dirs(cluster, dry_run=False):
    k8s = k8s_settings(cluster)
    c = cluster["_controller"]
    root = k8s["controller_workspace"]
    subs = ("scripts", "data", "logs", "pods", "push-tmp", "pull-tmp")
    if dry_run:
        for sub in subs:
            log("[k8s] (dry-run) would create {}/{}".format(root, sub))
        return root
    for sub in subs:
        controller_mkdir(c, posixpath.join(root, sub))
    log("[k8s] workspace ready on {}: {} ({})".format(
        controller_desc(c), root, ",".join(subs)))
    return root


def k8s_prerun(cluster, dry_run=False):
    """Run the user-maintained pre-run script on the controller (k8s only).
    File: k8s-deployment/scripts/k8s-prerun.sh — absent => silently skip.
    Non-zero rc => RuntimeError (abort the benchmark)."""
    script = K8S_PRERUN
    if not os.path.isfile(script):
        return
    k8s = k8s_settings(cluster)
    c = cluster["_controller"]
    remote_path = posixpath.join(k8s["controller_workspace"],
                                 "k8s-prerun.sh")
    if dry_run:
        log("[k8s] (dry-run) would run pre-run script -> {}:{}".format(
            controller_desc(c), remote_path))
        return
    log("[k8s] pre-run script: {} -> {}:{}".format(
        os.path.relpath(script, ROOT), controller_desc(c), remote_path))
    controller_put(c, [script], k8s["controller_workspace"])
    log("[k8s] prerun: bash {}".format(remote_path))
    t0 = time.time()
    out, rc = controller_exec(c, "bash " + remote_path)
    log("[k8s] pre-run done rc={} elapsed={:.1f}s".format(
        rc, time.time() - t0))
    if rc != 0:
        raise RuntimeError("k8s pre-run script failed (rc={}): {}".format(
            rc, out.strip()[:400]))


def reap_k8s_namespaces(cluster):
    """Delete namespaces recorded in .state by a crashed previous run."""
    c = cluster["_controller"]
    ctx = c.get("_ctx", "")
    removed = []
    for e in state_load():
        if e.get("type") != "k8s-namespace":
            continue
        if e.get("controller") != c.get("ip") or (e.get("context") or "") != ctx:
            log("[WARN] .state: k8s namespace {} belongs to controller {}/{}; kept"
                "".format(e.get("namespace"), e.get("controller"),
                          e.get("context") or "-"))
            continue
        ns = e.get("namespace")
        _, rc = controller_run(c, ["get", "namespace", ns], check=False)
        if rc != 0:
            removed.append(e)
            continue
        label, _ = controller_run(
            c, ["get", "namespace", ns, "-o",
                "jsonpath={.metadata.labels.managed-by}"], check=False)
        if label.strip() != "mcbench":
            log("[WARN] .state: namespace {} exists but is not managed by "
                "mcbench; kept".format(ns))
            continue
        log("[k8s] reaping leftover namespace {} from previous run".format(ns))
        controller_run(c, ["delete", "namespace", ns,
                           "--wait=true", "--timeout=180s"], check=False)
        removed.append(e)
    if removed:
        keys = {(e.get("controller"), e.get("context") or "", e.get("namespace"))
                for e in removed}
        kept = [e for e in state_load()
                if (e.get("controller"), e.get("context") or "",
                    e.get("namespace")) not in keys]
        _state_write(kept)


def ensure_k8s_namespace(cluster, ts, dry_run=False):
    k8s = k8s_settings(cluster)
    c = cluster["_controller"]
    ns = k8s["namespace"]
    if dry_run:
        log("[k8s] (dry-run) would create namespace {}".format(ns))
        return
    _, rc = controller_run(c, ["get", "namespace", ns], check=False)
    if rc == 0:
        raise RuntimeError(
            "k8s namespace {!r} already exists; refusing to reuse it "
            "(delete it or run --clean-up)".format(ns))
    controller_run(c, ["create", "namespace", ns])
    controller_run(c, ["label", "namespace", ns,
                       "app=mooncake-benchmark", "managed-by=mcbench",
                       "--overwrite"])
    state_append([{"type": "k8s-namespace", "controller": c.get("ip"),
                   "context": c.get("_ctx", ""), "namespace": ns, "ts": ts,
                   "controller_workspace": k8s.get("controller_workspace", "")}])
    log("[k8s] namespace {} created".format(ns))
    log("[k8s] pods:     {} get pods -o wide".format(" ".join(
        kubectl_prefix({"_ctx": c.get("_ctx", ""), "_ns": ns}))))


def cleanup_k8s_namespace(cluster, dry_run=False):
    k8s = k8s_settings(cluster)
    c = cluster["_controller"]
    ns = k8s["namespace"]
    if dry_run:
        log("[k8s] (dry-run) would delete namespace {}".format(ns))
        return
    _, rc = controller_run(c, ["get", "namespace", ns], check=False)
    if rc == 0:
        label, _ = controller_run(
            c, ["get", "namespace", ns, "-o",
                "jsonpath={.metadata.labels.managed-by}"], check=False)
        if label.strip() == "mcbench":
            t0 = time.time()
            controller_run(c, ["delete", "namespace", ns,
                               "--wait=true", "--timeout=180s"], check=False)
            log("[k8s] namespace {} deleted (elapsed={:.1f}s)".format(
                ns, time.time() - t0))
        else:
            log("[WARN] namespace {} is not managed by mcbench; not deleted"
                "".format(ns))
    keys = {(c.get("ip"), c.get("_ctx", ""), ns)}
    kept = [e for e in state_load()
            if (e.get("controller"), e.get("context") or "",
                e.get("namespace")) not in keys]
    _state_write(kept)


def cleanup_k8s_workspace(cluster, dry_run=False):
    k8s = k8s_settings(cluster)
    c = cluster["_controller"]
    root = k8s["controller_workspace"]
    for sub in ("scripts", "data", "logs", "pods", "push-tmp", "pull-tmp"):
        if dry_run:
            log("[k8s] (dry-run) would remove {}/{}".format(root, sub))
            continue
        controller_remove(c, posixpath.join(root, sub))
    log("[k8s] controller workspace cleaned: {}".format(root))


def _render_pod_yaml(cluster, ts, cfg_name):
    k8s = k8s_settings(cluster)
    run_label = k8s_run_label(ts, cfg_name)
    pod = k8s_pod_name(ts, cfg_name)
    with open(_pod_template_path(cluster), "r", encoding="utf-8") as f:
        text = f.read()
    repl = {
        "{{MC_POD_NAME}}": pod,
        "{{MC_IMAGE}}": k8s["image"],
        "{{MC_NAMESPACE}}": k8s["namespace"],
        "{{MC_RUN_LABEL}}": run_label,
        "{{MC_ROLE_LABEL}}": "client-0",
        "{{MC_CONFIGMAP_NAME}}": _configmap_name(run_label),
        "{{MC_SCRIPTS_MOUNT}}": k8s["pod_workspace"] + "/scripts",
        "{{MC_WORKDIR_MOUNT}}": k8s["pod_workspace"],
    }
    for key, val in repl.items():
        text = text.replace(key, val)
    if "{{MC_" in text:
        raise RuntimeError("pod template has unresolved {{MC_*}} placeholders")
    return pod, run_label, text


def _log_pod_events(controller, ns, pod):
    """Best-effort: dump the tail of `kubectl describe pod` on failure."""
    try:
        out, rc = controller_run(controller, ["-n", ns, "describe", "pod", pod],
                                 check=False)
        if rc == 0 and out.strip():
            lines = out.strip().splitlines()
            log("[k8s] pod {} describe (tail):\n{}".format(
                pod, "\n".join(lines[-25:])))
    except Exception:
        pass


_K8S_FATAL_WAITING = {
    "ErrImageNeverPull", "ErrImagePull", "ImagePullBackOff",
    "InvalidImageName", "CreateContainerConfigError",
    "CreateContainerError", "CrashLoopBackOff",
}
_K8S_FATAL_EVENTS = {
    "FailedScheduling", "ErrImageNeverPull", "ErrImagePull",
    "ImagePullBackOff", "InvalidImageName", "CreateContainerConfigError",
    "CreateContainerError", "FailedCreatePodSandBox", "FailedMount",
}


def _k8s_pod_events(controller, ns, pod, limit=20):
    """Best-effort pod events (reason/message/count), for anomaly reporting."""
    out, rc = controller_run(
        controller,
        ["-n", ns, "get", "events", "--field-selector",
         "involvedObject.name=" + pod,
         "-o", "custom-columns=REASON:.reason,MESSAGE:.message,COUNT:.count"],
        check=False)
    if rc != 0 or not out.strip():
        return []
    lines = [ln.rstrip() for ln in out.strip().splitlines() if ln.strip()]
    return lines[:limit]


def _k8s_fatal_events(events):
    fatal = []
    for ln in events:
        reason = ln.split()[0] if ln.split() else ""
        if reason in _K8S_FATAL_EVENTS:
            fatal.append(ln)
    return fatal


def wait_k8s_pod_ready(cluster, pod, timeout=60, poll=3.0):
    """Poll pod readiness; fail fast on fatal reasons/events.
    Returns (podIP, nodeName). Raises RuntimeError with a clear reason."""
    k8s = k8s_settings(cluster)
    c = cluster["_controller"]
    ns = k8s["namespace"]
    deadline = time.time() + timeout
    phase = "?"
    while True:
        out, _ = controller_run(
            c, ["-n", ns, "get", "pod", pod, "-o",
                "jsonpath={.status.phase}"], check=False)
        phase = out.strip() or phase
        out, _ = controller_run(
            c, ["-n", ns, "get", "pod", pod, "-o",
                "jsonpath={.status.conditions[?(@.type=='Ready')].status}"],
            check=False)
        if out.strip() == "True":
            ipout, rc = controller_run(
                c, ["-n", ns, "get", "pod", pod, "-o",
                    "jsonpath={.status.podIP}"], check=False)
            ip = ipout.strip()
            if rc == 0 and ip:
                nodeout, _ = controller_run(
                    c, ["-n", ns, "get", "pod", pod, "-o",
                        "jsonpath={.spec.nodeName}"], check=False)
                return ip, nodeout.strip()
        reasons, _ = controller_run(
            c, ["-n", ns, "get", "pod", pod, "-o",
                "jsonpath={.status.containerStatuses[*].state.waiting.reason}"],
            check=False)
        waiting = [r for r in reasons.split() if r in _K8S_FATAL_WAITING]
        events = _k8s_pod_events(c, ns, pod)
        fatal_events = _k8s_fatal_events(events)
        if waiting or fatal_events:
            if events:
                log("[k8s] pod {} events:\n{}".format(pod, "\n".join(events)))
            _log_pod_events(c, ns, pod)
            reason = waiting[0] if waiting else fatal_events[0].split()[0]
            detail = fatal_events[0] if fatal_events else ""
            raise RuntimeError(
                "pod {} failed to start (phase={}, reason={}){}".format(
                    pod, phase, reason, (": " + detail) if detail else ""))
        if time.time() >= deadline:
            if events:
                log("[k8s] pod {} events:\n{}".format(pod, "\n".join(events)))
            _log_pod_events(c, ns, pod)
            raise RuntimeError(
                "pod {} not Ready in {}s (phase={})".format(
                    pod, int(timeout), phase))
        time.sleep(poll)


def ensure_k8s_pod(cluster, ts, cfg_name, dry_run=False):
    """Create the per-config client pod and register a runtime node."""
    k8s = k8s_settings(cluster)
    c = cluster["_controller"]
    ns = k8s["namespace"]
    pod, run_label, yaml_text = _render_pod_yaml(cluster, ts, cfg_name)
    cm_name = _configmap_name(run_label)
    role = "client-0"
    mounts_line = ("configmap {} -> {} (ro); emptyDir -> {}".format(
        cm_name, k8s["pod_workspace"] + "/scripts", k8s["pod_workspace"]))
    if dry_run:
        log("[k8s] (dry-run) would create pod {} in ns {} (image {})"
            "".format(pod, ns, k8s["image"]))
        log("[k8s] (dry-run) mounts: {}".format(mounts_line))
        ip = "10.0.0.0"
    else:
        t0 = time.time()
        import tempfile
        local_dir = tempfile.mkdtemp(prefix="mc-pod-")
        local_yaml = os.path.join(local_dir, pod + ".yaml")
        try:
            with open(local_yaml, "w", encoding="utf-8", newline="\n") as f:
                f.write(yaml_text)
            yaml_dir = posixpath.join(k8s["controller_workspace"], "pods")
            controller_put(c, [local_yaml], yaml_dir)
            remote_yaml = posixpath.join(yaml_dir, pod + ".yaml")
            log("[k8s] pod manifest: template={} -> {}:{}".format(
                os.path.relpath(_pod_template_path(cluster), ROOT),
                controller_desc(c), remote_yaml))
            log("[k8s] pod {} create: ns={} image={} labels={{run={}, role={}}}"
                "".format(pod, ns, k8s["image"], run_label, role))
            log("[k8s] pod mounts: {}".format(mounts_line))
            out, rc = controller_run(
                c, ["-n", ns, "apply", "-f", remote_yaml], check=False)
            if rc != 0:
                raise RuntimeError("kubectl apply failed for {}: {}"
                                   "".format(pod, out.strip()[:400]))
            log("[k8s] describe: {} describe pod {}".format(" ".join(
                kubectl_prefix({"_ctx": c.get("_ctx", ""), "_ns": ns})), pod))
        finally:
            shutil.rmtree(local_dir, ignore_errors=True)
        ip, k8s_node = wait_k8s_pod_ready(cluster, pod)
        log("[k8s] pod {} ready (ns={}, ip={}, node={}, elapsed={:.1f}s)"
            "".format(pod, ns, ip, k8s_node or "?", time.time() - t0))
        _view = " ".join(kubectl_prefix({"_ctx": c.get("_ctx", ""),
                                          "_ns": ns}))
        log("[k8s] shell:    {} exec -it {} -- bash".format(_view, pod))
    node = {"kind": "k8s-pod", "_pod": pod, "_ns": ns,
            "_ctx": c.get("_ctx", ""), "_controller": c,
            "_controller_workspace": k8s["controller_workspace"],
            "_run_label": run_label, "_role": "client-0",
            "ip": ip, "fabric_ip": ip}
    cluster.setdefault("nodes", {})[pod] = node
    if dry_run:
        log("[k8s] (dry-run) pod {} would be ready "
            "(ns={}, ip=placeholder)".format(pod, ns))
    else:
        log("[k8s] pod {} ready (ns={}, ip={})".format(pod, ns, ip))
    return pod


def base_scenario_params(cfg, work_mode, topology, primary_node_name):
    # data_size is a sweep list; the env var carries the first entry as a
    # fallback only — the actual per-run size reaches the client via the
    # generate-data stage's --data-size and the value.bin file length.
    return {
        "BENCH_SCENARIO": cfg["scenario"],
        "BENCH_WORK_MODE": work_mode,
        "BENCH_OPERATION": cfg.get("operation", ""),
        "SETUP_PROTOCOL": cfg["protocol"],
        "BENCH_KEY_PREFIX": cfg.get("key_prefix", "mc_bench"),
        "BENCH_KEY_COUNT": str(cfg.get("key_count", 1000)),
        "BENCH_DATA_SIZE": data_size_list(cfg)[0],
        "BENCH_BATCH_SIZE": str(cfg.get("batch_size", 1)) if (
            cfg.get("operation") in ("put_batch", "get_batch")
            or any(s in ("put_batch", "get_batch") for s in str(cfg.get("operation", "")).split("/"))) else "",
        "BENCH_PUT_RATIO": str(cfg.get("put_ratio", "")) if "/" in str(cfg.get("operation", "")) else "",
        "BENCH_DURATION": str(cfg.get("duration", 0)),
        "BENCH_WARMUP_DURATION": str(cfg.get("warmup", {}).get("duration", 0)),
        # per-client core count for NUMA-aware sharding (0/unset = whole
        # node); the basic runner assigns each client its NUMA + slice index
        "ORCH_CPU_SHARD": str(cfg.get("cpu_shard", 0) or 0),
    }


ENV_GROUPS = [
    ("MC_", "mooncake runtime: read IN-PROCESS by the C++ SDK (MC_ prefix); "
             "MC_YLT_LOG_PATH is injected per-client by the runner (inline wins)"),
    ("SETUP_", "setup params: read by mooncake_benchmark_client.py and passed "
               "to store.setup() explicitly (SETUP_ prefix)"),
    ("BENCH_", "benchmark logic: repo-only, NOT read by the Mooncake process "
               "(BENCH_ prefix)"),
    ("ORCH_", "orchestrator / runner transport (ORCH_ prefix)"),
]


def _ub_probe_lines():
    """Shell snippet: resolve SETUP_UB_DEVICE from urma_admin (auto mode)."""
    return [
        "# ===== ub device auto-discovery (urma_admin) =====",
        "if command -v urma_admin >/dev/null 2>&1; then",
        "    SETUP_UB_DEVICE=$(urma_admin show "
        "| awk '$1 ~ /^[0-9]+$/ && $2 ~ /^bonding_dev_/ {print $2; exit}')",
        "    if [ -z \"$SETUP_UB_DEVICE\" ]; then",
        "        echo \"[ERROR] No bonding_dev_* URMA device found\" >&2",
        "        exit 1",
        "    fi",
        "    export SETUP_UB_DEVICE",
        "    echo \"[INFO] SETUP_UB_DEVICE=$SETUP_UB_DEVICE\"",
        "else",
        "    echo \"[ERROR] urma_admin command not found\" >&2",
        "    exit 1",
        "fi",
        "",
    ]


def build_env_content(scenario_params, cluster_session, fabric_ip,
                      exclude_cpus=""):
    """Generate the per-node env.sh with variables grouped by consumer (see
    ENV_GROUPS). Hand-tuning lives in env.mooncake.sh, sourced by the runner
    AFTER this file — same-name vars there win. LD_LIBRARY_PATH is no longer
    generated: inject it manually in env.mooncake.sh if a deployment needs
    it."""
    session = dict(cluster_session)
    session["SETUP_FABRIC_IP"] = fabric_ip
    # TCP transport bind address: every client process on this node must bind
    # the node's fabric IP, or store-side connections wedge during setup
    # (observed on the real cluster: readers hung unresponsive to /health and
    # /shutdown). Same value for all processes on the node -> per-node env.sh.
    session["MC_TCP_BIND_ADDRESS"] = fabric_ip
    # reserved cores (system / other users), subtracted from every binding
    # mode on this node (see client apply_cpu_binding)
    if exclude_cpus:
        session["BENCH_EXCLUDE_CPUS"] = exclude_cpus
    merged = dict(scenario_params)
    merged.update(session)
    lines = ["#!/usr/bin/env bash", "",
             "# generated by benchmark.py — do not edit; hand-tuning belongs in",
             "# env.mooncake.sh (sourced after this file; same-name vars win).",
             ""]
    consumed = set()
    for prefix, title in ENV_GROUPS:
        keys = sorted(k for k in merged if k.startswith(prefix) and merged[k])
        if not keys:
            continue
        consumed.update(keys)
        lines.append("# ===== {} =====".format(title))
        for k in keys:
            lines.append('export {}="{}"'.format(k, merged[k]))
        lines.append("")
    rest = sorted(k for k in merged if k not in consumed and merged[k])
    if rest:
        lines.append("# ===== unclassified =====")
        for k in rest:
            lines.append('export {}="{}"'.format(k, merged[k]))
        lines.append("")
    if str(merged.get("SETUP_PROTOCOL", "")).lower() == "ub" \
            and not merged.get("SETUP_UB_DEVICE"):
        lines.extend(_ub_probe_lines())
    return "\n".join(lines) + "\n"


def build_k8s_env_content(scenario_params, cluster_session):
    """k8s flavor env.sh: shared across all pods (identical content) —
    per-pod values are late-bound in-container:
      - SETUP_FABRIC_IP / MC_TCP_BIND_ADDRESS read $POD_IP (pod spec env via
        downward API)
      - SETUP_UB_DEVICE auto-detected via urma_admin inside the pod (every
        pod has a different bonding_dev_x name; the probe takes the first
        bonding_dev_* entry). Probe failure aborts the runner immediately.
    Core binding is intentionally omitted (containers do not numactl-bind)."""
    session = dict(cluster_session)
    protocol = (dict(scenario_params).get("SETUP_PROTOCOL", "")
                or session.get("SETUP_PROTOCOL", ""))
    merged = dict(scenario_params)
    merged.update(session)
    # late binding + zeroing for the k8s flavor
    merged["SETUP_FABRIC_IP"] = "$POD_IP"
    merged["MC_TCP_BIND_ADDRESS"] = "$POD_IP"
    merged.pop("BENCH_EXCLUDE_CPUS", None)
    merged.pop("ORCH_CPU_SHARD", None)
    lines = ["#!/usr/bin/env bash", "",
             "# generated by benchmark.py (k8s flavor, shared across pods) — per-pod",
             "# values (fabric IP / urma device) resolve in-container via $POD_IP",
             "# (downward API) and the urma_admin probe. Hand-tuning lives in",
             "# env.mooncake.sh (sourced after this file; same-name vars win).",
             ""]
    consumed = set()
    for prefix, title in ENV_GROUPS:
        keys = sorted(k for k in merged if k.startswith(prefix) and merged[k])
        if not keys:
            continue
        consumed.update(keys)
        lines.append("# ===== {} =====".format(title))
        for k in keys:
            lines.append('export {}="{}"'.format(k, merged[k]))
        lines.append("")
    rest = sorted(k for k in merged if k not in consumed and merged[k])
    if rest:
        lines.append("# ===== unclassified =====")
        for k in rest:
            lines.append('export {}="{}"'.format(k, merged[k]))
        lines.append("")
    if protocol == "ub" and not merged.get("SETUP_UB_DEVICE"):
        lines.extend(_ub_probe_lines())
    return "\n".join(lines) + "\n"


def _configmap_name(run_label):
    return "mc-bench-" + re.sub(r"[^a-z0-9-]", "-", run_label.lower())


def ensure_configmap(cluster, scenario_params, cluster_session,
                    ts, cfg_name, dry_run):
    """k8s: stage scripts on the controller and create one ConfigMap per
    config run. Pods mount it read-only at pod_workspace/scripts."""
    if cluster.get("_controller") is None:
        return None
    k8s = k8s_settings(cluster)
    c = cluster["_controller"]
    run_label = k8s_run_label(ts, cfg_name)
    cm_name = _configmap_name(run_label)
    if dry_run:
        log("[k8s] (dry-run) would create configmap {} (run={})"
            "".format(cm_name, run_label))
        return cm_name
    env_content = build_k8s_env_content(scenario_params, cluster_session)
    stage_remote = posixpath.join(k8s["controller_workspace"], "scripts", run_label)
    import tempfile
    local_stage = tempfile.mkdtemp(prefix="mc-cm-")
    try:
        files = [("run_benchmark_basic.sh", SCRIPTS_DIR),
                 ("run_benchmark_pipeline.sh", SCRIPTS_DIR),
                 ("mooncake_benchmark_client.py", SCRIPTS_DIR),
                 ("bench_etcd.py", SCRIPTS_DIR),
                 ("env.sh", None)]
        mooncake_env = os.path.join(SCRIPTS_DIR, "env.mooncake.sh")
        if os.path.exists(mooncake_env):
            files.append(("env.mooncake.sh", SCRIPTS_DIR))
        for fname, src_dir in files:
            if src_dir:
                shutil.copy(os.path.join(src_dir, fname),
                            os.path.join(local_stage, fname))
        with open(os.path.join(local_stage, "env.sh"), "w",
                  encoding="utf-8", newline="\n") as f:
            f.write(env_content)
        staged = sorted(os.listdir(local_stage))
        log("[k8s] staging {} files (run={}) -> {}:{}".format(
            len(staged), run_label, controller_desc(c), stage_remote))
        for fname in staged:
            log("[k8s]   + {}".format(fname))
        controller_put(c, [os.path.join(local_stage, f)
                           for f in staged], stage_remote)
    finally:
        shutil.rmtree(local_stage, ignore_errors=True)
    ns = k8s["namespace"]
    controller_run(c, ["-n", ns, "delete", "configmap", cm_name,
                       "--ignore-not-found=true"], check=False)
    out, rc = controller_run(
        c, ["-n", ns, "create", "configmap", cm_name,
            "--from-file=" + stage_remote], check=False)
    if rc != 0:
        raise RuntimeError("kubectl create configmap {} failed: {}"
                           "".format(cm_name, out.strip()[:400]))
    log("[k8s] configmap {} created (ns={}, keys={})".format(
        cm_name, ns, ",".join(staged)))
    return cm_name

def deploy_scripts(cluster, involved_nodes, scenario_params, cluster_session,
                   remote_scripts, remote_log_dir, dry_run):
    """Generate a per-node env.sh (node-specific fabric_ip) and push the
    orchestrator scripts (both runner variants + client + the hand-editable
    env.mooncake.sh tuning panel) to every involved node."""
    env_sh_path = os.path.join(SCRIPTS_DIR, "env.sh")
    # env.mooncake.sh is a static, git-tracked tuning file (sourced by both
    # runner scripts after env.sh). If it is missing the run still works —
    # the runners guard the source with a -f check.
    mooncake_env_path = os.path.join(SCRIPTS_DIR, "env.mooncake.sh")
    for node_name in involved_nodes:
        n = get_node(cluster, node_name)
        if is_k8s_node(n):
            # k8s: scripts + env.sh are already delivered via ConfigMap
            # (ensure_configmap); the pods mount them read-only at
            # remote_scripts. Only ensure the log dir exists (emptyDir).
            log("[k8s] pod {}: scripts via configmap (ro at {}), "
                "log dir {}".format(n.get("_pod", "?"), remote_scripts,
                                    remote_log_dir))
            run_ssh(n["ip"], n, "mkdir -p {}".format(remote_log_dir),
                    dry_run=dry_run)
            continue
        fab = n.get("fabric_ip", n.get("ip", ""))
        # reserved cores (system/other users): node-level override wins over
        # the cluster-wide exclude_cpus; injected per-node so every client
        # process on this node subtracts it from its binding
        exclude = n.get("exclude_cpus", "") or cluster.get("exclude_cpus", "") or ""
        with open(env_sh_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(build_env_content(scenario_params, cluster_session, fab,
                                      exclude_cpus=str(exclude)))
        log("env.sh written (node={}, fabric_ip={}, exclude_cpus={})".format(
            node_name, fab, exclude or "-"))
        run_ssh(n["ip"], n, "mkdir -p {} {}".format(
            remote_scripts, remote_log_dir), dry_run=dry_run)
        push_files = [env_sh_path,
                      os.path.join(SCRIPTS_DIR, "run_benchmark_basic.sh"),
                      os.path.join(SCRIPTS_DIR, "run_benchmark_pipeline.sh"),
                      os.path.join(SCRIPTS_DIR, "mooncake_benchmark_client.py"),
                      os.path.join(SCRIPTS_DIR, "bench_etcd.py")]
        if os.path.exists(mooncake_env_path):
            push_files.append(mooncake_env_path)
        scp_push(n["ip"], n, push_files, remote_scripts, dry_run=dry_run)
        run_ssh(n["ip"], n, "chmod +x {} {} {}".format(
            remote_scripts + "/env.sh",
            remote_scripts + "/run_benchmark_basic.sh",
            remote_scripts + "/run_benchmark_pipeline.sh"), dry_run=dry_run)


def value_check_cmd(remote_data_dir, scenario, data_size_bytes):
    return (
        'python3 -c "import os,sys; p=os.path.expanduser(sys.argv[1]); '
        "print('EXISTS' if os.path.isfile(p) and os.path.getsize(p)==int(sys.argv[2]) "
        "else 'MISSING')\" "
        "{}/{}/value.bin {}".format(remote_data_dir, scenario, data_size_bytes)
    )


# --------------------------------------------------------------------------
# NUMA binding (numactl cpu+mem): the orchestrator computes every client
# process's final (numa_node, cpu_list) and the launch sites only prepend
# `numactl --physcpubind=<list> [--membind|--preferred N]`. The client also
# receives --cpu-bind <list> so it re-confirms the affinity and logs the
# `cpu binding:` line (echo_cpu_bindings depends on it).
# --------------------------------------------------------------------------

def resolve_numa_policy(cfg, globals_cfg):
    """numa_membind / numa_strict resolution (case explicit > global.json >
    code default). numa_membind: membind (strict node memory, default) |
    preferred (soft fallback) | off (cpu affinity only). numa_strict:
    false (default) -> a node missing numactl degrades to cpu-only binding
    with a WARN; true -> hard error. Effective only when numa_bind is on."""
    def pick(key, default):
        v = cfg.get(key)
        if v is None:
            v = globals_cfg.get(key)
        return default if v is None else str(v).lower()
    membind = pick("numa_membind", "membind")
    if membind not in ("membind", "preferred", "off"):
        raise RuntimeError(
            "numa_membind must be membind|preferred|off (got {!r})".format(membind))
    strict = pick("numa_strict", "false")
    if strict not in ("true", "false"):
        raise RuntimeError(
            "numa_strict must be true|false (got {!r})".format(strict))
    return membind, strict == "true"


def node_exclude(cluster, node):
    """Reserved-core exclusion for a node: node-level override wins over the
    cluster-wide exclude_cpus (same resolution as deploy_scripts/env.sh)."""
    return node.get("exclude_cpus", "") or cluster.get("exclude_cpus", "") or ""


def _parse_cpu_list(spec):
    """'0-3,7' -> set of ints (orchestrator-side port of the client parser)."""
    cpus = set()
    for part in str(spec).split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            lo, hi = int(lo), int(hi)
            if lo < 0 or hi < lo:
                raise ValueError("invalid cpu range: {}".format(part))
            cpus.update(range(lo, hi + 1))
        else:
            cpus.add(int(part))
    return cpus


def _fmt_cpu_list(cpus):
    """set of ints -> compact '0-3,7' (accepted by the client --cpu-bind)."""
    out = []
    first = prev = None
    for c in sorted(cpus):
        if first is None:
            first = prev = c
            continue
        if c == prev + 1:
            prev = c
            continue
        out.append(str(first) if first == prev else "{}-{}".format(first, prev))
        first = prev = c
    if first is not None:
        out.append(str(first) if first == prev else "{}-{}".format(first, prev))
    return ",".join(out)


def probe_numactl(ip, node, dry_run=False):
    return ssh_check_output(
        ip, node, "command -v numactl >/dev/null 2>&1 && echo OK || echo MISSING",
        quiet=True, dry_run=dry_run) == "OK"


def probe_numa_topology(ip, node, dry_run=False):
    """One quiet ssh -> (node_count, {node_idx: 'cpu list str'}). Raises on
    empty/garbage output (replaces the old count-only probe)."""
    out = ssh_check_output(
        ip, node,
        "for d in /sys/devices/system/node/node[0-9]*; do "
        "echo \"$(basename \"$d\") $(cat \"$d/cpulist\")\"; done",
        quiet=True, dry_run=dry_run)
    node_cpus = {}
    for line in out.splitlines():
        parts = line.split(None, 1)
        if len(parts) != 2 or not parts[0].startswith("node"):
            continue
        try:
            idx = int(parts[0][4:])
            _parse_cpu_list(parts[1])
        except ValueError:
            continue
        node_cpus[idx] = parts[1].strip()
    if not node_cpus:
        raise RuntimeError(
            "numa_bind: failed to probe NUMA topology on {} (got {!r})".format(ip, out))
    return len(node_cpus), node_cpus


def numa_binding_spec(pool_str, exclude_str, shard, shard_index):
    """One client's final cpu list on a NUMA node: pool minus exclude, then
    the shard_index-th consecutive shard-core slice with wraparound when the
    pool cannot hold every client (shard=0 -> whole pool). Client-side
    logic, moved here so numactl can bind before exec."""
    pool = _parse_cpu_list(pool_str)
    if exclude_str:
        pool -= _parse_cpu_list(exclude_str)
    if not pool:
        raise RuntimeError("cpu binding pool is empty on this NUMA node "
                           "(exclude_cpus={!r} removed everything)".format(exclude_str))
    if shard > 0:
        if shard > len(pool):
            raise RuntimeError("cpu_shard {} > available cores {} on this NUMA node".format(
                shard, len(pool)))
        pool = sorted(pool)
        start = (shard_index * shard) % len(pool)
        end = start + shard
        sel = pool[start:end] if end <= len(pool) else pool[start:] + pool[:end - len(pool)]
        return _fmt_cpu_list(set(sel))
    return _fmt_cpu_list(pool)


def client_bind_spec(membind_mode, numa_node, cpu_list, numactl):
    """Encode one client's binding for the basic runner --client-binds flag:
    m:<node>:<list> membind | p:<node>:<list> preferred | c:<list> cpu-only
    (all three launch via numactl); bare <list> = degraded cpu-only binding
    by the client itself (numactl missing on that node)."""
    if not numactl:
        return cpu_list
    if membind_mode == "membind":
        return "m:{}:{}".format(numa_node, cpu_list)
    if membind_mode == "preferred":
        return "p:{}:{}".format(numa_node, cpu_list)
    return "c:{}".format(cpu_list)


def numactl_launch_prefix(membind_mode, numa_node, cpu_list):
    """numactl prefix for direct launches (pipeline reader/writer)."""
    prefix = "numactl --physcpubind={}".format(cpu_list)
    if membind_mode == "membind":
        return prefix + " --membind={}".format(numa_node)
    if membind_mode == "preferred":
        return prefix + " --preferred={}".format(numa_node)
    return prefix


def numa_node_check(node_name, numa_strict):
    """Per-node numactl gate: degrade with WARN or hard-error per policy."""
    if not numa_strict:
        log("[WARN] numactl missing on {} -> cpu-only binding, no membind "
            "(set numa_strict=true to make this fatal)".format(node_name))
        return False
    raise RuntimeError(
        "numa_bind requires numactl on node {} (numa_strict=true)".format(node_name))


def pull_logs(cluster, involved_nodes, remote_log_dir, cfg_name, local_log_dir, dry_run):
    pulled_ips = set()
    for node_name in involved_nodes:
        n = get_node(cluster, node_name)
        if n["ip"] in pulled_ips:
            continue
        pulled_ips.add(n["ip"])
        scp_pull(n["ip"], n, "{}/{}.*".format(remote_log_dir, cfg_name),
                 local_log_dir, dry_run=dry_run)


def pull_stage_logs(ip, node, remote_files, local_dir, dry_run):
    """Incremental pull of just-finished stage log(s) so previous stages are
    locally readable while later (long) stages run; never raises."""
    if dry_run or not remote_files:
        return
    for f in remote_files:
        try:
            scp_pull(ip, node, f, local_dir, dry_run=False)
        except Exception as exc:
            log("[WARN] stage log pull {}: {}".format(f, exc))


def format_duration(seconds):
    """Seconds -> 'x分x秒' / 'x小时x分x秒' style progress string."""
    seconds = int(max(0, round(seconds)))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return "{}小时{}分{}秒".format(h, m, s)
    return "{}分{}秒".format(m, s)


def _fmt_dur_en(seconds):
    """Seconds -> 'x min x s' / 'x h x min x s' style duration string."""
    seconds = int(max(0, round(seconds)))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return "{} h {} min {} s".format(h, m, s)
    return "{} min {} s".format(m, s)


def _wait_suffix(t0, expected_s):
    """One-shot wait-line suffix: '(Expected: <dur>, ETA <HH:MM:SS>)' when the
    expected duration is known, empty otherwise (count-based runs have no
    predictable end)."""
    if not expected_s:
        return ""
    from datetime import datetime as _dt
    end = _dt.fromtimestamp(t0 + expected_s)
    return " (Expected: {}, ETA {})".format(
        _fmt_dur_en(expected_s), end.strftime("%H:%M:%S"))


def wait_process_exit(cluster, node_name, pid, what, timeout_s, dry_run=False,
                      expected_s=None):
    """Poll `kill -0 PID` until the remote process exits or times out.

    Logs ONE 'running...' line at start (with the expected duration and ETA
    wall time when expected_s is given) and one result line at the end
    (exited / timeout); per-poll ssh commands are not logged."""
    n = get_node(cluster, node_name)
    t0 = time.time()
    log("{} (pid {}) running...{}".format(what, pid, _wait_suffix(t0, expected_s)))
    for _ in range(int(timeout_s)):
        alive = ssh_check_output(
            n["ip"], n,
            "kill -0 {} 2>/dev/null && echo ALIVE || echo DEAD".format(pid),
            dry_run=dry_run, quiet=True)
        if alive != "ALIVE":
            log("{} exited after {} ({:.0f}s)".format(
                what, format_duration(time.time() - t0), time.time() - t0))
            return True
        time.sleep(1)
    log("[WARN] {} (pid {}) still running after {} ({}s timeout)".format(
        what, pid, format_duration(timeout_s), timeout_s))
    return False


def _kill_remote_process(cluster, node_name, pid):
    """Best-effort kill of a remote test process (graceful then -9)."""
    n = get_node(cluster, node_name)
    try:
        ssh_check_output(n["ip"], n,
                         "kill {} 2>/dev/null; sleep 1; kill -9 {} 2>/dev/null || true".format(pid, pid),
                         quiet=True)
    except Exception as exc:
        log("[WARN] fail-cleanup: kill pid {} on {} failed: {}".format(pid, node_name, exc))


def _pipeline_etcd_endpoints(globals_cfg):
    """Pipeline rendezvous etcd endpoints (global.json etcd_endpoints).
    Required for pipeline regardless of master_type: readers self-register
    there and writers discover them from there."""
    eps = (globals_cfg.get("etcd_endpoints", "") or "").strip()
    if not eps:
        raise RuntimeError(
            "pipeline scenario requires global.json 'etcd_endpoints' "
            "(rendezvous: reader registration / writer discovery / start "
            "trigger); master_type may stay 'http' — the same etcd serves "
            "coordination")
    return eps


def _etcd_http_urls(cluster, endpoints):
    """Gateway base URLs for ORCHESTRATOR-side access. k8s mode resolves
    *.svc.cluster.local Service DNS via the controller first (the
    orchestrator host sits outside the cluster and cannot route it); the
    resolution is cached on the cluster dict (barriers poll every second)."""
    if cluster is not None and cluster.get("_controller") is not None:
        cache = cluster.setdefault("_etcd_resolved", {})
        if endpoints not in cache:
            try:
                cache[endpoints] = _resolve_k8s_addr(endpoints, cluster)
            except Exception as exc:
                log("[WARN] etcd endpoint resolution via controller: {}".format(exc))
                cache[endpoints] = endpoints
        endpoints = cache[endpoints] or endpoints
    return bench_etcd.endpoint_urls(endpoints)


def _controller_etcd_post(cluster, endpoints, path, payload, timeout):
    """POST to the etcd gateway FROM THE CONTROLLER (k8s mode: the
    orchestrator cannot reach cluster-internal etcd directly). Returns the
    parsed JSON response."""
    c = cluster["_controller"]
    body = json.dumps(payload)
    last = None
    for url in _etcd_http_urls(cluster, endpoints):
        cmd = ("curl -s --max-time {t} -X POST "
               "-H 'Content-Type: application/json' -d {d} {u}").format(
                   t=int(max(1, timeout)), d=shlex.quote(body),
                   u=shlex.quote(url + path))
        out, rc = controller_exec(c, cmd)
        if rc == 0 and out.strip():
            try:
                return json.loads(out)
            except ValueError:
                last = "bad json: {}".format(out.strip()[:120])
        else:
            last = (out or "").strip()[:120]
    raise RuntimeError("etcd gateway {} failed via controller: {}".format(
        path, last))


def _etcd_range(cluster, endpoints, prefix, timeout=5):
    """Orchestrator-side prefix scan: k8s -> via controller; ssh -> direct."""
    if cluster is not None and cluster.get("_controller") is not None:
        payload = {"key": bench_etcd._b64(prefix),
                   "range_end": bench_etcd._b64(bench_etcd._prefix_end(prefix))}
        out = _controller_etcd_post(cluster, endpoints, "/v3/kv/range",
                                    payload, timeout)
        kvs = out.get("kvs") or []
        return {bench_etcd._unb64(kv["key"]): bench_etcd._unb64(kv["value"])
                for kv in kvs}
    return bench_etcd.kv_range(endpoints, prefix, timeout)


def _etcd_delete_prefix(cluster, endpoints, prefix, timeout=5):
    """Orchestrator-side prefix delete: k8s -> via controller; ssh -> direct."""
    if cluster is not None and cluster.get("_controller") is not None:
        payload = {"key": bench_etcd._b64(prefix),
                   "range_end": bench_etcd._b64(bench_etcd._prefix_end(prefix))}
        out = _controller_etcd_post(cluster, endpoints, "/v3/kv/deleterange",
                                    payload, timeout)
        return int(out.get("deleted", 0) or 0)
    return bench_etcd.kv_delete_prefix(endpoints, prefix, timeout)


def _pipeline_wipe_etcd(cluster, globals_cfg):
    """Best-effort wipe of the whole mcbench/ rendezvous tree (single active
    benchmark per cluster assumption). No-op when etcd is unconfigured."""
    eps = (globals_cfg.get("etcd_endpoints", "") or "").strip()
    if not eps:
        return
    n = _etcd_delete_prefix(cluster, eps, "mcbench/")
    if n:
        log("[k8s-pipeline] etcd: wiped {} stale mcbench/ keys".format(n))


def _pipeline_http_call(node_entry, port, path, timeout=5, dry=False):
    """POST to a pipeline process's HTTP endpoint (writer /start, reader
    /shutdown) by curling LOOPBACK ON THE NODE ITSELF. The node transport
    dispatches automatically: ssh nodes run curl over the ssh connection,
    k8s pods run it via kubectl exec inside the pod. The orchestrator host
    must NEVER be assumed to reach pod IPs / fabric IPs directly — it sits
    outside those networks on real clusters (a direct http_probe to the
    pod IP times out there). Returns True on HTTP 200."""
    url = "http://127.0.0.1:{}{}".format(port, path)
    cmd = ("curl -s --noproxy '*' --connect-timeout 2 --max-time {} "
           "-X POST -o /dev/null -w '%{{http_code}}' {}").format(timeout, url)
    out = ssh_check_output(node_entry["ip"], node_entry, cmd, dry_run=dry)
    return str(out).strip() == "200"


def _pipeline_barrier(cluster, etcd_endpoints, prefix, expected, timeout_s,
                      what, run_id):
    """Poll etcd until `expected` registrations exist under prefix; raises
    with the registered/missing list on timeout. Returns the kv dict."""
    t0 = time.time()
    while True:
        kvs = _etcd_range(cluster, etcd_endpoints, prefix)
        if len(kvs) >= expected:
            return kvs
        if time.time() - t0 > timeout_s:
            raise RuntimeError(
                "pipeline rendezvous: only {}/{} {} registered in etcd "
                "after {}s (run_id={}, registered keys={})".format(
                    len(kvs), expected, what, int(timeout_s), run_id,
                    sorted(kvs)))
        _check_stop()
        time.sleep(1.0)


def _pipeline_entry_label(entry):
    """Human label for a plan entry (dual clients / readers / writers)."""
    if "id" in entry:
        return "dual c{}".format(entry["id"])
    if "index" in entry:
        return "reader r{}".format(entry["index"])
    if "gid" in entry:
        return "writer w{}".format(entry["gid"])
    return "client"


def fail_cleanup_pipeline(cluster, plan, reader_pids, writer_pids,
                          involved_nodes, remote_log_dir, cfg_name, local_log_dir,
                          etcd_endpoints=None, run_id=None):
    """Best-effort cleanup after a pipeline scenario failure, so remote logs
    (including the reader SUMMARY/RESULT that is only flushed at exit) are
    preserved for diagnosis and ports are freed. Every step is guarded so the
    original exception is never masked. Order: graceful reader shutdown ->
    wait for exit -> kill stragglers -> pull all logs -> wipe the etcd
    rendezvous prefix."""
    log("[FAIL] scenario {} failed; pulling remote logs for diagnosis ...".format(cfg_name))
    # 1. graceful reader shutdown so each reader flushes SUMMARY/RESULT
    for r in plan["readers"]:
        try:
            rn = get_node(cluster, r["node"])
            _pipeline_http_call(rn, r["port"], "/shutdown")
        except Exception as exc:
            log("[WARN] fail-cleanup: {} shutdown failed: {}".format(
                _pipeline_entry_label(r), exc))
    # 2. short wait for readers to exit (their final logs are written on exit)
    for r, pid in reader_pids:
        if pid:
            try:
                wait_process_exit(cluster, r["node"], pid,
                                  _pipeline_entry_label(r), 30)
            except Exception:
                pass
    # 3. kill any still-alive writers/readers
    for w, pid in writer_pids:
        if pid:
            _kill_remote_process(cluster, w["node"], pid)
    for r, pid in reader_pids:
        if pid:
            _kill_remote_process(cluster, r["node"], pid)
    # 4. pull every log of this scenario (stage logs + reader/writer + nohup)
    try:
        pull_logs(cluster, involved_nodes, remote_log_dir, cfg_name, local_log_dir, False)
    except Exception as exc:
        log("[WARN] fail-cleanup: log pull failed: {}".format(exc))
    # 5. wipe this run's etcd rendezvous prefix
    if etcd_endpoints and run_id:
        try:
            _etcd_delete_prefix(cluster, etcd_endpoints,
                                "mcbench/{}/".format(run_id))
        except Exception as exc:
            log("[WARN] fail-cleanup: etcd prefix wipe failed: {}".format(exc))


def fail_cleanup_basic(cluster, involved_nodes, remote_log_dir, cfg_name, local_log_dir):
    """Best-effort log pull after a basic scenario failure (foreground
    processes exit by themselves; only the evidence needs to be preserved)."""
    log("[FAIL] scenario {} failed; pulling remote logs for diagnosis ...".format(cfg_name))
    try:
        pull_logs(cluster, involved_nodes, remote_log_dir, cfg_name, local_log_dir, False)
    except Exception as exc:
        log("[WARN] fail-cleanup: log pull failed: {}".format(exc))


def _record_pids(host, node, pids_file, label, cfg_name, node_name,
                 marker="mooncake_benchmark_client"):
    """Read the runner's pids file once; returns True when pids were found
    and ledgered into .state (the caller latches this per invocation)."""
    try:
        raw = ssh_check_output(
            host, node,
            "cat {f} 2>/dev/null || true".format(f=pids_file), quiet=True)
    except Exception:
        raw = ""
    pids = [x for x in raw.split() if x.strip().isdigit()]
    if pids:
        state_append([{
            "ip": host, "node": node_name or "", "port": None,
            "fabric_host": None, "pid": p,
            "label": "{} client".format(label), "cfg": cfg_name,
            "marker": marker,
            "ts": time.strftime("%Y-%m-%d %H:%M:%S")} for p in pids])
        return True
    return False


def run_ssh_with_progress(host, node, command, label, expected_s=None, dry_run=False,
                          pids_file=None, cfg_name=None, node_name=None,
                          marker="mooncake_benchmark_client"):
    """run_ssh variant for long-blocking commands (e.g. the basic benchmark
    stage): logs ONE 'running...' line (expected duration + ETA when
    expected_s is given). rc!=0 raises like run_ssh. ssh nodes run over the
    cached paramiko connection (output relayed live to the console, exit
    polled each second); k8s nodes keep the kubectl Popen child.

    pids_file: when given, the first poll reads that remote file (client
    pids written by the runner script) and records every pid into the
    .state ledger under cfg_name — so a crashed orchestrator still leaves a
    killable trail."""
    if is_k8s_node(node):
        log("[k8s] $ {}".format(kubectl_exec_line(node, command)))
    else:
        log("[ssh] {} : {}".format(host, command))
    if dry_run:
        log("      (dry-run, skipped)")
        return
    if stop_requested():
        raise StopRequested()
    if is_k8s_node(node):
        holder = {"rc": 1, "stop": False}

        def _run():
            try:
                holder["rc"] = controller_stream(
                    node["_controller"],
                    ["-n", node["_ns"], "exec", node["_pod"],
                     "--", "bash", "-c", command])
            except StopRequested:
                holder["stop"] = True
            except Exception as exc:
                log("[exec] {} failed: {}".format(node["_pod"], exc))
                holder["rc"] = 1

        th = threading.Thread(target=_run, daemon=True)
        th.start()
        t0 = time.time()
        log("{} running...{}".format(label, _wait_suffix(t0, expected_s)))
        recorded = False
        while th.is_alive():
            if stop_requested():
                holder["stop"] = True
            if not recorded and pids_file:
                if _record_pids(host, node, pids_file, label, cfg_name,
                                node_name, marker):
                    recorded = True
            time.sleep(1)
        th.join()
        if holder.get("stop") or stop_requested():
            raise StopRequested()
        rc = holder["rc"]
    else:
        client = _ssh_client(node)
        stdin, stdout, stderr = client.exec_command(command)
        chan = stdout.channel
        t0 = time.time()
        log("{} running...{}".format(label, _wait_suffix(t0, expected_s)))
        recorded = False
        while not chan.exit_status_ready():
            if stop_requested():
                try:
                    chan.close()
                except Exception:
                    pass
                _ssh_drop(node)
                raise StopRequested()
            _drain_chan(chan, True, [])
            if not recorded and pids_file:
                if _record_pids(host, node, pids_file, label, cfg_name,
                                node_name, marker):
                    recorded = True
            time.sleep(1)
        grace = time.time() + 5
        while (chan.recv_ready() or chan.recv_stderr_ready()
                or not chan.eof_received) and time.time() < grace:
            _drain_chan(chan, True, [])
            time.sleep(0.05)
        _drain_chan(chan, True, [])
        rc = chan.recv_exit_status()
    log("{} done rc={} elapsed={:.1f}s".format(
        label, rc, time.time() - t0))
    if rc != 0:
        raise RuntimeError("ssh command failed (rc={}) on {}: {}".format(
            rc, host, command))


# ---------------------------------------------------------------------------
# process ledger (.state): every remote client process started by this
# orchestrator is recorded (ip/node/port/fabric_host/pid) so it can always
# be cleaned up, even after a crashed or manually killed orchestrator run.
# ---------------------------------------------------------------------------

STATE_FILE = os.path.join(ROOT, ".state")


def state_append(entries):
    if not entries:
        return
    with open(STATE_FILE, "a", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


def state_load():
    if not os.path.exists(STATE_FILE):
        return []
    out = []
    for line in open(STATE_FILE, encoding="utf-8", errors="replace"):
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
    return out


def _state_write(entries):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        for e in entries:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")


def state_remove(entries):
    """Drop the given entries (matched by ip+pid) from the ledger."""
    drop = {(e.get("ip"), str(e.get("pid"))) for e in entries}
    have = state_load()
    kept = [e for e in have if (e.get("ip"), str(e.get("pid"))) not in drop]
    if len(kept) != len(have):
        _state_write(kept)


def state_remove_cfg(cfg_name):
    """Drop every ledger row recorded for one scenario config."""
    have = state_load()
    kept = [e for e in have if e.get("cfg") != cfg_name]
    if len(kept) != len(have):
        _state_write(kept)


def reap_state_entries(cluster):
    """Kill leftover client processes recorded in .state and clean the ledger.

    Guards against pid reuse: a remote pid is only killed when its
    /proc/<pid>/cmdline still contains the benchmark client script name.
    Readers with a recorded port get a graceful HTTP /shutdown first.
    Every step is best-effort; unreachable nodes keep their records for a
    later reap."""
    entries = state_load()
    if cluster.get("_controller") is not None:
        # k8s: namespace deletion (reap_k8s_namespaces) covers pod processes;
        # keep only namespace ledger rows for later runs.
        keep = [e for e in entries if e.get("type") == "k8s-namespace"]
        if len(keep) != len(entries):
            log(".state: k8s mode, dropping {} stale process record(s)"
                "".format(len(entries) - len(keep)))
            _state_write(keep)
        return 0
    if not entries:
        log(".state: no leftover process records")
        return 0
    log(".state: {} leftover process record(s); reaping ...".format(len(entries)))
    removed = []
    for e in entries:
        ip, pid = e.get("ip"), str(e.get("pid"))
        node_name = e.get("node") or ""
        label = e.get("label") or "pid {}".format(pid)
        if not ip or not pid or pid.isdigit() is False:
            removed.append(e)
            continue
        try:
            node = get_node(cluster, node_name) if node_name else {"ip": ip}
        except Exception:
            node = {"ip": ip}
        try:
            alive = ssh_check_output(
                ip, node,
                "kill -0 {pid} 2>/dev/null && echo ALIVE || echo DEAD".format(pid=pid),
                quiet=True)
        except Exception as exc:
            log("[WARN] .state reap: {} on {} unreachable ({}); kept".format(label, ip, exc))
            continue
        if alive != "ALIVE":
            removed.append(e)  # already gone -> just drop the stale row
            continue
        try:
            marker = ssh_check_output(
                ip, node,
                "tr '\\0' ' ' < /proc/{pid}/cmdline 2>/dev/null | grep -cF {m} || true".format(
                    pid=pid, m=shlex.quote(e.get("marker") or "mooncake_benchmark_client")),
                quiet=True)
        except Exception:
            marker = "0"
        if marker.strip() in ("", "0"):
            # pid reuse (or a foreign process): never kill, drop stale row
            log("[WARN] .state reap: pid {} on {} is not {} ; "
                "skipping kill, dropping stale record".format(
                    pid, ip, e.get("marker") or "mooncake_benchmark_client"))
            removed.append(e)
            continue
        port, fab = e.get("port"), e.get("fabric_host")
        if port and fab:
            try:
                ssh_check_output(
                    ip, node,
                    "curl -s --noproxy '*' --connect-timeout 2 --max-time 5 "
                    "-X POST http://{fab}:{port}/shutdown || true".format(fab=fab, port=port),
                    quiet=True)
                time.sleep(2)
            except Exception:
                pass
        try:
            # tree kill: the ledger pid may be the runner-script bash wrapper
            # whose child is the python client — killing only the parent
            # would orphan the child (pgrep -P lists the children)
            ssh_check_output(ip, node,
                             "kill $(pgrep -P {pid} 2>/dev/null) {pid} 2>/dev/null || true".format(pid=pid),
                             quiet=True)
            time.sleep(2)
            still = ssh_check_output(
                ip, node,
                "kill -0 {pid} 2>/dev/null && echo ALIVE || echo DEAD".format(pid=pid),
                quiet=True)
            if still == "ALIVE":
                ssh_check_output(ip, node,
                                 "kill -9 $(pgrep -P {pid} 2>/dev/null) {pid} 2>/dev/null || true".format(pid=pid),
                                 quiet=True)
            log(".state reap: killed {} (pid {} on {})".format(label, pid, ip))
        except Exception as exc:
            log("[WARN] .state reap: {} kill failed: {}; kept".format(label, exc))
            continue
        removed.append(e)
    state_remove(removed)
    log(".state reap done: {} handled, {} record(s) remaining".format(
        len(removed), len(state_load())))
    return len(removed)


def run_stage(host, node, runner, mode, remote_log_dir, log_prefix, extra=None,
              dry_run=False, pull_to=None, cfg_name=None, node_name=None):
    _check_stop()
    remote_log = remote_log_path(remote_log_dir, log_prefix, mode)
    command = "bash {} {} {}".format(runner, mode, remote_log)
    if extra:
        command += " " + extra
    run_ssh_with_progress(
        host, node, command, label=log_prefix, dry_run=dry_run,
        pids_file=remote_log + ".pids", cfg_name=cfg_name or log_prefix,
        node_name=node_name)
    if pull_to is not None:
        pull_stage_logs(host, node, [remote_log], pull_to, dry_run)
    if not dry_run:
        state_remove_cfg(cfg_name or log_prefix)


# --------------------------------------------------------------------------
# basic scenario (interface-level benchmark: put/get/batch/mixed sweeps)
# --------------------------------------------------------------------------

def echo_cpu_bindings(cfg_name, local_log_dir):
    """Echo each client process's actual cpu binding (from its log's
    'cpu binding:' line) into the orchestrator output, so every run records
    exactly which cores each started client used."""
    import glob as _glob
    for path in sorted(_glob.glob(os.path.join(local_log_dir, "**", "*.log"),
                                  recursive=True)):
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    if "cpu binding: " in line:
                        log("{}: [{}] {}".format(
                            cfg_name, os.path.basename(path),
                            line.strip()))
                        # merged basic logs carry one line per client — no break
        except OSError:
            continue


def run_basic_scenario(cluster, globals_cfg, cfg, ts, args):
    apply_cli_overrides(cfg, args)
    matrix = getattr(args, "_matrix", None)
    scenario = cfg["scenario"]
    # store space guard (no-op when store_total_size is unset)
    _basic_store_check(globals_cfg, cfg)
    cfg_name = os.path.splitext(os.path.basename(cfg.get("__cfg_path__", scenario)))[0]
    dry = args.dry_run
    op = cfg.get("operation", "")
    work_mode = cfg.get("work_mode", cfg.get("mode", ""))
    is_k8s = cluster.get("_controller") is not None
    # k8s: pod name is generated by ensure_k8s_pod (cluster nodes are no
    # longer workers); SSH keeps client_node from the scenario config.
    primary_node_name = None if is_k8s else cfg["client_node"]
    involved_nodes = [] if is_k8s else [primary_node_name]
    # scenario_params/cluster_session have no pod-IP dependency (topology is
    # a dead param in base_scenario_params) -> build them first so the k8s
    # ConfigMap (scripts + env.sh) can exist before pods mount it
    remote_dir, remote_scripts, remote_log_dir, remote_runner_basic, \
        remote_runner_pipeline, local_log_dir = \
        scenario_paths(globals_cfg, ts, cfg_name)
    remote_runner = remote_runner_basic
    if not dry:
        os.makedirs(local_log_dir, exist_ok=True)
    scenario_params = base_scenario_params(
        cfg, work_mode, "k8s_pod" if is_k8s else "ssh",
        "client" if is_k8s else primary_node_name)
    cluster_session = build_cluster_session(cluster, globals_cfg, remote_dir)
    ensure_configmap(cluster, scenario_params, cluster_session, ts, cfg_name, dry)
    if is_k8s:
        # k8s: pods are created AFTER the ConfigMap they mount
        primary_node_name = ensure_k8s_pod(cluster, ts, cfg_name, dry)
        involved_nodes = [primary_node_name]
    else:
        ensure_pods(cluster, involved_nodes, ts, cfg_name, dry)
    cfg["_primary_node"] = primary_node_name
    node = get_node(cluster, primary_node_name)
    host = node["ip"]
    if is_k8s:
        # master/store are external Services; client pods are always remote
        topology = "remote_node"
    else:
        store_node_ips = resolve_store_node_ips(
            cluster, globals_cfg.get("mooncake_store_nodes", []) or [])
        topology = "local_node" if node["ip"] in store_node_ips else "remote_node"
    log("=== scenario {} (op={}, topo={}, protocol={}) primary_node={} ===".format(
        cfg_name, op, topology, cfg["protocol"], host))
    deploy_scripts(cluster, involved_nodes, scenario_params, cluster_session,
                   remote_scripts, remote_log_dir, dry)
    # The try MUST start here (before any stage): a failure in generate-data /
    # remove-keys / prepare-store previously raised before the try block, so
    # the FAIL path never pulled the remote logs — silent failure with no
    # diagnosable evidence. All stages are now covered.
    try:
        profile_sess = None  # profiling session (set below when matrix is on)
        run_stage(host, node, remote_runner,
                  "remove-keys", remote_log_dir, cfg_name,
                  extra="--scope full", dry_run=dry, pull_to=local_log_dir,
                  node_name=primary_node_name)
        warmup = "false" if args.no_warmup else "true"
        runs = build_runs(cfg)
        need_remove_before_run = op in PIPELINE_PUT_IFACES or "/" in op
        # basic pacing: target_qps (total across the scenario) is optional;
        # when set it is split per client process per combo. 0/absent keeps
        # the current count-based max-speed behaviour.
        total_target_qps = float(cfg.get("target_qps", 0) or 0)
        if total_target_qps < 0:
            raise RuntimeError("basic target_qps must be >= 0 (got {})".format(total_target_qps))
        # numa_bind resolution: case/layer explicit value > global.json
        # ("numa_bind", default true) > code default true. Only an explicit
        # false disables binding.
        numa_bind = cfg.get("numa_bind")
        if numa_bind is None:
            numa_bind = globals_cfg.get("numa_bind")
        numa_bind = True if numa_bind is None else bool(numa_bind)
        # per-client core count (NUMA-aware sharding); sharding implies the
        # NUMA topology probes even when numa_bind was disabled explicitly
        cpu_shard = int(cfg.get("cpu_shard", 0) or 0)
        if cpu_shard > 0 and not numa_bind:
            numa_bind = True
        # containers do not do numactl: k8s primary node forces binding off
        if is_k8s_node(node) and (numa_bind or cpu_shard > 0):
            numa_bind = False
            cpu_shard = 0
            log("k8s pod node {}: numa/cpu binding disabled (container)"
                "".format(primary_node_name))
        # numactl policy: membind (strict, default) | preferred | off, and
        # strict/auto behavior when numactl is missing on the node
        membind_mode, numa_strict = resolve_numa_policy(cfg, globals_cfg)
        numa_k = 0
        numa_topo = {}
        numactl_ok = False
        exclude = node_exclude(cluster, node)
        if numa_bind and not dry:
            numa_k, numa_topo = probe_numa_topology(host, node)
            numactl_ok = probe_numactl(host, node)
            if not numactl_ok:
                numa_node_check(primary_node_name, numa_strict)
        log("scenario {}: {} run combos, warmup={}, need_remove_before_run={}, target_qps={}".format(
            cfg_name, len(runs), warmup, need_remove_before_run, total_target_qps or "max-speed"))
        # profiling lifecycle (both triggers wrap the combo loop in basic;
        # whole_run additionally covers the per-ds setup stages inside it)
        if matrix and not dry and prof_core:
            try:
                profile_sess = prof_core.start_profiling(
                    cluster, matrix, involved_nodes, cfg_name,
                    remote_profiling_dir(remote_log_dir, cfg_name))
            except Exception as exc:
                log("[WARN] [profile] start failed: {}".format(exc))
        current_ds = None
        for c, t, ds in runs:
            _check_stop()
            if ds != current_ds:
                # data_size changed: (re)build value.bin at this size and
                # re-prefill the store for get-family combos
                ds_bytes = parse_data_size(ds)
                check_cmd = value_check_cmd(cluster_session["BENCH_REMOTE_DATA_DIR"], scenario, ds_bytes)
                data_status = ssh_check_output(host, node, check_cmd, dry_run=dry)
                if data_status != "EXISTS":
                    run_stage(host, node, remote_runner,
                              "generate-data", remote_log_dir, cfg_name,
                              extra="--data-size {}".format(ds), dry_run=dry,
                              pull_to=local_log_dir,
                              node_name=primary_node_name)
                if op in PIPELINE_GET_IFACES:
                    run_stage(host, node, remote_runner,
                              "prepare-store", remote_log_dir, cfg_name,
                              dry_run=dry, pull_to=local_log_dir,
                              node_name=primary_node_name)
                current_ds = ds
            if need_remove_before_run:
                run_stage(host, node, remote_runner,
                          "remove-keys", remote_log_dir, cfg_name,
                          extra="--scope full", dry_run=dry,
                          pull_to=local_log_dir,
                          node_name=primary_node_name)
            # data_size is always part of the run file name, so different
            # sizes under the same (c, t) never collide
            name = "benchmark_c{}_t{}_{}".format(c, t, ds)
            remote_log = remote_log_path(remote_log_dir, cfg_name, name)
            command = "bash {} benchmark {} --num-clients {} --num-threads {} --warmup {}".format(
                remote_runner, remote_log, c, t, warmup)
            if numa_bind:
                # orchestrator-computed per-client bindings; the runner only
                # prepends numactl (m/p/c spec) or falls back to client-side
                # --cpu-bind when numactl is missing on the node
                specs = []
                for i in range(1, c + 1):
                    if not dry:
                        nn = (i - 1) % numa_k
                        cl = numa_binding_spec(numa_topo[nn], exclude,
                                               cpu_shard, (i - 1) // numa_k)
                        specs.append(client_bind_spec(membind_mode, nn, cl, numactl_ok))
                    else:
                        specs.append("-")
                command += " --client-binds '{}'".format(";".join(specs))
            if total_target_qps > 0:
                # basic target_qps is TOTAL across the scenario; each client
                # process gets its share (client then divides across threads)
                command += " --target-qps {}".format(total_target_qps / c)
            # long-blocking stage: progress lines with elapsed/ETA while the
            # ssh child runs. ETA is only computable in duration mode
            # (warmup 5s + duration + small buffer); count-based runs have
            # no predictable end -> elapsed only.
            expected = None
            cfg_duration = float(cfg.get("duration", 0) or 0)
            if cfg_duration > 0:
                expected = cfg_duration + (5 if warmup == "true" else 0) + 10
            run_ssh_with_progress(host, node, command,
                                  "benchmark {} c{} t{} ({})".format(name, c, t, ds),
                                  expected_s=expected, dry_run=dry,
                                  pids_file=remote_log + ".pids",
                                  cfg_name=cfg_name, node_name=primary_node_name)
            pull_stage_logs(host, node,
                            [remote_log] + ([remote_log + ".pids"] if c > 1 else []),
                            local_log_dir, dry)
            # clients exited naturally -> drop their ledger rows
            if not dry:
                state_remove_cfg(cfg_name)
        # Config end: remove only the keys this config wrote (key-prefix
        # scope, bounded by BENCH_KEY_COUNT from env.sh). Full scope here would
        # wipe shared store data written by other nodes/scenarios; the full
        # sweep is deferred to the sweep-end remove-all in main(). The log
        # name is distinct (<cfg>.remove-keys-end.log) so it sits next to the
        # config-start remove-keys log instead of overwriting it.
        remove_log = remote_log_path(remote_log_dir, cfg_name, "remove-keys-end")
        command = "bash {} remove-keys {} --scope key-prefix".format(remote_runner, remove_log)
        run_ssh(host, node, command, dry_run=dry)
        pull_logs(cluster, involved_nodes, remote_log_dir, cfg_name, local_log_dir, dry)
        echo_cpu_bindings(cfg_name, local_log_dir)
        # profiling: stop sampling + spdiag show + collect (never raises)
        if profile_sess is not None:
            prof_core.stop_and_collect(cluster, profile_sess,
                                       os.path.join(RESULTS_DIR, ts, "logs", cfg_name, "profiling"))
        # k8s pods stay alive past this return: main()'s sweep-end
        # remove-keys still needs a live pod (store keys are not in the
        # pod workspace); cleanup_pods runs in main() after sweep-end
        # Sweep-end remove-all is issued by main() on the last successful
        # config's node; hand it this config's coordinates.
        return {"host": host, "node": primary_node_name, "runner": remote_runner,
                "remote_log_dir": remote_log_dir}
    except Exception:
        # FAIL path: preserve the remote logs for diagnosis, then re-raise.
        # Basic clients may have been orphaned mid-run -> reap the ledger.
        if not dry:
            fail_cleanup_basic(cluster, involved_nodes, remote_log_dir, cfg_name, local_log_dir)
            reap_state_entries(cluster)
            if profile_sess is not None:
                prof_core.stop_and_collect(cluster, profile_sess,
                                           os.path.join(RESULTS_DIR, ts, "logs", cfg_name, "profiling"))
            if is_k8s:
                # pod was Ready; the benchmark command failed -> keep pod and
                # namespace for troubleshooting (STOP / --clean-up tears down)
                cluster["_k8s_keep_ns"] = True
                _k8s_debug_hints(cluster, involved_nodes, cfg_name)
            else:
                cleanup_pods(cluster, involved_nodes, dry)
        raise


# --------------------------------------------------------------------------
# pipeline scenario (producer/consumer: writers notify random reader subsets)
# --------------------------------------------------------------------------

def _k8s_pipeline_nodes(cluster, dry_run=False):
    """k8s pipeline node set: N = ALL Ready cluster nodes (masters included —
    they participate in the work on this deployment). Returns sorted node
    names; the per-role Deployments spread replicas across them."""
    c = cluster["_controller"]
    out, rc = controller_run(
        c, ["get", "nodes", "-o",
            "jsonpath={range .items[*]}{.metadata.name}{' '}"
            "{.status.conditions[?(@.type=='Ready')].status}{'\\n'}{end}"],
        check=False)
    if rc != 0:
        raise RuntimeError("kubectl get nodes failed: {}".format(
            out.strip()[:300]))
    nodes = []
    for line in out.strip().splitlines():
        parts = line.split()
        if len(parts) == 2 and parts[1] == "True":
            nodes.append(parts[0])
    if not nodes:
        raise RuntimeError("no Ready k8s nodes found (kubectl get nodes)")
    return sorted(nodes)


def _deployment_template_path(cluster):
    k8s = cluster.get("k8s") or {}
    return k8s.get("deployment_template") or os.path.join(
        CONFIGS_DIR, "_k8s", "templates", "benchmark-deployment.yaml.tpl")


def _render_deployment_yaml(cluster, ts, cfg_name, role, replicas):
    k8s = k8s_settings(cluster)
    run_label = k8s_run_label(ts, cfg_name)
    deploy = "{}-{}".format(k8s_pod_name(ts, cfg_name), role)
    with open(_deployment_template_path(cluster), "r", encoding="utf-8") as f:
        text = f.read()
    repl = {
        "{{MC_DEPLOY_NAME}}": deploy,
        "{{MC_NAMESPACE}}": k8s["namespace"],
        "{{MC_REPLICAS}}": str(replicas),
        "{{MC_RUN_LABEL}}": run_label,
        "{{MC_ROLE_LABEL}}": role,
        "{{MC_IMAGE}}": k8s["image"],
        "{{MC_CONFIGMAP_NAME}}": _configmap_name(run_label),
        "{{MC_SCRIPTS_MOUNT}}": k8s["pod_workspace"] + "/scripts",
        "{{MC_WORKDIR_MOUNT}}": k8s["pod_workspace"],
    }
    for key, val in repl.items():
        text = text.replace(key, val)
    if "{{MC_" in text:
        raise RuntimeError("deployment template has unresolved "
                           "{{MC_*}} placeholders")
    return deploy, run_label, text


def _log_deployment_events(controller, ns, deploy):
    """Best-effort: dump the tail of `kubectl describe deployment`."""
    try:
        out, rc = controller_run(
            controller, ["-n", ns, "describe", "deployment", deploy],
            check=False)
        if rc == 0 and out.strip():
            lines = out.strip().splitlines()
            log("[k8s] deployment {} describe (tail):\n{}".format(
                deploy, "\n".join(lines[-25:])))
    except Exception:
        pass


def ensure_k8s_deployment(cluster, ts, cfg_name, role, replicas, dry_run=False):
    """Create the per-role pipeline Deployment (pod = process) and wait for
    its rollout (60s, describe dump on failure). Registers the deployment
    for cleanup_pods. Returns (deploy_name, run_label)."""
    k8s = k8s_settings(cluster)
    c = cluster["_controller"]
    ns = k8s["namespace"]
    deploy, run_label, yaml_text = _render_deployment_yaml(
        cluster, ts, cfg_name, role, replicas)
    cluster.setdefault("_k8s_deployments", []).append(deploy)
    if dry_run:
        log("[k8s] (dry-run) would create deployment {} (role={}, "
            "replicas={}, ns={})".format(deploy, role, replicas, ns))
        return deploy, run_label
    import tempfile
    local_dir = tempfile.mkdtemp(prefix="mc-deploy-")
    local_yaml = os.path.join(local_dir, deploy + ".yaml")
    try:
        with open(local_yaml, "w", encoding="utf-8", newline="\n") as f:
            f.write(yaml_text)
        yaml_dir = posixpath.join(k8s["controller_workspace"], "pods")
        controller_put(c, [local_yaml], yaml_dir)
        remote_yaml = posixpath.join(yaml_dir, deploy + ".yaml")
        log("[k8s] deployment manifest: template={} -> {}:{}".format(
            os.path.relpath(_deployment_template_path(cluster), ROOT),
            controller_desc(c), remote_yaml))
        log("[k8s] deployment {} create: ns={} role={} replicas={} image={}"
            "".format(deploy, ns, role, replicas, k8s["image"]))
        out, rc = controller_run(
            c, ["-n", ns, "apply", "-f", remote_yaml], check=False)
        if rc != 0:
            raise RuntimeError("kubectl apply failed for deployment {}: {}"
                               "".format(deploy, out.strip()[:400]))
    finally:
        shutil.rmtree(local_dir, ignore_errors=True)
    t0 = time.time()
    out, rc = controller_run(
        c, ["-n", ns, "rollout", "status", "deployment/" + deploy,
            "--timeout=60s"], check=False)
    if rc != 0:
        _log_deployment_events(c, ns, deploy)
        raise RuntimeError("deployment {} not ready in 60s: {}".format(
            deploy, out.strip()[:400]))
    log("[k8s] deployment {} ready (role={}, replicas={}, elapsed={:.1f}s)"
        "".format(deploy, role, replicas, time.time() - t0))
    return deploy, run_label


def _k8s_pod_ip(controller, ns, pod):
    out, rc = controller_run(
        controller, ["-n", ns, "get", "pod", pod, "-o",
                     "jsonpath={.status.podIP}"], check=False)
    ip = out.strip()
    if rc != 0 or not ip:
        raise RuntimeError("cannot resolve Pod IP for {}".format(pod))
    return ip


def _pipeline_k8s_roles(plan):
    """Deployment role list for the k8s pipeline channel. dual mode deploys
    ONE "client" role (pod = dual-role process); separate mode deploys a
    reader and a writer role. Each entry: (role, plan entries, port base).
    NOTE: the dual branch MUST stay a 1-tuple wrapping the role tuple —
    returning the bare tuple makes the caller's for-loop iterate its
    ELEMENTS and unpack the role STRING ("too many values to unpack")."""
    if plan.get("dual"):
        return (("client", plan["clients"], plan["writer_port_base"]),)
    return (("reader", plan["readers"], plan["reader_port_base"]),
            ("writer", plan["writers"], plan["writer_port_base"]))


def _k8s_deployment_pods(cluster, run_label, role):
    """Sorted pod names of a pipeline role's Deployment (label selector)."""
    c = cluster["_controller"]
    ns = k8s_settings(cluster)["namespace"]
    out, rc = controller_run(
        c, ["-n", ns, "get", "pods", "-l",
            "run={},role={}".format(run_label, role),
            "-o", "jsonpath={.items[*].metadata.name}"], check=False)
    if rc != 0:
        raise RuntimeError("cannot list pods (run={}, role={}): {}".format(
            run_label, role, out.strip()[:200]))
    return sorted(p for p in out.split() if p)


def run_pipeline_scenario(cluster, globals_cfg, cfg, ts, args):
    apply_cli_overrides(cfg, args)
    matrix = getattr(args, "_matrix", None)
    scenario = cfg["scenario"]
    cfg_name = os.path.splitext(os.path.basename(cfg.get("__cfg_path__", scenario)))[0]
    dry = args.dry_run
    op = cfg.get("operation", "")
    work_mode = cfg.get("work_mode", cfg.get("mode", ""))
    # pipeline rendezvous requires the coordination etcd (reader
    # registration / writer discovery / start trigger)
    etcd_endpoints = _pipeline_etcd_endpoints(globals_cfg)
    rendezvous_timeout_s = int(globals_cfg.get("rendezvous_timeout_s", 120) or 120)
    run_id = "{}-{}".format(ts, cfg_name)
    # ---- cluster-oriented node set: both roles run on EVERY cluster node ----
    is_k8s = cluster.get("_controller") is not None
    if is_k8s:
        # k8s channel: N = ALL Ready cluster nodes; one Deployment per role
        # (pod = process, replicas = count_per_node x N, spread maxSkew=1).
        # Plan slots materialize to POD names after the Deployments roll out.
        k8s_nodes = _k8s_pipeline_nodes(cluster, dry)
        n_nodes = len(k8s_nodes)
        plan = pipeline_plan(cfg, node_count=n_nodes)
    else:
        node_names = sorted(n for n in (cluster.get("nodes") or {})
                            if not str(n).startswith("_"))
        if not node_names:
            raise RuntimeError(
                "pipeline requires a non-empty cluster node set (N): put worker "
                "nodes in cluster.json (ssh) or use a k8s cluster with Ready "
                "nodes")
        n_nodes = len(node_names)
        plan = pipeline_plan(cfg, node_count=n_nodes)
        pool = plan["clients"] if plan.get("dual") else \
            plan["writers"] + plan["readers"]
        for entry in pool:
            entry["node"] = node_names[entry["slot"]]
    if plan["notify_count"] < 1:
        raise RuntimeError("pipeline config requires notify_count >= 1")
    if plan.get("dual"):
        # dual role: a writer never notifies its own reader -> the sampling
        # pool is clients-1
        if plan["notify_count"] > len(plan["clients"]) - 1:
            raise RuntimeError(
                "notify_count={} exceeds the dual-role sampling pool {} "
                "(client.count_per_node x N - 1; own reader excluded)".format(
                    plan["notify_count"], len(plan["clients"]) - 1))
    elif plan["notify_count"] > len(plan["readers"]):
        raise RuntimeError("notify_count={} exceeds reader count {} "
                           "(reader.count_per_node x N)".format(
                               plan["notify_count"], len(plan["readers"])))
    # optional per-writer key-space cap: must hold at least one full batch
    key_count = int(cfg.get("key_count", 0) or 0)
    if key_count and key_count < int(cfg.get("batch_size", 1)):
        raise RuntimeError(
            "key_count={} must be >= batch_size={}".format(
                key_count, cfg.get("batch_size", 1)))
    # duration is mandatory for pipeline: it is the measurement window for
    # qps/throughput on both writer and reader rows
    cfg_duration = cfg.get("duration", None)
    if not cfg_duration or float(cfg_duration) <= 0:
        raise RuntimeError("pipeline config requires duration > 0 (missing or invalid)")
    # writer/reader operation must name a supported store interface
    writer_op = (cfg.get("writer", {}) or {}).get("operation", "") or "put_batch"
    reader_op = (cfg.get("reader", {}) or {}).get("operation", "") or "get"
    if writer_op not in PIPELINE_PUT_IFACES:
        raise RuntimeError("pipeline writer operation must be one of {} "
                           "(got {})".format(PIPELINE_PUT_IFACES, writer_op))
    if reader_op not in PIPELINE_GET_IFACES:
        raise RuntimeError("pipeline reader operation must be one of {} "
                           "(got {})".format(PIPELINE_GET_IFACES, reader_op))
    if is_k8s:
        # pods do not exist yet: involved_nodes / primary / host materialize
        # after the Deployments roll out (below); topology is meaningless
        # for pods (the store is an external Service)
        involved_nodes = []
        primary_node_name = None
        node = None
        host = None
        topology = "remote_node"
    else:
        involved_nodes = []
        seen_nodes = set()
        for p in plan["writers"] + plan["readers"]:
            if p["node"] not in seen_nodes:
                seen_nodes.add(p["node"])
                involved_nodes.append(p["node"])
        primary_node_name = involved_nodes[0] if involved_nodes else cfg.get("store_node", "")
        node = get_node(cluster, primary_node_name)
        host = node["ip"]
        # topology: local iff the primary client node's IP is one of the
        # store nodes' IPs (mooncake_store_nodes from global.json,
        # resolved via cluster.json)
        store_node_ips = resolve_store_node_ips(
            cluster, globals_cfg.get("mooncake_store_nodes", []) or [])
        topology = "local_node" if node["ip"] in store_node_ips else "remote_node"
    # k8s-pod nodes: pods are created by ensure_k8s_deployment below
    ensure_pods(cluster, involved_nodes, ts, cfg_name, dry)
    remote_dir, remote_scripts, remote_log_dir, remote_runner_basic, \
        remote_runner_pipeline, local_log_dir = \
        scenario_paths(globals_cfg, ts, cfg_name)
    remote_runner = remote_runner_pipeline
    if not dry:
        os.makedirs(local_log_dir, exist_ok=True)
    log("=== scenario {} (op={}, topo={}, protocol={}) primary_node={} ===".format(
        cfg_name, op, topology, cfg["protocol"], host))
    scenario_params = base_scenario_params(cfg, work_mode, topology, primary_node_name)
    scenario_params.update({
        "BENCH_WRITER_COUNT": str(len(plan["writers"])),
        "BENCH_READER_COUNT": str(len(plan["readers"])),
        "BENCH_TARGET_QPS": str(plan["per_writer_qps"]),
        "BENCH_NOTIFY_COUNT": str(plan["notify_count"]),
        "BENCH_NOTIFY_THREADS": str(plan["notify_threads"]),
        "BENCH_NOTIFY_PIPELINE": ",".join(cfg.get("notify_pipeline", [])),
        "BENCH_TRAFFIC_MODE": cfg.get("traffic_mode", "pulse"),
        "BENCH_DURATION": str(cfg.get("duration", 30)),
        "BENCH_BATCH_SIZE": str(cfg.get("batch_size", 1)),
        # key_count (optional): per-writer key-space cap — when set the writer
        # wraps its sequence with delete-before-write, bounding store usage at
        # writers x key_count x data_size; empty = unbounded (legacy behaviour)
        "BENCH_KEY_COUNT": str(cfg.get("key_count", "") or ""),
        # clear basic-only vars that are meaningless for pipeline
        "BENCH_OPERATION": "",
        "BENCH_WARMUP_DURATION": str(cfg.get("warmup", {}).get("duration", 0)),
        # etcd rendezvous scope + barrier timeouts (client-side fallbacks)
        "BENCH_RUN_ID": run_id,
        "BENCH_RENDEZVOUS_TIMEOUT": str(rendezvous_timeout_s),
    })
    cluster_session = build_cluster_session(cluster, globals_cfg, remote_dir)
    # k8s: ConfigMap (scripts + shared env.sh) must exist before pods mount it
    ensure_configmap(cluster, scenario_params, cluster_session, ts, cfg_name, dry)
    if is_k8s:
        # ---- per-role Deployments (pod = process): replicas = cpn x N ----
        _k8s = k8s_settings(cluster)
        _run_label = k8s_run_label(ts, cfg_name)
        for role, entries, base in _pipeline_k8s_roles(plan):
            ensure_k8s_deployment(cluster, ts, cfg_name, role,
                                  len(entries), dry)
            if dry:
                pods = ["{}-{}-{:03d}".format(_run_label, role, i)
                        for i in range(len(entries))]
            else:
                pods = _k8s_deployment_pods(cluster, _run_label, role)
                if len(pods) < len(entries):
                    raise RuntimeError(
                        "deployment role={} has {}/{} pods (run={})".format(
                            role, len(pods), len(entries), _run_label))
            for entry, pod in zip(entries, pods):
                entry["node"] = pod
                # every pod has its own network namespace: ALL pods of a
                # role listen on the SAME port
                entry["port"] = base
                ip = "0.0.0.0" if dry else _k8s_pod_ip(
                    cluster["_controller"], _k8s["namespace"], pod)
                cluster.setdefault("nodes", {})[pod] = {
                    "kind": "k8s-pod", "_pod": pod, "_ns": _k8s["namespace"],
                    "_ctx": cluster["_controller"].get("_ctx", ""),
                    "_controller": cluster["_controller"],
                    "_controller_workspace": _k8s["controller_workspace"],
                    "_run_label": _run_label, "_role": role,
                    "ip": ip, "fabric_ip": ip}
        involved_nodes = []
        for e in plan["writers"] + plan["readers"]:
            if e["node"] not in involved_nodes:
                involved_nodes.append(e["node"])
        primary_node_name = (plan["writers"] or plan["readers"])[0]["node"]
        node = get_node(cluster, primary_node_name)
        host = node["ip"]
        if plan.get("dual"):
            log("[k8s] pipeline pods ready: {} dual clients "
                "(N={}, spread maxSkew=1, run={})".format(
                    len(plan["clients"]), n_nodes, _run_label))
        else:
            log("[k8s] pipeline pods ready: {} writers + {} readers "
                "(N={}, spread maxSkew=1, run={})".format(
                    len(plan["writers"]), len(plan["readers"]),
                    n_nodes, _run_label))
    deploy_scripts(cluster, involved_nodes, scenario_params, cluster_session,
                   remote_scripts, remote_log_dir, dry)
    reader_pids = []
    writer_pids = []
    profile_sess = None  # profiling session (started at Step 1 or Step 3)
    # optional NUMA binding: per node, round-robin the node's client
    # processes (readers launched first, then writers) across its NUMA nodes.
    # Resolution: case/layer explicit value > global.json ("numa_bind",
    # default true) > code default true; explicit false disables.
    numa_bind = cfg.get("numa_bind")
    if numa_bind is None:
        numa_bind = globals_cfg.get("numa_bind")
    numa_bind = True if numa_bind is None else bool(numa_bind)
    # per-client core count (NUMA-aware sharding). When set it takes
    # precedence over plain numa round-robin: each client process gets a
    # consecutive cpu_shard-core slice of its NUMA node's allowed pool
    # (exclude_cpus subtracted here), wrapping when the pool is smaller
    # than clients x shard.
    cpu_shard = int(cfg.get("cpu_shard", 0) or 0)
    if cpu_shard > 0 and not numa_bind:
        numa_bind = True  # sharding needs the NUMA topology probes
    if is_k8s and (numa_bind or cpu_shard > 0):
        # containers do not do numactl: binding disabled for pipeline pods
        numa_bind = False
        cpu_shard = 0
        log("k8s pipeline pods: numa/cpu binding disabled (containers)")
    # numactl policy: membind (strict, default) | preferred | off, and
    # strict/auto behavior when numactl is missing on a node
    membind_mode, numa_strict = resolve_numa_policy(cfg, globals_cfg)
    numa_node_count = {}
    numa_counter = {}
    shard_ordinals = {}  # (node, numa) -> next slice ordinal
    numa_topo = {}       # node_name -> {numa_idx: cpulist}
    numactl_ok = {}      # node_name -> numactl usable

    def shard_ordinal(node_name, numa):
        key = (node_name, numa)
        idx = shard_ordinals.get(key, 0)
        shard_ordinals[key] = idx + 1
        return idx

    if numa_bind:
        for node_name in involved_nodes:
            n = get_node(cluster, node_name)
            if not dry:
                k, topo = probe_numa_topology(n["ip"], n)
                numa_node_count[node_name] = k
                numa_topo[node_name] = topo
                numactl_ok[node_name] = probe_numactl(n["ip"], n)
                if not numactl_ok[node_name]:
                    numa_node_check(node_name, numa_strict)
            else:
                # dry-run: no ssh probing (the old count-only probe failed
                # on dry-run's empty result); commands print without
                # binding args
                numa_node_count[node_name] = 1
                numa_topo[node_name] = {0: "0"}
                numactl_ok[node_name] = False
            numa_counter[node_name] = 0
    try:
        sizes = data_size_list(cfg)
        warmup = "false" if args.no_warmup else "true"
        reader_threads = plan["reader_threads"]
        log("scenario {} (pipeline, {}): N={}, writers={}, readers={}, "
            "notify_count={}, total_qps={}, per_writer_qps={}, traffic={}, "
            "run_id={}, sizes={}".format(
                cfg_name, work_mode, n_nodes, len(plan["writers"]),
                len(plan["readers"]), plan["notify_count"], plan["total_qps"],
                plan["per_writer_qps"], cfg.get("traffic_mode", "pulse"),
                run_id, sizes))
        current_ds = None
        for ds in sizes:
            if ds != current_ds:
                # data_size changed: (re)build value.bin at this size on every
                # node that runs a writer or reader
                ds_bytes = parse_data_size(ds)
                check_cmd = value_check_cmd(cluster_session["BENCH_REMOTE_DATA_DIR"], scenario, ds_bytes)
                for node_name in involved_nodes:
                    n = get_node(cluster, node_name)
                    data_status = ssh_check_output(n["ip"], n, check_cmd, dry_run=dry)
                    if data_status != "EXISTS":
                        run_stage(n["ip"], n, remote_runner,
                                  "generate-data", remote_log_dir, cfg_name,
                                  extra="--data-size {}".format(ds), dry_run=dry,
                                  pull_to=local_log_dir, node_name=node_name)
                current_ds = ds
            # key-space budget against store_total_size: explicit key_count
            # is validated strictly; an unset key_count gets an auto
            # per-writer cap (writers wrap inside it with untimed
            # delete-before-write, so put samples stay real writes)
            kc, kc_auto = _pipeline_key_budget(
                globals_cfg, len(plan["writers"]), parse_data_size(ds),
                int(cfg.get("key_count", 0) or 0),
                int(cfg.get("batch_size", 1) or 1), ds)
            kc_arg = " --key-count {}".format(kc) if kc else ""
            if kc_auto:
                cfg.setdefault("_key_space", {})[str(ds)] = kc
                log("pipeline key space (auto): {} writers x {} keys x {} "
                    "= {}B of store_total_size {}B (0.9 headroom)".format(
                        len(plan["writers"]), kc, ds,
                        len(plan["writers"]) * kc * parse_data_size(ds),
                        _store_total_bytes(globals_cfg)))
            run_stage(host, node, remote_runner,
                      "remove-keys", remote_log_dir, cfg_name,
                      extra="--scope full", dry_run=dry, pull_to=local_log_dir,
                      node_name=primary_node_name)
            # fresh rendezvous state per size iteration (stale registrations
            # from a previous size must never leak into this one)
            reader_pids = []
            writer_pids = []
            if not dry:
                _etcd_delete_prefix(cluster, etcd_endpoints,
                                    "mcbench/{}/".format(run_id))
            # data_size is always part of the run file name, so different
            # sizes never share writer/reader logs
            run_name = "benchmark_c1_t{}_{}".format(reader_threads, ds)
            base_noext = remote_log_path(remote_log_dir, cfg_name, run_name).replace(".log", "")
            # profiling lifecycle: whole_run covers everything from reader
            # boot; per_stage starts just before the writers (measurement
            # window) — stop/collect happens after Step 6 / on failure
            if matrix and not dry and prof_core and profile_sess is None:
                if matrix.get("trigger", "whole_run") == "whole_run":
                    try:
                        profile_sess = prof_core.start_profiling(
                            cluster, matrix, involved_nodes, cfg_name,
                            remote_profiling_dir(remote_log_dir, cfg_name))
                    except Exception as exc:
                        log("[WARN] [profile] start failed: {}".format(exc))
            if plan.get("dual"):
                # ---- dual-role flow: every client hosts writer+reader behind
                # one HTTP port; pids ledger into BOTH lists so the shared
                # fail-path (graceful /shutdown + kill + pull) just works ----
                for c in plan["clients"]:
                    rn = get_node(cluster, c["node"])
                    merged_log = "{}.dual_{}_c{}_p{}.log".format(
                        base_noext, c["node"], c["id"], c["port"])
                    nohup_log = merged_log.replace(".log", ".nohup")
                    numa_arg = ""
                    launch_prefix = ""
                    if (cpu_shard > 0 or numa_bind) and not dry \
                            and not is_k8s_node(rn):
                        nn = numa_counter[c["node"]] % numa_node_count[c["node"]]
                        numa_counter[c["node"]] += 1
                        idx = shard_ordinal(c["node"], nn) if cpu_shard > 0 else 0
                        cl = numa_binding_spec(numa_topo[c["node"]][nn],
                                               node_exclude(cluster, rn),
                                               cpu_shard, idx)
                        numa_arg = "--cpu-bind {}".format(cl)
                        if numactl_ok.get(c["node"]):
                            launch_prefix = numactl_launch_prefix(membind_mode, nn, cl) + " "
                    ccmd = ("nohup {}bash {} pipeline {} --client-id {} --role both "
                            "--listen-port {} --writer-operation {} --reader-operation {} "
                            "--num-threads {} --warmup {} --timeout 300{} {} > {} 2>&1 "
                            "& echo $!").format(
                        launch_prefix, remote_runner, merged_log, c["id"], c["port"],
                        writer_op, reader_op, reader_threads, warmup, kc_arg,
                        numa_arg, nohup_log)
                    pid = ssh_check_output(rn["ip"], rn, ccmd, dry_run=dry)
                    reader_pids.append((c, pid))
                    writer_pids.append((c, pid))
                    if not dry and pid:
                        state_append([{
                            "ip": rn["ip"], "node": c["node"], "port": c["port"],
                            "fabric_host": rn.get("fabric_ip", rn.get("ip", "")),
                            "pid": pid.strip(), "label": "dual c{}".format(c["id"]),
                            "marker": "run_benchmark_pipeline.sh",
                            "cfg": cfg_name,
                            "ts": time.strftime("%Y-%m-%d %H:%M:%S")}])
                # barriers: every client registered BOTH roles, then trigger
                if not dry:
                    t0 = time.time()
                    _pipeline_barrier(cluster, etcd_endpoints,
                                      "mcbench/{}/readers/".format(run_id),
                                      len(plan["clients"]), rendezvous_timeout_s,
                                      "dual readers", run_id)
                    _pipeline_barrier(cluster, etcd_endpoints,
                                      "mcbench/{}/writers/".format(run_id),
                                      len(plan["clients"]), rendezvous_timeout_s,
                                      "dual writers", run_id)
                    log("pipeline rendezvous: {}/{} dual clients registered "
                        "({:.1f}s)".format(len(plan["clients"]),
                                           len(plan["clients"]),
                                           time.time() - t0))
                    for c in plan["clients"]:
                        cn = get_node(cluster, c["node"])
                        if not _pipeline_http_call(cn, c["port"], "/start"):
                            raise RuntimeError(
                                "pipeline trigger failed: dual c{} /start at "
                                "{} p{} returned non-200".format(
                                    c["id"], cn["ip"], c["port"]))
                    log("pipeline trigger: /start sent to {} dual clients"
                        "".format(len(plan["clients"])))
                    # writers run INSIDE the clients: after the expected
                    # window (+grace) shut the reader side down so every
                    # client flushes both summaries and exits
                    expected_s = float(cfg.get("warmup", {}).get("duration", 0) or 0) \
                        + float(cfg.get("duration", 30)) + 10.0
                    log("pipeline dual: waiting {:.0f}s for writer phases, "
                        "then /shutdown".format(expected_s))
                    t0 = time.time()
                    while time.time() - t0 < expected_s:
                        _check_stop()
                        time.sleep(min(1.0, expected_s - (time.time() - t0)))
                    for c in plan["clients"]:
                        cn = get_node(cluster, c["node"])
                        try:
                            _pipeline_http_call(cn, c["port"], "/shutdown")
                        except Exception as exc:
                            log("[WARN] dual c{} shutdown: {}".format(
                                c["id"], exc))
                    for c, pid in writer_pids:
                        if not dry and pid:
                            wait_process_exit(cluster, c["node"], pid,
                                              "dual c{}".format(c["id"]),
                                              60, dry_run=dry, expected_s=60)
                    for c in plan["clients"]:
                        cn = get_node(cluster, c["node"])
                        pull_stage_logs(cn["ip"], cn,
                                        ["{}.dual_{}_c{}_p{}.log".format(
                                            base_noext, c["node"], c["id"],
                                            c["port"])],
                                        local_log_dir, dry)
            else:
                # Step 1: start all readers (one process per reader). Each reader
                # binds its HTTP /notify server and SELF-REGISTERS into etcd
                # (host=POD_IP/SETUP_FABRIC_IP, port, node); no peer addresses
                # are injected by the orchestrator anymore. Log names carry the
                # node segment: per-node port numbering (9201+i) is no longer
                # globally unique once several nodes each run x readers.
                for r in plan["readers"]:
                    rn = get_node(cluster, r["node"])
                    merged_log = "{}.reader_{}_r{}_p{}.log".format(
                        base_noext, r["node"], r["index"], r["port"])
                    nohup_log = merged_log.replace(".log", ".nohup")
                    numa_arg = ""
                    launch_prefix = ""
                    if (cpu_shard > 0 or numa_bind) and not dry \
                            and not is_k8s_node(rn):
                        # round-robin over NUMA nodes; with cpu_shard the
                        # per-(node,numa) ordinal picks a consecutive slice
                        nn = numa_counter[r["node"]] % numa_node_count[r["node"]]
                        numa_counter[r["node"]] += 1
                        idx = shard_ordinal(r["node"], nn) if cpu_shard > 0 else 0
                        cl = numa_binding_spec(numa_topo[r["node"]][nn],
                                               node_exclude(cluster, rn),
                                               cpu_shard, idx)
                        numa_arg = "--cpu-bind {}".format(cl)
                        if numactl_ok.get(r["node"]):
                            launch_prefix = numactl_launch_prefix(membind_mode, nn, cl) + " "
                    rcmd = ("nohup {}bash {} pipeline {} --client-id {} --num-threads {} "
                            "--role reader --listen-port {} --warmup false --timeout 300 "
                            "--operation {} {} > {} 2>&1 & echo $!").format(
                        launch_prefix, remote_runner, merged_log, r["index"],
                        reader_threads, r["port"], reader_op, numa_arg, nohup_log)
                    pid = ssh_check_output(rn["ip"], rn, rcmd, dry_run=dry)
                    reader_pids.append((r, pid))
                    if not dry and pid:
                        # ledger: this reader can always be found & killed later
                        state_append([{
                            "ip": rn["ip"], "node": r["node"], "port": r["port"],
                            "fabric_host": rn.get("fabric_ip", rn.get("ip", "")),
                            "pid": pid.strip(), "label": "reader r{}".format(r["index"]),
                            # pid 是 run_benchmark_pipeline.sh 的 bash 包装进程（echo $!），
                            # python client 是其子进程；marker 用于 reap 的防误杀校验
                            "marker": "run_benchmark_pipeline.sh",
                            "cfg": cfg_name,
                            "ts": time.strftime("%Y-%m-%d %H:%M:%S")}])
                # Step 2: etcd barrier — wait for the FULL reader pool to
                # self-register before any writer starts (readers that fail to
                # boot surface here as a timeout with the missing list)
                if not dry:
                    t0 = time.time()
                    _pipeline_barrier(cluster, etcd_endpoints,
                                      "mcbench/{}/readers/".format(run_id),
                                      len(plan["readers"]), rendezvous_timeout_s,
                                      "readers", run_id)
                    log("pipeline rendezvous: {}/{} readers registered ({:.1f}s)"
                        "".format(len(plan["readers"]), len(plan["readers"]),
                                  time.time() - t0))
                # Step 3: start all writers in parallel (one invocation per writer; the
                # explicit --client-id in EXTRA overrides the per-invocation default
                # and gives each writer a globally unique id). ALL writers run the
                # warmup phase so every writer's store/notify connections are warmed.
                if matrix and not dry and prof_core and profile_sess is None:
                    # per_stage trigger: profiling starts at the measurement window
                    try:
                        profile_sess = prof_core.start_profiling(
                            cluster, matrix, involved_nodes, cfg_name,
                            remote_profiling_dir(remote_log_dir, cfg_name))
                    except Exception as exc:
                        log("[WARN] [profile] start failed: {}".format(exc))
                for w in plan["writers"]:
                    wn = get_node(cluster, w["node"])
                    merged_log = "{}.writer_{}_w{}.log".format(
                        base_noext, w["node"], w["gid"])
                    nohup_log = merged_log.replace(".log", ".nohup")
                    numa_arg = ""
                    launch_prefix = ""
                    if (cpu_shard > 0 or numa_bind) and not dry \
                            and not is_k8s_node(wn):
                        nn = numa_counter[w["node"]] % numa_node_count[w["node"]]
                        numa_counter[w["node"]] += 1
                        idx = shard_ordinal(w["node"], nn) if cpu_shard > 0 else 0
                        cl = numa_binding_spec(numa_topo[w["node"]][nn],
                                               node_exclude(cluster, wn),
                                               cpu_shard, idx)
                        numa_arg = "--cpu-bind {}".format(cl)
                        if numactl_ok.get(w["node"]):
                            launch_prefix = numactl_launch_prefix(membind_mode, nn, cl) + " "
                    wcmd = ("nohup {}bash {} pipeline {} --client-id {} --role writer "
                            "--listen-port {} --warmup {} --operation {}{} {} > {} 2>&1 "
                            "& echo $!").format(
                        launch_prefix, remote_runner, merged_log, w["gid"], w["port"],
                        warmup, writer_op, kc_arg, numa_arg, nohup_log)
                    pid = ssh_check_output(wn["ip"], wn, wcmd, dry_run=dry)
                    writer_pids.append((w, pid))
                    if not dry and pid:
                        state_append([{
                            "ip": wn["ip"], "node": w["node"], "port": w["port"],
                            "fabric_host": wn.get("fabric_ip", wn.get("ip", "")),
                            "pid": pid.strip(), "label": "writer w{}".format(w["gid"]),
                            "marker": "run_benchmark_pipeline.sh",
                            "cfg": cfg_name,
                            "ts": time.strftime("%Y-%m-%d %H:%M:%S")}])
                # Step 4: etcd barrier — every writer registered (= reader list
                # pulled from etcd + HTTP /start server bound), then TRIGGER the
                # run: POST /start to each writer (k8s: direct pod IP; ssh:
                # loopback curl on the writer's node). All writers enter their
                # warmup + measured phases only after this synchronized start.
                if not dry:
                    t0 = time.time()
                    _pipeline_barrier(cluster, etcd_endpoints,
                                      "mcbench/{}/writers/".format(run_id),
                                      len(plan["writers"]), rendezvous_timeout_s,
                                      "writers", run_id)
                    log("pipeline rendezvous: {}/{} writers registered ({:.1f}s)"
                        "".format(len(plan["writers"]), len(plan["writers"]),
                                  time.time() - t0))
                    for w in plan["writers"]:
                        wn = get_node(cluster, w["node"])
                        if not _pipeline_http_call(wn, w["port"], "/start"):
                            raise RuntimeError(
                                "pipeline trigger failed: writer w{} /start at "
                                "{} p{} returned non-200".format(
                                    w["gid"], wn["ip"], w["port"]))
                    log("pipeline trigger: /start sent to {} writers".format(
                        len(plan["writers"])))
                # Step 5: wait for all writers to finish (warmup + measured run)
                warmup_duration = float(cfg.get("warmup", {}).get("duration", 0) or 0)
                duration = float(cfg.get("duration", 30))
                for w, pid in writer_pids:
                    if not dry and pid:
                        wait_process_exit(cluster, w["node"], pid,
                                          "writer w{}".format(w["gid"]),
                                          int(warmup_duration + duration) + 300, dry_run=dry,
                                          expected_s=warmup_duration + duration)
                for w in plan["writers"]:
                    wn = get_node(cluster, w["node"])
                    pull_stage_logs(wn["ip"], wn,
                                    ["{}.writer_{}_w{}.log".format(
                                        base_noext, w["node"], w["gid"])],
                                    local_log_dir, dry)
                # Step 6: shutdown readers via their /shutdown endpoint (k8s:
                # direct pod IP; ssh: loopback curl on the reader's node) and
                # wait for their final logs
                for r in plan["readers"]:
                    rn = get_node(cluster, r["node"])
                    if not dry:
                        try:
                            _pipeline_http_call(rn, r["port"], "/shutdown")
                        except Exception as exc:
                            log("[WARN] reader r{} shutdown: {}".format(
                                r["index"], exc))
                for r, pid in reader_pids:
                    if not dry and pid:
                        wait_process_exit(cluster, r["node"], pid,
                                          "reader r{} (port {})".format(r["index"], r["port"]),
                                          60, dry_run=dry, expected_s=60)
                for r in plan["readers"]:
                    rn = get_node(cluster, r["node"])
                    pull_stage_logs(rn["ip"], rn,
                                    ["{}.reader_{}_r{}_p{}.log".format(
                                        base_noext, r["node"], r["index"], r["port"])],
                                    local_log_dir, dry)
            # Step 7: remove empty .nohup redirect files (they capture script-level
            # errors only — empty on success). Non-empty ones are kept for diagnostics
            # and pulled back with the rest of the logs.
            if not dry:
                for node_name in involved_nodes:
                    n = get_node(cluster, node_name)
                    nohup_files = []
                    if plan.get("dual"):
                        for c in plan["clients"]:
                            if c["node"] == node_name:
                                nohup_files.append(
                                    "{}.dual_{}_c{}_p{}.nohup".format(
                                        base_noext, c["node"], c["id"], c["port"]))
                    else:
                        for r in plan["readers"]:
                            if r["node"] == node_name:
                                nohup_files.append(
                                    "{}.reader_{}_r{}_p{}.nohup".format(
                                        base_noext, r["node"], r["index"], r["port"]))
                        for w in plan["writers"]:
                            if w["node"] == node_name:
                                nohup_files.append(
                                    "{}.writer_{}_w{}.nohup".format(
                                        base_noext, w["node"], w["gid"]))
                    if nohup_files:
                        cleanup_cmd = "for f in {}; do [ -s \"$f\" ] || rm -f \"$f\"; done".format(
                            " ".join(nohup_files))
                        ssh_check_output(n["ip"], n, cleanup_cmd, dry_run=dry)
                # this size iteration is done: wipe its rendezvous state
                _etcd_delete_prefix(cluster, etcd_endpoints,
                                    "mcbench/{}/".format(run_id))
        # Config end: remove-all — clears everything this config's writers
        # wrote. Pipeline env clears BENCH_KEY_COUNT, so the key-prefix scope has
        # no key count to work with; a full sweep here is short and precise
        # enough. The log name is distinct (<cfg>.remove-keys-end.log) so it
        # sits next to the config-start remove-keys log.
        remove_log = remote_log_path(remote_log_dir, cfg_name, "remove-keys-end")
        command = "bash {} remove-keys {} --scope full".format(remote_runner, remove_log)
        run_ssh(host, node, command, dry_run=dry)
        pull_logs(cluster, involved_nodes, remote_log_dir, cfg_name, local_log_dir, dry)
        echo_cpu_bindings(cfg_name, local_log_dir)
        # All pipeline processes exited naturally -> drop their ledger rows.
        if not dry:
            state_remove_cfg(cfg_name)
        # profiling: stop sampling + spdiag show + collect (never raises)
        if profile_sess is not None:
            prof_core.stop_and_collect(cluster, profile_sess,
                                       os.path.join(RESULTS_DIR, ts, "logs", cfg_name, "profiling"))
        # k8s pods stay alive past this return: main()'s sweep-end
        # remove-keys still needs a live pod (store keys are not in the
        # pod workspace); cleanup_pods runs in main() after sweep-end
        # Sweep-end remove-all is issued by main() on the last successful
        # config's node; hand it this config's coordinates.
        return {"host": host, "node": primary_node_name, "runner": remote_runner,
                "remote_log_dir": remote_log_dir}
    except Exception:
        # FAIL path: preserve every remote log (reader SUMMARY/RESULT is only
        # flushed on exit) and free ports, then re-raise for main's [FAIL]
        if not dry:
            fail_cleanup_pipeline(cluster, plan, reader_pids, writer_pids,
                                  involved_nodes, remote_log_dir, cfg_name,
                                  local_log_dir, etcd_endpoints=etcd_endpoints,
                                  run_id=run_id)
            # 复核台账：已死删行、杀不死的保留待下次 reap
            reap_state_entries(cluster)
            if profile_sess is not None:
                prof_core.stop_and_collect(cluster, profile_sess,
                                           os.path.join(RESULTS_DIR, ts, "logs", cfg_name, "profiling"))
            if is_k8s:
                # pods were Ready (or rolled out); keep Deployments + pods +
                # namespace for troubleshooting (STOP / --clean-up tears down)
                cluster["_k8s_keep_ns"] = True
                _k8s_debug_hints(cluster, involved_nodes, cfg_name)
            else:
                cleanup_pods(cluster, involved_nodes, dry)
        raise


def _pod_template_path(cluster):
    k8s = cluster.get("k8s") or {}
    return k8s.get("template") or os.path.join(
        CONFIGS_DIR, "_k8s", "templates", "benchmark-pod.yaml.tpl")


def ensure_pods(cluster, involved_nodes, ts, cfg_name, dry_run):
    """Legacy hook for the SSH path. k8s basic pods are created by
    ensure_k8s_pod (one per config); k8s pipeline pods live in per-role
    Deployments created inside run_pipeline_scenario."""
    return


def _k8s_debug_hints(cluster, involved_nodes, cfg_name):
    """Print copy-pasteable debug commands for kept pods (post-ready fail)."""
    k8s = k8s_settings(cluster)
    ns = k8s["namespace"]
    for name in involved_nodes:
        n = get_node(cluster, name)
        if not is_k8s_node(n) or not n.get("_pod"):
            continue
        pod = n["_pod"]
        view = " ".join(kubectl_prefix(n))
        log("[k8s][DEBUG] scenario {} failed; pod {} kept for troubleshooting"
            "".format(cfg_name, pod))
        log("[k8s][DEBUG]   {} exec -it {} -- bash".format(view, pod))
        log("[k8s][DEBUG]   {} describe pod {}".format(view, pod))
        log("[k8s][DEBUG]   {} exec {} -- bash -c {}".format(
            view, pod, shlex.quote(
                "ls -lt {}/logs | head".format(k8s["pod_workspace"]))))
    log("[k8s][DEBUG] cleanup: STOP / --clean-up deletes namespace {}; "
        "next run auto-reaps it".format(ns))


def cleanup_pods(cluster, involved_nodes, dry_run):
    """Delete the per-config client pods + Deployments + ConfigMap (k8s)."""
    c = cluster.get("_controller")
    if c is None:
        return
    k8s = k8s_settings(cluster)
    ns = k8s["namespace"]
    # pipeline Deployments must be deleted AS OBJECTS (deleting their pods
    # individually just makes the ReplicaSet recreate them)
    for deploy in (cluster.pop("_k8s_deployments", None) or []):
        if dry_run:
            log("[k8s] (dry-run) would delete deployment {} (ns={})"
                "".format(deploy, ns))
            continue
        log("[k8s] delete deployment {} (ns={})".format(deploy, ns))
        controller_run(c, ["-n", ns, "delete", "deployment", deploy,
                           "--ignore-not-found=true"], check=False)
    for name in involved_nodes:
        n = get_node(cluster, name)
        if not is_k8s_node(n):
            continue
        pod = n.get("_pod")
        label = n.get("_run_label")
        if dry_run:
            log("[k8s] (dry-run) would delete pod {} + configmap (run={})"
                "".format(pod, label))
            continue
        if pod:
            log("[k8s] delete pod {} (ns={})".format(pod, ns))
            controller_run(c, ["-n", ns, "delete", "pod", pod,
                               "--ignore-not-found=true"], check=False)
        if label:
            cm = _configmap_name(label)
            log("[k8s] delete configmap {} (ns={})".format(cm, ns))
            controller_run(c, ["-n", ns, "delete", "configmap", cm,
                               "--ignore-not-found=true"], check=False)
        cluster.get("nodes", {}).pop(name, None)

def cleanup_cluster(cluster, globals_cfg, dry_run=False):
    """Remove the remote test directory on every node in cluster.json.
    For k8s: delete namespaces recorded/managed by us, then remove the
    controller-side managed subdirectories (scripts/data/logs/pods)."""
    if cluster.get("_controller") is not None:
        reap_k8s_namespaces(cluster)
        cleanup_k8s_namespace(cluster, dry_run)
        cleanup_k8s_workspace(cluster, dry_run)
        return
    remote_dir = globals_cfg["remote_dir"]
    for node_name in sorted(cluster.get("nodes", {})):
        n = get_node(cluster, node_name)
        log("cleanup {} : {}".format(node_name, remote_dir))
        run_ssh(n["ip"], n, "rm -rf {}".format(remote_dir), dry_run=dry_run)

# --------------------------------------------------------------------------
# metrics aggregation
# --------------------------------------------------------------------------

def parse_benchmark_log(paths, op_filter=None):
    if isinstance(paths, (str, os.PathLike)):
        paths = [paths]
    latencies = []
    lat_sum_ms = 0.0
    ok = fail = 0
    total_bytes = 0
    starts = []
    walls = []
    queue_waits = []
    max_depth = 0
    for path in paths:
        for line in open(path, "r", encoding="utf-8", errors="replace"):
            line = line.rstrip("\n")
            if line.startswith("RESULT\t"):
                parts = line.split("\t")
                # RESULT rows carry a leading wall-clock timestamp
                # (HH:MM:SS.micro) since the plotting feature; legacy rows
                # from older runs start with the op name directly. Detect
                # and offset accordingly — both formats stay parseable.
                off = 1 if len(parts) > 1 and TS_PREFIX.match(parts[1]) else 0
                if len(parts) < 5 + off:
                    continue
                op = parts[1 + off]
                if op_filter is not None and op != op_filter:
                    continue
                # key_count (pipeline): writer 6th field, reader 7th field
                # (after op); basic lines and old logs have none -> 1
                kc = 1
                try:
                    if op == "pipeline_write" and len(parts) > 5 + off:
                        kc = int(parts[5 + off])
                    elif op == "pipeline_read" and len(parts) > 6 + off:
                        kc = int(parts[6 + off])
                except ValueError:
                    pass
                # latency statistics count successful samples only; totals are
                # key-based for pipeline (kc per RESULT line)
                if parts[4 + off] == "OK":
                    ok += kc
                    latencies.append(float(parts[3 + off]))
                    lat_sum_ms += float(parts[3 + off])
                else:
                    fail += kc
                if op == "pipeline_read" and len(parts) > 5 + off:
                    try:
                        queue_waits.append(float(parts[5 + off]))
                    except ValueError:
                        pass
            elif line.startswith("META\t"):
                try:
                    meta = json.loads(line.split("\t", 1)[1])
                except ValueError:
                    continue
                if meta.get("mode") in ("benchmark", "pipeline", "pipeline_reader", "pipeline_writer"):
                    starts.append(float(meta.get("start_ts", 0.0)))
            elif line.startswith("SUMMARY\t"):
                try:
                    s = json.loads(line.split("\t", 1)[1])
                except ValueError:
                    continue
                total_bytes += int(s.get("bytes", 0))
                walls.append(float(s.get("wall_s", 0.0)))
                max_depth = max(max_depth, int(s.get("max_depth", 0)))
    latencies.sort()
    if starts and len(walls) == len(starts):
        ends = [s + w for s, w in zip(starts, walls)]
        wall = max(ends) - min(starts)
    else:
        wall = 0.0
    n = len(latencies)
    total = ok + fail
    return {
        "total": total,
        "total_bytes": total_bytes,
        "wall_s": wall,
        "throughput_mb_s": (total_bytes / wall / 1024.0 ** 2) if wall > 0 else 0.0,
        "throughput_gb_s": (total_bytes / wall / 1024.0 ** 3) if wall > 0 else 0.0,
        "avg_ms": (sum(latencies) / n) if n else 0.0,
        "min_ms": percentile(latencies, 0.0),
        "max_ms": percentile(latencies, 1.0),
        "p99_ms": percentile(latencies, 0.99),
        "p999_ms": percentile(latencies, 0.999),
        "p9999_ms": percentile(latencies, 0.9999),
        "lat_sum_ms": lat_sum_ms,
        "ok": ok,
        "fail": fail,
        "qps": (total / wall) if wall > 0 else 0.0,
        "fail_rate": (fail / total) if total else 0.0,
        "queue_wait_avg_ms": (sum(queue_waits) / len(queue_waits)) if queue_waits else 0.0,
        "queue_wait_max_ms": max(queue_waits) if queue_waits else 0.0,
        "max_depth": max_depth,
    }


def write_scenario_csv(local_log_dir, cfg, ts, cfg_path, cluster=None,
                       mooncake_store_nodes=None):
    """拓扑判定（IP 匹配）：节点的 cluster.json `ip` 出现在
    mooncake_store_nodes 各节点的 `ip` 集合中即 local_node。"""
    store_ips = resolve_store_node_ips(
        cluster or {}, mooncake_store_nodes or [])

    def topo_of(nodename):
        if not nodename or cluster is None:
            return ""
        try:
            nip = get_node(cluster, nodename)["ip"]
        except (KeyError, RuntimeError):
            return "remote_node"
        return "local_node" if nip in store_ips else "remote_node"

    name = os.path.splitext(os.path.basename(cfg_path))[0]
    out_csv = os.path.join(RESULTS_DIR, ts, "{}.csv".format(name))
    rows = []
    is_pipeline = cfg.get("scenario") == "pipeline"
    if is_pipeline:
        plan = pipeline_plan(cfg)
        batch_size = int(cfg.get("batch_size", 1))
        duration = float(cfg.get("duration", 0) or 0)

        def base_row(role, instance, nodename, ds):
            if role == "writer":
                op = (cfg.get("writer", {}) or {}).get("operation", "")
            else:
                op = (cfg.get("reader", {}) or {}).get("operation", "")
            w_cpn = (cfg.get("writer", {}) or {}).get("count_per_node", 1)
            r_cpn = (cfg.get("reader", {}) or {}).get("count_per_node", 1)
            # node 列统一填管理网 IP（解析失败回退节点名）
            try:
                node_ip = get_node(cluster, nodename)["ip"] if cluster else nodename
            except (KeyError, RuntimeError):
                node_ip = nodename
            return {
                "config_name": name,
                "scenario": cfg["scenario"],
                "work_mode": cfg.get("work_mode", cfg.get("mode", "")),
                "operation": op,
                "role": role,
                "node": node_ip,
                "instance": instance,
                "topology": topo_of(nodename) if nodename else "",
                "protocol": cfg["protocol"],
                "num_threads": plan["reader_threads"] if role == "reader" else "",
                "notify_threads": plan["notify_threads"] if role == "writer" else "",
                "data_size": ds,
                "batch_size": cfg.get("batch_size", ""),
                # effective per-writer cap: auto-computed value when
                # store_total_size drove the budget, else the config value
                "key_count": (cfg.get("_key_space", {}).get(str(ds))
                              or cfg.get("key_count", "") or ""),
                "duration": cfg.get("duration", ""),
                "warmup_duration": cfg.get("warmup", {}).get("duration", ""),
                "writer_count": "N*{}".format(w_cpn),
                "reader_count": "N*{}".format(r_cpn),
                "notify_count": plan["notify_count"],
                # store count is RECORD-ONLY metadata (stores are deployed
                # externally and discovered via the master)
                "store_count": ("N*{}".format(cfg.get("store_count_per_node"))
                                if cfg.get("store_count_per_node") else ""),
                "base_qps": cfg.get("base_qps", ""),
                "qps_multiple": cfg.get("qps_multiple", ""),
                "target_qps": plan["total_qps"],
                "notify_pipeline": ",".join(cfg.get("notify_pipeline", [])),
                "traffic_mode": cfg.get("traffic_mode", ""),
            }

        def busy_throughput_mb(files, op_filter, bytes_per_op):
            """Sum of per-instance busy-time throughput (successful bytes /
            sum of successful interface call durations), 1024-based. parse
            already returns key-based counts."""
            total = 0.0
            for f in files:
                a = parse_benchmark_log(f, op_filter=op_filter)
                busy_s = a["lat_sum_ms"] / 1000.0
                if busy_s > 0:
                    total += (a["ok"] * bytes_per_op) / busy_s / 1024.0 ** 2
            return total

        def metrics_into(row, agg, bytes_per_op, with_queue=False, throughput_mb=None):
            # pipeline counts are key-based: RESULT lines carry key_count so
            # parse returns total/ok/fail already in keys
            total = agg["total"]
            ok = agg["ok"]
            fail = agg["fail"]
            wall = duration if duration > 0 else agg["wall_s"]
            row["total"] = total
            row["success"] = ok
            row["fail"] = fail
            row["fail_rate"] = agg["fail_rate"]
            # bytes/throughput count successful operations only
            row["total_bytes"] = int(ok * bytes_per_op)
            row["wall_s"] = wall
            row["qps"] = (total / wall) if wall > 0 else 0.0
            if throughput_mb is None:
                # interface throughput = successful bytes / sum of successful
                # interface call durations (busy time, not wall clock)
                busy_s = agg["lat_sum_ms"] / 1000.0
                throughput_mb = (row["total_bytes"] / busy_s / 1024.0 ** 2) if busy_s > 0 else 0.0
            row["throughput_mb_s"] = throughput_mb
            row["throughput_gb_s"] = throughput_mb / 1024.0
            for k in ("avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms", "p9999_ms"):
                row[k] = agg[k]
            if with_queue:
                row["queue_wait_avg_ms"] = agg["queue_wait_avg_ms"]
                row["queue_wait_max_ms"] = agg["queue_wait_max_ms"]
                row["max_depth"] = agg["max_depth"]
            for k in ("avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms",
                      "queue_wait_avg_ms", "queue_wait_max_ms", "wall_s"):
                v = row.get(k)
                if isinstance(v, float):
                    row[k] = round(v, 4)
            for k in ("throughput_mb_s", "throughput_gb_s",
                      "qps", "fail_rate"):
                v = row.get(k)
                if isinstance(v, float):
                    row[k] = round(v, 2)

        # one block of rows per data_size, in config order: writer rows,
        # writer TOTAL, reader rows, reader TOTAL
        for ds in data_size_list(cfg):
            ds_bytes = parse_data_size(ds)
            run_name = "benchmark_c1_t{}_{}".format(plan["reader_threads"], ds)
            # log names carry the node segment (per-node port numbering is
            # no longer globally unique): *.writer_<node>_w<gid>.log and
            # *.reader_<node>_r<index>_p<port>.log — node, gid, index and
            # port are parsed straight from the filename
            writer_files = {}   # gid -> (nodename, path)
            for p in sorted(glob.glob(os.path.join(
                    local_log_dir, "{}.{}.writer_*.log".format(name, run_name)))):
                base = os.path.basename(p)
                if ".ylt." in base:
                    continue
                m = re.search(r"\.writer_(.+)_w(\d+)\.log$", base)
                if not m:
                    continue
                writer_files[int(m.group(2))] = (m.group(1), p)
            reader_files = {}   # index -> (nodename, path)
            for p in sorted(glob.glob(os.path.join(
                    local_log_dir, "{}.{}.reader_*.log".format(name, run_name)))):
                base = os.path.basename(p)
                if ".ylt." in base:
                    continue
                m = re.search(r"\.reader_(.+)_r(\d+)_p(\d+)\.log$", base)
                if not m:
                    continue
                reader_files[int(m.group(2))] = (m.group(1), p)
            # dual-role logs: ONE file per client carries BOTH the writer
            # and the reader rows (role split by op filter)
            dual_files = {}   # id -> (nodename, path)
            for p in sorted(glob.glob(os.path.join(
                    local_log_dir, "{}.{}.dual_*.log".format(name, run_name)))):
                base = os.path.basename(p)
                if ".ylt." in base:
                    continue
                m = re.search(r"\.dual_(.+)_c(\d+)_p(\d+)\.log$", base)
                if not m:
                    continue
                dual_files[int(m.group(2))] = (m.group(1), p)
            for gid in sorted(writer_files):
                nodename, f = writer_files[gid]
                agg = parse_benchmark_log(f, op_filter="pipeline_write")
                row = base_row("writer", "w{}".format(gid), nodename, ds)
                metrics_into(row, agg, ds_bytes)
                rows.append(row)
            # writer TOTAL row: all writers aggregated; throughput = sum of each
            # writer's busy-time throughput
            if writer_files:
                agg = parse_benchmark_log(
                    [f for _n, f in writer_files.values()], op_filter="pipeline_write")
                row = base_row("writer", "TOTAL", "", ds)
                metrics_into(row, agg, ds_bytes,
                             throughput_mb=busy_throughput_mb(
                                 (f for _n, f in writer_files.values()),
                                 "pipeline_write", ds_bytes))
                rows.append(row)
            for idx in sorted(reader_files):
                nodename, f = reader_files[idx]
                agg = parse_benchmark_log(f, op_filter="pipeline_read")
                row = base_row("reader", "r{}".format(idx), nodename, ds)
                metrics_into(row, agg, ds_bytes, with_queue=True)
                rows.append(row)
            # TOTAL row: all readers aggregated (aligns with the datasystem
            # single GET row per test group); throughput = sum of each reader's
            # busy-time throughput
            if reader_files:
                agg = parse_benchmark_log(
                    [f for _n, f in reader_files.values()], op_filter="pipeline_read")
                row = base_row("reader", "TOTAL", "", ds)
                metrics_into(row, agg, ds_bytes, with_queue=True,
                             throughput_mb=busy_throughput_mb(
                                 (f for _n, f in reader_files.values()),
                                 "pipeline_read", ds_bytes))
                rows.append(row)
            # dual-role rows: writer metrics + reader metrics per client log
            for cid in sorted(dual_files):
                nodename, f = dual_files[cid]
                agg = parse_benchmark_log(f, op_filter="pipeline_write")
                row = base_row("writer", "c{}".format(cid), nodename, ds)
                metrics_into(row, agg, ds_bytes)
                rows.append(row)
                agg = parse_benchmark_log(f, op_filter="pipeline_read")
                row = base_row("reader", "c{}".format(cid), nodename, ds)
                metrics_into(row, agg, ds_bytes, with_queue=True)
                rows.append(row)
            if dual_files:
                agg = parse_benchmark_log(
                    [f for _n, f in dual_files.values()],
                    op_filter="pipeline_write")
                row = base_row("writer", "TOTAL", "", ds)
                metrics_into(row, agg, ds_bytes,
                             throughput_mb=busy_throughput_mb(
                                 (f for _n, f in dual_files.values()),
                                 "pipeline_write", ds_bytes))
                rows.append(row)
                agg = parse_benchmark_log(
                    [f for _n, f in dual_files.values()],
                    op_filter="pipeline_read")
                row = base_row("reader", "TOTAL", "", ds)
                metrics_into(row, agg, ds_bytes, with_queue=True,
                             throughput_mb=busy_throughput_mb(
                                 (f for _n, f in dual_files.values()),
                                 "pipeline_read", ds_bytes))
                rows.append(row)
    else:
        def fill_basic_row(row, agg, op_label):
            row["operation"] = op_label
            row.update({k: agg[k] for k in (
                "total", "total_bytes", "wall_s", "throughput_mb_s", "throughput_gb_s",
                "avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms", "p9999_ms",
                "ok", "fail", "qps", "fail_rate")})
            # throughput recomputed from this row's own bytes/wall so split
            # (mixed) rows never inherit combined SUMMARY bytes
            row["throughput_mb_s"] = (row["total_bytes"] / row["wall_s"] / 1024.0 ** 2) if row["wall_s"] > 0 else 0.0
            row["throughput_gb_s"] = (row["total_bytes"] / row["wall_s"] / 1024.0 ** 3) if row["wall_s"] > 0 else 0.0
            for k in ("avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms", "p9999_ms", "wall_s"):
                v = row.get(k)
                if v is not None and isinstance(v, float):
                    row[k] = round(v, 4)
            for k in ("throughput_mb_s", "throughput_gb_s",
                      "qps", "fail_rate", "total_bytes"):
                v = row.get(k)
                if v is not None and isinstance(v, float):
                    row[k] = round(v, 2)

        op_label = cfg.get("operation", "")
        batch_size = max(1, int(cfg.get("batch_size", 1)))
        put_iface = get_iface = None
        if "/" in op_label:
            put_iface, get_iface = op_label.split("/", 1)
        is_mixed = bool(put_iface and get_iface)
        primary = cfg.get("_primary_node") or cfg["client_node"]
        topo = topo_of(primary)
        node_ip = get_node(cluster, primary)["ip"] if cluster else ""
        warmup_duration = cfg.get("warmup", {}).get("duration", 0)
        # rows follow run order: outer data_size, inner params order —
        # identical to how run_basic_scenario executes the runs
        for c, t, ds in build_runs(cfg):
            path = os.path.join(local_log_dir, "{}.benchmark_c{}_t{}_{}.log".format(name, c, t, ds))
            if not os.path.exists(path):
                continue
            row_data_size_bytes = parse_data_size(ds)
            base_row_vals = {
                "scenario": cfg["scenario"],
                "work_mode": cfg.get("work_mode", cfg.get("mode", "")),
                "operation": op_label,
                "node": node_ip,
                "topology": topo,
                "protocol": cfg["protocol"],
                "num_clients": c,
                "num_threads": t,
                "data_size": ds,
                "key_count": cfg.get("key_count", ""),
                "batch_size": cfg.get("batch_size", "") if (op_label in ("put_batch", "get_batch") or
                                                            put_iface in ("put_batch",) or
                                                            get_iface in ("get_batch",)) else "",
                "put_ratio": cfg.get("put_ratio", "") if "/" in op_label else "",
                "duration": cfg.get("duration", ""),
                "warmup_duration": warmup_duration,
                "target_qps": cfg.get("target_qps") or "",
            }
            if is_mixed:
                # mixed must be split: put and get latencies are statistically
                # independent and are output as separate rows labelled with the
                # actual interface used on each side
                subs = ((put_iface, "put"), (get_iface, "get"))
                for iface, role in subs:
                    agg = parse_benchmark_log(path, op_filter=role)
                    row = dict(base_row_vals)
                    # parse bytes come from the combined SUMMARY; recompute per
                    # side counting successful ops with the side's interface
                    bp = batch_size if iface in ("put_batch", "get_batch") else 1
                    agg = dict(agg, total_bytes=int(agg["ok"] * bp * row_data_size_bytes))
                    fill_basic_row(row, agg, iface)
                    rows.append(row)
            else:
                agg = parse_benchmark_log(path)
                row = dict(base_row_vals)
                fill_basic_row(row, agg, op_label)
                rows.append(row)
    if is_pipeline:
        write_rows = collapse_config_name(rows)
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=PIPELINE_CSV_HEADER)
            writer.writeheader()
            for row in write_rows:
                writer.writerow(row)
    else:
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=BASIC_CSV_HEADER)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
    return rows


# --------------------------------------------------------------------------
# CLI entry
# --------------------------------------------------------------------------

def _deep_merge(base, overlay):
    """Recursively merge overlay into base: dict values merge key-by-key (so
    a child _configs.json can extend a parent's writer/reader skeleton
    without repeating every field), any other type replaces outright."""
    for k, v in overlay.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _deep_merge(base[k], v)
        else:
            base[k] = v
    return base


def load_scenario_config(cfg_path):
    """Load a scenario config with layered _configs.json inheritance.

    Configs under configs/<category>/ inherit shared defaults from every
    _configs.json on the directory chain from the category root down to the
    config's own directory, e.g. for 2-nodes/rank/x.json:
      configs/pipeline/_configs.json
      -> configs/pipeline/2-nodes/_configs.json
      -> configs/pipeline/2-nodes/rank/_configs.json
      -> the config file itself
    Each level supplements/overrides the level above (dicts deep-merge,
    other values replace); the config file is merged last and always wins.
    External config paths (outside configs/) get no inheritance. The
    fragments themselves are not runnable scenarios and are rejected if
    passed directly."""
    cfg = load_json(cfg_path)
    if os.path.basename(cfg_path).startswith("_"):
        raise RuntimeError(
            "{} is a shared-defaults fragment, not a runnable scenario".format(cfg_path))
    cfg_dir = os.path.normpath(os.path.dirname(os.path.abspath(cfg_path)))
    merged = {}
    for category in ("basic", "pipeline"):
        category_dir = os.path.normpath(os.path.join(CONFIGS_DIR, category))
        if cfg_dir == category_dir or cfg_dir.startswith(category_dir + os.sep):
            # directory chain: category root -> ... -> the config's own dir
            chain = [category_dir]
            rel = os.path.relpath(cfg_dir, category_dir)
            if rel != ".":
                acc = category_dir
                for part in rel.split(os.sep):
                    acc = os.path.join(acc, part)
                    chain.append(acc)
            for level in chain:
                frag = os.path.join(level, "_configs.json")
                if os.path.isfile(frag):
                    _deep_merge(merged, load_json(frag))
            break
    _deep_merge(merged, cfg)
    # pipeline roles are cluster-oriented now (count_per_node x N on every
    # cluster node); the legacy per-role node lists are ignored with a WARN
    for role in ("writer", "reader"):
        blk = merged.get(role)
        if isinstance(blk, dict) and "client_nodes" in blk:
            log("[WARN] pipeline {}.client_nodes is deprecated and ignored "
                "(cluster-oriented: count_per_node x N on every cluster node); "
                "remove the field from the config".format(role))
            blk.pop("client_nodes", None)
    return merged


def collect_config_files(directory):
    """Collect scenario config files under directory, RECURSIVELY (demo/ is a
    plain directory and is included like any other; "_"-prefixed files are
    skipped at every level — they are shared-defaults inheritance fragments,
    not runnable scenarios)."""
    found = []
    for root, dirs, files in os.walk(directory):
        # "_"-prefixed directories are deployment/tuning config dirs
        # (_ssh/_k8s/_spdiag_trees), never scenario collections
        dirs[:] = sorted(d for d in dirs if not d.startswith("_"))
        for f in sorted(files):
            if f.endswith(".json") and not f.startswith("_"):
                found.append(os.path.join(root, f))
    return found


def parse_arguments(argv=None):
    """Parse CLI args, validate combinations, and collect config paths.

    Returns (args, cfg_paths).
    """
    parser = argparse.ArgumentParser(description="Mooncake benchmark orchestrator (runs locally)")
    parser.add_argument("--ssh", action="store_true",
                        help="run against bare-metal nodes via ssh (default when "
                             "neither --ssh nor --k8s is given; deployment configs "
                             "default to configs/_ssh/)")
    parser.add_argument("--k8s", action="store_true",
                        help="run against k8s pods (kubectl transport; deployment "
                             "configs default to configs/_k8s/). basic: one pod "
                             "per config; pipeline: per-role Deployments "
                             "(pod = process, spread across all Ready nodes)")
    parser.add_argument("--cluster", default=None,
                        help="path to cluster json (default: configs/_ssh/cluster.json "
                             "or configs/_k8s/cluster.json per mode)")
    parser.add_argument("--global", dest="global_cfg", default=None,
                        help="path to global hyper json (default: configs/_ssh/global.json "
                             "or configs/_k8s/global.json per mode)")
    parser.add_argument("--profile", default=None,
                        help="profile json (default: <mode-dir>/profile.json; "
                             "must exist together with cluster/global)")
    parser.add_argument("--config", "-f", default=None,
                        help="scenario config json(s), comma-separated multi values; "
                             "resolved as given (relative to CWD or absolute; NOT "
                             "relative to configs/), e.g. "
                             "configs/pipeline/demo/simple_recall.json or /path/to/cfg.json")
    parser.add_argument("--config-dir", default=None,
                        help="run every scenario config under this directory, "
                             "RECURSIVELY (e.g. configs/basic runs everything under it "
                             "including demo/; _-prefixed fragments always skipped); "
                             "comma-separated multi values; config-dir entries run "
                             "BEFORE --config files")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="verbose logging (upload paths, per-file pulls, "
                             "kubectl commands, pod details)")
    parser.add_argument("--stop-file", default=None,
                        help="cooperative stop flag file (WebUI stop); a "
                             "SIGINT/SIGTERM/SIGBREAK also requests stop")
    parser.add_argument("--no-warmup", action="store_true")
    parser.add_argument("--no-plot", action="store_true",
                        help="skip the automatic latency/queue-wait plotting of "
                             "this run's logs (default: plotted after the sweep)")
    parser.add_argument("--protocol", default=None, choices=["tcp", "rdma", "ub"],
                        help="override protocol (tcp/rdma/ub); highest priority, "
                             "overrides the JSON config value")
    parser.add_argument("--clean-up", action="store_true",
                        help="remove the remote test directory on every node in "
                             "cluster.json. Standalone: only cleanup and exit. "
                             "Combined with --config/--config-dir: cleanup after the run.")
    args = parser.parse_args(argv)
    if args.ssh and args.k8s:
        sys.exit("[ERROR] --ssh and --k8s are mutually exclusive")
    # deployment config dir per mode (default --ssh for backward compat);
    # explicit --cluster/--global/--profile always win over the mode defaults
    args.mode = "k8s" if args.k8s else "ssh"
    args.mode_dir = os.path.join(CONFIGS_DIR, "_k8s" if args.k8s else "_ssh")
    if args.cluster is None:
        args.cluster = os.path.join(args.mode_dir, "cluster.json")
    if args.global_cfg is None:
        args.global_cfg = os.path.join(args.mode_dir, "global.json")
    standalone_cleanup = bool(
        args.clean_up and not (args.config or args.config_dir))
    if args.profile is None and not standalone_cleanup:
        args.profile = os.path.join(args.mode_dir, "profile.json")
    required = [("cluster", args.cluster), ("global", args.global_cfg)]
    if args.profile is not None or not standalone_cleanup:
        required.append(("profile", args.profile))
    for name, path in required:
        if not os.path.exists(path):
            example = os.path.join(args.mode_dir, name + ".example.json")
            target = os.path.join(args.mode_dir, name + ".json")
            sys.exit("[ERROR] {} config not found: {}\n"
                     "       copy {} to {} and edit it first.".format(
                         name, path, example, target))
    # selectors accept comma-separated multi values and MAY coexist:
    # --config-dir entries are expanded (recursively) first, --config files
    # follow in the given order
    cfg_paths = []
    for d in [x.strip() for x in (args.config_dir or "").split(",") if x.strip()]:
        # --config-dir always scans recursively; "_"-prefixed shared-defaults
        # fragments are the only exclusion (demo/ is a plain directory and
        # is included like any other)
        if not os.path.isdir(d):
            sys.exit("[ERROR] config dir not found: {}".format(d))
        found = collect_config_files(d)
        if not found:
            sys.exit("[ERROR] no config json found under {}".format(d))
        cfg_paths.extend(found)
    for p in [x.strip() for x in (args.config or "").split(",") if x.strip()]:
        # --config values are resolved verbatim (relative to CWD or absolute);
        # they are NOT joined with CONFIGS_DIR, so external paths work
        if not os.path.exists(p):
            sys.exit("[ERROR] config not found: {}".format(p))
        cfg_paths.append(p)
    if not cfg_paths and not args.clean_up:
        sys.exit("[ERROR] provide --config and/or --config-dir (comma-separated "
                 "multi values allowed), or --clean-up for a standalone cluster cleanup")
    return args, cfg_paths


def _profile_outliers(matrix, ts_dir, cfg_name):
    """Auto outlier report for one finished config: extract RESULT rows above
    the matrix threshold, append to <ts>/profiling/outliers.json and (when
    profiling artifacts were collected) write time-window slices around the
    top outliers into outliers.slices.txt."""
    out_cfg = matrix.get("outliers", {})
    threshold = float(out_cfg.get("threshold_ms", 1.0))
    window_ms = float(out_cfg.get("window_ms", 200))
    logs_dir = os.path.join(ts_dir, "logs", cfg_name)
    outliers = prof_core.extract_outliers(logs_dir, threshold)
    # run-level ledger lives at <ts>/profiling/; the artifacts it slices are
    # per-config at <ts>/logs/<cfg>/profiling/ (multi-config isolation)
    prof_dir = os.path.join(ts_dir, "profiling")
    slice_dir = os.path.join(ts_dir, "logs", cfg_name, "profiling")
    os.makedirs(prof_dir, exist_ok=True)
    # append per-config entries to the run-level outlier ledger
    ledger = os.path.join(prof_dir, "outliers.json")
    entries = []
    if os.path.isfile(ledger):
        try:
            entries = json.load(open(ledger, "r", encoding="utf-8"))
        except ValueError:
            entries = []
    entries.append({"cfg": cfg_name, "threshold_ms": threshold,
                    "count": len(outliers),
                    "outliers": [{k: o[k] for k in ("ts", "key", "lat_ms", "status", "file")}
                                 for o in outliers]})
    with open(ledger, "w", encoding="utf-8") as f:
        json.dump(entries, f, ensure_ascii=False, indent=2)
    log("[profile] outliers(>{:g}ms) for {}: {}".format(threshold, cfg_name, len(outliers)))
    # time-window slices around the top outliers (text artifacts only;
    # perf.data precise slicing is done node-side with perf script --time)
    if outliers and os.path.isdir(slice_dir):
        lines = []
        for o in outliers[:10]:
            lines.append("===== {} cfg={} lat={:.3f}ms key={} =====".format(
                o["ts"], cfg_name, o["lat_ms"], o["key"]))
            slices = prof_core.slice_window(slice_dir, o["seconds"], window_ms)
            for fname, slines in slices.items():
                lines.append("  --- {} ({} lines) ---".format(fname, len(slines)))
                lines.extend("    " + l for l in slines[:20])
            lines.append("  hint: perf script -i perf.data --time <start>,<end>")
        with open(os.path.join(prof_dir, "outliers.slices.txt"), "a", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")


def main(args, cfg_paths):
    stop_file = args.stop_file or os.environ.get("BENCH_STOP_FILE")
    if stop_file:
        args.stop_file = stop_file
        set_stop_file(stop_file)
    set_verbose(getattr(args, "verbose", False)
                or os.environ.get("MC_BENCH_VERBOSE", "") == "1")
    cluster = load_json(args.cluster)
    if args.mode == "k8s":
        cluster["_controller"] = build_controller(cluster)
        k8s_settings(cluster)      # fail fast on missing ns/image/workspaces
        try:
            info = controller_diagnose(cluster["_controller"])
            log("[k8s] controller info: " + " | ".join(info.splitlines()))
        except Exception as exc:
            log("[WARN] k8s controller info: {}".format(exc))
    enrich_k8s_nodes(cluster)
    globals_cfg = load_json(args.global_cfg)
    # pipeline rendezvous: wipe stale mcbench/ keys from crashed runs (the
    # run prefix is also wiped per scenario/size; this covers the rest)
    try:
        _pipeline_wipe_etcd(cluster, globals_cfg)
    except Exception as exc:
        log("[WARN] etcd stale-key wipe failed: {}".format(exc))
    if args.mode == "k8s":
        globals_cfg["_pod_workspace"] = k8s_settings(cluster)["pod_workspace"]
    # startup summary (mode, config paths, k8s settings)
    log("mode={} dry_run={} cluster={} global={} profile={}".format(
        args.mode, args.dry_run, os.path.relpath(args.cluster, ROOT),
        os.path.relpath(args.global_cfg, ROOT),
        os.path.relpath(args.profile, ROOT) if args.profile else "-"))
    if args.mode == "k8s":
        _ks = k8s_settings(cluster)
        log("[k8s] settings: namespace={} template={}".format(
            _ks["namespace"], os.path.relpath(_pod_template_path(cluster), ROOT)))
        log("[k8s] image: {}".format(_ks["image"]))
        log("[k8s] workspaces: controller={} pod={}".format(
            _ks["controller_workspace"], _ks["pod_workspace"]))
    _validate_master_mode(globals_cfg)
    _STOP_CTX.update(cluster=cluster, globals_cfg=globals_cfg, args=args)
    _check_stop()
    # profile config: parse_arguments already defaulted it to
    # <mode-dir>/profile.json and required it to exist.
    profile_path = args.profile
    matrix = None
    if prof_core is not None:
        matrix = prof_core.load_matrix(profile_path)
    args._matrix = matrix
    if matrix:
        log("profiling profile active: {} (trigger={})".format(
            profile_path, matrix.get("trigger")))
    # .state ledger check: reap any leftovers from previous runs
    # (crashed/killed orchestrator) BEFORE anything else touches nodes.
    if not args.dry_run:
        if args.mode == "k8s":
            try:
                reap_k8s_namespaces(cluster)
            except Exception as exc:
                log("[WARN] .state k8s namespace reap failed: {}".format(exc))
        try:
            reap_state_entries(cluster)
        except Exception as exc:
            log("[WARN] .state reap failed: {}".format(exc))
    if not cfg_paths:
        # standalone cleanup mode
        cleanup_cluster(cluster, globals_cfg, dry_run=args.dry_run)
        log("cleanup done")
        return
    ts = time.strftime("%Y%m%d-%H%M")
    ts_dir = os.path.join(RESULTS_DIR, ts)
    n = 1
    while os.path.exists(ts_dir):
        ts = "{}-{}".format(time.strftime("%Y%m%d-%H%M"), n)
        ts_dir = os.path.join(RESULTS_DIR, ts)
        n += 1
    if not args.dry_run:
        os.makedirs(os.path.join(ts_dir, "logs"), exist_ok=True)
        log("results root: {}".format(ts_dir))
    if args.mode == "k8s":
        k8s_workspace_dirs(cluster, dry_run=args.dry_run)
        k8s_prerun(cluster, dry_run=args.dry_run)
        ensure_k8s_namespace(cluster, ts, dry_run=args.dry_run)
        if not args.dry_run:
            def _ns_atexit():
                try:
                    if cluster.get("_k8s_keep_ns"):
                        log("[k8s] namespace {} kept for troubleshooting "
                            "(STOP / --clean-up deletes it; next run reaps it)"
                            "".format(k8s_settings(cluster)["namespace"]))
                        return
                    cleanup_k8s_namespace(cluster, dry_run=False)
                except Exception as exc:
                    log("[WARN] k8s namespace cleanup at exit: {}".format(exc))
            atexit.register(_ns_atexit)
    all_rows = []
    categories = set()
    failed_scenarios = []
    # coordinates of the last successful config — the sweep-end remove-all
    # runs on its node (failed configs skip their own end-remove, so the
    # sweep-end pass is what guarantees the store gets emptied)
    sweep_remove = None
    for cfg_path in cfg_paths:
        _check_stop()
        cfg = load_scenario_config(cfg_path)
        cfg["__cfg_path__"] = cfg_path
        name = os.path.splitext(os.path.basename(cfg_path))[0]
        try:
            if cfg.get("scenario") == "pipeline":
                sweep_remove = run_pipeline_scenario(cluster, globals_cfg, cfg, ts, args)
            else:
                sweep_remove = run_basic_scenario(cluster, globals_cfg, cfg, ts, args)
            if not args.dry_run:
                store_nodes = (globals_cfg.get("mooncake_store_nodes", []) or []) \
                    if args.mode != "k8s" else []
                rows = write_scenario_csv(os.path.join(ts_dir, "logs", name), cfg, ts, cfg_path,
                                          cluster, store_nodes)
                if cfg.get("scenario") == "pipeline":
                    categories.add("pipeline")
                    # detail keeps only the aggregated writer/reader TOTAL rows
                    all_rows.extend([r for r in rows if r.get("instance") == "TOTAL"])
                else:
                    categories.add("basic")
                    all_rows.extend(rows)
                # profiling outliers: auto-extract RESULT rows above the
                # matrix threshold and write the per-run outlier report
                if matrix and matrix.get("outliers", {}).get("auto") and prof_core:
                    try:
                        _profile_outliers(matrix, ts_dir, name)
                    except Exception as exc:
                        log("[WARN] [profile] outlier extraction: {}".format(exc))
        except Exception as exc:
            log("[FAIL] scenario {}: {}".format(name, exc))
            failed_scenarios.append(name)
    if args.dry_run:
        log("dry-run complete, no artifacts written")
        if failed_scenarios:
            sys.exit(1)
        return
    # Sweep end: remove-all on the last successful config's primary node.
    # Failed configs never reach their own end-remove, so this final pass is
    # what guarantees the store ends the sweep empty. Removing from an empty
    # store is a no-op (safe), and failure here must not fail the whole sweep
    # — hence WARN-only. The log is scp'd out because it does not match the
    # per-config glob used by pull_logs.
    if sweep_remove:
        try:
            nd = get_node(cluster, sweep_remove["node"])
            run_stage(sweep_remove["host"], nd, sweep_remove["runner"], "remove-keys",
                      sweep_remove["remote_log_dir"], "final",
                      extra="--scope full", dry_run=False,
                      cfg_name="final", node_name=sweep_remove["node"])
            scp_pull(sweep_remove["host"], nd,
                     "{}/final.remove-keys.log".format(sweep_remove["remote_log_dir"]),
                     os.path.join(ts_dir, "logs"), dry_run=False)
            log("sweep-end remove-keys -> {}".format(
                os.path.join(ts_dir, "logs", "final.remove-keys.log")))
        except Exception as exc:
            log("[WARN] sweep-end remove-keys failed: {}".format(exc))
        finally:
            # k8s: the sweep-end needed the last config's pods alive —
            # delete them only now (store-side keys are already cleared)
            try:
                cleanup_pods(cluster, [sweep_remove["node"]], False)
            except Exception as exc:
                log("[WARN] k8s pod cleanup after sweep-end: {}".format(exc))
    if categories == {"pipeline"}:
        detail_header = PIPELINE_CSV_HEADER
        detail_rows = collapse_config_name(all_rows)
    elif categories == {"basic"}:
        detail_header = BASIC_CSV_HEADER
        detail_rows = all_rows
    else:
        # mixed (not reachable through normal CLI): ordered union of both headers
        detail_header = [c for c in BASIC_CSV_HEADER] + \
                        [c for c in PIPELINE_CSV_HEADER if c not in BASIC_CSV_HEADER]
        detail_rows = all_rows
    detail_csv = os.path.join(ts_dir, "benchmark_detail.csv")
    with open(detail_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=detail_header)
        writer.writeheader()
        for row in detail_rows:
            writer.writerow(row)
    log("detail -> {}".format(detail_csv))
    if failed_scenarios:
        log("SUMMARY: {} scenarios OK, {} FAILED: {}".format(
            len(cfg_paths) - len(failed_scenarios), len(failed_scenarios),
            ", ".join(failed_scenarios)))
    else:
        log("SUMMARY: all {} scenarios OK".format(len(cfg_paths)))
    if args.mode == "k8s" and not args.dry_run:
        if cluster.get("_k8s_keep_ns"):
            log("[k8s] namespace {} kept for troubleshooting; teardown "
                "deferred until STOP/Ctrl+C".format(
                    k8s_settings(cluster)["namespace"]))
        else:
            cleanup_k8s_namespace(cluster, dry_run=False)
    log("done. results in {}".format(ts_dir))
    # auto-plot runs on the ORCHESTRATOR (local sys.executable, local logs dir);
    # it must never run on remote nodes. Skip when a failed run produced no logs.
    if not args.no_plot:
        log_dir = os.path.join(ts_dir, "logs")
        has_logs = bool(glob.glob(os.path.join(log_dir, "**", "*.log"),
                                  recursive=True))
        if not has_logs:
            log("[WARN] auto-plot skipped (no *.log under {})".format(log_dir))
        else:
            plot_script = os.path.join(ROOT, "scripts", "plot_latency.py")
            try:
                proc = subprocess.run(
                    [sys.executable, plot_script, "--log", log_dir],
                    check=False)
                if proc.returncode != 0:
                    log("[WARN] auto-plot failed (rc={}); figures skipped. "
                        "Is matplotlib installed? (pip install matplotlib)".format(
                            proc.returncode))
            except Exception as exc:
                log("[WARN] auto-plot failed: {}".format(exc))
    if args.clean_up:
        # reap first: recorded processes must be killed before their
        # scripts/binaries vanish with the remote directory
        try:
            reap_state_entries(cluster)
        except Exception as exc:
            log("[WARN] .state reap failed: {}".format(exc))
        cleanup_cluster(cluster, globals_cfg, dry_run=False)
        log("cleanup done")
    if (args.mode == "k8s" and not args.dry_run
            and cluster.get("_k8s_keep_ns")
            and not getattr(args, "clean_up", False)):
        _k8s_hold_for_debug(cluster)
        raise StopRequested()
    if failed_scenarios:
        # non-zero exit so runmgr/WebUI marks the run "failed"
        sys.exit(1)


def _k8s_hold_for_debug(cluster):
    """Block until STOP/Ctrl+C so the user can inspect kept pods."""
    ns = k8s_settings(cluster)["namespace"]
    log("[k8s][DEBUG] holding run for troubleshooting (namespace {}); "
        "press STOP (WebUI) or Ctrl+C to tear down".format(ns))
    last = time.time()
    while not stop_requested():
        time.sleep(1.0)
        if time.time() - last >= 30:
            last = time.time()
            log("[k8s][DEBUG] still holding namespace {} (STOP to tear down)"
                "".format(ns))


def cleanup_on_stop():
    """Cooperative stop cleanup: k8s delete namespace/pods; SSH reap remote
    client/stage/profiling processes. Best-effort; never raises."""
    cluster = _STOP_CTX.get("cluster")
    globals_cfg = _STOP_CTX.get("globals_cfg")
    args = _STOP_CTX.get("args")
    if cluster is None:
        log("[STOP] no cluster context; nothing to clean")
        clear_stop()
        return
    # allow cleanup SSH/kubectl commands to run (stop file/event would abort them)
    clear_stop()
    log("[STOP] cleanup phase started")
    try:
        if cluster.get("_controller") is not None:
            try:
                cleanup_k8s_namespace(cluster, dry_run=False)
                cluster.pop("_k8s_keep_ns", None)
            except Exception as exc:
                log("[WARN] [STOP] delete namespace: {}".format(exc))
            try:
                _pipeline_wipe_etcd(cluster, globals_cfg)
            except Exception as exc:
                log("[WARN] [STOP] etcd wipe: {}".format(exc))
            try:
                reap_state_entries(cluster)
            except Exception as exc:
                log("[WARN] [STOP] state reap: {}".format(exc))
            if args is not None and getattr(args, "clean_up", False):
                try:
                    cleanup_k8s_workspace(cluster, dry_run=False)
                except Exception as exc:
                    log("[WARN] [STOP] controller workspace: {}".format(exc))
        else:
            try:
                reap_state_entries(cluster)
            except Exception as exc:
                log("[WARN] [STOP] state reap: {}".format(exc))
            try:
                _pipeline_wipe_etcd(cluster, globals_cfg)
            except Exception as exc:
                log("[WARN] [STOP] etcd wipe: {}".format(exc))
            if args is not None and getattr(args, "clean_up", False):
                try:
                    cleanup_cluster(cluster, globals_cfg, dry_run=False)
                except Exception as exc:
                    log("[WARN] [STOP] remote cleanup: {}".format(exc))
        log("[STOP] cleanup phase done (logs/outputs kept)")
    finally:
        if args is not None and getattr(args, "stop_file", None):
            try:
                os.remove(args.stop_file)
            except OSError:
                pass
        clear_stop()


if __name__ == "__main__":
    _install_stop_handlers()
    args, cfg_paths = parse_arguments()
    try:
        main(args, cfg_paths)
    except (StopRequested, KeyboardInterrupt) as exc:
        log("[STOP] requested ({}); cleaning up ...".format(
            exc.__class__.__name__))
        try:
            cleanup_on_stop()
        except Exception as ce:
            log("[WARN] [STOP] cleanup failed: {}".format(ce))
        sys.exit(130)
    except RuntimeError as exc:
        sys.exit("[ERROR] {}".format(exc))
