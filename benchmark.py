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
import base64
import csv
import glob
import hashlib
import ipaddress
import json
import math
import os
import posixpath
import queue
import re
import atexit
import copy
import shutil
import signal
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import uuid

from utils import (StopRequested, _drain_chan, _ssh_client, _ssh_drop,
                   clear_stop, controller_desc, controller_diagnose,
                   controller_exec, controller_get_file, controller_mkdir,
                   controller_put,
                   controller_remove,
                   controller_run, controller_stream, ensure_utf8_stdio,
                   get_node, is_k8s_node,
                   kubectl_exec_line, kubectl_prefix, load_json, log,
                   log_detail, log_phase, parse_data_size, percentile,
                   close_detail_log, request_stop, run_ssh, scp_pull,
                   scp_push, set_detail_log, set_stop_file, set_verbose,
                   ssh_check_output, stop_requested, verbose_enabled)

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
# 实例快照运行：_configs.json 继承链的根改指向快照内的 configs/（默认实时目录）
CONFIG_INHERIT_ROOT = os.environ.get("MC_CONFIGS_INHERIT_ROOT") or CONFIGS_DIR
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


# --- background per-config post-processing --------------------------------
# CSV aggregation + report map/reduce only consume already-pulled LOCAL
# artifacts, so they no longer sit on the critical path: main() queues them
# and immediately proceeds to the finished config's teardown / the next
# config's deployment. A single daemon worker preserves the historical order
# (and keeps the run-level profiling ledger writers serialized); _bg_join() is
# the barrier before assembly / overview. Daemon threads deliberately do not
# block interpreter exit: a STOP must never be held hostage by a big map.
_BG_LOCK = threading.Lock()
_BG_JOBS = []          # [(name, threading.Event, result_box)]
_BG_WORKER = None
_BG_Q = queue.Queue()


def _bg_loop():
    while True:
        item = _BG_Q.get()
        if item is None:
            _BG_Q.task_done()
            return
        name, fn, args, kwargs, done, box = item
        try:
            box["result"] = fn(*args, **kwargs)
        except BaseException as exc:      # WARN-only; never kills the run
            box["error"] = exc
        finally:
            done.set()
            _BG_Q.task_done()


def _bg_submit(name, fn, *args, **kwargs):
    """Queue one local post-processing unit (per config, or run-level plots)."""
    global _BG_WORKER
    done = threading.Event()
    box = {}
    with _BG_LOCK:
        if _BG_WORKER is None or not _BG_WORKER.is_alive():
            _BG_WORKER = threading.Thread(target=_bg_loop,
                                          name="mc-postproc", daemon=True)
            _BG_WORKER.start()
        _BG_JOBS.append((name, done, box))
    _BG_Q.put((name, fn, args, kwargs, done, box))
    return name


def _bg_join(timeout_s=None):
    """Wait for every queued unit; returns [(name, ok, result, error)] in
    submission order. A timeout only WARNs: the daemon worker keeps finishing
    in the background instead of blocking a STOP."""
    with _BG_LOCK:
        pending = list(_BG_JOBS)
        _BG_JOBS.clear()
    deadline = None if timeout_s is None else time.time() + float(timeout_s)
    out = []
    for name, done, box in pending:
        wait = None if deadline is None else max(0.0, deadline - time.time())
        if not done.wait(wait):
            log("[WARN] post-processing {} is still running after {}s; "
                "continuing without it".format(name, timeout_s))
            out.append((name, False, None,
                        "timed out after {}s".format(timeout_s)))
            continue
        if box.get("error") is not None:
            out.append((name, False, None, box["error"]))
        else:
            out.append((name, True, box.get("result"), None))
    return out


def _cluster_snapshot(cluster):
    """Shallow snapshot handed to the background post-processing worker.

    The main thread keeps adding k8s pod entries to cluster["nodes"] while
    the next scenario deploys; iterating that live dict from the worker would
    raise "dictionary changed size during iteration". Node value dicts are
    frozen once their config finishes, so sharing them is safe."""
    snap = dict(cluster or {})
    snap["nodes"] = dict((cluster or {}).get("nodes") or {})
    return snap


_PARALLEL_READY_TIMEOUT_S = 600


def _parallel_start_barrier(args):
    """Hold the first pipeline /start until the peer rank/recall child is ready.

    ``run_rank_recall.py`` gives both children one private directory and a
    distinct role. Each child atomically publishes ``<role>.ready`` only after
    its own client pool finished rendezvous (i.e. it is one step away from
    triggering /start), then blocks until the peer marker exists — so both
    children enter their measured window together instead of staggering with
    the asymmetric data-generation phases. The barrier is one-shot: it
    synchronizes the first /start of each child, which is the shared
    rank+recall pressure point, without coupling later sweep entries.
    """
    ready_dir = getattr(args, "parallel_ready_dir", None)
    role = getattr(args, "parallel_ready_role", None)
    if not ready_dir or not role or getattr(args, "_parallel_ready_done", False):
        return
    if role not in ("rank", "recall"):
        raise RuntimeError("invalid parallel ready role: {}".format(role))
    os.makedirs(ready_dir, exist_ok=True)
    marker = os.path.join(ready_dir, role + ".ready")
    tmp = marker + ".{}.tmp".format(os.getpid())
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("{}\n".format(os.getpid()))
    os.replace(tmp, marker)
    peer = "recall" if role == "rank" else "rank"
    peer_marker = os.path.join(ready_dir, peer + ".ready")
    log("[parallel] {} clients registered; holding /start, waiting for "
        "{}".format(role, peer))
    deadline = time.time() + _PARALLEL_READY_TIMEOUT_S
    while not os.path.isfile(peer_marker):
        _check_stop()
        if time.time() >= deadline:
            raise RuntimeError(
                "parallel /start barrier timed out after {}s: {} marker "
                "not found".format(_PARALLEL_READY_TIMEOUT_S, peer))
        time.sleep(0.2)
    args._parallel_ready_done = True
    log("[parallel] rank and recall aligned at /start; continuing benchmark")

SCRIPTS_DIR = os.path.join(ROOT, "scripts")
RESULTS_DIR = os.path.join(ROOT, "outputs")
# RESULT rows whose first field is a wall-clock timestamp (HH:MM:SS.micro);
# legacy rows start with the op name directly (see parse_benchmark_log)
# RESULT row timestamp: new "YYYY-MM-DD HH:MM:SS.micro" (date disambiguates
# multi-day runs) or legacy "HH:MM:SS.micro". The optional date prefix
# keeps both formats detectable with one regex.
TS_PREFIX = re.compile(r"^(?:\d{4}-\d{2}-\d{2} )?\d{2}:\d{2}:\d{2}")
# store interface families (mirror scripts/mooncake_benchmark_client.py so the
# orchestrator can validate operation values and choose staging behaviour)
PIPELINE_PUT_IFACES = ("put", "put_batch", "put_from", "upsert_from")
PIPELINE_GET_IFACES = ("get", "get_batch", "get_into", "get_buffer")
PIPELINE_PUT_BATCH_IFACES = ("put_batch",)
PIPELINE_GET_BATCH_IFACES = ("get_batch",)
LATENCY_THRESHOLD_US = (2000, 5000, 6000, 7000, 8000, 9000, 10000, 20000)
LATENCY_THRESHOLD_COLUMNS = [">{}us".format(value)
                             for value in LATENCY_THRESHOLD_US]
LATENCY_SUMMARY_COLUMNS = [
    "min_ms", "avg_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms",
    "p999_ms", "p9999_ms", "max_ms",
] + LATENCY_THRESHOLD_COLUMNS
BASIC_CSV_HEADER = [
    "scenario", "work_mode", "operation", "node", "topology", "protocol",
    "num_clients", "num_threads",
    "data_size", "key_count", "batch_size", "put_ratio", "duration", "warmup_duration",
    "target_qps",
    "total", "fail", "fail_rate", "qps",
] + LATENCY_SUMMARY_COLUMNS + [
    "ok", "total_bytes", "wall_s",
    "throughput_mb_s", "throughput_gb_s",
]
PIPELINE_CSV_HEADER = [
    "config_name",
    "scenario", "work_mode", "operation", "role", "node", "instance", "topology", "protocol",
    "num_threads", "notify_threads",
    "data_size", "key_count", "batch_size", "duration", "warmup_duration",
    "client_count", "notify_count", "store_count",
    "base_qps", "qps_multiple", "target_qps", "notify_pipeline", "traffic_mode",
    "total", "fail", "fail_rate", "qps",
] + LATENCY_SUMMARY_COLUMNS + [
    "success", "total_bytes", "wall_s",
    "throughput_mb_s", "throughput_gb_s",
    "queue_wait_avg_ms", "queue_wait_max_ms", "max_depth",
    "notify_async", "notify_max_inflight", "notify_submitted", "notify_ok",
    "notify_fail", "notify_inflight_peak", "notify_backpressure_ms",
    "notify_drain_ms", "notify_avg_ms", "notify_max_ms",
    "notify_queue_size", "notify_queue_peak", "notify_queue_wait_avg_ms",
    "notify_queue_wait_max_ms", "notify_connections_created", "notify_connections_reused",
]
# kvtest rows come from each mooncake_kvtest instance's summary.json (per-op
# total/success/failure/bytes + latency percentiles). One writer row and/or
# one reader row per instance per data size + aggregate TOTAL rows.
KVTEST_CSV_HEADER = [
    "config_name",
    "scenario", "work_mode", "operation", "role", "node", "instance", "topology", "protocol",
    "num_threads", "notify_threads",
    "data_size", "key_pool_size", "batch_size", "duration", "warmup_duration",
    "client_count", "notify_count", "store_count",
    "base_qps", "qps_multiple", "target_qps",
    "total", "fail", "fail_rate", "qps",
] + LATENCY_SUMMARY_COLUMNS + [
    "success", "total_bytes", "wall_s",
    "throughput_mb_s", "throughput_gb_s",
    "verify_failures",
    "notify_submitted", "notify_ok", "notify_fail",
    "notify_avg_ms", "notify_max_ms",
    "queue_wait_avg_ms", "queue_wait_max_ms",
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


def _pod_base(base_noext, node):
    """Per-pod log base name. k8s pods get their OWN subdirectory under the
    (hostPath-mounted) log dir so the on-node layout is
    <host_log_root>/<run_id>/<pod_name>/… (log persistence across pod
    deletion); ssh nodes keep the flat layout (one shared log dir per node).

    base_noext = "<remote_log_dir>/<cfg>.<run_name>" (no trailing .log);
    POSIX-only string ops on purpose: remote paths are always /-joined."""
    if node is None or not node.get("_pod"):
        return base_noext
    head, tail = base_noext.rsplit("/", 1)
    return "{}/{}/{}".format(head, node["_pod"], tail)


def _pod_path_to_site(cluster, path):
    """Map a pod-internal log path to its node-side hostPath twin.

    Benchmark pods mount <host_log_root>/<run_id> at
    <pod_workspace>/logs/<run_id> (benchmark-pod.yaml.tpl), so every path
    under that mount has an exact node-side counterpart — the path the report
    must hand out for "ssh the node and cd there". ssh-mode paths and
    anything outside the mount are returned unchanged (there the node path IS
    the path)."""
    p = str(path or "")
    if not p.startswith("/"):
        return path
    try:
        k8s = k8s_settings(cluster)
    except Exception:
        return path
    pod_ws = (k8s.get("pod_workspace") or "").rstrip("/")
    root = (k8s.get("host_log_root") or "").rstrip("/")
    if not pod_ws or not root:
        return path
    mount = pod_ws + "/logs/"
    if not p.startswith(mount):
        return path
    return root + "/" + p[len(mount):]


def remote_log_path_pod(remote_log_dir, scenario, name, node):
    """<dir>/<scenario>.<name>.log with the k8s pod subdirectory inserted.

    k8s stages/pods write into <log_dir>/<pod>/...; using the flat form for
    the basic benchmark / remove-keys logs made pull_logs' per-pod glob miss
    them (and split the on-node layout)."""
    base = remote_log_path(remote_log_dir, scenario, name)[:-len(".log")]
    return _pod_base(base, node) + ".log"


def _kvtest_pod_dir(remote_log_dir, node):
    """kvtest 远端日志根目录：k8s 每 pod 独立子目录（与 pipeline 现场结构一致），
    ssh 模式保持平铺（无 pod 概念）。该目录同时容纳 .log/.log.ylt.log/
    .exitrc/.summary.json/.samples.csv/.trace.json 与 MC_LOG_DIR 的
    <base>.glog.d/（glog INFO 分段），ssh 现场可整体查看。"""
    if node is not None and node.get("_pod"):
        return "{}/{}".format(remote_log_dir.rstrip("/"), node["_pod"])
    return remote_log_dir


def _kvtest_pod_base(remote_log_dir, base, node):
    """kvtest 单个实例的远端路径前缀（<pod_dir>/<base>，带 pod 子目录）。"""
    return _pod_base("{}/{}".format(remote_log_dir.rstrip("/"), base), node)


def _kvtest_report_context(cluster, ts, remote_log_dir, is_k8s):
    """报表「现场索引」所需的运行上下文（host 目录 + run id + 登录提示）。"""
    ctx = {"run_id": ts, "remote_log_dir": remote_log_dir}
    if is_k8s:
        try:
            ks = k8s_settings(cluster)
            ctx["host_log_root"] = ks.get("host_log_root") or ""
            ctx["k8s_namespace"] = ks.get("namespace") or ""
        except Exception:
            pass
    return ctx


def _kvtest_site_dirs(cluster, remote_log_dir, cfg):
    """iid -> node-side per-pod log directory (the "ssh + cd" target).

    Only k8s runs have a hostPath twin; ssh-mode/old layouts yield {} so the
    report keeps its previous behaviour."""
    pods = (cfg or {}).get("_kvtest_iid_pods") or {}
    out = {}
    for iid, pod in pods.items():
        if not pod or not remote_log_dir:
            continue
        src = posixpath.join(remote_log_dir, pod)
        site = _pod_path_to_site(cluster, src)
        if site and site != src:
            out[str(iid)] = site
    return out


def _kvtest_remote_names(groups):
    """Locally pulled logical name -> actual node-side name.

    kvtest gzips samples/trace in place on the pod, so the node keeps
    <x>.samples.csv.gz while the pulled copy is gunzipped back to
    <x>.samples.csv. The report must point at the file that actually exists
    on the node."""
    out = {}
    for _n, files, _label in groups or []:
        for f in files or []:
            name = os.path.basename(f)
            if name.endswith(".gz"):
                out[name[:-len(".gz")]] = name
            else:
                out.setdefault(name, name)
    return out


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


_STORE_METRICS = (
    "segment_total_capacity_bytes",
    "segment_allocated_bytes",
    "master_total_capacity_bytes",
    "master_allocated_bytes",
    "mooncake_vsegment_logical_capacity_bytes",
    "mooncake_vsegment_logical_free_bytes",
)


def _master_metrics_url(globals_cfg):
    """Master Prometheus metrics URL: ONLY the explicit global.json
    master_metrics_url is used (must include the metrics port). Nothing is
    derived from master_addr and no port/host default is baked in — an
    unset URL simply disables the probe (pipeline then needs
    store_total_size)."""
    return (globals_cfg.get("master_metrics_url") or "").strip()


def _parse_prometheus_metrics(text):
    """Prometheus text exposition -> {metric_name: [values...]}.

    Label sets are stripped from the name; a LABELED family (e.g. the
    per-segment gauges, one series per segment) keeps EVERY series value so
    callers can aggregate correctly. (Keeping only the last value parsed a
    labeled family to 0 — real incident: segment_total_capacity_bytes has
    dozens of series, most zero.)"""
    vals = {}
    for line in (text or "").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        toks = line.split()
        if len(toks) < 2:
            continue
        name = toks[0].split("{", 1)[0]
        try:
            vals.setdefault(name, []).append(float(toks[1]))
        except ValueError:
            continue
    return vals


def _metric_total(vals, name):
    """Aggregate a gauge family: label-less gauges have one series (the
    value itself); labeled families (per segment) are summed."""
    series = vals.get(name)
    if not series:
        return None
    return sum(series)


def _http_get_text(url, timeout=5):
    """Orchestrator-direct GET (ssh mode). Returns text or None."""
    import urllib.request
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return resp.read().decode("utf-8", "replace")
    except Exception:
        return None


def _controller_curl_get(cluster, url, timeout=5):
    """GET executed ON the controller (k8s mode). Returns text or None."""
    c = cluster["_controller"]
    cmd = "curl -s --max-time {}".format(int(max(1, timeout)))
    cmd += " " + shlex.quote(url)
    try:
        out, rc = controller_exec(c, cmd)
    except Exception:
        return None
    return out if rc == 0 and out.strip() else None


def _probe_metrics_via_exec(cluster, url):
    """curl the metrics INSIDE the master pod via kubectl exec.

    The metrics port is typically pod-internal only: the controller node
    does not use cluster DNS (so Service names do not resolve there) and no
    Service may expose the port at all. kubectl exec needs zero cluster
    changes: API server -> kubelet -> container.

    The pod NAME is discovered from the Service named in
    master_metrics_url (endpoints targetRef), so svc/ns/port all come from
    the URL — no cluster.json metrics params, no baked-in defaults. Returns
    (text, source_label) or (None, reason)."""
    c = cluster["_controller"]
    try:
        p = urllib.parse.urlsplit(url or "")
        host, port = p.hostname, p.port
    except Exception:
        return None, "cannot parse master_metrics_url {!r}".format(url)
    if not host:
        return None, "master_metrics_url has no host"
    if port is None:
        return None, ("master_metrics_url must include the metrics port "
                      "(e.g. http://<svc>.<ns>.svc.cluster.local:<port>/metrics)")
    svc, ns = _svc_ns_from_host(host)
    if not svc:
        return None, ("cannot derive a Service name from host {!r}; the "
                      "exec fallback needs the svc DNS form "
                      "<svc>.<ns>.svc.cluster.local".format(host))
    out, rc = controller_run(
        c, ["-n", ns, "get", "endpoints", svc, "-o",
            "jsonpath={.subsets[0].addresses[0].targetRef.name}"], check=False)
    pod = (out or "").strip()
    if rc != 0 or not pod:
        return None, ("cannot find the backing pod of svc {!r} in ns {!r} "
                      "({})".format(svc, ns, (out or "").strip()[:200] or
                                    "no endpoints targetRef"))
    out, rc = controller_run(
        c, ["-n", ns, "exec", pod, "--", "curl", "-s", "--max-time", "5",
            "http://localhost:{}/metrics".format(port)], check=False)
    if rc != 0 or not out.strip():
        return None, "kubectl exec curl failed (rc={}): {}".format(
            rc, out.strip()[:200])
    return out, "kubectl exec {}/{}:{}".format(ns, pod, port)


def discover_store_capacity(cluster, globals_cfg):
    """Fetch the master's Prometheus metrics and record the store TOTAL
    capacity (B) for the key-space budget. Failure only WARNs.

    Single source of configuration: global.json master_metrics_url (a
    complete URL that MUST include the metrics port). No host/port default
    is baked into the code.

    Probe chain (k8s mode, all executed via the controller):
      1. translate <svc>.<ns>.svc.cluster.local -> ClusterIP (kubectl svc)
         and curl it (works when the controller is a cluster node and the
         Service declares the metrics port)
      2. kubectl get endpoints <svc> -> targetRef pod name -> kubectl exec
         the pod and curl localhost:<port>/metrics (zero cluster changes;
         works even when the port is not exposed by a Service)
    ssh mode: orchestrator direct GET, then curl on the first cluster node.

    Labeled gauge families (per-segment series) are summed; the label-less
    master_total gauge is preferred when present."""
    url = _master_metrics_url(globals_cfg)
    if "master_metrics_port" in globals_cfg:
        log("[WARN] global.json master_metrics_port is deprecated and "
            "ignored; include the port in master_metrics_url instead")
    text = None
    source = ""
    why = ""
    if cluster is not None and cluster.get("_controller") is not None:
        if url:
            cache = cluster.setdefault("_metrics_resolved", {})
            if url not in cache:
                try:
                    cache[url] = _resolve_k8s_addr(url, cluster)
                except Exception:
                    cache[url] = url
            text = _controller_curl_get(cluster, cache[url] or url)
            source = "direct {}".format(cache[url] or url)
            if not text:
                text, why = _probe_metrics_via_exec(cluster, url)
                if text:
                    source = why
        else:
            why = "master_metrics_url not set in global.json"
        k8s = cluster.get("k8s") or {}
        stale = sorted(k for k in k8s if k.startswith("master_metrics_"))
        if stale:
            log("[WARN] cluster.json k8s.{} is deprecated and ignored; set "
                "global.json master_metrics_url (svc DNS form + port) "
                "instead".format("/".join(stale)))
    else:
        if url:
            text = _http_get_text(url)
            source = url
            if text is None and cluster:
                for name in (cluster.get("nodes") or {}):
                    n = get_node(cluster, name)
                    try:
                        text = ssh_check_output(
                            n["ip"], n, "curl -s --max-time 5 {}".format(
                                shlex.quote(url)), quiet=True)
                    except Exception:
                        text = None
                    break
            if text is None:
                why = "master metrics unreachable at {}".format(url)
        else:
            why = "master_metrics_url not set in global.json"
    if not text:
        log("[WARN] store capacity probe failed ({}); pipeline scenarios "
            "require a store capacity and will FAIL — set global.json "
            "store_total_size manually and/or fix master_metrics_url "
            "(full URL incl. port)".format(why or "unknown"))
        return None
    vals = _parse_prometheus_metrics(text)
    if not any(k in vals for k in _STORE_METRICS):
        log("[WARN] store capacity probe: no known capacity gauges from {} "
            "(got {} metric names); set store_total_size manually".format(
                source, len(vals)))
        return None
    printed = []
    for k in _STORE_METRICS:
        t = _metric_total(vals, k)
        if t is None:
            continue
        n_series = len(vals[k])
        printed.append("{}{}={}".format(
            k, " ({} series summed)".format(n_series) if n_series > 1 else "",
            _fmt_gib(t)))
    log_detail("store metrics ({}): {}".format(source, ", ".join(printed)))
    total = _metric_total(vals, "master_total_capacity_bytes")
    if total is None:
        total = _metric_total(vals, "segment_total_capacity_bytes")
    if not total:
        total = _metric_total(vals, "mooncake_vsegment_logical_capacity_bytes")
    if not total:
        log("[WARN] store capacity probe: total capacity gauge missing in "
            "{}; set store_total_size manually".format(source))
        return None
    globals_cfg["_store_total_auto"] = int(total)
    allocated = _metric_total(vals, "segment_allocated_bytes")
    if allocated is None:
        allocated = _metric_total(vals, "master_allocated_bytes")
    free = (total - allocated) if allocated is not None else None
    globals_cfg["_store_free_auto"] = int(free) if free is not None else None
    log_detail("store capacity (probed): total={}{}".format(
        _fmt_gib(total),
        "" if free is None else ", free={}".format(_fmt_gib(free))))
    return int(total)


def _fmt_gib(n):
    """Human byte size for logs (None -> '<unset>')."""
    if n is None:
        return "<unset>"
    return "{:.2f} GiB".format(n / 1024.0 ** 3)


def _fmt_mb(n):
    if not n:
        return "0 MB"
    if n >= 1024 ** 3:
        return "{:.2f} GiB".format(n / 1024.0 ** 3)
    return "{:.0f} MB".format(n / 1024.0 ** 2)


def _store_capacity_resolution(globals_cfg):
    """Resolve the store capacity used for key-space budgets.

    A = global.json store_total_size (operator's allowed budget)
    B = probed master-metrics total (segment_total_capacity_bytes)

    effective = min(A, B) when both are known — A>B means "use the whole
    cluster capacity", A<B means "only use part of it"; a single source
    stands alone; None when neither is available (pipeline then FAILs).

    Returns (a_bytes, b_bytes, effective_bytes, source_label)."""
    a = None
    v = globals_cfg.get("store_total_size")
    if v not in (None, "", 0):
        a = parse_size_bytes(v)
    b = globals_cfg.get("_store_total_auto")
    b = int(b) if b else None
    if a is not None and b is not None:
        return a, b, min(a, b), "min(configured,probed)"
    if a is not None:
        return a, b, a, "configured"
    if b is not None:
        return a, b, b, "probed"
    return None, None, None, "none"


def _store_total_bytes(globals_cfg):
    """Effective store capacity for key-space budgets (min rule; None when
    unknown)."""
    return _store_capacity_resolution(globals_cfg)[2]


# cleanliness verify: after a remove-keys stage the store should be empty;
# allocated above this fraction of total capacity is suspicious (metadata
# overhead keeps a truly-empty store at ~0). WARN by default, FAIL when
# global.json pipeline.require_clean_store is true.
_STORE_CLEAN_TOLERANCE = 0.01


def _fetch_master_metrics_text(cluster, globals_cfg):
    """Fetch the master Prometheus text through the same probe chain used by
    discover_store_capacity (k8s: controller direct curl -> kubectl exec;
    ssh: orchestrator direct GET -> curl on the first cluster node).
    Returns (text, source) or (None, None) when the probe is unavailable."""
    url = _master_metrics_url(globals_cfg)
    if not url:
        return None, None
    if cluster is not None and cluster.get("_controller") is not None:
        cache = cluster.setdefault("_metrics_resolved", {})
        if url not in cache:
            try:
                cache[url] = _resolve_k8s_addr(url, cluster)
            except Exception:
                cache[url] = url
        text = _controller_curl_get(cluster, cache[url] or url)
        source = "direct {}".format(cache[url] or url)
        if not text:
            text, _why = _probe_metrics_via_exec(cluster, url)
            source = "exec {}".format(url)
        return (text or None), (source if text else None)
    text = _http_get_text(url)
    source = url
    if text is None and cluster:
        for name in (cluster.get("nodes") or {}):
            n = get_node(cluster, name)
            try:
                text = ssh_check_output(
                    n["ip"], n, "curl -s --max-time 5 {}".format(
                        shlex.quote(url)), quiet=True)
            except Exception:
                text = None
            if text:
                break
    if not text:
        return None, None
    return text, source


def _store_usage_snapshot(cluster, globals_cfg):
    """Per-series store usage right now: {allocated, total, pct,
    seg_alloc, seg_cap, source}. None when the metrics probe is unavailable.

    `seg_alloc`/`seg_cap` are the raw per-series values (one entry per
    store/segment as exposed by the master) — they are what tells a single
    store skew (one series near its own capacity) apart from a global
    watermark (all series high)."""
    text, source = _fetch_master_metrics_text(cluster, globals_cfg)
    if text is None:
        return None
    vals = _parse_prometheus_metrics(text)
    total = _metric_total(vals, "master_total_capacity_bytes")
    if not total:
        total = _metric_total(vals, "segment_total_capacity_bytes")
    if not total:
        total = _metric_total(vals, "mooncake_vsegment_logical_capacity_bytes")
    allocated = _metric_total(vals, "segment_allocated_bytes")
    if allocated is None:
        allocated = _metric_total(vals, "master_allocated_bytes")
    if total is None and allocated is None:
        return None
    total = total or 0.0
    allocated = allocated or 0.0
    return {
        "allocated": allocated,
        "total": total,
        "pct": (allocated / total) if total > 0 else 0.0,
        "seg_alloc": vals.get("segment_allocated_bytes") or [],
        "seg_cap": vals.get("segment_total_capacity_bytes") or [],
        "source": source,
    }


def _require_clean_store(globals_cfg):
    return bool((globals_cfg.get("pipeline") or {}).get(
        "require_clean_store", False))


def _verify_store_clean(cluster, globals_cfg, cfg_name, when):
    """Log (and optionally FAIL) when the store is not empty after a
    remove-keys stage. Never raises for a missing probe; raises only when
    pipeline.require_clean_store is true and allocated exceeds the
    tolerance."""
    snap = _store_usage_snapshot(cluster, globals_cfg)
    if snap is None:
        log_detail("[verify] {}: store cleanliness check skipped "
                   "(master_metrics_url unset/unreachable)".format(when))
        return None
    nonzero = [(a, snap["seg_cap"][i] if i < len(snap["seg_cap"]) else None)
               for i, a in enumerate(snap["seg_alloc"]) if a > 0]
    detail = ", ".join(
        "seg#{}={}{}".format(
            i, _fmt_gib(a),
            "" if c is None else " ({:.0f}% of {})".format(
                (a / c * 100.0) if c else 0.0, _fmt_gib(c)))
        for i, (a, c) in enumerate(nonzero[:8]))
    msg = ("[verify] {}: store after remove allocated={} / total={} "
           "({:.1f}%), nonzero segments={}/{}".format(
               when, _fmt_gib(snap["allocated"]), _fmt_gib(snap["total"]),
               snap["pct"] * 100.0, len(nonzero), len(snap["seg_alloc"])))
    if detail:
        msg += ": " + detail
    if snap["pct"] > _STORE_CLEAN_TOLERANCE:
        if _require_clean_store(globals_cfg):
            raise RuntimeError(
                "{} — store is NOT clean after remove-keys ({:.1f}% > {:.0f}% "
                "tolerance); pipeline.require_clean_store=true. Check for "
                "leftover objects / in-flight handles before rerunning"
                "".format(msg, snap["pct"] * 100.0,
                           _STORE_CLEAN_TOLERANCE * 100.0))
        log("[WARN] " + msg + " — store not fully empty after remove-keys; "
            "set pipeline.require_clean_store=true to hard-fail instead")
    else:
        log(msg)
    return snap


def _remove_skip_when_clean(globals_cfg):
    return bool((globals_cfg.get("pipeline") or {}).get(
        "remove_skip_when_clean", True))


def _remove_release_timeout_s(globals_cfg):
    """pipeline.remove_release_timeout_s: seconds to wait for the master
    oplog to drain a remove-all. 0 disables the wait (legacy behaviour)."""
    raw = (globals_cfg.get("pipeline") or {}).get(
        "remove_release_timeout_s", 120)
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = 120
    return max(0, value)


_REMOVE_RELEASE_POLL_S = 2.0
_REMOVE_RELEASE_LOG_EVERY_S = 10.0


def _store_snapshot_is_clean(snap):
    """Same acceptance criterion as _verify_store_clean: allocated within
    _STORE_CLEAN_TOLERANCE of total. An unavailable snapshot (None) is never
    clean, so probing failures always fall back to running the stage. When
    the master reports no capacity at all, fall back to an absolute-zero
    check so a missing total can never be read as "clean"."""
    if not snap:
        return False
    if (snap.get("total") or 0) <= 0:
        return (snap.get("allocated") or 0) <= 0
    return snap["pct"] <= _STORE_CLEAN_TOLERANCE


def _fmt_store_snapshot(snap):
    nonzero = sum(1 for a in snap["seg_alloc"] if a > 0)
    return ("allocated={} / total={} ({:.1f}%), nonzero segments={}/{}"
            .format(_fmt_gib(snap["allocated"]), _fmt_gib(snap["total"]),
                    snap["pct"] * 100.0, nonzero, len(snap["seg_alloc"])))


def _remove_keys_precheck(cluster, globals_cfg, cfg_name, when):
    """Skip a remove-keys stage outright when the store is already clean.

    One master-metrics probe (~1-2s) replaces a full client cold start
    (~40s with ub). Fail-safe: an unavailable probe or a disabled knob
    returns False (run the stage), so a degraded metrics path never
    silently skips a real cleanup. On skip the config's ledger rows are
    dropped as run_stage would have done."""
    if not _remove_skip_when_clean(globals_cfg):
        return False
    snap = _store_usage_snapshot(cluster, globals_cfg)
    if snap is None:
        log_detail("[remove-keys] {}: pre-check skipped (master metrics "
                   "unavailable)".format(when))
        return False
    if not _store_snapshot_is_clean(snap):
        return False
    log("[remove-keys] {}: store already clean ({}); skipping remove-keys"
        .format(when, _fmt_store_snapshot(snap)))
    if cfg_name:
        state_remove_cfg(cfg_name)
    return True


def _wait_store_released(cluster, globals_cfg, cfg_name, when):
    """Wait for the master oplog to apply a remove-all, then verify.

    remove_all only enqueues the deletion; segment_allocated_bytes drains
    afterwards, so an immediate post-remove verify could false-WARN (or
    false-FAIL under require_clean_store). Poll until the store reads clean,
    bounded by pipeline.remove_release_timeout_s. A disabled wait, an
    unavailable probe or an already-clean store degrades to the legacy
    single-shot verify."""
    timeout_s = _remove_release_timeout_s(globals_cfg)
    if timeout_s <= 0:
        return _verify_store_clean(cluster, globals_cfg, cfg_name, when)
    snap = _store_usage_snapshot(cluster, globals_cfg)
    if snap is None or _store_snapshot_is_clean(snap):
        return _verify_store_clean(cluster, globals_cfg, cfg_name, when)
    t0 = time.time()
    log("[remove-keys] {}: waiting for oplog release (timeout {}s): {}"
        .format(when, timeout_s, _fmt_store_snapshot(snap)))
    last_log = t0
    while time.time() - t0 < timeout_s:
        _check_stop()
        time.sleep(min(_REMOVE_RELEASE_POLL_S,
                       max(0.0, timeout_s - (time.time() - t0))))
        snap = _store_usage_snapshot(cluster, globals_cfg)
        if snap is None or _store_snapshot_is_clean(snap):
            break
        now = time.time()
        if now - last_log >= _REMOVE_RELEASE_LOG_EVERY_S:
            log("[remove-keys] {}: still releasing ({:.0f}s/{:.0f}s): {}"
                .format(when, now - t0, timeout_s,
                        _fmt_store_snapshot(snap)))
            last_log = now
    if snap is not None and _store_snapshot_is_clean(snap):
        log("[remove-keys] {}: released after {:.1f}s"
            .format(when, time.time() - t0))
    return _verify_store_clean(cluster, globals_cfg, cfg_name, when)


def _apply_monitor_overrides(matrix, args):
    """--monitor-mem/--monitor-urma on|off override the profile matrix.

    The WebUI run form always sends both (default off), so lab users can
    drop the two host-monitoring DaemonSets when the environment is too
    memory-constrained; direct CLI users who pass neither keep profile.json
    untouched. Turning the last enabled tool off disables profiling for the
    run (same as an all-disabled matrix)."""
    overrides = {"mem": getattr(args, "monitor_mem", None),
                 "urma": getattr(args, "monitor_urma", None)}
    if matrix is None:
        if any(v == "on" for v in overrides.values()):
            log("[profile] monitor override requested but the profile "
                "matrix has no available tools; staying off")
        return None
    for tool, val in sorted(overrides.items()):
        if val not in ("on", "off"):
            continue
        cfg = matrix["tools"].setdefault(tool, {})
        cfg["enabled"] = (val == "on")
        log("[profile] {} monitor: {}".format(
            "memory" if tool == "mem" else "URMA port",
            "on" if cfg["enabled"] else "off"))
    if not any(c.get("enabled") for c in matrix["tools"].values()):
        log("[profile] all monitors off; not starting the monitor DaemonSet")
        return None
    return matrix


_REMOVE_SVC_PORT = 9401
# The warm store client needs ~40s to initialize; a measurement window shorter
# than this cannot hide it (the config-end remove happens right after the
# window), so short smoke runs skip the prewarm entirely.
_REMOVE_SVC_MIN_WINDOW_S = 90
_REMOVE_SVC_REQUESTS = {"cpu": "10m", "memory": "32Mi"}
_REMOVE_SVC_LIMITS = {"cpu": "200m", "memory": "256Mi"}


def _remove_service_enabled(cluster, globals_cfg):
    """k8s.remove_service (default true): a tiny per-run remove pod holds one
    warm store client from the measurement window to the sweep-end remove."""
    if not cluster or cluster.get("_controller") is None:
        return False
    return bool(((globals_cfg or {}).get("k8s") or {})
                .get("remove_service", True))


def _remove_service_name(run_label):
    name = "mc-{}-remove".format(run_label).lower()
    name = re.sub(r"[^a-z0-9.-]+", "-", name)
    return name.strip("-.")[:63].rstrip("-.")


def _render_remove_deployment_yaml(cluster, ts, cfg_name):
    """Tiny 1-replica Deployment hosting the long-lived remove service."""
    k8s = k8s_settings(cluster)
    run_label = k8s_run_label(ts, cfg_name)
    name = _remove_service_name(run_label)
    ns = k8s["namespace"]
    image = k8s.get("image") or ""
    pod_ws = k8s.get("pod_workspace") or "/tmp/Mooncake-Performance"
    scripts_mount = posixpath.join(pod_ws, "scripts")
    cm = _configmap_name(run_label)
    yaml_text = """apiVersion: apps/v1
kind: Deployment
metadata:
  name: {name}
  namespace: {ns}
  labels:
    app: mc-remove
    run: "{run_label}"
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mc-remove
      run: "{run_label}"
  template:
    metadata:
      labels:
        app: mc-remove
        run: "{run_label}"
    spec:
      containers:
      - name: remove
        image: "{image}"
        imagePullPolicy: IfNotPresent
        command: ["sleep", "infinity"]
        env:
        - name: POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        resources:
          requests:
            cpu: "{req_cpu}"
            memory: "{req_mem}"
          limits:
            cpu: "{lim_cpu}"
            memory: "{lim_mem}"
        volumeMounts:
        - name: scripts
          mountPath: {scripts_mount}
          readOnly: true
      volumes:
      - name: scripts
        configMap:
          name: {cm}
""".format(name=name, ns=ns, run_label=run_label, image=image,
           req_cpu=_REMOVE_SVC_REQUESTS["cpu"],
           req_mem=_REMOVE_SVC_REQUESTS["memory"],
           lim_cpu=_REMOVE_SVC_LIMITS["cpu"],
           lim_mem=_REMOVE_SVC_LIMITS["memory"],
           scripts_mount=scripts_mount, cm=cm)
    return name, run_label, yaml_text


def _remove_service_ensure(cluster, globals_cfg, ts, cfg_name, dry=False):
    """Create the remove Deployment, launch the service and wait /health.

    Returns True once the store client is ready; raises on any setup error
    (the caller records it and remove stages fall back to the legacy path).
    """
    st = cluster.setdefault("_remove_svc", {})
    if st.get("deploy") and st.get("ok"):
        return True
    k8s = k8s_settings(cluster)
    c = cluster["_controller"]
    name, run_label, yaml_text = _render_remove_deployment_yaml(
        cluster, ts, cfg_name)
    st["deploy"] = name
    if dry:
        log("[remove-service] (dry-run) would create {} (ns={})".format(
            name, k8s["namespace"]))
        return False
    t_start = time.time()
    log("[remove-service] creating Deployment {} (ns={}, pod_ws={})".format(
        name, k8s["namespace"], k8s.get("pod_workspace") or "?"))
    import tempfile
    local_dir = tempfile.mkdtemp(prefix="mc-remove-")
    local_yaml = os.path.join(local_dir, name + ".yaml")
    try:
        with open(local_yaml, "w", encoding="utf-8", newline="\n") as f:
            f.write(yaml_text)
        yaml_dir = posixpath.join(k8s["controller_workspace"], "pods")
        log_detail("[remove-service] uploading manifest -> {}:{}".format(
            controller_desc(c), yaml_dir))
        controller_put(c, [local_yaml], yaml_dir)
        remote_yaml = posixpath.join(yaml_dir, name + ".yaml")
        log_detail("[remove-service] kubectl apply")
        out, rc = controller_run(
            c, ["-n", k8s["namespace"], "apply", "-f", remote_yaml],
            check=False, timeout=60)
        if rc != 0:
            raise RuntimeError("kubectl apply failed for {}: {}".format(
                name, (out or "").strip()[:300]))
    finally:
        shutil.rmtree(local_dir, ignore_errors=True)
    cluster.setdefault("_k8s_deployments", []).append(name)
    t0 = time.time()
    log_detail("[remove-service] waiting rollout (timeout 90s)")
    wait_k8s_deployment_ready(cluster, name, run_label, timeout=90)
    out, rc = controller_run(
        c, ["-n", k8s["namespace"], "get", "pods", "-l",
            "app=mc-remove,run=" + run_label, "-o",
            "jsonpath={.items[0].metadata.name}"], check=False, timeout=30)
    pod = (out or "").strip()
    if rc != 0 or not pod:
        raise RuntimeError("remove-service pod not found for {}".format(name))
    ip = _k8s_pod_ip(c, k8s["namespace"], pod)
    entry = {"name": pod, "ip": ip, "_pod": pod, "_ns": k8s["namespace"],
             "_controller": c,
             "_controller_workspace": k8s.get("controller_workspace")}
    st["node"] = entry
    scripts_dir = posixpath.join(k8s.get("pod_workspace")
                                 or "/tmp/Mooncake-Performance", "scripts")
    # must live INSIDE the hostPath mount (<pod_ws>/logs/<ts>) and under the
    # service pod's own directory — <pod_ws>/logs alone is the container
    # filesystem and is lost with the pod (the report indexes this file).
    log_dir = posixpath.join(k8s.get("pod_workspace")
                             or "/tmp/Mooncake-Performance",
                             "logs", ts, pod)
    launch = (
        "set -a; . {env}; if [ -f {envm} ]; then . {envm}; fi; "
        "export LD_LIBRARY_PATH=/home/Mooncake/outputs/lib:${{LD_LIBRARY_PATH:-}}; "
        "mkdir -p {logdir}; nohup \"${{ORCH_VENV:-python3}}\" {svc} "
        "--port {port} > {log} 2>&1 & echo $!").format(
            env=shlex.quote(posixpath.join(scripts_dir, "env.sh")),
            envm=shlex.quote(posixpath.join(scripts_dir, "env.mooncake.sh")),
            logdir=shlex.quote(log_dir),
            svc=shlex.quote(posixpath.join(scripts_dir, "remove_service.py")),
            port=_REMOVE_SVC_PORT,
            log=shlex.quote(posixpath.join(log_dir, "remove_service.log")))
    log_detail("[remove-service] launching service on {}:{} (pod={})".format(
        ip, _REMOVE_SVC_PORT, pod))
    ssh_check_output(ip, entry, launch, quiet=True, timeout=30)
    deadline = time.time() + 120
    while time.time() < deadline:
        _check_stop()
        try:
            code = ssh_check_output(
                ip, entry,
                "curl -s -o /dev/null -w '%{{http_code}}' --max-time 2 "
                "http://127.0.0.1:{}/health".format(_REMOVE_SVC_PORT),
                quiet=True, timeout=10)
            if str(code).strip() == "200":
                st["ok"] = True
                st["cold_start_s"] = round(time.time() - t0, 1)
                log("[remove-service] ready (cold start {:.1f}s, pod={})"
                    .format(st["cold_start_s"], pod))
                return True
        except Exception:
            pass
        time.sleep(2)
    raise RuntimeError("remove-service /health timed out after 120s")


def _remove_service_prewarm_async(cluster, globals_cfg, ts, cfg_name,
                                  dry=False, window_s=None):
    """Start the remove pod + warm store client in the background.

    Called at the kvtest measurement start so the ~40s client cold start is
    hidden under the (long) measured window; config-end remove then pays only
    the drain itself. A window shorter than _REMOVE_SVC_MIN_WINDOW_S cannot
    fit the cold start, so it is skipped (logged, not an error). Failures are
    recorded and remove stages fall back to the legacy per-stage path."""
    if dry or not _remove_service_enabled(cluster, globals_cfg):
        return
    if window_s is not None and float(window_s) < _REMOVE_SVC_MIN_WINDOW_S:
        log("[remove-service] skipped: measurement window {:.0f}s < {}s "
            "(cold start ~40s cannot finish before config-end remove); "
            "remove stages use the legacy path".format(
                float(window_s), _REMOVE_SVC_MIN_WINDOW_S))
        return
    st = cluster.setdefault("_remove_svc", {})
    if st.get("event") is not None:
        return
    st["event"] = threading.Event()

    def _run():
        try:
            _remove_service_ensure(cluster, globals_cfg, ts, cfg_name, dry)
        except StopRequested:
            st["ok"] = False
            st["error"] = "stopped"
        except Exception as exc:
            st["ok"] = False
            st["error"] = str(exc)
            log("[WARN] [remove-service] prewarm failed, remove stages fall "
                "back to legacy: {}".format(exc))
        finally:
            st["event"].set()

    threading.Thread(target=_run, name="remove-svc-prewarm",
                     daemon=True).start()


def _remove_service_call(cluster, globals_cfg, scope, when, timeout_s=120.0):
    """POST /remove to the warm service. None -> caller uses the legacy path.

    A SERVICE-reported failure (ok=false: RPC error / not drained) raises —
    that is a real stage failure, exactly like the client CLI exiting 1."""
    if not _remove_service_enabled(cluster, globals_cfg):
        return None
    st = (cluster or {}).get("_remove_svc") or {}
    ev = st.get("event")
    if ev is None:
        log_detail("[remove-service] {}: no prewarm event (not started); "
                   "legacy path".format(when))
        return None
    if not ev.wait(max(0.0, timeout_s)):
        log("[WARN] [remove-service] {}: not ready after {:.0f}s; legacy "
            "path".format(when, timeout_s))
        return None
    if not st.get("ok"):
        log("[WARN] [remove-service] {}: unavailable ({}); legacy path"
            .format(when, st.get("error") or "unknown"))
        return None
    entry = st.get("node")
    if not entry:
        return None
    try:
        max_passes = int(os.environ.get("MC_REMOVE_ALL_MAX_PASSES", "32"))
    except (TypeError, ValueError):
        max_passes = 32
    try:
        pass_delay = float(os.environ.get("MC_REMOVE_ALL_PASS_DELAY", "1.0"))
    except (TypeError, ValueError):
        pass_delay = 1.0
    payload = {"scope": scope, "max_passes": max(1, max_passes),
               "pass_delay": max(0.0, pass_delay)}
    cmd = ("curl -s --noproxy '*' --max-time {} -X POST "
           "-H 'Content-Type: application/json' --data {} "
           "http://127.0.0.1:{}/remove").format(
               int(timeout_s) + 300, shlex.quote(json.dumps(payload)),
               _REMOVE_SVC_PORT)
    try:
        raw = ssh_check_output(entry["ip"], entry, cmd, quiet=True)
        result = json.loads((raw or "").strip() or "{}")
    except Exception as exc:
        log("[WARN] [remove-service] {}: call failed ({}); legacy path"
            .format(when, exc))
        return None
    if not result.get("ok"):
        raise RuntimeError("remove-service {} failed: {}".format(
            when, result.get("error") or "unknown"))
    log("[remove-service] {}: cleared={} passes={} ({}s)".format(
        when, result.get("cleared"), result.get("passes"),
        result.get("elapsed_s")))
    return result


def _remove_service_cleanup(cluster):
    """Delete the remove Deployment (idempotent; namespace teardown兜底)."""
    st = (cluster or {}).get("_remove_svc") or {}
    name = st.get("deploy")
    c = (cluster or {}).get("_controller")
    if not name or not c:
        return
    k8s = k8s_settings(cluster)
    try:
        out, rc = controller_run(
            c, ["-n", k8s["namespace"], "delete", "deployment", name,
                "--ignore-not-found"], check=False)
        log_detail("[remove-service] cleanup {} rc={}".format(name, rc))
    except Exception as exc:
        log_detail("[remove-service] cleanup failed: {}".format(exc))


def _remove_keys_stage(cluster, globals_cfg, host, node, runner,
                       remote_log_dir, cfg_name, log_name, scope, when,
                       dry=False, pull_to=None, node_name=None, label=None,
                       tag=None):
    """The single orchestrator entry for every remove-keys execution.

    Wraps the gates around the one execution path:
      1. pre-check skip: store already clean -> skip outright (both scopes;
         never skipped when the metrics probe is unavailable).
      2. run_stage: one command/log composition for every call site.
      3. oplog release wait + terminal verify: full scope only — key-prefix
         names keys in a potentially shared store and can never drain it to
         zero, so it gets the pre-check but no wait.

    ``globals_cfg=None`` (fail-cleanup without a metrics config) degrades to
    a plain ungated stage. Returns True when the stage ran, False when the
    pre-check skipped it.
    """
    if label is None and tag:
        label = "[{}] remove-keys".format(tag)
    if dry:
        run_stage(host, node, runner, "remove-keys", remote_log_dir, log_name,
                  extra="--scope " + scope, dry_run=True, pull_to=pull_to,
                  cfg_name=cfg_name, node_name=node_name, label=label)
        return True
    if globals_cfg is not None and _remove_keys_precheck(
            cluster, globals_cfg, cfg_name, when):
        return False
    # run 级预热 remove 服务：热调用失败/不可用 -> 透明回退 legacy run_stage
    res = _remove_service_call(cluster, globals_cfg, scope, when)
    if res is not None:
        if scope == "full" and globals_cfg is not None:
            _wait_store_released(cluster, globals_cfg, cfg_name, when)
        return True
    run_stage(host, node, runner, "remove-keys", remote_log_dir, log_name,
              extra="--scope " + scope, dry_run=False, pull_to=pull_to,
              cfg_name=cfg_name, node_name=node_name, label=label)
    if scope == "full" and globals_cfg is not None:
        _wait_store_released(cluster, globals_cfg, cfg_name, when)
    return True


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


_DEFAULT_KEY_SPACE_RATIO = 0.9


def _pipeline_key_space_ratio(globals_cfg):
    """Fraction of the effective store capacity the pipeline may fill
    (global.json `pipeline.key_space_ratio`, default 0.9). Must be in (0,1);
    an out-of-range/non-numeric value raises (a silent fallback could
    overfill the store)."""
    raw = (globals_cfg.get("pipeline") or {}).get("key_space_ratio")
    if raw in (None, ""):
        return _DEFAULT_KEY_SPACE_RATIO
    try:
        r = float(raw)
    except (TypeError, ValueError):
        raise RuntimeError(
            "pipeline.key_space_ratio must be a number in (0,1), got {!r}"
            "".format(raw))
    if not (0.0 < r < 1.0):
        raise RuntimeError(
            "pipeline.key_space_ratio must be in (0,1), got {}".format(r))
    return r


def _pipeline_key_budget(globals_cfg, writer_count, ds_bytes, batch_size,
                         ds_label):
    """Derive the PER-WRITER key-space cap purely from the store capacity.

    Pipeline mode has NO key_count parameter: the writable key space is
    always floor(store x ratio / data_size / writers), where `store` comes
    from global.json store_total_size (explicit) or the master-metrics
    auto-discovery, and `ratio` is global.json pipeline.key_space_ratio
    (default 0.9). The headroom covers object metadata + in-flight batches.
    Each writer wraps inside its cap (put family: untimed delete-before-write;
    upsert_from: prefill once, then overwrite).

    Missing store source is a HARD error: without a store capacity the
    writable amount cannot be derived and an unbounded run would exhaust the
    store (observed: hidden 1000-key default -> 250GB attempted).
    """
    store = _store_total_bytes(globals_cfg)   # effective (min rule)
    if store is None:
        raise RuntimeError(
            "pipeline requires the store capacity to size the write key "
            "space: set global.json 'store_total_size' (e.g. \"20GB\") "
            "and/or set global.json 'master_metrics_url' to the master "
            "metrics endpoint (full URL including the port, e.g. "
            "http://<svc>.<ns>.svc.cluster.local:<port>/metrics)")
    ratio = _pipeline_key_space_ratio(globals_cfg)
    usable = int(store * ratio)
    per = usable // (ds_bytes * writer_count) if ds_bytes and writer_count else 0
    if per < max(1, int(batch_size or 1)):
        raise RuntimeError(
            "pipeline key space too small: store {}B x {} / ({}B x {} "
            "writers) = {} keys < batch_size {}; the store is too small "
            "for this data size ({})".format(
                store, ratio, ds_bytes, writer_count, per,
                int(batch_size or 1), ds_label))
    return per


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

    Per-node write QPS = base_qps * qps_multiple. It is split evenly across
    the writers/dual clients on that node, and cluster total is per-node QPS
    times N. Writers get global gids 1..W and readers global indices 0..R-1
    in node order. The plan carries the node SLOT; the caller materializes
    concrete node names (ssh cluster nodes / k8s pod names) into entry["node"].
    The reader pool must cover notify_count.

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
    notify_async = writer_cfg.get("notify_async", False)
    if not isinstance(notify_async, bool):
        raise RuntimeError("writer.notify_async must be a JSON boolean")
    notify_max_inflight = int(writer_cfg.get("notify_max_inflight", 256))
    if notify_max_inflight < 1:
        raise RuntimeError("writer.notify_max_inflight must be >= 1")
    notify_queue_size = int(writer_cfg.get("notify_queue_size", notify_max_inflight))
    if notify_queue_size < 1:
        raise RuntimeError("writer.notify_queue_size must be >= 1")
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
            "notify_async": notify_async,
            "notify_max_inflight": notify_max_inflight,
            "notify_queue_size": notify_queue_size,
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
        "notify_async": notify_async,
        "notify_max_inflight": notify_max_inflight,
        "notify_queue_size": notify_queue_size,
        "reader_threads": int(reader_cfg.get("num_threads", 1)),
        "writer_port_base": w_base,
        "reader_port_base": r_base,
    }


# --------------------------------------------------------------------------
# kvtest planning (C++ mooncake_kvtest binary; same config shape as pipeline)
# --------------------------------------------------------------------------

# our pipeline-style operation names -> kvtest-native names. The binary
# normalizes both forms, but writing the native form keeps summary.json op
# keys stable for the CSV aggregation.
KVTEST_WRITER_OPS = {
    "put": "upsert",
    "upsert": "upsert",
    "setStringView": "upsert",
    "put_batch": "batch_upsert_from",
    "batch_upsert_from": "batch_upsert_from",
    "batchUpsertFrom": "batch_upsert_from",
    "mSet": "batch_upsert_from",
    "put_from": "upsert_from",
    "upsert_from": "upsert_from",
    "upsertFrom": "upsert_from",
    "remove": "remove",
    "del": "remove",
}
KVTEST_READER_OPS = {
    "get": "get_buffer",
    "get_batch": "batch_get_buffer",
    "batch_get_buffer": "batch_get_buffer",
    "batchGetBuffer": "batch_get_buffer",
    "mGet": "batch_get_buffer",
    "get_into": "get_buffer",
    "get_buffer": "get_buffer",
    "getBuffer": "get_buffer",
    "exist": "exist",
    "isExist": "exist",
}


def _kvtest_artifact_prefix(cfg_name):
    """Short, stable per-config prefix for remote/local kvtest artifacts.

    The full config name already appears in the parent local directory. K8s
    pod names can be 80+ characters, so repeating config+pod in every file
    exceeded Windows MAX_PATH before plotting even started.
    """
    digest = hashlib.sha1(cfg_name.encode("utf-8")).hexdigest()[:10]
    return "kv" + digest


def _kvtest_artifact_base(cfg_name, iid, port, data_size):
    return "{}.i{}_p{}.kvtest_{}".format(
        _kvtest_artifact_prefix(cfg_name), iid, port, data_size)


def _kvtest_writer_operation(cfg):
    return ((cfg.get("writer", {}) or {}).get("operation") or "upsert_from")


def _kvtest_reader_operation(cfg):
    return ((cfg.get("reader", {}) or {}).get("operation") or "get_buffer")

# runner-script marker for pid liveness probes of kvtest instances (the
# ledger pid is the run_kvtest.sh bash wrapper; see _PIPELINE_PID_MARKER)
_KVTEST_PID_MARKER = "run_kvtest.sh"


def kvtest_plan(cfg, node_count=1):
    """Build the kvtest process plan on top of the pipeline topology.

    A ``client`` block normally means one dual-role process per client.  The
    kvtest-only ``client.process_mode=split`` variant keeps the same client
    pod count but creates a writer and a reader process inside every pod.
    Without a ``client`` block, writer and reader processes remain separately
    deployed as in pipeline mode.

    kvtest namespaces keys and notify senders by ``instance_id``, so every
    actual process gets a cluster-global id.  Dual clients take 0..C-1;
    separate/co-located-split writers take 0..W-1 and readers continue at W.
    """
    plan = pipeline_plan(cfg, node_count=node_count)
    writer_cfg = cfg.get("writer", {}) or {}
    reader_cfg = cfg.get("reader", {}) or {}
    plan["writer_threads"] = max(1, int(writer_cfg.get("num_threads", 1) or 1))
    if plan.get("dual"):
        client_cfg = cfg.get("client", {}) or {}
        process_mode = str(client_cfg.get("process_mode", "dual") or
                           "dual").strip().lower()
        if process_mode not in ("dual", "split"):
            raise RuntimeError(
                "kvtest client.process_mode must be dual or split (got {})"
                "".format(process_mode))
        plan["process_mode"] = process_mode
        if process_mode == "dual":
            for c in plan["clients"]:
                c["iid"] = c["id"]
        else:
            writer_base = int(writer_cfg.get("port_base", 9101))
            reader_base = int(reader_cfg.get("port_base", 9201))
            if writer_base == reader_base:
                raise RuntimeError(
                    "kvtest split mode requires different writer.port_base "
                    "and reader.port_base")
            clients = plan["clients"]
            writers = []
            readers = []
            for index, client in enumerate(clients):
                writer = dict(client)
                writer.update({
                    "iid": index,
                    "pair_index": index,
                    "port": writer_base + client["local"],
                })
                reader = dict(client)
                reader.update({
                    "iid": len(clients) + index,
                    "pair_index": index,
                    "port": reader_base + client["local"],
                })
                writers.append(writer)
                readers.append(reader)
            plan.update({
                "dual": False,
                "split_pod": True,
                "writers": writers,
                "readers": readers,
                "writer_port_base": writer_base,
                "reader_port_base": reader_base,
            })
    else:
        plan["process_mode"] = "separate"
        base = len(plan["writers"])
        for i, w in enumerate(plan["writers"]):
            w["iid"] = i
        for r in plan["readers"]:
            r["iid"] = base + r["index"]
    return plan


def _kvtest_split_qps(per_node_qps, cpn, local):
    """Integer QPS split of one node's rate across its cpn instances
    (105 across 2 -> 53 + 52, mirroring the binary's own thread split)."""
    total = max(0, int(per_node_qps))
    cpn = max(1, int(cpn))
    base, rem = divmod(total, cpn)
    return base + (1 if local < rem else 0)


def _kvtest_numa_node(cfg, entry, role):
    """Return an explicit per-process NUMA node from kvtest.numa_bindings."""
    bindings = (cfg.get("kvtest", {}) or {}).get("numa_bindings")
    if bindings is None:
        return None
    if role not in ("writer", "reader"):
        raise RuntimeError(
            "kvtest.numa_bindings only supports split writer/reader roles")
    if not isinstance(bindings, list):
        raise RuntimeError("kvtest.numa_bindings must be a list")
    local = int(entry.get("local", -1))
    if local < 0 or local >= len(bindings):
        raise RuntimeError(
            "kvtest.numa_bindings has no mapping for client local {}"
            .format(local))
    binding = bindings[local]
    if not isinstance(binding, dict) or role not in binding:
        raise RuntimeError(
            "kvtest.numa_bindings[{}] must define {}".format(local, role))
    node = binding[role]
    if isinstance(node, bool) or not isinstance(node, int) or node < 0:
        raise RuntimeError(
            "kvtest.numa_bindings[{}].{} must be a non-negative integer"
            .format(local, role))
    return node


def _kvtest_warmup_duration(cfg, args):
    if getattr(args, "no_warmup", False):
        return 0.0
    value = float((cfg.get("warmup", {}) or {}).get("duration", 0) or 0)
    if value < 0:
        raise RuntimeError("kvtest warmup.duration must be >= 0")
    return value


# --------------------------------------------------------------------------
# shared orchestration infrastructure (both scenario families)
# --------------------------------------------------------------------------

def apply_cli_overrides(cfg, args):
    # --protocol has the highest priority and overrides the JSON config value
    if getattr(args, "protocol", None):
        cfg["protocol"] = args.protocol


def scenario_paths(globals_cfg, ts, cfg_name, local_result_dir=None):
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
    # `ts` is also used as the remote/K8s/etcd run identifier.  Parallel
    # rank+recall flattens its nested result path into that identifier (for
    # example parent/rank -> parent-rank), so it must not be reused to derive
    # the local filesystem path.  The caller passes the real result directory
    # while standalone runs keep the historical outputs/<ts> fallback.
    result_dir = local_result_dir or os.path.join(RESULTS_DIR, ts)
    local_log_dir = os.path.join(result_dir, "logs", cfg_name)
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


def _svc_ns_from_host(host):
    """Parse a k8s Service DNS name -> (svc, ns), else (None, None).

    Accepted forms: <svc>.<ns>.svc.cluster.local / <svc>.<ns>.svc /
    <svc>.<ns> (2-part names are ambiguous; callers try svc.ns first)."""
    parts = (host or "").split(".")
    # <svc>.<ns>.svc.cluster.local (standard k8s service FQDN)
    if len(parts) >= 4 and parts[-1] == "local" \
            and parts[-2] == "cluster" and parts[-3] == "svc":
        return parts[0], parts[1]
    # <svc>.<ns>.svc
    if len(parts) >= 3 and parts[-1] == "svc":
        return parts[0], parts[1]
    if len(parts) == 2:
        return parts[0], parts[1]
    return None, None


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
    cache = cluster.setdefault("_dns_cache", {}) if cluster is not None else {}
    if host in cache:
        log_detail("[k8s] dns: {} -> {} (cached)".format(host, cache[host]))
        return cache[host]
    svc, ns = _svc_ns_from_host(host)
    if svc:
        k8s = cluster.get("k8s") or {}
        ip = _resolve_k8s_svc(svc, ns, k8s.get("kubecontext", ""),
                              cluster.get("_controller"))
        if ip:
            cache[host] = ip
            log("[k8s] dns: {} -> {} (kubectl svc {})".format(host, ip, svc))
            return ip
    try:
        infos = socket.getaddrinfo(host, None, socket.AF_INET)
        ip = infos[0][4][0]
        cache[host] = ip
        log("[k8s] dns: {} -> {} (resolver)".format(host, ip))
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
    # hostPath root for on-node log persistence (run_id/pod_name subdirs);
    # overridable per cluster, defaults to /var/lib/mooncake-bench
    k8s.setdefault("host_log_root", "/var/lib/mooncake-bench")
    return k8s


def host_log_dir(cluster, ts):
    """Per-run hostPath directory mounted at the pod's logs/<ts> path."""
    return "{}/{}".format(k8s_settings(cluster)["host_log_root"].rstrip("/"), ts)


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
    log("[k8s] workspace created: {}:{} ({})".format(
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
    log_detail("[k8s] pre-run upload: {} -> {}:{}".format(
        os.path.relpath(script, ROOT), controller_desc(c), remote_path))
    # Parallel rank+recall children share this controller workspace and both
    # install this file at startup. Uploading straight to the shared name is a
    # real race: SFTP opens with O_TRUNC, so a peer's truncate landing between
    # our write and paramiko's confirm-stat fails with
    # "size mismatch in put!  0 != N" (seen intermittently on real clusters).
    # Upload to a unique sibling name and atomically mv it into place: same
    # content, last rename wins, no truncation window on the shared path.
    tmp_dir = tempfile.mkdtemp(prefix="mc-prerun-")
    try:
        tmp_name = "k8s-prerun.sh.{}.{}".format(
            os.getpid(), uuid.uuid4().hex[:8])
        tmp_local = os.path.join(tmp_dir, tmp_name)
        shutil.copy2(script, tmp_local)
        controller_put(c, [tmp_local], k8s["controller_workspace"])
        tmp_remote = posixpath.join(k8s["controller_workspace"], tmp_name)
        _out, _rc = controller_exec(c, "mv -f {} {}".format(
            shlex.quote(tmp_remote), shlex.quote(remote_path)))
        if _rc != 0:
            raise RuntimeError(
                "k8s pre-run script install failed (rc={}): {}".format(
                    _rc, (_out or "").strip()[:300]))
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)
    log("[k8s] pre-run: bash {}".format(remote_path))
    t0 = time.time()
    out, rc = controller_exec(c, "bash " + remote_path)
    log("[k8s] pre-run: rc={} ({:.1f}s)".format(
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
    log("[k8s] namespace created: {}".format(ns))
    log_detail("[k8s] pods:     {} get pods -o wide".format(" ".join(
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
        "{{MC_HOST_LOG_DIR}}": host_log_dir(cluster, ts),
        "{{MC_LOG_MOUNT}}": k8s["pod_workspace"] + "/logs/" + ts,
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
            log_detail("[k8s] pod manifest: template={} -> {}:{}".format(
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
            log_detail("[k8s] describe: {} describe pod {}".format(" ".join(
                kubectl_prefix({"_ctx": c.get("_ctx", ""), "_ns": ns})), pod))
        finally:
            shutil.rmtree(local_dir, ignore_errors=True)
        ip, k8s_node = wait_k8s_pod_ready(cluster, pod)
        log("[k8s] pod {} ready (ns={}, ip={}, node={}, {:.1f}s)".format(
            pod, ns, ip, k8s_node or "?", time.time() - t0))
        log_detail("[k8s] shell:    {} exec -it {} -- bash".format(
            " ".join(kubectl_prefix({"_ctx": c.get("_ctx", ""), "_ns": ns})),
            pod))
    host_ip = "" if dry_run else _k8s_pod_host_ip(c, ns, pod)
    node = {"kind": "k8s-pod", "_pod": pod, "_ns": ns,
            "_ctx": c.get("_ctx", ""), "_controller": c,
            "_controller_workspace": k8s["controller_workspace"],
            "_run_label": run_label, "_role": "client-0",
            "ip": ip, "fabric_ip": ip, "host_ip": host_ip}
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
                 ("run_kvtest.sh", SCRIPTS_DIR),
                 ("mooncake_benchmark_client.py", SCRIPTS_DIR),
                 ("bench_etcd.py", SCRIPTS_DIR),
                 ("logstats.py", SCRIPTS_DIR),
                 ("tracestats.py", SCRIPTS_DIR),
                 ("remove_service.py", SCRIPTS_DIR),
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
        log("[k8s] upload scripts {} files (run={}) -> {}:{}".format(
            len(staged), run_label, controller_desc(c), stage_remote))
        for fname in staged:
            log_detail("[k8s]   + {}".format(fname))
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
    log("[k8s] configmap {} created *{}".format(cm_name, len(staged)))
    log_detail("[k8s] configmap {} keys: {}".format(cm_name, ",".join(staged)))
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
            log_detail("[k8s] pod {}: scripts via configmap (ro at {}), "
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
                      os.path.join(SCRIPTS_DIR, "run_kvtest.sh"),
                      os.path.join(SCRIPTS_DIR, "mooncake_benchmark_client.py"),
                      os.path.join(SCRIPTS_DIR, "bench_etcd.py"),
                      os.path.join(SCRIPTS_DIR, "logstats.py"),
                      os.path.join(SCRIPTS_DIR, "tracestats.py")]
        if os.path.exists(mooncake_env_path):
            push_files.append(mooncake_env_path)
        scp_push(n["ip"], n, push_files, remote_scripts, dry_run=dry_run)
        run_ssh(n["ip"], n, "chmod +x {} {} {}".format(
            remote_scripts + "/env.sh",
            remote_scripts + "/run_benchmark_basic.sh",
            remote_scripts + "/run_benchmark_pipeline.sh"), dry_run=dry_run)
        run_ssh(n["ip"], n, "chmod +x {}".format(
            remote_scripts + "/run_kvtest.sh"), dry_run=dry_run)


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


# Small-run threshold: below this total raw-log size the orchestrator pulls
# the raw logs as well (stats-only above it — raw stays on the nodes).
RAW_AUTO_PULL_MAX = 100 * 1024 ** 2

_TRACE_MOD = [None]


def _trace_module():
    if _TRACE_MOD[0] is None:
        import importlib.util as _ilu
        spec = _ilu.spec_from_file_location(
            "mc_tracestats", os.path.join(SCRIPTS_DIR, "tracestats.py"))
        mod = _ilu.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _TRACE_MOD[0] = mod
    return _TRACE_MOD[0]


def _map_timeout():
    """Per-file timeout for the in-pod map stage (generous: a GB glog costs
    tens of seconds). MC_MAP_TIMEOUT overrides, 0 disables."""
    try:
        return float(os.environ.get("MC_MAP_TIMEOUT", "900") or 0)
    except ValueError:
        return 900.0


def _hifreq_sample_rate():
    """MC_HIFREQ_LOG_SAMPLE_RATE from scripts/env.mooncake.sh (the same file
    staged to every node), default 0.1 when absent. Only used to annotate the
    report (sample count != request count)."""
    path = os.path.join(SCRIPTS_DIR, "env.mooncake.sh")
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                m = re.match(r"\s*export\s+MC_HIFREQ_LOG_SAMPLE_RATE=(\S+)",
                             line)
                if m:
                    return float(m.group(1).strip("'\""))
    except (OSError, ValueError):
        pass
    return 0.1


def _tracestats_cmd(py, remote_scripts, inputs, node, cluster, node_name,
                    role, size, sample_rate, remote_path, warmup_s=None):
    cmd = "{} {} {}".format(
        shlex.quote(py),
        shlex.quote(remote_scripts + "/tracestats.py"),
        " ".join("--log {}".format(shlex.quote(p)) for p in inputs))
    if node_name:
        cmd += " --node {}".format(shlex.quote(
            str(_node_display(cluster, node, node_name))))
    if node.get("_pod"):
        cmd += " --pod {}".format(shlex.quote(str(node["_pod"])))
    if role:
        cmd += " --role {}".format(shlex.quote(str(role)))
    if size:
        cmd += " --size {}".format(shlex.quote(str(size)))
    if sample_rate is not None:
        cmd += " --sample-rate {}".format(sample_rate)
    if warmup_s is not None and float(warmup_s) > 0:
        cmd += " --warmup-s {}".format(float(warmup_s))
    # 报表索引里的路径 = 节点侧真实位置（ssh 后可读），不是 pod 内路径
    if remote_path:
        cmd += " --remote-path {}".format(shlex.quote(remote_path))
    return cmd


def _run_map_cmd(cluster, node, cmd, dry):
    """Run one map command inside the pod / on the node. True on success."""
    if dry:
        log_detail("[map] (dry-run) {} {}".format(node.get("ip"), cmd))
        return True
    if node.get("_pod"):
        ns = node.get("_ns") or k8s_settings(cluster)["namespace"]
        o, rc = controller_run(
            cluster["_controller"],
            ["-n", ns, "exec", node["_pod"], "--", "bash", "-c", cmd],
            check=False, timeout=_map_timeout() or None)
        if rc != 0:
            log("[WARN] [map] tracestats failed on pod {}: {}".format(
                node["_pod"], (o or "").strip()[:200]))
            return False
        return True
    try:
        ssh_check_output(node["ip"], node, cmd, dry_run=dry,
                         quiet=True, timeout=_map_timeout() or None)
        return True
    except Exception as exc:
        log("[WARN] [map] tracestats failed on {}: {}".format(
            node.get("ip"), exc))
        return False


def run_tracestats_map(cluster, node, glog_paths, remote_scripts, globals_cfg,
                       dry=False, node_name=None, role=None, size=None,
                       sample_rate=None, remote_path=None, out_path=None,
                       warmup_s=None):
    """map 阶段（C++ trace）：在节点/pod 内执行 tracestats，只回传统计产物。

    out_path given -> ALL inputs (glog rotated segments) merge into ONE
    product there (kvtest/pipeline discovery path); otherwise legacy per-file
    products <glog>.trace.json."""
    py = resolve_venv(cluster, globals_cfg) or "python3"
    if out_path is not None:
        if not glog_paths:
            return []
        cmd = _tracestats_cmd(py, remote_scripts, glog_paths, node, cluster,
                              node_name, role, size, sample_rate,
                              remote_path or _pod_path_to_site(
                                  cluster, out_path), warmup_s)
        cmd += " --out {}".format(shlex.quote(out_path))
        t0 = time.time()
        ok = _run_map_cmd(cluster, node, cmd, dry)
        if not dry:
            log_detail("[map] tracestats {} ({} segment(s)) {:.1f}s".format(
                os.path.basename(out_path), len(glog_paths),
                time.time() - t0))
        return [out_path] if ok else []
    out = []
    # NOTE: the loop variable must NOT be named `log` (it would shadow the
    # module-level log() used by the failure branches below).
    for log_path in glog_paths:
        trace_file = log_path + ".trace.json"
        cmd = _tracestats_cmd(py, remote_scripts, [log_path], node, cluster,
                              node_name, role, size, sample_rate,
                              remote_path or _pod_path_to_site(
                                  cluster, trace_file), warmup_s)
        t0 = time.time()
        ok = _run_map_cmd(cluster, node, cmd, dry)
        if not dry:
            log_detail("[map] tracestats {} {:.1f}s".format(
                os.path.basename(log_path), time.time() - t0))
        if ok:
            out.append(trace_file)     # never request a file that wasn't made
    return out


def _resolve_glog_inputs(cluster, node_entry, glog_dir, legacy_file,
                         dry=False):
    """Resolve one process's glog inputs for the trace map.

    MC_LOG_DIR splits glog into <prog>.<host>.<user>.log.INFO.<date>-<time>.
    <pid> files inside a per-process directory (glog_compat initializes glog
    from MC_LOG_DIR) and glog ROTATES at max_log_size — the full stream is
    therefore the set of regular *.INFO.* files there (symlinks excluded).
    Old images / no MC_LOG_DIR keep the legacy stderr capture file.

    Returns (files, site_path). site_path is the NODE-side location the report
    index shows (the segment DIRECTORY when MC_LOG_DIR is in use, the file
    otherwise); ([], "") means this process produced no glog."""
    if dry:
        return [legacy_file], _pod_path_to_site(cluster, legacy_file)
    cmd = ("F=$(find {d} -maxdepth 1 -type f -name '*.INFO.*' 2>/dev/null "
           "| sort); if [ -z \"$F\" ] && [ -f {f} ]; then F={f}; fi; "
           "printf '%s\\n' \"$F\"").format(
               d=shlex.quote(glog_dir), f=shlex.quote(legacy_file))
    try:
        out = ssh_check_output(node_entry["ip"], node_entry, cmd, quiet=True)
    except Exception:
        out = ""
    files = [ln.strip() for ln in str(out or "").splitlines() if ln.strip()]
    if not files:
        return [], ""
    if files[0] == legacy_file:
        return files, _pod_path_to_site(cluster, legacy_file)
    return files, _pod_path_to_site(cluster, glog_dir)


def run_logstats_map(cluster, node, log_paths, remote_scripts, globals_cfg,
                     dry=False, node_name=None, role=None, size=None,
                     roles=None):
    """map 阶段：在节点/pod 内对每个 RESULT 日志执行 logstats，就地生成
    <log>.stats.json（编排器默认只回传统计产物，原始日志留在节点上）。
    产物 meta 里写入 node/pod/role/远程路径，供报表分节点统计与现场索引。"""
    py = resolve_venv(cluster, globals_cfg) or "python3"
    stats = []
    for log_path in log_paths:
        stats_file = log_path + ".stats.json"
        cmd = "{} {} --log {}".format(
            shlex.quote(py),
            shlex.quote(remote_scripts + "/logstats.py"),
            shlex.quote(log_path))
        if node_name:
            cmd += " --node {}".format(shlex.quote(
                str(_node_display(cluster, node, node_name))))
        if node.get("_pod"):
            cmd += " --pod {}".format(shlex.quote(str(node["_pod"])))
        file_role = (roles or {}).get(log_path, role)
        if file_role:
            cmd += " --role {}".format(shlex.quote(str(file_role)))
        if size:
            cmd += " --size {}".format(shlex.quote(str(size)))
        # 报表索引里的路径 = 节点侧真实文件（ssh 后可读），不是 pod 内路径
        cmd += " --remote-path {}".format(shlex.quote(
            _pod_path_to_site(cluster, log_path)))
        ok = True
        t0 = time.time()
        if dry:
            log_detail("[map] (dry-run) {} {}".format(node.get("ip"), cmd))
        elif node.get("_pod"):
            ns = node.get("_ns") or k8s_settings(cluster)["namespace"]
            out, rc = controller_run(
                cluster["_controller"],
                ["-n", ns, "exec", node["_pod"], "--", "bash", "-c", cmd],
                check=False, timeout=_map_timeout() or None)
            if rc != 0:
                ok = False
                log("[WARN] [map] logstats failed on pod {}: {}".format(
                    node["_pod"], (out or "").strip()[:200]))
        else:
            try:
                ssh_check_output(node["ip"], node, cmd, dry_run=dry,
                                 quiet=True, timeout=_map_timeout() or None)
            except Exception as exc:
                ok = False
                log("[WARN] [map] logstats failed on {}: {}".format(
                    node.get("ip"), exc))
        if not dry:
            log_detail("[map] logstats {} {:.1f}s".format(
                os.path.basename(log_path), time.time() - t0))
        if ok:
            stats.append(stats_file)   # never request a file that wasn't made
    return stats


def _map_and_pull_logs(cluster, groups, local_log_dir, dry, globals_cfg,
                       remote_scripts, pull_raw=False, size=None):
    """map（远端 logstats）→ 只拉 *.stats.json（pull_raw 时连原始日志一起拉）。
    groups: [(node, [log paths], label, node_display_name)]。
    返回 {node_display_name: 原始日志字节}（从 stats.meta.bytes 汇总）。"""
    node_bytes = {}
    sample_rate = _hifreq_sample_rate()
    for node, files, label, name in groups:
        # files entries are either a path (legacy) or (path, role): the role
        # is authoritative (filename sniffing fails for .writer_<node>_w0.log
        # and mislabels a dual client's reader log as writer).
        entries = [(f, None) if not isinstance(f, tuple) else (f[0], f[1])
                   for f in files]
        paths = [f for f, _r in entries]
        roles = {f: r for f, r in entries if r}
        result_logs = [f for f in paths if not f.endswith(".glog")]
        role = None
        if not roles and result_logs:
            base = os.path.basename(result_logs[0])
            for _r in ("writer", "reader"):
                if ".{}.log".format(_r) in base:
                    role = _r
                    break
        stats = run_logstats_map(cluster, node, result_logs, remote_scripts,
                                 globals_cfg, dry=dry, node_name=name,
                                 role=role, size=size, roles=roles)
        # glog 来源：每个客户端的 MC_LOG_DIR 目录 <base>.glog.d/*.INFO.*
        # （旧镜像回退 <base>.glog 的 stderr 捕获）；轮转多段合并成一个产物。
        traces = []
        glog_inputs = []
        for _lp in result_logs:
            if not _lp.endswith(".log"):
                continue
            _base = _lp[:-len(".log")]
            _files, _site = _resolve_glog_inputs(
                cluster, node, _base + ".glog.d", _base + ".glog", dry=dry)
            if not _files:
                continue
            traces += run_tracestats_map(
                cluster, node, _files, remote_scripts, globals_cfg, dry=dry,
                node_name=name, role=roles.get(_lp) or role, size=size,
                sample_rate=sample_rate, remote_path=_site,
                out_path=_base + ".trace.json")
            if pull_raw:
                glog_inputs.extend(_files)
        pull_files = stats + traces + (paths if pull_raw else []) + glog_inputs
        _pipeline_pull_parallel(
            cluster, [(node, pull_files, label)], local_log_dir, dry)
        if not dry and pull_raw:
            node_bytes[name] = sum(
                os.path.getsize(os.path.join(local_log_dir,
                                             os.path.basename(f)))
                for f in paths
                if os.path.isfile(os.path.join(local_log_dir,
                                               os.path.basename(f))))
        elif not dry:
            # raw-log volume = python logs (stats.meta.bytes) + SDK glog
            # (trace.meta.bytes): deciding on the .log alone pulled GB-sized
            # glogs back on "small" runs
            total = 0
            for sf in list(stats) + list(traces):
                lp = os.path.join(local_log_dir, os.path.basename(sf))
                if os.path.isfile(lp):
                    try:
                        with open(lp, "r", encoding="utf-8") as f:
                            total += json.load(f).get("meta", {}).get(
                                "bytes", 0)
                    except (OSError, ValueError):
                        pass
            node_bytes[name] = total
    # Small runs: pull the raw logs too (cheap) so the report can offer full
    # per-point inspection (scatter layer + exact excerpts). Large runs stay
    # stats-only (raw logs remain on the nodes).
    if not dry and not pull_raw:
        total_bytes = sum(node_bytes.values())
        if 0 < total_bytes <= RAW_AUTO_PULL_MAX:
            log_detail("[map] raw logs {} -> pulling raw too (<= {})".format(
                _fmt_gib(total_bytes), _fmt_gib(RAW_AUTO_PULL_MAX)))
            for node, files, label, name in groups:
                _raw = [(f, None) if not isinstance(f, tuple) else f
                        for f in files]
                _pipeline_pull_parallel(
                    cluster, [(node, [f for f, _r in _raw], label)],
                    local_log_dir, dry)
    return node_bytes


def _log_node_volumes(node_bytes, what):
    """Per-node raw-log volume line (user requirement: every run must show
    how much log each node produced)."""
    if not node_bytes:
        return
    parts = ["{}={}".format(n, _fmt_gib(b)) for n, b in
             sorted(node_bytes.items()) if b]
    if parts:
        log("[stats] {} log volume: {}".format(what, ", ".join(parts)))


_REPORT_MOD = [None]
_REPORT_ACC = []      # [{data, report_html}] accumulated for the run overview


def _report_module():
    if _REPORT_MOD[0] is None:
        import importlib.util as _ilu
        spec = _ilu.spec_from_file_location(
            "mc_report", os.path.join(SCRIPTS_DIR, "report.py"))
        mod = _ilu.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _REPORT_MOD[0] = mod
    return _REPORT_MOD[0]


_LOG_SIZE_RE = re.compile(r"_(\d+(?:\.\d+)?)(KB|MB|GB)(?=[._]|$)", re.I)
_LOG_ROLE_RE = re.compile(r"\.(?:dual|writer|reader)_([^/]*?)_(?:c|w|r)\d+"
                          r"(?:_p\d+)?(?:\.(?:writer|reader))?\.log$")


def _derive_log_meta(path, local_log_dir):
    """Best-effort provenance for a LOCALLY recomputed log/glog.

    Raw logs pulled on the failure/stop path have no map-produced stats, so
    the recompute used to lose size/role/node: every data_size collapsed into
    "all", role tabs vanished and --rebuild-report misread pipeline runs as
    basic. Derive what the filename/layout carries."""
    base = os.path.basename(str(path))
    size = None
    stem = base[:-4] if base.endswith(".log") else (
        base[:-5] if base.endswith(".glog") else base)
    m = _LOG_SIZE_RE.search(stem)
    if m:
        size = m.group(1) + m.group(2).upper()
    role = None
    if ".writer.log" in base or re.search(r"\.writer_[^/]*\.log$", base):
        role = "writer"
    elif ".reader.log" in base or re.search(r"\.reader_[^/]*\.log$", base):
        role = "reader"
    node = None
    m = _LOG_ROLE_RE.search(base)
    if m:
        node = m.group(1)
    pod = None
    try:
        rel = os.path.relpath(str(path), str(local_log_dir))
        if rel and not rel.startswith("..") and os.sep in rel:
            pod = rel.split(os.sep)[0] or None
    except ValueError:
        pass
    return {"size": size, "role": role, "node": node, "pod": pod}


def _is_client_log(name):
    """True for measured client logs (per role/combo), false for stage logs."""
    if not name.endswith(".log"):
        return False
    if any(k in name for k in (".ylt.", ".nohup", ".glog", ".exitrc")):
        return False
    if name.endswith(".err.log"):    # mem/urma 采样器错误日志（profiling 产物）
        return False
    if any(k in name for k in (".generate-data.", ".remove-keys",
                               ".prepare-store.")):
        return False
    if ".kvtest_" in name:
        # C++ kvtest instance logs carry no RESULT rows; the map for that
        # scenario is built from <...>.samples.csv (see _kvtest_map_stats)
        return False
    return True


def _collect_local_stats(local_log_dir):
    """Stats for every client log available locally: pulled remote stats are
    used as-is; raw-only logs get stats computed locally (small-run auto
    pull / --png / legacy runs). Returns sorted stats paths."""
    stats = []
    for root, dirs, files in os.walk(local_log_dir):
        # 监控产物（profiling/）与报表目录不是客户端日志：剪枝，避免把
        # mem.err.log 之类当作客户端生成幽灵数据档
        dirs[:] = [d for d in dirs if d not in ("profiling", "report")]
        for fn in files:
            if fn.endswith(".stats.json"):
                stats.append(os.path.join(root, fn))
    have = set(stats)
    for root, dirs, files in os.walk(local_log_dir):
        dirs[:] = [d for d in dirs if d not in ("profiling", "report")]
        for fn in sorted(files):
            if not _is_client_log(fn):
                continue
            p = os.path.join(root, fn)
            sp = p + ".stats.json"
            if sp in have:
                continue
            try:
                mi = _derive_log_meta(p, local_log_dir)
                st = _logstats_module().compute_stats(
                    p, size=mi["size"], role=mi["role"], node=mi["node"],
                    pod=mi["pod"], remote_path=p)
                st["meta"]["recomputed"] = True
                with open(sp, "w", encoding="utf-8") as f:
                    json.dump(st, f, ensure_ascii=False,
                              separators=(",", ":"))
                stats.append(sp)
            except Exception as exc:
                log_detail("[report] local stats failed for {}: {}".format(
                    fn, exc))
    return sorted(set(stats))


def _collect_local_traces(local_log_dir):
    """Trace stats available locally: pulled *.trace.json, or computed
    locally from raw .glog (small-run auto pull / --png / rebuild)."""
    traces = []
    for root, _dirs, files in os.walk(local_log_dir):
        for fn in sorted(files):
            if fn.endswith(".trace.json"):
                traces.append(os.path.join(root, fn))
    have = set(traces)
    for root, _dirs, files in os.walk(local_log_dir):
        for fn in sorted(files):
            if not fn.endswith(".glog"):
                continue
            p = os.path.join(root, fn)
            tp = p + ".trace.json"
            if tp in have:
                continue
            try:
                mi = _derive_log_meta(p, local_log_dir)
                st = _trace_module().compute_trace_stats(
                    p, sample_rate=_hifreq_sample_rate(), size=mi["size"],
                    role=mi["role"], node=mi["node"], pod=mi["pod"])
                st["meta"]["recomputed"] = True
                with open(tp, "w", encoding="utf-8") as f:
                    json.dump(st, f, ensure_ascii=False,
                              separators=(",", ":"))
                traces.append(tp)
            except Exception as exc:
                log_detail("[report] local trace stats failed for {}: {}"
                           .format(fn, exc))
    return sorted(set(traces))


def _build_report_params(cfg, meta, nodes=0):
    """Flatten scenario config + run context into an ordered [[label, value]]
    list for the report's 参数配置 section. Empty fields are omitted."""
    meta = meta or {}
    cfg = cfg or {}
    sc = (meta.get("scenario") or cfg.get("report_scenario")
          or cfg.get("scenario"))
    eng = (meta.get("engine")
           or ("kvtest" if cfg.get("scenario") == "kvtest" else "python"))
    out = []

    def add(label, value):
        if value is None or value == "" or value == [] or value == {}:
            return
        if isinstance(value, bool):
            value = "是" if value else "否"
        out.append([label, value])

    add("场景", cfg.get("scenario"))
    add("协议", cfg.get("protocol"))
    add("工作模式", cfg.get("work_mode") or cfg.get("mode"))
    add("流量模式", cfg.get("traffic_mode"))
    ds = cfg.get("data_size")
    if isinstance(ds, (list, tuple)):
        add("数据大小", "、".join(str(x) for x in ds))
    else:
        add("数据大小", ds)
    # 单元报表（一个 size×combo）：只显示本单元的 size / 并发参数
    if meta.get("unit_size"):
        _override_param(out, "数据大小", meta["unit_size"])
    if sc != "pipeline" and eng != "kvtest":
        # pipeline/kvtest 的 c/t 来自文件名（每客户端进程 c1_t1），不代表并发
        # 规模；客户端数/写读规模在报表单元行由 client.count_per_node×N 推导
        _override_param(out, "客户端数", meta.get("unit_clients"))
        _override_param(out, "线程数", meta.get("unit_threads"))
    if cfg.get("duration"):
        add("采集时长", "{} s".format(cfg.get("duration")))
    warm = cfg.get("warmup") or {}
    if warm.get("duration"):
        add("预热时长", "{} s（并入正式窗口，前 {}s 不计入统计）".format(
            warm["duration"], warm["duration"]))
    add("batch_size", cfg.get("batch_size"))
    add("操作", cfg.get("operation"))
    add("key_count", cfg.get("key_count"))
    qps = cfg.get("base_qps") or cfg.get("target_qps")
    mult = cfg.get("qps_multiple") or 1
    if qps:
        try:
            per_node_qps = float(qps) * float(mult)
            if sc == "pipeline":
                # pipeline 的 base_qps 是「每节点」速率：完整写出集群口径，
                # 并给出读 QPS（写总量 × notify_count）
                if nodes:
                    write_total = per_node_qps * int(nodes)
                    add("目标写 QPS", "{:g} × {} 节点 = {:g}".format(
                        per_node_qps, int(nodes), write_total))
                    notify = int(cfg.get("notify_count") or 0)
                    if notify:
                        add("目标读 QPS", "{:g} × {} 读 = {:g}".format(
                            write_total, notify, write_total * notify))
                else:
                    add("目标写 QPS", "{:g}".format(per_node_qps))
            else:
                add("目标 QPS", "{:g} × {:g} = {:g}".format(
                    float(qps), float(mult), per_node_qps))
        except (TypeError, ValueError):
            add("目标 QPS", qps)
    add("notify_count", cfg.get("notify_count"))
    wr = cfg.get("writer") or {}
    rd = cfg.get("reader") or {}
    add("notify_threads", cfg.get("notify_threads") or wr.get("notify_threads"))
    add("writer.operation", wr.get("operation"))
    add("writer.count_per_node", wr.get("count_per_node"))
    add("reader.operation", rd.get("operation"))
    add("reader.count_per_node", rd.get("count_per_node"))
    add("reader.num_threads", rd.get("num_threads"))
    add("client.count_per_node", (cfg.get("client") or {}).get("count_per_node"))
    # store 数量（=每节点 store 数 × 节点数，直接配置；仅展示）
    add("store_count_per_node", cfg.get("store_count_per_node"))
    add("cpu_shard", cfg.get("cpu_shard"))
    add("numa_bind", cfg.get("numa_bind"))
    # 运行上下文
    add("运行模式", {"ssh": "真机", "k8s": "k8s"}.get(
        meta.get("mode"), meta.get("mode")))
    add("启动方式", {"single": "单任务", "rank_recall": "Rank+Recall"}.get(
        meta.get("launch_mode"), meta.get("launch_mode")))
    if nodes:
        add("节点数", nodes)
    add("客户端总数", meta.get("clients"))
    add("cluster 配置", meta.get("cluster_cfg"))
    add("global 配置", meta.get("global_cfg"))
    add("profile 配置", meta.get("profile_cfg"))
    return out


_UNIT_COMBO_RE = re.compile(r"benchmark_c(\d+)_t(\d+)")


def _unit_of(path, mode):
    """(key, dir_name, label, c, t, size) for one stats/trace file.

    basic: 单元 = (c, t, data_size) —— 目录 c{c}_t{t}_{size}
    pipeline: 单元 = data_size（pipeline 的 c1_t{N} 里 t 是 reader_threads，
    不是 combo，绝不能当 combo 解析）—— 目录 {size}
    解析失败（旧命名）退化为一个 "all" 单元。
    """
    base = os.path.basename(str(path))
    size = None
    m = _LOG_SIZE_RE.search(base)
    if m:
        size = (m.group(1) + m.group(2)).upper()
    mc = _UNIT_COMBO_RE.search(base)
    c = t = None
    if mc:
        c, t = int(mc.group(1)), int(mc.group(2))
    if mode != "basic" or c is None:
        sz = size or "all"
        return (("size", sz), sz, sz, c, t, size)
    sz = size or "all"
    return (("basic", c, t, sz),
            "c{}_t{}_{}".format(c, t, sz),
            "c{}×t{}{}".format(c, t, " · " + sz if size else ""),
            c, t, size)


def _unit_groups(paths, mode):
    """Group file paths by report unit, preserving discovery order."""
    out = {}
    for p in paths:
        key, name, label, c, t, size = _unit_of(p, mode)
        u = out.get(key)
        if u is None:
            u = out[key] = {"paths": [], "name": name, "label": label,
                            "c": c, "t": t, "size": size}
        u["paths"].append(p)
    return out


def _stats_window(loaded):
    """(t0, t1) epoch seconds covering these stats, or (None, None)."""
    t0 = t1 = None
    for st in loaded or []:
        m = st.get("meta") or {}
        if m.get("ts_from") is not None:
            t0 = m["ts_from"] if t0 is None else min(t0, m["ts_from"])
        if m.get("ts_to") is not None:
            t1 = m["ts_to"] if t1 is None else max(t1, m["ts_to"])
    return t0, t1


def _override_param(params, label, value):
    if value is None or value == "":
        return params
    for p in params:
        if p and p[0] == label:
            p[1] = value
            return params
    params.append([label, value])
    return params


def _generate_report_units(cfg, local_log_dir, args, early_stop=False,
                           qps_target=0, launch_mode="single",
                           mode="pipeline", nodes=0, engine="python"):
    """Unit-based report generation: ONE self-contained report.html per
    (data_size × combo) unit, in <local_log_dir>/report/<unit>/.
    Returns the list of written report paths (empty on failure)."""
    rp = _report_module()
    stats = _collect_local_stats(local_log_dir)
    if not stats:
        log_detail("[report] no client logs under {}; skipped".format(
            local_log_dir))
        return []
    traces_all = _collect_local_traces(local_log_dir)
    cfg_name = os.path.splitext(os.path.basename(
        cfg.get("__cfg_path__") or "scenario"))[0]
    echarts = os.path.join(ROOT, "webui", "web", "vendor", "echarts.min.js")
    # 节点侧现场日志实测大小（配置收尾 du 快照；缺失则报表回退旧口径）
    site_sizes = None
    try:
        with open(os.path.join(local_log_dir, "report", "site_sizes.json"),
                  "r", encoding="utf-8") as f:
            site_sizes = json.load(f) or None
    except (OSError, ValueError):
        site_sizes = None
    units = _unit_groups(stats, mode)
    traces_by_unit = {}
    for tp in traces_all:
        key, _n, _l, _c, _t, _s = _unit_of(tp, mode)
        traces_by_unit.setdefault(key, []).append(tp)
    written = []
    for key, u in units.items():
        loaded = rp.load(u["paths"])
        if not loaded:
            continue
        t0, t1 = _stats_window(loaded)
        # P1-B：页头「测试开始于」取数据中最早的采样时刻（节点的绝对时间），
        # 而不是报表生成时刻（旧实现写入 time.strftime 的构建墙钟）
        _started = (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(t0))
                    if t0 is not None else time.strftime("%Y-%m-%d %H:%M:%S"))
        # basic 的合并日志每个 combo 一份，客户端数取自文件名 c{C}；
        # pipeline 一个单元 = 一个 size，客户端数按实际 stats 计。
        n_clients = (u["c"] if (mode == "basic" and u["c"])
                     else len(loaded))
        meta = {
            "config": cfg_name,
            "mode": mode,
            "scenario": mode,
            "engine": engine,
            "launch_mode": launch_mode,
            "clients": n_clients,
            "qps_target": qps_target,
            "protocol": cfg.get("protocol") or "",
            "started": _started,
            "early_stop": bool(early_stop),
            "cluster_cfg": os.path.basename(getattr(args, "cluster", "") or ""),
            "global_cfg": os.path.basename(getattr(args, "global_cfg", "") or ""),
            "profile_cfg": os.path.basename(getattr(args, "profile", "") or ""),
            "unit": u["label"],
            "unit_size": u["size"],
            "unit_clients": u["c"],
            "unit_threads": u["t"],
        }
        # kvtest 现场索引：host 日志根目录 / run_id / namespace（来自侧车）
        if engine == "kvtest":
            _side = _kvtest_read_report_meta(local_log_dir)
            for _k in ("run_id", "remote_log_dir", "host_log_root",
                       "k8s_namespace", "warmup_s"):
                if _side.get(_k) is not None:
                    meta[_k] = _side[_k]
        meta["params"] = _build_report_params(cfg, meta, nodes=nodes)
        data = rp.reduce_scenario(
            loaded, meta,
            monitors=rp.reduce_monitors(local_log_dir, t0=t0, t1=t1),
            traces=rp.load(traces_by_unit.get(key, [])),
            monitor_sites=rp.reduce_monitor_sites(local_log_dir),
            site_sizes=site_sizes)
        out_dir = os.path.join(local_log_dir, "report", u["name"])
        html, _datajs = rp.render_scenario(
            data, out_dir, echarts,
            rel_base=os.path.relpath(out_dir, ROOT).replace(os.sep, "/"))
        try:
            os.makedirs(out_dir, exist_ok=True)
            with open(os.path.join(out_dir, "meta.json"), "w",
                      encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=1)
        except OSError as exc:
            log_detail("[report] meta.json write failed: {}".format(exc))
        log("[report] {} · {} -> {}".format(
            cfg_name, u["label"], os.path.relpath(html, ROOT)))
        _REPORT_ACC.append({"data": data, "report_html": html,
                            "config": cfg_name, "unit": u["label"]})
        written.append(html)
    return written


REPORT_SCENARIOS = ("basic", "pipeline")
REPORT_ENGINES = ("python", "kvtest")


def _report_engine(cfg, explicit=None, mode_hint=None):
    """Report engine (python | kvtest): only tags the artifact source, it
    never decides the report format."""
    eng = explicit or (cfg or {}).get("engine")
    if eng in REPORT_ENGINES:
        return eng
    return "kvtest" if mode_hint == "kvtest" else "python"


def _report_scenario(cfg, explicit=None, engine="python"):
    """Report format (basic | pipeline). The old name "scenario" is kept.

    Priority: explicit argument > cfg["report_scenario"] > cfg["scenario"]
    when it is basic/pipeline > kvtest defaults to pipeline > basic.
    """
    cfg = cfg or {}
    for cand in (explicit, cfg.get("report_scenario"), cfg.get("scenario")):
        if cand in REPORT_SCENARIOS:
            return cand
    return "pipeline" if engine == "kvtest" else "basic"


def _generate_reports(cfg, local_log_dir, args, early_stop=False,
                      qps_target=0, launch_mode="single", mode="pipeline",
                      nodes=0, engine=None, scenario=None):
    """WARN-only wrapper around the unit-based report generation.

    mode may still be the legacy "kvtest"; it is normalised into
    engine=kvtest plus a basic/pipeline scenario (format only)."""
    if getattr(args, "no_report", False) or getattr(args, "dry_run", False):
        return None
    eng = _report_engine(cfg, engine, mode_hint=mode)
    explicit = (scenario if scenario in REPORT_SCENARIOS else
                (mode if mode in REPORT_SCENARIOS else None))
    sc = _report_scenario(cfg, explicit, eng)
    try:
        written = _generate_report_units(
            cfg, local_log_dir, args, early_stop=early_stop,
            qps_target=qps_target, launch_mode=launch_mode, mode=sc,
            nodes=nodes, engine=eng)
        return written or None
    except Exception as exc:
        log("[WARN] report generation failed: {}".format(exc))
        return None


def _slim_overview_unit(sc):
    """Overview-only projection of one unit's reduce payload.

    The parent (rank+recall) overview renders cards from summary fields only,
    so drop the per-sample bins/series here to keep overview_units.json small;
    the full payload stays in the unit's own data.js."""
    out = {}
    for key, val in sc.items():
        if key == "sizes":
            out["sizes"] = {
                size: {"summary": (z or {}).get("summary")}
                for size, z in (val or {}).items()}
        elif key == "monitors":
            out["monitors"] = {"summary": (val or {}).get("summary")}
        else:
            out[key] = val
    return out


def _write_overview_units(ov_dir, run_name, accs):
    """Persist the slim per-unit index beside the overview.html.

    scripts/run_rank_recall.py merges the two children's indexes into one
    parent overview. WARN-only: never affects this run's reports."""
    try:
        units = []
        for acc in accs:
            sc = _slim_overview_unit(dict(acc["data"]))
            sc["unit"] = acc.get("unit") or ""
            units.append({
                "report_html": acc.get("report_html") or "",
                "unit": sc["unit"],
                "sc": sc,
            })
        os.makedirs(ov_dir, exist_ok=True)
        with open(os.path.join(ov_dir, "overview_units.json"), "w",
                  encoding="utf-8") as f:
            json.dump({"run": run_name, "units": units}, f,
                      ensure_ascii=False)
    except Exception as exc:
        log_detail("[report] overview_units.json write failed: {}".format(exc))


def _render_run_overview(ts_dir, mode=""):
    """Render the run-level overview.html from the scenarios accumulated so
    far (also used by the STOP path). WARN-only, never raises outward."""
    if not _REPORT_ACC:
        return None
    rp = _report_module()
    ov_dir = os.path.join(ts_dir, "report")
    scenarios = []
    # snapshot: a STOP can race a background post-processing unit that is
    # still appending to the accumulator
    _accs = list(_REPORT_ACC)
    for acc in _accs:
        rel = os.path.relpath(acc["report_html"], ov_dir)
        sc = dict(acc["data"], report_rel=rel.replace(os.sep, "/"))
        sc["unit"] = acc.get("unit") or ""
        scenarios.append(sc)
    _write_overview_units(ov_dir, os.path.basename(ts_dir), _accs)
    by_node = {}
    for sc in scenarios:
        for n in sc.get("nodes") or []:
            e = by_node.setdefault(n.get("node"),
                                   {"node": n.get("node"), "pods": 0,
                                    "bytes": 0})
            e["pods"] += n.get("pods") or 0
            e["bytes"] += n.get("bytes") or 0
    run_meta = {
        "run": os.path.basename(ts_dir), "mode": mode,
        "started": time.strftime("%Y-%m-%d %H:%M:%S"),
        "nodes": sorted(by_node.values(), key=lambda x: -(x.get("bytes") or 0)),
    }
    # 现场索引：从场景 meta 里取 host 日志根目录 / run_id / namespace
    for _sc in scenarios:
        _m = _sc.get("meta") or {}
        for _k in ("host_log_root", "remote_log_dir", "run_id",
                   "k8s_namespace"):
            if _m.get(_k) and not run_meta.get(_k):
                run_meta[_k] = _m[_k]
    run_meta.setdefault("run_id", os.path.basename(ts_dir))
    return rp.render_overview(run_meta, scenarios, ov_dir,
                               rel_base=os.path.relpath(
                                   ov_dir, ROOT).replace(os.sep, "/"))


def _stop_reports():
    """Partial report for the scenario in flight when a STOP arrives.

    The scenario's exception path cannot run for StopRequested (BaseException
    by design), so this is the only chance to turn a stopped run into
    evidence: pull the raw logs while the remote processes/pods are still
    alive, reduce locally (no remote map — a stop must not wait for it), then
    render the scenario report and the run overview. kvtest runs /stop first
    in the scenario handler (C++ flushes samples only there) and hands the
    already-pulled artifacts in via skip_pull; this function then runs the
    map + reduce chain on them. WARN-only; disable with MC_STOP_REPORT=0."""
    if os.environ.get("MC_STOP_REPORT", "1") == "0":
        log_detail("[STOP] partial report disabled (MC_STOP_REPORT=0)")
        return
    args = _STOP_CTX.get("args")
    if args is not None and (getattr(args, "no_report", False)
                             or getattr(args, "dry_run", False)):
        return
    cluster = _STOP_CTX.get("cluster")
    ctx = _STOP_CTX.get("report")
    if not cluster or not ctx:
        log_detail("[STOP] no scenario in flight; nothing to report")
        return
    # Settle per-config post-processing still running in the background so the
    # partial overview is assembled from a consistent accumulator. Bounded: a
    # STOP must never wait on a big map (the daemon worker dies with us).
    try:
        _bg_join(os.environ.get("MC_BG_JOIN_TIMEOUT", "45"))
    except Exception as exc:
        log("[WARN] [STOP] joining background reports: {}".format(exc))
    clear_stop()          # the pull must be allowed to run
    mode = ctx.get("mode", "pipeline")
    if ctx.get("skip_pull"):
        log("[STOP] {} artifacts already collected; building the partial "
            "report ...".format(mode))
    else:
        log("[STOP] collecting logs for a partial report ...")
        try:
            pull_logs(cluster, ctx["involved_nodes"], ctx["remote_log_dir"],
                      ctx["cfg_name"], ctx["local_log_dir"], False)
        except Exception as exc:
            log("[WARN] [STOP] log pull for report failed: {}".format(exc))
    if mode == "kvtest":
        # kvtest samples are mapped on the orchestrator (same chain as the
        # normal path); _kvtest_stop_collect already pulled them, so this is
        # map + reduce only
        try:
            data = _kvtest_generate_reports(
                ctx["cfg"], ctx["local_log_dir"], args, early_stop=True)
            if data is not None:
                _render_run_overview(ctx["result_dir"], "kvtest")
                log("[STOP] partial kvtest report written (early_stop "
                    "flagged)")
            else:
                log_detail("[STOP] no kvtest samples locally; partial report "
                           "skipped")
        except Exception as exc:
            log("[WARN] [STOP] kvtest report failed: {}".format(exc))
        return
    qps_target, launch_mode = 0, "single"
    if ctx.get("mode") == "pipeline":
        try:
            plan = pipeline_plan(ctx["cfg"])
            qps_target = plan["total_qps"]
            launch_mode = "rank_recall" if plan.get("dual") else "single"
        except Exception:
            pass
    else:
        try:
            qps_target = float(ctx["cfg"].get("target_qps", 0) or 0)
        except Exception:
            qps_target = 0
    try:
        data = _generate_reports(ctx["cfg"], ctx["local_log_dir"], args,
                                 early_stop=True, qps_target=qps_target,
                                 launch_mode=launch_mode,
                                 mode=ctx.get("mode", "pipeline"))
        if data is not None:
            _render_run_overview(ctx["result_dir"],
                                 getattr(args, "mode", "") if args else "")
            log("[STOP] partial report written (early_stop flagged)")
    except Exception as exc:
        log("[WARN] [STOP] report failed: {}".format(exc))


def basic_k8s_numa_node(cfg):
    """Return the explicit NUMA node for a K8s basic client, if configured."""
    node = cfg.get("k8s_numa_node")
    if node is None:
        return None
    if isinstance(node, bool) or not isinstance(node, int) or node < 0:
        raise RuntimeError("k8s_numa_node must be a non-negative integer")
    return node


_LOG_PULL_ATTEMPTS = 3
_LOG_PULL_BACKOFF_S = (1.0, 3.0)


def _log_pull_transport(node):
    """Return the SSH endpoint that actually carries a log transfer.

    A k8s-pod entry is only a kubectl target.  Remote kubectl and the second
    controller->orchestrator copy both run over the shared controller SSH
    connection, so dropping the pod's synthetic cache key cannot repair an
    ``SSH session not active`` failure.
    """
    if is_k8s_node(node):
        controller = node.get("_controller")
        if not controller or controller.get("_local"):
            return None
        return controller
    return node


def _reset_log_pull_transport(node):
    transport = _log_pull_transport(node)
    if transport is not None:
        _ssh_drop(transport)


def _wait_log_pull_retry(attempt, label):
    """Back off before the next attempt; *attempt* is zero based."""
    delay = _LOG_PULL_BACKOFF_S[min(attempt, len(_LOG_PULL_BACKOFF_S) - 1)]
    log_detail("[WARN] log pull retry {}/{} for {} after {:.1f}s".format(
        attempt + 2, _LOG_PULL_ATTEMPTS, label, delay))
    time.sleep(delay)


def pull_logs(cluster, involved_nodes, remote_log_dir, cfg_name, local_log_dir,
              dry_run, remote_prefix=None):
    """Pull every <prefix>.* file from each involved node.

    Transfers are size verified by :func:`scp_pull`.  A failed transfer gets
    two retries with reconnect + backoff; this covers both a cut SFTP channel
    and a stale remote-controller SSH session.
    """
    pulled_ips = set()
    for node_name in involved_nodes:
        n = get_node(cluster, node_name)
        if n["ip"] in pulled_ips:
            continue
        pulled_ips.add(n["ip"])
        prefix = remote_prefix or cfg_name
        remote_glob = "{}/{}.*".format(remote_log_dir, prefix)
        # k8s stage logs live in per-pod subdirs of the hostPath log dir
        pod = n.get("_pod") if n else None
        if pod:
            remote_glob = "{}/{}/{}.*".format(remote_log_dir, pod, prefix)
        last_exc = None
        for attempt in range(_LOG_PULL_ATTEMPTS):
            try:
                scp_pull(n["ip"], n, remote_glob, local_log_dir, dry_run=dry_run)
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                if dry_run:
                    break
                if attempt + 1 < _LOG_PULL_ATTEMPTS:
                    try:
                        _reset_log_pull_transport(n)
                    except Exception:
                        pass
                    _wait_log_pull_retry(attempt, n["ip"])
        if last_exc is not None:
            raise last_exc


def pull_stage_logs(ip, node, remote_files, local_dir, dry_run):
    """Incremental pull of just-finished stage log(s) so previous stages are
    locally readable while later (long) stages run. Each file is retried twice
    (size-verified by scp_pull); still failing files only WARN — the
    authoritative full pull happens later via pull_logs/_pipeline_pull_parallel."""
    if dry_run or not remote_files:
        return
    for f in remote_files:
        for attempt in range(_LOG_PULL_ATTEMPTS):
            try:
                scp_pull(ip, node, f, local_dir, dry_run=False)
                break
            except Exception as exc:
                if attempt + 1 < _LOG_PULL_ATTEMPTS:
                    try:
                        _reset_log_pull_transport(node)
                    except Exception:
                        pass
                    _wait_log_pull_retry(attempt, os.path.basename(f))
                    continue
                log_detail("[WARN] stage log pull {}: {}".format(f, exc))



def format_duration(seconds):
    """Seconds -> 'x分x秒' / 'x小时x分x秒' style progress string."""
    seconds = int(max(0, round(seconds)))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return "{}小时{}分{}秒".format(h, m, s)
    return "{}分{}秒".format(m, s)


def _fmt_hms(seconds):
    s = int(max(0, seconds))
    return "{:02d}:{:02d}:{:02d}".format(s // 3600, (s % 3600) // 60, s % 60)


def _heartbeat_interval(globals_cfg):
    """Heartbeat period for long measurement waits (0 disables)."""
    try:
        v = int((globals_cfg or {}).get("heartbeat_interval_s", 60) or 0)
    except (TypeError, ValueError):
        v = 60
    return max(0, v)


class _Heartbeat(object):
    """Periodic progress line for long waits (all engines x scenarios).

    Not thread-safe by design: each wait loop owns one instance (pipeline
    writers share one across their sequential waits)."""

    def __init__(self, tag, total_s=None, interval_s=60):
        self.tag = tag
        self.total_s = float(total_s) if total_s else None
        self.interval_s = int(interval_s or 0)
        self.t0 = time.time()
        self.last = self.t0

    def tick(self):
        if self.interval_s <= 0:
            return
        now = time.time()
        if now - self.last < self.interval_s:
            return
        self.last = now
        elapsed = now - self.t0
        if self.total_s:
            remain = max(0.0, self.total_s - elapsed)
            log("[{}] 测试已进行 {}，剩余 {}，预计结束于 {}".format(
                self.tag, _fmt_hms(elapsed), _fmt_hms(remain),
                time.strftime("%H:%M:%S", time.localtime(now + remain))))
        else:
            log("[{}] 测试已进行 {}".format(self.tag, _fmt_hms(elapsed)))


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
                      expected_s=None, marker=None, heartbeat=None):
    """Poll the remote process until it exits or times out.

    marker: when given, `kill -0` alone is not trusted — a container's pid
    namespace recycles freed pids almost immediately (the polling execs
    themselves can grab them), so ALIVE additionally requires the pid's
    cmdline to contain the marker. The probe string is shell-split ("" concat)
    so the checker never matches its own /proc cmdline.

    Logs ONE 'running...' line at start (with the expected duration and ETA
    wall time when expected_s is given) and one result line at the end
    (exited / timeout); per-poll ssh commands are not logged."""
    n = get_node(cluster, node_name)
    t0 = time.time()
    where = ("pod {}".format(n["_pod"]) if is_k8s_node(n) else node_name)
    log_detail("{} ({}, pid {}) running...{}".format(
        what, where, pid, _wait_suffix(t0, expected_s)))
    if marker:
        poll_cmd = ("kill -0 {pid} 2>/dev/null && "
                    "tr '\\0' ' ' < /proc/{pid}/cmdline 2>/dev/null | "
                    "grep -qF {m} && echo ALIVE || echo DEAD").format(
                        pid=pid, m=_marker_probe(marker))
    else:
        poll_cmd = "kill -0 {pid} 2>/dev/null && echo ALIVE || echo DEAD".format(pid=pid)
    for _ in range(int(timeout_s)):
        alive = ssh_check_output(
            n["ip"], n, poll_cmd,
            dry_run=dry_run, quiet=True)
        if alive != "ALIVE":
            log_detail("{} exited after {} ({:.0f}s)".format(
                what, format_duration(time.time() - t0), time.time() - t0))
            return True
        if heartbeat is not None:
            heartbeat.tick()
        time.sleep(1)
    log("[WARN] {} ({}, pid {}) still running after {} ({}s timeout)".format(
        what, where, pid, format_duration(timeout_s), timeout_s))
    if is_k8s_node(n):
        _log_pod_state(cluster, node_name, "wait timeout {}s".format(
            int(timeout_s)))
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


def _kvtest_summary_stat(cluster, entry):
    """Per-instance kvtest_out summary.json (mtime, size), or None.

    Salvage evidence: summary.json is written LAST by MetricsCollector::Stop
    (WriteSamples -> WriteSummary), so a fresh file proves samples+summary
    are complete. Both the pre-/stop baseline and the post-timeout probe stat
    the SAME node filesystem, so the freshness comparison is immune to
    orchestrator/node clock skew."""
    n = get_node(cluster, entry["node"])
    try:
        out = ssh_check_output(
            n["ip"], n,
            "stat -c '%Y %s' {} 2>/dev/null || echo MISSING".format(
                shlex.quote(entry["_out_dir"] + "/summary.json")),
            quiet=True)
    except Exception:
        return None
    parts = str(out or "").split()
    if len(parts) != 2:
        return None
    try:
        return int(parts[0]), int(parts[1])
    except ValueError:
        return None


def _kvtest_salvage_stuck(cluster, stuck_pairs, baseline):
    """Salvage /stop-stuck kvtest instances whose metrics already persisted.

    Known Mooncake kvtest shutdown deadlock (image 202611250330.c38d27f6):
    after MetricsCollector::Stop() the process hangs in NotifyRpcServer::Stop
    / HttpServer::Stop (keep-alive connection-thread join), so /stop is
    acknowledged but the binary never exits. The measured data is complete on
    disk at that point. For every stuck instance whose summary.json is fresh
    (exists, non-empty and newer than the pre-/stop baseline) SIGKILL the
    kvtest child: the run_kvtest.sh wrapper then reaps it, writes .exitrc and
    exits on its own. Returns the salvaged [(entry, pid)]; anything not
    provably persisted is left to the caller's strict failure path."""
    salvaged = []
    for entry, pid in stuck_pairs:
        iid = entry["iid"]
        stat = _kvtest_summary_stat(cluster, entry)
        base = baseline.get(iid)
        fresh = (stat is not None and stat[1] > 0
                 and (base is None or stat[0] > base[0]))
        if not fresh:
            log("[WARN] [kvtest] i{}: metrics not persisted after /stop "
                "(summary.json {}) — not salvageable".format(
                    iid, "missing" if stat is None else "stale"))
            continue
        n = get_node(cluster, entry["node"])
        # SIGKILL bypasses the shutdown deadlock. Kill only the child first
        # so the wrapper finishes its exitrc ledger; the wrapper fallback is
        # guarded by the cmdline marker because container pids are recycled
        # almost immediately after exit (never signal a reused pid).
        try:
            ssh_check_output(
                n["ip"], n,
                "kill -9 $(pgrep -P {pid} 2>/dev/null) 2>/dev/null; "
                "sleep 2; if tr '\\0' ' ' < /proc/{pid}/cmdline 2>/dev/null "
                "| grep -qF {m}; then kill -9 {pid} 2>/dev/null; fi || true"
                .format(pid=pid, m=_marker_probe(_KVTEST_PID_MARKER)),
                quiet=True)
        except Exception as exc:
            log("[WARN] [kvtest] i{}: salvage kill failed: {}".format(
                iid, exc))
            continue
        if wait_process_exit(cluster, entry["node"], pid,
                             "kvtest salvage i{}".format(iid), 15,
                             marker=_KVTEST_PID_MARKER):
            log("[WARN] [kvtest] i{} on {} salvaged: metrics persisted "
                "before the shutdown hang; kvtest killed, collecting"
                .format(iid, entry["node"]))
            salvaged.append((entry, pid))
        else:
            log("[WARN] [kvtest] i{} on {}: kill did not end the wrapper"
                .format(iid, entry["node"]))
    return salvaged


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


# runner-script marker for pid liveness probes. Embedded in shell commands
# SPLIT ("" concatenation) so the probing process's own /proc cmdline can
# never match itself when a container's pid namespace recycles the freed pid.
_PIPELINE_PID_MARKER = "run_benchmark_pipeline.sh"


def _marker_probe(marker):
    return marker[:len(marker) // 2] + '""' + marker[len(marker) // 2:]


def _check_exitrc(cluster, node_name, exitrc_path, what, pod_name=None):
    """Read back the runner's exit-code file ($LOG.exitrc, written by
    run_benchmark_pipeline.sh after the client exits). A background client
    that died with an error must FAIL the scenario — otherwise the run
    reports OK with truncated/garbage metrics (store exhaustion killed
    every client mid-measurement exactly this way)."""
    if not exitrc_path:
        return
    n = get_node(cluster, node_name)
    out = ssh_check_output(
        n["ip"], n, "cat {} 2>/dev/null || true".format(exitrc_path),
        quiet=True)
    rc = out.strip()
    if rc and rc != "0":
        _log_pod_state(cluster, pod_name or node_name,
                       "client exited rc={}".format(rc))
        raise RuntimeError(
            "{} exited with rc={} (client error — see {} in the pulled "
            "logs)".format(what, rc, os.path.basename(exitrc_path)))


def _pipeline_wipe_etcd(cluster, globals_cfg):
    """Best-effort wipe of the whole mcbench/ rendezvous tree (single active
    benchmark per cluster assumption). No-op when etcd is unconfigured."""
    eps = (globals_cfg.get("etcd_endpoints", "") or "").strip()
    if not eps:
        return
    n = _etcd_delete_prefix(cluster, eps, "mcbench/")
    if n:
        log("[k8s-pipeline] etcd: wiped {} stale mcbench/ keys".format(n))


def _pipeline_http_call(node_entry, port, path, timeout=5, dry=False,
                        method="POST"):
    """POST/GET to a pipeline/kvtest process's HTTP endpoint (writer /start,
    reader /shutdown, kvtest /health //summary //stop) by curling LOOPBACK ON
    THE NODE ITSELF. The node transport dispatches automatically: ssh nodes
    run curl over the ssh connection, k8s pods run it via kubectl exec inside
    the pod. The orchestrator host must NEVER be assumed to reach pod IPs /
    fabric IPs directly — it sits outside those networks on real clusters (a
    direct http_probe to the pod IP times out there). Returns True on HTTP
    200."""
    url = "http://127.0.0.1:{}{}".format(port, path)
    cmd = ("curl -s --noproxy '*' --connect-timeout 2 --max-time {} "
           "-X {} -o /dev/null -w '%{{http_code}}' {}").format(
               timeout, method, url)
    out = ssh_check_output(node_entry["ip"], node_entry, cmd, dry_run=dry)
    return str(out).strip() == "200"


# bounded-parallelism knobs (global.json "parallel" block, applied once at
# startup by apply_parallel_knobs; every min(_PIPELINE_PARALLEL, n) call
# site honors them without globals_cfg plumbing). fanout covers kubectl
# exec / HTTP fan-out / config push / launch / exit-wait / flatten+trace —
# no controller session cap applies to those. pull covers controller SFTP
# pulls — sshd MaxSessions (~10) caps concurrent channels, stay at/below.
_PIPELINE_PARALLEL = 16
_PULL_WORKERS = 10


def apply_parallel_knobs(globals_cfg):
    """Startup: global.json parallel.fanout_workers / parallel.pull_workers
    override the module defaults. Kept as module globals so the many
    min(_PIPELINE_PARALLEL, n) sites stay untouched."""
    global _PIPELINE_PARALLEL, _PULL_WORKERS
    block = (globals_cfg or {}).get("parallel") or {}
    try:
        _PIPELINE_PARALLEL = max(1, int(
            block.get("fanout_workers", _PIPELINE_PARALLEL)))
    except (TypeError, ValueError):
        pass
    try:
        _PULL_WORKERS = max(1, int(
            block.get("pull_workers", _PULL_WORKERS)))
    except (TypeError, ValueError):
        pass


def _pipeline_http_parallel(cluster, items, path, timeout=5, dry=False,
                            method="POST"):
    """POST to many pipeline endpoints with bounded concurrency (default 8).

    The ssh transport keeps ONE cached connection per node and opens one
    channel per command, so parallel calls are safe; a failed call drops the
    shared connection (transport behaviour), which would poison sibling
    calls — each failed item therefore gets ONE reconnect retry.

    items: [(node_entry, port, pid, what)]. /start items must return HTTP
    200; /shutdown items count a dead-skip as success. Returns
    [(what, ok, err)] in item order."""
    if dry or not items:
        return [(what, True, "") for _n, _p, _pid, what in items]
    from concurrent.futures import ThreadPoolExecutor

    def _run(item):
        node_entry, port, pid, what = item
        err = ""
        for _attempt in (0, 1):
            try:
                if path == "/shutdown":
                    _pipeline_shutdown(cluster, node_entry, port, pid, what)
                    return what, True, ""
                ok = _pipeline_http_call(node_entry, port, path,
                                         timeout=timeout, dry=dry,
                                         method=method)
                if ok:
                    return what, True, ""
                err = "non-200"
            except Exception as exc:
                err = str(exc)
            try:
                _ssh_drop(node_entry)   # reconnect before the retry
            except Exception:
                pass
        return what, False, err

    workers = max(1, min(_PIPELINE_PARALLEL, len(items)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(_run, items))


def _pipeline_pull_parallel(cluster, groups, local_log_dir, dry, workers=None):
    """Pull per-pod log groups with bounded concurrency. groups:
    [(node_entry, [remote_files], label)].

    Paramiko allows one channel per command on the cached connection but the
    server caps concurrent sessions (MaxSessions, often 10) — pulling 8+
    groups at once intermittently kills channels. Concurrency therefore
    defaults to 4.  Retry is performed in whole-pool rounds: all active
    transfers finish before a shared controller connection is reset.  This
    avoids one worker closing a freshly reconnected session underneath the
    other workers.  Each retry only fetches files still missing locally.
    """
    if dry or not groups:
        return 0
    from concurrent.futures import ThreadPoolExecutor

    def _missing(group):
        _node, files, _label = group
        return [f for f in files
                if not os.path.exists(os.path.join(
                    local_log_dir, os.path.basename(f)))]

    def _one(group):
        node, _files, label = group
        errors = []
        for f in _missing(group):
            try:
                scp_pull(node["ip"], node, f, local_log_dir, dry_run=False)
            except Exception as exc:
                errors.append("{}: {}".format(os.path.basename(f), exc))
        return label, _missing(group), errors

    workers = max(1, min(workers or _PULL_WORKERS, len(groups)))
    pending = list(groups)
    last_errors = {}
    for attempt in range(_LOG_PULL_ATTEMPTS):
        failed_labels = set()
        with ThreadPoolExecutor(max_workers=min(workers, len(pending))) as pool:
            for label, missing, errors in pool.map(_one, pending):
                if missing:
                    failed_labels.add(label)
                    if errors:
                        last_errors[label] = errors[-1]
        if not failed_labels:
            return 0
        pending = [g for g in pending if g[2] in failed_labels]
        if attempt + 1 >= _LOG_PULL_ATTEMPTS:
            break

        # No worker is active here.  Reset each real transport once; many pod
        # groups normally share the same remote kubectl controller.
        reset = set()
        for node, _files, _label in pending:
            transport = _log_pull_transport(node)
            token = id(transport) if transport is not None else None
            if token is None or token in reset:
                continue
            reset.add(token)
            try:
                _ssh_drop(transport)
            except Exception:
                pass
        _wait_log_pull_retry(attempt, "{} missing group(s)".format(len(pending)))

    errs = []
    for group in pending:
        _node, _files, label = group
        missing = _missing(group)
        err = "missing: " + ", ".join(os.path.basename(f) for f in missing)
        if label in last_errors:
            err += "; last error: " + last_errors[label]
        errs.append((label, err))
        log("[WARN] log pull failed for {}: {}".format(label, err))
    if errs:
        raise RuntimeError("log pull failed for {}/{}: {} ({})".format(
            len(errs), len(groups), errs[0][0], errs[0][1]))
    return 0


def _pipeline_shutdown(cluster, node_entry, port, pid, what):
    """Send /shutdown to a pipeline process, but SKIP the probe when the
    process is already dead. At 32-client scale every dead client cost a
    full kubectl-exec round-trip (rc=7) and ~20s of WARN spam per phase.
    Returns True when the client was alive (probe attempted), False when
    skipped."""
    if pid:
        try:
            alive = ssh_check_output(
                node_entry["ip"], node_entry,
                "kill -0 {pid} 2>/dev/null && tr '\\0' ' ' < "
                "/proc/{pid}/cmdline 2>/dev/null | grep -qF {m} && "
                "echo ALIVE || echo DEAD".format(
                    pid=pid, m=_marker_probe(_PIPELINE_PID_MARKER)),
                quiet=True)
        except Exception:
            alive = "ALIVE"  # unknown -> still attempt the graceful probe
        if str(alive).strip() != "ALIVE":
            log("{}: already exited; skipping /shutdown".format(what))
            return False
    _pipeline_http_call(node_entry, port, "/shutdown")
    return True


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
                          etcd_endpoints=None, run_id=None,
                          remove_host=None, remove_node=None, remove_runner=None,
                          globals_cfg=None, failed_stage=""):
    """Best-effort cleanup after a pipeline scenario failure. Every step is
    guarded so the original exception is never masked. Order: graceful reader
    shutdown -> wait for exit -> kill stragglers -> store remove-all (skipped
    when the failing stage was itself a remove-keys) -> wipe the etcd
    rendezvous prefix. No log pull: raw logs stay on the nodes (the scenario's
    _scenario_failure_report printed their directories)."""
    # 1. graceful reader shutdown so each reader flushes SUMMARY/RESULT
    _pid_by_obj = {id(e): (pid or "").strip() for e, pid in reader_pids}
    for r in plan["readers"]:
        if "node" not in r:
            continue  # deployment never materialized the pod entries
        try:
            rn = get_node(cluster, r["node"])
            _pipeline_shutdown(cluster, rn, r["port"],
                               _pid_by_obj.get(id(r), ""),
                               "fail-cleanup " + _pipeline_entry_label(r))
        except Exception as exc:
            log("[WARN] fail-cleanup: {} shutdown failed: {}".format(
                _pipeline_entry_label(r), exc))
    # 2. short wait for readers to exit (their final logs are written on exit)
    for r, pid in reader_pids:
        if pid:
            try:
                wait_process_exit(cluster, r["node"], pid,
                                  _pipeline_entry_label(r), 30,
                                  marker=_PIPELINE_PID_MARKER)
            except Exception:
                pass
    # 3. kill any still-alive writers/readers
    for w, pid in writer_pids:
        if pid:
            _kill_remote_process(cluster, w["node"], pid)
    for r, pid in reader_pids:
        if pid:
            _kill_remote_process(cluster, r["node"], pid)
    # 4. best-effort store remove-all: a FAILED run must not leave objects
    # behind (the success path removes at config end; without this, residue
    # only clears on the NEXT run's pre-launch remove). Runs after the
    # clients are dead so no writer can re-create keys mid-sweep.
    if remove_host and remove_node and remove_runner:
        if str(failed_stage).startswith("remove-keys"):
            log("[FAIL] store remove-all skipped (failing stage was {})"
                .format(failed_stage))
        else:
            try:
                ran = _remove_keys_stage(
                    cluster, globals_cfg, remove_host,
                    get_node(cluster, remove_node), remove_runner,
                    remote_log_dir, cfg_name, cfg_name, "full", "fail cleanup",
                    dry=False, node_name=remove_node,
                    label="{} (fail-cleanup remove-all)".format(cfg_name))
                if ran:
                    log("[FAIL] fail-cleanup: store remove-all issued")
                else:
                    log("[FAIL] fail-cleanup: store already clean — "
                        "remove-all skipped")
            except Exception as exc:
                log("[WARN] fail-cleanup: store remove-all failed: {}".format(exc))
    # 5. wipe this run's etcd rendezvous prefix
    if etcd_endpoints and run_id:
        try:
            _etcd_delete_prefix(cluster, etcd_endpoints,
                                "mcbench/{}/".format(run_id))
        except Exception as exc:
            log("[WARN] fail-cleanup: etcd prefix wipe failed: {}".format(exc))


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
                          marker="mooncake_benchmark_client", quiet=False,
                          heartbeat=None):
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
        log_detail("[k8s] $ {}".format(kubectl_exec_line(node, command)))
    else:
        log_detail("[ssh] {} : {}".format(host, command))
    if dry_run:
        log_detail("      (dry-run, skipped)")
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
                     "--", "bash", "-c", command], quiet=quiet)
            except StopRequested:
                holder["stop"] = True
            except Exception as exc:
                log("[exec] {} failed: {}".format(node["_pod"], exc))
                holder["rc"] = 1

        th = threading.Thread(target=_run, daemon=True)
        th.start()
        t0 = time.time()
        (log_detail if quiet else log)(
            "{} running...{}".format(label, _wait_suffix(t0, expected_s)))
        recorded = False
        while th.is_alive():
            if stop_requested():
                holder["stop"] = True
            if not recorded and pids_file:
                if _record_pids(host, node, pids_file, label, cfg_name,
                                node_name, marker):
                    recorded = True
            if heartbeat is not None:
                heartbeat.tick()
            time.sleep(1)
        th.join()
        if holder.get("stop") or stop_requested():
            raise StopRequested()
        rc = holder["rc"]
    else:
        client = _ssh_client(node)
        stdin, stdout, stderr = client.exec_command(command)
        _relay = not quiet
        chan = stdout.channel
        t0 = time.time()
        (log_detail if quiet else log)(
            "{} running...{}".format(label, _wait_suffix(t0, expected_s)))
        recorded = False
        while not chan.exit_status_ready():
            if stop_requested():
                try:
                    chan.close()
                except Exception:
                    pass
                _ssh_drop(node)
                raise StopRequested()
            _drain_chan(chan, _relay, [])
            if not recorded and pids_file:
                if _record_pids(host, node, pids_file, label, cfg_name,
                                node_name, marker):
                    recorded = True
            if heartbeat is not None:
                heartbeat.tick()
            time.sleep(1)
        grace = time.time() + 5
        while (chan.recv_ready() or chan.recv_stderr_ready()
                or not chan.eof_received) and time.time() < grace:
            _drain_chan(chan, _relay, [])
            time.sleep(0.05)
        _drain_chan(chan, _relay, [])
        rc = chan.recv_exit_status()
    (log_detail if quiet else log)("{} done rc={} elapsed={:.1f}s".format(
        label, rc, time.time() - t0))
    if rc != 0:
        raise RuntimeError("ssh command failed (rc={}) on {}: {}".format(
            rc, host, command))


# ---------------------------------------------------------------------------
# process ledger (.state): every remote client process started by this
# orchestrator is recorded (ip/node/port/fabric_host/pid) so it can always
# be cleaned up, even after a crashed or manually killed orchestrator run.
# ---------------------------------------------------------------------------

STATE_FILE = os.environ.get("MC_BENCH_STATE_FILE", os.path.join(ROOT, ".state"))


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
            # marker probe is shell-split ("" concat): the reap poll's own
            # /proc cmdline must never match itself on a recycled pid
            _m = e.get("marker") or "mooncake_benchmark_client"
            marker = ssh_check_output(
                ip, node,
                "tr '\\0' ' ' < /proc/{pid}/cmdline 2>/dev/null | grep -cF {m} || true".format(
                    pid=pid, m=_marker_probe(_m)),
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
              dry_run=False, pull_to=None, cfg_name=None, node_name=None,
              quiet=False, label=None, tag=None):
    _check_stop()
    # k8s: stage logs land in the pod's OWN subdirectory of the hostPath
    # log dir (<root>/<run_id>/<pod>/…); ssh keeps the flat layout
    remote_log = _pod_base(remote_log_path(remote_log_dir, log_prefix, mode),
                           node)
    command = "bash {} {} {}".format(runner, mode, remote_log)
    if extra:
        command += " " + extra
    if not label:
        label = "[{}] {}".format(tag, mode) if tag else log_prefix
    run_ssh_with_progress(
        host, node, command, label=label, dry_run=dry_run,
        pids_file=remote_log + ".pids", cfg_name=cfg_name or log_prefix,
        node_name=node_name, quiet=quiet)
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
                        log_detail("{}: [{}] {}".format(
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
    hb_interval = _heartbeat_interval(globals_cfg)
    log_phase("场景 {} (engine=python, scenario=basic)".format(
        os.path.splitext(os.path.basename(cfg.get("__cfg_path__", "basic")))[0]))
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
    local_result_dir = getattr(args, "_result_dir_path", None) or \
        os.path.join(RESULTS_DIR, ts)
    remote_dir, remote_scripts, remote_log_dir, remote_runner_basic, \
        remote_runner_pipeline, local_log_dir = \
        scenario_paths(globals_cfg, ts, cfg_name, local_result_dir)
    remote_runner = remote_runner_basic
    if not dry:
        os.makedirs(local_log_dir, exist_ok=True)
    scenario_params = base_scenario_params(
        cfg, work_mode, "k8s_pod" if is_k8s else "ssh",
        "client" if is_k8s else primary_node_name)
    cluster_session = build_cluster_session(cluster, globals_cfg, remote_dir)
    # Resolved BEFORE the try: the FAIL handler passes it to the report, and
    # a failure in the very first stage would otherwise mask the real error
    # with UnboundLocalError (re-assigned inside the try below).
    total_target_qps = float(cfg.get("target_qps", 0) or 0)
    profile_sess = None  # profiling session (set below when matrix is on)
    stage = "configmap/scripts"
    try:
        # ssh: the client node is known from cluster.json (no pod yet);
        # k8s: the pod materializes below and the topology is always
        # remote_node (store is an external Service).
        if not is_k8s:
            node = get_node(cluster, primary_node_name)
            host = node["ip"]
            store_node_ips = resolve_store_node_ips(
                cluster, globals_cfg.get("mooncake_store_nodes", []) or [])
            topology = ("local_node" if node["ip"] in store_node_ips
                        else "remote_node")
        else:
            topology = "remote_node"
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
        k8s_numa_node = basic_k8s_numa_node(cfg) if is_k8s else None
        if is_k8s:
            numa_bind = False
            cpu_shard = 0
        # numactl policy: membind (strict, default) | preferred | off, and
        # strict/auto behavior when numactl is missing on the node
        membind_mode, numa_strict = resolve_numa_policy(cfg, globals_cfg)
        numa_k = 0
        numa_topo = {}
        numactl_ok = False
        exclude = "" if is_k8s else node_exclude(cluster, node)
        if numa_bind and not dry:
            numa_k, numa_topo = probe_numa_topology(host, node)
            numactl_ok = probe_numactl(host, node)
            if not numactl_ok:
                numa_node_check(primary_node_name, numa_strict)
        if is_k8s:
            _numa_txt = ("NUMA {}".format(k8s_numa_node)
                         if k8s_numa_node is not None else "off (container)")
        else:
            _numa_txt = "on" if numa_bind else "off"
        log("[basic] params: op={} topo={} protocol={} run_combos={} "
            "warmup={} need_remove_before_run={} target_qps={} "
            "NUMA/bind={}{}".format(
                op, topology, cfg["protocol"], len(runs), warmup,
                need_remove_before_run, total_target_qps or "max-speed",
                _numa_txt,
                "" if is_k8s else " primary_node={}".format(host)))
        log_phase("资源创建", level=2)
        ensure_configmap(cluster, scenario_params, cluster_session, ts, cfg_name, dry)
        stage = "deploy pods"
        if is_k8s:
            # k8s: pods are created AFTER the ConfigMap they mount
            primary_node_name = ensure_k8s_pod(cluster, ts, cfg_name, dry)
            involved_nodes = [primary_node_name]
            node = get_node(cluster, primary_node_name)
            host = node["ip"]
        else:
            ensure_pods(cluster, involved_nodes, ts, cfg_name, dry)
        cfg["_primary_node"] = primary_node_name
        stage = "stage scripts"
        deploy_scripts(cluster, involved_nodes, scenario_params, cluster_session,
                       remote_scripts, remote_log_dir, dry)
        # STOP context: lets the StopRequested handler pull logs while the
        # remote side is still alive and render a partial (early_stop) report.
        _STOP_CTX["report"] = {
            "cfg": cfg, "cfg_name": cfg_name, "result_dir": local_result_dir,
            "local_log_dir": local_log_dir, "remote_log_dir": remote_log_dir,
            "involved_nodes": involved_nodes, "mode": "basic",
        }
        stage = "remove-keys (pre-run)"
        log_phase("清库 remove-keys", level=2)
        _remove_keys_stage(cluster, globals_cfg, host, node, remote_runner,
                           remote_log_dir, cfg_name, cfg_name, "full",
                           "pre-run", dry=dry, pull_to=local_log_dir,
                           node_name=primary_node_name, tag="basic")
        # profiling lifecycle (both triggers wrap the combo loop in basic;
        # whole_run additionally covers the per-ds setup stages inside it)
        if matrix and not dry and prof_core:
            try:
                profile_sess = prof_core.start_profiling(
                    cluster, matrix,
                    _profiling_targets(cluster, matrix, involved_nodes, ts,
                                       cfg_name, dry),
                    cfg_name,
                    remote_profiling_dir(remote_log_dir, cfg_name))
            except Exception as exc:
                log("[WARN] [profile] start failed: {}".format(exc))
        log_phase("基准测试", level=2)
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
                    stage = "generate-data ({})".format(ds)
                    run_stage(host, node, remote_runner,
                              "generate-data", remote_log_dir, cfg_name,
                              extra="--data-size {}".format(ds), dry_run=dry,
                              pull_to=local_log_dir,
                              node_name=primary_node_name, tag="basic")
                if op in PIPELINE_GET_IFACES:
                    stage = "prepare-store ({})".format(ds)
                    run_stage(host, node, remote_runner,
                              "prepare-store", remote_log_dir, cfg_name,
                              dry_run=dry, pull_to=local_log_dir,
                              node_name=primary_node_name, tag="basic")
                current_ds = ds
            if need_remove_before_run:
                stage = "remove-keys (run {}, {})".format(c, ds)
                _remove_keys_stage(cluster, globals_cfg, host, node,
                                   remote_runner, remote_log_dir, cfg_name,
                                   cfg_name, "full", stage, dry=dry,
                                   pull_to=local_log_dir,
                                   node_name=primary_node_name, tag="basic")
            # data_size is always part of the run file name, so different
            # sizes under the same (c, t) never collide
            name = "benchmark_c{}_t{}_{}".format(c, t, ds)
            remote_log = remote_log_path_pod(remote_log_dir, cfg_name, name,
                                             node)
            command = "bash {} benchmark {} --num-clients {} --num-threads {} --warmup {}".format(
                remote_runner, remote_log, c, t, warmup)
            if k8s_numa_node is not None:
                command += " --numa-node {}".format(k8s_numa_node)
            elif numa_bind:
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
            stage = "benchmark ({}, c{} t{})".format(ds, c, t)
            _hb = (_Heartbeat("basic", expected, hb_interval)
                   if hb_interval else None)
            run_ssh_with_progress(host, node, command,
                                  "[basic] benchmark c{} t{} ({})".format(c, t, ds),
                                  expected_s=expected, dry_run=dry,
                                  pids_file=remote_log + ".pids",
                                  cfg_name=cfg_name, node_name=primary_node_name,
                                  heartbeat=_hb)
            # map (remote logstats) + stats-only pull by default — the
            # CSV/report consume *.stats.json; raw logs (+pids) only when
            # explicitly requested (--pull-raw-logs / --png)
            _stats_files = run_logstats_map(
                cluster, node, [remote_log], remote_scripts, globals_cfg,
                dry=dry, node_name=primary_node_name, size=ds)
            _gl_base = remote_log[:-len(".log")] if remote_log.endswith(
                ".log") else remote_log
            _gl_files, _gl_site = _resolve_glog_inputs(
                cluster, node, _gl_base + ".glog.d", remote_log + ".glog",
                dry=dry)
            _trace_files = run_tracestats_map(
                cluster, node, _gl_files, remote_scripts, globals_cfg,
                dry=dry, node_name=primary_node_name, size=ds,
                sample_rate=_hifreq_sample_rate(), remote_path=_gl_site,
                out_path=_gl_base + ".trace.json") if _gl_files else []
            _pull_files = list(_stats_files) + list(_trace_files)
            if getattr(args, "pull_raw_logs", False) \
                    or getattr(args, "png", False):
                _pull_files += [remote_log, remote_log + ".glog"] + (
                    [remote_log + ".pids"] if c > 1 else []) + list(_gl_files)
            pull_stage_logs(host, node, _pull_files, local_log_dir, dry)
            # small runs: pull the raw log too (cheap; full per-point
            # inspection + local stats recompute); large runs stay stats-only
            if not dry and not (getattr(args, "pull_raw_logs", False)
                                or getattr(args, "png", False)):
                _b = 0
                for _base in (remote_log + ".stats.json",
                              remote_log + ".glog.trace.json"):
                    _lp = os.path.join(local_log_dir,
                                       os.path.basename(_base))
                    try:
                        with open(_lp, "r", encoding="utf-8") as _f:
                            _b += json.load(_f).get("meta", {}).get("bytes", 0)
                    except (OSError, ValueError):
                        continue
                if 0 < _b <= RAW_AUTO_PULL_MAX:
                    pull_stage_logs(host, node,
                                    [remote_log, remote_log + ".glog"],
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
        stage = "remove-keys (config end)"
        log_phase("清理收尾", level=2)
        _remove_keys_stage(cluster, globals_cfg, host, node, remote_runner,
                           remote_log_dir, cfg_name, "remove-keys-end",
                           "key-prefix", "config end", dry=dry, tag="basic")
        # raw-log full pull only on explicit request (stats already pulled
        # per run combo; failure paths pull raw for diagnosis separately)
        if getattr(args, "pull_raw_logs", False) or getattr(args, "png", False):
            pull_logs(cluster, involved_nodes, remote_log_dir, cfg_name,
                      local_log_dir, dry)
        echo_cpu_bindings(cfg_name, local_log_dir)
        # profiling: stop sampling + spdiag show + collect (never raises)
        if profile_sess is not None:
            _stop_profiling(cluster, profile_sess, local_result_dir, cfg_name,
                            dry=dry)
        # 现场日志实测大小（pod/profile 产物已全部落盘、pod 仍存活）
        _site_du_snapshot(cluster, ts, cfg_name, involved_nodes,
                          local_result_dir, remote_log_dir, dry=dry)
        # k8s pods stay alive past this return: main()'s sweep-end
        # remove-keys still needs a live pod (store keys are not in the
        # pod workspace); cleanup_pods runs in main() after sweep-end
        # Sweep-end remove-all is issued by main() on the last successful
        # config's node; hand it this config's coordinates. The report is
        # local post-processing: main() queues it on the background worker so
        # it overlaps the next config's deployment (kwargs ride along).
        return {"host": host, "node": primary_node_name, "runner": remote_runner,
                "remote_log_dir": remote_log_dir,
                "_report_kwargs": {"nodes": len(involved_nodes),
                                   "qps_target": total_target_qps,
                                   "mode": "basic"}}
    except Exception as exc:
        # FAIL path: one line names scenario + failing stage + reason, then
        # the pod table and the raw-log directories. No log extraction.
        if not dry:
            class1 = is_k8s and stage.startswith("deploy")
            _scenario_failure_report(cluster, cfg_name, stage, exc,
                                     involved_nodes, remote_log_dir, ts,
                                     is_k8s, class1=class1)
            if class1:
                log("[FAIL] cleanup skipped (pods never started)")
            else:
                # profiling collect BEFORE the report so monitor CSVs reach it
                if profile_sess is not None:
                    _stop_profiling(cluster, profile_sess, local_result_dir,
                                    cfg_name, dry=dry)
                _generate_reports(cfg, local_log_dir, args, early_stop=True,
                                  nodes=len(involved_nodes),
                                  qps_target=total_target_qps, mode="basic")
                reap_state_entries(cluster)
            if is_k8s:
                # pod was Ready; the benchmark command failed -> keep pod and
                # namespace for troubleshooting (STOP / --clean-up tears down)
                cluster["_k8s_keep_ns"] = True
            else:
                cleanup_pods(cluster, involved_nodes, dry)
            cfg["_fail_reported"] = True
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
        "{{MC_HOST_LOG_DIR}}": host_log_dir(cluster, ts),
        "{{MC_LOG_MOUNT}}": k8s["pod_workspace"] + "/logs/" + ts,
    }
    for key, val in repl.items():
        text = text.replace(key, val)
    if "{{MC_" in text:
        raise RuntimeError("deployment template has unresolved "
                           "{{MC_*}} placeholders")
    return deploy, run_label, text


# Deployment rollout budget. Kept at 60s (user decision); fatal pod states
# are detected within ~3s by the fast-fail poll below, so the timeout is
# only a backstop for slow-but-healthy clusters.
_K8S_DEPLOYMENT_TIMEOUT_S = 60


def _k8s_all_events(controller, ns, limit=40):
    """Namespace events (tail), sorted by time — includes pod-level reasons
    (FailedScheduling / ErrImageNeverPull / CreateContainerError) that the
    deployment describe never shows."""
    out, rc = controller_run(
        controller, ["-n", ns, "get", "events",
                     "--sort-by=.lastTimestamp"], check=False)
    if rc != 0 or not out.strip():
        return []
    lines = [ln.rstrip() for ln in out.strip().splitlines() if ln.strip()]
    return lines[-limit:]


def _k8s_fatal_event_lines(events):
    """Free-text event lines carrying a fatal reason."""
    return [ln for ln in events
            if any(r in ln for r in _K8S_FATAL_EVENTS)]


def _k8s_deployment_diag(controller, ns, run_label, deploy):
    """Failure evidence dump — the three pieces you need to diagnose a failed
    rollout: pod list (which pod sits on which node, in what phase),
    namespace events (the REAL reason), and the deployment describe tail."""
    view = " ".join(kubectl_prefix({"_ctx": controller.get("_ctx", ""),
                                    "_ns": ns}))
    log("[k8s] diagnose pods:   {}".format(view + " get pods -o wide"))
    out, rc = controller_run(
        controller, ["-n", ns, "get", "pods", "-l", "run=" + run_label,
                     "-o", "wide"], check=False)
    if rc == 0 and out.strip():
        log("[k8s] pods (run={}):\n{}".format(run_label, out.strip()))
    log("[k8s] diagnose events: {}".format(
        view + " get events --sort-by=.lastTimestamp"))
    events = _k8s_all_events(controller, ns)
    if events:
        log("[k8s] namespace events (tail):\n{}".format("\n".join(events)))
    _log_deployment_events(controller, ns, deploy)


def wait_k8s_deployment_ready(cluster, deploy, run_label, timeout=None,
                              poll=3.0):
    """Poll the Deployment rollout; fail FAST on fatal pod states/events
    (ErrImageNeverPull / ImagePullBackOff / CreateContainer* /
    CrashLoopBackOff / FailedScheduling) instead of blind-waiting the whole
    timeout. Every failure path dumps the pod list + namespace events +
    deployment describe via _k8s_deployment_diag."""
    if timeout is None:
        timeout = _K8S_DEPLOYMENT_TIMEOUT_S
    c = cluster["_controller"]
    ns = k8s_settings(cluster)["namespace"]
    deadline = time.time() + timeout
    avail, want = 0, 0
    while True:
        out, _ = controller_run(
            c, ["-n", ns, "get", "deployment", deploy, "-o",
                "jsonpath={.status.availableReplicas}"], check=False)
        try:
            avail = int(out.strip() or 0)
        except ValueError:
            avail = 0
        out, _ = controller_run(
            c, ["-n", ns, "get", "deployment", deploy, "-o",
                "jsonpath={.spec.replicas}"], check=False)
        try:
            want = int(out.strip() or 0)
        except ValueError:
            want = 0
        if want and avail >= want:
            return
        states, _ = controller_run(
            c, ["-n", ns, "get", "pods", "-l", "run=" + run_label, "-o",
                "jsonpath={range .items[*]}{.metadata.name}{' '}"
                "{.status.phase}{' '}"
                "{range .status.containerStatuses[*]}"
                "{.state.waiting.reason}{' '}{end}{'\\n'}{end}"], check=False)
        fatal = []
        for line in states.splitlines():
            toks = line.split()
            if any(t in _K8S_FATAL_WAITING for t in toks):
                fatal.append(line.strip())
        events = _k8s_all_events(c, ns)
        fatal_events = _k8s_fatal_event_lines(events)
        if fatal or fatal_events:
            _k8s_deployment_diag(c, ns, run_label, deploy)
            reason = ""
            if fatal:
                reason = next((t for t in fatal[0].split()
                               if t in _K8S_FATAL_WAITING), "")
            if not reason and fatal_events:
                reason = next((r for r in _K8S_FATAL_EVENTS
                               if r in fatal_events[0]), "")
            raise RuntimeError(
                "deployment {}: pods failed to start (reason={}; evidence "
                "above)".format(deploy, reason or "unknown"))
        if time.time() >= deadline:
            _k8s_deployment_diag(c, ns, run_label, deploy)
            raise RuntimeError(
                "deployment {} not ready in {}s ({}/{} replicas available; "
                "evidence above)".format(deploy, int(timeout), avail, want))
        time.sleep(poll)


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
        log_detail("[k8s] deployment manifest: template={} -> {}:{}".format(
            os.path.relpath(_deployment_template_path(cluster), ROOT),
            controller_desc(c), remote_yaml))
        log("[k8s] deployment {} create: role={} replicas={}".format(
            deploy, role, replicas))
        out, rc = controller_run(
            c, ["-n", ns, "apply", "-f", remote_yaml], check=False)
        if rc != 0:
            raise RuntimeError("kubectl apply failed for deployment {}: {}"
                               "".format(deploy, out.strip()[:400]))
    finally:
        shutil.rmtree(local_dir, ignore_errors=True)
    t0 = time.time()
    wait_k8s_deployment_ready(cluster, deploy, run_label)
    log("[k8s] deployment {} ready (role={}, replicas={}, {:.1f}s)".format(
        deploy, role, replicas, time.time() - t0))
    return deploy, run_label


def _k8s_pod_ip(controller, ns, pod):
    out, rc = controller_run(
        controller, ["-n", ns, "get", "pod", pod, "-o",
                     "jsonpath={.status.podIP}"], check=False)
    ip = out.strip()
    if rc != 0 or not ip:
        raise RuntimeError("cannot resolve Pod IP for {}".format(pod))
    return ip


def _k8s_pod_host_ip(controller, ns, pod):
    """Pod 所在集群节点 IP（pod.status.hostIP，与 podIP 同权限，无需 node RBAC）。"""
    out, rc = controller_run(
        controller, ["-n", ns, "get", "pod", pod, "-o",
                     "jsonpath={.status.hostIP}"], check=False)
    ip = out.strip()
    if rc != 0 or not ip:
        return ""
    return ip


def _node_display(cluster, node, name):
    """报表/产物的节点显示名 = **节点 IP**：
    - k8s-pod → 宿主节点 IP（status.hostIP）；同主机多 pod 共用同一显示名
      （节点视角合并，符合「节点级」语义）；
    - ssh → 节点管理 IP；若多个节点共用同一 IP（示例配置常见 127.0.0.1）
      则回退原节点名，避免误合并；
    - 取不到 IP 时回退名称（绝不丢节点）。"""
    if not node:
        return name
    if node.get("kind") == "k8s-pod":
        return node.get("host_ip") or name
    ip = node.get("ip")
    if not ip:
        return name
    same = sum(1 for n in (cluster.get("nodes") or {}).values()
               if not n.get("_pod") and n.get("ip") == ip)
    return name if same > 1 else ip


def _pipeline_k8s_roles(plan):
    """Deployment role list for the k8s pipeline channel. dual mode deploys
    ONE "client" role (pod = dual-role process); separate mode deploys a
    reader and a writer role. kvtest split-pod mode also deploys one client
    role, but later binds two process entries to each resulting pod. Each
    entry: (role, plan entries, port base).
    NOTE: the dual branch MUST stay a 1-tuple wrapping the role tuple —
    returning the bare tuple makes the caller's for-loop iterate its
    ELEMENTS and unpack the role STRING ("too many values to unpack")."""
    if plan.get("dual") or plan.get("split_pod"):
        return (("client", plan["clients"], plan["writer_port_base"]),)
    return (("reader", plan["readers"], plan["reader_port_base"]),
            ("writer", plan["writers"], plan["writer_port_base"]))


def _kvtest_bind_split_pods(plan):
    """Bind each split writer/reader pair to its materialized client pod.

    The Deployment creates only ``plan['clients']``.  Once those entries have
    a pod name, copy it to the paired process entries and assign distinct
    in-pod HTTP ports.  Kept as a helper so the topology invariant is unit
    tested independently of kubectl.
    """
    if not plan.get("split_pod"):
        return
    clients = plan.get("clients") or []
    writers = plan.get("writers") or []
    readers = plan.get("readers") or []
    if len(clients) != len(writers) or len(clients) != len(readers):
        raise RuntimeError(
            "kvtest split-pod plan must have one writer and one reader per "
            "client pod")
    for client, writer, reader in zip(clients, writers, readers):
        pod = client.get("node")
        if not pod:
            raise RuntimeError("kvtest split-pod client is missing its pod")
        writer["node"] = pod
        reader["node"] = pod
        writer["port"] = plan["writer_port_base"]
        reader["port"] = plan["reader_port_base"]


def _pipeline_client_timeout(cfg, rendezvous_timeout_s, warmup_enabled=True):
    """Return a lifetime that cannot expire before orchestrated shutdown.

    Pipeline readers start their lifetime clock before rendezvous.  Budget for
    both registration barriers, the complete writer window, and the shutdown
    hand-off/exit grace.  The timeout is only a safety ceiling: the
    orchestrator still terminates clients through /shutdown on the normal
    path, so a generous value does not lengthen successful runs.
    """
    rendezvous_s = max(0.0, float(rendezvous_timeout_s or 0))
    duration_s = max(0.0, float(cfg.get("duration", 0) or 0))
    warmup_s = 0.0
    if warmup_enabled:
        warmup_s = max(
            0.0, float((cfg.get("warmup", {}) or {}).get("duration", 0) or 0))
    shutdown_grace_s = 60.0
    writer_cfg = cfg.get("writer", {}) or {}
    if writer_cfg.get("notify_async", False):
        # Each HTTP call has a 0.5s socket timeout; reserve queue-drain
        # budgets after both warmup and measurement, plus existing grace.
        queued = int(writer_cfg.get("notify_queue_size",
                                    writer_cfg.get("notify_max_inflight", 256)))
        threads = max(1, int(writer_cfg.get("notify_threads", 1) or 1))
        shutdown_grace_s += 2 * 0.5 * (math.ceil(queued / threads) + 1)
    return max(1, int(math.ceil(
        2.0 * rendezvous_s + warmup_s + duration_s + shutdown_grace_s)))


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


# --------------------------------------------------------------------------
# node-level monitors (k8s): per-run monitor DaemonSet
# --------------------------------------------------------------------------
# start_profiling targets "nodes". In k8s mode the run's nodes are PODS, so
# the mem/urma samplers would run once per pod inside containers: host memory
# numbers survive (container /proc + /sys are host views) but process
# visibility is per-pod and mdcli (installed on the NODE, never in the
# benchmark image) is unreachable. This DaemonSet gives every node exactly one
# monitor pod that
#   - runs with hostPID (mem.proc sees every host process)
#   - is privileged and borrows the node-installed tools via nsenter/chroot
#     (`_host_exec` prefix, default "nsenter -t 1 -m -n --")
#   - mounts the same per-run hostPath log root as the benchmark pods, so the
#     sampler scripts and CSVs land on node disk and the existing collect /
#     report.reduce_monitors path works unchanged.
# The sampler loop itself stays INSIDE the container (only each mdcli call
# crosses into the host), so pod deletion / namespace teardown always kills
# it — no orphan host process to reap.
#
# Routing: these pseudo-nodes are handed to start_profiling ALONGSIDE the
# involved benchmark pods. prof_core routes the node-level samplers
# (MONITOR_ARTIFACTS: mem/urma) to the ``_monitor`` pseudo-nodes ONLY, and the
# in-container tools (spdiag/perf/runqlat/offcputime) to the benchmark pods
# ONLY. spdiag is per-container (SHM /spdiag_shm_default) and no instrumented
# process runs in a monitor pod, so it must never target one (there `restart`
# has no initialized SHM to work on). spdiag itself is status-driven:
# `spdiag status` -> not running => start, is running => restart.
_K8S_MONITOR_TIMEOUT_S = 60


def _monitor_template_path(cluster):
    k8s = cluster.get("k8s") or {}
    return k8s.get("monitor_template") or os.path.join(
        CONFIGS_DIR, "_k8s", "templates", "monitor-daemonset.yaml.tpl")


def _render_monitor_daemonset_yaml(cluster, ts, cfg_name):
    k8s = k8s_settings(cluster)
    run_label = k8s_run_label(ts, cfg_name)
    name = (k8s_pod_name(ts, cfg_name) + "-monitor")[:63].strip("-")
    with open(_monitor_template_path(cluster), "r", encoding="utf-8") as f:
        text = f.read()
    repl = {
        "{{MC_MONITOR_NAME}}": name,
        "{{MC_NAMESPACE}}": k8s["namespace"],
        "{{MC_RUN_LABEL}}": run_label,
        "{{MC_IMAGE}}": k8s["image"],
        "{{MC_HOST_LOG_DIR}}": host_log_dir(cluster, ts),
        "{{MC_LOG_MOUNT}}": k8s["pod_workspace"] + "/logs/" + ts,
    }
    for key, val in repl.items():
        text = text.replace(key, val)
    if "{{MC_" in text:
        raise RuntimeError("monitor DaemonSet template has unresolved "
                           "{{MC_*}} placeholders")
    return name, run_label, text


def _k8s_monitor_pods(cluster, run_label):
    """[(pod, phase, podIP, hostIP, nodeName)] of the monitor DaemonSet."""
    c = cluster["_controller"]
    ns = k8s_settings(cluster)["namespace"]
    out, rc = controller_run(
        c, ["-n", ns, "get", "pods", "-l",
            "app=mooncake-bench-monitor,run={}".format(run_label), "-o",
            "jsonpath={range .items[*]}{.metadata.name}{'|'}"
            "{.status.phase}{'|'}{.status.podIP}{'|'}{.status.hostIP}{'|'}"
            "{.spec.nodeName}{'\\n'}{end}"], check=False)
    if rc != 0:
        raise RuntimeError("cannot list monitor pods (run={}): {}".format(
            run_label, out.strip()[:200]))
    pods = []
    for line in out.strip().splitlines():
        parts = line.split("|")
        if len(parts) >= 5 and parts[0]:
            pods.append(tuple(parts[:5]))
    return pods


def _wait_monitor_daemonset_ready(cluster, name, ns, timeout=None,
                                  poll=2.0):
    """Poll the DaemonSet until numberReady == desiredNumberScheduled."""
    if timeout is None:
        timeout = _K8S_MONITOR_TIMEOUT_S
    c = cluster["_controller"]
    deadline = time.time() + timeout
    ready, want = 0, 0
    while True:
        out, _ = controller_run(
            c, ["-n", ns, "get", "daemonset", name, "-o",
                "jsonpath={.status.numberReady} {.status.desiredNumberScheduled}"],
            check=False)
        toks = out.split()
        try:
            ready, want = int(toks[0]), int(toks[1])
        except (IndexError, ValueError):
            ready, want = 0, 0
        if want and ready >= want:
            return
        if time.time() >= deadline:
            tail = ""
            try:
                pods = controller_run(
                    c, ["-n", ns, "get", "pods", "-l",
                        "app=mooncake-bench-monitor",
                        "-o", "wide"], check=False)[0]
                tail = pods.strip()[-1200:]
                events = _k8s_all_events(c, ns)
                if events:
                    tail += "\n" + "\n".join(events[-15:])
            except Exception:
                pass
            raise RuntimeError(
                "monitor DaemonSet {} not ready in {}s ({}/{} pods ready)\n{}"
                .format(name, int(timeout), ready, want, tail))
        time.sleep(poll)


def ensure_k8s_monitor_daemonset(cluster, ts, cfg_name, involved_nodes,
                                 dry_run=False):
    """Create the per-config node-monitor DaemonSet and return its pseudo-node
    names (one per host, so start_profiling gets node-level targets)."""
    cache = cluster.setdefault("_k8s_monitor_by_cfg", {})
    if cfg_name in cache:
        return list(cache[cfg_name])
    k8s = k8s_settings(cluster)
    c = cluster["_controller"]
    ns = k8s["namespace"]
    name, run_label, yaml_text = _render_monitor_daemonset_yaml(
        cluster, ts, cfg_name)
    ds_names = cluster.setdefault("_k8s_monitor_ds", [])
    if name not in ds_names:
        ds_names.append(name)
    involved_nodes = list(involved_nodes or [])
    if dry_run:
        log("[k8s] (dry-run) would create monitor DaemonSet {} (ns={})"
            .format(name, ns))
        cache[cfg_name] = involved_nodes
        return involved_nodes
    # one monitor per DISTINCT host: this is the whole point (pod-level
    # fallback still works if the DaemonSet cannot start)
    hosts = set()
    for node_name in involved_nodes:
        try:
            n = get_node(cluster, node_name)
        except Exception:
            continue
        if n.get("host_ip"):
            hosts.add(n["host_ip"])
    import tempfile
    local_dir = tempfile.mkdtemp(prefix="mc-monitor-")
    local_yaml = os.path.join(local_dir, name + ".yaml")
    try:
        with open(local_yaml, "w", encoding="utf-8", newline="\n") as f:
            f.write(yaml_text)
        yaml_dir = posixpath.join(k8s["controller_workspace"], "pods")
        controller_put(c, [local_yaml], yaml_dir)
        remote_yaml = posixpath.join(yaml_dir, name + ".yaml")
        log_detail("[k8s] monitor DaemonSet manifest: template={} -> {}:{}"
                   .format(os.path.relpath(_monitor_template_path(cluster),
                                           ROOT),
                           controller_desc(c), remote_yaml))
        out, rc = controller_run(
            c, ["-n", ns, "apply", "-f", remote_yaml], check=False)
        if rc != 0:
            raise RuntimeError("kubectl apply failed for monitor DaemonSet "
                               "{}: {}".format(name, out.strip()[:400]))
    finally:
        shutil.rmtree(local_dir, ignore_errors=True)
    t0 = time.time()
    _wait_monitor_daemonset_ready(cluster, name, ns)
    # host-side tool wrapper: nsenter borrows the node binaries/libs/devices
    # (chroot /host is the lower-privilege fallback; "" = pod-only)
    host_exec = str(k8s.get("monitor_host_exec",
                            "nsenter -t 1 -m -n --") or "").strip()
    nodes = []
    for pod, phase, pod_ip, host_ip, node_name in _k8s_monitor_pods(
            cluster, run_label):
        if phase != "Running":
            continue
        if hosts and host_ip and host_ip not in hosts:
            continue
        cluster.setdefault("nodes", {})[pod] = {
            "kind": "k8s-pod", "_pod": pod, "_ns": ns,
            "_ctx": c.get("_ctx", ""), "_controller": c,
            "_controller_workspace": k8s.get("controller_workspace", ""),
            "_run_label": run_label, "_role": "monitor", "_monitor": True,
            "_node_name": node_name, "_host_exec": host_exec,
            "ip": pod_ip or host_ip, "fabric_ip": pod_ip or "",
            "host_ip": host_ip}
        nodes.append(pod)
    if not nodes:
        raise RuntimeError(
            "monitor DaemonSet {} has no Running pods".format(name))
    cache[cfg_name] = list(nodes)
    log("[k8s] node monitor DaemonSet {} ready: {} Pods / {} nodes @{:.1f}s{}"
        .format(name, len(nodes),
                len({(cluster["nodes"].get(p) or {}).get("host_ip")
                     for p in nodes}),
                time.time() - t0,
                " (host tools borrowed via {!r})".format(host_exec)
                if host_exec else " (container tools only)"))
    return list(nodes)


def _delete_monitor_daemonset(cluster, dry_run=False):
    """Delete the per-config monitor DaemonSet(s) and drop their pseudo-nodes.
    Never raises (profiling must not affect the measurement path)."""
    names = cluster.pop("_k8s_monitor_ds", None) or []
    cluster.pop("_k8s_monitor_by_cfg", None)
    c = cluster.get("_controller")
    if c is not None and names:
        try:
            ns = k8s_settings(cluster)["namespace"]
        except Exception:
            ns = ""
        for name in names:
            if dry_run:
                log("[k8s] (dry-run) would delete DaemonSet {} (ns={})"
                    .format(name, ns))
                continue
            controller_run(c, ["-n", ns, "delete", "daemonset", name,
                               "--ignore-not-found=true"], check=False)
            log("[k8s] delete DaemonSet {} (ns={})".format(name, ns))
    for key in [k for k, v in (cluster.get("nodes") or {}).items()
                if (v or {}).get("_monitor")]:
        cluster["nodes"].pop(key, None)


def _profiling_targets(cluster, matrix, involved_nodes, ts, cfg_name,
                       dry=False):
    """Pick the nodes start_profiling should target.

    ssh mode: unchanged involved nodes.
    k8s mode: the involved benchmark pods PLUS one monitor DaemonSet pod per
    HOST (node-level mem + host mdcli); prof_core routes each tool to its
    fitting targets by the pseudo-nodes' ``_monitor`` flag. Falls back to the
    involved pods (pod-level monitors) when the DaemonSet cannot be created —
    profiling must never fail the scenario."""
    if not matrix or prof_core is None:
        return involved_nodes
    tools = matrix.get("tools") or {}
    if not any((tools.get(t) or {}).get("enabled")
               for t in prof_core.MONITOR_ARTIFACTS):
        return involved_nodes
    if cluster.get("_controller") is None:
        return involved_nodes
    try:
        monitor_pods = ensure_k8s_monitor_daemonset(
            cluster, ts, cfg_name, involved_nodes, dry)
    except Exception as exc:
        log("[WARN] [profile] monitor DaemonSet unavailable -> falling back "
            "to pod-level monitors: {}".format(exc))
        return involved_nodes
    # Both target kinds travel in one list; start_profiling splits the tools
    # by the _monitor flag (node-level samplers vs in-container tools).
    return list(involved_nodes) + [p for p in monitor_pods
                                   if p not in involved_nodes]


def _stop_profiling(cluster, profile_sess, local_result_dir, cfg_name,
                    dry=False):
    """Stop samplers, collect artifacts, then tear down the monitor DaemonSet.
    Never raises (same WARN-only contract as prof_core itself)."""
    try:
        if profile_sess is not None and prof_core is not None:
            prof_core.stop_and_collect(
                cluster, profile_sess,
                os.path.join(local_result_dir, "logs", cfg_name, "profiling"))
    except Exception as exc:
        log("[WARN] [profile] stop/collect failed: {}".format(exc))
    try:
        _delete_monitor_daemonset(cluster, dry_run=dry)
    except Exception as exc:
        log("[WARN] [profile] monitor DaemonSet teardown failed: {}".format(exc))


_SITE_DU_TIMEOUT_S = 120


def _site_du_snapshot(cluster, ts, cfg_name, involved_nodes, local_result_dir,
                      remote_log_dir, dry=False):
    """配置收尾时在节点侧实测现场日志目录大小（du -sb，apparent bytes）。

    时机：成功路径、profiling 落盘之后、pod 销毁之前（失败/STOP 不调用）。
    k8s 经 kubectl exec 进该宿主任一存活 client pod（pod 内路径 = 挂载视图），
    ssh 直接远端执行；输出路径统一换算为节点侧真实路径。
    结果写入 logs/<cfg>/report/site_sizes.json（report/ 内：成功清理保留、
    --rebuild-report 可复用）。WARN-only：du 不可用时静默回退旧口径。"""
    if dry:
        return None
    is_k8s = cluster.get("_controller") is not None
    k8s = {}
    host_root = ""
    if is_k8s:
        try:
            k8s = k8s_settings(cluster)
            host_root = (k8s.get("host_log_root") or "").rstrip("/")
            pod_ws = (k8s.get("pod_workspace") or "").rstrip("/")
        except Exception:
            return None
        if not host_root or not pod_ws:
            return None
        root_view = pod_ws + "/logs/" + ts
    else:
        root_view = str(remote_log_dir or "").rstrip("/")
        if not root_view:
            return None
    root_site = (_pod_path_to_site(cluster, root_view) if is_k8s
                 else root_view).rstrip("/")
    cmd = "du -sb {root} {root}/* {root}/profiling/* 2>/dev/null".format(
        root=root_view)
    # 每宿主一次：k8s 挑一个存活 client pod；ssh 直接 ssh 节点
    targets = {}
    for node_name in sorted(involved_nodes or []):
        try:
            n = get_node(cluster, node_name)
        except Exception:
            continue
        try:
            disp = _node_display(cluster, n, node_name) or node_name
        except Exception:
            disp = node_name
        if is_k8s:
            host = n.get("host_ip") or ""
            pod = n.get("_pod") or ""
            if not host or not pod or host in targets:
                continue
            targets[host] = (disp, n, pod)
        else:
            targets[disp] = (disp, n, "")
    nodes = {}
    for _key, (disp, n, pod) in sorted(targets.items()):
        try:
            if is_k8s:
                ns = k8s.get("namespace") or ""
                out, rc = controller_run(
                    cluster["_controller"],
                    ["-n", ns, "exec", pod, "--", "sh", "-c", cmd],
                    check=False, timeout=_SITE_DU_TIMEOUT_S)
                if rc != 0:
                    raise RuntimeError("kubectl exec rc={}".format(rc))
            else:
                out = ssh_check_output(n["ip"], n, cmd, quiet=True)
        except Exception as exc:
            log("[WARN] [site] du failed on {}: {}".format(disp, exc))
            continue
        dirs = {}
        total = 0
        for line in str(out or "").splitlines():
            raw = line.split("\t", 1)
            if len(raw) != 2 or not raw[0].strip().isdigit():
                continue
            size = int(raw[0].strip())
            path = raw[1].strip().rstrip("/")
            if not path:
                continue
            site_path = _pod_path_to_site(cluster, path) if is_k8s else path
            dirs[site_path] = size
            if site_path == root_site:
                total = size
        if not dirs:
            log("[WARN] [site] du produced no data on {} (du missing?)"
                .format(disp))
            continue
        nodes[disp] = {"total": total, "root": root_site, "dirs": dirs}
    if not nodes:
        return None
    out_dir = os.path.join(local_result_dir, "logs", cfg_name, "report")
    try:
        os.makedirs(out_dir, exist_ok=True)
        with open(os.path.join(out_dir, "site_sizes.json"), "w",
                  encoding="utf-8") as f:
            json.dump({"nodes": nodes}, f, ensure_ascii=False,
                      separators=(",", ":"))
    except OSError as exc:
        log("[WARN] [site] write site_sizes.json failed: {}".format(exc))
        return None
    return {"nodes": nodes}


def _pipeline_reader_launch_cmd(launch_prefix, remote_runner, merged_log,
                                client_id, reader_threads, port,
                                client_timeout_s, reader_op, numa_arg,
                                nohup_log):
    """SSH launch command for one standalone pipeline reader process.

    The argument order is positionally bound to the format string; keep
    ``--timeout`` paired with ``client_timeout_s`` and ``--operation`` with
    ``reader_op``. A pre-merge revision passed them in the opposite order
    (``reader_op, _client_timeout``), so every reader exited with argparse
    rc=2 before registering in etcd and each writer waited out its rendezvous
    (``0/5 readers``). Regression test: test_pipeline_launch_args.py."""
    return ("nohup {}bash {} pipeline {} --client-id {} --num-threads {} "
            "--role reader --listen-port {} --warmup false --timeout {} "
            "--operation {} {} > {} 2>&1 & echo $!").format(
        launch_prefix, remote_runner, merged_log, client_id, reader_threads,
        port, client_timeout_s, reader_op, numa_arg, nohup_log)


def _pipeline_writer_launch_cmd(launch_prefix, remote_runner, merged_log,
                                client_id, port, warmup, writer_op, kc_arg,
                                numa_arg, nohup_log):
    """SSH launch command for one standalone pipeline writer process.

    Same positional contract as the reader builder: ``--listen-port`` pairs
    with ``port``, ``--warmup`` with ``warmup``, ``--operation`` with
    ``writer_op`` and the following two placeholders with ``kc_arg`` /
    ``numa_arg``."""
    return ("nohup {}bash {} pipeline {} --client-id {} --role writer "
            "--listen-port {} --warmup {} --operation {}{} {} > {} 2>&1 "
            "& echo $!").format(
        launch_prefix, remote_runner, merged_log, client_id, port, warmup,
        writer_op, kc_arg, numa_arg, nohup_log)


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
    # record the REAL cluster-wide process counts for the CSV (client_count);
    # write_scenario_csv rebuilds a node_count=1 plan, so it cannot derive N.
    cfg["_writer_count"] = len(plan["writers"])
    cfg["_reader_count"] = len(plan["readers"])
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
    # pipeline has NO key_count: the per-writer key space is derived from
    # the store capacity (see _pipeline_key_budget); a leftover field in a
    # scenario is stripped by the loader with a WARN
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
    local_result_dir = getattr(args, "_result_dir_path", None) or \
        os.path.join(RESULTS_DIR, ts)
    remote_dir, remote_scripts, remote_log_dir, remote_runner_basic, \
        remote_runner_pipeline, local_log_dir = \
        scenario_paths(globals_cfg, ts, cfg_name, local_result_dir)
    remote_runner = remote_runner_pipeline
    if not dry:
        os.makedirs(local_log_dir, exist_ok=True)
    log_phase("场景 {} (engine=python, scenario=pipeline)".format(cfg_name))
    # ---- key-space budget, resolved BEFORE any cluster resource is touched
    # (configmap/deployment/staging): a missing store source or a too-small
    # store fails in seconds with zero leftovers. One cap per sweep size.
    sizes = data_size_list(cfg)
    _a, _b, _eff, _src = _store_capacity_resolution(globals_cfg)
    _caps = {}
    for _ds in sizes:
        _caps[_ds] = _pipeline_key_budget(
            globals_cfg, len(plan["writers"]), parse_data_size(_ds),
            int(cfg.get("batch_size", 1) or 1), _ds)
    cfg["_key_space"] = {str(_ds): _kc for _ds, _kc in _caps.items()}
    log("key space (capacity source={}, effective={}):".format(
        _src, _fmt_gib(_eff)))
    for _ds in sizes:
        _kc = _caps[_ds]
        log("    {}: {} keys/writer × {} writers = {} (budget ratio {:g})"
            .format(
            _ds, _kc, len(plan["writers"]),
            _fmt_gib(len(plan["writers"]) * _kc * parse_data_size(_ds)),
            _pipeline_key_space_ratio(globals_cfg)))
    # Parameter block (engine x scenario, no cluster resource touched yet);
    # resolved here so the console order is 参数 -> 资源创建.
    warmup = "false" if args.no_warmup else "true"
    client_timeout_s = _pipeline_client_timeout(
        cfg, rendezvous_timeout_s, warmup_enabled=(warmup == "true"))
    if getattr(args, "parallel_ready_dir", None):
        client_timeout_s += _PARALLEL_READY_TIMEOUT_S
    reader_threads = plan["reader_threads"]
    hb_interval = _heartbeat_interval(globals_cfg)
    _pl_total = (float(cfg.get("warmup", {}).get("duration", 0) or 0)
                 + float(cfg.get("duration", 30)))
    numa_bind = cfg.get("numa_bind")
    if numa_bind is None:
        numa_bind = globals_cfg.get("numa_bind")
    numa_bind = True if numa_bind is None else bool(numa_bind)
    cpu_shard = int(cfg.get("cpu_shard", 0) or 0)
    if cpu_shard > 0 and not numa_bind:
        numa_bind = True
    if is_k8s:
        numa_bind = False
        cpu_shard = 0
    log("[pipeline] params: writer_op={} reader_op={} topo={} protocol={}{} "
        "N={} writers={} readers={} notify_count={} total_qps={} "
        "per_writer_qps={} traffic={} run_id={} client_timeout={}s sizes={} "
        "NUMA/bind={}".format(
            writer_op, reader_op, topology, cfg["protocol"],
            "" if is_k8s else " primary_node={}".format(host),
            n_nodes, len(plan["writers"]), len(plan["readers"]),
            plan["notify_count"], int(plan["total_qps"]),
            plan["per_writer_qps"], cfg.get("traffic_mode", "pulse"),
            run_id, client_timeout_s, ",".join(sizes),
            "off (container)" if is_k8s else ("on" if numa_bind else "off")))
    scenario_params = base_scenario_params(cfg, work_mode, topology, primary_node_name)
    scenario_params.update({
        "BENCH_WRITER_COUNT": str(len(plan["writers"])),
        "BENCH_READER_COUNT": str(len(plan["readers"])),
        "BENCH_TARGET_QPS": str(plan["per_writer_qps"]),
        "BENCH_NOTIFY_COUNT": str(plan["notify_count"]),
        "BENCH_NOTIFY_THREADS": str(plan["notify_threads"]),
        "BENCH_NOTIFY_ASYNC": "true" if plan["notify_async"] else "false",
        "BENCH_NOTIFY_MAX_INFLIGHT": str(plan["notify_max_inflight"]),
        "BENCH_NOTIFY_QUEUE_SIZE": str(plan["notify_queue_size"]),
        "BENCH_NOTIFY_PIPELINE": ",".join(cfg.get("notify_pipeline", [])),
        "BENCH_TRAFFIC_MODE": cfg.get("traffic_mode", "pulse"),
        "BENCH_DURATION": str(cfg.get("duration", 30)),
        "BENCH_BATCH_SIZE": str(cfg.get("batch_size", 1)),
        # clear basic-only vars that are meaningless for pipeline
        "BENCH_OPERATION": "",
        "BENCH_WARMUP_DURATION": str(cfg.get("warmup", {}).get("duration", 0)),
        # etcd rendezvous scope + barrier timeouts (client-side fallbacks)
        "BENCH_RUN_ID": run_id,
        # Client-side /start wait window = this value. In parallel rank+recall
        # the client idles at the cross-child gate (up to
        # _PARALLEL_READY_TIMEOUT_S) before /start arrives, so extend the
        # client's window by the same ceiling; orchestrator-side barriers
        # keep the configured rendezvous_timeout_s.
        "BENCH_RENDEZVOUS_TIMEOUT": str(
            rendezvous_timeout_s + (_PARALLEL_READY_TIMEOUT_S
                                    if getattr(args, "parallel_ready_dir", None)
                                    else 0)),
    })
    cluster_session = build_cluster_session(cluster, globals_cfg, remote_dir)
    # k8s: ConfigMap (scripts + shared env.sh) must exist before pods mount it
    reader_pids = []
    writer_pids = []
    profile_sess = None  # profiling session (started at Step 1 or Step 3)
    stage = "configmap/scripts"
    log_phase("资源创建", level=2)
    try:
        ensure_configmap(cluster, scenario_params, cluster_session, ts, cfg_name, dry)
        stage = "deploy pods"
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
                    host_ip = "" if dry else _k8s_pod_host_ip(
                        cluster["_controller"], _k8s["namespace"], pod)
                    cluster.setdefault("nodes", {})[pod] = {
                        "kind": "k8s-pod", "_pod": pod, "_ns": _k8s["namespace"],
                        "_ctx": cluster["_controller"].get("_ctx", ""),
                        "_controller": cluster["_controller"],
                        "_controller_workspace": _k8s["controller_workspace"],
                        "_run_label": _run_label, "_role": role,
                        "ip": ip, "fabric_ip": ip, "host_ip": host_ip}
            involved_nodes = []
            for e in plan["writers"] + plan["readers"]:
                if e["node"] not in involved_nodes:
                    involved_nodes.append(e["node"])
            primary_node_name = (plan["writers"] or plan["readers"])[0]["node"]
            node = get_node(cluster, primary_node_name)
            host = node["ip"]
            if plan.get("dual"):
                _total, _extra = len(plan["clients"]), " (dual)"
            else:
                _total = len(plan["writers"]) + len(plan["readers"])
                _extra = " ({} writers + {} readers)".format(
                    len(plan["writers"]), len(plan["readers"]))
            if n_nodes and _total % n_nodes == 0:
                log("[k8s] pipeline pods ready: {}Pod/Node * {}Nodes = "
                    "{}Pods{} (run={})".format(
                        _total // n_nodes, n_nodes, _total, _extra,
                        _run_label))
            else:
                log("[k8s] pipeline pods ready: {}Pods / {}Nodes{} "
                    "(run={})".format(_total, n_nodes, _extra, _run_label))
        stage = "stage scripts"
        deploy_scripts(cluster, involved_nodes, scenario_params, cluster_session,
                       remote_scripts, remote_log_dir, dry)
        _STOP_CTX["report"] = {
            "cfg": cfg, "cfg_name": cfg_name, "result_dir": local_result_dir,
            "local_log_dir": local_log_dir, "remote_log_dir": remote_log_dir,
            "involved_nodes": involved_nodes, "mode": "pipeline",
        }
        # NUMA probes (resolution already happened in the parameter block;
        # k8s containers never bind).
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
        current_ds = None
        for ds in sizes:
            if ds != current_ds:
                # data_size changed: (re)build value.bin at this size on every
                # node that runs a writer or reader. Per-pod work goes to the
                # full log; the console gets ONE aggregate line.
                ds_bytes = parse_data_size(ds)
                check_cmd = value_check_cmd(cluster_session["BENCH_REMOTE_DATA_DIR"], scenario, ds_bytes)
                _gen, _cached, _t0 = 0, 0, time.time()
                for node_name in involved_nodes:
                    n = get_node(cluster, node_name)
                    data_status = ssh_check_output(n["ip"], n, check_cmd, dry_run=dry)
                    if data_status != "EXISTS":
                        stage = "generate-data ({})".format(ds)
                        run_stage(n["ip"], n, remote_runner,
                                  "generate-data", remote_log_dir, cfg_name,
                                  extra="--data-size {}".format(ds), dry_run=dry,
                                  pull_to=local_log_dir, node_name=node_name,
                                  quiet=True, tag="pipeline")
                        _gen += 1
                    else:
                        _cached += 1
                log("value.bin ({}): {}/{} pods ready (generated {}, cached "
                    "{}, {:.1f}s)".format(
                        ds, _gen + _cached, len(involved_nodes), _gen, _cached,
                        time.time() - _t0))
                current_ds = ds
            # cap precomputed at scenario validation (see the key-space
            # preview block); always injected — the client's BENCH_KEY_COUNT
            # env default (1000) must never decide the write space
            kc = _caps[ds]
            kc_arg = " --key-count {}".format(kc)
            stage = "remove-keys (pre-run, {})".format(ds)
            log_phase("清库 remove-keys", level=2)
            _remove_keys_stage(cluster, globals_cfg, host, node,
                               remote_runner, remote_log_dir, cfg_name,
                               cfg_name, "full", "{} start".format(ds),
                               dry=dry, pull_to=local_log_dir,
                               node_name=primary_node_name, tag="pipeline")
            # fresh rendezvous state per size iteration (stale registrations
            # from a previous size must never leak into this one)
            reader_pids = []
            writer_pids = []
            if not dry:
                _etcd_delete_prefix(cluster, etcd_endpoints,
                                    "mcbench/{}/".format(run_id))
            log_phase("启动实例", level=2)
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
                            cluster, matrix,
                            _profiling_targets(cluster, matrix,
                                               involved_nodes, ts, cfg_name,
                                               dry),
                            cfg_name,
                            remote_profiling_dir(remote_log_dir, cfg_name))
                    except Exception as exc:
                        log("[WARN] [profile] start failed: {}".format(exc))
            if plan.get("dual"):
                stage = "launch clients ({})".format(ds)
                # ---- dual-role flow: every client hosts writer+reader behind
                # one HTTP port; pids ledger into BOTH lists so the shared
                # fail-path (graceful /shutdown + kill + pull) just works ----
                for c in plan["clients"]:
                    rn = get_node(cluster, c["node"])
                    # writer and reader output NEVER share a file: logs and
                    # per-role latency/queue-wait plots stay separated
                    merged_log = "{}.dual_{}_c{}_p{}.writer.log".format(
                        _pod_base(base_noext, rn), c["node"], c["id"], c["port"])
                    reader_log = "{}.dual_{}_c{}_p{}.reader.log".format(
                        _pod_base(base_noext, rn), c["node"], c["id"], c["port"])
                    nohup_log = merged_log.replace(".log", ".nohup")
                    # process-level client-glog (SDK glog + fatal) -> <base>.glog
                    glog_file = merged_log.replace(".writer.log", ".glog")
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
                    ccmd = ("MC_READER_LOG={rlog} MC_GLOG_FILE={glog} nohup {p}bash {runner} pipeline "
                            "{log} --client-id {cid} --role both --listen-port {port} "
                            "--writer-operation {wop} --reader-operation {rop} "
                            "--num-threads {thr} --warmup {wu} "
                            "--timeout {timeout}{kc}{na} "
                            "> {nohup} 2>&1 & echo $!").format(
                        rlog=reader_log, glog=glog_file, p=launch_prefix, runner=remote_runner,
                        log=merged_log, cid=c["id"], port=c["port"],
                        wop=writer_op, rop=reader_op, thr=reader_threads,
                        wu=warmup, timeout=client_timeout_s, kc=kc_arg,
                        na=numa_arg, nohup=nohup_log)
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
                log("[pipeline] launched {} dual clients, waiting rendezvous"
                    .format(len(plan["clients"])))
                # barriers: every client registered BOTH roles, then trigger
                stage = "rendezvous ({})".format(ds)
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
                    log("[pipeline] rendezvous: {}/{} dual clients "
                        "registered ({:.1f}s)".format(
                            len(plan["clients"]), len(plan["clients"]),
                            time.time() - t0))
                    # Cross-child rank+recall /start alignment: the full
                    # client pool is registered; hold /start until the peer
                    # child reaches the same gate so both children's
                    # measured windows overlap.
                    stage = "/start ({})".format(ds)
                    _parallel_start_barrier(args)
                    _items = [(get_node(cluster, c["node"]), c["port"], "",
                               "dual c{}".format(c["id"]))
                              for c in plan["clients"]]
                    _res = _pipeline_http_parallel(cluster, _items, "/start")
                    _fail = [r for r in _res if not r[1]]
                    if _fail:
                        raise RuntimeError(
                            "pipeline trigger failed for {}/{} clients: {} "
                            "({})".format(len(_fail), len(_items),
                                          _fail[0][0], _fail[0][2]))
                    log("[pipeline] /start sent to {} dual clients "
                        "(parallel x{})".format(
                            len(_items), min(_PIPELINE_PARALLEL,
                                             len(_items))))
                    # Writers publish completion only after draining notify.
                    # Keep all reader endpoints alive until every writer is
                    # done, including warmup drain and measured drain time.
                    stage = "measurement ({})".format(ds)
                    log_phase("测量", level=2)
                    completion_timeout = client_timeout_s
                    log("pipeline dual: waiting for all writers and notify drain")
                    _pipeline_barrier(cluster, etcd_endpoints,
                                      "mcbench/{}/writers_done/".format(run_id),
                                      len(plan["clients"]), completion_timeout,
                                      "completed dual writers", run_id)
                    _pid_by_obj = {id(e): (pid or "").strip()
                                   for e, pid in writer_pids}
                    _items = [(get_node(cluster, c["node"]), c["port"],
                               _pid_by_obj.get(id(c), ""),
                               "dual c{}".format(c["id"]))
                              for c in plan["clients"]]
                    stage = "stop/collect ({})".format(ds)
                    for _what, _ok, _err in _pipeline_http_parallel(
                            cluster, _items, "/shutdown"):
                        if not _ok:
                            log("[WARN] {} shutdown: {}".format(_what, _err))
                    _w_t0, _w_exited = time.time(), 0
                    _hb = (_Heartbeat("pipeline", _pl_total, hb_interval)
                           if hb_interval else None)
                    for c, pid in writer_pids:
                        if not dry and pid:
                            exited = wait_process_exit(
                                cluster, c["node"], pid,
                                "dual c{}".format(c["id"]),
                                60, dry_run=dry, expected_s=60,
                                marker=_PIPELINE_PID_MARKER, heartbeat=_hb)
                            if exited:
                                _w_exited += 1
                                _check_exitrc(
                                    cluster, c["node"],
                                    "{}.dual_{}_c{}_p{}.writer.log.exitrc".format(
                                        _pod_base(base_noext,
                                                  get_node(cluster, c["node"])),
                                        c["node"], c["id"], c["port"]),
                                    "dual c{}".format(c["id"]))
                    if not dry:
                        log("clients exited: {}/{} ({:.1f}s)".format(
                            _w_exited, len(writer_pids), time.time() - _w_t0))
                    log_phase("收集", level=2)
                    _groups = []
                    for c in plan["clients"]:
                        _rn = get_node(cluster, c["node"])
                        _pb = _pod_base(base_noext, _rn)
                        _groups.append((_rn,
                                        [("{}.dual_{}_c{}_p{}.writer.log".format(
                                            _pb, c["node"], c["id"], c["port"]),
                                          "writer"),
                                         ("{}.dual_{}_c{}_p{}.reader.log".format(
                                            _pb, c["node"], c["id"], c["port"]),
                                          "reader"),
                                         ("{}.dual_{}_c{}_p{}.glog".format(
                                            _pb, c["node"], c["id"], c["port"]),
                                          None)],
                                        "dual c{}".format(c["id"]),
                                        c["node"]))
                    _nb = _map_and_pull_logs(
                        cluster, _groups, local_log_dir, dry, globals_cfg,
                        remote_scripts,
                        pull_raw=bool(getattr(args, "pull_raw_logs", False)
                                      or getattr(args, "png", False)),
                        size=ds)
                    _log_node_volumes(_nb, cfg_name)
                    log("logs: {} clients / {} pods processed".format(
                        len(_groups), len(_groups)))
            else:
                # Step 1: start all readers (one process per reader). Each reader
                # binds its HTTP /notify server and SELF-REGISTERS into etcd
                # (host=POD_IP/SETUP_FABRIC_IP, port, node); no peer addresses
                # are injected by the orchestrator anymore. Log names carry the
                # node segment: per-node port numbering (9201+i) is no longer
                # globally unique once several nodes each run x readers.
                stage = "launch readers ({})".format(ds)
                for r in plan["readers"]:
                    rn = get_node(cluster, r["node"])
                    merged_log = "{}.reader_{}_r{}_p{}.log".format(
                        _pod_base(base_noext, rn), r["node"], r["index"], r["port"])
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
                    rcmd = _pipeline_reader_launch_cmd(
                        launch_prefix, remote_runner, merged_log, r["index"],
                        reader_threads, r["port"], client_timeout_s, reader_op,
                        numa_arg, nohup_log)
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
                log("[pipeline] launched {} readers, waiting rendezvous"
                    .format(len(plan["readers"])))
                # Step 2: etcd barrier — wait for the FULL reader pool to
                # self-register before any writer starts (readers that fail to
                # boot surface here as a timeout with the missing list)
                stage = "rendezvous (readers, {})".format(ds)
                if not dry:
                    t0 = time.time()
                    _pipeline_barrier(cluster, etcd_endpoints,
                                      "mcbench/{}/readers/".format(run_id),
                                      len(plan["readers"]), rendezvous_timeout_s,
                                      "readers", run_id)
                    log("[pipeline] rendezvous: {}/{} readers registered "
                        "({:.1f}s)".format(
                            len(plan["readers"]), len(plan["readers"]),
                            time.time() - t0))
                # Step 3: start all writers in parallel (one invocation per writer; the
                # explicit --client-id in EXTRA overrides the per-invocation default
                # and gives each writer a globally unique id). ALL writers run the
                # warmup phase so every writer's store/notify connections are warmed.
                if matrix and not dry and prof_core and profile_sess is None:
                    # per_stage trigger: profiling starts at the measurement window
                    try:
                        profile_sess = prof_core.start_profiling(
                            cluster, matrix,
                            _profiling_targets(cluster, matrix,
                                               involved_nodes, ts, cfg_name,
                                               dry),
                            cfg_name,
                            remote_profiling_dir(remote_log_dir, cfg_name))
                    except Exception as exc:
                        log("[WARN] [profile] start failed: {}".format(exc))
                stage = "launch writers ({})".format(ds)
                for w in plan["writers"]:
                    wn = get_node(cluster, w["node"])
                    merged_log = "{}.writer_{}_w{}.log".format(
                        _pod_base(base_noext, wn), w["node"], w["gid"])
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
                    wcmd = _pipeline_writer_launch_cmd(
                        launch_prefix, remote_runner, merged_log, w["gid"],
                        w["port"], warmup, writer_op, kc_arg, numa_arg,
                        nohup_log)
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
                log("[pipeline] launched {} writers, waiting rendezvous"
                    .format(len(plan["writers"])))
                # Step 4: etcd barrier — every writer registered (= reader list
                # pulled from etcd + HTTP /start server bound), then TRIGGER the
                # run: POST /start to each writer (k8s: direct pod IP; ssh:
                # loopback curl on the writer's node). All writers enter their
                # warmup + measured phases only after this synchronized start.
                stage = "rendezvous (writers, {})".format(ds)
                if not dry:
                    t0 = time.time()
                    _pipeline_barrier(cluster, etcd_endpoints,
                                      "mcbench/{}/writers/".format(run_id),
                                      len(plan["writers"]), rendezvous_timeout_s,
                                      "writers", run_id)
                    log("[pipeline] rendezvous: {}/{} writers registered "
                        "({:.1f}s)".format(
                            len(plan["writers"]), len(plan["writers"]),
                            time.time() - t0))
                    # Cross-child rank+recall /start alignment (see the dual
                    # branch above for rationale).
                    stage = "/start ({})".format(ds)
                    _parallel_start_barrier(args)
                    _items = [(get_node(cluster, w["node"]), w["port"], "",
                               "writer w{}".format(w["gid"]))
                              for w in plan["writers"]]
                    _res = _pipeline_http_parallel(cluster, _items, "/start")
                    _fail = [r for r in _res if not r[1]]
                    if _fail:
                        raise RuntimeError(
                            "pipeline trigger failed for {}/{} writers: {} "
                            "({})".format(len(_fail), len(_items),
                                          _fail[0][0], _fail[0][2]))
                    log("[pipeline] /start sent to {} writers "
                        "(parallel x{})".format(
                            len(_items), min(_PIPELINE_PARALLEL,
                                             len(_items))))
                # Step 5: wait for all writers to finish (warmup + measured run)
                stage = "measurement ({})".format(ds)
                log_phase("测量", level=2)
                warmup_duration = float(cfg.get("warmup", {}).get("duration", 0) or 0)
                duration = float(cfg.get("duration", 30))
                _wt0, _wex = time.time(), 0
                _hb = (_Heartbeat("pipeline", _pl_total, hb_interval)
                       if hb_interval else None)
                for w, pid in writer_pids:
                    if not dry and pid:
                        exited = wait_process_exit(
                            cluster, w["node"], pid,
                            "writer w{}".format(w["gid"]),
                            client_timeout_s, dry_run=dry,
                            expected_s=warmup_duration + duration,
                            marker=_PIPELINE_PID_MARKER, heartbeat=_hb)
                        if exited:
                            _wex += 1
                            _check_exitrc(
                                cluster, w["node"],
                                "{}.writer_{}_w{}.log.exitrc".format(
                                    _pod_base(base_noext,
                                              get_node(cluster, w["node"])),
                                    w["node"], w["gid"]),
                                "writer w{}".format(w["gid"]))
                if not dry:
                    log("writers exited: {}/{} ({:.1f}s)".format(
                        _wex, len(writer_pids), time.time() - _wt0))
                stage = "collect (writers, {})".format(ds)
                log_phase("收集 (writers)", level=2)
                _groups = [(get_node(cluster, w["node"]),
                            [("{}.writer_{}_w{}.log".format(
                                _pod_base(base_noext, get_node(cluster, w["node"])),
                                w["node"], w["gid"]), "writer"),
                             ("{}.writer_{}_w{}.glog".format(
                                _pod_base(base_noext, get_node(cluster, w["node"])),
                                w["node"], w["gid"]), "writer")],
                            "writer w{}".format(w["gid"]),
                            w["node"])
                           for w in plan["writers"]]
                _nb = _map_and_pull_logs(
                    cluster, _groups, local_log_dir, dry, globals_cfg,
                    remote_scripts,
                    pull_raw=bool(getattr(args, "pull_raw_logs", False)
                                  or getattr(args, "png", False)),
                    size=ds)
                _log_node_volumes(_nb, cfg_name)
                log("logs: {} writers / {} pods processed".format(
                    len(_groups), len(_groups)))
                # Step 6: shutdown readers via their /shutdown endpoint (k8s:
                # direct pod IP; ssh: loopback curl on the reader's node) and
                # wait for their final logs
                _pid_by_obj = {id(e): (pid or "").strip()
                               for e, pid in reader_pids}
                _items = [(get_node(cluster, r["node"]), r["port"],
                           _pid_by_obj.get(id(r), ""),
                           "reader r{}".format(r["index"]))
                          for r in plan["readers"]]
                for _what, _ok, _err in _pipeline_http_parallel(
                        cluster, _items, "/shutdown", dry=dry):
                    if not _ok:
                        log("[WARN] {} shutdown: {}".format(_what, _err))
                _rt0, _rex = time.time(), 0
                for r, pid in reader_pids:
                    if not dry and pid:
                        exited = wait_process_exit(
                            cluster, r["node"], pid,
                            "reader r{} (port {})".format(r["index"], r["port"]),
                            60, dry_run=dry, expected_s=60,
                            marker=_PIPELINE_PID_MARKER)
                        if exited:
                            _rex += 1
                            _check_exitrc(
                                cluster, r["node"],
                                "{}.reader_{}_r{}_p{}.log.exitrc".format(
                                    _pod_base(base_noext,
                                              get_node(cluster, r["node"])),
                                    r["node"], r["index"], r["port"]),
                                "reader r{}".format(r["index"]))
                if not dry:
                    log("readers exited: {}/{} ({:.1f}s)".format(
                        _rex, len(reader_pids), time.time() - _rt0))
                stage = "collect (readers, {})".format(ds)
                log_phase("收集 (readers)", level=2)
                _groups = [(get_node(cluster, r["node"]),
                            [("{}.reader_{}_r{}_p{}.log".format(
                                _pod_base(base_noext, get_node(cluster, r["node"])),
                                r["node"], r["index"], r["port"]), "reader"),
                             ("{}.reader_{}_r{}_p{}.glog".format(
                                _pod_base(base_noext, get_node(cluster, r["node"])),
                                r["node"], r["index"], r["port"]), "reader")],
                            "reader r{}".format(r["index"]),
                            r["node"])
                           for r in plan["readers"]]
                _nb = _map_and_pull_logs(
                    cluster, _groups, local_log_dir, dry, globals_cfg,
                    remote_scripts,
                    pull_raw=bool(getattr(args, "pull_raw_logs", False)
                                  or getattr(args, "png", False)),
                    size=ds)
                _log_node_volumes(_nb, cfg_name)
                log("logs: {} readers / {} pods processed".format(
                    len(_groups), len(_groups)))
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
                                    "{}.dual_{}_c{}_p{}.writer.nohup".format(
                                        _pod_base(base_noext, n), c["node"],
                                        c["id"], c["port"]))
                    else:
                        for r in plan["readers"]:
                            if r["node"] == node_name:
                                nohup_files.append(
                                    "{}.reader_{}_r{}_p{}.nohup".format(
                                        _pod_base(base_noext, n), r["node"],
                                        r["index"], r["port"]))
                        for w in plan["writers"]:
                            if w["node"] == node_name:
                                nohup_files.append(
                                    "{}.writer_{}_w{}.nohup".format(
                                        _pod_base(base_noext, n), w["node"],
                                        w["gid"]))
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
        stage = "remove-keys (config end)"
        log_phase("清理收尾", level=2)
        _remove_keys_stage(cluster, globals_cfg, host, node, remote_runner,
                           remote_log_dir, cfg_name, "remove-keys-end",
                           "full", "config end", dry=dry, tag="pipeline")
        # raw-log full pull only on explicit request (per-role stats were
        # pulled right after each role exited; failure paths pull raw)
        if getattr(args, "pull_raw_logs", False) or getattr(args, "png", False):
            pull_logs(cluster, involved_nodes, remote_log_dir, cfg_name,
                      local_log_dir, dry)
        echo_cpu_bindings(cfg_name, local_log_dir)
        # All pipeline processes exited naturally -> drop their ledger rows.
        if not dry:
            state_remove_cfg(cfg_name)
        # profiling: stop sampling + spdiag show + collect (never raises)
        if profile_sess is not None:
            _stop_profiling(cluster, profile_sess, local_result_dir, cfg_name,
                            dry=dry)
        # 现场日志实测大小（pod/profile 产物已全部落盘、pod 仍存活）
        _site_du_snapshot(cluster, ts, cfg_name, involved_nodes,
                          local_result_dir, remote_log_dir, dry=dry)
        # k8s pods stay alive past this return: main()'s sweep-end
        # remove-keys still needs a live pod (store keys are not in the
        # pod workspace); cleanup_pods runs in main() after sweep-end
        # Sweep-end remove-all is issued by main() on the last successful
        # config's node; hand it this config's coordinates. The report is
        # local post-processing: main() queues it on the background worker so
        # it overlaps the next config's deployment (kwargs ride along).
        return {"host": host, "node": primary_node_name, "runner": remote_runner,
                "remote_log_dir": remote_log_dir,
                "_report_kwargs": {
                    "nodes": len(involved_nodes),
                    "qps_target": plan["total_qps"],
                    "launch_mode": ("rank_recall" if plan.get("dual")
                                    else "single"),
                    "mode": "pipeline"}}
    except Exception as exc:
        # FAIL path: one line names scenario + failing stage + reason, then
        # the pod table and the raw-log directories. No log extraction.
        if not dry:
            class1 = is_k8s and stage.startswith("deploy")
            _scenario_failure_report(cluster, cfg_name, stage, exc,
                                     involved_nodes, remote_log_dir, ts,
                                     is_k8s, class1=class1)
            if class1:
                log("[FAIL] cleanup skipped (pods never started)")
            else:
                fail_cleanup_pipeline(cluster, plan, reader_pids, writer_pids,
                                      involved_nodes, remote_log_dir, cfg_name,
                                      local_log_dir,
                                      etcd_endpoints=etcd_endpoints,
                                      run_id=run_id,
                                      remove_host=locals().get("host"),
                                      remove_node=locals().get("primary_node_name"),
                                      remove_runner=locals().get("remote_runner"),
                                      globals_cfg=globals_cfg,
                                      failed_stage=stage)
                # profiling collect BEFORE the report so monitor CSVs reach it
                if profile_sess is not None:
                    _stop_profiling(cluster, profile_sess, local_result_dir,
                                    cfg_name, dry=dry)
                _generate_reports(cfg, local_log_dir, args, early_stop=True,
                                  nodes=len(involved_nodes),
                                  qps_target=plan["total_qps"],
                                  launch_mode="rank_recall" if plan.get("dual") else "single",
                                  mode="pipeline")
                # 复核台账：已死删行、杀不死的保留待下次 reap
                reap_state_entries(cluster)
            if is_k8s:
                # pods were Ready (or rolled out); keep Deployments + pods +
                # namespace for troubleshooting (STOP / --clean-up tears down)
                cluster["_k8s_keep_ns"] = True
            else:
                cleanup_pods(cluster, involved_nodes, dry)
            cfg["_fail_reported"] = True
        raise


# --------------------------------------------------------------------------
# kvtest scenario (C++ mooncake_kvtest: notify pipeline fully in C++)
# --------------------------------------------------------------------------

def _kvtest_deploy_binary(cluster, involved_nodes, local_binary, remote_scripts,
                          pod_workspace, is_k8s, dry_run):
    """kvtest.binary_local: push the C++ binary from the orchestrator to
    every node/pod and return the remote path the instances will run (no
    image rebuild / manual deployment needed). Returns "" when unset — the
    plain kvtest.binary name is then resolved from PATH on the target.
    k8s pods mount scripts/ read-only (ConfigMap), so the binary lands in
    the writable pod workspace under kvtest-bin/ instead."""
    local = str(local_binary or "").strip()
    if not local:
        return ""
    local = os.path.abspath(local)
    if not os.path.isfile(local):
        raise RuntimeError(
            "kvtest.binary_local not found on the orchestrator: {}"
            "".format(local))
    base = os.path.basename(local)
    bindir = (posixpath.join(pod_workspace or "/tmp", "kvtest-bin")
              if is_k8s else remote_scripts)
    lib_candidates = []
    for libdir in (os.path.join(os.path.dirname(local), "lib"),
                   os.path.join(os.path.dirname(os.path.dirname(local)),
                                "lib")):
        if os.path.isdir(libdir):
            lib_candidates.extend(glob.glob(os.path.join(libdir, "*.so*")))
    local_libs = sorted(set(os.path.abspath(p) for p in lib_candidates
                            if os.path.isfile(p)))
    for node_name in involved_nodes:
        n = get_node(cluster, node_name)
        run_ssh(n["ip"], n, "mkdir -p {}".format(bindir), dry_run=dry_run)
        scp_push(n["ip"], n, [local], bindir, dry_run=dry_run)
        if local_libs:
            remote_libdir = posixpath.join(bindir, "lib")
            run_ssh(n["ip"], n, "mkdir -p {}".format(remote_libdir),
                    dry_run=dry_run)
            scp_push(n["ip"], n, local_libs, remote_libdir,
                     dry_run=dry_run)
        run_ssh(n["ip"], n, "chmod +x {}".format(
            posixpath.join(bindir, base)), dry_run=dry_run)
    log("kvtest binary pushed: {} + {} shared object(s) -> {} target(s) "
        "at {}".format(base, len(local_libs), len(involved_nodes), bindir))
    return posixpath.join(bindir, base)


def _kvtest_store_targets(globals_cfg, cluster_session):
    """(metadata_server, master_server_address) for the kvtest store block,
    mirroring make_store() in mooncake_benchmark_client.py: etcd mode uses
    the etcd URI for both; http mode uses master_addr + metadata_server (or
    the derived http://<master>/metadata default)."""
    if str(globals_cfg.get("master_type", "") or "") == "etcd":
        eps = cluster_session.get("SETUP_ETCD_ENDPOINTS", "")
        if not eps:
            raise RuntimeError(
                "kvtest: master_type=etcd requires global.json etcd_endpoints")
        return eps, eps
    master = cluster_session.get("SETUP_MASTER_ADDR", "")
    if not master:
        raise RuntimeError(
            "kvtest: master_type=http requires global.json master_addr")
    metadata = (cluster_session.get("SETUP_METADATA_SERVER", "")
                or "http://{}/metadata".format(master))
    return metadata, master


def _kvtest_key_pool(cfg, globals_cfg, plan, writer_count, ds_bytes, ds_label):
    """Resolve the C++ key pool semantics without overloading zero.

    "auto"/absent derives a bounded per-writer pool from store capacity;
    numeric 0 preserves the binary's YuanRong-compatible unbounded-new-key
    behavior; positive values are explicit bounded pools.
    """
    kv = cfg.get("kvtest", {}) or {}
    configured = kv.get("key_pool_size", "auto")
    batch = max(1, int(cfg.get("batch_size", 1) or 1))
    minimum = plan["writer_threads"] * batch
    if configured is None or str(configured).strip().lower() in ("", "auto"):
        per = _pipeline_key_budget(globals_cfg, writer_count, ds_bytes, batch,
                                   ds_label)
        pool = (per // minimum) * minimum
        if pool < minimum:
            raise RuntimeError(
                "kvtest key pool too small: store-derived budget {} keys "
                "cannot provide a multiple of num_threads x batch = {} "
                "(data size {})".format(per, minimum, ds_label))
        return pool
    raw = int(configured)
    if raw < 0:
        raise RuntimeError("kvtest key_pool_size must be auto or >= 0")
    if raw == 0:
        return 0
    if raw > 0:
        if raw < minimum or raw % minimum:
            raise RuntimeError(
                "kvtest key_pool_size {} must be >= num_threads x batch = {} "
                "and divisible by it (binary requirement)".format(
                    raw, minimum))
        return raw


def build_kvtest_instance_config(cfg, globals_cfg, cluster_session, plan,
                                 entry, role, ds_label, key_pool,
                                 all_instances, key_namespace=None):
    """Per-instance mooncake_kvtest JSON config. __FABRIC_IP__ and
    __UB_DEVICE__ are late-bound by run_kvtest.sh (per-pod values that only
    exist at runtime, after env.sh's urma_admin / POD_IP resolution).

    key_namespace: parallel-mode role segment ("rank"/"recall"). Both children
    of a rank+recall run share ONE store and restart instance ids at 0, so
    without a segment their key spaces would collide outright."""
    kv = cfg.get("kvtest", {}) or {}
    # num_threads is also the baseline subtracted from num_total_threads to
    # derive the notification-worker count.  Keep the same baseline in pure
    # reader configs (the reader role does not launch writer loops) so a 4+12
    # YuanRong-style budget remains num_threads=4, num_total_threads=16 in
    # both halves of a split pair.
    writer_threads = plan["writer_threads"]
    metadata_server, master_addr = _kvtest_store_targets(
        globals_cfg, cluster_session)
    writer_op = KVTEST_WRITER_OPS[_kvtest_writer_operation(cfg)]
    reader_op = KVTEST_READER_OPS[_kvtest_reader_operation(cfg)]
    rpc_port_offset = int(kv.get("notify_rpc_port_offset", 10000) or 10000)
    notify_rpc_port = int(entry["port"]) + rpc_port_offset
    if not 0 < notify_rpc_port <= 65535:
        raise RuntimeError(
            "kvtest notify RPC port {} is invalid (HTTP port {} + offset {})"
            "".format(notify_rpc_port, entry["port"], rpc_port_offset))
    return {
        "mode": "pipeline",
        "instance_id": entry["iid"],
        "listen_port": entry["port"],
        "notify_rpc_port": notify_rpc_port,
        "notify_rpc_threads": int(kv.get("notify_rpc_threads", 4) or 4),
        "role": role,
        "pipeline": [writer_op],
        "notify_pipeline": [reader_op],
        "target_qps": max(0, int(entry.get("_qps", 0) or 0)),
        "num_threads": writer_threads,
        "num_total_threads": writer_threads + plan["reader_threads"],
        "notify_threads": plan["notify_threads"],
        "notify_rpc_connections_per_peer": int(
            kv.get("notify_rpc_connections_per_peer", 4) or 4),
        "notify_rpc_reuse_limit": int(
            kv.get("notify_rpc_reuse_limit", 4)),
        "notify_rpc_timeout_ms": int(
            kv.get("notify_rpc_timeout_ms", 3000) or 3000),
        "notify_count": plan["notify_count"],
        "batch_keys_count": max(1, int(cfg.get("batch_size", 1) or 1)),
        "key_pool_size": key_pool,
        # Key space is orchestrator-owned (the binary reads these fields):
        # warmup MUST use its own prefix or its leases block the first
        # measurement upserts (OBJECT_HAS_LEASE — 16.8% write failures when a
        # build tree wrote warmup with the measurement prefix).
        "key_prefix": "mooncake_kvtest_" + (
            "{}_".format(key_namespace) if key_namespace else ""),
        "warmup_key_prefix": "mooncake_kvtest_warmup_" + (
            "{}_".format(key_namespace) if key_namespace else ""),
        "notify_queue_max": int(kv.get("notify_queue_max", 8192) or 8192),
        "enable_jitter": bool(kv.get("enable_jitter", True)),
        "start_paused": True,
        "data_sizes": [ds_label],
        "metrics_interval_ms": int(kv.get("metrics_interval_ms", 3000) or 3000),
        "output_dir": entry["_out_dir"],
        "cpu_affinity": entry.get("_cpu_affinity", ""),
        "verify_level": kv.get("verify_level", "size"),
        "verify_sample_bytes": kv.get("verify_sample_bytes", "4KB"),
        "verify_sample_step_bytes": kv.get("verify_sample_step_bytes", "1MB"),
        "verify_fail_op": bool(kv.get("verify_fail_op", False)),
        "nodes": [
            {"host": i["host"], "port": i["port"],
             "notify_rpc_port": int(i["port"]) + rpc_port_offset,
             "instance_id": i["iid"], "role": i["role"]}
            for i in all_instances
        ],
        "store": {
            "local_hostname": "__FABRIC_IP__:{}".format(
                entry.get("_lhost_port", 12345)),
            "metadata_server": metadata_server,
            "global_segment_size": 0,
            "local_buffer_size": int(parse_size_bytes(
                globals_cfg.get("local_buffer_size", 8589934592))),
            "protocol": cfg["protocol"],
            "rdma_devices": "__UB_DEVICE__",
            "master_server_address": master_addr,
            "tenant_id": globals_cfg.get("tenant_id", "default"),
            "replica_num": max(1, int(kv.get("replica_num", 1) or 1)),
        },
    }


def _kvtest_split_launch_order(cfg):
    """kvtest.reader_first_launch=true restores the legacy two-phase
    reader-then-writer stagger. Default False: with start_paused the C++
    writer stays inert until /start (and only connects/sends then, main.cpp
    /start -> writer->Start), so both roles can be prepared, fired and
    health-gated as ONE batch — the readers-first contract holds as long as
    the combined gate passes before the /start fan-out."""
    return bool((cfg.get("kvtest", {}) or {}).get("reader_first_launch"))


def _kvtest_launch_split_roles(plan, split, prepare, fire, gate):
    """Choreograph the split-role launch/gate sequence (unit tested).

    prepare(role, entries) pushes per-instance configs (role = reader |
    writer); fire(entries) launches the prepared paused processes;
    gate(entries, what) health-gates them. The two staggers differ only in
    grouping — the caller's closures keep the scenario's `stage`
    attribution and the dry-run skip in sync."""
    if split:
        prepare("reader", plan["readers"])
        fire(plan["readers"])
        gate(plan["readers"], "readers")
        prepare("writer", plan["writers"])
        fire(plan["writers"])
        gate(plan["writers"], "writers")
    else:
        prepare("reader", plan["readers"])
        prepare("writer", plan["writers"])
        fire(plan["readers"] + plan["writers"])
        gate(plan["readers"] + plan["writers"], "instances")


def _kvtest_health_gate(cluster, entries, timeout_s, what):
    """Poll GET /health on every kvtest instance until all respond. The
    binary starts its HTTP server only AFTER the store client setup, so
    'healthy' means store connected + notify endpoint bound. Raises with the
    missing list on timeout."""
    if not entries:
        return
    from concurrent.futures import ThreadPoolExecutor
    t0 = time.time()
    pending = list(entries)

    def _probe(entry):
        n = get_node(cluster, entry["node"])
        try:
            return _pipeline_http_call(n, entry["port"], "/health",
                                       timeout=3, method="GET")
        except Exception:
            return False

    log("[kvtest] launched {} {}, waiting health gate (timeout {}s)".format(
        len(pending), what, timeout_s))
    while pending:
        _check_stop()
        with ThreadPoolExecutor(max_workers=min(_PIPELINE_PARALLEL,
                                               len(pending))) as pool:
            results = list(pool.map(_probe, pending))
        pending = [e for e, ok in zip(pending, results) if not ok]
        if not pending:
            break
        if time.time() - t0 > timeout_s:
            raise RuntimeError(
                "kvtest health gate timed out after {}s: {}/{} {} not "
                "healthy: {}".format(
                    timeout_s, len(pending), len(entries), what,
                    ", ".join("i{} on {}".format(e["iid"], e["node"])
                              for e in pending[:8])))
        time.sleep(2)
    log("[kvtest] health gate: {}/{} {} healthy ({:.1f}s)".format(
        len(entries), len(entries), what, time.time() - t0))


def _kvtest_kill_tree(cluster, node_name, pid):
    """Tree-kill a kvtest instance: the ledger pid is the run_kvtest.sh bash
    wrapper, so pgrep -P widens the kill to the C++ binary child. The first
    SIGTERM is the binary's own graceful-stop path; SIG9 only follows when
    it refuses to die."""
    n = get_node(cluster, node_name)
    try:
        ssh_check_output(
            n["ip"], n,
            "kill $(pgrep -P {pid} 2>/dev/null) {pid} 2>/dev/null; sleep 1; "
            "kill -9 $(pgrep -P {pid} 2>/dev/null) {pid} 2>/dev/null || true"
            "".format(pid=pid), quiet=True)
    except Exception as exc:
        log("[WARN] fail-cleanup: kill pid {} on {} failed: {}".format(
            pid, node_name, exc))


def _wait_kvtest_processes(cluster, instance_pids, timeout_s, expected_s=None,
                           label_prefix="kvtest", progress_interval=None):
    """Wait for kvtest wrappers in parallel under one shared deadline.

    The old per-instance loop multiplied the timeout by the number of
    instances (two stuck processes cost 240+ seconds).  Each wrapper owns one
    binary child, so independent polling is safe and keeps the wall timeout
    bounded by ``timeout_s``.
    """
    if not instance_pids:
        return []
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _one(pair):
        entry, pid = pair
        if not pid:
            return True
        return wait_process_exit(
            cluster, entry["node"], pid,
            "{} i{}".format(label_prefix, entry["iid"]), timeout_s,
            expected_s=expected_s, marker=_KVTEST_PID_MARKER)

    workers = max(1, min(_PIPELINE_PARALLEL, len(instance_pids)))
    results = [False] * len(instance_pids)
    done_count = 0
    t0 = time.time()
    last = t0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futs = {pool.submit(_one, pair): idx
                for idx, pair in enumerate(instance_pids)}
        for fut in as_completed(futs):
            idx = futs[fut]
            try:
                results[idx] = bool(fut.result())
            except Exception:
                results[idx] = False
            done_count += 1
            now = time.time()
            if (progress_interval and now - last >= progress_interval
                    and done_count < len(results)):
                last = now
                log("[kvtest] 实例正在退出: {}/{} ({:.0f}s)".format(
                    done_count, len(results), now - t0))
    return results


def _controller_reduce_enabled(globals_cfg):
    """k8s.reduce_on_controller (default true): kvtest artifacts take ONE
    kubectl cp hop pod -> controller, map+reduce run on the controller, and
    only reduced.json/meta/summary.json come back to the orchestrator. Any
    failure falls back to the local pull+map+reduce path."""
    block = (globals_cfg or {}).get("k8s") or {}
    return bool(block.get("reduce_on_controller", True))


def _controller_keep_data(globals_cfg):
    return bool(((globals_cfg or {}).get("k8s") or {})
                .get("controller_keep_data"))


def _controller_cp_groups(cluster, groups, cdir, workers=None):
    """kubectl cp every group's pod files into the controller dir (the
    FIRST hop of scp_pull's k8s branch only — data stops ON the controller,
    the second SFTP hop to the orchestrator never happens)."""
    from concurrent.futures import ThreadPoolExecutor
    controller_mkdir(cluster["_controller"], cdir)

    def _one(group):
        node, files, _label = group
        c = node["_controller"]
        for f in files:
            # kubectl cp treats a bare directory dest as a FILE target
            # ("is a directory"); scp_pull's working remote branch passes the
            # explicit file path — mirror it here.
            dst = posixpath.join(cdir, os.path.basename(f))
            out, rc = controller_run(
                c, ["-n", node["_ns"], "cp",
                    "{}:{}".format(node["_pod"], f), dst], check=False)
            if rc != 0:
                raise RuntimeError(
                    "kubectl cp failed (rc={}) for {} on {}: {}".format(
                        rc, os.path.basename(f), node["_pod"],
                        (out or "")[:200]))
        return True

    _workers = max(1, min(workers or _PIPELINE_PARALLEL, len(groups) or 1))
    with ThreadPoolExecutor(max_workers=_workers) as pool:
        return list(pool.map(_one, groups))


def _kvtest_controller_collect(cluster, globals_cfg, cfg, args, ts,
                                cfg_name, ds, instance_pids, groups,
                                local_log_dir, extra_bytes,
                                qps_target=0, nodes=0,
                                remote_log_dir=None, is_k8s=True):
    """Controller-side kvtest data plane: cp artifacts pod->controller,
    run the staged map+reduce driver there, pull back only the compact
    reduced/meta/summary products. Raises on any failure so the caller can
    fall back to the local path (pods still hold everything)."""
    c = cluster["_controller"]
    ws = k8s_settings(cluster).get("controller_workspace") or "/tmp"
    cdir = posixpath.join(ws, "data", "{}__{}__{}".format(ts, cfg_name, ds))
    red_dir = os.path.join(local_log_dir, "report", ds)
    os.makedirs(red_dir, exist_ok=True)
    _controller_cp_groups(cluster, groups, cdir)
    scripts_dst = posixpath.join(cdir, "scripts")
    stage = [os.path.join(SCRIPTS_DIR, n) for n in
             ("logstats.py", "report.py", "kvtest_controller_job.py")]
    controller_put(c, stage, scripts_dst)
    out, rc = controller_exec(
        c, "python3 -c 'import json,csv,heapq,gzip,argparse'")
    if rc != 0:
        raise RuntimeError("controller python3 probe failed: {}".format(
            (out or "")[:200]))
    # which pod files actually exist (R1 gzip decides the samples/trace
    # transfer form) — one scan of the group manifests
    names = set()
    for _n, files, _l in groups:
        for f in files:
            names.add(os.path.basename(f))
    iid_nodes = cfg.get("_kvtest_iid_nodes") or {}
    iid_pods = cfg.get("_kvtest_iid_pods") or {}
    start_epochs = cfg.get("_kvtest_start_epochs") or {}
    instances = []
    for entry, _pid in instance_pids:
        base = entry["_base"]
        if (base + ".samples.csv.gz") in names:
            samples = base + ".samples.csv.gz"
        elif (base + ".samples.csv") in names:
            samples = base + ".samples.csv"
        else:
            raise RuntimeError(
                "kvtest i{}: no samples artifact for the controller job"
                "".format(entry["iid"]))
        trace = None
        for cand in (base + ".trace.json", base + ".trace.json.gz",
                     base + ".log.glog.trace.json",
                     base + ".log.glog.trace.json.gz"):
            if cand in names:
                trace = cand
                break
        pod = iid_pods.get(str(entry["iid"]), "")
        site_dir = (_pod_path_to_site(
            cluster, posixpath.join(remote_log_dir, pod))
            if (remote_log_dir and pod) else "")
        _warm = float((cfg or {}).get("_kvtest_warmup_duration") or 0)
        _start = start_epochs.get(str(ds))
        instances.append({
            "samples": samples,
            "summary": base + ".summary.json",
            "trace": trace,
            "site_dir": site_dir,
            "node": iid_nodes.get(str(entry["iid"]),
                                  "i{}".format(entry["iid"])),
            "pod": pod,
            "ds": ds,
            "start_epoch": _start,
            # warmup ends this many seconds after the /start anchor; rows
            # before it are chart-only (excluded from the formal statistics)
            "warmup_until": ((float(_start) + _warm)
                             if (_start is not None and _warm > 0) else None),
            "extra_bytes": int(extra_bytes.get(base) or 0),
            "writer_ops": sorted(KVTEST_WRITE_OPS),
            "reader_ops": sorted(KVTEST_READ_OPS),
        })
    job = {"dir": cdir,
           "out_dir": posixpath.join(cdir, "reduced"),
           "meta_base": _kvtest_report_meta_base(
               cfg, args, cfg_name, ds,
               qps_target=qps_target, nodes=nodes,
               context=_kvtest_report_context(
                   cluster, ts, remote_log_dir or "", is_k8s)),
           "instances": instances}
    job_local = os.path.join(local_log_dir, "controller_job.json")
    with open(job_local, "w", encoding="utf-8") as f:
        json.dump(job, f, ensure_ascii=False, indent=1)
    controller_put(c, [job_local], cdir)
    os.remove(job_local)
    job_remote = posixpath.join(cdir, "controller_job.json")
    out, rc = controller_exec(
        c, "python3 {} --job {}".format(
            shlex.quote(posixpath.join(scripts_dst,
                                        "kvtest_controller_job.py")),
            shlex.quote(job_remote)))
    log_detail("[kvtest] controller job: {}".format((out or "")[-800:]))
    if rc != 0:
        raise RuntimeError("controller map+reduce job failed (rc={}): {}"
                           "".format(rc, (out or "")[-200:]))
    for _n, files, _l in groups:
        for f in files:
            if f.endswith(".summary.json"):
                controller_get_file(
                    c, posixpath.join(cdir, os.path.basename(f)),
                    local_log_dir)
    for name in ("{}.reduced.json".format(ds), "{}.meta.json".format(ds)):
        controller_get_file(
            c, posixpath.join(cdir, "reduced", name), red_dir)
    # reduced.json is the durable re-renderable artifact of this unit
    os.replace(os.path.join(red_dir, "{}.reduced.json".format(ds)),
               os.path.join(red_dir, "reduced.json"))
    os.replace(os.path.join(red_dir, "{}.meta.json".format(ds)),
               os.path.join(red_dir, "meta.json"))
    if not _controller_keep_data(globals_cfg):
        controller_exec(c, "rm -rf {}".format(shlex.quote(cdir)))
    return cdir


def _kvtest_report_meta_base(cfg, args, cfg_name, ds,
                              qps_target=0, nodes=0, context=None):
    """The reduce meta the controller driver needs (everything except
    'started', which only the data itself knows).

    ``context`` carries the 现场索引 (run_id / host_log_root /
    remote_log_dir / k8s_namespace) so the report produced from the
    controller's reduced.json can point at the on-node log directories —
    the same four fields the orchestrator-local path merges from the
    sidecar."""
    meta = {
        "config": cfg_name, "mode": "pipeline", "scenario": "pipeline",
        "engine": "kvtest", "launch_mode": "single",
        "qps_target": qps_target, "protocol": cfg.get("protocol") or "",
        "cluster_cfg": os.path.basename(getattr(args, "cluster", "") or ""),
        "global_cfg": os.path.basename(getattr(args, "global_cfg", "") or ""),
        "profile_cfg": os.path.basename(getattr(args, "profile", "") or ""),
        "early_stop": False, "unit": ds, "unit_size": ds,
        # 前 warmup_s 秒并入窗口但不计入正式统计（图表仍绘制并标注边界）
        "warmup_s": float((cfg or {}).get("_kvtest_warmup_duration") or 0),
    }
    for _k, _v in (context or {}).items():
        if _v:
            meta[_k] = _v
    try:
        meta["params"] = _build_report_params(cfg, meta, nodes=nodes)
    except Exception as exc:
        log_detail("[kvtest] controller meta params failed: {}".format(exc))
    return meta


def _kvtest_flatten_artifacts(cluster, entry, remote_log_dir, attempts=1,
                              retry_s=0):
    """Copy one instance's nested artifacts into the pullable log root.

    Return ``(ok, status)``. ``summary.json`` feeds CSV aggregation and the
    request samples feed the Python-compatible point-line plot. The obsolete
    windowed timeseries is deliberately not copied.
    """
    n = get_node(cluster, entry["node"])
    base = entry["_base"]
    # 收编到该 pod 自己的日志目录（<pod_dir>/<base>.*），现场即一目录全量
    tgt = _kvtest_pod_base(remote_log_dir, base, n)
    pod_dir = _kvtest_pod_dir(remote_log_dir, n)
    command = (
        "mkdir -p '{d}'; summary=OK; samples=MISSING; "
        "cp '{o}/summary.json' '{t}.summary.json' 2>/dev/null || "
        "summary=MISSING; "
        "cp '{o}/latency_samples.csv' '{t}.samples.csv' 2>/dev/null "
        "&& samples=OK; "
        "printf 'summary=%s samples=%s' \"$summary\" \"$samples\""
    ).format(o=entry["_out_dir"], d=pod_dir, t=tgt)
    last = "not attempted"
    for attempt in range(max(1, int(attempts))):
        try:
            last = ssh_check_output(n["ip"], n, command, quiet=True).strip()
        except Exception as exc:
            last = "copy command failed: {}".format(exc)
        # samples=MISSING is accepted for compatibility with older binaries;
        # new binaries expose one-request-per-row latency_samples.csv.
        if last.startswith("summary=OK samples="):
            return True, last
        if attempt + 1 < attempts and retry_s:
            time.sleep(retry_s)
    return False, last


def _kvtest_gzip_remote_files(cluster, node_name, files):
    """gzip files in place on the pod (samples.csv / trace.json compress
    ~8-12x, cutting the pod->controller transfer). Returns the .gz paths on
    success; on ANY failure returns the original paths so the pull keeps
    working uncompressed (transparent fallback, never a hard error)."""
    if not files:
        return []
    n = get_node(cluster, node_name)
    script = "; ".join("gzip -f {}".format(shlex.quote(f)) for f in files)
    try:
        ssh_check_output(n["ip"], n, script, quiet=True)
        return [f + ".gz" for f in files]
    except Exception as exc:
        log_detail("[kvtest] in-pod gzip skipped ({}): {}".format(
            os.path.basename(files[0]), exc))
        return list(files)


def _gunzip_local_dir(local_dir):
    """Decompress every *.gz artifact pulled into local_dir back to the
    original file names, so the map/report glob surface stays unchanged
    (*.samples.csv / *.trace.json). Best-effort per file."""
    import gzip as _gzip
    import shutil as _shutil
    for path in sorted(glob.glob(os.path.join(local_dir, "*.gz"))):
        target = path[:-len(".gz")]
        try:
            with _gzip.open(path, "rb") as src, open(target, "wb") as dst:
                _shutil.copyfileobj(src, dst)
            os.remove(path)
        except OSError as exc:
            log("[WARN] gunzip {} failed: {}".format(
                os.path.basename(path), exc))


def _kvtest_remote_trace_map(cluster, entry, remote_log_dir, remote_scripts,
                             globals_cfg, best_effort=True, warmup_s=None):
    """pod 内 tracestats：glog -> <base>.trace.json（只回传统计，glog 不传输）。

    Glog 来源 = MC_LOG_DIR 的 <base>.glog.d/ 下全部 *.INFO.*（glog 按
    max_log_size 轮转，多段合并成一个产物）；旧镜像回退 stderr 捕获文件
    <base>.log.glog。--node 由 run_tracestats_map 内部经 _node_display 解析
    （k8s=宿主节点 IP），报表的节点归属是 IP 而不是 pod 名。返回可回传的
    trace 路径列表（失败为空）。"""
    n = get_node(cluster, entry["node"])
    pbase = _kvtest_pod_base(remote_log_dir, entry["_base"], n)
    role = entry.get("_role")
    if role in ("dual", "", None):
        role = None
    try:
        files, site = _resolve_glog_inputs(
            cluster, n, pbase + ".glog.d", pbase + ".log.glog")
        if not files:
            return []
        return run_tracestats_map(
            cluster, n, files, remote_scripts, globals_cfg,
            node_name=entry["node"], role=role, size=entry.get("_ds"),
            sample_rate=_hifreq_sample_rate(),
            remote_path=site,
            out_path=pbase + ".trace.json",
            warmup_s=warmup_s)
    except Exception as exc:
        if not best_effort:
            raise
        log_detail("[report] kvtest remote trace failed for i{}: {}".format(
            entry.get("iid"), exc))
        return []


def _kvtest_small_log_bytes(cluster, entry, remote_log_dir):
    """该实例 .log/.ylt.log/.nohup 的远端字节数（glog 走 trace meta.bytes）。

    只 stat 不传内容；失败按 0 处理（这几个文件通常为空/极小）。"""
    n = get_node(cluster, entry["node"])
    pbase = _kvtest_pod_base(remote_log_dir, entry["_base"], n)
    cmd = ("stat -c %s '{a}.log' '{a}.ylt.log' '{a}.nohup' 2>/dev/null "
           "| awk '{{s+=$1}} END{{print s+0}}'").format(a=pbase)
    try:
        out = ssh_check_output(n["ip"], n, cmd, quiet=True)
        return int((out or "").strip() or 0)
    except Exception:
        return 0


def _kvtest_trace_bytes(local_log_dir, base):
    """本地 trace 产物里的 glog 真实字节数（trace meta.bytes）。

    New products are <base>.trace.json (glog lives in MC_LOG_DIR segments);
    pre-rename runs carry <base>.log.glog.trace.json — both are accepted."""
    for name in (base + ".trace.json", base + ".log.glog.trace.json"):
        try:
            with open(os.path.join(local_log_dir, name), "r",
                      encoding="utf-8") as f:
                return int((json.load(f).get("meta") or {}).get("bytes") or 0)
        except (OSError, ValueError):
            continue
    return 0


KEEP_INTERMEDIATE_DIRS = ("report", "profiling")


def _verify_config_reports(log_dir):
    """True only when every expected unit rendered a self-contained
    report.html + data.js (and no unit is missing). Report generation is
    WARN-only, so a failed write must be detected explicitly before any
    intermediate artifact is deleted. Returns (ok, detail)."""
    stats = _collect_local_stats(log_dir)
    # controller 数据面：map/reduce 在 controller 完成，本地没有 stats.json，
    # 单元由 report/<size>/reduced.json 定义（渲染产物同目录校验）。
    reds = sorted(glob.glob(os.path.join(log_dir, "report", "*",
                                         "reduced.json")))
    if not stats and not reds:
        return False, "no local stats"
    meta = {}
    try:
        with open(os.path.join(log_dir, "report", "meta.json"),
                  "r", encoding="utf-8") as f:
            meta = json.load(f) or {}
    except (OSError, ValueError):
        meta = {}
    engine = meta.get("engine") or (
        "kvtest" if meta.get("mode") == "kvtest" else "python")
    scenario = _report_scenario(
        {"report_scenario": meta.get("scenario"),
         "scenario": meta.get("mode")}, None, engine)
    if stats:
        units = _unit_groups(stats, scenario)
    else:
        units = {("size", os.path.basename(os.path.dirname(r))):
                 {"name": os.path.basename(os.path.dirname(r))}
                 for r in reds}
    if not units:
        return False, "no report unit"
    rep = os.path.join(log_dir, "report")
    if not os.path.isdir(rep):
        return False, "no report dir"
    for u in units.values():
        d = os.path.join(rep, u["name"])
        html = os.path.join(d, "report.html")
        data_js = os.path.join(d, "data.js")
        for path in (html, data_js):
            try:
                if os.path.getsize(path) <= 0:
                    return False, "empty " + os.path.basename(path)
            except OSError:
                return False, "missing " + os.path.basename(path)
        try:
            with open(html, "rb") as f:
                f.seek(max(0, os.path.getsize(html) - 2048))
                tail = f.read()
        except OSError:
            return False, "unreadable report.html"
        if b"</html>" not in tail:
            return False, "truncated report.html"
    dirs = [x for x in os.listdir(rep)
            if os.path.isdir(os.path.join(rep, x))]
    if len(dirs) != len(units):
        return False, "unit count mismatch ({} != {})".format(
            len(dirs), len(units))
    return True, "{} unit(s)".format(len(units))


def _cleanup_config_intermediates(log_dir):
    """Remove every intermediate artifact under logs/<cfg>/ once its reports
    are verified. report/ (the final artifact) and profiling/ (monitor CSVs,
    deliberately retained) always stay; run-level CSVs live outside this
    directory and are never touched."""
    removed = 0
    for entry in sorted(os.listdir(log_dir)):
        if entry in KEEP_INTERMEDIATE_DIRS:
            continue
        path = os.path.join(log_dir, entry)
        try:
            if os.path.isdir(path):
                removed += sum(len(files) for _r, _d, files in os.walk(path))
                shutil.rmtree(path)
            else:
                os.remove(path)
                removed += 1
        except OSError as exc:
            log("[WARN] cannot remove intermediate {}: {}".format(path, exc))
    return removed


def _cleanup_run_intermediates(ts_dir, failed_scenarios, args):
    """Delete per-config intermediates (python + kvtest, basic + pipeline)
    after a fully successful run, once each config's report tree verifies.

    Whole-run escape hatches: --keep-intermediates / MC_KEEP_INTERMEDIATES=1,
    --pull-raw-logs / --png (the user explicitly asked for raw logs), and
    --no-report (nothing to verify). Failed configs keep everything; STOP /
    Ctrl+C bypass this run-end block entirely (StopRequested unwinds main)."""
    if (getattr(args, "keep_intermediates", False)
            or os.environ.get("MC_KEEP_INTERMEDIATES", "0") == "1"):
        log("intermediates kept (--keep-intermediates / "
            "MC_KEEP_INTERMEDIATES=1)")
        return 0
    if getattr(args, "pull_raw_logs", False) or getattr(args, "png", False):
        log("intermediates kept (--pull-raw-logs / --png)")
        return 0
    if getattr(args, "no_report", False):
        log("intermediates kept (--no-report: no report to verify)")
        return 0
    failed = set(failed_scenarios or [])
    logs = os.path.join(ts_dir, "logs")
    if not os.path.isdir(logs):
        return 0
    total = 0
    for name in sorted(os.listdir(logs)):
        d = os.path.join(logs, name)
        if not os.path.isdir(d):
            continue
        if name in failed:
            log("[report] intermediates kept for failed config {}".format(name))
            continue
        ok, detail = _verify_config_reports(d)
        if not ok:
            log("[WARN] intermediates kept for {}: {}".format(name, detail))
            continue
        n = _cleanup_config_intermediates(d)
        total += n
        log("[report] {} verified ({}); removed {} intermediate file(s)"
            .format(name, detail, n))
    if total:
        log("[report] intermediate artifacts removed: {} file(s)".format(total))
    return total


def fail_cleanup_kvtest(cluster, instance_pids, involved_nodes, remote_log_dir,
                        cfg_name, local_log_dir, remove_host=None,
                        remove_node=None, remove_runner=None, globals_cfg=None,
                        remote_scripts=None, ts=None, is_k8s=False,
                        failed_stage=""):
    """Best-effort cleanup after a kvtest scenario failure.

    No log/artifact extraction: raw logs stay on the nodes and the scenario's
    _scenario_failure_report has already printed their directories. Only the
    graceful /stop, a short exit wait, tree-kills and — unless the failing
    stage was itself a remove-keys — the store remove-all are performed."""
    # 1. graceful /stop so summary.json is flushed where still possible
    items = []
    for entry, pid in instance_pids:
        if "node" not in entry:
            continue  # deployment never materialized the pod entries
        items.append((get_node(cluster, entry["node"]), entry["port"],
                      (pid or "").strip(),
                      "fail-cleanup kvtest i{}".format(entry["iid"])))
    for _what, _ok, _err in _pipeline_http_parallel(cluster, items, "/stop",
                                                    timeout=5):
        if not _ok:
            log("[WARN] fail-cleanup: {} /stop failed: {}".format(_what, _err))
    # 2. short, cluster-wide wait so graceful exits finish and final logs
    # flush without multiplying the timeout by the number of instances.
    try:
        exit_results = _wait_kvtest_processes(
            cluster, instance_pids, 30,
            label_prefix="fail-cleanup kvtest")
    except Exception as exc:
        log("[WARN] fail-cleanup: exit wait failed: {}".format(exc))
        exit_results = [False for _ in instance_pids]
    # 3. tree-kill any still-alive instance
    for (entry, pid), exited in zip(instance_pids, exit_results):
        if pid and not exited:
            _kvtest_kill_tree(cluster, entry["node"], pid)
    # 4. store remove-all — skipped when the failing stage was a remove-keys:
    #    the retry would fail identically (another full RPC timeout) and the
    #    operator already has the raw log directory for the first failure.
    if remove_host and remove_node and remove_runner:
        if str(failed_stage).startswith("remove-keys"):
            log("[FAIL] store remove-all skipped (failing stage was {})"
                .format(failed_stage))
        else:
            try:
                ran = _remove_keys_stage(
                    cluster, globals_cfg, remove_host,
                    get_node(cluster, remove_node), remove_runner,
                    remote_log_dir, cfg_name, cfg_name, "full", "fail cleanup",
                    dry=False, node_name=remove_node,
                    label="{} (fail-cleanup remove-all)".format(cfg_name))
                if ran:
                    log("[FAIL] fail-cleanup: store remove-all issued")
                else:
                    log("[FAIL] fail-cleanup: store already clean — "
                        "remove-all skipped")
            except Exception as exc:
                log("[WARN] fail-cleanup: store remove-all failed: {}".format(exc))


# kvtest map/reduce: the C++ binary emits request-level samples, so the same
# map (stats.json) -> reduce (report.py) chain the Python client uses can run
# on them. A dual instance emits BOTH roles, hence one CSV -> up to two
# per-role products (a dual kvtest process is the C++ analogue of the Python
# client's dual role with its writer/reader logs).
KVTEST_WRITE_OPS = frozenset(KVTEST_WRITER_OPS.values())
KVTEST_READ_OPS = frozenset(KVTEST_READER_OPS.values())
_KVTEST_SAMPLES_RE = re.compile(
    r"^.*?\.i(?P<iid>\d+)_p(?P<port>\d+)\.kvtest_(?P<ds>.+?)"
    r"\.samples\.csv$")


KVTEST_REPORT_META = "kvtest_report_meta.json"


def _kvtest_write_report_meta(local_log_dir, cfg, context=None):
    """Persist the run context the report and --rebuild-report need.

    Artifact filenames are deliberately compact (no node/pod names) and the
    /start epoch only lives in the orchestrator's memory, so a failed run
    would otherwise lose node attribution and the sample timeline.
    ``context`` carries the 现场索引 data（run_id / remote_log_dir /
    host_log_root / k8s_namespace）so the report can point at the on-node
    log directories without pulling them."""
    payload = {
        "format": "kvtest_report_meta",
        "nodes": cfg.get("_kvtest_nodes"),
        "iid_nodes": cfg.get("_kvtest_iid_nodes") or {},
        "iid_pods": cfg.get("_kvtest_iid_pods") or {},
        "start_epochs": cfg.get("_kvtest_start_epochs") or {},
        "writer_operation": _kvtest_writer_operation(cfg),
        "reader_operation": _kvtest_reader_operation(cfg),
    }
    if context:
        payload.update({k: v for k, v in context.items() if v})
    path = os.path.join(local_log_dir, KVTEST_REPORT_META)
    try:
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=1)
        os.replace(tmp, path)
    except OSError as exc:
        log_detail("[report] kvtest report meta write failed: {}".format(exc))


def _kvtest_read_report_meta(local_log_dir):
    """The sidecar written by _kvtest_write_report_meta ({} when absent)."""
    try:
        with open(os.path.join(local_log_dir, KVTEST_REPORT_META),
                  "r", encoding="utf-8") as f:
            doc = json.load(f) or {}
        return doc if isinstance(doc, dict) else {}
    except (OSError, ValueError):
        return {}


def _kvtest_summary_doc(path):
    """Parse one instance summary.json (None when missing/corrupt)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            doc = json.load(f)
        return doc if isinstance(doc, dict) else None
    except (OSError, ValueError):
        return None


def _kvtest_apply_summary_metrics(stats, summary_doc):
    """Surface C++ aggregates the request samples cannot carry.

    The binary reports reader value-verification failures and the notify
    queue wait only in summary.json; without this the HTML report cannot show
    the two correctness/backlog signals at all (the CSV already has them)."""
    if not summary_doc:
        return
    try:
        verify = int(summary_doc.get("verify_failures") or 0)
    except (TypeError, ValueError):
        verify = 0
    if verify:
        stats["global"]["verify_failures"] = verify
    qw = (summary_doc.get("operations") or {}).get("notify_queue_wait") or {}
    try:
        avg = float(qw.get("avg_ms") or 0.0)
        mx = float(qw.get("max_ms") or 0.0)
        n = int(qw.get("total") or qw.get("count") or 0)
    except (TypeError, ValueError):
        return
    if n > 0 and (avg > 0 or mx > 0):
        stats["global"]["qw_avg_ms"] = round(avg, 6)
        stats["global"]["qw_max_ms"] = round(mx, 6)
        stats["global"]["qw_n"] = n


def _kvtest_instance_log_bytes(local_log_dir, base):
    """Raw per-instance log volume (log + glog + nohup + exitrc).

    The map product's meta.bytes is what the report shows as the node's log
    volume; for kvtest the map input is samples.csv, which is far smaller than
    the actual client logs. Prefer the real log files when they were pulled.
    """
    total = 0
    for suffix in (".log", ".log.ylt.log", ".nohup", ".log.exitrc"):
        try:
            total += os.path.getsize(os.path.join(local_log_dir, base + suffix))
        except OSError:
            pass
    return total


def _kvtest_start_epoch(cfg, ds, samples_path, summary_path):
    """Epoch origin of one instance's samples (the /start trigger).

    A live run records it per data_size; ``--rebuild-report`` and failed-pull
    artifacts fall back to (summary mtime - elapsed_seconds), which is the
    /stop write time minus the C++ measurement window."""
    starts = cfg.get("_kvtest_start_epochs") or {}
    try:
        return float(starts[str(ds)])
    except (KeyError, TypeError, ValueError):
        pass
    elapsed = 0.0
    try:
        with open(summary_path, "r", encoding="utf-8") as f:
            elapsed = float((json.load(f) or {}).get("elapsed_seconds") or 0)
    except (OSError, ValueError, TypeError):
        elapsed = 0.0
    try:
        return max(0.0, os.path.getmtime(samples_path) - elapsed)
    except OSError:
        return 0.0


def _kvtest_map_stats(local_log_dir, cfg):
    """Map stage for kvtest: samples.csv -> per-role <...>.stats.json.

    Runs on the orchestrator (the samples are already pulled for the CSV and
    the request-level plot), writes the exact logstats product schema so the
    unchanged reduce stage renders the standard HTML report.
    """
    ls = _logstats_module()
    side = _kvtest_read_report_meta(local_log_dir)
    if not cfg.get("_kvtest_start_epochs") and side.get("start_epochs"):
        cfg = dict(cfg, _kvtest_start_epochs=side["start_epochs"])
    if not cfg.get("_kvtest_iid_nodes") and side.get("iid_nodes"):
        cfg = dict(cfg, _kvtest_iid_nodes=side["iid_nodes"])
    if not cfg.get("_kvtest_iid_pods") and side.get("iid_pods"):
        cfg = dict(cfg, _kvtest_iid_pods=side["iid_pods"])
    iid_nodes = cfg.get("_kvtest_iid_nodes") or {}
    iid_pods = cfg.get("_kvtest_iid_pods") or {}
    remote_log_root = side.get("remote_log_dir") or ""
    extra_bytes = cfg.get("_kvtest_extra_bytes") or {}
    _warm = float(side.get("warmup_s")
                  if side.get("warmup_s") is not None
                  else (cfg.get("_kvtest_warmup_duration")
                        if cfg.get("_kvtest_warmup_duration") is not None
                        else (cfg.get("warmup_s") or 0)))
    written = []
    for path in sorted(glob.glob(os.path.join(
            local_log_dir, "*.kvtest_*.samples.csv"))):
        match = _KVTEST_SAMPLES_RE.match(os.path.basename(path))
        if not match:
            continue
        iid = int(match.group("iid"))
        ds = match.group("ds")
        node = iid_nodes.get(str(iid), "i{}".format(iid))
        pod = iid_pods.get(str(iid), "")
        base = os.path.basename(path)[:-len(".samples.csv")]
        # 远程路径指向节点现场（ssh 后 cd 可用的真实文件）：优先 sidecar 的
        # 节点 per-pod 目录（site_dirs），文件名按 gzip 后的实际名
        # （remote_names），旧 sidecar 回退 pod 内路径。
        _name = os.path.basename(path)
        site_dir = (side.get("site_dirs") or {}).get(str(iid)) or ""
        if site_dir:
            remote_path = "{}/{}".format(
                site_dir,
                (side.get("remote_names") or {}).get(_name) or _name)
        elif remote_log_root:
            remote_path = ("{}/{}/{}".format(remote_log_root, pod, _name)
                           if pod else
                           "{}/{}".format(remote_log_root, _name))
        else:
            remote_path = path
        summary_path = path[:-len(".samples.csv")] + ".summary.json"
        start = _kvtest_start_epoch(cfg, ds, path, summary_path)
        # 前 warmup 秒并入窗口但不计入正式统计（图表仍绘制）
        _cutoff = ((float(start) + _warm)
                   if (_warm > 0 and start) else None)
        for role, ops in (("writer", KVTEST_WRITE_OPS),
                          ("reader", KVTEST_READ_OPS)):
            summaries = []
            payload = ls.kvtest_summary_payload(summary_path, ops)
            if payload is not None:
                summaries.append(("summary", payload))
            stats = ls.compute_stats(
                path, top_n=100, context=20, node=node, pod=pod, role=role,
                remote_path=remote_path, size=ds, warmup_cutoff=_cutoff,
                rows_factory=lambda p, _ops=ops, _start=start:
                    ls.iter_kvtest_rows(p, _start, _ops),
                summaries=summaries,
                source_name="{}.{}.samples.csv".format(base, role),
                extra_meta={"format": "kvtest_samples",
                            "scenario": "kvtest"})
            if not stats["meta"]["rows"]:
                continue
            if role == "reader":
                # verify_failures / notify queue wait exist only in summary.json
                _kvtest_apply_summary_metrics(
                    stats, _kvtest_summary_doc(summary_path))
            raw_bytes = _kvtest_instance_log_bytes(local_log_dir, base)
            stats["meta"]["samples_bytes"] = stats["meta"]["bytes"]
            if raw_bytes:
                # 旧产物 / --rebuild-report 现场有原始日志时以实测为准
                stats["meta"]["bytes"] = raw_bytes
            else:
                # stats-only：glog 真实字节取自 trace meta.bytes（原始 glog
                # 未拉回），再补 .log/.ylt/.nohup 的远端 stat 字节
                tbytes = _kvtest_trace_bytes(local_log_dir, base)
                extra = int(extra_bytes.get(base) or 0)
                if tbytes or extra:
                    stats["meta"]["bytes"] = tbytes + extra
            out = "{}.{}.stats.json".format(path, role)
            tmp = out + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(stats, f, ensure_ascii=False,
                          separators=(",", ":"))
            os.replace(tmp, out)
            written.append(out)
    return written


def _kvtest_generate_reports(cfg, local_log_dir, args, early_stop=False,
                             scenario=None):
    """Inject the map-reduce report chain into a kvtest scenario.

    map:   request samples -> per-role stats.json (see _kvtest_map_stats)
    reduce: stats.json -> one HTML report per data_size (report.py), plus the
            run overview accumulated by the caller.
    WARN-only: a report failure must never fail the benchmark."""
    if getattr(args, "no_report", False) or getattr(args, "dry_run", False):
        return None
    # controller 数据面：map+reduce 已在 controller 完成，本地只剩渲染
    try:
        _reds = sorted(glob.glob(os.path.join(
            local_log_dir, "report", "*", "reduced.json")))
        if _reds:
            rp = _report_module()
            echarts = os.path.join(ROOT, "webui", "web",
                                   "vendor", "echarts.min.js")
            written = []
            for red in _reds:
                with open(red, "r", encoding="utf-8") as f:
                    data = json.load(f)
                out_dir = os.path.dirname(red)
                html, _datajs = rp.render_scenario(
                    data, out_dir, echarts,
                    rel_base=os.path.relpath(out_dir, ROOT)
                    .replace(os.sep, "/"))
                _REPORT_ACC.append({
                    "data": data, "report_html": html,
                    "config": os.path.splitext(os.path.basename(
                        cfg.get("__cfg_path__") or "scenario"))[0],
                    "unit": os.path.basename(out_dir)})
                written.append(html)
            if written:
                log("[report] kvtest controller render: {} unit(s)".format(
                    len(written)))
                return written
    except Exception as exc:
        log("[WARN] kvtest controller render failed: {}".format(exc))
    try:
        mapped = _kvtest_map_stats(local_log_dir, cfg)
        if mapped:
            log("[report] kvtest map: {} per-role stats product(s)".format(
                len(mapped)))
    except Exception as exc:
        log("[WARN] kvtest map failed: {}".format(exc))
    qps_target = 0
    try:
        nodes = max(1, int(cfg.get("_kvtest_nodes") or 1))
        qps_target = kvtest_plan(cfg, node_count=nodes)["total_qps"]
    except Exception:
        pass
    return _generate_reports(
        cfg, local_log_dir, args, early_stop=early_stop,
        qps_target=qps_target, launch_mode="single", mode="kvtest",
        nodes=int(cfg.get("_kvtest_nodes") or 0), engine="kvtest",
        scenario=scenario)


def _kvtest_stop_collect(cluster, cfg, cfg_name, remote_log_dir,
                         local_log_dir, result_dir, instance_pids, items,
                         remote_scripts=None, globals_cfg=None, ts=None,
                         is_k8s=False):
    """Graceful artifact collection for a STOPped kvtest run.

    The C++ binary writes latency_samples.csv only while handling /stop, so a
    partial report needs this handshake first. Every step is best-effort: a
    STOP must still exit when the cluster is unresponsive. The collected
    evidence is turned into a report later by _stop_reports (skip_pull), which
    is the only place left once StopRequested unwinds the scenario."""
    clear_stop()          # the collection itself must be allowed to run
    log("[STOP] kvtest: sending /stop so the C++ instances flush samples ...")
    try:
        for _what, ok, err in _pipeline_http_parallel(
                cluster, items, "/stop", timeout=10):
            if not ok:
                log_detail("[STOP] {} /stop: {}".format(_what, err))
    except Exception as exc:
        log("[WARN] [STOP] kvtest /stop failed: {}".format(exc))
    try:
        _wait_kvtest_processes(cluster, instance_pids, 20,
                               label_prefix="kvtest STOP")
    except Exception as exc:
        log("[WARN] [STOP] kvtest wait failed: {}".format(exc))
    groups = []
    for entry, _pid in instance_pids:
        base = entry["_base"]
        try:
            ok, status = _kvtest_flatten_artifacts(
                cluster, entry, remote_log_dir, attempts=2, retry_s=1)
        except Exception as exc:
            ok, status = False, str(exc)
        if not ok:
            log_detail("[STOP] kvtest i{} artifacts: {}".format(
                entry["iid"], status))
        try:
            node_entry = get_node(cluster, entry["node"])
        except Exception:
            node_entry = entry
        pbase = _kvtest_pod_base(remote_log_dir, base, node_entry)
        # STOP 与成功路径同策略：只拉 compact（summary/samples/trace），
        # 原始 log/glog/ylt 留在节点现场（ssh <节点IP> 查看）。
        files = [pbase + ".summary.json"]
        if ok:
            files.append(pbase + ".samples.csv")
        if remote_scripts and globals_cfg is not None:
            files.extend(_kvtest_remote_trace_map(
                cluster, entry, remote_log_dir, remote_scripts,
                globals_cfg, best_effort=True,
                warmup_s=cfg.get("_kvtest_warmup_duration")))
        groups.append((node_entry, files,
                       "kvtest i{}".format(entry["iid"])))
    try:
        _pipeline_pull_parallel(cluster, groups, local_log_dir, False)
    except Exception as exc:
        log("[WARN] [STOP] kvtest artifact pull failed: {}".format(exc))
    _stop_ctx = (_kvtest_report_context(cluster, ts, remote_log_dir, is_k8s)
                 if ts else {})
    _stop_ctx["warmup_s"] = float(cfg.get("_kvtest_warmup_duration") or 0)
    _kvtest_write_report_meta(local_log_dir, cfg, _stop_ctx)
    _STOP_CTX["report"] = {
        "cfg": cfg, "cfg_name": cfg_name, "result_dir": result_dir,
        "local_log_dir": local_log_dir, "remote_log_dir": remote_log_dir,
        "involved_nodes": [e["node"] for e, _p in instance_pids],
        "mode": "kvtest", "skip_pull": True,
    }
    log("[STOP] kvtest partial artifacts collected into {}".format(
        os.path.relpath(local_log_dir, ROOT)))


def _kvtest_monitor_matrix(matrix, kv_binary):
    """kvtest runs mooncake_kvtest, not the python benchmark client: when the
    profile keeps the monitor default, retarget process-memory sampling at the
    kvtest binary (an explicit proc_pattern always wins).

    Returns a COPY: args._matrix is shared by every config in a multi-config
    run, so a kvtest retarget must never leak into a later basic/pipeline
    config."""
    if not matrix:
        return matrix
    out = copy.deepcopy(matrix)
    mem_cfg = (out.get("tools") or {}).get("mem")
    if mem_cfg is not None and str(mem_cfg.get("proc_pattern") or "") \
            in ("", "mooncake_benchmark_client"):
        mem_cfg["proc_pattern"] = os.path.basename(str(kv_binary))
    return out


def run_kvtest_scenario(cluster, globals_cfg, cfg, ts, args):
    """kvtest orchestration: deploy the C++ mooncake_kvtest instances with
    either a dual process per client pod, a co-located split writer/reader
    pair per client pod, or separate writer/reader pods,
    then run one full measurement per data_size:

      1. per-instance JSON config pushed via base64 (differs per instance:
         instance_id / listen_port / role / per-node QPS split / nodes view)
      2. split/separate readers first, health-gated, then writers (kvtest
         README rule); dual clients launch together (one process hosts both)
      3. after every /health gate passes, POST /start resets metrics and
         releases the paused C++ writer threads; optionally run warmup,
         pause+drain both notification sides, reset, then resume measurement
      4. run for `duration`, then POST /stop; the C++ shutdown path writes
         the final summary
      5. flatten artifacts, pull logs, verify exit codes

    kvtest needs no etcd rendezvous and no value.bin (the binary fills its
    own registered buffers)."""
    apply_cli_overrides(cfg, args)
    scenario = cfg["scenario"]
    cfg_name = os.path.splitext(os.path.basename(cfg.get("__cfg_path__", scenario)))[0]
    dry = args.dry_run
    work_mode = cfg.get("work_mode", cfg.get("mode", ""))
    is_k8s = cluster.get("_controller") is not None
    matrix = getattr(args, "_matrix", None)
    profile_sess = None  # profiling session (node monitors; started below)
    # ---- cluster-oriented node set (same as pipeline) ----
    if is_k8s:
        k8s_nodes = _k8s_pipeline_nodes(cluster, dry)
        n_nodes = len(k8s_nodes)
        plan = kvtest_plan(cfg, node_count=n_nodes)
    else:
        node_names = sorted(n for n in (cluster.get("nodes") or {})
                            if not str(n).startswith("_"))
        if not node_names:
            raise RuntimeError(
                "kvtest requires a non-empty cluster node set (N): put worker "
                "nodes in cluster.json (ssh) or use a k8s cluster with Ready "
                "nodes")
        n_nodes = len(node_names)
        plan = kvtest_plan(cfg, node_count=n_nodes)
        pool = plan["clients"] if plan.get("dual") else \
            plan["writers"] + plan["readers"]
        for entry in pool:
            entry["node"] = node_names[entry["slot"]]
    # record the REAL cluster-wide process counts for the CSV
    cfg["_writer_count"] = len(plan["writers"])
    cfg["_reader_count"] = len(plan["readers"])
    # ---- validation (fail before any cluster resource is touched) ----
    if plan["notify_count"] < 1:
        raise RuntimeError("kvtest config requires notify_count >= 1")
    if plan.get("dual"):
        # a dual instance never notifies itself -> sampling pool is C-1
        if plan["notify_count"] > len(plan["clients"]) - 1:
            raise RuntimeError(
                "notify_count={} exceeds the dual-role sampling pool {} "
                "(client.count_per_node x N - 1; own reader excluded)".format(
                    plan["notify_count"], len(plan["clients"]) - 1))
    elif plan["notify_count"] > len(plan["readers"]):
        raise RuntimeError("notify_count={} exceeds reader count {} "
                           "(reader.count_per_node x N)".format(
                               plan["notify_count"], len(plan["readers"])))
    cfg_duration = cfg.get("duration", None)
    if not cfg_duration or float(cfg_duration) <= 0:
        raise RuntimeError("kvtest config requires duration > 0 "
                           "(missing or invalid)")
    warmup_duration = _kvtest_warmup_duration(cfg, args)
    cfg["_kvtest_warmup_duration"] = warmup_duration
    writer_op = _kvtest_writer_operation(cfg)
    reader_op = _kvtest_reader_operation(cfg)
    if writer_op not in KVTEST_WRITER_OPS:
        raise RuntimeError("kvtest writer operation must be one of {} (got {})"
                           "".format(sorted(KVTEST_WRITER_OPS), writer_op))
    if reader_op not in KVTEST_READER_OPS:
        raise RuntimeError("kvtest reader operation must be one of {} (got {})"
                           "".format(sorted(KVTEST_READER_OPS), reader_op))
    kv_binary = (cfg.get("kvtest", {}) or {}).get("binary", "mooncake_kvtest")
    matrix = _kvtest_monitor_matrix(matrix, kv_binary)
    # health gate budget: covers the C++ store client setup (ub can take
    # tens of seconds); same knob as the pipeline rendezvous timeout
    health_timeout_s = int(globals_cfg.get("rendezvous_timeout_s", 120) or 120)
    if is_k8s:
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
        store_node_ips = resolve_store_node_ips(
            cluster, globals_cfg.get("mooncake_store_nodes", []) or [])
        topology = "local_node" if node["ip"] in store_node_ips else "remote_node"
    ensure_pods(cluster, involved_nodes, ts, cfg_name, dry)
    local_result_dir = getattr(args, "_result_dir_path", None) or \
        os.path.join(RESULTS_DIR, ts)
    remote_dir, remote_scripts, remote_log_dir, remote_runner_basic, \
        remote_runner_pipeline, local_log_dir = \
        scenario_paths(globals_cfg, ts, cfg_name, local_result_dir)
    remote_runner_kvtest = remote_scripts + "/run_kvtest.sh"
    if not dry:
        os.makedirs(local_log_dir, exist_ok=True)
    # NUMA/binding resolution is parameter info (k8s always disables it);
    # resolved before the scenario header so it can be printed there.
    numa_bind = cfg.get("numa_bind")
    if numa_bind is None:
        numa_bind = globals_cfg.get("numa_bind")
    numa_bind = True if numa_bind is None else bool(numa_bind)
    cpu_shard = int(cfg.get("cpu_shard", 0) or 0)
    if cpu_shard > 0 and not numa_bind:
        numa_bind = True  # sharding needs the NUMA topology probes
    if is_k8s:
        numa_bind = False
        cpu_shard = 0
    log_phase("场景 {} (engine=kvtest, scenario={})".format(
        cfg_name, cfg.get("report_scenario") or "pipeline"))
    # ---- key pool per sweep size, resolved BEFORE any cluster resource is
    # touched (a missing store source or a too-small store fails in seconds
    # with zero leftovers) ----
    sizes = data_size_list(cfg)
    _a, _b, _eff, _src = _store_capacity_resolution(globals_cfg)
    pools = {}
    for _ds in sizes:
        pools[_ds] = _kvtest_key_pool(
            cfg, globals_cfg, plan, len(plan["writers"]),
            parse_data_size(_ds), _ds)
    cfg["_kvtest_key_pool"] = {str(_ds): _p for _ds, _p in pools.items()}
    log("[kvtest] params: N={} writers={} readers={} notify_count={} "
        "total_qps={} per_node_qps={} binary={} warmup={}s duration={}s "
        "health_timeout={}s "
        "sizes={} NUMA/bind={}".format(
            n_nodes, len(plan["writers"]), len(plan["readers"]),
            plan["notify_count"], int(plan["total_qps"]),
            plan["per_node_qps"], kv_binary, warmup_duration,
            float(cfg.get("duration", 30)), health_timeout_s,
            ",".join(sizes),
            ("off (container)" if is_k8s
             else ("on" if numa_bind else "off"))))
    for _ds in sizes:
        if pools[_ds] == 0:
            log("key pool: {} unbounded per writer (key_pool_size=0)".format(_ds))
        else:
            log("key pool: {} {} keys/writer × {} writers = {} "
                "(capacity source={})".format(
                _ds, pools[_ds], len(plan["writers"]),
                _fmt_gib(len(plan["writers"]) * pools[_ds] *
                         parse_data_size(_ds)), _src))
    scenario_params = base_scenario_params(cfg, work_mode, topology,
                                           primary_node_name)
    scenario_params.update({
        "BENCH_WRITER_COUNT": str(len(plan["writers"])),
        "BENCH_READER_COUNT": str(len(plan["readers"])),
        "BENCH_NOTIFY_COUNT": str(plan["notify_count"]),
        "BENCH_NOTIFY_THREADS": str(plan["notify_threads"]),
        "BENCH_DURATION": str(cfg.get("duration", 30)),
        "BENCH_BATCH_SIZE": str(cfg.get("batch_size", 1)),
        "BENCH_OPERATION": "",
    })
    cluster_session = build_cluster_session(cluster, globals_cfg, remote_dir)
    instance_pids = []
    duration = float(cfg.get("duration", 30))
    hb_interval = _heartbeat_interval(globals_cfg)
    log_phase("资源创建", level=2)
    stage = "configmap/scripts"
    try:
        ensure_configmap(cluster, scenario_params, cluster_session, ts, cfg_name, dry)
        stage = "deploy client pods"
        if is_k8s:
            # ---- Deployments: normally pod=process; split-pod mode creates
            # one client Deployment and later binds two processes per pod. ----
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
                    host_ip = "" if dry else _k8s_pod_host_ip(
                        cluster["_controller"], _k8s["namespace"], pod)
                    cluster.setdefault("nodes", {})[pod] = {
                        "kind": "k8s-pod", "_pod": pod, "_ns": _k8s["namespace"],
                        "_ctx": cluster["_controller"].get("_ctx", ""),
                        "_controller": cluster["_controller"],
                        "_controller_workspace": _k8s["controller_workspace"],
                        "_run_label": _run_label, "_role": role,
                        "ip": ip, "fabric_ip": ip, "host_ip": host_ip}
            _kvtest_bind_split_pods(plan)
            involved_nodes = []
            for e in plan["writers"] + plan["readers"]:
                if e["node"] not in involved_nodes:
                    involved_nodes.append(e["node"])
            primary_node_name = (plan["writers"] or plan["readers"])[0]["node"]
            node = get_node(cluster, primary_node_name)
            host = node["ip"]
            if plan.get("dual"):
                _total, _extra = len(plan["clients"]), " (dual)"
            elif plan.get("split_pod"):
                _total = len(plan["clients"])
                _extra = " (每 pod 1 writer + 1 reader)"
            else:
                _total = len(plan["writers"]) + len(plan["readers"])
                _extra = " ({} writers + {} readers)".format(
                    len(plan["writers"]), len(plan["readers"]))
            if n_nodes and _total % n_nodes == 0:
                log("[k8s] kvtest pods ready: {}Pod/Node * {}Nodes = "
                    "{}Pods{} (run={})".format(
                        _total // n_nodes, n_nodes, _total, _extra,
                        _run_label))
            else:
                log("[k8s] kvtest pods ready: {}Pods / {}Nodes{} "
                    "(run={})".format(_total, n_nodes, _extra, _run_label))
        # Artifact filenames no longer carry the full node/Pod name. Preserve
        # the iid -> real node mapping in the in-memory scenario config so CSV
        # aggregation can still resolve node IP/topology after files are
        # pulled into the per-config local directory.
        def _iid_display_node(entry):
            """节点显示名 = 节点 IP（k8s=宿主 hostIP）；pod 名单列保存。"""
            try:
                n = get_node(cluster, entry["node"])
                return _node_display(cluster, n, entry["node"])
            except Exception:
                return entry["node"]

        cfg["_kvtest_iid_nodes"] = {
            str(entry["iid"]): _iid_display_node(entry)
            for entry in plan["writers"] + plan["readers"]
        }
        cfg["_kvtest_iid_pods"] = {
            str(entry["iid"]): (
                (get_node(cluster, entry["node"]) or {}).get("_pod") or "")
            for entry in plan["writers"] + plan["readers"]
        }
        # report context: node count drives the params card, and the /start
        # epoch is the origin of the samples' elapsed_us (map/reduce needs it)
        cfg["_kvtest_nodes"] = n_nodes
        # mid-scenario STOP before the measurement loop (health gate / data
        # gen) has no samples yet, but the context must still name the
        # scenario so _stop_reports does not fall back to the basic path
        _STOP_CTX["report"] = {
            "cfg": cfg, "cfg_name": cfg_name,
            "result_dir": local_result_dir, "local_log_dir": local_log_dir,
            "remote_log_dir": remote_log_dir,
            "involved_nodes": list(involved_nodes),
            "mode": "kvtest", "skip_pull": False,
        }
        stage = "stage scripts"
        deploy_scripts(cluster, involved_nodes, scenario_params, cluster_session,
                       remote_scripts, remote_log_dir, dry)
        stage = "push binary"
        # kvtest.binary_local: push the C++ binary to every node/pod; the
        # pushed path then overrides kvtest.binary for the launch commands
        _pushed_binary = _kvtest_deploy_binary(
            cluster, involved_nodes,
            (cfg.get("kvtest", {}) or {}).get("binary_local", ""),
            remote_scripts, globals_cfg.get("_pod_workspace"), is_k8s, dry)
        if _pushed_binary:
            kv_binary = _pushed_binary
        # optional NUMA binding (ssh only): same machinery as pipeline —
        # numactl prefix for cpu+mem binding, cpu_affinity in the JSON so the
        # binary re-confirms the affinity itself
        stage = "numa probe"
        membind_mode, numa_strict = resolve_numa_policy(cfg, globals_cfg)
        numa_node_count = {}
        numa_counter = {}
        shard_ordinals = {}
        numa_topo = {}
        numactl_ok = {}

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
                    numa_node_count[node_name] = 1
                    numa_topo[node_name] = {0: "0"}
                    numactl_ok[node_name] = False
                numa_counter[node_name] = 0

        def _entry_host(entry):
            """Fabric-network host for the kvtest nodes[] view (ssh: node
            fabric_ip; k8s: pod IP — pods reach each other directly)."""
            n = get_node(cluster, entry["node"])
            return n.get("fabric_ip", n.get("ip", ""))

        def _instance_view():
            """Full-cluster instance list for every instance's nodes array
            (kvtest derives notify peers from it; writers pick reader/dual
            roles, everyone excludes themselves by instance_id)."""
            view = []
            if plan.get("dual"):
                for c in plan["clients"]:
                    view.append({"iid": c["iid"], "port": c["port"],
                                 "role": "dual", "host": _entry_host(c)})
            else:
                for w in plan["writers"]:
                    view.append({"iid": w["iid"], "port": w["port"],
                                 "role": "writer", "host": _entry_host(w)})
                for r in plan["readers"]:
                    view.append({"iid": r["iid"], "port": r["port"],
                                 "role": "reader", "host": _entry_host(r)})
            return view


        def _prepare_instance(entry, role, ds, key_pool, all_instances,
                              lhost_counter):
            """Per-instance naming / QPS split / NUMA binding / config build
            plus the INERT base64 config push (base64 over ssh so the k8s
            ConfigMap stays shared/read-only). Nothing runs yet — the nohup
            command is stored on the entry for _fire_instances."""
            n = get_node(cluster, entry["node"])
            base = _kvtest_artifact_base(
                cfg_name, entry["iid"], entry["port"], ds)
            entry["_base"] = base
            entry["_ds"] = ds
            # 每 pod 独立目录（k8s）：<remote_log_dir>/<pod>/<base>.*；
            # ssh 模式保持平铺。现场同目录：.log/.log.ylt.log/.exitrc +
            # MC_LOG_DIR 的 <base>.glog.d/（glog INFO 分段）
            entry["_pod_dir"] = _kvtest_pod_dir(remote_log_dir, n)
            log_base = _kvtest_pod_base(remote_log_dir, base, n)
            # kvtest_out 也收进 pod 目录：<pod_dir>/kvtest_out/<base>（一个
            # pod 一个目录，现场不出现平铺的共享 kvtest_out 树）
            entry["_out_dir"] = "{}/kvtest_out/{}".format(
                _kvtest_pod_dir(remote_log_dir, n).rstrip("/"), base)
            cfg_file = log_base + ".json"
            log_file = log_base + ".log"
            nohup_log = log_base + ".nohup"
            # unique local_hostname port per node (tcp protocol binds it)
            cnt = lhost_counter.get(entry["node"], 0)
            lhost_counter[entry["node"]] = cnt + 1
            local_port_base = int((cfg.get("kvtest", {}) or {}).get(
                "local_port_base", 12345) or 12345)
            entry["_lhost_port"] = local_port_base + cnt
            # per-instance integer QPS split of the node's rate
            if role in ("writer", "dual"):
                cpn = int((cfg.get("client", {}) or {}).get("count_per_node", 1)) \
                    if plan.get("dual") or plan.get("split_pod") else \
                    int((cfg.get("writer", {}) or {}).get("count_per_node", 1))
                entry["_qps"] = _kvtest_split_qps(plan["per_node_qps"], cpn,
                                                  entry["local"])
            else:
                entry["_qps"] = 0
            # SSH uses the legacy topology-driven binding. K8s split-mode
            # scenarios may provide an explicit writer/reader NUMA map; the
            # runner applies CPU-node and memory-node binding inside the pod.
            launch_prefix = ""
            if (cpu_shard > 0 or numa_bind) and not dry \
                    and not is_k8s_node(n):
                nn = numa_counter[entry["node"]] % numa_node_count[entry["node"]]
                numa_counter[entry["node"]] += 1
                idx = shard_ordinal(entry["node"], nn) if cpu_shard > 0 else 0
                cl = numa_binding_spec(numa_topo[entry["node"]][nn],
                                       node_exclude(cluster, n),
                                       cpu_shard, idx)
                entry["_cpu_affinity"] = cl
                if numactl_ok.get(entry["node"]):
                    launch_prefix = numactl_launch_prefix(membind_mode, nn, cl) + " "
            else:
                entry["_cpu_affinity"] = ""
            explicit_numa_node = None
            if is_k8s:
                explicit_numa_node = _kvtest_numa_node(cfg, entry, role)
                if explicit_numa_node is not None:
                    launch_prefix = "env KVTEST_NUMA_NODE={} ".format(
                        explicit_numa_node)
            entry["_numa_node"] = explicit_numa_node
            inst_cfg = build_kvtest_instance_config(
                cfg, globals_cfg, cluster_session, plan, entry, role, ds,
                key_pool, all_instances,
                key_namespace=getattr(args, "parallel_ready_role", None))
            payload = base64.b64encode(
                json.dumps(inst_cfg).encode("utf-8")).decode("ascii")
            if dry:
                log("[dry-run] kvtest {} i{} on {}: config {} ({} B), "
                    "qps={}, numa_node={}, out {}".format(
                        role, entry["iid"], entry["node"], cfg_file,
                        len(payload), entry["_qps"], explicit_numa_node,
                        entry["_out_dir"]))
            else:
                # the push itself is deferred to _push_instances: the serial
                # build above owns the order-sensitive port/NUMA slot
                # assignment, only the per-instance execs overlap.
                entry["_push_cmd"] = (
                    "mkdir -p '{d}' && printf '%s' '{b}' | base64 -d > '{f}'"
                    .format(d=_kvtest_pod_dir(remote_log_dir, n),
                            b=payload, f=cfg_file))
            entry["_role"] = role
            entry["_ccmd"] = ("nohup {p}bash {runner} {log} {cfgf} {bin} "
                              "> {nohup} 2>&1 & echo $!").format(
                p=launch_prefix, runner=remote_runner_kvtest, log=log_file,
                cfgf=cfg_file, bin=kv_binary, nohup=nohup_log)

        def _push_instances(entries):
            """Push the PREPARED per-instance configs with bounded
            parallelism (54 serial execs ~= 13.5s -> ~2s at 16 workers)."""
            if dry or not entries:
                return

            def _push(entry):
                n = get_node(cluster, entry["node"])
                ssh_check_output(n["ip"], n, entry["_push_cmd"], quiet=True)

            from concurrent.futures import ThreadPoolExecutor
            workers = max(1, min(_PIPELINE_PARALLEL, len(entries)))
            with ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(_push, entries))

        def _fire_instances(entries):
            """Fire PREPARED kvtest instances with bounded parallelism.

            kvtest starts paused, so fire only creates healthy processes;
            the cross-child barrier runs after every health gate and aligns
            the later /start measurement window.
            The pid ledger and state append stay on the main thread in
            submission order."""
            if not entries:
                return

            def _fire(entry):
                n = get_node(cluster, entry["node"])
                return ssh_check_output(n["ip"], n, entry["_ccmd"],
                                        dry_run=dry)

            if dry:
                pids = ["" for _ in entries]
            else:
                from concurrent.futures import ThreadPoolExecutor
                workers = max(1, min(_PIPELINE_PARALLEL, len(entries)))
                with ThreadPoolExecutor(max_workers=workers) as pool:
                    pids = list(pool.map(_fire, entries))
            for entry, pid in zip(entries, pids):
                instance_pids.append((entry, pid))
                if not dry and pid:
                    n = get_node(cluster, entry["node"])
                    state_append([{
                        "ip": n["ip"], "node": entry["node"],
                        "port": entry["port"],
                        "fabric_host": n.get("fabric_ip", n.get("ip", "")),
                        "pid": pid.strip(),
                        "label": "kvtest {} i{}".format(entry["_role"],
                                                        entry["iid"]),
                        "marker": _KVTEST_PID_MARKER,
                        "cfg": cfg_name,
                        "ts": time.strftime("%Y-%m-%d %H:%M:%S")}])

        current_ds = None
        for ds in sizes:
            # fresh store per size: full remove-all (kvtest keys use the
            # hardcoded mooncake_kvtest_ prefix, but scope=full clears them)
            stage = "remove-keys (pre-run, {})".format(ds)
            log_phase("清库 remove-keys", level=2)
            _remove_keys_stage(cluster, globals_cfg, host, node,
                               remote_runner_pipeline, remote_log_dir,
                               cfg_name, cfg_name, "full",
                               "{} start".format(ds), dry=dry,
                               pull_to=local_log_dir,
                               node_name=primary_node_name, tag="kvtest")
            current_ds = ds
            instance_pids = []
            key_pool = pools[ds]
            all_instances = _instance_view()
            lhost_counter = {}
            log_phase("启动实例", level=2)
            # profiling lifecycle: whole_run covers instance boot + health
            # gates; per_stage starts at the measurement window (below)
            if (matrix and not dry and prof_core and profile_sess is None
                    and matrix.get("trigger", "whole_run") == "whole_run"):
                try:
                    profile_sess = prof_core.start_profiling(
                        cluster, matrix,
                        _profiling_targets(cluster, matrix, involved_nodes,
                                           ts, cfg_name, dry),
                        cfg_name,
                        remote_profiling_dir(remote_log_dir, cfg_name),
                        epoch_base=time.time())
                except Exception as exc:
                    log("[WARN] [profile] start failed: {}".format(exc))
            if plan.get("dual"):
                stage = "launch dual instances ({})".format(ds)
                # one process hosts writer+reader: prepare all configs (and
                # the peer child's remove-keys/setup) BEFORE any process
                # exists, then fire paused processes. The cross-child barrier
                # runs after health gates, immediately before /start.
                for c in plan["clients"]:
                    _prepare_instance(c, "dual", ds, key_pool, all_instances,
                                      lhost_counter)
                _push_instances(plan["clients"])
                _fire_instances(plan["clients"])
                stage = "health gate (dual, {})".format(ds)
                if not dry:
                    _kvtest_health_gate(cluster, plan["clients"],
                                        health_timeout_s, "dual instances")
            else:
                # start_paused keeps writer threads inert until the /start
                # fan-out below, and a C++ writer only starts sending at
                # /start (main.cpp /start -> writer->Start) — so the classic
                # "readers before writers" ordering contract is satisfied
                # as long as every health gate passes before /start. Default:
                # prepare+fire both roles in one batch and gate them together
                # (saves one serial config-push round and one whole gate,
                # ~35s at recall scale); kvtest.reader_first_launch=true
                # restores the legacy two-phase stagger.
                def _prepare_role(role, entries):
                    nonlocal stage
                    stage = "launch {}s ({})".format(role, ds)
                    for e in entries:
                        _prepare_instance(e, role, ds, key_pool,
                                          all_instances, lhost_counter)
                    _push_instances(entries)

                def _gate_role(entries, what):
                    nonlocal stage
                    stage = "health gate ({}, {})".format(what, ds)
                    if not dry:
                        _kvtest_health_gate(cluster, entries,
                                            health_timeout_s, what)

                _kvtest_launch_split_roles(
                    plan, _kvtest_split_launch_order(cfg),
                    _prepare_role, _fire_instances, _gate_role)
            # All processes are healthy but start_paused=true keeps writer
            # threads inert. /start releases the same deployment for warmup
            # (when configured) or directly for measurement.
            items = [(get_node(cluster, e["node"]), e["port"], "",
                      "kvtest i{}".format(e["iid"]))
                     for e, _pid in instance_pids if "node" in e]
            def _control(path, phase, timeout=health_timeout_s):
                results = _pipeline_http_parallel(
                    cluster, items, path, timeout=timeout, dry=dry)
                failures = ["{}: {}".format(what, err or "HTTP non-200")
                            for what, ok, err in results if not ok]
                if failures:
                    raise RuntimeError("kvtest {} failed: {}".format(
                        phase, "; ".join(failures)))

            def _wait_window(seconds, label):
                if dry or seconds <= 0:
                    return
                log("[kvtest] {} window: {}s (ETA {})".format(
                    label, seconds,
                    time.strftime("%H:%M:%S",
                                  time.localtime(time.time() + seconds))))
                hb = (_Heartbeat("kvtest", seconds, hb_interval)
                      if label == "measurement" else None)
                started = time.time()
                while time.time() - started < seconds:
                    _check_stop()
                    if hb is not None:
                        hb.tick()
                    time.sleep(min(
                        1.0, max(0.05, seconds - (time.time() - started))))

            log_phase("测量", level=2)
            if matrix and not dry and prof_core and profile_sess is None:
                # per_stage trigger: profiling starts at the measurement window
                try:
                    profile_sess = prof_core.start_profiling(
                        cluster, matrix,
                        _profiling_targets(cluster, matrix, involved_nodes,
                                           ts, cfg_name, dry),
                        cfg_name,
                        remote_profiling_dir(remote_log_dir, cfg_name),
                        epoch_base=time.time())
                except Exception as exc:
                    log("[WARN] [profile] start failed: {}".format(exc))
            stage = "/start ({})".format(ds)
            try:
                _parallel_start_barrier(args)
                _control("/start", "/start")
                if not dry:
                    # /start is the epoch origin for samples' elapsed_us AND
                    # for the warmup cutoff below. There is no separate
                    # warmup phase any more: the C++ runs ONE measured window
                    # (its warmup phase never runs — the instance config
                    # carries no warmup_duration) and the first
                    # warmup_duration seconds are classified as warmup by the
                    # MP data plane (excluded from the formal statistics,
                    # still drawn in the charts with a boundary line).
                    cfg.setdefault("_kvtest_start_epochs", {})[str(ds)] = \
                        time.time()

                # ---- single measured window = warmup + formal duration.
                # No /pause//reset//arm//resume cycle: keys are written once
                # (no self-collision at the boundary), and the MP-side cutoff
                # (start epoch + warmup_s) splits warmup from formal rows.
                _total_window = float(warmup_duration) + float(duration)
                _remove_service_prewarm_async(cluster, globals_cfg, ts,
                                              cfg_name, dry,
                                              window_s=_total_window)
                stage = "measurement ({})".format(ds)
                if warmup_duration > 0:
                    log("[kvtest] measurement window: {:g}s "
                        "(first {:g}s warmup, excluded from formal stats; "
                        "drawn in charts)".format(_total_window,
                                                  float(warmup_duration)))
                _wait_window(_total_window, "measurement")
            except StopRequested:
                # /stop first: the C++ collector flushes its request samples
                # only there, and the partial HTML report is built from them
                _kvtest_stop_collect(
                    cluster, cfg, cfg_name, remote_log_dir,
                    local_log_dir, local_result_dir, instance_pids, items,
                    remote_scripts=remote_scripts,
                    globals_cfg=globals_cfg, ts=ts, is_k8s=is_k8s)
                # node monitors: stop + collect so the STOP partial report
                # (reduce_monitors) sees the samples too
                if profile_sess is not None:
                    _stop_profiling(cluster, profile_sess, local_result_dir,
                                    cfg_name, dry=dry)
                    profile_sess = None
                raise
            # /stop joins in-flight work and MetricsCollector::Stop writes the
            # single final summary. Avoid a racing pre-stop /summary snapshot.
            # Salvage baseline: summary.json is only written by that Stop, so
            # "absent -> present" (or a later mtime) proves the data persisted
            # even when the binary then hangs in its shutdown path.
            _stop_baseline = {}
            if not dry:
                for _entry, _pid in instance_pids:
                    _stop_baseline[_entry["iid"]] = _kvtest_summary_stat(
                        cluster, _entry)
            stage = "/stop ({})".format(ds)
            log("[kvtest] /stop: 通知 {} 个实例停机并落盘...".format(len(items)))
            for _what, _ok, _err in _pipeline_http_parallel(cluster, items,
                                                            "/stop",
                                                            timeout=10):
                if not _ok:
                    log("[WARN] {} /stop: {}".format(_what, _err))
            # ---- wait for exit + exit-code ledger ----
            stage = "wait exit ({})".format(ds)
            # One shared 30s grace period replaces the old serial 120s per
            # instance waits.  A second /stop is an idempotent recovery probe
            # for older kvtest binaries whose blocking accept() needed a wake
            # connection before listener teardown could finish.
            _t0 = time.time()
            if dry:
                _exit_results = [True for _ in instance_pids]
            else:
                _exit_results = _wait_kvtest_processes(
                    cluster, instance_pids, 30, expected_s=30,
                    progress_interval=10)
                _remaining = [pair for pair, exited in
                              zip(instance_pids, _exit_results) if not exited]
                if _remaining:
                    log("[WARN] kvtest {}/{} instances still alive 30s after "
                        "/stop; retrying /stop once".format(
                            len(_remaining), len(instance_pids)))
                    _retry_items = [
                        (get_node(cluster, entry["node"]), entry["port"], "",
                         "kvtest retry-stop i{}".format(entry["iid"]))
                        for entry, _pid in _remaining]
                    for _what, _ok, _err in _pipeline_http_parallel(
                            cluster, _retry_items, "/stop", timeout=5):
                        if not _ok:
                            log("[WARN] {}: {}".format(_what, _err))
                    _retry_results = _wait_kvtest_processes(
                        cluster, _remaining, 30,
                        label_prefix="kvtest retry-stop")
                    _retry_by_iid = {
                        entry["iid"]: exited
                        for (entry, _pid), exited in
                        zip(_remaining, _retry_results)}
                    _exit_results = [
                        exited or _retry_by_iid.get(entry["iid"], False)
                        for (entry, _pid), exited in
                        zip(instance_pids, _exit_results)]
            _exited = sum(1 for exited in _exit_results if exited)
            for (entry, pid), exited in zip(instance_pids, _exit_results):
                if not dry and pid and exited:
                    _n = get_node(cluster, entry["node"])
                    _check_exitrc(
                        cluster, entry["node"],
                        _kvtest_pod_base(remote_log_dir, entry["_base"], _n)
                        + ".log.exitrc",
                        "kvtest i{}".format(entry["iid"]))
            if not dry:
                log("[kvtest] instances exited: {}/{} ({:.1f}s)".format(
                    _exited, len(instance_pids), time.time() - _t0))
                if _exited != len(instance_pids):
                    _stuck = [(entry, pid) for (entry, pid), exited in
                              zip(instance_pids, _exit_results)
                              if not exited]
                    _salvaged = _kvtest_salvage_stuck(
                        cluster, _stuck, _stop_baseline)
                    if len(_salvaged) != len(_stuck):
                        _alive = ["i{} on {}".format(entry["iid"],
                                                     entry["node"])
                                  for entry, _pid in _stuck]
                        raise RuntimeError(
                            "kvtest shutdown timed out after two /stop "
                            "attempts; still alive: {}".format(
                                ", ".join(_alive)))
                    log("[WARN] [kvtest] salvaged {}/{} /stop-stuck "
                        "instances ({}) — metrics persisted before the "
                        "shutdown hang; continuing to collection".format(
                            len(_salvaged), len(_stuck), ds))
            # ---- flatten artifacts + remote trace map + compact pull ----
            stage = "collect artifacts ({})".format(ds)
            log_phase("收集", level=2)
            _t_pull = time.time()
            # 对齐 pipeline 的 stats-only 策略：原始 .log 与 MC_LOG_DIR 的
            # glog 分段（可达数百 MB～GB/实例）一律不传输，留在节点现场；
            # glog 的 trace 统计在 pod 内算好后只回传小 trace.json。
            _extra_bytes = {}

            def _collect_one(pair):
                """Per-instance artifact pass: flatten (in-pod cp) + in-pod
                tracestats + gzip + byte stat. Runs for every instance with
                bounded parallelism — the former serial loop cost one exec
                round per instance (~0.3-0.5s x 54)."""
                entry, _pid = pair
                n = get_node(cluster, entry["node"])
                base = entry["_base"]
                pbase = _kvtest_pod_base(remote_log_dir, base, n)
                status = ""
                if not dry:
                    ok, status = _kvtest_flatten_artifacts(
                        cluster, entry, remote_log_dir,
                        attempts=5, retry_s=2)
                    if not ok:
                        raise RuntimeError(
                            "kvtest i{} exited but final artifacts were not "
                            "both available after retries: {} in {}"
                            "".format(entry["iid"], status,
                                      entry["_out_dir"]))
                    _trace_files = _kvtest_remote_trace_map(
                        cluster, entry, remote_log_dir, remote_scripts,
                        globals_cfg,
                        warmup_s=cfg.get("_kvtest_warmup_duration"))
                    _extra_bytes[base] = _kvtest_small_log_bytes(
                        cluster, entry, remote_log_dir)
                    # 压缩大件再传输（~8-12x）；失败时透明回退原文件名
                    _trace_files = _kvtest_gzip_remote_files(
                        cluster, entry["node"], _trace_files)
                    samples_file = pbase + ".samples.csv"
                else:
                    _trace_files = []
                    samples_file = None
                # 只请求确实存在的文件（trace map 失败时不拉、不因此判失败）
                _files = [pbase + ".summary.json"] + list(_trace_files)
                if dry:
                    _files.append(pbase + ".samples.csv")
                elif "samples=OK" in status:
                    _files.extend(_kvtest_gzip_remote_files(
                        cluster, entry["node"], [samples_file]))
                else:
                    log("[WARN] kvtest i{} uses an older binary without "
                        "latency_samples.csv; request-level plot unavailable"
                        "".format(entry["iid"]))
                return n, _files, "kvtest i{}".format(entry["iid"])

            _groups = []
            if dry:
                _groups = [_collect_one(p) for p in instance_pids]
            else:
                from concurrent.futures import ThreadPoolExecutor
                _workers = max(1, min(_PIPELINE_PARALLEL,
                                      len(instance_pids) or 1))
                with ThreadPoolExecutor(max_workers=_workers) as pool:
                    _groups = list(pool.map(_collect_one, instance_pids))
            _ctrl_done = False
            if (not dry and is_k8s and _controller_reduce_enabled(globals_cfg)
                    and not getattr(args, "png", False)
                    and not getattr(args, "pull_raw_logs", False)):
                try:
                    _t_red = time.time()
                    _kvtest_controller_collect(
                        cluster, globals_cfg, cfg, args, ts, cfg_name, ds,
                        instance_pids, _groups, local_log_dir,
                        _extra_bytes, qps_target=plan.get("total_qps") or 0,
                        nodes=int(cfg.get("_kvtest_nodes") or 0),
                        remote_log_dir=remote_log_dir, is_k8s=is_k8s)
                    _ctrl_done = True
                    log("[kvtest] controller data-plane done in {:.1f}s "
                        "(pod->controller single hop; orchestrator keeps no "
                        "intermediates, only reduced/meta/summary)".format(
                            time.time() - _t_red))
                except Exception as exc:
                    log("[WARN] controller map+reduce failed, falling back "
                        "to local pull: {}".format(exc))
            if not _ctrl_done:
                _pipeline_pull_parallel(cluster, _groups, local_log_dir, dry)
                # gunzip the pulled .gz back to original names so the map/report
                # glob surface stays unchanged, THEN account bytes per category.
                if not dry:
                    _gunzip_local_dir(local_log_dir)
            _pulled_bytes = 0
            _cat_bytes = {"samples": 0, "trace": 0, "summary": 0, "other": 0}
            if not dry and not _ctrl_done:
                for _n, _files, _l in _groups:
                    for _f in _files:
                        _name = os.path.basename(_f)
                        if _name.endswith(".gz"):
                            _name = _name[:-len(".gz")]
                        try:
                            _sz = os.path.getsize(
                                os.path.join(local_log_dir, _name))
                        except OSError:
                            continue
                        _pulled_bytes += _sz
                        if ".samples.csv" in _name:
                            _cat_bytes["samples"] += _sz
                        elif ".trace.json" in _name:
                            _cat_bytes["trace"] += _sz
                        elif ".summary.json" in _name:
                            _cat_bytes["summary"] += _sz
                        else:
                            _cat_bytes["other"] += _sz
            if not dry:
                # keep node mapping + this size's /start epoch local for the
                # report map and for --rebuild-report after a later failure
                cfg["_kvtest_extra_bytes"] = _extra_bytes
                _ctx = _kvtest_report_context(cluster, ts, remote_log_dir,
                                              is_k8s)
                _ctx["site_dirs"] = _kvtest_site_dirs(
                    cluster, remote_log_dir, cfg)
                _ctx["remote_names"] = _kvtest_remote_names(_groups)
                _ctx["warmup_s"] = float(
                    cfg.get("_kvtest_warmup_duration") or 0)
                _kvtest_write_report_meta(local_log_dir, cfg, _ctx)
            if not _ctrl_done:
                log("[kvtest] pulled: {} files / {} instances ({:.1f} min, "
                    "{}; samples={:.1f}MB trace={:.1f}MB summary={:.1f}MB "
                    "other={:.1f}MB; raw logs stay on nodes)".format(
                    sum(len(f) for _n, f, _l in _groups), len(_groups),
                    (time.time() - _t_pull) / 60.0, _fmt_mb(_pulled_bytes),
                    _cat_bytes["samples"] / 1048576.0,
                    _cat_bytes["trace"] / 1048576.0,
                    _cat_bytes["summary"] / 1048576.0,
                    _cat_bytes["other"] / 1048576.0))
            # remove empty .nohup redirect files (non-empty kept for
            # diagnostics on the node)
            if not dry:
                for node_name in involved_nodes:
                    n = get_node(cluster, node_name)
                    nohup_files = [
                        _kvtest_pod_base(remote_log_dir, e["_base"], n)
                        + ".nohup"
                        for e, _p in instance_pids
                        if e["node"] == node_name]
                    if nohup_files:
                        cleanup_cmd = ("for f in {}; do [ -s \"$f\" ] || "
                                       "rm -f \"$f\"; done").format(
                                           " ".join(nohup_files))
                        ssh_check_output(n["ip"], n, cleanup_cmd, dry_run=dry)
        # profiling: stop sampling + collect (never raises); also tears down
        # the node-monitor DaemonSet
        if profile_sess is not None:
            _stop_profiling(cluster, profile_sess, local_result_dir, cfg_name,
                            dry=dry)
            profile_sess = None
        # Config end: remove-all — clears everything this config's writers
        # wrote (kvtest keys share the store with nothing else here)
        stage = "remove-keys (config end)"
        log_phase("清理收尾", level=2)
        _remove_keys_stage(cluster, globals_cfg, host, node,
                           remote_runner_pipeline, remote_log_dir, cfg_name,
                           "remove-keys-end", "full", "config end",
                           dry=dry, pull_to=local_log_dir, tag="kvtest")
        # All kvtest instances exited naturally -> drop their ledger rows.
        if not dry:
            state_remove_cfg(cfg_name)
        # 现场日志实测大小（remove 服务日志/profile 产物已落盘、pod 仍存活）
        _site_du_snapshot(cluster, ts, cfg_name, involved_nodes,
                          local_result_dir, remote_log_dir, dry=dry)
        # k8s pods stay alive past this return: main()'s sweep-end
        # remove-keys still needs a live pod; cleanup_pods runs in main()
        # after sweep-end. Hand the sweep-end pass this config's coordinates.
        _STOP_CTX.pop("report", None)   # normal end: no partial report pending
        return {"host": host, "node": primary_node_name, "runner": remote_runner_pipeline,
                "remote_log_dir": remote_log_dir}
    except Exception as exc:
        _STOP_CTX.pop("report", None)   # failure path, not a STOP report
        # FAIL path: one line names scenario + failing stage + reason, then
        # the pod table and the raw-log directories. No log extraction, no
        # describe/event dumps (see _scenario_failure_report).
        if not dry:
            class1 = is_k8s and stage.startswith("deploy")
            _scenario_failure_report(cluster, cfg_name, stage, exc,
                                     involved_nodes, remote_log_dir, ts,
                                     is_k8s, class1=class1)
            if class1:
                log("[FAIL] cleanup skipped (pods never started)")
            else:
                fail_cleanup_kvtest(cluster, instance_pids, involved_nodes,
                                    remote_log_dir, cfg_name, local_log_dir,
                                    remove_host=locals().get("host"),
                                    remove_node=locals().get("primary_node_name"),
                                    remove_runner=locals().get("remote_runner_pipeline"),
                                    globals_cfg=globals_cfg,
                                    remote_scripts=locals().get("remote_scripts"),
                                    ts=locals().get("ts"), is_k8s=is_k8s,
                                    failed_stage=stage)
                # profiling collect BEFORE the report chain so monitor CSVs
                # reach reduce_monitors (same contract as pipeline/basic)
                if profile_sess is not None:
                    _stop_profiling(cluster, profile_sess, local_result_dir,
                                    cfg_name, dry=dry)
                reap_state_entries(cluster)
            if is_k8s:
                cluster["_k8s_keep_ns"] = True
            else:
                cleanup_pods(cluster, involved_nodes, dry)
            cfg["_fail_reported"] = True
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


def _k8s_pods_wide(cluster):
    """Print the full namespace pod list (-o wide) on failure."""
    c = cluster.get("_controller")
    if c is None:
        return
    try:
        ns = k8s_settings(cluster)["namespace"]
        out, rc = controller_run(c, ["-n", ns, "get", "pods", "-o", "wide"],
                                 check=False)
        if rc == 0 and out.strip():
            log("[k8s] pods (ns={}):\n{}".format(ns, out.strip()))
        else:
            log("[WARN] cannot list pods in namespace {}: {}".format(
                ns, out.strip()[:300]))
    except Exception as exc:
        log("[WARN] pod list failed: {}".format(exc))


def _k8s_run_pods(cluster, ts, cfg_name):
    """Pod names created for this run (by the run label), for describe hints."""
    c = cluster.get("_controller")
    if c is None:
        return []
    try:
        ns = k8s_settings(cluster)["namespace"]
        out, rc = controller_run(
            c, ["-n", ns, "get", "pods", "-l",
                "run=" + k8s_run_label(ts, cfg_name), "-o", "name"],
            check=False)
        if rc != 0:
            return []
        return [ln.split("/", 1)[1] for ln in out.splitlines() if "/" in ln]
    except Exception:
        return []


def _raw_log_dirs(cluster, involved_nodes, remote_log_dir, ts):
    """One line per pod/node: where its raw logs (log/glog/exitrc) stay."""
    for node_name in involved_nodes:
        try:
            n = get_node(cluster, node_name)
        except Exception:
            continue
        if is_k8s_node(n):
            host_root = (k8s_settings(cluster).get("host_log_root") or "")
            where = "{}/{}/{}/".format(host_root.rstrip("/"), ts,
                                       n.get("_pod") or "<pod>")
            ssh_to = n.get("host_ip") or n.get("ip") or node_name
        else:
            where = "{}/".format(_kvtest_pod_dir(remote_log_dir, n))
            ssh_to = n.get("ip") or node_name
        log("[FAIL] raw logs: ssh {} -> {}".format(ssh_to, where))


def _scenario_failure_report(cluster, cfg_name, stage, exc, involved_nodes,
                             remote_log_dir, ts, is_k8s, class1=False):
    """Unified failure output for every scenario type (kvtest/basic/pipeline).

    Case 1 (class1: pods never became ready): name the failing deploy stage
    and the detected reason, print the full pod list and the inspect commands
    only — no describe/events content. Case 2 (a stage failed after the pods
    were up): name scenario + stage + exception, print the full pod list and
    the per-pod raw-log directories. Nothing is pulled or truncated."""
    log("[FAIL] scenario {} failed at stage '{}': {}".format(
        cfg_name, stage, exc))
    if is_k8s:
        _k8s_pods_wide(cluster)
        if class1:
            ns = k8s_settings(cluster)["namespace"]
            pods = _k8s_run_pods(cluster, ts, cfg_name)
            pod = pods[0] if pods else "<pod>"
            view = " ".join(kubectl_prefix(
                {"_ctx": cluster["_controller"].get("_ctx", ""), "_ns": ns}))
            log("[FAIL] inspect: {} describe pod {}".format(view, pod))
            log("[FAIL] inspect: {} get events --sort-by=.lastTimestamp"
                .format(view))
            return
    _raw_log_dirs(cluster, involved_nodes, remote_log_dir, ts)


def _log_pod_state(cluster, node_name, why):
    """Best-effort pod describe tail when a k8s client dies or times out
    (container restarts / OOMKilled / Last State live only there)."""
    c = cluster.get("_controller")
    if c is None:
        return
    try:
        n = get_node(cluster, node_name)
    except Exception:
        return
    if not is_k8s_node(n):
        return
    try:
        ns = n.get("_ns") or k8s_settings(cluster)["namespace"]
        out, rc = controller_run(
            c, ["-n", ns, "describe", "pod", n["_pod"]], check=False)
        if rc == 0 and out.strip():
            lines = out.strip().splitlines()
            log("[k8s] pod {} state ({}; tail):\n{}".format(
                n["_pod"], why, "\n".join(lines[-25:])))
    except Exception:
        pass


def cleanup_pods(cluster, involved_nodes, dry_run):
    """Delete the per-config client pods + Deployments + ConfigMap (k8s)."""
    c = cluster.get("_controller")
    if c is None:
        return
    # safety net: the monitor DaemonSet normally goes away in _stop_profiling
    _delete_monitor_daemonset(cluster, dry_run=dry_run)
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
        # hostPath 日志按用户要求持久化在节点上（<root>/<run_id>/<pod>/），
        # CLEAN-UP 不自动删除（追查现场优先）；controller 无法直接登录
        # 各节点，给出逐节点清理命令提示。
        try:
            root = k8s_settings(cluster)["host_log_root"]
            log("[k8s] host logs kept on each node at {}/<run_id>/<pod>/ "
                "(CLEAN-UP does not delete them); free space per node with: "
                "rm -rf {}/*".format(root, root))
        except Exception:
            pass
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
    # stats-first: raw logs are no longer pulled by default — the compact
    # *.stats.json (remote logstats map) carries every metric this parser
    # returns. Raw parsing remains the fallback (--png / failure pulls /
    # legacy runs). Per-role files are already role-filtered, so op_filter is
    # not needed on the stats path.
    stats = []
    for p in paths:
        sp = str(p) if str(p).endswith(".stats.json") else str(p) + ".stats.json"
        st = load_stats(sp) if os.path.isfile(sp) else None
        if st is not None:
            stats.append(st)
    if stats and len(stats) == len(paths):
        return _merge_stats_aggs(stats, op_filter=op_filter)
    latencies = []
    lat_sum_ms = 0.0
    ok = fail = 0
    total_bytes = 0
    starts = []
    walls = []
    queue_waits = []
    max_depth = 0
    notify = {k: 0 for k in (
        "notify_submitted", "notify_ok", "notify_fail", "notify_inflight_peak",
        "notify_backpressure_ms", "notify_drain_ms", "notify_max_ms",
        "notify_queue_peak", "notify_queue_wait_max_ms",
        "notify_connections_created", "notify_connections_reused")}
    notify_elapsed_sum = 0.0
    notify_queue_wait_sum = 0.0
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
                if (s.get("mode") == "pipeline_writer_summary" and
                        op_filter in (None, "pipeline_write")):
                    for k in notify:
                        if k in ("notify_inflight_peak", "notify_max_ms",
                                 "notify_queue_peak", "notify_queue_wait_max_ms"):
                            notify[k] = max(notify[k], s.get(k, 0))
                        else:
                            notify[k] += s.get(k, 0)
                    notify_elapsed_sum += s.get("notify_avg_ms", 0) * (
                        s.get("notify_ok", 0) + s.get("notify_fail", 0))
                    notify_queue_wait_sum += s.get("notify_queue_wait_avg_ms", 0) * (
                        s.get("notify_ok", 0) + s.get("notify_fail", 0))
    latencies.sort()
    if starts and len(walls) == len(starts):
        ends = [s + w for s, w in zip(starts, walls)]
        wall = max(ends) - min(starts)
    else:
        wall = 0.0
    n = len(latencies)
    total = ok + fail
    completed = notify["notify_ok"] + notify["notify_fail"]
    notify["notify_avg_ms"] = notify_elapsed_sum / completed if completed else 0.0
    notify["notify_queue_wait_avg_ms"] = notify_queue_wait_sum / completed if completed else 0.0
    return dict(notify, **{
        "total": total,
        "total_bytes": total_bytes,
        "wall_s": wall,
        "throughput_mb_s": (total_bytes / wall / 1024.0 ** 2) if wall > 0 else 0.0,
        "throughput_gb_s": (total_bytes / wall / 1024.0 ** 3) if wall > 0 else 0.0,
        "avg_ms": (sum(latencies) / n) if n else 0.0,
        "min_ms": percentile(latencies, 0.0),
        "p50_ms": percentile(latencies, 0.50),
        "p90_ms": percentile(latencies, 0.90),
        "p95_ms": percentile(latencies, 0.95),
        "max_ms": percentile(latencies, 1.0),
        "p99_ms": percentile(latencies, 0.99),
        "p999_ms": percentile(latencies, 0.999),
        "p9999_ms": percentile(latencies, 0.9999),
        "lat_sum_ms": lat_sum_ms,
        "ok": ok,
        "fail": fail,
        "qps": (ok / wall) if wall > 0 else 0.0,
        "fail_rate": (fail * 100.0 / total) if total else 0.0,
        "queue_wait_avg_ms": (sum(queue_waits) / len(queue_waits)) if queue_waits else 0.0,
        "queue_wait_max_ms": max(queue_waits) if queue_waits else 0.0,
        "max_depth": max_depth,
    }, **{
        column: sum(1 for latency in latencies
                    if latency > threshold_us / 1000.0)
        for threshold_us, column in zip(
            LATENCY_THRESHOLD_US, LATENCY_THRESHOLD_COLUMNS)
    })


def _kvtest_latency_stats(values):
    """Python/basic-compatible latency statistics over successful samples."""
    ordered = sorted(values)
    n = len(ordered)
    return {
        "min_ms": ordered[0] if ordered else 0.0,
        "avg_ms": (sum(ordered) / n) if n else 0.0,
        "p50_ms": percentile(ordered, 0.50),
        "p90_ms": percentile(ordered, 0.90),
        "p95_ms": percentile(ordered, 0.95),
        "p99_ms": percentile(ordered, 0.99),
        "p999_ms": percentile(ordered, 0.999),
        "p9999_ms": percentile(ordered, 0.9999),
        "max_ms": ordered[-1] if ordered else 0.0,
        **{
            column: sum(1 for latency in ordered
                        if latency > threshold_us / 1000.0)
            for threshold_us, column in zip(
                LATENCY_THRESHOLD_US, LATENCY_THRESHOLD_COLUMNS)
        },
    }


_LOGSTATS_MOD = [None]


def load_stats(path):
    """Load one logstats <log>.stats.json (None when missing/corrupt)."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def _logstats_module():
    """The scripts/logstats.py module (histogram helpers shared with the map
    stage, so the CSV never re-implements the bin ladder)."""
    if _LOGSTATS_MOD[0] is None:
        import importlib.util as _ilu
        spec = _ilu.spec_from_file_location(
            "mc_logstats", os.path.join(SCRIPTS_DIR, "logstats.py"))
        mod = _ilu.module_from_spec(spec)
        spec.loader.exec_module(mod)
        _LOGSTATS_MOD[0] = mod
    return _LOGSTATS_MOD[0]


def _merge_stats_op(stats_list, op, ls):
    """Aggregate ONE op across stats files (CSV rows of mixed put/get runs)."""
    total = ok = fail = 0
    ok_rows = 0
    lat_sum = 0.0
    hist = {}
    gmin = gmax = None
    total_bytes = 0
    wall = 0.0
    max_depth = 0
    versions = set()
    ts_from = ts_to = None
    th_counts = dict.fromkeys(LATENCY_THRESHOLD_COLUMNS, 0)
    for st in stats_list:
        o = (st.get("ops") or {}).get(op) or {}
        for column in LATENCY_THRESHOLD_COLUMNS:
            th_counts[column] += int(o.get(column, 0) or 0)
        n_ok = int(o.get("ok") or 0)
        ok += int(o.get("keys_ok", n_ok) or 0)
        fail += int(o.get("keys_fail", o.get("fail") or 0) or 0)
        total += int(o.get("keys") or 0)
        ok_rows += n_ok
        lat_sum += float(o.get("avg_ms") or 0.0) * n_ok
        for b, n in (o.get("hist") or {}).items():
            hist[int(b)] = hist.get(int(b), 0) + int(n)
        if o.get("min_ms") is not None:
            gmin = (float(o["min_ms"]) if gmin is None
                    else min(gmin, float(o["min_ms"])))
        if o.get("max_ms") is not None:
            gmax = (float(o["max_ms"]) if gmax is None
                    else max(gmax, float(o["max_ms"])))
        meta = st.get("meta") or {}
        if isinstance(meta.get("hist_version"), int):
            versions.add(meta["hist_version"])
        if meta.get("ts_from") is not None:
            ts_from = meta["ts_from"] if ts_from is None else min(
                ts_from, meta["ts_from"])
        if meta.get("ts_to") is not None:
            ts_to = meta["ts_to"] if ts_to is None else max(
                ts_to, meta["ts_to"])
        for kind, payload in st.get("summaries") or []:
            if kind != "summary":
                continue
            max_depth = max(max_depth, int(payload.get("max_depth", 0) or 0))
            total_bytes += int(payload.get("bytes", 0) or 0)
            wall = max(wall, float(payload.get("wall_s", 0) or 0))
    span = (ts_to - ts_from) if (ts_from is not None and ts_to is not None) else 0.0
    wall = wall or span
    edges = ls.EDGES_BY_VERSION.get(max(versions), ls.BIN_EDGES) if versions \
        else ls.BIN_EDGES
    single_op = None
    if len(stats_list) == 1:
        single_op = (stats_list[0].get("ops") or {}).get(op) or {}

    def pct(q, key):
        # single file with exact per-op percentiles -> use them verbatim
        if single_op and single_op.get("exact") and single_op.get(key) is not None:
            return round(float(single_op[key]), 4)
        if not hist:
            return 0.0
        v = ls.hist_percentile(hist, q, edges)
        return round(min(v, gmax) if gmax is not None else v, 4)

    return {
        "total": total,
        "total_bytes": total_bytes,
        "wall_s": wall,
        "throughput_mb_s": (total_bytes / wall / 1024.0 ** 2)
        if wall > 0 else 0.0,
        "throughput_gb_s": (total_bytes / wall / 1024.0 ** 3)
        if wall > 0 else 0.0,
        "avg_ms": (lat_sum / ok_rows) if ok_rows else 0.0,
        "min_ms": gmin if gmin is not None else 0.0,
        "max_ms": gmax if gmax is not None else 0.0,
        "p50_ms": pct(0.50, "p50_ms"), "p90_ms": pct(0.90, "p90_ms"),
        "p95_ms": pct(0.95, "p95_ms"),
        "p99_ms": pct(0.99, "p99_ms"), "p999_ms": pct(0.999, "p999_ms"),
        "p9999_ms": pct(0.9999, "p9999_ms"),
        **th_counts,
        "lat_sum_ms": lat_sum,
        "ok": ok, "fail": fail,
        # YLT unified semantics: qps = successful requests / wall,
        # fail_rate is a percentage
        "qps": (ok / wall) if wall > 0 else 0.0,
        "fail_rate": (fail * 100.0 / total) if total else 0.0,
        "queue_wait_avg_ms": 0.0,
        "queue_wait_max_ms": 0.0,
        "max_depth": max_depth,
    }


def _merge_stats_aggs(stats_list, op_filter=None):
    """Merge one or more per-instance stats into the agg dict shape that
    parse_benchmark_log returns (CSV TOTAL rows / multi-file aggregates):
    key-based counts sum, wall = timestamp span, percentiles from the merged
    histogram, queue-wait weighted by qw sample count.

    op_filter (mixed basic runs: separate put/get rows) is honoured from the
    per-op aggregates instead of silently returning the combined numbers."""
    ls = _logstats_module()
    if op_filter is not None:
        # true mixed logs (several ops in one file) need per-op aggregation;
        # role-filtered pipeline logs contain only that op, so the combined
        # merge stays authoritative (queue-wait / bytes / max_depth included)
        ops_seen = set()
        for st in stats_list:
            ops_seen.update((st.get("ops") or {}).keys())
        if ops_seen - {op_filter}:
            return _merge_stats_op(stats_list, op_filter, ls)
    # single instance: the stats global fields are already exact (or
    # histogram-derived by the same ladder) — use them verbatim instead of
    # re-deriving from the rounded histogram buckets
    if len(stats_list) == 1:
        st = stats_list[0]
        g = st.get("global") or {}
        meta = st.get("meta") or {}
        max_depth = 0
        total_bytes = 0
        wall = 0.0
        for kind, payload in st.get("summaries") or []:
            if kind != "summary":
                continue
            max_depth = max(max_depth, int(payload.get("max_depth", 0) or 0))
            total_bytes += int(payload.get("bytes", 0) or 0)
            wall = max(wall, float(payload.get("wall_s", 0) or 0))
        total = g.get("key_total") or 0
        ok = g.get("key_ok") or 0
        fail = g.get("key_fail") or 0
        wall = wall or (meta.get("wall_s") or 0.0)
        return {
            "total": total,
            "total_bytes": total_bytes,
            "wall_s": wall,
            "throughput_mb_s": (total_bytes / wall / 1024.0 ** 2)
            if wall > 0 else 0.0,
            "throughput_gb_s": (total_bytes / wall / 1024.0 ** 3)
            if wall > 0 else 0.0,
            "avg_ms": g.get("avg_ms") or 0.0,
            "min_ms": g.get("min_ms") or 0.0,
            "max_ms": g.get("max_ms") or 0.0,
            "p50_ms": g.get("p50_ms") or 0.0,
            "p90_ms": g.get("p90_ms") or 0.0,
            "p95_ms": g.get("p95_ms") or 0.0,
            "p99_ms": g.get("p99_ms") or 0.0,
            "p999_ms": g.get("p999_ms") or 0.0,
            "p9999_ms": g.get("p9999_ms") or 0.0,
            **{column: int(g.get(column, 0) or 0)
               for column in LATENCY_THRESHOLD_COLUMNS},
            "lat_sum_ms": (g.get("avg_ms") or 0.0) * (g.get("ok") or 0),
            "ok": ok,
            "fail": fail,
            "qps": (ok / wall) if wall > 0 else 0.0,
            "fail_rate": (fail * 100.0 / total) if total else 0.0,
            "queue_wait_avg_ms": (g.get("qw_avg_ms")
                                  if g.get("qw_avg_ms") is not None else 0.0),
            "queue_wait_max_ms": (g.get("qw_max_ms")
                                  if g.get("qw_max_ms") is not None else 0.0),
            "max_depth": max_depth,
        }
    total = ok = fail = ok_rows = 0
    lat_sum = 0.0
    qw_sum = 0.0
    qw_n = 0
    qw_max = 0.0
    max_depth = 0
    total_bytes = 0
    ts_from = ts_to = None
    wall_summary = 0.0
    hist = {}
    th_counts = dict.fromkeys(LATENCY_THRESHOLD_COLUMNS, 0)
    for st in stats_list:
        g = st.get("global") or {}
        meta = st.get("meta") or {}
        for column in LATENCY_THRESHOLD_COLUMNS:
            th_counts[column] += int(g.get(column, 0) or 0)
        total += g.get("key_total") or 0
        ok += g.get("key_ok") or 0
        fail += g.get("key_fail") or 0
        rows_ok = g.get("ok") or 0
        ok_rows += rows_ok
        lat_sum += (g.get("avg_ms") or 0.0) * rows_ok
        if g.get("qw_avg_ms") is not None:
            n = g.get("qw_n")
            n = rows_ok if n is None else n
            qw_sum += g["qw_avg_ms"] * n
            qw_n += n
            qw_max = max(qw_max, g.get("qw_max_ms") or 0.0)
        if meta.get("ts_from") is not None:
            ts_from = meta["ts_from"] if ts_from is None else min(
                ts_from, meta["ts_from"])
        if meta.get("ts_to") is not None:
            ts_to = meta["ts_to"] if ts_to is None else max(
                ts_to, meta["ts_to"])
        for b, n in (st.get("histogram") or {}).items():
            hist[int(b)] = hist.get(int(b), 0) + n
        for kind, payload in st.get("summaries") or []:
            if kind != "summary":
                continue
            max_depth = max(max_depth, int(payload.get("max_depth", 0) or 0))
            total_bytes += int(payload.get("bytes", 0) or 0)
            wall_summary = max(wall_summary, float(payload.get("wall_s", 0) or 0))
    span = (ts_to - ts_from) if (ts_from is not None and ts_to is not None) else 0.0
    # single instance: the client SUMMARY wall is authoritative (it excludes
    # connection setup); multi-instance: the common timestamp span
    wall = wall_summary if len(stats_list) == 1 and wall_summary > 0 else span
    n_rows = ok_rows
    hist_versions = {(st.get("meta") or {}).get("hist_version")
                     for st in stats_list}
    hist_edges = ls.EDGES_BY_VERSION.get(
        max(v for v in hist_versions if isinstance(v, int)), ls.BIN_EDGES) \
        if any(isinstance(v, int) for v in hist_versions) else ls.BIN_EDGES
    return {
        "total": total,
        "total_bytes": total_bytes,
        "wall_s": wall,
        "throughput_mb_s": (total_bytes / wall / 1024.0 ** 2) if wall > 0 else 0.0,
        "throughput_gb_s": (total_bytes / wall / 1024.0 ** 3) if wall > 0 else 0.0,
        "avg_ms": (lat_sum / n_rows) if n_rows else 0.0,
        "min_ms": (ls.hist_percentile(hist, 0.0, hist_edges) if hist else 0.0),
        "max_ms": (ls.hist_percentile(hist, 1.0, hist_edges) if hist else 0.0),
        "p50_ms": (ls.hist_percentile(hist, 0.50, hist_edges) if hist else 0.0),
        "p90_ms": (ls.hist_percentile(hist, 0.90, hist_edges) if hist else 0.0),
        "p95_ms": (ls.hist_percentile(hist, 0.95, hist_edges) if hist else 0.0),
        "p99_ms": (ls.hist_percentile(hist, 0.99, hist_edges) if hist else 0.0),
        "p999_ms": (ls.hist_percentile(hist, 0.999, hist_edges) if hist else 0.0),
        "p9999_ms": (ls.hist_percentile(hist, 0.9999, hist_edges) if hist else 0.0),
        **th_counts,
        "lat_sum_ms": lat_sum,
        "ok": ok,
        "fail": fail,
        "qps": (ok / wall) if wall > 0 else 0.0,
        "fail_rate": (fail * 100.0 / total) if total else 0.0,
        "queue_wait_avg_ms": (qw_sum / qw_n) if qw_n else 0.0,
        "queue_wait_max_ms": qw_max,
        "max_depth": max_depth,
    }


def _glob_log_sources(directory, pattern):
    """Log sources matching `pattern`: the raw log and/or its stats sibling
    (stats wins when both exist)."""
    found = {}
    for p in glob.glob(os.path.join(directory, pattern)) + \
            glob.glob(os.path.join(directory, pattern + ".stats.json")):
        key = os.path.basename(p)
        if key.endswith(".stats.json"):
            key = key[:-len(".stats.json")]
        found[key] = p        # stats version added last -> wins
    return [found[k] for k in sorted(found)]
def _kvtest_reduced_for_size(local_log_dir, ds, role):
    """Exact merged percentiles of one role's TOTAL row from a
    controller-reduce reduced.json (report/<ds>/reduced.json); None when
    absent (the caller then leaves the percentile columns empty — never a
    weighted approximation)."""
    try:
        with open(os.path.join(local_log_dir, "report", ds, "reduced.json"),
                  "r", encoding="utf-8") as f:
            doc = json.load(f)
    except (OSError, ValueError):
        return None
    blk = ((doc.get("sizes") or {}).get(ds) or {})
    return ((blk.get("role_summaries") or {}).get(role)) or None


def _load_kvtest_sample_rows(local_log_dir, cfg_name, iid_nodes=None):
    """Load request-level rows keyed by (size, iid, node, op).

    ALL statuses are kept: with warmup folded into the single measured
    window the CSV recomputes the formal totals from the samples themselves
    (the C++ summary counters include the warmup region). Each row carries
    the relative ``elapsed`` seconds (same origin as /start), so the warmup
    cutoff needs no epoch/clock alignment."""
    legacy_re = re.compile(
        r"^{}\.(?P<node>.+)_i(?P<iid>\d+)_p(?P<port>\d+)\.kvtest_"
        r"(?P<ds>.+)\.samples\.csv$".format(re.escape(cfg_name)))
    compact_re = re.compile(
        r"^{}\.i(?P<iid>\d+)_p(?P<port>\d+)\.kvtest_"
        r"(?P<ds>.+)\.samples\.csv$".format(
            re.escape(_kvtest_artifact_prefix(cfg_name))))
    iid_nodes = iid_nodes or {}
    samples = {}
    pattern = os.path.join(local_log_dir, "*.kvtest_*.samples.csv")
    for path in sorted(glob.glob(pattern)):
        match = compact_re.match(os.path.basename(path))
        legacy = False
        if not match:
            match = legacy_re.match(os.path.basename(path))
            legacy = bool(match)
        if not match:
            continue
        iid = int(match.group("iid"))
        node = (match.group("node") if legacy else
                iid_nodes.get(str(iid), "i{}".format(iid)))
        try:
            with open(path, newline="", encoding="utf-8") as stream:
                for row in csv.DictReader(stream):
                    operation = row.get("operation", "")
                    if not operation:
                        continue
                    status = (row.get("status") or "").strip()
                    try:
                        lat = float(row["latency_ms"])
                    except (TypeError, ValueError, KeyError):
                        lat = None
                    try:
                        elapsed = float(row.get("elapsed_us") or 0.0) / 1e6
                    except (TypeError, ValueError):
                        elapsed = 0.0
                    try:
                        nbytes = int(float(row.get("bytes") or 0))
                    except (TypeError, ValueError):
                        nbytes = 0
                    key = (match.group("ds"), iid, node, operation)
                    samples.setdefault(key, []).append({
                        "elapsed": elapsed, "lat": lat,
                        "status": status, "bytes": nbytes})
        except (OSError, ValueError, KeyError, csv.Error) as exc:
            log("[WARN] kvtest samples {}: {} (skipped)".format(
                os.path.basename(path), exc))
    return samples


def write_scenario_csv(local_log_dir, cfg, ts, cfg_path, cluster=None,
                       mooncake_store_nodes=None, result_dir=None):
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
    out_csv = os.path.join(
        result_dir or os.path.join(RESULTS_DIR, ts),
        "{}.csv".format(name))
    rows = []
    is_pipeline = cfg.get("scenario") == "pipeline"
    is_kvtest = cfg.get("scenario") == "kvtest"
    if is_pipeline:
        plan = pipeline_plan(cfg)
        batch_size = int(cfg.get("batch_size", 1))
        duration = float(cfg.get("duration", 0) or 0)

        def base_row(role, instance, nodename, ds):
            if role == "writer":
                op = (cfg.get("writer", {}) or {}).get("operation", "")
            else:
                op = (cfg.get("reader", {}) or {}).get("operation", "")
            # client_count = 本行角色对应的客户端进程总数（dual 模式写/读
            # 两侧都等于 N*count_per_node；分离模式各取各自的规划数）。
            # 合并了旧的 writer_count/reader_count 两列——role 列已区分角色。
            # 优先用运行期记录的真实总数（plan here uses node_count=1，不能
            # 用 len(plan[...]) 代表集群规模）；无运行时退化为 "N*cpn" 描述。
            role_key = "_writer_count" if role == "writer" else "_reader_count"
            if cfg.get(role_key) is not None:
                client_count = int(cfg[role_key])
            else:
                role_cfg = (cfg.get(role, {}) or {}) or (cfg.get("client", {}) or {})
                client_count = "N*{}".format(role_cfg.get("count_per_node", 1))
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
                "notify_async": plan["notify_async"] if role == "writer" else "",
                "notify_max_inflight": plan["notify_max_inflight"] if role == "writer" else "",
                "notify_queue_size": plan["notify_queue_size"] if role == "writer" else "",
                "data_size": ds,
                "batch_size": cfg.get("batch_size", ""),
                # effective per-writer cap: auto-computed value when
                # store_total_size drove the budget, else the config value
                "key_count": (cfg.get("_key_space", {}).get(str(ds))
                              or cfg.get("key_count", "") or ""),
                "duration": cfg.get("duration", ""),
                "warmup_duration": cfg.get("warmup", {}).get("duration", ""),
                "client_count": client_count,
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
            if row["role"] == "writer":
                for k in ("notify_submitted", "notify_ok", "notify_fail",
                          "notify_inflight_peak", "notify_backpressure_ms",
                          "notify_drain_ms", "notify_avg_ms", "notify_max_ms",
                          "notify_queue_peak", "notify_queue_wait_avg_ms",
                          "notify_queue_wait_max_ms", "notify_connections_created",
                          "notify_connections_reused"):
                    # stats-first aggregation (logstats map products) does
                    # not carry client-side notify counters; keep the CSV
                    # columns numeric instead of failing the whole row.
                    row[k] = agg.get(k, 0)
            # bytes/throughput count successful operations only
            row["total_bytes"] = int(ok * bytes_per_op)
            row["wall_s"] = wall
            row["qps"] = (ok / wall) if wall > 0 else 0.0
            if throughput_mb is None:
                # interface throughput = successful bytes / sum of successful
                # interface call durations (busy time, not wall clock)
                busy_s = agg["lat_sum_ms"] / 1000.0
                throughput_mb = (row["total_bytes"] / busy_s / 1024.0 ** 2) if busy_s > 0 else 0.0
            row["throughput_mb_s"] = throughput_mb
            row["throughput_gb_s"] = throughput_mb / 1024.0
            for k in LATENCY_SUMMARY_COLUMNS:
                # older stats products predate p90/threshold columns
                row[k] = agg.get(k, 0)
            if with_queue:
                row["queue_wait_avg_ms"] = agg["queue_wait_avg_ms"]
                row["queue_wait_max_ms"] = agg["queue_wait_max_ms"]
                row["max_depth"] = agg["max_depth"]
            for k in ("avg_ms", "min_ms", "max_ms", "p50_ms", "p90_ms",
                      "p95_ms", "p99_ms", "p999_ms", "p9999_ms",
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
            for p in _glob_log_sources(
                    local_log_dir, "{}.{}.writer_*.log".format(name, run_name)):
                base = os.path.basename(p)
                if base.endswith(".stats.json"):
                    base = base[:-len(".stats.json")]
                if ".ylt." in base:
                    continue
                m = re.search(r"\.writer_(.+)_w(\d+)\.log$", base)
                if not m:
                    continue
                writer_files[int(m.group(2))] = (m.group(1), p)
            reader_files = {}   # index -> (nodename, path)
            for p in _glob_log_sources(
                    local_log_dir, "{}.{}.reader_*.log".format(name, run_name)):
                base = os.path.basename(p)
                if base.endswith(".stats.json"):
                    base = base[:-len(".stats.json")]
                if ".ylt." in base:
                    continue
                m = re.search(r"\.reader_(.+)_r(\d+)_p(\d+)\.log$", base)
                if not m:
                    continue
                reader_files[int(m.group(2))] = (m.group(1), p)
            # dual-role logs: writer and reader output are SEPARATE files
            # (*.writer.log / *.reader.log); legacy mixed logs (single
            # *.log) still parse — role split by op filter
            dual_writer_files = {}   # id -> (nodename, path)
            dual_reader_files = {}   # id -> (nodename, path)
            for p in _glob_log_sources(
                    local_log_dir, "{}.{}.dual_*.log".format(name, run_name)):
                base = os.path.basename(p)
                if base.endswith(".stats.json"):
                    base = base[:-len(".stats.json")]
                if ".ylt." in base:
                    continue
                m = re.search(r"\.dual_(.+)_c(\d+)_p(\d+)\.(writer|reader)\.log$",
                              base)
                if m:
                    target = (dual_reader_files if m.group(4) == "reader"
                              else dual_writer_files)
                    target[int(m.group(2))] = (m.group(1), p)
                    continue
                m = re.search(r"\.dual_(.+)_c(\d+)_p(\d+)\.log$", base)
                if m:
                    dual_writer_files[int(m.group(2))] = (m.group(1), p)
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
            # dual-role rows: writer metrics from the writer log, reader
            # metrics from the reader log (legacy mixed logs fall back)
            for cid in sorted(dual_writer_files):
                nodename, f = dual_writer_files[cid]
                agg = parse_benchmark_log(f, op_filter="pipeline_write")
                row = base_row("writer", "c{}".format(cid), nodename, ds)
                metrics_into(row, agg, ds_bytes)
                rows.append(row)
            for cid in sorted(dual_reader_files or dual_writer_files):
                nodename, f = (dual_reader_files.get(cid)
                               or dual_writer_files[cid])
                agg = parse_benchmark_log(f, op_filter="pipeline_read")
                row = base_row("reader", "c{}".format(cid), nodename, ds)
                metrics_into(row, agg, ds_bytes, with_queue=True)
                rows.append(row)
            if dual_writer_files:
                agg = parse_benchmark_log(
                    [f for _n, f in dual_writer_files.values()],
                    op_filter="pipeline_write")
                row = base_row("writer", "TOTAL", "", ds)
                metrics_into(row, agg, ds_bytes,
                             throughput_mb=busy_throughput_mb(
                                 (f for _n, f in dual_writer_files.values()),
                                 "pipeline_write", ds_bytes))
                rows.append(row)
                reader_src = dual_reader_files or dual_writer_files
                agg = parse_benchmark_log(
                    [f for _n, f in reader_src.values()],
                    op_filter="pipeline_read")
                row = base_row("reader", "TOTAL", "", ds)
                metrics_into(row, agg, ds_bytes, with_queue=True,
                             throughput_mb=busy_throughput_mb(
                                 (f for _n, f in reader_src.values()),
                                 "pipeline_read", ds_bytes))
                rows.append(row)
    elif is_kvtest:
        # kvtest rows come from compact per-instance summary.json artifacts;
        # the runtime iid->node map restores node/IP/topology metadata.
        # Throughput/qps use each C++ instance's measured elapsed_seconds,
        # starting at its successful /start metric reset.
        plan = kvtest_plan(cfg)
        duration = float(cfg.get("duration", 0) or 0)
        writer_op = KVTEST_WRITER_OPS[_kvtest_writer_operation(cfg)]
        reader_op = KVTEST_READER_OPS[_kvtest_reader_operation(cfg)]
        lat_keys = tuple(LATENCY_SUMMARY_COLUMNS)

        def base_row(role, instance, nodename, ds):
            op = writer_op if role == "writer" else reader_op
            role_key = "_writer_count" if role == "writer" else "_reader_count"
            if cfg.get(role_key) is not None:
                client_count = int(cfg[role_key])
            else:
                role_cfg = (cfg.get(role, {}) or {}) or (cfg.get("client", {}) or {})
                client_count = "N*{}".format(role_cfg.get("count_per_node", 1))
            try:
                node_ip = get_node(cluster, nodename)["ip"] if cluster else nodename
            except (KeyError, RuntimeError):
                node_ip = nodename
            runtime_pools = cfg.get("_kvtest_key_pool", {}) or {}
            if str(ds) in runtime_pools:
                effective_pool = runtime_pools[str(ds)]
            else:
                effective_pool = (cfg.get("kvtest", {}) or {}).get(
                    "key_pool_size", "")
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
                "num_threads": plan["writer_threads"] if role == "writer"
                               else plan["reader_threads"],
                "notify_threads": plan["notify_threads"] if role == "writer" else "",
                "data_size": ds,
                # effective pool: auto value recorded at run time, else the
                # explicit kvtest.key_pool_size config value
                "key_pool_size": effective_pool,
                "batch_size": cfg.get("batch_size", ""),
                "duration": cfg.get("duration", ""),
                "warmup_duration": cfg.get(
                    "_kvtest_warmup_duration",
                    (cfg.get("warmup", {}) or {}).get("duration", 0)),
                "client_count": client_count,
                "notify_count": plan["notify_count"],
                "store_count": ("N*{}".format(cfg.get("store_count_per_node"))
                                if cfg.get("store_count_per_node") else ""),
                "base_qps": cfg.get("base_qps", ""),
                "qps_multiple": cfg.get("qps_multiple", ""),
                "target_qps": plan["total_qps"],
            }

        def op_stats(summary, op):
            return (summary.get("operations", {}) or {}).get(op) or {}

        def fill_metrics(row, st, wall, values=None):
            total = int(st.get("total", 0) or 0)
            ok = int(st.get("success", 0) or 0)
            fail = int(st.get("failure", 0) or 0)
            row["total"] = total
            row["success"] = ok
            row["fail"] = fail
            row["fail_rate"] = (fail * 100.0 / total) if total else 0.0
            row["total_bytes"] = int(st.get("bytes", 0) or 0)
            row["wall_s"] = wall
            row["qps"] = (ok / wall) if wall > 0 else 0.0
            row["throughput_mb_s"] = (row["total_bytes"] / wall / 1024.0 ** 2
                                      if wall > 0 else 0.0)
            row["throughput_gb_s"] = row["throughput_mb_s"] / 1024.0
            stats = (_kvtest_latency_stats(values) if values is not None
                     else st)
            for k in lat_keys:
                row[k] = round(float(stats.get(k, 0) or 0), 4)
            for k in ("qps", "fail_rate", "throughput_mb_s",
                      "throughput_gb_s"):
                row[k] = round(row[k], 2)

        def fill_notify(row, summary, values=None):
            operation = ("notify_rpc"
                         if "notify_rpc" in (summary.get("operations") or {})
                         else "notify_http")
            nt = op_stats(summary, operation)
            row["notify_submitted"] = int(nt.get("total", 0) or 0)
            row["notify_ok"] = int(nt.get("success", 0) or 0)
            row["notify_fail"] = int(nt.get("failure", 0) or 0)
            stats = (_kvtest_latency_stats(values) if values is not None
                     else nt)
            row["notify_avg_ms"] = round(
                float(stats.get("avg_ms", 0) or 0), 4)
            row["notify_max_ms"] = round(
                float(stats.get("max_ms", 0) or 0), 4)

        def fill_queue(row, summary, values=None):
            q = op_stats(summary, "notify_queue_wait")
            stats = (_kvtest_latency_stats(values) if values is not None
                     else q)
            row["queue_wait_avg_ms"] = round(
                float(stats.get("avg_ms", 0) or 0), 4)
            row["queue_wait_max_ms"] = round(
                float(stats.get("max_ms", 0) or 0), 4)

        def weighted_of(inst_rows, value_key, weight_key="total"):
            weighted = [(float(r.get(value_key, 0) or 0),
                         int(r.get(weight_key, 0) or 0))
                        for r in inst_rows]
            weight = sum(w for _v, w in weighted)
            return round(sum(v * w for v, w in weighted) / weight, 4) \
                if weight else 0.0

        def total_row(role, inst_rows, ds, wall, pooled_values=None):
            # Counts/bytes sum exactly. With complete request samples the
            # latency distribution is recomputed over the merged OK samples.
            row = base_row(role, "TOTAL", "", ds)
            total = sum(r["total"] for r in inst_rows)
            fail = sum(r["fail"] for r in inst_rows)
            row["total"] = total
            row["success"] = sum(r["success"] for r in inst_rows)
            row["fail"] = fail
            row["fail_rate"] = (fail * 100.0 / total) if total else 0.0
            row["total_bytes"] = sum(r["total_bytes"] for r in inst_rows)
            row["wall_s"] = max((float(r.get("wall_s", 0) or 0)
                                 for r in inst_rows), default=wall)
            row["qps"] = sum(
                (float(r.get("success", 0) or 0) /
                 float(r.get("wall_s", 0) or 1))
                if float(r.get("wall_s", 0) or 0) > 0 else 0.0
                for r in inst_rows)
            row["throughput_mb_s"] = sum(
                float(r.get("throughput_mb_s", 0) or 0) for r in inst_rows)
            row["throughput_gb_s"] = sum(
                float(r.get("throughput_gb_s", 0) or 0) for r in inst_rows)
            if pooled_values is not None:
                stats = _kvtest_latency_stats(pooled_values)
                for k in lat_keys:
                    row[k] = round(stats[k], 4)
            else:
                # 无原始样本时绝不使用加权近似：优先 controller reduce 的
                # 精确合并值（report/<ds>/reduced.json），否则分位列留空（N/A）。
                # avg/min/max/阈值计数仍然精确（极值/求和天然无损）。
                _red = _kvtest_reduced_for_size(local_log_dir, ds, role)
                for k in lat_keys:
                    if k == "min_ms":
                        row[k] = min((float(r.get(k, 0) or 0)
                                      for r in inst_rows
                                      if int(r.get("success", 0) or 0) > 0),
                                     default=0.0)
                    elif k == "max_ms":
                        row[k] = max((float(r.get(k, 0) or 0)
                                      for r in inst_rows), default=0.0)
                    elif k in LATENCY_THRESHOLD_COLUMNS:
                        row[k] = sum(int(r.get(k, 0) or 0)
                                     for r in inst_rows)
                    elif k == "avg_ms":
                        # 加权均值在数学上等于全样本均值（精确，非近似）
                        row[k] = weighted_of(
                            inst_rows, k, weight_key="success")
                    elif _red is not None and _red.get(k) is not None:
                        row[k] = round(float(_red[k]), 4)
                    else:
                        row[k] = ""
                        log("[WARN] kvtest TOTAL quantile unavailable (no raw "
                            "samples and no controller reduce product); left "
                            "empty: {} {} {}".format(ds, role, k))
            for k in ("qps", "fail_rate", "throughput_mb_s",
                      "throughput_gb_s"):
                row[k] = round(row[k], 2)
            return row

        # one block of rows per data_size: writer rows, writer TOTAL,
        # reader rows, reader TOTAL (dual instances emit both sides)
        legacy_summary_re = re.compile(
            r"^{}\.(?P<node>.+)_i(?P<iid>\d+)_p(?P<port>\d+)\.kvtest_"
            r"(?P<ds>.+)\.summary\.json$".format(re.escape(name)))
        compact_summary_re = re.compile(
            r"^{}\.i(?P<iid>\d+)_p(?P<port>\d+)\.kvtest_"
            r"(?P<ds>.+)\.summary\.json$".format(
                re.escape(_kvtest_artifact_prefix(name))))
        iid_nodes = cfg.get("_kvtest_iid_nodes", {}) or {}
        summaries = {}   # ds -> [(iid, node, summary_dict)]
        for p in sorted(glob.glob(os.path.join(
                local_log_dir, "*.kvtest_*.summary.json"))):
            m = compact_summary_re.match(os.path.basename(p))
            legacy = False
            if not m:
                m = legacy_summary_re.match(os.path.basename(p))
                legacy = bool(m)
            if not m:
                continue
            iid = int(m.group("iid"))
            nodename = (m.group("node") if legacy else
                        iid_nodes.get(str(iid), "i{}".format(iid)))
            try:
                with open(p, encoding="utf-8") as f:
                    summary = json.load(f)
            except (OSError, ValueError) as exc:
                log("[WARN] kvtest summary {}: {} (skipped)".format(
                    os.path.basename(p), exc))
                continue
            summaries.setdefault(m.group("ds"), []).append(
                (iid, nodename, summary))
        sample_rows = _load_kvtest_sample_rows(
            local_log_dir, name, iid_nodes=iid_nodes)
        # warmup 已并入单窗口：前 warm_sec 秒的行不计入正式统计（但样本里
        # 仍有它们，图表照常绘制）。elapsed_us 与 /start 同源，用相对时长
        # 过滤即可。活跑用 cfg 的 _kvtest_warmup_duration；--rebuild-report
        # 从 meta 的 warmup_s 取；旧产物无字段 -> 0（旧样本本身就不含 warmup）。
        warm_sec = float(
            cfg.get("_kvtest_warmup_duration")
            if cfg.get("_kvtest_warmup_duration") is not None
            else (cfg.get("warmup_s") or 0))

        def sample_rows_of(ds, iid, nodename, operation, expected):
            """样本完整 -> 返回 formal 行（切掉 warmup 区）；否则 None 走回退。"""
            rows_ = sample_rows.get((ds, iid, nodename, operation), [])
            ok_all = sum(1 for r in rows_
                         if r["status"] == "OK" and r["lat"] is not None)
            if ok_all != expected:
                log("[WARN] kvtest {} i{} {} samples incomplete: {} OK rows, "
                    "summary success={}; using summary fallback".format(
                        ds, iid, operation, ok_all, expected))
                return None
            if warm_sec > 0:
                return [r for r in rows_ if r["elapsed"] >= warm_sec]
            return list(rows_)

        def rows_stats(rows_):
            """formal 行 -> fill_metrics/fill_notify/fill_queue 需要的 stats。"""
            ok_rows = [r for r in (rows_ or [])
                       if r["status"] == "OK" and r["lat"] is not None]
            return {"total": len(rows_ or []),
                    "success": len(ok_rows),
                    "failure": len(rows_ or []) - len(ok_rows),
                    "bytes": sum(r["bytes"] for r in ok_rows)}

        def rows_values(rows_):
            return [r["lat"] for r in (rows_ or [])
                    if r["status"] == "OK" and r["lat"] is not None]

        for ds in data_size_list(cfg):
            wall = duration
            writer_rows, reader_rows = [], []
            writer_pooled, reader_pooled = [], []
            writer_exact = reader_exact = True
            for iid, nodename, summary in sorted(summaries.get(ds, [])):
                ops = summary.get("operations") or {}
                elapsed = float(summary.get("elapsed_seconds", 0) or duration)
                # C++ elapsed_seconds 是单窗口（含 warmup）；正式时长扣掉
                # warmup 区，qps/吞吐口径才正确
                instance_wall = (max(0.0, elapsed - warm_sec)
                                 if warm_sec > 0 else elapsed)
                if writer_op in ops:
                    writer_st = op_stats(summary, writer_op)
                    wrows = sample_rows_of(
                        ds, iid, nodename, writer_op,
                        int(writer_st.get("success", 0) or 0))
                    writer_values = (rows_values(wrows)
                                     if wrows is not None else None)
                    row = base_row("writer", "i{}".format(iid), nodename, ds)
                    fill_metrics(
                        row, rows_stats(wrows) if wrows is not None
                        else writer_st, instance_wall, writer_values)
                    notify_operation = (
                        "notify_rpc" if "notify_rpc" in ops
                        else "notify_http")
                    notify_st = op_stats(summary, notify_operation)
                    nrows = sample_rows_of(
                        ds, iid, nodename, notify_operation,
                        int(notify_st.get("success", 0) or 0))
                    notify_values = (rows_values(nrows)
                                     if nrows is not None else None)
                    fill_notify(
                        row,
                        ({"operations": {notify_operation: rows_stats(nrows)}}
                         if nrows is not None else summary),
                        notify_values)
                    writer_rows.append(row)
                    if writer_values is None:
                        writer_exact = False
                    else:
                        writer_pooled.extend(writer_values)
                if reader_op in ops:
                    reader_st = op_stats(summary, reader_op)
                    rrows = sample_rows_of(
                        ds, iid, nodename, reader_op,
                        int(reader_st.get("success", 0) or 0))
                    reader_values = (rows_values(rrows)
                                     if rrows is not None else None)
                    row = base_row("reader", "i{}".format(iid), nodename, ds)
                    fill_metrics(
                        row, rows_stats(rrows) if rrows is not None
                        else reader_st, instance_wall, reader_values)
                    queue_st = op_stats(summary, "notify_queue_wait")
                    qrows = sample_rows_of(
                        ds, iid, nodename, "notify_queue_wait",
                        int(queue_st.get("success", 0) or 0))
                    queue_values = (rows_values(qrows)
                                    if qrows is not None else None)
                    fill_queue(
                        row,
                        ({"operations": {"notify_queue_wait": rows_stats(qrows)}}
                         if qrows is not None else summary),
                        queue_values)
                    # data verification runs on the read path (C++ scalar;
                    # cannot be split by time -> keeps the warmup region)
                    row["verify_failures"] = int(
                        summary.get("verify_failures", 0) or 0)
                    reader_rows.append(row)
                    if reader_values is None:
                        reader_exact = False
                    else:
                        reader_pooled.extend(reader_values)
            # grouped like the pipeline CSV: all writers + TOTAL, then all
            # readers + TOTAL
            rows.extend(writer_rows)
            if writer_rows:
                row = total_row(
                    "writer", writer_rows, ds, wall,
                    writer_pooled if writer_exact else None)
                for k in ("notify_submitted", "notify_ok", "notify_fail"):
                    row[k] = sum(r.get(k, 0) for r in writer_rows)
                row["notify_avg_ms"] = weighted_of(
                    writer_rows, "notify_avg_ms", "notify_ok")
                row["notify_max_ms"] = max(
                    (r.get("notify_max_ms", 0) or 0 for r in writer_rows),
                    default=0)
                rows.append(row)
            rows.extend(reader_rows)
            if reader_rows:
                row = total_row(
                    "reader", reader_rows, ds, wall,
                    reader_pooled if reader_exact else None)
                row["verify_failures"] = sum(
                    r.get("verify_failures", 0) for r in reader_rows)
                row["queue_wait_avg_ms"] = weighted_of(
                    reader_rows, "queue_wait_avg_ms", "success")
                row["queue_wait_max_ms"] = max(
                    (r.get("queue_wait_max_ms", 0) or 0 for r in reader_rows),
                    default=0)
                rows.append(row)
    else:
        def fill_basic_row(row, agg, op_label):
            row["operation"] = op_label
            row.update({k: agg[k] for k in (
                "total", "total_bytes", "wall_s", "throughput_mb_s", "throughput_gb_s",
                "ok", "fail", "qps", "fail_rate")})
            for key in LATENCY_SUMMARY_COLUMNS:
                row[key] = agg.get(key, 0)
            # throughput recomputed from this row's own bytes/wall so split
            # (mixed) rows never inherit combined SUMMARY bytes
            row["throughput_mb_s"] = (row["total_bytes"] / row["wall_s"] / 1024.0 ** 2) if row["wall_s"] > 0 else 0.0
            row["throughput_gb_s"] = (row["total_bytes"] / row["wall_s"] / 1024.0 ** 3) if row["wall_s"] > 0 else 0.0
            for k in ("avg_ms", "min_ms", "max_ms", "p50_ms", "p90_ms",
                      "p95_ms", "p99_ms", "p999_ms", "p9999_ms", "wall_s"):
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
            if not os.path.exists(path) and not os.path.exists(
                    path + ".stats.json"):
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
    if is_pipeline or is_kvtest:
        write_rows = collapse_config_name(rows)
        header = PIPELINE_CSV_HEADER if is_pipeline else KVTEST_CSV_HEADER
        with open(out_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header)
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


def load_scenario_config(cfg_path, configs_root=None):
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
    root = configs_root or CONFIG_INHERIT_ROOT
    merged = {}
    for category in ("basic", "pipeline", "kvtest"):
        category_dir = os.path.normpath(os.path.join(root, category))
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
    # pipeline has no key_count: the writable key space is derived from the
    # store capacity; a leftover field is ignored (basic keeps key_count)
    if merged.get("scenario") == "pipeline" and "key_count" in merged:
        log("[WARN] pipeline key_count is removed and ignored — the write "
            "key space is derived from the store capacity "
            "(store_total_size / master metrics); remove the field")
        merged.pop("key_count", None)
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


def _copy_audit_snapshot(result_dir):
    """把实例配置快照复制到本次 run 的产物目录 snapshot/（审计/复现用）。"""
    src = os.environ.get("MC_INSTANCE_SNAPSHOT")
    if not src or not os.path.isdir(src):
        return
    dest = os.path.join(result_dir, "snapshot")
    if os.path.isdir(dest):
        return
    try:
        shutil.copytree(src, dest)
        log("[snapshot] 配置快照已归档到 {}".format(
            os.path.relpath(dest, ROOT)))
    except Exception as exc:
        log("[WARN] 配置快照归档失败: {}".format(exc))


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
                             "per config; pipeline/kvtest: role Deployments "
                             "spread across all Ready nodes (kvtest split mode "
                             "runs one writer + one reader per client pod)")
    parser.add_argument("--cluster", default=None,
                        help="path to cluster json (default: configs/_ssh/cluster.json "
                             "or configs/_k8s/cluster.json per mode)")
    parser.add_argument("--global", dest="global_cfg", default=None,
                        help="path to global hyper json (default: configs/_ssh/global.json "
                             "or configs/_k8s/global.json per mode)")
    parser.add_argument("--monitor-mem", choices=("on", "off"), default=None,
                        help="override profile.json mem monitor (on|off); "
                             "the WebUI always sends it (default off)")
    parser.add_argument("--monitor-urma", choices=("on", "off"), default=None,
                        help="override profile.json URMA-port monitor "
                             "(on|off); the WebUI always sends it (default off)")
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
    # Internal orchestration hook used by the WebUI rank+recall launcher.
    # Keeping it out of --help avoids presenting an implementation detail as
    # a normal benchmark knob.
    parser.add_argument("--result-dir", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--parallel-ready-dir", default=None,
                        help=argparse.SUPPRESS)
    parser.add_argument("--parallel-ready-role", choices=("rank", "recall"),
                        default=None, help=argparse.SUPPRESS)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="verbose logging (upload paths, per-file pulls, "
                             "kubectl commands, pod details)")
    parser.add_argument("--stop-file", default=None,
                        help="cooperative stop flag file (WebUI stop); a "
                             "SIGINT/SIGTERM/SIGBREAK also requests stop")
    parser.add_argument("--no-warmup", action="store_true")
    parser.add_argument("--no-report", action="store_true",
                        help="skip the HTML reports of this run (per-unit "
                             "report.html + the run-level overview.html); "
                             "independent of plotting")
    parser.add_argument("--png", action="store_true",
                        help="generate the legacy PNG plots (plot_latency.py); "
                             "plotting is off by default and --png requires "
                             "--pull-raw-logs (the plots read raw client logs)")
    parser.add_argument("--pull-raw-logs", action="store_true",
                        help="pull the raw client logs back (default: only the "
                             "compact *.stats.json produced by remote logstats)")
    parser.add_argument("--keep-intermediates", action="store_true",
                        help="keep the pulled/mapped intermediates (*.stats.json,"
                             " raw *.log/*.glog, kvtest samples/summary) after "
                             "a successful report; default: remove them "
                             "(report/ and profiling/ are always kept). "
                             "env: MC_KEEP_INTERMEDIATES=1")
    parser.add_argument("--rebuild-report", default=None, metavar="TS",
                        help="rebuild the reports for an existing LOCAL run "
                             "from its timestamp directory name (e.g. "
                             "20261110-1308, NOT outputs/<ts>): merges the "
                             "local stats and/or raw logs and re-renders "
                             "report.html + overview.html (remote map is NOT "
                             "re-run; pull raw logs first if stats are absent)")
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
    if args.png and not args.pull_raw_logs:
        parser.error("开启 png 必须开启 pull-raw-logs（PNG 绘图需要原始客户端"
                     "日志）")
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
    if not cfg_paths and not args.clean_up and not args.rebuild_report:
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


def rebuild_reports(ts):
    """--rebuild-report: regenerate the reports for an existing run purely
    from LOCAL artifacts (pulled stats and/or raw logs). Covers the
    "run finished but report generation failed" case without touching the
    remote side. Returns the process exit code."""
    ts_dir = os.path.join(RESULTS_DIR, ts)
    logs_root = os.path.join(ts_dir, "logs")
    if not os.path.isdir(logs_root):
        log("[ERROR] no logs directory under {}".format(
            os.path.relpath(ts_dir, ROOT)))
        return 2
    rp = _report_module()
    echarts = os.path.join(ROOT, "webui", "web", "vendor", "echarts.min.js")
    scenarios = []
    for name in sorted(os.listdir(logs_root)):
        d = os.path.join(logs_root, name)
        if not os.path.isdir(d) or name == "report":
            continue
        stats = _collect_local_stats(d)
        _samples = glob.glob(os.path.join(d, "*.kvtest_*.samples.csv"))
        if not stats and not _samples:
            rep = os.path.join(d, "report")
            has_report = os.path.isdir(rep) and any(
                os.path.isfile(os.path.join(rep, x, "report.html"))
                for x in os.listdir(rep)
                if os.path.isdir(os.path.join(rep, x)))
            if has_report:
                log("[ERROR] {}: intermediates were cleaned after success "
                    "(only report/ and profiling/ kept); rebuild unavailable. "
                    "Reports and data.js remain usable. Use "
                    "--keep-intermediates (or MC_KEEP_INTERMEDIATES=1) when "
                    "running".format(name))
                continue
        _have_kvtest_stats = any(
            os.path.basename(p).endswith(".stats.json") and
            ".samples.csv." in os.path.basename(p) for p in stats)
        if _samples and not _have_kvtest_stats:
            # failed kvtest run: rebuild the map products from the request
            # samples (the run config is gone; the sidecar supplies the node
            # mapping + /start epochs, the summary mtime is the last resort)
            try:
                _kvtest_map_stats(d, {"_kvtest_start_epochs": {}})
            except Exception as exc:
                log("[WARN] {}: kvtest map rebuild failed: {}".format(
                    name, exc))
            stats = _collect_local_stats(d)
        if not stats:
            log("[WARN] {}: no stats/raw logs locally; skipped".format(name))
            continue
        probe = rp.load(stats[:1])
        roles = {(st.get("meta") or {}).get("role") for st in probe}
        if any((st.get("meta") or {}).get("format") == "kvtest_samples"
               for st in probe):
            mode = "kvtest"
        else:
            mode = "pipeline" if roles & {"writer", "reader"} else "basic"
        traces_all = _collect_local_traces(d)
        # legacy merged meta (pre-unit layout) is still honoured as fallback
        saved = {}
        try:
            with open(os.path.join(d, "report", "meta.json"),
                      "r", encoding="utf-8") as f:
                saved = json.load(f) or {}
        except (OSError, ValueError):
            saved = {}
        if saved.get("mode"):
            mode = saved["mode"]
        # 归一化旧产物：mode="kvtest" 曾把引擎写进形态槽位
        _engine = saved.get("engine") or (
            "kvtest" if mode == "kvtest" else "python")
        mode = (saved.get("scenario")
                or (saved.get("mode") if saved.get("mode") in REPORT_SCENARIOS
                    else None)
                or ("pipeline" if _engine == "kvtest" else
                    (mode if mode in REPORT_SCENARIOS else "basic")))
        units = _unit_groups(stats, mode)
        traces_by_unit = {}
        for tp in traces_all:
            key, _n, _l, _c, _t, _s = _unit_of(tp, mode)
            traces_by_unit.setdefault(key, []).append(tp)
        for key, u in units.items():
            loaded = rp.load(u["paths"])
            if not loaded:
                continue
            t0, t1 = _stats_window(loaded)
            # 单元级 meta.json（新布局）优先，旧合并 meta 兜底
            usaved = {}
            try:
                with open(os.path.join(d, "report", u["name"], "meta.json"),
                          "r", encoding="utf-8") as f:
                    usaved = json.load(f) or {}
            except (OSError, ValueError):
                usaved = {}
            base = usaved or saved
            # 恢复 run 时参数，但把「客户端总数 / 节点数」按本次实际加载的
            # stats 重算（历史 run 的旧 params 可能含已修复的幽灵客户端）
            n_nodes = len({(st.get("meta") or {}).get("node")
                           for st in loaded
                           if (st.get("meta") or {}).get("node")})
            n_clients = (u["c"] if (mode == "basic" and u["c"])
                         else len(loaded))
            params = []
            for pr in (base.get("params") or []):
                if pr and pr[0] == "开始时间":
                    continue          # 开始时间已在页脚副标题给出
                if pr and pr[0] == "客户端总数":
                    params.append(["客户端总数", n_clients])
                elif pr and pr[0] == "节点数" and n_nodes:
                    params.append(["节点数", n_nodes])
                else:
                    params.append(pr)
            if params:   # 派生字段缺失时补上（历史 params 可能没有）
                keys = {p[0] for p in params if p}
                if "客户端总数" not in keys:
                    params.append(["客户端总数", n_clients])
                if n_nodes and "节点数" not in keys:
                    params.append(["节点数", n_nodes])
            if not params and _engine == "kvtest":
                # no scenario meta.json (failed/best-effort run): rebuild the
                # params card from what the map products know
                # prefer the C++ summary wall over the sample span (the
                # span can be shorter when the last requests land early)
                wall = max(
                    [float((p or {}).get("wall_s") or 0)
                     for st in loaded
                     for kind, p in (st.get("summaries") or [])
                     if kind == "summary"]
                    + [float((st.get("meta") or {}).get("wall_s") or 0)
                       for st in loaded] + [0.0])
                params = _build_report_params(
                    {"scenario": "kvtest", "data_size": [u["size"]],
                     "duration": int(round(wall)) if wall > 0 else None},
                    {"unit_size": u["size"], "unit_clients": None,
                     "unit_threads": None, "clients": n_clients,
                     "scenario": mode, "engine": _engine},
                    nodes=n_nodes)
            # 单元只显示自己的 size / combo
            params = _override_param(params, "数据大小", u["size"])
            if mode == "basic":
                params = _override_param(params, "客户端数", u["c"])
                params = _override_param(params, "线程数", u["t"])
            n_clients = (u["c"] if (mode == "basic" and u["c"])
                         else len(loaded))
            # P1-B：重建同样以数据最早采样时刻作为「测试开始于」
            _started = (time.strftime("%Y-%m-%d %H:%M:%S",
                                      time.localtime(t0))
                        if t0 is not None else
                        (base.get("started")
                         or time.strftime("%Y-%m-%d %H:%M:%S")))
            meta = {
                "config": name, "mode": mode,
                "scenario": mode, "engine": _engine,
                "launch_mode": base.get("launch_mode") or "",
                "clients": n_clients,
                "qps_target": base.get("qps_target") or 0,
                "protocol": base.get("protocol") or "",
                "started": _started,
                "early_stop": False,      # unknown for historical runs
                "cluster_cfg": base.get("cluster_cfg") or "",
                "global_cfg": base.get("global_cfg") or "",
                "profile_cfg": base.get("profile_cfg") or "",
                "unit": u["label"],
                "unit_size": u["size"],
                "unit_clients": u["c"],
                "unit_threads": u["t"],
                "params": params,
            }
            site_sizes = None
            try:
                with open(os.path.join(d, "report", "site_sizes.json"),
                          "r", encoding="utf-8") as f:
                    site_sizes = json.load(f) or None
            except (OSError, ValueError):
                site_sizes = None
            data = rp.reduce_scenario(
                loaded, meta,
                monitors=rp.reduce_monitors(d, t0=t0, t1=t1),
                traces=rp.load(traces_by_unit.get(key, [])),
                monitor_sites=rp.reduce_monitor_sites(d),
                site_sizes=site_sizes)
            out_d = os.path.join(d, "report", u["name"])
            # 回写修正后的 meta.json：后续重建/打开读到一致的参数
            try:
                os.makedirs(out_d, exist_ok=True)
                with open(os.path.join(out_d, "meta.json"), "w",
                          encoding="utf-8") as f:
                    json.dump(meta, f, ensure_ascii=False, indent=1)
            except OSError:
                pass
            html, _dj = rp.render_scenario(
                data, out_d, echarts,
                rel_base=os.path.relpath(out_d, ROOT).replace(os.sep, "/"))
            log("[report] rebuilt {} · {} -> {}".format(
                name, u["label"], os.path.relpath(html, ROOT)))
            scenarios.append(dict(
                data, unit=u["label"],
                report_rel=os.path.relpath(
                    html, os.path.join(ts_dir, "report")
                ).replace(os.sep, "/")))

    if not scenarios:
        log("[ERROR] rebuild: no local stats or raw logs found under {} — "
            "re-run with --pull-raw-logs, use --rebuild-report on the "
            "originating machine, or keep the intermediates next time with "
            "--keep-intermediates".format(os.path.relpath(logs_root, ROOT)))
        return 1
    by_node = {}
    for sc in scenarios:
        for n in sc.get("nodes") or []:
            e = by_node.setdefault(n.get("node"), {"node": n.get("node"),
                                                   "pods": 0, "bytes": 0})
            e["pods"] += n.get("pods") or 0
            e["bytes"] += n.get("bytes") or 0
    ov_d = os.path.join(ts_dir, "report")
    ov = rp.render_overview({
        "run": ts, "mode": "", "started": time.strftime("%Y-%m-%d %H:%M:%S"),
        "nodes": sorted(by_node.values(), key=lambda x: -(x.get("bytes") or 0)),
    }, scenarios, ov_d, rel_base=os.path.relpath(
        ov_d, ROOT).replace(os.sep, "/"))
    log("[report] overview -> {}".format(os.path.relpath(ov, ROOT)))
    return 0


def _post_process_config(cfg, cfg_name, scenario, ts, ts_dir,
                         cluster_snapshot, store_nodes, report_kwargs, args):
    """Local per-config post-processing: CSV aggregation -> HTML report map/
    reduce -> profiling outliers.

    Runs on the background worker (see _bg_submit) while the orchestrator
    tears the finished config down and deploys the next one. It only touches
    already-pulled LOCAL artifacts plus a cluster SNAPSHOT (the live
    cluster["nodes"] is still appended to by the next k8s scenario).
    Returns (category, rows) for the run-level detail CSV; raising here marks
    only this config failed — the report wrappers themselves stay WARN-only,
    exactly like the old inline path."""
    log_dir = os.path.join(ts_dir, "logs", cfg_name)
    rows = write_scenario_csv(log_dir, cfg, ts,
                              cfg.get("__cfg_path__") or "",
                              cluster_snapshot, store_nodes, result_dir=ts_dir)
    if scenario == "kvtest":
        # map (samples -> per-role stats.json) + reduce, same chain as
        # pipeline/basic; no extra kwargs (derived inside from cfg)
        _kvtest_generate_reports(cfg, log_dir, args)
    else:
        _generate_reports(cfg, log_dir, args, **report_kwargs)
    matrix = getattr(args, "_matrix", None)
    if matrix and matrix.get("outliers", {}).get("auto") and prof_core:
        # run-level outlier ledger: safe because the single background worker
        # serializes every config's post-processing (no read-modify-write race)
        try:
            _profile_outliers(matrix, ts_dir, cfg_name)
        except Exception as exc:
            log("[WARN] [profile] outlier extraction: {}".format(exc))
    return scenario, rows


def main(args, cfg_paths):
    if getattr(args, "rebuild_report", None):
        sys.exit(rebuild_reports(args.rebuild_report))
    stop_file = args.stop_file or os.environ.get("BENCH_STOP_FILE")
    if stop_file:
        args.stop_file = stop_file
        set_stop_file(stop_file)
    set_verbose(getattr(args, "verbose", False)
                or os.environ.get("MC_BENCH_VERBOSE", "") == "1")
    log_phase("run 参数与环境")
    cluster = load_json(args.cluster)
    if args.mode == "k8s":
        cluster["_controller"] = build_controller(cluster)
        k8s_settings(cluster)      # fail fast on missing ns/image/workspaces
        try:
            info = controller_diagnose(cluster["_controller"])
            keep, extra = [], []
            for ln in info.splitlines():
                if ln.startswith(("HOME=", "PATH=")):
                    extra.append(ln)
                    continue
                k, _, v = ln.partition("=")
                keep.append("{}={}".format(k, v or "<unset>"))
            log("[k8s] controller: {} | {}".format(
                controller_desc(cluster["_controller"]), " | ".join(keep)))
            if extra:
                log_detail("[k8s] controller env: " + " | ".join(extra))
        except Exception as exc:
            log("[WARN] k8s controller info: {}".format(exc))
    enrich_k8s_nodes(cluster)
    globals_cfg = load_json(args.global_cfg)
    apply_parallel_knobs(globals_cfg)
    # pipeline rendezvous: wipe stale mcbench/ keys from crashed runs (the
    # run prefix is also wiped per scenario/size; this covers the rest).
    # Done BEFORE the capacity probe so both k8s DNS resolutions print as
    # one adjacent pair.
    try:
        _pipeline_wipe_etcd(cluster, globals_cfg)
    except Exception as exc:
        log("[WARN] etcd stale-key wipe failed: {}".format(exc))
    # store capacity: ALWAYS probe the master metrics (B) — even when
    # store_total_size (A) is set — so the effective capacity can follow the
    # min(A, B) rule. Probe failure only WARNs (A still stands).
    try:
        discover_store_capacity(cluster, globals_cfg)
    except Exception as exc:
        log("[WARN] store capacity auto-discovery: {}".format(exc))
    _a, _b, _eff, _src = _store_capacity_resolution(globals_cfg)
    _free = globals_cfg.get("_store_free_auto")
    log("store mem: [探测] total={} available={}; [配置]use={}".format(
        _fmt_gib(_b), _fmt_gib(_free), _fmt_gib(_a)))
    log("store mem: 本测试实际将使用容量={}".format(_fmt_gib(_eff)))
    if args.mode == "k8s":
        globals_cfg["_pod_workspace"] = k8s_settings(cluster)["pod_workspace"]
    # startup summary (mode, config paths, k8s settings)
    log("mode={} dry_run={} cluster={} global={} profile={}".format(
        args.mode, args.dry_run, os.path.relpath(args.cluster, ROOT),
        os.path.relpath(args.global_cfg, ROOT),
        os.path.relpath(args.profile, ROOT) if args.profile else "-"))
    if args.mode == "k8s":
        _ks = k8s_settings(cluster)
        log("[k8s] namespace={} | image={} | yaml={}".format(
            _ks["namespace"], _ks["image"],
            os.path.relpath(_pod_template_path(cluster), ROOT)))
        log("[k8s] workspace: controller={} pod={}".format(
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
        matrix = _apply_monitor_overrides(matrix, args)
    args._matrix = matrix
    if matrix:
        log("profiling active: {} (trigger={})".format(
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
    if args.result_dir:
        ts_dir = os.path.abspath(
            args.result_dir if os.path.isabs(args.result_dir)
            else os.path.join(ROOT, args.result_dir))
        try:
            inside_outputs = os.path.commonpath([RESULTS_DIR, ts_dir]) == RESULTS_DIR
        except ValueError:
            inside_outputs = False
        if not inside_outputs:
            sys.exit("[ERROR] --result-dir must be under {}".format(RESULTS_DIR))
        if os.path.exists(ts_dir):
            sys.exit("[ERROR] --result-dir already exists: {}".format(ts_dir))
        # The rendezvous/run identifier must remain unique even though rank
        # and recall now live below one shared results parent.
        ts = os.path.relpath(ts_dir, RESULTS_DIR).replace(os.sep, "-")
    else:
        ts = time.strftime("%Y%m%d-%H%M")
        ts_dir = os.path.join(RESULTS_DIR, ts)
        n = 1
        while os.path.exists(ts_dir):
            ts = "{}-{}".format(time.strftime("%Y%m%d-%H%M"), n)
            ts_dir = os.path.join(RESULTS_DIR, ts)
            n += 1
    # Keep the real local path separate from `ts`, which may be a flattened
    # remote run identifier for a nested rank/recall result directory.
    args._result_dir_path = ts_dir
    if not args.dry_run:
        os.makedirs(os.path.join(ts_dir, "logs"), exist_ok=True)
        if not args.result_dir:
            _copy_audit_snapshot(ts_dir)
        # full-detail log: every per-command/per-pod line goes here; the
        # console (WebUI) keeps the summary + verbose adds detail to it
        set_detail_log(os.path.join(ts_dir, "benchmark.log"))
        atexit.register(close_detail_log)
        log("Results: {}".format(ts_dir))
        log("benchmark log: {}".format(
            os.path.join(ts_dir, "benchmark.log")))
    if args.mode == "k8s":
        log_phase("run 初始化")
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
            elif cfg.get("scenario") == "kvtest":
                sweep_remove = run_kvtest_scenario(cluster, globals_cfg, cfg, ts, args)
            else:
                sweep_remove = run_basic_scenario(cluster, globals_cfg, cfg, ts, args)
            if not args.dry_run:
                scenario = cfg.get("scenario")
                if scenario not in ("pipeline", "kvtest"):
                    scenario = "basic"
                store_nodes = (globals_cfg.get("mooncake_store_nodes", []) or []) \
                    if args.mode != "k8s" else []
                # CSV + report map/reduce only read already-pulled LOCAL
                # artifacts: dispatch them to the background worker so they
                # overlap this config's teardown and the next one's
                # deployment. The snapshot keeps the worker out of the live
                # cluster["nodes"] dict the next k8s scenario keeps appending
                # to ("dictionary changed size during iteration" otherwise).
                _bg_submit(name, _post_process_config,
                           cfg, name, scenario, ts, ts_dir,
                           _cluster_snapshot(cluster), store_nodes,
                           (sweep_remove or {}).get("_report_kwargs") or {},
                           args)
        except Exception as exc:
            # the scenario's failure handler already printed the stage-aware
            # [FAIL] line (with pods + raw-log dirs); do not repeat it
            if not cfg.get("_fail_reported"):
                log("[FAIL] scenario {}: {}".format(name, exc))
            failed_scenarios.append(name)
    if args.dry_run:
        log("dry-run complete, no artifacts written")
        if failed_scenarios:
            sys.exit(1)
        return
    # Settle every background unit (CSV + report + profiling outliers) before
    # the run-level assembly. A failing CSV marks that scenario failed; the
    # report wrappers are WARN-only (same contract as the old inline path).
    for name, ok, payload, err in _bg_join():
        if not ok:
            log("[FAIL] scenario {}: {}".format(name, err))
            failed_scenarios.append(name)
            continue
        scenario, rows = payload
        categories.add(scenario)
        if scenario in ("pipeline", "kvtest"):
            # detail keeps only the aggregated writer/reader TOTAL rows
            all_rows.extend([r for r in rows if r.get("instance") == "TOTAL"])
        else:
            all_rows.extend(rows)
    # --- run-level assembly (local, fast) — done BEFORE the global teardown
    # so the detail CSV / overview are available immediately and the PNG plots
    # (also purely local) can run in parallel with the cluster teardown.
    if categories == {"pipeline"}:
        detail_header = PIPELINE_CSV_HEADER
        detail_rows = collapse_config_name(all_rows)
    elif categories == {"kvtest"}:
        detail_header = KVTEST_CSV_HEADER
        detail_rows = collapse_config_name(all_rows)
    elif categories == {"basic"}:
        detail_header = BASIC_CSV_HEADER
        detail_rows = all_rows
    else:
        # mixed (not reachable through normal CLI): ordered union of headers
        detail_header = [c for c in BASIC_CSV_HEADER]
        for extra in (PIPELINE_CSV_HEADER, KVTEST_CSV_HEADER):
            for c in extra:
                if c not in detail_header:
                    detail_header.append(c)
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
    # Reports + optional PNG: everything runs on the ORCHESTRATOR (local
    # sys.executable, local logs dir), never on remote nodes.
    #   - per-config reports were rendered by the background worker
    #   - the run-level overview is rendered here             [--no-report]
    #   - the legacy PNG plots run on the background worker while the global
    #     teardown below proceeds                            [--png]
    #   - kvtest has no per-unit HTML of its own: its request samples go
    #     through the same map+reduce chain (report.py)
    # Plotting is opt-in (--png, paired with --pull-raw-logs); --no-report
    # controls the HTML reports only and is independent of it.
    log_dir = os.path.join(ts_dir, "logs")
    if not args.no_report:
        if _REPORT_ACC:
            try:
                ov = _render_run_overview(ts_dir, args.mode)
                log("[report] overview -> {}".format(
                    os.path.relpath(ov, ROOT)))
            except Exception as exc:
                log("[WARN] overview report failed: {}".format(exc))
    plot_job = None
    if args.png:
        has_logs = bool(glob.glob(os.path.join(log_dir, "**", "*.log"),
                                  recursive=True))
        has_samples = bool(glob.glob(os.path.join(log_dir, "**", "*.samples.csv"),
                                   recursive=True))
        # kvtest samples are always plotted when the switch is on: PNG is its
        # only per-request view; python plots need the pulled raw logs.
        if has_logs or has_samples:
            plot_job = _bg_submit("plots", _run_legacy_plots, log_dir)
        else:
            log("[WARN] --png: no raw *.log / samples under {} (raw logs are "
                "only pulled for small runs / failures / --pull-raw-logs)"
                .format(log_dir))
    # Sweep end: remove-all on the last successful config's primary node.
    # Failed configs never reach their own end-remove, so this final pass is
    # what guarantees the store ends the sweep empty. Removing from an empty
    # store is a no-op (safe), and failure here must not fail the whole sweep
    # — hence WARN-only. The log is scp'd out because it does not match the
    # per-config glob used by pull_logs.
    log_phase("运行收尾")
    if sweep_remove:
        try:
            nd = get_node(cluster, sweep_remove["node"])
            ran = _remove_keys_stage(
                cluster, globals_cfg, sweep_remove["host"], nd,
                sweep_remove["runner"], sweep_remove["remote_log_dir"],
                "final", "final", "full", "sweep end", dry=False,
                node_name=sweep_remove["node"], label="[remove-keys] final")
            if not ran:
                log("sweep-end remove-keys skipped (store already clean)")
            else:
                scp_pull(sweep_remove["host"], nd,
                         "{}*".format(_pod_base("{}/final".format(
                             sweep_remove["remote_log_dir"]), nd)
                             + ".remove-keys.log"),
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
    if not (args.mode == "k8s" and cluster.get("_k8s_keep_ns")):
        _remove_service_cleanup(cluster)
    if args.mode == "k8s" and not args.dry_run:
        if cluster.get("_k8s_keep_ns"):
            log("[k8s] namespace {} kept for troubleshooting; teardown "
                "deferred until STOP/Ctrl+C".format(
                    k8s_settings(cluster)["namespace"]))
        else:
            cleanup_k8s_namespace(cluster, dry_run=False)
    # Join the run-level PNG job started before the teardown; it is purely
    # local, so the cluster teardown above ran in parallel with it.
    if plot_job is not None:
        for name, ok, result, err in _bg_join():
            if name != "plots":
                continue
            if not ok:
                log("[WARN] auto-plot failed: {}".format(err))
    # Success-only intermediate cleanup (both engines, both scenarios):
    # a config is cleaned only after every unit's report.html + data.js is
    # verified; failed configs keep their artifacts for --rebuild-report.
    _cleanup_run_intermediates(ts_dir, failed_scenarios, args)
    log("done. results in {}".format(ts_dir))
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
        if time.time() - last >= 300:
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


def _run_legacy_plots(log_dir):
    """Legacy PNG plots (plot_latency.py) — --png compat switch, and the only
    per-request view for kvtest runs. Returns True when plotting succeeded."""
    plot_script = os.path.join(ROOT, "scripts", "plot_latency.py")
    # Plotting is 100% local. Cap the BLAS/OpenMP thread pools: OpenBLAS
    # pre-allocates one buffer per thread at import, which on Windows can
    # fail with "OpenBLAS error: Memory allocation still failed" when the
    # commit limit/pagefile is tight or Python is 32-bit.
    plot_env = dict(os.environ)
    for var in ("OPENBLAS_NUM_THREADS", "OMP_NUM_THREADS",
                "MKL_NUM_THREADS", "NUMEXPR_NUM_THREADS"):
        plot_env[var] = "1"
    log("plotting locally on this machine (orchestrator): "
        "{} {} --log {}".format(sys.executable, plot_script, log_dir))
    try:
        proc = subprocess.run([sys.executable, plot_script, "--log", log_dir],
                              check=False, env=plot_env)
        if proc.returncode != 0:
            log("[WARN] auto-plot failed (rc={}); figures skipped - see the "
                "traceback above; if matplotlib is missing: pip install "
                "matplotlib".format(proc.returncode))
            return False
        return True
    except Exception as exc:
        log("[WARN] auto-plot failed: {}".format(exc))
        return False


if __name__ == "__main__":
    ensure_utf8_stdio()
    _install_stop_handlers()
    args, cfg_paths = parse_arguments()
    try:
        main(args, cfg_paths)
    except (StopRequested, KeyboardInterrupt) as exc:
        log("[STOP] requested ({}); collecting a partial report ...".format(
            exc.__class__.__name__))
        try:
            _stop_reports()
        except KeyboardInterrupt:
            raise
        except Exception as ce:
            log("[WARN] [STOP] partial report failed: {}".format(ce))
        log("[STOP] cleaning up ...")
        try:
            cleanup_on_stop()
        except Exception as ce:
            log("[WARN] [STOP] cleanup failed: {}".format(ce))
        sys.exit(130)
    except RuntimeError as exc:
        sys.exit("[ERROR] {}".format(exc))
