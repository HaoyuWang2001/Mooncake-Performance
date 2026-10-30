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
import json
import os
import re
import shlex
import subprocess
import sys
import time

from utils import (get_node, load_json, log, parse_data_size, percentile,
                   run_ssh, scp_pull, scp_push, ssh_args, ssh_check_output)

# profiling matrix core (optional tools: spdiag / perf / bpftrace)
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "profiling"))
try:
    import prof_core
except ImportError:  # profiling dir missing -> feature silently off
    prof_core = None

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIGS_DIR = os.path.join(ROOT, "configs")
SCRIPTS_DIR = os.path.join(ROOT, "scripts")
RESULTS_DIR = os.path.join(ROOT, "outputs")
# RESULT rows whose first field is a wall-clock timestamp (HH:MM:SS.micro);
# legacy rows start with the op name directly (see parse_benchmark_log)
TS_PREFIX = re.compile(r"^\d{2}:\d{2}:\d{2}")
DEFAULT_LD_PATH = ("/home/w00921547/stress-cluster-bench/lib/:/usr/local/cuda-12.9/lib64:"
                   "/usr/local/lib64:${LD_LIBRARY_PATH:-}")
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
    "writer_count", "reader_count", "notify_count",
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


def pipeline_plan(cfg):
    """Derive the writer/reader process plan from a pipeline config.

    Shared by run_pipeline_scenario (orchestration) and write_scenario_csv
    (metrics attribution) so the assignment stays deterministic and
    consistent. Both roles start count_per_node processes on EVERY node of
    their client_nodes (ascending node order), so:
      writer total = writer.count_per_node * len(writer.client_nodes)
      reader pool = reader.count_per_node * len(reader.client_nodes)
    Total write QPS = base_qps * qps_multiple, split evenly across all
    writers (n2 dual-writer reproduces the datasystem "105/2" semantics
    with the total held constant). Writers get global ids 1..W in node
    order; readers get ports 9201..9200+R. The reader pool must cover
    notify_count (each batch notifies a random subset of notify_count
    readers). The business model "1写10读" maps to writer count_per_node=1
    and notify_count=10 — the reader pool is a deployment parameter.
    """
    writer_cfg = cfg.get("writer", {}) or {}
    reader_cfg = cfg.get("reader", {}) or {}
    w_cpn = int(writer_cfg.get("count_per_node", 1))
    r_cpn = int(reader_cfg.get("count_per_node", 1))
    w_nodes = writer_cfg.get("client_nodes") or [cfg.get("store_node", "")]
    r_nodes = reader_cfg.get("client_nodes") or [cfg.get("store_node", "")]
    writers = []
    for node in w_nodes:
        for _ in range(w_cpn):
            writers.append({"gid": len(writers) + 1, "node": node})
    readers = []
    for node in r_nodes:
        for _ in range(r_cpn):
            readers.append({"index": len(readers) + 1, "port": 9201 + len(readers), "node": node})
    base_qps = float(cfg.get("base_qps", 0))
    qps_multiple = float(cfg.get("qps_multiple", 1))
    total_qps = base_qps * qps_multiple
    per_writer_qps = (total_qps / len(writers)) if writers else total_qps
    return {
        "writers": writers,
        "readers": readers,
        "total_qps": total_qps,
        "per_writer_qps": per_writer_qps,
        "notify_count": int(cfg.get("notify_count", 1)),
        "notify_threads": int(writer_cfg.get("notify_threads", 1)),
        "reader_threads": int(reader_cfg.get("num_threads", 1)),
        "writer_nodes": w_nodes,
        "reader_nodes": r_nodes,
    }


# --------------------------------------------------------------------------
# shared orchestration infrastructure (both scenario families)
# --------------------------------------------------------------------------

def apply_cli_overrides(cfg, args):
    # --protocol has the highest priority and overrides the JSON config value
    if getattr(args, "protocol", None):
        cfg["protocol"] = args.protocol


def scenario_paths(globals_cfg, ts, cfg_name):
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


def build_cluster_session(cluster, globals_cfg, remote_dir):
    master_type = globals_cfg.get("master_type", "") or ""
    session = {
        "MC_MASTER_TYPE": master_type,
        "MC_UB_DEVICE": globals_cfg.get("protocol", {}).get("ub", {}).get("device", ""),
        "MC_VENV": resolve_venv(cluster, globals_cfg),
        "MC_LOCAL_BUFFER_SIZE": str(globals_cfg.get("local_buffer_size", 8589934592)),
        "MC_TENANT_ID": globals_cfg.get("tenant_id", "default"),
        "MC_RANDOM_SEED": str(globals_cfg.get("random_seed", 42)),
        "MC_REMOTE_DATA_DIR": globals_cfg.get("remote_data_dir", remote_dir + "/data"),
        # NOTE: MC_GLOBAL_SEGMENT_SIZE, MC_YLT_LOG_LEVEL, MC_YLT_LOG_MAX_FILE_SIZE
        # and MC_YLT_LOG_MAX_FILES are intentionally NOT generated here. Their
        # ownership moved to scripts/env.mooncake.sh (hand-editable tuning
        # panel, sourced by the runner scripts after env.sh) — see that file.
        # Same for MC_HIFREQ_LOG_SAMPLE_RATE (moved earlier).
    }
    if master_type == "etcd":
        # etcd mode: only the etcd URI is exported; the client passes it to
        # both setup params. master_addr/metadata_server (if set) are dormant.
        session["MC_ETCD_ENDPOINTS"] = globals_cfg.get("etcd_endpoints", "")
    else:
        # http mode: only rpc master + http metadata are exported
        session["MC_MASTER_ADDR"] = globals_cfg.get("master_addr", "")
        session["MC_METADATA_SERVER"] = globals_cfg.get("metadata_server", "")
    return session


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
        ips.add(n["ip"])
    return ips


def base_scenario_params(cfg, work_mode, topology, primary_node_name):
    # data_size is a sweep list; the env var carries the first entry as a
    # fallback only — the actual per-run size reaches the client via the
    # generate-data stage's --data-size and the value.bin file length.
    return {
        "MC_SCENARIO": cfg["scenario"],
        "MC_WORK_MODE": work_mode,
        "MC_OPERATION": cfg.get("operation", ""),
        "MC_PROTOCOL": cfg["protocol"],
        "MC_KEY_PREFIX": cfg.get("key_prefix", "mc_bench"),
        "MC_KEY_COUNT": str(cfg.get("key_count", 1000)),
        "MC_DATA_SIZE": data_size_list(cfg)[0],
        "MC_BATCH_SIZE": str(cfg.get("batch_size", 1)) if (
            cfg.get("operation") in ("put_batch", "get_batch")
            or any(s in ("put_batch", "get_batch") for s in str(cfg.get("operation", "")).split("/"))) else "",
        "MC_PUT_RATIO": str(cfg.get("put_ratio", "")) if "/" in str(cfg.get("operation", "")) else "",
        "MC_DURATION": str(cfg.get("duration", 0)),
        "MC_WARMUP_DURATION": str(cfg.get("warmup", {}).get("duration", 0)),
        # per-client core count for NUMA-aware sharding (0/unset = whole
        # node); the basic runner assigns each client its NUMA + slice index
        "MC_CPU_SHARD": str(cfg.get("cpu_shard", 0) or 0),
    }


def build_env_content(scenario_params, cluster_session, ld_path, fabric_ip,
                      exclude_cpus=""):
    session = dict(cluster_session)
    session["MC_FABRIC_IP"] = fabric_ip
    # TCP transport bind address: every client process on this node must bind
    # the node's fabric IP, or store-side connections wedge during setup
    # (observed on the real cluster: readers hung unresponsive to /health and
    # /shutdown). Same value for all processes on the node -> per-node env.sh.
    session["MC_TCP_BIND_ADDRESS"] = fabric_ip
    # reserved cores (system / other users), subtracted from every binding
    # mode on this node (see client apply_cpu_binding)
    if exclude_cpus:
        session["MC_EXCLUDE_CPUS"] = exclude_cpus
    lines = ["#!/usr/bin/env bash", ""]
    lines.append("# scenario params")
    for k, v in sorted(scenario_params.items()):
        if v:
            lines.append('export {}="{}"'.format(k, v))
    lines.append("")
    lines.append("# cluster / session")
    for k, v in sorted(session.items()):
        if v:
            lines.append('export {}="{}"'.format(k, v))
    lines.append("")
    lines.append('export LD_LIBRARY_PATH={}'.format(ld_path))
    lines.append("")
    return "\n".join(lines) + "\n"


def deploy_scripts(cluster, involved_nodes, scenario_params, cluster_session,
                   ld_path, remote_scripts, remote_log_dir, dry_run):
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
        fab = n.get("fabric_ip", n.get("ip", ""))
        # reserved cores (system/other users): node-level override wins over
        # the cluster-wide exclude_cpus; injected per-node so every client
        # process on this node subtracts it from its binding
        exclude = n.get("exclude_cpus", "") or cluster.get("exclude_cpus", "") or ""
        with open(env_sh_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(build_env_content(scenario_params, cluster_session, ld_path, fab,
                                      exclude_cpus=str(exclude)))
        log("env.sh written (node={}, fabric_ip={}, exclude_cpus={})".format(
            node_name, fab, exclude or "-"))
        run_ssh(n["ip"], n, "mkdir -p {} {}".format(
            remote_scripts, remote_log_dir), dry_run=dry_run)
        push_files = [env_sh_path,
                      os.path.join(SCRIPTS_DIR, "run_benchmark_basic.sh"),
                      os.path.join(SCRIPTS_DIR, "run_benchmark_pipeline.sh"),
                      os.path.join(SCRIPTS_DIR, "mooncake_benchmark_client.py")]
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


def fail_cleanup_pipeline(cluster, plan, reader_peers, reader_pids, writer_pids,
                          involved_nodes, remote_log_dir, cfg_name, local_log_dir):
    """Best-effort cleanup after a pipeline scenario failure, so remote logs
    (including the reader SUMMARY/RESULT that is only flushed at exit) are
    preserved for diagnosis and ports are freed. Every step is guarded so the
    original exception is never masked. Order: graceful reader shutdown ->
    wait for exit -> kill stragglers -> pull all logs."""
    log("[FAIL] scenario {} failed; pulling remote logs for diagnosis ...".format(cfg_name))
    # 1. graceful reader shutdown so each reader flushes SUMMARY/RESULT
    for r, peer in zip(plan["readers"], reader_peers):
        try:
            rn = get_node(cluster, r["node"])
            url = "http://{}:{}/shutdown".format(peer["host"], r["port"])
            ssh_check_output(rn["ip"], rn,
                             "curl -s --noproxy '*' --connect-timeout 2 --max-time 5 -X POST {} || true".format(url),
                             quiet=True)
        except Exception as exc:
            log("[WARN] fail-cleanup: reader {} shutdown failed: {}".format(r["index"], exc))
    # 2. short wait for readers to exit (their final logs are written on exit)
    for r, pid in reader_pids:
        if pid:
            try:
                wait_process_exit(cluster, r["node"], pid,
                                  "reader r{}".format(r["index"]), 30)
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


def fail_cleanup_basic(cluster, involved_nodes, remote_log_dir, cfg_name, local_log_dir):
    """Best-effort log pull after a basic scenario failure (foreground
    processes exit by themselves; only the evidence needs to be preserved)."""
    log("[FAIL] scenario {} failed; pulling remote logs for diagnosis ...".format(cfg_name))
    try:
        pull_logs(cluster, involved_nodes, remote_log_dir, cfg_name, local_log_dir, False)
    except Exception as exc:
        log("[WARN] fail-cleanup: log pull failed: {}".format(exc))


def run_ssh_with_progress(host, node, command, label, expected_s=None, dry_run=False,
                          pids_file=None, cfg_name=None, node_name=None):
    """run_ssh variant for long-blocking commands (e.g. the basic benchmark
    stage): launches the ssh child with Popen and logs ONE 'running...' line
    (expected duration + ETA when expected_s is given). rc!=0 raises like
    run_ssh.

    pids_file: when given, the first poll reads that remote file (client
    pids written by the runner script) and records every pid into the
    .state ledger under cfg_name — so a crashed orchestrator still leaves a
    killable trail."""
    log("[ssh] {} : {}".format(host, command))
    if dry_run:
        log("      (dry-run, skipped)")
        return
    proc = subprocess.Popen(ssh_args(node) + [command])
    t0 = time.time()
    log("{} running...{}".format(label, _wait_suffix(t0, expected_s)))
    recorded = False
    while proc.poll() is None:
        if not recorded and pids_file:
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
                    "ts": time.strftime("%Y-%m-%d %H:%M:%S")} for p in pids])
                recorded = True
        time.sleep(1)
    rc = proc.wait()
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
            ssh_check_output(ip, node,
                             "kill {pid} 2>/dev/null || true".format(pid=pid), quiet=True)
            time.sleep(2)
            still = ssh_check_output(
                ip, node,
                "kill -0 {pid} 2>/dev/null && echo ALIVE || echo DEAD".format(pid=pid),
                quiet=True)
            if still == "ALIVE":
                ssh_check_output(ip, node,
                                 "kill -9 {pid} 2>/dev/null || true".format(pid=pid), quiet=True)
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
              dry_run=False, pull_to=None):
    remote_log = remote_log_path(remote_log_dir, log_prefix, mode)
    command = "bash {} {} {}".format(runner, mode, remote_log)
    if extra:
        command += " " + extra
    run_ssh(host, node, command, dry_run=dry_run)
    if pull_to is not None:
        pull_stage_logs(host, node, [remote_log], pull_to, dry_run)


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
    cfg_name = os.path.splitext(os.path.basename(cfg.get("__cfg_path__", scenario)))[0]
    dry = args.dry_run
    op = cfg.get("operation", "")
    work_mode = cfg.get("work_mode", cfg.get("mode", ""))
    primary_node_name = cfg["client_node"]
    involved_nodes = [primary_node_name]
    node = get_node(cluster, primary_node_name)
    host = node["ip"]
    # topology: local iff the client node's IP is one of the store nodes' IPs
    # (mooncake_store_nodes from global.json, resolved via cluster.json)
    store_node_ips = resolve_store_node_ips(
        cluster, globals_cfg.get("mooncake_store_nodes", []) or [])
    topology = "local_node" if node["ip"] in store_node_ips else "remote_node"
    # duration mode is supported by ALL basic operations: get-family loops
    # over prefilled keys; put-family/mixed run full-key passes with untimed
    # self-clear between cycles (client _run_thread)
    remote_dir, remote_scripts, remote_log_dir, remote_runner_basic, \
        remote_runner_pipeline, local_log_dir = \
        scenario_paths(globals_cfg, ts, cfg_name)
    remote_runner = remote_runner_basic
    if not dry:
        os.makedirs(local_log_dir, exist_ok=True)
    log("=== scenario {} (op={}, topo={}, protocol={}) primary_node={} ===".format(
        cfg_name, op, topology, cfg["protocol"], host))
    scenario_params = base_scenario_params(cfg, work_mode, topology, primary_node_name)
    cluster_session = build_cluster_session(cluster, globals_cfg, remote_dir)
    ld_path = cluster.get("ld_library_path", DEFAULT_LD_PATH)
    deploy_scripts(cluster, involved_nodes, scenario_params, cluster_session,
                   ld_path, remote_scripts, remote_log_dir, dry)
    # The try MUST start here (before any stage): a failure in generate-data /
    # remove-keys / prepare-store previously raised before the try block, so
    # the FAIL path never pulled the remote logs — silent failure with no
    # diagnosable evidence. All stages are now covered.
    try:
        profile_sess = None  # profiling session (set below when matrix is on)
        run_stage(host, node, remote_runner,
                  "remove-keys", remote_log_dir, cfg_name, extra="--scope full",
                  dry_run=dry, pull_to=local_log_dir)
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
            if ds != current_ds:
                # data_size changed: (re)build value.bin at this size and
                # re-prefill the store for get-family combos
                ds_bytes = parse_data_size(ds)
                check_cmd = value_check_cmd(cluster_session["MC_REMOTE_DATA_DIR"], scenario, ds_bytes)
                data_status = ssh_check_output(host, node, check_cmd, dry_run=dry)
                if data_status != "EXISTS":
                    run_stage(host, node, remote_runner,
                              "generate-data", remote_log_dir, cfg_name,
                              extra="--data-size {}".format(ds), dry_run=dry,
                              pull_to=local_log_dir)
                if op in PIPELINE_GET_IFACES:
                    run_stage(host, node, remote_runner,
                              "prepare-store", remote_log_dir, cfg_name,
                              dry_run=dry, pull_to=local_log_dir)
                current_ds = ds
            if need_remove_before_run:
                run_stage(host, node, remote_runner,
                          "remove-keys", remote_log_dir, cfg_name,
                          extra="--scope full", dry_run=dry,
                          pull_to=local_log_dir)
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
        # scope, bounded by MC_KEY_COUNT from env.sh). Full scope here would
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
        raise


# --------------------------------------------------------------------------
# pipeline scenario (producer/consumer: writers notify random reader subsets)
# --------------------------------------------------------------------------

def run_pipeline_scenario(cluster, globals_cfg, cfg, ts, args):
    apply_cli_overrides(cfg, args)
    matrix = getattr(args, "_matrix", None)
    scenario = cfg["scenario"]
    cfg_name = os.path.splitext(os.path.basename(cfg.get("__cfg_path__", scenario)))[0]
    dry = args.dry_run
    op = cfg.get("operation", "")
    work_mode = cfg.get("work_mode", cfg.get("mode", ""))
    plan = pipeline_plan(cfg)
    if plan["notify_count"] < 1:
        raise RuntimeError("pipeline config requires notify_count >= 1")
    if plan["notify_count"] > len(plan["readers"]):
        raise RuntimeError("notify_count={} exceeds reader count {}".format(
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
    involved_nodes = []
    seen_nodes = set()
    for p in plan["writers"] + plan["readers"]:
        if p["node"] not in seen_nodes:
            seen_nodes.add(p["node"])
            involved_nodes.append(p["node"])
    primary_node_name = involved_nodes[0] if involved_nodes else cfg.get("store_node", "")
    node = get_node(cluster, primary_node_name)
    host = node["ip"]
    # topology: local iff the primary client node's IP is one of the store
    # nodes' IPs (mooncake_store_nodes from global.json, resolved via
    # cluster.json)
    store_node_ips = resolve_store_node_ips(
        cluster, globals_cfg.get("mooncake_store_nodes", []) or [])
    topology = "local_node" if node["ip"] in store_node_ips else "remote_node"
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
        "MC_WRITER_COUNT": str(len(plan["writers"])),
        "MC_READER_COUNT": str(len(plan["readers"])),
        "MC_TARGET_QPS": str(plan["per_writer_qps"]),
        "MC_NOTIFY_COUNT": str(plan["notify_count"]),
        "MC_NOTIFY_THREADS": str(plan["notify_threads"]),
        "MC_NOTIFY_PIPELINE": ",".join(cfg.get("notify_pipeline", [])),
        "MC_TRAFFIC_MODE": cfg.get("traffic_mode", "pulse"),
        "MC_DURATION": str(cfg.get("duration", 30)),
        "MC_BATCH_SIZE": str(cfg.get("batch_size", 1)),
        # key_count (optional): per-writer key-space cap — when set the writer
        # wraps its sequence with delete-before-write, bounding store usage at
        # writers x key_count x data_size; empty = unbounded (legacy behaviour)
        "MC_KEY_COUNT": str(cfg.get("key_count", "") or ""),
        # clear basic-only vars that are meaningless for pipeline
        "MC_OPERATION": "",
        "MC_WARMUP_DURATION": str(cfg.get("warmup", {}).get("duration", 0)),
    })
    cluster_session = build_cluster_session(cluster, globals_cfg, remote_dir)
    ld_path = cluster.get("ld_library_path", DEFAULT_LD_PATH)
    deploy_scripts(cluster, involved_nodes, scenario_params, cluster_session,
                   ld_path, remote_scripts, remote_log_dir, dry)
    reader_peers = []
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
        log("scenario {} (pipeline, {}): writers={}, readers={}, notify_count={}, "
            "total_qps={}, per_writer_qps={}, traffic={}, sizes={}".format(
                cfg_name, work_mode, len(plan["writers"]), len(plan["readers"]),
                plan["notify_count"], plan["total_qps"], plan["per_writer_qps"],
                cfg.get("traffic_mode", "pulse"), sizes))
        current_ds = None
        for ds in sizes:
            if ds != current_ds:
                # data_size changed: (re)build value.bin at this size on every
                # node that runs a writer or reader
                ds_bytes = parse_data_size(ds)
                check_cmd = value_check_cmd(cluster_session["MC_REMOTE_DATA_DIR"], scenario, ds_bytes)
                for node_name in involved_nodes:
                    n = get_node(cluster, node_name)
                    data_status = ssh_check_output(n["ip"], n, check_cmd, dry_run=dry)
                    if data_status != "EXISTS":
                        run_stage(n["ip"], n, remote_runner,
                                  "generate-data", remote_log_dir, cfg_name,
                                  extra="--data-size {}".format(ds), dry_run=dry,
                                  pull_to=local_log_dir)
                current_ds = ds
            run_stage(host, node, remote_runner,
                      "remove-keys", remote_log_dir, cfg_name, extra="--scope full",
                      dry_run=dry, pull_to=local_log_dir)
            # Reader peers: host resolved from each reader node's fabric_ip so the
            # same config works unchanged on any deployment. Rebuilt per size so
            # the fail-path cleanup only ever sees the current run's processes.
            reader_peers = []
            reader_pids = []
            writer_pids = []
            for r in plan["readers"]:
                rn = get_node(cluster, r["node"])
                reader_peers.append({
                    "role": "reader",
                    "host": rn.get("fabric_ip", rn.get("ip", "")),
                    "port": r["port"],
                    "node": r["node"],
                })
            peers_json = json.dumps(reader_peers)
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
            # Step 1: start all readers (one process per reader, unique port and id)
            for r, peer in zip(plan["readers"], reader_peers):
                rn = get_node(cluster, r["node"])
                merged_log = "{}.reader_port{}.log".format(base_noext, r["port"])
                nohup_log = "{}.reader_port{}.nohup".format(base_noext, r["port"])
                numa_arg = ""
                launch_prefix = ""
                if (cpu_shard > 0 or numa_bind) and not dry:
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
                rcmd = "nohup {}bash {} pipeline {} --client-id {} --num-threads {} --role reader --listen-port {} --peers '{}' --warmup false --timeout 300 --operation {} {} > {} 2>&1 & echo $!".format(
                    launch_prefix, remote_runner, merged_log, r["index"], reader_threads,
                    r["port"], peers_json, reader_op, numa_arg, nohup_log)
                pid = ssh_check_output(rn["ip"], rn, rcmd, dry_run=dry)
                reader_pids.append((r, pid))
                if not dry and pid:
                    # ledger: this reader can always be found & killed later
                    state_append([{
                        "ip": rn["ip"], "node": r["node"], "port": r["port"],
                        "fabric_host": peer["host"],
                        "pid": pid.strip(), "label": "reader r{}".format(r["index"]),
                        "cfg": cfg_name,
                        "ts": time.strftime("%Y-%m-%d %H:%M:%S")}])
            # give readers a moment to boot before the health polling starts
            if not dry:
                time.sleep(5)
            # Step 2: wait for all readers to be healthy
            for r, peer in zip(plan["readers"], reader_peers):
                rn = get_node(cluster, r["node"])
                health_url = "http://{}:{}/health".format(peer["host"], r["port"])
                HEALTHY = False
                for attempt in range(10):
                    health_cmd = "curl -s --noproxy '*' --connect-timeout 2 --max-time 2 {} || echo '__NOT_READY__'".format(health_url)
                    result = ssh_check_output(rn["ip"], rn, health_cmd, dry_run=dry, quiet=True) if not dry else "HEALTHY"
                    if "HEALTHY" in result:
                        HEALTHY = True
                        break
                    time.sleep(1)
                if not dry:
                    if not HEALTHY:
                        err_msg = "reader at {}:{} not ready after 30s".format(peer["host"], r["port"])
                        log("[ERROR] " + err_msg)
                        raise RuntimeError(err_msg)
                    else:
                        log("reader at {}:{}=READY".format(peer["host"], r["port"]))
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
                merged_log = "{}.writer{}.log".format(base_noext, w["gid"])
                nohup_log = "{}.writer{}.nohup".format(base_noext, w["gid"])
                numa_arg = ""
                launch_prefix = ""
                if (cpu_shard > 0 or numa_bind) and not dry:
                    nn = numa_counter[w["node"]] % numa_node_count[w["node"]]
                    numa_counter[w["node"]] += 1
                    idx = shard_ordinal(w["node"], nn) if cpu_shard > 0 else 0
                    cl = numa_binding_spec(numa_topo[w["node"]][nn],
                                           node_exclude(cluster, wn),
                                           cpu_shard, idx)
                    numa_arg = "--cpu-bind {}".format(cl)
                    if numactl_ok.get(w["node"]):
                        launch_prefix = numactl_launch_prefix(membind_mode, nn, cl) + " "
                wcmd = "nohup {}bash {} pipeline {} --client-id {} --role writer --peers '{}' --warmup {} --operation {} {} > {} 2>&1 & echo $!".format(
                    launch_prefix, remote_runner, merged_log, w["gid"], peers_json,
                    warmup, writer_op, numa_arg, nohup_log)
                pid = ssh_check_output(wn["ip"], wn, wcmd, dry_run=dry)
                writer_pids.append((w, pid))
                if not dry and pid:
                    state_append([{
                        "ip": wn["ip"], "node": w["node"], "port": None,
                        "fabric_host": None,
                        "pid": pid.strip(), "label": "writer w{}".format(w["gid"]),
                        "cfg": cfg_name,
                        "ts": time.strftime("%Y-%m-%d %H:%M:%S")}])
            # Step 4: wait for all writers to finish (warmup + measured run)
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
                                ["{}.writer{}.log".format(base_noext, w["gid"])],
                                local_log_dir, dry)
            # Step 5: shutdown readers and wait for their final logs
            for r, peer in zip(plan["readers"], reader_peers):
                rn = get_node(cluster, r["node"])
                shutdown_url = "http://{}:{}/shutdown".format(peer["host"], r["port"])
                curl_cmd = "curl -s --noproxy '*' --connect-timeout 2 --max-time 5 -X POST {} || true".format(shutdown_url)
                if not dry:
                    ssh_check_output(rn["ip"], rn, curl_cmd, dry_run=dry)
            for r, pid in reader_pids:
                if not dry and pid:
                    wait_process_exit(cluster, r["node"], pid,
                                      "reader r{} (port {})".format(r["index"], r["port"]),
                                      60, dry_run=dry, expected_s=60)
            for r in plan["readers"]:
                rn = get_node(cluster, r["node"])
                pull_stage_logs(rn["ip"], rn,
                                ["{}.reader_port{}.log".format(base_noext, r["port"])],
                                local_log_dir, dry)
            # Step 6: remove empty .nohup redirect files (they capture script-level
            # errors only — empty on success). Non-empty ones are kept for diagnostics
            # and pulled back with the rest of the logs.
            if not dry:
                for node_name in involved_nodes:
                    n = get_node(cluster, node_name)
                    nohup_files = []
                    for r in plan["readers"]:
                        if r["node"] == node_name:
                            nohup_files.append("{}.reader_port{}.nohup".format(base_noext, r["port"]))
                    for w in plan["writers"]:
                        if w["node"] == node_name:
                            nohup_files.append("{}.writer{}.nohup".format(base_noext, w["gid"]))
                    if nohup_files:
                        cleanup_cmd = "for f in {}; do [ -s \"$f\" ] || rm -f \"$f\"; done".format(
                            " ".join(nohup_files))
                        ssh_check_output(n["ip"], n, cleanup_cmd, dry_run=dry)
        # Config end: remove-all — clears everything this config's writers
        # wrote. Pipeline env clears MC_KEY_COUNT, so the key-prefix scope has
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
        # Sweep-end remove-all is issued by main() on the last successful
        # config's node; hand it this config's coordinates.
        return {"host": host, "node": primary_node_name, "runner": remote_runner,
                "remote_log_dir": remote_log_dir}
    except Exception:
        # FAIL path: preserve every remote log (reader SUMMARY/RESULT is only
        # flushed on exit) and free ports, then re-raise for main's [FAIL]
        if not dry:
            fail_cleanup_pipeline(cluster, plan, reader_peers, reader_pids, writer_pids,
                                  involved_nodes, remote_log_dir, cfg_name, local_log_dir)
            # 复核台账：已死删行、杀不死的保留待下次 reap
            reap_state_entries(cluster)
            if profile_sess is not None:
                prof_core.stop_and_collect(cluster, profile_sess,
                                           os.path.join(RESULTS_DIR, ts, "logs", cfg_name, "profiling"))
        raise


def cleanup_cluster(cluster, globals_cfg, dry_run=False):
    """Remove the remote test directory on every node in cluster.json."""
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
                "key_count": cfg.get("key_count", "") or "",
                "duration": cfg.get("duration", ""),
                "warmup_duration": cfg.get("warmup", {}).get("duration", ""),
                "writer_count": "N*{}".format(w_cpn),
                "reader_count": "N*{}".format(r_cpn),
                "notify_count": plan["notify_count"],
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
            writer_files = {}
            for p in sorted(glob.glob(os.path.join(
                    local_log_dir, "{}.{}.writer*.log".format(name, run_name)))):
                base = os.path.basename(p)
                if ".ylt." in base:
                    continue
                try:
                    gid = int(base.replace(".log", "").split(".writer")[-1])
                except ValueError:
                    continue
                writer_files[gid] = p
            reader_files = {}
            for p in sorted(glob.glob(os.path.join(
                    local_log_dir, "{}.{}.reader_port*.log".format(name, run_name)))):
                base = os.path.basename(p)
                if ".ylt." in base:
                    continue
                try:
                    port = int(base.replace(".log", "").split(".reader_port")[-1])
                except ValueError:
                    continue
                reader_files[port] = p
            for w in plan["writers"]:
                f = writer_files.get(w["gid"])
                if not f:
                    continue
                agg = parse_benchmark_log(f, op_filter="pipeline_write")
                row = base_row("writer", "w{}".format(w["gid"]), w["node"], ds)
                metrics_into(row, agg, ds_bytes)
                rows.append(row)
            # writer TOTAL row: all writers aggregated; throughput = sum of each
            # writer's busy-time throughput
            if writer_files:
                agg = parse_benchmark_log(list(writer_files.values()), op_filter="pipeline_write")
                row = base_row("writer", "TOTAL", "", ds)
                metrics_into(row, agg, ds_bytes,
                             throughput_mb=busy_throughput_mb(
                                 writer_files.values(), "pipeline_write", ds_bytes))
                rows.append(row)
            for r in plan["readers"]:
                f = reader_files.get(r["port"])
                if not f:
                    continue
                agg = parse_benchmark_log(f, op_filter="pipeline_read")
                row = base_row("reader", "r{}".format(r["index"]), r["node"], ds)
                metrics_into(row, agg, ds_bytes, with_queue=True)
                rows.append(row)
            # TOTAL row: all readers aggregated (aligns with the datasystem
            # single GET row per test group); throughput = sum of each reader's
            # busy-time throughput
            if reader_files:
                agg = parse_benchmark_log(list(reader_files.values()), op_filter="pipeline_read")
                row = base_row("reader", "TOTAL", "", ds)
                metrics_into(row, agg, ds_bytes, with_queue=True,
                             throughput_mb=busy_throughput_mb(
                                 reader_files.values(), "pipeline_read", ds_bytes))
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
        topo = topo_of(cfg["client_node"])
        node_ip = get_node(cluster, cfg["client_node"])["ip"] if cluster else ""
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
    return merged


def collect_config_files(directory):
    """Collect scenario config files under directory, RECURSIVELY (demo/ is a
    plain directory and is included like any other; "_"-prefixed files are
    skipped at every level — they are shared-defaults inheritance fragments,
    not runnable scenarios)."""
    found = []
    for root, dirs, files in os.walk(directory):
        dirs[:] = sorted(dirs)
        for f in sorted(files):
            if f.endswith(".json") and not f.startswith("_"):
                found.append(os.path.join(root, f))
    return found


def parse_arguments(argv=None):
    """Parse CLI args, validate combinations, and collect config paths.

    Returns (args, cfg_paths).
    """
    parser = argparse.ArgumentParser(description="Mooncake benchmark orchestrator (runs locally)")
    parser.add_argument("--cluster", default=os.path.join(CONFIGS_DIR, "cluster.json"),
                        help="path to cluster json")
    parser.add_argument("--global", dest="global_cfg",
                        default=os.path.join(CONFIGS_DIR, "global.json"),
                        help="path to global hyper json")
    parser.add_argument("--profile", default=None,
                        help="profiling matrix json (default: configs/matrix.json "
                             "when present; no file -> profiling off)")
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
    if not os.path.exists(args.cluster):
        example = os.path.join(CONFIGS_DIR, "cluster.example.json")
        sys.exit("[ERROR] cluster config not found: {}\n"
                 "       copy {} to {} and edit it first.".format(
                     args.cluster, example, os.path.join(CONFIGS_DIR, "cluster.json")))
    if not os.path.exists(args.global_cfg):
        sys.exit("[ERROR] global config not found: {}".format(args.global_cfg))
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
    cluster = load_json(args.cluster)
    globals_cfg = load_json(args.global_cfg)
    _validate_master_mode(globals_cfg)
    # profiling matrix: explicit --profile wins (and must exist); otherwise
    # default configs/matrix.json when present; absent -> profiling off
    profile_path = args.profile or os.path.join(CONFIGS_DIR, "matrix.json")
    matrix = None
    if args.profile and not os.path.isfile(args.profile):
        sys.exit("[ERROR] --profile matrix not found: {}".format(args.profile))
    if prof_core is not None:
        matrix = prof_core.load_matrix(profile_path)
    args._matrix = matrix
    if matrix:
        log("profiling matrix active: {} (trigger={})".format(
            profile_path, matrix.get("trigger")))
    # .state ledger check: reap any client processes left over from previous
    # runs (crashed/killed orchestrator) BEFORE anything else touches nodes.
    if not args.dry_run:
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
    all_rows = []
    categories = set()
    failed_scenarios = []
    # coordinates of the last successful config — the sweep-end remove-all
    # runs on its node (failed configs skip their own end-remove, so the
    # sweep-end pass is what guarantees the store gets emptied)
    sweep_remove = None
    for cfg_path in cfg_paths:
        cfg = load_scenario_config(cfg_path)
        cfg["__cfg_path__"] = cfg_path
        name = os.path.splitext(os.path.basename(cfg_path))[0]
        try:
            if cfg.get("scenario") == "pipeline":
                sweep_remove = run_pipeline_scenario(cluster, globals_cfg, cfg, ts, args)
            else:
                sweep_remove = run_basic_scenario(cluster, globals_cfg, cfg, ts, args)
            if not args.dry_run:
                rows = write_scenario_csv(os.path.join(ts_dir, "logs", name), cfg, ts, cfg_path,
                                          cluster, globals_cfg.get("mooncake_store_nodes", []))
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
                      extra="--scope full", dry_run=False)
            scp_pull(sweep_remove["host"], nd,
                     "{}/final.remove-keys.log".format(sweep_remove["remote_log_dir"]),
                     os.path.join(ts_dir, "logs"), dry_run=False)
            log("sweep-end remove-keys -> {}".format(
                os.path.join(ts_dir, "logs", "final.remove-keys.log")))
        except Exception as exc:
            log("[WARN] sweep-end remove-keys failed: {}".format(exc))
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
    log("done. results in {}".format(ts_dir))
    # auto-plot: latency/queue-wait figures for every config under this run's
    # logs/ (subprocess isolation — a broken matplotlib must never affect the
    # benchmark result or exit code). Opt out with --no-plot.
    if not args.no_plot:
        plot_script = os.path.join(ROOT, "scripts", "plot_latency.py")
        try:
            proc = subprocess.run(
                [sys.executable, plot_script,
                 "--log", os.path.join(ts_dir, "logs")],
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


if __name__ == "__main__":
    args, cfg_paths = parse_arguments()
    main(args, cfg_paths)
