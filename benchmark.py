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
import sys
import time

from utils import (get_node, load_json, log, parse_data_size, percentile,
                   run_ssh, scp_pull, scp_push, ssh_check_output)

ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIGS_DIR = os.path.join(ROOT, "configs")
SCRIPTS_DIR = os.path.join(ROOT, "scripts")
RESULTS_DIR = os.path.join(ROOT, "outputs")
DEFAULT_LD_PATH = ("/home/w00921547/stress-cluster-bench/lib/:/usr/local/cuda-12.9/lib64:"
                   "/usr/local/lib64:${LD_LIBRARY_PATH:-}")
# store interface families (mirror scripts/mooncake_benchmark_client.py so the
# orchestrator can validate operation values and choose staging behaviour)
PIPELINE_PUT_IFACES = ("put", "put_batch", "put_from")
PIPELINE_GET_IFACES = ("get", "get_batch", "get_into", "get_buffer")
PIPELINE_PUT_BATCH_IFACES = ("put_batch",)
PIPELINE_GET_BATCH_IFACES = ("get_batch",)
BASIC_CSV_HEADER = [
    "scenario", "work_mode", "operation", "role", "node", "instance", "topology", "protocol",
    "num_clients", "num_threads",
    "data_size", "key_count", "batch_size", "put_ratio", "duration", "warmup_duration",
    "writer_count", "reader_count", "notify_count", "notify_threads", "reader_threads",
    "base_qps", "qps_multiple", "target_qps", "notify_pipeline", "traffic_mode",
    "total", "total_bytes", "wall_s",
    "throughput_mb_s", "throughput_gb_s",
    "avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms",
    "ok", "fail", "qps", "fail_rate",
]
PIPELINE_CSV_HEADER = [
    "config_name",
    "scenario", "work_mode", "operation", "role", "node", "instance", "topology", "protocol",
    "num_threads", "notify_threads",
    "data_size", "batch_size", "duration", "warmup_duration",
    "writer_count", "reader_count", "notify_count",
    "base_qps", "qps_multiple", "target_qps", "notify_pipeline", "traffic_mode",
    "total", "success", "fail", "fail_rate",
    "total_bytes", "wall_s", "qps",
    "throughput_mb_s", "throughput_gb_s",
    "avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms",
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


def build_runs(cfg):
    params = cfg["params"]
    return [(int(p["num_clients"]), int(p["num_threads"])) for p in params]


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
    remote_runner = remote_scripts + "/run_benchmark.sh"
    local_log_dir = os.path.join(RESULTS_DIR, ts, "logs", cfg_name)
    return remote_dir, remote_scripts, remote_log_dir, remote_runner, local_log_dir


def build_cluster_session(cluster, globals_cfg, remote_dir):
    master_type = globals_cfg.get("master_type", "") or ""
    session = {
        "MC_MASTER_TYPE": master_type,
        "MC_UB_DEVICE": globals_cfg.get("protocol", {}).get("ub", {}).get("device", ""),
        "MC_VENV": cluster.get("venv", ""),
        "MC_LOCAL_BUFFER_SIZE": str(globals_cfg.get("local_buffer_size", 8589934592)),
        "MC_GLOBAL_SEGMENT_SIZE": str(globals_cfg.get("global_segment_size", 0)),
        "MC_TENANT_ID": globals_cfg.get("tenant_id", "default"),
        "MC_RANDOM_SEED": str(globals_cfg.get("random_seed", 42)),
        "MC_REMOTE_DATA_DIR": globals_cfg.get("remote_data_dir", remote_dir + "/data"),
        # ylt easylog (coro_rpc): level configurable, rotation capped so long
        # runs cannot fill the disk; the path itself is set per-client by
        # run_benchmark.sh (absolute, alongside the client log)
        "MC_YLT_LOG_LEVEL": globals_cfg.get("ylt_log_level", "info"),
        "MC_YLT_LOG_MAX_FILE_SIZE": "52428800",
        "MC_YLT_LOG_MAX_FILES": "3",
        # mooncake per-RPC high-frequency logs (master_rpc_client_breakdown
        # etc., gated by ShouldSampleHiFreqLog) are noisy at full sampling
        # (35-315 lines/s during measured runs). Default 0.01 = 1% sampling:
        # keeps the slow-RPC diagnostic (rpc_call_us anomalies) at ~100x less
        # volume. 1.0 = full trace, 0 = silence. Parsed once at process start,
        # so it must be in env.sh (it is: generated before launch).
        "MC_HIFREQ_LOG_SAMPLE_RATE": str(globals_cfg.get("mc_hifreq_log_sample_rate", 0.01)),
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


def base_scenario_params(cfg, work_mode, topology, primary_node_name):
    return {
        "MC_SCENARIO": cfg["scenario"],
        "MC_WORK_MODE": work_mode,
        "MC_OPERATION": cfg.get("operation", ""),
        "MC_PROTOCOL": cfg["protocol"],
        "MC_KEY_PREFIX": cfg.get("key_prefix", "mc_bench"),
        "MC_KEY_COUNT": str(cfg.get("key_count", 1000)),
        "MC_DATA_SIZE": cfg.get("data_size", "8MB"),
        "MC_BATCH_SIZE": str(cfg.get("batch_size", 1)) if (
            cfg.get("operation") in ("put_batch", "get_batch")
            or any(s in ("put_batch", "get_batch") for s in str(cfg.get("operation", "")).split("/"))) else "",
        "MC_PUT_RATIO": str(cfg.get("put_ratio", "")) if "/" in str(cfg.get("operation", "")) else "",
        "MC_DURATION": str(cfg.get("duration", 0)),
        "MC_WARMUP_KEY_COUNT": str(cfg.get("warmup", {}).get("key_count", cfg.get("warmup_key_count", 100))),
        "MC_WARMUP_DATA_SIZE": cfg.get("warmup", {}).get("data_size", cfg.get("warmup_data_size", "1MB")),
    }


def build_env_content(scenario_params, cluster_session, ld_path, fabric_ip):
    session = dict(cluster_session)
    session["MC_FABRIC_IP"] = fabric_ip
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
                   ld_path, remote_scripts, remote_log_dir, remote_runner, dry_run):
    """Generate a per-node env.sh (node-specific fabric_ip) and push the
    three orchestrator scripts to every involved node."""
    env_sh_path = os.path.join(SCRIPTS_DIR, "env.sh")
    for node_name in involved_nodes:
        n = get_node(cluster, node_name)
        fab = n.get("fabric_ip", n.get("ip", ""))
        with open(env_sh_path, "w", encoding="utf-8", newline="\n") as f:
            f.write(build_env_content(scenario_params, cluster_session, ld_path, fab))
        log("env.sh written (node={}, fabric_ip={})".format(node_name, fab))
        run_ssh(n["ip"], n, "mkdir -p {} {}".format(
            remote_scripts, remote_log_dir), dry_run=dry_run)
        scp_push(n["ip"], n,
                 [env_sh_path,
                  os.path.join(SCRIPTS_DIR, "run_benchmark.sh"),
                  os.path.join(SCRIPTS_DIR, "mooncake_benchmark_client.py")],
                 remote_scripts, dry_run=dry_run)
        run_ssh(n["ip"], n, "chmod +x {} {}".format(
            remote_scripts + "/env.sh", remote_runner), dry_run=dry_run)


def value_check_cmd(remote_data_dir, scenario, data_size_bytes):
    return (
        'python3 -c "import os,sys; p=os.path.expanduser(sys.argv[1]); '
        "print('EXISTS' if os.path.isfile(p) and os.path.getsize(p)==int(sys.argv[2]) "
        "else 'MISSING')\" "
        "{}/{}/value.bin {}".format(remote_data_dir, scenario, data_size_bytes)
    )


def pull_logs(cluster, involved_nodes, remote_log_dir, cfg_name, local_log_dir, dry_run):
    pulled_ips = set()
    for node_name in involved_nodes:
        n = get_node(cluster, node_name)
        if n["ip"] in pulled_ips:
            continue
        pulled_ips.add(n["ip"])
        scp_pull(n["ip"], n, "{}/{}.*".format(remote_log_dir, cfg_name),
                 local_log_dir, dry_run=dry_run)


def wait_process_exit(cluster, node_name, pid, what, timeout_s, dry_run=False):
    """Poll `kill -0 PID` until the remote process exits or times out.

    Logs a single 'waiting' line up front and one result line at the end
    (exited / timeout); per-poll ssh commands are not logged."""
    n = get_node(cluster, node_name)
    log("waiting {} (pid {}) on {} ...".format(what, pid, node_name))
    t0 = time.time()
    for _ in range(int(timeout_s)):
        alive = ssh_check_output(
            n["ip"], n,
            "kill -0 {} 2>/dev/null && echo ALIVE || echo DEAD".format(pid),
            dry_run=dry_run, quiet=True)
        if alive != "ALIVE":
            log("{} exited after {:.0f}s".format(what, time.time() - t0))
            return True
        time.sleep(1)
    log("[WARN] {} (pid {}) still running after {}s (timeout)".format(what, pid, timeout_s))
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


def run_stage(host, node, runner, mode, remote_log_dir, log_prefix, extra=None, dry_run=False):
    remote_log = remote_log_path(remote_log_dir, log_prefix, mode)
    command = "bash {} {} {}".format(runner, mode, remote_log)
    if extra:
        command += " " + extra
    run_ssh(host, node, command, dry_run=dry_run)


# --------------------------------------------------------------------------
# basic scenario (interface-level benchmark: put/get/batch/mixed sweeps)
# --------------------------------------------------------------------------

def run_basic_scenario(cluster, globals_cfg, cfg, ts, args):
    apply_cli_overrides(cfg, args)
    scenario = cfg["scenario"]
    cfg_name = os.path.splitext(os.path.basename(cfg.get("__cfg_path__", scenario)))[0]
    dry = args.dry_run
    op = cfg.get("operation", "")
    work_mode = cfg.get("work_mode", cfg.get("mode", ""))
    primary_node_name = cfg["client_node"]
    involved_nodes = [primary_node_name]
    node = get_node(cluster, primary_node_name)
    host = node["ip"]
    # topology: local iff the primary client node is one of the store nodes
    # (mooncake_store_nodes from global.json)
    store_nodes = globals_cfg.get("mooncake_store_nodes", []) or []
    topology = "local_node" if primary_node_name in store_nodes else "remote_node"
    if cfg.get("duration") and op not in PIPELINE_GET_IFACES:
        raise RuntimeError(
            "duration configured but operation={} does not support duration "
            "stress (only get-family: get/get_batch/get_into/get_buffer)".format(op))
    remote_dir, remote_scripts, remote_log_dir, remote_runner, local_log_dir = \
        scenario_paths(globals_cfg, ts, cfg_name)
    if not dry:
        os.makedirs(local_log_dir, exist_ok=True)
    log("=== scenario {} (op={}, topo={}, protocol={}) primary_node={} ===".format(
        cfg_name, op, topology, cfg["protocol"], host))
    scenario_params = base_scenario_params(cfg, work_mode, topology, primary_node_name)
    cluster_session = build_cluster_session(cluster, globals_cfg, remote_dir)
    ld_path = cluster.get("ld_library_path", DEFAULT_LD_PATH)
    deploy_scripts(cluster, involved_nodes, scenario_params, cluster_session,
                   ld_path, remote_scripts, remote_log_dir, remote_runner, dry)
    # The try MUST start here (before any stage): a failure in prepare-data /
    # clean / prepare-store previously raised before the try block, so the
    # FAIL path never pulled the remote logs — silent failure with no
    # diagnosable evidence. All stages are now covered.
    try:
        data_size_bytes = parse_data_size(cfg.get("data_size", "8MB"))
        check_cmd = value_check_cmd(cluster_session["MC_REMOTE_DATA_DIR"], scenario, data_size_bytes)
        data_status = ssh_check_output(host, node, check_cmd, dry_run=dry)
        if data_status != "EXISTS":
            run_stage(host, node, remote_runner,
                      "prepare-data", remote_log_dir, cfg_name, dry_run=dry)
        run_stage(host, node, remote_runner,
                  "clean", remote_log_dir, cfg_name, extra="--scope full", dry_run=dry)
        if op in PIPELINE_GET_IFACES:
            run_stage(host, node, remote_runner,
                      "prepare-store", remote_log_dir, cfg_name, dry_run=dry)
        warmup = "false" if args.no_warmup else "true"
        runs = build_runs(cfg)
        need_clean_before_run = op in PIPELINE_PUT_IFACES or "/" in op
        log("scenario {}: {} run combos, warmup={}, need_clean_before_run={}".format(
            cfg_name, len(runs), warmup, need_clean_before_run))
        for c, t in runs:
            if need_clean_before_run:
                run_stage(host, node, remote_runner,
                          "clean", remote_log_dir, cfg_name,
                          extra="--scope full", dry_run=dry)
            name = "benchmark_c{}_t{}".format(c, t)
            remote_log = remote_log_path(remote_log_dir, cfg_name, name)
            command = "bash {} benchmark {} --num-clients {} --num-threads {} --warmup {}".format(
                remote_runner, remote_log, c, t, warmup)
            run_ssh(host, node, command, dry_run=dry)
        # Final cleanup after all params complete
        run_stage(host, node, remote_runner,
                  "clean", remote_log_dir, cfg_name, extra="--scope full", dry_run=dry)
        pull_logs(cluster, involved_nodes, remote_log_dir, cfg_name, local_log_dir, dry)
    except Exception:
        # FAIL path: preserve the remote logs for diagnosis, then re-raise
        if not dry:
            fail_cleanup_basic(cluster, involved_nodes, remote_log_dir, cfg_name, local_log_dir)
        raise


# --------------------------------------------------------------------------
# pipeline scenario (producer/consumer: writers notify random reader subsets)
# --------------------------------------------------------------------------

def run_pipeline_scenario(cluster, globals_cfg, cfg, ts, args):
    apply_cli_overrides(cfg, args)
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
    # topology: local iff the primary client node is one of the store nodes
    # (mooncake_store_nodes from global.json)
    store_nodes = globals_cfg.get("mooncake_store_nodes", []) or []
    topology = "local_node" if primary_node_name in store_nodes else "remote_node"
    remote_dir, remote_scripts, remote_log_dir, remote_runner, local_log_dir = \
        scenario_paths(globals_cfg, ts, cfg_name)
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
        # clear basic-only vars that are meaningless for pipeline
        "MC_OPERATION": "",
        "MC_KEY_COUNT": "",
        "MC_WARMUP_KEY_COUNT": "",
        "MC_WARMUP_DATA_SIZE": "",
        "MC_WARMUP_DURATION": str(cfg.get("warmup", {}).get("duration", 0)),
    })
    cluster_session = build_cluster_session(cluster, globals_cfg, remote_dir)
    ld_path = cluster.get("ld_library_path", DEFAULT_LD_PATH)
    deploy_scripts(cluster, involved_nodes, scenario_params, cluster_session,
                   ld_path, remote_scripts, remote_log_dir, remote_runner, dry)
    reader_peers = []
    reader_pids = []
    writer_pids = []
    try:
        data_size_bytes = parse_data_size(cfg.get("data_size", "8MB"))
        check_cmd = value_check_cmd(cluster_session["MC_REMOTE_DATA_DIR"], scenario, data_size_bytes)
        # value.bin must exist on EVERY node that runs a writer or reader
        for node_name in involved_nodes:
            n = get_node(cluster, node_name)
            data_status = ssh_check_output(n["ip"], n, check_cmd, dry_run=dry)
            if data_status != "EXISTS":
                run_stage(n["ip"], n, remote_runner,
                          "prepare-data", remote_log_dir, cfg_name, dry_run=dry)
        run_stage(host, node, remote_runner,
                  "clean", remote_log_dir, cfg_name, extra="--scope full", dry_run=dry)
        warmup = "false" if args.no_warmup else "true"
        reader_threads = plan["reader_threads"]
        log("scenario {} (pipeline, {}): writers={}, readers={}, notify_count={}, "
            "total_qps={}, per_writer_qps={}, traffic={}".format(
                cfg_name, work_mode, len(plan["writers"]), len(plan["readers"]),
                plan["notify_count"], plan["total_qps"], plan["per_writer_qps"],
                cfg.get("traffic_mode", "pulse")))
        # Reader peers: host resolved from each reader node's fabric_ip so the
        # same config works unchanged on any deployment.
        for r in plan["readers"]:
            rn = get_node(cluster, r["node"])
            reader_peers.append({
                "role": "reader",
                "host": rn.get("fabric_ip", rn.get("ip", "")),
                "port": r["port"],
                "node": r["node"],
            })
        peers_json = json.dumps(reader_peers)
        run_name = "benchmark_c1_t{}".format(reader_threads)
        base_noext = remote_log_path(remote_log_dir, cfg_name, run_name).replace(".log", "")
        # Step 1: start all readers (one process per reader, unique port and id)
        for r in plan["readers"]:
            rn = get_node(cluster, r["node"])
            merged_log = "{}.reader_port{}.log".format(base_noext, r["port"])
            nohup_log = "{}.reader_port{}.nohup".format(base_noext, r["port"])
            rcmd = "nohup bash {} pipeline {} --client-id {} --num-threads {} --role reader --listen-port {} --peers '{}' --warmup false --timeout 300 --operation {} > {} 2>&1 & echo $!".format(
                remote_runner, merged_log, r["index"], reader_threads,
                r["port"], peers_json, reader_op, nohup_log)
            pid = ssh_check_output(rn["ip"], rn, rcmd, dry_run=dry)
            reader_pids.append((r, pid))
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
        for w in plan["writers"]:
            wn = get_node(cluster, w["node"])
            merged_log = "{}.writer{}.log".format(base_noext, w["gid"])
            nohup_log = "{}.writer{}.nohup".format(base_noext, w["gid"])
            wcmd = "nohup bash {} pipeline {} --client-id {} --role writer --peers '{}' --warmup {} --operation {} > {} 2>&1 & echo $!".format(
                remote_runner, merged_log, w["gid"], peers_json, warmup, writer_op, nohup_log)
            pid = ssh_check_output(wn["ip"], wn, wcmd, dry_run=dry)
            writer_pids.append((w, pid))
        # Step 4: wait for all writers to finish (warmup + measured run)
        warmup_duration = float(cfg.get("warmup", {}).get("duration", 0) or 0)
        duration = float(cfg.get("duration", 30))
        for w, pid in writer_pids:
            if not dry and pid:
                wait_process_exit(cluster, w["node"], pid,
                                  "writer w{}".format(w["gid"]),
                                  int(warmup_duration + duration) + 300, dry_run=dry)
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
                                  60, dry_run=dry)
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
        pull_logs(cluster, involved_nodes, remote_log_dir, cfg_name, local_log_dir, dry)
    except Exception:
        # FAIL path: preserve every remote log (reader SUMMARY/RESULT is only
        # flushed on exit) and free ports, then re-raise for main's [FAIL]
        if not dry:
            fail_cleanup_pipeline(cluster, plan, reader_peers, reader_pids, writer_pids,
                                  involved_nodes, remote_log_dir, cfg_name, local_log_dir)
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
                if len(parts) < 5:
                    continue
                if op_filter is not None and parts[1] != op_filter:
                    continue
                # key_count (pipeline): writer 6th field, reader 7th field;
                # basic lines and old logs have no key_count -> 1
                kc = 1
                try:
                    if parts[1] == "pipeline_write" and len(parts) > 5:
                        kc = int(parts[5])
                    elif parts[1] == "pipeline_read" and len(parts) > 6:
                        kc = int(parts[6])
                except ValueError:
                    pass
                # latency statistics count successful samples only; totals are
                # key-based for pipeline (kc per RESULT line)
                if parts[4] == "OK":
                    ok += kc
                    latencies.append(float(parts[3]))
                    lat_sum_ms += float(parts[3])
                else:
                    fail += kc
                if len(parts) > 5 and parts[1] == "pipeline_read":
                    try:
                        queue_waits.append(float(parts[5]))
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
        "lat_sum_ms": lat_sum_ms,
        "ok": ok,
        "fail": fail,
        "qps": (total / wall) if wall > 0 else 0.0,
        "fail_rate": (fail / total) if total else 0.0,
        "queue_wait_avg_ms": (sum(queue_waits) / len(queue_waits)) if queue_waits else 0.0,
        "queue_wait_max_ms": max(queue_waits) if queue_waits else 0.0,
        "max_depth": max_depth,
    }


def write_scenario_csv(local_log_dir, cfg, ts, cfg_path, store_nodes=None):
    store_nodes = store_nodes or []
    name = os.path.splitext(os.path.basename(cfg_path))[0]
    out_csv = os.path.join(RESULTS_DIR, ts, "{}.csv".format(name))
    rows = []
    is_pipeline = cfg.get("scenario") == "pipeline"
    if is_pipeline:
        plan = pipeline_plan(cfg)
        data_size_bytes = parse_data_size(cfg.get("data_size", "8MB"))
        batch_size = int(cfg.get("batch_size", 1))
        duration = float(cfg.get("duration", 0) or 0)
        writer_files = {}
        for p in sorted(glob.glob(os.path.join(local_log_dir, "{}*writer*".format(name)))):
            if os.path.isdir(p) or p.endswith(".nohup"):
                continue
            try:
                gid = int(os.path.basename(p).replace(".log", "").split(".writer")[-1])
            except ValueError:
                continue
            writer_files[gid] = p
        reader_files = {}
        for p in sorted(glob.glob(os.path.join(local_log_dir, "{}*reader_port*".format(name)))):
            if os.path.isdir(p) or p.endswith(".nohup"):
                continue
            try:
                port = int(os.path.basename(p).replace(".log", "").split(".reader_port")[-1])
            except ValueError:
                continue
            reader_files[port] = p

        def base_row(role, instance, nodename):
            if role == "writer":
                op = (cfg.get("writer", {}) or {}).get("operation", "")
            else:
                op = (cfg.get("reader", {}) or {}).get("operation", "")
            w_cpn = (cfg.get("writer", {}) or {}).get("count_per_node", 1)
            r_cpn = (cfg.get("reader", {}) or {}).get("count_per_node", 1)
            return {
                "config_name": name,
                "scenario": cfg["scenario"],
                "work_mode": cfg.get("work_mode", cfg.get("mode", "")),
                "operation": op,
                "role": role,
                "node": nodename,
                "instance": instance,
                "topology": ("local_node" if nodename in store_nodes else "remote_node") if nodename else "",
                "protocol": cfg["protocol"],
                "num_threads": plan["reader_threads"] if role == "reader" else "",
                "notify_threads": plan["notify_threads"] if role == "writer" else "",
                "data_size": cfg.get("data_size", ""),
                "batch_size": cfg.get("batch_size", ""),
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
            for k in ("avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms"):
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

        for w in plan["writers"]:
            f = writer_files.get(w["gid"])
            if not f:
                continue
            agg = parse_benchmark_log(f, op_filter="pipeline_write")
            row = base_row("writer", "w{}".format(w["gid"]), w["node"])
            metrics_into(row, agg, data_size_bytes)
            rows.append(row)
        # writer TOTAL row: all writers aggregated; throughput = sum of each
        # writer's busy-time throughput
        if writer_files:
            agg = parse_benchmark_log(list(writer_files.values()), op_filter="pipeline_write")
            row = base_row("writer", "TOTAL", "")
            metrics_into(row, agg, data_size_bytes,
                         throughput_mb=busy_throughput_mb(
                             writer_files.values(), "pipeline_write", data_size_bytes))
            rows.append(row)
        for r in plan["readers"]:
            f = reader_files.get(r["port"])
            if not f:
                continue
            agg = parse_benchmark_log(f, op_filter="pipeline_read")
            row = base_row("reader", "r{}".format(r["index"]), r["node"])
            metrics_into(row, agg, data_size_bytes, with_queue=True)
            rows.append(row)
        # TOTAL row: all readers aggregated (aligns with the datasystem
        # single GET row per test group); throughput = sum of each reader's
        # busy-time throughput
        if reader_files:
            agg = parse_benchmark_log(list(reader_files.values()), op_filter="pipeline_read")
            row = base_row("reader", "TOTAL", "")
            metrics_into(row, agg, data_size_bytes, with_queue=True,
                         throughput_mb=busy_throughput_mb(
                             reader_files.values(), "pipeline_read", data_size_bytes))
            rows.append(row)
    else:
        pattern = os.path.join(local_log_dir, "{}.*.log".format(name))
        def fill_basic_row(row, agg, op_label):
            row["operation"] = op_label
            row.update({k: agg[k] for k in (
                "total", "total_bytes", "wall_s", "throughput_mb_s", "throughput_gb_s",
                "avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms", "ok", "fail",
                "qps", "fail_rate")})
            # throughput recomputed from this row's own bytes/wall so split
            # (mixed) rows never inherit combined SUMMARY bytes
            row["throughput_mb_s"] = (row["total_bytes"] / row["wall_s"] / 1024.0 ** 2) if row["wall_s"] > 0 else 0.0
            row["throughput_gb_s"] = (row["total_bytes"] / row["wall_s"] / 1024.0 ** 3) if row["wall_s"] > 0 else 0.0
            for k in ("avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms", "wall_s"):
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
        data_size_bytes = parse_data_size(cfg.get("data_size", "8MB"))
        for path in sorted(glob.glob(pattern)):
            base = os.path.basename(path)
            try:
                c_part = base.split("benchmark_c")[1].split("_t")[0]
                t_part = base.split("_t")[1].replace(".log", "")
                c, t = int(c_part), int(t_part)
            except (IndexError, ValueError):
                continue
            topo = "local_node" if cfg["client_node"] in store_nodes else "remote_node"
            base_row_vals = {
                "scenario": cfg["scenario"],
                "work_mode": cfg.get("work_mode", cfg.get("mode", "")),
                "operation": op_label,
                "role": "",
                "node": "",
                "instance": "",
                "topology": topo,
                "protocol": cfg["protocol"],
                "num_clients": c,
                "num_threads": t,
                "data_size": cfg.get("data_size", ""),
                "key_count": cfg.get("key_count", ""),
                "batch_size": cfg.get("batch_size", "") if (op_label in ("put_batch", "get_batch") or
                                                            put_iface in ("put_batch",) or
                                                            get_iface in ("get_batch",)) else "",
                "put_ratio": cfg.get("put_ratio", "") if "/" in op_label else "",
                "duration": cfg.get("duration", ""),
                "warmup_duration": "",
                "writer_count": "",
                "reader_count": "",
                "notify_count": "",
                "notify_threads": "",
                "reader_threads": "",
                "base_qps": "",
                "qps_multiple": "",
                "target_qps": "",
                "notify_pipeline": "",
                "traffic_mode": "",
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
                    agg = dict(agg, total_bytes=int(agg["ok"] * bp * data_size_bytes))
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
    parser.add_argument("--scenario", default=None,
                        choices=["basic", "pipeline"],
                        help="scenario category: basic (interface tests) or pipeline (recall/refine)")
    parser.add_argument("--config", "-f", default=None,
                        help="full path to scenario config json, e.g. basic/get_remote.json or basic/demo/simple_get.json")
    parser.add_argument("--all", action="store_true",
                        help="run all configs in the selected scenario category (excludes demo/ subdir)")
    parser.add_argument("--demo", action="store_true",
                        help="run all demo configs in the selected scenario category's demo/ subdir")
    parser.add_argument("--no-warmup", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--protocol", default=None, choices=["tcp", "rdma", "ub"],
                        help="override protocol (tcp/rdma/ub); highest priority, "
                             "overrides the JSON config value")
    parser.add_argument("--clean-up", action="store_true",
                        help="remove the remote test directory on every node in "
                             "cluster.json. Standalone: only cleanup and exit. "
                             "Combined with --all/--demo/--config: cleanup after the run.")
    args = parser.parse_args(argv)
    if not os.path.exists(args.cluster):
        example = os.path.join(CONFIGS_DIR, "cluster.example.json")
        sys.exit("[ERROR] cluster config not found: {}\n"
                 "       copy {} to {} and edit it first.".format(
                     args.cluster, example, os.path.join(CONFIGS_DIR, "cluster.json")))
    if not os.path.exists(args.global_cfg):
        sys.exit("[ERROR] global config not found: {}".format(args.global_cfg))
    scenario_category = args.scenario
    cfg_path = None
    if args.config:
        cfg_path = os.path.join(CONFIGS_DIR, args.config)
        if scenario_category is None:
            scenario_category = args.config.replace("\\", "/").split("/")[0]
    elif args.all or args.demo:
        if scenario_category is None:
            sys.exit("[ERROR] --all/--demo requires --scenario")
    elif not args.clean_up:
        sys.exit("[ERROR] provide --scenario with --all or --demo or --config, "
                 "or --clean-up for a standalone cluster cleanup")
    if args.scenario and not args.all and not args.config and not args.demo:
        sys.exit("[ERROR] --scenario provided but none of --all/--demo/--config is given")
    cfg_paths = []
    if args.config:
        if not os.path.exists(cfg_path):
            sys.exit("[ERROR] config not found: {}".format(cfg_path))
        cfg_paths = [cfg_path]
    elif args.all:
        scenario_dir = os.path.join(CONFIGS_DIR, scenario_category)
        for f in sorted(os.listdir(scenario_dir)):
            if f.endswith(".json"):
                cfg_paths.append(os.path.join(scenario_dir, f))
    elif args.demo:
        scenario_dir = os.path.join(CONFIGS_DIR, scenario_category, "demo")
        if not os.path.isdir(scenario_dir):
            sys.exit("[ERROR] demo directory not found: {}".format(scenario_dir))
        for f in sorted(os.listdir(scenario_dir)):
            if f.endswith(".json"):
                cfg_paths.append(os.path.join(scenario_dir, f))
    return args, cfg_paths


def main(args, cfg_paths):
    cluster = load_json(args.cluster)
    globals_cfg = load_json(args.global_cfg)
    _validate_master_mode(globals_cfg)
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
    for cfg_path in cfg_paths:
        cfg = load_json(cfg_path)
        cfg["__cfg_path__"] = cfg_path
        name = os.path.splitext(os.path.basename(cfg_path))[0]
        try:
            if cfg.get("scenario") == "pipeline":
                run_pipeline_scenario(cluster, globals_cfg, cfg, ts, args)
            else:
                run_basic_scenario(cluster, globals_cfg, cfg, ts, args)
            if not args.dry_run:
                rows = write_scenario_csv(os.path.join(ts_dir, "logs", name), cfg, ts, cfg_path,
                                          globals_cfg.get("mooncake_store_nodes", []))
                if cfg.get("scenario") == "pipeline":
                    categories.add("pipeline")
                    # detail keeps only the aggregated writer/reader TOTAL rows
                    all_rows.extend([r for r in rows if r.get("instance") == "TOTAL"])
                else:
                    categories.add("basic")
                    all_rows.extend(rows)
        except Exception as exc:
            log("[FAIL] scenario {}: {}".format(name, exc))
            failed_scenarios.append(name)
    if args.dry_run:
        log("dry-run complete, no artifacts written")
        return
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
    if args.clean_up:
        cleanup_cluster(cluster, globals_cfg, dry_run=False)
        log("cleanup done")


if __name__ == "__main__":
    args, cfg_paths = parse_arguments()
    main(args, cfg_paths)
