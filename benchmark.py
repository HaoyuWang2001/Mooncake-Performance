#!/usr/bin/env python3
import argparse
import csv
import glob
import json
import os
import subprocess
import sys
import time
ROOT = os.path.dirname(os.path.abspath(__file__))
CONFIGS_DIR = os.path.join(ROOT, "configs")
SCRIPTS_DIR = os.path.join(ROOT, "scripts")
RESULTS_DIR = os.path.join(ROOT, "outputs")
CSV_HEADER = [
    "scenario", "mode", "operation", "topology", "protocol",
    "num_clients", "num_threads",
    "data_size", "key_count", "batch_size", "put_ratio", "duration",
    "writer_count", "reader_count", "target_qps",
    "ttl", "notify_count", "notify_pipeline", "traffic_mode",
    "total", "total_bytes", "wall_s",
    "throughput_mb_s", "throughput_gb_s",
    "avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms",
    "ok", "fail", "qps", "fail_rate",
]
OVERVIEW_HEADER = CSV_HEADER
def log(msg):
    print("[benchmark] {}".format(msg), flush=True)
def _node(cluster, name):
    node = cluster.get("nodes", {}).get(name)
    if node is None:
        raise RuntimeError("unknown node in cluster.json: {}".format(name))
    return node
def _ssh_args(node):
    args = ["ssh", "-q", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
            "-o", "ConnectTimeout=15"]
    port = int(node.get("ssh_port", 22))
    if port != 22:
        args += ["-p", str(port)]
    key = node.get("ssh_key") or ""
    if key:
        args += ["-i", key]
    args.append("{}@{}".format(node.get("ssh_user", "root"), node["ip"]))
    return args
def _scp_args(node):
    args = ["scp", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes", "-o", "LogLevel=QUIET"]
    port = int(node.get("ssh_port", 22))
    if port != 22:
        args += ["-P", str(port)]
    key = node.get("ssh_key") or ""
    if key:
        args += ["-i", key]
    return args
def run_ssh(cluster, host, node, command, dry_run=False):
    cmd = _ssh_args(node) + [command]
    log("[ssh] {} : {}".format(host, command))
    if dry_run:
        log("      (dry-run, skipped)")
        return
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise RuntimeError("ssh command failed (rc={}) on {}: {}".format(
            result.returncode, host, command))
def ssh_check_output(cluster, host, node, command, dry_run=False):
    cmd = _ssh_args(node) + [command]
    log("[ssh] {} : {}".format(host, command))
    if dry_run:
        log("      (dry-run, skipped)")
        return ""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError("ssh command failed (rc={}) on {}: {}".format(
            result.returncode, host, command))
    return result.stdout.strip()
def scp_push(cluster, host, node, local_files, remote_path, dry_run=False):
    cmd = _scp_args(node) + list(local_files) + \
        ["{}@{}:{}".format(node.get("ssh_user", "root"), host, remote_path)]
    log("[scp] -> {} : {}".format(host, remote_path))
    if dry_run:
        log("      (dry-run, skipped)")
        return
    subprocess.run(cmd, check=True)
def scp_pull(cluster, host, node, remote_glob, local_dir, dry_run=False):
    cmd = _scp_args(node) + ["-r"] + \
        ["{}@{}:{}".format(node.get("ssh_user", "root"), host, remote_glob),
         local_dir.rstrip("\\/") + os.sep]
    log("[scp] <- {} : {}".format(host, remote_glob))
    if dry_run:
        log("      (dry-run, skipped)")
        return
    subprocess.run(cmd, check=True)
def build_runs(cfg):
    params = cfg["params"]
    return [(int(p["num_clients"]), int(p["num_threads"])) for p in params]
def remote_log_path(remote_log_dir, scenario, name):
    return "{}/{}.{}.log".format(remote_log_dir, scenario, name)
def parse_data_size(data_size_str):
    data_size_str = data_size_str.strip().upper()
    if data_size_str.endswith("KB"):
        return int(float(data_size_str[:-2]) * 1024)
    elif data_size_str.endswith("MB"):
        return int(float(data_size_str[:-2]) * 1024 * 1024)
    elif data_size_str.endswith("GB"):
        return int(float(data_size_str[:-2]) * 1024 * 1024 * 1024)
    elif data_size_str.endswith("B"):
        return int(float(data_size_str[:-1]))
    else:
        return int(float(data_size_str))
def run_stage(cluster, globals_cfg, host, node, runner, mode, remote_log_dir, log_prefix, extra=None, dry_run=False):
    remote_log = remote_log_path(remote_log_dir, log_prefix, mode)
    command = "bash {} {} 1 1 {}".format(runner, mode, remote_log)
    if extra:
        command += " " + extra
    run_ssh(cluster, host, node, command, dry_run=dry_run)
def run_scenario(cluster, globals_cfg, cfg, ts, args):
    if getattr(args, "protocol", None):
        cfg["protocol"] = args.protocol
    if getattr(args, "key_count", None):
        cfg["key_count"] = args.key_count
    if getattr(args, "data_size", None):
        cfg["data_size"] = args.data_size
    scenario = cfg["scenario"]
    cfg_name = os.path.splitext(os.path.basename(cfg.get("__cfg_path__", scenario)))[0]
    node = _node(cluster, cfg["client_node"])
    host = node["ip"]
    dry = args.dry_run
    op = cfg["operation"]
    mode = cfg.get("mode", "standard")
    is_pipeline = cfg.get("scenario") == "pipeline"
    # topology: dynamic from client_node vs store_node
    topology = "local_node" if cfg["client_node"] == cfg.get("store_node", cfg["client_node"]) else "remote_node"
    if cfg.get("duration") and not is_pipeline and op not in ("get", "get_batch"):
        raise RuntimeError(
            "duration configured but operation={} does not support duration "
            "stress (only get/get_batch)".format(op))
    remote_dir = globals_cfg["remote_dir"]
    remote_scripts = remote_dir + "/scripts"
    remote_log_dir = "{}/{}/logs".format(globals_cfg["remote_results_dir"], ts)
    remote_runner = remote_scripts + "/run_benchmark.sh"
    local_log_dir = os.path.join(RESULTS_DIR, ts, "logs", cfg_name)
    if not dry:
        os.makedirs(local_log_dir, exist_ok=True)
    log("=== scenario {} (op={}, topo={}, protocol={}) client_node={} ===".format(
        cfg_name, op, topology, cfg["protocol"], host))
    scenario_params = {
        "MC_SCENARIO": scenario,
        "MC_SCENARIO_MODE": cfg.get("mode", ""),
        "MC_MODE": cfg.get("mode", ""),
        "MC_OPERATION": cfg.get("operation", ""),
        "MC_TOPOLOGY": topology,
        "MC_PROTOCOL": cfg["protocol"],
        "MC_CLIENT_NODE": cfg["client_node"],
        "MC_KEY_PREFIX": cfg.get("key_prefix", "mc_bench"),
        "MC_KEY_COUNT": str(cfg.get("key_count", 1000)),
        "MC_DATA_SIZE": cfg.get("data_size", "8MB"),
        "MC_BATCH_SIZE": str(cfg.get("batch_size", 1)) if cfg.get("operation") in ("put_batch", "get_batch") else "",
        "MC_PUT_RATIO": str(cfg.get("put_ratio", "")) if cfg.get("operation") in ("mixed", "put/get") else "",
        "MC_DURATION": str(cfg.get("duration", 0)),
        "MC_WARMUP_KEY_COUNT": str(cfg.get("warmup", {}).get("key_count", cfg.get("warmup_key_count", 100))),
        "MC_WARMUP_DATA_SIZE": cfg.get("warmup", {}).get("data_size", cfg.get("warmup_data_size", "1MB")),
        "MC_PARAMS": json.dumps(cfg.get("params", [])),
    }
    if is_pipeline:
        scenario_params.update({
            "MC_WORK_MODE": cfg.get("mode", ""),
            "MC_WRITER_COUNT": str(cfg.get("writer_count", 1)),
            "MC_READER_COUNT": str(cfg.get("reader_count", 1)),
            "MC_TARGET_QPS": str(cfg.get("target_qps", 200)),
            "MC_TTL": str(cfg.get("ttl", 0)),
            "MC_NOTIFY_COUNT": str(cfg.get("notify_count", 1)),
            "MC_NOTIFY_PIPELINE": ",".join(cfg.get("notify_pipeline", [])),
            "MC_TRAFFIC_MODE": cfg.get("traffic_mode", "pulse"),
            "MC_DURATION": str(cfg.get("duration", 30)),
            "MC_BATCH_SIZE": str(cfg.get("batch_size", 1)),
        })
    client_node_name = cfg["client_node"]
    client_node_cfg = cluster["nodes"][client_node_name]
    fabric_ip = client_node_cfg.get("fabric_ip", client_node_cfg.get("ip", ""))
    cluster_session = {
        "MC_MASTER_ADDR": globals_cfg.get("master_addr", ""),
        "MC_METADATA_SERVER": globals_cfg.get("metadata_server", ""),
        "MC_PROTOCOL_UB_DEVICE": globals_cfg.get("protocol", {}).get("ub", {}).get("device", ""),
        "MC_VENV": cluster.get("venv", ""),
        "MC_FABRIC_IP": fabric_ip,
        "MC_LOCAL_BUFFER_SIZE": str(globals_cfg.get("local_buffer_size", 8589934592)),
        "MC_GLOBAL_SEGMENT_SIZE": str(globals_cfg.get("global_segment_size", 0)),
        "MC_TENANT_ID": globals_cfg.get("tenant_id", "default"),
        "MC_RANDOM_SEED": str(globals_cfg.get("random_seed", 42)),
        "MC_REMOTE_DIR": remote_dir,
        "MC_REMOTE_DATA_DIR": globals_cfg.get("remote_data_dir", remote_dir + "/data"),
        "MC_REMOTE_RESULTS_DIR": globals_cfg.get("remote_results_dir", remote_dir + "/results"),
    }
    runtime = {
        "MC_TIMESTAMP": ts,
        "MC_LOG_DIR": remote_log_dir,
    }
    env_lines = ["#!/usr/bin/env bash"]
    env_lines.append("")
    env_lines.append("# scenario params")
    for k, v in sorted(scenario_params.items()):
        if v:
            env_lines.append('export {}="{}"'.format(k, v))
    env_lines.append("")
    env_lines.append("# cluster / session")
    for k, v in sorted(cluster_session.items()):
        if k in ("MC_PARAMS", "MC_PEERS") and isinstance(v, str) and v.startswith("{"):
            env_lines.append("export {}='{}'".format(k, v))
        else:
            env_lines.append('export {}="{}"'.format(k, v))
    env_lines.append("")
    env_lines.append("# runtime")
    for k, v in sorted(runtime.items()):
        env_lines.append('export {}="{}"'.format(k, v))
    env_lines.append("")
    ld_path = cluster.get("ld_library_path",
                          "/home/w00921547/stress-cluster-bench/lib/:/usr/local/cuda-12.9/lib64:/usr/local/lib64:${LD_LIBRARY_PATH:-}")
    env_lines.append('export LD_LIBRARY_PATH={}'.format(ld_path))
    env_lines.append("")
    env_content = "\n".join(env_lines) + "\n"
    env_sh_path = os.path.join(SCRIPTS_DIR, "env.sh")
    with open(env_sh_path, "w", encoding="utf-8", newline="\n") as f:
        f.write(env_content)
    log("env.sh written -> {}".format(env_sh_path))
    run_ssh(cluster, host, node, "mkdir -p {} {}".format(
        remote_scripts, remote_log_dir), dry_run=dry)
    scp_push(cluster, host, node,
             [os.path.join(SCRIPTS_DIR, "env.sh"),
              os.path.join(SCRIPTS_DIR, "run_benchmark.sh"),
              os.path.join(SCRIPTS_DIR, "mooncake_benchmark_client.py")],
             remote_scripts, dry_run=dry)
    run_ssh(cluster, host, node, "chmod +x {} {}".format(
        remote_scripts + "/env.sh", remote_runner), dry_run=dry)
    data_size_bytes = parse_data_size(cfg.get("data_size", "8MB"))
    remote_data_dir = cluster_session["MC_REMOTE_DATA_DIR"]
    check_cmd = (
        'python3 -c "import os,sys; p=os.path.expanduser(sys.argv[1]); '
        "print('EXISTS' if os.path.isfile(p) and os.path.getsize(p)==int(sys.argv[2]) "
        "else 'MISSING')\" "
        "{}/{}/value.bin {}".format(remote_data_dir, scenario, data_size_bytes)
    )
    data_status = ssh_check_output(cluster, host, node, check_cmd, dry_run=dry)
    if is_pipeline:
        if data_status != "EXISTS":
            run_stage(cluster, globals_cfg, host, node, remote_runner,
                      "prepare-data", remote_log_dir, cfg_name, dry_run=dry)
        run_stage(cluster, globals_cfg, host, node, remote_runner,
                  "clean", remote_log_dir, cfg_name, extra="--scope full", dry_run=dry)
        peers = cfg.get("peers", [])
        for p in peers:
            pnode = _node(cluster, p.get("node", cfg["client_node"]))
            p["host"] = pnode.get("fabric_ip", pnode.get("ip", p.get("host", "")))
        reader_peers = [p for p in peers if p.get("role") == "reader"]
        writer_peers = [p for p in peers if p.get("role") == "writer"]
        all_peer_nodes = []
        seen_ips = set()
        for p in peers:
            pn = _node(cluster, p.get("node", cfg["client_node"]))
            if pn["ip"] not in seen_ips:
                seen_ips.add(pn["ip"])
                all_peer_nodes.append(pn)
        warmup = "false" if args.no_warmup else "true"
        runs = build_runs(cfg)
        log("scenario {} (pipeline, {}): {} run combos, warmup={}, readers={}, writers={}".format(
            cfg_name, cfg.get("mode", "recall"), len(runs), warmup,
            len(reader_peers), len(writer_peers)))
        for c, t in runs:
            name = "benchmark_c{}_t{}".format(c, t)
            remote_log_base = remote_log_path(remote_log_dir, cfg_name, name)
            # Step 0: ensure scripts exist on reader nodes (may differ from host)
            reader_nodes_seen = set()
            for rp in reader_peers:
                rn = _node(cluster, rp["node"])
                rn_ip = rn["ip"]
                if rn_ip != host and rn_ip not in reader_nodes_seen:
                    reader_nodes_seen.add(rn_ip)
                    rn_scripts = remote_dir + "/scripts"
                    run_ssh(cluster, rn_ip, rn, "mkdir -p {}".format(rn_scripts), dry_run=dry)
                    scp_push(cluster, rn_ip, rn,
                             [os.path.join(SCRIPTS_DIR, "env.sh"),
                              os.path.join(SCRIPTS_DIR, "run_benchmark.sh"),
                              os.path.join(SCRIPTS_DIR, "mooncake_benchmark_client.py")],
                             rn_scripts, dry_run=dry)
                    run_ssh(cluster, rn_ip, rn,
                            "chmod +x {}/env.sh {}/run_benchmark.sh".format(rn_scripts, rn_scripts),
                            dry_run=dry)
            # Step 1: start all readers
            reader_pids = []
            for rp in reader_peers:
                rnode = _node(cluster, rp["node"])
                rlog = "{}.reader_port{}".format(remote_log_base, rp["port"])
                rcmd = "nohup bash {} pipeline {} {} {} --role reader --listen-port {} --peers '{}' --warmup false --timeout 300 > {} 2>&1 & echo $!".format(
                    remote_runner, c, t, rlog.replace(".log", ""),
                    rp["port"], json.dumps(peers), rlog)
                pid = ssh_check_output(cluster, rnode["ip"], rnode, rcmd, dry_run=dry)
                reader_pids.append((rp, pid))
            # Step 2: wait for all readers to be healthy
            for rp in reader_peers:
                rnode = _node(cluster, rp["node"])
                rlog = "{}.reader_port{}".format(remote_log_base, rp["port"])
                health_url = "http://{}:{}/health".format(rp["host"], rp["port"])
                HEALTHY = False
                for attempt in range(10):
                    health_cmd = "curl -s --noproxy '*' --connect-timeout 2 --max-time 2 {} || echo '__NOT_READY__'".format(health_url)
                    result = ssh_check_output(cluster, rnode["ip"], rnode, health_cmd, dry_run=dry) if not dry else "HEALTHY"
                    if "HEALTHY" in result:
                        HEALTHY = True
                        break
                    time.sleep(1)
                if not dry:
                    if not HEALTHY:
                        err_msg = "reader at {}:{} not ready after 30s".format(rp["host"], rp["port"])
                        log("[ERROR] " + err_msg)
                        raise RuntimeError(err_msg)
                    else:
                        log("reader at {}:{}=READY".format(rp["host"], rp["port"]))
            # Step 3: start all writers
            for wp in writer_peers:
                wnode = _node(cluster, wp["node"])
                wlog = "{}.writer_port{}".format(remote_log_base, wp["port"])
                wcmd = "bash {} pipeline {} {} {} --role writer --listen-port {} --peers '{}' --warmup {}".format(
                    remote_runner, c, t, wlog, wp["port"], json.dumps(peers), warmup)
                run_ssh(cluster, wnode["ip"], wnode, wcmd, dry_run=dry)
            # Step 4: shutdown readers and wait for their final logs
            for rp in reader_peers:
                rnode = _node(cluster, rp["node"])
                shutdown_url = "http://{}:{}/shutdown".format(rp["host"], rp["port"])
                curl_cmd = "curl -s --noproxy '*' --connect-timeout 2 --max-time 5 -X POST {} || true".format(shutdown_url)
                if not dry:
                    ssh_check_output(cluster, rnode["ip"], rnode, curl_cmd, dry_run=dry)
            # Wait for all reader processes to exit (flush final SUMMARY/RESULT)
            for rp, pid in reader_pids:
                rnode = _node(cluster, rp["node"])
                if not dry and pid:
                    for _ in range(60):
                        alive = ssh_check_output(
                            cluster, rnode["ip"], rnode,
                            "kill -0 {} 2>/dev/null && echo ALIVE || echo DEAD".format(pid),
                            dry_run=dry)
                        if alive != "ALIVE":
                            break
                        time.sleep(1)
    else:
        if data_status != "EXISTS":
            run_stage(cluster, globals_cfg, host, node, remote_runner,
                      "prepare-data", remote_log_dir, cfg_name, dry_run=dry)
        run_stage(cluster, globals_cfg, host, node, remote_runner,
                  "clean", remote_log_dir, cfg_name, extra="--scope full", dry_run=dry)
        if op in ("get", "get_batch"):
            run_stage(cluster, globals_cfg, host, node, remote_runner,
                      "prepare-store", remote_log_dir, cfg_name, dry_run=dry)
        warmup = "false" if args.no_warmup else "true"
        runs = build_runs(cfg)
        need_clean_before_run = op in ("put", "put_batch") or "/" in op
        log("scenario {}: {} run combos, warmup={}, need_clean_before_run={}".format(
            cfg_name, len(runs), warmup, need_clean_before_run))
        for c, t in runs:
            if need_clean_before_run:
                run_stage(cluster, globals_cfg, host, node, remote_runner,
                          "clean", remote_log_dir, cfg_name,
                          extra="--scope full", dry_run=dry)
            name = "benchmark_c{}_t{}".format(c, t)
            remote_log = remote_log_path(remote_log_dir, cfg_name, name)
            command = "bash {} benchmark {} {} {} --warmup {}".format(
                remote_runner, c, t, remote_log, warmup)
            run_ssh(cluster, host, node, command, dry_run=dry)
        # Final cleanup after all params complete
        run_stage(cluster, globals_cfg, host, node, remote_runner,
                  "clean", remote_log_dir, cfg_name, extra="--scope full", dry_run=dry)
    if is_pipeline:
        pulled_ips = set()
        for pnode in all_peer_nodes:
            if pnode["ip"] in pulled_ips:
                continue
            pulled_ips.add(pnode["ip"])
            scp_pull(cluster, pnode["ip"], pnode, "{}/{}.*".format(remote_log_dir, cfg_name),
                     local_log_dir, dry_run=dry)
    else:
        scp_pull(cluster, host, node, "{}/{}.*".format(remote_log_dir, cfg_name),
                 local_log_dir, dry_run=dry)
    if args.clean_up:
        run_ssh(cluster, host, node, "rm -rf {}".format(remote_dir), dry_run=dry)
def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    idx = int(round(p * (len(sorted_vals) - 1)))
    return sorted_vals[idx]
def parse_benchmark_log(paths):
    if isinstance(paths, (str, os.PathLike)):
        paths = [paths]
    latencies = []
    ok = fail = 0
    total_bytes = 0
    starts = []
    walls = []
    for path in paths:
        for line in open(path, "r", encoding="utf-8", errors="replace"):
            line = line.rstrip("\n")
            if line.startswith("RESULT\t"):
                parts = line.split("\t")
                if len(parts) < 5:
                    continue
                latencies.append(float(parts[3]))
                if parts[4] == "OK":
                    ok += 1
                else:
                    fail += 1
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
    latencies.sort()
    if starts and len(walls) == len(starts):
        ends = [s + w for s, w in zip(starts, walls)]
        wall = max(ends) - min(starts)
    else:
        wall = 0.0
    n = len(latencies)
    total = n
    return {
        "total": total,
        "total_bytes": total_bytes,
        "wall_s": wall,
        "throughput_mb_s": (total_bytes / wall / 1e6) if wall > 0 else 0.0,
        "throughput_gb_s": (total_bytes / wall / 1e9) if wall > 0 else 0.0,
        "avg_ms": (sum(latencies) / n) if n else 0.0,
        "min_ms": percentile(latencies, 0.0),
        "max_ms": percentile(latencies, 1.0),
        "p99_ms": percentile(latencies, 0.99),
        "p999_ms": percentile(latencies, 0.999),
        "ok": ok,
        "fail": fail,
        "qps": (total / wall) if wall > 0 else 0.0,
        "fail_rate": (fail / total) if total else 0.0,
    }
def write_scenario_csv(local_log_dir, cfg, ts, cfg_path):
    name = os.path.splitext(os.path.basename(cfg_path))[0]
    out_csv = os.path.join(RESULTS_DIR, ts, "{}.csv".format(name))
    rows = []
    is_pipeline = cfg.get("scenario") == "pipeline"
    if is_pipeline:
        # Pipeline: aggregate all merged reader logs (one per reader port)
        reader_paths = []
        for p in sorted(glob.glob(os.path.join(local_log_dir, "{}*reader*".format(name)))):
            # Skip .d directories and files ending with .log.reader (nohup redirect)
            if os.path.isdir(p) or ".log.reader" in p:
                continue
            reader_paths.append(p)
        if reader_paths:
            agg = parse_benchmark_log(reader_paths)
            c, t = 1, cfg.get("num_threads", 1)
            topo = "local_node" if cfg["client_node"] == cfg.get("store_node", cfg["client_node"]) else "remote_node"
            row = {
                "scenario": cfg["scenario"],
                "mode": cfg.get("mode", ""),
                "operation": cfg.get("operation", ""),
                "topology": topo,
                "protocol": cfg["protocol"],
                "num_clients": c,
                "num_threads": t,
            }
            row.update({
                "data_size": cfg.get("data_size", ""),
                "key_count": cfg.get("key_count", ""),
                "batch_size": "",
                "put_ratio": "",
                "duration": cfg.get("duration", ""),
                "writer_count": cfg.get("writer_count", ""),
                "reader_count": cfg.get("reader_count", ""),
                "target_qps": cfg.get("target_qps", ""),
                "ttl": cfg.get("ttl", ""),
                "notify_count": cfg.get("notify_count", ""),
                "notify_pipeline": ",".join(cfg.get("notify_pipeline", [])),
                "traffic_mode": cfg.get("traffic_mode", ""),
            })
            row.update({k: agg[k] for k in (
                "total", "total_bytes", "wall_s", "throughput_mb_s", "throughput_gb_s",
                "avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms", "ok", "fail",
                "qps", "fail_rate")})
            # Use configured duration for wall_s instead of reader's timeout
            if is_pipeline and cfg.get("duration"):
                row["wall_s"] = float(cfg["duration"])
                total = row["total"]
                total_bytes = row["total_bytes"]
                wall_s = row["wall_s"]
                row["throughput_mb_s"] = round(total_bytes / wall_s / 1e6, 2) if wall_s > 0 else 0.0
                row["throughput_gb_s"] = round(total_bytes / wall_s / 1e9, 2) if wall_s > 0 else 0.0
                row["qps"] = round(total / wall_s, 2) if wall_s > 0 else 0.0
            for k in ("wall_s", "throughput_mb_s", "throughput_gb_s",
                       "avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms",
                       "qps", "fail_rate", "total_bytes"):
                v = row.get(k)
                if v is not None and isinstance(v, float):
                    row[k] = round(v, 2)
            rows.append(row)
    else:
        pattern = os.path.join(local_log_dir, "{}.*.log".format(name))
        for path in sorted(glob.glob(pattern)):
            base = os.path.basename(path)
            try:
                c_part = base.split("benchmark_c")[1].split("_t")[0]
                t_part = base.split("_t")[1].replace(".log", "")
                c, t = int(c_part), int(t_part)
            except (IndexError, ValueError):
                continue
            agg = parse_benchmark_log(path)
            topo = "local_node" if cfg["client_node"] == cfg.get("store_node", cfg["client_node"]) else "remote_node"
            row = {
                "scenario": cfg["scenario"],
                "mode": cfg.get("mode", ""),
                "operation": cfg.get("operation", ""),
                "topology": topo,
                "protocol": cfg["protocol"],
                "num_clients": c,
                "num_threads": t,
            }
            row.update({
                "data_size": cfg.get("data_size", ""),
                "key_count": cfg.get("key_count", ""),
                "batch_size": cfg.get("batch_size", "") if not is_pipeline and cfg.get("operation") in ("put_batch", "get_batch") else "",
                "put_ratio": cfg.get("put_ratio", "") if not is_pipeline and cfg.get("operation") in ("mixed", "put/get") else "",
                "duration": cfg.get("duration", ""),
                "writer_count": cfg.get("writer_count", ""),
                "reader_count": cfg.get("reader_count", ""),
                "target_qps": cfg.get("target_qps", ""),
                "ttl": cfg.get("ttl", ""),
                "notify_count": cfg.get("notify_count", ""),
                "notify_pipeline": ",".join(cfg.get("notify_pipeline", [])),
                "traffic_mode": cfg.get("traffic_mode", ""),
            })
            row.update({k: agg[k] for k in (
                "total", "total_bytes", "wall_s", "throughput_mb_s", "throughput_gb_s",
                "avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms", "ok", "fail",
                "qps", "fail_rate")})
            for k in ("wall_s", "throughput_mb_s", "throughput_gb_s",
                       "avg_ms", "min_ms", "max_ms", "p99_ms", "p999_ms",
                       "qps", "fail_rate", "total_bytes"):
                v = row.get(k)
                if v is not None and isinstance(v, float):
                    row[k] = round(v, 2)
            rows.append(row)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return rows

def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
def main():
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
    parser.add_argument("--protocol", default=None,
                        help="override protocol (e.g. tcp) for local testing")
    parser.add_argument("--key-count", type=int, default=None,
                        help="override key_count for local scaled-down testing")
    parser.add_argument("--data-size", default=None,
                        help="override data_size (e.g. 1MB) for local scaled-down testing")
    parser.add_argument("--clean-up", action="store_true",
                        help="after run, ssh delete all remote files (scripts, data, results)")
    args = parser.parse_args()
    if not os.path.exists(args.cluster):
        example = os.path.join(CONFIGS_DIR, "cluster.example.json")
        sys.exit("[ERROR] cluster config not found: {}\n"
                 "       copy {} to {} and edit it first.".format(
                     args.cluster, example, os.path.join(CONFIGS_DIR, "cluster.json")))
    if not os.path.exists(args.global_cfg):
        sys.exit("[ERROR] global config not found: {}".format(args.global_cfg))
    scenario_category = args.scenario
    if args.config:
        cfg_path = os.path.join(CONFIGS_DIR, args.config)
        if scenario_category is None:
            parts = args.config.replace("\\", "/").split("/")
            scenario_category = parts[0]
    elif args.all or args.demo:
        if scenario_category is None:
            sys.exit("[ERROR] --all/--demo requires --scenario")
    else:
        sys.exit("[ERROR] provide --scenario with --all or --demo or --config")
    if args.scenario and not args.all and not args.config and not args.demo:
        sys.exit("[ERROR] --scenario provided but none of --all/--demo/--config is given")
    cluster = load_json(args.cluster)
    globals_cfg = load_json(args.global_cfg)
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
    cfg_paths = []
    if args.config:
        cfg_path = os.path.join(CONFIGS_DIR, args.config)
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
    all_rows = []
    for cfg_path in cfg_paths:
        cfg = load_json(cfg_path)
        cfg["__cfg_path__"] = cfg_path
        name = os.path.splitext(os.path.basename(cfg_path))[0]
        try:
            run_scenario(cluster, globals_cfg, cfg, ts, args)
            if not args.dry_run:
                rows = write_scenario_csv(os.path.join(ts_dir, "logs", name), cfg, ts, cfg_path)
                all_rows.extend(rows)
        except Exception as exc:
            log("[FAIL] scenario {}: {}".format(name, exc))
    if args.dry_run:
        log("dry-run complete, no artifacts written")
        return
    detail_csv = os.path.join(ts_dir, "benchmark_detail.csv")
    with open(detail_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADER)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)
    log("detail -> {}".format(detail_csv))
    log("done. results in {}".format(ts_dir))
if __name__ == "__main__":
    main()