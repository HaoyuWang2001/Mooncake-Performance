"""Shared utilities for the Mooncake benchmark orchestrator.

This module is orchestrator-side only: scripts/mooncake_benchmark_client.py
stays self-contained because it is uploaded standalone to the test servers.
"""
import json
import os
import subprocess


def log(msg):
    print("[benchmark] {}".format(msg), flush=True)


def get_node(cluster, name):
    node = cluster.get("nodes", {}).get(name)
    if node is None:
        raise RuntimeError("unknown node in cluster.json: {}".format(name))
    return node


def ssh_args(node):
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


def scp_args(node):
    args = ["scp", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes", "-o", "LogLevel=QUIET"]
    port = int(node.get("ssh_port", 22))
    if port != 22:
        args += ["-P", str(port)]
    key = node.get("ssh_key") or ""
    if key:
        args += ["-i", key]
    return args


def run_ssh(host, node, command, dry_run=False):
    cmd = ssh_args(node) + [command]
    log("[ssh] {} : {}".format(host, command))
    if dry_run:
        log("      (dry-run, skipped)")
        return
    result = subprocess.run(cmd)
    if result.returncode != 0:
        raise RuntimeError("ssh command failed (rc={}) on {}: {}".format(
            result.returncode, host, command))


def ssh_check_output(host, node, command, dry_run=False):
    cmd = ssh_args(node) + [command]
    log("[ssh] {} : {}".format(host, command))
    if dry_run:
        log("      (dry-run, skipped)")
        return ""
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError("ssh command failed (rc={}) on {}: {}".format(
            result.returncode, host, command))
    return result.stdout.strip()


def scp_push(host, node, local_files, remote_path, dry_run=False):
    cmd = scp_args(node) + list(local_files) + \
        ["{}@{}:{}".format(node.get("ssh_user", "root"), host, remote_path)]
    log("[scp] -> {} : {}".format(host, remote_path))
    if dry_run:
        log("      (dry-run, skipped)")
        return
    subprocess.run(cmd, check=True)


def scp_pull(host, node, remote_glob, local_dir, dry_run=False):
    cmd = scp_args(node) + ["-r"] + \
        ["{}@{}:{}".format(node.get("ssh_user", "root"), host, remote_glob),
         local_dir.rstrip("\\/") + os.sep]
    log("[scp] <- {} : {}".format(host, remote_glob))
    if dry_run:
        log("      (dry-run, skipped)")
        return
    subprocess.run(cmd, check=True)


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


def percentile(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    idx = int(round(p * (len(sorted_vals) - 1)))
    return sorted_vals[idx]


def load_json(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
