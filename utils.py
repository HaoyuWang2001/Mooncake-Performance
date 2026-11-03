"""Shared utilities for the Mooncake benchmark orchestrator.

This module is orchestrator-side only: scripts/mooncake_benchmark_client.py
stays self-contained because it is uploaded standalone to the test servers.

Node transports (cluster.json `kind` field, default "ssh"):
  - "ssh":     paramiko SSH/SFTP against bare-metal nodes (pure Python, no
               external ssh/scp/sshpass binaries -> works on native Windows).
               Auth: node `passwd` field (password) or ssh_key/agent (keys).
               One cached connection per node (keepalive 30s), one channel
               per command.
  - "k8s-pod": kubectl exec/cp against pods. Node dicts are enriched by
               benchmark.py (enrich_k8s_nodes + ensure_pods) with "_pod"
               (pod name), "_ns" (namespace) and "_ctx" (kubecontext) before
               any transport call. Pod IPs are written into node["ip"] so
               peers/fabric/dedup logic is transport-agnostic.
"""
import json
import os
import subprocess
import sys
import threading
import time


def log(msg):
    print("[benchmark] {}".format(msg), flush=True)


def is_k8s_node(node):
    return node.get("kind") == "k8s-pod"


def kubectl_cmd(node, *args):
    """kubectl invocation for a k8s-pod node (context/namespace injected)."""
    cmd = ["kubectl"]
    if node.get("_ctx"):
        cmd += ["--context", node["_ctx"]]
    if node.get("_ns"):
        cmd += ["-n", node["_ns"]]
    return cmd + list(args)


def get_node(cluster, name):
    node = cluster.get("nodes", {}).get(name)
    if node is None:
        raise RuntimeError("unknown node in cluster.json: {}".format(name))
    return node


# --- paramiko SSH transport (ssh nodes; k8s-pod nodes use kubectl) --------

_SSH_CLIENTS = {}
_SSH_LOCK = threading.Lock()


def node_passwd(node):
    return node.get("passwd") or ""


def _ssh_cache_key(node):
    return (node.get("ip"), node.get("ssh_port", 22),
            node.get("ssh_user", "root"), node.get("ssh_key") or "",
            node_passwd(node))


def _ssh_connect(node):
    try:
        import paramiko
    except ImportError:
        raise RuntimeError("SSH nodes need paramiko: pip install paramiko")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    kwargs = {
        "hostname": node["ip"],
        "port": int(node.get("ssh_port", 22)),
        "username": node.get("ssh_user", "root"),
        "timeout": 15,
        "allow_agent": True,
        "look_for_keys": True,
    }
    pw = node_passwd(node)
    if pw:
        # explicit password auth: do not also try keys/agent
        kwargs["password"] = pw
        kwargs["allow_agent"] = False
        kwargs["look_for_keys"] = False
    key = node.get("ssh_key") or ""
    if key:
        kwargs["key_filename"] = os.path.expanduser(key)
    try:
        client.connect(**kwargs)
    except paramiko.AuthenticationException:
        raise RuntimeError(
            "SSH authentication failed for {}@{} (check passwd/ssh_key)"
            "".format(kwargs["username"], kwargs["hostname"]))
    client.get_transport().set_keepalive(30)
    return client


def _ssh_client(node):
    """Cached per-node connection (keepalive 30s); one channel per command."""
    k = _ssh_cache_key(node)
    with _SSH_LOCK:
        client = _SSH_CLIENTS.get(k)
        if client is None:
            client = _ssh_connect(node)
            _SSH_CLIENTS[k] = client
        return client


def _ssh_drop(node):
    """Drop the cached connection (stale/broken); next call reconnects."""
    k = _ssh_cache_key(node)
    with _SSH_LOCK:
        client = _SSH_CLIENTS.pop(k, None)
    if client is not None:
        try:
            client.close()
        except Exception:
            pass


def _drain_chan(chan, relay, out):
    """Move pending channel output: relay to the console or capture it."""
    while chan.recv_ready():
        d = chan.recv(65536)
        if relay:
            sys.stdout.write(d.decode("utf-8", "replace"))
            sys.stdout.flush()
        else:
            out.append(d)
    while chan.recv_stderr_ready():
        d = chan.recv_stderr(65536)
        if relay:
            sys.stdout.write(d.decode("utf-8", "replace"))
            sys.stdout.flush()
        # else: discard (parity with the old CLI capture: stdout only)


def _ssh_exec(node, command, relay=False):
    """Execute one command on the cached connection -> (stdout_text, rc).
    A broken cached connection is dropped so the next call reconnects."""
    out = []
    try:
        client = _ssh_client(node)
        stdin, stdout, stderr = client.exec_command(command)
        chan = stdout.channel
        while not chan.exit_status_ready():
            _drain_chan(chan, relay, out)
            time.sleep(0.1)
        # post-exit drain: sshd delivers EOF after any remaining output
        grace = time.time() + 5
        while (chan.recv_ready() or chan.recv_stderr_ready()
                or not chan.eof_received) and time.time() < grace:
            _drain_chan(chan, relay, out)
            time.sleep(0.05)
        _drain_chan(chan, relay, out)
        rc = chan.recv_exit_status()
        return b"".join(out).decode("utf-8", "replace"), rc
    except Exception:
        _ssh_drop(node)
        raise


def run_ssh(host, node, command, dry_run=False):
    if is_k8s_node(node):
        cmd = kubectl_cmd(node, "exec", node["_pod"], "--", "bash", "-c", command)
        log("[exec] {} : {}".format(node["_pod"], command))
        if dry_run:
            log("      (dry-run, skipped)")
            return
        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise RuntimeError("kubectl exec failed (rc={}) on {}: {}".format(
                result.returncode, node["_pod"], command))
        return
    log("[ssh] {} : {}".format(host, command))
    if dry_run:
        log("      (dry-run, skipped)")
        return
    _, rc = _ssh_exec(node, command, relay=True)
    if rc != 0:
        raise RuntimeError("ssh command failed (rc={}) on {}: {}".format(
            rc, host, command))


def ssh_check_output(host, node, command, dry_run=False, quiet=False):
    if is_k8s_node(node):
        cmd = kubectl_cmd(node, "exec", node["_pod"], "--", "bash", "-c", command)
        if not quiet:
            log("[exec] {} : {}".format(node["_pod"], command))
        if dry_run:
            log("      (dry-run, skipped)")
            return ""
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError("kubectl exec failed (rc={}) on {}: {}".format(
                result.returncode, node["_pod"], command))
        return result.stdout.strip()
    if not quiet:
        log("[ssh] {} : {}".format(host, command))
    if dry_run:
        log("      (dry-run, skipped)")
        return ""
    out, rc = _ssh_exec(node, command)
    if rc != 0:
        raise RuntimeError("ssh command failed (rc={}) on {}: {}".format(
            rc, host, command))
    return out.strip()


def scp_push(host, node, local_files, remote_path, dry_run=False):
    if is_k8s_node(node):
        # kubectl cp is single-file: loop over the files
        for f in local_files:
            cmd = kubectl_cmd(node, "cp", f,
                              "{}:{}".format(node["_pod"], remote_path))
            log("[kcp] -> {} : {}".format(node["_pod"], remote_path))
            if dry_run:
                log("      (dry-run, skipped)")
                continue
            subprocess.run(cmd, check=True)
        return
    log("[scp] -> {} : {}".format(host, remote_path))
    if dry_run:
        log("      (dry-run, skipped)")
        return
    sftp = _ssh_client(node).open_sftp()
    try:
        for f in local_files:
            sftp.put(f, remote_path.rstrip("/") + "/" + os.path.basename(f))
    finally:
        sftp.close()


def scp_pull(host, node, remote_glob, local_dir, dry_run=False):
    if is_k8s_node(node):
        # kubectl cp does NOT expand globs: list matching paths via exec,
        # then copy each file individually. A single missing non-glob path
        # raises (mimics scp failure semantics for guarded collectors).
        listing = ssh_check_output(host, node,
                                   "ls -1 {} 2>/dev/null || true".format(remote_glob),
                                   quiet=True)
        files = [l.strip() for l in listing.splitlines() if l.strip()]
        if not files and not any(c in remote_glob for c in "*?["):
            raise RuntimeError("kubectl cp pull failed: no such file on {}: {}".format(
                node["_pod"], remote_glob))
        for f in files:
            cmd = kubectl_cmd(node, "cp", "{}:{}".format(node["_pod"], f),
                              local_dir.rstrip("\\/") + os.sep)
            log("[kcp] <- {} : {}".format(node["_pod"], f))
            if dry_run:
                log("      (dry-run, skipped)")
                continue
            subprocess.run(cmd, check=True)
        return
    # SFTP cannot expand remote globs: list matching paths via exec, then
    # fetch each file (mirrors the k8s branch). A single missing non-glob
    # path raises (parity with scp failure for guarded collectors).
    listing = ssh_check_output(host, node,
                               "ls -1 {} 2>/dev/null || true".format(remote_glob),
                               quiet=True)
    files = [l.strip() for l in listing.splitlines() if l.strip()]
    if not files and not any(c in remote_glob for c in "*?["):
        raise RuntimeError("scp pull failed: no such file on {}: {}".format(
            host, remote_glob))
    log("[scp] <- {} : {}".format(host, remote_glob))
    if dry_run:
        log("      (dry-run, skipped)")
        return
    os.makedirs(local_dir, exist_ok=True)
    sftp = _ssh_client(node).open_sftp()
    try:
        for f in files:
            sftp.get(f, os.path.join(local_dir, os.path.basename(f)))
    finally:
        sftp.close()


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
