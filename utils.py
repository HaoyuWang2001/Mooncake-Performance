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
import posixpath
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time


# --- dual-sink logging: console summary + full benchmark.log --------------
# log()        console (summary) AND the full log file
# log_detail() the full log file ALWAYS; console only when verbose
# The full file lives at outputs/<ts>/benchmark.log and receives every
# per-pod/per-command line, so the WebUI console stays short while every
# detail remains locatable on disk.
_DETAIL_FH = None


def set_detail_log(path):
    """Open the full-detail log (outputs/<ts>/benchmark.log, append mode)."""
    global _DETAIL_FH
    try:
        _DETAIL_FH = open(path, "a", encoding="utf-8", buffering=1)
        _DETAIL_FH.write("# full orchestrator log (console lines + all "
                         "per-command/per-pod detail)\n")
    except OSError:
        _DETAIL_FH = None


def close_detail_log():
    global _DETAIL_FH
    if _DETAIL_FH is not None:
        try:
            _DETAIL_FH.close()
        except Exception:
            pass
        _DETAIL_FH = None


def _detail_write(msg):
    if _DETAIL_FH is not None:
        try:
            _DETAIL_FH.write("[{}] [benchmark] {}\n".format(
                time.strftime("%H:%M:%S"), msg))
            _DETAIL_FH.flush()
        except Exception:
            pass


def log(msg):
    print("[{}] [benchmark] {}".format(time.strftime("%H:%M:%S"), msg),
          flush=True)
    _detail_write(msg)


# --- verbose logging (CLI --verbose / env MC_BENCH_VERBOSE / WebUI toggle) ---
_VERBOSE = False


def set_verbose(enabled):
    global _VERBOSE
    _VERBOSE = bool(enabled)


def verbose_enabled():
    return _VERBOSE


def log_detail(msg):
    """Full-detail line: always to benchmark.log, console only when verbose."""
    _detail_write(msg)
    if _VERBOSE:
        print("[{}] [benchmark] {}".format(time.strftime("%H:%M:%S"), msg),
              flush=True)


# --- cooperative stop (WebUI stop file / Ctrl+C signals) -------------------

class StopRequested(BaseException):
    """Raised when a stop was requested; BaseException so scenario-level
    except Exception handlers do NOT swallow it."""


_STOP_EVENT = threading.Event()
_STOP_FILE = None
_STOP_LOCK = threading.Lock()


def set_stop_file(path):
    global _STOP_FILE
    with _STOP_LOCK:
        _STOP_FILE = path


def clear_stop():
    _STOP_EVENT.clear()
    with _STOP_LOCK:
        global _STOP_FILE
        _STOP_FILE = None


def request_stop():
    _STOP_EVENT.set()


def stop_requested():
    if _STOP_EVENT.is_set():
        return True
    with _STOP_LOCK:
        path = _STOP_FILE
    return bool(path and os.path.exists(path))


def is_k8s_node(node):
    return node.get("kind") == "k8s-pod"


def kubectl_cmd(node, *args):
    """kubectl invocation for a k8s-pod node (context/namespace injected).

    Kept for backward compatibility with callers that build argv; actual
    execution now goes through the controller helpers below.
    """
    cmd = ["kubectl"]
    if node.get("_ctx"):
        cmd += ["--context", node["_ctx"]]
    if node.get("_ns"):
        cmd += ["-n", node["_ns"]]
    return cmd + list(args)


# --- controller transport (kubectl host; local or SSH) ---------------------

def controller_is_local(controller):
    return bool(controller.get("_local"))


def controller_desc(controller):
    """Human-readable controller destination for upload/pull logs."""
    if controller_is_local(controller):
        return "local"
    return "{}@{}".format(controller.get("ssh_user", "root"),
                          controller.get("ip", "?"))


def kubectl_prefix(node):
    """kubectl base args (context + namespace) matching kubectl_cmd()."""
    parts = ["kubectl"]
    if node.get("_ctx"):
        parts += ["--context", node["_ctx"]]
    if node.get("_ns"):
        parts += ["-n", node["_ns"]]
    return parts


def kubectl_exec_line(node, command):
    """Full copy-pasteable kubectl exec command (runs on the controller)."""
    parts = kubectl_prefix(node)
    parts += ["exec", node["_pod"], "--", "bash", "-c", command]
    return " ".join(str(a) for a in parts)


def kubectl_cp_to_line(node, local_file, remote_path):
    """Copy-pasteable kubectl cp (local -> pod)."""
    parts = kubectl_prefix(node)
    parts += ["cp", local_file,
              "{}:{}".format(node["_pod"], remote_path)]
    return " ".join(str(a) for a in parts)


def kubectl_cp_from_line(node, remote_file, local_path):
    """Copy-pasteable kubectl cp (pod -> local)."""
    parts = kubectl_prefix(node)
    parts += ["cp", "{}:{}".format(node["_pod"], remote_file), local_path]
    return " ".join(str(a) for a in parts)


def _remote_path(path):
    """Controller/pod paths are POSIX; Windows orchestrators must not emit '\\'."""
    return str(path).replace("\\", "/")


def _controller_env(controller):
    env = dict(os.environ)
    if controller.get("kubeconfig"):
        env["KUBECONFIG"] = controller["kubeconfig"]
    return env


def _controller_kubectl_argv(controller, args):
    argv = [controller.get("_kubectl") or "kubectl"]
    if controller.get("_ctx"):
        argv += ["--context", controller["_ctx"]]
    return argv + [str(a) for a in args]


def controller_run(controller, args, check=True, timeout=None):
    """Run kubectl on the controller; return (combined_output, rc)."""
    argv = _controller_kubectl_argv(controller, args)
    if _VERBOSE:
        log_detail("[k8s] $ {} {}".format(
            controller_desc(controller), " ".join(str(a) for a in argv)))
    if controller_is_local(controller):
        try:
            proc = subprocess.Popen(argv, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, text=True,
                                    env=_controller_env(controller))
        except FileNotFoundError:
            raise RuntimeError("kubectl not found (controller local)")
        deadline = time.time() + timeout if timeout else None
        while True:
            if stop_requested():
                proc.kill()
                proc.communicate()
                raise StopRequested()
            try:
                sout, serr = proc.communicate(timeout=0.2)
                break
            except subprocess.TimeoutExpired:
                if deadline is not None and time.time() > deadline:
                    proc.kill()
                    proc.communicate()
                    raise RuntimeError("kubectl {} timed out".format(
                        " ".join(str(a) for a in args)))
        out = (sout or "") + (serr or "")
        rc = proc.returncode
    else:
        shell = " ".join(shlex.quote(a) for a in argv)
        if controller.get("kubeconfig"):
            shell = "KUBECONFIG={} {}".format(
                shlex.quote(controller["kubeconfig"]), shell)
        out, rc = _ssh_exec(controller, shell + " 2>&1")
    if check and rc != 0:
        raise RuntimeError("kubectl {} failed (rc={}): {}".format(
            " ".join(str(a) for a in args), rc, out.strip()[:500]))
    return out, rc


def controller_diagnose(controller):
    """Human-readable kubectl/kubeconfig/context info for diagnostics."""
    kubectl = controller.get("_kubectl") or "kubectl"
    kubeconfig = controller.get("kubeconfig") or ""
    if controller_is_local(controller):
        env = _controller_env(controller)
        kpath = shutil.which(kubectl) or kubectl
        effective = (kubeconfig or env.get("KUBECONFIG")
                     or os.path.join(os.path.expanduser("~"), ".kube", "config"))
        lines = ["kubectl={}".format(kpath),
                 "kubeconfig={}".format(effective),
                 "HOME={}".format(os.path.expanduser("~")),
                 "PATH={}".format(env.get("PATH", ""))]
        try:
            r = subprocess.run([kpath, "config", "current-context"],
                               capture_output=True, text=True, env=env,
                               timeout=15)
            lines.append("context={}".format(
                (r.stdout or r.stderr).strip() or "<empty>"))
        except Exception as exc:
            lines.append("context=<error: {}>".format(exc))
        return "\n".join(lines)
    prefix = ""
    if kubeconfig:
        prefix = "export KUBECONFIG={}; ".format(shlex.quote(kubeconfig))
    cmd = (
        prefix +
        'printf "HOME=%s\\n" "$HOME"; '
        'printf "KUBECONFIG=%s\\n" "${KUBECONFIG:-}"; '
        'printf "PATH=%s\\n" "$PATH"; '
        'K=$(command -v ' + shlex.quote(kubectl) + ' 2>/dev/null || true); '
        'printf "kubectl=%s\\n" "${K:-NOT_FOUND}"; '
        'if [ -n "$K" ]; then "$K" config current-context 2>&1 | '
        'sed "s/^/context=/"; fi'
    )
    out, _rc = _ssh_exec(controller, cmd + " 2>&1")
    return out.strip()


def controller_stream(controller, args, quiet=False):
    """Run kubectl and relay output live; return rc. quiet=True discards the
    child output (it is already captured in the stage log file) so mass
    operations (e.g. generate-data on 32 pods) do not flood the console."""
    argv = _controller_kubectl_argv(controller, args)
    sink = subprocess.DEVNULL if quiet else None
    if controller_is_local(controller):
        try:
            proc = subprocess.Popen(argv, env=_controller_env(controller),
                                    stdout=sink, stderr=sink)
        except FileNotFoundError:
            raise RuntimeError("kubectl not found (controller local)")
        while proc.poll() is None:
            if stop_requested():
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
                raise StopRequested()
            time.sleep(0.1)
        return proc.returncode
    shell = " ".join(shlex.quote(a) for a in argv) + " 2>&1"
    if controller.get("kubeconfig"):
        shell = "KUBECONFIG={} {}".format(
            shlex.quote(controller["kubeconfig"]), shell)
    _, rc = _ssh_exec(controller, shell, relay=not quiet)
    return rc


def controller_mkdir(controller, path):
    if controller_is_local(controller):
        os.makedirs(path, exist_ok=True)
        return
    path = _remote_path(path)
    out, rc = _ssh_exec(controller, "mkdir -p " + shlex.quote(path))
    if rc != 0:
        raise RuntimeError("controller mkdir {} failed: {}".format(path, out.strip()[:300]))


def controller_put(controller, local_files, remote_dir):
    """Copy local files into a controller directory (local copy or SFTP)."""
    controller_mkdir(controller, remote_dir)
    desc = controller_desc(controller)
    clean_dir = _remote_path(remote_dir)
    for f in local_files:
        base = os.path.basename(f)
        log_detail("[k8s] upload {} -> {}:{}".format(base, desc,
                                              clean_dir.rstrip("/") + "/" + base))
        log_detail("[k8s]   size: {} bytes".format(
            os.path.getsize(f) if os.path.isfile(f) else "?"))
    if controller_is_local(controller):
        for f in local_files:
            shutil.copy2(f, os.path.join(remote_dir, os.path.basename(f)))
        return
    sftp = _ssh_client(controller).open_sftp()
    try:
        for f in local_files:
            sftp.put(f, remote_dir.rstrip("/") + "/" + os.path.basename(f))
    finally:
        sftp.close()


def controller_get_file(controller, remote_path, local_dir):
    os.makedirs(local_dir, exist_ok=True)
    if controller_is_local(controller):
        shutil.copy2(remote_path, os.path.join(local_dir, os.path.basename(remote_path)))
        return
    remote_path = _remote_path(remote_path)
    sftp = _ssh_client(controller).open_sftp()
    try:
        sftp.get(remote_path, os.path.join(local_dir, os.path.basename(remote_path)))
    finally:
        sftp.close()


def controller_get_dir(controller, remote_dir, local_dir):
    """Recursively copy a controller directory to local (tar over SFTP)."""
    os.makedirs(local_dir, exist_ok=True)
    if controller_is_local(controller):
        if os.path.isdir(remote_dir):
            shutil.copytree(remote_dir, local_dir, dirs_exist_ok=True)
        return
    tmp_remote = "/tmp/mc-pull-{}.tgz".format(int(time.time() * 1000))
    out, rc = _ssh_exec(controller, "tar -czf {} -C {} .".format(
        shlex.quote(tmp_remote), shlex.quote(remote_dir)))
    if rc != 0:
        raise RuntimeError("controller tar {} failed: {}".format(
            remote_dir, out.strip()[:300]))
    tmp_local = tempfile.mktemp(suffix=".tgz")
    sftp = _ssh_client(controller).open_sftp()
    try:
        sftp.get(tmp_remote, tmp_local)
    finally:
        sftp.close()
    try:
        with tarfile.open(tmp_local, "r:gz") as tf:
            try:
                tf.extractall(local_dir, filter="data")
            except TypeError:       # Python < 3.12
                tf.extractall(local_dir)
    finally:
        os.remove(tmp_local)
        _ssh_exec(controller, "rm -f " + shlex.quote(tmp_remote))


def controller_remove(controller, path):
    if controller_is_local(controller):
        shutil.rmtree(path, ignore_errors=True)
        return
    _ssh_exec(controller, "rm -rf " + shlex.quote(_remote_path(path)))


def controller_exec(controller, command):
    """Run a raw shell command on the controller (not kubectl).
    Returns (combined_output, rc)."""
    if controller_is_local(controller):
        try:
            proc = subprocess.run(["bash", "-c", command],
                                  capture_output=True, text=True,
                                  env=_controller_env(controller))
            return (proc.stdout or "") + (proc.stderr or ""), proc.returncode
        except FileNotFoundError:
            raise RuntimeError("bash not found (controller local)")
    out, rc = _ssh_exec(controller, command + " 2>&1")
    return out, rc


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
            if stop_requested():
                try:
                    chan.close()
                except Exception:
                    pass
                _ssh_drop(node)
                raise StopRequested()
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
        log_detail("[k8s] $ {}".format(kubectl_exec_line(node, command)))
        if dry_run:
            log_detail("      (dry-run, skipped)")
            return
        rc = controller_stream(node["_controller"],
                               ["-n", node["_ns"], "exec", node["_pod"],
                                "--", "bash", "-c", command])
        if rc != 0:
            raise RuntimeError(
                "kubectl exec failed (rc={}) on {}: {}\n"
                "  (command output was streamed live above this error)"
                "".format(rc, node["_pod"], command))
        return
    log_detail("[ssh] {} : {}".format(host, command))
    if dry_run:
        log_detail("      (dry-run, skipped)")
        return
    _, rc = _ssh_exec(node, command, relay=True)
    if rc != 0:
        raise RuntimeError(
            "ssh command failed (rc={}) on {}: {}\n"
            "  (command output was streamed live above this error)"
            "".format(rc, host, command))


def ssh_check_output(host, node, command, dry_run=False, quiet=False):
    if is_k8s_node(node):
        if not quiet:
            log_detail("[k8s] $ {}".format(kubectl_exec_line(node, command)))
        if dry_run:
            log_detail("      (dry-run, skipped)")
            return ""
        out, rc = controller_run(node["_controller"],
                                 ["-n", node["_ns"], "exec", node["_pod"],
                                  "--", "bash", "-c", command], check=False)
        if rc != 0:
            raise RuntimeError(
                "kubectl exec failed (rc={}) on {}: {}\n  output: {}".format(
                    rc, node["_pod"], command,
                    out.strip()[-400:] or "<empty>"))
        return out.strip()
    if not quiet:
        log_detail("[ssh] {} : {}".format(host, command))
    if dry_run:
        log_detail("      (dry-run, skipped)")
        return ""
    out, rc = _ssh_exec(node, command)
    if rc != 0:
        raise RuntimeError(
            "ssh command failed (rc={}) on {}: {}\n  output: {}".format(
                rc, host, command, out.strip()[-400:] or "<empty>"))
    return out.strip()


def scp_push(host, node, local_files, remote_path, dry_run=False):
    if is_k8s_node(node):
        c = node["_controller"]
        stage = posixpath.join(node.get("_controller_workspace") or "/tmp", "push-tmp")
        log_detail("[kcp] -> {} : {}".format(node["_pod"], remote_path))
        if dry_run:
            log_detail("      (dry-run, skipped)")
            return
        controller_put(c, local_files, stage)
        for f in local_files:
            staged = posixpath.join(stage, os.path.basename(f))
            log_detail("[k8s] $ {}".format(
                kubectl_cp_to_line(node, staged, remote_path)))
            out, rc = controller_run(c,
                                     ["-n", node["_ns"], "cp", staged,
                                      "{}:{}".format(node["_pod"], remote_path)],
                                     check=False)
            if rc != 0:
                raise RuntimeError("kubectl cp failed for {}: {}".format(f, out.strip()[:300]))
        return
    log_detail("[scp] -> {} : {}".format(host, remote_path))
    if dry_run:
        log_detail("      (dry-run, skipped)")
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
        if dry_run:
            log_detail("[kcp] <- {} : {}".format(node["_pod"], remote_glob))
            log_detail("      (dry-run, skipped)")
            return
        listing = ssh_check_output(host, node,
                                   "ls -1 {} 2>/dev/null || true".format(remote_glob),
                                   quiet=True)
        files = [l.strip() for l in listing.splitlines() if l.strip()]
        if not files and not any(c in remote_glob for c in "*?["):
            raise RuntimeError("kubectl cp pull failed: no such file on {}: {}".format(
                node["_pod"], remote_glob))
        c = node["_controller"]
        stage = posixpath.join(node.get("_controller_workspace") or "/tmp", "pull-tmp")
        if not dry_run:
            controller_mkdir(c, stage)
        for f in files:
            if controller_is_local(c):
                dst = local_dir.rstrip("\\/") + os.sep
            else:
                dst = posixpath.join(stage, os.path.basename(f))
            log_detail("[k8s] $ {}".format(
                kubectl_cp_from_line(node, f, dst)))
            if dry_run:
                log_detail("      (dry-run, skipped)")
                continue
            if controller_is_local(c):
                out, rc = controller_run(
                    c, ["-n", node["_ns"], "cp", "{}:{}".format(node["_pod"], f),
                        dst], check=False)
            else:
                out, rc = controller_run(
                    c, ["-n", node["_ns"], "cp", "{}:{}".format(node["_pod"], f), dst],
                    check=False)
                if rc == 0:
                    controller_get_file(c, dst, local_dir)
            if rc != 0:
                raise RuntimeError("kubectl cp pull failed for {}: {}".format(f, out.strip()[:300]))
        log_detail("[k8s] pulled {} files from pod {} -> {}".format(
            len(files), node["_pod"], local_dir))
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
    log_detail("[scp] <- {} : {}".format(host, remote_glob))
    if dry_run:
        log_detail("      (dry-run, skipped)")
        return
    os.makedirs(local_dir, exist_ok=True)
    sftp = _ssh_client(node).open_sftp()
    try:
        for f in files:
            log_detail("[scp] <- {} : {}".format(host, f))
            sftp.get(f, os.path.join(local_dir, os.path.basename(f)))
    finally:
        sftp.close()
    log_detail("[ssh] pulled {} files from {} -> {}".format(
        len(files), host, local_dir))


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
