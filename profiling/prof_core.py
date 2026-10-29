#!/usr/bin/env python3
"""prof_core — 性能诊断矩阵核心（编排层，不采样）。

由 benchmark.py 调用（--profile / 默认 configs/matrix.json）：
  load_matrix()          解析矩阵（enabled 过滤 + 默认值合并）
  start_profiling()      场景启动：逐节点探测工具 -> spdiag restart ->
                         perf/bpftrace nohup 后台（pid 记入 .state 台账，
                         marker 记录预期 cmdline，reap 防误杀按 marker 校验）
  stop_and_collect()     场景结束：SIGINT 停采样（perf 优雅落盘）->
                         spdiag show -> scp 收集至 outputs/<ts>/profiling/
                         <节点>/ -> 清台账
  extract_outliers()     RESULT 行 > threshold 的异常值清单（时刻/key/时延）
  slice_window()         异常时刻 ±window 的文本数据源切片（按行内
                         HH:MM:SS 时间戳过滤；perf.data 建议真机用
                         `perf script --time` 原生切片）

手动 CLI 见 profiling/profiling.py。所有故障仅 [WARN]，绝不影响测试主路径。
采样命令（perf/bpftrace）默认加 `sudo -n` 前缀（matrix 逐工具可关，"sudo": false）；
sudo 运行产生的 root 属主产物在收集前自动 chown 回 ssh 用户；spdiag 节点级全局生效。
"""
import json
import os
import re
import sys
import time

if __package__ in (None, ""):
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils import get_node, log, scp_pull, scp_push, ssh_check_output  # noqa: E402

BT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bt")

TOOL_DEFAULTS = {
    "spdiag": {"enabled": False, "bin": "spdiag", "sudo": False},
    "perf": {"enabled": False, "bin": "perf", "sudo": True,
             "freq": 99, "callgraph": "fp", "output": "perf.data"},
    "runqlat": {"enabled": False, "bin": "bpftrace", "sudo": True,
                "interval_s": 10},
    "offcputime": {"enabled": False, "bin": "bpftrace", "sudo": True,
                   "min_block_us": 1000},
}


def load_matrix(path):
    """Load the profiling matrix; missing file -> None (profiling off)."""
    if not path or not os.path.isfile(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    tools = {}
    for name, defaults in TOOL_DEFAULTS.items():
        cfg = dict(defaults)
        cfg.update(raw.get("tools", {}).get(name, {}) or {})
        tools[name] = cfg
    if not any(c.get("enabled") for c in tools.values()):
        return None
    return {
        "tools": tools,
        "nodes": raw.get("nodes", "auto"),
        "trigger": raw.get("trigger", "whole_run"),
        "outliers": {"threshold_ms": 1.0, "window_ms": 200, "auto": True,
                     **(raw.get("outliers") or {})},
    }


def _run(ip, node, cmd, quiet=True):
    return ssh_check_output(ip, node, cmd, quiet=quiet)


def _bin(tool_cfg):
    prefix = "sudo -n " if tool_cfg.get("sudo") else ""
    return prefix + tool_cfg.get("bin", "")


def _profiling_nodes(cluster, matrix, involved_nodes):
    spec = matrix.get("nodes", "auto")
    if spec == "auto":
        return list(involved_nodes)
    names = spec if isinstance(spec, list) else [spec]
    known = cluster.get("nodes") or {}
    return [n for n in names if n in known] or list(involved_nodes)


def _ledger_row(ip, node_name, pid, label, marker, cfg_name):
    return {"ip": ip, "node": node_name, "port": None, "fabric_host": None,
            "pid": pid, "label": "profiling {}".format(label),
            "marker": marker, "cfg": cfg_name,
            "ts": time.strftime("%Y-%m-%d %H:%M:%S")}


def start_profiling(cluster, matrix, involved_nodes, cfg_name, remote_out_dir):
    """Start every enabled tool on every profiling node. Returns the session
    dict (or None when nothing could start). Per-tool failures are isolated:
    one broken tool only skips itself ([WARN]) and never aborts the rest."""
    import benchmark as bm  # .state ledger helpers
    nodes = _profiling_nodes(cluster, matrix, involved_nodes)
    session = {"nodes": nodes, "tools": {}, "remote_out_dir": remote_out_dir,
               "cfg": cfg_name, "matrix_tools": matrix["tools"]}
    for node_name in nodes:
        n = get_node(cluster, node_name)
        ip = n["ip"]
        try:
            # remote output dir must exist before any tool writes into it
            _run(ip, n, "mkdir -p {}".format(remote_out_dir))
        except Exception as exc:
            log("[WARN] [profile] mkdir on {}: {}".format(node_name, exc))
            continue
        started = {}
        for tool, tcfg in matrix["tools"].items():
            if not tcfg.get("enabled"):
                continue
            try:
                # presence probe (absolute bin path checked directly)
                probe = tcfg.get("bin", "")
                if probe.startswith("/"):
                    present = _run(ip, n, "test -x {} && echo OK || echo MISSING".format(probe))
                else:
                    present = _run(ip, n, "command -v {} >/dev/null 2>&1 && echo OK || echo MISSING".format(probe))
                if present != "OK":
                    log("[profile] {} unavailable on {} -> skipped".format(tool, node_name))
                    continue
                if tcfg.get("sudo"):
                    # privilege probe: without NOPASSWD the tool would die
                    # instantly (stderr is only visible in perf.data.err)
                    if _run(ip, n, "sudo -n true 2>/dev/null && echo OK || echo FAIL") != "OK":
                        log("[WARN] [profile] {} on {}: sudo -n unavailable -> skipped "
                            "(grant NOPASSWD or set \"sudo\": false in the matrix)".format(
                                tool, node_name))
                        continue
                if tool == "spdiag":
                    # node-global counters: reset and start accumulating
                    out = _run(ip, n, "{} restart".format(_bin(tcfg)))
                    log("[profile] spdiag restart on {} ({})".format(node_name, out or "ok"))
                    started[tool] = None
                elif tool == "perf":
                    out_file = "{}/{}".format(remote_out_dir, tcfg.get("output", "perf.data"))
                    err_file = "{}/perf.data.err".format(remote_out_dir)
                    # system-wide sampling until SIGINT (-a); stderr kept in
                    # perf.data.err (collected later) so startup failures
                    # (sudo/paranoid/perf_event settings) stay diagnosable
                    # instead of being swallowed by /dev/null
                    cmd = ("nohup {} record -F {} -a -g --call-graph {} -o {} "
                           "> {} 2>&1 & echo $!").format(
                               _bin(tcfg), tcfg.get("freq", 99),
                               tcfg.get("callgraph", "fp"), out_file, err_file)
                    pid = _run(ip, n, cmd)
                    if pid.strip().isdigit():
                        started[tool] = pid.strip()
                        bm.state_append([_ledger_row(ip, node_name, pid.strip(),
                                                     "perf record", "perf record", cfg_name)])
                else:  # runqlat / offcputime via the shipped bpftrace scripts
                    # the .bt lives orchestrator-side only -> push it next to
                    # the outputs and reference the REMOTE posix path (the
                    # old command embedded the orchestrator-local path, which
                    # never exists on the node -> bpftrace died instantly)
                    local_bt = os.path.join(BT_DIR, "{}.bt".format(tool))
                    remote_bt = "{}/{}.bt".format(remote_out_dir, tool)
                    scp_push(ip, n, [local_bt], remote_out_dir)
                    if tool == "runqlat":
                        args = "INTERVAL={}".format(int(tcfg.get("interval_s", 10)))
                    else:
                        args = "MIN_BLOCK_US={}".format(int(tcfg.get("min_block_us", 1000)))
                    out_file = "{}/{}.out".format(remote_out_dir, tool)
                    # sudo -n env VAR=... : the var must be set AFTER sudo
                    # (sudo resets the environment by default)
                    sudo_prefix = "sudo -n " if tcfg.get("sudo") else ""
                    cmd = "nohup {}env {} {} {} > {} 2>&1 & echo $!".format(
                        sudo_prefix, args, tcfg.get("bin", ""), remote_bt, out_file)
                    pid = _run(ip, n, cmd)
                    if pid.strip().isdigit():
                        started[tool] = pid.strip()
                        marker = "bpftrace {}".format(remote_bt)
                        bm.state_append([_ledger_row(ip, node_name, pid.strip(),
                                                     tool, marker, cfg_name)])
            except Exception as exc:
                log("[WARN] [profile] start {} on {}: {}".format(tool, node_name, exc))
        if started:
            session["tools"][node_name] = started
    if not session["tools"]:
        return None
    log("[profile] started: {}".format(
        {k: list(v.keys()) for k, v in session["tools"].items()}))
    return session


def _wait_artifact_stable(ip, node, out_file, sudo="", polls=15, interval_s=2):
    """Poll the remote artifact size until it stops growing (a big
    system-wide perf.data keeps flushing for seconds after SIGINT).
    Bounded at polls*interval_s; a missing artifact returns immediately."""
    last = -1
    for _ in range(polls):
        try:
            raw = _run(ip, node, "{}stat -c %s {} 2>/dev/null || echo -1".format(
                sudo, out_file), quiet=True)
            size = int(raw.strip() or -1)
        except Exception:
            size = -1
        if size < 0:
            return  # artifact never appeared (tool died) -> nothing to wait for
        if size == last:
            return  # size stabilized -> flush complete
        last = size
        time.sleep(interval_s)


def stop_and_collect(cluster, session, local_prof_dir):
    """Stop sampling, dump spdiag, collect artifacts. Never raises."""
    if not session:
        return
    import benchmark as bm
    try:
        for node_name, tools in session["tools"].items():
            n = get_node(cluster, node_name)
            ip = n["ip"]
            remote_out = session["remote_out_dir"]
            collected = []
            for tool, pid in tools.items():
                tcfg = session["matrix_tools"].get(tool, {})
                try:
                    if tool == "spdiag":
                        out_file = "{}/spdiag.show.txt".format(remote_out)
                        _run(ip, n, "{} show > {}".format(_bin(tcfg), out_file))
                        collected.append("spdiag.show.txt")
                    elif pid:
                        fname = "perf.data" if tool == "perf" else "{}.out".format(tool)
                        out_file = "{}/{}".format(remote_out, fname)
                        sudo = "sudo -n " if tcfg.get("sudo") else ""
                        # SIGINT so perf flushes its data file gracefully;
                        # sudo-started pids need sudo to signal
                        _run(ip, n, "{}kill -INT {} 2>/dev/null || true".format(sudo, pid))
                        if tool == "perf":
                            # big system-wide captures flush for a while
                            # after SIGINT -> wait for the size to stabilize
                            # instead of a fixed 2s (perf.data.err is created
                            # by the launching shell, so it always collects)
                            _wait_artifact_stable(ip, n, out_file, sudo)
                            collected.append("perf.data.err")
                        else:
                            time.sleep(2)
                        if tcfg.get("sudo"):
                            # privileged run wrote a root-owned artifact
                            # (perf.data): hand it back to the ssh user before
                            # scp; $(id -un) expands remotely as that user
                            _run(ip, n, 'sudo -n chown "$(id -un):$(id -gn)" "{}" '
                                        '2>/dev/null || true'.format(out_file))
                        collected.append(fname)
                except Exception as exc:
                    log("[WARN] [profile] stop {} on {}: {}".format(tool, node_name, exc))
            node_local = os.path.join(local_prof_dir, node_name)
            os.makedirs(node_local, exist_ok=True)
            for fname in collected:
                try:
                    scp_pull(ip, n, "{}/{}".format(remote_out, fname), node_local)
                    log("[profile] collected {}/{}".format(node_name, fname))
                except Exception as exc:
                    log("[WARN] [profile] collect {}/{}: {}".format(node_name, fname, exc))
            bm.state_remove([{"ip": ip, "pid": p} for p in tools.values() if p])
    except Exception as exc:
        log("[WARN] [profile] stop_and_collect: {}".format(exc))


# --------------------------------------------------------------------------
# outlier extraction & time-window slicing (RESULT rows carry HH:MM:SS.micro)
# --------------------------------------------------------------------------

def _ts_to_seconds(ts):
    h, m, rest = ts.split(":")
    return int(h) * 3600 + int(m) * 60 + float(rest)


def extract_outliers(local_log_dir, threshold_ms):
    """Scan every *.log under local_log_dir for RESULT rows above threshold.

    Returns [{ts, seconds, key, lat_ms, status, file}] sorted by time."""
    out = []
    for root, _dirs, files in os.walk(local_log_dir):
        for f in sorted(files):
            if not f.endswith(".log"):
                continue
            path = os.path.join(root, f)
            for line in open(path, "r", encoding="utf-8", errors="replace"):
                if not line.startswith("RESULT\t"):
                    continue
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 6 or ":" not in parts[1]:
                    continue  # legacy rows without timestamps
                try:
                    lat = float(parts[4])
                except ValueError:
                    continue
                if lat <= threshold_ms:
                    continue
                out.append({"ts": parts[1], "seconds": _ts_to_seconds(parts[1]),
                            "key": parts[3] if len(parts) > 3 else "",
                            "lat_ms": lat, "status": parts[5],
                            "file": os.path.relpath(path, local_log_dir)})
    out.sort(key=lambda x: (x["seconds"], x["file"]))
    return out


def slice_window(prof_dir, center_seconds, window_ms):
    """Slice collected text artifacts (bpftrace .out / spdiag .txt) around
    [center ± window] by per-line HH:MM:SS timestamps. perf.data slicing is
    done node-side with `perf script --time` (hinted by the caller)."""
    half = window_ms / 1000.0
    lo, hi = center_seconds - half, center_seconds + half
    slices = {}
    for root, _dirs, files in os.walk(prof_dir):
        for f in sorted(files):
            path = os.path.join(root, f)
            if not (f.endswith(".out") or f.endswith(".txt")):
                continue
            lines = []
            for line in open(path, "r", encoding="utf-8", errors="replace"):
                for m in re.finditer(r"\b(\d{2}:\d{2}:\d{2}(?:\.\d+)?)\b", line):
                    try:
                        s = _ts_to_seconds(m.group(1))
                    except ValueError:
                        continue
                    if lo <= s <= hi:
                        lines.append(line.rstrip("\n"))
                        break
            if lines:
                slices[os.path.relpath(path, prof_dir)] = lines
    return slices
