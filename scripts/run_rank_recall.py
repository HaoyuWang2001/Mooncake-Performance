#!/usr/bin/env python3
"""Run rank and recall benchmark groups concurrently as one managed task."""
import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import benchmark as bm
from utils import ensure_utf8_stdio


def _csv(value):
    return [x.strip() for x in (value or "").split(",") if x.strip()]


def partition_configs(config_dirs, config_files, configs_root=None):
    """Expand selectors, resolve inheritance, then split by effective work_mode."""
    paths = []
    for directory in config_dirs:
        if not os.path.isdir(directory):
            raise ValueError("config dir not found: {}".format(directory))
        paths.extend(bm.collect_config_files(directory))
    for path in config_files:
        if not os.path.isfile(path):
            raise ValueError("config file not found: {}".format(path))
        paths.append(path)

    groups = {"rank": [], "recall": []}
    cfgs = {"rank": [], "recall": []}
    seen = set()
    for path in paths:
        full = os.path.abspath(path)
        if full in seen:
            continue
        seen.add(full)
        cfg = bm.load_scenario_config(path, configs_root=configs_root)
        work_mode = cfg.get("work_mode", cfg.get("mode", ""))
        if work_mode not in groups:
            raise ValueError(
                "parallel rank+recall only accepts work_mode=rank|recall: "
                "{} ({})".format(path, work_mode or "missing"))
        groups[work_mode].append(path)
        cfgs[work_mode].append(cfg)
    for work_mode in ("rank", "recall"):
        if not groups[work_mode]:
            raise ValueError("no {} test config selected".format(work_mode))
    # Deliberately strict: the combined mode exists to align ONE rank and ONE
    # recall load against each other, so exactly one config per side and an
    # identical measurement window (warmup + duration) are required — anything
    # else would interleave mismatched epochs under one barrier.
    for work_mode in ("rank", "recall"):
        if len(groups[work_mode]) != 1:
            raise ValueError(
                "parallel rank+recall 仅支持一份 rank + 一份 recall 配置"
                "（当前 rank={}, recall={}）".format(
                    len(groups["rank"]), len(groups["recall"])))

    def _times(cfg):
        # effective values, mirroring how benchmark.py resolves them
        w = cfg.get("warmup")
        warm = float(w.get("duration", 0) or 0) if isinstance(w, dict) \
            else float(w or 0)
        dur = float(cfg.get("duration", 30))
        return warm, dur

    rank_t, recall_t = _times(cfgs["rank"][0]), _times(cfgs["recall"][0])
    if rank_t != recall_t:
        raise ValueError(
            "parallel rank+recall 要求两份配置的 warmup/duration 完全一致："
            "rank warmup={:g}s duration={:g}s, "
            "recall warmup={:g}s duration={:g}s".format(
                rank_t[0], rank_t[1], recall_t[0], recall_t[1]))
    return groups


def _unique_parent():
    base = time.strftime("%Y%m%d-%H%M-rank-recall")
    name = base
    n = 1
    while os.path.exists(os.path.join(bm.RESULTS_DIR, name)):
        name = "{}-{}".format(base, n)
        n += 1
    return name


def _child_argv(args, work_mode, files, parent, ready_dir):
    cluster = getattr(args, work_mode + "_cluster")
    global_cfg = getattr(args, work_mode + "_global")
    mode = getattr(args, "mode", "k8s") or "k8s"
    argv = [sys.executable, os.path.join(ROOT, "benchmark.py"), "--" + mode,
            "--cluster", cluster, "--global", global_cfg,
            "--profile", args.profile, "-f", ",".join(files)]
    # Internal cross-child barrier: each benchmark child runs setup (Pods,
    # data, smoke) and registers its clients, marks its own role ready at
    # the /start gate, then waits until the peer marker exists so both
    # children trigger /start together.
    argv += ["--parallel-ready-dir", ready_dir,
             "--parallel-ready-role", work_mode]
    if not args.dry_run:
        argv += ["--result-dir", os.path.join("outputs", parent, work_mode)]
    for enabled, flag in ((args.dry_run, "--dry-run"),
                          (args.no_warmup, "--no-warmup"),
                          (getattr(args, "no_report", False), "--no-report"),
                          (getattr(args, "png", False), "--png"),
                          (getattr(args, "pull_raw_logs", False),
                           "--pull-raw-logs"),
                          (getattr(args, "keep_intermediates", False),
                           "--keep-intermediates"),
                          (args.verbose, "--verbose")):
        if enabled:
            argv.append(flag)
    if args.protocol:
        argv += ["--protocol", args.protocol]
    for flag, val in (("--monitor-mem", getattr(args, "monitor_mem", None)),
                      ("--monitor-urma", getattr(args, "monitor_urma", None))):
        if val in ("on", "off"):
            argv += [flag, val]
    if args.stop_file:
        argv += ["--stop-file", args.stop_file]
    return argv


def _stream_safe_text(stream, text):
    """Keep log forwarding alive when the console cannot encode a character."""
    encoding = getattr(stream, "encoding", None)
    if not encoding:
        return text
    return text.encode(encoding, errors="backslashreplace").decode(encoding)


def _pump(label, proc, output=None):
    output = output or sys.stdout
    try:
        for line in iter(proc.stdout.readline, ""):
            text = _stream_safe_text(output, "[{}] {}".format(label, line))
            output.write(text)
            output.flush()
    finally:
        if proc.stdout:
            proc.stdout.close()


def _child_env(work_mode):
    # Namespace ledgers must be isolated. Otherwise one child can interpret
    # the other child's freshly-created namespace as a stale previous run and
    # delete it during startup. A WebUI-managed run passes its own per-run
    # MC_BENCH_STATE_FILE; derive child ledgers from it so two concurrent
    # combined runs never share a ledger.
    env = dict(os.environ)
    base = env.get("MC_BENCH_STATE_FILE") or os.path.join(ROOT, ".state")
    env["MC_BENCH_STATE_FILE"] = base + "." + work_mode
    # The parent decodes both child pipes as UTF-8.  Make that contract
    # explicit on Windows too, where redirected Python stdio otherwise uses
    # the active ANSI code page (commonly GBK/CP936).
    env["PYTHONUTF8"] = "1"
    env["PYTHONIOENCODING"] = "utf-8"
    return env


def _render_parent_overview(parent_dir):
    """One combined overview.html over both children.

    Thin forwarder: the implementation lives in benchmark.py
    (_render_combined_overview) so --rebuild-report re-renders exactly the
    same parent overview from the children's overview_units.json."""
    return bm._render_combined_overview(parent_dir)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="run inherited rank and recall configs concurrently")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--ssh", dest="mode", action="store_const",
                      const="ssh",
                      help="run both workloads on bare-metal SSH clusters")
    mode.add_argument("--k8s", dest="mode", action="store_const",
                      const="k8s",
                      help="run both workloads on k8s (default)")
    parser.set_defaults(mode="k8s")
    parser.add_argument("--config-dir", default="")
    parser.add_argument("-f", "--config", default="")
    parser.add_argument("--rank-cluster", required=True)
    parser.add_argument("--rank-global", required=True)
    parser.add_argument("--recall-cluster", required=True)
    parser.add_argument("--recall-global", required=True)
    parser.add_argument("--profile", required=True)
    parser.add_argument("--protocol", choices=("tcp", "rdma", "ub"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-warmup", action="store_true")
    parser.add_argument("--no-report", action="store_true")
    parser.add_argument("--png", action="store_true")
    parser.add_argument("--pull-raw-logs", action="store_true")
    parser.add_argument("--keep-intermediates", action="store_true")
    parser.add_argument("--monitor-mem", choices=("on", "off"), default=None)
    parser.add_argument("--monitor-urma", choices=("on", "off"), default=None)
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--stop-file")
    args = parser.parse_args(argv)
    if args.png and not args.pull_raw_logs:
        parser.error("开启 png 必须开启 pull-raw-logs（PNG 绘图需要原始客户端"
                     "日志）")
    return args


def main(argv=None):
    ensure_utf8_stdio()
    args = parse_args(argv)
    owned_stop_file = not args.stop_file
    if owned_stop_file:
        args.stop_file = os.path.join(
            tempfile.gettempdir(),
            "mcbench-rank-recall-{}-{}.stop".format(os.getpid(), time.time_ns()))
    try:
        groups = partition_configs(_csv(args.config_dir), _csv(args.config))
    except (OSError, ValueError) as exc:
        print("[ERROR] {}".format(exc), flush=True)
        return 2

    parent = _unique_parent()
    ready_dir = tempfile.mkdtemp(prefix="mcbench-rank-recall-ready-")
    if not args.dry_run:
        os.makedirs(os.path.join(bm.RESULTS_DIR, parent))
        print("results root: {}".format(os.path.join(bm.RESULTS_DIR, parent)),
              flush=True)
        snap_src = os.environ.get("MC_INSTANCE_SNAPSHOT")
        if snap_src and os.path.isdir(snap_src):
            snap_dst = os.path.join(bm.RESULTS_DIR, parent, "snapshot")
            try:
                shutil.copytree(snap_src, snap_dst, dirs_exist_ok=True)
                print("[parallel] 配置快照已归档到 {}".format(
                    os.path.relpath(snap_dst, bm.ROOT)), flush=True)
            except Exception as exc:
                print("[WARN] 配置快照归档失败: {}".format(exc), flush=True)
    print("[parallel] starting rank={} config(s), recall={} config(s)".format(
        len(groups["rank"]), len(groups["recall"])), flush=True)

    stop_requested = threading.Event()
    peer_failure = False

    def request_stop(signum, _frame):
        stop_requested.set()
        if args.stop_file:
            try:
                with open(args.stop_file, "w", encoding="utf-8") as fh:
                    fh.write("stop\n")
            except OSError:
                pass
        print("[parallel] stop signal {} received; waiting for both cleanups".format(
            signum), flush=True)

    for sig_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            signal.signal(sig, request_stop)

    children = {}
    pumps = []
    spawn_error = None
    try:
        for work_mode in ("rank", "recall"):
            child = subprocess.Popen(
                _child_argv(args, work_mode, groups[work_mode], parent,
                            ready_dir),
                cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace", bufsize=1,
                env=_child_env(work_mode))
            children[work_mode] = child
            thread = threading.Thread(target=_pump, args=(work_mode, child),
                                      daemon=True)
            thread.start()
            pumps.append(thread)
        while any(proc.poll() is None for proc in children.values()):
            if args.stop_file and os.path.isfile(args.stop_file):
                stop_requested.set()
            failed = [name for name, proc in children.items()
                      if proc.poll() not in (None, 0)]
            if failed and not stop_requested.is_set():
                # A child may fail before publishing its /start-gate marker.
                # Wake the peer out of the barrier so it can clean up instead
                # of waiting for the full barrier timeout.
                peer_failure = True
                request_stop("{}-failed".format(failed[0]), None)
            time.sleep(0.25)
    except KeyboardInterrupt:
        request_stop(signal.SIGINT, None)
    except Exception as exc:
        spawn_error = exc
        print("[ERROR] cannot start both workloads: {}".format(exc), flush=True)
        request_stop("spawn-error", None)
    finally:
        for proc in children.values():
            try:
                proc.wait()
            except Exception:
                pass
        for thread in pumps:
            thread.join(timeout=2)
        shutil.rmtree(ready_dir, ignore_errors=True)
        if owned_stop_file:
            try:
                os.remove(args.stop_file)
            except OSError:
                pass

    codes = {name: proc.returncode for name, proc in children.items()}
    print("[parallel] finished: rank rc={}, recall rc={}".format(
        codes.get("rank"), codes.get("recall")), flush=True)
    if not args.dry_run:
        try:
            _render_parent_overview(os.path.join(bm.RESULTS_DIR, parent))
        except Exception as exc:
            print("[WARN] 总览生成失败: {}".format(exc), flush=True)
    if stop_requested.is_set():
        return 1 if (spawn_error or peer_failure) else 130
    return 0 if all(code == 0 for code in codes.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
