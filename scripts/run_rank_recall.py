#!/usr/bin/env python3
"""Run rank and recall benchmark groups concurrently as one managed task."""
import argparse
import os
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


def _csv(value):
    return [x.strip() for x in (value or "").split(",") if x.strip()]


def partition_configs(config_dirs, config_files):
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
    seen = set()
    for path in paths:
        full = os.path.abspath(path)
        if full in seen:
            continue
        seen.add(full)
        cfg = bm.load_scenario_config(path)
        work_mode = cfg.get("work_mode", cfg.get("mode", ""))
        if work_mode not in groups:
            raise ValueError(
                "parallel rank+recall only accepts work_mode=rank|recall: "
                "{} ({})".format(path, work_mode or "missing"))
        groups[work_mode].append(path)
    for work_mode in ("rank", "recall"):
        if not groups[work_mode]:
            raise ValueError("no {} test config selected".format(work_mode))
    return groups


def _unique_parent():
    base = time.strftime("%Y%m%d-%H%M-rank-recall")
    name = base
    n = 1
    while os.path.exists(os.path.join(bm.RESULTS_DIR, name)):
        name = "{}-{}".format(base, n)
        n += 1
    return name


def _child_argv(args, work_mode, files, parent):
    cluster = getattr(args, work_mode + "_cluster")
    global_cfg = getattr(args, work_mode + "_global")
    argv = [sys.executable, os.path.join(ROOT, "benchmark.py"), "--k8s",
            "--cluster", cluster, "--global", global_cfg,
            "--profile", args.profile, "-f", ",".join(files)]
    if not args.dry_run:
        argv += ["--result-dir", os.path.join("outputs", parent, work_mode)]
    for enabled, flag in ((args.dry_run, "--dry-run"),
                          (args.no_warmup, "--no-warmup"),
                          (args.no_plot, "--no-plot"),
                          (args.verbose, "--verbose")):
        if enabled:
            argv.append(flag)
    if args.protocol:
        argv += ["--protocol", args.protocol]
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


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="run inherited rank and recall configs concurrently")
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
    parser.add_argument("--no-plot", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--stop-file")
    return parser.parse_args(argv)


def main(argv=None):
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
    if not args.dry_run:
        os.makedirs(os.path.join(bm.RESULTS_DIR, parent))
        print("results root: {}".format(os.path.join(bm.RESULTS_DIR, parent)),
              flush=True)
    print("[parallel] starting rank={} config(s), recall={} config(s)".format(
        len(groups["rank"]), len(groups["recall"])), flush=True)

    stop_requested = threading.Event()

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
                _child_argv(args, work_mode, groups[work_mode], parent),
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
        if owned_stop_file:
            try:
                os.remove(args.stop_file)
            except OSError:
                pass

    codes = {name: proc.returncode for name, proc in children.items()}
    print("[parallel] finished: rank rc={}, recall rc={}".format(
        codes.get("rank"), codes.get("recall")), flush=True)
    if stop_requested.is_set():
        return 1 if spawn_error else 130
    return 0 if all(code == 0 for code in codes.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
