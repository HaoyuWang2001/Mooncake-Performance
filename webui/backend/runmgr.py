"""webui/backend/runmgr — 进程与调度管理器（无 HTTP 逻辑）。

职责：
  - 单一 benchmark 槽位：同一时刻只允许一个 benchmark 进程（占用即拒绝）
  - 跨平台停止：Windows taskkill /F /T，POSIX 进程组 TERM->KILL
  - 运行历史持久化：webui/data/runs/<id>.json + <id>.log
  - scheduler 线程：复用 run_schedule 的计划解析，任务触发时若 benchmark
    槽位被占则等待其结束后再执行；scheduler 等待期间允许手动 benchmark
    （router 层附 warning），但任务执行期间拒绝新 run

模块级 MGR 单例供各 router 共享。
"""
import importlib.util
import json
import os
import signal
import subprocess
import sys
import threading
import time
from datetime import datetime

from . import conflicts
from .paths import ROOT, RUNS_DIR

BENCH = os.path.join(ROOT, "benchmark.py")
IS_WIN = os.name == "nt"
STOP_GRACE = float(os.environ.get("MCWEB_STOP_GRACE", "60"))


def _write_stop_file(path):
    if not path:
        return
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write("stop\n")
    except OSError:
        pass


def _remove_stop_file(path):
    if path:
        try:
            os.remove(path)
        except OSError:
            pass


def _send_graceful_stop(proc):
    """Ask the child to run its cleanup; force kill is a separate fallback."""
    if proc is None or proc.poll() is not None:
        return
    try:
        if IS_WIN:
            os.kill(proc.pid, signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(os.getpgid(proc.pid), signal.SIGINT)
    except Exception:
        pass


def _popen(argv_or_cmd, log_fh, shell=False, env=None):
    kwargs = {"stdout": log_fh, "stderr": subprocess.STDOUT,
              "cwd": ROOT, "shell": shell}
    if env is not None:
        kwargs["env"] = env
    if IS_WIN:
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen(argv_or_cmd, **kwargs)

_seq_lock = threading.Lock()
_seq = [0]


def _new_id(prefix):
    with _seq_lock:
        _seq[0] += 1
        return "{}{}-{:03d}".format(prefix, time.strftime("%Y%m%d-%H%M%S"), _seq[0])





def _kill(proc):
    """Cross-platform process-tree kill. Never raises."""
    try:
        if IS_WIN:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           capture_output=True)
        else:
            import signal
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except ProcessLookupError:
                return
            for _ in range(10):
                if proc.poll() is not None:
                    return
                time.sleep(0.3)
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
    except Exception:
        pass


def _proc_cmdline(pid):
    """Linux: /proc/<pid>/cmdline -> argv list（其它平台返回空）。"""
    try:
        with open("/proc/{}/cmdline".format(pid), "rb") as f:
            return [p.decode("utf-8", "replace")
                    for p in f.read().split(b"\0") if p]
    except OSError:
        return []


def _find_run_pid(stop_path):
    """按 `--stop-file <精确路径>` 扫描 /proc 定位遗留 benchmark 进程。

    仅 POSIX；路径精确匹配（run id 唯一），不会误认其它进程。
    """
    if IS_WIN or not stop_path:
        return None
    try:
        pids = [p for p in os.listdir("/proc") if p.isdigit()]
    except OSError:
        return None
    for pid in pids:
        argv = _proc_cmdline(pid)
        for i in range(len(argv) - 1):
            if argv[i] == "--stop-file" and argv[i + 1] == stop_path:
                return int(pid)
    return None


def _pid_alive(pid, stop_path=None):
    """进程存活 + （可选）cmdline 仍带我方 stop-file（防 pid 复用误杀）。"""
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    if stop_path:
        return any(a == stop_path for a in _proc_cmdline(pid))
    return True


def _signal_pid(pid, sig):
    """给遗留 benchmark 的进程组（start_new_session 的会话首进程）发信号。

    安全阀：若目标与本进程同组（理论上不该发生），只对单 pid 发信号，
    绝不用 killpg 波及 webui 自身。"""
    try:
        pgid = os.getpgid(pid)
        if pgid != os.getpgid(0):
            os.killpg(pgid, sig)
            return
    except Exception:
        pass
    try:
        os.kill(pid, sig)
    except Exception:
        pass


def _terminal_status_from_log(log_path, stopping):
    """无进程句柄（webui 重启接管）时按运行日志推断终态。"""
    if stopping:
        return "stopped"
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            tail = f.read()[-200000:]
    except OSError:
        return "aborted"
    if "SUMMARY: all " in tail:
        return "done"
    if "SUMMARY: " in tail and "FAILED" in tail:
        return "failed"
    return "aborted"


def load_run_schedule():
    """schedule/ has no __init__.py -> import run_schedule.py by path."""
    path = os.path.join(ROOT, "schedule", "run_schedule.py")
    spec = importlib.util.spec_from_file_location("webui_run_schedule", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class Run(object):
    """One managed benchmark process (or scheduler task placeholder)."""

    def __init__(self, rid, kind, cmd, argv, log_path, meta=None,
                 stop_path=None, state_path=None, identities=None):
        self.id = rid
        self.kind = kind              # benchmark
        self.cmd = cmd                # display string
        self.argv = argv
        self.log_path = log_path
        self.stop_path = stop_path
        self.state_path = state_path  # per-run .state ledger (concurrency)
        self.identities = identities or []   # not persisted (frozensets)
        self.meta = meta or {}
        self.proc = None
        self.pid = None               # 接管遗留进程时记录（webui 重启后）
        self.t0 = time.time()
        self.t1 = None
        self.rc = None
        self.status = "running"       # running|done|failed|stopped|failed_spawn
        self.stopping = False
        self.ts_dir = None            # outputs/<ts>, parsed from the log

    def to_dict(self):
        return {"id": self.id, "kind": self.kind, "cmd": self.cmd,
                "argv": self.argv, "log": os.path.relpath(self.log_path, ROOT),
                "status": self.status, "rc": self.rc,
                "stopping": self.stopping,
                "t0": self.t0, "t1": self.t1,
                "elapsed": (self.t1 or time.time()) - self.t0,
                "ts_dir": self.ts_dir, "meta": self.meta}

    def save(self):
        with open(os.path.join(RUNS_DIR, self.id + ".json"), "w",
                  encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=1)

    def parse_ts_dir(self):
        # benchmark.py logs "results root: outputs/<ts>" (non-dry runs only);
        # normalize to a repo-relative path ("outputs/<ts>")
        if self.ts_dir or not os.path.isfile(self.log_path):
            return
        try:
            with open(self.log_path, "r", encoding="utf-8", errors="replace") as f:
                for line in f:
                    if "results root: " in line:
                        val = line.split("results root: ", 1)[1].strip()
                        if val.startswith(ROOT):
                            val = os.path.relpath(val, ROOT).replace("\\", "/")
                        self.ts_dir = val
                        return
        except OSError:
            pass


class Scheduler(object):
    """A running schedule plan (one thread). Task commands reuse the same
    output-file header/footer format as run_schedule.run_task."""

    def __init__(self, sid, plan_path, plan, log_path):
        self.id = sid
        self.plan_path = plan_path
        self.plan = plan
        self.log_path = log_path
        self.state = "waiting"        # waiting|waiting_slot|executing|done|stopped|aborted
        self.current_task = None      # 1-based idx while executing
        self.next_trigger = None      # display string
        self.task_proc = None
        self.task_stop_path = None
        self.stop_event = threading.Event()
        self.events = []              # recent status lines (capped)
        self.t0 = time.time()

    def log(self, msg):
        line = "[{}] {}".format(datetime.now().strftime("%H:%M:%S"), msg)
        self.events.append(line)
        del self.events[:-50]
        try:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except OSError:
            pass

    def to_dict(self):
        return {"id": self.id, "plan": os.path.relpath(self.plan_path, ROOT),
                "state": self.state, "current_task": self.current_task,
                "next_trigger": self.next_trigger, "tasks": len(self.plan["tasks"]),
                "t0": self.t0, "events": list(self.events[-15:])}


class SlotBusy(Exception):
    def __init__(self, reason):
        Exception.__init__(self, reason)
        self.reason = reason


class RunManager(object):
    def __init__(self):
        self.lock = threading.RLock()
        self.run = None               # newest Run (compat: last started)
        self.active = {}              # rid -> Run (status == running)
        self.history = []             # Run objects, newest first
        self.scheduler = None         # Scheduler or None
        self._load_history()
        self._recover_active()
        self._thread = threading.Thread(target=self._poll_loop)
        self._thread.daemon = True
        self._thread.start()

    # ---------------- history ----------------

    def _load_history(self):
        try:
            names = sorted(os.listdir(RUNS_DIR), reverse=True)
        except OSError:
            names = []
        for n in names:
            if not n.endswith(".json"):
                continue
            try:
                with open(os.path.join(RUNS_DIR, n), "r", encoding="utf-8") as f:
                    d = json.load(f)
                run = Run(d.get("id", n[:-5]), d.get("kind", "benchmark"),
                          d.get("cmd", ""), d.get("argv", []),
                          os.path.join(RUNS_DIR, n[:-5] + ".log"),
                          d.get("meta"))
                run.status = d.get("status", "done")
                run.rc = d.get("rc")
                run.t0 = d.get("t0", 0)
                run.t1 = d.get("t1")
                run.ts_dir = d.get("ts_dir")
                # 重启后停止/接管需要这些路径（原先只活在启动进程的内存里）
                run.stop_path = os.path.join(RUNS_DIR, run.id + ".stop")
                run.state_path = os.path.join(RUNS_DIR, run.id + ".state")
                try:
                    req = (d.get("meta") or {}).get("request") or {}
                    run.identities = conflicts.run_identities(
                        ROOT, req.get("flags") or {})
                except Exception:
                    run.identities = []
                self.history.append(run)
                if len(self.history) >= 200:
                    break
            except Exception:
                continue

    def _recover_active(self):
        """接管上一条 webui 进程遗留的 benchmark（start_new_session 使其在
        webui 重启后仍存活，例如 k8s 失败后 hold 等 STOP 的 run）。

        按 `--stop-file` 精确路径扫 /proc 找 pid；扫不到（进程已退出）的
        running 残留按日志终态收敛，避免实例卡片永久卡「运行中」。
        """
        for run in list(self.history):
            if run.kind != "benchmark" or run.status != "running":
                continue
            pid = _find_run_pid(run.stop_path)
            if pid is not None:
                run.pid = pid
                run.parse_ts_dir()
                self.active[run.id] = run
                sys.stderr.write(
                    "[webui] recovered active run {} (pid {})\n".format(
                        run.id, pid))
                continue
            run.status = _terminal_status_from_log(run.log_path,
                                                   run.stopping)
            run.t1 = run.t1 or time.time()
            try:
                run.save()
            except Exception:
                pass
            sys.stderr.write("[webui] stale run {} -> {}\n".format(
                run.id, run.status))

    def get_run(self, rid):
        for r in self.history:
            if r.id == rid:
                return r
        return None

    # ---------------- benchmark slot ----------------

    def slot_busy_reason(self):
        """Why a new benchmark cannot start right now (None = free).

        Benchmark-vs-benchmark conflicts are handled per shared resource in
        `_check_conflicts` (multiple instances may run concurrently); only an
        executing scheduler task still blocks unconditionally because its
        command may touch any cluster."""
        sch = self.scheduler
        if sch is not None and sch.state in ("executing", "stopping"):
            return "scheduler task {} is {} (plan {}, stop it first)".format(
                sch.current_task, sch.state, sch.plan_path)
        return None

    def _check_conflicts(self, identities, meta):
        """Raise SlotBusy when the candidate shares a resource with an
        active run (or the same instance is already running)."""
        inst = (meta or {}).get("instance")
        active = {}
        for rid, r in self.active.items():
            if inst and (r.meta or {}).get("instance") == inst:
                raise SlotBusy(
                    "instance {} already has an active run {}".format(
                        inst, rid))
            active[rid] = ((r.meta or {}).get("instance_name") or rid,
                           r.identities)
        hit = conflicts.find_conflict(identities, active)
        if hit:
            kind, value, rid, label = hit
            raise SlotBusy(
                "shared resource in use by instance {!r} (run {}): {} — "
                "wait for it to finish or pick a different cluster/global"
                "".format(label, rid, conflicts.describe(kind, value)))

    def start_benchmark(self, argv, meta=None, identities=None):
        with self.lock:
            busy = self.slot_busy_reason()
            if busy:
                raise SlotBusy(busy)
            if identities:
                self._check_conflicts(identities, meta)
            rid = _new_id("r")
            log_path = os.path.join(RUNS_DIR, rid + ".log")
            stop_path = os.path.join(RUNS_DIR, rid + ".stop")
            state_path = os.path.join(RUNS_DIR, rid + ".state")
            _remove_stop_file(stop_path)
            try:
                os.remove(state_path)
            except OSError:
                pass
            argv = list(argv) + ["--stop-file", stop_path]
            run = Run(rid, "benchmark", " ".join(argv), list(argv), log_path,
                      meta, stop_path=stop_path, state_path=state_path,
                      identities=identities)
            env = dict(os.environ)
            env["MC_BENCH_STATE_FILE"] = state_path
            try:
                fh = open(log_path, "wb")
            except OSError as exc:
                raise SlotBusy("cannot create log file: {}".format(exc))
            try:
                run.proc = _popen(argv, fh, env=env)
            except Exception as exc:
                fh.close()
                run.status = "failed_spawn"
                run.t1 = time.time()
                run.rc = -1
                run.save()
                raise SlotBusy("spawn failed: {}".format(exc))
            self.run = run
            self.active[rid] = run
            self.history.insert(0, run)
            run.save()
            return run

    def stop_run(self, rid):
        with self.lock:
            run = self.get_run(rid)
            if run is None:
                return None, "no such run: {}".format(rid)
            if run.status != "running" or (run.proc is None and not run.pid):
                return run, None  # already finished; not an error
            run.stopping = True
            if not run.stop_path:
                run.stop_path = os.path.join(RUNS_DIR, run.id + ".stop")
            _write_stop_file(run.stop_path)
            if run.proc is not None:
                _send_graceful_stop(run.proc)
            elif run.pid:
                _signal_pid(run.pid, signal.SIGINT)
            threading.Thread(target=self._stop_watch, args=(run,),
                             daemon=True).start()
            return run, None

    def stop_orphan(self, rid):
        """停止未接管的持久化 running run（webui 重启后遗留的 benchmark）。

        仅写 stop 文件即可让 benchmark 的 hold/清理循环退出并删 k8s
        namespace；能定位 pid 时再补发优雅信号并交给 _poll_loop 收敛终态。
        """
        with self.lock:
            run = self.get_run(rid)
            if run is None:
                return None, "no such run: {}".format(rid)
            if run.status != "running":
                return run, None
            if not run.stop_path:
                run.stop_path = os.path.join(RUNS_DIR, run.id + ".stop")
            if not run.state_path:
                run.state_path = os.path.join(RUNS_DIR, run.id + ".state")
            run.stopping = True
            _write_stop_file(run.stop_path)
            pid = run.pid or _find_run_pid(run.stop_path)
            if pid:
                run.pid = pid
                run.parse_ts_dir()
                self.active[run.id] = run
                _signal_pid(pid, signal.SIGINT)
                threading.Thread(target=self._stop_watch, args=(run,),
                                 daemon=True).start()
            else:
                # 无 pid 可发信号：stop 文件是唯一通道（benchmark 自己轮询并
                # 做远端清理）；先把卡片/实例状态收敛，stop 文件保留待下次
                # 启动时清除。
                run.status = "stopped"
                run.t1 = run.t1 or time.time()
                try:
                    run.save()
                except Exception:
                    pass
            return run, None

    def _stop_watch(self, run):
        """Wait up to STOP_GRACE for cooperative cleanup, then force-kill。
        兼容两种句柄：proc（本进程启动）与 pid（重启接管的遗留进程）。"""
        proc = run.proc

        def _alive():
            if proc is not None:
                return proc.poll() is None
            return bool(run.pid) and _pid_alive(run.pid, run.stop_path)

        deadline = time.time() + STOP_GRACE
        while time.time() < deadline and _alive():
            time.sleep(0.5)
        if _alive():
            sys.stderr.write("[webui] stop grace {}s expired; force kill {}\n"
                             "".format(STOP_GRACE, run.id))
            if proc is not None:
                _kill(proc)
            elif run.pid:
                _signal_pid(run.pid, signal.SIGKILL)
        try:
            self._reap_remote_async(run)
        finally:
            _remove_stop_file(run.stop_path)

    def _reap_remote_async(self, run):
        """停止后清理远端残留：按 .state 台账 kill 远端 client 进程
        （cmdline 校验防 pid 重用误杀、带端口的 reader 先优雅 shutdown）、
        清空台账。复用 benchmark.reap_state_entries。daemon 线程执行——
        多节点 SSH 需数秒，不阻塞停止响应。"""
        def worker():
            try:
                import benchmark as bm  # lazy: heavy module
                base_state = run.state_path or os.path.join(ROOT, ".state")
                cluster_specs = [("configs/cluster.json", base_state)]
                try:
                    req = (run.meta or {}).get("request") or {}
                    flags = req.get("flags", {})
                    if flags.get("launch_mode") == "rank_recall":
                        # children derive their ledger from the parent's
                        # MC_BENCH_STATE_FILE: <base>.rank / <base>.recall
                        cluster_specs = [
                            (flags.get("rank_cluster"), base_state + ".rank"),
                            (flags.get("recall_cluster"), base_state + ".recall")]
                    else:
                        cluster_specs = [(flags.get("cluster") or
                                          "configs/cluster.json", base_state)]
                except Exception:
                    pass
                seen_paths = set()
                for cluster_path, state_path in cluster_specs:
                    if not cluster_path or cluster_path in seen_paths:
                        continue
                    seen_paths.add(cluster_path)
                    bm.STATE_FILE = state_path
                    full = os.path.join(ROOT, cluster_path)
                    if not os.path.isfile(full):
                        continue
                    with open(full, "r", encoding="utf-8") as f:
                        cluster = json.load(f)
                    if cluster.get("controller"):
                        cluster["_controller"] = bm.build_controller(cluster)
                        try:
                            bm.k8s_settings(cluster)
                            bm.reap_k8s_namespaces(cluster)
                            bm.cleanup_k8s_namespace(cluster, dry_run=False)
                        except Exception as exc:
                            sys.stderr.write(
                                "[webui] k8s cleanup for {} ({}): {}\n".format(
                                    run.id, cluster_path, exc))
                    bm.reap_state_entries(cluster)
            except Exception as exc:
                sys.stderr.write("[webui] remote cleanup for {}: {}\n".format(
                    run.id, exc))
        threading.Thread(target=worker, daemon=True).start()

    # ---------------- scheduler ----------------

    def start_scheduler(self, plan_path):
        with self.lock:
            if self.scheduler is not None and \
                    self.scheduler.state in ("waiting", "waiting_slot",
                                             "executing", "stopping"):
                raise SlotBusy("scheduler {} is already active (plan {})".format(
                    self.scheduler.id, self.scheduler.plan_path))
            rs = load_run_schedule()
            plan = rs.load_plan(plan_path)  # raises SystemExit on bad plan
            sid = _new_id("s")
            log_path = os.path.join(RUNS_DIR, sid + ".log")
            sch = Scheduler(sid, plan_path, plan, log_path)
            self.scheduler = sch
            t = threading.Thread(target=self._scheduler_loop, args=(sch, rs))
            t.daemon = True
            t.start()
            return sch

    def stop_scheduler(self):
        with self.lock:
            sch = self.scheduler
            if sch is None or sch.state in ("done", "stopped", "aborted"):
                return None, "no active scheduler"
            if sch.state == "stopping":
                return sch, None
            sch.stop_event.set()
            proc = sch.task_proc
            if proc is not None and proc.poll() is None:
                sch.state = "stopping"
                _write_stop_file(sch.task_stop_path)
                _send_graceful_stop(proc)
                threading.Thread(
                    target=self._scheduler_stop_watch,
                    args=(sch, proc, sch.task_stop_path),
                    daemon=True).start()
            return sch, None

    def _scheduler_stop_watch(self, sch, proc, stop_path):
        deadline = time.time() + STOP_GRACE
        while time.time() < deadline and proc.poll() is None:
            time.sleep(0.5)
        if proc.poll() is None:
            sys.stderr.write("[webui] scheduler stop grace {}s expired; "
                             "force kill task\n".format(STOP_GRACE))
            _kill(proc)
        _remove_stop_file(stop_path)

    def _scheduler_loop(self, sch, rs):
        try:
            self._scheduler_loop_inner(sch, rs)
        except Exception as exc:
            sch.state = "aborted"
            sch.log("scheduler aborted: {}".format(exc))

    def _scheduler_loop_inner(self, sch, rs):
        tasks = sch.plan["tasks"]
        wait_seconds = sch.plan["wait_seconds"]
        prev_end = None
        sch.log("scheduler {} started, {} tasks, on_fail={}".format(
            sch.id, len(tasks), sch.plan["on_fail"]))
        for idx, task in enumerate(tasks, 1):
            trigger = rs.parse_trigger(task, idx)
            if trigger is None:
                trigger = prev_end + datetime.timedelta(seconds=wait_seconds)
            sch.next_trigger = trigger.strftime("%Y-%m-%d %H:%M:%S")
            # wait for the trigger time (interruptible)
            while not sch.stop_event.is_set():
                remain = (trigger - datetime.now()).total_seconds()
                if remain <= 0:
                    break
                time.sleep(min(1.0, remain))
            if sch.stop_event.is_set():
                sch.state = "stopped"
                sch.log("scheduler stopped before task {}".format(idx))
                return
            # user rule: a running benchmark delays the task, never overlaps
            while not sch.stop_event.is_set():
                with self.lock:
                    blockers = list(self.active.values())
                if not blockers:
                    break
                if sch.state != "waiting_slot":
                    sch.state = "waiting_slot"
                    sch.log("task {} waits: benchmark run(s) {} still active"
                            "".format(idx, ", ".join(r.id for r in blockers)))
                time.sleep(5)
            if sch.stop_event.is_set():
                sch.state = "stopped"
                sch.log("scheduler stopped while waiting for the slot")
                return
            # execute the task (blocks the benchmark slot via state=executing)
            sch.state = "executing"
            sch.current_task = idx
            out_path = task["output"]
            out_dir = os.path.dirname(os.path.abspath(os.path.join(ROOT, out_path)))
            os.makedirs(out_dir, exist_ok=True)
            now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            t0 = time.time()
            sch.log("task {} START: {}".format(idx, task["command"]))
            rc = 0
            try:
                with open(os.path.join(ROOT, out_path), "a", encoding="utf-8") as f:
                    f.write("===== [{}] [task {}] {}\n".format(
                        now_str, idx, task["command"]))
                    f.flush()
                    stop_path = os.path.join(
                        RUNS_DIR, "{}.task{}.stop".format(sch.id, idx))
                    _remove_stop_file(stop_path)
                    env = dict(os.environ)
                    env["BENCH_STOP_FILE"] = stop_path
                    proc = _popen(task["command"], f, shell=True, env=env)
                    sch.task_stop_path = stop_path
                    sch.task_proc = proc
                    proc.wait()
                    rc = proc.returncode
            except Exception as exc:
                rc = -1
                sch.log("task {} spawn error: {}".format(idx, exc))
            finally:
                sch.task_proc = None
                sch.current_task = None
                _remove_stop_file(sch.task_stop_path)
                sch.task_stop_path = None
            elapsed = time.time() - t0
            end_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            try:
                with open(os.path.join(ROOT, out_path), "a", encoding="utf-8") as f:
                    f.write("===== [{}] [task {}] rc={} elapsed={:.1f}s\n".format(
                        end_str, idx, rc, elapsed))
            except OSError:
                pass
            sch.log("task {} DONE: rc={} elapsed={:.1f}s -> {}".format(
                idx, rc, elapsed, out_path))
            if sch.stop_event.is_set():
                sch.state = "stopped"
                sch.log("scheduler stopped after task {}".format(idx))
                return
            prev_end = datetime.now()
            if rc != 0 and sch.plan["on_fail"] == "abort":
                sch.state = "aborted"
                sch.log("on_fail=abort: stopping the queue after task {}".format(idx))
                return
        sch.state = "done"
        sch.log("queue finished ({} tasks ran)".format(len(tasks)))

    # ---------------- background poll ----------------

    def _poll_loop(self):
        while True:
            try:
                with self.lock:
                    for rid, run in list(self.active.items()):
                        if run.proc is not None:
                            rc = run.proc.poll()
                            if rc is None:
                                run.parse_ts_dir()
                                continue
                        elif run.pid:
                            # webui 重启接管的 run：无进程句柄，按 pid 存活判断
                            if _pid_alive(run.pid, run.stop_path):
                                run.parse_ts_dir()
                                continue
                            rc = None
                        else:
                            self.active.pop(rid, None)
                            continue
                        run.rc = rc
                        run.t1 = time.time()
                        if run.stopping:
                            run.status = "stopped"
                        elif rc is None:
                            run.status = _terminal_status_from_log(
                                run.log_path, False)
                        else:
                            run.status = "done" if rc == 0 else "failed"
                        run.pid = None
                        run.save()
                        self.active.pop(rid, None)
            except Exception:
                pass
            time.sleep(1)

    # ---------------- status ----------------

    def status(self):
        with self.lock:
            running = [r for r in self.active.values()
                       if r.status == "running"]
            return {
                # compat: newest running run (single-run UI paths)
                "active_run": running[-1].to_dict() if running else None,
                "active_runs": [r.to_dict() for r in running],
                "scheduler": self.scheduler.to_dict() if self.scheduler else None,
            }


MGR = RunManager()
