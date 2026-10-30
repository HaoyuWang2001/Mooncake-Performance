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
import subprocess
import sys
import threading
import time
from datetime import datetime

from .paths import ROOT, RUNS_DIR

BENCH = os.path.join(ROOT, "benchmark.py")
IS_WIN = os.name == "nt"

_seq_lock = threading.Lock()
_seq = [0]


def _new_id(prefix):
    with _seq_lock:
        _seq[0] += 1
        return "{}{}-{:03d}".format(prefix, time.strftime("%Y%m%d-%H%M%S"), _seq[0])


def _popen(argv_or_cmd, log_fh, shell=False):
    kwargs = {"stdout": log_fh, "stderr": subprocess.STDOUT,
              "cwd": ROOT, "shell": shell}
    if IS_WIN:
        kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True
    return subprocess.Popen(argv_or_cmd, **kwargs)


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


def load_run_schedule():
    """schedule/ has no __init__.py -> import run_schedule.py by path."""
    path = os.path.join(ROOT, "schedule", "run_schedule.py")
    spec = importlib.util.spec_from_file_location("webui_run_schedule", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class Run(object):
    """One managed benchmark process (or scheduler task placeholder)."""

    def __init__(self, rid, kind, cmd, argv, log_path, meta=None):
        self.id = rid
        self.kind = kind              # benchmark
        self.cmd = cmd                # display string
        self.argv = argv
        self.log_path = log_path
        self.meta = meta or {}
        self.proc = None
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
        self.run = None               # newest Run (slot free when not running)
        self.history = []             # Run objects, newest first
        self.scheduler = None         # Scheduler or None
        self._load_history()
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
                self.history.append(run)
                if len(self.history) >= 200:
                    break
            except Exception:
                continue

    def get_run(self, rid):
        for r in self.history:
            if r.id == rid:
                return r
        return None

    # ---------------- benchmark slot ----------------

    def slot_busy_reason(self):
        """Why a new benchmark cannot start right now (None = free)."""
        r = self.run
        if r is not None and r.status == "running":
            return "benchmark run {} is still active (started {}, cmd: {})".format(
                r.id, datetime.fromtimestamp(r.t0).strftime("%H:%M:%S"), r.cmd)
        sch = self.scheduler
        if sch is not None and sch.state == "executing":
            return "scheduler task {} is executing (plan {}, stop it first)".format(
                sch.current_task, sch.plan_path)
        return None

    def start_benchmark(self, argv, meta=None):
        with self.lock:
            busy = self.slot_busy_reason()
            if busy:
                raise SlotBusy(busy)
            rid = _new_id("r")
            log_path = os.path.join(RUNS_DIR, rid + ".log")
            run = Run(rid, "benchmark", " ".join(argv), list(argv), log_path, meta)
            try:
                fh = open(log_path, "wb")
            except OSError as exc:
                raise SlotBusy("cannot create log file: {}".format(exc))
            try:
                run.proc = _popen(argv, fh)
            except Exception as exc:
                fh.close()
                run.status = "failed_spawn"
                run.t1 = time.time()
                run.rc = -1
                run.save()
                raise SlotBusy("spawn failed: {}".format(exc))
            self.run = run
            self.history.insert(0, run)
            run.save()
            return run

    def stop_run(self, rid):
        with self.lock:
            run = self.get_run(rid)
            if run is None:
                return None, "no such run: {}".format(rid)
            if run.status != "running" or run.proc is None:
                return run, None  # already finished; not an error
            run.stopping = True
            _kill(run.proc)
            self._reap_remote_async(run)
            return run, None

    def _reap_remote_async(self, run):
        """停止后清理远端残留：按 .state 台账 kill 远端 client 进程
        （cmdline 校验防 pid 重用误杀、带端口的 reader 先优雅 shutdown）、
        清空台账。复用 benchmark.reap_state_entries。daemon 线程执行——
        多节点 SSH 需数秒，不阻塞停止响应。"""
        def worker():
            try:
                import benchmark as bm  # lazy: heavy module
                cluster_path = "configs/cluster.json"
                try:
                    req = (run.meta or {}).get("request") or {}
                    cluster_path = req.get("flags", {}).get("cluster") or cluster_path
                except Exception:
                    pass
                full = os.path.join(ROOT, cluster_path)
                if not os.path.isfile(full):
                    return
                with open(full, "r", encoding="utf-8") as f:
                    cluster = json.load(f)
                bm.reap_state_entries(cluster)
            except Exception as exc:
                sys.stderr.write("[webui] remote cleanup for {}: {}\n".format(
                    run.id, exc))
        threading.Thread(target=worker, daemon=True).start()

    # ---------------- scheduler ----------------

    def start_scheduler(self, plan_path):
        with self.lock:
            if self.scheduler is not None and \
                    self.scheduler.state in ("waiting", "waiting_slot", "executing"):
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
            sch.stop_event.set()
            proc = sch.task_proc
            if proc is not None and proc.poll() is None:
                _kill(proc)
            return sch, None

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
                    busy = self.run is not None and self.run.status == "running"
                    blocker = self.run if busy else None
                if not busy:
                    break
                if sch.state != "waiting_slot":
                    sch.state = "waiting_slot"
                    sch.log("task {} waits: benchmark run {} still active".format(
                        idx, blocker.id))
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
                    proc = _popen(task["command"], f, shell=True)
                    sch.task_proc = proc
                    proc.wait()
                    rc = proc.returncode
            except Exception as exc:
                rc = -1
                sch.log("task {} spawn error: {}".format(idx, exc))
            finally:
                sch.task_proc = None
                sch.current_task = None
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
                    run = self.run
                    if run is not None and run.status == "running":
                        rc = run.proc.poll()
                        if rc is not None:
                            run.rc = rc
                            run.t1 = time.time()
                            run.status = "stopped" if run.stopping else \
                                ("done" if rc == 0 else "failed")
                            run.save()
                        else:
                            run.parse_ts_dir()
            except Exception:
                pass
            time.sleep(1)

    # ---------------- status ----------------

    def status(self):
        with self.lock:
            return {
                "active_run": self.run.to_dict() if (
                    self.run is not None and self.run.status == "running") else None,
                "scheduler": self.scheduler.to_dict() if self.scheduler else None,
            }


MGR = RunManager()
