#!/usr/bin/env python3
"""runmgr 逻辑测试：多实例并发 / 共享资源守卫 / 停止 / scheduler 等待语义。

不依赖 mock 集群：benchmark 槽用 sleep 假进程模拟。
"""
import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from webui.backend import conflicts, runmgr


def _sleep_argv(sec=30):
    return [sys.executable, "-c", "import time; time.sleep({})".format(sec)]


def _ident(kind="k8s", value="c1||ns1"):
    return [{kind: value}]


def _wait_status(run, want, tries=60):
    for _ in range(tries):
        if run.status == want:
            return
        time.sleep(0.2)
    raise AssertionError("status {} != {}".format(run.status, want))


def test_concurrent_runs_with_disjoint_identities():
    mgr = runmgr.RunManager()
    a = mgr.start_benchmark(_sleep_argv(), identities=_ident(value="c1||ns1"))
    b = mgr.start_benchmark(_sleep_argv(), identities=_ident(value="c1||ns2"))
    try:
        assert set(mgr.active) == {a.id, b.id}
        status = mgr.status()
        assert status["active_run"] is not None
        assert len(status["active_runs"]) == 2
        assert a.state_path and a.state_path.endswith(".state")
    finally:
        mgr.stop_run(a.id)
        mgr.stop_run(b.id)
        _wait_status(a, "stopped")
        _wait_status(b, "stopped")
    assert mgr.active == {}


def test_conflict_refuses_same_resource():
    mgr = runmgr.RunManager()
    a = mgr.start_benchmark(_sleep_argv(), identities=_ident(value="c1||ns1"))
    try:
        try:
            mgr.start_benchmark(_sleep_argv(), identities=_ident(value="c1||ns1"))
            raise AssertionError("shared k8s identity must be rejected")
        except runmgr.SlotBusy as exc:
            assert "shared resource" in exc.reason and a.id in exc.reason
    finally:
        mgr.stop_run(a.id)
        _wait_status(a, "stopped")


def test_same_instance_refused_while_running():
    mgr = runmgr.RunManager()
    a = mgr.start_benchmark(_sleep_argv(),
                            meta={"instance": "i1", "instance_name": "A"},
                            identities=_ident(value="c1||ns1"))
    try:
        try:
            mgr.start_benchmark(_sleep_argv(),
                                meta={"instance": "i1"},
                                identities=_ident(value="c9||ns9"))
            raise AssertionError("same instance must not run twice")
        except runmgr.SlotBusy as exc:
            assert "already has an active run" in exc.reason
    finally:
        mgr.stop_run(a.id)
        _wait_status(a, "stopped")


def test_stop_and_slot_release():
    mgr = runmgr.RunManager()
    run = mgr.start_benchmark(_sleep_argv(), identities=_ident())
    assert run.status == "running"
    mgr.stop_run(run.id)
    _wait_status(run, "stopped")
    # after stop the identity is free again
    run2 = mgr.start_benchmark(_sleep_argv(1), identities=_ident())
    mgr.stop_run(run2.id)
    time.sleep(1.5)


def test_scheduler_runs_task_and_waits_for_active_runs():
    mgr = runmgr.RunManager()
    with tempfile.TemporaryDirectory() as td:
        out = os.path.join(td, "task.out")
        soon = time.strftime("%Y-%m-%d") + " " + time.strftime(
            "%H:%M:%S", time.localtime(time.time() + 3))
        plan_path = os.path.join(td, "plan.json")
        with open(plan_path, "w", encoding="utf-8") as f:
            json.dump({"wait_seconds": 1, "on_fail": "continue",
                       "tasks": [{"date": soon.split()[0],
                                  "time": soon.split()[1],
                                  "command": "echo scheduler-ok",
                                  "output": out}]}, f)
        blocker = mgr.start_benchmark(_sleep_argv(8), identities=_ident())
        sch = mgr.start_scheduler(plan_path)
        # trigger time reached but a benchmark is active -> waiting_slot
        time.sleep(4.5)
        assert sch.state in ("waiting_slot", "executing"), sch.state
        mgr.stop_run(blocker.id)
        _wait_status(blocker, "stopped")
        for _ in range(60):
            if sch.state == "done":
                break
            time.sleep(0.5)
        assert sch.state == "done", sch.state
        content = open(out, encoding="utf-8").read()
        assert "scheduler-ok" in content
        assert "[task 1]" in content


if __name__ == "__main__":
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("PASS", name)
            except AssertionError as exc:
                fails += 1
                print("FAIL", name, exc)
    sys.exit(1 if fails else 0)
