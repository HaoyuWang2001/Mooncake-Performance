#!/usr/bin/env python3
"""runmgr 逻辑测试：槽位互斥 / 停止 / scheduler 任务执行与等待语义。

不依赖 mock 集群：benchmark 槽用 sleep 假进程模拟。
"""
import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from webui.backend import runmgr


def _sleep_argv(sec=30):
    return [sys.executable, "-c", "import time; time.sleep({})".format(sec)]


def test_slot_mutex_and_stop():
    mgr = runmgr.RunManager()
    run = mgr.start_benchmark(_sleep_argv())
    assert run.status == "running"
    try:
        mgr.start_benchmark(_sleep_argv())
        raise AssertionError("second start should be rejected")
    except runmgr.SlotBusy:
        pass
    mgr.stop_run(run.id)
    for _ in range(50):
        if run.status != "running":
            break
        time.sleep(0.2)
    assert run.status == "stopped", run.status
    # 停止后槽位释放
    run2 = mgr.start_benchmark(_sleep_argv(1))
    mgr.stop_run(run2.id)
    time.sleep(1.5)


def test_scheduler_runs_task_and_waits_for_slot():
    mgr = runmgr.RunManager()
    with tempfile.TemporaryDirectory() as td:
        out = os.path.join(td, "task.out")
        soon = time.strftime("%Y-%m-%d") + " " + time.strftime("%H:%M:%S",
                                                               time.localtime(time.time() + 3))
        plan_path = os.path.join(td, "plan.json")
        with open(plan_path, "w", encoding="utf-8") as f:
            json.dump({"wait_seconds": 1, "on_fail": "continue",
                       "tasks": [{"date": soon.split()[0], "time": soon.split()[1],
                                  "command": "echo scheduler-ok",
                                  "output": out}]}, f)
        blocker = mgr.start_benchmark(_sleep_argv(8))
        sch = mgr.start_scheduler(plan_path)
        # 触发时刻到达但 benchmark 槽被占 -> waiting_slot
        time.sleep(4.5)
        assert sch.state in ("waiting_slot", "executing"), sch.state
        mgr.stop_run(blocker.id)
        # 槽释放后任务执行直至 done
        for _ in range(60):
            if sch.state == "done":
                break
            time.sleep(0.5)
        assert sch.state == "done", sch.state
        content = open(out, encoding="utf-8").read()
        assert "scheduler-ok" in content
        assert "[task 1]" in content   # run_schedule 兼容的分隔头


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
