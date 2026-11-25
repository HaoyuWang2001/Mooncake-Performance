#!/usr/bin/env python3
"""runmgr 逻辑测试：多实例并发 / 共享资源守卫 / 停止 / scheduler 等待语义。

不依赖 mock 集群：benchmark 槽用 sleep 假进程模拟。
"""
import json
import os
import subprocess
import sys
import tempfile
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from webui.backend import conflicts, runmgr


@pytest.fixture(autouse=True)
def _isolated_runs_dir(tmp_path, monkeypatch):
    """恢复逻辑会扫描 RUNS_DIR 里的 running 记录：每个用例用独立目录，
    避免测试之间互相接管残留进程。"""
    d = tmp_path / "runs"
    d.mkdir()
    monkeypatch.setattr(runmgr, "RUNS_DIR", str(d))
    yield d


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


def _orphan_json(d, rid, status="running", meta=None):
    with open(os.path.join(str(d), rid + ".json"), "w", encoding="utf-8") as f:
        json.dump({"id": rid, "kind": "benchmark", "cmd": "orphan",
                   "argv": [], "status": status, "rc": None,
                   "t0": time.time(), "meta": meta or {}}, f)


def test_restart_recovers_orphan_run_and_stop(tmp_path, monkeypatch):
    """webui 重启后：按 --stop-file 定位遗留进程并接管；stop 可正常触发。"""
    d = tmp_path / "runs"
    rid = "rORPHAN"
    stop = d / (rid + ".stop")
    _orphan_json(d, rid, meta={"instance": "i1", "request": {"flags": {}}})
    (d / (rid + ".log")).write_text("[benchmark] running\n", encoding="utf-8")
    # 模拟 runmgr 的拉起方式：独立会话（webui 重启后仍存活）
    proc = subprocess.Popen([sys.executable, "-c",
                             "import time; time.sleep(60)",
                             "--stop-file", str(stop)],
                            start_new_session=True)
    try:
        mgr = runmgr.RunManager()
        assert rid in mgr.active, "orphan benchmark must be re-adopted"
        assert mgr.active[rid].pid == proc.pid
        assert mgr.active[rid].stop_path == str(stop)
        run, err = mgr.stop_run(rid)
        assert err is None and run.stopping
        deadline = time.time() + 15
        while time.time() < deadline and rid in mgr.active:
            time.sleep(0.2)
        assert rid not in mgr.active, "adopted run must finish after stop"
        saved = json.load(open(d / (rid + ".json"), encoding="utf-8"))
        assert saved["status"] == "stopped"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def test_restart_marks_processless_running_terminal(tmp_path, monkeypatch):
    """running 记录无对应进程：按日志终态收敛（而不是永久卡运行中）。"""
    d = tmp_path / "runs"
    _orphan_json(d, "rDEAD1")
    (d / "rDEAD1.log").write_text("[benchmark] no summary here\n",
                                  encoding="utf-8")
    _orphan_json(d, "rDEAD2")
    (d / "rDEAD2.log").write_text(
        "[benchmark] SUMMARY: all 1 scenarios OK\n", encoding="utf-8")
    # 日志置为过期（新鲜日志会被「不误判」保护保持 running，见下一个用例）
    old = time.time() - 3600
    os.utime(str(d / "rDEAD1.log"), (old, old))
    os.utime(str(d / "rDEAD2.log"), (old, old))
    mgr = runmgr.RunManager()
    assert not mgr.active
    assert json.load(open(d / "rDEAD1.json"))["status"] == "aborted"
    assert json.load(open(d / "rDEAD2.json"))["status"] == "done"


def test_stop_orphan_without_pid_writes_stop_file(tmp_path, monkeypatch):
    """定位不到 pid 时：写 stop 文件（benchmark hold 循环轮询它）+ 状态收敛。"""
    d = tmp_path / "runs"
    mgr = runmgr.RunManager()
    rid = "rORPHAN2"
    stop = d / (rid + ".stop")
    run = runmgr.Run(rid, "benchmark", "orphan", [],
                     str(d / (rid + ".log")),
                     meta={}, stop_path=str(stop),
                     state_path=str(d / (rid + ".state")))
    run.status = "running"
    run.save()
    mgr.history.insert(0, run)
    out, err = mgr.stop_orphan(rid)
    assert err is None and out.stopping
    assert stop.exists(), "stop file is the cooperative teardown channel"
    assert out.status == "stopped"


def _run_json(d, rid, status, pid=None, meta=None, t0=None):
    with open(os.path.join(str(d), rid + ".json"), "w", encoding="utf-8") as f:
        json.dump({"id": rid, "kind": "benchmark", "cmd": "x", "argv": [],
                   "status": status, "rc": None, "pid": pid,
                   "t0": t0 or time.time(), "meta": meta or {}}, f)


def test_reattach_repairs_wrongly_finalized_run(tmp_path, monkeypatch):
    """被误判成 stopped 的 run：进程仍在 → reattach 修回 running 并可停止。"""
    d = tmp_path / "runs"
    monkeypatch.setattr(runmgr, "RUNS_DIR", str(d))
    rid = "rREATTACH"
    stop = d / (rid + ".stop")
    _run_json(d, rid, "stopped", meta={"instance": "i1",
                                      "request": {"flags": {}}})
    (d / (rid + ".log")).write_text("[benchmark] running\n", encoding="utf-8")
    proc = subprocess.Popen([sys.executable, "-c",
                             "import time; time.sleep(60)",
                             "--stop-file", str(stop)],
                            start_new_session=True)
    try:
        mgr = runmgr.RunManager()
        assert rid not in mgr.active              # 终态记录启动时不接管
        run, err = mgr.reattach(rid)
        assert err is None and run.status == "running"
        assert rid in mgr.active and run.pid == proc.pid
        assert json.load(open(d / (rid + ".json")))["status"] == "running"
        # 幂等
        run2, err = mgr.reattach(rid)
        assert err is None and run2.status == "running"
        # 停止仍然可用
        mgr.stop_run(rid)
        deadline = time.time() + 15
        while time.time() < deadline and rid in mgr.active:
            time.sleep(0.2)
        assert json.load(open(d / (rid + ".json")))["status"] == "stopped"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)


def test_reattach_without_process_keeps_status(tmp_path, monkeypatch):
    d = tmp_path / "runs"
    monkeypatch.setattr(runmgr, "RUNS_DIR", str(d))
    _run_json(d, "rGONE", "stopped")
    mgr = runmgr.RunManager()
    run, err = mgr.reattach("rGONE")
    assert err is None and run.status == "stopped"
    assert "rGONE" not in mgr.active
    assert json.load(open(d / "rGONE.json"))["status"] == "stopped"


def test_recover_keeps_fresh_log_running_then_finalizes_stale(tmp_path, monkeypatch):
    """启动恢复：无 pid 但日志仍新鲜 → 保持 running（不误判）；日志过期 → 终态。"""
    d = tmp_path / "runs"
    monkeypatch.setattr(runmgr, "RUNS_DIR", str(d))
    fresh = d / "rFRESH.log"
    stale = d / "rSTALE.log"
    fresh.write_text("[benchmark] ticking\n", encoding="utf-8")
    stale.write_text("[benchmark] no summary\n", encoding="utf-8")
    old = time.time() - 3600
    os.utime(str(stale), (old, old))
    _run_json(d, "rFRESH", "running")
    _run_json(d, "rSTALE", "running")
    mgr = runmgr.RunManager()
    assert not mgr.active
    assert json.load(open(d / "rFRESH.json"))["status"] == "running"
    assert json.load(open(d / "rSTALE.json"))["status"] == "aborted"


def test_parse_ts_dir_accepts_old_and_new_log_markers(tmp_path, monkeypatch):
    """日志文案改版（results root -> Results）后 ts_dir 仍能解析（产物/报表入口）。"""
    from webui.backend.paths import ROOT
    d = tmp_path / "runs"
    monkeypatch.setattr(runmgr, "RUNS_DIR", str(d))
    abs_ts = os.path.join(ROOT, "outputs", "20260101-0000")
    for rid, marker in (("rOLD", "results root: "), ("rNEW", "Results: ")):
        log = d / (rid + ".log")
        log.write_text("[benchmark] {} {}\n".format(marker, abs_ts),
                       encoding="utf-8")
        run = runmgr.Run(rid, "benchmark", "x", [], str(log))
        run.parse_ts_dir()
        assert run.ts_dir == "outputs/20260101-0000", rid


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
