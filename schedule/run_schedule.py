#!/usr/bin/env python3
"""run_schedule.py — 夜间定时串行执行器（纯标准库，无第三方依赖）。

按计划文件在指定时刻串行运行命令，命令输出（stdout+stderr）追加写入各自
指定的文件。触发模型（混合）：
  - 任务显式给出 date+time  → 睡到该时刻（本地时区；已过点则立即执行）
  - 任务省略时刻            → 链式：上一条完成时刻 + wait_seconds（已过则立即）
某条命令失败（rc≠0）按 on_fail 决定 continue（记录后继续队列）或 abort。

用法：
  python run_schedule.py --plan plan.json [--dry-run]
  夜间挂法：nohup python run_schedule.py --plan plan.json > schedule.log 2>&1 &

计划文件格式见 plan.example.json。
"""
import argparse
import json
import os
import subprocess
import sys
import time
from datetime import datetime

FMT_DATE = "%Y-%m-%d"
FMT_TIME = "%H:%M:%S"


def log(msg):
    print("[schedule {}] {}".format(datetime.now().strftime("%H:%M:%S"), msg), flush=True)


def parse_trigger(task, idx):
    """Explicit date+time -> datetime; missing -> None (chained)."""
    date, t = task.get("date"), task.get("time")
    if not date and not t:
        return None
    if not (date and t):
        raise SystemExit("[ERROR] task {}: date and time must be given together".format(idx))
    for tf in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime("{} {}".format(date, t), FMT_DATE + " " + tf)
        except ValueError:
            continue
    raise SystemExit("[ERROR] task {}: bad time {!r} (use HH:MM or HH:MM:SS)".format(idx, t))


def load_plan(path):
    with open(path, "r", encoding="utf-8") as f:
        plan = json.load(f)
    tasks = plan.get("tasks") or []
    if not tasks:
        raise SystemExit("[ERROR] plan has no tasks")
    for i, t in enumerate(tasks, 1):
        if not t.get("command"):
            raise SystemExit("[ERROR] task {} missing 'command'".format(i))
        if not t.get("output"):
            raise SystemExit("[ERROR] task {} missing 'output'".format(i))
    return {
        "wait_seconds": float(plan.get("wait_seconds", 60)),
        "on_fail": plan.get("on_fail", "continue"),
        "tasks": tasks,
    }


def wait_until(trigger, label):
    while True:
        remain = (trigger - datetime.now()).total_seconds()
        if remain <= 0:
            return
        if remain > 30:
            log("waiting for [{}] at {} (ETA in {:.0f}s)".format(
                label, trigger.strftime("%Y-%m-%d %H:%M:%S"), remain))
            time.sleep(min(30, remain - 1))
        else:
            time.sleep(remain)


def run_task(idx, task, now_str):
    out_path = task["output"]
    out_dir = os.path.dirname(os.path.abspath(out_path))
    os.makedirs(out_dir, exist_ok=True)
    log("task {} START: {}".format(idx, task["command"]))
    t0 = time.time()
    with open(out_path, "a", encoding="utf-8") as f:
        f.write("===== [{}] [task {}] {}\n".format(now_str, idx, task["command"]))
        f.flush()
        proc = subprocess.run(task["command"], shell=True,
                              stdout=f, stderr=subprocess.STDOUT)
    elapsed = time.time() - t0
    end_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(out_path, "a", encoding="utf-8") as f:
        f.write("===== [{}] [task {}] rc={} elapsed={:.1f}s\n".format(
            end_str, idx, proc.returncode, elapsed))
    log("task {} DONE: rc={} elapsed={:.1f}s -> {}".format(
        idx, proc.returncode, elapsed, out_path))
    return proc.returncode, elapsed


def main(argv=None):
    parser = argparse.ArgumentParser(description="Night-time serial command scheduler")
    parser.add_argument("--plan", required=True, help="plan JSON file")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the resolved schedule and exit")
    args = parser.parse_args(argv)

    plan = load_plan(args.plan)
    wait_seconds = plan["wait_seconds"]
    on_fail = plan["on_fail"]
    if on_fail not in ("continue", "abort"):
        raise SystemExit("[ERROR] on_fail must be 'continue' or 'abort'")

    # 解析触发时刻：显式时刻即刻确定；链式任务标记为相对于上一条完成
    resolved = []
    for i, t in enumerate(plan["tasks"], 1):
        trigger = parse_trigger(t, i)
        resolved.append({"idx": i, "task": t, "trigger": trigger})
    if resolved[0]["trigger"] is None:
        raise SystemExit("[ERROR] the FIRST task must carry date+time")

    print("plan: {} tasks, wait_seconds={}, on_fail={}".format(
        len(resolved), wait_seconds, on_fail))
    for r in resolved:
        when = r["trigger"].strftime("%Y-%m-%d %H:%M:%S") if r["trigger"] \
            else "chained (prev done + {}s)".format(wait_seconds)
        print("  task {} @ {}: {}".format(r["idx"], when, r["task"]["command"]))
        print("             output -> {}".format(r["task"]["output"]))
    if args.dry_run:
        print("dry-run complete, nothing executed")
        return

    summary = []
    prev_end = None
    aborted = False
    for r in resolved:
        idx, task = r["idx"], r["task"]
        trigger = r["trigger"]
        if trigger is None:
            trigger = prev_end + timedelta_s(wait_seconds)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        wait_until(trigger, "task {}".format(idx))
        rc, elapsed = run_task(idx, task, now_str)
        prev_end = datetime.now()
        summary.append((idx, task["command"], rc, elapsed, task["output"]))
        if rc != 0 and on_fail == "abort":
            log("on_fail=abort: stopping the queue after failed task {}".format(idx))
            aborted = True
            break

    print("summary:")
    for idx, cmd, rc, elapsed, out in summary:
        print("  task {}: rc={} elapsed={:.1f}s -> {}".format(idx, rc, elapsed, out))
    print("queue finished{} ({} tasks ran)".format(
        " (aborted)" if aborted else "", len(summary)))


def timedelta_s(seconds):
    from datetime import timedelta
    return timedelta(seconds=seconds)


if __name__ == "__main__":
    main()
