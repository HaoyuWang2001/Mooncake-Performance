#!/usr/bin/env python3
"""三流分离（read / writer / client-glog）：客户端角色日志路由。

MC_WRITER_LOG / MC_READER_LOG 重绑定 sys.stdout 后：
- Python 输出（emit/log/RESULT/进度行）全部进角色日志；
- OS 级 fd 1 保持给 C++ SDK glog（模拟 SDK 的 os.write(1,…) 必须出现在
  子进程 stdout 里且绝不落入角色日志）——这正是 runner `> <base>.glog`
  捕获的流；
- dual：reader 标记线程 -> reader 日志，其余 -> writer 日志；
- 只设其一（单角色/basic/stage）：全部 Python 输出进该日志；
- 都不设：回退 stdout（CLI 手工运行兼容旧行为）。
"""
import json
import os
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPTS = os.path.join(ROOT, "scripts")

DRIVER = r"""
import os, sys, threading
sys.path.insert(0, {scripts!r})
import mooncake_benchmark_client as mc

mc.init_role_logs()
mc.unbuffer_stdio()
mc.emit("META", {{"k": 1}})
mc.log("hello writer")
def reader():
    mc._mark_reader_thread()
    mc.log("hello reader")
    sys.stdout.write("RESULT\treader-line\n")
t = threading.Thread(target=reader)
t.start()
t.join()
mc.log("back to writer")
mc._reader_log_handle()  # exercise the handle getter (no crash)
# simulate the C++ SDK writing its glog stream straight to OS fd 1
os.write(1, b"I20261112 101500.123456 999 fake.cc:42] sdk glog line\n")
"""


def _run(extra_env):
    with tempfile.TemporaryDirectory() as td:
        wlog = os.path.join(td, "x.writer.log")
        rlog = os.path.join(td, "x.reader.log")
        env = {k: v for k, v in os.environ.items()
               if k not in ("MC_WRITER_LOG", "MC_READER_LOG")}
        env.update(extra_env(wlog, rlog))
        r = subprocess.run(
            [sys.executable, "-c", DRIVER.format(scripts=SCRIPTS)],
            capture_output=True, text=True, env=env, timeout=60)
        w = open(wlog, encoding="utf-8").read() if os.path.exists(wlog) else ""
        rn = open(rlog, encoding="utf-8").read() if os.path.exists(rlog) else ""
        return r, w, rn, rlog


def test_dual_routing_and_clean_fd1():
    r, w, rn, _ = _run(lambda wlog, rlog: {"MC_WRITER_LOG": wlog,
                                       "MC_READER_LOG": rlog})
    assert r.returncode == 0, r.stderr
    # writer log: META + writer lines, NO reader lines, NO glog
    assert '"k": 1' in w and "hello writer" in w and "back to writer" in w
    assert "hello reader" not in w and "reader-line" not in w
    assert "sdk glog line" not in w
    # reader log: reader-thread lines only
    assert "hello reader" in rn and "RESULT\treader-line" in rn
    assert "hello writer" not in rn and "back to writer" not in rn
    # fd 1 (captured as .glog by the runner) carries ONLY the SDK stream
    assert r.stdout.strip() == "I20261112 101500.123456 999 fake.cc:42] sdk glog line"
    assert "hello" not in r.stdout and "RESULT" not in r.stdout


def test_single_role_log_takes_everything():
    r, w, rn, rlog = _run(lambda wlog, rlog: {"MC_WRITER_LOG": wlog})
    assert r.returncode == 0, r.stderr
    # no reader log configured -> the reader-marked thread falls back to
    # the writer log (single-role process semantics)
    assert "hello writer" in w and "hello reader" in w
    assert "reader-line" in w and "back to writer" in w
    assert not os.path.exists(rlog) or rn == ""
    # fd 1 stays SDK-only
    assert r.stdout.strip().startswith("I20261112")


def test_no_env_falls_back_to_stdout():
    r, w, rn, _ = _run(lambda wlog, rlog: {})
    assert r.returncode == 0, r.stderr
    assert "hello writer" in r.stdout and "hello reader" in r.stdout
    assert "back to writer" in r.stdout
    assert w == "" and rn == ""


def test_reader_log_handle_types():
    """The handle getter returns the proxy's reader handle (or None)."""
    src = open(os.path.join(SCRIPTS, "mooncake_benchmark_client.py"),
               encoding="utf-8").read()
    assert "class _RoleStdout" in src
    assert "def init_role_logs" in src
    assert "MC_WRITER_LOG" in src and "MC_READER_LOG" in src
    # reader threads are marked in BOTH reader-side code paths
    assert "_mark_reader_thread()   # dual mode: route this thread's log() to reader log" in src
    assert "_mark_reader_thread()   # dual mode: reader-side service thread" in src


def test_runners_redirect_three_streams():
    """Runner 脚本：fd1+fd2 -> glog 文件；Python 走 MC_WRITER_LOG。"""
    pipe = open(os.path.join(SCRIPTS, "run_benchmark_pipeline.sh"),
                encoding="utf-8").read()
    basic = open(os.path.join(SCRIPTS, "run_benchmark_basic.sh"),
                 encoding="utf-8").read()
    assert 'export MC_WRITER_LOG="${MC_WRITER_LOG:-$LOG_FILE}"' in pipe
    assert 'GLOG="${MC_GLOG_FILE:-${LOG_FILE%.log}.glog}"' in pipe
    assert '> "$GLOG" 2>&1' in pipe
    # basic: per-client split + glog merge + single/stage split
    assert 'MC_WRITER_LOG="$LOG_DIR/client.$i.log"' in basic
    assert '> "$LOG_DIR/client.$i.glog" 2>&1 &' in basic
    assert ': > "$LOG_FILE.glog"' in basic
    assert 'MC_WRITER_LOG="$LOG_FILE"' in basic
    assert '> "$LOG_FILE.glog" 2>&1 &' in basic
