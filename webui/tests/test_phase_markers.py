#!/usr/bin/env python3
"""① 阶段标记（MC_PHASE）：stderr 输出、每 (phase,action) 进程内一次、
5 组调用点接线。

标记写给 stderr（runner 把 fd1+fd2 收进 .glog，与 SDK trace 同流），
tracestats 据此把 breakdown 行分类进 setup/warmup/measured/teardown。
"""
import os
import re
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CLIENT = os.path.join(ROOT, "scripts", "mooncake_benchmark_client.py")

MARK_RE = re.compile(
    r"^MC_PHASE (warmup|measured) (start|end) (\d+\.\d+)$")

DRIVER = r"""
import sys, threading
sys.path.insert(0, {scripts!r})
import mooncake_benchmark_client as mc

# 1) format + epoch sanity
mc._phase_marker("warmup", "start")
mc._phase_marker("measured", "start")
mc._phase_marker("measured", "end")

# 2) once-semantics under a race: 32 threads all try the same pair
def hammer():
    mc._phase_marker("measured", "start")
    mc._phase_marker("warmup", "end")   # never emitted before -> once
threads = [threading.Thread(target=hammer) for _ in range(32)]
for t in threads: t.start()
for t in threads: t.join()
"""


def _run():
    r = subprocess.run(
        [sys.executable, "-c", DRIVER.format(scripts=os.path.join(ROOT, "scripts"))],
        capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, r.stderr
    marks = [ln for ln in r.stderr.splitlines() if ln.startswith("MC_PHASE")]
    return marks, r


def test_phase_marker_format_and_epoch():
    marks, _ = _run()
    ts = []
    for m in marks:
        mm = MARK_RE.match(m)
        assert mm, "bad marker line: {!r}".format(m)
        ts.append(float(mm.group(3)))
    assert ts == sorted(ts)             # monotonic wall clock
    assert 1e9 < ts[0] < 1e12           # epoch seconds sanity


def test_phase_marker_once_semantics():
    marks, _ = _run()
    # exactly one of each pair despite the 32-thread hammer
    assert marks.count("MC_PHASE warmup start") == 0 or \
        sum(1 for m in marks if m == "MC_PHASE warmup start") == 1
    seen = set()
    for m in marks:
        mm = MARK_RE.match(m)
        seen.add((mm.group(1), mm.group(2)))
    assert seen == {("warmup", "start"), ("measured", "start"),
                    ("measured", "end"), ("warmup", "end")}
    assert len(marks) == 4              # no duplicates from the race


def test_phase_markers_go_to_stderr_not_stdout():
    marks, r = _run()
    assert marks and not any(ln.startswith("MC_PHASE")
                             for ln in r.stdout.splitlines())


def test_all_call_sites_wired():
    """源级断言：9 个调用点（basic 3 / writer 3 / notify 1 / reader end 2）。"""
    src = open(CLIENT, encoding="utf-8").read()
    assert src.count("_phase_marker(") == 10   # 1 def + 9 call sites
    for needle in (
            '_phase_marker("warmup", "start")   # warmup window opens (incl. prefill puts)',
            '_phase_marker("measured", "start")   # measured window opens',
            '_phase_marker("measured", "end")     # measured window closed',
            '_phase_marker("warmup", "start")   # warmup window opens',
            '_phase_marker("measured", "start")     # measured window opens',
            '_phase_marker("measured", "end")       # measured window closed',
            '_phase_marker("warmup" if is_warmup else "measured", "start")',
            '_phase_marker("measured", "end")   # reader window closed (pure-reader pods)',
            '_phase_marker("measured", "end")   # no-op when the writer already ended'):
        assert needle in src, needle
    # marker writes to stderr (fd2 -> runner's glog redirect), never stdout
    assert 'sys.stderr.write("MC_PHASE ' in src
