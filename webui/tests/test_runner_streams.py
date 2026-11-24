#!/usr/bin/env python3
"""三流分离 runner e2e：真实 client + 真实 runner 脚本（密闭环境）。

generate-data 模式不需要 store；MC_FAKE_SDK_GLOG 钩子让真实 client 在 fd1
上输出伪造 glog 行（模拟 C++ SDK 的流）。断言：
- LOG_FILE 只含 Python 流（client-env / 进度 / META），无任何 glog 行；
- LOG_FILE.glog 只含 glog 行；
- 每流文件存在且非空。
"""
import os
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SRC = os.path.join(ROOT, "scripts")
GLOG_RE = re.compile(r"^[EIWF]\d{8}\s")


def _setup(td):
    sd = os.path.join(td, "scripts")
    os.makedirs(sd)
    for f in ("run_benchmark_basic.sh", "run_benchmark_pipeline.sh",
              "mooncake_benchmark_client.py", "bench_etcd.py"):
        shutil.copy2(os.path.join(SRC, f), os.path.join(sd, f))
    with open(os.path.join(sd, "env.sh"), "w", encoding="utf-8") as f:
        f.write('export BENCH_REMOTE_DATA_DIR="{}"\n'.format(
            os.path.join(td, "data")))
    return sd


def test_basic_runner_three_streams(tmp_path):
    sd = _setup(str(tmp_path))
    log_file = os.path.join(str(tmp_path), "out.log")
    env = dict(os.environ)
    env["MC_FAKE_SDK_GLOG"] = "1"
    r = subprocess.run(
        ["bash", os.path.join(sd, "run_benchmark_basic.sh"),
         "generate-data", log_file,
         "--scenario", "t", "--data-size", "8MB", "--key-count", "10"],
        capture_output=True, text=True, env=env, timeout=60)
    assert r.returncode == 0, r.stdout + r.stderr
    app = open(log_file, encoding="utf-8").read()
    glog = open(log_file + ".glog", encoding="utf-8").read()
    # Python stream: env snapshot + progress + META; NO glog lines
    assert "[client-env]" in app
    assert "generate-data done" in app and "generate-data start" in app
    assert "META\t" in app
    for line in app.splitlines():
        assert not GLOG_RE.match(line), "glog leaked into app log: " + line
    # glog stream: ONLY the fake SDK lines
    glog_lines = [ln for ln in glog.splitlines() if ln.strip()]
    assert len(glog_lines) == 2, glog
    assert all(GLOG_RE.match(ln) for ln in glog_lines), glog
    assert "fake sdk glog" in glog and "fake sdk warn" in glog
    # the runner's own env echo stays on the script stdout, out of both files
    assert "[client-env]" not in glog


def test_pipeline_runner_three_streams(tmp_path):
    sd = _setup(str(tmp_path))
    log_file = os.path.join(str(tmp_path), "x.writer.log")
    env = dict(os.environ)
    env["MC_FAKE_SDK_GLOG"] = "1"
    env["MC_GLOG_FILE"] = os.path.join(str(tmp_path), "x.glog")
    # generate-data through the PIPELINE runner (single client path): proves
    # the runner exports MC_WRITER_LOG and captures fd1+fd2 into the glog file
    r = subprocess.run(
        ["bash", os.path.join(sd, "run_benchmark_pipeline.sh"),
         "generate-data", log_file,
         "--scenario", "t", "--data-size", "8MB", "--key-count", "10"],
        capture_output=True, text=True, env=env, timeout=60)
    assert r.returncode == 0, r.stdout + r.stderr
    app = open(log_file, encoding="utf-8").read()
    glog = open(env["MC_GLOG_FILE"], encoding="utf-8").read()
    assert "[client-env]" in app and "generate-data done" in app
    for line in app.splitlines():
        assert not GLOG_RE.match(line), "glog leaked into writer log: " + line
    assert "fake sdk glog" in glog and "fake sdk warn" in glog
    assert "[client-env]" not in glog and "generate-data" not in glog


def test_pipeline_runner_glog_derivation(tmp_path):
    """No MC_GLOG_FILE -> derived as <LOG_FILE basename>.glog."""
    sd = _setup(str(tmp_path))
    log_file = os.path.join(str(tmp_path), "x.dual_n1_c0_p9301.writer.log")
    env = dict(os.environ)
    env.pop("MC_GLOG_FILE", None)
    env["MC_FAKE_SDK_GLOG"] = "1"
    r = subprocess.run(
        ["bash", os.path.join(sd, "run_benchmark_pipeline.sh"),
         "generate-data", log_file,
         "--scenario", "t", "--data-size", "8MB", "--key-count", "5"],
        capture_output=True, text=True, env=env, timeout=60)
    assert r.returncode == 0, r.stdout + r.stderr
    derived = log_file[:-len(".log")] + ".glog"
    assert os.path.isfile(derived), sorted(os.listdir(str(tmp_path)))
    glog = open(derived, encoding="utf-8").read()
    assert "fake sdk glog" in glog
