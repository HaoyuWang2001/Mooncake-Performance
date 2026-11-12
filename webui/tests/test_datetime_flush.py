#!/usr/bin/env python3
"""① datetime 带日期的 RESULT 时间戳 + basic 流式 flush。

- now_hms 输出 `YYYY-MM-DD HH:MM:SS.micro`（7x24h 跨天无歧义）；
- 所有解析器（benchmark.parse_benchmark_log / plot_latency.parse_result_file /
  prof_core.extract_outliers）对新旧双格式兼容；
- basic RESULT 行逐条流式写出（_write_result_row），warmup 不写，
  结束时不再批量补写。
"""
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SCRIPTS = os.path.join(ROOT, "scripts")
sys.path.insert(0, SCRIPTS)
sys.path.insert(0, ROOT)

import mooncake_benchmark_client as mc   # noqa: E402
import benchmark as bm                   # noqa: E402
import plot_latency as pl                # noqa: E402

DATED = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{6}$")


def test_now_hms_is_dated():
    assert DATED.match(mc.now_hms())


def test_basic_streaming_flush_wiring():
    src = open(os.path.join(SCRIPTS, "mooncake_benchmark_client.py"),
               encoding="utf-8").read()
    assert "def _write_result_row" in src
    # per-op streaming via record()
    assert "def record(op_name, key, lat, status):" in src
    assert "record(op, group[0], lat, status)" in src
    assert 'record("put", group[0], lat, status)' in src
    # warmup rows are NOT streamed
    assert "emit_rows=False)" in src
    # the end-of-run bulk writer is gone
    assert "for opname, k, lat, status, ts in results:" not in src


def test_write_result_row_routes_to_role_log(tmp_path):
    wlog = str(tmp_path / "w.log")
    env = dict(os.environ)
    env["MC_WRITER_LOG"] = wlog
    code = (
        "import sys; sys.path.insert(0, {s!r});\n"
        "import mooncake_benchmark_client as mc\n"
        "mc.init_role_logs()\n"
        "mc._write_result_row('put', 'k1', 1.25, 'OK', '2026-11-12 10:00:00.000001')\n"
    ).format(s=SCRIPTS)
    import subprocess
    r = subprocess.run([sys.executable, "-c", code], env=env,
                       capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, r.stderr
    line = open(wlog, encoding="utf-8").read().strip()
    assert line == ("RESULT\t2026-11-12 10:00:00.000001\tput\tk1\t"
                    "1.250000\tOK")
    assert r.stdout == ""   # fd 1 untouched


NEW_LOG = (
    "RESULT\t2026-11-12 10:00:01.000000\tput\tk1\t1.0\tOK\n"
    "RESULT\t2026-11-12 10:00:02.000000\tput\tk2\t2.0\tOK\n"
    "RESULT\t2026-11-12 10:00:03.000000\tput\tk3\t30.0\tERR:-200\n"
    "RESULT\t2026-11-12 10:00:04.000000\tput\tk4\t4.0\tOK\n"
)
OLD_LOG = (
    "RESULT\t10:00:01.000000\tput\tk1\t1.0\tOK\n"
    "RESULT\t10:00:02.000000\tput\tk2\t2.0\tOK\n"
)


def test_parse_benchmark_log_dual_format(tmp_path):
    for name, text, want_ok in (("new.log", NEW_LOG, 3), ("old.log", OLD_LOG, 2)):
        p = tmp_path / name
        p.write_text(text, encoding="utf-8")
        agg = bm.parse_benchmark_log(str(p))
        assert agg["ok"] == want_ok, (name, agg)
        assert agg["total"] == want_ok + (1 if name == "new.log" else 0)
        assert agg["fail"] == (1 if name == "new.log" else 0)


def test_plot_parse_result_file_dual_format(tmp_path):
    p = tmp_path / "new.log"
    p.write_text(NEW_LOG, encoding="utf-8")
    lats, qws, times, counts, qw_times = pl.parse_result_file(str(p))
    assert lats == [1.0, 2.0, 4.0]
    assert counts == {"OK": 3, "ERR:-200": 1}
    assert times == [1.0 + 36000, 2.0 + 36000, 4.0 + 36000]  # 10h + s
    p2 = tmp_path / "old.log"
    p2.write_text(OLD_LOG, encoding="utf-8")
    lats2, _q, times2, _c, _qt = pl.parse_result_file(str(p2))
    assert lats2 == [1.0, 2.0] and times2 == [36001.0, 36002.0]


def test_extract_outliers_dual_format(tmp_path):
    from profiling import prof_core
    d = tmp_path / "logs"
    d.mkdir()
    (d / "a.log").write_text(NEW_LOG, encoding="utf-8")
    (d / "b.log").write_text(OLD_LOG, encoding="utf-8")
    out = prof_core.extract_outliers(str(d), threshold_ms=5.0)
    # only the 30ms row is above the threshold
    assert len(out) == 1
    assert out[0]["key"] == "k3" and out[0]["lat_ms"] == 30.0
    assert out[0]["ts"] == "2026-11-12 10:00:03.000000"
