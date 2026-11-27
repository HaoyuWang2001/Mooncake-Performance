"""通用日志聚合规则：多节点相似行 console 折叠、benchmark.log 全量、错误立即上屏。

Contract (utils.log_agg / flush_agg / flush_agg_all):
  1) log_agg 成功行：非 verbose 时不上 console，逐行全量写 benchmark.log；
     flush_agg 后 console 只出现一条（自定义 summary 或首行 + (+N similar)）；
  2) 错误行（[WARN]/[ERROR]/[FAIL] 前缀或 error=True）绕过折叠，到达即完整
     上屏（并计入组内 error 统计；纯错误组 flush 不再重复打印）；
  3) flush 幂等：组已弹出后再次 flush 无副作用；
  4) flush_agg_all 兜底清空所有组；
  5) verbose 下成功行也走 console（原始流）。
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

import utils as u  # noqa: E402


def _reset():
    with u._LOG_LOCK:
        u._AGG.clear()


def test_log_agg_folds_console_keeps_full_log(tmp_path, capsys):
    logf = tmp_path / "benchmark.log"
    u.set_detail_log(str(logf))
    _reset()
    try:
        u.set_verbose(False)
        for node in ("n1", "n2", "n3"):
            u.log_agg("g", "cleanup {} : /remote/dir".format(node))
        out = capsys.readouterr().out
        assert "cleanup n1" not in out, "folded success lines must stay off console"
        assert "cleanup n2" not in out
        text = logf.read_text(encoding="utf-8")
        for node in ("n1", "n2", "n3"):
            assert "cleanup {} : /remote/dir".format(node) in text, \
                "benchmark.log must keep every original line"
        st = u.flush_agg("g")
        out = capsys.readouterr().out
        assert out.count("\n") == 1, out  # exactly one console line
        assert "cleanup n1 : /remote/dir" in out
        assert "(+2 similar; see benchmark.log)" in out
        assert st["count"] == 3 and st["errors"] == 0
        # the fold summary itself also lands in benchmark.log (digest marker)
        assert "(+2 similar; see benchmark.log)" in \
            logf.read_text(encoding="utf-8")
    finally:
        u.set_verbose(False)
        _reset()
        u.close_detail_log()


def test_log_agg_error_lines_escalate_immediately(tmp_path, capsys):
    logf = tmp_path / "benchmark.log"
    u.set_detail_log(str(logf))
    _reset()
    try:
        u.set_verbose(False)
        u.log_agg("g", "collected n1/mem.host.csv")
        u.log_agg("g", "[WARN] [profile] collect n2/mem.proc.csv: scp failed")
        u.log_agg("g", "ok line", error=True)
        out = capsys.readouterr().out
        assert "[WARN] [profile] collect n2/mem.proc.csv: scp failed" in out, \
            "error-prefixed lines must hit the console immediately, never folded"
        assert "[benchmark] ok line" in out, "error=True escalates too"
        assert "collected n1/mem.host.csv" not in out
        text = logf.read_text(encoding="utf-8")
        assert "collected n1/mem.host.csv" in text  # full log still complete
        st = u.flush_agg("g")  # first=success, 2 errors
        assert st["count"] == 3 and st["errors"] == 2
        out = capsys.readouterr().out
        assert "[2 error line(s) printed above]" in out
    finally:
        u.set_verbose(False)
        _reset()
        u.close_detail_log()


def test_flush_agg_custom_summary_and_idempotent(tmp_path, capsys):
    u.set_detail_log(str(tmp_path / "benchmark.log"))
    _reset()
    try:
        u.log_agg("g", "line one")
        u.log_agg("g", "line two")
        u.flush_agg("g", "collected 8 files from 2 nodes: mem.*×2")
        out = capsys.readouterr().out
        assert "collected 8 files from 2 nodes: mem.*×2" in out
        assert "line one" not in out and "line two" not in out
        # flush again -> no-op, no duplicate console line
        assert u.flush_agg("g") is None
        assert capsys.readouterr().out == ""
    finally:
        _reset()
        u.close_detail_log()


def test_flush_agg_all_drains_every_group(tmp_path, capsys):
    u.set_detail_log(str(tmp_path / "benchmark.log"))
    _reset()
    try:
        u.log_agg("g1", "a one")
        u.log_agg("g1", "a two")
        u.log_agg("g2", "b one")
        u.flush_agg_all()
        out = capsys.readouterr().out
        assert "a one" in out and "(+1 similar; see benchmark.log)" in out
        assert "b one" in out
        with u._LOG_LOCK:
            assert not u._AGG, u._AGG
        # second sweep over empty state prints nothing
        u.flush_agg_all()
        assert capsys.readouterr().out == ""
    finally:
        _reset()
        u.close_detail_log()


def test_log_agg_verbose_shows_raw_stream(tmp_path, capsys):
    u.set_detail_log(str(tmp_path / "benchmark.log"))
    _reset()
    try:
        u.set_verbose(True)
        u.log_agg("g", "verbose visible")
        assert "verbose visible" in capsys.readouterr().out
    finally:
        u.set_verbose(False)
        _reset()
        u.close_detail_log()


def test_flush_agg_pure_error_group_prints_nothing(tmp_path, capsys):
    """组内全是错误行（已逐条上屏）时 flush 不再打印汇总，避免重复。"""
    u.set_detail_log(str(tmp_path / "benchmark.log"))
    _reset()
    try:
        u.set_verbose(False)
        u.log_agg("g", "[ERROR] boom on n1")
        u.log_agg("g", "[ERROR] boom on n2")
        out = capsys.readouterr().out
        assert out.count("[ERROR]") == 2
        st = u.flush_agg("g")
        assert st["count"] == 2 and st["errors"] == 2
        assert capsys.readouterr().out == "", "pure-error group must not fold-print"
    finally:
        u.set_verbose(False)
        _reset()
        u.close_detail_log()
