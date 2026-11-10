"""Dual-sink logging: console summary + outputs/<ts>/benchmark.log detail."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

import utils as u


def test_dual_sink(tmp_path, capsys):
    logf = tmp_path / "benchmark.log"
    u.set_detail_log(str(logf))
    try:
        u.set_verbose(False)
        u.log("console-visible")
        u.log_detail("detail-only")
        out = capsys.readouterr().out
        assert "console-visible" in out
        assert "detail-only" not in out, "non-verbose detail must stay off console"
        text = logf.read_text(encoding="utf-8")
        assert "console-visible" in text
        assert "detail-only" in text, "detail must always land in benchmark.log"
        # verbose -> detail lines also reach the console
        u.set_verbose(True)
        u.log_detail("detail-verbose")
        assert "detail-verbose" in capsys.readouterr().out
    finally:
        u.set_verbose(False)
        u.close_detail_log()
