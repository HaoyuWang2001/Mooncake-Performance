#!/usr/bin/env python3
"""run 日志分页：stream=all|rank|recall 过滤 + 行边界分页不丢不重。"""
import io
import os
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from webui.backend import runmgr
from webui.backend.routers import runs as rr


class _Run(object):
    def __init__(self, log_path):
        self.log_path = str(log_path)
        self.id = "rtest"


def _install(monkeypatch, tmp_path, text, chunk=64):
    p = tmp_path / "r.log"
    p.write_text(text, encoding="utf-8")
    run = _Run(p)
    monkeypatch.setattr(runmgr.MGR, "get_run", lambda rid: run)
    monkeypatch.setattr(rr, "MAX_TEXT_CHUNK", chunk)
    return run


def _page_all(monkeypatch, run, stream, chunk):
    monkeypatch.setattr(rr, "MAX_TEXT_CHUNK", chunk)
    text, offset, pages = "", 0, 0
    while True:
        r = rr._read_log_page(run.log_path, offset, stream)
        text += r["content"]
        offset = r["offset"]
        pages += 1
        assert pages < 10000, "offset did not advance"
        if r["eof"]:
            break
    return text, offset, pages


LOG = (
    "parent line one\n"
    "[rank] rank-a\n"
    "[recall] recall-a\n"
    "parent line two\n"
    "[rank] rank-b\n"
    "[recall] recall-b\n"
    "[rank] rank-c\n"
)


def test_stream_split_and_prefix_strip(tmp_path, monkeypatch):
    run = _install(monkeypatch, tmp_path, LOG, chunk=4096)
    rank = rr._read_log_page(run.log_path, 0, "rank")
    recall = rr._read_log_page(run.log_path, 0, "recall")
    allv = rr._read_log_page(run.log_path, 0, "all")
    assert rank["content"] == "rank-a\nrank-b\nrank-c\n"
    assert recall["content"] == "recall-a\nrecall-b\n"
    assert "[rank]" in allv["content"] and "parent line one" in allv["content"]
    assert rank["size"] == recall["size"] == allv["size"] == len(LOG.encode())


def test_filtered_pagination_is_lossless(tmp_path, monkeypatch):
    run = _install(monkeypatch, tmp_path, LOG)
    for chunk in (7, 11, 16, 23, 64):
        rank, off, pages = _page_all(monkeypatch, run, "rank", chunk)
        assert rank == "rank-a\nrank-b\nrank-c\n", "chunk={}".format(chunk)
        assert off == len(LOG.encode())
        assert pages > 1 or chunk >= 64
        recall, _, _ = _page_all(monkeypatch, run, "recall", chunk)
        assert recall == "recall-a\nrecall-b\n", "chunk={}".format(chunk)


def test_line_boundary_never_splits_a_prefix(tmp_path, monkeypatch):
    """A chunk boundary inside "[rank] " must not leak a partial line or a
    false prefix match — the partial line is re-read next page."""
    run = _install(monkeypatch, tmp_path, LOG)
    for chunk in range(1, 10):
        text, _, _ = _page_all(monkeypatch, run, "rank", chunk)
        assert text == "rank-a\nrank-b\nrank-c\n", "chunk={}".format(chunk)


def test_all_stream_keeps_raw_bytes(tmp_path, monkeypatch):
    run = _install(monkeypatch, tmp_path, LOG)
    text, off, _ = _page_all(monkeypatch, run, "all", 13)
    assert text == LOG
    assert off == len(LOG.encode())


def test_api_validates_stream_and_missing_run(tmp_path, monkeypatch):
    run = _install(monkeypatch, tmp_path, LOG)
    try:
        rr.api_run_log(None, {"stream": ["bogus"]}, "rtest")
        raise AssertionError("bad stream must 400")
    except Exception as exc:
        assert "stream must be" in str(exc)
    monkeypatch.setattr(runmgr.MGR, "get_run", lambda rid: None)
    try:
        rr.api_run_log(None, {}, "nope")
        raise AssertionError("missing run must 404")
    except Exception as exc:
        assert "no such run" in str(exc)
