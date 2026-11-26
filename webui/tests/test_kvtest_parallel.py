#!/usr/bin/env python3
"""R1 并行旋钮与 gzip 助手：knob 读取/默认值/回退、in-pod gzip 回退、本地 gunzip。"""
import gzip
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import benchmark as bm  # noqa: E402


def test_parallel_knob_defaults():
    # module defaults (restore in case a previous test overrode them)
    bm._PIPELINE_PARALLEL = 16
    bm._PULL_WORKERS = 10
    bm.apply_parallel_knobs({})
    assert bm._PIPELINE_PARALLEL == 16
    assert bm._PULL_WORKERS == 10
    bm.apply_parallel_knobs({"parallel": {"fanout_workers": 32,
                                          "pull_workers": 8}})
    assert bm._PIPELINE_PARALLEL == 32
    assert bm._PULL_WORKERS == 8
    # invalid values keep the previous setting
    bm.apply_parallel_knobs({"parallel": {"fanout_workers": "abc",
                                          "pull_workers": None}})
    assert bm._PIPELINE_PARALLEL == 32
    assert bm._PULL_WORKERS == 8
    # floor at 1
    bm.apply_parallel_knobs({"parallel": {"fanout_workers": 0}})
    assert bm._PIPELINE_PARALLEL == 1


def test_gzip_remote_files_is_strict(monkeypatch):
    calls = []

    def fake_ssh(ip, node, cmd, **kw):
        calls.append(cmd)
        raise RuntimeError("exec down")

    monkeypatch.setattr(bm, "ssh_check_output", fake_ssh)
    monkeypatch.setattr(bm, "get_node",
                        lambda cluster, name: {"ip": "1.2.3.4"})
    # exec failure -> hard error (no uncompressed fallback)
    try:
        bm._kvtest_gzip_remote_files(None, "n1", ["/r/a.samples.csv"])
        raise AssertionError("should raise")
    except RuntimeError as exc:
        assert "gzip failed" in str(exc)
    # success -> .gz paths
    monkeypatch.setattr(
        bm, "ssh_check_output",
        lambda ip, node, cmd, **kw: calls.append(cmd))
    out = bm._kvtest_gzip_remote_files(
        None, "n1", ["/r/a.samples.csv", "/r/a.trace.json"])
    assert out == ["/r/a.samples.csv.gz", "/r/a.trace.json.gz"]
    assert any("gzip -f /r/a.samples.csv" in c for c in calls)
    # empty input
    assert bm._kvtest_gzip_remote_files(None, "n1", []) == []


def test_gunzip_local_dir(tmp_path):
    src = tmp_path / "a.samples.csv"
    src.write_text("hello,csv\n")
    with gzip.open(str(tmp_path / "a.samples.csv.gz"), "wb") as f:
        f.write(b"hello,csv\n")
    src.unlink()
    (tmp_path / "keep.txt").write_text("stay")
    bm._gunzip_local_dir(str(tmp_path))
    assert src.read_text() == "hello,csv\n"
    assert not (tmp_path / "a.samples.csv.gz").exists()
    assert (tmp_path / "keep.txt").read_text() == "stay"
