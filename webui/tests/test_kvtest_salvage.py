#!/usr/bin/env python3
"""/stop 卡死挽救：summary.json 新鲜度判定 + SIGKILL + 严格回退。

背景：镜像 202611250330.c38d27f6 的 kvtest 在 MetricsCollector::Stop 之后
挂死在 NotifyRpcServer::Stop / HttpServer::Stop（keep-alive 连接线程
join 死锁），/stop 有应答但进程不退。数据在挂死前已由 WriteSamples ->
WriteSummary 完整落盘，故以 summary.json 的「基线不存在 -> 现在存在」或
mtime 前进作为挽救依据；不可证实的实例仍走原来的严格失败路径。
"""
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import benchmark as bm  # noqa: E402


def _entry(iid=0, node="node1"):
    return {"iid": iid, "node": node,
            "_out_dir": "/ws/kvtest_out/kv.i0_p9301.kvtest_8KB"}


def _mock_node(monkeypatch):
    monkeypatch.setattr(bm, "get_node",
                        lambda cluster, name: {"ip": "10.0.0.1", "name": name})


def test_summary_stat_parses_size_and_mtime(monkeypatch):
    _mock_node(monkeypatch)
    seen = {}

    def _ssh(ip, node, cmd, **kw):
        seen["cmd"] = cmd
        return "1700000000 4096"

    monkeypatch.setattr(bm, "ssh_check_output", _ssh)
    assert bm._kvtest_summary_stat(None, _entry()) == (1700000000, 4096)
    assert "/ws/kvtest_out/kv.i0_p9301.kvtest_8KB/summary.json" in seen["cmd"]


def test_summary_stat_missing(monkeypatch):
    _mock_node(monkeypatch)
    monkeypatch.setattr(bm, "ssh_check_output",
                        lambda *a, **k: "MISSING")
    assert bm._kvtest_summary_stat(None, _entry()) is None


def test_summary_stat_garbage_and_error(monkeypatch):
    _mock_node(monkeypatch)
    monkeypatch.setattr(bm, "ssh_check_output",
                        lambda *a, **k: "stat: cannot stat: No such file")
    assert bm._kvtest_summary_stat(None, _entry()) is None

    def _boom(*a, **k):
        raise RuntimeError("ssh down")

    monkeypatch.setattr(bm, "ssh_check_output", _boom)
    assert bm._kvtest_summary_stat(None, _entry()) is None


def test_salvage_fresh_from_absent_baseline(monkeypatch):
    _mock_node(monkeypatch)
    killed = []
    monkeypatch.setattr(bm, "_kvtest_summary_stat",
                        lambda cluster, entry: (1700000000, 4096))
    monkeypatch.setattr(
        bm, "ssh_check_output",
        lambda ip, node, cmd, **kw: killed.append(cmd) or "")
    monkeypatch.setattr(bm, "wait_process_exit",
                        lambda *a, **k: True)
    out = bm._kvtest_salvage_stuck(None, [(_entry(), 4242)], {0: None})
    assert out == [(_entry(), 4242)]
    assert killed and "pgrep -P 4242" in killed[0]
    assert "run_kv" in killed[0]          # wrapper fallback guarded by marker


def test_salvage_refuses_stale_summary(monkeypatch):
    _mock_node(monkeypatch)
    killed = []
    monkeypatch.setattr(bm, "_kvtest_summary_stat",
                        lambda cluster, entry: (1699999999, 4096))
    monkeypatch.setattr(bm, "ssh_check_output",
                        lambda *a, **k: killed.append(a) or "")
    monkeypatch.setattr(bm, "wait_process_exit", lambda *a, **k: True)
    out = bm._kvtest_salvage_stuck(None, [(_entry(), 4242)],
                                   {0: (1699999999, 4096)})
    assert out == [] and killed == []


def test_salvage_accepts_newer_mtime(monkeypatch):
    _mock_node(monkeypatch)
    monkeypatch.setattr(bm, "_kvtest_summary_stat",
                        lambda cluster, entry: (1700000005, 4096))
    monkeypatch.setattr(bm, "ssh_check_output", lambda *a, **k: "")
    monkeypatch.setattr(bm, "wait_process_exit", lambda *a, **k: True)
    out = bm._kvtest_salvage_stuck(None, [(_entry(), 1)],
                                   {0: (1700000000, 4096)})
    assert len(out) == 1


def test_salvage_refuses_empty_file(monkeypatch):
    _mock_node(monkeypatch)
    monkeypatch.setattr(bm, "_kvtest_summary_stat",
                        lambda cluster, entry: (1700000000, 0))
    monkeypatch.setattr(bm, "ssh_check_output", lambda *a, **k: "")
    monkeypatch.setattr(bm, "wait_process_exit", lambda *a, **k: True)
    assert bm._kvtest_salvage_stuck(None, [(_entry(), 1)], {0: None}) == []


def test_salvage_mixed_keeps_strict_for_stale(monkeypatch):
    _mock_node(monkeypatch)

    def _stat(cluster, entry):
        return (1700000000, 4096) if entry["iid"] == 0 else None

    monkeypatch.setattr(bm, "_kvtest_summary_stat", _stat)
    monkeypatch.setattr(bm, "ssh_check_output", lambda *a, **k: "")
    monkeypatch.setattr(bm, "wait_process_exit", lambda *a, **k: True)
    out = bm._kvtest_salvage_stuck(
        None, [(_entry(0), 10), (_entry(1), 11)], {0: None, 1: None})
    assert [e["iid"] for e, _pid in out] == [0]


def test_salvage_requires_exit_after_kill(monkeypatch):
    _mock_node(monkeypatch)
    monkeypatch.setattr(bm, "_kvtest_summary_stat",
                        lambda cluster, entry: (1700000000, 4096))
    monkeypatch.setattr(bm, "ssh_check_output", lambda *a, **k: "")
    monkeypatch.setattr(bm, "wait_process_exit", lambda *a, **k: False)
    assert bm._kvtest_salvage_stuck(None, [(_entry(), 1)], {0: None}) == []


def test_salvage_kill_exception_is_not_salvaged(monkeypatch):
    _mock_node(monkeypatch)
    monkeypatch.setattr(bm, "_kvtest_summary_stat",
                        lambda cluster, entry: (1700000000, 4096))

    def _boom(*a, **k):
        raise RuntimeError("exec failed")

    monkeypatch.setattr(bm, "ssh_check_output", _boom)
    assert bm._kvtest_salvage_stuck(None, [(_entry(), 1)], {0: None}) == []
