import sys, os
sys.path.insert(0, '/home/haoyu/Mooncake-Performance')
import benchmark as bm


def test_map_failure_warns_instead_of_typeerror(monkeypatch):
    """失败分支曾因循环变量 log 遮蔽日志函数而抛 'str' object is not callable。"""
    msgs = []
    monkeypatch.setattr(bm, "log", lambda m: msgs.append(m))
    monkeypatch.setattr(bm, "log_detail", lambda m: msgs.append(m))
    monkeypatch.setattr(bm, "resolve_venv", lambda *a, **k: "python3")
    monkeypatch.setattr(bm, "_hifreq_sample_rate", lambda: 1.0)

    def boom(*a, **k):
        raise RuntimeError("ssh down")
    monkeypatch.setattr(bm, "ssh_check_output", boom)
    node = {"ip": "10.0.0.1", "name": "n1"}
    out = bm.run_logstats_map({}, node, ["/r/a.log"], "/scripts", {})
    assert isinstance(out, list)
    assert any("[WARN] [map] logstats failed" in m for m in msgs), msgs
    out2 = bm.run_tracestats_map({}, node, ["/r/a.glog"], "/scripts", {})
    assert isinstance(out2, list)
    assert any("[WARN] [map] tracestats failed" in m for m in msgs), msgs
