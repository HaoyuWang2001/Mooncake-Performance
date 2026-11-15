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
    assert out == []                    # 失败的产物不能进回拉清单
    assert any("[WARN] [map] logstats failed" in m for m in msgs), msgs
    out2 = bm.run_tracestats_map({}, node, ["/r/a.glog"], "/scripts", {})
    assert out2 == []
    assert any("[WARN] [map] tracestats failed" in m for m in msgs), msgs


def test_map_and_pull_lists_only_produced_files(monkeypatch, tmp_path):
    """map 失败的产物不进入回拉/拉取清单（否则 pull 直接判场景失败）。"""
    pulled = []
    monkeypatch.setattr(bm, "run_logstats_map",
                        lambda *a, **k: ["/r/a.log.stats.json"])
    monkeypatch.setattr(bm, "run_tracestats_map", lambda *a, **k: [])
    monkeypatch.setattr(bm, "_pipeline_pull_parallel",
                        lambda cluster, groups, local, dry, workers=None:
                        pulled.extend(groups[0][1]) or 0)
    groups = [({"ip": "10.0.0.1"}, ["/r/a.log", "/r/a.log.glog"], "g", "n1")]
    bm._map_and_pull_logs({}, groups, str(tmp_path), False, {}, "/scripts",
                          size="8MB")
    assert pulled == ["/r/a.log.stats.json"]


def _fake_maps(monkeypatch, seen):
    def fake_logstats(cluster, node, logs, scripts, g, dry=False,
                      node_name=None, role=None, size=None, roles=None):
        seen["roles"] = dict(roles or {})
        seen["role"] = role
        seen["logs"] = list(logs)
        return []

    monkeypatch.setattr(bm, "run_logstats_map", fake_logstats)
    monkeypatch.setattr(bm, "run_tracestats_map", lambda *a, **k: [])
    monkeypatch.setattr(bm, "_pipeline_pull_parallel", lambda *a, **k: 0)


def test_explicit_file_roles_reach_map(monkeypatch, tmp_path):
    """dual 模式下 writer/reader 日志必须各自带 role（曾把 reader 标成 writer）。"""
    seen = {}
    _fake_maps(monkeypatch, seen)
    node = {"ip": "10.0.0.1"}
    groups = [(node,
               [("/r/x.dual_n1_c0_p9301.writer.log", "writer"),
                ("/r/x.dual_n1_c0_p9301.reader.log", "reader"),
                ("/r/x.dual_n1_c0_p9301.glog", None)],
               "dual c0", "n1")]
    bm._map_and_pull_logs({}, groups, str(tmp_path), False, {}, "/s",
                          size="8MB")
    assert seen["roles"] == {
        "/r/x.dual_n1_c0_p9301.writer.log": "writer",
        "/r/x.dual_n1_c0_p9301.reader.log": "reader"}
    assert seen["role"] is None
    assert seen["logs"] == ["/r/x.dual_n1_c0_p9301.writer.log",
                            "/r/x.dual_n1_c0_p9301.reader.log"]


def test_role_filename_fallback_still_works(monkeypatch, tmp_path):
    """旧式纯字符串分组仍按文件名兜底识别 role。"""
    seen = {}
    _fake_maps(monkeypatch, seen)
    node = {"ip": "10.0.0.1"}
    groups = [(node, ["/r/x.dual_n1_c0_p9301.writer.log",
                       "/r/x.dual_n1_c0_p9301.glog"], "w", "n1")]
    bm._map_and_pull_logs({}, groups, str(tmp_path), False, {}, "/s",
                          size="8MB")
    assert seen["role"] == "writer" and seen["roles"] == {}
