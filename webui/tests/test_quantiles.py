#!/usr/bin/env python3
"""统一分位口径：REQ sketch 封装、map/reduce/CSV 接线与优雅降级。

本机（WSL）没有 datasketches，因此用可注入的 fake 库验证接线逻辑；
真实库路径用 importorskip 的用例覆盖（在装了库的环境执行）。
"""
import json
import os
import sys

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import benchmark as bm  # noqa: E402
import logstats as ls   # noqa: E402
import quantiles as qt  # noqa: E402
import report as rp     # noqa: E402


class _FakeSketch:
    """Deterministic stand-in: keeps the values, exact order-statistic query."""
    __slots__ = ("k", "v")

    def __init__(self, k=12):
        self.k = k
        self.v = []

    def update(self, x):
        self.v.append(float(x))

    def merge(self, other):
        self.v.extend(other.v)

    def get_quantile(self, q):
        s = sorted(self.v)
        if not s:
            return 0.0
        idx = min(len(s) - 1, max(0, int(round(q * (len(s) - 1)))))
        return s[idx]

    def serialize(self):
        return json.dumps({"k": self.k, "v": self.v}).encode("utf-8")

    @classmethod
    def deserialize(cls, raw):
        doc = json.loads(raw.decode("utf-8"))
        sk = cls(doc["k"])
        sk.v = [float(x) for x in doc["v"]]
        return sk

    def get_RSE(self):
        return 0.0


class _FakeDS:
    req_floats_sketch = _FakeSketch


@pytest.fixture
def fake_lib(monkeypatch):
    monkeypatch.setattr(qt, "_lib", lambda: _FakeDS)
    return _FakeDS


def test_wrapper_roundtrip_and_merge(fake_lib):
    sk = qt.make(24)
    assert sk is not None and sk.k == 24
    for v in (1.0, 2.0, 3.0):
        qt.update(sk, v)
    b64 = qt.serialize(sk)
    assert b64 and qt.quantile_from([b64], 0.5) == 2.0
    other = qt.make(24)
    for v in (4.0, 5.0):
        qt.update(other, v)
    merged = qt.merge([b64, qt.serialize(other)])
    assert qt.quantile(merged, 0.999) == 5.0
    assert qt.resolve_k(None) == qt.DEFAULT_K
    assert qt.resolve_k(3) == 4 and qt.resolve_k(9999) == 512


def test_wrapper_degrades_without_library(monkeypatch):
    monkeypatch.setattr(qt, "_lib", lambda: None)
    assert qt.available() is False
    assert qt.make(24) is None
    assert qt.quantile_from(["x"], 0.99) is None
    assert qt.is_tail(0.99) and not qt.is_tail(0.50)


def test_unified_pct_routing(fake_lib):
    hist = {ls.bin_of(1.0, ls.BIN_EDGES_V2): 2,
            ls.bin_of(100.0, ls.BIN_EDGES_V2): 1}
    sk = qt.make(24)
    for v in (1.0, 2.0, 100.0):
        qt.update(sk, v)
    items = [qt.serialize(sk)]
    # 中心：直方图插值（<实际值，受桶宽影响）；尾部：sketch 精确
    assert ls.unified_pct(hist, items, 0.50) == pytest.approx(
        ls.hist_percentile(hist, 0.50, ls.BIN_EDGES_V2))
    assert ls.unified_pct(hist, items, 0.99) == 100.0
    # 无 sketch -> 尾部回退直方图
    assert ls.unified_pct(hist, None, 0.99) == pytest.approx(
        ls.hist_percentile(hist, 0.99, ls.BIN_EDGES_V2))


def _log(path, rows):
    path.write_text("".join(
        "RESULT\t2026-11-12 10:00:{:02d}.{:06d}\tput\tk{}\t{:.3f}\tOK\n"
        .format(i % 60, i, i, v) for i, v in enumerate(rows)), encoding="utf-8")
    return str(path)


def test_compute_stats_digest_block_and_warmup_excluded(tmp_path, fake_lib):
    p = _log(tmp_path / "a.log", [1.0, 2.0, 3.0, 4.0])
    # warmup_cutoff 用 epoch：fixture 的 ts 是 10:00:xx（本地时区）→ 取前两行的
    # epoch 作为 cutoff 边界
    st_all = ls.compute_stats(p, digest_k=24)
    assert st_all["digest"]["fmt"] == qt.FMT
    assert st_all["digest"]["k"] == 24
    assert st_all["digest"]["global"]
    assert "put" in st_all["digest"]["ops"]
    cut = st_all["meta"]["ts_from"] + 2.5      # 前三秒为 warmup -> 只留 4.0
    st = ls.compute_stats(p, digest_k=24, warmup_cutoff=cut)
    assert st["meta"]["warmup_rows"] == 3
    vals = json.loads(__import__("base64").b64decode(
        st["digest"]["global"]).decode("utf-8"))["v"]
    assert vals == [4.0]                       # warmup 行不进 digest
    assert st["global"]["ok"] == 1


def test_compute_stats_without_library_has_no_digest(tmp_path, monkeypatch):
    monkeypatch.setattr(qt, "_lib", lambda: None)
    p = _log(tmp_path / "b.log", [1.0, 2.0])
    st = ls.compute_stats(p, digest_k=24)
    assert "digest" not in st
    assert st["global"]["p99_ms"] >= 0


def test_summary_of_prefers_digest_tails(tmp_path, fake_lib):
    p = _log(tmp_path / "c.log", [1.0, 2.0, 50.0])
    st = ls.compute_stats(p, digest_k=24)
    out = rp._summary_of([st])
    assert out["pct_method"] == "hist+digest"
    assert out["pct_k"] == 24
    assert out["p99_ms"] == 50.0               # sketch 尾部（hist 会偏低）
    assert out["p50_ms"] == round(st["global"]["p50_ms"], 4)


def test_summary_of_hist_when_library_missing(tmp_path, fake_lib, monkeypatch):
    p = _log(tmp_path / "d.log", [1.0, 2.0, 50.0])
    st = ls.compute_stats(p, digest_k=24)      # 产物带 digest
    monkeypatch.setattr(qt, "_lib", lambda: None)
    out = rp._summary_of([st])                 # reduce 环境无库 -> 只能 hist
    assert out["pct_method"] == "hist"
    assert out["p99_ms"] == round(
        ls.hist_percentile(st["histogram"], 0.99, ls.BIN_EDGES_V2), 4)


def test_instance_ops_stats_streaming(tmp_path, fake_lib):
    samples = tmp_path / "kv.i0_p9301.kvtest_8MB.samples.csv"
    samples.write_text(
        "elapsed_us,instance_id,operation,request_index,latency_ms,status,bytes\n"
        "1000000,0,upsert_from,0,10.0,OK,1024\n"
        "2000000,0,upsert_from,1,20.0,OK,1024\n"
        "3000000,0,upsert_from,2,5000.0,ERR,0\n"
        "6000000,0,upsert_from,3,30.0,OK,1024\n", encoding="utf-8")
    stats = ls.kvtest_instance_ops_stats(
        str(samples), {"upsert_from"}, warmup_sec=5.0, digest_k=24)
    op = stats["upsert_from"]
    assert op["total"] == 1 and op["success"] == 1     # 只留 >=5s 的正式行
    assert op["avg_ms"] == 30.0 and op["max_ms"] == 30.0
    assert op[">2000us"] == 1 and op[">20000us"] == 1   # 30ms 超两档
    assert op["hist"] and op["digest"]
    assert op["p99_ms"] == 30.0                        # sketch 尾部


def test_csv_consumes_controller_instances_block(tmp_path, fake_lib):
    """controller 模式：CSV 实例行/TOTAL 与报表同源（instances 块 + 同一 policy）。"""
    logs = tmp_path / "logs"
    logs.mkdir()
    # summary 的数值故意写错：实例行必须用 instances 块
    base = bm._kvtest_artifact_base("kv_inst", 0, 9301, "8MB")
    (logs / (base + ".summary.json")).write_text(json.dumps({
        "instance_id": 0, "elapsed_seconds": 10.0, "verify_failures": 0,
        "operations": {"upsert_from": {"total": 2, "success": 2,
                                       "failure": 0, "bytes": 2048,
                                       "avg_ms": -1, "p50_ms": -1}}}),
        encoding="utf-8")
    # controller reduce 产物：instances 块
    rep = logs / "report" / "8MB"
    rep.mkdir(parents=True)
    (rep / "reduced.json").write_text(json.dumps({
        "sizes": {"8MB": {"role_summaries": {}}},
        "instances": [{"iid": 0, "node": "n1", "pod": "p1", "ops": {
            "upsert_from": {"total": 2, "success": 2, "failure": 0,
                            "bytes": 2048, "avg_ms": 1.5, "min_ms": 1.0,
                            "max_ms": 2.0,
                            "hist": {str(ls.bin_of(1.0, ls.BIN_EDGES_V2)): 1,
                                     str(ls.bin_of(2.0, ls.BIN_EDGES_V2)): 1},
                            "p50_ms": 1.4, "p90_ms": 1.9, "p95_ms": 1.95,
                            "p99_ms": 2.0, "p999_ms": 2.0, "p9999_ms": 2.0}}}]}),
        encoding="utf-8")
    cfg = {"scenario": "kvtest", "work_mode": "rank", "protocol": "ub",
           "data_size": ["8MB"], "duration": 30, "batch_size": 1,
           "notify_count": 1, "writer": {"operation": "upsert_from"},
           "reader": {"operation": "get_buffer"},
           "client": {"count_per_node": 1, "port_base": 9301},
           "_kvtest_iid_nodes": {"0": "n1"}}
    out = bm.write_scenario_csv(
        str(logs), cfg, "ts", os.path.join(str(tmp_path), "kv_inst.json"),
        cluster=None, mooncake_store_nodes=[], result_dir=str(tmp_path))
    w = next(r for r in out if r["role"] == "writer" and r["instance"] == "i0")
    assert w["total"] == 2 and w["avg_ms"] == 1.5 and w["max_ms"] == 2.0
    assert w["p99_ms"] == 2.0
    total = next(r for r in out if r["role"] == "writer"
                 and r["instance"] == "TOTAL")
    assert total["avg_ms"] == 1.5
    # TOTAL 分位 = 实例行 hist 合并插值（与报表同一 policy/桶梯）
    _h = {ls.bin_of(1.0, ls.BIN_EDGES_V2): 1,
          ls.bin_of(2.0, ls.BIN_EDGES_V2): 1}
    assert total["p50_ms"] == round(
        ls.hist_percentile(_h, 0.50, ls.BIN_EDGES_V2), 4)
    assert total["max_ms"] == 2.0


def test_report_pct_note_present():
    src = open(os.path.join(ROOT, "scripts", "report.py"),
               encoding="utf-8").read()
    assert 'id="pctNote"' in src
    assert "统计口径：count/avg/min/max 精确" in src
    assert "REQ sketch（k=" in src


@pytest.mark.skipif(not qt.available(), reason="datasketches not installed")
def test_real_library_roundtrip():
    sk = qt.make(24)
    for i in range(1000):
        qt.update(sk, float(i))
    b64 = qt.serialize(sk)
    assert b64
    q = qt.quantile_from([b64], 0.999)
    assert 985.0 <= q <= 1000.0
