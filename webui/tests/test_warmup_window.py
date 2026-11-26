#!/usr/bin/env python3
"""warmup 并入正式窗口：MP 侧数据面按 cutoff 剔除（图表仍绘制）。

单窗口 = warmup + duration；前 warmup 秒的行：
- CSV：不计入 total/success/fail/bytes/notify 与分位；实例 wall 扣掉 warmup
- Map：只进 seconds 时间桶（图表），不进 global/op/hist/top/errors
- Reduce：pooled lats（分位数）过滤、bucket（图表）保留；wall 用正式域
- Trace：tracestats --warmup-s 相对首行锚定，计入 phase=warmup 并剔除
"""
import csv
import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import benchmark as bm  # noqa: E402
import logstats as ls   # noqa: E402
import report as rp     # noqa: E402
import tracestats as ts  # noqa: E402

MB = 8 * 1024 * 1024


def _cfg(**over):
    cfg = {
        "scenario": "kvtest", "work_mode": "rank", "protocol": "ub",
        "data_size": ["8MB"], "duration": 30, "batch_size": 1,
        "notify_count": 1,
        "client": {"count_per_node": 1, "port_base": 9301},
        "writer": {"operation": "upsert_from", "num_threads": 1,
                   "notify_threads": 1},
        "reader": {"operation": "get_buffer", "num_threads": 1},
        "_kvtest_warmup_duration": 5.0,
    }
    cfg.update(over)
    return cfg


# ---------------------------------------------------------------- CSV 层

def _write_samples(logs, cfg_name, iid, port, rows):
    base = bm._kvtest_artifact_base(cfg_name, iid, port, "8MB")
    with open(os.path.join(logs, base + ".samples.csv"), "w",
              newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "elapsed_us", "instance_id", "operation", "request_index",
            "latency_ms", "status", "bytes"])
        w.writeheader()
        for i, (elapsed_s, op, lat, status) in enumerate(rows):
            w.writerow({"elapsed_us": int(elapsed_s * 1e6),
                        "instance_id": iid, "operation": op,
                        "request_index": i, "latency_ms": lat,
                        "status": status,
                        "bytes": MB if status == "OK" else 0})
    return base


def _write_summary(logs, base, iid, operations, elapsed=35.0):
    with open(os.path.join(logs, base + ".summary.json"), "w",
              encoding="utf-8") as f:
        json.dump({"instance_id": iid, "elapsed_seconds": elapsed,
                   "verify_failures": 2, "operations": operations}, f)


def _map_and_finalize(tmp_path, role, rows, ops, elapsed=35.0, verify=0):
    import importlib.util
    logs = tmp_path / ("logs_" + role)
    logs.mkdir(exist_ok=True)
    base = _write_samples(logs, "warmup_csv", 0, 9301, rows)
    _write_summary(logs, base, 0, ops, elapsed=elapsed)
    inst_dir = logs / ("inst_" + role)
    inst_dir.mkdir(exist_ok=True)
    with open(str(inst_dir / "keep.log"), "w") as f:
        f.write("x" * 64)
    stats, csvstats = ls.kvtest_map_instance(
        str(logs / (base + ".samples.csv")),
        str(logs / (base + ".summary.json")), role,
        node="10.0.0.1", pod="pod-a", size="8MB", start_epoch=1000.0,
        warmup_s=5.0, digest_k=24, iid=0, instance_dir=str(inst_dir),
        remote_path=str(logs / (base + ".stats.json.gz")))
    for doc, name in ((stats, ".stats.json"), (csvstats, ".csvstats.json")):
        with open(str(logs / (base + name)), "w", encoding="utf-8") as f:
            json.dump(doc, f)
    mod_path = os.path.join(ROOT, "scripts", "kvtest_partial_reduce.py")
    spec = importlib.util.spec_from_file_location(
        "kvtest_partial_reduce", mod_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    out = str(logs / "report" / "8MB" / "partial.json.gz")
    mod.run({"dir": str(logs), "out": out, "ds": "8MB",
             "meta": {"config": "cfg", "mode": "pipeline",
                      "engine": "kvtest"},
             "instances": [{"iid": 0, "node": "10.0.0.1", "pod": "pod-a",
                            "role": role, "size": "8MB",
                            "stats": base + ".stats.json",
                            "csvstats": base + ".csvstats.json",
                            "trace": None}]})
    return logs, out


def test_csv_excludes_warmup_rows_and_shrinks_instance_wall(tmp_path,
                                                            fake_sketch):
    writer_rows = [
        (1.0, "upsert_from", 40.0, "OK"),      # warmup
        (2.0, "upsert_from", 41.0, "OK"),      # warmup
        (1.0, "notify_http", 1.0, "OK"),       # warmup notify
        (10.0, "upsert_from", 1.0, "OK"),
        (20.0, "upsert_from", 2.0, "OK"),
        (12.0, "upsert_from", 3.0, "ERR"),
        (10.0, "notify_http", 1.0, "OK"),
        (20.0, "notify_http", 1.0, "OK"),
    ]
    _logs, partial = _map_and_finalize(tmp_path, "writer", writer_rows, {
        "upsert_from": {"total": 5, "success": 4, "failure": 1,
                        "count": 5, "bytes": 4 * MB,
                        "avg_ms": 99.0, "max_ms": 999.0},
        "notify_http": {"total": 3, "success": 3, "failure": 0,
                        "count": 3, "bytes": 0},
    })
    bm._kvtest_finalize_stage(str(tmp_path / "logs_writer"))
    cfg = _cfg()
    out = bm.write_scenario_csv(
        str(tmp_path / "logs_writer"), cfg, "ts",
        os.path.join(str(tmp_path), "warmup_csv.json"), cluster=None,
        mooncake_store_nodes=[], result_dir=str(tmp_path))
    w = next(r for r in out if r["role"] == "writer"
             and r["instance"] == "i0")
    assert w["total"] == 3 and w["success"] == 2 and w["fail"] == 1
    assert w["fail_rate"] == round(1 / 3 * 100.0, 2)
    assert w["total_bytes"] == 2 * MB          # warmup 的写不计入
    assert w["wall_s"] == 30.0                 # 35s elapsed - 5s warmup
    assert w["qps"] == round(2 / 30.0, 2)
    assert w["notify_submitted"] == 2          # warmup 的 notify 不计入
    assert w["notify_ok"] == 2


def test_csv_reader_verify_from_summary_without_formal_split(tmp_path,
                                                             fake_sketch):
    _logs, _partial = _map_and_finalize(
        tmp_path, "reader",
        [(10.0, "get_buffer", 0.5, "OK"),
         (21.0, "get_buffer", 0.6, "OK")],
        {"get_buffer": {"total": 2, "success": 2, "failure": 0,
                        "count": 2, "bytes": 2 * MB}},
        verify=2)
    bm._kvtest_finalize_stage(str(tmp_path / "logs_reader"))
    cfg = _cfg()
    out = bm.write_scenario_csv(
        str(tmp_path / "logs_reader"), cfg, "ts",
        os.path.join(str(tmp_path), "warmup_reader.json"), cluster=None,
        mooncake_store_nodes=[], result_dir=str(tmp_path))
    r = next(r for r in out if r["role"] == "reader"
             and r["instance"] == "i0")
    assert r["total"] == 2 and r["success"] == 2
    assert r["wall_s"] == 30.0
    assert r["verify_failures"] == 2           # C++ 标量：无法按时段切


# ---------------------------------------------------------------- Map 层

def _rows_factory(p, _ops=("upsert_from",)):
    rows = [
        # (elapsed, epoch, lat, status) —— warmup 两条、正式两条
        (1.0, 1001.0, 40.0, "OK"),
        (2.0, 1002.0, 41.0, "OK"),
        (10.0, 1010.0, 1.0, "OK"),
        (20.0, 1020.0, 2.0, "OK"),
    ]

    def _it(_path):
        for _e, ep, lat, st in rows:
            yield {"ts": "", "op": "upsert_from", "key": "", "lat": lat,
                   "status": st, "qw": None, "kc": 1, "lineno": 1,
                   "epoch": ep}
    return _it


def test_compute_stats_cutoff_excludes_globals_keeps_series(tmp_path):
    p = tmp_path / "a.log"
    p.write_text("x", encoding="utf-8")
    st = ls.compute_stats(
        str(p), rows_factory=_rows_factory(str(p)),
        extra_meta={"format": "kvtest_samples"}, size="8MB",
        warmup_cutoff=1005.0)
    g = st["global"]
    assert g["total"] == 2 and g["ok"] == 2      # warmup 两条被剔除
    assert g["avg_ms"] == round((1.0 + 2.0) / 2, 6)
    # 图表：warmup 行仍在秒桶里（4 个桶）
    assert len(st["seconds"]) == 4
    m = st["meta"]
    assert m["warmup_cutoff"] == 1005.0 and m["warmup_rows"] == 2
    # 图表域含 warmup，正式域从 cutoff 之后开始
    assert m["ts_from"] == 1001.0 and m["formal_ts_from"] == 1010.0
    assert m["formal_ts_to"] == 1020.0


def test_compute_stats_without_cutoff_unchanged(tmp_path):
    p = tmp_path / "b.log"
    p.write_text("x", encoding="utf-8")
    st = ls.compute_stats(str(p), rows_factory=_rows_factory(str(p)))
    assert st["global"]["total"] == 4
    assert st["meta"]["warmup_cutoff"] is None
    assert st["meta"]["warmup_rows"] == 0


# ---------------------------------------------------------------- Reduce 层

def _stats_product(tmp_path, role="writer", cutoff=1005.0,
                   formal_from=1010.0, formal_to=1020.0, wall=35.0):
    """一个 kvtest stats 产物 + sibling samples.csv（epoch 域）。"""
    samples = tmp_path / "kv.i0_p9301.kvtest_8MB.samples.csv"
    with open(samples, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "elapsed_us", "instance_id", "operation", "request_index",
            "latency_ms", "status", "bytes"])
        w.writeheader()
        for i, (ep, lat) in enumerate([(1001, 40.0), (1002, 41.0),
                                       (1010, 1.0), (1020, 2.0)]):
            w.writerow({"elapsed_us": int((ep - 1000) * 1e6),
                        "instance_id": 0, "operation": "upsert_from",
                        "request_index": i, "latency_ms": lat,
                        "status": "OK", "bytes": MB})
    # 产物名与 map 侧一致：<samples.csv 全路径>.<role>.stats.json
    prod = tmp_path / (
        "kv.i0_p9301.kvtest_8MB.samples.csv.{}.stats.json".format(role))
    prod.write_text(json.dumps({
        "meta": {"format": "kvtest_samples", "role": role,
                 "source": "kv.i0_p9301.kvtest_8MB",
                 "ts_from": 1000.0, "warmup_cutoff": cutoff,
                 "formal_ts_from": formal_from,
                 "formal_ts_to": formal_to},
        "global": {"total": 2, "ok": 2, "fail": 0, "avg_ms": 1.5,
                   "ok_rows": 2, "key_total": 2, "key_ok": 2,
                   "key_fail": 0},
        "ops": {"upsert_from": {}},
        "summaries": [("summary", {"wall_s": wall, "bytes": 2 * MB,
                                   "max_depth": 0})],
        "seconds": [], "histogram": {},
    }), encoding="utf-8")
    return str(prod)


def test_summary_of_uses_formal_wall_not_cpp_elapsed(tmp_path):
    st = json.load(open(_stats_product(tmp_path), encoding="utf-8"))
    out = rp._summary_of([st])
    # summary payload 的 wall_s=35（含 warmup）被正式域 10s 覆盖
    assert out["wall_s"] == 10.0
    assert out["qps"] == round(2 / 10.0, 2)


def test_reduce_size_carries_warmup_cutoff(tmp_path):
    st = json.load(open(_stats_product(tmp_path), encoding="utf-8"))
    blk = rp.reduce_size([st], {"config": "cfg", "scenario": "kvtest"})
    assert blk["warmup_cutoff"] == 1005.0


# ---------------------------------------------------------------- Trace 层

def _trace_line(sec, key):
    return ("I20261126 10:23:{:02d}.000000 45 real_client.cpp:222] "
            "trace_id[{}] breakdown[upsert] key[{}] status[ok] size[8192] "
            "replicas[1] start_time[2026-11-26 10:23:{:02d}.000000] "
            "prep_total_us[10] checksum_us[1] prepare_us[2] query_us[3] "
            "put_start_us[0] memwrite_total_us[4] put_end_us[0] "
            "finalize_us[5] cache_invalidate_us[0] revoke_us[0] "
            "total_us[30]\n").format(sec, sec, key, sec)


def test_tracestats_warmup_s_excludes_first_rows(tmp_path):
    p = tmp_path / "w.INFO.1"
    p.write_text(_trace_line(0, "k0") + _trace_line(2, "k1")
                 + _trace_line(10, "k2") + _trace_line(20, "k3"),
                 encoding="utf-8")
    st = ts.compute_trace_stats(str(p), warmup_s=5.0)
    op = st["groups"]["write"]["ops"]["upsert"]
    assert op["count"] == 2                      # 前 5s 的两条被剔除
    assert st["meta"]["phase_counts"]["warmup"] == 2
    assert st["meta"]["warmup_cutoff"] is not None
    assert st["meta"]["warmup_s"] == 5.0


def test_tracestats_without_warmup_keeps_everything(tmp_path):
    p = tmp_path / "w2.INFO.1"
    p.write_text(_trace_line(0, "k0") + _trace_line(10, "k1"),
                 encoding="utf-8")
    st = ts.compute_trace_stats(str(p))
    assert st["groups"]["write"]["ops"]["upsert"]["count"] == 2
    assert st["meta"]["warmup_cutoff"] is None


# ---------------------------------------------------------------- 报表/持久化

def test_report_charts_carry_warmup_boundary_line():
    src = open(os.path.join(ROOT, "scripts", "report.py"),
               encoding="utf-8").read()
    assert "z.warmup_cutoff" in src           # 主图竖线
    assert "_tm.warmup_cutoff" in src         # trace 阶段图竖线
    assert "'warmup 结束'" in src


def test_meta_base_carries_warmup_s():
    cfg = {"_kvtest_warmup_duration": 5.0}
    meta = bm._kvtest_report_meta_base(
        cfg, __import__("types").SimpleNamespace(
            cluster="c", global_cfg="g", profile="p"), "cfg", "8MB")
    assert meta["warmup_s"] == 5.0
