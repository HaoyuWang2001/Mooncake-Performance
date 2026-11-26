"""kvtest HTML report: inject the map-reduce chain into the C++ scenario.

The orchestrator pulls each kvtest instance's request-level samples.csv +
summary.json; the map stage (scripts/logstats.py --kvtest-samples) turns them
into the same per-client stats.json the Python client produces, and the
unchanged reduce stage (scripts/report.py) renders one HTML report per
data_size with writer/reader roles, per-op tables, slowest list and the run
overview.
"""
import csv
import json
import os
import re
import subprocess
import sys
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import benchmark as bm  # noqa: E402
import logstats as ls  # noqa: E402

START = 1763596800.0  # fixed epoch so bucket/epoch assertions are exact


def _cfg(**over):
    cfg = {
        "scenario": "kvtest", "work_mode": "rank", "protocol": "ub",
        "data_size": ["8MB"], "duration": 10, "base_qps": 105,
        "qps_multiple": 1, "batch_size": 1, "notify_count": 10,
        "client": {"count_per_node": 2, "port_base": 9301},
        "writer": {"operation": "upsert_from", "num_threads": 1,
                   "notify_threads": 10},
        "reader": {"operation": "get_buffer", "num_threads": 4},
        "_kvtest_start_epochs": {"8MB": START},
        "_kvtest_nodes": 2,
        "_kvtest_iid_nodes": {"0": "nodeA", "1": "nodeB"},
        "_kvtest_key_pool": {"8MB": 256},
        "_writer_count": 2, "_reader_count": 2,
    }
    cfg.update(over)
    return cfg


def _write_instance(logs, iid, port, ds, writer_lats, reader_lats,
                    bad=(), cfg_name="kvtest_demo", verify=0, qw=None):
    """One kvtest instance: samples.csv + summary.json (writer+reader rows)."""
    base = bm._kvtest_artifact_base(cfg_name, iid, port, ds)
    rows = []
    seq = 0
    for latency in writer_lats:
        rows.append({"elapsed_us": int(seq * 200000), "instance_id": iid,
                     "operation": "upsert_from", "request_index": seq,
                     "latency_ms": latency, "status": "OK",
                     "bytes": 8388608})
        seq += 1
    for latency in reader_lats:
        rows.append({"elapsed_us": int(seq * 200000), "instance_id": iid,
                     "operation": "get_buffer", "request_index": seq,
                     "latency_ms": latency, "status": "OK",
                     "bytes": 8388608})
        seq += 1
    for latency in bad:
        rows.append({"elapsed_us": int(seq * 200000), "instance_id": iid,
                     "operation": "upsert_from", "request_index": seq,
                     "latency_ms": latency, "status": "ERR", "bytes": 0})
    with open(os.path.join(logs, base + ".samples.csv"), "w",
              newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "elapsed_us", "instance_id", "operation", "request_index",
            "latency_ms", "status", "bytes"])
        w.writeheader()
        for row in rows:
            w.writerow(row)
    operations = {
            "upsert_from": {"count": len(writer_lats), "total": len(writer_lats),
                            "success": len(writer_lats), "failure": 0,
                            "bytes": len(writer_lats) * 8388608},
            "get_buffer": {"count": len(reader_lats), "total": len(reader_lats),
                           "success": len(reader_lats), "failure": 0,
                           "bytes": len(reader_lats) * 8388608},
    }
    if qw:
        operations["notify_queue_wait"] = {
            "count": int(qw.get("count") or 0),
            "total": int(qw.get("count") or 0),
            "success": int(qw.get("count") or 0), "failure": 0,
            "avg_ms": float(qw.get("avg_ms") or 0.0),
            "max_ms": float(qw.get("max_ms") or 0.0), "bytes": 0}
    summary = {
        "instance_id": iid, "elapsed_seconds": 10.0,
        "verify_failures": verify,
        "operations": operations,
    }
    with open(os.path.join(logs, base + ".summary.json"), "w",
              encoding="utf-8") as f:
        json.dump(summary, f)
    return base


@pytest.fixture(autouse=True)
def _clean_report_acc():
    saved = list(bm._REPORT_ACC)
    saved_ctx = dict(bm._STOP_CTX)
    bm._REPORT_ACC.clear()
    yield
    bm._REPORT_ACC.clear()
    bm._REPORT_ACC.extend(saved)
    bm._STOP_CTX.clear()
    bm._STOP_CTX.update(saved_ctx)


def _args():
    return SimpleNamespace(dry_run=False, png=False, pull_raw_logs=False,
                           keep_intermediates=False,
                           cluster="mock/cluster.json",
                           global_cfg="configs/_ssh/global.json",
                           profile="configs/_ssh/profile.json", mode="ssh")


# ---------------------------------------------------------------- map stage

def test_iter_kvtest_rows_maps_latency_status_and_epoch(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    _write_instance(str(logs), 0, 9301, "8MB", [1.0, 2.0], [3.0])
    path = str(next(logs.glob("*.samples.csv")))
    rows = list(ls.iter_kvtest_rows(path, START, ls.KVTEST_WRITE_OPS))
    assert [r["lat"] for r in rows] == [1.0, 2.0]
    assert all(r["op"] == "upsert_from" for r in rows)
    assert rows[0]["epoch"] == START
    assert rows[1]["epoch"] == pytest.approx(START + 0.2)
    assert rows[1]["ts"]        # display timestamp is synthesized too
    # role filter is exclusive: reader ops never leak into the writer product
    assert not any(r["op"] == "get_buffer" for r in rows)


def test_kvtest_map_writes_per_role_products(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    _write_instance(str(logs), 0, 9301, "8MB", [1.0, 2.0], [3.0])
    _write_instance(str(logs), 1, 9302, "8MB", [4.0], [5.0, 6.0])
    cfg = _cfg()
    mapped = bm._kvtest_map_stats(str(logs), cfg)
    assert len(mapped) == 4                     # 2 roles x 2 instances
    writer = json.load(open(
        [p for p in mapped if ".writer.stats.json" in p][0], encoding="utf-8"))
    reader = json.load(open(
        [p for p in mapped if ".reader.stats.json" in p][0], encoding="utf-8"))
    assert writer["meta"]["role"] == "writer"
    assert writer["meta"]["format"] == "kvtest_samples"
    assert writer["meta"]["scenario"] == "kvtest"
    assert writer["meta"]["node"] == "nodeA"
    assert writer["meta"]["size"] == "8MB"
    assert writer["meta"]["source"].endswith(".writer.samples.csv")
    assert writer["global"]["ok"] == 2 and writer["global"]["fail"] == 0
    assert writer["global"]["p99_ms"] == 2.0
    assert set(writer["ops"]) == {"upsert_from"}
    assert reader["global"]["ok"] == 1
    assert set(reader["ops"]) == {"get_buffer"}
    # summary-derived wall/bytes reach the reduce stage
    payload = writer["summaries"][0][1]
    assert payload["wall_s"] == 10.0
    assert payload["bytes"] == 2 * 8388608


def test_kvtest_map_skips_role_without_samples(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    _write_instance(str(logs), 0, 9301, "8MB", [1.0], [])
    mapped = bm._kvtest_map_stats(str(logs), _cfg())
    assert len(mapped) == 1
    assert mapped[0].endswith(".writer.stats.json")


def test_kvtest_map_records_failures_and_epochs(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    _write_instance(str(logs), 0, 9301, "8MB", [1.0, 2.0], [1.5],
                    bad=(900.0,))
    mapped = bm._kvtest_map_stats(str(logs), _cfg())
    writer = json.load(open(
        [p for p in mapped if ".writer.stats.json" in p][0], encoding="utf-8"))
    assert writer["global"]["fail"] == 1
    assert writer["errors"] == {"ERR": 1}
    # the failed request is the slowest row and keeps its absolute epoch
    slow = writer["slowest"][0]
    assert slow["status"] == "ERR" and slow["lat_ms"] == 900.0
    assert slow["ts_epoch"] is not None
    # time buckets share the /start origin (no host-timezone shift)
    assert writer["meta"]["ts_from"] == START


def test_kvtest_map_surfaces_summary_only_metrics(tmp_path):
    """summary.json-only signals must reach the stats product.

    Reader value-verification failures (correctness) and the notify queue
    wait (backlog) have no per-request sample column; the map lifts them from
    the C++ summary so the HTML report can show them."""
    logs = tmp_path / "logs"
    logs.mkdir()
    _write_instance(str(logs), 0, 9301, "8MB", [1.0], [2.0],
                    verify=3, qw={"count": 10, "avg_ms": 0.2, "max_ms": 1.1})
    mapped = bm._kvtest_map_stats(str(logs), _cfg())
    writer = json.load(open(
        [p for p in mapped if ".writer.stats.json" in p][0], encoding="utf-8"))
    reader = json.load(open(
        [p for p in mapped if ".reader.stats.json" in p][0], encoding="utf-8"))
    assert reader["global"]["verify_failures"] == 3
    assert reader["global"]["qw_avg_ms"] == 0.2
    assert reader["global"]["qw_max_ms"] == 1.1
    assert reader["global"]["qw_n"] == 10
    # writer side carries neither signal (they belong to the read path)
    assert "verify_failures" not in writer["global"]
    assert writer["global"].get("qw_avg_ms") is None


def test_kvtest_map_prefers_raw_log_bytes(tmp_path):
    """The report's node-log volume must be the pulled .log/.glog size, not
    the (much smaller) map input samples.csv."""
    logs = tmp_path / "logs"
    logs.mkdir()
    base = _write_instance(str(logs), 0, 9301, "8MB", [1.0], [2.0])
    (logs / (base + ".log")).write_bytes(b"x" * 300)
    # glog 已移居 MC_LOG_DIR（不再有 .log.glog）；.ylt.log 仍是现场文件
    (logs / (base + ".log.ylt.log")).write_bytes(b"y" * 120)
    mapped = bm._kvtest_map_stats(str(logs), _cfg())
    reader = json.load(open(
        [p for p in mapped if ".reader.stats.json" in p][0], encoding="utf-8"))
    samples_bytes = (logs / (base + ".samples.csv")).stat().st_size
    assert reader["meta"]["bytes"] == 420
    assert reader["meta"]["samples_bytes"] == samples_bytes
    # no raw logs pulled (older binary / stats-only): fall back to样本大小
    (logs / (base + ".log")).unlink()
    (logs / (base + ".log.ylt.log")).unlink()
    mapped = bm._kvtest_map_stats(str(logs), _cfg())
    reader = json.load(open(
        [p for p in mapped if ".reader.stats.json" in p][0], encoding="utf-8"))
    assert reader["meta"]["bytes"] == samples_bytes


def test_kvtest_start_epoch_falls_back_to_summary_mtime(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    base = _write_instance(str(logs), 0, 9301, "8MB", [1.0], [])
    samples = str(logs / (base + ".samples.csv"))
    summary = str(logs / (base + ".summary.json"))
    os.utime(samples, (START + 100, START + 100))
    assert bm._kvtest_start_epoch({}, "8MB", samples, summary) == \
        pytest.approx(START + 90, abs=0.5)   # mtime - elapsed_seconds


# ------------------------------------------------------------- reduce stage

def test_kvtest_report_end_to_end(tmp_path):
    root = tmp_path / "run"
    logs = root / "logs" / "kvtest_demo"
    logs.mkdir(parents=True)
    _write_instance(str(logs), 0, 9301, "8MB", [1.0, 2.0], [3.0],
                    bad=(800.0,))
    _write_instance(str(logs), 1, 9302, "8MB", [4.0], [5.0, 6.0])
    cfg = _cfg()
    cfg["warmup"] = {"duration": 5}       # YLT: warmup now applies to kvtest
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_demo.json")
    bm._kvtest_generate_reports(cfg, str(logs), _args())
    out = logs / "report" / "8MB"
    assert (out / "report.html").is_file()
    assert (out / "data.js").is_file()
    assert (out / "meta.json").is_file()
    meta = json.load(open(out / "meta.json", encoding="utf-8"))
    assert meta["mode"] == "pipeline"
    assert meta["scenario"] == "pipeline"
    assert meta["engine"] == "kvtest"
    assert meta["unit_size"] == "8MB"
    assert any(p[0] == "场景" and p[1] == "kvtest" for p in meta["params"])
    # kvtest warmup now reaches the params card ("预热时长")
    assert ["预热时长", "5 s"] in meta["params"]

    raw = (out / "data.js").read_text(encoding="utf-8")
    data = json.loads(re.search(r"=\s*(\{.*\})\s*;?\s*$", raw,
                                re.S).group(1))
    assert set(data["sizes"]) == {"8MB"}
    blk = data["sizes"]["8MB"]
    assert set(blk["role_summaries"]) == {"writer", "reader"}
    assert blk["summary"]["total"] == 7          # 4 writer + 3 reader rows
    assert blk["summary"]["ok"] == 6
    assert blk["summary"]["fail"] == 1
    assert blk["summary"]["bytes"] == 6 * 8388608
    assert blk["errors"] == {"ERR": 1}
    # writer/reader roles stay separate in the object tree
    assert {"all", "writer", "reader"} <= set(blk["objects"])
    assert any(k.startswith("node:nodeA:writer") for k in blk["objects"])
    # slowest request keeps the absolute epoch computed at map time
    assert blk["slowest"][0]["ts_epoch"] == pytest.approx(START + 0.6)
    assert blk["slowest"][0]["role"] == "writer"

    html = (out / "report.html").read_text(encoding="utf-8")
    for needle in ("kvtest", "writer", "reader", "upsert_from",
                   "get_buffer"):
        assert needle in html

    overview = bm._render_run_overview(str(root), "ssh")
    assert overview and os.path.isfile(overview)
    assert "kvtest_demo" in open(overview, encoding="utf-8").read()


def test_kvtest_report_generates_one_unit_per_size(tmp_path):
    logs = tmp_path / "logs" / "kvtest_multi"
    logs.mkdir(parents=True)
    _write_instance(str(logs), 0, 9301, "8MB", [1.0], [2.0],
                    cfg_name="kvtest_multi")
    _write_instance(str(logs), 0, 9301, "3.5MB", [3.0], [4.0],
                    cfg_name="kvtest_multi")
    cfg = _cfg(data_size=["8MB", "3.5MB"],
               _kvtest_start_epochs={"8MB": START, "3.5MB": START},
               _kvtest_iid_nodes={"0": "nodeA"})
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_multi.json")
    written = bm._kvtest_generate_reports(cfg, str(logs), _args())
    assert written and len(written) == 2
    assert {os.path.basename(os.path.dirname(p)) for p in written} == \
        {"8MB", "3.5MB"}


def test_kvtest_rebuild_from_samples_only(tmp_path, monkeypatch):
    """--rebuild-report: a failed kvtest run keeps samples/summary; the map
    stage can regenerate stats and the HTML report without the run config."""
    monkeypatch.setattr(bm, "RESULTS_DIR", str(tmp_path))
    logs = tmp_path / "run1" / "logs" / "kvtest_rebuild"
    logs.mkdir(parents=True)
    base = _write_instance(str(logs), 0, 9301, "8MB", [1.0, 2.0], [3.0],
                           cfg_name="kvtest_rebuild")
    os.utime(str(logs / (base + ".samples.csv")), (START + 10, START + 10))
    # a real pull also brings the wrapper .log + SDK glog; logstats must not
    # mistake them for Python client logs (that used to suppress the kvtest map)
    (logs / (base + ".log")).write_text("no RESULT rows\n", encoding="utf-8")
    (logs / (base + ".log.glog")).write_text("I2026 log\n", encoding="utf-8")
    # the run sidecar survives a failure: node mapping + /start epoch + nodes
    bm._kvtest_write_report_meta(str(logs), {
        "_kvtest_nodes": 2, "_kvtest_iid_nodes": {"0": "nodeA"},
        "_kvtest_start_epochs": {"8MB": START}})
    rc = bm.rebuild_reports("run1")
    assert rc == 0
    out = logs / "report" / "8MB"
    assert (out / "report.html").is_file()
    stats_files = list(logs.glob("*.stats.json"))
    assert len(stats_files) == 2
    assert not (logs / (base + ".log.stats.json")).exists()
    writer = json.load(open(
        [p for p in stats_files if ".writer.stats.json" in str(p)][0],
        encoding="utf-8"))
    assert writer["meta"]["format"] == "kvtest_samples"
    assert writer["meta"]["node"] == "nodeA"          # from the sidecar
    assert writer["meta"]["ts_from"] == START         # exact /start epoch
    raw = (out / "data.js").read_text(encoding="utf-8")
    data = json.loads(re.search(r"=\s*(\{.*\})\s*;?\s*$", raw,
                                re.S).group(1))
    blk = data["sizes"]["8MB"]
    assert data["meta"]["mode"] == "pipeline"
    assert data["meta"]["scenario"] == "pipeline"
    assert data["meta"]["engine"] == "kvtest"
    assert any(k.startswith("node:nodeA:") for k in blk["objects"])
    params = data["meta"]["params"]
    assert ["场景", "kvtest"] in params
    assert any(p[0] == "客户端总数" for p in params)


def test_verify_reports_requires_data_js_and_complete_html(tmp_path):
    logs = tmp_path / "logs" / "kvtest_verify"
    logs.mkdir(parents=True)
    _write_instance(str(logs), 0, 9301, "8MB", [1.0], [2.0],
                    cfg_name="kvtest_verify")
    cfg = _cfg()
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_verify.json")
    bm._kvtest_generate_reports(cfg, str(logs), _args())
    ok, _ = bm._verify_config_reports(str(logs))
    assert ok
    unit = logs / "report" / "8MB"
    (unit / "data.js").unlink()
    ok, detail = bm._verify_config_reports(str(logs))
    assert not ok and "data.js" in detail
    (unit / "data.js").write_text("x", encoding="utf-8")
    html = unit / "report.html"
    text = html.read_text(encoding="utf-8")
    html.write_text(text.rsplit("</html>", 1)[0], encoding="utf-8")
    ok, detail = bm._verify_config_reports(str(logs))
    assert not ok and "truncated" in detail
    html.write_text(text, encoding="utf-8")
    extra = logs / "report" / "16MB"
    extra.mkdir()
    (extra / "report.html").write_text("<html></html>", encoding="utf-8")
    (extra / "data.js").write_text("x", encoding="utf-8")
    ok, detail = bm._verify_config_reports(str(logs))
    assert not ok and "mismatch" in detail


def test_rebuild_reports_explains_cleaned_intermediates(tmp_path, monkeypatch):
    logs = tmp_path / "run1" / "logs" / "kvtest_cleaned"
    unit = logs / "report" / "8MB"
    unit.mkdir(parents=True)
    (unit / "report.html").write_text("<html></html>", encoding="utf-8")
    (unit / "data.js").write_text("x", encoding="utf-8")
    monkeypatch.setattr(bm, "RESULTS_DIR", str(tmp_path))
    msgs = []
    monkeypatch.setattr(bm, "log", lambda m: msgs.append(str(m)))
    assert bm.rebuild_reports("run1") == 1
    assert any("intermediates were cleaned after success" in m
               for m in msgs), msgs


def test_kvtest_report_meta_roundtrip(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    assert bm._kvtest_read_report_meta(str(logs)) == {}
    bm._kvtest_write_report_meta(str(logs), {
        "_kvtest_nodes": 3, "_kvtest_iid_nodes": {"1": "nodeB"},
        "_kvtest_iid_pods": {"1": "pod-b"},
        "_kvtest_start_epochs": {"8MB": START},
        "writer": {"operation": "upsert"}},
        {"run_id": "20261121-1244", "remote_log_dir": "/logs/ts",
         "host_log_root": "/var/lib/mooncake-bench",
         "k8s_namespace": "mc-ns"})
    meta = bm._kvtest_read_report_meta(str(logs))
    assert meta["nodes"] == 3
    assert meta["iid_nodes"] == {"1": "nodeB"}
    assert meta["iid_pods"] == {"1": "pod-b"}
    assert meta["start_epochs"] == {"8MB": START}
    assert meta["writer_operation"] == "upsert"
    # 现场索引上下文（host 日志根目录 / run id / namespace）
    assert meta["run_id"] == "20261121-1244"
    assert meta["remote_log_dir"] == "/logs/ts"
    assert meta["host_log_root"] == "/var/lib/mooncake-bench"
    assert meta["k8s_namespace"] == "mc-ns"


def test_kvtest_report_field_index_uses_real_host_dir(tmp_path):
    """现场索引：报告给出逐 pod 的真实现场目录（ssh IP + host 路径），
    不再输出 <host_log_root>/<run_id> 占位符。"""
    root = tmp_path / "run"
    logs = root / "logs" / "kvtest_demo"
    logs.mkdir(parents=True)
    _write_instance(str(logs), 0, 9301, "8MB", [1.0], [2.0])
    cfg = _cfg(_kvtest_iid_pods={"0": "pod-x"})
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_demo.json")
    bm._kvtest_write_report_meta(str(logs), cfg, {
        "run_id": "20261121-1244",
        "remote_log_dir": "/workspace/logs/20261121-1244",
        "host_log_root": "/var/lib/mooncake-bench",
        "k8s_namespace": "mc-ns"})
    bm._kvtest_generate_reports(cfg, str(logs), _args())
    out = logs / "report" / "8MB"
    meta = json.load(open(out / "meta.json", encoding="utf-8"))
    assert meta["host_log_root"] == "/var/lib/mooncake-bench"
    assert meta["run_id"] == "20261121-1244"
    # 现场目录由报表 JS 依据 D.meta + D.sources 在浏览器端拼接：
    # data.js 必须带真值（host_log_root / run_id），JS 必须带拼接与登录提示
    datajs = (out / "data.js").read_text(encoding="utf-8")
    assert "host_log_root" in datajs and "/var/lib/mooncake-bench" in datajs
    assert "20261121-1244" in datajs
    # 现场日志表的行在 reduce 侧生成：pod 目录必须是节点侧真实路径
    # （旧 sidecar 的 pod 内路径经挂载逆变换成 <host_log_root>/<run_id>/<pod>）
    assert "/var/lib/mooncake-bench/20261121-1244/pod-x" in datajs, datajs[-2000:]
    html = (out / "report.html").read_text(encoding="utf-8")
    assert "logsite-table" in html and "日志路径" in html
    assert "节点与登录现场" not in html and "其它产物索引" not in html
    # overview 的节点日志量说明由 Python 渲染真值
    overview = bm._render_run_overview(str(root), "kvtest")
    ov = open(overview, encoding="utf-8").read()
    assert "/var/lib/mooncake-bench/20261121-1244/" in ov
    assert "每 pod 一目录" in ov


def test_kvtest_map_uses_sidecar_when_cfg_is_gone(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    _write_instance(str(logs), 0, 9301, "8MB", [1.0], [2.0])
    bm._kvtest_write_report_meta(str(logs), {
        "_kvtest_iid_nodes": {"0": "nodeZ"},
        "_kvtest_start_epochs": {"8MB": START}})
    mapped = bm._kvtest_map_stats(str(logs), {})
    assert mapped
    writer = json.load(open(
        [p for p in mapped if ".writer.stats.json" in p][0], encoding="utf-8"))
    assert writer["meta"]["node"] == "nodeZ"
    assert writer["meta"]["ts_from"] == START


def test_kvtest_report_js_does_not_crash(tmp_path):
    """The report app JS must render kvtest data (roles, no qw/depth) too."""
    import shutil
    import subprocess as sp
    node = shutil.which("node")
    if not node:
        pytest.skip("node not found")
    logs = tmp_path / "logs" / "kvtest_js"
    logs.mkdir(parents=True)
    _write_instance(str(logs), 0, 9301, "8MB", [1.0, 2.0], [3.0],
                    bad=(800.0,), cfg_name="kvtest_js", verify=2,
                    qw={"count": 5, "avg_ms": 0.4, "max_ms": 2.5})
    _write_instance(str(logs), 1, 9302, "8MB", [4.0], [5.0, 6.0],
                    cfg_name="kvtest_js")
    cfg = _cfg()
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_js.json")
    bm._kvtest_generate_reports(cfg, str(logs), _args())
    raw = (logs / "report" / "8MB" / "data.js").read_text(encoding="utf-8")
    data = re.search(r"=\s*(\{.*\})\s*;?\s*$", raw, re.S).group(1)
    rp = bm._report_module()
    app = tmp_path / "app.js"
    app.write_text(rp._APP_JS, encoding="utf-8")
    dj = tmp_path / "data.json"
    dj.write_text(data, encoding="utf-8")
    proc = sp.run([node, os.path.join(ROOT, "webui", "tests",
                                      "report_js_check.js"),
                   str(app), str(dj), "--expect-trace", "0"],
                  capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stdout + proc.stderr
    out = json.loads(proc.stdout)
    assert out["init_len"] > 0
    # summary-only kvtest signals must be rendered (not just in data.js);
    # kvtest-pipeline renders per-role blocks (rolecards), not a merged card
    cards = out["cards_text"] + " " + (out.get("rolecards_text") or "")
    assert "值校验失败" in cards
    assert "队列等待均" in cards


def test_kvtest_cleanup_removes_all_intermediates(tmp_path):
    """Verified report -> every intermediate (samples/summary/stats) goes;
    report/ and profiling/ stay."""
    logs = tmp_path / "logs" / "kvtest_clean"
    logs.mkdir(parents=True)
    base = _write_instance(str(logs), 0, 9301, "8MB", [1.0], [2.0],
                           cfg_name="kvtest_clean")
    cfg = _cfg()
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_clean.json")
    bm._kvtest_generate_reports(cfg, str(logs), _args())
    (logs / "profiling").mkdir()
    (logs / "profiling" / "mem.host.csv").write_text("x\n")
    ok, detail = bm._verify_config_reports(str(logs))
    assert ok, detail
    removed = bm._cleanup_config_intermediates(str(logs))
    assert removed >= 3                       # samples + summary + stats
    assert not (logs / (base + ".samples.csv")).exists()
    assert not (logs / (base + ".summary.json")).exists()
    assert not list(logs.glob("*.stats.json"))
    assert (logs / "report" / "8MB" / "report.html").is_file()
    assert (logs / "report" / "8MB" / "data.js").is_file()
    assert (logs / "profiling" / "mem.host.csv").is_file()


def test_kvtest_report_disabled_with_no_report(tmp_path):
    logs = tmp_path / "logs" / "kvtest_noreport"
    logs.mkdir(parents=True)
    _write_instance(str(logs), 0, 9301, "8MB", [1.0], [2.0],
                    cfg_name="kvtest_noreport")
    cfg = _cfg()
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_noreport.json")
    args = _args()
    args.no_report = True
    assert bm._kvtest_generate_reports(cfg, str(logs), args) is None
    assert not (logs / "report").exists()
    assert not list(logs.glob("*.stats.json"))


def test_kvtest_report_independent_of_plot_switch(tmp_path):
    """The plot switch (--png) never gates the HTML report."""
    logs = tmp_path / "logs" / "kvtest_noplot"
    logs.mkdir(parents=True)
    _write_instance(str(logs), 0, 9301, "8MB", [1.0], [2.0],
                    cfg_name="kvtest_noplot")
    cfg = _cfg()
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_noplot.json")
    args = _args()
    args.png = True
    args.pull_raw_logs = True
    written = bm._kvtest_generate_reports(cfg, str(logs), args)
    assert written and (logs / "report" / "8MB" / "report.html").is_file()
    assert list(logs.glob("*.stats.json"))


def test_kvtest_report_is_warn_only_on_bad_samples(tmp_path):
    """A corrupt/unreadable sample set must never fail the benchmark path."""
    logs = tmp_path / "logs" / "kvtest_bad"
    logs.mkdir(parents=True)
    (logs / "kv.i0_p9301.kvtest_8MB.samples.csv").write_text(
        "not,the,expected\n1,2,3\n", encoding="utf-8")
    cfg = _cfg()
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_bad.json")
    assert bm._kvtest_generate_reports(cfg, str(logs), _args()) is None


def test_kvtest_full_lifecycle_matches_documented_artifacts(tmp_path):
    """CSV -> map -> reduce -> overview -> success cleanup, in run order.

    Final contract for a successful kvtest run: <cfg>.csv + per-instance
    report/<size>/report.html + report/overview.html + stats.json kept,
    raw summary/samples removed."""
    root = tmp_path / "run"
    logs = root / "logs" / "kvtest_life"
    logs.mkdir(parents=True)
    for iid, port in ((0, 9301), (1, 9302)):
        _write_instance(str(logs), iid, port, "8MB", [1.0, 2.0], [3.0],
                        cfg_name="kvtest_life")
    cfg = _cfg()
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_life.json")
    # 1) CSV aggregation (main() does this before the report)
    rows = bm.write_scenario_csv(str(logs), cfg, "ts", cfg["__cfg_path__"],
                                 cluster=None, mooncake_store_nodes=[],
                                 result_dir=str(root))
    assert rows
    assert (root / "kvtest_life.csv").is_file()
    # 2) the runner writes the report sidecar while pulling artifacts
    bm._kvtest_write_report_meta(str(logs), cfg)
    # 3) map + reduce
    written = bm._kvtest_generate_reports(cfg, str(logs), _args())
    assert written and len(written) == 1
    # 4) run overview
    assert bm._render_run_overview(str(root), "ssh")
    # 5) success cleanup: verified reports -> all intermediates removed
    ok, detail = bm._verify_config_reports(str(logs))
    assert ok, detail
    bm._cleanup_config_intermediates(str(logs))
    assert not list(logs.glob("*.samples.csv"))
    assert not list(logs.glob("*.summary.json"))
    assert not list(logs.glob("*.stats.json"))
    assert (logs / "report" / "8MB" / "report.html").is_file()
    assert (logs / "report" / "8MB" / "data.js").is_file()
    assert (root / "report" / "overview.html").is_file()
    assert not (logs / bm.KVTEST_REPORT_META).exists()


def test_kvtest_partial_report_on_stop(tmp_path, monkeypatch):
    """STOP path: the scenario handler already pulled samples, _stop_reports
    must map+reduce them into an early_stop-marked report and the overview."""
    root = tmp_path / "run"
    logs = root / "logs" / "kvtest_stop"
    logs.mkdir(parents=True)
    _write_instance(str(logs), 0, 9301, "8MB", [1.0, 2.0], [3.0],
                    cfg_name="kvtest_stop", verify=1,
                    qw={"count": 3, "avg_ms": 0.3, "max_ms": 2.0})
    bm._kvtest_write_report_meta(str(logs), {
        "_kvtest_nodes": 1, "_kvtest_iid_nodes": {"0": "nodeA"},
        "_kvtest_start_epochs": {"8MB": START}})
    cfg = _cfg()
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_stop.json")
    pulled = []
    monkeypatch.setattr(bm, "pull_logs",
                        lambda *a, **k: pulled.append(True))
    monkeypatch.setattr(bm, "_STOP_CTX", {
        "cluster": {"nodes": {}}, "args": _args(),
        "report": {"cfg": cfg, "cfg_name": "kvtest_stop",
                   "result_dir": str(root), "local_log_dir": str(logs),
                   "remote_log_dir": "/remote/logs", "involved_nodes": [],
                   "mode": "kvtest", "skip_pull": True}})
    bm._stop_reports()
    assert pulled == []                       # skip_pull honoured
    out = logs / "report" / "8MB"
    assert (out / "report.html").is_file()
    meta = json.load(open(out / "meta.json", encoding="utf-8"))
    assert meta["early_stop"] is True
    assert (root / "report" / "overview.html").is_file()
    raw = (out / "data.js").read_text(encoding="utf-8")
    data = json.loads(re.search(r"=\s*(\{.*\})\s*;?\s*$", raw,
                                re.S).group(1))
    blk = data["sizes"]["8MB"]
    assert blk["summary"]["verify_failures"] == 1
    assert blk["role_summaries"]["reader"]["verify_failures"] == 1
    assert blk["role_summaries"]["reader"]["qw_avg_ms"] == 0.3


def test_kvtest_stop_collect_stops_flushes_and_pulls(tmp_path, monkeypatch):
    """The STOP collector must /stop (samples flush there), wait, flatten,
    pull and hand the context over to _stop_reports."""
    logs = tmp_path / "logs"
    logs.mkdir()
    cfg = _cfg()
    entry = {"iid": 0, "node": "nodeA", "port": 9301,
             "_base": "kvx.i0_p9301.kvtest_8MB"}
    items = [({"ip": "1.2.3.4", "node": "nodeA"}, 9301, "", "kvtest i0")]
    calls = {"http": [], "wait": False, "pull": []}
    monkeypatch.setattr(bm, "get_node",
                        lambda cluster, name: {"ip": "1.2.3.4", "node": name})
    monkeypatch.setattr(
        bm, "_pipeline_http_parallel",
        lambda cluster, items, path, timeout=5, dry=False:
        calls["http"].append(path) or [(it[3], True, "") for it in items])
    monkeypatch.setattr(
        bm, "_wait_kvtest_processes",
        lambda *a, **k: calls.__setitem__("wait", True) or [True])
    monkeypatch.setattr(bm, "_kvtest_flatten_artifacts",
                        lambda *a, **k: (True, "summary=OK samples=OK"))
    monkeypatch.setattr(
        bm, "_pipeline_pull_parallel",
        lambda cluster, groups, local_log_dir, dry: calls["pull"].append(groups))
    bm._kvtest_stop_collect({"nodes": {}}, cfg, "kvtest_stop", "/remote/logs",
                            str(logs), str(tmp_path), [(entry, "pid")], items)
    assert calls["http"] == ["/stop"]
    assert calls["wait"] is True
    files = calls["pull"][0][0][1]
    assert any(f.endswith(".samples.csv") for f in files)
    assert any(f.endswith(".summary.json") for f in files)
    assert (logs / bm.KVTEST_REPORT_META).is_file()
    ctx = bm._STOP_CTX["report"]
    assert ctx["skip_pull"] is True
    assert ctx["mode"] == "kvtest"
    assert ctx["involved_nodes"] == ["nodeA"]


def test_collect_local_stats_ignores_kvtest_raw_logs(tmp_path):
    """kvtest .log/.glog are C++ wrapper/SDK output, not RESULT logs."""
    logs = tmp_path / "logs"
    logs.mkdir()
    base = _write_instance(str(logs), 0, 9301, "8MB", [1.0], [2.0])
    (logs / (base + ".log")).write_text("no result rows\n", encoding="utf-8")
    (logs / (base + ".log.glog")).write_text("I2026 log\n", encoding="utf-8")
    stats = bm._collect_local_stats(str(logs))
    assert stats == []                       # nothing derived from raw logs
    assert not (logs / (base + ".log.stats.json")).exists()
    assert bm._is_client_log(base + ".log") is False
    assert bm._is_client_log("cfg.dual_node_c0_p9301.reader.log") is True


def test_kvtest_warmup_epoch_and_stop_integration():
    """YLT warmup shifts the measured epoch to /arm; the report map must use
    that epoch, and STOP must still collect samples from either window."""
    src = open(os.path.join(ROOT, "benchmark.py"), encoding="utf-8").read()
    start_pos = src.index('_control("/start", "/start")')
    arm_pos = src.index('_control("/arm", "measurement /arm"')
    resume_pos = src.index('_control("/resume", "measurement /resume"')
    wait_pos = src.index('_wait_window(duration, "measurement")')
    assert start_pos < arm_pos < resume_pos < wait_pos
    # cold path (/start) records an epoch before the warmup branch
    assert "_kvtest_start_epochs" in src[start_pos:arm_pos]
    # warmup path refreshes it between /arm and /resume
    assert "_kvtest_start_epochs" in src[arm_pos:resume_pos]
    # StopRequested from either window collects samples and re-raises
    stop_pos = src.index("except StopRequested:", wait_pos)
    block = src[stop_pos:stop_pos + 900]
    assert "_kvtest_stop_collect(" in block and "raise" in block


# --------------------------------------------------------------------- CLI

def test_logstats_cli_kvtest_mode(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    base = _write_instance(str(logs), 0, 9301, "8MB", [1.0, 2.0], [3.0])
    samples = str(logs / (base + ".samples.csv"))
    summary = str(logs / (base + ".summary.json"))
    out = str(tmp_path / "writer.stats.json")
    proc = subprocess.run(
        [sys.executable, os.path.join(ROOT, "scripts", "logstats.py"),
         "--kvtest-samples", samples, "--start-epoch", str(START),
         "--summary", summary, "--role", "writer", "--size", "8MB",
         "--node", "nodeA", "--source-name", "inst.writer.samples.csv",
         "--out", out],
        capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stderr
    stats = json.load(open(out, encoding="utf-8"))
    assert stats["meta"]["format"] == "kvtest_samples"
    assert stats["meta"]["role"] == "writer"
    assert stats["meta"]["source"] == "inst.writer.samples.csv"
    assert stats["global"]["ok"] == 2
    assert "[logstats]" in proc.stdout
