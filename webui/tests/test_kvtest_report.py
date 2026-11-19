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
                    bad=(), cfg_name="kvtest_demo"):
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
    summary = {
        "instance_id": iid, "elapsed_seconds": 10.0, "verify_failures": 0,
        "operations": {
            "upsert_from": {"count": len(writer_lats), "total": len(writer_lats),
                            "success": len(writer_lats), "failure": 0,
                            "bytes": len(writer_lats) * 8388608},
            "get_buffer": {"count": len(reader_lats), "total": len(reader_lats),
                           "success": len(reader_lats), "failure": 0,
                           "bytes": len(reader_lats) * 8388608},
        },
    }
    with open(os.path.join(logs, base + ".summary.json"), "w",
              encoding="utf-8") as f:
        json.dump(summary, f)
    return base


@pytest.fixture(autouse=True)
def _clean_report_acc():
    saved = list(bm._REPORT_ACC)
    bm._REPORT_ACC.clear()
    yield
    bm._REPORT_ACC.clear()
    bm._REPORT_ACC.extend(saved)


def _args():
    return SimpleNamespace(no_plot=False, dry_run=False, png=False,
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
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_demo.json")
    bm._kvtest_generate_reports(cfg, str(logs), _args())
    out = logs / "report" / "8MB"
    assert (out / "report.html").is_file()
    assert (out / "data.js").is_file()
    assert (out / "meta.json").is_file()
    meta = json.load(open(out / "meta.json", encoding="utf-8"))
    assert meta["mode"] == "kvtest"
    assert meta["unit_size"] == "8MB"
    assert any(p[0] == "场景" and p[1] == "kvtest" for p in meta["params"])

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
    assert data["meta"]["mode"] == "kvtest"
    assert any(k.startswith("node:nodeA:") for k in blk["objects"])
    params = data["meta"]["params"]
    assert ["场景", "kvtest"] in params
    assert any(p[0] == "客户端总数" for p in params)


def test_kvtest_report_meta_roundtrip(tmp_path):
    logs = tmp_path / "logs"
    logs.mkdir()
    assert bm._kvtest_read_report_meta(str(logs)) == {}
    bm._kvtest_write_report_meta(str(logs), {
        "_kvtest_nodes": 3, "_kvtest_iid_nodes": {"1": "nodeB"},
        "_kvtest_start_epochs": {"8MB": START},
        "writer": {"operation": "upsert"}})
    meta = bm._kvtest_read_report_meta(str(logs))
    assert meta["nodes"] == 3
    assert meta["iid_nodes"] == {"1": "nodeB"}
    assert meta["start_epochs"] == {"8MB": START}
    assert meta["writer_operation"] == "upsert"


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
                    bad=(800.0,), cfg_name="kvtest_js")
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


def test_kvtest_cleanup_keeps_stats_and_report(tmp_path):
    """Success cleanup removes raw kvtest inputs but keeps map products."""
    logs = tmp_path / "logs" / "kvtest_clean"
    logs.mkdir(parents=True)
    base = _write_instance(str(logs), 0, 9301, "8MB", [1.0], [2.0],
                           cfg_name="kvtest_clean")
    cfg = _cfg()
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_clean.json")
    bm._kvtest_generate_reports(cfg, str(logs), _args())
    removed = bm._cleanup_kvtest_success_intermediates(str(logs))
    assert removed == 2                       # summary + samples
    assert not (logs / (base + ".samples.csv")).exists()
    assert not (logs / (base + ".summary.json")).exists()
    assert list(logs.glob("*.stats.json"))
    assert (logs / "report" / "8MB" / "report.html").is_file()


def test_kvtest_report_disabled_with_no_plot(tmp_path):
    logs = tmp_path / "logs" / "kvtest_noplot"
    logs.mkdir(parents=True)
    _write_instance(str(logs), 0, 9301, "8MB", [1.0], [2.0],
                    cfg_name="kvtest_noplot")
    cfg = _cfg()
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_noplot.json")
    args = _args()
    args.no_plot = True
    assert bm._kvtest_generate_reports(cfg, str(logs), args) is None
    assert not (logs / "report").exists()
    assert not list(logs.glob("*.stats.json"))


def test_kvtest_report_is_warn_only_on_bad_samples(tmp_path):
    """A corrupt/unreadable sample set must never fail the benchmark path."""
    logs = tmp_path / "logs" / "kvtest_bad"
    logs.mkdir(parents=True)
    (logs / "kv.i0_p9301.kvtest_8MB.samples.csv").write_text(
        "not,the,expected\n1,2,3\n", encoding="utf-8")
    cfg = _cfg()
    cfg["__cfg_path__"] = str(tmp_path / "kvtest_bad.json")
    assert bm._kvtest_generate_reports(cfg, str(logs), _args()) is None


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
