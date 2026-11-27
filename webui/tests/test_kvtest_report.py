"""kvtest two-stage report chain: pod map -> partial reduce -> finalize/render.

Contract under test (no sample/summary transfer):
  * each PROCESS maps its own latency_samples.csv once, in place, writing a
    stats product (histogram + REQ digests) and a csvstats product;
  * the controller reduces everything except the digest tails (stdlib only),
    producing one partial.json.gz with an instances block and _digest_pending;
  * the orchestrator merges the digests, injects monitors, builds the logsite,
    writes reduced.json and renders; the scenario CSV reads ONLY reduced.json.
"""
import csv
import gzip
import json
import os
import sys
from types import SimpleNamespace

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "scripts"))

import benchmark as bm  # noqa: E402
import logstats as ls  # noqa: E402
import report as rp  # noqa: E402

START = 1763596800.0  # fixed epoch so bucket/epoch assertions are exact
WARM = 2.0


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
        "_kvtest_warmup_duration": WARM,
        "_kvtest_nodes": 2,
        "_kvtest_iid_nodes": {"0": "nodeA", "1": "nodeB"},
        "_kvtest_iid_pods": {"0": "pod-a", "1": "pod-b"},
        "_kvtest_key_pool": {"8MB": 256},
        "_writer_count": 2, "_reader_count": 2,
        "_kvtest_digest_k": 24,
    }
    cfg.update(over)
    return cfg


def _args():
    return SimpleNamespace(dry_run=False, png=False, pull_raw_logs=False,
                           keep_intermediates=False, no_report=False,
                           cluster="mock/cluster.json",
                           global_cfg="configs/_ssh/global.json",
                           profile="configs/_ssh/profile.json", mode="ssh")


def _write_instance(d, iid, role, lats, ds="8MB", warm_rows=0, verify=0,
                    qw=None):
    """Write one process's samples.csv + summary.json into its own dir."""
    inst_dir = os.path.join(d, "{}_i{}_p{}".format(role, iid, 9100 + iid))
    os.makedirs(inst_dir, exist_ok=True)
    base = "case.i{}_p{}.kvtest_{}".format(iid, 9100 + iid, ds)
    samples = os.path.join(inst_dir, base + ".samples.csv")
    op = "upsert_from" if role == "writer" else "get_buffer"
    rows = []
    seq = 0
    for i in range(warm_rows):
        # elapsed < warmup seconds -> chart-only, excluded from the products
        rows.append((int(i * 100000), seq, 0.5))
        seq += 1
    for i, lat in enumerate(lats):
        rows.append((int((WARM + i * 0.2) * 1e6), seq, lat))
        seq += 1
    with open(samples, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["elapsed_us", "instance_id", "operation",
                    "request_index", "latency_ms", "status", "bytes"])
        for elapsed_us, idx, lat in rows:
            w.writerow([elapsed_us, iid, op, idx, lat, "OK", 8388608])
    ops = {op: {"count": len(lats), "total": len(lats),
                "success": len(lats), "failure": 0,
                "bytes": len(lats) * 8388608}}
    if qw:
        n = int(qw.get("count") or qw.get("total") or 1)
        ops["notify_queue_wait"] = dict(
            qw, count=n, total=n, success=n, failure=0, bytes=0)
    summary = os.path.join(inst_dir, base + ".summary.json")
    with open(summary, "w", encoding="utf-8") as f:
        json.dump({"instance_id": iid, "elapsed_seconds": 10.0,
                   "verify_failures": verify, "operations": ops}, f)
    with open(os.path.join(inst_dir, "case.log"), "w") as f:
        f.write("x" * 128)
    return inst_dir, base, samples, summary


def _map(d, iid, role, lats, ds="8MB", warm_rows=0, verify=0, qw=None,
         source_name=None, samples_name=None):
    """Map one process. samples_name: rename the samples CSV to the k8s
    on-site name ("latency_samples.csv", identical across ALL processes —
    the incident shape where an omitted source_name collides every
    instance). source_name: pass through to kvtest_map_instance (None = the
    basename(samples) fallback)."""
    inst_dir, base, samples, summary = _write_instance(
        d, iid, role, lats, ds=ds, warm_rows=warm_rows, verify=verify, qw=qw)
    if samples_name:
        import shutil
        same = os.path.join(inst_dir, samples_name)
        shutil.copy(samples, same)
        samples = same
    pod = "pod-{}".format(iid)
    node_path = ("/var/lib/mooncake-bench/20260101-0001/{}/{}_{}_p{}/"
                 "{}.stats.json.gz".format(pod, role, iid, 9100 + iid, base))
    stats, csvstats = ls.kvtest_map_instance(
        samples, summary, role, node="10.0.0.{}".format(iid + 1),
        pod=pod, size=ds, start_epoch=START,
        warmup_s=WARM, digest_k=24, remote_path=node_path, iid=iid,
        instance_dir=inst_dir, source_name=source_name)
    paths = {}
    for key, doc, name in (("stats", stats, ".stats.json"),
                           ("csvstats", csvstats, ".csvstats.json")):
        path = os.path.join(os.path.dirname(samples), base + name)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(doc, f)
        paths[key] = path
    paths.update({"base": base, "inst_dir": inst_dir, "iid": iid,
                  "node": "10.0.0.{}".format(iid + 1),
                  "pod": pod, "role": role})
    return paths


def _partial(d, prods, ds="8MB"):
    import importlib.util
    path = os.path.join(ROOT, "scripts", "kvtest_partial_reduce.py")
    spec = importlib.util.spec_from_file_location("kvtest_partial_reduce", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    instances = []
    for p in prods:
        instances.append({"iid": int(p["iid"]), "node": p["node"],
                          "pod": p["pod"], "role": p["role"], "size": ds,
                          "stats": os.path.relpath(p["stats"], d),
                          "csvstats": os.path.relpath(p["csvstats"], d),
                          "trace": None})
    out = os.path.join(d, "report", ds, "partial.json.gz")
    mod.run({"dir": d, "out": out, "ds": ds,
             "meta": {"config": "kvtest_demo", "mode": "pipeline",
                      "engine": "kvtest", "unit": ds,
                      "run_id": "20260101-0001",
                      "host_log_root": "/var/lib/mooncake-bench"},
             "instances": instances})
    return out


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


# ------------------------------------------------------------------ pod map

def test_iter_kvtest_rows_maps_latency_status_and_epoch(tmp_path):
    _inst_dir, base, samples, _summary = _write_instance(
        str(tmp_path), 0, "writer", [1.0, 2.0])
    rows = list(ls.iter_kvtest_rows(samples, START, ls.KVTEST_WRITE_OPS))
    assert [r["lat"] for r in rows] == [1.0, 2.0]
    assert all(r["op"] == "upsert_from" for r in rows)
    assert rows[0]["epoch"] == pytest.approx(START + WARM)
    assert rows[1]["epoch"] == pytest.approx(START + WARM + 0.2)
    assert not any(r["op"] == "get_buffer" for r in rows)


def test_pod_map_writes_products_and_dir_bytes(tmp_path, fake_sketch):
    p = _map(str(tmp_path), 0, "writer", [1.0, 2.0, 3.0])
    with open(p["stats"], encoding="utf-8") as f:
        stats = json.load(f)
    with open(p["csvstats"], encoding="utf-8") as f:
        csvstats = json.load(f)
    assert stats["meta"]["role"] == "writer"
    assert stats["meta"]["format"] == "kvtest_samples"
    assert stats["meta"]["size_mode"] == "dir"
    assert stats["meta"]["bytes"] > 0            # per-process dir apparent size
    assert stats["meta"]["bytes"] == csvstats["meta_bytes"]
    assert stats["digest"]["global"]             # pod map carries the sketch
    assert stats["global"]["ok"] == 3
    assert set(stats["ops"]) == {"upsert_from"}
    assert csvstats["role"] == "writer"
    assert csvstats["ops"]["upsert_from"]["total"] == 3
    assert csvstats["ops"]["upsert_from"]["bytes"] == 3 * 8388608


def test_pod_map_excludes_warmup_rows_from_formal_window(tmp_path,
                                                         fake_sketch):
    p = _map(str(tmp_path), 0, "writer", [1.0, 2.0], warm_rows=5)
    with open(p["stats"], encoding="utf-8") as f:
        stats = json.load(f)
    with open(p["csvstats"], encoding="utf-8") as f:
        csvstats = json.load(f)
    # 5 warmup rows are chart-only: neither the formal stats nor the CSV block
    # (meta.rows counts every parsed row; the formal window is in global/ops)
    assert stats["global"]["ok"] == 2
    assert stats["meta"]["rows"] == 7
    assert csvstats["ops"]["upsert_from"]["total"] == 2
    assert csvstats["warmup_s"] == WARM


def test_pod_map_reads_summary_in_place(tmp_path, fake_sketch):
    p = _map(str(tmp_path), 2, "reader", [5.0], verify=7,
             qw={"avg_ms": 0.5, "max_ms": 1.5})
    with open(p["csvstats"], encoding="utf-8") as f:
        cs = json.load(f)
    assert cs["verify_failures"] == 7
    assert cs["queue"]["avg_ms"] == 0.5 and cs["queue"]["max_ms"] == 1.5
    assert cs["notify"] is None
    # summary is read in place, never copied next to the products
    assert not os.path.exists(os.path.join(p["inst_dir"], "summary.copy"))
    with open(p["stats"], encoding="utf-8") as f:
        st = json.load(f)
    assert st["global"]["qw_avg_ms"] == 0.5


# ------------------------------------------------------- controller partial

def test_partial_reduce_defers_tails_and_keeps_instances(tmp_path,
                                                         fake_sketch):
    d = str(tmp_path)
    prods = [_map(d, 0, "writer", [1.0, 2.0, 3.0]),
             _map(d, 1, "writer", [4.0, 5.0]),
             _map(d, 2, "reader", [6.0, 7.0], verify=3)]
    out = _partial(d, prods)
    with gzip.open(out, "rb") as f:
        data = json.loads(f.read().decode("utf-8"))
    blk = data["sizes"]["8MB"]
    w = blk["role_summaries"]["writer"]
    assert w["pct_method"] == "pending"
    assert w["p99_ms"] is None
    assert w["max_ms"] == 5.0
    pend = data["_digest_pending"]
    assert pend["k"] == 24
    assert len(pend["scopes"]["8MB"]["all"]) == 3
    assert len(pend["scopes"]["8MB"]["writer"]) == 2
    assert len(pend["scopes"]["8MB"]["reader"]) == 1
    assert "logsite" not in data
    by_iid = {it["iid"]: it for it in data["instances"]}
    assert by_iid[1]["total"] == 2 and by_iid[1]["bytes"] == 2 * 8388608
    assert by_iid[2]["verify_failures"] == 3
    assert by_iid[2]["pcts"]["p99_ms"] is not None


# ------------------------------------------------------- orchestrator finalize

def test_finalize_merges_digests_and_builds_logsite(tmp_path, fake_sketch):
    d = str(tmp_path)
    prods = [_map(d, 0, "writer", [1.0, 2.0, 3.0]),
             _map(d, 1, "writer", [4.0, 5.0]),
             _map(d, 2, "reader", [6.0, 7.0])]
    partial = _partial(d, prods)
    reds = bm._kvtest_finalize_stage(d)
    assert reds == [os.path.join(d, "report", "8MB", "reduced.json")]
    with open(reds[0], encoding="utf-8") as f:
        data = json.load(f)
    w = data["sizes"]["8MB"]["role_summaries"]["writer"]
    assert w["pct_method"] == "hist+digest"
    assert w["p99_ms"] is not None
    assert w["p99_ms"] <= w["max_ms"]          # never above the true maximum
    assert "_digest_pending" not in data
    assert data["monitors"] == {}
    site = data["logsite"]
    pods = [r for r in site if r["tier"] == "pod"]
    dirs = {r["dir"] for r in pods}
    # each process directory is inside its pod dir; bytes come from meta.bytes
    assert all("/pod-" in x for x in dirs)
    assert sum(r["bytes"] for r in pods) > 0
    vol = data["sizes"]["8MB"]["volume"]
    assert vol["reduce"]["pulled_bytes"] == os.path.getsize(partial)
    assert vol["raw"]["dir_mode"] is True               # 目录口径（DR-61）


def test_finalize_is_idempotent(tmp_path, fake_sketch):
    d = str(tmp_path)
    _partial(d, [_map(d, 0, "writer", [1.0])])
    red = bm._kvtest_finalize_stage(d)[0]
    before = os.path.getmtime(red)
    assert bm._kvtest_finalize_stage(d) == [red]
    assert os.path.getmtime(red) == before


def test_finalize_requires_pending_and_scope_digests(tmp_path, fake_sketch):
    with pytest.raises(RuntimeError):
        rp.finalize_digests({"sizes": {"8MB": {"summary": {}}}})
    import base64
    good = base64.b64encode(b"[1.0]").decode("ascii")
    data = {"meta": {}, "sources": [], "sizes": {"8MB": {
        "summary": {"max_ms": 5.0},
        "role_summaries": {"writer": {"max_ms": 5.0}},
    }}, "_digest_pending": {"k": 24, "scopes": {"8MB": {
        "all": [good], "writer": []}}}}
    with pytest.raises(RuntimeError):
        rp.finalize_digests(data)


# -------------------------------------------------------------------- render

def test_finalize_stage_and_render_write_html(tmp_path, fake_sketch):
    d = str(tmp_path)
    _partial(d, [_map(d, 0, "writer", [1.0, 2.0]),
                 _map(d, 1, "reader", [3.0])])
    bm._kvtest_finalize_stage(d)
    written = bm._kvtest_render_stage(_cfg(), d, _args())
    assert len(written) == 1 and written[0].endswith("report.html")
    unit = os.path.join(d, "report", "8MB")
    assert os.path.isfile(os.path.join(unit, "data.js"))
    assert os.path.isfile(os.path.join(unit, "meta.json"))
    html = open(written[0], encoding="utf-8").read()
    assert "kvtest" in html or "场景" in html
    assert bm._REPORT_ACC and bm._REPORT_ACC[-1]["unit"] == "8MB"


def test_client_identity_survives_duplicate_sources(tmp_path, fake_sketch):
    """事故（49772ea 回归）：map 未传 source_name 时所有进程 meta.source
    都是 latency_samples.csv，reduce_size 的 objects["client:<name>"] 撞键，
    10 个实例只剩 1 个对象（幸存 writer），pipeline 默认 reader 视图下端到
    端时延的对象树里看不到实例。防御：同名组按 remote_path 剥 stats 后缀
    重建 <base>.<role> 唯一名（=历史报告的实例命名）。"""
    d = str(tmp_path)
    prods = [_map(d, 0, "writer", [1.0, 2.0],
                  samples_name="latency_samples.csv"),
             _map(d, 1, "reader", [3.0, 4.0],
                  samples_name="latency_samples.csv")]
    for p in prods:            # 事故形态：两个进程 meta.source 同名
        with open(p["stats"], encoding="utf-8") as f:
            assert json.load(f)["meta"]["source"] == "latency_samples.csv"
    _partial(d, prods)
    bm._kvtest_finalize_stage(d)
    bm._kvtest_render_stage(_cfg(), d, _args())
    with open(os.path.join(d, "report", "8MB", "reduced.json"),
              encoding="utf-8") as f:
        z = json.load(f)["sizes"]["8MB"]
    # 对象树：每个实例一个 client 对象，键唯一，无撞键幸存者
    cobjs = {k: v for k, v in z["objects"].items()
             if (v or {}).get("kind") == "client"}
    want = {"case.i0_p9100.kvtest_8MB.writer",
            "case.i1_p9101.kvtest_8MB.reader"}
    assert "client:latency_samples.csv" not in z["objects"]
    assert len(cobjs) == 2 == len(z["clients"])
    assert set(cobjs) == {"client:" + c["name"] for c in z["clients"]}
    assert {o["label"] for o in cobjs.values()} == want
    assert {c["name"] for c in z["clients"]} == want
    # slowest 行的 client 字段与对象名同源
    assert z["slowest"] and all(r["client"] in want for r in z["slowest"])
    # 渲染出的 data.js 同样保留两个实例对象
    dj = open(os.path.join(d, "report", "8MB", "data.js"),
              encoding="utf-8").read()
    assert "case.i0_p9100.kvtest_8MB.writer" in dj
    assert "client:latency_samples.csv" not in dj
    # 正常路径：显式传旧格式 source_name —— meta.source 保持传入值，
    # _client_name 剥 .samples.csv 尾即最终对象名（无重名，dedupe 不介入）
    d2 = os.path.join(str(tmp_path), "named")
    base = "case.i0_p9100.kvtest_8MB"
    p2 = _map(d2, 0, "writer", [1.0],
              source_name="{}.writer.samples.csv".format(base))
    with open(p2["stats"], encoding="utf-8") as f:
        st = json.load(f)
    assert st["meta"]["source"] == "{}.writer.samples.csv".format(base)
    z2 = rp.reduce_size([st], {"config": "kvtest_demo",
                               "scenario": "kvtest"})
    assert [k for k, v in z2["objects"].items()
            if v.get("kind") == "client"] == ["client:" + base + ".writer"]
    assert z2["clients"][0]["name"] == base + ".writer"


# ----------------------------------------------------------------------- CSV

def _csv_env(tmp_path, fake_sketch):
    d = str(tmp_path)
    _partial(d, [_map(d, 0, "writer", [1.0, 2.0, 3.0]),
                 _map(d, 1, "writer", [4.0, 5.0]),
                 _map(d, 2, "reader", [6.0, 7.0], verify=3,
                      qw={"avg_ms": 0.5, "max_ms": 1.5})])
    bm._kvtest_finalize_stage(d)
    cfg = _cfg()
    path = os.path.join(d, "case.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f)
    cfg["__cfg_path__"] = path
    return d, cfg


def test_csv_is_built_from_reduced_only(tmp_path, fake_sketch):
    d, cfg = _csv_env(tmp_path, fake_sketch)
    rows = bm.write_scenario_csv(d, cfg, "20260101-0001",
                                 cfg["__cfg_path__"], None, None,
                                 result_dir=d)
    by_role = {}
    for r in rows:
        by_role.setdefault(r["role"], []).append(r)
    assert set(by_role) == {"writer", "reader"}
    w = {r["instance"]: r for r in by_role["writer"]}
    assert set(w) == {"i0", "i1", "TOTAL"}
    # wall = elapsed - warmup; qps = success / wall
    assert w["i0"]["wall_s"] == 10.0 - WARM
    assert w["i0"]["total"] == 3 and w["i0"]["success"] == 3
    assert w["i0"]["total_bytes"] == 3 * 8388608
    # instance percentiles come from the pod map (hist + digests)
    assert w["i0"]["p99_ms"] not in ("", None)
    # TOTAL counts/bytes are exact sums; percentiles are the role summary
    assert w["TOTAL"]["total"] == 5
    assert w["TOTAL"]["total_bytes"] == 5 * 8388608
    with open(os.path.join(d, "report", "8MB", "reduced.json"),
              encoding="utf-8") as f:
        red = json.load(f)
    summ = red["sizes"]["8MB"]["role_summaries"]["writer"]
    assert w["TOTAL"]["p99_ms"] == round(summ["p99_ms"], 4)
    # reader row carries summary-only signals
    r = {x["instance"]: x for x in by_role["reader"]}
    assert r["i2"]["verify_failures"] == 3
    assert r["i2"]["queue_wait_avg_ms"] == 0.5
    assert r["TOTAL"]["verify_failures"] == 3
    header = open(os.path.join(d, "case.csv"), encoding="utf-8").readline()
    assert header.strip().split(",") == bm.KVTEST_CSV_HEADER


def test_csv_requires_reduced(tmp_path):
    d = str(tmp_path)
    cfg = _cfg()
    path = os.path.join(d, "case.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cfg, f)
    cfg["__cfg_path__"] = path
    with pytest.raises(RuntimeError) as ei:
        bm.write_scenario_csv(d, cfg, "20260101-0001", path, None, None,
                              result_dir=d)
    assert "reduced" in str(ei.value)


# ------------------------------------------------------------------- rebuild

def test_rebuild_renders_from_reduced(tmp_path, fake_sketch, monkeypatch):
    d, _cfg_ = _csv_env(tmp_path, fake_sketch)
    ts = "20260101-0001"
    ts_dir = os.path.join(d, ts)
    os.makedirs(os.path.join(ts_dir, "logs"))
    src = os.path.join(d, "report")
    if os.path.isdir(src):
        import shutil
        shutil.move(src, os.path.join(ts_dir, "logs", "kvtest_demo",
                                      "report"))
    monkeypatch.setattr(bm, "RESULTS_DIR", d)
    assert bm.rebuild_reports(ts) == 0
    assert os.path.isfile(os.path.join(
        ts_dir, "logs", "kvtest_demo", "report", "8MB", "report.html"))
    assert os.path.isfile(os.path.join(ts_dir, "report", "overview.html"))


def test_rebuild_errors_without_products_or_report(tmp_path, monkeypatch):
    d = str(tmp_path)
    os.makedirs(os.path.join(d, "20260101-0002", "logs", "kvtest_demo"))
    monkeypatch.setattr(bm, "RESULTS_DIR", d)
    assert bm.rebuild_reports("20260101-0002") == 1


def _move_fixture_into_run(tmp_path, fake_sketch, ts):
    """_csv_env 产物搬进 outputs/<ts>/logs/kvtest_demo/ 的 run 布局。"""
    d, cfg = _csv_env(tmp_path, fake_sketch)
    ts_dir = os.path.join(d, ts)
    os.makedirs(os.path.join(ts_dir, "logs"))
    src = os.path.join(d, "report")
    if os.path.isdir(src):
        import shutil
        shutil.move(src, os.path.join(ts_dir, "logs", "kvtest_demo",
                                      "report"))
    return d, ts_dir, {k: v for k, v in cfg.items()
                       if not str(k).startswith("__")}


def test_rebuild_regenerates_csv_and_detail(tmp_path, fake_sketch,
                                            monkeypatch):
    """rebuild 补齐 run 时失败的 CSV：report/cfg.json（run 时持久化的
    生效配置）让 write_scenario_csv 以与 run 相同的输入重算
    kvtest_demo.csv 与 benchmark_detail.csv。"""
    d, ts_dir, persist = _move_fixture_into_run(
        tmp_path, fake_sketch, "20260101-0001")
    with open(os.path.join(ts_dir, "logs", "kvtest_demo", "report",
                           "cfg.json"), "w", encoding="utf-8") as f:
        json.dump(persist, f, ensure_ascii=False)
    monkeypatch.setattr(bm, "RESULTS_DIR", d)
    assert bm.rebuild_reports("20260101-0001") == 0
    with open(os.path.join(ts_dir, "kvtest_demo.csv"), newline="",
              encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert {r["role"] for r in rows} == {"writer", "reader"}
    totals = [r for r in rows if r["instance"] == "TOTAL"]
    assert len(totals) == 2
    w = [r for r in totals if r["role"] == "writer"][0]
    assert w["total"] == "5" and w["total_bytes"] == str(5 * 8388608)
    with open(os.path.join(ts_dir, "benchmark_detail.csv"), newline="",
              encoding="utf-8") as f:
        detail = list(csv.DictReader(f))
    assert [r["instance"] for r in detail] == ["TOTAL", "TOTAL"]
    assert {r["role"] for r in detail} == {"writer", "reader"}


def test_rebuild_keeps_runtime_csv_without_cfg(tmp_path, fake_sketch,
                                               monkeypatch):
    """无 report/cfg.json 且快照/仓库找不到配置：per-config CSV 保持
    原样不清空，detail 从既有 CSV 读回组装。"""
    d, ts_dir, _persist = _move_fixture_into_run(
        tmp_path, fake_sketch, "20260101-0003")
    with open(os.path.join(ts_dir, "kvtest_demo.csv"), "w", newline="",
              encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=bm.KVTEST_CSV_HEADER)
        w.writeheader()
        row = {k: "" for k in bm.KVTEST_CSV_HEADER}
        row.update({"config_name": "kvtest_demo", "scenario": "kvtest",
                    "role": "writer", "instance": "TOTAL",
                    "data_size": "8MB", "total": "5", "success": "5",
                    "fail": "0"})
        w.writerow(row)
    monkeypatch.setattr(bm, "RESULTS_DIR", d)
    assert bm.rebuild_reports("20260101-0003") == 0
    with open(os.path.join(ts_dir, "kvtest_demo.csv"), newline="",
              encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 1 and rows[0]["total"] == "5"
    with open(os.path.join(ts_dir, "benchmark_detail.csv"), newline="",
              encoding="utf-8") as f:
        detail = list(csv.DictReader(f))
    assert [r["instance"] for r in detail] == ["TOTAL"]


def test_rebuild_loads_cfg_from_instance_snapshot(tmp_path, fake_sketch,
                                                  monkeypatch):
    """无 report/cfg.json 的历史 run（cfg.json 持久化之前）：实例快照里
    冻结的配置作为 CSV 重建的来源。"""
    d, ts_dir, persist = _move_fixture_into_run(
        tmp_path, fake_sketch, "20260101-0004")
    snap = os.path.join(ts_dir, "snapshot", "files", "configs", "kvtest",
                        "smoke")
    os.makedirs(snap)
    with open(os.path.join(snap, "kvtest_demo.json"), "w",
              encoding="utf-8") as f:
        json.dump(persist, f, ensure_ascii=False)
    monkeypatch.setattr(bm, "RESULTS_DIR", d)
    assert bm.rebuild_reports("20260101-0004") == 0
    with open(os.path.join(ts_dir, "kvtest_demo.csv"), newline="",
              encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    assert {r["role"] for r in rows} == {"writer", "reader"}


# ---------------------------------------------------------------------- STOP

def test_stop_collect_partial_reuses_map_and_partial_reduce(monkeypatch,
                                                            tmp_path):
    seen = {}
    def _fake_map(*a, **k):
        seen["map"] = seen.get("map", 0) + 1
    monkeypatch.setattr(bm, "_kvtest_pod_map", _fake_map)
    monkeypatch.setattr(bm, "_kvtest_remote_trace_map",
                        lambda *a, **k: ["/x.trace.json"])
    monkeypatch.setattr(bm, "_kvtest_gzip_remote_files",
                        lambda cluster, node, files: [f + ".gz"
                                                      for f in files])
    monkeypatch.setattr(bm, "_pipeline_http_parallel",
                        lambda *a, **k: [("i0", True, None)])
    monkeypatch.setattr(bm, "_wait_kvtest_processes",
                        lambda *a, **k: None)
    monkeypatch.setattr(bm, "_pipeline_pull_parallel",
                        lambda *a, **k: seen.setdefault("pull", True))
    monkeypatch.setattr(bm, "_kvtest_local_partial_reduce",
                        lambda *a, **k: seen.setdefault("partial", True))
    monkeypatch.setattr(bm, "get_node",
                        lambda cluster, name: {"ip": "10.0.0.1"})
    entry = {"iid": 0, "node": "n0", "_base": "case.i0",
             "_log_base": "/logs/case.i0",
             "_ds": "8MB", "_out_dir": "/logs/out", "_role": "writer"}
    cfg = _cfg()
    bm._STOP_CTX.update(args=_args())
    bm._kvtest_stop_collect(
        {"nodes": {}}, cfg, "kvtest_demo", "/remote", str(tmp_path),
        str(tmp_path), [(entry, None)], [(entry, "/start/0")],
        remote_scripts="/scripts", globals_cfg={}, ts="20260101-0001",
        is_k8s=False)
    assert seen.get("map") and seen.get("pull") and seen.get("partial")
    assert bm._STOP_CTX["report"]["mode"] == "kvtest"
    assert bm._STOP_CTX["report"]["skip_pull"] is True
