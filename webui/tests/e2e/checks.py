"""E2E 契约检查：每条 check_* 对一个真实 run 的产物做机器断言（规则 id 见 registry）。

只读产物，不接触 lab。缺字段/文件即失败——绝不靠"看起来对"。
"""
import csv
import json
import os
import re
import subprocess
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_HERE)))
for _p in (ROOT, os.path.join(ROOT, "scripts")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import benchmark as bm  # noqa: E402   (CSV header 契约)

_EMBED_RE = re.compile(r"window\.__MC_REPORT__ = (.*?);</script>", re.S)
_FULL_RE = re.compile(r"window\.__MC_FULL__ = (.*?);\s*$", re.S)


def _data(art):
    raw = open(art.data_js, encoding="utf-8").read()
    return json.loads(_FULL_RE.search(raw).group(1))


def _embedded(art):
    src = open(art.report_html, encoding="utf-8").read()
    return json.loads(_EMBED_RE.search(src).group(1))


def _csv_header(path):
    with open(path, encoding="utf-8") as f:
        return next(csv.reader(f))


def check_run_ok(art):
    """run 成功完成：SUMMARY all OK、无 [ERROR]。"""
    log = open(art.benchmark_log, encoding="utf-8").read()
    assert "SUMMARY: all 1 scenarios OK" in log, "SUMMARY 未达 all OK"
    assert " [ERROR] " not in log, "benchmark.log 含 [ERROR]"


def check_artifacts_present(art):
    """产物四件套 + 两份 CSV + reduced 齐备且非空。"""
    for p in (art.report_html, art.data_js, art.meta_json, art.reduced_json,
              art.detail_csv, art.cfg_csv):
        assert os.path.isfile(p) and os.path.getsize(p) > 0, p


def check_logsite_embedded(art):
    """日志现场表行必须进 report.html 内嵌副本（不能只进 data.js）。"""
    emb = _embedded(art)
    rows = emb.get("logsite") or []
    assert rows, "内嵌副本无 logsite"
    assert len(rows) >= 5 * 3, len(rows)      # 5 节点 × 3 行


def check_trace_parsed(art):
    """trace 解析：无错误、读=breakdown[get_buffer]、写=breakdown[upsert]。"""
    d = _data(art)
    tr = list(d["sizes"].values())[0].get("trace") or {}
    meta = tr.get("meta") or {}
    assert meta.get("parse_errors", 0) == 0, meta
    groups = tr.get("groups") or {}
    assert "get_buffer" in (groups.get("read") or {}).get("ops") or {}, groups
    assert "upsert" in (groups.get("write") or {}).get("ops") or {}, groups


def check_trace_engine(art):
    """每个 op 的 engine 分布非空（classic/tent×N）——TENT/classic 可见。"""
    d = _data(art)
    tr = list(d["sizes"].values())[0].get("trace") or {}
    for g, gv in (tr.get("groups") or {}).items():
        for op, o in (gv.get("ops") or {}).items():
            assert o.get("engine"), (g, op, o.get("engine"))


def check_trace_read_residual(art):
    """读侧残差 ≤ 5%·total（字段错配会把残差撑爆）。"""
    d = _data(art)
    tr = list(d["sizes"].values())[0].get("trace") or {}
    read = (tr.get("groups") or {}).get("read") or {}
    for op, o in (read.get("ops") or {}).items():
        total = (o.get("fields") or {}).get("total_us", {}).get("max", 0)
        res = (o.get("residual") or {}).get("max", 0)
        assert res <= max(50.0, 0.05 * float(total or 0)), (op, res, total)


def check_trace_unknown_ops(art):
    """读/写组的每个 op 都在树注册表里（未知 op 只允许出现在 other 组）。

    树 = configs/_trace_trees/<op>.json（单一事实源）；read/write 组出现
    未登记 op 说明树落后于 SDK 打点，必须补树而不是静默归组。"""
    d = _data(art)
    tr = list(d["sizes"].values())[0].get("trace") or {}
    import json as _json
    import os as _os
    _here = _os.path.dirname(_os.path.dirname(_os.path.dirname(
        _os.path.abspath(__file__))))
    _trees = _os.path.join(_here, "configs", "_trace_trees")
    known = set()
    for fn in _os.listdir(_trees):
        if fn.endswith(".json") and not fn.startswith("_"):
            with open(_os.path.join(_trees, fn), encoding="utf-8") as f:
                known.add(_json.load(f)["op"])
    for g in ("read", "write"):
        for op in ((tr.get("groups") or {}).get(g) or {}).get("ops") or {}:
            assert op in known, ("op 不在树注册表： %s（补 configs/_trace_trees/"
                                 "%s.json）" % (op, op))


def _ssh_du(art, path):
    key = os.environ.get("MC_E2E_SSH_KEY",
                         os.path.expanduser(
                             "~/Mooncake-Sandbox/lab/dev-vm/ssh/id_ed25519"))
    d = _data(art)
    node_ip = (d.get("sources") or [{}])[0].get("node")
    assert node_ip, "sources 无 node"
    out = subprocess.run(
        ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no",
         "-o", "ConnectTimeout=8", "-o", "LogLevel=ERROR",
         "haoyu@%s" % node_ip, "du -sb %s" % path],
        capture_output=True, text=True, timeout=60).stdout.strip()
    m = re.match(r"(\d+)\s", out)
    assert m, "ssh du 失败: %s" % out[:200]
    return int(m.group(1))


def check_logsite_matches_node_du(art):
    """logsite 的 pod 目录大小 ≈ 节点侧 du -sb（±15%）。"""
    d = _data(art)
    meta = d.get("meta") or {}
    root = meta.get("host_log_root") or ""
    run_id = meta.get("run_id") or ""
    assert root and run_id, meta
    rows = d.get("logsite") or []
    pod = next((r for r in rows if r.get("tier") == "pod"), None)
    assert pod, "无 pod 行"
    assert pod["dir"], pod
    real = _ssh_du(art, pod["dir"])
    diff = abs(real - pod["bytes"]) / max(1, real)
    assert diff <= 0.15, (real, pod["bytes"], diff)


def check_logsite_structure(art):
    """行序 [node,pod,monitor]×N；pod 目录以 <host_root>/<run_id>/ 开头。"""
    d = _data(art)
    meta = d.get("meta") or {}
    root = meta.get("host_log_root") or ""
    run_id = meta.get("run_id") or ""
    rows = d.get("logsite") or []
    nodes = []
    for r in rows:
        if r["tier"] == "node":
            nodes.append([])
        elif nodes:
            nodes[-1].append(r["tier"])
    assert nodes, "无节点分组"
    for group in nodes:
        assert group == ["pod", "monitor"], group
    for r in rows:
        if r.get("dir"):
            assert r["dir"].startswith(root + "/" + run_id), r["dir"]


def check_volume_retention(art):
    """V-7「留存」区块：data.js 有 site 台账，report.html 有 留存 行。"""
    d = _data(art)
    site = None
    for z in (d.get("sizes") or {}).values():
        site = (z.get("volume") or {}).get("site")
        if site:
            break
    assert site and site.get("total", 0) > 0, site
    assert "留存" in open(art.report_html, encoding="utf-8").read()


def check_csv_headers(art):
    """两份 CSV 表头精确匹配 KVTEST/PIPELINE 契约。"""
    for p in (art.detail_csv, art.cfg_csv):
        h = _csv_header(p)
        assert h in (bm.KVTEST_CSV_HEADER, bm.PIPELINE_CSV_HEADER), h[:8]


def check_client_objects(art):
    """实例对象身份唯一（49772ea 撞键回归）：每个 size 块的 client 对象
    数 == clients 列表长度（防对象被同名键覆盖丢失）且 ≥1，对象键集合与
    clients 一一对应（client:+name）；client 对象总数 == instances 条数；
    pipeline（kvtest 双角色）下 reader 角色的 client 对象 ≥1（默认 reader
    视图下实例可见的前提）。"""
    d = _data(art)
    with open(art.reduced_json, encoding="utf-8") as f:
        red = json.load(f)
    n_inst = len(red.get("instances") or [])
    total = readers = 0
    for size, blk in (d.get("sizes") or {}).items():
        clients = blk.get("clients") or []
        cobjs = {k: v for k, v in (blk.get("objects") or {}).items()
                 if (v or {}).get("kind") == "client"}
        names = {"client:" + c["name"] for c in clients}
        assert clients and cobjs, size
        assert len(cobjs) == len(clients), (size, len(cobjs), len(clients))
        assert len(names) == len(clients), (size, sorted(names))
        assert set(cobjs) == names, (size, sorted(cobjs), sorted(names))
        total += len(cobjs)
        readers += sum(1 for v in cobjs.values() if v.get("role") == "reader")
    assert total == n_inst, (total, n_inst)
    meta = d.get("meta") or {}
    is_pipeline = ((meta.get("scenario") or meta.get("mode")) in
                   ("pipeline", "kvtest")) or meta.get("engine") == "kvtest"
    if is_pipeline:
        assert readers >= 1, "无 reader 角色实例对象（默认 reader 视图看不到实例）"


def check_embed_skeleton_complete(art):
    """内嵌骨架完整性（20261128 报表残缺回归）：report.html 的内嵌副本
    必须覆盖 data.js 的全部 node/client/role 结构行（端到端对象树与
    分级时延曲线 trace 对象树同款），且每个 client 的 node+role 与同名
    节点对象一致。曲线可裁剪，结构行不可丢——否则 data.js 未加载时树
    会退化成「节点有统计、实例只剩 Top-K」。"""
    emb = _embedded(art)
    d = _data(art)
    for size, blk in (d.get("sizes") or {}).items():
        eb = (emb.get("sizes") or {}).get(size) or {}
        eo = eb.get("objects") or {}
        fo = blk.get("objects") or {}
        want = {k for k, v in fo.items()
                if (v or {}).get("kind") in ("node", "client", "role")}
        assert want, size
        missing = want - set(eo)
        assert not missing, (size, sorted(missing)[:5])
        nodes = {((v or {}).get("node"), (v or {}).get("role"))
                 for v in fo.values() if (v or {}).get("kind") == "node"}
        node_names = {n for n, _ in nodes}
        pipe = any(r for _, r in nodes)   # pipeline: 节点按角色分键
        for k, v in fo.items():
            if (v or {}).get("kind") != "client":
                continue
            ev = eo.get(k) or {}
            assert ev.get("node") == v.get("node"), (size, k)
            assert ev.get("role") == v.get("role"), (size, k)
            if pipe:
                assert (v.get("node"), v.get("role")) in nodes, (size, k)
            else:
                assert v.get("node") in node_names, (size, k)
            if not ev.get("tiers"):
                assert ev.get("trimmed"), (size, k)   # 无曲线须显式标裁剪
        # 实例统计行（错误与异常节点合计、慢点树）同样必须全量内嵌
        by = {(c or {}).get("name"): (c or {})
              for c in (eb.get("clients") or [])}
        fc = blk.get("clients") or []
        missing = {(c or {}).get("name") for c in fc} - set(by)
        assert not missing, (size, "clients", sorted(missing)[:5])
        for c in fc:
            got = by.get((c or {}).get("name")) or {}
            assert got.get("node") == c.get("node"), (size, c.get("name"))
            assert got.get("role") == c.get("role"), (size, c.get("name"))
    # 分级时延曲线（trace）同款：内嵌 trace.objects 覆盖全部结构行；
    # 超预算对象必须是带 trimmed 的骨架（无 groups），曲线留 data.js。
    for size, blk in (d.get("sizes") or {}).items():
        ft = (blk.get("trace") or {}).get("objects") or {}
        if not ft:
            continue
        et = (((emb.get("sizes") or {}).get(size) or {})
              .get("trace") or {}).get("objects") or {}
        want = {k for k, v in ft.items()
                if (v or {}).get("kind") in ("node", "client", "role")}
        missing = want - set(et)
        assert not missing, (size, "trace", sorted(missing)[:5])
        nodes = {((v or {}).get("node"), (v or {}).get("role"))
                 for v in ft.values() if (v or {}).get("kind") == "node"}
        for k, v in ft.items():
            if (v or {}).get("kind") != "client":
                continue
            ev = et.get(k) or {}
            assert ev.get("node") == v.get("node"), (size, k)
            assert ev.get("role") == v.get("role"), (size, k)
            assert (v.get("node"), v.get("role")) in nodes, (size, k)
            if not ev.get("groups"):
                assert ev.get("trimmed"), (size, k)
