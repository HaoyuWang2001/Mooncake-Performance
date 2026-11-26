#!/usr/bin/env python3
"""API 集成测试：线程内起真实 server（随机端口），urllib 打全部端点。

涉及 benchmark 真跑的用例需要 mock 集群（mock/cluster.json + mock venv，
即本仓库 mock/README.md 的环境）；缺省自动跳过该部分。
"""
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from webui.backend.main import create_server

BASE = None          # http://127.0.0.1:<port>
HTTPD = None


def setup_module():
    global BASE, HTTPD
    HTTPD = create_server("127.0.0.1", 0)
    threading.Thread(target=HTTPD.serve_forever, daemon=True).start()
    BASE = "http://127.0.0.1:{}".format(HTTPD.server_address[1])


def _req(method, path, body=None):
    url = BASE + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


def test_status():
    code, d = _req("GET", "/api/status")
    assert code == 200
    assert d["paths"]["runs"] == "webui/data/runs"


def test_help():
    code, d = _req("GET", "/api/help")
    assert code == 200 and "POST /api/run" in d["markdown"]


def test_configs_inventory():
    code, d = _req("GET", "/api/configs")
    assert code == 200
    assert "configs/cluster.json" in d["general"]
    assert not any(p.endswith(".example.json") for p in d["general"])
    assert "basic" in d["categories"] and "pipeline" in d["categories"]
    assert d["categories"]["basic"]["config_count"] > 0


def test_config_get_put_validate():
    tmp = "configs/basic/zz-webui-test.json"   # basic 子目录内才有语义校验
    try:
        # 语义校验失败 -> 422
        code, d = _req("PUT", "/api/config", {"path": tmp,
                                              "content": '{"scenario":"basic","data_size":"8MB"}'})
        assert code == 422 and "data_size" in d["error"]
        # 合法内容 -> 保存 + 备份
        code, d = _req("PUT", "/api/config", {
            "path": tmp,
            "content": json.dumps({"scenario": "basic", "protocol": "tcp",
                                   "data_size": ["64KB"],
                                   "params": [{"num_clients": 1, "num_threads": 2}]})})
        assert code == 200 and "backup" not in d
        # 二次保存已有文件 -> 仍然不备份（WebUI 不留隐藏副本）
        code, d = _req("PUT", "/api/config", {
            "path": tmp,
            "content": json.dumps({"scenario": "basic", "protocol": "tcp",
                                   "data_size": ["64KB"],
                                   "params": [{"num_clients": 1, "num_threads": 2}]})})
        assert code == 200 and "backup" not in d
        code, d = _req("GET", "/api/config?path=" + tmp)
        assert code == 200 and json.loads(d["content"])["scenario"] == "basic"
        # 白名单外 -> 400/404
        code, _ = _req("GET", "/api/config?path=benchmark.py")
        assert code == 404
    finally:
        if os.path.isfile(tmp):
            os.remove(tmp)


def test_config_chain_inheritance():
    """配置继承链：各级 _configs.json（含缺失级）+ 本文件 + 合并后的生效配置。"""
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    tmp = "configs/basic/zz-chain-test.json"
    fp = os.path.join(root, tmp)
    try:
        with open(fp, "w", encoding="utf-8") as f:
            json.dump({"scenario": "basic", "operation": "put",
                       "data_size": ["64KB"],
                       "params": [{"num_clients": 1, "num_threads": 1}]}, f)
        code, d = _req("GET", "/api/config/chain?path=" + tmp)
        assert code == 200 and d["inherits"] is True
        assert d["category"] == "basic"
        paths = [lv["path"] for lv in d["levels"]]
        assert paths[0] == "configs/basic/_configs.json"
        assert paths[-1] == tmp
        assert d["levels"][-1]["kind"] == "self"
        assert all(lv["exists"] for lv in d["levels"])
        eff = json.loads(d["effective"])
        assert eff["scenario"] == "basic" and "data_size" in eff
        # 已有多级链：pipeline 根 + rank 子目录两级都存在
        code, d = _req("GET",
            "/api/config/chain?path=configs/pipeline/rank/rank_balanced_1w10r_1x.json")
        assert code == 200 and d["inherits"] is True
        assert [lv["exists"] for lv in d["levels"]][:2] == [True, True]
        # 子目录缺失的 _configs.json 也列出（exists=False）
        code, d = _req("GET",
            "/api/config/chain?path=configs/pipeline/smoke/rank_smoke.json")
        assert code == 200 and d["inherits"] is True
        assert [lv["exists"] for lv in d["levels"]][:2] == [True, False]
        # 非继承配置：只有 self，无 effective
        code, d = _req("GET", "/api/config/chain?path=configs/cluster.json")
        assert code == 200 and d["inherits"] is False
        assert [lv["kind"] for lv in d["levels"]] == ["self"]
        assert d["effective"] == ""
        # 不存在 / 越界
        code, _ = _req("GET", "/api/config/chain?path=configs/basic/nope.json")
        assert code == 404
        code, _ = _req("GET", "/api/config/chain?path=../etc/passwd")
        assert code == 404
    finally:
        if os.path.isfile(fp):
            os.remove(fp)


def test_run_selector_validation():
    code, d = _req("POST", "/api/run", {"selector": {"type": "file", "path": "x"}})
    assert code == 400 and "legacy" in d["error"]
    code, _ = _req("POST", "/api/run", {"selector": {}})
    assert code == 400
    code, _ = _req("POST", "/api/run", {"selector": {"dirs": ["no/such/dir"]}})
    assert code == 400
    code, _ = _req("POST", "/api/run", {
        "selector": {"files": ["mock/smoke/put.json"]},
        "flags": {"profile": "no/such/profile.json"}})
    assert code == 400


def test_outputs_and_files():
    code, d = _req("GET", "/api/outputs")
    assert code == 200 and isinstance(d["outputs"], list)
    if d["outputs"]:
        ts = d["outputs"][0]["ts"]
        code, d2 = _req("GET", "/api/outputs/{}/tree".format(ts))
        assert code == 200 and isinstance(d2["files"], list)
    # 白名单外 -> 403
    code, _ = _req("GET", "/api/file?path=benchmark.py")
    assert code == 403


def test_output_rename_delete():
    import shutil
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    out = os.path.join(root, "outputs")
    old = "zz-webui-test-" + str(int(time.time() * 1000))
    new = old + "-renamed"
    src = os.path.join(out, old)
    dst = os.path.join(out, new)
    os.makedirs(src)
    try:
        with open(os.path.join(src, "benchmark_detail.csv"), "w") as f:
            f.write("a,b\n1,2\n")
        code, d = _req("GET", "/api/outputs/{}/tree".format(old))
        assert code == 200 and "benchmark_detail.csv" in d["files"]

        # 非法名 / 重名
        code, _ = _req("POST", "/api/outputs/{}/rename".format(old), {"name": "../x"})
        assert code == 400
        os.makedirs(dst)
        code, _ = _req("POST", "/api/outputs/{}/rename".format(old), {"name": new})
        assert code == 409
        os.rmdir(dst)

        # 正常重命名
        code, d = _req("POST", "/api/outputs/{}/rename".format(old), {"name": new})
        assert code == 200 and d["renamed"] is True and d["ts"] == new
        assert os.path.isdir(dst) and not os.path.isdir(src)
        code, d = _req("GET", "/api/outputs/{}/tree".format(new))
        assert code == 200

        # 单删（WSL/Linux 下永久删除；无 trash/backup 副本）
        code, d = _req("DELETE", "/api/outputs/{}".format(new))
        assert code == 200 and "trash" not in d
        assert not os.path.isdir(dst)
        assert not os.path.isdir(os.path.join(root, "webui", "backups"))
    finally:
        for p in (src, dst):
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)

    # 批量删除
    names = []
    for i in range(2):
        ts = "zz-webui-test-batch-{}-{}".format(i, int(time.time() * 1000))
        os.makedirs(os.path.join(out, ts))
        names.append(ts)
    try:
        code, d = _req("POST", "/api/outputs/delete", {"names": names})
        assert code == 200 and sorted(d["deleted"]) == sorted(names) and not d["failed"]
        assert not any(os.path.isdir(os.path.join(out, t)) for t in names)
    finally:
        for t in names:
            shutil.rmtree(os.path.join(out, t), ignore_errors=True)
    code, _ = _req("POST", "/api/outputs/delete", {"names": []})
    assert code == 400


def test_instance_snapshot_and_cleanup():
    """创建实例即冻结配置快照；删除实例连同快照目录一起清理。"""
    root = os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))
    code, d = _req("POST", "/api/instances", {
        "name": "zz-snapshot-smoke",
        "selector": {"files": ["configs/basic/get.json"]},
        "flags": {"mode": "ssh", "cluster": "configs/cluster.json",
                  "global": "configs/global.json",
                  "profile": "configs/_ssh/profile.json"}})
    assert code == 200
    iid = d["instance"]["id"]
    assert d["instance"]["snapshot"]["files"] >= 4
    snap_dir = os.path.join(root, "webui/data/instances", iid)
    assert os.path.isfile(os.path.join(snap_dir, "snapshot.json"))
    assert os.path.isfile(os.path.join(
        snap_dir, "files/configs/basic/_configs.json"))
    # 快照查看接口：清单 + 单文件原文（含 k8s/部署配置分组信息）
    code, snap = _req("GET", "/api/instances/{}/snapshot".format(iid))
    assert code == 200 and snap["files"]
    groups = {f["group"] for f in snap["files"]}
    assert {"部署配置", "场景配置", "继承碎片", "元数据"} <= groups
    assert all(f["live"] in ("same", "changed", "missing", None)
               for f in snap["files"])
    code, fc = _req("GET", "/api/instances/{}/snapshot/file?path={}".format(
        iid, "configs/_ssh/profile.json"))
    assert code == 200 and "\"scenario\"" not in fc["content"][:0]  # 只要求可读
    assert fc["bytes"] > 0
    code, _ = _req("DELETE", "/api/instances/" + iid)
    assert code == 200
    assert not os.path.isdir(snap_dir)
    code, _ = _req("GET", "/api/instances/" + iid)
    assert code == 404


def test_schedule_plans():
    code, d = _req("GET", "/api/schedule/plans")
    assert code == 200
    names = [p["name"] for p in d["plans"]]
    assert "plan.example.json" in names


def _mock_ready():
    return (os.path.isfile("mock/cluster.json")
            and os.path.isfile("mock/smoke/put.json"))


def test_dry_run_via_api():
    if not _mock_ready():
        print("  [skip] mock cluster not present")
        return
    prof = "configs/zz-webui-test-profile.json"
    with open(prof, "w", encoding="utf-8") as f:
        json.dump({"tools": {"spdiag": {"enabled": False}}}, f)
    # dry-run 用 mock 集群专属 global（venv=mock），不依赖
    # configs/_ssh/global.json 的环境默认值——该文件在本地 lab 沙箱里是
    # 适配过的（venv=mooncake-bench），依赖它会让本测试因环境而失败。
    gpath = "mock/zz-global-test.json"
    with open(gpath, "w", encoding="utf-8") as f:
        json.dump({"venv": "mock", "master_type": "http",
                   "etcd_endpoints": "", "master_addr": "127.0.0.1:50051",
                   "metadata_server": "http://127.0.0.1:9999/metadata",
                   "mooncake_store_nodes": ["store1"],
                   "remote_dir": "/tmp/mc_mock",
                   "remote_data_dir": "/tmp/mc_mock/data",
                   "remote_results_dir": "/tmp/mc_mock/results"}, f)
    try:
        code, d = _req("POST", "/api/run", {
            "selector": {"files": ["mock/smoke/put.json"]},
            "flags": {"cluster": "mock/cluster.json",
                      "global": gpath,
                      "profile": prof, "dry_run": True}})
        assert code == 200, d
        rid = d["run"]["id"]
        for _ in range(120):
            code, d = _req("GET", "/api/runs/" + rid)
            if d.get("status") != "running":
                break
            time.sleep(0.5)
        assert d["status"] == "done" and d["rc"] == 0, d
        # 日志端点可读（webui/data/runs 白名单）
        code, d = _req("GET", "/api/runs/{}/log?offset=0".format(rid))
        assert code == 200 and len(d["content"]) > 0
    finally:
        if os.path.isfile(prof):
            os.remove(prof)


if __name__ == "__main__":
    setup_module()
    fails = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("PASS", name)
            except AssertionError as exc:
                fails += 1
                print("FAIL", name, exc)
            except Exception as exc:
                fails += 1
                print("ERROR", name, exc)
    HTTPD.shutdown()
    sys.exit(1 if fails else 0)
