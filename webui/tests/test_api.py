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
        assert code == 200
        assert d["backup"] is None or d["backup"].startswith("webui/backups/")
        # 二次保存已有文件 -> 必有备份
        code, d = _req("PUT", "/api/config", {
            "path": tmp,
            "content": json.dumps({"scenario": "basic", "protocol": "tcp",
                                   "data_size": ["64KB"],
                                   "params": [{"num_clients": 1, "num_threads": 2}]})})
        assert code == 200 and d["backup"].startswith("webui/backups/")
        code, d = _req("GET", "/api/config?path=" + tmp)
        assert code == 200 and json.loads(d["content"])["scenario"] == "basic"
        # 白名单外 -> 400/404
        code, _ = _req("GET", "/api/config?path=benchmark.py")
        assert code == 404
    finally:
        if os.path.isfile(tmp):
            os.remove(tmp)


def test_run_selector_validation():
    code, d = _req("POST", "/api/run", {"selector": {"type": "file", "path": "x"}})
    assert code == 400 and "legacy" in d["error"]
    code, _ = _req("POST", "/api/run", {"selector": {}})
    assert code == 400
    code, _ = _req("POST", "/api/run", {"selector": {"dirs": ["no/such/dir"]}})
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
    code, d = _req("POST", "/api/run", {
        "selector": {"files": ["mock/smoke/put.json"]},
        "flags": {"cluster": "mock/cluster.json", "dry_run": True}})
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
