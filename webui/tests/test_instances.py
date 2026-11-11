#!/usr/bin/env python3
"""instances：实例持久化 + instances 路由（创建/运行/停止/改名/删除）。

运行用 sleep 假进程模拟；run-argv 构造器被替换以避免依赖真实配置。
"""
import io
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from webui.backend import instances, runmgr
from webui.backend.routers import instances as ir


class _H(object):
    def __init__(self, body=None):
        data = json.dumps(body or {}).encode()
        self.headers = {"Content-Length": str(len(data))}
        self.rfile = io.BytesIO(data)


def _ctx(tmp_path, monkeypatch):
    inst_dir = tmp_path / "instances"
    trash = tmp_path / "trash"
    runs = tmp_path / "runs"
    inst_dir.mkdir()
    runs.mkdir()
    monkeypatch.setattr(instances, "INSTANCES_DIR", str(inst_dir))
    monkeypatch.setattr(instances, "INSTANCES_TRASH_DIR", str(trash))
    monkeypatch.setattr(instances, "RUNS_DIR", str(runs))
    return inst_dir, runs


def _sleep_argv(sec=30):
    return [sys.executable, "-c", "import time; time.sleep({})".format(sec)]


def _wait(run, want, tries=60):
    for _ in range(tries):
        if run.status == want:
            return
        time.sleep(0.2)
    raise AssertionError("status {} != {}".format(run.status, want))


def _fake_run_argv(monkeypatch, sec=30):
    monkeypatch.setattr(ir, "_build_run_argv", lambda body: _sleep_argv(sec))


def _req(flags=None):
    return {"selector": {"dirs": [], "files": ["configs/basic/get.json"]},
            "flags": flags or {"mode": "ssh", "cluster": "c/a.json",
                               "global": "g/a.json", "profile": "p.json"}}


def test_store_crud(tmp_path, monkeypatch):
    _ctx(tmp_path, monkeypatch)
    a = instances.create(_req({"n": 1}), "A")
    assert a["id"] and a["name"] == "A"
    assert instances.load(a["id"])["name"] == "A"
    assert len(instances.list_all()) == 1
    r = instances.rename(a["id"], "  A2  ")
    assert r["name"] == "A2"
    assert instances.delete(a["id"]) is True
    assert instances.load(a["id"]) is None
    assert os.path.isdir(str(tmp_path / "trash"))
    assert os.listdir(str(tmp_path / "trash"))
    # bad ids never touch the filesystem
    assert instances.load("../../etc/passwd") is None
    assert instances.delete("bad id") is False


def test_create_validates_and_runs_lifecycle(tmp_path, monkeypatch):
    _ctx(tmp_path, monkeypatch)
    _fake_run_argv(monkeypatch)
    code = ir.api_instance_create(_H(_req()), {})
    inst = code["instance"]
    assert inst["status"] == "created" and inst["name"] == inst["id"]

    # run -> running summary
    out = ir.api_instance_run(_H(), {}, inst["id"])
    rid = out["run"]["id"]
    time.sleep(0.3)
    listed = ir.api_instances(_H(), {})["instances"]
    assert listed[0]["status"] in ("running", "stopping")
    assert ir._active_run(inst["id"]) is not None

    # same instance cannot run twice
    try:
        ir.api_instance_run(_H(), {}, inst["id"])
        raise AssertionError("running instance must not start twice")
    except Exception as exc:
        assert "正在运行" in str(exc)

    # delete refused while running
    try:
        ir.api_instance_delete(_H(), {}, inst["id"])
        raise AssertionError("delete must be refused while running")
    except Exception as exc:
        assert "正在运行" in str(exc)

    runmgr.MGR.stop_run(rid)
    for _ in range(60):
        if ir._active_run(inst["id"]) is None:
            break
        time.sleep(0.2)
    ir.api_instance_delete(_H(), {}, inst["id"])
    assert instances.load(inst["id"]) is None


def test_conflict_between_instances(tmp_path, monkeypatch):
    _ctx(tmp_path, monkeypatch)
    _fake_run_argv(monkeypatch)
    same = _req()
    i1 = ir.api_instance_create(_H(same), {})["instance"]
    i2 = ir.api_instance_create(_H(same), {})["instance"]
    r1 = ir.api_instance_run(_H(), {}, i1["id"])["run"]
    try:
        try:
            ir.api_instance_run(_H(), {}, i2["id"])
            raise AssertionError("shared cluster must be rejected")
        except Exception as exc:
            assert "shared resource" in str(exc)
    finally:
        runmgr.MGR.stop_run(r1["id"])
        _wait(runmgr.MGR.get_run(r1["id"]), "stopped")
    # different cluster/global paths -> allowed in parallel
    other = _req()
    other["flags"].update({"cluster": "c/b.json", "global": "g/b.json"})
    i3 = ir.api_instance_create(_H(other), {})["instance"]
    r1b = ir.api_instance_run(_H(), {}, i1["id"])["run"]
    r3 = ir.api_instance_run(_H(), {}, i3["id"])["run"]
    try:
        assert len(runmgr.MGR.active) == 2
    finally:
        for rid in (r1b["id"], r3["id"]):
            runmgr.MGR.stop_run(rid)
        _wait(runmgr.MGR.get_run(r1b["id"]), "stopped")
        _wait(runmgr.MGR.get_run(r3["id"]), "stopped")


def test_rename_and_detail_include_runs(tmp_path, monkeypatch):
    _ctx(tmp_path, monkeypatch)
    _fake_run_argv(monkeypatch)
    iid = ir.api_instance_create(_H(_req()), {})["instance"]["id"]
    out = ir.api_instance_rename(_H({"name": "rank-8MB"}), {}, iid)
    assert out["instance"]["name"] == "rank-8MB"
    # a persisted run record of this instance shows up in detail
    runs_dir = str(tmp_path / "runs")
    with open(os.path.join(runs_dir, "r20260101-0000-001.json"), "w",
              encoding="utf-8") as f:
        json.dump({"id": "r20260101-0000-001", "status": "done", "rc": 0,
                   "t0": 1, "t1": 2, "elapsed": 1.0, "ts_dir": "outputs/x",
                   "meta": {"instance": iid}}, f)
    d = ir.api_instance_get(_H(), {}, iid)
    assert d["instance"]["name"] == "rank-8MB"
    assert d["instance"]["status"] == "done"
    assert d["runs"] and d["runs"][0]["id"] == "r20260101-0000-001"


def test_missing_instance_404(tmp_path, monkeypatch):
    _ctx(tmp_path, monkeypatch)
    for fn in (ir.api_instance_get, ir.api_instance_run,
               ir.api_instance_stop, ir.api_instance_delete):
        try:
            fn(_H(), {}, "i20260101-0000-999")
            raise AssertionError("missing instance must 404")
        except Exception as exc:
            assert "no such instance" in str(exc)
