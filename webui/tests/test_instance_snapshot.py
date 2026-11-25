#!/usr/bin/env python3
"""实例配置快照：创建瞬间冻结全部配置（含 k8s 模板），运行只用快照。

覆盖：快照内容/清单/快照 request、k8s 模板冻结与 cluster 改写、运行 argv/env
全部指向快照、删除实例清理快照、旧实例回退、_configs.json 继承根、rank+recall
分组、以及每次 run 的审计快照复制。
"""
import io
import json
import os
import shutil
import sys
import time

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

import benchmark as bm
from webui.backend import inst_snapshot, instances
from webui.backend.paths import ROOT
from webui.backend.routers import instances as ir


def _abs(ref):
    return ref if os.path.isabs(ref) else os.path.join(ROOT, ref)


def _h(body):
    data = json.dumps(body).encode()
    h = io.BytesIO(data)
    h.headers = {"Content-Length": str(len(data))}
    h.rfile = io.BytesIO(data)
    return h


REQ = {"selector": {"files": ["configs/basic/get.json"]},
       "flags": {"mode": "ssh", "launch_mode": "single",
                 "cluster": "configs/cluster.json",
                 "global": "configs/global.json",
                 "profile": "configs/_ssh/profile.json"}}


@pytest.fixture(autouse=True)
def _isolate(tmp_path, monkeypatch):
    idir = tmp_path / "instances"
    idir.mkdir()
    rdir = tmp_path / "runs"
    rdir.mkdir()
    monkeypatch.setattr(inst_snapshot, "INSTANCES_DIR", str(idir))
    monkeypatch.setattr(instances, "INSTANCES_DIR", str(idir))
    monkeypatch.setattr(instances, "RUNS_DIR", str(rdir))
    yield idir


def test_snapshot_freezes_deployment_scenario_and_fragments(tmp_path, monkeypatch):
    iid = instances.new_id()
    snap = inst_snapshot.build(iid, "snap-a", REQ)
    root = os.path.join(str(tmp_path / "instances"), iid)
    assert os.path.isdir(root)
    manifest = json.load(open(os.path.join(root, "snapshot.json"),
                              encoding="utf-8"))
    files = [f["path"] for f in manifest["files"]]
    for need in ("configs/basic/get.json", "configs/basic/_configs.json",
                 "configs/cluster.json", "configs/global.json",
                 "configs/_ssh/profile.json"):
        assert need in files, "missing {}".format(need)
    # 快照 request 全部指向快照内文件
    assert snap["request"]["selector"]["files"][0].startswith(
        snap["dir"] + "/files/configs/")
    assert snap["request"]["flags"]["cluster"].startswith(
        snap["dir"] + "/files/configs/")
    assert snap["configs_root"].endswith("/files/configs")
    # 内容逐字节一致
    a = open(os.path.join(root, "files/configs/basic/get.json")).read()
    b = open(os.path.join(ROOT, "configs/basic/get.json")).read()
    assert a == b


def test_k8s_templates_frozen_and_cluster_rewritten(tmp_path):
    req = {"selector": {"files": ["configs/basic/get.json"]},
           "flags": {"mode": "k8s", "launch_mode": "single",
                     "cluster": "configs/_k8s/cluster.json",
                     "global": "configs/_k8s/global.json",
                     "profile": "configs/_k8s/profile.example.json"}}
    iid = instances.new_id()
    snap = inst_snapshot.build(iid, "snap-k8s", req)
    root = os.path.join(str(tmp_path / "instances"), iid)
    cluster = json.load(open(os.path.join(
        root, "files/configs/_k8s/cluster.json"), encoding="utf-8"))
    for key in ("template", "deployment_template", "monitor_template"):
        p = cluster["k8s"][key]
        assert iid in p and os.path.isfile(_abs(p)), key
    manifest = json.load(open(os.path.join(root, "snapshot.json"),
                              encoding="utf-8"))
    assert len(manifest["templates"]) == 3


def test_run_uses_only_snapshot_and_is_immune_to_live_edits(tmp_path, monkeypatch):
    src = os.path.join(ROOT, "configs/basic/get.json")
    live_rel = "configs/basic/zz-snap-test.json"
    live = os.path.join(ROOT, live_rel)
    shutil.copy(src, live)
    try:
        body = {"name": "snap-run",
                "selector": {"files": [live_rel]},
                "flags": dict(REQ["flags"])}
        created = ir.api_instance_create(_h(body), {})["instance"]
        iid = created["id"]
        assert created["snapshot"]["files"] > 0
        snap_file = os.path.join(str(tmp_path / "instances"), iid,
                                 "files", live_rel)
        frozen = open(snap_file, encoding="utf-8").read()
        # 修改实时配置（旧实例不应受影响）
        with open(live, "w", encoding="utf-8") as f:
            f.write(json.dumps({"scenario": "basic", "duration": 999,
                                "data_size": ["64KB"],
                                "params": [{"num_clients": 1, "num_threads": 1}]}))
        captured = {}

        class _FakeRun(object):
            def to_dict(self):
                return {"id": "rSNAP", "status": "running"}

        class _FakeMgr(object):
            active = {}
            scheduler = None

            def start_benchmark(self, argv, meta=None, identities=None,
                                env_extra=None):
                captured.update(argv=argv, meta=meta, env=env_extra)
                return _FakeRun()

        monkeypatch.setattr(ir.runmgr, "MGR", _FakeMgr())
        out = ir.api_instance_run(None, {}, iid)
        assert out["run"]["id"] == "rSNAP"
        argv = captured["argv"]
        # 所有配置路径都在实例快照目录内（无实时 configs/basic/zz-snap-test.json）
        assert any(iid in a and "zz-snap-test.json" in a for a in argv)
        assert not any(a == live_rel for a in argv)
        env = captured["env"]
        assert env["MC_CONFIGS_INHERIT_ROOT"].endswith("/files/configs")
        assert env["MC_INSTANCE_SNAPSHOT"].endswith(iid)
        # 快照内容保持创建时版本
        assert open(snap_file, encoding="utf-8").read() == frozen
        # 清理
        ir.api_instance_delete(None, {}, iid)
    finally:
        if os.path.isfile(live):
            os.remove(live)


def test_delete_instance_removes_snapshot_dir(tmp_path):
    iid = instances.new_id()
    snap = inst_snapshot.build(iid, "snap-del", REQ)
    instances.create(REQ, "snap-del", iid=iid, snapshot=snap)
    root = os.path.join(str(tmp_path / "instances"), iid)
    assert os.path.isdir(root)
    assert instances.delete(iid) is True
    assert not os.path.isdir(root)
    assert instances.load(iid) is None


def test_legacy_instance_without_snapshot_runs_live(tmp_path, monkeypatch):
    inst = instances.create(REQ, "legacy")
    captured = {}

    class _FakeRun(object):
        def to_dict(self):
            return {"id": "rLIVE", "status": "running"}

    class _FakeMgr(object):
        active = {}
        scheduler = None

        def start_benchmark(self, argv, meta=None, identities=None,
                            env_extra=None):
            captured.update(argv=argv, env=env_extra)
            return _FakeRun()

    monkeypatch.setattr(ir.runmgr, "MGR", _FakeMgr())
    ir.api_instance_run(None, {}, inst["id"])
    assert "configs/basic/get.json" in captured["argv"]
    assert not captured["env"]


def test_load_scenario_config_uses_snapshot_root(tmp_path):
    live = tmp_path / "live/configs"
    snap = tmp_path / "snap/configs"
    for base in (live, snap):
        (base / "basic").mkdir(parents=True)
        (base / "basic/_configs.json").write_text(
            json.dumps({"scenario": "basic", "protocol": "tcp"}),
            encoding="utf-8")
        (base / "basic/a.json").write_text(
            json.dumps({"duration": 5}), encoding="utf-8")
    # 实时根被改；快照根不受影响
    (live / "basic/_configs.json").write_text(
        json.dumps({"scenario": "basic", "protocol": "rdma"}),
        encoding="utf-8")
    live_cfg = bm.load_scenario_config(str(live / "basic/a.json"),
                                       configs_root=str(live))
    snap_cfg = bm.load_scenario_config(str(snap / "basic/a.json"),
                                       configs_root=str(snap))
    assert live_cfg["protocol"] == "rdma"
    assert snap_cfg["protocol"] == "tcp"
    assert snap_cfg["duration"] == 5


def test_partition_configs_with_snapshot_root(tmp_path):
    from scripts.run_rank_recall import partition_configs
    root = tmp_path / "configs"
    (root / "pipeline/rank").mkdir(parents=True)
    (root / "pipeline/recall").mkdir()
    (root / "pipeline/_configs.json").write_text(
        json.dumps({"scenario": "pipeline"}), encoding="utf-8")
    (root / "pipeline/rank/_configs.json").write_text(
        json.dumps({"work_mode": "rank"}), encoding="utf-8")
    (root / "pipeline/recall/_configs.json").write_text(
        json.dumps({"work_mode": "recall"}), encoding="utf-8")
    (root / "pipeline/rank/r.json").write_text("{}", encoding="utf-8")
    (root / "pipeline/recall/c.json").write_text("{}", encoding="utf-8")
    groups = partition_configs([str(root / "pipeline")], [],
                               configs_root=str(root))
    assert len(groups["rank"]) == 1 and len(groups["recall"]) == 1


def test_audit_snapshot_copy(tmp_path, monkeypatch):
    src = tmp_path / "inst"
    (src / "files/configs/basic").mkdir(parents=True)
    (src / "snapshot.json").write_text("{}", encoding="utf-8")
    (src / "instance.json").write_text("{}", encoding="utf-8")
    ts = tmp_path / "ts"
    ts.mkdir()
    monkeypatch.setenv("MC_INSTANCE_SNAPSHOT", str(src))
    bm._copy_audit_snapshot(str(ts))
    assert (ts / "snapshot/snapshot.json").is_file()
    assert (ts / "snapshot/files/configs/basic").is_dir()
    # 幂等：再次调用不覆盖/不报错
    bm._copy_audit_snapshot(str(ts))
