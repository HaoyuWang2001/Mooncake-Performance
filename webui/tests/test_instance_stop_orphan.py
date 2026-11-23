#!/usr/bin/env python3
"""实例停止回退：webui 重启后 active 丢失、持久化 run 仍 running 时，
stop 必须走 stop_orphan（写 stop 文件触发 benchmark hold 循环 teardown），
而不是报「实例未在运行」。
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from webui.backend import runmgr
from webui.backend.common import ApiError
from webui.backend.routers import instances as router


class _FakeRun(object):
    id = "rORPH"

    def __init__(self):
        self.stopping = True

    def to_dict(self):
        return {"id": self.id, "status": "stopped", "stopping": True}


def test_stop_falls_back_to_orphan(monkeypatch):
    calls = {}

    class _FakeMgr(object):
        active = {}

        def stop_orphan(self, rid):
            calls["rid"] = rid
            return _FakeRun(), None

    class _FakeInstances(object):
        @staticmethod
        def load(iid):
            return {"id": iid, "name": "x"}

        @staticmethod
        def latest_run(iid):
            return {"id": "rORPH", "status": "running"}

    monkeypatch.setattr(router, "instances", _FakeInstances)
    monkeypatch.setattr(runmgr, "MGR", _FakeMgr())
    out = router.api_instance_stop(None, {}, "i1")
    assert out["ok"] is True
    assert out["run"]["id"] == "rORPH"
    assert calls["rid"] == "rORPH"


def test_stop_rejects_when_latest_not_running(monkeypatch):
    class _FakeMgr(object):
        active = {}

    class _FakeInstances(object):
        @staticmethod
        def load(iid):
            return {"id": iid}

        @staticmethod
        def latest_run(iid):
            return {"id": "rX", "status": "done"}

    monkeypatch.setattr(router, "instances", _FakeInstances)
    monkeypatch.setattr(runmgr, "MGR", _FakeMgr())
    with pytest.raises(ApiError) as exc:
        router.api_instance_stop(None, {}, "i1")
    assert exc.value.code == 409
