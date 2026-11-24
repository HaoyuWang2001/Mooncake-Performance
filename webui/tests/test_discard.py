#!/usr/bin/env python3
"""删除语义：Windows 原生送系统回收站；WSL/Linux 永久删除；失败不降级。"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from webui.backend import discard


def test_discard_posix_is_permanent(tmp_path, monkeypatch):
    monkeypatch.setattr(discard, "IS_WIN", False)
    d = tmp_path / "run"
    d.mkdir()
    (d / "f.log").write_text("x", encoding="utf-8")
    assert discard.discard_path(str(d)) == "deleted"
    assert not d.exists()
    f = tmp_path / "a.log"
    f.write_text("x", encoding="utf-8")
    assert discard.discard_path(str(f)) == "deleted"
    assert not f.exists()


def test_discard_windows_uses_recycle_bin(tmp_path, monkeypatch):
    monkeypatch.setattr(discard, "IS_WIN", True)
    d = tmp_path / "run"
    d.mkdir()
    calls = []
    monkeypatch.setattr(discard, "recycle_path_win",
                        lambda p: calls.append(p))
    assert discard.discard_path(str(d)) == "recycle-bin"
    assert calls == [str(d)]
    assert d.exists()          # 回收站语义：不原地 rmtree


def test_discard_windows_failure_does_not_degrade(tmp_path, monkeypatch):
    monkeypatch.setattr(discard, "IS_WIN", True)
    d = tmp_path / "run"
    d.mkdir()

    def boom(path):
        raise RuntimeError("recycle failed")

    monkeypatch.setattr(discard, "recycle_path_win", boom)
    with pytest.raises(RuntimeError):
        discard.discard_path(str(d))
    assert d.exists()          # 失败中止，绝不静默改永久删除


def test_discard_missing_path_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        discard.discard_path(str(tmp_path / "nope"))
