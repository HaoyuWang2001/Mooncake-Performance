#!/usr/bin/env python3
"""Verbose logging infrastructure and argv builder."""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

import utils
from webui.backend.routers.runs import _build_run_argv, _build_cleanup_argv


def test_log_has_timestamp(capsys):
    utils.log("hello timestamp")
    out = capsys.readouterr().out
    assert re.match(r"^\[\d{2}:\d{2}:\d{2}\] \[benchmark\] hello timestamp", out)


def test_log_detail_off_by_default(capsys):
    utils.set_verbose(False)
    utils.log_detail("hidden")
    assert capsys.readouterr().out == ""


def test_log_detail_when_verbose(capsys):
    utils.set_verbose(True)
    try:
        utils.log_detail("shown")
        out = capsys.readouterr().out
        assert "[benchmark] shown" in out
    finally:
        utils.set_verbose(False)


def test_controller_desc():
    assert utils.controller_desc({"_local": True}) == "local"
    assert utils.controller_desc({"_local": False, "ip": "1.2.3.4",
                                  "ssh_user": "root"}) == "root@1.2.3.4"


_CFG = {"cluster": "configs/_ssh/cluster.example.json",
         "global": "configs/_ssh/global.example.json",
         "profile": "configs/_ssh/profile.example.json"}


def test_run_argv_verbose():
    argv = _build_run_argv({
        "selector": {"files": ["configs/basic/put.json"]},
        "flags": dict(_CFG, verbose=True)})
    assert "--verbose" in argv


def test_run_argv_no_verbose():
    argv = _build_run_argv({
        "selector": {"files": ["configs/basic/put.json"]},
        "flags": dict(_CFG)})
    assert "--verbose" not in argv


def test_cleanup_argv_verbose():
    argv = _build_cleanup_argv({
        "flags": dict(_CFG, mode="ssh", verbose=True)})
    assert "--verbose" in argv and "--clean-up" in argv


if __name__ == "__main__":
    test_log_has_timestamp.__wrapped__ if hasattr(test_log_has_timestamp, "__wrapped__") else None
    # simple runner for non-pytest use
    import io, contextlib
    utils.set_verbose(False)
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print("PASS", name)
            except Exception as exc:
                print("FAIL", name, exc)
