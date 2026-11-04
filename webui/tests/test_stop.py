#!/usr/bin/env python3
"""Cooperative stop primitives."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

import utils


def test_stop_event_and_file(tmp_path):
    utils.clear_stop()
    path = tmp_path / "run.stop"
    utils.set_stop_file(str(path))
    assert not utils.stop_requested()
    path.write_text("stop\n", encoding="utf-8")
    assert utils.stop_requested()
    utils.clear_stop()
    assert not utils.stop_requested()
    utils.request_stop()
    assert utils.stop_requested()
    utils.clear_stop()
    assert not utils.stop_requested()


def test_stop_requested_is_not_swallowed_by_except_exception():
    assert issubclass(utils.StopRequested, BaseException)
    assert not issubclass(utils.StopRequested, Exception)
