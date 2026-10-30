#!/usr/bin/env python3
"""system router：/api/status、/api/help。"""
import os
import sys

from .. import runmgr
from ..common import ApiError
from ..paths import API_MD, ROOT

ROUTES = []


def api_status(handler, query):
    return {"version": "1.0", "platform": runmgr.IS_WIN and "windows" or "posix",
            "python": sys.version.split()[0], "root": ROOT,
            "paths": {"outputs": "outputs", "runs": "webui/data/runs"},
            **runmgr.MGR.status()}


def api_help(handler, query):
    try:
        with open(API_MD, "r", encoding="utf-8") as f:
            return {"markdown": f.read()}
    except OSError:
        return {"markdown": "API.md not found"}


ROUTES += [
    ("GET", r"^/api/status$", lambda h, q, m=None: api_status(h, q)),
    ("GET", r"^/api/help$", lambda h, q, m=None: api_help(h, q)),
]
