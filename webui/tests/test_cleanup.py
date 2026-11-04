#!/usr/bin/env python3
"""Standalone clean-up API argv / CLI validation."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

import benchmark as bm
from webui.backend.common import ApiError
from webui.backend.routers.runs import _build_cleanup_argv

SSH_CLUSTER = "configs/_ssh/cluster.example.json"
SSH_GLOBAL = "configs/_ssh/global.example.json"
K8S_CLUSTER = "configs/_k8s/cluster.example.json"
K8S_GLOBAL = "configs/_k8s/global.example.json"


def test_build_cleanup_argv_ssh():
    argv = _build_cleanup_argv({"flags": {
        "mode": "ssh", "cluster": SSH_CLUSTER, "global": SSH_GLOBAL}})
    assert argv[-1] == "--clean-up"
    assert "--ssh" in argv and "--k8s" not in argv
    assert "--profile" not in argv
    assert SSH_CLUSTER in argv and SSH_GLOBAL in argv


def test_build_cleanup_argv_k8s():
    argv = _build_cleanup_argv({"flags": {
        "mode": "k8s", "cluster": K8S_CLUSTER, "global": K8S_GLOBAL}})
    assert argv[-1] == "--clean-up"
    assert "--k8s" in argv and "--ssh" not in argv
    assert "--profile" not in argv


def test_build_cleanup_missing_cluster():
    try:
        _build_cleanup_argv({"flags": {
            "mode": "ssh", "cluster": "no/such/cluster.json",
            "global": SSH_GLOBAL}})
    except ApiError as exc:
        assert exc.code == 400 and "copy" in exc.message
    else:
        raise AssertionError("missing cluster must be rejected")


def test_parse_standalone_cleanup_without_profile():
    args, cfg_paths = bm.parse_arguments([
        "--ssh", "--cluster", SSH_CLUSTER, "--global", SSH_GLOBAL,
        "--clean-up"])
    assert cfg_paths == []
    assert args.clean_up is True
    assert args.profile is None
