#!/usr/bin/env python3
"""Benchmark client prints an environment snapshot at process start."""
import os
import subprocess
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
CLIENT = os.path.join(ROOT, "scripts", "mooncake_benchmark_client.py")
KEYS = ("POD_IP", "NODE_NAME", "LD_LIBRARY_PATH", "SETUP_FABRIC_IP",
        "MC_FABRIC_IP", "MC_TCP_BIND_ADDRESS", "SETUP_UB_DEVICE",
        "MC_URMA_TRANS_MODE", "SETUP_PROTOCOL")


def _run(extra_env):
    env = {k: v for k, v in os.environ.items() if k not in KEYS}
    env.update(extra_env)
    r = subprocess.run([sys.executable, CLIENT, "--help"],
                       capture_output=True, text=True, env=env, timeout=30)
    return r.stdout + r.stderr


def test_client_env_snapshot_values():
    out = _run({
        "POD_IP": "10.0.0.9", "NODE_NAME": "node-a",
        "LD_LIBRARY_PATH": "/tmp/mc-ld", "SETUP_FABRIC_IP": "10.0.0.9",
        "MC_FABRIC_IP": "10.0.0.9", "MC_TCP_BIND_ADDRESS": "10.0.0.9",
        "SETUP_UB_DEVICE": "bonding_dev_1", "MC_URMA_TRANS_MODE": "RM",
        "SETUP_PROTOCOL": "ub"})
    assert "[client-env]" in out
    for item in ("POD_IP=10.0.0.9", "NODE_NAME=node-a",
                 "LD_LIBRARY_PATH=/tmp/mc-ld",
                 "SETUP_FABRIC_IP=10.0.0.9", "MC_FABRIC_IP=10.0.0.9",
                 "MC_TCP_BIND_ADDRESS=10.0.0.9",
                 "SETUP_UB_DEVICE=bonding_dev_1",
                 "MC_URMA_TRANS_MODE=RM", "SETUP_PROTOCOL=ub"):
        assert item in out, item


def test_client_env_snapshot_unset():
    out = _run({})
    assert "[client-env]" in out
    for key in KEYS:
        assert "{}=".format(key) in out
    assert "LD_LIBRARY_PATH=<unset>" in out
    assert "POD_IP=<unset>" in out
