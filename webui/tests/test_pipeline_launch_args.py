"""Pipeline ssh launch commands: flag/value pairing (reader + writer).

The launch strings bind argparse options to values positionally via
``str.format``. A pre-merge revision of benchmark.py changed
``--timeout 300`` into ``--timeout {}`` but appended ``_client_timeout``
AFTER ``reader_op`` in the argument list, so the runtime command became

    --timeout get_buffer --operation 2413 --cpu-bind 0-3

Every reader then died at argparse startup (rc=2, "invalid int value"), never
registered in etcd, and each writer waited out its rendezvous (``0/5
readers``). These tests build the commands through the real builders and
assert every flag receives the value of the matching argument.
"""
import os
import shlex
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import benchmark as bm  # noqa: E402


def _argv(cmd):
    return shlex.split(cmd)


def _flag_values(argv, flag):
    return [argv[i + 1] for i, a in enumerate(argv) if a == flag]


def test_reader_launch_pairs_timeout_and_operation():
    cmd = bm._pipeline_reader_launch_cmd(
        "numactl --physcpubind=0-3 ",          # launch_prefix (trailing space)
        "/remote/scripts/run_benchmark_pipeline.sh",
        "/remote/logs/cfg.reader.log",
        7,                                     # client_id
        4,                                     # reader_threads
        9201,                                  # listen port
        2413,                                  # client lifetime (int)
        "get_buffer",                          # reader operation
        "--cpu-bind 0-3",
        "/remote/logs/cfg.reader.nohup")
    argv = _argv(cmd)
    assert _flag_values(argv, "--role") == ["reader"]
    assert _flag_values(argv, "--client-id") == ["7"]
    assert _flag_values(argv, "--num-threads") == ["4"]
    assert _flag_values(argv, "--listen-port") == ["9201"]
    assert _flag_values(argv, "--warmup") == ["false"]
    # the regression: timeout must be the numeric lifetime, never the op name
    timeout = _flag_values(argv, "--timeout")
    assert timeout == ["2413"]
    assert int(timeout[0]) == 2413             # client argparse int() succeeds
    assert "get_buffer" not in timeout
    # and operation must be the op name, never the timeout
    operation = _flag_values(argv, "--operation")
    assert operation and operation[0] == "get_buffer"
    assert not operation[0].isdigit()
    # shell plumbing still intact
    assert "2>&1" in argv and argv[-3:] == ["&", "echo", "$!"]
    assert "numactl" in argv and "pipeline" in argv


def test_reader_launch_without_numa_binding():
    """Empty numa_arg must not shift any later value (double space only)."""
    cmd = bm._pipeline_reader_launch_cmd(
        "", "/r/run.sh", "/r/a.log", 0, 8, 9301, 600, "get", "", "/r/a.nohup")
    argv = _argv(cmd)
    assert _flag_values(argv, "--timeout") == ["600"]
    assert _flag_values(argv, "--operation")[0] == "get"
    assert _flag_values(argv, "--listen-port") == ["9301"]


def test_writer_launch_pairs_warmup_and_operation():
    cmd = bm._pipeline_writer_launch_cmd(
        "", "/remote/scripts/run_benchmark_pipeline.sh",
        "/remote/logs/cfg.writer.log",
        3,                                     # client id
        9103,                                  # listen port
        "true",                                # warmup
        "upsert_from",                         # writer operation
        " --key-count 256",                    # kc_arg (leading space, as built)
        "--cpu-bind 4-7",
        "/remote/logs/cfg.writer.nohup")
    argv = _argv(cmd)
    assert _flag_values(argv, "--role") == ["writer"]
    assert _flag_values(argv, "--client-id") == ["3"]
    assert _flag_values(argv, "--listen-port") == ["9103"]
    assert _flag_values(argv, "--warmup") == ["true"]
    assert _flag_values(argv, "--operation")[0] == "upsert_from"
    assert _flag_values(argv, "--key-count") == ["256"]
    assert _flag_values(argv, "--cpu-bind") == ["4-7"]
    assert argv[-3:] == ["&", "echo", "$!"]
