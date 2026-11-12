"""Full-log pull integrity: size verification + retry semantics.

The incident this guards against: an SFTP channel killed mid-transfer left a
half-written local log; an existence-only check accepted it, so the "pulled"
log was silently truncated.
"""
import os
import sys
import types

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

import benchmark as bm
import utils as u


class _FakeSFTP:
    def __init__(self, mode, remote_size, src):
        self.mode = mode          # "full" | "partial" | "raise"
        self.remote_size = remote_size
        self.src = src
        self.closed = False

    def get(self, remote, local):
        if self.mode == "raise":
            with open(local, "wb") as f:
                f.write(b"half")
            raise IOError("channel closed mid-transfer")
        data = self.src if self.mode == "full" else self.src[:3]
        with open(local, "wb") as f:
            f.write(data)

    def stat(self, remote):
        return types.SimpleNamespace(st_size=self.remote_size)

    def close(self):
        self.closed = True


def _install(monkeypatch, sftp, listing):
    monkeypatch.setattr(u, "is_k8s_node", lambda node: False)
    monkeypatch.setattr(u, "ssh_check_output",
                        lambda host, node, cmd, quiet=False, timeout=None: listing)
    monkeypatch.setattr(u, "_sftp_client",
                        lambda node, timeout=None: sftp)


def test_scp_pull_full_file_ok(tmp_path, monkeypatch):
    payload = b"0123456789"
    _install(monkeypatch, _FakeSFTP("full", len(payload), payload),
             "/remote/x.log")
    u.scp_pull("1.2.3.4", {"ip": "1.2.3.4"}, "/remote/x.log",
               str(tmp_path), dry_run=False)
    p = tmp_path / "x.log"
    assert p.read_bytes() == payload
    assert p.stat().st_size == len(payload)


def test_scp_pull_partial_file_rejected_and_removed(tmp_path, monkeypatch):
    payload = b"0123456789"
    _install(monkeypatch, _FakeSFTP("partial", len(payload), payload),
             "/remote/x.log")
    try:
        u.scp_pull("1.2.3.4", {"ip": "1.2.3.4"}, "/remote/x.log",
                   str(tmp_path), dry_run=False)
        raise AssertionError("should raise on size mismatch")
    except RuntimeError as exc:
        assert "size mismatch" in str(exc)
    assert not (tmp_path / "x.log").exists(), "partial local file must be gone"


def test_scp_pull_mid_transfer_exception_removes_partial(tmp_path, monkeypatch):
    _install(monkeypatch, _FakeSFTP("raise", 10, b""), "/remote/x.log")
    try:
        u.scp_pull("1.2.3.4", {"ip": "1.2.3.4"}, "/remote/x.log",
                   str(tmp_path), dry_run=False)
        raise AssertionError("should raise")
    except IOError:
        pass
    assert not (tmp_path / "x.log").exists(), "partial local file must be gone"


def test_pull_logs_retries_then_succeeds(tmp_path, monkeypatch):
    calls = []

    def fake_scp(host, node, glob, local, dry_run=False):
        calls.append(glob)
        if len(calls) == 1:
            raise RuntimeError("transient channel failure")

    monkeypatch.setattr(bm, "scp_pull", fake_scp)
    monkeypatch.setattr(bm, "get_node", lambda cluster, name: {"ip": "1.2.3.4"})
    monkeypatch.setattr(bm, "_ssh_drop", lambda node: None)
    monkeypatch.setattr(bm, "_wait_log_pull_retry", lambda *a, **k: None)
    bm.pull_logs({}, ["n1"], "/rlog", "cfg", str(tmp_path), False)
    assert len(calls) == 2


def test_pull_logs_fails_after_retries(tmp_path, monkeypatch):
    calls = []

    def fake_scp(host, node, glob, local, dry_run=False):
        calls.append(glob)
        raise RuntimeError("persistent failure")

    monkeypatch.setattr(bm, "scp_pull", fake_scp)
    monkeypatch.setattr(bm, "get_node", lambda cluster, name: {"ip": "1.2.3.4"})
    monkeypatch.setattr(bm, "_ssh_drop", lambda node: None)
    monkeypatch.setattr(bm, "_wait_log_pull_retry", lambda *a, **k: None)
    try:
        bm.pull_logs({}, ["n1"], "/rlog", "cfg", str(tmp_path), False)
        raise AssertionError("should raise after all retries")
    except RuntimeError as exc:
        assert "persistent failure" in str(exc)
    assert len(calls) == bm._LOG_PULL_ATTEMPTS


def test_pull_stage_logs_retries_but_never_raises(tmp_path, monkeypatch):
    calls = []

    def fake_scp(host, node, f, local, dry_run=False):
        calls.append(f)
        if len(calls) == 1:
            raise RuntimeError("transient")

    monkeypatch.setattr(bm, "scp_pull", fake_scp)
    monkeypatch.setattr(bm, "_ssh_drop", lambda node: None)
    monkeypatch.setattr(bm, "_wait_log_pull_retry", lambda *a, **k: None)
    # transient -> retried, no raise
    bm.pull_stage_logs("1.2.3.4", {"ip": "1.2.3.4"}, ["/r/x.log"],
                       str(tmp_path), False)
    assert len(calls) == 2
    # persistent -> still no raise (authoritative pull happens later)
    calls.clear()

    def always_fail(host, node, f, local, dry_run=False):
        calls.append(f)
        raise RuntimeError("persistent")

    monkeypatch.setattr(bm, "scp_pull", always_fail)
    bm.pull_stage_logs("1.2.3.4", {"ip": "1.2.3.4"}, ["/r/x.log"],
                       str(tmp_path), False)
    assert len(calls) == bm._LOG_PULL_ATTEMPTS
