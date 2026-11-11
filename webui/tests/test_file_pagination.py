"""Backend tests for the WebUI text-file pager (`_read_text_page`).

The pager must reconstruct the WHOLE file across pages, with byte offsets that
advance correctly even when a UTF-8 character straddles the chunk boundary.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from webui.backend.routers import outputs as om


def _page_all(path, chunk):
    """Walk the pager exactly like the frontend: read pages until eof."""
    old = om.MAX_TEXT_CHUNK
    om.MAX_TEXT_CHUNK = chunk
    try:
        text, offset, size, pages = "", 0, None, 0
        while True:
            r = om._read_text_page(path, offset)
            text += r["content"]
            offset = r["offset"]
            size = r["size"]
            pages += 1
            assert pages < 100000, "offset did not advance"
            if r["eof"]:
                break
            assert offset > 0
        return text, offset, size, pages
    finally:
        om.MAX_TEXT_CHUNK = old


def test_ascii_full_reconstruction(tmp_path):
    p = tmp_path / "a.log"
    payload = "".join("RESULT\t{}\tpipeline_write\tk{}\t1.0\tOK\t1\n".format(
        i, i) for i in range(500)).encode()
    p.write_bytes(payload)
    text, offset, size, pages = _page_all(str(p), 1024)
    assert text == payload.decode()
    assert offset == size == len(payload)
    assert pages > 1, "payload must span multiple pages for this test"


def test_page_flags(tmp_path):
    p = tmp_path / "b.txt"
    p.write_bytes(b"x" * 2500)
    old = om.MAX_TEXT_CHUNK
    om.MAX_TEXT_CHUNK = 1000
    try:
        r0 = om._read_text_page(str(p), 0)
        # page consumes at least the chunk; trailing seam bytes may be included
        assert r0["offset"] >= 1000 and not r0["eof"]
        assert r0["size"] == 2500
        r_end = om._read_text_page(str(p), 2500)
        assert r_end["eof"] and r_end["offset"] == 2500 and r_end["content"] == ""
    finally:
        om.MAX_TEXT_CHUNK = old


def test_utf8_boundary_never_corrupts(tmp_path):
    # 3-byte CJK chars with odd chunk sizes (down to 2B) force splits mid-char
    payload = ("基准测试日志" * 500).encode("utf-8")
    p = tmp_path / "c.log"
    p.write_bytes(payload)
    for chunk in (2, 3, 5, 7, 11, 16):
        text, offset, size, _ = _page_all(str(p), chunk)
        assert text == payload.decode("utf-8"), "chunk={}".format(chunk)
        assert offset == size
        assert "\ufffd" not in text, "chunk={} produced replacement chars".format(chunk)


def test_page_starts_at_char_boundary(tmp_path):
    """Every page after the first starts exactly at a valid char boundary."""
    raw = "基准测试".encode("utf-8")          # 4 chars x 3 bytes
    p = tmp_path / "d.log"
    p.write_bytes(raw)
    old = om.MAX_TEXT_CHUNK
    om.MAX_TEXT_CHUNK = 4                     # splits through the 2nd char
    try:
        offset, collected = 0, ""
        while True:
            r = om._read_text_page(str(p), offset)
            collected += r["content"]
            offset = r["offset"]
            if r["eof"]:
                break
        assert collected == raw.decode("utf-8")
    finally:
        om.MAX_TEXT_CHUNK = old
