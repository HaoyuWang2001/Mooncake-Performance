#!/usr/bin/env python3
"""webui/backend/common.py — 各 router 共用的错误类型与请求/文件工具。"""
import json
import os
import shutil
import time

from .paths import BACKUP_DIR, ROOT


class ApiError(Exception):
    """带 HTTP 状态码的业务错误（dispatch 统一转 JSON 响应）。"""

    def __init__(self, code, message):
        Exception.__init__(self, message)
        self.code = code
        self.message = message


def real_under(path, roots):
    """realpath 是否落在任一白名单根内（防目录穿越）。"""
    rp = os.path.realpath(path)
    for r in roots:
        rr = os.path.realpath(r)
        if rp == rr or rp.startswith(rr + os.sep):
            return True
    return False


def jpath(query, key, required=True):
    val = query.get(key, [None])[0]
    if not val and required:
        raise ApiError(400, "missing query parameter: {}".format(key))
    return val


def read_json_body(handler):
    try:
        n = int(handler.headers.get("Content-Length", 0))
        raw = handler.rfile.read(n) if n else b""
        return json.loads(raw.decode("utf-8")) if raw else {}
    except (ValueError, UnicodeDecodeError) as exc:
        raise ApiError(400, "bad JSON body: {}".format(exc))


def backup_file(path):
    """写入前备份到 webui/backups/（文件名 = 相对路径斜杠转下划线 + 时间戳）。"""
    rel = os.path.relpath(path, ROOT).replace("\\", "/").replace("/", "__")
    dst = os.path.join(BACKUP_DIR, rel + "." + time.strftime("%Y%m%d-%H%M%S"))
    os.makedirs(BACKUP_DIR, exist_ok=True)
    shutil.copy2(path, dst)
    return os.path.relpath(dst, ROOT)


def atomic_write(path, content):
    tmp = path + ".webui-tmp"
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write(content)
    os.replace(tmp, path)
