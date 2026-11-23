#!/usr/bin/env python3
"""discard — 删除语义（无隐藏回收站、无后台备份）。

- Windows 原生运行：送入「资源管理器回收站」（SHFileOperationW +
  FOF_ALLOWUNDO，零复制、可恢复、由用户自己清空）；
- WSL / Linux：永久删除（不留任何隐藏副本）；
- 回收站调用失败一律抛错中止，绝不静默降级为永久删除。
"""
import os
import shutil

IS_WIN = os.name == "nt"


def recycle_path_win(path):
    """Windows：把文件/目录送入系统回收站（可恢复）。失败抛 RuntimeError。"""
    import ctypes
    from ctypes import wintypes

    class SHFILEOPSTRUCTW(ctypes.Structure):
        _fields_ = [
            ("hwnd", wintypes.HWND),
            ("wFunc", wintypes.UINT),
            ("pFrom", wintypes.LPCWSTR),
            ("pTo", wintypes.LPCWSTR),
            ("fFlags", ctypes.c_uint16),
            ("fAnyOperationsAborted", wintypes.BOOL),
            ("hNameMappings", ctypes.c_void_p),
            ("lpszProgressTitle", wintypes.LPCWSTR),
        ]

    FO_DELETE = 3
    FOF_ALLOWUNDO = 0x40          # 进回收站（而非永久删除）
    FOF_NOCONFIRMATION = 0x10
    FOF_SILENT = 0x4
    FOF_NOERRORUI = 0x400

    # SHFileOperation 的 pFrom 需要双 NUL 结尾（create_unicode_buffer 自带一个）
    src = ctypes.create_unicode_buffer(os.path.abspath(path) + "\0")
    op = SHFILEOPSTRUCTW(
        None, FO_DELETE, ctypes.cast(src, wintypes.LPCWSTR), None,
        FOF_ALLOWUNDO | FOF_NOCONFIRMATION | FOF_SILENT | FOF_NOERRORUI,
        False, None, None)
    windll = getattr(ctypes, "windll", None)
    if windll is None:                      # 非 Windows 解释器
        raise RuntimeError("Windows Shell API 不可用")
    rc = windll.shell32.SHFileOperationW(ctypes.byref(op))
    if rc != 0 or op.fAnyOperationsAborted:
        raise RuntimeError(
            "Windows 回收站删除失败（SHFileOperationW rc={}）：{}".format(
                rc, path))


def discard_path(path):
    """删除 path（文件或目录）。

    返回 "recycle-bin"（Windows 原生）或 "deleted"（WSL/Linux 永久删除）。
    路径不存在 / 删除失败均抛错，调用方转成 API 错误。
    """
    if not path or not os.path.exists(path):
        raise FileNotFoundError(path)
    if IS_WIN:
        recycle_path_win(path)
        return "recycle-bin"
    if os.path.isdir(path) and not os.path.islink(path):
        shutil.rmtree(path)
    else:
        os.remove(path)
    return "deleted"
