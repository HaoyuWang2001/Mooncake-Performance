#!/usr/bin/env python3
"""webui/backend/paths.py — webui 全部路径常量与 sys.path 装配（单一事实来源）。

目录职责（学习 Services-Deployment 的分层）：
  backend/  Python 后端（main.py 装配 + routers/ 按域拆分 + runmgr 进程管理）
  web/      前端成品（无构建步骤；URL 前缀保持 /static/）
  data/     运行时状态（gitignored）：runs/ 运行记录与日志
  backups/  配置编辑备份（gitignored）
  tests/    可重跑测试（pytest 风格，亦可直接 python 执行）
"""
import os
import re
import sys

WEBUI_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))   # webui/
ROOT = os.path.dirname(WEBUI_DIR)                                          # 仓库根
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)      # 使 import benchmark / webui.backend.* 可用

DATA_DIR = os.path.join(WEBUI_DIR, "data")
RUNS_DIR = os.path.join(DATA_DIR, "runs")
BACKUP_DIR = os.path.join(WEBUI_DIR, "backups")
STATIC_DIR = os.path.join(WEBUI_DIR, "web")        # 磁盘目录 web/，URL 前缀 /static/
SCHED_DIR = os.path.join(ROOT, "schedule")
API_MD = os.path.join(WEBUI_DIR, "API.md")

# 文件读取白名单（/api/file、/api/csv）
READ_ROOTS = [os.path.join(ROOT, "outputs"), RUNS_DIR]
# 配置读写白名单（/api/config*、schedule 计划）
CONF_ROOTS = [os.path.join(ROOT, "configs"), SCHED_DIR]

MAX_TEXT_CHUNK = 512 * 1024
TS_RE = re.compile(r"^[0-9A-Za-z._-]+$")

os.makedirs(RUNS_DIR, exist_ok=True)
os.makedirs(BACKUP_DIR, exist_ok=True)
