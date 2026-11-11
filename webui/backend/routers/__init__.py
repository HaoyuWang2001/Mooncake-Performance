#!/usr/bin/env python3
"""webui/backend/routers — 按领域拆分的 API 路由聚合。

每个子模块暴露 ROUTES = [(method, pattern, fn), ...]；main.py 的 dispatch
按序匹配。fn 签名：fn(handler, query, **match.groupdict())。
"""
from . import configs, instances, outputs, runs, schedule, system

ROUTES = (system.ROUTES + configs.ROUTES + runs.ROUTES
          + instances.ROUTES + outputs.ROUTES + schedule.ROUTES)
