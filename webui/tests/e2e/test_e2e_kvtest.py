"""E2E：真跑 30s kvtest 用例，对产物做契约断言（每个 check_id = 一条规则）。

门控：pytest -m e2e；lab 不可用自动 skip（见 e2e/conftest.py）。
"""
import json
import os

import pytest

from . import checks

_HERE = os.path.dirname(os.path.abspath(__file__))
REG = os.path.join(os.path.dirname(_HERE), "registry.json")


def _checks():
    with open(REG, encoding="utf-8") as f:
        reg = json.load(f)
    out = []
    for r in reg.get("rules") or []:
        for c in r.get("e2e") or []:
            out.append(pytest.param(c, id="{}[{}]".format(c, r["id"])))
    return out


@pytest.mark.e2e
@pytest.mark.parametrize("check_id", _checks())
def test_e2e(check_id, run_artifacts):
    fn = getattr(checks, check_id, None)
    assert fn is not None, "未实现的 E2E 检查: " + check_id
    fn(run_artifacts)
