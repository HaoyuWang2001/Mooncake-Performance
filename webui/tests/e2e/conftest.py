"""E2E 驱动（lab 门控）：真跑一个 30s kvtest 用例，产物供全部契约检查复用。

触发：pytest -m e2e（marker 门控）；lab 不可用（mp 后端/k8s 栈未就绪）时自动 skip。
重资源：整批只跑一次 run；每轮只允许一个 run（lab 规则），前置校验 mc-bench 命名空间不存在。
"""
import json
import os
import time
import urllib.request

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(os.path.dirname(_HERE)))
MP = "http://127.0.0.1:8500"


class RunArtifacts(object):
    """一次真实 run 的产物集合（checks 只读它，不接触 lab）。"""
    def __init__(self, ts_dir, cfg_name, meta):
        self.ts_dir = ts_dir
        self.cfg_name = cfg_name
        self.meta = meta
        self.benchmark_log = os.path.join(ts_dir, "benchmark.log")
        self.detail_csv = os.path.join(ts_dir, "benchmark_detail.csv")
        self.cfg_csv = os.path.join(ts_dir, cfg_name + ".csv")
        self.logs_dir = os.path.join(ts_dir, "logs", cfg_name)
        self.report_dir = os.path.join(self.logs_dir, "report")
        self.unit_dir = self._find_unit()
        self.data_js = os.path.join(self.unit_dir, "data.js")
        self.report_html = os.path.join(self.unit_dir, "report.html")
        self.meta_json = os.path.join(self.unit_dir, "meta.json")
        self.reduced_json = os.path.join(self.unit_dir, "reduced.json")

    def _find_unit(self):
        if not os.path.isdir(self.report_dir):
            return self.report_dir
        for name in os.listdir(self.report_dir):
            p = os.path.join(self.report_dir, name)
            if os.path.isdir(p) and os.path.isfile(
                    os.path.join(p, "report.html")):
                return p
        return self.report_dir


def _req(method, url, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.load(r)


def _sd_url():
    return os.environ.get("MC_E2E_SD_URL", "http://127.0.0.1:8499")


def _runtime_image():
    """真实 runtime 镜像：优先读运行中 pod 的镜像（ssh kubectl）。

    绝不读仓库工作树的 configs/_k8s/cluster.json——那是示例占位
    (local/stub:dev)。回退：沙箱 lab 配置（~/Mooncake-Sandbox/mp）。"""
    import subprocess as _sp
    key = os.environ.get("MC_E2E_SSH_KEY",
                         os.path.expanduser(
                             "~/Mooncake-Sandbox/lab/dev-vm/ssh/id_ed25519"))
    ctl = os.environ.get("MC_E2E_CONTROLLER", "192.168.137.10")
    for ns in ("mooncake-rank",):
        out = _sp.run(
            ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no",
             "-o", "ConnectTimeout=6", "-o", "LogLevel=ERROR",
             "haoyu@%s" % ctl,
             "kubectl -n %s get pods -o jsonpath="
             "{.items[0].spec.containers[0].image}" % ns],
            capture_output=True, text=True, timeout=30).stdout.strip()
        if out and ":" in out and "stub" not in out:
            return out
    sand = os.path.expanduser(
        "~/Mooncake-Sandbox/mp/configs/_k8s/cluster.json")
    if os.path.isfile(sand):
        img = json.load(open(sand, encoding="utf-8")).get(
            "k8s", {}).get("image")
        if img and ":" in img and "stub" not in img:
            return img
    raise RuntimeError("无法确定真实 runtime 镜像")


def _restart_stack():
    """按 lab 规则每轮开跑前重启 Mooncake 栈（k8s-3 stop→start）。

    MC_E2E_NO_RESTART=1 时跳过（调用方保证栈干净）。"""
    if os.environ.get("MC_E2E_NO_RESTART") == "1":
        return
    sd = _sd_url()
    img = _runtime_image()
    tag = img.split(":")[-1]
    _req("POST", sd + "/api/tasks", {
        "service": "k8s-3-mooncake-deployment-rank", "action": "stop",
        "nodes": ["node4-compile"]})
    time.sleep(8)
    _req("POST", sd + "/api/tasks", {
        "service": "k8s-3-mooncake-deployment-rank", "action": "start",
        "nodes": ["node4-compile"],
        "variables": {"TAG": tag, "MOONCAKE_IMAGE": img}})
    # 等待 READY
    import subprocess as _sp
    key = os.environ.get("MC_E2E_SSH_KEY",
                         os.path.expanduser(
                             "~/Mooncake-Sandbox/lab/dev-vm/ssh/id_ed25519"))
    ctl = os.environ.get("MC_E2E_CONTROLLER", "192.168.137.10")
    deadline = time.time() + 240
    while time.time() < deadline:
        time.sleep(6)
        out = _sp.run(
            ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no",
             "-o", "ConnectTimeout=6", "-o", "LogLevel=ERROR",
             "haoyu@%s" % ctl,
             "cat ~/k8s-ws/k8s-3-mooncake-deployment-rank/run/"
             "deploy.status 2>/dev/null"],
            capture_output=True, text=True, timeout=30).stdout
        if "READY" in out:
            return
    raise RuntimeError("Mooncake 栈重启未就绪")


def _mp_ok():
    try:
        d = _req("GET", MP + "/api/status")
        return d.get("active_run") is None
    except Exception:
        return False


def _k8s_stack_ok():
    try:
        # controller 上 mc-bench 命名空间应不存在（上一 run 已清理）
        import subprocess
        key = os.environ.get("MC_E2E_SSH_KEY",
                             os.path.expanduser(
                                 "~/Mooncake-Sandbox/lab/dev-vm/ssh/id_ed25519"))
        cluster = os.path.join(ROOT, "configs", "_k8s", "cluster.json")
        ctl = json.load(open(cluster, encoding="utf-8")).get(
            "controller", {}).get("ip")
        out = subprocess.run(
            ["ssh", "-i", key, "-o", "StrictHostKeyChecking=no",
             "-o", "ConnectTimeout=6", "-o", "LogLevel=ERROR",
             "haoyu@%s" % ctl,
             "kubectl get ns mc-bench --no-headers 2>&1 || true"],
            capture_output=True, text=True, timeout=30).stdout
        return "mc-bench" not in out
    except Exception:
        return False


@pytest.fixture(scope="session")
def run_artifacts():
    """真跑 matrix 里的第一个用例，返回产物。lab 不可用则 skip。"""
    if not _mp_ok():
        pytest.skip("mp 后端不可用（E2E 需要本地 lab :8500）")
    if not _k8s_stack_ok():
        pytest.skip("k8s 栈未就绪或 mc-bench 命名空间仍存在（每轮仅一个 run）")
    _restart_stack()                      # lab 规则：每轮开跑前重启栈
    matrix = json.load(open(os.path.join(_HERE, "matrix.json"),
                            encoding="utf-8"))
    case = matrix[0]
    iid = _req("POST", MP + "/api/instances", {
        "name": "e2e-" + case["name"],
        "selector": {"files": [case["config"]]},
        "flags": {"mode": "k8s", "monitor_mem": False,
                  "monitor_urma": False}}).get("instance", {}).get("id")
    assert iid, "实例创建失败"
    rid = _req("POST", MP + "/api/instances/%s/run" % iid,
               {}).get("run", {}).get("id")
    assert rid, "run 启动失败"
    deadline = time.time() + 480
    status = ts_dir = None
    while time.time() < deadline:
        time.sleep(6)
        r = _req("GET", MP + "/api/runs/%s" % rid)
        r = r.get("run") or r                      # 响应可能带 run 包装
        status = r.get("status")
        ts_dir = r.get("ts_dir")
        if status in ("done", "failed", "stopped", "error"):
            break
    assert status == "done", "run 未成功完成（status=%s）" % status
    assert ts_dir, "无 ts_dir"
    if not os.path.isabs(ts_dir):
        # mp 返回的 ts_dir 相对其 root（沙箱），不是工作树
        _root = _req("GET", MP + "/api/status").get("root") or ROOT
        ts_dir = os.path.join(_root, ts_dir)
    meta = {"run_id": rid, "ts": os.path.basename(ts_dir)}
    return RunArtifacts(ts_dir, case["name"].replace(
        "kvtest_pipeline_dual_30s", "kvtest_pipeline_dual_30s"), meta)
