"""节点身份口径：报表/产物中的「节点」= **节点 IP**
（k8s 取 pod.status.hostIP；ssh 取节点管理 IP，重复 IP 回退原名）。"""
import json
import pathlib
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import benchmark as bm  # noqa: E402
import report as rp  # noqa: E402


def _cluster(nodes):
    return {"nodes": nodes}


def test_node_display_k8s_uses_host_ip():
    cl = _cluster({"pod-a": {"kind": "k8s-pod", "_pod": "pod-a",
                             "ip": "10.244.1.5", "host_ip": "192.168.1.11"},
                   "pod-b": {"kind": "k8s-pod", "_pod": "pod-b",
                             "ip": "10.244.2.9", "host_ip": "192.168.1.12"}})
    assert bm._node_display(cl, cl["nodes"]["pod-a"], "pod-a") == "192.168.1.11"
    # 同主机多 pod → 同一显示名（节点视角合并）
    cl["nodes"]["pod-c"] = {"kind": "k8s-pod", "_pod": "pod-c",
                            "ip": "10.244.1.6", "host_ip": "192.168.1.11"}
    assert bm._node_display(cl, cl["nodes"]["pod-c"], "pod-c") == "192.168.1.11"


def test_node_display_k8s_missing_host_ip_falls_back():
    node = {"kind": "k8s-pod", "_pod": "pod-a", "ip": "10.0.0.5",
            "host_ip": ""}
    assert bm._node_display(_cluster({"pod-a": node}), node, "pod-a") == "pod-a"


def test_node_display_ssh_uses_management_ip():
    cl = _cluster({"node1": {"ip": "192.168.137.12"},
                   "node2": {"ip": "192.168.137.13"}})
    assert bm._node_display(cl, cl["nodes"]["node1"], "node1") == \
        "192.168.137.12"


def test_node_display_ssh_duplicate_ip_falls_back():
    # 示例配置常见所有节点 127.0.0.1 —— 不能把无关节点合并成一个
    cl = _cluster({"n1": {"ip": "127.0.0.1"},
                   "n2": {"ip": "127.0.0.1"}})
    assert bm._node_display(cl, cl["nodes"]["n1"], "n1") == "n1"


def test_k8s_pod_host_ip_parse(monkeypatch):
    calls = []

    def fake(controller, args, check=False):
        calls.append(args)
        return ("192.168.1.11\n", 0)

    monkeypatch.setattr(bm, "controller_run", fake)
    assert bm._k8s_pod_host_ip({}, "ns", "pod-a") == "192.168.1.11"
    assert "status.hostIP" in " ".join(calls[0])

    monkeypatch.setattr(bm, "controller_run",
                        lambda *a, **k: ("", 1))
    assert bm._k8s_pod_host_ip({}, "ns", "pod-a") == ""


def test_map_cmd_uses_node_ip(tmp_path, monkeypatch):
    """logstats/tracestats 的 --node 使用节点显示名（ssh 模式 = 管理 IP）。"""
    logp = tmp_path / "x.writer.log"
    logp.write_text("", encoding="utf-8")
    glog = tmp_path / "x.glog"
    glog.write_text("", encoding="utf-8")
    seen = []
    monkeypatch.setattr(bm, "log_detail",
                        lambda *a, **k: seen.append(" ".join(map(str, a))))
    node = {"ip": "192.168.137.12"}
    cl = _cluster({"node1": node})
    monkeypatch.setattr(bm, "resolve_venv", lambda *a, **k: "python3")
    bm.run_logstats_map(cl, node, [str(logp)], "/s", {}, dry=True,
                        node_name="node1")
    bm.run_tracestats_map(cl, node, [str(glog)], "/s", {}, dry=True,
                          node_name="node1")
    cmds = " ".join(seen)
    assert "--node 192.168.137.12" in cmds, cmds
    assert "--node node1" not in cmds, cmds


def _write_mon(root, node_ip, with_host=True, used=10000):
    d = pathlib.Path(root) / "profiling" / "pod-x"
    d.mkdir(parents=True)
    (d / "monitor.config.json").write_text(json.dumps(
        {"node_ip": node_ip,
         "mem": {"leak_growth_pct": 30, "leak_min_mb": 200}}),
        encoding="utf-8")
    if with_host:
        (d / "mem.host.csv").write_text(
            "ts,MemUsed_mb,MemAvailable_mb\n100,{},\n".format(used),
            encoding="utf-8")
    return d


def test_reduce_monitors_labels_by_node_ip_and_dedupes_host(tmp_path):
    # 同主机两个 pod 目录（不同目录名）→ 标签同一节点 IP，宿主内存只保留一份
    _write_mon(tmp_path, "192.168.1.11", used=10000)
    d2 = tmp_path / "profiling" / "pod-y"
    d2.mkdir(parents=True)
    (d2 / "monitor.config.json").write_text(json.dumps(
        {"node_ip": "192.168.1.11"}), encoding="utf-8")
    (d2 / "mem.host.csv").write_text(
        "ts,MemUsed_mb,MemAvailable_mb\n100,10000,\n", encoding="utf-8")
    mon = rp.reduce_monitors(str(tmp_path))
    assert mon["mem_host"], mon
    assert mon["mem_host"][0]["node"] == "192.168.1.11"
    assert len(mon["mem_host"]) == 1          # 同主机去重
