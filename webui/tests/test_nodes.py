#!/usr/bin/env python3
"""集群节点 API 测试：节点/角色合并、路径与节点名校验、探测输出解析。

SSH 相关用例 monkeypatch utils.ssh_check_output（探测输出用样本），
不依赖真实节点。
"""
import json
import os
import sys
import threading
import urllib.error
import urllib.request
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from webui.backend.main import create_server
from webui.backend.routers import nodes

BASE = None          # http://127.0.0.1:<port>
HTTPD = None


def setup_module():
    global BASE, HTTPD
    HTTPD = create_server("127.0.0.1", 0)
    threading.Thread(target=HTTPD.serve_forever, daemon=True).start()
    BASE = "http://127.0.0.1:{}".format(HTTPD.server_address[1])


def _req(method, path, body=None):
    url = BASE + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())


SAMPLE_PROBE = """@@UNAME@@
Linux 6.8.0
@@ARCH@@
x86_64
@@SS@@
State Recv-Q Send-Q Local Address:Port Peer Address:Port Process
LISTEN 0 4096 0.0.0.0:22 0.0.0.0:* users:(("sshd",pid=111,fd=3))
LISTEN 0 128 127.0.0.1:50051 127.0.0.1:* users:(("mooncake_master",pid=222,fd=4))
LISTEN 0 128 127.0.0.1:9999 127.0.0.1:*
@@PS@@
  111 root sshd
  222 haoyu mooncake_master
@@PROCS@@
  222 /opt/mooncake/bin/mooncake_master --port=50051
  333 /opt/mooncake/bin/mooncake_client --store_port=9101
"""


def test_nodes_list_and_roles():
    code, d = _req("GET", "/api/nodes")
    assert code == 200
    assert d["mode"] == "ssh"
    assert d["cluster"] == "configs/cluster.json"
    assert d["nodes"], "repo cluster.json should expose nodes"
    by_name = {n["name"]: n for n in d["nodes"]}
    assert "store1" in by_name
    # global.json mooncake_store_nodes -> store 角色
    assert "store" in by_name["store1"]["roles"]
    for n in d["nodes"]:
        assert n["roles"], "every node must carry at least one role"
        assert isinstance(n["expected_ports"], list)


def test_nodes_k8s_cluster_exposes_controller():
    """k8s 型配置：无 SSH 节点，但把 controller 作为节点暴露（SSH 探测/套件）。"""
    code, d = _req("GET", "/api/nodes?cluster=configs/_k8s/cluster.json")
    assert code == 200
    assert d["mode"] == "k8s"
    names = [n["name"] for n in d["nodes"]]
    assert names == ["controller"]
    ctrl = d["nodes"][0]
    assert ctrl["roles"] == ["controller"]
    assert ctrl["ip"] and ctrl["expected_ports"] == []


def test_nodes_reject_unknown_node_and_path_escape():
    code, d = _req("GET", "/api/nodes/nope/test")
    assert code == 404 and "nope" in d["error"]
    code, d = _req("GET", "/api/nodes?cluster=../../etc/passwd")
    assert code == 400 and "越界" in d["error"]


def test_nodes_parse_ports_ss_and_netstat():
    ss = ("State Recv-Q Send-Q Local Address:Port Peer Address:Port Process\n"
          'LISTEN 0 4096 0.0.0.0:22 0.0.0.0:* users:(("sshd",pid=111,fd=3))\n'
          "LISTEN 0 128 [::]:50051 [::]:*\n")
    ports = nodes._parse_ports(ss)
    assert [p["port"] for p in ports] == [22, 50051]
    assert ports[0]["users"] == [{"process": "sshd", "pid": 111}]
    assert ports[1]["local_addr"] == "::" and ports[1]["users"] == []

    netstat = ("Proto Recv-Q Send-Q Local Address Foreign Address State PID/Program\n"
               "tcp 0 0 0.0.0.0:22 0.0.0.0:* LISTEN 111/sshd\n")
    ports = nodes._parse_ports(netstat)
    assert ports == [{"proto": "tcp", "state": "LISTEN", "local_addr": "0.0.0.0",
                      "port": 22,
                      "users": [{"process": "sshd", "pid": 111}]}]


def test_nodes_parse_procs_and_kinds():
    procs = nodes._parse_procs(
        "  222 /opt/mooncake/bin/mooncake_master --port=50051\n"
        "  333 /opt/mooncake/bin/mooncake_client --store_port=9101\n")
    assert [p["pid"] for p in procs] == [222, 333]
    assert nodes._proc_kind("/x/mooncake_master --port=50051") == "master"
    assert nodes._proc_kind("/x/mooncake_client --store_port=9101") == "store"
    assert nodes._proc_kind("/x/mooncake-store --port=9101") == "store"
    assert nodes._proc_kind("python3 scripts/mooncake_benchmark_client.py") == "benchmark"
    assert nodes._proc_kind("/x/other --flag") == "other"
    assert nodes._parse_ps("  111 root sshd\n")[111] == {"user": "root", "comm": "sshd"}


def test_nodes_role_and_expected_ports():
    cluster = {"nodes": {
        "sp1": {"ip": "10.0.0.1", "fabric_ip": "10.1.0.1"},
        "md1": {"ip": "10.0.0.2"},
        "cli1": {"ip": "10.0.0.3"},
    }}
    glob = {"mooncake_store_nodes": ["sp1"],
            "master_addr": "10.0.0.2:50051",
            "metadata_server": "http://10.0.0.2:9999/metadata",
            "etcd_endpoints": "http://10.0.0.3:2379"}
    ns = nodes._cluster_nodes(cluster)
    roles = nodes._role_map(ns, glob)
    assert roles["sp1"] == ["store"]
    assert roles["md1"] == ["master", "metadata"]
    assert roles["cli1"] == ["etcd"]
    exp = nodes._expected_ports(ns[0], glob)
    assert exp == []                      # store 节点没有 master/元数据/etcd 端口
    exp = nodes._expected_ports(ns[1], glob)
    assert {"purpose": "master RPC", "port": 50051} in exp
    assert {"purpose": "元数据 HTTP", "port": 9999} in exp


def test_node_test_endpoint_with_mocked_ssh():
    with mock.patch("utils.ssh_check_output", return_value=SAMPLE_PROBE):
        code, d = _req("GET", "/api/nodes/store1/test")
    assert code == 200 and d["ok"] is True
    assert d["kernel"] == "Linux 6.8.0" and d["arch"] == "x86_64"
    kinds = [p["kind"] for p in d["processes"]]
    assert kinds == ["master", "store"]
    checks = {p["port"]: p for p in d["ports"]}
    assert checks[50051]["listening"] is True
    assert checks[50051]["purpose"] == "master RPC"
    assert checks[9999]["listening"] is True


def test_node_test_endpoint_ssh_failure():
    with mock.patch("utils.ssh_check_output",
                    side_effect=RuntimeError("SSH connect timeout to 10.0.0.1")):
        code, d = _req("GET", "/api/nodes/store1/test")
    assert code == 200 and d["ok"] is False
    assert "timeout" in d["detail"] and d["processes"] == []


def test_node_ports_endpoint_with_mocked_ssh():
    with mock.patch("utils.ssh_check_output", return_value=SAMPLE_PROBE):
        code, d = _req("GET", "/api/nodes/store1/ports")
    assert code == 200 and d["ok"] is True
    assert d["warning"] is None
    by_port = {p["port"]: p for p in d["ports"]}
    assert by_port[50051]["processes"] == [
        {"pid": 222, "process": "mooncake_master", "user": "haoyu"}]
    assert by_port[22]["processes"] == [
        {"pid": 111, "process": "sshd", "user": "root"}]
    assert by_port[9999]["processes"] == []


def test_node_ports_endpoint_ssh_failure():
    with mock.patch("utils.ssh_check_output",
                    side_effect=RuntimeError("connection refused")):
        code, d = _req("GET", "/api/nodes/store1/ports")
    assert code == 200 and d["ok"] is False
    assert d["ports"] == [] and "refused" in d["detail"]
