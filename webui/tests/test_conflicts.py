#!/usr/bin/env python3
"""conflicts: 并发实例身份键与冲突判定（纯函数）。"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))

from webui.backend import conflicts


def _write(root, rel, obj):
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        if obj is None:
            f.write("{not json")
        else:
            json.dump(obj, f)
    return rel


def _clusters(root):
    return {
        "k8s_a": _write(root, "c/k8s_a.json", {
            "controller": {"ip": "10.0.0.1", "kubectl": "kubectl"},
            "k8s": {"namespace": "ns-a", "kubecontext": ""}}),
        "k8s_b": _write(root, "c/k8s_b.json", {
            "controller": {"ip": "10.0.0.1", "kubectl": "kubectl"},
            "k8s": {"namespace": "ns-b", "kubecontext": ""}}),
        "k8s_ctx": _write(root, "c/k8s_ctx.json", {
            "controller": {"ip": "10.0.0.1", "kubectl": "kubectl"},
            "k8s": {"namespace": "ns-a", "kubecontext": "other"}}),
        "ssh_a": _write(root, "c/ssh_a.json", {
            "nodes": {"n1": {"ip": "10.0.1.1"}, "n2": {"ip": "10.0.1.2"}}}),
        "ssh_b": _write(root, "c/ssh_b.json", {
            "nodes": {"n3": {"ip": "10.0.1.3"}}}),
        "ssh_overlap": _write(root, "c/ssh_ov.json", {
            "nodes": {"n9": {"ip": "10.0.1.2"}}}),
    }


def _globals(root):
    return {
        "g_a": _write(root, "g/g_a.json", {
            "etcd_endpoints": "etcd://etcd-a:2379",
            "master_metrics_url": "http://m-a.ns.svc.cluster.local:17702/metrics"}),
        "g_b": _write(root, "g/g_b.json", {
            "etcd_endpoints": "etcd://etcd-b:2379",
            "master_metrics_url": "http://m-b.ns.svc.cluster.local:17702/metrics"}),
        "g_same_etcd": _write(root, "g/g_etcd.json", {
            "etcd_endpoints": "etcd://etcd-a:2379",
            "master_metrics_url": "http://m-z:17702/metrics"}),
        "g_same_master": _write(root, "g/g_master.json", {
            "etcd_endpoints": "etcd://etcd-z:2379",
            "master_addr": "10.9.9.9:50051"}),
        "g_master2": _write(root, "g/g_master2.json", {
            "etcd_endpoints": "etcd://etcd-y:2379",
            "master_addr": "10.9.9.9:50051"}),
        "g_same_metrics": _write(root, "g/g_metrics.json", {
            "etcd_endpoints": "etcd://etcd-w:2379",
            "master_metrics_url": "http://m-a.ns.svc.cluster.local:17702/metrics"}),
    }


def _ident(root, **flags):
    return conflicts.run_identities(root, flags)


def test_k8s_namespace_identity(tmp_path):
    root = str(tmp_path)
    c = _clusters(root)
    g = _globals(root)
    active = {"r1": ("A", _ident(root, cluster=c["k8s_a"],
                                 **{"global": g["g_a"]}))}
    # same controller+ctx+ns -> conflict
    hit = conflicts.find_conflict(
        _ident(root, cluster=c["k8s_a"], **{"global": g["g_b"]}), active)
    assert hit and hit[0] == conflicts.KIND_K8S
    # same controller, different ns -> no conflict (allowed parallel)
    assert conflicts.find_conflict(
        _ident(root, cluster=c["k8s_b"], **{"global": g["g_b"]}),
        active) is None
    # same ns, different context -> no conflict
    assert conflicts.find_conflict(
        _ident(root, cluster=c["k8s_ctx"], **{"global": g["g_b"]}),
        active) is None


def test_ssh_node_overlap(tmp_path):
    root = str(tmp_path)
    c = _clusters(root)
    g = _globals(root)
    active = {"r1": ("A", _ident(root, mode="ssh", cluster=c["ssh_a"],
                                 **{"global": g["g_a"]}))}
    assert conflicts.find_conflict(
        _ident(root, mode="ssh", cluster=c["ssh_b"],
               **{"global": g["g_b"]}), active) is None
    hit = conflicts.find_conflict(
        _ident(root, mode="ssh", cluster=c["ssh_overlap"],
               **{"global": g["g_b"]}), active)
    assert hit and hit[0] == conflicts.KIND_SSH


def test_global_etcd_master_metrics_identity(tmp_path):
    root = str(tmp_path)
    c = _clusters(root)
    g = _globals(root)
    base = _ident(root, cluster=c["k8s_a"], **{"global": g["g_a"]})
    active = {"r1": ("A", base)}
    # same etcd
    hit = conflicts.find_conflict(
        _ident(root, cluster=c["k8s_b"], **{"global": g["g_same_etcd"]}),
        active)
    assert hit and hit[0] == conflicts.KIND_ETCD
    # same metrics host (same store)
    hit = conflicts.find_conflict(
        _ident(root, cluster=c["k8s_b"], **{"global": g["g_same_metrics"]}),
        active)
    assert hit and hit[0] == conflicts.KIND_METRICS
    # same master_addr across two global files
    a2 = {"r1": ("A", _ident(root, cluster=c["k8s_a"],
                             **{"global": g["g_same_master"]}))}
    hit = conflicts.find_conflict(
        _ident(root, cluster=c["k8s_b"], **{"global": g["g_master2"]}), a2)
    assert hit and hit[0] == conflicts.KIND_MASTER


def test_rank_recall_union_identities(tmp_path):
    root = str(tmp_path)
    c = _clusters(root)
    g = _globals(root)
    combined = _ident(root, launch_mode="rank_recall",
                      rank_cluster=c["k8s_a"], rank_global=g["g_a"],
                      recall_cluster=c["k8s_b"], recall_global=g["g_b"])
    assert len(combined) == 2
    # holds rank's namespace -> a single run on rank conflicts
    active = {"r1": ("R", _ident(root, cluster=c["k8s_a"],
                                 **{"global": g["g_a"]}))}
    assert conflicts.find_conflict(combined, active) is not None
    # holds recall's etcd -> a single run on recall conflicts
    active = {"r1": ("R", _ident(root, cluster=c["k8s_b"],
                                 **{"global": g["g_b"]}))}
    assert conflicts.find_conflict(combined, active) is not None
    # an unrelated stack does not conflict
    active = {"r1": ("X", _ident(root, cluster=c["ssh_b"],
                                 **{"global": g["g_master2"]}))}
    assert conflicts.find_conflict(combined, active) is None


def test_unparsable_config_falls_back_to_file_identity(tmp_path):
    root = str(tmp_path)
    bad = _write(root, "c/bad.json", None)
    ident = conflicts.run_identities(root, {"cluster": bad,
                                            "global": "missing.json"})
    kinds = set(ident[0])
    assert "cluster_file" in kinds and "global_file" in kinds
    active = {"r1": ("A", ident)}
    hit = conflicts.find_conflict(ident, active)
    assert hit and hit[0] == "cluster_file"


def test_describe_is_human_readable(tmp_path):
    txt = conflicts.describe(conflicts.KIND_K8S, "10.0.0.1||ns-a")
    assert "namespace" in txt and "10.0.0.1" in txt
    txt = conflicts.describe(conflicts.KIND_SSH, frozenset(["10.0.1.1"]))
    assert "节点" in txt and "10.0.1.1" in txt
