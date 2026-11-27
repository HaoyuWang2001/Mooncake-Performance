import json

import pytest

from webui.backend.common import ApiError
from webui.backend.routers import instances


def _make_cluster(root, rel="configs/_k8s/cluster.json", image="mooncake-runtime-x:oldtag"):
    # root 模拟仓库根；rel 与运行时 flags.cluster 一致（含前导 configs/）
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "k8s": {"namespace": "mc-bench", "image": image,
                 "controller_workspace": "/tmp/ws", "pod_workspace": "/workspace/mp",
                 "host_log_root": "/var/lib/mooncake-bench"},
    }, ensure_ascii=False))
    return path


def _snap(root):
    # 运行时 configs_root 指向 <根>/configs（与 _build_run_argv 一致）
    return {"configs_root": str(root / "configs")}


def test_apply_image_tag_overrides_base_with_new_tag(tmp_path):
    p = _make_cluster(tmp_path)
    snap = _snap(tmp_path)
    request = {"flags": {"mode": "k8s", "cluster": "configs/_k8s/cluster.json"}}
    instances._apply_image_tag_override(snap, request, "newtag")
    out = json.loads(p.read_text())
    assert out["k8s"]["image"] == "mooncake-runtime-x:newtag"


def test_apply_image_tag_empty_is_noop(tmp_path):
    p = _make_cluster(tmp_path)
    snap = _snap(tmp_path)
    request = {"flags": {"mode": "k8s", "cluster": "configs/_k8s/cluster.json"}}
    instances._apply_image_tag_override(snap, request, "")
    instances._apply_image_tag_override(snap, request, None)
    out = json.loads(p.read_text())
    assert out["k8s"]["image"] == "mooncake-runtime-x:oldtag"


def test_apply_image_tag_ssh_mode_is_noop(tmp_path):
    p = _make_cluster(tmp_path)
    snap = _snap(tmp_path)
    request = {"flags": {"mode": "ssh", "cluster": "configs/_ssh/cluster.json"}}
    instances._apply_image_tag_override(snap, request, "newtag")
    out = json.loads(p.read_text())
    assert out["k8s"]["image"] == "mooncake-runtime-x:oldtag"


def test_apply_image_tag_rejects_slash(tmp_path):
    _make_cluster(tmp_path)
    snap = _snap(tmp_path)
    request = {"flags": {"mode": "k8s", "cluster": "configs/_k8s/cluster.json"}}
    with pytest.raises(ApiError):
        instances._apply_image_tag_override(snap, request, "repo/img:tag")


def test_apply_image_tag_rank_recall_patches_both(tmp_path):
    rank = _make_cluster(tmp_path, "configs/_k8s/cluster.rank.json", "mooncake-runtime-x:oldtag")
    recall = _make_cluster(tmp_path, "configs/_k8s/cluster.recall.json", "mooncake-runtime-x:oldtag")
    snap = _snap(tmp_path)
    request = {"flags": {"mode": "k8s", "launch_mode": "rank_recall",
                        "rank_cluster": "configs/_k8s/cluster.rank.json",
                        "recall_cluster": "configs/_k8s/cluster.recall.json"}}
    instances._apply_image_tag_override(snap, request, "fresh")
    assert json.loads(rank.read_text())["k8s"]["image"] == "mooncake-runtime-x:fresh"
    assert json.loads(recall.read_text())["k8s"]["image"] == "mooncake-runtime-x:fresh"
