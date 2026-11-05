#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# k8s-prerun.sh — controller 预运行脚本（benchmark 流程开始前执行一次）
#
# 用途：在 controller 节点上执行自定义前置操作（检查/导入镜像、准备节点
# 环境等）。脚本内容由用户维护，编排器不校验、不覆盖。
#
# 执行规则：
#   - 仅 k8s 模式执行；SSH 模式不受影响
#   - 在 controller 节点上运行（非编排机、非 pod 内）
#   - 本文件不存在 → 静默跳过，不影响现有流程
#   - 返回非零 → 终止 benchmark 运行（前置条件不满足）
#   - standalone --clean-up 不执行
#   - 编排器自动上传至 <controller_workspace>/ 并以 bash 执行
#
# 在这里写自定义逻辑，例如：
#   docker images | grep mooncake || echo "[WARN] image not found"
#   ctr -n k8s.io images import /path/to/image.tar
# ---------------------------------------------------------------------------
set -u

exit 0
