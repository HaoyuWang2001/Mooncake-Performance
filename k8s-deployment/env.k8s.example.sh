#!/usr/bin/env bash
# env.k8s.example.sh — 手动在 pod 内跑 basic 场景的 env.sh 模板。
# 用法：填充尖括号占位后，kubectl cp 到 pod 的 /tmp/mooncake_bench/scripts/env.sh。
# 注意：
#   - MC_MASTER_TYPE / MC_MASTER_ADDR 是仅有的硬性必填（客户端启动即校验）
#   - MC_TCP_BIND_ADDRESS 必须为 Pod IP（不绑对会导致连接挂死）
#   - 域名（Service DNS）请先解析为 IP 再填入（kubectl get svc / getaddrinfo）
#   - 容器内不做绑核：不设 MC_CPU_SHARD / MC_EXCLUDE_CPUS
export MC_VENV=python3
export MC_MASTER_TYPE=http
export MC_MASTER_ADDR=<master-IP:端口>
export MC_METADATA_SERVER=<metadata-IP:端口/metadata>
export MC_PROTOCOL=tcp
# ub 协议时：
# export MC_UB_DEVICE=bonding_dev_0
export MC_KEY_PREFIX=mc_bench
export MC_KEY_COUNT=1000
export MC_DATA_SIZE=8MB
export MC_REMOTE_DATA_DIR=/tmp/mooncake_bench/data
export MC_FABRIC_IP=<POD_IP>
export MC_TCP_BIND_ADDRESS=<POD_IP>
