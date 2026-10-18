#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# env.mooncake.sh — mooncake store/SDK 调优默认值（手工维护面板）
#
# - 本文件由 deploy_scripts 随 env.sh / runner 脚本 / client 自动推送到所有
#   参与节点，run_benchmark_basic.sh / run_benchmark_pipeline.sh 在 source
#   env.sh 之后 source 本文件。
# - 覆盖顺序：env.sh（编排器生成）→ 本文件（同名变量以本文件为准）→
#   命令行 inline 注入（如 MC_YLT_LOG_PATH，永远最优先）。
# - 本文件可自由增删改；编排器不生成、不校验、不覆盖本文件内容。
#   所有权已从 global.json / 编排器迁移至此的变量：MC_HIFREQ_LOG_SAMPLE_RATE、
#   MC_YLT_LOG_LEVEL、MC_YLT_LOG_MAX_FILE_SIZE、MC_YLT_LOG_MAX_FILES、
#   MC_GLOBAL_SEGMENT_SIZE（global.json 同名字段已删除，编排器不再生成）。
# ---------------------------------------------------------------------------

# --- 连接与容错 ---
export MC_STORE_CLIENT_SETUP_RETRIES=3        # store client setup 失败重试次数
export MC_STORE_WARMUP=1                      # store client 连接预热
export MC_STORE_CLIENT_METRIC_BANDWIDTH=1     # 采集带宽指标

# --- 客户端资源 ---
export MC_GLOBAL_SEGMENT_SIZE=0               # 本 client 贡献的存储大小（纯客户端 0）

# --- ylt RPC 连接池 ---
export MC_YLT_RPC_POOL_WARMUP=1               # 连接池预热
export MC_OFFLOAD_RPC_THREAD_NUM=16           # offload RPC 线程数
export MC_YLT_RPC_POOL_MAX_CONNECTION=24      # 连接池最大连接数
export MC_YLT_RPC_POOL_WARMUP_CONNECTIONS=24  # 预热连接数
export MC_YLT_RPC_POOL_IDLE_TIMEOUT_MS=604800000      # 空闲连接超时（7 天）
export MC_YLT_RPC_POOL_SHORT_IDLE_TIMEOUT_MS=5000     # 短空闲超时（5 秒）

# --- 日志 ---
export MC_LOG_ENABLE=on                       # glog 开关
# export MC_LOG_DIR=/var/log/mooncake         # glog 目录（默认 stdout，按需启用）
export MC_HIFREQ_LOG_SAMPLE_RATE=1.0          # 高频日志采样率（1.0 = 全量；走 glog，已在客户端日志采集）
export MC_YLT_LOG_LEVEL=info                  # ylt easylog（coro_rpc 内部事件）级别
export MC_YLT_LOG_MAX_FILE_SIZE=52428800      # ylt 单文件上限（50MB）
export MC_YLT_LOG_MAX_FILES=3                 # ylt 轮转文件数
# 已知行为：ylt 为 async 写且 flush_every_time 硬编码 false，info 级日志量
# 通常不足以撑满 ofstream 缓冲，叠加客户端 os._exit 跳过静态析构 →
# .ylt.log 可能是空文件（详见 README 7.3）。诊断 coro_rpc 问题时可临时：
# export MC_YLT_LOG_LEVEL=debug                # 加大日志量靠缓冲溢出自然落盘
# 未来 Mooncake 补丁（MC_YLT_LOG_FLUSH env 化）就绪后取消下行注释即逐条落盘：
# export MC_YLT_LOG_FLUSH=1

# --- 传输层（URMA/UB） ---
export MC_SLICE_SIZE=1048576                  # slice 大小（1MB）
export MC_WORKERS_PER_CTX=2                   # 每个 ctx 的 worker 数
export MC_NUM_CQ_PER_CTX=2                    # 每个 ctx 的 CQ 数
export MC_MAX_WR=32                           # 最大 outstanding work request
export MC_URMA_TRANS_MODE=RM                  # URMA 传输模式（RM）
export MC_OFFLOAD_PUSH=false                  # 关闭 offload push
