#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# env.mooncake.sh — mooncake store/SDK 调优默认值（手工维护面板）
#
# - 本文件由 deploy_scripts 随 env.sh / runner 脚本 / client 自动推送到所有
#   参与节点，run_benchmark_basic.sh / run_benchmark_pipeline.sh 在 source
#   env.sh 之后 source 本文件。
# - 覆盖顺序：env.sh（编排器生成）→ 本文件（同名变量以本文件为准）→
#   命令行 inline 注入（如 MC_YLT_LOG_PATH，永远最优先）。
# - 本文件可自由增删改；编排器不生成、不校验、不覆盖本文件内容。
#   所有权已从 global.json / 编排器迁移至此的变量：MC_HIFREQ_LOG_SAMPLE_RATE、
#   MC_YLT_LOG_LEVEL、MC_YLT_LOG_MAX_FILE_SIZE、MC_YLT_LOG_MAX_FILES、
#   MC_GLOBAL_SEGMENT_SIZE（global.json 同名字段已删除，编排器不再生成）。
# ---------------------------------------------------------------------------
export LD_LIBRARY_PATH="/usr/lib64/urma:${LD_LIBRARY_PATH:-}"
# export LD_LIBRARY_PATH="/usr/local/cuda/lib64:${LD_LIBRARY_PATH:-}"
export LD_LIBRARY_PATH="/home/Mooncake/outputs/lib:${LD_LIBRARY_PATH:-}"
# --- 连接与容错 ---
export MC_STORE_CLIENT_SETUP_RETRIES=3        # store client setup 失败重试次数
export MC_STORE_WARMUP=1                      # store client 连接预热
export MC_STORE_CLIENT_METRIC_BANDWIDTH=1     # 采集带宽指标
# --- 客户端资源 ---
export MC_GLOBAL_SEGMENT_SIZE=0               # 本 client 贡献的存储大小（纯客户端 0）
# --- ylt RPC 连接池 ---
# export MC_YLT_RPC_POOL_WARMUP=1               # 连接池预热
export MC_OFFLOAD_RPC_THREAD_NUM=16           # offload RPC 线程数      
# export MC_YLT_RPC_POOL_MAX_CONNECTION=32      # 连接池最大连接数
# export MC_YLT_RPC_POOL_WARMUP_CONNECTIONS=1  # 预热连接数
# export MC_YLT_RPC_POOL_IDLE_TIMEOUT_MS=604800000      # 空闲连接超时（7 天）
# export MC_YLT_RPC_POOL_SHORT_IDLE_TIMEOUT_MS=50000     # 短空闲超时（5 秒）
# --- 日志 ---
export MC_LOG_ENABLE=on                       # glog 开关
# export MC_LOG_DIR=/var/log/mooncake         # glog 目录（默认 stdout，按需启用）
export MC_HIFREQ_LOG_SAMPLE_RATE=1            # 高频日志采样率
export MC_URMA_LOG_REDIRECT=1                 # 导入urma日志到glog，方便统一采集
export URMA_LOG_LEVEL=debug                   # urma日志级别
export GLOG_v=1                               # glog日志级别
# --- 传输层（URMA/UB） ---
export MC_SLICE_SIZE=1048576                 # slice 大小（1MB）
export MC_WORKERS_PER_CTX=2                  # 每个 ctx 的 worker 数
export MC_NUM_CQ_PER_CTX=2                   # 每个 ctx 的 CQ 数
export MC_MAX_WR=32                          # 最大 outstanding work request
export MC_URMA_TRANS_MODE=RM                 # URMA 传输模式（RM）
export MC_OFFLOAD_PUSH=false                 # 关闭 offload push
export MC_URMA_BONDING_MULTIPATH_ENABLE=on
export MC_UB_NUMA_AFFINITY_ENABLE=on
# export MC_YLT_LOG_LEVEL=info                  # ylt easylog（coro_rpc 内部事件）级别
# export MC_YLT_LOG_MAX_FILE_SIZE=52428800      # ylt 单文件上限（50MB）
# export MC_YLT_LOG_MAX_FILES=3                 # ylt 轮转文件数
# 已知行为：ylt 为 async 写且 flush_every_time 硬编码 false，info 级日志量
# 通常不足以撑满 ofstream 缓冲，叠加客户端 os._exit 跳过静态析构 →
# .ylt.log 可能是空文件（详见 README 7.3）。诊断 coro_rpc 问题时可临时：
# export MC_YLT_LOG_LEVEL=debug                # 加大日志量靠缓冲溢出自然落盘
# 未来 Mooncake 补丁（MC_YLT_LOG_FLUSH env 化）就绪后取消下行注释即逐条落盘：
# export MC_YLT_LOG_FLUSH=1
# ============================================================================
# Mooncake + ylt(URMA RPC) 运行时环境变量
# 适用：编译期带 -DYLT_ENABLE_URMA=ON，且 yalantinglibs 来自 extern/ 子模块
# ============================================================================
# 1) 总开关 目前存在冗余和语义混乱问题
export MC_RPC_PROTOCOL=tcp                   # 默认未设=tcp；可选 tcp|rdma|urma，大小写敏感
export MC_YLT_URMA_ENABLE=0                  # 默认空=关；1|on|true|yes 均可（ylt 内部转小写）
# 2) URMA 设备与 socket 资源（ylt，均有默认值；不设即用默认/自动探测）
export MC_YLT_URMA_DEVICE=bonding_dev_13    # 默认空=自动探测首个可用设备
export MC_YLT_URMA_EID_INDEX=0              # 默认 0, 玮琪说使用1会用到primary eid
export MC_YLT_URMA_TP_TYPE=ctp              # 默认 ctp；可选 ctp|rtp 或 0|1
export MC_YLT_URMA_CQ_SIZE=128              # 默认 128
export MC_YLT_URMA_RECV_BUFFER_CNT=8        # 默认 8
export MC_YLT_URMA_SEND_BUFFER_CNT=4        # 默认 4
export MC_YLT_URMA_BUFFER_SIZE=4096        # 默认 4096（bonding CTP 单包上限）
export MC_YLT_URMA_MAX_MEMORY_USAGE=268435456 # 默认 256 MiB，per-device buffer pool 上限
export MC_YLT_URMA_PRIORITY=15                # 默认 15（URMA 最高优先级；>15 会被忽略）
export MC_YLT_URMA_EVENT_MODE=false           # 默认 true；false=走轮询，时延更低、CPU 更高
export MC_YLT_URMA_BUSY_POLL_BUDGET=16        # 默认 16，event_mode=false 时的忙轮询预算
export MC_YLT_URMA_POLL_INTERVAL=5           # 默认 5（微秒），轮询间隔
# 3) ylt 日志（easylog，进程内只初始化一次）
export MC_YLT_LOG_LEVEL=info                 # 默认 warn；trace|debug|info|warn|error|critical
export MC_YLT_LOG_PATH=./logs/ylt_rpc.log   # 默认 logs/rpc.log；目录需先存在
export MC_YLT_LOG_MAX_FILE_SIZE=1048576000    # 默认 1000 MiB（字节）
export MC_YLT_LOG_MAX_FILES=3                 # 默认 3
# 4) RPC 连接池 / 超时（主要作用于 store 的 master/client coro_rpc 池）
export MC_YLT_RPC_POOL_MAX_CONNECTION=4        # 默认 100（每 target 上限）
export MC_YLT_RPC_POOL_IDLE_TIMEOUT_MS=3000000 # 默认 30s
export MC_YLT_RPC_POOL_SHORT_IDLE_TIMEOUT_MS=10000000 # 默认 1s
export MC_YLT_RPC_POOL_WARMUP=true             # 默认 true，建池预热
export MC_YLT_RPC_POOL_WARMUP_CONNECTIONS=1   # 默认 = MC_YLT_RPC_POOL_MAX_CONNECTION
export MC_RPC_TIMEOUT_MS=30000                 # 默认 coro_rpc 30s；负值=关闭单请求计时
export MC_RPC_CONNECT_TIMEOUT_MS=30000         # 默认 coro_rpc 30s
# 5) RPC IO 线程（ylt io_context 池）
export MC_RPC_CLIENT_IO_THREADS=16             # 默认 min(16, CPU 核数)
# export MC_TE_RPC_CLIENT_IO_THREADS=16          # 不设则继承上一项
# export MC_STORE_RPC_CLIENT_IO_THREADS=16       # 不设则继承上一项
# 6) IP与端口
# export MC_TCP_BIND_ADDRESS="192.168.49.61"
# 7) YLT RPC 性能跟踪日志开关
export YLT_RPC_TRACE=1
export MC_UB_NUMA_NODES=0
export MC_URMA_TE_PRIORITY=6
export MC_STORE_PARTITION_SLOT_COUNT=32
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
