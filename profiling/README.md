# profiling/ — 性能诊断配置（profile.json）

benchmark 运行期间自动激活性能诊断工具（spdiag / perf / bpftrace），结束后收集采样数据
并生成异常值追踪报告。**编排层零侵入测量路径**：所有诊断故障仅 `[WARN]`，绝不影响
测试结果与退出码。

## 用法

```bash
cp configs/_ssh/profile.example.json configs/_ssh/profile.json   # 编辑工具开关（已 gitignore）
# k8s: cp configs/_k8s/profile.example.json configs/_k8s/profile.json
python benchmark.py --config-dir configs/pipeline/demo   # 自动读取 <mode-dir>/profile.json
python benchmark.py ... --profile /path/to/profile.json   # 显式指定
# 手动异常值追踪（离线）
python profiling/profiling.py --trace outputs/<ts> [--at HH:MM:SS.micro] [--threshold-ms 1.0] [--window-ms 200]
```

未显式指定 `--profile` 时使用 `<mode-dir>/profile.json`；该文件必须存在（缺失报错并提示从 example 复制）。文件内工具全关 → 诊断关闭（主路径零变化）。

## 工具矩阵

| 工具 | 覆盖 | 命令形态 | 权限 |
| --- | --- | --- | --- |
| `spdiag` | 侵入式代码段耗时聚合（GetReplicaList 等） | `spdiag restart` → 运行 → `spdiag show > spdiag.show.txt` | 节点级全局 |
| `perf` | on-CPU 任意函数火焰图（系统级 `-a`） | `sudo -n perf record -F 99 -a -g --call-graph fp -o perf.data`，SIGINT 后轮询落盘再收集；stderr 存 `perf.data.err` 一并收集 | sudo（可关） |
| `runqlat` | 调度延迟直方图（bt/runqlat.bt，周期输出带时间戳） | `sudo -n env INTERVAL=10 bpftrace runqlat.bt`（.bt 启动时推送到远端 profiling 目录） | sudo（可关） |
| `offcputime` | off-CPU/持锁等待（bt/offcputime.bt，大事件明细带时间戳） | `sudo -n env MIN_BLOCK_US=1000 bpftrace offcputime.bt`（同上） | sudo（可关） |
| `mem` | 整机内存（numastat 各 NUMA MemUsed + MemAvailable）与进程内存（`/proc/<pid>/smaps_rollup` 的 Rss/Pss/Private/Swap），用于泄露观测。**默认开启** | `env INTERVAL_S=1 PROC_PATTERN=mooncake_benchmark_client bash mem_monitor.sh`，追加写 CSV | 无（仅 client 进程） |
| `urma` | UBPU 端口流量：mdcli 读取 flit 计数器，报表端差分计算带宽 | `env INTERVAL_S=1 SLOT=2 UBPUS="0 1" bash urma_monitor.sh`，追加写 CSV | 无（仅装有 mdcli 的节点启动） |

工具在节点上不可用（探测失败）→ `[WARN]` 跳过该工具，其余照常。**逐工具故障隔离**：任一
工具启动失败仅 `[WARN]` 跳过自身，不影响其余工具与场景。sudo 用法（profile 逐工具
`"sudo": false` 可关）：带 sudo 的工具启动前先探 `sudo -n true`，无 NOPASSWD 则明确
`[WARN]` 跳过；sudo 运行产生的 root 属主产物（perf.data）在 scp 前自动
`chown "$(id -un):$(id -gn)"` 归还 ssh 用户。

## 生命周期

- **启动**：场景开始（`trigger=whole_run`）或测量窗（`per_stage`：pipeline 为
  writer 启动前）→ 逐 involved 节点（`nodes=auto` 或显式列表）探测 → spdiag restart
  → 采样进程 nohup 后台，**pid 记入 `.state` 台账**（崩溃后可清；marker 记录预期
  cmdline 防误杀）
- **结束**（成功/失败都执行）：SIGINT 停采样（perf 优雅落盘）→ `spdiag show` →
  scp 收集至 `outputs/<ts>/logs/<配置名>/profiling/<节点>/`（spdiag 表同时生成文本打点树）→ 清台账
- **异常值自动报告**：run 结束提取 RESULT 行 > `outliers.threshold_ms` →
  `profiling/outliers.json`（时刻/key/时延/节点）+ `outliers.slices.txt`
  （每异常 ±`window_ms` 时间窗内各数据源的行切片）

## 异常值全链路追踪

时间窗切片源：bpftrace `.out` / spdiag `.txt`（按行内 `HH:MM:SS` 时间戳过滤）；
**perf.data 精确切片在节点侧原生支持**：`perf script -i perf.data --time <start>,<end>`。
跨节点切片受 NTP 偏差影响（同节点数据精确对齐）。

## 输出结构

```
outputs/<ts>/
├── profiling/                          # run 级异常值台账（跨配置聚合）
│   ├── outliers.json
│   └── outliers.slices.txt
└── logs/<配置名>/profiling/            # 每配置独立（多配置 run 互不覆盖）
    └── <节点>/
        ├── spdiag.show.txt
        ├── spdiag_tree.<接口>.txt      # 自动生成的文本打点树（树定义 configs/_spdiag_trees/）
        ├── perf.data / perf.data.err
        ├── runqlat.out / offcputime.out
        ├── mem.host.csv / mem.proc.csv # 整机/进程内存时间序列（mem 工具）
        ├── urma.flits.csv              # UBPU 端口 flit 计数器（urma 工具）
        └── monitor.config.json         # 生效的监控参数（阈值/间隔/slot/flit 字节）
```

内存与 URMA 监控数据由 `scripts/report.py` 的 `reduce_monitors()` 汇总进
`outputs/<ts>/logs/<配置名>/report/report.html` 的「节点监控」卡片：整机内存、
进程 RSS 曲线 + 摘要（峰值内存 / RSS 最大增幅进程「疑似泄露」/ 端口峰值带宽）。
无监控产物的旧 run 不会渲染该卡片；`--rebuild-report` 直接复用本地 CSV。

spdiag_tree.<接口>.txt：按 `configs/_spdiag_trees/*.json` 的 (point, module) 挂载
Avg/P999/P9999/Max/Ticks，自动计算残差（`|r|>1µs 且 >2%·parent.avg → ALERT`），
带树形符号可直接阅读；WebUI「运行结果」页渲染为可折叠/缩放树状图。
手动重生成：`python profiling/spdiag_tree.py --spdiag <表文件> --tree <树定义.json>`。
