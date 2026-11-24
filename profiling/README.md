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
| `mem` | 整机内存（`numastat -m` 各 NUMA MemUsed；缺失时回退内核 `/sys/devices/system/node/node*/meminfo`）+ MemAvailable，以及进程内存（`/proc/<pid>/smaps_rollup` 的 Rss/Pss/Private/Swap），用于泄露观测。**默认开启** | `env INTERVAL_S=1 PROC_PATTERN=mooncake_benchmark_client bash mem_monitor.sh`，追加写 CSV | 无（仅 client 进程；无需 numactl） |
| `urma` | UBPU 端口流量：mdcli 读取 flit 计数器，报表端差分计算带宽 | `env INTERVAL_S=1 SLOT=2 UBPUS="0 1" [MDCLI_BIN="nsenter -t 1 -m -n -- mdcli"] bash urma_monitor.sh`，追加写 CSV | 无（ssh 模式仅装有 mdcli 的节点启动；k8s 走 monitor DaemonSet 借宿主 mdcli） |

工具在节点上不可用（探测失败）→ `[WARN]` 跳过该工具，其余照常。**逐工具故障隔离**：任一
工具启动失败仅 `[WARN]` 跳过自身，不影响其余工具与场景。sudo 用法（profile 逐工具
`"sudo": false` 可关）：带 sudo 的工具启动前先探 `sudo -n true`，无 NOPASSWD 则明确
`[WARN]` 跳过；sudo 运行产生的 root 属主产物（perf.data）在 scp 前自动
`chown "$(id -un):$(id -gn)"` 归还 ssh 用户。

## 生命周期

- **启动**：场景开始（`trigger=whole_run`）或测量窗（`per_stage`：pipeline/kvtest
  为 writer/实例启动前）→ 逐 involved 节点（`nodes=auto` 或显式列表）探测 → spdiag
  restart → 采样进程 nohup 后台，**pid 记入 `.state` 台账**（崩溃后可清；marker
  记录预期 cmdline 防误杀）
- **结束**（成功/失败/STOP 都执行）：SIGINT 停采样（perf 优雅落盘）→ `spdiag show` →
  scp 收集至 `outputs/<ts>/logs/<配置名>/profiling/<节点>/`（spdiag 表同时生成文本打点树）→ 清台账
- **异常值自动报告**：run 结束提取 RESULT 行 > `outliers.threshold_ms` →
  `profiling/outliers.json`（时刻/key/时延/节点）+ `outliers.slices.txt`
  （每异常 ±`window_ms` 时间窗内各数据源的行切片）

三个场景（basic / pipeline / kvtest）都接入了同一套生命周期；kvtest 会把 mem 的
`proc_pattern` 默认值自动改成 kvtest 二进制名（`mooncake_kvtest`），profile.json
里显式写了的 pattern 优先。

## k8s 节点级监控（monitor DaemonSet）

k8s 模式下 run 的"节点"其实是 pod：直接在 client pod 里跑采样器会变成 **pod 级**
（每 pod 一份、进程视野只有本 pod、镜像里也没有宿主机的 mdcli）。启用 mem/urma 任一
监控时，编排器会为本次 run 额外建一个 **monitor DaemonSet**（`benchmark.py`
`ensure_k8s_monitor_daemonset`，每节点 1 个 pod，随 run namespace 创建/删除）：

- `hostPID: true`：容器 `/proc` 即宿主进程表 → `mem.proc` 看到整机跨 pod 进程
- `privileged: true` + `_host_exec` 包装（默认 `nsenter -t 1 -m -n --`）：探测/调用
  宿主安装的 mdcli（`MDCLI_BIN`），镜像里不装这些宿主工具；`chroot /host` 是低权限
  兜底（`cluster.json: k8s.monitor_host_exec`，空串 = 仅容器内工具）
- 挂载与 benchmark pod **相同的 hostPath**（`<host_log_root>/<ts>` → 
  `<pod_workspace>/logs/<ts>`）：脚本与 CSV 落节点磁盘，收集链路与报表 reduce 零改动
- 采样循环进程留在容器内（仅每次 mdcli 调用跨界），pod 删除/namespace 清理即回收，
  不会在宿主留下孤儿进程

宿主内存（`mem.host.csv`）与 URMA 端口（`urma.flits.csv`）因此是**每节点一份**；
报表 `monitor.config.json: node_ip` 取宿主 IP，同主机多 pod 的宿主内存曲线自动去重。
DaemonSet 起不来时自动回退到旧的 pod 级监控（`[WARN]`，不影响场景）。

模板：`configs/_k8s/templates/monitor-daemonset.yaml.tpl`（`cluster.json:
k8s.monitor_template` 可换）。**上真实集群前先做一次 mdcli 可达性烟测**：
```bash
kubectl debug node/<node> -it --image=<runtime镜像>   # 或 privileged+hostPID pod
chroot /host command -v mdcli                          # ① 宿主有 mdcli
chroot /host bash -c 'echo "display diagnose/forward-enp-informations/forward-enp-information[slot=\"2\"][ubpu=0][enp-name=\"get port statistic all\"]" | mdcli'   # ② 输出 7 列计数
command -v nsenter                                     # ③ 镜像内有 nsenter（否则用 chroot 兜底）
```
②必须能打出 `$1/$4/$5` 为数字的计数行（`urma_monitor.sh` 的解析口径），连跑两次确认
计数器递增。


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
