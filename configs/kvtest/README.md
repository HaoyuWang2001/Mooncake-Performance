# configs/kvtest — mooncake_kvtest（C++ 原生压测）配置

`scenario: "kvtest"` 调用 mooncake-store 仓库 `benchmarks/kvtest` 编译出的
`mooncake_kvtest` C++ 二进制：Writer→Reader 通知使用 YLT `coro_rpc`，HTTP
仅保留控制接口；通知队列、Mooncake 调用与指标采集全部留在 C++ 内（无
Python 请求路径），用于对照 Python client 的 pipeline（rank/recall）结果。

> 需要包含 `start_paused`、`POST /start|pause|drain|reset|arm|resume` 和 summary
> `elapsed_seconds` 的新版 `mooncake_kvtest`；开启 warmup 时，旧二进制会在
> `/pause` 控制门处明确失败。

## 运行方式（与现有场景一致）

```bash
# k8s 通道
python benchmark.py --k8s -f configs/kvtest/smoke/kvtest_smoke.json
# ssh 通道
python benchmark.py --ssh -f configs/kvtest/demo/simple_kvtest.json
# WebUI：配置树选 configs/kvtest/ 下任一场景，launch mode = single
```

## rank+recall 并行

`kvtest_rank_smoke.json` / `kvtest_recall_smoke.json` 分别对齐 pipeline 的
rank_smoke / recall_smoke 负载参数（105qps/8MB/notify10 vs 35qps/3.5MB/notify1），
`work_mode` 取 rank / recall 供 `run_rank_recall.py` 分组：

两者保留原有 Pod 数量并使用 `client.process_mode=split`：rank 每节点 2 个
client Pod，recall 每节点 6 个 client Pod；每个 Pod 内分别启动 1 个 writer
进程和 1 个 reader 进程。rank 使用端口 9101/9201，recall 使用
9301/9401。6 节点时 rank 共 12 个 Pod/12 对进程，recall 共 36 个 Pod/36 对进程。

两者的 C++ 线程预算对齐 YuanRong kvtest pipeline 基线：4 个 writer pipeline
线程、`num_total_threads=16`，因此每实例有 12 个 reader notification worker；
`notify_queue_max=65536`、`batch_size=1`、`metrics_interval_ms=3000` 和 lane jitter
也与 YuanRong 基线一致。

### 真机 SSH：一行命令

rank 与 recall 可以使用同一份 worker `cluster.json`（端口已经错开），但必须使用
两套互相独立的 Store/ETCD global 配置；两个任务都会执行全量 remove-keys，不能
指向同一个 Store。首次使用先复制模板并分别填写两套地址：

```bash
cp configs/_ssh/global.example.json configs/_ssh/global.rank.json
cp configs/_ssh/global.example.json configs/_ssh/global.recall.json
```

准备好 `cluster.json`、两个 global 和 `profile.json` 后，一行运行：

```bash
python3 scripts/run_rank_recall.py --ssh \
  -f configs/kvtest/smoke/kvtest_rank_smoke.json,configs/kvtest/smoke/kvtest_recall_smoke.json \
  --rank-cluster configs/_ssh/cluster.json --rank-global configs/_ssh/global.rank.json \
  --recall-cluster configs/_ssh/cluster.json --recall-global configs/_ssh/global.recall.json \
  --profile configs/_ssh/profile.json
```

真机共用节点时，rank 使用 HTTP 端口 9101/9201 和传输端口基址 12345；recall
使用 HTTP 端口 9301/9401 和传输端口基址 13345，避免两个并行任务端口冲突。

### K8s CLI

```bash
python3 scripts/run_rank_recall.py --k8s \
  -f configs/kvtest/smoke/kvtest_rank_smoke.json,configs/kvtest/smoke/kvtest_recall_smoke.json \
  --rank-cluster configs/_k8s/cluster.16node-rank.json --rank-global configs/_k8s/global.rank.json \
  --recall-cluster configs/_k8s/cluster.16node-recall.json --recall-global configs/_k8s/global.recall.json \
  --profile configs/_k8s/profile.json
```

### K8s WebUI

1. 启动前把 controller 密码放入环境变量（使用 SSH key 时不需要
   `passwd_env`），再启动 WebUI：

   ```bash
   export MC_K8S_16NODE_CONTROLLER_PASSWORD='<controller-password>'
   python3 -m webui.backend.main --bind 0.0.0.0 --port 8080
   ```

2. 浏览器打开 `http://<WebUI主机>:8080`，进入“运行”。
3. 选择 `k8s`，启动方式选择“Rank + Recall 并行”。
4. rank 选择 `cluster.16node-rank.json` + `global.rank.json`；recall 选择
   `cluster.16node-recall.json` + `global.recall.json`；profile 选择
   `configs/_k8s/profile.json`。
5. 点击“选择配置”，只勾选 `kvtest_rank_smoke.json` 和
   `kvtest_recall_smoke.json`，确认后点击 `RUN`。

WebUI 会按 `work_mode` 自动分组，在两边全部到达 `/start` 门后同时释放测试。

kvtest 进程以 `start_paused=true` 启动；全部实例 `/health` 后，编排器先进入
cross-child barrier，再并行 `POST /start`，由 C++ 清空启动阶段指标并释放
writer。这样 rank/recall 的 Store 初始化耗时不同也不会污染窗口对齐。注意并行建议单 data_size 场景
（barrier 是一次性的，后续 sweep 条目不再对齐，与 pipeline 并行语义一致）。

编排器把本目录的场景配置翻译成 **每实例一份 kvtest JSON**（`role`/
`listen_port`/`notify_rpc_port`/`target_qps`/`nodes`/`store` 全部由
cluster.json + global.json 派生），推送到每个
pod/节点后经 `scripts/run_kvtest.sh` 启动；运行窗口 = `duration`，到点后编排器
`POST /stop`，C++ 停机路径落盘最终 `summary.json`。

## 二进制部署

- `kvtest.binary`（公共继承配置为 `/home/Mooncake/bin/kvtest`，单个场景可覆盖；
  字段未配置时默认 `mooncake_kvtest`）：节点/pod 上的二进制路径，从 PATH
  或镜像内解析；
- `kvtest.binary_local`（可选）：编排器本地二进制路径；设置后自动 scp/kubectl cp
  推送到每个目标再执行。若二进制同级 `lib/` 或上一级的 `lib/` 中存在 `.so`，
  会一并推送并由 runner 加入 `LD_LIBRARY_PATH`。ssh 节点落在 scripts 目录，k8s
  pod 因 scripts 只读落在 pod 工作区 `kvtest-bin/`；推送路径优先于
  `kvtest.binary`。

## 配置形态

- **dual 模式**（`client` 块出现且 `process_mode` 缺省或为 `dual`）：每个 Pod
  启动一个同时承担 writer + reader 的进程，单端口；
  qps 按节点内进程数整数拆分（如 105 = 53 + 52）；
- **Pod 内分进程模式**（`client.process_mode=split`）：仍创建
  `client.count_per_node × N` 个 Pod，但每个 Pod 启动一个 reader 进程和一个
  writer 进程；两者使用 `reader.port_base` / `writer.port_base` 两个端口；
- **完全分离模式**（无 `client` 块）：writer/reader 各自创建 Deployment 和 Pod，
  数量分别为各自的 `count_per_node × N`。

后两种模式都会先启动全部 reader 并通过 `/health` 健康门，再启动 writer。
Pod 内分进程模式中，`local_buffer_size` 对每个进程分别生效，因此一个 Pod 的
内存预算至少要覆盖两个 client buffer 加进程额外开销。

K8s split 场景可用 `kvtest.numa_bindings` 按节点内 client 序号分别指定
writer/reader 的 NUMA 节点。runner 使用 `numactl` 的 `--cpunodebind=<node>` 和
`--membind=<node>` 同时绑定 CPU 与内存；目标节点必须位于 kubelet 为该 Pod
生成的 cgroup `cpuset.cpus` / `cpuset.mems` 允许范围内。rank smoke 的两对进程
映射为 `0/1`、`2/3`，recall smoke 的前三对映射为 `0/1`、后三对映射为
`2/3`。

| 字段 | 语义 |
| --- | --- |
| `client.count_per_node` | dual/split 模式每节点目标 Pod 数 |
| `client.process_mode` | `dual`（默认，单进程）或 `split`（同 Pod 两个独立进程） |
| `base_qps` / `qps_multiple` | 单节点 QPS 基数 × 倍率，集群总量 = per-node × N |
| `notify_count` | 每次写随机通知的 reader 数（dual：≤ client 数 - 1；split/完全分离：≤ reader 进程数） |
| `writer.operation` | `upsert` / `upsert_from` / `batch_upsert_from` / `remove` |
| `reader.operation` | `get_buffer` / `batch_get_buffer` / `exist` |
| `writer.port_base` / `reader.port_base` | split/完全分离模式的进程 HTTP 端口，必须不同 |
| `kvtest.local_port_base` | 每个节点/Pod 的 Mooncake 传输监听端口基址；真机并行场景之间必须错开 |
| `kvtest.numa_bindings` | K8s split 模式按 client 序号指定 `{writer, reader}` NUMA 节点，并同时绑定 CPU/内存 |
| `writer.num_threads` | 每进程 C++ 写管线线程数（默认 1） |
| `reader.num_threads` | 每进程 C++ 通知消费 worker 数（默认 4；纯 reader 进程同理） |
| `writer.notify_threads` | 出站 RPC 任务线程池大小 |
| `kvtest.notify_rpc_port_offset` | YLT RPC 端口相对 HTTP 控制端口的偏移，默认 10000 |
| `kvtest.notify_rpc_threads` | 每实例 YLT RPC server 的 I/O 线程数，默认 4 |
| `kvtest.notify_rpc_connections_per_peer` | 每个 Reader 的 YLT 长连接缓存容量，默认 4；突发时 YLT 可临时创建额外连接 |
| `kvtest.notify_rpc_reuse_limit` | YLT 连接开始 pipeline 复用的并发阈值，默认 4 |
| `kvtest.notify_rpc_timeout_ms` | 建连和单次 RPC 请求超时，默认 3000 ms |
| `batch_size` | 映射 kvtest `batch_keys_count` |
| `kvtest.key_pool_size` | `auto`（默认）= 按 Store 容量计算；0 = 无限新 key（YuanRong 行为）；>0 = 显式容量受限 key 池 |
| `kvtest.verify_level` | `off` / `size`（默认）/ `sample` / `full` |
| `warmup.duration` | warmup 窗口秒数，默认 5；`--no-warmup` 强制设为 0 |
| `duration` | 正式测量窗口秒数，不包含 warmup 和队列排空时间 |

## 结果

成功运行的最终目录保留每实例 `log`/`glog`、请求级 PNG、**HTML 报表
（map-reduce 产物，见下节）**、run 级 `<cfg>.csv` 和 `benchmark_detail.csv`，
以及 `summary.json` / `samples.csv`。后两者是 `--rebuild-report` 补跑 map 的
**唯一输入**，因此默认保留；确认系统稳定后可设
`MC_KVTEST_CLEAN_INTERMEDIATES=1`，在 run 全部成功且绘图成功后清掉它们
（失败或绘图失败时该开关也不会删除）。注意：
kvtest 无 etcd rendezvous；测量窗口从全部实例 `/health` 后的并行 `/start`
开始。配置 warmup 时，`/start` 先释放 warmup；到时后编排器依次执行
`/pause`（停 writer 并排空出站通知）、`/drain`（排空 reader 入站队列）、
`/reset`（清除 warmup metrics/samples）、`/arm`（启动正式计时）和 `/resume`
（进入正式测量）。每实例 QPS/吞吐使用 summary 中从 arm 后开始计算的实际
`elapsed_seconds`，不包含跨任务 barrier 等待。
延迟统计只使用 `status=OK` 的请求；TOTAL 行会先合并全部实例的成功请求样本，
再重新计算 avg/max/P50/P90/P99/P999/P9999，因此 max 是真正的全局最大值，
分位数也不是各实例分位数的加权平均。旧二进制没有请求样本时才回退为近似值并
打印 WARN。
CSV 中 `qps=success/wall_s`，直接表示实测成功 QPS；TOTAL 行汇总所有并发
实例的成功 QPS。汇总延迟列为 min/avg/P50/P90/P95/P99/P99.9/P99.99/max，
并记录 `>2000us`、`>5000us`、`>6000us`、`>7000us`、`>8000us`、
`>9000us`、`>10000us`、`>20000us` 的成功请求数。

kvtest 在请求热路径中只把 `elapsed_us/operation/latency/status/bytes` 保存到
内存，`/stop` 后一次性写出 `latency_samples.csv`，不会逐请求刷盘。编排器将其
拉回为 `*.samples.csv`，`scripts/plot_latency.py` 按 operation 生成一请求一点、
并按完成时间顺序连接的点线图
`<实例>.<operation>.latency.png`；`notify_queue_wait` 使用
`<实例>.notify_queue_wait.queuewait.png`。横轴下方是测量开始后的秒数，上方是
request #i。旧的 `latency_timeseries.csv` 已删除，不再生成或拉回窗口聚合数据；
`--no-plot` 可关闭这份自动绘图（PNG），HTML 报表不受影响（由 `--no-report` 单独控制）。

## HTML 报表（map-reduce）

kvtest 与 Python pipeline 共用同一套报表链路，map 阶段把 C++ 的请求级样本转换成
标准的 `<log>.stats.json`，reduce 阶段（`scripts/report.py`）渲染与 pipeline 同构的
单元报表：

1. **map**（编排器本地，场景结束、CSV 汇总后自动执行）：
   `scripts/logstats.py --kvtest-samples <实例>.samples.csv --start-epoch <测量起点
   epoch> --summary <实例>.summary.json --role writer|reader`。
   `samples.csv` 的 `elapsed_us` 是相对**测量起点**的偏移：无 warmup 时即 `/start`；
   配置 warmup 时 C++ 在 `/reset` 清空样本、`/arm` 启动正式计时，编排器在 `/arm`
   完成后刷新 epoch（`/start` 时刻先记录一次，warmup 中途 STOP 也能得到正确的
   warmup 时间轴）。报表时间轴、1s/1min 桶与慢点列表因此都是绝对时刻；每个实例
   按 operation 拆成 **writer**（`upsert*`/`remove`）与 **reader**
   （`get*`/`exist`）两份产物（dual 实例同时有两边，对应 Python dual client 的
   writer.log/reader.log）。
2. **reduce**（同一进程内）：`report.py` 按 data_size 汇总这些 stats，生成
   `logs/<cfg>/report/<size>/report.html` + `data.js` + `meta.json`，并在 run 结束
   时生成 `report/overview.html`。版式与 pipeline 一致：writer/reader 角色页签、
   角色概要、QPS/时延曲线、客户端/节点对象树、错误分布、慢请求 Top-N、异常窗口，
   若 `.glog` 含 SDK trace 还会出现 C++ 全链路卡片。
3. **重建**：`python benchmark.py --rebuild-report <ts>`。失败 run 保留了
   `samples.csv` 时，会自动先补跑 map 再重建报表。运行期会把
   `iid→节点/节点数/每个 size 的测量起点 epoch`写进
   `logs/<cfg>/kvtest_report_meta.json`（旁挂文件，成功后也保留），重建据此
   恢复真实节点名与精确时间轴；该文件缺失时才回退为
   `summary.json` 的 mtime 减去 `elapsed_seconds` 估算。

报表参数卡沿用 pipeline 字段（场景/协议/工作模式/QPS/notify/操作/节点数/客户端
总数/KV 配置名等），并标记「运行模式 = kvtest」。注意：

- `--no-report` 关闭 HTML 报表；`--no-plot` 只关 PNG 绘图（与 pipeline 一致，两者独立）；
- 报表生成是 WARN-only，失败不影响压测结论；
- 成功清理只删除 `summary.json`/`samples.csv`，`*.stats.json` 与
  `report/` 目录保留；
- **只存在于 summary 的指标也已进 HTML**：reader 的「值校验失败」
  （`verify_failures`，数据正确性信号）与「队列等待均/峰」
  （`notify_queue_wait` 的 avg/max）由 map 从 `summary.json` 提升进 reader
  角色概要；通知发送侧指标（YLT 为 `notify_rpc`，旧二进制为 `notify_http`
  的提交/失败/平均/最大）仍只在 CSV；
- **节点日志体积取真实日志**：stats 的 `meta.bytes` 优先用该实例
  `log + glog` 体积（另存 `meta.samples_bytes` 为样本文件体积），
  run 总览的“节点日志量”不再被样本 CSV 低估；
- **STOP（Ctrl+C）也出部分报表**：中断时先向实例发 `/stop`（C++ 只在
  这里落盘样本）、等待退出、拉回样本与 summary，再做 map-reduce；
  报表标注 `early_stop`。收集中按 Ctrl+C 第二次仍为强制退出。

kvtest 实例文件使用紧凑名称
`kv<配置哈希>.i<instance>_p<port>.kvtest_<size>.*`。K8s Pod 全名不再进入
日志、summary/sample 或 PNG 文件名；instance 到真实节点/Pod 的映射保存在运行时
配置中并写入最终 CSV，从而避免 Windows 260/264 字符路径限制。旧版
`<cfg>.<node>_i...` 文件仍可被汇总和绘图脚本读取。
