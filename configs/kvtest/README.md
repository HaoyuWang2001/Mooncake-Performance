# configs/kvtest — mooncake_kvtest（C++ 原生压测）配置

`scenario: "kvtest"` 调用 mooncake-store 仓库 `benchmarks/kvtest` 编译出的
`mooncake_kvtest` C++ 二进制：HTTP 通知处理、通知队列、Mooncake 调用与指标
采集全部留在 C++ 内（无 Python 请求路径），用于对照 Python client 的
pipeline（rank/recall）结果。

> 需要包含 `start_paused`、`POST /start` 和 summary `elapsed_seconds` 的新版
> `mooncake_kvtest`；旧二进制会在编排器的 `/start` 健康门处明确失败。

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

编排器把本目录的场景配置翻译成 **每实例一份 kvtest JSON**（`role`/`listen_port`/
`target_qps`/`nodes`/`store` 全部由 cluster.json + global.json 派生），推送到每个
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
| `writer.num_threads` | 每进程 C++ 写管线线程数（默认 1） |
| `reader.num_threads` | 每进程 C++ 通知消费 worker 数（默认 4；纯 reader 进程同理） |
| `writer.notify_threads` | 出站 HTTP 通知线程池大小 |
| `batch_size` | 映射 kvtest `batch_keys_count` |
| `kvtest.key_pool_size` | `auto`（默认）= 按 Store 容量计算；0 = 无限新 key（YuanRong 行为）；>0 = 显式容量受限 key 池 |
| `kvtest.verify_level` | `off` / `size`（默认）/ `sample` / `full` |
| `duration` | 测量窗口秒数（kvtest 无 warmup 概念，残留 warmup 字段会被忽略并 WARN） |

## 结果

每实例输出
`<cfg>.<ds>.kvtest_<node>_<tag>.{log,glog,exitrc,summary.json,timeseries.csv,samples.csv}`
拉回 `outputs/<ts>/logs/<cfg>/`；`summary.json` 聚合进 `<cfg>.csv` 与
`benchmark_detail.csv`（`KVTEST_CSV_HEADER`，仅保留 TOTAL 行）。注意：
kvtest 无 etcd rendezvous；测量窗口从全部实例 `/health` 后的并行 `/start`
开始。每实例 QPS/吞吐使用 summary 中的实际 `elapsed_seconds`。

kvtest 在请求热路径中只把 `elapsed_us/operation/latency/status/bytes` 保存到
内存，`/stop` 后一次性写出 `latency_samples.csv`，不会逐请求刷盘。编排器将其
拉回为 `*.samples.csv`，`scripts/plot_latency.py` 按 operation 生成一请求一点的
`<实例>.<operation>.latency.png`；`notify_queue_wait` 使用
`<实例>.notify_queue_wait.queuewait.png`。横轴下方是测量开始后的秒数，上方是
request #i。`latency_timeseries.csv` 仍会保留和拉回作为聚合原始数据，但不再
生成窗口聚合 PNG；`--no-plot` 可关闭自动绘图。
