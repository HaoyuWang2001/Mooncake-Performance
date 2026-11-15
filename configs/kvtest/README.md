# configs/kvtest — mooncake_kvtest（C++ 原生压测）配置

`scenario: "kvtest"` 调用 mooncake-store 仓库 `benchmarks/kvtest` 编译出的
`mooncake_kvtest` C++ 二进制：HTTP 通知处理、通知队列、Mooncake 调用与指标
采集全部留在 C++ 内（无 Python 请求路径），用于对照 Python client 的
pipeline（rank/recall）结果。

## 运行方式（与现有场景一致）

```bash
# k8s 通道
python benchmark.py --k8s -f configs/kvtest/smoke/kvtest_smoke.json
# ssh 通道
python benchmark.py --ssh -f configs/kvtest/demo/simple_kvtest.json
# WebUI：配置树选 configs/kvtest/ 下任一场景，launch mode = single
```

## rank+recall 并行（k8s）

`kvtest_rank_smoke.json` / `kvtest_recall_smoke.json` 分别对齐 pipeline 的
rank_smoke / recall_smoke 形态（105qps/8MB/notify10 vs 35qps/3.5MB/notify1），
`work_mode` 取 rank / recall 供 `run_rank_recall.py` 分组：

```bash
python scripts/run_rank_recall.py \
  -f configs/kvtest/smoke/kvtest_rank_smoke.json,configs/kvtest/smoke/kvtest_recall_smoke.json \
  --rank-cluster configs/_k8s/cluster.json --rank-global configs/_k8s/global.rank.json \
  --recall-cluster configs/_k8s/cluster.json --recall-global configs/_k8s/global.recall.json \
  --profile configs/_k8s/profile.json
```

kvtest 无 /start：dual 进程从 boot 即写，编排器把 cross-child barrier 放在
**写侧 fire 之前**（prepare 全部完成、remove-keys 全部完成后互相等待），
并以线程池并行 fire 收紧两侧窗口起点。注意并行建议单 data_size 场景
（barrier 是一次性的，后续 sweep 条目不再对齐，与 pipeline 并行语义一致）。

编排器把本目录的场景配置翻译成 **每实例一份 kvtest JSON**（`role`/`listen_port`/
`target_qps`/`nodes`/`store` 全部由 cluster.json + global.json 派生），推送到每个
pod/节点后经 `scripts/run_kvtest.sh` 启动；运行窗口 = `duration`，到点后编排器
`POST /summary`（落盘 summary.json）再 `POST /stop`（优雅退出）。

## 二进制部署

- `kvtest.binary`（默认 `mooncake_kvtest`）：节点/pod 上的二进制路径，从 PATH
  或镜像内解析；
- `kvtest.binary_local`（可选）：编排器本地二进制路径；设置后自动 scp/kubectl cp
  推送到每个目标再执行（无需打进镜像）。ssh 节点落在 scripts 目录，k8s pod 因
  scripts 只读落在 pod 工作区 `kvtest-bin/`；推送路径优先于 `kvtest.binary`。

## 配置形态（继承 pipeline 的部署拓扑）

- **dual 模式**（`client` 块出现）：每进程 = writer + reader，单端口；
  qps 按节点内进程数整数拆分（如 105 = 53 + 52）；
- **分离模式**：writer/reader 各自 `count_per_node` × N 部署，reader 先启动并通过
  `/health` 健康门后再启动 writer（kvtest 的 writer 进程启动即写）。

| 字段 | 语义 |
| --- | --- |
| `base_qps` / `qps_multiple` | 单节点 QPS 基数 × 倍率，集群总量 = per-node × N |
| `notify_count` | 每次写随机通知的 reader 数（dual：≤ client 数 - 1） |
| `writer.operation` | `upsert` / `upsert_from` / `batch_upsert_from` / `remove` |
| `reader.operation` | `get_buffer` / `batch_get_buffer` / `exist` |
| `writer.num_threads` | 每进程 C++ 写管线线程数（默认 1） |
| `reader.num_threads` | 每进程 C++ 通知消费 worker 数（默认 4；纯 reader 进程同理） |
| `writer.notify_threads` | 出站 HTTP 通知线程池大小 |
| `batch_size` | 映射 kvtest `batch_keys_count` |
| `kvtest.key_pool_size` | 0 = 无限新 key（YuanRong 行为）；>0 = 容量受限 key 池 |
| `kvtest.verify_level` | `off` / `size`（默认）/ `sample` / `full` |
| `duration` | 测量窗口秒数（kvtest 无 warmup 概念，残留 warmup 字段会被忽略并 WARN） |

## 结果

每实例输出 `<cfg>.<ds>.kvtest_<node>_<tag>.{log,glog,exitrc,summary.json,timeseries.csv}`
拉回 `outputs/<ts>/logs/<cfg>/`；`summary.json` 聚合进 `<cfg>.csv` 与
`benchmark_detail.csv`（`KVTEST_CSV_HEADER`，仅保留 TOTAL 行）。注意：
kvtest 无 etcd rendezvous、无 `/start` 同步门——测量窗口从全部实例
`/health` 就绪后起算，进程启动斜坡包含在窗口内。
