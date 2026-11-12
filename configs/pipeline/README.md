# configs/pipeline — 精排/召排（生产-消费）配置

writer 按 QPS 写入并通知随机 reader 子集，reader 消费读取。

**运行架构（etcd rendezvous）**：reader 自注册进 etcd（`mcbench/<run_id>/readers/<i>`）
→ 编排器等全部 reader 就绪 → writer 从 etcd 拉全量 reader 列表并注册自身 → 编排器
HTTP `/start` 统一触发 → warmup + 测量。要求 global.json `etcd_endpoints` 非空
（与 master_type 无关，同一 etcd 兼作协调面）；屏障超时 `rendezvous_timeout_s`（默认 120）。

**面向集群**：不再按角色指定节点（`client_nodes` 已删除）。writer/reader 进程数 =
`count_per_node × N`，N = cluster.json 全部节点（SSH）/ 全部 Ready 节点（k8s，master 计入；
每角色一个 Deployment，pod=进程，全部同角色 pod 用同一端口）。目录 `1/2/4-nodes/`
为历史档位归档，n{N} 仅作标注。

配置按 **档位 → work_mode** 两级目录组织，每层 `_configs.json` 逐层继承。

## 目录结构与继承链

```
configs/pipeline/
├── _configs.json               # 根基座（scenario/protocol/key_prefix/notify_pipeline/warmup/duration/batch_size）
├── rank/                       # 精排档位（42 个，平铺）
│   ├── _configs.json           # work_mode=rank, base_qps=105, data_size=8MB, writer/reader 骨架
│   ├── rank_{pulse,balanced}_1w{1,2,4,8,10,16}r_{1,2,3}x.json
│   └── rank_{pulse,balanced}_2w10r_{1,2,3}x.json      # 真双写（writer.count_per_node=2）
├── recall/                     # 召排（6 个，平铺，恒 1w1r）
│   ├── _configs.json
│   └── recall_{pulse,balanced}_1w1r_{1,2,3}x.json
├── smoke/                      # 双角色最小场景集（client 块选择 dual 模式）
│   ├── rank_smoke.json         # 105qps/1x/1写10读/client N*2/store N*1/8MB（需 N>=6）
│   └── recall_smoke.json       # 35qps/1x/1写1读/client N*6/store N*2/3.5MB
└── demo/                       # 传统分离角色冒烟（2 个）
```

继承链：根基座 → `rank|recall/_configs.json` → 用例叶子（仅档位差异字段）。
历史 `1/2/4-nodes/` 档位目录已废止（N 为集群节点数，与配置无关）；叶子文件名的
`_n{N}` 后缀已去除。

## 档位矩阵（rank）

| 档位 | notify_count | writer cpn | reader cpn（canonical，旧 2 节点参考） |
| --- | --- | --- | --- |
| 1w1r / 1w2r | 1 / 2 | 1 | 1 |
| 1w4r / 1w8r | 4 / 8 | 1 | 2 / 4 |
| 1w10r / 1w16r | 10 / 16 | 1 | 5 / 4 |
| 2w10r | 10 | 2（真双写） | 5 |

## 双角色模式（smoke/）

`client` 块出现即选择 dual 部署：`{"count_per_node": X, "port_base": 9301}`。每个
client 进程 = 1 writer + 1 reader（单端口四 endpoint）；notify 抽样排除自身，
**校验条件 notify_count ≤ client 数 - 1**；`store_count_per_node` 为记录性元数据
（进 CSV `store_count` 列，store 由外部部署）。qps 为单节点语义：每 client =
节点速率 / cpn。

**key 空间**：pipeline **没有 `key_count` 参数**（残留字段 WARN+忽略）——可写量完全由 store
容量决定：`floor(effective × 0.9 / data_size / writers)`，经 `--key-count` 恒注入（写满回绕，
put 家族先删后写、upsert_from 覆盖写）。**容量解析**：探测恒执行（B = master metrics
`segment_total_capacity_bytes`）；配置 `store_total_size`（A）与 B 同时存在时
**effective = min(A, B)**（A>B 用满集群、A<B 只用部分），单源直用，双缺场景 FAIL。
启动摘要与场景头（资源创建前）会打印 configured/probed/effective 与每 size 的 per-writer key 数。

**日志分离**：dual client 的 writer/reader 输出写入两个日志
（`*.dual_<node>_c<id>_p<port>.writer.log` / `.reader.log`），CSV 与 latency/queue-wait
绘图按角色分开，不再混在一起。

## writer 块

| 字段 | 语义 |
| --- | --- |
| `count_per_node` | 每节点 writer 进程数 |
| `operation` | 写接口，可选值见下表 |
| `notify_threads` | notify 发送线程池大小 |
| `notify_async` | true 时写入只提交到发送队列，不逐批等待 HTTP 响应；默认 false |
| `notify_queue_size` | 每个 writer 待发送 HTTP 请求的队列容量，不含正在发送的请求；rank 默认 8192，满额时背压、不丢弃 |
| `notify_max_inflight` | 旧配置兼容字段：未设置 notify_queue_size 时作为队列容量，默认 256 |
| `port_base` | writer HTTP `/start` 触发端口段，默认 9101；节点内编号（第 i 个进程用 port_base+i；k8s 全部 pod 同端口） |

**writer.operation 可选值**：

| 值 | SDK 调用 | 计时粒度 | 说明 |
| --- | --- | --- | --- |
| `put` | `store.put(key, value)` | 每 key 一次计时调用 | 基础单写 |
| `put_batch` | `store.put_batch(keys, values)` | 整批一次计时调用 | 批大小 = `batch_size` |
| `put_from` | `store.put_from(key, ptr, size)` | 每 key 一次计时调用 | 从已注册本地缓冲写入；writer 启动时注册页对齐缓冲（value 写入缓冲为未计时准备），注册失败即退出 |
| `upsert_from` | `store.upsert_from(key, ptr, size)` | 每 key 一次计时调用 | 零拷贝插入或覆盖写；pipeline 下 key 空间由 store 容量推导（写满回绕覆盖，不做删除） |

### reader 块

| 字段 | 语义 |
| --- | --- |
| `count_per_node` | 每节点 reader 进程数（池 = count_per_node × 节点数，须 ≥ notify_count） |
| `operation` | 读接口，可选值见下表 |
| `num_threads` | 单进程消费线程数 |
| `port_base` | reader HTTP `/notify` 端口段，默认 9201；节点内编号（第 i 个进程用 port_base+i；k8s 全部 pod 同端口） |

**reader.operation 可选值**：

| 值 | SDK 调用 | 计时粒度 | 说明 |
| --- | --- | --- | --- |
| `get` | `store.get(key)` | 逐 key | 返回内容与 `value.bin` 比对校验（MISS/MISMATCH） |
| `get_batch` | `store.get_batch(keys)` | 整批一次计时调用 | 批 = notify 批内全部 key |
| `get_into` | `store.get_into(key, ptr, size)` | 逐 key | reader 启动时注册本地缓冲（页对齐 mmap），数据读入用户态缓冲 |
| `get_buffer` | `store.get_buffer(key)` | 逐 key | 返回 handle（ptr/size），不注册缓冲；校验 size 与内容前缀 |

### notify_pipeline 可选步骤

| 步骤 | 行为 |
| --- | --- |
| `exist` | 读前对批内每个 key 做**未计时** `store.is_exist()` 预检查；任一不存在 → 该批记 `MISS/MISMATCH` 并跳过读取 |
| `get` / `getBuffer` | **被忽略的占位名**——数据接口完全由 `reader.operation` 决定 |

默认 `["get"]` 实际不产生任何预检行为；需要存在性预检时应写 `["exist"]`。

## 各层字段

仅罗列每层出现的字段（语义见「配置总览」，取值见「resolved 全参数语义」）：

| 层 | 字段 |
| --- | --- |
| 根基座 `_configs.json` | `scenario` / `protocol` / `key_prefix` / `notify_pipeline` / `warmup` / `duration` / `batch_size` |
| `rank/_configs.json` | `work_mode` / `base_qps` / `data_size` / `writer{count_per_node, operation}` / `reader{operation, num_threads}` |
| `recall/_configs.json` | `work_mode` / `base_qps` / `notify_count` / `writer{count_per_node, operation, notify_threads}` / `reader{operation, num_threads, count_per_node}` / `data_size` |
| rank 用例 | `traffic_mode` / `qps_multiple` / `notify_count` / `writer.notify_threads` / `reader.count_per_node` |
| recall 用例 | `traffic_mode` / `qps_multiple` |
| smoke/ 用例 | 自带全字段（`client` 块 + 业务参数，不参与继承链） |
| `demo/` | 自带全字段（不参与继承链） |

档位矩阵与双角色说明见上文「档位矩阵（rank）」「双角色模式（smoke/）」两节。

## resolved 全参数语义（业务模型）

| 参数 | rank | recall | 语义 |
| --- | --- | --- | --- |
| `base_qps` × `qps_multiple` | 105×1/2/3 | 35×1/2/3 | **单节点**写入 QPS，节点内全部 writer 均分（每 client = 节点速率 / cpn；集群总量 = × N） |
| `traffic_mode` | pulse/balanced | pulse/balanced | 脉冲（每秒窗口突发）/ 均衡（匀速节流） |
| `notify_count` | =档位读者数 | 1 | 每批写后随机通知的 reader 数 |
| `writer.notify_threads` | =notify_count | 1 | notify 发送线程池 |
| `writer.count_per_node` | 1 | 同左 | 2w10r 档为 2（每节点） |
| `reader.count_per_node` | 按档位表 | 1 | 池 = cpn × N |
| `reader.operation` / `num_threads` | get / 1 | get / 1 | 层基座固定（可选值语义见「配置总览」） |
| `data_size` | ["8MB"] | ["3.5MB"] | 每 size 完整跑一轮写读流水 |

## 运行方式

```bash
python benchmark.py --config-dir configs/pipeline                       # 递归全部（含 smoke/ demo/）
python benchmark.py --config-dir configs/pipeline/rank                  # rank 全部档位
python benchmark.py --config-dir configs/pipeline/smoke                 # 双角色最小场景集
python benchmark.py --config-dir configs/pipeline/demo                  # 冒烟（demo 目录）
```
