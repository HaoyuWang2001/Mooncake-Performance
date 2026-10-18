# configs/pipeline — 精排/召排（生产-消费）配置

writer 按 QPS 写入并通知随机 reader 子集，reader 消费读取。配置按
**reader 参与节点数 → work_mode** 两级目录组织，每层 `_configs.json` 逐层继承。

## 目录结构与继承链

```
configs/pipeline/
├── _configs.json               # 根基座（全部 102 用例共用）
├── README.md
├── demo/                       # 冒烟（--demo，自带全字段）
├── 1-nodes/                    # reader 侧 1 个节点
│   ├── _configs.json           # writer/reader 骨架（client_nodes 按层）
│   ├── rank/_configs.json      # work_mode=rank + base_qps=105 + data_size
│   ├── rank/   (24 用例)
│   ├── recall/_configs.json    # work_mode=recall + base_qps=35 + 常量字段 + data_size
│   └── recall/ (6 用例)
├── 2-nodes/ (36: rank 30 + recall 6)
└── 4-nodes/ (36: rank 30 + recall 6)
```

合并顺序：根 → x-nodes → rank|recall → 用例（dict 深合并，下层覆盖上层，
**用例永远最优先**）。`_configs.json` 不作为用例运行，收集时自动跳过。

## 各层字段

**根基座**（所有用例共用）：

| 字段 | 默认值 | 语义 |
| --- | --- | --- |
| `scenario` | `"pipeline"` | 场景类别 |
| `protocol` | `"ub"` | 传输协议 |
| `key_prefix` | `"mc_bench"` | key 前缀 |
| `notify_pipeline` | `["get"]` | 通知后的预检步骤（exist） |
| `warmup` | `{duration: 5}` | 链路预热秒数（带标记不计入指标） |
| `duration` | `30` | 压测时长（秒） |
| `batch_size` | `1` | writer 每批 key 数 |
| `numa_bind` | 已移交 `configs/global.json`（默认 true） | 用例/层级仍可显式覆盖 |

**x-nodes 层**（writer/reader 骨架，client_nodes 按层不同）：

```json
{"writer": {"count_per_node": 1, "operation": "put_batch", "client_nodes": ["writer_node1"]},
 "reader": {"operation": "get", "num_threads": 1, "client_nodes": ["reader_node1", ...]}}
```

**rank 层**：`work_mode=rank`、`base_qps=105`、`data_size=["8MB"]`
**recall 层**：`work_mode=recall`、`base_qps=35`、`notify_count=1`、
`writer.notify_threads=1`、`reader.count_per_node=1`、`data_size=["3.5MB"]`

**rank 用例**（5 字段）：`traffic_mode` / `qps_multiple` / `notify_count` /
`writer.notify_threads` / `reader.count_per_node`
**recall 用例**（2 字段）：`traffic_mode` / `qps_multiple`

## 档位矩阵（reader 节点数 × 稀疏读者档）

名字 `rank_{traffic}_1w{N}r_{mult}_n{K}.json`：`{N}r` 即 notify_count（x==notify 语义），
reader 池 = count_per_node × 节点数 ≥ notify_count。每档跑 pulse/balanced × 1x/2x/3x。

| 目录 | 档位 | 池 |
| --- | --- | --- |
| 1-nodes/rank | 1w1r、1w2r、1w4r、1w10r | 1 / 2 / 4 / 10 |
| 2-nodes/rank | 1w2r、1w4r、1w8r、1w10r、2w10r（双写） | 2 / 4 / 8 / 10 / 10 |
| 4-nodes/rank | 1w4r、1w8r、1w10r、1w16r、2w10r（双写） | 4 / 8 / 12 / 16 / 12 |
| */recall | 1w1r（n1/n2/n4） | 1 |

## resolved 全参数语义（业务模型）

| 参数 | rank | recall | 语义 |
| --- | --- | --- | --- |
| `base_qps` × `qps_multiple` | 105×1/2/3 | 35×1/2/3 | 总写入 QPS，全部 writer 均分 |
| `traffic_mode` | pulse/balanced | pulse/balanced | 脉冲（每秒窗口突发）/ 均衡（匀速节流） |
| `notify_count` | =档位读者数 | 1 | 每批写后随机通知的 reader 数 |
| `writer.notify_threads` | =notify_count | 1 | notify 发送线程池 |
| `writer.count_per_node` / client_nodes | 1 / [writer_node1] | 同左 | 2w10r 档用例显式双写节点 |
| `reader.count_per_node` / client_nodes | 按档位表 | 1 / 按 tier | 部署拓扑 |
| `reader.operation` / `num_threads` | get / 1 | get / 1 | 层基座固定 |
| `data_size` | ["8MB"] | ["3.5MB"] | 每 size 完整跑一轮写读流水 |

## 运行方式

```bash
python benchmark.py --scenario pipeline --all                      # 递归全部 102
python benchmark.py --scenario pipeline --config-dir 2-nodes --recursive
python benchmark.py --scenario pipeline --config-dir 2-nodes/rank  # 单层
python benchmark.py --scenario pipeline --demo
```
