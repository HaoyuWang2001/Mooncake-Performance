# configs/pipeline — 精排/召排（生产-消费）配置

writer 按 QPS 写入并通知随机 reader 子集，reader 消费读取。配置按
**reader 参与节点数 → work_mode** 两级目录组织，每层 `_configs.json` 逐层继承。

## 目录结构与继承链

```
configs/pipeline/
├── _configs.json               # 根基座（全部 102 用例共用）
├── README.md
├── demo/                       # 冒烟（--config-dir configs/pipeline/demo，自带全字段）
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

## 配置总览

核心规则：**实际调用哪个 store SDK 函数完全由各自 `operation` 决定**；
`notify_pipeline` 只是 notify 到达后、读取前的附加处理步骤，不参与函数选择。

### 顶层字段

| 字段 | 类型/默认 | 语义 |
| --- | --- | --- |
| `scenario` | `"pipeline"` | 场景类别（固定） |
| `work_mode` | `rank` / `recall` | 精排 / 召排业务模式 |
| `protocol` | `"ub"` / `"tcp"` | 传输协议 |
| `key_prefix` | `"mc_bench"` | key 前缀 |
| `data_size` | 数组，**必填** | value 大小数组：逐个 size 循环，每个 size 完整跑一轮写读流水，`value.bin` 自动按尺寸重建。单 size 写 `["8MB"]`，扫描写 `["512KB", "8MB"]` |
| `notify_pipeline` | `["get"]` | notify 到达后、读取前的处理步骤列表，可选步骤见下文 |
| `warmup` | `{duration: 5}` | 正式测量前以相同部署形态预热（writer 同 pacing 写+notify、reader 真实读取）；带 warmup 标记不计入指标 |
| `duration` | 秒，**必填 > 0** | writer/reader 共同的测量窗口 |
| `batch_size` | `1` | writer 每批写入的 key 数（reader 按 notify 批处理） |
| `traffic_mode` | `pulse` / `balanced` | 脉冲（每秒窗口集中突发）/ 均衡（匀速节流） |
| `base_qps` | 数值 | 基准写入 QPS |
| `qps_multiple` | `1` / `2` / `3` | 总写入 QPS = base_qps × 倍数，全部 writer 均分（2w 双写各半，总量守恒） |
| `notify_count` | ≥ 1 | 每批写完后随机选择的 reader 数，须 ≤ reader 池（count_per_node × 节点数） |
| `numa_bind` | 三级解析 | 用例/层级显式值 > `configs/global.json`（默认 true）> 代码默认 true；显式 `false` 关闭 |
| `writer` / `reader` | 对象 | 角色块，字段见下 |

### writer 块

| 字段 | 语义 |
| --- | --- |
| `count_per_node` | 每节点 writer 进程数 |
| `operation` | 写接口，可选值见下表 |
| `notify_threads` | notify 发送线程池大小 |
| `client_nodes` | writer 部署节点列表（每节点启动 count_per_node 个进程） |

**writer.operation 可选值**：

| 值 | SDK 调用 | 计时粒度 | 说明 |
| --- | --- | --- | --- |
| `put` | `store.put(key, value)` | 每 key 一次计时调用 | 基础单写 |
| `put_batch` | `store.put_batch(keys, values)` | 整批一次计时调用 | 批大小 = `batch_size` |
| `put_from` | `store.put_from(key, ptr, size)` | 每 key 一次计时调用 | 从已注册本地缓冲写入；writer 启动时注册页对齐缓冲（value 写入缓冲为未计时准备），注册失败即退出 |

### reader 块

| 字段 | 语义 |
| --- | --- |
| `count_per_node` | 每节点 reader 进程数（池 = count_per_node × 节点数，须 ≥ notify_count） |
| `operation` | 读接口，可选值见下表 |
| `num_threads` | 单进程消费线程数 |
| `client_nodes` | reader 部署节点列表 |

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
| x-nodes 层 `_configs.json` | `writer{count_per_node, operation, client_nodes}` + `reader{operation, num_threads, client_nodes}`（client_nodes 按层：writer_node1 + reader_node1..K） |
| rank 层 `_configs.json` | `work_mode` / `base_qps` / `data_size` |
| recall 层 `_configs.json` | `work_mode` / `base_qps` / `notify_count` / `writer.notify_threads` / `reader.count_per_node` / `data_size` |
| rank 用例 | `traffic_mode` / `qps_multiple` / `notify_count` / `writer.notify_threads`（2w 双写档另覆盖 `writer.client_nodes`）/ `reader.count_per_node` |
| recall 用例 | `traffic_mode` / `qps_multiple` |
| `demo/` | 自带全字段（不参与继承链） |

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
| `reader.operation` / `num_threads` | get / 1 | get / 1 | 层基座固定（可选值语义见「配置总览」） |
| `data_size` | ["8MB"] | ["3.5MB"] | 每 size 完整跑一轮写读流水 |

## 运行方式

```bash
python benchmark.py --config-dir configs/pipeline                       # 递归全部（含 demo/）
python benchmark.py --config-dir configs/pipeline/2-nodes               # 含子目录递归
python benchmark.py --config-dir configs/pipeline/2-nodes/rank          # rank 层
python benchmark.py --config-dir configs/pipeline/demo                  # 冒烟
```
