# configs/basic — 接口基准测试配置

单接口性能基准（put/get 家族与 mixed 混合负载）。每个配置文件是一个**场景**，
运行矩阵由 `params × data_size` 笛卡尔积展开（外层 data_size）。

## 目录结构与继承

```
configs/basic/
├── _configs.json      # 根基座：全部场景共用默认值（本层唯一公共默认）
├── README.md
├── demo/              # 冒烟用例（--config-dir configs/basic/demo 运行，自带全字段，不依赖继承）
└── <operation>_<topo>.json ×18   # 正式场景，仅 3-4 字段
```

加载时用例继承 `_configs.json`（浅层结构，dict 深合并、用例永远最优先）；
子目录若再放 `_configs.json` 亦可逐层覆盖（机制同 pipeline）。

## 正式场景矩阵（9 操作 × 2 拓扑 = 18）

| 操作族 | 文件 |
| --- | --- |
| 写 | put_local/remote、put_batch_local/remote、put_from_local/remote |
| 读 | get_local/remote、get_batch_local/remote、get_into_local/remote、get_buffer_local/remote |
| 混合 | mixed_local/remote（`put/get`）、mixed_batch_local/remote（`put_batch/get_batch`） |

拓扑：`_local` = client 与 store 同节点（store1）；`_remote` = client 在独立节点（node1）。

## 根基座 `_configs.json` 字段

| 字段 | 默认值 | 语义 |
| --- | --- | --- |
| `scenario` | `"basic"` | 场景类别（分发用） |
| `protocol` | `"ub"` | 传输协议（可被 `--protocol` CLI 覆盖） |
| `key_prefix` | `"mc_bench"` | 测试 key 前缀 |
| `key_count` | `1000` | key 空间大小（多 client 按 key_start/end 均分） |
| `data_size` | `["128KB","1MB","4MB","8MB"]` | **数组扫掠**：逐 size 循环，每个 size 自动重建 value.bin 并（读族）重预填 |
| `target_qps` | `null` | 场景总 QPS 限速；null/0 = 极速连发 |
| `duration` | `0` | 压测时长（秒）；0 = 跑一遍结束（见下文 duration 语义） |
| `warmup` | `{key_count:100, data_size:"1MB", num_clients:1, num_threads:1}` | 预热参数（独立前缀 mc_warmup，不计入指标） |
| `params` | 22 组合（clients 1/2/4/8/16 × threads 1/4/8/16/32） | 运行矩阵（× data_size 笛卡尔积） |

> `numa_bind` 不在本层：属全局配置，见 `configs/global.json`（默认 true；
> 用例/层级仍可显式覆盖）。

## 用例字段

| 字段 | 必填 | 语义 |
| --- | --- | --- |
| `operation` | 是 | `put` / `put_batch` / `put_from` / `get` / `get_batch` / `get_into` / `get_buffer`；mixed 为 `"<写>/<读>"` |
| `client_node` | 是 | cluster.json 节点名（决定拓扑标注） |
| `work_mode` | 是 | 与 operation 一致的标注（put/get/mixed） |
| `data_size` | 否 | 覆盖根基座扫掠（单 size 写 `["8MB"]`） |

## duration 压测语义（全部操作支持）

- **get 族**：循环读取预填数据直至到时
- **put 族**：以"完整遍历自己 key 分片"为一轮，跑满一轮后客户端清除**自己的**
  key（`batch_remove`，维护动作不计时、不产生 RESULT）再进入下一轮
- **mixed**：put/get 交替，put 侧写满一轮后清空本轮 put key 继续循环
- 多 client/多线程按 key 分片各清各的，互不干扰

## target_qps 限速

`target_qps`（总 QPS）按 client 数均分、client 内再按线程均分，绝对时间表节流；
长周期均值精确。0/null = 极速连发。
