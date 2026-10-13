# Mooncake 基准测试用例迁移方案

针对 `yuanrong-datasystem` KV 性能用例向 **Mooncake Store** 的迁移实现。本目录包含：
本地编排脚本（Windows PC 上运行的 Python 脚本）、服务器端测试进程（Python/Shell 脚本）、场景配置文件与结果汇总工具。

## 1. 迁移背景

原 `yuanrong-datasystem` 将测试区分为 `set_local` / `set_remote` / `get_local` / `get_cross_node` / `get_remote_direct` / `get_remote_cross` 等十余种 test_mode。迁移到 Mooncake 后，`mooncake_master` 统一负责元数据与数据放置，客户端（real client）固定先访问 Master 获取元数据，再从目标 store segment 拉取/写入数据，因此**原多套拓扑语义可收敛为两种**：测试进程与 store 同节点（local_node）或不同节点（remote_node）。

## 2. 测试维度

### 2.1 操作类型（5 种）

- `put`：单 key 写入
- `get`：单 key 读取（校验返回值前 64 字节与预期一致）
- `put_batch`：批量写入（一次 `put_batch` 调用写 `batch_size` 个 key，记录为一次 batch 操作的时延）
- `get_batch`：批量读取（同上）
- `mixed`：读写混合，`put_ratio` 比例的执行 put，其余执行 get

### 2.2 拓扑（2 种）

- `local_node`：测试进程（client）与 mooncake store 部署在同一节点
- `remote_node`：测试进程（client）与 mooncake store 部署在不同节点
**拓扑实现说明**：测试进程内置 Mooncake real client，无论数据实际存储在哪个节点，
client 均先访问 `mooncake_master` 获取元数据，再向目标 store 发起传输。因此测试时只需保证：
1）client 与 store 同节点（local_node）；2）client 与 store 不同节点（remote_node）。

### 2.3 传输协议（2 种）

- `tcp`：TCP 传输
- `ub`：URMA/UB 传输（使用 `/dev/uburma` 设备，设备名在 cluster.json 的 `protocol.ub.device` 中配置）
（测试 ub 与原生 tcp 的性能差异。）

## 3. 场景分类

测试用例按类别放在 `configs/` 子目录中：

| 目录 | 说明 |
| --- | --- |
| `configs/basic/` | 接口级 benchmark（5 种操作 × 2 种拓扑 = 10 个场景） |
| `configs/basic/demo/` | basic 冒烟用例（simple-*，非主测试，用于打通验证） |
| `configs/pipeline/` | 精排/召排 pipeline 场景 |
| `configs/pipeline/demo/` | pipeline 冒烟用例 |
| `configs/cluster.json` | 集群基础设施（不变） |
| `configs/global.json` | 业务无关全局超参（不变） |

### 3.4 并发维度（params 组合）

并发维度由场景配置中的 `params` 数组显式指定，每个元素为一个
`{"num_clients": N, "num_threads": T}` 组合，逐组顺序执行：

- `num_clients`：独立测试进程数（单节点多 client）
- `num_threads`：单进程内工作线程数
默认场景扫描 22 组组合：
`1×1, 1×4, 1×8, 1×16, 1×32, 2×1, 2×4, 2×8, 2×16, 2×32, 4×1, 4×4, 4×8, 4×16, 4×32, 8×1, 8×4, 8×8, 8×16, 16×1, 16×4, 16×8`。
最简打通用例 `get_simple` 仅包含 `1×1` 一组。

### 3.5 参数配置

| 参数 | 说明 | 测试场景使用值 |
| --- | --- | --- |
| `data_size` | 单个 value 的大小 | put/get 单 key 场景 8MB；mixed 场景 3.5MB；预热 1MB |
| `key_count` | key 数量 | 正式测量 1000（数据总量 8GB，适配单 store ~12GB 内存限制）；预热场景 100 |
| `batch_size` | 单个 batch 操作包含的 key 数 | put_batch / get_batch 场景 16 |
| `put_ratio` | mixed 场景中 put 操作占比 | mixed 场景 0.5 |
| `duration` | 压测时长（秒，可选） | 未配置：跑完 key_count 一遍即结束；配置：循环访问全部 key 直到到达时长（仅 get / get_batch） |
| **压测模式（duration）备注**：由于 `put` 为 create-only（key 已存在时 no-op 不覆盖），重复写入会失效，因此 duration 目前仅适用于 **get 与get_batch**。 |  |  |

### 3.6 pipeline（精排/召排）模式

精排/召排是**生产-消费（pipeline）**测试模式，与单接口 benchmark 并列。通过 `mode: "pipeline"` 识别。

| 参数 | 说明 | recall 值 | refine 值 |
| --- | --- | --- | --- |
| `work_mode` | recall(召排) / refine(精排) | `recall` | `refine` |
| `data_size` | 单个 value 大小 | 8MB | 3.5MB |
| `ttl` | key 自动过期秒数（0=不过期） | 3s | 0 |
| `notify_count` | 每次写入通知的 reader 数（扇出比） | 10 | 1 |
| `notify_pipeline` | reader 操作链 | `["getBuffer"]` | `["exist","getBuffer"]` |
| `writer_count` | 生产者线程数（配置值，非扫描维度） | 1 | 1 |
| `reader_count` | 消费者线程数（配置值，非扫描维度） | 1 | 1 |
| `target_qps` | 写入目标 QPS（pacing） | 200 | 200 |
| `enable_jitter` | false=均衡模式（均匀节流）; true=pulse 脉冲模式（突发） | false | false |
| `batch_keys_count` | 每次写入的 key 数 | 1 | 1 |
| `stage_duration_seconds` | 持续运行秒数 | 30 | 30 |
| **工作流**：prepare-data（生成 value.bin）→ clean(full) → pipeline benchmark（writer 按 target_qps 写入；读取完成后扇出通知；reader 执行 notify_pipeline 链读取并校验） |  |  |  |
| **扇出语义**：每次 writer 写入成功后，该 BatchItem 被 `notify_count` 个 reader 各读一遍（1 写 → N 读）。recall notify_count=10 即每写一次 10 个 reader 扇出读；refine notify_count=1 即每写一次 1 个 reader 消费。 |  |  |  |
| **pacing 模式**：`enable_jitter=false` 写线程按 target_qps 均匀节流（balanced 均衡模式）；`true` 写线程无节流以最大速率写入（模拟推理批到达的 pulse 突刺）。 |  |  |  |
| **输出指标**：与单接口一致，另增加 `work_mode/writer_count/reader_count/target_qps/ttl/notify_count/notify_pipeline/enable_jitter/stage_duration_seconds/batch_keys_count`。 |  |  |  |
| **运行**： |  |  |  |

```
python benchmark.py --scenario pipeline --config pipeline/recall_pipeline.json
python benchmark.py --scenario pipeline --config pipeline/refine_pipeline.json
python benchmark.py --scenario pipeline --all                     # 跑全部 pipeline 场景
```

### 3.7 数据集

- **文件结构**：数据按场景持久化在 `remote_data_dir/<scenario>/` 下：
  - `value.bin`：value 内容，`data_size` 字节，固定随机种子 `data_seed=42` 生成；
  - `keys.txt`：全部 key 列表（`mc_bench_{scenario}_{index:08d}`，每行一个），与正式测试的 key 集合一致；
- **共享 value**：全部 key 共享同一份 `value.bin` 内容（与原 datasystem 一致），get 场景
  通过校验返回数据前 64 字节是否与预期一致来判断成功；
- **可复现性**：固定种子保证数据文件被删除后重新生成的内容完全一致；配合
  `--skip-data` 可复用已生成的持久化数据，避免数据集差异引入性能偏差；
- **预热数据隔离**：预热使用独立前缀 `mc_warmup_{scenario}_{pid}_*`，测试结束后即删除，
  不污染正式数据（`mc_bench_*`）与持久化数据集。

## 4. 用例矩阵

5 种操作 × 2 种拓扑 = 10 个场景：

| 操作 / 拓扑 | local_node | remote_node |
| --- | --- | --- |
| put | put_local | put_remote |
| get | get_local | get_remote |
| put_batch | put_batch_local | put_batch_remote |
| get_batch | get_batch_local | get_batch_remote |
| mixed | mixed_local | mixed_remote |

## 5. 测试部署架构

```
+-----------------------------------------------------------+
| Windows PC                                                |
|   benchmark.py : 读配置 -> 生成 env.sh -> scp 上传(3文件)    |
|                  -> ssh 驱动阶段 -> scp 收集 -> 汇总整理      |
+----------------------------+------------------------------+
                             | ssh / scp（仅上传 3 个文件）
+----------------------------+------------------------------+
| Node1: mooncake_master + mooncake_store (唯一 store 实例)   |
|                                                           |
|   local_node 场景: client 测试进程也运行在本节点               |
+----------------------------+------------------------------+
                             | tcp/ub
+----------------------------+------------------------------+
| Node2: 仅 client 测试进程                                   |
|                                                           |
|   remote_node 场景: client 测试进程运行在本节点               |
+-----------------------------------------------------------+
```

说明：

- `mooncake_master` 与 `mooncake_store` 由运维手动启动，本套件不管理其生命周期；
- 仅启动一个 store 实例（部署在 Node1）；local 场景 client 在 Node1，remote 场景 client 在 Node2；
- 每个 benchmark 运行由 `run_benchmark.sh` 在本节点并行拉起 `num_clients` 个
  `mooncake_benchmark_client.py` 进程，每个进程内再开启 `num_threads` 个工作线程。
- **不再上传 JSON 配置文件**：benchmark.py 读取 JSON 后生成 `env.sh`（环境变量形式），仅上传
  `env.sh` + `run_benchmark.sh` + `mooncake_benchmark_client.py` 三个文件到服务器。

## 6. 输出指标

| 指标 | 含义 | 单位 |
| --- | --- | --- |
| throughput_mb_s / throughput_gb_s | 吞吐量 | MB/s、GB/s |
| qps | 每秒操作数（total / wall_s） | ops/s |
| total | 总操作数（duration 模式下为截止前实际执行的次数） | ops |
| ok / fail / fail_rate | 成功数 / 失败数 / 失败率（fail / total） | ops、% |
| avg_ms / min_ms / max_ms / p99_ms / p999_ms | 时延（平均 / 最小 / 最大 / P99 / P99.9） | ms |

- 时延基于全部 client 上报的单次操作样本精确统计；
- 吞吐量 = 全部 client 传输总字节数 / 全局墙钟时间；
- 每条结果（CSV 行）同时打印本次运行配置的核心参数：`data_size`、`key_count`、
  `batch_size`（put_batch / get_batch 场景）、`put_ratio`（mixed 场景）、
  `duration`（仅配置时）。

## 7. 配置文件

测试使用一个 Python 编排程序（`benchmark.py`）+ 服务器端 Python 测试程序
（`scripts/mooncake_benchmark_client.py`）+ Shell 启动脚本（`scripts/run_benchmark.sh`），
行为由 JSON 配置文件驱动。
配置流程：benchmark.py 读取 `cluster.json` + `global.json` + 场景 JSON → 合并生成
`scripts/env.sh`（环境变量形式）→ scp 上传到服务器。mooncake_benchmark_client.py
从环境变量读取配置，同时也支持显式 CLI 参数覆写。

### 7.1 configs/cluster.json（集群基础设施）

仓库仅提供 `configs/cluster.example.json` 模板，使用前先复制为 `configs/cluster.json` 并修改。

| 字段 | 说明 |
| --- | --- |
| master_addr | mooncake_master 地址（必须用内网高性能网卡 IP），如 `192.168.84.151:50051` |
| metadata_server | `setup` 的 metadata_server 参数，如 `[http://192.168.84.151:9999/metadata](http://192.168.84.151:9999/metadata)`（master 的 HTTP 元数据端口，必须用内网 IP） |
| protocol.ub.device | ub 传输使用的设备名（如 `bonding_dev_0`）；tcp 场景不使用 |
| venv | 服务器端 Python 虚拟环境解释器（如 `/root/venv/bin/python`），所有服务器端脚本均用它执行 |
| nodes | 可用节点列表，每个节点含 `ip`（仅用于 ssh/scp 登录，管理网地址）、`fabric_ip`（内网高性能网卡地址，客户端 local_hostname 使用，可省略则回退到 ip）、`ssh_user`、`ssh_port`、`ssh_key`（空 = 默认密钥，仅支持 ssh 免密登录） |
| 节点示例： |  |

```
"nodes": {
  "node4": { "ip": "141.61.84.151", "fabric_ip": "192.168.84.151", "ssh_user": "root", "ssh_port": 22, "ssh_key": "" },
  "node3": { "ip": "141.61.84.149", "fabric_ip": "192.168.84.149", "ssh_user": "root", "ssh_port": 22, "ssh_key": "" }
}
```

### 7.1b configs/global.json（业务无关全局超参）

与业务无关的全局超参数与服务器端路径独立放在 `configs/global.json`，由 `--global` 指定（默认即该路径）：

| 字段 | 说明 |
| --- | --- |
| local_buffer_size | client 本地传输暂存缓冲，建议 ≥ num_threads × data_size |
| global_segment_size | 本 client 贡献的存储大小，纯客户端设 0 |
| tenant_id | 租户标识，默认 "default" |
| data_seed | 数据生成固定随机种子 |
| remote_dir | 服务器端脚本/配置根目录 |
| remote_data_dir / remote_results_dir | 服务器端数据 / 结果目录 |

### 7.2 configs/<category>/<scenario>.json（场景配置）

场景配置按类别放在子目录中：

- `configs/basic/` — 接口级 benchmark（10 个正式场景）
- `configs/basic/demo/` — 冒烟用例（simple-*，非主测试）
- `configs/pipeline/` — 精排/召排场景
- `configs/pipeline/demo/` — 冒烟用例| 字段 | 说明 |
| --- | --- |
| scenario | 场景名，同时参与 key 前缀 |
| operation | put / get / put_batch / get_batch / mixed |
| topology | local_node / remote_node |
| protocol | tcp / ub（ub 的设备名在 cluster.json `protocol.ub.device` 中配置） |
| client_node / store_node | client 所在节点（决定 ssh 目标）；store 所在节点（示意） |
| key_prefix | key 前缀 |
| key_count / data_size / batch_size / put_ratio | 见 3.5 |
| duration | 压测时长（秒，可选），仅 get / get_batch 支持，见 3.1 |
| params | 并发组合数组 `[{"num_clients": N, "num_threads": T}, ...]`，逐组顺序执行 |
| warmup | 预热参数（key_count / data_size / num_clients / num_threads） |

### 7.3 测试阶段（--mode）

- `prepare-data`：按固定种子生成数据文件并持久化
- `clean`：`--scope key-prefix` 仅清理本场景 key（`batch_is_exist` + `batch_remove` 分类统计不存在/失败）；`--scope full` 使用 `remove_all()`，失败即报错退出，无回退
- `prepare-store`：`batch_is_exist` 检查存在性，仅对缺失 key 执行 `put_batch` 补写
- `benchmark`：`--warmup true|false`，预热在正式测量前执行小规模运行
- `pipeline`：生产-消费 pipeline 模式（精排/召排）

### 7.4 各操作类型完整流程

- `put / put_batch`：prepare-data → clean → warmup → benchmark
- `get / get_batch`：prepare-data → prepare-store（先检查数据是否存在）→ warmup → benchmark
- `mixed`：prepare-data → clean → prepare-store → warmup → benchmark
`duration` 仅改变 benchmark 阶段 key 的遍历方式（跑一遍结束 vs 循环到截止），
不改变上述阶段流程。

### 7.5 key 格式

```
mc_bench_{scenario}_{index:08d}     例：mc_bench_put_local_00000000
```

## 8. 运行方式

前置条件：

- 集群中 mooncake_master、mooncake_store 已手动启动；
- PC 上 `ssh` / `scp`（OpenSSH）可用，已配置到服务器的密钥免密登录；
- 服务器已安装 mooncake Python 包。
首次使用前，复制集群配置模板并修改：

```
copy configs\cluster.example.json configs\cluster.json   # 然后编辑其中的地址/账号
```

```
# 运行 basic 类别下全部 10 个场景
python benchmark.py --scenario basic --all
# 只运行 basic 类别下指定场景
python benchmark.py --scenario basic --config basic/get_remote.json
# 运行 demo 冒烟用例
python benchmark.py --config basic/demo/simple_get.json
# 运行 pipeline 场景全部
python benchmark.py --scenario pipeline --all
# 运行 pipeline 指定场景
python benchmark.py --scenario pipeline --config pipeline/recall_pipeline.json
# 指定集群/全局配置文件
python benchmark.py --cluster configs/cluster.json --global configs/global.json \
    --scenario basic --config basic/put_local.json
# 关闭预热；仅打印命令不执行
python benchmark.py --scenario basic --config basic/put_local.json --no-warmup --dry-run
# 运行后清理远程文件
python benchmark.py --scenario basic --config basic/get_local.json --clean-up
```

## 9. 结果目录结构

```
results/<时间戳，如 20261010-1308>/
├── logs/
│   ├── <scenario>.prepare-data.log
│   ├── <scenario>.prepare-store.log
│   ├── <scenario>.clean.log
│   └── <scenario>.benchmark_c{clients}_t{threads}.log
├── <scenario>.csv                  # 该场景所有组合的性能明细
└── benchmark_overview.csv          # 全部场景最优吞吐汇总
```

## 10. 注意事项与待验证项

- API 语义以 `mooncake_api.md` 为准：`put` 为 create-only（key 存在时 no-op 不覆盖），
  因此 put 类场景必须保证先 clean 再 put（编排流程已保证，clean 失败会终止场景）；
- `get` 缺失时返回空 bytes `b""`，本套件 value 恒非空，故以空结果判定 MISS；
- 数据路径错误码为 int 负值（如 OBJECT_NOT_FOUND），`clean` 通过 `batch_is_exist` 前置
  判定区分"key 不存在"与"删除失败"，失败即报错退出；
- 构造使用无参 `MooncakeDistributedStore()` + `setup(...)`，`setup` 返回非 0 即报错退出；
- `local_buffer_size` 需满足 `num_threads × data_size` 的并发占用，避免缓冲区不足；
- `prepare-data` 阶段自动检测：每次运行前 ssh 检查 `value.bin` 的文件大小是否匹配当前
  `data_size`，匹配则跳过 prepare-data，避免重复生成浪费。种子变更或数据大小变更时自动触发重新生成；
- `--clean-up` 参数：运行完成后 ssh 删除远程全部文件（`rm -rf $MC_REMOTE_DIR`），
  本地结果已提前 scp 拉取不受影响。# Mooncake-Performance
