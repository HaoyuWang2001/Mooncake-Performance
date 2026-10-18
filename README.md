# Mooncake 基准测试用例迁移方案

针对 `yuanrong-datasystem` KV 性能用例向 **Mooncake Store** 的迁移实现。本目录包含：
本地编排脚本、服务器端测试进程（Python/Shell 脚本）、场景配置文件与结果汇总工具。

## 1. 迁移背景

原 `yuanrong-datasystem` 将测试区分为 `set_local` / `set_remote` / `get_local` / `get_cross_node` /
`get_remote_direct` / `get_remote_cross` 等十余种 test_mode。迁移到 Mooncake 后，`mooncake_master`
统一负责元数据与数据放置，客户端（real client）固定先访问 Master 获取元数据，再从目标 store
segment 拉取/写入数据，因此**原多套拓扑语义可收敛为两种**：测试进程与 store 同节点
（local_node）或不同节点（remote_node）。

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

**拓扑实现说明**：测试进程内置 Mooncake real client，无论数据实际存储在哪个节点，client 均先
访问 `mooncake_master` 获取元数据，再向目标 store 发起传输。因此测试时只需保证：
1）client 与 store 同节点（local_node）；2）client 与 store 不同节点（remote_node）。
拓扑判定基于 global.json 的 `mooncake_store_nodes`（store 节点名列表）：client 节点 ∈ 列表即
local_node，否则 remote_node。

### 2.3 传输协议

- `tcp`：TCP 传输
- `ub`：URMA/UB 传输（使用 `/dev/uburma` 设备，设备名在 global.json 的 `protocol.ub.device` 中配置）
- `rdma`：RDMA 传输（设备名不配置时自动发现）

CLI 的 `--protocol` 可选值为 `tcp / rdma / ub`，优先级最高、可覆盖配置文件的协议值。
（测试重点为 ub 与原生 tcp 的性能差异。）

## 3. 场景分类与配置

测试用例按类别放在 `configs/` 子目录中；仓库只提交**两个模板**（`cluster.example.json`、
`global.example.json`）与 **basic/pipeline 两个目录的全部场景配置**，工作副本
`cluster.json` / `global.json` 为本地文件（不入库）：

| 目录 | 说明 |
| --- | --- |
| `configs/basic/` | 接口级 benchmark（put/put_batch/put_from、get/get_batch/get_into/get_buffer、mixed `put_iface/get_iface`，各 × local/remote） |
| `configs/basic/demo/` | basic 冒烟用例（simple-*，非主测试，用于打通验证） |
| `configs/pipeline/` | 精排/召排 pipeline 场景（48 个正式场景） |
| `configs/pipeline/demo/` | pipeline 冒烟用例 |
| `configs/cluster.example.json` | 集群基础设施模板（复制为 cluster.json 后修改） |
| `configs/global.example.json` | 全局超参模板（复制为 global.json 后修改） |

接口清单、签名与梯队分类详见 **`Mooncake-api.md`**。

### 3.1 并发维度（params 组合）

并发维度由场景配置中的 `params` 数组显式指定，每个元素为一个
`{"num_clients": N, "num_threads": T}` 组合，逐组顺序执行。basic 场景的
运行矩阵为 **params × data_size 的笛卡尔积**（外层 data_size、内层 params，
CSV 行序与执行顺序一致）：

- `num_clients`：独立测试进程数（basic 场景，单节点多 client）
- `num_threads`：单进程内工作线程数

默认场景扫描 22 组组合：
`1×1, 1×4, 1×8, 1×16, 1×32, 2×1, 2×4, 2×8, 2×16, 2×32, 4×1, 4×4, 4×8, 4×16, 4×32, 8×1, 8×4, 8×8, 8×16, 16×1, 16×4, 16×8`。
demo 冒烟用例（simple-*）均仅包含 `1×1` 一组。

### 3.2 参数配置

**分层公共默认（`_configs.json`）**：每个目录层级可放一个 `_configs.json`，
加载用例时从场景根目录（如 `configs/pipeline/`）沿目录链逐层读取并合并——
下层补充/覆盖上层（dict 递归深合并、其余类型整体替换），用例文件最后合并、
永远最优先。`_configs.json` 本身不作为场景运行（收集时自动跳过 `_` 前缀文件）。
basic 根基座 `configs/basic/_configs.json` 存放 18 个正式场景共用字段
（`scenario/protocol/key_prefix/key_count/data_size/target_qps/warmup/numa_bind`）。

| 参数 | 说明 | 测试场景使用值 |
| --- | --- | --- |
| `operation` | 使用的 store 接口：`put / put_batch / put_from`（写）或 `get / get_batch / get_into / get_buffer`（读）；mixed 场景为 `"<写接口>/<读接口>"`（如 `put/get`、`put_batch/get_batch`、`put_from/get_buffer`） | 见各场景 |
| `data_size` | **必填，数组形式**（如 `["8MB"]` 或 `["128KB", "1MB", "4MB", "8MB"]`）：数组内逐个 size 循环运行（basic 按 params × size 笛卡尔积；pipeline 每个 size 完整跑一轮写读流水）。不同尺寸间自动重建 `value.bin` 并（get 家族）重新预填充 store；运行日志/CSV 行以 size 后缀区分 | basic 默认 `["128KB", "1MB", "4MB", "8MB"]`（`_configs.json`）；pipeline 的 data_size 不进基座、逐用例携带；预热 1MB |
| `key_count` | key 数量 | 正式测量 1000（数据总量 8GB，适配单 store ~12GB 内存限制）；预热场景 100 |
| `batch_size` | 单个 batch 操作包含的 key 数 | put_batch / get_batch 场景 16；mixed_batch 8 |
| `put_ratio` | mixed 场景中 put 操作占比 | mixed 场景 0.5 |
| `duration` | 压测时长（秒，可选） | 未配置：跑完 key_count 一遍即结束；配置：循环访问全部 key 直到到达时长（仅 get 家族：get / get_batch / get_into / get_buffer） |
| `target_qps` | 总限速 QPS（可选）：按该总 QPS 对操作均匀限速（跨 client×thread 均分，绝对时间表自纠偏） | 空/0：计数模式极速连发（现行为）；设置后用于低压时延对比 |
| `numa_bind` | 客户端 NUMA 绑定（可选）：为 true 时每个 client 进程绑定到本节点的一个 NUMA 节点（CPU 亲和，round-robin 轮询分配） | 空/`null`：不绑定（现行为） |

- **限速说明**：`target_qps` 为**整个场景的总 QPS**——编排器按组合拆给各 client 进程，客户端再按线程均分；与 `duration` 可组合（限速 + 定时循环）。注意：低压限速会显著拉长计数扫描耗时（如 1000 keys ÷ 100qps ≈ 10s/组合，22 组合 ≈ 220s），做低压对比时建议配精简 `params`。
- **NUMA 绑定**：`numa_bind` **默认开启**（缺省/null 即 true，仅显式 false 关闭；basic/pipeline 基座均显式写 true）。开启时basic 的多 client 由 `run_benchmark_basic.sh` 探测本节点 NUMA 节点数 K 并让 client i 绑定 `node (i-1)%K`；pipeline 按节点探测 K，该节点上所有 client 进程（reader 先、writer 后）节点内 round-robin 绑定。客户端内部用 `os.sched_setaffinity`（CPU 亲和，内存为默认本地分配策略）；也可手动传 `--numa-node N` 或 `--cpu-bind CPULIST`（如 `0-83,90`）直接指定。绑定结果在客户端日志输出 `cpu binding: ...` 行。
- **mixed 交替语义**：put 与 get **交替执行**（从 put 开始），`put k0, get k0, put k1, get k1, …`；
  每个 get 读取本次 put 刚写入的 key（保证可见性），一侧配额先尽则另一侧连续收尾；配额 =
  `put_ratio × key_count`（put）/ 余量（get）。
- 零拷贝接口（put_from / get_into / get_buffer）在场景进程内自动 `register_buffer` 并注销，
  无需额外配置。
- **pipeline 同样由 operation 指定接口**：`writer.operation`（写接口，`put_batch` 等）与
  `reader.operation`（读接口，`get` 等）真实驱动调用（见 3.3）。

> 压测模式（duration）备注：**全部 basic 操作均支持**。get 家族循环读取预填数据；put 家族与
> mixed 以"完整 key 遍历"为一轮——put 跑满一轮后由客户端清除**自己的** key（`batch_remove`，
> 维护动作不计时、不产生 RESULT）再进入下一轮；mixed 按 put/get 交替，put 侧写满一轮后清空
> 本轮 put key 继续循环。多客户端/多线程按 key 分片各清各的，互不干扰。

### 3.3 pipeline（精排/召排）模式

精排/召排是**生产-消费（pipeline）**测试模式，与单接口 benchmark 并列。通过
`scenario: "pipeline"` 识别。

**配置组织**：`configs/pipeline/` 顶层仅放根基座 `_configs.json` 与 `demo/`，正式配置按
reader 侧节点数归档进 `1-nodes/`、`2-nodes/`、`4-nodes/`，其下再按 work_mode 分 `rank/` 与
`recall/` 子目录。每层 `_configs.json` 逐层继承：根基座（scenario/protocol/key_prefix/
notify_pipeline/warmup/duration/batch_size/numa_bind，不含 data_size/writer/reader）→
x-nodes 层（writer/reader 骨架，client_nodes 按层配置）→ rank|recall 层（work_mode、
base_qps；recall 另含 notify_count/writer.notify_threads/reader.count_per_node）→
用例（rank：traffic_mode/qps_multiple/notify_count/writer.notify_threads/reader.count_per_node/
data_size；recall：traffic_mode/qps_multiple/data_size）。data_size 一律逐用例携带。
`--config-dir` 指定某目录运行：用例已下沉到 rank|recall 层，指向 x-nodes 层需配 `--recursive`
（如 `--config-dir 2-nodes --recursive`），或直接指向叶子目录（如 `--config-dir 2-nodes/rank`）。

**配置命名**：`{work_mode}_{traffic_mode}_1w{notify}r_{倍数}x_n{节点数}.json`
（单 writer 家族中名字里的 `{notify}r` 即 notify_count 扇出数，非 reader 池大小；池 = cpn×节点数 ≥ notify_count）

| 参数 | 说明 | rank 值 | recall 值 |
| --- | --- | --- | --- |
| `work_mode` | rank(精排) / recall(召排) | `rank` | `recall` |
| `traffic_mode` | `balanced`=均衡（按 QPS 均匀节流）/ `pulse`=脉冲（每秒窗口集中突发） | 两者皆测 | 两者皆测 |
| `base_qps` | 基准写入 QPS | 105 | 35 |
| `qps_multiple` | QPS 倍数（总写入 = base × 倍数） | 1 / 2 / 3 | 1 / 2 / 3 |
| `notify_count` | 每个 writer 每批写完后**随机选择**的 reader 数（业务模型"1写10读"中的 10 读；单 writer 家族 = 名字中的 r 数） | 档位相关：10r 档=10，稀疏档=读者总数 | 1 |
| `writer` | `{count_per_node, operation, notify_threads, client_nodes}`：每节点 writer 数 / 写接口（`put_batch`/`put`/`put_from`，真实驱动调用）/ notify 发送线程池 / 部署节点 | cpn=1（notify_threads=notify_count） | cpn=1 |
| `reader` | `{count_per_node, operation, num_threads, client_nodes}`：每节点 reader 数（池 = cpn×节点数，须 ≥ notify_count）/ 读接口（`get`/`get_batch`/`get_into`/`get_buffer`）/ 消费线程数 / 部署节点 | 见下方档位表 | 恒 1 |
| `data_size` | **必填，数组形式**：逐个 size 循环，每个 size 完整跑一轮写读流水（value.bin 自动按尺寸重建）；单 size 写 `["8MB"]`，扫描写 `["512KB", "8MB"]` | `["8MB"]` | `["3.5MB"]` |
| `warmup` | `{duration}`：相同部署形态下的短暂运行（writer 同 pacing 写+notify、reader 真实读取），激活 writer→store、writer→reader、reader→store 链路；warmup 项带标记**不计入指标** | 5s | 5s |
| `duration` | 压测时长（秒） | 30 | 30 |
| `batch_size` | writer 每批写入的 key 数（reader 按 notify 批处理） | 1 | 1 |

rank 档位矩阵（按 reader 参与节点数归档进同名目录；每档均跑 pulse/balanced × 1x/2x/3x）：

| 目录 | 档位（1w{x}r = notify x） | reader 池（cpn×节点） |
| --- | --- | --- |
| 1-nodes/ | 1w1r_n1、1w2r_n1、1w4r_n1；另有 1w10r_n1（池 10） | 1 / 2 / 4 / 10 |
| 2-nodes/ | 1w2r_n2、1w4r_n2、1w8r_n2；另有 1w10r_n2（池 10）、2w10r_n2（双写） | 2 / 4 / 8 / 10 |
| 4-nodes/ | 1w4r_n4、1w8r_n4、1w16r_n4；另有 1w10r_n4（池 12=3×4）、2w10r_n4（双写） | 4 / 8 / 16 / 12 |

recall（1w1r，notify=1，恒 cpn=1）按 n1/n2/n4 同样归档进三个目录。

store 节点由 **global.json 的 `mooncake_store_nodes`**（cluster.json 节点名列表，可多个）统一标识，
各场景配置**不再携带 store_node**；该列表仅用于指标行的 local/remote 拓扑标注（local = 该节点 ∈
store 节点列表），实际通信仍经 master 发现 store。

- **部署语义**：writer/reader 各自在 client_nodes 的**每个节点**启动 count_per_node 个进程（节点列表
  正序）；writer 全局编号按节点正序，reader 端口 9201..9200+池大小；节点按角色命名（`writer_nodeX` /
  `reader_nodeX`，见 cluster 配置）；**n{N} 表示 reader 侧参与节点数**（reader_node1..N），writer 数由
  家族名固定：1w 家族仅 `writer_node1`（1 个 writer），2w 家族为 `writer_node1,2`（2 个 writer，
  与 n 无关）；
- **负载语义**：总写入 QPS = base_qps × qps_multiple，由全部 writer（cpn × 节点数）均分（2w 家族
  双 writer 各半即原系统 "105/2" 语义，总量守恒；小数速率由窗口预算累积保证长期精确）；
- **扇出语义**：每批写入后从 reader 池随机选 notify_count 个 reader，单次 HTTP 请求携带整批 key
  （读总量 = 写入批数 × notify_count）；reader 侧**按 notify 批入队处理**（queue 深度单位 = 待处理
  notify 批数，`max_depth` 以此计）；
- **warmup 语义**：正式测量前以相同部署形态运行 warmup.duration 秒（notify 带 warmup=1 标记，writer
  不输出 RESULT、reader 照常读取但不记录，SUMMARY 计入 warmup_total）；
- **operation 角色化**：`writer.operation`（写接口）与 `reader.operation`（读接口）**真实驱动调用**
  ——writer 支持 `put_batch`（整批一次调用）/`put`、`put_from`（逐 key 调用），reader 支持
  `get`/`get_into`/`get_buffer`（逐 key）与 `get_batch`（整批一次调用）；时延样本 = 单次接口调用
  （逐 key 接口每 key 一条、批量接口每批一条，RESULT 行携带 key_count，total/qps 均按 key 计）；
  `notify_pipeline` 收敛为预检步骤（`exist`），`get/getBuffer` 步骤名不再承担读数据职责；
- **指标输出**：每个 writer 一行、每 writer 聚合一行、每个 reader 一行、全 reader 聚合一行
  （instance=TOTAL），`role/node/instance/topology` 列区分；
- **FAIL 处理**：场景失败时先优雅 shutdown reader（使其落盘 SUMMARY/RESULT）→ kill 残留进程 → 
  拉取全部远程日志，再继续后续场景（端口随进程结束释放，避免级联失败）；
- **运行**：

```
python benchmark.py --scenario pipeline --config configs/pipeline/2-nodes/rank/rank_pulse_2w10r_3x_n2.json
python benchmark.py --scenario pipeline --all     # 递归跑全部 pipeline 场景（跳 demo/ 与 _ 前缀）
python benchmark.py --scenario pipeline --config-dir 2-nodes          # 跑 2-nodes 目录全部配置
python benchmark.py --scenario pipeline --config-dir 4-nodes --recursive   # 含子目录递归
python benchmark.py --scenario pipeline --demo    # 冒烟（demo 目录）
```

### 3.4 数据集

- **文件结构**：数据按场景持久化在 `remote_data_dir/<scenario>/` 下：
  - `value.bin`：value 内容，`data_size` 字节，固定随机种子 `data_seed=42` 生成；
  - `keys.txt`：全部 key 列表（每行一个），与正式测试的 key 集合一致；
- **共享 value**：全部 key 共享同一份 `value.bin` 内容（与原 datasystem 一致），get 场景通过校验
  返回数据前 64 字节是否与预期一致来判断成功；
- **可复现性**：固定种子保证数据文件被删除后重新生成的内容完全一致；`generate-data` 阶段自动检测
  `value.bin` 大小是否匹配当前 `data_size`，匹配则跳过生成，避免重复浪费；
- **预热数据隔离**：预热使用独立前缀 `mc_warmup*{scenario}*{pid}_*`，测试结束后即删除，不污染
  正式数据（`mc_bench_*`）与持久化数据集。

## 4. 用例矩阵

basic：9 种操作 × 2 种拓扑 = 18 个场景（`configs/basic/` 顶层平铺）：

| 操作族 / 拓扑 | local_node | remote_node |
| --- | --- | --- |
| put / put_batch / put_from | put_local、put_batch_local、put_from_local | put_remote、put_batch_remote、put_from_remote |
| get / get_batch / get_into / get_buffer | get_local、get_batch_local、get_into_local、get_buffer_local | get_remote、get_batch_remote、get_into_remote、get_buffer_remote |
| mixed / mixed_batch | mixed_local、mixed_batch_local | mixed_remote、mixed_batch_remote |

pipeline：正式配置按 reader 节点数归档在 `configs/pipeline/{1,2,4}-nodes/` 下（102 个，见 3.3
档位矩阵表），demo/ 另有 2 个冒烟配置。

## 5. 测试部署架构

```
+-----------------------------------------------------------+
| 编排机（Linux/WSL）                                        |
|   benchmark.py : 读配置 -> 生成 env.sh -> scp 上传(5文件)    |
|                  -> ssh 驱动阶段 -> scp 收集 -> 汇总整理      |
+----------------------------+------------------------------+
                            | ssh / scp（仅上传 5 个文件）
+----------------------------+------------------------------+
| 节点A：mooncake_master + mooncake_store（store 实例）        |
|                                                            |
|   local_node 场景: client 测试进程也运行在本节点              |
+----------------------------+------------------------------+
                            | tcp/ub/rdma
+----------------------------+------------------------------+
| 节点B：仅 client 测试进程                                    |
|                                                            |
|   remote_node 场景: client 测试进程运行在本节点               |
+-----------------------------------------------------------+
```

说明：

- `mooncake_master` 与 `mooncake_store` 由运维手动启动，本套件不管理其生命周期；
- store 实例部署在 `mooncake_store_nodes` 指定的节点上；local 场景 client 与 store 同节点，
  remote 场景 client 在不同节点；
- 每个 benchmark 运行由 `run_benchmark_basic.sh` 在本节点并行拉起 `num_clients` 个
  `mooncake_benchmark_client.py` 进程（basic），每个进程内再开启 `num_threads` 个工作线程；
- **不再上传 JSON 配置文件**：benchmark.py 读取 JSON 后生成 `env.sh`（环境变量形式），仅上传
  `env.sh` + `run_benchmark_basic.sh` + `run_benchmark_pipeline.sh` + `mooncake_benchmark_client.py`
  + `env.mooncake.sh` 五个文件到服务器。

## 6. 输出指标

| 指标 | 含义 | 单位 |
| --- | --- | --- |
| throughput_mb_s / throughput_gb_s | 吞吐量（1024 进制，1 MB/s = 1024² B/s），详见下方公式 | MB/s、GB/s |
| qps | 每秒操作数（total / wall_s） | ops/s |
| total | 总操作数；pipeline 场景按 key 数计（writer 为写入 key 数，reader 为读取 key 数） | ops |
| success / ok、fail、fail_rate | 成功数 / 失败数 / 失败率（fail / total） | ops、% |
| avg_ms / min_ms / max_ms / p99_ms / p999_ms | 时延（平均 / 最小 / 最大 / P99 / P99.9），仅统计成功样本 | ms |
| queue_wait_avg_ms / queue_wait_max_ms / max_depth | reader 队列积压诊断：notify 到达→出队等待（平均/最大）与最大队列深度 | ms、ops |

- 时延基于全部 client 上报的成功操作样本精确统计（失败样本计入 fail/fail_rate，不计入时延百分位）；
- `total_bytes` 与吞吐量仅统计**成功操作**的传输字节数（失败操作未实际搬运数据）；
- 吞吐量公式（均为 1024 进制）：
  - basic：`吞吐 = 成功操作字节数 / 全局墙钟时间`；
  - pipeline 单实例行：`吞吐 = 成功接口调用字节数 / 成功接口调用耗时之和`（接口忙时，剔除
    脉冲定速下的空闲等待，衡量接口真实能力）；
  - pipeline TOTAL 行：`吞吐 = 各实例忙时吞吐之和`（保留多实例聚合视角）；
- 每条结果（CSV 行）同时打印本次运行配置的核心参数：`data_size`、`key_count`、
  `batch_size`（put_batch / get_batch 场景）、`put_ratio`（mixed 场景）、`duration`（仅配置时）；
- **basic 与 pipeline 使用不同 CSV Schema**：basic 保留 `num_clients/key_count/put_ratio/ok` 等列；
  pipeline 采用 `config_name/success/num_threads/notify_threads/writer_count(N*{cpn})` 等 40 列，
  每个 writer 与 reader 各一行 + 各聚合（TOTAL）一行；
- **pipeline 多行输出**：每个 writer 一行（指标取 pipeline_write 时延）、全 writer 聚合一行、
  每个 reader 一行（指标取 pipeline_read 时延，含 node/instance/topology 列以区分不同节点与 store
  的通信路径差异）、全 reader 聚合一行；
  reader 的 pipeline_read 时延为 reader 节点本地对 `store.get` 接口调用的耗时（`time.perf_counter()`
  计时，不依赖跨节点时钟同步；exist 预检与数据校验不计入时延）。writer 时延样本为每次 `put_batch`
  接口调用的耗时，total/success/fail/qps 均按 key 数计（qps = key 数 / duration），TOTAL 行将
  全部 writer 写入的 key 数或全部 reader 读取的 key 数叠加后除以 duration。reader 行附
  queue_wait/max_depth 列用于观察积压是否发生及严重程度（writer 行为空）；
- **basic mixed（put/get）拆分输出**：put 与 get 分别统计、各输出一行（operation 列取
  `put` / `get`，`put_ratio` 列标识混合来源）；
- **benchmark_detail.csv**：pipeline 场景仅保留 writer/reader 的 TOTAL 行（先 writer 后 reader），
  `config_name` 列组首填充便于横向对比。

## 7. 配置文件

测试使用一个 Python 编排程序（`benchmark.py`）+ 服务器端 Python 测试程序
（`scripts/mooncake_benchmark_client.py`）+ Shell 启动脚本（`scripts/run_benchmark_basic.sh` / `scripts/run_benchmark_pipeline.sh`），
行为由 JSON 配置文件驱动。
配置流程：benchmark.py 读取 `cluster.json` + `global.json` + 场景 JSON → 合并生成
`scripts/env.sh`（环境变量形式）→ 连同手工调优面板 `scripts/env.mooncake.sh`
（见 7.2.1）一起 scp 上传到服务器。mooncake_benchmark_client.py
从环境变量读取配置，同时也支持显式 CLI 参数覆写。

### 7.1 configs/cluster.json（集群基础设施）

仓库仅提供 `configs/cluster.example.json` 模板，使用前先复制为 `configs/cluster.json` 并修改
（该文件不入库，已被忽略）。

| 字段 | 说明 |
| --- | --- |
| venv | 服务器端 Python 虚拟环境解释器（如 `/root/venv/bin/python`），所有服务器端脚本均用它执行 |
| nodes | 可用节点列表，每个节点含 `ip`（仅用于 ssh/scp 登录，管理网地址）、`fabric_ip`（内网高性能网卡地址，客户端 local_hostname 使用，可省略则回退到 ip）、`ssh_user`、`ssh_port`、`ssh_key`（空 = 默认密钥，仅支持 ssh 免密登录） |

节点示例：

```
"nodes": {
  "store1": { "ip": "127.0.0.1", "fabric_ip": "127.0.0.1", "ssh_user": "root", "ssh_port": 22, "ssh_key": "" },
  "node1": { "ip": "127.0.0.1", "fabric_ip": "127.0.0.1", "ssh_user": "root", "ssh_port": 22, "ssh_key": "" },
  "writer_node1": { "ip": "127.0.0.1", "fabric_ip": "127.0.0.1", "ssh_user": "root", "ssh_port": 22, "ssh_key": "" },
  "writer_node2": { "ip": "127.0.0.1", "fabric_ip": "127.0.0.1", "ssh_user": "root", "ssh_port": 22, "ssh_key": "" },
  "reader_node1": { "ip": "127.0.0.1", "fabric_ip": "127.0.0.1", "ssh_user": "root", "ssh_port": 22, "ssh_key": "" },
  "reader_node2": { "ip": "127.0.0.1", "fabric_ip": "127.0.0.1", "ssh_user": "root", "ssh_port": 22, "ssh_key": "" }
}
```

节点命名约定：`store1`（store 节点，basic `_local` 场景客户端）、`node1`（非 store 客户端，basic
`_remote`）、`writer_nodeX` / `reader_nodeX`（pipeline 角色节点，n{N} 即 reader_node1..N）。

### 7.2 configs/global.json（业务无关全局超参）

与业务无关的全局超参数与服务器端路径独立放在 `configs/global.json`，由 `--global` 指定
（默认即该路径）；仓库提供 `global.example.json` 模板，本地工作副本不入库。

| 字段 | 说明 |
| --- | --- |
| master_type | **必填**，选择连接 mooncake master 的方式：`"etcd"`（走 etcd_endpoints）或 `"http"`（走 master_addr） |
| etcd_endpoints | **etcd 模式**：经 etcd 连接 master 的完整 URI，如 `etcd://192.168.84.151:2379;192.168.84.149:2379`（分号分隔多节点）。`master_type="etcd"` 时必须非空；客户端把该 URI 原样传给 `setup` 的 `metadata_server` 与 `master_server_addr` |
| master_addr | **http 模式**：mooncake_master rpc 地址（必须用内网高性能网卡 IP），如 `192.168.84.151:50051`；`master_type="http"` 时必须非空 |
| metadata_server | **http 模式**：`setup` 的 metadata_server 参数，如 `http://192.168.84.151:9999/metadata`（master 的 HTTP 元数据端口，必须用内网 IP）；可省略，默认 `http://{master_addr}/metadata` |
| protocol.ub.device | ub 传输使用的设备名（如 `bonding_dev_0`）；tcp 场景不使用 |
| mooncake_store_nodes | store 节点名列表（取自 cluster.json 的 nodes），可多个；仅用于指标行 local/remote 拓扑标注 |
| local_buffer_size | client 本地传输暂存缓冲，建议 ≥ num_threads × data_size |
| tenant_id | 租户标识，默认 "default" |
| data_seed | 数据生成固定随机种子 |
| remote_dir | 服务器端脚本/配置根目录 |
| remote_data_dir / remote_results_dir | 服务器端数据 / 结果目录 |

> 已迁移至 `env.mooncake.sh` 手工调优面板（global.json 同名字段已删除）：`ylt_log_level`、
> `global_segment_size`（更早还有 `mc_hifreq_log_sample_rate`）。SDK 调优旋钮统一由面板管理，
> global.json 仅保留编排决策输入、部署路径与硬件/身份属性。注意覆盖顺序：面板在 env.sh 之后
> source，**即使 global.json 写回同名字段也会被面板覆盖**。

> **master 连接方式由 `master_type` 显式选择**（必填，`"http"` 或 `"etcd"`）。两套地址
> （`etcd_endpoints` 与 `master_addr`/`metadata_server`）可以同时填写，只有选中的一套生效，
> 未选中仅存档。etcd 模式下 env.sh 只导出 `MC_MASTER_TYPE`/`MC_ETCD_ENDPOINTS`（客户端
> 把同一 etcd URI 传给 setup 两参数）；http 模式只导出 `MC_MASTER_TYPE`/`MC_MASTER_ADDR`/
> `MC_METADATA_SERVER`。

### 7.2.1 scripts/env.mooncake.sh（手工调优面板）

mooncake store/SDK 级调优环境变量独立放在 `scripts/env.mooncake.sh`（git 跟踪，手工编辑），
与编排器生成的 `env.sh` 分离——调优项可自由增删改而不会在每次运行时被覆盖。

- **内容**：连接与容错（`MC_STORE_CLIENT_SETUP_RETRIES`/`MC_STORE_WARMUP`/
  `MC_STORE_CLIENT_METRIC_BANDWIDTH`）、客户端资源（`MC_GLOBAL_SEGMENT_SIZE`）、
  ylt RPC 连接池（`MC_YLT_RPC_POOL_*`/`MC_OFFLOAD_RPC_THREAD_NUM`）、日志
  （`MC_HIFREQ_LOG_SAMPLE_RATE`/`MC_LOG_ENABLE`/`MC_LOG_DIR`/`MC_YLT_LOG_LEVEL`/
  `MC_YLT_LOG_MAX_FILE_SIZE`/`MC_YLT_LOG_MAX_FILES`）、传输层（`MC_SLICE_SIZE`/
  `MC_WORKERS_PER_CTX`/`MC_NUM_CQ_PER_CTX`/`MC_MAX_WR`/`MC_URMA_TRANS_MODE`/
  `MC_OFFLOAD_PUSH`）。
- **部署**：`deploy_scripts` 自动随 env.sh / runner / client 一起推送到所有参与节点；
- **加载**：两个 runner 脚本在 `source env.sh` 之后 source 本文件（带 `-f` 防御检查，
  文件缺失不报错）；generate-data / remove-keys / prepare-store / benchmark / pipeline
  全模式生效；
- **覆盖顺序**：`env.sh`（编排器生成）→ `env.mooncake.sh`（同名变量以本文件为准）→
  命令行 inline 注入（如 `MC_YLT_LOG_PATH`，永远最优先）；
- **所有权变更**：以下变量已从 global.json / 编排器硬编码迁移至本文件（原字段/生成行
  已删除，写回 global.json 也会被本文件覆盖）：`MC_HIFREQ_LOG_SAMPLE_RATE`（原
  `mc_hifreq_log_sample_rate`，默认 `1.0` 全量追踪）、`MC_YLT_LOG_LEVEL`（原
  `ylt_log_level`）、`MC_YLT_LOG_MAX_FILE_SIZE`/`MC_YLT_LOG_MAX_FILES`（原编排器
  硬编码 50MB/3）、`MC_GLOBAL_SEGMENT_SIZE`（原 `global_segment_size`，纯客户端 0）。

### 7.3 日志体系

客户端进程有三条日志流，落点如下（`<LOG_FILE>` 为对应 stage 的 client 日志，随
`cfg_name.*` glob 自动拉回本地 `outputs/<ts>/logs/<cfg>/`）：

| 日志流 | 承载内容 | 落点 |
|---|---|---|
| Python stdout（`log()`/`emit()`/RESULT） | 进度打点、META/SUMMARY/RESULT | 合并进 `<LOG_FILE>`（与 C++ stderr 按时间交织） |
| C++ glog（`LOG()`/`MC_LOG()`） | setup 期连接证据：`ConnectToCvmSubmasters`、`connected to submaster`、`Loaded partition routing`、`Version mismatch`、`MountSegment` 等 | **stderr**，被 `2>&1` 合入 `<LOG_FILE>` |
| C++ ylt easylog（coro_rpc 内部） | 连接池事件、建连失败细节（connect/fail/refuse/timeout） | `<LOG_FILE>.ylt.log`（绝对路径，multi-client 带 `===== client.N =====` 分节头） |

关键说明：

- **Python 输出实时可见**：客户端启动即把 stdout/stderr 设为行缓冲，重定向到文件也逐行落盘；
  关键阶段（连接 master、setup 完成、generate-data/remove-keys/prepare 起止、pipeline 各阶段）都有带时间戳的
  `[HH:MM:SS]` 进度行——挂死时最后一行即挂点（如 `connecting to mooncake master (...)` 表示
  卡在 setup/建连，`remove-keys start (scope=full)` 之后无 `remove-keys done` 表示卡在 `remove_all`）。
- **glog 级别不可从 Python 调整**：`store.so` 加载路径不调用 `InitGoogleLogging`，未初始化时
  glog 全部输出到 stderr 且 `GLOG_v`/`minloglevel` 环境变量不生效——只能通过捕获 stderr 获取。
  若其他库恰好初始化过 glog，日志会改写到 `/tmp/python3.<host>.<user>.log.*`，可用
  `ls /tmp/python*.log*` 排除。
- **ylt easylog 配置**：级别经 `MC_YLT_LOG_LEVEL`（env.mooncake.sh，默认 `info`），
  路径由 run_benchmark_*.sh 强制设为绝对路径（修复默认相对 `logs/rpc.log` 随 cwd 丢失的问题）；
  轮转上限 `MC_YLT_LOG_MAX_FILE_SIZE=50MB`、`MC_YLT_LOG_MAX_FILES=3`（均在 env.mooncake.sh）。
  **空 `.ylt.log` 为已知行为**：Mooncake 以 async 写且 `flush_every_time=false`（硬编码）初始化
  easylog，info 级日志量通常不足以撑满 ofstream 缓冲，叠加客户端 `os._exit`（双 free 规避）
  跳过静态析构 flush → 缓冲内容永不落盘。诊断 coro_rpc 问题时在 env.mooncake.sh 临时改
  `MC_YLT_LOG_LEVEL=debug` 靠日志量自然落盘（尾部可能缺最后几 KB）；per-RPC 高频诊断日志
  走 glog（`MC_LOG`）不依赖该文件。未来 Mooncake 将 `flush_every_time` env 化
  （`MC_YLT_LOG_FLUSH`）后取消面板注释即逐条实时落盘。
- **MC_LOG 异步刷盘**：C++ `MC_LOG` 走 ring buffer + 后台线程 + 1s 周期 flush，`kill -9` 会丢
  最后一秒日志。排查挂死时先 `kill -TERM`（atexit flush）或等待 ≥1s 再读文件；编排器 FAIL
  清理路径已按 `kill → sleep 1 → kill -9` 顺序执行。

诊断 grep：

```bash
# glog 侧：连接建立证据
grep -E "ConnectToCvmSubmasters|connected to submaster|Loaded partition routing|Version mismatch" <LOG_FILE>
# ylt 侧：coro_rpc 建连细节（连接池对 submaster 的建连失败在这）
grep -iE "connect|fail|refuse|timeout" <LOG_FILE>.ylt.log
```

### 7.4 configs/<category>/<scenario>.json（场景配置）

场景配置按类别放在子目录中：

- `configs/basic/` — 接口级 benchmark（18 个正式场景）
- `configs/basic/demo/` — 冒烟用例（simple-*，非主测试）
- `configs/pipeline/` — 精排/召排场景（48 个正式场景）
- `configs/pipeline/demo/` — 冒烟用例

basic 场景字段：

| 字段 | 说明 |
| --- | --- |
| scenario | 场景名（basic 场景统一为 "basic"，共享同一 key 空间） |
| work_mode | 业务类型（basic 场景为 operation 名，仅记录） |
| operation | put / get / put_batch / get_batch / mixed（mixed 配置为 "put/get"） |
| protocol | tcp / ub / rdma（ub 的设备名在 global.json `protocol.ub.device` 中配置） |
| client_node | client 所在节点（决定 ssh 目标） |
| key_prefix | key 前缀 |
| key_count / data_size / batch_size / put_ratio | 见 3.2 |
| duration | 压测时长（秒，可选），仅 get / get_batch 支持 |
| params | 并发组合数组 `[{"num_clients": N, "num_threads": T}, ...]`，逐组顺序执行 |
| warmup | 预热参数 `{key_count, data_size, num_clients, num_threads}` |

pipeline 场景字段见 3.3（含 `writer`/`reader` 角色块，无顶层 store_node）。

### 7.5 测试阶段（--mode）

- `generate-data`：按固定种子生成数据文件并持久化
- `remove-keys`：`--scope key-prefix` 仅清理本场景 key（`batch_is_exist` + `batch_remove` 分类统计不存在/失败）；`--scope full` 使用 `remove_all()`，失败即报错退出，无回退
- `prepare-store`：`batch_is_exist` 检查存在性，仅对缺失 key 执行 `put_batch` 补写
- `benchmark`：`--warmup true|false`，预热在正式测量前执行小规模运行
- `pipeline`：生产-消费 pipeline 模式（精排/召排，按 `--role writer|reader` 分派）

### 7.6 各操作类型完整流程

- `put / put_batch`：generate-data → remove-keys → warmup → benchmark
- `get / get_batch`：generate-data → prepare-store（先检查数据是否存在）→ warmup → benchmark
- `mixed`：generate-data → remove-keys → prepare-store → warmup → benchmark

`duration`（可选，全部 basic 操作支持）改变 benchmark 阶段的负载形态：缺省跑一遍结束；设置后
get 家族循环读取预填数据直至到时；put 家族与 mixed 以"完整遍历自己 key 分片"为一轮，put 侧跑满
一轮后客户端清除自己的 key（维护动作不计时、不产生 RESULT）再进入下一轮，直至到时。阶段流程不变。

### 7.6.1 remove-keys 触发时机

层级约定：**场景** = 一次 `benchmark.py` 调用（可含多套配置）；**配置** = 单个配置文件运行。

basic 场景：

| 时机 | 动作 | 日志 |
| --- | --- | --- |
| 场景启动 | 无 | — |
| 配置启动 | remove-all（`--scope full`） | `<配置名>.remove-keys.log` |
| put/put_batch/mixed 每个 combo 运行前 | remove-all（`--scope full`，保证写入隔离） | `<配置名>.remove-keys.log`（覆盖） |
| 配置结束 | 仅删除本配置写入的 key（`--scope key-prefix`） | `<配置名>.remove-keys-end.log` |
| 场景结束 | remove-all（`--scope full`，兜底失败配置遗留） | `final.remove-keys.log`（scp 回 results 的 `logs/` 目录） |

pipeline 场景：

| 时机 | 动作 | 日志 |
| --- | --- | --- |
| 场景启动 | 无 | — |
| 配置启动 | remove-all（`--scope full`） | `<配置名>.remove-keys.log` |
| 配置结束 | remove-all（`--scope full`；pipeline 运行期清空了 `MC_KEY_COUNT`，key-prefix 不可用） | `<配置名>.remove-keys-end.log` |
| 场景结束 | remove-all（`--scope full`，兜底） | `final.remove-keys.log`（scp 回 results 的 `logs/` 目录） |

场景结束的 remove-all 在**最后一个成功配置**的主节点上执行（该配置返回的 runner/日志目录可复用）；
配置失败时其配置结束清理不会执行，因此场景结束兜底必须执行。该步骤失败仅 `[WARN]`，不影响整体结果汇总。

### 7.7 key 格式

正式数据：`mc_bench*{scenario}*{index:08d}`，例：`mc_bench*basic*00000000`（basic 各场景共享同一
key 空间，靠 remove-keys 串行隔离）。
预热数据：`mc_warmup*{scenario}*{pid}*{index:08d}`，测试结束即删除。

## 8. 运行方式

前置条件：

- 集群中 mooncake_master、mooncake_store 已手动启动；
- 编排机 `ssh` / `scp`（OpenSSH）可用，已配置到服务器的密钥免密登录；
- 服务器已安装 mooncake Python 包。

首次使用前，复制集群配置模板并修改：

```
cp configs/cluster.example.json configs/cluster.json
cp configs/global.example.json configs/global.json
```

> `--config` 按给定路径**原样**解析（相对当前目录或绝对路径），不再默认拼接 `configs/` 前缀。

```
# 运行 basic 类别下全部场景（自动跳过 _configs.json 公共默认）
python benchmark.py --scenario basic --all
# 只运行 basic 类别下指定场景
python benchmark.py --scenario basic --config configs/basic/get_remote.json
# 运行 demo 冒烟用例
python benchmark.py --scenario basic --demo
# 运行 pipeline 场景全部（递归收集 1/2/4-nodes 子目录，跳过 demo/ 与 _ 前缀）
python benchmark.py --scenario pipeline --all
# 运行 pipeline 指定场景
python benchmark.py --scenario pipeline --config configs/pipeline/2-nodes/rank/rank_pulse_2w10r_3x_n2.json
# 运行 pipeline 指定目录下全部配置（按 reader 节点数分层的目录）
python benchmark.py --scenario pipeline --config-dir 2-nodes
# 目录递归（含其下所有子目录；demo/ 与 _ 前缀始终跳过）
python benchmark.py --scenario pipeline --config-dir 4-nodes --recursive
# 指定集群/全局配置文件
python benchmark.py --cluster configs/cluster.json --global configs/global.json \
    --scenario basic --config configs/basic/put_local.json
# 关闭预热；仅打印命令不执行
python benchmark.py --scenario basic --config configs/basic/put_local.json --no-warmup --dry-run
# 覆盖传输协议（tcp/rdma/ub，优先级最高，覆盖 JSON 配置值）
python benchmark.py --scenario basic --config configs/basic/put_local.json --protocol tcp
# 独立清理：删除 cluster.json 全部节点的远程测试目录
python benchmark.py --clean-up
# 运行并追加清理阶段（与 --all/--demo/--config/--config-dir 共现时）
python benchmark.py --scenario basic --config configs/basic/get_local.json --clean-up
```

## 9. 结果目录结构

```
outputs/<时间戳，如 20261010-1308>/
├── <配置名>.csv                # 各用例性能明细（按配置文件名；pipeline 为双 Schema 40 列）
├── benchmark_detail.csv        # 全部用例汇总（pipeline 仅 writer/reader TOTAL 行）
└── logs/
    ├── final.remove-keys.log   # 场景结束兜底 remove-all 日志（最后一个成功配置节点）
    └── <配置名>/               # 每个配置一个子目录
        ├── <配置名>.generate-data.log
        ├── <配置名>.prepare-store.log
        ├── <配置名>.remove-keys.log
        ├── <配置名>.benchmark_c{clients}_t{threads}.log
        └── (pipeline) <配置名>.benchmark_c1_t1.reader_port{port}.log / .writer{gid}.log ...
            （脚本级启动失败时可能保留非空 .nohup 诊断文件，成功时自动清理）
```

## 10. 注意事项与待验证项

- API 语义以 `mooncake_api.md` 为准：`put` 为 create-only（key 存在时 no-op 不覆盖），因此 put
  类场景必须保证先 remove-keys 再 put（编排流程已保证，remove-keys 失败会终止场景）；
- `get` 缺失时返回空 bytes `b""`，本套件 value 恒非空，故以空结果判定 MISS；
- 数据路径错误码为 int 负值（如 OBJECT_NOT_FOUND），`remove-keys` 通过 `batch_is_exist` 前置判定区分
  "key 不存在"与"删除失败"，失败即报错退出；
- 构造使用无参 `MooncakeDistributedStore()` + `setup(...)`，`setup` 返回非 0 即报错退出；
- `local_buffer_size` 需满足 `num_threads × data_size` 的并发占用，避免缓冲区不足；
- `generate-data` 阶段自动检测：每次运行前 ssh 检查 `value.bin` 的文件大小是否匹配当前 `data_size`，
  匹配则跳过 generate-data，避免重复生成浪费。种子变更或数据大小变更时自动触发重新生成；
- `--clean-up` 参数：删除 cluster.json 中**全部节点**的远程测试目录（`rm -rf $MC_REMOTE_DIR`）。
  独立使用（不带 --all/--demo/--config/--config-dir）时仅执行清理即退出；与运行参数共现时在跑完全部配置后追加清理。
