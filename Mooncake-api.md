# Mooncake Python API 接口清单（数据面）

来源：`mooncake-integration/store/store_py.cpp`（`mooncake.store` 扩展模块，
`MooncakeDistributedStore` 绑定于 2065 行起）与 `mooncake-integration/store/engram_store_py.cpp`。
行为样板：`mooncake-wheel/tests/test_distributed_object_store.py`（覆盖下述梯队 ①②）。

## 1. MooncakeDistributedStore 数据面接口

### 写（put 家族）

| 接口 | 签名 | 返回 | 语义 / 前置条件 | 梯队 |
|---|---|---|---|---|
| `put` | `put(key, value, config=ReplicateConfig())` | int（0 成功） | 单 key 写；value 为 bytes-like | ① |
| `put_batch` | `put_batch(keys, values, config=...)` | int | 整批一次调用，一次写入 batch_size 个 key | ① |
| `put_from` | `put_from(key, buffer_ptr, size, config=...)` | int | 零拷贝写；buffer 须先 `register_buffer(ptr, size)` | ② |
| `put_parts` | `put_parts(key, *parts, config=...)` | int | 分段写入同一 key（parts 为 1-D bytes-like） | ③ |
| `batch_put_from` | `batch_put_from(keys, buffer_ptrs, sizes, config=...)` | list[int] | 批量零拷贝写 | ③ |
| `batch_put_from_multi_buffers` | `(keys, all_buffer_ptrs, all_sizes)` | — | 多 buffer 变体 | 不进基础集 |

### 读（get 家族）

| 接口 | 签名 | 返回 | 语义 / 前置条件 | 梯队 |
|---|---|---|---|---|
| `get` | `get(key)` | bytes（缺失 `b""`） | 单 key 读 | ① |
| `get_batch` | `get_batch(keys)` | list[bytes] | 整批一次调用 | ① |
| `get_into` | `get_into(key, buffer_ptr, size)` | int64（读入字节数，<0 失败） | 零拷贝读入注册 buffer；size 不足返回负值 | ② |
| `get_buffer` | `get_buffer(key)` | BufferHandle | 零拷贝句柄：`.ptr()` / `.size()` / `__len__` | ② |
| `batch_get_buffer` | `batch_get_buffer(keys)` | list[BufferHandle] | buffer 批量（get_buffer 的 batch 版） | 不进基础集 |
| `get_into_ranges` | `(buffer_ptrs, all_keys, all_dst_offsets, all_src_offsets, all_sizes)` | — | 多对象多 range 读入多 buffer | ③ |
| `batch_get_into` | `batch_get_into(keys, buffer_ptrs, sizes)` | list[int] | 批量零拷贝读 | ③ |
| `batch_get_into_multi_buffers` | `(keys, all_buffer_ptrs, all_sizes)` | — | 多 buffer 变体 | 不进基础集 |

### buffer 管理与辅助

| 接口 | 说明 |
|---|---|
| `register_buffer(ptr, size)` / `unregister_buffer(ptr)` | 零拷贝接口的前置登记（put_from/get_into 必须） |
| `is_exist(key)` → int(1/0) | 存在性 |
| `batch_is_exist(keys)` → list[int] | 批量存在性 |
| `remove(key, force=False)` / `batch_remove(keys, force)` / `remove_all(force)` | 删除 |
| `get_size(key)` → int64 | 对象大小 |
| `get_hostname()` | 本客户端主机名 |

## 2. 梯队分类（按"打通流程"目标）

- **① 数据面主干（必测）**：`put`、`get`
- **① 批量语义（必测）**：`put_batch`、`get_batch`
- **② 零拷贝路径（强烈建议）**：`put_from`、`get_into`（需 `register_buffer`）
- **② buffer 协议（建议）**：`get_buffer`（BufferHandle）
- **③ 语义边界（补齐）**：`put_parts`、`batch_put_from`、`batch_get_into`、`get_into_ranges`
- **④ 按需**：其余组合/变体
- **不建议进基础集**：`batch_put_from_multi_buffers`、`batch_get_into_multi_buffers`、
  TP/parallelism 变体（`*_with_tp`/`*_with_parallelism`）、`get_buffer` 的 batch 版
  （`batch_get_buffer`）——它们是上表接口的组合或变体，基础打通后再按需加。

## 3. EngramStore（recall 可选，预留）

`EngramStore`（embedding 专用存储，`engram_store_py.cpp`）写读模型与 KV 不同：
按词表灌入、按 row id 查询。

| 接口 | 签名 | 语义 |
|---|---|---|
| 构造 | `EngramStore(layer_id, config, store=None)` | `store` 可选传入底层 `MooncakeDistributedStore` |
| `populate` | `populate(embedding_buffers)` | 按 head 灌入词表 embedding（`list[np.float32]`，长度=num_heads） |
| `lookup` | `lookup(row_ids)` → ndarray `[B, L, H, D]` | 按预计算 row id 查询 embedding |
| 辅助 | `get_store_keys` / `remove_from_store` / `get_embedding_dim` / `get_num_heads` / `get_table_vocab_sizes` | |

## 4. 基准适配状态

| 接口 | basic | pipeline | 状态 |
|---|---|---|---|
| put / get | ✅ | writer=put / reader=get | 已支持 |
| put_batch / get_batch | ✅（含 mixed `put_batch/get_batch`） | writer=put_batch / reader=get_batch | 已支持 |
| put_from / get_into / get_buffer | ✅ | writer=put_from / reader=get_into、get_buffer | 已支持 |
| ③ 语义边界接口 | — | — | 计划中 |
| EngramStore populate/lookup | — | 后续（writer.operation/reader.operation 指定） | 预留 |
