# Mooncake Mock（本地端到端验证用）

本目录提供一个 `mooncake` Python 包的**假实现**，用于在没有 Mooncake 部署的机器上
完整跑通基准测试流程：编排器（benchmark.py）→ 真实 ssh/scp → run_benchmark_basic.sh / run_benchmark_pipeline.sh →
mooncake_benchmark_client.py（真实代码）。**唯一被替换的是 `mooncake.store` 接口**，
其余全部是真实代码。

## 结构

```
mock/
└── mooncake/            # 假 mooncake 包
    ├── __init__.py
    └── store.py         # MooncakeDistributedStore 假实现（file-per-key 存储）
```

`store.py` 实现了客户端用到的全部接口，签名对齐真实 Mooncake Python API：

| 接口 | 说明 |
| --- | --- |
| `setup(local_hostname, metadata_server, global_segment_size, local_buffer_size, protocol, rdma_devices, master_server_addr, tenant_id, ...)` | 返回 0（成功） |
| `put(key, value)` | 返回 0，写入 key |
| `get(key) -> bytes` | 缺失返回 `b""`（与真实语义一致） |
| `put_batch(keys, values)` | 返回 0 |
| `get_batch(keys) -> List[bytes]` | |
| `remove_all(force)` / `batch_remove(keys, force)` | 清理 |
| `batch_is_exist(keys)` / `is_exist(key)` | 存在性（1/0） |
| `close()` | 返回 0 |

## venv 注入方式（推荐）

用虚拟环境注入 mock，**无需改动任何产品代码、无需 PYTHONPATH**：

```bash
# 1. 创建 venv 并把 mock 包装进 site-packages
python3 -m venv /tmp/opencode/mockvenv
cp -r mock/mooncake /tmp/opencode/mockvenv/lib/python3.*/site-packages/

# 2. 验证
/tmp/opencode/mockvenv/bin/python -c "from mooncake.store import MooncakeDistributedStore; print(MooncakeDistributedStore().setup())"
# 0

# 3. 本地测试集群配置中把 venv 指向 mockvenv（configs/cluster.json 的 venv 字段）
{
  "venv": "/tmp/opencode/mockvenv/bin/python",
  "nodes": { "writer_node1": {"ip": "127.0.0.1", "fabric_ip": "127.0.0.1", ... } }
}
```

此后 `python benchmark.py --scenario pipeline --demo` 等命令走**真实 ssh/scp + 真实客户端
代码**，仅 store 层使用本 mock。

> 未来接入真实环境：把 `configs/cluster.json` 的 `venv` 换成安装了 mooncake SDK 的
> 解释器路径即可，**零代码变更**。

## 存储目录

mock store 的数据落在 `MOCK_STORE_DIR` 环境变量指定的目录（默认 `/tmp/mooncake_mock_store`），
每次运行前建议清空。键按 `md5(key)` 映射为单个文件，支持跨进程并发读写
（写用临时文件 + `os.replace` 原子替换，读按 close-to-open 语义即可见）。

```bash
rm -rf /tmp/mooncake_mock_store
```

## 局限

- 单机进程内语义：模拟"所有 client 共享同一个 store"，不模拟真实分布式网络的带宽/时延；
  因此**性能数字无参考价值**，仅用于验证流程与指标解析正确性；
- `setup()` 恒定返回 0，不校验参数；
- 不模拟 TTL、副本、lease 等真实 store 语义（当前基准也未用到）。
