# k8s 场景下 store 清理机制分析（remove-analysis）

> 状态：**分析落盘，暂不实施**（该需求转交同事完成）。
> 代码基线：`2661008`（2026-11-12）。
> 目标：回答"运行级 remove-all"和"配置级清理"在 k8s 下分别是如何执行的（pod 产生、命令调用者、调用结果），并给出正确性评估与缺口。

---

## 0. 结论摘要

- **运行级**：只有**结束时**的 remove-all（`main()` 的 sweep-end），**运行开始时没有** run 级 remove-all（启动只做进程台账 reap、遗留 namespace reap、etcd `mcbench/` 前缀清理、k8s prerun 脚本，不触碰 store）。
- **配置级**：
  - basic：配置开始**无条件** full remove；每个写类 combo 前**再** full remove；配置结束用 **key-prefix** 只删自己的键；失败清理**不清 store**。
  - pipeline：每个 data_size 开始 full remove；配置结束 full remove；失败清理**额外兜底** full remove。
- 所有 remove 都在 **benchmark pod 内**通过 `kubectl exec` 运行与测试同源的 runner/client，访问**外部 master**；返回码全链路透传。配置级 remove 失败会让配置 FAIL；run 级 sweep 与失败清理为 WARN-only。
- 主要缺口：run-start 无 remove（k8s 下此时无 pod）、全失败 run 跳过 sweep、basic 失败无 store 兜底、basic start 无 verify、full scope 是**全局清空**（store 必须专用）、remove 无超时。

---

## 1. 运行级 remove-all（sweep-end）

### 1.1 触发条件与 pod 来源

| 项 | 事实（源码位置） |
|---|---|
| 触发 | 全部配置循环结束后，且**至少一个配置成功**（`sweep_remove` 非空）— `benchmark.py:5607`、`:5652` |
| pod 来源 | **最后一个成功配置的 primary pod**：basic = 该配置的 client pod；pipeline = primary writer pod |
| 为何此时有 pod | 配置成功后**不提前删 pod**：basic 成功路径不调 `cleanup_pods`；main 把 `cleanup_pods` 放在 sweep **之后**（注释见 `:3287-3293`、`:4360-4370`；执行见 `:5672`） |
| 执行位置 | sweep 之后：`cleanup_pods`（删 Deployment/最后一个 pod/ConfigMap）→ CSV/报表 → `cleanup_k8s_namespace`（删整个 namespace）— `:5672`、`:5705` |
| 跳过条件 | 所有配置都失败 → `sweep_remove=None` → **跳过 sweep**；失败时 `_k8s_keep_ns=True`，namespace/pod 保留等 STOP/`--clean-up`（`:3310`、`:4368`） |

### 1.2 调用链

```
benchmark.py main()
  → run_stage(host, nd, runner, "remove-keys", remote_log_dir, "final",
              extra="--scope full", ...)                      # :5655
      → run_ssh_with_progress()                                # :2764
          → is_k8s_node(node) 为真
              → controller_stream([kubectl, -n <ns>, exec, <pod>, --, bash, -c, cmd],
                                   quiet=?)                     # utils.py:299
                  → 命令: bash <pod_ws>/scripts/run_benchmark_pipeline.sh remove-keys \
                          <pod_ws>/logs/<ts>/<cfg>/<pod>/<cfg>.final.remove-keys.log \
                          --scope full
                      → runner 内: python <client> --mode remove-keys --scope full
                          → store.remove_all(force=True)    # client:401-412
```

- runner 脚本产出 `<LOG_FILE>.exitrc`（`run_benchmark_pipeline.sh:79`），但 sweep 的判定以 **kubectl exec 的 rc** 为准。
- 环境：pod 内挂载的 ConfigMap（`env.sh` + `env.mooncake.sh` + 脚本）与 benchmark 完全一致；master/store 为外部 Service。

### 1.3 结果判定与后续动作

1. `controller_stream` 返回 rc；`run_ssh_with_progress` 中 **rc≠0 抛 RuntimeError**（`:2840-2845`）。
2. sweep-end 被 `try/except` 包裹 → **WARN-only**（`:5653-5667`）：`[WARN] sweep-end remove-keys failed: ...`。
3. 随后 `scp_pull(<remote_log_dir>/final.remove-keys.log*)` 拉回本地 `outputs/<ts>/logs/`。
4. `_verify_store_clean(cluster, globals_cfg, "final", "sweep end")`（`:5663`；实现 `:548`）：
   - 经 master metrics 复核 store 是否为空（探测链见主 README 的 master_metrics 说明）；
   - 默认 **WARN**，仅 `global.json pipeline.require_clean_store=true` 时 **硬失败**。
5. 最后 `cleanup_pods` → `cleanup_k8s_namespace`（`_k8s_keep_ns` 时不删）。

---

## 2. 配置级清理（前/后）

### 2.1 basic（pod = 该配置的常驻 client pod）

| 阶段 | 实现 |
|---|---|
| pod 产生 | 先建 ConfigMap（`:3087`）→ `ensure_k8s_pod`（`:1421-1491`）：`kubectl apply` pod → `wait_k8s_pod_ready` → 注册 `node{_pod,_ns,_controller,ip=podIP}`。整配置复用该 pod（`sleep infinity` + `kubectl exec`） |
| 脚本下发 | `deploy_scripts` 对 k8s 节点只 `mkdir -p` 日志目录（脚本经 ConfigMap 只读挂载） |
| **配置开始（前）** | `try` 内第一步：**无条件** `remove-keys --scope full`（`:3113-3117`，在 pod 内）。**注意：之后没有 `_verify_store_clean`** |
| **每个 combo 前** | put 族或 mixed（`need_remove_before_run`，`:3119`）→ 再 `remove-keys --scope full`（`:3188-3193`）；纯 get 族不 pre-remove，但 `prepare-store` 自身会先 `store.remove_all(force=True)` 再预填（`client:442-446`） |
| **配置结束（后）** | `remove-keys --scope key-prefix`（`remove-keys-end`，`:3273-3276`，`run_ssh` 在 pod 内）→ 只删本配置 `key_prefix+scenario+key_count` 的键；rc≠0 抛错（`utils.py:559-580`） |
| 失败清理 | `fail_cleanup_basic` **只拉日志，不清 store**（`:2732-2739`） |

### 2.2 pipeline（pod = per-role Deployment 的副本；pod 即进程）

| 阶段 | 实现 |
|---|---|
| pod 产生 | 先算 N=Ready 节点与 plan → 建 ConfigMap → `try` 内建 per-role Deployment 并等 rollout（`:3740`）→ 按实际 pod 列表逐 entry 物化 pod 名，**数量不足直接 FAIL**（`:3742-3765`）→ 注册 node（pod IP） |
| **每个 data_size 前** | `generate-data`（每 pod 一次，缓存）→ **`remove-keys --scope full`**（primary writer pod，`:3876-3881`）→ `_verify_store_clean("{ds} start")`（`:3882`）→ 清 etcd 前缀 → 启动 reader/writer |
| **配置结束（后）** | `remove-keys --scope full`（`remove-keys-end`，`:4314-4318`）→ `_verify_store_clean("config end")` → 按需拉原始日志 |
| 失败清理 | `fail_cleanup_pipeline` 第 4 步：客户端全死后**额外发一次 full remove**（primary pod，best-effort，WARN）→ `_verify_store_clean("fail cleanup")` → 拉日志（`:2702-2717`） |

### 2.3 客户端语义（所有 remove 的最终执行体）

`scripts/mooncake_benchmark_client.py`：

- `--scope full` → `store.remove_all(force=True)`；返回 `<0` 判失败 `exit 1`；成功打印 `remove-keys done (scope=full, cleared=N)`（`:401-412`）。
- `--scope key-prefix` → `batch_is_exist` + `batch_remove` 按 `key_prefix+scenario+key_count`；结果长度不符或存在失败键 → 打印样本并 `exit 1`（`:413-441`）。
- `prepare-store` → 先 `store.remove_all(force=True)`，再逐键预填（`:442+`）。

---

## 3. 正确性评估

### 3.1 成立的部分

1. **通道一致**：remove 与 benchmark 使用同一 pod 镜像、同一 ConfigMap（env.sh/env.mooncake.sh）、同一 venv、同一 runner；不依赖节点 ssh，k8s 下零额外权限。
2. **时序正确**：
   - 配置前 remove：pod 必已 Ready（basic 先 `ensure_k8s_pod`；pipeline 先 Deployment rollout 且校验 pod 数不足即 FAIL）。
   - 配置后 remove：pod 仍存活（常驻/infinity）。
   - run 级 sweep：依赖"最后一个成功配置的 pod 不提前删"，main 顺序 `sweep → cleanup_pods → 删 namespace` 与之吻合。
3. **结果链路完整**：`kubectl exec rc → runner exit code → client exit code` 全透传；配置开始/结束/每 combo 的 remove 失败都会让配置 FAIL；sweep/失败清理 WARN-only。
4. **可观测**：remove 日志按 pod 子目录落 hostPath 并拉回（`<pod>/<cfg>.remove-keys*.log`）；`[k8s] $ kubectl … exec …` 原文入日志；sweep 单独拉 `final.remove-keys.log*`。
5. **并发隔离**：同 master / 同 etcd 的实例被并发守卫拒绝；rank+recall 各自 namespace/master，互不清理。

### 3.2 风险与缺口（供转交时决策）

| # | 问题 | 影响 | 备注 |
|---|---|---|---|
| 1 | **运行开始时没有 run 级 remove-all** | 上次崩溃残留只能等下一个配置开始或下次成功 run 的 sweep | k8s 下 run 开始时**没有 pod**，需"一次性清理 pod"或并入 CLEAN-UP 流程，需先定方向 |
| 2 | **全部配置失败 → 跳过 sweep** | store 残留 + namespace/pod 保留（后两者是刻意的现场保留） | 可考虑失败时也做一次 best-effort run 级 sweep |
| 3 | **basic 失败清理不清 store**（pipeline 有） | 失败后残留到下一个配置开始 | 建议与 pipeline 对齐 |
| 4 | **basic 配置开始 remove 后无 `_verify_store_clean`**（pipeline 每 ds 都有） | 清空失败不易被发现 | 建议补 verify（默认 WARN） |
| 5 | **配置结束清理失败会把"数据已完整"的配置标 FAILED** | 语义上是"清理失败=场景失败" | 需确认是否接受；pipeline 失败清理还会再试一次 |
| 6 | **`--scope full` = 全局 `remove_all(force=True)`**（prepare-store 也如此） | store 必须**专用**；共享 master 时会误删非 benchmark 数据 | 并发守卫只防同 master 的实例并发，防不了"其他业务也在用" |
| 7 | **remove 阶段无超时**（仅 STOP 可打断） | 大 store 的 remove_all 可能长时间阻塞 | `controller_stream` 只在 stop_requested 时终止 |
| 8 | Deployment 异常重建后，sweep 仍指向原 primary pod 名 | sweep 的 exec 可能失败 → 仅 WARN | 可接受，但需知晓 |
| 9 | 只删最后一个配置的 pod/ConfigMap，早前配置的残留 | 由最终的 namespace 删除兜底 | 失败保留 namespace 时全部保留（设计） |

---

## 4. 真机快速验证清单

1. **时序**：run 日志中 `[k8s] deployment … ready` 之后才出现 `remove-keys … running/done rc=0`；`sweep-end remove-keys` **早于** `delete deployment`。
2. **内容**：`outputs/<ts>/logs/<cfg>/<pod>/<cfg>.remove-keys*.log` 内含 `remove-keys done (scope=full, cleared=N)` 或 `(scope=key-prefix, removed=…)`。
3. **调用证据**：日志中的 `[k8s] $ kubectl -n <ns> exec <pod> -- bash -c …` 行（verbose 下更完整）。
4. **失败路径**：`[FAIL] fail-cleanup: store remove-all issued`、`[WARN] sweep-end remove-keys failed`、`[verify] … store not fully empty`（依据 `require_clean_store` 决定是否硬失败）。
5. **clean 校验**：`_verify_store_clean` 的 `when` 取值应依次出现：`<ds> start` / `config end`（pipeline）、`fail cleanup`（pipeline 失败）、`sweep end`（run 结束）。

---

## 5. 建议的修复候选（未实施）

1. run-start remove（k8s）：新方向二选一 ——
   - (a) 一次性清理 pod（临时 pod 挂同样的 env，跑 remove-keys 后删除）；
   - (b) 不做自动 run-start remove，把"清残留"并入 CLEAN-UP（显式、带确认）。
2. 失败时也做 best-effort run 级 sweep（不依赖"有配置成功"）。
3. basic 失败清理补 store remove（与 pipeline 对齐）。
4. basic 配置开始 remove 后补 `_verify_store_clean`。
5. 为 remove 阶段加超时（例如 60s，超时 WARN 并按失败处理/保留现场）。
6. 明确文档：`--scope full` 为全局清空，store 必须专用。
