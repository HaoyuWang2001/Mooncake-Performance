# Mooncake-Performance WebUI — HTTP API

服务启动：`python -m webui.backend.main [--port 8080] [--bind 127.0.0.1]`（`--bind 0.0.0.0` 开放局域网）。
前端为 vendored **Vue3 + Element Plus**（`webui/web/vendor/`，约 1.8MB，无构建步骤、离线可用；URL 前缀 /static/）。后端结构：`backend/{main,paths,common,runmgr}.py + routers/`（按域拆分），测试 `python -m pytest webui/tests`。
页面与 API 使用同一套后端。
所有 API 返回 JSON（`Content-Type: application/json`）；错误形如 `{"error": "..."}` 并带恰当 HTTP 状态码
（400 参数错 / 403 路径越界 / 404 不存在 / 409 槽位占用 / 422 校验失败）。

约定：
- `path` 参数一律是**相对仓库根**的 POSIX 风格路径（如 `configs/basic/put.json`）
- 文件读取白名单：`outputs/`、`webui/data/runs/`；配置读写白名单：`configs/`、`schedule/`
- benchmark 与 scheduler 任务共用单一执行槽：benchmark 运行中（或 scheduler 任务执行中）新 run 请求 → 409
- scheduler 等待期间发起 benchmark → 允许，响应携带 `warnings`

---

## 系统

### GET /api/status
```json
{"version":"1.0","platform":"posix","python":"3.12.3","root":"/path/Mooncake-Performance",
 "paths":{"outputs":"outputs","runs":"webui/data/runs"},
 "active_run":{"id":"r20261029-223001-001","status":"running",...} | null,
 "active_runs":[{"id":"r...","status":"running",...}, ...],   // 所有活跃 run（多实例并发）
 "scheduler":{"id":"s20261029-223005-002","state":"waiting",...} | null}
```

### GET /api/help
返回本文档：`{"markdown":"# Mooncake-Performance WebUI ..."}`

---

## 配置浏览 / 编辑

### GET /api/configs
配置清单：`general` = configs 根下全部 `*.json`（过滤 `*.example.json` 模板，含
cluster/global/profile 及任意其他通用文件）；`categories` = configs 下**每个非 `_`
开头子目录**的配置树 `{name, path, dirs[], files[]}`（恒含 demo/，`_` 前缀碎片排除）
与 `config_count`；`cluster_candidates` / `global_candidates` / `profile_candidates`
由 general 按文件名前缀推导（运行框下拉数据源）。

### GET /api/config?path=configs/basic/put.json
→ `{"path":"...","content":"<原文>"}`

### PUT /api/config
请求体：`{"path":"configs/basic/put.json","content":"{...}"}`
- 先校验：JSON 语法 + 场景配置语义（复用编排器自身的 `data_size_list`/`pipeline_plan`/`_configs.json` 继承合并）
- 通过后：旧文件备份到 `webui/backups/` → 原子写回
- → `{"ok":true,"path":"...","backup":"webui/backups/..."}`
- 校验失败 → 422 `{"error":"data_size must be ..."}`

### POST /api/config/validate
`{"path":"...","content":"..."}` → `{"ok_path":true,"errors":[]}`（只校验不落盘）

---

## 运行 benchmark

### POST /api/run
请求体：
```json
{
  "selector": {
    "dirs": ["configs/basic"],          // 可选；目录数组（恒递归，含 demo/，_ 碎片排除）
    "files": ["configs/basic/get.json"] // 可选；文件数组
  },
  "flags": {
    "mode": "ssh",                         // ssh | k8s
    "launch_mode": "single",               // single | rank_recall（仅 k8s）
    "dry_run": false, "no_warmup": false, "no_plot": false,
    "verbose": false,                     // 详细日志（上传路径/逐文件拉取/kubectl 命令/pod 详情）
    "protocol": null,                     // null | "tcp"|"rdma"|"ub"
    "cluster": "configs/_ssh/cluster.json",
    "global": "configs/_ssh/global.json",
    "profile": "configs/_ssh/profile.json"  // 空/缺省 = <mode-dir>/profile.json；三者都必须存在
  }
}
```
selector 语义：`dirs` + `files` 至少一项非空；**dirs 先执行、files 后执行**（CLI 映射为
`--config-dir a,b` + `-f x,y` 多值形式）。旧 `{type, path}` 格式已移除（400）。

`launch_mode=rank_recall` 时不用单任务的 `cluster/global`，改传以下四项：
```json
{
  "rank_cluster": "configs/_k8s/cluster.16node-rank.json",
  "rank_global": "configs/_k8s/global.rank.json",
  "recall_cluster": "configs/_k8s/cluster.16node-recall.json",
  "recall_global": "configs/_k8s/global.recall.json"
}
```
所选目录/文件会先展开并解析 `_configs.json` 继承，再按有效 `work_mode` 分组；rank 或 recall
任一为空、或包含其他 work_mode，父任务即失败。两个 benchmark 并发执行，日志带角色前缀，
结果写入同一 `outputs/<run>/rank` 与 `outputs/<run>/recall`；共享 `profile` 与通用运行选项。
→ `201` 意义上返回 `{"run":{"id":"r...","status":"running","cmd":"..."},"warnings":[]}`
共享资源冲突 → `409 {"error":"shared resource in use by instance ..."}`（见「实例管理」并发规则）。

> WebUI 性能测试页不再直接调用 /api/run：它先「创建实例」（`POST /api/instances`，
> 只存表单快照不运行），再由实例卡片/详情页触发运行。`/api/run` 保留给脚本/CLI 式调用。

### 并发规则（多实例）
多个实例可同时运行，但共享资源的组合会被拒绝（409，错误信息标明占用实例与资源）：
- k8s：`(controller.ip, kubecontext, namespace)` 相同 → 冲突（同一批 pod/Deployment）
- SSH：节点 IP 集合相交 → 冲突（端口段 9101/9201/9301+ 与共享 value.bin）
- global：etcd 端点相交、`master_addr` 相同、`master_metrics_url` host 相同 → 冲突
  （rendezvous `mcbench/` 前缀会被互清；同 store 容量会被双方同时按 0.9× 预算打爆）
- 同一实例已有活跃 run → 409；scheduler 任务执行中 → 409
- 不同集群 + 不同栈（etcd/master）→ 可真正并行；每个 run 使用独立
  `webui/data/runs/<id>.state` 台账（Rank+Recall 子任务为 `<id>.state.rank|recall`）

### POST /api/cleanup
standalone clean-up（不跑场景；不需要 profile）：
```json
{"flags": {"mode": "ssh|k8s", "cluster": "configs/_k8s/cluster.json",
           "global": "configs/_k8s/global.json"}}
```
- k8s：删除该 cluster 配置的 namespace（级联 pods/configmaps）与 controller workspace；
- SSH：`rm -rf remote_dir`（cluster.json 所有节点）；
- 本地 `outputs/` 与 run 日志保留；不可恢复；
- 受并发守卫约束：与活跃 run 共享集群/等栈时 409；scheduler 任务执行中 → 409；
- → `{"run":{"id":"r...","status":"running",...},"warnings":[]}`，清理过程写入该 run 的日志。

### GET /api/runs
历史运行列表（最多 60 条，新→旧）：`{"runs":[<run meta>...]}`。
run meta 字段：`id/kind/cmd/argv/log/status/stopping/rc/t0/t1/elapsed/ts_dir/meta`；`ts_dir` 如 `outputs/20261029-2230`（dry-run 无）。

### POST /api/cleanup
standalone clean-up（不跑场景；不需要 profile）：
```json
{"flags": {"mode": "ssh|k8s", "cluster": "configs/_k8s/cluster.json",
           "global": "configs/_k8s/global.json"}}
```
- k8s：删除该 cluster 配置的 namespace（级联 pods/configmaps）与 controller workspace；
- SSH：`rm -rf remote_dir`（cluster.json 所有节点）；
- 本地 `outputs/` 与 run 日志保留；不可恢复；
- 受并发守卫约束：与活跃 run 共享集群/等栈时 409；scheduler 任务执行中 → 409；
- → `{"run":{"id":"r...","status":"running",...},"warnings":[]}`，清理过程写入该 run 的日志。

### GET /api/runs/{id}
单条 run meta。404 若不存在。

### POST /api/cleanup
standalone clean-up（不跑场景；不需要 profile）：
```json
{"flags": {"mode": "ssh|k8s", "cluster": "configs/_k8s/cluster.json",
           "global": "configs/_k8s/global.json"}}
```
- k8s：删除该 cluster 配置的 namespace（级联 pods/configmaps）与 controller workspace；
- SSH：`rm -rf remote_dir`（cluster.json 所有节点）；
- 本地 `outputs/` 与 run 日志保留；不可恢复；
- 受并发守卫约束：与活跃 run 共享集群/等栈时 409；scheduler 任务执行中 → 409；
- → `{"run":{"id":"r...","status":"running",...},"warnings":[]}`，清理过程写入该 run 的日志。

### GET /api/runs/{id}/log?offset=0&stream=all|rank|recall
增量日志：`{"offset":<新字节偏移>,"size":<总字节>,"content":"<新增文本>","eof":bool}`
单次最多 512KB；`offset` 为字节偏移（首次传 0，之后传上次返回值）。
`stream=rank|recall` 只返回组合实例（Rank+Recall 并行）对应子任务的行并剥离
`[rank] `/`[recall] ` 前缀；分页始终停在行边界（跨块残行留到下一页），默认 `all` 为原始日志。

### POST /api/runs/{id}/stop
**优雅停止**：写 `webui/data/runs/<id>.stop` 并向进程组发送 SIGINT/CTRL_BREAK，
benchmark 先进入清理阶段（k8s 删除 namespace/pods；SSH 按 `.state` 台账 kill
client/stage/profiling 进程，reader 先 HTTP /shutdown）。默认 grace 60s
（环境变量 `MCWEB_STOP_GRACE` 可调），超时后强制 kill；后台线程再做一次兜底清理。
停止期间 `status` 仍为 `running` 且 `stopping=true`，完成后变 `stopped`。
→ `{"ok":true,"run":{...stopping:true}}`（已结束的 run 调用也返回 ok）

---

## 实例管理（instances）

实例 = 一份表单快照（`selector` + `flags`，与 POST /api/run 同构）构成的可复用测试定义；
创建本身**不运行**。每次运行产生一个 run（`meta.instance` 指回实例），日志/停止/守卫沿用 run 机制。

### POST /api/instances
```json
{"name": "rank-8mb（可空=自动命名）", "selector": {...}, "flags": {...}}
```
校验与 POST /api/run 相同（配置存在性、launch_mode 规则），但不占用运行槽。
→ `{"instance":{"id":"i...","name":...,"status":"created","launch_mode":"single","mode":"k8s",
   "flags":{...},"selection":{"dirs":[],"files":[]},"last_run":null}}`

### GET /api/instances
`{"instances":[<instance summary>, ...]}`（新→旧）。`status` 派生自活跃 run 或最近持久化 run
（`created|running|stopping|done|failed|stopped`），`last_run` 含 `id/status/rc/elapsed/ts_dir/cmd`。

### GET /api/instances/{iid}
`{"instance":<summary>,"runs":[<run meta>, ...]}`（该实例全部运行记录）。

### POST /api/instances/{iid}/run
启动该实例的一次运行（表单快照 → benchmark argv）。守卫/冲突语义同 `POST /api/run`。
→ `{"run":{...},"warnings":[]}`；同实例已在运行或共享资源冲突 → 409。

### POST /api/instances/{iid}/stop
停止该实例当前的活跃 run（优雅停止语义同 `POST /api/runs/{id}/stop`）；未在运行 → 409。

### POST /api/instances/{iid}/rename
```json
{"name": "新名称"}
```
→ `{"instance":<summary>}`。

### DELETE /api/instances/{iid}
删除实例记录（移入 `webui/backups/instances-trash/`）；运行记录/日志/outputs **保留**。
运行中 → 409。

---

## 结果浏览

### GET /api/outputs
`{"outputs":[{"ts":"20261029-2230","mtime":"...","has_detail":true,"csv_count":3},...]}`（新→旧）

### GET /api/outputs/{ts}/tree
`{"ts":"...","files":["benchmark_detail.csv","logs/put/put.benchmark_c1_t2_64KB.log",...]}`

### POST /api/outputs/{ts}/rename
请求体：`{"name":"put-3nodes"}`
- 新名需匹配 `[0-9A-Za-z._-]+`，不得以 `.` 开头、不得与已有结果重名（409）
- 直接重命名 `outputs/<旧名>` → `outputs/<新名>`，并同步更新 run 历史里的 `ts_dir`
- → `{"ok":true,"ts":"新名","from":"旧名","renamed":true}`（同名时 `renamed:false`）

### DELETE /api/outputs/{ts}
把结果目录**移入回收站** `webui/backups/outputs-trash/<ts>`（重名自动追加时间戳），
并把指向它的 run 历史 `ts_dir` 置空。
→ `{"ok":true,"ts":"...","trash":"webui/backups/outputs-trash/..."}`

### POST /api/outputs/delete
请求体：`{"names":["20261029-2230","..."]}`（批量删除，同样移入回收站）
→ `{"deleted":["..."],"failed":[{"ts":"...","error":"..."}]}`

### GET /api/file?path=outputs/.../xxx.log&mode=text&offset=0
- `mode=text`（默认）：`{"content":"...","offset":N,"size":M,"eof":bool}`；可用 `tail=65536` 直接取尾部 N 字节
- `mode=raw`：原始字节流（PNG 等），`Content-Type` 按扩展名

### GET /api/csv?path=outputs/.../benchmark_detail.csv&max_rows=5000
→ `{"headers":[...],"rows":[[...],...],"truncated":false}`

---

## 调度（schedule）

计划文件格式与 `schedule/run_schedule.py` 完全一致（`wait_seconds`/`on_fail`/`tasks[].date|time|command|output`）。

### GET /api/schedule/plans
`{"plans":[{"name":"plan.json","example":false,"valid":true,"tasks":2,"first_task":"2026-10-30 23:30:00"}]}`

### GET /api/schedule/plan/{name} / PUT /api/schedule/plan/{name}
- GET → `{"name":"...","content":"..."}`
- PUT 请求体 `{"content":"..."}`：用 run_schedule 自身的 `load_plan`/`parse_trigger` 校验（首任务必须有 date+time）→ 备份 → 写入
- `plan.example.json` 只读（400）

### POST /api/schedule/start
`{"plan":"plan.json"}` → 启动 scheduler 线程：
- 等待触发时刻（状态 `waiting`）；触发时若 benchmark 正在运行 → **等待其结束**（`waiting_slot`）再执行
- 任务执行期间占用执行槽（此时 POST /api/run → 409）
- 任务的 stdout/stderr 以与 run_schedule 相同的起止分隔头追加写入计划指定的 output 文件
- scheduler 自身事件（等待/开始/完成）写入 `webui/data/runs/s<id>.log`，经 status 轮询可见
→ `{"scheduler":{"id":"s...","state":"waiting",...}}`；已有活跃 scheduler → 409

### POST /api/schedule/stop
停止 scheduler。等待期直接停；执行期先写 stop 文件 + 发优雅信号，任务（benchmark）
自行清理，grace 超时后再强制 kill；`state` 先 `stopping`，最终 `stopped`。
→ `{"ok":true,"scheduler":{...}}`

### GET /api/schedule/status
→ `{"scheduler":{"state":"waiting|waiting_slot|executing|stopping|done|stopped|aborted",
                  "plan":"schedule/plan.json","next_trigger":"...","current_task":1,
                  "tasks":2,"events":["[22:30:01] task 1 START: ..."]} | null}`

---

## 典型调用序列（curl）

```bash
BASE=http://127.0.0.1:8080

# 1. 看有哪些可跑的配置
curl -s $BASE/api/configs | python -m json.tool

# 2. 发起运行（单文件）
curl -s -X POST $BASE/api/run -H 'Content-Type: application/json' -d '{
  "selector": {"type":"file","path":"configs/basic/put.json"},
  "flags": {"cluster":"configs/cluster.json"}}'
# → {"run":{"id":"r20261029-223001-001",...},"warnings":[]}

# 3. 轮询日志（offset 增量推进）
curl -s "$BASE/api/runs/r20261029-223001-001/log?offset=0"
curl -s "$BASE/api/runs/r20261029-223001-001/log?offset=15321"

# 4. 状态/结果
curl -s $BASE/api/runs/r20261029-223001-001        # status=done, ts_dir=outputs/...
curl -s $BASE/api/outputs
curl -s "$BASE/api/csv?path=outputs/20261029-2230/benchmark_detail.csv&max_rows=100"

# 5. 停止
curl -s -X POST $BASE/api/runs/r20261029-223001-001/stop

# 6. 编辑配置（校验失败会 422 并说明原因）
curl -s -X PUT $BASE/api/config -H 'Content-Type: application/json' \
  -d '{"path":"configs/basic/put.json","content":"{...}"}'

# 7. 夜间计划
curl -s -X PUT $BASE/api/schedule/plan/night.json -H 'Content-Type: application/json' \
  -d '{"content":"{\"wait_seconds\":60,\"on_fail\":\"continue\",\"tasks\":[{\"date\":\"2026-10-30\",\"time\":\"23:30\",\"command\":\"python benchmark.py -f configs/basic/put.json\",\"output\":\"logs/night/put.log\"}]}"}'
curl -s -X POST $BASE/api/schedule/start -H 'Content-Type: application/json' -d '{"plan":"night.json"}'
curl -s $BASE/api/schedule/status
```

## 行为备注

- **停止残留**：强制停止后远端进程可能残留，`.state` 台账会在下次 run 启动时自动 reap（既有机制）
- **dry-run**：无 `ts_dir`、不产生 outputs
- **并发规则**：单 benchmark 槽 + 单 scheduler；scheduler 等待期手动 run 允许（带 warning），任务执行期互斥
- **备份**：所有 PUT 写入前自动备份到 `webui/backups/`（含原相对路径与时间戳）
