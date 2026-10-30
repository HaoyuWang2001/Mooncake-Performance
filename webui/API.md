# Mooncake-Performance WebUI — HTTP API

服务启动：`python webui/server.py [--port 8080] [--bind 127.0.0.1]`（`--bind 0.0.0.0` 开放局域网）。
前端为 vendored **Vue3 + Element Plus**（`webui/static/vendor/`，约 1.8MB，无构建步骤、离线可用），
页面与 API 使用同一套后端。
所有 API 返回 JSON（`Content-Type: application/json`）；错误形如 `{"error": "..."}` 并带恰当 HTTP 状态码
（400 参数错 / 403 路径越界 / 404 不存在 / 409 槽位占用 / 422 校验失败）。

约定：
- `path` 参数一律是**相对仓库根**的 POSIX 风格路径（如 `configs/basic/put.json`）
- 文件读取白名单：`outputs/`、`webui/runs/`；配置读写白名单：`configs/`、`schedule/`
- benchmark 与 scheduler 任务共用单一执行槽：benchmark 运行中（或 scheduler 任务执行中）新 run 请求 → 409
- scheduler 等待期间发起 benchmark → 允许，响应携带 `warnings`

---

## 系统

### GET /api/status
```json
{"version":"1.0","platform":"posix","python":"3.12.3","root":"/path/Mooncake-Performance",
 "paths":{"outputs":"outputs","runs":"webui/runs"},
 "active_run":{"id":"r20261029-223001-001","status":"running",...} | null,
 "scheduler":{"id":"s20261029-223005-002","state":"waiting",...} | null}
```

### GET /api/help
返回本文档：`{"markdown":"# Mooncake-Performance WebUI ..."}`

---

## 配置浏览 / 编辑

### GET /api/configs
配置清单：`general` = configs 根下全部 `*.json`（过滤 `*.example.json` 模板，含
cluster/global/matrix 及任意其他通用文件）；`categories` = configs 下**每个非 `_`
开头子目录**的配置树 `{name, path, dirs[], files[]}`（恒含 demo/，`_` 前缀碎片排除）
与 `config_count`；`cluster_candidates` / `global_candidates` / `matrix_candidates`
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
    "dry_run": false, "no_warmup": false, "no_plot": false,
    "protocol": null,                     // null | "tcp"|"rdma"|"ub"
    "cluster": "configs/cluster.json",
    "global": "configs/global.json",
    "profile": "auto"                     // auto=默认行为(matrix.json 存在即启用)；或矩阵路径
  }
}
```
selector 语义：`dirs` + `files` 至少一项非空；**dirs 先执行、files 后执行**（CLI 映射为
`--config-dir a,b` + `-f x,y` 多值形式）。旧 `{type, path}` 格式已移除（400）。
→ `201` 意义上返回 `{"run":{"id":"r...","status":"running","cmd":"..."},"warnings":[]}`
槽位被占 → `409 {"error":"benchmark run r... is still active ..."}`

### GET /api/runs
历史运行列表（最多 60 条，新→旧）：`{"runs":[<run meta>...]}`。
run meta 字段：`id/kind/cmd/argv/log/status/rc/t0/t1/elapsed/ts_dir/meta`；`ts_dir` 如 `outputs/20261029-2230`（dry-run 无）。

### GET /api/runs/{id}
单条 run meta。404 若不存在。

### GET /api/runs/{id}/log?offset=0
增量日志：`{"offset":<新字节偏移>,"size":<总字节>,"content":"<新增文本>","eof":bool}`
单次最多 512KB；`offset` 为字节偏移（首次传 0，之后传上次返回值）。

### POST /api/runs/{id}/stop
跨平台停止（Windows `taskkill /F /T`；POSIX 进程组 TERM→KILL）。
→ `{"ok":true,"run":{...status:"stopped"}}`（已结束的 run 调用也返回 ok）

---

## 结果浏览

### GET /api/outputs
`{"outputs":[{"ts":"20261029-2230","mtime":"...","has_detail":true,"csv_count":3},...]}`（新→旧）

### GET /api/outputs/{ts}/tree
`{"ts":"...","files":["benchmark_detail.csv","logs/put/put.benchmark_c1_t2_64KB.log",...]}`

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
- scheduler 自身事件（等待/开始/完成）写入 `webui/runs/s<id>.log`，经 status 轮询可见
→ `{"scheduler":{"id":"s...","state":"waiting",...}}`；已有活跃 scheduler → 409

### POST /api/schedule/stop
停止 scheduler（等待期直接停；执行期连任务进程一起杀）→ `{"ok":true,"scheduler":{...}}`

### GET /api/schedule/status
→ `{"scheduler":{"state":"waiting|waiting_slot|executing|done|stopped|aborted",
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
