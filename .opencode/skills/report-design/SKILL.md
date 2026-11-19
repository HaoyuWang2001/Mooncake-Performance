---
name: report-design
description: Use when editing, reviewing, or adding any UI in scripts/report.py's generated pages (report.html basic/pipeline templates, overview.html). The requirements doc (docs/spec/report/REQUIREMENTS.md) says WHAT the report must contain; this skill says HOW the UI must look and behave — tokens, layout, colors, interactions, wording, template architecture, and a do/don't checklist distilled from the project's design decisions.
---

# 报表 UI 设计规范（report.html / overview.html）

需求文档（`docs/spec/report/REQUIREMENTS.md`）描述**做什么**；本文件规范**怎么做 UI**（配色、布局、交互、文案、模板结构）。
两者冲突时：功能范围以需求文档为准，视觉与交互形态以本文件为准。

> **本 skill 是活规范**：每次对话中新确认的 UI 约束必须同步补入本文件；
> 后续任何 UI 更改必须严格遵循本文件。违反规范即缺陷。

## 适用范围

- 适用：`scripts/report.py` 生成的产物——`report.html`（basic / pipeline 两套 body 模板）、`overview.html`。
- 不适用：`webui/web/**`（管理台，见 `webui-design` skill）。
- 生成器约束：纯 Python 标准库；产物单文件自包含（CSS/ECharts/主题内联、零网络）；data.js 可选懒加载（缺失可降级）。

## 设计基调

- 主题 = lieflat Wire（`webui/web/vendor/lieflat/`）：纸灰底 / 炭黑底两套，token 见 `_PAGE_CSS` 的 `--paper/--ink/--mut/--faint/--grid/--quiet/--track/--hero`。
- 第一读者是工程师复盘：信息密度优先，不追求演示效果；不使用装饰性图形、渐变、阴影（卡片仅 1px 描边或底色区分）。
- 数字统一 `font-variant-numeric: tabular-nums`；时延按 µs/ms/s 自适应（`fmtUs`）；不要出现 `5m30s` 式缩写。
- 中文全角标点（，。：；「」（）），中英混排保留英文技术词；省略号用 `…`。

## 硬性规则（做 / 不做）

### 1. 配色

- 端到端时延曲线**全灰阶**：`avg / p99 / p9999` 用深灰（原 avg 色），其余指标用稍浅的灰（原 p999 色）；多对象对比调色板亦为灰阶。
- 强调色（品牌橙）**只允许出现在语义注解**：离群标注/阈值线、异常窗口竖线、QPS 失败柱、慢点最慢一根、泄露警示。禁止用于常规曲线或纯装饰。
- 新增颜色必须复用既有 token 或既有的灰阶阶梯，不引入新的 hex 色。

### 2. 段1（性能统计 / 信息展示 / 参数配置）

- 性能统计两行：**仅第一行强调（加粗）**：吞吐 / avg / p99 / p9999；第二行 = p50 / p95 / p999 / min / max，常规字重。
- **队列相关项（队列等待均/峰、最大队列深度）不放在性能统计里**：独占一行，位于信息展示「总数/成功/失败/失败率/QPS」行下方。
- 字号层级：强调值 24px/800；非强调值（性能第二行、信息展示、队列行）18px/500。视觉重心只在第一行。
- 指标名统一小写 avg / p99 / p9999 / min / max（曲线与段1 一致），不套大写。
- 信息展示两行：第一行 = 总数 / 成功 / 失败 / 失败率 / QPS；第二行（参数配置标题下、参数表格上方）= 数据大小 / 客户端数 / 线程数 / 时长。
- 参数配置**不得重复**信息展示已给出的值：渲染层过滤 数据大小 / 客户端数 / 线程数 / 客户端总数 / 采集时长 / 开始时间（pipeline 另过滤 client.count_per_node / store_count_per_node / notify_count）。
- pipeline 单元行（顺序固定）：`数据大小 | 部署模式 xwyr | 客户端数 x×N nodes | store 数量 s×N nodes | 时长`（部署模式固定在左起第二；英文 nodes 说明 x×N；**不写「= 总数」**；部署模式用 `2w4r` 形式）。
- QPS 不属于性能统计卡；未提供「QPS 达成率」刻度卡。

### 3. 工具栏

- 一行制：`flex-wrap:nowrap` + `overflow-x:auto` + 子项 `flex:0 0 auto;margin:0`（嵌套 `.toolbar` 的 margin 必须清掉，否则按钮整体上移、错行）。
- 不使用竖向分割线（`.tb-sep`）分隔同组按钮。
- 阶段曲线工具栏：「粒度档 + 重置视图」一行；「阶段」选择器独立一行。
- 所有按钮与文字在同一行内垂直居中（`align-items:center`）。

### 4. 分桶策略（1s / 10s / 1min / 自适应）

**map 只产出时间片桶**（不再有等样本桶）：尺度集合固定 `{1s, 10s, 1min}`。

- **收集规则**（logstats 与 tracestats 的 breakdown 字段同策略）：尺度 `s` 回传 ⇔ 桶数 `span/s ∈ [59, 8192]`。
  - 下限 60（用 59 判定避免 60 边界清空）：例 10min run → 1s=600 ✓、10s=60 ✓、1min=10 ✗。
  - 上限 8192：更粗尺度足够时舍弃过细尺度：例 3h run → 1s=10800 ✗、10s=1080 ✓、1min=180 ✓。
  - 无任何尺度达标（极短 run）→ 回退最细步长。
- **map 只回传通过规则的最细尺度**，其余可达尺度由 reduce 从该尺度聚合推导（可见行为 = 上述收集结果）。
- **自适应**（生成 report.html 时计算，仅基于时间桶）：
  - 每个可达尺度产生候选：`n ≤ 4096` → 原样；`n > 4096` → 选整数 k 使 `n/k ∈ [2048,4096]` 且**最接近 2048**（6144 → ÷3 = 2048；5000 → ÷2 = 2500）。
  - 所有候选取桶数最接近 2048 的方案；**核心原则：尽量 2048 点**。
  - 常量：`MIN_TIER_BUCKETS=59`、`MAX_TIER_BUCKETS=8192`、`ADAPTIVE_MIN_BUCKETS=2048`、`ADAPTIVE_MAX_BUCKETS=4096`（服务端与前端必须一致）。
- 档位按钮：**自适应（默认）固定最左**，其后为可用尺度；不可用档**隐藏**；自适应按钮 tooltip 标注折叠方案（如「1 秒 × 3 ≈ 2048 点」）。当前档失效回落自适应。
- 阶段曲线的分桶策略必须与端到端曲线一致（trace breakdown 同规则）。

### 4b. 多对象对比（节点/实例）

- 配色：**总计 = 灰阶**（按指标取深/浅灰，同单对象）；**每个节点一个色系**（按节点名排序固定分配，亮暗各一套）；**实例 = 所属节点色系的色阶**（同系不同深浅）。
- **不提供多对象指标切换按钮组**：多对象显示哪些指标只由「时延曲线选择」决定；QPS/失败在下方副图。
- 「队列等待」属于时延曲线选择（一条线，**默认不展示**）。
- 工具栏提供 `全选 / 清空`（当前范围内的节点与实例；pipeline 按当前角色）。

### 4c. 离群值 / 三清单

- **离群值 = 单请求时延 > 该客户端 p99 × 10**（样本 < 1000 不做判定），在 **map 阶段**逐客户端判定。
- 离群样本从 1s 桶的**标量统计**（avg/max/队列等待）中剔除；**计数（c/ok/fail）与直方图保持全量**（QPS 与时延分布口径不变）。
- 主图把离群值画成**独立散点**（可显示/隐藏）；**不做整桶截顶**。
- 三份清单（各客户端上限 200 条）：异常请求（未完成/失败）、慢点（已完成 Top）、离群值。

### 4d. 无数据窗口与缩放

- 无数据窗口：浅色竖带标记，**仅第一处标注时长**，其余保持纯色带；图下加一行图注说明。
- 判定基于该序列**真实缺失的桶**（自适应折叠后的时间网格不会因桶键跳跃误报）。
- **QPS 副图**：x 轴缩放时 y 轴不得重算（`filterMode:'none'`，保留全局量程）。

### 4e. 慢点捕捉（独立控件）

- 自带一套**独立于主图**的控件：角色单选（pipeline，默认 reader）、总计（默认开，**不自动关闭**）、节点与实例（全选/清空 + 勾选）。
- **叠加语义**：总计 = 当前角色全部慢点（灰色）；勾选的节点/实例以节点色系/实例色阶**叠加高亮**。
- 摘要条（不受勾选影响）：每节点慢点次数 + 该节点自身 p99，用于跨节点数量级定位。
- x 轴与端到端曲线**同窗口**，缩放独立；writer/reader 用不同标记形状（方/圆）。

### 5. 尺寸与布局

- 块宽估算：块内是**两行堆叠**（名称 / 值+占比），宽度取两行中最宽者 + 标准 padding（`.lvl-blk` 水平 22px）；**禁止**把两行拼成一串测宽（会虚高约 2 倍，导致整行横向溢出、行尾残差被裁）。
- 0% 块的宽度 = 文字宽 + 标准 padding（尽可能窄，但不能截断文字）。
- 行内容优先用 CSS grid 的 `minmax(可读下限, Nfr)`；行溢出时必须横向滚动而不是截断内容。
- 长内容允许横向滚动容器（`.wrapx` / `.lvl-row`），但关键块（残差等）在常见窗口宽度（≥1000px）下必须完整可见。

### 6. 交互边界

- **曲线叠加只由对象树勾选控制**；其他位置（实例明细表等）不得改变曲线/选中集。
- 实例详情表**只读**：不点击、不展开、不嵌小图。
- 展开交互只允许出现在「最慢请求」：点击行展开该请求的完整分级时延（树序全部 L1 + 残差；所有有二级子区间且值>0 的 L1 同时展开二级，含「未归因」）。
- QPS 副图图例可看不可点（`selectedMode:false`，固定显示）；副图上方无小标题。
- 平移/缩放滑块窗口不加描边；zslider 把手为填充色（不是描边）。
- 端点标识：标签与引线**两端**都钳制在绘图区内；标签聚集时依次分散、绝不重叠（顶部/底部/贴底同时成立）。
- 展开互斥（同时只展开一个对象/一张附带图表，避免多个 ECharts 实例）。

### 7. 文案

- 卡片标题不加括号注记（例：「实例详情」，不写「（点击行…）」）。
- 指标名统一 avg / p50 / p95 / p99 / p999 / p9999 / min / max；不要用「平均」「最大」作为曲线名（段1 的「平均时延」卡片除外）。
- 说明文字极简：直方图仅保留「p9999 需样本 ≥ 10000」；不写教程式长段落。
- 图注必须与实际行为一致（颜色/交互变化时同步更新，例：「橙色为该组最慢」）。
- 表头与 tooltip 说明改动交互时，必须同步修改 `datanote` 等降级提示。

### 8. 模式差异（basic vs pipeline）

- 对象树：basic = 顶部单行「总览」+「节点与实例」；pipeline = **角色单选**（`角色 [reader] [writer]`，互斥必选，默认 reader，独立一行）+ **总计**（独立一行，默认开；切换角色时保持开关状态）+「节点与实例」（按当前角色过滤；**切换角色不丢弃节点/实例选择**——按角色记忆，切回自动恢复）。pipeline **没有「总体（全部）」概念**（写入/读取数据不可合并展示）。
- trace 卡的读/写/其它分组按钮两种模式均保留；trace 卡内**没有**独立数据大小选择器（跟随页面最上方主选择器）。
- basic 与 pipeline 使用**两套独立 body 模板**（见下）；功能差异在模板/JS 分支中表达，不做隐式行为。

## 模板架构规范

- 共享：`_PAGE_CSS`、`_APP_JS`、`_PAGE_ADAPTER_JS`、外壳 `_page_html()`、以及各区块片段常量（`_SEC_*`）。
- 两套 body 模板：`_BODY_BASIC` / `_BODY_PIPELINE`，由片段常量组装；`render_scenario` 按 `meta.mode` 选择；pipeline 页面标题为「Pipeline 场景报表」并带 `<!-- tpl:pipeline -->` 标记（basic 对应 `<!-- tpl:basic -->`）。
- JS 保持**单份共享**，模式差异只用 `D.meta.mode` 分支；禁止复制整份 JS。
- 新增区块的流程：先抽 `_SEC_<NAME>` 片段 → 在两个 body 模板中显式引用 → 再写渲染 JS（用 `safe()` 隔离）→ 补测试。
- 单元化产物：basic 目录 `c{C}_t{T}_{size}`，pipeline 目录 `{size}`；一个 size×参数组一份 report.html + data.js + meta.json。

## 验证流程（改完必过）

1. `env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY python3 -m pytest webui/tests -q`（全绿）
2. `python3 -c "import report; open('/tmp/a.js','w').write(report._APP_JS)"` + `node --check /tmp/a.js`
3. 真实 ECharts SSR：`python3 /tmp/opencode/real_render.py <report.html> | timeout 90 node`（`MOUNT_FAIL/TOTAL` 检查）
4. 真实浏览器截图（Windows Edge headless：`--headless=new --virtual-time-budget=6000 --window-size=1400,5200`），必要项交互可用注入脚本点击后截图；至少检查 1000px 与 1400px 两种宽度
5. 沙箱同步并重建：`cp scripts/report.py <sandbox>/scripts/` → `benchmark.py --rebuild-report <ts>`；`curl --noproxy '*' http://127.0.0.1:8500/api/file?...` 返回 200

## 自查清单（每次改动逐项确认）

- [ ] 曲线无橙色；强调色只出现在语义注解
- [ ] 段1 两行制、参数不重复已展示值
- [ ] 工具栏单行、无竖向分割线、垂直居中
- [ ] 分桶档位按「桶数 ∈ [59,8192] 且 ≥ftier」提供，不可用档隐藏；自适应折叠≈2048 点；无等样本桶；**自适应按钮在最左**
- [ ] 块宽按「最宽一行 + padding」计算；0% 块窄且文字完整
- [ ] 无新增点击交互（除最慢请求展开）；实例表只读；叠线只走对象树
- [ ] 图例固定、滑块无描边、端点标签不越界不重叠
- [ ] 标题无括号注记；指标名 avg/max；说明文字极简且与行为一致
- [ ] basic/pipeline 模板选择正确（标记注释与标题徽记）
- [ ] pytest / node --check / SSR / 截图 / 8500 全部通过
