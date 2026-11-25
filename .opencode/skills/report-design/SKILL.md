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

### 0. 同构原则（端到端 ↔ 分级时延）

端到端时延曲线上确立的交互/展示策略，**默认同构映射到分级时延曲线（trace）**并逐项核对：
对象树（含 toggle 按钮/清空自动总计）、固定槽位标签+压暗聚焦、点击任意处开详情框、
粒度档/缩放/平移、色语义（总计灰阶/节点色系/实例色阶）、线宽、图例禁点。
数据形态不支持时（如桶内只有均值/最大）必须在 UI/文档注明，不得静默缺省。


### 1. 配色

- 曲线配色**按对象类型**（单/多对象一致，不按选中数量切换）：
  - **总览（basic）/ 总计（pipeline）= 灰阶**：`avg / p99 / p9999` 深灰，其余指标浅灰；
  - **节点 = 每节点固定色系**（`NODE_HUES_L/D`，按节点名排序分配，跨选择稳定）；
  - **实例 = 所属节点色系的色阶**（`objLineColor`，同系不同深浅）。
- **线宽**：`avg` 1.0px，其余指标 0.8px；hover 强调 = 基准 +0.2px（1.2 / 1.0）。**多对象不再把首对象加粗**——粗细只表达指标重要性。阶段曲线同规则（total 1.0 / 阶段 0.8）。
- 强调色（品牌橙）**只允许出现在语义注解**：离群标注/阈值线、异常窗口竖线、QPS 失败柱、慢点最慢一根、泄露警示。禁止用于常规曲线或纯装饰。
- 新增颜色必须复用既有 token、既有灰阶阶梯或 `NODE_HUES_L/D`，不引入新的 hex 色。

### 2. 段1（性能统计 / 参数配置）

- 性能统计两行：**仅第一行强调（加粗）**：吞吐 / avg / p99 / p9999；第二行 = p50 / p95 / p999 / min / max，常规字重。
- **队列相关项（队列等待均/峰、最大队列深度）不放在性能统计两行里**：独占一行，位于性能统计**第三行**下方。
- 字号层级：强调值 24px/800；非强调值（性能第二行、第三行、队列行）18px/500。视觉重心只在第一行。
- 指标名统一小写 avg / p99 / p9999 / min / max（曲线与段1 一致），不套大写。
- **基础场景取消「信息展示」子区**：总数 / 成功 / 失败 / 失败率 / QPS 直接作为**性能统计第三行**（basic 与 pipeline 的段1 都不再出现「信息展示」标题）。数据大小 / 客户端数 / 线程数 / 时长 仍在参数配置标题下、参数表格上方。
- 参数配置**不得重复**段1 已给出的值：渲染层过滤 数据大小 / 客户端数 / 线程数 / 客户端总数 / 采集时长 / 开始时间（pipeline 另过滤 client.count_per_node / store_count_per_node / notify_count）。
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

- 配色：**总计 = 灰阶**（按指标取深/浅灰，同单对象）；**每个节点一个色系**（按节点名排序固定分配，亮暗各一套）；**实例 = 所属节点色系的色阶**（同系不同深浅）。**QPS 副图柱色与端到端一致**（总计灰阶 / 节点色系 / 实例色阶），失败柱保持强调色。
- **不提供多对象指标切换按钮组**：多对象显示哪些指标只由「时延曲线选择」决定；QPS/失败在下方副图。
- 「队列等待」属于时延曲线选择（一条线，**默认不展示**）。
- 「节点与实例」组按钮按状态**动态切换**：未全选时 `全选节点 / 全选实例`，已全选时对应变为 `清空节点 / 清空实例`；`全部清空` 清空节点与实例并**自动开启总计/总览**。总计/总览不参与这三键。慢点捕捉控件同样适用。
- **多对象右侧标签 = 对象名固定槽位**（≥2 个对象时）：总计/总览最上，其余按树序平铺（节点 → 其下实例），无引线、不绑任何曲线端点；超长名 `max-width` 截断 + `title` 全名；单对象仍是指标名 + 引线。

### 4c. 离群值 / 三清单

- **离群值 = 单请求时延 > 该客户端 p99 × 10**（样本 < 1000 不做判定），在 **map 阶段**逐客户端判定。
- 离群样本从 1s 桶的**标量统计**（avg/max/队列等待）中剔除；**计数（c/ok/fail）与直方图保持全量**（QPS 与时延分布口径不变）。
- 主图把离群值画成**独立散点**（可显示/隐藏）；**不做整桶截顶**。
- 三份清单（各客户端上限 200 条）：异常请求（未完成/失败）、慢点（已完成 Top）、离群值。

### 4d. 无数据窗口与缩放

- 无数据窗口：浅色竖带标记，**仅第一处标注时长**，其余保持纯色带；图下加一行图注说明。
- 判定基于该序列**真实缺失的桶**（自适应折叠后的时间网格不会因桶键跳跃误报）。
- **QPS 副图**：x 轴缩放时 y 轴不得重算（`filterMode:'none'`，保留全局量程）。

### 4f. 交互（tooltip / 端点标签 / 时间）

- **tooltip 一律原生悬浮**：竖线 + 各曲线交点 + 内容框由 ECharts 同时渲染/消失（`triggerOn:'mousemove'`、`showContent:true`、`hideDelay:250`、`enterable:true`，框内滚动 `max-height:46vh`）；**不得设置 `showDelay`**（显示定时器与 globalout 竞态 → 快速扫过后框永久残留并挡住右侧标签，见 DR-44）；`bindHoverGuards` 在 `globalout` 时**主动 hideTip** 兜底。**拒绝点击钉住**（DR-42）。所有图统一由 `lfTooltip` 提供。
- **端点标签聚焦**：多对象标签 hover = 该对象整簇保持原色、其他对象压暗（`opacity:0.15`，用 `setOption` 局部更新，**不用 highlight/focus blur**——多条 highlight 的 blur 会互相覆盖导致只亮一条）；单对象标签 hover = 高亮该指标序列。多对象图例 `selectedMode:false`（只说明颜色，不可点击隐藏）。
- **多对象 tooltip** 显示时间 + 各对象各指标（带颜色圆点与单位），不得落回 ECharts 默认的原始毫秒时间戳。
- **SDK 对象树 = 总计 + 节点与实例**（角色由 section 顶部写/读总控决定，树里不再有角色行）；节点/实例行可用（阶段桶按 node/pod 保留）。
- **页头「测试开始于」= 数据最早采样时刻**（`meta.ts_from` 转本地时间），不是报表生成时刻；生成时刻只在「报告生成」位置。

### 4g. QPS 副图

- 柱色 = `objLineColor(key)`（总计灰阶、节点色系、实例色阶）；失败柱固定强调色（橙），左右轴独立计数。

### 4h. SDK 全链路（trace）

- **顶部总控 = 写路径/读路径**（互斥）+ op 选择；**各控台不再有角色行**。
- **对象树** = 总计（当前读写组聚合）+ 节点与实例；行为与主图同构（全选节点/全选实例
  toggle、全部清空→总计）。数据侧：总计全嵌，节点/实例按 `TRACE_EMBED_POINTS`(192)
  抽稀 + 行预算(60000)，超出留 data.js。
- **分级时延块图（trace）**：对象单选（下拉）+ 指标单选；不再展示指标概览卡。
- **分级时延曲线（trace）**：维度收敛规则 **单对象可阶段多选 / 单阶段可对象多选**；
  指标单选（均值/最大，map 桶限制）；多对象=对象色，多阶段=阶段调色板；
  右侧固定槽位标签（无引线）+ 悬浮压暗；阶段行有 全选/清空。
- **慢点追踪（trace）**：柱按时延降序（轴标签=时延值），自下而上=阶段，
  **每阶段不同颜色**；图例点击=solo（再点同一图例恢复全部）；表格行点开的块图
  **不含 total 行**、单行压缩、横向滚动；Top-N 参数化（当前 100，后续 1000 需同步
  map `--top-n` 与抽稀/虚拟化）。

### 4i. 尾部章节（错误 / 实例与日志现场 / 页脚）

- 章节顺序：SDK 全链路 → 错误与异常 → 实例与日志现场 → 节点监控 → 页脚（最底部）。
- **错误与异常**：写→读平铺，节点→客户端两级表（含状态码前 3），保留首错误摘录；
  异常窗口带「角色 · 节点 · 实例」归属。
- **实例与日志现场**：节点树（节点行可展开；客户端行只读）→ 客户端行 writer 前
  reader 后；列=角色/节点/客户端/总数/成功/失败/日志 + **对慢点追踪的贡献**
  （无 trace 整列隐藏）；不含性能指标列；现场索引（节点汇总/登录命令/glog 产物）并入。
- **页脚**：数据文件（report.html 自包含 + data.js）、map→reduce 流程、正确性口径
  （总计=同桶按请求数加权平均、max 取最大、直方图全量、离群剔除口径）。

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
- 实例与日志现场：**客户端行只读**（不点击、不嵌小图）；仅**节点行**可展开/收起。
- 允许的展开交互：慢点追踪表行（展开该请求的块图）、实例节点行、分级时延块图聚焦（下一级展开）。分级时延块展开：树序全部 L1 + 残差；所有有二级子区间且值>0 的 L1 同时展开二级（含「未归因」）。
- QPS 副图图例可看不可点（`selectedMode:false`，固定显示）；副图上方无小标题。
- 平移/缩放滑块窗口不加描边；zslider 把手为填充色（不是描边）。
- 端点标识：标签与引线**两端**都钳制在绘图区内；标签聚集时依次分散、绝不重叠（顶部/底部/贴底同时成立）。
- 展开互斥（同时只展开一个对象/一张附带图表，避免多个 ECharts 实例）。

### 7. 文案

- 卡片标题不加**交互**括号注记（例：「实例与日志现场」）；仅 SDK 三段命名允许带技术标识：`分级时延块图（trace）`、`分级时延曲线（trace）`、`慢点追踪（trace）`。
- 指标名统一 avg / p50 / p95 / p99 / p999 / p9999 / min / max；不要用「平均」「最大」作为曲线名（段1 的「平均时延」卡片除外）。
- 说明文字极简：直方图仅保留「p9999 需样本 ≥ 10000」；不写教程式长段落。
- 图注必须与实际行为一致（颜色/交互变化时同步更新，例：「橙色为该组最慢」）。
- 表头与 tooltip 说明改动交互时，必须同步修改 `datanote` 等降级提示。

### 8. 模式差异（basic vs pipeline）

- 对象树：basic = 顶部单行「总览」+「节点与实例」；pipeline = **角色单选**（`角色 [reader] [writer]`，互斥必选，默认 reader，独立一行）+ **总计**（独立一行，默认开；切换角色时保持开关状态）+「节点与实例」（按当前角色过滤；**切换角色不丢弃节点/实例选择**——按角色记忆，切回自动恢复）。pipeline **没有「总体（全部）」概念**（写入/读取数据不可合并展示）。
- trace 卡的读/写/其它分组按钮两种模式均保留；trace 卡内**没有**独立数据大小选择器（跟随页面最上方主选择器）。
- basic 与 pipeline 使用**两套独立 body 模板**（见下）；功能差异在模板/JS 分支中表达，不做隐式行为。

## 组件规则册（逐组件契约）

> **与速查层的关系**：速查层是浓缩，本规则册是**实现契约**（精确到行为/数值/根因）；
> 两者冲突时以本规则册为准，并反手修正速查层。
> **条目格式**：`- C<组件>.<类型><序号> <规则> —— <需求/决策> 〔<测试/状态>〕`
> 类型：**V** 展示 · **I** 交互 · **D** 数据口径 · **B** bug 锁（含根因，防回归）。
> 状态：`〔test_<name>〕` 已锁 · `〔◐ 说明〕` 部分/桩局限 · `〔✗〕` 无测试 · `〔n/a〕` 静态/流程。
> **维护闭环**：任何 UI 批次 = 代码 + 本册条目 + 测试 三件套；删改测试必须先改条目；
> 测试改名由 `test_skill_rulebook_integrity` 自动拦截；✗ 即下一批测试待办。
> **组件索引**：C0 · C1 · C2 · C3 · C4 · C5 · C6 · C7 · C8 · C9 · C10 · C11 · C12 · C13 · C14 · C15 · C16 · C17 · C18 · C19 · C20
>
> **覆盖率（自动校验）**：共 230 条 · ✓203 · ◐25 · ✗1 · n/a1

### C0 外壳（页头 / 主题 / 大小选择 / 页脚 / 降级 / 自包含）

- C0.V1 `<title>` 与 `<h1>` 携带场景名+单元身份；生成时间只在副标题 —— R-SHELL-11 〔test_b01_unit_identity_in_title_and_h1〕
- C0.V2 副标题按序：测试开始于 / 报告生成于 / 协议 /（提前停止追加警示） —— R-SHELL-5 〔test_a10_subtitle_sequence〕
- C0.V3 「测试开始于」= 数据最早 `ts_from` 转本地时间，不是生成时刻 —— R-SHELL-5 〔test_report_started_time_uses_data_timestamp〕
- C0.V4 主题三态 auto/亮/暗，localStorage 记忆；图表色板是加载时常量（切换只翻 CSS，已知局限） —— R-SHELL-4 〔test_b05_theme_manual_toggle, test_a16_theme_and_numeric_css〕
- C0.V5 数据大小选择器：单 size 隐藏、多 size 切换整页 —— R-SHELL-6 〔◐ harness 捕获 size_sel_display〕
- C0.V6 每个图表容器 `role="img"` + `aria-label` —— R-SHELL-12 〔test_b02_chart_role_and_aria_labels〕
- C0.V7 页脚最底部：数据文件（report.html 自包含 + data.js）、map→reduce 流程、正确性口径 —— R-SHELL-15 〔test_section_order_and_footer_note〕
- C0.I1 主题按钮三态循环 auto→light→dark，写入 localStorage —— R-SHELL-4 〔test_b05_theme_manual_toggle〕
- C0.I2 data.js 缺失降级：明细表按 p99 前 20 名 + `datanote` 提示；加载后自动隐藏并渲染全量 —— R-SHELL-2 〔test_a08_data_missing_degrades_to_note, test_a09_data_loaded_hides_note, test_data_js_loader_points_to_real_file〕
- C0.I3 WebUI raw 模式适配：相对链接改写为 `/api/file?path=…&mode=raw`（`..` 逐级解析） —— R-SHELL-2 〔test_render_report_has_rel_base_and_adapter, test_adapter_resolves_parent_relative_links〕
- C0.D1 单文件自包含：CSS/ECharts/主题内联，零外部请求；生成器纯标准库 —— R-SHELL-1 〔test_render_self_contained_and_data_js, test_a15_report_stdlib_only〕
- C0.D2 注入 HTML 的用户可控字符串一律转义 —— R-SHELL-9 〔test_html_meta_is_escaped〕
- C0.B1 标题层级不跳级 h1→h2→h3；h3 无交互括号注记 —— DR-24 〔test_b03_heading_hierarchy_no_skips, test_c01_dr24_alignment_lock〕
- C0.B2 字号 <12px 微字幕回潮 —— DR-24 〔test_report_has_no_micro_captions_and_tiny_fonts〕
- C0.B3 弱化文本对比度不足（--faint .32 → .72） —— C-5 〔test_b04_text_contrast_c5〕
- C0.B4 「阀值」错字 / 死 CSS / 卡片无描边回潮 —— DR-24 〔test_c01_dr24_alignment_lock〕

### C1 段1 · 性能统计

- C1.V1 两行制：第一行粗体 吞吐/avg/p99/p9999；第二行 p50/p95/p999/min/max —— R-PERF-1 〔test_summary_three_sections_and_params, test_summary_layout_rows_and_dedup〕
- C1.V2 字号层级：强调值 24px/800、其余 18px/500；视觉重心只在第一行 —— R-PERF-3 〔◐ 仅下限 12px 被自动检查〕
- C1.V3 指标名统一小写 avg/p50/p99/p999/p9999/min/max —— R-PERF-1 〔test_c01_dr24_alignment_lock〕
- C1.V4 pipeline 按角色分块（写路径/读路径各一套），合并卡隐藏 —— R-PERF-5 〔test_pipeline_summary_splits_roles〕
- C1.V5 basic 保持合并卡，不受分角色逻辑影响 —— R-PERF-5 〔test_basic_summary_unchanged_by_role_split〕
- C1.V6 吞吐用 MB/s（bytes/wall）；QPS 不在性能卡 —— R-PERF-2/4 〔test_summary_throughput_uses_bytes〕
- C1.I1 多 combo：性能统计为合并口径并在第三行下方注明；单 combo 仍列 —— R-PERF-4 〔test_summary_reports_multiple_combos, test_summary_single_combo_still_listed〕
- C1.D1 分位数口径与 CSV 一致（直方图归并） —— R-PERF-1 〔test_summary_percentiles_consistent_with_csv〕
- C1.D2 wall_s=0 不产生 Infinity/NaN —— R-PERF-1 〔test_zero_wall_has_no_infinity〕
- C1.B1 combo 回调引用未定义函数（renderMain/renderQps/renderTable）曾致图表空白 —— DR-29 〔test_combo_switch_click_works〕
- C1.B2 表头截断「队列等待均」、曲线名「平均/最大」回潮 —— DR-24 〔test_c01_dr24_alignment_lock〕

### C2 段1 · 第三行与队列行

- C2.V1 **性能统计第三行**（原「信息展示」子区已删除）：总数 / 成功 / 失败 / 失败率 / QPS；basic 与 pipeline 均不再有「信息展示」标题与 `#infocards` 容器 —— R-INFO-1 〔test_summary_three_sections_and_params, test_summary_layout_rows_and_dedup〕
- C2.V2 队列等待均/峰、最大队列深度独占一行，位于第三行下方 —— R-INFO-2 〔test_summary_three_sections_and_params〕
- C2.D1 队列等待均值只按成功行数平均（ok 行） —— R-INFO-2 〔test_qw_average_divides_by_ok_rows〕
- C2.I1 降级（data.js 缺失）不改变本卡数值 —— R-SHELL-2 〔◐〕

### C3 段1 · 参数配置与单元信息行

- C3.V1 参数表过滤已展示值（数据大小/客户端数/线程数/客户端总数/采集时长/开始时间；pipeline 另滤 count_per_node 等） —— R-PARAM-1 〔test_summary_layout_rows_and_dedup〕
- C3.V2 pipeline 单元行顺序：数据大小 | 部署模式 `xwyr` | 客户端数 `x×N nodes` | store 数量 | 时长 —— R-PARAM-2 〔test_pipeline_summary_splits_roles〕
- C3.V3 目标写 QPS 全口径（`25 × 4 节点 = 100`）、读 QPS（`100 × 4 读 = 400`） —— R-PARAM-2 〔test_build_report_params_pipeline_and_basic, test_build_report_params_pipeline_qps_total〕
- C3.V4 basic 单元行保留线程数；不写「= 总数」 —— R-PARAM-2 〔test_basic_summary_unchanged_by_role_split〕
- C3.D1 单元拆分：basic `c{C}_t{T}_{size}`，pipeline `{size}` —— R-OV-1 〔test_generate_reports_unit_split_basic, test_unit_of_basic_and_pipeline〕
- C3.B1 参数配置重复信息展示值（pipeline 幽灵值） —— DR-13 〔test_summary_layout_rows_and_dedup〕

### C4 端到端时延曲线 · 对象树

- C4.V1 scenario=basic = 单行「总览」+ 节点与实例；scenario=pipeline = 角色 radio（默认 reader、互斥）+ 总计（默认开、独立行）+ 节点与实例；无「总体（全部）」 —— R-LAT-1 〔test_basic_tree_keeps_single_overview_selection, test_pipeline_tree_role_radio_total_and_scoped_nodes〕
- C4.V2 单实例单元隐藏「节点与实例」组（避免与总览三重冗余） —— R-LAT-1 〔test_single_instance_tree_hides_duplicate_groups〕
- C4.V3 组头显示「已选 N 项」 —— R-LAT-17 〔◐〕
- C4.V4 **格式只由 scenario 决定，engine 只做徽记**：kvtest-pipeline 与 python-pipeline 同构（同一节点每角色仅一行，不再 writer/reader 并排重复）；kvtest-basic 单「总览」；旧 `meta.mode="kvtest"` 归一化为 pipeline；副标题「引擎 python|kvtest」 —— R-LAT-1/R-SHELL-5/DR-47 〔test_kvtest_pipeline_role_tree_no_duplicate_node_rows, test_kvtest_basic_is_single_overview, test_legacy_kvtest_mode_keeps_pipeline_shape, test_subtitle_shows_engine〕
- C4.V5 pipeline 仅剩单一角色（早停）时角色视图自动切到现存角色，禁止空树/空图 —— R-LAT-1 〔test_pipeline_single_role_falls_back〕
- C4.I1 切换角色互斥；按角色记忆节点/实例选择，切回自动恢复 —— R-LAT-1 〔test_pipeline_tree_role_radio_total_and_scoped_nodes〕
- C4.I2 全选节点/全选实例为 toggle：未全选显示「全选*」，全选后变「清空*」 —— R-LAT-17 〔test_tree_toggle_buttons〕
- C4.I3 全部清空：清节点与实例并**自动开启总计/总览** —— R-LAT-17 〔test_clear_all_auto_enables_total, test_tree_group_select_buttons〕
- C4.I4 参数组筛选隐藏不在该 combo 的实例；被隐藏实例处于选中态时选中集回落「总体」 —— R-INST-2 〔test_combo_hidden_client_falls_back〕
- C4.I5 对象条横向滚动位置从顶部开始（切换 size/renderAll 后不残留旧滚动） —— R-LAT-1 〔test_objtree_strip_scrolls_from_top〕
- C4.B1 树行点击失效（onclick 绑错元素/重渲染丢事件） —— DR-27 〔test_tree_group_select_buttons〕
- C4.B2 角色行残留/节点重复行（角色化 key 未去重） —— DR-29 〔test_pipeline_tree_role_radio_total_and_scoped_nodes〕

### C5 端到端时延曲线 · 主图

- C5.V1 指标清单锁定 `LAT_LINES`（avg/p50/p99/p999/p9999/min/max/qw），无超纲 —— R-LAT-3 〔test_a06_metric_list_locked_to_spec〕
- C5.V2 色语义：总览/总计灰阶、节点色系（按名排序固定）、实例=节点色阶 —— R-LAT-12 〔test_a03_latency_color_semantics_gray_only, test_multi_object_node_hues_and_metric_buttons_removed〕
- C5.V3 线宽恒定：avg 1.0 / 其余 0.8；**无 hover 加粗、无「首对象加粗」**（鼠标扫过曲线不产生聚焦/变粗；强调只由右侧标签触发） —— R-LAT-15 〔test_line_widths_avg_thicker_only〕
- C5.V4 单对象无图例；多对象图例 `selectedMode:false`（只说明颜色） —— R-LAT-1/19 〔test_multi_object_node_hues_and_metric_buttons_removed, test_multilabel_tree_order_and_truncation〕
- C5.V5 异常窗口竖线：强调色 **15% 透明度**、宽度 1、≤50 条（低权重，不抢离群点视觉）；图注明确「红色竖线 = 异常窗口（错误突发/时延突增），非离群点」 —— R-LAT-10 〔test_a07_anomalies_capped_at_50, test_anomaly_lines_low_weight_and_outlier_flagpole〕
- C5.V6 无数据窗口浅带，**仅第一处**标注时长；判定用真实缺失桶 —— R-LAT-9 〔test_a01_gap_markarea_between_runs, test_a02_no_gap_no_markarea, test_qps_zoom_keeps_y_and_gap_labels_once〕
- C5.I1 tooltip 原生悬浮：鼠标在绘图区任意位置（含空白）即按竖线时间点显示竖线+交点+内容框；移出/离开即收起 —— R-SHELL-14 〔test_all_charts_native_hover_tooltip〕
- C5.I2 内容框 `enterable:true`（移入框保持并滚动）；**不设 showDelay**；移出画布（globalout）主动 hideTip 兜底；不做点击钉住 —— R-SHELL-14 〔test_all_charts_native_hover_tooltip, test_anomaly_lines_low_weight_and_outlier_flagpole〕
- C5.I3 多对象标签 = 对象名**锚定该对象 p99 曲线在当前窗口的末端**（endLabelLayout 防重叠：19px 间距/顶底钳制，相对上下顺序与曲线一致）；无引线、不绑单点（无可见点退化整体末点）；超长截断，**悬浮时用详情框展示全名**（`data-full` + `.mc-label-tip`，不用原生 title）；单序列不出标签 —— R-LAT-18 〔test_endlabel_fixed_slots_and_cluster_focus, test_label_anchor_follows_curve, test_label_focus_hover_restores〕
- C5.I4 标签聚焦 = **纯悬浮预览**：悬浮压暗其他对象（opacity 0.15）；**离开标签/移入画布/移出图表（globalout）都必须恢复**；不做点击锁定。清除机制三重兜底：标签 mouseleave + 画布 mousemove + **document mousemove（指针不在标签上即清）+ globalout**——修复长截断标签向右移出后事件不再命中而聚焦残留 —— R-LAT-16/19 〔test_endlabel_fixed_slots_and_cluster_focus, test_label_focus_hover_restores, test_trace_curve_fixed_labels_and_focus〕
- C5.I5 单对象标签 = 指标名+引线；标签与引线两端钳制在绘图区、聚集不重叠；**hover 用 mcFocus 压暗其他指标**（与多对象/分级同一机制，不依赖 emphasis） —— R-LAT-7/14/16 〔test_end_label_layout_stays_inside_plot, test_endlabel_focus_single_object〕
- C5.I6 离群点 = 散点 + **从底部到该点的旗杆虚线**（上端止于离群点，便于定位其位置）；开关固定标签「离群点」、**默认选中（显示散点）**、点击变灰即不显示；位于「时延曲线选择」行（重置视图之后），无可命中离群时仍置灰（title 说明判定条件，不隐藏） —— R-LAT-4 〔test_a18_outlier_button_disabled_without_outliers, test_a19_outlier_button_enabled_with_outliers, test_outlier_scatter_toggle_and_merge, test_anomaly_lines_low_weight_and_outlier_flagpole, test_reset_and_outlier_buttons_live_in_latency_selection_row〕
- C5.D1 离群 = 单请求 > 客户端 p99×10（样本≥1000）；从桶标量剔除、计数/直方图全量 —— R-LAT-4 〔test_outlier_scatter_toggle_and_merge〕
- C5.D2 总计 = 同桶按请求数加权平均，max 取最大 —— R-SHELL-15 〔◐ 页脚文字被 test_section_order_and_footer_note 覆盖〕
- C5.D3 时间口径统一 `ts_epoch`（节点 UTC vs 宿主 CST 的时区归一） —— R-SLOW-2 〔test_slowest_ts_epoch_matches_buckets, test_c02_slow_tooltip_epoch_not_undefined〕
- C5.B1 逐条 highlight 与 `emphasis.focus` blur 互撞 → 只剩一条；聚焦一律 setOption 压暗 —— DR-28 〔test_endlabel_fixed_slots_and_cluster_focus〕
- C5.B2 标签绑 p99 端点 → p99 隐藏/无可见点时标签消失或堆顶；改固定槽位 —— DR-27 〔test_endlabel_fixed_slots_and_cluster_focus〕
- C5.B3 曾为「点击任意位置开框」引入自定义状态机（triggerOn 切换/showTip/保活）→ 交点消失、虚线残留、左右关闭不对称；已回归原生悬浮 —— DR-42 〔test_all_charts_native_hover_tooltip〕
- C5.B4 markLine `xAxis=undefined` 曾杀整页 —— DR-18 〔test_real_echarts_render_smoke〕
- C5.B5 单区块异常外溢（opt/series 空）→ `safe()` 隔离 —— R-SHELL-8 〔test_a14_safe_isolation_single_block〕
- C5.B6 曲线曾配 `emphasis.focus:'series'`：鼠标扫过曲线即 blur 其他线 → 闪烁；已删除全部线序列 emphasis（强调只由标签触发） —— DR-42 〔test_line_widths_avg_thicker_only〕
- C5.B7 标签曾按树序均分槽位，与曲线位置错位（query 在下、标签居中）；且聚焦靠标签 mouseleave 恢复，分级图 `finished` 每次 setOption 重渲标签导致元素销毁、mouseleave 丢失 → 聚焦卡死无法取消。改为锚定排布 + 画布 mousemove 兜底恢复（放弃点击锁定） —— DR-33 〔test_label_anchor_follows_curve, test_label_focus_hover_restores〕

### C6 端到端时延曲线 · 工具行

- C6.V1 工具栏单行 `nowrap`+横向滚动、无竖向分割线、子项垂直居中 —— R-LAT-13 〔◐〕
- C6.V2 粒度档：自适应固定最左、不可用档隐藏、tooltip 标注折叠方案；当前档失效回落自适应 —— R-LAT-2 〔test_tier_buttons_and_ladder, test_tier_policy_by_span〕
- C6.I1 时延曲线选择行从左到右 = 重置视图 / 离群点 /「时延曲线选择」标签 / **全选 清空（矩形按钮，与指标 pill 区分，固定在指标之前）** / 指标 toggle（清空保底 avg，防空图） —— R-LAT-3 〔test_latency_metric_select_all_clear, test_a06_metric_list_locked_to_spec, test_reset_and_outlier_buttons_live_in_latency_selection_row〕
- C6.I2 「队列等待」默认不展示 —— R-LAT-3 〔test_a06_metric_list_locked_to_spec〕
- C6.I3 重置视图 = 时间轴回 0..100、y 倍率回 1x（不改档位/选择；档位由「自适应」按钮恢复）；按钮位于「时延曲线选择」行首（与离群点同排，不再留在粒度档工具栏） —— R-LAT-2 〔test_reset_view_restores_zoom, test_reset_and_outlier_buttons_live_in_latency_selection_row〕
- C6.D1 分桶常量前后端一致：59 / 8192 / 2048 / 4096 —— R-LAT-2 〔test_tier_policy_by_span, test_adaptive_tier_folds_time_buckets〕
- C6.B1 等样本桶已删除（只有时间片桶 + 自适应折叠） —— DR-? 〔test_tier_policy_by_span〕
- C6.B2 嵌套 `.toolbar` margin 未清 → 按钮整体上移/错行 —— R-LAT-13 〔◐〕
- C6.B3 「阀值」错字回潮 —— DR-24 〔test_c01_dr24_alignment_lock〕

### C7 QPS 副图

- C7.V1 双轴：QPS 柱（左）+ 失败柱（右，独立计数）；**多对象时 QPS 用细柱（按槽位像素宽自适应 1–3px）、同桶按槽位并排**（第 i 个对象 x 偏移 `(i-(n-1)/2)·桶距×0.8/n`，不重叠；全览桶密时必然贴拢，放大后分开）；失败柱**与所属对象同槽位对齐**（位置即归属）、统一强调橙、同宽；单对象维持 7px/3px 且不偏移 —— R-LAT-8 〔test_a05_qps_fail_series_hero_only, test_qps_grouped_thin_bars, test_qps_single_object_bars_unchanged〕
- C7.V2 柱色随对象：总计灰阶 / 节点色系 / 实例色阶 —— R-LAT-8 〔test_qps_bars_follow_object_colors〕
- C7.V3 图例固定不可点，**只列各对象的 QPS**（失败统一橙色，不逐个列）；无失败显示「本次测试无失败」；副图上方无小标题 —— R-LAT-8 〔test_qps_legend_fixed_and_caption_removed, test_qps_grouped_thin_bars〕
- C7.V4 同一时间点只用**一根竖直虚线**（`axisPointer` line/dashed）；柱不逐根强调（`emphasis.disabled`）；**原生悬浮**详情框用 formatter 兜底（按轴值找最近桶，忽略 ECS 序列匹配）一次性列出该桶**全部对象**的 QPS+失败（`对象：QPS x · 失败 y`，带对象色圆点） —— R-LAT-8 〔test_qps_single_pointer_and_all_object_tooltip〕
- C7.I1 x 缩放时 y 不重算（`filterMode:'none'`） —— R-LAT-8 〔test_qps_zoom_keeps_y_and_gap_labels_once〕
- C7.I2 主图与 QPS 缩放联动（zoomState 同步） —— R-LAT-6 〔test_a13_zoom_links_main_and_qps〕
- C7.D1 QPS 分母 = 实测 wall_s（writer elapsed / reader notify 跨度），不是数据 span —— DR-25 〔test_measured_wall_prefers_notify_span〕
- C7.B1 失败柱误用曲线色 / 图例点击隐藏柱 —— R-LAT-8 〔test_a05_qps_fail_series_hero_only, test_qps_legend_fixed_and_caption_removed〕
- C7.B2 x 轴放大时柱**画出绘图区**（越界）且多对象柱完全重叠：value 轴上 `barGap` 不生效（fail 系列 `-100%` 无意义）→ 改为显式槽位偏移 + 细柱 + 全部 `clip:true`；不再依赖带状布局 —— DR-36 〔test_qps_grouped_thin_bars, test_qps_single_object_bars_unchanged〕

### C8 缩放滑块（y 轨 / x pan）

- C8.V1 y 缩放滑轨 1x..10x + 窗口指示轨可拖；滑块无描边（填充色） —— R-LAT-5 〔test_sliders_have_no_border〕
- C8.V2 缩放/平移后端点标签随窗口重算，不跑出绘图区 —— R-LAT-14 〔test_end_label_layout_stays_inside_plot〕
- C8.I1 拖 y 轨 = 缩放（锚点下缘）；拖窗口 = 平移时延轴 —— R-LAT-5 〔test_y_zoom_slider_factor_valid〕
- C8.I2 时间轴：滚轮缩放、左右拖平移；上下拖 = 时延轴平移（互斥，一次只作用一轴） —— R-LAT-6 〔test_a13_zoom_links_main_and_qps〕
- C8.B1 zslider 把手由描边改填充后回潮（黑边） —— DR-13 〔test_sliders_have_no_border〕
- C8.B2 平移窗口越界（start/end 未夹紧） —— R-LAT-6 〔◐〕

### C9 时延分布（直方图）

- C9.V1 对数分箱直方图（跨客户端合并），x 轴为分箱区间标签 —— R-HIST-1 〔test_hist_chart_units_and_marklines〕
- C9.V2 标注 avg/p50/p99/p999/p9999（p9999 仅样本≥10000）；全部为 p50 灰、细虚线；标签按像素错层 —— R-HIST-2 〔test_hist_chart_units_and_marklines, test_hist_labels_dynamic_top_margin, test_hist_labels_no_extra_margin_when_separate〕
- C9.V3 卡内说明仅保留「p9999 需样本 ≥ 10000」 —— R-HIST-2 〔◐ 由 test_c01_dr24_alignment_lock 类审计覆盖〕
- C9.V4 无直方图数据整卡隐藏 —— R-HIST-3 〔test_a11_empty_hist_and_slow_cards_hidden, test_a12_hist_and_slow_cards_visible_with_data〕
- C9.D1 直方图为全量样本（离群值计入），与 QPS/计数口径一致 —— R-LAT-4 〔◐ 由离群合并测试间接覆盖〕
- C9.B1 标注挤压/越界（顶部 margin 需随是否错层动态计算） —— R-HIST-2 〔test_hist_labels_dynamic_top_margin, test_hist_labels_no_extra_margin_when_separate〕

### C10 慢点捕捉（独立控件）

- C10.V1 独立控件：角色单选（pipeline 默认 reader）/ 总计（默认开）/ 节点与实例 —— R-SLOW-1 〔test_slow_capture_independent_controls〕
- C10.V2 叠加语义：总计=该角色全部慢点（灰背景点），勾选节点/实例以对象色高亮 —— R-SLOW-1 〔test_slow_capture_independent_controls〕
- C10.V3 摘要条（不受勾选影响）：每节点慢点次数 + 该节点自身 p99 —— R-SLOW-1 〔test_slow_capture_independent_controls〕
- C10.V4 x 轴与端到端曲线同窗口、缩放独立；writer=方点 / reader=圆点 —— R-SLOW-1 〔test_slow_capture_independent_controls〕
- C10.V5 该组最慢一根用强调色（图注说明） —— R-SLOW-1 〔test_a04_slow_strip_hero_only_on_max〕
- C10.V6 basic 无角色 radio（单组） —— R-SLOW-1 〔test_slow_capture_basic_has_no_role_radio〕
- C10.V7 无慢点数据整卡隐藏 —— R-SLOW-3 〔test_a11_empty_hist_and_slow_cards_hidden〕
- C10.I1 全选节点/全选实例 toggle（全选后变清空*）；全部清空**自动显示总计** —— R-LAT-17 〔test_slow_capture_independent_controls〕
- C10.D1 慢点时刻用归一化 `ts_epoch`，与主图同口径（不受查看者时区影响） —— R-SLOW-2 〔test_c02_slow_tooltip_epoch_not_undefined, test_slowest_ts_epoch_matches_buckets〕
- C10.D2 慢点清单两级截断：map 每客户端 Top-N → reduce 合并后整体 Top-N —— R-SLOW-1 〔◐〕
- C10.B1 tooltip 读行上不存在的 `r.t` → 显示 undefined —— DR-26 〔test_c02_slow_tooltip_epoch_not_undefined〕
- C10.B2 隐藏已选客户端后高亮残留（选中集未回落） —— R-INST-2 〔◐〕

### C11 SDK 全链路 · 顶部总控与元信息

- C11.V1 标题「SDK 全链路（C++ trace）」；元信息行 = trace 文件数/解析行/slice 行/解析失败/采样率/阶段剔除统计 —— R-TRACE-1 〔test_render_trace_section〕
- C11.V2 写路径/读路径/其它分组按钮（basic 与 pipeline 均保留）；op 选择器仅在可选项 >1 时显示 —— R-TRACE-2 〔test_trace_toolbar_stage_rows〕
- C11.V3 卡内不再有独立「数据大小」选择器（跟随页面主选择器） —— R-TRACE-2 〔test_render_trace_section〕
- C11.V4 无 trace 产物整卡隐藏 —— R-TRACE-8 〔test_render_without_trace_keeps_card_hidden〕
- C11.V5 旧产物无阶段标记 → 注明「无阶段标记（旧客户端），样本未分阶段」 —— R-TRACE-1 〔test_legacy_trace_no_marker_note〕
- C11.I1 写/读互斥，控制该节全部子图（分级块图/曲线/慢点追踪） —— R-TRACE-13 〔test_slow_tracking_ranked_colors_and_legend_solo, test_trace_curve_dimension_coupling〕
- C11.V6 顶部总控只有 读写/op；**对象树与阶段树位于分级时延曲线控台**（不在顶部独立条） —— R-TRACE-12 〔test_trace_toolbar_stage_rows〕
- C11.I2 切换读写组时重置 op 选择与阶段/对象选择（对象按组作用域过滤） —— R-TRACE-6 〔◐〕
- C11.B1 写/读分组曾只作用于部分子图（慢点追踪未跟随） —— DR-29 〔test_slow_tracking_ranked_colors_and_legend_solo〕

### C12 SDK 全链路 · 对象树

- C12.V1 树 = 总计（当前读写组全实例聚合）+ 节点与实例；**无角色行**（角色在顶部）；位于分级时延曲线控台**左栏** —— R-TRACE-6 〔test_stage_tree_objects_enabled, test_trace_toolbar_stage_rows〕
- C12.V2 组按钮：全选节点/全选实例 toggle；**全部清空 = 真清空（允许空选，不回填总计）**（与主图不同：主图清空自动回总计，trace 允许空态） —— R-TRACE-6 〔test_trace_object_tree_toggle, test_trace_curve_dimension_coupling〕
- C12.I1 节点/实例可选中并驱动分级时延块图与曲线 —— R-TRACE-6/11 〔test_stage_tree_objects_enabled, test_trace_per_object_series〕
- C12.I2 节点行可展开其下实例；单节点也显示该组（trace 不做「单实例隐藏」优化） —— R-TRACE-6 〔test_stage_tree_objects_enabled〕
- C12.D1 reduce 按 `meta.node/pod` 保留阶段桶；总计全嵌、节点/实例 192 点抽稀 + 6 万行预算；data.js 全量；**抽稀后 `ftier` 每 op 只乘一次**（k，不是 k^字段数） —— R-TRACE-6 〔test_trace_embed_trim_budget_and_coarsen, test_trace_embed_ftier_multiplied_once, test_render_embeds_trimmed_trace_full_in_datajs〕
- C12.B3 嵌入抽稀曾把 `ftier` 更新写在逐字段循环内 → ftier = k^字段数（600s run、5 字段 → 4^5=1024s）→ 档位强制 ≥1024 → 只剩 1 桶 → 节点/实例曲线空白（总计不抽稀所以正常） —— DR-35 〔test_trace_embed_ftier_multiplied_once, test_trace_curve_dimension_coupling〕
- C12.D2 每对象计数 = 该对象各文件请求数之和；总计 = 全部之和 —— R-TRACE-6 〔test_trace_per_object_series〕
- C12.B1 节点/实例曾被禁用（R-TRACE-9 欠账）→ 数据补齐后开放 —— DR-29 〔test_stage_tree_objects_enabled〕
- C12.B2 角色化对象模型导致同名节点重复行 —— DR-29 〔test_stage_tree_objects_enabled〕

### C13 SDK 全链路 · 分级时延块图（trace）

- C13.V1 标题「分级时延块图（trace）」；**已删除指标概览卡**（tcards） —— R-TRACE-11 〔test_trace_cards_stack_and_breakdown_js〕
- C13.V2 对象**单选**= **树状弹层**（总计 / 节点（下挂实例，缩进）/ 实例；点选即定并收起；点外部/Esc 收起）+ 指标**单选**（min/avg/p50/p99/p999/p9999/max，按字段可用） —— R-TRACE-11 〔test_block_obj_tree_dropdown, test_trace_curve_dimension_coupling〕
- C13.V3 块宽 = 阶段值占父项百分比；0% 块宽 = 该块文字宽 + 标准 padding；残差单独成块 —— R-TRACE-4 〔test_levels_focus_expand_and_residual, test_trace_cards_stack_and_breakdown_js〕
- C13.V4 块 tooltip 附全局均值与倍率（本次值 vs 全局 avg） —— R-TRACE-4 〔◐〕
- C13.V5 p9999 仅当样本 ≥ 10000 可选 —— R-TRACE-3 〔test_trace_field_has_p9999〕
- C13.I1 点击块聚焦、下一级在新行展开；展开全部/收起全部/取消聚焦 —— R-TRACE-4 〔test_levels_focus_expand_and_residual〕
- C13.I2 切换对象/指标后块图即时重算（同一 focus 状态不串） —— R-TRACE-11 〔◐〕
- C13.B1 块宽把两行文字拼成一串测宽 → 整行溢出、行尾残差被裁 —— DR-24 〔test_trace_cards_stack_and_breakdown_js〕
- C13.B2 0% 块文字被截断 —— R-TRACE-4 〔test_trace_cards_stack_and_breakdown_js〕

### C14 SDK 全链路 · 分级时延曲线（trace）

- C14.V1 标题「分级时延曲线（trace）」；控件与**端到端同构**：一行 plain `.toolbar`（粒度档+指标+重置）+ **两个标准 `.objtree-strip` 并排**（对象树 | 阶段树，`.trace-cols` 仅做 grid 布局、无底色/描边）；禁止自定义控台盒 —— R-TRACE-5/12 〔test_trace_toolbar_stage_rows, test_control_style_consistency, test_render_trace_section〕
- C14.V2 阶段按树序列出（total 根行 → L1 → L2 子阶段），L1 带稳定颜色点；**命名 `total`（不用 total（端到端））**，与 avg/p99 技术词风格一致，阶段树/序列/标签/块图根行统一 —— R-TRACE-12 〔test_stage_tree_l2_disabled, test_trace_stage_row_all_clear〕
- C14.V8 阶段树 **L2 子阶段显示但禁用**（map 只回传 L1+残差时间桶），行尾注明「无时间桶」，title 说明仅用于块图展开 —— R-TRACE-12 〔test_stage_tree_l2_disabled〕
- C14.V3 指标单选 = 均值 / 最大（map 阶段桶仅存 avg/max） —— R-TRACE-12 〔test_trace_curve_dimension_coupling〕
- C14.V4 线宽：total 1.0 / 阶段 0.8；hover 强调 +0.2 —— R-TRACE-12/15 〔test_line_widths_avg_thicker_only〕
- C14.V5 多对象=对象色（总计灰阶/节点色系/实例色阶）；多阶段=阶段调色板 —— R-TRACE-12 〔test_trace_curve_dimension_coupling〕
- C14.V6 分桶策略与端到端一致（span/s∈[59,8192] 且 ≥ftier；自适应默认） —— R-TRACE-5 〔test_stage_tier_policy_by_span〕
- C14.V7 空态文案：**未选择对象（总计 / 节点 / 实例）** / **未选择阶段** / 旧产物 / 对象未内嵌，各有明确提示；**对象为空时不得回退画总计**（即使阶段仍有勾选） —— R-TRACE-12 〔test_trace_curve_dimension_coupling〕
- C14.V9 名词统一：总计/节点/实例统称「**对象**」（树头、空态、按钮 title、文档一致）；树头只显示「已选 N 项」（自由组合下无单选/多选模式） —— R-TRACE-12 〔test_trace_curve_dimension_coupling〕
- C14.I1 **对象与阶段自由组合（互不收敛）**：两者都是独立 checkbox，任意 N×M 组合；**允许空选**（清空后空态，不回填）；每组首次进入默认 `总计 + total` —— R-TRACE-12 〔test_trace_curve_dimension_coupling〕
- C14.V10 组合呈现：**单对象**=阶段调色板（可叠加任意多阶段）；**多对象**=颜色按对象（总计灰/节点色系/实例色阶），**阶段>1 时线型区分阶段**（实/虚/点循环）；多对象多阶段的序列名 = `对象 · 阶段` —— R-TRACE-12 〔test_trace_curve_dimension_coupling〕
- C14.I2 阶段**树**头 全选/清空：全选=全部可选阶段（total/L1/残差，不影响对象选择）；**清空=允许空**（图表空态，不回填 total） —— R-TRACE-12 〔test_trace_stage_row_all_clear, test_trace_curve_dimension_coupling〕
- C14.I3 右侧标签与端到端同构：**锚定各序列曲线末端**（endLabelLayout 防重叠，无引线）；单序列不出标签；聚焦=悬浮预览（移入画布/离开标签即恢复） —— R-TRACE-12 〔test_trace_curve_fixed_labels_and_focus, test_label_anchor_follows_curve, test_label_focus_hover_restores〕
- C14.I4 原生悬浮详情框（同主图）：格式化时间 + 各序列值；不做点击钉住 —— R-SHELL-14 〔test_all_charts_native_hover_tooltip〕
- C14.I5 粒度档/重置/缩放/平移与端到端同构（含 y 滑块） —— R-TRACE-5 〔test_stage_tier_policy_by_span〕
- C14.B1 tooltip 落回原始毫秒时间戳（无 formatter） —— DR-29 〔test_all_charts_native_hover_tooltip〕
- C14.B2 三重维度无收敛 → 曲线灾难（阶段×对象×指标全开） —— DR-29 〔test_trace_curve_dimension_coupling〕
- C14.B3 阶段曲线对象树在角色行/节点禁用状态间的历史漂移 —— DR-27/29 〔test_stage_tree_objects_enabled〕

### C15 SDK 全链路 · 慢点追踪（trace）

- C15.V1 标题「慢点追踪（trace）」；柱按端到端时延**降序**排列，轴标签=时延值（可读出聚集簇） —— R-TRACE-13 〔test_slow_tracking_ranked_colors_and_legend_solo〕
- C15.V2 柱高=端到端时延，自下而上=一级阶段（含残差）；**每阶段不同颜色** —— R-TRACE-13 〔test_slow_tracking_ranked_colors_and_legend_solo, test_trace_cards_stack_and_breakdown_js〕
- C15.V3 表格 Top-N（当前 100，参数化）；**表格跟随当前写/读组，与图取同一组 Top-K 集合（行数与柱数 1:1）**；行点击展开该请求阶段构成的块图：**无 total 行**、单行压缩、横向滚动 —— R-TRACE-13 〔test_slow_tracking_ranked_colors_and_legend_solo, test_slow_tracking_table_and_chart_follow_group〕
- C15.V4 tooltip：时刻 + op + 端到端 + 节点/实例 + 各阶段分项 —— R-TRACE-7/13 〔◐〕
- C15.V5 说明文字：采样门控 / memwrite 副本口径 / -1 哨兵 / slice 仅计数 —— R-TRACE-7 〔◐〕
- C15.I1 图例点击 = **solo**：点某阶段只保留它；再点同一图例恢复全部；点其它图例切换 solo 对象 —— R-TRACE-13 〔test_slow_tracking_ranked_colors_and_legend_solo〕
- C15.D1 **每组各自 Top-N**：reduce 先按写/读组过滤慢请求，再组内按 total_us 降序截断 K=100（跨节点跨 op 合并；图/表同集；根级全局榜不保留） —— R-TRACE-13 〔test_trace_top_is_per_group_topk, test_slow_tracking_ranked_colors_and_legend_solo〕
- C15.B1 图例 solo 依赖事件回传状态会误判（点击已 solo 项被当成「全隐」）；用自身 solo 状态机 + 防重入锁 —— DR-29 〔test_slow_tracking_ranked_colors_and_legend_solo〕
- C15.B2 灰阶阶段色导致相邻阶段无法区分 —— DR-29 〔test_trace_cards_stack_and_breakdown_js〕

### C16 错误与异常

- C16.V1 按**写路径 → 读路径**平铺（basic 单块）；每块为「节点 → 客户端」两级表：节点汇总行（实例数/总数/失败/状态码前 3）+ 客户端行（总数/失败/状态码明细） —— R-ERR-1 〔test_errors_split_by_role_and_client〕
- C16.V2 保留首错误原文摘录（≤16KB）与全局「状态分布」；无错误显示「无错误」 —— R-ERR-1 〔◐〕
- C16.V3 异常窗口列表（≤100）：时刻 + 类型（错误突发/时延突增）+ 数值 + **归属（角色 · 节点 · 实例）** —— R-ERR-2 〔◐〕
- C16.I1 状态码按次数降序，取前 3 个显示（`503×3 / 500×1`） —— R-ERR-1 〔test_errors_split_by_role_and_client〕
- C16.D1 reduce 保留 per-client errors 直方图（不再只合并为全局） —— R-ERR-1 〔test_errors_split_by_role_and_client〕
- C16.D2 异常窗口按 (时刻, 类型, 节点) 去重并携带归属 —— R-ERR-2 〔◐〕
- C16.B1 全局错误地图丢失节点/客户端精度 —— DR-29 〔test_errors_split_by_role_and_client〕
- C16.B2 异常窗口时刻的时区错位（节点 UTC vs 宿主 CST） —— DR-18 〔test_slowest_ts_epoch_matches_buckets〕

### C17 实例与日志现场（合并卡）

- C17.V1 节点树：节点行可展开/收起 → 客户端行；同一节点内 **writer 在前、reader 在后** —— R-INST-1 〔test_client_site_tree_readonly_rows, test_client_site_contribution_and_writer_first〕
- C17.V7 **节点身份 = 节点 IP**（k8s=宿主机 `status.hostIP`，ssh=管理 IP；同主机多 pod 节点级合并；pod 名保留在 `meta.pod`）—— 节点标签直接取数据，不做前端映射 —— R-INST-4 〔test_node_display_k8s_uses_host_ip, test_node_display_ssh_uses_management_ip, test_reduce_monitors_labels_by_node_ip_and_dedupes_host〕
- C17.V2 列 = 角色 / 节点 / 客户端 / 总数 / 成功 / 失败 / 日志 + **对慢点追踪的贡献**（Top-N 条数） —— R-INST-1 〔test_client_site_header_and_columns, test_client_site_contribution_and_writer_first〕
- C17.V3 无 C++ trace 时「贡献」整列隐藏 —— R-INST-1 〔test_client_site_contribution_and_writer_first〕
- C17.V4 日志列 = 大小 · 文件名（CSS 截断），title = 远程全路径 —— R-INST-1 〔✗〕
- C17.V5 不再展示性能指标（avg/p50/p99/队列等待） —— R-INST-1 〔test_client_site_header_and_columns〕
- C17.V6 「节点与登录现场」小节：节点汇总 + 登录命令 + 非客户端产物（glog/monitor）索引 —— R-SRC-1 〔◐〕
- C17.I1 客户端行只读（无点击/不叠加曲线）；仅节点行可点击展开 —— R-INST-3 〔test_client_site_tree_readonly_rows〕
- C17.I2 节点行展开状态跨重渲染保持（clientOpen 记忆） —— R-INST-1 〔◐〕
- C17.D1 贡献 = 该客户端（名称/pod）在慢点追踪 Top-N 中的条数 —— R-INST-1 〔test_client_site_contribution_and_writer_first〕
- C17.B1 性能指标列残留（旧 13 列表格） —— DR-29 〔test_client_site_header_and_columns〕
- C17.B2 实例表点击曾叠加主图曲线 —— R-INST-3 〔test_client_site_tree_readonly_rows〕

### C18 节点监控（内存 / URMA）

- C18.V1 三图：宿主内存（mem.host）/ 进程 RSS（mem.proc）/ URMA 端口流量（urma.flits）；标题后缀 —— R-MON-1 〔test_render_new_charts_structure, test_render_scenario_has_monitor_card〕
- C18.V2 部分缺失分支：URMA 无数据不留空占位、标题与图注同步 —— R-MON-4 〔test_b06_monitor_card_partial_missing〕
- C18.V3 进程 RSS 增幅排序、疑似泄露标记 —— R-MON-2/3 〔test_monitor_proc_growth_sorted〕
- C18.V4 无监控数据整卡隐藏 —— R-MON-1 〔test_render_scenario_without_monitors_hides_card〕
- C18.D1 监控时间窗与主图对齐（reduce 按 t0/t1 切片） —— R-MON-1 〔test_reduce_monitors_window_slice〕
- C18.D2 URMA 计数器回绕/重置不产生尖峰 —— R-MON-3 〔test_urma_counter_wrap_and_reset_no_spike〕
- C18.D3 产物下采样有界（compact_monitors） —— R-MON-1 〔test_compact_monitors_downsamples〕
- C18.I1 图表 tooltip 同构（悬浮指针 / 点击开框） —— R-SHELL-14 〔◐ 通用 tooltip 测试仅覆盖主图族〕
- C18.B1 监控卡在页脚之后（曾被追加到 body 末尾） —— R-SHELL-15 〔test_section_order_and_footer_note〕

### C19 overview.html（run 总览）

- C19.V1 每配置一卡 + 单元链接（`report/<unit>/report.html`） —— R-OV-1/3 〔test_render_overview, test_overview_groups_units_per_config〕
- C19.V2 跨单元聚合口径：计数求和、分位数取最差单元、注明单元数 —— R-OV-2 〔test_overview_sums_all_data_sizes〕
- C19.V3 节点日志量卡（节点/客户端数/字节） —— R-OV-1 〔test_render_overview〕
- C19.V4 监控摘要随 run 展示 —— R-MON-1 〔test_overview_shows_monitor_summary〕
- C19.I1 单元链接新标签打开 —— R-OV-3 〔test_overview_links_open_new_tab〕
- C19.I2 相对链接在 WebUI raw 模式下改写为 /api/file（`..` 逐级解析，不裁前缀） —— R-OV-3 〔test_render_overview_has_rel_base_and_adapter, test_adapter_resolves_parent_relative_links〕
- C19.B1 链接被 `slice(3)` 裁掉 `../` → 404 —— R-OV-3 〔test_adapter_resolves_parent_relative_links〕

### C21 数据量与优化（volume）

- C21.V1 独立卡「数据量与优化」，位于 **节点监控之后、页脚之前** —— R-SHELL-15/DR-40 〔test_volume_card_estimates〕
- C21.V2 表格分七段：输入（时长/实例数/采样率）→ 行数（总请求行/trace 行 + `行/秒 = 行数 ÷ 时长` 公式）→ 原始（writer.log / reader.log / glog / ylt / 合计）→ map 后（单实例 stats.json / trace.json 均值 + 构成口径：时间桶 ≤8192/档、直方图箱数、慢点 Top-N、异常/离群清单上限、trace 阶段秒桶）→ reduce 回传总量 → 输出（data.js / report.html）→ 对比（原始→回传、原始→报表倍数） —— DR-40 〔test_volume_card_estimates〕
- C21.V3 **ylt（coro_rpc easylog）不采集/不处理**：只出现在「原始」行并标注「未采集/暂不处理（仅占节点磁盘，轮转 50MB×3）」，**绝不进入 map 回传或 reduce 统计** —— DR-40 〔test_volume_card_estimates〕
- C21.V4 **glog 参与 map-reduce**：原始 glog 字节取 trace meta.bytes（实测），回传取 trace.json 文件大小（实测）；卡内必须注明该关系 —— DR-40 〔test_volume_card_estimates〕
- C21.D1 数值来源：原始 = stats/trace meta.bytes；map 产物 = 文件大小（load 时记 `_bytes`）；输出 = data.js 精确（数值迭代收敛）、report.html 两遍渲染实测 —— DR-40 〔test_volume_card_estimates〕
- C21.D2 对比口径：`原始合计 ÷ 回传总量` 与 `原始合计 ÷ (data.js + report.html)`；0910 实测约 500MB → 5MB（≈100×）→ 报表 14MB —— 〔test_volume_card_estimates〕
- C21.B1 旧产物无 volume 块 → 卡内降级说明（用 --rebuild-report 重生成），不隐藏 —— 〔test_volume_card_estimates〕
- C21.B2 曾把 ylt 误当 map 输入/回传量 → 行级文案与统计口径必须分离（ylt 只有原始行） —— 〔test_volume_card_estimates〕

### C20 跨切面（排版 / 数据口径 / 工程约束）

- C20.V1 全站字号 ≥12px（无微字幕） —— DR-24 〔test_report_has_no_micro_captions_and_tiny_fonts〕
- C20.V2 正文/标题文本对比度 ≥4.5:1（亮暗两套） —— C-5 〔test_b04_text_contrast_c5〕
- C20.V3 数字 `tabular-nums`；时延 `fmtUs` 自适应 µs/ms/s —— R-SHELL-10 〔test_a16_theme_and_numeric_css, test_a17_fmt_us_adaptive_units〕
- C20.V4 中文全角标点、省略号 `…`、中英混排保留技术词 —— 设计基调 〔◐〕
- C20.V5 卡片仅 1px 描边或底色区分（无阴影/渐变/装饰图形） —— 设计基调 〔test_c01_dr24_alignment_lock〕
- C20.V6 章节顺序与标题：SDK → 错误与异常 → 实例与日志现场 → 节点监控 → **数据量与优化** → 页脚 —— R-SHELL-15 〔test_section_order_and_footer_note, test_section_order_and_renames〕
- C20.V7 body 卡片层级完整（`div` 闭合平衡） —— 模板架构 〔test_body_div_balance_cards_inside_wrap〕
- C20.V8 **控件风格统一**：新控件必须复用既有形态（`.toolbar` 行 / `.tl-label` 标签 / `.objtree-strip` 树条 / `.tree-*` 树行 / 既有按钮）；**禁止引入新的容器底色或描边风格**；多栏布局只允许无样式 grid（先例 `.stage-split`/`.trace-cols`） —— 设计基调 〔test_control_style_consistency〕
- C20.D1 正确性口径：总计=同桶按请求数加权平均、max 取最大、直方图全量合并、离群剔除口径 —— R-SHELL-15 〔test_section_order_and_footer_note〕
- C20.D2 跨节点时间戳：当前用绝对秒 + 节点时区归一；相对秒 + 编排机参考钟为欠账 —— V-6 〔◐ test_slowest_ts_epoch_matches_buckets〕
- C20.D3 data.js 规模与嵌入预算（对象/档位/行预算） —— R-SHELL-2 〔test_objects_and_budget_trimming, test_long_run_embed_tier_and_datajs_finer〕
- C20.I1 单区块异常隔离（`safe()`），其余区块正常渲染 —— R-SHELL-8 〔test_a14_safe_isolation_single_block〕
- C20.B1 单文件自包含/零外部请求/纯标准库 —— R-SHELL-1 〔test_render_self_contained_and_data_js, test_a15_report_stdlib_only〕
- C20.B2 死 CSS 回潮（`.gkv/.gtick/.tb-sep` 等） —— DR-24 〔test_c01_dr24_alignment_lock〕
- C20.B3 SSR 冒烟兜底：桩环境测不出的真实渲染异常（markLine undefined 杀整页） —— DR-18 〔test_real_echarts_render_smoke〕
- C20.B4 大范围删改前必须 grep 引用（曾两次误删相邻函数/测试基建） —— 工程流程 〔n/a〕
- C20.B5 测试改名/删除必须同步本规则册（由完整性检查拦截） —— 工程流程 〔test_skill_rulebook_integrity〕
- C20.B7 桩环境把 `el.children` 模拟成数组，掩盖了真实浏览器 HTMLCollection 无 `forEach/map/filter` 的问题（标签聚焦标记、对象弹层「点外部关闭」在浏览器里抛错静默失效）→ 一律用 `Array.prototype.slice.call(...)`/`Array.from(...)`；新增静态审计锁 —— DR-33 〔test_no_htmlcollection_foreach〕
- C20.B6 曾给 SDK 曲线控台引入「灰底+描边的 tconsole 盒」，凭空造出第三种容器形态（与端到端 plain toolbar + objtree-strip 不一致）→ 删除该样式，用测试锁死（CSS 不得含 tconsole；trace-cols 规则不得含 background/border） —— DR-32 〔test_control_style_consistency〕

## 模板架构规范

- 共享：`_PAGE_CSS`、`_APP_JS`、`_PAGE_ADAPTER_JS`、外壳 `_page_html()`、以及各区块片段常量（`_SEC_*`）。
- 两套 body 模板：`_BODY_BASIC` / `_BODY_PIPELINE`，由片段常量组装；`render_scenario` 按 `meta.scenario`（旧产物回退 `meta.mode`；`mode="kvtest"` 视为 pipeline）选择，与 `meta.engine`（python/kvtest）无关；pipeline 页面标题为「Pipeline 场景报表」并带 `<!-- tpl:pipeline -->` 标记（basic 对应 `<!-- tpl:basic -->`）。
- JS 保持**单份共享**，模式差异只用 `D.meta.mode` 分支；禁止复制整份 JS。
- 新增区块的流程：先抽 `_SEC_<NAME>` 片段 → 在两个 body 模板中显式引用 → 再写渲染 JS（用 `safe()` 隔离）→ 补测试。
- 单元化产物：basic 目录 `c{C}_t{T}_{size}`，pipeline 目录 `{size}`；一个 size×参数组一份 report.html + data.js + meta.json。

## 验证流程（改完必过）

1. `env -u http_proxy -u https_proxy -u HTTP_PROXY -u HTTPS_PROXY python3 -m pytest webui/tests -q`（全绿）；其中 `-k rulebook_integrity` 校验规则册 ID/测试引用/覆盖率汇总一致
1b. **改代码前先 `grep` 引用**：大范围删改（splice/区域替换）前确认目标区间内没有其它函数/测试（曾两次误删相邻实现与测试基建）
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
- [ ] 规则册条目与测试同步（新增/变更行为先改 C* 条目；`rulebook_integrity` 绿）
- [ ] pytest / node --check / SSR / 截图 / 8500 全部通过
