# report.html 报表子系统 · 需求规格（REQUIREMENTS）

> **本文档是 report.html 报表子系统行为的事实源（source of truth）。**
> 每条编号需求须由至少一个通过的测试背书：本文档「通过」当且仅当全部需求在代码中成立。
> 代码行为与本文档不符即缺陷——修复方向为**改代码**或**修订本文档**，二者必居其一，不允许静默漂移。
>
> 版本：v1.0（2026-11-19 SDK 全链路重构 + 尾部章节重排：SDK 对象树/分级时延块图/分级时延曲线三维联动/慢点追踪；错误与异常按写读+节点客户端平铺；实例与日志现场合并；页脚 map-reduce 说明；登记 V-6 时间戳对齐与 V-7 数据量预估；见 DR-29） · 日期：2026-11-19 · 状态：**冻结**
>
> ⚠️ **冻结（2026-11-27 起）**：新规则/需求统一写入 `webui/tests/registry.json`（每条一句话中文 desc，机器可读，测试↔规则 1:1 由 `test_registry.py` 双向强制）；本文档仅保留历史存量，**不再新增条目**，后续整体迁移至 GitHub issues。设计判断（怎么做才对）保留在 `.opencode/skills/report-design/SKILL.md`（不再内联测试引用）。

---

## 0. 文档约定

- **编号规则**：`R-<区>-<n>`（需求）、`C-<n>`（约束）、`P-<n>`（兼容策略）、`NG-<n>`（非目标）、`OQ-<n>`（开放问题）、`DR-<n>`（决策记录）。
- **句式**：需求用受限自然语言（EARS 风格）书写——「当/当…时，〈系统〉须…」「若〈条件〉，则…」，一条需求只陈述一个可判定真假的行为。禁用「支持 / 处理 / 优化 / 更好」等不可测动词。
- **状态标记**：
  - `[已固化]` —— 现有行为，按现状固化为需求（多数已有实现，测试映射见 TESTS.md）；
  - `[新增]` —— 本轮决策引入、**尚未实现**的行为（进入实现阶段后须先写失败测试）；
  - `[推断]` —— 从代码反推的意图，**待用户确认**。
- **粒度分工**：本文档只回答「系统长什么样、如何工作」；「为什么这样实现、算法与数据结构、阈值推导」属于 [DESIGN.md](./DESIGN.md)（待撰写）。

---

## 1. 系统定位与边界

### 1.1 定位

报表子系统把一次 Mooncake 压测 run 的统计产物加工成**可独立打开的 HTML 报表**，供工程师复盘性能（定位时延毛刺、失败、泄露、阶段热点）。第一读者是**工程师本人**（DR-1），信息密度优先于演示效果。

### 1.2 数据流与组件边界

```
benchmark.py（调用方，不属于本组件）
   │  run 结束（done / failed / stopped）或 --rebuild-report
   ▼
map 产物（本组件的输入；格式契约由其各自脚本拥有）
   ├─ *.log.stats.json        ← scripts/logstats.py（节点上运行）
   ├─ <base>.trace.json       ← scripts/tracestats.py（节点上运行；glog 分段合并）
   ├─ profiling CSV           ← 采样脚本：mem.host.csv / mem.proc.csv /
   │                            urma.flits.csv / monitor.config.json
   └─ meta.json               ← run 元信息（benchmark.py 写入）
   ▼
report.py（本组件：reduce + render，纯标准库）
   ▼
产物（**单元化**：一个 data_size × 参数组 一份报表）
   ├─ logs/<cfg>/report/<unit>/report.html   单元完整报表（自包含单文件）
   │     unit = c{C}_t{T}_{size}（basic）/ {size}（pipeline）
   ├─ logs/<cfg>/report/<unit>/data.js       全量数据（可选懒加载）
   ├─ logs/<cfg>/report/<unit>/meta.json     参数/上下文存档
   └─ report/overview.html                   run 总览（每配置一卡 + 单元链接）
   ▼
浏览器（file:// 双击打开，或 WebUI /api/file?mode=raw 服务）
```

**边界声明**：
- 本组件拥有：reduce 聚合逻辑、HTML/JS 渲染、产物的全部运行时行为。
- 本组件**不拥有**：map 产物格式（logstats / tracestats / 采样脚本各自拥有；本组件在其上声明依赖字段，格式变更须双侧同步评审——例：秒级阶段桶需求横跨 tracestats 与 report 两组件）；benchmark.py 的调用时机；WebUI 的文件服务方式。
- 运行环境：桌面 Chrome / Firefox / Edge；含亮暗两套配色（见 R-SHELL-3/4）。

---

## 2. 需求主体

### A. 报告外壳（SHELL）

- **R-SHELL-1** `[已固化]` 当用户从磁盘直接打开 report.html（file:// 协议、离线环境）时，页面完整渲染：全部 CSS 与图表库（ECharts）内联于单文件，不发起任何外部请求。
- **R-SHELL-2** `[已固化·DR-50]` 当同目录存在 data.js 时，页面自动加载全量数据（对象树等覆盖全部对象）；若 data.js 加载失败（如经 WebUI raw 模式打开、相对路径不可达），静默使用内嵌子集，页面不崩，**不再有明细表降级提示**（原 p99 前 20 名实例表与 datanote 已随日志现场改版删除）。
- **R-SHELL-3** `[已固化]` 页面默认跟随系统 `prefers-color-scheme`，在亮色（纸灰底）与暗色（炭黑底）两套配色间自动切换。
- **R-SHELL-4** `[已固化·DR-6·v0.7 实现]` 用户可手动切换亮/暗主题以覆盖系统偏好；同一浏览器须记住该选择（浏览器存储不可用时退化为本次会话内生效）。交互形态（三态循环还是双态切换）在设计阶段确定。
- **R-SHELL-5** `[已固化·v0.8 修订·DR-27·DR-47]` 页头副标题按序显示：测试开始时间、报告生成时间、协议、**引擎（python / kvtest）**；（若提前停止/异常）追加「⚠ 提前停止/异常，数据不完整」。引擎取自 `meta.engine`（旧产物按 `meta.mode="kvtest"` 回退），只做标注，不改变报表格式。**「测试开始时间」取数据中最早的采样时刻**（stats 的 `ts_from` 转本地时间；pipeline 多客户端取最小值），不得使用报表生成时刻（P1-B：旧实现写生成墙钟，导致页头时间错位）。
- **R-SHELL-6** `[已固化]` 若报告包含多个 data_size，则显示「数据大小」选择器并可切换全部区块；单 size 时隐藏该选择器。
- **R-SHELL-7** `[已固化]` run 以 done / failed / stopped 任一路径结束时都产出报表（stopped 在清理前回拉原始日志本地重算）。
- **R-SHELL-8** `[已固化]` 单区块隔离：任一图表或区块初始化/渲染抛出异常时，仅该区块失效，页面其余部分正常渲染与交互。
- **R-SHELL-9** `[已固化]` 从数据注入页面的用户可控字符串（key、节点名、文件路径等）一律经 HTML 转义。
- **R-SHELL-10** `[已固化]` 时延类数值自适应单位显示（µs / ms / s），统计数字使用等宽数字（tabular-nums）。
- **R-SHELL-11** `[已固化·DR-20·v0.7 实现]` `<title>` 与 `<h1>` 须携带场景名与单元身份（形如「get_buffer_multi · c4×t8 · 128KB」，取自 meta.json 的 config 与 unit）；多个单元报表在浏览器标签页之间须可相互区分；报告生成时间只出现在副标题，不进 h1。
- **R-SHELL-12** `[已固化·DR-21·v0.7 实现]` 每个图表容器须有 `role="img"` 与 `aria-label`（内容 = 卡片标题 + 当前关键状态一句话，如档位/对象/口径）。
- **R-SHELL-13** `[已固化·DR-24·v0.7 实现]` 标题层级不跳级：h1 → h2 → h3 严格递进（h3 分节标题须有所属 h2 卡片标题）；交互说明不得写进标题文本。
- **R-SHELL-14** `[已固化·v1.2 简化·DR-42·v1.3 修订·DR-44]` 图表 tooltip 一律**原生悬浮**：竖线 + 各曲线交点 + 内容框由 ECharts 同时渲染与消失（`triggerOn:'mousemove'`、`showContent:true`、`hideDelay:250ms`、`enterable:true`、框内滚动 `max-height:46vh`）；**不得设置 `showDelay`**（与 globalout 竞态会导致框残留并遮挡右侧标签）；`globalout` 时主动 `hideTip` 兜底。**拒绝「点击钉住」**。鼠标扫过曲线不得触发聚焦/加粗；聚焦只由右侧标签悬浮触发。范围：全部图表。
- **R-SHELL-15** `[已固化·DR-29·v1.1 修订·DR-40]` 章节顺序固定为：段1 → 端到端时延曲线 → 时延分布 → 慢点捕捉 → **SDK 全链路 → 错误与异常 → 日志现场 → 节点监控 → 数据量与优化 → 页脚**；页脚（最底部）说明数据文件（report.html 自包含 + data.js 全量）、map→reduce 生成流程、展示策略与**数据正确性口径**（总计=各节点同桶按请求数加权平均、max 取最大、直方图全量合并、离群值剔除口径）。

### B. 性能统计卡（PERF）

- **R-PERF-6** `[v1.1 新增·DR-53·DR-54 修订]` **分位口径（受控近似，单一管线）**：count/avg/min/max 精确；p50/p90/p95 = 合并直方图插值（us-v1 桶）；p99/p999/p9999 = 合并 REQ sketch（Apache DataSketches，`quantiles.req_k` 可配、默认 24）。**两阶段 reduce**：pod 内 map 生成 digest 并把 p50..p9999 写入 stats 产物（进程内可算尾部）；controller partial reduce 置尾部 `null` + `pct_method="pending"` + `_digest_pending`（按 scope 去重的 digest 列表）；编排机 finalize 合并 3 个 scope（all/writer/reader）后回填 9 个尾部值 + `pct_method="hist+digest"`。**依赖严格**：pod（map）与编排机（finalize）必须安装 datasketches（缺失即报错，不允许静默退化为直方图尾部）；controller 零第三方库。报表顶部（测试概览）以数据驱动注记统计值获取方式与实测误差量级，size 块携带 `pct_method`；CSV 与报表消费同一口径，CSV 只读 reduced.json（instances 块 + role_summaries）。
- **R-PERF-1** `[已固化·DR-13·v1.1 修订·DR-41]` 性能统计分三行：第一行 = 吞吐 + 平均时延 / P99 / P9999（三项加粗）；第二行 = p50 / p95 / p999 / 最大（常规字重）；**第三行 = 总数 / 成功 / 失败 / 失败率 / QPS**（常规字重；原「信息展示」子区已删除）。
- **R-PERF-2** `[已固化·DR-13]` 性能统计不再包含 QPS，也不显示「QPS 达成率」刻度卡（实际 QPS 见信息展示）。
- **R-PERF-3** `[已固化]` 队列等待均值/峰值（及最大队列深度）仅在日志含对应数据时显示于第二行。
- **R-PERF-4** `[已固化]` 分位数由跨客户端**合并后的稀疏直方图**还原，不是各客户端分位数的平均。
- **R-PERF-5** `[已固化]` 若时长 wall_s = 0（无时间戳日志），则时长显示「-」，QPS/吞吐不产生 Infinity 或除零错误。

### C. 信息展示（INFO）

- **R-INFO-1** `[已固化·DR-13·v1.1 重写·DR-41]` basic 场景**不再有「信息展示」子区**：总数 / 成功 / 失败 / 失败率（两位小数百分比）/ QPS 直接作为性能统计**第三行**；数据大小 / 客户端数 / 线程数 / 时长 仍位于参数配置标题下、参数表格上方（`#unitmeta`）。pipeline 分角色区块内同构（性能两行 + 第三行统计项）。
- **R-INFO-2** `[已固化·DR-3]` 多参数组 sweep（≥2 组 num_clients×num_threads）时，客户端数显示全部实例数，并追加合并口径说明（单组数值见明细表与对象曲线）。

### D. 参数配置（PARAM）

- **R-PARAM-1** `[已固化·DR-13]` 参数配置区显示本次 run 的参数与运行上下文（场景/协议/工作模式/采集与预热时长/操作/key_count/目标 QPS/节点数/配置文件名等），空值省略；**信息展示已给出的值不重复**（数据大小、客户端数、线程数、客户端总数、采集时长、开始时间均由渲染层过滤）。
- **R-PARAM-2** `[已固化·DR-59 修订]` 上述参数在生成时持久化到 report 同目录 meta.json；`--rebuild-report` 优先从 meta.json 恢复；历史 run 无 meta.json 时按文件名与 stats 角色降级推断。生效配置（继承解析结果+运行期键）另持久化为 `logs/<cfg>/report/cfg.json`，`--rebuild-report` 的 CSV 重算以它与 run 时严格同源（发现链：cfg.json → 实例快照 → 仓库配置，见 DR-59）。

### E. 端到端时延曲线（LAT）

- **R-LAT-1** `[已固化·DR-15·DR-47]` 对象树按**报表形态 scenario**组织：**basic** = 顶部单行「总览」（全部节点/实例的平均，无角色分组）+「节点与实例」组（节点均值与其下实例）；**pipeline** = 「总体（全部）/ 角色（writer、reader）」+「节点与实例」。可勾选多个对象叠加对比；单对象时不显示图例；默认仅选「总览/总体」。
  - **格式只由 scenario 决定，与 engine 无关**：python 与 kvtest 都可有 basic / pipeline 形态；kvtest-pipeline 与 python-pipeline 同构（角色单选 + 总计 + 角色化节点，同一节点每角色仅一行），kvtest-basic 与 python-basic 同构（单「总览」、无角色）。
  - 生成器写 `meta.scenario`（basic|pipeline）与 `meta.engine`（python|kvtest），`meta.mode` 镜像 scenario 仅作兼容；旧产物 `meta.mode="kvtest"` 读取时归一化为 pipeline 形态 + kvtest 引擎。
  - pipeline 只剩单一角色（早停）时，角色视图自动切到现存角色，不得出现空树/空图。
- **R-LAT-2** `[已固化]` 粒度档提供 1 秒 / 10 秒 / 1 分钟 / 自适应；可用档由 run 跨度与产物内嵌档决定，默认档按跨度自适应选择。
- **R-LAT-3** `[已固化·DR-13]` 曲线指标命名为 avg / p50 / p99 / p999 / p9999 / min / max（原「平均/最大」已更名）；多对象对比可在 8 个指标间切换：avg / p50 / p99 / p999 / max / QPS / 失败 / 队列等待；单对象视图显示 p50 / p99 / p999 / p9999（样本≥10000）+ min / max 线 + QPS 柱 + 失败柱。**「时延曲线选择」提供 全选（8 项全开，含队列等待）/ 清空（全关但保底 avg，防空图）**；SDK 全链路的阶段行同样提供全选/清空（v1.0 增补·DR-29）
- **R-LAT-4** `[已固化·v0.5 修订·DR-23]` 「剔除离群值」开关：开启后单桶最大值超过阈值（p99 × 10）的桶在图上截顶至阈值、以虚线标注，Tooltip 注明该桶含离群值；若阈值 ≥ 该序列最大值（开关无任何可命中桶），则隐藏或置灰该开关，不得呈现永远无效的控件。**离群点显示 = 散点 + 从底部到该点的旗杆虚线（上端止于离群点）**；开关在无可命中离群时置灰并在 title 说明判定条件（v1.3 修订·DR-44）
- **R-LAT-5** `[已固化]` 时延轴缩放滑杆：范围 1.0x（完整视图）至最大倍率，放大锚点为当前窗口下边缘；时延轴窗口指示条可拖动平移（与图内纵向拖动等效）。
- **R-LAT-6** `[已固化]` 时间轴支持图内横向拖拽平移与滚轮缩放；主图与 QPS 副图缩放联动。
- **R-LAT-7** `[已固化]` 端点标识锚定绘图区右缘：引线指向各对象在当前窗口内最后一个可见点（主指标 p99），多条标识纵向自动排开防重叠。
- **R-LAT-20** `[v1.1 新增·DR-52]` **全局统计横线（仅端到端时延曲线）**：在当前角色（pipeline 的 writer/reader，basic 无角色则不画）上叠加 avg / p50 / p95 / p99 / p999 / p9999 六条**水平参考线**，取值 = 该角色 `role_summaries` 的正式窗口全局统计（warmup 区剔除后），**不随所选节点/实例变化**；样式 = 同指标颜色 + 虚线 + 无端点标签、沉于分桶曲线下方；图例 = 「全局横线」按钮组（点击隐藏/显示，默认仅 avg / p99 / p9999 可见）；tooltip 按可见性追加全局行；本特性为「同构原则」的**显式例外**，分级时延曲线不绘制。
- **R-LAT-8** `[已固化·DR-13·v0.9 修订·v1.0 修订·DR-36]` QPS 副图为双轴：QPS 柱（左）+ 失败柱（右，独立计数）。**多对象时**：QPS 用细柱（按槽位像素宽自适应 1–3px）、同一时间桶按槽位并排（第 i 个对象 x 偏移 `(i-(n-1)/2)·桶距×0.8/n`，不再完全重叠）；失败柱与所属对象**同槽位对齐**（位置即归属）、统一强调橙。**单对象**：QPS 7px / 失败 3px、不偏移。所有柱 `clip:true`（x 轴放大不得画出绘图区）。图例只列各对象 QPS（失败不逐个列）、不可点击隐藏；无失败显示「本次测试无失败」。
- **R-LAT-9** `[已固化]` 无数据窗口标注：相邻桶间隔超过该序列桶宽中位数的 3 倍时，主图与 QPS 图以浅色竖带标出并注明「N 秒/分钟无数据」。
- **R-LAT-10** `[已固化·v1.3 修订·DR-44]` 异常窗口（map 阶段 anomalies：错误突发 / 时延突增）在主图上以强调色竖线标注，**15% 透明度、宽度 1、≤50 条**（低权重，避免与离群点混淆）；图注说明「红色竖线 = 异常窗口（非离群点）」。
- **R-LAT-11** `[已固化·DR-14·DR-19]` 曲线叠加仅通过对象树勾选控制；实例明细表为只读表格，行点击不产生任何交互（不叠加曲线、不展开）。
- **R-LAT-12** `[已固化·v0.8 重写·DR-27]` 曲线配色按**对象类型**（单/多对象一致，不按选中数量切换）：**总览（basic）/ 总计（pipeline）= 灰阶**（avg / p99 / p9999 深灰，其余浅灰）；**节点 = 固定色系**（按节点名排序分配，跨选择稳定，亮暗各一套）；**实例 = 所属节点色系的色阶**；任何常规曲线不得使用品牌橙。
- **R-LAT-13** `[已固化·DR-13]` 时延工具栏为单行（粒度档 / 指标按钮 / 剔除离群值 / 重置视图，无竖向分割线；窄屏横向滚动不换行，子项垂直居中）；时延轴与时间轴的平移滑块窗口无描边。
- **R-LAT-14** `[已固化]` 端点标识的标签与引线两端均钳制在绘图区内；标签纵向聚集时依次分散、不重叠（含顶/底同时聚集与全贴底）。
- **R-LAT-15** `[已固化·DR-27·v1.2 修订·DR-42]` 曲线线宽**恒定**：avg 1.0px，其余指标 0.8px；**无 hover 加粗**（鼠标扫过曲线不得变粗/聚焦其他线被压暗——曾因 `emphasis.focus:'series'` 导致闪烁）；**无「首对象加粗」**。强调效果只由右侧标签悬浮触发（R-LAT-16/19）。阶段曲线同规则（total 1.0 / 阶段 0.8）。
- **R-LAT-16** `[已固化·DR-27·v0.8 实现·v0.9 扩展·DR-28]` 端点标签 hover 时高亮该标签所属整簇：多对象为「`<对象> <指标>`」前缀匹配的全部序列，单对象为该指标序列；移出后复原（downplay）。标签 title 说明「多对象时高亮整个曲线簇」。**单对象指标标签同样以 `mcFocus` 压暗其他指标实现聚焦**（不依赖 emphasis，DR-42 已删除线序列 emphasis）；分级时延曲线标签同机制（v1.3 修订·DR-45）
- **R-LAT-17** `[已固化·v0.9 修订·DR-28]` 「节点与实例」组的操作按钮按选择状态**动态切换**：存在未选节点时显示「全选节点」（点击全选），已全选时显示「清空节点」（点击取消）；实例同理（全选实例 ↔ 清空实例）。另有「全部清空」清空节点与实例并**自动开启总计/总览**（pipeline 置 `totalOn=true`，basic 保留总览）。上述行为同时适用于主对象树与慢点捕捉控件。对象树之外的工具栏不再重复提供全选/清空按钮。
- **R-LAT-18** `[已固化·v0.8 实现·v1.0 修订·DR-33]` **多对象**（≥2 个对象）时右侧端点标签 = 对象名，**锚定该对象 p99 曲线在当前窗口内的末端位置**（endLabelLayout 防重叠：最小间距 19px、顶底钳制；标签相对上下顺序与曲线一致）；不画引线、不绑单点（窗口内无可见点退化为整体最后一点）；标签颜色=对象颜色；超长名称截断（`max-width`+省略号），`title` 悬浮展示全名。**单序列不显示右侧标签**。
- **R-LAT-19** `[已固化·v0.9 实现·v1.0 修订·DR-33/34]` 标签聚焦 = **纯悬浮预览**：悬浮时该对象/序列整簇保持原色、其他压暗（opacity 0.15）；指针移入绘图区或离开标签立即恢复全部；**不提供点击锁定**（分级图 `finished` 重渲会使锁定态与元素失联）。画布 mousemove 兜底清除预览（防标签重建丢 mouseleave 导致卡死）。多对象主图图例 `selectedMode:false`。

### F. 时延分布（HIST）

- **R-HIST-1** `[已固化]` 对数分箱直方图（跨客户端合并）显示样本频数，x 轴为分箱区间标签。
- **R-HIST-2** `[已固化·DR-13]` 标注 avg / p50 / p99 / p999 / p9999（p9999 仅当总样本 ≥ 10000）；**所有标注（文字 + 竖直虚线）统一为 p50 的灰色、细虚线**；标签水平居中，并按像素距离错层防重叠（不再按边缘做左右对齐）；卡内说明仅保留「p9999 需样本 ≥ 10000」。
- **R-HIST-3** `[已固化]` 无直方图数据时整卡隐藏。

### G. 慢点捕捉（SLOW）

- **R-SLOW-1** `[已固化]` 取时延 Top-100 慢操作（两级截断：map 每客户端 Top-100 → reduce 合并后整体 Top-100）绘制时间条带图（横轴=发生时刻，纵轴=时延），该组最慢一根以强调色高亮（图注：橙色为该组最慢）。
- **R-SLOW-2** `[已固化]` 慢点时刻使用归一化的绝对时刻（ts_epoch），与主图时间轴同口径，不受查看者时区影响。
- **R-SLOW-3** `[已固化]` 无慢点数据时整卡隐藏。

### H. SDK 全链路（TRACE）

- **R-TRACE-1** `[已固化]` 元信息行显示：trace 文件数、解析行数、slice 行数、解析失败数、采样率、按阶段标记剔除的样本统计（预热/启动/收尾/计入统计）。
- **R-TRACE-2** `[已固化·DR-13]` 读 / 写 / 其它分组按钮切换（basic 与 pipeline 均保留）；本卡**不再有独立「数据大小」选择器**（跟随页面最上方主选择器）；操作选择器仅在可选项 >1 时显示；删除方向说明注释。
- **R-TRACE-3** `[已固化·DR-13]` 样本统计分两行：第一行加粗 平均 / P99 / P9999（p9999 仅当样本 ≥ 10000）；第二行 样本数 / 失败 / p50 / p95 / p999 / 最大；数值按 µs / ms 自适应（与段1 同构）。
- **R-TRACE-4** `[已固化·DR-13]` 分级时延块图：块宽 = 阶段值占父阶段的百分比；**0% 块的宽度 = 该块文字宽（名称/值两行取最宽）+ 标准 padding**（尽可能窄）；点击块聚焦、下一级在新行展开；提供展开全部 / 收起全部 / 取消聚焦；残差（total − 阶段和）单独成块且完整显示；basic 下对象树同样为单行「总览」。
- **R-TRACE-5** `[已固化·DR-17·v1.0 修订]` 分级时延曲线：total + L1 阶段 + 残差可多选同图对比（单对象可多阶段 / 单阶段可多对象，多选一侧另一侧自动收敛）；**分桶档位策略与端到端曲线一致**：档位 s ∈ {1s,10s,1min} 仅在 `span/s ≥ 8` 且 `s ≥ 产物 ftier` 时提供（不满足的档位隐藏），自适应恒提供（目标 30~120 桶）；当前档失效回落自适应；工具栏为「粒度档 + 重置视图」一行、「阶段」独立一行，垂直居中；对象树（总览/节点/实例）与指标行位于分级时延上方。
- **R-TRACE-6** `[已固化·v1.0 重写·DR-29/35]` SDK 区对象树为 **总计（当前读写组的全部实例聚合）+ 节点与实例**；**不再有角色行**——写路径/读路径由 section 顶部总控切换（互斥）。tree 行为：全选节点/全选实例按状态变「清空节点/清空实例」；**全部清空 = 真清空（允许空选，空态提示；不回填总计）**。数据侧 reduce 按 `meta.node/pod` 保留阶段桶（总计全嵌；节点/实例按 `TRACE_EMBED_POINTS` 抽稀 + 行预算，`ftier` 每 op 只乘一次）。
- **R-TRACE-7** `[已固化·DR-18·DR-55 修订]` 最慢 trace 表（Top-100：start_time / trace_id / key / op / status / node / client / size / total_us / 主要阶段）；状态分布已移到错误与异常卡（见 R-ERR-1，按写/读路径各一条）；点击行展开该请求的**完整分级时延**：树序全部 L1（含 0/缺失）+ 残差，且**所有有二级子区间且值>0 的 L1 同时各展开一行二级**（含「未归因」）；块 tooltip 附全局均值与倍率。同表上方新增**慢请求阶段构成堆叠直方图**（柱高=端到端时延，自下而上按树序堆一级阶段含残差，灰阶=平均占比越大越深，仅一级）。
- **R-TRACE-10** `[已固化·DR-18]` trace map 的 Top-N 慢请求条目保留 **L1 + L2 阶段字段**与 `ts_epoch`（本地重算时按 MC_PHASE 标记自校准宿主时区偏移，保证 measured 窗口分类正确）。
- **R-TRACE-8** `[已固化]` 无 trace 产物时整卡隐藏。
- **R-TRACE-9** `[已关闭·v1.0·DR-29]` 节点/实例级分组欠账已由 trace 对象数据补齐（见 R-TRACE-6/11）；保留编号供历史追溯。
- **R-TRACE-11** `[已固化·DR-29·v1.0 实现]` **分级时延块图（trace）**（原名「分级时延」）：删除「指标概览」卡（tcards，段1 已给一眼指标）；对象**单选**（总计/某节点/某实例，下拉）+ 指标**单选**（min/avg/p50/p99/p999/p9999/max，按字段统计可用）；块宽与百分比作用于所选对象。
- **R-TRACE-12** `[已固化·DR-29·v1.0 修订·DR-35/37]` **分级时延曲线（trace）**：对象与阶段**自由组合**（两者均为独立多选 checkbox，N×M 任意叠加，互不收敛）；允许空选（清空阶段或对象 → 明确空态文案，**对象为空时不回退画总计**）；每组首次进入默认 `总计 + total`。呈现：单对象=阶段调色板（可叠加多阶段）；多对象=颜色按对象（总计灰阶/节点色系/实例色阶）、阶段>1 时以线型（实/虚/点）区分阶段，序列名 `对象 · 阶段`；指标单选（均值/最大）；右侧标签锚定各曲线末端 + 悬浮压暗；阶段树 L2 无时间桶显示但禁用；总计/节点/实例统称「对象」。
- **R-TRACE-13** `[已固化·DR-29·v1.0 实现]` **慢点追踪（trace）**（原名「最慢请求」）：受顶部写/读总控（互斥）；柱状图横轴改为**按端到端时延降序**排列（轴标签=时延值，可看出 1000ms/500ms 等聚集簇），柱高=端到端时延、自下而上=一级阶段（含残差），**每个阶段使用不同颜色**；**图例点击 = solo**（只保留该阶段，再点同一图例恢复全部）；表格行点击展开该请求的阶段构成，**不含 total 行**、单行压缩、横向滚动；Top-N 参数化（当前 100，后续可 1000）。**图与表均按当前写/读组取数：先按组过滤、再各组各自 Top-N（组内恰 K 条），图/表同集（行数与柱数一致）**。
- **R-TRACE-14** `[已固化·DR-56·DR-66 修订]` **统一 `breakdown[<op>]` 解析（树驱动单一事实源）**（对齐 Mooncake 仓库 `docs/yh/log-reference.md` §14）：每个 op 一份阶段树 `configs/_trace_trees/<op>.json`（schema `mp-trace-tree/1`；13 棵 = 12 个真实接口 + `batch_upsert_from` 薄行），`scripts/trace_trees.py` 为唯一读取入口，**目录缺失或静态校验失败一律硬失败（TreeError，不静默回退）**；tracestats（map）与 report.py（build 时 import 并向页面嵌 `TRACE_TREES`）同源消费，渲染 JS 对具体 op 零知识（只遍历 `[field,level,parent,view]` 通用行）。**字段名 = 日志原生名**（`query_total_us`/`buf_alloc_us`，不再归一改名）；旧 `get_breakdown` 行按 legacy 别名**单向**映射（`query_us → query_total_us`、`alloc_us → buf_alloc_us`；旧 op 名 `get`/`get_batch`/`batch_get_buffer`/`batch_get_into_buffer` 归一到 `get_buffer`）。**树口径**：读 = `get_buffer`（query_total/select/buf_alloc/read + query 三段下钻；read 的 classic 传输段四字段、tent 四相位、A-B 分段为并列 views）/ `get_into`（query/select/read + offload 视图）/ `batch_get`/`batch_get_into`（+`prep_us` L1）；写 = `put`/`put_from`（传输段 switch：classic `memwrite_*` ↔ tent 四相位；不列恒 0 的 query/finalize/cache_invalidate）/ `upsert`/`upsert_from`（query/finalize；不列恒 0 的 put_start/put_end）/ `batch_put`/`batch_put_into`/`batch_upsert_from` 薄行（prep/write 两段）；`remove`/`batchRemove` 薄行归「其它」组。**views/switch**：同一父节点的多套分解互不加和，L2 子行带徽标（classic/tent/A-B/offload）；put 族传输段按行内 `engine` 切换 L1 形状（缺字段的行自然为空，混跑残差按出现的 L1 计算）。未知 op 归「其它」组并计 `meta.unknown_ops`（字段探测仅兜底）；`engine[tent|classic]` 仅作分布标注（TENT+TCP 时四相位为诚实零值，残差吸收传输），不参与分组与门控。成功状态统一 `status[ok]`（`read_ok` 仅旧产物兼容，归一为 ok）。**reduce 必须按 op 合并 `engine` 分布**（classic/tent 计数不得在合并阶段丢失），SDK 元信息行展示 `engine classic×N / tent×M`（TENT+TCP 退化时可见 tent 与诚实零相位）。

### I. 实例详情 / 错误 / 现场索引（INST / ERR / SRC）

- **R-INST-1** `[已固化·DR-29·DR-48·DR-50 重写]` **日志现场 = 唯一一张「现场日志」表**（展平不折叠），列固定为 **节点 / 角色 / pod / 日志属性 / 日志路径**：
  - **节点行**（该节点聚合）：节点列填节点 IP；日志属性 = `x 个子项 · xx KB`（该节点全部已知产物的去重文件数与字节合计）；日志路径 = 节点日志根目录（`<host_log_root>/<run_id>`；ssh = 该节点日志目录）；角色/pod 列留空；
  - **pod 行**：节点列留空；角色 = 该 pod 日志的角色集合（如 `reader、writer`）；pod 列填 pod 名；日志属性 = 大小；日志路径 = 该 pod 的节点侧目录全路径；
  - **监控行**（每节点恰好一行，位于该节点 pod 行之后）：角色「监控」；开启监控时 pod 列填监控 pod 名、日志属性 = 大小、日志路径 = 节点侧 profiling 目录；**未开启时 pod/日志属性/日志路径均为 `-`**（行仍保留）；
  - **整表逐行 nowrap + 全表水平滑动**；节点行加粗；
  - **结构性不变量**：任何产物（stats/glog/监控 CSV 等）必须落在表中某行的日志路径之下；落不进规范目录的产物强制以自身目录成行（**永不静默丢产物**；属结构性 bug 时必须显式暴露）；
  - **大小口径（单一口径）**：kvtest 每进程 `meta.bytes` = 其现场目录 `<pod>/<role>_i<iid>_p<port>/` 的 apparent size（pod 内 map 开始前 `os.lstat` 求和，含样本/glog/产物；**不得跟随符号链接**——glog_compat 在 `.glog.d/` 留 `<prog>.INFO` 软链，`os.stat` 会把每段 glog 计两次、目录大小虚高 ~2×）；python 引擎为已知产物字节合计。**pod 行按 `remote_path`（进程目录/文件路径）维度去重**（writer/reader 的 `source` 同名 `latency_samples.csv` 不得折叠），**嵌套在已计目录内的产物（glog trace 等）不再叠加字节**；节点行 = 该节点各条目之和 + 监控产物；节点行「x 个子项」= 进程目录数 + 未嵌套文件数；不再有 `du -sb` 快照/`site_sizes.json`；
  - 已删除：客户端实例树（总数/成功/失败/客户端列、对慢点追踪的贡献列、writer 在前排序）、节点汇总表、登录命令/查看列、其它产物索引、data.js 降级提示；性能指标（avg/p50/p99…）不在本卡展示。
- **R-INST-3** `[已固化·DR-19·DR-29·DR-50]` 现场日志表**全部只读**（无点击交互、不叠加曲线、无折叠）；「节点与登录现场」小节及其节点汇总/登录命令/非客户端产物索引已删除，内容由 R-INST-1 单表承载。
- **R-INST-4** `[已固化·DR-46]` **节点身份 = 节点 IP**：k8s 场景取 `pod.status.hostIP`（宿主节点 IP），ssh 场景取节点管理 IP；同一宿主上的多个 client pod 在**节点级合并**为一个节点（对象树/实例节点列/错误归属/来源表/监控标签一致）；pod 名保留在 `meta.pod` 与客户端名中（明细不丢）。取不到 IP 或 ssh 配置出现重复 IP 时回退原节点名；历史产物（IP 未记录）沿用 pod 名/节点名。
- **R-INST-2** `[已固化·DR-3]` 多参数组时「参数组」筛选栏同时作用于对象树与明细表（隐藏不在该组的客户端）；被隐藏的客户端若正处于选中态，则选中集回落到「总体」。
- **R-ERR-1** `[已固化·v1.0 重写·DR-29·DR-48·DR-55 修订]` 错误与异常按**写路径 → 读路径**平铺（basic 单块）；每块为「节点 → 客户端」两级表：节点汇总行（实例数/总数/失败/状态码前 3）+ 客户端行（总数/失败/状态码明细），**精确到节点与客户端**；**节点行带收放按钮（▸/▾），实例行默认折叠**；保留首错误原文摘录（≤16KB）；**状态分布为写路径、读路径各一条横向分段条**（位于各自路径块的最后一行，段色=状态、图例=状态×次数、右侧=合计），数据取 SDK 全链路对应组（write/read）的**全实例**状态计数，**与「SDK 全链路」的对象选择器完全解耦**（切换全链路对象不影响该条）；成功状态统一归档 `ok`（C++ 读路径历史 `read_ok` 归一为 `ok`，否则会被误判为失败）；无错误时显示「无错误」。
- **R-ERR-2** `[已固化·v1.0 修订·DR-29]` 异常窗口列表（≤100 条）：时刻 + 类型（错误突发/时延突增）+ 数值 + **归属（角色 · 节点 · 实例）**，与主图强调色竖线同源；reduce 按 (时刻,类型,节点) 去重并携带归属。
- **R-SRC-1** `[已固化·v0.7 修订·DR-50·DR-54]` 现场索引 = R-INST-1 的**现场日志单表**（节点行 + pod 行 + 监控行）：节点行给出节点日志根目录，pod 行给出逐 pod 的节点侧目录（`<host_log_root>/<run_id>/<pod>/`，其下为每进程子目录 `<role>_i<iid>_p<port>/`），监控行给出节点侧 profiling 目录；路径一律来自产物 `meta.remote_path`（kvtest=该进程 stats 产物，故行目录=进程目录）；ssh `<节点IP>` 后 cd 对应路径即可看到该处全部日志。路径必须由产物生成阶段写入节点侧真实路径（见 R-SRC-2），报表只做展示；旧产物的 pod 内路径允许在 reduce 侧做一次挂载逆变换（`remote_log_dir` → `host_log_root/run_id`）。
- **R-SRC-2** `[已固化]` **日志目录契约**：k8s 每 run 每节点一个唯一目录（hostPath `<host_log_root>/<run_id>` 挂载到 `<pod_workspace>/logs/<run_id>`），每 pod 一个专属子目录存放该实例全部产物（`.log/.log.ylt.log/.log.exitrc/.samples.csv(.gz)/.summary.json/<base>.trace.json(.gz)/kvtest_out/`、remove-keys 四件套、remove 服务日志、以及 `MC_LOG_DIR` 的 `<base>.glog.d/`——完整 glog 的 INFO 分段，glog 按 max_log_size 轮转、解析时多段合并；不再产生 `.log.glog` stderr 捕获）；索引路径在 collect/job/map 阶段生成，**禁止渲染侧改写**；`MC_LOG_DIR` 默认指向 `<per-pod>/<client>.glog.d/`（位于 hostPath 映射内，每进程/size 独立）。
  - **已删除（DR-54）**：`du -sb` 现场大小快照与 `logs/<配置>/report/site_sizes.json`（全局删除，含 python 引擎路径）；大小统一回到 `meta.bytes`（见 R-INST-1）。
- **R-SRC-3** `[v1.1 新增·DR-54]` **kvtest 两阶段数据面（严格，无回退）**：① pod 内单遍 map（`logstats.py --kvtest-map-instance`）：读本进程 `latency_samples.csv` 一次，就地读 `summary.json`，写 `stats.json`（直方图/digest/慢点/异常）与 `csvstats.json`（正式窗口逐 op 计数/字节/均值/极值 + summary 的 notify/queue/verify），`meta.bytes`=目录 apparent size（写产物前采集）；② controller partial reduce（`scripts/kvtest_partial_reduce.py`，仅 stdlib + staged `report.py`/`logstats.py`）：合并除 digest 外的全部聚合，增发 `instances` 块，输出单个 `partial.json.gz`（尾部置 null + `_digest_pending`）；不建 logsite、不注入监控；③ 编排机 finalize/render/CSV：合并 digest、注入 monitors、构建 logsite、写 `reduced.json`、渲染 HTML；CSV 只读 reduced。**禁止**：samples/summary 传输、flatten、uncompressed gzip 回退、`.log.glog` 回退、controller 失败回退本地、digest 缺失回退直方图。STOP 走同一 pod map→partial 流程（controller 不可达则 WARN 跳过报表、清理照常）。

### J. 节点监控（MON）

- **R-MON-1** `[已固化·DR-49]` 节点监控卡含三图：宿主内存（mem.host.csv）、进程 RSS（mem.proc.csv）、URMA 端口流量（urma.flits.csv）。**产物目录名 = 节点 IP**（k8s=宿主 IP / ssh=管理 IP；显示名重复时回退原名，绝不互相覆盖），报表标签与目录名一致（旧产物按 `monitor.config.json` 的 `node_ip`）。**有监控产物但没有可用序列时渲染占位卡列出原因**（URMA 采集失败原文 / CSV 无数据行 / 时间窗无交叠），只有完全无 profiling 产物才整卡隐藏。
- **R-MON-2** `[已固化·DR-7]` 进程 RSS 增幅正负条按节点展示增幅（MB 与百分比）；疑似泄露判定为「增幅 ≥ 阈值 且 增幅 MB ≥ 下限」，阈值经 monitor.config.json 可配（`mem.leak_growth_pct` / `mem.leak_min_mb`），默认 **30% / 200MB**。
- **R-MON-3** `[已固化]` URMA 计数器差分遇负增量（计数器回绕/复位归零）时标记 resync 并跳过紧随的一拍，不产生假尖峰。
- **R-MON-4** `[已固化·v0.5 修订·DR-23·DR-49]` 无 profiling 产物时整卡隐藏；部分缺失（如仅有 mem 无 urma）时只显示有数据的图，卡片标题与图注同步去掉对应分句，不留下空图占位或对不存在内容的说明；此时若有失败原因（如 URMA slot 探测失败），以原因注记形式展示，不静默。
- **R-MON-5** `[已固化·DR-49]` **监控时间基准与 URMA slot 探测**：kvtest 采样器启动时注入编排器时钟基准 `EPOCH_BASE`，采样时间戳 = `EPOCH_BASE + 节点时钟流逝`（编排器时钟域，与 kvtest stats 的 `/start` 时间窗同域，不受节点/编排机时钟偏差影响）；python 引擎保持节点绝对时间（其 stats 为节点钟域）。若时间窗与采样仍无交叠（旧产物/异常时钟），reduce 回退全量采样并注记「疑似时钟偏差」，绝不因裁空丢图。URMA `slot` 以 `mdcli` 报错中的 `available slot id` 为准（配置值仅作首选提示），自动切换并记录 probe 日志。
- **R-MON-6** `[已固化·DR-68]` 进程 RSS：**RSS 增长 TOP-10 正负条位于进程图下方**并带标题「RSS 增长 TOP-10」；进程节点行仅分组、**无 checkbox**（节点级求和/平均语义有歧义），视觉与标准 `.tree-row` 一致（不单独染节点色/加粗），保留节点内「全选/清空」mini；首次进入**默认只勾选** `mem.proc_pattern` 前缀进程（内核 `comm` 截断 15 字符），配置缺失回退 `mooncake_kvtest`、一个未命中回退全选；其余进程仍在树中可手动勾选；未勾选时**不显示「未勾选进程：…」文案**（空图即可）。
- **R-MON-7** `[已固化·DR-68]` URMA 采样默认 1s 一次（`urma.interval_s` 可配）；速率点时间**量化到采样周期**（相对任务开始 t0 的桶）并对同桶样本取平均，使各节点共享同一组 x 值——hover 竖线可同时命中所有序列，不再“一条竖线只对应一个节点”。
- **R-CHART-1** `[已固化·DR-68]` **统一图表框架**：新增/改造任何图必须走 `lfChartFrame` 模板（图 / 图例 / x 轴控件 / y 轴控件唯一入口），禁止再单独拼 legend/axis/dataZoom。
- **R-CHART-2** `[已固化·DR-68]` **图距与图例边界**：图与上方控件的垂直间距统一（`#memchart/#procchart/#urmachart` 统一 `margin-top`），图例顶部留白由 `grid.top` 保证、不被容器上沿截断；图例左右边界 = `grid.left`/`grid.right`，宽度不越过 y 轴标签区。
- **R-CHART-3** `[已固化·DR-68]` **tooltip 时间为人类可读**：监控图显示 `T+mm:ss` 相对时间 + 墙钟 `YYYY-MM-DD HH:MM:SS`；任何图不得输出 epoch 浮点数时间戳。
- **R-CHART-4** `[已固化·DR-68]` **缩放控件**：x 轴底部原生 `slider` + `inside`（滚轮缩放/拖拽平移）；y 轴右侧原生竖向 `slider` + `inside`（上下平移）；一律使用 ECharts 原生组件，不新增自绘缩放轨。

### K. run 总览（OV）

- **R-OV-1** `[已固化·DR-9·DR-12·DR-69]` overview.html 为 run 级索引：**每配置一张卡**（跨单元聚合口径 + 分位数取最差单元），**卡内指标行显示 吞吐/avg/p99/p9999**：pipeline 形态按角色拆两行（`写路径（writer）` / `读路径（reader）`，各取该角色 `role_summaries` 跨单元聚合；basic 形态单行合并口径）；卡内列出该配置的**逐单元链接**（一个 size×参数组 一份报表）；点击链接在新标签页打开对应单元报表。
- **R-OV-2** `[已固化]` 节点日志量表：每节点的客户端数与日志量。
- **R-OV-3** `[已固化]` overview 内链接对产物所在目录层级自适应（磁盘双击打开与 WebUI raw 模式均可达）。
- **R-OV-4** `[已固化·DR-69]` 跨单元聚合口径与注记：计数/字节求和，avg 按成功数加权，**吞吐 = 字节合计 ÷ 时长合计**，p99/p9999 取最差单元；单元数 > 1 时卡内注明该口径。

---


### K. 数据量与优化（VOL）

- **R-VOL-1** `[已固化·DR-40]` 报表含**独立卡「数据量与优化」**，位于节点监控之后、页脚之前。
- **R-VOL-2** `[已固化·DR-40·DR-54·DR-61 修订]` 卡内按层级给出：输入（时长/实例数/采样率）→ 行数与**每秒新增行数**（公式：行数 ÷ 时长）→ **原始合计**（仅合计行：kvtest 为各进程目录 apparent size 合计〔glog 已含在目录内、不重复计〕；python 引擎为 writer + reader + glog 合计〔ylt 未采集不计〕）→ **单实例 map 后**（stats.json / csvstats.json / trace.json 均值，注明构成）→ **reduce 回传统计总量**（= `partial.json.gz` 一个文件；编排机只接收这一个文件、原始日志留节点）→ 输出（data.js / report.html）→ **留存合计**（仅合计行：进程目录含 `.glog.d`/summary/exitrc/remove 服务日志等全部现场文件 + profiling 目录；口径见 R-INST-1）→ **原始 vs 体系优化倍数**（仅 原始÷回传）。原始分文件行（writer.log / reader.log / glog / ylt）、每节点留存行与 原始÷报表 倍数已删除（DR-61 行数精简；data.js 台账字段不变）。
- **R-VOL-3** `[已固化·DR-40·DR-61 修订]` **glog 参与 map-reduce**（原始 glog 字节实测，产物 trace.json 实测）；**ylt（coro_rpc easylog）暂不处理**——不计入原始合计、**不得进入 map 回传或 reduce 统计**（卡内不再单列 ylt 行，未采集说明仅保留在卡顶小注）。
- **R-VOL-4** `[已固化·DR-40·DR-54 修订]` 数据以实测为准：原始=各进程目录 apparent size（kvtest，见 R-INST-1）/stats meta.bytes（python 引擎）、map 产物=文件大小、回传=partial.json.gz 实际大小、输出=实际写盘大小（data.js 精确、report.html 两遍渲染实测）；不再有 du 快照。

## 3. 硬约束（C）

- **C-1** `[已固化·DR-2]` report.html 单文件自包含：全部 CSS/JS（含 ECharts）内联，打开零网络依赖。data.js 是唯一允许的外部文件，且仅作可选增强（缺失可降级，见 R-SHELL-2）。
- **C-2** `[已固化]` 生成器（report.py）仅使用 Python 标准库，无第三方依赖。
- **C-3** `[已固化]` 内嵌数据体积有界：report.html 内嵌时序行数总预算 150000（EMBED_ROW_BUDGET），超预算对象留在 data.js；监控序列内嵌上限 2000 点。
- **C-4** `[推断]` 渲染性能预算：产物页面在桌面浏览器打开后 2 秒内可交互（长 run 内嵌降为分钟档以保证体积，见 C-3）。**待确认**：该指标是否作为可验收约束。
- **C-5** `[已固化·DR-21·v0.7 实现]` 文本对比度：亮/暗两套主题下，正文与标题类文本所用颜色相对纸底的 WCAG 对比度须 ≥ 4.5:1（节标题/表头等弱化文本复用既有更深 token 档位实现，不引入新 hex 色）。

---

## 4. 产物格式与兼容策略（P）

- **P-1** `[已确认·DR-5]` 报表只保证配套**最新产物格式**（与同版本生成器产出的 stats / trace / monitor 产物协同），不承诺打开历史旧格式产物；旧 run 建议用 `--rebuild-report` 重算产物。
  - 注记：现有代码存在部分旧格式降级路径（如 ts_epoch 缺失时回退字符串解析）。该类路径**不被本文档背书**，处置见 OQ-2。

---

## 5. 非目标（NG）

- **NG-1** 跨 run 对比（多次 run 同屏 / diff）。〔DR-8〕
- **NG-2** 导出 PNG / PDF。〔DR-8〕
- **NG-3** 移动端 / 触屏适配。〔DR-8〕
- **NG-4** 阶段曲线的节点/实例级分组（欠账，见 R-TRACE-9）。〔DR-4〕
- **NG-5** overview.html 演进为仪表盘。〔DR-9〕
- **NG-6** 界面国际化（保持中英混排现状）。

---

## 6. 开放问题（OQ）

登记时不阻塞起草，进入对应阶段前必须闭环：

| # | 问题 | 归属阶段 |
|---|---|---|
| OQ-1 | 主题手动切换的交互形态：三态循环（自动→亮→暗）还是双态？持久化载体在 file:// 下各浏览器行为差异如何兜底？ | 设计 |
| OQ-2 | 「不承诺兼容」后，存量旧格式降级代码路径（ts_epoch 回退、ftier=60 兼容、legacy trace 无 marker 注记等）保留现状还是移除？ | 实现 |
| OQ-3 | EMBED_ROW_BUDGET 的分配顺序（角色全嵌 → 节点按日志量 → Top 客户端）是需求级承诺还是设计自由度？ | 设计 |
| OQ-4 | ECharts 版本锁定策略与升级回归方式？ | 设计 |
| OQ-5 | 离群阈值系数（p99 × 10）是设计自由度还是需求承诺？ | 设计 |
| OQ-6 | C-4 性能预算（2 秒可交互）是否作为可验收约束？ | 需求确认 |
| OQ-7 | 间隔 map 的触发载体与间隔取值（benchmark 进程内定时器 / 旁路 tail / 采样脚本）？ | 数据管道 |
| OQ-8 | 慢请求阈值口径：静态配置 vs 窗口动态（如最近 p99 × k）；两条管道是否共用同一配置源？ | 数据管道 |
| OQ-9 | 「完整链路」的字段集与单条记录体量上限（span 树深度、字段裁剪、每窗口条数）？ | 数据管道 |
| OQ-10 | 异常定义（RPC 错误码 / 超时 / trace 解析失败）及两条管道判定的一致性？ | 数据管道 |
| OQ-11 | 同 run 跨单元对照的形态：可分享 URL 状态（档位/指标/缩放）/ 最小对照视图 / 仅页头单元身份（R-SHELL-11）？（NG-1 的「跨 run」边界维持不变） | 设计 |
| OQ-12 | 拖拽件（zslider/yscale/xpan/分级块聚焦）的键盘替代是否提供、交互语义与范围（C-5 与 R-SHELL-12 之外的完整键盘通路）？ | 设计 |
| OQ-13 | 慢点条带聚集秒叠合（pipeline 评审 P2-C，记录待改）：Top-100 集中在 44s 突发窗时一秒最多 42 条，bar 宽缩至 1px 互相覆盖（SSR 实测仅 42 条可见、15 对 x≤0.5px 叠合），图注与画面矛盾——聚集秒散布/每秒计数标注/同秒声明，取哪种形态？ | 设计 |
| OQ-14 | op 显示名双轨（pipeline 评审 P2-D，记录后续更改）：端到端侧 pipeline_write/pipeline_read 与 trace 侧 upsert_from/get_buffer 无映射注记；notify 流量不在 trace 覆盖内且无口径说明——reduce 层统一显示名还是前端加映射注记？ | 设计 |
| OQ-15 | 最慢请求表 start_time 与页内其他时刻双基准（pipeline 评审 P2-F）：ttop 表输出 UTC 原文（00:13:23）而异常列表/慢点为本地时刻（08:13:18），疑为时区处理缺失或跨节点时钟偏差——查根因后定 ttop 是否复用 ts_epoch 归一路径（rowEpoch 已有未走） | 实现 |

---

## 7. 决策记录（DR）

| # | 日期 | 决策 | 来源 |
|---|---|---|---|
| DR-1 | 2026-11-18 | 第一读者 = 工程师复盘：信息密度优先、图表默认展开 | Q1 |
| DR-2 | 2026-11-18 | 自包含单文件、零网络依赖为**硬性约束** | Q2 |
| DR-3 | 2026-11-18 | 多参数组口径 = 合并 + 标注 + 参数组筛选，定为终态 | Q3 |
| DR-4 | 2026-11-18 | 阶段曲线节点/实例分组列为欠账，不进本轮需求 | Q4 |
| DR-5 | 2026-11-18 | 不承诺历史产物兼容；旧 run 靠 `--rebuild-report` 重算 | Q5 |
| DR-6 | 2026-11-18 | 主题 = 自动跟随 + 手动切换（引入新增需求 R-SHELL-4） | Q6 |
| DR-7 | 2026-11-18 | 泄露阈值可配（leak_growth_pct / leak_min_mb），默认 30% / 200MB | Q7 |
| DR-8 | 2026-11-18 | 跨 run 对比、导出 PNG/PDF、移动端适配全部列为非目标 | Q8 |
| DR-9 | 2026-11-18 | overview.html 定位纯索引页 | Q9 |
| DR-10 | 2026-11-18 | 需求文档中文、位于 docs/spec/report/、随仓库版本化 | Q10 |
| DR-11 | 2026-11-18 | 登记数据管道愿景（间隔 map、异常/慢链路留痕、双管道同构）；初版仅 end-of-run 单次 map，本轮不动 map 阶段 | 用户愿景描述 |
| DR-12 | 2026-11-18 | 报表单元化：一个 data_size × 参数组 一份自包含报表（basic 目录 c{C}_t{T}_{size}，pipeline 按 size）；overview 每配置一卡 + 单元链接 | 用户决策 |
| DR-13 | 2026-11-18 | 段1 两行布局（吞吐+加粗平均/P99/P9999；第二行其余），QPS 移入信息展示并移除达成率表；参数配置去重；曲线改名 avg/max；QPS 副图图例固定、删小标题；阶段工具栏单行/阶段独立行 | 用户需求 |
| DR-14 | 2026-11-18 | 实例详情点击改为互斥展开（元信息+统计+独立小图），不再叠加主图曲线；曲线叠加仅由对象树控制 | 用户需求 |
| DR-15 | 2026-11-18 | basic 对象树 = 单行「总览」+ 节点与实例（pipeline 保持总体与角色） | 用户需求 |
| DR-16 | 2026-11-18 | 曲线全部灰阶（avg/p99/p9999 深灰、其余浅灰；多对象调色板去橙） | 用户需求 |
| DR-17 | 2026-11-18 | 阶段曲线分桶与端到端同策略（span/s ≥ 8 且 s ≥ ftier；不满足隐藏；自适应默认） | 用户需求 |
| DR-18 | 2026-11-18 | trace 增强：map 的 Top-N 慢请求保留 L1+L2；展开行多主要原因同时二级展开；新增慢请求阶段堆叠直方图；Top 上限 100；本地重算按 MC_PHASE 自校准时区 | 用户需求 |
| DR-19 | 2026-11-18 | 实例详情取消点击展开/小图（废弃 DR-14 的展开方案），仅保留一行关键信息表格；标题去掉括号注记 | 用户需求 |
| DR-20 | 2026-11-19 | 单元身份进页头与标签页：title/h1 携带 config · unit，生成时间只在副标题（评审 P1-1：三份单元报表标签页同名，meta 已有 unit 未使用） | impeccable 评审 |
| DR-21 | 2026-11-19 | 无障碍低成本组合：C-5 对比度 ≥4.5:1（复用既有深档，不引入新色）+ R-SHELL-12 图表 role/aria-label；拖拽件键盘替代登记 OQ-12，暂不作硬约束（评审 P1-2：--faint 亮底 1.97:1、零 aria/键盘事件；暗底 2.27:1） | impeccable 评审 |
| DR-22 | 2026-11-19 | 同 run 跨单元对照登记 OQ-11；NG-1「跨 run」边界维持（跨单元是 DR-12 单元化的核心收益） | impeccable 评审 |
| DR-23 | 2026-11-19 | R-MON-4 增部分缺失分支（URMA 无数据不留空占位、标题图注同步）；R-LAT-4 增离群开关无可命中桶时隐藏/置灰 | impeccable 评审 |
| DR-24 | 2026-11-19 | 新增 R-SHELL-13 标题层级；登记对齐性缺陷清单（既有规范已覆盖、修代码即可，不新增需求）：「阀值」错字×3、表头「队列等待均」截断（违 R-INST-3）、表列「平均/最大」vs 曲线 avg/max（违 report-design skill 文案规则6）、h3 交互括号注记（违 skill 规则1）、卡片无描边（违 skill 设计基调「1px 描边或底色区分」）、死 CSS（.gkv/.gtick/.tb-sep——另 .anml 实际在用，清单已复核订正）与单元化后不触发的 combo 筛选路径 | impeccable 评审 |
| DR-25 | 2026-11-19 | pipeline 模板首评 26/40（双代理，评审对象为 08:22 构建）。6 项优先发现处置：P1-A（QPS 分母，wall_s=600 vs span=354.3）判为异常 run 产物不入册——正常清空不会产生；P1-B（页头时间错位）与 P2-E（阶段曲线混合勾选静默丢对象）业务 agent 处理中；P2-C/P2-D/P2-F 登记 OQ-13/14/15；R-TRACE-6/latSel 文档-实现漂移暂不处理（当前页面未见阶段树）；writer/reader 线型例外正在改动、改动后重新观察 | impeccable 评审 |
| DR-26 | 2026-11-19 | 适配批次执行记录：① 核实 v4 提交（7ef5b21）**未包含** P1-B/P2-E（started 仍为 meta 构建时刻、objHasData 仍 some() 逻辑），两项维持待处理；② 批次落地 7 项 [新增] 实现（主题三态切换/单元身份 title+h1/9 图 role+aria-label/首卡 h2 消除跳级/--faint 对比度 .32→.72 达 6.13:1 与 8.31:1/离群 no-op 置灰既有实现补测/监控卡部分缺失分支），每项 TDD 先红后绿；③ A 类补测 19 项 + C 类 DR-24 修复并以 test_c01/c02 锁死；④ 顺手修复慢点 tooltip 读 r.t（不存在键，曾显示 undefined）回归 test_c02；⑤ 全量 340+ passed、双模板 SSR 8/8、两真实 run 报表已用新生成器重建 | 适配批次 |
| DR-27 | 2026-11-19 | UI 调整批次（用户逐条验收）：① 删除工具栏全选/清空，树「节点与实例」组改 全选节点/全选实例/全部清空；② 线宽 avg 1.0 / 其余 0.8，删除「多对象首对象加粗」；③ 单/多对象统一色语义（总览/总计灰阶、节点色系、实例色阶）；④ 端点标签簇聚焦；⑤ 全图 tooltip 点击触发；⑥ P1-B 页头时间取数据最早采样；⑦ P2-E 阶段树节点/实例禁用（R-TRACE-9 欠账）；新版 v0.8 | 用户需求 |
| DR-28 | 2026-11-19 | 交互增强批次（用户逐条验收）：① 全部清空自动开启总计；② 全选节点/实例改 toggle（全选后变清空）；③ 多对象右侧标签改对象名固定槽位（总计在上、树序平铺、无引线、截断+title、常显）并修「悬浮只亮一条」根因（改压暗聚焦）+ 图例禁点；④ tooltip 悬浮/点击分离（悬浮=竖线+交点，点击=钉住内容框，框内可滚动）；⑤ QPS 柱配色随对象；⑥ 多对象 tooltip 不再显示原始毫秒时间戳与无圆点行；新版 v0.9 | 用户需求 |
| DR-29 | 2026-11-19 | SDK 全链路重构批次：① 时延曲线选择增全选/清空；② tooltip 改为点击绘图区任意位置（含空白）按竖线时间点打开，分级时延曲线同构；③ SDK 对象树（总计+节点与实例，角色上移顶部总控），删指标概览卡；④ 分级时延块图（对象+指标单选）；⑤ 分级时延曲线三维联动（单对象可多阶段/单阶段可多对象，指标单选均值/最大）；⑥ 慢点追踪（按时延降序+阶段彩色+图例 solo+表格展开无 total）；⑦ 错误与异常按写读+节点/客户端；⑧ 实例与日志现场合并（含慢点贡献列，无 trace 隐藏）；⑨ 章节重排+页脚 map-reduce/正确性；⑩ Top-N 参数化（后续 1000）。v1.0 | 用户需求 |
| DR-30 | 2026-11-19 | 跨节点时间戳对齐设计入册（V-6）：相对秒桶键 + 节点 t0_epoch + 编排机 t0_ref 参考钟；实现另批（需多节点真实验证） | 用户提出 |
| DR-31 | 2026-11-19 | 数据量预估模型入册（V-7）：给定节点/场景参数预估日志、map、传输、reduce、data.js 六项规模；校准源=0910/0811 真实 run | 用户提出 |
| DR-32 | 2026-11-19 | SDK 分级时延曲线控件风格回归：曾引入「灰底+描边 tconsole 盒」，与端到端时延曲线（plain toolbar + objtree-strip）不一致；改为同构控件 + 无样式两栏 grid；新增控件风格统一规则与 CSS 锁测试（C20.V8/B6） | 用户验收 |
| DR-33 | 2026-11-19 | 标签与曲线错位 + 聚焦卡死修复：① 固定槽位（总计最上、树序均分）改为**锚定各曲线末端**（端到端多对象与分级曲线同构）；② 聚焦简化为**纯悬浮预览**（放弃点击锁定；根因：分级图 `finished` 每次 setOption 重渲标签，hover 元素销毁导致 mouseleave 丢失）；③ 单序列不出标签 | 用户验收 |
| DR-34 | 2026-11-19 | total 命名统一为 `total`（废弃 total（端到端）），阶段树根行/序列/标签/块图根行一致；桩环境 HTMLCollection 迭代掩盖问题：真实浏览器 `el.children` 无 forEach → 标签聚焦与弹层外点关闭抛错静默失效；统一 slice 包装并加静态审计测试 | 用户验收 |
| DR-35 | 2026-11-19 | 分级时延曲线五修复：① 嵌入抽稀 `ftier` 误在逐字段循环乘（k^字段数，600s→1024s）导致节点/实例曲线空白 → 每 op 只乘一次；② 维度联动改 radio/checkbox 随维度（多对象时阶段单选切换、多阶段时对象单选切换）；③ 允许空选（阶段/对象可全清，空态文案，不回填）；④ 树头单选/多选徽记；⑤ 总计/节点/实例统一称「对象」 | 用户验收 |
| DR-36 | 2026-11-19 | QPS 副图：① 多对象柱由完全重叠改为 **2px 细柱按槽位并排**（value 轴上 barGap 不生效，需显式 x 偏移）；② 失败柱与所属对象同槽位、保持统一橙（位置即归属，图例只列对象 QPS）；③ x 放大时柱画出绘图区的越界问题：移除 fail 的 `barGap:-100%` + 显式槽位 + `clip:true`；单对象样式不变 | 用户验收 |
| DR-37 | 2026-11-19 | 分级时延曲线取消「一边多选强制另一边单选」的联动：对象与阶段自由组合（N×M）；单对象=阶段调色板、多对象=对象色+阶段线型（实/虚/点）；序列名 `对象 · 阶段`；全选/清空不再收敛另一侧；对象为空不回退总计 | 用户决策 |
| DR-38 | 2026-11-19 | QPS 悬停/点击交互：同一时间点只用一根竖直虚线锁定（axisPointer line/dashed，柱不逐根强调）；点击详情框一次性列出该桶全部对象的 QPS+失败（`对象：QPS x · 失败 y`，带对象色圆点） | 用户验收 |
| DR-39 | 2026-11-19 | 标签悬浮两修复：① 长截断标签悬浮时用详情框展示全名（`data-full` + `.mc-label-tip`，替代原生 title）；② 聚焦残留（长标签向右移出后不恢复）：清除改为三重兜底（标签 mouseleave / 画布 mousemove / document mousemove + globalout） | 用户验收 |
| DR-40 | 2026-11-19 | 报表新增「数据量与优化」独立卡（V-7 落地基础版）：输入/行数速率 → 原始分文件（writer/reader/glog/ylt）→ 单实例 map 后（分桶+直方图+慢点+异常/离群+阶段桶）→ reduce 回传 → 输出大小 → 优化倍数；**glog 参与 map-reduce，ylt 暂不处理但计入原始数据量**（用户强调）；数值全部实测（meta.bytes/产物大小/写盘大小） | 用户需求 |
| DR-41 | 2026-11-19 | basic 段1：删除「信息展示」子区（标题+容器），总数/成功/失败/失败率/QPS 并入**性能统计第三行**；数据大小/客户端数/线程数/时长 保持在参数配置下；pipeline 分角色区块同构；清理 #infocards 死 CSS | 用户需求 |
| DR-42 | 2026-11-19 | 交互简化（用户授权拒绝高复杂度需求）：① 曲线 hover 闪烁根因=`emphasis.focus:'series'` 的 blur → 删除全部线序列 emphasis（强调仅由右侧标签触发）；② 点击钉住 tooltip 的自定义状态机导致「交点消失/虚线残留/左右关闭不对称」→ **整体拒绝**，回归原生悬浮 tooltip（showDelay 150ms + enterable；竖线/交点/框同生同灭）；③ QPS 用 formatter 兜底按轴值找桶，悬浮即列该桶全部对象 QPS+失败 | 用户需求 |
| DR-44 | 2026-11-20 | 离群/异常视觉与 tooltip 竞态修复：① 异常窗口竖线降为 25% 透明度 + 图注「红色竖线=异常窗口，非离群点」（原全高实色竖线误导为离群值位置）；② 离群点增加 0→点 旗杆虚线（上端止于离群点）；③ tooltip 去掉 `showDelay:150`（与 globalout 的显示/隐藏竞态导致移出后框残留，且残留框遮挡右侧标签使标签聚焦失效），globalout 主动 hideTip 兜底 | 用户验收 |
| DR-45 | 2026-11-20 | ① 异常窗口竖线透明度 25%→**15%**（仍不够淡，继续降权）；② 端到端**单对象多指标**标签聚焦失效——该路径仍用 `dispatchAction highlight`，依赖已被 DR-42 删除的 `emphasis.focus`，且未登记 `__mcLabelBox` 导致 document 兜底立即清聚焦；改为 `mcFocus` 压暗机制并与多对象/分级统一 | 用户验收 |
| DR-46 | 2026-11-20 | 节点身份统一为节点 IP（用户需求）：k8s 注册 pod 时取 `status.hostIP`；map `--node` 与监控标签改用节点 IP（ssh=管理 IP）；同主机多 pod 节点级合并（host 内存去重、进程/URMA 保留）；pod 名保留在 meta.pod/客户端名；重复 IP/缺 IP 回退名称；历史产物无法回溯（pod 已删） | 用户决策 |
| DR-47 | 2026-11-23 | **报表形态（scenario）与引擎（engine）解耦**：kvtest 与 python 并列，各有 basic/pipeline 形态；格式唯一依据 `meta.scenario`，`meta.engine` 仅标注/排查；旧 `mode="kvtest"` 归一化为 `scenario="pipeline" + engine="kvtest"`；kvtest 配置可用 `report_scenario` 声明形态（缺省 pipeline），生成链按「显式参数 > report_scenario > cfg.scenario(basic|pipeline) > kvtest→pipeline > basic」推断；副标题增「引擎」徽记；修复 kvtest 报表同节点 writer/reader 两行重复 | 用户需求 |
| DR-48 | 2026-11-25 | ① 错误与异常：节点级新增收放按钮（▸/▾），实例行默认折叠（节点汇总行仍显示实例数/总数/失败/聚合状态码）；② 「实例与日志现场」更名为「日志现场」，节点行默认折叠（实例默认不展开），展开状态仍跨重渲染记忆 | 用户需求 |
| DR-49 | 2026-11-25 | 黄区 k8s 监控三修：① kvtest 采样器注入编排器时钟基准 EPOCH_BASE（编排机与节点时钟差 5 分钟导致时间窗裁空、监控图整卡消失）；时间窗裁空回退全量+注记；python 引擎不改（stats 为节点钟域）；② profiling 产物目录改为节点 IP（原为 monitor pod 名），显示名冲突回退原名，monitor.config.json 保留；③ URMA slot 以 mdcli 报错的 available slot id 为准自动切换（配置仅提示）并记录 probe；④ 有产物无序列时渲染占位卡列出原因，不再静默隐藏 | 用户需求 |
| DR-50 | 2026-11-26 | 日志现场改版（用户需求）：唯一一张「现场日志」表（节点行聚合 + pod 行 + 每节点一个监控行；列=节点/角色/pod/日志属性/日志路径；展平不折叠、全表水平滑动、节点列仅节点行填写）；删除客户端实例树（总数/成功/失败/贡献列）、节点汇总、登录命令、其它产物索引与 data.js 降级提示；监控行未开启时三列为 `-`；新增结构性不变量（任何产物必须落在表内路径下，落不进的自成一行）；prof_core 在 monitor.config.json 记录监控 pod 与节点侧 profiling 目录；python 引擎经节点侧 remote_path 同步（无需额外 meta） | 用户需求 |
| DR-55 | 2026-11-27 | **错误与异常的状态分布按写/读路径落位、与 SDK 全链路选择器解耦（用户需求）**：原单条「状态分布」由「SDK 全链路」的对象选择控制，现改为写路径、读路径各一条横向分段条（全实例状态计数，取自 trace 的 write/read 组），段色=状态、图例=状态×次数；成功状态统一归 `ok`——C++ 读路径历史写 `status[read_ok]`，严格 `status=="ok"` 会把全部读请求误判失败，tracestats 现将 `read_ok` 归为成功并归档为 `ok`（生产端建议改回 `ok`）；R-TRACE-7 移除「状态分布统计」 | 用户需求 |
| DR-54 | 2026-11-26 | **kvtest 两阶段数据面 + 每进程目录（用户需求）**：map 移回 pod（每进程单遍，stats+csvstats+digest），controller 只做 stdlib partial reduce（尾部 digest 延后、instances 块、无 logsite/监控），编排机 finalize（合并 digest/注入监控/建 logsite/写 reduced）后 CSV+渲染；每进程目录 `<pod>/<role>_i<iid>_p<port>/`，meta.bytes 取目录 apparent size（单一口径）；**删除** samples/summary 传输、flatten、gzip 回退、`.log.glog` 回退、controller 失败回退、site_sizes/du 快照、CSV summary/samples 回退、digest 缺失回退；STOP 走同一 partial 流程（controller 不可达仅 WARN） | 用户需求 |
| DR-51（**被 DR-54 取代**） | 2026-11-26 | 现场日志大小改为节点侧 **du -sb 实测**（用户需求：不再依赖 map/pull 源头记录的已知产物字节，避免遗漏 .glog.d/summary/exitrc/remove 服务日志等运行时文件）：成功收尾时（profiling 落盘后、pod 销毁前）每宿主一次 du 快照（k8s=kubectl exec 进存活 client pod / ssh=远端直跑），pod 内路径换算为节点侧真实路径，写入 `logs/<cfg>/report/site_sizes.json`（report/ 内：成功清理保留、rebuild 可复用）；失败/STOP 不采集并回退旧口径；现场日志表 pod/监控/节点行与 V-7「数据量与优化」新增「留存（du 实测，场景级）」同源消费 | 用户需求 |
| DR-53 | 2026-11-26 | **受控近似分位口径（单一管线）**（用户需求）：引入 Apache DataSketches REQ sketch（尾部聚焦）作为尾部分位来源——count/avg/min/max/count 精确；**p50/p90/p95 = 合并直方图插值**（us-v1 桶，误差≈桶宽，实测 p50≈0.1%/p95≈1.4%）；**p99/p999/p9999 = 合并 REQ sketch**（k 可配 `quantiles.req_k`，默认 24；实测尾部 ≤0.6%）；**退役**精确原始样本池化与 map 侧 5M 精确窗口（map/reduce 内存均降为 O(1)，口径不随规模切换）；sketch 在 map 阶段按 op/产物生成（warmup 区不插入），reduce 按 scope 合并；库缺失时优雅降级为直方图并如实标注；报表顶部与 size 块标注 `pct_method`（hist+digest / hist）；controller 数据面在 reduced.json 增发 per-instance 正式统计块（instances），CSV 实例行与 TOTAL 行消费同源 helper，与报表严格同口径 | 用户需求 |
| DR-52 | 2026-11-26 | 端到端时延图新增**全局统计横线**（用户需求）：六条水平参考线 avg/p50/p95/p99/p999/p9999 取当前角色（pipeline writer/reader；basic 无 role_summaries 不画）的正式窗口 `role_summaries` 值，**图表生成时即确定、不随所选 pod/node/实例变化**；同指标同色 + 虚线 + 沉于分桶曲线下方、无端点标签；图例为「时延曲线选择」后的「全局横线」按钮组（默认仅 avg/p99/p9999 可见），tooltip 按可见性追加全局行；**仅端到端图绘制**——分级时延曲线不绘制（同构原则的显式例外） | 用户需求 |
| DR-57 | 2026-11-27 | **map 侧现场大小记账修正（用户需求）**：① `_dir_apparent_bytes` 改 `os.lstat`——glog_compat 的 `<prog>.INFO` 软链曾使 `os.stat` 把每段 glog 计两次（实测进程目录虚高 ~2×：5.66/6.15MB vs 真实 2.88/3.16MB）；② 报表按 `remote_path`（进程目录/文件）维度去重（writer/reader 的 `source` 同为 `latency_samples.csv` 会被错误折叠），嵌套在已计目录内的 glog trace 不再叠加字节；③ V-7「数据量与优化」恢复「留存（map 侧目录实测，场景级）」行（每节点 pods + profiling + 合计） | 用户需求 |
| DR-56 | 2026-11-27 | **端点解析同步 Mooncake 统一 `breakdown[op]` 体系**（用户需求；依据 `docs/yh/log-reference.md` §14）：`get_buffer` 主行由 `get_breakdown` 改为 `breakdown[get_buffer]`（旧行保留兼容），新增字段 `query_total_us/buf_alloc_us` 归一为报表树规范名 `query_us/alloc_us`；读接口补 `batch_get_into`、`prep_us`（batch L1）与 query 三段下钻（route/rpc/retry）；写接口补 `batch_put/batch_put_into` 与 query 三段下钻；`remove/batchRemove` 薄行归「其它」组；TENT+TCP 的诚实零四相位按 `engine[tent]` 解析（不误判 classic、残差吸收传输）；成功状态 `status[ok]`，`read_ok` 仅旧产物兼容 | 用户需求 |
| DR-59 | 2026-11-27 | **--rebuild-report 补齐 CSV 与组合 run（用户需求）**：① run 时把生效配置（继承解析结果+运行期键）持久化到 `logs/<cfg>/report/cfg.json`（`report/` 永不清理，随报表长期可重建）；② 重建按 cfg.json → 实例快照 `snapshot/files/**/<cfg>.json` → 仓库 `configs/` 同名配置的优先级发现配置，运行期键（实例数/warmup）从 `reduced.json` meta 回填，走与 run 时同一 `write_scenario_csv` 重算 `<cfg>.csv` 与 run 级 `benchmark_detail.csv`（无法重算的配置读回既有 CSV 合并、空结果不覆盖成功 run 的存量）；③ cluster/global 按 unit meta 记录的文件名在快照/仓库中发现，找不到仅拓扑列降级不报错；④ CSV 失败 = WARN + rc=1（报表仍重建），与 run 时「CSV 失败算 run 失败」同源；⑤ rank+recall 组合父目录递归重建每个子 run（子 run 补写 `overview_units.json`）并重渲染父总览（与 run 结束同一实现，`run_rank_recall` 转发）；⑥ WebUI 重建端点放行「子目录有 logs」的组合父目录（无子 logs 仍 400）；`parse_arguments` 对 `--rebuild-report` 跳过 cluster/global/profile 存在性校验（纯本地操作） | 用户需求 |
| DR-58 | 2026-11-27 | **通用控制台日志聚合规则（用户需求）**：多节点相似成功行在 console 折叠、benchmark.log 逐行全量保留、错误行绝不折叠——`utils` 新增 `log_agg(key,msg)/flush_agg(key,summary)/flush_agg_all()`（atexit 兜底、`_LOG_LOCK` 线程安全、`[WARN]/[ERROR]/[FAIL]` 前缀或 `error=True` 自动升级立即完整上屏、纯错误组 flush 不重复打印、verbose 原始流直上 console）；首批接入：prof_core collect 逐文件行（结构化汇总 `[profile] collected N files from M nodes: mem.*×K, urma.*×K, monitor.config.json×K`，组内计数不均退化为 `mem.*: mem.host.csv×a, …` 逐文件计数并附错误计数）、spdiag tree、工具 unavailable 跳过行、ssh 逐节点 cleanup 行；进度类行（pod ready/心跳）不聚合 | 用户需求 |
| DR-60 | 2026-11-27 | **节点监控三树控件改版（用户需求）**：① memchart 节点级=该节点各 NUMA used **求和实线**（与峰值 KV 卡同口径），可展开勾选 NUMA 分列（同系色阶）；「用量/可用量」指标开关（可用量=虚线，默认关）；② procchart 树=节点**仅分组不可选**（默认展开、节点名着节点色），进程行勾选（删除 slice(0,8) 截断，选择权在树）；③ urma 树=仅节点勾选；④ 三图统一加固定图例（selectedMode:false，显隐只走树勾选）、h3 标题（宿主内存/进程 RSS/URMA 端口流量监控，URMA 文字在图之前）；⑤ 部分缺失改三元组整组隐藏（标题+树+图+空态）；配色=主图节点色系/同系色阶同源（`monNodeHue/monShade` 与 `nodeHue/instanceColor` 同算法），空选允许显示 empty-tip 不回填 | 用户需求 |
| DR-61 | 2026-11-27 | **V-7「数据量与优化」卡行精简（用户需求）**：① 「原始」删分文件行（writer.log / reader.log / glog / ylt），仅保留合计行，口径文字按引擎分支（kvtest=各进程目录 apparent size 合计；python=writer + reader + glog 合计），`volume.raw` 新增 `dir_mode` 供渲染分支，`render_scenario` 对旧产物（DR-61 前的 partial/reduced 台账无该标志）按块内 clients 的 `size_mode` 回填（`--rebuild-report` 重建旧 run 不落错分支）；② 「留存」删每节点行，仅保留合计行；③ 「对比」仅保留 原始→回传（删 原始→报表 行与 data.js+report.html 求和口径）；④ 回传与留存合计口径文字按用户订正改写（回传=partial.json.gz 编排机只接收这一个文件、原始日志留节点；留存合计=进程目录全部现场文件 + profiling 目录）；data.js 台账字段（分文件/每节点）保持完整不变 | 用户需求 |
| DR-63 | 2026-11-28 | **节点监控摘要行精简 + 三树控件统一模板（用户需求）**：① 删除监控卡三处摘要 KV 行——逐节点「峰值内存」、`RSS 最大增幅`（含疑似泄露 tag）、「端口峰值带宽」；进程 RSS 增幅正负条保留，data.js `summary` 与 overview 摘要行不变；② 宿主内存/进程 RSS/URMA 三棵选择树统一走 `renderMonTree` + `treeNodeGroup`/`treeRow` 模板（与主图/慢点捕捉同一套 `.tree-group`/`.node-group-head`/`.tree-list`/`.tree-row`/`button.mini`），删除监控私有行/分组头实现；③ 渲染入口先清空容器，修复勾选/caret/主题/size 重渲染 append-only 导致的重复树（表现为折叠失效、勾选后 UI 重影） | 用户需求 |
| DR-64 | 2026-11-28 | **节点监控控件与端到端曲线严格同构 + 用量可选（用户需求）**：① 宿主内存「用量」由常开改为可选指标（`monMemUsed`，默认开；与「可用量」同一开关风格），全关时 mem 图显示指标空态提示；② 三棵监控树顶栏统一为端到端曲线的三键语义——全选节点 / 全选实例 / 全部清空（已全选则按钮文字变「清空*」）；树缺少的维度（进程树节点仅分组、URMA 无实例）按钮保留但置灰并附 tooltip，三棵树控件形态不分叉；③ mem/proc 每个节点行跟随该节点子项（NUMA / 进程）的「全选 / 清空」mini 按钮，只作用于本节点；④ 抽出 `_monTopBtns` 统一生成三键，渲染仍走 `renderMonTree` 模板 | 用户需求 |
| DR-65 | 2026-11-28 | **URMA 端口流量三级列表 + 固定平均流量表 + 监控统一相对时间（用户需求）**：① URMA 树由「仅节点」扩为 节点 → DIE（=现有 `ubpu` 字段，UI 显示 DIE）→ 端口 三级均可勾选，采用标准控件逻辑（父子不联动）：勾节点/ DIE 得该层全部端口的 RX/TX 求和曲线，勾端口得单端口曲线；节点行 mini 只操作直接子项 DIE，DIE 行 mini 只操作其端口；顶栏三键=全选节点 / 全选实例（端口叶子）/ 全部清空（允许空图并显示 empty-tip）；② URMA 图下新增固定表（节点 / DIE / 端口 / RX 平均 / TX 平均），**不随树勾选过滤**，平均值=warmup 结束→任务结束窗口内逐采样点算术平均（`rx_avg_formal`/`tx_avg_formal`，无 warmup 回退任务开始）；③ 监控三图时间基准统一为「任务开始 t0 = 0」的相对秒：`reduce_monitors` 把 mem/urma 的 `t` 重定为 `采样时刻 - t0` 并携带 `time_relative/time_origin`，x 轴刻度 `T+mm:ss`、tooltip 同时显示相对时间与墙钟；三图共用 `[0, time_span]` 任务窗口（`time_span` = 任务结束 − t0），任一图首个采样偏晚也从 T+0 起算并与其它图终点对齐；④ `_parse_urma` 改为**先差分再按窗口裁剪**并保留窗口前一条样本，使窗口内首个速率点锚在首个采样拍、不再晚一拍，避免单元窗口只剩单样本时整条曲线消失；`_stats_window` 返回 `warmup_cutoff` 并传入 `reduce_monitors` | 用户需求 |
| DR-62 | 2026-11-28 | **慢点捕捉「节点与实例」控件 UI 与报告其他位置统一（用户需求）**：外层改为 `.objtree-strip`（JS 内建 `.objtree-body`），内部复用标准对象树组件——`.tree-group`（角色/总计）、`.node-group-head`（标题 + 已选 N 项 + `button.mini` 全选节点/全选实例/全部清空）、`.tree-list` 的 `.tree-row`（`.tree-caret` 展开实例、`.tree-check`/`.tree-box` 勾选、节点行带该节点实例的全选/清空与「N 实例」note）；删除无 CSS 定义的自定义 `slow-chip-btn` 与 `.slow-ctl` 专属容器样式；独立选择/叠加语义/摘要条/图表逻辑不变 | 用户需求 |
| DR-66 | 2026-11-28 | **trace 阶段树重构为 per-op JSON 单一事实源（用户需求；修复读/写路径解析的系统性错误）**：六根因——读树无传输段（classic `memread_*`/TENT 四相位缺失）、`batch_put` 误按 put 族并集解析（残差 = total）、树按 group 共享字段错并集、树定义 Python/JS 双份漂移（TREAD/TWRITE）、字段归一引入第三套名（`query_us`/`alloc_us`）、`read_us` 双分解未表达；重构为每 op 一份 `configs/_trace_trees/<op>.json`（13 棵，schema `mp-trace-tree/1`，`scripts/trace_trees.py` 唯一入口，目录缺失/校验失败硬失败）；字段名 = 日志原生名（legacy 方向翻转：旧 `get_breakdown` 行的 `query_us`/`alloc_us` 单向别名到 `query_total_us`/`buf_alloc_us`）；新增 views（`read_us` 的 classic/tent/A-B 并列分解，L2 子行带徽标）与 switch（put 族传输段按行内 engine 切 L1 形状，case 名作 view）；report 页面嵌 `TRACE_TREES`、删 TREAD/TWRITE，渲染 JS 零 op 知识；`batch_put`/`batch_put_into`/`batch_upsert_from` 薄行 = prep/write 两段；`get` 非独立树（legacy_ops 归一到 get_buffer）；未知 op 归「其它」组并计 `meta.unknown_ops` | 用户需求 |
| DR-67 | 2026-11-28 | **map/reduce 侧树下发补齐（E2E 事故 20261128-1331/1355）**：tracestats 在 pod 内、kvtest_partial_reduce 在 controller 上 `import trace_trees`（report.py → trace_trees），但两处远端脚本清单都未包含树 → pod 内 map 全挂（`no trace product`）→ 修后收集阶段又挂（partial reduce `ModuleNotFoundError`）→ 场景 FAIL。修复：benchmark 新增 `_trace_tree_payload`（`trace_trees.py` + `configs/_trace_trees/*.json` 随脚本**平铺**下发——configmap 键不允许子目录），三处下发点统一接入（k8s pod configmap / ssh `deploy_scripts` / controller partial reduce `_controller_reduce_stage`）；`trace_trees._resolve_tree_dir` 支持平铺布局（`scripts/` 本身，以 `_schema.json` 作标记，源仓库布局优先，两个候选都缺 → load 硬失败不静默）。单测无法覆盖此路径（源仓库本地 `configs/` 天然存在），E2E 逐处抓出 | E2E 事故 |
| DR-68 | 2026-11-28 | **监控三图统一图表框架 + RSS/URMA 交互修正（用户需求）**：① 抽出 `lfChartFrame` 统一图表模板（后续任何图必须走它）：图与上方控件间距统一、图例左右边界=grid（不越过 y 轴）、`grid.top` 保证图例不被截断、tooltip 时间一律人类可读（`T+mm:ss` + 墙钟）、x 底部 slider+inside、y 右侧竖向 slider+inside（全部 ECharts 原生）；② 进程 RSS：RSS 增长 TOP-10 移到图下方并加标题；进程节点行去掉专属节点色/加粗（仍无 checkbox，节点级聚合语义有歧义）；默认只勾选 `mem.proc_pattern` 前缀（comm 截断 15 字符，缺省 `mooncake_kvtest`，无命中回退全选）；删除「未勾选进程：…」空态文案；③ URMA：速率点按 `urma.interval_s`（默认 1s）量化到相对 t0 的时间桶并同桶平均，修复纳秒时间戳抖动导致“每条竖线只命中一个节点”；compact payload 透传 `config` 供前端取默认进程前缀 | 用户需求 |
| DR-69 | 2026-11-28 | **overview 卡片指标行加强（用户需求）**：每配置卡在 成功/失败/失败率 之上新增指标行 **吞吐 / avg / p99 / p9999**；pipeline 形态按角色拆两行（`写路径（writer）` / `读路径（reader）`，各自取该角色 `role_summaries` 跨单元聚合；无角色摘要的旧产物回落合并口径），basic 单行合并口径；聚合口径 = 计数/字节求和、avg 按成功数加权、吞吐 = 字节合计 ÷ 时长合计、p99/p9999 取最差单元，单元数 >1 时卡内注明；计数行降为次要字重（18px/500），指标行保持强调字重 | 用户需求 |

---

## 8. 数据管道愿景（VISION · 后续阶段 · 尚未实现）

> 本节记录 2026-11-18 与用户确认的数据采集/处理方向。**不属于当前实现**；
> 进入实现阶段前须拆解为可测需求（R-*）并补设计文档（DESIGN.md）。
> 背景：大集群、高流量、大量日志。

### 8.1 愿景条目

- **V-1** `[新增·未实现]` 常态聚合：测试运行期间数据持续产生（内存或写日志），每节点/容器按固定时间间隔触发一次本地 map（聚合 + 落盘）；正常流量只保留有界聚合产物，从源头避免巨量无效原始日志。
- **V-2** `[新增·未实现]` 异常留痕：当某次请求链路出现异常时，须记录该请求的完整链路。
- **V-3** `[新增·未实现]` 慢请求留痕：当某次请求时延超过阈值（预先配置，或按窗口动态计算）时，须记录该请求的完整链路。
- **V-4** `[新增·未实现]` 双管道同构：RESULT（测试框架打印）与 trace（Mooncake 打印）日志采用同一套处理逻辑（同构的聚合、阈值与留痕策略），但作为两处独立实现、在各自进程中运行。
- **V-5** `[新增·未实现]` 初版形态：每节点在测试完全结束后触发一次 map（end-of-run 单次），作为上述流程的最简实现。
- **V-6** `[新增·未实现·DR-30]` **跨节点时间戳对齐**：512 节点规模下节点时钟不可能对齐，纯绝对时间戳分桶会错位。方案：各节点 map 产物记录自身起始钟面 `meta.t0_epoch`，桶键改为**相对秒**（0..N）；编排机下发时记录**每节点启动时刻（编排机钟）`t0_ref[node]`**，渲染 x = `t0_ref + rel`（统一参考钟，节点钟偏差彻底消除）；现场追溯绝对时间 = 该节点 `t0_epoch + rel`。旧产物无字段则回退绝对秒；tracestats 的 MC_PHASE 校准降为兜底。
- **V-7** `[新增·未实现·DR-31]` **数据量预估模型**（给定节点数/场景参数提前估算）：输入 = 节点数 N、每节点客户端数 C、QPS（写 Q、notify R）、时长 D、数据大小、trace 采样率 ρ、阶段字段数 F、直方图桶数 B。输出六项 = 原始日志规模 / map 开销 / map 后传输规模 / reduce 接收规模 / reduce 后规模 / data.js 规模。模型骨架：请求量 ≈ (Q + Q·R)·D；RESULT 行 ≈ 量×~120B；trace 行 ≈ ρ·量×F 字段；map 有界状态 ≈ 桶数(≤8192)×(1+F)×组数 + B 分箱 + 3×清单上限；reduce 输出 ≈ 档位数×对象数×行宽。校准：用 0910/0811 两个真实 run 的实测值（日志、stats/trace 产物、data.js 字节数）拟合常数，再推演 512 节点。

### 8.2 与现状的对照

| 愿景 | 现状 | 缺口 |
|---|---|---|
| V-5 end-of-run 单次 map | 已具备：logstats.py / tracestats.py 均在 run 结束后于节点上执行一次 | — |
| V-1 间隔增量 map | 未实现：只有结尾一次 | 定时 / tail 触发；内存有界聚合（直方图天然有界）；增量落盘；可驱动实时进度 |
| V-2 异常完整链路 | 部分：errors 计数 + 截断的错误摘要（error_excerpt） | 无完整链路留痕 |
| V-3 慢请求完整链路 | 部分：logstats 保留 top-100 慢点（固定条数，非阈值制） | 阈值触发 + 完整链路（trace span 树） |
| V-4 双管道同构 | 形式具备（两个独立脚本），策略未对齐 | 共享的阈值 / 留痕策略抽象 |

### 8.3 已识别约束（实现阶段须遵守）

- **V-C1** 粒度下限：现有 map 产物（1 s 行 + 直方图）是报表可见的最细粒度；「完整链路」须在 trace 管道内独立判阈值（trace 行带 `request_id`，RESULT 行无），与 V-4 的「分别处理」一致。
- **V-C2** 留痕有界：异常 / 慢链路记录必须有条数上限（如每窗口 N 条 + 计数），防止网络抖动期记录量雪崩。
- **V-C3** 落盘分离：留痕记录与聚合行分开存放（如 `*.slow.json` / `*.anomaly.json`），由报表侧新增卡片消费。
- **V-C4** 本轮边界：不改动 map 阶段（logstats / tracestats），仅登记愿景。
