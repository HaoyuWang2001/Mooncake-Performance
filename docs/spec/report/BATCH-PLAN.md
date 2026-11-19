# 批处理计划 · 适配与测试补齐（等待 HTML 修复完成后统一执行）

> **状态：已完成（2026-11-19 14:2x）** · 落盘：2026-11-19 12:19
> ①～⑤ 全部执行完毕：340+ passed、双模板 SSR 8/8、截图×4、真实 run 重建；
> R-SHELL-4/11/12/13、C-5、R-LAT-4/R-MON-4 分支实现转绿；DR-24 锁死（test_c01/c02）。
> 遗留（非本批）：P1-B/P2-E 经核实未含于 v4，待用户裁决归属（见 DR-26）。
> 执行人：lead（用户解冻后开工）。执行前先重读本文件与 REQUIREMENTS.md 最新版——
> 业务 agent 的修复可能已改变下述「未实现」状态，逐项核对后再动手，不盲目按旧清单做。

---

## 目标

在 HTML 修复完成后，统一执行三批工作，使「文档中总结的每一条规则都落为测试用例」成立：

1. **A 类补齐**：11 条「已实现、无测试」的规则补测试（见测试审计）。
2. **B 类 TDD**：v0.5/v0.6 登记的 `[新增]` 需求逐条红→绿实现（代码适配）。
3. **C 类防回归**：DR-24 对齐性缺陷修复 + 断言。
4. **重新观察项**：随修复一并复核的评审遗留。

## 完成条件（验收标准）

- `env -u http_proxy -u HTTPS_PROXY python3 -m pytest webui/tests -q` 全绿（排除既有环境性失败：etcd 系，15 个）
- `python3 -c "import report; …"` 提取 `_APP_JS` → `node --check` 通过
- 真实 ECharts SSR：basic + pipeline 两模板 `--real` 全绿（`real.ok == real.total`）
- Edge headless 截图 1000px/1400px 两宽度人工核对（skill 验证流程第 4 步）
- A 类每条有测试背书；B 类每条走「先写失败测试 → 实现 → 转绿」；需求与实现无漂移（REQUIREMENTS 前言规则）
- 修完重跑 `/impeccable critique` 双模板看分数变化（趋势 29→26 → ?）

## 已完成并验证（截至落盘时点）

- REQUIREMENTS.md **v0.6**：65 条实体（R-*/C-*/P-*/NG-*/OQ-*/DR-*/VISION），两轮评审 5+6 项发现已按用户裁决入册/留档
- 两轮双代理 critique 快照：`.impeccable/critique/2026-11-19T01-13-21Z__report-html.md`（basic 29/40）、`…03-01-32Z…`（pipeline 26/40）
- 测试审计完成：115 个测试四层清单、无真冗余、覆盖矩阵 38✓/10◐/7✗
- 代码冻结良好：本会话（lead）未改任何源码/测试；本轮仅写文档

## 决策与教训（用户裁决，执行时不得翻案）

- **P1-A（QPS 分母）不入册**：判为异常 run 产物（正常清空不会产生）；业务 agent 已在 map 侧源头修复（`test_pipeline_summary.py::measured_wall_prefers_notify_span`），比报表层护栏更对症——B 类不再做 render 层护栏，避免双重口径
- **P1-B（页头时间）/P2-E（静默丢对象）**：业务 agent 处理中，恢复时验证其修复 + 补测试，不重复实现
- **OQ-13/14/15**：P2-C 慢点叠合、P2-D op 双名、P2-F ttop 时区——已登记待后续（设计/实现阶段定形态），**不属本批**
- **暂缓重观察**：R-TRACE-6/latSel 漂移、writer/reader 线型例外——业务 agent 改动后重看，勿在旧构建上下结论
- `.anml` 在用（DR-24 死 CSS 清单已订正）；`test_report.py:1102` docstring「阀值」错字随 C 类一并修
- latSel 含 avg 超出 R-LAT-3——教训：指标清单无锁定测试才会漏；A 类 #3 即补此洞
- 全程纪律：未经用户批准不动文件；approval 提示已开启，需要时走审批

## 下一步（执行序，恢复时从 ① 开始）

**前置**：`git status` + mtimes 确认业务 agent 已停手；重读 REQUIREMENTS.md（可能已 v0.7+）；跑全量 pytest 取新基线。

① **核对修复**：业务 agent 改动逐项对照（P1-B 页头时间、P2-E 静默丢对象、其余可见变化），确认各自有测试背书；无测试的补。
② **A 类 11 条**（有码无测，纯补测试，不动产品码）：
   1. R-LAT-9 gap 标注——构造 30→46s 跳变，断言 markArea「16 秒无数据」（reduce 层 + JS 冒烟各一）
   2. R-LAT-12 色彩语义——把评审 SVG 色审计改造成 pytest：SSR 解析 mainchart series 颜色∈两灰档、slowchart 仅最慢橙、qpschart 橙仅失败系列（skill §1 落死）
   3. R-LAT-3 指标清单锁定——断言单对象线集合与 METRICS（终结 latSel 超纲类问题）
   4. R-LAT-10 anomalies 50 上限——构造 60 条断言画 50
   5. R-SHELL-2 降级路径——JS 冒烟置 `__mcDataMissing` → 表 ≤20 行 + datanote 显示
   6. R-SHELL-5 副标题序列——started/生成时间/协议/早停警告
   7. R-HIST-3 + R-SLOW-3 空数据整卡隐藏
   8. R-LAT-6 主副图缩放联动——harness 已有 VC_ZOOM 钩子，dispatch 后断言两图同步（零新基建）
   9. R-SHELL-8 safe() 隔离——注入单块抛错，其余块完好
   10. C-2 纯标准库——AST 扫 report.py 禁第三方 import
   11. R-SHELL-3/6/10 CSS 存在性——双主题 @media、sizeSel 显隐、tabular-nums/fmtUs
③ **B 类 TDD**（先红后绿，每条独立提交粒度）：R-SHELL-4（主题手动切换）· R-SHELL-11（单元身份进 title/h1，注意 pipeline 侧更致命）· R-SHELL-12（图表 role/aria-label）· R-SHELL-13（标题层级）· C-5（对比度 ≥4.5:1，复用 --lab 档）· R-LAT-4 no-op 分支 · R-MON-4 部分缺失分支——逐条先写失败测试，再实现，再转绿
④ **C 类**：DR-24 清单修复 + 断言（`assert "阀值" not in src`、死 CSS not in、表头「队列等待均值」、avg/max 命名、括号注记、卡片 1px 描边）；`test_report.py:1102` docstring 同步
⑤ **全量回归 + 验收**（完成条件四件套）+ 重跑双模板 critique 记趋势 + 更新 REQUIREMENTS 状态标记（`[新增]`→`[已固化]`）

**风险**：业务 agent 修复可能与 B 类重叠（如页头时间即 R-SHELL-5 相关）——执行前逐项核对现状，重叠的跳过实现只补测试。

---

## 后续批次 · UI 调整收口（2026-11-19，已完成）

按用户逐条验收的 6 项 UI 调整 + 上批遗留 P1-B/P2-E 已全部落地（见 DR-27 / REQUIREMENTS v0.8）：

| 项 | 内容 | 测试 |
|---|---|---|
| A1 | 删除工具栏 `selAllBtn/selClearBtn` | test_multi_object…（断言不存在） |
| A2 | 树「节点与实例」组 → 全选节点 / 全选实例 / 全部清空 | test_tree_group_select_buttons |
| A3 | 线宽 avg 1.0 / 其余 0.8、强调 +0.2、删首对象 ×1.2 | test_line_widths_avg_thicker_only |
| A4 | 单/多对象统一色语义（总览/总计灰、节点色系、实例色阶） | test_a03 + S4 色阶测试 |
| A5 | 端点标签簇聚焦（主图 + 阶段图） | test_endlabel_cluster_focus |
| A6 | 全图 tooltip 点击触发（bindClickTip） | test_all_charts_tooltip_click_trigger |
| B | P1-B 页头开始时间 = 数据最早采样 | test_report_started_time_uses_data_timestamp / test_rebuild_reports_local |
| C | P2-E 阶段树节点/实例禁用 + 说明 | test_stage_tree_nodes_disabled |

验证：388 passed；双模板 SSR 8/8；0910 / 0811 / demo 三份报表已重建。

---

## 后续批次 · 交互增强收口（2026-11-19，已完成，见 DR-28 / v0.9）

| 项 | 内容 | 测试 |
|---|---|---|
| 1 | 全部清空 → 自动开启总计（主树 + 慢点控件） | test_clear_all_auto_enables_total |
| 2 | 全选节点/实例 → 全选后变「清空节点/清空实例」 | test_tree_toggle_buttons / slow 断言 |
| 3 | 多对象固定槽位标签（顺序/无引线/截断+title/常显）+ 压暗聚焦 + 图例禁点 | test_endlabel_fixed_slots_and_cluster_focus / test_multilabel_tree_order_and_truncation |
| 4 | tooltip 悬浮/点击分离（悬浮=竖线+交点，点击=钉框） | test_all_charts_tooltip_hover_pointer_click_box / test_tooltip_click_pins_content |
| 5 | QPS 柱配色随对象（失败保持橙） | test_qps_bars_follow_object_colors |

验证：393 passed；Edge/CDP 实浏览器核对（标签顺序与截断、悬浮压暗、悬浮指针、点击钉框与格式化 tooltip）。

---

## 后续批次 · SDK 全链路重构与尾部章节（2026-11-19，已完成，见 DR-29 / v1.0）

| 项 | 内容 | 测试 |
|---|---|---|
| P0 | 时延曲线选择 全选/清空；tooltip 点击绘图区任意位置（含空白）打开（分级时延同构） | test_latency_metric_select_all_clear / test_tooltip_click_anywhere_opens |
| P1a | trace 按对象（总计/节点/实例）保留阶段桶 + 嵌入预算抽稀 | test_trace_per_object_series / test_trace_embed_trim_budget_and_coarsen |
| P1b | SDK 对象树/分级时延块图/分级时延曲线三维联动/删除指标概览卡 | test_stage_tree_objects_enabled / test_trace_curve_dimension_coupling |
| P1c | 慢点追踪（降序+阶段彩色+图例 solo+无 total 展开） | test_slow_tracking_ranked_colors_and_legend_solo |
| P2 | 错误按写读/节点客户端；实例与日志现场合并；章节重排+页脚；监控卡前移 | test_errors_split_by_role_and_client / test_client_site_* / test_section_order_and_footer_note |
| P3 | 设计入册：V-6 时间戳对齐、V-7 数据量预估模型 | 文档 |

验证：400 passed；沙箱重建 + SSR 并发冒烟 + Edge/CDP 截图。

---

## 组件规则册批次（2026-11-19，已完成）

- 规则册写入 `report-design` skill：C0–C20 共 207 条（V/I/D/B 四类，覆盖 UI 展示/交互/数据口径/历史 bug 根因），每条带需求 ID 与测试追溯或显式状态（◐/✗/n/a）。
- 覆盖率（自动校验）：✓178 · ◐27 · ✗1（C17.V4 日志列压缩，展示类留后续） · n/a1。
- 新增 `test_skill_rulebook_integrity`：ID 唯一/格式、引用测试存在、覆盖率汇总与实际一致 → 防测试改名失联。
- 补齐交互类 ✗/◐：重置视图、阶段行全选/清空、分级时延曲线固定标签+压暗、combo 隐藏实例回落（+4 测试）。
- 恢复被误删的 3 条历史测试（toggle/清空自动总计/QPS 配色）；全量 408 passed。

