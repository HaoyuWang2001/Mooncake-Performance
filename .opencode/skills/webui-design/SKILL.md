---
name: webui-design
description: Use when editing, reviewing, or adding any UI in this repo's WebUI (webui/web/**, plain Vue3 + Element Plus, no build step). Covers design tokens, Element Plus theming, dark mode, danger/status color semantics, typography, spacing, empty states, copy, button conventions, and accessibility rules distilled from anthropics/frontend-design, Vercel Web Interface Guidelines, emilkowalski/design-eng and the Services-Deployment webui-design skill.
---

# WebUI 设计规范（本仓库）

管理台（Operate 模式）：可扫读性、一致性、操作精度优先于表现欲。品牌感只出现在精确的细节里，不靠装饰。

来源：`anthropics/skills` frontend-design · `vercel-labs/web-interface-guidelines` · `emilkowalski/skills` design-eng，
以及 `/home/haoyu/Services-Deployment` 的 `.opencode/skills/webui-design`（配色、按钮形态、token 体系与本仓库对齐）。
冲突时以本文件的本项目规则为准。

## 本仓库技术约束（与 Services-Deployment 的差异）

- **无构建**：`webui/web/js/**` 是普通脚本 + 字符串模板（非 SFC），无 vite/npm。所有改动必须是 HTML/CSS/JS。
- **vendored**：Vue3 / Element Plus / icons 均已入库（`webui/web/vendor/`），离线可用，禁止引入外部字体或 CDN 资源。
- **EP 样式是运行时注入**（脚本执行时 `<style>` 追加到 head），静态 style.css 可能早于它。主色等变量覆盖一律用
  高特异性选择器：亮色 `:root:root { … }`、暗色 `:root:root.dark { … }`（0,2,0 / 0,3,0 恒胜 EP 的 `:root`(0,1,0) 与
  `html.dark`(0,1,1)），**不要**用 JS 分支做主题适配。
- 视图：`perf-view`（创建实例+实例卡片）、`instance-view`（实例详情/日志）、`results-view`、`config-view`、
  `schedule-view`、`spdiag-tree`；全局壳 `app.js`。

## 硬性规则

1. **颜色只用 token**：优先 `var(--el-*)`，其次 `--mc-*`。禁止新增 hex 字面量（terminal 色板也必须走 `--mc-term-*`）。
2. **亮/暗双主题必须都成立**：用 `html.dark` 作用域适配，**禁止**用 `:plain="isDark"` 之类的 JS/Prop 分支做主题适配。
3. **危险色语义**：`type="danger"` 只用于破坏性操作本身及其确认按钮（删除/清理）。断开、停止、关闭等非破坏操作不得用 danger。
4. **每页至多一个 primary 实心按钮**（页面主动作）。批量/次级操作一律 plain，不给颜色堆叠。
5. **状态色=含义**：成功/警告/危险/进行中保持 Element Plus 语义色，不做品牌化改色；状态文案统一走
   `webui/web/js/status.js` 的 `statusText/statusType/confirmDanger`（中文，运行中/停止中/已完成/失败/已停止/未运行）。
6. **表格统一 `size="small"`**；数字列（PID/耗时/rc/吞吐/计数）加 `.mc-num`（`font-variant-numeric: tabular-nums`）。
7. **空状态统一 `el-empty`**，必须给出下一步操作（按钮/链接）；禁止把长教程塞进表格 `empty-text`。
8. **无外部字体**（离线局域网）：只用 `--mc-font-ui` / `--mc-font-mono` 本地字体栈。
9. **中文文案**：全角标点（，。：；「」（））；省略号用 `…`；按钮用动词且名称与结果一致（保存→已保存）；错误信息包含下一步。
10. **动效克制**：交互反馈 <300ms、ease-out；高频操作（菜单切换、表格刷新）零动效；`@media (prefers-reduced-motion: reduce)` 关闭位移类动画；禁止 `transition: all`。
11. **无障碍**：仅图标按钮必须有 `aria-label`；可点击卡片/行必须可键盘聚焦（`tabindex="0"` + Enter/Space）；表单必须有 label 或 `aria-label`。
12. **危险操作必须有确认**（`confirmDanger()`，确认按钮 danger）；批量操作确认框要列出目标。
13. **按钮形态**（对齐 Services-Deployment）：工具栏按钮 `size="small"`；行内操作 `link type="primary"`；
    刷新等图标操作 `circle :icon` + tooltip + `aria-label`；破坏性 `danger plain`；页面主动作唯一 primary 实心。
14. **disabled 必须解释**：禁用按钮外包 tooltip（或 `title`）说明原因与如何满足，而不是只灰掉。

## Design tokens（webui/web/style.css）

| Token | 用途 |
|---|---|
| `--mc-space-1..6` | 4/8/12/16/20/24px，所有间距只用这几档 |
| `--mc-font-ui` / `--mc-font-mono` | 本地字体栈 |
| `--mc-radius` | 面板圆角（4px，与 EP 一致） |
| `--mc-term-bg/-fg/-muted/-error/-hit-bg/-hit-fg/-cur-bg` | 终端配色（亮/暗两套，恒深底保证日志可读） |
| `--el-color-primary*` | 深青技术主色（亮 `#0e7490` / 暗 `#27b3c9`，派生档同在 style.css 覆盖） |

字号层级：12 辅助 / 13 表单标签与说明 / 14 正文基准 / 16 页面标题（600）。不要引入 12.5 / 13.5 / 15px。

## 结构约定

- 页面标题行统一 `PageHeader` 形态：左侧 h1（16px/600）+ 说明（12px muted），右侧动作区（`.mc-page-head`）。
- 终端类视图（日志/实例详情）统一使用全局 `.mc-term` 样式，禁止复制终端配色/字体。
- 时间显示：相对时间（`fmtTimeRel`，`webui/web/js/status.js`）优先，悬停 `title` 给出绝对时间。
- 危险确认：`status.js` 的 `confirmDanger()`。
- 工具类：`.mc-mono` / `.mc-num` / `.mc-muted` / `.mc-label` / `.mc-toolbar` / `.mc-page`。

## 自查清单（改完 UI 必过）

- [ ] 无新增 hex 字面量（`:root` token 定义除外）、无内联 `style` 布局属性
- [ ] 亮色、暗色主题下均已检查（文字对比、边框、终端块）
- [ ] 危险操作确认按钮为 danger；非破坏操作未用 danger
- [ ] 每页 primary 实心按钮 ≤ 1
- [ ] 仅图标按钮有 `aria-label`；禁用按钮有原因提示
- [ ] 空状态可操作（el-empty + 下一步）；错误信息包含下一步
- [ ] 中文全角标点；无 `5m30s` 式英文缩写；状态文案已中文化
- [ ] 长内容有截断（`show-overflow-tooltip` / `text-overflow`）
- [ ] `python -m pytest webui/tests -q` 通过（含 test_style lint 与 boot check）
