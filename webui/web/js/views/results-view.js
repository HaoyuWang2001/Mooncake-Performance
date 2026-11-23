"use strict";
/* 运行结果视图（v2）：动态结果框（0..N 竖向排列，独立拖高/全屏/关闭）+ 同文件锁定 +
   左侧 run 列表分配到焦点框。全屏 = teleport 蒙版 + 居中大框（宽 92% / 高 88vh）。 */

function _fileGroup(rel) {
  const base = rel.split("/").pop().toLowerCase();
  if (/spdiag_tree\.[^.]+\.txt$/.test(base)) return "spdiag树";
  if (rel.includes("/profiling/") || rel.startsWith("profiling/")) return "profiling";
  if (base.endsWith(".csv")) return "csv";
  if (base.endsWith(".html")) return "报表";
  if (base.endsWith(".png") || base.endsWith(".jpg") || base.endsWith(".gif")) return "图片";
  if (base.endsWith(".log") || base.endsWith(".glog") || base.endsWith(".txt")) return "日志";
  if (base.endsWith(".json")) return "json";
  return "其他";
}

/* 模块级 helper：全部以 pane 对象为参（ResPane 与父视图共用） */
function _kindOf(pane) {
  if (!pane.file) return "none";
  const base = pane.file.split("/").pop().toLowerCase();
  if (/spdiag_tree\.[^.]+\.txt$/.test(base)) return "tree";
  const g = _fileGroup(pane.file);
  if (g === "图片") return "img";
  if (g === "csv") return "csv";
  if (g === "报表") return "html";
  if (g === "json") return "json";
  return "txt";
}

function _groupedFiles(pane) {
  const groups = {};
  for (const f of pane.files) {
    const g = _fileGroup(f);
    (groups[g] = groups[g] || []).push(f);
  }
  const order = ["csv", "报表", "图片", "spdiag树", "profiling", "日志", "json", "其他"];
  return order.filter(g => groups[g]).map(g => ({ label: g, opts: groups[g] }));
}

function _treeShortcut(pane) {
  const base = (pane.file || "").split("/").pop();
  if (base !== "spdiag.show.txt") return [];
  return pane.files.filter(f => /spdiag_tree\.[^.]+\.txt$/.test(f.split("/").pop()));
}

function _defaultDetail(files) {
  return files.find(f => f === "benchmark_detail.csv") ||
         files.find(f => f === "rank/benchmark_detail.csv") ||
         files.find(f => f.endsWith("/benchmark_detail.csv")) || "";
}

async function _loadFiles(pane, keepFile) {
  const prev = pane.file;
  pane.rows = []; pane.headers = []; pane.text = "";
  pane.offset = 0; pane.size = 0; pane.eof = true;
  if (!pane.ts) { pane.files = []; pane.file = ""; return; }
  pane.loading = true;
  try {
    const r = await api("GET", "/api/outputs/" + encodeURIComponent(pane.ts) + "/tree");
    pane.files = r.files;
    let target = "";
    if (keepFile) {
      // 切换 run：保留之前选中的文件；没有同名文件时自动展示
      // benchmark_detail.csv（并 toast 提示），再没有才清空
      if (prev && pane.files.includes(prev)) {
        target = prev;
      } else if (prev) {
        const fallback = _defaultDetail(pane.files);
        target = fallback;
        if (fallback) {
          ElementPlus.ElMessage.warning(
            "该 run 无同名文件（" + prev + "），已自动展示 benchmark_detail.csv");
        } else {
          ElementPlus.ElMessage.warning("该 run 无同名文件，已清空选择：" + prev);
        }
      }
    } else {
      // 新建框 / 结果跳转：沿用默认选中 benchmark_detail.csv
      target = _defaultDetail(pane.files);
    }
    if (target === prev) {
      if (target) await _loadContent(pane);   // 值未变不会触发 watcher
    } else {
      pane.file = target;                     // 由文件 watcher 加载内容
    }
  } catch (e) {
    ElementPlus.ElMessage.error(e.message);
  } finally {
    pane.loading = false;
  }
}

async function _loadContent(pane) {
  pane.rows = []; pane.headers = []; pane.text = "";
  pane.offset = 0; pane.size = 0; pane.eof = true;
  if (!pane.ts || !pane.file) return;
  const path = "outputs/" + pane.ts + "/" + pane.file;
  const kind = _kindOf(pane);
  pane.loading = true;
  try {
    if (kind === "csv") {
      const r = await api("GET", "/api/csv?path=" + encodeURIComponent(path) + "&max_rows=3000");
      pane.headers = r.headers;
      pane.rows = r.rows.map(row => {
        const o = {};
        r.headers.forEach((h, i) => { o["c" + i] = row[i]; });
        return o;
      });
    } else if (kind === "json") {
      const r = await api("GET", "/api/file?path=" + encodeURIComponent(path));
      try { pane.text = JSON.stringify(JSON.parse(r.content), null, 2); }
      catch (e) { pane.text = r.content; }
    } else if (kind === "tree") {
      const r = await api("GET", "/api/file?path=" + encodeURIComponent(path));
      pane.text = r.content;
    } else {
      // 分页：从文件头开始，每次加载一页（后端 MAX_TEXT_CHUNK），
      // 「加载更多」按 offset 续读，日志再大也不会被静默截断
      const r = await api("GET", "/api/file?path=" + encodeURIComponent(path) + "&offset=0");
      pane.text = r.content;
      pane.offset = r.offset || 0;
      pane.size = r.size || 0;
      pane.eof = r.eof !== false;
    }
  } catch (e) {
    ElementPlus.ElMessage.error(e.message);
  } finally {
    pane.loading = false;
  }
}

/* 追加下一页文本（分页加载入口） */
async function _loadMore(pane) {
  if (pane.loadingMore || pane.eof || !pane.ts || !pane.file) return;
  const kind = _kindOf(pane);
  if (kind !== "txt") return;
  const path = "outputs/" + pane.ts + "/" + pane.file;
  pane.loadingMore = true;
  try {
    const r = await api("GET", "/api/file?path=" + encodeURIComponent(path) +
                               "&offset=" + (pane.offset || 0));
    pane.text += r.content;
    pane.offset = r.offset || pane.offset;
    pane.size = r.size || pane.size;
    pane.eof = r.eof !== false;
  } catch (e) {
    ElementPlus.ElMessage.error(e.message);
  } finally {
    pane.loadingMore = false;
  }
}

/* 数字列判定：整列非空值均为数字（含负号/小数）则右对齐 + tabular-nums。 */
function _csvIsNumeric(rows, key) {
  let seen = false;
  for (const r of rows || []) {
    const v = (r[key] === null || r[key] === undefined) ? "" : String(r[key]).trim();
    if (!v || v === "-") continue;
    if (!/^[-+]?\d+(\.\d+)?$/.test(v)) return false;
    seen = true;
  }
  return seen;
}

function rawUrlOf(pane) {
  return "/api/file?path=" +
    encodeURIComponent("outputs/" + pane.ts + "/" + pane.file) + "&mode=raw";
}

/* CSV 列宽按表头显示宽度估算（CJK≈2 个显示单位），保证表头单行显示；
   Element Plus 默认 min-width=90 会把 queue_wait_avg_ms 等长表头挤成两行。 */
function _csvColWidth(h) {
  let units = 0;
  for (const ch of String(h || "")) units += (ch.codePointAt(0) > 0x2e7f ? 2 : 1);
  return Math.max(90, units * 8 + 28);
}

function _fmtBytes(n) {
  n = Number(n) || 0;
  if (n < 1024) return n + " B";
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KB";
  if (n < 1024 * 1024 * 1024) return (n / (1024 * 1024)).toFixed(1) + " MB";
  return (n / (1024 * 1024 * 1024)).toFixed(2) + " GB";
}

let _paneUid = 0;

/* 单个结果框（行内 / 全屏两种形态共用） */
const ResPane = {
  name: "ResPane",
  props: { pane: { type: Object, required: true },
           idx: { type: Number, required: true },
           runs: { type: Array, default: () => [] },
           fs: { type: Boolean, default: false },
           focused: { type: Boolean, default: false } },
  emits: ["focus", "close", "toggle-fs"],
  template: `
  <div class="pane" :class="{ focuspane: !fs }" @click="$emit('focus')">
    <div class="pane-head">
      <el-tag size="small" :type="focused && !fs ? 'primary' : 'info'">#{{ idx + 1 }}</el-tag>
      <el-select v-model="pane.ts" filterable placeholder="选择 run" size="small"
                 style="width: 190px" @change="loadFiles(pane, true)">
        <el-option v-for="r in runs" :key="r.ts" :label="r.ts" :value="r.ts"></el-option>
      </el-select>
      <el-select v-model="pane.file" filterable placeholder="选择文件" size="small"
                 style="flex: 1; min-width: 300px">
        <el-option-group v-for="g in groupedFiles(pane)" :key="g.label"
                         :label="g.label + ' (' + g.opts.length + ')'">
          <el-option v-for="o in g.opts" :key="o" :label="o" :value="o"></el-option>
        </el-option-group>
      </el-select>
      <el-radio-group v-if="kindOf(pane) === 'tree'" v-model="pane.rawView" size="small">
        <el-radio-button :value="false">树状图</el-radio-button>
        <el-radio-button :value="true">源文件</el-radio-button>
      </el-radio-group>
      <el-dropdown v-if="treeShortcut(pane).length" @command="f => { pane.file = f; }">
        <el-button size="small" type="primary">查看树状图</el-button>
        <template #dropdown>
          <el-dropdown-menu>
            <el-dropdown-item v-for="f in treeShortcut(pane)" :key="f" :command="f">{{ f }}</el-dropdown-item>
          </el-dropdown-menu>
        </template>
      </el-dropdown>
      <el-button v-if="kindOf(pane) === 'txt' || kindOf(pane) === 'json'"
                 size="small" text :type="pane.wrap ? 'primary' : ''"
                 @click.stop="toggleWrap(pane)"
                 title="长行换行 / 横向滚动 切换">换行</el-button>
      <span class="pane-tools">
        <el-button size="small" text :type="pane.auto ? 'primary' : ''"
                   @click.stop="setAuto(pane)" title="根据内容自适应高度">高度自适应</el-button>
        <el-button size="small" type="primary" plain @click.stop="$emit('toggle-fs')"
                   :title="fs ? '退出全屏' : '全屏查看'">{{ fs ? "退出全屏" : "全屏" }}</el-button>
        <el-button v-if="!fs" size="small" text type="danger" @click.stop="$emit('close')"
                   title="关闭此结果框">✕</el-button>
      </span>
    </div>
    <div class="pane-body" :class="{ fsbody: fs, auto: pane.auto }" v-loading="pane.loading"
         :style="(fs || pane.auto) ? null : { height: pane.h + 'px' }">
      <div v-if="!pane.file" class="hint hint-pad">选择文件查看</div>
      <template v-else-if="kindOf(pane) === 'img'">
        <div v-if="fs" class="imgfs">
          <div class="imgbar">
            <el-button-group size="small">
              <el-button @click="imgStep(pane, -0.25)">－</el-button>
              <el-button disabled>{{ pane.imgScale === "fit" ? "适应" : Math.round(pane.imgScale * 100) + "%" }}</el-button>
              <el-button @click="imgStep(pane, 0.25)">＋</el-button>
              <el-button @click="imgFit(pane)">适应</el-button>
            </el-button-group>
            <span class="hint">等比缩放，超出可滚动</span>
          </div>
          <div class="imgwrap">
            <img :src="rawUrl(pane)" :style="imgStyle(pane)"
                 :class="dragging ? 'dragging' : (pane.imgScale === 'fit' ? '' : 'can-drag')"
                 @mousedown.left.prevent="imgDragStart(pane, $event)"
                 :title="pane.imgScale === 'fit' ? '' : '按住左键拖动查看'">
          </div>
        </div>
        <img v-else :src="rawUrl(pane)">
      </template>
      <el-table v-else-if="kindOf(pane) === 'csv'" :data="pane.rows" size="small"
                class="csv-table" border style="width: 100%"
                :empty-text="pane.loading ? '加载中…' : '无数据'">
        <el-table-column v-for="(h, i) in pane.headers" :key="i" :label="h"
                         :prop="'c' + i" show-overflow-tooltip
                         :min-width="csvColWidth(h)"
                         :align="csvIsNumeric(pane.rows, 'c' + i) ? 'right' : 'left'"
                         :class-name="csvIsNumeric(pane.rows, 'c' + i) ? 'mc-num' : ''"></el-table-column>
      </el-table>
      <div v-else-if="kindOf(pane) === 'html'" class="report-pane">
        <div class="report-pane-icon">📊</div>
        <div class="report-pane-title">{{ pane.file }}</div>
        <div class="report-pane-desc">
          HTML 报表与 WebUI 解耦，可独立打开；点击按钮在新标签页查看
          （图表可缩放/悬停查看每个桶的统计量）。
        </div>
        <el-button type="primary" @click="openReport(pane)">在新标签页打开报表</el-button>
        <div class="mc-muted mt-2">
          也可直接分享该文件：report.html 单文件可用；同目录 data.js 提供全量深查数据。
        </div>
      </div>
      <pre v-else-if="kindOf(pane) === 'json'" class="json" :class="{ nw: !pane.wrap }">{{ pane.text }}</pre>
      <spdiag-tree v-else-if="kindOf(pane) === 'tree' && !pane.rawView"
                   :text="pane.text"></spdiag-tree>
      <template v-else>
        <pre class="txt" :class="{ nw: !pane.wrap }">{{ pane.text }}</pre>
        <div v-if="!pane.eof" class="page-bar">
          <span class="hint">
            已加载 {{ fmtBytes(pane.offset) }} / {{ fmtBytes(pane.size) }}
          </span>
          <el-button size="small" type="primary" plain :loading="pane.loadingMore"
                     @click.stop="loadMore(pane)">加载更多</el-button>
        </div>
      </template>
    </div>
    <drag-handle v-if="!fs" :model-value="pane.auto ? 0 : (pane.h || 0)"
                 :min="200" :max="1000"
                 @update:model-value="v => { pane.h = v; pane.auto = false; }"></drag-handle>
  </div>`,
  setup() {
    function setAuto(pane) { pane.auto = true; }
    function toggleWrap(pane) {
      pane.wrap = !pane.wrap;
      try { localStorage.setItem("mcweb-wrap", pane.wrap ? "1" : "0"); } catch (e) { /* ignore */ }
    }
    function imgStep(pane, d) {
      const cur = pane.imgScale === "fit" ? 1 : pane.imgScale;
      pane.imgScale = Math.min(4, Math.max(0.25, Math.round((cur + d) * 100) / 100));
      pane.imgX = 0; pane.imgY = 0;   // 缩放后回中
    }
    function imgFit(pane) {
      pane.imgScale = "fit";
      pane.imgX = 0; pane.imgY = 0;
    }
    // 左键拖拽平移：替代滚动条；按 图片-容器 差值钳制，fit 模式（无越界）禁用
    function imgDragStart(pane, ev) {
      if (pane.imgScale === "fit") return;
      const img = ev.target, wrap = img.parentElement;
      const maxX = Math.max(0, (img.offsetWidth - wrap.clientWidth) / 2);
      const maxY = Math.max(0, (img.offsetHeight - wrap.clientHeight) / 2);
      if (maxX === 0 && maxY === 0) return;
      const sx = ev.clientX, sy = ev.clientY, ox = pane.imgX, oy = pane.imgY;
      dragging.value = true;
      const move = e => {
        pane.imgX = Math.min(maxX, Math.max(-maxX, ox + (e.clientX - sx)));
        pane.imgY = Math.min(maxY, Math.max(-maxY, oy + (e.clientY - sy)));
      };
      const up = () => {
        window.removeEventListener("mousemove", move);
        window.removeEventListener("mouseup", up);
        dragging.value = false;
      };
      window.addEventListener("mousemove", move);
      window.addEventListener("mouseup", up);
    }
    function imgStyle(pane) {
      const t = "translate(calc(-50% + " + (pane.imgX || 0) + "px), calc(-50% + " + (pane.imgY || 0) + "px))";
      if (pane.imgScale === "fit") return { maxWidth: "100%", maxHeight: "100%", transform: t };
      return { width: (pane.imgScale * 100) + "%", height: "auto", transform: t };
    }
    const dragging = Vue.ref(false);
    return { setAuto, kindOf: _kindOf, groupedFiles: _groupedFiles, treeShortcut: _treeShortcut,
             csvColWidth: _csvColWidth, csvIsNumeric: _csvIsNumeric,
             loadFiles: _loadFiles, toggleWrap, imgStep, imgFit, imgDragStart, dragging, imgStyle,
             loadMore: _loadMore, fmtBytes: _fmtBytes,
             openReport: p => window.open(rawUrlOf(p), "_blank"),
             rawUrl: rawUrlOf };
  },
};

const ResultsView = {
  template: `
  <div class="page">
    <div class="mc-page-head">
      <div>
        <h1 class="title">统计结果</h1>
        <p class="desc">点击左侧 run 分配到焦点框（主色描边 = 焦点）；同文件锁定便于跨 run 对比 CSV 与图片。</p>
      </div>
      <div class="actions">
        <el-tooltip :content="listOpen ? '收起运行结果列表' : '展开运行结果列表'" placement="top">
          <el-button size="small" circle @click="toggleList"
                     :aria-label="listOpen ? '收起运行结果列表' : '展开运行结果列表'">
            <el-icon><component :is="listOpen ? 'Fold' : 'Expand'"/></el-icon>
          </el-button>
        </el-tooltip>
        <template v-if="panes.length >= 2">
          <span class="mc-label">同文件锁定</span>
          <el-switch v-model="lockFile"></el-switch>
        </template>
        <el-button size="small" type="primary" plain @click="addPane()">＋ 添加结果框</el-button>
        <el-tooltip content="刷新运行结果列表" placement="top">
          <el-button size="small" circle :loading="loading" aria-label="刷新运行结果列表"
                     @click="loadRuns">
            <el-icon><component is="Refresh"/></el-icon>
          </el-button>
        </el-tooltip>
      </div>
    </div>

    <div class="results-layout">
      <el-card v-show="listOpen" shadow="never" class="run-list-card"
               :style="{ width: listW + 'px' }">
        <template #header>
            <div class="mc-toolbar">
              <b>运行结果（{{ runs.length }}）</b>
              <span class="spacer"></span>
              <el-button size="small" plain :type="multiSelect ? 'primary' : ''" @click="toggleMulti">
                {{ multiSelect ? "退出多选" : "多选" }}
              </el-button>
            </div>
          </template>
          <div v-if="multiSelect" class="ms-bar">
            <el-checkbox :model-value="allSelected" @change="onSelectAll">全选</el-checkbox>
            <span class="hint mc-num">已选 {{ selected.length }}</span>
            <span class="spacer"></span>
            <el-button size="small" type="danger" :disabled="!selected.length"
                       title="先多选勾选要删除的运行结果"
                       @click="deleteSelected">删除选中</el-button>
          </div>
          <div class="run-list">
          <el-empty v-if="!runs.length" :image-size="60" description="还没有运行结果" />
          <div v-for="r in runs" :key="r.ts" class="run-item"
               :class="{ active: isActivePaneTs(r.ts), selected: isSelected(r.ts) }"
               @click="pickRun(r.ts)">
            <el-checkbox v-if="multiSelect" class="ri-check"
                         :model-value="isSelected(r.ts)"
                         @click.stop @change="toggleSelect(r.ts)"></el-checkbox>
            <div class="ri-body">
              <div class="ri-title">
                <span class="ri-name" :title="r.ts">{{ r.ts }}</span>
                <el-tag class="ri-tag" :type="r.has_detail ? 'success' : 'info'" size="small">
                  {{ r.has_detail ? "detail" : "无 detail" }}
                </el-tag>
              </div>
              <div class="ri-sub">{{ r.mtime }} · {{ r.csv_count }} csv</div>
            </div>
            <span class="ri-more-wrap" @click.stop>
              <el-dropdown trigger="click" placement="bottom-end"
                           popper-class="ri-more-menu"
                           @command="cmd => onRunCommand(cmd, r.ts)">
                <el-button size="small" text circle class="ri-more"
                           :loading="rebuilding === r.ts"
                           :disabled="!!rebuilding && rebuilding !== r.ts"
                           aria-label="更多操作"
                           :title="rebuilding && rebuilding !== r.ts
                                   ? '另一运行结果正在重建报表，完成后再试' : '更多操作'"
                           @click.stop>
                  <el-icon v-if="rebuilding !== r.ts"><component is="MoreFilled"/></el-icon>
                </el-button>
                <template #dropdown>
                  <el-dropdown-menu>
                    <el-dropdown-item command="rebuild">重建报表</el-dropdown-item>
                    <el-dropdown-item command="rename">改名</el-dropdown-item>
                    <el-dropdown-item command="delete" class="danger-item">删除</el-dropdown-item>
                  </el-dropdown-menu>
                </template>
              </el-dropdown>
            </span>
          </div>
          </div>
        </el-card>
      <hdrag-handle v-show="listOpen" v-model="listW" :min="240" :max="560"
                    persist="mcweb-results-list-w"></hdrag-handle>

      <div class="results-main">
        <div v-if="!panes.length" class="pane pane-empty">
          <div class="hint hint-pad">
            尚无结果框——点击左侧 run 或「＋ 添加结果框」开始查看
          </div>
        </div>
        <res-pane v-for="(p, i) in panes" :key="p.uid"
                  :class="{ focus: activePane === i }"
                  :pane="p" :idx="i" :runs="runs" :focused="activePane === i"
                  @focus="activePane = i"
                  @close="removePane(i)"
                  @toggle-fs="fullscreenIdx = fullscreenIdx === i ? -1 : i"></res-pane>
      </div>
    </div>

    <teleport to="body">
      <div v-if="fullscreenIdx >= 0 && panes[fullscreenIdx]" class="fs-mask"
           @click.self="fullscreenIdx = -1">
        <div class="fs-box">
          <res-pane :pane="panes[fullscreenIdx]" :idx="fullscreenIdx" :runs="runs" :fs="true"
                    @focus="activePane = fullscreenIdx"
                    @toggle-fs="fullscreenIdx = -1"></res-pane>
        </div>
      </div>
    </teleport>
  </div>`,
  setup() {
    const loading = Vue.ref(false);
    const runs = Vue.ref([]);
    const lockFile = Vue.ref(true);
    const activePane = Vue.ref(0);
    const fullscreenIdx = Vue.ref(-1);
    const panes = Vue.reactive([]);

    // 左列运行结果：可收起（整体隐藏）+ 可拖宽（240–560px），两者都持久化
    const LIST_W_KEY = "mcweb-results-list-w";
    const LIST_OPEN_KEY = "mcweb-results-list-open";
    function _loadListW() {
      try {
        const v = parseInt(localStorage.getItem(LIST_W_KEY), 10);
        return Number.isFinite(v) && v >= 240 && v <= 560 ? v : 300;
      } catch (e) { return 300; }
    }
    function _loadListOpen() {
      try { return localStorage.getItem(LIST_OPEN_KEY) !== "0"; }
      catch (e) { return true; }
    }
    const listW = Vue.ref(_loadListW());
    const listOpen = Vue.ref(_loadListOpen());
    Vue.watch(listW, v => {
      try { localStorage.setItem(LIST_W_KEY, String(Math.round(v))); } catch (e) { /* ignore */ }
    });
    Vue.watch(listOpen, v => {
      try { localStorage.setItem(LIST_OPEN_KEY, v ? "1" : "0"); } catch (e) { /* ignore */ }
    });
    function toggleList() {
      listOpen.value = !listOpen.value;
      Vue.nextTick(fitRunList);
    }

    // 左侧结果列表精确贴合可用高度（不依赖固定 magic number）
    function fitRunList() {
      Vue.nextTick(() => {
        const main = document.querySelector(".el-main");
        const card = document.querySelector(".run-list-card");
        if (!main || !card) return;
        const mr = main.getBoundingClientRect();
        const cr = card.getBoundingClientRect();
        const pad = parseFloat(getComputedStyle(main).paddingBottom) || 20;
        const avail = mr.height - (cr.top - mr.top) - pad;
        card.style.maxHeight = Math.max(240, avail) + "px";
      });
    }

    function newPane(idx) {
      // Vue.reactive：pane 必须从创建起即为代理对象——addPane 持有该引用做
      // 异步写入（_loadFiles），raw 对象的赋值不会触发响应式更新（首个
      // 结果框曾因此卡在 loading 且文件列表永不刷新）
      // 与实例详情日志页统一：未设置时默认开启换行（关闭后长行可横向滚动）
      let wrap = true;
      try { wrap = localStorage.getItem("mcweb-wrap") !== "0"; } catch (e) { /* ignore */ }
      return Vue.reactive({ uid: ++_paneUid, ts: "", files: [], file: "", loading: false,
                            rawView: false, wrap: wrap, imgScale: "fit",
                            imgX: 0, imgY: 0,
                            rows: [], headers: [], text: "",
                            offset: 0, size: 0, eof: true, loadingMore: false,
                            auto: true, h: null });
    }

    async function loadRuns() {
      loading.value = true;
      try {
        runs.value = (await api("GET", "/api/outputs")).outputs;
        for (const p of panes) {
          if (p.ts && !runs.value.some(r => r.ts === p.ts)) {
            p.ts = ""; p.file = ""; p.files = [];
          }
        }
      } catch (e) {
        ElementPlus.ElMessage.error(e.message);
      } finally {
        loading.value = false;
      }
    }

    function addPane(ts, keepFile) {
      const p = newPane(panes.length);
      p.ts = ts !== undefined ? ts : (runs.value.length ? runs.value[0].ts : "");
      panes.push(p);
      activePane.value = panes.length - 1;
      _loadFiles(p, !!keepFile);
      return p;
    }

    function removePane(i) {
      if (fullscreenIdx.value === i) fullscreenIdx.value = -1;
      else if (fullscreenIdx.value > i) fullscreenIdx.value -= 1;
      panes.splice(i, 1);
      if (activePane.value >= panes.length) activePane.value = Math.max(0, panes.length - 1);
    }

    // ---- 多选（删除） ----
    const multiSelect = Vue.ref(false);
    const selected = Vue.ref([]);
    const allSelected = Vue.computed(() =>
      runs.value.length > 0 && selected.value.length === runs.value.length);
    function isSelected(ts) { return selected.value.includes(ts); }
    function toggleSelect(ts) {
      const i = selected.value.indexOf(ts);
      if (i >= 0) selected.value.splice(i, 1);
      else selected.value.push(ts);
    }
    function selectAll() { selected.value = runs.value.map(r => r.ts); }
    function clearSelected() { selected.value = []; }
    function toggleMulti() {
      multiSelect.value = !multiSelect.value;
      if (!multiSelect.value) clearSelected();
    }
    function onSelectAll() {
      if (allSelected.value) clearSelected();
      else selectAll();
    }

    function pickRun(ts) {
      if (multiSelect.value) { toggleSelect(ts); return; }
      if (!panes.length) { addPane(ts, true); return; }
      const p = panes[activePane.value];
      if (p.ts !== ts) { p.ts = ts; _loadFiles(p, true); }
    }

    function isActivePaneTs(ts) {
      const p = panes[activePane.value];
      return p && p.ts === ts;
    }

    function _applyRename(oldName, newName) {
      for (const p of panes) if (p.ts === oldName) p.ts = newName;
      if (store.resultsTarget === oldName) store.resultsTarget = newName;
      const i = selected.value.indexOf(oldName);
      if (i >= 0) selected.value.splice(i, 1);
    }

    function onRunCommand(cmd, ts) {
      if (cmd === "rebuild") return rebuildReport(ts);
      if (cmd === "rename") return renameRun(ts);
      if (cmd === "delete") return deleteRun(ts);
    }

    async function renameRun(ts) {
      let name;
      try {
        const r = await ElementPlus.ElMessageBox.prompt("重命名运行结果目录", "重命名 " + ts, {
          confirmButtonText: "重命名", cancelButtonText: "取消", inputValue: ts,
          inputValidator: v => {
            v = (v || "").trim();
            if (!v) return "名称不能为空";
            if (v === "." || v === ".." || v.startsWith(".")) return "不能以 . 开头";
            if (!/^[0-9A-Za-z._-]+$/.test(v)) return "仅允许字母、数字、点、下划线、短横线";
            if (v === ts) return "与原名称相同";
            return true;
          },
        });
        name = r.value.trim();
      } catch (e) { return; }
      try {
        const r = await api("POST", "/api/outputs/" + encodeURIComponent(ts) + "/rename",
                            { name: name });
        _applyRename(ts, r.ts);
        ElementPlus.ElMessage.success("已重命名为 " + r.ts);
        await loadRuns();
      } catch (e) {
        ElementPlus.ElMessage.error(e.message);
      }
    }

    // ---- 重建报表：某 run 已有日志/产物但 report.html 缺失或过期时重出 ----
    const rebuilding = Vue.ref("");
    async function _waitRun(rid, timeoutMs) {
      const deadline = Date.now() + (timeoutMs || 180000);
      while (Date.now() < deadline) {
        await new Promise(res => setTimeout(res, 1200));
        try {
          const d = await api("GET", "/api/runs/" + encodeURIComponent(rid));
          if (d.status !== "running") return d;
        } catch (e) { return null; }
      }
      return null;
    }
    async function rebuildReport(ts) {
      if (rebuilding.value) return;
      try {
        await confirmDanger("将用本地 stats / 原始日志重建 " + ts +
                            " 的 report.html 与 overview.html；不访问集群，" +
                            "不会重新拉取远端产物。", "重建报表");
      } catch (e) { return; }
      rebuilding.value = ts;
      try {
        const r = await api("POST", "/api/runs/rebuild-report", { ts: ts });
        const done = await _waitRun(r.run.id);
        if (done && done.rc === 0) {
          ElementPlus.ElMessage.success("报表已重建：" + ts);
          await loadRuns();
          for (const p of panes) if (p.ts === ts) _loadFiles(p, true);
        } else if (done) {
          ElementPlus.ElMessage.error(
            "重建失败（rc=" + done.rc + "），日志见「性能测试 → 历史运行」");
        } else {
          ElementPlus.ElMessage.warning("重建仍在进行，稍后刷新查看结果");
        }
      } catch (e) {
        ElementPlus.ElMessage.error(e.message);
      } finally {
        rebuilding.value = "";
      }
    }

    async function _deleteNames(names, label) {
      try {
        await confirmDanger("将把" + label + "移入回收站（webui/backups/outputs-trash）。" +
                            (names.length > 1 ? "目标：" + names.slice(0, 5).join("、") +
                             (names.length > 5 ? " 等 " + names.length + " 个" : "") : ""),
                            "删除运行结果");
      } catch (e) { return; }
      try {
        const r = await api("POST", "/api/outputs/delete", { names: names });
        if (r.failed && r.failed.length) {
          ElementPlus.ElMessage.warning("部分删除失败：" +
            r.failed.map(f => f.ts + "（" + f.error + "）").join("；"));
        } else {
          ElementPlus.ElMessage.success("已删除 " + r.deleted.length + " 项（已移入回收站）");
        }
        selected.value = selected.value.filter(t => !names.includes(t));
        await loadRuns();
      } catch (e) {
        ElementPlus.ElMessage.error(e.message);
      }
    }

    function deleteRun(ts) { return _deleteNames([ts], " " + ts + " "); }
    function deleteSelected() {
      if (!selected.value.length) return;
      return _deleteNames(selected.value.slice(), "选中的 " + selected.value.length + " 个运行结果");
    }

    // 文件变化：加载内容 + 同文件锁定（任一框换文件，其余框跟随）
    Vue.watch(() => panes.map(p => p.file), (vals, old) => {
      let i = -1;
      for (let k = 0; k < vals.length; k++) {
        if (old[k] === undefined || vals[k] !== old[k]) { i = k; break; }
      }
      if (i < 0) return;
      _loadContent(panes[i]);
      if (!vals[i] || !lockFile.value || panes.length < 2) return;
      for (let j = 0; j < panes.length; j++) {
        if (j === i) continue;
        const p = panes[j];
        if (p.files.includes(vals[i])) {
          if (p.file !== vals[i]) p.file = vals[i];
        } else if (p.file) {
          ElementPlus.ElMessage.warning("窗格 #" + (j + 1) + " 该 run 无同名文件：" + vals[i]);
        }
      }
    });

    function onKey(ev) {
      if (ev.key === "Escape" && fullscreenIdx.value >= 0) fullscreenIdx.value = -1;
    }
    Vue.onMounted(() => {
      window.addEventListener("keydown", onKey);
      window.addEventListener("resize", fitRunList);
      fitRunList();
    });
    Vue.onUnmounted(() => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("resize", fitRunList);
    });

    Vue.onActivated(async () => {
      await loadRuns();
      if (store.resultsTarget) {
        const target = store.resultsTarget;
        store.resultsTarget = null;
        activePane.value = 0;
        if (!panes.length) {
          addPane(target);
        } else if (panes[0].ts !== target) {
          panes[0].ts = target;
          _loadFiles(panes[0]);
        }
      } else if (!panes.length && runs.value.length) {
        addPane();
      }
      fitRunList();
    });

    return { runs, loading, lockFile, activePane, fullscreenIdx, panes,
             listW, listOpen, toggleList,
             multiSelect, selected, allSelected, rebuilding,
             loadRuns, addPane, removePane, pickRun, isActivePaneTs,
             isSelected, toggleSelect, onSelectAll, toggleMulti,
             rebuildReport, renameRun, deleteRun, deleteSelected,
             onRunCommand };
  },
};
