"use strict";
/* 运行结果视图（v2）：动态结果框（0..N 竖向排列，独立拖高/全屏/关闭）+ 同文件锁定 +
   左侧 run 列表分配到焦点框。全屏 = teleport 蒙版 + 居中大框（宽 92% / 高 88vh）。 */

function _fileGroup(rel) {
  const base = rel.split("/").pop().toLowerCase();
  if (/spdiag_tree\.[^.]+\.txt$/.test(base)) return "spdiag树";
  if (rel.includes("/profiling/") || rel.startsWith("profiling/")) return "profiling";
  if (base.endsWith(".csv")) return "csv";
  if (base.endsWith(".png") || base.endsWith(".jpg") || base.endsWith(".gif")) return "图片";
  if (base.endsWith(".log") || base.endsWith(".txt")) return "日志";
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
  if (g === "json") return "json";
  return "txt";
}

function _groupedFiles(pane) {
  const groups = {};
  for (const f of pane.files) {
    const g = _fileGroup(f);
    (groups[g] = groups[g] || []).push(f);
  }
  const order = ["csv", "图片", "spdiag树", "profiling", "日志", "json", "其他"];
  return order.filter(g => groups[g]).map(g => ({ label: g, opts: groups[g] }));
}

function _treeShortcut(pane) {
  const base = (pane.file || "").split("/").pop();
  if (base !== "spdiag.show.txt") return [];
  return pane.files.filter(f => /spdiag_tree\.[^.]+\.txt$/.test(f.split("/").pop()));
}

async function _loadFiles(pane) {
  pane.file = "";
  pane.rows = []; pane.headers = []; pane.text = "";
  if (!pane.ts) { pane.files = []; return; }
  pane.loading = true;
  try {
    const r = await api("GET", "/api/outputs/" + encodeURIComponent(pane.ts) + "/tree");
    pane.files = r.files;
    const detail = pane.files.find(f => f === "benchmark_detail.csv");
    if (detail) pane.file = detail;
  } catch (e) {
    ElementPlus.ElMessage.error(e.message);
  } finally {
    pane.loading = false;
  }
}

async function _loadContent(pane) {
  pane.rows = []; pane.headers = []; pane.text = "";
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
      const r = await api("GET", "/api/file?path=" + encodeURIComponent(path) + "&tail=131072");
      pane.text = r.content;
    }
  } catch (e) {
    ElementPlus.ElMessage.error(e.message);
  } finally {
    pane.loading = false;
  }
}

let _paneUid = 0;

/* 单个结果框（行内 / 全屏两种形态共用） */
const ResPane = {
  name: "ResPane",
  props: { pane: { type: Object, required: true },
           idx: { type: Number, required: true },
           runs: { type: Array, default: () => [] },
           fs: { type: Boolean, default: false } },
  emits: ["focus", "close", "toggle-fs"],
  template: `
  <div class="pane" :class="{ focuspane: !fs }" @click="$emit('focus')">
    <div class="pane-head">
      <el-tag size="small" type="info">#{{ idx + 1 }}</el-tag>
      <el-select v-model="pane.ts" filterable placeholder="选择 run" size="small"
                 style="width: 190px" @change="loadFiles(pane)">
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
      <span class="pane-tools">
        <el-button size="small" text @click.stop="$emit('toggle-fs')"
                   :title="fs ? '退出全屏' : '全屏查看'">{{ fs ? "退出全屏" : "全屏" }}</el-button>
        <el-button v-if="!fs" size="small" text type="danger" @click.stop="$emit('close')"
                   title="关闭此结果框">✕</el-button>
      </span>
    </div>
    <div class="pane-body" :class="{ fsbody: fs }" v-loading="pane.loading"
         :style="fs ? null : { height: pane.h + 'px' }">
      <div v-if="!pane.file" class="hint" style="padding: 30px; text-align: center">选择文件查看</div>
      <img v-else-if="kindOf(pane) === 'img'" :src="rawUrl(pane)">
      <el-table v-else-if="kindOf(pane) === 'csv'" :data="pane.rows" size="small"
                border style="width: 100%" :empty-text="pane.loading ? '加载中…' : '无数据'">
        <el-table-column v-for="(h, i) in pane.headers" :key="i" :label="h"
                         :prop="'c' + i" show-overflow-tooltip min-width="90"></el-table-column>
      </el-table>
      <pre v-else-if="kindOf(pane) === 'json'" class="json">{{ pane.text }}</pre>
      <spdiag-tree v-else-if="kindOf(pane) === 'tree' && !pane.rawView"
                   :text="pane.text"></spdiag-tree>
      <pre v-else class="txt">{{ pane.text }}</pre>
    </div>
    <drag-handle v-if="!fs" v-model="pane.h" :min="200" :max="1000"
                 :persist="'mcweb-res-h' + idx"></drag-handle>
  </div>`,
  setup() {
    return { kindOf: _kindOf, groupedFiles: _groupedFiles, treeShortcut: _treeShortcut,
             loadFiles: _loadFiles,
             rawUrl: p => "/api/file?path=" +
               encodeURIComponent("outputs/" + p.ts + "/" + p.file) + "&mode=raw" };
  },
};

const ResultsView = {
  template: `
  <div class="page">
    <div class="header-row">
      <span class="hint">点击左侧 run 分配到焦点框（虚线高亮 = 焦点，点任意框头部切换）；框可任意增删/拖高/全屏</span>
      <span class="spacer"></span>
      <template v-if="panes.length >= 2">
        <span style="font-size: 13px">同文件锁定</span>
        <el-switch v-model="lockFile"></el-switch>
      </template>
      <el-button size="small" type="primary" plain @click="addPane()">＋ 添加结果框</el-button>
      <el-button size="small" @click="loadRuns" :loading="loading">刷新</el-button>
    </div>

    <el-row :gutter="14">
      <el-col :span="6">
        <el-card shadow="never">
          <template #header><b>运行结果（{{ runs.length }}）</b></template>
          <div v-for="r in runs" :key="r.ts" class="run-item"
               :class="{ active: isActivePaneTs(r.ts) }" @click="pickRun(r.ts)">
            <div class="ri-title">
              <span>{{ r.ts }}</span>
              <el-tag :type="r.has_detail ? 'success' : 'info'" size="small">
                {{ r.has_detail ? "detail" : "无 detail" }}
              </el-tag>
            </div>
            <div class="ri-sub">{{ r.mtime }} · {{ r.csv_count }} csv</div>
          </div>
          <el-empty v-if="!runs.length" description="暂无结果" :image-size="60"></el-empty>
        </el-card>
      </el-col>

      <el-col :span="18">
        <div v-if="!panes.length" class="pane pane-empty">
          <div class="hint" style="padding: 40px; text-align: center">
            尚无结果框——点击左侧 run 或「＋ 添加结果框」开始查看
          </div>
        </div>
        <res-pane v-for="(p, i) in panes" :key="p.uid"
                  :class="{ focus: activePane === i }"
                  :pane="p" :idx="i" :runs="runs"
                  @focus="activePane = i"
                  @close="removePane(i)"
                  @toggle-fs="fullscreenIdx = fullscreenIdx === i ? -1 : i"
                  style="margin-bottom: 14px"></res-pane>
      </el-col>
    </el-row>

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

    function newPane(idx) {
      return { uid: ++_paneUid, ts: "", files: [], file: "", loading: false,
               rawView: false, rows: [], headers: [], text: "",
               h: loadHeight("mcweb-res-h" + idx, 420) };
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

    function addPane(ts) {
      const p = newPane(panes.length);
      p.ts = ts !== undefined ? ts : (runs.value.length ? runs.value[0].ts : "");
      panes.push(p);
      activePane.value = panes.length - 1;
      _loadFiles(p);
      return p;
    }

    function removePane(i) {
      if (fullscreenIdx.value === i) fullscreenIdx.value = -1;
      else if (fullscreenIdx.value > i) fullscreenIdx.value -= 1;
      panes.splice(i, 1);
      if (activePane.value >= panes.length) activePane.value = Math.max(0, panes.length - 1);
    }

    function pickRun(ts) {
      if (!panes.length) { addPane(ts); return; }
      const p = panes[activePane.value];
      if (p.ts !== ts) { p.ts = ts; _loadFiles(p); }
    }

    function isActivePaneTs(ts) {
      const p = panes[activePane.value];
      return p && p.ts === ts;
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
    Vue.onMounted(() => window.addEventListener("keydown", onKey));
    Vue.onUnmounted(() => window.removeEventListener("keydown", onKey));

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
    });

    return { runs, loading, lockFile, activePane, fullscreenIdx, panes,
             loadRuns, addPane, removePane, pickRun, isActivePaneTs };
  },
};
