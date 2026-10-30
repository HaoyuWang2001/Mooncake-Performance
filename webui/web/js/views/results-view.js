"use strict";
/* 运行结果视图：runs 列表 + 单窗格查看 / 对比模式（上下双窗格）+ 同文件锁定。
   支持类型：CSV（表格渲染）、图片（直接显示）、JSON（pretty，matrix 指标占位）、文本（tail）。 */

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

const ResultsView = {
  template: `
  <div class="page">
    <div class="header-row">
      <span class="hint">点击左侧 run 分配到当前焦点窗格；对比模式上下并排查看，同文件锁定自动匹配同名文件</span>
      <span class="spacer"></span>
      <span style="font-size: 13px">对比模式</span>
      <el-switch v-model="compareMode"></el-switch>
      <template v-if="compareMode">
        <span style="font-size: 13px">同文件锁定</span>
        <el-switch v-model="lockFile"></el-switch>
      </template>
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
        <div class="pane" :class="{ focus: activePane === 0 }" style="margin-bottom: 14px"
             @click="activePane = 0">
          <div class="pane-head">
            <el-tag size="small" type="info">上</el-tag>
            <el-select v-model="panes[0].ts" filterable placeholder="选择 run" size="small"
                       style="width: 190px" @change="loadFiles(0)">
              <el-option v-for="r in runs" :key="r.ts" :label="r.ts" :value="r.ts"></el-option>
            </el-select>
            <el-select v-model="panes[0].file" filterable placeholder="选择文件" size="small"
                       style="flex: 1; min-width: 320px">
              <el-option-group v-for="g in groupedFiles(0)" :key="g.label" :label="g.label + ' (' + g.opts.length + ')'">
                <el-option v-for="o in g.opts" :key="o" :label="o" :value="o"></el-option>
              </el-option-group>
            </el-select>
            <el-radio-group v-if="kindOf(panes[0]) === 'tree'" v-model="panes[0].rawView" size="small">
              <el-radio-button :value="false">树状图</el-radio-button>
              <el-radio-button :value="true">源文件</el-radio-button>
            </el-radio-group>
            <el-dropdown v-if="treeShortcut(0).length" @command="f => { panes[0].file = f; }">
              <el-button size="small" type="primary">查看树状图</el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item v-for="f in treeShortcut(0)" :key="f" :command="f">{{ f }}</el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
          </div>
          <div class="pane-body" v-loading="panes[0].loading">
            <div v-if="!panes[0].file" class="hint" style="padding: 30px; text-align: center">选择文件查看</div>
            <img v-else-if="kindOf(panes[0]) === 'img'"
                 :src="rawUrl(panes[0])">
            <el-table v-else-if="kindOf(panes[0]) === 'csv'" :data="panes[0].rows" size="small"
                      border style="width: 100%" :empty-text="panes[0].loading ? '加载中…' : '无数据'">
              <el-table-column v-for="(h, i) in panes[0].headers" :key="i" :label="h"
                               :prop="'c' + i" show-overflow-tooltip min-width="90"></el-table-column>
            </el-table>
            <pre v-else-if="kindOf(panes[0]) === 'json'" class="json">{{ panes[0].text }}</pre>
            <spdiag-tree v-else-if="kindOf(panes[0]) === 'tree' && !panes[0].rawView"
                         :text="panes[0].text"></spdiag-tree>
            <pre v-else class="txt">{{ panes[0].text }}</pre>
          </div>
        </div>

        <div class="pane" v-if="compareMode" :class="{ focus: activePane === 1 }"
             @click="activePane = 1">
          <div class="pane-head">
            <el-tag size="small" type="info">下</el-tag>
            <el-select v-model="panes[1].ts" filterable placeholder="选择 run" size="small"
                       style="width: 190px" @change="loadFiles(1)">
              <el-option v-for="r in runs" :key="r.ts" :label="r.ts" :value="r.ts"></el-option>
            </el-select>
            <el-select v-model="panes[1].file" filterable placeholder="选择文件" size="small"
                       style="flex: 1; min-width: 320px">
              <el-option-group v-for="g in groupedFiles(1)" :key="g.label" :label="g.label + ' (' + g.opts.length + ')'">
                <el-option v-for="o in g.opts" :key="o" :label="o" :value="o"></el-option>
              </el-option-group>
            </el-select>
            <el-radio-group v-if="kindOf(panes[1]) === 'tree'" v-model="panes[1].rawView" size="small">
              <el-radio-button :value="false">树状图</el-radio-button>
              <el-radio-button :value="true">源文件</el-radio-button>
            </el-radio-group>
            <el-dropdown v-if="treeShortcut(1).length" @command="f => { panes[1].file = f; }">
              <el-button size="small" type="primary">查看树状图</el-button>
              <template #dropdown>
                <el-dropdown-menu>
                  <el-dropdown-item v-for="f in treeShortcut(1)" :key="f" :command="f">{{ f }}</el-dropdown-item>
                </el-dropdown-menu>
              </template>
            </el-dropdown>
          </div>
          <div class="pane-body" v-loading="panes[1].loading">
            <div v-if="!panes[1].file" class="hint" style="padding: 30px; text-align: center">选择文件查看</div>
            <img v-else-if="kindOf(panes[1]) === 'img'"
                 :src="rawUrl(panes[1])">
            <el-table v-else-if="kindOf(panes[1]) === 'csv'" :data="panes[1].rows" size="small"
                      border style="width: 100%" :empty-text="panes[1].loading ? '加载中…' : '无数据'">
              <el-table-column v-for="(h, i) in panes[1].headers" :key="i" :label="h"
                               :prop="'c' + i" show-overflow-tooltip min-width="90"></el-table-column>
            </el-table>
            <pre v-else-if="kindOf(panes[1]) === 'json'" class="json">{{ panes[1].text }}</pre>
            <spdiag-tree v-else-if="kindOf(panes[1]) === 'tree' && !panes[1].rawView"
                         :text="panes[1].text"></spdiag-tree>
            <pre v-else class="txt">{{ panes[1].text }}</pre>
          </div>
        </div>
      </el-col>
    </el-row>
  </div>`,
  setup() {
    const loading = Vue.ref(false);
    const runs = Vue.ref([]);
    const compareMode = Vue.ref(false);
    const lockFile = Vue.ref(true);
    const activePane = Vue.ref(0);

    function newPane() {
      return { ts: "", files: [], file: "", loading: false, rawView: false,
               rows: [], headers: [], text: "" };
    }
    const panes = Vue.reactive([newPane(), newPane()]);

    function isActivePaneTs(ts) {
      return panes[activePane.value] && panes[activePane.value].ts === ts;
    }

    function pickRun(ts) { panes[activePane.value].ts = ts; loadFiles(activePane.value); }

    function groupedFiles(idx) {
      const groups = {};
      for (const f of panes[idx].files) {
        const g = _fileGroup(f);
        (groups[g] = groups[g] || []).push(f);
      }
      const order = ["csv", "图片", "spdiag树", "profiling", "日志", "json", "其他"];
      return order.filter(g => groups[g]).map(g => ({ label: g, opts: groups[g] }));
    }

    function kindOf(p) {
      if (!p.file) return "none";
      const base = p.file.split("/").pop().toLowerCase();
      if (/spdiag_tree\.[^.]+\.txt$/.test(base)) return "tree";
      const g = _fileGroup(p.file);
      if (g === "图片") return "img";
      if (g === "csv") return "csv";
      if (g === "json") return "json";
      return "txt";
    }

    function treeShortcut(idx) {
      // viewing the raw spdiag table -> jump to generated tree files
      const base = (panes[idx].file || "").split("/").pop();
      if (base !== "spdiag.show.txt") return [];
      return panes[idx].files.filter(f => /spdiag_tree\.[^.]+\.txt$/.test(f.split("/").pop()));
    }

    function rawUrl(p) {
      return "/api/file?path=" + encodeURIComponent("outputs/" + p.ts + "/" + p.file) + "&mode=raw";
    }

    async function loadRuns() {
      loading.value = true;
      try {
        runs.value = (await api("GET", "/api/outputs")).outputs;
        for (const p of panes) {
          if (p.ts && !runs.value.some(r => r.ts === p.ts)) { p.ts = ""; p.file = ""; p.files = []; }
        }
      } catch (e) {
        ElementPlus.ElMessage.error(e.message);
      } finally {
        loading.value = false;
      }
    }

    async function loadFiles(idx) {
      const p = panes[idx];
      p.file = "";
      p.rows = []; p.headers = []; p.text = "";
      if (!p.ts) { p.files = []; return; }
      p.loading = true;
      try {
        const r = await api("GET", "/api/outputs/" + encodeURIComponent(p.ts) + "/tree");
        p.files = r.files;
        if (idx === 0) {
          // 默认选 benchmark_detail.csv（若存在）
          const detail = p.files.find(f => f === "benchmark_detail.csv");
          if (detail) p.file = detail;
        }
      } catch (e) {
        ElementPlus.ElMessage.error(e.message);
      } finally {
        p.loading = false;
      }
    }

    async function loadContent(idx) {
      const p = panes[idx];
      p.rows = []; p.headers = []; p.text = "";
      if (!p.ts || !p.file) return;
      const path = "outputs/" + p.ts + "/" + p.file;
      const kind = kindOf(p);
      p.loading = true;
      try {
        if (kind === "csv") {
          const r = await api("GET", "/api/csv?path=" + encodeURIComponent(path) + "&max_rows=3000");
          p.headers = r.headers;
          p.rows = r.rows.map(row => {
            const o = {};
            r.headers.forEach((h, i) => { o["c" + i] = row[i]; });
            return o;
          });
        } else if (kind === "json") {
          const r = await api("GET", "/api/file?path=" + encodeURIComponent(path));
          try { p.text = JSON.stringify(JSON.parse(r.content), null, 2); }
          catch (e) { p.text = r.content; }
        } else if (kind === "tree") {
          // full text (tree files are small and the parser needs the whole file)
          const r = await api("GET", "/api/file?path=" + encodeURIComponent(path));
          p.text = r.content;
        } else {
          const r = await api("GET", "/api/file?path=" + encodeURIComponent(path) + "&tail=131072");
          p.text = r.content;
        }
      } catch (e) {
        ElementPlus.ElMessage.error(e.message);
      } finally {
        p.loading = false;
      }
    }

    // 同文件锁定：上窗格文件变化 → 下窗格自动匹配同名文件（仅非空选择时）
    Vue.watch(() => panes[0].file, (f) => {
      loadContent(0);
      if (!f || !compareMode.value || !lockFile.value) return;
      const p1 = panes[1];
      if (p1.files.includes(f)) {
        if (p1.file !== f) p1.file = f;   // 触发下窗格 watch 加载
      } else if (p1.file) {
        p1.file = "";
        ElementPlus.ElMessage.warning("下窗格该 run 无同名文件：" + f);
      }
    });
    Vue.watch(() => panes[1].file, () => loadContent(1));

    Vue.onActivated(async () => {
      await loadRuns();
      if (store.resultsTarget) {
        activePane.value = 0;
        if (panes[0].ts !== store.resultsTarget) {
          panes[0].ts = store.resultsTarget;
          await loadFiles(0);
        }
        store.resultsTarget = null;
      } else if (!panes[0].ts && runs.value.length) {
        panes[0].ts = runs.value[0].ts;
        await loadFiles(0);
      }
    });

    return { runs, loading, compareMode, lockFile, activePane, panes,
             isActivePaneTs, pickRun, groupedFiles, kindOf, rawUrl, treeShortcut, loadRuns };
  },
};
