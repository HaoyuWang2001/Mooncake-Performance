"use strict";
/* 性能测试视图：运行框（模式/配置选择/三件套/选项）+ 日志查看（自适应高度）。
   运行发起与日志查看在同一组件内直调（消除跨组件 pendingRunId 时序 bug）。 */

function _perfCountSel(sel) {
  let total = 0;
  const count = (node) => {
    if (node.checked) { total += node.children ? node.count : 1; return; }
    node.children.forEach(count);
  };
  sel.forEach(count);
  return total;
}

const _SelNode = {
  name: "SelNode",
  props: { node: { type: Object, required: true },
           parentSel: { type: Boolean, default: false },
           depth: { type: Number, default: 0 } },
  template: `
  <div>
    <div class="cfg-row" :style="{ paddingLeft: (depth * 14 + 4) + 'px' }">
      <span v-if="node.isDir" class="tgl" @click="open = !open">{{ open ? "▾" : "▸" }}</span>
      <span v-else class="tgl tgl-dot"></span>
      <el-checkbox v-if="node.isDir"
                   :model-value="node.checked"
                   :indeterminate="!node.checked && hasSel"
                   @update:model-value="v => { node.checked = v; }" />
      <el-checkbox v-else
                   :model-value="node.checked || parentSel"
                   :disabled="parentSel"
                   @update:model-value="v => { node.checked = v; }" />
      <span class="cfg-name" :class="{ dir: node.isDir }" :title="node.path"
            @click="open = !open">{{ node.name }}</span>
      <span v-if="node.isDir" class="cfg-count">({{ node.count }})</span>
    </div>
    <div v-if="node.isDir" v-show="open">
      <sel-node v-for="c in node.children" :key="c.path" :node="c"
                :parent-sel="parentSel || node.checked"
                :depth="depth + 1"></sel-node>
    </div>
  </div>`,
  setup(props) {
    const open = Vue.ref(props.depth < 1);
    const hasSel = Vue.computed(() => {
      const walk = (n) => n.checked || n.children.some(walk);
      return walk(props.node);
    });
    return { open, hasSel };
  },
};

function _perfCollectSel(node, acc) {
  if (node.isDir && node.checked) { acc.dirs.push(node.path); return; }
  if (!node.isDir && node.checked) acc.files.push(node.path);
  for (const c of node.children || []) _perfCollectSel(c, acc);
}

const PerfView = {
  template: `
  <div class="page">
    <el-card shadow="never" class="runbox">
      <template #header><b>运行</b></template>
      <div class="rb-row">
        <el-radio-group v-model="mode" size="small" @change="onModeChange">
          <el-radio-button value="ssh">真机</el-radio-button>
          <el-radio-button value="k8s">k8s</el-radio-button>
        </el-radio-group>
        <el-button size="small" @click="cfgDialog = true">
          配置选择（已选 {{ selCount }} 个）
        </el-button>
        <el-button size="small" @click="clearSel">清空</el-button>
      </div>
      <div class="rb-row">
        <span class="rb-label">cluster</span>
        <el-select v-model="flags.cluster" size="small" style="width: 280px" filterable
                   placeholder="选择 cluster 配置">
          <el-option v-for="c in modeCandidates.cluster" :key="c.path"
                     :label="c.path" :value="c.path"></el-option>
        </el-select>
        <span class="rb-label" style="margin-left: 16px">global</span>
        <el-select v-model="flags.global" size="small" style="width: 280px" filterable
                   placeholder="选择 global 配置">
          <el-option v-for="g in modeCandidates.global" :key="g.path"
                     :label="g.path" :value="g.path"></el-option>
        </el-select>
        <span class="rb-label" style="margin-left: 16px">matrix</span>
        <el-select v-model="flags.profile" size="small" style="width: 240px"
                   placeholder="profiling matrix">
          <el-option label="auto" value="auto"></el-option>
          <el-option v-for="m in modeCandidates.matrix" :key="m.path"
                     :label="m.path" :value="m.path"></el-option>
        </el-select>
      </div>
      <div class="rb-row">
        <span class="rb-label">选项</span>
        <el-checkbox v-model="flags.dryRun">dry-run</el-checkbox>
        <el-checkbox v-model="flags.noWarmup">no-warmup</el-checkbox>
        <el-checkbox v-model="flags.noPlot">no-plot</el-checkbox>
        <el-select v-model="flags.protocol" size="small" style="width: 110px; margin-left: 12px">
          <el-option label="(协议=配置值)" value=""></el-option>
          <el-option label="tcp" value="tcp"></el-option>
          <el-option label="rdma" value="rdma"></el-option>
          <el-option label="ub" value="ub"></el-option>
        </el-select>
      </div>
      <div class="rb-row">
        <el-button type="primary" size="large" style="width: 100%" :loading="starting"
                   :disabled="selCount === 0"
                   @click="startRun">运行测试</el-button>
      </div>
      <el-alert v-if="warnings.length" type="warning" :closable="false" style="margin-top: 8px"
                :title="warnings.join('；')"></el-alert>
    </el-card>

    <el-card shadow="never" style="margin-top: 14px">
      <template #header>
        <div class="header-row" style="margin: 0">
          <b>当前 RUN</b>
          <span v-if="meta.status" class="status" :class="statusType(meta.status)">{{ meta.status }}</span>
          <span v-if="meta.id" style="margin-left: 10px; color: var(--el-text-color-secondary); font-size: 12.5px">
            {{ Math.round(meta.elapsed) }}s · rc={{ meta.rc === null ? '-' : meta.rc }}
          </span>
          <span class="spacer"></span>
          <el-button v-if="meta.status === 'running'" size="small" type="danger" @click="stop" :loading="stopping">停止</el-button>
          <el-button v-if="meta.ts_dir && meta.status !== 'running'" size="small" type="primary"
                     @click="gotoResults">查看运行结果</el-button>
          <el-button size="small" @click="drawerOpen = true">历史</el-button>
        </div>
      </template>
      <div class="logview log-lg" ref="logEl">
        <div v-for="(ln, i) in logLines" :key="i" :class="ln.cls">{{ ln.text }}</div>
        <div v-if="!logLines.length && !meta.id" class="hint" style="padding: 40px; text-align: center">
          尚未运行——配置完成后点击「运行测试」
        </div>
      </div>
    </el-card>

    <!-- 配置选择弹窗 -->
    <el-dialog v-model="cfgDialog" title="选择测试配置" width="560px">
      <el-alert type="info" :closable="false" show-icon style="margin-bottom: 10px"
                title="勾选文件或目录（勾选目录 = 递归该目录全部配置）" />
      <div style="max-height: 400px; overflow-y: auto; border: 1px solid var(--el-border-color-lighter); border-radius: 6px; padding: 8px">
        <sel-node v-for="root in treeRoots" :key="root.path" :node="root" :depth="0"></sel-node>
      </div>
      <template #footer>
        <el-button size="small" @click="clearSel">清空选择</el-button>
        <el-button size="small" type="primary" @click="cfgDialog = false">
          确定（{{ selCount }} 个）
        </el-button>
      </template>
    </el-dialog>

    <!-- 历史抽屉 -->
    <el-drawer v-model="drawerOpen" title="历史 Runs" size="560px" direction="rtl">
      <el-table :data="runs" size="small" highlight-current-row @row-click="onHistoryClick">
        <el-table-column prop="id" label="id" width="180" show-overflow-tooltip></el-table-column>
        <el-table-column label="状态" width="88">
          <template #default="scope">
            <el-tag :type="statusType(scope.row.status)" size="small">{{ scope.row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="rc" label="rc" width="56"></el-table-column>
        <el-table-column label="耗时" width="72">
          <template #default="scope">{{ Math.round(scope.row.elapsed) + "s" }}</template>
        </el-table-column>
        <el-table-column prop="cmd" label="命令" show-overflow-tooltip></el-table-column>
      </el-table>
    </el-drawer>
  </div>`,
  setup() {
    const mode = Vue.ref("ssh");
    const cfgDialog = Vue.ref(false);
    const treeRoots = Vue.ref([]);
    const candidates = Vue.ref({ cluster: [], global: [], matrix: [] });
    const flags = Vue.reactive({ dryRun: false, noWarmup: false, noPlot: false,
                                 protocol: "", cluster: "", global: "", profile: "auto" });
    const warnings = Vue.ref([]);
    const starting = Vue.ref(false);

    const selCount = Vue.computed(() => _perfCountSel(treeRoots.value));
    const modeCandidates = Vue.computed(() => {
      const f = (list) => list.filter(c => c.owner === mode.value ||
        c.owner === "root" && mode.value === "ssh");
      return { cluster: f(candidates.value.cluster),
               global: f(candidates.value.global),
               matrix: f(candidates.value.matrix) };
    });

    function onModeChange() {
      // reset trio to mode-default
      const c = modeCandidates.value;
      flags.cluster = c.cluster.length ? c.cluster[0].path : "";
      flags.global = c.global.length ? c.global[0].path : "";
      flags.profile = "auto";
    }

    function _toNode(treeNode) {
      const children = [
        ...treeNode.dirs.map(_toNode),
        ...treeNode.files.map(f => ({ name: f.split("/").pop(), path: f,
                                      isDir: false, checked: false })),
      ];
      return { name: treeNode.name, path: treeNode.path, isDir: true,
               checked: false, count: treeNode.files.length +
               treeNode.dirs.reduce((a, d) => a + d.files.length, 0),
               children };
    }

    async function loadConfigs() {
      try {
        const cfg = await api("GET", "/api/configs");
        candidates.value = {
          cluster: cfg.cluster_candidates,
          global: cfg.global_candidates,
          matrix: cfg.matrix_candidates,
        };
        treeRoots.value = Object.values(cfg.categories)
          .sort((a, b) => a.tree.path.localeCompare(b.tree.path))
          .map(c => _toNode(c.tree));
        onModeChange();
      } catch (e) {
        ElementPlus.ElMessage.error("加载配置失败: " + e.message);
      }
    }

    function clearSel() {
      const walk = (n) => { n.checked = false; (n.children || []).forEach(walk); };
      treeRoots.value.forEach(walk);
    }

    async function startRun() {
      const acc = { dirs: [], files: [] };
      treeRoots.value.forEach(r => _perfCollectSel(r, acc));
      if (!acc.dirs.length && !acc.files.length) {
        ElementPlus.ElMessage.warning("先选择配置文件或目录");
        return;
      }
      starting.value = true;
      warnings.value = [];
      try {
        const r = await api("POST", "/api/run", {
          selector: { dirs: acc.dirs, files: acc.files },
          flags: { mode: mode.value,
                   dry_run: flags.dryRun, no_warmup: flags.noWarmup,
                   no_plot: flags.noPlot, protocol: flags.protocol || null,
                   cluster: flags.cluster, global: flags.global,
                   profile: flags.profile },
        });
        warnings.value = r.warnings || [];
        viewRun(r.run.id, true);
        refreshRuns();
      } catch (e) {
        ElementPlus.ElMessage.error("启动失败: " + e.message);
      } finally {
        starting.value = false;
      }
    }

    // ---- 日志查看（原 run-view 逻辑合并到同一组件）----
    const meta = Vue.ref({});
    const logLines = Vue.ref([]);
    const logOffset = Vue.ref(0);
    const logRem = Vue.ref("");
    const logEof = Vue.ref(true);
    const logEl = Vue.ref(null);
    const runs = Vue.ref([]);
    const stopping = Vue.ref(false);
    const drawerOpen = Vue.ref(false);
    const currentId = Vue.ref(null);

    const viewingHistory = Vue.computed(() =>
      !!(currentId.value && meta.value.id && meta.value.id !== currentId.value));

    function statusType(s) {
      return { running: "primary", done: "success", failed: "danger",
               stopped: "warning", aborted: "warning" }[s] || "info";
    }

    function classify(ln) {
      if (/\\bFAIL\\b|\\bERROR\\b/.test(ln)) return "ln-err";
      if (/\\bWARN\\b/.test(ln)) return "ln-warn";
      if (/SUMMARY: all .* OK|queue finished/.test(ln)) return "ln-ok";
      return "";
    }

    function appendLog(text) {
      if (!text) return;
      const el = logEl.value;
      const wasBottom = !el || (el.scrollHeight - el.scrollTop - el.clientHeight < 60);
      const lines = (logRem.value + text).split("\\n");
      logRem.value = lines.pop();
      for (const ln of lines) logLines.value.push({ text: ln, cls: classify(ln) });
      if (logLines.value.length > 5000) logLines.value.splice(0, logLines.value.length - 5000);
      if (wasBottom) Vue.nextTick(() => {
        // page-scroll: scroll the outer el-main, not the log container
        const main = document.querySelector(".el-main");
        if (main) main.scrollTop = main.scrollHeight;
      });
    }

    function viewRun(id, live) {
      currentId.value = currentId.value || id;
      meta.value = { id };
      logLines.value = [];
      logOffset.value = 0;
      logRem.value = "";
      logEof.value = false;
      pollRun();
    }

    async function pollRun() {
      const id = meta.value.id;
      if (!id) return;
      try {
        const m = await api("GET", "/api/runs/" + id);
        if (meta.value.id !== id) return;
        meta.value = m;
        if (m.status !== "running" && logEof.value) return;
        const lg = await api("GET", "/api/runs/" + id + "/log?offset=" + logOffset.value);
        if (meta.value.id !== id) return;
        if (lg.content) appendLog(lg.content);
        logOffset.value = lg.offset;
        logEof.value = lg.eof;
        if (logEof.value && logRem.value && m.status !== "running") {
          logLines.value.push({ text: logRem.value, cls: classify(logRem.value) });
          logRem.value = "";
        }
      } catch (e) { /* run pruned or server hiccup */ }
    }

    async function refreshRuns() {
      try { runs.value = (await api("GET", "/api/runs")).runs; }
      catch (e) { /* ignore */ }
    }

    function onHistoryClick(row) {
      viewRun(row.id, row.status === "running");
      drawerOpen.value = false;
    }

    async function stop() {
      if (!meta.value.id) return;
      stopping.value = true;
      try { await api("POST", "/api/runs/" + meta.value.id + "/stop"); }
      catch (e) { ElementPlus.ElMessage.error(e.message); }
      finally { stopping.value = false; }
    }

    function gotoResults() {
      if (meta.value.ts_dir) {
        store.resultsTarget = meta.value.ts_dir.split("/").pop();
        store.view = "results";
      }
    }

    let timer = null, histTimer = null;
    Vue.onMounted(() => { loadConfigs(); refreshRuns(); });
    Vue.onActivated(() => {
      loadConfigs();
      refreshRuns();
      timer = setInterval(pollRun, 1500);
      histTimer = setInterval(refreshRuns, 6000);
    });
    Vue.onDeactivated(() => { clearInterval(timer); clearInterval(histTimer); });

    return { mode, cfgDialog, treeRoots, selCount, modeCandidates, onModeChange,
             flags, warnings, starting, clearSel, startRun,
             meta, logLines, logEl, runs, stopping, drawerOpen, viewingHistory,
             statusType, stop, gotoResults, onHistoryClick };
  },
};
