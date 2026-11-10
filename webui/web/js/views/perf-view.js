"use strict";
/* 性能测试视图：运行框（模式/配置选择/三件套/选项）+ 日志查看（自适应高度）。
   运行发起与日志查看在同一组件内直调（消除跨组件 pendingRunId 时序 bug）。 */

function _perfTreeCount(node) {
  return node.files.length + node.dirs.reduce((a, d) => a + _perfTreeCount(d), 0);
}

/* 按运行模式过滤配置候选。owner 由后端给出（_ssh/_k8s/root），
   注意 mode 值是 "ssh"/"k8s"（无下划线）。字符串候选兼容旧后端。 */
function _perfModeCandidates(list, mode) {
  return (list || []).filter(c => {
    const owner = (c && typeof c === "object") ? c.owner : null;
    if (!owner) return true;
    if (mode === "ssh") return owner === "_ssh" || owner === "root";
    return owner === "_k8s";
  });
}

/* 默认只选精确同名配置（如 cluster.json）；变体（cluster.2node.json）
   仍保留在候选里，但不自动选中。没有同名文件则返回 ""。 */
function _perfPickDefault(list, fileName) {
  const arr = list || [];
  const hit = arr.find(c => {
    const p = (c && typeof c === "object") ? c.path : c;
    return p && p.split("/").pop() === fileName;
  });
  return hit ? (hit.path || hit) : "";
}

/* 配置树勾选状态按 path 跨刷新迁移（页签切换不丢选择）。 */
function _perfCheckedPaths(roots) {
  const set = new Set();
  const walk = (n) => {
    if (n.checked) set.add(n.path);
    (n.children || []).forEach(walk);
  };
  (roots || []).forEach(walk);
  return set;
}

function _perfApplyChecked(roots, set) {
  const walk = (n) => {
    if (set.has(n.path)) n.checked = true;
    (n.children || []).forEach(walk);
  };
  (roots || []).forEach(walk);
}

/* 目录勾选级联：勾选=收编整棵子树；取消=清空整棵子树。修复子项被
   逐一勾选后目录变 indeterminate、取消目录却残留子项的问题。 */
function _perfSetSubtree(node, v) {
  node.checked = v;
  (node.children || []).forEach(c => _perfSetSubtree(c, v));
}

/* 是否需要拉取日志：running 必拉；从 running 变为终态的那一次也必须拉，
   否则快速结束的 run 会因首次空日志把 logEof 置 true 而永远跳过。 */
function _perfShouldFetchLog(prevStatus, status, logEof) {
  if (status === "running") return true;
  return prevStatus === "running" || !logEof;
}

const SelNode = {
  name: "SelNode",
  props: { node: { type: Object, required: true },
           parentSel: { type: Boolean, default: false },
           depth: { type: Number, default: 0 } },
  template: `
  <div>
    <div class="cfg-row" :style="{ paddingLeft: (depth * 14 + 4) + 'px' }">
      <span v-if="node.isDir" class="tgl" @click="toggle(node.path)">{{ open ? "▾" : "▸" }}</span>
      <span v-else class="tgl tgl-dot"></span>
      <el-checkbox v-if="node.isDir"
                   :model-value="node.checked"
                   :indeterminate="!node.checked && hasSel"
                   @update:model-value="setSubtree" />
      <el-checkbox v-else
                   :model-value="node.checked || parentSel"
                   :disabled="parentSel"
                   @update:model-value="v => { node.checked = v; }" />
      <span class="cfg-name" :class="{ dir: node.isDir }" :title="node.path"
            @click="toggle(node.path)">{{ node.name }}</span>
      <span v-if="node.isDir" class="cfg-count">({{ node.count }})</span>
    </div>
    <div v-if="node.isDir" v-show="open">
      <sel-node v-for="c in node.children" :key="c.path" :node="c"
                :parent-sel="parentSel || node.checked"
                :depth="depth + 1"></sel-node>
    </div>
  </div>`,
  setup(props) {
    // 手风琴展开约束：展开状态来自注入的共享链（见 perf-view 的
    // cfgTreeNav），展开某目录时只保留「根→该目录」一条链，兄弟与
    // 父辈兄弟自动回收；无注入时退化为独立开关（组件独立可用）
    const nav = Vue.inject("cfgTreeNav", null);
    const localOpen = Vue.ref(false);
    const open = Vue.computed(() =>
      nav ? nav.expandedPaths.value.has(props.node.path) : localOpen.value);
    const toggle = (path) => {
      if (nav) { nav.toggleDir(path); return; }
      localOpen.value = !localOpen.value;
    };
    const hasSel = Vue.computed(() => {
      const walk = (n) => n.checked || (n.children || []).some(walk);
      return walk(props.node);
    });
    return { open, toggle, hasSel, setSubtree: (v) => _perfSetSubtree(props.node, v) };
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
      </div>
      <div class="rb-row">
        <span class="rb-label">cluster</span>
        <el-select v-model="flags.cluster" size="small" style="width: 280px" filterable
                   placeholder="选择 cluster 配置">
          <el-option v-for="c in modeCandidates.cluster" :key="c.path || c"
                     :label="c.path || c" :value="c.path || c"></el-option>
        </el-select>
        <span class="rb-label" style="margin-left: 16px">global</span>
        <el-select v-model="flags.global" size="small" style="width: 280px" filterable
                   placeholder="选择 global 配置">
          <el-option v-for="g in modeCandidates.global" :key="g.path || g"
                     :label="g.path || g" :value="g.path || g"></el-option>
        </el-select>
        <span class="rb-label" style="margin-left: 16px">profile</span>
        <el-select v-model="flags.profile" size="small" style="width: 240px"
                   placeholder="选择 profile 配置">
          <el-option v-for="m in modeCandidates.profile" :key="m.path || m"
                     :label="m.path || m" :value="m.path || m"></el-option>
        </el-select>
      </div>
      <div v-if="mode === 'k8s'" class="rb-row" style="margin-top: 2px">
        <span class="rb-label">image</span>
        <span :title="clusterImage" class="rb-image">{{ clusterImage || "-" }}</span>
      </div>
      <div class="rb-sel-head">
        <span class="rb-label">配置选择</span>
        <el-button size="small" @click="cfgDialog = true">选择配置</el-button>
        <el-button size="small" @click="clearSel">清空</el-button>
      </div>
      <div class="rb-selected">
        <div class="rb-sel-col">
          <div class="rb-sel-title">目录 <span class="rb-sel-count">{{ selected.dirs.length }}</span></div>
          <div v-if="!selected.dirs.length" class="rb-sel-empty">—</div>
          <div v-for="d in selected.dirs" :key="d" class="rb-sel-path" :title="d">{{ d }}</div>
        </div>
        <div class="rb-sel-col">
          <div class="rb-sel-title">文件 <span class="rb-sel-count">{{ selected.files.length }}</span></div>
          <div v-if="!selected.files.length" class="rb-sel-empty">—</div>
          <div v-for="f in selected.files" :key="f" class="rb-sel-path" :title="f">{{ f }}</div>
        </div>
        <div class="rb-sel-note">目录先执行、文件后执行；已被目录收编的文件不重复列出</div>
      </div>
      <div class="rb-row">
        <span class="rb-label">选项</span>
        <el-checkbox v-model="flags.dryRun">dry-run</el-checkbox>
        <el-checkbox v-model="flags.noWarmup">no-warmup</el-checkbox>
        <el-checkbox v-model="flags.noPlot">no-plot</el-checkbox>
        <el-checkbox v-model="flags.verbose" title="详细日志：上传路径、逐文件拉取、kubectl 命令、pod 详情">verbose</el-checkbox>
        <span class="rb-label rb-label-inline">协议</span>
        <el-select v-model="flags.protocol" size="small" style="width: 130px">
          <el-option label="配置值" value=""></el-option>
          <el-option label="tcp" value="tcp"></el-option>
          <el-option label="rdma" value="rdma"></el-option>
          <el-option label="ub" value="ub"></el-option>
        </el-select>
      </div>
      <div class="rb-row rb-btn-row">
        <el-button type="primary" size="default" style="width: 110px" :loading="starting"
                   :disabled="!!runHint || busy || cleaning || starting"
                   @click="startRun">RUN</el-button>
        <el-button type="warning" size="default" style="width: 110px"
                   :loading="stopLoading"
                   :disabled="meta.status !== 'running' || stopLoading"
                   @click="stop">{{ stopLoading ? "STOP..." : "STOP" }}</el-button>
        <span class="spacer"></span>
        <el-button type="danger" plain size="default" style="width: 110px"
                   :loading="cleaning"
                   :disabled="busy || cleaning || starting"
                   @click="cleanup">CLEAN-UP</el-button>
      </div>
      <div v-if="runHint" class="rb-run-hint">{{ runHint }}</div>
      <div class="rb-clean-hint">
        <b>危险</b>：clean-up 删除远端残留——k8s：配置的 namespace（pods/configmaps）
        与 controller workspace；SSH：cluster.json 所有节点的 remote_dir；
        本地 outputs/日志保留，操作不可恢复。
      </div>
      <el-alert v-if="warnings.length" type="warning" :closable="false" style="margin-top: 8px"
                :title="warnings.join('；')"></el-alert>
    </el-card>

    <el-card shadow="never" style="margin-top: 14px">
      <template #header>
        <div class="header-row" style="margin: 0">
          <b>当前 RUN</b>
          <span class="spacer"></span>
          <el-button v-if="meta.ts_dir && meta.status !== 'running'" size="small" type="primary"
                     @click="gotoResults">查看运行结果</el-button>
          <el-button size="small" @click="drawerOpen = true">历史</el-button>
        </div>
      </template>
      <div class="cur-run">
        <div class="cr-row">
          <span class="cr-item cr-grow">
            <span class="cr-k">id:</span>
            <span class="cr-v mono" :title="meta.id || ''">{{ meta.id || "-" }}</span>
          </span>
          <span class="cr-item cr-w-status">
            <span class="cr-k">状态:</span>
            <span class="cr-v">
              <el-tag v-if="meta.stopping && meta.status === 'running'" type="warning" size="small">stopping（清理中）</el-tag>
              <el-tag v-else-if="meta.status" :type="statusType(meta.status)" size="small">{{ meta.status }}</el-tag>
              <span v-else>-</span>
            </span>
          </span>
          <span class="cr-item cr-w-elapsed">
            <span class="cr-k">耗时:</span>
            <span class="cr-v">{{ meta.id && meta.elapsed !== undefined ? Math.round(meta.elapsed) + "s" : "-" }}</span>
          </span>
          <span class="cr-item cr-w-rc">
            <span class="cr-k">rc:</span>
            <span class="cr-v">{{ (meta.rc === null || meta.rc === undefined) ? "-" : meta.rc }}</span>
          </span>
        </div>
        <div class="cr-row">
          <span class="cr-item cr-w-cmd">
            <span class="cr-k">命令:</span>
            <span class="cr-v mono" :title="meta.cmd || ''">{{ meta.cmd || "-" }}</span>
          </span>
          <span class="cr-item cr-w-out">
            <span class="cr-k">产物:</span>
            <span class="cr-v mono" :title="meta.ts_dir || ''">{{ meta.ts_dir || "-" }}</span>
          </span>
        </div>
      </div>
      <div class="log-toolbar">
        <el-button size="small" @click="copyLog">复制</el-button>
        <span class="log-wrap-toggle">
          <el-switch v-model="logWrap" size="small" />
          <span class="log-wrap-label" @click="logWrap = !logWrap">自动换行</span>
        </span>
        <span class="spacer"></span>
        <el-input v-model="searchQ" size="small" placeholder="搜索日志（高亮+跳转）"
                  clearable style="width: 230px" @keyup.enter="nextMatch" />
        <el-button size="small" :disabled="!matchCount" @click="prevMatch">↑</el-button>
        <el-button size="small" :disabled="!matchCount" @click="nextMatch">↓</el-button>
        <span v-if="searchQ" class="log-match-count">{{ matchCount ? (matchIdx + 1) + "/" + matchCount : "0/0" }}</span>
      </div>
      <div class="logview log-fixed" ref="logEl" :class="{ 'log-nowrap': !logWrap }">
        <div v-for="(ln, i) in logLines" :key="i"
             :class="[ln.cls, { 'ln-cur-match': i === curMatchLine }]">
          <template v-if="searchQ && hasHit(ln.text)">
            <span v-for="(seg, j) in lineSegs(ln.text)" :key="j"
                  :class="{ 'ln-mark': seg.hit }">{{ seg.t }}</span>
          </template>
          <template v-else>{{ ln.text }}</template>
        </div>
        <div v-if="!logLines.length && !meta.id" class="hint" style="padding: 40px; text-align: center">
          尚未运行——配置完成后点击 RUN
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
          确定（目录 {{ selected.dirs.length }} / 文件 {{ selected.files.length }}）
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
    const candidates = Vue.ref({ cluster: [], global: [], profile: [] });
    const flags = Vue.reactive({ dryRun: false, noWarmup: false, noPlot: false,
                                 verbose: false, protocol: "", cluster: "",
                                 global: "", profile: "" });
    const warnings = Vue.ref([]);
    const starting = Vue.ref(false);
    const cleaning = Vue.ref(false);
    const clusterImage = Vue.ref("");

    // 配置树手风琴约束：同一时刻仅保留「根→当前展开目录」一条展开链。
    // 展开某目录 -> 集合替换为其全部前缀级（兄弟、父辈兄弟随之回收）；
    // 折叠某目录 -> 移除自身及子孙（重新展开时不记忆之前的深度）。
    // 目录路径均以 "/" 结尾，前缀链即父子链，无需组件间传递祖先信息。
    const expandedPaths = Vue.ref(new Set());
    function toggleDir(path) {
      const cur = expandedPaths.value;
      if (cur.has(path)) {
        expandedPaths.value = new Set([...cur].filter(p => !p.startsWith(path)));
      } else {
        const parts = path.replace(/\/+$/, "").split("/");
        const chain = [];
        for (let i = 2; i <= parts.length; i++)   // "configs/<cat>/" 起始
          chain.push(parts.slice(0, i).join("/") + "/");
        expandedPaths.value = new Set(chain);
      }
    }
    Vue.provide("cfgTreeNav", { expandedPaths, toggleDir });

    // 最终选择（与 startRun 提交内容一致）：勾目录=收编该目录并停止下钻
    const selected = Vue.computed(() => {
      const acc = { dirs: [], files: [] };
      treeRoots.value.forEach(r => _perfCollectSel(r, acc));
      return acc;
    });
    const hasSelection = Vue.computed(() =>
      selected.value.dirs.length > 0 || selected.value.files.length > 0);
    const modeCandidates = Vue.computed(() => ({
      cluster: _perfModeCandidates(candidates.value.cluster, mode.value),
      global: _perfModeCandidates(candidates.value.global, mode.value),
      profile: _perfModeCandidates(candidates.value.profile, mode.value),
    }));

    let servedDefaults = false;

    // 仅首次加载 / 用户显式切换模式时调用；不触碰 mode 与选项开关
    function applyDefaults() {
      const c = modeCandidates.value;
      flags.cluster = _perfPickDefault(c.cluster, "cluster.json");
      flags.global = _perfPickDefault(c.global, "global.json");
      flags.profile = _perfPickDefault(c.profile, "profile.json");
    }

    function onModeChange() {
      applyDefaults();
    }

    function _toNode(treeNode) {
      const children = [
        ...treeNode.dirs.map(_toNode),
        ...treeNode.files.map(f => ({ name: f.split("/").pop(), path: f,
                                      isDir: false, checked: false, children: [] })),
      ];
      return { name: treeNode.name, path: treeNode.path, isDir: true,
               checked: false, count: _perfTreeCount(treeNode), children };
    }

    async function loadClusterImage() {
      clusterImage.value = "";
      const p = flags.cluster || "";
      if (mode.value !== "k8s" || !p) return;
      try {
        const r = await api("GET", "/api/config?path=" + encodeURIComponent(p));
        const obj = typeof r.content === "string" ? JSON.parse(r.content) : r.content;
        clusterImage.value = (obj && obj.k8s && obj.k8s.image) || "";
      } catch (e) { clusterImage.value = ""; }
    }
    Vue.watch([() => flags.cluster, () => mode.value], loadClusterImage);

    async function loadConfigs() {
      try {
        const checked = _perfCheckedPaths(treeRoots.value);
        const cfg = await api("GET", "/api/configs");
        candidates.value = {
          cluster: cfg.cluster_candidates,
          global: cfg.global_candidates,
          profile: cfg.profile_candidates,
        };
        const roots = Object.values(cfg.categories)
          .sort((a, b) => a.tree.path.localeCompare(b.tree.path))
          .map(c => _toNode(c.tree));
        _perfApplyChecked(roots, checked);
        treeRoots.value = roots;
        // 页签切换（onActivated）只刷新候选/树，绝不重置已选项；
        // 只有首次成功加载才应用精确同名默认值。
        if (!servedDefaults) {
          applyDefaults();
          servedDefaults = true;
        }
      } catch (e) {
        ElementPlus.ElMessage.error("加载配置失败: " + e.message);
      }
    }

    function clearSel() {
      const walk = (n) => { n.checked = false; (n.children || []).forEach(walk); };
      treeRoots.value.forEach(walk);
    }

    async function startRun() {
      const acc = { dirs: selected.value.dirs.slice(),
                    files: selected.value.files.slice() };
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
                   no_plot: flags.noPlot, verbose: flags.verbose,
                   protocol: flags.protocol || null,
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
    // 日志高度固定（禁止拖动）：不再使用 loadHeight/drag-handle
    const logWrap = Vue.ref(true);        // 自动换行（默认开，与既有 pre-wrap 一致）
    const searchQ = Vue.ref("");          // 日志搜索词
    const matchIdx = Vue.ref(0);          // 当前命中序号
    const runs = Vue.ref([]);
    const stopping = Vue.ref(false);
    const drawerOpen = Vue.ref(false);
    const currentId = Vue.ref(null);

    const busy = Vue.computed(() => {
      if (store.status.active_run) return true;
      if (meta.value.status === "running") return true;
      const sch = store.status.scheduler;
      return !!(sch && ["executing", "stopping"].includes(sch.state));
    });

    // RUN 阻塞原因（小字提示问题点）；busy 类状态不提示
    const runHint = Vue.computed(() => {
      if (busy.value || starting.value || cleaning.value) return "";
      if (!hasSelection.value) return "未选择配置：先打开「选择配置」勾选文件或目录";
      if (!flags.cluster) return "未选择 cluster 配置";
      if (!flags.global) return "未选择 global 配置";
      if (!flags.profile) return "未选择 profile 配置";
      return "";
    });
    // STOP 的 loading/文案只在「仍在运行且停止中」时生效——后端 run.stopping
    // 在完成时不重置，若直接绑定会永远停在 STOP...（本次修复点）
    const stopLoading = Vue.computed(() =>
      stopping.value || (!!meta.value.stopping && meta.value.status === "running"));

    const viewingHistory = Vue.computed(() =>
      !!(currentId.value && meta.value.id && meta.value.id !== currentId.value));

    function statusType(s) {
      return { running: "primary", done: "success", failed: "danger",
               stopped: "warning", aborted: "warning" }[s] || "info";
    }

    function classify(ln) {
      if (/\bFAIL\b|\bERROR\b/.test(ln)) return "ln-err";
      if (/\bWARN\b/.test(ln)) return "ln-warn";
      if (/SUMMARY: all .* OK|queue finished/.test(ln)) return "ln-ok";
      return "";
    }

    function appendLog(text) {
      if (!text) return;
      const el = logEl.value;
      const wasBottom = !el || (el.scrollHeight - el.scrollTop - el.clientHeight < 60);
      const lines = (logRem.value + text).split("\n");
      logRem.value = lines.pop();
      for (const ln of lines) logLines.value.push({ text: ln, cls: classify(ln) });
      if (logLines.value.length > 5000) logLines.value.splice(0, logLines.value.length - 5000);
      if (wasBottom) Vue.nextTick(() => {
        if (logEl.value) logEl.value.scrollTop = logEl.value.scrollHeight;
      });
    }

    // ---- 日志搜索/复制（命中行高亮 + 跳转）----
    const matchLines = Vue.computed(() => {
      const q = searchQ.value.trim().toLowerCase();
      if (!q) return [];
      const hits = [];
      logLines.value.forEach((ln, i) => {
        if (ln.text.toLowerCase().includes(q)) hits.push(i);
      });
      return hits;
    });
    const matchCount = Vue.computed(() => matchLines.value.length);
    const curMatchLine = Vue.computed(() =>
      matchCount.value ? matchLines.value[matchIdx.value % matchCount.value] : -1);

    function hasHit(text) {
      const q = searchQ.value.trim().toLowerCase();
      return !!q && text.toLowerCase().includes(q);
    }
    function lineSegs(text) {
      const q = searchQ.value.trim();
      if (!q) return [{ t: text, hit: false }];
      const lower = text.toLowerCase(), lq = q.toLowerCase();
      const out = [];
      let pos = 0;
      for (;;) {
        const at = lower.indexOf(lq, pos);
        if (at < 0) {
          if (pos < text.length) out.push({ t: text.slice(pos), hit: false });
          break;
        }
        if (at > pos) out.push({ t: text.slice(pos, at), hit: false });
        out.push({ t: text.slice(at, at + q.length), hit: true });
        pos = at + q.length;
      }
      return out.length ? out : [{ t: text, hit: false }];
    }
    function scrollToCurMatch() {
      Vue.nextTick(() => {
        const el = logEl.value, idx = curMatchLine.value;
        if (!el || idx < 0 || !el.children[idx]) return;
        el.children[idx].scrollIntoView({ block: "center" });
      });
    }
    function nextMatch() {
      if (!matchCount.value) return;
      matchIdx.value = (matchIdx.value + 1) % matchCount.value;
      scrollToCurMatch();
    }
    function prevMatch() {
      if (!matchCount.value) return;
      matchIdx.value = (matchIdx.value - 1 + matchCount.value) % matchCount.value;
      scrollToCurMatch();
    }
    Vue.watch(searchQ, () => { matchIdx.value = 0; scrollToCurMatch(); });
    async function copyLog() {
      const text = logLines.value.map(l => l.text).join("\n");
      try {
        await navigator.clipboard.writeText(text);
        ElementPlus.ElMessage.success("日志已复制");
      } catch (e) {
        try {
          const ta = document.createElement("textarea");
          ta.value = text;
          document.body.appendChild(ta);
          ta.select();
          document.execCommand("copy");
          document.body.removeChild(ta);
          ElementPlus.ElMessage.success("日志已复制");
        } catch (e2) {
          ElementPlus.ElMessage.error("复制失败: " + e2.message);
        }
      }
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
        const prevStatus = meta.value.status;
        const m = await api("GET", "/api/runs/" + id);
        if (meta.value.id !== id) return;
        meta.value = m;
        if (!_perfShouldFetchLog(prevStatus, m.status, logEof.value)) return;
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
      try {
        const r = await api("POST", "/api/runs/" + meta.value.id + "/stop");
        if (r && r.run) meta.value = r.run;
      } catch (e) { ElementPlus.ElMessage.error(e.message); }
      finally { stopping.value = false; }
    }

    async function cleanup() {
      if (busy.value) {
        ElementPlus.ElMessage.warning("当前有运行/调度任务，先停止后再清理");
        return;
      }
      try {
        await ElementPlus.ElMessageBox.confirm(
          "clean-up 会删除远端残留，且不可恢复：\n" +
          "· k8s：配置的 namespace（pods/configmaps）+ controller workspace\n" +
          "· SSH：cluster.json 中所有节点的 remote_dir\n" +
          "本地 outputs/日志保留。确定继续？",
          "clean-up（危险）",
          { type: "error", confirmButtonText: "清理", cancelButtonText: "取消",
            confirmButtonClass: "el-button--danger" });
      } catch (e) { return; }
      cleaning.value = true;
      warnings.value = [];
      try {
        const r = await api("POST", "/api/cleanup", {
          flags: { mode: mode.value, cluster: flags.cluster,
                   global: flags.global },
        });
        warnings.value = r.warnings || [];
        viewRun(r.run.id, true);
        refreshRuns();
      } catch (e) {
        ElementPlus.ElMessage.error("清理失败: " + e.message);
      } finally {
        cleaning.value = false;
      }
    }

    function gotoResults() {
      if (meta.value.ts_dir) {
        store.resultsTarget = meta.value.ts_dir.split("/").pop();
        store.view = "results";
      }
    }

    let timer = null, histTimer = null;
    Vue.onMounted(() => { loadConfigs().then(loadClusterImage); refreshRuns(); });
    Vue.onActivated(() => {
      loadConfigs();
      refreshRuns();
      timer = setInterval(pollRun, 1500);
      histTimer = setInterval(refreshRuns, 6000);
    });
    Vue.onDeactivated(() => { clearInterval(timer); clearInterval(histTimer); });

    return { mode, cfgDialog, treeRoots, selected, hasSelection, modeCandidates, onModeChange,
             flags, warnings, starting, cleaning, busy, clusterImage, clearSel, startRun, cleanup,
             meta, logLines, logEl, runs, stopping, drawerOpen, viewingHistory,
             statusType, stop, gotoResults, onHistoryClick,
             runHint, stopLoading, logWrap, searchQ, matchCount, matchIdx, curMatchLine,
             hasHit, lineSegs, nextMatch, prevMatch, copyLog };
  },
};
