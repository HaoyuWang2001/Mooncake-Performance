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

/* 是否需要拉取日志（实例详情页共用）：running 必拉；从 running 变为终态
   的那一次也必须拉，否则快速结束的 run 会因首次空日志把 logEof 置 true
   而永远跳过。 */
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
        <span class="rb-label" style="margin-left: 20px">启动方式</span>
        <el-radio-group v-model="launchMode" size="small">
          <el-radio-button value="single">单任务</el-radio-button>
          <el-radio-button value="rank_recall" :disabled="mode !== 'k8s'">Rank + Recall 并行</el-radio-button>
        </el-radio-group>
      </div>
      <div v-if="launchMode === 'single'" class="rb-row">
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
      <template v-else>
        <div class="rb-row">
          <el-tag size="small" type="primary" style="width: 58px; justify-content: center">rank</el-tag>
          <span class="rb-label">cluster</span>
          <el-select v-model="flags.rankCluster" size="small" style="width: 300px" filterable
                     placeholder="选择 rank cluster">
            <el-option v-for="c in modeCandidates.cluster" :key="c.path || c"
                       :label="c.path || c" :value="c.path || c"></el-option>
          </el-select>
          <span class="rb-label" style="margin-left: 12px">global</span>
          <el-select v-model="flags.rankGlobal" size="small" style="width: 300px" filterable
                     placeholder="选择 rank global">
            <el-option v-for="g in modeCandidates.global" :key="g.path || g"
                       :label="g.path || g" :value="g.path || g"></el-option>
          </el-select>
        </div>
        <div class="rb-row">
          <el-tag size="small" type="success" style="width: 58px; justify-content: center">recall</el-tag>
          <span class="rb-label">cluster</span>
          <el-select v-model="flags.recallCluster" size="small" style="width: 300px" filterable
                     placeholder="选择 recall cluster">
            <el-option v-for="c in modeCandidates.cluster" :key="c.path || c"
                       :label="c.path || c" :value="c.path || c"></el-option>
          </el-select>
          <span class="rb-label" style="margin-left: 12px">global</span>
          <el-select v-model="flags.recallGlobal" size="small" style="width: 300px" filterable
                     placeholder="选择 recall global">
            <el-option v-for="g in modeCandidates.global" :key="g.path || g"
                       :label="g.path || g" :value="g.path || g"></el-option>
          </el-select>
        </div>
        <div class="rb-row">
          <span class="rb-label">profile</span>
          <el-select v-model="flags.profile" size="small" style="width: 300px"
                     placeholder="选择共享 profile 配置">
            <el-option v-for="m in modeCandidates.profile" :key="m.path || m"
                       :label="m.path || m" :value="m.path || m"></el-option>
          </el-select>
          <span class="rb-sel-note" style="margin-left: 12px">所选配置会按继承后的 work_mode 自动分到两个并行任务</span>
        </div>
      </template>
      <div v-if="mode === 'k8s' && launchMode === 'single'" class="rb-row" style="margin-top: 2px">
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
        <div class="rb-sel-note">
          {{ launchMode === 'rank_recall'
             ? '并行模式要求选择中同时包含 rank 与 recall；按有效 work_mode 自动分组'
             : '目录先执行、文件后执行；已被目录收编的文件不重复列出' }}
        </div>
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
        <el-button type="primary" size="default" style="width: 130px" :loading="creating"
                   :disabled="!!runHint || creating || cleaning"
                   @click="createInstance">创建实例</el-button>
        <span class="spacer"></span>
        <el-button type="danger" plain size="default" style="width: 110px"
                   :loading="cleaning"
                   :disabled="cleaning || creating || schedulerBusy || launchMode === 'rank_recall'"
                   :title="launchMode === 'rank_recall' ? '并行模式请切回单任务后按 namespace 分别清理' : ''"
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
          <b>实例（{{ instances.length }}）</b>
          <span class="spacer"></span>
          <el-button size="small" :loading="instLoading" @click="loadInstances">刷新</el-button>
          <el-button size="small" @click="drawerOpen = true">历史</el-button>
        </div>
      </template>
      <div v-if="!instances.length" class="hint" style="padding: 30px; text-align: center">
        还没有实例——配置完成后点击「创建实例」
      </div>
      <div class="inst-list">
        <div v-for="it in instances" :key="it.id" class="inst-card">
          <div class="inst-head">
            <span class="inst-name" :title="it.name">{{ it.name }}</span>
            <el-tag size="small" :type="it.launch_mode === 'rank_recall' ? 'warning' : 'info'">
              {{ it.launch_mode === 'rank_recall' ? 'Rank+Recall' : '单任务' }}
            </el-tag>
            <el-tag size="small" :type="it.stopping ? 'warning' : statusType(it.status)">
              {{ it.stopping && it.status === 'running' ? 'stopping' : it.status }}
            </el-tag>
            <span class="spacer"></span>
            <span class="inst-id mono">{{ it.id }}</span>
          </div>
          <div class="inst-meta">
            <span><b>mode</b> {{ it.mode }}</span>
            <template v-if="it.launch_mode !== 'rank_recall'">
              <span><b>cluster</b> {{ it.flags.cluster || "-" }}</span>
              <span><b>global</b> {{ it.flags.global || "-" }}</span>
            </template>
            <template v-else>
              <span><b>rank</b> {{ it.flags.rank_cluster || "-" }} / {{ it.flags.rank_global || "-" }}</span>
              <span><b>recall</b> {{ it.flags.recall_cluster || "-" }} / {{ it.flags.recall_global || "-" }}</span>
            </template>
            <span><b>配置</b> 目录 {{ it.selection.dirs.length }} / 文件 {{ it.selection.files.length }}</span>
          </div>
          <div v-if="it.last_run" class="inst-meta">
            <span><b>最近运行</b> {{ it.last_run.id }}</span>
            <span><b>耗时</b> {{ Math.round(it.last_run.elapsed || 0) }}s</span>
            <span><b>rc</b> {{ (it.last_run.rc === null || it.last_run.rc === undefined) ? "-" : it.last_run.rc }}</span>
            <span v-if="it.last_run.ts_dir">
              <b>产物</b>
              <a href="#" class="mono inst-link" @click.prevent="openResults(it.last_run.ts_dir)">{{ it.last_run.ts_dir }}</a>
            </span>
          </div>
          <div class="inst-actions">
            <el-button size="small" type="primary"
                       :loading="!!it._running"
                       :disabled="it.status === 'running' || schedulerBusy"
                       @click="runInstance(it)">运行</el-button>
            <el-button size="small" type="warning" v-if="it.status === 'running'"
                       :loading="!!it._stopping"
                       :disabled="!!it._stopping"
                       @click="stopInstance(it)">停止</el-button>
            <el-button size="small" type="danger" plain
                       :disabled="it.status === 'running'"
                       @click="deleteInstance(it)">删除</el-button>
            <span class="spacer"></span>
            <el-button size="small" @click="openInstance(it)">详情</el-button>
          </div>
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
    const launchMode = Vue.ref("single");
    const cfgDialog = Vue.ref(false);
    const treeRoots = Vue.ref([]);
    const candidates = Vue.ref({ cluster: [], global: [], profile: [] });
    const flags = Vue.reactive({ dryRun: false, noWarmup: false, noPlot: false,
                                 verbose: false, protocol: "", cluster: "",
                                 global: "", profile: "", rankCluster: "",
                                 rankGlobal: "", recallCluster: "",
                                 recallGlobal: "" });
    const warnings = Vue.ref([]);
    const creating = Vue.ref(false);
    const cleaning = Vue.ref(false);
    const clusterImage = Vue.ref("");
    const instances = Vue.ref([]);
    const instLoading = Vue.ref(false);
    const runs = Vue.ref([]);          // 历史抽屉（/api/runs）
    const drawerOpen = Vue.ref(false);

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

    // 最终选择（创建实例时提交的内容）：勾目录=收编该目录并停止下钻
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
      flags.rankCluster = _perfPickDefault(c.cluster, "cluster.16node-rank.json");
      flags.rankGlobal = _perfPickDefault(c.global, "global.rank.json");
      flags.recallCluster = _perfPickDefault(c.cluster, "cluster.16node-recall.json");
      flags.recallGlobal = _perfPickDefault(c.global, "global.recall.json");
    }

    function onModeChange() {
      if (mode.value !== "k8s") launchMode.value = "single";
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

    const schedulerBusy = Vue.computed(() => {
      const sch = store.status.scheduler;
      return !!(sch && ["executing", "stopping"].includes(sch.state));
    });

    // 创建实例前的前置提示（只校验表单，实例可随时创建，不占运行槽）
    const runHint = Vue.computed(() => {
      if (!hasSelection.value) return "未选择配置：先打开「选择配置」勾选文件或目录";
      if (launchMode.value === "rank_recall") {
        if (!flags.rankCluster) return "未选择 rank cluster 配置";
        if (!flags.rankGlobal) return "未选择 rank global 配置";
        if (!flags.recallCluster) return "未选择 recall cluster 配置";
        if (!flags.recallGlobal) return "未选择 recall global 配置";
        if (!flags.profile) return "未选择共享 profile 配置";
        return "";
      }
      if (!flags.cluster) return "未选择 cluster 配置";
      if (!flags.global) return "未选择 global 配置";
      if (!flags.profile) return "未选择 profile 配置";
      return "";
    });

    function statusType(s) {
      return { running: "primary", done: "success", failed: "danger",
               stopped: "warning", aborted: "warning" }[s] || "info";
    }

    // ---- 实例管理（创建=快照，不运行；运行/停止/删除/详情各自独立）----
    function _instFlags() {
      return { mode: mode.value, launch_mode: launchMode.value,
               dry_run: flags.dryRun, no_warmup: flags.noWarmup,
               no_plot: flags.noPlot, verbose: flags.verbose,
               protocol: flags.protocol || null,
               cluster: flags.cluster, global: flags.global,
               profile: flags.profile,
               rank_cluster: flags.rankCluster,
               rank_global: flags.rankGlobal,
               recall_cluster: flags.recallCluster,
               recall_global: flags.recallGlobal };
    }

    async function loadInstances() {
      instLoading.value = true;
      try {
        const r = await api("GET", "/api/instances");
        instances.value = r.instances || [];
      } catch (e) {
        ElementPlus.ElMessage.error("加载实例失败: " + e.message);
      } finally {
        instLoading.value = false;
      }
    }

    async function createInstance() {
      if (runHint.value) { ElementPlus.ElMessage.warning(runHint.value); return; }
      const acc = { dirs: selected.value.dirs.slice(),
                    files: selected.value.files.slice() };
      if (!acc.dirs.length && !acc.files.length) {
        ElementPlus.ElMessage.warning("先选择配置文件或目录");
        return;
      }
      let name = "";
      try {
        const r = await ElementPlus.ElMessageBox.prompt(
          "实例名称（留空 = 自动命名）", "创建实例",
          { inputValue: "", confirmButtonText: "创建", cancelButtonText: "取消" });
        name = (r.value || "").trim();
      } catch (e) { return; }
      creating.value = true;
      warnings.value = [];
      try {
        await api("POST", "/api/instances", {
          name: name,
          selector: { dirs: acc.dirs, files: acc.files },
          flags: _instFlags(),
        });
        ElementPlus.ElMessage.success("实例已创建");
        await loadInstances();
      } catch (e) {
        ElementPlus.ElMessage.error("创建失败: " + e.message);
      } finally {
        creating.value = false;
      }
    }

    async function runInstance(it) {
      it._running = true;
      warnings.value = [];
      try {
        const r = await api("POST", "/api/instances/" + it.id + "/run");
        if (r.warnings && r.warnings.length) warnings.value = r.warnings;
        ElementPlus.ElMessage.success("实例已启动");
        await loadInstances();
      } catch (e) {
        ElementPlus.ElMessage.error("启动失败: " + e.message);
      } finally { it._running = false; }
    }

    async function stopInstance(it) {
      it._stopping = true;
      try {
        await api("POST", "/api/instances/" + it.id + "/stop");
        ElementPlus.ElMessage.success("已请求停止");
        await loadInstances();
      } catch (e) {
        ElementPlus.ElMessage.error("停止失败: " + e.message);
      } finally { it._stopping = false; }
    }

    async function deleteInstance(it) {
      try {
        await ElementPlus.ElMessageBox.confirm(
          "删除实例「" + it.name + "」？\n运行记录、日志与 outputs 保留（不可恢复仅实例记录）。",
          "删除实例", { type: "warning", confirmButtonText: "删除",
                       cancelButtonText: "取消" });
      } catch (e) { return; }
      try {
        await api("DELETE", "/api/instances/" + it.id);
        ElementPlus.ElMessage.success("实例已删除");
        await loadInstances();
      } catch (e) {
        ElementPlus.ElMessage.error("删除失败: " + e.message);
      }
    }

    function openInstance(it) {
      const open = store.instancesOpen || [];
      if (!open.includes(it.id)) open.push(it.id);
      store.instancesOpen = open;
      store.instancesTarget = it.id;
      store.view = "instances";
    }

    function openResults(tsDir) {
      if (!tsDir) return;
      const rel = String(tsDir).replace(/^outputs[\\/]/, "");
      store.resultsTarget = rel.split("/")[0];
      store.view = "results";
    }

    async function refreshRuns() {
      try { runs.value = (await api("GET", "/api/runs")).runs; }
      catch (e) { /* ignore */ }
    }

    function onHistoryClick(row) {
      drawerOpen.value = false;
      const iid = row && row.meta && row.meta.instance;
      if (iid) {
        const open = store.instancesOpen || [];
        if (!open.includes(iid)) open.push(iid);
        store.instancesOpen = open;
        store.instancesTarget = iid;
        store.view = "instances";
      } else if (row && row.ts_dir) {
        openResults(row.ts_dir);
      }
    }

    async function cleanup() {
      if (schedulerBusy.value) {
        ElementPlus.ElMessage.warning("调度任务执行中，先停止调度后再清理");
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
        refreshRuns();
      } catch (e) {
        ElementPlus.ElMessage.error("清理失败: " + e.message);
      } finally {
        cleaning.value = false;
      }
    }

    let instTimer = null, histTimer = null;
    Vue.onMounted(() => {
      loadConfigs().then(loadClusterImage);
      refreshRuns();
      loadInstances();
    });
    Vue.onActivated(() => {
      loadConfigs();
      refreshRuns();
      loadInstances();
      instTimer = setInterval(loadInstances, 3000);
      histTimer = setInterval(refreshRuns, 8000);
    });
    Vue.onDeactivated(() => { clearInterval(instTimer); clearInterval(histTimer); });

    return { mode, launchMode, cfgDialog, treeRoots, selected, hasSelection,
             modeCandidates, onModeChange, flags, warnings, creating, cleaning,
             clusterImage, clearSel, runHint, schedulerBusy, statusType,
             instances, instLoading, loadInstances, createInstance, runInstance,
             stopInstance, deleteInstance, openInstance, openResults,
             runs, drawerOpen, onHistoryClick, cleanup };
  },
};
