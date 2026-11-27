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
    <div class="mc-page-head">
      <div>
        <h1 class="title">性能测试</h1>
        <p class="desc">配置后创建实例（创建不运行）；在下方卡片单独运行、停止、删除，日志与产物在「实例详情」查看。实例可反复运行。</p>
      </div>
      <div class="actions">
        <el-button size="small" @click="drawerOpen = true">历史运行</el-button>
      </div>
    </div>
    <el-card shadow="never" class="runbox">
      <template #header>
        <div class="mc-toolbar">
          <b>实例模板</b>
          <span class="spacer"></span>
          <el-tooltip content="刷新配置（重新拉取 cluster / global / profile 候选与配置树）" placement="top">
            <el-button size="small" circle :loading="cfgLoading"
                       aria-label="刷新配置" @click="refreshConfigs">
              <el-icon><component is="Refresh"/></el-icon>
            </el-button>
          </el-tooltip>
        </div>
      </template>
      <div class="rb-row">
        <el-radio-group v-model="mode" size="small" @change="onModeChange">
          <el-radio-button value="ssh">真机</el-radio-button>
          <el-radio-button value="k8s">k8s</el-radio-button>
        </el-radio-group>
        <span class="rb-label rb-group">启动方式</span>
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
        <span class="rb-label rb-group">global</span>
        <el-select v-model="flags.global" size="small" style="width: 280px" filterable
                   placeholder="选择 global 配置">
          <el-option v-for="g in modeCandidates.global" :key="g.path || g"
                     :label="g.path || g" :value="g.path || g"></el-option>
        </el-select>
        <span class="rb-label rb-group">profile</span>
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
          <span class="rb-label rb-label-inline">global</span>
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
          <span class="rb-label rb-label-inline">global</span>
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
        </div>
      </template>
      <div v-if="mode === 'k8s'" class="rb-row">
        <span class="rb-label">镜像TAG</span>
        <el-input v-model="flags.imageTag" size="small" style="width: 240px"
                  :placeholder="clusterImage ? clusterImage.split(':').pop() : '沿用 cluster.json 的 TAG'"
                  :title="clusterImage"></el-input>
      </div>
      <div class="rb-params" v-loading="paramLoading">
        <div class="rb-params-title">启动参数（选中配置的解析值，供核对）</div>
        <div v-for="g in paramGroups" :key="g.title" class="rb-params-group">
          <div v-if="paramGroups.length > 1" class="rb-params-sub">{{ g.title }}</div>
          <el-descriptions :column="3" size="small" border>
            <el-descriptions-item v-for="(row, i) in g.rows" :key="i" :label="row[0]">
              <span class="mc-mono rb-param-v" :title="row[1]">{{ row[1] }}</span>
            </el-descriptions-item>
          </el-descriptions>
        </div>
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
        <el-checkbox v-model="flags.noReport" title="只跳过 HTML 报表（report.html + overview.html），不影响绘图与原始日志">no-report</el-checkbox>
        <span class="rb-opt">
          <el-checkbox v-model="flags.png" title="生成旧版 PNG 散点图（plot_latency）；默认不绘图，且必须同时开启『拉取原始日志』">PNG 绘图</el-checkbox>
          <span class="rb-opt-hint">必须与「拉取原始日志」同时开启</span>
        </span>
        <span class="rb-opt">
          <el-checkbox v-model="flags.pullRawLogs" title="把原始客户端日志也拉回本地（默认只拉远端产出的紧凑 stats.json）">拉取原始日志</el-checkbox>
          <span class="rb-opt-hint">可能导致拉取数据量过大</span>
        </span>
        <el-checkbox v-model="flags.keepIntermediates" title="报表成功生成后仍保留全部中间产物（stats/原始日志/kvtest 样本）；默认清理，report/ 与 profiling/ 始终保留">保留中间产物</el-checkbox>
        <span class="rb-opt">
          <el-checkbox v-model="flags.monitorMem" title="每节点启动内存监控 DaemonSet（mem.host.csv / mem.proc.csv）；环境内存紧张时关闭">内存监控</el-checkbox>
          <span class="rb-opt-hint">2G 节点上会与压测争内存</span>
        </span>
        <span class="rb-opt">
          <el-checkbox v-model="flags.monitorUrma" title="每节点采集 URMA 端口计数（urma.flits.csv，需宿主 mdcli）">URMA 端口监控</el-checkbox>
          <span class="rb-opt-hint">需要节点具备宿主工具</span>
        </span>
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
        <el-tooltip :disabled="!runHint" :content="runHint" placement="top">
          <span>
            <el-button type="primary" size="default" :loading="creating"
                       :disabled="!!runHint || creating || cleaning"
                       @click="createInstance">创建实例</el-button>
          </span>
        </el-tooltip>
        <span class="spacer"></span>
        <el-tooltip :disabled="!cleanupHint" :content="cleanupHint" placement="top">
          <span>
            <el-button type="danger" plain size="default"
                       :loading="cleaning"
                       :disabled="cleaning || creating || schedulerBusy || launchMode === 'rank_recall'"
                       @click="cleanup">清理远端</el-button>
          </span>
        </el-tooltip>
      </div>
      <div v-if="runHint" class="rb-run-hint">{{ runHint }}</div>
      <div class="rb-clean-hint">
        <b>危险</b>：clean-up 删除远端残留——k8s：配置的 namespace（pods/configmaps）
        与 controller workspace；SSH：cluster.json 所有节点的 remote_dir；
        本地 outputs/日志保留，操作不可恢复。
      </div>
      <el-alert v-if="warnings.length" type="warning" :closable="false"
                :title="warnings.join('；')"></el-alert>
    </el-card>

    <el-card shadow="never">
      <template #header>
        <div class="mc-toolbar">
          <b>实例（{{ instances.length }}）</b>
          <span class="spacer"></span>
          <el-tooltip content="刷新实例列表" placement="top">
            <el-button size="small" circle :loading="instLoading"
                       aria-label="刷新实例列表" @click="loadInstances">
              <el-icon><component is="Refresh"/></el-icon>
            </el-button>
          </el-tooltip>
        </div>
      </template>
      <div v-if="!instances.length" class="inst-empty">
        <el-empty description="还没有实例" :image-size="72">
          <el-button type="primary" size="small" @click="scrollToForm">打开选择配置</el-button>
        </el-empty>
      </div>
      <div v-else v-loading="instLoading" class="inst-list">
        <div v-for="it in instances" :key="it.id" class="inst-card">
          <div class="inst-head">
            <span class="inst-name" :title="it.name">{{ it.name }}</span>
            <el-tag size="small" :type="it.launch_mode === 'rank_recall' ? 'warning' : 'info'">
              {{ it.launch_mode === 'rank_recall' ? 'Rank+Recall' : '单任务' }}
            </el-tag>
            <el-tooltip v-if="it.snapshot" placement="top"
                        :content="'配置快照：创建于 ' + fmtTimeAbs(it.snapshot.created) + '；之后修改配置文件不影响该实例（点击查看）'">
              <el-tag size="small" type="success" effect="plain" class="snap-tag"
                      role="button" tabindex="0"
                      @click="openSnapshot(it)"
                      @keydown.enter.prevent="openSnapshot(it)"
                      @keydown.space.prevent="openSnapshot(it)">快照 {{ it.snapshot.files }}</el-tag>
            </el-tooltip>
            <el-popover v-if="it.status === 'running' && it.stopping" placement="top"
                        :width="300" trigger="hover" :show-after="150"
                        content="正在优雅停止：先通知客户端清理并回收远端进程，宽限期内完成；超时后才会强制结束。">
              <template #reference>
                <el-tag size="small" type="warning">停止中</el-tag>
              </template>
            </el-popover>
            <el-tag v-else size="small" :type="statusType(it.status)">{{ statusText(it.status) }}</el-tag>
            <span class="spacer"></span>
            <span class="inst-id mono">{{ it.id }}</span>
          </div>
          <div class="inst-meta">
            <span><b>创建</b> <span :title="fmtTimeAbs(it.created)">{{ fmtTimeRel(it.created) }}</span></span>
            <span><b>模式</b> {{ it.mode === 'k8s' ? 'k8s' : '真机' }}</span>
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
            <span><b>最近运行</b> <span class="mc-mono">{{ it.last_run.id }}</span></span>
            <span><b>耗时</b> <span class="mc-num">{{ Math.round(it.last_run.elapsed || 0) }}s</span></span>
            <span><b>rc</b> <span class="mc-num">{{ (it.last_run.rc === null || it.last_run.rc === undefined) ? "-" : it.last_run.rc }}</span></span>
            <span v-if="it.last_run.ts_dir">
              <b>产物</b>
              <a href="#" class="mono inst-link" @click.prevent="openResults(it.last_run.ts_dir)">{{ it.last_run.ts_dir }}</a>
            </span>
          </div>
          <div class="inst-actions">
            <el-tooltip :disabled="!instanceRunHint(it)" :content="instanceRunHint(it)" placement="top">
              <span>
                <el-button size="small" link type="primary"
                           :loading="busy[it.id] === 'run'"
                           :disabled="it.status === 'running' || schedulerBusy"
                           @click="runInstance(it)">运行</el-button>
              </span>
            </el-tooltip>
            <el-button size="small" link type="warning" v-if="it.status === 'running'"
                       :loading="busy[it.id] === 'stop'"
                       :disabled="busy[it.id] === 'stop'"
                       @click="stopInstance(it)">停止</el-button>
            <el-button size="small" link type="danger"
                       :disabled="it.status === 'running'"
                       @click="deleteInstance(it)">删除</el-button>
            <span class="spacer"></span>
            <el-button size="small" link type="primary" @click="openInstance(it)">详情</el-button>
          </div>
        </div>
      </div>
    </el-card>

    <!-- 配置选择弹窗 -->
    <el-dialog v-model="cfgDialog" title="选择测试配置" width="560px">
      <el-alert type="info" :closable="false" show-icon class="mb-2"
                title="勾选文件或目录（勾选目录 = 递归该目录全部配置）" />
      <div class="dlg-tree-box">
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
    <el-drawer v-model="drawerOpen" title="历史运行" size="560px" direction="rtl">
      <el-table :data="runs" size="small" highlight-current-row @row-click="onHistoryClick">
        <el-table-column prop="id" label="id" width="180" show-overflow-tooltip></el-table-column>
        <el-table-column label="状态" width="88">
          <template #default="scope">
            <el-tag :type="statusType(scope.row.status)" size="small">{{ statusText(scope.row.status) }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="rc" label="rc" width="56" class-name="mc-num"></el-table-column>
        <el-table-column label="耗时" width="72" class-name="mc-num">
          <template #default="scope">{{ Math.round(scope.row.elapsed) + "s" }}</template>
        </el-table-column>
        <el-table-column prop="cmd" label="命令" show-overflow-tooltip></el-table-column>
      </el-table>
    </el-drawer>
  </div>`,
  setup() {
    // ---- 表单持久化（刷新 / 重启 webui 后恢复上次的选项与配置）----
    const FORM_KEY = "mcweb-perf-form";
    function _loadPerfForm() {
      try {
        const v = JSON.parse(localStorage.getItem(FORM_KEY) || "null");
        return (v && typeof v === "object") ? v : null;
      } catch (e) { return null; }
    }
    const _savedForm = _loadPerfForm();
    const _savedFlags = (_savedForm && _savedForm.flags
                         && typeof _savedForm.flags === "object")
      ? _savedForm.flags : null;
    const mode = Vue.ref(_savedForm &&
      (_savedForm.mode === "ssh" || _savedForm.mode === "k8s")
      ? _savedForm.mode : "ssh");
    const launchMode = Vue.ref(_savedForm && _savedForm.launchMode === "rank_recall"
                               && mode.value === "k8s" ? "rank_recall" : "single");
    const cfgDialog = Vue.ref(false);
    const treeRoots = Vue.ref([]);
    const candidates = Vue.ref({ cluster: [], global: [], profile: [] });
    const cfgLoading = Vue.ref(false);
    const flags = Vue.reactive(Object.assign(
      { dryRun: false, noWarmup: false, noReport: false, png: false,
        imageTag: "",
        pullRawLogs: false, keepIntermediates: false,
        monitorMem: false, monitorUrma: false,
        verbose: false, protocol: "", cluster: "",
        global: "", profile: "", rankCluster: "",
        rankGlobal: "", recallCluster: "", recallGlobal: "" },
      _savedFlags || {}));
    const _savedSel = (_savedForm && _savedForm.sel
                       && typeof _savedForm.sel === "object")
      ? _savedForm.sel : null;
    let _selRestored = false;
    let _saveTimer = null;
    function _savePerfForm() {
      try {
        localStorage.setItem(FORM_KEY, JSON.stringify({
          v: 1, mode: mode.value, launchMode: launchMode.value,
          flags: { ...flags },
          sel: { dirs: selected.value.dirs.slice(),
                 files: selected.value.files.slice() },
        }));
      } catch (e) { /* ignore */ }
    }
    function _scheduleSave() {
      clearTimeout(_saveTimer);
      _saveTimer = setTimeout(_savePerfForm, 300);
    }
    const warnings = Vue.ref([]);
    const creating = Vue.ref(false);
    const cleaning = Vue.ref(false);
    const clusterImage = Vue.ref("");
    const instances = Vue.computed(() => store.instances || []);
    const instLoading = Vue.ref(false);
    const busy = Vue.reactive({});     // 卡片按钮忙态（id -> "run" | "stop"）
    const runs = Vue.computed(() => store.runs || []);   // 历史抽屉（/api/runs）
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

    // ---- 启动参数面板：解析所选 cluster / global / profile 的关键字段 ----
    const paramDocs = Vue.reactive({});     // path -> 解析后的对象 | null
    const paramLoading = Vue.ref(false);
    async function _loadParamDoc(path) {
      if (!path) return null;
      if (Object.prototype.hasOwnProperty.call(paramDocs, path)) return paramDocs[path];
      paramDocs[path] = null;
      try {
        const r = await api("GET", "/api/config?path=" + encodeURIComponent(path));
        const obj = typeof r.content === "string" ? JSON.parse(r.content) : r.content;
        paramDocs[path] = (obj && typeof obj === "object") ? obj : null;
      } catch (e) { paramDocs[path] = null; }
      return paramDocs[path];
    }
    async function loadParamDocs() {
      const paths = launchMode.value === "rank_recall"
        ? [flags.rankCluster, flags.rankGlobal, flags.recallCluster,
           flags.recallGlobal, flags.profile]
        : [flags.cluster, flags.global, flags.profile];
      paramLoading.value = true;
      try {
        await Promise.all([...new Set(paths.filter(Boolean))].map(_loadParamDoc));
      } finally { paramLoading.value = false; }
    }
    function _perfFmtBytes(n) {
      if (n === "" || n === null || n === undefined) return "-";
      const v = Number(n);
      if (!isFinite(v) || v <= 0) return String(n);
      if (v < 1024) return v + " B";
      if (v < 1024 * 1024) return (v / 1024).toFixed(1) + " KB";
      if (v < 1024 * 1024 * 1024) return (v / 1024 / 1024).toFixed(1) + " MB";
      return (v / 1024 / 1024 / 1024).toFixed(2) + " GB";
    }
    function _perfShortList(arr, n) {
      const a = Array.isArray(arr) ? arr : [];
      if (!a.length) return "-";
      const head = a.slice(0, n).join("、");
      return a.length > n ? head + " 等 " + a.length + " 项" : head;
    }
    function _perfEffectiveProto(glob) {
      const p = String(flags.protocol || "").trim();
      if (p) return p + "（命令行覆盖）";
      const raw = glob && glob.protocol;
      if (!raw) return "配置值";
      if (typeof raw === "string") return raw;
      const keys = Object.keys(raw);
      if (!keys.length) return "配置值";
      return keys.map(k => {
        const v = raw[k] || {};
        return v && v.device ? k + "（device=" + v.device + "）" : k;
      }).join("、");
    }
    function _perfClusterRows(doc) {
      if (!doc) return [["集群", "-"]];
      if (doc.nodes && typeof doc.nodes === "object") {
        const names = Object.keys(doc.nodes);
        const head = names.slice(0, 4).map(n =>
          n + "=" + ((doc.nodes[n] || {}).ip || "?")).join("、");
        const venv = doc.venv || (doc.venvs ? Object.keys(doc.venvs).join("、") : "");
        return [["cluster 节点", names.length ? names.length + " 节点 · " +
                 head + (names.length > 4 ? " 等" : "") : "-"],
                ["venv", venv || "-"]];
      }
      const k8s = doc.k8s || {};
      const ctrl = doc.controller || {};
      return [["controller", (ctrl.ssh_user || "?") + "@" + (ctrl.ip || "?")],
              ["namespace", k8s.namespace || "-"],
              ["镜像", k8s.image || "-"],
              ["host 日志根", k8s.host_log_root || "-"]];
    }
    function _perfGlobalRows(doc) {
      if (!doc) return [["global", "-"]];
      return [["master", [doc.master_type, doc.master_addr].filter(Boolean)
               .join(" · ") || "-"],
              ["metadata", doc.metadata_server || "-"],
              ["etcd", doc.etcd_endpoints || "-"],
              ["store 节点", _perfShortList(doc.mooncake_store_nodes, 6)],
              ["本地 buffer", _perfFmtBytes(doc.local_buffer_size)],
              ["store 容量", doc.store_total_size || "-"],
              ["remote_dir", doc.remote_dir || "-"]];
    }
    function _perfProfileRows(doc) {
      if (!doc) return [["profile", "-"]];
      const tools = doc.tools || {};
      const on = Object.keys(tools).filter(k =>
        tools[k] && tools[k].enabled !== false);
      return [["trigger", doc.trigger || "-"],
              ["监控项", on.length ? on.join("、") : "无"],
              ["outliers", (doc.outliers || {}).threshold_ms != null
                ? (doc.outliers || {}).threshold_ms + " ms" : "-"]];
    }
    const paramGroups = Vue.computed(() => {
      const opt = [];
      if (flags.dryRun) opt.push("dry-run");
      if (flags.noWarmup) opt.push("no-warmup");
      if (flags.noReport) opt.push("no-report");
      if (flags.png) opt.push("png");
      if (flags.pullRawLogs) opt.push("pull-raw-logs");
      if (flags.keepIntermediates) opt.push("keep-intermediates");
      opt.push(flags.monitorMem ? "内存监控" : "无内存监控");
      opt.push(flags.monitorUrma ? "URMA 监控" : "无 URMA 监控");
      if (flags.verbose) opt.push("verbose");
      const common = [
        ["模式", mode.value === "k8s" ? "k8s" : "真机"],
        ["启动方式", launchMode.value === "rank_recall" ? "Rank + Recall 并行" : "单任务"],
        ["有效协议", _perfEffectiveProto(paramDocs[flags.global])],
        ["已选配置", "目录 " + selected.value.dirs.length +
         " / 文件 " + selected.value.files.length],
        ["选项", opt.length ? opt.join("、") : "默认"],
      ];
      if (launchMode.value === "rank_recall") {
        return [
          { title: "rank", rows: [
            ..._perfClusterRows(paramDocs[flags.rankCluster]),
            ..._perfGlobalRows(paramDocs[flags.rankGlobal]),
          ]},
          { title: "recall", rows: [
            ..._perfClusterRows(paramDocs[flags.recallCluster]),
            ..._perfGlobalRows(paramDocs[flags.recallGlobal]),
          ]},
          { title: "公共", rows: [
            ...common,
            ..._perfProfileRows(paramDocs[flags.profile]),
          ]},
        ];
      }
      return [{ title: "启动参数", rows: [
        ...common,
        ..._perfClusterRows(paramDocs[flags.cluster]),
        ..._perfGlobalRows(paramDocs[flags.global]),
        ..._perfProfileRows(paramDocs[flags.profile]),
      ]}];
    });
    Vue.watch([() => flags.cluster, () => flags.global, () => flags.profile,
               () => flags.rankCluster, () => flags.rankGlobal,
               () => flags.recallCluster, () => flags.recallGlobal,
               () => launchMode.value], () => { loadParamDocs(); });
    Vue.watch([mode, launchMode, () => flags, selected], _scheduleSave,
              { deep: true });

    let servedDefaults = !!_savedForm;

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
      cfgLoading.value = true;
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
        // 首次加载：按存档回填上次勾选的目录 / 文件（路径不存在则忽略）
        if (!_selRestored && _savedSel) {
          const set = new Set([
            ...(Array.isArray(_savedSel.dirs) ? _savedSel.dirs : []),
            ...(Array.isArray(_savedSel.files) ? _savedSel.files : []),
          ]);
          if (set.size) _perfApplyChecked(roots, set);
        }
        _selRestored = true;
        treeRoots.value = roots;
        // 页签切换（onActivated）只刷新候选/树，绝不重置已选项；
        // 只有首次成功加载（且无存档）才应用精确同名默认值。
        if (!servedDefaults) {
          applyDefaults();
          servedDefaults = true;
        }
        loadParamDocs();
      } catch (e) {
        ElementPlus.ElMessage.error("加载配置失败: " + e.message);
      } finally {
        cfgLoading.value = false;
      }
    }

    function refreshConfigs() {
      Object.keys(paramDocs).forEach(k => { delete paramDocs[k]; });
      loadConfigs();
    }

    function clearSel() {
      const walk = (n) => { n.checked = false; (n.children || []).forEach(walk); };
      treeRoots.value.forEach(walk);
    }

    const schedulerBusy = Vue.computed(() => {
      const sch = store.status.scheduler;
      return !!(sch && ["executing", "stopping"].includes(sch.state));
    });

    const cleanupHint = Vue.computed(() => {
      if (launchMode.value === "rank_recall")
        return "并行模式请切回单任务后按 namespace 分别清理";
      if (schedulerBusy.value)
        return "定时任务执行中，先停止调度后再清理";
      return "";
    });

    function instanceRunHint(it) {
      if (it.status === "running") return "该实例正在运行，先停止再运行";
      if (schedulerBusy.value) return "定时任务执行中，暂不能启动实例";
      return "";
    }

    function scrollToForm() {
      window.scrollTo({ top: 0, behavior: "smooth" });
    }

    // 创建实例前的前置提示（只校验表单，实例可随时创建，不占运行槽）
    const runHint = Vue.computed(() => {
      if (!hasSelection.value) return "未选择配置：先打开「选择配置」勾选文件或目录";
      if (flags.png && !flags.pullRawLogs)
        return "开启 PNG 绘图必须同时开启「拉取原始日志」";
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

    // ---- 实例管理（创建=快照，不运行；运行/停止/删除/详情各自独立）----
    function _instFlags() {
      return { mode: mode.value, launch_mode: launchMode.value,
               dry_run: flags.dryRun, no_warmup: flags.noWarmup,
               no_report: flags.noReport, png: flags.png,
               pull_raw_logs: flags.pullRawLogs,
               keep_intermediates: flags.keepIntermediates,
               monitor_mem: !!flags.monitorMem,
               monitor_urma: !!flags.monitorUrma,
               verbose: flags.verbose,
               protocol: flags.protocol || null,
               cluster: flags.cluster, global: flags.global,
               profile: flags.profile,
               rank_cluster: flags.rankCluster,
               rank_global: flags.rankGlobal,
               recall_cluster: flags.recallCluster,
               recall_global: flags.recallGlobal,
               image_tag: flags.imageTag || null };
    }

    async function loadInstances() {
      instLoading.value = true;
      try {
        await refreshInstances();
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
      busy[it.id] = "run";
      warnings.value = [];
      try {
        const r = await api("POST", "/api/instances/" + it.id + "/run");
        if (r.warnings && r.warnings.length) warnings.value = r.warnings;
        ElementPlus.ElMessage.success("实例已启动");
        await loadInstances();
      } catch (e) {
        ElementPlus.ElMessage.error("启动失败: " + e.message);
      } finally { delete busy[it.id]; }
    }

    async function stopInstance(it) {
      busy[it.id] = "stop";
      try {
        await api("POST", "/api/instances/" + it.id + "/stop");
        ElementPlus.ElMessage.success("已请求停止");
        await loadInstances();
      } catch (e) {
        ElementPlus.ElMessage.error("停止失败: " + e.message);
      } finally { delete busy[it.id]; }
    }

    async function deleteInstance(it) {
      try {
        await confirmDanger(
          "删除实例「" + it.name + "」？实例定义将被直接删除；运行记录、日志与 outputs 保留。",
          "删除实例");
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

    function openSnapshot(it) {
      if (!it || !it.snapshot) return;
      store.snapshotTarget = it.id;
      store.view = "instances";
    }

    function openResults(tsDir) {
      if (!tsDir) return;
      const rel = String(tsDir).replace(/^outputs[\\/]/, "");
      store.resultsTarget = rel.split("/")[0];
      store.view = "results";
    }

    async function reloadRuns() {
      try { await refreshRuns(); }
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
        reloadRuns();
        ElementPlus.ElMessage.success("清理完成（远端残留已删除）");
      } catch (e) {
        ElementPlus.ElMessage.error("清理失败: " + e.message);
      } finally {
        cleaning.value = false;
      }
    }

    Vue.onMounted(() => {
      loadConfigs().then(loadClusterImage);
      reloadRuns();
      loadInstances();
    });
    Vue.onActivated(() => {
      loadConfigs();
      reloadRuns();
      loadInstances();
    });

    return { mode, launchMode, cfgDialog, treeRoots, selected, hasSelection,
             paramDocs, paramGroups, paramLoading, refreshConfigs,
             modeCandidates, onModeChange, flags, warnings, creating, cleaning,
             clusterImage, clearSel, loadConfigs, cfgLoading, runHint, cleanupHint, schedulerBusy,
             statusType, statusText, fmtTimeRel, fmtTimeAbs,
             instanceRunHint, scrollToForm,
             instances, instLoading, busy, loadInstances, createInstance, runInstance,
             stopInstance, deleteInstance, openInstance, openSnapshot, openResults,
             runs, drawerOpen, onHistoryClick, cleanup };
  },
};
