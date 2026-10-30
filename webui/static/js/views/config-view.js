"use strict";
/* 配置维护视图（v3）：
   - 运行框（通栏最上方）：选中文件展示 / cluster / global / profile / 选项 + 运行
   - 左列：通用配置（configs 根 *.json，点击编辑）+ 运行配置（复选框树）
   - 右侧：编辑器
   复选框语义：勾目录=整目录收编（子孙 disabled+checked，不可单独取消）；勾文件=独立
   选中（父目录半选）；目录已选时文件勾选标志保留（取消目录后恢复独立选中），提交时
   按祖先过滤去重。提交：dirs（先）+ files（后）。 */

function _cfgHasSel(node) {
  if (node.checked) return true;
  return (node.children || []).some(_cfgHasSel);
}

function _cfgCollectSel(node, acc) {
  if (node.isDir && node.checked) { acc.dirs.push(node.path); return; }
  if (!node.isDir && node.checked) acc.files.push(node.path);
  for (const c of node.children || []) _cfgCollectSel(c, acc);
}

const CfgNode = {
  name: "CfgNode",
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
            @click="onName">{{ node.name }}</span>
      <span v-if="node.isDir" class="cfg-count">({{ node.count }})</span>
    </div>
    <div v-if="node.isDir" v-show="open">
      <cfg-node v-for="c in node.children" :key="c.path" :node="c"
                :parent-sel="parentSel || node.checked"
                :depth="depth + 1"></cfg-node>
    </div>
  </div>`,
  setup(props) {
    const open = Vue.ref(props.depth < 1);
    const hasSel = Vue.computed(() => _cfgHasSel(props.node));
    const openFile = Vue.inject("openConfigFile", () => {});
    const onName = () => { if (!props.node.isDir) openFile(props.node.path); };
    return { open, hasSel, onName };
  },
};

const ConfigView = {
  template: `
  <div class="page">
    <el-card shadow="never" class="runbox">
      <template #header><b>运行</b></template>
      <div class="rb-row">
        <span class="rb-label">文件</span>
        <div class="rb-files">
          <span class="rb-seg"><span class="rb-k">目录</span>{{ sel.dirs.length ? sel.dirs.join("，") : "—" }}</span>
          <span class="rb-seg"><span class="rb-k">文件</span>{{ sel.files.length ? sel.files.join("，") : "—" }}</span>
          <span class="rb-note">（目录先执行，文件后执行）</span>
        </div>
      </div>
      <div class="rb-row">
        <span class="rb-label">cluster</span>
        <el-select v-model="flags.cluster" size="small" style="width: 280px" filterable>
          <el-option v-for="c in clusterCandidates" :key="c" :label="c" :value="c"></el-option>
        </el-select>
        <span class="rb-label" style="margin-left: 24px">global</span>
        <el-select v-model="flags.global" size="small" style="width: 280px" filterable>
          <el-option v-for="g in globalCandidates" :key="g" :label="g" :value="g"></el-select>
        </el-select>
        <span class="rb-label" style="margin-left: 24px">profile</span>
        <el-select v-model="flags.profile" size="small" style="width: 240px">
          <el-option label="auto" value="auto"></el-option>
          <el-option v-for="m in matrixCandidates" :key="m" :label="m" :value="m"></el-option>
        </el-select>
        <span class="hint" style="margin-left: 8px">{{ matrixHint }}</span>
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
        <span class="rb-spacer"></span>
        <el-button type="primary" :loading="starting" :disabled="!sel.dirs.length && !sel.files.length"
                   @click="startRun">运行（跳转运行页）</el-button>
      </div>
      <el-alert v-if="warnings.length" type="warning" :closable="false" style="margin-top: 8px"
                :title="warnings.join('；')"></el-alert>
    </el-card>

    <el-row :gutter="14" style="margin-top: 14px">
      <el-col :span="8">
        <el-card shadow="never" style="margin-bottom: 14px">
          <template #header><b>通用配置</b><span class="hint" style="margin-left: 8px">configs 根目录</span></template>
          <div v-for="p in general" :key="p" class="leaf general-item" @click="openFile(p)">
            {{ p }}
          </div>
          <el-empty v-if="!general.length" description="无通用配置" :image-size="50"></el-empty>
        </el-card>

        <el-card shadow="never">
          <template #header><b>运行配置</b><span class="hint" style="margin-left: 8px">复选框选择运行范围；点击文件名可编辑</span></template>
          <div class="cfg-tree">
            <cfg-node v-for="root in treeRoots" :key="root.path" :node="root" :depth="0"></cfg-node>
          </div>
        </el-card>
      </el-col>

      <el-col :span="16">
        <el-card shadow="never">
          <template #header>
            <b>编辑器</b>
            <span v-if="editPath" style="margin-left: 10px; font-family: monospace; font-weight: 400">
              {{ editPath }}
            </span>
          </template>
          <el-input type="textarea" :rows="20" v-model="editContent" class="editor"
                    :placeholder="editPath ? '' : '点击通用配置或运行配置文件开始编辑'"></el-input>
          <div class="header-row" style="margin: 10px 0 0">
            <el-button size="small" @click="validate" :disabled="!editPath">校验</el-button>
            <el-button size="small" type="primary" @click="save" :disabled="!editPath" :loading="saving">保存</el-button>
            <span v-if="editMsg.text" :style="{ color: editMsg.ok ? 'var(--el-color-success)' : 'var(--el-color-error)',
                      fontSize: '12.5px' }">{{ editMsg.text }}</span>
            <span class="hint" style="margin-left: auto">保存自动备份到 webui/backups/</span>
          </div>
        </el-card>
      </el-col>
    </el-row>
  </div>`,
  setup() {
    const loading = Vue.ref(false);
    const treeRoots = Vue.ref([]);
    const general = Vue.ref([]);
    const clusterCandidates = Vue.ref([]);
    const globalCandidates = Vue.ref([]);
    const matrixCandidates = Vue.ref([]);

    const flags = Vue.reactive({ dryRun: false, noWarmup: false, noPlot: false,
                                 protocol: "", cluster: "configs/cluster.json",
                                 global: "configs/global.json", profile: "auto" });
    const warnings = Vue.ref([]);
    const starting = Vue.ref(false);

    const editPath = Vue.ref("");
    const editContent = Vue.ref("");
    const editMsg = Vue.reactive({ text: "", ok: true });
    const saving = Vue.ref(false);

    const sel = Vue.computed(() => {
      const acc = { dirs: [], files: [] };
      for (const r of treeRoots.value) _cfgCollectSel(r, acc);
      return acc;
    });
    const matrixHint = Vue.computed(() => matrixCandidates.value.length
      ? "matrix.json 存在 → auto 即启用 profiling" : "无 matrix.json → profiling 关闭");

    function toNode(treeNode) {
      const children = [
        ...treeNode.dirs.map(toNode),
        ...treeNode.files.map(f => ({ name: f.split("/").pop(), path: f,
                                      isDir: false, checked: false })),
      ];
      return { name: treeNode.name, path: treeNode.path, isDir: true,
               checked: false, count: _cfgTreeCount(treeNode), children };
    }
    function _cfgTreeCount(n) {
      return n.files.length + n.dirs.reduce((a, d) => a + _cfgTreeCount(d), 0);
    }

    async function loadAll() {
      loading.value = true;
      try {
        const cfg = await api("GET", "/api/configs");
        store.cfgTree = cfg;
        general.value = cfg.general;
        clusterCandidates.value = cfg.cluster_candidates.length
          ? cfg.cluster_candidates : ["configs/cluster.json"];
        globalCandidates.value = cfg.global_candidates.length
          ? cfg.global_candidates : ["configs/global.json"];
        matrixCandidates.value = cfg.matrix_candidates;
        if (!clusterCandidates.value.includes(flags.cluster)) flags.cluster = clusterCandidates.value[0];
        if (!globalCandidates.value.includes(flags.global)) flags.global = globalCandidates.value[0];
        if (flags.profile !== "auto" && !matrixCandidates.value.includes(flags.profile)) {
          flags.profile = matrixCandidates.value.includes("configs/matrix.json")
            ? "configs/matrix.json" : "auto";
        }
        treeRoots.value = Object.values(cfg.categories)
          .sort((a, b) => a.tree.path.localeCompare(b.tree.path))
          .map(c => toNode(c.tree));
      } catch (e) {
        ElementPlus.ElMessage.error("加载配置失败: " + e.message);
      } finally {
        loading.value = false;
      }
    }

    async function openFile(path) {
      try {
        const r = await api("GET", "/api/config?path=" + encodeURIComponent(path));
        editPath.value = path;
        editContent.value = r.content;
        editMsg.text = "";
      } catch (e) {
        ElementPlus.ElMessage.error(e.message);
      }
    }

    async function validate() {
      try {
        const r = await api("POST", "/api/config/validate",
                            { path: editPath.value, content: editContent.value });
        editMsg.ok = r.errors.length === 0;
        editMsg.text = editMsg.ok ? "校验通过" : r.errors.join("；");
      } catch (e) { editMsg.ok = false; editMsg.text = e.message; }
    }

    async function save() {
      saving.value = true;
      try {
        const r = await api("PUT", "/api/config",
                            { path: editPath.value, content: editContent.value });
        editMsg.ok = true;
        editMsg.text = "已保存" + (r.backup ? "（备份 " + r.backup + "）" : "");
        await loadAll();
      } catch (e) {
        editMsg.ok = false; editMsg.text = e.message;
      } finally {
        saving.value = false;
      }
    }

    async function startRun() {
      starting.value = true;
      warnings.value = [];
      try {
        const r = await api("POST", "/api/run", {
          selector: { dirs: sel.value.dirs, files: sel.value.files },
          flags: { dry_run: flags.dryRun, no_warmup: flags.noWarmup,
                   no_plot: flags.noPlot, protocol: flags.protocol || null,
                   cluster: flags.cluster, global: flags.global,
                   profile: flags.profile },
        });
        warnings.value = r.warnings || [];
        store.pendingRunId = r.run.id;
        store.view = "run";
      } catch (e) {
        ElementPlus.ElMessage.error("启动失败: " + e.message);
      } finally {
        starting.value = false;
      }
    }

    Vue.provide("openConfigFile", openFile);
    Vue.onMounted(() => { if (!store.cfgTree) loadAll(); });
    Vue.onActivated(() => { if (!store.cfgTree) loadAll(); });

    return { loading, treeRoots, general, clusterCandidates, globalCandidates,
             matrixCandidates, matrixHint, flags, warnings, starting, sel,
             startRun, openFile, editPath, editContent, editMsg, saving,
             validate, save, loadAll };
  },
};
