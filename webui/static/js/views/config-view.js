"use strict";
/* 配置维护视图：通用配置快捷卡片 + 文件层级树 + 在线编辑器 + 运行发起 */

function _treeCount(node) {
  return node.files.length + node.dirs.reduce((a, d) => a + _treeCount(d), 0);
}

const ConfigView = {
  template: `
  <div class="page">
    <div class="header-row">
      <span class="hint">维护配置 → 在树上选择文件或目录 → 点击「运行」发起 benchmark；编辑保存自动备份到 webui/backups/</span>
      <span class="spacer"></span>
      <el-button size="small" @click="loadAll" :loading="loading">刷新</el-button>
    </div>

    <el-row :gutter="14" class="quick-cards">
      <el-col :span="8" v-for="key in ['cluster','global','matrix']" :key="key">
        <el-card shadow="hover" class="quick-card" @click="openSpecial(key)">
          <div class="qc-title">
            <span>{{ key }}</span>
            <el-tag v-if="special(key).present" type="success" size="small">存在</el-tag>
            <el-tag v-else type="info" size="small">缺失</el-tag>
          </div>
          <div class="qc-sub">{{ special(key).path }}</div>
        </el-card>
      </el-col>
    </el-row>

    <el-row :gutter="14">
      <el-col :span="8">
        <el-card shadow="never" class="cfg-tree-card">
          <template #header><b>配置文件</b></template>
          <el-tree
            :data="treeData"
            :props="{ label: 'label', children: 'children' }"
            node-key="path"
            highlight-current
            :default-expanded-keys="expandedKeys"
            @node-click="onNodeClick">
          </el-tree>
        </el-card>
      </el-col>

      <el-col :span="16">
        <el-card shadow="never" v-if="target" style="margin-bottom: 14px">
          <template #header><b>运行</b></template>
          <div class="runbar-tags" style="margin-bottom: 10px">
            <el-tag :type="target.type === 'dir' ? 'primary' : 'success'">
              {{ target.type === 'dir' ? '目录' : '文件' }}
            </el-tag>
            <span style="font-family: monospace">{{ target.path }}</span>
            <el-tag v-if="target.type === 'dir'" type="info" size="small">
              将运行 {{ target.count }} 个配置（含 demo）
            </el-tag>
          </div>
          <el-form label-width="70px" size="small" inline>
            <el-form-item label="选项">
              <el-checkbox v-model="flags.dryRun">dry-run</el-checkbox>
              <el-checkbox v-model="flags.noWarmup">no-warmup</el-checkbox>
              <el-checkbox v-model="flags.noPlot">no-plot</el-checkbox>
            </el-form-item>
            <el-form-item label="protocol">
              <el-select v-model="flags.protocol" style="width: 110px">
                <el-option label="(配置值)" value=""></el-option>
                <el-option label="tcp" value="tcp"></el-option>
                <el-option label="rdma" value="rdma"></el-option>
                <el-option label="ub" value="ub"></el-option>
              </el-select>
            </el-form-item>
            <el-form-item label="cluster">
              <el-select v-model="flags.cluster" style="width: 220px">
                <el-option v-for="c in clusterCandidates" :key="c" :label="c" :value="c"></el-option>
              </el-select>
            </el-form-item>
            <el-form-item label="profile">
              <el-select v-model="flags.profile" style="width: 110px">
                <el-option label="auto" value="auto"></el-option>
              </el-select>
              <span class="hint" style="margin-left: 6px">{{ matrixHint }}</span>
            </el-form-item>
          </el-form>
          <el-alert v-if="warnings.length" type="warning" :closable="false" style="margin-bottom: 10px"
                    :title="warnings.join('；')"></el-alert>
          <el-button type="primary" size="large" style="width: 100%" :loading="starting"
                     @click="startRun">运行（跳转运行页）</el-button>
        </el-card>

        <el-card shadow="never">
          <template #header>
            <b>编辑器</b>
            <span v-if="editPath" style="margin-left: 10px; font-family: monospace; font-weight: 400">
              {{ editPath }}
            </span>
          </template>
          <el-input type="textarea" :rows="18" v-model="editContent" class="editor"
                    :placeholder="editPath ? '' : '点击左侧配置文件或上方通用配置卡片开始编辑'"></el-input>
          <div class="header-row" style="margin: 10px 0 0">
            <el-button size="small" @click="validate" :disabled="!editPath">校验</el-button>
            <el-button size="small" type="primary" @click="save" :disabled="!editPath" :loading="saving">保存</el-button>
            <span v-if="editMsg.text" :style="{ color: editMsg.ok ? 'var(--el-color-success)' : 'var(--el-color-error)',
                      fontSize: '12.5px' }">{{ editMsg.text }}</span>
          </div>
        </el-card>
      </el-col>
    </el-row>
  </div>`,
  setup() {
    const loading = Vue.ref(false);
    const treeData = Vue.ref([]);
    const expandedKeys = Vue.ref([]);
    const clusterCandidates = Vue.ref(["configs/cluster.json"]);
    const specialMap = Vue.ref({});
    const matrixPresent = Vue.ref(false);

    const target = Vue.ref(null);
    const flags = Vue.reactive({ dryRun: false, noWarmup: false, noPlot: false,
                                 protocol: "", cluster: "configs/cluster.json", profile: "auto" });
    const warnings = Vue.ref([]);
    const starting = Vue.ref(false);

    const editPath = Vue.ref("");
    const editContent = Vue.ref("");
    const editMsg = Vue.reactive({ text: "", ok: true });
    const saving = Vue.ref(false);

    function special(key) { return specialMap.value[key] || { path: "configs/" + key + ".json", present: false }; }
    const matrixHint = Vue.computed(() => matrixPresent.value
      ? "matrix.json 存在 → auto 即启用 profiling" : "无 matrix.json → profiling 关闭");

    function toTreeNodes(node) {
      const children = [
        ...node.dirs.map(toTreeNodes),
        ...node.files.map(f => ({ label: f.split("/").pop(), path: f, isDir: false })),
      ];
      return { label: node.name + " (" + _treeCount(node) + ")", path: node.path,
               isDir: true, count: _treeCount(node), children };
    }

    async function loadAll() {
      loading.value = true;
      try {
        const cfg = await api("GET", "/api/configs");
        store.cfgTree = cfg;
        const nodes = [];
        for (const cat of ["basic", "pipeline"]) {
          const t = cfg.categories[cat].tree;
          if (t) { nodes.push(toTreeNodes(t)); expandedKeys.value.push(t.path); }
        }
        treeData.value = nodes;
        specialMap.value = cfg.special;
        clusterCandidates.value = cfg.cluster_candidates.length ? cfg.cluster_candidates : ["configs/cluster.json"];
        if (!clusterCandidates.value.includes(flags.cluster)) flags.cluster = clusterCandidates.value[0];
        matrixPresent.value = cfg.special.matrix.present;
      } catch (e) {
        ElementPlus.ElMessage.error("加载配置树失败: " + e.message);
      } finally {
        loading.value = false;
      }
    }

    function onNodeClick(data) {
      if (data.isDir) {
        target.value = { type: "dir", path: data.path, count: data.count };
      } else {
        target.value = { type: "file", path: data.path, count: 1 };
        openFile(data.path);
      }
    }

    function openSpecial(key) { openFile(special(key).path); }

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
        editMsg.ok = false;
        editMsg.text = e.message;
      } finally {
        saving.value = false;
      }
    }

    async function startRun() {
      if (!target.value) return;
      starting.value = true;
      warnings.value = [];
      try {
        const r = await api("POST", "/api/run", {
          selector: { type: target.value.type, path: target.value.path },
          flags: { dry_run: flags.dryRun, no_warmup: flags.noWarmup, no_plot: flags.noPlot,
                   protocol: flags.protocol || null, cluster: flags.cluster,
                   global: "configs/global.json", profile: flags.profile },
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

    Vue.onMounted(() => { if (!store.cfgTree) loadAll(); });
    Vue.onActivated(() => { if (!store.cfgTree) loadAll(); });

    return { loading, treeData, expandedKeys, clusterCandidates, special, matrixHint,
             target, flags, warnings, starting, startRun,
             editPath, editContent, editMsg, saving, validate, save,
             loadAll, onNodeClick, openSpecial };
  },
};
