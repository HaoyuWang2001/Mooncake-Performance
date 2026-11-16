"use strict";
/* 配置维护视图（v5）：
   - 左：通用配置（configs 根 *.json）｜右：运行配置树（默认全折叠）
   - 点击文件名 -> 弹窗编辑器（校验/保存，未保存关闭需确认）；不再常驻编辑器
   运行框已移至“性能测试”页；本页只负责配置浏览与编辑。 */

const CfgNode = {
  name: "CfgNode",
  props: { node: { type: Object, required: true },
           depth: { type: Number, default: 0 } },
  template: `
  <div>
    <div class="cfg-row" :style="{ paddingLeft: (depth * 14 + 4) + 'px' }">
      <span v-if="node.isDir" class="tgl" @click="open = !open">{{ open ? "▾" : "▸" }}</span>
      <span v-else class="tgl tgl-dot"></span>
      <span class="cfg-name" :class="{ dir: node.isDir }" :title="node.path"
            @click="onName">{{ node.name }}</span>
      <span v-if="node.isDir" class="cfg-count">({{ node.count }})</span>
    </div>
    <div v-if="node.isDir" v-show="open">
      <cfg-node v-for="c in node.children" :key="c.path" :node="c"
                :depth="depth + 1"></cfg-node>
    </div>
  </div>`,
  setup(props) {
    // 默认全部折叠：目录需要逐层展开（文件多时不至于满屏刷出）
    const open = Vue.ref(false);
    const openFile = Vue.inject("openConfigFile", () => {});
    const onName = () => {
      if (props.node.isDir) open.value = !open.value;
      else openFile(props.node.path);
    };
    return { open, onName };
  },
};

const ConfigView = {
  template: `
  <div class="page">
    <div class="mc-page-head">
      <div>
        <h1 class="title">配置维护</h1>
        <p class="desc">编辑 cluster/global/profile 与测试场景配置；点击文件名在弹窗中编辑，保存前自动校验并备份，运行框的配置选择来自此处。</p>
      </div>
    </div>
    <el-row :gutter="14">
      <el-col :span="10">
        <el-card shadow="never">
          <template #header><b>通用配置</b>
            <span class="card-hint">configs/_ssh、_k8s 及根目录</span></template>
          <div class="newcfg-row">
            <el-input v-model="newCfgName" placeholder="新文件名，如 cluster.3nodes.json"
                      size="small" style="width: 220px"></el-input>
            <el-select v-model="newCfgSource" size="small" style="width: 230px" filterable
                       title="新文件的初始内容来源">
              <el-option v-for="t in newCfgSources" :key="t" :label="t" :value="t"></el-option>
            </el-select>
            <el-button size="small" type="primary" plain @click="newConfig">新建</el-button>
          </div>
          <div class="newcfg-hint">
            命名前缀约定：<code>cluster*</code> 进运行框 cluster 下拉，<code>global*</code>/<code>profile*</code> 同理
          </div>
          <div v-for="p in general" :key="p" class="leaf general-item" @click="openFile(p)">
            {{ p }}
          </div>
          <el-empty v-if="!general.length" description="无通用配置" :image-size="50"></el-empty>
        </el-card>
      </el-col>
      <el-col :span="14">
        <el-card shadow="never">
          <template #header>
            <div class="mc-toolbar">
              <b>运行配置</b>
              <span class="card-hint">目录默认折叠，点击文件名编辑</span>
            </div>
          </template>
          <div class="cfg-tree">
            <cfg-node v-for="root in treeRoots" :key="root.path" :node="root" :depth="0"></cfg-node>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-dialog v-model="dlgOpen" width="72%" top="6vh" class="cfg-dlg"
               :close-on-click-modal="false" :before-close="askClose">
      <template #header>
        <b>编辑配置</b>
        <span v-if="editPath" class="cfg-dlg-path">{{ editPath }}</span>
        <el-tag v-if="isNew" size="small" type="warning">新文件（未保存）</el-tag>
      </template>
      <el-input type="textarea" :rows="22" v-model="editContent" class="editor"></el-input>
      <el-alert v-if="editMsg.text" :type="editMsg.ok ? 'success' : 'error'"
                :title="editMsg.text" :closable="false" show-icon class="mt-2" />
      <template #footer>
        <el-button size="small" @click="askClose">取消</el-button>
        <el-button size="small" :disabled="!editPath" @click="validate">校验</el-button>
        <el-button size="small" type="primary" :disabled="!dirty" :loading="saving"
                   @click="save">保存</el-button>
      </template>
    </el-dialog>
  </div>
`,
  setup() {
    const treeRoots = Vue.ref([]);
    const general = Vue.ref([]);

    // 新建配置：默认基于 cluster.example.json 模板；候选 = 三个模板 + 现有通用配置
    const newCfgName = Vue.ref("");
    const newCfgSource = Vue.ref("configs/_ssh/cluster.example.json");
    const newCfgSources = Vue.ref(["configs/_ssh/cluster.example.json"]);

    const dlgOpen = Vue.ref(false);
    const newSession = Vue.ref(false);   // 新建（未保存）会话标记
    const editPath = Vue.ref("");
    const editContent = Vue.ref("");
    const editSnapshot = Vue.ref("");
    const editMsg = Vue.reactive({ text: "", ok: true });
    const saving = Vue.ref(false);
    const dirty = Vue.computed(() =>
      !!editPath.value && editContent.value !== editSnapshot.value);
    const isNew = Vue.computed(() => newSession.value);

    function toNode(treeNode) {
      const children = [
        ...treeNode.dirs.map(toNode),
        ...treeNode.files.map(f => ({ name: f.split("/").pop(), path: f, isDir: false })),
      ];
      return { name: treeNode.name, path: treeNode.path, isDir: true,
               count: _cfgTreeCount(treeNode), children };
    }
    function _cfgTreeCount(n) {
      return n.files.length + n.dirs.reduce((a, d) => a + _cfgTreeCount(d), 0);
    }

    async function loadAll() {
      try {
        const cfg = await api("GET", "/api/configs");
        store.cfgTree = cfg;
        general.value = cfg.general;
        newCfgSources.value = ["configs/_ssh/cluster.example.json",
                               "configs/_ssh/global.example.json",
                               "configs/_ssh/profile.example.json",
                               "configs/_k8s/cluster.example.json",
                               "configs/_k8s/global.example.json",
                               "configs/_k8s/profile.example.json",
                               ...cfg.general];
        if (!newCfgSources.value.includes(newCfgSource.value)) {
          newCfgSource.value = "configs/_ssh/cluster.example.json";
        }
        treeRoots.value = Object.values(cfg.categories)
          .sort((a, b) => a.tree.path.localeCompare(b.tree.path))
          .map(c => toNode(c.tree));
      } catch (e) {
        ElementPlus.ElMessage.error("加载配置失败: " + e.message);
      }
    }

    async function openFile(path) {
      try {
        const r = await api("GET", "/api/config?path=" + encodeURIComponent(path));
        editPath.value = path;
        editContent.value = r.content;
        editSnapshot.value = r.content;
        editMsg.text = "";
        newSession.value = false;
        dlgOpen.value = true;
      } catch (e) {
        ElementPlus.ElMessage.error(e.message);
      }
    }

    // 未保存关闭 -> 丢弃确认（破坏性丢弃未保存内容，确认键 danger）
    function askClose(done) {
      const close = () => {
        editMsg.text = "";
        dlgOpen.value = false;
        if (typeof done === "function") done();
      };
      if (!dirty.value) { close(); return; }
      ElementPlus.ElMessageBox.confirm(
        "有未保存的修改，丢弃并关闭？", "未保存的修改",
        { confirmButtonText: "丢弃修改", cancelButtonText: "继续编辑",
          type: "warning" })
        .then(() => close())
        .catch(() => { /* 继续编辑 */ });
    }

    // 新建配置：拉取复制源内容，以新文件名打开未保存的编辑会话；
    // 保存走现有 PUT /api/config（新文件 backup=None）
    async function newConfig() {
      const name = newCfgName.value.trim();
      if (!name.endsWith(".json")) {
        ElementPlus.ElMessage.warning("新文件名必须以 .json 结尾");
        return;
      }
      const path = "configs/" + name;
      if (general.value.includes(path)) {
        ElementPlus.ElMessage.warning("文件已存在：" + path);
        return;
      }
      try {
        const r = await api("GET", "/api/config?path=" +
                            encodeURIComponent(newCfgSource.value));
        editPath.value = path;
        editContent.value = r.content;
        editSnapshot.value = r.content;
        editMsg.ok = true;
        editMsg.text = "新文件（基于 " + newCfgSource.value + "，保存后创建）";
        newSession.value = true;
        dlgOpen.value = true;
      } catch (e) {
        ElementPlus.ElMessage.error("读取复制源失败: " + e.message);
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
        ElementPlus.ElMessage.success(
          "已保存" + (r.backup ? "（备份 " + r.backup + "）" : ""));
        editMsg.text = "";
        editSnapshot.value = editContent.value;
        newSession.value = false;
        dlgOpen.value = false;
        await loadAll();
      } catch (e) {
        editMsg.ok = false; editMsg.text = e.message;
      } finally {
        saving.value = false;
      }
    }

    Vue.provide("openConfigFile", openFile);
    Vue.onMounted(loadAll);
    // 每次切入页签都重新拉取：手动创建/修改的配置文件切回即自动可见
    Vue.onActivated(loadAll);

    return { treeRoots, general, openFile, newCfgName, newCfgSource, newCfgSources,
             newConfig, dlgOpen, editPath, editContent, editMsg, saving, dirty,
             isNew, askClose, validate, save, loadAll };
  },
};
