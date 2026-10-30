"use strict";
/* SpdiagTree — 解析 spdiag_tree.<interface>.txt 并渲染可折叠/缩放的打点树。
   nested = 包含盒（子节点在父盒内纵向堆叠）；phase/leaf = 同父内按 order 横向并列。 */

const SdtNode = {
  name: "SdtNode",
  props: { node: { type: Object, required: true } },
  template: `
  <div class="sdt-box" :class="{ nodata: !node.stats }">
    <div class="sdt-title" @click="toggle">
      <span v-if="node.children.length" class="sdt-arrow">{{ node.open ? "▾" : "▸" }}</span>
      <span v-else class="sdt-arrow sdt-nochild">·</span>
      <span class="sdt-name">{{ node.point }}</span>
      <el-tag size="small" :type="kindTag" class="sdt-kind">{{ kindLabel }}</el-tag>
      <span v-if="node.order !== null" class="sdt-order">#{{ node.order }}</span>
      <el-tag v-if="!node.stats" size="small" type="info" effect="plain">无数据</el-tag>
    </div>
    <div class="sdt-stats" v-if="node.stats">
      <span class="s">ticks {{ node.stats.ticks }}</span>
      <span class="s">avg <b>{{ fmt(node.stats.avg) }}</b></span>
      <span class="s">p999 {{ fmt(node.stats.p999) }}</span>
      <span class="s">p9999 {{ fmt(node.stats.p9999) }}</span>
      <span class="s">max {{ fmt(node.stats.max) }}</span>
    </div>
    <div v-if="node.residual" class="sdt-res"
         :class="{ alert: node.residual.status === 'ALERT' }">
      残差 {{ fmt(node.residual.r) }} / parent {{ fmt(node.residual.parent) }}
      · {{ node.residual.status === 'ALERT' ? 'ALERT（打点缺失/嵌套变更?）' : 'OK' }}
    </div>
    <div v-if="node.glue && (node.open || !node.children.length)" class="sdt-glue">
      glue: {{ node.glue }}
    </div>
    <div v-if="node.open && rowKids.length" class="sdt-row">
      <sdt-node v-for="c in rowKids" :key="c.point + c.module" :node="c"></sdt-node>
    </div>
    <div v-if="node.open && stackKids.length" class="sdt-stack">
      <sdt-node v-for="c in stackKids" :key="c.point + c.module" :node="c"></sdt-node>
    </div>
  </div>`,
  setup(props) {
    const toggle = () => {
      if (props.node.children.length) props.node.open = !props.node.open;
    };
    const rowKids = Vue.computed(() =>
      props.node.children.filter(c => c.kind === "PHASE" || c.kind === "LEAF"));
    const stackKids = Vue.computed(() =>
      props.node.children.filter(c => c.kind === "NESTED"));
    const kindTag = Vue.computed(() =>
      ({ ROOT: "primary", PHASE: "success", NESTED: "warning", LEAF: "info" }[props.node.kind] || "info"));
    const kindLabel = Vue.computed(() =>
      ({ ROOT: "顶层", PHASE: "阶段", NESTED: "嵌套", LEAF: "末段" }[props.node.kind] || props.node.kind));
    return { toggle, rowKids, stackKids, kindTag, kindLabel, fmt: _sdtFmtNs };
  },
};

const SpdiagTree = {
  name: "SpdiagTree",
  props: { text: { type: String, default: "" } },
  template: `
  <div class="sdtree">
    <div class="sdtree-bar">
      <el-button-group size="small">
        <el-button @click="zoomBy(-0.1)">−</el-button>
        <el-button disabled>{{ Math.round(scale * 100) }}%</el-button>
        <el-button @click="zoomBy(0.1)">＋</el-button>
        <el-button @click="scale = 1">重置</el-button>
      </el-button-group>
      <el-button size="small" @click="expandAll(true)">全部展开</el-button>
      <el-button size="small" @click="expandAll(false)">全部折叠</el-button>
      <span class="sdtree-meta">
        {{ state.meta.interface }} · node {{ state.meta.node }}
        · 残差 {{ alertCount }} ALERT / {{ resCount }} 项
        <span v-if="nodataCount" class="sdtree-dim">· {{ nodataCount }} 无数据</span>
      </span>
    </div>
    <div class="sdtree-scroll">
      <div class="sdtree-canvas" :style="{ transform: 'scale(' + scale + ')' }">
        <sdt-node v-if="state.root" :node="state.root"></sdt-node>
        <div v-else class="sdtree-empty">无法解析文本树（非 spdiag_tree 文件？）</div>
        <el-collapse v-if="state.invariants.length" class="sdtree-inv">
          <el-collapse-item :title="'invariants（对账等式 ' + state.invariants.length + '）'">
            <div v-for="(inv, i) in state.invariants" :key="i" class="sdtree-dim">{{ inv }}</div>
          </el-collapse-item>
        </el-collapse>
        <el-collapse v-if="state.unmatched.length" class="sdtree-inv">
          <el-collapse-item :title="'未挂载打点（' + state.unmatched.length + '）'">
            <div v-for="(u, i) in state.unmatched" :key="i" class="sdtree-dim mono">{{ u }}</div>
          </el-collapse-item>
        </el-collapse>
      </div>
    </div>
  </div>`,
  setup(props) {
    const state = Vue.reactive({ root: null, meta: {}, invariants: [], unmatched: [] });
    const scale = Vue.ref(1);

    function walk(n, fn) {
      fn(n);
      (n.children || []).forEach(c => walk(c, fn));
    }
    function reparse() {
      const p = parseTreeText(props.text);
      state.meta = p.meta;
      state.invariants = p.invariants;
      state.unmatched = p.unmatched;
      state.root = p.root ? Vue.reactive(p.root) : null;
    }
    Vue.watch(() => props.text, reparse, { immediate: true });

    const resCount = Vue.ref(0);
    const alertCount = Vue.ref(0);
    const nodataCount = Vue.ref(0);
    Vue.watch(() => props.text, () => {
      let r = 0, a = 0, nd = 0;
      if (state.root) walk(state.root, n => {
        if (n.residual) { r++; if (n.residual.status === "ALERT") a++; }
        if (!n.stats) nd++;
      });
      resCount.value = r; alertCount.value = a; nodataCount.value = nd;
    }, { immediate: true });

    function zoomBy(d) { scale.value = Math.min(2, Math.max(0.4, Math.round((scale.value + d) * 10) / 10)); }
    function expandAll(open) {
      if (state.root) walk(state.root, n => { if (n.children.length) n.open = open; });
    }
    return { state, scale, zoomBy, expandAll, resCount, alertCount, nodataCount };
  },
};
