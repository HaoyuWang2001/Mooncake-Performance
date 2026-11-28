#!/usr/bin/env node
/* report.html 的 app JS 行为冒烟（桩 DOM + 桩 ECharts）。
 *
 * 用法：
 *   node report_js_check.js <app.js> <data.json> [--switch-size KEY]
 *                           [--expect-trace 0|1]
 * 输出一行 JSON：{init_len, min_len, init_x0, switched_error, trace_visible}
 *
 * 背景：报表的时间线粒度（1s/1min）、仅 trace 的 size 块等逻辑只在 JS 里，
 * Python 单测覆盖不到；页面一旦抛异常整页白屏。本脚本用最小桩执行 app.js，
 * 记录 ECharts setOption 的序列做断言。
 */
"use strict";

const fs = require("fs");
const vm = require("vm");

function arg(name, fallback) {
  const i = process.argv.indexOf(name);
  return i >= 0 ? process.argv[i + 1] : fallback;
}

const appPath = process.argv[2];
const dataPath = process.argv[3];
if (!appPath || !dataPath) {
  console.error("usage: node report_js_check.js <app.js> <data.json> " +
                "[--switch-size KEY] [--expect-trace 0|1]");
  process.exit(2);
}
const switchSize = arg("--switch-size", null);
const expectTrace = arg("--expect-trace", null) === "1";
const tierLabel = arg("--tier-label", null);
const addObjKey = arg("--add-obj", null);
const useReal = process.argv.includes("--real");
const clickCombo = process.argv.includes("--click-combo");

const elements = {};
function makeEl(id) {
  const el = {
    id: id || "",
    style: {},
    className: "",
    _text: "",
    _html: "",
    children: [],
    title: "",
    value: "",
    attrs: {},
    setAttribute(k, v) { this.attrs[k] = String(v); },
    getAttribute(k) { return this.attrs[k]; },
    onclick: null,
    onchange: null,
    parentNode: null,
    nextSibling: null,
    appendChild(c) {
      c.parentNode = this; c.nextSibling = null;
      this.children.push(c); return c;
    },
    insertBefore(node, ref) {
      node.parentNode = this;
      const i = ref ? this.children.indexOf(ref) : -1;
      if (i >= 0) { this.children.splice(i, 0, node); node.nextSibling = ref; }
      else { this.children.push(node); node.nextSibling = null; }
      return node;
    },
    removeChild(node) {
      const i = this.children.indexOf(node);
      if (i >= 0) { this.children.splice(i, 1); node.parentNode = null; }
      return node;
    },
    __attrs: {},
    setAttribute(k, v) { this.__attrs[String(k)] = String(v); },
    getAttribute(k) { return this.__attrs[String(k)]; },
    addEventListener() {},
    querySelector() { return makeEl(""); },
    get textContent() { return this._text; },
    set textContent(v) { this._text = String(v); },
    get innerHTML() { return this._html; },
    set innerHTML(v) {
      this._html = String(v);
      if (!v) this.children = [];
    },
  };
  return el;
}
function getEl(id) {
  if (!elements[id]) elements[id] = makeEl(id);
  return elements[id];
}

const charts = {};        // elId -> [opt]
const chartRefs = {};     // elId -> 最新一次 init 的对象（含 __h/__acts）
function echartsInit(el) {
  const id = el.id || "anon";
  if (!charts[id]) charts[id] = [];
  const c = {
    setOption(opt, notMerge) {
      const a = charts[id];
      const base = a.length ? a[a.length - 1] : null;
      if (notMerge || !base) { a.push(opt); return; }
      const merged = Object.assign({}, base, opt);
      // 与 ECharts 一致：series 数组按索引合并（保留 name/data 等未覆盖字段）
      if (Array.isArray(base.series) && Array.isArray(opt.series)) {
        const n = Math.max(base.series.length, opt.series.length);
        merged.series = [];
        for (let i = 0; i < n; i++) {
          merged.series.push(Object.assign({}, base.series[i] || {},
                                            opt.series[i] || {}));
        }
      }
      a.push(merged);
    },
    getOption() { const a = charts[id]; return a.length ? a[a.length - 1] : {}; },
    dispatchAction(a) { (c.__acts = c.__acts || []).push(a); },
    on(ev, fn) { (c.__h = c.__h || {}); (c.__h[ev] = c.__h[ev] || []).push(fn); },
    getZr() {
      if (!c.__zr) {
        const h = {};
        c.__zr = {
          on(ev, fn) { (h[ev] = h[ev] || []).push(fn); },
          fire(ev, e) { (h[ev] || []).forEach(function (f) { f(e); }); },
        };
      }
      return c.__zr;
    },
    getDom() { return makeEl("chartdom"); },
    getWidth() { return 960; },
    getHeight() { return 420; },
    clear() {},
    resize() {},
  };
  chartRefs[id] = c;
  return c;
}

let domReady = null;
const windowStub = {};
/* R-SHELL-4 主题桩：VC_THEME 控制 stored preference；matchMedia 跟随 */
windowStub.localStorage = { _v: process.env.VC_THEME || "",
  getItem(k){ return this._v; },
  setItem(k,v){ this._v = String(v); } };
windowStub.matchMedia = (q) => ({ matches:
  q.indexOf("dark") >= 0 && process.env.VC_SYS === "dark" });
const documentStub = {
  getElementById: (id) => getEl(id),
  querySelector: (sel) => getEl(sel),
  createElement: () => makeEl(""),
  addEventListener: (ev, cb) => { if (ev === "DOMContentLoaded") domReady = cb; },
};

const sandbox = {
  window: windowStub,
  document: documentStub,
  echarts: { init: echartsInit },
  console,
  setTimeout,
  JSON,
  Math,
  Object,
  Array,
  String,
  Number,
  Date,
  RegExp,
  Error,
};
sandbox.globalThis = sandbox;
vm.createContext(sandbox);

/* ---- 真实 ECharts（SSR SVG）模式：桩 setOption 不解析 option，测不出
   真实渲染异常（曾漏掉 markLine xAxis=undefined 杀整页的事故）。
   --real 时用 vendored ECharts 真正 setOption + renderToSVGString，并把
   option 同步记录进 charts 以保持既有断言可用。 ---- */
const realCharts = [];    // {id, chart}
let realInit = null;
if (useReal) {
  const path = require("path");
  globalThis.window = windowStub;
  globalThis.document = documentStub;
  const realEcharts = require(path.resolve(__dirname, "..", "web", "vendor",
                                           "echarts.min.js"));
  if (typeof realEcharts.setPlatformAPI === "function") {
    realEcharts.setPlatformAPI({
      createCanvas: () => ({ getContext: () => ({
        measureText: (t) => ({ width: String(t).length * 7 }) }) }),
      measureText: (t) => ({ width: String(t).length * 7 }),
      loadImage: null,
    });
  }
  realInit = (el) => {
    const id = (el && el.id) || "anon";
    if (!charts[id]) charts[id] = [];
    const VC_W = Number(process.env.VC_WIDTH || 960);
    const c = realEcharts.init(null, null,
      { renderer: "svg", ssr: true, width: VC_W, height: 420 });
    realCharts.push({ id, chart: c });
    return new Proxy(c, {
      get(t, p) {
        if (p === "setOption") {
          return (opt, notMerge) => {
            charts[id].push(opt);
            return t.setOption(opt, notMerge);
          };
        }
        const v = t[p];
        return typeof v === "function" ? v.bind(t) : v;
      },
    });
  };
}

if (realInit) sandbox.echarts = { init: realInit };

const data = JSON.parse(fs.readFileSync(dataPath, "utf-8"));
windowStub.__MC_REPORT__ = data;
windowStub.__MC_FULL__ = null;       // data.js 不在本次桩内加载
windowStub.addEventListener = () => {};
/* VC_DATAMISSING=1：模拟 data.js 加载失败（降级路径 R-SHELL-2） */
if (process.env.VC_DATAMISSING === "1") windowStub.__mcDataMissing = 1;
/* VC_FULLSIM=1：把同一份数据当作 __MC_FULL__（模拟 data.js 已加载） */
if (process.env.VC_FULLSIM === "1") windowStub.__MC_FULL__ = data;

const errs = [];
try {
  vm.runInContext(fs.readFileSync(appPath, "utf-8"), sandbox,
                  { filename: "app.js" });
  if (!domReady) throw new Error("DOMContentLoaded handler not registered");
  /* VC_BREAK=<函数名>：把指定渲染函数替换为抛错，验证 safe() 单块隔离
     （R-SHELL-8）——其余块必须照常渲染 */
  const brk = process.env.VC_BREAK || null;
  if (brk) {
    try {
      sandbox[brk] = function () { throw new Error("injected:" + brk); };
    } catch (e) { errs.push("break setup: " + String(e && e.message || e)); }
  }
  domReady();
} catch (e) {
  errs.push(String(e && e.stack || e));
}

const out = { errors: errs, init_len: null, min_len: null, init_x0: null,
              switched_error: false, trace_visible: null, cards_text: "" };

function textOf(el) {
  if (!el) return "";
  if (el._text) return String(el._text);
  return (el.children || []).map(textOf).join(" ");
}

function mainOpt() {
  const seq = charts.mainchart || [];
  return seq.length ? seq[seq.length - 1] : null;
}
function seriesLen(opt, name) {
  if (!opt) return null;
  const s = (opt.series || []).find((x) => x.name === name);
  return s ? s.data.length : null;
}
function seriesX0(opt, name) {
  if (!opt) return null;
  const s = (opt.series || []).find((x) => x.name === name);
  return s && s.data.length ? s.data[0][0] : null;
}
if (!errs.length) {
  out.cards_text = [textOf(getEl("perfcards")), textOf(getEl("infocards")),
                    textOf(getEl("params"))].join(" ");
  function kids(id){
    return (getEl(id).children || []).map(function(c){
      return {cls: String(c.className || ""), text: textOf(c)};
    });
  }
  out.selobjs = (sandbox.window && sandbox.window.__MC_SEL)
    ? sandbox.window.__MC_SEL() : [];
  function lwOf(id) {
    const a = charts[id] || [];
    const o = a.length ? a[a.length - 1] : null;
    return ((o && o.series) || []).map(function (sr) {
      return {name: sr.name, width: (sr.lineStyle || {}).width,
              emphasis: ((sr.emphasis || {}).lineStyle || {}).width};
    });
  }
  out.main_line_w = lwOf("mainchart");
  out.tchart_line_w = lwOf("tchart");
  // A6：所有图表 tooltip 均为点击触发、可进入
  out.tooltips = {};
  Object.keys(charts).forEach(function (id) {
    const a = charts[id] || [];
    const o = a.length ? a[a.length - 1] : null;
    if (o && o.tooltip) out.tooltips[id] = {triggerOn: o.tooltip.triggerOn,
                                            showContent: o.tooltip.showContent,
                                            showDelay: o.tooltip.showDelay,
                                            enterable: o.tooltip.enterable};
  });
  // P2-E：阶段对象树行（角色可用；节点/实例禁用）
  out.stage_rows = [];
  (function walk(node) {
    (node.children || []).forEach(function (c) {
      if (String(c.className || "").indexOf("tree-check") >= 0)
        out.stage_rows.push({text: textOf(c), disabled: !!c.disabled});
      walk(c);
    });
  })(getEl("stageObjTree"));
  out.stage_tree_text = textOf(getEl("stageObjTree"));
  out.lat_btns = (getEl("latSel").children || []).map(function (c) {
    return {t: textOf(c),
            cls: String(c.className || ""),
            off: String(c.className || "").indexOf("off") >= 0};
  });
  out.main_colors = ((mainOpt() && mainOpt().series) || []).map(function (s) {
    return {name: s.name, color: (s.lineStyle || {}).color};
  });
  const sseq = charts.slowchart || [];
  const so = sseq.length ? sseq[sseq.length - 1] : null;
  out.slow_series = ((so && so.series) || []).map(function (s) {
    return {name: s.name, type: s.type, symbol: s.symbol,
            len: (s.data || []).length};
  });
  out.slow_x = (so && so.xAxis) ? [so.xAxis.min, so.xAxis.max] : null;
  out.slow_ctl_text = textOf(getEl("slowctl"));
  out.slow_tree_rows = [];
  out.slow_tree_checks = 0;
  (function walkSlow(n) {
    ((n && n.children) || []).forEach(function (c) {
      const cls = String(c.className || "");
      if (cls.indexOf("tree-row") >= 0) out.slow_tree_rows.push(textOf(c));
      if (cls.indexOf("tree-check") >= 0) out.slow_tree_checks += 1;
      walkSlow(c);
    });
  })(getEl("slowctl"));
  // R-SLOW-2：慢点 tooltip 时刻必须来自 ts_epoch（不得出现 undefined）
  const _slowOpt = sseq.length ? sseq[sseq.length - 1] : null;
  const _fmt = (_slowOpt && _slowOpt.tooltip && _slowOpt.tooltip.formatter)
    ? _slowOpt.tooltip.formatter : null;
  if (typeof _fmt === "function") {
    try {
      const _row = ((so && so.series && so.series[0] &&
        so.series[0].data) || [])[0];
      out.slow_tip = _fmt({ data: _row });
    } catch (e) { out.slow_tip = "ERR:" + e.message; }
  }
  if (process.argv.includes("--tree-select")) {
    try {
      const sel = function () {
        return (sandbox.window && sandbox.window.__MC_SEL)
          ? sandbox.window.__MC_SEL() : [];
      };
      const nb = findBtn(getEl("objTree"), "全选节点");
      if (nb && nb.onclick) nb.onclick();
      out.sel_after_nodes = sel();
      const ib = findBtn(getEl("objTree"), "全选实例");
      if (ib && ib.onclick) ib.onclick();
      out.sel_after_instances = sel();
      const cb = findBtn(getEl("objTree"), "全部清空");
      if (cb && cb.onclick) cb.onclick();
      out.sel_after_clear = sel();
    } catch (e) { errs.push("tree-select: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--expand-tree")) {
    try {
      for (let i = 0; i < 32; i++) {
        let clicked = false;
        (function walk(node) {
          (node.children || []).forEach(function (c) {
            if (clicked) return;
            if (String(c.className || "").indexOf("tree-caret") >= 0 &&
                c.onclick && c._text !== "▾") {
              c.onclick(); clicked = true;
            }
            walk(c);
          });
        })(getEl("objTree"));
        if (!clicked) break;
      }
      out.tree_rows_expanded = (getEl("objTree").children || []).map(textOf);
    } catch (e) { errs.push("expand: " + String(e && e.stack || e)); }
  }
  function findBtn(root, text) {
    let hit = null;
    (function walk(node) {
      if (hit || !node) return;
      if (node._text === text && node.onclick) { hit = node; return; }
      (node.children || []).forEach(walk);
    })(root);
    return hit;
  }
  if (process.argv.includes("--role-writer")) {
    try {
      const wb = findBtn(getEl("objTree"), "writer");
      if (wb && wb.onclick) wb.onclick();
      out.selobjs_after_role = (sandbox.window && sandbox.window.__MC_SEL)
        ? sandbox.window.__MC_SEL() : [];
      const qseq = charts.qpschart || [];
      const qo = qseq.length ? qseq[qseq.length - 1] : null;
      out.qps_after_role = ((qo && qo.series) || []).map(function (s) {
        return s.name;
      });
    } catch (e) { errs.push("role: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--role-roundtrip")) {
    try {
      const sel = function () {
        return (sandbox.window && sandbox.window.__MC_SEL)
          ? sandbox.window.__MC_SEL() : [];
      };
      sandbox.toggleObj("node:n2:reader");          // reader 视角选中一个节点
      out.selobjs_reader_selected = sel();
      const wb = findBtn(getEl("objTree"), "writer");
      if (wb && wb.onclick) wb.onclick();
      out.selobjs_writer_view = sel();
      const rb = findBtn(getEl("objTree"), "reader");
      if (rb && rb.onclick) rb.onclick();
      out.selobjs_back_reader = sel();
    } catch (e) { errs.push("roundtrip: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--slow-writer")) {
    try {
      const wb = findBtn(getEl("slowctl"), "writer");
      if (wb && wb.onclick) wb.onclick();
      const q1 = charts.slowchart || [];
      const o1 = q1.length ? q1[q1.length - 1] : null;
      out.slow_series_writer = ((o1 && o1.series) || []).map(function (s) {
        return {name: s.name, symbol: s.symbol};
      });
      out.slow_btn_before = treeMiniLabels(getEl("slowctl"));
      const nb = findBtn(getEl("slowctl"), "全选节点");
      if (nb && nb.onclick) nb.onclick();
      out.slow_btn_after_nodes = treeMiniLabels(getEl("slowctl"));
      const ib = findBtn(getEl("slowctl"), "全选实例");
      if (ib && ib.onclick) ib.onclick();
      out.slow_btn_after_instances = treeMiniLabels(getEl("slowctl"));
      const q2 = charts.slowchart || [];
      const o2 = q2.length ? q2[q2.length - 1] : null;
      out.slow_series_all = ((o2 && o2.series) || []).map(function (s) {
        return {name: s.name, len: (s.data || []).length};
      });
    } catch (e) { errs.push("slow: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--total-off")) {
    try {
      const tb = findBtn(getEl("objTree"), "总计");
      if (tb && tb.onclick) tb.onclick();
      out.selobjs_total_off = (sandbox.window && sandbox.window.__MC_SEL)
        ? sandbox.window.__MC_SEL() : [];
    } catch (e) { errs.push("total: " + String(e && e.stack || e)); }
  }
  out.perf_kids = kids("perfcards");
  out.unit_kids = kids("unitmeta");
  out.tmeta_text = textOf(getEl("tmeta"));
  out.perf_shown = getEl("perfcards").style.display !== "none";
  out.rolecards_shown = getEl("rolecards").style.display !== "none";
  out.rolecards_text = textOf(getEl("rolecards"));
  out.param_rows = ((getEl("params").children || []).map(function(r){
    const k = (r.children || []).find(function(c){ return c.className === "pk"; });
    return k ? textOf(k) : textOf(r);
  }));
  const hseq = charts.histchart || [];
  const ho = hseq.length ? hseq[hseq.length - 1] : null;
  out.hist_marks = (ho && ho.series && ho.series[0] && ho.series[0].markLine)
    ? (ho.series[0].markLine.data || []).length : 0;
  out.hist_cats = (ho && ho.xAxis) ? (ho.xAxis.data || []).length : 0;
  out.hist_first = (ho && ho.xAxis && ho.xAxis.data)
    ? String(ho.xAxis.data[0] || "") : "";
  out.hist_mark_x = (ho && ho.series && ho.series[0] && ho.series[0].markLine
    && ho.series[0].markLine.data)
    ? ho.series[0].markLine.data.map((d) => d.xAxis) : [];
  out.hist_grid_top = (ho && ho.grid) ? ho.grid.top : null;
  out.init_len = seriesLen(mainOpt(), "p50");
  out.init_x0 = seriesX0(mainOpt(), "p50");

  /* ---- A 类补测的机械输出键（语义断言在 pytest 侧） ---- */
  // 无数据窗口带（R-LAT-9）与异常竖线上限（R-LAT-10）
  const _s0 = (mainOpt() && mainOpt().series) ? mainOpt().series[0] : null;
  out.main_markarea = (_s0 && _s0.markArea && _s0.markArea.data) || null;
  out.main_markline_n = (_s0 && _s0.markLine && _s0.markLine.data)
    ? _s0.markLine.data.length : 0;
  out.anom_line_style = (_s0 && _s0.markLine && _s0.markLine.data &&
    _s0.markLine.data[0] && _s0.markLine.data[0].lineStyle) || null;
  out.outlier_series = (((mainOpt() || {}).series) || []).filter(function (x) {
    return String(x.name || "").indexOf("离群点") >= 0;
  }).map(function (x) {
    return {name: x.name,
            marks: (x.markLine && (x.markLine.data || []).length) || 0,
            mark0: (x.markLine && x.markLine.data && x.markLine.data[0]) || null,
            style: (x.markLine && x.markLine.lineStyle) || null};
  });
  const _qseq = charts.qpschart || [];
  const _qo = _qseq.length ? _qseq[_qseq.length - 1] : null;
  out.qps_markarea = (_qo && _qo.series && _qo.series[0] &&
    _qo.series[0].markArea && _qo.series[0].markArea.data) || null;
  out.qps_meta = ((_qo && _qo.series) || []).map(function (s) {
    const d = s.data || [];
    return {name: s.name, type: s.type, width: s.barMaxWidth,
            x0: d.length ? d[0][0] : null,
            color: (s.itemStyle || {}).color,
            radius: (s.itemStyle || {}).borderRadius};
  });
  out.qps_legend_data = (_qo && _qo.legend && _qo.legend.data) || null;
  out.qps_axis_pointer = (_qo && _qo.tooltip && _qo.tooltip.axisPointer) || null;
  try {
    const qf = (_qo && _qo.tooltip && _qo.tooltip.formatter);
    out.qps_fmt_tip = (typeof qf === "function")
      ? String(qf([{value: [0, 0], seriesName: "x"}])) : null;
  } catch (e) { out.qps_fmt_tip = null; }
  out.qps_emphasis = ((_qo && _qo.series) || []).map(function (s) {
    return (s.emphasis || {}).disabled === true;});
  out.qps_colors = ((_qo && _qo.series) || []).map(function (s) {
    return { name: s.name, color: (s.itemStyle || {}).color };
  });
  // 慢点条带逐条颜色（R-LAT-12：仅最慢一根 hero）
  out.slow_colors = ((so && so.series && so.series[0] &&
    so.series[0].data) || []).map(function (d) {
    return (d && d.itemStyle && d.itemStyle.color) || null;
  });
  // 整卡可见性（R-HIST-3 / R-SLOW-3：无数据保持 display:none）
  out.hist_visible = getEl("histcard")
    ? String(getEl("histcard").style.display || "") : "missing";
  out.slow_visible = getEl("slowcard")
    ? String(getEl("slowcard").style.display || "") : "missing";
  // data.js 降级（R-SHELL-2）
  out.datanote_visible = getEl("datanote")
    ? String(getEl("datanote").style.display || "") : "missing";
  out.size_sel_display = getEl("sizeSel")
    ? String(getEl("sizeSel").style.display || "") : null;
  // R-SHELL-4 主题状态
  const _th = windowStub.__MC_THEME__ || null;
  out.theme_pref = _th ? String(_th.pref) : null;
  out.theme_dark = _th ? !!_th.dark : null;
  const _tg = getEl("themeToggle");
  out.theme_btn_exists = !!_tg;
  // R-MON-4 部分缺失：标题后缀 / urma 图占位 / 图注分句
  const _suf = getEl("monTitleSuffix");
  out.mon_title_suffix = _suf ? String(_suf._text || "") : null;
  const _ub2 = getEl("urmachart");
  out.urma_chart_display = _ub2
    ? String(_ub2.style.display || "") : null;
  const _un2 = getEl("monUrmaNote");
  out.mon_urma_note = _un2 ? String(_un2._text || "") : null;
  const _mc = getEl("monitorcard");
  out.monitorcard_visible = _mc
    ? String(_mc.style.display || "") : "missing";
  // fmtUs 自适应单位（R-SHELL-10）
  out.fmt_us = (typeof sandbox.fmtUs === "function")
    ? [String(sandbox.fmtUs(1500)), String(sandbox.fmtUs(500)),
       String(sandbox.fmtUs(0)), String(sandbox.fmtUs(1234567))]
    : null;
  // 离群按钮状态（R-LAT-4：无可命中对象时不得呈现无效控件）
  const _ob = getEl("outlierBtn");
  out.outlier_btn_state = _ob
    ? { display: String(_ob.style.display || ""),
        disabled: !!_ob.disabled,
        text: textOf(_ob),
        on: String(_ob.className || "").indexOf("on") >= 0,
        off: String(_ob.className || "").indexOf("off") >= 0,
        title: String(_ob.title || "") }
    : null;

  // 点击一个比默认更粗的粒度按钮（时间分桶阶梯），验证切档
  const stepBox = getEl("tierBtns");
  const btns = (stepBox.children || []).filter(
    (b) => b && b.onclick && !b.disabled && String(b._text) !== "");
  const cur = btns.find((b) => b.className === "on");
  const target = tierLabel
    ? btns.find((b) => String(b._text) === tierLabel)
    : btns.find((b) => b !== cur);
  if (target && target.onclick) {
    try { target.onclick(); out.min_len = seriesLen(mainOpt(), "p50"); }
    catch (e) { errs.push("tier switch: " + String(e)); }
  } else {
    out.min_len = out.init_len;
  }

  if (clickCombo) {
    try {
      const btns = (getEl("comboBtns").children || []);
      out.combo_btns = btns.length;
      if (btns.length > 1) {
        btns[btns.length - 1].onclick();
        out.combo_clicked = true;
        out.combo_cards = (getEl("infocards").children || []).length;
      }
    } catch (e) { errs.push("combo click: " + String(e && e.stack || e)); }
  }

  if (addObjKey) {
    try {
      // 对象树：找到 data-key 匹配的行并点击（等价于用户勾选）
      let clicked = false;
      (function walk(node) {
        if (clicked || !node) return;
        const kids = node.children || [];
        for (const c of kids) {
          const k = c.getAttribute && c.getAttribute("data-key");
          if (k === addObjKey && c.onclick) { c.onclick(); clicked = true; return; }
          walk(c);
        }
      })(getEl("objTree"));
      if (!clicked) errs.push("add obj: tree row not found " + addObjKey);
      const opt = mainOpt();
      out.overlay_names = (opt && opt.series || []).map((s) => s.name);
      out.overlay_colors = (opt && opt.series || []).map((s) => {
        return {name: s.name, color: (s.lineStyle || {}).color};
      });
      out.overlay_len = opt && opt.series && opt.series.length
        ? opt.series[0].data.length : null;
      /* 全局统计横线：名字 + 水平线取值（data[0][1]） */
      out.globals = (opt && opt.series || []).filter(function(ss){
        return String(ss.name || "").indexOf("全局") === 0;
      }).map(function(ss){
        return {name: ss.name,
                y: (ss.data && ss.data[0] ? ss.data[0][1] : null),
                x0: (ss.data && ss.data[0] ? ss.data[0][0] : null),
                x1: (ss.data && ss.data.length
                     ? ss.data[ss.data.length - 1][0] : null),
                dash: !!(ss.lineStyle && ss.lineStyle.type === "dashed")};
      });
    } catch (e) { errs.push("add obj: " + String(e && e.stack || e)); }
  }

  /* ---- 节点监控（C18.V8/V9）：三树 + 三图 + 模拟点击勾选 ---- */
  function monSeries(id) {
    const a = charts[id] || [];
    const o = a.length ? a[a.length - 1] : null;
    return o ? (o.series || []).map(function(s) {
      return {name: s.name, color: (s.lineStyle || {}).color,
              dash: (s.lineStyle || {}).type === "dashed",
              n: (s.data || []).length}; }) : null;
  }
  function monLegend(id) {
    const a = charts[id] || [];
    const o = a.length ? a[a.length - 1] : null;
    return o ? (o.legend || null) : null;
  }
  out.mem_series = monSeries("memchart");
  out.proc_series = monSeries("procchart");
  out.urma_series = monSeries("urmachart");
  out.mem_legend = monLegend("memchart");
  out.urma_legend = monLegend("urmachart");
  out.mem_empty = getEl("memEmpty") ? String(getEl("memEmpty").style.display || "") : null;
  out.proc_empty = getEl("procEmpty") ? String(getEl("procEmpty").style.display || "") : null;
  out.urma_empty = getEl("urmaEmpty") ? String(getEl("urmaEmpty").style.display || "") : null;
  out.mem_title_display = getEl("memHostTitle") ? String(getEl("memHostTitle").style.display || "") : null;
  out.urma_title_display = getEl("urmaTitle") ? String(getEl("urmaTitle").style.display || "") : null;
  /* 树行文本（节点行/分组头/计数），用于断言控件结构 */
  function monTreeTexts(id) {
    const box = getEl(id);
    const texts = [];
    (function walk(node) {
      if (!node) return;
      const kids = node.children || [];
      for (const c of kids) {
        const cls = String(c.className || "");
        if (cls.indexOf("tree-check") >= 0) {
          const nm = c.children && c.children[1];
          texts.push("row:" + (nm ? String(nm._text || "") : "?"));
        } else if (cls.indexOf("mon-node-head") >= 0) {
          texts.push("head:" + String((c.children || [])[1]
            ? ((c.children || [])[1]._text || "") : "?"));
        } else if (cls.indexOf("node-group-acts") >= 0) {
          (c.children || []).forEach(function(b) {
            texts.push("btn:" + String(b._text || "")); });
        } else if (cls.indexOf("objtree-count") >= 0) {
          texts.push("cnt:" + String(c._text || ""));
        } else if (cls.indexOf("tree-caret") >= 0) {
          texts.push("caret:" + String(c._text || ""));
        }
        walk(c);
      }
    })(box);
    return texts;
  }
  out.mem_tree = monTreeTexts("memTree");
  out.proc_tree = monTreeTexts("procTree");
  out.urma_tree = monTreeTexts("urmaTree");
  /* 行/节点头出现次数：用于锁死重复追加（旧实现 append-only 会重影） */
  function monCounts(id) {
    const acc = {};
    monTreeTexts(id).forEach(function(t) {
      if (/^(row|head):/.test(t)) acc[t] = (acc[t] || 0) + 1;
    });
    return acc;
  }
  function monCountClass(id, name) {
    let n = 0;
    (function walk(node) {
      if (!node) return;
      (node.children || []).forEach(function(c) {
        if (String(c.className || "").split(" ").indexOf(name) >= 0) n++;
        walk(c);
      });
    })(getEl(id));
    return n;
  }
  out.mon_tree_lists = {mem: monCountClass("memTree", "tree-list"),
                        proc: monCountClass("procTree", "tree-list"),
                        urma: monCountClass("urmaTree", "tree-list")};
  /* 每个节点行跟随的 mini 全选/清空按钮数（URMA 无子行 = 0） */
  function monRowMinis(id) {
    let n = 0;
    (function walk(node) {
      if (!node) return;
      (node.children || []).forEach(function(c) {
        if (String(c.className || "").split(" ").indexOf("tree-row") >= 0) {
          (c.children || []).forEach(function(x) {
            if (String(x.className || "").split(" ").indexOf("mini") >= 0) n++;
          });
        }
        walk(c);
      });
    })(getEl(id));
    return n;
  }
  out.mon_row_minis = {mem: monRowMinis("memTree"),
                       proc: monRowMinis("procTree"),
                       urma: monRowMinis("urmaTree")};
  out.mon_counts = {mem: monCounts("memTree"), proc: monCounts("procTree"),
                    urma: monCounts("urmaTree")};
  /* 幂等：模拟主题切换 / size 切换时 renderMonitors 再次进入 */
  try {
    sandbox.renderMonitors();
    sandbox.renderMonitors();
    out.mon_counts_rerender = {mem: monCounts("memTree"),
                               proc: monCounts("procTree"),
                               urma: monCounts("urmaTree")};
  } catch (e) { errs.push("mon rerender: " + String(e && e.stack || e)); }
  out.mon_tree_rerender_stable =
    JSON.stringify(out.mon_counts) === JSON.stringify(out.mon_counts_rerender);
  const monCaret = arg("--mon-caret", null);
  if (monCaret) {
    try {
      const parts = monCaret.split("::");
      const treeId = parts[0];
      const rowText = parts[1] || "";
      const treeBox = getEl(treeId);
      let clicked = false;
      (function walk(node) {
        if (clicked || !node) return;
        const kids = node.children || [];
        for (const c of kids) {
          const cls = String(c.className || "");
          if (cls.indexOf("tree-row") >= 0) {
            let nm = null;
            (c.children || []).forEach(function(x) {
              const xc = String(x.className || "");
              if (xc.indexOf("tree-check") >= 0) nm = (x.children || [])[1];
              else if (xc.indexOf("tree-name") >= 0) nm = x;
            });
            if (nm && String(nm._text || "") === rowText) {
              const car = (c.children || []).find(function(x) {
                return String(x.className || "").indexOf("tree-caret") >= 0;
              });
              if (car && car.onclick) { car.onclick(); clicked = true; return; }
            }
          }
          walk(c);
        }
      })(treeBox);
      if (!clicked) errs.push("mon caret: row not found " + monCaret);
      out.mon_tree_after_caret = {mem: monTreeTexts("memTree"),
        proc: monTreeTexts("procTree"), urma: monTreeTexts("urmaTree")};
      out.mon_counts_after_caret = {mem: monCounts("memTree"),
        proc: monCounts("procTree"), urma: monCounts("urmaTree")};
    } catch (e) {
      errs.push("mon caret: " + String(e && e.stack || e));
    }
  }
  const monMini = arg("--mon-mini", null);
  if (monMini) {
    /* 点击某节点行跟随的 mini 全选/清空（树::节点::按钮） */
    try {
      const parts = monMini.split("::");
      const treeId = parts[0];
      const rowLabel = parts[1] || "";
      const btnLabel = parts[2] || "";
      const treeBox = getEl(treeId);
      let clicked = false;
      (function walk(node) {
        if (clicked || !node) return;
        (node.children || []).forEach(function(c) {
          if (clicked) return;
          if (String(c.className || "").split(" ").indexOf("tree-row") >= 0) {
            let nm = null;
            (c.children || []).forEach(function(x) {
              const xc = String(x.className || "");
              if (xc.indexOf("tree-check") >= 0) nm = (x.children || [])[1];
              else if (xc.indexOf("tree-name") >= 0) nm = x;
            });
            if (nm && String(nm._text || "") === rowLabel) {
              (c.children || []).forEach(function(x) {
                if (!clicked
                    && String(x.className || "").split(" ").indexOf("mini") >= 0
                    && String(x._text || "") === btnLabel && x.onclick) {
                  x.onclick(); clicked = true;
                }
              });
            }
          }
          walk(c);
        });
      })(treeBox);
      if (!clicked) errs.push("mon mini: not found " + monMini);
      out.mon_mini_series = {mem: monSeries("memchart"),
                             proc: monSeries("procchart"),
                             urma: monSeries("urmachart")};
      out.mon_counts_after_mini = {mem: monCounts("memTree"),
        proc: monCounts("procTree"), urma: monCounts("urmaTree")};
    } catch (e) {
      errs.push("mon mini: " + String(e && e.stack || e));
    }
  }
  const monMetric = arg("--mon-metric", null);
  if (monMetric) {
    /* 点击宿主内存「用量/可用量」指标开关（逗号分隔可连续点） */
    try {
      monMetric.split(",").forEach(function(label) {
        const treeNow = getEl("memTree");
        let done = false;
        (function walk(node) {
          if (done || !node) return;
          (node.children || []).forEach(function(c) {
            if (done) return;
            if (String(c.className || "").split(" ").indexOf("lat-sel") >= 0) {
              (c.children || []).forEach(function(b) {
                if (!done && String(b._text || "") === label && b.onclick) {
                  b.onclick(); done = true;
                }
              });
            }
            walk(c);
          });
        })(treeNow);
        if (!done) errs.push("mon metric: button not found " + label);
      });
      out.mem_series_after_metric = monSeries("memchart");
      out.mem_empty_after_metric = getEl("memEmpty")
        ? String(getEl("memEmpty").style.display || "") : null;
      out.mem_empty_text_after_metric = getEl("memEmpty")
        ? String(getEl("memEmpty")._text || "") : null;
    } catch (e) {
      errs.push("mon metric: " + String(e && e.stack || e));
    }
  }
  const monClick = arg("--mon-click", null);
  if (monClick) {
    try {
      const parts = monClick.split("::");
      const treeId = parts[0];
      const rowText = parts[1] || "";
      const treeBox = getEl(treeId);
      let clicked = false;
      (function walk(node) {
        if (clicked || !node) return;
        const kids = node.children || [];
        for (const c of kids) {
          const cls = String(c.className || "");
          if (!clicked && cls.indexOf("tree-check") >= 0
              && c.onclick) {
            const nm = c.children && c.children[1];
            if (nm && String(nm._text || "") === rowText) {
              c.onclick(); clicked = true; return;
            }
          }
          walk(c);
        }
      })(treeBox);
      if (!clicked) errs.push("mon click: row not found " + monClick);
      out.mem_series_after = monSeries("memchart");
      out.proc_series_after = monSeries("procchart");
      out.urma_series_after = monSeries("urmachart");
      out.mon_counts_after_click = {mem: monCounts("memTree"),
        proc: monCounts("procTree"), urma: monCounts("urmaTree")};
    } catch (e) {
      errs.push("mon click: " + String(e && e.stack || e));
    }
  }

  const focusKey = arg("--focus", null);
  if (focusKey) {
    try {
      let done = false;
      (function walk(node) {
        if (done || !node) return;
        const kids = node.children || [];
        for (const c of kids) {
          // 聚焦 = 直接点击块本身（块 title 里带字段名）
          if (c.className && c.className.indexOf("lvl-blk") === 0 &&
              String(c.title || "").indexOf(focusKey) >= 0 && c.onclick) {
            c.onclick(); done = true; return;
          }
          walk(c);
        }
      })(getEl("levels"));
      if (!done) errs.push("focus: toggle not found " + focusKey);
      out.focus_text = textOf(getEl("levels"));
    } catch (e) { errs.push("focus: " + String(e && e.stack || e)); }
  }

  const outlierArg = arg("--outlier", null);
  if (outlierArg === "1") {
    try {
      const names = function () {
        const opt = mainOpt();
        return ((opt && opt.series) || []).map(function (s) { return s.name; });
      };
      out.outlier_before = names();
      const btn = getEl("outlierBtn");
      btn.onclick();
      out.outlier_after = names();
      out.outlier_btn = textOf(btn);
      out.outlier_btn_off =
        String(btn.className || "").indexOf("off") >= 0;
      out.outlier_btn_on =
        String(btn.className || "").indexOf("on") >= 0;
    } catch (e) { errs.push("outlier: " + String(e && e.stack || e)); }
  }

  const _tc=(charts.tchart||[]); const _tco=_tc.length?_tc[_tc.length-1]:null;
  out.tchart_series=((_tco&&_tco.series)||[]).map(function(s){return s.name;});
  try{
    const fmt=(_tco&&_tco.tooltip&&_tco.tooltip.formatter);
    out.tchart_tip_fmt=(typeof fmt==='function')
      ? String(fmt([{value:[1795050994000,5350],seriesName:'total',marker:''}]))
      : null;
  }catch(e){ out.tchart_tip_fmt=null; }
  out.tchart_empty=textOf(getEl("tchartEmpty"));
  out.zslider_cap = textOf(getEl("zsliderCap"));
  out.tree_rows = (getEl("objTree").children || []).map(function (r) {
    return textOf(r);
  });
  out.qps_series = (charts.qpschart || []).length;
  out.qps_note = textOf(getEl("qpsNote"));
  const qseq = charts.qpschart || [];
  const qo = qseq.length ? qseq[qseq.length - 1] : null;
  out.qps_legend_mode = (qo && qo.legend) ? qo.legend.selectedMode : null;
  out.qps_filter_mode = (qo && qo.dataZoom && qo.dataZoom[0])
    ? qo.dataZoom[0].filterMode : null;

  function treeMiniLabels(root){
    const acc=[];
    (function walk(n){
      (n.children||[]).forEach(function(c){
        if (String(c.className||"").indexOf("mini")>=0 && c._text)
          acc.push(String(c._text));
        walk(c);
      });
    })(root);
    return acc;
  }
  function labelEls(root){
    return ((root&&root.children)||[]).filter(function(c){
      return String(c.className||"").indexOf("endlabel")>=0;});
  }
  if (process.argv.includes("--cluster-hover")) {
    try {
      const ov=getEl("endlabels");
      const labels=labelEls(ov);
      out.cluster_labels=labels.map(function(c){return textOf(c);});
      out.cluster_titles=labels.map(function(c){return String(c.title||"");});
      out.cluster_fulls=labels.map(function(c){
        return String((c.getAttribute&&c.getAttribute("data-full"))||"");});
      out.cluster_classes=labels.map(function(c){return String(c.className||"");});
      out.leader_count=((ov.children||[]).filter(function(c){
        return String(c.className||"").indexOf("endleader")>=0;})).length;
      const opt0=mainOpt();
      out.cluster_series=((opt0&&opt0.series)||[]).map(function(x){
        return x.name;});
      const lab=labels.length>1?labels[1]:labels[0];
      if(lab&&lab.onmouseenter){
        lab.onmouseenter();
        const o1=mainOpt();
        out.cluster_dims=((o1&&o1.series)||[]).map(function(x){
          return (x.lineStyle||{}).opacity;});
        if(lab.onmouseleave) lab.onmouseleave();
        const o2=mainOpt();
        out.cluster_restored=((o2&&o2.series)||[]).map(function(x){
          return (x.lineStyle||{}).opacity;});
      }
    } catch (e) { errs.push("cluster-hover: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--mlabels")) {
    try {
      const all=(sandbox.objAll?sandbox.objAll():{})||{};
      const byKind=function(kind){
        return Object.keys(all).filter(function(k){
          const o=all[k]||{}; return (o.kind||'client')===kind;}).sort();
      };
      const nodes=byKind('node');
      nodes.slice(0,2).forEach(function(k){ sandbox.toggleObj(k); });
      const clients=byKind('client');
      if(clients[0]) sandbox.toggleObj(clients[0]);
      const ov=getEl("endlabels");
      const labels=labelEls(ov);
      out.fixed_labels=labels.map(function(c){return textOf(c);});
      out.fixed_titles=labels.map(function(c){return String(c.title||"");});
      out.fixed_fulls=labels.map(function(c){
        return String((c.getAttribute&&c.getAttribute("data-full"))||"");});
      out.fixed_classes=labels.map(function(c){return String(c.className||"");});
      out.leader_count=((ov.children||[]).filter(function(c){
        return String(c.className||"").indexOf("endleader")>=0;})).length;
      out.ml_node_labels=nodes.slice(0,2).map(function(k){
        return String(all[k].label||k);});
      out.ml_client_label=clients[0]
        ? String((all[clients[0]]||{}).label||clients[0]) : null;
    } catch (e) { errs.push("mlabels: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--lat-buttons")) {
    try {
      const states = function () {
        return (getEl("latSel").children || []).filter(function (b) {
          return b._text !== "全选" && b._text !== "清空";
        }).map(function (b) {
          return {t: textOf(b), off: String(b.className || "").indexOf("off") >= 0};
        });
      };
      out.lat_before = states();
      const allb = findBtn(getEl("latSel"), "全选");
      if (allb && allb.onclick) allb.onclick();
      out.lat_all = states();
      const clrb = findBtn(getEl("latSel"), "清空");
      if (clrb && clrb.onclick) clrb.onclick();
      out.lat_clear = states();
    } catch (e) { errs.push("lat-buttons: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--tree-toggle")) {
    try {
      const sel=function(){return (sandbox.window&&sandbox.window.__MC_SEL)
        ?sandbox.window.__MC_SEL():[];};
      out.btn_labels_before=treeMiniLabels(getEl("objTree"));
      const nb=findBtn(getEl("objTree"),"全选节点");
      if(nb&&nb.onclick)nb.onclick();
      out.sel_after_nodes=sel();
      out.btn_labels_after_nodes=treeMiniLabels(getEl("objTree"));
      const cb=findBtn(getEl("objTree"),"清空节点");
      if(cb&&cb.onclick)cb.onclick();
      out.sel_after_clear_nodes=sel();
      const ib=findBtn(getEl("objTree"),"全选实例");
      if(ib&&ib.onclick)ib.onclick();
      out.sel_after_instances=sel();
      out.btn_labels_after_instances=treeMiniLabels(getEl("objTree"));
      const ic=findBtn(getEl("objTree"),"清空实例");
      if(ic&&ic.onclick)ic.onclick();
      out.sel_after_clear_instances=sel();
      out.btn_labels_after_clear_instances=treeMiniLabels(getEl("objTree"));
    } catch (e) { errs.push("tree-toggle: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--clear-total-off")) {
    try {
      const sel=function(){return (sandbox.window&&sandbox.window.__MC_SEL)
        ?sandbox.window.__MC_SEL():[];};
      const tb=findBtn(getEl("objTree"),"总计");
      if(tb&&tb.onclick)tb.onclick();
      out.sel_total_off=sel();
      const cb=findBtn(getEl("objTree"),"全部清空");
      if(cb&&cb.onclick)cb.onclick();
      out.sel_after_clear_auto_total=sel();
    } catch (e) { errs.push("clear-total-off: " + String(e && e.stack || e)); }
  }
  function stageTreeL1() {
    let hit = null;
    (function walk(n) {
      if (hit || !n) return;
      (n.children || []).forEach(function (c) {
        const k = c.getAttribute && c.getAttribute("data-key");
        if (k && k !== "total_us" && k !== "residual" && c.onclick) {
          hit = c; return;
        }
        walk(c);
      });
    })(getEl("stageTree"));
    return hit;
  }
  if (process.argv.includes("--label-anchor")) {
    try {
      // 主图：自选一个节点对象（多对象才有固定标签）
      let nk = null;
      (function walk(n) {
        if (nk || !n) return;
        (n.children || []).forEach(function (c) {
          const k = c.getAttribute && c.getAttribute("data-key");
          if (k && k.indexOf("node:") === 0) { nk = k; return; }
          walk(c);
        });
      })(getEl("objTree"));
      if (nk && sandbox.toggleObj) sandbox.toggleObj(nk);
      const labelsOf = function (id) {
        return ((getEl(id).children || []).filter(function (c) {
          return String(c.className || "").indexOf("endlabel") >= 0;
        })).map(function (c) {
          return {
            name: String((c.getAttribute && c.getAttribute("data-name")) ||
                        textOf(c)),
            top: parseFloat(String(c.style.top || "0").replace("px", "")),
          };
        });
      };
      out.main_labels = labelsOf("endlabels");
      // 阶段图：点一个 L1 形成两序列后取标签顺序
      const l1 = stageTreeL1();
      if (l1 && l1.onclick) l1.onclick();
      out.stage_labels = labelsOf("tEndlabels");
      const to = (charts.tchart || []).slice(-1)[0] || {};
      out.stage_last = {};
      ((to && to.series) || []).forEach(function (x) {
        const d = x.data || [];
        out.stage_last[x.name] = d.length ? d[d.length - 1][1] : null;
      });
    } catch (e) { errs.push("label-anchor: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--label-focus")) {
    try {
      const c = chartRefs.tchart;
      const opac = function () {
        const a = charts.tchart || [];
        const o = a.length ? a[a.length - 1] : null;
        return ((o && o.series) || []).map(function (x) {
          return (x.lineStyle || {}).opacity;});
      };
      const labels = function () {
        return ((getEl("tEndlabels").children || []).filter(function (x) {
          return String(x.className || "").indexOf("endlabel") >= 0;}));
      };
      const tipText = function () {
        const box = getEl("tEndlabels");
        const el = (box.children || []).find(function (c) {
          return String(c.className || "").indexOf("mc-label-tip") >= 0;});
        return el ? textOf(el) : "";
      };
      const l1 = stageTreeL1();
      if (l1 && l1.onclick) l1.onclick();
      const lab = labels()[0];
      out.lf_count = labels().length;
      if (lab && lab.onmouseenter) {
        lab.onmouseenter();
        out.lf_hover = opac();
        out.lf_tip = tipText();
        out.lf_tip_full = lab.getAttribute ? lab.getAttribute("data-full") : null;
        c.getZr().fire("mousemove", {offsetX: 500, offsetY: 300});
        out.lf_after_move = opac();
        out.lf_tip_after_move = tipText();
        lab.onmouseenter();
        out.lf_hover2 = opac();
        c.getZr().fire("globalout", {});
        out.lf_after_globalout = opac();
        out.lf_tip_after_go = tipText();
        lab.onmouseenter();
        if (lab.onmouseleave) lab.onmouseleave();
        out.lf_after_leave = opac();
        out.lf_cls = String(lab.className || "");
      }
    } catch (e) { errs.push("label-focus: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--globalout-tip")) {
    try {
      const c = chartRefs.mainchart;
      const n0 = ((c && c.__acts) || []).length;
      c.getZr().fire("globalout", {});
      out.go_actions = ((c && c.__acts) || []).slice(n0)
        .map(function (a) { return a.type; });
    } catch (e) { errs.push("globalout-tip: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--endlabels-focus")) {
    try {
      const acts = [];
      const fake = {
        convertToPixel: function () { return [600, 120]; },
        getWidth: function () { return 960; },
        getHeight: function () { return 420; },
        getOption: function () { return mainOpt() || {series: []}; },
        setOption: function (o) { acts.push(o); },
        dispatchAction: function () {},
        getDom: function () { return makeEl("chartdom"); },
        on: function () {},
        getZr: function () { return {on: function () {}}; },
      };
      sandbox.layoutEndLabels(fake);
      const labels = ((getEl("endlabels").children || []).filter(function (c) {
        return String(c.className || "").indexOf("endlabel") >= 0;}));
      out.el_labels = labels.map(function (c) { return textOf(c); });
      out.el_box_set = !!fake.__mcLabelBox;
      const lab = labels[0];
      if (lab && lab.onmouseenter) {
        lab.onmouseenter();
        const l1 = acts.length ? acts[acts.length - 1] : {};
        out.el_focus = ((l1.series) || []).map(function (x) {
          return (x.lineStyle || {}).opacity;});
        if (lab.onmouseleave) lab.onmouseleave();
        const l2 = acts.length ? acts[acts.length - 1] : {};
        out.el_restored = ((l2.series) || []).map(function (x) {
          return (x.lineStyle || {}).opacity;});
        out.el_title = String(lab.title || "");
      }
    } catch (e) { errs.push("endlabels-focus: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--stage-tree")) {
    try {
      // 展开第一个 L1（点 caret）后采集阶段树行
      let caret = null;
      (function walk(n) {
        if (caret || !n) return;
        (n.children || []).forEach(function (c) {
          if (String(c.className || "").indexOf("tree-caret") >= 0 &&
              c.onclick) { caret = c; return; }
          walk(c);
        });
      })(getEl("stageTree"));
      if (caret && caret.onclick) caret.onclick();
      const rows = [];
      (function walk(n) {
        (n.children || []).forEach(function (c) {
          if (String(c.className || "").indexOf("tree-check") >= 0) {
            const parent = c.parentNode;
            const pad = parent && parent.style && parent.style.paddingLeft;
            rows.push({key: c.getAttribute && c.getAttribute("data-key"),
                       disabled: !!c.disabled, text: textOf(c),
                       pad: String(pad || "")});
          }
          walk(c);
        });
      })(getEl("stageTree"));
      out.stage_tree_rows = rows;
      out.stage_tree_note = textOf(getEl("stageTree"));
    } catch (e) { errs.push("stage-tree: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--trace-tree")) {
    try {
      const tsel = function () {
        return (sandbox.window && sandbox.window.__MC_TSEL)
          ? sandbox.window.__MC_TSEL() : [];
      };
      const ssel = function () {
        return (sandbox.window && sandbox.window.__MC_SSEL)
          ? sandbox.window.__MC_SSEL() : [];
      };
      out.tsel_before = tsel();
      const nb = findBtn(getEl("stageObjTree"), "全选节点");
      if (nb && nb.onclick) nb.onclick();
      out.tsel_nodes = tsel();
      out.tree_btn_after_nodes = treeMiniLabels(getEl("stageObjTree"));
      const cb = findBtn(getEl("stageObjTree"), "全部清空");
      if (cb && cb.onclick) cb.onclick();
      out.tsel_clear = tsel();
      const ib = findBtn(getEl("stageObjTree"), "全选实例");
      out.has_inst_btn = !!ib;
    } catch (e) { errs.push("trace-tree: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--trace-dims")) {
    try {
      const meta = function () {
        const a = charts.tchart || [];
        const o = a.length ? a[a.length - 1] : null;
        return ((o && o.series) || []).map(function (s) {
          return {name: s.name, dash: (s.lineStyle || {}).type,
                  color: (s.lineStyle || {}).color};
        });
      };
      const tsel = function () {
        return (sandbox.window && sandbox.window.__MC_TSEL)
          ? sandbox.window.__MC_TSEL() : [];
      };
      const ssel = function () {
        return (sandbox.window && sandbox.window.__MC_SSEL)
          ? sandbox.window.__MC_SSEL() : [];
      };
      const nodeRows = function () {
        const acc = [];
        (function walk(n) {
          (n.children || []).forEach(function (c) {
            const k = c.getAttribute && c.getAttribute("data-key");
            if (k && k.indexOf("node:") === 0 && c.onclick) acc.push(c);
            walk(c);
          });
        })(getEl("stageObjTree"));
        return acc;
      };
      const clickL1 = function () {
        const l1 = stageTreeL1();
        if (l1 && l1.onclick) l1.onclick();
      };
      // 1) 单对象 × 多阶段
      clickL1();
      out.dim_series_1 = meta();
      out.dim_sel_1 = {obj: tsel(), stage: ssel()};
      // 2) 单阶段选择之上再勾一个节点（不收敛：对象+1，阶段保持）
      const nodes1 = nodeRows();
      if (nodes1[0] && nodes1[0].onclick) nodes1[0].onclick();
      out.dim_series_2 = meta();
      out.dim_sel_2 = {obj: tsel(), stage: ssel()};
      // 3) 再勾一个节点：3 对象 × 2 阶段 = 6 条
      const nodes2 = nodeRows();
      if (nodes2[1] && nodes2[1].onclick) nodes2[1].onclick();
      out.dim_series_3 = meta();
      out.dim_sel_3 = {obj: tsel(), stage: ssel()};
      // 4) 取消该阶段（checkbox）：3 对象 × 1 阶段 = 3 条，回到对象名
      clickL1();
      out.dim_series_4 = meta();
      out.dim_sel_4 = {obj: tsel(), stage: ssel()};
      // 5) 再次加回该阶段（自由组合仍成立）
      clickL1();
      out.dim_series_5 = meta();
      // 6) 清空阶段 → 允许空
      const sc = findBtn(getEl("stageTree"), "清空");
      if (sc && sc.onclick) sc.onclick();
      out.dim_empty_stage = textOf(getEl("tchartEmpty"));
      // 7) 对象全空 + 阶段有选 → 不得回退画总计
      clickL1();
      const oc0 = findBtn(getEl("stageObjTree"), "全部清空");
      if (oc0 && oc0.onclick) oc0.onclick();
      out.dim_no_obj = textOf(getEl("tchartEmpty"));
      out.dim_no_obj_sel = tsel();
    } catch (e) { errs.push("trace-dims: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--blockobj-tree")) {
    try {
      const bbtn = getEl("tBlockObj");
      const menu = getEl("tBlockObjMenu");
      out.bobj_closed = String(menu.style.display || "");
      if (bbtn.onclick) bbtn.onclick();     // 打开
      out.bobj_open = String(menu.style.display || "");
      out.bobj_rows = (menu.children || []).map(function (r) {
        return {key: r.getAttribute && r.getAttribute("data-key"),
                text: r._text, pad: r.style.paddingLeft};
      });
      // 选一个节点对象（从弹层行里取第一行 node:*）
      const row = (menu.children || []).find(function (r) {
        const k = r.getAttribute && r.getAttribute("data-key");
        return k && k.indexOf("node:") === 0;});
      out.bobj_picked = row ? row.getAttribute("data-key") : null;
      if (row && row.onclick) row.onclick();
      out.bobj_closed_after = String(menu.style.display || "");
      out.bobj_btn_after = String(bbtn._text || "");
      out.bobj_levels = textOf(getEl("levels")).slice(0, 40);
    } catch (e) { errs.push("blockobj-tree: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--slow-tracking")) {
    try {
      const seq = charts.tstackchart || [];
      const o = seq.length ? seq[seq.length - 1] : null;
      out.stack_names = ((o && o.series) || []).map(function (x) {
        return x.name;});
      const totals = [];
      ((o && o.series) || []).forEach(function (x) {
        (x.data || []).forEach(function (v, i) {
          totals[i] = (totals[i] || 0) + (Number(v) || 0);
        });
      });
      out.stack_totals = totals;
      // 图例 solo：模拟 ECharts 点击后「只剩被点项」的状态
      const c = chartRefs.tstackchart;
      const hs = (c && c.__h && c.__h.legendselectchanged) || [];
      const names = out.stack_names;
      const clicked = names[0];
      const after = {};
      names.forEach(function (n) { after[n] = (n === clicked); });
      hs.forEach(function (fn) { fn({name: clicked, selected: after}); });
      const o2 = (charts.tstackchart || []).slice(-1)[0] || {};
      out.legend_solo = (o2.legend && o2.legend.selected) || null;
      // 再次点击（此时它是唯一选中 → 恢复全部）
      const after2 = {};
      names.forEach(function (n) { after2[n] = false; });
      hs.forEach(function (fn) { fn({name: clicked, selected: after2}); });
      const o3 = (charts.tstackchart || []).slice(-1)[0] || {};
      out.legend_restored = (o3.legend && o3.legend.selected) || null;
      // 表格与图必须同集：捕获当前组的表格 op 列 / 行数
      function ttopOps() {
        const tb = getEl("ttop");
        const tbody = (tb.children || []).find(function (c) {
          return String(c.tagName || "").toLowerCase() === "tbody";}) ||
          (tb.children || [])[1];
        const body = tbody || getEl("#ttop tbody");
        return ((body && body.children) || []).map(function (tr) {
          const td = (tr.children || [])[3];
          return td ? textOf(td).trim() : "";
        });
      }
      out.ttop_ops = ttopOps();
      out.ttop_rows = out.ttop_ops.length;
      const wantG = arg("--ttop-group", null);
      if (wantG) {
        const label = wantG === "write" ? "写路径"
          : (wantG === "read" ? "读路径" : "其它");
        const gbtn = (getEl("tgroups").children || []).find(function (b) {
          return textOf(b).trim() === label;});
        if (gbtn && gbtn.onclick) gbtn.onclick();
        out.ttop_ops_after = ttopOps();
        out.ttop_rows_after = out.ttop_ops_after.length;
        const seq2 = charts.tstackchart || [];
        const oA = seq2.length ? seq2[seq2.length - 1] : null;
        out.stack_series_after = ((oA && oA.series) || []).map(function (x) {
          return {name: x.name, len: (x.data || []).length};});
      }
      // 表格展开：点击第一行，检查紧凑块图（无 total 根行）
      const tb = getEl("ttop");
      const tbody = (tb.children || []).find(function (c) {
        return String(c.tagName || "").toLowerCase() === "tbody";}) ||
        (tb.children || [])[1];
      const body = tbody || getEl("#ttop tbody");
      const tr0 = (body.children || [])[0];
      if (tr0 && tr0.onclick) {
        tr0.onclick();
        const detail = (body.children || [])[(body.children || []).length - 1];
        const td = detail && (detail.children || [])[0];
        const sbrk = td && (td.children || [])[0];
        out.detail_cls = sbrk ? String(sbrk.className || "") : "";
        const rows = [];
        (function walk(n) {
          (n.children || []).forEach(function (c) {
            if (String(c.className || "").indexOf("lvl-row") >= 0)
              rows.push(String(c.className || ""));
            walk(c);
          });
        })(sbrk || getEl("none"));
        out.detail_rows = rows;
      }
    } catch (e) { errs.push("slow-tracking: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--qps-colors")) {
    try {
      const q=charts.qpschart||[];
      const o=q.length?q[q.length-1]:null;
      out.qps_obj_colors=((o&&o.series)||[]).map(function(x){
        return {name:x.name,color:(x.itemStyle||{}).color};});
      out.qps_meta=((o&&o.series)||[]).map(function(x){
        const d=x.data||[];
        return {name:x.name,type:x.type,width:x.barMaxWidth,
          x0:d.length?d[0][0]:null,
          color:(x.itemStyle||{}).color,
          radius:(x.itemStyle||{}).borderRadius};});
      out.qps_legend_data=(o&&o.legend&&o.legend.data)||null;
      try{
        const qf=(o&&o.tooltip&&o.tooltip.formatter);
        out.qps_fmt_tip=(typeof qf==='function')
          ?String(qf([{value:[0,0],seriesName:'x'}])):null;
      }catch(e){ out.qps_fmt_tip=null; }
    } catch (e) { errs.push("qps-colors: " + String(e && e.stack || e)); }
  }
  out.legend_selmode = (mainOpt()&&mainOpt().legend)
    ? mainOpt().legend.selectedMode : null;
  // 错误与异常 / 日志现场（Phase 2）
  out.errors_text = textOf(getEl("errors"));
  out.logsite_text = textOf(getEl("logsite"));
  out.logsite_table_cls = (function () {
    let found = "";
    (function walk(n) {
      (n.children || []).forEach(function (c) {
        const cls = String(c.className || "");
        if (cls.indexOf("logsite-table") >= 0) found = cls;
        walk(c);
      });
    })(getEl("logsite"));
    return found;
  })();
  out.logsite_wrapx = (function () {
    let ok = false;
    (function walk(n) {
      (n.children || []).forEach(function (c) {
        if (String(c.className || "").indexOf("wrapx") >= 0) ok = true;
        walk(c);
      });
    })(getEl("logsite"));
    return ok;
  })();
  out.logsite_rows = (function () {
    const acc = [];
    (function walk(n) {
      (n.children || []).forEach(function (c) {
        const cls = String(c.className || "");
        const tiers = { "node-row": "node", "pod-row": "pod",
                        "monitor-row": "monitor" };
        for (const k in tiers) {
          if (cls.indexOf(k) >= 0) {
            acc.push({tier: tiers[k],
                      cells: (c.children || []).map(function (td) {
                        return textOf(td).trim();})});
            break;
          }
        }
        walk(c);
      });
    })(getEl("logsite"));
    return acc;
  })();

  function _rowsByClass(root, cls) {
    const acc = [];
    (function walk(n) {
      (n.children || []).forEach(function (c) {
        if (String(c.className || "").indexOf(cls) >= 0) acc.push(c);
        walk(c);
      });
    })(root);
    return acc;
  }
  function _errToggles() {
    const acc = [];
    (function walk(n) {
      (n.children || []).forEach(function (c) {
        if (String(c.className || "").indexOf("err-toggle") >= 0) acc.push(c);
        walk(c);
      });
    })(getEl("errors"));
    return acc;
  }
  out.err_toggles = _errToggles().length;
  out.err_nodes_default = _rowsByClass(getEl("errors"), "node-row").length;
  out.err_clients_default = _rowsByClass(getEl("errors"), "client-row").length;
  out.client_nodes_default = _rowsByClass(getEl("clients"), "node-row").length;
  out.client_rows_default = _rowsByClass(getEl("clients"), "client-row").length;
  if (process.argv.includes("--expand-errors")) {
    let guard = 0;
    while (guard++ < 100) {
      const b = _errToggles().find(function (x) {
        return textOf(x).trim() === "▸";});
      if (!b || !b.onclick) break;
      b.onclick();
    }
    out.errors_text_expanded = textOf(getEl("errors"));
    out.err_clients_expanded =
      _rowsByClass(getEl("errors"), "client-row").length;
    out.err_toggles_expanded = _errToggles().length;
  }
  out.volume_text = textOf(getEl("volume"));
  out.volume_note = textOf(getEl("volumeNote"));

  if (process.argv.includes("--reset-view")) {
    try {
      const c = chartRefs.mainchart;
      const n0 = ((c && c.__acts) || []).length;
      const rb = getEl("resetBtn");
      if (rb && rb.onclick) rb.onclick();
      out.reset_actions = ((c && c.__acts) || []).slice(n0);
      out.reset_cap = textOf(getEl("zsliderCap"));
    } catch (e) { errs.push("reset-view: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--trace-stage-all")) {
    try {
      const series = function () {
        const a = charts.tchart || [];
        const o = a.length ? a[a.length - 1] : null;
        return ((o && o.series) || []).map(function (x) { return x.name; });
      };
      out.tstage_before = series();
      const allb = findBtn(getEl("stageTree"), "全选");
      if (allb && allb.onclick) allb.onclick();
      out.tstage_all = series();
      const clrb = findBtn(getEl("stageTree"), "清空");
      if (clrb && clrb.onclick) clrb.onclick();
      out.tstage_clear_sel = (sandbox.window && sandbox.window.__MC_SSEL)
        ? sandbox.window.__MC_SSEL() : null;
    } catch (e) { errs.push("trace-stage-all: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--tchart-labels")) {
    try {
      // 先点阶段树第一个 L1 → 两条序列（多阶段模式）
      const l1 = stageTreeL1();
      if (l1 && l1.onclick) l1.onclick();
      const ov = getEl("tEndlabels");
      const labels = (ov.children || []).filter(function (c) {
        return String(c.className || "").indexOf("endlabel") >= 0;});
      out.tlabel_texts = labels.map(function (c) { return textOf(c); });
      out.tlabel_classes = labels.map(function (c) {
        return String(c.className || "");});
      out.tlabel_leader = (ov.children || []).filter(function (c) {
        return String(c.className || "").indexOf("endleader") >= 0;}).length;
      const series = function () {
        const a = charts.tchart || [];
        const o = a.length ? a[a.length - 1] : null;
        return ((o && o.series) || []).map(function (x) {
          return (x.lineStyle || {}).opacity;});
      };
      if (labels[0] && labels[0].onmouseenter) {
        labels[0].onmouseenter();
        out.tlabel_dims = series();
        if (labels[0].onmouseleave) labels[0].onmouseleave();
        out.tlabel_restored = series();
      }
    } catch (e) { errs.push("tchart-labels: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--combo-fallback")) {
    try {
      const sel = function () {
        return (sandbox.window && sandbox.window.__MC_SEL)
          ? sandbox.window.__MC_SEL() : [];
      };
      // 先展开第一个节点，让客户端行出现
      (function walk(n) {
        let done = false;
        (function go(n) {
          if (done || !n) return;
          (n.children || []).forEach(function (c) {
            if (done) return;
            if (String(c.className || "").indexOf("tree-caret") >= 0 &&
                c.onclick) { c.onclick(); done = true; return; }
            go(c);
          });
        })(n);
      })(getEl("objTree"));
      let clicked = false;
      (function walk(n) {
        if (clicked || !n) return;
        (n.children || []).forEach(function (c) {
          const k = c.getAttribute && c.getAttribute("data-key");
          if (k && k.indexOf("client:") === 0 && c.onclick) {
            c.onclick(); clicked = true; return;
          }
          walk(c);
        });
      })(getEl("objTree"));
      out.combo_sel_before = sel();
      const btns = (getEl("comboBtns").children || []);
      const target = btns[btns.length - 1];
      if (target && target.onclick) target.onclick();
      out.combo_sel_after = sel();
    } catch (e) { errs.push("combo-fallback: " + String(e && e.stack || e)); }
  }
  if (process.argv.includes("--endlabels")) {
    try {
      const top = 52, bot = 350;
      const cases = [
        {name: "edge-clamp", pys: [-80, 40, 200, 900]},
        {name: "bottom-cluster+top", pys: [60, 348, 350, 352, 353, 354, 354]},
        {name: "top+bottom", pys: [52, 52, 52, 353, 354, 354, 354]},
        {name: "top-cluster", pys: [52, 52, 52, 53, 60, 80, 120]},
        {name: "all-bottom", pys: [354, 354, 354, 354, 354, 354, 354]},
      ];
      out.endlabel_bounds = [top, bot];
      out.endlabel_cases = cases.map(function (c) {
        const laid = sandbox.endLabelLayout(c.pys.map(function (py) {
          return {px: 600, py: py, ly: 0};
        }), top, bot) || [];
        const lys = laid.map(function (it) { return it.ly; });
        let minGap = Infinity;
        for (let i = 1; i < lys.length; i++) {
          minGap = Math.min(minGap, lys[i] - lys[i - 1]);
        }
        return {name: c.name, pys: laid.map(function (it) { return it.py; }),
                lys: lys, min_gap: minGap};
      });
      out.endlabels = out.endlabel_cases[0].lys.map(function (ly, i) {
        return {py: out.endlabel_cases[0].pys[i], ly: ly};
      });
    } catch (e) {
      errs.push("endlabels: " + String(e && e.stack || e));
    }
  }

  const hsizes = data.sizes || {};
  const hsize = Object.keys(hsizes)[0];
  const zz = hsize ? hsizes[hsize] : null;
  if (zz && zz.trace) {
    out.levels_text = textOf(getEl("levels"));
    out.tree_text = textOf(getEl("ttree"));
    let clicked = false;
    (getEl("ttree").children || []).forEach((row) => {
      if (clicked) return;
      const name = (row.children || []).find((c) => c.className === "tname");
      if (name && name.onclick) { name.onclick(); clicked = true; }
    });
    out.detail_text = textOf(getEl("tdetail"));
    out.tcards_kids = kids("tcards");
    const tseq = charts.tstackchart || [];
    const topt = tseq.length ? tseq[tseq.length - 1] : null;
    out.tstack_series = ((topt && topt.series) || []).map(function (s) {
      return {name: s.name, stack: s.stack,
              color: (s.itemStyle || {}).color,
              len: (s.data || []).length};
    });
    out.tstack_cats = (topt && topt.xAxis && topt.xAxis.data)
      ? topt.xAxis.data.length : null;
    try {
      const _anyTop = (function () {
        const gs = (zz.trace && zz.trace.groups) || {};
        for (const k in gs) {
          const lst = gs[k].top || [];
          if (lst.length) return lst[0];
        }
        return null;
      })();
      const bk = _anyTop ? sandbox.stageBreakdownRow(_anyTop) : null;
      out.tbreak_text = bk ? textOf(bk) : "";
    } catch (e) { errs.push("tbreak: " + String(e && e.stack || e)); }
    try {
      out.lvl_line_w = [
        sandbox.lvlLineW("残差", "1.5 µs  0.0%"),
        sandbox.lvlLineW("buf_alloc_us", "1.3 µs  0.0%"),
        sandbox.lvlLineW("query_total_us", "18.8 ms  50.3%")
      ];
      out.lvl_zero_px = [
        sandbox.lvlZeroW("残差", "1.5 µs  0.0%"),
        sandbox.lvlZeroW("buf_alloc_us", "1.3 µs  0.0%")
      ];
    } catch (e) { errs.push("lvlw: " + String(e && e.stack || e)); }
    try {
      out.stage_tier_btns = (getEl("stageTierBtns").children || [])
        .map(function (b) { return b._text; });
    } catch (e) { errs.push("stier: " + String(e && e.stack || e)); }
  }

  if (switchSize) {
    try {
      const sel = getEl("sizeSel");
      sel.value = switchSize;
      sel.onchange();
    } catch (e) {
      out.switched_error = true;
      errs.push("size switch: " + String(e && e.stack || e));
    }
    if (expectTrace !== null) {
      out.trace_visible = getEl("tracecard").style.display !== "none";
    }
  }
}
if (useReal && !errs.length) {
  const zoomArg = process.env.VC_ZOOM || null;   /* "30,60" -> dispatch 后再渲染 */
  if (zoomArg) {
    const parts = zoomArg.split(",").map(Number);
    realCharts.forEach((rc) => {
      if (rc.id === "mainchart" || rc.id === "qpschart") {
        try {
          rc.chart.dispatchAction({ type: "dataZoom", dataZoomIndex: 0,
            start: parts[0], end: parts[1] });
        } catch (e) { errs.push("zoom " + rc.id + ": " + e.message); }
      }
    });
    /* 缩放联动断言（R-LAT-6）：dispatch 后读取两图的 dataZoom 窗口 */
    try {
      const zs = {};
      realCharts.forEach((rc) => {
        if (rc.id !== "mainchart" && rc.id !== "qpschart") return;
        const o = rc.chart.getOption();
        const dz = (o.dataZoom || [])[0] || {};
        zs[rc.id] = { start: dz.start, end: dz.end };
      });
      out.zoom_states = zs;
    } catch (e) { errs.push("zoom states: " + e.message); }
  }
  const svgDir = process.env.VC_SVGDIR || null;
  const real = { total: realCharts.length, ok: 0, fail: 0, fails: [] };
  realCharts.forEach((rc) => {
    try {
      const svg = rc.chart.renderToSVGString();
      if (svgDir) {
        fs.mkdirSync(svgDir, { recursive: true });
        fs.writeFileSync(svgDir + "/" + rc.id + "_" +
                         (process.env.VC_WIDTH || 960) + ".svg", svg);
      }
      if (!svg || svg.length < 200) throw new Error("empty svg " + (svg || "").length);
      real.ok++;
    } catch (e) {
      real.fail++;
      if (real.fails.length < 5) {
        real.fails.push(rc.id + ": " + String(e && e.message || e));
      }
    }
  });
  out.real = real;
  if (real.fail) errs.push("real-echarts render failures: " +
                           real.fails.join(" | "));
}
process.stdout.write(JSON.stringify(out) + "\n");
process.exit(errs.length ? 1 : 0);
