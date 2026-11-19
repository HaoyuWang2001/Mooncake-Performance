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
function echartsInit(el) {
  const id = el.id || "anon";
  if (!charts[id]) charts[id] = [];
  return {
    setOption(opt) { charts[id].push(opt); },
    clear() {},
    resize() {},
  };
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
  out.lat_btns = (getEl("latSel").children || []).map(function (c) {
    return {t: textOf(c), off: String(c.className || "").indexOf("off") >= 0};
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
  if (process.argv.includes("--sel-all-clear")) {
    try {
      const sel = function () {
        return (sandbox.window && sandbox.window.__MC_SEL)
          ? sandbox.window.__MC_SEL() : [];
      };
      const sa = getEl("selAllBtn");
      if (sa && sa.onclick) sa.onclick();
      out.selobjs_all = sel();
      const sc = getEl("selClearBtn");
      if (sc && sc.onclick) sc.onclick();
      out.selobjs_cleared = sel();
    } catch (e) { errs.push("selall: " + String(e && e.stack || e)); }
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
      const ab = findBtn(getEl("slowctl"), "全选");
      if (ab && ab.onclick) ab.onclick();
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
  out.info_kids = kids("infocards");
  out.unit_kids = kids("unitmeta");
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
  const _qseq = charts.qpschart || [];
  const _qo = _qseq.length ? _qseq[_qseq.length - 1] : null;
  out.qps_markarea = (_qo && _qo.series && _qo.series[0] &&
    _qo.series[0].markArea && _qo.series[0].markArea.data) || null;
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
    } catch (e) { errs.push("add obj: " + String(e && e.stack || e)); }
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
    } catch (e) { errs.push("outlier: " + String(e && e.stack || e)); }
  }

  const _tc=(charts.tchart||[]); const _tco=_tc.length?_tc[_tc.length-1]:null;
  out.tchart_series=((_tco&&_tco.series)||[]).map(function(s){return s.name;});
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

  if (process.argv.includes("--click-client")) {
    try {
      const tb = getEl("#ctable tbody");
      const tr0 = (tb.children || [])[0];
      const sel = function () {
        return (sandbox.window && sandbox.window.__MC_SEL)
          ? sandbox.window.__MC_SEL() : [];
      };
      const before = JSON.stringify(sel());
      const mseq0 = charts.mainchart || [];
      const mo0 = mseq0.length ? mseq0[mseq0.length - 1] : null;
      out.main_series_before = (mo0 && mo0.series) ? mo0.series.length : null;
      out.row_onclick = !!(tr0 && tr0.onclick);
      if (tr0 && tr0.onclick) tr0.onclick();
      out.selobjs_before = before;
      out.selobjs_after = JSON.stringify(sel());
      out.rows_after = (tb.children || []).length;
      out.cld_rows = (tb.children || []).filter(function (r) {
        return String(r.className || "").indexOf("cld-row") >= 0;
      }).length;
      const mseq1 = charts.mainchart || [];
      const mo1 = mseq1.length ? mseq1[mseq1.length - 1] : null;
      out.main_series_after = (mo1 && mo1.series) ? mo1.series.length : null;
    } catch (e) { errs.push("clickclient: " + String(e && e.stack || e)); }
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
      const bk = sandbox.stageBreakdownRow(zz.trace.top[0]);
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
