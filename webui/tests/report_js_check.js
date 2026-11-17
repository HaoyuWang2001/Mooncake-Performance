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
    appendChild(c) { this.children.push(c); return c; },
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
    const c = realEcharts.init(null, null,
      { renderer: "svg", ssr: true, width: 960, height: 420 });
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

const errs = [];
try {
  vm.runInContext(fs.readFileSync(appPath, "utf-8"), sandbox,
                  { filename: "app.js" });
  if (!domReady) throw new Error("DOMContentLoaded handler not registered");
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
  out.init_len = seriesLen(mainOpt(), "p50");
  out.init_x0 = seriesX0(mainOpt(), "p50");

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
      const btn = getEl("outlierBtn");
      btn.onclick();
      const opt = mainOpt();
      out.outlier_nulls = ((opt && opt.series) || []).map(function (s) {
        return (s.data || []).filter(function (d) { return d[1] === null; }).length;
      });
      out.outlier_max = Math.max.apply(null, [0].concat.apply([], ((opt && opt.series) || []).map(function (s) {
        return (s.data || []).map(function (d) { return (d && d[1] != null) ? d[1] : 0; });
      })));
      out.outlier_btn = textOf(btn);
    } catch (e) { errs.push("outlier: " + String(e && e.stack || e)); }
  }

  const _tc=(charts.tchart||[]); const _tco=_tc.length?_tc[_tc.length-1]:null;
  out.tchart_series=((_tco&&_tco.series)||[]).map(function(s){return s.name;});
  out.tchart_empty=textOf(getEl("tchartEmpty"));
  out.yscale_hi = textOf(getEl("yscaleHi"));
  out.yscale_lo = textOf(getEl("yscaleLo"));
  out.tree_rows = (getEl("objTree").children || []).map(function (r) {
    return textOf(r);
  });
  out.qps_series = (charts.qpschart || []).length;
  out.qps_note = textOf(getEl("qpsNote"));

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
  const real = { total: realCharts.length, ok: 0, fail: 0, fails: [] };
  realCharts.forEach((rc) => {
    try {
      const svg = rc.chart.renderToSVGString();
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
