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

const charts = {};        // elId -> [{opt, merge}]
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
  out.cards_text = textOf(getEl("cards"));
  out.init_len = seriesLen(mainOpt(), "p50");
  out.init_x0 = seriesX0(mainOpt(), "p50");

  // 点击「1 分钟粒度」按钮（第二个 step 按钮，若存在）
  const stepBox = getEl("steps");
  const btn = (stepBox.children || []).find(
    (b) => String(b._text) === "1 分钟粒度");
  if (btn && btn.onclick) {
    try { btn.onclick(); out.min_len = seriesLen(mainOpt(), "p50"); }
    catch (e) { errs.push("step switch: " + String(e)); }
  } else {
    out.min_len = out.init_len;
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
process.stdout.write(JSON.stringify(out) + "\n");
process.exit(errs.length ? 1 : 0);
