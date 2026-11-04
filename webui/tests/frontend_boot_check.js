#!/usr/bin/env node
/* 前端启动冒烟：用桩 Vue/ElementPlus/DOM 顺序执行全部脚本，
   捕获顶层 ReferenceError，并校验 app.component 注册的组件均已定义。
   （浏览器白屏最常见原因：app.js 引用了未定义的组件常量。） */
"use strict";

const fs = require("fs");
const path = require("path");
const vm = require("vm");

const jsDir = path.resolve(__dirname, "..", "web", "js");
const files = [
  "spdiag.js",
  "views/config-view.js",
  "views/perf-view.js",
  "views/results-view.js",
  "views/schedule-view.js",
  "views/spdiag-tree.js",
  "app.js",
];

const noop = () => {};
const ref = v => ({ value: v });
const reactive = o => o;
const computed = fn => ({ get value() { return fn(); } });
const createApp = () => {
  const comps = {};
  const app = {
    use() { return app; },
    component(name, comp) {
      if (arguments.length === 1) return comps[name];
      comps[name] = comp;
      registered.push([name, comp]);
      return app;
    },
    mount() { return app; },
  };
  return app;
};
const registered = [];

const Vue = new Proxy({
  createApp, reactive, computed, ref,
  watch: noop, nextTick: f => f && f(),
  onMounted: noop, onUnmounted: noop, onActivated: noop, onDeactivated: noop,
  provide: noop, inject: () => noop,
}, {
  get(t, p) { return (p in t) ? t[p] : noop; },
  has() { return true; },
});

const elStub = new Proxy({}, { get: () => noop });
const sandbox = {
  console,
  setTimeout, clearTimeout, setInterval, clearInterval,
  fetch: () => Promise.resolve({ ok: true, json: async () => ({}) }),
  Vue,
  ElementPlus: elStub,
  ElementPlusIconsVue: {},
  ElementPlusLocaleZhCn: {},
  localStorage: { getItem: () => null, setItem: noop, removeItem: noop },
  window: {
    matchMedia: () => ({ matches: false }),
    addEventListener: noop, removeEventListener: noop,
  },
  document: {
    documentElement: { classList: { toggle: noop } },
    getElementById: () => ({}),
    addEventListener: noop, removeEventListener: noop,
  },
};
sandbox.globalThis = sandbox;
const ctx = vm.createContext(sandbox);

let failed = false;
for (const rel of files) {
  const fp = path.join(jsDir, rel);
  const code = fs.readFileSync(fp, "utf8");
  try {
    vm.runInContext(code, ctx, { filename: fp });
  } catch (e) {
    failed = true;
    console.error("[FAIL] " + rel + ": " + e.name + ": " + e.message);
  }
}

const mustHave = ["perf-view", "sel-node", "config-view", "cfg-node",
                  "results-view", "res-pane", "schedule-view",
                  "spdiag-tree", "sdt-node", "drag-handle"];
for (const [name, comp] of registered) {
  if (comp === undefined) {
    failed = true;
    console.error("[FAIL] app.component('" + name + "') received undefined");
  }
}
for (const name of mustHave) {
  if (!registered.some(([n]) => n === name)) {
    failed = true;
    console.error("[FAIL] component not registered: " + name);
  }
}

if (failed) process.exit(1);
console.log("FRONTEND_BOOT_OK (" + registered.map(([n]) => n).join(", ") + ")");
