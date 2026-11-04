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

// 回归：选择树计数需正确处理叶子节点（曾有 children 缺失导致整页渲染崩溃）
try {
  const sample = [{ isDir: true, checked: false, count: 3, children: [
    { isDir: false, checked: false, children: [] },
    { isDir: false, checked: true, children: [] },
  ] }];
  const n = vm.runInContext("_perfCountSel(" + JSON.stringify(sample) + ")", ctx);
  if (n !== 1) { failed = true; console.error("[FAIL] _perfCountSel expected 1, got " + n); }
  const c = vm.runInContext("_perfTreeCount({files:['a'],dirs:[{files:['b','c'],dirs:[]}]})", ctx);
  if (c !== 3) { failed = true; console.error("[FAIL] _perfTreeCount expected 3, got " + c); }
} catch (e) {
  failed = true;
  console.error("[FAIL] selection tree helpers: " + e.message);
}

// 回归：模式候选过滤（backend owner=_ssh/_k8s, mode=ssh/k8s）
try {
  const cands = [
    { path: "configs/cluster.json", owner: "root" },
    { path: "configs/_ssh/cluster.json", owner: "_ssh" },
    { path: "configs/_k8s/cluster.json", owner: "_k8s" },
    "legacy/path.json",
  ];
  const ssh = vm.runInContext(
    "_perfModeCandidates(" + JSON.stringify(cands) + ',"ssh")', ctx);
  const k8s = vm.runInContext(
    "_perfModeCandidates(" + JSON.stringify(cands) + ',"k8s")', ctx);
  const sshPaths = ssh.map(c => c.path || c).sort();
  const k8sPaths = k8s.map(c => c.path || c).sort();
  if (JSON.stringify(sshPaths) !== JSON.stringify([
      "configs/_ssh/cluster.json", "configs/cluster.json", "legacy/path.json"])) {
    failed = true;
    console.error("[FAIL] ssh candidates: " + JSON.stringify(sshPaths));
  }
  if (JSON.stringify(k8sPaths) !== JSON.stringify([
      "configs/_k8s/cluster.json", "legacy/path.json"])) {
    failed = true;
    console.error("[FAIL] k8s candidates: " + JSON.stringify(k8sPaths));
  }
} catch (e) {
  failed = true;
  console.error("[FAIL] _perfModeCandidates: " + e.message);
}

// 回归：默认仅精确同名；变体不自动选中；勾选按 path 迁移
try {
  const cands = [
    { path: "configs/_k8s/cluster.2node.json", owner: "_k8s" },
    { path: "configs/_k8s/cluster.json", owner: "_k8s" },
  ];
  const pick = vm.runInContext(
    "_perfPickDefault(" + JSON.stringify(cands) + ',"cluster.json")', ctx);
  if (pick !== "configs/_k8s/cluster.json") {
    failed = true;
    console.error("[FAIL] _perfPickDefault exact: " + pick);
  }
  const pickMissing = vm.runInContext(
    "_perfPickDefault(" + JSON.stringify([cands[0]]) + ',"cluster.json")', ctx);
  if (pickMissing !== "") {
    failed = true;
    console.error("[FAIL] _perfPickDefault missing should be empty: " + pickMissing);
  }
  const migrated = vm.runInContext(`(function () {
    const oldTree = [
      { path: "configs/basic", checked: true, children: [] },
      { path: "configs/pipeline", checked: false, children: [
        { path: "configs/pipeline/a.json", checked: true, children: [] },
      ] },
    ];
    const set = _perfCheckedPaths(oldTree);
    const fresh = [
      { path: "configs/basic", checked: false, children: [] },
      { path: "configs/pipeline", checked: false, children: [
        { path: "configs/pipeline/a.json", checked: false, children: [] },
      ] },
    ];
    _perfApplyChecked(fresh, set);
    return fresh[0].checked === true && fresh[1].checked === false &&
           fresh[1].children[0].checked === true;
  })()`, ctx);
  if (!migrated) {
    failed = true;
    console.error("[FAIL] _perfCheckedPaths/_perfApplyChecked roundtrip");
  }
} catch (e) {
  failed = true;
  console.error("[FAIL] default/checked helpers: " + e.message);
}

if (failed) process.exit(1);
console.log("FRONTEND_BOOT_OK (" + registered.map(([n]) => n).join(", ") + ")");
