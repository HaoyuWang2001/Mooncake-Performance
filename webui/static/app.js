"use strict";
/* Mooncake-Performance WebUI frontend (vanilla JS, no build step) */

const $ = s => document.querySelector(s);
const $$ = s => Array.from(document.querySelectorAll(s));

async function api(method, url, body) {
  const opt = { method, headers: { "Content-Type": "application/json" } };
  if (body !== undefined) opt.body = JSON.stringify(body);
  const resp = await fetch(url, opt);
  let data = {};
  try { data = await resp.json(); } catch (e) { /* empty */ }
  if (!resp.ok) throw new Error(data.error || (resp.status + " " + resp.statusText));
  return data;
}

const S = {
  cfg: null, selMode: "file", selFile: null, selDir: null, selCat: null,
  runId: null, runLive: false, logOffset: 0, logRem: "",
  viewRunId: null, viewLive: false, viewOffset: 0, viewRem: "",
  outTs: null, cfgPath: null, planName: null,
};

/* ----------------------------------------------------------------- tabs */
function switchTab(name) {
  $$("#tabs button").forEach(b => b.classList.toggle("active", b.dataset.tab === name));
  $$(".tab").forEach(t => t.classList.toggle("active", t.id === "tab-" + name));
  if (name === "results") loadOutputs();
  if (name === "configs" && !S.cfgPath) loadConfigs();
  if (name === "schedule") { loadPlans(); }
  if (name === "api") loadApiDoc();
}

/* ------------------------------------------------------------ config tree */
function renderTree(el, onPick, opts) {
  el.innerHTML = "";
  const cfg = S.cfg;
  if (!cfg) return;
  const mk = (text, cls, onclick) => {
    const d = document.createElement("div");
    d.className = "leaf " + (cls || "");
    d.textContent = text;
    d.onclick = onclick;
    return d;
  };
  for (const cat of ["basic", "pipeline"]) {
    const g = document.createElement("div");
    g.className = "grp";
    g.textContent = cat + " (" + cfg.categories[cat].configs.length + " configs)";
    el.appendChild(g);
    if (opts.dirPick) {
      el.appendChild(mk(cat + "/  [目录运行]", "dirsel",
        ev => onPick({ type: "dir", path: "configs/" + cat, scenario: cat, label: "configs/" + cat }, ev)));
      el.appendChild(mk("demo/  [demo 冒烟]", "dirsel",
        ev => onPick({ type: "demo", scenario: cat, label: cat + " demo" }, ev)));
    }
    for (const f of cfg.categories[cat].configs) {
      el.appendChild(mk(f, "", ev => onPick({ type: "file", path: f, scenario: cat, label: f }, ev)));
    }
  }
  const g2 = document.createElement("div");
  g2.className = "grp";
  g2.textContent = "特殊配置（可在 Configs 页编辑）";
  el.appendChild(g2);
  for (const key of ["cluster", "global", "matrix"]) {
    const info = cfg.special[key];
    el.appendChild(mk(info.path + (info.present ? "" : "  (missing)"), "special",
      ev => onPick({ type: "file", path: info.path, special: true, label: info.path }, ev)));
  }
}

function onRunPick(sel, ev) {
  S.selFile = null; S.selDir = null;
  if (sel.type === "file" && !sel.special) S.selFile = sel;
  else if (sel.type === "dir") { S.selDir = sel; S.selCat = sel.scenario; }
  else if (sel.type === "demo") { S.selDir = { type: "demo", scenario: sel.scenario }; }
  else if (sel.special) { switchTab("configs"); openConfig(sel.path); return; }
  $$("#cfg-tree .leaf").forEach(x => x.classList.remove("sel"));
  if (ev && ev.target) ev.target.classList.add("sel");
}

function onCfgPick(sel) {
  if (sel.path) openConfig(sel.path);
}

async function loadConfigs() {
  S.cfg = await api("GET", "/api/configs");
  renderTree($("#cfg-tree"), onRunPick, { dirPick: true });
  renderTree($("#cfg-tree2"), onCfgPick, { dirPick: false });
  const sel = $("#f-cluster");
  sel.innerHTML = "";
  for (const c of S.cfg.cluster_candidates) {
    const o = document.createElement("option");
    o.textContent = c;
    if (c === "configs/cluster.json") o.selected = true;
    sel.appendChild(o);
  }
  const m = S.cfg.special.matrix;
  $("#matrix-hint").textContent = m.present
    ? "matrix.json 存在 → auto 即启用 profiling" : "无 matrix.json → profiling off";
}

/* ------------------------------------------------------------------- run */
function runFlags() {
  return {
    dry_run: $("#f-dry").checked,
    no_warmup: $("#f-nowarmup").checked,
    no_plot: $("#f-noplot").checked,
    protocol: $("#f-protocol").value || null,
    cluster: $("#f-cluster").value || "configs/cluster.json",
    global: "configs/global.json",
    profile: $("#f-profile").value || "auto",
  };
}

async function startRun() {
  let selector;
  const mode = S.selMode;
  if (mode === "file") {
    if (!S.selFile) { alert("先在左侧选择一个配置文件"); return; }
    selector = { type: "file", path: S.selFile.path, scenario: S.selFile.scenario };
  } else if (mode === "dir") {
    if (!S.selDir) { alert("先点击一个目录/类别条目"); return; }
    selector = { type: "dir", path: S.selDir.path, scenario: S.selDir.scenario, recursive: true };
  } else {
    const cat = S.selCat || (S.selDir && S.selDir.scenario);
    if (!cat) { alert("先点击一个类别（demo 或 目录条目）"); return; }
    selector = { type: mode, scenario: cat };
  }
  try {
    const r = await api("POST", "/api/run", { selector, flags: runFlags() });
    $("#run-warn").classList.toggle("hidden", !(r.warnings && r.warnings.length));
    if (r.warnings) $("#run-warn").textContent = r.warnings.join("；");
    viewRun(r.run.id, true);
    refreshRuns();
  } catch (e) {
    $("#run-warn").classList.remove("hidden");
    $("#run-warn").textContent = "启动失败: " + e.message;
  }
}

function appendLog(container, text, rem) {
  const lines = (rem + text).split("\n");
  const keep = lines.pop();
  for (const ln of lines) {
    const d = document.createElement("div");
    if (/\bFAIL\b|\bERROR\b/.test(ln)) d.className = "ln-err";
    else if (/\bWARN\b/.test(ln)) d.className = "ln-warn";
    else if (/SUMMARY: all .* OK|queue finished/.test(ln)) d.className = "ln-ok";
    d.textContent = ln;
    container.appendChild(d);
  }
  const atBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 60;
  if (atBottom) container.scrollTop = container.scrollHeight;
  return keep;
}

function viewRun(rid, live) {
  S.viewRunId = rid; S.viewLive = live; S.viewOffset = 0; S.viewRem = "";
  $("#log-view").innerHTML = "";
  pollViewRun();
}

async function pollViewRun() {
  if (!S.viewRunId) return;
  try {
    const run = await api("GET", "/api/runs/" + S.viewRunId);
    const st = $("#run-status");
    st.textContent = run.status;
    st.className = "status " + run.status;
    $("#run-meta").textContent =
      run.id + " · " + Math.round(run.elapsed) + "s · rc=" + run.rc +
      (run.ts_dir ? " · " + run.ts_dir : "");
    $("#btn-stop").classList.toggle("hidden", run.status !== "running");
    $("#btn-goto-results").classList.toggle("hidden", !run.ts_dir);
    S.viewTsDir = run.ts_dir;
    if (S.viewLive && run.status !== "running") {
      S.viewLive = false;
      refreshRuns();
    }
    const lg = await api("GET", "/api/runs/" + S.viewRunId + "/log?offset=" + S.viewOffset);
    if (lg.content) {
      S.viewRem = appendLog($("#log-view"), lg.content, S.viewRem);
      S.viewOffset = lg.offset;
    } else if (lg.content === "" && lg.size > S.viewOffset) {
      S.viewOffset = lg.offset;
    }
    if (!S.viewLive && lg.eof && S.viewRem) {
      const d = document.createElement("div");
      d.textContent = S.viewRem;
      $("#log-view").appendChild(d);
      S.viewRem = "";
    }
  } catch (e) { /* run pruned */ }
}

async function stopRun() {
  if (!S.viewRunId) return;
  try { await api("POST", "/api/runs/" + S.viewRunId + "/stop"); }
  catch (e) { alert(e.message); }
}

async function refreshRuns() {
  try {
    const r = await api("GET", "/api/runs");
    const tb = $("#runs-table tbody");
    tb.innerHTML = "";
    for (const run of r.runs) {
      const tr = document.createElement("tr");
      tr.innerHTML = "<td>" + run.id + "</td>" +
        "<td class='status " + run.status + "'>" + run.status + "</td>" +
        "<td>" + (run.rc === null ? "-" : run.rc) + "</td>" +
        "<td>" + Math.round(run.elapsed) + "s</td>" +
        "<td title='" + run.cmd + "'>" + run.cmd + "</td>";
      tr.onclick = () => viewRun(run.id, run.status === "running");
      tb.appendChild(tr);
    }
  } catch (e) { /* ignore */ }
}

/* --------------------------------------------------------------- results */
async function loadOutputs() {
  const r = await api("GET", "/api/outputs");
  const el = $("#outputs-list");
  el.innerHTML = "";
  for (const o of r.outputs) {
    const d = document.createElement("div");
    d.className = "item" + (S.outTs === o.ts ? " sel" : "");
    d.innerHTML = o.ts +
      " <span class='tag " + (o.has_detail ? "ok'>detail" : "bad'>no-detail") + "</span>" +
      "<div class='sub'>" + o.mtime + " · " + o.csv_count + " csv</div>";
    d.onclick = () => selectOutput(o.ts);
    el.appendChild(d);
  }
}

async function selectOutput(ts) {
  S.outTs = ts;
  $("#out-title").textContent = "Run " + ts;
  const r = await api("GET", "/api/outputs/" + encodeURIComponent(ts) + "/tree");
  const el = $("#out-files");
  el.innerHTML = "";
  for (const f of r.files) {
    const d = document.createElement("div");
    d.className = "f" + (f.endsWith(".png") ? " png" : f.endsWith(".csv") ? " csv" : "");
    d.textContent = f;
    d.onclick = () => openOutFile(ts, f);
    el.appendChild(d);
  }
  loadOutputs();
}

async function openOutFile(ts, f) {
  $$("#out-files .f").forEach(x => x.classList.toggle("sel", x.textContent === f));
  const path = "outputs/" + ts + "/" + f;
  const v = $("#out-viewer");
  v.innerHTML = "";
  if (f.endsWith(".png")) {
    const img = document.createElement("img");
    img.src = "/api/file?path=" + encodeURIComponent(path) + "&mode=raw";
    v.appendChild(img);
  } else if (f.endsWith(".csv")) {
    const r = await api("GET", "/api/csv?path=" + encodeURIComponent(path));
    const wrap = document.createElement("div");
    wrap.className = "tblwrap";
    const t = document.createElement("table");
    t.className = "tbl";
    t.innerHTML = "<thead><tr>" + r.headers.map(h => "<th>" + h + "</th>").join("") + "</tr></thead>" +
      "<tbody>" + r.rows.slice(0, 300).map(row =>
        "<tr>" + row.map(c => "<td>" + c + "</td>").join("") + "</tr>").join("") + "</tbody>";
    wrap.appendChild(t);
    v.appendChild(wrap);
    if (r.rows.length > 300 || r.truncated) {
      const n = document.createElement("div");
      n.className = "sub";
      n.textContent = "显示前 300 行 / " + (r.truncated ? "已截断" : r.rows.length + " 行");
      v.appendChild(n);
    }
  } else {
    const r = await api("GET", "/api/file?path=" + encodeURIComponent(path) + "&tail=131072");
    const pre = document.createElement("div");
    pre.className = "txt";
    pre.textContent = r.content;
    v.appendChild(pre);
  }
}

/* ---------------------------------------------------------------- configs */
async function openConfig(path) {
  S.cfgPath = path;
  $("#cfg-edit-title").textContent = "编辑器 — " + path;
  $("#cfg-msg").textContent = "";
  try {
    const r = await api("GET", "/api/config?path=" + encodeURIComponent(path));
    $("#cfg-editor").value = r.content;
  } catch (e) {
    $("#cfg-editor").value = "";
    $("#cfg-msg").textContent = e.message;
    $("#cfg-msg").className = "msg err";
  }
}

async function validateConfig() {
  if (!S.cfgPath) return;
  try {
    const r = await api("POST", "/api/config/validate",
      { path: S.cfgPath, content: $("#cfg-editor").value });
    const ok = r.errors.length === 0;
    $("#cfg-msg").textContent = ok ? "校验通过" : r.errors.join("；");
    $("#cfg-msg").className = "msg " + (ok ? "ok" : "err");
  } catch (e) {
    $("#cfg-msg").textContent = e.message;
    $("#cfg-msg").className = "msg err";
  }
}

async function saveConfig() {
  if (!S.cfgPath) return;
  try {
    const r = await api("PUT", "/api/config",
      { path: S.cfgPath, content: $("#cfg-editor").value });
    $("#cfg-msg").textContent = "已保存" + (r.backup ? "（备份 " + r.backup + "）" : "");
    $("#cfg-msg").className = "msg ok";
    loadConfigs();
  } catch (e) {
    $("#cfg-msg").textContent = e.message;
    $("#cfg-msg").className = "msg err";
  }
}

/* --------------------------------------------------------------- schedule */
async function loadPlans() {
  const r = await api("GET", "/api/schedule/plans");
  const el = $("#plans-list");
  el.innerHTML = "";
  for (const p of r.plans) {
    const d = document.createElement("div");
    d.className = "item" + (S.planName === p.name ? " sel" : "");
    d.innerHTML = p.name +
      (p.example ? " <span class='tag ok'>模板</span>" : "") +
      "<div class='sub'>" + (p.valid
        ? (p.tasks + " tasks · 首任务 " + (p.first_task || "链式?"))
        : "无效: " + p.error) + "</div>";
    d.onclick = () => openPlan(p.name);
    el.appendChild(d);
  }
}

async function openPlan(name) {
  S.planName = name;
  $("#plan-edit-title").textContent = "计划编辑器 — " + name;
  $("#plan-msg").textContent = "";
  try {
    const r = await api("GET", "/api/schedule/plan/" + encodeURIComponent(name));
    $("#plan-editor").value = r.content;
    loadPlans();
  } catch (e) {
    $("#plan-msg").textContent = e.message;
  }
}

function newPlan() {
  const name = $("#new-plan-name").value.trim();
  if (!name.endsWith(".json")) { alert("计划名必须以 .json 结尾"); return; }
  S.planName = name;
  $("#plan-edit-title").textContent = "计划编辑器 — " + name + " (新)";
  $("#plan-editor").value = JSON.stringify({
    wait_seconds: 60, on_fail: "continue",
    tasks: [{ date: "2026-01-01", time: "23:00",
              command: "python benchmark.py --scenario basic --demo",
              output: "logs/night/demo.log" }],
  }, null, 2);
}

async function savePlan() {
  if (!S.planName) return;
  try {
    const r = await api("PUT", "/api/schedule/plan/" + encodeURIComponent(S.planName),
      { content: $("#plan-editor").value });
    $("#plan-msg").textContent = "已保存" + (r.backup ? "（备份 " + r.backup + "）" : "");
    $("#plan-msg").className = "msg ok";
    loadPlans();
  } catch (e) {
    $("#plan-msg").textContent = e.message;
    $("#plan-msg").className = "msg err";
  }
}

async function validatePlan() {
  if (!S.planName) return;
  // full validation runs on save (PUT); local syntax check here
  try {
    JSON.parse($("#plan-editor").value);
    $("#plan-msg").textContent = "JSON 语法通过（完整校验在保存时执行）";
    $("#plan-msg").className = "msg ok";
  } catch (e) {
    $("#plan-msg").textContent = "JSON 语法错误: " + e.message;
    $("#plan-msg").className = "msg err";
  }
}

async function startScheduler() {
  if (!S.planName || S.planName === "plan.example.json") { alert("先选择一个计划"); return; }
  try {
    await api("POST", "/api/schedule/start", { plan: S.planName });
    pollScheduler();
  } catch (e) { alert(e.message); }
}

async function stopScheduler() {
  try { await api("POST", "/api/schedule/stop"); }
  catch (e) { alert(e.message); }
}

async function pollScheduler() {
  try {
    const r = await api("GET", "/api/schedule/status");
    const sch = r.scheduler;
    const el = $("#sch-state");
    if (!sch) { el.textContent = "off"; $("#sch-events").textContent = ""; return; }
    el.textContent = sch.state + " · plan " + sch.plan +
      (sch.next_trigger ? " · 下一任务 @ " + sch.next_trigger : "") +
      (sch.current_task ? " · 执行中 task " + sch.current_task : "");
    const ev = $("#sch-events");
    ev.innerHTML = "";
    for (const line of sch.events) {
      const d = document.createElement("div");
      if (/DONE.*rc=0|finished/.test(line)) d.className = "ln-ok";
      if (/waits|stopped|abort/.test(line)) d.className = "ln-warn";
      d.textContent = line;
      ev.appendChild(d);
    }
  } catch (e) { /* ignore */ }
}

/* --------------------------------------------------------------- api doc */
async function loadApiDoc() {
  try {
    const r = await api("GET", "/api/help");
    $("#api-doc").textContent = r.markdown;
  } catch (e) { $("#api-doc").textContent = "failed to load API.md"; }
}

/* -------------------------------------------------------- global badges */
async function pollStatus() {
  try {
    const r = await api("GET", "/api/status");
    const rb = $("#badge-run");
    if (r.active_run) {
      rb.textContent = "run: " + r.active_run.id + " (" + Math.round(r.active_run.elapsed) + "s)";
      rb.className = "badge run";
    } else { rb.textContent = "run: idle"; rb.className = "badge idle"; }
    const sb = $("#badge-sch");
    if (r.scheduler && ["waiting", "waiting_slot", "executing"].includes(r.scheduler.state)) {
      sb.textContent = "scheduler: " + r.scheduler.state +
        (r.scheduler.next_trigger ? " @" + r.scheduler.next_trigger.slice(-8) : "");
      sb.className = "badge " + (r.scheduler.state === "executing" ? "exec" : "run");
    } else if (r.scheduler) {
      sb.textContent = "scheduler: " + r.scheduler.state;
      sb.className = "badge idle";
    } else { sb.textContent = "scheduler: off"; sb.className = "badge idle"; }
  } catch (e) { /* server down? */ }
}

/* ------------------------------------------------------------------ init */
function init() {
  $$("#tabs button").forEach(b => b.onclick = () => switchTab(b.dataset.tab));
  $$('input[name=selmode]').forEach(r => r.onchange = () => { S.selMode = r.value; });
  $("#btn-start").onclick = startRun;
  $("#btn-stop").onclick = stopRun;
  $("#btn-goto-results").onclick = () => {
    if (S.viewTsDir) { S.outTs = S.viewTsDir.split("/").pop(); switchTab("results"); }
  };
  $("#btn-validate").onclick = validateConfig;
  $("#btn-save").onclick = saveConfig;
  $("#btn-plan-validate").onclick = validatePlan;
  $("#btn-plan-save").onclick = savePlan;
  $("#btn-new-plan").onclick = newPlan;
  $("#btn-sch-start").onclick = startScheduler;
  $("#btn-sch-stop").onclick = stopScheduler;
  loadConfigs();
  refreshRuns();
  pollStatus();
  setInterval(pollStatus, 3000);
  setInterval(() => { if (S.viewRunId) pollViewRun(); }, 1500);
  setInterval(refreshRuns, 6000);
  setInterval(pollScheduler, 2500);
}

init();
