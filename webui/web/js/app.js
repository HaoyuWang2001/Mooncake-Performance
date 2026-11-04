"use strict";
/* Mooncake-Performance WebUI — 根应用（Vue3 global build + Element Plus，无构建步骤）
   各视图组件在 views/*.js 中定义为全局常量（ConfigView / RunView / ResultsView /
   ScheduleView / ApiView），由本文件注册并按侧边栏菜单切换（KeepAlive 保状态）。 */

const { createApp, reactive, computed, onMounted, onUnmounted, onActivated, onDeactivated,
        watch, nextTick } = Vue;

/* ---------------- 公共 API helper（全部视图共用） ---------------- */
async function api(method, url, body) {
  const opt = { method, headers: { "Content-Type": "application/json" } };
  if (body !== undefined) opt.body = JSON.stringify(body);
  const resp = await fetch(url, opt);
  let data = {};
  try { data = await resp.json(); } catch (e) { /* empty */ }
  if (!resp.ok) throw new Error(data.error || (resp.status + " " + resp.statusText));
  return data;
}

/* ---------------- 全局共享状态 ---------------- */
/* ---------------- 视图页签持久化（浏览器刷新后停留在当前页签） ---------------- */
const VIEW_KEYS = ["perf", "results", "config", "schedule"];
function loadInitialView() {
  try {
    const v = localStorage.getItem("mcweb-view");
    if (VIEW_KEYS.includes(v)) return v;
  } catch (e) { /* ignore */ }
  return "config";
}

const store = reactive({
  view: loadInitialView(),       // 当前视图：config | run | results | schedule | api
  status: { active_run: null, scheduler: null },   // /api/status 轮询结果
  resultsTarget: null,          // 跳转 results 时聚焦的 outputs/<ts>
  cfgTree: null,                // /api/configs 缓存（config 视图刷新）
});

watch(() => store.view, v => {
  try { localStorage.setItem("mcweb-view", v); } catch (e) { /* ignore */ }
});

/* ---------------- 主题（亮/暗，首次跟随系统，localStorage 持久化） ---------------- */
const THEME_KEY = "mcweb-theme";
function initialDark() {
  const saved = localStorage.getItem(THEME_KEY);
  if (saved === "dark") return true;
  if (saved === "light") return false;
  return window.matchMedia("(prefers-color-scheme: dark)").matches;
}
const theme = reactive({ dark: initialDark() });
function applyTheme() {
  document.documentElement.classList.toggle("dark", theme.dark);
}
applyTheme();
function toggleDark() {
  theme.dark = !theme.dark;
  localStorage.setItem(THEME_KEY, theme.dark ? "dark" : "light");
  applyTheme();
}


/* ---------------- 根组件 ---------------- */
const App = {
  template: `
  <el-container class="layout">
    <el-aside width="210px" class="aside">
      <div class="brand">Mooncake<span>-Performance</span></div>
      <div class="badges">
        <div class="badge-line" title="benchmark 执行槽">
          <span class="badge-dot" :class="runDot"></span>
          <span>{{ runText }}</span>
        </div>
        <div class="badge-line" title="scheduler 状态">
          <span class="badge-dot" :class="schDot"></span>
          <span>{{ schText }}</span>
        </div>
      </div>
      <el-menu :default-active="store.view" class="menu" @select="go">
        <el-menu-item index="perf">性能测试</el-menu-item>
        <el-menu-item index="results">统计结果</el-menu-item>
        <el-menu-item index="config">配置维护</el-menu-item>
        <el-menu-item index="schedule">定时任务</el-menu-item>
      </el-menu>
      <div class="aside-footer">
        <el-button size="small" circle @click="toggleDark" :title="theme.dark ? '切换到亮色' : '切换到暗色'">
          <el-icon><component :is="theme.dark ? 'Sunny' : 'Moon'"/></el-icon>
        </el-button>
        <el-link type="primary" :underline="false" href="/docs" target="_blank">API 文档</el-link>
      </div>
    </el-aside>
    <el-main class="main">
      <keep-alive>
        <component :is="viewComp"/>
      </keep-alive>
    </el-main>
  </el-container>`,
  setup() {
    const VIEWS = { perf: "perf-view", results: "results-view",
                    config: "config-view", schedule: "schedule-view" };
    const viewComp = computed(() => VIEWS[store.view] || "config-view");
    function go(v) { store.view = v; }

    const runDot = computed(() => store.status.active_run ? "on" : "");
    const runText = computed(() => {
      const r = store.status.active_run;
      return r ? "run: " + r.id + " (" + Math.round(r.elapsed) + "s)" : "run: 空闲";
    });
    const schDot = computed(() => {
      const s = store.status.scheduler;
      if (!s) return "";
      if (s.state === "executing") return "exec";
      if (["waiting", "waiting_slot"].includes(s.state)) return "on";
      return "";
    });
    const schText = computed(() => {
      const s = store.status.scheduler;
      if (!s) return "scheduler: 关闭";
      let t = "scheduler: " + s.state;
      if (s.next_trigger && ["waiting", "waiting_slot"].includes(s.state))
        t += " @" + s.next_trigger.slice(-8);
      return t;
    });

    let timer = null;
    async function pollStatus() {
      try { Object.assign(store.status, await api("GET", "/api/status")); }
      catch (e) { /* server down */ }
    }
    onMounted(() => { pollStatus(); timer = setInterval(pollStatus, 3000); });
    onUnmounted(() => clearInterval(timer));

    return { store, theme, toggleDark, go, viewComp, runDot, runText, schDot, schText };
  },
};

/* ---------------- 注册与挂载 ---------------- */
const app = createApp(App);
app.use(ElementPlus, { locale: ElementPlusLocaleZhCn });
for (const [iconName, iconComp] of Object.entries(ElementPlusIconsVue || {})) {
  app.component(iconName, iconComp);
}
app.component("perf-view", PerfView);
app.component("sel-node", SelNode);
app.component("config-view", ConfigView);
app.component("cfg-node", CfgNode);
app.component("results-view", ResultsView);
app.component("res-pane", ResPane);
app.component("schedule-view", ScheduleView);
app.component("spdiag-tree", SpdiagTree);
app.component("sdt-node", SdtNode);
app.mount("#app");
