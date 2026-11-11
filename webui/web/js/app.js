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
const VIEW_KEYS = ["perf", "instances", "results", "config", "schedule"];
function loadInitialView() {
  try {
    const v = localStorage.getItem("mcweb-view");
    if (VIEW_KEYS.includes(v)) return v;
  } catch (e) { /* ignore */ }
  return "config";
}

function _loadInstanceTabs() {
  try {
    const raw = sessionStorage.getItem("mcweb-instance-tabs");
    const arr = raw ? JSON.parse(raw) : [];
    return Array.isArray(arr) ? arr.filter(x => typeof x === "string") : [];
  } catch (e) { return []; }
}

const store = reactive({
  view: loadInitialView(),       // 当前视图：perf | instances | results | config | schedule
  status: { active_run: null, active_runs: [], scheduler: null },  // /api/status 轮询结果
  resultsTarget: null,          // 跳转 results 时聚焦的 outputs/<ts>
  instancesOpen: _loadInstanceTabs(),   // 实例详情页已打开的实例（浏览器式页签）
  instancesTarget: null,        // 当前激活的实例详情
  cfgTree: null,                // /api/configs 缓存（config 视图刷新）
});

watch(() => store.instancesOpen, v => {
  try { sessionStorage.setItem("mcweb-instance-tabs", JSON.stringify(v)); }
  catch (e) { /* ignore */ }
}, { deep: true });

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

/* ---------------- 高度持久化（拖拽手柄用） ---------------- */
function loadHeight(key, def) {
  try {
    const v = parseInt(localStorage.getItem(key), 10);
    return Number.isFinite(v) && v > 0 ? v : def;
  } catch (e) { return def; }
}
function saveHeight(key, h) {
  try { localStorage.setItem(key, String(Math.round(h))); } catch (e) { /* ignore */ }
}

/* ---------------- 通用垂直拖拽手柄（v-model：modelValue / update:modelValue） ----------------
   拖动时若指针接近滚动容器上下边缘，容器自动滚动并把实际滚动量补偿进高度，
   使手柄始终跟随光标（可一直往下拖大）。 */
const DragHandle = {
  name: "DragHandle",
  props: { modelValue: { type: Number, required: true },
           min: { type: Number, default: 200 },
           max: { type: Number, default: 1600 },
           persist: { type: String, default: "" } },
  emits: ["update:modelValue"],
  template: `<div class="vdrag" title="拖动调节高度" @mousedown="down"><span></span></div>`,
  setup(props, { emit }) {
    const clamp = h => Math.min(props.max, Math.max(props.min, h));
    const down = (e) => {
      e.preventDefault();
      const scroller = document.querySelector(".el-main");
      const startDocY = e.clientY + (scroller ? scroller.scrollTop : 0);
      const prev = e.currentTarget && e.currentTarget.previousElementSibling;
      const startH = props.modelValue > 0
        ? props.modelValue
        : (prev ? prev.offsetHeight : 420);
      let lastY = e.clientY;
      let raf = 0;
      const EDGE = 44, STEP = 8;
      const apply = () => {
        const docY = lastY + (scroller ? scroller.scrollTop : 0);
        const h = clamp(startH + (docY - startDocY));
        emit("update:modelValue", h);
        if (props.persist) saveHeight(props.persist, h);
      };
      const tick = () => {
        if (scroller) {
          const r = scroller.getBoundingClientRect();
          let d = 0;
          if (lastY > r.bottom - EDGE) d = Math.min(STEP, lastY - (r.bottom - EDGE));
          else if (lastY < r.top + EDGE) d = -Math.min(STEP, (r.top + EDGE) - lastY);
          if (d) { scroller.scrollTop += d; apply(); }
        }
        raf = requestAnimationFrame(tick);
      };
      const move = (ev) => { lastY = ev.clientY; apply(); };
      const up = () => {
        cancelAnimationFrame(raf);
        window.removeEventListener("mousemove", move);
        window.removeEventListener("mouseup", up);
        document.body.style.cursor = "";
        document.body.style.userSelect = "";
      };
      window.addEventListener("mousemove", move);
      window.addEventListener("mouseup", up);
      document.body.style.cursor = "row-resize";
      document.body.style.userSelect = "none";
      raf = requestAnimationFrame(tick);
    };
    return { down };
  },
};


/* ---------------- 根组件 ---------------- */
const App = {
  template: `
  <el-container class="layout">
    <el-aside width="210px" class="aside">
      <div class="brand">
        <span class="brand-mark" aria-hidden="true"></span>
        <span>Mooncake<span>-Performance</span></span>
      </div>
      <div class="badges">
        <div class="badge-line" :title="runTitle">
          <span class="badge-dot" :class="runDot"></span>
          <span>{{ runText }}</span>
        </div>
        <div class="badge-line" title="定时任务状态">
          <span class="badge-dot" :class="schDot"></span>
          <span>{{ schText }}</span>
        </div>
      </div>
      <el-menu :default-active="store.view" class="menu" @select="go">
        <el-menu-item index="perf">
          <el-icon><component is="Monitor"/></el-icon><span>性能测试</span>
        </el-menu-item>
        <el-menu-item index="instances">
          <el-icon><component is="List"/></el-icon><span>实例详情</span>
        </el-menu-item>
        <el-menu-item index="results">
          <el-icon><component is="DataAnalysis"/></el-icon><span>统计结果</span>
        </el-menu-item>
        <el-menu-item index="config">
          <el-icon><component is="Setting"/></el-icon><span>配置维护</span>
        </el-menu-item>
        <el-menu-item index="schedule">
          <el-icon><component is="Clock"/></el-icon><span>定时任务</span>
        </el-menu-item>
      </el-menu>
      <div class="aside-footer">
        <el-tooltip :content="theme.dark ? '切换到亮色' : '切换到暗色'" placement="top">
          <el-button size="small" circle aria-label="切换主题" @click="toggleDark">
            <el-icon><component :is="theme.dark ? 'Sunny' : 'Moon'"/></el-icon>
          </el-button>
        </el-tooltip>
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
    const VIEWS = { perf: "perf-view", instances: "instance-view",
                    results: "results-view",
                    config: "config-view", schedule: "schedule-view" };
    const viewComp = computed(() => VIEWS[store.view] || "config-view");
    function go(v) {
      store.view = v;
      nextTick(() => {
        const m = document.querySelector(".el-main");
        if (m) m.scrollTop = 0;
      });
    }

    const _activeRuns = computed(() => store.status.active_runs
      || (store.status.active_run ? [store.status.active_run] : []));
    const runDot = computed(() => {
      const runs = _activeRuns.value;
      if (!runs.length) return "";
      return runs.some(r => r.stopping) ? "exec" : "on";
    });
    const runText = computed(() => {
      const runs = _activeRuns.value;
      if (!runs.length) return "运行实例：空闲";
      if (runs.length === 1) {
        return "运行实例：" + (runs[0].stopping ? "停止中" : "运行中")
          + "（" + Math.round(runs[0].elapsed) + "s）";
      }
      return "运行实例：" + runs.length + " 个";
    });
    const runTitle = computed(() => {
      const runs = _activeRuns.value;
      if (!runs.length) return "当前没有运行中的实例";
      return runs.map(r => r.id + "：" + r.status
        + (r.stopping ? "（停止中）" : "")).join("\n");
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
      if (!s) return "定时任务：关闭";
      let t = "定时任务：" + schedulerText(s.state);
      if (s.next_trigger && ["waiting", "waiting_slot"].includes(s.state))
        t += "（" + s.next_trigger.slice(-8) + "）";
      return t;
    });

    let timer = null;
    async function pollStatus() {
      try { Object.assign(store.status, await api("GET", "/api/status")); }
      catch (e) { /* server down */ }
    }
    onMounted(() => { pollStatus(); timer = setInterval(pollStatus, 3000); });
    onUnmounted(() => clearInterval(timer));

    return { store, theme, toggleDark, go, viewComp,
             runDot, runText, runTitle, schDot, schText };
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
app.component("instance-view", InstanceView);
app.component("instance-log-pane", InstanceLogPane);
app.component("config-view", ConfigView);
app.component("cfg-node", CfgNode);
app.component("results-view", ResultsView);
app.component("res-pane", ResPane);
app.component("schedule-view", ScheduleView);
app.component("spdiag-tree", SpdiagTree);
app.component("sdt-node", SdtNode);
app.component("drag-handle", DragHandle);
app.mount("#app");
