"use strict";
/* Mooncake-Performance WebUI — 根应用（Vue3 global build + Element Plus，无构建步骤）
   各视图组件在 views/*.js 中定义为全局常量（PerfView / InstanceView /
   NodesView / ResultsView / ConfigView / ScheduleView），由本文件注册并按
   侧边栏菜单切换（KeepAlive 保状态）。
   全局轮询 /api/status、/api/instances（3s）与 /api/runs（8s）：任意页签下
   agent 经 API 创建实例 / 改变实例状态都能立刻在共享 store 与侧边栏徽标看到。 */

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

/* ---------------- 全局共享数据刷新（API 操作的统一观察窗口） ---------------- */
/* 任何视图（无论当前激活与否）都读 store.instances / store.runs；
   视图在变更操作后调用这两个函数做即时刷新。 */
async function refreshInstances() {
  store.instances = (await api("GET", "/api/instances")).instances || [];
  return store.instances;
}
async function refreshRuns() {
  store.runs = (await api("GET", "/api/runs")).runs || [];
  return store.runs;
}

/* ---------------- 全局共享状态 ---------------- */
/* ---------------- 视图页签持久化（浏览器刷新后停留在当前页签） ---------------- */
const VIEW_KEYS = ["perf", "instances", "nodes", "results", "config", "schedule"];
function loadInitialView() {
  try {
    const v = localStorage.getItem("mcweb-view");
    if (VIEW_KEYS.includes(v)) return v;
  } catch (e) { /* ignore */ }
  return "perf";
}

function _loadInstanceTabs() {
  try {
    const raw = sessionStorage.getItem("mcweb-instance-tabs");
    const arr = raw ? JSON.parse(raw) : [];
    return Array.isArray(arr) ? arr.filter(x => typeof x === "string") : [];
  } catch (e) { return []; }
}

const store = reactive({
  view: loadInitialView(),       // 当前视图：perf | instances | nodes | results | config | schedule
  status: { active_run: null, active_runs: [], scheduler: null },  // /api/status 轮询结果
  instances: [],                // /api/instances 全局轮询结果（统一 API 操作显示）
  runs: [],                     // /api/runs 全局轮询结果（历史运行）
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
        <span class="brand-text">Mooncake<span>Performance</span></span>
      </div>
      <el-menu :default-active="store.view" class="menu" @select="go">
        <el-menu-item index="perf">
          <el-icon><component is="Monitor"/></el-icon><span>性能测试</span>
          <span v-if="instTotal" class="menu-badge" :class="{ on: instRunning > 0 }"
                :title="'实例 ' + instTotal + ' 个，其中运行中 ' + instRunning + ' 个'">
            {{ instTotal }}<template v-if="instRunning"> · 运行 {{ instRunning }}</template>
          </span>
        </el-menu-item>
        <el-menu-item index="instances">
          <el-icon><component is="List"/></el-icon><span>实例详情</span>
        </el-menu-item>
        <el-menu-item index="nodes">
          <el-icon><component is="Odometer"/></el-icon><span>集群节点</span>
        </el-menu-item>
        <el-menu-item index="results">
          <el-icon><component is="DataAnalysis"/></el-icon><span>统计结果</span>
        </el-menu-item>
        <el-menu-item index="config">
          <el-icon><component is="Setting"/></el-icon><span>配置维护</span>
        </el-menu-item>
        <el-menu-item index="schedule">
          <el-icon><component is="Clock"/></el-icon><span>定时任务</span>
          <span v-if="schedState" class="menu-badge" :class="schedBadgeClass"
                :title="'调度状态：' + schedulerText(schedState)">{{ schedulerText(schedState) }}</span>
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
                    nodes: "nodes-view", results: "results-view",
                    config: "config-view", schedule: "schedule-view" };
    const viewComp = computed(() => VIEWS[store.view] || "config-view");
    function go(v) {
      store.view = v;
      nextTick(() => {
        const m = document.querySelector(".el-main");
        if (m) m.scrollTop = 0;
      });
    }

    async function pollStatus() {
      try { Object.assign(store.status, await api("GET", "/api/status")); }
      catch (e) { /* server down */ }
    }

    /* 实例/运行状态全局轮询（与 status 同拍，runs 每 3 拍 ~9s 一次）：
       任意页签都能实时反映 agent 经 API 创建实例 / 启动 / 停止 / 结束。 */
    let timer = null;
    let runsTick = 0;
    function tick() {
      pollStatus();
      refreshInstances().catch(() => { /* server down */ });
      if (runsTick++ % 3 === 0) refreshRuns().catch(() => { /* ignore */ });
    }

    /* 侧边栏徽标（全局可见的 API 操作观察点） */
    const instTotal = computed(() => (store.instances || []).length);
    const instRunning = computed(() =>
      (store.instances || []).filter(it => it.status === "running").length);
    const ACTIVE_SCHED = ["waiting", "waiting_slot", "executing", "stopping"];
    const schedState = computed(() => {
      const sch = store.status.scheduler;
      return (sch && ACTIVE_SCHED.includes(sch.state)) ? sch.state : "";
    });
    const schedBadgeClass = computed(() =>
      (schedState.value === "executing" || schedState.value === "stopping") ? "warn" : "on");

    onMounted(() => { tick(); timer = setInterval(tick, 3000); });
    onUnmounted(() => clearInterval(timer));

    return { store, theme, toggleDark, go, viewComp,
             instTotal, instRunning, schedState, schedBadgeClass, schedulerText };
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
app.component("nodes-view", NodesView);
app.component("config-view", ConfigView);
app.component("cfg-node", CfgNode);
app.component("results-view", ResultsView);
app.component("res-pane", ResPane);
app.component("schedule-view", ScheduleView);
app.component("spdiag-tree", SpdiagTree);
app.component("sdt-node", SdtNode);
app.component("drag-handle", DragHandle);
app.mount("#app");
