"use strict";
/* 实例详情视图：浏览器式页签（可同时打开多个实例）+ 实例信息 + 操作 +
   日志查看。组合实例（Rank+Recall 并行）日志按 rank/recall 上下分屏展示。 */

function _instResultsTarget(tsDir) {
  const rel = String(tsDir || "").replace(/^outputs[\\/]/, "");
  return rel.split("/")[0];
}

/* 单个日志流面板（rank / recall / all 各一份），自带分页、搜索、换行。 */
const InstanceLogPane = {
  name: "InstanceLogPane",
  props: { rid: { type: String, default: "" },
           stream: { type: String, default: "all" },
           title: { type: String, default: "" },
           live: { type: Boolean, default: false },
           ticket: { type: Number, default: 0 } },
  template: `
  <div class="ilog-pane">
    <div class="ilog-head" v-if="title">{{ title }}</div>
    <div class="log-toolbar">
      <el-button size="small" @click="copyLog">复制</el-button>
      <span class="log-wrap-toggle">
        <el-switch v-model="logWrap" size="small" />
        <span class="log-wrap-label" @click="logWrap = !logWrap">自动换行</span>
      </span>
      <span class="hint ilog-progress" v-if="offset">已加载 {{ fmtBytes(offset) }} / {{ fmtBytes(size) }}</span>
      <span class="spacer"></span>
      <el-input v-model="searchQ" size="small" placeholder="搜索日志（高亮+跳转）"
                clearable style="width: 210px" @keyup.enter="nextMatch" />
      <el-button size="small" :disabled="!matchCount" @click="prevMatch">↑</el-button>
      <el-button size="small" :disabled="!matchCount" @click="nextMatch">↓</el-button>
      <span v-if="searchQ" class="log-match-count mc-num">{{ matchCount ? (matchIdx + 1) + "/" + matchCount : "0/0" }}</span>
    </div>
    <div class="logview ilog-view" ref="logEl" :class="{ 'log-nowrap': !logWrap }">
      <div v-for="(ln, i) in lines" :key="i"
           :class="[ln.cls, { 'ln-cur-match': i === curMatchLine }]">
        <template v-if="searchQ && hasHit(ln.text)">
          <span v-for="(seg, j) in lineSegs(ln.text)" :key="j"
                :class="{ 'ln-mark': seg.hit }">{{ seg.t }}</span>
        </template>
        <template v-else>{{ ln.text }}</template>
      </div>
      <div v-if="!lines.length" class="hint" style="padding: 24px; text-align: center">
        {{ rid ? (loading ? "加载中…" : "（本流暂无日志）") : "该实例尚未运行" }}
      </div>
    </div>
  </div>`,
  setup(props) {
    const lines = Vue.ref([]);
    const offset = Vue.ref(0);
    const size = Vue.ref(0);
    const rem = Vue.ref("");
    const eof = Vue.ref(true);
    const loading = Vue.ref(false);
    const logEl = Vue.ref(null);
    let wrap = true;
    try { wrap = localStorage.getItem("mcweb-wrap") !== "0"; } catch (e) { /* ignore */ }
    const logWrap = Vue.ref(wrap);
    const searchQ = Vue.ref("");
    const matchIdx = Vue.ref(0);
    let timer = null;

    function classify(ln) {
      if (/\bFAIL\b|\bERROR\b/.test(ln)) return "ln-err";
      if (/\bWARN\b/.test(ln)) return "ln-warn";
      if (/SUMMARY: all .* OK|queue finished/.test(ln)) return "ln-ok";
      return "";
    }
    function fmtBytes(n) {
      n = Number(n) || 0;
      if (n < 1024) return n + " B";
      if (n < 1024 * 1024) return (n / 1024).toFixed(1) + " KB";
      if (n < 1024 * 1024 * 1024) return (n / (1024 * 1024)).toFixed(1) + " MB";
      return (n / (1024 * 1024 * 1024)).toFixed(2) + " GB";
    }
    function append(text) {
      if (!text) return;
      const el = logEl.value;
      const wasBottom = !el || (el.scrollHeight - el.scrollTop - el.clientHeight < 60);
      const parts = (rem.value + text).split("\n");
      rem.value = parts.pop();
      for (const ln of parts) lines.value.push({ text: ln, cls: classify(ln) });
      if (lines.value.length > 20000)
        lines.value.splice(0, lines.value.length - 20000);
      if (wasBottom) Vue.nextTick(() => {
        if (logEl.value) logEl.value.scrollTop = logEl.value.scrollHeight;
      });
    }
    function reset() {
      lines.value = []; offset.value = 0; size.value = 0; rem.value = "";
      eof.value = true; matchIdx.value = 0;
    }
    async function pullOnce() {
      if (!props.rid) return false;
      const r = await api("GET", "/api/runs/" + encodeURIComponent(props.rid) +
        "/log?offset=" + offset.value + "&stream=" + encodeURIComponent(props.stream));
      if (r.content) append(r.content);
      offset.value = r.offset;
      size.value = r.size;
      eof.value = r.eof;
      return !r.eof;
    }
    async function poll() {
      if (loading.value || !props.rid) return;
      loading.value = true;
      try {
        // drain several pages per tick so a finished run's log fills fast
        for (let i = 0; i < 8; i++) {
          const more = await pullOnce();
          if (!more) break;
        }
      } catch (e) { /* run pruned / transient */ }
      finally { loading.value = false; }
    }

    const matchLines = Vue.computed(() => {
      const q = searchQ.value.trim().toLowerCase();
      if (!q) return [];
      const hits = [];
      lines.value.forEach((ln, i) => {
        if (ln.text.toLowerCase().includes(q)) hits.push(i);
      });
      return hits;
    });
    const matchCount = Vue.computed(() => matchLines.value.length);
    const curMatchLine = Vue.computed(() =>
      matchCount.value ? matchLines.value[matchIdx.value % matchCount.value] : -1);
    function hasHit(text) {
      const q = searchQ.value.trim().toLowerCase();
      return !!q && text.toLowerCase().includes(q);
    }
    function lineSegs(text) {
      const q = searchQ.value.trim();
      if (!q) return [{ t: text, hit: false }];
      const lower = text.toLowerCase(), lq = q.toLowerCase();
      const out = [];
      let pos = 0;
      for (;;) {
        const at = lower.indexOf(lq, pos);
        if (at < 0) {
          if (pos < text.length) out.push({ t: text.slice(pos), hit: false });
          break;
        }
        if (at > pos) out.push({ t: text.slice(pos, at), hit: false });
        out.push({ t: text.slice(at, at + q.length), hit: true });
        pos = at + q.length;
      }
      return out.length ? out : [{ t: text, hit: false }];
    }
    function scrollToCurMatch() {
      Vue.nextTick(() => {
        const el = logEl.value, idx = curMatchLine.value;
        if (!el || idx < 0 || !el.children[idx]) return;
        el.children[idx].scrollIntoView({ block: "center" });
      });
    }
    function nextMatch() {
      if (!matchCount.value) return;
      matchIdx.value = (matchIdx.value + 1) % matchCount.value;
      scrollToCurMatch();
    }
    function prevMatch() {
      if (!matchCount.value) return;
      matchIdx.value = (matchIdx.value - 1 + matchCount.value) % matchCount.value;
      scrollToCurMatch();
    }
    Vue.watch(searchQ, () => { matchIdx.value = 0; scrollToCurMatch(); });
    Vue.watch(() => props.rid, () => { reset(); poll(); });
    Vue.watch(() => props.ticket, () => { reset(); poll(); });
    Vue.watch(logWrap, v => {
      try { localStorage.setItem("mcweb-wrap", v ? "1" : "0"); } catch (e) { /* ignore */ }
    });
    async function copyLog() {
      const text = lines.value.map(l => l.text).join("\n");
      try {
        await navigator.clipboard.writeText(text);
        ElementPlus.ElMessage.success("日志已复制");
      } catch (e) {
        try {
          const ta = document.createElement("textarea");
          ta.value = text;
          document.body.appendChild(ta);
          ta.select();
          document.execCommand("copy");
          document.body.removeChild(ta);
          ElementPlus.ElMessage.success("日志已复制");
        } catch (e2) {
          ElementPlus.ElMessage.error("复制失败: " + e2.message);
        }
      }
    }
    Vue.onMounted(() => {
      poll();
      timer = setInterval(() => {
        if (props.live || !eof.value) poll();
      }, 1500);
    });
    Vue.onUnmounted(() => clearInterval(timer));

    return { lines, offset, size, loading, logEl, logWrap, searchQ, matchIdx,
             matchCount, curMatchLine, hasHit, lineSegs, nextMatch, prevMatch,
             copyLog, fmtBytes };
  },
};

const InstanceView = {
  name: "InstanceView",
  template: `
  <div class="page">
    <el-tabs v-if="tabs.length" :model-value="active" type="card" closable
             class="inst-tabs" @tab-click="(p) => activate(p.paneName)"
             @tab-remove="closeTab">
      <el-tab-pane v-for="iid in tabs" :key="iid" :name="iid" :label="nameOf(iid)" lazy>
        <template #label>
          <span class="it-label">
            <span class="badge-dot" :class="tabDot(iid)" aria-hidden="true"></span>
            <span class="it-name">{{ nameOf(iid) }}</span>
          </span>
        </template>
      </el-tab-pane>
    </el-tabs>

    <el-card v-if="!tabs.length" shadow="never">
      <el-empty description="还没有打开实例详情" :image-size="80">
        <el-button type="primary" size="small" @click="goPerf">去性能测试页选择实例</el-button>
      </el-empty>
    </el-card>

    <template v-else-if="detail">
      <el-card shadow="never" class="inst-detail-head">
        <div class="inst-head">
          <span class="inst-name" :title="detail.instance.name">{{ detail.instance.name }}</span>
          <el-tag size="small" :type="detail.instance.launch_mode === 'rank_recall' ? 'warning' : 'info'">
            {{ detail.instance.launch_mode === 'rank_recall' ? 'Rank+Recall' : '单任务' }}
          </el-tag>
          <el-popover v-if="detail.instance.status === 'running' && detail.instance.stopping"
                      placement="top" :width="300" trigger="hover" :show-after="150"
                      content="正在优雅停止：先通知客户端清理并回收远端进程，宽限期内完成；超时后才会强制结束。">
            <template #reference><el-tag size="small" type="warning">停止中</el-tag></template>
          </el-popover>
          <el-tag v-else size="small" :type="statusType(detail.instance.status)">
            {{ statusText(detail.instance.status) }}
          </el-tag>
          <span class="spacer"></span>
          <span class="inst-id mc-mono">{{ detail.instance.id }}</span>
        </div>
        <el-descriptions :column="2" size="small" border class="inst-desc">
          <el-descriptions-item label="模式">{{ detail.instance.mode === 'k8s' ? 'k8s' : '真机' }}</el-descriptions-item>
          <el-descriptions-item label="配置">
            目录 {{ detail.instance.selection.dirs.length }} / 文件 {{ detail.instance.selection.files.length }}
          </el-descriptions-item>
          <el-descriptions-item label="cluster" :span="2">
            <span class="mc-mono">{{ pairCluster }}</span>
          </el-descriptions-item>
          <el-descriptions-item label="global" :span="2">
            <span class="mc-mono">{{ pairGlobal }}</span>
          </el-descriptions-item>
          <el-descriptions-item label="profile" :span="2">
            <span class="mc-mono">{{ detail.instance.flags.profile || "-" }}</span>
          </el-descriptions-item>
          <template v-if="latestRun">
            <el-descriptions-item label="最近运行"><span class="mc-mono">{{ latestRun.id }}</span></el-descriptions-item>
            <el-descriptions-item label="状态">
              <el-tag size="small" :type="statusType(latestRun.status)">{{ statusText(latestRun.status) }}</el-tag>
            </el-descriptions-item>
            <el-descriptions-item label="耗时"><span class="mc-num">{{ Math.round(latestRun.elapsed || 0) }}s</span></el-descriptions-item>
            <el-descriptions-item label="返回码"><span class="mc-num">{{ (latestRun.rc === null || latestRun.rc === undefined) ? "-" : latestRun.rc }}</span></el-descriptions-item>
            <el-descriptions-item label="产物" :span="2">
              <span class="mc-mono">{{ latestRun.ts_dir || "-" }}</span>
            </el-descriptions-item>
          </template>
        </el-descriptions>
        <div class="inst-actions">
          <el-tooltip :disabled="!runHint" :content="runHint" placement="top">
            <span>
              <el-button size="small" type="primary" :loading="busyRun"
                         :disabled="detail.instance.status === 'running' || schedulerBusy"
                         @click="run">运行</el-button>
            </span>
          </el-tooltip>
          <el-button size="small" type="warning" plain v-if="detail.instance.status === 'running'"
                     :loading="busyStop" :disabled="busyStop" @click="stop">停止</el-button>
          <el-button size="small" type="danger" plain
                     :disabled="detail.instance.status === 'running'" @click="remove">删除</el-button>
          <el-button size="small" @click="rename">改名</el-button>
          <span class="spacer"></span>
          <el-checkbox v-if="isCombined" v-model="merged" style="margin-right: 12px">
            合并日志
          </el-checkbox>
          <el-button size="small" plain
                     :disabled="!latestRun || !latestRun.ts_dir"
                     @click="gotoResults">跳转至结果详情</el-button>
          <el-button size="small" plain :disabled="!latestRun"
                     @click="openReport">查看报表</el-button>
        </div>
      </el-card>

      <el-card shadow="never" style="margin-top: 12px" v-if="latestRun">
        <div v-if="isCombined && !merged" class="ilog-split">
          <instance-log-pane :rid="latestRun.id" stream="rank" title="rank"
                             :live="detail.instance.status === 'running'"
                             :ticket="ticket"></instance-log-pane>
          <instance-log-pane :rid="latestRun.id" stream="recall" title="recall"
                             :live="detail.instance.status === 'running'"
                             :ticket="ticket"></instance-log-pane>
        </div>
        <instance-log-pane v-else :rid="latestRun.id" stream="all"
                           :live="detail.instance.status === 'running'"
                           :ticket="ticket"></instance-log-pane>
      </el-card>
      <el-card v-else shadow="never" style="margin-top: 12px">
        <el-empty description="该实例尚未运行" :image-size="72">
          <el-button type="primary" size="small" :disabled="schedulerBusy" @click="run">运行</el-button>
        </el-empty>
      </el-card>
    </template>
  </div>`,
  setup() {
    const loading = Vue.ref(false);
    const detail = Vue.ref(null);
    const busyRun = Vue.ref(false);
    const busyStop = Vue.ref(false);
    const merged = Vue.ref(false);
    const ticket = Vue.ref(0);
    const instIndex = Vue.ref({});
    let timer = null;

    const tabs = Vue.computed(() => store.instancesOpen || []);
    const active = Vue.computed(() => store.instancesTarget || tabs.value[0] || "");

    function persist() {
      try {
        sessionStorage.setItem("mcweb-instance-tabs",
                               JSON.stringify(store.instancesOpen || []));
      } catch (e) { /* ignore */ }
    }
    function nameOf(iid) {
      if (detail.value && detail.value.instance.id === iid)
        return detail.value.instance.name;
      const it = instIndex.value[iid];
      return (it && it.name) || iid;
    }
    function tabDot(iid) {
      const it = instIndex.value[iid];
      const st = it && it.status;
      if (st === "running") return "on";
      if (st === "stopping") return "exec";
      if (st === "done") return "ok";
      if (st === "failed" || st === "failed_spawn") return "err";
      return "";
    }
    async function loadNames() {
      try {
        const r = await api("GET", "/api/instances");
        const idx = {};
        for (const it of r.instances || []) idx[it.id] = it;
        instIndex.value = idx;
      } catch (e) { /* ignore */ }
    }
    function goPerf() { store.view = "perf"; }

    const runHint = Vue.computed(() => {
      if (!detail.value) return "";
      if (detail.value.instance.status === "running") return "该实例正在运行，先停止再运行";
      if (schedulerBusy.value) return "定时任务执行中，暂不能启动实例";
      return "";
    });
    const pairCluster = Vue.computed(() => {
      const f = (detail.value && detail.value.instance.flags) || {};
      return detail.value && detail.value.instance.launch_mode === "rank_recall"
        ? "rank " + (f.rank_cluster || "-") + " ｜ recall " + (f.recall_cluster || "-")
        : (f.cluster || "-");
    });
    const pairGlobal = Vue.computed(() => {
      const f = (detail.value && detail.value.instance.flags) || {};
      return detail.value && detail.value.instance.launch_mode === "rank_recall"
        ? "rank " + (f.rank_global || "-") + " ｜ recall " + (f.recall_global || "-")
        : (f.global || "-");
    });
    async function load() {
      const iid = active.value;
      if (!iid) { detail.value = null; return; }
      loading.value = true;
      try {
        const r = await api("GET", "/api/instances/" + encodeURIComponent(iid));
        if (active.value !== iid) return;
        const prevRun = latestRun.value && latestRun.value.id;
        detail.value = r;
        const nowRun = r.runs && r.runs.length ? r.runs[0] : null;
        if (nowRun && nowRun.id !== prevRun) ticket.value++;
      } catch (e) {
        ElementPlus.ElMessage.error(e.message);
      } finally { loading.value = false; }
    }
    function activate(iid) {
      store.instancesTarget = iid;
      detail.value = null;
      load();
    }
    function closeTab(iid) {
      const open = (store.instancesOpen || []).filter(x => x !== iid);
      store.instancesOpen = open;
      persist();
      if (store.instancesTarget === iid) {
        store.instancesTarget = open.length ? open[open.length - 1] : null;
        detail.value = null;
        if (store.instancesTarget) load();
      }
    }
    async function run() {
      const iid = active.value; if (!iid) return;
      busyRun.value = true;
      try {
        const r = await api("POST", "/api/instances/" + iid + "/run");
        if (r.warnings && r.warnings.length)
          ElementPlus.ElMessage.warning(r.warnings.join("；"));
        ElementPlus.ElMessage.success("实例已启动");
        await load();
      } catch (e) { ElementPlus.ElMessage.error("启动失败: " + e.message); }
      finally { busyRun.value = false; }
    }
    async function stop() {
      const iid = active.value; if (!iid) return;
      busyStop.value = true;
      try {
        await api("POST", "/api/instances/" + iid + "/stop");
        ElementPlus.ElMessage.success("已请求停止");
        await load();
      } catch (e) { ElementPlus.ElMessage.error("停止失败: " + e.message); }
      finally { busyStop.value = false; }
    }
    async function remove() {
      const iid = active.value; if (!iid) return;
      try {
        await confirmDanger("删除实例？运行记录、日志与 outputs 保留，仅实例记录移入回收站。",
                            "删除实例");
      } catch (e) { return; }
      try {
        await api("DELETE", "/api/instances/" + iid);
        ElementPlus.ElMessage.success("实例已删除");
        closeTab(iid);
      } catch (e) { ElementPlus.ElMessage.error("删除失败: " + e.message); }
    }
    async function rename() {
      const iid = active.value; if (!iid) return;
      let name;
      try {
        const r = await ElementPlus.ElMessageBox.prompt(
          "实例名称", "改名",
          { inputValue: detail.value.instance.name, confirmButtonText: "保存",
            cancelButtonText: "取消" });
        name = (r.value || "").trim();
      } catch (e) { return; }
      try {
        await api("POST", "/api/instances/" + iid + "/rename", { name });
        await load();
      } catch (e) { ElementPlus.ElMessage.error("改名失败: " + e.message); }
    }
    function gotoResults() {
      const r = latestRun.value;
      if (!r || !r.ts_dir) return;
      store.resultsTarget = _instResultsTarget(r.ts_dir);
      store.view = "results";
    }
    /* 报表与 WebUI 解耦：定位该 run 的 report.html 并在新标签页打开 */
    async function openReport() {
      const r = latestRun.value;
      if (!r || !r.ts_dir) return;
      const rel = String(r.ts_dir).replace(/^outputs[\\/]/, "");
      const parts = rel.split("/");
      const root = parts[0];
      const sub = parts.slice(1).join("/");
      let files = [];
      try {
        files = (await api("GET", "/api/outputs/" +
          encodeURIComponent(root) + "/tree")).files || [];
      } catch (e) { files = []; }
      const pref = sub ? sub + "/" : "";
      const pick =
        files.find(f => f === pref + "report/overview.html") ||
        files.find(f => f.startsWith(pref) && /(^|\/)report\/report\.html$/.test(f)) ||
        files.find(f => /(^|\/)report\/overview\.html$/.test(f)) ||
        files.find(f => /(^|\/)report\/report\.html$/.test(f));
      if (!pick) {
        ElementPlus.ElMessage.warning("该 run 还没有报表（未生成或已清理）");
        return;
      }
      window.open("/api/file?path=" + encodeURIComponent(
        "outputs/" + root + "/" + pick) + "&mode=raw", "_blank");
    }

    const latestRun = Vue.computed(() => {
      const d = detail.value;
      if (!d) return null;
      return (d.runs && d.runs.length) ? d.runs[0] : null;
    });
    const isCombined = Vue.computed(() =>
      !!(detail.value && detail.value.instance.launch_mode === "rank_recall"));
    const schedulerBusy = Vue.computed(() => {
      const sch = store.status.scheduler;
      return !!(sch && ["executing", "stopping"].includes(sch.state));
    });

    Vue.watch(active, () => { merged.value = false; });
    Vue.onActivated(() => {
      loadNames();
      load();
      timer = setInterval(load, 2000);
    });
    Vue.onDeactivated(() => { clearInterval(timer); });

    return { tabs, active, detail, busyRun, busyStop, merged, ticket,
             latestRun, isCombined, schedulerBusy, runHint, pairCluster,
             pairGlobal, nameOf, tabDot, goPerf, activate, closeTab,
             statusType, statusText, run, stop, remove, rename,
             gotoResults, openReport };
  },
};
