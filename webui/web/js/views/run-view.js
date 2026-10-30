"use strict";
/* 运行视图：当前 run 状态 + 实时日志（FAIL/ERROR 高亮）+ 停止 + 历史 + 查看结果跳转。
   仅观察——运行发起在「配置维护」页。 */

const RunView = {
  template: `
  <div class="page">
    <el-card shadow="never" style="margin-bottom: 14px">
      <template #header><b>当前 Run</b></template>
      <el-descriptions :column="4" size="small" border>
        <el-descriptions-item label="id">{{ meta.id || "-" }}</el-descriptions-item>
        <el-descriptions-item label="状态">
          <el-tag v-if="meta.status" :type="statusType(meta.status)" size="small">{{ meta.status }}</el-tag>
          <span v-else>-</span>
        </el-descriptions-item>
        <el-descriptions-item label="耗时">{{ meta.id ? Math.round(meta.elapsed) + "s" : "-" }}</el-descriptions-item>
        <el-descriptions-item label="rc">{{ meta.rc === null || meta.rc === undefined ? "-" : meta.rc }}</el-descriptions-item>
        <el-descriptions-item label="命令" :span="3">
          <span style="font-family: monospace; font-size: 12px">{{ meta.cmd || "-" }}</span>
        </el-descriptions-item>
        <el-descriptions-item label="产物">
          <span v-if="meta.ts_dir" style="font-family: monospace; font-size: 12px">{{ meta.ts_dir }}</span>
          <span v-else>-</span>
        </el-descriptions-item>
      </el-descriptions>
      <div class="header-row" style="margin: 12px 0 0">
        <el-button v-if="meta.status === 'running'" type="danger" @click="stop" :loading="stopping">停止</el-button>
        <el-button v-if="meta.ts_dir && meta.status && meta.status !== 'running'" type="primary"
                   @click="gotoResults">查看运行结果</el-button>
        <span class="hint">查看结果为手动跳转；停止将终止编排器并自动清理远端残留进程（reader 优雅关停、客户端校验后 kill、清台账）</span>
      </div>
    </el-card>

    <el-card shadow="never" style="margin-bottom: 14px">
      <template #header><b>日志</b></template>
      <div class="logview log-lg" ref="logEl" :style="{ height: logH + 'px' }">
        <div v-for="(ln, i) in logLines" :key="i" :class="ln.cls">{{ ln.text }}</div>
      </div>
      <drag-handle v-model="logH" :min="200" :max="1600" persist="mcweb-log-h"></drag-handle>
    </el-card>

    <el-card shadow="never">
      <template #header><b>历史 Runs</b></template>
      <el-table :data="runs" size="small" highlight-current-row @row-click="onHistoryClick">
        <el-table-column prop="id" label="id" width="180"></el-table-column>
        <el-table-column label="状态" width="90">
          <template #default="scope">
            <el-tag :type="statusType(scope.row.status)" size="small">{{ scope.row.status }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="rc" label="rc" width="60"></el-table-column>
        <el-table-column label="耗时" width="80">
          <template #default="scope">{{ Math.round(scope.row.elapsed) + "s" }}</template>
        </el-table-column>
        <el-table-column prop="cmd" label="命令" show-overflow-tooltip></el-table-column>
      </el-table>
    </el-card>
  </div>`,
  setup() {
    const meta = Vue.ref({});
    const logLines = Vue.ref([]);
    const logOffset = Vue.ref(0);
    const logRem = Vue.ref("");
    const logEof = Vue.ref(true);
    const logEl = Vue.ref(null);
    const runs = Vue.ref([]);
    const stopping = Vue.ref(false);
    const logH = Vue.ref(loadHeight("mcweb-log-h", 460));

    function statusType(s) {
      return { running: "primary", done: "success", failed: "danger",
               stopped: "warning", aborted: "warning" }[s] || "info";
    }

    function classify(ln) {
      if (/\bFAIL\b|\bERROR\b/.test(ln)) return "ln-err";
      if (/\bWARN\b/.test(ln)) return "ln-warn";
      if (/SUMMARY: all .* OK|queue finished/.test(ln)) return "ln-ok";
      return "";
    }

    function appendLog(text) {
      if (!text) return;
      const el = logEl.value;
      const wasBottom = !el || (el.scrollHeight - el.scrollTop - el.clientHeight < 60);
      const lines = (logRem.value + text).split("\n");
      logRem.value = lines.pop();
      for (const ln of lines) logLines.value.push({ text: ln, cls: classify(ln) });
      if (logLines.value.length > 5000) logLines.value.splice(0, logLines.value.length - 5000);
      if (wasBottom) Vue.nextTick(() => { if (logEl.value) logEl.value.scrollTop = logEl.value.scrollHeight; });
    }

    function viewRun(id, live) {
      meta.value = { id };
      logLines.value = [];
      logOffset.value = 0;
      logRem.value = "";
      logEof.value = false;
      pollRun();
    }

    async function pollRun() {
      const id = meta.value.id;
      if (!id) return;
      try {
        const m = await api("GET", "/api/runs/" + id);
        if (meta.value.id !== id) return;
        meta.value = m;
        if (m.status !== "running" && logEof.value) return;
        const lg = await api("GET", "/api/runs/" + id + "/log?offset=" + logOffset.value);
        if (meta.value.id !== id) return;
        if (lg.content) appendLog(lg.content);
        logOffset.value = lg.offset;
        logEof.value = lg.eof;
        if (logEof.value && logRem.value && m.status !== "running") {
          logLines.value.push({ text: logRem.value, cls: classify(logRem.value) });
          logRem.value = "";
        }
      } catch (e) { /* run pruned or server hiccup */ }
    }

    async function refreshRuns() {
      try { runs.value = (await api("GET", "/api/runs")).runs; }
      catch (e) { /* ignore */ }
    }

    function onHistoryClick(row) { viewRun(row.id, row.status === "running"); }

    async function stop() {
      if (!meta.value.id) return;
      stopping.value = true;
      try { await api("POST", "/api/runs/" + meta.value.id + "/stop"); }
      catch (e) { ElementPlus.ElMessage.error(e.message); }
      finally { stopping.value = false; }
    }

    function gotoResults() {
      if (meta.value.ts_dir) {
        store.resultsTarget = meta.value.ts_dir.split("/").pop();
        store.view = "results";
      }
    }

    let timer = null, histTimer = null;
    Vue.onActivated(() => {
      if (store.pendingRunId) {
        viewRun(store.pendingRunId, true);
        store.pendingRunId = null;
      } else if (!meta.value.id && runs.value.length) {
        viewRun(runs.value[0].id, runs.value[0].status === "running");
      }
      refreshRuns();
      timer = setInterval(pollRun, 1500);
      histTimer = setInterval(refreshRuns, 6000);
    });
    Vue.onDeactivated(() => { clearInterval(timer); clearInterval(histTimer); });

    return { meta, logLines, logEl, runs, stopping, logH, statusType, stop, gotoResults, onHistoryClick };
  },
};
