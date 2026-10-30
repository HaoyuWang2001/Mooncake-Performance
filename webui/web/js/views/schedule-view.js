"use strict";
/* Schedule 视图：计划管理（列表/编辑/新建）+ scheduler 启停与事件日志。
   计划格式与 schedule/run_schedule.py 一致；任务触发时若 benchmark 在跑会等待其结束。 */

const ScheduleView = {
  template: `
  <div class="page">
    <el-row :gutter="14">
      <el-col :span="8">
        <el-card shadow="never" style="margin-bottom: 14px">
          <template #header><b>计划列表</b></template>
          <el-table :data="plans" size="small" highlight-current-row @row-click="onPlanClick">
            <el-table-column prop="name" label="名称" show-overflow-tooltip></el-table-column>
            <el-table-column label="任务数" width="70">
              <template #default="scope">{{ scope.row.tasks ?? "-" }}</template>
            </el-table-column>
            <el-table-column label="首任务" width="150">
              <template #default="scope">{{ scope.row.first_task || "-" }}</template>
            </el-table-column>
            <el-table-column label="状态" width="80">
              <template #default="scope">
                <el-tag :type="scope.row.valid ? 'success' : 'danger'" size="small">
                  {{ scope.row.valid ? "有效" : "无效" }}</el-tag>
              </template>
            </el-table-column>
          </el-table>
          <div class="header-row" style="margin: 10px 0 0">
            <el-input v-model="newName" placeholder="新计划名 xxx.json" size="small"
                      style="width: 200px"></el-input>
            <el-button size="small" @click="newPlan">新建</el-button>
          </div>
        </el-card>

        <el-card shadow="never">
          <template #header><b>Scheduler</b></template>
          <div style="margin-bottom: 10px; font-size: 13px">
            <el-tag :type="schTagType" size="small">{{ sch ? sch.state : "off" }}</el-tag>
            <span style="margin-left: 8px; font-family: monospace">{{ schText }}</span>
          </div>
          <div class="header-row" style="margin: 0">
            <el-button type="primary" size="small" @click="start" :disabled="!currentPlan || currentPlan === 'plan.example.json'"
                       :loading="starting">启动 Scheduler</el-button>
            <el-button type="danger" size="small" @click="stop">停止</el-button>
          </div>
          <div class="logview log-sm" style="margin-top: 10px">
            <div v-for="(e, i) in schEvents" :key="i">{{ e }}</div>
          </div>
        </el-card>
      </el-col>

      <el-col :span="16">
        <el-card shadow="never">
          <template #header>
            <b>计划编辑器</b>
            <span v-if="currentPlan" style="margin-left: 10px; font-family: monospace; font-weight: 400">
              {{ currentPlan }}</span>
          </template>
          <el-input type="textarea" :rows="20" v-model="planContent" class="editor"
                    :placeholder="currentPlan ? '' : '点击左侧计划或新建'"></el-input>
          <div class="header-row" style="margin: 10px 0 0">
            <el-button size="small" @click="syntaxCheck" :disabled="!currentPlan">JSON 语法检查</el-button>
            <el-button size="small" type="primary" @click="savePlan" :disabled="!currentPlan" :loading="saving">保存计划</el-button>
            <span v-if="msg.text" :style="{ color: msg.ok ? 'var(--el-color-success)' : 'var(--el-color-error)',
                      fontSize: '12.5px' }">{{ msg.text }}</span>
          </div>
        </el-card>
      </el-col>
    </el-row>
  </div>`,
  setup() {
    const plans = Vue.ref([]);
    const currentPlan = Vue.ref("");
    const planContent = Vue.ref("");
    const newName = Vue.ref("");
    const msg = Vue.reactive({ text: "", ok: true });
    const saving = Vue.ref(false);
    const starting = Vue.ref(false);
    const sch = Vue.ref(null);

    const schTagType = Vue.computed(() => {
      const s = sch.value;
      if (!s) return "info";
      return { waiting: "primary", waiting_slot: "warning", executing: "warning",
               done: "success", stopped: "info", aborted: "danger" }[s.state] || "info";
    });
    const schText = Vue.computed(() => {
      const s = sch.value;
      if (!s) return "";
      let t = s.plan + " · " + s.tasks + " tasks";
      if (s.next_trigger && ["waiting", "waiting_slot"].includes(s.state)) t += " · 下一任务 @" + s.next_trigger;
      if (s.current_task) t += " · 执行中 task " + s.current_task;
      return t;
    });
    const schEvents = Vue.computed(() => (sch.value ? sch.value.events : []));

    async function loadPlans() {
      try { plans.value = (await api("GET", "/api/schedule/plans")).plans; }
      catch (e) { ElementPlus.ElMessage.error(e.message); }
    }

    async function onPlanClick(p) {
      currentPlan.value = p.name;
      msg.text = "";
      try {
        const r = await api("GET", "/api/schedule/plan/" + encodeURIComponent(p.name));
        planContent.value = r.content;
      } catch (e) { ElementPlus.ElMessage.error(e.message); }
    }

    function newPlan() {
      const name = newName.value.trim();
      if (!name.endsWith(".json")) { ElementPlus.ElMessage.warning("计划名必须以 .json 结尾"); return; }
      currentPlan.value = name;
      planContent.value = JSON.stringify({
        wait_seconds: 60, on_fail: "continue",
        tasks: [{ date: "2026-01-01", time: "23:00",
                  command: "python benchmark.py --config-dir configs/basic/demo",
                  output: "logs/night/demo.log" }],
      }, null, 2);
      msg.text = "新计划（未保存）";
      msg.ok = true;
    }

    function syntaxCheck() {
      try {
        JSON.parse(planContent.value);
        msg.ok = true; msg.text = "JSON 语法通过（完整校验在保存时执行）";
      } catch (e) { msg.ok = false; msg.text = "JSON 语法错误: " + e.message; }
    }

    async function savePlan() {
      saving.value = true;
      try {
        const r = await api("PUT", "/api/schedule/plan/" + encodeURIComponent(currentPlan.value),
                            { content: planContent.value });
        msg.ok = true;
        msg.text = "已保存" + (r.backup ? "（备份 " + r.backup + "）" : "");
        await loadPlans();
      } catch (e) {
        msg.ok = false; msg.text = e.message;
      } finally { saving.value = false; }
    }

    async function start() {
      starting.value = true;
      try { await api("POST", "/api/schedule/start", { plan: currentPlan.value }); }
      catch (e) { ElementPlus.ElMessage.error(e.message); }
      finally { starting.value = false; }
    }

    async function stop() {
      try { await api("POST", "/api/schedule/stop"); }
      catch (e) { ElementPlus.ElMessage.error(e.message); }
    }

    async function pollSch() {
      try { sch.value = (await api("GET", "/api/schedule/status")).scheduler; }
      catch (e) { /* ignore */ }
    }

    let timer = null;
    Vue.onActivated(() => {
      loadPlans();
      pollSch();
      timer = setInterval(pollSch, 2500);
    });
    Vue.onDeactivated(() => clearInterval(timer));

    return { plans, currentPlan, planContent, newName, msg, saving, starting,
             sch, schTagType, schText, schEvents,
             loadPlans, onPlanClick, newPlan, syntaxCheck, savePlan, start, stop };
  },
};
