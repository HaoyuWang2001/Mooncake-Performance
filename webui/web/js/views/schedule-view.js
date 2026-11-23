"use strict";
/* Schedule 视图：计划管理（点击计划 -> 弹窗编辑）+ scheduler 启停与事件日志。
   计划格式与 schedule/run_schedule.py 一致；任务触发时若 benchmark 在跑会等待其结束。 */

const ScheduleView = {
  template: `
  <div class="page">
    <div class="mc-page-head">
      <div>
        <h1 class="title">定时任务</h1>
        <p class="desc">按计划在指定时刻运行命令；定时任务执行期间不会与运行实例并行（等待实例空闲）。</p>
      </div>
    </div>
    <el-row :gutter="14">
      <el-col :span="14">
        <el-card shadow="never">
          <template #header><b>计划列表</b></template>
          <el-table :data="plans" size="small" highlight-current-row @row-click="onPlanClick">
            <template #empty>
              <el-empty :image-size="60" description="暂无计划，在下方输入名称新建" />
            </template>
            <el-table-column prop="name" label="名称" show-overflow-tooltip></el-table-column>
            <el-table-column label="任务数" width="70" class-name="mc-num">
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
          <div class="mc-toolbar mt-2">
            <el-input v-model="newName" placeholder="新计划名 xxx.json" size="small"
                      style="width: 200px"></el-input>
            <el-button size="small" @click="newPlan">新建</el-button>
          </div>
        </el-card>
      </el-col>

      <el-col :span="10">
        <el-card shadow="never">
          <template #header><b>调度器</b></template>
          <div class="mb-2" style="font-size: 13px">
            <el-tag :type="schTagType" size="small">{{ sch ? schedulerText(sch.state) : "关闭" }}</el-tag>
            <span class="ml-2 mc-mono">{{ schText }}</span>
          </div>
          <div class="mc-toolbar">
            <el-tooltip :disabled="!startHint" :content="startHint" placement="top">
              <span>
                <el-button type="primary" size="small" @click="start"
                           :disabled="!currentPlan || currentPlan === 'plan.example.json'"
                           :loading="starting">启动调度</el-button>
              </span>
            </el-tooltip>
            <el-button type="warning" plain size="small" @click="stop">停止调度</el-button>
          </div>
          <div class="logview log-sm mt-2">
            <div v-for="(e, i) in schEvents" :key="i">{{ e }}</div>
          </div>
        </el-card>
      </el-col>
    </el-row>

    <el-dialog v-model="dlgOpen" width="56%" top="8vh" class="cfg-dlg"
               :close-on-click-modal="false" :before-close="askClose">
      <template #header>
        <b>编辑计划</b>
        <span v-if="currentPlan" class="cfg-dlg-path">{{ currentPlan }}</span>
      </template>
      <el-input type="textarea" :rows="18" v-model="planContent" class="editor" ref="planInput"></el-input>
      <el-alert v-if="msg.text" :type="msg.ok ? 'success' : 'error'"
                :title="msg.text" :closable="false" show-icon class="mt-2" />
      <template #footer>
        <el-button size="small" @click="askClose">取消</el-button>
        <el-button size="small" :disabled="!currentPlan" @click="syntaxCheck">JSON 语法检查</el-button>
        <el-button size="small" type="primary" :disabled="!currentPlan || !dirty"
                   :loading="saving" @click="savePlan">保存计划</el-button>
      </template>
    </el-dialog>
  </div>`,
  setup() {
    const plans = Vue.ref([]);
    const currentPlan = Vue.ref("");
    const planContent = Vue.ref("");
    const planSnapshot = Vue.ref("");
    const newName = Vue.ref("");
    const msg = Vue.reactive({ text: "", ok: true });
    const saving = Vue.ref(false);
    const starting = Vue.ref(false);
    const sch = Vue.ref(null);
    const dlgOpen = Vue.ref(false);
    const planInput = Vue.ref(null);

    const dirty = Vue.computed(() =>
      !!currentPlan.value && planContent.value !== planSnapshot.value);

    const startHint = Vue.computed(() => {
      if (!currentPlan.value) return "先点击计划或新建一个计划";
      if (currentPlan.value === "plan.example.json")
        return "示例计划仅供参考，请新建自己的计划";
      return "";
    });

    const schTagType = Vue.computed(() => {
      const s = sch.value;
      if (!s) return "info";
      return { waiting: "primary", waiting_slot: "warning", executing: "warning",
               stopping: "warning", done: "success", stopped: "info",
               aborted: "danger" }[s.state] || "info";
    });
    const schText = Vue.computed(() => {
      const s = sch.value;
      if (!s) return "";
      let t = s.plan + " · " + s.tasks + " 个任务";
      if (s.next_trigger && ["waiting", "waiting_slot"].includes(s.state)) t += " · 下一任务 @" + s.next_trigger;
      if (s.current_task) t += " · 执行任务 " + s.current_task;
      if (s.state === "stopping") t += " · 停止中（清理）";
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
        planSnapshot.value = r.content;
        dlgOpen.value = true;
        Vue.nextTick(() => { if (planInput.value) planInput.value.focus(); });
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
      planSnapshot.value = "";
      msg.ok = true;
      msg.text = "新计划（未保存）";
      dlgOpen.value = true;
      Vue.nextTick(() => { if (planInput.value) planInput.value.focus(); });
    }

    // 未保存关闭 -> 丢弃确认（同配置维护交互）
    function askClose(done) {
      const close = () => {
        msg.text = "";
        dlgOpen.value = false;
        if (typeof done === "function") done();
      };
      if (!dirty.value) { close(); return; }
      ElementPlus.ElMessageBox.confirm(
        "有未保存的修改，丢弃并关闭？", "未保存的修改",
        { confirmButtonText: "丢弃修改", cancelButtonText: "继续编辑",
          type: "warning" })
        .then(() => close())
        .catch(() => { /* 继续编辑 */ });
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
        ElementPlus.ElMessage.success("已保存");
        msg.text = "";
        planSnapshot.value = planContent.value;
        dlgOpen.value = false;
        await loadPlans();
      } catch (e) {
        msg.ok = false; msg.text = e.message;
      } finally { saving.value = false; }
    }

    async function start() {
      starting.value = true;
      try {
        await api("POST", "/api/schedule/start", { plan: currentPlan.value });
        ElementPlus.ElMessage.success("调度已启动");
      }
      catch (e) { ElementPlus.ElMessage.error(e.message); }
      finally { starting.value = false; }
    }

    async function stop() {
      try {
        await api("POST", "/api/schedule/stop");
        ElementPlus.ElMessage.success("调度已停止");
      }
      catch (e) { ElementPlus.ElMessage.error(e.message); }
    }

    async function pollSch() {
      try { sch.value = (await api("GET", "/api/schedule/status")).scheduler; }
      catch (e) { /* ignore */ }
    }

    let timer = null;
    function onEditorKey(e) {
      if (!dlgOpen.value) return;
      if (!(e.ctrlKey || e.metaKey)) return;
      if (e.key === "s" || e.key === "S") {
        e.preventDefault();
        if (currentPlan.value && dirty.value) savePlan();
      } else if (e.key === "Enter") {
        e.preventDefault();
        syntaxCheck();
      }
    }
    Vue.onMounted(() => window.addEventListener("keydown", onEditorKey));
    Vue.onUnmounted(() => window.removeEventListener("keydown", onEditorKey));
    Vue.onActivated(() => {
      loadPlans();
      pollSch();
      timer = setInterval(pollSch, 2500);
    });
    Vue.onDeactivated(() => clearInterval(timer));

    return { plans, currentPlan, planContent, newName, msg, saving, starting,
             dlgOpen, planInput, dirty, sch, schTagType, schText, schEvents, schedulerText,
             startHint, loadPlans, onPlanClick, newPlan, askClose,
             syntaxCheck, savePlan, start, stop };
  },
};
