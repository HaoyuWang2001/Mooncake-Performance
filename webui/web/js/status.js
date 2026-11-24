"use strict";
/* 状态文案/颜色、相对时间与危险确认（webui-design skill：
   状态色=含义、中文文案、危险操作确认按钮强制 danger）。 */

const _MC_STATUS_TEXT = {
  created: "未运行",
  running: "运行中",
  stopping: "停止中",
  done: "已完成",
  failed: "失败",
  stopped: "已停止",
  failed_spawn: "启动失败",
  aborted: "已中止",
};

const _MC_STATUS_TYPE = {
  created: "info",
  running: "primary",
  stopping: "warning",
  done: "success",
  failed: "danger",
  stopped: "warning",
  failed_spawn: "danger",
  aborted: "warning",
};

function statusText(s) {
  return _MC_STATUS_TEXT[s] || s || "未知";
}

function statusType(s) {
  return _MC_STATUS_TYPE[s] || "info";
}

const _MC_SCHED_TEXT = {
  waiting: "等待中",
  waiting_slot: "等待实例空闲",
  executing: "执行中",
  stopping: "停止中",
  done: "已完成",
  stopped: "已停止",
  aborted: "已中止",
};

function schedulerText(s) {
  return _MC_SCHED_TEXT[s] || s || "关闭";
}

function schedulerType(s) {
  if (s === "executing") return "warning";
  if (s === "waiting" || s === "waiting_slot") return "primary";
  if (s === "done") return "success";
  if (s === "aborted") return "danger";
  return "info";
}

function _toEpoch(ts) {
  if (ts === null || ts === undefined || ts === "") return NaN;
  if (typeof ts === "number") return ts;
  const n = Number(ts);
  if (isFinite(n) && n > 1e9) return n;
  return Date.parse(ts) / 1000;
}

/* 相对时间优先（悬停 title 给绝对时间，见 fmtTimeAbs）。 */
function fmtTimeRel(ts) {
  const sec = _toEpoch(ts);
  if (!isFinite(sec)) return "-";
  const d = Math.max(0, Date.now() / 1000 - sec);
  if (d < 60) return "刚刚";
  if (d < 3600) return Math.floor(d / 60) + " 分钟前";
  if (d < 86400) return Math.floor(d / 3600) + " 小时前";
  if (d < 86400 * 30) return Math.floor(d / 86400) + " 天前";
  return new Date(sec * 1000).toLocaleDateString();
}

function fmtTimeAbs(ts) {
  const sec = _toEpoch(ts);
  if (!isFinite(sec)) return "";
  return new Date(sec * 1000).toLocaleString();
}

/* 危险操作统一确认：确认按钮强制 danger（skill 硬性规则）。 */
function confirmDanger(message, title) {
  return ElementPlus.ElMessageBox.confirm(message, title || "危险操作确认", {
    type: "warning",
    confirmButtonText: "确认",
    cancelButtonText: "取消",
    confirmButtonClass: "el-button--danger",
  });
}
