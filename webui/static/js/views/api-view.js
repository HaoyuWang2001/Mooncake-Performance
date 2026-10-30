"use strict";
/* API 文档视图：渲染 webui/API.md（GET /api/help 返回 markdown 原文）。 */

const ApiView = {
  template: `
  <div class="page">
    <el-card shadow="never">
      <template #header>
        <b>HTTP API 文档</b>
        <span class="hint" style="margin-left: 10px">可通过 API 直接编程调用（curl 示例见文档末尾）</span>
      </template>
      <pre class="apidoc">{{ markdown }}</pre>
    </el-card>
  </div>`,
  setup() {
    const markdown = Vue.ref("加载中…");
    Vue.onActivated(async () => {
      try { markdown.value = (await api("GET", "/api/help")).markdown; }
      catch (e) { markdown.value = "加载失败: " + e.message; }
    });
    return { markdown };
  },
};
