"use strict";
/* 集群节点视图：cluster 配置节点表 + SSH 实测探测。
   形式对齐 Services-Deployment 的「集群节点」页：节点表 / 连接测试 /
   端口抽屉 / 多节点端口对照。额外合并 global.json 角色与应有端口，
   并显示节点上的 mooncake 进程（master / SP / 压测）。 */

const NodesView = {
  name: "NodesView",
  template: `
  <div class="page">
    <div class="mc-page-head">
      <div>
        <h1 class="title">集群节点</h1>
        <p class="desc">cluster 配置中的节点与角色；实测 SSH 连通、mooncake 进程与监听端口（测试手动触发，不自动轮询）</p>
      </div>
      <div class="actions">
        <el-select v-model="clusterPath" size="small" style="width: 250px" filterable
                   placeholder="选择 cluster 配置" @change="loadNodes">
          <el-option v-for="c in clusterCandidates" :key="c.path"
                     :label="c.path" :value="c.path"></el-option>
        </el-select>
        <el-select v-model="globalPath" size="small" style="width: 250px" filterable
                   placeholder="选择 global 配置（角色）" @change="loadNodes">
          <el-option v-for="g in globalCandidates" :key="g.path"
                     :label="g.path" :value="g.path"></el-option>
        </el-select>
        <el-tooltip content="刷新节点表" placement="top">
          <el-button size="small" circle :loading="loading" aria-label="刷新节点表"
                     @click="loadNodes">
            <el-icon><component is="Refresh"/></el-icon>
          </el-button>
        </el-tooltip>
        <el-button size="small" @click="openPortQuery">端口查询</el-button>
        <el-tooltip :disabled="nodes.length > 0" placement="top"
                    content="当前配置没有可探测的 SSH 节点：先选择真机 cluster 配置">
          <span>
            <el-button size="small" type="primary" :loading="testing"
                       :disabled="!nodes.length" @click="testAll">全部测试连接</el-button>
          </span>
        </el-tooltip>
      </div>
    </div>

    <el-alert v-if="error" :title="error" type="error" show-icon :closable="false"></el-alert>
    <el-alert v-if="mode === 'k8s'" type="info" show-icon :closable="false"
              title="该 cluster 配置为 k8s 模式：节点表仅含 controller（控制面），跨命名空间 mooncake 套件见下方「集群 mooncake 套件」卡片"></el-alert>

    <el-card shadow="never">
      <el-table :data="nodes" v-loading="loading" size="small">
        <template #empty>
          <el-empty description="该配置没有可探测的 SSH 节点" :image-size="60">
            <el-button size="small" @click="loadConfigs">重新加载配置</el-button>
          </el-empty>
        </template>
        <el-table-column prop="name" label="名称" width="130" show-overflow-tooltip />
        <el-table-column label="IP / fabric" width="150">
          <template #default="{ row }">
            <span class="mc-mono">{{ row.ip || "-" }}</span>
            <span v-if="row.fabric_ip && row.fabric_ip !== row.ip" class="mc-mono mc-muted"> ｜ {{ row.fabric_ip }}</span>
          </template>
        </el-table-column>
        <el-table-column prop="ssh_user" label="用户名" width="95" show-overflow-tooltip />
        <el-table-column label="端口" width="64">
          <template #default="{ row }"><span class="mc-num">{{ row.ssh_port }}</span></template>
        </el-table-column>
        <el-table-column label="角色" min-width="140">
          <template #default="{ row }">
            <el-tooltip v-for="r in row.roles" :key="r" :content="roleHint(r)" placement="top">
              <el-tag size="small" :type="roleType(r)" effect="plain" class="tag-gap">{{ roleText(r) }}</el-tag>
            </el-tooltip>
          </template>
        </el-table-column>
        <el-table-column label="连接状态" width="100">
          <template #default="{ row }">
            <el-tag v-if="probe[row.name] && probe[row.name].ok" type="success" size="small">正常</el-tag>
            <el-tooltip v-else-if="probe[row.name]" :content="probe[row.name].detail" placement="top">
              <el-tag type="danger" size="small">失败</el-tag>
            </el-tooltip>
            <el-tag v-else type="info" size="small" effect="plain">未测试</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="mooncake 服务" min-width="190">
          <template #default="{ row }">
            <template v-if="probe[row.name] && probe[row.name].ok">
              <el-tag v-for="p in probe[row.name].processes" :key="p.pid"
                      size="small" :type="procType(p.kind)" effect="plain" class="tag-gap">
                {{ procText(p.kind) }} #{{ p.pid }}
              </el-tag>
              <span v-if="!probe[row.name].processes.length" class="mc-muted">无进程</span>
            </template>
            <span v-else class="mc-muted">-</span>
          </template>
        </el-table-column>
        <el-table-column label="应有端口" min-width="160">
          <template #default="{ row }">
            <template v-if="probe[row.name] && probe[row.name].ok && probe[row.name].ports.length">
              <el-tooltip v-for="p in probe[row.name].ports" :key="p.purpose + p.port"
                          :content="p.purpose + (p.listening ? '：监听中' : '：未监听')" placement="top">
                <el-tag size="small" :type="p.listening ? 'success' : 'danger'"
                        effect="plain" class="tag-gap mc-num">
                  {{ p.port }}{{ p.listening ? "✓" : "✗" }}
                </el-tag>
              </el-tooltip>
            </template>
            <span v-else class="mc-muted">-</span>
          </template>
        </el-table-column>
        <el-table-column label="环境" min-width="150">
          <template #default="{ row }">
            <span v-if="probe[row.name] && probe[row.name].ok">
              {{ probe[row.name].kernel }}（{{ probe[row.name].arch }}）
            </span>
            <span v-else class="mc-muted">-</span>
          </template>
        </el-table-column>
        <el-table-column label="操作" width="140" fixed="right">
          <template #default="{ row }">
            <el-button size="small" link type="primary" :loading="testingOne === row.name"
                       @click="testOne(row.name)">测试</el-button>
            <el-button size="small" link type="primary" @click="openPorts(row.name)">端口</el-button>
            <el-button v-if="row.roles.includes('controller')" size="small" link type="primary"
                       :loading="k8sLoading" @click="loadK8s">k8s 套件</el-button>
          </template>
        </el-table-column>
      </el-table>
    </el-card>

    <!-- 集群 mooncake 套件（跨命名空间，controller kubectl） -->
    <el-card shadow="never" v-if="controllerName" class="mt-3">
      <template #header>
        <div class="mc-page-head">
          <div>
            <h3 class="title" style="font-size:15px">集群 mooncake 套件（多命名空间）</h3>
            <p class="desc">从 controller 用 kubectl 列出所有命名空间下的 mooncake 套件；非本部署命名空间标「他人套件」</p>
          </div>
          <div class="actions">
            <el-button size="small" type="primary" :loading="k8sLoading" @click="loadK8s">刷新集群套件</el-button>
          </div>
        </div>
      </template>
      <el-alert v-if="k8sError" :title="k8sError" type="error" show-icon :closable="false" class="mb-2"></el-alert>
      <el-table :data="k8sPods" v-loading="k8sLoading" size="small">
        <template #empty><el-empty description="暂无 mooncake 套件（或 controller 不可达 / 未刷新）" :image-size="60" /></template>
        <el-table-column label="命名空间" min-width="160">
          <template #default="{ row }">
            <el-tag v-if="row.namespace !== currentNs" size="small" type="warning" effect="plain">{{ row.namespace }}</el-tag>
            <el-tag v-else size="small" type="info" effect="plain">{{ row.namespace }}</el-tag>
          </template>
        </el-table-column>
        <el-table-column prop="name" label="Pod" min-width="230" show-overflow-tooltip />
        <el-table-column label="状态" width="110">
          <template #default="{ row }"><el-tag size="small" :type="podStatusType(row.status)">{{ row.status }}</el-tag></template>
        </el-table-column>
        <el-table-column prop="node" label="所在节点" min-width="130" show-overflow-tooltip />
        <el-table-column label="时长" width="90"><template #default="{ row }"><span class="mc-num">{{ row.age }}</span></template></el-table-column>
        <el-table-column label="重启" width="70"><template #default="{ row }"><span class="mc-num">{{ row.restarts }}</span></template></el-table-column>
      </el-table>
    </el-card>

    <!-- 节点监听端口抽屉 -->
    <el-drawer v-model="portsVisible" :title="'节点 ' + portsNode + ' 监听端口'" size="62%">
      <div class="mc-toolbar">
        <el-input v-model="portsFilter" placeholder="按端口/进程/用户过滤" size="small"
                  clearable style="width: 260px" />
        <span class="spacer"></span>
        <el-button size="small" @click="loadPorts(portsNode)" :loading="portsLoading">刷新</el-button>
      </div>
      <el-alert v-if="portsWarning" :title="portsWarning" type="warning" show-icon
                :closable="false" class="mb-2"></el-alert>
      <el-alert v-if="portsError" :title="portsError" type="error" show-icon
                :closable="false" class="mb-2"></el-alert>
      <el-table :data="filteredPorts" v-loading="portsLoading" size="small">
        <template #empty><el-empty description="暂无监听端口（或节点不可达）" :image-size="60" /></template>
        <el-table-column prop="proto" label="协议" width="70" />
        <el-table-column prop="state" label="状态" width="90" />
        <el-table-column prop="local_addr" label="监听地址" min-width="140" show-overflow-tooltip />
        <el-table-column label="端口" width="90" sortable>
          <template #default="{ row }"><span class="mc-num">{{ row.port }}</span></template>
        </el-table-column>
        <el-table-column label="用户" width="110">
          <template #default="{ row }">
            <span v-if="usersOf(row).length">{{ usersOf(row).join("、") }}</span>
            <span v-else class="mc-muted">-</span>
          </template>
        </el-table-column>
        <el-table-column label="进程" min-width="200">
          <template #default="{ row }">
            <span v-if="row.processes.length">
              <span v-for="(p, i) in row.processes" :key="i" class="mc-mono">
                {{ p.process }}<span class="mc-muted">（#{{ p.pid }}）</span>{{ i < row.processes.length - 1 ? "、" : "" }}
              </span>
            </span>
            <span v-else class="mc-muted">-</span>
          </template>
        </el-table-column>
      </el-table>
    </el-drawer>

    <!-- 端口对照查询 -->
    <el-dialog v-model="portQueryVisible" title="端口查询：多节点占用对照" width="760px">
      <div class="mc-toolbar">
        <span class="mc-label">端口号</span>
        <el-input-number v-model="pqPort" :min="1" :max="65535" controls-position="right"
                         size="small" style="width: 140px" />
        <span class="mc-label">节点</span>
        <el-select v-model="pqNodes" multiple filterable placeholder="选择节点" size="small"
                   style="width: 340px">
          <el-option v-for="n in nodes" :key="n.name" :label="n.name" :value="n.name" />
        </el-select>
        <el-button type="primary" size="small" :loading="pqLoading" @click="runPortQuery">查询</el-button>
      </div>
      <el-table :data="pqResults" v-loading="pqLoading" size="small" class="mb-2">
        <template #empty><el-empty description="输入端口号并选择节点后查询" :image-size="60" /></template>
        <el-table-column prop="node" label="节点" width="120" />
        <el-table-column label="结果" width="90">
          <template #default="{ row }">
            <el-tag v-if="row.ok === null" type="info" size="small" effect="plain">未查询</el-tag>
            <el-tag v-else-if="row.ok && row.matches.length" type="danger" size="small">占用</el-tag>
            <el-tag v-else-if="row.ok" type="success" size="small">空闲</el-tag>
            <el-tag v-else type="warning" size="small">失败</el-tag>
          </template>
        </el-table-column>
        <el-table-column label="占用进程 / 说明" min-width="320">
          <template #default="{ row }">
            <span v-if="row.ok && row.matches.length" class="mc-mono">
              <span v-for="(m, i) in row.matches" :key="i">
                {{ m.proto }} {{ m.local_addr }}:{{ m.port }} → {{ portProcs(m) }}{{ i < row.matches.length - 1 ? "；" : "" }}
              </span>
            </span>
            <span v-else>{{ row.detail || "-" }}</span>
          </template>
        </el-table-column>
      </el-table>
    </el-dialog>
  </div>`,
  setup() {
    const loading = Vue.ref(false);
    const testing = Vue.ref(false);
    const testingOne = Vue.ref("");
    const error = Vue.ref("");
    const mode = Vue.ref("ssh");
    const nodes = Vue.ref([]);
    const probe = Vue.reactive({});
    const clusterPath = Vue.ref("");
    const globalPath = Vue.ref("");
    const clusterCandidates = Vue.ref([]);
    const globalCandidates = Vue.ref([]);

    const ROLE_TEXT = { store: "SP", master: "master", metadata: "metadata",
                        etcd: "etcd", client: "客户端", controller: "controller" };
    const ROLE_TYPE = { store: "success", master: "warning", metadata: "info",
                        etcd: "info", client: "info", controller: "warning" };
    const ROLE_HINT = {
      store: "mooncake_store_nodes 中的存储节点（store / SP）",
      master: "master_addr 所在节点",
      metadata: "metadata_server 所在节点",
      etcd: "etcd_endpoints 所在节点",
      client: "非 store/master 角色（压测客户端或编排节点）",
      controller: "k8s 控制面（kubectl + 编译机）：可探测连通/进程/端口，并查看跨命名空间 mooncake 套件",
    };
    const PROC_TEXT = { master: "master", store: "SP", benchmark: "压测", other: "mooncake" };
    const PROC_TYPE = { master: "warning", store: "success", benchmark: "primary", other: "info" };

    function roleText(r) { return ROLE_TEXT[r] || r; }
    function roleType(r) { return ROLE_TYPE[r] || "info"; }
    function roleHint(r) { return ROLE_HINT[r] || r; }
    function procText(k) { return PROC_TEXT[k] || "mooncake"; }
    function procType(k) { return PROC_TYPE[k] || "info"; }
    function usersOf(row) { return [...new Set((row.processes || []).map(p => p.user).filter(Boolean))]; }
    function portProcs(m) {
      return (m.processes || []).map(p =>
        (p.user ? p.user + ":" : "") + p.process + "（#" + p.pid + "）").join("、") || "未知进程";
    }

    function _sshCandidate(c) {
      const owner = (c && typeof c === "object") ? c.owner : null;
      return !owner || owner === "_ssh" || owner === "root" || owner === "_k8s";
    }
    function _pick(list, fileName) {
      const hit = (list || []).find(c => (c.path || "").split("/").pop() === fileName);
      return hit ? hit.path : "";
    }
    function _q() {
      return "?cluster=" + encodeURIComponent(clusterPath.value) +
             "&global=" + encodeURIComponent(globalPath.value);
    }

    async function loadConfigs() {
      try {
        const cfg = await api("GET", "/api/configs");
        const cs = (cfg.cluster_candidates || []).filter(_sshCandidate);
        const gs = (cfg.global_candidates || []).filter(_sshCandidate);
        clusterCandidates.value = cs;
        globalCandidates.value = gs;
        if (!clusterPath.value) clusterPath.value = _pick(cs, "cluster.json") || (cs[0] && cs[0].path) || "";
        if (!globalPath.value) globalPath.value = _pick(gs, "global.json") || (gs[0] && gs[0].path) || "";
        await loadNodes();
      } catch (e) {
        error.value = "加载配置清单失败：" + e.message + "；请检查 configs/ 后点「重新加载配置」";
      }
    }

    async function loadNodes() {
      if (!clusterPath.value) { nodes.value = []; return; }
      loading.value = true;
      error.value = "";
      try {
        const d = await api("GET", "/api/nodes" + _q());
        nodes.value = d.nodes || [];
        mode.value = d.mode || "ssh";
        for (const k of Object.keys(probe)) {
          if (!nodes.value.some(n => n.name === k)) delete probe[k];
        }
        if (controllerName.value) loadK8s();
      } catch (e) {
        error.value = "加载节点失败：" + e.message;
        nodes.value = [];
      } finally {
        loading.value = false;
      }
    }

    async function probeNode(name) {
      try {
        const r = await api("GET", "/api/nodes/" + encodeURIComponent(name) + "/test" + _q());
        probe[name] = r;
      } catch (e) {
        probe[name] = { ok: false, detail: e.message, processes: [], ports: [] };
      }
      return probe[name];
    }

    async function testOne(name) {
      testingOne.value = name;
      try {
        const r = await probeNode(name);
        if (!r.ok) ElementPlus.ElMessage.warning(name + "：" + (r.detail || "连接失败"));
      } finally { testingOne.value = ""; }
    }

    async function testAll() {
      if (!nodes.value.length) return;
      testing.value = true;
      const queue = nodes.value.map(n => n.name);
      const out = [];
      const worker = async () => {
        while (queue.length) {
          const name = queue.shift();
          out.push({ name, r: await probeNode(name) });
        }
      };
      try {
        await Promise.all(Array.from({ length: Math.min(5, queue.length) }, worker));
        const failed = out.filter(x => !x.r.ok);
        if (!failed.length) {
          ElementPlus.ElMessage.success("全部 " + out.length + " 个节点连接正常");
        } else {
          ElementPlus.ElMessage.warning(out.length + " 个节点中 " + failed.length +
            " 个连接失败：" + failed.map(f => f.name).join("、"));
        }
      } finally { testing.value = false; }
    }

    /* ---- 端口抽屉 ---- */
    const portsVisible = Vue.ref(false);
    const portsNode = Vue.ref("");
    const portsData = Vue.ref([]);
    const portsLoading = Vue.ref(false);
    const portsError = Vue.ref("");
    const portsWarning = Vue.ref("");
    const portsFilter = Vue.ref("");
    const filteredPorts = Vue.computed(() => {
      const kw = portsFilter.value.trim().toLowerCase();
      if (!kw) return portsData.value;
      return portsData.value.filter(p => String(p.port).includes(kw) ||
        (p.processes || []).some(x =>
          (x.process || "").toLowerCase().includes(kw) ||
          (x.user || "").toLowerCase().includes(kw)));
    });
    async function loadPorts(name) {
      portsLoading.value = true;
      portsError.value = "";
      portsWarning.value = "";
      try {
        const r = await api("GET", "/api/nodes/" + encodeURIComponent(name) +
          "/ports?cluster=" + encodeURIComponent(clusterPath.value));
        if (r.ok) {
          portsData.value = r.ports || [];
          portsWarning.value = r.warning || "";
        } else {
          portsData.value = [];
          portsError.value = r.detail || "读取端口失败；请确认节点可达";
        }
      } catch (e) {
        portsData.value = [];
        portsError.value = e.message;
      } finally { portsLoading.value = false; }
    }
    async function openPorts(name) {
      portsNode.value = name;
      portsFilter.value = "";
      portsVisible.value = true;
      await loadPorts(name);
    }

    /* ---- 端口对照查询 ---- */
    const portQueryVisible = Vue.ref(false);
    const pqPort = Vue.ref(22);
    const pqNodes = Vue.ref([]);
    const pqLoading = Vue.ref(false);
    const pqResults = Vue.ref([]);
    function openPortQuery() {
      if (!pqNodes.value.length) pqNodes.value = nodes.value.map(n => n.name);
      portQueryVisible.value = true;
    }
    async function runPortQuery() {
      if (!pqPort.value || !pqNodes.value.length) {
        ElementPlus.ElMessage.warning("请输入端口号并选择节点");
        return;
      }
      pqLoading.value = true;
      pqResults.value = pqNodes.value.map(n => ({ node: n, ok: null, matches: [], detail: "" }));
      const q = "?cluster=" + encodeURIComponent(clusterPath.value);
      await Promise.all(pqNodes.value.map(async (n) => {
        const row = pqResults.value.find(r => r.node === n);
        try {
          const r = await api("GET", "/api/nodes/" + encodeURIComponent(n) + "/ports" + q);
          if (r.ok) {
            row.ok = true;
            row.matches = (r.ports || []).filter(p => p.port === pqPort.value);
          } else {
            row.ok = false;
            row.detail = r.detail || "读取失败";
          }
        } catch (e) {
          row.ok = false;
          row.detail = e.message;
        }
      }));
      pqLoading.value = false;
    }

    Vue.onActivated(() => {
      if (!clusterCandidates.value.length) loadConfigs();
      else loadNodes();
    });

    /* ---- 集群 mooncake 套件（跨命名空间，controller kubectl） ---- */
    const k8sPods = Vue.ref([]);
    const k8sLoading = Vue.ref(false);
    const k8sError = Vue.ref("");
    const currentNs = Vue.ref("");
    const controllerName = Vue.computed(() => {
      const c = (nodes.value || []).find(n => (n.roles || []).includes("controller"));
      return c ? c.name : "";
    });
    function podStatusType(s) {
      if (s === "Running") return "success";
      if (s === "Pending") return "warning";
      if (s === "Failed") return "danger";
      if (s === "Succeeded") return "success";
      return "info";
    }
    async function loadK8s() {
      const cn = controllerName.value;
      if (!cn) return;
      k8sLoading.value = true;
      k8sError.value = "";
      try {
        const r = await api("GET", "/api/nodes/" + encodeURIComponent(cn) + "/k8s" + _q());
        if (r.ok) {
          k8sPods.value = r.pods || [];
          currentNs.value = r.current_namespace || "";
        } else {
          k8sPods.value = [];
          k8sError.value = r.detail || "查询失败";
        }
      } catch (e) {
        k8sPods.value = [];
        k8sError.value = e.message;
      } finally {
        k8sLoading.value = false;
      }
    }

    return {
      loading, testing, testingOne, error, mode, nodes, probe,
      clusterPath, globalPath, clusterCandidates, globalCandidates,
      loadConfigs, loadNodes, testOne, testAll,
      roleText, roleType, roleHint, procText, procType, usersOf, portProcs,
      portsVisible, portsNode, portsData, portsLoading, portsError, portsWarning,
      portsFilter, filteredPorts, loadPorts, openPorts,
      portQueryVisible, pqPort, pqNodes, pqLoading, pqResults, openPortQuery, runPortQuery,
      controllerName, k8sPods, k8sLoading, k8sError, currentNs, loadK8s, podStatusType,
    };
  },
};
