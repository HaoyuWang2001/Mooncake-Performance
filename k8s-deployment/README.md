# k8s-deployment — k8s 形态部署与运行支持

SSH 直连裸机节点之外的第二种运行形态：**编排器创建 k8s pod（每配置 run 一批全新 pod）→
`kubectl cp` 注入脚本 → `kubectl exec` 驱动全部 stage → `kubectl cp` 拉回日志 →
`kubectl delete` 清理**。master/store 已在 k8s 内（Service 寻址，域名一律先解析为 IP）。

```
编排机（Linux + kubectl）
  ├─ kubectl apply / wait Ready / 取 Pod IP          ← ensure_pods（benchmark.py）
  ├─ kubectl cp 注入 env.sh + runners + client        ← deploy_scripts（utils 传输层分支）
  ├─ kubectl exec 跑 stage（bash -c 包裹，透传退出码）
  ├─ reader /health、/shutdown：编排机直连 PodIP:port（urllib，不经节点 curl）
  ├─ kubectl cp 拉回日志（glob→exec ls+逐文件）
  └─ kubectl delete pod -l run=<label>（成功/失败路径与 --clean-up）
```

## 前提

- 编排机 kubectl + kubeconfig（可创建/删除 pod 的 RBAC）
- 镜像：含 mooncake SDK + `python3` + `bash` + **`tar`**（`kubectl cp` 依赖；缺失时手动路径用 base64 兜底）
- pod 网络可直连 mooncake master / store（tcp）；ub 协议设备访问按模板占位段自行修改
- `cluster.json`：节点 `kind: "k8s-pod"` + 集群级 `k8s.image`（模板 `cluster.k8s.example.json`）

## 集成行为（编排器自动）

| 项 | 行为 |
| --- | --- |
| pod 生命周期 | **每配置 run 一批全新 pod**（多配置 run 互不残留）；名 `mc-<ts>-<节点名>-<配置>`，标签 `app=mooncake-benchmark, run=<ts>-<cfg>, node=<节点名>`；结束/失败/`--clean-up` 按标签删除 |
| 节点数据形态 | pod Ready 后 Pod IP 写入 `node["ip"]/["fabric_ip"]` —— k8s 节点与 SSH 节点数据形态一致，下游逻辑零感知 |
| 域名→IP | `master_addr / metadata_server / etcd_endpoints` 在 env.sh 生成前解析：IP 直通 → kubectl svc（含 headless endpoints 兜底）→ getaddrinfo；解析失败保留原值 + `[WARN]` |
| 绑核 | 容器内不做 numactl：k8s 节点 `numa_bind/cpu_shard` 强制关闭、`MC_CPU_SHARD/MC_EXCLUDE_CPUS` 不注入 |
| venv | cluster.json `venvs` 直接指向容器内 python（如 `python3`） |
| reader 健康检查 | 编排机直连 `http://<PodIP>:port/{health,shutdown}`（确认可直连）；SSH 节点保持原 curl 路径 |
| 进程管理 | `.state` 台账 + reap 走同一传输层（exec kill）；pod 删除是终极兜底 |
| ub 设备 | **编排器不感知**——按 `templates/benchmark-pod.yaml.tpl` 占位段自行修改（device plugin / privileged+hostNetwork / 镜像内置三选一） |

## 手动运行（无编排器 / P0 验证）

```bash
# 一键 basic（注入 + 三 stage + 拉回）：
bash k8s-deployment/scripts/manual-basic.sh -n mooncake -p <pod> -o put --clients 1 --threads 2 \
    --size 8MB --keys 1000 --master mooncake-master.mooncake:50051

# 生命周期调试：
bash k8s-deployment/scripts/pod_lifecycle.sh list -n mooncake
bash k8s-deployment/scripts/pod_lifecycle.sh ip   <pod> -n mooncake
bash k8s-deployment/scripts/pod_lifecycle.sh delete-all -n mooncake
```

P0 验证清单（真机第一次跑）：
1. `manual-basic.sh -o put` tcp 跑通（RESULT 行出现）→ 验证镜像 python/bash/tar、直连、解析
2. `-o get`（prepare-store 路径）→ 验证预填充
3. 修改模板 ub 段 → `--protocol ub` → 验证设备访问
4. 之后切编排器全自动（cluster.json 换 k8s 形态）

## 目录

```
k8s-deployment/
├── templates/
│   └── benchmark-pod.yaml.tpl    # pod 模板（POD_NAME/IMAGE/NAMESPACE/RUN_LABEL/NODE_LABEL 占位 + ub 注释段）
├── scripts/
│   ├── manual-basic.sh           # 手动一键 basic（含 base64 注入兜底）
│   └── pod_lifecycle.sh          # create/wait/ip/delete-run/delete-all/list
├── env.k8s.example.sh            # 手动 env.sh 模板
└── cluster.k8s.example.json      # k8s 形态 cluster.json 模板
```
