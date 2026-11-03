apiVersion: v1
kind: Pod
metadata:
  name: POD_NAME
  namespace: NAMESPACE
  labels:
    app: mooncake-benchmark
    run: RUN_LABEL
    node: NODE_LABEL
spec:
  # NOTE — ub/RDMA 设备访问占位段（按集群情况修改，编排器不感知）：
  #   方案 1  device plugin 资源配额：
  #     containers[0].resources.limits 增加 {"rdma/hca": "1"}（资源名按插件实际）
  #   方案 2  privileged + hostNetwork，并挂载设备目录：
  #     containers[0].securityContext.privileged: true
  #     spec.hostNetwork: true
  #   方案 3  镜像/集群已内置设备访问（如 SR-IOV / vDPA CNI）——无需修改本模板
  containers:
  - name: benchmark
    image: IMAGE
    # 常驻占位：pod 生命周期 = 配置 run，全部 stage 由编排器 kubectl exec 驱动
    command: ["sleep", "infinity"]
    # resources:
    #   limits:
    #     rdma/hca: 1
    # securityContext:
    #   privileged: true
  # hostNetwork: true
  restartPolicy: Never
