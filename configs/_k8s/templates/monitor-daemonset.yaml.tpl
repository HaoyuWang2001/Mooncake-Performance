apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: {{MC_MONITOR_NAME}}
  namespace: {{MC_NAMESPACE}}
  labels:
    app: mooncake-bench-monitor
    run: {{MC_RUN_LABEL}}
spec:
  selector:
    matchLabels:
      app: mooncake-bench-monitor
      run: {{MC_RUN_LABEL}}
  updateStrategy:
    type: RollingUpdate
  template:
    metadata:
      labels:
        app: mooncake-bench-monitor
        run: {{MC_RUN_LABEL}}
        role: monitor
    spec:
      # 节点级监控 pod（每节点 1 个，随 run namespace 创建/删除）：
      #  - hostPID: 容器 /proc 即宿主进程表（mem.proc 看跨 pod 的进程）
      #  - privileged: 允许 nsenter 进入宿主 mount/net（借用节点上安装的
      #    mdcli / numastat——镜像里不装这些宿主工具）
      #  - hostPath / → /host (RO): nsenter 不可用时的 chroot /host 兜底
      #  - hostPath <host_log_root>/<ts> → <pod_ws>/logs/<ts>: 与 benchmark
      #    pod 同一挂载，采样脚本与 CSV 落在节点磁盘，收集链路与报表 reduce
      #    零改动（采样进程本身留在容器里，随 pod 删除，无宿主孤儿进程）
      hostPID: true
      # 覆盖控制面等带 taint 的节点，保证每节点都有监控 pod
      tolerations:
      - operator: Exists
      volumes:
      - name: hostroot
        hostPath:
          path: /
      - name: hostlogs
        hostPath:
          path: {{MC_HOST_LOG_DIR}}
          type: DirectoryOrCreate
      containers:
      - name: monitor
        image: {{MC_IMAGE}}
        imagePullPolicy: Never
        command: ["sleep", "infinity"]
        securityContext:
          privileged: true
        volumeMounts:
        - name: hostroot
          mountPath: /host
          readOnly: true
        - name: hostlogs
          mountPath: {{MC_LOG_MOUNT}}
      restartPolicy: Always
