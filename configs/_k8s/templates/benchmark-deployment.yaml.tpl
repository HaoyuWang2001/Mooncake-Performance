apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{MC_DEPLOY_NAME}}
  namespace: {{MC_NAMESPACE}}
  labels:
    app: mooncake-benchmark
    run: {{MC_RUN_LABEL}}
    role: {{MC_ROLE_LABEL}}
spec:
  replicas: {{MC_REPLICAS}}
  # pipeline 是短命基准负载：run 结束即删除整个 Deployment，
  # 不要在节点故障时自动补副本（补上的 pod 没有被 exec 启动进程）。
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: mooncake-benchmark
      run: {{MC_RUN_LABEL}}
      role: {{MC_ROLE_LABEL}}
  template:
    metadata:
      labels:
        app: mooncake-benchmark
        run: {{MC_RUN_LABEL}}
        role: {{MC_ROLE_LABEL}}
    spec:
      # 同角色副本尽量均匀分布在各个节点（maxSkew=1 软保证；节点故障/
      # 资源不足时允许扎堆，由使用者按需另行处理）。注意：该字段属于
      # Pod spec —— 必须放在 template.spec 里，放在 Deployment.spec 会被
      # 服务端校验拒绝（unknown field）。
      topologySpreadConstraints:
      - maxSkew: 1
        topologyKey: kubernetes.io/hostname
        whenUnsatisfiable: ScheduleAnyway
        labelSelector:
          matchLabels:
            app: mooncake-benchmark
            run: {{MC_RUN_LABEL}}
            role: {{MC_ROLE_LABEL}}
      # 占位符由 benchmark.py 渲染：{{MC_DEPLOY_NAME}} {{MC_NAMESPACE}}
      # {{MC_REPLICAS}} {{MC_RUN_LABEL}} {{MC_ROLE_LABEL}} {{MC_IMAGE}}
      # {{MC_CONFIGMAP_NAME}} {{MC_SCRIPTS_MOUNT}} {{MC_WORKDIR_MOUNT}}
      # 其余段落（securityContext/resources/imagePullPolicy/UB device
      # plugin）按你的集群环境自行维护。
      terminationGracePeriodSeconds: 5
      volumes:
      - name: scripts
        configMap:
          name: {{MC_CONFIGMAP_NAME}}
      - name: workdir
        emptyDir: {}
      containers:
      - name: benchmark
        image: {{MC_IMAGE}}
        imagePullPolicy: Never
        command: ["sleep", "infinity"]
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        - name: POD_IP
          valueFrom:
            fieldRef:
              fieldPath: status.podIP
        - name: NODE_NAME
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        securityContext:
          privileged: false
          capabilities:
            add:
            - SYS_ADMIN
        resources:
          requests:
            cpu: "16"
            memory: "64Gi"
            unifiedbus.com/ub_net_device: 1
          limits:
            cpu: "16"
            memory: "64Gi"
            unifiedbus.com/ub_net_device: 1
        volumeMounts:
        - name: scripts
          mountPath: {{MC_SCRIPTS_MOUNT}}
        - name: workdir
          mountPath: {{MC_WORKDIR_MOUNT}}
      # Deployment 的 pod template 强制 restartPolicy=Always（裸 Pod 模板
      # 的 Never 不适用于 Deployment）；容器为 sleep infinity，无副作用
      restartPolicy: Always
