apiVersion: v1
kind: Pod
metadata:
  name: {{MC_POD_NAME}}
  namespace: {{MC_NAMESPACE}}
  labels:
    app: mooncake-benchmark
    run: {{MC_RUN_LABEL}}
    role: {{MC_ROLE_LABEL}}
spec:
  # 占位符由 benchmark.py 渲染：{{MC_POD_NAME}} {{MC_IMAGE}} {{MC_NAMESPACE}}
  # {{MC_RUN_LABEL}} {{MC_ROLE_LABEL}} {{MC_CONFIGMAP_NAME}}
  # {{MC_SCRIPTS_MOUNT}} {{MC_WORKDIR_MOUNT}}
  # 其余段落（securityContext/resources/nodeSelector/imagePullPolicy/UB
  # device plugin）按你的集群环境自行维护。
  terminationGracePeriodSeconds: 5
  # 默认不设 nodeSelector（调度到全部可用节点）。注意：下方 resources 里的
  # UB device request 仍会限制到装有 device plugin 的节点；TCP-only 想用全部
  # 节点时，自行删减 resources 中的 unifiedbus.com/ub_net_device。
  # 如需限定节点，自行取消注释：
  # nodeSelector:
  #   mooncake-benchmark: "true"
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
  restartPolicy: Never
