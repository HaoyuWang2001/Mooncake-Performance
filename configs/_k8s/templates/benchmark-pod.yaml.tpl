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
  # 学习自用户的 benchmark DaemonSet 模板：securityContext / resources（ub
  # device plugin）/ nodeSelector / downward API。这些段落由用户按集群环境
  # 自行维护，编排器仅渲染 POD_NAME/NAMESPACE/IMAGE/CM_NAME/RUN_LABEL/
  # NODE_LABEL/SCRIPTS_MOUNT/WORKDIR_MOUNT 占位符。
  terminationGracePeriodSeconds: 5
  nodeSelector:
    mooncake-benchmark: "true"
  volumes:
  - name: scripts
    configMap:
      name: CM_NAME
  - name: workdir
    emptyDir: {}
  containers:
  - name: benchmark
    image: IMAGE
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
      mountPath: SCRIPTS_MOUNT
    - name: workdir
      mountPath: WORKDIR_MOUNT
  restartPolicy: Never
