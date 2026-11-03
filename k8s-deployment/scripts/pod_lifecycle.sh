#!/usr/bin/env bash
# pod_lifecycle.sh — pod 生命周期手工调试入口（编排器自动做的是同一组 kubectl 调用）。
# 用法:
#   pod_lifecycle.sh create <yaml文件>                     # kubectl apply -f
#   pod_lifecycle.sh wait   <pod> [-n ns]                  # 等待 Ready（180s）
#   pod_lifecycle.sh ip     <pod> [-n ns]                  # 打印 Pod IP
#   pod_lifecycle.sh delete-run  <run-label> [-n ns]       # 按标签删除本次 run 的全部 pod
#   pod_lifecycle.sh delete-all  [-n ns]                   # 删除全部 benchmark pod（label app=mooncake-benchmark）
#   pod_lifecycle.sh list   [-n ns]                        # 列出 benchmark pod
set -euo pipefail
NS=mooncake
cmd=${1:-}; [ $# -gt 0 ] && shift

# 提取可选 -n
args=()
while [ $# -gt 0 ]; do
  if [ "$1" = "-n" ]; then NS=$2; shift 2; else args+=("$1"); shift; fi
done
set -- "${args[@]}"

case "$cmd" in
  create)     [ $# -ge 1 ] || { echo "usage: create <yaml>" >&2; exit 2; }
               kubectl apply -f "$1" ;;
  wait)       [ $# -ge 1 ] || { echo "usage: wait <pod>" >&2; exit 2; }
               kubectl -n "$NS" wait --for=condition=Ready "pod/$1" --timeout=180s ;;
  ip)         [ $# -ge 1 ] || { echo "usage: ip <pod>" >&2; exit 2; }
               kubectl -n "$NS" get pod "$1" -o jsonpath='{.status.podIP}' ;;
  delete-run) [ $# -ge 1 ] || { echo "usage: delete-run <run-label>" >&2; exit 2; }
               kubectl -n "$NS" delete pod -l "run=$1" --ignore-not-found=true ;;
  delete-all) kubectl -n "$NS" delete pod -l app=mooncake-benchmark --ignore-not-found=true ;;
  list)       kubectl -n "$NS" get pod -l app=mooncake-benchmark -o wide ;;
  *)          grep '^#' "$0" | sed 's/^# \{0,1\}//' | head -10; exit 2 ;;
esac
