#!/usr/bin/env bash
# manual-basic.sh — 手动在单个 pod 内跑 basic 场景（无编排器时的临时路径 / P0 验证）。
# 完成注入 env.sh + 三个 stage + 日志拉回。失败可重跑（幂等注入）。
#
# 用法:
#   bash manual-basic.sh -n <namespace> -p <pod> [-o put|get] [--clients N] [--threads N]
#        [--size 8MB] [--keys 1000] [--master host:port] [--protocol tcp|ub] [--keep]
# 依赖: kubectl 可用；pod 内有 bash/python3/tar（kubectl cp 需要 tar；缺失时自动 base64 兜底）。
set -euo pipefail

NS=mooncake; POD=""; OP=put; CLIENTS=1; THREADS=2; SIZE=8MB; KEYS=1000
MASTER=""; PROTOCOL=tcp; KEEP=0
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPTS_SRC="$REPO_DIR/scripts"

while [ $# -gt 0 ]; do
  case "$1" in
    -n) NS=$2; shift 2;;
    -p) POD=$2; shift 2;;
    -o) OP=$2; shift 2;;
    --clients) CLIENTS=$2; shift 2;;
    --threads) THREADS=$2; shift 2;;
    --size) SIZE=$2; shift 2;;
    --keys) KEYS=$2; shift 2;;
    --master) MASTER=$2; shift 2;;
    --protocol) PROTOCOL=$2; shift 2;;
    --keep) KEEP=1; shift;;
    *) echo "unknown arg: $1" >&2; exit 2;;
  esac
done
[ -n "$POD" ] || { echo "usage: $0 -n <ns> -p <pod> [options]" >&2; exit 2; }

DIR=/tmp/mooncake_bench/scripts
TS=$(date +%m%d-%H%M)
LOGDIR=/tmp/mooncake_bench/results/$TS/logs
POD_IP=$(kubectl -n "$NS" get pod "$POD" -o jsonpath='{.status.podIP}')
[ -n "$POD_IP" ] || { echo "[FATAL] cannot resolve Pod IP (pod not running?)" >&2; exit 1; }

# resolve master host -> IP（域名先解析后使用）
resolve_host() {
  local host=$1
  if [[ "$host" =~ ^[0-9.]+$ || "$host" =~ : ]]; then echo "$host"; return; fi
  local svc ns
  svc=${host%%.*}; ns=$(echo "$host" | cut -d. -f2)
  local ip
  ip=$(kubectl -n "${ns:-$NS}" get svc "$svc" -o jsonpath='{.spec.clusterIP}' 2>/dev/null || true)
  [ -z "$ip" ] || [ "$ip" = "None" ] || { echo "$ip"; return; }
  ip=$(getent hosts "$host" 2>/dev/null | awk '{print $1}' | head -1 || true)
  echo "${ip:-$host}"
}
MASTER_HOST="${MASTER%%:*}"; MASTER_PORT="${MASTER##*:}"
[ -z "$MASTER" ] || MASTER="$(resolve_host "$MASTER_HOST"):$MASTER_PORT"

echo "== pod=$POD ns=$NS ip=$POD_IP op=$OP c=$CLIENTS t=$THREADS size=$SIZE master=$MASTER"

# 1) 注入（tar 缺失时 base64 兜底）
kubectl -n "$NS" exec "$POD" -- mkdir -p "$DIR" "$LOGDIR"
inject() {
  local f=$1 dst="$DIR/$(basename "$1")"
  if kubectl -n "$NS" cp "$f" "$NS/$POD:$dst" 2>/dev/null; then return 0; fi
  echo "[warn] kubectl cp failed (no tar?), base64 fallback: $f"
  base64 -w0 "$f" | kubectl -n "$NS" exec -i "$POD" -- bash -c "base64 -d > '$dst'"
}
inject "$SCRIPTS_SRC/run_benchmark_basic.sh"
inject "$SCRIPTS_SRC/mooncake_benchmark_client.py"

# 2) env.sh
cat > /tmp/mc-env-$$.sh <<EOF
export MC_VENV=python3
export MC_MASTER_TYPE=http
export MC_MASTER_ADDR=${MASTER:-<master-IP:port>}
export MC_PROTOCOL=$PROTOCOL
export MC_KEY_PREFIX=mc_bench
export MC_KEY_COUNT=$KEYS
export MC_DATA_SIZE=$SIZE
export MC_REMOTE_DATA_DIR=/tmp/mooncake_bench/data
export MC_FABRIC_IP=$POD_IP
export MC_TCP_BIND_ADDRESS=$POD_IP
EOF
inject /tmp/mc-env-$$.sh
mv /tmp/mc-env-$$.sh /dev/null 2>/dev/null || rm -f /tmp/mc-env-$$.sh
kubectl -n "$NS" exec "$POD" -- mv "$DIR/mc-env-$$.sh" "$DIR/env.sh"
kubectl -n "$NS" exec "$POD" -- chmod +x "$DIR/run_benchmark_basic.sh"

# 3) 三 stage
kubectl -n "$NS" exec "$POD" -- bash "$DIR/run_benchmark_basic.sh" generate-data "$LOGDIR/$OP.generate-data.log" --data-size "$SIZE"
if [ "$OP" = "get" ]; then
  kubectl -n "$NS" exec "$POD" -- bash "$DIR/run_benchmark_basic.sh" prepare-store "$LOGDIR/$OP.prepare-store.log"
else
  kubectl -n "$NS" exec "$POD" -- bash "$DIR/run_benchmark_basic.sh" remove-keys "$LOGDIR/$OP.remove-keys.log" --scope full
fi
kubectl -n "$NS" exec "$POD" -- bash "$DIR/run_benchmark_basic.sh" benchmark \
  "$LOGDIR/$OP.benchmark_c${CLIENTS}_t${THREADS}_${SIZE}.log" \
  --num-clients "$CLIENTS" --num-threads "$THREADS" --warmup true

# 4) 拉回
OUT="out-$NS-$POD-$TS"
mkdir -p "$OUT"
kubectl -n "$NS" exec "$POD" -- bash -c "ls -1 $LOGDIR/*" | while read -r f; do
  kubectl -n "$NS" cp "$NS/$POD:$f" "$OUT/$(basename "$f")"
done
echo "== done -> $OUT/ （RESULT/META 行格式与编排器一致；plot_latency.py --log $OUT 可直接出图）"
[ "$KEEP" = "1" ] || kubectl -n "$NS" exec "$POD" -- rm -rf "$LOGDIR"
