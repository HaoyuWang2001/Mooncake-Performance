#!/usr/bin/env bash
# urma_monitor.sh — UBPU port traffic recorder for the profiling matrix.
#
# Pushed to each node by profiling/prof_core.py and run in the background.
# Wraps the IODie/UBPU port-statistic command exposed by `mdcli` and records
# the RAW flit counters (rate is computed later by report.py, so a restart or
# a lost sample never corrupts the series and counters are idempotent):
#   urma.flits.csv : ts_ns,ubpu,port,rx_flits,tx_flits
#
# Only nodes that actually have `mdcli` are started (prof_core probes it), so
# URMA monitoring silently stays off on hosts without the tool.
#
# env: OUT_DIR (required), INTERVAL_S (default 1), SLOT (default 2; only a
#      HINT — the available slot is probed from mdcli's own error message and
#      overrides it), UBPUS (space-separated, default "0 1"),
#      MDCLI_BIN (default "mdcli"; k8s 节点级监控设成
#                 "nsenter -t 1 -m -n -- mdcli" / "chroot /host mdcli"
#                 以借用宿主机安装的 mdcli——镜像里不装同名工具),
#      EPOCH_BASE (optional; orchestrator clock at sampler start — when set,
#                  ts_ns = EPOCH_BASE*1e9 + elapsed_ns, i.e. orchestrator time
#                  domain, aligned with the kvtest report window despite node
#                  clock skew; unset = node absolute time)
set -u

OUT_DIR="${OUT_DIR:?OUT_DIR required}"
INTERVAL_S="${INTERVAL_S:-1}"
SLOT="${SLOT:-2}"
EPOCH_BASE="${EPOCH_BASE:-}"
case "$EPOCH_BASE" in ''|*[!0-9]*) EPOCH_BASE="" ;; esac
START_NS=$(date +%s%N)

query() {  # $1=slot $2=ubpu
  echo "display diagnose/forward-enp-informations/forward-enp-information[slot=\"$1\"][ubpu=$2][enp-name=\"get port statistic all\"]" | $MDCLI_BIN 2>&1
}
UBPUS="${UBPUS:-0 1}"
MDCLI_BIN="${MDCLI_BIN:-mdcli}"

CSV="$OUT_DIR/urma.flits.csv"
ERR_LOG="$OUT_DIR/urma.err.log"
mkdir -p "$OUT_DIR"
: >> "$ERR_LOG"
[ -s "$CSV" ] || echo "ts_ns,ubpu,port,rx_flits,tx_flits" >> "$CSV"

while true; do
  for UBPU in $UBPUS; do
    OUTPUT=$(query "$SLOT" "$UBPU")
    DATA=$(echo "$OUTPUT" | awk 'NF == 7 && $1 ~ /^[0-9]+$/ {print $1, $4, $5}')

    # slot 探测（权威）：mdcli 报错里直接给出可用 slot，切一次并重试；
    # 配置里的 SLOT 仅作首选提示（不同机器 slot 编号不同，如 <1> vs 2）。
    if [ -z "$DATA" ]; then
      AVAIL=$(echo "$OUTPUT" | sed -n 's/.*available slot id is <\([0-9][0-9]*\)>.*/\1/p' | head -1)
      if [ -n "$AVAIL" ] && [ "$AVAIL" != "$SLOT" ]; then
        echo "$(date '+%F %T') slot probe: configured=$SLOT available=$AVAIL -> using $AVAIL" >> "$ERR_LOG"
        SLOT="$AVAIL"
        OUTPUT=$(query "$SLOT" "$UBPU")
        DATA=$(echo "$OUTPUT" | awk 'NF == 7 && $1 ~ /^[0-9]+$/ {print $1, $4, $5}')
      fi
    fi

    if [ -n "$EPOCH_BASE" ]; then
      ts_ns=$(( EPOCH_BASE * 1000000000 + $(date +%s%N) - START_NS ))
    else
      ts_ns=$(date +%s%N)
    fi

    if [ -z "$DATA" ]; then
      echo "$(date '+%F %T') slot=$SLOT ubpu=$UBPU fetch failed: $(echo "$OUTPUT" | grep -Ei 'error|fail' | head -3 | tr '\n' ' ')" >> "$ERR_LOG"
      continue
    fi

    while read -r port rx tx; do
      case "$port" in ''|*[!0-9]*) continue ;; esac
      case "$rx" in ''|*[!0-9]*) continue ;; esac
      case "$tx" in ''|*[!0-9]*) continue ;; esac
      echo "$ts_ns,$UBPU,$port,$rx,$tx" >> "$CSV"
    done <<< "$DATA"
  done
  sleep "$INTERVAL_S"
done
