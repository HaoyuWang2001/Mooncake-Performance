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
# env: OUT_DIR (required), INTERVAL_S (default 1), SLOT (default 2),
#      UBPUS (space-separated, default "0 1")
set -u

OUT_DIR="${OUT_DIR:?OUT_DIR required}"
INTERVAL_S="${INTERVAL_S:-1}"
SLOT="${SLOT:-2}"
UBPUS="${UBPUS:-0 1}"

CSV="$OUT_DIR/urma.flits.csv"
ERR_LOG="$OUT_DIR/urma.err.log"
mkdir -p "$OUT_DIR"
: >> "$ERR_LOG"
[ -s "$CSV" ] || echo "ts_ns,ubpu,port,rx_flits,tx_flits" >> "$CSV"

while true; do
  for UBPU in $UBPUS; do
    CMD="display diagnose/forward-enp-informations/forward-enp-information[slot=\"$SLOT\"][ubpu=$UBPU][enp-name=\"get port statistic all\"]"
    OUTPUT=$(echo "$CMD" | mdcli 2>&1)
    DATA=$(echo "$OUTPUT" | awk 'NF == 7 && $1 ~ /^[0-9]+$/ {print $1, $4, $5}')
    ts_ns=$(date +%s%N)

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
