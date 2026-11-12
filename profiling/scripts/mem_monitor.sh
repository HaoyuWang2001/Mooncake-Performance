#!/usr/bin/env bash
# mem_monitor.sh — host + process memory sampler for the profiling matrix.
#
# Pushed to each node by profiling/prof_core.py and run in the background for
# the whole scenario (or the measured window). Appends CSV rows so the run can
# be interrupted without losing samples:
#   mem.host.csv : ts,<numa0_used_mb>,...,<numaN_used_mb>,mem_available_mb
#   mem.proc.csv : ts,pid,comm,rss_kb,pss_kb,private_dirty_kb,
#                  private_clean_kb,swap_kb
# Timestamps are epoch seconds (aligned with the report timeline).
#
# It is used to spot memory leaks: whole-node usage (numastat) plus per-process
# RSS/PSS (/proc/<pid>/smaps_rollup). No sudo is required for same-user
# processes (the benchmark clients run as the ssh user).
#
# env: OUT_DIR (required), INTERVAL_S (default 1),
#      PROC_PATTERN (default mooncake_benchmark_client)
set -u

OUT_DIR="${OUT_DIR:?OUT_DIR required}"
INTERVAL_S="${INTERVAL_S:-1}"
PROC_PATTERN="${PROC_PATTERN:-mooncake_benchmark_client}"

HOST_CSV="$OUT_DIR/mem.host.csv"
PROC_CSV="$OUT_DIR/mem.proc.csv"
ERR_LOG="$OUT_DIR/mem.err.log"
mkdir -p "$OUT_DIR"
: >> "$ERR_LOG"

host_header_written=0
[ -s "$HOST_CSV" ] && host_header_written=1
[ -s "$PROC_CSV" ] || echo "ts,pid,comm,rss_kb,pss_kb,private_dirty_kb,private_clean_kb,swap_kb" >> "$PROC_CSV"

while true; do
  ts=$(date +%s)

  # ---- whole-node memory: per-NUMA MemUsed (numastat -m) + MemAvailable ----
  nums=$(numastat -m 2>>"$ERR_LOG" | awk '/^MemUsed/ {for (i = 2; i <= NF; i++) printf "%s ", $i}')
  if [ -n "$nums" ]; then
    if [ "$host_header_written" -eq 0 ]; then
      hdr="ts"
      n=0
      for _v in $nums; do hdr="$hdr,n${n}_used_mb"; n=$((n + 1)); done
      hdr="$hdr,mem_available_mb"
      echo "$hdr" >> "$HOST_CSV"
      host_header_written=1
    fi
    avail_kb=$(awk '/^MemAvailable:/ {print $2}' /proc/meminfo 2>/dev/null)
    avail_mb=$(awk -v k="$avail_kb" 'BEGIN { printf "%.1f", (k == "" ? 0 : k / 1024) }')
    row="$ts"
    for v in $nums; do row="$row,$v"; done
    echo "$row,$avail_mb" >> "$HOST_CSV"
  fi

  # ---- per-process memory (benchmark clients by default) ----
  for pid in $(pgrep -f "$PROC_PATTERN" 2>/dev/null); do
    [ -r "/proc/$pid/smaps_rollup" ] || continue
    comm=$(cat "/proc/$pid/comm" 2>/dev/null || echo "?")
    rss=0; pss=0; pd=0; pc=0; sw=0
    while read -r key val _; do
      case "$key" in
        Rss:) rss=$val ;;
        Pss:) pss=$val ;;
        Private_Dirty:) pd=$val ;;
        Private_Clean:) pc=$val ;;
        Swap:) sw=$val ;;
      esac
    done < "/proc/$pid/smaps_rollup"
    echo "$ts,$pid,$comm,$rss,$pss,$pd,$pc,$sw" >> "$PROC_CSV"
  done

  sleep "$INTERVAL_S"
done
