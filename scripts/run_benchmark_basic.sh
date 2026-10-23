#!/usr/bin/env bash
# run_benchmark_basic.sh — launch mooncake_benchmark_client.py processes for
# basic (interface benchmark) scenarios on the server.
#
# Usage: run_benchmark_basic.sh MODE LOG_FILE [client args...]
#   MODE: generate-data | remove-keys | prepare-store | benchmark
#
# MODE semantics:
#   - stage modes (generate-data / remove-keys / prepare-store): single
#     foreground process; all extra args are passed through.
#   - benchmark: --num-clients N is a SCRIPT-level flag (NOT forwarded to the
#     client). The script reads MC_KEY_COUNT from env.sh, splits the key
#     range evenly, and spawns N parallel processes, injecting
#     --client-id i --key-start S --key-end E into each. All other args are
#     passed through. --numa-bind is also script-level (binds client i to
#     NUMA node (i-1)%K, K = NUMA node count on this host).
#
# All scenario/cluster configuration comes from env.sh (sourced here), then
# env.mooncake.sh (hand-editable mooncake store/SDK tuning defaults; same-name
# vars in env.mooncake.sh win). Per-client stdout goes to
# $LOG_FILE.d/client.$i.log and is merged into LOG_FILE afterwards.

set -u
# kill backgrounded clients if this shell is interrupted or exits
trap 'kill $(jobs -p) 2>/dev/null' EXIT HUP INT TERM
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/env.sh"

usage() { echo "usage: $0 MODE LOG_FILE [client args...]" >&2; exit 2; }

[ $# -ge 2 ] || usage
MODE=$1
LOG_FILE=$2
shift 2

case "$MODE" in
  generate-data|remove-keys|prepare-store|benchmark) ;;
  *) echo "[FATAL] unknown MODE: $MODE" >&2; usage ;;
esac

if [ ! -f "$ENV_FILE" ]; then
  echo "[FATAL] env.sh not found in $SCRIPT_DIR" >&2
  exit 1
fi
source "$ENV_FILE"
# Hand-editable mooncake tuning defaults (git-tracked, pushed by deploy_scripts).
# Sourced AFTER env.sh: same-name vars here override the generated env.sh.
# -f guard keeps the script working without the file (local ad-hoc runs).
# Command-line inline env (e.g. MC_YLT_LOG_PATH) still wins over both.
[ -f "$SCRIPT_DIR/env.mooncake.sh" ] && source "$SCRIPT_DIR/env.mooncake.sh"

# Collect passthrough args; extract script-level flags (--num-clients and
# --numa-bind are consumed here and NOT forwarded to the client).
NUM_CLIENTS=1
NUMA_BIND=0
# per-client core count (NUMA-aware sharding, env from the scenario config);
# when set it supersedes plain numa round-robin in both launch branches
CPU_SHARD="${MC_CPU_SHARD:-0}"
ARGS=()
while [ $# -gt 0 ]; do
  if [ "$1" = "--num-clients" ] && [ $# -ge 2 ]; then
    NUM_CLIENTS=$2
    shift 2
    continue
  fi
  if [ "$1" = "--numa-bind" ]; then
    NUMA_BIND=1
    shift
    continue
  fi
  ARGS+=("$1")
  shift
done

if [ "$MODE" != "benchmark" ] && [ "$NUM_CLIENTS" -gt 1 ]; then
  echo "[FATAL] --num-clients is only valid for benchmark mode" >&2
  exit 1
fi

# NUMA topology: number of NUMA nodes on this host (sysfs first, lscpu fallback)
numa_node_count() {
  local k
  k=$(ls -d /sys/devices/system/node/node[0-9]* 2>/dev/null | wc -l)
  if [ "$k" -gt 0 ]; then
    echo "$k"
  else
    lscpu 2>/dev/null | grep -cE "^NUMA[[:space:]]+node[0-9]+[[:space:]]+CPU\(s\):" || echo 0
  fi
}

PYTHON="${MC_VENV:-python3}"
CLIENT="$SCRIPT_DIR/mooncake_benchmark_client.py"
mkdir -p "$(dirname "$LOG_FILE")"

if [ "$MODE" = "benchmark" ] && [ "$NUM_CLIENTS" -gt 1 ]; then
  KEY_COUNT="${MC_KEY_COUNT:-0}"
  if [ "$KEY_COUNT" -lt "$NUM_CLIENTS" ]; then
    echo "[FATAL] MC_KEY_COUNT=$KEY_COUNT < num-clients=$NUM_CLIENTS" >&2
    exit 1
  fi
  LOG_DIR="$LOG_FILE.d"
  rm -rf "$LOG_DIR"
  mkdir -p "$LOG_DIR"
  RC=0
  if [ "$NUMA_BIND" = "1" ] || [ "$CPU_SHARD" -gt 0 ]; then
    K=$(numa_node_count)
    if [ "$K" -lt 1 ]; then
      echo "[FATAL] --numa-bind: no NUMA topology found on this node" >&2
      exit 1
    fi
  else
    K=1
  fi
  # client pids are exported for the orchestrator's .state ledger (crash
  # recovery: a killed orchestrator can still find and kill these later)
  : > "$LOG_FILE.pids"
  for i in $(seq 1 "$NUM_CLIENTS"); do
    START=$(( (i - 1) * KEY_COUNT / NUM_CLIENTS ))
    END=$(( i * KEY_COUNT / NUM_CLIENTS ))
    # binding args: shard mode (numa round-robin + per-numa slice ordinal)
    # or plain numa round-robin, or none
    if [ "$CPU_SHARD" -gt 0 ]; then
      BIND_ARGS="--numa-node $(( (i - 1) % K )) --cpu-shard $CPU_SHARD --shard-index $(( (i - 1) / K ))"
    elif [ "$NUMA_BIND" = "1" ]; then
      BIND_ARGS="--numa-node $(( (i - 1) % K ))"
    else
      BIND_ARGS=""
    fi
    # per-client ylt easylog file (absolute path; merged below with headers)
    MC_YLT_LOG_PATH="$LOG_DIR/client.$i.ylt.log" \
    "$PYTHON" "$CLIENT" \
      --mode "$MODE" \
      --client-id "$i" --key-start "$START" --key-end "$END" \
      $BIND_ARGS \
      "${ARGS[@]}" \
      > "$LOG_DIR/client.$i.log" 2>&1 &
    echo $! >> "$LOG_FILE.pids"
  done
  for pid in $(jobs -p); do
    wait "$pid" || RC=1
  done
  # merge per-client logs + ylt easylog into the final LOG_FILE with headers
  : > "$LOG_FILE"
  for f in "$LOG_DIR"/client.*.log; do
    [ -f "$f" ] || continue
    cat "$f" >> "$LOG_FILE"
  done
  : > "$LOG_FILE.ylt.log"
  for f in "$LOG_DIR"/client.*.ylt.log; do
    [ -f "$f" ] || continue
    echo "===== $(basename "$f") =====" >> "$LOG_FILE.ylt.log"
    cat "$f" >> "$LOG_FILE.ylt.log"
  done
  rm -rf "$LOG_DIR"
else
  # single-process/stage mode: write DIRECTLY to LOG_FILE so the output is
  # live while the process runs (Python output is line-buffered); ylt
  # easylog -> absolute path next to the log. NUMA binding only applies to
  # benchmark mode (the single benchmark client binds NUMA node 0).
  if [ "$MODE" = "benchmark" ]; then
    if [ "$CPU_SHARD" -gt 0 ]; then
      BIND_ARGS="--numa-node 0 --cpu-shard $CPU_SHARD --shard-index 0"
    elif [ "$NUMA_BIND" = "1" ]; then
      BIND_ARGS="--numa-node 0"
    else
      BIND_ARGS=""
    fi
  else
    BIND_ARGS=""
  fi
  MC_YLT_LOG_PATH="$LOG_FILE.ylt.log" \
  "$PYTHON" "$CLIENT" --mode "$MODE" ${BIND_ARGS} "${ARGS[@]}" \
    > "$LOG_FILE" 2>&1
  RC=$?
fi
exit "$RC"
