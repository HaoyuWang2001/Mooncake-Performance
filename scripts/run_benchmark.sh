#!/usr/bin/env bash
# run_benchmark.sh — launch mooncake_benchmark_client.py processes on the server.
#
# Usage: run_benchmark.sh MODE LOG_FILE [client args...]
#   MODE: prepare-data | clean | prepare-store | benchmark | pipeline
#
# MODE semantics:
#   - stage modes (prepare-data / clean / prepare-store): single foreground
#     process; all extra args are passed through.
#   - benchmark: --num-clients N is a SCRIPT-level flag (NOT forwarded to the
#     client). The script reads MC_KEY_COUNT from env.sh, splits the key
#     range evenly, and spawns N parallel processes, injecting
#     --client-id i --key-start S --key-end E into each. All other args are
#     passed through.
#   - pipeline: single process; --role/--client-id/--listen-port/--peers etc.
#     are passed through (writer: --client-id is the global writer id;
#     reader: --client-id is the reader index).
#
# All scenario/cluster configuration comes from env.sh (sourced here).
# Per-client stdout goes to $LOG_FILE.d/client.$i.log and is merged into
# LOG_FILE afterwards.

set -u
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/env.sh"

usage() { echo "usage: $0 MODE LOG_FILE [client args...]" >&2; exit 2; }

[ $# -ge 2 ] || usage
MODE=$1
LOG_FILE=$2
shift 2

case "$MODE" in
  prepare-data|clean|prepare-store|benchmark|pipeline) ;;
  *) echo "[FATAL] unknown MODE: $MODE" >&2; usage ;;
esac

if [ ! -f "$ENV_FILE" ]; then
  echo "[FATAL] env.sh not found in $SCRIPT_DIR" >&2
  exit 1
fi
source "$ENV_FILE"

# Collect passthrough args; extract the script-level --num-clients flag.
NUM_CLIENTS=1
ARGS=()
while [ $# -gt 0 ]; do
  if [ "$1" = "--num-clients" ] && [ $# -ge 2 ]; then
    NUM_CLIENTS=$2
    shift 2
    continue
  fi
  ARGS+=("$1")
  shift
done

if [ "$MODE" != "benchmark" ] && [ "$NUM_CLIENTS" -gt 1 ]; then
  echo "[FATAL] --num-clients is only valid for benchmark mode" >&2
  exit 1
fi

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
  for i in $(seq 1 "$NUM_CLIENTS"); do
    START=$(( (i - 1) * KEY_COUNT / NUM_CLIENTS ))
    END=$(( i * KEY_COUNT / NUM_CLIENTS ))
    # per-client ylt easylog file (absolute path; merged below with headers)
    MC_YLT_LOG_PATH="$LOG_DIR/client.$i.ylt.log" \
    "$PYTHON" "$CLIENT" \
      --mode "$MODE" \
      --client-id "$i" --key-start "$START" --key-end "$END" \
      "${ARGS[@]}" \
      > "$LOG_DIR/client.$i.log" 2>&1 &
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
  # easylog -> absolute path next to the log
  MC_YLT_LOG_PATH="$LOG_FILE.ylt.log" \
  "$PYTHON" "$CLIENT" --mode "$MODE" "${ARGS[@]}" \
    > "$LOG_FILE" 2>&1
  RC=$?
fi
exit "$RC"
