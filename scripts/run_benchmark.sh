#!/usr/bin/env bash
# run_benchmark.sh     [--warmup true|false] [--scope ...]
# Sources env.sh for all scenario/cluster/global configuration, then launches
# mooncake_benchmark_client.py on the server. For --mode benchmark/pipeline,
# NUM_CLIENTS independent client processes are started in parallel, each with
# NUM_THREADS worker threads. Every client writes to its own log file; all logs
# are merged into LOG_FILE afterwards.

set -u
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/env.sh"
if [ ! -f "$ENV_FILE" ]; then
  echo "[FATAL] env.sh not found in $SCRIPT_DIR" >&2
  exit 1
fi
source "$ENV_FILE"
MODE=$1
NUM_CLIENTS=$2
NUM_THREADS=$3
LOG_FILE=$4
shift 4
EXTRA=()
while [ $# -gt 0 ]; do
  EXTRA+=("$1")
  shift
done
PYTHON="${MC_VENV:-python3}"
CLIENT="$SCRIPT_DIR/mooncake_benchmark_client.py"
mkdir -p "$(dirname "$LOG_FILE")"
LOG_DIR="$LOG_FILE.d"
rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"
if [ "$MODE" = "benchmark" ] || [ "$MODE" = "pipeline" ]; then
  for i in $(seq 1 "$NUM_CLIENTS"); do
    "$PYTHON" "$CLIENT" \
      --mode "$MODE" \
      --num-clients "$NUM_CLIENTS" --client-id "$i" --num-threads "$NUM_THREADS" \
      "${EXTRA[@]}" \
      > "$LOG_DIR/client.$i.log" 2>&1 &
  done
  RC=0
  for pid in $(jobs -p); do
    wait "$pid" || RC=1
  done
else
  "$PYTHON" "$CLIENT" --mode "$MODE" "${EXTRA[@]}" \
    > "$LOG_DIR/client.1.log" 2>&1
  RC=$?
fi
cat "$LOG_DIR"/client.*.log > "$LOG_FILE"
rm -rf "$LOG_DIR"
exit "$RC"
