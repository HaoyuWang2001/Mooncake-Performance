#!/usr/bin/env bash
# run_benchmark_pipeline.sh — launch mooncake_benchmark_client.py processes for
# pipeline (精排/召排) scenarios on the server.
#
# Usage: run_benchmark_pipeline.sh MODE LOG_FILE [client args...]
#   MODE: generate-data | remove-keys | pipeline
#
# MODE semantics:
#   - stage modes (generate-data / remove-keys): single foreground process;
#     all extra args are passed through.
#   - pipeline: single process; --role/--client-id/--listen-port/--peers etc.
#     are passed through (writer: --client-id is the global writer id;
#     reader: --client-id is the reader index). There are NO script-level
#     flags — everything is forwarded to the client.
#
# All scenario/cluster configuration comes from env.sh (sourced here), then
# env.mooncake.sh (hand-editable mooncake store/SDK tuning defaults; same-name
# vars in env.mooncake.sh win). Output is written DIRECTLY to LOG_FILE (live
# while the process runs); ylt easylog goes to $LOG_FILE.ylt.log.

set -u
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$SCRIPT_DIR/env.sh"

usage() { echo "usage: $0 MODE LOG_FILE [client args...]" >&2; exit 2; }

[ $# -ge 2 ] || usage
MODE=$1
LOG_FILE=$2
shift 2

case "$MODE" in
  generate-data|remove-keys|pipeline) ;;
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

PYTHON="${MC_VENV:-python3}"
CLIENT="$SCRIPT_DIR/mooncake_benchmark_client.py"
mkdir -p "$(dirname "$LOG_FILE")"

MC_YLT_LOG_PATH="$LOG_FILE.ylt.log" \
"$PYTHON" "$CLIENT" --mode "$MODE" "$@" \
  > "$LOG_FILE" 2>&1
exit "$?"
