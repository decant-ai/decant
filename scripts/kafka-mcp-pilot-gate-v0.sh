#!/bin/sh
set -eu

fail() {
    printf '%s\n' 'Kafka-MCP pilot gate: failed: arguments' >&2
    exit 1
}

test "$#" -eq 1 || fail
case "$1" in
    --engineering) pilot_mode=engineering ;;
    --production-pilot) pilot_mode=production-pilot ;;
    *) fail ;;
esac
command -v python3 >/dev/null 2>&1 || fail
SCRIPT_DIRECTORY=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || fail
REPOSITORY_ROOT=$(CDPATH= cd -- "$SCRIPT_DIRECTORY/.." && pwd) || fail

python3 "$REPOSITORY_ROOT/tools/run_kafka_mcp_pilot_gate_v0.py" \
    --root "$REPOSITORY_ROOT" \
    --mode "$pilot_mode"
