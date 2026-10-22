#!/bin/sh
set -eu

fail() {
    printf '%s\n' 'release gate: failed: arguments' >&2
    exit 1
}

test "$#" -eq 1 || fail
case "$1" in
    --engineering-preview) release_mode=engineering-preview ;;
    --public) release_mode=public ;;
    *) fail ;;
esac
command -v python3 >/dev/null 2>&1 || fail
SCRIPT_DIRECTORY=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || fail
REPOSITORY_ROOT=$(CDPATH= cd -- "$SCRIPT_DIRECTORY/.." && pwd) || fail

if ! python3 "$REPOSITORY_ROOT/tools/run_release_gate_v0.py" \
    --root "$REPOSITORY_ROOT" \
    --mode "$release_mode"
then
    exit 1
fi
