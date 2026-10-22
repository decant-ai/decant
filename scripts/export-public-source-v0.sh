#!/bin/sh
set -eu

fail() {
    printf '%s\n' 'public source export: failed' >&2
    exit 1
}

test "$#" -eq 2 || fail
mode=$1
destination=$2
case "$mode" in
    --engineering-preview) export_mode=engineering-preview ;;
    --public) export_mode=public ;;
    *) fail ;;
esac
command -v python3 >/dev/null 2>&1 || fail
SCRIPT_DIRECTORY=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || fail
REPOSITORY_ROOT=$(CDPATH= cd -- "$SCRIPT_DIRECTORY/.." && pwd) || fail

if ! python3 "$REPOSITORY_ROOT/tools/export_public_source_v0.py" \
    --root "$REPOSITORY_ROOT" \
    --destination "$destination" \
    --mode "$export_mode"
then
    exit 1
fi
