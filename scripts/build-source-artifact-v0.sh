#!/bin/sh
set -eu

fail() {
    printf '%s\n' 'source artifact: failed: arguments' >&2
    exit 1
}

test "$#" -eq 3 || fail
mode=$1
test "$2" = "--output" || fail
output=$3
case "$mode" in
    --engineering-preview) artifact_mode=engineering-preview ;;
    --public) artifact_mode=public ;;
    *) fail ;;
esac
command -v python3 >/dev/null 2>&1 || fail
SCRIPT_DIRECTORY=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || fail
REPOSITORY_ROOT=$(CDPATH= cd -- "$SCRIPT_DIRECTORY/.." && pwd) || fail

if ! python3 "$REPOSITORY_ROOT/tools/build_source_artifact_v0.py" \
    --root "$REPOSITORY_ROOT" \
    --output "$output" \
    --mode "$artifact_mode"
then
    exit 1
fi
