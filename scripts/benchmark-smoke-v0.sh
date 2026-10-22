#!/bin/sh
set -eu

fail() {
    printf '%s\n' 'benchmark smoke: failed' >&2
    exit 1
}

for tool in cargo cmp mktemp python3; do
    command -v "$tool" >/dev/null 2>&1 || fail
done

SCRIPT_DIRECTORY=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || fail
REPOSITORY_ROOT=$(CDPATH= cd -- "$SCRIPT_DIRECTORY/.." && pwd) || fail
TEMPORARY_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/decant-benchmark-smoke-v0.XXXXXX") || fail
cleanup() {
    rm -rf -- "$TEMPORARY_ROOT"
}
trap cleanup EXIT HUP INT TERM
cd -- "$REPOSITORY_ROOT" || fail

python3 tools/check_benchmark_smoke_v0.py || fail
cargo run --quiet --release --locked --offline -p decant-cli -- demo sre \
    >"$TEMPORARY_ROOT/first.json" 2>"$TEMPORARY_ROOT/first.err" || fail
cargo run --quiet --release --locked --offline -p decant-cli -- demo sre \
    >"$TEMPORARY_ROOT/second.json" 2>"$TEMPORARY_ROOT/second.err" || fail
test ! -s "$TEMPORARY_ROOT/first.err" || fail
test ! -s "$TEMPORARY_ROOT/second.err" || fail
cmp -s "$TEMPORARY_ROOT/first.json" "$TEMPORARY_ROOT/second.json" || fail
cmp -s "$TEMPORARY_ROOT/first.json" crates/decant-cli/tests/golden/sre-demo-v0.json || fail

printf '%s\n' 'PASS: benchmark correctness smoke v0 (no measurements)'
