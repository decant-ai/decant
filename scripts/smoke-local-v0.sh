#!/bin/sh
set -eu

fail() {
    printf '%s\n' 'local smoke: failed' >&2
    exit 1
}

for tool in cargo cmp mktemp python3; do
    command -v "$tool" >/dev/null 2>&1 || fail
done

SCRIPT_DIRECTORY=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || fail
REPOSITORY_ROOT=$(CDPATH= cd -- "$SCRIPT_DIRECTORY/.." && pwd) || fail
TEMPORARY_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/decant-local-smoke-v0.XXXXXX") || fail
cleanup() {
    rm -rf -- "$TEMPORARY_ROOT"
}
trap cleanup EXIT HUP INT TERM

cd -- "$REPOSITORY_ROOT" || fail

run_sre() {
    output=$1
    diagnostic=$2
    cargo run --quiet --locked --offline -p decant-cli -- demo sre >"$output" 2>"$diagnostic" || fail
    test ! -s "$diagnostic" || fail
}

run_otlp() {
    output=$1
    diagnostic=$2
    cargo run --quiet --locked --offline -p decant-cli -- \
        ingest otlp-jsonl --file examples/opentelemetry-demo/traces-v0.jsonl \
        >"$output" 2>"$diagnostic" || fail
    test ! -s "$diagnostic" || fail
}

run_sre "$TEMPORARY_ROOT/sre-first.json" "$TEMPORARY_ROOT/sre-first.err"
run_sre "$TEMPORARY_ROOT/sre-second.json" "$TEMPORARY_ROOT/sre-second.err"
cmp -s "$TEMPORARY_ROOT/sre-first.json" "$TEMPORARY_ROOT/sre-second.json" || fail
cmp -s "$TEMPORARY_ROOT/sre-first.json" crates/decant-cli/tests/golden/sre-demo-v0.json || fail

run_otlp "$TEMPORARY_ROOT/otlp-first.json" "$TEMPORARY_ROOT/otlp-first.err"
run_otlp "$TEMPORARY_ROOT/otlp-second.json" "$TEMPORARY_ROOT/otlp-second.err"
cmp -s "$TEMPORARY_ROOT/otlp-first.json" "$TEMPORARY_ROOT/otlp-second.json" || fail
cmp -s "$TEMPORARY_ROOT/otlp-first.json" crates/decant-cli/tests/golden/otlp-jsonl-run-v0.json || fail

python3 tools/validate_local_smoke_v0.py \
    --sre "$TEMPORARY_ROOT/sre-first.json" \
    --otlp "$TEMPORARY_ROOT/otlp-first.json" \
    --otlp-input examples/opentelemetry-demo/traces-v0.jsonl || fail

printf '%s\n' 'PASS: deterministic local SRE and OTLP JSONL smoke'
