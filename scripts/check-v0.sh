#!/bin/sh
set -eu

fail() {
    printf '%s\n' 'repository check: failed' >&2
    exit 1
}

case "$#" in
    0) hygiene_mode=internal ;;
    1)
        test "$1" = "--public-export" || fail
        hygiene_mode=public-export
        ;;
    *) fail ;;
esac

for tool in cargo find python3 sh sort; do
    command -v "$tool" >/dev/null 2>&1 || fail
done
if test "$hygiene_mode" = internal; then
    command -v git >/dev/null 2>&1 || fail
fi

SCRIPT_DIRECTORY=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd) || fail
REPOSITORY_ROOT=$(CDPATH= cd -- "$SCRIPT_DIRECTORY/.." && pwd) || fail
cd -- "$REPOSITORY_ROOT" || fail

if test "$hygiene_mode" = public-export; then
    python3 tools/check_repository_hygiene_v0.py --root . --mode public-export || fail
fi

cargo fmt --all -- --check || fail
cargo clippy --workspace --all-targets --locked --offline -- -D warnings || fail
cargo test --workspace --all-targets --locked --offline || fail
cargo test --workspace --doc --locked --offline || fail

shell_count=0
if test "$hygiene_mode" = internal; then
    shell_files=$(git ls-files -- '*.sh') || fail
else
    shell_files=$(find scripts examples -type f -name '*.sh' -print | LC_ALL=C sort) || fail
fi
printf '%s\n' "$shell_files" | while IFS= read -r shell_file; do
    shell_count=$((shell_count + 1))
    test "$shell_count" -le 64 || fail
    test -n "$shell_file" || fail
    sh -n "$shell_file" || fail
done

python3 -m unittest discover -s tools/tests -p 'test_*.py' || fail
python3 tools/check_github_workflow_v0.py || fail
python3 tools/check_dependency_inventory_v0.py || fail
python3 tools/check_project_licensing_v1.py || fail
python3 tools/check_kafka_mcp_pilot_evidence_v0.py --root . --spec-only || fail
python3 tools/evaluate_evidence_v1.py spec || fail
python3 tools/evaluate_evidence_v1.py correctness || fail
if test "$hygiene_mode" = internal; then
    python3 tools/check_repository_hygiene_v0.py --root . --mode internal || fail
fi
./scripts/smoke-local-v0.sh || fail
./scripts/benchmark-smoke-v0.sh || fail

printf '%s\n' 'PASS: repository quality gate v0'
