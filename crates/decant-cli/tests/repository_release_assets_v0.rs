use std::fs;
use std::path::Path;

const ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../..");

fn text(path: &str) -> String {
    fs::read_to_string(Path::new(ROOT).join(path)).unwrap()
}

#[test]
fn local_smoke_is_finite_offline_and_uses_public_cli_paths() {
    let script = text("scripts/smoke-local-v0.sh");
    for required in [
        "cargo run --quiet --locked --offline -p decant-cli -- demo sre",
        "ingest otlp-jsonl --file examples/opentelemetry-demo/traces-v0.jsonl",
        "crates/decant-cli/tests/golden/sre-demo-v0.json",
        "crates/decant-cli/tests/golden/otlp-jsonl-run-v0.json",
        "tools/validate_local_smoke_v0.py",
        "mktemp -d",
        "trap cleanup",
    ] {
        assert!(script.contains(required), "missing local smoke contract");
    }
    for forbidden in [
        "docker",
        "curl",
        "wget",
        "kafka-json --config",
        "time ",
        "hyperfine",
    ] {
        assert!(
            !script.contains(forbidden),
            "unexpected local smoke behavior"
        );
    }
}

#[test]
fn local_smoke_validator_freezes_bounds_and_protected_sentinels() {
    let validator = text("tools/validate_local_smoke_v0.py");
    for required in [
        "MAX_DOCUMENT_BYTES = 128 * 1024",
        "PROTECTED_OTLP_SENTINELS",
        "decant-sre-demo",
        "decant-otlp-jsonl-run",
        "sha256:v1:",
        "local smoke validation: failed",
    ] {
        assert!(validator.contains(required), "missing validator contract");
    }
}

#[test]
fn repository_gate_is_offline_fail_fast_and_keeps_real_kafka_opt_in() {
    let script = text("scripts/check-v0.sh");
    let ordered = [
        "cargo fmt --all -- --check",
        "cargo clippy --workspace --all-targets --locked --offline -- -D warnings",
        "cargo test --workspace --all-targets --locked --offline",
        "cargo test --workspace --doc --locked --offline",
        "git ls-files -- '*.sh'",
        "python3 -m unittest discover -s tools/tests -p 'test_*.py'",
        "python3 tools/check_dependency_inventory_v0.py",
        "python3 tools/check_repository_hygiene_v0.py --root . --mode internal",
        "./scripts/smoke-local-v0.sh",
        "./scripts/benchmark-smoke-v0.sh",
    ];
    let mut previous = 0;
    for required in ordered {
        let offset = script.find(required).expect("missing quality-gate command");
        assert!(offset >= previous, "quality-gate command order changed");
        previous = offset;
    }
    for forbidden in [
        "cargo fetch",
        "cargo install",
        "apt-get",
        "brew ",
        "docker",
        "run-apache-kafka-v0.sh",
        "curl",
        "wget",
    ] {
        assert!(
            !script.contains(forbidden),
            "unexpected quality-gate behavior"
        );
    }
    assert!(script.contains("test \"$shell_count\" -le 64"));
    assert!(script.contains("set -eu"));
}

#[test]
fn public_export_is_allowlisted_tracked_only_and_license_gated() {
    let allowlist = text("release/public-source-allowlist-v0.tsv");
    for required in [
        "file\tREADME.md",
        "file\tSECURITY.md",
        "tree\tcrates/decant-cli/",
        "file\tcrates/decant-state/Cargo.toml",
        "tree\tcrates/decant-state/src/",
        "tree\tcrates/decant-state/tests/",
        "file\tdocs/release/EVIDENCE_QUICKSTART.md",
        "file\tdocs/release/KNOWN_LIMITATIONS.md",
        "tree\texamples/",
        "file\trelease/third-party-dependencies-v0.tsv",
        "tree\tscripts/",
        "file\ttools/export_public_source_v0.py",
        "tree\ttools/tests/",
    ] {
        assert!(allowlist.contains(required));
    }
    for forbidden in [
        ".agents/",
        ".idea/",
        "docs/source/",
        "docs/reviews/",
        "AGENTS.override.md",
        "SOURCE_ONLY_PUBLICATION.md",
        "ALPHA_ACTIVATION.md",
        "RELEASE_CHECKLIST.md",
        "tree\tcrates/\n",
        "tree\ttools/\n",
        "tree\tdocs/",
        "tree\trelease/\n",
    ] {
        assert!(!allowlist.contains(forbidden));
    }
    let exporter = text("tools/export_public_source_v0.py");
    for required in [
        "MAX_TRACKED_FILES = 4096",
        "MAX_EXPORTED_FILES = 2048",
        "MAX_TOTAL_BYTES = 32 * 1024 * 1024",
        "git\", \"ls-files\", \"-z",
        "LICENSE-MISSING.txt",
        "public-release-approval-v0.json",
        "follow_symlinks=False",
    ] {
        assert!(exporter.contains(required));
    }
}

#[test]
fn source_artifact_is_reproducible_bounded_unsigned_and_source_only() {
    let builder = text("tools/build_source_artifact_v0.py");
    for required in [
        "gzip.GzipFile",
        "mtime=0",
        "tarfile.USTAR_FORMAT",
        "MAX_SOURCE_FILES = 2048",
        "MAX_SOURCE_BYTES = 32 * 1024 * 1024",
        "MAX_ARCHIVE_BYTES = 64 * 1024 * 1024",
        "\"signed\": False",
        "source_inventory_sha256",
    ] {
        assert!(builder.contains(required));
    }
    for forbidden in ["upload", "twine", "sigstore", "gpg", "cosign"] {
        assert!(!builder.contains(forbidden));
    }
    let wrapper = text("scripts/build-source-artifact-v0.sh");
    assert!(wrapper.contains("--engineering-preview"));
    assert!(wrapper.contains("--public"));
    assert!(wrapper.contains("set -eu"));
}

#[test]
fn github_ci_is_exact_pinned_read_only_and_non_publishing() {
    let workflow = text(".github/workflows/ci.yml");
    for required in [
        "pull_request:",
        "push:\n    branches:\n      - main",
        "permissions:\n  contents: read",
        "cancel-in-progress: true",
        "runs-on: ubuntu-24.04",
        "timeout-minutes: 30",
        "actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd # v5.0.1",
        "persist-credentials: false",
        "cargo fetch --locked",
        "./scripts/check-v0.sh",
        "python3 tools/build_binary_candidate_v1.py --output .local/ci-candidate",
        "python3 tools/verify_binary_candidate_v1.py .local/ci-candidate",
    ] {
        assert!(workflow.contains(required));
    }
    for forbidden in [
        "pull_request_target",
        "contents: write",
        "secrets.",
        "upload-artifact",
        "actions/cache",
        "docker",
    ] {
        assert!(!workflow.contains(forbidden));
    }
    let validator = text("tools/check_github_workflow_v0.py");
    assert!(validator.contains("MAX_WORKFLOW_BYTES = 16 * 1024"));
    assert!(validator.contains("content != EXPECTED_WORKFLOW"));
}

#[test]
fn release_gate_separates_engineering_evidence_from_public_authority() {
    let gate = text("tools/run_release_gate_v0.py");
    for required in [
        "missing-license",
        "missing-approval",
        "tracked-changes",
        "./scripts/check-v0.sh",
        "--public-export",
        "artifact-reproducibility",
        "extracted-metadata",
        "no performance privacy production legal signing or publication conclusion",
    ] {
        assert!(gate.contains(required));
    }
    for forbidden in [
        "git push",
        "git tag",
        "gh release",
        "upload",
        "cargo publish",
    ] {
        assert!(!gate.contains(forbidden));
    }
    let approval = text("tools/check_public_release_approval_v0.py");
    assert!(approval.contains("MAX_APPROVAL_BYTES = 64 * 1024"));
    assert!(approval.contains("REQUIRED_APPROVALS"));
    assert!(approval.contains("license_sha256"));
    let rollback = text("docs/release/ROLLBACK.md");
    assert!(rollback.contains("strict-latest"));
    assert!(rollback.contains("cannot be recalled"));
    assert!(rollback.contains("non-reversible"));
}

#[test]
fn benchmark_smoke_is_correctness_only_and_forbids_measurements() {
    let manifest = text("release/benchmark-smoke-sre-v0.json");
    for required in [
        "\"result_status\": \"not_measured\"",
        "last-n-raw-events",
        "time-window-field-filter",
        "rag-recent-event-text",
        "hand-built-aggregation",
        "run_count_variance",
        "156335465e1de162f580d74bbb994daf658b16f46c17cdd757c912133f12eeb3",
    ] {
        assert!(manifest.contains(required));
    }
    let script = text("scripts/benchmark-smoke-v0.sh");
    assert!(script.contains("--release --locked --offline"));
    assert!(script.contains("no measurements"));
    for forbidden in ["time ", "hyperfine", "perf ", "criterion", "curl", "docker"] {
        assert!(!script.contains(forbidden));
    }
}

#[test]
fn repository_hygiene_is_bounded_redacted_and_not_a_security_claim() {
    let validator = text("tools/check_repository_hygiene_v0.py");
    for required in [
        "MAX_FILES = 4096",
        "MAX_FILE_BYTES = 1024 * 1024",
        "MAX_TOTAL_BYTES = 64 * 1024 * 1024",
        "FORBIDDEN_PUBLIC_PREFIXES",
        "PROTECTED_SENTINELS",
        "repository hygiene: failed:",
    ] {
        assert!(validator.contains(required));
    }
    let policy = text("SECURITY.md");
    for required in [
        "not a full-history secret scanner",
        "does not claim exactly-once delivery",
        "Do not include event data, credentials",
        "no stable or production-supported release",
    ] {
        assert!(policy.contains(required));
    }
}

#[test]
fn dependency_inventory_is_exact_bounded_and_non_legal() {
    let allowlist = text("release/third-party-dependencies-v0.tsv");
    assert_eq!(allowlist.lines().count(), 168);
    assert!(allowlist.lines().all(|line| line.split('\t').count() == 4));
    let validator = text("tools/check_dependency_inventory_v0.py");
    for required in [
        "MAX_INPUT_BYTES = 1024 * 1024",
        "MAX_PACKAGES = 256",
        "cargo\", \"metadata\", \"--locked\", \"--offline",
        "dependency inventory: failed",
    ] {
        assert!(validator.contains(required));
    }
    for forbidden in ["license compatibility", "cargo install", "curl", "requests"] {
        assert!(!validator.contains(forbidden));
    }
}
