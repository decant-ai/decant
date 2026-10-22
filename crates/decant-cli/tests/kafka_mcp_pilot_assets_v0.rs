const SPEC: &str = include_str!("../../../release/kafka-mcp-pilot-benchmark-spec-v0.json");
const RUNNER: &str = include_str!("../../../examples/kafka-mcp/run-pilot-benchmark-v0.py");
const VALIDATOR: &str = include_str!("../../../tools/check_kafka_mcp_pilot_evidence_v0.py");
const GATE: &str = include_str!("../../../tools/run_kafka_mcp_pilot_gate_v0.py");
const WRAPPER: &str = include_str!("../../../scripts/kafka-mcp-pilot-gate-v0.sh");
const EVIDENCE_EXAMPLE: &str =
    include_str!("../../../release/kafka-mcp-pilot-evidence-v0.example.json");
const CHECKLIST: &str = include_str!("../../../docs/release/PRODUCTION_PILOT_CHECKLIST.md");

#[test]
fn benchmark_spec_is_fixed_complete_and_contains_no_result() {
    let document: serde_json::Value = serde_json::from_str(SPEC).unwrap();
    assert_eq!(document["format"], "decant-kafka-mcp-pilot-benchmark-spec");
    assert_eq!(document["result_status"], "not_measured");
    assert_eq!(document["workload"]["broker_image"], "apache/kafka:4.3.1");
    assert_eq!(document["workload"]["run_count"], 3);
    assert_eq!(document["workload"]["records_per_run"], 10_000);
    assert_eq!(document["workload"]["mcp_latency_samples_per_run"], 30);
    assert_eq!(document["baselines"].as_array().unwrap().len(), 4);
    assert_eq!(
        document["required_measurements"].as_array().unwrap().len(),
        21
    );
    assert_eq!(document["non_claims"].as_array().unwrap().len(), 4);
    assert!(document.get("results").is_none());
    assert!(document.get("throughput").is_none());
}

#[test]
fn benchmark_runner_is_opt_in_commit_bound_and_measures_the_product_path() {
    for required in [
        "OPT_IN = \"DECANT_RUN_KAFKA_MCP_BENCHMARK_V0\"",
        "--release",
        "--locked",
        "--offline",
        "git\", \"diff\", \"--quiet",
        "git\", \"rev-parse\", \"HEAD",
        "127.0.0.1:9092:9092",
        "ai.decant.kafka-mcp-benchmark",
        "for ordinal in range(workload[\"run_count\"])",
        "produce_benchmark",
        "workload[\"seed\"]",
        "helper.wait_for_checkpoint",
        "helper.request_context",
        "expected_transitions",
        "protected_output_matches",
        "restart_context_equal",
        "context_reduction_ppm_by_baseline",
        "benchmarks/results",
        "measured_local_not_product_claim",
        "raw_values_persisted_in_result\": False",
    ] {
        assert!(
            RUNNER.contains(required),
            "missing benchmark runner contract: {required}"
        );
    }
    let lower = RUNNER.to_ascii_lowercase();
    for forbidden in [
        ":latest",
        "kubernetes",
        "kubectl",
        "requests.",
        "curl ",
        "wget ",
        "cargo publish",
        "git push",
        "git tag",
        "docker system prune",
    ] {
        assert!(
            !lower.contains(forbidden),
            "forbidden benchmark surface: {forbidden}"
        );
    }
}

#[test]
fn validator_recomputes_gates_and_binds_external_approval() {
    for required in [
        "MAX_BENCHMARK_BYTES = 256 * 1024",
        "MAX_EVIDENCE_BYTES = 64 * 1024",
        "benchmark-json\", canonical=True",
        "evidence-json\", canonical=True",
        "hashlib.sha256(spec_bytes).hexdigest()",
        "hashlib.sha256(benchmark_bytes).hexdigest()",
        "throughput_range_ppm",
        "context_reduction_ppm_by_baseline",
        "benchmark-context-repeatability",
        "benchmark-protected-value",
        "APPROVALS",
        "evidence-incomplete",
    ] {
        assert!(
            VALIDATOR.contains(required),
            "missing validator contract: {required}"
        );
    }
    let example: serde_json::Value = serde_json::from_str(EVIDENCE_EXAMPLE).unwrap();
    assert_eq!(example["format"], "decant-kafka-mcp-pilot-evidence");
    assert_eq!(example["approvals"].as_object().unwrap().len(), 12);
    assert!(
        example["approvals"]
            .as_object()
            .unwrap()
            .values()
            .all(|value| value == false)
    );
}

#[test]
fn pilot_gate_is_non_publishing_and_separates_engineering_from_external_evidence() {
    for required in [
        "choices=(\"engineering\", \"production-pilot\")",
        "missing-benchmark-result",
        "missing-pilot-evidence",
        "tracked-changes",
        "./scripts/check-v0.sh",
        "external evidence not evaluated",
        "no public-release, exactly-once, privacy, security, or general production-readiness conclusion",
    ] {
        assert!(
            GATE.contains(required),
            "missing pilot gate contract: {required}"
        );
    }
    assert!(WRAPPER.contains("--engineering"));
    assert!(WRAPPER.contains("--production-pilot"));
    assert!(WRAPPER.contains("set -eu"));
    for forbidden in ["docker", "git push", "git tag", "deploy", "cargo publish"] {
        assert!(!GATE.to_ascii_lowercase().contains(forbidden));
        assert!(!WRAPPER.to_ascii_lowercase().contains(forbidden));
    }
}

#[test]
fn checklist_requires_supervision_recovery_and_partner_acceptance_without_overclaiming() {
    for required in [
        "one bounded, supervised design-partner pilot",
        "single-node, single-writer",
        "no-broker-commit behavior",
        "verified TLS with SCRAM",
        "external process supervisor",
        "restart throttling",
        "access-controlled bounded sink",
        "design partner accepts",
        "release/kafka-mcp-pilot-evidence-v0.json",
        "not a license decision",
        "general production-readiness claim",
    ] {
        assert!(
            CHECKLIST.contains(required),
            "missing pilot checklist boundary: {required}"
        );
    }
}
