const HARNESS: &str = include_str!("../../../examples/kafka-mcp/run-pilot-fault-matrix-v0.py");
const GUIDE: &str = include_str!("../../../examples/kafka-mcp/README.md");

#[test]
fn fault_matrix_is_opt_in_pinned_bounded_and_label_scoped() {
    for required in [
        "IMAGE = \"apache/kafka:4.3.1\"",
        "OPT_IN = \"DECANT_RUN_KAFKA_MCP_FAULT_MATRIX_V0\"",
        "MAX_MCP_LINE_BYTES = 128 * 1024",
        "MAX_RSS_KIB = 512 * 1024",
        "SERVICE_WAIT_SECONDS = 45",
        "TOTAL_RECORDS = 2_000",
        "CHECKPOINT_CADENCE = 50",
        "--locked",
        "--offline",
        "127.0.0.1:9092:9092",
        "ai.decant.kafka-mcp-fault",
        "\"inspect\",",
        "inspected.stdout.decode().strip() == run_id",
        "checkpoint-*.dcp",
        "len(list(checkpoint_directory.glob(\"checkpoint-*.dcp\"))) != 3",
        "MAX_RSS_KIB",
        "OBSERVED-NOT-A-BENCHMARK",
    ] {
        assert!(
            HARNESS.contains(required),
            "missing fault-matrix contract: {required}"
        );
    }

    for forbidden in [
        ":latest",
        "kubectl",
        "kubernetes",
        "curl ",
        "wget ",
        "docker system prune",
        "docker rm -f $(docker",
    ] {
        assert!(
            !HARNESS.to_ascii_lowercase().contains(forbidden),
            "forbidden fault-matrix surface: {forbidden}"
        );
    }
}

#[test]
fn fault_matrix_covers_the_approved_recovery_and_fail_closed_cases() {
    for required in [
        "signal.SIGKILL",
        "hard-kill-recovery-context",
        "request_context(active, 2, read=False)",
        "slow_response = read_message(active)",
        "docker(\"stop\", container",
        "broker-loss-must-fail-closed",
        "broker-loss-diagnostic",
        "docker(\"start\", container",
        "broker-restart-context",
        "fault-matrix-corrupt-checkpoint",
        "corrupt-checkpoint-fail-closed",
        "protected-value-in-mcp",
        "value-safe-output-scan",
        "source_ownership",
        "broker_offsets_committed",
    ] {
        assert!(
            HARNESS.contains(required),
            "missing fault-matrix scenario: {required}"
        );
    }
}

#[test]
fn guide_separates_opt_in_evidence_from_product_claims() {
    for required in [
        "Opt-in pilot fault matrix",
        "excluded from ordinary offline tests",
        "last verified cut",
        "supervisor-style restart",
        "512 MiB sampled-RSS engineering ceiling",
        "2,000 records",
        "does not establish a Kafka compatibility matrix",
        "graceful signal handling",
        "performance result",
        "production readiness",
        "explicitly not a benchmark",
        "Denied values remain in local state/checkpoints by design",
    ] {
        assert!(GUIDE.contains(required), "missing guide caveat: {required}");
    }
}
