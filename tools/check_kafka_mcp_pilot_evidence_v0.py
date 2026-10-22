#!/usr/bin/env python3
"""Validate commit-bound Kafka-MCP benchmark and production-pilot evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sys
from pathlib import Path
from typing import Any

RELEASE_VERSION = "0.1.0-alpha.1"
SPEC_PATH = Path("release/kafka-mcp-pilot-benchmark-spec-v0.json")
BENCHMARK_PATH = Path("benchmarks/results/kafka-mcp-pilot-v0.json")
EVIDENCE_PATH = Path("release/kafka-mcp-pilot-evidence-v0.json")
MAX_SPEC_BYTES = 128 * 1024
MAX_BENCHMARK_BYTES = 256 * 1024
MAX_EVIDENCE_BYTES = 64 * 1024
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
DIGEST_PATTERN = re.compile(r"[0-9a-f]{64}")
BASELINES = {
    "hand-built-latest-transition-aggregation",
    "last-60s-allowlisted-events",
    "last-64-raw-events",
    "rag-recent-event-text",
}
APPROVALS = {
    "authorized_data_and_broker_access_confirmed",
    "benchmark_reviewed_on_candidate",
    "broker_identity_and_static_partitions_reviewed",
    "checkpoint_backup_capacity_and_permissions_reviewed",
    "design_partner_acceptance_criteria_met",
    "egress_mapping_and_schema_reviewed",
    "fault_matrix_passed_on_candidate",
    "founder_production_pilot_approved",
    "restart_and_rollback_rehearsed",
    "single_node_and_no_broker_commits_accepted",
    "supervision_and_alerting_configured",
    "tls_scram_or_trusted_plaintext_reviewed",
}
KNOWN_LIMITATIONS = [
    "single local broker, one partition, static ownership, and no broker offset commits",
    "producer-to-verified-checkpoint throughput includes the bundled console producer",
    "MCP latency measures a local stdio request after ingestion, not source-to-agent latency",
    "RSS is sampled and may miss a transient process peak",
    "baseline comparisons cover serialized bytes, not equivalent implementation throughput",
    "results do not establish exactly-once delivery, power-loss durability, security, privacy, or general production readiness",
]


class PilotEvidenceFailure(Exception):
    """A value-safe pilot evidence failure."""

    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


def require(condition: bool, category: str) -> None:
    if not condition:
        raise PilotEvidenceFailure(category)


def bounded_regular_file(path: Path, maximum: int, category: str) -> bytes:
    try:
        metadata = path.lstat()
        require(path.is_file() and not path.is_symlink(), category)
        require(0 < metadata.st_size <= maximum, category)
        return path.read_bytes()
    except PilotEvidenceFailure:
        raise
    except OSError:
        raise PilotEvidenceFailure(category) from None


def decode_document(data: bytes, category: str, *, canonical: bool) -> dict[str, Any]:
    try:
        document = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise PilotEvidenceFailure(category) from None
    require(isinstance(document, dict), category)
    if canonical:
        expected = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
        require(data == expected, f"{category}-canonical")
    return document


def validated_spec(root: Path) -> tuple[dict[str, Any], bytes]:
    data = bounded_regular_file(root / SPEC_PATH, MAX_SPEC_BYTES, "spec")
    document = decode_document(data, "spec-json", canonical=False)
    require(
        set(document)
        == {
            "baselines",
            "format",
            "non_claims",
            "pilot_engineering_gates",
            "required_measurements",
            "result_status",
            "version",
            "workload",
        },
        "spec-shape",
    )
    require(document["format"] == "decant-kafka-mcp-pilot-benchmark-spec", "spec-format")
    require(document["version"] == 0 and document["version"] is not False, "spec-version")
    require(document["result_status"] == "not_measured", "spec-status")
    workload = document["workload"]
    require(
        workload
        == {
            "seed": 42,
            "broker_image": "apache/kafka:4.3.1",
            "partition_count": 1,
            "run_count": 3,
            "records_per_run": 10_000,
            "checkpoint_cadence_records": 250,
            "retention_ns": 60_000_000_000,
            "context_item_limit": 64,
            "context_byte_budget": 16_384,
            "mcp_latency_samples_per_run": 30,
        },
        "spec-workload",
    )
    require(set(document["baselines"]) == BASELINES and len(document["baselines"]) == 4, "spec-baselines")
    gates = document["pilot_engineering_gates"]
    require(
        gates
        == {
            "minimum_throughput_events_per_second_milli": 100_000,
            "maximum_mcp_p99_us": 5_000_000,
            "maximum_sampled_peak_rss_kib": 524_288,
            "maximum_throughput_range_ppm": 500_000,
            "maximum_context_bytes": 16_384,
            "required_task_fidelity_ppm": 1_000_000,
            "maximum_protected_output_matches": 0,
            "required_restart_context_equal": True,
        },
        "spec-gates",
    )
    require(len(document["required_measurements"]) == 21, "spec-measurements")
    require(len(document["non_claims"]) == 4, "spec-non-claims")
    return document, data


def nearest_rank(values: list[int], percentile: int) -> int:
    return sorted(values)[math.ceil(len(values) * percentile / 100) - 1]


def validate_benchmark(root: Path, expected_commit: str) -> bytes:
    require(COMMIT_PATTERN.fullmatch(expected_commit) is not None, "expected-commit")
    spec, spec_bytes = validated_spec(root)
    data = bounded_regular_file(root / BENCHMARK_PATH, MAX_BENCHMARK_BYTES, "benchmark")
    require(b"fault-matrix-protected-sentinel-v0" not in data, "benchmark-protected-value")
    document = decode_document(data, "benchmark-json", canonical=True)
    require(
        set(document)
        == {
            "baselines",
            "benchmark_spec_sha256",
            "configuration",
            "dataset",
            "environment",
            "format",
            "known_limitations",
            "raw_artifacts",
            "result_status",
            "runs",
            "source_commit",
            "summary",
            "version",
            "warmup",
            "workload",
        },
        "benchmark-shape",
    )
    require(document["format"] == "decant-kafka-mcp-pilot-benchmark-result", "benchmark-format")
    require(document["version"] == 0 and document["version"] is not False, "benchmark-version")
    require(document["result_status"] == "measured_local_not_product_claim", "benchmark-status")
    require(document["source_commit"] == expected_commit, "benchmark-commit")
    require(document["benchmark_spec_sha256"] == hashlib.sha256(spec_bytes).hexdigest(), "benchmark-spec")
    require(document["workload"] == spec["workload"], "benchmark-workload")
    require(
        document["configuration"]
        == {
            "security": "trusted-local-plaintext-benchmark-only",
            "source_ownership": "static_partitions",
            "broker_offsets_committed": False,
            "mcp_transport": "local_stdio",
        },
        "benchmark-configuration",
    )
    environment = document["environment"]
    require(
        isinstance(environment, dict)
        and set(environment)
        == {
            "build_profile",
            "cargo_lock_sha256",
            "cargo_version",
            "filesystem_free_bytes",
            "logical_cpu_count",
            "os_kernel_architecture",
            "rust_version",
            "total_memory_bytes",
        },
        "benchmark-environment",
    )
    require(environment["build_profile"] == "release", "benchmark-profile")
    require(DIGEST_PATTERN.fullmatch(environment["cargo_lock_sha256"]) is not None, "benchmark-lock")
    for field in ["cargo_version", "os_kernel_architecture", "rust_version"]:
        require(isinstance(environment[field], str) and 0 < len(environment[field]) <= 256, "benchmark-environment-value")
    require(isinstance(environment["logical_cpu_count"], int) and environment["logical_cpu_count"] > 0, "benchmark-cpu")
    require(environment["total_memory_bytes"] is None or (isinstance(environment["total_memory_bytes"], int) and environment["total_memory_bytes"] > 0), "benchmark-memory")
    require(isinstance(environment["filesystem_free_bytes"], int) and environment["filesystem_free_bytes"] > 0, "benchmark-filesystem")

    dataset = document["dataset"]
    require(isinstance(dataset, dict) and set(dataset) == {"entity_key_cardinality", "event_size_bytes_maximum", "event_size_bytes_minimum", "format", "seed", "serialized_bytes", "sha256"}, "benchmark-dataset")
    require(dataset["format"] == "synthetic.kafka.mcp.benchmark.v0" and dataset["seed"] == 42, "benchmark-dataset-contract")
    require(dataset["entity_key_cardinality"] == 8, "benchmark-cardinality")
    require(0 < dataset["event_size_bytes_minimum"] <= dataset["event_size_bytes_maximum"] <= 65_536, "benchmark-event-size")
    require(dataset["serialized_bytes"] > 0 and DIGEST_PATTERN.fullmatch(dataset["sha256"]) is not None, "benchmark-dataset-bytes")
    require(document["warmup"] == {"duration_ns": 0, "reason": "each run measures cold service and topic"}, "benchmark-warmup")

    baselines = document["baselines"]
    require(isinstance(baselines, dict) and set(baselines) == BASELINES, "benchmark-baselines")
    baseline_sizes: dict[str, int] = {}
    for name, baseline in baselines.items():
        require(isinstance(baseline, dict) and set(baseline) == {"serialized_bytes"}, "benchmark-baseline-shape")
        size = baseline["serialized_bytes"]
        require(isinstance(size, int) and not isinstance(size, bool) and size > 0, "benchmark-baseline-size")
        baseline_sizes[name] = size

    runs = document["runs"]
    workload = spec["workload"]
    gates = spec["pilot_engineering_gates"]
    require(isinstance(runs, list) and len(runs) == workload["run_count"], "benchmark-runs")
    throughput_values: list[int] = []
    context_sizes: list[int] = []
    context_hashes: set[str] = set()
    diagnostic_hashes: list[str] = []
    for ordinal, run in enumerate(runs):
        require(
            isinstance(run, dict)
            and set(run)
            == {
                "candidate_count",
                "context_bytes",
                "context_sha256",
                "diagnostic_sha256",
                "included_count",
                "mcp_added_latency_us",
                "omitted_count",
                "protected_output_matches",
                "restart_context_equal",
                "retained_checkpoint_generations",
                "run_duration_ns",
                "run_ordinal",
                "sampled_peak_rss_kib",
                "task_fidelity_ppm",
                "throughput_events_per_second_milli",
            },
            "benchmark-run-shape",
        )
        require(run["run_ordinal"] == ordinal and run["run_duration_ns"] > 0, "benchmark-run-order")
        throughput = run["throughput_events_per_second_milli"]
        require(isinstance(throughput, int) and throughput >= gates["minimum_throughput_events_per_second_milli"], "benchmark-throughput")
        latency = run["mcp_added_latency_us"]
        require(isinstance(latency, dict) and set(latency) == {"p50", "p95", "p99", "sample_count"}, "benchmark-latency")
        require(latency["sample_count"] == workload["mcp_latency_samples_per_run"], "benchmark-latency-count")
        require(0 <= latency["p50"] <= latency["p95"] <= latency["p99"] <= gates["maximum_mcp_p99_us"], "benchmark-latency-order")
        require(0 < run["sampled_peak_rss_kib"] <= gates["maximum_sampled_peak_rss_kib"], "benchmark-rss")
        require(0 < run["context_bytes"] <= gates["maximum_context_bytes"], "benchmark-context-bytes")
        require(DIGEST_PATTERN.fullmatch(run["context_sha256"]) is not None, "benchmark-context-hash")
        require(DIGEST_PATTERN.fullmatch(run["diagnostic_sha256"]) is not None, "benchmark-diagnostic-hash")
        require((run["candidate_count"], run["included_count"], run["omitted_count"]) == (8, 8, 0), "benchmark-context-counts")
        require(run["task_fidelity_ppm"] == gates["required_task_fidelity_ppm"], "benchmark-fidelity")
        require(run["protected_output_matches"] == gates["maximum_protected_output_matches"], "benchmark-leakage")
        require(run["restart_context_equal"] is gates["required_restart_context_equal"], "benchmark-restart")
        require(run["retained_checkpoint_generations"] == 3, "benchmark-pruning")
        throughput_values.append(throughput)
        context_sizes.append(run["context_bytes"])
        context_hashes.add(run["context_sha256"])
        diagnostic_hashes.append(run["diagnostic_sha256"])
    require(len(context_hashes) == 1, "benchmark-context-repeatability")

    summary = document["summary"]
    require(isinstance(summary, dict) and set(summary) == {"context_bytes_maximum", "context_reduction_ppm_by_baseline", "mcp_p99_us_maximum", "run_count", "sampled_peak_rss_kib_maximum", "throughput_median_events_per_second_milli", "throughput_range_ppm"}, "benchmark-summary")
    median = nearest_rank(throughput_values, 50)
    spread = (max(throughput_values) - min(throughput_values)) * 1_000_000 // median
    require(summary["run_count"] == len(runs), "benchmark-summary-count")
    require(summary["throughput_median_events_per_second_milli"] == median, "benchmark-summary-throughput")
    require(summary["throughput_range_ppm"] == spread <= gates["maximum_throughput_range_ppm"], "benchmark-variance")
    require(summary["mcp_p99_us_maximum"] == max(run["mcp_added_latency_us"]["p99"] for run in runs), "benchmark-summary-latency")
    require(summary["sampled_peak_rss_kib_maximum"] == max(run["sampled_peak_rss_kib"] for run in runs), "benchmark-summary-rss")
    require(summary["context_bytes_maximum"] == max(context_sizes), "benchmark-summary-context")
    expected_reductions = {name: (size - context_sizes[0]) * 1_000_000 // size for name, size in baseline_sizes.items()}
    require(summary["context_reduction_ppm_by_baseline"] == expected_reductions, "benchmark-reduction")

    raw = document["raw_artifacts"]
    require(isinstance(raw, dict) and set(raw) == {"input_dataset_sha256", "per_run_diagnostic_sha256", "raw_values_persisted_in_result"}, "benchmark-artifacts")
    require(raw["input_dataset_sha256"] == dataset["sha256"], "benchmark-artifact-input")
    require(raw["per_run_diagnostic_sha256"] == diagnostic_hashes, "benchmark-artifact-diagnostics")
    require(raw["raw_values_persisted_in_result"] is False, "benchmark-artifact-values")
    require(document["known_limitations"] == KNOWN_LIMITATIONS, "benchmark-limitations")
    return data


def validate_evidence(root: Path, expected_commit: str, benchmark_bytes: bytes) -> None:
    data = bounded_regular_file(root / EVIDENCE_PATH, MAX_EVIDENCE_BYTES, "evidence")
    document = decode_document(data, "evidence-json", canonical=True)
    require(set(document) == {"approved_source_commit", "approvals", "benchmark_result_sha256", "format", "release_version", "version"}, "evidence-shape")
    require(document["format"] == "decant-kafka-mcp-pilot-evidence", "evidence-format")
    require(document["version"] == 0 and document["version"] is not False, "evidence-version")
    require(document["release_version"] == RELEASE_VERSION, "evidence-release")
    require(document["approved_source_commit"] == expected_commit, "evidence-commit")
    require(document["benchmark_result_sha256"] == hashlib.sha256(benchmark_bytes).hexdigest(), "evidence-benchmark")
    approvals = document["approvals"]
    require(isinstance(approvals, dict) and set(approvals) == APPROVALS, "evidence-approvals")
    require(all(value is True for value in approvals.values()), "evidence-incomplete")


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--expected-source-commit")
    parser.add_argument("--spec-only", action="store_true")
    try:
        arguments = parser.parse_args()
        root = arguments.root.resolve(strict=True)
        require(root.is_dir() and not arguments.root.is_symlink(), "root")
        if arguments.spec_only:
            require(arguments.expected_source_commit is None, "arguments")
            validated_spec(root)
            print("PASS: Kafka-MCP pilot benchmark specification v0 (not measured)")
            return 0
        require(arguments.expected_source_commit is not None, "arguments")
        benchmark = validate_benchmark(root, arguments.expected_source_commit)
        validate_evidence(root, arguments.expected_source_commit, benchmark)
    except (SystemExit, OSError, KeyError, TypeError, ValueError, PilotEvidenceFailure) as error:
        category = error.category if isinstance(error, PilotEvidenceFailure) else "arguments"
        print(f"Kafka-MCP pilot evidence: failed: {category}", file=sys.stderr)
        return 1
    print("PASS: commit-bound Kafka-MCP production-pilot evidence v0")
    print("LIMITS: production-pilot evidence is not public-release or general production-readiness approval")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
