from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.check_kafka_mcp_pilot_evidence_v0 import (
    APPROVALS,
    BENCHMARK_PATH,
    EVIDENCE_PATH,
    KNOWN_LIMITATIONS,
    PilotEvidenceFailure,
    validate_benchmark,
    validate_evidence,
    validated_spec,
)
from tools.run_kafka_mcp_pilot_gate_v0 import PilotGateFailure, production_preflight


class KafkaMcpPilotGateTests(unittest.TestCase):
    commit = "a" * 40

    def make_root(self, directory: str) -> Path:
        root = Path(directory)
        (root / "release").mkdir(parents=True)
        repository = Path(__file__).resolve().parents[2]
        shutil.copyfile(
            repository / "release/kafka-mcp-pilot-benchmark-spec-v0.json",
            root / "release/kafka-mcp-pilot-benchmark-spec-v0.json",
        )
        return root

    def benchmark_document(self, root: Path) -> dict[str, object]:
        spec, spec_bytes = validated_spec(root)
        runs = []
        for ordinal, throughput in enumerate([200_000, 210_000, 205_000]):
            runs.append(
                {
                    "run_ordinal": ordinal,
                    "run_duration_ns": 50_000_000_000,
                    "throughput_events_per_second_milli": throughput,
                    "mcp_added_latency_us": {"sample_count": 30, "p50": 100, "p95": 200, "p99": 300},
                    "sampled_peak_rss_kib": 100_000,
                    "context_bytes": 500,
                    "context_sha256": "b" * 64,
                    "candidate_count": 8,
                    "included_count": 8,
                    "omitted_count": 0,
                    "task_fidelity_ppm": 1_000_000,
                    "protected_output_matches": 0,
                    "restart_context_equal": True,
                    "retained_checkpoint_generations": 3,
                    "diagnostic_sha256": chr(ord("c") + ordinal) * 64,
                }
            )
        baselines = {
            "hand-built-latest-transition-aggregation": {"serialized_bytes": 1_000},
            "last-60s-allowlisted-events": {"serialized_bytes": 4_000},
            "last-64-raw-events": {"serialized_bytes": 8_000},
            "rag-recent-event-text": {"serialized_bytes": 2_000},
        }
        reductions = {
            name: (value["serialized_bytes"] - 500) * 1_000_000 // value["serialized_bytes"]
            for name, value in baselines.items()
        }
        diagnostics = [run["diagnostic_sha256"] for run in runs]
        return {
            "format": "decant-kafka-mcp-pilot-benchmark-result",
            "version": 0,
            "result_status": "measured_local_not_product_claim",
            "source_commit": self.commit,
            "benchmark_spec_sha256": hashlib.sha256(spec_bytes).hexdigest(),
            "workload": spec["workload"],
            "environment": {
                "build_profile": "release",
                "rust_version": "rustc synthetic-test",
                "cargo_version": "cargo synthetic-test",
                "os_kernel_architecture": "synthetic-test-os",
                "logical_cpu_count": 4,
                "total_memory_bytes": 8_000_000_000,
                "filesystem_free_bytes": 1_000_000_000,
                "cargo_lock_sha256": "f" * 64,
            },
            "configuration": {
                "security": "trusted-local-plaintext-benchmark-only",
                "source_ownership": "static_partitions",
                "broker_offsets_committed": False,
                "mcp_transport": "local_stdio",
            },
            "dataset": {
                "format": "synthetic.kafka.mcp.benchmark.v0",
                "seed": 42,
                "serialized_bytes": 1_000_000,
                "sha256": "1" * 64,
                "event_size_bytes_minimum": 90,
                "event_size_bytes_maximum": 120,
                "entity_key_cardinality": 8,
            },
            "warmup": {"duration_ns": 0, "reason": "each run measures cold service and topic"},
            "baselines": baselines,
            "runs": runs,
            "summary": {
                "run_count": 3,
                "throughput_median_events_per_second_milli": 205_000,
                "throughput_range_ppm": 48_780,
                "mcp_p99_us_maximum": 300,
                "sampled_peak_rss_kib_maximum": 100_000,
                "context_bytes_maximum": 500,
                "context_reduction_ppm_by_baseline": reductions,
            },
            "raw_artifacts": {
                "input_dataset_sha256": "1" * 64,
                "per_run_diagnostic_sha256": diagnostics,
                "raw_values_persisted_in_result": False,
            },
            "known_limitations": KNOWN_LIMITATIONS.copy(),
        }

    def write_benchmark(self, root: Path, document: object) -> bytes:
        path = root / BENCHMARK_PATH
        path.parent.mkdir(parents=True, exist_ok=True)
        data = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
        path.write_bytes(data)
        return data

    def evidence_document(self, benchmark: bytes) -> dict[str, object]:
        return {
            "format": "decant-kafka-mcp-pilot-evidence",
            "version": 0,
            "release_version": "0.1.0-alpha.1",
            "approved_source_commit": self.commit,
            "benchmark_result_sha256": hashlib.sha256(benchmark).hexdigest(),
            "approvals": {name: True for name in APPROVALS},
        }

    def write_evidence(self, root: Path, document: object) -> None:
        (root / EVIDENCE_PATH).write_text(
            json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )

    def test_exact_commit_bound_benchmark_and_evidence_pass(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self.make_root(directory)
            benchmark = self.write_benchmark(root, self.benchmark_document(root))
            self.write_evidence(root, self.evidence_document(benchmark))
            self.assertEqual(validate_benchmark(root, self.commit), benchmark)
            validate_evidence(root, self.commit, benchmark)

    def test_measurement_gate_and_evidence_mutations_fail_closed(self) -> None:
        mutations = [
            ("benchmark-throughput", lambda value: value["runs"][0].__setitem__("throughput_events_per_second_milli", 99_999)),
            ("benchmark-leakage", lambda value: value["runs"][0].__setitem__("protected_output_matches", 1)),
            ("benchmark-restart", lambda value: value["runs"][0].__setitem__("restart_context_equal", False)),
            ("benchmark-context-repeatability", lambda value: value["runs"][0].__setitem__("context_sha256", "9" * 64)),
            ("benchmark-commit", lambda value: value.__setitem__("source_commit", "9" * 40)),
            ("benchmark-workload", lambda value: value["workload"].__setitem__("records_per_run", 9_999)),
            ("benchmark-shape", lambda value: value.__setitem__("unknown", True)),
        ]
        with tempfile.TemporaryDirectory() as directory:
            for index, (category, mutate) in enumerate(mutations):
                root = self.make_root(str(Path(directory) / str(index)))
                document = self.benchmark_document(root)
                mutate(document)
                self.write_benchmark(root, document)
                with self.assertRaises(PilotEvidenceFailure) as raised:
                    validate_benchmark(root, self.commit)
                self.assertEqual(raised.exception.category, category)

            root = self.make_root(str(Path(directory) / "evidence"))
            benchmark = self.write_benchmark(root, self.benchmark_document(root))
            evidence = self.evidence_document(benchmark)
            evidence["approvals"]["founder_production_pilot_approved"] = False
            self.write_evidence(root, evidence)
            with self.assertRaises(PilotEvidenceFailure) as raised:
                validate_evidence(root, self.commit, benchmark)
            self.assertEqual(raised.exception.category, "evidence-incomplete")

    def test_noncanonical_oversized_and_protected_results_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self.make_root(directory)
            path = root / BENCHMARK_PATH
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(self.benchmark_document(root), indent=2) + "\n")
            with self.assertRaises(PilotEvidenceFailure) as raised:
                validate_benchmark(root, self.commit)
            self.assertEqual(raised.exception.category, "benchmark-json-canonical")
            path.write_bytes(b"x" * (256 * 1024 + 1))
            with self.assertRaises(PilotEvidenceFailure) as raised:
                validate_benchmark(root, self.commit)
            self.assertEqual(raised.exception.category, "benchmark")
            document = self.benchmark_document(root)
            document["known_limitations"].append("fault-matrix-protected-sentinel-v0")
            self.write_benchmark(root, document)
            with self.assertRaises(PilotEvidenceFailure) as raised:
                validate_benchmark(root, self.commit)
            self.assertEqual(raised.exception.category, "benchmark-protected-value")

    def test_production_preflight_requires_external_files_before_git(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = self.make_root(directory)
            with self.assertRaises(PilotGateFailure) as raised:
                production_preflight(root)
            self.assertEqual(raised.exception.category, "missing-benchmark-result")
            benchmark = self.write_benchmark(root, self.benchmark_document(root))
            with self.assertRaises(PilotGateFailure) as raised:
                production_preflight(root)
            self.assertEqual(raised.exception.category, "missing-pilot-evidence")
            self.write_evidence(root, self.evidence_document(benchmark))
            with self.assertRaises(PilotGateFailure) as raised:
                production_preflight(root)
            self.assertEqual(raised.exception.category, "source-boundary")

            (root / ".git").mkdir()
            with patch("tools.run_kafka_mcp_pilot_gate_v0.run"), patch(
                "tools.run_kafka_mcp_pilot_gate_v0.git_output", return_value=self.commit
            ):
                self.assertEqual(production_preflight(root), self.commit)


if __name__ == "__main__":
    unittest.main()
