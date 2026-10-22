#!/usr/bin/env python3
"""Validate benchmark-smoke semantics without producing measurements."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

MAX_INPUT_BYTES = 128 * 1024
FORBIDDEN_RESULT_KEYS = {
    "benchmark_results",
    "throughput",
    "events_per_second",
    "latency",
    "p50",
    "p95",
    "p99",
    "peak_rss",
    "context_reduction",
    "fidelity",
    "variance",
}
REQUIRED_BASELINES = [
    "last-n-raw-events",
    "time-window-field-filter",
    "rag-recent-event-text",
    "hand-built-aggregation",
]
REQUIRED_MEASUREMENT_FIELDS = [
    "git_commit",
    "build_profile",
    "rust_version",
    "os_kernel_architecture",
    "cpu_ram_storage_filesystem",
    "dependency_versions",
    "configuration",
    "dataset_version_seed",
    "event_size_key_cardinality",
    "warmup_duration",
    "run_duration",
    "run_count_variance",
    "raw_artifacts",
    "known_limitations",
]


class BenchmarkSmokeFailure(Exception):
    """Content-independent benchmark-smoke failure."""


def read_bounded(path: Path) -> bytes:
    try:
        if path.stat().st_size > MAX_INPUT_BYTES:
            raise BenchmarkSmokeFailure
        return path.read_bytes()
    except OSError:
        raise BenchmarkSmokeFailure from None


def require(condition: bool) -> None:
    if not condition:
        raise BenchmarkSmokeFailure


def walk_keys(value: Any) -> set[str]:
    if isinstance(value, dict):
        keys = set(value)
        for nested in value.values():
            keys.update(walk_keys(nested))
        return keys
    if isinstance(value, list):
        keys: set[str] = set()
        for nested in value:
            keys.update(walk_keys(nested))
        return keys
    return set()


def validate(manifest_path: Path, golden_path: Path) -> None:
    manifest_bytes = read_bounded(manifest_path)
    golden_bytes = read_bounded(golden_path)
    try:
        manifest = json.loads(manifest_bytes)
        output = json.loads(golden_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise BenchmarkSmokeFailure from None
    require(isinstance(manifest, dict) and isinstance(output, dict))
    require(manifest.get("format") == "decant-benchmark-smoke")
    require(manifest.get("version") == 0)
    require(manifest.get("result_status") == "not_measured")
    require(not (walk_keys(manifest) & FORBIDDEN_RESULT_KEYS))

    workload = manifest.get("workload", {})
    expected = manifest.get("expected", {})
    require(workload == {
        "scenario": "sre-v0-primary",
        "seed": 42,
        "data_deliveries": 100_000,
        "command": ["decant", "demo", "sre"],
    })
    require(manifest.get("future_required_baselines") == REQUIRED_BASELINES)
    require(manifest.get("future_required_measurement_fields") == REQUIRED_MEASUREMENT_FIELDS)
    require(expected.get("output_sha256") == hashlib.sha256(golden_bytes).hexdigest())

    context = output.get("context", {})
    facts = context.get("facts", [])
    require(output.get("format") == "decant-sre-demo")
    require(output.get("version") == 0)
    require(output.get("input", {}).get("data_deliveries") == workload["data_deliveries"])
    require(output.get("admission", {}).get("accepted") == expected.get("accepted"))
    require(output.get("admission", {}).get("duplicate_noops") == expected.get("duplicate_noops"))
    require(output.get("admission", {}).get("late_rejected") == expected.get("late_rejected"))
    require(context.get("included_count") == expected.get("included_facts"))
    require(len(facts) == 2)
    require(facts[0].get("previous", {}).get("value", {}).get("value") == expected.get("p99_previous_ms"))
    require(facts[0].get("current", {}).get("value", {}).get("value") == expected.get("p99_current_ms"))
    require(facts[1].get("ratio", {}).get("rate_ppm") == expected.get("payment_error_rate_ppm"))
    prefix = expected.get("provenance_prefix")
    require(isinstance(prefix, str) and facts[0].get("current", {}).get("provenance", "").startswith(prefix))


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--manifest", type=Path, default=Path("release/benchmark-smoke-sre-v0.json")
    )
    parser.add_argument(
        "--golden", type=Path, default=Path("crates/decant-cli/tests/golden/sre-demo-v0.json")
    )
    try:
        arguments = parser.parse_args()
        validate(arguments.manifest, arguments.golden)
    except (SystemExit, BenchmarkSmokeFailure):
        print("benchmark smoke contract: failed", file=sys.stderr)
        return 1
    print("PASS: benchmark smoke contract v0 (not measured)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
