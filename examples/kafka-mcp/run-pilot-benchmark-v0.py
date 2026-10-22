#!/usr/bin/env python3
"""Opt-in, commit-bound Kafka-to-MCP production-pilot benchmark V0."""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from types import ModuleType
from typing import Any


OPT_IN = "DECANT_RUN_KAFKA_MCP_BENCHMARK_V0"
RESULT_FORMAT = "decant-kafka-mcp-pilot-benchmark-result"
MAX_RESULT_BYTES = 256 * 1024
SOURCE_COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
KNOWN_LIMITATIONS = [
    "single local broker, one partition, static ownership, and no broker offset commits",
    "producer-to-verified-checkpoint throughput includes the bundled console producer",
    "MCP latency measures a local stdio request after ingestion, not source-to-agent latency",
    "RSS is sampled and may miss a transient process peak",
    "baseline comparisons cover serialized bytes, not equivalent implementation throughput",
    "results do not establish exactly-once delivery, power-loss durability, security, privacy, or general production readiness",
]
OWNED_PROCESSES: list[subprocess.Popen[bytes]] = []


class BenchmarkFailure(RuntimeError):
    pass


def require(condition: bool) -> None:
    if not condition:
        raise BenchmarkFailure


def load_helper(root: Path) -> ModuleType:
    path = root / "examples/kafka-mcp/run-pilot-fault-matrix-v0.py"
    spec = importlib.util.spec_from_file_location("decant_kafka_mcp_fault_v0", path)
    require(spec is not None and spec.loader is not None)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def command_output(command: list[str], root: Path) -> str:
    result = subprocess.run(
        command,
        cwd=root,
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )
    return result.stdout.decode("utf-8", "strict").strip()


def nearest_rank(values: list[int], percentile: int) -> int:
    require(values and 1 <= percentile <= 100)
    ordered = sorted(values)
    return ordered[math.ceil(len(ordered) * percentile / 100) - 1]


def total_memory_bytes() -> int | None:
    try:
        if Path("/proc/meminfo").is_file():
            match = re.search(r"(?m)^MemTotal:\s+(\d+)\s+kB$", Path("/proc/meminfo").read_text())
            return int(match.group(1)) * 1024 if match else None
        value = subprocess.run(
            ["sysctl", "-n", "hw.memsize"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        ).stdout.decode("ascii", "strict").strip()
        return int(value)
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def exact_spec(root: Path) -> dict[str, Any]:
    data = (root / "release/kafka-mcp-pilot-benchmark-spec-v0.json").read_bytes()
    require(len(data) <= 128 * 1024)
    document = json.loads(data)
    require(document.get("format") == "decant-kafka-mcp-pilot-benchmark-spec")
    require(document.get("version") == 0 and document.get("result_status") == "not_measured")
    return document


def start_broker(helper: ModuleType, container: str, run_id: str, image: str) -> None:
    helper.docker(
        "run",
        "--detach",
        "--name",
        container,
        "--label",
        f"ai.decant.kafka-mcp-benchmark={run_id}",
        "--publish",
        "127.0.0.1:9092:9092",
        "--env",
        "KAFKA_NODE_ID=1",
        "--env",
        "KAFKA_PROCESS_ROLES=broker,controller",
        "--env",
        "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
        "--env",
        "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092",
        "--env",
        "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "--env",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
        "--env",
        "KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093",
        "--env",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "--env",
        "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
        "--env",
        "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
        image,
        stdout=subprocess.DEVNULL,
    )
    helper.wait_for_broker(container)


def create_topic(helper: ModuleType, container: str, topic: str) -> tuple[str, str]:
    helper.docker(
        "exec",
        container,
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--create",
        "--topic",
        topic,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        stdout=subprocess.DEVNULL,
    )
    return helper.broker_identity(container, topic)


def start_owned_service(
    helper: ModuleType, binary: Path, config: Path, stderr_path: Path
) -> subprocess.Popen[bytes]:
    process = helper.start_service(binary, config, stderr_path)
    OWNED_PROCESSES.append(process)
    return process


def provenance_offset(value: str) -> int:
    match = re.search(r"/kafka/0/(\d+)$", value)
    require(match is not None)
    return int(match.group(1))


def benchmark_event_lines(start: int, end: int, seed: int) -> bytes:
    base_time = 1_700_000_000_000_000_000
    lines = []
    for index in range(start, end):
        lines.append(
            json.dumps(
                {
                    "time_ns": base_time + index * 1_000_000_000,
                    "service": f"service-{(index + seed) % 8}",
                    "series": "latency_ms",
                    "value": (index * 17 + seed) % 101,
                    "protected": "fault-matrix-protected-sentinel-v0",
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        )
    return ("\n".join(lines) + "\n").encode()


def produce_benchmark(
    helper: ModuleType, container: str, topic: str, records: int, seed: int
) -> None:
    helper.docker(
        "exec",
        "-i",
        container,
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        topic,
        input=benchmark_event_lines(0, records, seed),
        stdout=subprocess.DEVNULL,
    )


def expected_transitions(
    record_count: int, retention_ns: int, seed: int
) -> set[tuple[int, int, int, int]]:
    base_time = 1_700_000_000_000_000_000
    cutoff = base_time + (record_count - 1) * 1_000_000_000 - retention_ns
    by_service: dict[int, list[int]] = {index: [] for index in range(8)}
    for offset in range(record_count):
        if base_time + offset * 1_000_000_000 >= cutoff:
            by_service[(offset + seed) % 8].append(offset)
    expected = set()
    for offsets in by_service.values():
        require(len(offsets) >= 2)
        previous, current = offsets[-2:]
        expected.add(
            (
                previous,
                (previous * 17 + seed) % 101,
                current,
                (current * 17 + seed) % 101,
            )
        )
    return expected


def observed_transitions(context: dict[str, Any]) -> set[tuple[int, int, int, int]]:
    observed = set()
    for item in context.get("changes", {}).get("items", []):
        previous = item["previous"]
        current = item["current"]
        require(previous["value"].get("kind") == "u64")
        require(current["value"].get("kind") == "u64")
        observed.add(
            (
                provenance_offset(previous["provenance"]),
                previous["value"]["value"],
                provenance_offset(current["provenance"]),
                current["value"]["value"],
            )
        )
    return observed


def baselines(records: int, retention_ns: int, seed: int) -> dict[str, int]:
    raw = benchmark_event_lines(records - 64, records, seed)
    cutoff_records = retention_ns // 1_000_000_000 + 1
    filtered = []
    rag = []
    for index in range(records - cutoff_records, records):
        value = {
            "series": "latency_ms",
            "service": f"service-{(index + seed) % 8}",
            "time_ns": 1_700_000_000_000_000_000 + index * 1_000_000_000,
            "value": (index * 17 + seed) % 101,
        }
        filtered.append(json.dumps(value, sort_keys=True, separators=(",", ":")))
    for index in range(records - 64, records):
        rag.append(
            f"service-{(index + seed) % 8} latency_ms {(index * 17 + seed) % 101} at {index}"
        )
    aggregation = json.dumps(
        sorted(expected_transitions(records, retention_ns, seed)), separators=(",", ":")
    ).encode()
    return {
        "last-64-raw-events": len(raw),
        "last-60s-allowlisted-events": len(("\n".join(filtered) + "\n").encode()),
        "rag-recent-event-text": len(("\n".join(rag) + "\n").encode()),
        "hand-built-latest-transition-aggregation": len(aggregation),
    }


def sample_rss(helper: ModuleType, process: subprocess.Popen[bytes], stop: threading.Event, values: list[int]) -> None:
    while not stop.wait(0.02):
        try:
            values.append(helper.current_rss_kib(process))
        except (OSError, ValueError, subprocess.SubprocessError):
            if process.poll() is not None:
                return


def run_once(
    helper: ModuleType,
    binary: Path,
    container: str,
    cluster_id: str,
    topic: str,
    topic_id: str,
    directory: Path,
    ordinal: int,
    workload: dict[str, Any],
) -> tuple[dict[str, Any], bytes]:
    checkpoint_directory = directory / f"checkpoints-{ordinal}"
    checkpoint_directory.mkdir()
    config = helper.service_config(cluster_id, topic_id, topic, checkpoint_directory)
    config["source_namespace"] = "synthetic.kafka.mcp.benchmark.v0"
    config["lifecycle"]["records_between_checkpoints"] = workload["checkpoint_cadence_records"]
    config["context"]["retention_ns"] = workload["retention_ns"]
    config["context"]["item_limit"] = workload["context_item_limit"]
    config["context"]["byte_budget"] = workload["context_byte_budget"]
    config_path = directory / f"config-{ordinal}.json"
    config_path.write_text(json.dumps(config, sort_keys=True, separators=(",", ":")) + "\n")
    stderr_path = directory / f"service-{ordinal}.stderr"
    process = start_owned_service(helper, binary, config_path, stderr_path)
    captures = [helper.initialize(process)]
    rss_values: list[int] = []
    stop = threading.Event()
    sampler = threading.Thread(
        target=sample_rss,
        args=(helper, process, stop, rss_values),
        daemon=True,
    )
    sampler.start()
    start_ns = time.perf_counter_ns()
    produce_benchmark(
        helper,
        container,
        topic,
        workload["records_per_run"],
        workload["seed"],
    )
    helper.wait_for_checkpoint(stderr_path, workload["records_per_run"])
    duration_ns = time.perf_counter_ns() - start_ns
    stop.set()
    sampler.join()
    rss_values.append(helper.current_rss_kib(process))

    latencies_us = []
    context_response = b""
    for sample in range(workload["mcp_latency_samples_per_run"]):
        request_start = time.perf_counter_ns()
        context_response = helper.request_context(process, 100 + sample)
        latencies_us.append((time.perf_counter_ns() - request_start + 999) // 1_000)
        captures.append(context_response)
    context_text = helper.assert_context(context_response)
    context_bytes = context_text.encode()
    context = json.loads(context_text)
    expected = expected_transitions(
        workload["records_per_run"], workload["retention_ns"], workload["seed"]
    )
    observed = observed_transitions(context)
    fidelity = 1_000_000 if observed == expected else 0
    helper.close_service(process)

    restart_stderr = directory / f"restart-{ordinal}.stderr"
    restarted = start_owned_service(helper, binary, config_path, restart_stderr)
    captures.append(helper.initialize(restarted))
    restart_response = helper.request_context(restarted, 1_000 + ordinal)
    captures.append(restart_response)
    restart_equal = helper.assert_context(restart_response) == context_text
    helper.close_service(restarted)

    for diagnostic_path in [stderr_path, restart_stderr]:
        require(diagnostic_path.stat().st_size <= 512 * 1024)
    diagnostic_bytes = stderr_path.read_bytes() + restart_stderr.read_bytes()
    protected_matches = sum(value.count(helper.PROTECTED_SENTINEL.encode()) for value in captures)
    protected_matches += diagnostic_bytes.count(helper.PROTECTED_SENTINEL.encode())
    require(topic.encode() not in diagnostic_bytes and topic_id.encode() not in diagnostic_bytes)
    checkpoints = len(list(checkpoint_directory.glob("checkpoint-*.dcp")))
    throughput_milli = workload["records_per_run"] * 1_000_000_000_000 // duration_ns
    return (
        {
            "run_ordinal": ordinal,
            "run_duration_ns": duration_ns,
            "throughput_events_per_second_milli": throughput_milli,
            "mcp_added_latency_us": {
                "sample_count": len(latencies_us),
                "p50": nearest_rank(latencies_us, 50),
                "p95": nearest_rank(latencies_us, 95),
                "p99": nearest_rank(latencies_us, 99),
            },
            "sampled_peak_rss_kib": max(rss_values),
            "context_bytes": len(context_bytes),
            "context_sha256": hashlib.sha256(context_bytes).hexdigest(),
            "candidate_count": context["changes"]["candidate_count"],
            "included_count": context["changes"]["included_count"],
            "omitted_count": context["changes"]["omitted_count"],
            "task_fidelity_ppm": fidelity,
            "protected_output_matches": protected_matches,
            "restart_context_equal": restart_equal,
            "retained_checkpoint_generations": checkpoints,
            "diagnostic_sha256": hashlib.sha256(diagnostic_bytes).hexdigest(),
        },
        context_bytes,
    )


def build_result(root: Path, output: Path) -> None:
    spec = exact_spec(root)
    workload = spec["workload"]
    helper = load_helper(root)
    subprocess.run(["git", "diff", "--quiet", "--ignore-submodules"], cwd=root, check=True)
    subprocess.run(["git", "diff", "--cached", "--quiet", "--ignore-submodules"], cwd=root, check=True)
    source_commit = command_output(["git", "rev-parse", "HEAD"], root)
    require(SOURCE_COMMIT_PATTERN.fullmatch(source_commit) is not None)
    subprocess.run(
        [
            "cargo",
            "build",
            "--release",
            "--locked",
            "--offline",
            "-p",
            "decant-cli",
            "--bin",
            "decant",
        ],
        cwd=root,
        check=True,
    )
    binary = root / "target/release/decant"
    run_id = f"{os.getpid()}-{int(time.time())}"
    container = f"decant-kafka-mcp-benchmark-v0-{run_id}"
    directory = Path(tempfile.mkdtemp(prefix="decant-kafka-mcp-benchmark-v0-"))
    helper_module = helper
    attempted = False
    try:
        attempted = True
        start_broker(helper_module, container, run_id, workload["broker_image"])
        runs = []
        contexts = []
        cluster_id: str | None = None
        for ordinal in range(workload["run_count"]):
            topic = f"decant-mcp-benchmark-v0-{run_id}-{ordinal}"
            observed_cluster_id, topic_id = create_topic(helper_module, container, topic)
            if cluster_id is None:
                cluster_id = observed_cluster_id
            require(cluster_id == observed_cluster_id)
            run_result, context = run_once(
                helper_module,
                binary,
                container,
                cluster_id,
                topic,
                topic_id,
                directory,
                ordinal,
                workload,
            )
            runs.append(run_result)
            contexts.append(context)
        require(len(set(contexts)) == 1)
        baseline_bytes = baselines(
            workload["records_per_run"], workload["retention_ns"], workload["seed"]
        )
        context_size = len(contexts[0])
        reductions = {
            name: (size - context_size) * 1_000_000 // size
            for name, size in baseline_bytes.items()
        }
        throughputs = [run["throughput_events_per_second_milli"] for run in runs]
        median_throughput = nearest_rank(throughputs, 50)
        result = {
            "format": RESULT_FORMAT,
            "version": 0,
            "result_status": "measured_local_not_product_claim",
            "source_commit": source_commit,
            "benchmark_spec_sha256": hashlib.sha256(
                (root / "release/kafka-mcp-pilot-benchmark-spec-v0.json").read_bytes()
            ).hexdigest(),
            "workload": workload,
            "environment": {
                "build_profile": "release",
                "rust_version": command_output(["rustc", "--version"], root),
                "cargo_version": command_output(["cargo", "--version"], root),
                "os_kernel_architecture": platform.platform(),
                "logical_cpu_count": os.cpu_count() or 1,
                "total_memory_bytes": total_memory_bytes(),
                "filesystem_free_bytes": shutil.disk_usage(root).free,
                "cargo_lock_sha256": hashlib.sha256((root / "Cargo.lock").read_bytes()).hexdigest(),
            },
            "configuration": {
                "security": "trusted-local-plaintext-benchmark-only",
                "source_ownership": "static_partitions",
                "broker_offsets_committed": False,
                "mcp_transport": "local_stdio",
            },
            "dataset": {
                "format": "synthetic.kafka.mcp.benchmark.v0",
                "seed": workload["seed"],
                "serialized_bytes": len(
                    benchmark_event_lines(0, workload["records_per_run"], workload["seed"])
                ),
                "sha256": hashlib.sha256(
                    benchmark_event_lines(0, workload["records_per_run"], workload["seed"])
                ).hexdigest(),
                "event_size_bytes_minimum": min(
                    len(line)
                    for line in benchmark_event_lines(
                        0, workload["records_per_run"], workload["seed"]
                    ).splitlines()
                ),
                "event_size_bytes_maximum": max(
                    len(line)
                    for line in benchmark_event_lines(
                        0, workload["records_per_run"], workload["seed"]
                    ).splitlines()
                ),
                "entity_key_cardinality": 8,
            },
            "warmup": {"duration_ns": 0, "reason": "each run measures cold service and topic"},
            "baselines": {name: {"serialized_bytes": size} for name, size in baseline_bytes.items()},
            "runs": runs,
            "summary": {
                "run_count": len(runs),
                "throughput_median_events_per_second_milli": median_throughput,
                "throughput_range_ppm": (max(throughputs) - min(throughputs)) * 1_000_000
                // median_throughput,
                "mcp_p99_us_maximum": max(run["mcp_added_latency_us"]["p99"] for run in runs),
                "sampled_peak_rss_kib_maximum": max(run["sampled_peak_rss_kib"] for run in runs),
                "context_bytes_maximum": max(run["context_bytes"] for run in runs),
                "context_reduction_ppm_by_baseline": reductions,
            },
            "raw_artifacts": {
                "input_dataset_sha256": hashlib.sha256(
                    benchmark_event_lines(0, workload["records_per_run"], workload["seed"])
                ).hexdigest(),
                "per_run_diagnostic_sha256": [run["diagnostic_sha256"] for run in runs],
                "raw_values_persisted_in_result": False,
            },
            "known_limitations": KNOWN_LIMITATIONS,
        }
        encoded = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
        require(len(encoded) <= MAX_RESULT_BYTES and helper_module.PROTECTED_SENTINEL.encode() not in encoded)
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("xb") as handle:
            handle.write(encoded)
    finally:
        for process in OWNED_PROCESSES:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=helper_module.SERVICE_WAIT_SECONDS)
        inspected = subprocess.run(
            [
                "docker",
                "inspect",
                "--format",
                '{{ index .Config.Labels "ai.decant.kafka-mcp-benchmark" }}',
                container,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=False,
        ) if attempted else None
        if inspected is not None and inspected.returncode == 0 and inspected.stdout.decode().strip() == run_id:
            subprocess.run(
                ["docker", "rm", "-f", container],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        if directory.name.startswith("decant-kafka-mcp-benchmark-v0-"):
            shutil.rmtree(directory, ignore_errors=True)


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--output", required=True, type=Path)
    try:
        arguments = parser.parse_args()
        if os.environ.get(OPT_IN) != "1":
            print(f"refusing without {OPT_IN}=1", file=sys.stderr)
            return 2
        root = Path(__file__).resolve().parents[2]
        allowed = (root / "benchmarks/results").resolve()
        output = (root / arguments.output).resolve() if not arguments.output.is_absolute() else arguments.output.resolve()
        require(allowed == output.parent and not output.exists() and not output.is_symlink())
        build_result(root, output)
    except (OSError, ValueError, KeyError, TypeError, json.JSONDecodeError, subprocess.SubprocessError, BenchmarkFailure):
        print("benchmark failed", file=sys.stderr)
        return 1
    print("PASS: measured local Kafka-MCP pilot benchmark; result is not a product claim")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
