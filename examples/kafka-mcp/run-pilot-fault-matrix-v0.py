#!/usr/bin/env python3
"""Opt-in real-broker fault matrix for the local Kafka-to-MCP pilot path."""

from __future__ import annotations

import json
import os
import re
import selectors
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path


IMAGE = "apache/kafka:4.3.1"
OPT_IN = "DECANT_RUN_KAFKA_MCP_FAULT_MATRIX_V0"
MAX_MCP_LINE_BYTES = 128 * 1024
MAX_RSS_KIB = 512 * 1024
BROKER_READY_ATTEMPTS = 60
BROKER_READY_INTERVAL_SECONDS = 1
SERVICE_WAIT_SECONDS = 45
TOTAL_RECORDS = 2_000
CHECKPOINT_CADENCE = 50
PROTECTED_SENTINEL = "fault-matrix-protected-sentinel-v0"


class VerificationFailure(RuntimeError):
    pass


def run(command: list[str], **kwargs: object) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(command, check=True, **kwargs)


def docker(*arguments: str, **kwargs: object) -> subprocess.CompletedProcess[bytes]:
    return run(["docker", *arguments], **kwargs)


def wait_for_broker(container: str) -> None:
    for _ in range(BROKER_READY_ATTEMPTS):
        result = subprocess.run(
            [
                "docker",
                "exec",
                container,
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                "localhost:9092",
                "--list",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if result.returncode == 0:
            return
        time.sleep(BROKER_READY_INTERVAL_SECONDS)
    raise VerificationFailure("broker-ready-timeout")


def broker_identity(container: str, topic: str) -> tuple[str, str]:
    cluster_output = docker(
        "exec",
        container,
        "/opt/kafka/bin/kafka-metadata-quorum.sh",
        "--bootstrap-server",
        "localhost:9092",
        "describe",
        "--status",
        stdout=subprocess.PIPE,
    ).stdout.decode("utf-8", "strict")
    topic_output = docker(
        "exec",
        container,
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--describe",
        "--topic",
        topic,
        stdout=subprocess.PIPE,
    ).stdout.decode("utf-8", "strict")
    cluster_match = re.search(r"(?m)^ClusterId:\s+(\S+)$", cluster_output)
    topic_match = re.search(r"\bTopicId:\s+(\S+)", topic_output)
    if cluster_match is None or topic_match is None:
        raise VerificationFailure("broker-identity-output")
    cluster_id = cluster_match.group(1)
    topic_id = topic_match.group(1)
    if re.fullmatch(r"[A-Za-z0-9_-]{22}", topic_id) is None:
        raise VerificationFailure("topic-id-contract")
    return cluster_id, topic_id


def service_config(
    cluster_id: str, topic_id: str, topic: str, checkpoint_directory: Path
) -> dict[str, object]:
    return {
        "format": "decant-kafka-mcp-service-config",
        "version": 0,
        "bootstrap_servers": "127.0.0.1:9092",
        "topic": topic,
        "source_namespace": "synthetic.kafka.mcp.fault-matrix.v0",
        "expected_cluster_id": cluster_id,
        "expected_topic_id": topic_id,
        "partitions": [{"partition": 0, "start_offset": 0}],
        "checkpoint_directory": str(checkpoint_directory),
        "epoch_start_ns": 0,
        "epoch_end_ns": 4_102_444_800_000_000_000,
        "accepted_identity_limit": 100_000,
        "accepted_event_byte_limit": 67_108_864,
        "poll_timeout_ms": 250,
        "lifecycle": {
            "version": 0,
            "records_between_checkpoints": CHECKPOINT_CADENCE,
            "idle_observations_between_checkpoints": 5,
            "retained_checkpoint_generations": 3,
        },
        "security": {"mode": "plaintext", "version": 0},
        "mapping": {
            "format": "decant-kafka-json-object-mapping",
            "version": 1,
            "entity_key": {"source": "utf8_projection", "field": "service"},
            "event_time": {"field": "time_ns", "unit": "nanoseconds"},
            "payload": [
                {"source": "service", "target": "service", "type": "utf8"},
                {"source": "series", "target": "series", "type": "utf8"},
                {"source": "value", "target": "value", "type": "u64"},
                {"source": "protected", "target": "protected", "type": "utf8"},
            ],
        },
        "egress": {
            "format": "decant-kafka-field-egress-policy",
            "version": 0,
            "fields": [
                {"field": "protected", "action": "deny"},
                {"field": "series", "action": "allow"},
                {"field": "service", "action": "allow"},
                {"field": "value", "action": "allow"},
            ],
        },
        "context": {
            "format": "decant-kafka-material-change-context-request",
            "version": 1,
            "view_id": "fault-matrix-material-change",
            "retention_ns": 60_000_000_000,
            "discriminator_field": "series",
            "value_field": "value",
            "item_limit": 64,
            "byte_budget": 16_384,
            "entity_key_egress": "deny",
        },
    }


def event_lines(start: int, end: int) -> bytes:
    base_time = 1_700_000_000_000_000_000
    lines = []
    for index in range(start, end):
        lines.append(
            json.dumps(
                {
                    "time_ns": base_time + index * 1_000_000_000,
                    "service": f"service-{index % 8}",
                    "series": "latency_ms",
                    "value": index % 101,
                    "protected": PROTECTED_SENTINEL,
                },
                separators=(",", ":"),
                sort_keys=True,
            )
        )
    return ("\n".join(lines) + "\n").encode("utf-8")


def produce(container: str, topic: str, start: int, end: int) -> None:
    docker(
        "exec",
        "-i",
        container,
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        topic,
        input=event_lines(start, end),
        stdout=subprocess.DEVNULL,
    )


def start_service(binary: Path, config: Path, stderr_path: Path) -> subprocess.Popen[bytes]:
    stderr_file = stderr_path.open("wb", buffering=0)
    process = subprocess.Popen(
        [str(binary), "serve", "kafka-mcp", "--config", str(config)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=stderr_file,
        bufsize=0,
    )
    stderr_file.close()
    return process


def send_message(process: subprocess.Popen[bytes], document: dict[str, object]) -> None:
    if process.stdin is None:
        raise VerificationFailure("service-stdin")
    process.stdin.write(json.dumps(document, separators=(",", ":")).encode("utf-8") + b"\n")
    process.stdin.flush()


def read_message(process: subprocess.Popen[bytes]) -> bytes:
    if process.stdout is None:
        raise VerificationFailure("service-stdout")
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    deadline = time.monotonic() + SERVICE_WAIT_SECONDS
    output = bytearray()
    try:
        while time.monotonic() < deadline:
            events = selector.select(timeout=0.25)
            if not events:
                if process.poll() is not None:
                    raise VerificationFailure("service-ended-before-response")
                continue
            chunk = os.read(process.stdout.fileno(), 4_096)
            if not chunk:
                raise VerificationFailure("service-stdout-closed")
            output.extend(chunk)
            if len(output) > MAX_MCP_LINE_BYTES + 1:
                raise VerificationFailure("service-output-bound")
            newline = output.find(b"\n")
            if newline >= 0:
                if newline != len(output) - 1:
                    raise VerificationFailure("unexpected-multiple-responses")
                return bytes(output[:newline])
        raise VerificationFailure("service-response-timeout")
    finally:
        selector.close()


def initialize(process: subprocess.Popen[bytes]) -> bytes:
    send_message(
        process,
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "decant-fault-matrix", "version": "0"},
            },
        },
    )
    response = read_message(process)
    parsed = json.loads(response)
    if parsed.get("result", {}).get("protocolVersion") != "2025-11-25":
        raise VerificationFailure("mcp-negotiation")
    send_message(process, {"jsonrpc": "2.0", "method": "notifications/initialized"})
    return response


def request_context(process: subprocess.Popen[bytes], request_id: int, read: bool = True) -> bytes:
    send_message(
        process,
        {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "tools/call",
            "params": {"name": "decant_material_changes", "arguments": {}},
        },
    )
    return read_message(process) if read else b""


def diagnostic_documents(path: Path) -> list[dict[str, object]]:
    documents = []
    if not path.exists():
        return documents
    for line in path.read_bytes().splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("format") == "decant-kafka-mcp-service-diagnostic":
            documents.append(value)
    return documents


def wait_for_checkpoint(path: Path, minimum_records: int) -> None:
    deadline = time.monotonic() + SERVICE_WAIT_SECONDS
    while time.monotonic() < deadline:
        if any(
            item.get("kind") == "checkpoint"
            and isinstance(item.get("data_records"), int)
            and int(item["data_records"]) >= minimum_records
            for item in diagnostic_documents(path)
        ):
            return
        time.sleep(0.1)
    raise VerificationFailure("checkpoint-diagnostic-timeout")


def current_rss_kib(process: subprocess.Popen[bytes]) -> int:
    output = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(process.pid)],
        check=True,
        stdout=subprocess.PIPE,
    ).stdout.decode("ascii", "strict")
    return int(output.strip())


def assert_context(response: bytes) -> str:
    if PROTECTED_SENTINEL.encode() in response:
        raise VerificationFailure("protected-value-in-mcp")
    document = json.loads(response)
    context = document.get("result", {}).get("structuredContent", {}).get("context_json")
    if not isinstance(context, str):
        raise VerificationFailure("missing-compiled-context")
    parsed = json.loads(context)
    if parsed.get("format") != "decant-kafka-material-change-context":
        raise VerificationFailure("compiled-context-format")
    return context


def close_service(process: subprocess.Popen[bytes]) -> None:
    if process.stdin is not None:
        process.stdin.close()
    status = process.wait(timeout=SERVICE_WAIT_SECONDS)
    if status != 0:
        raise VerificationFailure("graceful-service-status")


def main() -> int:
    if os.environ.get(OPT_IN) != "1":
        print(f"refusing without {OPT_IN}=1", file=sys.stderr)
        return 2
    root = Path(__file__).resolve().parents[2]
    binary = root / "target" / "debug" / "decant"
    run(
        [
            "cargo",
            "build",
            "--manifest-path",
            str(root / "Cargo.toml"),
            "--locked",
            "--offline",
            "-p",
            "decant-cli",
            "--bin",
            "decant",
        ]
    )
    run_id = f"{os.getpid()}-{int(time.time())}"
    container = f"decant-kafka-mcp-fault-v0-{run_id}"
    topic = f"decant-mcp-fault-v0-{run_id}"
    directory = Path(tempfile.mkdtemp(prefix="decant-kafka-mcp-fault-v0-"))
    checkpoint_directory = directory / "checkpoints"
    checkpoint_directory.mkdir()
    config_path = directory / "config.json"
    active: subprocess.Popen[bytes] | None = None
    container_attempted = False
    captured: list[bytes] = []
    diagnostic_paths: list[Path] = []
    try:
        container_attempted = True
        docker(
            "run",
            "--detach",
            "--name",
            container,
            "--label",
            f"ai.decant.kafka-mcp-fault={run_id}",
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
            IMAGE,
            stdout=subprocess.DEVNULL,
        )
        wait_for_broker(container)
        docker(
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
        cluster_id, topic_id = broker_identity(container, topic)
        config_path.write_text(
            json.dumps(
                service_config(cluster_id, topic_id, topic, checkpoint_directory),
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        produce(container, topic, 0, 100)

        first_stderr = directory / "first.stderr"
        diagnostic_paths.append(first_stderr)
        active = start_service(binary, config_path, first_stderr)
        captured.append(initialize(active))
        wait_for_checkpoint(first_stderr, 100)

        request_context(active, 2, read=False)
        produce(container, topic, 100, TOTAL_RECORDS)
        wait_for_checkpoint(first_stderr, TOTAL_RECORDS)
        time.sleep(1)
        slow_response = read_message(active)
        captured.append(slow_response)
        pre_kill_context = assert_context(slow_response)
        rss_samples = [current_rss_kib(active) for _ in range(5)]
        if max(rss_samples) > MAX_RSS_KIB:
            raise VerificationFailure("rss-engineering-ceiling")

        active.send_signal(signal.SIGKILL)
        if active.wait(timeout=SERVICE_WAIT_SECONDS) == 0:
            raise VerificationFailure("hard-kill-status")
        active = None
        if len(list(checkpoint_directory.glob("checkpoint-*.dcp"))) != 3:
            raise VerificationFailure("checkpoint-pruning-bound")

        restart_stderr = directory / "restart.stderr"
        diagnostic_paths.append(restart_stderr)
        active = start_service(binary, config_path, restart_stderr)
        captured.append(initialize(active))
        restart_response = request_context(active, 3)
        captured.append(restart_response)
        if assert_context(restart_response) != pre_kill_context:
            raise VerificationFailure("hard-kill-recovery-context")

        docker("stop", container, stdout=subprocess.DEVNULL)
        status = active.wait(timeout=SERVICE_WAIT_SECONDS)
        if status == 0:
            raise VerificationFailure("broker-loss-must-fail-closed")
        active = None
        if not any(
            item.get("kind") == "fault" and item.get("termination") == "poll"
            for item in diagnostic_documents(restart_stderr)
        ):
            raise VerificationFailure("broker-loss-diagnostic")

        docker("start", container, stdout=subprocess.DEVNULL)
        wait_for_broker(container)
        recovered_stderr = directory / "recovered.stderr"
        diagnostic_paths.append(recovered_stderr)
        active = start_service(binary, config_path, recovered_stderr)
        captured.append(initialize(active))
        recovered_response = request_context(active, 4)
        captured.append(recovered_response)
        if assert_context(recovered_response) != pre_kill_context:
            raise VerificationFailure("broker-restart-context")
        close_service(active)
        active = None

        latest = max(checkpoint_directory.glob("checkpoint-*.dcp"))
        latest.write_bytes(b"fault-matrix-corrupt-checkpoint")
        corrupt = subprocess.run(
            [str(binary), "serve", "kafka-mcp", "--config", str(config_path)],
            input=b"",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=SERVICE_WAIT_SECONDS,
            check=False,
        )
        if corrupt.returncode != 1 or corrupt.stdout:
            raise VerificationFailure("corrupt-checkpoint-fail-closed")
        captured.extend([corrupt.stdout, corrupt.stderr])

        combined = b"".join(captured)
        combined += b"".join(path.read_bytes() for path in diagnostic_paths)
        lifecycle = [
            item
            for path in diagnostic_paths
            for item in diagnostic_documents(path)
            if item.get("kind") in {"ready", "fault", "stopped"}
        ]
        if not lifecycle or not any(item.get("kind") == "stopped" for item in lifecycle):
            raise VerificationFailure("lifecycle-diagnostics")
        if any(
            item.get("source_ownership") != "static_partitions"
            or item.get("broker_offsets_committed") is not False
            for item in lifecycle
        ):
            raise VerificationFailure("source-ownership-diagnostics")
        for forbidden in [
            PROTECTED_SENTINEL.encode(),
            cluster_id.encode(),
            topic_id.encode(),
            topic.encode(),
            str(checkpoint_directory).encode(),
        ]:
            if forbidden in combined:
                raise VerificationFailure("value-safe-output-scan")
        print(
            "PASS: Kafka-MCP hard-kill recovery, slow-reader progress, broker-loss fail-closed/restart, corrupt-latest refusal, pruning bound, output leakage scan, and RSS engineering ceiling"
        )
        print(
            f"OBSERVED-NOT-A-BENCHMARK: sampled_peak_rss_kib={max(rss_samples)} records={TOTAL_RECORDS}"
        )
        return 0
    except (OSError, subprocess.SubprocessError, VerificationFailure, ValueError) as error:
        print(f"verification failed: {type(error).__name__}", file=sys.stderr)
        return 1
    finally:
        if active is not None and active.poll() is None:
            active.kill()
            active.wait(timeout=SERVICE_WAIT_SECONDS)
        if container_attempted:
            inspected = subprocess.run(
                [
                    "docker",
                    "inspect",
                    "--format",
                    '{{ index .Config.Labels "ai.decant.kafka-mcp-fault" }}',
                    container,
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                check=False,
            )
            if inspected.returncode == 0 and inspected.stdout.decode().strip() == run_id:
                subprocess.run(
                    ["docker", "rm", "-f", container],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                )
        if directory.name.startswith("decant-kafka-mcp-fault-v0-"):
            shutil.rmtree(directory, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
