#!/usr/bin/env python3
"""Opt-in synthetic Kafka -> verified evidence -> real MCP stdio acceptance.

Uses the existing pinned Kafka test helpers. Never pulls an image, contacts a
user broker, mounts a host directory, or cleans up a container it did not own.
"""
import json
from pathlib import Path
import runpy
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
HELPERS = runpy.run_path(str(ROOT / "examples/kafka-mcp/run-pilot-fault-matrix-v0.py"))
IMAGE = HELPERS["IMAGE"]
send = HELPERS["send_message"]
read = HELPERS["read_message"]


def docker(*arguments, **kwargs):
    return subprocess.run(["docker", *arguments], check=True, timeout=120, **kwargs)


def check(condition, message):
    if not condition:
        raise RuntimeError(message)


def call(process, tool):
    send(process, {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                   "params": {"name": tool, "arguments": {}}})
    reply = read(process)
    check(b"synthetic-private-detail" not in reply, "egress sentinel")
    return json.loads(reply)["result"]["structuredContent"]


def wait_report(process, accepted=4):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        reply = call(process, "decant_evidence_snapshot")
        if reply["status"] == "available":
            report = json.loads(reply["report_json"])
            if report["source_cut"]["accepted_retained_entries"] == accepted:
                return reply["report_json"]
        time.sleep(0.1)
    raise RuntimeError("evidence checkpoint timeout")


def main():
    if sys.argv[1:] != ["--run"]:
        print("Opt in: python3 tools/test_kafka_evidence_v1.py --run")
        return 2
    docker("image", "inspect", IMAGE, stdout=subprocess.DEVNULL)
    owner = uuid.uuid4().hex
    name = "decant-evidence-test-" + owner
    active = None
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    with tempfile.TemporaryDirectory(prefix="decant-evidence-kafka-") as temporary:
        directory = Path(temporary)
        try:
            docker("run", "--detach", "--pull", "never", "--name", name,
                   "--label", "ai.decant.evidence-test=" + owner,
                   "--memory", "1536m", "--cpus", "2", "--cap-drop", "ALL",
                   "--security-opt", "no-new-privileges", "--publish", f"127.0.0.1:{port}:{port}",
                   "--env", "KAFKA_NODE_ID=1", "--env", "KAFKA_PROCESS_ROLES=broker,controller",
                   "--env", f"KAFKA_LISTENERS=EXTERNAL://:{port},INTERNAL://:9092,CONTROLLER://:9093",
                   "--env", f"KAFKA_ADVERTISED_LISTENERS=EXTERNAL://127.0.0.1:{port},INTERNAL://localhost:9092",
                   "--env", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=EXTERNAL:PLAINTEXT,INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
                   "--env", "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
                   "--env", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
                   "--env", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093",
                   "--env", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
                   "--env", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
                   "--env", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1", IMAGE, stdout=subprocess.DEVNULL)
            HELPERS["wait_for_broker"](name)
            topic = "synthetic-spans"
            docker("exec", name, "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
                   "--create", "--topic", topic, "--partitions", "1", "--replication-factor", "1", stdout=subprocess.DEVNULL)
            cluster, topic_id = HELPERS["broker_identity"](name, topic)
            config = json.loads((ROOT / "examples/evidence/kafka-config-v1.json").read_bytes())
            checkpoints = directory / "checkpoints"
            checkpoints.mkdir()
            config.update(bootstrap_servers=f"127.0.0.1:{port}", topic=topic, expected_cluster_id=cluster,
                          expected_topic_id=topic_id, checkpoint_directory=str(checkpoints),
                          security={"version": 0, "mode": "plaintext"})
            config["lifecycle"].update(records_between_checkpoints=2, idle_observations_between_checkpoints=10000)
            config["recipe"]["policy"]["allow_service_versions"] = True
            path = directory / "config.json"
            path.write_text(json.dumps(config))
            binary = ROOT / "target/debug/decant"
            diagnostics = directory / "stderr"

            def start():
                with diagnostics.open("ab", buffering=0) as errors:
                    process = subprocess.Popen([str(binary), "evidence", "serve-kafka", "--config", str(path)],
                                               stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors, bufsize=0)
                return process

            def produce(data):
                docker("exec", "-i", name, "/opt/kafka/bin/kafka-console-producer.sh",
                       "--bootstrap-server", "localhost:9092", "--topic", topic,
                       input=data, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            active = start()
            HELPERS["initialize"](active)
            check(call(active, "decant_evidence_snapshot")["status"] == "unavailable", "unverified initial report")
            check(call(active, "decant_evidence_live_status")["status"] == "awaiting_checkpoint", "initial health")
            produce((ROOT / "examples/evidence/kafka-spans-v1.jsonl").read_bytes())
            original = wait_report(active)
            service = json.loads(original)["services"][0]
            check(service["baseline"]["observed_server_spans"] == 2 and service["baseline"]["p95_duration_ns"] == 20000000, "baseline oracle")
            check(service["current"]["error_spans"] == 1 and service["current"]["p95_duration_ns"] == 200000000, "current oracle")
            status = call(active, "decant_evidence_live_status")
            check(status["report_id"] == json.loads(original)["report_id"] and status["pending_records"] == 0, "cut status identity")
            before = {item.name: item.read_bytes() for item in checkpoints.iterdir()}
            duplicate = subprocess.run([str(binary), "evidence", "serve-kafka", "--config", str(path)],
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10, check=False)
            check(duplicate.returncode == 1 and duplicate.stdout == b"", "duplicate owner status")
            check(duplicate.stderr == b"error: Kafka MCP service failed (checkpoint_in_use)\nhint: stop the existing Decant MCP server before restarting; do not delete its lock file\n", "duplicate owner diagnostic")
            check(before == {item.name: item.read_bytes() for item in checkpoints.iterdir()}, "duplicate owner mutation")
            active.send_signal(signal.SIGINT)
            check(active.wait(timeout=35) == 0, "SIGINT shutdown")
            active = start()
            HELPERS["initialize"](active)
            check(wait_report(active) == original, "restart byte equality")
            fifth = json.loads((ROOT / "examples/evidence/kafka-spans-v1.jsonl").read_bytes().splitlines()[-1])
            fifth["span_id"] = "0000000000000005"
            fifth["time_ns"] += 1000
            produce(json.dumps(fifth).encode() + b"\n")
            deadline = time.monotonic() + 45
            while call(active, "decant_evidence_live_status")["pending_records"] != 1:
                check(time.monotonic() < deadline, "pending observation timeout")
                time.sleep(0.1)
            # Fill output without reading responses. Intake remains separately
            # bounded; a signal must not wait for the blocked stdio writer.
            for index in range(16):
                send(active, {"jsonrpc":"2.0", "id":10 + index, "method":"tools/call",
                              "params":{"name":"decant_evidence_snapshot", "arguments":{}}})
            time.sleep(0.2)
            active.send_signal(signal.SIGTERM)
            check(active.wait(timeout=35) == 0, "slow-output SIGTERM shutdown")
            active = start()
            HELPERS["initialize"](active)
            recovered = json.loads(wait_report(active, 5))
            check(not recovered["source_cut"]["source_finalized"], "signal finalized event time")
            conflict = dict(fifth)
            conflict["duration_ns"] = 99
            produce((json.dumps(conflict).encode() + b"\n") * 2)
            # Keep stdin open: a failed source must still terminate the service.
            check(active.wait(timeout=45) == 1, "compiler fault did not terminate")
            check(b"error: Kafka evidence processing or checkpoint publication failed\n" in diagnostics.read_bytes(), "missing processing diagnostic")
            # New isolated checkpoint namespace at the explicit end position;
            # do not bypass the previous conflicting suffix in its old store.
            isolated = directory / "disconnect-checkpoints"
            isolated.mkdir()
            config["checkpoint_directory"] = str(isolated)
            config["partitions"][0]["start_offset"] = 7
            path.write_text(json.dumps(config))
            active = start()
            HELPERS["initialize"](active)
            docker("stop", "--time", "5", name, stdout=subprocess.DEVNULL)
            check(active.wait(timeout=45) == 1, "broker-loss termination")
            allowed = {b"info: evidence MCP ready",
                       b"error: Kafka evidence processing or checkpoint publication failed",
                       b"hint: inspect authorized input and storage locally; the pending suffix was not acknowledged to Kafka",
                       b"error: Kafka evidence source validation failed",
                       b"hint: verify connectivity, authorization and configured cluster/topic identity"}
            check(set(diagnostics.read_bytes().splitlines()) <= allowed, "unsafe/unexpected diagnostic")
            print("PASS: real Kafka evidence, SDK wire, ownership, signal/restart, slow output, conflict and broker loss")
            return 0
        finally:
            if active is not None and active.poll() is None:
                active.kill()
                active.wait(timeout=10)
            owned = subprocess.run(["docker", "inspect", "--format", '{{ index .Config.Labels "ai.decant.evidence-test" }}', name],
                                   check=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            if owned.stdout.strip() == owner.encode():
                docker("rm", "--force", name, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    raise SystemExit(main())
