#!/usr/bin/env python3
"""Explicit synthetic Collector serialization test; never run on user telemetry."""

from __future__ import annotations

import http.client
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parent.parent
IMAGE = "otel/opentelemetry-collector-contrib@sha256:799dc6cf12c96192af37b5bdba804da8c10b3bc563b43cb90c3f3c58d9572ad6"
CONFIG = """receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318
exporters:
  file:
    path: /capture/traces.jsonl
    flush_interval: 100ms
service:
  telemetry:
    logs:
      level: error
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [file]
"""


def command(arguments: list[str], timeout: int = 30) -> str:
    result = subprocess.run(arguments, cwd=ROOT, check=True, timeout=timeout,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if len(result.stdout) > 1024 * 1024:
        raise RuntimeError("command output capacity")
    return result.stdout.decode("utf-8").strip()


def run() -> None:
    # The image must already be present. This command never pulls a mutable tag.
    command(["docker", "image", "inspect", IMAGE, "--format", "{{.Id}}"])
    with tempfile.TemporaryDirectory(prefix="decant-collector-evidence-") as directory:
        root = Path(directory)
        capture = root / "capture"
        capture.mkdir()
        config = root / "collector.yaml"
        config.write_text(CONFIG, encoding="utf-8")
        container = command([
            "docker", "run", "--pull=never", "-d", "--read-only", "--cap-drop=ALL",
            "--security-opt=no-new-privileges", "--memory=256m", "--cpus=1",
            "--user", f"{os.getuid()}:{os.getgid()}",
            "--label", "decant.test=collector-evidence-v1",
            "-p", "127.0.0.1::4318",
            "--mount", f"type=bind,src={config},dst=/config.yaml,readonly",
            "--mount", f"type=bind,src={capture},dst=/capture",
            IMAGE, "--config=/config.yaml",
        ])
        if not re.fullmatch(r"[0-9a-f]{64}", container):
            raise RuntimeError("invalid owned container ID")
        try:
            port_text = command(["docker", "port", container, "4318/tcp"])
            if not re.fullmatch(r"127\.0\.0\.1:[0-9]{1,5}", port_text):
                raise RuntimeError("listener is not loopback-only")
            port = int(port_text.rsplit(":", 1)[1])
            lines = (ROOT / "examples/evidence/trace-capture-v1.jsonl").read_bytes().splitlines()
            for index, line in enumerate(lines):
                deadline = time.monotonic() + 15
                while True:
                    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)
                    try:
                        connection.request("POST", "/v1/traces", body=line,
                                           headers={"Content-Type": "application/json"})
                        response = connection.getresponse()
                        if response.status != 200 or len(response.read(4097)) > 4096:
                            raise RuntimeError("Collector rejected synthetic trace")
                        break
                    except ConnectionRefusedError:
                        if index != 0 or time.monotonic() >= deadline:
                            raise
                        time.sleep(0.1)
                    finally:
                        connection.close()
            command(["docker", "stop", "--time=10", container], timeout=20)
            result = capture / "traces.jsonl"
            if not 0 < result.stat().st_size <= 1024 * 1024:
                raise RuntimeError("capture capacity")
            environment = os.environ.copy()
            environment["DECANT_TEST_COLLECTOR_CAPTURE"] = str(result)
            subprocess.run([
                "cargo", "test", "--locked", "--offline", "-p", "decant-otlp-ingest",
                "--test", "evidence_v1", "collector_exported_capture_v1", "--",
                "--ignored", "--exact",
            ], cwd=ROOT, env=environment, check=True, timeout=120)
            print("PASS: Collector 0.160.0 serialization -> production evidence recipe")
            print("Image digest: " + IMAGE.split("@", 1)[1])
        finally:
            # Exact ID returned by our run, not names, globs or other containers.
            command(["docker", "rm", "-f", container])


if __name__ == "__main__":
    if sys.argv[1:] != ["--run"]:
        print("usage: python3 tools/test_collector_evidence_v1.py --run", file=sys.stderr)
        raise SystemExit(2)
    try:
        run()
    except (OSError, RuntimeError, subprocess.SubprocessError, http.client.HTTPException):
        print("Collector evidence test: failed (no compatibility claim)", file=sys.stderr)
        raise SystemExit(1)
