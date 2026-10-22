#!/usr/bin/env python3
"""Run Decant's non-publishing Kafka-MCP production-pilot gate."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.dont_write_bytecode = True

if __package__:
    from tools.check_kafka_mcp_pilot_evidence_v0 import (
        BENCHMARK_PATH,
        EVIDENCE_PATH,
        PilotEvidenceFailure,
        validate_benchmark,
        validate_evidence,
        validated_spec,
    )
else:
    from check_kafka_mcp_pilot_evidence_v0 import (
        BENCHMARK_PATH,
        EVIDENCE_PATH,
        PilotEvidenceFailure,
        validate_benchmark,
        validate_evidence,
        validated_spec,
    )


class PilotGateFailure(Exception):
    """A value-safe pilot-gate failure."""

    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


def run(command: list[str], root: Path, category: str, *, quiet: bool = False) -> None:
    try:
        subprocess.run(
            command,
            cwd=root,
            check=True,
            stdout=subprocess.DEVNULL if quiet else None,
        )
    except (OSError, subprocess.CalledProcessError):
        raise PilotGateFailure(category) from None


def git_output(root: Path, arguments: list[str], category: str) -> str:
    try:
        result = subprocess.run(
            ["git", *arguments],
            cwd=root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        return result.stdout.decode("ascii", "strict").strip()
    except (OSError, UnicodeDecodeError, subprocess.CalledProcessError):
        raise PilotGateFailure(category) from None


def validated_root(root: Path) -> Path:
    try:
        canonical = root.resolve(strict=True)
    except OSError:
        raise PilotGateFailure("root") from None
    if root.is_symlink() or not canonical.is_dir():
        raise PilotGateFailure("root")
    return canonical


def production_preflight(root: Path) -> str:
    if not (root / BENCHMARK_PATH).is_file() or (root / BENCHMARK_PATH).is_symlink():
        raise PilotGateFailure("missing-benchmark-result")
    if not (root / EVIDENCE_PATH).is_file() or (root / EVIDENCE_PATH).is_symlink():
        raise PilotGateFailure("missing-pilot-evidence")
    if not (root / ".git").exists():
        raise PilotGateFailure("source-boundary")
    run(["git", "diff", "--quiet", "--ignore-submodules"], root, "tracked-changes", quiet=True)
    run(
        ["git", "diff", "--cached", "--quiet", "--ignore-submodules"],
        root,
        "tracked-changes",
        quiet=True,
    )
    commit = git_output(root, ["rev-parse", "HEAD"], "source-commit")
    try:
        benchmark = validate_benchmark(root, commit)
        validate_evidence(root, commit, benchmark)
    except PilotEvidenceFailure as error:
        raise PilotGateFailure(f"evidence-{error.category}") from None
    return commit


def run_gate(root: Path, mode: str) -> None:
    root = validated_root(root)
    try:
        validated_spec(root)
    except PilotEvidenceFailure as error:
        raise PilotGateFailure(f"spec-{error.category}") from None
    if mode == "production-pilot":
        production_preflight(root)
    run(["./scripts/check-v0.sh"], root, "quality")


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--mode", required=True, choices=("engineering", "production-pilot"))
    try:
        arguments = parser.parse_args()
        run_gate(arguments.root, arguments.mode)
    except (SystemExit, PilotGateFailure) as error:
        category = error.category if isinstance(error, PilotGateFailure) else "arguments"
        print(f"Kafka-MCP pilot gate: failed: {category}", file=sys.stderr)
        return 1
    if arguments.mode == "engineering":
        print("PASS: Kafka-MCP pilot engineering gate; external evidence not evaluated")
    else:
        print("PASS: commit-bound Kafka-MCP production-pilot gate")
    print("LIMITS: no public-release, exactly-once, privacy, security, or general production-readiness conclusion")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
