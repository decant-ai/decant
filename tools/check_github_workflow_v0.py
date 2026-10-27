#!/usr/bin/env python3
"""Validate the exact least-privilege GitHub CI workflow contract."""

from __future__ import annotations

import sys
from pathlib import Path

MAX_WORKFLOW_BYTES = 16 * 1024
WORKFLOW_PATH = Path(".github/workflows/ci.yml")
EXPECTED_WORKFLOW = """name: CI

on:
  pull_request:
  push:
    branches:
      - main

permissions:
  contents: read

concurrency:
  group: ci-${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

jobs:
  quality:
    runs-on: ubuntu-24.04
    timeout-minutes: 30
    steps:
      - name: Check out source
        uses: actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd # v5.0.1
        with:
          persist-credentials: false
      - name: Install system dependencies
        run: |
          sudo apt-get update
          sudo apt-get install --yes --no-install-recommends librdkafka-dev
      - name: Fetch locked Rust dependencies
        run: cargo fetch --locked
      - name: Run canonical offline gate
        run: ./scripts/check-v0.sh
      - name: Build and verify unsigned native candidate (no publication)
        run: |
          mkdir -p .local
          python3 tools/build_binary_candidate_v1.py --output .local/ci-candidate
          python3 tools/verify_binary_candidate_v1.py .local/ci-candidate
  investigation:
    runs-on: ubuntu-24.04
    timeout-minutes: 30
    steps:
      - name: Check out source
        uses: actions/checkout@93cb6efe18208431cddfb8368fd83d5badbf9bfd # v5.0.1
        with:
          persist-credentials: false
      - name: Install TLS build prerequisites (no Kafka)
        run: |
          sudo apt-get update
          sudo apt-get install --yes --no-install-recommends pkg-config libssl-dev
      - name: Fetch locked Rust dependencies
        run: cargo fetch --locked
      - name: Check investigation-only graph and code
        run: |
          python3 tools/investigation_candidate_v1.py graph
          cargo clippy --locked --offline -p decant-cli --no-default-features --all-targets -- -D warnings
          cargo test --locked --offline -p decant-cli --no-default-features --all-targets
      - name: Build and verify unsigned investigation candidate (no publication)
        run: |
          mkdir -p .local
          python3 tools/investigation_candidate_v1.py build --output .local/investigation-candidate
          python3 tools/investigation_candidate_v1.py verify --output .local/investigation-candidate
"""


class WorkflowFailure(Exception):
    """A content-independent workflow validation failure."""


def validate(path: Path = WORKFLOW_PATH) -> None:
    try:
        metadata = path.lstat()
        if path.is_symlink() or not path.is_file():
            raise WorkflowFailure
        if metadata.st_size == 0 or metadata.st_size > MAX_WORKFLOW_BYTES:
            raise WorkflowFailure
        content = path.read_text(encoding="utf-8")
    except WorkflowFailure:
        raise
    except (OSError, UnicodeDecodeError):
        raise WorkflowFailure from None
    if content != EXPECTED_WORKFLOW:
        raise WorkflowFailure


def main() -> int:
    try:
        validate()
    except WorkflowFailure:
        print("github workflow: failed: contract", file=sys.stderr)
        return 1
    print("PASS: least-privilege GitHub CI workflow v0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
