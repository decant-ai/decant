#!/usr/bin/env python3
"""Validate bounded release-smoke outputs without printing their content."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

MAX_DOCUMENT_BYTES = 128 * 1024
PROTECTED_OTLP_SENTINELS = (
    b"fixture-checkout-private",
    b"fixture-payment-private",
    b"fixture-cart-read-private",
    b"fixture-checkout-write-private",
    b"fixture-payment-error-private",
    b"fixture-status-message-private",
    b"11111111111111111111111111111111",
    b"3333333333333333",
)


class ValidationFailure(Exception):
    """Content-independent validation failure."""


def load_json(path: Path) -> tuple[bytes, dict[str, Any]]:
    try:
        size = path.stat().st_size
        if size > MAX_DOCUMENT_BYTES:
            raise ValidationFailure
        data = path.read_bytes()
        value = json.loads(data)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValidationFailure) as error:
        raise ValidationFailure from None
    if not isinstance(value, dict):
        raise ValidationFailure
    return data, value


def require(value: bool) -> None:
    if not value:
        raise ValidationFailure


def validate(sre_path: Path, otlp_path: Path, otlp_input_path: Path) -> None:
    sre_bytes, sre = load_json(sre_path)
    otlp_bytes, otlp = load_json(otlp_path)
    try:
        input_size = otlp_input_path.stat().st_size
        if input_size > MAX_DOCUMENT_BYTES:
            raise ValidationFailure
        otlp_input = otlp_input_path.read_bytes()
    except OSError:
        raise ValidationFailure from None

    require(sre.get("format") == "decant-sre-demo")
    require(sre.get("version") == 0)
    require(sre.get("input", {}).get("data_deliveries") == 100_000)
    require(sre.get("admission", {}).get("accepted") == 98_750)
    require(sre.get("context", {}).get("included_count") == 2)
    require(
        str(
            sre.get("context", {})
            .get("facts", [{}])[0]
            .get("current", {})
            .get("provenance", "")
        ).startswith("decant://")
    )

    require(otlp.get("format") == "decant-otlp-jsonl-run")
    require(otlp.get("version") == 0)
    require(otlp.get("decode", {}).get("spans") == 3)
    require(otlp.get("admission", {}).get("accepted") == 3)
    require(otlp.get("admission", {}).get("finalized") is True)
    require(
        str(otlp.get("context", {}).get("receipt", {}).get("admission_state_hash", "")).startswith(
            "sha256:v1:"
        )
    )

    for sentinel in PROTECTED_OTLP_SENTINELS:
        require(sentinel in otlp_input)
        require(sentinel not in otlp_bytes)
        require(sentinel not in sre_bytes)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--sre", required=True, type=Path)
    parser.add_argument("--otlp", required=True, type=Path)
    parser.add_argument("--otlp-input", required=True, type=Path)
    return parser.parse_args()


def main() -> int:
    try:
        arguments = parse_arguments()
        validate(arguments.sre, arguments.otlp, arguments.otlp_input)
    except (SystemExit, ValidationFailure):
        print("local smoke validation: failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
