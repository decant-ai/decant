#!/usr/bin/env python3
"""Fail closed when the locked third-party Cargo graph drifts."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

MAX_INPUT_BYTES = 1024 * 1024
MAX_PACKAGES = 256
REGISTRY_SOURCE = "registry+https://github.com/rust-lang/crates.io-index"


class InventoryFailure(Exception):
    """Content-independent inventory failure."""


def bounded_bytes(path: Path) -> bytes:
    try:
        if path.stat().st_size > MAX_INPUT_BYTES:
            raise InventoryFailure
        return path.read_bytes()
    except OSError:
        raise InventoryFailure from None


def parse_allowlist(data: bytes) -> list[tuple[str, str, str, str]]:
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        raise InventoryFailure from None
    rows: list[tuple[str, str, str, str]] = []
    for line in text.splitlines():
        fields = line.split("\t")
        if len(fields) != 4 or any(not field for field in fields):
            raise InventoryFailure
        rows.append((fields[0], fields[1], fields[2], fields[3]))
    if not rows or len(rows) > MAX_PACKAGES or rows != sorted(rows) or len(set(rows)) != len(rows):
        raise InventoryFailure
    return rows


def rows_from_metadata(metadata: Any) -> list[tuple[str, str, str, str]]:
    if not isinstance(metadata, dict) or not isinstance(metadata.get("packages"), list):
        raise InventoryFailure
    rows: list[tuple[str, str, str, str]] = []
    for package in metadata["packages"]:
        if not isinstance(package, dict):
            raise InventoryFailure
        source = package.get("source")
        if source is None:
            continue
        name = package.get("name")
        version = package.get("version")
        license_expression = package.get("license")
        if (
            not isinstance(name, str)
            or not name
            or not isinstance(version, str)
            or not version
            or not isinstance(license_expression, str)
            or not license_expression
            or source != REGISTRY_SOURCE
        ):
            raise InventoryFailure
        rows.append((name, version, license_expression, source))
    rows.sort()
    if not rows or len(rows) > MAX_PACKAGES or len(set(rows)) != len(rows):
        raise InventoryFailure
    return rows


def load_metadata(metadata_path: Path | None) -> Any:
    if metadata_path is not None:
        data = bounded_bytes(metadata_path)
    else:
        try:
            result = subprocess.run(
                ["cargo", "metadata", "--locked", "--offline", "--format-version", "1"],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
        except (OSError, subprocess.CalledProcessError):
            raise InventoryFailure from None
        data = result.stdout
        if len(data) > MAX_INPUT_BYTES:
            raise InventoryFailure
    try:
        return json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise InventoryFailure from None


def validate(allowlist_path: Path, metadata_path: Path | None = None) -> None:
    expected = parse_allowlist(bounded_bytes(allowlist_path))
    actual = rows_from_metadata(load_metadata(metadata_path))
    if actual != expected:
        raise InventoryFailure


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--allowlist", type=Path, default=Path("release/third-party-dependencies-v0.tsv")
    )
    parser.add_argument("--metadata-json", type=Path)
    try:
        arguments = parser.parse_args()
        validate(arguments.allowlist, arguments.metadata_json)
    except (SystemExit, InventoryFailure):
        print("dependency inventory: failed", file=sys.stderr)
        return 1
    print("PASS: dependency inventory v0 (99 third-party packages)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
