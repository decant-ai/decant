#!/usr/bin/env python3
"""Bounded high-confidence repository hygiene checks with redacted failures."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath
from typing import Iterable

MAX_FILES = 4096
MAX_FILE_BYTES = 1024 * 1024
MAX_TOTAL_BYTES = 64 * 1024 * 1024

PRIVATE_KEY_PATTERN = re.compile(b"-----BEGIN " + b"(?:RSA |EC |OPENSSH )?" + b"PRIVATE KEY-----")
GITHUB_TOKEN_PATTERN = re.compile(b"gh" + b"[pousr]_[A-Za-z0-9]{36,}")
AWS_ACCESS_KEY_PATTERN = re.compile(b"AK" + b"IA[0-9A-Z]{16}")
CREDENTIAL_ASSIGNMENT_PATTERN = re.compile(
    rb"(?i)(?<![A-Za-z0-9_])(?:password|api[_-]?key|access[_-]?token|client[_-]?secret)(?![A-Za-z0-9_])[ \t]{0,16}[:=][ \t]{0,16}[\"'][^\"'\r\n]{12,}[\"']"
)
PATTERNS = (
    ("private-key", PRIVATE_KEY_PATTERN),
    ("github-token", GITHUB_TOKEN_PATTERN),
    ("aws-access-key", AWS_ACCESS_KEY_PATTERN),
    ("credential-assignment", CREDENTIAL_ASSIGNMENT_PATTERN),
)

FORBIDDEN_PUBLIC_PREFIXES = (
    ".agents/",
    ".idea/",
    ".git/",
    "docs/adr/",
    "docs/dependencies/",
    "docs/reviews/",
    "docs/source/",
    "docs/tasks/",
    "prompts/",
    "opentelemetry-demo/",
    "target/",
)
FORBIDDEN_PUBLIC_EXACT = {
    "AGENTS.md",
    "README_FIRST.md",
    "Decant_AI_Codex_Bootstrap_Brief.pdf",
    "GLOBAL_AGENTS.example.md",
    "CODEX_DESKTOP_SETUP.md",
    "decant_output.json",
    "docs/release/ALPHA_ACTIVATION.md",
    "docs/release/RELEASE_CHECKLIST.md",
    "docs/release/SOURCE_ONLY_PUBLICATION.md",
    "release/public-release-approval-v0.example.json",
    "release/evidence-alpha-approval-v1.example.json",
    "release/public-release-approval-v0.json",
    "release/evidence-alpha-approval-v1.json",
    "release/kafka-mcp-pilot-evidence-v0.json",
}
FORBIDDEN_PUBLIC_BASENAMES = {"AGENTS.md", "AGENTS.override.md", "SKILL.md"}
PROTECTED_SENTINELS = (
    b"fixture-checkout-private",
    b"fixture-payment-private",
    b"fixture-cart-read-private",
    b"fixture-checkout-write-private",
    b"fixture-payment-error-private",
    b"fixture-status-message-private",
    b"11111111111111111111111111111111",
    b"3333333333333333",
)


class HygieneFailure(Exception):
    def __init__(self, rule: str, path: str = "<repository>") -> None:
        super().__init__(rule)
        self.rule = rule
        self.path = path


def validate_relative_path(value: str) -> str:
    if not value or "\\" in value or any(ord(character) < 32 for character in value):
        raise HygieneFailure("invalid-path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise HygieneFailure("invalid-path")
    return path.as_posix()


def tracked_paths(root: Path) -> list[str]:
    try:
        result = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        raw_paths = [entry for entry in result.stdout.split(b"\0") if entry]
        paths = [entry.decode("utf-8") for entry in raw_paths]
    except (OSError, UnicodeDecodeError, subprocess.CalledProcessError):
        raise HygieneFailure("tracked-inventory") from None
    return paths


def exported_paths(root: Path) -> list[str]:
    paths: list[str] = []
    try:
        for directory, names, files in os.walk(root, followlinks=False):
            directory_path = Path(directory)
            for name in names:
                candidate = directory_path / name
                if candidate.is_symlink():
                    raise HygieneFailure("symlink", candidate.relative_to(root).as_posix())
            for name in files:
                candidate = directory_path / name
                relative = candidate.relative_to(root).as_posix()
                if candidate.is_symlink():
                    raise HygieneFailure("symlink", relative)
                paths.append(relative)
    except OSError:
        raise HygieneFailure("export-inventory") from None
    return paths


def scan_files(root: Path, paths: Iterable[str], mode: str) -> None:
    normalized = [validate_relative_path(path) for path in paths]
    if not normalized or len(normalized) > MAX_FILES or len(set(normalized)) != len(normalized):
        raise HygieneFailure("file-count")
    total_bytes = 0
    for relative in sorted(normalized):
        if mode == "public-export" and (
            relative in FORBIDDEN_PUBLIC_EXACT
            or PurePosixPath(relative).name in FORBIDDEN_PUBLIC_BASENAMES
            or any(relative.startswith(prefix) for prefix in FORBIDDEN_PUBLIC_PREFIXES)
        ):
            raise HygieneFailure("forbidden-public-path", relative)
        path = root / relative
        try:
            stat = path.lstat()
            if not path.is_file() or path.is_symlink():
                raise HygieneFailure("non-regular", relative)
            if stat.st_size > MAX_FILE_BYTES:
                raise HygieneFailure("file-size", relative)
            total_bytes += stat.st_size
            if total_bytes > MAX_TOTAL_BYTES:
                raise HygieneFailure("total-size")
            data = path.read_bytes()
        except HygieneFailure:
            raise
        except OSError:
            raise HygieneFailure("file-read", relative) from None
        if b"\0" in data:
            continue
        for rule, pattern in PATTERNS:
            if pattern.search(data):
                raise HygieneFailure(rule, relative)


def check_protected_golden_values(root: Path) -> None:
    input_path = root / "examples/opentelemetry-demo/traces-v0.jsonl"
    output_paths = (
        root / "crates/decant-cli/tests/golden/otlp-jsonl-run-v0.json",
        root / "crates/decant-cli/tests/golden/sre-demo-v0.json",
    )
    try:
        source = input_path.read_bytes()
        outputs = [path.read_bytes() for path in output_paths]
    except OSError:
        raise HygieneFailure("protected-fixture-read") from None
    for sentinel in PROTECTED_SENTINELS:
        if sentinel not in source:
            raise HygieneFailure("protected-fixture-missing", input_path.as_posix())
        if any(sentinel in output for output in outputs):
            raise HygieneFailure("protected-output", "<golden-output>")


def validate(root: Path, mode: str) -> None:
    try:
        canonical_root = root.resolve(strict=True)
    except OSError:
        raise HygieneFailure("root") from None
    if not canonical_root.is_dir() or root.is_symlink():
        raise HygieneFailure("root")
    paths = tracked_paths(canonical_root) if mode == "internal" else exported_paths(canonical_root)
    scan_files(canonical_root, paths, mode)
    check_protected_golden_values(canonical_root)


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--mode", required=True, choices=("internal", "public-export"))
    try:
        arguments = parser.parse_args()
        validate(arguments.root, arguments.mode)
    except (SystemExit, HygieneFailure) as error:
        if isinstance(error, HygieneFailure):
            print(f"repository hygiene: failed: {error.rule}: {error.path}", file=sys.stderr)
        else:
            print("repository hygiene: failed: arguments: <repository>", file=sys.stderr)
        return 1
    print(f"PASS: repository hygiene v0 ({arguments.mode})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
