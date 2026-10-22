#!/usr/bin/env python3
"""Validate the bounded, commit-bound founder approval required for public release."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path

RELEASE_VERSION = "0.1.0-alpha.1"
APPROVAL_PATH = Path("release/public-release-approval-v0.json")
MAX_APPROVAL_BYTES = 64 * 1024
MAX_LICENSE_BYTES = 1024 * 1024
COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")
REQUIRED_APPROVALS = {
    "clean_machine_reproduction_complete",
    "contributor_strategy_approved",
    "employment_ip_review_complete",
    "external_user_activation_requirement_met",
    "founder_publication_approved",
    "full_history_secret_scan_complete",
    "license_approved",
    "name_review_complete",
    "signing_policy_approved",
}


class ApprovalFailure(Exception):
    """A content-independent public approval failure."""

    def __init__(self, category: str) -> None:
        super().__init__(category)
        self.category = category


def bounded_regular_file(path: Path, maximum: int, category: str) -> bytes:
    try:
        metadata = path.lstat()
        if path.is_symlink() or not path.is_file():
            raise ApprovalFailure(category)
        if metadata.st_size == 0 or metadata.st_size > maximum:
            raise ApprovalFailure(category)
        return path.read_bytes()
    except ApprovalFailure:
        raise
    except OSError:
        raise ApprovalFailure(category) from None


def validate(root: Path, expected_source_commit: str) -> None:
    if COMMIT_PATTERN.fullmatch(expected_source_commit) is None:
        raise ApprovalFailure("expected-commit")
    try:
        canonical_root = root.resolve(strict=True)
    except OSError:
        raise ApprovalFailure("root") from None
    if root.is_symlink() or not canonical_root.is_dir():
        raise ApprovalFailure("root")

    license_bytes = bounded_regular_file(canonical_root / "LICENSE", MAX_LICENSE_BYTES, "license")
    approval_bytes = bounded_regular_file(
        canonical_root / APPROVAL_PATH, MAX_APPROVAL_BYTES, "approval-record"
    )
    try:
        document = json.loads(approval_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ApprovalFailure("approval-json") from None
    canonical = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
    if approval_bytes != canonical:
        raise ApprovalFailure("approval-canonical")
    if not isinstance(document, dict) or set(document) != {
        "approvals",
        "approved_source_commit",
        "format",
        "license_sha256",
        "release_version",
        "version",
    }:
        raise ApprovalFailure("approval-shape")
    if document["format"] != "decant-public-release-approval":
        raise ApprovalFailure("approval-format")
    if isinstance(document["version"], bool) or document["version"] != 0:
        raise ApprovalFailure("approval-version")
    if document["release_version"] != RELEASE_VERSION:
        raise ApprovalFailure("release-version")
    approved_commit = document["approved_source_commit"]
    if not isinstance(approved_commit, str) or approved_commit != expected_source_commit:
        raise ApprovalFailure("source-commit")
    license_digest = document["license_sha256"]
    if (
        not isinstance(license_digest, str)
        or license_digest != hashlib.sha256(license_bytes).hexdigest()
    ):
        raise ApprovalFailure("license-digest")
    approvals = document["approvals"]
    if not isinstance(approvals, dict) or set(approvals) != REQUIRED_APPROVALS:
        raise ApprovalFailure("approvals-shape")
    if any(value is not True for value in approvals.values()):
        raise ApprovalFailure("approval-incomplete")


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--expected-source-commit", required=True)
    try:
        arguments = parser.parse_args()
        validate(arguments.root, arguments.expected_source_commit)
    except (SystemExit, ApprovalFailure) as error:
        category = error.category if isinstance(error, ApprovalFailure) else "arguments"
        print(f"public release approval: failed: {category}", file=sys.stderr)
        return 1
    print("PASS: public release approval v0")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
