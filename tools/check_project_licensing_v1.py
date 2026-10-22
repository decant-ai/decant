#!/usr/bin/env python3
"""Check approved project license bytes/scope offline; not a legal opinion."""

from __future__ import annotations

import argparse
import hashlib
import re
import sys
import tomllib
from pathlib import Path

MAX_FILE_BYTES = 64 * 1024
LICENSE_DIGESTS = {
    "LICENSE": "0d96a4ff68ad6d4b6f1f30f713b18d5184912ba8dd389f86aa7710db079abcb0",
    "LICENSES/Apache-2.0.txt": "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30",
}
APACHE_FILES = (
    "examples/evidence/kafka-config-v1.json",
    "examples/kafka-live/grouped-ratio-config-v0.example.json",
    "examples/kafka-mcp/config-v0.json",
    "examples/opentelemetry-demo/compose.decant.yaml",
    "examples/opentelemetry-demo/otelcol-config-decant.yml",
)
COPYRIGHT = "Copyright (C) 2026 Eshan Vaid."
YAML_NOTICE = b"# SPDX-FileCopyrightText: 2026 Eshan Vaid\n# SPDX-License-Identifier: Apache-2.0\n"


class LicensingFailure(Exception):
    """Content-independent metadata failure, never a legal conclusion."""


def read(root: Path, relative: str) -> bytes:
    path = root / relative
    if any((root / Path(*Path(relative).parts[:index])).is_symlink()
           for index in range(1, len(Path(relative).parts) + 1)):
        raise LicensingFailure
    if not path.is_file() or not 0 < path.stat().st_size <= MAX_FILE_BYTES:
        raise LicensingFailure
    with path.open("rb") as handle:
        data = handle.read(MAX_FILE_BYTES + 1)
    if not 0 < len(data) <= MAX_FILE_BYTES:
        raise LicensingFailure
    return data


def validate(root: Path) -> None:
    if root.is_symlink() or not root.is_dir():
        raise LicensingFailure
    for path, expected in LICENSE_DIGESTS.items():
        if hashlib.sha256(read(root, path)).hexdigest() != expected:
            raise LicensingFailure
    notice = read(root, "LICENSING.md").decode("utf-8")
    if COPYRIGHT not in notice or "SPDX: `AGPL-3.0-only`" not in notice:
        raise LicensingFailure
    listed = tuple(re.findall(r"^- `(examples/[^`]+)`$", notice, re.MULTILINE))
    if listed != APACHE_FILES:
        raise LicensingFailure
    for path in APACHE_FILES:
        data = read(root, path)
        if path.endswith((".yaml", ".yml")) and not data.startswith(YAML_NOTICE):
            raise LicensingFailure
    workspace = tomllib.loads(read(root, "Cargo.toml").decode())["workspace"]
    if workspace["package"]["license"] != "AGPL-3.0-only":
        raise LicensingFailure
    members = workspace["members"]
    if not isinstance(members, list) or not 1 <= len(members) <= 64:
        raise LicensingFailure
    if not all(isinstance(member, str) and re.fullmatch(r"crates/decant-[a-z0-9-]+", member)
               for member in members) or len(set(members)) != len(members):
        raise LicensingFailure
    for member in members:
        package = tomllib.loads(read(root, member + "/Cargo.toml").decode())["package"]
        license_value = package.get("license")
        if (not isinstance(license_value, dict) or set(license_value) != {"workspace"}
                or license_value["workspace"] is not True or "license-file" in package):
            raise LicensingFailure


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    args = parser.parse_args()
    try:
        validate(args.root)
    except (LicensingFailure, OSError, ValueError, KeyError, TypeError):
        print("project licensing: failed: approved metadata drift", file=sys.stderr)
        return 1
    print("PASS: approved project license bytes and scope; no legal or release conclusion")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
