#!/usr/bin/env python3
"""Export a bounded allowlisted public source tree from tracked files only."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable

MAX_ALLOWLIST_BYTES = 64 * 1024
MAX_TRACKED_FILES = 4096
MAX_EXPORTED_FILES = 2048
MAX_FILE_BYTES = 1024 * 1024
MAX_TOTAL_BYTES = 32 * 1024 * 1024
MAX_APPROVAL_BYTES = 64 * 1024
INVENTORY_NAME = "PUBLIC-SOURCE-INVENTORY-v0.json"
LICENSE_MARKER_NAME = "LICENSE-MISSING.txt"

FORBIDDEN_PREFIXES = (
    ".agents/",
    ".codex/",
    ".git/",
    ".idea/",
    "docs/adr/",
    "docs/dependencies/",
    "docs/reviews/",
    "docs/source/",
    "docs/tasks/",
    "prompts/",
    "opentelemetry-demo/",
    "target/",
)
FORBIDDEN_EXACT = {
    "AGENTS.md",
    "README_FIRST.md",
    "Decant_AI_Codex_Bootstrap_Brief.pdf",
    "GLOBAL_AGENTS.example.md",
    "CODEX_DESKTOP_SETUP.md",
    "CHECKSUMS.sha256",
    "MANIFEST.txt",
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
FORBIDDEN_BASENAMES = {"AGENTS.md", "AGENTS.override.md", "SKILL.md"}
REQUIRED_EXACT = {
    ".gitignore",
    "CHANGELOG.md",
    "Cargo.lock",
    "Cargo.toml",
    "README.md",
    "SECURITY.md",
    "rust-toolchain.toml",
}


class ExportFailure(Exception):
    """Content-independent export failure."""

    def __init__(self, category: str = "contract") -> None:
        super().__init__(category)
        self.category = category


@dataclass(frozen=True)
class AllowEntry:
    kind: str
    path: str


@dataclass(frozen=True)
class SourceFile:
    path: str
    size: int
    digest: str
    executable: bool


def relative_path(value: str, *, tree: bool = False, allow_forbidden: bool = False) -> str:
    if not value or "\\" in value or any(ord(character) < 32 for character in value):
        raise ExportFailure
    if tree != value.endswith("/"):
        raise ExportFailure
    raw = value[:-1] if tree else value
    path = PurePosixPath(raw)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ExportFailure
    normalized = path.as_posix() + ("/" if tree else "")
    if not allow_forbidden and (
        normalized in FORBIDDEN_EXACT
        or path.name in FORBIDDEN_BASENAMES
        or any(normalized.startswith(prefix) for prefix in FORBIDDEN_PREFIXES)
    ):
        raise ExportFailure
    return normalized


def parse_allowlist(path: Path) -> list[AllowEntry]:
    try:
        if path.stat().st_size > MAX_ALLOWLIST_BYTES:
            raise ExportFailure
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        raise ExportFailure from None
    entries: list[AllowEntry] = []
    for line in text.splitlines():
        fields = line.split("\t")
        if len(fields) != 2 or fields[0] not in {"file", "tree"}:
            raise ExportFailure
        entries.append(AllowEntry(fields[0], relative_path(fields[1], tree=fields[0] == "tree")))
    if not entries or len(entries) > 128 or entries != sorted(entries, key=lambda entry: (entry.kind, entry.path)):
        raise ExportFailure
    if len(set(entries)) != len(entries):
        raise ExportFailure
    exact = {entry.path for entry in entries if entry.kind == "file"}
    if not REQUIRED_EXACT.issubset(exact):
        raise ExportFailure
    return entries


def tracked_paths(root: Path) -> list[str]:
    try:
        result = subprocess.run(
            ["git", "ls-files", "-z"],
            cwd=root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        paths = [entry.decode("utf-8") for entry in result.stdout.split(b"\0") if entry]
    except (OSError, UnicodeDecodeError, subprocess.CalledProcessError):
        raise ExportFailure from None
    if not paths or len(paths) > MAX_TRACKED_FILES or len(set(paths)) != len(paths):
        raise ExportFailure
    return paths


def is_allowed(path: str, entries: Iterable[AllowEntry]) -> bool:
    return any(
        path == entry.path if entry.kind == "file" else path.startswith(entry.path)
        for entry in entries
    )


def collect_source_files(
    root: Path, entries: list[AllowEntry], paths: Iterable[str]
) -> list[SourceFile]:
    normalized = [relative_path(path, allow_forbidden=True) for path in paths]
    selected = sorted(path for path in normalized if is_allowed(path, entries))
    if not selected or len(selected) > MAX_EXPORTED_FILES:
        raise ExportFailure
    for entry in entries:
        if not any(
            path == entry.path if entry.kind == "file" else path.startswith(entry.path)
            for path in selected
        ):
            raise ExportFailure
    files: list[SourceFile] = []
    total = 0
    for relative in selected:
        if (
            relative in FORBIDDEN_EXACT
            or PurePosixPath(relative).name in FORBIDDEN_BASENAMES
            or any(relative.startswith(prefix) for prefix in FORBIDDEN_PREFIXES)
        ):
            raise ExportFailure
        source = root / relative
        try:
            stat = source.lstat()
            if source.is_symlink() or not source.is_file():
                raise ExportFailure
            if stat.st_size > MAX_FILE_BYTES:
                raise ExportFailure
            total += stat.st_size
            if total > MAX_TOTAL_BYTES:
                raise ExportFailure
            data = source.read_bytes()
        except ExportFailure:
            raise
        except OSError:
            raise ExportFailure from None
        files.append(
            SourceFile(
                path=relative,
                size=len(data),
                digest=hashlib.sha256(data).hexdigest(),
                executable=bool(stat.st_mode & 0o111),
            )
        )
    return files


def prepare_destination(destination: Path) -> None:
    if destination.is_symlink():
        raise ExportFailure
    try:
        if destination.exists():
            if not destination.is_dir() or any(destination.iterdir()):
                raise ExportFailure
        else:
            destination.mkdir(parents=False)
    except ExportFailure:
        raise
    except OSError:
        raise ExportFailure from None


def validate_public_gates(root: Path) -> None:
    for relative, maximum, category in [
        ("LICENSE", MAX_FILE_BYTES, "public-license"),
        (
            "release/public-release-approval-v0.json",
            MAX_APPROVAL_BYTES,
            "public-approval",
        ),
    ]:
        path = root / relative
        try:
            stat = path.lstat()
            if path.is_symlink() or not path.is_file() or stat.st_size == 0 or stat.st_size > maximum:
                raise ExportFailure(category)
        except OSError:
            raise ExportFailure(category) from None


def copy_export(root: Path, destination: Path, files: list[SourceFile], mode: str) -> None:
    for source_file in files:
        source = root / source_file.path
        target = destination / source_file.path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target, follow_symlinks=False)
        copied = target.read_bytes()
        if len(copied) != source_file.size or hashlib.sha256(copied).hexdigest() != source_file.digest:
            raise ExportFailure
        os.chmod(target, 0o755 if source_file.executable else 0o644)
        os.utime(target, (0, 0), follow_symlinks=False)

    inventory_files = [
        {
            "path": source_file.path,
            "bytes": source_file.size,
            "sha256": source_file.digest,
            "mode": "755" if source_file.executable else "644",
        }
        for source_file in files
    ]
    license_present = any(item["path"] == "LICENSE" for item in inventory_files)
    if mode == "engineering-preview" and not license_present:
        marker = (
            "No license is granted. This engineering preview is not approved for public "
            "distribution. Add only a founder-approved LICENSE through the public release gate.\n"
        ).encode("utf-8")
        marker_path = destination / LICENSE_MARKER_NAME
        marker_path.write_bytes(marker)
        os.chmod(marker_path, 0o644)
        os.utime(marker_path, (0, 0), follow_symlinks=False)
        inventory_files.append(
            {
                "path": LICENSE_MARKER_NAME,
                "bytes": len(marker),
                "sha256": hashlib.sha256(marker).hexdigest(),
                "mode": "644",
            }
        )

    inventory = {
        "format": "decant-public-source-inventory",
        "version": 0,
        "release_version": "0.1.0-alpha.1",
        # License approval/presence is separate from public release authority.
        "license_status": "approved-present" if license_present else "missing",
        "files": sorted(inventory_files, key=lambda item: item["path"]),
    }
    inventory_bytes = (json.dumps(inventory, sort_keys=True, separators=(",", ":")) + "\n").encode()
    inventory_path = destination / INVENTORY_NAME
    inventory_path.write_bytes(inventory_bytes)
    os.chmod(inventory_path, 0o644)
    os.utime(inventory_path, (0, 0), follow_symlinks=False)
    for directory, names, _files in os.walk(destination, topdown=False):
        for name in names:
            os.utime(Path(directory) / name, (0, 0), follow_symlinks=False)
        os.utime(directory, (0, 0), follow_symlinks=False)


def export_source(
    root: Path,
    destination: Path,
    mode: str,
    *,
    allowlist_path: Path | None = None,
    source_paths: Iterable[str] | None = None,
) -> None:
    try:
        canonical_root = root.resolve(strict=True)
    except OSError:
        raise ExportFailure from None
    if root.is_symlink() or not canonical_root.is_dir():
        raise ExportFailure
    if mode == "public":
        validate_public_gates(canonical_root)
    allowlist = allowlist_path or canonical_root / "release/public-source-allowlist-v0.tsv"
    entries = parse_allowlist(allowlist)
    paths = list(source_paths) if source_paths is not None else tracked_paths(canonical_root)
    files = collect_source_files(canonical_root, entries, paths)
    if mode == "public":
        # Approval remains private authorization input, never public source.
        for required in ["LICENSE"]:
            if required not in [source_file.path for source_file in files]:
                files.append(
                    collect_source_files(
                        canonical_root,
                        [AllowEntry("file", required)],
                        [required],
                    )[0]
                )
        files.sort(key=lambda source_file: source_file.path)
    prepare_destination(destination)
    copy_export(canonical_root, destination, files, mode)


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--destination", required=True, type=Path)
    parser.add_argument("--mode", required=True, choices=("engineering-preview", "public"))
    try:
        arguments = parser.parse_args()
        export_source(arguments.root, arguments.destination, arguments.mode)
    except (SystemExit, ExportFailure) as error:
        category = error.category if isinstance(error, ExportFailure) else "arguments"
        print(f"public source export: failed: {category}", file=sys.stderr)
        return 1
    print(f"PASS: public source export v0 ({arguments.mode})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
