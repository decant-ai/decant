#!/usr/bin/env python3
"""Build a bounded reproducible source archive from a validated public export."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import stat
import sys
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

if __package__:
    from tools.export_public_source_v0 import ExportFailure, INVENTORY_NAME, export_source
else:
    from export_public_source_v0 import ExportFailure, INVENTORY_NAME, export_source

RELEASE_VERSION = "0.1.0-alpha.1"
ARCHIVE_NAME = f"decant-{RELEASE_VERSION}-source.tar.gz"
CHECKSUM_NAME = f"{ARCHIVE_NAME}.sha256"
MANIFEST_NAME = f"decant-{RELEASE_VERSION}-source-manifest-v0.json"
ARCHIVE_PREFIX = f"decant-{RELEASE_VERSION}"
MAX_INVENTORY_BYTES = 4 * 1024 * 1024
MAX_SOURCE_FILES = 2048
MAX_FILE_BYTES = 1024 * 1024
MAX_SOURCE_BYTES = 32 * 1024 * 1024
MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
SOURCE_COMMIT_PATTERN = re.compile(r"[0-9a-f]{40}")


class ArtifactFailure(Exception):
    """A content-independent artifact failure."""

    def __init__(self, category: str = "contract") -> None:
        super().__init__(category)
        self.category = category


@dataclass(frozen=True)
class InventoryFile:
    path: str
    size: int
    digest: str
    mode: int


def relative_path(value: object) -> str:
    if not isinstance(value, str) or not value or "\\" in value:
        raise ArtifactFailure("inventory-path")
    if any(ord(character) < 32 for character in value):
        raise ArtifactFailure("inventory-path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ArtifactFailure("inventory-path")
    return path.as_posix()


def exact_integer(value: object, *, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ArtifactFailure("inventory-shape")
    if value < minimum or value > maximum:
        raise ArtifactFailure("inventory-bound")
    return value


def load_inventory(source: Path) -> tuple[list[InventoryFile], str, bytes]:
    inventory_path = source / INVENTORY_NAME
    try:
        metadata = inventory_path.lstat()
        if inventory_path.is_symlink() or not inventory_path.is_file():
            raise ArtifactFailure("inventory-file")
        if metadata.st_size == 0 or metadata.st_size > MAX_INVENTORY_BYTES:
            raise ArtifactFailure("inventory-bound")
        raw = inventory_path.read_bytes()
        document = json.loads(raw)
    except ArtifactFailure:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise ArtifactFailure("inventory-read") from None
    if not raw.endswith(b"\n") or raw != (
        json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8"):
        raise ArtifactFailure("inventory-canonical")
    if not isinstance(document, dict) or set(document) != {
        "files",
        "format",
        "license_status",
        "release_version",
        "version",
    }:
        raise ArtifactFailure("inventory-shape")
    if (
        document["format"] != "decant-public-source-inventory"
        or isinstance(document["version"], bool)
        or document["version"] != 0
        or document["release_version"] != RELEASE_VERSION
        or document["license_status"] not in {"missing", "approved-present"}
        or not isinstance(document["files"], list)
    ):
        raise ArtifactFailure("inventory-contract")
    if not document["files"] or len(document["files"]) > MAX_SOURCE_FILES:
        raise ArtifactFailure("inventory-bound")

    files: list[InventoryFile] = []
    total = 0
    for item in document["files"]:
        if not isinstance(item, dict) or set(item) != {"bytes", "mode", "path", "sha256"}:
            raise ArtifactFailure("inventory-shape")
        path = relative_path(item["path"])
        if path == INVENTORY_NAME:
            raise ArtifactFailure("inventory-path")
        size = exact_integer(item["bytes"], minimum=0, maximum=MAX_FILE_BYTES)
        digest = item["sha256"]
        if (
            not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise ArtifactFailure("inventory-digest")
        if item["mode"] not in {"644", "755"}:
            raise ArtifactFailure("inventory-mode")
        total += size
        if total > MAX_SOURCE_BYTES:
            raise ArtifactFailure("inventory-bound")
        files.append(InventoryFile(path, size, digest, int(item["mode"], 8)))
    if files != sorted(files, key=lambda item: item.path):
        raise ArtifactFailure("inventory-order")
    if len({item.path for item in files}) != len(files):
        raise ArtifactFailure("inventory-duplicate")
    return files, document["license_status"], raw


def enumerate_source(source: Path) -> list[str]:
    paths: list[str] = []
    try:
        for directory, names, files in os.walk(source, followlinks=False):
            directory_path = Path(directory)
            for name in names:
                child = directory_path / name
                if child.is_symlink():
                    raise ArtifactFailure("source-symlink")
            for name in files:
                child = directory_path / name
                if child.is_symlink() or not child.is_file():
                    raise ArtifactFailure("source-non-regular")
                paths.append(relative_path(child.relative_to(source).as_posix()))
    except ArtifactFailure:
        raise
    except OSError:
        raise ArtifactFailure("source-inventory") from None
    if len(paths) > MAX_SOURCE_FILES + 1 or len(paths) != len(set(paths)):
        raise ArtifactFailure("source-bound")
    return sorted(paths)


def validate_source(source: Path) -> tuple[list[tuple[InventoryFile, bytes]], str, bytes]:
    try:
        canonical_source = source.resolve(strict=True)
    except OSError:
        raise ArtifactFailure("source-root") from None
    if source.is_symlink() or not canonical_source.is_dir():
        raise ArtifactFailure("source-root")
    files, license_status, inventory_bytes = load_inventory(canonical_source)
    expected_paths = sorted([item.path for item in files] + [INVENTORY_NAME])
    if enumerate_source(canonical_source) != expected_paths:
        raise ArtifactFailure("source-inventory-mismatch")
    validated: list[tuple[InventoryFile, bytes]] = []
    for item in files:
        path = canonical_source / item.path
        try:
            metadata = path.lstat()
            if path.is_symlink() or not path.is_file():
                raise ArtifactFailure("source-non-regular")
            data = path.read_bytes()
        except ArtifactFailure:
            raise
        except OSError:
            raise ArtifactFailure("source-read") from None
        if (
            len(data) != item.size
            or hashlib.sha256(data).hexdigest() != item.digest
            or stat.S_IMODE(metadata.st_mode) != item.mode
        ):
            raise ArtifactFailure("source-content-mismatch")
        validated.append((item, data))
    return validated, license_status, inventory_bytes


def validate_output(output: Path) -> None:
    if output.is_symlink():
        raise ArtifactFailure("output")
    try:
        if output.exists() and (not output.is_dir() or any(output.iterdir())):
            raise ArtifactFailure("output")
        if not output.exists() and not output.parent.is_dir():
            raise ArtifactFailure("output")
    except ArtifactFailure:
        raise
    except OSError:
        raise ArtifactFailure("output") from None


def normalized_tar_info(name: str, size: int, mode: int) -> tarfile.TarInfo:
    info = tarfile.TarInfo(name=name)
    info.size = size
    info.mode = mode
    info.uid = 0
    info.gid = 0
    info.uname = ""
    info.gname = ""
    info.mtime = 0
    info.type = tarfile.REGTYPE
    return info


def archive_bytes(files: list[tuple[InventoryFile, bytes]], inventory_bytes: bytes) -> bytes:
    tar_buffer = io.BytesIO()
    try:
        with tarfile.open(fileobj=tar_buffer, mode="w", format=tarfile.USTAR_FORMAT) as archive:
            members = [(item.path, item.mode, data) for item, data in files]
            members.append((INVENTORY_NAME, 0o644, inventory_bytes))
            for relative, mode, data in sorted(members, key=lambda item: item[0]):
                name = f"{ARCHIVE_PREFIX}/{relative}"
                archive.addfile(normalized_tar_info(name, len(data), mode), io.BytesIO(data))
    except (OSError, tarfile.TarError, ValueError):
        raise ArtifactFailure("archive") from None
    gzip_buffer = io.BytesIO()
    try:
        with gzip.GzipFile(filename="", mode="wb", fileobj=gzip_buffer, mtime=0) as compressed:
            compressed.write(tar_buffer.getvalue())
    except OSError:
        raise ArtifactFailure("archive") from None
    result = gzip_buffer.getvalue()
    if not result or len(result) > MAX_ARCHIVE_BYTES:
        raise ArtifactFailure("archive-bound")
    return result


def build_artifact(
    source: Path,
    output: Path,
    *,
    source_commit: str | None = None,
) -> None:
    if source_commit is not None and SOURCE_COMMIT_PATTERN.fullmatch(source_commit) is None:
        raise ArtifactFailure("source-commit")
    validate_output(output)
    files, license_status, inventory_bytes = validate_source(source)
    archive = archive_bytes(files, inventory_bytes)
    archive_digest = hashlib.sha256(archive).hexdigest()
    manifest = {
        "format": "decant-source-release-artifact",
        "version": 0,
        "release_version": RELEASE_VERSION,
        "source_commit": source_commit,
        "signed": False,
        "license_status": license_status,
        "archive": {
            "path": ARCHIVE_NAME,
            "bytes": len(archive),
            "sha256": archive_digest,
        },
        "source_inventory_sha256": hashlib.sha256(inventory_bytes).hexdigest(),
    }
    manifest_bytes = (json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n").encode()
    checksum_bytes = f"{archive_digest}  {ARCHIVE_NAME}\n".encode()
    validate_output(output)
    try:
        output.mkdir(exist_ok=True)
        for name, data in [
            (ARCHIVE_NAME, archive),
            (CHECKSUM_NAME, checksum_bytes),
            (MANIFEST_NAME, manifest_bytes),
        ]:
            path = output / name
            with path.open("xb") as handle:
                handle.write(data)
            os.chmod(path, 0o644)
            os.utime(path, (0, 0), follow_symlinks=False)
        os.utime(output, (0, 0), follow_symlinks=False)
    except OSError:
        raise ArtifactFailure("output-write") from None


def build_from_repository(
    root: Path,
    output: Path,
    mode: str,
    *,
    source_commit: str | None = None,
) -> None:
    validate_output(output)
    with tempfile.TemporaryDirectory(prefix="decant-source-export-v0-") as directory:
        export = Path(directory) / "export"
        try:
            export_source(root, export, mode)
        except ExportFailure as error:
            raise ArtifactFailure(error.category) from None
        build_artifact(export, output, source_commit=source_commit)


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--mode", required=True, choices=("engineering-preview", "public"))
    parser.add_argument("--source-commit")
    try:
        arguments = parser.parse_args()
        build_from_repository(
            arguments.root,
            arguments.output,
            arguments.mode,
            source_commit=arguments.source_commit,
        )
    except (SystemExit, ArtifactFailure) as error:
        category = error.category if isinstance(error, ArtifactFailure) else "arguments"
        print(f"source artifact: failed: {category}", file=sys.stderr)
        return 1
    print(f"PASS: reproducible source artifact v0 ({arguments.mode})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
