#!/usr/bin/env python3
"""Run Decant's non-publishing engineering or public release evidence gate."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

sys.dont_write_bytecode = True

if __package__:
    from tools.build_source_artifact_v0 import (
        ARCHIVE_NAME,
        ARCHIVE_PREFIX,
        CHECKSUM_NAME,
        MANIFEST_NAME,
        ArtifactFailure,
        build_artifact,
    )
    from tools.check_public_release_approval_v0 import (
        APPROVAL_PATH,
        ApprovalFailure,
        validate as validate_approval,
    )
    from tools.export_public_source_v0 import ExportFailure, INVENTORY_NAME, export_source
else:
    from build_source_artifact_v0 import (
        ARCHIVE_NAME,
        ARCHIVE_PREFIX,
        CHECKSUM_NAME,
        MANIFEST_NAME,
        ArtifactFailure,
        build_artifact,
    )
    from check_public_release_approval_v0 import (
        APPROVAL_PATH,
        ApprovalFailure,
        validate as validate_approval,
    )
    from export_public_source_v0 import ExportFailure, INVENTORY_NAME, export_source

MAX_MEMBERS = 2049
MAX_MEMBER_BYTES = 4 * 1024 * 1024
MAX_EXTRACTED_BYTES = 36 * 1024 * 1024


class ReleaseGateFailure(Exception):
    """A content-independent release-gate failure."""

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
        raise ReleaseGateFailure(category) from None


def git_output(root: Path, arguments: list[str], category: str) -> str:
    try:
        result = subprocess.run(
            ["git", *arguments],
            cwd=root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        value = result.stdout.decode("ascii").strip()
    except (OSError, UnicodeDecodeError, subprocess.CalledProcessError):
        raise ReleaseGateFailure(category) from None
    return value


def public_preflight(root: Path) -> str:
    license_path = root / "LICENSE"
    if license_path.is_symlink() or not license_path.is_file():
        raise ReleaseGateFailure("missing-license")
    approval_path = root / APPROVAL_PATH
    if approval_path.is_symlink() or not approval_path.is_file():
        raise ReleaseGateFailure("missing-approval")
    if not (root / ".git").exists():
        raise ReleaseGateFailure("public-internal-source")
    run(["git", "diff", "--quiet", "--ignore-submodules"], root, "tracked-changes", quiet=True)
    run(
        ["git", "diff", "--cached", "--quiet", "--ignore-submodules"],
        root,
        "tracked-changes",
        quiet=True,
    )
    commit = git_output(root, ["rev-parse", "HEAD"], "source-commit")
    try:
        validate_approval(root, commit)
    except ApprovalFailure:
        raise ReleaseGateFailure("approval-invalid") from None
    # V0 legal/publication approvals alone do not establish evidence-alpha
    # signing, activation or independently reviewed candidate acceptance.
    if __package__:
        from tools.check_evidence_alpha_approval_v1 import validate as validate_alpha
        from tools.binary_candidate_v1 import CandidateFailure
    else:
        from check_evidence_alpha_approval_v1 import validate as validate_alpha
        from binary_candidate_v1 import CandidateFailure
    try:
        validate_alpha(root, commit)
    except (CandidateFailure, ExportFailure, OSError, ValueError, KeyError, TypeError, RecursionError):
        raise ReleaseGateFailure("alpha-evidence-invalid") from None
    return commit


def validated_root(root: Path) -> Path:
    try:
        canonical = root.resolve(strict=True)
    except OSError:
        raise ReleaseGateFailure("root") from None
    if root.is_symlink() or not canonical.is_dir():
        raise ReleaseGateFailure("root")
    return canonical


def artifact_bytes_equal(first: Path, second: Path) -> None:
    for name in [ARCHIVE_NAME, CHECKSUM_NAME, MANIFEST_NAME]:
        try:
            if (first / name).read_bytes() != (second / name).read_bytes():
                raise ReleaseGateFailure("artifact-reproducibility")
        except ReleaseGateFailure:
            raise
        except OSError:
            raise ReleaseGateFailure("artifact-read") from None


def archive_relative_path(name: str) -> str:
    prefix = f"{ARCHIVE_PREFIX}/"
    if not name.startswith(prefix):
        raise ReleaseGateFailure("archive-prefix")
    value = name[len(prefix) :]
    if not value or "\\" in value or any(ord(character) < 32 for character in value):
        raise ReleaseGateFailure("archive-path")
    path = PurePosixPath(value)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        raise ReleaseGateFailure("archive-path")
    return path.as_posix()


def extract_archive(archive_path: Path, destination: Path) -> Path:
    total = 0
    seen: set[str] = set()
    try:
        with tarfile.open(archive_path, mode="r:gz") as archive:
            members = archive.getmembers()
            if not members or len(members) > MAX_MEMBERS:
                raise ReleaseGateFailure("archive-members")
            for member in members:
                relative = archive_relative_path(member.name)
                if (
                    relative in seen
                    or not member.isfile()
                    or member.size > MAX_MEMBER_BYTES
                    or member.mode not in {0o644, 0o755}
                ):
                    raise ReleaseGateFailure("archive-member")
                seen.add(relative)
                total += member.size
                if total > MAX_EXTRACTED_BYTES:
                    raise ReleaseGateFailure("archive-bound")
                source = archive.extractfile(member)
                if source is None:
                    raise ReleaseGateFailure("archive-member")
                data = source.read(MAX_MEMBER_BYTES + 1)
                if len(data) != member.size:
                    raise ReleaseGateFailure("archive-member")
                target = destination / ARCHIVE_PREFIX / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                with target.open("xb") as handle:
                    handle.write(data)
                os.chmod(target, member.mode)
    except ReleaseGateFailure:
        raise
    except (OSError, tarfile.TarError):
        raise ReleaseGateFailure("archive-read") from None
    if INVENTORY_NAME not in seen:
        raise ReleaseGateFailure("archive-inventory")
    return destination / ARCHIVE_PREFIX


def run_gate(root: Path, mode: str) -> None:
    root = validated_root(root)
    source_commit: str | None = None
    if mode == "public":
        source_commit = public_preflight(root)

    is_internal = (root / ".git").exists()
    with tempfile.TemporaryDirectory(prefix="decant-release-gate-v0-") as directory:
        temporary = Path(directory)
        if is_internal:
            run(["./scripts/check-v0.sh"], root, "internal-quality")
            export = temporary / "export"
            try:
                export_source(root, export, mode)
            except ExportFailure:
                raise ReleaseGateFailure("export") from None
        else:
            if mode != "engineering-preview" or not (root / INVENTORY_NAME).is_file():
                raise ReleaseGateFailure("source-boundary")
            export = root

        first = temporary / "artifact-first"
        second = temporary / "artifact-second"
        try:
            build_artifact(export, first, source_commit=source_commit)
            build_artifact(export, second, source_commit=source_commit)
        except ArtifactFailure:
            raise ReleaseGateFailure("artifact") from None
        artifact_bytes_equal(first, second)

        extracted = extract_archive(first / ARCHIVE_NAME, temporary / "extracted")
        run(
            [
                "python3",
                str(export / "tools/check_repository_hygiene_v0.py"),
                "--root",
                str(extracted),
                "--mode",
                "public-export",
            ],
            export,
            "extracted-hygiene",
        )
        run(["./scripts/check-v0.sh", "--public-export"], extracted, "export-quality")
        run(
            ["cargo", "metadata", "--format-version", "1", "--locked", "--offline", "--no-deps"],
            extracted,
            "extracted-metadata",
            quiet=True,
        )


def main() -> int:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--mode", required=True, choices=("engineering-preview", "public"))
    try:
        arguments = parser.parse_args()
        run_gate(arguments.root, arguments.mode)
    except (SystemExit, ReleaseGateFailure) as error:
        category = error.category if isinstance(error, ReleaseGateFailure) else "arguments"
        if category == "missing-license":
            print("public release gate: missing LICENSE", file=sys.stderr)
        elif category == "missing-approval":
            print("public release gate: missing approval record", file=sys.stderr)
        else:
            print(f"release gate: failed: {category}", file=sys.stderr)
        return 1
    print(f"PASS: Decant 0.1.0-alpha.1 {arguments.mode} release gate")
    print("EVIDENCE: quality dependency hygiene benchmark-smoke export artifact extracted-tree")
    print("LIMITS: no performance privacy production legal signing or publication conclusion")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
