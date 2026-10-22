#!/usr/bin/env python3
"""Build and test a local candidate from the allowlisted source export."""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

sys.dont_write_bytecode = True

if __package__:
    from tools.binary_candidate_v1 import *
    from tools.build_source_artifact_v0 import ArtifactFailure, build_artifact
    from tools.check_public_release_approval_v0 import ApprovalFailure, validate as approve_source
    from tools.export_public_source_v0 import ExportFailure, INVENTORY_NAME, export_source
else:
    from binary_candidate_v1 import *
    from build_source_artifact_v0 import ArtifactFailure, build_artifact
    from check_public_release_approval_v0 import ApprovalFailure, validate as approve_source
    from export_public_source_v0 import ExportFailure, INVENTORY_NAME, export_source


def command(args: list[str], cwd: Path, *, env: dict | None = None, maximum: int = MAX_SMALL) -> bytes:
    result = subprocess.run(args, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    require(result.returncode == 0 and len(result.stdout) <= maximum and not result.stderr)
    return result.stdout


def write_new(path: Path, content: bytes, *, executable: bool = False) -> None:
    with path.open("xb") as handle:
        handle.write(content)
    path.chmod(0o755 if executable else 0o644)


def rust_build_environment(root: Path, temporary: Path) -> tuple[dict, list[bytes]]:
    environment = os.environ.copy()
    mappings = [(Path.home().resolve(), "/decant-host"), (root, "/decant-source")]
    if environment.get("CARGO_HOME"):
        mappings.append((Path(environment["CARGO_HOME"]).resolve(), "/decant-cargo"))
    # More specific mappings come later; encoded arguments handle spaces without
    # shell interpolation or copying environment values to the manifest/output.
    mappings.extend([(temporary, "/decant-build"), (temporary / "export", "/decant-source")])
    environment["CARGO_TARGET_DIR"] = str(temporary / "build")
    environment.pop("RUSTFLAGS", None)
    environment["CARGO_ENCODED_RUSTFLAGS"] = "\x1f".join(
        f"--remap-path-prefix={path}={replacement}" for path, replacement in mappings
    )
    # OpenSSL embeds its temporary installation path as native configuration,
    # not a source filename. Do not patch binary strings or change TLS features
    # to hide it. This non-hermetic native detail requires release review.
    forbidden = [str(path).encode() + b"/" for path, _ in mappings if path != temporary]
    return environment, forbidden


def check_source_paths(binary: bytes, forbidden: list[bytes]) -> None:
    require(all(prefix not in binary for prefix in forbidden))


def smoke(binary: Path, directory: Path, expected_demo: bytes) -> None:
    run = lambda *args: command([str(binary), *args], directory, env={})
    require(run("--version") == f"decant {VERSION}\n".encode())
    require(run("evidence", "demo") == expected_demo)
    require(run("evidence", "init") == regular(directory / "evidence-config.json"))
    require(run("evidence", "validate", "--config", "evidence-config.json") == b"valid: evidence configuration v1\n")
    args = ("evidence", "analyze", "--file", "trace-capture.jsonl", "--config", "evidence-config.json")
    first = run(*args)
    require(0 < len(first) <= 16385 and first == run(*args))
    write_new(directory / "report.json", first)
    require(run("evidence", "verify", "--file", "report.json").startswith(b"verified:"))
    markdown = run("evidence", "render", "--file", "report.json")
    require(0 < len(markdown) <= 65536 and b"checkout" in markdown)


def build(root: Path, output: Path, mode: str) -> None:
    require(mode in MODES and not root.is_symlink())
    root = root.resolve(strict=True)
    # Preserve every existing output, including an empty directory or symlink.
    require(not output.exists() and not output.is_symlink() and output.parent.is_dir())
    require(not any(path.is_symlink() for path in (output.parent, *output.parent.parents)))
    output = output.absolute()
    commit = command(["git", "rev-parse", "HEAD"], root).decode().strip()
    require(COMMIT.fullmatch(commit) is not None)
    clean = not command(["git", "status", "--porcelain", "--untracked-files=no"], root)
    require(not command(["git", "diff", "--name-only", "--diff-filter=U"], root))
    if mode == "public-candidate":
        require(clean)
        approve_source(root, commit)
    rustc = command(["rustc", "-vV"], root).decode()
    hosts = re.findall(r"^host: ([a-z0-9_-]+)$", rustc, re.MULTILINE)
    require(len(hosts) == 1 and hosts[0] in TARGETS)
    target = hosts[0]
    rustc_version = rustc.splitlines()[0]

    with tempfile.TemporaryDirectory(prefix="decant-binary-candidate-v1-") as temporary:
        temporary_path = Path(temporary).resolve(strict=True)
        export = temporary_path / "export"
        export_source(root, export, "public" if mode == "public-candidate" else "engineering-preview")
        inventory = regular(export / INVENTORY_NAME, 4 * MAX_SMALL)
        cut = digest(inventory)
        build_env, private_source_prefixes = rust_build_environment(root, temporary_path)
        # An explicit native target prevents inherited Cargo target selection.
        # Host/global compiler configuration is not a hermetic-build guarantee.
        subprocess.run(["python3", "tools/check_dependency_inventory_v0.py"], cwd=export, env=build_env, check=True, timeout=60)
        subprocess.run(["cargo", "build", "--locked", "--offline", "--release", "--target", target,
                        "-p", "decant-cli", "--bin", "decant"], cwd=export, env=build_env, check=True, timeout=1800)
        binary_path = temporary_path / "build" / target / "release/decant"
        binary = regular(binary_path, MAX_BINARY)
        check_source_paths(binary, private_source_prefixes)
        configuration = command([str(binary_path), "evidence", "init"], export, env={})
        lock = regular(export / "Cargo.lock")
        dependencies = regular(export / "release/third-party-dependencies-v0.tsv")
        license_present = document(inventory)["license_status"] == "approved-present"
        require(mode != "public-candidate" or license_present)
        license_name = "LICENSE" if license_present else "LICENSE-MISSING.txt"
        members = {
            "decant": binary,
            "evidence-config.json": configuration,
            "trace-capture.jsonl": regular(export / "examples/evidence/trace-capture-v1.jsonl"),
            "README.md": regular(export / "docs/release/BINARY_PACKAGE_README.md"),
            "SBOM.cdx.json": sbom(lock, dependencies, binary, commit, cut),
            "third-party-dependencies.tsv": dependencies,
            "Cargo.lock": lock,
            license_name: regular(export / license_name),
        }
        archive = package_bytes(members, target, mode)
        require(archive == package_bytes(members, target, mode))
        extracted = temporary_path / "runtime"
        extracted.mkdir()
        for name, content in unpack(archive, target, mode).items():
            write_new(extracted / name, content, executable=name == "decant")
        smoke(extracted / "decant", extracted, regular(export / "examples/evidence/report-v1.md"))
        require(not output.exists() and not output.is_symlink())
        output.mkdir()
        build_artifact(export, output, source_commit=commit)
        manifest = {
            "format": "decant-binary-candidate", "version": 1, "release_version": VERSION,
            "mode": mode, "target": target, "source_commit": commit, "source_tree_clean": clean,
            "export_inventory_sha256": cut, "cargo_lock_sha256": digest(lock),
            "rustc_version": rustc_version, "signing_status": "unsigned",
            "native_build_reproducibility": "not_established", "runtime_smoke": "passed",
            "archive": description(archive_name(target), archive),
            "members": [dict(description(name, data), mode="755" if name == "decant" else "644") for name, data in sorted(members.items())],
            "source_manifest_sha256": digest(regular(output / SOURCE_MANIFEST)),
        }
        write_new(output / archive_name(target), archive)
        write_new(output / "SHA256SUMS", f"{digest(archive)}  {archive_name(target)}\n".encode())
        write_new(output / "manifest.json", canonical(manifest))
        verify_candidate(output)


class Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise CandidateFailure


def main() -> int:
    parser = Parser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--mode", choices=sorted(MODES), default="engineering-preview")
    try:
        args = parser.parse_args()
        build(args.root, args.output, args.mode)
    except (CandidateFailure, ArtifactFailure, ApprovalFailure, ExportFailure, OSError, ValueError,
            KeyError, TypeError, subprocess.SubprocessError):
        print("binary candidate: failed (no publication performed)", file=sys.stderr)
        return 1
    print("PASS: unsigned native candidate, packaged runtime smoke and checksums; no release approval")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
