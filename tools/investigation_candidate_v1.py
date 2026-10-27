#!/usr/bin/env python3
"""Build/verify a local unsigned investigation-only candidate; no publication."""
from __future__ import annotations

import argparse
import re
import stat
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path

sys.dont_write_bytecode = True
if __package__:
    from tools.binary_candidate_v1 import (CandidateFailure, COMMIT, TARGETS, VERSION, MAX_BINARY,
        MAX_SMALL, canonical, description, digest, document, regular, require, sbom)
    from tools.build_binary_candidate_v1 import command, write_new, rust_build_environment, check_source_paths
    from tools.build_source_artifact_v0 import ArtifactFailure, build_artifact, ARCHIVE_NAME, MANIFEST_NAME, CHECKSUM_NAME
    from tools.export_public_source_v0 import ExportFailure, INVENTORY_NAME, export_source
else:
    from binary_candidate_v1 import (CandidateFailure, COMMIT, TARGETS, VERSION, MAX_BINARY,
        MAX_SMALL, canonical, description, digest, document, regular, require, sbom)
    from build_binary_candidate_v1 import command, write_new, rust_build_environment, check_source_paths
    from build_source_artifact_v0 import ArtifactFailure, build_artifact, ARCHIVE_NAME, MANIFEST_NAME, CHECKSUM_NAME
    from export_public_source_v0 import ExportFailure, INVENTORY_NAME, export_source

PAYLOAD = {"decant", "README.md", "LICENSE", "Cargo.lock", "third-party-dependencies.tsv",
           "SBOM.cdx.json", "build-dependencies.txt", ARCHIVE_NAME, MANIFEST_NAME, CHECKSUM_NAME}
KEYS = {"format", "version", "release_version", "source_commit", "source_tree_clean", "target",
        "rustc_version", "build_flavor", "default_features", "features", "signing_status",
        "native_build_reproducibility", "runtime_smoke", "export_inventory_sha256", "files"}


def graph_bytes(raw):
    require(len(raw) <= MAX_SMALL)
    rows = set()
    for line in raw.decode().splitlines():
        match = re.fullmatch(r"([a-zA-Z0-9_-]+) v([0-9][a-zA-Z0-9_.+\-]*)(?: \(.*\))?", line)
        require(match is not None)
        name, version = match.groups()
        require(not name.startswith(("rdkafka", "decant-kafka")) and name not in ("rmcp", "rmcp-macros", "decant-mcp"))
        rows.add(f"{name} v{version}")
    require(3 <= len(rows) <= 256)
    require(f"decant-cli v{VERSION}" in rows and "decant-backend v0.0.0" in rows
            and "decant-compiler v0.0.0" in rows)
    return ("\n".join(sorted(rows)) + "\n").encode()


def build_graph(root, target):
    return graph_bytes(command(["cargo", "tree", "--locked", "--offline", "-p", "decant-cli",
                               "--no-default-features", "--target", target, "--edges", "normal,build",
                               "--prefix", "none", "--format", "{p}"], root))


def host(root):
    rustc = command(["rustc", "-vV"], root).decode()
    hosts = re.findall(r"^host: ([a-z0-9_-]+)$", rustc, re.MULTILINE)
    require(len(hosts) == 1 and hosts[0] in TARGETS)
    return hosts[0], rustc.splitlines()[0]


def checksums(files):
    return "".join(f"{digest(data)}  {name}\n" for name, data in sorted(files.items())).encode()


def manifest_for(files, commit, clean, target, rustc, cut):
    require(set(files) == PAYLOAD)
    return {"format": "decant-investigation-candidate", "version": 1, "release_version": VERSION,
            "source_commit": commit, "source_tree_clean": clean, "target": target,
            "rustc_version": rustc, "build_flavor": "investigation-only",
            "default_features": False, "features": [], "signing_status": "unsigned",
            "native_build_reproducibility": "not_established", "runtime_smoke": "help_passed",
            "export_inventory_sha256": cut,
            "files": [dict(description(name, data), mode="755" if name == "decant" else "644")
                      for name, data in sorted(files.items())]}


def verify(directory):
    require(not directory.is_symlink())
    entries = set()
    for entry in directory.iterdir():
        entries.add(entry.name)
        require(len(entries) <= len(PAYLOAD) + 2)
    require(entries == PAYLOAD | {"manifest.json", "SHA256SUMS"})
    manifest_raw = regular(directory / "manifest.json")
    manifest = document(manifest_raw)
    require(set(manifest) == KEYS)
    require(manifest["format"] == "decant-investigation-candidate"
            and type(manifest["version"]) is int and manifest["version"] == 1
            and manifest["release_version"] == VERSION)
    require(manifest["build_flavor"] == "investigation-only" and manifest["default_features"] is False
            and manifest["features"] == [] and manifest["signing_status"] == "unsigned"
            and manifest["native_build_reproducibility"] == "not_established"
            and manifest["runtime_smoke"] == "help_passed")
    require(type(manifest["source_tree_clean"]) is bool and manifest["target"] in TARGETS)
    require(isinstance(manifest["source_commit"], str) and COMMIT.fullmatch(manifest["source_commit"]) is not None)
    require(isinstance(manifest["rustc_version"], str)
            and re.fullmatch(r"rustc [0-9][A-Za-z0-9. ()_-]{0,127}", manifest["rustc_version"]) is not None)
    cut = manifest["export_inventory_sha256"]
    require(isinstance(cut, str) and re.fullmatch(r"[0-9a-f]{64}", cut) is not None)
    require(isinstance(manifest["files"], list) and len(manifest["files"]) == len(PAYLOAD))
    for item in manifest["files"]:
        require(isinstance(item, dict) and set(item) == {"path", "bytes", "sha256", "mode"})
        require(type(item["bytes"]) is int)
    files = {}
    for name in sorted(PAYLOAD):
        maximum = MAX_BINARY if name == "decant" else 64 * MAX_SMALL if name == ARCHIVE_NAME else MAX_SMALL
        files[name] = regular(directory / name, maximum)
        require(stat.S_IMODE(directory.joinpath(name).stat().st_mode) == (0o755 if name == "decant" else 0o644))
    require(manifest == manifest_for(files, manifest["source_commit"], manifest["source_tree_clean"],
                                    manifest["target"], manifest["rustc_version"], cut))
    require(regular(directory / "SHA256SUMS") == checksums({**files, "manifest.json": manifest_raw}))
    require(graph_bytes(files["build-dependencies.txt"]) == files["build-dependencies.txt"])
    lock = tomllib.loads(files["Cargo.lock"].decode())
    locked = {f'{p["name"]} v{p["version"]}' for p in lock["package"]}
    require(set(files["build-dependencies.txt"].decode().splitlines()) <= locked)
    require(files["SBOM.cdx.json"] == sbom(files["Cargo.lock"], files["third-party-dependencies.tsv"],
                                         files["decant"], manifest["source_commit"], cut))
    source = document(files[MANIFEST_NAME])
    require(set(source) == {"format", "version", "release_version", "source_commit", "signed",
                           "license_status", "archive", "source_inventory_sha256"})
    require(source["format"] == "decant-source-release-artifact" and type(source["version"]) is int
            and source["version"] == 0 and source["release_version"] == VERSION
            and source["source_commit"] == manifest["source_commit"] and source["signed"] is False
            and source["source_inventory_sha256"] == cut and source["license_status"] == "approved-present")
    require(isinstance(source["archive"], dict) and type(source["archive"].get("bytes")) is int)
    require(source["archive"] == description(ARCHIVE_NAME, files[ARCHIVE_NAME]))
    require(files[CHECKSUM_NAME] == checksums({ARCHIVE_NAME: files[ARCHIVE_NAME]}))
    return manifest


def build(root, output):
    require(not root.is_symlink())
    root = root.resolve(strict=True)
    require(not output.exists() and not output.is_symlink() and output.parent.is_dir())
    require(not any(p.is_symlink() for p in (output.parent, *output.parent.parents)))
    output = output.absolute()
    commit = command(["git", "rev-parse", "HEAD"], root).decode().strip()
    require(COMMIT.fullmatch(commit) is not None)
    clean = not command(["git", "status", "--porcelain", "--untracked-files=no"], root)
    require(not command(["git", "diff", "--name-only", "--diff-filter=U"], root))
    target, rustc = host(root)
    with tempfile.TemporaryDirectory(prefix="decant-investigation-candidate-") as temporary:
        temporary = Path(temporary).resolve(strict=True)
        export = temporary / "export"
        export_source(root, export, "engineering-preview")
        inventory = regular(export / INVENTORY_NAME, 4 * MAX_SMALL)
        require(document(inventory)["license_status"] == "approved-present")
        cut = digest(inventory)
        graph = build_graph(export, target)
        environment, prefixes = rust_build_environment(root, temporary)
        subprocess.run(["cargo", "build", "--locked", "--offline", "--release", "--target", target,
                        "-p", "decant-cli", "--bin", "decant", "--no-default-features"],
                       cwd=export, env=environment, check=True, timeout=1800)
        binary = regular(temporary / "build" / target / "release/decant", MAX_BINARY)
        check_source_paths(binary, prefixes)
        runtime = temporary / "runtime"
        runtime.mkdir()
        write_new(runtime / "decant", binary, executable=True)
        for args in [("--help",), ("--version",), ("connect", "grafana", "--help"),
                     ("doctor", "--help"), ("investigate", "--help")]:
            text = command([str(runtime / "decant"), *args], runtime, env={}, maximum=65536)
            require(bool(text))
            if args == ("--help",):
                require(b"investigation-only build" in text)
            if args == ("--version",):
                require(text == f"decant {VERSION}\n".encode())
        require(not output.exists() and not output.is_symlink())
        build_artifact(export, output, source_commit=commit)
        lock = regular(export / "Cargo.lock")
        dependencies = regular(export / "release/third-party-dependencies-v0.tsv")
        files = {"decant": binary, "Cargo.lock": lock, "third-party-dependencies.tsv": dependencies,
                 "build-dependencies.txt": graph, "LICENSE": regular(export / "LICENSE"),
                 "README.md": regular(export / "docs/release/INVESTIGATION_BINARY.md"),
                 "SBOM.cdx.json": sbom(lock, dependencies, binary, commit, cut)}
        for name, data in files.items():
            write_new(output / name, data, executable=name == "decant")
        for name in (ARCHIVE_NAME, MANIFEST_NAME, CHECKSUM_NAME):
            files[name] = regular(output / name, 64 * MAX_SMALL)
        manifest = canonical(manifest_for(files, commit, clean, target, rustc, cut))
        write_new(output / "manifest.json", manifest)
        write_new(output / "SHA256SUMS", checksums({**files, "manifest.json": manifest}))
        verify(output)


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise CandidateFailure


def main():
    parser = Parser(description=__doc__)
    parser.add_argument("command", choices=("build", "verify", "graph"))
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--output", type=Path)
    try:
        args = parser.parse_args()
        if args.command == "graph":
            require(args.output is None)
            graph = build_graph(args.root, host(args.root)[0])
            sys.stdout.buffer.write(graph)
        else:
            require(args.output is not None)
            if args.command == "build":
                build(args.root, args.output)
            else:
                verify(args.output)
            print("PASS: investigation candidate consistency; unsigned, not authenticated or release-approved")
    except (CandidateFailure, ArtifactFailure, ExportFailure, OSError, ValueError, KeyError,
            TypeError, RecursionError, subprocess.SubprocessError):
        print("investigation candidate: failed (no publication performed)", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
