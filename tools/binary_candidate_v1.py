"""Bounded candidate packaging/verification; no signing, network or execution."""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import os
import re
import stat
import tarfile
import tomllib
from pathlib import Path
from urllib.parse import quote

if __package__:
    from tools.build_source_artifact_v0 import ARCHIVE_NAME as SOURCE_ARCHIVE
    from tools.build_source_artifact_v0 import MANIFEST_NAME as SOURCE_MANIFEST
    from tools.build_source_artifact_v0 import normalized_tar_info
    from tools.check_dependency_inventory_v0 import InventoryFailure, parse_allowlist
else:
    from build_source_artifact_v0 import ARCHIVE_NAME as SOURCE_ARCHIVE
    from build_source_artifact_v0 import MANIFEST_NAME as SOURCE_MANIFEST
    from build_source_artifact_v0 import normalized_tar_info
    from check_dependency_inventory_v0 import InventoryFailure, parse_allowlist

VERSION = "0.1.0-alpha.1"
TARGETS = {"aarch64-apple-darwin", "x86_64-unknown-linux-gnu"}
MODES = {"engineering-preview", "public-candidate"}
MAX_BINARY = 128 * 1024 * 1024
MAX_ARCHIVE = 160 * 1024 * 1024
MAX_SMALL = 1024 * 1024
MAX_MEMBERS = 8
DIGEST = re.compile(r"[0-9a-f]{64}")
COMMIT = re.compile(r"[0-9a-f]{40}")
NAME = re.compile(r"[A-Za-z0-9_-]{1,128}")
PACKAGE_VERSION = re.compile(r"[0-9][A-Za-z0-9.+_-]{0,127}")
REGISTRY = "registry+https://github.com/rust-lang/crates.io-index"
BASE_MEMBERS = {
    "decant", "evidence-config.json", "trace-capture.jsonl", "README.md",
    "SBOM.cdx.json", "third-party-dependencies.tsv", "Cargo.lock",
}
MANIFEST_KEYS = {
    "format", "version", "release_version", "mode", "target", "source_commit",
    "source_tree_clean", "export_inventory_sha256", "cargo_lock_sha256",
    "rustc_version", "signing_status", "native_build_reproducibility",
    "runtime_smoke", "archive", "members", "source_manifest_sha256",
}


class CandidateFailure(Exception):
    """Content-independent candidate failure."""


def require(condition: bool) -> None:
    if not condition:
        raise CandidateFailure


def canonical(document: object) -> bytes:
    return (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def regular(path: Path, maximum: int = MAX_SMALL) -> bytes:
    # No symlink components, including a symlinked parent output directory.
    try:
        require(not any(part.is_symlink() for part in (path, *path.parents)))
        before = path.lstat()
        require(stat.S_ISREG(before.st_mode) and 0 < before.st_size <= maximum)
        descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as handle:
            metadata = os.fstat(handle.fileno())
            require(stat.S_ISREG(metadata.st_mode) and 0 < metadata.st_size <= maximum)
            require((metadata.st_dev, metadata.st_ino) == (before.st_dev, before.st_ino))
            data = handle.read(maximum + 1)
        require(0 < len(data) <= maximum and len(data) == metadata.st_size)
        return data
    except (OSError, ValueError):
        raise CandidateFailure from None


def document(data: bytes) -> dict:
    try:
        value = json.loads(data)
        require(isinstance(value, dict) and canonical(value) == data)
        return value
    except (ValueError, RecursionError, UnicodeError):
        raise CandidateFailure from None


def description(name: str, data: bytes) -> dict:
    return {"path": name, "bytes": len(data), "sha256": digest(data)}


def archive_name(target: str) -> str:
    require(target in TARGETS)
    return f"decant-{VERSION}-{target}.tar.gz"


def member_names(mode: str, *, license_present: bool = False) -> set[str]:
    require(mode in MODES)
    return BASE_MEMBERS | {"LICENSE" if mode == "public-candidate" or license_present else "LICENSE-MISSING.txt"}


def sbom(lock_bytes: bytes, inventory: bytes, binary: bytes, commit: str, cut: str) -> bytes:
    try:
        lock = tomllib.loads(lock_bytes.decode())
        rows = parse_allowlist(inventory)
        packages = lock["package"]
        require(isinstance(packages, list) and 1 <= len(packages) <= 256)
        licenses = {(name, version): license_text for name, version, license_text, _ in rows}
        components, by_name, refs, externals = [], {}, {}, set()
        for package in packages:
            name, version = package["name"], package["version"]
            require(isinstance(name, str) and NAME.fullmatch(name) is not None)
            require(isinstance(version, str) and PACKAGE_VERSION.fullmatch(version) is not None)
            require((name, version) not in refs)
            source = package.get("source")
            require(source in {None, REGISTRY})
            ref = f"pkg:cargo/{name}@{quote(version, safe='')}" if source else f"workspace:{name}@{version}"
            component = {"type": "library", "bom-ref": ref, "name": name, "version": version}
            if source:
                checksum = package["checksum"]
                require(isinstance(checksum, str) and DIGEST.fullmatch(checksum) is not None)
                require((name, version) in licenses)
                component.update({"purl": ref, "hashes": [{"alg": "SHA-256", "content": checksum}],
                                  "properties": [{"name": "decant:declared-license", "value": licenses[name, version]}]})
                externals.add((name, version))
            components.append(component)
            refs[name, version] = ref
            by_name.setdefault(name, []).append((name, version))
        require(externals == set(licenses))
        dependencies = []
        for package in packages:
            edges = []
            for item in package.get("dependencies", []):
                parts = item.split(" ")
                require(1 <= len(parts) <= 2)
                matches = by_name.get(parts[0], []) if len(parts) == 1 else [(parts[0], parts[1])]
                require(len(matches) == 1 and matches[0] in refs)
                edges.append(refs[matches[0]])
            require(len(edges) == len(set(edges)))
            dependencies.append({"ref": refs[package["name"], package["version"]], "dependsOn": sorted(edges)})
        require(("decant-cli", VERSION) in refs)
        dependencies.append({"ref": "decant-binary", "dependsOn": [refs["decant-cli", VERSION]]})
        result = {
            "bomFormat": "CycloneDX", "specVersion": "1.6", "version": 1,
            "metadata": {
                "lifecycles": [{"phase": "build"}],
                "component": {"type": "application", "bom-ref": "decant-binary", "name": "decant", "version": VERSION,
                              "hashes": [{"alg": "SHA-256", "content": digest(binary)}]},
                "properties": [
                    {"name": "decant:source-commit", "value": commit},
                    {"name": "decant:export-inventory-sha256", "value": cut},
                    {"name": "decant:inventory-scope", "value": "All locked Cargo packages, including dev and target-specific dependencies; source archive checksums, not proof of linked components."},
                    {"name": "decant:inventory-gaps", "value": "Bundled native projects and host/system libraries require separate review. No license compatibility or vulnerability conclusion."},
                ],
            },
            "components": sorted(components, key=lambda item: item["bom-ref"]),
            "dependencies": sorted(dependencies, key=lambda item: item["ref"]),
            "compositions": [{"aggregate": "incomplete", "assemblies": ["decant-binary"]}],
        }
        raw = canonical(result)
        require(len(raw) <= MAX_SMALL)
        return raw
    except (KeyError, TypeError, ValueError, UnicodeError, InventoryFailure):
        raise CandidateFailure from None


def package_bytes(members: dict[str, bytes], target: str, mode: str) -> bytes:
    require(set(members) == member_names(mode, license_present="LICENSE" in members))
    prefix = archive_name(target)[:-7]
    buffer = io.BytesIO()
    with gzip.GzipFile(filename="", fileobj=buffer, mode="wb", mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode="w|", format=tarfile.USTAR_FORMAT) as archive:
            for name, data in sorted(members.items()):
                require(0 < len(data) <= (MAX_BINARY if name == "decant" else MAX_SMALL))
                info = normalized_tar_info(f"{prefix}/{name}", len(data), 0o755 if name == "decant" else 0o644)
                archive.addfile(info, io.BytesIO(data))
    result = buffer.getvalue()
    require(len(result) <= MAX_ARCHIVE)
    return result


def unpack(data: bytes, target: str, mode: str) -> dict[str, bytes]:
    require(0 < len(data) <= MAX_ARCHIVE)
    prefix = archive_name(target)[:-7] + "/"
    result = {}
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(data), mode="rb") as archive:
            # Parse bounded USTAR headers directly with the standard-library
            # parser, before tarfile can expand PAX/GNU extension records.
            while True:
                header = archive.read(512)
                require(len(header) == 512)
                if header == bytes(512):
                    require(archive.read(512) == bytes(512))
                    padding = archive.read(10241)
                    require(len(padding) <= 10240 and not any(padding))
                    break
                info = tarfile.TarInfo.frombuf(header, "utf-8", "strict")
                require(len(result) < MAX_MEMBERS and info.name.startswith(prefix))
                name = info.name[len(prefix):]
                allowed = member_names(mode) | member_names(mode, license_present=True)
                require(name in allowed and name not in result and info.isfile())
                require(info.mode == (0o755 if name == "decant" else 0o644))
                require(info.uid == info.gid == info.mtime == 0 and info.uname == info.gname == "")
                maximum = MAX_BINARY if name == "decant" else MAX_SMALL
                require(0 < info.size <= maximum)
                content = archive.read(info.size)
                require(len(content) == info.size)
                padding = archive.read((-info.size) % 512)
                require(len(padding) == (-info.size) % 512 and not any(padding))
                result[name] = content
    except (OSError, tarfile.TarError, EOFError, ValueError, UnicodeError):
        raise CandidateFailure from None
    require(set(result) == member_names(mode, license_present="LICENSE" in result))
    return result


def verify_candidate(directory: Path) -> tuple[dict, dict[str, bytes]]:
    """Verify matching bytes/shape only. Never execute an untrusted candidate."""
    manifest = document(regular(directory / "manifest.json"))
    require(set(manifest) == MANIFEST_KEYS)
    require(manifest["format"] == "decant-binary-candidate" and type(manifest["version"]) is int and manifest["version"] == 1)
    require(manifest["release_version"] == VERSION and manifest["mode"] in MODES)
    require(manifest["signing_status"] == "unsigned" and manifest["native_build_reproducibility"] == "not_established")
    require(manifest["runtime_smoke"] == "passed" and type(manifest["source_tree_clean"]) is bool)
    require(isinstance(manifest["source_commit"], str) and COMMIT.fullmatch(manifest["source_commit"]) is not None)
    for key in ["export_inventory_sha256", "cargo_lock_sha256", "source_manifest_sha256"]:
        require(isinstance(manifest[key], str) and DIGEST.fullmatch(manifest[key]) is not None)
    require(isinstance(manifest["rustc_version"], str) and re.fullmatch(r"rustc [0-9][A-Za-z0-9. ()_-]{0,127}", manifest["rustc_version"]) is not None)
    target = manifest["target"]
    require(isinstance(target, str) and target in TARGETS)
    name = archive_name(target)
    archive = regular(directory / name, MAX_ARCHIVE)
    require(manifest["archive"] == description(name, archive))
    require(regular(directory / "SHA256SUMS") == f"{digest(archive)}  {name}\n".encode())
    members = unpack(archive, target, manifest["mode"])
    expected = [dict(description(name, data), mode="755" if name == "decant" else "644") for name, data in sorted(members.items())]
    require(manifest["members"] == expected)
    require(digest(members["Cargo.lock"]) == manifest["cargo_lock_sha256"])
    require(members["SBOM.cdx.json"] == sbom(members["Cargo.lock"], members["third-party-dependencies.tsv"], members["decant"], manifest["source_commit"], manifest["export_inventory_sha256"]))
    source_raw = regular(directory / SOURCE_MANIFEST)
    require(digest(source_raw) == manifest["source_manifest_sha256"])
    source = document(source_raw)
    require(set(source) == {"format", "version", "release_version", "source_commit", "signed", "license_status", "archive", "source_inventory_sha256"})
    require(source["format"] == "decant-source-release-artifact" and type(source["version"]) is int and source["version"] == 0)
    require(source["release_version"] == VERSION)
    require(source["source_commit"] == manifest["source_commit"] and source["signed"] is False)
    require(source["source_inventory_sha256"] == manifest["export_inventory_sha256"])
    require(source["license_status"] == ("approved-present" if "LICENSE" in members else "missing"))
    source_bytes = regular(directory / SOURCE_ARCHIVE, 64 * 1024 * 1024)
    require(source["archive"] == description(SOURCE_ARCHIVE, source_bytes))
    require(regular(directory / (SOURCE_ARCHIVE + ".sha256")) == f"{digest(source_bytes)}  {SOURCE_ARCHIVE}\n".encode())
    return manifest, members
