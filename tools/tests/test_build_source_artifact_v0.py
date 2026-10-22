from __future__ import annotations

import gzip
import io
import json
import os
import tarfile
import tempfile
import unittest
from pathlib import Path

from tools.build_source_artifact_v0 import (
    ARCHIVE_NAME,
    ARCHIVE_PREFIX,
    CHECKSUM_NAME,
    MANIFEST_NAME,
    ArtifactFailure,
    build_artifact,
)
from tools.export_public_source_v0 import export_source


class SourceArtifactTests(unittest.TestCase):
    def export_fixture(self, directory: str, *, licensed: bool = False) -> Path:
        root = Path(directory) / "root"
        root.mkdir()
        files = {
            ".gitignore": "target/\n",
            "CHANGELOG.md": "alpha\n",
            "Cargo.lock": "version = 4\n",
            "Cargo.toml": "[workspace]\n",
            "README.md": "preview\n",
            "SECURITY.md": "policy\n",
            "rust-toolchain.toml": "[toolchain]\n",
            "crates/demo/src/lib.rs": "pub fn demo() {}\n",
        }
        for relative, content in files.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
        executable = root / "scripts/demo.sh"
        executable.parent.mkdir()
        executable.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        os.chmod(executable, 0o755)
        files["scripts/demo.sh"] = executable.read_text(encoding="utf-8")
        allowlist = root / "allowlist.tsv"
        allowlist.write_text(
            "file\t.gitignore\n"
            "file\tCHANGELOG.md\n"
            "file\tCargo.lock\n"
            "file\tCargo.toml\n"
            "file\tREADME.md\n"
            "file\tSECURITY.md\n"
            "file\trust-toolchain.toml\n"
            "tree\tcrates/\n"
            "tree\tscripts/\n",
            encoding="utf-8",
        )
        if licensed:
            files["LICENSE"] = "Synthetic test license only.\n"
            (root / "LICENSE").write_text(files["LICENSE"])
            allowlist.write_text("\n".join(sorted(allowlist.read_text().splitlines() + ["file\tLICENSE"])) + "\n")
        export = Path(directory) / "export"
        export_source(
            root,
            export,
            "engineering-preview",
            allowlist_path=allowlist,
            source_paths=sorted(files),
        )
        return export

    def test_licensed_preview_archives_preserve_license_and_reproduce(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            export = self.export_fixture(directory, licensed=True)
            first, second = Path(directory) / "first", Path(directory) / "second"
            build_artifact(export, first)
            build_artifact(export, second)
            for name in (ARCHIVE_NAME, CHECKSUM_NAME, MANIFEST_NAME):
                self.assertEqual((first / name).read_bytes(), (second / name).read_bytes())
            manifest = json.loads((first / MANIFEST_NAME).read_bytes())
            self.assertEqual(manifest["license_status"], "approved-present")
            self.assertFalse(manifest["signed"])
            with tarfile.open(first / ARCHIVE_NAME, "r:gz") as archive:
                self.assertEqual(archive.extractfile(f"{ARCHIVE_PREFIX}/LICENSE").read(), b"Synthetic test license only.\n")
                self.assertNotIn(f"{ARCHIVE_PREFIX}/LICENSE-MISSING.txt", archive.getnames())

    def test_independent_builds_are_identical_and_metadata_is_normalized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            export = self.export_fixture(directory)
            first = Path(directory) / "first"
            second = Path(directory) / "second"
            commit = "a" * 40
            build_artifact(export, first, source_commit=commit)
            build_artifact(export, second, source_commit=commit)
            for name in [ARCHIVE_NAME, CHECKSUM_NAME, MANIFEST_NAME]:
                self.assertEqual((first / name).read_bytes(), (second / name).read_bytes())

            inventory = json.loads(
                (export / "PUBLIC-SOURCE-INVENTORY-v0.json").read_text(encoding="utf-8")
            )
            expected = sorted(
                [f"{ARCHIVE_PREFIX}/{item['path']}" for item in inventory["files"]]
                + [f"{ARCHIVE_PREFIX}/PUBLIC-SOURCE-INVENTORY-v0.json"]
            )
            with gzip.open(first / ARCHIVE_NAME, "rb") as compressed:
                with tarfile.open(fileobj=io.BytesIO(compressed.read()), mode="r:") as archive:
                    members = archive.getmembers()
                    self.assertEqual([member.name for member in members], expected)
                    self.assertTrue(all(member.isfile() for member in members))
                    self.assertTrue(all(member.uid == 0 and member.gid == 0 for member in members))
                    self.assertTrue(all(member.uname == "" and member.gname == "" for member in members))
                    self.assertTrue(all(member.mtime == 0 for member in members))
            manifest = json.loads((first / MANIFEST_NAME).read_text(encoding="utf-8"))
            self.assertEqual(manifest["source_commit"], commit)
            self.assertEqual(manifest["license_status"], "missing")
            self.assertFalse(manifest["signed"])

    def test_missing_inventory_and_content_mismatch_fail_before_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            export = self.export_fixture(directory)
            inventory = export / "PUBLIC-SOURCE-INVENTORY-v0.json"
            inventory.unlink()
            output = Path(directory) / "missing"
            with self.assertRaises(ArtifactFailure):
                build_artifact(export, output)
            self.assertFalse(output.exists())

        with tempfile.TemporaryDirectory() as directory:
            export = self.export_fixture(directory)
            (export / "README.md").write_text("changed\n", encoding="utf-8")
            output = Path(directory) / "changed"
            with self.assertRaises(ArtifactFailure) as raised:
                build_artifact(export, output)
            self.assertEqual(raised.exception.category, "source-content-mismatch")
            self.assertFalse(output.exists())

    def test_symlink_extra_file_nonempty_output_and_invalid_commit_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            export = self.export_fixture(directory)
            (export / "extra").symlink_to(export / "README.md")
            with self.assertRaises(ArtifactFailure):
                build_artifact(export, Path(directory) / "symlink")
            (export / "extra").unlink()
            (export / "extra").write_text("unexpected", encoding="utf-8")
            with self.assertRaises(ArtifactFailure) as raised:
                build_artifact(export, Path(directory) / "extra-output")
            self.assertEqual(raised.exception.category, "source-inventory-mismatch")

            output = Path(directory) / "nonempty"
            output.mkdir()
            (output / "keep").write_text("keep", encoding="utf-8")
            with self.assertRaises(ArtifactFailure):
                build_artifact(export, output)
            self.assertEqual((output / "keep").read_text(encoding="utf-8"), "keep")
            with self.assertRaises(ArtifactFailure) as raised:
                build_artifact(export, Path(directory) / "commit", source_commit="A" * 40)
            self.assertEqual(raised.exception.category, "source-commit")

    def test_noncanonical_inventory_and_escaping_member_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            export = self.export_fixture(directory)
            inventory_path = export / "PUBLIC-SOURCE-INVENTORY-v0.json"
            document = json.loads(inventory_path.read_text(encoding="utf-8"))
            document["files"][0]["path"] = "../escape"
            inventory_path.write_text(
                json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n",
                encoding="utf-8",
            )
            with self.assertRaises(ArtifactFailure) as raised:
                build_artifact(export, Path(directory) / "escape")
            self.assertEqual(raised.exception.category, "inventory-path")


if __name__ == "__main__":
    unittest.main()
