from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.export_public_source_v0 import ExportFailure, export_source, is_allowed, parse_allowlist


class PublicSourceExportTests(unittest.TestCase):
    def test_actual_allowlist_keeps_product_files_and_excludes_private_records(self) -> None:
        project = Path(__file__).resolve().parents[2]
        entries = parse_allowlist(project / "release/public-source-allowlist-v0.tsv")
        for path in [
            "LICENSE", "LICENSING.md", "LICENSES/Apache-2.0.txt", "Cargo.lock",
            "crates/decant-cli/src/main.rs", "crates/decant-state/src/lib.rs",
            "crates/decant-state/tests/semantic_admission_v0.rs",
            "tools/evidence_evaluation/fixtures.py", "tools/tests/test_binary_candidate_v1.py",
            "docs/release/EVIDENCE_QUICKSTART.md", "docs/release/KNOWN_LIMITATIONS.md",
            "docs/release/KAFKA_EVIDENCE.md", "release/third-party-dependencies-v0.tsv",
            ".github/workflows/ci.yml",
        ]:
            self.assertTrue(is_allowed(path, entries), path)
        for path in [
            "AGENTS.md", ".agents/skills/example/SKILL.md", "README_FIRST.md",
            "crates/decant-state/AGENTS.override.md", "tools/evidence_evaluation/AGENTS.override.md",
            "docs/adr/example.md", "docs/tasks/example.md", "docs/dependencies/example.md",
            "docs/source/initial-blueprint.pdf", "docs/reviews/example.md",
            "docs/release/ALPHA_ACTIVATION.md", "docs/release/RELEASE_CHECKLIST.md",
            "docs/release/SOURCE_ONLY_PUBLICATION.md", "docs/release/private-future-note.md",
            "release/public-release-approval-v0.example.json",
            "release/evidence-alpha-approval-v1.example.json",
            "release/public-release-approval-v0.json", "release/evidence-alpha-approval-v1.json",
            "decant-kafka.json", "mcp-policy.json", "decant_output.json",
        ]:
            self.assertFalse(is_allowed(path, entries), path)

    def test_nested_instruction_files_fail_before_export_even_under_broad_tree(self) -> None:
        for name in ["AGENTS.md", "AGENTS.override.md", "SKILL.md"]:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "root"
                root.mkdir()
                allowlist, paths = self.fixture(root)
                relative = "crates/demo/" + name
                (root / relative).write_text("synthetic private instructions\n")
                destination = Path(directory) / "export"
                with self.assertRaises(ExportFailure):
                    export_source(root, destination, "engineering-preview",
                                  allowlist_path=allowlist, source_paths=paths + [relative])
                self.assertFalse(destination.exists())

    def test_public_export_does_not_copy_private_authorization_input(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "root"
            root.mkdir()
            allowlist, paths = self.fixture(root)
            (root / "LICENSE").write_text("Synthetic test license only.\n")
            approval = "release/public-release-approval-v0.json"
            (root / approval).write_text('{"synthetic_test_only":true}\n')
            destination = Path(directory) / "export"
            # Full approval semantics are checked by the release preflight;
            # this test covers only the exporter boundary, not authorization.
            export_source(root, destination, "public", allowlist_path=allowlist, source_paths=paths)
            self.assertTrue((destination / "LICENSE").is_file())
            self.assertFalse((destination / approval).exists())
            inventory = json.loads((destination / "PUBLIC-SOURCE-INVENTORY-v0.json").read_bytes())
            self.assertNotIn(approval, [item["path"] for item in inventory["files"]])
            with self.assertRaises(ExportFailure):
                export_source(root, Path(directory) / "tracked-private", "public",
                              allowlist_path=allowlist, source_paths=paths + [approval])
            self.assertFalse((Path(directory) / "tracked-private").exists())

    def fixture(self, root: Path) -> tuple[Path, list[str]]:
        files = {
            ".gitignore": "target/\n",
            "CHANGELOG.md": "alpha\n",
            "Cargo.lock": "version = 4\n",
            "Cargo.toml": "[workspace]\n",
            "README.md": "preview\n",
            "SECURITY.md": "policy\n",
            "rust-toolchain.toml": "[toolchain]\n",
            "crates/demo/src/lib.rs": "pub fn demo() {}\n",
            "release/data.txt": "release-data\n",
        }
        for relative, content in files.items():
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content, encoding="utf-8")
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
            "tree\trelease/\n",
            encoding="utf-8",
        )
        return allowlist, sorted(files)

    def test_engineering_export_is_allowlisted_normalized_and_license_marked(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "root"
            root.mkdir()
            allowlist, paths = self.fixture(root)
            (root / "crates/demo/untracked.txt").write_text("not selected", encoding="utf-8")
            destination = Path(directory) / "export"
            export_source(
                root,
                destination,
                "engineering-preview",
                allowlist_path=allowlist,
                source_paths=paths,
            )
            self.assertFalse((destination / "crates/demo/untracked.txt").exists())
            self.assertTrue((destination / "LICENSE-MISSING.txt").is_file())
            inventory = json.loads(
                (destination / "PUBLIC-SOURCE-INVENTORY-v0.json").read_text(encoding="utf-8")
            )
            self.assertEqual(inventory["license_status"], "missing")
            self.assertEqual(
                [item["path"] for item in inventory["files"]],
                sorted(paths + ["LICENSE-MISSING.txt"]),
            )

    def test_public_mode_refuses_missing_gates_before_creating_destination(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "root"
            root.mkdir()
            allowlist, paths = self.fixture(root)
            destination = Path(directory) / "export"
            with self.assertRaises(ExportFailure) as raised:
                export_source(
                    root,
                    destination,
                    "public",
                    allowlist_path=allowlist,
                    source_paths=paths,
                )
            self.assertEqual(raised.exception.category, "public-license")
            self.assertFalse(destination.exists())

    def test_licensed_engineering_export_keeps_all_terms_without_public_approval(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "root"
            root.mkdir()
            allowlist, paths = self.fixture(root)
            project = Path(__file__).resolve().parents[2]
            license_paths = ["LICENSE", "LICENSING.md", "LICENSES/Apache-2.0.txt"]
            for relative in license_paths:
                path = root / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes((project / relative).read_bytes())
            rows = allowlist.read_text().splitlines() + ["file\t" + path for path in license_paths]
            allowlist.write_text("\n".join(sorted(rows)) + "\n")
            paths += license_paths
            destination = Path(directory) / "export"
            export_source(root, destination, "engineering-preview", allowlist_path=allowlist, source_paths=paths)
            self.assertFalse((destination / "LICENSE-MISSING.txt").exists())
            inventory = json.loads((destination / "PUBLIC-SOURCE-INVENTORY-v0.json").read_bytes())
            self.assertEqual(inventory["license_status"], "approved-present")
            for relative in license_paths:
                self.assertEqual((destination / relative).read_bytes(), (project / relative).read_bytes())
            with self.assertRaises(ExportFailure) as raised:
                export_source(root, Path(directory) / "public", "public", allowlist_path=allowlist, source_paths=paths)
            self.assertEqual(raised.exception.category, "public-approval")
            self.assertFalse((Path(directory) / "public").exists())

    def test_symlink_nonempty_destination_and_unbounded_file_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "root"
            root.mkdir()
            allowlist, paths = self.fixture(root)
            target = root / "crates/demo/src/lib.rs"
            target.unlink()
            target.symlink_to(root / "README.md")
            with self.assertRaises(ExportFailure):
                export_source(
                    root,
                    Path(directory) / "symlink-export",
                    "engineering-preview",
                    allowlist_path=allowlist,
                    source_paths=paths,
                )
            target.unlink()
            target.write_bytes(b"x" * (1024 * 1024 + 1))
            with self.assertRaises(ExportFailure):
                export_source(
                    root,
                    Path(directory) / "large-export",
                    "engineering-preview",
                    allowlist_path=allowlist,
                    source_paths=paths,
                )
            destination = Path(directory) / "nonempty"
            destination.mkdir()
            (destination / "owned.txt").write_text("keep", encoding="utf-8")
            with self.assertRaises(ExportFailure):
                export_source(
                    root,
                    destination,
                    "engineering-preview",
                    allowlist_path=allowlist,
                    source_paths=paths,
                )
            self.assertEqual((destination / "owned.txt").read_text(encoding="utf-8"), "keep")

    def test_traversal_duplicate_forbidden_and_missing_required_allowlists_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "root"
            root.mkdir()
            allowlist, paths = self.fixture(root)
            cases = [
                allowlist.read_text(encoding="utf-8") + "file\t../escape\n",
                allowlist.read_text(encoding="utf-8") + "file\tREADME.md\n",
                allowlist.read_text(encoding="utf-8") + "tree\tdocs/reviews/\n",
                allowlist.read_text(encoding="utf-8").replace("file\tREADME.md\n", ""),
            ]
            for index, content in enumerate(cases):
                changed = root / f"allowlist-{index}.tsv"
                changed.write_text(content, encoding="utf-8")
                with self.assertRaises(ExportFailure):
                    export_source(
                        root,
                        Path(directory) / f"export-{index}",
                        "engineering-preview",
                        allowlist_path=changed,
                        source_paths=paths,
                    )


if __name__ == "__main__":
    unittest.main()
