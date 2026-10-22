from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.check_repository_hygiene_v0 import HygieneFailure, scan_files


class RepositoryHygieneTests(unittest.TestCase):
    def test_nested_instructions_and_internal_release_documents_are_not_public(self) -> None:
        for relative in [
            "crates/example/AGENTS.md", "crates/example/AGENTS.override.md",
            "tools/example/SKILL.md", "docs/release/ALPHA_ACTIVATION.md",
            "docs/release/RELEASE_CHECKLIST.md", "docs/release/SOURCE_ONLY_PUBLICATION.md",
            "release/public-release-approval-v0.example.json",
            "release/evidence-alpha-approval-v1.example.json",
            "release/public-release-approval-v0.json",
            "release/evidence-alpha-approval-v1.json",
            "release/kafka-mcp-pilot-evidence-v0.json",
        ]:
            with self.subTest(path=relative), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                path = root / relative
                path.parent.mkdir(parents=True)
                path.write_text("synthetic private record\n")
                with self.assertRaises(HygieneFailure) as raised:
                    scan_files(root, [relative], "public-export")
                self.assertEqual(raised.exception.rule, "forbidden-public-path")

    def assert_rule(self, content: bytes, expected: str) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "fixture.txt").write_bytes(content)
            with self.assertRaises(HygieneFailure) as raised:
                scan_files(root, ["fixture.txt"], "internal")
            self.assertEqual(raised.exception.rule, expected)
            self.assertNotIn("DISTINCTIVE", str(raised.exception))

    def test_high_confidence_credential_signatures_fail_without_value_echo(self) -> None:
        self.assert_rule(
            b"-----BEGIN " + b"PRIVATE KEY-----\nDISTINCTIVE\n",
            "private-key",
        )
        self.assert_rule(b"gh" + b"p_" + b"A" * 40 + b"DISTINCTIVE", "github-token")
        self.assert_rule(b"AK" + b"IA" + b"A" * 16 + b"DISTINCTIVE", "aws-access-key")
        self.assert_rule(
            b"pass" + b"word=" + b"'DISTINCTIVE-CREDENTIAL'",
            "credential-assignment",
        )

    def test_ordinary_source_and_placeholder_examples_pass(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            content = b"token_count = 42\npassword = '<set-me>'\nsource_id = 'synthetic'\n"
            (root / "ordinary.txt").write_bytes(content)
            scan_files(root, ["ordinary.txt"], "internal")

    def test_paths_duplicates_symlinks_sizes_and_public_boundaries_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "safe.txt").write_text("safe", encoding="utf-8")
            for paths in [["../safe.txt"], ["safe.txt", "safe.txt"], []]:
                with self.assertRaises(HygieneFailure):
                    scan_files(root, paths, "internal")
            (root / "large.txt").write_bytes(b"x" * (1024 * 1024 + 1))
            with self.assertRaises(HygieneFailure):
                scan_files(root, ["large.txt"], "internal")
            (root / "link.txt").symlink_to(root / "safe.txt")
            with self.assertRaises(HygieneFailure):
                scan_files(root, ["link.txt"], "internal")
            (root / "AGENTS.md").write_text("internal", encoding="utf-8")
            with self.assertRaises(HygieneFailure) as raised:
                scan_files(root, ["AGENTS.md"], "public-export")
            self.assertEqual(raised.exception.rule, "forbidden-public-path")


if __name__ == "__main__":
    unittest.main()
