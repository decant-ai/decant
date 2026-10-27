"""Candidate consistency tests with non-executable bytes, never release approvals."""
import copy
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from tools import investigation_candidate_v1 as c

ROOT = Path(__file__).resolve().parents[2]


def fixture(directory, clean=True):
    commit, cut = "1" * 40, "2" * 64
    binary = b"SYNTHETIC NON-EXECUTABLE BINARY"
    lock = (ROOT / "Cargo.lock").read_bytes()
    inventory = (ROOT / "release/third-party-dependencies-v0.tsv").read_bytes()
    source = b"SYNTHETIC SOURCE ARTIFACT"
    source_manifest = c.canonical({
        "format": "decant-source-release-artifact", "version": 0, "release_version": c.VERSION,
        "source_commit": commit, "signed": False, "license_status": "approved-present",
        "archive": c.description(c.ARCHIVE_NAME, source), "source_inventory_sha256": cut})
    files = {"decant": binary, "README.md": b"SYNTHETIC TEST\n", "LICENSE": b"SYNTHETIC TEST\n",
             "Cargo.lock": lock, "third-party-dependencies.tsv": inventory,
             "SBOM.cdx.json": c.sbom(lock, inventory, binary, commit, cut),
             "build-dependencies.txt": c.graph_bytes(
                 f"decant-cli v{c.VERSION}\ndecant-backend v0.0.0\ndecant-compiler v0.0.0\n".encode()),
             c.ARCHIVE_NAME: source, c.MANIFEST_NAME: source_manifest,
             c.CHECKSUM_NAME: c.checksums({c.ARCHIVE_NAME: source})}
    for name, data in files.items():
        c.write_new(directory / name, data, executable=name == "decant")
    manifest = c.manifest_for(files, commit, clean, "aarch64-apple-darwin", "rustc 1.98.0 (synthetic)", cut)
    c.write_new(directory / "manifest.json", c.canonical(manifest))
    c.write_new(directory / "SHA256SUMS", c.checksums({**files, "manifest.json": c.canonical(manifest)}))
    return manifest


class Candidate(unittest.TestCase):
    def test_consistency_does_not_execute_binary_or_approve_signing(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            expected = fixture(directory, clean=False)
            with patch.object(c.subprocess, "run", side_effect=AssertionError("must not execute")):
                self.assertEqual(c.verify(directory), expected)
            self.assertFalse(expected["source_tree_clean"])
            self.assertEqual(expected["signing_status"], "unsigned")

    def test_tamper_extra_missing_symlink_and_mode_fail(self):
        for kind in ("tamper", "extra", "missing", "symlink", "mode"):
            with tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary).resolve()
                fixture(directory)
                binary = directory / "decant"
                if kind == "tamper":
                    binary.write_bytes(b"CHANGED")
                elif kind == "extra":
                    (directory / "unexpected").write_bytes(b"x")
                elif kind == "missing":
                    binary.unlink()
                elif kind == "symlink":
                    binary.unlink()
                    binary.symlink_to(directory / "README.md")
                else:
                    binary.chmod(0o644)
                with self.assertRaises((c.CandidateFailure, OSError)):
                    c.verify(directory)

    def test_special_mode_bits_are_not_masked_away(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            fixture(directory)
            # Some development filesystems clear privileged bits on chmod.
            # Inject the observed mode instead of depending on that host policy.
            with patch.object(c.stat, "S_IMODE", return_value=0o4755):
                with self.assertRaises(c.CandidateFailure):
                    c.verify(directory)

    def test_manifest_cannot_change_flavor_version_target_or_checksums(self):
        for field, value in [("build_flavor", "full"), ("version", True), ("default_features", True),
                             ("signing_status", "signed"), ("target", "unknown"),
                             ("source_commit", "bad"), ("features", ["full"])]:
            with tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary).resolve()
                manifest = fixture(directory)
                manifest[field] = value
                (directory / "manifest.json").write_bytes(c.canonical(manifest))
                with self.assertRaises(c.CandidateFailure):
                    c.verify(directory)

    def test_graph_strips_paths_is_canonical_and_rejects_kafka_mcp(self):
        raw = f"decant-cli v{c.VERSION} (/SYNTHETIC_PRIVATE)\ndecant-backend v0.0.0\ndecant-compiler v0.0.0\n"
        canonical = c.graph_bytes(raw.encode())
        self.assertNotIn(b"SYNTHETIC_PRIVATE", canonical)
        self.assertEqual(c.graph_bytes(canonical), canonical)
        for dependency in ("rdkafka v0.39.0", "rdkafka-sys v4.10.0+2.12.1", "decant-kafka v0.0.0",
                           "decant-mcp v0.0.0", "rmcp v3.2.0", "malformed"):
            with self.assertRaises(c.CandidateFailure):
                c.graph_bytes((raw + dependency + "\n").encode())

    def test_existing_output_is_preserved_before_any_build_command(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            sentinel = directory / "preserve"
            sentinel.write_bytes(b"SYNTHETIC KEEP")
            with patch.object(c, "command", side_effect=AssertionError("must not execute")):
                with self.assertRaises(c.CandidateFailure):
                    c.build(ROOT, directory)
            self.assertEqual(sentinel.read_bytes(), b"SYNTHETIC KEEP")


if __name__ == "__main__":
    unittest.main()
