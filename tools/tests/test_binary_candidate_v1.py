from __future__ import annotations

import copy
import gzip
import io
import json
import os
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from tools.binary_candidate_v1 import (
    CandidateFailure, SOURCE_ARCHIVE, SOURCE_MANIFEST, VERSION, archive_name,
    canonical, description, digest, package_bytes, regular, sbom, unpack, verify_candidate,
)
from tools.build_binary_candidate_v1 import build, check_source_paths, rust_build_environment
from tools.check_evidence_alpha_approval_v1 import APPROVALS, RECORDS, validate_bundle
from tools.run_release_gate_v0 import ReleaseGateFailure, public_preflight

ROOT = Path(__file__).resolve().parents[2]
TARGET = "aarch64-apple-darwin"
COMMIT = "a" * 40
CUT = "b" * 64


def members(mode="engineering-preview", *, licensed=False):
    binary = b"SYNTHETIC NON-EXECUTABLE CANDIDATE TEST BYTES"
    lock = (ROOT / "Cargo.lock").read_bytes()
    inventory = (ROOT / "release/third-party-dependencies-v0.tsv").read_bytes()
    result = {
        "decant": binary, "Cargo.lock": lock, "third-party-dependencies.tsv": inventory,
        "SBOM.cdx.json": sbom(lock, inventory, binary, COMMIT, CUT),
        "evidence-config.json": b"{}\n", "trace-capture.jsonl": b"{}\n",
        "README.md": b"SYNTHETIC TEST, NO APPROVAL.\n",
    }
    result["LICENSE" if mode == "public-candidate" or licensed else "LICENSE-MISSING.txt"] = b"Synthetic test license only.\n"
    return result


def candidate_fixture(directory: Path, mode="engineering-preview", *, licensed=False) -> dict:
    contents = members(mode, licensed=licensed)
    archive = package_bytes(contents, TARGET, mode)
    name = archive_name(TARGET)
    # These are deliberately non-executable, synthetic consistency-test data.
    source = b"SYNTHETIC SOURCE ARCHIVE BYTES"
    source_manifest = {
        "format": "decant-source-release-artifact", "version": 0, "release_version": VERSION,
        "source_commit": COMMIT, "signed": False,
        "license_status": "approved-present" if "LICENSE" in contents else "missing",
        "source_inventory_sha256": CUT, "archive": description(SOURCE_ARCHIVE, source),
    }
    manifest = {
        "format": "decant-binary-candidate", "version": 1, "release_version": VERSION,
        "mode": mode, "target": TARGET, "source_commit": COMMIT, "source_tree_clean": True,
        "export_inventory_sha256": CUT, "cargo_lock_sha256": digest(contents["Cargo.lock"]),
        "rustc_version": "rustc 1.98.0 (synthetic)", "signing_status": "unsigned",
        "native_build_reproducibility": "not_established", "runtime_smoke": "passed",
        "archive": description(name, archive),
        "members": [dict(description(key, value), mode="755" if key == "decant" else "644") for key, value in sorted(contents.items())],
        "source_manifest_sha256": digest(canonical(source_manifest)),
    }
    for path, content in {
        name: archive, "manifest.json": canonical(manifest),
        "SHA256SUMS": f"{digest(archive)}  {name}\n".encode(),
        SOURCE_ARCHIVE: source, SOURCE_MANIFEST: canonical(source_manifest),
        SOURCE_ARCHIVE + ".sha256": f"{digest(source)}  {SOURCE_ARCHIVE}\n".encode(),
    }.items():
        (directory / path).write_bytes(content)
    return manifest


def approval_fixture(directory: Path) -> dict:
    manifest = candidate_fixture(directory, "public-candidate")
    records = directory / "records"
    records.mkdir()
    receipts = {}
    for name in RECORDS:
        text = f"SYNTHETIC TEST REVIEW {name}; NOT A REAL APPROVAL.\n".encode()
        (records / (name + ".txt")).write_bytes(text)
        receipts[name] = digest(text)
    signature = b"SYNTHETIC NON-SIGNATURE TEST BYTES"
    (directory / "candidate-signature").write_bytes(signature)
    (directory / "signature-verification.json").write_bytes(canonical({
        "format": "decant-reviewed-signature-verification", "version": 1,
        "artifact_sha256": manifest["archive"]["sha256"], "signature_sha256": digest(signature),
        "policy_record_sha256": receipts["signing-custody-platform"], "verified": True,
    }))
    return {
        "format": "decant-evidence-alpha-approval", "version": 1, "release_version": VERSION,
        "source_commit": COMMIT, "candidate_manifest_sha256": digest((directory / "manifest.json").read_bytes()),
        "activation": {"independent_install_attempts": 10, "own_data_successes": 5, "repeat_users": 3},
        "records": receipts, "approvals": dict.fromkeys(APPROVALS, True),
    }


class CandidateTests(unittest.TestCase):
    def test_licensed_and_legacy_previews_round_trip_without_public_authority(self):
        for mode, licensed in [("engineering-preview", False), ("engineering-preview", True), ("public-candidate", True)]:
            with self.subTest(mode=mode, licensed=licensed), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary).resolve()
                original = candidate_fixture(directory, mode, licensed=licensed)
                with patch("subprocess.run", side_effect=AssertionError("must not execute")):
                    manifest, contents = verify_candidate(directory)
                self.assertEqual(manifest, original)
                self.assertEqual(len(contents), 8)
                self.assertEqual("LICENSE" in contents, licensed)
                self.assertEqual(manifest["signing_status"], "unsigned")
                for present in ("LICENSE", "LICENSE-MISSING.txt"):
                    altered = dict(contents)
                    altered[present] = b"synthetic\n"
                    if set(altered) != set(contents):
                        with self.assertRaises(CandidateFailure):
                            package_bytes(altered, TARGET, mode)
                altered = {key: value for key, value in contents.items() if not key.startswith("LICENSE")}
                with self.assertRaises(CandidateFailure):
                    package_bytes(altered, TARGET, mode)
                source_path = directory / SOURCE_MANIFEST
                source = json.loads(source_path.read_bytes())
                source["license_status"] = "missing" if licensed else "approved-present"
                source_path.write_bytes(canonical(source))
                manifest["source_manifest_sha256"] = digest(canonical(source))
                (directory / "manifest.json").write_bytes(canonical(manifest))
                with self.assertRaises(CandidateFailure):
                    verify_candidate(directory)

    def test_license_alone_cannot_pass_public_preflight(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "LICENSE").write_bytes((ROOT / "LICENSE").read_bytes())
            with patch("subprocess.run", side_effect=AssertionError("must fail before build")):
                with self.assertRaises(ReleaseGateFailure) as raised:
                    public_preflight(root)
            self.assertEqual(raised.exception.category, "missing-approval")

    def test_licensed_preview_cannot_be_used_as_public_alpha_candidate(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            approval = approval_fixture(directory)
            candidate_fixture(directory, "engineering-preview", licensed=True)
            approval["candidate_manifest_sha256"] = digest((directory / "manifest.json").read_bytes())
            with self.assertRaises(CandidateFailure):
                validate_bundle(directory, approval, COMMIT, b"Synthetic test license only.\n")

    def test_build_paths_are_remapped_and_known_private_sources_rejected(self):
        with patch("pathlib.Path.home", return_value=Path("/synthetic-private-home")), patch.dict(os.environ, {"CARGO_HOME": "/synthetic-cargo", "RUSTFLAGS": "synthetic old flags"}):
            environment, forbidden = rust_build_environment(Path("/synthetic-source"), Path("/synthetic-build"))
        self.assertNotIn("RUSTFLAGS", environment)
        self.assertEqual(len(environment["CARGO_ENCODED_RUSTFLAGS"].split("\x1f")), 5)
        for prefix in forbidden:
            with self.assertRaises(CandidateFailure):
                check_source_paths(b"compiler metadata:" + prefix + b"file.rs", forbidden)
        check_source_paths(b"/decant-source/file.rs", forbidden)

    def test_identical_packaging_and_sbom_are_byte_equal_and_scoped(self):
        contents = members()
        first = package_bytes(contents, TARGET, "engineering-preview")
        self.assertEqual(first, package_bytes(dict(reversed(list(contents.items()))), TARGET, "engineering-preview"))
        self.assertEqual(unpack(first, TARGET, "engineering-preview"), contents)
        inventory = json.loads(contents["SBOM.cdx.json"])
        self.assertEqual(inventory["bomFormat"], "CycloneDX")
        self.assertEqual(inventory["specVersion"], "1.6")
        self.assertEqual(len([row for row in inventory["components"] if "purl" in row]), 99)
        self.assertEqual(inventory["compositions"][0]["aggregate"], "incomplete")
        self.assertNotIn(str(ROOT).encode(), contents["SBOM.cdx.json"])
        self.assertNotIn(b"licenses", contents["SBOM.cdx.json"])  # Declared strings, no legal selection.
        with self.assertRaises(CandidateFailure):
            sbom(contents["Cargo.lock"], contents["third-party-dependencies.tsv"].replace(b"tokio\t", b"wrong\t"), contents["decant"], COMMIT, CUT)

    def test_archive_rejects_traversal_links_extensions_duplicates_and_bombs(self):
        prefix = archive_name(TARGET)[:-7] + "/"
        for name, kind, size in [
            (prefix + "../escape", tarfile.REGTYPE, 1),
            (prefix + "decant", tarfile.SYMTYPE, 0),
            (prefix + "decant", tarfile.XHDTYPE, 2 ** 30),
            (prefix + "decant", tarfile.REGTYPE, 2 ** 30),
        ]:
            info = tarfile.TarInfo(name)
            info.type, info.size, info.mode = kind, size, 0o755
            encoded = gzip.compress(info.tobuf(format=tarfile.USTAR_FORMAT) + bytes(1024), mtime=0)
            with self.assertRaises(CandidateFailure):
                unpack(encoded, TARGET, "engineering-preview")
        encoded = gzip.decompress(package_bytes(members(), TARGET, "engineering-preview"))
        first_size = 512 + ((len(members()["Cargo.lock"]) + 511) // 512) * 512
        with self.assertRaises(CandidateFailure):
            unpack(gzip.compress(encoded[:first_size] + encoded, mtime=0), TARGET, "engineering-preview")
        with self.assertRaises(CandidateFailure):
            package_bytes(members(), "invented-target", "engineering-preview")

    def test_verifier_never_executes_and_refuses_manifest_member_and_sbom_drift(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            original = candidate_fixture(directory)
            with patch("subprocess.run", side_effect=AssertionError("must not execute")):
                verify_candidate(directory)
            for key, value in [("source_commit", "wrong"), ("version", True), ("source_tree_clean", 1),
                               ("target", "../escape"), ("signing_status", "signed"), ("unknown", True)]:
                changed = dict(original, **{key: value})
                (directory / "manifest.json").write_bytes(canonical(changed))
                with self.assertRaises((CandidateFailure, TypeError)):
                    verify_candidate(directory)
            candidate_fixture(directory)
            contents = members()
            contents["SBOM.cdx.json"] = b"{}\n"
            archive = package_bytes(contents, TARGET, "engineering-preview")
            (directory / archive_name(TARGET)).write_bytes(archive)
            (directory / "SHA256SUMS").write_bytes(f"{digest(archive)}  {archive_name(TARGET)}\n".encode())
            # Even updated outer receipts cannot disguise a false SBOM.
            original["archive"] = description(archive_name(TARGET), archive)
            original["members"] = [dict(description(key, value), mode="755" if key == "decant" else "644") for key, value in sorted(contents.items())]
            (directory / "manifest.json").write_bytes(canonical(original))
            with self.assertRaises(CandidateFailure):
                verify_candidate(directory)

    def test_files_and_builder_refuse_symlink_fifo_bounds_and_overwrite(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            target = directory / "keep"
            target.write_bytes(b"keep")
            for output in [target, directory]:
                with self.assertRaises(CandidateFailure):
                    build(ROOT, output, "engineering-preview")
            self.assertEqual(target.read_bytes(), b"keep")
            link = directory / "link"
            link.symlink_to(target)
            with self.assertRaises(CandidateFailure):
                regular(link)
            parent_link = directory / "parent"
            parent_link.symlink_to(directory, target_is_directory=True)
            with self.assertRaises(CandidateFailure):
                regular(parent_link / "keep")
            with self.assertRaises(CandidateFailure):
                regular(target, 3)
            fifo = directory / "fifo"
            os.mkfifo(fifo)
            with self.assertRaises(CandidateFailure):
                regular(fifo)

    def test_alpha_evidence_requires_every_approval_review_and_signature(self):
        license_bytes = b"Synthetic test license only.\n"
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            approval = approval_fixture(directory)
            validate_bundle(directory, approval, COMMIT, license_bytes)
            changes = []
            for name in APPROVALS:
                altered = copy.deepcopy(approval)
                altered["approvals"][name] = False
                changes.append(altered)
            for field, value in [("independent_install_attempts", 9), ("own_data_successes", 4), ("repeat_users", 2), ("repeat_users", True)]:
                altered = copy.deepcopy(approval)
                altered["activation"][field] = value
                changes.append(altered)
            altered = copy.deepcopy(approval)
            altered["source_commit"] = "c" * 40
            changes.append(altered)
            altered = copy.deepcopy(approval)
            altered["records"]["activation"] = "d" * 64
            changes.append(altered)
            for altered in changes:
                with self.assertRaises(CandidateFailure):
                    validate_bundle(directory, altered, COMMIT, license_bytes)
            for path in [directory / "candidate-signature", directory / "records/desktop-mcp.txt"]:
                saved = path.read_bytes()
                path.unlink()
                with self.assertRaises(CandidateFailure):
                    validate_bundle(directory, approval, COMMIT, license_bytes)
                path.write_bytes(saved)
            with self.assertRaises(CandidateFailure):
                validate_bundle(directory, approval, COMMIT, b"different license")
            receipt_path = directory / "signature-verification.json"
            receipt = json.loads(receipt_path.read_bytes())
            for key, value in [("verified", 1), ("version", True), ("signature_sha256", "e" * 64), ("artifact_sha256", "f" * 64)]:
                changed = dict(receipt, **{key: value})
                receipt_path.write_bytes(canonical(changed))
                with self.assertRaises(CandidateFailure):
                    validate_bundle(directory, approval, COMMIT, license_bytes)
            receipt_path.write_bytes(canonical(receipt))
            manifest = json.loads((directory / "manifest.json").read_bytes())
            manifest["source_tree_clean"] = False
            (directory / "manifest.json").write_bytes(canonical(manifest))
            with self.assertRaises(CandidateFailure):
                validate_bundle(directory, approval, COMMIT, license_bytes)

    def test_blank_approval_cannot_authorize_and_public_gate_keeps_new_boundary(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary).resolve()
            example = approval_fixture(directory)
            example["source_commit"] = "0" * 40
            example["candidate_manifest_sha256"] = "0" * 64
            example["activation"] = {name: 0 for name in example["activation"]}
            example["records"] = {name: "0" * 64 for name in RECORDS}
            example["approvals"] = {name: False for name in APPROVALS}
            self.assertEqual(set(example["records"]), RECORDS)
            self.assertTrue(all(value is False for value in example["approvals"].values()))
            with self.assertRaises(CandidateFailure):
                validate_bundle(directory, example, COMMIT, b"Synthetic test license only.\n")
        self.assertIn("validate_alpha(root, commit)", (ROOT / "tools/run_release_gate_v0.py").read_text())

    def test_old_public_approval_cannot_bypass_new_evidence_gate(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            (root / "LICENSE").write_bytes(b"Synthetic only.\n")
            (root / ".git").mkdir()
            (root / "release").mkdir()
            (root / "release/public-release-approval-v0.json").write_bytes(b"{}\n")
            with patch("tools.run_release_gate_v0.run"), patch("tools.run_release_gate_v0.git_output", return_value=COMMIT), patch("tools.run_release_gate_v0.validate_approval"), patch("tools.check_evidence_alpha_approval_v1.validate", side_effect=CandidateFailure) as validator:
                with self.assertRaises(ReleaseGateFailure) as raised:
                    public_preflight(root)
                self.assertEqual(raised.exception.category, "alpha-evidence-invalid")
                validator.assert_called_once_with(root, COMMIT)


if __name__ == "__main__":
    unittest.main()
