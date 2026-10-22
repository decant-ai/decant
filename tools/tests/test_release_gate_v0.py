from __future__ import annotations

import gzip
import hashlib
import io
import json
import shutil
import subprocess
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path

from tools.check_public_release_approval_v0 import ApprovalFailure, validate
from tools.run_release_gate_v0 import ReleaseGateFailure, extract_archive, public_preflight


class ReleaseGateTests(unittest.TestCase):
    commit = "a" * 40

    def valid_document(self, license_bytes: bytes) -> dict[str, object]:
        return {
            "format": "decant-public-release-approval",
            "version": 0,
            "release_version": "0.1.0-alpha.1",
            "approved_source_commit": self.commit,
            "license_sha256": hashlib.sha256(license_bytes).hexdigest(),
            "approvals": {
                "clean_machine_reproduction_complete": True,
                "contributor_strategy_approved": True,
                "employment_ip_review_complete": True,
                "external_user_activation_requirement_met": True,
                "founder_publication_approved": True,
                "full_history_secret_scan_complete": True,
                "license_approved": True,
                "name_review_complete": True,
                "signing_policy_approved": True,
            },
        }

    def write_fixture(self, root: Path, document: object) -> None:
        license_bytes = b"Synthetic test license only.\n"
        (root / "LICENSE").write_bytes(license_bytes)
        approval = root / "release/public-release-approval-v0.json"
        approval.parent.mkdir(exist_ok=True)
        approval.write_text(
            json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n",
            encoding="utf-8",
        )

    def test_exact_complete_commit_and_license_bound_approval_passes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            license_bytes = b"Synthetic test license only.\n"
            self.write_fixture(root, self.valid_document(license_bytes))
            validate(root, self.commit)

    def test_unknown_stale_incomplete_commit_and_license_drift_fail_closed(self) -> None:
        license_bytes = b"Synthetic test license only.\n"
        mutations: list[tuple[str, object]] = []
        unknown = self.valid_document(license_bytes)
        unknown["unknown"] = True
        mutations.append(("approval-shape", unknown))
        stale = self.valid_document(license_bytes)
        stale["release_version"] = "0.1.0-alpha.0"
        mutations.append(("release-version", stale))
        incomplete = self.valid_document(license_bytes)
        incomplete["approvals"]["license_approved"] = False  # type: ignore[index]
        mutations.append(("approval-incomplete", incomplete))
        commit = self.valid_document(license_bytes)
        commit["approved_source_commit"] = "b" * 40
        mutations.append(("source-commit", commit))
        license_drift = self.valid_document(b"different\n")
        mutations.append(("license-digest", license_drift))

        with tempfile.TemporaryDirectory() as directory:
            for index, (category, document) in enumerate(mutations):
                root = Path(directory) / str(index)
                root.mkdir()
                self.write_fixture(root, document)
                with self.assertRaises(ApprovalFailure) as raised:
                    validate(root, self.commit)
                self.assertEqual(raised.exception.category, category)

    def test_malformed_noncanonical_symlink_and_oversized_approval_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            root.mkdir(exist_ok=True)
            self.write_fixture(root, self.valid_document(b"Synthetic test license only.\n"))
            approval = root / "release/public-release-approval-v0.json"
            approval.write_bytes(b"{")
            with self.assertRaises(ApprovalFailure) as raised:
                validate(root, self.commit)
            self.assertEqual(raised.exception.category, "approval-json")

            self.write_fixture(root, self.valid_document(b"Synthetic test license only.\n"))
            document = json.loads(approval.read_text(encoding="utf-8"))
            approval.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
            with self.assertRaises(ApprovalFailure) as raised:
                validate(root, self.commit)
            self.assertEqual(raised.exception.category, "approval-canonical")

            approval.unlink()
            approval.symlink_to(root / "LICENSE")
            with self.assertRaises(ApprovalFailure) as raised:
                validate(root, self.commit)
            self.assertEqual(raised.exception.category, "approval-record")

            approval.unlink()
            approval.write_bytes(b"x" * (64 * 1024 + 1))
            with self.assertRaises(ApprovalFailure) as raised:
                validate(root, self.commit)
            self.assertEqual(raised.exception.category, "approval-record")

    def test_public_preflight_reports_missing_gates_before_git(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with self.assertRaises(ReleaseGateFailure) as raised:
                public_preflight(root)
            self.assertEqual(raised.exception.category, "missing-license")
            (root / "LICENSE").write_text("Synthetic test license only.\n", encoding="utf-8")
            with self.assertRaises(ReleaseGateFailure) as raised:
                public_preflight(root)
            self.assertEqual(raised.exception.category, "missing-approval")

    def test_archive_extraction_refuses_escape_and_symlink_members(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index, member in enumerate(["decant-0.1.0-alpha.1/../escape", "other/file"]):
                archive_path = root / f"bad-{index}.tar.gz"
                buffer = io.BytesIO()
                with tarfile.open(fileobj=buffer, mode="w") as archive:
                    info = tarfile.TarInfo(member)
                    info.size = 1
                    archive.addfile(info, io.BytesIO(b"x"))
                archive_path.write_bytes(gzip.compress(buffer.getvalue(), mtime=0))
                destination = root / f"extract-{index}"
                destination.mkdir()
                with self.assertRaises(ReleaseGateFailure):
                    extract_archive(archive_path, destination)

            archive_path = root / "link.tar.gz"
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode="w") as archive:
                info = tarfile.TarInfo("decant-0.1.0-alpha.1/link")
                info.type = tarfile.SYMTYPE
                info.linkname = "target"
                archive.addfile(info)
            archive_path.write_bytes(gzip.compress(buffer.getvalue(), mtime=0))
            destination = root / "extract-link"
            destination.mkdir()
            with self.assertRaises(ReleaseGateFailure):
                extract_archive(archive_path, destination)

    def test_entry_point_does_not_mutate_an_export_with_bytecode_cache(self) -> None:
        repository = Path(__file__).resolve().parents[2]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            tools = root / "tools"
            tools.mkdir()
            for name in [
                "run_release_gate_v0.py",
                "build_source_artifact_v0.py",
                "check_public_release_approval_v0.py",
                "export_public_source_v0.py",
            ]:
                shutil.copyfile(repository / "tools" / name, tools / name)

            result = subprocess.run(
                [
                    sys.executable,
                    str(tools / "run_release_gate_v0.py"),
                    "--root",
                    str(root / "missing"),
                    "--mode",
                    "engineering-preview",
                ],
                cwd=root,
                check=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((tools / "__pycache__").exists())


if __name__ == "__main__":
    unittest.main()
