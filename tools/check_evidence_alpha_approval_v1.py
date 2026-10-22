#!/usr/bin/env python3
"""Check externally reviewed alpha evidence, never generate or grant approval."""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

sys.dont_write_bytecode = True

if __package__:
    from tools.binary_candidate_v1 import CandidateFailure, COMMIT, VERSION, canonical, digest, document, regular, require, verify_candidate
    from tools.export_public_source_v0 import ExportFailure, INVENTORY_NAME, export_source
else:
    from binary_candidate_v1 import CandidateFailure, COMMIT, VERSION, canonical, digest, document, regular, require, verify_candidate
    from export_public_source_v0 import ExportFailure, INVENTORY_NAME, export_source

APPROVAL = Path("release/evidence-alpha-approval-v1.json")
BUNDLE = Path(".local/evidence-alpha")
RECORDS = {
    "independent-security-correctness", "native-dependency-sbom", "clean-target-installation",
    "desktop-mcp", "sre-agent", "comparative-evaluation", "activation",
    "signing-custody-platform", "rollback", "publication", "history-export-scan",
}
APPROVALS = {"founder_reviewed_records", "signing_identity_custody_approved", "exact_publication_action_approved"}


def validate_bundle(bundle: Path, approval: dict, expected_commit: str, license_bytes: bytes) -> dict:
    require(COMMIT.fullmatch(expected_commit) is not None)
    require(set(approval) == {"format", "version", "release_version", "source_commit", "candidate_manifest_sha256", "activation", "records", "approvals"})
    require(approval["format"] == "decant-evidence-alpha-approval" and type(approval["version"]) is int and approval["version"] == 1)
    require(approval["release_version"] == VERSION and approval["source_commit"] == expected_commit)
    manifest, members = verify_candidate(bundle)
    require(manifest["source_tree_clean"] is True and manifest["mode"] == "public-candidate")
    require(manifest["source_commit"] == expected_commit and members["LICENSE"] == license_bytes)
    require(approval["candidate_manifest_sha256"] == digest(regular(bundle / "manifest.json")))
    require(isinstance(approval["approvals"], dict) and set(approval["approvals"]) == APPROVALS)
    require(all(value is True for value in approval["approvals"].values()))
    counts = approval["activation"]
    require(isinstance(counts, dict) and set(counts) == {"independent_install_attempts", "own_data_successes", "repeat_users"})
    require(all(type(value) is int and 0 <= value <= 10000 for value in counts.values()))
    require(10 <= counts["independent_install_attempts"])
    require(5 <= counts["own_data_successes"] <= counts["independent_install_attempts"])
    require(3 <= counts["repeat_users"] <= counts["own_data_successes"])
    require(isinstance(approval["records"], dict) and set(approval["records"]) == RECORDS)
    for name in sorted(RECORDS):
        # Fixed paths only; receipts cannot select arbitrary files or commands.
        content = regular(bundle / "records" / (name + ".txt"), 64 * 1024)
        require(approval["records"][name] == digest(content))
    signature = regular(bundle / "candidate-signature", 1024 * 1024)
    receipt = document(regular(bundle / "signature-verification.json", 64 * 1024))
    require(type(receipt.get("version")) is int and receipt.get("verified") is True)
    require(receipt == {
        "format": "decant-reviewed-signature-verification", "version": 1,
        "artifact_sha256": manifest["archive"]["sha256"], "signature_sha256": digest(signature),
        "policy_record_sha256": approval["records"]["signing-custody-platform"], "verified": True,
    })
    # This is a reviewed verifier receipt, not independent cryptographic
    # verification or proof of truthful human attestations. Never execute it.
    return manifest


def validate(root: Path, expected_commit: str) -> None:
    require(not root.is_symlink())
    root = root.resolve(strict=True)
    approval = document(regular(root / APPROVAL, 64 * 1024))
    manifest = validate_bundle(root / BUNDLE, approval, expected_commit, regular(root / "LICENSE"))
    # Bind the candidate to the actual current approved export as well as HEAD.
    with tempfile.TemporaryDirectory(prefix="decant-alpha-preflight-") as temporary:
        exported = Path(temporary).resolve(strict=True) / "export"
        export_source(root, exported, "public")
        require(digest(regular(exported / INVENTORY_NAME, 4 * 1024 * 1024)) == manifest["export_inventory_sha256"])


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--expected-source-commit", required=True)
    try:
        args = parser.parse_args()
        validate(args.root, args.expected_source_commit)
    except (CandidateFailure, ExportFailure, OSError, KeyError, ValueError, TypeError, RecursionError):
        print("alpha release evidence: NO-GO (missing, incomplete or inconsistent evidence)", file=sys.stderr)
        return 1
    print("PASS: reviewed alpha evidence consistency; no signing, publication or independent attestation of truth")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
