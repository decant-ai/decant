from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from tools.check_project_licensing_v1 import (
    APACHE_FILES, LICENSE_DIGESTS, LicensingFailure, validate,
)

ROOT = Path(__file__).resolve().parents[2]


class ProjectLicensingTests(unittest.TestCase):
    def fixture(self, directory: str) -> Path:
        root = Path(directory)
        for relative in [*LICENSE_DIGESTS, "LICENSING.md", *APACHE_FILES]:
            path = root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes((ROOT / relative).read_bytes())
        (root / "Cargo.toml").write_text(
            '[workspace]\nmembers = ["crates/decant-test"]\n'
            '[workspace.package]\nlicense = "AGPL-3.0-only"\n')
        crate = root / "crates/decant-test"
        crate.mkdir(parents=True)
        (crate / "Cargo.toml").write_text('[package]\nlicense.workspace = true\n')
        return root

    def test_repository_licenses_metadata_and_exact_exception_scope(self):
        validate(ROOT)

    def test_missing_modified_and_symlink_license_fail(self):
        for relative in LICENSE_DIGESTS:
            for change in ("missing", "modified", "symlink"):
                with self.subTest(relative=relative, change=change), tempfile.TemporaryDirectory() as directory:
                    root = self.fixture(directory)
                    path = root / relative
                    data = path.read_bytes()
                    path.unlink()
                    if change == "modified":
                        path.write_bytes(data + b"extra license restriction\n")
                    elif change == "symlink":
                        path.symlink_to(ROOT / relative)
                    with self.assertRaises((LicensingFailure, OSError)):
                        validate(root)

    def test_wrong_owner_scope_metadata_and_notices_fail(self):
        changes = [
            ("LICENSING.md", "2026 Eshan Vaid", "2026 Incorrect Owner"),
            ("LICENSING.md", APACHE_FILES[0], "examples/"),
            ("Cargo.toml", "AGPL-3.0-only", "AGPL-3.0-or-later"),
            ("crates/decant-test/Cargo.toml", "license.workspace = true", 'license = "Apache-2.0"'),
            ("crates/decant-test/Cargo.toml", "license.workspace = true", 'license.workspace = 1'),
            (APACHE_FILES[-1], "Apache-2.0", "MIT"),
        ]
        for relative, old, new in changes:
            with self.subTest(relative=relative), tempfile.TemporaryDirectory() as directory:
                root = self.fixture(directory)
                path = root / relative
                path.write_text(path.read_text().replace(old, new))
                with self.assertRaises(LicensingFailure):
                    validate(root)

    def test_unlisted_exception_and_oversized_text_fail(self):
        for suffix in ("\n- `examples/extra.json`\n", "x" * (64 * 1024)):
            with tempfile.TemporaryDirectory() as directory:
                root = self.fixture(directory)
                path = root / "LICENSING.md"
                path.write_text(path.read_text() + suffix)
                with self.assertRaises(LicensingFailure):
                    validate(root)


if __name__ == "__main__":
    unittest.main()
