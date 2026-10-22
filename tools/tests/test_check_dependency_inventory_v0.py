from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.check_dependency_inventory_v0 import InventoryFailure, validate


class DependencyInventoryTests(unittest.TestCase):
    def fixture(self, root: Path, *, license_expression: str = "MIT") -> tuple[Path, Path]:
        source = "registry+https://github.com/rust-lang/crates.io-index"
        allowlist = root / "allowlist.tsv"
        allowlist.write_text(f"example\t1.2.3\t{license_expression}\t{source}\n", encoding="utf-8")
        metadata = root / "metadata.json"
        metadata.write_text(
            json.dumps(
                {
                    "packages": [
                        {
                            "name": "local",
                            "version": "0.0.0",
                            "license": None,
                            "source": None,
                        },
                        {
                            "name": "example",
                            "version": "1.2.3",
                            "license": license_expression,
                            "source": source,
                        },
                    ]
                }
            ),
            encoding="utf-8",
        )
        return allowlist, metadata

    def test_exact_external_row_passes_and_local_package_is_excluded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            allowlist, metadata = self.fixture(Path(directory))
            validate(allowlist, metadata)

    def test_version_license_source_add_remove_and_duplicate_drift_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            allowlist, metadata = self.fixture(root)
            original = json.loads(metadata.read_text(encoding="utf-8"))
            mutations = []
            for field, value in [
                ("version", "1.2.4"),
                ("license", None),
                ("source", "git+https://example.invalid/repository"),
            ]:
                changed = json.loads(json.dumps(original))
                changed["packages"][1][field] = value
                mutations.append(changed)
            mutations.append({"packages": [original["packages"][0]]})
            duplicated = json.loads(json.dumps(original))
            duplicated["packages"].append(dict(duplicated["packages"][1]))
            mutations.append(duplicated)
            for index, changed in enumerate(mutations):
                case = root / f"changed-{index}.json"
                case.write_text(json.dumps(changed), encoding="utf-8")
                with self.assertRaises(InventoryFailure):
                    validate(allowlist, case)

    def test_malformed_unsorted_duplicate_empty_and_oversized_allowlists_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            allowlist, metadata = self.fixture(root)
            source = "registry+https://github.com/rust-lang/crates.io-index"
            cases = [
                "",
                "missing-fields\t1\n",
                f"z\t1\tMIT\t{source}\na\t1\tMIT\t{source}\n",
                f"a\t1\tMIT\t{source}\na\t1\tMIT\t{source}\n",
            ]
            for index, content in enumerate(cases):
                case = root / f"allowlist-{index}.tsv"
                case.write_text(content, encoding="utf-8")
                with self.assertRaises(InventoryFailure):
                    validate(case, metadata)
            oversized = root / "oversized.tsv"
            oversized.write_bytes(b"x" * (1024 * 1024 + 1))
            with self.assertRaises(InventoryFailure):
                validate(oversized, metadata)


if __name__ == "__main__":
    unittest.main()
