from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from tools.check_benchmark_smoke_v0 import BenchmarkSmokeFailure, validate


class BenchmarkSmokeTests(unittest.TestCase):
    def paths(self, root: Path) -> tuple[Path, Path]:
        repository = Path(__file__).resolve().parents[2]
        manifest = root / "manifest.json"
        golden = root / "golden.json"
        manifest.write_bytes((repository / "release/benchmark-smoke-sre-v0.json").read_bytes())
        golden.write_bytes(
            (repository / "crates/decant-cli/tests/golden/sre-demo-v0.json").read_bytes()
        )
        return manifest, golden

    def test_exact_manifest_and_golden_pass(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            manifest, golden = self.paths(Path(directory))
            validate(manifest, golden)

    def test_result_status_baseline_measurement_and_performance_drift_fail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, golden = self.paths(root)
            original = json.loads(manifest.read_text(encoding="utf-8"))
            mutations = []
            for key, value in [
                ("result_status", "measured"),
                ("future_required_baselines", original["future_required_baselines"][:-1]),
                (
                    "future_required_measurement_fields",
                    original["future_required_measurement_fields"][:-1],
                ),
                ("throughput", 999_999),
            ]:
                changed = json.loads(json.dumps(original))
                changed[key] = value
                mutations.append(changed)
            for index, changed in enumerate(mutations):
                path = root / f"changed-{index}.json"
                path.write_text(json.dumps(changed), encoding="utf-8")
                with self.assertRaises(BenchmarkSmokeFailure):
                    validate(path, golden)

    def test_golden_or_expected_fact_drift_fails(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, golden = self.paths(root)
            changed_output = json.loads(golden.read_text(encoding="utf-8"))
            changed_output["admission"]["accepted"] -= 1
            golden.write_text(json.dumps(changed_output), encoding="utf-8")
            with self.assertRaises(BenchmarkSmokeFailure):
                validate(manifest, golden)


if __name__ == "__main__":
    unittest.main()
