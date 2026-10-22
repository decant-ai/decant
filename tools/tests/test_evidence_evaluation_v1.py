"""Falsify the independent evaluation contract; no performance run in tests."""
import copy
import importlib.util
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools"))
from evidence_evaluation import baseline, fixtures
from evidence_evaluation.results import summaries

loading = importlib.util.spec_from_file_location("evaluator", ROOT / "tools/evaluate_evidence_v1.py")
evaluator = importlib.util.module_from_spec(loading)
loading.loader.exec_module(evaluator)


class EvidenceEvaluation(unittest.TestCase):
    def test_missing_failed_or_relabelled_measurements_cannot_pass(self):
        contract = evaluator.spec()
        runs = []
        for size in (10000, 20000):
            for trial in (-1, 0, 1, 2):
                for implementation in (["decant", "sql"] if trial % 2 == 0 else ["sql", "decant"]):
                    # Synthetic arithmetic fixture, not a benchmark result.
                    runs.append({"size": size, "implementation": implementation, "trial": trial,
                                 "phase": "warmup" if trial == -1 else "measured", "status": 0,
                                 "timeout": False, "seed": 104, "services": 8,
                                 "elapsed_ns": size * 100, "peak_rss_bytes": 1024})
        self.assertEqual(len(summaries(runs, contract)), 4)
        for mutation in ("missing", "failed", "relabelled", "scaling"):
            changed = copy.deepcopy(runs)
            if mutation == "missing":
                changed.pop()
            elif mutation == "failed":
                changed[2]["status"] = 1
            elif mutation == "relabelled":
                changed[0]["phase"] = "measured"
            else:
                for row in changed:
                    if row["size"] == 20000:
                        row["elapsed_ns"] *= 4
            with self.assertRaises(ValueError):
                summaries(changed, contract)

    def test_specification_cannot_silently_relax_workload_or_gate(self):
        original = evaluator.spec()
        self.assertEqual(tuple(original["cases"]), fixtures.CASES)
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "spec.json"
            for field, value in [("seed", 7), ("services", 1), ("trials", 1), ("timeout_seconds", 300)]:
                changed = copy.deepcopy(original)
                changed["load"][field] = value
                path.write_text(json.dumps(changed))
                with patch.object(evaluator, "SPEC", path):
                    with self.assertRaises(ValueError):
                        evaluator.spec()

    def test_sql_matches_manual_facts_and_conflict_is_not_filtered_away(self):
        recipe = evaluator.spec()["recipe"]
        with tempfile.TemporaryDirectory() as temporary:
            for case in fixtures.CASES:
                path = Path(temporary) / (case + ".jsonl")
                fixtures.write_capture(path, fixtures.capture(case))
                if case in {"conflict", "high_cardinality"}:
                    with self.assertRaises(ValueError):
                        baseline.aggregate(path, recipe)
                else:
                    oracle = baseline.aggregate(path, recipe)
                    evaluator.frozen_oracle(case, oracle)
                    if case == "regression":
                        self.assertEqual(oracle["services"][0]["current"]["selected_references"], [
                            "decant://event/v0/otel-evidence-v1/otlp-file/3/0/0/0",
                            "decant://event/v0/otel-evidence-v1/otlp-file/4/0/0/0"])
                        oracle["services"][0]["current"]["p95_duration_ns"] = 20
                        with self.assertRaises(ValueError):
                            evaluator.frozen_oracle(case, oracle)

    def test_baseline_packing_is_bounded_and_discloses_omissions(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "capture.jsonl"
            fixtures.write_capture(path, fixtures.capture("output_omission"))
            oracle = baseline.aggregate(path, evaluator.spec()["recipe"])
            packed = baseline.packed(oracle, 2048)
            self.assertLessEqual(len(packed), 2048)
            document = json.loads(packed)
            self.assertEqual(document["candidate_services"], 20)
            self.assertGreater(document["omitted_services"], 0)
            self.assertEqual(document["services"], oracle["services"][:len(document["services"])] )
            self.assertNotIn(fixtures.SENTINEL.encode(), packed)

    def test_load_is_fixed_cardinality_seed_and_rejects_unregistered_sizes(self):
        with self.assertRaises(ValueError):
            next(fixtures.load(9))
        rows = list(fixtures.load(10000))
        self.assertEqual(len(rows), 10000)
        self.assertEqual(len({row["resourceSpans"][0]["resource"]["attributes"][0]["value"]["stringValue"] for row in rows}), 8)
        first = rows[0]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
        last = rows[-1]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
        self.assertEqual(first["startTimeUnixNano"], "100000000000")
        self.assertEqual(last["startTimeUnixNano"], "600000000000")
        self.assertEqual(int(first["endTimeUnixNano"]) - int(first["startTimeUnixNano"]), 13000000)


if __name__ == "__main__":
    unittest.main()
