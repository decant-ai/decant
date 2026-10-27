"""Synthetic record-validation tests, not an agent comparison or measurements."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tools"))
import investigation_evaluation_v1 as ev


class Evaluation(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.objects = self.directory / "objects"
        self.objects.mkdir()
        self.reg = ev.template()
        self.reg.update(status="registered", source_commit="1" * 40, model="synthetic-model",
                        environment=self.put({"synthetic": "environment"}),
                        fairness_protocol=self.put({"synthetic": "fairness"}))
        for arm in self.reg["comparators"]:
            arm["configuration"] = self.put({"synthetic": arm["id"]})
        for case in self.reg["cases"]:
            case["input"] = self.put({"synthetic_input": case["id"]})
            case["expected"] = self.put({"synthetic_expected": case["id"]})
        self.raw = ev.canonical(self.reg)

    def put(self, value):
        data = ev.canonical(value)
        identity = ev.digest(data)
        path = self.objects / identity
        if not path.exists():
            path.write_bytes(data)
        return identity

    def store(self):
        return ev.Objects(self.objects)

    def ledger(self):
        answer = self.put({"synthetic": "answer"})
        measurement = self.put(dict(wall_ms=10, cpu_ms=2, peak_rss_bytes=1024, disk_bytes=5))
        review = self.put(dict(reviewer="synthetic-reviewer", rating="correct_supported",
                               fabricated_measurements=0, invalid_references=0))
        call = {"request": self.put({"synthetic": "request"}), "response": answer,
                "input_tokens": 10, "output_tokens": 2, "cache_read_tokens": 3,
                "cache_write_tokens": 4, "cost_micro_usd": 7}
        return {"format": "decant-investigation-evaluation-ledger", "version": 1,
                "registration_sha256": ev.digest(self.raw), "trials": [
                    dict(case=case, arm=arm, regime=regime, repeat=repeat, status="completed",
                         answer=answer, measurement=measurement, review=review,
                         model_calls=[] if arm in ("engineer", "decant_deterministic") else [copy.deepcopy(call)],
                         backend_calls=[call["request"]])
                    for case, arm, regime, repeat in sorted(ev.trial_keys())]}

    def summary(self, ledger):
        return ev.summarize(self.raw, ev.canonical(ledger), self.store())

    def test_template_is_not_registration_and_case_matrix_is_frozen(self):
        self.assertEqual(len(ev.slots()), 60)
        self.assertEqual(len(ev.trial_keys()), 1020)
        self.assertEqual(sum(c["split"] == "held_out" for c in ev.slots()), 30)
        with self.assertRaises(ev.Invalid):
            ev.registration(ev.canonical(ev.template()), self.store())
        for field, value in [("model", None), ("repeats", {"holmesgpt": 1}),
                             ("evaluator_sha256", "0" * 64), ("version", True)]:
            changed = copy.deepcopy(self.reg)
            changed[field] = value
            with self.assertRaises(ev.Invalid):
                ev.registration(ev.canonical(changed), self.store())

    def test_complete_records_are_canonical_but_not_claim_approval(self):
        ledger = self.ledger()
        first = self.summary(ledger)
        self.assertEqual(first["trial_count"], 1020)
        self.assertEqual(first["comparative_conclusion"], "requires_independent_analysis")
        self.assertFalse(first["publication_approved"])
        self.assertFalse(first["authenticity_verified"])
        ledger["trials"].reverse()
        second = self.summary(ledger)
        self.assertEqual(first["scenarios"], second["scenarios"])
        self.assertNotEqual(first["ledger_sha256"], second["ledger_sha256"])

    def test_missing_duplicate_relabelled_and_cross_registration_trials_fail(self):
        for kind in ("missing", "duplicate", "development", "repeat", "registration"):
            ledger = self.ledger()
            if kind == "missing":
                ledger["trials"].pop()
            elif kind == "duplicate":
                ledger["trials"][-1] = ledger["trials"][0]
            elif kind == "development":
                ledger["trials"][0]["case"] = self.reg["cases"][0]["id"]
            elif kind == "repeat":
                ledger["trials"][0]["repeat"] = True
            else:
                ledger["registration_sha256"] = "0" * 64
            with self.assertRaises(ev.Invalid):
                self.summary(ledger)

    def test_failures_are_retained_and_unknown_cost_is_not_zero(self):
        ledger = self.ledger()
        trial = next(t for t in ledger["trials"] if t["arm"] == "holmesgpt")
        trial["status"] = "timeout"
        trial["review"] = self.put(dict(reviewer="synthetic-reviewer", rating="unscorable",
                                        fabricated_measurements=0, invalid_references=0))
        trial["model_calls"][0]["cost_micro_usd"] = None
        trial["model_calls"][0]["output_tokens"] = None
        summary = self.summary(ledger)
        rows = [t for g in summary["scenarios"] for t in g["trials"] if t["status"] == "timeout"]
        self.assertEqual(len(rows), 1)
        self.assertIsNone(rows[0]["cost_micro_usd"])
        self.assertIsNone(rows[0]["output_tokens"])
        self.assertEqual(rows[0]["cache_read_tokens"], 3)
        self.assertEqual(rows[0]["cache_write_tokens"], 4)

    def test_invalid_correct_ratings_and_missing_model_accounting_fail(self):
        for kind in ("failed_correct", "fabricated", "reference", "no_calls", "unknown"):
            ledger = self.ledger()
            trial = next(t for t in ledger["trials"] if t["arm"] == "decant_reasoned")
            if kind == "failed_correct":
                trial["status"] = "failed"
            elif kind == "no_calls":
                trial["model_calls"] = []
            elif kind == "unknown":
                trial["private"] = "SYNTHETIC_PROTECTED"
            else:
                rating = dict(reviewer="synthetic-reviewer", rating="correct_supported",
                              fabricated_measurements=kind == "fabricated", invalid_references=kind == "reference")
                rating["fabricated_measurements"] = int(rating["fabricated_measurements"])
                rating["invalid_references"] = int(rating["invalid_references"])
                trial["review"] = self.put(rating)
            with self.assertRaises(ev.Invalid):
                self.summary(ledger)

    def test_duplicate_cases_and_reused_inputs_do_not_fill_sixty_slots(self):
        for kind in ("slot", "input"):
            changed = copy.deepcopy(self.reg)
            if kind == "slot":
                changed["cases"][1] = changed["cases"][0]
            else:
                for field in ("input", "expected"):
                    changed["cases"][1][field] = changed["cases"][0][field]
            with self.assertRaises(ev.Invalid):
                ev.registration(ev.canonical(changed), self.store())

    def test_corruption_symlink_hardlink_and_artifact_limits_fail(self):
        identity = self.reg["cases"][0]["input"]
        path = self.objects / identity
        original = path.read_bytes()
        path.write_bytes(b"changed")
        with self.assertRaises(ev.Invalid):
            ev.registration(self.raw, self.store())
        path.unlink()
        outside = self.directory / "outside"
        outside.write_bytes(original)
        path.symlink_to(outside)
        with self.assertRaises(ev.Invalid):
            ev.registration(self.raw, self.store())
        path.unlink()
        path.hardlink_to(outside)
        with self.assertRaises(ev.Invalid):
            ev.registration(self.raw, self.store())
        path.unlink()
        path.write_bytes(original)
        fifo = self.directory / "fifo"
        os.mkfifo(fifo)
        with self.assertRaises(ev.Invalid):
            ev.regular(fifo, ev.MAX_OBJECT)
        with patch.object(ev, "MAX_OBJECT", 1), self.assertRaises(ev.Invalid):
            self.store().read(identity)
        with patch.object(ev, "MAX_TOTAL", 1), self.assertRaises(ev.Invalid):
            self.store().read(identity)

    def test_json_caps_duplicate_keys_and_fixed_cli_errors(self):
        for raw in (b'{"x":1,"x":2}', b"[" * 25 + b"0" + b"]" * 25,
                    b" " * (ev.MAX_JSON + 1)):
            with self.assertRaises(ev.Invalid):
                ev.document(raw)
        path = self.directory / "SYNTHETIC_PROTECTED.json"
        path.write_text('{"SYNTHETIC_PROTECTED":true}')
        out = subprocess.run([sys.executable, str(ROOT / "tools/investigation_evaluation_v1.py"),
                              "registration", "--registration", str(path), "--objects", str(self.objects)],
                             capture_output=True, timeout=10)
        self.assertEqual(out.returncode, 1)
        self.assertEqual(out.stdout, b"")
        self.assertEqual(out.stderr, b"investigation evaluation: invalid or incomplete records\n")


if __name__ == "__main__":
    unittest.main()
