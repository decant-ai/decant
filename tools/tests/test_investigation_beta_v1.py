"""Synthetic completeness tests: no real participants, signatures, or trials."""
import copy
from pathlib import Path
import subprocess
import sys
import unittest

from tools import investigation_beta_v1 as beta
from tools.tests import test_investigation_candidate_v1 as candidate_fixture
from tools.tests import test_investigation_evaluation_v1 as evaluation_fixture


class Beta(unittest.TestCase):
    def setUp(self):
        self.helper = evaluation_fixture.Evaluation()
        self.helper.setUp()
        self.addCleanup(self.helper.doCleanups)
        self.helper.directory = self.helper.directory.resolve()
        self.helper.objects = self.helper.objects.resolve()
        self.directories = []
        identities = {}
        for target in beta.TARGETS:
            directory = self.helper.directory / target
            directory.mkdir()
            manifest = candidate_fixture.fixture(directory)
            manifest["target"] = target
            raw = beta.candidate.canonical(manifest)
            (directory / "manifest.json").write_bytes(raw)
            files = {name: (directory / name).read_bytes() for name in beta.candidate.PAYLOAD}
            (directory / "SHA256SUMS").write_bytes(beta.candidate.checksums({**files, "manifest.json": raw}))
            identities[target] = beta.candidate.digest(raw)
            self.directories.append(directory)
        self.cut = beta.evaluation.digest(beta.evaluation.canonical(identities))
        self.base = {"version": 1, "source_commit": "1" * 40, "candidate_set_sha256": self.cut}
        self.proof = self.helper.put({"synthetic": "not real evidence or approval"})
        self.records = beta.template()
        self.records.update(source_commit="1" * 40, candidate_set_sha256=self.cut)
        for name in beta.REVIEWS:
            self.records["reviews"][name] = self.helper.put({**self.base,
                "format": "decant-investigation-review", "scope": name,
                "reviewer": "synthetic-reviewer", "independent": True,
                "status": "passed", "evidence": [self.proof]})
        for index in range(5):
            self.records["activation"].append(self.helper.put({**self.base,
                "format": "decant-investigation-activation", "participant": f"synthetic-{index}",
                "target": beta.TARGETS[index % 2], "consent_confirmed": True, "independent": True,
                "assisted": False, "installed": True, "own_data_completed": True,
                "repeat_investigation": index < 3, "prefers_another": index < 3, "evidence": [self.proof]}))
        self.records["comparison"] = {"registration": beta.evaluation.digest(self.helper.raw),
                                      "ledger": self.helper.put(self.helper.ledger())}
        self.helper.objects.joinpath(self.records["comparison"]["registration"]).write_bytes(self.helper.raw)
        self.records["reviews"]["comparison_analysis"] = self.mutate(
            self.records["reviews"]["comparison_analysis"],
            lambda v: v.update(evidence=[self.proof, *self.records["comparison"].values()]))

    def assess(self, records=None, directories=None):
        return beta.assess(self.directories if directories is None else directories,
                           beta.evaluation.canonical(self.records if records is None else records),
                           beta.evaluation.Objects(self.helper.objects))

    def mutate(self, identity, change):
        value = beta.evaluation.document(self.helper.objects.joinpath(identity).read_bytes())
        change(value)
        return self.helper.put(value)

    def test_empty_template_is_no_go_and_complete_synthetic_records_only_request_review(self):
        pending = self.assess(beta.template(), [])
        self.assertEqual(pending["decision"], "NO-GO")
        self.assertEqual(pending["counts"]["attempts"], 0)
        self.assertIn("comparison_trials_missing", pending["blockers"])
        complete = self.assess()
        self.assertEqual(complete["decision"], "FOUNDER_REVIEW_REQUIRED")
        self.assertTrue(complete["records_complete"])
        self.assertFalse(complete["publication_authorized"])
        self.assertFalse(complete["authenticity_verified"])
        self.assertEqual(complete["counts"]["own_data_completions"], 5)
        self.assertEqual(complete["counts"]["repeat_users"], 3)
        self.assertEqual(complete, self.assess(directories=list(reversed(self.directories))))

    def test_missing_failed_or_nonindependent_review_cannot_pass(self):
        for kind in ("missing", "failed", "independence"):
            changed = copy.deepcopy(self.records)
            name = "independent_security_correctness"
            if kind == "missing":
                changed["reviews"][name] = None
            else:
                field, value = ("status", "failed") if kind == "failed" else ("independent", False)
                changed["reviews"][name] = self.mutate(changed["reviews"][name], lambda v: v.update({field: value}))
            self.assertEqual(self.assess(changed)["decision"], "NO-GO")

    def test_assisted_nonconsenting_and_author_runs_cannot_be_independent_activation(self):
        for field, value in (("assisted", True), ("consent_confirmed", False), ("independent", False)):
            changed = copy.deepcopy(self.records)
            for index in range(3):
                changed["activation"][index] = self.mutate(changed["activation"][index], lambda v: v.update({field: value}))
            out = self.assess(changed)
            self.assertEqual(out["counts"]["attempts"], 5)
            self.assertEqual(out["counts"]["own_data_completions"], 2)
            self.assertIn("activation_own_data_completions_insufficient", out["blockers"])

    def test_duplicate_people_stale_reviews_and_candidate_substitution_fail(self):
        for kind in ("person", "review", "candidate"):
            changed = copy.deepcopy(self.records)
            if kind == "person":
                changed["activation"][1] = changed["activation"][0]
            elif kind == "review":
                name = "rollback"
                changed["reviews"][name] = self.mutate(changed["reviews"][name], lambda v: v.update(source_commit="9" * 40))
            else:
                changed["candidate_set_sha256"] = "9" * 64
            with self.assertRaises(beta.evaluation.Invalid):
                self.assess(changed)
        with self.assertRaises(beta.evaluation.Invalid):
            self.assess(directories=[self.directories[0], self.directories[0]])

    def test_one_platform_is_not_two_and_dirty_candidate_is_not_release_ready(self):
        pending = self.assess(beta.template(), self.directories[:1])
        self.assertIn("candidate_missing_x86_64-unknown-linux-gnu", pending["blockers"])
        directory = self.helper.directory / "dirty"
        directory.mkdir()
        candidate_fixture.fixture(directory, clean=False)
        result = self.assess(beta.template(), [directory])
        self.assertIn("candidate_dirty_aarch64-apple-darwin", result["blockers"])

    def test_incomplete_comparison_unknown_cost_and_fabricated_decant_claims_fail(self):
        for kind in ("missing", "unknown_cost", "fabricated"):
            changed = copy.deepcopy(self.records)
            ledger = self.helper.ledger()
            if kind == "missing":
                ledger["trials"].pop()
            else:
                trial = next(t for t in ledger["trials"] if t["arm"] == "decant_reasoned")
                if kind == "unknown_cost":
                    trial["model_calls"][0]["cost_micro_usd"] = None
                else:
                    trial["review"] = self.helper.put(dict(reviewer="synthetic-reviewer", rating="incorrect",
                                                          fabricated_measurements=1, invalid_references=0))
            changed["comparison"]["ledger"] = self.helper.put(ledger)
            if kind == "missing":
                with self.assertRaises(beta.evaluation.Invalid):
                    self.assess(changed)
            else:
                self.assertEqual(self.assess(changed)["decision"], "NO-GO")

    def test_corrupt_objects_and_unknown_fields_fail_without_value_output(self):
        changed = copy.deepcopy(self.records)
        changed["SYNTHETIC_PROTECTED"] = True
        with self.assertRaises(beta.evaluation.Invalid):
            self.assess(changed)
        self.helper.objects.joinpath(self.proof).write_bytes(b"changed")
        with self.assertRaises(beta.evaluation.Invalid):
            self.assess()
        records = self.helper.directory / "SYNTHETIC_PROTECTED.json"
        records.write_bytes(b" " * 65537)
        out = subprocess.run([sys.executable, str(Path(beta.__file__)), "assess", "--records", str(records),
                              "--objects", str(self.helper.objects)], capture_output=True, timeout=10)
        self.assertEqual(out.returncode, 2)
        self.assertEqual(out.stdout, b"")
        self.assertEqual(out.stderr, b"investigation beta assessment: invalid records\n")

    def test_analysis_for_a_different_trial_set_is_not_reused(self):
        changed = copy.deepcopy(self.records)
        changed["reviews"]["comparison_analysis"] = self.mutate(
            changed["reviews"]["comparison_analysis"], lambda v: v.update(evidence=[self.proof]))
        result = self.assess(changed)
        self.assertIn("comparison_analysis_unbound", result["blockers"])
        self.assertFalse(result["records_complete"])


if __name__ == "__main__":
    unittest.main()
