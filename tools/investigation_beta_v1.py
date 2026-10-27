#!/usr/bin/env python3
"""Assess local beta evidence completeness; never authorize or perform publication."""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

sys.dont_write_bytecode = True
if __package__:
    from tools import investigation_candidate_v1 as candidate
    from tools import investigation_evaluation_v1 as evaluation
else:
    import investigation_candidate_v1 as candidate
    import investigation_evaluation_v1 as evaluation

REVIEWS = (
    "quality_full", "quality_investigation", "independent_security_correctness",
    "native_dependency_review", "clean_install_linux", "clean_install_macos",
    "rollback", "comparison_analysis", "signing_platform_review", "publication_review",
)
INDEPENDENT = {"independent_security_correctness", "clean_install_linux", "clean_install_macos", "comparison_analysis"}
TARGETS = ("aarch64-apple-darwin", "x86_64-unknown-linux-gnu")
COMMON = ("format", "version", "source_commit", "candidate_set_sha256")
require, exact = evaluation.require, evaluation.exact


def template():
    return {"format": "decant-investigation-beta-records", "version": 1,
            "source_commit": None, "candidate_set_sha256": None,
            "reviews": dict.fromkeys(REVIEWS), "activation": [],
            "comparison": {"registration": None, "ledger": None}}


def record(objects, identity):
    data = objects.read(identity)
    require(len(data) <= 65536)
    return evaluation.document(data)


def bound(value, format_name, source, cut):
    require(value["format"] == format_name and type(value["version"]) is int and value["version"] == 1)
    require(source is not None and value["source_commit"] == source and value["candidate_set_sha256"] == cut)


def evidence(objects, references, required):
    require(isinstance(references, list) and len(references) <= 16 and (references or not required))
    seen = set()
    for identity in references:
        data = objects.read(identity)
        require(data and identity not in seen)
        seen.add(identity)


def assess(directories, records_raw, objects):
    require(len(directories) <= 2)
    manifests, identities = {}, {}
    blockers = []
    source = None
    for directory in directories:
        manifest = candidate.verify(directory)
        target = manifest["target"]
        require(target not in manifests)
        if source is None:
            source = manifest["source_commit"]
        require(manifest["source_commit"] == source)
        manifests[target] = manifest
        identities[target] = candidate.digest(candidate.regular(directory / "manifest.json"))
        if not manifest["source_tree_clean"]:
            blockers.append("candidate_dirty_" + target)
    cut = evaluation.digest(evaluation.canonical(identities))
    for target in TARGETS:
        if target not in manifests:
            blockers.append("candidate_missing_" + target)
    records = evaluation.document(records_raw)
    exact(records, template())
    require(records["format"] == "decant-investigation-beta-records"
            and type(records["version"]) is int and records["version"] == 1)
    if records["source_commit"] is None:
        blockers.append("source_commit_unconfirmed")
    else:
        require(source is not None and records["source_commit"] == source)
    if records["candidate_set_sha256"] is None:
        blockers.append("candidate_set_unconfirmed")
    else:
        require(records["candidate_set_sha256"] == cut)
    exact(records["reviews"], REVIEWS)
    analysis_evidence = []
    for name in REVIEWS:
        identity = records["reviews"][name]
        if identity is None:
            blockers.append(name + "_missing")
            continue
        review = record(objects, identity)
        exact(review, (*COMMON, "scope", "reviewer", "independent", "status", "evidence"))
        bound(review, "decant-investigation-review", source, cut)
        require(review["scope"] == name and isinstance(review["reviewer"], str)
                and evaluation.ID.fullmatch(review["reviewer"]) is not None)
        require(type(review["independent"]) is bool and review["status"] in ("pending", "passed", "failed"))
        evidence(objects, review["evidence"], review["status"] == "passed")
        if name == "comparison_analysis" and review["status"] == "passed":
            analysis_evidence = review["evidence"]
        if review["status"] != "passed":
            blockers.append(name + "_" + review["status"])
        if name in INDEPENDENT and not review["independent"]:
            blockers.append(name + "_not_independent")

    activation = records["activation"]
    require(isinstance(activation, list) and len(activation) <= 20)
    participants = set()
    counts = dict(attempts=0, eligible_attempts=0, own_data_completions=0, repeat_users=0, prefer_another=0)
    platforms = set()
    for identity in activation:
        attempt = record(objects, identity)
        flags = ("consent_confirmed", "independent", "assisted", "installed", "own_data_completed",
                 "repeat_investigation", "prefers_another")
        exact(attempt, (*COMMON, "participant", "target", *flags, "evidence"))
        bound(attempt, "decant-investigation-activation", source, cut)
        person = attempt["participant"]
        require(isinstance(person, str) and evaluation.ID.fullmatch(person) is not None and person not in participants)
        participants.add(person)
        require(attempt["target"] in TARGETS and all(type(attempt[flag]) is bool for flag in flags))
        require(not attempt["own_data_completed"] or attempt["installed"])
        require(not attempt["repeat_investigation"] or attempt["own_data_completed"])
        require(not attempt["prefers_another"] or attempt["own_data_completed"])
        evidence(objects, attempt["evidence"], True)
        counts["attempts"] += 1
        if attempt["consent_confirmed"] and attempt["independent"] and not attempt["assisted"]:
            counts["eligible_attempts"] += 1
            counts["own_data_completions"] += int(attempt["own_data_completed"])
            counts["repeat_users"] += int(attempt["repeat_investigation"])
            counts["prefer_another"] += int(attempt["prefers_another"])
            if attempt["own_data_completed"]:
                platforms.add(attempt["target"])
    for name, minimum in (("own_data_completions", 5), ("repeat_users", 3), ("prefer_another", 3)):
        if counts[name] < minimum:
            blockers.append("activation_" + name + "_insufficient")
    for target in TARGETS:
        if target not in platforms:
            blockers.append("activation_missing_" + target)

    exact(records["comparison"], ("registration", "ledger"))
    comparison = records["comparison"]
    summary_identity = None
    if comparison["registration"] is None or comparison["ledger"] is None:
        blockers.append("comparison_trials_missing")
    else:
        summary = evaluation.summarize(objects.read(comparison["registration"]),
                                       objects.read(comparison["ledger"]), objects)
        require(source is not None and summary["source_commit"] == source)
        if (len(analysis_evidence) < 3 or comparison["registration"] not in analysis_evidence
                or comparison["ledger"] not in analysis_evidence):
            blockers.append("comparison_analysis_unbound")
        summary_identity = evaluation.digest(evaluation.canonical(summary))
        for group in summary["scenarios"]:
            for trial in group["trials"]:
                if trial["cost_micro_usd"] is None:
                    blockers.append("comparison_cost_unknown")
                if group["arm"] in ("decant_reasoned", "decant_deterministic"):
                    if trial["fabricated_measurements"] or trial["invalid_references"]:
                        blockers.append("comparison_invalid_decant_evidence")
    blockers = sorted(set(blockers))
    return {"format": "decant-investigation-beta-assessment", "version": 1,
            "decision": "NO-GO" if blockers else "FOUNDER_REVIEW_REQUIRED",
            "records_complete": not blockers, "publication_authorized": False,
            "authenticity_verified": False, "source_commit": source,
            "candidate_set_sha256": cut, "candidate_targets": sorted(manifests),
            "counts": counts, "comparison_summary_sha256": summary_identity,
            "blockers": blockers}


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise evaluation.Invalid


def main():
    parser = Parser(description=__doc__)
    parser.add_argument("command", choices=("template", "assess"))
    parser.add_argument("--candidate", type=Path, action="append", default=[])
    parser.add_argument("--records", type=Path)
    parser.add_argument("--objects", type=Path)
    try:
        args = parser.parse_args()
        if args.command == "template":
            require(not args.candidate and args.records is None and args.objects is None)
            output = template()
            status = 0
        else:
            require(args.records is not None and args.objects is not None)
            output = assess(args.candidate, evaluation.regular(args.records, 65536), evaluation.Objects(args.objects))
            status = 0 if output["records_complete"] else 1
        sys.stdout.buffer.write(evaluation.canonical(output))
        return status
    except (evaluation.Invalid, candidate.CandidateFailure, OSError, ValueError, TypeError, KeyError, RecursionError):
        print("investigation beta assessment: invalid records", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
