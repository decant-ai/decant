"""Fail closed on changed raw observations, missing trials or edited summaries."""
import json
import hashlib
from pathlib import Path
import re
import statistics

from evaluate_evidence_v1 import ROOT, SPEC_SHA256, assert_report, bounded, require, sha, spec
from evidence_evaluation import baseline, fixtures


def frozen_input_identity(rows):
    hasher, size = hashlib.sha256(), 0
    for row in rows:
        line = baseline.encode(row) + b"\n"
        hasher.update(line)
        size += len(line)
    return hasher.hexdigest(), size


def summaries(runs, contract):
    expected = [(size, implementation, trial)
                for size in contract["load"]["sizes"]
                for trial in range(-1, contract["load"]["trials"])
                for implementation in (["decant", "sql"] if trial % 2 == 0 else ["sql", "decant"])]
    require([(r["size"], r["implementation"], r["trial"]) for r in runs] == expected, "trial set/order changed")
    output = []
    for run in runs:
        require(run["status"] == 0 and run["timeout"] is False, "failed trial omitted from gate")
        require(run["phase"] == ("warmup" if run["trial"] == -1 else "measured"), "warmup relabelled")
        require(run["seed"] == 104 and run["services"] == 8, "workload changed")
        require(isinstance(run["elapsed_ns"], int) and 0 < run["elapsed_ns"] <= 30 * 10**9, "duration target")
        require(isinstance(run["peak_rss_bytes"], int) and 0 < run["peak_rss_bytes"] <= 536870912, "memory target")
    for size in contract["load"]["sizes"]:
        for implementation in ("decant", "sql"):
            measured = [r for r in runs if r["size"] == size and r["implementation"] == implementation and r["phase"] == "measured"]
            values = [r["elapsed_ns"] for r in measured]
            output.append({"size": size, "implementation": implementation, "trials": len(measured),
                           "median_elapsed_ns": statistics.median(values), "range_elapsed_ns": max(values) - min(values),
                           "variance_elapsed_ns": statistics.pvariance(values),
                           "maximum_peak_rss_bytes": max(r["peak_rss_bytes"] for r in measured)})
    medians = [s["median_elapsed_ns"] for s in output if s["implementation"] == "decant"]
    require(medians[1] * 1000 <= medians[0] * 3500, "scaling target")
    return output


def validate(path):
    contract = spec()
    require(path.name == "result.json" and not path.is_symlink(), "result filename")
    result = json.loads(bounded(path))
    require(result["format"] == "decant-evidence-evaluation-result" and result["version"] == 1, "result contract")
    require(result["engineering_status"] == "passed" and result["external_commercial_status"] == "not_run", "unearned commercial claim")
    metadata = result["metadata"]
    require(metadata["spec_sha256"] == SPEC_SHA256 and re.fullmatch(r"[0-9a-f]{40}", metadata["source_commit"]), "source/spec identity")
    require(metadata["runner_sha256"] == sha(ROOT / "tools/evaluate_evidence_v1.py")
            and metadata["baseline_sha256"] == sha(ROOT / "tools/evidence_evaluation/baseline.py")
            and metadata["fixtures_sha256"] == sha(ROOT / "tools/evidence_evaluation/fixtures.py"), "evaluator implementation changed")
    require(metadata["cargo_lock_sha256"] == sha(ROOT / "Cargo.lock"), "dependency drift")
    require(metadata["binary_sha256"] == sha(ROOT / "target/release/decant"), "binary drift")
    require(result["summaries"] == summaries(result["runs"], contract), "summary does not match all raw runs")
    require([r["case"] for r in result["correctness"]] == contract["cases"] and all(r["status"] == "pass" for r in result["correctness"]), "correctness cases missing")
    directory = path.parent
    settings = json.loads(bounded(directory / "config.json", 16384))
    require(settings == {"format": "decant-evidence-config", "version": 1, "recipe": contract["recipe"]}, "recipe changed")
    for case in result["correctness"]:
        require(case["input_sha256"] == sha(directory / "correctness" / (case["case"] + ".jsonl")) == frozen_input_identity(fixtures.capture(case["case"]))[0], "correctness input changed")
    for size in contract["load"]["sizes"]:
        capture = directory / f"load-{size}.jsonl"
        require(not capture.is_symlink() and capture.stat().st_size <= 32 * 1024 * 1024, "input capacity")
        input_hash = sha(capture)
        require((input_hash, capture.stat().st_size) == frozen_input_identity(fixtures.load(size)), "frozen load input changed")
        oracle = baseline.aggregate(capture, contract["recipe"])
        require(oracle["accepted"] == size and len(oracle["services"]) == 8 and oracle["late"] == 0, "load facts changed")
        replay_hashes = set()
        for run in [r for r in result["runs"] if r["size"] == size]:
            stem = f'{size}-{run["implementation"]}-{run["trial"]}'
            require(run["stdout_file"] == stem + ".stdout" and run["stderr_file"] == stem + ".stderr" and run["resource_file"] == stem + ".resource.json", "artifact path changed")
            stdout, stderr, resource = [directory / run[field] for field in ("stdout_file", "stderr_file", "resource_file")]
            require(all(not item.is_symlink() for item in (stdout, stderr, resource)), "artifact symlink")
            require(run["input_sha256"] == input_hash and run["input_bytes"] == capture.stat().st_size, "input mismatch")
            require(run["stdout_sha256"] == sha(stdout) and run["stderr_sha256"] == sha(stderr), "raw output changed")
            require(run["output_bytes"] == stdout.stat().st_size and not bounded(stderr), "output status changed")
            observations = json.loads(bounded(resource, 4096))
            require(set(observations) == {"status", "timeout", "elapsed_ns", "user_cpu_ns", "system_cpu_ns", "peak_rss_bytes"}, "resource fields")
            require(all(run[key] == value for key, value in observations.items()), "resource summary altered")
            document = json.loads(bounded(stdout, 16385))
            if run["implementation"] == "decant":
                assert_report(document, oracle, contract["recipe"])
                replay_hashes.add(run["stdout_sha256"])
            else:
                require(document["services"] == oracle["services"] and document["omitted_services"] == 0, "baseline facts changed")
        require(len(replay_hashes) == 1, "replay differs")
    return result
