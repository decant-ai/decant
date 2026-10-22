#!/usr/bin/env python3
"""Explicit offline evidence evaluation. Never publishes measurements."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import resource
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time

from evidence_evaluation import baseline, fixtures

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "release/evidence-evaluation-spec-v1.json"
SPEC_SHA256 = "09f6dba6c676b8cccccfe9e4e5bf6aef01bf4f943b8b2119d7669fd4419985db"
MAX_ARTIFACT = 1024 * 1024


def require(condition, reason):
    if not condition:
        raise ValueError(reason)


def sha(path):
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(65536), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def spec():
    require(sha(SPEC) == SPEC_SHA256, "preregistered specification changed")
    return json.loads(SPEC.read_bytes())


def bounded(path, limit=MAX_ARTIFACT):
    require(path.stat().st_size <= limit, "evaluation artifact bound")
    return path.read_bytes()


def config(recipe):
    return {"format": "decant-evidence-config", "version": 1, "recipe": recipe}


def assert_report(report, oracle, recipe, allow_omissions=False):
    require(report["format"] == "decant-evidence" and report["version"] == 1, "report contract")
    require(report["candidate_services"] == len(oracle["services"]), "candidate count")
    count = len(report["services"])
    require(report["services"] == oracle["services"][:count], "independent SQL facts or provenance mismatch")
    require(report["omitted_services"] == len(oracle["services"]) - count, "omission count")
    require(allow_omissions or report["omitted_services"] == 0, "required service facts omitted")
    require(report["source_cut"]["anchor_ns"] == oracle["anchor_ns"], "data cut mismatch")
    require(report["source_cut"]["accepted_retained_entries"] == oracle["accepted"], "admission count")
    for field, expected in [("logical_duplicates_folded", "duplicates"), ("expired_entries_excluded", "expired"),
                            ("non_server_spans_excluded", "non_server"), ("missing_service_spans", "missing_service")]:
        require(report["gaps"][field] == oracle[expected], "gap mismatch")
    require(sum(report["gaps"]["rejections"]) == oracle["late"], "late admission accounting")
    require(report["gaps"]["sampling"] == "unknown" and report["gaps"]["upstream_completeness"] == "unknown", "sampling overclaim")
    require(report["recipe"] == recipe and report["gaps"]["raw_fields_emitted"] is False, "policy mismatch")
    require(fixtures.SENTINEL.encode() not in baseline.encode(report), "egress sentinel")


def frozen_oracle(case, oracle):
    # Hand-calculated anchors separate from either compiler or SQL aggregation.
    if case in {"regression", "regression_no_version_change", "duplicates", "late"}:
        service = oracle["services"][0]
        require(service["baseline"]["observed_server_spans"] == 2 and service["current"]["observed_server_spans"] == 2, "frozen counts")
        require(service["baseline"]["p95_duration_ns"] == 20000000 and service["current"]["p95_duration_ns"] == 200000000, "frozen p95")
        require(service["baseline"]["error_spans"] == 1 and service["current"]["error_spans"] == 1, "frozen errors")
        require(service["observed_version_differs"] == (case != "regression_no_version_change"), "noncausal version comparison")
    if case in {"healthy", "version_only"}:
        for interval in ("baseline", "current"):
            row = oracle["services"][0][interval]
            require(row["observed_server_spans"] == 2 and row["error_spans"] == 0 and row["p95_duration_ns"] == 10000000, "healthy oracle")
        require(oracle["services"][0]["observed_version_differs"] == (case == "version_only"), "version-only oracle")
    if case == "duplicates":
        require(oracle["duplicates"] == 4, "duplicate oracle")
    if case == "late":
        require(oracle["late"] == 1 and oracle["duplicates"] == 0, "late oracle")
    if case == "missing_baseline":
        require(oracle["services"][0]["baseline"]["p95_duration_ns"] is None, "unknown baseline")
    if case == "missing_version":
        require(oracle["services"][0]["observed_version_differs"] is None, "unknown version")
    if case in {"missing_service", "non_server"}:
        require(not oracle["services"], "no service facts")


def correctness(binary, directory):
    contract = spec()
    results = []
    for name in contract["cases"]:
        recipe = json.loads(json.dumps(contract["recipe"]))
        if name == "partial_retention":
            recipe["retention_ns"] = 300000000000
        if name == "output_omission":
            recipe["output_bytes"] = 2048
        capture, settings = directory / (name + ".jsonl"), directory / (name + ".config.json")
        fixtures.write_capture(capture, fixtures.capture(name))
        settings.write_bytes(baseline.encode(config(recipe)))
        argv = [str(binary), "evidence", "analyze", "--file", str(capture), "--config", str(settings)]
        first = subprocess.run(argv, check=False, capture_output=True, timeout=30)
        second = subprocess.run(argv, check=False, capture_output=True, timeout=30)
        for result in (first, second):
            require(len(result.stdout) <= recipe["output_bytes"] + 1 and len(result.stderr) <= 1024, "CLI bound")
            require(fixtures.SENTINEL.encode() not in result.stdout + result.stderr, "output sentinel")
        require((first.returncode, first.stdout, first.stderr) == (second.returncode, second.stdout, second.stderr), "replay differs")
        if name in {"conflict", "high_cardinality"}:
            require(first.returncode == 1 and not first.stdout, "expected whole-report refusal")
            try:
                baseline.aggregate(capture, recipe)
            except ValueError:
                pass
            else:
                raise ValueError("baseline failed to refuse")
        else:
            require(first.returncode == 0 and not first.stderr, "unexpected CLI failure")
            oracle = baseline.aggregate(capture, recipe)
            frozen_oracle(name, oracle)
            report = json.loads(first.stdout)
            assert_report(report, oracle, recipe, allow_omissions=name == "output_omission")
            if name == "partial_retention":
                require(report["gaps"]["history_coverage"] == "partial", "retention gap hidden")
            if name == "output_omission":
                require(report["omitted_services"] > 0, "output gap hidden")
            else:
                require(report["omitted_services"] == 0, "unexpected omission")
            reference = baseline.packed(oracle, recipe["output_bytes"])
            require(len(reference) <= recipe["output_bytes"] and fixtures.SENTINEL.encode() not in reference, "reference bound/egress")
        results.append({"case": name, "status": "pass", "input_sha256": sha(capture),
                        "output_sha256": hashlib.sha256(first.stdout).hexdigest(), "expected_refusal": name in {"conflict", "high_cardinality"}})
    return results


def measured_child(args):
    # This wrapper is a fresh process and starts exactly one child. Thus the
    # per-children peak is that child's peak, not a reused cumulative maximum.
    started = time.perf_counter_ns()
    timed_out = False
    with args.stdout.open("xb") as output, args.stderr.open("xb") as error:
        try:
            result = subprocess.run(args.command, stdout=output, stderr=error, timeout=30, check=False)
            status = result.returncode
        except subprocess.TimeoutExpired:
            timed_out, status = True, None
    elapsed = time.perf_counter_ns() - started
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    print(json.dumps({"status": status, "timeout": timed_out, "elapsed_ns": elapsed,
                      "user_cpu_ns": round(usage.ru_utime * 1e9), "system_cpu_ns": round(usage.ru_stime * 1e9),
                      "peak_rss_bytes": usage.ru_maxrss if sys.platform == "darwin" else usage.ru_maxrss * 1024}))


def command_text(command):
    return subprocess.run(command, cwd=ROOT, check=True, capture_output=True, timeout=30).stdout.decode().strip()


def metadata(binary, notes):
    require(not command_text(["git", "status", "--porcelain", "--", "crates", "Cargo.toml", "Cargo.lock"]), "production sources must match committed build")
    machine = json.loads(bounded(notes, 8192))
    require(set(machine) == {"cpu", "ram_bytes", "storage", "filesystem"} and isinstance(machine["ram_bytes"], int) and machine["ram_bytes"] > 0, "machine record required")
    require(all(isinstance(machine[key], str) and 0 < len(machine[key]) <= 256 for key in ("cpu", "storage", "filesystem")), "machine record invalid")
    return {"source_commit": command_text(["git", "rev-parse", "HEAD"]), "binary_sha256": sha(binary),
            "recorded_at_utc": datetime.now(timezone.utc).isoformat(),
            "spec_sha256": sha(SPEC), "runner_sha256": sha(Path(__file__)),
            "baseline_sha256": sha(ROOT / "tools/evidence_evaluation/baseline.py"),
            "fixtures_sha256": sha(ROOT / "tools/evidence_evaluation/fixtures.py"),
            "cargo_lock_sha256": sha(ROOT / "Cargo.lock"), "rustc": command_text(["rustc", "--version"]),
            "python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
            "os": platform.system(), "kernel": platform.release(), "architecture": platform.machine(),
            "logical_cpus": os.cpu_count(), "machine": machine, "build_profile": "release"}


def load_run(args):
    contract = spec()
    binary = ROOT / "target/release/decant"
    facts = metadata(binary, args.machine_notes)
    output = args.output.resolve()
    require(output.is_relative_to(ROOT / "benchmarks/results") or output.is_relative_to(ROOT / ".agent-work"), "results must stay in an ignored directory")
    output.mkdir(parents=True, exist_ok=False)
    result = {"format": "decant-evidence-evaluation-result", "version": 1, "metadata": facts,
              "correctness": [], "runs": [], "summaries": [], "engineering_status": "failed", "external_commercial_status": "not_run"}
    try:
        correctness_dir = output / "correctness"
        correctness_dir.mkdir()
        result["correctness"] = correctness(binary, correctness_dir)
        settings = output / "config.json"
        settings.write_bytes(baseline.encode(config(contract["recipe"])))
        for size in contract["load"]["sizes"]:
            capture = output / f"load-{size}.jsonl"
            fixtures.write_capture(capture, fixtures.load(size))
            for trial in range(-1, contract["load"]["trials"]):
                order = ["decant", "sql"] if trial % 2 == 0 else ["sql", "decant"]
                for implementation in order:
                    name = f"{size}-{implementation}-{trial}"
                    stdout, stderr = output / (name + ".stdout"), output / (name + ".stderr")
                    command = ([str(binary), "evidence", "analyze", "--file", str(capture), "--config", str(settings)]
                               if implementation == "decant" else [sys.executable, str(Path(__file__)), "query", "--input", str(capture), "--config", str(settings)])
                    observation = subprocess.run([sys.executable, str(Path(__file__)), "measure", "--stdout", str(stdout), "--stderr", str(stderr), "--", *command], check=False, capture_output=True, timeout=40)
                    (output / (name + ".resource.json")).write_bytes(observation.stdout)
                    (output / (name + ".wrapper.stderr")).write_bytes(observation.stderr)
                    require(observation.returncode == 0, "measurement wrapper failed")
                    run = json.loads(observation.stdout)
                    run.update(size=size, services=8, seed=104, implementation=implementation, trial=trial,
                               phase="warmup" if trial == -1 else "measured", input_sha256=sha(capture), input_bytes=capture.stat().st_size,
                               stdout_sha256=sha(stdout), stderr_sha256=sha(stderr), output_bytes=stdout.stat().st_size,
                               stdout_file=stdout.name, stderr_file=stderr.name)
                    run["resource_file"] = name + ".resource.json"
                    result["runs"].append(run)
                    require(run["status"] == 0 and not run["timeout"], "process failure or timeout")
                    require(run["peak_rss_bytes"] <= contract["load"]["maximum_peak_rss_bytes"], "RSS target failed")
                    require(run["output_bytes"] <= 16385 and not bounded(stderr), "output boundary failed")
                    document = json.loads(bounded(stdout, 16385))
                    oracle = baseline.aggregate(capture, contract["recipe"])
                    if implementation == "decant":
                        assert_report(document, oracle, contract["recipe"])
                        repeated = [r for r in result["runs"] if r["size"] == size and r["implementation"] == "decant"]
                        require(len({r["stdout_sha256"] for r in repeated}) == 1, "load replay mismatch")
                    else:
                        require(document["services"] == oracle["services"] and document["omitted_services"] == 0, "SQL load facts")
                    require(fixtures.SENTINEL.encode() not in bounded(stdout), "load egress sentinel")
        for size in contract["load"]["sizes"]:
            for implementation in ("decant", "sql"):
                runs = [r for r in result["runs"] if r["size"] == size and r["implementation"] == implementation and r["phase"] == "measured"]
                values = [r["elapsed_ns"] for r in runs]
                result["summaries"].append({"size": size, "implementation": implementation, "trials": len(runs),
                    "median_elapsed_ns": statistics.median(values), "range_elapsed_ns": max(values) - min(values),
                    "variance_elapsed_ns": statistics.pvariance(values), "maximum_peak_rss_bytes": max(r["peak_rss_bytes"] for r in runs)})
        decant = [row["median_elapsed_ns"] for row in result["summaries"] if row["implementation"] == "decant"]
        require(decant[1] * 1000 <= decant[0] * contract["load"]["maximum_median_size_ratio_milli"], "scaling hypothesis failed")
        result["engineering_status"] = "passed"
    finally:
        # Keep failures and every completed observation; never overwrite a run.
        (output / "result.json").write_bytes(baseline.encode(result))
    print("PASS: internal evidence evaluation; external utility and publication gates remain")


def main():
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="mode", required=True)
    commands.add_parser("spec")
    commands.add_parser("correctness")
    query = commands.add_parser("query")
    query.add_argument("--input", type=Path, required=True)
    query.add_argument("--config", type=Path, required=True)
    run = commands.add_parser("run")
    run.add_argument("--output", type=Path, required=True)
    run.add_argument("--machine-notes", type=Path, required=True)
    measure = commands.add_parser("measure")
    measure.add_argument("--stdout", type=Path, required=True)
    measure.add_argument("--stderr", type=Path, required=True)
    measure.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    if args.mode == "spec":
        spec()
        print("PASS: evidence evaluation specification V1 (not measured)")
    elif args.mode == "query":
        recipe = json.loads(bounded(args.config, 16384))["recipe"]
        sys.stdout.buffer.write(baseline.packed(baseline.aggregate(args.input, recipe), recipe["output_bytes"]) + b"\n")
    elif args.mode == "measure":
        args.command = args.command[1:] if args.command[:1] == ["--"] else args.command
        measured_child(args)
    elif args.mode == "correctness":
        with tempfile.TemporaryDirectory(prefix="decant-evidence-eval-") as directory:
            results = correctness(ROOT / "target/debug/decant", Path(directory))
        print(f"PASS: {len(results)} frozen evidence cases; no performance measurement")
    else:
        load_run(args)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, subprocess.SubprocessError):
        print("evidence evaluation failed; retain raw run evidence", file=sys.stderr)
        raise SystemExit(1)
