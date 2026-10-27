#!/usr/bin/env python3
"""Validate local investigation comparison records; never run agents or publish claims."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys

sys.dont_write_bytecode = True
MAX_JSON = 8 * 1024 * 1024
MAX_OBJECT = 64 * 1024 * 1024
MAX_TOTAL = 16 * 1024 * 1024 * 1024
HASH = re.compile(r"[0-9a-f]{64}\Z")
ID = re.compile(r"[a-zA-Z0-9_.-]{1,96}\Z")
FAMILIES = (
    "local_latency", "downstream_latency", "error_increase", "traffic_mix",
    "version_only", "healthy", "missing_baseline", "partial_backends",
    "conflicting_evidence", "out_of_scope",
)
ARMS = {"decant_reasoned": 5, "holmesgpt": 5, "grafana_mcp": 5,
        "engineer": 1, "decant_deterministic": 1}
REGIMES = ("equal_budget", "normal_defaults")
USAGE = ("input_tokens", "output_tokens", "cache_read_tokens", "cache_write_tokens", "cost_micro_usd")


class Invalid(Exception):
    pass


def require(ok):
    if not ok:
        raise Invalid


def exact(value, fields):
    require(isinstance(value, dict) and set(value) == set(fields))


def number(value, maximum=10**15):
    require(type(value) is int and 0 <= value <= maximum)


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def pairs(items):
    result = {}
    for key, value in items:
        require(key not in result)
        result[key] = value
    return result


def document(data):
    require(len(data) <= MAX_JSON)
    # Reject excessive nesting before the recursive JSON parser allocates it.
    depth = 0
    quoted = escaped = False
    for byte in data:
        if quoted:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                quoted = False
        elif byte == 34:
            quoted = True
        elif byte in (91, 123):
            depth += 1
            require(depth <= 24)
        elif byte in (93, 125):
            depth -= 1
            require(depth >= 0)
    return json.loads(data, object_pairs_hook=pairs, parse_constant=lambda _: require(False))


def regular(path, maximum):
    before = path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink == 1 and before.st_size <= maximum)
    # Do not block opening a substituted FIFO; reject a substituted symlink too.
    descriptor = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as handle:
        info = os.fstat(handle.fileno())
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size <= maximum)
        data = handle.read(maximum + 1)
    require(len(data) <= maximum)
    return data


class Objects:
    def __init__(self, directory):
        require(directory.is_dir() and not directory.is_symlink())
        self.directory = directory
        self.seen = set()
        self.total = 0

    def read(self, identity):
        require(isinstance(identity, str) and HASH.fullmatch(identity) is not None)
        data = regular(self.directory / identity, MAX_OBJECT)
        require(digest(data) == identity)
        # Charge repeated reads too; edits never benefit from a verification cache.
        self.total += len(data)
        self.seen.add(identity)
        require(self.total <= MAX_TOTAL and len(self.seen) <= 20000)
        return data


def slots():
    return [{"id": f"{split}-{family}-{index}", "split": split, "family": family,
             "input": None, "expected": None}
            for split in ("development", "held_out")
            for family in FAMILIES for index in range(1, 4)]


def template():
    return {
        "format": "decant-investigation-evaluation-registration", "version": 1,
        "status": "incomplete_template", "source_commit": None,
        "evaluator_sha256": digest(Path(__file__).read_bytes()),
        "model": None, "environment": None, "fairness_protocol": None,
        "comparators": [{"id": arm, "configuration": None} for arm in ARMS],
        "regimes": list(REGIMES), "repeats": ARMS, "cases": slots(),
    }


def registration(raw, objects):
    value = document(raw)
    exact(value, template())
    require(value["format"] == "decant-investigation-evaluation-registration"
            and type(value["version"]) is int and value["version"] == 1
            and value["status"] == "registered")
    require(isinstance(value["source_commit"], str)
            and re.fullmatch(r"[0-9a-f]{40}", value["source_commit"]) is not None)
    require(value["evaluator_sha256"] == digest(Path(__file__).read_bytes()))
    require(isinstance(value["model"], str) and ID.fullmatch(value["model"]) is not None)
    require(value["regimes"] == list(REGIMES) and value["repeats"] == ARMS)
    for count in value["repeats"].values():
        number(count, 5)
    objects.read(value["environment"])
    objects.read(value["fairness_protocol"])
    require(isinstance(value["comparators"], list) and len(value["comparators"]) == len(ARMS))
    names = set()
    for arm in value["comparators"]:
        exact(arm, ("id", "configuration"))
        require(isinstance(arm["id"], str) and arm["id"] in ARMS and arm["id"] not in names)
        names.add(arm["id"])
        objects.read(arm["configuration"])
    require(isinstance(value["cases"], list) and len(value["cases"]) == 60)
    expected_slots = {case["id"]: case for case in slots()}
    identities = set()
    for case in value["cases"]:
        exact(case, ("id", "split", "family", "input", "expected"))
        require(isinstance(case["id"], str) and case["id"] in expected_slots)
        expected = expected_slots.pop(case["id"])
        require(case["split"] == expected["split"] and case["family"] == expected["family"])
        objects.read(case["input"])
        objects.read(case["expected"])
        identity = (case["input"], case["expected"])
        require(identity not in identities)
        identities.add(identity)
    return value


def trial_keys():
    return {(case["id"], arm, regime, repeat)
            for case in slots() if case["split"] == "held_out"
            for arm, repeats in ARMS.items() for regime in REGIMES
            for repeat in range(1, repeats + 1)}


def summarize(registration_raw, ledger_raw, objects):
    registered = registration(registration_raw, objects)
    ledger = document(ledger_raw)
    exact(ledger, ("format", "version", "registration_sha256", "trials"))
    require(ledger["format"] == "decant-investigation-evaluation-ledger"
            and type(ledger["version"]) is int and ledger["version"] == 1
            and ledger["registration_sha256"] == digest(registration_raw))
    remaining = trial_keys()
    require(isinstance(ledger["trials"], list) and len(ledger["trials"]) == len(remaining))
    groups = {}
    for trial in ledger["trials"]:
        exact(trial, ("case", "arm", "regime", "repeat", "status", "answer",
                      "measurement", "review", "model_calls", "backend_calls"))
        require(all(isinstance(trial[k], str) for k in ("case", "arm", "regime")))
        number(trial["repeat"], 5)
        key = (trial["case"], trial["arm"], trial["regime"], trial["repeat"])
        require(key in remaining)
        remaining.remove(key)
        require(trial["status"] in ("completed", "failed", "timeout"))
        objects.read(trial["answer"])
        measurement = document(objects.read(trial["measurement"]))
        exact(measurement, ("wall_ms", "cpu_ms", "peak_rss_bytes", "disk_bytes"))
        for value in measurement.values():
            number(value)
        review = document(objects.read(trial["review"]))
        exact(review, ("reviewer", "rating", "fabricated_measurements", "invalid_references"))
        require(isinstance(review["reviewer"], str) and ID.fullmatch(review["reviewer"]) is not None)
        require(review["rating"] in ("correct_supported", "correct_abstention", "incorrect", "unscorable"))
        number(review["fabricated_measurements"])
        number(review["invalid_references"])
        correct = review["rating"] in ("correct_supported", "correct_abstention")
        require(not correct or (trial["status"] == "completed"
                                and review["fabricated_measurements"] == 0
                                and review["invalid_references"] == 0))
        for name in ("model_calls", "backend_calls"):
            require(isinstance(trial[name], list) and len(trial[name]) <= 1024)
        for backend in trial["backend_calls"]:
            objects.read(backend)
        require(trial["arm"] not in ("engineer", "decant_deterministic") or not trial["model_calls"])
        usage = {field: 0 for field in USAGE}
        for call in trial["model_calls"]:
            exact(call, ("request", "response", *USAGE))
            objects.read(call["request"])
            objects.read(call["response"])
            for field in USAGE:
                if call[field] is None:
                    usage[field] = None
                else:
                    number(call[field])
                    if usage[field] is not None:
                        usage[field] += call[field]
        # A completed reasoning-agent trial must account for its model access.
        require(trial["status"] != "completed" or trial["arm"] in ("engineer", "decant_deterministic")
                or len(trial["model_calls"]) > 0)
        group = groups.setdefault(key[:3], [])
        group.append({"repeat": key[3], "status": trial["status"], "rating": review["rating"],
                      "fabricated_measurements": review["fabricated_measurements"],
                      "invalid_references": review["invalid_references"], **measurement,
                      "model_calls": len(trial["model_calls"]),
                      "backend_calls": len(trial["backend_calls"]), **usage})
    require(not remaining)
    # Retain per-scenario observations; do not pool repeated runs into independent samples.
    result = {
        "format": "decant-investigation-evaluation-summary", "version": 1,
        "registration_sha256": digest(registration_raw), "ledger_sha256": digest(ledger_raw),
        "source_commit": registered["source_commit"], "trial_count": len(ledger["trials"]),
        "comparative_conclusion": "requires_independent_analysis",
        "authenticity_verified": False, "publication_approved": False,
        "scenarios": [{"case": key[0], "arm": key[1], "regime": key[2],
                       "trials": sorted(values, key=lambda t: t["repeat"])}
                      for key, values in sorted(groups.items())],
    }
    require(len(canonical(result)) <= MAX_JSON)
    return result


class Parser(argparse.ArgumentParser):
    def error(self, message):
        raise Invalid


def main():
    parser = Parser(description=__doc__)
    parser.add_argument("command", choices=("template", "registration", "summarize"))
    parser.add_argument("--registration", type=Path)
    parser.add_argument("--ledger", type=Path)
    parser.add_argument("--objects", type=Path)
    try:
        args = parser.parse_args()
        if args.command == "template":
            require(args.registration is None and args.ledger is None and args.objects is None)
            output = template()
        else:
            require(args.registration is not None and args.objects is not None)
            raw = regular(args.registration, MAX_JSON)
            objects = Objects(args.objects)
            if args.command == "registration":
                require(args.ledger is None)
                registration(raw, objects)
                output = {"registration_sha256": digest(raw), "records_valid": True,
                          "preregistration_timing_verified": False, "trials_run": False}
            else:
                require(args.ledger is not None)
                output = summarize(raw, regular(args.ledger, MAX_JSON), objects)
        sys.stdout.buffer.write(canonical(output))
    except (Invalid, OSError, ValueError, TypeError, KeyError, RecursionError):
        print("investigation evaluation: invalid or incomplete records", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
