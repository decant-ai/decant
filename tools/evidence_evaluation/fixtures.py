"""Frozen synthetic holdouts and load generator. Change semantics only as V2."""
import copy
import json

CASES = ("regression", "healthy", "version_only", "regression_no_version_change",
         "missing_baseline", "missing_version", "missing_service", "non_server",
         "duplicates", "conflict", "late", "partial_retention", "output_omission", "high_cardinality")
SENTINEL = "SYNTHETIC-EVALUATION-PROTECTED-104"


def record(index, time_ns, duration, status, service="checkout", version="v1", kind=2):
    attributes = []
    if service is not None:
        attributes.append({"key": "service.name", "value": {"stringValue": service}})
    if version is not None:
        attributes.append({"key": "service.version", "value": {"stringValue": version}})
    return {"resourceSpans": [{"resource": {"attributes": attributes}, "scopeSpans": [{"spans": [{
        "traceId": "00000000000000000000000000000001", "spanId": f"{index + 1:016x}",
        "name": SENTINEL, "kind": kind, "startTimeUnixNano": str(time_ns),
        "endTimeUnixNano": str(time_ns + duration), "status": {"code": status},
        "attributes": [{"key": "protected", "value": {"stringValue": SENTINEL}}],
        "flags": 1}]}]}]}


def capture(case):
    if case not in CASES:
        raise ValueError("unknown frozen case")
    rows = [record(i, time * 10**9, duration * 10**6, status, version=version)
            for i, (time, duration, status, version) in enumerate([
                (100, 10, 1, "v1"), (200, 20, 2, "v1"),
                (400, 100, 0, "v2"), (600, 200, 2, "v2")])]
    if case in {"healthy", "version_only"}:
        rows = [record(i, time * 10**9, 10 * 10**6, 1,
                       version=("v2" if i >= 2 and case == "version_only" else "v1"))
                for i, time in enumerate([100, 200, 400, 600])]
    if case == "regression_no_version_change":
        for row in rows:
            row["resourceSpans"][0]["resource"]["attributes"][1]["value"]["stringValue"] = "v1"
    if case == "missing_baseline":
        rows = rows[2:]
    if case in {"missing_version", "missing_service"}:
        for row in rows:
            row["resourceSpans"][0]["resource"]["attributes"].pop(1 if case == "missing_version" else 0)
    if case == "non_server":
        for row in rows:
            row["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["kind"] = 1
    if case == "duplicates":
        rows = [copy.deepcopy(row) for row in rows for _ in range(2)]
    if case == "late":
        rows.append(copy.deepcopy(rows[0]))
    if case == "conflict":
        changed = copy.deepcopy(rows[-1])
        changed["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["endTimeUnixNano"] = "600300000000"
        rows.append(changed)
    if case in {"high_cardinality", "output_omission"}:
        rows = [record(i, 600 * 10**9 + i, 10, 1, service=f"service-{i:03}")
                for i in range(257 if case == "high_cardinality" else 20)]
    return rows


def load(size):
    if size not in (10000, 20000):
        raise ValueError("unregistered workload size")
    for index in range(size):
        time = 100 * 10**9 + index * 500 * 10**9 // (size - 1)
        value = (index * 1664525 + 104) % 101
        changed = time >= 350 * 10**9
        yield record(index, time, (10 + value + (100 if changed else 0)) * 10**6,
                     2 if value < (20 if changed else 5) else 1,
                     service=f"service-{index % 8:03}", version="v2" if changed else "v1")


def write_capture(path, rows):
    with path.open("xb") as stream:
        for row in rows:
            stream.write(json.dumps(row, sort_keys=True, separators=(",", ":")).encode() + b"\n")
