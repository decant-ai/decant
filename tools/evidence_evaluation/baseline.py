"""Independent raw-OTLP SQLite oracle. No Decant library/output imports."""
import json
import sqlite3


def aggregate(path, recipe):
    # This reference supports the evaluation's numeric-OTLP, <=20k-span corpus;
    # it is not a general ingestion replacement or an untrusted-input service.
    if path.stat().st_size > 32 * 1024 * 1024:
        raise ValueError("reference input bound")
    accepted, maximum, late = [], None, 0
    with path.open("rb") as source:
        for line_number, line in enumerate(source, 1):
            if line_number > 40000 or len(line) > 4 * 1024 * 1024:
                raise ValueError("reference record bound")
            for ri, resource in enumerate(json.loads(line)["resourceSpans"]):
                attrs = {a["key"]: a["value"]["stringValue"] for a in resource["resource"].get("attributes", [])}
                for si, scope in enumerate(resource["scopeSpans"]):
                    for pi, span in enumerate(scope["spans"]):
                        time = int(span["startTimeUnixNano"])
                        if maximum is not None and time < maximum - 30 * 10**9:
                            late += 1
                            continue
                        maximum = time if maximum is None else max(maximum, time)
                        if len(accepted) >= 40000:
                            raise ValueError("reference span bound")
                        accepted.append((span["traceId"].lower(), span["spanId"].lower(), time,
                                         int(span["endTimeUnixNano"]) - time, span.get("kind", 0),
                                         span.get("status", {}).get("code", 0), attrs.get("service.name"),
                                         attrs.get("service.version"), (line_number, ri, si, pi)))
    anchor = maximum + 1 if maximum is not None else None
    middle = anchor - recipe["window_ns"] if anchor is not None else None
    start = middle - recipe["window_ns"] if middle is not None else None
    cutoff = anchor - recipe["retention_ns"] if anchor is not None else None
    identities, expired, duplicates = {}, 0, 0
    for span in accepted:
        if span[2] < cutoff:
            expired += 1
            continue
        key = span[:2]
        if key in identities:
            if identities[key][2:8] != span[2:8]:
                raise ValueError("conflicting span")
            duplicates += 1
        else:
            identities[key] = span
    non_server = sum(s[4] != 2 for s in identities.values())
    missing = sum(s[4] == 2 and s[6] is None for s in identities.values())
    eligible = [s for s in identities.values() if s[4] == 2 and s[6] is not None and s[2] >= start]
    services = sorted({s[6] for s in eligible})
    if len(services) > recipe["max_services"]:
        raise ValueError("service capacity")
    database = sqlite3.connect(":memory:")
    database.execute("CREATE TABLE spans(service TEXT, phase TEXT, time INTEGER, ordinal INTEGER, duration INTEGER, status INTEGER, version TEXT, reference TEXT)")
    for ordinal, span in enumerate(sorted(eligible, key=lambda s: (s[2], s[8]))):
        reference = "decant://event/v0/otel-evidence-v1/otlp-file/" + "/".join(map(str, span[8]))
        database.execute("INSERT INTO spans VALUES(?,?,?,?,?,?,?,?)", (span[6], "baseline" if span[2] < middle else "current", span[2], ordinal, span[3], span[5], span[7], reference))
    database.execute("CREATE INDEX grouped_time ON spans(service,phase,time,ordinal)")
    result = []
    for service in services:
        entry = {"service": service}
        for phase in ("baseline", "current"):
            params = (service, phase)
            count, errors, unset, duration_sum, missing_versions = database.execute(
                "SELECT COUNT(*), COALESCE(SUM(status=2),0), COALESCE(SUM(status=0),0), COALESCE(SUM(duration),0), COALESCE(SUM(version IS NULL),0) FROM spans WHERE service=? AND phase=?", params).fetchone()
            p95 = database.execute("""SELECT duration,reference FROM (
                SELECT duration,reference,ROW_NUMBER() OVER (ORDER BY duration,time,ordinal) AS rank,COUNT(*) OVER () AS n
                FROM spans WHERE service=? AND phase=?) WHERE rank=(95*n+99)/100""", params).fetchone()
            versions = [row[0] for row in database.execute("SELECT version FROM spans WHERE service=? AND phase=? AND version IS NOT NULL ORDER BY time,ordinal", params)]
            references = [row[0] for row in database.execute("SELECT reference FROM spans WHERE service=? AND phase=? ORDER BY time,ordinal", params)]
            selected = list(dict.fromkeys(([references[0], references[-1]] + [p95[1]]) if references else []))
            allow_versions = recipe["policy"]["allow_service_versions"]
            entry[phase] = {"observed_server_spans": count, "error_spans": errors,
                "error_fraction": {"numerator": errors, "denominator": count} if count else None,
                "unset_status_spans": unset, "p95_duration_ns": p95[0] if p95 else None,
                "duration_sum_ns": str(duration_sum), "missing_version_spans": missing_versions,
                "first_observed_version": versions[0] if versions and allow_versions else None,
                "last_observed_version": versions[-1] if versions and allow_versions else None,
                "observed_version_transitions": sum(a != b for a, b in zip(versions, versions[1:])) if allow_versions else None,
                "selected_references": selected}
        before, after = entry["baseline"]["last_observed_version"], entry["current"]["last_observed_version"]
        entry["observed_version_differs"] = before != after if before is not None and after is not None else None
        result.append(entry)
    database.close()
    return {"services": result, "anchor_ns": anchor, "accepted": len(accepted), "late": late,
            "expired": expired, "duplicates": duplicates, "non_server": non_server, "missing_service": missing}


def packed(oracle, budget):
    result = {"format": "decant-evaluation-sql-reference", "version": 1,
              "sampling": "unknown", "upstream_completeness": "unknown", "causality": "not_established",
              "candidate_services": len(oracle["services"]), "omitted_services": len(oracle["services"]),
              "late": oracle["late"], "duplicates": oracle["duplicates"], "expired": oracle["expired"], "services": []}
    for service in oracle["services"]:
        result["services"].append(service)
        result["omitted_services"] -= 1
        if len(encode(result)) > budget:
            result["services"].pop()
            result["omitted_services"] += 1
            break
    return encode(result)


def encode(value):
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
