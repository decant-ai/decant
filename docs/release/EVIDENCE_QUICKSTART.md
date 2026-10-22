# Analyze your own OTel trace capture

Use this path when you have—or can obtain—a completed Collector-exported trace
file. You do not need Kafka, an LLM or a Decant account. If your traces exist only
in an observability backend, Decant does **not** currently query that backend.
Its JSON download may be a different format and cannot simply be renamed JSONL.

## 1. Build once and check the bundled example

Follow [build and demo](../../README.md#build-and-run-the-demo) first. Commands
below run from the repository root and use `./target/release/decant`; if you
installed your own compatible binary, substitute its executable path.

The [README fixture walkthrough](../../README.md#make-and-verify-your-first-report)
needs no outside data. The [complete synthetic demo report](../../examples/evidence/report-v1.md)
shows the result, and the [fixture notes](../../examples/evidence/README.md)
explain its independently specified facts. This is not a performance test.

## 2. Obtain a compatible file

Required input: an uncompressed, completed **OTLP trace JSONL** regular file.
Each line is one JSON object with `resourceSpans`, with LF-terminated records,
valid trace/span IDs and timestamps, and numeric OTLP enum values. The evidence
recipe counts SERVER spans (`kind: 2`); another span kind does not substitute for
a server request. Service names come from resource attribute `service.name`.

If you run an OpenTelemetry Collector, its contrib file exporter is one way to
capture traces. This is a configuration fragment for a reviewed Collector
deployment, **not a complete config or a command to change production**:

```yaml
exporters:
  file/decant:
    path: /var/lib/otelcol/decant-traces.jsonl
    format: json
```

Add `file/decant` to the exporters list of the appropriate existing trace
pipeline, keeping its other exporters. The path must be writable by that
Collector and protected as sensitive telemetry. Use your change/review process;
do not copy the repository's test Collector into production. The checked-in
[Collector configuration](../../examples/opentelemetry-demo/otelcol-config-decant.yml)
and [pinned interoperability test](../../examples/evidence/README.md) show the
tested setup. This does not promise compatibility with every exporter version.

Obtain a **closed, stable capture** using your Collector's supported file
lifecycle or from an isolated capture session. Do not analyze or copy a file
while it is still being written. Move the completed file to an authorized local
location and protect it; Decant does not upload it anywhere on this path.
The [OTel Demo capture guide](../../examples/opentelemetry-demo/README.md) is an
optional local learning exercise, not a required source or product dependency.

Scope the capture to the environment and service set you intend to compare.
The current report groups by `service.name`, **not** environment, region or
endpoint; identically named services from different environments would be
combined. Do not remove the preceding baseline just to make the capture smaller.

Unsupported: OTLP protobuf, pretty-printed multi-line JSON, compressed files,
arbitrary backend JSON, OTLP logs/metrics, stdin/live growing files or a native
live OTLP endpoint. Kafka is a [separate explicitly mapped path](KAFKA_EVIDENCE.md),
not an automatic decoder for Collector OTLP Kafka messages.

## 3. Configure, analyze and read

Choose a fresh local output directory. `mkdir` must succeed before continuing;
shell redirections create/overwrite files. Replace `/absolute/path/traces.jsonl`
with your completed capture, not a directory or backend URL.

```sh
mkdir decant-own-report
./target/release/decant evidence init > decant-own-report/config.json
```

Review `decant-own-report/config.json` **before analysis or sharing**:

- Service names are permitted by default. Turn that permission off if they must
  not leave the local evidence boundary.
- Service-version labels are withheld by default. Enable `allow_service_versions`
  only after reviewing the values; the labelled demo opts in for synthetic values.
- Choose the window/retention and bounded output budget for the investigation.
  Defaults compare the latest five-minute interval with the preceding five minutes.

Span names, raw IDs, keys and arbitrary attributes are not emitted. This allowlist
is not anonymization; source captures and checkpoints remain potentially sensitive.

```sh
./target/release/decant evidence validate --config decant-own-report/config.json
./target/release/decant evidence analyze --file /absolute/path/traces.jsonl --config decant-own-report/config.json > decant-own-report/report.json
./target/release/decant evidence render --file decant-own-report/report.json
./target/release/decant evidence verify --file decant-own-report/report.json
```

Stop on a nonzero exit. A failed analysis may leave an empty redirection target;
it is not a valid report. Keep these generated files local and out of commits.
`analyze` writes canonical JSON plus LF. `render` presents those same verified
facts as Markdown; append `> decant-own-report/report.md` to save that rendering.
Verification checks shape, policy consistency and checksum, not source truth or
origin. Anyone can calculate a checksum; this is not a signature.

To check deterministic replay, keep the same capture/configuration and run:

```sh
./target/release/decant evidence analyze --file /absolute/path/traces.jsonl --config decant-own-report/config.json > decant-own-report/replayed.json
cmp decant-own-report/report.json decant-own-report/replayed.json
```

Success is exit status 0 with no `cmp` output. It proves byte equality for those
inputs, not completeness or authenticity of the original telemetry.

## 4. Interpret the measurements and gaps

The latest accepted source timestamp plus one nanosecond anchors two adjacent
half-open windows: current and preceding baseline. Wall-clock time does not move
an existing report. Configure smaller `window_ns` for short captures and retain
at least two windows when possible. Short/partial history does not prove a
healthy baseline. Missing version and sampling information stay unknown.

Counts include accepted SERVER spans (`kind: 2`). Error fractions are exact
counts with status code 2 divided by observed server spans; UNSET status (0) is
reported separately. p95 uses nearest rank over observed durations, in integer
nanoseconds (1,000,000 ns = 1 ms). These are not automatically whole-production
request rates or SLOs. A first version observation is not a deployment timestamp;
version transitions skip missing labels, whose counts remain visible.

Admission consumes file order with the existing 30-second lateness allowance;
it does not sort the capture. The report discloses late rejection, duplicate
folding, expiration, non-server/unspecified spans, missing services, decoder
omissions, upstream reported drops and budget omissions. Selected provenance
names positions from this exact report; full replay inputs remain separate.

Limits: 4 MiB per JSONL record, 100,000 records, 256 MiB input, 100,000 accepted
events/128 MiB accounted state, 4096 spans per line, 256 services, 16 KiB JSON
report and 64 KiB Markdown. Service-group overflow fails; byte packing includes
a lexical service prefix and counts omitted rows. It does not prioritize the
most severe regression. See [limitations](KNOWN_LIMITATIONS.md).

## 5. Optionally let your agent read this report

You already have useful JSON/Markdown; MCP is optional. Configure a client that
supports local stdio MCP with an **absolute** executable path and these arguments:

```text
command: /absolute/path/to/decant
args: ["evidence", "serve", "--file", "/absolute/path/report.json"]
```

Use your client's configuration format; the above names the process, not a
universal client config. Let the client launch it. Running it in a terminal alone
waits for MCP protocol traffic, not a natural-language prompt. Read the
[client/lifecycle guide](MCP_COMPATIBILITY.md) for compatibility boundaries.

The report is verified/read once at startup. Replacing the file does not change
the running server's evidence. Stdout carries only MCP; no tool chooses paths,
policies or payload fields. The server advertises MCP `2025-11-25` and two tools:

- `decant_evidence_snapshot`, with empty arguments, returns `report_json` and
  `report_id` under structured content.
- `decant_explain_evidence`, with `report_id` and `provenance_uri`, checks a
  selected reference in that exact report. Mismatches or absent references are
  unavailable, not access to raw data, a backend or another report.

For example, ask your agent: “Use Decant's evidence snapshot to compare checkout
before and after. State what the report measures, what is missing and which
references support it. Do not infer causality.” Treat telemetry labels as data,
never instructions. Actual desktop-agent acceptance is separate from protocol
tests; this guide does not certify every MCP client.

## If it does not work

`./target/release/decant evidence --help` lists supported commands. Fixed errors
intentionally omit paths and source values. Check the file format, numeric enums,
IDs, timestamps, final LF and documented size limits locally. On conflict, inspect
repeated trace/span IDs for different normalized facts; do not discard conflicting
data merely to get a report. Empty/non-server-only inputs yield gaps, not a
guessed incident. Share only synthetic reproductions in issues.
