# Decant

Investigate service latency and error changes with reproducible evidence and explicit gaps.

**“Checkout got slower. What changed, and what evidence do we have?”**

Decant reads your existing Grafana metrics and optional Tempo/Loki data, compares
two time windows, and produces a bounded report: observed changes, measurements,
supporting evidence and missing checks. Read it locally; optionally authorize
bounded model reasoning. Evidence calculation and replay work without an LLM.
Completed OTel trace captures and compatible Kafka streams remain supported.

**Source alpha (`0.1.0-alpha.1`).** Build from source; signed downloads are not
available. This is an experimental investigation tool, not a production support
promise or a demonstrated reduction in your agent bill.

## See what you get

Already have Grafana? Build without Kafka prerequisites:

```sh
cargo fetch --locked
cargo build --release --locked --offline -p decant-cli --bin decant --no-default-features
```

Use Rust 1.98.0 and the platform TLS build prerequisites in the
[investigation-only build guide](docs/release/INVESTIGATION_BINARY.md). Then connect with an
environment-referenced token and follow the prompts—no telemetry export or JSON
hand-editing is needed:

```sh
./target/release/decant connect grafana --url https://grafana.example.com --token-env GRAFANA_TOKEN --profile production
./target/release/decant investigate configure --profile production
./target/release/decant investigate --profile production --since 15m
```

Use your authorized instance and explicit metric mappings. The
[own-backend walkthrough](docs/release/GRAFANA_INVESTIGATION.md) covers permissions,
reading the result, optional model access, offline follow-ups and refresh.
This workflow requires a request counter and classic latency histogram in seconds;
missing or incompatible data remains a gap, not a healthy verdict.

To ask a question, explicitly authorize model access:

```sh
./target/release/decant investigate ask --profile production --question "Why is checkout slower?" --model YOUR_MODEL_ID --model-key-env ANTHROPIC_API_KEY --allow-model-egress
```

The question applies only to the service/environment in the confirmed profile.
This path makes at most two model calls: select a latency/error recipe, then
explain the evidence. Between them, Decant compares metrics and conditionally
fetches configured Tempo/Loki evidence when the selected measurement increased
or is unknown. It saves evidence before the final call. Your question and confirmed
service/environment names are sent to Anthropic; never include secrets. Backend
log bodies, queries and raw traces are
not sent to the model. Explanations are labelled interpretations, not causal proof.
See [question workflow and limits](docs/release/GRAFANA_INVESTIGATION.md#ask-a-question).
Two calls is a resource ceiling, not demonstrated token or cost savings.

The bundled **synthetic** example produces these facts for `checkout`:

| Observed measurement | Before | After |
| --- | --- | --- |
| Server spans | 2 | 2 |
| Error spans / server spans | 1/2 | 1/2 |
| p95 duration | 20 ms | 200 ms |
| Service version, explicitly permitted in this demo | v1 | v2 |

Sampling and completeness remain unknown. These four spans illustrate the
report, not a real incident or a benchmark. The version change does **not** prove
what caused the duration difference. See the [complete generated report](examples/evidence/report-v1.md)
for its gaps, report identity and selected evidence references.

## Will it work with my data today?

| What you have | Supported path |
| --- | --- |
| A completed, uncompressed OTLP **trace JSONL** capture | Analyze locally → JSON/Markdown → optional report-backed MCP |
| Kafka records with the required span fields in explicitly mapped flat JSON | Publish the same evidence recipe at verified checkpoint cuts → local MCP |
| Grafana with Prometheus/Mimir and optional Tempo/Loki | [Guided read-only investigation](docs/release/GRAFANA_INVESTIGATION.md) → measurements/gaps → optional bounded reasoning and offline replay |
| Traces only inside Datadog, Jaeger or another backend | No direct connector; a backend's arbitrary JSON export is not automatically compatible |

There is no live OTLP receiver, arbitrary-JSON inference, remediation or
agent-traffic proxy. Decant does not replace your telemetry backend. Grafana
investigation uses explicit mappings and fixed queries; it does not fabricate
spans from aggregates. Traffic-mix and version-comparison checks remain unavailable
on that path. For file analysis, obtaining a compatible capture is still a real
setup step. No claim of broad incident coverage or a comparative agent win is made.

## Build and run the demo

From this checkout's root, with Rust **1.98.0** and native build tools installed:

```sh
cargo fetch --locked
cargo build --release --locked --offline -p decant-cli --bin decant
./target/release/decant evidence demo
```

The source build requires a C/C++ compiler, make, Perl and the platform's
Kafka/zlib development prerequisites; see [build prerequisites](CONTRIBUTING.md#development-workflow).
This default full build includes Kafka even for file-only use. The investigation-only
build above omits file, streaming and MCP commands and their build dependencies. Fetching
dependencies needs network access. Running the demo or analyzing a local file
does not need a Kafka cluster, model account, Docker or network connection.

## Make and verify your first report

This uses the checked-in fixture, so you do not need your own telemetry yet.
Run once from the repository root. `mkdir` deliberately refuses an existing
directory, helping you avoid overwriting an earlier report; do not continue if
it fails.

```sh
mkdir decant-first-report
./target/release/decant evidence init > decant-first-report/config.json
./target/release/decant evidence analyze --file examples/evidence/trace-capture-v1.jsonl --config decant-first-report/config.json > decant-first-report/report.json
./target/release/decant evidence render --file decant-first-report/report.json
./target/release/decant evidence verify --file decant-first-report/report.json
```

The measurements match the table above, but **version labels are withheld by
default**. Review the generated config: service names are permitted; service
versions require opt-in. Verification checks report structure, policy consistency
and checksum—not source truth or origin. Keep generated reports/configuration
local; do not commit them or attach production captures to public issues.

**Next: [analyze your own Collector trace capture](docs/release/EVIDENCE_QUICKSTART.md).**
That guide covers acquiring a compatible file, scoping a service/environment,
interpreting gaps, byte-identical replay and exposing one report to an MCP client.
The [OTel Demo guide](examples/opentelemetry-demo/README.md) is an optional way to
learn, not the required data source.

Already using Kafka? Follow the [live Kafka evidence guide](docs/release/KAFKA_EVIDENCE.md).
It requires compatible mapped records and static partition ownership; it does
not implement consumer-group rebalancing or broker-offset commits.

## What your agent can—and cannot—conclude

- Counts and duration statistics describe **observed SERVER spans**, not every
  production request. Sampling, unavailable baselines and omitted data stay visible.
- Version observations are not deployment timestamps or causal diagnoses.
- Reports carry selected references to their source cut, not a raw-trace archive
  or a link that queries your observability backend.
- One source per report/runtime; no cross-source joins. Retained-scope deduplication
  is not exactly-once delivery.
- Local captures/checkpoints can contain sensitive data. Field omission is not
  anonymization or prompt-injection immunity. Review reports before sharing.

Decant packages deterministic, policy-filtered evidence for a repeatable workflow.
A competent SQL/script or backend-native MCP workflow can produce similar
aggregations. Smaller output alone does not prove lower total agent cost or
better investigations; those require comparative evaluation.

Read [supported scope](docs/release/SUPPORTED_SCOPE.md),
[known limitations](docs/release/KNOWN_LIMITATIONS.md),
[MCP compatibility](docs/release/MCP_COMPATIBILITY.md) and the
[command reference](docs/release/COMMAND_REFERENCE.md).

## Feedback and development

Tell us which backend/agent you use, the investigation you wanted to run and
where the setup stopped helping. Use synthetic reproductions, never production
captures or credentials. Feedback is opt-in; Decant has no automatic usage
telemetry. See [contributing](CONTRIBUTING.md) for the current code-intake policy
and [security](SECURITY.md) for private vulnerability reporting.

For development, install Python 3.11+ in addition to the build prerequisites,
fetch the locked Cargo dependencies, then run:

```sh
./scripts/check-v0.sh
```

This checks deterministic correctness and repository hygiene; it is not a
performance or independent security certification. Real Collector/Kafka tests
are explicitly opt-in. The ordinary gate includes synthetic loopback HTTPS
tests and requires the OpenSSL command-line tool. See the
[evaluation protocol](docs/release/EVIDENCE_EVALUATION.md).
Maintainers: [local binary builds](docs/release/BINARY_CANDIDATES.md) and
[rollback](docs/release/ROLLBACK.md).

## License

Copyright (C) 2026 Eshan Vaid. The core, compiler and CLI use
[AGPL-3.0-only](LICENSE). Only the five standalone configuration examples listed
in [LICENSING.md](LICENSING.md) use Apache-2.0; this is not an alternative license
for the core. Third-party components retain their own terms.
