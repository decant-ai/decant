# Decant

Turn traces into compact, reproducible evidence for your AI debugging agent.

**“Checkout got slower. What changed, and what evidence do we have?”**

Decant compares observed service behavior across two time windows and produces
a bounded report: span counts, error fractions, p95 duration, permitted version
observations, supporting references and explicit gaps. Read it locally or let
your existing agent read it through MCP. Compilation runs locally, without an
LLM, model account or Decant service.

**Source alpha (`0.1.0-alpha.1`).** Build from source; signed downloads are not
available. This is an experimental investigation tool, not a production support
promise or a demonstrated reduction in your agent bill.

## See what you get

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
| Grafana with Prometheus/Mimir and optional Tempo/Loki | [Connect and check scoped query access](docs/release/GRAFANA_SETUP.md); not an incident diagnosis or trace-report command |
| Traces only inside Datadog, Jaeger or another backend | No direct connector; a backend's arbitrary JSON export is not automatically compatible |

There is no live OTLP receiver, logs/metrics recipe, arbitrary-JSON inference or
agent-traffic proxy. You do not have to change an application's instrumentation
if you already have a compatible capture, but obtaining that capture is a real
setup step. Decant complements your backend and investigation agent; it replaces
neither. Grafana setup and query-access diagnostics are available as a separate,
read-only workflow; they do not turn backend responses into a trace capture.

## Build and run the demo

From this checkout's root, with Rust **1.98.0** and native build tools installed:

```sh
cargo fetch --locked
cargo build --release --locked --offline -p decant-cli --bin decant
./target/release/decant evidence demo
```

The source build requires a C/C++ compiler, make, Perl and the platform's
Kafka/zlib development prerequisites; see [build prerequisites](CONTRIBUTING.md#development-workflow).
The CLI currently builds its Kafka support even for file-only use. Fetching
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
