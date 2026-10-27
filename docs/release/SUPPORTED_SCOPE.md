# Supported Scope for 0.1.0-alpha.1

## Supported developer-preview paths

- `decant evidence analyze --file <PATH> --config <PATH>`: a completed OTLP trace capture to versioned observed-span before/after evidence; render/verify locally or serve one immutable report over stdio MCP.
- `decant evidence serve-kafka --config <PATH>`: the same recipe over compatible explicitly mapped Kafka spans, published only at verified checkpoint cuts with separate pending/health observations.
- Evidence includes observed SERVER counts/error fractions, exact nearest-rank p95, permitted version observations, selected references and explicit sampling/admission/retention/output gaps. Version observation is not deployment causality or a whole-production SLO.
- `decant demo sre`: deterministic generated workload to bounded context.
- `decant ingest otlp-jsonl --file <PATH>`: one finite OTLP JSON trace capture.
- `decant ingest kafka-json --config <PATH>`: one finite, statically assigned Kafka topic/partition run using config versions 0 through 4.
- `decant serve kafka-mcp --config <PATH>`: one continuing statically assigned Kafka source to deterministic material-change compiler to local stdio MCP service.
- Mapping V1 maps required top-level JSON scalar fields, explicit integer event time, and either the Kafka key or one mapped UTF-8 field as entity identity.
- Finite config V3 adds one policy-gated grouped Boolean ratio. Finite config V4 and the resident service add one policy-gated exact scalar-transition view.
- Kafka progress is recorded in a dedicated bounded local checkpoint directory and recovered from the strict latest generation.
- Context compilation is local, deterministic, byte-budgeted, provenance-carrying, and LLM-independent.
- The resident service accepts explicitly trusted plaintext, verified TLS, or TLS plus SCRAM-SHA-256/512 using environment-variable secret references.
- The legacy MCP server exposes exactly `decant_context_snapshot`, `decant_material_changes`, and `decant_explain_provenance`. Report-backed evidence exposes snapshot/explanation; live evidence additionally exposes status. All have one in-flight request and bounded JSON-line framing. See [MCP compatibility](MCP_COMPATIBILITY.md).

## Compatibility status

- CLI release identity: `0.1.0-alpha.1`.
- Event, context, config, checkpoint, and provenance formats carry their own V0/V1 version fields.
- Those formats are preview contracts, not a general compatibility promise.
- Host-native unsigned packaging supports Darwin arm64 and Linux x86_64 as engineering targets only; each needs actual clean-target/signing/OS evidence before distribution. There is no signed public download yet.
- Mapping/source/config changes can require a fresh checkpoint namespace; there is no migration command.

## Operating boundary

- Single process and single node.
- One process, one statically configured source, one configured view, and one checkpoint writer.
- Finite invocations or one supervised resident service; the service does not daemonize or restart itself.
- Local stdout output for finite commands; MCP protocol-only stdout and value-safe lifecycle stderr for the resident service; local checkpoint files.
- No hosted Decant service. The optional model library makes explicitly authorized external calls; deterministic commands do not require a model.
- Backend network access is opt-in through Kafka or Grafana commands; build/test downloads are separate explicit operations.
- No automatic usage telemetry. One source per report/runtime; no cross-source join or consistent multi-source snapshot.

This document defines the supported preview and bounded production-pilot candidate, not the ultimate Decant feature set or a general production support promise.
## Grafana setup and query diagnostics

The additive connect/doctor commands support one explicit HTTPS Grafana origin,
a confirmed Prometheus-compatible datasource and optional Tempo/Loki UIDs.
Readiness comes from bounded scoped queries, with empty, partial and failed
results preserved. No raw log bodies or full traces are emitted by doctor.
This is query-access diagnostics, not a backend incident-investigation report.
See [setup and compatibility limits](GRAFANA_SETUP.md).

The backend library also contains bounded sampled Tempo operation/dependency
evidence with explicit gaps. It is not exposed as an investigation CLI command.
See [trace evidence semantics and limits](TRACE_EVIDENCE.md).
The Loki library recipe compares scoped log counts, with body acquisition off
unless bounded field projection is explicitly configured. See
[log evidence semantics and limits](LOG_EVIDENCE.md).
The application library can compose these captures into versioned JSON/Markdown
investigation documents and private offline replay bundles. It does not add a
standalone investigation command or model reasoning. See
[investigation and replay limits](INVESTIGATION_REPLAY.md).

The optional model adapter is a separate library boundary, not an automatic
agent or new CLI command. See [model access and limits](MODEL_ACCESS.md).
