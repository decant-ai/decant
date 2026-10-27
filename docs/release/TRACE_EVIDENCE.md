# Tempo evidence semantics

The internal Tempo recipe reads through a confirmed Grafana datasource. It
uses two explicitly scoped searches and fetches each distinct selected trace
once through `/api/v2/traces/:id`, with span pruning disabled. The
[investigation CLI](GRAFANA_INVESTIGATION.md) uses this recipe when Tempo is
explicitly selected during configuration, with names omitted by default.

The comparison uses equal preceding/current windows, 60–3600 seconds each,
with an explicit Unix-second cut. Span starts use `(start, end]` membership.
Each search selects at most four traces. These are **first-N search samples**,
not population statistics. A repeated live search can return different traces.
Only reduction of the same captured responses is reproducible.

Operation cohorts summarize observed server spans for the configured service
and environment. Dependency cohorts require an observed client parent and
server child in the same environment. Counts, error/unknown-status counts and
nearest-rank duration p95 describe only those observed spans. A missing parent
does not create an inferred dependency. No diagnosis or causality is asserted.

Identical repeated spans fold; conflicting copies fail. Missing parents,
partial responses, failures, cap hits, excluded spans and dropped-item counters
remain visible. Backend-complete does not establish full-trace completeness or
known sampling. Responses are not an atomic snapshot.

Default output omits operation/dependency names, raw IDs, arbitrary attributes,
events and messages. Names require explicit allowlists. Opaque hashes are not
anonymization; queries contain the operator-selected scope. Raw captures remain
potentially sensitive private inputs.

Supported trace shapes are the V2 envelope containing `trace.resourceSpans` or
legacy `trace.batches`, with `scopeSpans`; hexadecimal or strict padded base64
IDs and numeric or named protobuf enums. Bounds: 4 MiB per response, 256 input
spans per trace, 2048 unique spans, 32 groups, 64 KiB output. The strict JSON
parser additionally caps depth, nodes, attributes and arrays. Larger or malformed
traces become explicit unavailable observations, not silently truncated success.

Local TLS protocol fixtures exercise acquisition and reduction. A real Grafana/
Tempo deployment interoperability trial has not been performed. API behavior:
[Tempo HTTP API](https://grafana.com/docs/tempo/latest/api_docs/).
