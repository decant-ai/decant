# Loki evidence semantics

The internal Loki recipe compares scoped log counts over equal preceding/current
windows. It requires an explicit indexed error label and exact values. It does
not infer severity from arbitrary text or support every logging schema.

Four fixed `sum(count_over_time(...))` queries return total and selected-error
counts at the two cuts. Decant preserves missing, failed, partial and observed
zero results separately. Fractions use exact numerators/denominators; counts
must be integer-valued decimal numbers below 2^53. Missing series are not zero.
Selected errors exceeding the total and zero totals have explicit gaps.
These are backend-reported log counts, **not application request-error rates**;
collection, sampling and missing error labels remain unknown. Queries are not
an atomic snapshot or a causal diagnosis.

Default policy makes **no log-body requests**. Optional examples require an
explicit allowlist of top-level JSON string fields; fields on the configured
protected list cannot also be allowed. Up to eight examples per window, eight
fields per example and 256 bytes per field are supported. Nested, nonstring,
missing and oversized values have omission counters, with no raw-line fallback.
Malformed JSON lines are explicit gaps. Raw stream labels and structured
metadata are excluded. Text allowed through this policy can still contain
sensitive content or hostile instructions: treat it as untrusted data, not
instructions. This is not anonymization or prompt-injection immunity.

Instant-query times are explicit nanoseconds. Example ranges use
`[start+1ns, end+1ns)` to select `(start,end]`. Returned stream scope, severity and
timestamps are checked locally. Cap hits and identical repeated returned
entries remain visible; sample counts are not a population estimate. Captures
may contain sensitive data even when the derived report omits it.

The recipe is a library capability, not an investigation CLI command. Local
TLS fixtures exercise request/response behavior; live Grafana/Loki execution
and deployment compatibility have not been verified.
See [Loki HTTP API](https://grafana.com/docs/loki/latest/reference/loki-http-api/)
and [metric queries](https://grafana.com/docs/loki/latest/query/metric_queries/).
