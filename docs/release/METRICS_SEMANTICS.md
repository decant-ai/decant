# Metrics comparison semantics

The internal backend library has a versioned, model-independent Prometheus
comparison recipe. It is not a new CLI investigation command or a replacement
for the observed-span evidence format. Grafana connect/doctor only configures
and checks query access.

The recipe requires an explicit request counter, an error label and exact
values, a classic duration histogram bucket metric, and an explicit seconds
unit declaration. Metric names do not prove instrumentation semantics. Scope
comes from the confirmed Grafana profile; the recipe cannot override it.

It freezes a Unix-second end cut and a 60–3600 second window. The baseline is the
immediately preceding equal-length window. PromQL ranges are left-open and
right-closed. Each window uses five aggregate queries:

- Sum of per-series request rates.
- Sum of per-series rates for the selected error values.
- p95 from rate-adjusted classic histogram buckets aggregated by bucket bound.
- Observed request-counter resets.
- Number of series contributing a request-rate estimate.

Rate is computed before aggregation so instance resets are not hidden by
summing counters. Per-instance percentiles are never averaged. Rates are
backend-extrapolated estimates and histogram p95 is a backend estimate, not
the exact nearest-rank statistic of Decant's observed-span recipe. Neither
measurement establishes complete production coverage or whole-service SLOs.
Native histograms, summaries and inferred units are not accepted substitutes.

The recipe preserves missing, partial, unavailable, unattempted, nonfinite and successful
zero results separately. Missing error series is unknown, not zero. Zero total
rate leaves the error fraction undefined. Error rate above total is inconsistent,
not clamped. A positive rate with zero contributing series is an explicit gap.
Warning/info counts remain visible; arbitrary backend text and labels are not
included. Derived comparisons require complete inputs; valid standalone
measurements can still be retained when another probe fails.

At most ten reads are made through the configured Grafana datasource proxy;
normal transport budgets still apply. Requests, evaluation cuts, datasource
identity, response hashes, recipe and scope identity accompany measurements.
Responses are not an atomic snapshot. A hash does not authenticate source truth.
Private in-memory captures can be reduced again without network or model calls;
the same captures reproduce the same canonical result bytes. Requerying a live
backend is not replay. Captures are not automatically anonymous.

Finite IEEE-754 arithmetic is used for backend floating-point estimates. All
nonfinite, negative, underflowed and out-of-range count values are checked. An
error-fraction calculation that underflows becomes an explicit gap, not zero.
Comparisons are not anomaly thresholds, causal diagnoses or declarations of health.

Acceptance uses fixed response oracles and synthetic HTTPS endpoints executing
the real client/parser/reducer. It does not execute a Prometheus engine or
certify Grafana/Mimir interoperability. The query rules follow the official
[rate](https://prometheus.io/docs/prometheus/latest/querying/functions/#rate) and
[histogram quantile](https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile)
semantics.
