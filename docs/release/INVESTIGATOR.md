# Captured-evidence investigation

This page describes the preserved captured-view `--reason` mode. The additive
[`investigate ask` workflow](GRAFANA_INVESTIGATION.md#ask-a-question) instead uses
one planning call, deterministic conditional backend acquisition, and one final
explanation call. It does not run this six-call inspection loop.

The application library inspects one verified investigation report. It always
computes mandatory checks for metrics, sampled operations, sampled dependencies,
logs, traffic mix and versions. The current aggregate recipes do not acquire
operation traffic weights or version comparisons: those checks remain unavailable.
Request volume is not traffic mix, and missing evidence is not a healthy result.

Deterministic findings report observed increases, decreases, unchanged values or
unknown comparisons. Metrics retain backend estimate qualifications. Trace p95
comparisons use integer nanoseconds over captured samples, never averages of
percentiles. Log counts are not request-error rates. These comparisons do not
establish statistical significance, population completeness or causality.

Optional reasoning can inspect only fixed views of the same capture. It cannot
generate queries, change selectors, refresh data, run shell commands or perform
remediation. Repeated inspections stop the loop; all failures retain deterministic
findings. The controller and model transport independently cap calls at six and
elapsed allowance at five minutes. The caller shares remaining time and received
body-byte allowance with acquisition. Output is bounded to 64 KiB.

Model egress is stricter than local report policy: numeric facts, closed statuses
and opaque evidence identities only. Service/environment values, source origins,
queries, metric names, trace names and all log text are excluded. Local report
permission to display a log field is not permission to send its text to a model.
Hashes are not an anonymization or source-authentication guarantee.

`decant-investigation-assessment` V1 is an additive sidecar, not a replacement for
`decant-investigation` V1 or its replay bundle. It contains deterministic findings,
mandatory checks, missing checks, separately labelled model interpretations,
usage and the stopping reason. It binds references to the original report.
References must have been exposed to the model in this run; scalar assessments
cannot reverse the computed direction or cite an unavailable check as support.
Valid citations do not prove general semantic entailment. Contradictory sampled
cohorts may require an insufficient assessment instead of a broad conclusion.

Replaying a bundle reproduces the evidence, not a new provider's model output.
No live backend or model interoperability, diagnostic superiority or cost saving
is established by the synthetic tests.
