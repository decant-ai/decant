# Optional model access

The backend library includes a bounded Anthropic Messages adapter. It requires
an explicitly selected model and key; there is no default model, credential
discovery, automatic retry or model routing. Production requests use verified
TLS to `https://api.anthropic.com/v1/messages`, with redirects and implicit
proxies disabled. No shell, remediation or general network tools are offered.

Only policy-projected evidence should be passed to the adapter, never private
replay captures. Calling a model sends that projection to an external provider
and can incur charges. Operator authorization and provider terms still apply.

The existing `--reason` captured-evidence session allows at most six requests, 300 seconds elapsed, 30 seconds per
request, 96 KiB request bodies, 64 KiB response bodies and 384 KiB total received
body data. Each request asks for at most 4096 output tokens. Attempts are charged
before awaiting, including cancellation. Usage distinguishes input, output and
cache tokens; missing usage is recorded as unknown, not zero cost. These are
request/resource ceilings, not monetary guarantees.

In that existing mode, responses must contain one structured decision: inspect a fixed evidence probe,
or assess a closed set of regression hypotheses using supplied references.
Arbitrary measurements, commands and free-form provider text are not accepted.
References are validated, but membership does not prove that an interpretation
follows from the evidence. Model interpretations are not deterministic facts,
causal proof or a guarantee against prompt injection.

Malformed responses, refusals, truncation, authorization failures, redirects and
timeouts stop safely without changing deterministic evidence. Synthetic TLS
fixtures exercise the wire contract. Live provider/model compatibility has not
been established by those tests. Models that cannot produce the declared tool
response may fail closed.

## Two-stage question mode

`investigate ask` uses a separate single-use plan/explain adapter: at most two
attempts, including failures, with no retry or intermediate model access. The
first call receives the user question and confirmed service/environment names;
the final call receives only a bounded numeric/closed-code/opaque-reference
projection, not the question or raw telemetry. The explicit authorization prompt
discloses question and scope egress. Only these two validated profile values
are projected (128 UTF-8 bytes each); label names, datasource IDs, origins, paths
and credential references are excluded. The planner can request clarification for
conflicting names, but this is not guaranteed semantic detection. Local access
remains bound to the confirmed scope independently of the model response.

The final structured response may contain up to 2048 UTF-8 bytes of explanatory
prose, alongside validated references and typed hypotheses. Prose is an unverified
interpretation, never substituted for measured fields; citation checks do not
prove entailment or prevent every unsupported statement. Markdown renders it as
escaped text, not active links or markup. No arbitrary tools are executed.

Plan requests: 8 KiB and 512 output tokens maximum. Explanation requests: 24 KiB
and 2048 output tokens maximum, with at most 16 KiB projected evidence. Response
bodies: 64 KiB each, 128 KiB reserved within the shared acquisition allowance.
All token categories and unknown usage remain visible. These are bounds, not a
monetary guarantee or evidence of lower investigation cost. One structured tool
result is required; text-only, refused or malformed responses fail without repair.
See [question workflow](GRAFANA_INVESTIGATION.md#ask-a-question).

Protocol references: [API overview](https://platform.claude.com/docs/en/api/overview)
and [tool definitions](https://platform.claude.com/docs/en/agents-and-tools/tool-use/define-tools).
