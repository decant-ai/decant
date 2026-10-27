# Optional model access

The backend library includes a bounded Anthropic Messages adapter. It requires
an explicitly selected model and key; there is no default model, credential
discovery, automatic retry or model routing. Production requests use verified
TLS to `https://api.anthropic.com/v1/messages`, with redirects and implicit
proxies disabled. No shell, remediation or general network tools are offered.

Only policy-projected evidence should be passed to the adapter, never private
replay captures. Calling a model sends that projection to an external provider
and can incur charges. Operator authorization and provider terms still apply.

One session allows at most six requests, 300 seconds elapsed, 30 seconds per
request, 96 KiB request bodies, 64 KiB response bodies and 384 KiB total received
body data. Each request asks for at most 4096 output tokens. Attempts are charged
before awaiting, including cancellation. Usage distinguishes input, output and
cache tokens; missing usage is recorded as unknown, not zero cost. These are
request/resource ceilings, not monetary guarantees.

Responses must contain one structured decision: inspect a fixed evidence probe,
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

Protocol references: [API overview](https://platform.claude.com/docs/en/api/overview)
and [tool definitions](https://platform.claude.com/docs/en/agents-and-tools/tool-use/define-tools).
