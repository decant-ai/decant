# Evidence trace fixture and Collector interoperability

## Understand the example

`trace-capture-v1.jsonl` is deliberately synthetic, hand-written OTLP-shaped
input, not a production incident. Its independent expected measurements are:
checkout baseline 2 server spans / 1 error / 20,000,000 ns p95; current 2 server
spans / 1 error / 200,000,000 ns p95. The observed version changes from v1 to v2;
this is not a claim that a deployment caused the duration difference.

The durations above are 20 ms and 200 ms. Two spans per interval are enough to
exercise the recipe, not to characterize a production workload. The unchanged
1/2 error fraction also illustrates why a report must distinguish error and
duration measurements instead of declaring every metric a regression.

`decant evidence demo` explicitly permits these synthetic version labels and
prints [the frozen report](report-v1.md). The
[default-policy file walkthrough](../../README.md#make-and-verify-your-first-report)
uses the same capture and implementation but withholds versions, so its report
identity and bytes differ from the demo. Do not enable versions just to make
the hashes match. Repeated analysis with the same input and config must instead
match its own bytes exactly.

Sampling and history coverage remain unknown. References locate selected
observations in this report; they do not query a backend or retrieve a raw trace.
For actual data, follow the [capture guide](../../docs/release/EVIDENCE_QUICKSTART.md).

## Optional real Collector interoperability test

The opt-in test sends only this fixture to a separate official Collector and
tests its actual file-exporter output through the production ingestion/recipe.
It does not download or run the OpenTelemetry Demo or become a runtime feature.

```sh
docker pull otel/opentelemetry-collector-contrib:0.160.0
python3 tools/test_collector_evidence_v1.py --run
```

The harness requires content digest
`sha256:799dc6cf12c96192af37b5bdba804da8c10b3bc563b43cb90c3f3c58d9572ad6`,
never pulls an image itself, uses a random loopback-only port, mounts only its
temporary config/output, caps the container at one CPU/256 MiB, and removes only
the exact container it created. A local Docker runtime and the image are
external test prerequisites, not Decant installation requirements.

Captures are consumed in file order. Late spans can be rejected by the existing
30-second admission rule; reports disclose that rather than sort the input.
Missing kind is unknown/unspecified and is excluded from server-span metrics.
Missing service versions and unknown sampling are not inferred. Default report
policy emits service names but not version labels; inspect labels before sharing.
