# Decant command reference

All commands are experimental preview contracts. Use `decant evidence --help`
for the new evidence workflow and per-subcommand help for exact arguments.

## Grafana setup and query diagnostics

For the read-only investigation workflow:

- `decant investigate configure --profile production`: guided explicit metric and optional trace/log mappings.
- `decant investigate --profile production --since 15m`: acquire, save and compare evidence; Markdown by default.
- `decant investigate follow-up --bundle ID --focus errors`: inspect the same evidence offline.
- `decant investigate report --bundle ID --format json`: verify/recompute the original evidence report offline.
- `decant investigate refresh --bundle ID --profile production`: acquire a new later cut without replacing the old one.

Run `decant investigate --help` for exact options. Model use requires `--reason`,
an explicit model/key reference and egress authorization. See the
[own-backend walkthrough](GRAFANA_INVESTIGATION.md). Exit 0 means report produced,
not a healthy service or complete investigation. Existing formats remain unchanged.

- Run decant connect grafana --help for guided datasource and scope setup.
- Run decant doctor --profile production to check the saved profile.

These are explicit HTTPS read operations, not local file analysis. See
[Grafana setup](GRAFANA_SETUP.md) for credential references, mappings,
permissions, bounded probes and exit statuses. Doctor readiness is not a
diagnosis, service-health assessment or complete investigation coverage.

## Evidence (preferred own-data workflow)

- `decant evidence demo`: labelled synthetic Markdown report.
- `decant evidence init`: emit the V1 configuration template to stdout.
- `decant evidence validate --config PATH`: local config validation.
- `decant evidence analyze --file PATH --config PATH`: completed OTLP capture to JSON.
- `decant evidence render --file PATH`: verified JSON facts to Markdown.
- `decant evidence verify --file PATH`: canonical shape/checksum, not source truth.
- `decant evidence serve --file PATH`: one immutable report over local stdio MCP.
- `decant evidence validate-kafka --config PATH`: syntax/policy only, no source contact.
- `decant evidence serve-kafka --config PATH`: compatible mapped Kafka spans to verified-cut evidence/MCP.

The [capture](EVIDENCE_QUICKSTART.md), [Kafka](KAFKA_EVIDENCE.md) and
[MCP](MCP_COMPATIBILITY.md) guides describe the limits and policy choices.

## Preserved earlier workflows

```sh
decant demo sre
decant ingest otlp-jsonl --file examples/opentelemetry-demo/traces-v0.jsonl
decant ingest kafka-json --config kafka-config.json
decant serve kafka-mcp --config kafka-mcp-config.json
```

The first generates the fixed 100,000-delivery SRE workload. The second emits
the original bounded activity/count context, not the new before/after recipe.
Kafka finite configs V0–V4 support the previously documented envelope, activity,
explicitly allowlisted grouped Boolean ratio and exact scalar-transition paths.
The legacy resident service exposes `decant_context_snapshot`,
`decant_material_changes` and `decant_explain_provenance` for its one selected
view. It does not silently switch to the new evidence format.

Use the matching [Kafka finite](../../examples/kafka-live/README.md) or
[Kafka material-change MCP](../../examples/kafka-mcp/README.md) example. Do not
reuse an incompatible checkpoint namespace across mappings/recipes/configs.
Static assignment, verified local recovery, explicit input limits and no broker
commits apply. New commands preserve the old output/config version contracts.
