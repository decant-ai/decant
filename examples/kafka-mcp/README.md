# Kafka to local MCP service

`decant serve kafka-mcp` continuously consumes one statically assigned Kafka topic, restores and periodically publishes verified local checkpoints, compiles one bounded material-change view, and serves that view through MCP over standard input/output.

Copy `config-v0.json`, then replace the broker addresses, cluster ID, topic ID, partitions, absolute checkpoint directory, epoch, mapping, egress policy, and view request. The example selects TLS plus SCRAM and contains only environment-variable references:

```sh
export DECANT_KAFKA_USERNAME='replace-at-runtime'
export DECANT_KAFKA_PASSWORD='replace-at-runtime'
cargo run --locked --offline -p decant-cli --bin decant -- \
  serve kafka-mcp --config /absolute/path/to/config-v0.json
```

Use `{"mode":"plaintext","version":0}` only for an explicitly trusted plaintext broker. TLS can use system roots with `"ca_location": null` or one absolute CA-file location. Raw credentials are never valid service-config fields.

Stdout is MCP JSON-line protocol traffic only. Value-safe lifecycle diagnostics are emitted as JSON lines on stderr. The process uses static partition ownership, never commits broker offsets, and treats its verified local checkpoint as the recovery authority. One checkpoint directory supports one writer.

The service exposes exactly `decant_context_snapshot`, `decant_material_changes`, and `decant_explain_provenance`. Material changes are exact bounded observations, not anomaly, causality, completeness, or exactly-once claims. The MCP boundary is local stdio only; remote access, tenant authentication, automatic partition rebalancing, and automatic epoch rollover are not provided.

## Opt-in pilot fault matrix

The real-broker fault matrix is intentionally excluded from ordinary offline tests. It uses a pinned Apache Kafka Docker image, synthetic records, a temporary checkpoint directory, and host port `127.0.0.1:9092`. It verifies hard-kill recovery from the last verified cut, progress while an MCP client delays reading one response, broker-loss fail-closed behavior followed by supervisor-style restart, strict corrupt-latest refusal, three-generation pruning, output leakage sentinels, and a 512 MiB sampled-RSS engineering ceiling over 2,000 records.

```sh
DECANT_RUN_KAFKA_MCP_FAULT_MATRIX_V0=1 \
  ./examples/kafka-mcp/run-pilot-fault-matrix-v0.py
```

The script can pull `apache/kafka:4.3.1` and deliberately sends `SIGKILL` to its uniquely owned Decant child. Its label-checked cleanup removes only its temporary container and directory. It does not establish a Kafka compatibility matrix, graceful signal handling, a performance result, durability beyond the verified local checkpoint, or production readiness. The printed RSS observation is explicitly not a benchmark and is not committed as a product claim. Denied values remain in local state/checkpoints by design; the leakage scan covers MCP stdout and operational stderr, not encrypted-at-rest or noninterference properties.

## Opt-in production-pilot benchmark

The separate benchmark runner executes three fixed cold 10,000-record Kafka-to-checkpoint-to-MCP runs and writes a bounded canonical result into the ignored `benchmarks/results/` directory:

```sh
DECANT_RUN_KAFKA_MCP_BENCHMARK_V0=1 \
  ./examples/kafka-mcp/run-pilot-benchmark-v0.py \
  --output benchmarks/results/kafka-mcp-pilot-v0.json
```

Run it only from a clean committed candidate on the intended pilot-class host. The result includes raw timing/resource observations, exact correctness checks, and serialized-size comparisons with four named baselines; it contains no source records or compiled context bytes. It is local evidence, not a published benchmark, performance promise, SLA, compatibility result, or production certification. Follow `docs/release/PRODUCTION_PILOT_CHECKLIST.md` before using the stricter commit-bound pilot gate.
