# Test the finite live Kafka CLI with Apache Kafka

This opt-in workflow runs the actual `decant ingest kafka-json --config` command against one local Apache Kafka broker. It proves the narrow static-assignment, plaintext developer-preview path with synthetic Decant-native JSON values and separately with ordinary top-level JSON objects. It exercises version-0 compatibility output, version-1 payload-free Kafka activity context, strict version-2 configured JSON-object mapping, and strict version-4 entity-keyed material-change context. The product also accepts additive version 3 for a policy-gated grouped Boolean ratio; that path has broker-free recovery coverage but is not part of this Docker harness. This is an interoperability check around Decant, not a broker-specific product architecture or an ordinary test-suite dependency.

## Run the automated workflow

Prerequisites:

- Docker capable of running the official Linux image;
- Rust 1.98 and Cargo with this repository's locked dependencies already available offline;
- host port `127.0.0.1:9092` available; and
- standard POSIX shell tools listed by the script.

From the repository root:

```sh
./examples/kafka-live/run-apache-kafka-v0.sh
```

The script pins `apache/kafka:4.3.1`. Docker may download that external image on first use. The script builds Decant from the locked workspace, starts one uniquely named container, creates isolated single-partition topics for Decant-native JSON record values, ordinary JSON activity, and ordinary JSON material changes, asks Kafka's own tools for the cluster and topic IDs, writes separate temporary strict V0/V1/V2/V4 configs and checkpoint directories, and produces the checked-in synthetic fixtures. The V2/V4 producers also supply synthetic Kafka keys and headers so their omission is measured.

It then verifies these exact stages through the public CLI:

1. A deliberately different topic ID exits one with the fixed diagnostic, no stdout, and no checkpoint artifact.
2. The first valid run advances and accepts three records, publishes local checkpoint generation 1, reports no broker offset commit, and reports no context compilation.
3. An immediate restart recovers generation 1, advances zero records, and publishes nothing.
4. After one suffix record is produced, the next restart advances and accepts only that record and publishes generation 2.
5. A final restart recovers generation 2, advances zero records, and publishes nothing.
6. An independent V1 first run consumes only the first three records, publishes generation 1, and emits one three-event schema activity summary.
7. Its suffix restart consumes only the fourth record, publishes generation 2, and emits four-event context whose latest provenance is Kafka offset 3.
8. Its idle restart recovers generation 2, advances zero records, publishes nothing, and emits byte-identical nested context.
9. An isolated V2 first run maps and accepts three ordinary JSON objects, accounts for three omitted keys/headers/timestamps, publishes generation 1, and emits one mapping-derived schema activity summary.
10. Its suffix restart maps only the fourth object, publishes generation 2, and advances the latest provenance to Kafka offset 3.
11. Its idle restart consumes nothing, preserves generation 2, and emits byte-identical nested context.
12. Reusing that checkpoint history with a semantic mapping change fails before broker-client construction, emits no stdout, and leaves both published generations unchanged.
13. An isolated V4 first run maps two ordinary JSON objects to one payload-derived entity and emits the exact `10` to `20` transition with Kafka offsets 0 and 1 before publishing generation 1.
14. Its suffix restart consumes only the third object, emits the exact `20` to `30` transition with Kafka offsets 1 and 2, and publishes generation 2.
15. Its idle restart consumes nothing, preserves generation 2, and emits byte-identical nested material-change context with a matching admission-state receipt.

On success the last line is:

```text
PASS: v0 first=3/generation-1 idle=0 suffix=1/generation-2 restart=0; v1 first=3/generation-1 suffix=1/generation-2 idle=0/context-stable; v2 JSON objects first=3/generation-1 suffix=1/generation-2 idle=0/context-stable mapping-change=refused; v4 material changes first=2/generation-1 suffix=1/generation-2 idle=0/context-stable
```

The exit trap removes only the uniquely named container and temporary directory created by that invocation. It does not remove images, volumes, other containers, topics on another broker, or repository files. If the process is forcibly terminated without running its trap, remove only the container name printed by `docker ps --filter name=decant-kafka-live-v0-` after inspecting it.

## Use another local plaintext broker

The CLI does not discover topics or choose partitions. Before running it, create a topic and obtain the broker's cluster ID and that topic's 22-character URL-safe Kafka topic ID from broker administration tooling. The harness demonstrates the Apache commands used for both values. Do not guess or copy an ID from a different topic incarnation.

Copy the strict config shape from `write_config` (compatibility V0), `write_context_config` (context V1), `write_json_object_config` (mapped ordinary JSON V2), or `write_material_change_config` (entity-keyed material changes V4) in the harness and set:

- `bootstrap_servers` to the reachable plaintext bootstrap address;
- `topic`, `expected_cluster_id`, and `expected_topic_id` to the observed broker values;
- a unique stable `source_id` for V0/V1, or `source_namespace` for V2/V4;
- the exact static `partitions` and their first-run offsets;
- a dedicated existing `checkpoint_directory` with no unrelated files;
- explicit admission bounds, event-time epoch, poll timeout, consecutive-idle limit, and maximum data-record count.

For payload-free activity context, use top-level config version 1 and add this exact request shape:

```json
"context": {
  "format": "decant-kafka-activity-context-request",
  "version": 0,
  "byte_budget": 4096
}
```

The context byte budget must be between 512 and 16384 inclusive. V1 keeps all V0 ingestion and checkpoint bounds. It adds a deterministic schema-level summary containing accepted-event counts, event-time ranges, earliest/latest canonical provenance, explicit schema omissions when the budget is exhausted, and a V1 admission-state hash receipt. It does not emit payload fields or values, Kafka keys, or transport metadata. Config version 0 remains available and continues to report `context_compiled:false`.

For bounded ordinary JSON objects, use top-level config version 2. Replace V0/V1 `source_id` with a stable `source_namespace` and add one explicit mapping. This complete example uses millisecond event time plus required Boolean and UTF-8 scalar projections:

```json
{
  "format": "decant-kafka-live-config",
  "version": 2,
  "bootstrap_servers": "127.0.0.1:9092",
  "topic": "example-events",
  "source_namespace": "example-json-source",
  "expected_cluster_id": "BROKER_CLUSTER_ID",
  "expected_topic_id": "TOPIC_ID_22_CHARACTERS",
  "partitions": [{"partition": 0, "start_offset": 0}],
  "checkpoint_directory": "/absolute/path/to/empty-checkpoints",
  "epoch_start_ns": 0,
  "epoch_end_ns": 2000000000000000000,
  "accepted_identity_limit": 10000,
  "accepted_event_byte_limit": 67108864,
  "poll_timeout_ms": 1000,
  "idle_poll_limit": 2,
  "max_records": 10000,
  "context": {
    "format": "decant-kafka-activity-context-request",
    "version": 0,
    "byte_budget": 4096
  },
  "mapping": {
    "format": "decant-kafka-json-object-mapping",
    "version": 0,
    "event_time": {"field": "observed_time", "unit": "milliseconds"},
    "payload": [
      {"source": "ok", "target": "ok", "type": "bool"},
      {"source": "service", "target": "service", "type": "utf8"}
    ]
  }
}
```

Each Kafka value for that example must be a top-level object such as:

```json
{"observed_time":1720000000000,"ok":true,"service":"checkout","ignored_field":"not selected"}
```

V2 is a generic bounded top-level JSON mapping, not an OpenTelemetry-specific path and not arbitrary JSON support. The event-time field must be an integer with one of `nanoseconds`, `microseconds`, `milliseconds`, or `seconds`. Every configured projection is required and must exactly match `null`, `bool`, `i64`, `u64`, or `utf8`; there is no nesting, JSONPath, array/float support, inference, coercion, or defaulting. Unselected top-level members are accepted within the structural bounds but are not copied into canonical payload or output.

Projection presentation order is canonicalized, but any semantic mapping change derives a different opaque source/schema identity. Use a fresh checkpoint directory or a deliberately new source namespace when semantics change; Decant refuses to resume a checkpoint created under different mapping semantics. The namespace and projected values can still exist in local state/checkpoint bytes. The current activity context exposes only derived schema activity and provenance, not payload semantics or policy-safe values.

### Compile an allowlisted grouped ratio with V3

Config V3 keeps the V2 source, mapping, admission, polling, and checkpoint contract. It replaces the activity request with one grouped-ratio request and adds a complete field policy. A ready-to-edit example is [grouped-ratio-config-v0.example.json](grouped-ratio-config-v0.example.json). Replace both broker identity values with values observed from your broker. A checkpoint created by V2 can be reused when its source namespace and mapping are byte-for-byte semantically identical; otherwise use a fresh checkpoint directory because Decant rejects changed mapping semantics.

For input values shaped like:

```json
{"timestamp":1788542730724,"service":"paymentservice","is_error":true,"user_id":"not mapped"}
```

the example groups the last ten minutes relative to the latest accepted event by `service`, counts records where `is_error` equals `true`, and emits exact numerator, denominator, floor parts-per-million, remainder, and at most three numerator provenance IDs. Live results carry `query_closed:false` until the watermark or source finalization closes that interval.

Every mapped target must have one classification in `egress.fields`, with no missing or duplicate entries. The `group_field` must map as `utf8`, the predicate field must map as `bool`, and both must be `allow`. A mapped `deny` field remains in local admitted/checkpoint state but its name and value cannot be selected or serialized by V3. `deny` is omission, not pseudonymization or noninterference: denied values can still affect admission resource accounting or rejection outcomes. There is no automatic sensitive-field discovery or final free-text scanner.

### Compile an entity-keyed material change with V4

Config V4 keeps the bounded top-level JSON, static Kafka assignment, local checkpoint, and complete field-policy contracts. Mapping V1 additionally requires one explicit entity-key source: either `{"source":"kafka_record_key"}` or `{"source":"utf8_projection","field":"service"}`. Selecting a projected field requires an exactly mapped `utf8` target. Changing the entity-key rule changes the mapping-derived source and schema identities, so its checkpoint history cannot be resumed under a different rule.

The V4 harness uses `service` as the entity key, `series` as an allowlisted UTF-8 discriminator, and `value` as an allowlisted scalar. Its request is:

```json
"context": {
  "format": "decant-kafka-material-change-context-request",
  "version": 1,
  "view_id": "service-latency",
  "retention_ns": 600000000,
  "discriminator_field": "series",
  "value_field": "value",
  "item_limit": 16,
  "byte_budget": 4096,
  "entity_key_egress": "allow_lower_hex"
}
```

The compiler emits only the latest pair of distinct retained scalar values for each canonical entity-key/discriminator pair. Each previous and current observation carries exact event time and canonical Kafka provenance. Results say `provisional:true` while the source is not finalized. This is a deterministic materialized transition, not an anomaly, causality, source-completeness, or real-world change guarantee. `entity_key_egress` can instead be `deny`; allowing a payload-derived key also requires that mapped field to be allowed by the field policy.

Produce one JSON object per Kafka record value. The accepted V0/V1 value has only `record_format`, `record_version`, `schema_id`, `event_time_ns`, and `payload`; see the Decant-native fixture files for canonical examples. V2/V4 instead use their configured top-level mappings and the ordinary JSONL fixture files. Kafka record offsets form the canonical source position. Kafka keys may be absent or binary. Headers and broker timestamps are bounded and counted as omitted transport metadata rather than copied into the canonical payload.

Then run:

```sh
cargo build --locked --offline -p decant-cli --bin decant
./target/debug/decant ingest kafka-json --config /absolute/path/to/config.json
```

Use a fresh checkpoint directory if the configured source identity, mapping semantics, partition set, or desired config-version history changes. Once a checkpoint exists, recovered offsets override configured starting offsets. Inspect the one-line aggregate output rather than assuming progress: `advanced` counts newly applied positions, `checkpoint.published` identifies whether a new local generation was published, V1/V2 `context` describes schema activity, V3 `context.groups.items` contains payload-derived grouped ratios, and V4 `context.changes.items` contains bounded exact scalar transitions.

## Limits and claims

The automated workflow covers finite runs, one statically assigned partition, a plaintext local listener, Decant-native JSON values, bounded top-level ordinary JSON mapping, explicit local checkpoints, bounded idle termination, payload-free schema activity context, and one strict V4 payload-derived semantic context with field-level policy. V3 additionally has broker-free end-to-end and checkpoint-recovery tests for one explicitly allowlisted grouped Boolean ratio. The Docker workflow does not establish a supported Kafka-version matrix, consumer-group rebalancing, broker offset commits, exactly-once delivery, authenticated or encrypted transport, arbitrary JSON support, arbitrary upstream schema mapping, a general policy-safe egress claim, source completeness, long-running operation, durability beyond the documented local checkpoint behavior, performance, security, privacy, production readiness, or launch readiness. The V3/V4 paths remain limited to their explicitly described field policies and views; they do not add pseudonymization, automatic sensitive-field discovery, or a general egress guarantee.

The image tag is pinned but is not a content digest. Docker/image licensing, redistribution duties, vulnerability posture, and production suitability remain separate review gates. Ordinary `cargo test` only validates these checked-in public-safe assets; it does not contact Docker, pull an image, or start a broker.

Official upstream references:

- [Apache Kafka Docker usage](https://kafka.apache.org/43/getting-started/docker/)
- [Apache Kafka KRaft operations](https://kafka.apache.org/43/operations/kraft/)
- [Apache Kafka downloads](https://kafka.apache.org/community/downloads/)
