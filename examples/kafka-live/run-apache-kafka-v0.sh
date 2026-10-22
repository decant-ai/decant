#!/bin/sh
set -eu

IMAGE="apache/kafka:4.3.1"
RUN_ID="$$-$(date +%s)"
CONTAINER_NAME="decant-kafka-live-v0-${RUN_ID}"
TOPIC="decant-live-v0-${RUN_ID}"
SOURCE_ID="decant-source-v0-${RUN_ID}"
JSON_TOPIC="decant-json-object-v0-${RUN_ID}"
JSON_SOURCE_NAMESPACE="private-source-namespace"
JSON_SCHEMA_ID="2efb7cb9f6816c71fb5f3371ef8903d70a41afe80f80929fc37a64d59896dac6"
JSON_EFFECTIVE_SOURCE_ID="8a272b4ed412e12d1d7e4cd4810bf8dc59d08e82962db7fa8c897cbbb1211991"
MATERIAL_TOPIC="decant-material-change-v0-${RUN_ID}"
MATERIAL_SOURCE_NAMESPACE="private-material-source-namespace"
WORK_DIRECTORY="$(mktemp -d "${TMPDIR:-/tmp}/decant-kafka-live-v0.XXXXXX")"
CHECKPOINT_DIRECTORY="${WORK_DIRECTORY}/checkpoints"
CONTEXT_CHECKPOINT_DIRECTORY="${WORK_DIRECTORY}/context-checkpoints"
JSON_CHECKPOINT_DIRECTORY="${WORK_DIRECTORY}/json-object-checkpoints"
MATERIAL_CHECKPOINT_DIRECTORY="${WORK_DIRECTORY}/material-change-checkpoints"
CONFIG_PATH="${WORK_DIRECTORY}/config.json"
WRONG_CONFIG_PATH="${WORK_DIRECTORY}/wrong-config.json"
CONTEXT_CONFIG_PATH="${WORK_DIRECTORY}/context-config.json"
JSON_CONFIG_PATH="${WORK_DIRECTORY}/json-object-config.json"
JSON_CHANGED_CONFIG_PATH="${WORK_DIRECTORY}/json-object-changed-config.json"
MATERIAL_CONFIG_PATH="${WORK_DIRECTORY}/material-change-config.json"
CONTAINER_ATTEMPTED=0

case "$WORK_DIRECTORY" in
  */decant-kafka-live-v0.*) ;;
  *)
    printf '%s\n' 'refusing unexpected temporary directory' >&2
    exit 1
    ;;
esac

cleanup() {
  if [ "$CONTAINER_ATTEMPTED" -eq 1 ]; then
    owned_label="$(
      docker inspect --format '{{ index .Config.Labels "ai.decant.kafka-harness" }}' \
        "$CONTAINER_NAME" 2>/dev/null || true
    )"
    if [ "$owned_label" = "$RUN_ID" ]; then
      docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
    fi
  fi
  case "$WORK_DIRECTORY" in
    */decant-kafka-live-v0.*) rm -rf -- "$WORK_DIRECTORY" ;;
  esac
}
trap cleanup EXIT HUP INT TERM

fail() {
  printf 'verification failed: %s\n' "$1" >&2
  exit 1
}

require_fragment() {
  grep -F "$2" "$1" >/dev/null || fail "$3"
}

require_empty() {
  [ "$(wc -c <"$1" | tr -d ' ')" -eq 0 ] || fail "$2"
}

write_config() {
  destination=$1
  configured_topic_id=$2
  {
    printf '%s\n' '{'
    printf '  "format": "decant-kafka-live-config",\n'
    printf '  "version": 0,\n'
    printf '  "bootstrap_servers": "127.0.0.1:9092",\n'
    printf '  "topic": "%s",\n' "$TOPIC"
    printf '  "source_id": "%s",\n' "$SOURCE_ID"
    printf '  "expected_cluster_id": "%s",\n' "$CLUSTER_ID"
    printf '  "expected_topic_id": "%s",\n' "$configured_topic_id"
    printf '  "partitions": [{"partition": 0, "start_offset": 0}],\n'
    printf '  "checkpoint_directory": "%s",\n' "$CHECKPOINT_DIRECTORY"
    printf '  "epoch_start_ns": 0,\n'
    printf '  "epoch_end_ns": 1000,\n'
    printf '  "accepted_identity_limit": 100,\n'
    printf '  "accepted_event_byte_limit": 1000000,\n'
    printf '  "poll_timeout_ms": 5000,\n'
    printf '  "idle_poll_limit": 2,\n'
    printf '  "max_records": 3\n'
    printf '%s\n' '}'
  } >"$destination"
}

write_context_config() {
  destination=$1
  configured_topic_id=$2
  {
    printf '%s\n' '{'
    printf '  "format": "decant-kafka-live-config",\n'
    printf '  "version": 1,\n'
    printf '  "bootstrap_servers": "127.0.0.1:9092",\n'
    printf '  "topic": "%s",\n' "$TOPIC"
    printf '  "source_id": "%s",\n' "$SOURCE_ID"
    printf '  "expected_cluster_id": "%s",\n' "$CLUSTER_ID"
    printf '  "expected_topic_id": "%s",\n' "$configured_topic_id"
    printf '  "partitions": [{"partition": 0, "start_offset": 0}],\n'
    printf '  "checkpoint_directory": "%s",\n' "$CONTEXT_CHECKPOINT_DIRECTORY"
    printf '  "epoch_start_ns": 0,\n'
    printf '  "epoch_end_ns": 1000,\n'
    printf '  "accepted_identity_limit": 100,\n'
    printf '  "accepted_event_byte_limit": 1000000,\n'
    printf '  "poll_timeout_ms": 5000,\n'
    printf '  "idle_poll_limit": 2,\n'
    printf '  "max_records": 3,\n'
    printf '  "context": {"format": "decant-kafka-activity-context-request", "version": 0, "byte_budget": 4096}\n'
    printf '%s\n' '}'
  } >"$destination"
}

write_json_object_config() {
  destination=$1
  configured_topic_id=$2
  event_time_unit=$3
  {
    printf '%s\n' '{'
    printf '  "format": "decant-kafka-live-config",\n'
    printf '  "version": 2,\n'
    printf '  "bootstrap_servers": "127.0.0.1:9092",\n'
    printf '  "topic": "%s",\n' "$JSON_TOPIC"
    printf '  "source_namespace": "%s",\n' "$JSON_SOURCE_NAMESPACE"
    printf '  "expected_cluster_id": "%s",\n' "$CLUSTER_ID"
    printf '  "expected_topic_id": "%s",\n' "$configured_topic_id"
    printf '  "partitions": [{"partition": 0, "start_offset": 0}],\n'
    printf '  "checkpoint_directory": "%s",\n' "$JSON_CHECKPOINT_DIRECTORY"
    printf '  "epoch_start_ns": 0,\n'
    printf '  "epoch_end_ns": 1000000000,\n'
    printf '  "accepted_identity_limit": 100,\n'
    printf '  "accepted_event_byte_limit": 1000000,\n'
    printf '  "poll_timeout_ms": 5000,\n'
    printf '  "idle_poll_limit": 2,\n'
    printf '  "max_records": 3,\n'
    printf '  "context": {"format": "decant-kafka-activity-context-request", "version": 0, "byte_budget": 4096},\n'
    printf '  "mapping": {\n'
    printf '    "format": "decant-kafka-json-object-mapping",\n'
    printf '    "version": 0,\n'
    printf '    "event_time": {"field": "observed_time", "unit": "%s"},\n' "$event_time_unit"
    printf '    "payload": [\n'
    printf '      {"source": "raw_ok", "target": "ok", "type": "bool"},\n'
    printf '      {"source": "raw_service", "target": "service", "type": "utf8"}\n'
    printf '    ]\n'
    printf '  }\n'
    printf '%s\n' '}'
  } >"$destination"
}

write_material_change_config() {
  destination=$1
  configured_topic_id=$2
  {
    printf '%s\n' '{'
    printf '  "format": "decant-kafka-live-config",\n'
    printf '  "version": 4,\n'
    printf '  "bootstrap_servers": "127.0.0.1:9092",\n'
    printf '  "topic": "%s",\n' "$MATERIAL_TOPIC"
    printf '  "source_namespace": "%s",\n' "$MATERIAL_SOURCE_NAMESPACE"
    printf '  "expected_cluster_id": "%s",\n' "$CLUSTER_ID"
    printf '  "expected_topic_id": "%s",\n' "$configured_topic_id"
    printf '  "partitions": [{"partition": 0, "start_offset": 0}],\n'
    printf '  "checkpoint_directory": "%s",\n' "$MATERIAL_CHECKPOINT_DIRECTORY"
    printf '  "epoch_start_ns": 0,\n'
    printf '  "epoch_end_ns": 1000000000,\n'
    printf '  "accepted_identity_limit": 100,\n'
    printf '  "accepted_event_byte_limit": 1000000,\n'
    printf '  "poll_timeout_ms": 5000,\n'
    printf '  "idle_poll_limit": 2,\n'
    printf '  "max_records": 2,\n'
    printf '  "context": {"format": "decant-kafka-material-change-context-request", "version": 1, "view_id": "service-latency", "retention_ns": 600000000, "discriminator_field": "series", "value_field": "value", "item_limit": 16, "byte_budget": 4096, "entity_key_egress": "allow_lower_hex"},\n'
    printf '  "egress": {"format": "decant-kafka-field-egress-policy", "version": 0, "fields": [{"field": "secret", "action": "deny"}, {"field": "series", "action": "allow"}, {"field": "service", "action": "allow"}, {"field": "value", "action": "allow"}]},\n'
    printf '  "mapping": {\n'
    printf '    "format": "decant-kafka-json-object-mapping",\n'
    printf '    "version": 1,\n'
    printf '    "entity_key": {"source": "utf8_projection", "field": "service"},\n'
    printf '    "event_time": {"field": "observed_time", "unit": "milliseconds"},\n'
    printf '    "payload": [{"source": "raw_secret", "target": "secret", "type": "utf8"}, {"source": "raw_series", "target": "series", "type": "utf8"}, {"source": "raw_service", "target": "service", "type": "utf8"}, {"source": "raw_value", "target": "value", "type": "u64"}]\n'
    printf '  }\n'
    printf '%s\n' '}'
  } >"$destination"
}

produce_json_objects() {
  source_path=$1
  first_key_ordinal=$2
  producer_path=$3
  awk -v first="$first_key_ordinal" '{
    printf "private-header:private-header-value\tprivate-key-%d\t%s\n", first + NR - 1, $0
  }' "$source_path" >"$producer_path"
  docker exec -i "$CONTAINER_NAME" /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$JSON_TOPIC" \
    --property parse.headers=true --property parse.key=true <"$producer_path" >/dev/null
}

produce_material_objects() {
  source_path=$1
  first_key_ordinal=$2
  producer_path=$3
  awk -v first="$first_key_ordinal" '{
    printf "private-header:private-header-value\tprivate-key-%d\t%s\n", first + NR - 1, $0
  }' "$source_path" >"$producer_path"
  docker exec -i "$CONTAINER_NAME" /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$MATERIAL_TOPIC" \
    --property parse.headers=true --property parse.key=true <"$producer_path" >/dev/null
}

assert_run() {
  output_path=$1
  expected_data=$2
  expected_advanced=$3
  expected_accepted=$4
  expected_published=$5
  expected_generation=$6
  require_fragment "$output_path" '"format":"decant-kafka-live-run","version":0' 'run format/version'
  require_fragment "$output_path" "\"data_records\":${expected_data}" 'data-record count'
  require_fragment "$output_path" "\"advanced\":${expected_advanced}" 'advanced count'
  require_fragment "$output_path" "\"accepted\":${expected_accepted}" 'accepted count'
  require_fragment "$output_path" "\"published\":${expected_published},\"generation\":${expected_generation}" 'checkpoint result'
  require_fragment "$output_path" '"broker_offsets_committed":false' 'broker-commit disclosure'
  require_fragment "$output_path" '"context_compiled":false' 'context disclosure'
  if grep -F 'synthetic.kafka.metric.v0' "$output_path" >/dev/null; then
    fail 'raw schema value reached CLI output'
  fi
}

assert_context_run() {
  output_path=$1
  expected_data=$2
  expected_advanced=$3
  expected_accepted=$4
  expected_published=$5
  expected_generation=$6
  expected_context_events=$7
  expected_latest_offset=$8
  require_fragment "$output_path" '"format":"decant-kafka-live-run","version":1' 'context run format/version'
  require_fragment "$output_path" "\"data_records\":${expected_data}" 'context data-record count'
  require_fragment "$output_path" "\"advanced\":${expected_advanced}" 'context advanced count'
  require_fragment "$output_path" "\"accepted\":${expected_accepted}" 'context accepted count'
  require_fragment "$output_path" "\"published\":${expected_published},\"generation\":${expected_generation}" 'context checkpoint result'
  require_fragment "$output_path" '"broker_offsets_committed":false' 'context broker-commit disclosure'
  require_fragment "$output_path" '"context_compiled":true' 'context compilation disclosure'
  require_fragment "$output_path" '"format":"decant-kafka-activity-context","version":0' 'nested context format/version'
  require_fragment "$output_path" "\"accepted_events\":${expected_context_events},\"distinct_schemas\":1" 'context accepted-event count'
  require_fragment "$output_path" '"candidate_count":1,"included_count":1,"omitted_count":0' 'context schema accounting'
  require_fragment "$output_path" '"schema_id":"synthetic.kafka.metric.v0"' 'context schema ID'
  require_fragment "$output_path" '"earliest":"decant://event/v0/' 'context earliest provenance'
  require_fragment "$output_path" '/kafka/0/0"' 'context earliest offset'
  require_fragment "$output_path" "/kafka/0/${expected_latest_offset}\"" 'context latest offset'
  require_fragment "$output_path" '"payload_fields_emitted":false,"keys_emitted":false,"transport_metadata_emitted":false' 'context omission disclosure'
  grep -E '"admission_state_hash":"sha256:v1:[0-9a-f]{64}"' "$output_path" >/dev/null || \
    fail 'context admission-state receipt'
  for private_value in '"sample"' "$TOPIC" "$CLUSTER_ID" "$TOPIC_ID"; do
    if grep -F "$private_value" "$output_path" >/dev/null; then
      fail 'payload or broker identity reached context output'
    fi
  done
}

assert_json_object_run() {
  output_path=$1
  expected_data=$2
  expected_published=$3
  expected_generation=$4
  expected_context_events=$5
  expected_latest_offset=$6
  expected_header_count=$7
  expected_header_bytes=$8
  require_fragment "$output_path" '"format":"decant-kafka-live-run","version":1' 'JSON-object run format/version'
  require_fragment "$output_path" "\"data_records\":${expected_data}" 'JSON-object data-record count'
  require_fragment "$output_path" "\"processed\":${expected_data},\"advanced\":${expected_data},\"accepted\":${expected_data}" 'JSON-object admission counts'
  require_fragment "$output_path" '"duplicate_noops":0,"rejected":0,"before_frontier":0' 'JSON-object terminal admission counts'
  require_fragment "$output_path" "\"published\":${expected_published},\"generation\":${expected_generation}" 'JSON-object checkpoint result'
  require_fragment "$output_path" "\"header_count\":${expected_header_count},\"header_bytes\":${expected_header_bytes}" 'JSON-object header omissions'
  require_fragment "$output_path" '"timestamp_not_available_records":0' 'JSON-object unavailable timestamp count'
  require_fragment "$output_path" "\"timestamp_create_time_records\":${expected_data}" 'JSON-object create-time timestamp count'
  require_fragment "$output_path" '"timestamp_log_append_time_records":0' 'JSON-object append-time timestamp count'
  require_fragment "$output_path" '"broker_offsets_committed":false' 'JSON-object broker-commit disclosure'
  require_fragment "$output_path" '"context_compiled":true' 'JSON-object context compilation disclosure'
  require_fragment "$output_path" "\"accepted_events\":${expected_context_events},\"distinct_schemas\":1" 'JSON-object context event count'
  require_fragment "$output_path" '"candidate_count":1,"included_count":1,"omitted_count":0' 'JSON-object context schema accounting'
  require_fragment "$output_path" "\"schema_id\":\"${JSON_SCHEMA_ID}\"" 'JSON-object derived schema ID'
  require_fragment "$output_path" "\"earliest\":\"decant://event/v0/${JSON_EFFECTIVE_SOURCE_ID}/kafka/0/0\"" 'JSON-object earliest provenance'
  require_fragment "$output_path" "\"latest\":\"decant://event/v0/${JSON_EFFECTIVE_SOURCE_ID}/kafka/0/${expected_latest_offset}\"" 'JSON-object latest provenance'
  require_fragment "$output_path" '"payload_fields_emitted":false,"keys_emitted":false,"transport_metadata_emitted":false' 'JSON-object omission disclosure'
  grep -E '"admission_state_hash":"sha256:v1:[0-9a-f]{64}"' "$output_path" >/dev/null || \
    fail 'JSON-object admission-state receipt'
  for private_value in \
    "$JSON_SOURCE_NAMESPACE" 'observed_time' 'raw_ok' 'raw_service' \
    'private-selected-service' 'private-unselected-value' 'private-key' \
    'private-header' 'private-header-value' "$JSON_TOPIC" "$CLUSTER_ID" "$JSON_TOPIC_ID"; do
    if grep -F "$private_value" "$output_path" >/dev/null; then
      fail 'JSON-object raw or broker content reached output'
    fi
  done
}

assert_material_change_run() {
  output_path=$1
  expected_data=$2
  expected_published=$3
  expected_generation=$4
  expected_previous=$5
  expected_current=$6
  expected_previous_offset=$7
  expected_current_offset=$8
  require_fragment "$output_path" '"format":"decant-kafka-live-run","version":3' 'material-change run format/version'
  require_fragment "$output_path" "\"data_records\":${expected_data}" 'material-change data-record count'
  require_fragment "$output_path" "\"processed\":${expected_data},\"advanced\":${expected_data},\"accepted\":${expected_data}" 'material-change admission counts'
  require_fragment "$output_path" "\"published\":${expected_published},\"generation\":${expected_generation}" 'material-change checkpoint result'
  require_fragment "$output_path" '"broker_offsets_committed":false' 'material-change broker-commit disclosure'
  require_fragment "$output_path" '"source_ownership":"static_partitions"' 'material-change source ownership'
  require_fragment "$output_path" '"context_compiled":true' 'material-change context compilation disclosure'
  require_fragment "$output_path" '"format":"decant-kafka-material-change-context","version":1' 'material-change context format/version'
  require_fragment "$output_path" '"view_id":"service-latency"' 'material-change view ID'
  require_fragment "$output_path" '"definition":"latest_distinct_retained_scalar_transition"' 'material-change definition'
  require_fragment "$output_path" '"source_finalized":false,"provisional":true' 'material-change provisional status'
  require_fragment "$output_path" '"candidate_count":1,"included_count":1,"omitted_count":0' 'material-change candidate accounting'
  require_fragment "$output_path" '"entity_key":{"encoding":"lower_hex","value":"636865636b6f7574"}' 'material-change entity key'
  require_fragment "$output_path" '"series":"latency"' 'material-change discriminator'
  require_fragment "$output_path" "\"previous\":{\"value\":{\"kind\":\"u64\",\"value\":${expected_previous}}" 'material-change previous value'
  require_fragment "$output_path" "\"current\":{\"value\":{\"kind\":\"u64\",\"value\":${expected_current}}" 'material-change current value'
  require_fragment "$output_path" "/kafka/0/${expected_previous_offset}\"" 'material-change previous provenance'
  require_fragment "$output_path" "/kafka/0/${expected_current_offset}\"" 'material-change current provenance'
  require_fragment "$output_path" '"allowed_field_count":3,"denied_field_count":1,"entity_key_action":"allow_lower_hex"' 'material-change field policy'
  require_fragment "$output_path" '"denied_fields_emitted":false,"unselected_payload_fields_emitted":false,"transport_metadata_emitted":false' 'material-change omissions'
  grep -E '"admission_state_hash":"sha256:v1:[0-9a-f]{64}"' "$output_path" >/dev/null || \
    fail 'material-change admission-state receipt'
  grep -E 'decant://event/v0/[0-9a-f]{64}/kafka/0/' "$output_path" >/dev/null || \
    fail 'material-change canonical Kafka provenance'
  for private_value in \
    "$MATERIAL_SOURCE_NAMESPACE" 'observed_time' 'raw_secret' 'raw_series' 'raw_service' \
    'raw_value' 'private-material-secret' 'private-unselected-value' 'private-key' \
    'private-header' 'private-header-value' "$MATERIAL_TOPIC" "$CLUSTER_ID" "$MATERIAL_TOPIC_ID"; do
    if grep -F "$private_value" "$output_path" >/dev/null; then
      fail 'material-change denied, unselected, or broker content reached output'
    fi
  done
}

extract_context() {
  awk '{ marker="\"context\":"; start=index($0, marker); if (start == 0) exit 1; print substr($0, start + length(marker), length($0) - start - length(marker)); }' "$1"
}

for command_name in docker cargo awk grep cmp find wc tr mktemp; do
  command -v "$command_name" >/dev/null 2>&1 || fail "missing prerequisite: ${command_name}"
done

REPOSITORY_ROOT="$(CDPATH= cd -- "$(dirname -- "$0")/../.." && pwd)"
DECANT_BINARY="${REPOSITORY_ROOT}/target/debug/decant"
INITIAL_EVENTS="${REPOSITORY_ROOT}/examples/kafka-live/events-initial-v0.jsonl"
SUFFIX_EVENT="${REPOSITORY_ROOT}/examples/kafka-live/event-suffix-v0.jsonl"
JSON_INITIAL_EVENTS="${REPOSITORY_ROOT}/examples/kafka-live/json-object-events-initial-v0.jsonl"
JSON_SUFFIX_EVENT="${REPOSITORY_ROOT}/examples/kafka-live/json-object-event-suffix-v0.jsonl"
MATERIAL_INITIAL_EVENTS="${REPOSITORY_ROOT}/examples/kafka-live/material-change-events-initial-v0.jsonl"
MATERIAL_SUFFIX_EVENT="${REPOSITORY_ROOT}/examples/kafka-live/material-change-event-suffix-v0.jsonl"

mkdir "$CHECKPOINT_DIRECTORY" "$CONTEXT_CHECKPOINT_DIRECTORY" "$JSON_CHECKPOINT_DIRECTORY" \
  "$MATERIAL_CHECKPOINT_DIRECTORY"
cargo build --manifest-path "${REPOSITORY_ROOT}/Cargo.toml" --locked --offline -p decant-cli --bin decant

printf '%s\n' "Starting pinned Apache Kafka broker on 127.0.0.1:9092"
CONTAINER_ATTEMPTED=1
docker run --detach --name "$CONTAINER_NAME" \
  --label "ai.decant.kafka-harness=${RUN_ID}" \
  --publish 127.0.0.1:9092:9092 \
  --env KAFKA_NODE_ID=1 \
  --env KAFKA_PROCESS_ROLES=broker,controller \
  --env KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  --env KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092 \
  --env KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  --env KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  --env KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 \
  --env KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  --env KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  --env KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  --env KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  "$IMAGE" >/dev/null

ready=0
attempt=0
while [ "$attempt" -lt 60 ]; do
  if docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    ready=1
    break
  fi
  attempt=$((attempt + 1))
  sleep 1
done
[ "$ready" -eq 1 ] || fail 'broker did not become ready'

docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null
docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic "$JSON_TOPIC" --partitions 1 --replication-factor 1 >/dev/null
docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic "$MATERIAL_TOPIC" --partitions 1 --replication-factor 1 >/dev/null

CLUSTER_ID="$(
  docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-metadata-quorum.sh \
    --bootstrap-server localhost:9092 describe --status |
    awk '$1 == "ClusterId:" { print $2; exit }'
)"
TOPIC_ID="$(
  docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --describe --topic "$TOPIC" |
    awk '{ for (field = 1; field <= NF; field += 1) if ($field == "TopicId:") { print $(field + 1); exit } }'
)"
JSON_TOPIC_ID="$(
  docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --describe --topic "$JSON_TOPIC" |
    awk '{ for (field = 1; field <= NF; field += 1) if ($field == "TopicId:") { print $(field + 1); exit } }'
)"
MATERIAL_TOPIC_ID="$(
  docker exec "$CONTAINER_NAME" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --describe --topic "$MATERIAL_TOPIC" |
    awk '{ for (field = 1; field <= NF; field += 1) if ($field == "TopicId:") { print $(field + 1); exit } }'
)"

[ -n "$CLUSTER_ID" ] || fail 'broker tooling returned no cluster ID'
[ "${#TOPIC_ID}" -eq 22 ] || fail 'broker tooling returned a noncanonical topic ID'
[ "${#JSON_TOPIC_ID}" -eq 22 ] || fail 'broker tooling returned a noncanonical JSON-object topic ID'
[ "${#MATERIAL_TOPIC_ID}" -eq 22 ] || fail 'broker tooling returned a noncanonical material-change topic ID'
case "$TOPIC_ID" in
  *[!A-Za-z0-9_-]*) fail 'broker tooling returned a noncanonical topic ID' ;;
esac
case "$JSON_TOPIC_ID" in
  *[!A-Za-z0-9_-]*) fail 'broker tooling returned a noncanonical JSON-object topic ID' ;;
esac
case "$MATERIAL_TOPIC_ID" in
  *[!A-Za-z0-9_-]*) fail 'broker tooling returned a noncanonical material-change topic ID' ;;
esac

case "$TOPIC_ID" in
  A*) WRONG_TOPIC_ID="B${TOPIC_ID#?}" ;;
  *) WRONG_TOPIC_ID="A${TOPIC_ID#?}" ;;
esac
write_config "$CONFIG_PATH" "$TOPIC_ID"
write_config "$WRONG_CONFIG_PATH" "$WRONG_TOPIC_ID"
write_context_config "$CONTEXT_CONFIG_PATH" "$TOPIC_ID"
write_json_object_config "$JSON_CONFIG_PATH" "$JSON_TOPIC_ID" milliseconds
write_json_object_config "$JSON_CHANGED_CONFIG_PATH" "$JSON_TOPIC_ID" microseconds
write_material_change_config "$MATERIAL_CONFIG_PATH" "$MATERIAL_TOPIC_ID"

docker exec -i "$CONTAINER_NAME" /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC" <"$INITIAL_EVENTS" >/dev/null

set +e
"$DECANT_BINARY" ingest kafka-json --config "$WRONG_CONFIG_PATH" \
  >"${WORK_DIRECTORY}/wrong.stdout" 2>"${WORK_DIRECTORY}/wrong.stderr"
wrong_status=$?
set -e
[ "$wrong_status" -eq 1 ] || fail 'wrong topic ID did not exit one'
require_empty "${WORK_DIRECTORY}/wrong.stdout" 'wrong topic ID wrote stdout'
printf '%s\n' 'error: Kafka JSON processing failed' >"${WORK_DIRECTORY}/expected-error.txt"
cmp -s "${WORK_DIRECTORY}/expected-error.txt" "${WORK_DIRECTORY}/wrong.stderr" || \
  fail 'wrong topic ID diagnostic changed'
[ -z "$(find "$CHECKPOINT_DIRECTORY" -mindepth 1 -print -quit)" ] || \
  fail 'wrong topic ID created checkpoint state'

"$DECANT_BINARY" ingest kafka-json --config "$CONFIG_PATH" >"${WORK_DIRECTORY}/first.json"
assert_run "${WORK_DIRECTORY}/first.json" 3 3 3 true 1
[ -f "${CHECKPOINT_DIRECTORY}/checkpoint-00000000000000000001.dcp" ] || \
  fail 'generation 1 checkpoint missing'

"$DECANT_BINARY" ingest kafka-json --config "$CONFIG_PATH" >"${WORK_DIRECTORY}/idle.json"
assert_run "${WORK_DIRECTORY}/idle.json" 0 0 0 false 1

docker exec -i "$CONTAINER_NAME" /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TOPIC" <"$SUFFIX_EVENT" >/dev/null
"$DECANT_BINARY" ingest kafka-json --config "$CONFIG_PATH" >"${WORK_DIRECTORY}/suffix.json"
assert_run "${WORK_DIRECTORY}/suffix.json" 1 1 1 true 2
[ -f "${CHECKPOINT_DIRECTORY}/checkpoint-00000000000000000002.dcp" ] || \
  fail 'generation 2 checkpoint missing'

"$DECANT_BINARY" ingest kafka-json --config "$CONFIG_PATH" \
  >"${WORK_DIRECTORY}/after-suffix.json"
assert_run "${WORK_DIRECTORY}/after-suffix.json" 0 0 0 false 2

[ "$(find "$CHECKPOINT_DIRECTORY" -type f -name '*.dcp' | wc -l | tr -d ' ')" -eq 2 ] || \
  fail 'unexpected published checkpoint count'
[ -z "$(find "$CHECKPOINT_DIRECTORY" -type f -name '*.pending' -print -quit)" ] || \
  fail 'pending checkpoint remained after successful runs'

"$DECANT_BINARY" ingest kafka-json --config "$CONTEXT_CONFIG_PATH" \
  >"${WORK_DIRECTORY}/context-first.json"
assert_context_run "${WORK_DIRECTORY}/context-first.json" 3 3 3 true 1 3 2
[ -f "${CONTEXT_CHECKPOINT_DIRECTORY}/checkpoint-00000000000000000001.dcp" ] || \
  fail 'context generation 1 checkpoint missing'

"$DECANT_BINARY" ingest kafka-json --config "$CONTEXT_CONFIG_PATH" \
  >"${WORK_DIRECTORY}/context-suffix.json"
assert_context_run "${WORK_DIRECTORY}/context-suffix.json" 1 1 1 true 2 4 3
[ -f "${CONTEXT_CHECKPOINT_DIRECTORY}/checkpoint-00000000000000000002.dcp" ] || \
  fail 'context generation 2 checkpoint missing'

"$DECANT_BINARY" ingest kafka-json --config "$CONTEXT_CONFIG_PATH" \
  >"${WORK_DIRECTORY}/context-idle.json"
assert_context_run "${WORK_DIRECTORY}/context-idle.json" 0 0 0 false 2 4 3
extract_context "${WORK_DIRECTORY}/context-suffix.json" >"${WORK_DIRECTORY}/context-suffix-only.json"
extract_context "${WORK_DIRECTORY}/context-idle.json" >"${WORK_DIRECTORY}/context-idle-only.json"
cmp -s "${WORK_DIRECTORY}/context-suffix-only.json" "${WORK_DIRECTORY}/context-idle-only.json" || \
  fail 'idle recovery changed nested context bytes'

[ "$(find "$CONTEXT_CHECKPOINT_DIRECTORY" -type f -name '*.dcp' | wc -l | tr -d ' ')" -eq 2 ] || \
  fail 'unexpected context published checkpoint count'
[ -z "$(find "$CONTEXT_CHECKPOINT_DIRECTORY" -type f -name '*.pending' -print -quit)" ] || \
  fail 'pending context checkpoint remained after successful runs'

produce_json_objects "$JSON_INITIAL_EVENTS" 0 "${WORK_DIRECTORY}/json-object-initial-producer.txt"
"$DECANT_BINARY" ingest kafka-json --config "$JSON_CONFIG_PATH" \
  >"${WORK_DIRECTORY}/json-object-first.json" 2>"${WORK_DIRECTORY}/json-object-first.stderr"
require_empty "${WORK_DIRECTORY}/json-object-first.stderr" 'JSON-object first run wrote stderr'
assert_json_object_run "${WORK_DIRECTORY}/json-object-first.json" 3 true 1 3 2 3 129
[ -f "${JSON_CHECKPOINT_DIRECTORY}/checkpoint-00000000000000000001.dcp" ] || \
  fail 'JSON-object generation 1 checkpoint missing'

produce_json_objects "$JSON_SUFFIX_EVENT" 3 "${WORK_DIRECTORY}/json-object-suffix-producer.txt"
"$DECANT_BINARY" ingest kafka-json --config "$JSON_CONFIG_PATH" \
  >"${WORK_DIRECTORY}/json-object-suffix.json" 2>"${WORK_DIRECTORY}/json-object-suffix.stderr"
require_empty "${WORK_DIRECTORY}/json-object-suffix.stderr" 'JSON-object suffix run wrote stderr'
assert_json_object_run "${WORK_DIRECTORY}/json-object-suffix.json" 1 true 2 4 3 1 43
[ -f "${JSON_CHECKPOINT_DIRECTORY}/checkpoint-00000000000000000002.dcp" ] || \
  fail 'JSON-object generation 2 checkpoint missing'

"$DECANT_BINARY" ingest kafka-json --config "$JSON_CONFIG_PATH" \
  >"${WORK_DIRECTORY}/json-object-idle.json" 2>"${WORK_DIRECTORY}/json-object-idle.stderr"
require_empty "${WORK_DIRECTORY}/json-object-idle.stderr" 'JSON-object idle run wrote stderr'
assert_json_object_run "${WORK_DIRECTORY}/json-object-idle.json" 0 false 2 4 3 0 0
extract_context "${WORK_DIRECTORY}/json-object-suffix.json" >"${WORK_DIRECTORY}/json-object-suffix-context.json"
extract_context "${WORK_DIRECTORY}/json-object-idle.json" >"${WORK_DIRECTORY}/json-object-idle-context.json"
cmp -s "${WORK_DIRECTORY}/json-object-suffix-context.json" \
  "${WORK_DIRECTORY}/json-object-idle-context.json" || \
  fail 'JSON-object idle recovery changed nested context bytes'

set +e
"$DECANT_BINARY" ingest kafka-json --config "$JSON_CHANGED_CONFIG_PATH" \
  >"${WORK_DIRECTORY}/json-object-changed.stdout" 2>"${WORK_DIRECTORY}/json-object-changed.stderr"
json_changed_status=$?
set -e
[ "$json_changed_status" -eq 1 ] || fail 'changed JSON-object mapping did not exit one'
require_empty "${WORK_DIRECTORY}/json-object-changed.stdout" 'changed JSON-object mapping wrote stdout'
cmp -s "${WORK_DIRECTORY}/expected-error.txt" "${WORK_DIRECTORY}/json-object-changed.stderr" || \
  fail 'changed JSON-object mapping diagnostic changed'

[ "$(find "$JSON_CHECKPOINT_DIRECTORY" -type f -name '*.dcp' | wc -l | tr -d ' ')" -eq 2 ] || \
  fail 'unexpected JSON-object published checkpoint count'
[ -z "$(find "$JSON_CHECKPOINT_DIRECTORY" -type f -name '*.pending' -print -quit)" ] || \
  fail 'pending JSON-object checkpoint remained after successful runs'

produce_material_objects "$MATERIAL_INITIAL_EVENTS" 0 \
  "${WORK_DIRECTORY}/material-change-initial-producer.txt"
"$DECANT_BINARY" ingest kafka-json --config "$MATERIAL_CONFIG_PATH" \
  >"${WORK_DIRECTORY}/material-change-first.json" \
  2>"${WORK_DIRECTORY}/material-change-first.stderr"
require_empty "${WORK_DIRECTORY}/material-change-first.stderr" 'material-change first run wrote stderr'
assert_material_change_run "${WORK_DIRECTORY}/material-change-first.json" 2 true 1 10 20 0 1
[ -f "${MATERIAL_CHECKPOINT_DIRECTORY}/checkpoint-00000000000000000001.dcp" ] || \
  fail 'material-change generation 1 checkpoint missing'

produce_material_objects "$MATERIAL_SUFFIX_EVENT" 2 \
  "${WORK_DIRECTORY}/material-change-suffix-producer.txt"
"$DECANT_BINARY" ingest kafka-json --config "$MATERIAL_CONFIG_PATH" \
  >"${WORK_DIRECTORY}/material-change-suffix.json" \
  2>"${WORK_DIRECTORY}/material-change-suffix.stderr"
require_empty "${WORK_DIRECTORY}/material-change-suffix.stderr" 'material-change suffix run wrote stderr'
assert_material_change_run "${WORK_DIRECTORY}/material-change-suffix.json" 1 true 2 20 30 1 2
[ -f "${MATERIAL_CHECKPOINT_DIRECTORY}/checkpoint-00000000000000000002.dcp" ] || \
  fail 'material-change generation 2 checkpoint missing'

"$DECANT_BINARY" ingest kafka-json --config "$MATERIAL_CONFIG_PATH" \
  >"${WORK_DIRECTORY}/material-change-idle.json" \
  2>"${WORK_DIRECTORY}/material-change-idle.stderr"
require_empty "${WORK_DIRECTORY}/material-change-idle.stderr" 'material-change idle run wrote stderr'
assert_material_change_run "${WORK_DIRECTORY}/material-change-idle.json" 0 false 2 20 30 1 2
extract_context "${WORK_DIRECTORY}/material-change-suffix.json" \
  >"${WORK_DIRECTORY}/material-change-suffix-context.json"
extract_context "${WORK_DIRECTORY}/material-change-idle.json" \
  >"${WORK_DIRECTORY}/material-change-idle-context.json"
cmp -s "${WORK_DIRECTORY}/material-change-suffix-context.json" \
  "${WORK_DIRECTORY}/material-change-idle-context.json" || \
  fail 'material-change idle recovery changed nested context bytes'

[ "$(find "$MATERIAL_CHECKPOINT_DIRECTORY" -type f -name '*.dcp' | wc -l | tr -d ' ')" -eq 2 ] || \
  fail 'unexpected material-change published checkpoint count'
[ -z "$(find "$MATERIAL_CHECKPOINT_DIRECTORY" -type f -name '*.pending' -print -quit)" ] || \
  fail 'pending material-change checkpoint remained after successful runs'

printf '%s\n' 'PASS: v0 first=3/generation-1 idle=0 suffix=1/generation-2 restart=0; v1 first=3/generation-1 suffix=1/generation-2 idle=0/context-stable; v2 JSON objects first=3/generation-1 suffix=1/generation-2 idle=0/context-stable mapping-change=refused; v4 material changes first=2/generation-1 suffix=1/generation-2 idle=0/context-stable'
