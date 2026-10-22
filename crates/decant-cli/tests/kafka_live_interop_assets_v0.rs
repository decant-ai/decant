use decant_kafka::decode_decant_kafka_event_value_v0;

const HARNESS: &str = include_str!("../../../examples/kafka-live/run-apache-kafka-v0.sh");
const GUIDE: &str = include_str!("../../../examples/kafka-live/README.md");
const INITIAL_EVENTS: &str = include_str!("../../../examples/kafka-live/events-initial-v0.jsonl");
const SUFFIX_EVENT: &str = include_str!("../../../examples/kafka-live/event-suffix-v0.jsonl");
const JSON_OBJECT_INITIAL_EVENTS: &str =
    include_str!("../../../examples/kafka-live/json-object-events-initial-v0.jsonl");
const JSON_OBJECT_SUFFIX_EVENT: &str =
    include_str!("../../../examples/kafka-live/json-object-event-suffix-v0.jsonl");
const MATERIAL_CHANGE_INITIAL_EVENTS: &str =
    include_str!("../../../examples/kafka-live/material-change-events-initial-v0.jsonl");
const MATERIAL_CHANGE_SUFFIX_EVENT: &str =
    include_str!("../../../examples/kafka-live/material-change-event-suffix-v0.jsonl");

#[test]
fn opt_in_harness_is_pinned_bounded_and_uses_the_public_cli() {
    for required in [
        "IMAGE=\"apache/kafka:4.3.1\"",
        "--publish 127.0.0.1:9092:9092",
        "--label \"ai.decant.kafka-harness=${RUN_ID}\"",
        "docker inspect --format",
        "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092",
        "kafka-metadata-quorum.sh",
        "kafka-topics.sh",
        "TopicId:",
        "ingest kafka-json --config",
        "wrong topic ID created checkpoint state",
        "assert_run \"${WORK_DIRECTORY}/first.json\" 3 3 3 true 1",
        "assert_run \"${WORK_DIRECTORY}/idle.json\" 0 0 0 false 1",
        "assert_run \"${WORK_DIRECTORY}/suffix.json\" 1 1 1 true 2",
        "assert_run \"${WORK_DIRECTORY}/after-suffix.json\" 0 0 0 false 2",
        "SOURCE_ID=\"decant-source-v0-${RUN_ID}\"",
        "CONTEXT_CHECKPOINT_DIRECTORY",
        "write_context_config",
        "\"version\": 1",
        "decant-kafka-activity-context-request",
        "assert_context_run \"${WORK_DIRECTORY}/context-first.json\" 3 3 3 true 1 3 2",
        "assert_context_run \"${WORK_DIRECTORY}/context-suffix.json\" 1 1 1 true 2 4 3",
        "assert_context_run \"${WORK_DIRECTORY}/context-idle.json\" 0 0 0 false 2 4 3",
        "idle recovery changed nested context bytes",
        "payload or broker identity reached context output",
        "JSON_TOPIC=\"decant-json-object-v0-${RUN_ID}\"",
        "JSON_CHECKPOINT_DIRECTORY",
        "write_json_object_config",
        "\"version\": 2",
        "decant-kafka-json-object-mapping",
        "\"event_time\": {\"field\": \"observed_time\", \"unit\": \"%s\"}",
        "--property parse.headers=true --property parse.key=true",
        "json-object-events-initial-v0.jsonl",
        "json-object-event-suffix-v0.jsonl",
        "assert_json_object_run \"${WORK_DIRECTORY}/json-object-first.json\" 3 true 1 3 2 3 129",
        "assert_json_object_run \"${WORK_DIRECTORY}/json-object-suffix.json\" 1 true 2 4 3 1 43",
        "assert_json_object_run \"${WORK_DIRECTORY}/json-object-idle.json\" 0 false 2 4 3 0 0",
        "JSON-object idle recovery changed nested context bytes",
        "changed JSON-object mapping wrote stdout",
        "pending JSON-object checkpoint remained after successful runs",
        "JSON-object raw or broker content reached output",
        "JSON_EFFECTIVE_SOURCE_ID",
        "JSON_SCHEMA_ID",
        "MATERIAL_TOPIC=\"decant-material-change-v0-${RUN_ID}\"",
        "MATERIAL_CHECKPOINT_DIRECTORY",
        "write_material_change_config",
        "\"version\": 4",
        "decant-kafka-material-change-context-request",
        "\"entity_key\": {\"source\": \"utf8_projection\", \"field\": \"service\"}",
        "material-change-events-initial-v0.jsonl",
        "material-change-event-suffix-v0.jsonl",
        "assert_material_change_run \"${WORK_DIRECTORY}/material-change-first.json\" 2 true 1 10 20 0 1",
        "assert_material_change_run \"${WORK_DIRECTORY}/material-change-suffix.json\" 1 true 2 20 30 1 2",
        "assert_material_change_run \"${WORK_DIRECTORY}/material-change-idle.json\" 0 false 2 20 30 1 2",
        "material-change idle recovery changed nested context bytes",
        "pending material-change checkpoint remained after successful runs",
        "material-change denied, unselected, or broker content reached output",
        "latest_distinct_retained_scalar_transition",
        "source_ownership\":\"static_partitions",
        "admission_state_hash",
        "\"broker_offsets_committed\":false",
        "\"context_compiled\":false",
        "\"context_compiled\":true",
        "trap cleanup EXIT HUP INT TERM",
    ] {
        assert!(
            HARNESS.contains(required),
            "missing harness contract: {required}"
        );
    }

    let lower = HARNESS.to_ascii_lowercase();
    for forbidden in [
        ":latest",
        "sasl",
        "ssl",
        "password",
        "username",
        "credential",
        "opentelemetry",
        "otel",
        concat!("sub", "scribe("),
        concat!("com", "mit("),
    ] {
        assert!(
            !lower.contains(forbidden),
            "forbidden harness surface: {forbidden}"
        );
    }
}

#[test]
fn material_change_fixtures_are_bounded_ordinary_json_with_one_exact_transition() {
    let initial = MATERIAL_CHANGE_INITIAL_EVENTS.lines().collect::<Vec<_>>();
    let suffix = MATERIAL_CHANGE_SUFFIX_EVENT.lines().collect::<Vec<_>>();
    assert_eq!(initial.len(), 2);
    assert_eq!(suffix.len(), 1);

    for (ordinal, line) in initial.into_iter().chain(suffix).enumerate() {
        assert!(line.len() <= 1_024);
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        let object = value.as_object().unwrap();
        assert_eq!(object.len(), 6);
        assert_eq!(object["observed_time"], serde_json::json!(100 + ordinal));
        assert_eq!(object["raw_service"], "checkout");
        assert_eq!(object["raw_series"], "latency");
        assert_eq!(
            object["raw_value"],
            serde_json::json!(10_u64 * u64::try_from(ordinal + 1).unwrap())
        );
        assert_eq!(object["raw_secret"], "private-material-secret");
        assert_eq!(object["private_unselected"], "private-unselected-value");
        assert!(!object.contains_key("record_format"));
        assert!(!object.contains_key("payload"));
    }
}

#[test]
fn ordinary_json_fixtures_are_bounded_top_level_objects_not_decant_envelopes() {
    let initial = JSON_OBJECT_INITIAL_EVENTS.lines().collect::<Vec<_>>();
    let suffix = JSON_OBJECT_SUFFIX_EVENT.lines().collect::<Vec<_>>();
    assert_eq!(initial.len(), 3);
    assert_eq!(suffix.len(), 1);

    for (ordinal, line) in initial.into_iter().chain(suffix).enumerate() {
        assert!(line.len() <= 1_024);
        let value: serde_json::Value = serde_json::from_str(line).unwrap();
        let object = value.as_object().unwrap();
        assert_eq!(object.len(), 4);
        assert_eq!(
            object["observed_time"],
            serde_json::json!(i64::try_from((ordinal + 1) * 100).unwrap())
        );
        assert!(object["raw_ok"].is_boolean());
        assert!(object["raw_service"].is_string());
        assert!(object["private_unselected"].is_string());
        for envelope_field in [
            "record_format",
            "record_version",
            "schema_id",
            "event_time_ns",
            "payload",
        ] {
            assert!(!object.contains_key(envelope_field));
        }
    }
}

#[test]
fn fixtures_are_canonical_bounded_and_unmistakably_synthetic() {
    let initial = INITIAL_EVENTS.lines().collect::<Vec<_>>();
    let suffix = SUFFIX_EVENT.lines().collect::<Vec<_>>();
    assert_eq!(initial.len(), 3);
    assert_eq!(suffix.len(), 1);

    for (ordinal, line) in initial.into_iter().chain(suffix).enumerate() {
        assert!(line.len() <= 1_024);
        let decoded = decode_decant_kafka_event_value_v0(Some(line.as_bytes())).unwrap();
        assert_eq!(decoded.schema_id().as_str(), "synthetic.kafka.metric.v0");
        assert_eq!(
            decoded.event_time_ns(),
            i64::try_from((ordinal + 1) * 100).unwrap()
        );
        assert_eq!(decoded.payload().fields().len(), 1);
    }

    let all = format!("{INITIAL_EVENTS}{SUFFIX_EVENT}").to_ascii_lowercase();
    for forbidden in [
        "customer",
        "email",
        "token",
        "secret",
        "password",
        "authorization",
        "trace_id",
        "span_id",
        "opentelemetry",
    ] {
        assert!(
            !all.contains(forbidden),
            "production-looking fixture field: {forbidden}"
        );
    }
}

#[test]
fn guide_marks_the_workflow_optional_and_freezes_its_limitations() {
    for required in [
        "opt-in workflow",
        "not a broker-specific product architecture",
        "./examples/kafka-live/run-apache-kafka-v0.sh",
        "apache/kafka:4.3.1",
        "Decant-native JSON record values",
        "payload-free Kafka activity context",
        "ordinary top-level JSON objects",
        "strict version-2 configured JSON-object mapping",
        "strict version-4 entity-keyed material-change context",
        "generic bounded top-level JSON mapping",
        "not an OpenTelemetry-specific path",
        "not arbitrary JSON support",
        "source_namespace",
        "decant-kafka-json-object-mapping",
        "nanoseconds`, `microseconds`, `milliseconds`, or `seconds",
        "`null`, `bool`, `i64`, `u64`, or `utf8`",
        "there is no nesting, JSONPath, array/float support, inference, coercion, or defaulting",
        "Use a fresh checkpoint directory or a deliberately new source namespace",
        "not payload semantics or policy-safe values",
        "decant-kafka-activity-context-request",
        "between 512 and 16384 inclusive",
        "Config version 0 remains available",
        "byte-identical nested context",
        "does not contact Docker",
        "does not establish a supported Kafka-version matrix",
        "consumer-group rebalancing",
        "broker offset commits",
        "exactly-once delivery",
        "authenticated or encrypted transport",
        "performance",
        "security",
        "production readiness",
        "launch readiness",
        "payload-derived semantic context",
        "field-level policy",
        "policy-safe egress",
        "source completeness",
        "privacy",
        "not a content digest",
        "https://kafka.apache.org/43/getting-started/docker/",
    ] {
        assert!(
            GUIDE.contains(required),
            "missing guide contract: {required}"
        );
    }
    assert!(!GUIDE.contains("exactly once"));
    assert!(!GUIDE.contains("production-ready"));
    assert!(!GUIDE.contains("all Kafka"));
}
