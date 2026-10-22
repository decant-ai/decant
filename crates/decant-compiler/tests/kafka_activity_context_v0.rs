use std::collections::BTreeMap;

use decant_compiler::{
    KAFKA_ACTIVITY_CONTEXT_VERSION_V0, KafkaActivityContextConfigErrorV0,
    KafkaActivityContextErrorV0, KafkaActivityContextRequestV0, KafkaActivityContractIssueV0,
    MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0, MAX_KAFKA_ACTIVITY_DISTINCT_SCHEMAS_V0,
    MIN_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0, compile_kafka_activity_context_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    FinalizeOutcomeV0, encode_admission_snapshot_v1, hash_admission_state_v1,
    restore_admission_snapshot_v1,
};

const SOURCE_ID: &str = "kafka-activity-v0";

fn source() -> SourceIdV0 {
    SourceIdV0::new(SOURCE_ID).unwrap()
}

fn config(kind: SourcePositionKindV0) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source(),
        kind,
        i64::MIN,
        i64::MAX,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn admission(kind: SourcePositionKindV0) -> AdmissionStateV0 {
    AdmissionStateV0::new(config(kind))
}

fn request(byte_budget: usize) -> KafkaActivityContextRequestV0 {
    KafkaActivityContextRequestV0::new(KAFKA_ACTIVITY_CONTEXT_VERSION_V0, byte_budget).unwrap()
}

fn payload() -> CanonicalPayloadV0 {
    let mut fields = BTreeMap::new();
    fields.insert(
        "private.payload.field".to_owned(),
        ScalarValueV0::utf8("private-payload-value").unwrap(),
    );
    fields.insert(
        "transport.header.secret".to_owned(),
        ScalarValueV0::utf8("private-header-value").unwrap(),
    );
    CanonicalPayloadV0::try_from_fields(fields).unwrap()
}

fn event(schema_id: &str, partition: i32, offset: i64, event_time_ns: i64) -> EventEnvelopeV0 {
    event_with_sensitivity(schema_id, partition, offset, event_time_ns, u64::MAX)
}

fn event_with_sensitivity(
    schema_id: &str,
    partition: i32,
    offset: i64,
    event_time_ns: i64,
    sensitivity: u64,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(),
        SchemaIdV0::new(schema_id).unwrap(),
        SourcePositionV0::kafka(partition, offset).unwrap(),
        Some(b"private-kafka-key"),
        event_time_ns,
        987_654_321,
        SensitivityBitmapV0::from_bits(sensitivity),
        payload(),
    )
    .unwrap()
}

fn accept(state: &mut AdmissionStateV0, event: &EventEnvelopeV0) {
    assert_eq!(state.apply_event_v1(event), Ok(ApplyOutcomeV0::Accepted));
}

fn populated(events: &[EventEnvelopeV0]) -> AdmissionStateV0 {
    let mut state = admission(SourcePositionKindV0::Kafka);
    for event in events {
        accept(&mut state, event);
    }
    state
}

#[test]
fn request_boundaries_are_exact_and_value_safe() {
    assert_eq!(
        KafkaActivityContextRequestV0::new(1, MIN_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0),
        Err(KafkaActivityContextConfigErrorV0::UnsupportedVersion { actual: 1 })
    );
    for actual in [0, MIN_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0 - 1] {
        assert_eq!(
            KafkaActivityContextRequestV0::new(KAFKA_ACTIVITY_CONTEXT_VERSION_V0, actual),
            Err(KafkaActivityContextConfigErrorV0::OutputBytesBelowMinimum {
                minimum: MIN_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0,
                actual,
            })
        );
    }
    assert!(
        KafkaActivityContextRequestV0::new(
            KAFKA_ACTIVITY_CONTEXT_VERSION_V0,
            MIN_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0
        )
        .is_ok()
    );
    assert!(
        KafkaActivityContextRequestV0::new(
            KAFKA_ACTIVITY_CONTEXT_VERSION_V0,
            MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0
        )
        .is_ok()
    );
    assert_eq!(
        KafkaActivityContextRequestV0::new(
            KAFKA_ACTIVITY_CONTEXT_VERSION_V0,
            MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0 + 1,
        ),
        Err(KafkaActivityContextConfigErrorV0::OutputBytesAboveMaximum {
            maximum: MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0,
            actual: MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0 + 1,
        })
    );
}

#[test]
fn empty_kafka_state_is_explicit_and_bounded() {
    let state = admission(SourcePositionKindV0::Kafka);
    let expected_hash = hash_admission_state_v1(&state)
        .unwrap()
        .to_canonical_string();
    let context = compile_kafka_activity_context_v0(
        &state,
        request(MIN_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0),
    )
    .unwrap();
    let text = std::str::from_utf8(context.as_bytes()).unwrap();

    assert_eq!(context.accepted_events(), 0);
    assert_eq!(context.candidate_schema_count(), 0);
    assert_eq!(context.included_schema_count(), 0);
    assert_eq!(context.omitted_schema_count(), 0);
    assert!(!context.source_finalized());
    assert!(context.as_bytes().len() <= MIN_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0);
    assert!(text.contains("\"source_finalized\":false"));
    assert!(text.contains("\"accepted_events\":0,\"distinct_schemas\":0"));
    assert!(
        text.contains(
            "\"candidate_count\":0,\"included_count\":0,\"omitted_count\":0,\"items\":[]"
        )
    );
    assert!(text.contains(&format!("\"admission_state_hash\":\"{expected_hash}\"")));
}

#[test]
fn schemas_are_canonical_and_evidence_uses_total_event_rank() {
    let events = [
        event("schema-b", 0, 2, 200),
        event("schema-a", 1, 0, 100),
        event("schema-a", 0, 9, 100),
        event("schema-a", 0, 3, 300),
    ];
    let mut state = populated(&events);
    assert_eq!(
        state.finalize_source(&source(), i64::MAX),
        FinalizeOutcomeV0::Finalized
    );
    let context = compile_kafka_activity_context_v0(
        &state,
        request(MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0),
    )
    .unwrap();
    let text = std::str::from_utf8(context.as_bytes()).unwrap();

    assert_eq!(context.accepted_events(), 4);
    assert_eq!(context.candidate_schema_count(), 2);
    assert_eq!(context.included_schema_count(), 2);
    assert_eq!(context.omitted_schema_count(), 0);
    assert!(context.source_finalized());
    assert!(text.contains("\"source_finalized\":true"));
    let schema_a = text.find("\"schema_id\":\"schema-a\"").unwrap();
    let schema_b = text.find("\"schema_id\":\"schema-b\"").unwrap();
    assert!(schema_a < schema_b);
    assert!(text.contains("\"event_count\":3,\"event_time_ns\":{\"minimum\":100,\"maximum\":300}"));
    assert!(text.contains(
        "\"earliest\":\"decant://event/v0/kafka-activity-v0/kafka/0/9\",\"latest\":\"decant://event/v0/kafka-activity-v0/kafka/0/3\""
    ));
}

#[test]
fn arrival_permutation_and_snapshot_restore_are_byte_identical() {
    let events = [
        event("schema-z", 1, 1, 1_000),
        event("schema-a", 0, 2, 1_001),
        event("schema-z", 0, 4, 1_002),
    ];
    let left = populated(&events);
    let right = populated(&[events[2].clone(), events[0].clone(), events[1].clone()]);
    let bytes = encode_admission_snapshot_v1(&left).unwrap();
    let restored =
        restore_admission_snapshot_v1(&bytes, &config(SourcePositionKindV0::Kafka)).unwrap();
    let compile = |state: &AdmissionStateV0| {
        compile_kafka_activity_context_v0(
            state,
            request(MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0),
        )
        .unwrap()
        .as_bytes()
        .to_vec()
    };

    assert_eq!(compile(&left), compile(&right));
    assert_eq!(compile(&left), compile(&restored));
    assert_eq!(
        hash_admission_state_v1(&left),
        hash_admission_state_v1(&restored)
    );
}

#[test]
fn byte_budget_selects_the_largest_canonical_prefix() {
    let state = populated(&[
        event("schema-a", 0, 0, 100),
        event("schema-b", 0, 1, 101),
        event("schema-c", 0, 2, 102),
    ]);
    let full = compile_kafka_activity_context_v0(
        &state,
        request(MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0),
    )
    .unwrap();
    assert_eq!(full.included_schema_count(), 3);

    let one_byte_short =
        compile_kafka_activity_context_v0(&state, request(full.as_bytes().len() - 1)).unwrap();
    let text = std::str::from_utf8(one_byte_short.as_bytes()).unwrap();
    assert_eq!(one_byte_short.candidate_schema_count(), 3);
    assert_eq!(one_byte_short.included_schema_count(), 2);
    assert_eq!(one_byte_short.omitted_schema_count(), 1);
    assert!(one_byte_short.as_bytes().len() < full.as_bytes().len());
    assert!(text.contains("\"schema_id\":\"schema-a\""));
    assert!(text.contains("\"schema_id\":\"schema-b\""));
    assert!(!text.contains("\"schema_id\":\"schema-c\""));
}

#[test]
fn distinct_schema_capacity_refuses_the_next_group_without_context() {
    let mut state = admission(SourcePositionKindV0::Kafka);
    for index in 0..MAX_KAFKA_ACTIVITY_DISTINCT_SCHEMAS_V0 {
        accept(
            &mut state,
            &event(&format!("schema-{index:04}"), 0, index as i64, 1_000),
        );
    }
    let context = compile_kafka_activity_context_v0(
        &state,
        request(MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0),
    )
    .unwrap();
    assert_eq!(
        context.candidate_schema_count(),
        MAX_KAFKA_ACTIVITY_DISTINCT_SCHEMAS_V0
    );
    assert_eq!(
        context.included_schema_count() + context.omitted_schema_count(),
        MAX_KAFKA_ACTIVITY_DISTINCT_SCHEMAS_V0
    );

    accept(
        &mut state,
        &event(
            "schema-over-capacity",
            0,
            MAX_KAFKA_ACTIVITY_DISTINCT_SCHEMAS_V0 as i64,
            1_000,
        ),
    );
    assert_eq!(
        compile_kafka_activity_context_v0(
            &state,
            request(MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0)
        ),
        Err(KafkaActivityContextErrorV0::DistinctSchemaCapacity {
            maximum: MAX_KAFKA_ACTIVITY_DISTINCT_SCHEMAS_V0,
            attempted: MAX_KAFKA_ACTIVITY_DISTINCT_SCHEMAS_V0 + 1,
        })
    );
}

#[test]
fn non_kafka_and_invalid_event_contracts_fail_without_value_disclosure() {
    let wrong_kind = admission(SourcePositionKindV0::Synthetic);
    assert_eq!(
        compile_kafka_activity_context_v0(
            &wrong_kind,
            request(MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0)
        ),
        Err(KafkaActivityContextErrorV0::UnexpectedSourcePositionKind)
    );

    let invalid = event_with_sensitivity("schema-private", 0, 0, 100, 0);
    let state = populated(&[invalid]);
    let error = compile_kafka_activity_context_v0(
        &state,
        request(MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0),
    )
    .unwrap_err();
    assert_eq!(
        error,
        KafkaActivityContextErrorV0::InvalidEventContract {
            issue: KafkaActivityContractIssueV0::Sensitivity,
        }
    );
    let diagnostic = format!("{error:?} {error}");
    for private in [
        "schema-private",
        "private.payload.field",
        "private-payload-value",
        "private-kafka-key",
        "private-header-value",
    ] {
        assert!(!diagnostic.contains(private));
    }
}

#[test]
fn context_omits_payload_keys_and_transport_like_values() {
    let state = populated(&[event("schema-public", 0, 7, 100)]);
    let context = compile_kafka_activity_context_v0(
        &state,
        request(MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0),
    )
    .unwrap();
    let text = std::str::from_utf8(context.as_bytes()).unwrap();

    assert!(text.contains("\"payload_fields_emitted\":false"));
    assert!(text.contains("\"keys_emitted\":false"));
    assert!(text.contains("\"transport_metadata_emitted\":false"));
    assert!(text.contains("decant://event/v0/kafka-activity-v0/kafka/0/7"));
    for private in [
        "private.payload.field",
        "private-payload-value",
        "private-kafka-key",
        "transport.header.secret",
        "private-header-value",
    ] {
        assert!(
            !text.contains(private),
            "private fixture value leaked: {private}"
        );
    }
}
