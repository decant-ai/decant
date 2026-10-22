use std::collections::BTreeMap;

use decant_compiler::{
    KAFKA_FIELD_EGRESS_POLICY_VERSION_V0, KAFKA_GROUPED_RATIO_CONTEXT_VERSION_V0,
    KafkaFieldEgressActionV0, KafkaFieldEgressPolicyV0, KafkaFieldEgressRuleV0,
    KafkaGroupedRatioContextConfigErrorV0, KafkaGroupedRatioContextRequestV0,
    MAX_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0,
    MIN_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0, compile_kafka_grouped_ratio_context_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    encode_admission_snapshot_v1, restore_admission_snapshot_v1,
};

fn source() -> SourceIdV0 {
    SourceIdV0::new("kafka-grouped-ratio-source").unwrap()
}

fn schema() -> SchemaIdV0 {
    SchemaIdV0::new("mapped-orders-v0").unwrap()
}

fn config() -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source(),
        SourcePositionKindV0::Kafka,
        0,
        1_000_000_000_000,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn state() -> AdmissionStateV0 {
    AdmissionStateV0::new(config())
}

fn policy(
    group_action: KafkaFieldEgressActionV0,
) -> Result<KafkaFieldEgressPolicyV0, KafkaGroupedRatioContextConfigErrorV0> {
    KafkaFieldEgressPolicyV0::new(
        KAFKA_FIELD_EGRESS_POLICY_VERSION_V0,
        vec![
            KafkaFieldEgressRuleV0::new("service", group_action)?,
            KafkaFieldEgressRuleV0::new("is_error", KafkaFieldEgressActionV0::Allow)?,
            KafkaFieldEgressRuleV0::new("secret", KafkaFieldEgressActionV0::Deny)?,
        ],
    )
}

fn request(byte_budget: usize) -> KafkaGroupedRatioContextRequestV0 {
    KafkaGroupedRatioContextRequestV0::new(
        KAFKA_GROUPED_RATIO_CONTEXT_VERSION_V0,
        schema(),
        "error-rate-by-service",
        "service",
        "is_error",
        true,
        600,
        3,
        byte_budget,
        policy(KafkaFieldEgressActionV0::Allow).unwrap(),
    )
    .unwrap()
}

fn event(offset: i64, time: i64, service: &str, is_error: bool) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(),
        schema(),
        SourcePositionV0::kafka(0, offset).unwrap(),
        Some(b"private-kafka-key"),
        time,
        0,
        SensitivityBitmapV0::from_bits(u64::MAX),
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([
            ("is_error".to_owned(), ScalarValueV0::from_bool(is_error)),
            (
                "secret".to_owned(),
                ScalarValueV0::utf8("private-denied-sentinel").unwrap(),
            ),
            ("service".to_owned(), ScalarValueV0::utf8(service).unwrap()),
        ]))
        .unwrap(),
    )
    .unwrap()
}

fn populated(events: &[EventEnvelopeV0]) -> AdmissionStateV0 {
    let mut state = state();
    for event in events {
        assert_eq!(state.apply_event_v1(event), Ok(ApplyOutcomeV0::Accepted));
    }
    state
}

#[test]
fn policy_and_request_boundaries_are_fail_closed() {
    assert_eq!(
        KafkaFieldEgressPolicyV0::new(
            1,
            vec![KafkaFieldEgressRuleV0::new("service", KafkaFieldEgressActionV0::Allow,).unwrap()],
        ),
        Err(KafkaGroupedRatioContextConfigErrorV0::UnsupportedPolicyVersion { actual: 1 })
    );
    assert_eq!(
        KafkaFieldEgressPolicyV0::new(
            0,
            vec![
                KafkaFieldEgressRuleV0::new("service", KafkaFieldEgressActionV0::Allow).unwrap(),
                KafkaFieldEgressRuleV0::new("service", KafkaFieldEgressActionV0::Deny).unwrap(),
            ],
        ),
        Err(KafkaGroupedRatioContextConfigErrorV0::DuplicatePolicyField)
    );
    assert_eq!(
        KafkaGroupedRatioContextRequestV0::new(
            0,
            schema(),
            "ratio",
            "service",
            "is_error",
            true,
            1,
            0,
            512,
            policy(KafkaFieldEgressActionV0::Deny).unwrap(),
        ),
        Err(KafkaGroupedRatioContextConfigErrorV0::FieldNotAllowed)
    );
    for budget in [511, 16 * 1_024 + 1] {
        assert!(matches!(
            KafkaGroupedRatioContextRequestV0::new(
                0,
                schema(),
                "ratio",
                "service",
                "is_error",
                true,
                1,
                0,
                budget,
                policy(KafkaFieldEgressActionV0::Allow).unwrap(),
            ),
            Err(KafkaGroupedRatioContextConfigErrorV0::OutputBudgetOutOfRange { .. })
        ));
    }
}

#[test]
fn payload_derived_ratios_are_ordered_provenanced_and_policy_safe() {
    let state = populated(&[
        event(0, 100, "payments", true),
        event(1, 101, "checkout", false),
        event(2, 102, "payments", false),
        event(3, 103, "payments", true),
    ]);
    let context = compile_kafka_grouped_ratio_context_v0(
        &state,
        &request(MAX_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0),
    )
    .unwrap();
    let text = std::str::from_utf8(context.as_bytes()).unwrap();

    assert_eq!(context.candidate_count(), 2);
    assert_eq!(context.included_count(), 2);
    assert_eq!(context.omitted_count(), 0);
    assert!(!context.query_closed());
    assert!(text.contains("\"start_event_time_ns\":0,\"end_exclusive_event_time_ns\":104"));
    let checkout = text.find("\"value\":\"checkout\"").unwrap();
    let payments = text.find("\"value\":\"payments\"").unwrap();
    assert!(checkout < payments);
    assert!(text.contains("\"value\":\"checkout\"},\"predicate\":{\"field\":\"is_error\",\"equals\":true},\"ratio\":{\"numerator\":0,\"denominator\":1,\"rate_ppm\":0,\"remainder\":0}"));
    assert!(text.contains("\"value\":\"payments\"},\"predicate\":{\"field\":\"is_error\",\"equals\":true},\"ratio\":{\"numerator\":2,\"denominator\":3,\"rate_ppm\":666666,\"remainder\":2}"));
    assert!(text.contains("\"denied_field_count\":1"));
    assert!(text.contains("decant://event/v0/kafka-grouped-ratio-source/kafka/0/0"));
    for forbidden in ["private-denied-sentinel", "private-kafka-key", "\"secret\""] {
        assert!(!text.contains(forbidden));
    }
}

#[test]
fn permutation_restore_and_budget_selection_are_byte_stable() {
    let events = [
        event(0, 100, "a", true),
        event(1, 101, "b", true),
        event(2, 102, "c", false),
    ];
    let left = populated(&events);
    let right = populated(&[events[2].clone(), events[0].clone(), events[1].clone()]);
    let restored =
        restore_admission_snapshot_v1(&encode_admission_snapshot_v1(&left).unwrap(), &config())
            .unwrap();
    let compile = |state: &AdmissionStateV0, budget| {
        compile_kafka_grouped_ratio_context_v0(state, &request(budget))
            .unwrap()
            .as_bytes()
            .to_vec()
    };
    let full = compile(&left, MAX_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0);
    assert_eq!(
        full,
        compile(&right, MAX_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0)
    );
    assert_eq!(
        full,
        compile(&restored, MAX_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0)
    );

    let bounded = compile_kafka_grouped_ratio_context_v0(
        &left,
        &request(MIN_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0),
    )
    .unwrap();
    assert!(bounded.as_bytes().len() <= MIN_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0);
    assert_eq!(bounded.candidate_count(), 3);
    assert_eq!(bounded.included_count() + bounded.omitted_count(), 3);
}

#[test]
fn empty_state_compiles_with_no_payload_or_fabricated_query() {
    let context = compile_kafka_grouped_ratio_context_v0(
        &state(),
        &request(MIN_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0),
    )
    .unwrap();
    let text = std::str::from_utf8(context.as_bytes()).unwrap();
    assert!(text.contains("\"available\":false"));
    assert!(text.contains("\"start_event_time_ns\":null"));
    assert!(text.contains("\"candidate_count\":0"));
    assert!(context.as_bytes().len() <= MIN_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0);
}
