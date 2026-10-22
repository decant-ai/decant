use std::any::Any;
use std::collections::BTreeMap;

use decant_compiler::{
    KAFKA_FIELD_EGRESS_POLICY_VERSION_V0, KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V0,
    KafkaEntityKeyEgressActionV0, KafkaFieldEgressActionV0, KafkaFieldEgressPolicyV0,
    KafkaFieldEgressRuleV0, KafkaMaterialChangeConfigErrorV0, KafkaMaterialChangeRequestV0,
    MAX_KAFKA_MATERIAL_CHANGE_ITEMS_V0, MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0,
    MIN_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0, compile_kafka_material_change_context_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigV0,
};

fn source() -> SourceIdV0 {
    SourceIdV0::new("private-kafka-material-source").unwrap()
}

fn schema() -> SchemaIdV0 {
    SchemaIdV0::new("private-kafka-material-schema").unwrap()
}

fn admission_config() -> AdmissionConfigV0 {
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

fn retention() -> EventTimeRetentionConfigV0 {
    EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, 1_000_000_000).unwrap()
}

fn policy(value_action: KafkaFieldEgressActionV0) -> KafkaFieldEgressPolicyV0 {
    KafkaFieldEgressPolicyV0::new(
        KAFKA_FIELD_EGRESS_POLICY_VERSION_V0,
        vec![
            KafkaFieldEgressRuleV0::new("series", KafkaFieldEgressActionV0::Allow).unwrap(),
            KafkaFieldEgressRuleV0::new("value", value_action).unwrap(),
            KafkaFieldEgressRuleV0::new("secret", KafkaFieldEgressActionV0::Deny).unwrap(),
        ],
    )
    .unwrap()
}

fn request(
    item_limit: usize,
    byte_budget: usize,
    key_action: KafkaEntityKeyEgressActionV0,
) -> KafkaMaterialChangeRequestV0 {
    KafkaMaterialChangeRequestV0::new(
        KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V0,
        "orders-status",
        schema(),
        retention(),
        "series",
        "value",
        item_limit,
        byte_budget,
        policy(KafkaFieldEgressActionV0::Allow),
        key_action,
    )
    .unwrap()
}

fn event(
    offset: i64,
    time: i64,
    key: &[u8],
    series: &str,
    value: ScalarValueV0,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(),
        schema(),
        SourcePositionV0::kafka(0, offset).unwrap(),
        Some(key),
        time,
        0,
        SensitivityBitmapV0::from_bits(u64::MAX),
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([
            ("series".to_owned(), ScalarValueV0::utf8(series).unwrap()),
            ("value".to_owned(), value),
            (
                "secret".to_owned(),
                ScalarValueV0::utf8("private-denied-sentinel").unwrap(),
            ),
        ]))
        .unwrap(),
    )
    .unwrap()
}

fn populated(events: &[EventEnvelopeV0]) -> AdmissionStateV0 {
    let mut state = AdmissionStateV0::new(admission_config());
    for event in events {
        assert_eq!(state.apply_event_v1(event), Ok(ApplyOutcomeV0::Accepted));
    }
    state
}

#[test]
fn request_contract_is_bounded_policy_gated_and_value_safe() {
    let denied = KafkaMaterialChangeRequestV0::new(
        0,
        "private-view",
        schema(),
        retention(),
        "series",
        "value",
        1,
        512,
        policy(KafkaFieldEgressActionV0::Deny),
        KafkaEntityKeyEgressActionV0::Deny,
    );
    assert_eq!(
        denied,
        Err(KafkaMaterialChangeConfigErrorV0::FieldNotAllowed)
    );
    for (items, expected) in [
        (0, 0),
        (
            MAX_KAFKA_MATERIAL_CHANGE_ITEMS_V0 + 1,
            MAX_KAFKA_MATERIAL_CHANGE_ITEMS_V0 + 1,
        ),
    ] {
        assert_eq!(
            KafkaMaterialChangeRequestV0::new(
                0,
                "view",
                schema(),
                retention(),
                "series",
                "value",
                items,
                512,
                policy(KafkaFieldEgressActionV0::Allow),
                KafkaEntityKeyEgressActionV0::Deny,
            ),
            Err(KafkaMaterialChangeConfigErrorV0::ItemLimitOutOfRange {
                maximum: MAX_KAFKA_MATERIAL_CHANGE_ITEMS_V0,
                actual: expected,
            })
        );
    }
    for budget in [
        MIN_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0 - 1,
        MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0 + 1,
    ] {
        assert!(matches!(
            KafkaMaterialChangeRequestV0::new(
                0,
                "view",
                schema(),
                retention(),
                "series",
                "value",
                1,
                budget,
                policy(KafkaFieldEgressActionV0::Allow),
                KafkaEntityKeyEgressActionV0::Deny,
            ),
            Err(KafkaMaterialChangeConfigErrorV0::OutputBudgetOutOfRange { .. })
        ));
    }
    let private = "private\nview";
    let result = KafkaMaterialChangeRequestV0::new(
        0,
        private,
        schema(),
        retention(),
        "series",
        "value",
        1,
        512,
        policy(KafkaFieldEgressActionV0::Allow),
        KafkaEntityKeyEgressActionV0::Deny,
    );
    assert!(!format!("{result:?}").contains(private));
}

#[test]
fn exact_changes_are_canonical_typed_provenanced_and_not_overclaimed() {
    let events = vec![
        event(0, 100, b"b", "bool", ScalarValueV0::from_bool(false)),
        event(1, 101, b"b", "bool", ScalarValueV0::from_bool(true)),
        event(2, 102, b"i", "i64", ScalarValueV0::from_i64(-1)),
        event(3, 103, b"i", "i64", ScalarValueV0::from_i64(2)),
        event(4, 104, b"u", "u64", ScalarValueV0::from_u64(1)),
        event(5, 105, b"u", "u64", ScalarValueV0::from_u64(2)),
        event(6, 106, b"s", "utf8", ScalarValueV0::utf8("old").unwrap()),
        event(7, 107, b"s", "utf8", ScalarValueV0::utf8("new").unwrap()),
        event(
            8,
            108,
            b"x",
            "bytes",
            ScalarValueV0::bytes(&[0, 1]).unwrap(),
        ),
        event(
            9,
            109,
            b"x",
            "bytes",
            ScalarValueV0::bytes(&[2, 0xff]).unwrap(),
        ),
        event(10, 110, b"z", "null", ScalarValueV0::null()),
        event(11, 111, b"z", "null", ScalarValueV0::null()),
    ];
    let mut state = populated(&events);
    assert_eq!(
        state.apply_event_v1(&events[11]),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    let context = compile_kafka_material_change_context_v0(
        &state,
        &request(
            MAX_KAFKA_MATERIAL_CHANGE_ITEMS_V0,
            MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0,
            KafkaEntityKeyEgressActionV0::AllowHex,
        ),
    )
    .unwrap();
    let text = std::str::from_utf8(context.as_bytes()).unwrap();

    assert_eq!(context.candidate_count(), 5);
    assert_eq!(context.included_count(), 5);
    assert_eq!(context.omitted_count(), 0);
    assert!(!context.source_finalized());
    assert!(text.contains("\"definition\":\"latest_distinct_retained_scalar_transition\""));
    assert!(text.contains("\"entity_key\":{\"encoding\":\"lower_hex\",\"value\":\"62\"}"));
    assert!(text.contains("\"kind\":\"bool\",\"value\":false"));
    assert!(text.contains("\"kind\":\"i64\",\"value\":-1"));
    assert!(text.contains("\"kind\":\"u64\",\"value\":2"));
    assert!(text.contains("\"kind\":\"utf8\",\"value\":\"new\""));
    assert!(text.contains("\"kind\":\"bytes\",\"value\":\"02ff\""));
    assert!(text.contains("decant://event/v0/private-kafka-material-source/kafka/0/0"));
    for forbidden in [
        "private-denied-sentinel",
        "\"secret\"",
        "anomaly",
        "caused_by",
        "source_complete\":true",
    ] {
        assert!(!text.contains(forbidden));
    }
}

#[test]
fn permutation_key_denial_and_budget_prefix_are_deterministic() {
    let events = [
        event(0, 100, b"private-key-a", "a", ScalarValueV0::from_u64(1)),
        event(1, 101, b"private-key-a", "a", ScalarValueV0::from_u64(2)),
        event(2, 102, b"private-key-b", "b", ScalarValueV0::from_u64(1)),
        event(3, 103, b"private-key-b", "b", ScalarValueV0::from_u64(2)),
    ];
    let left = populated(&events);
    let right = populated(&[
        events[3].clone(),
        events[1].clone(),
        events[2].clone(),
        events[0].clone(),
    ]);
    let compile = |state: &AdmissionStateV0, budget| {
        compile_kafka_material_change_context_v0(
            state,
            &request(2, budget, KafkaEntityKeyEgressActionV0::Deny),
        )
        .unwrap()
    };
    let full = compile(&left, MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0);
    assert_eq!(
        full.as_bytes(),
        compile(&right, MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0).as_bytes()
    );
    let text = std::str::from_utf8(full.as_bytes()).unwrap();
    assert!(text.contains("\"entity_key_action\":\"deny\""));
    assert!(text.contains("\"entity_key\":null"));
    assert!(!text.contains("private-key"));

    let bounded = compile(&left, MIN_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0);
    assert!(bounded.as_bytes().len() <= MIN_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0);
    assert_eq!(bounded.candidate_count(), 2);
    assert_eq!(bounded.included_count() + bounded.omitted_count(), 2);
}

#[test]
fn wrong_source_kind_fails_without_value_disclosure() {
    let synthetic_config = AdmissionConfigV0::new_with_source_position_kind(
        0,
        SourceIdV0::new("private-wrong-source").unwrap(),
        SourcePositionKindV0::Synthetic,
        0,
        100,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap();
    let result = compile_kafka_material_change_context_v0(
        &AdmissionStateV0::new(synthetic_config),
        &request(1, 512, KafkaEntityKeyEgressActionV0::Deny),
    );
    let panic = std::panic::catch_unwind(|| assert!(result.is_ok())).unwrap_err();
    let text = panic_text(panic);
    assert!(!text.contains("private-wrong-source"));
    assert!(!text.contains("orders-status"));
}

fn panic_text(payload: Box<dyn Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        String::new()
    }
}
