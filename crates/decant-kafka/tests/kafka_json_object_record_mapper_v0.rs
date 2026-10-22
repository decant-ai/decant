use std::any::Any;

use decant_ir::{KafkaPositionV0, ScalarKindV0, SourceIdV0};
use decant_kafka::{
    KAFKA_JSON_OBJECT_MAPPING_VERSION_V0, KAFKA_SOURCE_CONTRACT_VERSION_V0, KafkaClusterIdV0,
    KafkaFetchedRecordV0, KafkaJsonEventTimeUnitV0, KafkaJsonObjectMappingV0,
    KafkaJsonObjectProjectionV0, KafkaJsonObjectRecordMapErrorV0, KafkaJsonObjectValueRoleV0,
    KafkaJsonScalarKindV0, KafkaRecordHeaderV0, KafkaRecordTimestampKindV0, KafkaRecordTimestampV0,
    KafkaSourceContractV0, KafkaSourceObservationErrorV0, KafkaTopicIdV0,
    MAX_KAFKA_JSON_OBJECT_MEMBERS_V0, map_json_object_kafka_record_v0,
};

fn mapping(unit: KafkaJsonEventTimeUnitV0) -> KafkaJsonObjectMappingV0 {
    KafkaJsonObjectMappingV0::new(
        KAFKA_JSON_OBJECT_MAPPING_VERSION_V0,
        SourceIdV0::new("private-source-namespace").unwrap(),
        "time",
        unit,
        vec![
            KafkaJsonObjectProjectionV0::new("a_null", "a_null", KafkaJsonScalarKindV0::Null)
                .unwrap(),
            KafkaJsonObjectProjectionV0::new("a_bool", "a_bool", KafkaJsonScalarKindV0::Bool)
                .unwrap(),
            KafkaJsonObjectProjectionV0::new("an_i64", "an_i64", KafkaJsonScalarKindV0::I64)
                .unwrap(),
            KafkaJsonObjectProjectionV0::new("a_u64", "a_u64", KafkaJsonScalarKindV0::U64).unwrap(),
            KafkaJsonObjectProjectionV0::new("text", "text", KafkaJsonScalarKindV0::Utf8).unwrap(),
        ],
    )
    .unwrap()
}

fn cluster() -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new("private-cluster-identity").unwrap()
}

fn topic() -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([0x51; 16]).unwrap()
}

fn contract(mapping: &KafkaJsonObjectMappingV0) -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(
        KAFKA_SOURCE_CONTRACT_VERSION_V0,
        mapping.effective_source_id().clone(),
        cluster(),
        topic(),
    )
    .unwrap()
}

fn record(value: Option<&[u8]>) -> KafkaFetchedRecordV0 {
    KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(2, 7).unwrap(),
        Some(b"private-kafka-key"),
        value,
        KafkaRecordTimestampV0::LogAppendTime { unix_ms: 999 },
        &[KafkaRecordHeaderV0::new("private-header", Some(b"private-header-value")).unwrap()],
    )
    .unwrap()
}

fn map(
    mapping: &KafkaJsonObjectMappingV0,
    record: &KafkaFetchedRecordV0,
) -> Result<decant_kafka::KafkaMappedEventV0, KafkaJsonObjectRecordMapErrorV0> {
    map_json_object_kafka_record_v0(
        mapping,
        &contract(mapping),
        mapping.effective_source_id(),
        &cluster(),
        topic(),
        record,
    )
}

#[test]
fn exact_object_maps_all_types_identity_key_provenance_and_omissions() {
    let mapping = mapping(KafkaJsonEventTimeUnitV0::Milliseconds);
    let record = record(Some(
        br#"{"ignored_private":"do-not-emit","text":"private-selected","a_u64":9,"time":1234,"an_i64":-7,"a_bool":true,"a_null":null}"#,
    ));
    let mapped = map(&mapping, &record).unwrap();
    let event = mapped.event();
    assert_eq!(event.source_id(), mapping.effective_source_id());
    assert_eq!(event.schema_id(), mapping.schema_id());
    assert_eq!(event.event_time_ns(), 1_234_000_000);
    assert_eq!(event.ingest_time_ns(), 0);
    assert_eq!(event.key(), Some(b"private-kafka-key".as_slice()));
    assert_eq!(event.sensitivity().bits(), u64::MAX);
    assert_eq!(
        event.source_position().kafka_position().unwrap(),
        KafkaPositionV0::new(2, 7).unwrap()
    );
    assert_eq!(
        event.provenance().as_str(),
        format!(
            "decant://event/v0/{}/kafka/2/7",
            mapping.effective_source_id().as_str()
        )
    );
    let kinds: Vec<_> = event
        .payload()
        .fields()
        .map(|(name, value)| (name, value.kind()))
        .collect();
    assert_eq!(
        kinds,
        vec![
            ("a_bool", ScalarKindV0::Bool),
            ("a_null", ScalarKindV0::Null),
            ("a_u64", ScalarKindV0::U64),
            ("an_i64", ScalarKindV0::I64),
            ("text", ScalarKindV0::Utf8),
        ]
    );
    let omission = mapped.omitted_transport_metadata();
    assert_eq!(omission.header_count(), 1);
    assert!(omission.header_bytes() > 0);
    assert_eq!(
        omission.timestamp_kind(),
        KafkaRecordTimestampKindV0::LogAppendTime
    );
}

#[test]
fn timestamp_units_are_checked_and_exact() {
    for (unit, input, expected) in [
        (KafkaJsonEventTimeUnitV0::Nanoseconds, 7, 7),
        (KafkaJsonEventTimeUnitV0::Microseconds, 7, 7_000),
        (KafkaJsonEventTimeUnitV0::Milliseconds, 7, 7_000_000),
        (KafkaJsonEventTimeUnitV0::Seconds, 7, 7_000_000_000),
    ] {
        let mapping = KafkaJsonObjectMappingV0::new(
            0,
            SourceIdV0::new("namespace").unwrap(),
            "time",
            unit,
            Vec::new(),
        )
        .unwrap();
        let value = format!("{{\"time\":{input}}}");
        assert_eq!(
            map(&mapping, &record(Some(value.as_bytes())))
                .unwrap()
                .event()
                .event_time_ns(),
            expected
        );
    }

    let seconds = KafkaJsonObjectMappingV0::new(
        0,
        SourceIdV0::new("namespace").unwrap(),
        "time",
        KafkaJsonEventTimeUnitV0::Seconds,
        Vec::new(),
    )
    .unwrap();
    let overflow = format!("{{\"time\":{}}}", i64::MAX);
    assert_eq!(
        map(&seconds, &record(Some(overflow.as_bytes()))),
        Err(KafkaJsonObjectRecordMapErrorV0::EventTimeOverflow)
    );
}

#[test]
fn source_validation_and_mapping_binding_precede_value_parsing() {
    let mapping = mapping(KafkaJsonEventTimeUnitV0::Nanoseconds);
    let wrong_contract = KafkaSourceContractV0::new(
        0,
        SourceIdV0::new("different-source").unwrap(),
        cluster(),
        topic(),
    )
    .unwrap();
    assert_eq!(
        map_json_object_kafka_record_v0(
            &mapping,
            &wrong_contract,
            wrong_contract.source_id(),
            &cluster(),
            topic(),
            &record(None),
        ),
        Err(KafkaJsonObjectRecordMapErrorV0::SourceObservation {
            category: KafkaSourceObservationErrorV0::SourceMismatch,
        })
    );

    assert_eq!(
        map_json_object_kafka_record_v0(
            &mapping,
            &contract(&mapping),
            mapping.effective_source_id(),
            &KafkaClusterIdV0::new("other-cluster").unwrap(),
            topic(),
            &record(Some(b"not-json")),
        ),
        Err(KafkaJsonObjectRecordMapErrorV0::SourceObservation {
            category: KafkaSourceObservationErrorV0::ClusterMismatch,
        })
    );
}

#[test]
fn structural_and_typed_failures_are_closed_and_content_free() {
    let mapping = mapping(KafkaJsonEventTimeUnitV0::Nanoseconds);
    let cases: Vec<(Option<&[u8]>, KafkaJsonObjectRecordMapErrorV0)> = vec![
        (None, KafkaJsonObjectRecordMapErrorV0::TombstoneUnsupported),
        (
            Some(b"[]"),
            KafkaJsonObjectRecordMapErrorV0::MalformedJsonObject,
        ),
        (
            Some(b"{"),
            KafkaJsonObjectRecordMapErrorV0::MalformedJsonObject,
        ),
        (
            Some(br#"{"time":1,"time":2}"#),
            KafkaJsonObjectRecordMapErrorV0::DuplicateObjectMember,
        ),
        (
            Some(br#"{"a_null":null}"#),
            KafkaJsonObjectRecordMapErrorV0::MissingValue {
                role: KafkaJsonObjectValueRoleV0::EventTime,
            },
        ),
        (
            Some(br#"{"time":1.5}"#),
            KafkaJsonObjectRecordMapErrorV0::InvalidValueType {
                role: KafkaJsonObjectValueRoleV0::EventTime,
                expected: None,
            },
        ),
        (
            Some(br#"{"time":1,"a_null":null,"a_bool":true,"an_i64":-1,"a_u64":2}"#),
            KafkaJsonObjectRecordMapErrorV0::MissingValue {
                role: KafkaJsonObjectValueRoleV0::Projection,
            },
        ),
        (
            Some(br#"{"time":1,"a_null":null,"a_bool":true,"an_i64":-1,"a_u64":2,"text":[]}"#),
            KafkaJsonObjectRecordMapErrorV0::InvalidValueType {
                role: KafkaJsonObjectValueRoleV0::Projection,
                expected: Some(KafkaJsonScalarKindV0::Utf8),
            },
        ),
    ];
    for (value, expected) in cases {
        let result = map(&mapping, &record(value));
        assert_eq!(result, Err(expected));
        assert_safe(&format!("{result:?}"));
    }

    let mut too_many = String::from("{");
    for index in 0..=MAX_KAFKA_JSON_OBJECT_MEMBERS_V0 {
        if index > 0 {
            too_many.push(',');
        }
        too_many.push_str(&format!("\"field_{index}\":null"));
    }
    too_many.push('}');
    assert_eq!(
        map(&mapping, &record(Some(too_many.as_bytes()))),
        Err(KafkaJsonObjectRecordMapErrorV0::TooManyObjectMembers {
            maximum: MAX_KAFKA_JSON_OBJECT_MEMBERS_V0,
        })
    );
}

#[test]
fn exact_record_and_member_bounds_are_accepted_before_one_past_refusal() {
    let mapping = KafkaJsonObjectMappingV0::new(
        0,
        SourceIdV0::new("namespace").unwrap(),
        "time",
        KafkaJsonEventTimeUnitV0::Nanoseconds,
        Vec::new(),
    )
    .unwrap();
    let prefix = "{\"time\":1,\"padding\":\"";
    let suffix = "\"}";
    let padding =
        "x".repeat(decant_kafka::MAX_KAFKA_RECORD_VALUE_BYTES_V0 - prefix.len() - suffix.len());
    let exact = format!("{prefix}{padding}{suffix}");
    assert_eq!(exact.len(), decant_kafka::MAX_KAFKA_RECORD_VALUE_BYTES_V0);
    assert!(map(&mapping, &record(Some(exact.as_bytes()))).is_ok());

    let mut exact_members = String::from("{\"time\":1");
    for index in 1..MAX_KAFKA_JSON_OBJECT_MEMBERS_V0 {
        exact_members.push_str(&format!(",\"field_{index}\":null"));
    }
    exact_members.push('}');
    assert!(map(&mapping, &record(Some(exact_members.as_bytes()))).is_ok());

    exact_members.insert_str(exact_members.len() - 1, ",\"one_past\":null");
    assert_eq!(
        map(&mapping, &record(Some(exact_members.as_bytes()))),
        Err(KafkaJsonObjectRecordMapErrorV0::TooManyObjectMembers {
            maximum: MAX_KAFKA_JSON_OBJECT_MEMBERS_V0,
        })
    );
}

#[test]
fn presentation_order_is_deterministic_and_diagnostics_redact_every_value() {
    let mapping = mapping(KafkaJsonEventTimeUnitV0::Nanoseconds);
    let left = record(Some(
        br#"{"time":7,"a_null":null,"a_bool":true,"an_i64":-7,"a_u64":9,"text":"private-selected","ignored_private":"do-not-emit"}"#,
    ));
    let right = record(Some(
        br#" { "ignored_private" : "do-not-emit", "text" : "private-selected", "a_u64" : 9, "an_i64" : -7, "a_bool" : true, "a_null" : null, "time" : 7 } "#,
    ));
    assert_eq!(map(&mapping, &left), map(&mapping, &right));
    let panic =
        std::panic::catch_unwind(|| assert_eq!(map(&mapping, &left), map(&mapping, &record(None))))
            .unwrap_err();
    assert_safe(&panic_text(panic));
}

fn assert_safe(text: &str) {
    for secret in [
        "private-source-namespace",
        "private-cluster-identity",
        "private-kafka-key",
        "private-header",
        "private-header-value",
        "ignored_private",
        "do-not-emit",
        "private-selected",
        "a_bool",
        "an_i64",
    ] {
        assert!(!text.contains(secret), "leaked {secret}");
    }
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
