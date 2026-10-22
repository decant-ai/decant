use std::any::Any;

use decant_ir::{KafkaPositionV0, SourceIdV0};
use decant_kafka::{
    KAFKA_JSON_OBJECT_MAPPING_VERSION_V0, KafkaClusterIdV0, KafkaFetchedRecordV0,
    KafkaJsonEntityKeySourceV1, KafkaJsonEventTimeUnitV0, KafkaJsonObjectMappingV0,
    KafkaJsonObjectMappingV1, KafkaJsonObjectProjectionV0, KafkaJsonObjectRecordMapErrorV1,
    KafkaJsonScalarKindV0, KafkaRecordTimestampV0, KafkaSourceContractV0, KafkaTopicIdV0,
    map_json_object_kafka_record_v1,
};

fn mapping(key: KafkaJsonEntityKeySourceV1) -> KafkaJsonObjectMappingV1 {
    let base = KafkaJsonObjectMappingV0::new(
        KAFKA_JSON_OBJECT_MAPPING_VERSION_V0,
        SourceIdV0::new("private-source-namespace").unwrap(),
        "time",
        KafkaJsonEventTimeUnitV0::Nanoseconds,
        vec![
            KafkaJsonObjectProjectionV0::new("raw_service", "service", KafkaJsonScalarKindV0::Utf8)
                .unwrap(),
            KafkaJsonObjectProjectionV0::new("raw_value", "value", KafkaJsonScalarKindV0::U64)
                .unwrap(),
        ],
    )
    .unwrap();
    KafkaJsonObjectMappingV1::new(1, base, key).unwrap()
}

fn cluster() -> KafkaClusterIdV0 {
    KafkaClusterIdV0::new("private-cluster").unwrap()
}

fn topic() -> KafkaTopicIdV0 {
    KafkaTopicIdV0::from_bytes([0x61; 16]).unwrap()
}

fn record(key: Option<&[u8]>, service: &str) -> KafkaFetchedRecordV0 {
    let value = format!(r#"{{"time":7,"raw_service":"{service}","raw_value":9}}"#);
    KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(3, 11).unwrap(),
        key,
        Some(value.as_bytes()),
        KafkaRecordTimestampV0::NotAvailable,
        &[],
    )
    .unwrap()
}

fn map(
    mapping: &KafkaJsonObjectMappingV1,
    record: &KafkaFetchedRecordV0,
) -> Result<decant_kafka::KafkaMappedEventV0, KafkaJsonObjectRecordMapErrorV1> {
    let contract =
        KafkaSourceContractV0::new(0, mapping.effective_source_id().clone(), cluster(), topic())
            .unwrap();
    map_json_object_kafka_record_v1(
        mapping,
        &contract,
        mapping.effective_source_id(),
        &cluster(),
        topic(),
        record,
    )
}

#[test]
fn kafka_key_mode_preserves_absent_empty_utf8_and_binary_keys() {
    let mapping = mapping(KafkaJsonEntityKeySourceV1::kafka_record_key());
    for (input, expected) in [
        (None, None),
        (Some(&b""[..]), Some(&b""[..])),
        (Some(&b"service-a"[..]), Some(&b"service-a"[..])),
        (Some(&[0, 0xff, 7][..]), Some(&[0, 0xff, 7][..])),
    ] {
        let record = record(input, "payload-service");
        assert_eq!(map(&mapping, &record).unwrap().event().key(), expected);
    }
}

#[test]
fn projection_mode_uses_exact_utf8_and_ignores_kafka_key() {
    let mapping = mapping(KafkaJsonEntityKeySourceV1::utf8_projection("service").unwrap());
    let record = record(Some(b"private-broker-key"), "payload-service");
    let event = map(&mapping, &record).unwrap();
    assert_eq!(event.event().key(), Some(b"payload-service".as_slice()));
    assert_eq!(event.event().schema_id(), mapping.schema_id());
    assert_eq!(event.event().source_id(), mapping.effective_source_id());
}

#[test]
fn projected_key_bound_is_exact_and_failure_is_value_safe() {
    let mapping = mapping(KafkaJsonEntityKeySourceV1::utf8_projection("service").unwrap());
    for length in [0, decant_ir::MAX_KEY_BYTES] {
        let value = "k".repeat(length);
        assert_eq!(
            map(&mapping, &record(None, &value)).unwrap().event().key(),
            Some(value.as_bytes())
        );
    }

    let private = "s".repeat(decant_ir::MAX_KEY_BYTES + 1);
    let result = map(
        &mapping,
        &record(Some(b"fallback-must-not-apply"), &private),
    );
    assert!(matches!(
        result,
        Err(KafkaJsonObjectRecordMapErrorV1::CanonicalAssembly { .. })
    ));
    let debug = format!("{result:?}");
    assert!(!debug.contains(&private));
    assert!(!debug.contains("fallback-must-not-apply"));
}

#[test]
fn malformed_or_wrong_kind_input_is_atomic_and_diagnostics_are_redacted() {
    let mapping = mapping(KafkaJsonEntityKeySourceV1::utf8_projection("service").unwrap());
    let malformed = KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(3, 12).unwrap(),
        Some(b"private-key"),
        Some(br#"{"time":7,"raw_service":9,"raw_value":9}"#),
        KafkaRecordTimestampV0::NotAvailable,
        &[],
    )
    .unwrap();
    let result = map(&mapping, &malformed);
    assert!(matches!(
        result,
        Err(KafkaJsonObjectRecordMapErrorV1::ObjectMapping { .. })
    ));
    let panic = std::panic::catch_unwind(|| assert!(result.is_ok())).unwrap_err();
    let text = panic_text(panic);
    assert!(!text.contains("private-key"));
    assert!(!text.contains("raw_service"));
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
