use decant_ir::{KafkaPositionV0, SourceIdV0, SourcePositionV0};
use decant_kafka::{
    KAFKA_DECANT_JSON_RECORD_MAPPER_VERSION_V0, KafkaClusterIdV0, KafkaDecantJsonRecordMapErrorV0,
    KafkaFetchedRecordV0, KafkaRecordHeaderV0, KafkaRecordTimestampKindV0, KafkaRecordTimestampV0,
    KafkaSourceContractV0, KafkaSourceObservationErrorV0, KafkaTopicIdV0,
    map_decant_json_kafka_record_v0,
};

fn contract() -> KafkaSourceContractV0 {
    KafkaSourceContractV0::new(
        0,
        SourceIdV0::new("kafka-source-v0").unwrap(),
        KafkaClusterIdV0::new("cluster-v0").unwrap(),
        KafkaTopicIdV0::from_bytes([9; 16]).unwrap(),
    )
    .unwrap()
}

fn value() -> &'static [u8] {
    br#"{"record_format":"decant-kafka-event","record_version":0,"schema_id":"payments.event.v0","event_time_ns":123,"payload":[{"name":"count","value":{"kind":"u64","value":7}}]}"#
}

fn observed() -> (SourceIdV0, KafkaClusterIdV0, KafkaTopicIdV0) {
    (
        SourceIdV0::new("kafka-source-v0").unwrap(),
        KafkaClusterIdV0::new("cluster-v0").unwrap(),
        KafkaTopicIdV0::from_bytes([9; 16]).unwrap(),
    )
}

#[test]
fn exact_record_maps_key_value_position_and_explicit_omissions() {
    assert_eq!(KAFKA_DECANT_JSON_RECORD_MAPPER_VERSION_V0, 0);
    let headers = [
        KafkaRecordHeaderV0::new("trace", Some(b"hidden")).unwrap(),
        KafkaRecordHeaderV0::new("trace", None).unwrap(),
    ];
    let record = KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(3, 44).unwrap(),
        Some(b"payment-7"),
        Some(value()),
        KafkaRecordTimestampV0::LogAppendTime { unix_ms: 999 },
        &headers,
    )
    .unwrap();
    let (source, cluster, topic) = observed();
    let mapped =
        map_decant_json_kafka_record_v0(&contract(), &source, &cluster, topic, &record).unwrap();

    let event = mapped.event();
    assert_eq!(event.source_id().as_str(), "kafka-source-v0");
    assert_eq!(event.schema_id().as_str(), "payments.event.v0");
    assert_eq!(
        event.source_position(),
        SourcePositionV0::kafka(3, 44).unwrap()
    );
    assert_eq!(event.key(), Some(&b"payment-7"[..]));
    assert_eq!(event.event_time_ns(), 123);
    assert_eq!(event.ingest_time_ns(), 0);
    assert_eq!(event.sensitivity().bits(), u64::MAX);
    assert_eq!(
        event.provenance().as_str(),
        "decant://event/v0/kafka-source-v0/kafka/3/44"
    );
    let omission = mapped.omitted_transport_metadata();
    assert_eq!(omission.header_count(), 2);
    assert_eq!(omission.header_bytes(), record.header_bytes());
    assert_eq!(
        omission.timestamp_kind(),
        KafkaRecordTimestampKindV0::LogAppendTime
    );
}

#[test]
fn source_observation_precedes_tombstone_and_malformed_value() {
    let bad_source = SourceIdV0::new("other-source-v0").unwrap();
    let (_, cluster, topic) = observed();
    for value in [None, Some(b"not json".as_slice())] {
        let record = KafkaFetchedRecordV0::new(
            KafkaPositionV0::new(0, 0).unwrap(),
            None,
            value,
            KafkaRecordTimestampV0::NotAvailable,
            &[],
        )
        .unwrap();
        assert_eq!(
            map_decant_json_kafka_record_v0(&contract(), &bad_source, &cluster, topic, &record,),
            Err(KafkaDecantJsonRecordMapErrorV0::SourceObservation {
                category: KafkaSourceObservationErrorV0::SourceMismatch,
            })
        );
    }
}

#[test]
fn tombstone_and_invalid_value_return_no_event() {
    let (source, cluster, topic) = observed();
    for value in [None, Some(b"not json".as_slice())] {
        let record = KafkaFetchedRecordV0::new(
            KafkaPositionV0::new(0, 0).unwrap(),
            None,
            value,
            KafkaRecordTimestampV0::NotAvailable,
            &[],
        )
        .unwrap();
        assert!(matches!(
            map_decant_json_kafka_record_v0(&contract(), &source, &cluster, topic, &record,),
            Err(KafkaDecantJsonRecordMapErrorV0::ValueDecode { .. })
        ));
    }
}

#[test]
fn absent_and_empty_keys_remain_distinct_and_mapping_is_repeatable() {
    let (source, cluster, topic) = observed();
    let absent = KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(0, 0).unwrap(),
        None,
        Some(value()),
        KafkaRecordTimestampV0::NotAvailable,
        &[],
    )
    .unwrap();
    let empty = KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(0, 0).unwrap(),
        Some(b""),
        Some(value()),
        KafkaRecordTimestampV0::NotAvailable,
        &[],
    )
    .unwrap();
    let first =
        map_decant_json_kafka_record_v0(&contract(), &source, &cluster, topic, &absent).unwrap();
    let again =
        map_decant_json_kafka_record_v0(&contract(), &source, &cluster, topic, &absent).unwrap();
    let second =
        map_decant_json_kafka_record_v0(&contract(), &source, &cluster, topic, &empty).unwrap();
    assert_eq!(first, again);
    assert_eq!(first.event().key(), None);
    assert_eq!(second.event().key(), Some(&b""[..]));
    assert_ne!(first, second);
}

#[test]
fn diagnostics_expose_only_categories_counts_and_kinds() {
    let first = KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(1, 11).unwrap(),
        Some(b"secret-a"),
        Some(value()),
        KafkaRecordTimestampV0::CreateTime { unix_ms: 111 },
        &[KafkaRecordHeaderV0::new("hidden-a", Some(b"value-a")).unwrap()],
    )
    .unwrap();
    let second = KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(2, 99).unwrap(),
        Some(b"secret-b"),
        Some(value()),
        KafkaRecordTimestampV0::CreateTime { unix_ms: 999 },
        &[KafkaRecordHeaderV0::new("hidden-b", Some(b"value-b")).unwrap()],
    )
    .unwrap();
    let (source, cluster, topic) = observed();
    let first =
        map_decant_json_kafka_record_v0(&contract(), &source, &cluster, topic, &first).unwrap();
    let second =
        map_decant_json_kafka_record_v0(&contract(), &source, &cluster, topic, &second).unwrap();
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
    for probe in [
        "secret-a", "secret-b", "hidden-a", "hidden-b", "value-a", "value-b", "111", "999",
    ] {
        assert!(!format!("{first:?}{second:?}").contains(probe));
    }
}
