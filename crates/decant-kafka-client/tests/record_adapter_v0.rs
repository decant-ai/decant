use decant_kafka::{
    KafkaRecordTimestampV0, KafkaRecordValidationErrorV0, MAX_KAFKA_RECORD_HEADER_KEY_BYTES_V0,
    MAX_KAFKA_RECORD_HEADER_VALUE_BYTES_V0, MAX_KAFKA_RECORD_HEADERS_V0,
    MAX_KAFKA_RECORD_KEY_BYTES_V0, MAX_KAFKA_RECORD_VALUE_BYTES_V0,
};
use decant_kafka_client::{KafkaRecordAdapterErrorV0, convert_kafka_message_v0};
use rdkafka::message::{Header, Message, OwnedHeaders, OwnedMessage, Timestamp};

fn message(
    payload: Option<Vec<u8>>,
    key: Option<Vec<u8>>,
    timestamp: Timestamp,
    partition: i32,
    offset: i64,
    headers: Option<OwnedHeaders>,
) -> OwnedMessage {
    OwnedMessage::new(
        payload,
        key,
        "decant-events".to_owned(),
        timestamp,
        partition,
        offset,
        headers,
    )
}

#[test]
fn exact_message_preserves_null_empty_timestamp_and_ordered_duplicate_headers() {
    let headers = OwnedHeaders::new_with_capacity(3)
        .insert(Header {
            key: "same",
            value: None::<&[u8]>,
        })
        .insert(Header {
            key: "same",
            value: Some(&b""[..]),
        })
        .insert(Header {
            key: "same",
            value: Some(&b"value"[..]),
        });
    let native = message(
        Some(Vec::new()),
        None,
        Timestamp::LogAppendTime(42),
        2,
        9,
        Some(headers),
    );
    let record =
        convert_kafka_message_v0(&native, "decant-events", |partition| partition == 2).unwrap();
    assert_eq!(record.position().partition(), 2);
    assert_eq!(record.position().offset(), 9);
    assert_eq!(record.key(), None);
    assert_eq!(record.value(), Some(&b""[..]));
    assert_eq!(
        record.timestamp(),
        KafkaRecordTimestampV0::LogAppendTime { unix_ms: 42 }
    );
    assert_eq!(record.headers().len(), 3);
    assert_eq!(record.headers()[0].value(), None);
    assert_eq!(record.headers()[1].value(), Some(&b""[..]));
    assert_eq!(record.headers()[2].value(), Some(&b"value"[..]));
}

#[test]
fn every_native_timestamp_kind_is_preserved() {
    let cases = [
        (
            Timestamp::NotAvailable,
            KafkaRecordTimestampV0::NotAvailable,
        ),
        (
            Timestamp::CreateTime(-7),
            KafkaRecordTimestampV0::CreateTime { unix_ms: -7 },
        ),
        (
            Timestamp::LogAppendTime(42),
            KafkaRecordTimestampV0::LogAppendTime { unix_ms: 42 },
        ),
    ];
    for (native_timestamp, expected_timestamp) in cases {
        let native = message(None, None, native_timestamp, 0, 0, None);
        let converted =
            convert_kafka_message_v0(&native, "decant-events", |partition| partition == 0).unwrap();
        assert_eq!(converted.timestamp(), expected_timestamp);
    }
}

#[test]
fn topic_partition_position_and_record_bounds_fail_closed() {
    let native = message(
        Some(vec![0; MAX_KAFKA_RECORD_VALUE_BYTES_V0 + 1]),
        Some(Vec::new()),
        Timestamp::NotAvailable,
        0,
        0,
        None,
    );
    assert_eq!(
        convert_kafka_message_v0(&native, "other-topic", |_| true),
        Err(KafkaRecordAdapterErrorV0::TopicMismatch)
    );
    assert_eq!(
        convert_kafka_message_v0(&native, "decant-events", |_| false),
        Err(KafkaRecordAdapterErrorV0::UnassignedPartition)
    );
    assert!(matches!(
        convert_kafka_message_v0(&native, "decant-events", |_| true),
        Err(KafkaRecordAdapterErrorV0::Record(
            KafkaRecordValidationErrorV0::BoundExceeded { .. }
        ))
    ));

    let invalid_position = message(None, None, Timestamp::NotAvailable, -1, 0, None);
    assert!(matches!(
        convert_kafka_message_v0(&invalid_position, "decant-events", |_| true),
        Err(KafkaRecordAdapterErrorV0::Position(_))
    ));
}

#[test]
fn header_count_is_refused_before_per_header_copy() {
    let mut headers = OwnedHeaders::new_with_capacity(MAX_KAFKA_RECORD_HEADERS_V0 + 1);
    for _ in 0..=MAX_KAFKA_RECORD_HEADERS_V0 {
        headers = headers.insert(Header {
            key: "header",
            value: Some(&b"private"[..]),
        });
    }
    let native = message(None, None, Timestamp::CreateTime(1), 0, 0, Some(headers));
    assert_eq!(
        convert_kafka_message_v0(&native, "decant-events", |_| true),
        Err(KafkaRecordAdapterErrorV0::TooManyHeaders {
            maximum: MAX_KAFKA_RECORD_HEADERS_V0,
            actual: MAX_KAFKA_RECORD_HEADERS_V0 + 1,
        })
    );
    let diagnostic = format!(
        "{:?}",
        convert_kafka_message_v0(&native, "decant-events", |_| true).unwrap_err()
    );
    assert!(!diagnostic.contains("private"));
    assert!(!diagnostic.contains("header"));
    assert!(!diagnostic.contains(native.topic()));
}

#[test]
fn native_key_and_header_bounds_are_delegated_without_value_leakage() {
    let large_key = message(
        None,
        Some(vec![b'k'; MAX_KAFKA_RECORD_KEY_BYTES_V0 + 1]),
        Timestamp::NotAvailable,
        0,
        0,
        None,
    );
    assert!(matches!(
        convert_kafka_message_v0(&large_key, "decant-events", |_| true),
        Err(KafkaRecordAdapterErrorV0::Record(
            KafkaRecordValidationErrorV0::BoundExceeded { .. }
        ))
    ));

    let large_header_key = "h".repeat(MAX_KAFKA_RECORD_HEADER_KEY_BYTES_V0 + 1);
    let headers = OwnedHeaders::new().insert(Header {
        key: &large_header_key,
        value: Some(&b"synthetic"[..]),
    });
    let native = message(None, None, Timestamp::NotAvailable, 0, 0, Some(headers));
    assert!(matches!(
        convert_kafka_message_v0(&native, "decant-events", |_| true),
        Err(KafkaRecordAdapterErrorV0::Header(
            KafkaRecordValidationErrorV0::BoundExceeded { .. }
        ))
    ));

    let large_header_value = vec![b'v'; MAX_KAFKA_RECORD_HEADER_VALUE_BYTES_V0 + 1];
    let headers = OwnedHeaders::new().insert(Header {
        key: "header",
        value: Some(&large_header_value),
    });
    let native = message(None, None, Timestamp::NotAvailable, 0, 0, Some(headers));
    let diagnostic = format!(
        "{:?}",
        convert_kafka_message_v0(&native, "decant-events", |_| true).unwrap_err()
    );
    assert!(!diagnostic.contains("synthetic"));
    assert!(!diagnostic.contains("header"));
}
