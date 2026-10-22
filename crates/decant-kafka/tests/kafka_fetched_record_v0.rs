use decant_ir::KafkaPositionV0;
use decant_kafka::{
    KAFKA_FETCHED_RECORD_VERSION_V0, KafkaFetchedRecordV0, KafkaRecordBoundV0, KafkaRecordHeaderV0,
    KafkaRecordTimestampKindV0, KafkaRecordTimestampV0, KafkaRecordValidationErrorV0,
    MAX_KAFKA_FETCHED_RECORD_BYTES_V0, MAX_KAFKA_RECORD_HEADER_BYTES_V0,
    MAX_KAFKA_RECORD_HEADER_KEY_BYTES_V0, MAX_KAFKA_RECORD_HEADER_VALUE_BYTES_V0,
    MAX_KAFKA_RECORD_HEADERS_V0, MAX_KAFKA_RECORD_KEY_BYTES_V0, MAX_KAFKA_RECORD_VALUE_BYTES_V0,
};

fn position() -> KafkaPositionV0 {
    KafkaPositionV0::new(7, 19).unwrap()
}

#[test]
fn constants_and_exact_record_fields_are_stable() {
    assert_eq!(KAFKA_FETCHED_RECORD_VERSION_V0, 0);
    assert_eq!(MAX_KAFKA_RECORD_KEY_BYTES_V0, 256);
    assert_eq!(MAX_KAFKA_RECORD_VALUE_BYTES_V0, 65_536);
    assert_eq!(MAX_KAFKA_RECORD_HEADERS_V0, 64);
    assert_eq!(MAX_KAFKA_RECORD_HEADER_KEY_BYTES_V0, 128);
    assert_eq!(MAX_KAFKA_RECORD_HEADER_VALUE_BYTES_V0, 4_096);
    assert_eq!(MAX_KAFKA_RECORD_HEADER_BYTES_V0, 65_536);
    assert_eq!(MAX_KAFKA_FETCHED_RECORD_BYTES_V0, 131_072);

    let headers = [
        KafkaRecordHeaderV0::new("trace", Some(b"one")).unwrap(),
        KafkaRecordHeaderV0::new("trace", None).unwrap(),
    ];
    let record = KafkaFetchedRecordV0::new(
        position(),
        Some(b"key"),
        Some(b"value"),
        KafkaRecordTimestampV0::CreateTime { unix_ms: 42 },
        &headers,
    )
    .unwrap();

    assert_eq!(record.position(), position());
    assert_eq!(record.key(), Some(&b"key"[..]));
    assert_eq!(record.value(), Some(&b"value"[..]));
    assert!(!record.is_tombstone());
    assert_eq!(record.timestamp().unix_ms(), Some(42));
    assert_eq!(record.headers(), headers);
    assert_eq!(record.headers()[0].key(), "trace");
    assert_eq!(record.headers()[0].value(), Some(&b"one"[..]));
    assert_eq!(record.headers()[1].key(), "trace");
    assert_eq!(record.headers()[1].value(), None);
    assert_eq!(record.header_bytes(), 27);
    assert_eq!(record.logical_bytes(), 70);
}

#[test]
fn null_empty_and_timestamp_states_remain_distinct() {
    let nulls = KafkaFetchedRecordV0::new(
        position(),
        None,
        None,
        KafkaRecordTimestampV0::NotAvailable,
        &[],
    )
    .unwrap();
    let empty = KafkaFetchedRecordV0::new(
        position(),
        Some(b""),
        Some(b""),
        KafkaRecordTimestampV0::LogAppendTime { unix_ms: -9 },
        &[],
    )
    .unwrap();

    assert_eq!(nulls.key(), None);
    assert!(nulls.is_tombstone());
    assert_eq!(
        nulls.timestamp().kind(),
        KafkaRecordTimestampKindV0::NotAvailable
    );
    assert_eq!(empty.key(), Some(&b""[..]));
    assert_eq!(empty.value(), Some(&b""[..]));
    assert!(!empty.is_tombstone());
    assert_eq!(
        empty.timestamp().kind(),
        KafkaRecordTimestampKindV0::LogAppendTime
    );
    assert_eq!(empty.timestamp().unix_ms(), Some(-9));
    assert_ne!(nulls, empty);
}

#[test]
fn every_individual_bound_accepts_exact_and_rejects_one_past() {
    let exact_key = vec![b'k'; MAX_KAFKA_RECORD_KEY_BYTES_V0];
    let exact_value = vec![b'v'; MAX_KAFKA_RECORD_VALUE_BYTES_V0];
    KafkaFetchedRecordV0::new(
        position(),
        Some(&exact_key),
        Some(&exact_value),
        KafkaRecordTimestampV0::NotAvailable,
        &[],
    )
    .unwrap();

    let too_long_key = vec![b'k'; MAX_KAFKA_RECORD_KEY_BYTES_V0 + 1];
    assert!(matches!(
        KafkaFetchedRecordV0::new(
            position(),
            Some(&too_long_key),
            Some(b"v"),
            KafkaRecordTimestampV0::NotAvailable,
            &[],
        ),
        Err(KafkaRecordValidationErrorV0::BoundExceeded { actual, .. })
            if actual == MAX_KAFKA_RECORD_KEY_BYTES_V0 + 1
    ));
    let too_long_value = vec![b'v'; MAX_KAFKA_RECORD_VALUE_BYTES_V0 + 1];
    assert!(
        KafkaFetchedRecordV0::new(
            position(),
            None,
            Some(&too_long_value),
            KafkaRecordTimestampV0::NotAvailable,
            &[],
        )
        .is_err()
    );

    KafkaRecordHeaderV0::new(
        &"h".repeat(MAX_KAFKA_RECORD_HEADER_KEY_BYTES_V0),
        Some(&vec![b'x'; MAX_KAFKA_RECORD_HEADER_VALUE_BYTES_V0]),
    )
    .unwrap();
    assert!(KafkaRecordHeaderV0::new("", None).is_err());
    assert!(
        KafkaRecordHeaderV0::new(&"h".repeat(MAX_KAFKA_RECORD_HEADER_KEY_BYTES_V0 + 1), None)
            .is_err()
    );
    assert!(
        KafkaRecordHeaderV0::new(
            "h",
            Some(&vec![b'x'; MAX_KAFKA_RECORD_HEADER_VALUE_BYTES_V0 + 1])
        )
        .is_err()
    );

    let headers: Vec<_> = (0..MAX_KAFKA_RECORD_HEADERS_V0)
        .map(|_| KafkaRecordHeaderV0::new("h", None).unwrap())
        .collect();
    KafkaFetchedRecordV0::new(
        position(),
        None,
        Some(b"v"),
        KafkaRecordTimestampV0::NotAvailable,
        &headers,
    )
    .unwrap();
    let mut too_many = headers;
    too_many.push(KafkaRecordHeaderV0::new("h", None).unwrap());
    assert!(
        KafkaFetchedRecordV0::new(
            position(),
            None,
            Some(b"v"),
            KafkaRecordTimestampV0::NotAvailable,
            &too_many,
        )
        .is_err()
    );
}

#[test]
fn aggregate_header_limit_is_enforced_without_partial_record() {
    let large = KafkaRecordHeaderV0::new(
        &"h".repeat(MAX_KAFKA_RECORD_HEADER_KEY_BYTES_V0),
        Some(&vec![b'x'; MAX_KAFKA_RECORD_HEADER_VALUE_BYTES_V0]),
    )
    .unwrap();
    let mut exact: Vec<_> = (0..15).map(|_| large.clone()).collect();
    exact.push(KafkaRecordHeaderV0::new("h", Some(&vec![b'x'; 2_031])).unwrap());
    let accepted = KafkaFetchedRecordV0::new(
        position(),
        None,
        Some(&vec![b'v'; 65_513]),
        KafkaRecordTimestampV0::NotAvailable,
        &exact,
    )
    .unwrap();
    assert_eq!(accepted.header_bytes(), MAX_KAFKA_RECORD_HEADER_BYTES_V0);
    assert_eq!(accepted.logical_bytes(), MAX_KAFKA_FETCHED_RECORD_BYTES_V0);
    assert!(matches!(
        KafkaFetchedRecordV0::new(
            position(),
            None,
            Some(&vec![b'v'; 65_514]),
            KafkaRecordTimestampV0::NotAvailable,
            &exact,
        ),
        Err(KafkaRecordValidationErrorV0::BoundExceeded {
            bound: KafkaRecordBoundV0::RecordBytes,
            maximum: MAX_KAFKA_FETCHED_RECORD_BYTES_V0,
            actual,
        }) if actual == MAX_KAFKA_FETCHED_RECORD_BYTES_V0 + 1
    ));

    let mut over = exact;
    over.pop();
    over.push(KafkaRecordHeaderV0::new("h", Some(&vec![b'x'; 2_032])).unwrap());
    assert!(matches!(
        KafkaFetchedRecordV0::new(
            position(),
            None,
            Some(&vec![b'v'; 65_513]),
            KafkaRecordTimestampV0::NotAvailable,
            &over,
        ),
        Err(KafkaRecordValidationErrorV0::BoundExceeded { maximum, actual, .. })
            if maximum == MAX_KAFKA_RECORD_HEADER_BYTES_V0 && actual > maximum
    ));
}

#[test]
fn diagnostics_depend_on_shape_not_record_content() {
    let first_header = KafkaRecordHeaderV0::new("secret-a", Some(b"value-a")).unwrap();
    let second_header = KafkaRecordHeaderV0::new("secret-b", Some(b"value-b")).unwrap();
    assert_eq!(format!("{first_header:?}"), format!("{second_header:?}"));

    let first = KafkaFetchedRecordV0::new(
        position(),
        Some(b"secret-a"),
        Some(b"payload-a"),
        KafkaRecordTimestampV0::CreateTime { unix_ms: 11 },
        &[first_header],
    )
    .unwrap();
    let second = KafkaFetchedRecordV0::new(
        KafkaPositionV0::new(8, 20).unwrap(),
        Some(b"secret-b"),
        Some(b"payload-b"),
        KafkaRecordTimestampV0::CreateTime { unix_ms: 99 },
        &[second_header],
    )
    .unwrap();
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
    for probe in ["secret-a", "secret-b", "payload-a", "payload-b", "11", "99"] {
        assert!(!format!("{first:?}{second:?}").contains(probe));
    }
}
