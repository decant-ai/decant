use decant_ir::ScalarKindV0;
use decant_kafka::{
    DECANT_KAFKA_EVENT_FORMAT_V0, DECANT_KAFKA_EVENT_VALUE_VERSION_V0,
    KafkaEventValueDecodeErrorV0, MAX_KAFKA_JSON_VALUE_BYTES_V0,
    decode_decant_kafka_event_value_v0,
};

const ALL_SCALARS: &[u8] = br#"{
  "record_format": "decant-kafka-event",
  "record_version": 0,
  "schema_id": "payments.event.v0",
  "event_time_ns": 123,
  "payload": [
    {"name":"z_bytes","value":{"kind":"bytes","value":"00ff"}},
    {"name":"a_null","value":{"kind":"null"}},
    {"name":"e_utf8","value":{"kind":"utf8","value":"hello"}},
    {"name":"b_bool","value":{"kind":"bool","value":true}},
    {"name":"d_u64","value":{"kind":"u64","value":9}},
    {"name":"c_i64","value":{"kind":"i64","value":-7}}
  ]
}"#;

#[test]
fn version_format_and_all_scalar_kinds_decode_to_canonical_order() {
    assert_eq!(DECANT_KAFKA_EVENT_FORMAT_V0, "decant-kafka-event");
    assert_eq!(DECANT_KAFKA_EVENT_VALUE_VERSION_V0, 0);
    assert_eq!(MAX_KAFKA_JSON_VALUE_BYTES_V0, 65_536);

    let decoded = decode_decant_kafka_event_value_v0(Some(ALL_SCALARS)).unwrap();
    assert_eq!(decoded.schema_id().as_str(), "payments.event.v0");
    assert_eq!(decoded.event_time_ns(), 123);
    let fields: Vec<_> = decoded
        .payload()
        .fields()
        .map(|(name, value)| (name, value.kind()))
        .collect();
    assert_eq!(
        fields,
        vec![
            ("a_null", ScalarKindV0::Null),
            ("b_bool", ScalarKindV0::Bool),
            ("c_i64", ScalarKindV0::I64),
            ("d_u64", ScalarKindV0::U64),
            ("e_utf8", ScalarKindV0::Utf8),
            ("z_bytes", ScalarKindV0::Bytes),
        ]
    );
    assert_eq!(
        decoded
            .payload()
            .fields()
            .find(|(name, _)| *name == "z_bytes")
            .unwrap()
            .1
            .as_bytes(),
        Some(&[0, 255][..])
    );
}

#[test]
fn whitespace_and_member_order_do_not_change_decoded_semantics() {
    let compact = br#"{"payload":[{"value":{"value":1,"kind":"u64"},"name":"count"}],"event_time_ns":5,"schema_id":"schema.v0","record_version":0,"record_format":"decant-kafka-event"}"#;
    let expanded = br#"{
      "record_format":"decant-kafka-event",
      "record_version":0,
      "schema_id":"schema.v0",
      "event_time_ns":5,
      "payload":[{"name":"count","value":{"kind":"u64","value":1}}]
    }"#;
    assert_eq!(
        decode_decant_kafka_event_value_v0(Some(compact)).unwrap(),
        decode_decant_kafka_event_value_v0(Some(expanded)).unwrap()
    );
}

#[test]
fn tombstones_size_and_structural_errors_fail_closed() {
    assert_eq!(
        decode_decant_kafka_event_value_v0(None),
        Err(KafkaEventValueDecodeErrorV0::TombstoneUnsupported)
    );
    let oversized = vec![b'x'; MAX_KAFKA_JSON_VALUE_BYTES_V0 + 1];
    assert!(matches!(
        decode_decant_kafka_event_value_v0(Some(&oversized)),
        Err(KafkaEventValueDecodeErrorV0::InputTooLarge { actual, .. })
            if actual == MAX_KAFKA_JSON_VALUE_BYTES_V0 + 1
    ));

    let base = br#"{"record_format":"decant-kafka-event","record_version":0,"schema_id":"s","event_time_ns":1,"payload":[]}"#;
    let mut exact = base.to_vec();
    exact.resize(MAX_KAFKA_JSON_VALUE_BYTES_V0, b' ');
    assert!(decode_decant_kafka_event_value_v0(Some(&exact)).is_ok());

    let unsupported_format = br#"{"record_format":"other","record_version":0,"schema_id":"s","event_time_ns":1,"payload":[]}"#;
    assert_eq!(
        decode_decant_kafka_event_value_v0(Some(unsupported_format)),
        Err(KafkaEventValueDecodeErrorV0::UnsupportedFormat)
    );
    let unsupported_version = br#"{"record_format":"decant-kafka-event","record_version":1,"schema_id":"s","event_time_ns":1,"payload":[]}"#;
    assert_eq!(
        decode_decant_kafka_event_value_v0(Some(unsupported_version)),
        Err(KafkaEventValueDecodeErrorV0::UnsupportedVersion { actual: 1 })
    );
    let missing_time = br#"{"record_format":"decant-kafka-event","record_version":0,"schema_id":"s","payload":[]}"#;
    assert_eq!(
        decode_decant_kafka_event_value_v0(Some(missing_time)),
        Err(KafkaEventValueDecodeErrorV0::MissingEventTime)
    );

    for invalid in [
        br#"{}"#.as_slice(),
        br#"{"record_format":"decant-kafka-event","record_version":0,"schema_id":"s","event_time_ns":1,"payload":[],"extra":1}"#,
        br#"{"record_format":"decant-kafka-event","record_version":0,"schema_id":"s","event_time_ns":1,"payload":[{"name":"x","value":{"kind":"u64","value":1.5}}]}"#,
    ] {
        assert!(decode_decant_kafka_event_value_v0(Some(invalid)).is_err());
    }
}

#[test]
fn duplicate_names_and_non_lowercase_hex_are_typed() {
    let duplicate = br#"{"record_format":"decant-kafka-event","record_version":0,"schema_id":"s","event_time_ns":1,"payload":[{"name":"x","value":{"kind":"null"}},{"name":"x","value":{"kind":"null"}}]}"#;
    assert_eq!(
        decode_decant_kafka_event_value_v0(Some(duplicate)),
        Err(KafkaEventValueDecodeErrorV0::DuplicatePayloadField { field_count: 2 })
    );
    for value in ["0", "0A", "zz"] {
        let input = format!(
            "{{\"record_format\":\"decant-kafka-event\",\"record_version\":0,\"schema_id\":\"s\",\"event_time_ns\":1,\"payload\":[{{\"name\":\"x\",\"value\":{{\"kind\":\"bytes\",\"value\":\"{value}\"}}}}]}}"
        );
        assert!(matches!(
            decode_decant_kafka_event_value_v0(Some(input.as_bytes())),
            Err(KafkaEventValueDecodeErrorV0::InvalidHex { .. })
        ));
    }
}

#[test]
fn malformed_corpus_and_diagnostics_do_not_expose_values() {
    for length in 0..512 {
        let input: Vec<u8> = (0..length)
            .map(|index| ((index * 73 + length * 19) % 256) as u8)
            .collect();
        let result = std::panic::catch_unwind(|| decode_decant_kafka_event_value_v0(Some(&input)));
        assert!(result.is_ok());
    }

    let first = br#"{"record_format":"decant-kafka-event","record_version":0,"schema_id":"secret-a","event_time_ns":111,"payload":[{"name":"hidden-a","value":{"kind":"utf8","value":"payload-a"}}]}"#;
    let second = br#"{"record_format":"decant-kafka-event","record_version":0,"schema_id":"secret-b","event_time_ns":999,"payload":[{"name":"hidden-b","value":{"kind":"utf8","value":"payload-b"}}]}"#;
    let first_debug = format!(
        "{:?}",
        decode_decant_kafka_event_value_v0(Some(first)).unwrap()
    );
    let second_debug = format!(
        "{:?}",
        decode_decant_kafka_event_value_v0(Some(second)).unwrap()
    );
    assert_eq!(first_debug, second_debug);
    for probe in [
        "secret-a",
        "secret-b",
        "hidden-a",
        "hidden-b",
        "payload-a",
        "payload-b",
        "111",
        "999",
    ] {
        assert!(!format!("{first_debug}{second_debug}").contains(probe));
    }
}
