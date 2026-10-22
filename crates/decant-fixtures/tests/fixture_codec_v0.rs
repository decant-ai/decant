use std::collections::BTreeMap;

use decant_fixtures::{
    BinaryFieldV0, EndOfInputV0, FixtureCodecErrorV0, FixtureRecordV0, MAX_FIXTURE_DOCUMENT_BYTES,
    decode_fixture_record_v0, encode_fixture_record_v0,
};
use decant_ir::{
    CanonicalPayloadV0, ENVELOPE_VERSION_V0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0,
    SensitivityBitmapV0, SourceIdV0, SourcePositionV0, ValidationErrorV0,
};

const EVENT_GOLDEN: &[u8] = include_bytes!("golden/event-all-scalars-v0.jsonl");
const END_GOLDEN: &[u8] = include_bytes!("golden/end-of-input-v0.jsonl");

fn all_scalar_event() -> EventEnvelopeV0 {
    let payload = CanonicalPayloadV0::try_from_fields(BTreeMap::from([
        ("alpha_null".to_owned(), ScalarValueV0::null()),
        ("beta_bool".to_owned(), ScalarValueV0::from_bool(true)),
        (
            "bytes".to_owned(),
            ScalarValueV0::bytes(&[0x00, 0xff, 0x10]).unwrap(),
        ),
        ("i64".to_owned(), ScalarValueV0::from_i64(-7)),
        ("u64".to_owned(), ScalarValueV0::from_u64(u64::MAX)),
        (
            "utf8".to_owned(),
            ScalarValueV0::utf8("safe synthetic").unwrap(),
        ),
    ]))
    .unwrap();
    EventEnvelopeV0::new(
        ENVELOPE_VERSION_V0,
        SourceIdV0::new("sre-v0").unwrap(),
        SchemaIdV0::new("payment-v0").unwrap(),
        SourcePositionV0::Synthetic { sequence: 7 },
        Some(&[0x00, 0x0f, 0x10, 0xab, 0xff]),
        1_767_225_600_000_000_000,
        1_767_225_601_000_000_000,
        SensitivityBitmapV0::from_bits(5),
        payload,
    )
    .unwrap()
}

fn event_record() -> FixtureRecordV0 {
    FixtureRecordV0::Event(all_scalar_event())
}

fn otlp_event_record() -> FixtureRecordV0 {
    FixtureRecordV0::Event(
        EventEnvelopeV0::new(
            ENVELOPE_VERSION_V0,
            SourceIdV0::new("otel-demo-capture").unwrap(),
            SchemaIdV0::new("otlp-span-v0").unwrap(),
            SourcePositionV0::otlp_file(4, 1, 2, 3).unwrap(),
            Some(&[7; 16]),
            100,
            0,
            SensitivityBitmapV0::from_bits(u64::MAX),
            CanonicalPayloadV0::try_from_fields(BTreeMap::from([(
                "span.name".to_owned(),
                ScalarValueV0::utf8("charge").unwrap(),
            )]))
            .unwrap(),
        )
        .unwrap(),
    )
}

fn kafka_event_record() -> FixtureRecordV0 {
    FixtureRecordV0::Event(
        EventEnvelopeV0::new(
            ENVELOPE_VERSION_V0,
            SourceIdV0::new("payments-kafka-v0").unwrap(),
            SchemaIdV0::new("kafka-json-v0").unwrap(),
            SourcePositionV0::kafka(23, 42).unwrap(),
            Some(b"payments-v2/region-b"),
            100,
            0,
            SensitivityBitmapV0::from_bits(u64::MAX),
            CanonicalPayloadV0::try_from_fields(BTreeMap::from([(
                "outcome".to_owned(),
                ScalarValueV0::utf8("error").unwrap(),
            )]))
            .unwrap(),
        )
        .unwrap(),
    )
}

fn replace_once(input: &[u8], from: &str, to: &str) -> Vec<u8> {
    let text = std::str::from_utf8(input).unwrap();
    assert_eq!(
        text.matches(from).count(),
        1,
        "mutation must be unambiguous"
    );
    text.replacen(from, to, 1).into_bytes()
}

#[test]
fn all_scalar_variants_round_trip_and_event_bytes_are_golden() {
    let encoded = encode_fixture_record_v0(&event_record()).unwrap();
    assert_eq!(encoded, EVENT_GOLDEN);
    assert_eq!(
        decode_fixture_record_v0(EVENT_GOLDEN).unwrap(),
        event_record()
    );
    assert_eq!(
        encode_fixture_record_v0(&decode_fixture_record_v0(EVENT_GOLDEN).unwrap()).unwrap(),
        EVENT_GOLDEN
    );
}

#[test]
fn synthetic_fixture_encoder_rejects_external_positions_without_mislabeled_bytes() {
    for record in [otlp_event_record(), kafka_event_record()] {
        assert_eq!(
            encode_fixture_record_v0(&record),
            Err(FixtureCodecErrorV0::UnsupportedSourcePosition)
        );
    }
    assert_eq!(
        encode_fixture_record_v0(&event_record()).unwrap(),
        EVENT_GOLDEN
    );
}

#[test]
fn end_of_input_round_trips_and_matches_golden() {
    let record = FixtureRecordV0::EndOfInput(
        EndOfInputV0::new(
            SourceIdV0::new("sre-v0").unwrap(),
            1_767_226_800_000_000_000,
        )
        .unwrap(),
    );
    assert_eq!(encode_fixture_record_v0(&record).unwrap(), END_GOLDEN);
    assert_eq!(decode_fixture_record_v0(END_GOLDEN).unwrap(), record);
}

#[test]
fn missing_and_null_event_time_share_the_typed_rejection() {
    let missing = replace_once(EVENT_GOLDEN, ",\"event_time_ns\":1767225600000000000", "");
    let explicit_null = replace_once(
        EVENT_GOLDEN,
        "\"event_time_ns\":1767225600000000000",
        "\"event_time_ns\":null",
    );
    for input in [missing, explicit_null] {
        assert_eq!(
            decode_fixture_record_v0(&input),
            Err(FixtureCodecErrorV0::MissingEventTime)
        );
    }
}

#[test]
fn input_provenance_is_rejected_and_valid_provenance_is_derived() {
    let injected = replace_once(
        EVENT_GOLDEN,
        "\"payload\":[",
        "\"provenance\":\"decant://event/v0/forged\",\"payload\":[",
    );
    assert_eq!(
        decode_fixture_record_v0(&injected),
        Err(FixtureCodecErrorV0::MalformedJson)
    );

    let FixtureRecordV0::Event(event) = decode_fixture_record_v0(EVENT_GOLDEN).unwrap() else {
        panic!("golden event decoded as control")
    };
    assert_eq!(
        event.provenance().as_str(),
        "decant://event/v0/sre-v0/synthetic/7"
    );
}

#[test]
fn noncanonical_json_variants_are_rejected() {
    let whitespace = replace_once(EVENT_GOLDEN, "{\"record_type\"", "{ \"record_type\"");
    let reordered = replace_once(
        EVENT_GOLDEN,
        "\"record_type\":\"event\",\"record_format\":\"decant-fixture-record\"",
        "\"record_format\":\"decant-fixture-record\",\"record_type\":\"event\"",
    );
    let escaped = replace_once(EVENT_GOLDEN, "sre-v0", "sr\\u0065-v0");
    let mut crlf = EVENT_GOLDEN[..EVENT_GOLDEN.len() - 1].to_vec();
    crlf.extend_from_slice(b"\r\n");

    for input in [whitespace, reordered, escaped, crlf] {
        assert_eq!(
            decode_fixture_record_v0(&input),
            Err(FixtureCodecErrorV0::NonCanonicalEncoding)
        );
    }
    assert_eq!(
        decode_fixture_record_v0(&EVENT_GOLDEN[..EVENT_GOLDEN.len() - 1]),
        Err(FixtureCodecErrorV0::MissingTerminalLineFeed)
    );
}

#[test]
fn duplicate_payload_names_and_unknown_fields_are_rejected() {
    let duplicate = replace_once(
        EVENT_GOLDEN,
        "]}\n",
        ",{\"name\":\"alpha_null\",\"value\":{\"kind\":\"null\"}}]}\n",
    );
    assert_eq!(
        decode_fixture_record_v0(&duplicate),
        Err(FixtureCodecErrorV0::DuplicatePayloadField { field_count: 7 })
    );

    let unknown = replace_once(EVENT_GOLDEN, "\"payload\":[", "\"unknown\":0,\"payload\":[");
    assert_eq!(
        decode_fixture_record_v0(&unknown),
        Err(FixtureCodecErrorV0::MalformedJson)
    );

    let unknown_scalar_field = replace_once(
        EVENT_GOLDEN,
        "{\"kind\":\"null\"}",
        "{\"kind\":\"null\",\"extra\":0}",
    );
    assert_eq!(
        decode_fixture_record_v0(&unknown_scalar_field),
        Err(FixtureCodecErrorV0::MalformedJson)
    );
}

#[test]
fn unknown_versions_and_float_scalars_fail_closed() {
    let record_version = replace_once(EVENT_GOLDEN, "\"record_version\":0", "\"record_version\":1");
    assert_eq!(
        decode_fixture_record_v0(&record_version),
        Err(FixtureCodecErrorV0::UnsupportedRecordVersion { actual: 1 })
    );

    let envelope_version = replace_once(
        EVENT_GOLDEN,
        "\"envelope_version\":0",
        "\"envelope_version\":1",
    );
    assert!(matches!(
        decode_fixture_record_v0(&envelope_version),
        Err(FixtureCodecErrorV0::EnvelopeValidation(
            ValidationErrorV0::UnsupportedEnvelopeVersion { actual: 1 }
        ))
    ));

    let float = replace_once(
        EVENT_GOLDEN,
        "{\"kind\":\"i64\",\"value\":-7}",
        "{\"kind\":\"i64\",\"value\":1.5}",
    );
    assert_eq!(
        decode_fixture_record_v0(&float),
        Err(FixtureCodecErrorV0::MalformedJson)
    );

    let record_type = replace_once(
        EVENT_GOLDEN,
        "\"record_type\":\"event\"",
        "\"record_type\":\"other\"",
    );
    assert_eq!(
        decode_fixture_record_v0(&record_type),
        Err(FixtureCodecErrorV0::MalformedJson)
    );

    let source_position = replace_once(
        EVENT_GOLDEN,
        "\"kind\":\"synthetic\",\"sequence\":7",
        "\"kind\":\"external\",\"sequence\":7",
    );
    assert_eq!(
        decode_fixture_record_v0(&source_position),
        Err(FixtureCodecErrorV0::UnsupportedSourcePosition)
    );
}

#[test]
fn uppercase_odd_and_non_hex_binary_values_are_rejected() {
    for (replacement, byte_len) in [("AB", 2), ("abc", 3), ("gg", 2)] {
        let input = replace_once(EVENT_GOLDEN, "000f10abff", replacement);
        assert_eq!(
            decode_fixture_record_v0(&input),
            Err(FixtureCodecErrorV0::InvalidHex {
                field: BinaryFieldV0::Key,
                byte_len,
            })
        );
    }

    let scalar = replace_once(EVENT_GOLDEN, "\"value\":\"00ff10\"", "\"value\":\"00FF10\"");
    assert_eq!(
        decode_fixture_record_v0(&scalar),
        Err(FixtureCodecErrorV0::InvalidHex {
            field: BinaryFieldV0::Scalar,
            byte_len: 6,
        })
    );
}

#[test]
fn record_cap_is_checked_before_parsing() {
    let input = vec![b'x'; MAX_FIXTURE_DOCUMENT_BYTES + 1];
    assert_eq!(
        decode_fixture_record_v0(&input),
        Err(FixtureCodecErrorV0::DocumentTooLarge {
            maximum: MAX_FIXTURE_DOCUMENT_BYTES,
            actual: MAX_FIXTURE_DOCUMENT_BYTES + 1,
        })
    );
}

#[test]
fn equal_length_sensitive_inputs_have_value_safe_diagnostics() {
    let first = replace_once(EVENT_GOLDEN, "000f10abff", "zzzzzzzzzz");
    let second = replace_once(EVENT_GOLDEN, "000f10abff", "yyyyyyyyyy");
    let first_error = decode_fixture_record_v0(&first).unwrap_err();
    let second_error = decode_fixture_record_v0(&second).unwrap_err();
    assert_eq!(format!("{first_error:?}"), format!("{second_error:?}"));
    for diagnostic in [format!("{first_error:?}"), first_error.to_string()] {
        assert!(!diagnostic.contains("zz"));
        assert!(!diagnostic.contains("yy"));
        assert!(!diagnostic.contains("sre-v0"));
    }
}

#[test]
fn malformed_byte_corpus_never_panics() {
    for length in 0..=512 {
        let mut input = (0..length)
            .map(|index| ((index * 31 + length) % 256) as u8)
            .collect::<Vec<_>>();
        if let Some(last) = input.last_mut() {
            *last = b'\n';
        }
        assert!(std::panic::catch_unwind(|| decode_fixture_record_v0(&input)).is_ok());
    }
}
