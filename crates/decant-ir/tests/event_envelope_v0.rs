use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use decant_ir::{
    CanonicalPayloadV0, ENVELOPE_VERSION_V0, EventEnvelopeV0, IdentifierKindV0,
    KafkaPositionFieldV0, KafkaPositionProblemV0, LengthComponentV0, MAX_FIELD_NAME_BYTES,
    MAX_IDENTIFIER_BYTES, MAX_KAFKA_OFFSET_V0, MAX_KAFKA_PARTITION_V0, MAX_KEY_BYTES,
    MAX_LOGICAL_PAYLOAD_BYTES, MAX_OTLP_FILE_LINE_NUMBER_V0, MAX_OTLP_FILE_RESOURCE_SPANS_INDEX_V0,
    MAX_OTLP_FILE_SCOPE_SPANS_INDEX_V0, MAX_OTLP_FILE_SPAN_INDEX_V0, MAX_PAYLOAD_FIELDS,
    MAX_SCALAR_BYTES, OtlpFilePositionFieldV0, OtlpFilePositionProblemV0,
    RedeliveryClassificationV0, ScalarKindV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0, ValidationErrorV0,
};

fn payload(fields: impl IntoIterator<Item = (String, ScalarValueV0)>) -> CanonicalPayloadV0 {
    CanonicalPayloadV0::try_from_fields(fields.into_iter().collect()).unwrap()
}

fn empty_payload() -> CanonicalPayloadV0 {
    payload([])
}

fn otlp_envelope(
    source_id: &str,
    line_number: u64,
    resource_index: u32,
    scope_index: u32,
    span_index: u32,
    ingest_time_ns: i64,
    payload: CanonicalPayloadV0,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        ENVELOPE_VERSION_V0,
        SourceIdV0::new(source_id).unwrap(),
        SchemaIdV0::new("otlp-span-v0").unwrap(),
        SourcePositionV0::otlp_file(line_number, resource_index, scope_index, span_index).unwrap(),
        Some(&[7; 16]),
        101,
        ingest_time_ns,
        SensitivityBitmapV0::from_bits(u64::MAX),
        payload,
    )
    .unwrap()
}

fn kafka_envelope(
    source_id: &str,
    partition: i32,
    offset: i64,
    ingest_time_ns: i64,
    payload: CanonicalPayloadV0,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        ENVELOPE_VERSION_V0,
        SourceIdV0::new(source_id).unwrap(),
        SchemaIdV0::new("kafka-json-v0").unwrap(),
        SourcePositionV0::kafka(partition, offset).unwrap(),
        Some(b"payments-v2/region-b"),
        101,
        ingest_time_ns,
        SensitivityBitmapV0::from_bits(u64::MAX),
        payload,
    )
    .unwrap()
}

#[test]
fn otlp_file_position_bounds_ordering_and_diagnostics_are_exact() {
    let minimum = SourcePositionV0::otlp_file(1, 0, 0, 0).unwrap();
    assert_eq!(minimum.kind(), SourcePositionKindV0::OtlpFile);
    assert_eq!(minimum.source_ordering_ordinal(), 0);
    assert_eq!(minimum.synthetic_sequence(), 0);
    let minimum_coordinates = minimum.otlp_file_position().unwrap();
    assert_eq!(minimum_coordinates.line_number(), 1);
    assert_eq!(minimum_coordinates.resource_spans_index(), 0);
    assert_eq!(minimum_coordinates.scope_spans_index(), 0);
    assert_eq!(minimum_coordinates.span_index(), 0);

    let asymmetric = SourcePositionV0::otlp_file(9, 2, 3, 4).unwrap();
    assert_eq!(
        asymmetric.source_ordering_ordinal(),
        (8_u64 << 26) | (2_u64 << 19) | (3_u64 << 12) | 4
    );

    let last_span = SourcePositionV0::otlp_file(1, 0, 0, 4095).unwrap();
    let next_scope = SourcePositionV0::otlp_file(1, 0, 1, 0).unwrap();
    assert_eq!(
        last_span.source_ordering_ordinal() + 1,
        next_scope.source_ordering_ordinal()
    );
    let last_scope = SourcePositionV0::otlp_file(1, 0, 127, 4095).unwrap();
    let next_resource = SourcePositionV0::otlp_file(1, 1, 0, 0).unwrap();
    assert_eq!(
        last_scope.source_ordering_ordinal() + 1,
        next_resource.source_ordering_ordinal()
    );
    let last_resource = SourcePositionV0::otlp_file(1, 127, 127, 4095).unwrap();
    let next_line = SourcePositionV0::otlp_file(2, 0, 0, 0).unwrap();
    assert_eq!(next_line.source_ordering_ordinal(), 1_u64 << 26);
    assert_eq!(
        last_resource.source_ordering_ordinal() + 1,
        next_line.source_ordering_ordinal()
    );

    let maximum = SourcePositionV0::otlp_file(
        MAX_OTLP_FILE_LINE_NUMBER_V0,
        MAX_OTLP_FILE_RESOURCE_SPANS_INDEX_V0,
        MAX_OTLP_FILE_SCOPE_SPANS_INDEX_V0,
        MAX_OTLP_FILE_SPAN_INDEX_V0,
    )
    .unwrap();
    assert_eq!(maximum.source_ordering_ordinal(), u64::MAX);

    assert!(minimum < asymmetric);
    assert!(
        SourcePositionV0::Synthetic { sequence: u64::MAX }
            < SourcePositionV0::otlp_file(1, 0, 0, 0).unwrap()
    );
    assert_eq!(
        SourcePositionV0::Synthetic { sequence: 7 }
            .cmp(&SourcePositionV0::Synthetic { sequence: 7 }),
        std::cmp::Ordering::Equal
    );

    let synthetic_zero = SourcePositionV0::Synthetic { sequence: 0 };
    let otlp_zero = SourcePositionV0::otlp_file(1, 0, 0, 0).unwrap();
    assert_eq!(synthetic_zero.source_ordering_ordinal(), 0);
    assert_eq!(otlp_zero.source_ordering_ordinal(), 0);
    assert_ne!(synthetic_zero, otlp_zero);
    assert_ne!(synthetic_zero.cmp(&otlp_zero), std::cmp::Ordering::Equal);
    assert_eq!(BTreeSet::from([synthetic_zero, otlp_zero]).len(), 2);
    assert_eq!(
        BTreeMap::from([(synthetic_zero, "synthetic"), (otlp_zero, "otlp")]).len(),
        2
    );

    for (result, field, problem) in [
        (
            SourcePositionV0::otlp_file(0, 0, 0, 0),
            OtlpFilePositionFieldV0::LineNumber,
            OtlpFilePositionProblemV0::Zero,
        ),
        (
            SourcePositionV0::otlp_file(MAX_OTLP_FILE_LINE_NUMBER_V0 + 1, 0, 0, 0),
            OtlpFilePositionFieldV0::LineNumber,
            OtlpFilePositionProblemV0::AboveMaximum,
        ),
        (
            SourcePositionV0::otlp_file(1, MAX_OTLP_FILE_RESOURCE_SPANS_INDEX_V0 + 1, 0, 0),
            OtlpFilePositionFieldV0::ResourceSpansIndex,
            OtlpFilePositionProblemV0::AboveMaximum,
        ),
        (
            SourcePositionV0::otlp_file(1, 0, MAX_OTLP_FILE_SCOPE_SPANS_INDEX_V0 + 1, 0),
            OtlpFilePositionFieldV0::ScopeSpansIndex,
            OtlpFilePositionProblemV0::AboveMaximum,
        ),
        (
            SourcePositionV0::otlp_file(1, 0, 0, MAX_OTLP_FILE_SPAN_INDEX_V0 + 1),
            OtlpFilePositionFieldV0::SpanIndex,
            OtlpFilePositionProblemV0::AboveMaximum,
        ),
    ] {
        assert_eq!(
            result,
            Err(ValidationErrorV0::InvalidOtlpFilePosition { field, problem })
        );
    }

    assert_eq!(
        SourcePositionV0::Synthetic { sequence: 41 }.kind(),
        SourcePositionKindV0::Synthetic
    );
    assert!(
        SourcePositionV0::Synthetic { sequence: 41 }
            .otlp_file_position()
            .is_none()
    );
    assert_eq!(
        format!("{minimum:?}"),
        "SourcePositionV0::OtlpFile { position: <redacted> }"
    );
    assert_eq!(
        format!("{minimum_coordinates:?}"),
        "OtlpFilePositionV0(<redacted>)"
    );
}

#[test]
fn otlp_file_identity_provenance_and_redelivery_are_exact() {
    let payload_a = payload([(
        "span.name".to_owned(),
        ScalarValueV0::utf8("charge").unwrap(),
    )]);
    let event = otlp_envelope("otel-demo-capture", 9, 2, 3, 4, 17, payload_a.clone());
    assert_eq!(
        event.provenance().as_str(),
        "decant://event/v0/otel-demo-capture/otlp-file/9/2/3/4"
    );
    assert_eq!(
        event.classify_redelivery(&otlp_envelope(
            "otel-demo-capture",
            9,
            2,
            3,
            4,
            99,
            payload_a.clone(),
        )),
        RedeliveryClassificationV0::ExactRedelivery
    );
    assert_eq!(
        event.classify_redelivery(&otlp_envelope(
            "otel-demo-capture",
            9,
            2,
            3,
            5,
            17,
            payload_a.clone(),
        )),
        RedeliveryClassificationV0::Distinct
    );
    for candidate in [
        otlp_envelope("otel-demo-capture", 10, 2, 3, 4, 17, payload_a.clone()),
        otlp_envelope("otel-demo-capture", 9, 3, 3, 4, 17, payload_a.clone()),
        otlp_envelope("otel-demo-capture", 9, 2, 4, 4, 17, payload_a.clone()),
    ] {
        assert_eq!(
            event.classify_redelivery(&candidate),
            RedeliveryClassificationV0::Distinct
        );
        assert_ne!(event.provenance(), candidate.provenance());
    }
    assert_eq!(
        event.classify_redelivery(&otlp_envelope(
            "another-capture",
            9,
            2,
            3,
            4,
            17,
            payload_a.clone(),
        )),
        RedeliveryClassificationV0::Distinct
    );
    assert_eq!(
        event.classify_redelivery(&otlp_envelope(
            "otel-demo-capture",
            9,
            2,
            3,
            4,
            17,
            payload([(
                "span.name".to_owned(),
                ScalarValueV0::utf8("refund").unwrap()
            )]),
        )),
        RedeliveryClassificationV0::ConflictingRedelivery
    );

    let diagnostic = format!("{event:?}");
    for secret in ["otel-demo-capture", "charge", "9/2/3/4"] {
        assert!(!diagnostic.contains(secret));
    }

    let sensitive_position = SourcePositionV0::otlp_file(271_828_183, 109, 107, 3583).unwrap();
    let sensitive_event = EventEnvelopeV0::new(
        ENVELOPE_VERSION_V0,
        SourceIdV0::new("numeric-secret-capture").unwrap(),
        SchemaIdV0::new("numeric-secret-schema").unwrap(),
        sensitive_position,
        Some(&[193; 16]),
        9_081_726_354_001,
        -9_081_791_789_123,
        SensitivityBitmapV0::from_bits(u64::MAX),
        payload([(
            "numeric.secret".to_owned(),
            ScalarValueV0::from_u64(65_435_122),
        )]),
    )
    .unwrap();
    let numeric_diagnostic = format!("{sensitive_position:?} {sensitive_event:?}");
    assert_diagnostic_safe(
        &numeric_diagnostic,
        &[
            "271828183",
            "109",
            "107",
            "3583",
            "9081726354001",
            "-9081791789123",
            "65435122",
        ],
    );
}

#[test]
fn kafka_position_bounds_ordering_and_diagnostics_are_exact() {
    let minimum = SourcePositionV0::kafka(0, 0).unwrap();
    assert_eq!(minimum.kind(), SourcePositionKindV0::Kafka);
    assert_eq!(minimum.source_ordering_ordinal(), 0);
    let minimum_coordinates = minimum.kafka_position().unwrap();
    assert_eq!(minimum_coordinates.partition(), 0);
    assert_eq!(minimum_coordinates.offset(), 0);
    assert!(minimum.otlp_file_position().is_none());

    let asymmetric = SourcePositionV0::kafka(23, 42).unwrap();
    assert_eq!(asymmetric.source_ordering_ordinal(), (23_u64 << 48) | 42);
    assert!(minimum < asymmetric);

    let last_offset = SourcePositionV0::kafka(0, MAX_KAFKA_OFFSET_V0).unwrap();
    let next_partition = SourcePositionV0::kafka(1, 0).unwrap();
    assert_eq!(
        last_offset.source_ordering_ordinal() + 1,
        next_partition.source_ordering_ordinal()
    );
    assert!(last_offset < next_partition);

    let maximum = SourcePositionV0::kafka(MAX_KAFKA_PARTITION_V0, MAX_KAFKA_OFFSET_V0).unwrap();
    assert_eq!(maximum.source_ordering_ordinal(), u64::MAX);
    assert_eq!(
        maximum.kafka_position().unwrap().partition(),
        MAX_KAFKA_PARTITION_V0
    );
    assert_eq!(
        maximum.kafka_position().unwrap().offset(),
        MAX_KAFKA_OFFSET_V0
    );

    let synthetic_zero = SourcePositionV0::Synthetic { sequence: 0 };
    let otlp_zero = SourcePositionV0::otlp_file(1, 0, 0, 0).unwrap();
    let kafka_zero = SourcePositionV0::kafka(0, 0).unwrap();
    assert!(synthetic_zero < otlp_zero);
    assert!(otlp_zero < kafka_zero);
    assert_eq!(
        BTreeSet::from([synthetic_zero, otlp_zero, kafka_zero]).len(),
        3
    );
    assert_eq!(
        BTreeMap::from([
            (synthetic_zero, "synthetic"),
            (otlp_zero, "otlp"),
            (kafka_zero, "kafka"),
        ])
        .len(),
        3
    );

    for (result, field, problem) in [
        (
            SourcePositionV0::kafka(-1, 0),
            KafkaPositionFieldV0::Partition,
            KafkaPositionProblemV0::Negative,
        ),
        (
            SourcePositionV0::kafka(MAX_KAFKA_PARTITION_V0 + 1, 0),
            KafkaPositionFieldV0::Partition,
            KafkaPositionProblemV0::AboveMaximum,
        ),
        (
            SourcePositionV0::kafka(0, -1),
            KafkaPositionFieldV0::Offset,
            KafkaPositionProblemV0::Negative,
        ),
        (
            SourcePositionV0::kafka(0, MAX_KAFKA_OFFSET_V0 + 1),
            KafkaPositionFieldV0::Offset,
            KafkaPositionProblemV0::AboveMaximum,
        ),
    ] {
        assert_eq!(
            result,
            Err(ValidationErrorV0::InvalidKafkaPosition { field, problem })
        );
        let diagnostic = format!("{:?}", result.unwrap_err());
        for secret in ["65536", "281474976710656"] {
            assert!(!diagnostic.contains(secret));
        }
    }

    assert_eq!(
        format!("{minimum:?}"),
        "SourcePositionV0::Kafka { position: <redacted> }"
    );
    assert_eq!(
        format!("{minimum_coordinates:?}"),
        "KafkaPositionV0(<redacted>)"
    );
}

#[test]
fn kafka_identity_provenance_and_redelivery_are_exact() {
    let retained_payload = payload([("outcome".to_owned(), ScalarValueV0::utf8("error").unwrap())]);
    let event = kafka_envelope("payments-kafka-v0", 23, 42, 17, retained_payload.clone());
    assert_eq!(
        event.provenance().as_str(),
        "decant://event/v0/payments-kafka-v0/kafka/23/42"
    );
    assert_eq!(
        event.classify_redelivery(&kafka_envelope(
            "payments-kafka-v0",
            23,
            42,
            99,
            retained_payload.clone(),
        )),
        RedeliveryClassificationV0::ExactRedelivery
    );
    for candidate in [
        kafka_envelope("payments-kafka-v0", 23, 43, 17, retained_payload.clone()),
        kafka_envelope("payments-kafka-v0", 24, 42, 17, retained_payload.clone()),
        kafka_envelope("another-kafka-v0", 23, 42, 17, retained_payload.clone()),
    ] {
        assert_eq!(
            event.classify_redelivery(&candidate),
            RedeliveryClassificationV0::Distinct
        );
        assert_ne!(event.provenance(), candidate.provenance());
    }
    assert_eq!(
        event.classify_redelivery(&kafka_envelope(
            "payments-kafka-v0",
            23,
            42,
            17,
            payload([(
                "outcome".to_owned(),
                ScalarValueV0::utf8("success").unwrap(),
            )]),
        )),
        RedeliveryClassificationV0::ConflictingRedelivery
    );

    let diagnostic = format!("{event:?} {:?}", event.source_position());
    for secret in ["payments-kafka-v0", "23", "42", "error"] {
        assert!(!diagnostic.contains(secret));
    }
}

#[allow(clippy::too_many_arguments)]
fn envelope(
    source_id: &str,
    schema_id: &str,
    sequence: u64,
    key: Option<&[u8]>,
    event_time_ns: i64,
    ingest_time_ns: i64,
    sensitivity_bits: u64,
    payload: CanonicalPayloadV0,
) -> Result<EventEnvelopeV0, ValidationErrorV0> {
    EventEnvelopeV0::new(
        ENVELOPE_VERSION_V0,
        SourceIdV0::new(source_id).unwrap(),
        SchemaIdV0::new(schema_id).unwrap(),
        SourcePositionV0::Synthetic { sequence },
        key,
        event_time_ns,
        ingest_time_ns,
        SensitivityBitmapV0::from_bits(sensitivity_bits),
        payload,
    )
}

#[test]
fn identifier_bounds_and_syntax_are_enforced() {
    assert_eq!(SourceIdV0::new("a").unwrap().as_str(), "a");
    assert_eq!(SchemaIdV0::new("0").unwrap().as_str(), "0");

    let exact = format!("a{}", "z".repeat(MAX_IDENTIFIER_BYTES - 1));
    assert_eq!(SourceIdV0::new(&exact).unwrap().as_str(), exact);
    assert_eq!(SchemaIdV0::new(&exact).unwrap().as_str(), exact);
    assert_eq!(
        SchemaIdV0::new("schema-1.v0").unwrap().as_str(),
        "schema-1.v0"
    );

    assert!(matches!(
        SourceIdV0::new(""),
        Err(ValidationErrorV0::InvalidIdentifierLength {
            kind: IdentifierKindV0::Source,
            actual: 0,
            ..
        })
    ));
    assert!(matches!(
        SchemaIdV0::new(&format!("a{}", "z".repeat(MAX_IDENTIFIER_BYTES))),
        Err(ValidationErrorV0::InvalidIdentifierLength {
            kind: IdentifierKindV0::Schema,
            actual,
            ..
        }) if actual == MAX_IDENTIFIER_BYTES + 1
    ));

    for invalid in ["Uppercase", "-leading", "contains/slash", "naïve"] {
        assert!(matches!(
            SourceIdV0::new(invalid),
            Err(ValidationErrorV0::InvalidIdentifierSyntax {
                kind: IdentifierKindV0::Source
            })
        ));
    }
}

#[test]
fn key_boundary_is_exact() {
    let empty = envelope("source", "schema", 0, Some(&[]), 0, 0, 0, empty_payload()).unwrap();
    assert_eq!(empty.key(), Some([].as_slice()));

    let exact = vec![b'k'; MAX_KEY_BYTES];
    let accepted = envelope(
        "source",
        "schema",
        1,
        Some(&exact),
        0,
        0,
        0,
        empty_payload(),
    )
    .unwrap();
    assert_eq!(accepted.key(), Some(exact.as_slice()));

    let too_long = vec![b'k'; MAX_KEY_BYTES + 1];
    assert!(matches!(
        envelope(
            "source",
            "schema",
            1,
            Some(&too_long),
            0,
            0,
            0,
            empty_payload(),
        ),
        Err(ValidationErrorV0::KeyTooLong { actual, .. }) if actual == MAX_KEY_BYTES + 1
    ));
}

#[test]
fn field_count_boundary_is_exact() {
    let exact: BTreeMap<_, _> = (0..MAX_PAYLOAD_FIELDS)
        .map(|index| (format!("f{index}"), ScalarValueV0::null()))
        .collect();
    assert_eq!(
        CanonicalPayloadV0::try_from_fields(exact)
            .unwrap()
            .fields()
            .len(),
        MAX_PAYLOAD_FIELDS
    );

    let too_many: BTreeMap<_, _> = (0..=MAX_PAYLOAD_FIELDS)
        .map(|index| (format!("f{index}"), ScalarValueV0::null()))
        .collect();
    assert!(matches!(
        CanonicalPayloadV0::try_from_fields(too_many),
        Err(ValidationErrorV0::TooManyFields { actual, .. })
            if actual == MAX_PAYLOAD_FIELDS + 1
    ));
}

#[test]
fn field_name_boundary_and_syntax_are_enforced() {
    let minimum = payload([("_".to_owned(), ScalarValueV0::null())]);
    assert_eq!(minimum.fields().next().unwrap().0, "_");

    let exact = format!("a{}", "z".repeat(MAX_FIELD_NAME_BYTES - 1));
    let accepted = payload([(exact.clone(), ScalarValueV0::null())]);
    assert_eq!(accepted.fields().next().unwrap().0, exact);

    let too_long = format!("a{}", "z".repeat(MAX_FIELD_NAME_BYTES));
    assert!(matches!(
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([(
            too_long,
            ScalarValueV0::null(),
        )])),
        Err(ValidationErrorV0::InvalidFieldNameLength { actual, .. })
            if actual == MAX_FIELD_NAME_BYTES + 1
    ));

    for invalid in ["", "9leading", "Uppercase", "contains/slash", "naïve"] {
        let result = CanonicalPayloadV0::try_from_fields(BTreeMap::from([(
            invalid.to_owned(),
            ScalarValueV0::null(),
        )]));
        assert!(matches!(
            result,
            Err(ValidationErrorV0::InvalidFieldNameLength { .. })
                | Err(ValidationErrorV0::InvalidFieldNameSyntax)
        ));
    }
}

#[test]
fn scalar_boundaries_and_read_only_accessors_are_exact() {
    assert_eq!(ScalarValueV0::utf8("").unwrap().as_utf8(), Some(""));
    assert_eq!(
        ScalarValueV0::bytes(&[]).unwrap().as_bytes(),
        Some([].as_slice())
    );

    let exact_text = "u".repeat(MAX_SCALAR_BYTES);
    let exact_bytes = vec![0xa5; MAX_SCALAR_BYTES];
    assert_eq!(
        ScalarValueV0::utf8(&exact_text).unwrap().as_utf8(),
        Some(exact_text.as_str())
    );
    assert_eq!(
        ScalarValueV0::bytes(&exact_bytes).unwrap().as_bytes(),
        Some(exact_bytes.as_slice())
    );

    assert!(matches!(
        ScalarValueV0::utf8(&"u".repeat(MAX_SCALAR_BYTES + 1)),
        Err(ValidationErrorV0::ScalarTooLong {
            kind: ScalarKindV0::Utf8,
            actual,
            ..
        }) if actual == MAX_SCALAR_BYTES + 1
    ));
    assert!(matches!(
        ScalarValueV0::bytes(&vec![0xa5; MAX_SCALAR_BYTES + 1]),
        Err(ValidationErrorV0::ScalarTooLong {
            kind: ScalarKindV0::Bytes,
            actual,
            ..
        }) if actual == MAX_SCALAR_BYTES + 1
    ));

    assert_eq!(ScalarValueV0::from_bool(true).as_bool(), Some(true));
    assert_eq!(ScalarValueV0::from_i64(i64::MIN).as_i64(), Some(i64::MIN));
    assert_eq!(ScalarValueV0::from_u64(u64::MAX).as_u64(), Some(u64::MAX));
    assert_eq!(ScalarValueV0::null().kind(), ScalarKindV0::Null);
}

#[test]
fn logical_payload_boundary_is_exact() {
    let exact = payload([
        (
            "a".to_owned(),
            ScalarValueV0::bytes(&vec![0; MAX_SCALAR_BYTES]).unwrap(),
        ),
        (
            "b".to_owned(),
            ScalarValueV0::bytes(&vec![0; MAX_SCALAR_BYTES]).unwrap(),
        ),
        (
            "c".to_owned(),
            ScalarValueV0::bytes(&vec![0; MAX_SCALAR_BYTES]).unwrap(),
        ),
        (
            "d".to_owned(),
            ScalarValueV0::bytes(&vec![0; 4_052]).unwrap(),
        ),
    ]);
    assert_eq!(exact.logical_len(), MAX_LOGICAL_PAYLOAD_BYTES);

    let one_past = CanonicalPayloadV0::try_from_fields(BTreeMap::from([
        (
            "a".to_owned(),
            ScalarValueV0::bytes(&vec![0; MAX_SCALAR_BYTES]).unwrap(),
        ),
        (
            "b".to_owned(),
            ScalarValueV0::bytes(&vec![0; MAX_SCALAR_BYTES]).unwrap(),
        ),
        (
            "c".to_owned(),
            ScalarValueV0::bytes(&vec![0; MAX_SCALAR_BYTES]).unwrap(),
        ),
        (
            "d".to_owned(),
            ScalarValueV0::bytes(&vec![0; 4_053]).unwrap(),
        ),
    ]));
    assert!(matches!(
        one_past,
        Err(ValidationErrorV0::PayloadTooLarge { actual, .. })
            if actual == MAX_LOGICAL_PAYLOAD_BYTES + 1
    ));
}

#[test]
fn payload_order_is_canonical() {
    let mut forward = BTreeMap::new();
    forward.insert("alpha".to_owned(), ScalarValueV0::from_u64(1));
    forward.insert("beta".to_owned(), ScalarValueV0::from_u64(2));

    let mut reverse = BTreeMap::new();
    reverse.insert("beta".to_owned(), ScalarValueV0::from_u64(2));
    reverse.insert("alpha".to_owned(), ScalarValueV0::from_u64(1));

    let forward = CanonicalPayloadV0::try_from_fields(forward).unwrap();
    let reverse = CanonicalPayloadV0::try_from_fields(reverse).unwrap();
    assert_eq!(forward, reverse);
    assert_eq!(forward.logical_len(), reverse.logical_len());
    assert_eq!(
        forward.fields().map(|(name, _)| name).collect::<Vec<_>>(),
        vec!["alpha", "beta"]
    );
}

#[test]
fn version_and_extreme_timestamps_have_explicit_behavior() {
    let rejected = EventEnvelopeV0::new(
        1,
        SourceIdV0::new("source").unwrap(),
        SchemaIdV0::new("schema").unwrap(),
        SourcePositionV0::Synthetic { sequence: 0 },
        None,
        0,
        0,
        SensitivityBitmapV0::from_bits(0),
        empty_payload(),
    );
    assert!(matches!(
        rejected,
        Err(ValidationErrorV0::UnsupportedEnvelopeVersion { actual: 1 })
    ));

    let accepted = envelope(
        "source",
        "schema",
        u64::MAX,
        None,
        i64::MIN,
        i64::MAX,
        u64::MAX,
        empty_payload(),
    )
    .unwrap();
    assert_eq!(accepted.envelope_version(), ENVELOPE_VERSION_V0);
    assert_eq!(accepted.event_time_ns(), i64::MIN);
    assert_eq!(accepted.ingest_time_ns(), i64::MAX);
    assert_eq!(accepted.source_position().synthetic_sequence(), u64::MAX);
    assert_eq!(accepted.sensitivity().bits(), u64::MAX);

    let inverse_extremes = envelope(
        "source",
        "schema",
        0,
        None,
        i64::MAX,
        i64::MIN,
        0,
        empty_payload(),
    )
    .unwrap();
    assert_eq!(inverse_extremes.event_time_ns(), i64::MAX);
    assert_eq!(inverse_extremes.ingest_time_ns(), i64::MIN);
}

#[test]
fn provenance_is_derived_exactly() {
    let event = envelope(
        "synthetic-source",
        "schema.v0",
        42,
        None,
        10,
        20,
        0,
        empty_payload(),
    )
    .unwrap();
    assert_eq!(event.source_id().as_str(), "synthetic-source");
    assert_eq!(event.schema_id().as_str(), "schema.v0");
    assert_eq!(
        event.provenance().as_str(),
        "decant://event/v0/synthetic-source/synthetic/42"
    );
}

#[test]
fn redelivery_classification_matches_identity_and_immutable_content() {
    let base = envelope(
        "source",
        "schema",
        7,
        Some(b"key"),
        100,
        200,
        1,
        payload([("value".to_owned(), ScalarValueV0::from_i64(5))]),
    )
    .unwrap();

    let exact_with_different_ingest_time = envelope(
        "source",
        "schema",
        7,
        Some(b"key"),
        100,
        999,
        1,
        payload([("value".to_owned(), ScalarValueV0::from_i64(5))]),
    )
    .unwrap();
    assert_eq!(
        base.classify_redelivery(&exact_with_different_ingest_time),
        RedeliveryClassificationV0::ExactRedelivery
    );

    for distinct in [
        envelope(
            "other-source",
            "schema",
            7,
            Some(b"key"),
            100,
            200,
            1,
            payload([("value".to_owned(), ScalarValueV0::from_i64(5))]),
        )
        .unwrap(),
        envelope(
            "source",
            "schema",
            8,
            Some(b"key"),
            100,
            200,
            1,
            payload([("value".to_owned(), ScalarValueV0::from_i64(5))]),
        )
        .unwrap(),
    ] {
        assert_eq!(
            base.classify_redelivery(&distinct),
            RedeliveryClassificationV0::Distinct
        );
    }

    let conflicts = [
        envelope(
            "source",
            "other-schema",
            7,
            Some(b"key"),
            100,
            200,
            1,
            payload([("value".to_owned(), ScalarValueV0::from_i64(5))]),
        )
        .unwrap(),
        envelope(
            "source",
            "schema",
            7,
            Some(b"other-key"),
            100,
            200,
            1,
            payload([("value".to_owned(), ScalarValueV0::from_i64(5))]),
        )
        .unwrap(),
        envelope(
            "source",
            "schema",
            7,
            Some(b"key"),
            101,
            200,
            1,
            payload([("value".to_owned(), ScalarValueV0::from_i64(5))]),
        )
        .unwrap(),
        envelope(
            "source",
            "schema",
            7,
            Some(b"key"),
            100,
            200,
            2,
            payload([("value".to_owned(), ScalarValueV0::from_i64(5))]),
        )
        .unwrap(),
        envelope(
            "source",
            "schema",
            7,
            Some(b"key"),
            100,
            200,
            1,
            payload([("value".to_owned(), ScalarValueV0::from_i64(6))]),
        )
        .unwrap(),
    ];

    for conflict in conflicts {
        assert_eq!(
            base.classify_redelivery(&conflict),
            RedeliveryClassificationV0::ConflictingRedelivery
        );
    }
}

#[test]
fn all_scalar_debug_variants_are_content_independent() {
    let left_text = "SYNTHETIC_SECRET_AAAA";
    let right_text = "SYNTHETIC_SECRET_BBBB";
    let left_bytes = left_text.as_bytes();
    let right_bytes = right_text.as_bytes();

    let pairs = [
        (ScalarValueV0::null(), ScalarValueV0::null()),
        (
            ScalarValueV0::from_bool(false),
            ScalarValueV0::from_bool(true),
        ),
        (
            ScalarValueV0::from_i64(-7_654_321),
            ScalarValueV0::from_i64(9_876_543),
        ),
        (
            ScalarValueV0::from_u64(1_234_567),
            ScalarValueV0::from_u64(7_654_321),
        ),
        (
            ScalarValueV0::utf8(left_text).unwrap(),
            ScalarValueV0::utf8(right_text).unwrap(),
        ),
        (
            ScalarValueV0::bytes(left_bytes).unwrap(),
            ScalarValueV0::bytes(right_bytes).unwrap(),
        ),
    ];

    for (left, right) in pairs {
        let left_debug = format!("{left:?}");
        let right_debug = format!("{right:?}");
        assert_eq!(left_debug, right_debug);
        assert_diagnostic_safe(&left_debug, &[left_text, right_text]);
    }
}

#[test]
fn aggregate_debug_is_content_independent() {
    let left_secret = "SYNTHETIC_SECRET_AAAA";
    let right_secret = "SYNTHETIC_SECRET_BBBB";

    let left = envelope(
        "source-aaaa",
        "schema-aaaa",
        111,
        Some(left_secret.as_bytes()),
        123,
        456,
        1,
        payload([(
            "secret_aaaa".to_owned(),
            ScalarValueV0::utf8(left_secret).unwrap(),
        )]),
    )
    .unwrap();
    let right = envelope(
        "source-bbbb",
        "schema-bbbb",
        999,
        Some(right_secret.as_bytes()),
        789,
        101,
        2,
        payload([(
            "secret_bbbb".to_owned(),
            ScalarValueV0::utf8(right_secret).unwrap(),
        )]),
    )
    .unwrap();

    let left_debug = format!("{left:?}");
    let right_debug = format!("{right:?}");
    assert_eq!(left_debug, right_debug);
    assert_diagnostic_safe(
        &left_debug,
        &[
            left_secret,
            right_secret,
            "source-aaaa",
            "source-bbbb",
            "schema-aaaa",
            "schema-bbbb",
            "secret_aaaa",
            "secret_bbbb",
        ],
    );
}

#[test]
fn every_validation_error_has_content_independent_debug_and_display() {
    let secret_a = "SYNTHETIC_SECRET_AAAA";
    let secret_b = "SYNTHETIC_SECRET_BBBB";

    assert_error_pair(
        ValidationErrorV0::UnsupportedEnvelopeVersion { actual: 7 },
        ValidationErrorV0::UnsupportedEnvelopeVersion { actual: 7 },
        &[],
    );
    assert_error_pair(
        SourceIdV0::new(&"a".repeat(MAX_IDENTIFIER_BYTES + 1)).unwrap_err(),
        SourceIdV0::new(&"b".repeat(MAX_IDENTIFIER_BYTES + 1)).unwrap_err(),
        &[],
    );
    assert_error_pair(
        SourceIdV0::new(secret_a).unwrap_err(),
        SourceIdV0::new(secret_b).unwrap_err(),
        &[secret_a, secret_b],
    );

    let long_key_a = vec![b'a'; MAX_KEY_BYTES + 1];
    let long_key_b = vec![b'b'; MAX_KEY_BYTES + 1];
    assert_error_pair(
        envelope(
            "source",
            "schema",
            0,
            Some(&long_key_a),
            0,
            0,
            0,
            empty_payload(),
        )
        .unwrap_err(),
        envelope(
            "source",
            "schema",
            0,
            Some(&long_key_b),
            0,
            0,
            0,
            empty_payload(),
        )
        .unwrap_err(),
        &[],
    );

    let too_many_a = (0..=MAX_PAYLOAD_FIELDS)
        .map(|index| (format!("a{index}"), ScalarValueV0::null()))
        .collect();
    let too_many_b = (0..=MAX_PAYLOAD_FIELDS)
        .map(|index| (format!("b{index}"), ScalarValueV0::null()))
        .collect();
    assert_error_pair(
        CanonicalPayloadV0::try_from_fields(too_many_a).unwrap_err(),
        CanonicalPayloadV0::try_from_fields(too_many_b).unwrap_err(),
        &[],
    );

    assert_error_pair(
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([(
            "a".repeat(MAX_FIELD_NAME_BYTES + 1),
            ScalarValueV0::null(),
        )]))
        .unwrap_err(),
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([(
            "b".repeat(MAX_FIELD_NAME_BYTES + 1),
            ScalarValueV0::null(),
        )]))
        .unwrap_err(),
        &[],
    );
    assert_error_pair(
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([(
            "SecretA".to_owned(),
            ScalarValueV0::null(),
        )]))
        .unwrap_err(),
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([(
            "SecretB".to_owned(),
            ScalarValueV0::null(),
        )]))
        .unwrap_err(),
        &["SecretA", "SecretB"],
    );

    assert_error_pair(
        ScalarValueV0::utf8(&"a".repeat(MAX_SCALAR_BYTES + 1)).unwrap_err(),
        ScalarValueV0::utf8(&"b".repeat(MAX_SCALAR_BYTES + 1)).unwrap_err(),
        &[],
    );
    assert_error_pair(
        ScalarValueV0::bytes(&vec![b'a'; MAX_SCALAR_BYTES + 1]).unwrap_err(),
        ScalarValueV0::bytes(&vec![b'b'; MAX_SCALAR_BYTES + 1]).unwrap_err(),
        &[],
    );

    let oversized_a = oversized_payload_error(b'a');
    let oversized_b = oversized_payload_error(b'b');
    assert_error_pair(oversized_a, oversized_b, &[]);

    for component in [
        LengthComponentV0::FieldCount,
        LengthComponentV0::FieldNameLength,
        LengthComponentV0::FieldName,
        LengthComponentV0::ScalarTag,
        LengthComponentV0::ScalarContent,
    ] {
        assert_error_pair(
            ValidationErrorV0::LengthOverflow { component },
            ValidationErrorV0::LengthOverflow { component },
            &[],
        );
    }
    assert_error_pair(
        SourcePositionV0::otlp_file(0, 0, 0, 0).unwrap_err(),
        SourcePositionV0::otlp_file(0, 0, 0, 0).unwrap_err(),
        &[],
    );
    assert_error_pair(
        SourcePositionV0::otlp_file(1, MAX_OTLP_FILE_RESOURCE_SPANS_INDEX_V0 + 1, 0, 0)
            .unwrap_err(),
        SourcePositionV0::otlp_file(1, MAX_OTLP_FILE_RESOURCE_SPANS_INDEX_V0 + 2, 0, 0)
            .unwrap_err(),
        &[],
    );
}

#[test]
fn unwrap_and_assertion_panics_do_not_expose_fixture_values() {
    let invalid_secret = "SYNTHETIC_SECRET_AAAA";
    let unwrap_panic = std::panic::catch_unwind(|| {
        SourceIdV0::new(invalid_secret).unwrap();
    })
    .unwrap_err();
    let unwrap_text = panic_payload_text(unwrap_panic);
    assert_diagnostic_safe(&unwrap_text, &[invalid_secret]);

    let left_secret = "SYNTHETIC_SECRET_CCCC";
    let right_secret = "SYNTHETIC_SECRET_DDDD";
    let assertion_panic = std::panic::catch_unwind(|| {
        assert_eq!(
            ScalarValueV0::utf8(left_secret).unwrap(),
            ScalarValueV0::utf8(right_secret).unwrap()
        );
    })
    .unwrap_err();
    let assertion_text = panic_payload_text(assertion_panic);
    assert_diagnostic_safe(&assertion_text, &[left_secret, right_secret]);
}

fn oversized_payload_error(fill: u8) -> ValidationErrorV0 {
    CanonicalPayloadV0::try_from_fields(BTreeMap::from([
        (
            "a".to_owned(),
            ScalarValueV0::bytes(&vec![fill; MAX_SCALAR_BYTES]).unwrap(),
        ),
        (
            "b".to_owned(),
            ScalarValueV0::bytes(&vec![fill; MAX_SCALAR_BYTES]).unwrap(),
        ),
        (
            "c".to_owned(),
            ScalarValueV0::bytes(&vec![fill; MAX_SCALAR_BYTES]).unwrap(),
        ),
        (
            "d".to_owned(),
            ScalarValueV0::bytes(&vec![fill; 4_053]).unwrap(),
        ),
    ]))
    .unwrap_err()
}

fn assert_error_pair(left: ValidationErrorV0, right: ValidationErrorV0, secrets: &[&str]) {
    let left_debug = format!("{left:?}");
    let right_debug = format!("{right:?}");
    let left_display = left.to_string();
    let right_display = right.to_string();

    assert_eq!(left_debug, right_debug);
    assert_eq!(left_display, right_display);
    assert_diagnostic_safe(&left_debug, secrets);
    assert_diagnostic_safe(&left_display, secrets);
}

fn assert_diagnostic_safe(diagnostic: &str, secrets: &[&str]) {
    for secret in secrets {
        let escaped: String = secret.chars().flat_map(char::escape_default).collect();
        let decimal_comma = secret
            .as_bytes()
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let decimal_debug = format!("{:?}", secret.as_bytes());
        let hex_lower = hex(secret.as_bytes(), false);
        let hex_upper = hex(secret.as_bytes(), true);
        let base64 = base64(secret.as_bytes());
        let prefix = &secret[..secret.len().min(8)];

        for recoverable in [
            *secret,
            escaped.as_str(),
            decimal_comma.as_str(),
            decimal_debug.as_str(),
            hex_lower.as_str(),
            hex_upper.as_str(),
            base64.as_str(),
            prefix,
        ] {
            assert!(
                !diagnostic.contains(recoverable),
                "diagnostic contained a recoverable fixture representation"
            );
        }
    }
}

fn hex(bytes: &[u8], uppercase: bool) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        if uppercase {
            write!(&mut encoded, "{byte:02X}").unwrap();
        } else {
            write!(&mut encoded, "{byte:02x}").unwrap();
        }
    }
    encoded
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);

    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        let combined = (u32::from(first) << 16) | (u32::from(second) << 8) | u32::from(third);

        encoded.push(char::from(ALPHABET[((combined >> 18) & 0x3f) as usize]));
        encoded.push(char::from(ALPHABET[((combined >> 12) & 0x3f) as usize]));
        if chunk.len() > 1 {
            encoded.push(char::from(ALPHABET[((combined >> 6) & 0x3f) as usize]));
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(char::from(ALPHABET[(combined & 0x3f) as usize]));
        } else {
            encoded.push('=');
        }
    }

    encoded
}

fn panic_payload_text(payload: Box<dyn Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_owned()
    } else {
        "non-string panic payload".to_owned()
    }
}
