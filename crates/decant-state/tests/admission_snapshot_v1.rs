use std::collections::BTreeMap;

use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, MAX_KAFKA_OFFSET_V0, MAX_KAFKA_PARTITION_V0,
    MAX_OTLP_FILE_LINE_NUMBER_V0, MAX_OTLP_FILE_RESOURCE_SPANS_INDEX_V0,
    MAX_OTLP_FILE_SCOPE_SPANS_INDEX_V0, MAX_OTLP_FILE_SPAN_INDEX_V0, ScalarValueV0, SchemaIdV0,
    SensitivityBitmapV0, SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_SNAPSHOT_VERSION_V0, ADMISSION_SNAPSHOT_VERSION_V1, ADMISSION_VERSION_V0,
    AdmissionConfigV0, AdmissionErrorV0, AdmissionLimitsV0, AdmissionSnapshotDecodeErrorV0,
    AdmissionSnapshotDecodeErrorV1, AdmissionSnapshotExpectedConfigFieldV1,
    AdmissionSnapshotFieldV1, AdmissionSnapshotOrderV1, AdmissionStateV0, ApplyOutcomeV0,
    FinalizeOutcomeV0, MAX_ACCEPTED_EVENT_BYTES_V0, MAX_ACCEPTED_IDENTITIES_V0,
    MAX_ADMISSION_SNAPSHOT_BYTES_V0, MAX_ADMISSION_SNAPSHOT_BYTES_V1, RejectionReasonV0,
    encode_admission_snapshot_v0, encode_admission_snapshot_v1, restore_admission_snapshot_v0,
    restore_admission_snapshot_v1,
};

const ALLOWED_LATENESS_NS: i64 = 30_000_000_000;
const SYNTHETIC_GOLDEN: &str = include_str!("golden/admission-snapshot-v1-synthetic.hex");
const OTLP_FILE_GOLDEN: &str = include_str!("golden/admission-snapshot-v1-otlp-file.hex");
const KAFKA_GOLDEN: &str = include_str!("golden/admission-snapshot-v1-kafka.hex");

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn config_with(
    source_id: &str,
    kind: SourcePositionKindV0,
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    limits: AdmissionLimitsV0,
) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source(source_id),
        kind,
        epoch_start_ns,
        epoch_end_ns,
        limits,
    )
    .unwrap()
}

fn config(source_id: &str, kind: SourcePositionKindV0) -> AdmissionConfigV0 {
    config_with(source_id, kind, 0, 100, AdmissionLimitsV0::maximum())
}

fn canonical_event(source_id: &str, position: SourcePositionV0) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(source_id),
        SchemaIdV0::new("schema-v0").unwrap(),
        position,
        Some(b"k"),
        50,
        999,
        SensitivityBitmapV0::from_bits(5),
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([
            ("flag".to_owned(), ScalarValueV0::from_bool(true)),
            ("value".to_owned(), ScalarValueV0::from_u64(9)),
        ]))
        .unwrap(),
    )
    .unwrap()
}

fn push_u8(output: &mut Vec<u8>, value: u8) {
    output.push(value);
}

fn push_u16(output: &mut Vec<u8>, value: u16) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn push_u32(output: &mut Vec<u8>, value: u32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn push_u64(output: &mut Vec<u8>, value: u64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn push_i32(output: &mut Vec<u8>, value: i32) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn push_i64(output: &mut Vec<u8>, value: i64) {
    output.extend_from_slice(&value.to_be_bytes());
}

fn push_length_prefixed(output: &mut Vec<u8>, value: &[u8]) {
    push_u32(output, u32::try_from(value.len()).unwrap());
    output.extend_from_slice(value);
}

fn position_tag(kind: SourcePositionKindV0) -> u8 {
    match kind {
        SourcePositionKindV0::Synthetic => 0,
        SourcePositionKindV0::OtlpFile => 1,
        SourcePositionKindV0::Kafka => 2,
    }
}

fn push_position(output: &mut Vec<u8>, position: SourcePositionV0) {
    push_u8(output, position_tag(position.kind()));
    match position {
        SourcePositionV0::Synthetic { sequence } => push_u64(output, sequence),
        SourcePositionV0::OtlpFile { position } => {
            push_u64(output, position.line_number());
            push_u32(output, position.resource_spans_index());
            push_u32(output, position.scope_spans_index());
            push_u32(output, position.span_index());
        }
        SourcePositionV0::Kafka { position } => {
            push_i32(output, position.partition());
            push_i64(output, position.offset());
        }
    }
}

#[derive(Debug)]
struct SnapshotLayout {
    config_kind: usize,
    event_count: usize,
    record_lengths: Vec<usize>,
    event_tags: Vec<usize>,
    event_coordinates: Vec<usize>,
    scalar_tags: Vec<usize>,
}

fn independent_snapshot(
    admission_config: &AdmissionConfigV0,
    positions: &[SourcePositionV0],
    finalized: bool,
    canonical_payload_order: bool,
) -> (Vec<u8>, SnapshotLayout) {
    let mut output = Vec::new();
    output.extend_from_slice(b"DECANTST");
    push_u16(&mut output, 1);
    push_u16(&mut output, admission_config.version());
    push_i64(&mut output, ALLOWED_LATENESS_NS);
    push_length_prefixed(
        &mut output,
        admission_config.source_id().as_str().as_bytes(),
    );
    let config_kind = output.len();
    push_u8(
        &mut output,
        position_tag(admission_config.source_position_kind()),
    );
    push_i64(&mut output, admission_config.epoch_start_ns());
    push_i64(&mut output, admission_config.epoch_end_ns());
    push_u64(
        &mut output,
        u64::try_from(admission_config.limits().accepted_identity_limit()).unwrap(),
    );
    push_u64(
        &mut output,
        u64::try_from(admission_config.limits().accepted_event_byte_limit()).unwrap(),
    );
    push_u8(&mut output, u8::from(finalized));
    push_u32(&mut output, 8);
    for tag in 0_u8..8 {
        push_u8(&mut output, tag);
        push_u64(&mut output, 0);
        push_u8(&mut output, 0);
        push_u8(&mut output, 0);
    }
    let event_count = output.len();
    push_u32(&mut output, u32::try_from(positions.len()).unwrap());

    let mut record_lengths = Vec::new();
    let mut event_tags = Vec::new();
    let mut event_coordinates = Vec::new();
    let mut scalar_tags = Vec::new();
    for position in positions {
        let record_length = output.len();
        record_lengths.push(record_length);
        push_u32(&mut output, 0);
        let body_start = output.len();
        push_u16(&mut output, 0);
        event_tags.push(output.len());
        event_coordinates.push(output.len() + 1);
        push_position(&mut output, *position);
        push_length_prefixed(&mut output, b"schema-v0");
        push_u8(&mut output, 1);
        push_length_prefixed(&mut output, b"k");
        push_i64(&mut output, 50);
        push_u64(&mut output, 5);
        push_u32(&mut output, 2);
        let fields: [(&[u8], u8); 2] = if canonical_payload_order {
            [(b"flag", 1), (b"value", 3)]
        } else {
            [(b"value", 3), (b"flag", 1)]
        };
        for (name, scalar_tag) in fields {
            push_length_prefixed(&mut output, name);
            scalar_tags.push(output.len());
            push_u8(&mut output, scalar_tag);
            match scalar_tag {
                1 => push_u8(&mut output, 1),
                3 => push_u64(&mut output, 9),
                _ => unreachable!(),
            }
        }
        let body_length = u32::try_from(output.len() - body_start).unwrap();
        output[record_length..record_length + 4].copy_from_slice(&body_length.to_be_bytes());
    }
    (
        output,
        SnapshotLayout {
            config_kind,
            event_count,
            record_lengths,
            event_tags,
            event_coordinates,
            scalar_tags,
        },
    )
}

fn from_hex(input: &str) -> Vec<u8> {
    let bytes = input.trim().as_bytes();
    let (pairs, remainder) = bytes.as_chunks::<2>();
    assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| {
            let text = std::str::from_utf8(pair.as_slice()).unwrap();
            u8::from_str_radix(text, 16).unwrap()
        })
        .collect()
}

fn to_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(DIGITS[usize::from(byte >> 4)]));
        output.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    output
}

fn golden_cases() -> [(SourcePositionKindV0, SourcePositionV0, &'static str); 3] {
    [
        (
            SourcePositionKindV0::Synthetic,
            SourcePositionV0::Synthetic { sequence: 7 },
            SYNTHETIC_GOLDEN,
        ),
        (
            SourcePositionKindV0::OtlpFile,
            SourcePositionV0::otlp_file(7, 1, 2, 3).unwrap(),
            OTLP_FILE_GOLDEN,
        ),
        (
            SourcePositionKindV0::Kafka,
            SourcePositionV0::kafka(3, 7).unwrap(),
            KAFKA_GOLDEN,
        ),
    ]
}

#[test]
fn all_three_independent_golden_layouts_restore_and_reencode_exactly() {
    assert_eq!(ADMISSION_SNAPSHOT_VERSION_V1, 1);
    assert_eq!(
        MAX_ADMISSION_SNAPSHOT_BYTES_V1,
        MAX_ADMISSION_SNAPSHOT_BYTES_V0
    );
    for (kind, position, golden) in golden_cases() {
        let admission_config = config("source-v0", kind);
        let (independent, _) = independent_snapshot(&admission_config, &[position], false, true);
        assert_eq!(to_hex(&independent), golden.trim(), "{kind:?} golden");
        assert_eq!(from_hex(golden), independent);

        let restored = restore_admission_snapshot_v1(&independent, &admission_config).unwrap();
        assert_eq!(restored.config(), &admission_config);
        assert_eq!(restored.accepted_events().count(), 1);
        let accepted = restored.accepted_events().next().unwrap();
        assert_eq!(accepted.source_position(), position);
        assert_eq!(accepted.ingest_time_ns(), 0);
        assert_eq!(accepted.event_time_ns(), 50);
        assert_eq!(accepted.key(), Some(b"k".as_slice()));
        assert_eq!(accepted.sensitivity().bits(), 5);
        assert_eq!(
            accepted.provenance().as_str(),
            canonical_event("source-v0", position).provenance().as_str()
        );
        assert_eq!(restored.maximum_accepted_event_time_ns(), Some(50));
        assert_eq!(restored.watermark_ns(), Some(50 - ALLOWED_LATENESS_NS));
        assert_eq!(
            encode_admission_snapshot_v1(&restored).unwrap(),
            independent
        );
    }
}

#[test]
fn public_synthetic_apply_produces_the_independent_v1_golden() {
    let admission_config = config("source-v0", SourcePositionKindV0::Synthetic);
    let mut state = AdmissionStateV0::new(admission_config.clone());
    let candidate = canonical_event("source-v0", SourcePositionV0::Synthetic { sequence: 7 });
    assert_eq!(state.apply_event(&candidate), Ok(ApplyOutcomeV0::Accepted));
    assert_eq!(
        encode_admission_snapshot_v1(&state).unwrap(),
        from_hex(SYNTHETIC_GOLDEN)
    );
    assert_eq!(
        restore_admission_snapshot_v1(
            &encode_admission_snapshot_v1(&state).unwrap(),
            &admission_config,
        )
        .unwrap(),
        state
    );
}

#[test]
fn empty_open_and_finalized_states_of_each_kind_round_trip_exactly() {
    for kind in [
        SourcePositionKindV0::Synthetic,
        SourcePositionKindV0::OtlpFile,
        SourcePositionKindV0::Kafka,
    ] {
        let admission_config = config("source-v0", kind);
        let mut state = AdmissionStateV0::new(admission_config.clone());
        let open = encode_admission_snapshot_v1(&state).unwrap();
        assert_eq!(
            restore_admission_snapshot_v1(&open, &admission_config).unwrap(),
            state
        );
        assert_eq!(
            state.finalize_source(&source("source-v0"), 100),
            FinalizeOutcomeV0::Finalized
        );
        let finalized = encode_admission_snapshot_v1(&state).unwrap();
        assert_ne!(open, finalized);
        assert_eq!(
            restore_admission_snapshot_v1(&finalized, &admission_config).unwrap(),
            state
        );
    }
}

#[test]
fn synthetic_permutations_rejections_and_finality_have_one_canonical_snapshot() {
    let admission_config = config("source-v0", SourcePositionKindV0::Synthetic);
    let positions = [7, 4, 9, 2];
    let mut expected = None;
    for order in [[0, 1, 2, 3], [3, 2, 1, 0], [1, 3, 0, 2]] {
        let mut state = AdmissionStateV0::new(admission_config.clone());
        for index in order {
            assert_eq!(
                state.apply_event(&canonical_event(
                    "source-v0",
                    SourcePositionV0::Synthetic {
                        sequence: positions[index],
                    },
                )),
                Ok(ApplyOutcomeV0::Accepted)
            );
        }
        assert_eq!(
            state.apply_event(&canonical_event(
                "other-v0",
                SourcePositionV0::Synthetic { sequence: 100 },
            )),
            Ok(ApplyOutcomeV0::Rejected(
                RejectionReasonV0::UnexpectedSource
            ))
        );
        assert_eq!(
            state.finalize_source(&source("source-v0"), 100),
            FinalizeOutcomeV0::Finalized
        );
        let bytes = encode_admission_snapshot_v1(&state).unwrap();
        let restored = restore_admission_snapshot_v1(&bytes, &admission_config).unwrap();
        assert_eq!(restored, state);
        assert_eq!(restored.watermark_ns(), Some(100));
        assert_eq!(
            restored
                .rejection_summary()
                .range(RejectionReasonV0::UnexpectedSource)
                .count(),
            1
        );
        if let Some(expected) = &expected {
            assert_eq!(&bytes, expected);
        } else {
            expected = Some(bytes);
        }
    }
}

#[test]
fn expected_configuration_mismatch_precedes_event_count_and_is_value_safe() {
    let admission_config = config("secret-source-a", SourcePositionKindV0::Kafka);
    let (mut bytes, layout) = independent_snapshot(
        &admission_config,
        &[SourcePositionV0::kafka(3, 7).unwrap()],
        false,
        true,
    );
    bytes[layout.event_count..layout.event_count + 4].copy_from_slice(&u32::MAX.to_be_bytes());

    let cases = [
        (
            config("secret-source-b", SourcePositionKindV0::Synthetic),
            AdmissionSnapshotExpectedConfigFieldV1::SourceId,
        ),
        (
            config("secret-source-a", SourcePositionKindV0::Synthetic),
            AdmissionSnapshotExpectedConfigFieldV1::SourcePositionKind,
        ),
        (
            config_with(
                "secret-source-a",
                SourcePositionKindV0::Kafka,
                1,
                100,
                AdmissionLimitsV0::maximum(),
            ),
            AdmissionSnapshotExpectedConfigFieldV1::Epoch,
        ),
        (
            config_with(
                "secret-source-a",
                SourcePositionKindV0::Kafka,
                0,
                100,
                AdmissionLimitsV0::new(MAX_ACCEPTED_IDENTITIES_V0 - 1, MAX_ACCEPTED_EVENT_BYTES_V0)
                    .unwrap(),
            ),
            AdmissionSnapshotExpectedConfigFieldV1::AdmissionLimits,
        ),
        (
            config_with(
                "secret-source-a",
                SourcePositionKindV0::Kafka,
                0,
                100,
                AdmissionLimitsV0::new(MAX_ACCEPTED_IDENTITIES_V0, MAX_ACCEPTED_EVENT_BYTES_V0 - 1)
                    .unwrap(),
            ),
            AdmissionSnapshotExpectedConfigFieldV1::AdmissionLimits,
        ),
    ];
    for (expected, field) in cases {
        let error = restore_admission_snapshot_v1(&bytes, &expected).unwrap_err();
        assert_eq!(
            error,
            AdmissionSnapshotDecodeErrorV1::ExpectedConfigurationMismatch { field }
        );
        let debug = format!("{error:?}");
        let display = error.to_string();
        for output in [&debug, &display] {
            assert!(!output.contains("secret-source-a"));
            assert!(!output.contains("secret-source-b"));
        }
    }
    assert_eq!(
        restore_admission_snapshot_v1(&bytes, &admission_config).unwrap_err(),
        AdmissionSnapshotDecodeErrorV1::CapacityExceeded {
            field: AdmissionSnapshotFieldV1::EventCount,
            maximum: MAX_ACCEPTED_IDENTITIES_V0,
            attempted: u32::MAX as usize,
        }
    );
}

#[test]
fn versions_are_explicit_and_never_auto_routed() {
    let synthetic_config = config("source-v0", SourcePositionKindV0::Synthetic);
    let state = AdmissionStateV0::new(synthetic_config.clone());
    let v0 = encode_admission_snapshot_v0(&state).unwrap();
    let v1 = encode_admission_snapshot_v1(&state).unwrap();
    assert_ne!(v0, v1);
    assert_eq!(ADMISSION_SNAPSHOT_VERSION_V0, 0);
    assert_eq!(
        restore_admission_snapshot_v1(&v0, &synthetic_config).unwrap_err(),
        AdmissionSnapshotDecodeErrorV1::UnsupportedSnapshotVersion { actual: 0 }
    );
    assert_eq!(
        restore_admission_snapshot_v0(&v1),
        Err(AdmissionSnapshotDecodeErrorV0::UnsupportedSnapshotVersion { actual: 1 })
    );
}

#[test]
fn invalid_tags_coordinates_kinds_order_and_payload_fail_closed() {
    let otlp_config = config("source-v0", SourcePositionKindV0::OtlpFile);
    let otlp_position = SourcePositionV0::otlp_file(7, 1, 2, 3).unwrap();
    let (otlp, otlp_layout) = independent_snapshot(&otlp_config, &[otlp_position], false, true);

    let mut invalid_config_tag = otlp.clone();
    invalid_config_tag[otlp_layout.config_kind] = 3;
    assert_eq!(
        restore_admission_snapshot_v1(&invalid_config_tag, &otlp_config).unwrap_err(),
        AdmissionSnapshotDecodeErrorV1::InvalidTag {
            field: AdmissionSnapshotFieldV1::SourcePositionKind,
        }
    );

    let mut wrong_event_kind = otlp.clone();
    wrong_event_kind[otlp_layout.event_tags[0]] = 0;
    assert_eq!(
        restore_admission_snapshot_v1(&wrong_event_kind, &otlp_config).unwrap_err(),
        AdmissionSnapshotDecodeErrorV1::InvalidValue {
            field: AdmissionSnapshotFieldV1::SourcePosition,
        }
    );

    let mut zero_line = otlp.clone();
    zero_line[otlp_layout.event_coordinates[0]..otlp_layout.event_coordinates[0] + 8]
        .copy_from_slice(&0_u64.to_be_bytes());
    assert!(matches!(
        restore_admission_snapshot_v1(&zero_line, &otlp_config),
        Err(AdmissionSnapshotDecodeErrorV1::InvalidEvent(_))
    ));
    for (coordinate_offset, width, invalid) in [
        (
            0,
            8,
            (MAX_OTLP_FILE_LINE_NUMBER_V0 + 1).to_be_bytes().to_vec(),
        ),
        (
            8,
            4,
            (MAX_OTLP_FILE_RESOURCE_SPANS_INDEX_V0 + 1)
                .to_be_bytes()
                .to_vec(),
        ),
        (
            12,
            4,
            (MAX_OTLP_FILE_SCOPE_SPANS_INDEX_V0 + 1)
                .to_be_bytes()
                .to_vec(),
        ),
        (
            16,
            4,
            (MAX_OTLP_FILE_SPAN_INDEX_V0 + 1).to_be_bytes().to_vec(),
        ),
    ] {
        let mut invalid_coordinate = otlp.clone();
        let start = otlp_layout.event_coordinates[0] + coordinate_offset;
        invalid_coordinate[start..start + width].copy_from_slice(&invalid);
        assert!(matches!(
            restore_admission_snapshot_v1(&invalid_coordinate, &otlp_config),
            Err(AdmissionSnapshotDecodeErrorV1::InvalidEvent(_))
        ));
    }

    let kafka_config = config("source-v0", SourcePositionKindV0::Kafka);
    let (kafka, kafka_layout) = independent_snapshot(
        &kafka_config,
        &[SourcePositionV0::kafka(3, 7).unwrap()],
        false,
        true,
    );
    let mut negative_partition = kafka.clone();
    negative_partition[kafka_layout.event_coordinates[0]..kafka_layout.event_coordinates[0] + 4]
        .copy_from_slice(&(-1_i32).to_be_bytes());
    assert!(matches!(
        restore_admission_snapshot_v1(&negative_partition, &kafka_config),
        Err(AdmissionSnapshotDecodeErrorV1::InvalidEvent(_))
    ));
    for (coordinate_offset, width, invalid) in [
        (0, 4, (MAX_KAFKA_PARTITION_V0 + 1).to_be_bytes().to_vec()),
        (4, 8, (MAX_KAFKA_OFFSET_V0 + 1).to_be_bytes().to_vec()),
        (4, 8, (-1_i64).to_be_bytes().to_vec()),
    ] {
        let mut invalid_coordinate = kafka.clone();
        let start = kafka_layout.event_coordinates[0] + coordinate_offset;
        invalid_coordinate[start..start + width].copy_from_slice(&invalid);
        assert!(matches!(
            restore_admission_snapshot_v1(&invalid_coordinate, &kafka_config),
            Err(AdmissionSnapshotDecodeErrorV1::InvalidEvent(_))
        ));
    }

    for positions in [
        vec![
            SourcePositionV0::Synthetic { sequence: 7 },
            SourcePositionV0::Synthetic { sequence: 7 },
        ],
        vec![
            SourcePositionV0::Synthetic { sequence: 8 },
            SourcePositionV0::Synthetic { sequence: 7 },
        ],
    ] {
        let synthetic_config = config("source-v0", SourcePositionKindV0::Synthetic);
        let (noncanonical, _) = independent_snapshot(&synthetic_config, &positions, false, true);
        assert_eq!(
            restore_admission_snapshot_v1(&noncanonical, &synthetic_config).unwrap_err(),
            AdmissionSnapshotDecodeErrorV1::NonCanonicalOrder {
                order: AdmissionSnapshotOrderV1::AcceptedEvents,
            }
        );
    }

    let (noncanonical_payload, _) =
        independent_snapshot(&otlp_config, &[otlp_position], false, false);
    assert_eq!(
        restore_admission_snapshot_v1(&noncanonical_payload, &otlp_config).unwrap_err(),
        AdmissionSnapshotDecodeErrorV1::NonCanonicalOrder {
            order: AdmissionSnapshotOrderV1::PayloadFields,
        }
    );
}

#[test]
fn framing_record_scalar_and_document_bounds_fail_closed() {
    let admission_config = config("source-v0", SourcePositionKindV0::Synthetic);
    let (bytes, layout) = independent_snapshot(
        &admission_config,
        &[SourcePositionV0::Synthetic { sequence: 7 }],
        false,
        true,
    );
    for length in 0..bytes.len() {
        assert!(
            restore_admission_snapshot_v1(&bytes[..length], &admission_config).is_err(),
            "prefix {length} restored"
        );
    }
    assert_eq!(
        restore_admission_snapshot_v1(&bytes[..layout.config_kind], &admission_config).unwrap_err(),
        AdmissionSnapshotDecodeErrorV1::Truncated {
            field: AdmissionSnapshotFieldV1::SourcePositionKind,
        }
    );

    let mut trailing = bytes.clone();
    trailing.push(0);
    assert_eq!(
        restore_admission_snapshot_v1(&trailing, &admission_config).unwrap_err(),
        AdmissionSnapshotDecodeErrorV1::TrailingBytes
    );

    let mut bad_record_length = bytes.clone();
    let record_length = layout.record_lengths[0];
    bad_record_length[record_length..record_length + 4].copy_from_slice(&1_u32.to_be_bytes());
    assert!(restore_admission_snapshot_v1(&bad_record_length, &admission_config).is_err());

    let mut invalid_scalar = bytes;
    invalid_scalar[layout.scalar_tags[0]] = 0xff;
    assert_eq!(
        restore_admission_snapshot_v1(&invalid_scalar, &admission_config).unwrap_err(),
        AdmissionSnapshotDecodeErrorV1::InvalidTag {
            field: AdmissionSnapshotFieldV1::Scalar,
        }
    );

    let oversized = vec![0_u8; MAX_ADMISSION_SNAPSHOT_BYTES_V1 + 1];
    assert_eq!(
        restore_admission_snapshot_v1(&oversized, &admission_config).unwrap_err(),
        AdmissionSnapshotDecodeErrorV1::DocumentTooLarge {
            maximum: MAX_ADMISSION_SNAPSHOT_BYTES_V1,
            actual: MAX_ADMISSION_SNAPSHOT_BYTES_V1 + 1,
        }
    );
}

#[test]
fn restored_external_state_does_not_activate_external_apply() {
    for (kind, position, _) in golden_cases().into_iter().skip(1) {
        let admission_config = config("source-v0", kind);
        let (bytes, _) = independent_snapshot(&admission_config, &[position], false, true);
        let mut state = restore_admission_snapshot_v1(&bytes, &admission_config).unwrap();
        let before = encode_admission_snapshot_v1(&state).unwrap();
        assert_eq!(
            state.apply_event(&canonical_event("source-v0", position)),
            Err(AdmissionErrorV0::UnsupportedSourcePosition { kind })
        );
        let other = match kind {
            SourcePositionKindV0::OtlpFile => SourcePositionV0::kafka(3, 7).unwrap(),
            SourcePositionKindV0::Kafka => SourcePositionV0::otlp_file(7, 1, 2, 3).unwrap(),
            SourcePositionKindV0::Synthetic => unreachable!(),
        };
        assert_eq!(
            state.apply_event(&canonical_event("source-v0", other)),
            Err(AdmissionErrorV0::UnexpectedSourcePositionKind {
                expected: kind,
                actual: other.kind(),
            })
        );
        assert_eq!(encode_admission_snapshot_v1(&state).unwrap(), before);
    }
}
