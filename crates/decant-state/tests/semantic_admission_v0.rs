use std::collections::BTreeMap;

use decant_fixtures::{
    FixtureRecordV0, GENERATOR_VERSION_V0, SreGeneratorV0, decode_scenario_manifest_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, ALLOWED_LATENESS_NS_V0, AdmissionConfigErrorV0, AdmissionConfigV0,
    AdmissionErrorV0, AdmissionLimitFieldV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    FinalizeOutcomeV0, MAX_ACCEPTED_EVENT_BYTES_V0, MAX_ACCEPTED_IDENTITIES_V0,
    REJECTION_CLASS_COUNT_V0, RejectionReasonV0, accepted_event_accounted_bytes_v0,
    encode_admission_snapshot_v0, hash_admission_state_v0,
};

const SECOND_NS: i64 = 1_000_000_000;
const PRIMARY_START_NS: i64 = 1_767_225_600_000_000_000;
const PRIMARY_END_NS: i64 = 1_767_226_800_000_000_000;
const PRIMARY_SCENARIO: &[u8] =
    include_bytes!("../../decant-fixtures/contracts/sre-v0/scenario.json");

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn payload(value: ScalarValueV0) -> CanonicalPayloadV0 {
    CanonicalPayloadV0::try_from_fields(BTreeMap::from([("value".to_owned(), value)])).unwrap()
}

fn event(
    source_id: &str,
    sequence: u64,
    times_ns: (i64, i64),
    schema_id: &str,
    key: Option<&[u8]>,
    sensitivity: u64,
    value: ScalarValueV0,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(source_id),
        SchemaIdV0::new(schema_id).unwrap(),
        SourcePositionV0::Synthetic { sequence },
        key,
        times_ns.0,
        times_ns.1,
        SensitivityBitmapV0::from_bits(sensitivity),
        payload(value),
    )
    .unwrap()
}

fn basic_event(source_id: &str, sequence: u64, event_time_ns: i64) -> EventEnvelopeV0 {
    event(
        source_id,
        sequence,
        (event_time_ns, event_time_ns),
        "schema-v0",
        Some(b"key"),
        0,
        ScalarValueV0::from_u64(sequence),
    )
}

fn otlp_event(source_id: &str, line_number: u64) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(source_id),
        SchemaIdV0::new("otlp-span-v0").unwrap(),
        SourcePositionV0::otlp_file(line_number, 1, 2, 3).unwrap(),
        Some(&[9; 16]),
        50,
        0,
        SensitivityBitmapV0::from_bits(u64::MAX),
        payload(ScalarValueV0::utf8("charge").unwrap()),
    )
    .unwrap()
}

fn kafka_event(source_id: &str, partition: i32, offset: i64) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(source_id),
        SchemaIdV0::new("kafka-json-v0").unwrap(),
        SourcePositionV0::kafka(partition, offset).unwrap(),
        Some(b"payments-v2/region-b"),
        50,
        0,
        SensitivityBitmapV0::from_bits(u64::MAX),
        payload(ScalarValueV0::utf8("error").unwrap()),
    )
    .unwrap()
}

fn config(
    source_id: &str,
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    limits: AdmissionLimitsV0,
) -> AdmissionConfigV0 {
    AdmissionConfigV0::new(
        ADMISSION_VERSION_V0,
        source(source_id),
        epoch_start_ns,
        epoch_end_ns,
        limits,
    )
    .unwrap()
}

fn generous_config(source_id: &str, start: i64, end: i64) -> AdmissionConfigV0 {
    config(source_id, start, end, AdmissionLimitsV0::maximum())
}

fn independent_accounted_bytes(event: &EventEnvelopeV0) -> usize {
    let source_position_bytes = match event.source_position() {
        SourcePositionV0::Synthetic { .. } => 1 + 8,
        SourcePositionV0::OtlpFile { .. } => 1 + 8 + 4 + 4 + 4,
        SourcePositionV0::Kafka { .. } => 1 + 4 + 8,
    };
    2 + 4
        + event.source_id().as_str().len()
        + 4
        + event.schema_id().as_str().len()
        + source_position_bytes
        + 1
        + event.key().map_or(0, |key| 4 + key.len())
        + 8
        + 8
        + 8
        + 4
        + event.payload().logical_len()
        + 4
        + event.provenance().as_str().len()
}

#[test]
fn otlp_file_position_is_accounted_but_matching_source_admission_is_non_mutating() {
    let admission_config = generous_config("otel-demo-capture", 0, 100);
    let expected = AdmissionStateV0::new(admission_config.clone());
    let expected_snapshot = encode_admission_snapshot_v0(&expected).unwrap();
    let expected_hash = hash_admission_state_v0(&expected).unwrap();
    let candidate = otlp_event("otel-demo-capture", 7);
    assert_eq!(
        accepted_event_accounted_bytes_v0(&candidate).unwrap(),
        independent_accounted_bytes(&candidate)
    );

    let mut state = AdmissionStateV0::new(admission_config);
    for _ in 0..2 {
        assert_eq!(
            state.apply_event(&candidate),
            Err(AdmissionErrorV0::UnsupportedSourcePosition {
                kind: SourcePositionKindV0::OtlpFile,
            })
        );
        assert_eq!(state, expected);
        assert_eq!(
            encode_admission_snapshot_v0(&state).unwrap(),
            expected_snapshot
        );
        assert_eq!(hash_admission_state_v0(&state).unwrap(), expected_hash);
    }

    assert_eq!(
        state.finalize_source(&source("otel-demo-capture"), 100),
        FinalizeOutcomeV0::Finalized
    );
    assert_eq!(
        state.apply_event(&candidate),
        Err(AdmissionErrorV0::UnsupportedSourcePosition {
            kind: SourcePositionKindV0::OtlpFile,
        })
    );
}

#[test]
fn kafka_position_is_accounted_but_matching_source_admission_is_non_mutating() {
    let admission_config = generous_config("payments-kafka-v0", 0, 100);
    let expected = AdmissionStateV0::new(admission_config.clone());
    let expected_snapshot = encode_admission_snapshot_v0(&expected).unwrap();
    let expected_hash = hash_admission_state_v0(&expected).unwrap();
    let candidate = kafka_event("payments-kafka-v0", 23, 42);
    assert_eq!(
        accepted_event_accounted_bytes_v0(&candidate).unwrap(),
        independent_accounted_bytes(&candidate)
    );

    let mut state = AdmissionStateV0::new(admission_config);
    for _ in 0..2 {
        assert_eq!(
            state.apply_event(&candidate),
            Err(AdmissionErrorV0::UnsupportedSourcePosition {
                kind: SourcePositionKindV0::Kafka,
            })
        );
        assert_eq!(state, expected);
        assert_eq!(
            encode_admission_snapshot_v0(&state).unwrap(),
            expected_snapshot
        );
        assert_eq!(hash_admission_state_v0(&state).unwrap(), expected_hash);
    }

    assert_eq!(
        state.finalize_source(&source("payments-kafka-v0"), 100),
        FinalizeOutcomeV0::Finalized
    );
    assert_eq!(
        state.apply_event(&candidate),
        Err(AdmissionErrorV0::UnsupportedSourcePosition {
            kind: SourcePositionKindV0::Kafka,
        })
    );
}

#[test]
fn tagged_position_collision_cannot_become_a_redelivery() {
    let mut state = AdmissionStateV0::new(generous_config("source-v0", 0, 100));
    let synthetic = basic_event("source-v0", 0, 50);
    assert_eq!(state.apply_event(&synthetic), Ok(ApplyOutcomeV0::Accepted));

    let external = EventEnvelopeV0::new(
        0,
        source("source-v0"),
        SchemaIdV0::new("otlp-span-v0").unwrap(),
        SourcePositionV0::otlp_file(1, 0, 0, 0).unwrap(),
        Some(&[9; 16]),
        50,
        0,
        SensitivityBitmapV0::from_bits(u64::MAX),
        payload(ScalarValueV0::utf8("charge").unwrap()),
    )
    .unwrap();
    assert_eq!(synthetic.source_position().source_ordering_ordinal(), 0);
    assert_eq!(external.source_position().source_ordering_ordinal(), 0);
    assert_ne!(synthetic.source_position(), external.source_position());

    let expected_snapshot = encode_admission_snapshot_v0(&state).unwrap();
    let expected_hash = hash_admission_state_v0(&state).unwrap();
    let expected_usage = state.usage();
    assert_eq!(
        state.apply_event(&external),
        Err(AdmissionErrorV0::UnsupportedSourcePosition {
            kind: SourcePositionKindV0::OtlpFile,
        })
    );
    assert_eq!(state.usage(), expected_usage);
    assert_eq!(
        encode_admission_snapshot_v0(&state).unwrap(),
        expected_snapshot
    );
    assert_eq!(hash_admission_state_v0(&state).unwrap(), expected_hash);
    assert_eq!(state.accepted_events().count(), 1);
    assert_eq!(
        state.accepted_events().next().unwrap().source_position(),
        SourcePositionV0::Synthetic { sequence: 0 }
    );
}

#[test]
fn unexpected_source_precedes_otlp_position_compatibility_check() {
    let mut state = AdmissionStateV0::new(generous_config("configured-source", 0, 100));
    assert_eq!(
        state.apply_event(&otlp_event("other-source", 7)).unwrap(),
        ApplyOutcomeV0::Rejected(RejectionReasonV0::UnexpectedSource)
    );
    assert_eq!(state.usage().accepted_identities(), 0);
    assert_eq!(
        state
            .rejection_summary()
            .range(RejectionReasonV0::UnexpectedSource)
            .count(),
        1
    );
}

fn accepted_events(state: &AdmissionStateV0) -> Vec<EventEnvelopeV0> {
    state.accepted_events().cloned().collect()
}

#[test]
fn config_enforces_version_epoch_and_bounded_limits() {
    let minimum = AdmissionLimitsV0::new(1, 1).unwrap();
    let maximum =
        AdmissionLimitsV0::new(MAX_ACCEPTED_IDENTITIES_V0, MAX_ACCEPTED_EVENT_BYTES_V0).unwrap();
    assert_eq!(minimum.accepted_identity_limit(), 1);
    assert_eq!(maximum, AdmissionLimitsV0::maximum());
    assert!(AdmissionConfigV0::new(0, source("source-v0"), 0, 1, minimum).is_ok());
    assert!(AdmissionConfigV0::new(0, source("source-v0"), 0, 1, maximum).is_ok());
    assert_eq!(
        AdmissionLimitsV0::new(0, 1),
        Err(AdmissionConfigErrorV0::ZeroLimit {
            field: AdmissionLimitFieldV0::AcceptedIdentities,
        })
    );
    assert_eq!(
        AdmissionLimitsV0::new(1, 0),
        Err(AdmissionConfigErrorV0::ZeroLimit {
            field: AdmissionLimitFieldV0::AcceptedEventBytes,
        })
    );
    assert_eq!(
        AdmissionLimitsV0::new(MAX_ACCEPTED_IDENTITIES_V0 + 1, 1),
        Err(AdmissionConfigErrorV0::AboveMaximum {
            field: AdmissionLimitFieldV0::AcceptedIdentities,
            maximum: MAX_ACCEPTED_IDENTITIES_V0,
            actual: MAX_ACCEPTED_IDENTITIES_V0 + 1,
        })
    );
    assert_eq!(
        AdmissionLimitsV0::new(1, MAX_ACCEPTED_EVENT_BYTES_V0 + 1),
        Err(AdmissionConfigErrorV0::AboveMaximum {
            field: AdmissionLimitFieldV0::AcceptedEventBytes,
            maximum: MAX_ACCEPTED_EVENT_BYTES_V0,
            actual: MAX_ACCEPTED_EVENT_BYTES_V0 + 1,
        })
    );

    let limits = AdmissionLimitsV0::new(1, 1).unwrap();
    assert_eq!(
        AdmissionConfigV0::new(1, source("source-v0"), 0, 1, limits),
        Err(AdmissionConfigErrorV0::UnsupportedVersion { actual: 1 })
    );
    assert_eq!(
        AdmissionConfigV0::new(0, source("source-v0"), 1, 1, limits),
        Err(AdmissionConfigErrorV0::InvalidEpoch)
    );
    assert_eq!(
        AdmissionConfigV0::new(0, source("source-v0"), 2, 1, limits),
        Err(AdmissionConfigErrorV0::InvalidEpoch)
    );
}

#[test]
fn accounting_matches_independent_formula_for_keys_and_every_scalar_kind() {
    let values = vec![
        ScalarValueV0::null(),
        ScalarValueV0::from_bool(true),
        ScalarValueV0::from_i64(-1),
        ScalarValueV0::from_u64(1),
        ScalarValueV0::utf8("text").unwrap(),
        ScalarValueV0::bytes(b"bytes").unwrap(),
    ];
    for (index, value) in values.into_iter().enumerate() {
        let candidate = event(
            "source-v0",
            index as u64,
            (0, 1),
            "schema-v0",
            Some(b"key"),
            7,
            value,
        );
        assert_eq!(
            accepted_event_accounted_bytes_v0(&candidate).unwrap(),
            independent_accounted_bytes(&candidate)
        );
    }

    for key in [None, Some(&[][..]), Some(&vec![b'x'; 256][..])] {
        let candidate = event(
            "source-v0",
            99,
            (0, 1),
            "schema-v0",
            key,
            0,
            ScalarValueV0::null(),
        );
        assert_eq!(
            accepted_event_accounted_bytes_v0(&candidate).unwrap(),
            independent_accounted_bytes(&candidate)
        );
    }
}

#[test]
fn watermark_is_saturating_monotonic_and_accepts_the_late_boundary() {
    let mut minimum_state = AdmissionStateV0::new(generous_config(
        "source-v0",
        i64::MIN,
        i64::MIN + 100 * SECOND_NS,
    ));
    assert_eq!(
        minimum_state.apply_event(&basic_event("source-v0", 1, i64::MIN)),
        Ok(ApplyOutcomeV0::Accepted)
    );
    assert_eq!(
        minimum_state.maximum_accepted_event_time_ns(),
        Some(i64::MIN)
    );
    assert_eq!(minimum_state.watermark_ns(), Some(i64::MIN));

    let mut state = AdmissionStateV0::new(generous_config("source-v0", 0, 200 * SECOND_NS));
    assert_eq!(
        state.apply_event(&basic_event("source-v0", 1, 60 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Accepted)
    );
    assert_eq!(state.watermark_ns(), Some(30 * SECOND_NS));
    assert_eq!(
        state.apply_event(&basic_event("source-v0", 2, 100 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Accepted)
    );
    assert_eq!(state.watermark_ns(), Some(70 * SECOND_NS));
    assert_eq!(
        state.apply_event(&basic_event("source-v0", 3, 80 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Accepted)
    );
    assert_eq!(state.watermark_ns(), Some(70 * SECOND_NS));
    assert_eq!(
        state.apply_event(&basic_event("source-v0", 4, 70 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Accepted)
    );

    let accepted_before_late = accepted_events(&state);
    let usage_before_late = state.usage();
    assert_eq!(
        state.apply_event(&basic_event("source-v0", 5, 70 * SECOND_NS - 1)),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::Late))
    );
    assert_eq!(accepted_events(&state), accepted_before_late);
    assert_eq!(
        state.usage().accepted_identities(),
        usage_before_late.accepted_identities()
    );
    assert_eq!(
        state.usage().accepted_event_bytes(),
        usage_before_late.accepted_event_bytes()
    );
    assert_eq!(
        state.maximum_accepted_event_time_ns(),
        Some(100 * SECOND_NS)
    );
    assert_eq!(state.watermark_ns(), Some(70 * SECOND_NS));
}

#[test]
fn redelivery_precedes_epoch_and_lateness_and_never_replaces_retained_event() {
    let original = event(
        "source-v0",
        1,
        (10 * SECOND_NS, 11),
        "schema-v0",
        Some(b"key-a"),
        1,
        ScalarValueV0::utf8("value-a").unwrap(),
    );
    let mut state = AdmissionStateV0::new(generous_config("source-v0", 0, 200 * SECOND_NS));
    assert_eq!(state.apply_event(&original), Ok(ApplyOutcomeV0::Accepted));
    assert_eq!(
        state.apply_event(&basic_event("source-v0", 2, 100 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Accepted)
    );
    let retained_before = accepted_events(&state);
    let usage_before = state.usage();
    let maximum_before = state.maximum_accepted_event_time_ns();
    let watermark_before = state.watermark_ns();

    let ingest_only = event(
        "source-v0",
        1,
        (10 * SECOND_NS, i64::MAX),
        "schema-v0",
        Some(b"key-a"),
        1,
        ScalarValueV0::utf8("value-a").unwrap(),
    );
    assert_eq!(
        state.apply_event(&ingest_only),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    assert_eq!(accepted_events(&state), retained_before);
    assert_eq!(state.usage(), usage_before);
    assert_eq!(state.maximum_accepted_event_time_ns(), maximum_before);
    assert_eq!(state.watermark_ns(), watermark_before);

    let conflicts = [
        event(
            "source-v0",
            1,
            (10 * SECOND_NS, 11),
            "schema-v1",
            Some(b"key-a"),
            1,
            ScalarValueV0::utf8("value-a").unwrap(),
        ),
        event(
            "source-v0",
            1,
            (10 * SECOND_NS, 11),
            "schema-v0",
            Some(b"key-b"),
            1,
            ScalarValueV0::utf8("value-a").unwrap(),
        ),
        event(
            "source-v0",
            1,
            (-1, 11),
            "schema-v0",
            Some(b"key-a"),
            1,
            ScalarValueV0::utf8("value-a").unwrap(),
        ),
        event(
            "source-v0",
            1,
            (10 * SECOND_NS, 11),
            "schema-v0",
            Some(b"key-a"),
            2,
            ScalarValueV0::utf8("value-a").unwrap(),
        ),
        event(
            "source-v0",
            1,
            (10 * SECOND_NS, 11),
            "schema-v0",
            Some(b"key-a"),
            1,
            ScalarValueV0::utf8("value-b").unwrap(),
        ),
    ];
    for conflict in conflicts {
        assert_eq!(
            state.apply_event(&conflict),
            Ok(ApplyOutcomeV0::Rejected(
                RejectionReasonV0::ConflictingRedelivery
            ))
        );
        assert_eq!(accepted_events(&state), retained_before);
        assert_eq!(
            state.usage().accepted_identities(),
            usage_before.accepted_identities()
        );
        assert_eq!(
            state.usage().accepted_event_bytes(),
            usage_before.accepted_event_bytes()
        );
        assert_eq!(state.maximum_accepted_event_time_ns(), maximum_before);
        assert_eq!(state.watermark_ns(), watermark_before);
    }
}

#[test]
fn source_epoch_and_capacity_rejections_leave_accepted_state_unchanged() {
    let first = basic_event("source-v0", 1, 50 * SECOND_NS);
    let first_bytes = accepted_event_accounted_bytes_v0(&first).unwrap();

    let mut entry_limited = AdmissionStateV0::new(config(
        "source-v0",
        0,
        200 * SECOND_NS,
        AdmissionLimitsV0::new(1, MAX_ACCEPTED_EVENT_BYTES_V0).unwrap(),
    ));
    assert_eq!(
        entry_limited.apply_event(&first),
        Ok(ApplyOutcomeV0::Accepted)
    );
    let entry_snapshot = accepted_events(&entry_limited);
    assert_eq!(
        entry_limited.apply_event(&basic_event("source-v0", 2, 60 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedIdentityCapacity
        ))
    );
    assert_eq!(accepted_events(&entry_limited), entry_snapshot);

    let mut byte_limited = AdmissionStateV0::new(config(
        "source-v0",
        0,
        200 * SECOND_NS,
        AdmissionLimitsV0::new(2, first_bytes).unwrap(),
    ));
    assert_eq!(
        byte_limited.apply_event(&first),
        Ok(ApplyOutcomeV0::Accepted)
    );
    assert_eq!(byte_limited.usage().accepted_event_bytes(), first_bytes);
    let byte_snapshot = accepted_events(&byte_limited);
    assert_eq!(
        byte_limited.apply_event(&basic_event("source-v0", 2, 60 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedEventByteCapacity
        ))
    );
    assert_eq!(accepted_events(&byte_limited), byte_snapshot);

    let mut state = AdmissionStateV0::new(generous_config("source-v0", 0, 200 * SECOND_NS));
    assert_eq!(
        state.apply_event(&basic_event("source-x0", 1, 50 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::UnexpectedSource
        ))
    );
    assert_eq!(
        state.apply_event(&basic_event("source-v0", 1, 200 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch))
    );
    assert_eq!(state.usage().accepted_identities(), 0);
    assert_eq!(state.usage().accepted_event_bytes(), 0);
    assert_eq!(state.maximum_accepted_event_time_ns(), None);
    assert_eq!(state.watermark_ns(), None);
}

#[test]
fn rejection_evidence_has_eight_fixed_classes_and_exact_ranges() {
    let mut state = AdmissionStateV0::new(generous_config("source-v0", 0, 200 * SECOND_NS));
    assert_eq!(
        state.rejection_summary().class_count(),
        REJECTION_CLASS_COUNT_V0
    );
    assert_eq!(state.usage().rejection_classes(), REJECTION_CLASS_COUNT_V0);

    assert_eq!(
        state.apply_event(&basic_event("source-x0", 1, 4)),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::UnexpectedSource
        ))
    );
    assert_eq!(
        state.apply_event(&basic_event("source-x0", 2, 2)),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::UnexpectedSource
        ))
    );
    assert_eq!(
        state.apply_event(&basic_event("source-x0", 3, 6)),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::UnexpectedSource
        ))
    );
    let range = state
        .rejection_summary()
        .range(RejectionReasonV0::UnexpectedSource);
    assert_eq!(range.count(), 3);
    assert!(!range.count_saturated());
    assert_eq!(range.minimum_attempted_time_ns(), Some(2));
    assert_eq!(range.maximum_attempted_time_ns(), Some(6));

    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS - 1),
        FinalizeOutcomeV0::Rejected(RejectionReasonV0::InvalidFinalization)
    );
    let original = basic_event("source-v0", 10, 100 * SECOND_NS);
    assert_eq!(state.apply_event(&original), Ok(ApplyOutcomeV0::Accepted));
    let conflict = event(
        "source-v0",
        10,
        (100 * SECOND_NS, 100 * SECOND_NS),
        "schema-v0",
        Some(b"key"),
        0,
        ScalarValueV0::from_u64(11),
    );
    for (candidate, expected) in [
        (conflict, RejectionReasonV0::ConflictingRedelivery),
        (
            basic_event("source-v0", 11, 200 * SECOND_NS),
            RejectionReasonV0::OutsideEpoch,
        ),
        (
            basic_event("source-v0", 12, 70 * SECOND_NS - 1),
            RejectionReasonV0::Late,
        ),
    ] {
        assert_eq!(
            state.apply_event(&candidate),
            Ok(ApplyOutcomeV0::Rejected(expected))
        );
        assert_eq!(state.rejection_summary().class_count(), 8);
    }
    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    assert_eq!(
        state.apply_event(&basic_event("source-v0", 13, 100 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::EpochFinalized))
    );

    let accepted = basic_event("source-v0", 1, 50 * SECOND_NS);
    let accepted_bytes = accepted_event_accounted_bytes_v0(&accepted).unwrap();
    let mut entry_limited = AdmissionStateV0::new(config(
        "source-v0",
        0,
        200 * SECOND_NS,
        AdmissionLimitsV0::new(1, MAX_ACCEPTED_EVENT_BYTES_V0).unwrap(),
    ));
    assert_eq!(
        entry_limited.apply_event(&accepted),
        Ok(ApplyOutcomeV0::Accepted)
    );
    assert_eq!(
        entry_limited.apply_event(&basic_event("source-v0", 2, 60 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedIdentityCapacity
        ))
    );
    let mut byte_limited = AdmissionStateV0::new(config(
        "source-v0",
        0,
        200 * SECOND_NS,
        AdmissionLimitsV0::new(2, accepted_bytes).unwrap(),
    ));
    assert_eq!(
        byte_limited.apply_event(&accepted),
        Ok(ApplyOutcomeV0::Accepted)
    );
    assert_eq!(
        byte_limited.apply_event(&basic_event("source-v0", 2, 60 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedEventByteCapacity
        ))
    );
    assert_eq!(entry_limited.rejection_summary().class_count(), 8);
    assert_eq!(byte_limited.rejection_summary().class_count(), 8);

    for reason in RejectionReasonV0::ALL {
        let _ = state.rejection_summary().range(reason);
    }
    assert_eq!(state.rejection_summary().class_count(), 8);
}

#[test]
fn ingest_normalization_makes_delivery_order_semantically_identical() {
    let early_ingest = event(
        "source-v0",
        1,
        (50 * SECOND_NS, 1),
        "schema-v0",
        Some(b"key"),
        0,
        ScalarValueV0::from_u64(1),
    );
    let late_ingest = event(
        "source-v0",
        1,
        (50 * SECOND_NS, i64::MAX),
        "schema-v0",
        Some(b"key"),
        0,
        ScalarValueV0::from_u64(1),
    );
    let mut a = AdmissionStateV0::new(generous_config("source-v0", 0, 200 * SECOND_NS));
    let mut b = AdmissionStateV0::new(generous_config("source-v0", 0, 200 * SECOND_NS));
    assert_eq!(a.apply_event(&early_ingest), Ok(ApplyOutcomeV0::Accepted));
    assert_eq!(
        a.apply_event(&late_ingest),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    assert_eq!(b.apply_event(&late_ingest), Ok(ApplyOutcomeV0::Accepted));
    assert_eq!(
        b.apply_event(&early_ingest),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    assert_eq!(a, b);
    assert!(
        a.accepted_events()
            .all(|accepted| accepted.ingest_time_ns() == 0)
    );
}

#[test]
fn finalization_is_exact_idempotent_and_closes_the_epoch() {
    let configured_source = source("source-v0");
    let wrong_source = source("source-x0");
    let epoch_end = 200 * SECOND_NS;
    let mut state = AdmissionStateV0::new(generous_config("source-v0", 0, epoch_end));
    assert_eq!(
        state.finalize_source(&wrong_source, epoch_end),
        FinalizeOutcomeV0::Rejected(RejectionReasonV0::UnexpectedSource)
    );
    assert_eq!(state.watermark_ns(), None);
    assert_eq!(
        state.finalize_source(&configured_source, epoch_end - 1),
        FinalizeOutcomeV0::Rejected(RejectionReasonV0::InvalidFinalization)
    );
    assert_eq!(state.watermark_ns(), None);
    let retained = basic_event("source-v0", 1, 50 * SECOND_NS);
    assert_eq!(state.apply_event(&retained), Ok(ApplyOutcomeV0::Accepted));
    assert_eq!(
        state.finalize_source(&configured_source, epoch_end),
        FinalizeOutcomeV0::Finalized
    );
    assert!(state.is_finalized());
    assert_eq!(state.watermark_ns(), Some(epoch_end));
    let summary = state.rejection_summary().clone();
    assert_eq!(
        state.finalize_source(&configured_source, epoch_end),
        FinalizeOutcomeV0::AlreadyFinalizedNoOp
    );
    assert_eq!(state.rejection_summary(), &summary);
    assert_eq!(
        state.apply_event(&retained),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::EpochFinalized))
    );
    assert_eq!(state.usage().accepted_identities(), 1);
    assert_eq!(
        state.finalize_source(&configured_source, epoch_end - 1),
        FinalizeOutcomeV0::Rejected(RejectionReasonV0::InvalidFinalization)
    );
}

fn apply_order(order: &[usize]) -> (AdmissionStateV0, Vec<ApplyOutcomeV0>) {
    let events = [
        basic_event("source-v0", 1, 60 * SECOND_NS),
        basic_event("source-v0", 2, 70 * SECOND_NS),
        basic_event("source-v0", 3, 80 * SECOND_NS),
        basic_event("source-v0", 4, 90 * SECOND_NS),
    ];
    let mut state = AdmissionStateV0::new(generous_config("source-v0", 0, 200 * SECOND_NS));
    let outcomes = order
        .iter()
        .map(|index| state.apply_event(&events[*index]).unwrap())
        .collect();
    (state, outcomes)
}

#[test]
fn bounded_disorder_permutations_and_replay_are_deterministic() {
    let orders = [
        [0, 1, 2, 3],
        [0, 1, 3, 2],
        [0, 2, 1, 3],
        [0, 2, 3, 1],
        [0, 3, 1, 2],
        [0, 3, 2, 1],
        [1, 0, 2, 3],
        [1, 0, 3, 2],
        [1, 2, 0, 3],
        [1, 2, 3, 0],
        [1, 3, 0, 2],
        [1, 3, 2, 0],
        [2, 0, 1, 3],
        [2, 0, 3, 1],
        [2, 1, 0, 3],
        [2, 1, 3, 0],
        [2, 3, 0, 1],
        [2, 3, 1, 0],
        [3, 0, 1, 2],
        [3, 0, 2, 1],
        [3, 1, 0, 2],
        [3, 1, 2, 0],
        [3, 2, 0, 1],
        [3, 2, 1, 0],
    ];
    let (expected, _) = apply_order(&orders[0]);
    for order in orders {
        let (first, first_outcomes) = apply_order(&order);
        let (replayed, replayed_outcomes) = apply_order(&order);
        assert_eq!(first_outcomes, vec![ApplyOutcomeV0::Accepted; 4]);
        assert_eq!(first_outcomes, replayed_outcomes);
        assert_eq!(first, replayed);
        assert_eq!(first, expected);
        assert_eq!(first.maximum_accepted_event_time_ns(), Some(90 * SECOND_NS));
        assert_eq!(
            first.watermark_ns(),
            Some(90 * SECOND_NS - ALLOWED_LATENESS_NS_V0)
        );
    }
}

#[test]
fn primary_sre_stream_matches_the_approved_admission_oracle() {
    let manifest = decode_scenario_manifest_v0(PRIMARY_SCENARIO).unwrap();
    let generator = SreGeneratorV0::new(GENERATOR_VERSION_V0, &manifest, "sre-v0-primary").unwrap();
    let mut state =
        AdmissionStateV0::new(generous_config("sre-v0", PRIMARY_START_NS, PRIMARY_END_NS));
    let mut accepted = 0;
    let mut duplicates = 0;
    let mut rejected = [0_usize; REJECTION_CLASS_COUNT_V0];
    let mut terminal_controls = 0;

    for record in generator {
        match record.unwrap() {
            FixtureRecordV0::Event(event) => match state.apply_event(&event).unwrap() {
                ApplyOutcomeV0::Accepted => accepted += 1,
                ApplyOutcomeV0::DuplicateNoOp => duplicates += 1,
                ApplyOutcomeV0::Rejected(reason) => {
                    let index = RejectionReasonV0::ALL
                        .iter()
                        .position(|candidate| *candidate == reason)
                        .unwrap();
                    rejected[index] += 1;
                }
            },
            FixtureRecordV0::EndOfInput(control) => {
                terminal_controls += 1;
                assert_eq!(
                    state.finalize_source(control.source_id(), control.finalized_through_ns()),
                    FinalizeOutcomeV0::Finalized
                );
            }
        }
    }

    assert_eq!(accepted, 98_750);
    assert_eq!(duplicates, 1_000);
    assert_eq!(terminal_controls, 1);
    assert_eq!(
        rejected,
        [0, 0, 0, 0, 250, 0, 0, 0],
        "only the approved late distractors may be rejected"
    );
    assert_eq!(state.usage().accepted_identities(), 98_750);
    assert!(state.usage().accepted_identities() <= MAX_ACCEPTED_IDENTITIES_V0);
    assert!(state.usage().accepted_event_bytes() <= MAX_ACCEPTED_EVENT_BYTES_V0);
    assert!(
        state
            .accepted_events()
            .all(|event| event.ingest_time_ns() == 0)
    );
    assert!(state.is_finalized());
    assert_eq!(state.watermark_ns(), Some(PRIMARY_END_NS));

    let late = state.rejection_summary().range(RejectionReasonV0::Late);
    assert_eq!(late.count(), 250);
    assert!(!late.count_saturated());
    assert!(late.minimum_attempted_time_ns().unwrap() >= PRIMARY_START_NS);
    assert!(late.maximum_attempted_time_ns().unwrap() < PRIMARY_START_NS + 5 * 60 * SECOND_NS);
    for reason in RejectionReasonV0::ALL {
        if reason != RejectionReasonV0::Late {
            assert_eq!(state.rejection_summary().range(reason).count(), 0);
        }
    }
}

#[test]
fn diagnostics_are_content_independent_for_equal_length_inputs() {
    fn diagnostic(source_id: &str, secret: &str) -> String {
        let mut state =
            AdmissionStateV0::new(generous_config(source_id, 0, 200_000_000_000_000_000));
        let input = event(
            source_id,
            9_876_543_210,
            (123_456_789_012_345_678, 123_456_789_012_345_679),
            "schema-v0",
            Some(secret.as_bytes()),
            u64::MAX,
            ScalarValueV0::utf8(secret).unwrap(),
        );
        let outcome = state.apply_event(&input).unwrap();
        format!("{state:?}|{outcome:?}")
    }

    let left = diagnostic("source-aa", "secret-aa");
    let right = diagnostic("source-bb", "secret-bb");
    assert_eq!(left, right);
    for forbidden in [
        "source-aa",
        "source-bb",
        "secret-aa",
        "secret-bb",
        "secret",
        "9_876_543_210",
        "9876543210",
        "123456789012345678",
        "7365637265742d6161",
        "7365637265742d6262",
        "c2VjcmV0LWFh",
        "c2VjcmV0LWJi",
        "decant://event/v0/",
        "115, 101, 99, 114, 101, 116",
    ] {
        assert!(
            !left.contains(forbidden),
            "diagnostic disclosed {forbidden}"
        );
        assert!(
            !right.contains(forbidden),
            "diagnostic disclosed {forbidden}"
        );
    }
}
