use std::collections::BTreeMap;

use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_EVENT_TRANSITION_VERSION_V1, ADMISSION_VERSION_V0, AdmissionConfigV0,
    AdmissionErrorV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0, FinalizeOutcomeV0,
    MAX_ACCEPTED_EVENT_BYTES_V0, RejectionReasonV0, accepted_event_accounted_bytes_v0,
    encode_admission_snapshot_v1, hash_admission_state_v1, restore_admission_snapshot_v1,
};

const SOURCE: &str = "source-v1";

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn position(kind: SourcePositionKindV0, ordinal: u64) -> SourcePositionV0 {
    match kind {
        SourcePositionKindV0::Synthetic => SourcePositionV0::Synthetic { sequence: ordinal },
        SourcePositionKindV0::OtlpFile => {
            SourcePositionV0::otlp_file(ordinal + 1, 1, 2, u32::try_from(ordinal).unwrap()).unwrap()
        }
        SourcePositionKindV0::Kafka => {
            SourcePositionV0::kafka(7, i64::try_from(ordinal).unwrap()).unwrap()
        }
    }
}

fn config_with_limits(kind: SourcePositionKindV0, limits: AdmissionLimitsV0) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source(SOURCE),
        kind,
        0,
        100_000_000_000,
        limits,
    )
    .unwrap()
}

fn config(kind: SourcePositionKindV0) -> AdmissionConfigV0 {
    config_with_limits(kind, AdmissionLimitsV0::maximum())
}

fn event(
    source_id: &str,
    position: SourcePositionV0,
    event_time_ns: i64,
    ingest_time_ns: i64,
    value: u64,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(source_id),
        SchemaIdV0::new("schema-v1").unwrap(),
        position,
        Some(b"bounded-key"),
        event_time_ns,
        ingest_time_ns,
        SensitivityBitmapV0::from_bits(u64::MAX),
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([(
            "value".to_owned(),
            ScalarValueV0::from_u64(value),
        )]))
        .unwrap(),
    )
    .unwrap()
}

fn kinds() -> [SourcePositionKindV0; 3] {
    [
        SourcePositionKindV0::Synthetic,
        SourcePositionKindV0::OtlpFile,
        SourcePositionKindV0::Kafka,
    ]
}

#[test]
fn matching_kinds_normalize_ingest_and_round_trip_v1() {
    assert_eq!(ADMISSION_EVENT_TRANSITION_VERSION_V1, 1);

    for kind in kinds() {
        let admission_config = config(kind);
        let original = event(SOURCE, position(kind, 3), 50_000_000_000, 918_273, 3);
        let mut state = AdmissionStateV0::new(admission_config.clone());

        assert_eq!(
            state.apply_event_v1(&original),
            Ok(ApplyOutcomeV0::Accepted)
        );
        let retained = state.accepted_events().next().unwrap();
        assert_eq!(retained.source_position(), position(kind, 3));
        assert_eq!(retained.ingest_time_ns(), 0);

        let snapshot = encode_admission_snapshot_v1(&state).unwrap();
        let hash = hash_admission_state_v1(&state).unwrap();
        let restored = restore_admission_snapshot_v1(&snapshot, &admission_config).unwrap();
        assert_eq!(restored, state);
        assert_eq!(encode_admission_snapshot_v1(&restored).unwrap(), snapshot);
        assert_eq!(hash_admission_state_v1(&restored).unwrap(), hash);

        let ingest_only_redelivery = event(SOURCE, position(kind, 3), 50_000_000_000, -44_321, 3);
        assert_eq!(
            state.apply_event_v1(&ingest_only_redelivery),
            Ok(ApplyOutcomeV0::DuplicateNoOp)
        );
        assert_eq!(encode_admission_snapshot_v1(&state).unwrap(), snapshot);
        assert_eq!(hash_admission_state_v1(&state).unwrap(), hash);
    }
}

#[test]
fn source_then_kind_precedence_is_exact_and_non_leaking() {
    let admission_config = config(SourcePositionKindV0::Kafka);
    let mut state = AdmissionStateV0::new(admission_config);
    let wrong_source_and_kind = event(
        "source-sensitive-probe",
        position(SourcePositionKindV0::Synthetic, 0),
        50,
        7_654_321,
        0,
    );

    assert_eq!(
        state.apply_event_v1(&wrong_source_and_kind),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::UnexpectedSource
        ))
    );
    assert_eq!(state.accepted_events().count(), 0);
    assert_eq!(
        state
            .rejection_summary()
            .range(RejectionReasonV0::UnexpectedSource)
            .count(),
        1
    );

    let snapshot_after_source = encode_admission_snapshot_v1(&state).unwrap();
    let usage_after_source = state.usage();
    let maximum_after_source = state.maximum_accepted_event_time_ns();
    let watermark_after_source = state.watermark_ns();
    let finalized_after_source = state.is_finalized();
    let rejections_after_source = state.rejection_summary().clone();
    let wrong_kind = event(
        SOURCE,
        position(SourcePositionKindV0::Synthetic, 0),
        50,
        7_654_321,
        0,
    );
    let error = state.apply_event_v1(&wrong_kind).unwrap_err();
    assert_eq!(
        error,
        AdmissionErrorV0::UnexpectedSourcePositionKind {
            expected: SourcePositionKindV0::Kafka,
            actual: SourcePositionKindV0::Synthetic,
        }
    );
    assert_eq!(
        encode_admission_snapshot_v1(&state).unwrap(),
        snapshot_after_source
    );
    assert_eq!(state.usage(), usage_after_source);
    assert_eq!(state.maximum_accepted_event_time_ns(), maximum_after_source);
    assert_eq!(state.watermark_ns(), watermark_after_source);
    assert_eq!(state.is_finalized(), finalized_after_source);
    assert_eq!(state.rejection_summary(), &rejections_after_source);

    let diagnostics = format!("{state:?} {error:?} {error}");
    for secret in [
        "source-sensitive-probe",
        "source-v1",
        "bounded-key",
        "7654321",
    ] {
        assert!(!diagnostics.contains(secret));
    }
}

#[test]
fn semantic_rejections_and_capacity_apply_to_every_kind() {
    for kind in kinds() {
        let accepted = event(SOURCE, position(kind, 0), 80_000_000_000, 1, 0);
        let accepted_bytes = accepted_event_accounted_bytes_v0(&accepted).unwrap();
        let limits = AdmissionLimitsV0::new(1, MAX_ACCEPTED_EVENT_BYTES_V0).unwrap();
        let mut state = AdmissionStateV0::new(config_with_limits(kind, limits));
        assert_eq!(
            state.apply_event_v1(&accepted),
            Ok(ApplyOutcomeV0::Accepted)
        );

        let conflict = event(SOURCE, position(kind, 0), 80_000_000_000, 2, 9);
        assert_eq!(
            state.apply_event_v1(&conflict),
            Ok(ApplyOutcomeV0::Rejected(
                RejectionReasonV0::ConflictingRedelivery
            ))
        );
        assert_eq!(
            state.apply_event_v1(&event(SOURCE, position(kind, 1), 40_000_000_000, 0, 1,)),
            Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::Late))
        );
        assert_eq!(
            state.apply_event_v1(&event(SOURCE, position(kind, 2), 100_000_000_000, 0, 2,)),
            Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch))
        );
        assert_eq!(
            state.apply_event_v1(&event(SOURCE, position(kind, 3), 90_000_000_000, 0, 3,)),
            Ok(ApplyOutcomeV0::Rejected(
                RejectionReasonV0::AcceptedIdentityCapacity
            ))
        );
        assert_eq!(state.accepted_events().count(), 1);

        let byte_limits = AdmissionLimitsV0::new(2, accepted_bytes).unwrap();
        let mut byte_limited = AdmissionStateV0::new(config_with_limits(kind, byte_limits));
        assert_eq!(
            byte_limited.apply_event_v1(&accepted),
            Ok(ApplyOutcomeV0::Accepted)
        );
        assert_eq!(
            byte_limited.apply_event_v1(&event(SOURCE, position(kind, 1), 90_000_000_000, 0, 1,)),
            Ok(ApplyOutcomeV0::Rejected(
                RejectionReasonV0::AcceptedEventByteCapacity
            ))
        );

        assert_eq!(
            state.finalize_source(&source(SOURCE), 100_000_000_000),
            FinalizeOutcomeV0::Finalized
        );
        assert_eq!(
            state.apply_event_v1(&event(SOURCE, position(kind, 4), 90_000_000_000, 0, 4,)),
            Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::EpochFinalized))
        );
        encode_admission_snapshot_v1(&state).unwrap();
        hash_admission_state_v1(&state).unwrap();
    }
}

#[test]
fn accepted_permutations_have_canonical_snapshot_and_hash() {
    for kind in kinds() {
        let events = [
            event(SOURCE, position(kind, 2), 50, 20, 2),
            event(SOURCE, position(kind, 0), 50, 10, 0),
            event(SOURCE, position(kind, 1), 50, 30, 1),
        ];
        let mut first = AdmissionStateV0::new(config(kind));
        let mut second = AdmissionStateV0::new(config(kind));
        for index in [0, 1, 2] {
            assert_eq!(
                first.apply_event_v1(&events[index]),
                Ok(ApplyOutcomeV0::Accepted)
            );
        }
        for index in [2, 0, 1] {
            assert_eq!(
                second.apply_event_v1(&events[index]),
                Ok(ApplyOutcomeV0::Accepted)
            );
        }
        let positions: Vec<_> = first
            .accepted_events()
            .map(EventEnvelopeV0::source_position)
            .collect();
        assert_eq!(
            positions,
            (0..3)
                .map(|index| position(kind, index))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            encode_admission_snapshot_v1(&first).unwrap(),
            encode_admission_snapshot_v1(&second).unwrap()
        );
        assert_eq!(
            hash_admission_state_v1(&first).unwrap(),
            hash_admission_state_v1(&second).unwrap()
        );
    }
}

#[test]
fn legacy_apply_event_remains_external_fail_closed() {
    for kind in [SourcePositionKindV0::OtlpFile, SourcePositionKindV0::Kafka] {
        let mut state = AdmissionStateV0::new(config(kind));
        let candidate = event(SOURCE, position(kind, 0), 50, 999, 0);
        assert_eq!(
            state.apply_event(&candidate),
            Err(AdmissionErrorV0::UnsupportedSourcePosition { kind })
        );
        assert_eq!(state.accepted_events().count(), 0);
    }
}
