use std::collections::BTreeMap;

use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_REPLAY_VERSION_V0, ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionErrorV0,
    AdmissionLimitsV0, AdmissionReplayErrorV0, AdmissionReplayInputV0, AdmissionReplayJournalV0,
    AdmissionReplayLimitsV0, AdmissionReplayOutcomeV0, AdmissionSnapshotEncodeErrorV0,
    AdmissionSnapshotFieldV0, AdmissionStateHashErrorV0, AdmissionStateV0, ApplyOutcomeV0,
    FinalizeOutcomeV0, RejectionReasonV0, encode_admission_snapshot_v0, hash_admission_state_v0,
    replay_admission_journal_v0, replay_admission_journal_with_interruption_v0,
};

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn config(source_id: &str, kind: SourcePositionKindV0) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source(source_id),
        kind,
        0,
        100,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap()
}

fn event(source_id: &str, position: SourcePositionV0) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(source_id),
        SchemaIdV0::new("schema-v0").unwrap(),
        position,
        Some(b"opaque-key"),
        50,
        0,
        SensitivityBitmapV0::from_bits(u64::MAX),
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([(
            "value".to_owned(),
            ScalarValueV0::utf8("protected-value").unwrap(),
        )]))
        .unwrap(),
    )
    .unwrap()
}

fn synthetic_event(source_id: &str) -> EventEnvelopeV0 {
    event(source_id, SourcePositionV0::Synthetic { sequence: 7 })
}

fn otlp_event(source_id: &str) -> EventEnvelopeV0 {
    event(source_id, SourcePositionV0::otlp_file(7, 1, 2, 3).unwrap())
}

fn kafka_event(source_id: &str) -> EventEnvelopeV0 {
    event(source_id, SourcePositionV0::kafka(3, 7).unwrap())
}

fn assert_empty_unmodified(state: &AdmissionStateV0, expected: &AdmissionStateV0) {
    assert_eq!(state, expected);
    assert_eq!(state.accepted_events().count(), 0);
    assert_eq!(state.usage().accepted_identities(), 0);
    assert_eq!(state.usage().accepted_event_bytes(), 0);
    assert_eq!(state.maximum_accepted_event_time_ns(), None);
    assert_eq!(state.watermark_ns(), None);
    assert!(!state.is_finalized());
    for reason in decant_state::RejectionReasonV0::ALL {
        assert_eq!(state.rejection_summary().range(reason).count(), 0);
    }
}

#[test]
fn constructors_bind_one_explicit_immutable_kind() {
    let legacy = AdmissionConfigV0::new(
        ADMISSION_VERSION_V0,
        source("source-v0"),
        0,
        100,
        AdmissionLimitsV0::maximum(),
    )
    .unwrap();
    let explicit_synthetic = config("source-v0", SourcePositionKindV0::Synthetic);
    assert_eq!(legacy, explicit_synthetic);
    assert_eq!(
        legacy.source_position_kind(),
        SourcePositionKindV0::Synthetic
    );

    let otlp = config("source-v0", SourcePositionKindV0::OtlpFile);
    let kafka = config("source-v0", SourcePositionKindV0::Kafka);
    assert_eq!(otlp.source_position_kind(), SourcePositionKindV0::OtlpFile);
    assert_eq!(kafka.source_position_kind(), SourcePositionKindV0::Kafka);
    assert_ne!(legacy, otlp);
    assert_ne!(legacy, kafka);
    assert_ne!(otlp, kafka);
}

#[test]
fn synthetic_binding_preserves_existing_external_error_categories() {
    let admission_config = config("source-v0", SourcePositionKindV0::Synthetic);
    let expected = AdmissionStateV0::new(admission_config.clone());
    let mut state = AdmissionStateV0::new(admission_config);

    for (candidate, kind) in [
        (otlp_event("source-v0"), SourcePositionKindV0::OtlpFile),
        (kafka_event("source-v0"), SourcePositionKindV0::Kafka),
    ] {
        assert_eq!(
            state.apply_event(&candidate),
            Err(AdmissionErrorV0::UnsupportedSourcePosition { kind })
        );
        assert_empty_unmodified(&state, &expected);
    }

    assert_eq!(
        state.apply_event(&synthetic_event("source-v0")),
        Ok(ApplyOutcomeV0::Accepted)
    );
}

#[test]
fn source_mismatch_precedes_kind_checks_and_records_only_existing_rejection() {
    let mut state = AdmissionStateV0::new(config("configured-source", SourcePositionKindV0::Kafka));
    assert_eq!(
        state.apply_event(&otlp_event("different-source")),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::UnexpectedSource
        ))
    );
    assert_eq!(state.accepted_events().count(), 0);
    assert_eq!(state.usage().accepted_event_bytes(), 0);
    assert_eq!(
        state
            .rejection_summary()
            .range(RejectionReasonV0::UnexpectedSource)
            .count(),
        1
    );
    for reason in RejectionReasonV0::ALL {
        if reason != RejectionReasonV0::UnexpectedSource {
            assert_eq!(state.rejection_summary().range(reason).count(), 0);
        }
    }
}

#[test]
fn external_binding_rejects_mismatched_and_matching_kinds_without_mutation() {
    let kafka_config = config("source-v0", SourcePositionKindV0::Kafka);
    let expected_kafka = AdmissionStateV0::new(kafka_config.clone());
    let mut kafka_state = AdmissionStateV0::new(kafka_config);
    for (candidate, actual) in [
        (
            synthetic_event("source-v0"),
            SourcePositionKindV0::Synthetic,
        ),
        (otlp_event("source-v0"), SourcePositionKindV0::OtlpFile),
    ] {
        assert_eq!(
            kafka_state.apply_event(&candidate),
            Err(AdmissionErrorV0::UnexpectedSourcePositionKind {
                expected: SourcePositionKindV0::Kafka,
                actual,
            })
        );
        assert_empty_unmodified(&kafka_state, &expected_kafka);
    }
    for _ in 0..2 {
        assert_eq!(
            kafka_state.apply_event(&kafka_event("source-v0")),
            Err(AdmissionErrorV0::UnsupportedSourcePosition {
                kind: SourcePositionKindV0::Kafka,
            })
        );
        assert_empty_unmodified(&kafka_state, &expected_kafka);
    }

    let otlp_config = config("source-v0", SourcePositionKindV0::OtlpFile);
    let expected_otlp = AdmissionStateV0::new(otlp_config.clone());
    let mut otlp_state = AdmissionStateV0::new(otlp_config);
    assert_eq!(
        otlp_state.apply_event(&otlp_event("source-v0")),
        Err(AdmissionErrorV0::UnsupportedSourcePosition {
            kind: SourcePositionKindV0::OtlpFile,
        })
    );
    assert_empty_unmodified(&otlp_state, &expected_otlp);
}

fn snapshot_invariant() -> AdmissionSnapshotEncodeErrorV0 {
    AdmissionSnapshotEncodeErrorV0::StateInvariant {
        field: AdmissionSnapshotFieldV0::SourcePosition,
    }
}

#[test]
fn snapshot_and_hash_v0_fail_closed_for_empty_and_finalized_external_configs() {
    for kind in [SourcePositionKindV0::OtlpFile, SourcePositionKindV0::Kafka] {
        let mut state = AdmissionStateV0::new(config("source-v0", kind));
        assert_eq!(
            encode_admission_snapshot_v0(&state),
            Err(snapshot_invariant())
        );
        assert_eq!(
            hash_admission_state_v0(&state),
            Err(AdmissionStateHashErrorV0::SnapshotEncoding(
                snapshot_invariant()
            ))
        );

        assert_eq!(
            state.finalize_source(&source("source-v0"), 100),
            FinalizeOutcomeV0::Finalized
        );
        assert_eq!(
            encode_admission_snapshot_v0(&state),
            Err(snapshot_invariant())
        );
        assert_eq!(
            hash_admission_state_v0(&state),
            Err(AdmissionStateHashErrorV0::SnapshotEncoding(
                snapshot_invariant()
            ))
        );
    }
}

#[test]
fn replay_reports_apply_error_but_v0_interruption_fails_closed() {
    let admission_config = config("source-v0", SourcePositionKindV0::Kafka);
    let mut journal = AdmissionReplayJournalV0::new(
        ADMISSION_REPLAY_VERSION_V0,
        AdmissionReplayLimitsV0::maximum(),
    )
    .unwrap();
    journal
        .try_push(AdmissionReplayInputV0::Event(kafka_event("source-v0")))
        .unwrap();

    let report = replay_admission_journal_v0(&admission_config, &journal).unwrap();
    assert_eq!(
        report.outcomes(),
        &[AdmissionReplayOutcomeV0::Event(Err(
            AdmissionErrorV0::UnsupportedSourcePosition {
                kind: SourcePositionKindV0::Kafka,
            }
        ))]
    );
    assert_eq!(
        report.state(),
        &AdmissionStateV0::new(admission_config.clone())
    );

    for interruption_after in 0..=journal.len() {
        assert_eq!(
            replay_admission_journal_with_interruption_v0(
                &admission_config,
                &journal,
                interruption_after,
            )
            .unwrap_err(),
            AdmissionReplayErrorV0::SnapshotEncoding(snapshot_invariant())
        );
    }
}

#[test]
fn diagnostics_expose_categories_but_not_sensitive_values() {
    let secret_source = "secret-source-028";
    let config_debug = format!("{:?}", config(secret_source, SourcePositionKindV0::Kafka));
    let error = AdmissionErrorV0::UnexpectedSourcePositionKind {
        expected: SourcePositionKindV0::Kafka,
        actual: SourcePositionKindV0::OtlpFile,
    };
    let error_debug = format!("{error:?}");
    let error_display = error.to_string();

    for output in [&config_debug, &error_debug, &error_display] {
        assert!(!output.contains(secret_source));
        assert!(!output.contains("protected-value"));
        assert!(!output.contains("opaque-key"));
    }
    assert!(config_debug.contains("Kafka"));
    assert!(error_debug.contains("UnexpectedSourcePositionKind"));
    assert!(error_display.contains("expected: Kafka"));
    assert!(error_display.contains("actual: OtlpFile"));
}
