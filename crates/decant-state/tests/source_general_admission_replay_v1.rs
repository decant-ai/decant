use std::collections::BTreeMap;

use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_REPLAY_TRANSITION_VERSION_V1, ADMISSION_REPLAY_VERSION_V0, ADMISSION_VERSION_V0,
    AdmissionConfigV0, AdmissionErrorV0, AdmissionLimitsV0, AdmissionReplayErrorV1,
    AdmissionReplayInputV0, AdmissionReplayJournalV0, AdmissionReplayLimitsV0,
    AdmissionReplayOutcomeV1, AdmissionReplayReportV1, AdmissionSnapshotEncodeErrorV1,
    AdmissionSnapshotFieldV1, AdmissionStateV0, ApplyOutcomeV0, FinalizeOutcomeV0,
    MAX_ACCEPTED_EVENT_BYTES_V0, RejectionReasonV0, accepted_event_accounted_bytes_v0,
    encode_admission_snapshot_v1, hash_admission_state_v1, replay_admission_journal_v1,
    replay_admission_journal_with_interruption_v1,
};

const SOURCE: &str = "replay-source-v1";
const EPOCH_END: i64 = 100_000_000_000;

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn position(kind: SourcePositionKindV0, ordinal: u64) -> SourcePositionV0 {
    match kind {
        SourcePositionKindV0::Synthetic => SourcePositionV0::Synthetic { sequence: ordinal },
        SourcePositionKindV0::OtlpFile => {
            SourcePositionV0::otlp_file(ordinal + 1, 2, 3, u32::try_from(ordinal).unwrap()).unwrap()
        }
        SourcePositionKindV0::Kafka => {
            SourcePositionV0::kafka(11, i64::try_from(ordinal).unwrap()).unwrap()
        }
    }
}

fn next_kind(kind: SourcePositionKindV0) -> SourcePositionKindV0 {
    match kind {
        SourcePositionKindV0::Synthetic => SourcePositionKindV0::OtlpFile,
        SourcePositionKindV0::OtlpFile => SourcePositionKindV0::Kafka,
        SourcePositionKindV0::Kafka => SourcePositionKindV0::Synthetic,
    }
}

fn config_with_limits(kind: SourcePositionKindV0, limits: AdmissionLimitsV0) -> AdmissionConfigV0 {
    AdmissionConfigV0::new_with_source_position_kind(
        ADMISSION_VERSION_V0,
        source(SOURCE),
        kind,
        0,
        EPOCH_END,
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
        SchemaIdV0::new("replay-schema-v1").unwrap(),
        position,
        Some(b"replay-key-probe"),
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

fn journal(inputs: Vec<AdmissionReplayInputV0>) -> AdmissionReplayJournalV0 {
    let mut journal = AdmissionReplayJournalV0::new(
        ADMISSION_REPLAY_VERSION_V0,
        AdmissionReplayLimitsV0::maximum(),
    )
    .unwrap();
    for input in inputs {
        journal.try_push(input).unwrap();
    }
    journal
}

fn direct_reference(
    admission_config: &AdmissionConfigV0,
    inputs: &[AdmissionReplayInputV0],
) -> (AdmissionStateV0, Vec<AdmissionReplayOutcomeV1>) {
    let mut state = AdmissionStateV0::new(admission_config.clone());
    let mut outcomes = Vec::with_capacity(inputs.len());
    for input in inputs {
        let outcome = match input {
            AdmissionReplayInputV0::Event(candidate) => {
                AdmissionReplayOutcomeV1::Event(state.apply_event_v1(candidate))
            }
            AdmissionReplayInputV0::FinalizeSource {
                source_id,
                finalized_through_ns,
            } => AdmissionReplayOutcomeV1::FinalizeSource(
                state.finalize_source(source_id, *finalized_through_ns),
            ),
        };
        outcomes.push(outcome);
    }
    (state, outcomes)
}

fn assert_report(
    report: &AdmissionReplayReportV1,
    expected_state: &AdmissionStateV0,
    expected_outcomes: &[AdmissionReplayOutcomeV1],
) {
    assert_eq!(report.outcomes(), expected_outcomes);
    assert_eq!(report.state(), expected_state);
    assert_eq!(
        encode_admission_snapshot_v1(report.state()).unwrap(),
        encode_admission_snapshot_v1(expected_state).unwrap()
    );
    assert_eq!(
        hash_admission_state_v1(report.state()).unwrap(),
        hash_admission_state_v1(expected_state).unwrap()
    );
}

fn assert_every_interruption(
    admission_config: &AdmissionConfigV0,
    admission_journal: &AdmissionReplayJournalV0,
) {
    let (expected_state, expected_outcomes) =
        direct_reference(admission_config, admission_journal.inputs());
    assert_report(
        &replay_admission_journal_v1(admission_config, admission_journal).unwrap(),
        &expected_state,
        &expected_outcomes,
    );
    for interruption_after in 0..=admission_journal.len() {
        let report = replay_admission_journal_with_interruption_v1(
            admission_config,
            admission_journal,
            interruption_after,
        )
        .unwrap();
        assert_report(&report, &expected_state, &expected_outcomes);
    }
}

fn kinds() -> [SourcePositionKindV0; 3] {
    [
        SourcePositionKindV0::Synthetic,
        SourcePositionKindV0::OtlpFile,
        SourcePositionKindV0::Kafka,
    ]
}

#[test]
fn every_kind_matches_direct_reference_at_every_interruption() {
    assert_eq!(ADMISSION_REPLAY_VERSION_V0, 0);
    assert_eq!(ADMISSION_REPLAY_TRANSITION_VERSION_V1, 1);

    for kind in kinds() {
        let wrong_kind = next_kind(kind);
        let first = event(SOURCE, position(kind, 0), 80_000_000_000, 91_827, 0);
        let inputs = vec![
            AdmissionReplayInputV0::Event(event(
                "replay-wrong-source-probe",
                position(wrong_kind, 0),
                10,
                999,
                0,
            )),
            AdmissionReplayInputV0::Event(event(SOURCE, position(wrong_kind, 0), 10, 999, 0)),
            AdmissionReplayInputV0::Event(first.clone()),
            AdmissionReplayInputV0::Event(event(SOURCE, position(kind, 0), 80_000_000_000, -31, 0)),
            AdmissionReplayInputV0::Event(event(SOURCE, position(kind, 0), 80_000_000_000, 0, 99)),
            AdmissionReplayInputV0::Event(event(SOURCE, position(kind, 1), EPOCH_END, 0, 1)),
            AdmissionReplayInputV0::Event(event(SOURCE, position(kind, 2), 40_000_000_000, 0, 2)),
            AdmissionReplayInputV0::FinalizeSource {
                source_id: source(SOURCE),
                finalized_through_ns: EPOCH_END - 1,
            },
            AdmissionReplayInputV0::FinalizeSource {
                source_id: source(SOURCE),
                finalized_through_ns: EPOCH_END,
            },
            AdmissionReplayInputV0::Event(event(SOURCE, position(kind, 3), 90_000_000_000, 0, 3)),
            AdmissionReplayInputV0::FinalizeSource {
                source_id: source(SOURCE),
                finalized_through_ns: EPOCH_END,
            },
        ];
        let admission_journal = journal(inputs);
        let (_, outcomes) = direct_reference(&config(kind), admission_journal.inputs());
        assert_eq!(
            outcomes,
            vec![
                AdmissionReplayOutcomeV1::Event(Ok(ApplyOutcomeV0::Rejected(
                    RejectionReasonV0::UnexpectedSource,
                ))),
                AdmissionReplayOutcomeV1::Event(Err(
                    AdmissionErrorV0::UnexpectedSourcePositionKind {
                        expected: kind,
                        actual: wrong_kind,
                    },
                )),
                AdmissionReplayOutcomeV1::Event(Ok(ApplyOutcomeV0::Accepted)),
                AdmissionReplayOutcomeV1::Event(Ok(ApplyOutcomeV0::DuplicateNoOp)),
                AdmissionReplayOutcomeV1::Event(Ok(ApplyOutcomeV0::Rejected(
                    RejectionReasonV0::ConflictingRedelivery,
                ))),
                AdmissionReplayOutcomeV1::Event(Ok(ApplyOutcomeV0::Rejected(
                    RejectionReasonV0::OutsideEpoch,
                ))),
                AdmissionReplayOutcomeV1::Event(Ok(ApplyOutcomeV0::Rejected(
                    RejectionReasonV0::Late,
                ))),
                AdmissionReplayOutcomeV1::FinalizeSource(FinalizeOutcomeV0::Rejected(
                    RejectionReasonV0::InvalidFinalization,
                )),
                AdmissionReplayOutcomeV1::FinalizeSource(FinalizeOutcomeV0::Finalized),
                AdmissionReplayOutcomeV1::Event(Ok(ApplyOutcomeV0::Rejected(
                    RejectionReasonV0::EpochFinalized,
                ))),
                AdmissionReplayOutcomeV1::FinalizeSource(FinalizeOutcomeV0::AlreadyFinalizedNoOp,),
            ]
        );
        assert_every_interruption(&config(kind), &admission_journal);

        let uninterrupted = replay_admission_journal_v1(&config(kind), &admission_journal).unwrap();
        assert_eq!(
            uninterrupted
                .state()
                .accepted_events()
                .next()
                .unwrap()
                .ingest_time_ns(),
            0
        );
        let diagnostics = format!("{admission_journal:?} {uninterrupted:?}");
        for probe in [
            "replay-wrong-source-probe",
            "replay-source-v1",
            "replay-schema-v1",
            "replay-key-probe",
            "91827",
        ] {
            assert!(!diagnostics.contains(probe));
        }
    }
}

#[test]
fn both_capacity_rejections_match_at_every_interruption() {
    for kind in kinds() {
        let first = event(SOURCE, position(kind, 0), 50_000_000_000, 1, 0);
        let second = event(SOURCE, position(kind, 1), 60_000_000_000, 2, 1);

        let identity_config = config_with_limits(
            kind,
            AdmissionLimitsV0::new(1, MAX_ACCEPTED_EVENT_BYTES_V0).unwrap(),
        );
        let identity_journal = journal(vec![
            AdmissionReplayInputV0::Event(first.clone()),
            AdmissionReplayInputV0::Event(second.clone()),
        ]);
        assert_every_interruption(&identity_config, &identity_journal);
        assert_eq!(
            replay_admission_journal_v1(&identity_config, &identity_journal)
                .unwrap()
                .outcomes()[1],
            AdmissionReplayOutcomeV1::Event(Ok(ApplyOutcomeV0::Rejected(
                RejectionReasonV0::AcceptedIdentityCapacity
            )))
        );

        let first_bytes = accepted_event_accounted_bytes_v0(&first).unwrap();
        let byte_config = config_with_limits(kind, AdmissionLimitsV0::new(2, first_bytes).unwrap());
        let byte_journal = journal(vec![
            AdmissionReplayInputV0::Event(first),
            AdmissionReplayInputV0::Event(second),
        ]);
        assert_every_interruption(&byte_config, &byte_journal);
        assert_eq!(
            replay_admission_journal_v1(&byte_config, &byte_journal)
                .unwrap()
                .outcomes()[1],
            AdmissionReplayOutcomeV1::Event(Ok(ApplyOutcomeV0::Rejected(
                RejectionReasonV0::AcceptedEventByteCapacity
            )))
        );
    }
}

#[test]
fn input_order_is_preserved_while_legal_permutations_converge() {
    for kind in kinds() {
        let events = [
            event(SOURCE, position(kind, 2), 50, 3, 2),
            event(SOURCE, position(kind, 0), 50, 1, 0),
            event(SOURCE, position(kind, 1), 50, 2, 1),
        ];
        let first = journal(
            [0, 1, 2]
                .map(|index| AdmissionReplayInputV0::Event(events[index].clone()))
                .into(),
        );
        let second = journal(
            [2, 0, 1]
                .map(|index| AdmissionReplayInputV0::Event(events[index].clone()))
                .into(),
        );
        assert_eq!(
            first.inputs()[0].as_event().unwrap().source_position(),
            position(kind, 2)
        );
        assert_eq!(
            second.inputs()[0].as_event().unwrap().source_position(),
            position(kind, 1)
        );
        let first_report = replay_admission_journal_v1(&config(kind), &first).unwrap();
        let second_report = replay_admission_journal_v1(&config(kind), &second).unwrap();
        assert_eq!(
            encode_admission_snapshot_v1(first_report.state()).unwrap(),
            encode_admission_snapshot_v1(second_report.state()).unwrap()
        );
        assert_eq!(
            hash_admission_state_v1(first_report.state()).unwrap(),
            hash_admission_state_v1(second_report.state()).unwrap()
        );
    }
}

#[test]
fn interruption_range_and_error_diagnostics_are_typed_and_safe() {
    let admission_config = config(SourcePositionKindV0::Kafka);
    let admission_journal = journal(vec![AdmissionReplayInputV0::Event(event(
        SOURCE,
        position(SourcePositionKindV0::Kafka, 0),
        50,
        99_887_766,
        0,
    ))]);
    assert!(
        replay_admission_journal_with_interruption_v1(&admission_config, &admission_journal, 0)
            .is_ok()
    );
    assert!(
        replay_admission_journal_with_interruption_v1(
            &admission_config,
            &admission_journal,
            admission_journal.len()
        )
        .is_ok()
    );
    let range_error = replay_admission_journal_with_interruption_v1(
        &admission_config,
        &admission_journal,
        admission_journal.len() + 1,
    )
    .unwrap_err();
    assert_eq!(
        range_error,
        AdmissionReplayErrorV1::InterruptionOutOfRange {
            maximum: 1,
            actual: 2,
        }
    );

    let snapshot_error =
        AdmissionReplayErrorV1::SnapshotEncoding(AdmissionSnapshotEncodeErrorV1::StateInvariant {
            field: AdmissionSnapshotFieldV1::EventRecord,
        });
    let diagnostics = format!("{range_error:?} {range_error} {snapshot_error:?}");
    for probe in ["replay-source-v1", "replay-key-probe", "99887766"] {
        assert!(!diagnostics.contains(probe));
    }
}
