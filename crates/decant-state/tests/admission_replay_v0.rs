use std::collections::BTreeSet;

use decant_fixtures::{
    FixtureRecordV0, GENERATOR_VERSION_V0, SRE_V0_DATA_DELIVERIES, SreGeneratorV0,
    decode_scenario_manifest_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_REPLAY_VERSION_V0, ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionErrorV0,
    AdmissionLimitsV0, AdmissionReplayAppendErrorV0, AdmissionReplayConfigErrorV0,
    AdmissionReplayErrorV0, AdmissionReplayInputKindV0, AdmissionReplayInputV0,
    AdmissionReplayJournalV0, AdmissionReplayLimitFieldV0, AdmissionReplayLimitsV0,
    AdmissionReplayOutcomeV0, AdmissionReplayReportV0, AdmissionStateV0, ApplyOutcomeV0,
    FinalizeOutcomeV0, MAX_ACCEPTED_EVENT_BYTES_V0, MAX_ACCEPTED_IDENTITIES_V0,
    MAX_ADMISSION_REPLAY_BYTES_V0, MAX_ADMISSION_REPLAY_INPUTS_V0, REJECTION_CLASS_COUNT_V0,
    RejectionReasonV0, accepted_event_accounted_bytes_v0,
    admission_replay_input_accounted_bytes_v0, encode_admission_snapshot_v0,
    hash_admission_state_v0, replay_admission_journal_v0,
    replay_admission_journal_with_interruption_v0,
};

const SECOND_NS: i64 = 1_000_000_000;
const PRIMARY_START_NS: i64 = 1_767_225_600_000_000_000;
const PRIMARY_END_NS: i64 = 1_767_226_800_000_000_000;
const PRIMARY_SCENARIO: &[u8] =
    include_bytes!("../../decant-fixtures/contracts/sre-v0/scenario.json");

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn schema(value: &str) -> SchemaIdV0 {
    SchemaIdV0::new(value).unwrap()
}

fn payload(value: u64) -> CanonicalPayloadV0 {
    CanonicalPayloadV0::try_from_fields(
        vec![("value".to_owned(), ScalarValueV0::from_u64(value))]
            .into_iter()
            .collect(),
    )
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
fn event(
    source_id: &str,
    sequence: u64,
    schema_id: &str,
    key: Option<&[u8]>,
    event_time_ns: i64,
    ingest_time_ns: i64,
    value: u64,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(source_id),
        schema(schema_id),
        SourcePositionV0::Synthetic { sequence },
        key,
        event_time_ns,
        ingest_time_ns,
        SensitivityBitmapV0::from_bits(value),
        payload(value),
    )
    .unwrap()
}

fn otlp_event(source_id: &str, line_number: u64) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(source_id),
        schema("otlp-span-v0"),
        SourcePositionV0::otlp_file(line_number, 1, 2, 3).unwrap(),
        Some(&[7; 16]),
        50,
        0,
        SensitivityBitmapV0::from_bits(u64::MAX),
        payload(9),
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
    config: &AdmissionConfigV0,
    inputs: &[AdmissionReplayInputV0],
) -> (AdmissionStateV0, Vec<AdmissionReplayOutcomeV0>) {
    let mut state = AdmissionStateV0::new(config.clone());
    let mut outcomes = Vec::with_capacity(inputs.len());
    for input in inputs {
        let outcome = match input {
            AdmissionReplayInputV0::Event(candidate) => {
                AdmissionReplayOutcomeV0::Event(state.apply_event(candidate))
            }
            AdmissionReplayInputV0::FinalizeSource {
                source_id,
                finalized_through_ns,
            } => AdmissionReplayOutcomeV0::FinalizeSource(
                state.finalize_source(source_id, *finalized_through_ns),
            ),
        };
        outcomes.push(outcome);
    }
    (state, outcomes)
}

fn assert_report_matches(
    report: &AdmissionReplayReportV0,
    expected_state: &AdmissionStateV0,
    expected_outcomes: &[AdmissionReplayOutcomeV0],
) {
    assert_eq!(report.outcomes(), expected_outcomes);
    assert_eq!(report.state(), expected_state);
    assert_eq!(
        encode_admission_snapshot_v0(report.state()).unwrap(),
        encode_admission_snapshot_v0(expected_state).unwrap()
    );
    assert_eq!(
        hash_admission_state_v0(report.state()).unwrap(),
        hash_admission_state_v0(expected_state).unwrap()
    );
}

#[test]
fn otlp_position_error_replays_identically_at_every_interruption() {
    let admission_config = config("otel-demo-capture", 0, 100, AdmissionLimitsV0::maximum());
    let candidate = otlp_event("otel-demo-capture", 7);
    let input = AdmissionReplayInputV0::Event(candidate.clone());
    assert_eq!(
        admission_replay_input_accounted_bytes_v0(&input).unwrap(),
        1 + accepted_event_accounted_bytes_v0(&candidate).unwrap()
    );
    let journal = journal(vec![input]);
    let expected_state = AdmissionStateV0::new(admission_config.clone());
    let expected_outcomes = [AdmissionReplayOutcomeV0::Event(Err(
        AdmissionErrorV0::UnsupportedSourcePosition {
            kind: SourcePositionKindV0::OtlpFile,
        },
    ))];

    let uninterrupted = replay_admission_journal_v0(&admission_config, &journal).unwrap();
    assert_report_matches(&uninterrupted, &expected_state, &expected_outcomes);
    for interruption_after in 0..=journal.len() {
        let interrupted = replay_admission_journal_with_interruption_v0(
            &admission_config,
            &journal,
            interruption_after,
        )
        .unwrap();
        assert_report_matches(&interrupted, &expected_state, &expected_outcomes);
        assert_eq!(
            hash_admission_state_v0(interrupted.state()).unwrap(),
            hash_admission_state_v0(&expected_state).unwrap()
        );
    }
}

fn assert_all_interruptions_match(config: &AdmissionConfigV0, journal: &AdmissionReplayJournalV0) {
    let (expected_state, expected_outcomes) = direct_reference(config, journal.inputs());
    let uninterrupted = replay_admission_journal_v0(config, journal).unwrap();
    assert_report_matches(&uninterrupted, &expected_state, &expected_outcomes);
    for interruption_after in 0..=journal.len() {
        let interrupted =
            replay_admission_journal_with_interruption_v0(config, journal, interruption_after)
                .unwrap();
        assert_report_matches(&interrupted, &expected_state, &expected_outcomes);
    }
}

#[test]
fn version_and_limits_have_exact_boundaries() {
    assert_eq!(
        AdmissionReplayLimitsV0::new(0, 1),
        Err(AdmissionReplayConfigErrorV0::ZeroLimit {
            field: AdmissionReplayLimitFieldV0::Inputs,
        })
    );
    assert_eq!(
        AdmissionReplayLimitsV0::new(1, 0),
        Err(AdmissionReplayConfigErrorV0::ZeroLimit {
            field: AdmissionReplayLimitFieldV0::Bytes,
        })
    );
    assert_eq!(
        AdmissionReplayLimitsV0::new(MAX_ADMISSION_REPLAY_INPUTS_V0 + 1, 1),
        Err(AdmissionReplayConfigErrorV0::AboveMaximum {
            field: AdmissionReplayLimitFieldV0::Inputs,
            maximum: MAX_ADMISSION_REPLAY_INPUTS_V0,
            actual: MAX_ADMISSION_REPLAY_INPUTS_V0 + 1,
        })
    );
    assert_eq!(
        AdmissionReplayLimitsV0::new(1, MAX_ADMISSION_REPLAY_BYTES_V0 + 1),
        Err(AdmissionReplayConfigErrorV0::AboveMaximum {
            field: AdmissionReplayLimitFieldV0::Bytes,
            maximum: MAX_ADMISSION_REPLAY_BYTES_V0,
            actual: MAX_ADMISSION_REPLAY_BYTES_V0 + 1,
        })
    );

    let maxima = AdmissionReplayLimitsV0::new(
        MAX_ADMISSION_REPLAY_INPUTS_V0,
        MAX_ADMISSION_REPLAY_BYTES_V0,
    )
    .unwrap();
    assert_eq!(maxima, AdmissionReplayLimitsV0::maximum());
    assert_eq!(maxima.input_limit(), MAX_ADMISSION_REPLAY_INPUTS_V0);
    assert_eq!(maxima.byte_limit(), MAX_ADMISSION_REPLAY_BYTES_V0);
    assert_eq!(
        AdmissionReplayJournalV0::new(ADMISSION_REPLAY_VERSION_V0 + 1, maxima),
        Err(AdmissionReplayConfigErrorV0::UnsupportedVersion {
            actual: ADMISSION_REPLAY_VERSION_V0 + 1,
        })
    );
    let empty = AdmissionReplayJournalV0::new(ADMISSION_REPLAY_VERSION_V0, maxima).unwrap();
    assert_eq!(empty.version(), ADMISSION_REPLAY_VERSION_V0);
    assert_eq!(empty.limits(), maxima);
    assert!(empty.is_empty());
}

#[test]
fn accounting_caps_return_refused_input_without_mutation() {
    let candidate =
        AdmissionReplayInputV0::Event(event("source-v0", 9, "schema-v0", Some(b"key"), 10, 20, 7));
    let independently_accounted =
        1 + accepted_event_accounted_bytes_v0(candidate.as_event().expect("event variant"))
            .unwrap();
    assert_eq!(
        admission_replay_input_accounted_bytes_v0(&candidate),
        Ok(independently_accounted)
    );

    let finalization = AdmissionReplayInputV0::FinalizeSource {
        source_id: source("source-v0"),
        finalized_through_ns: 100,
    };
    assert_eq!(
        admission_replay_input_accounted_bytes_v0(&finalization),
        Ok(1 + 4 + "source-v0".len() + 8)
    );

    let limits = AdmissionReplayLimitsV0::new(1, independently_accounted).unwrap();
    let mut count_limited =
        AdmissionReplayJournalV0::new(ADMISSION_REPLAY_VERSION_V0, limits).unwrap();
    count_limited.try_push(candidate.clone()).unwrap();
    let before = count_limited.clone();
    let (error, refused) = count_limited.try_push(finalization.clone()).unwrap_err();
    assert_eq!(
        error,
        AdmissionReplayAppendErrorV0::CapacityExceeded {
            field: AdmissionReplayLimitFieldV0::Inputs,
            maximum: 1,
            attempted: 2,
        }
    );
    assert_eq!(refused, finalization);
    assert_eq!(count_limited, before);

    let limits = AdmissionReplayLimitsV0::new(2, independently_accounted).unwrap();
    let mut byte_limited =
        AdmissionReplayJournalV0::new(ADMISSION_REPLAY_VERSION_V0, limits).unwrap();
    byte_limited.try_push(candidate).unwrap();
    let before = byte_limited.clone();
    let (error, refused) = byte_limited.try_push(finalization.clone()).unwrap_err();
    assert_eq!(
        error,
        AdmissionReplayAppendErrorV0::CapacityExceeded {
            field: AdmissionReplayLimitFieldV0::Bytes,
            maximum: independently_accounted,
            attempted: independently_accounted
                + admission_replay_input_accounted_bytes_v0(&finalization).unwrap(),
        }
    );
    assert_eq!(refused, finalization);
    assert_eq!(byte_limited, before);
    assert_eq!(byte_limited.usage().inputs(), 1);
    assert_eq!(byte_limited.usage().bytes(), independently_accounted);
    assert_eq!(byte_limited.usage().limits(), limits);
}

#[test]
fn insertion_order_is_not_source_sequence_order() {
    let first = AdmissionReplayInputV0::Event(event("source-v0", 9, "schema-v0", None, 9, 9, 9));
    let second = AdmissionReplayInputV0::Event(event("source-v0", 1, "schema-v0", None, 1, 1, 1));
    let terminal = AdmissionReplayInputV0::FinalizeSource {
        source_id: source("source-v0"),
        finalized_through_ns: 100,
    };
    let journal = journal(vec![first.clone(), second.clone(), terminal.clone()]);
    assert_eq!(journal.inputs(), &[first, second, terminal]);
    assert_eq!(
        journal.inputs()[0].kind(),
        AdmissionReplayInputKindV0::Event
    );
    assert_eq!(
        journal.inputs()[1].kind(),
        AdmissionReplayInputKindV0::Event
    );
    assert_eq!(
        journal.inputs()[2].kind(),
        AdmissionReplayInputKindV0::FinalizeSource
    );
    assert_eq!(
        journal.inputs()[0]
            .as_event()
            .unwrap()
            .source_position()
            .synthetic_sequence(),
        9
    );
    assert_eq!(
        journal.inputs()[1]
            .as_event()
            .unwrap()
            .source_position()
            .synthetic_sequence(),
        1
    );
    assert_eq!(
        journal.inputs()[2].as_finalization(),
        Some((&source("source-v0"), 100))
    );
}

#[test]
fn all_admission_outcomes_survive_every_interruption() {
    let generous = config(
        "source-v0",
        0,
        200 * SECOND_NS,
        AdmissionLimitsV0::maximum(),
    );
    let accepted = event(
        "source-v0",
        1,
        "schema-v0",
        Some(b"k"),
        90 * SECOND_NS,
        90 * SECOND_NS,
        1,
    );
    let mixed = journal(vec![
        AdmissionReplayInputV0::Event(event("other-v0", 99, "schema-v0", None, 10, 10, 99)),
        AdmissionReplayInputV0::Event(event(
            "source-v0",
            98,
            "schema-v0",
            None,
            200 * SECOND_NS,
            10,
            98,
        )),
        AdmissionReplayInputV0::Event(accepted.clone()),
        AdmissionReplayInputV0::Event(accepted.clone()),
        AdmissionReplayInputV0::Event(event(
            "source-v0",
            1,
            "schema-v1",
            Some(b"k"),
            90 * SECOND_NS,
            90 * SECOND_NS,
            1,
        )),
        AdmissionReplayInputV0::Event(event(
            "source-v0",
            2,
            "schema-v0",
            None,
            0,
            91 * SECOND_NS,
            2,
        )),
        AdmissionReplayInputV0::FinalizeSource {
            source_id: source("source-v0"),
            finalized_through_ns: 200 * SECOND_NS - 1,
        },
        AdmissionReplayInputV0::FinalizeSource {
            source_id: source("source-v0"),
            finalized_through_ns: 200 * SECOND_NS,
        },
        AdmissionReplayInputV0::FinalizeSource {
            source_id: source("source-v0"),
            finalized_through_ns: 200 * SECOND_NS,
        },
        AdmissionReplayInputV0::Event(event(
            "source-v0",
            3,
            "schema-v0",
            None,
            100 * SECOND_NS,
            100 * SECOND_NS,
            3,
        )),
    ]);
    assert_all_interruptions_match(&generous, &mixed);

    let identity_limited = config(
        "source-v0",
        0,
        100,
        AdmissionLimitsV0::new(1, MAX_ACCEPTED_EVENT_BYTES_V0).unwrap(),
    );
    let identity_case = journal(vec![
        AdmissionReplayInputV0::Event(event("source-v0", 1, "schema-v0", None, 10, 10, 1)),
        AdmissionReplayInputV0::Event(event("source-v0", 2, "schema-v0", None, 20, 20, 2)),
    ]);
    assert_all_interruptions_match(&identity_limited, &identity_case);

    let first = event("source-v0", 1, "schema-v0", None, 10, 10, 1);
    let byte_limited = config(
        "source-v0",
        0,
        100,
        AdmissionLimitsV0::new(
            MAX_ACCEPTED_IDENTITIES_V0,
            accepted_event_accounted_bytes_v0(&first).unwrap(),
        )
        .unwrap(),
    );
    let byte_case = journal(vec![
        AdmissionReplayInputV0::Event(first),
        AdmissionReplayInputV0::Event(event("source-v0", 2, "schema-v0", None, 20, 20, 2)),
    ]);
    assert_all_interruptions_match(&byte_limited, &byte_case);

    let mut seen_rejections = BTreeSet::new();
    for (case_config, case_journal) in [
        (&generous, &mixed),
        (&identity_limited, &identity_case),
        (&byte_limited, &byte_case),
    ] {
        let (_, outcomes) = direct_reference(case_config, case_journal.inputs());
        for outcome in outcomes {
            match outcome {
                AdmissionReplayOutcomeV0::Event(Ok(ApplyOutcomeV0::Rejected(reason)))
                | AdmissionReplayOutcomeV0::FinalizeSource(FinalizeOutcomeV0::Rejected(reason)) => {
                    seen_rejections.insert(reason as u8);
                }
                AdmissionReplayOutcomeV0::Event(_)
                | AdmissionReplayOutcomeV0::FinalizeSource(_) => {}
            }
        }
    }
    assert_eq!(seen_rejections.len(), REJECTION_CLASS_COUNT_V0);
}

#[test]
fn all_twenty_four_arrival_permutations_match_every_interruption() {
    let candidates = [
        event("source-v0", 1, "schema-v0", Some(b"a"), 10, 10, 1),
        event("source-v0", 2, "schema-v0", Some(b"b"), 20, 20, 2),
        event("source-v0", 3, "schema-v0", Some(b"c"), 30, 30, 3),
        event("source-v0", 4, "schema-v0", Some(b"d"), 40, 40, 4),
    ];
    let config = config("source-v0", 0, 100, AdmissionLimitsV0::maximum());
    let mut permutations = 0;
    for first in 0..4 {
        for second in 0..4 {
            for third in 0..4 {
                for fourth in 0..4 {
                    let order = [first, second, third, fourth];
                    if order.iter().copied().collect::<BTreeSet<_>>().len() != 4 {
                        continue;
                    }
                    let inputs = order
                        .map(|index| AdmissionReplayInputV0::Event(candidates[index].clone()))
                        .into_iter()
                        .collect();
                    assert_all_interruptions_match(&config, &journal(inputs));
                    permutations += 1;
                }
            }
        }
    }
    assert_eq!(permutations, 24);
}

struct ScenarioReference {
    config: AdmissionConfigV0,
    journal: AdmissionReplayJournalV0,
    state: AdmissionStateV0,
    outcomes: Vec<AdmissionReplayOutcomeV0>,
    first_duplicate: Option<usize>,
    first_late: Option<usize>,
}

fn scenario_reference(scenario_id: &str) -> ScenarioReference {
    let manifest = decode_scenario_manifest_v0(PRIMARY_SCENARIO).unwrap();
    let generator = SreGeneratorV0::new(GENERATOR_VERSION_V0, &manifest, scenario_id).unwrap();
    let config = config(
        "sre-v0",
        PRIMARY_START_NS,
        PRIMARY_END_NS,
        AdmissionLimitsV0::maximum(),
    );
    let mut state = AdmissionStateV0::new(config.clone());
    let mut journal = AdmissionReplayJournalV0::new(
        ADMISSION_REPLAY_VERSION_V0,
        AdmissionReplayLimitsV0::maximum(),
    )
    .unwrap();
    let mut outcomes = Vec::with_capacity(SRE_V0_DATA_DELIVERIES + 1);
    let mut first_duplicate = None;
    let mut first_late = None;
    for record in generator {
        let input = match record.unwrap() {
            FixtureRecordV0::Event(candidate) => AdmissionReplayInputV0::Event(candidate),
            FixtureRecordV0::EndOfInput(control) => AdmissionReplayInputV0::FinalizeSource {
                source_id: control.source_id().clone(),
                finalized_through_ns: control.finalized_through_ns(),
            },
        };
        journal.try_push(input).unwrap();
        let recorded_input = journal.inputs().last().expect("input was just appended");
        let outcome = match recorded_input {
            AdmissionReplayInputV0::Event(candidate) => {
                AdmissionReplayOutcomeV0::Event(state.apply_event(candidate))
            }
            AdmissionReplayInputV0::FinalizeSource {
                source_id,
                finalized_through_ns,
            } => AdmissionReplayOutcomeV0::FinalizeSource(
                state.finalize_source(source_id, *finalized_through_ns),
            ),
        };
        let index = outcomes.len();
        if outcome == AdmissionReplayOutcomeV0::Event(Ok(ApplyOutcomeV0::DuplicateNoOp)) {
            first_duplicate.get_or_insert(index);
        }
        if outcome
            == AdmissionReplayOutcomeV0::Event(Ok(ApplyOutcomeV0::Rejected(
                RejectionReasonV0::Late,
            )))
        {
            first_late.get_or_insert(index);
        }
        outcomes.push(outcome);
    }
    ScenarioReference {
        config,
        journal,
        state,
        outcomes,
        first_duplicate,
        first_late,
    }
}

fn assert_selected_interruptions(reference: &ScenarioReference, points: &BTreeSet<usize>) {
    for &interruption_after in points {
        let report = replay_admission_journal_with_interruption_v0(
            &reference.config,
            &reference.journal,
            interruption_after,
        )
        .unwrap();
        assert_report_matches(&report, &reference.state, &reference.outcomes);
    }
}

#[test]
fn primary_fixture_replays_outcomes_and_frozen_state_at_selected_boundaries() {
    let reference = scenario_reference("sre-v0-primary");
    assert_eq!(reference.journal.len(), SRE_V0_DATA_DELIVERIES + 1);
    assert_eq!(reference.journal.len(), MAX_ADMISSION_REPLAY_INPUTS_V0);
    assert!(reference.journal.usage().bytes() <= MAX_ADMISSION_REPLAY_BYTES_V0);

    let mut accepted = 0;
    let mut duplicates = 0;
    let mut late = 0;
    let mut finalized = 0;
    for outcome in &reference.outcomes {
        match outcome {
            AdmissionReplayOutcomeV0::Event(Ok(ApplyOutcomeV0::Accepted)) => accepted += 1,
            AdmissionReplayOutcomeV0::Event(Ok(ApplyOutcomeV0::DuplicateNoOp)) => duplicates += 1,
            AdmissionReplayOutcomeV0::Event(Ok(ApplyOutcomeV0::Rejected(
                RejectionReasonV0::Late,
            ))) => late += 1,
            AdmissionReplayOutcomeV0::FinalizeSource(FinalizeOutcomeV0::Finalized) => {
                finalized += 1;
            }
            other => panic!("unexpected primary outcome: {other:?}"),
        }
    }
    assert_eq!(
        (accepted, duplicates, late, finalized),
        (98_750, 1_000, 250, 1)
    );
    assert_eq!(
        hash_admission_state_v0(&reference.state)
            .unwrap()
            .to_string(),
        "sha256:v0:5094443d917e11a7805d4e2362fe3a2542c86ad441f8ca2837713075370e66ef"
    );

    let first_duplicate = reference.first_duplicate.unwrap();
    let first_late = reference.first_late.unwrap();
    let len = reference.journal.len();
    let points = BTreeSet::from([
        0,
        first_duplicate,
        first_duplicate + 1,
        first_late,
        first_late + 1,
        len / 2,
        len - 1,
        len,
    ]);
    assert_selected_interruptions(&reference, &points);
}

#[test]
fn semantic_holdouts_replay_at_midpoint_and_keep_frozen_hashes() {
    for (scenario_id, expected_hash) in [
        (
            "sre-v0-region-swap",
            "sha256:v0:0ad0e5771fda2918da65c926648a94ae23611b68e1443aec97485556af5fc6b9",
        ),
        (
            "sre-v0-service-swap",
            "sha256:v0:9782c8f265a5e11bca66642f834ca09eb2ce6959c027786c0891f95233dfd350",
        ),
        (
            "sre-v0-no-regression",
            "sha256:v0:b2ba1f7626d77b9aed6103e02e8e2edebbd9285f8fd231baa67db9d7f87ddf86",
        ),
    ] {
        let reference = scenario_reference(scenario_id);
        assert_eq!(
            hash_admission_state_v0(&reference.state)
                .unwrap()
                .to_string(),
            expected_hash
        );
        let points = BTreeSet::from([reference.journal.len() / 2]);
        assert_selected_interruptions(&reference, &points);
    }
}

#[test]
fn interruption_endpoints_and_out_of_range_are_explicit() {
    let config = config("source-v0", 0, 100, AdmissionLimitsV0::maximum());
    let journal = journal(vec![AdmissionReplayInputV0::Event(event(
        "source-v0",
        1,
        "schema-v0",
        None,
        10,
        10,
        1,
    ))]);
    let at_zero = replay_admission_journal_with_interruption_v0(&config, &journal, 0).unwrap();
    let at_end = replay_admission_journal_with_interruption_v0(&config, &journal, 1).unwrap();
    let uninterrupted = replay_admission_journal_v0(&config, &journal).unwrap();
    assert_report_matches(&at_zero, uninterrupted.state(), uninterrupted.outcomes());
    assert_report_matches(&at_end, uninterrupted.state(), uninterrupted.outcomes());
    assert_eq!(
        replay_admission_journal_with_interruption_v0(&config, &journal, 2).unwrap_err(),
        AdmissionReplayErrorV0::InterruptionOutOfRange {
            maximum: 1,
            actual: 2,
        }
    );
}

#[test]
fn debug_output_is_content_independent() {
    const PROBES: [&str; 9] = [
        "private-source-79241",
        "private-schema-79242",
        "private-key-79243",
        "private-field-79244",
        "private-payload-79245",
        "792470000000009",
        "792480000000009",
        "792490000000009",
        "792500000000009",
    ];
    let payload = CanonicalPayloadV0::try_from_fields(
        vec![(
            PROBES[3].to_owned(),
            ScalarValueV0::utf8(PROBES[4]).unwrap(),
        )]
        .into_iter()
        .collect(),
    )
    .unwrap();
    let candidate = EventEnvelopeV0::new(
        0,
        source(PROBES[0]),
        schema(PROBES[1]),
        SourcePositionV0::Synthetic {
            sequence: 792_500_000_000_009,
        },
        Some(PROBES[2].as_bytes()),
        792_470_000_000_009,
        792_480_000_000_009,
        SensitivityBitmapV0::from_bits(792_490_000_000_009),
        payload,
    )
    .unwrap();
    let input = AdmissionReplayInputV0::Event(candidate);
    let input_debug = format!("{input:?}");

    let input_bytes = admission_replay_input_accounted_bytes_v0(&input).unwrap();
    let mut journal = AdmissionReplayJournalV0::new(
        ADMISSION_REPLAY_VERSION_V0,
        AdmissionReplayLimitsV0::new(1, input_bytes).unwrap(),
    )
    .unwrap();
    journal.try_push(input).unwrap();
    let journal_debug = format!("{journal:?}");
    let (append_error, refused) = journal
        .try_push(AdmissionReplayInputV0::FinalizeSource {
            source_id: source(PROBES[0]),
            finalized_through_ns: 792_470_000_000_009,
        })
        .unwrap_err();
    let append_debug = format!("{append_error:?} {refused:?}");
    let config = config(
        PROBES[0],
        792_470_000_000_000,
        792_480_000_000_000,
        AdmissionLimitsV0::maximum(),
    );
    let report_debug = format!(
        "{:?}",
        replay_admission_journal_v0(&config, &journal).unwrap()
    );
    let replay_error_debug = format!(
        "{:?}",
        replay_admission_journal_with_interruption_v0(&config, &journal, 2).unwrap_err()
    );
    let config_error_debug = format!(
        "{:?}",
        AdmissionReplayJournalV0::new(7924, AdmissionReplayLimitsV0::maximum(),).unwrap_err()
    );
    let combined = format!(
        "{input_debug} {journal_debug} {append_debug} {report_debug} {replay_error_debug} {config_error_debug}"
    );
    for probe in PROBES {
        assert!(!combined.contains(probe), "debug leaked probe {probe}");
    }
}
