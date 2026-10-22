use std::collections::BTreeMap;

use decant_fixtures::{
    FixtureRecordV0, GENERATOR_VERSION_V0, SreGeneratorV0, decode_scenario_manifest_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigV0, FinalizeOutcomeV0,
    LATEST_BY_KEY_VERSION_V0, LatestByKeyConfigV0, LatestByKeyLimitsV0,
    LatestMaterializationErrorV0, MAX_LATEST_INDEX_BYTES_V0, RETAINED_LATEST_BY_KEY_VERSION_V0,
    RejectionReasonV0, RetainedLatestByKeyConfigErrorV0, RetainedLatestByKeyConfigV0,
    RetainedLatestByKeyViewV0,
};

const SECOND_NS: i64 = 1_000_000_000;
const PRIMARY_START_NS: i64 = 1_767_225_600_000_000_000;
const PRIMARY_END_NS: i64 = 1_767_226_800_000_000_000;
const PRIMARY_SCENARIO: &[u8] =
    include_bytes!("../../decant-fixtures/contracts/sre-v0/scenario.json");

type LatestTuple = (String, Vec<u8>, u64);
type ViewObservation = (
    Option<i64>,
    Vec<LatestTuple>,
    decant_state::EventTimeRetentionUsageV0,
    decant_state::LatestByKeyUsageV0,
    Option<i64>,
    bool,
);

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn schema(value: &str) -> SchemaIdV0 {
    SchemaIdV0::new(value).unwrap()
}

fn payload(value: &str) -> CanonicalPayloadV0 {
    CanonicalPayloadV0::try_from_fields(BTreeMap::from([(
        "value".to_owned(),
        ScalarValueV0::utf8(value).unwrap(),
    )]))
    .unwrap()
}

fn event(
    source_id: &str,
    sequence: u64,
    times_ns: (i64, i64),
    schema_id: &str,
    key: Option<&[u8]>,
    sensitivity: u64,
    value: &str,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(source_id),
        schema(schema_id),
        SourcePositionV0::Synthetic { sequence },
        key,
        times_ns.0,
        times_ns.1,
        SensitivityBitmapV0::from_bits(sensitivity),
        payload(value),
    )
    .unwrap()
}

fn basic_event(
    sequence: u64,
    event_time_ns: i64,
    schema_id: &str,
    key: Option<&[u8]>,
    value: &str,
) -> EventEnvelopeV0 {
    event(
        "source-v0",
        sequence,
        (event_time_ns, event_time_ns),
        schema_id,
        key,
        0,
        value,
    )
}

fn admission_with(limits: AdmissionLimitsV0) -> AdmissionStateV0 {
    AdmissionStateV0::new(
        AdmissionConfigV0::new(
            ADMISSION_VERSION_V0,
            source("source-v0"),
            0,
            200 * SECOND_NS,
            limits,
        )
        .unwrap(),
    )
}

fn admission() -> AdmissionStateV0 {
    admission_with(AdmissionLimitsV0::maximum())
}

fn primary_admission() -> AdmissionStateV0 {
    AdmissionStateV0::new(
        AdmissionConfigV0::new(
            ADMISSION_VERSION_V0,
            source("sre-v0"),
            PRIMARY_START_NS,
            PRIMARY_END_NS,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    )
}

fn latest_config(limits: LatestByKeyLimitsV0) -> LatestByKeyConfigV0 {
    LatestByKeyConfigV0::new(LATEST_BY_KEY_VERSION_V0, limits).unwrap()
}

fn retained_latest_config(
    horizon_ns: u64,
    limits: LatestByKeyLimitsV0,
) -> RetainedLatestByKeyConfigV0 {
    RetainedLatestByKeyConfigV0::new(
        RETAINED_LATEST_BY_KEY_VERSION_V0,
        EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, horizon_ns).unwrap(),
        latest_config(limits),
    )
    .unwrap()
}

fn maximum_config(horizon_ns: u64) -> RetainedLatestByKeyConfigV0 {
    retained_latest_config(horizon_ns, LatestByKeyLimitsV0::maximum())
}

fn accept(state: &mut AdmissionStateV0, candidate: &EventEnvelopeV0) {
    assert_eq!(state.apply_event(candidate), Ok(ApplyOutcomeV0::Accepted));
}

fn sequence(event: &EventEnvelopeV0) -> u64 {
    event.source_position().synthetic_sequence()
}

fn independent_entry_bytes(schema_id: &str, key: &[u8]) -> usize {
    4 + schema_id.len() + 4 + key.len() + 8
}

fn view_tuples(view: &RetainedLatestByKeyViewV0<'_>) -> Vec<LatestTuple> {
    view.entries()
        .map(|(schema_id, key, winner)| {
            (
                schema_id.as_str().to_owned(),
                key.to_vec(),
                sequence(winner),
            )
        })
        .collect()
}

fn assert_usage_conserved(view: &RetainedLatestByKeyViewV0<'_>) {
    let retention = view.retention_usage();
    assert_eq!(
        retention.accepted_events_considered(),
        retention.retained_events() + retention.expired_events_excluded()
    );
    assert_eq!(
        retention.retained_events(),
        view.latest_usage().accepted_events_considered()
    );
}

#[test]
fn combined_config_is_versioned_and_preserves_nested_boundaries() {
    let minimum_retention = EventTimeRetentionConfigV0::new(0, 1).unwrap();
    let maximum_retention = EventTimeRetentionConfigV0::new(0, u64::MAX).unwrap();
    let minimum_latest = latest_config(LatestByKeyLimitsV0::new(1, 1).unwrap());
    let maximum_latest = latest_config(LatestByKeyLimitsV0::maximum());
    let minimum = RetainedLatestByKeyConfigV0::new(0, minimum_retention, minimum_latest).unwrap();
    let maximum = RetainedLatestByKeyConfigV0::new(0, maximum_retention, maximum_latest).unwrap();
    assert_eq!(minimum.version(), 0);
    assert_eq!(minimum.retention(), minimum_retention);
    assert_eq!(minimum.latest(), minimum_latest);
    assert_eq!(maximum.retention(), maximum_retention);
    assert_eq!(maximum.latest(), maximum_latest);
    assert_eq!(
        RetainedLatestByKeyConfigV0::new(1, minimum_retention, minimum_latest),
        Err(RetainedLatestByKeyConfigErrorV0::UnsupportedVersion { actual: 1 })
    );
}

#[test]
fn empty_admission_has_exact_empty_composed_evidence() {
    let state = admission();
    let config = maximum_config(100 * SECOND_NS as u64);
    let view = state.materialize_retained_latest_by_key(config).unwrap();
    assert_eq!(view.config(), config);
    assert_eq!(view.retention_cutoff_event_time_ns(), None);
    assert_eq!(view.retention_usage().accepted_events_considered(), 0);
    assert_eq!(view.retention_usage().retained_events(), 0);
    assert_eq!(view.retention_usage().expired_events_excluded(), 0);
    assert_eq!(view.latest_usage().accepted_events_considered(), 0);
    assert_eq!(view.latest_usage().indexed_entries(), 0);
    assert_eq!(view.latest_usage().index_bytes(), 0);
    assert_eq!(view.latest_usage().unkeyed_events_omitted(), 0);
    assert_eq!(view.source_watermark_ns(), None);
    assert!(!view.is_source_finalized());
    assert!(view.get(&schema("schema-v0"), b"key").is_none());
    assert!(view.entries().next().is_none());
    assert_usage_conserved(&view);
}

#[test]
fn cutoff_boundary_and_full_winner_restriction_are_exact() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 39 * SECOND_NS, "expired-v0", Some(b"old"), "old"),
        basic_event(2, 40 * SECOND_NS, "exact-v0", Some(b"key"), "first"),
        basic_event(3, 40 * SECOND_NS, "exact-v0", Some(b"key"), "second"),
        basic_event(4, 41 * SECOND_NS, "above-v0", Some(b"key"), "above"),
        basic_event(5, 39 * SECOND_NS, "mixed-v0", Some(b"key"), "expired"),
        basic_event(6, 40 * SECOND_NS, "mixed-v0", Some(b"key"), "retained"),
        basic_event(7, 90 * SECOND_NS, "clock-v0", Some(b"clock"), "clock"),
    ] {
        accept(&mut state, &candidate);
    }

    let retained = state
        .materialize_retained_latest_by_key(maximum_config(20 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(
        retained.retention_cutoff_event_time_ns(),
        Some(40 * SECOND_NS)
    );
    assert!(retained.get(&schema("expired-v0"), b"old").is_none());
    assert_eq!(
        sequence(retained.get(&schema("exact-v0"), b"key").unwrap()),
        3
    );
    assert_eq!(
        sequence(retained.get(&schema("above-v0"), b"key").unwrap()),
        4
    );
    assert_eq!(
        sequence(retained.get(&schema("mixed-v0"), b"key").unwrap()),
        6
    );
    assert_eq!(retained.retention_usage().accepted_events_considered(), 7);
    assert_eq!(retained.retention_usage().retained_events(), 5);
    assert_eq!(retained.retention_usage().expired_events_excluded(), 2);
    assert_usage_conserved(&retained);

    let full = state
        .materialize_latest_by_key(latest_config(LatestByKeyLimitsV0::maximum()))
        .unwrap();
    for (schema_id, key, winner) in full.entries() {
        let retained_winner = retained.get(schema_id, key);
        if winner.event_time_ns() >= 40 * SECOND_NS {
            assert!(std::ptr::eq(retained_winner.unwrap(), winner));
        } else {
            assert!(retained_winner.is_none());
        }
    }
}

#[test]
fn schema_empty_key_and_unkeyed_accounting_are_retention_aware() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 10 * SECOND_NS, "old-v0", None, "expired-unkeyed"),
        basic_event(2, 40 * SECOND_NS, "kept-v0", None, "retained-unkeyed"),
        basic_event(3, 41 * SECOND_NS, "schema-a", Some(b""), "a"),
        basic_event(4, 42 * SECOND_NS, "schema-b", Some(b""), "b"),
        basic_event(5, 90 * SECOND_NS, "clock-v0", Some(b"clock"), "clock"),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_retained_latest_by_key(maximum_config(20 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(view.retention_usage().retained_events(), 4);
    assert_eq!(view.retention_usage().expired_events_excluded(), 1);
    assert_eq!(view.latest_usage().accepted_events_considered(), 4);
    assert_eq!(view.latest_usage().unkeyed_events_omitted(), 1);
    assert_eq!(view.latest_usage().indexed_entries(), 3);
    assert_eq!(sequence(view.get(&schema("schema-a"), b"").unwrap()), 3);
    assert_eq!(sequence(view.get(&schema("schema-b"), b"").unwrap()), 4);
    assert_usage_conserved(&view);
}

#[test]
fn duplicates_and_semantic_rejections_cannot_enter_retained_latest() {
    let mut state = admission();
    let original = event(
        "source-v0",
        1,
        (40 * SECOND_NS, 1),
        "schema-v0",
        Some(b"key"),
        7,
        "original",
    );
    accept(&mut state, &original);
    accept(
        &mut state,
        &basic_event(2, 90 * SECOND_NS, "clock-v0", Some(b"clock"), "clock"),
    );
    let duplicate = event(
        "source-v0",
        1,
        (40 * SECOND_NS, i64::MAX),
        "schema-v0",
        Some(b"key"),
        7,
        "original",
    );
    assert_eq!(
        state.apply_event(&duplicate),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    let conflict = event(
        "source-v0",
        1,
        (40 * SECOND_NS, 1),
        "schema-v0",
        Some(b"key"),
        7,
        "conflict",
    );
    assert_eq!(
        state.apply_event(&conflict),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::ConflictingRedelivery
        ))
    );
    assert_eq!(
        state.apply_event(&basic_event(
            3,
            60 * SECOND_NS - 1,
            "schema-v0",
            Some(b"key"),
            "late",
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::Late))
    );
    assert_eq!(
        state.apply_event(&event(
            "source-x0",
            4,
            (100 * SECOND_NS, 4),
            "schema-v0",
            Some(b"key"),
            0,
            "wrong-source",
        )),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::UnexpectedSource
        ))
    );
    assert_eq!(
        state.apply_event(&basic_event(
            5,
            200 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            "outside",
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch))
    );
    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    assert_eq!(
        state.apply_event(&basic_event(
            6,
            100 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            "closed",
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::EpochFinalized))
    );
    let view = state
        .materialize_retained_latest_by_key(maximum_config(200 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(view.retention_usage().accepted_events_considered(), 2);
    assert_eq!(view.latest_usage().accepted_events_considered(), 2);
    assert_eq!(sequence(view.get(&schema("schema-v0"), b"key").unwrap()), 1);

    let first = basic_event(10, 40 * SECOND_NS, "schema-v0", Some(b"key"), "first");
    let second = basic_event(11, 50 * SECOND_NS, "schema-v0", Some(b"other"), "second");
    let mut identity_limited =
        admission_with(AdmissionLimitsV0::new(1, 128 * 1_024 * 1_024).unwrap());
    accept(&mut identity_limited, &first);
    assert_eq!(
        identity_limited.apply_event(&second),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedIdentityCapacity
        ))
    );
    assert_eq!(
        identity_limited
            .materialize_retained_latest_by_key(maximum_config(100 * SECOND_NS as u64))
            .unwrap()
            .retention_usage()
            .accepted_events_considered(),
        1
    );

    let mut byte_limited = admission_with(AdmissionLimitsV0::new(1, 1).unwrap());
    assert_eq!(
        byte_limited.apply_event(&first),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedEventByteCapacity
        ))
    );
    assert_eq!(
        byte_limited
            .materialize_retained_latest_by_key(maximum_config(100 * SECOND_NS as u64))
            .unwrap()
            .retention_usage()
            .accepted_events_considered(),
        0
    );
}

#[test]
fn retained_capacity_bytes_and_replacement_use_only_retained_keys() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 10 * SECOND_NS, "old-v0", Some(b"a"), "old-a"),
        basic_event(2, 20 * SECOND_NS, "old-v0", Some(b"b"), "old-b"),
        basic_event(3, 70 * SECOND_NS, "kept-v0", Some(b"c"), "kept-old"),
        basic_event(4, 80 * SECOND_NS, "kept-v0", Some(b"c"), "kept-new"),
        basic_event(5, 90 * SECOND_NS, "clock-v0", Some(b"d"), "clock"),
    ] {
        accept(&mut state, &candidate);
    }
    assert_eq!(
        state
            .materialize_latest_by_key(latest_config(
                LatestByKeyLimitsV0::new(2, MAX_LATEST_INDEX_BYTES_V0).unwrap(),
            ))
            .unwrap_err(),
        LatestMaterializationErrorV0::IndexEntryCapacity {
            maximum: 2,
            attempted: 3,
        }
    );

    let expected_bytes =
        independent_entry_bytes("kept-v0", b"c") + independent_entry_bytes("clock-v0", b"d");
    let exact = state
        .materialize_retained_latest_by_key(retained_latest_config(
            20 * SECOND_NS as u64,
            LatestByKeyLimitsV0::new(2, expected_bytes).unwrap(),
        ))
        .unwrap();
    assert_eq!(exact.retention_usage().retained_events(), 3);
    assert_eq!(exact.latest_usage().accepted_events_considered(), 3);
    assert_eq!(exact.latest_usage().indexed_entries(), 2);
    assert_eq!(exact.latest_usage().index_bytes(), expected_bytes);
    assert_eq!(sequence(exact.get(&schema("kept-v0"), b"c").unwrap()), 4);
    drop(exact);

    assert_eq!(
        state
            .materialize_retained_latest_by_key(retained_latest_config(
                20 * SECOND_NS as u64,
                LatestByKeyLimitsV0::new(1, MAX_LATEST_INDEX_BYTES_V0).unwrap(),
            ))
            .unwrap_err(),
        LatestMaterializationErrorV0::IndexEntryCapacity {
            maximum: 1,
            attempted: 2,
        }
    );
    assert_eq!(
        state
            .materialize_retained_latest_by_key(retained_latest_config(
                20 * SECOND_NS as u64,
                LatestByKeyLimitsV0::new(2, expected_bytes - 1).unwrap(),
            ))
            .unwrap_err(),
        LatestMaterializationErrorV0::IndexByteCapacity {
            maximum: expected_bytes - 1,
            attempted: expected_bytes,
        }
    );
    assert_eq!(state.usage().accepted_identities(), 5);
}

#[test]
fn retained_rank_and_multi_key_iteration_are_canonical() {
    let mut state = admission();
    for candidate in [
        basic_event(31, 41 * SECOND_NS, "schema-a", Some(b"key-a"), "new"),
        basic_event(30, 40 * SECOND_NS, "schema-a", Some(b"key-a"), "old"),
        basic_event(21, 41 * SECOND_NS, "schema-a", Some(b"key-b"), "second"),
        basic_event(20, 41 * SECOND_NS, "schema-a", Some(b"key-b"), "first"),
        basic_event(10, 42 * SECOND_NS, "schema-b", Some(b"key-z"), "z"),
        basic_event(40, 90 * SECOND_NS, "clock-v0", Some(b"clock"), "clock"),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_retained_latest_by_key(maximum_config(20 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(
        view_tuples(&view),
        vec![
            ("clock-v0".to_owned(), b"clock".to_vec(), 40),
            ("schema-a".to_owned(), b"key-a".to_vec(), 31),
            ("schema-a".to_owned(), b"key-b".to_vec(), 21),
            ("schema-b".to_owned(), b"key-z".to_vec(), 10),
        ]
    );
}

fn permutation_events() -> [EventEnvelopeV0; 4] {
    [
        basic_event(1, 39 * SECOND_NS, "old-v0", Some(b"old"), "expired"),
        basic_event(2, 40 * SECOND_NS, "schema-v0", Some(b"key"), "before"),
        basic_event(3, 41 * SECOND_NS, "schema-v0", Some(b"key"), "after"),
        basic_event(4, 90 * SECOND_NS, "clock-v0", Some(b"clock"), "clock"),
    ]
}

fn observation_for_order(order: [usize; 4]) -> Option<ViewObservation> {
    let events = permutation_events();
    let mut state = admission();
    for index in order {
        if state.apply_event(&events[index]).ok()? != ApplyOutcomeV0::Accepted {
            return None;
        }
    }
    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    let view = state
        .materialize_retained_latest_by_key(maximum_config(160 * SECOND_NS as u64))
        .unwrap();
    Some((
        view.retention_cutoff_event_time_ns(),
        view_tuples(&view),
        view.retention_usage(),
        view.latest_usage(),
        view.source_watermark_ns(),
        view.is_source_finalized(),
    ))
}

#[test]
fn every_permitted_permutation_and_fresh_replay_converges() {
    let permutations = [
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
    let mut accepted_permutations = 0;
    let mut expected = None;
    for order in permutations {
        let Some(observation) = observation_for_order(order) else {
            continue;
        };
        accepted_permutations += 1;
        if let Some(expected) = &expected {
            assert_eq!(&observation, expected);
        } else {
            expected = Some(observation);
        }
    }
    assert_eq!(accepted_permutations, 6);
    let expected = expected.unwrap();
    assert_eq!(expected.0, Some(40 * SECOND_NS));
    assert_eq!(
        expected.1,
        vec![
            ("clock-v0".to_owned(), b"clock".to_vec(), 4),
            ("schema-v0".to_owned(), b"key".to_vec(), 3),
        ]
    );
    assert_eq!(observation_for_order([0, 1, 2, 3]).unwrap(), expected);
}

#[test]
fn finalization_rebuild_can_remove_expired_only_keys() {
    let mut state = admission();
    accept(
        &mut state,
        &basic_event(1, 40 * SECOND_NS, "schema-v0", Some(b"key"), "value"),
    );
    accept(
        &mut state,
        &basic_event(2, 90 * SECOND_NS, "clock-v0", Some(b"clock"), "clock"),
    );
    {
        let before = state
            .materialize_retained_latest_by_key(maximum_config(100 * SECOND_NS as u64))
            .unwrap();
        assert_eq!(before.retention_cutoff_event_time_ns(), Some(0));
        assert!(before.get(&schema("schema-v0"), b"key").is_some());
        assert!(!before.is_source_finalized());
    }
    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    let after = state
        .materialize_retained_latest_by_key(maximum_config(100 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(
        after.retention_cutoff_event_time_ns(),
        Some(100 * SECOND_NS)
    );
    assert!(after.get(&schema("schema-v0"), b"key").is_none());
    assert!(after.entries().next().is_none());
    assert_eq!(after.retention_usage().expired_events_excluded(), 2);
    assert!(after.is_source_finalized());
}

#[test]
fn retained_winner_is_pointer_identical_with_provenance() {
    let mut state = admission();
    let older = event(
        "source-v0",
        1,
        (40 * SECOND_NS, 400),
        "schema-v0",
        Some(b"key"),
        1,
        "older",
    );
    let winner = event(
        "source-v0",
        2,
        (41 * SECOND_NS, 500),
        "schema-v0",
        Some(b"key"),
        2,
        "winner",
    );
    accept(&mut state, &older);
    accept(&mut state, &winner);
    let admitted = state.accepted_events().collect::<Vec<_>>();
    let view = state
        .materialize_retained_latest_by_key(maximum_config(100 * SECOND_NS as u64))
        .unwrap();
    let observed = view.get(&schema("schema-v0"), b"key").unwrap();
    assert!(std::ptr::eq(observed, admitted[1]));
    assert_eq!(observed.provenance(), winner.provenance());
    assert_eq!(observed.sensitivity(), winner.sensitivity());
    assert_eq!(observed.ingest_time_ns(), 0);
}

fn primary_state() -> AdmissionStateV0 {
    let manifest = decode_scenario_manifest_v0(PRIMARY_SCENARIO).unwrap();
    let generator = SreGeneratorV0::new(GENERATOR_VERSION_V0, &manifest, "sre-v0-primary").unwrap();
    let mut state = primary_admission();
    for record in generator {
        match record.unwrap() {
            FixtureRecordV0::Event(candidate) => {
                state.apply_event(&candidate).unwrap();
            }
            FixtureRecordV0::EndOfInput(control) => assert_eq!(
                state.finalize_source(control.source_id(), control.finalized_through_ns()),
                FinalizeOutcomeV0::Finalized
            ),
        }
    }
    state
}

fn reference_latest<'a>(events: impl Iterator<Item = &'a EventEnvelopeV0>) -> Vec<LatestTuple> {
    let mut winners: BTreeMap<(String, Vec<u8>), &EventEnvelopeV0> = BTreeMap::new();
    for candidate in events {
        let Some(key) = candidate.key() else {
            continue;
        };
        let identity = (candidate.schema_id().as_str().to_owned(), key.to_vec());
        let rank = (
            candidate.event_time_ns(),
            candidate.source_position().synthetic_sequence(),
        );
        match winners.get_mut(&identity) {
            Some(current)
                if rank
                    > (
                        current.event_time_ns(),
                        current.source_position().synthetic_sequence(),
                    ) =>
            {
                *current = candidate;
            }
            None => {
                winners.insert(identity, candidate);
            }
            _ => {}
        }
    }
    winners
        .into_iter()
        .map(|((schema_id, key), winner)| (schema_id, key, sequence(winner)))
        .collect()
}

#[test]
fn primary_full_horizon_matches_task_006_and_short_horizon_batch_reference() {
    let state = primary_state();
    let epoch_horizon = (PRIMARY_END_NS - PRIMARY_START_NS) as u64;
    let retained = state
        .materialize_retained_latest_by_key(maximum_config(epoch_horizon))
        .unwrap();
    let full = state
        .materialize_latest_by_key(latest_config(LatestByKeyLimitsV0::maximum()))
        .unwrap();
    let full_tuples = full
        .entries()
        .map(|(schema_id, key, winner)| {
            (
                schema_id.as_str().to_owned(),
                key.to_vec(),
                sequence(winner),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(view_tuples(&retained), full_tuples);
    assert_eq!(
        retained.retention_usage().accepted_events_considered(),
        98_750
    );
    assert_eq!(retained.retention_usage().retained_events(), 98_750);
    assert_eq!(retained.retention_usage().expired_events_excluded(), 0);
    assert_eq!(retained.latest_usage().accepted_events_considered(), 98_750);
    assert_eq!(retained.latest_usage().indexed_entries(), 13);
    assert_eq!(retained.latest_usage().unkeyed_events_omitted(), 0);
    assert!(retained.is_source_finalized());
    drop(full);
    drop(retained);

    let short_horizon = 10 * 60 * SECOND_NS as u64;
    let cutoff = PRIMARY_END_NS - 10 * 60 * SECOND_NS;
    let reference = reference_latest(
        state
            .accepted_events()
            .filter(|candidate| candidate.event_time_ns() >= cutoff),
    );
    let short = state
        .materialize_retained_latest_by_key(maximum_config(short_horizon))
        .unwrap();
    assert_eq!(short.retention_cutoff_event_time_ns(), Some(cutoff));
    assert_eq!(view_tuples(&short), reference);
    assert!(short.retention_usage().expired_events_excluded() > 0);
    assert_usage_conserved(&short);

    let full = state
        .materialize_latest_by_key(latest_config(LatestByKeyLimitsV0::maximum()))
        .unwrap();
    for (schema_id, key, winner) in full.entries() {
        let retained_winner = short.get(schema_id, key);
        if winner.event_time_ns() >= cutoff {
            assert!(std::ptr::eq(retained_winner.unwrap(), winner));
        } else {
            assert!(retained_winner.is_none());
        }
    }
}

fn diagnostic(
    source_id: &str,
    schema_id: &str,
    key: &[u8],
    values: (&str, &str),
    sequences: (u64, u64),
    times: (i64, i64),
) -> String {
    let mut state = AdmissionStateV0::new(
        AdmissionConfigV0::new(
            ADMISSION_VERSION_V0,
            source(source_id),
            0,
            200 * SECOND_NS,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    );
    accept(
        &mut state,
        &event(
            source_id,
            sequences.0,
            (times.0, times.0 + 7),
            schema_id,
            Some(key),
            1,
            values.0,
        ),
    );
    accept(
        &mut state,
        &event(
            source_id,
            sequences.1,
            (times.1, times.1 + 9),
            schema_id,
            Some(key),
            2,
            values.1,
        ),
    );
    let config = maximum_config(100 * SECOND_NS as u64);
    let view = state.materialize_retained_latest_by_key(config).unwrap();
    format!(
        "{config:?}|{:?}|{:?}|{view:?}|{:?}|{:?}",
        view.retention_usage(),
        view.latest_usage(),
        RetainedLatestByKeyConfigErrorV0::UnsupportedVersion { actual: 1 },
        LatestMaterializationErrorV0::IndexInvariant,
    )
}

fn error_diagnostic(source_id: &str, schema_id: &str, keys: (&[u8], &[u8])) -> String {
    let mut state = AdmissionStateV0::new(
        AdmissionConfigV0::new(
            ADMISSION_VERSION_V0,
            source(source_id),
            0,
            200 * SECOND_NS,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    );
    accept(
        &mut state,
        &event(
            source_id,
            1,
            (40 * SECOND_NS, 41),
            schema_id,
            Some(keys.0),
            1,
            "first-v0",
        ),
    );
    accept(
        &mut state,
        &event(
            source_id,
            2,
            (41 * SECOND_NS, 42),
            schema_id,
            Some(keys.1),
            2,
            "secondv0",
        ),
    );
    let error = state
        .materialize_retained_latest_by_key(retained_latest_config(
            100 * SECOND_NS as u64,
            LatestByKeyLimitsV0::new(1, MAX_LATEST_INDEX_BYTES_V0).unwrap(),
        ))
        .unwrap_err();
    format!("{error:?}|{error}")
}

#[test]
fn diagnostics_are_content_independent_and_value_safe() {
    let first = diagnostic(
        "source-aa",
        "schema-aa",
        b"secret-aa",
        ("value-aa", "value-ab"),
        (111, 112),
        (40 * SECOND_NS, 41 * SECOND_NS),
    );
    let second = diagnostic(
        "source-bb",
        "schema-bb",
        b"secret-bb",
        ("value-ba", "value-bb"),
        (998, 999),
        (50 * SECOND_NS, 51 * SECOND_NS),
    );
    assert_eq!(first, second);
    let first_error = error_diagnostic("source-aa", "schema-aa", (b"secret-aa", b"secret-ab"));
    let second_error = error_diagnostic("source-bb", "schema-bb", (b"secret-ba", b"secret-bb"));
    assert_eq!(first_error, second_error);
    for forbidden in [
        "source-aa",
        "source-bb",
        "schema-aa",
        "schema-bb",
        "secret-aa",
        "secret-ab",
        "secret-ba",
        "secret-bb",
        "value-aa",
        "value-ab",
        "value-ba",
        "value-bb",
        "decant://",
        "111",
        "112",
        "998",
        "999",
    ] {
        assert!(!first.contains(forbidden));
        assert!(!second.contains(forbidden));
        assert!(!first_error.contains(forbidden));
        assert!(!second_error.contains(forbidden));
    }
}
