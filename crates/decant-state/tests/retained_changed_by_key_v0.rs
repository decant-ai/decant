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
    CHANGED_BY_KEY_VERSION_V0, ChangedByKeyConfigV0, ChangedByKeyLimitsV0,
    ChangedMaterializationErrorV0, EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigV0,
    FinalizeOutcomeV0, MAX_CHANGED_INDEX_BYTES_V0, MAX_CHANGED_INDEX_ENTRIES_V0,
    RETAINED_CHANGED_BY_KEY_VERSION_V0, RejectionReasonV0, RetainedChangedByKeyConfigErrorV0,
    RetainedChangedByKeyConfigV0, RetainedChangedByKeyViewV0,
    changed_index_entry_accounted_bytes_v0,
};

const SECOND_NS: i64 = 1_000_000_000;
const PRIMARY_START_NS: i64 = 1_767_225_600_000_000_000;
const PRIMARY_END_NS: i64 = 1_767_226_800_000_000_000;
const PRIMARY_SCENARIO: &[u8] =
    include_bytes!("../../decant-fixtures/contracts/sre-v0/scenario.json");

type ChangeTuple = (String, Vec<u8>, u64, u64);
type ViewObservation = (
    Option<i64>,
    Vec<ChangeTuple>,
    decant_state::EventTimeRetentionUsageV0,
    decant_state::ChangedByKeyUsageV0,
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

fn changed_config(limits: ChangedByKeyLimitsV0) -> ChangedByKeyConfigV0 {
    ChangedByKeyConfigV0::new(CHANGED_BY_KEY_VERSION_V0, limits).unwrap()
}

fn retained_changed_config(
    horizon_ns: u64,
    changed_limits: ChangedByKeyLimitsV0,
) -> RetainedChangedByKeyConfigV0 {
    RetainedChangedByKeyConfigV0::new(
        RETAINED_CHANGED_BY_KEY_VERSION_V0,
        EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, horizon_ns).unwrap(),
        changed_config(changed_limits),
    )
    .unwrap()
}

fn maximum_config(horizon_ns: u64) -> RetainedChangedByKeyConfigV0 {
    retained_changed_config(horizon_ns, ChangedByKeyLimitsV0::maximum())
}

fn accept(state: &mut AdmissionStateV0, candidate: &EventEnvelopeV0) {
    assert_eq!(state.apply_event(candidate), Ok(ApplyOutcomeV0::Accepted));
}

fn sequence(event: &EventEnvelopeV0) -> u64 {
    event.source_position().synthetic_sequence()
}

fn view_tuples(view: &RetainedChangedByKeyViewV0<'_>) -> Vec<ChangeTuple> {
    view.entries()
        .map(|(schema_id, key, observation)| {
            (
                schema_id.as_str().to_owned(),
                key.to_vec(),
                sequence(observation.previous()),
                sequence(observation.current()),
            )
        })
        .collect()
}

fn assert_usage_conserved(view: &RetainedChangedByKeyViewV0<'_>) {
    let retention = view.retention_usage();
    let changed = view.changed_usage();
    assert_eq!(
        retention.accepted_events_considered(),
        retention.retained_events() + retention.expired_events_excluded()
    );
    assert_eq!(
        retention.retained_events(),
        changed.accepted_events_considered()
    );
}

#[test]
fn combined_config_is_versioned_and_preserves_nested_boundaries() {
    let retention_minimum =
        EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, 1).unwrap();
    let retention_maximum =
        EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, u64::MAX).unwrap();
    let changed_minimum = changed_config(ChangedByKeyLimitsV0::new(1, 1).unwrap());
    let changed_maximum = changed_config(
        ChangedByKeyLimitsV0::new(MAX_CHANGED_INDEX_ENTRIES_V0, MAX_CHANGED_INDEX_BYTES_V0)
            .unwrap(),
    );

    let minimum = RetainedChangedByKeyConfigV0::new(
        RETAINED_CHANGED_BY_KEY_VERSION_V0,
        retention_minimum,
        changed_minimum,
    )
    .unwrap();
    let maximum = RetainedChangedByKeyConfigV0::new(
        RETAINED_CHANGED_BY_KEY_VERSION_V0,
        retention_maximum,
        changed_maximum,
    )
    .unwrap();
    assert_eq!(minimum.version(), RETAINED_CHANGED_BY_KEY_VERSION_V0);
    assert_eq!(minimum.retention().retention_horizon_ns(), 1);
    assert_eq!(minimum.changed().limits().index_entry_limit(), 1);
    assert_eq!(maximum.retention().retention_horizon_ns(), u64::MAX);
    assert_eq!(maximum.changed().limits(), ChangedByKeyLimitsV0::maximum());
    assert_eq!(
        RetainedChangedByKeyConfigV0::new(1, retention_minimum, changed_minimum),
        Err(RetainedChangedByKeyConfigErrorV0::UnsupportedVersion { actual: 1 })
    );
}

#[test]
fn empty_admission_has_exact_empty_composed_evidence() {
    let state = admission();
    let view = state
        .materialize_retained_changed_by_key(maximum_config(20 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(view.retention_cutoff_event_time_ns(), None);
    assert_eq!(view.source_watermark_ns(), None);
    assert!(!view.is_source_finalized());
    assert_eq!(view.retention_usage().accepted_events_considered(), 0);
    assert_eq!(view.retention_usage().retained_events(), 0);
    assert_eq!(view.retention_usage().expired_events_excluded(), 0);
    assert_eq!(view.changed_usage().tracked_entries(), 0);
    assert_eq!(view.changed_usage().changed_entries(), 0);
    assert_eq!(view.changed_usage().accepted_events_considered(), 0);
    assert_usage_conserved(&view);
    assert!(view.entries().next().is_none());
    assert!(!view.is_tracked(&schema("schema-v0"), b"key-v0"));
    assert!(view.get(&schema("schema-v0"), b"key-v0").is_none());
}

#[test]
fn cutoff_membership_precedes_change_reduction_at_exact_nanoseconds() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 39 * SECOND_NS, "target-v0", Some(b"key"), "expired"),
        basic_event(2, 40 * SECOND_NS, "target-v0", Some(b"key"), "before"),
        basic_event(3, 40 * SECOND_NS + 1, "target-v0", Some(b"key"), "after"),
        basic_event(4, 90 * SECOND_NS, "clock-v0", Some(b"clock"), "clock"),
    ] {
        accept(&mut state, &candidate);
    }

    let view = state
        .materialize_retained_changed_by_key(maximum_config(20 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(view.retention_cutoff_event_time_ns(), Some(40 * SECOND_NS));
    assert_eq!(view.retention_usage().accepted_events_considered(), 4);
    assert_eq!(view.retention_usage().retained_events(), 3);
    assert_eq!(view.retention_usage().expired_events_excluded(), 1);
    assert_eq!(view.changed_usage().accepted_events_considered(), 3);
    assert_eq!(view.changed_usage().tracked_entries(), 2);
    assert_eq!(view.changed_usage().changed_entries(), 1);
    let observed = view.get(&schema("target-v0"), b"key").unwrap();
    assert_eq!(sequence(observed.previous()), 2);
    assert_eq!(sequence(observed.current()), 3);
    assert_usage_conserved(&view);
}

#[test]
fn expired_only_keys_disappear_and_expired_previous_cannot_form_a_pair() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 39 * SECOND_NS, "target-v0", Some(b"target"), "before"),
        basic_event(2, 40 * SECOND_NS, "target-v0", Some(b"target"), "after"),
        basic_event(3, 39 * SECOND_NS, "old-v0", Some(b"old"), "old"),
        basic_event(4, 90 * SECOND_NS, "clock-v0", Some(b"clock"), "clock"),
    ] {
        accept(&mut state, &candidate);
    }

    let full = state
        .materialize_changed_by_key(changed_config(ChangedByKeyLimitsV0::maximum()))
        .unwrap();
    let full_pair = full.get(&schema("target-v0"), b"target").unwrap();
    assert_eq!(sequence(full_pair.previous()), 1);
    assert_eq!(sequence(full_pair.current()), 2);
    drop(full);

    let retained = state
        .materialize_retained_changed_by_key(maximum_config(20 * SECOND_NS as u64))
        .unwrap();
    assert!(retained.is_tracked(&schema("target-v0"), b"target"));
    assert!(retained.get(&schema("target-v0"), b"target").is_none());
    assert!(!retained.is_tracked(&schema("old-v0"), b"old"));
    assert!(retained.get(&schema("old-v0"), b"old").is_none());
}

#[test]
fn retained_return_to_value_uses_only_retained_history() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 39 * SECOND_NS, "schema-v0", Some(b"key"), "expired-a"),
        basic_event(2, 40 * SECOND_NS, "schema-v0", Some(b"key"), "b"),
        basic_event(3, 41 * SECOND_NS, "schema-v0", Some(b"key"), "a"),
        basic_event(4, 42 * SECOND_NS, "schema-v0", Some(b"key"), "a"),
        basic_event(5, 90 * SECOND_NS, "clock-v0", Some(b"clock"), "clock"),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_retained_changed_by_key(maximum_config(20 * SECOND_NS as u64))
        .unwrap();
    let observed = view.get(&schema("schema-v0"), b"key").unwrap();
    assert_eq!(sequence(observed.previous()), 2);
    assert_eq!(sequence(observed.current()), 4);
    assert_eq!(view.changed_usage().changed_entries(), 1);
}

#[test]
fn schema_empty_key_and_unkeyed_accounting_are_retention_aware() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 39 * SECOND_NS, "schema-a", None, "expired-unkeyed"),
        basic_event(2, 40 * SECOND_NS, "schema-a", None, "retained-unkeyed"),
        basic_event(3, 40 * SECOND_NS, "schema-a", Some(b""), "a-before"),
        basic_event(4, 41 * SECOND_NS, "schema-a", Some(b""), "a-after"),
        basic_event(5, 40 * SECOND_NS, "schema-b", Some(b""), "b-before"),
        basic_event(6, 41 * SECOND_NS, "schema-b", Some(b""), "b-after"),
        basic_event(7, 90 * SECOND_NS, "clock-v0", Some(b"clock"), "clock"),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_retained_changed_by_key(maximum_config(20 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(view.retention_usage().accepted_events_considered(), 7);
    assert_eq!(view.retention_usage().retained_events(), 6);
    assert_eq!(view.retention_usage().expired_events_excluded(), 1);
    assert_eq!(view.changed_usage().accepted_events_considered(), 6);
    assert_eq!(view.changed_usage().unkeyed_events_omitted(), 1);
    assert!(view.get(&schema("schema-a"), b"").is_some());
    assert!(view.get(&schema("schema-b"), b"").is_some());
    assert_usage_conserved(&view);
}

#[test]
fn duplicates_and_semantic_rejections_cannot_enter_retained_changes() {
    let mut state = admission();
    let original = basic_event(1, 40 * SECOND_NS, "schema-v0", Some(b"key"), "before");
    accept(&mut state, &original);
    let duplicate = event(
        "source-v0",
        1,
        (40 * SECOND_NS, 99 * SECOND_NS),
        "schema-v0",
        Some(b"key"),
        0,
        "before",
    );
    assert_eq!(
        state.apply_event(&duplicate),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    assert_eq!(
        state.apply_event(&basic_event(
            1,
            40 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            "conflict",
        )),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::ConflictingRedelivery
        ))
    );
    assert_eq!(
        state.apply_event(&event(
            "other-v0",
            2,
            (50 * SECOND_NS, 0),
            "schema-v0",
            Some(b"key"),
            0,
            "wrong",
        )),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::UnexpectedSource
        ))
    );
    assert_eq!(
        state.apply_event(&basic_event(
            3,
            200 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            "outside",
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch))
    );
    accept(
        &mut state,
        &basic_event(4, 90 * SECOND_NS, "schema-v0", Some(b"key"), "after"),
    );
    assert_eq!(
        state.apply_event(&basic_event(
            5,
            60 * SECOND_NS - 1,
            "schema-v0",
            Some(b"key"),
            "late",
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::Late))
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
        .materialize_retained_changed_by_key(maximum_config(200 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(view.retention_usage().accepted_events_considered(), 2);
    assert_eq!(view.changed_usage().accepted_events_considered(), 2);
    let observed = view.get(&schema("schema-v0"), b"key").unwrap();
    assert_eq!(sequence(observed.previous()), 1);
    assert_eq!(sequence(observed.current()), 4);

    let first = basic_event(10, 40 * SECOND_NS, "schema-v0", Some(b"key"), "value");
    let second = basic_event(11, 50 * SECOND_NS, "schema-v0", Some(b"other"), "value");
    let mut identity_limited =
        admission_with(AdmissionLimitsV0::new(1, 128 * 1_024 * 1_024).unwrap());
    assert_eq!(
        identity_limited.apply_event(&first),
        Ok(ApplyOutcomeV0::Accepted)
    );
    assert_eq!(
        identity_limited.apply_event(&second),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedIdentityCapacity
        ))
    );
    let identity_limited_view = identity_limited
        .materialize_retained_changed_by_key(maximum_config(100 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(
        identity_limited_view
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
        byte_limited.apply_event(&second),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedEventByteCapacity
        ))
    );
    let byte_limited_view = byte_limited
        .materialize_retained_changed_by_key(maximum_config(100 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(
        byte_limited_view
            .retention_usage()
            .accepted_events_considered(),
        0
    );
}

fn capacity_state() -> AdmissionStateV0 {
    let mut state = admission();
    for candidate in [
        basic_event(1, 10 * SECOND_NS, "old-v0", Some(b"a"), "old-a"),
        basic_event(2, 20 * SECOND_NS, "old-v0", Some(b"b"), "old-b"),
        basic_event(3, 40 * SECOND_NS, "kept-v0", Some(b"c"), "kept"),
        basic_event(4, 90 * SECOND_NS, "clock-v0", Some(b"d"), "clock"),
    ] {
        accept(&mut state, &candidate);
    }
    state
}

#[test]
fn changed_capacity_and_bytes_apply_only_after_retention() {
    let state = capacity_state();
    let full_error = state
        .materialize_changed_by_key(changed_config(
            ChangedByKeyLimitsV0::new(2, MAX_CHANGED_INDEX_BYTES_V0).unwrap(),
        ))
        .unwrap_err();
    assert_eq!(
        full_error,
        ChangedMaterializationErrorV0::IndexEntryCapacity {
            maximum: 2,
            attempted: 3,
        }
    );

    let expected_bytes = changed_index_entry_accounted_bytes_v0(&schema("kept-v0"), b"c", false)
        .unwrap()
        + changed_index_entry_accounted_bytes_v0(&schema("clock-v0"), b"d", false).unwrap();
    let retained = state
        .materialize_retained_changed_by_key(retained_changed_config(
            20 * SECOND_NS as u64,
            ChangedByKeyLimitsV0::new(2, expected_bytes).unwrap(),
        ))
        .unwrap();
    assert_eq!(retained.retention_usage().retained_events(), 2);
    assert_eq!(retained.changed_usage().tracked_entries(), 2);
    assert_eq!(retained.changed_usage().index_bytes(), expected_bytes);

    assert_eq!(
        state
            .materialize_retained_changed_by_key(retained_changed_config(
                20 * SECOND_NS as u64,
                ChangedByKeyLimitsV0::new(1, MAX_CHANGED_INDEX_BYTES_V0).unwrap(),
            ))
            .unwrap_err(),
        ChangedMaterializationErrorV0::IndexEntryCapacity {
            maximum: 1,
            attempted: 2,
        }
    );
    assert_eq!(
        state
            .materialize_retained_changed_by_key(retained_changed_config(
                20 * SECOND_NS as u64,
                ChangedByKeyLimitsV0::new(2, expected_bytes - 1).unwrap(),
            ))
            .unwrap_err(),
        ChangedMaterializationErrorV0::IndexByteCapacity {
            maximum: expected_bytes - 1,
            attempted: expected_bytes,
        }
    );
    assert_eq!(state.usage().accepted_identities(), 4);
}

#[test]
fn retained_first_previous_coordinate_is_preflighted() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 40 * SECOND_NS, "schema-v0", Some(b"key"), "before"),
        basic_event(2, 41 * SECOND_NS, "schema-v0", Some(b"key"), "after"),
        basic_event(3, 90 * SECOND_NS, "clock-v0", Some(b"clock"), "clock"),
    ] {
        accept(&mut state, &candidate);
    }
    let target_base =
        changed_index_entry_accounted_bytes_v0(&schema("schema-v0"), b"key", false).unwrap();
    let clock_base =
        changed_index_entry_accounted_bytes_v0(&schema("clock-v0"), b"clock", false).unwrap();
    assert_eq!(
        state
            .materialize_retained_changed_by_key(retained_changed_config(
                20 * SECOND_NS as u64,
                ChangedByKeyLimitsV0::new(2, target_base + 7).unwrap(),
            ))
            .unwrap_err(),
        ChangedMaterializationErrorV0::IndexByteCapacity {
            maximum: target_base + 7,
            attempted: target_base + 8,
        }
    );
    let exact = state
        .materialize_retained_changed_by_key(retained_changed_config(
            20 * SECOND_NS as u64,
            ChangedByKeyLimitsV0::new(2, target_base + 8 + clock_base).unwrap(),
        ))
        .unwrap();
    assert_eq!(exact.changed_usage().tracked_entries(), 2);
    assert_eq!(exact.changed_usage().changed_entries(), 1);
    assert_eq!(
        exact.changed_usage().index_bytes(),
        target_base + 8 + clock_base
    );
    assert_eq!(state.usage().accepted_identities(), 3);
}

#[test]
fn retained_equal_time_tie_and_multi_key_iteration_are_canonical() {
    let mut state = admission();
    for candidate in [
        basic_event(30, 40 * SECOND_NS, "schema-a", Some(b"key-a"), "before"),
        basic_event(31, 41 * SECOND_NS, "schema-a", Some(b"key-a"), "after"),
        basic_event(20, 40 * SECOND_NS, "schema-a", Some(b"key-b"), "before"),
        basic_event(21, 41 * SECOND_NS, "schema-a", Some(b"key-b"), "after"),
        basic_event(10, 40 * SECOND_NS, "schema-b", Some(b"key-z"), "before"),
        basic_event(11, 40 * SECOND_NS, "schema-b", Some(b"key-z"), "after"),
        basic_event(40, 90 * SECOND_NS, "clock-v0", Some(b"clock"), "clock"),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_retained_changed_by_key(maximum_config(20 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(
        view_tuples(&view),
        vec![
            ("schema-a".to_owned(), b"key-a".to_vec(), 30, 31),
            ("schema-a".to_owned(), b"key-b".to_vec(), 20, 21),
            ("schema-b".to_owned(), b"key-z".to_vec(), 10, 11),
        ]
    );
}

fn permutation_events() -> [EventEnvelopeV0; 4] {
    [
        basic_event(1, 39 * SECOND_NS, "schema-v0", Some(b"key"), "expired"),
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
        .materialize_retained_changed_by_key(maximum_config(160 * SECOND_NS as u64))
        .unwrap();
    Some((
        view.retention_cutoff_event_time_ns(),
        view_tuples(&view),
        view.retention_usage(),
        view.changed_usage(),
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
        vec![("schema-v0".to_owned(), b"key".to_vec(), 2, 3)]
    );
    assert_eq!(observation_for_order([0, 1, 2, 3]).unwrap(), expected);
}

#[test]
fn finalization_rebuild_can_remove_a_transition() {
    let mut state = admission();
    accept(
        &mut state,
        &basic_event(1, 40 * SECOND_NS, "schema-v0", Some(b"key"), "before"),
    );
    accept(
        &mut state,
        &basic_event(2, 90 * SECOND_NS, "schema-v0", Some(b"key"), "after"),
    );
    {
        let before = state
            .materialize_retained_changed_by_key(maximum_config(100 * SECOND_NS as u64))
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
        .materialize_retained_changed_by_key(maximum_config(100 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(
        after.retention_cutoff_event_time_ns(),
        Some(100 * SECOND_NS)
    );
    assert!(!after.is_tracked(&schema("schema-v0"), b"key"));
    assert!(after.entries().next().is_none());
    assert_eq!(after.retention_usage().expired_events_excluded(), 2);
    assert!(after.is_source_finalized());
}

#[test]
fn retained_change_evidence_is_pointer_identical_with_provenance() {
    let mut state = admission();
    let before = event(
        "source-v0",
        1,
        (40 * SECOND_NS, 400),
        "schema-v0",
        Some(b"key"),
        1,
        "before",
    );
    let after = event(
        "source-v0",
        2,
        (41 * SECOND_NS, 500),
        "schema-v0",
        Some(b"key"),
        2,
        "after",
    );
    accept(&mut state, &before);
    accept(&mut state, &after);
    let admitted = state.accepted_events().collect::<Vec<_>>();
    let view = state
        .materialize_retained_changed_by_key(maximum_config(100 * SECOND_NS as u64))
        .unwrap();
    let observed = view.get(&schema("schema-v0"), b"key").unwrap();
    assert!(std::ptr::eq(observed.previous(), admitted[0]));
    assert!(std::ptr::eq(observed.current(), admitted[1]));
    assert_eq!(observed.previous().provenance(), before.provenance());
    assert_eq!(observed.current().provenance(), after.provenance());
    assert_eq!(observed.previous().sensitivity(), before.sensitivity());
    assert_eq!(observed.current().sensitivity(), after.sensitivity());
    assert_eq!(observed.previous().ingest_time_ns(), 0);
    assert_eq!(observed.current().ingest_time_ns(), 0);
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

fn reference_changes<'a>(events: impl Iterator<Item = &'a EventEnvelopeV0>) -> Vec<ChangeTuple> {
    let mut groups: BTreeMap<(String, Vec<u8>), Vec<&EventEnvelopeV0>> = BTreeMap::new();
    for candidate in events {
        let Some(key) = candidate.key() else {
            continue;
        };
        groups
            .entry((candidate.schema_id().as_str().to_owned(), key.to_vec()))
            .or_default()
            .push(candidate);
    }

    let mut result = Vec::new();
    for ((schema_id, key), mut observations) in groups {
        observations.sort_by_key(|candidate| {
            (
                candidate.event_time_ns(),
                candidate.source_position().synthetic_sequence(),
            )
        });
        let current = observations.last().unwrap();
        let previous = observations
            .iter()
            .rev()
            .find(|candidate| candidate.payload() != current.payload());
        if let Some(previous) = previous {
            result.push((schema_id, key, sequence(previous), sequence(current)));
        }
    }
    result
}

#[test]
fn primary_full_horizon_matches_task_008_and_short_horizon_batch_reference() {
    let state = primary_state();
    let epoch_horizon = (PRIMARY_END_NS - PRIMARY_START_NS) as u64;
    let retained = state
        .materialize_retained_changed_by_key(maximum_config(epoch_horizon))
        .unwrap();
    let full = state
        .materialize_changed_by_key(changed_config(ChangedByKeyLimitsV0::maximum()))
        .unwrap();
    let full_tuples = full
        .entries()
        .map(|(schema_id, key, observation)| {
            (
                schema_id.as_str().to_owned(),
                key.to_vec(),
                sequence(observation.previous()),
                sequence(observation.current()),
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
    assert_eq!(
        retained.changed_usage().accepted_events_considered(),
        98_750
    );
    assert_eq!(retained.changed_usage().tracked_entries(), 13);
    assert_eq!(retained.changed_usage().changed_entries(), 12);
    assert!(retained.is_source_finalized());
    drop(full);
    drop(retained);

    let short_horizon = 10 * 60 * SECOND_NS as u64;
    let cutoff = PRIMARY_END_NS - 10 * 60 * SECOND_NS;
    let reference = reference_changes(
        state
            .accepted_events()
            .filter(|candidate| candidate.event_time_ns() >= cutoff),
    );
    let short = state
        .materialize_retained_changed_by_key(maximum_config(short_horizon))
        .unwrap();
    assert_eq!(short.retention_cutoff_event_time_ns(), Some(cutoff));
    assert_eq!(view_tuples(&short), reference);
    assert!(short.retention_usage().expired_events_excluded() > 0);
    assert_usage_conserved(&short);
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
    let view = state.materialize_retained_changed_by_key(config).unwrap();
    let observation = view.get(&schema(schema_id), key).unwrap();
    format!(
        "{config:?}|{:?}|{:?}|{observation:?}|{view:?}|{:?}",
        view.retention_usage(),
        view.changed_usage(),
        RetainedChangedByKeyConfigErrorV0::UnsupportedVersion { actual: 1 }
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
        .materialize_retained_changed_by_key(retained_changed_config(
            100 * SECOND_NS as u64,
            ChangedByKeyLimitsV0::new(1, MAX_CHANGED_INDEX_BYTES_V0).unwrap(),
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
