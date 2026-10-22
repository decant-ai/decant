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
    CountMaterializationErrorV0, EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigV0,
    EventTimeRetentionUsageV0, FinalizeOutcomeV0, MAX_ACCEPTED_EVENT_BYTES_V0,
    MAX_TUMBLING_COUNT_INDEX_BYTES_V0, RETAINED_TUMBLING_COUNT_VERSION_V0, RejectionReasonV0,
    RetainedTumblingCountConfigErrorV0, RetainedTumblingCountConfigV0, RetainedTumblingCountViewV0,
    TUMBLING_COUNT_VERSION_V0, TumblingCountConfigV0, TumblingCountLimitsV0, TumblingCountUsageV0,
    accepted_event_accounted_bytes_v0, tumbling_count_bucket_accounted_bytes_v0,
    tumbling_count_contribution_accounted_bytes_v0,
};

const SECOND_NS: i64 = 1_000_000_000;
const PRIMARY_START_NS: i64 = 1_767_225_600_000_000_000;
const PRIMARY_END_NS: i64 = 1_767_226_800_000_000_000;
const PRIMARY_SCENARIO: &[u8] =
    include_bytes!("../../decant-fixtures/contracts/sre-v0/scenario.json");

type BucketTuple = (
    String,
    Vec<u8>,
    u64,
    i64,
    i64,
    i64,
    u64,
    bool,
    bool,
    Vec<u64>,
);

type ViewEvidence = (
    Option<i64>,
    Vec<BucketTuple>,
    EventTimeRetentionUsageV0,
    TumblingCountUsageV0,
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
) -> EventEnvelopeV0 {
    event(
        "source-v0",
        sequence,
        (event_time_ns, event_time_ns),
        schema_id,
        key,
        0,
        "synthetic-value",
    )
}

fn admission_for(
    epoch_start_ns: i64,
    epoch_end_ns: i64,
    limits: AdmissionLimitsV0,
) -> AdmissionStateV0 {
    AdmissionStateV0::new(
        AdmissionConfigV0::new(
            ADMISSION_VERSION_V0,
            source("source-v0"),
            epoch_start_ns,
            epoch_end_ns,
            limits,
        )
        .unwrap(),
    )
}

fn admission() -> AdmissionStateV0 {
    admission_for(0, 200 * SECOND_NS, AdmissionLimitsV0::maximum())
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

fn count_config(window_width_ns: u64, limits: TumblingCountLimitsV0) -> TumblingCountConfigV0 {
    TumblingCountConfigV0::new(TUMBLING_COUNT_VERSION_V0, window_width_ns, limits).unwrap()
}

fn retained_count_config(
    horizon_ns: u64,
    window_width_ns: u64,
    limits: TumblingCountLimitsV0,
) -> RetainedTumblingCountConfigV0 {
    RetainedTumblingCountConfigV0::new(
        RETAINED_TUMBLING_COUNT_VERSION_V0,
        EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, horizon_ns).unwrap(),
        count_config(window_width_ns, limits),
    )
    .unwrap()
}

fn maximum_config(horizon_ns: u64, window_width_ns: u64) -> RetainedTumblingCountConfigV0 {
    retained_count_config(
        horizon_ns,
        window_width_ns,
        TumblingCountLimitsV0::maximum(),
    )
}

fn accept(state: &mut AdmissionStateV0, candidate: &EventEnvelopeV0) {
    assert_eq!(state.apply_event(candidate), Ok(ApplyOutcomeV0::Accepted));
}

fn view_tuples(view: &RetainedTumblingCountViewV0<'_>) -> Vec<BucketTuple> {
    view.entries()
        .map(|(schema_id, key, observation)| {
            let contributors = view
                .contributors(schema_id, key, observation.window_ordinal())
                .unwrap()
                .map(|(position, _)| position.synthetic_sequence())
                .collect();
            (
                schema_id.as_str().to_owned(),
                key.to_vec(),
                observation.window_ordinal(),
                observation.window_start_ns(),
                observation.window_end_ns(),
                observation.retained_interval_start_ns(),
                observation.count(),
                observation.is_cutoff_partial(),
                observation.is_finalized(),
                contributors,
            )
        })
        .collect()
}

fn assert_usage_conserved(view: &RetainedTumblingCountViewV0<'_>) {
    let retention = view.retention_usage();
    assert_eq!(
        retention.accepted_events_considered(),
        retention.retained_events() + retention.expired_events_excluded()
    );
    assert_eq!(
        retention.retained_events(),
        view.count_usage().accepted_events_considered()
    );
}

#[test]
fn combined_config_is_versioned_and_preserves_nested_boundaries() {
    let minimum_retention = EventTimeRetentionConfigV0::new(0, 1).unwrap();
    let maximum_retention = EventTimeRetentionConfigV0::new(0, u64::MAX).unwrap();
    let minimum_count = count_config(1, TumblingCountLimitsV0::new(1, 1).unwrap());
    let maximum_count = count_config(u64::MAX, TumblingCountLimitsV0::maximum());
    let minimum = RetainedTumblingCountConfigV0::new(0, minimum_retention, minimum_count).unwrap();
    let maximum = RetainedTumblingCountConfigV0::new(0, maximum_retention, maximum_count).unwrap();
    assert_eq!(minimum.version(), 0);
    assert_eq!(minimum.retention(), minimum_retention);
    assert_eq!(minimum.count(), minimum_count);
    assert_eq!(maximum.retention(), maximum_retention);
    assert_eq!(maximum.count(), maximum_count);
    assert_eq!(
        RetainedTumblingCountConfigV0::new(1, minimum_retention, minimum_count),
        Err(RetainedTumblingCountConfigErrorV0::UnsupportedVersion { actual: 1 })
    );
}

#[test]
fn empty_admission_has_exact_empty_composed_evidence() {
    let state = admission();
    let config = maximum_config(100 * SECOND_NS as u64, 50 * SECOND_NS as u64);
    let view = state.materialize_retained_tumbling_count(config).unwrap();
    assert_eq!(view.config(), config);
    assert_eq!(view.retention_cutoff_event_time_ns(), None);
    assert_eq!(view.retention_usage().accepted_events_considered(), 0);
    assert_eq!(view.retention_usage().retained_events(), 0);
    assert_eq!(view.retention_usage().expired_events_excluded(), 0);
    assert_eq!(view.count_usage().accepted_events_considered(), 0);
    assert_eq!(view.count_usage().represented_buckets(), 0);
    assert_eq!(view.count_usage().contributions(), 0);
    assert_eq!(view.count_usage().index_bytes(), 0);
    assert_eq!(view.count_usage().unkeyed_events_omitted(), 0);
    assert_eq!(view.source_watermark_ns(), None);
    assert!(!view.is_source_finalized());
    assert!(view.get(&schema("schema-v0"), b"key", 0).is_none());
    assert!(view.entries().next().is_none());
    assert_usage_conserved(&view);
}

#[test]
fn cutoff_bisects_bucket_without_disguising_expired_contributors() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 10 * SECOND_NS, "expired-v0", Some(b"old")),
        basic_event(2, 39 * SECOND_NS, "schema-v0", Some(b"key")),
        basic_event(3, 40 * SECOND_NS, "schema-v0", Some(b"key")),
        basic_event(4, 41 * SECOND_NS, "schema-v0", Some(b"key")),
        basic_event(5, 90 * SECOND_NS, "clock-v0", Some(b"clock")),
    ] {
        accept(&mut state, &candidate);
    }

    let view = state
        .materialize_retained_tumbling_count(maximum_config(
            20 * SECOND_NS as u64,
            50 * SECOND_NS as u64,
        ))
        .unwrap();
    assert_eq!(view.retention_cutoff_event_time_ns(), Some(40 * SECOND_NS));
    assert!(view.get(&schema("expired-v0"), b"old", 0).is_none());
    let observed = view.get(&schema("schema-v0"), b"key", 0).unwrap();
    assert_eq!(observed.window_ordinal(), 0);
    assert_eq!(observed.window_start_ns(), 0);
    assert_eq!(observed.window_end_ns(), 50 * SECOND_NS);
    assert_eq!(observed.retained_interval_start_ns(), 40 * SECOND_NS);
    assert_eq!(observed.count(), 2);
    assert!(observed.is_cutoff_partial());
    assert!(observed.is_finalized());
    assert_eq!(
        view.contributors(&schema("schema-v0"), b"key", 0)
            .unwrap()
            .map(|(position, _)| position.synthetic_sequence())
            .collect::<Vec<_>>(),
        vec![3, 4]
    );
    assert_eq!(view.retention_usage().accepted_events_considered(), 5);
    assert_eq!(view.retention_usage().retained_events(), 3);
    assert_eq!(view.retention_usage().expired_events_excluded(), 2);
    assert_eq!(view.count_usage().represented_buckets(), 2);
    assert_eq!(view.count_usage().contributions(), 3);
    assert_usage_conserved(&view);
}

#[test]
fn cutoff_at_window_start_is_not_partial_and_prior_bucket_is_absent() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 10 * SECOND_NS, "schema-v0", Some(b"key")),
        basic_event(2, 50 * SECOND_NS, "schema-v0", Some(b"key")),
        basic_event(3, 90 * SECOND_NS, "schema-v0", Some(b"key")),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_retained_tumbling_count(maximum_config(
            10 * SECOND_NS as u64,
            50 * SECOND_NS as u64,
        ))
        .unwrap();
    assert_eq!(view.retention_cutoff_event_time_ns(), Some(50 * SECOND_NS));
    assert!(view.get(&schema("schema-v0"), b"key", 0).is_none());
    let observed = view.get(&schema("schema-v0"), b"key", 1).unwrap();
    assert_eq!(observed.retained_interval_start_ns(), 50 * SECOND_NS);
    assert_eq!(observed.count(), 2);
    assert!(!observed.is_cutoff_partial());
    assert_eq!(view.entries().count(), 1);
}

#[test]
fn final_epoch_clamp_and_signed_extremes_remain_exact() {
    let mut state = admission_for(i64::MIN, i64::MAX, AdmissionLimitsV0::maximum());
    accept(
        &mut state,
        &basic_event(1, i64::MIN, "schema-v0", Some(b"key")),
    );
    accept(
        &mut state,
        &basic_event(2, i64::MAX - 1, "schema-v0", Some(b"key")),
    );
    let view = state
        .materialize_retained_tumbling_count(maximum_config(u64::MAX, u64::MAX))
        .unwrap();
    assert_eq!(view.retention_cutoff_event_time_ns(), Some(i64::MIN));
    let observed = view.get(&schema("schema-v0"), b"key", 0).unwrap();
    assert_eq!(observed.window_start_ns(), i64::MIN);
    assert_eq!(observed.window_end_ns(), i64::MAX);
    assert_eq!(observed.retained_interval_start_ns(), i64::MIN);
    assert_eq!(observed.count(), 2);
    assert!(!observed.is_cutoff_partial());
}

#[test]
fn schema_empty_key_and_unkeyed_accounting_are_retention_aware() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 10 * SECOND_NS, "old-v0", None),
        basic_event(2, 40 * SECOND_NS, "kept-v0", None),
        basic_event(3, 41 * SECOND_NS, "schema-a", Some(b"")),
        basic_event(4, 42 * SECOND_NS, "schema-b", Some(b"")),
        basic_event(5, 90 * SECOND_NS, "clock-v0", Some(b"clock")),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_retained_tumbling_count(maximum_config(
            20 * SECOND_NS as u64,
            50 * SECOND_NS as u64,
        ))
        .unwrap();
    assert_eq!(view.retention_usage().retained_events(), 4);
    assert_eq!(view.retention_usage().expired_events_excluded(), 1);
    assert_eq!(view.count_usage().accepted_events_considered(), 4);
    assert_eq!(view.count_usage().unkeyed_events_omitted(), 1);
    assert_eq!(view.count_usage().represented_buckets(), 3);
    assert_eq!(view.get(&schema("schema-a"), b"", 0).unwrap().count(), 1);
    assert_eq!(view.get(&schema("schema-b"), b"", 0).unwrap().count(), 1);
    assert_usage_conserved(&view);
}

#[test]
fn duplicates_and_semantic_rejections_cannot_contribute() {
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
        &basic_event(2, 90 * SECOND_NS, "clock-v0", Some(b"clock")),
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
        state.apply_event(&basic_event(5, 200 * SECOND_NS, "schema-v0", Some(b"key"),)),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch))
    );
    let view = state
        .materialize_retained_tumbling_count(maximum_config(
            100 * SECOND_NS as u64,
            50 * SECOND_NS as u64,
        ))
        .unwrap();
    assert_eq!(view.retention_usage().accepted_events_considered(), 2);
    assert_eq!(view.count_usage().contributions(), 2);
    assert_eq!(
        view.get(&schema("schema-v0"), b"key", 0).unwrap().count(),
        1
    );
}

#[test]
fn admission_capacity_and_finalization_rejections_cannot_contribute() {
    let first = basic_event(1, 40 * SECOND_NS, "schema-v0", Some(b"key"));
    let second = basic_event(2, 41 * SECOND_NS, "schema-v0", Some(b"other"));
    let first_bytes = accepted_event_accounted_bytes_v0(&first).unwrap();

    let mut identity_limited = admission_for(
        0,
        200 * SECOND_NS,
        AdmissionLimitsV0::new(1, MAX_ACCEPTED_EVENT_BYTES_V0).unwrap(),
    );
    accept(&mut identity_limited, &first);
    assert_eq!(
        identity_limited.apply_event(&second),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedIdentityCapacity
        ))
    );

    let mut byte_limited = admission_for(
        0,
        200 * SECOND_NS,
        AdmissionLimitsV0::new(2, first_bytes).unwrap(),
    );
    accept(&mut byte_limited, &first);
    assert_eq!(
        byte_limited.apply_event(&second),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedEventByteCapacity
        ))
    );

    let mut finalized = admission();
    accept(&mut finalized, &first);
    assert_eq!(
        finalized.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    assert_eq!(
        finalized.apply_event(&second),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::EpochFinalized))
    );

    for state in [&identity_limited, &byte_limited, &finalized] {
        let view = state
            .materialize_retained_tumbling_count(maximum_config(u64::MAX, 50 * SECOND_NS as u64))
            .unwrap();
        assert_eq!(view.retention_usage().accepted_events_considered(), 1);
        assert_eq!(view.count_usage().contributions(), 1);
        assert_eq!(
            view.get(&schema("schema-v0"), b"key", 0).unwrap().count(),
            1
        );
        assert!(view.get(&schema("schema-v0"), b"other", 0).is_none());
    }
}

#[test]
fn retained_subset_alone_consumes_bucket_and_byte_capacity() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 10 * SECOND_NS, "old-a", Some(b"a")),
        basic_event(2, 60 * SECOND_NS, "old-b", Some(b"bb")),
        basic_event(3, 150 * SECOND_NS, "kept-a", Some(b"k")),
        basic_event(4, 190 * SECOND_NS, "clock-v0", Some(b"clock")),
    ] {
        accept(&mut state, &candidate);
    }
    let cutoff_horizon = 60 * SECOND_NS as u64;
    let width = 50 * SECOND_NS as u64;
    let kept_a_bytes = tumbling_count_bucket_accounted_bytes_v0(&schema("kept-a"), b"k").unwrap()
        + tumbling_count_contribution_accounted_bytes_v0();
    let clock_bytes = tumbling_count_bucket_accounted_bytes_v0(&schema("clock-v0"), b"clock")
        .unwrap()
        + tumbling_count_contribution_accounted_bytes_v0();
    let exact_bytes = kept_a_bytes + clock_bytes;
    let exact = retained_count_config(
        cutoff_horizon,
        width,
        TumblingCountLimitsV0::new(2, exact_bytes).unwrap(),
    );
    let view = state.materialize_retained_tumbling_count(exact).unwrap();
    assert_eq!(view.retention_cutoff_event_time_ns(), Some(100 * SECOND_NS));
    assert_eq!(view.retention_usage().expired_events_excluded(), 2);
    assert_eq!(view.count_usage().represented_buckets(), 2);
    assert_eq!(view.count_usage().index_bytes(), exact_bytes);
    drop(view);

    assert_eq!(
        state
            .materialize_retained_tumbling_count(retained_count_config(
                cutoff_horizon,
                width,
                TumblingCountLimitsV0::new(1, MAX_TUMBLING_COUNT_INDEX_BYTES_V0).unwrap(),
            ))
            .unwrap_err(),
        CountMaterializationErrorV0::BucketCapacity {
            maximum: 1,
            attempted: 2,
        }
    );
    assert_eq!(
        state
            .materialize_retained_tumbling_count(retained_count_config(
                cutoff_horizon,
                width,
                TumblingCountLimitsV0::new(2, exact_bytes - 1).unwrap(),
            ))
            .unwrap_err(),
        CountMaterializationErrorV0::IndexByteCapacity {
            maximum: exact_bytes - 1,
            attempted: exact_bytes,
        }
    );
}

#[test]
fn existing_retained_bucket_adds_exactly_one_contribution_coordinate() {
    let mut one = admission();
    accept(
        &mut one,
        &basic_event(1, 40 * SECOND_NS, "schema-v0", Some(b"key")),
    );
    let one_view = one
        .materialize_retained_tumbling_count(maximum_config(
            100 * SECOND_NS as u64,
            50 * SECOND_NS as u64,
        ))
        .unwrap();
    let one_usage = one_view.count_usage();

    let mut two = admission();
    accept(
        &mut two,
        &basic_event(1, 40 * SECOND_NS, "schema-v0", Some(b"key")),
    );
    accept(
        &mut two,
        &basic_event(2, 41 * SECOND_NS, "schema-v0", Some(b"key")),
    );
    let two_view = two
        .materialize_retained_tumbling_count(maximum_config(
            100 * SECOND_NS as u64,
            50 * SECOND_NS as u64,
        ))
        .unwrap();
    assert_eq!(one_usage.represented_buckets(), 1);
    assert_eq!(two_view.count_usage().represented_buckets(), 1);
    assert_eq!(one_usage.contributions(), 1);
    assert_eq!(two_view.count_usage().contributions(), 2);
    assert_eq!(
        two_view.count_usage().index_bytes() - one_usage.index_bytes(),
        8
    );
}

fn permutation_events() -> [EventEnvelopeV0; 4] {
    [
        basic_event(1, 60 * SECOND_NS, "schema-b", Some(b"z")),
        basic_event(2, 70 * SECOND_NS, "schema-a", Some(b"b")),
        basic_event(3, 80 * SECOND_NS, "schema-a", Some(b"a")),
        basic_event(4, 90 * SECOND_NS, "schema-a", Some(b"a")),
    ]
}

fn observation_for_order(order: [usize; 4]) -> ViewEvidence {
    let events = permutation_events();
    let mut state = admission();
    for index in order {
        accept(&mut state, &events[index]);
    }
    let view = state
        .materialize_retained_tumbling_count(maximum_config(
            SECOND_NS as u64,
            50 * SECOND_NS as u64,
        ))
        .unwrap();
    (
        view.retention_cutoff_event_time_ns(),
        view_tuples(&view),
        view.retention_usage(),
        view.count_usage(),
        view.source_watermark_ns(),
        view.is_source_finalized(),
    )
}

#[test]
fn all_permutations_and_fresh_replay_converge_canonically() {
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
    let expected = observation_for_order(permutations[0]);
    for order in permutations {
        assert_eq!(observation_for_order(order), expected);
    }
    assert_eq!(expected.0, Some(59 * SECOND_NS));
    assert_eq!(expected.1.len(), 3);
    assert_eq!(expected.1[0].0, "schema-a");
    assert_eq!(expected.1[0].1, b"a");
    assert_eq!(expected.1[0].9, vec![3, 4]);
    assert_eq!(observation_for_order([0, 1, 2, 3]), expected);
}

#[test]
fn finalization_rebuild_advances_cutoff_and_changes_earliest_bucket_shape() {
    let mut state = admission();
    accept(
        &mut state,
        &basic_event(1, 40 * SECOND_NS, "schema-v0", Some(b"key")),
    );
    accept(
        &mut state,
        &basic_event(2, 90 * SECOND_NS, "schema-v0", Some(b"key")),
    );
    let config = maximum_config(130 * SECOND_NS as u64, 50 * SECOND_NS as u64);
    {
        let before = state.materialize_retained_tumbling_count(config).unwrap();
        assert_eq!(before.retention_cutoff_event_time_ns(), Some(0));
        assert_eq!(before.entries().count(), 2);
        assert!(
            !before
                .get(&schema("schema-v0"), b"key", 1)
                .unwrap()
                .is_cutoff_partial()
        );
        assert!(!before.is_source_finalized());
    }
    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    let after = state.materialize_retained_tumbling_count(config).unwrap();
    assert_eq!(after.retention_cutoff_event_time_ns(), Some(70 * SECOND_NS));
    assert!(after.get(&schema("schema-v0"), b"key", 0).is_none());
    let survivor = after.get(&schema("schema-v0"), b"key", 1).unwrap();
    assert_eq!(survivor.retained_interval_start_ns(), 70 * SECOND_NS);
    assert!(survivor.is_cutoff_partial());
    assert!(survivor.is_finalized());
    assert!(after.is_source_finalized());
}

#[test]
fn contributor_is_pointer_identical_with_provenance_and_sensitivity() {
    let mut state = admission();
    let input = event(
        "source-v0",
        41,
        (100 * SECOND_NS, i64::MAX),
        "schema-v0",
        Some(b"key"),
        7,
        "synthetic-value",
    );
    accept(&mut state, &input);
    let admitted = state.accepted_events().next().unwrap();
    let view = state
        .materialize_retained_tumbling_count(maximum_config(
            100 * SECOND_NS as u64,
            50 * SECOND_NS as u64,
        ))
        .unwrap();
    let (position, contributor) = view
        .contributors(&schema("schema-v0"), b"key", 2)
        .unwrap()
        .next()
        .unwrap();
    assert_eq!(position, SourcePositionV0::Synthetic { sequence: 41 });
    assert!(std::ptr::eq(contributor, admitted));
    assert_eq!(contributor.provenance(), input.provenance());
    assert_eq!(contributor.sensitivity(), input.sensitivity());
    assert_eq!(contributor.ingest_time_ns(), 0);
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

fn reference_count(
    state: &AdmissionStateV0,
    cutoff_ns: i64,
    window_width_ns: i64,
) -> Vec<BucketTuple> {
    let mut buckets: BTreeMap<(String, Vec<u8>, u64), Vec<u64>> = BTreeMap::new();
    for candidate in state
        .accepted_events()
        .filter(|candidate| candidate.event_time_ns() >= cutoff_ns)
    {
        let Some(key) = candidate.key() else {
            continue;
        };
        let ordinal = ((candidate.event_time_ns() - PRIMARY_START_NS) / window_width_ns) as u64;
        buckets
            .entry((
                candidate.schema_id().as_str().to_owned(),
                key.to_vec(),
                ordinal,
            ))
            .or_default()
            .push(candidate.source_position().synthetic_sequence());
    }
    buckets
        .into_iter()
        .map(|((schema_id, key, ordinal), contributors)| {
            let start = PRIMARY_START_NS + ordinal as i64 * window_width_ns;
            let end = (start + window_width_ns).min(PRIMARY_END_NS);
            let retained_start = start.max(cutoff_ns);
            let partial = start < cutoff_ns && cutoff_ns < end;
            (
                schema_id,
                key,
                ordinal,
                start,
                end,
                retained_start,
                contributors.len() as u64,
                partial,
                true,
                contributors,
            )
        })
        .collect()
}

#[test]
fn primary_full_horizon_matches_task_007_and_short_horizon_batch_reference() {
    let state = primary_state();
    let window_width = 5 * 60 * SECOND_NS;
    let epoch_horizon = (PRIMARY_END_NS - PRIMARY_START_NS) as u64;
    let retained = state
        .materialize_retained_tumbling_count(maximum_config(epoch_horizon, window_width as u64))
        .unwrap();
    let full = state
        .materialize_tumbling_count(count_config(
            window_width as u64,
            TumblingCountLimitsV0::maximum(),
        ))
        .unwrap();
    let full_tuples = full
        .entries()
        .map(|(schema_id, key, observation)| {
            (
                schema_id.as_str().to_owned(),
                key.to_vec(),
                observation.window_ordinal(),
                observation.window_start_ns(),
                observation.window_end_ns(),
                observation.window_start_ns(),
                observation.count(),
                false,
                observation.is_finalized(),
                full.contributors(schema_id, key, observation.window_ordinal())
                    .unwrap()
                    .map(|(position, _)| position.synthetic_sequence())
                    .collect(),
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
    assert_eq!(retained.count_usage().accepted_events_considered(), 98_750);
    assert_eq!(retained.count_usage().represented_buckets(), 49);
    assert_eq!(retained.count_usage().contributions(), 98_750);
    assert_eq!(retained.count_usage().unkeyed_events_omitted(), 0);
    assert!(retained.entries().all(|(_, _, observation)| {
        observation.is_finalized() && !observation.is_cutoff_partial()
    }));
    drop(full);
    drop(retained);

    let short_horizon = 9 * 60 * SECOND_NS as u64 + 30 * SECOND_NS as u64;
    let cutoff = PRIMARY_END_NS - short_horizon as i64;
    let reference = reference_count(&state, cutoff, window_width);
    let short = state
        .materialize_retained_tumbling_count(maximum_config(short_horizon, window_width as u64))
        .unwrap();
    assert_eq!(short.retention_cutoff_event_time_ns(), Some(cutoff));
    assert_eq!(view_tuples(&short), reference);
    assert!(short.retention_usage().expired_events_excluded() > 0);
    let bisected: Vec<_> = short
        .entries()
        .filter(|(_, _, observation)| {
            observation.window_start_ns() < cutoff && cutoff < observation.window_end_ns()
        })
        .collect();
    assert!(!bisected.is_empty());
    assert!(bisected.iter().all(|(_, _, observation)| {
        observation.is_cutoff_partial() && observation.retained_interval_start_ns() == cutoff
    }));
    assert_usage_conserved(&short);
}

fn diagnostic(
    source_id: &str,
    schema_id: &str,
    key: &[u8],
    sequence: u64,
    event_time_ns: i64,
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
            sequence,
            (event_time_ns, event_time_ns + 7),
            schema_id,
            Some(key),
            u64::MAX,
            "secret-value",
        ),
    );
    let config = maximum_config(100 * SECOND_NS as u64, 50 * SECOND_NS as u64);
    let view = state.materialize_retained_tumbling_count(config).unwrap();
    let observation = view.entries().next().unwrap().2;
    format!(
        "{config:?}|{:?}|{:?}|{observation:?}|{view:?}|{:?}|{:?}",
        view.retention_usage(),
        view.count_usage(),
        RetainedTumblingCountConfigErrorV0::UnsupportedVersion { actual: 1 },
        CountMaterializationErrorV0::IndexInvariant,
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
            (90 * SECOND_NS, 42),
            schema_id,
            Some(keys.1),
            2,
            "secondv0",
        ),
    );
    let error = state
        .materialize_retained_tumbling_count(retained_count_config(
            100 * SECOND_NS as u64,
            50 * SECOND_NS as u64,
            TumblingCountLimitsV0::new(1, MAX_TUMBLING_COUNT_INDEX_BYTES_V0).unwrap(),
        ))
        .unwrap_err();
    format!("{error:?}|{error}")
}

#[test]
fn diagnostics_are_content_independent_and_value_safe() {
    let first = diagnostic("source-aa", "schema-aa", b"secret-aa", 111, 40 * SECOND_NS);
    let second = diagnostic("source-bb", "schema-bb", b"secret-bb", 999, 41 * SECOND_NS);
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
        "secret-value",
        "first-v0",
        "secondv0",
        "decant://",
        "111",
        "999",
        "40000000000",
        "41000000000",
        "window_ordinal: 0",
    ] {
        assert!(!first.contains(forbidden));
        assert!(!second.contains(forbidden));
        assert!(!first_error.contains(forbidden));
        assert!(!second_error.contains(forbidden));
    }
}
