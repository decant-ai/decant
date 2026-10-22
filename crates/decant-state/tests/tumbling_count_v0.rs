use std::collections::{BTreeMap, BTreeSet};

use decant_fixtures::{
    FixtureRecordV0, GENERATOR_VERSION_V0, SreGeneratorV0, decode_scenario_manifest_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    CountMaterializationErrorV0, FinalizeOutcomeV0, MAX_TUMBLING_COUNT_BUCKETS_V0,
    MAX_TUMBLING_COUNT_INDEX_BYTES_V0, RejectionReasonV0, TUMBLING_COUNT_VERSION_V0,
    TumblingCountConfigErrorV0, TumblingCountConfigV0, TumblingCountLimitFieldV0,
    TumblingCountLimitsV0, TumblingCountViewV0, tumbling_count_bucket_accounted_bytes_v0,
    tumbling_count_contribution_accounted_bytes_v0,
};

const SECOND_NS: i64 = 1_000_000_000;
const PRIMARY_START_NS: i64 = 1_767_225_600_000_000_000;
const PRIMARY_END_NS: i64 = 1_767_226_800_000_000_000;
const PRIMARY_SCENARIO: &[u8] =
    include_bytes!("../../decant-fixtures/contracts/sre-v0/scenario.json");

type ViewTuple = (String, Vec<u8>, u64, i64, i64, u64, bool, Vec<u64>);

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn schema(value: &str) -> SchemaIdV0 {
    SchemaIdV0::new(value).unwrap()
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
        ScalarValueV0::from_u64(sequence),
    )
}

fn admission_for(epoch_start_ns: i64, epoch_end_ns: i64) -> AdmissionStateV0 {
    AdmissionStateV0::new(
        AdmissionConfigV0::new(
            ADMISSION_VERSION_V0,
            source("source-v0"),
            epoch_start_ns,
            epoch_end_ns,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    )
}

fn admission() -> AdmissionStateV0 {
    admission_for(0, 200 * SECOND_NS)
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

fn maximum_count_config(window_width_ns: u64) -> TumblingCountConfigV0 {
    count_config(window_width_ns, TumblingCountLimitsV0::maximum())
}

fn accept(state: &mut AdmissionStateV0, event: &EventEnvelopeV0) {
    assert_eq!(state.apply_event(event), Ok(ApplyOutcomeV0::Accepted));
}

fn view_tuples(view: &TumblingCountViewV0<'_>) -> Vec<ViewTuple> {
    view.entries()
        .map(|(schema_id, key, observation)| {
            let sequences = view
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
                observation.count(),
                observation.is_finalized(),
                sequences,
            )
        })
        .collect()
}

#[test]
fn config_enforces_width_version_and_exact_bounded_limits() {
    let minimum = TumblingCountLimitsV0::new(1, 1).unwrap();
    let maximum = TumblingCountLimitsV0::new(
        MAX_TUMBLING_COUNT_BUCKETS_V0,
        MAX_TUMBLING_COUNT_INDEX_BYTES_V0,
    )
    .unwrap();
    assert_eq!(minimum.bucket_limit(), 1);
    assert_eq!(minimum.index_byte_limit(), 1);
    assert_eq!(maximum, TumblingCountLimitsV0::maximum());
    assert!(TumblingCountConfigV0::new(0, 1, minimum).is_ok());
    assert!(TumblingCountConfigV0::new(0, u64::MAX, maximum).is_ok());
    assert_eq!(
        TumblingCountConfigV0::new(1, 1, minimum),
        Err(TumblingCountConfigErrorV0::UnsupportedVersion { actual: 1 })
    );
    assert_eq!(
        TumblingCountConfigV0::new(0, 0, minimum),
        Err(TumblingCountConfigErrorV0::ZeroWindowWidth)
    );
    assert_eq!(
        TumblingCountLimitsV0::new(0, 1),
        Err(TumblingCountConfigErrorV0::ZeroLimit {
            field: TumblingCountLimitFieldV0::Buckets,
        })
    );
    assert_eq!(
        TumblingCountLimitsV0::new(1, 0),
        Err(TumblingCountConfigErrorV0::ZeroLimit {
            field: TumblingCountLimitFieldV0::IndexBytes,
        })
    );
    assert_eq!(
        TumblingCountLimitsV0::new(MAX_TUMBLING_COUNT_BUCKETS_V0 + 1, 1),
        Err(TumblingCountConfigErrorV0::AboveMaximum {
            field: TumblingCountLimitFieldV0::Buckets,
            maximum: MAX_TUMBLING_COUNT_BUCKETS_V0,
            actual: MAX_TUMBLING_COUNT_BUCKETS_V0 + 1,
        })
    );
    assert_eq!(
        TumblingCountLimitsV0::new(1, MAX_TUMBLING_COUNT_INDEX_BYTES_V0 + 1),
        Err(TumblingCountConfigErrorV0::AboveMaximum {
            field: TumblingCountLimitFieldV0::IndexBytes,
            maximum: MAX_TUMBLING_COUNT_INDEX_BYTES_V0,
            actual: MAX_TUMBLING_COUNT_INDEX_BYTES_V0 + 1,
        })
    );
}

#[test]
fn index_accounting_matches_an_independent_formula_at_boundaries() {
    let ordinary_schema = schema("schema-v0");
    let maximum_schema = schema(&"s".repeat(64));
    for (schema_id, key) in [
        (&ordinary_schema, &[][..]),
        (&ordinary_schema, &b"ordinary"[..]),
        (&maximum_schema, &vec![b'k'; 256][..]),
    ] {
        assert_eq!(
            tumbling_count_bucket_accounted_bytes_v0(schema_id, key).unwrap(),
            4 + schema_id.as_str().len() + 4 + key.len() + 8 + 4
        );
    }
    assert_eq!(tumbling_count_contribution_accounted_bytes_v0(), 8);
}

#[test]
fn empty_unkeyed_and_empty_key_behavior_is_explicit() {
    let mut state = admission();
    {
        let view = state
            .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
            .unwrap();
        assert_eq!(view.usage().represented_buckets(), 0);
        assert_eq!(view.usage().contributions(), 0);
        assert_eq!(view.usage().index_bytes(), 0);
        assert_eq!(view.usage().accepted_events_considered(), 0);
        assert_eq!(view.usage().unkeyed_events_omitted(), 0);
        assert_eq!(view.source_watermark_ns(), None);
        assert!(!view.is_source_finalized());
        assert!(view.get(&schema("schema-v0"), b"", 0).is_none());
    }

    accept(
        &mut state,
        &basic_event(1, 10 * SECOND_NS, "schema-v0", None),
    );
    accept(
        &mut state,
        &basic_event(2, 20 * SECOND_NS, "schema-v0", Some(b"")),
    );
    let view = state
        .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(view.usage().accepted_events_considered(), 2);
    assert_eq!(view.usage().unkeyed_events_omitted(), 1);
    assert_eq!(view.usage().represented_buckets(), 1);
    assert_eq!(view.usage().contributions(), 1);
    assert_eq!(view.get(&schema("schema-v0"), b"", 0).unwrap().count(), 1);
}

#[test]
fn half_open_boundaries_and_final_epoch_clamp_are_exact() {
    let mut state = admission_for(0, 125 * SECOND_NS);
    for candidate in [
        basic_event(1, 50 * SECOND_NS - 1, "schema-v0", Some(b"key")),
        basic_event(2, 50 * SECOND_NS, "schema-v0", Some(b"key")),
        basic_event(3, 50 * SECOND_NS + 1, "schema-v0", Some(b"key")),
        basic_event(4, 125 * SECOND_NS - 1, "schema-v0", Some(b"key")),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
        .unwrap();
    let expected = [(0, 0, 50, 1), (1, 50, 100, 2), (2, 100, 125, 1)];
    for (ordinal, start_seconds, end_seconds, count) in expected {
        let observed = view.get(&schema("schema-v0"), b"key", ordinal).unwrap();
        assert_eq!(observed.window_start_ns(), start_seconds * SECOND_NS);
        assert_eq!(observed.window_end_ns(), end_seconds * SECOND_NS);
        assert_eq!(observed.count(), count);
    }
    assert!(view.get(&schema("schema-v0"), b"key", 3).is_none());
}

#[test]
fn signed_timestamp_extremes_use_checked_wide_arithmetic() {
    let mut state = admission_for(i64::MIN, i64::MAX);
    accept(
        &mut state,
        &basic_event(1, i64::MIN, "schema-v0", Some(b"key")),
    );
    accept(
        &mut state,
        &basic_event(2, i64::MAX - 1, "schema-v0", Some(b"key")),
    );
    let view = state
        .materialize_tumbling_count(maximum_count_config(u64::MAX))
        .unwrap();
    let observed = view.get(&schema("schema-v0"), b"key", 0).unwrap();
    assert_eq!(observed.window_start_ns(), i64::MIN);
    assert_eq!(observed.window_end_ns(), i64::MAX);
    assert_eq!(observed.count(), 2);
    assert!(view.get(&schema("schema-v0"), b"key", 1).is_none());
}

#[test]
fn schema_key_and_window_namespaces_are_distinct() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 10 * SECOND_NS, "schema-a", Some(b"shared")),
        basic_event(2, 60 * SECOND_NS, "schema-a", Some(b"shared")),
        basic_event(3, 60 * SECOND_NS, "schema-b", Some(b"shared")),
        basic_event(4, 60 * SECOND_NS, "schema-a", Some(b"other")),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(view.usage().represented_buckets(), 4);
    for (schema_id, key, ordinal) in [
        ("schema-a", &b"shared"[..], 0),
        ("schema-a", &b"shared"[..], 1),
        ("schema-b", &b"shared"[..], 1),
        ("schema-a", &b"other"[..], 1),
    ] {
        assert_eq!(
            view.get(&schema(schema_id), key, ordinal).unwrap().count(),
            1
        );
    }
}

#[test]
fn duplicates_and_all_semantic_rejections_cannot_increment_counts() {
    let mut state = admission();
    let original = event(
        "source-v0",
        1,
        (50 * SECOND_NS, 1),
        "schema-v0",
        Some(b"target"),
        0,
        ScalarValueV0::utf8("original").unwrap(),
    );
    accept(&mut state, &original);
    accept(
        &mut state,
        &basic_event(2, 100 * SECOND_NS, "schema-v0", Some(b"advance")),
    );

    let duplicate = event(
        "source-v0",
        1,
        (50 * SECOND_NS, i64::MAX),
        "schema-v0",
        Some(b"target"),
        0,
        ScalarValueV0::utf8("original").unwrap(),
    );
    assert_eq!(
        state.apply_event(&duplicate),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    let conflict = event(
        "source-v0",
        1,
        (50 * SECOND_NS, 1),
        "schema-v0",
        Some(b"target"),
        0,
        ScalarValueV0::utf8("conflict").unwrap(),
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
            70 * SECOND_NS - 1,
            "schema-v0",
            Some(b"target"),
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::Late))
    );
    assert_eq!(
        state.apply_event(&event(
            "source-x0",
            4,
            (120 * SECOND_NS, 120 * SECOND_NS),
            "schema-v0",
            Some(b"target"),
            0,
            ScalarValueV0::from_u64(4),
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
            Some(b"target"),
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch))
    );

    let view = state
        .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(view.usage().accepted_events_considered(), 2);
    assert_eq!(view.usage().contributions(), 2);
    assert_eq!(
        view.get(&schema("schema-v0"), b"target", 1)
            .unwrap()
            .count(),
        1
    );
}

#[test]
fn bucket_and_byte_capacity_are_exact_and_admission_is_immutable() {
    let mut state = admission();
    let first = basic_event(1, 10 * SECOND_NS, "schema-a", Some(b"a"));
    let second = basic_event(2, 60 * SECOND_NS, "schema-b", Some(b"bb"));
    accept(&mut state, &first);
    accept(&mut state, &second);
    let accepted_before: Vec<_> = state.accepted_events().cloned().collect();
    let usage_before = state.usage();
    let watermark_before = state.watermark_ns();

    let contribution_bytes = tumbling_count_contribution_accounted_bytes_v0();
    let first_bytes = tumbling_count_bucket_accounted_bytes_v0(first.schema_id(), b"a").unwrap()
        + contribution_bytes;
    let second_bytes = tumbling_count_bucket_accounted_bytes_v0(second.schema_id(), b"bb").unwrap()
        + contribution_bytes;
    let exact_bytes = first_bytes + second_bytes;
    let exact = count_config(
        50 * SECOND_NS as u64,
        TumblingCountLimitsV0::new(2, exact_bytes).unwrap(),
    );
    let view = state.materialize_tumbling_count(exact).unwrap();
    assert_eq!(view.usage().represented_buckets(), 2);
    assert_eq!(view.usage().index_bytes(), exact_bytes);
    drop(view);

    let bucket_error = state
        .materialize_tumbling_count(count_config(
            50 * SECOND_NS as u64,
            TumblingCountLimitsV0::new(1, MAX_TUMBLING_COUNT_INDEX_BYTES_V0).unwrap(),
        ))
        .unwrap_err();
    assert_eq!(
        bucket_error,
        CountMaterializationErrorV0::BucketCapacity {
            maximum: 1,
            attempted: 2,
        }
    );
    let byte_error = state
        .materialize_tumbling_count(count_config(
            50 * SECOND_NS as u64,
            TumblingCountLimitsV0::new(2, exact_bytes - 1).unwrap(),
        ))
        .unwrap_err();
    assert_eq!(
        byte_error,
        CountMaterializationErrorV0::IndexByteCapacity {
            maximum: exact_bytes - 1,
            attempted: exact_bytes,
        }
    );
    assert_eq!(
        state.accepted_events().cloned().collect::<Vec<_>>(),
        accepted_before
    );
    assert_eq!(state.usage(), usage_before);
    assert_eq!(state.watermark_ns(), watermark_before);
}

#[test]
fn existing_bucket_adds_only_one_contribution_coordinate() {
    let mut one = admission();
    accept(
        &mut one,
        &basic_event(1, 10 * SECOND_NS, "schema-v0", Some(b"key")),
    );
    let one_view = one
        .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
        .unwrap();
    let one_usage = one_view.usage();

    let mut two = admission();
    accept(
        &mut two,
        &basic_event(1, 10 * SECOND_NS, "schema-v0", Some(b"key")),
    );
    accept(
        &mut two,
        &basic_event(2, 20 * SECOND_NS, "schema-v0", Some(b"key")),
    );
    let two_view = two
        .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(one_usage.represented_buckets(), 1);
    assert_eq!(two_view.usage().represented_buckets(), 1);
    assert_eq!(one_usage.contributions(), 1);
    assert_eq!(two_view.usage().contributions(), 2);
    assert_eq!(two_view.usage().index_bytes() - one_usage.index_bytes(), 8);
    assert_eq!(
        two_view
            .get(&schema("schema-v0"), b"key", 0)
            .unwrap()
            .count(),
        2
    );
}

#[test]
fn iteration_is_canonical_by_schema_key_and_ordinal() {
    let inputs = [
        basic_event(1, 60 * SECOND_NS, "schema-b", Some(b"z")),
        basic_event(2, 60 * SECOND_NS, "schema-a", Some(b"b")),
        basic_event(3, 30 * SECOND_NS, "schema-a", Some(b"a")),
        basic_event(4, 60 * SECOND_NS, "schema-a", Some(b"a")),
    ];
    let expected = vec![
        ("schema-a".to_owned(), b"a".to_vec(), 0),
        ("schema-a".to_owned(), b"a".to_vec(), 1),
        ("schema-a".to_owned(), b"b".to_vec(), 1),
        ("schema-b".to_owned(), b"z".to_vec(), 1),
    ];
    for order in [[0, 1, 2, 3], [3, 2, 1, 0]] {
        let mut state = admission();
        for index in order {
            accept(&mut state, &inputs[index]);
        }
        let view = state
            .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
            .unwrap();
        let actual: Vec<_> = view
            .entries()
            .map(|(schema_id, key, observation)| {
                (
                    schema_id.as_str().to_owned(),
                    key.to_vec(),
                    observation.window_ordinal(),
                )
            })
            .collect();
        assert_eq!(actual, expected);
    }
}

fn apply_order(order: &[usize], finalize: bool) -> AdmissionStateV0 {
    let events = [
        basic_event(1, 60 * SECOND_NS, "schema-v0", Some(b"key")),
        basic_event(2, 90 * SECOND_NS, "schema-v0", Some(b"key")),
        basic_event(3, 70 * SECOND_NS, "schema-v0", Some(b"key")),
        basic_event(4, 90 * SECOND_NS, "schema-v0", Some(b"key")),
    ];
    let mut state = admission();
    for index in order {
        accept(&mut state, &events[*index]);
    }
    if finalize {
        assert_eq!(
            state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
            FinalizeOutcomeV0::Finalized
        );
    }
    state
}

#[test]
fn every_bounded_disorder_permutation_and_fresh_replay_converges() {
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
    let expected_state = apply_order(&orders[0], false);
    let expected_view = expected_state
        .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
        .unwrap();
    let expected_tuples = view_tuples(&expected_view);
    let expected_usage = expected_view.usage();

    for order in orders {
        let first = apply_order(&order, false);
        let replayed = apply_order(&order, false);
        let first_view = first
            .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
            .unwrap();
        let replayed_view = replayed
            .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
            .unwrap();
        assert_eq!(view_tuples(&first_view), expected_tuples);
        assert_eq!(view_tuples(&first_view), view_tuples(&replayed_view));
        assert_eq!(first_view.usage(), expected_usage);
        assert_eq!(first_view.usage(), replayed_view.usage());
        assert_eq!(first_view.source_watermark_ns(), Some(60 * SECOND_NS));
        assert!(!first_view.is_source_finalized());
        assert_eq!(
            first_view
                .contributors(&schema("schema-v0"), b"key", 1)
                .unwrap()
                .map(|(position, _)| position.synthetic_sequence())
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
    }

    let finalized_first = apply_order(&[3, 1, 0, 2], true);
    let finalized_replay = apply_order(&[3, 1, 0, 2], true);
    let finalized_first_view = finalized_first
        .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
        .unwrap();
    let finalized_replay_view = finalized_replay
        .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(
        view_tuples(&finalized_first_view),
        view_tuples(&finalized_replay_view)
    );
    assert_eq!(finalized_first_view.usage(), finalized_replay_view.usage());
    assert!(finalized_first_view.is_source_finalized());
}

#[test]
fn watermark_finalizes_only_windows_at_or_before_it() {
    let mut state = admission();
    accept(
        &mut state,
        &basic_event(1, 40 * SECOND_NS, "schema-v0", Some(b"key")),
    );
    accept(
        &mut state,
        &basic_event(2, 90 * SECOND_NS, "schema-v0", Some(b"key")),
    );
    let prefinal = state
        .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(prefinal.source_watermark_ns(), Some(60 * SECOND_NS));
    assert!(
        prefinal
            .get(&schema("schema-v0"), b"key", 0)
            .unwrap()
            .is_finalized()
    );
    assert!(
        !prefinal
            .get(&schema("schema-v0"), b"key", 1)
            .unwrap()
            .is_finalized()
    );
    let expected_counts: Vec<_> = prefinal
        .entries()
        .map(|(_, _, observation)| observation.count())
        .collect();
    drop(prefinal);

    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    let finalized = state
        .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
        .unwrap();
    assert!(finalized.is_source_finalized());
    assert_eq!(finalized.source_watermark_ns(), Some(200 * SECOND_NS));
    assert!(
        finalized
            .entries()
            .all(|(_, _, observed)| observed.is_finalized())
    );
    assert_eq!(
        finalized
            .entries()
            .map(|(_, _, observation)| observation.count())
            .collect::<Vec<_>>(),
        expected_counts
    );
}

#[test]
fn contributors_are_exact_normalized_admission_events_with_provenance() {
    let mut state = admission();
    let input = event(
        "source-v0",
        41,
        (100 * SECOND_NS, i64::MAX),
        "schema-v0",
        Some(b"key"),
        7,
        ScalarValueV0::utf8("synthetic-value").unwrap(),
    );
    accept(&mut state, &input);
    let retained = state.accepted_events().next().unwrap();
    let view = state
        .materialize_tumbling_count(maximum_count_config(50 * SECOND_NS as u64))
        .unwrap();
    let (position, contributor) = view
        .contributors(&schema("schema-v0"), b"key", 2)
        .unwrap()
        .next()
        .unwrap();
    assert_eq!(position, SourcePositionV0::Synthetic { sequence: 41 });
    assert!(std::ptr::eq(contributor, retained));
    assert_eq!(contributor.ingest_time_ns(), 0);
    assert_eq!(
        contributor.provenance().as_str(),
        "decant://event/v0/source-v0/synthetic/41"
    );
}

#[test]
fn primary_sre_count_windows_match_frozen_workload_facts() {
    let manifest = decode_scenario_manifest_v0(PRIMARY_SCENARIO).unwrap();
    let generator = SreGeneratorV0::new(GENERATOR_VERSION_V0, &manifest, "sre-v0-primary").unwrap();
    let mut state = primary_admission();
    for record in generator {
        match record.unwrap() {
            FixtureRecordV0::Event(event) => {
                let _ = state.apply_event(&event).unwrap();
            }
            FixtureRecordV0::EndOfInput(control) => assert_eq!(
                state.finalize_source(control.source_id(), control.finalized_through_ns()),
                FinalizeOutcomeV0::Finalized
            ),
        }
    }
    let accepted_positions: BTreeSet<_> = state
        .accepted_events()
        .map(EventEnvelopeV0::source_position)
        .collect();
    let view = state
        .materialize_tumbling_count(maximum_count_config(300 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(view.usage().represented_buckets(), 49);
    assert_eq!(view.usage().contributions(), 98_750);
    assert_eq!(view.usage().accepted_events_considered(), 98_750);
    assert_eq!(view.usage().unkeyed_events_omitted(), 0);
    assert!(view.usage().represented_buckets() <= MAX_TUMBLING_COUNT_BUCKETS_V0);
    assert!(view.usage().index_bytes() <= MAX_TUMBLING_COUNT_INDEX_BYTES_V0);
    assert!(view.is_source_finalized());
    assert_eq!(view.source_watermark_ns(), Some(PRIMARY_END_NS));
    assert!(
        view.entries()
            .all(|(_, _, observed)| observed.is_finalized())
    );

    for ordinal in 0..4 {
        for region in [b"payments-v2/region-a", b"payments-v2/region-b"] {
            assert_eq!(
                view.get(&schema("payment-outcome-v0"), region, ordinal)
                    .unwrap()
                    .count(),
                7_500
            );
            assert_eq!(
                view.get(&schema("metric-sample-v0"), region, ordinal)
                    .unwrap()
                    .count(),
                60
            );
        }
    }

    let total_counts: u64 = view
        .entries()
        .map(|(_, _, observation)| observation.count())
        .sum();
    let mut contributor_sequences = BTreeSet::new();
    let mut contributor_count = 0;
    for (schema_id, key, observation) in view.entries() {
        for (position, contributor) in view
            .contributors(schema_id, key, observation.window_ordinal())
            .unwrap()
        {
            assert_eq!(contributor.source_position(), position);
            assert_eq!(contributor.ingest_time_ns(), 0);
            assert!(
                contributor
                    .provenance()
                    .as_str()
                    .starts_with("decant://event/v0/")
            );
            assert!(contributor_sequences.insert(position));
            contributor_count += 1;
        }
    }
    assert_eq!(total_counts, 98_750);
    assert_eq!(contributor_count, 98_750);
    assert_eq!(contributor_sequences, accepted_positions);
}

#[test]
fn diagnostics_are_content_independent_for_equal_length_inputs() {
    fn successful_diagnostic(source_id: &str, schema_id: &str, secret: &str) -> String {
        let mut state = AdmissionStateV0::new(
            AdmissionConfigV0::new(
                0,
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
                9_876_543_210,
                (123_456_789_012, 123_456_789_013),
                schema_id,
                Some(secret.as_bytes()),
                u64::MAX,
                ScalarValueV0::utf8(secret).unwrap(),
            ),
        );
        let config = maximum_count_config(50 * SECOND_NS as u64);
        let view = state.materialize_tumbling_count(config).unwrap();
        let observation = view.get(&schema(schema_id), secret.as_bytes(), 2).unwrap();
        format!("{config:?}|{:?}|{observation:?}|{view:?}", view.usage())
    }

    fn error_diagnostic(source_id: &str, schema_id: &str, secret: &str) -> String {
        let mut state = AdmissionStateV0::new(
            AdmissionConfigV0::new(
                0,
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
                (10 * SECOND_NS, 10 * SECOND_NS),
                schema_id,
                Some(secret.as_bytes()),
                u64::MAX,
                ScalarValueV0::utf8(secret).unwrap(),
            ),
        );
        accept(
            &mut state,
            &event(
                source_id,
                2,
                (60 * SECOND_NS, 60 * SECOND_NS),
                "other-v00",
                Some(secret.as_bytes()),
                u64::MAX,
                ScalarValueV0::utf8(secret).unwrap(),
            ),
        );
        let error = state
            .materialize_tumbling_count(count_config(
                50 * SECOND_NS as u64,
                TumblingCountLimitsV0::new(1, MAX_TUMBLING_COUNT_INDEX_BYTES_V0).unwrap(),
            ))
            .unwrap_err();
        format!("{error:?}|{error}")
    }

    let left = successful_diagnostic("source-aa", "secret-aa", "payload-a");
    let right = successful_diagnostic("source-bb", "secret-bb", "payload-b");
    assert_eq!(left, right);
    let left_error = error_diagnostic("source-aa", "secret-aa", "payload-a");
    let right_error = error_diagnostic("source-bb", "secret-bb", "payload-b");
    assert_eq!(left_error, right_error);
    for diagnostic in [&left, &right, &left_error, &right_error] {
        for forbidden in [
            "source-aa",
            "source-bb",
            "secret-aa",
            "secret-bb",
            "payload-a",
            "payload-b",
            "payload",
            "9_876_543_210",
            "9876543210",
            "123456789012",
            "123456789013",
            "100000000000",
            "150000000000",
            "window_ordinal: 2",
            "7365637265742d6161",
            "7365637265742d6262",
            "c2VjcmV0LWFh",
            "c2VjcmV0LWJi",
            "decant://event/v0/",
            "115, 101, 99, 114, 101, 116",
            "18446744073709551615",
        ] {
            assert!(
                !diagnostic.contains(forbidden),
                "diagnostic disclosed {forbidden}"
            );
        }
    }
}
