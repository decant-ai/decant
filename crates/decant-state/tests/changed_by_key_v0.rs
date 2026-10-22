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
    CHANGED_BY_KEY_VERSION_V0, ChangedByKeyConfigErrorV0, ChangedByKeyConfigV0,
    ChangedByKeyLimitFieldV0, ChangedByKeyLimitsV0, ChangedByKeyViewV0,
    ChangedMaterializationErrorV0, FinalizeOutcomeV0, MAX_CHANGED_INDEX_BYTES_V0,
    MAX_CHANGED_INDEX_ENTRIES_V0, RejectionReasonV0, changed_index_entry_accounted_bytes_v0,
};

const SECOND_NS: i64 = 1_000_000_000;
const PRIMARY_START_NS: i64 = 1_767_225_600_000_000_000;
const PRIMARY_END_NS: i64 = 1_767_226_800_000_000_000;
const PRIMARY_SCENARIO: &[u8] =
    include_bytes!("../../decant-fixtures/contracts/sre-v0/scenario.json");

type ChangeTuple = (String, Vec<u8>, u64, u64);

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
    value: &str,
) -> EventEnvelopeV0 {
    event(
        "source-v0",
        sequence,
        (event_time_ns, event_time_ns),
        schema_id,
        key,
        0,
        ScalarValueV0::utf8(value).unwrap(),
    )
}

fn admission() -> AdmissionStateV0 {
    AdmissionStateV0::new(
        AdmissionConfigV0::new(
            ADMISSION_VERSION_V0,
            source("source-v0"),
            0,
            200 * SECOND_NS,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    )
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

fn maximum_changed_config() -> ChangedByKeyConfigV0 {
    changed_config(ChangedByKeyLimitsV0::maximum())
}

fn accept(state: &mut AdmissionStateV0, event: &EventEnvelopeV0) {
    assert_eq!(state.apply_event(event), Ok(ApplyOutcomeV0::Accepted));
}

fn sequence(event: &EventEnvelopeV0) -> u64 {
    event.source_position().synthetic_sequence()
}

fn view_tuples(view: &ChangedByKeyViewV0<'_>) -> Vec<ChangeTuple> {
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

#[test]
fn config_enforces_version_and_exact_bounded_limits() {
    let minimum = ChangedByKeyLimitsV0::new(1, 1).unwrap();
    let maximum =
        ChangedByKeyLimitsV0::new(MAX_CHANGED_INDEX_ENTRIES_V0, MAX_CHANGED_INDEX_BYTES_V0)
            .unwrap();
    assert_eq!(minimum.index_entry_limit(), 1);
    assert_eq!(minimum.index_byte_limit(), 1);
    assert_eq!(maximum, ChangedByKeyLimitsV0::maximum());
    assert!(ChangedByKeyConfigV0::new(0, minimum).is_ok());
    assert!(ChangedByKeyConfigV0::new(0, maximum).is_ok());
    assert_eq!(
        ChangedByKeyConfigV0::new(1, minimum),
        Err(ChangedByKeyConfigErrorV0::UnsupportedVersion { actual: 1 })
    );
    assert_eq!(
        ChangedByKeyLimitsV0::new(0, 1),
        Err(ChangedByKeyConfigErrorV0::ZeroLimit {
            field: ChangedByKeyLimitFieldV0::IndexEntries,
        })
    );
    assert_eq!(
        ChangedByKeyLimitsV0::new(1, 0),
        Err(ChangedByKeyConfigErrorV0::ZeroLimit {
            field: ChangedByKeyLimitFieldV0::IndexBytes,
        })
    );
    assert_eq!(
        ChangedByKeyLimitsV0::new(MAX_CHANGED_INDEX_ENTRIES_V0 + 1, 1),
        Err(ChangedByKeyConfigErrorV0::AboveMaximum {
            field: ChangedByKeyLimitFieldV0::IndexEntries,
            maximum: MAX_CHANGED_INDEX_ENTRIES_V0,
            actual: MAX_CHANGED_INDEX_ENTRIES_V0 + 1,
        })
    );
    assert_eq!(
        ChangedByKeyLimitsV0::new(1, MAX_CHANGED_INDEX_BYTES_V0 + 1),
        Err(ChangedByKeyConfigErrorV0::AboveMaximum {
            field: ChangedByKeyLimitFieldV0::IndexBytes,
            maximum: MAX_CHANGED_INDEX_BYTES_V0,
            actual: MAX_CHANGED_INDEX_BYTES_V0 + 1,
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
        let base = 4 + schema_id.as_str().len() + 4 + key.len() + 1 + 8;
        assert_eq!(
            changed_index_entry_accounted_bytes_v0(schema_id, key, false).unwrap(),
            base
        );
        assert_eq!(
            changed_index_entry_accounted_bytes_v0(schema_id, key, true).unwrap(),
            base + 8
        );
    }
}

#[test]
fn empty_unkeyed_and_empty_key_behavior_is_explicit() {
    let mut state = admission();
    {
        let view = state
            .materialize_changed_by_key(maximum_changed_config())
            .unwrap();
        assert_eq!(view.usage().tracked_entries(), 0);
        assert_eq!(view.usage().changed_entries(), 0);
        assert_eq!(view.usage().index_bytes(), 0);
        assert_eq!(view.usage().accepted_events_considered(), 0);
        assert_eq!(view.usage().unkeyed_events_omitted(), 0);
        assert_eq!(view.source_watermark_ns(), None);
        assert!(!view.is_source_finalized());
        assert!(!view.is_tracked(&schema("schema-v0"), b""));
        assert!(view.get(&schema("schema-v0"), b"").is_none());
    }

    accept(
        &mut state,
        &basic_event(1, 10 * SECOND_NS, "schema-v0", None, "unkeyed"),
    );
    accept(
        &mut state,
        &basic_event(2, 20 * SECOND_NS, "schema-v0", Some(b""), "before"),
    );
    accept(
        &mut state,
        &basic_event(3, 30 * SECOND_NS, "schema-v0", Some(b""), "after"),
    );
    let view = state
        .materialize_changed_by_key(maximum_changed_config())
        .unwrap();
    assert_eq!(view.usage().accepted_events_considered(), 3);
    assert_eq!(view.usage().unkeyed_events_omitted(), 1);
    assert_eq!(view.usage().tracked_entries(), 1);
    assert_eq!(view.usage().changed_entries(), 1);
    assert!(view.is_tracked(&schema("schema-v0"), b""));
    let observed = view.get(&schema("schema-v0"), b"").unwrap();
    assert_eq!(sequence(observed.previous()), 2);
    assert_eq!(sequence(observed.current()), 3);
}

#[test]
fn schema_namespaces_are_distinct() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 10 * SECOND_NS, "schema-a", Some(b"shared"), "a0"),
        basic_event(2, 20 * SECOND_NS, "schema-a", Some(b"shared"), "a1"),
        basic_event(3, 30 * SECOND_NS, "schema-b", Some(b"shared"), "b0"),
        basic_event(4, 40 * SECOND_NS, "schema-b", Some(b"shared"), "b1"),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_changed_by_key(maximum_changed_config())
        .unwrap();
    assert_eq!(view.usage().tracked_entries(), 2);
    assert_eq!(view.usage().changed_entries(), 2);
    assert_eq!(
        sequence(view.get(&schema("schema-a"), b"shared").unwrap().current()),
        2
    );
    assert_eq!(
        sequence(view.get(&schema("schema-b"), b"shared").unwrap().current()),
        4
    );
}

#[test]
fn metadata_only_differences_are_tracked_but_unchanged() {
    let mut state = admission();
    let first = event(
        "source-v0",
        1,
        (10 * SECOND_NS, 111),
        "schema-v0",
        Some(b"key"),
        1,
        ScalarValueV0::utf8("same-value").unwrap(),
    );
    let second = event(
        "source-v0",
        2,
        (20 * SECOND_NS, 222),
        "schema-v0",
        Some(b"key"),
        2,
        ScalarValueV0::utf8("same-value").unwrap(),
    );
    accept(&mut state, &first);
    accept(&mut state, &second);
    let view = state
        .materialize_changed_by_key(maximum_changed_config())
        .unwrap();
    assert!(view.is_tracked(&schema("schema-v0"), b"key"));
    assert!(view.get(&schema("schema-v0"), b"key").is_none());
    assert_eq!(view.usage().tracked_entries(), 1);
    assert_eq!(view.usage().changed_entries(), 0);
    assert!(view.entries().next().is_none());
}

#[test]
fn full_payload_difference_and_total_order_select_exact_evidence() {
    let mut state = admission();
    for candidate in [
        basic_event(100, 80 * SECOND_NS, "schema-v0", Some(b"time"), "old"),
        basic_event(1, 100 * SECOND_NS, "schema-v0", Some(b"time"), "new"),
        basic_event(2, 100 * SECOND_NS, "schema-v0", Some(b"tie"), "low"),
        basic_event(3, 100 * SECOND_NS, "schema-v0", Some(b"tie"), "high"),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_changed_by_key(maximum_changed_config())
        .unwrap();
    let time = view.get(&schema("schema-v0"), b"time").unwrap();
    assert_eq!(sequence(time.previous()), 100);
    assert_eq!(sequence(time.current()), 1);
    let tie = view.get(&schema("schema-v0"), b"tie").unwrap();
    assert_eq!(sequence(tie.previous()), 2);
    assert_eq!(sequence(tie.current()), 3);
}

#[test]
fn return_to_value_and_three_value_histories_retain_the_exact_top_pair() {
    let mut state = admission();
    for candidate in [
        basic_event(1, 10 * SECOND_NS, "schema-v0", Some(b"return"), "a"),
        basic_event(2, 20 * SECOND_NS, "schema-v0", Some(b"return"), "b"),
        basic_event(3, 30 * SECOND_NS, "schema-v0", Some(b"return"), "a"),
        basic_event(4, 40 * SECOND_NS, "schema-v0", Some(b"return"), "a"),
        basic_event(5, 50 * SECOND_NS, "schema-v0", Some(b"three"), "a"),
        basic_event(6, 60 * SECOND_NS, "schema-v0", Some(b"three"), "b"),
        basic_event(7, 70 * SECOND_NS, "schema-v0", Some(b"three"), "c"),
        basic_event(8, 80 * SECOND_NS, "schema-v0", Some(b"pivot"), "a"),
        basic_event(9, 70 * SECOND_NS, "schema-v0", Some(b"pivot"), "b"),
        basic_event(10, 90 * SECOND_NS, "schema-v0", Some(b"pivot"), "b"),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_changed_by_key(maximum_changed_config())
        .unwrap();
    let returned = view.get(&schema("schema-v0"), b"return").unwrap();
    assert_eq!(sequence(returned.previous()), 2);
    assert_eq!(sequence(returned.current()), 4);
    let three = view.get(&schema("schema-v0"), b"three").unwrap();
    assert_eq!(sequence(three.previous()), 6);
    assert_eq!(sequence(three.current()), 7);
    let pivot = view.get(&schema("schema-v0"), b"pivot").unwrap();
    assert_eq!(sequence(pivot.previous()), 8);
    assert_eq!(sequence(pivot.current()), 10);
}

#[test]
fn duplicates_and_all_semantic_rejections_cannot_change_the_pair() {
    let mut state = admission();
    let original = event(
        "source-v0",
        1,
        (50 * SECOND_NS, 1),
        "schema-v0",
        Some(b"target"),
        0,
        ScalarValueV0::utf8("before").unwrap(),
    );
    accept(&mut state, &original);
    accept(
        &mut state,
        &basic_event(2, 100 * SECOND_NS, "schema-v0", Some(b"advance"), "advance"),
    );

    let duplicate = event(
        "source-v0",
        1,
        (50 * SECOND_NS, i64::MAX),
        "schema-v0",
        Some(b"target"),
        0,
        ScalarValueV0::utf8("before").unwrap(),
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
            "late",
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
            ScalarValueV0::utf8("wrong-source").unwrap(),
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
            "outside",
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch))
    );

    let view = state
        .materialize_changed_by_key(maximum_changed_config())
        .unwrap();
    assert_eq!(view.usage().accepted_events_considered(), 2);
    assert_eq!(view.usage().tracked_entries(), 2);
    assert_eq!(view.usage().changed_entries(), 0);
    assert!(view.get(&schema("schema-v0"), b"target").is_none());
}

#[test]
fn entry_and_byte_capacity_are_exact_and_admission_is_immutable() {
    let mut state = admission();
    let inputs = [
        basic_event(1, 10 * SECOND_NS, "schema-a", Some(b"a"), "a0"),
        basic_event(2, 20 * SECOND_NS, "schema-a", Some(b"a"), "a1"),
        basic_event(3, 30 * SECOND_NS, "schema-b", Some(b"bb"), "b0"),
    ];
    for candidate in &inputs {
        accept(&mut state, candidate);
    }
    let accepted_before: Vec<_> = state.accepted_events().cloned().collect();
    let usage_before = state.usage();
    let watermark_before = state.watermark_ns();

    let first_bytes =
        changed_index_entry_accounted_bytes_v0(inputs[0].schema_id(), b"a", true).unwrap();
    let second_bytes =
        changed_index_entry_accounted_bytes_v0(inputs[2].schema_id(), b"bb", false).unwrap();
    let exact_bytes = first_bytes + second_bytes;
    let exact = changed_config(ChangedByKeyLimitsV0::new(2, exact_bytes).unwrap());
    let view = state.materialize_changed_by_key(exact).unwrap();
    assert_eq!(view.usage().tracked_entries(), 2);
    assert_eq!(view.usage().changed_entries(), 1);
    assert_eq!(view.usage().index_bytes(), exact_bytes);
    drop(view);

    let entry_error = state
        .materialize_changed_by_key(changed_config(
            ChangedByKeyLimitsV0::new(1, MAX_CHANGED_INDEX_BYTES_V0).unwrap(),
        ))
        .unwrap_err();
    assert_eq!(
        entry_error,
        ChangedMaterializationErrorV0::IndexEntryCapacity {
            maximum: 1,
            attempted: 2,
        }
    );
    let byte_error = state
        .materialize_changed_by_key(changed_config(
            ChangedByKeyLimitsV0::new(2, exact_bytes - 1).unwrap(),
        ))
        .unwrap_err();
    assert_eq!(
        byte_error,
        ChangedMaterializationErrorV0::IndexByteCapacity {
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
fn first_distinct_value_adds_eight_bytes_and_replacements_add_nothing() {
    fn usage_for(values: &[&str]) -> decant_state::ChangedByKeyUsageV0 {
        let mut state = admission();
        for (index, value) in values.iter().enumerate() {
            let sequence = u64::try_from(index + 1).unwrap();
            accept(
                &mut state,
                &basic_event(
                    sequence,
                    i64::try_from(index + 1).unwrap() * SECOND_NS,
                    "schema-v0",
                    Some(b"key"),
                    value,
                ),
            );
        }
        state
            .materialize_changed_by_key(maximum_changed_config())
            .unwrap()
            .usage()
    }

    let one = usage_for(&["a"]);
    let two = usage_for(&["a", "b"]);
    let three = usage_for(&["a", "b", "c"]);
    assert_eq!(one.tracked_entries(), 1);
    assert_eq!(two.tracked_entries(), 1);
    assert_eq!(three.tracked_entries(), 1);
    assert_eq!(one.changed_entries(), 0);
    assert_eq!(two.changed_entries(), 1);
    assert_eq!(three.changed_entries(), 1);
    assert_eq!(two.index_bytes() - one.index_bytes(), 8);
    assert_eq!(three.index_bytes(), two.index_bytes());
}

#[test]
fn first_previous_coordinate_capacity_is_preflighted() {
    let mut state = admission();
    let before = basic_event(1, 10 * SECOND_NS, "schema-v0", Some(b"key"), "before");
    let after = basic_event(2, 20 * SECOND_NS, "schema-v0", Some(b"key"), "after");
    accept(&mut state, &before);
    accept(&mut state, &after);
    let accepted_before: Vec<_> = state.accepted_events().cloned().collect();
    let admission_usage_before = state.usage();
    let base_bytes =
        changed_index_entry_accounted_bytes_v0(before.schema_id(), b"key", false).unwrap();

    let error = state
        .materialize_changed_by_key(changed_config(
            ChangedByKeyLimitsV0::new(1, base_bytes).unwrap(),
        ))
        .unwrap_err();
    assert_eq!(
        error,
        ChangedMaterializationErrorV0::IndexByteCapacity {
            maximum: base_bytes,
            attempted: base_bytes + 8,
        }
    );
    assert_eq!(
        state.accepted_events().cloned().collect::<Vec<_>>(),
        accepted_before
    );
    assert_eq!(state.usage(), admission_usage_before);
}

#[test]
fn changed_iteration_is_canonical_across_permitted_arrival_orders() {
    let inputs = [
        basic_event(1, 60 * SECOND_NS, "schema-b", Some(b"z"), "z0"),
        basic_event(2, 60 * SECOND_NS, "schema-b", Some(b"z"), "z1"),
        basic_event(3, 30 * SECOND_NS, "schema-a", Some(b"b"), "b0"),
        basic_event(4, 60 * SECOND_NS, "schema-a", Some(b"b"), "b1"),
        basic_event(5, 40 * SECOND_NS, "schema-a", Some(b"a"), "a0"),
        basic_event(6, 60 * SECOND_NS, "schema-a", Some(b"a"), "a1"),
    ];
    let expected = vec![
        ("schema-a".to_owned(), b"a".to_vec(), 5, 6),
        ("schema-a".to_owned(), b"b".to_vec(), 3, 4),
        ("schema-b".to_owned(), b"z".to_vec(), 1, 2),
    ];
    for order in [[0, 1, 2, 3, 4, 5], [5, 4, 3, 2, 1, 0]] {
        let mut state = admission();
        for index in order {
            accept(&mut state, &inputs[index]);
        }
        let view = state
            .materialize_changed_by_key(maximum_changed_config())
            .unwrap();
        assert_eq!(view_tuples(&view), expected);
    }
}

fn apply_order(order: &[usize], finalize: bool) -> AdmissionStateV0 {
    let events = [
        basic_event(1, 60 * SECOND_NS, "schema-v0", Some(b"key"), "a"),
        basic_event(2, 90 * SECOND_NS, "schema-v0", Some(b"key"), "b"),
        basic_event(3, 70 * SECOND_NS, "schema-v0", Some(b"key"), "c"),
        basic_event(4, 90 * SECOND_NS, "schema-v0", Some(b"key"), "a"),
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
        .materialize_changed_by_key(maximum_changed_config())
        .unwrap();
    let expected_tuples = view_tuples(&expected_view);
    let expected_usage = expected_view.usage();
    assert_eq!(
        expected_tuples,
        vec![("schema-v0".to_owned(), b"key".to_vec(), 2, 4)]
    );

    for order in orders {
        let first = apply_order(&order, false);
        let replayed = apply_order(&order, false);
        let first_view = first
            .materialize_changed_by_key(maximum_changed_config())
            .unwrap();
        let replayed_view = replayed
            .materialize_changed_by_key(maximum_changed_config())
            .unwrap();
        assert_eq!(view_tuples(&first_view), expected_tuples);
        assert_eq!(view_tuples(&first_view), view_tuples(&replayed_view));
        assert_eq!(first_view.usage(), expected_usage);
        assert_eq!(first_view.usage(), replayed_view.usage());
        assert_eq!(first_view.source_watermark_ns(), Some(60 * SECOND_NS));
        assert!(!first_view.is_source_finalized());
    }

    let finalized_first = apply_order(&[3, 1, 0, 2], true);
    let finalized_replay = apply_order(&[3, 1, 0, 2], true);
    let finalized_first_view = finalized_first
        .materialize_changed_by_key(maximum_changed_config())
        .unwrap();
    let finalized_replay_view = finalized_replay
        .materialize_changed_by_key(maximum_changed_config())
        .unwrap();
    assert_eq!(
        view_tuples(&finalized_first_view),
        view_tuples(&finalized_replay_view)
    );
    assert_eq!(finalized_first_view.usage(), finalized_replay_view.usage());
    assert!(finalized_first_view.is_source_finalized());
}

#[test]
fn source_finalization_changes_only_source_observations() {
    let mut state = apply_order(&[3, 0, 2, 1], false);
    let prefinal = state
        .materialize_changed_by_key(maximum_changed_config())
        .unwrap();
    let expected_tuples = view_tuples(&prefinal);
    let expected_usage = prefinal.usage();
    assert_eq!(prefinal.source_watermark_ns(), Some(60 * SECOND_NS));
    assert!(!prefinal.is_source_finalized());
    drop(prefinal);

    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    let finalized = state
        .materialize_changed_by_key(maximum_changed_config())
        .unwrap();
    assert_eq!(view_tuples(&finalized), expected_tuples);
    assert_eq!(finalized.usage(), expected_usage);
    assert_eq!(finalized.source_watermark_ns(), Some(200 * SECOND_NS));
    assert!(finalized.is_source_finalized());
}

#[test]
fn change_evidence_is_the_exact_normalized_admission_objects_with_provenance() {
    let mut state = admission();
    let before = event(
        "source-v0",
        40,
        (90 * SECOND_NS, i64::MAX),
        "schema-v0",
        Some(b"key"),
        7,
        ScalarValueV0::utf8("before").unwrap(),
    );
    let after = event(
        "source-v0",
        41,
        (100 * SECOND_NS, i64::MAX),
        "schema-v0",
        Some(b"key"),
        7,
        ScalarValueV0::utf8("after").unwrap(),
    );
    accept(&mut state, &before);
    accept(&mut state, &after);
    let retained: Vec<_> = state.accepted_events().collect();
    let view = state
        .materialize_changed_by_key(maximum_changed_config())
        .unwrap();
    let observed = view.get(&schema("schema-v0"), b"key").unwrap();
    assert!(std::ptr::eq(observed.previous(), retained[0]));
    assert!(std::ptr::eq(observed.current(), retained[1]));
    assert_eq!(observed.previous().ingest_time_ns(), 0);
    assert_eq!(observed.current().ingest_time_ns(), 0);
    assert_eq!(
        observed.previous().provenance().as_str(),
        "decant://event/v0/source-v0/synthetic/40"
    );
    assert_eq!(
        observed.current().provenance().as_str(),
        "decant://event/v0/source-v0/synthetic/41"
    );
}

fn batch_reference(state: &AdmissionStateV0) -> Vec<ChangeTuple> {
    let mut groups: BTreeMap<(String, Vec<u8>), Vec<&EventEnvelopeV0>> = BTreeMap::new();
    for event in state.accepted_events() {
        if let Some(key) = event.key() {
            groups
                .entry((event.schema_id().as_str().to_owned(), key.to_vec()))
                .or_default()
                .push(event);
        }
    }

    groups
        .into_iter()
        .filter_map(|((schema_id, key), mut events)| {
            events.sort_by_key(|event| (event.event_time_ns(), sequence(event)));
            let current = *events.last()?;
            let previous = events
                .iter()
                .rev()
                .copied()
                .find(|event| event.payload() != current.payload())?;
            Some((schema_id, key, sequence(previous), sequence(current)))
        })
        .collect()
}

#[test]
fn primary_sre_changes_match_frozen_matrix_and_independent_batch_reference() {
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

    let reference = batch_reference(&state);
    let view = state
        .materialize_changed_by_key(maximum_changed_config())
        .unwrap();
    let expected = vec![
        (
            "deployment-v0".to_owned(),
            b"catalog-v1/region-a".to_vec(),
            98_997,
            98_999,
        ),
        (
            "deployment-v0".to_owned(),
            b"catalog-v1/region-b".to_vec(),
            98_996,
            98_998,
        ),
        (
            "distractor-request-v0".to_owned(),
            b"catalog-v1/region-a".to_vec(),
            98_238,
            98_244,
        ),
        (
            "distractor-request-v0".to_owned(),
            b"catalog-v1/region-b".to_vec(),
            98_241,
            98_247,
        ),
        (
            "distractor-request-v0".to_owned(),
            b"checkout-v1/region-a".to_vec(),
            98_239,
            98_245,
        ),
        (
            "distractor-request-v0".to_owned(),
            b"checkout-v1/region-b".to_vec(),
            98_242,
            98_248,
        ),
        (
            "distractor-request-v0".to_owned(),
            b"search-v1/region-a".to_vec(),
            98_240,
            98_246,
        ),
        (
            "distractor-request-v0".to_owned(),
            b"search-v1/region-b".to_vec(),
            98_243,
            98_249,
        ),
        (
            "metric-sample-v0".to_owned(),
            b"payments-v2/region-a".to_vec(),
            98_976,
            98_977,
        ),
        (
            "metric-sample-v0".to_owned(),
            b"payments-v2/region-b".to_vec(),
            98_978,
            98_979,
        ),
        (
            "payment-outcome-v0".to_owned(),
            b"payments-v2/region-a".to_vec(),
            59_996,
            59_998,
        ),
        (
            "payment-outcome-v0".to_owned(),
            b"payments-v2/region-b".to_vec(),
            59_997,
            59_999,
        ),
    ];
    assert_eq!(view_tuples(&view), expected);
    assert_eq!(view_tuples(&view), reference);
    assert_eq!(view.usage().tracked_entries(), 13);
    assert_eq!(view.usage().changed_entries(), 12);
    assert_eq!(view.usage().accepted_events_considered(), 98_750);
    assert_eq!(view.usage().unkeyed_events_omitted(), 0);
    assert!(view.usage().tracked_entries() <= MAX_CHANGED_INDEX_ENTRIES_V0);
    assert!(view.usage().index_bytes() <= MAX_CHANGED_INDEX_BYTES_V0);
    assert!(view.is_source_finalized());
    assert_eq!(view.source_watermark_ns(), Some(PRIMARY_END_NS));
    assert!(view.is_tracked(&schema("deployment-v0"), b"payments-v2/region-b"));
    assert!(
        view.get(&schema("deployment-v0"), b"payments-v2/region-b")
            .is_none()
    );
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
                9_876_543_209,
                (123_456_789_011, 123_456_789_012),
                schema_id,
                Some(secret.as_bytes()),
                u64::MAX,
                ScalarValueV0::utf8("before-aa").unwrap(),
            ),
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
        let config = maximum_changed_config();
        let view = state.materialize_changed_by_key(config).unwrap();
        let observation = view.get(&schema(schema_id), secret.as_bytes()).unwrap();
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
                (20 * SECOND_NS, 20 * SECOND_NS),
                "other-v00",
                Some(secret.as_bytes()),
                u64::MAX,
                ScalarValueV0::utf8(secret).unwrap(),
            ),
        );
        let error = state
            .materialize_changed_by_key(changed_config(
                ChangedByKeyLimitsV0::new(1, MAX_CHANGED_INDEX_BYTES_V0).unwrap(),
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
            "before-aa",
            "9_876_543_210",
            "9876543210",
            "123456789012",
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
