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
    FinalizeOutcomeV0, LATEST_BY_KEY_VERSION_V0, LatestByKeyConfigErrorV0, LatestByKeyConfigV0,
    LatestByKeyLimitFieldV0, LatestByKeyLimitsV0, LatestByKeyViewV0, LatestMaterializationErrorV0,
    MAX_LATEST_INDEX_BYTES_V0, MAX_LATEST_INDEX_ENTRIES_V0, RejectionReasonV0,
    latest_index_entry_accounted_bytes_v0,
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

fn latest_config(limits: LatestByKeyLimitsV0) -> LatestByKeyConfigV0 {
    LatestByKeyConfigV0::new(LATEST_BY_KEY_VERSION_V0, limits).unwrap()
}

fn maximum_latest_config() -> LatestByKeyConfigV0 {
    latest_config(LatestByKeyLimitsV0::maximum())
}

fn accept(state: &mut AdmissionStateV0, event: &EventEnvelopeV0) {
    assert_eq!(state.apply_event(event), Ok(ApplyOutcomeV0::Accepted));
}

fn view_tuples(view: &LatestByKeyViewV0<'_>) -> Vec<(String, Vec<u8>, u64)> {
    view.entries()
        .map(|(schema_id, key, event)| {
            (
                schema_id.as_str().to_owned(),
                key.to_vec(),
                event.source_position().synthetic_sequence(),
            )
        })
        .collect()
}

#[test]
fn config_enforces_version_and_exact_bounded_limits() {
    let minimum = LatestByKeyLimitsV0::new(1, 1).unwrap();
    let maximum =
        LatestByKeyLimitsV0::new(MAX_LATEST_INDEX_ENTRIES_V0, MAX_LATEST_INDEX_BYTES_V0).unwrap();
    assert_eq!(minimum.index_entry_limit(), 1);
    assert_eq!(minimum.index_byte_limit(), 1);
    assert_eq!(maximum, LatestByKeyLimitsV0::maximum());
    assert!(LatestByKeyConfigV0::new(0, minimum).is_ok());
    assert!(LatestByKeyConfigV0::new(0, maximum).is_ok());
    assert_eq!(
        LatestByKeyConfigV0::new(1, minimum),
        Err(LatestByKeyConfigErrorV0::UnsupportedVersion { actual: 1 })
    );
    assert_eq!(
        LatestByKeyLimitsV0::new(0, 1),
        Err(LatestByKeyConfigErrorV0::ZeroLimit {
            field: LatestByKeyLimitFieldV0::IndexEntries,
        })
    );
    assert_eq!(
        LatestByKeyLimitsV0::new(1, 0),
        Err(LatestByKeyConfigErrorV0::ZeroLimit {
            field: LatestByKeyLimitFieldV0::IndexBytes,
        })
    );
    assert_eq!(
        LatestByKeyLimitsV0::new(MAX_LATEST_INDEX_ENTRIES_V0 + 1, 1),
        Err(LatestByKeyConfigErrorV0::AboveMaximum {
            field: LatestByKeyLimitFieldV0::IndexEntries,
            maximum: MAX_LATEST_INDEX_ENTRIES_V0,
            actual: MAX_LATEST_INDEX_ENTRIES_V0 + 1,
        })
    );
    assert_eq!(
        LatestByKeyLimitsV0::new(1, MAX_LATEST_INDEX_BYTES_V0 + 1),
        Err(LatestByKeyConfigErrorV0::AboveMaximum {
            field: LatestByKeyLimitFieldV0::IndexBytes,
            maximum: MAX_LATEST_INDEX_BYTES_V0,
            actual: MAX_LATEST_INDEX_BYTES_V0 + 1,
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
            latest_index_entry_accounted_bytes_v0(schema_id, key).unwrap(),
            4 + schema_id.as_str().len() + 4 + key.len() + 8
        );
    }
}

#[test]
fn empty_unkeyed_and_empty_key_behavior_is_explicit() {
    let mut state = admission();
    {
        let view = state
            .materialize_latest_by_key(maximum_latest_config())
            .unwrap();
        assert_eq!(view.usage().indexed_entries(), 0);
        assert_eq!(view.usage().index_bytes(), 0);
        assert_eq!(view.usage().accepted_events_considered(), 0);
        assert_eq!(view.usage().unkeyed_events_omitted(), 0);
        assert_eq!(view.source_watermark_ns(), None);
        assert!(!view.is_source_finalized());
        assert!(view.get(&schema("schema-v0"), b"").is_none());
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
        .materialize_latest_by_key(maximum_latest_config())
        .unwrap();
    assert_eq!(view.usage().accepted_events_considered(), 2);
    assert_eq!(view.usage().unkeyed_events_omitted(), 1);
    assert_eq!(view.usage().indexed_entries(), 1);
    assert_eq!(
        view.get(&schema("schema-v0"), b"")
            .unwrap()
            .source_position()
            .synthetic_sequence(),
        2
    );
}

#[test]
fn schema_namespace_and_total_winner_order_are_exact() {
    let mut state = admission();
    let newer_lower_sequence = basic_event(1, 100 * SECOND_NS, "schema-a", Some(b"shared"));
    let older_higher_sequence = basic_event(2, 90 * SECOND_NS, "schema-a", Some(b"shared"));
    let equal_time_higher_sequence = basic_event(3, 100 * SECOND_NS, "schema-a", Some(b"shared"));
    let other_schema = basic_event(4, 80 * SECOND_NS, "schema-b", Some(b"shared"));
    for candidate in [
        &newer_lower_sequence,
        &older_higher_sequence,
        &equal_time_higher_sequence,
        &other_schema,
    ] {
        accept(&mut state, candidate);
    }

    let view = state
        .materialize_latest_by_key(maximum_latest_config())
        .unwrap();
    assert_eq!(view.usage().indexed_entries(), 2);
    assert_eq!(
        view.get(&schema("schema-a"), b"shared")
            .unwrap()
            .source_position()
            .synthetic_sequence(),
        3
    );
    assert_eq!(
        view.get(&schema("schema-b"), b"shared")
            .unwrap()
            .source_position()
            .synthetic_sequence(),
        4
    );
}

#[test]
fn admission_duplicates_and_rejections_cannot_change_latest() {
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
    let advance = basic_event(2, 100 * SECOND_NS, "schema-v0", Some(b"advance"));
    accept(&mut state, &advance);

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
        .materialize_latest_by_key(maximum_latest_config())
        .unwrap();
    assert_eq!(view.usage().accepted_events_considered(), 2);
    assert_eq!(view.usage().indexed_entries(), 2);
    assert_eq!(
        view.get(&schema("schema-v0"), b"target").unwrap().payload(),
        original.payload()
    );
}

#[test]
fn entry_and_byte_capacity_are_exact_and_admission_is_immutable() {
    let mut state = admission();
    let first = basic_event(1, 10 * SECOND_NS, "schema-a", Some(b"a"));
    let second = basic_event(2, 20 * SECOND_NS, "schema-b", Some(b"bb"));
    accept(&mut state, &first);
    accept(&mut state, &second);
    let accepted_before: Vec<_> = state.accepted_events().cloned().collect();
    let usage_before = state.usage();
    let watermark_before = state.watermark_ns();

    let first_bytes = latest_index_entry_accounted_bytes_v0(first.schema_id(), b"a").unwrap();
    let second_bytes = latest_index_entry_accounted_bytes_v0(second.schema_id(), b"bb").unwrap();
    let exact_bytes = first_bytes + second_bytes;
    let exact = latest_config(LatestByKeyLimitsV0::new(2, exact_bytes).unwrap());
    let view = state.materialize_latest_by_key(exact).unwrap();
    assert_eq!(view.usage().indexed_entries(), 2);
    assert_eq!(view.usage().index_bytes(), exact_bytes);
    drop(view);

    let entry_error = state
        .materialize_latest_by_key(latest_config(
            LatestByKeyLimitsV0::new(1, MAX_LATEST_INDEX_BYTES_V0).unwrap(),
        ))
        .unwrap_err();
    assert_eq!(
        entry_error,
        LatestMaterializationErrorV0::IndexEntryCapacity {
            maximum: 1,
            attempted: 2,
        }
    );
    let byte_error = state
        .materialize_latest_by_key(latest_config(
            LatestByKeyLimitsV0::new(2, exact_bytes - 1).unwrap(),
        ))
        .unwrap_err();
    assert_eq!(
        byte_error,
        LatestMaterializationErrorV0::IndexByteCapacity {
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
fn replacement_preserves_index_ledger_and_iteration_is_canonical() {
    let mut state = admission();
    let inputs = [
        basic_event(1, 10 * SECOND_NS, "schema-b", Some(b"z")),
        basic_event(2, 20 * SECOND_NS, "schema-a", Some(b"b")),
        basic_event(3, 30 * SECOND_NS, "schema-a", Some(b"a")),
        basic_event(4, 40 * SECOND_NS, "schema-a", Some(b"b")),
    ];
    for candidate in &inputs {
        accept(&mut state, candidate);
    }
    let view = state
        .materialize_latest_by_key(maximum_latest_config())
        .unwrap();
    let expected_bytes = latest_index_entry_accounted_bytes_v0(&schema("schema-a"), b"a").unwrap()
        + latest_index_entry_accounted_bytes_v0(&schema("schema-a"), b"b").unwrap()
        + latest_index_entry_accounted_bytes_v0(&schema("schema-b"), b"z").unwrap();
    assert_eq!(view.usage().indexed_entries(), 3);
    assert_eq!(view.usage().index_bytes(), expected_bytes);
    assert_eq!(
        view_tuples(&view),
        vec![
            ("schema-a".to_owned(), b"a".to_vec(), 3),
            ("schema-a".to_owned(), b"b".to_vec(), 4),
            ("schema-b".to_owned(), b"z".to_vec(), 1),
        ]
    );
}

fn apply_order(order: &[usize], finalize: bool) -> AdmissionStateV0 {
    let events = [
        basic_event(1, 60 * SECOND_NS, "schema-b", Some(b"b")),
        basic_event(2, 90 * SECOND_NS, "schema-a", Some(b"a")),
        basic_event(3, 70 * SECOND_NS, "schema-a", Some(b"a")),
        basic_event(4, 90 * SECOND_NS, "schema-a", Some(b"a")),
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
        .materialize_latest_by_key(maximum_latest_config())
        .unwrap();
    let expected_tuples = view_tuples(&expected_view);
    let expected_usage = expected_view.usage();
    drop(expected_view);

    for order in orders {
        let first = apply_order(&order, false);
        let replayed = apply_order(&order, false);
        let first_view = first
            .materialize_latest_by_key(maximum_latest_config())
            .unwrap();
        let replayed_view = replayed
            .materialize_latest_by_key(maximum_latest_config())
            .unwrap();
        assert_eq!(view_tuples(&first_view), expected_tuples);
        assert_eq!(view_tuples(&first_view), view_tuples(&replayed_view));
        assert_eq!(first_view.usage(), expected_usage);
        assert_eq!(first_view.usage(), replayed_view.usage());
        assert_eq!(first_view.source_watermark_ns(), Some(60 * SECOND_NS));
        assert_eq!(
            first_view.source_watermark_ns(),
            replayed_view.source_watermark_ns()
        );
        assert!(!first_view.is_source_finalized());
    }

    let finalized_first = apply_order(&[3, 1, 0, 2], true);
    let finalized_replay = apply_order(&[3, 1, 0, 2], true);
    let finalized_first_view = finalized_first
        .materialize_latest_by_key(maximum_latest_config())
        .unwrap();
    let finalized_replay_view = finalized_replay
        .materialize_latest_by_key(maximum_latest_config())
        .unwrap();
    assert_eq!(
        view_tuples(&finalized_first_view),
        view_tuples(&finalized_replay_view)
    );
    assert_eq!(finalized_first_view.usage(), finalized_replay_view.usage());
    assert_eq!(
        finalized_first_view.source_watermark_ns(),
        Some(200 * SECOND_NS)
    );
    assert!(finalized_first_view.is_source_finalized());
}

#[test]
fn finalization_observation_changes_without_changing_winners() {
    let mut state = apply_order(&[3, 0, 2, 1], false);
    let prefinal = state
        .materialize_latest_by_key(maximum_latest_config())
        .unwrap();
    let expected = view_tuples(&prefinal);
    assert_eq!(prefinal.source_watermark_ns(), Some(60 * SECOND_NS));
    assert!(!prefinal.is_source_finalized());
    drop(prefinal);

    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    let finalized = state
        .materialize_latest_by_key(maximum_latest_config())
        .unwrap();
    assert_eq!(view_tuples(&finalized), expected);
    assert_eq!(finalized.source_watermark_ns(), Some(200 * SECOND_NS));
    assert!(finalized.is_source_finalized());
}

#[test]
fn resolved_winner_is_the_normalized_admission_event_with_provenance() {
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
        .materialize_latest_by_key(maximum_latest_config())
        .unwrap();
    let winner = view.get(&schema("schema-v0"), b"key").unwrap();
    assert!(std::ptr::eq(winner, retained));
    assert_eq!(winner.ingest_time_ns(), 0);
    assert_eq!(
        winner.provenance().as_str(),
        "decant://event/v0/source-v0/synthetic/41"
    );
}

#[test]
fn primary_sre_latest_view_matches_the_approved_oracle() {
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
    let view = state
        .materialize_latest_by_key(maximum_latest_config())
        .unwrap();
    let expected = vec![
        ("deployment-v0", "catalog-v1/region-a", 98_999),
        ("deployment-v0", "catalog-v1/region-b", 98_998),
        ("deployment-v0", "payments-v2/region-b", 98_980),
        ("distractor-request-v0", "catalog-v1/region-a", 98_244),
        ("distractor-request-v0", "catalog-v1/region-b", 98_247),
        ("distractor-request-v0", "checkout-v1/region-a", 98_245),
        ("distractor-request-v0", "checkout-v1/region-b", 98_248),
        ("distractor-request-v0", "search-v1/region-a", 98_246),
        ("distractor-request-v0", "search-v1/region-b", 98_249),
        ("metric-sample-v0", "payments-v2/region-a", 98_977),
        ("metric-sample-v0", "payments-v2/region-b", 98_979),
        ("payment-outcome-v0", "payments-v2/region-a", 59_998),
        ("payment-outcome-v0", "payments-v2/region-b", 59_999),
    ];
    let actual: Vec<_> = view
        .entries()
        .map(|(schema_id, key, event)| {
            (
                schema_id.as_str(),
                std::str::from_utf8(key).unwrap(),
                event.source_position().synthetic_sequence(),
            )
        })
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(view.usage().indexed_entries(), 13);
    assert_eq!(view.usage().accepted_events_considered(), 98_750);
    assert_eq!(view.usage().unkeyed_events_omitted(), 0);
    assert!(view.usage().indexed_entries() <= MAX_LATEST_INDEX_ENTRIES_V0);
    assert!(view.usage().index_bytes() <= MAX_LATEST_INDEX_BYTES_V0);
    assert!(view.is_source_finalized());
    assert_eq!(view.source_watermark_ns(), Some(PRIMARY_END_NS));
    for (_, _, event) in view.entries() {
        let sequence = event.source_position().synthetic_sequence();
        assert_eq!(
            event.provenance().as_str(),
            format!("decant://event/v0/sre-v0/synthetic/{sequence}")
        );
        assert_eq!(event.ingest_time_ns(), 0);
    }
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
        let input = event(
            source_id,
            9_876_543_210,
            (123_456_789_012, 123_456_789_013),
            schema_id,
            Some(secret.as_bytes()),
            u64::MAX,
            ScalarValueV0::utf8(secret).unwrap(),
        );
        accept(&mut state, &input);
        let config = maximum_latest_config();
        let view = state.materialize_latest_by_key(config).unwrap();
        format!("{config:?}|{:?}|{view:?}", view.usage())
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
            .materialize_latest_by_key(latest_config(
                LatestByKeyLimitsV0::new(1, MAX_LATEST_INDEX_BYTES_V0).unwrap(),
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
