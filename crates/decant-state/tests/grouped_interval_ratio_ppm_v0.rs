use std::collections::BTreeMap;

use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigV0,
    GROUPED_INTERVAL_RATIO_PPM_VERSION_V0, GroupedIntervalRatioFieldIssueV0,
    GroupedIntervalRatioFieldRoleV0, GroupedIntervalRatioLimitsV0,
    GroupedIntervalRatioMaterializationErrorV0, GroupedIntervalRatioPpmConfigV0,
};

fn source() -> SourceIdV0 {
    SourceIdV0::new("grouped-ratio-source").unwrap()
}

fn schema() -> SchemaIdV0 {
    SchemaIdV0::new("orders-v0").unwrap()
}

fn state() -> AdmissionStateV0 {
    AdmissionStateV0::new(
        AdmissionConfigV0::new(
            ADMISSION_VERSION_V0,
            source(),
            0,
            100_000_000_000,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    )
}

fn event(
    sequence: u64,
    event_time_ns: i64,
    service: ScalarValueV0,
    is_error: ScalarValueV0,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(),
        schema(),
        SourcePositionV0::Synthetic { sequence },
        None,
        event_time_ns,
        0,
        SensitivityBitmapV0::from_bits(u64::MAX),
        CanonicalPayloadV0::try_from_fields(BTreeMap::from([
            ("is_error".to_owned(), is_error),
            ("service".to_owned(), service),
        ]))
        .unwrap(),
    )
    .unwrap()
}

fn config(limits: GroupedIntervalRatioLimitsV0) -> GroupedIntervalRatioPpmConfigV0 {
    GroupedIntervalRatioPpmConfigV0::new(
        GROUPED_INTERVAL_RATIO_PPM_VERSION_V0,
        EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, u64::MAX).unwrap(),
        schema(),
        "service",
        "is_error",
        true,
        0,
        100,
        3,
        limits,
    )
    .unwrap()
}

fn accept(state: &mut AdmissionStateV0, event: &EventEnvelopeV0) {
    assert_eq!(state.apply_event(event), Ok(ApplyOutcomeV0::Accepted));
}

fn observations(state: &AdmissionStateV0) -> Vec<(String, u64, u64, u64, u64, Vec<u64>)> {
    state
        .materialize_grouped_interval_ratio_ppm(config(GroupedIntervalRatioLimitsV0::maximum()))
        .unwrap()
        .groups()
        .map(|(group, aggregate)| {
            (
                group.to_owned(),
                aggregate.numerator(),
                aggregate.denominator(),
                aggregate.rate_ppm(),
                aggregate.remainder(),
                aggregate
                    .numerator_evidence()
                    .map(|event| event.source_position().synthetic_sequence())
                    .collect(),
            )
        })
        .collect()
}

#[test]
fn ratios_group_order_and_evidence_rank_are_exact_and_permutation_stable() {
    let events = [
        event(
            4,
            20,
            ScalarValueV0::utf8("payments").unwrap(),
            ScalarValueV0::from_bool(true),
        ),
        event(
            1,
            10,
            ScalarValueV0::utf8("checkout").unwrap(),
            ScalarValueV0::from_bool(false),
        ),
        event(
            3,
            20,
            ScalarValueV0::utf8("payments").unwrap(),
            ScalarValueV0::from_bool(true),
        ),
        event(
            2,
            15,
            ScalarValueV0::utf8("payments").unwrap(),
            ScalarValueV0::from_bool(false),
        ),
    ];
    let mut left = state();
    for candidate in &events {
        accept(&mut left, candidate);
    }
    assert_eq!(
        left.apply_event(&events[0]),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    let mut right = state();
    for index in [2, 0, 3, 1] {
        accept(&mut right, &events[index]);
    }

    let expected = vec![
        ("checkout".to_owned(), 0, 1, 0, 0, vec![]),
        ("payments".to_owned(), 2, 3, 666_666, 2, vec![3, 4]),
    ];
    assert_eq!(observations(&left), expected);
    assert_eq!(observations(&right), expected);
}

#[test]
fn half_open_interval_and_evidence_limit_are_visible() {
    let mut state = state();
    for candidate in [
        event(
            0,
            0,
            ScalarValueV0::utf8("payments").unwrap(),
            ScalarValueV0::from_bool(true),
        ),
        event(
            1,
            50,
            ScalarValueV0::utf8("payments").unwrap(),
            ScalarValueV0::from_bool(true),
        ),
        event(
            2,
            99,
            ScalarValueV0::utf8("payments").unwrap(),
            ScalarValueV0::from_bool(true),
        ),
        event(
            3,
            100,
            ScalarValueV0::utf8("payments").unwrap(),
            ScalarValueV0::from_bool(true),
        ),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_grouped_interval_ratio_ppm(config(GroupedIntervalRatioLimitsV0::maximum()))
        .unwrap();
    let (_, aggregate) = view.groups().next().unwrap();
    assert_eq!(aggregate.denominator(), 3);
    assert_eq!(aggregate.numerator(), 3);
    assert_eq!(aggregate.numerator_evidence().len(), 3);
    assert_eq!(view.usage().at_or_after_interval_events_omitted(), 1);
}

#[test]
fn group_and_index_limits_fail_without_a_partial_view() {
    let mut state = state();
    accept(
        &mut state,
        &event(
            0,
            1,
            ScalarValueV0::utf8("a").unwrap(),
            ScalarValueV0::from_bool(false),
        ),
    );
    accept(
        &mut state,
        &event(
            1,
            2,
            ScalarValueV0::utf8("b").unwrap(),
            ScalarValueV0::from_bool(false),
        ),
    );
    let one_group = GroupedIntervalRatioLimitsV0::new(1, 1_024).unwrap();
    assert_eq!(
        state
            .materialize_grouped_interval_ratio_ppm(config(one_group))
            .err()
            .unwrap(),
        GroupedIntervalRatioMaterializationErrorV0::GroupCapacity {
            maximum: 1,
            attempted: 2,
        }
    );

    let exact_first_entry = 4 + 1 + 16 + (3 * 21);
    let short = GroupedIntervalRatioLimitsV0::new(2, exact_first_entry - 1).unwrap();
    assert_eq!(
        state
            .materialize_grouped_interval_ratio_ppm(config(short))
            .err()
            .unwrap(),
        GroupedIntervalRatioMaterializationErrorV0::IndexByteCapacity {
            maximum: exact_first_entry - 1,
            attempted: exact_first_entry,
        }
    );
}

#[test]
fn schema_field_contract_fails_closed_and_debug_redacts_values() {
    let private_group = "private-group-sentinel";
    let mut state = state();
    accept(
        &mut state,
        &event(
            0,
            1,
            ScalarValueV0::from_u64(7),
            ScalarValueV0::from_bool(true),
        ),
    );
    let error = state
        .materialize_grouped_interval_ratio_ppm(config(GroupedIntervalRatioLimitsV0::maximum()))
        .unwrap_err();
    assert_eq!(
        error,
        GroupedIntervalRatioMaterializationErrorV0::FieldContract {
            role: GroupedIntervalRatioFieldRoleV0::Group,
            issue: GroupedIntervalRatioFieldIssueV0::WrongKind {
                expected: decant_ir::ScalarKindV0::Utf8,
                actual: decant_ir::ScalarKindV0::U64,
            },
        }
    );
    let diagnostics = format!(
        "{error:?} {:?}",
        config(GroupedIntervalRatioLimitsV0::maximum())
    );
    assert!(!diagnostics.contains(private_group));
    assert!(!diagnostics.contains("service"));
    assert!(!diagnostics.contains("is_error"));
}
