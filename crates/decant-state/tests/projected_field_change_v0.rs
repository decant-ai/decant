use std::collections::BTreeMap;

use decant_fixtures::{
    FixtureRecordV0, GENERATOR_VERSION_V0, SreGeneratorV0, decode_scenario_manifest_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarKindV0, ScalarValueV0, SchemaIdV0,
    SensitivityBitmapV0, SourceIdV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigV0, FinalizeOutcomeV0,
    MAX_ACCEPTED_EVENT_BYTES_V0, MAX_PROJECTED_FIELD_CHANGE_ENTRIES_V0,
    MAX_PROJECTED_FIELD_CHANGE_INDEX_BYTES_V0, PROJECTED_FIELD_CHANGE_VERSION_V0,
    ProjectedFieldChangeConfigErrorV0, ProjectedFieldChangeConfigV0,
    ProjectedFieldChangeLimitFieldV0, ProjectedFieldChangeLimitsV0,
    ProjectedFieldChangeMaterializationErrorV0, ProjectedFieldChangeObservationV0,
    ProjectedFieldChangeUsageV0, ProjectedFieldChangeViewV0, ProjectedFieldContractIssueV0,
    ProjectedFieldRoleV0, RejectionReasonV0, accepted_event_accounted_bytes_v0,
    projected_field_change_entry_accounted_bytes_v0,
};

const SECOND_NS: i64 = 1_000_000_000;
const PRIMARY_START_NS: i64 = 1_767_225_600_000_000_000;
const PRIMARY_END_NS: i64 = 1_767_226_800_000_000_000;
const PRIMARY_SCENARIO: &[u8] =
    include_bytes!("../../decant-fixtures/contracts/sre-v0/scenario.json");

#[derive(Clone, Debug, Eq, PartialEq)]
enum ScalarRepr {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    Utf8(String),
    Bytes(Vec<u8>),
}

type ChangeTuple = (Vec<u8>, String, ScalarRepr, ScalarRepr, u64, u64);

type ViewEvidence = (
    Option<i64>,
    Vec<ChangeTuple>,
    decant_state::EventTimeRetentionUsageV0,
    ProjectedFieldChangeUsageV0,
    Option<i64>,
    bool,
);

fn source(value: &str) -> SourceIdV0 {
    SourceIdV0::new(value).unwrap()
}

fn schema(value: &str) -> SchemaIdV0 {
    SchemaIdV0::new(value).unwrap()
}

fn payload(fields: impl IntoIterator<Item = (&'static str, ScalarValueV0)>) -> CanonicalPayloadV0 {
    CanonicalPayloadV0::try_from_fields(
        fields
            .into_iter()
            .map(|(name, value)| (name.to_owned(), value))
            .collect(),
    )
    .unwrap()
}

#[allow(clippy::too_many_arguments)]
fn event(
    source_id: &str,
    sequence: u64,
    event_time_ns: i64,
    schema_id: &str,
    key: Option<&[u8]>,
    sensitivity: u64,
    fields: impl IntoIterator<Item = (&'static str, ScalarValueV0)>,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(source_id),
        schema(schema_id),
        SourcePositionV0::Synthetic { sequence },
        key,
        event_time_ns,
        event_time_ns,
        SensitivityBitmapV0::from_bits(sensitivity),
        payload(fields),
    )
    .unwrap()
}

fn projected_event(
    sequence: u64,
    event_time_ns: i64,
    schema_id: &str,
    key: Option<&[u8]>,
    discriminator: ScalarValueV0,
    value: ScalarValueV0,
    noise: u64,
) -> EventEnvelopeV0 {
    event(
        "source-v0",
        sequence,
        event_time_ns,
        schema_id,
        key,
        0,
        [
            ("noise", ScalarValueV0::from_u64(noise)),
            ("series", discriminator),
            ("value", value),
        ],
    )
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
        event_time_ns,
        schema_id,
        key,
        0,
        [("other", ScalarValueV0::from_u64(sequence))],
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

fn config_with(
    horizon_ns: u64,
    schema_id: &str,
    discriminator_field: &str,
    value_field: &str,
    limits: ProjectedFieldChangeLimitsV0,
) -> ProjectedFieldChangeConfigV0 {
    ProjectedFieldChangeConfigV0::new(
        PROJECTED_FIELD_CHANGE_VERSION_V0,
        EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, horizon_ns).unwrap(),
        schema(schema_id),
        discriminator_field,
        value_field,
        limits,
    )
    .unwrap()
}

fn maximum_config(horizon_ns: u64, schema_id: &str) -> ProjectedFieldChangeConfigV0 {
    config_with(
        horizon_ns,
        schema_id,
        "series",
        "value",
        ProjectedFieldChangeLimitsV0::maximum(),
    )
}

fn metric_config(horizon_ns: u64) -> ProjectedFieldChangeConfigV0 {
    config_with(
        horizon_ns,
        "metric-sample-v0",
        "metric_name",
        "metric_value",
        ProjectedFieldChangeLimitsV0::maximum(),
    )
}

fn accept(state: &mut AdmissionStateV0, candidate: &EventEnvelopeV0) {
    assert_eq!(state.apply_event(candidate), Ok(ApplyOutcomeV0::Accepted));
}

fn sequence(candidate: &EventEnvelopeV0) -> u64 {
    candidate.source_position().synthetic_sequence()
}

fn field<'a>(candidate: &'a EventEnvelopeV0, name: &str) -> &'a ScalarValueV0 {
    candidate
        .payload()
        .fields()
        .find_map(|(candidate_name, value)| (candidate_name == name).then_some(value))
        .unwrap()
}

fn scalar_repr(value: &ScalarValueV0) -> ScalarRepr {
    match value.kind() {
        ScalarKindV0::Null => ScalarRepr::Null,
        ScalarKindV0::Bool => ScalarRepr::Bool(value.as_bool().unwrap()),
        ScalarKindV0::I64 => ScalarRepr::I64(value.as_i64().unwrap()),
        ScalarKindV0::U64 => ScalarRepr::U64(value.as_u64().unwrap()),
        ScalarKindV0::Utf8 => ScalarRepr::Utf8(value.as_utf8().unwrap().to_owned()),
        ScalarKindV0::Bytes => ScalarRepr::Bytes(value.as_bytes().unwrap().to_vec()),
    }
}

fn change_tuple(
    key: &[u8],
    discriminator: &str,
    observation: ProjectedFieldChangeObservationV0<'_>,
) -> ChangeTuple {
    (
        key.to_vec(),
        discriminator.to_owned(),
        scalar_repr(observation.previous_value()),
        scalar_repr(observation.current_value()),
        sequence(observation.previous_event()),
        sequence(observation.current_event()),
    )
}

fn view_tuples(view: &ProjectedFieldChangeViewV0<'_>) -> Vec<ChangeTuple> {
    view.entries()
        .map(|(key, discriminator, observation)| change_tuple(key, discriminator, observation))
        .collect()
}

fn assert_usage_conserved(view: &ProjectedFieldChangeViewV0<'_>) {
    let retention = view.retention_usage();
    let usage = view.usage();
    assert_eq!(
        retention.accepted_events_considered(),
        retention.retained_events() + retention.expired_events_excluded()
    );
    assert_eq!(
        retention.retained_events(),
        usage.accepted_events_considered()
    );
    assert_eq!(
        usage.accepted_events_considered(),
        usage.matching_schema_events() + usage.other_schema_events_omitted()
    );
    assert_eq!(
        usage.matching_schema_events(),
        usage.unkeyed_matching_events_omitted() + usage.keyed_matching_events_processed()
    );
}

#[test]
fn config_and_accounting_enforce_exact_boundaries() {
    let minimum_limits = ProjectedFieldChangeLimitsV0::new(1, 1).unwrap();
    let maximum_limits = ProjectedFieldChangeLimitsV0::new(
        MAX_PROJECTED_FIELD_CHANGE_ENTRIES_V0,
        MAX_PROJECTED_FIELD_CHANGE_INDEX_BYTES_V0,
    )
    .unwrap();
    assert_eq!(maximum_limits, ProjectedFieldChangeLimitsV0::maximum());
    let retention = EventTimeRetentionConfigV0::new(0, 1).unwrap();
    let schema_id = schema("schema-v0");
    let maximum_name = format!("a{}", "b".repeat(63));
    let config = ProjectedFieldChangeConfigV0::new(
        0,
        retention,
        schema_id.clone(),
        &maximum_name,
        "value",
        maximum_limits,
    )
    .unwrap();
    assert_eq!(config.version(), 0);
    assert_eq!(config.retention(), retention);
    assert_eq!(config.schema_id(), &schema_id);
    assert_eq!(config.discriminator_field(), maximum_name);
    assert_eq!(config.value_field(), "value");
    assert_eq!(config.limits(), maximum_limits);
    assert_eq!(minimum_limits.index_entry_limit(), 1);
    assert_eq!(minimum_limits.index_byte_limit(), 1);

    assert_eq!(
        ProjectedFieldChangeConfigV0::new(
            1,
            retention,
            schema_id.clone(),
            "series",
            "value",
            maximum_limits,
        ),
        Err(ProjectedFieldChangeConfigErrorV0::UnsupportedVersion { actual: 1 })
    );
    for (role, discriminator, value) in [
        (ProjectedFieldRoleV0::Discriminator, "", "value"),
        (ProjectedFieldRoleV0::Value, "series", ""),
    ] {
        assert_eq!(
            ProjectedFieldChangeConfigV0::new(
                0,
                retention,
                schema_id.clone(),
                discriminator,
                value,
                maximum_limits,
            ),
            Err(ProjectedFieldChangeConfigErrorV0::InvalidFieldNameLength {
                role,
                minimum: 1,
                maximum: 64,
                actual: 0,
            })
        );
    }
    let too_long = format!("a{}", "b".repeat(64));
    assert_eq!(
        ProjectedFieldChangeConfigV0::new(
            0,
            retention,
            schema_id.clone(),
            &too_long,
            "value",
            maximum_limits,
        ),
        Err(ProjectedFieldChangeConfigErrorV0::InvalidFieldNameLength {
            role: ProjectedFieldRoleV0::Discriminator,
            minimum: 1,
            maximum: 64,
            actual: 65,
        })
    );
    assert_eq!(
        ProjectedFieldChangeConfigV0::new(
            0,
            retention,
            schema_id.clone(),
            "Bad",
            "value",
            maximum_limits,
        ),
        Err(ProjectedFieldChangeConfigErrorV0::InvalidFieldNameSyntax {
            role: ProjectedFieldRoleV0::Discriminator,
        })
    );
    assert_eq!(
        ProjectedFieldChangeConfigV0::new(0, retention, schema_id, "same", "same", maximum_limits,),
        Err(ProjectedFieldChangeConfigErrorV0::DuplicateFieldRoles)
    );
    assert_eq!(
        ProjectedFieldChangeLimitsV0::new(0, 1),
        Err(ProjectedFieldChangeConfigErrorV0::ZeroLimit {
            field: ProjectedFieldChangeLimitFieldV0::IndexEntries,
        })
    );
    assert_eq!(
        ProjectedFieldChangeLimitsV0::new(1, 0),
        Err(ProjectedFieldChangeConfigErrorV0::ZeroLimit {
            field: ProjectedFieldChangeLimitFieldV0::IndexBytes,
        })
    );
    assert_eq!(
        ProjectedFieldChangeLimitsV0::new(MAX_PROJECTED_FIELD_CHANGE_ENTRIES_V0 + 1, 1),
        Err(ProjectedFieldChangeConfigErrorV0::AboveMaximum {
            field: ProjectedFieldChangeLimitFieldV0::IndexEntries,
            maximum: MAX_PROJECTED_FIELD_CHANGE_ENTRIES_V0,
            actual: MAX_PROJECTED_FIELD_CHANGE_ENTRIES_V0 + 1,
        })
    );

    for (key, discriminator) in [
        (&[][..], ""),
        (&b"ordinary"[..], "metric"),
        (&vec![b'k'; 256][..], &"d".repeat(4_096)),
    ] {
        let base = 4 + key.len() + 4 + discriminator.len() + 1 + 8;
        assert_eq!(
            projected_field_change_entry_accounted_bytes_v0(key, discriminator, false).unwrap(),
            base
        );
        assert_eq!(
            projected_field_change_entry_accounted_bytes_v0(key, discriminator, true).unwrap(),
            base + 8
        );
    }
}

#[test]
fn empty_and_accounted_omission_behavior_is_exact() {
    let mut state = admission();
    {
        let view = state
            .materialize_projected_field_changes(maximum_config(u64::MAX, "schema-v0"))
            .unwrap();
        assert_eq!(view.retention_cutoff_event_time_ns(), None);
        assert_eq!(view.usage().tracked_series(), 0);
        assert_eq!(view.usage().changed_series(), 0);
        assert_eq!(view.usage().index_bytes(), 0);
        assert_eq!(view.source_watermark_ns(), None);
        assert!(!view.is_source_finalized());
        assert!(!view.is_tracked(b"", ""));
        assert!(view.get(b"", "").is_none());
        assert!(view.entries().next().is_none());
        assert_usage_conserved(&view);
    }

    accept(
        &mut state,
        &basic_event(1, 10 * SECOND_NS, "other-v0", Some(b"other")),
    );
    accept(
        &mut state,
        &event("source-v0", 2, 20 * SECOND_NS, "schema-v0", None, 0, []),
    );
    accept(
        &mut state,
        &projected_event(
            3,
            30 * SECOND_NS,
            "schema-v0",
            Some(b""),
            ScalarValueV0::utf8("").unwrap(),
            ScalarValueV0::from_u64(7),
            0,
        ),
    );
    let view = state
        .materialize_projected_field_changes(maximum_config(u64::MAX, "schema-v0"))
        .unwrap();
    assert_eq!(view.usage().accepted_events_considered(), 3);
    assert_eq!(view.usage().matching_schema_events(), 2);
    assert_eq!(view.usage().other_schema_events_omitted(), 1);
    assert_eq!(view.usage().unkeyed_matching_events_omitted(), 1);
    assert_eq!(view.usage().keyed_matching_events_processed(), 1);
    assert_eq!(view.usage().tracked_series(), 1);
    assert!(view.is_tracked(b"", ""));
    assert_usage_conserved(&view);
}

#[test]
fn matching_schema_contract_fails_closed_and_all_value_kinds_are_valid() {
    for (fields, expected) in [
        (
            vec![("value", ScalarValueV0::from_u64(1))],
            ProjectedFieldChangeMaterializationErrorV0::SchemaContract {
                role: ProjectedFieldRoleV0::Discriminator,
                issue: ProjectedFieldContractIssueV0::Missing,
            },
        ),
        (
            vec![("series", ScalarValueV0::utf8("metric").unwrap())],
            ProjectedFieldChangeMaterializationErrorV0::SchemaContract {
                role: ProjectedFieldRoleV0::Value,
                issue: ProjectedFieldContractIssueV0::Missing,
            },
        ),
        (
            vec![
                ("series", ScalarValueV0::from_u64(7)),
                ("value", ScalarValueV0::from_u64(1)),
            ],
            ProjectedFieldChangeMaterializationErrorV0::SchemaContract {
                role: ProjectedFieldRoleV0::Discriminator,
                issue: ProjectedFieldContractIssueV0::ExpectedUtf8 {
                    actual: ScalarKindV0::U64,
                },
            },
        ),
    ] {
        let mut state = admission();
        accept(
            &mut state,
            &event(
                "source-v0",
                1,
                10 * SECOND_NS,
                "schema-v0",
                Some(b"key"),
                0,
                fields,
            ),
        );
        assert_eq!(
            state
                .materialize_projected_field_changes(maximum_config(u64::MAX, "schema-v0"))
                .unwrap_err(),
            expected
        );
    }

    let values = [
        ScalarValueV0::null(),
        ScalarValueV0::from_bool(true),
        ScalarValueV0::from_i64(-7),
        ScalarValueV0::from_u64(7),
        ScalarValueV0::utf8("text").unwrap(),
        ScalarValueV0::bytes(b"bytes").unwrap(),
    ];
    let mut state = admission();
    for (index, value) in values.into_iter().enumerate() {
        accept(
            &mut state,
            &projected_event(
                index as u64 + 1,
                (index as i64 + 1) * SECOND_NS,
                "schema-v0",
                Some(b"key"),
                ScalarValueV0::utf8(&format!("series-{index}")).unwrap(),
                value,
                0,
            ),
        );
    }
    let view = state
        .materialize_projected_field_changes(maximum_config(u64::MAX, "schema-v0"))
        .unwrap();
    assert_eq!(view.usage().tracked_series(), 6);
    assert_eq!(view.usage().changed_series(), 0);
}

#[test]
fn identity_projection_and_selected_scalar_equality_are_exact() {
    let mut state = admission();
    for candidate in [
        projected_event(
            1,
            10 * SECOND_NS,
            "schema-v0",
            Some(b"key-a"),
            ScalarValueV0::utf8("alpha").unwrap(),
            ScalarValueV0::from_i64(1),
            1,
        ),
        projected_event(
            2,
            20 * SECOND_NS,
            "schema-v0",
            Some(b"key-a"),
            ScalarValueV0::utf8("alpha").unwrap(),
            ScalarValueV0::from_i64(1),
            2,
        ),
        projected_event(
            3,
            30 * SECOND_NS,
            "schema-v0",
            Some(b"key-a"),
            ScalarValueV0::utf8("alpha").unwrap(),
            ScalarValueV0::from_u64(1),
            3,
        ),
        projected_event(
            4,
            40 * SECOND_NS,
            "schema-v0",
            Some(b"key-a"),
            ScalarValueV0::utf8("beta").unwrap(),
            ScalarValueV0::from_u64(1),
            4,
        ),
        projected_event(
            5,
            50 * SECOND_NS,
            "schema-v0",
            Some(b"key-b"),
            ScalarValueV0::utf8("alpha").unwrap(),
            ScalarValueV0::from_u64(1),
            5,
        ),
        projected_event(
            6,
            60 * SECOND_NS,
            "other-v0",
            Some(b"key-a"),
            ScalarValueV0::utf8("alpha").unwrap(),
            ScalarValueV0::from_u64(2),
            6,
        ),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_projected_field_changes(maximum_config(u64::MAX, "schema-v0"))
        .unwrap();
    assert_eq!(view.usage().tracked_series(), 3);
    assert_eq!(view.usage().changed_series(), 1);
    assert!(view.is_tracked(b"key-a", "alpha"));
    assert!(view.is_tracked(b"key-a", "beta"));
    assert!(view.is_tracked(b"key-b", "alpha"));
    let change = view.get(b"key-a", "alpha").unwrap();
    assert_eq!(scalar_repr(change.previous_value()), ScalarRepr::I64(1));
    assert_eq!(scalar_repr(change.current_value()), ScalarRepr::U64(1));
    assert_eq!(sequence(change.previous_event()), 2);
    assert_eq!(sequence(change.current_event()), 3);
}

#[test]
fn total_rank_repetition_and_multi_value_history_are_deterministic() {
    let mut state = admission();
    for candidate in [
        projected_event(
            40,
            40 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("series").unwrap(),
            ScalarValueV0::utf8("a").unwrap(),
            0,
        ),
        projected_event(
            10,
            50 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("series").unwrap(),
            ScalarValueV0::utf8("b").unwrap(),
            0,
        ),
        projected_event(
            20,
            50 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("series").unwrap(),
            ScalarValueV0::utf8("c").unwrap(),
            0,
        ),
        projected_event(
            30,
            50 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("series").unwrap(),
            ScalarValueV0::utf8("a").unwrap(),
            0,
        ),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_projected_field_changes(maximum_config(u64::MAX, "schema-v0"))
        .unwrap();
    let observed = view.get(b"key", "series").unwrap();
    assert_eq!(
        scalar_repr(observed.previous_value()),
        ScalarRepr::Utf8("c".into())
    );
    assert_eq!(
        scalar_repr(observed.current_value()),
        ScalarRepr::Utf8("a".into())
    );
    assert_eq!(sequence(observed.previous_event()), 20);
    assert_eq!(sequence(observed.current_event()), 30);
}

#[test]
fn semantic_rejections_and_admission_caps_cannot_participate() {
    let original = projected_event(
        1,
        40 * SECOND_NS,
        "schema-v0",
        Some(b"key"),
        ScalarValueV0::utf8("series").unwrap(),
        ScalarValueV0::from_u64(1),
        0,
    );
    let mut state = admission();
    accept(&mut state, &original);
    accept(
        &mut state,
        &basic_event(2, 90 * SECOND_NS, "other-v0", Some(b"clock")),
    );
    assert_eq!(
        state.apply_event(&original),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    let conflict = event(
        "source-v0",
        1,
        40 * SECOND_NS,
        "schema-v0",
        Some(b"key"),
        0,
        [
            ("noise", ScalarValueV0::from_u64(0)),
            ("series", ScalarValueV0::utf8("series").unwrap()),
            ("value", ScalarValueV0::from_u64(2)),
        ],
    );
    assert_eq!(
        state.apply_event(&conflict),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::ConflictingRedelivery
        ))
    );
    assert_eq!(
        state.apply_event(&projected_event(
            3,
            60 * SECOND_NS - 1,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("series").unwrap(),
            ScalarValueV0::from_u64(2),
            0,
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::Late))
    );
    assert_eq!(
        state.apply_event(&event(
            "source-x0",
            4,
            100 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            [
                ("series", ScalarValueV0::utf8("series").unwrap()),
                ("value", ScalarValueV0::from_u64(2)),
            ],
        )),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::UnexpectedSource
        ))
    );
    assert_eq!(
        state.apply_event(&projected_event(
            5,
            200 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("series").unwrap(),
            ScalarValueV0::from_u64(2),
            0,
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch))
    );
    let view = state
        .materialize_projected_field_changes(maximum_config(u64::MAX, "schema-v0"))
        .unwrap();
    assert_eq!(view.usage().keyed_matching_events_processed(), 1);
    assert_eq!(view.usage().changed_series(), 0);
    drop(view);

    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    assert_eq!(
        state.apply_event(&projected_event(
            6,
            100 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("series").unwrap(),
            ScalarValueV0::from_u64(2),
            0,
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::EpochFinalized))
    );

    let first_bytes = accepted_event_accounted_bytes_v0(&original).unwrap();
    for (limits, expected) in [
        (
            AdmissionLimitsV0::new(1, MAX_ACCEPTED_EVENT_BYTES_V0).unwrap(),
            RejectionReasonV0::AcceptedIdentityCapacity,
        ),
        (
            AdmissionLimitsV0::new(2, first_bytes).unwrap(),
            RejectionReasonV0::AcceptedEventByteCapacity,
        ),
    ] {
        let mut limited = admission_with(limits);
        accept(&mut limited, &original);
        let next = projected_event(
            2,
            41 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("series").unwrap(),
            ScalarValueV0::from_u64(2),
            0,
        );
        assert_eq!(
            limited.apply_event(&next),
            Ok(ApplyOutcomeV0::Rejected(expected))
        );
        let view = limited
            .materialize_projected_field_changes(maximum_config(u64::MAX, "schema-v0"))
            .unwrap();
        assert_eq!(view.usage().keyed_matching_events_processed(), 1);
        assert_eq!(view.usage().changed_series(), 0);
    }
}

#[test]
fn retention_precedes_projection_at_exact_cutoff() {
    let mut state = admission();
    for candidate in [
        projected_event(
            1,
            39 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("series").unwrap(),
            ScalarValueV0::from_u64(1),
            0,
        ),
        projected_event(
            2,
            40 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("series").unwrap(),
            ScalarValueV0::from_u64(2),
            0,
        ),
        projected_event(
            3,
            41 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("series").unwrap(),
            ScalarValueV0::from_u64(3),
            0,
        ),
        basic_event(4, 90 * SECOND_NS, "clock-v0", Some(b"clock")),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .materialize_projected_field_changes(maximum_config(20 * SECOND_NS as u64, "schema-v0"))
        .unwrap();
    assert_eq!(view.retention_cutoff_event_time_ns(), Some(40 * SECOND_NS));
    assert_eq!(view.retention_usage().expired_events_excluded(), 1);
    let observed = view.get(b"key", "series").unwrap();
    assert_eq!(scalar_repr(observed.previous_value()), ScalarRepr::U64(2));
    assert_eq!(scalar_repr(observed.current_value()), ScalarRepr::U64(3));
    assert_eq!(sequence(observed.previous_event()), 2);
    assert_eq!(sequence(observed.current_event()), 3);
    assert_usage_conserved(&view);
}

#[test]
fn retained_selected_subset_alone_consumes_capacity_and_bytes() {
    let mut state = admission();
    for candidate in [
        projected_event(
            1,
            10 * SECOND_NS,
            "schema-v0",
            Some(b"expired"),
            ScalarValueV0::utf8("old").unwrap(),
            ScalarValueV0::from_u64(1),
            0,
        ),
        basic_event(2, 150 * SECOND_NS, "other-v0", Some(b"other")),
        projected_event(
            3,
            151 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("one").unwrap(),
            ScalarValueV0::from_u64(1),
            0,
        ),
        projected_event(
            4,
            152 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("two").unwrap(),
            ScalarValueV0::from_u64(1),
            0,
        ),
        basic_event(5, 190 * SECOND_NS, "clock-v0", Some(b"clock")),
    ] {
        accept(&mut state, &candidate);
    }
    let horizon = 60 * SECOND_NS as u64;
    let one_bytes = projected_field_change_entry_accounted_bytes_v0(b"key", "one", false).unwrap();
    let two_bytes = projected_field_change_entry_accounted_bytes_v0(b"key", "two", false).unwrap();
    let exact_bytes = one_bytes + two_bytes;
    let exact = config_with(
        horizon,
        "schema-v0",
        "series",
        "value",
        ProjectedFieldChangeLimitsV0::new(2, exact_bytes).unwrap(),
    );
    let view = state.materialize_projected_field_changes(exact).unwrap();
    assert_eq!(view.retention_cutoff_event_time_ns(), Some(100 * SECOND_NS));
    assert_eq!(view.retention_usage().expired_events_excluded(), 1);
    assert_eq!(view.usage().other_schema_events_omitted(), 2);
    assert_eq!(view.usage().tracked_series(), 2);
    assert_eq!(view.usage().index_bytes(), exact_bytes);
    drop(view);

    assert_eq!(
        state
            .materialize_projected_field_changes(config_with(
                horizon,
                "schema-v0",
                "series",
                "value",
                ProjectedFieldChangeLimitsV0::new(1, MAX_PROJECTED_FIELD_CHANGE_INDEX_BYTES_V0,)
                    .unwrap(),
            ))
            .unwrap_err(),
        ProjectedFieldChangeMaterializationErrorV0::IndexEntryCapacity {
            maximum: 1,
            attempted: 2,
        }
    );
    assert_eq!(
        state
            .materialize_projected_field_changes(config_with(
                horizon,
                "schema-v0",
                "series",
                "value",
                ProjectedFieldChangeLimitsV0::new(2, exact_bytes - 1).unwrap(),
            ))
            .unwrap_err(),
        ProjectedFieldChangeMaterializationErrorV0::IndexByteCapacity {
            maximum: exact_bytes - 1,
            attempted: exact_bytes,
        }
    );
}

#[test]
fn first_change_adds_eight_bytes_and_replacements_add_nothing() {
    let mut state = admission();
    accept(
        &mut state,
        &projected_event(
            1,
            10 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("series").unwrap(),
            ScalarValueV0::from_u64(1),
            0,
        ),
    );
    let config = maximum_config(u64::MAX, "schema-v0");
    let initial = state
        .materialize_projected_field_changes(config.clone())
        .unwrap();
    let initial_usage = initial.usage();
    drop(initial);
    accept(
        &mut state,
        &projected_event(
            2,
            20 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("series").unwrap(),
            ScalarValueV0::from_u64(2),
            0,
        ),
    );
    let changed = state
        .materialize_projected_field_changes(config.clone())
        .unwrap();
    let changed_usage = changed.usage();
    assert_eq!(changed_usage.tracked_series(), 1);
    assert_eq!(changed_usage.changed_series(), 1);
    assert_eq!(changed_usage.index_bytes() - initial_usage.index_bytes(), 8);
    drop(changed);
    accept(
        &mut state,
        &projected_event(
            3,
            30 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            ScalarValueV0::utf8("series").unwrap(),
            ScalarValueV0::from_u64(3),
            0,
        ),
    );
    let replaced = state.materialize_projected_field_changes(config).unwrap();
    assert_eq!(replaced.usage().tracked_series(), 1);
    assert_eq!(replaced.usage().changed_series(), 1);
    assert_eq!(replaced.usage().index_bytes(), changed_usage.index_bytes());
}

fn permutation_events() -> [EventEnvelopeV0; 4] {
    [
        projected_event(
            1,
            60 * SECOND_NS,
            "schema-v0",
            Some(b"z"),
            ScalarValueV0::utf8("one").unwrap(),
            ScalarValueV0::from_u64(1),
            0,
        ),
        projected_event(
            2,
            70 * SECOND_NS,
            "schema-v0",
            Some(b"a"),
            ScalarValueV0::utf8("two").unwrap(),
            ScalarValueV0::from_u64(1),
            0,
        ),
        projected_event(
            3,
            80 * SECOND_NS,
            "schema-v0",
            Some(b"a"),
            ScalarValueV0::utf8("one").unwrap(),
            ScalarValueV0::from_u64(1),
            0,
        ),
        projected_event(
            4,
            90 * SECOND_NS,
            "schema-v0",
            Some(b"a"),
            ScalarValueV0::utf8("one").unwrap(),
            ScalarValueV0::from_u64(2),
            0,
        ),
    ]
}

fn evidence_for_order(order: [usize; 4]) -> ViewEvidence {
    let events = permutation_events();
    let mut state = admission();
    for index in order {
        accept(&mut state, &events[index]);
    }
    let view = state
        .materialize_projected_field_changes(maximum_config(u64::MAX, "schema-v0"))
        .unwrap();
    (
        view.retention_cutoff_event_time_ns(),
        view_tuples(&view),
        view.retention_usage(),
        view.usage(),
        view.source_watermark_ns(),
        view.is_source_finalized(),
    )
}

#[test]
fn every_permutation_fresh_replay_and_iteration_order_converge() {
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
    let expected = evidence_for_order(permutations[0]);
    for order in permutations {
        assert_eq!(evidence_for_order(order), expected);
    }
    assert_eq!(expected.1.len(), 1);
    assert_eq!(expected.1[0].0, b"a");
    assert_eq!(expected.1[0].1, "one");
    assert_eq!(expected.1[0].4, 3);
    assert_eq!(expected.1[0].5, 4);
    assert_eq!(evidence_for_order([0, 1, 2, 3]), expected);
}

#[test]
fn finalization_rebuild_removes_transition_and_evidence_is_exact() {
    let mut state = admission();
    let previous = event(
        "source-v0",
        1,
        40 * SECOND_NS,
        "schema-v0",
        Some(b"key"),
        1,
        [
            ("series", ScalarValueV0::utf8("series").unwrap()),
            ("value", ScalarValueV0::from_u64(1)),
        ],
    );
    let current = event(
        "source-v0",
        2,
        90 * SECOND_NS,
        "schema-v0",
        Some(b"key"),
        2,
        [
            ("series", ScalarValueV0::utf8("series").unwrap()),
            ("value", ScalarValueV0::from_u64(2)),
        ],
    );
    accept(&mut state, &previous);
    accept(&mut state, &current);
    let admitted: Vec<_> = state.accepted_events().collect();
    let config = maximum_config(130 * SECOND_NS as u64, "schema-v0");
    {
        let before = state
            .materialize_projected_field_changes(config.clone())
            .unwrap();
        let observed = before.get(b"key", "series").unwrap();
        assert!(std::ptr::eq(observed.previous_event(), admitted[0]));
        assert!(std::ptr::eq(observed.current_event(), admitted[1]));
        assert!(std::ptr::eq(
            observed.previous_value(),
            field(admitted[0], "value")
        ));
        assert!(std::ptr::eq(
            observed.current_value(),
            field(admitted[1], "value")
        ));
        assert_eq!(
            observed.previous_event().provenance(),
            previous.provenance()
        );
        assert_eq!(observed.current_event().provenance(), current.provenance());
        assert_eq!(
            observed.previous_event().sensitivity(),
            previous.sensitivity()
        );
        assert_eq!(
            observed.current_event().sensitivity(),
            current.sensitivity()
        );
    }
    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    let after = state.materialize_projected_field_changes(config).unwrap();
    assert_eq!(after.retention_cutoff_event_time_ns(), Some(70 * SECOND_NS));
    assert!(after.is_tracked(b"key", "series"));
    assert!(after.get(b"key", "series").is_none());
    assert_eq!(after.usage().changed_series(), 0);
    assert!(after.is_source_finalized());
}

fn scenario_state(scenario_id: &str) -> AdmissionStateV0 {
    let manifest = decode_scenario_manifest_v0(PRIMARY_SCENARIO).unwrap();
    let generator = SreGeneratorV0::new(GENERATOR_VERSION_V0, &manifest, scenario_id).unwrap();
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

fn reference_changes(state: &AdmissionStateV0, cutoff_ns: i64) -> Vec<ChangeTuple> {
    let mut grouped: BTreeMap<(Vec<u8>, String), Vec<&EventEnvelopeV0>> = BTreeMap::new();
    for candidate in state.accepted_events().filter(|candidate| {
        candidate.event_time_ns() >= cutoff_ns
            && candidate.schema_id() == &schema("metric-sample-v0")
    }) {
        let key = candidate.key().unwrap().to_vec();
        let discriminator = field(candidate, "metric_name")
            .as_utf8()
            .unwrap()
            .to_owned();
        grouped
            .entry((key, discriminator))
            .or_default()
            .push(candidate);
    }
    let mut changes = Vec::new();
    for ((key, discriminator), mut candidates) in grouped {
        candidates.sort_by_key(|candidate| (candidate.event_time_ns(), sequence(candidate)));
        let current = *candidates.last().unwrap();
        let current_value = field(current, "metric_value");
        let previous = candidates
            .into_iter()
            .rev()
            .find(|candidate| field(candidate, "metric_value") != current_value);
        if let Some(previous) = previous {
            changes.push((
                key,
                discriminator,
                scalar_repr(field(previous, "metric_value")),
                scalar_repr(current_value),
                sequence(previous),
                sequence(current),
            ));
        }
    }
    changes
}

#[test]
fn all_sre_scenarios_match_independent_projection_and_expected_metamorphics() {
    let epoch_horizon = (PRIMARY_END_NS - PRIMARY_START_NS) as u64;
    for (scenario_id, affected_key, expected_changed) in [
        ("sre-v0-primary", b"payments-v2/region-b".as_slice(), 2),
        ("sre-v0-region-swap", b"payments-v2/region-a".as_slice(), 2),
        ("sre-v0-service-swap", b"checkout-v1/region-b".as_slice(), 2),
        (
            "sre-v0-no-regression",
            b"payments-v2/region-b".as_slice(),
            0,
        ),
    ] {
        let state = scenario_state(scenario_id);
        let view = state
            .materialize_projected_field_changes(metric_config(epoch_horizon))
            .unwrap();
        assert_eq!(
            view.retention_cutoff_event_time_ns(),
            Some(PRIMARY_START_NS)
        );
        assert_eq!(view.retention_usage().accepted_events_considered(), 98_750);
        assert_eq!(view.retention_usage().retained_events(), 98_750);
        assert_eq!(view.retention_usage().expired_events_excluded(), 0);
        assert_eq!(view.usage().matching_schema_events(), 480);
        assert_eq!(view.usage().other_schema_events_omitted(), 98_270);
        assert_eq!(view.usage().unkeyed_matching_events_omitted(), 0);
        assert_eq!(view.usage().keyed_matching_events_processed(), 480);
        assert_eq!(view.usage().tracked_series(), 4);
        assert_eq!(view.usage().changed_series(), expected_changed);
        assert_eq!(
            view_tuples(&view),
            reference_changes(&state, PRIMARY_START_NS)
        );
        assert_usage_conserved(&view);
        if expected_changed == 2 {
            let error_rate = view.get(affected_key, "error_rate_ppm").unwrap();
            assert_eq!(error_rate.previous_value().as_u64(), Some(4_000));
            assert_eq!(error_rate.current_value().as_u64(), Some(32_000));
            let latency = view.get(affected_key, "p99_latency_ms").unwrap();
            assert_eq!(latency.previous_value().as_u64(), Some(180));
            assert_eq!(latency.current_value().as_u64(), Some(720));
        } else {
            assert!(view.entries().next().is_none());
        }
    }
}

#[test]
fn finalized_ten_minute_horizon_cannot_use_expired_pre_change_values() {
    let state = scenario_state("sre-v0-primary");
    let view = state
        .materialize_projected_field_changes(metric_config(10 * 60 * SECOND_NS as u64))
        .unwrap();
    assert_eq!(
        view.retention_cutoff_event_time_ns(),
        Some(PRIMARY_START_NS + 10 * 60 * SECOND_NS)
    );
    assert_eq!(view.usage().tracked_series(), 4);
    assert_eq!(view.usage().changed_series(), 0);
    assert!(view.entries().next().is_none());
    assert_usage_conserved(&view);
}

struct DiagnosticInput<'a> {
    source_id: &'a str,
    schema_id: &'a str,
    field_names: (&'a str, &'a str),
    key: &'a [u8],
    discriminator: &'a str,
    values: (u64, u64),
    sequences: (u64, u64),
    times: (i64, i64),
}

fn diagnostic(input: DiagnosticInput<'_>) -> String {
    let DiagnosticInput {
        source_id,
        schema_id,
        field_names,
        key,
        discriminator,
        values,
        sequences,
        times,
    } = input;
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
    for (sequence, time, value) in [
        (sequences.0, times.0, values.0),
        (sequences.1, times.1, values.1),
    ] {
        accept(
            &mut state,
            &event(
                source_id,
                sequence,
                time,
                schema_id,
                Some(key),
                u64::MAX,
                [
                    (
                        if field_names.0 == "seriesaa" {
                            "seriesaa"
                        } else {
                            "seriesbb"
                        },
                        ScalarValueV0::utf8(discriminator).unwrap(),
                    ),
                    (
                        if field_names.1 == "valueaaa" {
                            "valueaaa"
                        } else {
                            "valuebbb"
                        },
                        ScalarValueV0::from_u64(value),
                    ),
                ],
            ),
        );
    }
    let config = config_with(
        100 * SECOND_NS as u64,
        schema_id,
        field_names.0,
        field_names.1,
        ProjectedFieldChangeLimitsV0::maximum(),
    );
    let view = state
        .materialize_projected_field_changes(config.clone())
        .unwrap();
    let observation = view.get(key, discriminator).unwrap();
    format!(
        "{config:?}|{:?}|{:?}|{observation:?}|{view:?}|{:?}",
        view.retention_usage(),
        view.usage(),
        ProjectedFieldChangeConfigErrorV0::DuplicateFieldRoles,
    )
}

fn error_diagnostic(
    source_id: &str,
    schema_id: &str,
    field_names: (&str, &str),
    keys: (&[u8], &[u8]),
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
    for (sequence, key, discriminator) in [(1, keys.0, "series-a"), (2, keys.1, "series-b")] {
        accept(
            &mut state,
            &event(
                source_id,
                sequence,
                (39 + sequence as i64) * SECOND_NS,
                schema_id,
                Some(key),
                u64::MAX,
                [
                    (
                        if field_names.0 == "seriesaa" {
                            "seriesaa"
                        } else {
                            "seriesbb"
                        },
                        ScalarValueV0::utf8(discriminator).unwrap(),
                    ),
                    (
                        if field_names.1 == "valueaaa" {
                            "valueaaa"
                        } else {
                            "valuebbb"
                        },
                        ScalarValueV0::from_u64(sequence),
                    ),
                ],
            ),
        );
    }
    let error = state
        .materialize_projected_field_changes(config_with(
            100 * SECOND_NS as u64,
            schema_id,
            field_names.0,
            field_names.1,
            ProjectedFieldChangeLimitsV0::new(1, MAX_PROJECTED_FIELD_CHANGE_INDEX_BYTES_V0)
                .unwrap(),
        ))
        .unwrap_err();
    format!("{error:?}|{error}")
}

#[test]
fn diagnostics_are_content_independent_and_value_safe() {
    let first = diagnostic(DiagnosticInput {
        source_id: "source-aa",
        schema_id: "schema-aa",
        field_names: ("seriesaa", "valueaaa"),
        key: b"secret-aa",
        discriminator: "metric-aa",
        values: (111, 112),
        sequences: (301, 302),
        times: (40 * SECOND_NS, 41 * SECOND_NS),
    });
    let second = diagnostic(DiagnosticInput {
        source_id: "source-bb",
        schema_id: "schema-bb",
        field_names: ("seriesbb", "valuebbb"),
        key: b"secret-bb",
        discriminator: "metric-bb",
        values: (998, 999),
        sequences: (701, 702),
        times: (50 * SECOND_NS, 51 * SECOND_NS),
    });
    assert_eq!(first, second);
    let first_error = error_diagnostic(
        "source-aa",
        "schema-aa",
        ("seriesaa", "valueaaa"),
        (b"secret-aa", b"secret-ab"),
    );
    let second_error = error_diagnostic(
        "source-bb",
        "schema-bb",
        ("seriesbb", "valuebbb"),
        (b"secret-ba", b"secret-bb"),
    );
    assert_eq!(first_error, second_error);
    for forbidden in [
        "source-aa",
        "source-bb",
        "schema-aa",
        "schema-bb",
        "seriesaa",
        "seriesbb",
        "valueaaa",
        "valuebbb",
        "secret-aa",
        "secret-ab",
        "secret-ba",
        "secret-bb",
        "metric-aa",
        "metric-bb",
        "decant://",
        "111",
        "112",
        "998",
        "999",
        "301",
        "302",
        "701",
        "702",
        "40000000000",
        "51000000000",
    ] {
        assert!(!first.contains(forbidden));
        assert!(!second.contains(forbidden));
        assert!(!first_error.contains(forbidden));
        assert!(!second_error.contains(forbidden));
    }
}
