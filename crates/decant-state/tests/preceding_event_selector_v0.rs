use std::collections::BTreeMap;

use decant_fixtures::{
    FixtureRecordV0, GENERATOR_VERSION_V0, SreGeneratorV0, decode_scenario_manifest_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, MAX_KEY_BYTES, ScalarValueV0, SchemaIdV0,
    SensitivityBitmapV0, SourceIdV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigV0, FinalizeOutcomeV0,
    MAX_PROJECTED_FIELD_CHANGE_INDEX_BYTES_V0, PRECEDING_EVENT_SELECTOR_VERSION_V0,
    PROJECTED_FIELD_CHANGE_VERSION_V0, PrecedingEventSelectorConfigErrorV0,
    PrecedingEventSelectorConfigV0, PrecedingEventSelectorErrorV0, PrecedingEventSelectorUsageV0,
    PrecedingEventSelectorViewV0, ProjectedFieldChangeConfigV0, ProjectedFieldChangeLimitsV0,
    RejectionReasonV0, accepted_event_accounted_bytes_v0,
};

const SECOND_NS: i64 = 1_000_000_000;
const MINUTE_NS: i64 = 60 * SECOND_NS;
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
    CanonicalPayloadV0::try_from_fields(BTreeMap::from([(
        "value".to_owned(),
        ScalarValueV0::from_u64(value),
    )]))
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
    value: u64,
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
        payload(value),
    )
    .unwrap()
}

fn admission_with(
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
    admission_with(0, 200 * SECOND_NS, AdmissionLimitsV0::maximum())
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

fn retention(horizon_ns: u64) -> EventTimeRetentionConfigV0 {
    EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, horizon_ns).unwrap()
}

fn config(
    horizon_ns: u64,
    schema_id: &str,
    key: &[u8],
    anchor_event_time_ns: i64,
    lookback_ns: u64,
) -> PrecedingEventSelectorConfigV0 {
    PrecedingEventSelectorConfigV0::new(
        PRECEDING_EVENT_SELECTOR_VERSION_V0,
        retention(horizon_ns),
        schema(schema_id),
        key,
        anchor_event_time_ns,
        lookback_ns,
    )
    .unwrap()
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

fn assert_usage_conserved(view: &PrecedingEventSelectorViewV0<'_>) {
    let retention = view.retention_usage();
    let usage = view.usage();
    assert_eq!(
        retention.accepted_events_considered(),
        retention.retained_events() + retention.expired_events_excluded()
    );
    assert_eq!(
        retention.retained_events(),
        usage.retained_events_considered()
    );
    assert_eq!(
        usage.retained_events_considered(),
        usage.other_schema_events_omitted() + usage.matching_schema_events()
    );
    assert_eq!(
        usage.matching_schema_events(),
        usage.unkeyed_matching_events_omitted()
            + usage.different_key_matching_events_omitted()
            + usage.same_key_events()
    );
    assert_eq!(
        usage.same_key_events(),
        usage.before_requested_interval_events_omitted()
            + usage.at_or_after_anchor_events_omitted()
            + usage.eligible_candidate_events()
    );
}

#[test]
fn config_enforces_version_key_and_lookback_boundaries() {
    let minimum = config(1, "schema-v0", b"", i64::MIN, 1);
    let maximum_key = vec![b'k'; MAX_KEY_BYTES];
    let maximum = config(u64::MAX, "schema-v0", &maximum_key, i64::MAX, u64::MAX);
    assert_eq!(minimum.version(), PRECEDING_EVENT_SELECTOR_VERSION_V0);
    assert_eq!(minimum.key(), b"");
    assert_eq!(minimum.anchor_event_time_ns(), i64::MIN);
    assert_eq!(minimum.lookback_ns(), 1);
    assert_eq!(maximum.key(), maximum_key);
    assert_eq!(maximum.lookback_ns(), u64::MAX);
    assert_eq!(maximum.retention().retention_horizon_ns(), u64::MAX);
    assert_eq!(maximum.candidate_schema_id(), &schema("schema-v0"));
    assert_eq!(
        PrecedingEventSelectorConfigV0::new(1, retention(1), schema("schema-v0"), b"key", 0, 1,),
        Err(PrecedingEventSelectorConfigErrorV0::UnsupportedVersion { actual: 1 })
    );
    let one_past = vec![b'k'; MAX_KEY_BYTES + 1];
    assert_eq!(
        PrecedingEventSelectorConfigV0::new(
            PRECEDING_EVENT_SELECTOR_VERSION_V0,
            retention(1),
            schema("schema-v0"),
            &one_past,
            0,
            1,
        ),
        Err(PrecedingEventSelectorConfigErrorV0::KeyTooLong {
            maximum: MAX_KEY_BYTES,
            actual: MAX_KEY_BYTES + 1,
        })
    );
    assert_eq!(
        PrecedingEventSelectorConfigV0::new(
            PRECEDING_EVENT_SELECTOR_VERSION_V0,
            retention(1),
            schema("schema-v0"),
            b"key",
            0,
            0,
        ),
        Err(PrecedingEventSelectorConfigErrorV0::ZeroLookback)
    );
}

#[test]
fn empty_admission_and_anchor_epoch_boundaries_are_exact() {
    let state = admission();
    let empty = state
        .select_preceding_event(config(
            u64::MAX,
            "schema-v0",
            b"key",
            50 * SECOND_NS,
            u64::MAX,
        ))
        .unwrap();
    assert_eq!(empty.requested_start_event_time_ns(), 0);
    assert_eq!(empty.anchor_event_time_ns(), 50 * SECOND_NS);
    assert_eq!(empty.retention_cutoff_event_time_ns(), None);
    assert_eq!(empty.source_watermark_ns(), None);
    assert!(!empty.is_source_finalized());
    assert_eq!(empty.usage().retained_events_considered(), 0);
    assert_eq!(empty.usage().other_schema_events_omitted(), 0);
    assert_eq!(empty.usage().matching_schema_events(), 0);
    assert_eq!(empty.usage().unkeyed_matching_events_omitted(), 0);
    assert_eq!(empty.usage().different_key_matching_events_omitted(), 0);
    assert_eq!(empty.usage().same_key_events(), 0);
    assert_eq!(empty.usage().before_requested_interval_events_omitted(), 0);
    assert_eq!(empty.usage().at_or_after_anchor_events_omitted(), 0);
    assert_eq!(empty.usage().eligible_candidate_events(), 0);
    assert!(empty.candidate().is_none());
    assert_usage_conserved(&empty);

    assert!(
        state
            .select_preceding_event(config(u64::MAX, "schema-v0", b"key", 0, 1))
            .is_ok()
    );
    assert!(
        state
            .select_preceding_event(config(u64::MAX, "schema-v0", b"key", 200 * SECOND_NS, 1,))
            .is_ok()
    );
    assert_eq!(
        state
            .select_preceding_event(config(u64::MAX, "schema-v0", b"key", -1, 1))
            .unwrap_err(),
        PrecedingEventSelectorErrorV0::AnchorOutsideEpoch
    );
    assert_eq!(
        state
            .select_preceding_event(config(
                u64::MAX,
                "schema-v0",
                b"key",
                200 * SECOND_NS + 1,
                1,
            ))
            .unwrap_err(),
        PrecedingEventSelectorErrorV0::AnchorOutsideEpoch
    );
}

#[test]
fn signed_extremes_clamp_requested_start_without_ambiguity() {
    let epoch_start = i64::MIN + 7;
    let epoch_end = i64::MAX - 7;
    let state = admission_with(epoch_start, epoch_end, AdmissionLimitsV0::maximum());
    let at_start = state
        .select_preceding_event(config(u64::MAX, "schema-v0", b"key", epoch_start, u64::MAX))
        .unwrap();
    assert_eq!(at_start.requested_start_event_time_ns(), epoch_start);
    let at_end = state
        .select_preceding_event(config(u64::MAX, "schema-v0", b"key", epoch_end, 1))
        .unwrap();
    assert_eq!(at_end.requested_start_event_time_ns(), epoch_end - 1);
}

#[test]
fn interval_schema_key_and_omission_accounting_are_exact() {
    let mut state = admission();
    for candidate in [
        event(
            "source-v0",
            1,
            25 * SECOND_NS,
            "other-v0",
            Some(b"key"),
            0,
            1,
        ),
        event("source-v0", 2, 25 * SECOND_NS, "schema-v0", None, 0, 2),
        event(
            "source-v0",
            3,
            25 * SECOND_NS,
            "schema-v0",
            Some(b"other"),
            0,
            3,
        ),
        event(
            "source-v0",
            4,
            19 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            4,
        ),
        event(
            "source-v0",
            5,
            20 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            5,
        ),
        event(
            "source-v0",
            6,
            40 * SECOND_NS - 1,
            "schema-v0",
            Some(b"key"),
            0,
            6,
        ),
        event(
            "source-v0",
            7,
            40 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            7,
        ),
        event(
            "source-v0",
            8,
            40 * SECOND_NS + 1,
            "schema-v0",
            Some(b"key"),
            0,
            8,
        ),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state
        .select_preceding_event(config(
            u64::MAX,
            "schema-v0",
            b"key",
            40 * SECOND_NS,
            20 * SECOND_NS as u64,
        ))
        .unwrap();
    assert_eq!(view.requested_start_event_time_ns(), 20 * SECOND_NS);
    assert_eq!(sequence(view.candidate().unwrap()), 6);
    assert_eq!(view.usage().retained_events_considered(), 8);
    assert_eq!(view.usage().other_schema_events_omitted(), 1);
    assert_eq!(view.usage().matching_schema_events(), 7);
    assert_eq!(view.usage().unkeyed_matching_events_omitted(), 1);
    assert_eq!(view.usage().different_key_matching_events_omitted(), 1);
    assert_eq!(view.usage().same_key_events(), 5);
    assert_eq!(view.usage().before_requested_interval_events_omitted(), 1);
    assert_eq!(view.usage().at_or_after_anchor_events_omitted(), 2);
    assert_eq!(view.usage().eligible_candidate_events(), 2);
    assert_usage_conserved(&view);
}

#[test]
fn empty_key_is_distinct_from_unkeyed_and_payload_is_uninterpreted() {
    let mut state = admission();
    let unkeyed = event("source-v0", 1, 20 * SECOND_NS, "schema-v0", None, 0, 1);
    let empty_key = event("source-v0", 2, 21 * SECOND_NS, "schema-v0", Some(b""), 9, 2);
    accept(&mut state, &unkeyed);
    accept(&mut state, &empty_key);
    let view = state
        .select_preceding_event(config(
            u64::MAX,
            "schema-v0",
            b"",
            30 * SECOND_NS,
            20 * SECOND_NS as u64,
        ))
        .unwrap();
    assert_eq!(sequence(view.candidate().unwrap()), 2);
    assert_eq!(view.usage().unkeyed_matching_events_omitted(), 1);
    assert_eq!(view.usage().same_key_events(), 1);
    assert_usage_conserved(&view);
}

type Evidence = (
    i64,
    Option<i64>,
    PrecedingEventSelectorUsageV0,
    Option<u64>,
    Option<i64>,
    bool,
);

fn evidence_for_order(order: [usize; 4]) -> Evidence {
    let events = [
        event(
            "source-v0",
            1,
            60 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            1,
        ),
        event(
            "source-v0",
            2,
            70 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            2,
        ),
        event(
            "source-v0",
            3,
            65 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            3,
        ),
        event(
            "source-v0",
            4,
            70 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            4,
        ),
    ];
    let mut state = admission();
    for index in order {
        accept(&mut state, &events[index]);
    }
    let view = state
        .select_preceding_event(config(
            u64::MAX,
            "schema-v0",
            b"key",
            90 * SECOND_NS,
            40 * SECOND_NS as u64,
        ))
        .unwrap();
    (
        view.requested_start_event_time_ns(),
        view.retention_cutoff_event_time_ns(),
        view.usage(),
        view.candidate().map(sequence),
        view.source_watermark_ns(),
        view.is_source_finalized(),
    )
}

#[test]
fn every_permutation_and_fresh_replay_converge_to_total_rank() {
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
    assert_eq!(expected.3, Some(4));
    assert_eq!(evidence_for_order([0, 1, 2, 3]), expected);
}

#[test]
fn exact_cutoff_preserves_interval_and_one_past_fails_closed() {
    let mut state = admission();
    for candidate in [
        event(
            "source-v0",
            1,
            40 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            1,
        ),
        event(
            "source-v0",
            2,
            79 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            2,
        ),
        event(
            "source-v0",
            3,
            100 * SECOND_NS,
            "other-v0",
            Some(b"clock"),
            0,
            3,
        ),
    ] {
        accept(&mut state, &candidate);
    }
    let exact = state
        .select_preceding_event(config(
            30 * SECOND_NS as u64,
            "schema-v0",
            b"key",
            80 * SECOND_NS,
            40 * SECOND_NS as u64,
        ))
        .unwrap();
    assert_eq!(exact.retention_cutoff_event_time_ns(), Some(40 * SECOND_NS));
    assert_eq!(sequence(exact.candidate().unwrap()), 2);
    assert_usage_conserved(&exact);
    drop(exact);

    assert_eq!(
        state
            .select_preceding_event(config(
                30 * SECOND_NS as u64 - 1,
                "schema-v0",
                b"key",
                80 * SECOND_NS,
                40 * SECOND_NS as u64,
            ))
            .unwrap_err(),
        PrecedingEventSelectorErrorV0::RequestedEvidenceExpired
    );
}

fn assert_selected_sequence(state: &AdmissionStateV0, expected: u64) {
    let view = state
        .select_preceding_event(config(
            u64::MAX,
            "schema-v0",
            b"key",
            120 * SECOND_NS,
            100 * SECOND_NS as u64,
        ))
        .unwrap();
    assert_eq!(view.candidate().map(sequence), Some(expected));
    assert_usage_conserved(&view);
}

#[test]
fn duplicates_and_all_semantic_rejections_cannot_become_candidate() {
    let mut state = admission();
    let selected = event(
        "source-v0",
        1,
        50 * SECOND_NS,
        "schema-v0",
        Some(b"key"),
        1,
        1,
    );
    assert_eq!(
        state.apply_event(&event(
            "wrong-v0",
            90,
            60 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            90,
        )),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::UnexpectedSource
        ))
    );
    assert_eq!(
        state.apply_event(&event(
            "source-v0",
            91,
            -1,
            "schema-v0",
            Some(b"key"),
            0,
            91,
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch))
    );
    accept(&mut state, &selected);
    assert_eq!(
        state.apply_event(&selected),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    assert_eq!(
        state.apply_event(&event(
            "source-v0",
            1,
            51 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            99,
        )),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::ConflictingRedelivery
        ))
    );
    accept(
        &mut state,
        &event(
            "source-v0",
            2,
            100 * SECOND_NS,
            "other-v0",
            Some(b"clock"),
            0,
            2,
        ),
    );
    assert_eq!(
        state.apply_event(&event(
            "source-v0",
            3,
            70 * SECOND_NS - 1,
            "schema-v0",
            Some(b"key"),
            0,
            3,
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::Late))
    );
    assert_selected_sequence(&state, 1);
    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    assert_eq!(
        state.apply_event(&event(
            "source-v0",
            4,
            80 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            4,
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::EpochFinalized))
    );
    assert_selected_sequence(&state, 1);

    let first = event(
        "source-v0",
        1,
        50 * SECOND_NS,
        "schema-v0",
        Some(b"key"),
        0,
        1,
    );
    let second = event(
        "source-v0",
        2,
        60 * SECOND_NS,
        "schema-v0",
        Some(b"key"),
        0,
        2,
    );
    let mut identity_capped = admission_with(
        0,
        200 * SECOND_NS,
        AdmissionLimitsV0::new(1, decant_state::MAX_ACCEPTED_EVENT_BYTES_V0).unwrap(),
    );
    accept(&mut identity_capped, &first);
    assert_eq!(
        identity_capped.apply_event(&second),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedIdentityCapacity
        ))
    );
    assert_selected_sequence(&identity_capped, 1);

    let first_bytes = accepted_event_accounted_bytes_v0(&first).unwrap();
    let mut byte_capped = admission_with(
        0,
        200 * SECOND_NS,
        AdmissionLimitsV0::new(2, first_bytes).unwrap(),
    );
    accept(&mut byte_capped, &first);
    assert_eq!(
        byte_capped.apply_event(&second),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedEventByteCapacity
        ))
    );
    assert_selected_sequence(&byte_capped, 1);
}

#[test]
fn candidate_is_exact_admission_evidence_and_finalization_can_expire_request() {
    let mut state = admission();
    let original = event(
        "source-v0",
        1,
        50 * SECOND_NS,
        "schema-v0",
        Some(b"key"),
        0x55,
        7,
    );
    accept(&mut state, &original);
    accept(
        &mut state,
        &event(
            "source-v0",
            2,
            100 * SECOND_NS,
            "other-v0",
            Some(b"clock"),
            0,
            2,
        ),
    );
    let admitted = state
        .accepted_events()
        .find(|candidate| sequence(candidate) == 1)
        .unwrap();
    let selector_config = config(
        70 * SECOND_NS as u64,
        "schema-v0",
        b"key",
        80 * SECOND_NS,
        40 * SECOND_NS as u64,
    );
    {
        let before = state
            .select_preceding_event(selector_config.clone())
            .unwrap();
        let candidate = before.candidate().unwrap();
        assert!(std::ptr::eq(candidate, admitted));
        assert_eq!(candidate.provenance(), original.provenance());
        assert_eq!(candidate.sensitivity(), original.sensitivity());
        assert_usage_conserved(&before);
    }
    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    assert_eq!(
        state.select_preceding_event(selector_config).unwrap_err(),
        PrecedingEventSelectorErrorV0::RequestedEvidenceExpired
    );
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

fn selector_for_scenario(key: &[u8]) -> PrecedingEventSelectorConfigV0 {
    config(
        (PRIMARY_END_NS - PRIMARY_START_NS) as u64,
        "deployment-v0",
        key,
        PRIMARY_END_NS,
        (15 * MINUTE_NS) as u64,
    )
}

fn independent_candidate<'a>(state: &'a AdmissionStateV0, key: &[u8]) -> &'a EventEnvelopeV0 {
    let start = PRIMARY_END_NS - 15 * MINUTE_NS;
    state
        .accepted_events()
        .filter(|candidate| {
            candidate.schema_id() == &schema("deployment-v0")
                && candidate.key() == Some(key)
                && candidate.event_time_ns() >= start
                && candidate.event_time_ns() < PRIMARY_END_NS
        })
        .max_by_key(|candidate| (candidate.event_time_ns(), sequence(candidate)))
        .unwrap()
}

fn metric_change_config() -> ProjectedFieldChangeConfigV0 {
    ProjectedFieldChangeConfigV0::new(
        PROJECTED_FIELD_CHANGE_VERSION_V0,
        retention((PRIMARY_END_NS - PRIMARY_START_NS) as u64),
        schema("metric-sample-v0"),
        "metric_name",
        "metric_value",
        ProjectedFieldChangeLimitsV0::new(
            decant_state::MAX_PROJECTED_FIELD_CHANGE_ENTRIES_V0,
            MAX_PROJECTED_FIELD_CHANGE_INDEX_BYTES_V0,
        )
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn all_sre_scenarios_select_exact_temporal_marker_without_causal_inference() {
    for (scenario_id, key, expected_deployment_id, expected_changes) in [
        (
            "sre-v0-primary",
            b"payments-v2/region-b".as_slice(),
            "payments-v2-region-b-09",
            2,
        ),
        (
            "sre-v0-region-swap",
            b"payments-v2/region-a".as_slice(),
            "payments-v2-region-a-09",
            2,
        ),
        (
            "sre-v0-service-swap",
            b"checkout-v1/region-b".as_slice(),
            "checkout-v1-region-b-09",
            2,
        ),
        (
            "sre-v0-no-regression",
            b"payments-v2/region-b".as_slice(),
            "payments-v2-region-b-09",
            0,
        ),
    ] {
        let state = scenario_state(scenario_id);
        let view = state
            .select_preceding_event(selector_for_scenario(key))
            .unwrap();
        assert_eq!(
            view.requested_start_event_time_ns(),
            PRIMARY_END_NS - 15 * MINUTE_NS
        );
        assert_eq!(
            view.retention_cutoff_event_time_ns(),
            Some(PRIMARY_START_NS)
        );
        assert_eq!(view.retention_usage().accepted_events_considered(), 98_750);
        assert_eq!(view.retention_usage().retained_events(), 98_750);
        assert_eq!(view.retention_usage().expired_events_excluded(), 0);
        assert_eq!(view.usage().retained_events_considered(), 98_750);
        assert_eq!(view.usage().other_schema_events_omitted(), 98_730);
        assert_eq!(view.usage().matching_schema_events(), 20);
        assert_eq!(view.usage().unkeyed_matching_events_omitted(), 0);
        assert_eq!(view.usage().different_key_matching_events_omitted(), 19);
        assert_eq!(view.usage().same_key_events(), 1);
        assert_eq!(view.usage().before_requested_interval_events_omitted(), 0);
        assert_eq!(view.usage().at_or_after_anchor_events_omitted(), 0);
        assert_eq!(view.usage().eligible_candidate_events(), 1);
        let candidate = view.candidate().unwrap();
        assert!(std::ptr::eq(candidate, independent_candidate(&state, key)));
        assert_eq!(candidate.event_time_ns(), PRIMARY_START_NS + 9 * MINUTE_NS);
        assert_eq!(
            field(candidate, "deployment_id").as_utf8(),
            Some(expected_deployment_id)
        );
        assert_eq!(field(candidate, "causal_claim").as_bool(), Some(false));
        assert_eq!(
            field(candidate, "notable_temporal_evidence").as_bool(),
            Some(true)
        );
        assert!(view.is_source_finalized());
        assert_usage_conserved(&view);

        let changes = state
            .materialize_projected_field_changes(metric_change_config())
            .unwrap();
        assert_eq!(changes.usage().changed_series(), expected_changes);
        if expected_changes == 0 {
            assert!(changes.entries().next().is_none());
            assert!(view.candidate().is_some());
        }
    }
}

#[test]
fn short_retention_fails_instead_of_silently_shortening_sre_reachback() {
    let state = scenario_state("sre-v0-primary");
    assert_eq!(
        state
            .select_preceding_event(config(
                (10 * MINUTE_NS) as u64,
                "deployment-v0",
                b"payments-v2/region-b",
                PRIMARY_END_NS,
                (15 * MINUTE_NS) as u64,
            ))
            .unwrap_err(),
        PrecedingEventSelectorErrorV0::RequestedEvidenceExpired
    );
}

struct DiagnosticInput<'a> {
    source_id: &'a str,
    schema_id: &'a str,
    key: &'a [u8],
    sequences: (u64, u64),
    times: (i64, i64),
    values: (u64, u64),
}

fn diagnostic(input: DiagnosticInput<'_>) -> String {
    let mut state = AdmissionStateV0::new(
        AdmissionConfigV0::new(
            ADMISSION_VERSION_V0,
            source(input.source_id),
            0,
            200 * SECOND_NS,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    );
    for (sequence, time, value) in [
        (input.sequences.0, input.times.0, input.values.0),
        (input.sequences.1, input.times.1, input.values.1),
    ] {
        accept(
            &mut state,
            &event(
                input.source_id,
                sequence,
                time,
                input.schema_id,
                Some(input.key),
                u64::MAX,
                value,
            ),
        );
    }
    let selector_config = PrecedingEventSelectorConfigV0::new(
        PRECEDING_EVENT_SELECTOR_VERSION_V0,
        retention(100 * SECOND_NS as u64),
        schema(input.schema_id),
        input.key,
        80 * SECOND_NS,
        60 * SECOND_NS as u64,
    )
    .unwrap();
    let view = state
        .select_preceding_event(selector_config.clone())
        .unwrap();
    format!(
        "{selector_config:?}|{view:?}|{:?}|{:?}",
        view.usage(),
        PrecedingEventSelectorConfigErrorV0::ZeroLookback,
    )
}

#[test]
fn diagnostics_are_content_independent_and_value_safe() {
    let first = diagnostic(DiagnosticInput {
        source_id: "source-aa",
        schema_id: "schema-aa",
        key: b"secret-aa",
        sequences: (301, 302),
        times: (40 * SECOND_NS, 41 * SECOND_NS),
        values: (111, 112),
    });
    let second = diagnostic(DiagnosticInput {
        source_id: "source-bb",
        schema_id: "schema-bb",
        key: b"secret-bb",
        sequences: (701, 702),
        times: (50 * SECOND_NS, 51 * SECOND_NS),
        values: (998, 999),
    });
    assert_eq!(first, second);
    let outside_first = format!(
        "{:?}|{}",
        PrecedingEventSelectorErrorV0::AnchorOutsideEpoch,
        PrecedingEventSelectorErrorV0::AnchorOutsideEpoch
    );
    let outside_second = outside_first.clone();
    assert_eq!(outside_first, outside_second);
    for forbidden in [
        "source-aa",
        "source-bb",
        "schema-aa",
        "schema-bb",
        "secret-aa",
        "secret-bb",
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
        assert!(!outside_first.contains(forbidden));
        assert!(!outside_second.contains(forbidden));
    }
}
