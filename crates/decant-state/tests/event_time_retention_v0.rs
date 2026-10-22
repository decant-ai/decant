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
    EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigErrorV0, EventTimeRetentionConfigV0,
    EventTimeRetentionUsageV0, EventTimeRetentionViewV0, FinalizeOutcomeV0, RejectionReasonV0,
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
    key: &[u8],
    sensitivity: u64,
    value: &str,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(source_id),
        schema(schema_id),
        SourcePositionV0::Synthetic { sequence },
        Some(key),
        times_ns.0,
        times_ns.1,
        SensitivityBitmapV0::from_bits(sensitivity),
        payload(value),
    )
    .unwrap()
}

fn basic_event(sequence: u64, event_time_ns: i64) -> EventEnvelopeV0 {
    event(
        "source-v0",
        sequence,
        (event_time_ns, event_time_ns),
        "schema-v0",
        b"key-v0",
        0,
        "value-v0",
    )
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

fn retention_config(retention_horizon_ns: u64) -> EventTimeRetentionConfigV0 {
    EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, retention_horizon_ns).unwrap()
}

fn accept(state: &mut AdmissionStateV0, candidate: &EventEnvelopeV0) {
    assert_eq!(state.apply_event(candidate), Ok(ApplyOutcomeV0::Accepted));
}

fn sequence(event: &EventEnvelopeV0) -> u64 {
    event.source_position().synthetic_sequence()
}

fn position(sequence: u64) -> SourcePositionV0 {
    SourcePositionV0::Synthetic { sequence }
}

fn sequences(view: &EventTimeRetentionViewV0<'_>) -> Vec<u64> {
    view.events().map(sequence).collect()
}

fn assert_usage_conserved(usage: EventTimeRetentionUsageV0) {
    assert_eq!(
        usage.accepted_events_considered(),
        usage.retained_events() + usage.expired_events_excluded()
    );
}

#[test]
fn config_enforces_version_and_horizon_boundaries() {
    let minimum = retention_config(1);
    let maximum = retention_config(u64::MAX);
    assert_eq!(minimum.version(), EVENT_TIME_RETENTION_VERSION_V0);
    assert_eq!(minimum.retention_horizon_ns(), 1);
    assert_eq!(maximum.retention_horizon_ns(), u64::MAX);
    assert_eq!(
        EventTimeRetentionConfigV0::new(1, 1),
        Err(EventTimeRetentionConfigErrorV0::UnsupportedVersion { actual: 1 })
    );
    assert_eq!(
        EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, 0),
        Err(EventTimeRetentionConfigErrorV0::ZeroRetentionHorizon)
    );
}

#[test]
fn empty_admission_has_no_cutoff_or_hidden_usage() {
    let state = admission();
    let view = state.project_event_time_retention(retention_config(20 * SECOND_NS as u64));
    assert_eq!(view.config().retention_horizon_ns(), 20 * SECOND_NS as u64);
    assert_eq!(view.cutoff_event_time_ns(), None);
    assert_eq!(view.source_watermark_ns(), None);
    assert!(!view.is_source_finalized());
    assert_eq!(view.usage().accepted_events_considered(), 0);
    assert_eq!(view.usage().retained_events(), 0);
    assert_eq!(view.usage().expired_events_excluded(), 0);
    assert_usage_conserved(view.usage());
    assert_eq!(sequences(&view), Vec::<u64>::new());
    assert!(view.get(position(0)).is_none());
    assert!(
        view.get(SourcePositionV0::otlp_file(1, 0, 0, 0).unwrap())
            .is_none()
    );
}

#[test]
fn horizons_before_the_epoch_clamp_to_start_at_signed_extremes() {
    let minimum_start = i64::MIN + 7;
    let minimum_end = minimum_start + 100 * SECOND_NS;
    let mut minimum_state =
        admission_with(minimum_start, minimum_end, AdmissionLimitsV0::maximum());
    accept(&mut minimum_state, &basic_event_for_epoch(1, minimum_start));
    let minimum_view = minimum_state.project_event_time_retention(retention_config(u64::MAX));
    assert_eq!(minimum_view.source_watermark_ns(), Some(i64::MIN));
    assert_eq!(minimum_view.cutoff_event_time_ns(), Some(minimum_start));
    assert_eq!(sequences(&minimum_view), vec![1]);

    let maximum_start = i64::MAX - 100 * SECOND_NS;
    let mut maximum_state = admission_with(maximum_start, i64::MAX, AdmissionLimitsV0::maximum());
    accept(&mut maximum_state, &basic_event_for_epoch(2, i64::MAX - 1));
    assert_eq!(
        maximum_state.finalize_source(&source("source-v0"), i64::MAX),
        FinalizeOutcomeV0::Finalized
    );
    let maximum_view = maximum_state.project_event_time_retention(retention_config(1));
    assert_eq!(maximum_view.source_watermark_ns(), Some(i64::MAX));
    assert_eq!(maximum_view.cutoff_event_time_ns(), Some(i64::MAX - 1));
    assert_eq!(sequences(&maximum_view), vec![2]);

    let mut crossing_state = admission_with(
        -100 * SECOND_NS,
        100 * SECOND_NS,
        AdmissionLimitsV0::maximum(),
    );
    accept(
        &mut crossing_state,
        &basic_event_for_epoch(3, 60 * SECOND_NS),
    );
    let crossing_view =
        crossing_state.project_event_time_retention(retention_config(50 * SECOND_NS as u64));
    assert_eq!(crossing_view.source_watermark_ns(), Some(30 * SECOND_NS));
    assert_eq!(crossing_view.cutoff_event_time_ns(), Some(-20 * SECOND_NS));
}

fn basic_event_for_epoch(sequence: u64, event_time_ns: i64) -> EventEnvelopeV0 {
    event(
        "source-v0",
        sequence,
        (event_time_ns, event_time_ns),
        "schema-v0",
        b"key-v0",
        0,
        "value-v0",
    )
}

#[test]
fn exact_cutoff_is_inclusive_and_future_of_watermark_is_retained() {
    let mut state = admission();
    for (sequence, event_time_ns) in [
        (1, 39 * SECOND_NS),
        (2, 40 * SECOND_NS),
        (3, 40 * SECOND_NS + 1),
        (4, 90 * SECOND_NS),
    ] {
        accept(&mut state, &basic_event(sequence, event_time_ns));
    }

    let view = state.project_event_time_retention(retention_config(20 * SECOND_NS as u64));
    assert_eq!(view.source_watermark_ns(), Some(60 * SECOND_NS));
    assert_eq!(view.cutoff_event_time_ns(), Some(40 * SECOND_NS));
    assert_eq!(sequences(&view), vec![2, 3, 4]);
    assert_eq!(view.usage().accepted_events_considered(), 4);
    assert_eq!(view.usage().retained_events(), 3);
    assert_eq!(view.usage().expired_events_excluded(), 1);
    assert_usage_conserved(view.usage());
    assert!(view.get(position(1)).is_none());
    assert!(view.get(position(2)).is_some());
    assert!(view.get(position(4)).is_some());
    assert!(view.get(position(999)).is_none());
}

#[test]
fn iteration_is_canonical_source_sequence_not_event_time_or_arrival_order() {
    let mut state = admission();
    for candidate in [
        basic_event(30, 40 * SECOND_NS),
        basic_event(10, 60 * SECOND_NS),
        basic_event(20, 50 * SECOND_NS),
    ] {
        accept(&mut state, &candidate);
    }
    let view = state.project_event_time_retention(retention_config(100 * SECOND_NS as u64));
    assert_eq!(sequences(&view), vec![10, 20, 30]);
    assert_eq!(
        view.events()
            .map(EventEnvelopeV0::event_time_ns)
            .collect::<Vec<_>>(),
        vec![60 * SECOND_NS, 50 * SECOND_NS, 40 * SECOND_NS]
    );
}

#[test]
fn retained_lookup_is_exact_normalized_admission_evidence() {
    let mut state = admission();
    let candidate = event(
        "source-v0",
        7,
        (40 * SECOND_NS, 77 * SECOND_NS),
        "schema-v0",
        b"key-v0",
        9,
        "value-v0",
    );
    accept(&mut state, &candidate);
    let admitted = state.accepted_events().next().unwrap();
    assert_eq!(admitted.ingest_time_ns(), 0);
    assert_eq!(admitted.provenance(), candidate.provenance());

    let view = state.project_event_time_retention(retention_config(20 * SECOND_NS as u64));
    let projected = view.get(position(7)).unwrap();
    assert!(std::ptr::eq(admitted, projected));
    assert_eq!(projected.provenance(), candidate.provenance());
    assert_eq!(projected.sensitivity(), candidate.sensitivity());
    assert_eq!(projected.ingest_time_ns(), 0);
}

#[test]
fn duplicates_and_every_rejection_path_cannot_enter_the_projection() {
    let mut state = admission();
    let original = basic_event(1, 10 * SECOND_NS);
    accept(&mut state, &original);
    let ingest_only_redelivery = event(
        "source-v0",
        1,
        (10 * SECOND_NS, 99 * SECOND_NS),
        "schema-v0",
        b"key-v0",
        0,
        "value-v0",
    );
    assert_eq!(
        state.apply_event(&ingest_only_redelivery),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    let conflict = event(
        "source-v0",
        1,
        (10 * SECOND_NS, 0),
        "schema-v0",
        b"key-v0",
        0,
        "changed",
    );
    assert_eq!(
        state.apply_event(&conflict),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::ConflictingRedelivery
        ))
    );
    assert_eq!(
        state.apply_event(&event(
            "other-v0",
            2,
            (20 * SECOND_NS, 0),
            "schema-v0",
            b"key-v0",
            0,
            "value-v0",
        )),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::UnexpectedSource
        ))
    );
    assert_eq!(
        state.apply_event(&basic_event(3, 200 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch))
    );
    accept(&mut state, &basic_event(4, 100 * SECOND_NS));
    assert_eq!(
        state.apply_event(&basic_event(5, 70 * SECOND_NS - 1)),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::Late))
    );
    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    assert_eq!(
        state.apply_event(&basic_event(6, 100 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::EpochFinalized))
    );

    let view = state.project_event_time_retention(retention_config(200 * SECOND_NS as u64));
    assert_eq!(view.usage().accepted_events_considered(), 2);
    assert_eq!(sequences(&view), vec![1, 4]);

    let mut capacity_state = admission_with(
        0,
        200 * SECOND_NS,
        AdmissionLimitsV0::new(1, 128 * 1_024 * 1_024).unwrap(),
    );
    accept(&mut capacity_state, &basic_event(10, 10 * SECOND_NS));
    assert_eq!(
        capacity_state.apply_event(&basic_event(11, 20 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedIdentityCapacity
        ))
    );
    let capacity_view =
        capacity_state.project_event_time_retention(retention_config(20 * SECOND_NS as u64));
    assert_eq!(capacity_view.usage().accepted_events_considered(), 1);
    assert_eq!(sequences(&capacity_view), vec![10]);

    let mut byte_capacity_state =
        admission_with(0, 200 * SECOND_NS, AdmissionLimitsV0::new(1, 1).unwrap());
    assert_eq!(
        byte_capacity_state.apply_event(&basic_event(12, 10 * SECOND_NS)),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedEventByteCapacity
        ))
    );
    let byte_capacity_view =
        byte_capacity_state.project_event_time_retention(retention_config(20 * SECOND_NS as u64));
    assert_eq!(byte_capacity_view.usage().accepted_events_considered(), 0);
    assert_eq!(sequences(&byte_capacity_view), Vec::<u64>::new());
}

fn projection_for_order(
    order: &[usize],
) -> (
    Option<i64>,
    Option<i64>,
    Vec<u64>,
    EventTimeRetentionUsageV0,
    bool,
) {
    let candidates = [
        basic_event(30, 39 * SECOND_NS),
        basic_event(10, 40 * SECOND_NS),
        basic_event(20, 90 * SECOND_NS),
    ];
    let mut state = admission();
    for &index in order {
        accept(&mut state, &candidates[index]);
    }
    let view = state.project_event_time_retention(retention_config(20 * SECOND_NS as u64));
    (
        view.cutoff_event_time_ns(),
        view.source_watermark_ns(),
        sequences(&view),
        view.usage(),
        view.is_source_finalized(),
    )
}

#[test]
fn permitted_arrival_disorder_converges() {
    let forward = projection_for_order(&[0, 1, 2]);
    let reordered = projection_for_order(&[1, 0, 2]);
    assert_eq!(forward, reordered);
    assert_eq!(forward.0, Some(40 * SECOND_NS));
    assert_eq!(forward.1, Some(60 * SECOND_NS));
    assert_eq!(forward.2, vec![10, 20]);
    assert!(!forward.4);
}

fn finalized_replay() -> (Option<i64>, Vec<u64>, EventTimeRetentionUsageV0, bool) {
    let mut state = admission();
    for candidate in [
        basic_event(1, 40 * SECOND_NS),
        basic_event(2, 90 * SECOND_NS),
    ] {
        accept(&mut state, &candidate);
    }
    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    let view = state.project_event_time_retention(retention_config(100 * SECOND_NS as u64));
    (
        view.cutoff_event_time_ns(),
        sequences(&view),
        view.usage(),
        view.is_source_finalized(),
    )
}

#[test]
fn fresh_event_and_control_replays_are_identical() {
    assert_eq!(finalized_replay(), finalized_replay());
}

#[test]
fn finalization_advances_cutoff_only_when_a_new_view_is_built() {
    let mut state = admission();
    accept(&mut state, &basic_event(1, 40 * SECOND_NS));
    accept(&mut state, &basic_event(2, 90 * SECOND_NS));
    {
        let before = state.project_event_time_retention(retention_config(100 * SECOND_NS as u64));
        assert_eq!(before.source_watermark_ns(), Some(60 * SECOND_NS));
        assert_eq!(before.cutoff_event_time_ns(), Some(0));
        assert_eq!(sequences(&before), vec![1, 2]);
        assert!(!before.is_source_finalized());
    }

    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    let after = state.project_event_time_retention(retention_config(100 * SECOND_NS as u64));
    assert_eq!(after.source_watermark_ns(), Some(200 * SECOND_NS));
    assert_eq!(after.cutoff_event_time_ns(), Some(100 * SECOND_NS));
    assert_eq!(sequences(&after), Vec::<u64>::new());
    assert_eq!(after.usage().accepted_events_considered(), 2);
    assert_eq!(after.usage().retained_events(), 0);
    assert_eq!(after.usage().expired_events_excluded(), 2);
    assert!(after.is_source_finalized());
}

#[test]
fn usage_and_membership_match_an_independent_batch_predicate() {
    let mut state = admission();
    for (sequence, event_time_ns) in [
        (8, 39 * SECOND_NS),
        (2, 40 * SECOND_NS),
        (9, 41 * SECOND_NS),
        (1, 90 * SECOND_NS),
    ] {
        accept(&mut state, &basic_event(sequence, event_time_ns));
    }
    let horizon = 20 * SECOND_NS as u64;
    let watermark = state.watermark_ns().unwrap();
    let independently_computed_cutoff = i64::try_from(i128::from(watermark) - i128::from(horizon))
        .unwrap_or(i64::MIN)
        .max(state.config().epoch_start_ns());
    let expected = state
        .accepted_events()
        .filter(|candidate| candidate.event_time_ns() >= independently_computed_cutoff)
        .map(sequence)
        .collect::<Vec<_>>();

    let view = state.project_event_time_retention(retention_config(horizon));
    assert_eq!(
        view.cutoff_event_time_ns(),
        Some(independently_computed_cutoff)
    );
    assert_eq!(sequences(&view), expected);
    assert_eq!(view.usage().retained_events(), expected.len());
    assert_usage_conserved(view.usage());
}

#[test]
fn primary_twenty_minute_epoch_retains_every_accepted_event() {
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
    assert_eq!(state.usage().accepted_identities(), 98_750);

    let horizon = (PRIMARY_END_NS - PRIMARY_START_NS) as u64;
    let independently_retained = state
        .accepted_events()
        .filter(|candidate| candidate.event_time_ns() >= PRIMARY_START_NS)
        .map(sequence)
        .collect::<Vec<_>>();
    let view = state.project_event_time_retention(retention_config(horizon));
    assert_eq!(view.source_watermark_ns(), Some(PRIMARY_END_NS));
    assert_eq!(view.cutoff_event_time_ns(), Some(PRIMARY_START_NS));
    assert_eq!(view.usage().accepted_events_considered(), 98_750);
    assert_eq!(view.usage().retained_events(), 98_750);
    assert_eq!(view.usage().expired_events_excluded(), 0);
    assert_eq!(sequences(&view), independently_retained);
    assert!(view.is_source_finalized());
}

fn diagnostic_projection(
    source_id: &str,
    sequence: u64,
    event_time_ns: i64,
    schema_id: &str,
    key: &[u8],
    sensitivity: u64,
    value: &str,
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
            key,
            sensitivity,
            value,
        ),
    );
    let config = retention_config(100 * SECOND_NS as u64);
    let view = state.project_event_time_retention(config);
    format!(
        "{config:?}|{:?}|{view:?}|{:?}|{:?}",
        view.usage(),
        EventTimeRetentionConfigErrorV0::ZeroRetentionHorizon,
        EventTimeRetentionConfigErrorV0::UnsupportedVersion { actual: 1 }
    )
}

#[test]
fn diagnostics_are_content_independent_and_value_safe() {
    let first = diagnostic_projection(
        "secret-a",
        111,
        20 * SECOND_NS,
        "schema-a",
        b"key-a",
        1,
        "value-a",
    );
    let second = diagnostic_projection(
        "secret-b",
        999,
        21 * SECOND_NS,
        "schema-b",
        b"key-b",
        2,
        "value-b",
    );
    assert_eq!(first, second);
    for forbidden in [
        "secret-a",
        "secret-b",
        "schema-a",
        "schema-b",
        "key-a",
        "key-b",
        "value-a",
        "value-b",
        "decant://",
        "111",
        "999",
    ] {
        assert!(!first.contains(forbidden));
        assert!(!second.contains(forbidden));
    }
}
