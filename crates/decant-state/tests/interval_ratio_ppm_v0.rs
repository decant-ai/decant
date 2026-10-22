use decant_fixtures::{
    FixtureRecordV0, GENERATOR_VERSION_V0, SreGeneratorV0, decode_scenario_manifest_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, MAX_FIELD_NAME_BYTES, MAX_KEY_BYTES, ScalarValueV0,
    SchemaIdV0, SensitivityBitmapV0, SourceIdV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigV0, FinalizeOutcomeV0,
    INTERVAL_RATIO_PPM_VERSION_V0, IntervalRatioPpmConfigErrorV0, IntervalRatioPpmConfigV0,
    IntervalRatioPpmMaterializationErrorV0, IntervalRatioPpmViewV0, MAX_ACCEPTED_EVENT_BYTES_V0,
    MAX_INTERVAL_RATIO_EXAMPLES_V0, RejectionReasonV0, accepted_event_accounted_bytes_v0,
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

fn payload(fields: &[(&str, ScalarValueV0)]) -> CanonicalPayloadV0 {
    CanonicalPayloadV0::try_from_fields(
        fields
            .iter()
            .map(|(name, value)| ((*name).to_owned(), value.clone()))
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
    fields: &[(&str, ScalarValueV0)],
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

#[allow(clippy::too_many_arguments)]
fn config_for(
    horizon_ns: u64,
    schema_id: &str,
    key: &[u8],
    field: &str,
    value: ScalarValueV0,
    interval_start_ns: i64,
    interval_end_ns: i64,
    example_limit: usize,
) -> IntervalRatioPpmConfigV0 {
    IntervalRatioPpmConfigV0::new(
        INTERVAL_RATIO_PPM_VERSION_V0,
        retention(horizon_ns),
        schema(schema_id),
        key,
        field,
        value,
        interval_start_ns,
        interval_end_ns,
        example_limit,
    )
    .unwrap()
}

fn config(
    interval_start_ns: i64,
    interval_end_ns: i64,
    example_limit: usize,
) -> IntervalRatioPpmConfigV0 {
    config_for(
        u64::MAX,
        "schema-v0",
        b"key",
        "outcome",
        ScalarValueV0::utf8("error").unwrap(),
        interval_start_ns,
        interval_end_ns,
        example_limit,
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

fn assert_usage_conserved(view: &IntervalRatioPpmViewV0<'_>) {
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
        usage.before_interval_events_omitted()
            + usage.at_or_after_interval_events_omitted()
            + usage.denominator_events()
    );
    assert!(usage.numerator_events() <= usage.denominator_events());
    assert_eq!(
        usage.numerator_events(),
        usage.numerator_examples_selected() + usage.numerator_examples_omitted()
    );
}

#[test]
fn config_enforces_all_boundaries_and_accepts_all_scalar_kinds() {
    let maximum_key = vec![b'k'; MAX_KEY_BYTES];
    for predicate in [
        ScalarValueV0::null(),
        ScalarValueV0::from_bool(true),
        ScalarValueV0::from_i64(-7),
        ScalarValueV0::from_u64(7),
        ScalarValueV0::utf8("value").unwrap(),
        ScalarValueV0::bytes(b"value").unwrap(),
    ] {
        let minimum = IntervalRatioPpmConfigV0::new(
            INTERVAL_RATIO_PPM_VERSION_V0,
            retention(1),
            schema("schema-v0"),
            b"",
            "_",
            predicate.clone(),
            i64::MIN,
            i64::MIN + 1,
            0,
        )
        .unwrap();
        assert_eq!(minimum.predicate_value(), &predicate);
        assert_eq!(minimum.example_limit(), 0);
        let maximum = IntervalRatioPpmConfigV0::new(
            INTERVAL_RATIO_PPM_VERSION_V0,
            retention(u64::MAX),
            schema("schema-v0"),
            &maximum_key,
            &format!("a{}", "z".repeat(MAX_FIELD_NAME_BYTES - 1)),
            predicate,
            i64::MAX - 1,
            i64::MAX,
            MAX_INTERVAL_RATIO_EXAMPLES_V0,
        )
        .unwrap();
        assert_eq!(maximum.key(), maximum_key);
        assert_eq!(maximum.predicate_field().len(), MAX_FIELD_NAME_BYTES);
        assert_eq!(maximum.example_limit(), MAX_INTERVAL_RATIO_EXAMPLES_V0);
    }

    assert_eq!(
        IntervalRatioPpmConfigV0::new(
            1,
            retention(1),
            schema("schema-v0"),
            b"key",
            "field",
            ScalarValueV0::null(),
            0,
            1,
            0,
        ),
        Err(IntervalRatioPpmConfigErrorV0::UnsupportedVersion { actual: 1 })
    );
    assert_eq!(
        IntervalRatioPpmConfigV0::new(
            0,
            retention(1),
            schema("schema-v0"),
            &vec![b'k'; MAX_KEY_BYTES + 1],
            "field",
            ScalarValueV0::null(),
            0,
            1,
            0,
        ),
        Err(IntervalRatioPpmConfigErrorV0::KeyTooLong {
            maximum: MAX_KEY_BYTES,
            actual: MAX_KEY_BYTES + 1,
        })
    );
    for invalid in ["", "Upper", "a/b", "é"] {
        assert!(matches!(
            IntervalRatioPpmConfigV0::new(
                0,
                retention(1),
                schema("schema-v0"),
                b"key",
                invalid,
                ScalarValueV0::null(),
                0,
                1,
                0,
            ),
            Err(IntervalRatioPpmConfigErrorV0::InvalidFieldNameLength { .. })
                | Err(IntervalRatioPpmConfigErrorV0::InvalidFieldNameSyntax)
        ));
    }
    assert!(matches!(
        IntervalRatioPpmConfigV0::new(
            0,
            retention(1),
            schema("schema-v0"),
            b"key",
            &"a".repeat(MAX_FIELD_NAME_BYTES + 1),
            ScalarValueV0::null(),
            0,
            1,
            0,
        ),
        Err(IntervalRatioPpmConfigErrorV0::InvalidFieldNameLength { .. })
    ));
    for (start, end) in [(0, 0), (1, 0)] {
        assert_eq!(
            IntervalRatioPpmConfigV0::new(
                0,
                retention(1),
                schema("schema-v0"),
                b"key",
                "field",
                ScalarValueV0::null(),
                start,
                end,
                0,
            ),
            Err(IntervalRatioPpmConfigErrorV0::InvalidInterval)
        );
    }
    assert_eq!(
        IntervalRatioPpmConfigV0::new(
            0,
            retention(1),
            schema("schema-v0"),
            b"key",
            "field",
            ScalarValueV0::null(),
            0,
            1,
            MAX_INTERVAL_RATIO_EXAMPLES_V0 + 1,
        ),
        Err(IntervalRatioPpmConfigErrorV0::ExampleLimitAboveMaximum {
            maximum: MAX_INTERVAL_RATIO_EXAMPLES_V0,
            actual: MAX_INTERVAL_RATIO_EXAMPLES_V0 + 1,
        })
    );
}

#[test]
fn exact_interval_ratio_usage_and_borrowed_contributors_are_auditable() {
    let mut state = admission();
    let candidates = [
        event("source-v0", 1, 9, "other-v0", Some(b"key"), 1, &[]),
        event("source-v0", 2, 9, "schema-v0", None, 2, &[]),
        event("source-v0", 3, 9, "schema-v0", Some(b"other"), 3, &[]),
        event(
            "source-v0",
            4,
            9,
            "schema-v0",
            Some(b"key"),
            4,
            &[("outcome", ScalarValueV0::utf8("error").unwrap())],
        ),
        event(
            "source-v0",
            5,
            10,
            "schema-v0",
            Some(b"key"),
            5,
            &[("outcome", ScalarValueV0::utf8("error").unwrap())],
        ),
        event(
            "source-v0",
            6,
            15,
            "schema-v0",
            Some(b"key"),
            6,
            &[("outcome", ScalarValueV0::utf8("ok").unwrap())],
        ),
        event(
            "source-v0",
            7,
            19,
            "schema-v0",
            Some(b"key"),
            7,
            &[("outcome", ScalarValueV0::utf8("ok").unwrap())],
        ),
        event(
            "source-v0",
            8,
            20,
            "schema-v0",
            Some(b"key"),
            8,
            &[("outcome", ScalarValueV0::utf8("error").unwrap())],
        ),
    ];
    for candidate in &candidates {
        accept(&mut state, candidate);
    }

    let view = state
        .materialize_interval_ratio_ppm(config(10, 20, 1))
        .unwrap();
    let observation = view.observation().unwrap();
    assert_eq!(observation.numerator(), 1);
    assert_eq!(observation.denominator(), 3);
    assert_eq!(observation.rate_ppm(), 333_333);
    assert_eq!(observation.remainder(), 1);
    let usage = view.usage();
    assert_eq!(usage.retained_events_considered(), 8);
    assert_eq!(usage.other_schema_events_omitted(), 1);
    assert_eq!(usage.matching_schema_events(), 7);
    assert_eq!(usage.unkeyed_matching_events_omitted(), 1);
    assert_eq!(usage.different_key_matching_events_omitted(), 1);
    assert_eq!(usage.same_key_events(), 5);
    assert_eq!(usage.before_interval_events_omitted(), 1);
    assert_eq!(usage.at_or_after_interval_events_omitted(), 1);
    assert_eq!(usage.denominator_events(), 3);
    assert_eq!(usage.numerator_events(), 1);
    assert_eq!(usage.numerator_examples_selected(), 1);
    assert_eq!(usage.numerator_examples_omitted(), 0);
    assert_usage_conserved(&view);

    let denominator: Vec<_> = view.denominator_events().collect();
    assert_eq!(
        denominator
            .iter()
            .map(|event| sequence(event))
            .collect::<Vec<_>>(),
        [5, 6, 7]
    );
    let numerator: Vec<_> = view.numerator_events().collect();
    assert_eq!(
        numerator
            .iter()
            .map(|event| sequence(event))
            .collect::<Vec<_>>(),
        [5]
    );
    let examples: Vec<_> = view.numerator_examples().collect();
    assert_eq!(
        examples
            .iter()
            .map(|event| sequence(event))
            .collect::<Vec<_>>(),
        [5]
    );
    let admitted = state
        .accepted_events()
        .find(|event| sequence(event) == 5)
        .unwrap();
    assert!(std::ptr::eq(denominator[0], admitted));
    assert!(std::ptr::eq(numerator[0], admitted));
    assert!(std::ptr::eq(examples[0], admitted));
    assert_eq!(examples[0].provenance(), candidates[4].provenance());
    assert_eq!(examples[0].sensitivity(), candidates[4].sensitivity());
}

#[test]
fn zero_denominator_is_absence_not_zero() {
    let state = admission();
    let view = state
        .materialize_interval_ratio_ppm(config(0, 1, 0))
        .unwrap();
    assert_eq!(view.observation(), None);
    assert_eq!(view.usage().denominator_events(), 0);
    assert_eq!(view.numerator_examples().len(), 0);
    assert_usage_conserved(&view);
}

#[test]
fn interval_must_be_inside_epoch_and_exact_epoch_boundaries_work() {
    let state = admission();
    assert_eq!(
        state
            .materialize_interval_ratio_ppm(config(-1, 1, 0))
            .unwrap_err(),
        IntervalRatioPpmMaterializationErrorV0::IntervalOutsideEpoch
    );
    assert_eq!(
        state
            .materialize_interval_ratio_ppm(config(0, 200 * SECOND_NS + 1, 0))
            .unwrap_err(),
        IntervalRatioPpmMaterializationErrorV0::IntervalOutsideEpoch
    );
    let boundary = state
        .materialize_interval_ratio_ppm(config(0, 200 * SECOND_NS, 0))
        .unwrap();
    assert_eq!(boundary.observation(), None);
}

#[test]
fn all_scalar_kinds_match_exactly_and_schema_drift_fails_closed() {
    for (index, predicate) in [
        ScalarValueV0::null(),
        ScalarValueV0::from_bool(true),
        ScalarValueV0::from_i64(-9),
        ScalarValueV0::from_u64(9),
        ScalarValueV0::utf8("match").unwrap(),
        ScalarValueV0::bytes(b"match").unwrap(),
    ]
    .into_iter()
    .enumerate()
    {
        let mut state = admission();
        accept(
            &mut state,
            &event(
                "source-v0",
                index as u64,
                10,
                "schema-v0",
                Some(b"key"),
                0,
                &[("predicate", predicate.clone())],
            ),
        );
        let view = state
            .materialize_interval_ratio_ppm(config_for(
                u64::MAX,
                "schema-v0",
                b"key",
                "predicate",
                predicate,
                0,
                20,
                1,
            ))
            .unwrap();
        assert_eq!(view.observation().unwrap().rate_ppm(), 1_000_000);
    }

    let mut missing = admission();
    accept(
        &mut missing,
        &event("source-v0", 1, 10, "schema-v0", Some(b"key"), 0, &[]),
    );
    assert_eq!(
        missing
            .materialize_interval_ratio_ppm(config(0, 20, 1))
            .unwrap_err(),
        IntervalRatioPpmMaterializationErrorV0::PredicateFieldMissing
    );

    let mut mismatch = admission();
    accept(
        &mut mismatch,
        &event(
            "source-v0",
            1,
            10,
            "schema-v0",
            Some(b"key"),
            0,
            &[("outcome", ScalarValueV0::from_u64(1))],
        ),
    );
    assert_eq!(
        mismatch
            .materialize_interval_ratio_ppm(config(0, 20, 1))
            .unwrap_err(),
        IntervalRatioPpmMaterializationErrorV0::PredicateFieldKindMismatch
    );

    let mut excluded = admission();
    for candidate in [
        event("source-v0", 1, 10, "other-v0", Some(b"key"), 0, &[]),
        event(
            "source-v0",
            2,
            10,
            "schema-v0",
            Some(b"other"),
            0,
            &[("outcome", ScalarValueV0::from_u64(1))],
        ),
        event("source-v0", 3, 20, "schema-v0", Some(b"key"), 0, &[]),
    ] {
        accept(&mut excluded, &candidate);
    }
    assert_eq!(
        excluded
            .materialize_interval_ratio_ppm(config(0, 20, 1))
            .unwrap()
            .observation(),
        None
    );
}

#[test]
fn retention_cutoff_boundary_and_finalization_expiration_are_explicit() {
    let mut state = admission();
    for candidate in [
        event(
            "source-v0",
            0,
            60 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            &[("outcome", ScalarValueV0::utf8("error").unwrap())],
        ),
        event(
            "source-v0",
            1,
            70 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            &[("outcome", ScalarValueV0::utf8("error").unwrap())],
        ),
        event(
            "source-v0",
            2,
            130 * SECOND_NS,
            "other-v0",
            Some(b"key"),
            0,
            &[],
        ),
    ] {
        accept(&mut state, &candidate);
    }
    let exact = config_for(
        30 * SECOND_NS as u64,
        "schema-v0",
        b"key",
        "outcome",
        ScalarValueV0::utf8("error").unwrap(),
        70 * SECOND_NS,
        131 * SECOND_NS,
        1,
    );
    let view = state.materialize_interval_ratio_ppm(exact.clone()).unwrap();
    assert_eq!(view.retention_cutoff_event_time_ns(), Some(70 * SECOND_NS));
    assert_eq!(view.observation().unwrap().rate_ppm(), 1_000_000);
    assert_eq!(view.retention_usage().expired_events_excluded(), 1);
    assert!(!view.is_source_finalized());
    drop(view);

    let one_past = config_for(
        30 * SECOND_NS as u64 - 1,
        "schema-v0",
        b"key",
        "outcome",
        ScalarValueV0::utf8("error").unwrap(),
        70 * SECOND_NS,
        131 * SECOND_NS,
        1,
    );
    assert_eq!(
        state.materialize_interval_ratio_ppm(one_past).unwrap_err(),
        IntervalRatioPpmMaterializationErrorV0::RequestedIntervalExpired
    );

    let retained_after_finalize = config_for(
        200 * SECOND_NS as u64,
        "schema-v0",
        b"key",
        "outcome",
        ScalarValueV0::utf8("error").unwrap(),
        70 * SECOND_NS,
        131 * SECOND_NS,
        1,
    );
    let open = state
        .materialize_interval_ratio_ppm(retained_after_finalize.clone())
        .unwrap();
    let open_observation = open.observation();
    let open_usage = open.usage();
    assert!(!open.is_source_finalized());
    drop(open);

    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    assert_eq!(
        state.materialize_interval_ratio_ppm(exact).unwrap_err(),
        IntervalRatioPpmMaterializationErrorV0::RequestedIntervalExpired
    );
    let finalized = state
        .materialize_interval_ratio_ppm(retained_after_finalize)
        .unwrap();
    assert!(finalized.is_source_finalized());
    assert_eq!(finalized.observation(), open_observation);
    assert_eq!(finalized.usage(), open_usage);
}

fn permutation_state(order: &[usize]) -> AdmissionStateV0 {
    let candidates = [
        event(
            "source-v0",
            40,
            12,
            "schema-v0",
            Some(b"key"),
            0,
            &[("outcome", ScalarValueV0::utf8("error").unwrap())],
        ),
        event(
            "source-v0",
            10,
            10,
            "schema-v0",
            Some(b"key"),
            0,
            &[("outcome", ScalarValueV0::utf8("error").unwrap())],
        ),
        event(
            "source-v0",
            30,
            10,
            "schema-v0",
            Some(b"key"),
            0,
            &[("outcome", ScalarValueV0::utf8("error").unwrap())],
        ),
        event(
            "source-v0",
            20,
            11,
            "schema-v0",
            Some(b"key"),
            0,
            &[("outcome", ScalarValueV0::utf8("ok").unwrap())],
        ),
    ];
    let mut state = admission();
    for &index in order {
        accept(&mut state, &candidates[index]);
    }
    state
}

#[test]
fn example_bounds_and_permitted_input_permutations_converge() {
    let state_a = permutation_state(&[0, 1, 2, 3]);
    let state_b = permutation_state(&[3, 2, 1, 0]);
    for limit in [0, 1, MAX_INTERVAL_RATIO_EXAMPLES_V0] {
        let view_a = state_a
            .materialize_interval_ratio_ppm(config(0, 20, limit))
            .unwrap();
        let view_b = state_b
            .materialize_interval_ratio_ppm(config(0, 20, limit))
            .unwrap();
        assert_eq!(view_a.observation(), view_b.observation());
        assert_eq!(view_a.usage(), view_b.usage());
        assert_eq!(
            view_a
                .denominator_events()
                .map(sequence)
                .collect::<Vec<_>>(),
            view_b
                .denominator_events()
                .map(sequence)
                .collect::<Vec<_>>()
        );
        assert_eq!(
            view_a.numerator_events().map(sequence).collect::<Vec<_>>(),
            view_b.numerator_events().map(sequence).collect::<Vec<_>>()
        );
        let expected = match limit {
            0 => vec![],
            1 => vec![10],
            _ => vec![10, 30, 40],
        };
        assert_eq!(
            view_a
                .numerator_examples()
                .map(sequence)
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            view_b
                .numerator_examples()
                .map(sequence)
                .collect::<Vec<_>>(),
            expected
        );
        assert!(view_a.numerator_examples().len() <= limit);
        assert_usage_conserved(&view_a);
    }

    let mut capped_state = admission();
    for index in 0..20_u64 {
        accept(
            &mut capped_state,
            &event(
                "source-v0",
                index,
                i64::try_from(20 - index).unwrap(),
                "schema-v0",
                Some(b"key"),
                0,
                &[("outcome", ScalarValueV0::utf8("error").unwrap())],
            ),
        );
    }
    let capped = capped_state
        .materialize_interval_ratio_ppm(config(0, 100, MAX_INTERVAL_RATIO_EXAMPLES_V0))
        .unwrap();
    assert_eq!(
        capped
            .numerator_examples()
            .map(sequence)
            .collect::<Vec<_>>(),
        (4..20).rev().collect::<Vec<_>>()
    );
    assert_eq!(
        capped.usage().numerator_examples_selected(),
        MAX_INTERVAL_RATIO_EXAMPLES_V0
    );
    assert_eq!(capped.usage().numerator_examples_omitted(), 4);
    assert_usage_conserved(&capped);
}

#[test]
fn duplicate_and_rejected_attempts_never_contribute() {
    let mut state = admission();
    let accepted = event(
        "source-v0",
        1,
        10 * SECOND_NS,
        "schema-v0",
        Some(b"key"),
        0,
        &[("outcome", ScalarValueV0::utf8("error").unwrap())],
    );
    accept(&mut state, &accepted);
    assert_eq!(
        state.apply_event(&accepted),
        Ok(ApplyOutcomeV0::DuplicateNoOp)
    );
    let conflicting = event(
        "source-v0",
        1,
        10 * SECOND_NS,
        "schema-v0",
        Some(b"key"),
        0,
        &[("outcome", ScalarValueV0::utf8("ok").unwrap())],
    );
    assert_eq!(
        state.apply_event(&conflicting),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::ConflictingRedelivery
        ))
    );
    assert_eq!(
        state.apply_event(&event(
            "other-source-v0",
            2,
            11 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            &[("outcome", ScalarValueV0::utf8("error").unwrap())],
        )),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::UnexpectedSource
        ))
    );
    assert_eq!(
        state.apply_event(&event(
            "source-v0",
            3,
            200 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            &[("outcome", ScalarValueV0::utf8("error").unwrap())],
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::OutsideEpoch))
    );
    accept(
        &mut state,
        &event(
            "source-v0",
            4,
            150 * SECOND_NS,
            "other-v0",
            Some(b"key"),
            0,
            &[],
        ),
    );
    assert_eq!(
        state.apply_event(&event(
            "source-v0",
            5,
            119 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            &[("outcome", ScalarValueV0::utf8("error").unwrap())],
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::Late))
    );
    assert_eq!(
        state.finalize_source(&source("source-v0"), 200 * SECOND_NS),
        FinalizeOutcomeV0::Finalized
    );
    assert_eq!(
        state.apply_event(&event(
            "source-v0",
            6,
            160 * SECOND_NS,
            "schema-v0",
            Some(b"key"),
            0,
            &[("outcome", ScalarValueV0::utf8("error").unwrap())],
        )),
        Ok(ApplyOutcomeV0::Rejected(RejectionReasonV0::EpochFinalized))
    );
    let view = state
        .materialize_interval_ratio_ppm(config_for(
            200 * SECOND_NS as u64,
            "schema-v0",
            b"key",
            "outcome",
            ScalarValueV0::utf8("error").unwrap(),
            0,
            200 * SECOND_NS,
            1,
        ))
        .unwrap();
    assert_eq!(view.observation().unwrap().numerator(), 1);
    assert_eq!(view.observation().unwrap().denominator(), 1);

    let byte_probe = event(
        "source-v0",
        1,
        1,
        "schema-v0",
        Some(b"key"),
        0,
        &[("outcome", ScalarValueV0::utf8("error").unwrap())],
    );
    let exact_bytes = accepted_event_accounted_bytes_v0(&byte_probe).unwrap();
    let mut identity_limited = admission_with(
        0,
        100,
        AdmissionLimitsV0::new(1, MAX_ACCEPTED_EVENT_BYTES_V0).unwrap(),
    );
    accept(&mut identity_limited, &byte_probe);
    assert_eq!(
        identity_limited.apply_event(&event(
            "source-v0",
            2,
            2,
            "schema-v0",
            Some(b"key"),
            0,
            &[("outcome", ScalarValueV0::utf8("error").unwrap())],
        )),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedIdentityCapacity
        ))
    );
    let mut byte_limited = admission_with(0, 100, AdmissionLimitsV0::new(2, exact_bytes).unwrap());
    accept(&mut byte_limited, &byte_probe);
    assert_eq!(
        byte_limited.apply_event(&event(
            "source-v0",
            2,
            2,
            "schema-v0",
            Some(b"key"),
            0,
            &[("outcome", ScalarValueV0::utf8("error").unwrap())],
        )),
        Ok(ApplyOutcomeV0::Rejected(
            RejectionReasonV0::AcceptedEventByteCapacity
        ))
    );
    for limited in [&identity_limited, &byte_limited] {
        let observation = limited
            .materialize_interval_ratio_ppm(config(0, 100, 1))
            .unwrap()
            .observation()
            .unwrap();
        assert_eq!((observation.numerator(), observation.denominator()), (1, 1));
    }
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

fn scenario_config(key: &[u8], example_limit: usize) -> IntervalRatioPpmConfigV0 {
    config_for(
        (PRIMARY_END_NS - PRIMARY_START_NS) as u64,
        "payment-outcome-v0",
        key,
        "outcome",
        ScalarValueV0::utf8("error").unwrap(),
        PRIMARY_START_NS + 10 * MINUTE_NS,
        PRIMARY_END_NS,
        example_limit,
    )
}

fn independent_ratio(state: &AdmissionStateV0, key: &[u8]) -> (u64, u64) {
    let mut numerator = 0;
    let mut denominator = 0;
    for candidate in state.accepted_events().filter(|candidate| {
        candidate.schema_id() == &schema("payment-outcome-v0")
            && candidate.key() == Some(key)
            && candidate.event_time_ns() >= PRIMARY_START_NS + 10 * MINUTE_NS
            && candidate.event_time_ns() < PRIMARY_END_NS
    }) {
        denominator += 1;
        if field(candidate, "outcome").as_utf8() == Some("error") {
            numerator += 1;
        }
    }
    (numerator, denominator)
}

#[test]
fn sre_primary_and_holdouts_match_independent_payment_outcome_ratios() {
    for (scenario_id, key, expected_ppm) in [
        ("sre-v0-primary", b"payments-v2/region-b".as_slice(), 32_000),
        ("sre-v0-primary", b"payments-v2/region-a".as_slice(), 4_000),
        (
            "sre-v0-region-swap",
            b"payments-v2/region-a".as_slice(),
            32_000,
        ),
        (
            "sre-v0-service-swap",
            b"checkout-v1/region-b".as_slice(),
            32_000,
        ),
        (
            "sre-v0-no-regression",
            b"payments-v2/region-b".as_slice(),
            4_000,
        ),
    ] {
        let state = scenario_state(scenario_id);
        let view = state
            .materialize_interval_ratio_ppm(scenario_config(key, 3))
            .unwrap();
        let observation = view.observation().unwrap();
        let independent = independent_ratio(&state, key);
        assert_eq!(independent.1, 15_000);
        assert_eq!(
            (observation.numerator(), observation.denominator()),
            independent
        );
        assert_eq!(observation.rate_ppm(), expected_ppm);
        assert_eq!(observation.remainder(), 0);
        assert_eq!(view.numerator_events().count(), independent.0 as usize);
        let examples: Vec<_> = view.numerator_examples().collect();
        assert_eq!(examples.len(), 3);
        assert!(examples.windows(2).all(|pair| {
            (pair[0].event_time_ns(), sequence(pair[0]))
                < (pair[1].event_time_ns(), sequence(pair[1]))
        }));
        assert!(examples.iter().all(|candidate| {
            candidate.provenance().as_str().starts_with("decant://")
                && field(candidate, "outcome").as_utf8() == Some("error")
        }));
        assert!(view.is_source_finalized());
        assert_eq!(
            view.retention_cutoff_event_time_ns(),
            Some(PRIMARY_START_NS)
        );
        assert_usage_conserved(&view);
    }
}

#[test]
fn diagnostics_redact_configured_and_event_content() {
    let probes = [
        "source-secret-v0",
        "schema-secret-v0",
        "key-secret-v0",
        "field_secret_v0",
        "value-secret-v0",
        "-123456789012345678",
        "987654321098765432",
        "decant://source-secret-v0/synthetic/77",
    ];
    let config = config_for(
        123_456_789_012_345_679,
        "schema-secret-v0",
        b"key-secret-v0",
        "field_secret_v0",
        ScalarValueV0::utf8("value-secret-v0").unwrap(),
        -123_456_789_012_345_678,
        987_654_321_098_765_432,
        1,
    );
    let mut state = AdmissionStateV0::new(
        AdmissionConfigV0::new(
            0,
            source("source-secret-v0"),
            -123_456_789_012_345_678,
            987_654_321_098_765_432,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    );
    accept(
        &mut state,
        &event(
            "source-secret-v0",
            77,
            0,
            "schema-secret-v0",
            Some(b"key-secret-v0"),
            u64::MAX,
            &[(
                "field_secret_v0",
                ScalarValueV0::utf8("value-secret-v0").unwrap(),
            )],
        ),
    );
    let view = state
        .materialize_interval_ratio_ppm(config.clone())
        .unwrap();
    let diagnostics = format!(
        "{config:?}|{view:?}|{:?}|{}|{:?}|{}",
        IntervalRatioPpmMaterializationErrorV0::PredicateFieldMissing,
        IntervalRatioPpmMaterializationErrorV0::PredicateFieldMissing,
        IntervalRatioPpmMaterializationErrorV0::PredicateFieldKindMismatch,
        IntervalRatioPpmMaterializationErrorV0::PredicateFieldKindMismatch,
    );
    for probe in probes {
        assert!(!diagnostics.contains(probe), "leaked probe {probe}");
    }
    assert!(!diagnostics.contains(&u64::MAX.to_string()));
    assert!(!diagnostics.contains("77"));
}
