use std::collections::BTreeMap;

use decant_compiler::{
    CONTEXT_COMPILER_VERSION_V0, ContextCompileConfigErrorV0, ContextCompileErrorV0,
    ContextCompileRequestV0, ContextConfigFieldV0, ContextEvidenceRoleV0, ContextFactIdV0,
    ContextScopeIssueV0, ContextScopePredicateV0, IntervalRatioPpmFactSpecV0,
    MAX_CONTEXT_FACT_REQUESTS_V0, MAX_CONTEXT_LOOKBACK_NS_V0, MAX_CONTEXT_OUTPUT_BYTES_V0,
    MAX_CONTEXT_RATIO_EVIDENCE_V0, MAX_CONTEXT_REQUEST_BYTES_V0, PrecedingEvidenceSpecV0,
    ProjectedChangeFactSpecV0, compile_context_v0,
};
use decant_fixtures::{
    FixtureRecordV0, GENERATOR_VERSION_V0, SreGeneratorV0, decode_scenario_manifest_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, MAX_SCALAR_BYTES, ScalarValueV0, SchemaIdV0,
    SensitivityBitmapV0, SourceIdV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    EVENT_TIME_RETENTION_VERSION_V0, EventTimeRetentionConfigV0, FinalizeOutcomeV0,
    IntervalRatioPpmMaterializationErrorV0, PROJECTED_FIELD_CHANGE_VERSION_V0,
    ProjectedFieldChangeConfigV0, ProjectedFieldChangeLimitsV0, encode_admission_snapshot_v0,
    hash_admission_state_v0, restore_admission_snapshot_v0,
};

const SECOND_NS: i64 = 1_000_000_000;
const MINUTE_NS: i64 = 60 * SECOND_NS;
const EPOCH_END_NS: i64 = 200 * SECOND_NS;
const KEY: &[u8] = b"payments-v2/region-b";
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
            .collect::<BTreeMap<_, _>>(),
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

fn admission() -> AdmissionStateV0 {
    admission_with_epoch_end(EPOCH_END_NS)
}

fn admission_with_epoch_end(epoch_end_ns: i64) -> AdmissionStateV0 {
    AdmissionStateV0::new(
        AdmissionConfigV0::new(
            ADMISSION_VERSION_V0,
            source("source-v0"),
            0,
            epoch_end_ns,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    )
}

fn retention(horizon_ns: u64) -> EventTimeRetentionConfigV0 {
    EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, horizon_ns).unwrap()
}

fn scope() -> Vec<ContextScopePredicateV0> {
    vec![
        ContextScopePredicateV0::new("service_id", "payments-v2").unwrap(),
        ContextScopePredicateV0::new("region_id", "region-b").unwrap(),
    ]
}

fn ratio_fact_with(
    id: &str,
    horizon_ns: u64,
    schema_id: &str,
    field: &str,
    value: ScalarValueV0,
    evidence_limit: usize,
) -> IntervalRatioPpmFactSpecV0 {
    IntervalRatioPpmFactSpecV0::new(
        ContextFactIdV0::new(id).unwrap(),
        retention(horizon_ns),
        schema(schema_id),
        field,
        value,
        evidence_limit,
    )
    .unwrap()
}

fn ratio_fact(id: &str, evidence_limit: usize) -> IntervalRatioPpmFactSpecV0 {
    ratio_fact_with(
        id,
        EPOCH_END_NS as u64,
        "payment-outcome-v0",
        "outcome",
        ScalarValueV0::utf8("error").unwrap(),
        evidence_limit,
    )
}

fn ratio_request(
    as_of_time_ns: i64,
    lookback_ns: u64,
    byte_budget: usize,
    key: &[u8],
    request_scope: Vec<ContextScopePredicateV0>,
    facts: Vec<IntervalRatioPpmFactSpecV0>,
) -> ContextCompileRequestV0 {
    ContextCompileRequestV0::new_with_interval_ratio_facts(
        CONTEXT_COMPILER_VERSION_V0,
        as_of_time_ns,
        lookback_ns,
        byte_budget,
        key,
        request_scope,
        vec![],
        facts,
    )
    .unwrap()
}

fn payment_event(sequence: u64, event_time_ns: i64, outcome: ScalarValueV0) -> EventEnvelopeV0 {
    event(
        "source-v0",
        sequence,
        event_time_ns,
        "payment-outcome-v0",
        Some(KEY),
        sequence,
        &[
            ("outcome", outcome),
            ("region_id", ScalarValueV0::utf8("region-b").unwrap()),
            (
                "secret_payload",
                ScalarValueV0::utf8("must-not-enter-context").unwrap(),
            ),
            ("service_id", ScalarValueV0::utf8("payments-v2").unwrap()),
        ],
    )
}

fn payment_event_with_scope(
    sequence: u64,
    event_time_ns: i64,
    outcome: ScalarValueV0,
    service: Option<ScalarValueV0>,
) -> EventEnvelopeV0 {
    let mut fields = vec![
        ("outcome", outcome),
        ("region_id", ScalarValueV0::utf8("region-b").unwrap()),
    ];
    if let Some(service) = service {
        fields.push(("service_id", service));
    }
    event(
        "source-v0",
        sequence,
        event_time_ns,
        "payment-outcome-v0",
        Some(KEY),
        0,
        &fields,
    )
}

fn accept(state: &mut AdmissionStateV0, candidate: &EventEnvelopeV0) {
    assert_eq!(state.apply_event(candidate), Ok(ApplyOutcomeV0::Accepted));
}

fn projected_fact(id: &str) -> ProjectedChangeFactSpecV0 {
    let retained = retention(EPOCH_END_NS as u64);
    let change = ProjectedFieldChangeConfigV0::new(
        PROJECTED_FIELD_CHANGE_VERSION_V0,
        retained,
        schema("metric-v0"),
        "metric_name",
        "metric_value",
        ProjectedFieldChangeLimitsV0::maximum(),
    )
    .unwrap();
    let preceding = PrecedingEvidenceSpecV0::new(
        retained,
        schema("deployment-v0"),
        30 * SECOND_NS as u64,
        "deployment_id",
    )
    .unwrap();
    ProjectedChangeFactSpecV0::new(
        ContextFactIdV0::new(id).unwrap(),
        change,
        "p99_latency_ms",
        20 * SECOND_NS as u64,
        Some(preceding),
    )
    .unwrap()
}

fn metric_event(sequence: u64, event_time_ns: i64, value: u64) -> EventEnvelopeV0 {
    event(
        "source-v0",
        sequence,
        event_time_ns,
        "metric-v0",
        Some(KEY),
        0,
        &[
            (
                "metric_name",
                ScalarValueV0::utf8("p99_latency_ms").unwrap(),
            ),
            ("metric_value", ScalarValueV0::from_u64(value)),
            ("region_id", ScalarValueV0::utf8("region-b").unwrap()),
            ("service_id", ScalarValueV0::utf8("payments-v2").unwrap()),
        ],
    )
}

fn deployment_event() -> EventEnvelopeV0 {
    event(
        "source-v0",
        3,
        159 * SECOND_NS,
        "deployment-v0",
        Some(KEY),
        0,
        &[
            (
                "deployment_id",
                ScalarValueV0::utf8("payments-v2-region-b-09").unwrap(),
            ),
            ("region_id", ScalarValueV0::utf8("region-b").unwrap()),
            ("service_id", ScalarValueV0::utf8("payments-v2").unwrap()),
        ],
    )
}

fn mixed_state(order: &[usize]) -> AdmissionStateV0 {
    let candidates = [
        metric_event(1, 130 * SECOND_NS, 180),
        payment_event(10, 140 * SECOND_NS, ScalarValueV0::utf8("error").unwrap()),
        payment_event(5, 150 * SECOND_NS, ScalarValueV0::utf8("ok").unwrap()),
        deployment_event(),
        metric_event(2, 160 * SECOND_NS, 720),
        payment_event(7, 199 * SECOND_NS, ScalarValueV0::utf8("ok").unwrap()),
    ];
    let mut state = admission();
    for &index in order {
        accept(&mut state, &candidates[index]);
    }
    assert_eq!(
        state.finalize_source(&source("source-v0"), EPOCH_END_NS),
        FinalizeOutcomeV0::Finalized
    );
    state
}

fn mixed_request(byte_budget: usize) -> ContextCompileRequestV0 {
    ContextCompileRequestV0::new_with_interval_ratio_facts(
        0,
        EPOCH_END_NS,
        60 * SECOND_NS as u64,
        byte_budget,
        KEY,
        scope(),
        vec![projected_fact("a-p99-latency")],
        vec![ratio_fact("b-error-rate", 3)],
    )
    .unwrap()
}

#[test]
fn ratio_spec_and_opt_in_request_enforce_exact_bounds() {
    for scalar in [
        ScalarValueV0::null(),
        ScalarValueV0::from_bool(true),
        ScalarValueV0::from_i64(-1),
        ScalarValueV0::from_u64(1),
        ScalarValueV0::utf8("value").unwrap(),
        ScalarValueV0::bytes(b"value").unwrap(),
    ] {
        for evidence_limit in [0, MAX_CONTEXT_RATIO_EVIDENCE_V0] {
            let fact = ratio_fact_with(
                "fact-v0",
                1,
                "schema-v0",
                "predicate",
                scalar.clone(),
                evidence_limit,
            );
            assert_eq!(fact.predicate_value(), &scalar);
            assert_eq!(fact.evidence_limit(), evidence_limit);
        }
    }
    assert!(matches!(
        IntervalRatioPpmFactSpecV0::new(
            ContextFactIdV0::new("fact-v0").unwrap(),
            retention(1),
            schema("schema-v0"),
            "Upper",
            ScalarValueV0::null(),
            0,
        ),
        Err(ContextCompileConfigErrorV0::InvalidSyntax {
            field: ContextConfigFieldV0::RatioPredicateField,
        })
    ));
    assert_eq!(
        IntervalRatioPpmFactSpecV0::new(
            ContextFactIdV0::new("fact-v0").unwrap(),
            retention(1),
            schema("schema-v0"),
            "predicate",
            ScalarValueV0::null(),
            MAX_CONTEXT_RATIO_EVIDENCE_V0 + 1,
        ),
        Err(ContextCompileConfigErrorV0::AboveMaximum {
            field: ContextConfigFieldV0::RatioEvidence,
            maximum: MAX_CONTEXT_RATIO_EVIDENCE_V0,
            actual: MAX_CONTEXT_RATIO_EVIDENCE_V0 + 1,
        })
    );

    let old = ContextCompileRequestV0::new(
        0,
        EPOCH_END_NS,
        1,
        1,
        KEY,
        vec![],
        vec![projected_fact("old")],
    )
    .unwrap();
    let opt_in_empty = ContextCompileRequestV0::new_with_interval_ratio_facts(
        0,
        EPOCH_END_NS,
        1,
        1,
        KEY,
        vec![],
        vec![projected_fact("old")],
        vec![],
    )
    .unwrap();
    assert_eq!(old, opt_in_empty);
    assert!(old.interval_ratio_facts().is_empty());

    assert!(matches!(
        ContextCompileRequestV0::new_with_interval_ratio_facts(
            0,
            EPOCH_END_NS,
            1,
            1,
            KEY,
            vec![],
            vec![],
            vec![],
        ),
        Err(ContextCompileConfigErrorV0::Zero {
            field: ContextConfigFieldV0::FactRequests,
        })
    ));
    assert!(matches!(
        ContextCompileRequestV0::new_with_interval_ratio_facts(
            0,
            EPOCH_END_NS,
            2,
            1,
            KEY,
            vec![],
            vec![],
            vec![ratio_fact_with(
                "short",
                1,
                "schema-v0",
                "predicate",
                ScalarValueV0::null(),
                0,
            )],
        ),
        Err(ContextCompileConfigErrorV0::InsufficientRetention {
            field: ContextConfigFieldV0::RatioRetention,
            minimum_ns: 2,
            actual_ns: 1,
        })
    ));

    for (projected, ratios) in [
        (
            vec![],
            vec![ratio_fact("duplicate", 0), ratio_fact("duplicate", 0)],
        ),
        (
            vec![projected_fact("duplicate")],
            vec![ratio_fact("duplicate", 0)],
        ),
    ] {
        assert_eq!(
            ContextCompileRequestV0::new_with_interval_ratio_facts(
                0,
                EPOCH_END_NS,
                1,
                1,
                KEY,
                vec![],
                projected,
                ratios,
            ),
            Err(ContextCompileConfigErrorV0::DuplicateFactId)
        );
    }

    let maximum: Vec<_> = (0..MAX_CONTEXT_FACT_REQUESTS_V0)
        .map(|index| ratio_fact(&format!("fact-{index:04}"), 0))
        .collect();
    assert!(
        ContextCompileRequestV0::new_with_interval_ratio_facts(
            0,
            EPOCH_END_NS,
            1,
            1,
            KEY,
            vec![],
            vec![],
            maximum.clone(),
        )
        .is_ok()
    );
    let mut one_past = maximum;
    one_past.push(ratio_fact("overflow", 0));
    assert!(matches!(
        ContextCompileRequestV0::new_with_interval_ratio_facts(
            0,
            EPOCH_END_NS,
            1,
            1,
            KEY,
            vec![],
            vec![],
            one_past,
        ),
        Err(ContextCompileConfigErrorV0::AboveMaximum {
            field: ContextConfigFieldV0::FactRequests,
            maximum: MAX_CONTEXT_FACT_REQUESTS_V0,
            ..
        })
    ));

    let large_value = ScalarValueV0::utf8(&"x".repeat(MAX_SCALAR_BYTES)).unwrap();
    let oversized: Vec<_> = (0..2_100)
        .map(|index| {
            ratio_fact_with(
                &format!("large-{index:04}"),
                1,
                "schema-v0",
                "predicate",
                large_value.clone(),
                0,
            )
        })
        .collect();
    assert!(matches!(
        ContextCompileRequestV0::new_with_interval_ratio_facts(
            0,
            EPOCH_END_NS,
            1,
            1,
            KEY,
            vec![],
            vec![],
            oversized,
        ),
        Err(ContextCompileConfigErrorV0::AboveMaximum {
            field: ContextConfigFieldV0::RequestBytes,
            maximum: MAX_CONTEXT_REQUEST_BYTES_V0,
            ..
        })
    ));
}

#[test]
fn exact_ratio_receipt_is_integer_bounded_and_omits_full_payload() {
    let state = mixed_state(&[0, 1, 2, 3, 4, 5]);
    let response = compile_context_v0(
        &state,
        &ratio_request(
            EPOCH_END_NS,
            60 * SECOND_NS as u64,
            MAX_CONTEXT_OUTPUT_BYTES_V0,
            KEY,
            scope(),
            vec![ratio_fact("error-rate", 3)],
        ),
    )
    .unwrap();
    assert_eq!(
        (
            response.candidate_count(),
            response.included_count(),
            response.omitted_count()
        ),
        (1, 1, 0)
    );
    let text = std::str::from_utf8(response.as_bytes()).unwrap();
    for expected in [
        "\"kind\":\"interval_ratio_ppm\"",
        "\"field\":\"outcome\"",
        "\"equals\":{\"kind\":\"utf8\",\"value\":\"error\"}",
        "\"start_event_time_ns\":140000000000",
        "\"end_exclusive_event_time_ns\":200000000000",
        "\"numerator\":1,\"denominator\":3,\"rate_ppm\":333333,\"remainder\":1",
        "\"operator\":\"interval_ratio_ppm_v0\"",
        "\"numerator_evidence_count\":1",
        "\"numerator_evidence_omitted\":0",
        "decant://event/v0/source-v0/synthetic/10",
    ] {
        assert!(text.contains(expected), "missing {expected}: {text}");
    }
    let hash = hash_admission_state_v0(&state)
        .unwrap()
        .to_canonical_string();
    assert!(text.contains(&hash));
    assert!(!text.contains("must-not-enter-context"));
    assert!(!text.contains("secret_payload"));
    assert!(!text.contains("1.0"));

    let empty = compile_context_v0(
        &state,
        &ratio_request(
            EPOCH_END_NS,
            60 * SECOND_NS as u64,
            MAX_CONTEXT_OUTPUT_BYTES_V0,
            b"missing-key",
            vec![],
            vec![ratio_fact("empty", 3)],
        ),
    )
    .unwrap();
    assert_eq!((empty.candidate_count(), empty.included_count()), (0, 0));

    let mut boundaries = admission_with_epoch_end(EPOCH_END_NS + SECOND_NS);
    for (sequence, event_time_ns) in [
        (1, 140 * SECOND_NS - 1),
        (2, 140 * SECOND_NS),
        (3, EPOCH_END_NS - 1),
        (4, EPOCH_END_NS),
    ] {
        accept(
            &mut boundaries,
            &payment_event(
                sequence,
                event_time_ns,
                ScalarValueV0::utf8("error").unwrap(),
            ),
        );
    }
    let boundary_response = compile_context_v0(
        &boundaries,
        &ratio_request(
            EPOCH_END_NS,
            60 * SECOND_NS as u64,
            MAX_CONTEXT_OUTPUT_BYTES_V0,
            KEY,
            scope(),
            vec![ratio_fact("boundaries", 0)],
        ),
    )
    .unwrap();
    let boundary_text = std::str::from_utf8(boundary_response.as_bytes()).unwrap();
    assert!(boundary_text.contains("\"numerator\":2,\"denominator\":2"));
    assert_eq!(
        compile_context_v0(
            &state,
            &ratio_request(
                EPOCH_END_NS + 1,
                60 * SECOND_NS as u64,
                MAX_CONTEXT_OUTPUT_BYTES_V0,
                KEY,
                scope(),
                vec![ratio_fact("outside-epoch", 0)],
            ),
        ),
        Err(ContextCompileErrorV0::QueryOutsideEpoch)
    );
}

#[test]
fn ratio_view_failures_and_denominator_scope_fail_closed() {
    for (service, issue) in [
        (None, ContextScopeIssueV0::Missing),
        (
            Some(ScalarValueV0::from_u64(7)),
            ContextScopeIssueV0::ExpectedUtf8 {
                actual: decant_ir::ScalarKindV0::U64,
            },
        ),
        (
            Some(ScalarValueV0::utf8("checkout-v1").unwrap()),
            ContextScopeIssueV0::Unequal,
        ),
    ] {
        let mut state = admission();
        accept(
            &mut state,
            &payment_event_with_scope(
                1,
                150 * SECOND_NS,
                ScalarValueV0::utf8("error").unwrap(),
                service,
            ),
        );
        assert_eq!(
            compile_context_v0(
                &state,
                &ratio_request(
                    EPOCH_END_NS,
                    60 * SECOND_NS as u64,
                    MAX_CONTEXT_OUTPUT_BYTES_V0,
                    KEY,
                    scope(),
                    vec![ratio_fact("scope", 1)],
                ),
            ),
            Err(ContextCompileErrorV0::ScopeContract {
                role: ContextEvidenceRoleV0::RatioDenominator,
                issue,
            })
        );
    }

    let mut excluded = admission();
    accept(
        &mut excluded,
        &event(
            "source-v0",
            1,
            150 * SECOND_NS,
            "other-v0",
            Some(KEY),
            0,
            &[],
        ),
    );
    assert_eq!(
        compile_context_v0(
            &excluded,
            &ratio_request(
                EPOCH_END_NS,
                60 * SECOND_NS as u64,
                MAX_CONTEXT_OUTPUT_BYTES_V0,
                KEY,
                scope(),
                vec![ratio_fact("excluded", 1)],
            ),
        )
        .unwrap()
        .candidate_count(),
        0
    );

    for (candidate, expected) in [
        (
            event(
                "source-v0",
                1,
                150 * SECOND_NS,
                "payment-outcome-v0",
                Some(KEY),
                0,
                &[
                    ("region_id", ScalarValueV0::utf8("region-b").unwrap()),
                    ("service_id", ScalarValueV0::utf8("payments-v2").unwrap()),
                ],
            ),
            IntervalRatioPpmMaterializationErrorV0::PredicateFieldMissing,
        ),
        (
            payment_event(1, 150 * SECOND_NS, ScalarValueV0::from_u64(1)),
            IntervalRatioPpmMaterializationErrorV0::PredicateFieldKindMismatch,
        ),
    ] {
        let mut state = admission();
        accept(&mut state, &candidate);
        assert_eq!(
            compile_context_v0(
                &state,
                &ratio_request(
                    EPOCH_END_NS,
                    60 * SECOND_NS as u64,
                    MAX_CONTEXT_OUTPUT_BYTES_V0,
                    KEY,
                    scope(),
                    vec![ratio_fact("contract", 1)],
                ),
            ),
            Err(ContextCompileErrorV0::IntervalRatioView(expected))
        );
    }

    let mut expired = admission();
    accept(
        &mut expired,
        &payment_event(1, 100 * SECOND_NS, ScalarValueV0::utf8("error").unwrap()),
    );
    assert_eq!(
        expired.finalize_source(&source("source-v0"), EPOCH_END_NS),
        FinalizeOutcomeV0::Finalized
    );
    let expired_fact = ratio_fact_with(
        "expired",
        60 * SECOND_NS as u64,
        "payment-outcome-v0",
        "outcome",
        ScalarValueV0::utf8("error").unwrap(),
        1,
    );
    assert_eq!(
        compile_context_v0(
            &expired,
            &ratio_request(
                150 * SECOND_NS,
                60 * SECOND_NS as u64,
                MAX_CONTEXT_OUTPUT_BYTES_V0,
                KEY,
                scope(),
                vec![expired_fact],
            ),
        ),
        Err(ContextCompileErrorV0::IntervalRatioView(
            IntervalRatioPpmMaterializationErrorV0::RequestedIntervalExpired
        ))
    );
}

#[test]
fn mixed_order_is_canonical_and_frozen() {
    let state_a = mixed_state(&[0, 1, 2, 3, 4, 5]);
    let state_b = mixed_state(&[1, 0, 3, 2, 4, 5]);
    let first = compile_context_v0(&state_a, &mixed_request(MAX_CONTEXT_OUTPUT_BYTES_V0)).unwrap();

    let mut reversed_scope = scope();
    reversed_scope.reverse();
    let reversed = ContextCompileRequestV0::new_with_interval_ratio_facts(
        0,
        EPOCH_END_NS,
        60 * SECOND_NS as u64,
        MAX_CONTEXT_OUTPUT_BYTES_V0,
        KEY,
        reversed_scope,
        vec![projected_fact("a-p99-latency")],
        vec![ratio_fact("z-ratio", 0), ratio_fact("b-error-rate", 3)],
    )
    .unwrap();
    let canonical = ContextCompileRequestV0::new_with_interval_ratio_facts(
        0,
        EPOCH_END_NS,
        60 * SECOND_NS as u64,
        MAX_CONTEXT_OUTPUT_BYTES_V0,
        KEY,
        scope(),
        vec![projected_fact("a-p99-latency")],
        vec![ratio_fact("b-error-rate", 3), ratio_fact("z-ratio", 0)],
    )
    .unwrap();
    assert_eq!(
        compile_context_v0(&state_a, &reversed).unwrap(),
        compile_context_v0(&state_b, &canonical).unwrap()
    );

    assert_eq!(first.candidate_count(), 2);
    assert_eq!(first.included_count(), 2);
    let text = std::str::from_utf8(first.as_bytes()).unwrap();
    assert!(
        text.find("projected_scalar_change").unwrap() < text.find("interval_ratio_ppm").unwrap()
    );
    assert_eq!(
        first.as_bytes(),
        include_str!("golden/interval-ratio-context-v0.json")
            .trim_end()
            .as_bytes()
    );
}

#[test]
fn mixed_budget_is_atomic_and_preserves_family_prefix() {
    let state = mixed_state(&[0, 1, 2, 3, 4, 5]);
    let minimum = match compile_context_v0(&state, &mixed_request(1)).unwrap_err() {
        ContextCompileErrorV0::BudgetTooSmall { minimum, actual: 1 } => minimum,
        other => panic!("unexpected error: {other:?}"),
    };
    let metadata = compile_context_v0(&state, &mixed_request(minimum)).unwrap();
    assert_eq!(
        (metadata.included_count(), metadata.omitted_count()),
        (0, 2)
    );

    let mut first_fit = None;
    let mut second_fit = None;
    for budget in minimum..=MAX_CONTEXT_OUTPUT_BYTES_V0 {
        let response = compile_context_v0(&state, &mixed_request(budget)).unwrap();
        assert!(response.as_bytes().len() <= budget);
        if response.included_count() == 1 && first_fit.is_none() {
            first_fit = Some(budget);
        }
        if response.included_count() == 2 {
            second_fit = Some(budget);
            break;
        }
    }
    let first_fit = first_fit.unwrap();
    let second_fit = second_fit.unwrap();
    assert_eq!(
        compile_context_v0(&state, &mixed_request(first_fit - 1))
            .unwrap()
            .included_count(),
        0
    );
    let one = compile_context_v0(&state, &mixed_request(first_fit)).unwrap();
    assert_eq!(one.as_bytes().len(), first_fit);
    let one_text = std::str::from_utf8(one.as_bytes()).unwrap();
    assert!(one_text.contains("projected_scalar_change"));
    assert!(!one_text.contains("interval_ratio_ppm"));
    assert_eq!(
        compile_context_v0(&state, &mixed_request(second_fit - 1))
            .unwrap()
            .included_count(),
        1
    );
    assert_eq!(
        compile_context_v0(&state, &mixed_request(second_fit))
            .unwrap()
            .included_count(),
        2
    );
}

#[test]
fn evidence_cap_total_rank_snapshot_restore_and_fresh_replay_converge() {
    fn evidence_state(order: &[usize]) -> AdmissionStateV0 {
        let candidates = [
            payment_event(30, 150 * SECOND_NS, ScalarValueV0::utf8("error").unwrap()),
            payment_event(10, 145 * SECOND_NS, ScalarValueV0::utf8("error").unwrap()),
            payment_event(20, 145 * SECOND_NS, ScalarValueV0::utf8("error").unwrap()),
            payment_event(40, 151 * SECOND_NS, ScalarValueV0::utf8("error").unwrap()),
            payment_event(50, 152 * SECOND_NS, ScalarValueV0::utf8("error").unwrap()),
        ];
        let mut state = admission();
        for &index in order {
            accept(&mut state, &candidates[index]);
        }
        state
    }

    let state = evidence_state(&[0, 1, 2, 3, 4]);
    let permuted = evidence_state(&[2, 1, 0, 4, 3]);
    let request = ratio_request(
        EPOCH_END_NS,
        60 * SECOND_NS as u64,
        MAX_CONTEXT_OUTPUT_BYTES_V0,
        KEY,
        scope(),
        vec![ratio_fact("evidence", 3)],
    );
    let original = compile_context_v0(&state, &request).unwrap();
    assert_eq!(original, compile_context_v0(&permuted, &request).unwrap());
    let restored =
        restore_admission_snapshot_v0(&encode_admission_snapshot_v0(&state).unwrap()).unwrap();
    assert_eq!(original, compile_context_v0(&restored, &request).unwrap());
    let text = std::str::from_utf8(original.as_bytes()).unwrap();
    assert!(text.contains("\"numerator_evidence_count\":3"));
    assert!(text.contains("\"numerator_evidence_omitted\":2"));
    let sequence_10 = text.find("synthetic/10").unwrap();
    let sequence_20 = text.find("synthetic/20").unwrap();
    let sequence_30 = text.find("synthetic/30").unwrap();
    assert!(sequence_10 < sequence_20 && sequence_20 < sequence_30);
    assert!(!text.contains("synthetic/40"));
    assert!(!text.contains("synthetic/50"));
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

fn sre_ratio_fact(id: &str) -> IntervalRatioPpmFactSpecV0 {
    ratio_fact_with(
        id,
        (PRIMARY_END_NS - PRIMARY_START_NS) as u64,
        "payment-outcome-v0",
        "outcome",
        ScalarValueV0::utf8("error").unwrap(),
        3,
    )
}

fn sre_projected_fact() -> ProjectedChangeFactSpecV0 {
    let retained = retention(25 * MINUTE_NS as u64);
    let change = ProjectedFieldChangeConfigV0::new(
        PROJECTED_FIELD_CHANGE_VERSION_V0,
        retained,
        schema("metric-sample-v0"),
        "metric_name",
        "metric_value",
        ProjectedFieldChangeLimitsV0::maximum(),
    )
    .unwrap();
    let preceding = PrecedingEvidenceSpecV0::new(
        retained,
        schema("deployment-v0"),
        15 * MINUTE_NS as u64,
        "deployment_id",
    )
    .unwrap();
    ProjectedChangeFactSpecV0::new(
        ContextFactIdV0::new("p99-latency").unwrap(),
        change,
        "p99_latency_ms",
        15 * MINUTE_NS as u64,
        Some(preceding),
    )
    .unwrap()
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
        let outcome = candidate
            .payload()
            .fields()
            .find_map(|(name, value)| (name == "outcome").then_some(value))
            .unwrap();
        if outcome.as_utf8() == Some("error") {
            numerator += 1;
        }
    }
    (numerator, denominator)
}

#[test]
fn sre_primary_and_holdout_ratios_match_independent_outcome_counts() {
    let primary = scenario_state("sre-v0-primary");
    let primary_request = ContextCompileRequestV0::new_with_interval_ratio_facts(
        0,
        PRIMARY_END_NS,
        MAX_CONTEXT_LOOKBACK_NS_V0,
        MAX_CONTEXT_OUTPUT_BYTES_V0,
        KEY,
        scope(),
        vec![sre_projected_fact()],
        vec![sre_ratio_fact("payment-error-rate")],
    )
    .unwrap();
    let primary_response = compile_context_v0(&primary, &primary_request).unwrap();
    assert_eq!(
        (
            primary_response.candidate_count(),
            primary_response.included_count()
        ),
        (2, 2)
    );
    let primary_text = std::str::from_utf8(primary_response.as_bytes()).unwrap();
    assert!(primary_text.contains("\"numerator\":480,\"denominator\":15000"));
    assert!(primary_text.contains("\"rate_ppm\":32000,\"remainder\":0"));
    assert!(primary_text.contains("\"value\":180"));
    assert!(primary_text.contains("\"value\":720"));
    assert!(primary_text.contains("payments-v2-region-b-09"));
    assert!(primary_text.contains("\"numerator_evidence_count\":3"));
    let ratio_text = &primary_text[primary_text
        .find("\"kind\":\"interval_ratio_ppm\"")
        .unwrap()..];
    assert_eq!(
        ratio_text
            .matches("\"provenance\":\"decant://event/v0/sre-v0/synthetic/")
            .count(),
        3
    );
    assert!(!primary_text.contains("request_ordinal"));
    assert_eq!(independent_ratio(&primary, KEY), (480, 15_000));

    for (scenario_id, key, expected_numerator, expected_ppm) in [
        (
            "sre-v0-primary",
            b"payments-v2/region-a".as_slice(),
            60,
            4_000,
        ),
        (
            "sre-v0-region-swap",
            b"payments-v2/region-a".as_slice(),
            480,
            32_000,
        ),
        (
            "sre-v0-service-swap",
            b"checkout-v1/region-b".as_slice(),
            480,
            32_000,
        ),
        (
            "sre-v0-no-regression",
            b"payments-v2/region-b".as_slice(),
            60,
            4_000,
        ),
    ] {
        let state = scenario_state(scenario_id);
        let expected = independent_ratio(&state, key);
        assert_eq!(expected, (expected_numerator, 15_000));
        let response = compile_context_v0(
            &state,
            &ratio_request(
                PRIMARY_END_NS,
                MAX_CONTEXT_LOOKBACK_NS_V0,
                MAX_CONTEXT_OUTPUT_BYTES_V0,
                key,
                vec![],
                vec![sre_ratio_fact("payment-error-rate")],
            ),
        )
        .unwrap();
        let text = std::str::from_utf8(response.as_bytes()).unwrap();
        assert!(text.contains(&format!("\"numerator\":{expected_numerator}")));
        assert!(text.contains("\"denominator\":15000"));
        assert!(text.contains(&format!("\"rate_ppm\":{expected_ppm}")));
    }
}

#[test]
fn ratio_diagnostics_redact_configured_and_event_content() {
    let fact = ratio_fact_with(
        "probe-fact",
        EPOCH_END_NS as u64,
        "probe-schema-v0",
        "probe_field",
        ScalarValueV0::utf8("probe-value").unwrap(),
        3,
    );
    let request = ContextCompileRequestV0::new_with_interval_ratio_facts(
        0,
        EPOCH_END_NS,
        60 * SECOND_NS as u64,
        MAX_CONTEXT_OUTPUT_BYTES_V0,
        b"probe-key",
        vec![ContextScopePredicateV0::new("probe_scope", "probe-scope-value").unwrap()],
        vec![],
        vec![fact.clone()],
    )
    .unwrap();
    let state = admission();
    let response = compile_context_v0(&state, &request).unwrap();
    let hash = hash_admission_state_v0(&state).unwrap();
    for diagnostic in [
        format!("{fact:?}"),
        format!("{request:?}"),
        format!("{response:?}"),
        format!("{hash:?}"),
        format!("{:?}", ContextCompileErrorV0::IntervalRatioConfigInvariant),
        format!(
            "{:?}",
            ContextCompileErrorV0::IntervalRatioView(
                IntervalRatioPpmMaterializationErrorV0::PredicateFieldMissing
            )
        ),
    ] {
        assert!(!diagnostic.contains("probe"), "leaked: {diagnostic}");
        assert!(!diagnostic.contains("decant://"), "leaked: {diagnostic}");
    }
}

#[test]
fn public_ratio_limits_remain_frozen() {
    assert_eq!(MAX_CONTEXT_RATIO_EVIDENCE_V0, 3);
    assert_eq!(MAX_CONTEXT_OUTPUT_BYTES_V0, 16_384);
    assert_eq!(MAX_CONTEXT_FACT_REQUESTS_V0, 4_096);
}
