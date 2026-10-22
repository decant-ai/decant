use decant_compiler::{
    CONTEXT_COMPILER_VERSION_V0, ContextCompileConfigErrorV0, ContextCompileErrorV0,
    ContextCompileRequestV0, ContextConfigFieldV0, ContextEvidenceRoleV0, ContextFactIdV0,
    ContextScopeIssueV0, ContextScopePredicateV0, MAX_CONTEXT_CANDIDATE_BYTES_V0,
    MAX_CONTEXT_EVIDENCE_REACHBACK_NS_V0, MAX_CONTEXT_FACT_REQUESTS_V0, MAX_CONTEXT_LOOKBACK_NS_V0,
    MAX_CONTEXT_OUTPUT_BYTES_V0, MAX_CONTEXT_REQUEST_BYTES_V0, MAX_CONTEXT_SCOPE_PREDICATES_V0,
    PrecedingEvidenceSpecV0, ProjectedChangeFactSpecV0, compile_context_v0,
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
    PROJECTED_FIELD_CHANGE_VERSION_V0, ProjectedFieldChangeConfigV0, ProjectedFieldChangeLimitsV0,
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

fn retention(seconds: u64) -> EventTimeRetentionConfigV0 {
    EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, seconds * SECOND_NS as u64)
        .unwrap()
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

fn metric_event(
    sequence: u64,
    event_time_ns: i64,
    discriminator: &str,
    value: ScalarValueV0,
) -> EventEnvelopeV0 {
    metric_event_with_region(
        sequence,
        event_time_ns,
        discriminator,
        value,
        ScalarValueV0::utf8("region-b").unwrap(),
    )
}

fn metric_event_with_region(
    sequence: u64,
    event_time_ns: i64,
    discriminator: &str,
    value: ScalarValueV0,
    region: ScalarValueV0,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source("source-v0"),
        schema("metric-v0"),
        SourcePositionV0::Synthetic { sequence },
        Some(KEY),
        event_time_ns,
        event_time_ns,
        SensitivityBitmapV0::from_bits(0),
        payload([
            ("metric_name", ScalarValueV0::utf8(discriminator).unwrap()),
            ("metric_value", value),
            ("region_id", region),
            ("service_id", ScalarValueV0::utf8("payments-v2").unwrap()),
        ]),
    )
    .unwrap()
}

fn deployment_event(
    sequence: u64,
    event_time_ns: i64,
    key: &[u8],
    deployment_id: &str,
) -> EventEnvelopeV0 {
    deployment_event_with_region(
        sequence,
        event_time_ns,
        key,
        deployment_id,
        ScalarValueV0::utf8("region-b").unwrap(),
    )
}

fn deployment_event_with_region(
    sequence: u64,
    event_time_ns: i64,
    key: &[u8],
    deployment_id: &str,
    region: ScalarValueV0,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source("source-v0"),
        schema("deployment-v0"),
        SourcePositionV0::Synthetic { sequence },
        Some(key),
        event_time_ns,
        event_time_ns,
        SensitivityBitmapV0::from_bits(0),
        payload([
            ("deployment_id", ScalarValueV0::utf8(deployment_id).unwrap()),
            ("region_id", region),
            ("service_id", ScalarValueV0::utf8("payments-v2").unwrap()),
        ]),
    )
    .unwrap()
}

fn admission() -> AdmissionStateV0 {
    AdmissionStateV0::new(
        AdmissionConfigV0::new(
            ADMISSION_VERSION_V0,
            source("source-v0"),
            0,
            EPOCH_END_NS,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    )
}

fn accept(state: &mut AdmissionStateV0, event: &EventEnvelopeV0) {
    assert_eq!(state.apply_event(event), Ok(ApplyOutcomeV0::Accepted));
}

fn base_state(finalized: bool) -> AdmissionStateV0 {
    let mut state = admission();
    for event in [
        deployment_event(1, 40 * SECOND_NS, KEY, "deploy-old"),
        deployment_event(2, 159 * SECOND_NS, b"payments-v2/region-a", "decoy"),
        deployment_event(3, 159 * SECOND_NS, KEY, "deploy-near-low-sequence"),
        deployment_event(4, 159 * SECOND_NS, KEY, "deploy-near"),
        metric_event(
            10,
            130 * SECOND_NS,
            "error_rate_ppm",
            ScalarValueV0::from_u64(4_000),
        ),
        metric_event(
            11,
            160 * SECOND_NS,
            "error_rate_ppm",
            ScalarValueV0::from_u64(32_000),
        ),
        metric_event(
            12,
            131 * SECOND_NS,
            "p99_latency_ms",
            ScalarValueV0::from_u64(180),
        ),
        metric_event(
            13,
            161 * SECOND_NS,
            "p99_latency_ms",
            ScalarValueV0::from_u64(720),
        ),
    ] {
        accept(&mut state, &event);
    }
    if finalized {
        assert_eq!(
            state.finalize_source(&source("source-v0"), EPOCH_END_NS),
            FinalizeOutcomeV0::Finalized
        );
    }
    state
}

fn change_config(retention_seconds: u64) -> ProjectedFieldChangeConfigV0 {
    ProjectedFieldChangeConfigV0::new(
        PROJECTED_FIELD_CHANGE_VERSION_V0,
        retention(retention_seconds),
        schema("metric-v0"),
        "metric_name",
        "metric_value",
        ProjectedFieldChangeLimitsV0::maximum(),
    )
    .unwrap()
}

fn preceding_spec() -> PrecedingEvidenceSpecV0 {
    PrecedingEvidenceSpecV0::new(
        retention(200),
        schema("deployment-v0"),
        130 * SECOND_NS as u64,
        "deployment_id",
    )
    .unwrap()
}

fn fact(id: &str, discriminator: &str, preceding: bool) -> ProjectedChangeFactSpecV0 {
    ProjectedChangeFactSpecV0::new(
        ContextFactIdV0::new(id).unwrap(),
        change_config(200),
        discriminator,
        20 * SECOND_NS as u64,
        preceding.then(preceding_spec),
    )
    .unwrap()
}

fn scope() -> Vec<ContextScopePredicateV0> {
    vec![
        ContextScopePredicateV0::new("service_id", "payments-v2").unwrap(),
        ContextScopePredicateV0::new("region_id", "region-b").unwrap(),
    ]
}

fn request_with(
    as_of_time_ns: i64,
    byte_budget: usize,
    scope: Vec<ContextScopePredicateV0>,
    facts: Vec<ProjectedChangeFactSpecV0>,
) -> ContextCompileRequestV0 {
    ContextCompileRequestV0::new(
        CONTEXT_COMPILER_VERSION_V0,
        as_of_time_ns,
        60 * SECOND_NS as u64,
        byte_budget,
        KEY,
        scope,
        facts,
    )
    .unwrap()
}

fn two_fact_request(byte_budget: usize) -> ContextCompileRequestV0 {
    request_with(
        EPOCH_END_NS,
        byte_budget,
        scope(),
        vec![
            fact("p99-latency", "p99_latency_ms", true),
            fact("error-rate", "error_rate_ppm", true),
        ],
    )
}

#[test]
fn constructors_enforce_versions_bounds_uniqueness_and_retention() {
    assert_eq!(
        ContextFactIdV0::new("UPPER"),
        Err(ContextCompileConfigErrorV0::InvalidSyntax {
            field: ContextConfigFieldV0::FactId
        })
    );
    assert_eq!(
        ContextScopePredicateV0::new("Upper", "value"),
        Err(ContextCompileConfigErrorV0::InvalidSyntax {
            field: ContextConfigFieldV0::ScopeField
        })
    );
    assert_eq!(
        ContextCompileRequestV0::new(
            1,
            EPOCH_END_NS,
            1,
            1,
            KEY,
            vec![],
            vec![fact("one", "error_rate_ppm", false)],
        ),
        Err(ContextCompileConfigErrorV0::UnsupportedVersion { actual: 1 })
    );
    assert!(matches!(
        ContextCompileRequestV0::new(
            0,
            EPOCH_END_NS,
            0,
            1,
            KEY,
            vec![],
            vec![fact("one", "error_rate_ppm", false)],
        ),
        Err(ContextCompileConfigErrorV0::Zero {
            field: ContextConfigFieldV0::Lookback
        })
    ));
    assert!(matches!(
        ContextCompileRequestV0::new(
            0,
            EPOCH_END_NS,
            MAX_CONTEXT_LOOKBACK_NS_V0 + 1,
            MAX_CONTEXT_OUTPUT_BYTES_V0,
            KEY,
            vec![],
            vec![fact("one", "error_rate_ppm", false)],
        ),
        Err(ContextCompileConfigErrorV0::AboveMaximum {
            field: ContextConfigFieldV0::Lookback,
            ..
        })
    ));
    assert!(matches!(
        ContextCompileRequestV0::new(
            0,
            EPOCH_END_NS,
            1,
            MAX_CONTEXT_OUTPUT_BYTES_V0 + 1,
            KEY,
            vec![],
            vec![fact("one", "error_rate_ppm", false)],
        ),
        Err(ContextCompileConfigErrorV0::AboveMaximum {
            field: ContextConfigFieldV0::OutputBytes,
            ..
        })
    ));

    let duplicate_scope = vec![
        ContextScopePredicateV0::new("region_id", "a").unwrap(),
        ContextScopePredicateV0::new("region_id", "b").unwrap(),
    ];
    assert_eq!(
        ContextCompileRequestV0::new(
            0,
            EPOCH_END_NS,
            1,
            1,
            KEY,
            duplicate_scope,
            vec![fact("one", "error_rate_ppm", false)],
        ),
        Err(ContextCompileConfigErrorV0::DuplicateScopeField)
    );
    assert_eq!(
        ContextCompileRequestV0::new(
            0,
            EPOCH_END_NS,
            1,
            1,
            KEY,
            vec![],
            vec![
                fact("one", "error_rate_ppm", false),
                fact("one", "p99_latency_ms", false),
            ],
        ),
        Err(ContextCompileConfigErrorV0::DuplicateFactId)
    );

    let too_many_scope = (0..=MAX_CONTEXT_SCOPE_PREDICATES_V0)
        .map(|index| ContextScopePredicateV0::new(&format!("field-{index}"), "v").unwrap())
        .collect();
    assert!(matches!(
        ContextCompileRequestV0::new(
            0,
            EPOCH_END_NS,
            1,
            1,
            KEY,
            too_many_scope,
            vec![fact("one", "error_rate_ppm", false)],
        ),
        Err(ContextCompileConfigErrorV0::AboveMaximum {
            field: ContextConfigFieldV0::ScopePredicates,
            ..
        })
    ));

    let insufficient = ProjectedChangeFactSpecV0::new(
        ContextFactIdV0::new("short").unwrap(),
        change_config(60),
        "error_rate_ppm",
        20 * SECOND_NS as u64,
        None,
    )
    .unwrap();
    assert!(matches!(
        ContextCompileRequestV0::new(
            0,
            EPOCH_END_NS,
            60 * SECOND_NS as u64,
            1,
            KEY,
            vec![],
            vec![insufficient],
        ),
        Err(ContextCompileConfigErrorV0::InsufficientRetention {
            field: ContextConfigFieldV0::ChangeRetention,
            ..
        })
    ));
    assert!(matches!(
        ProjectedChangeFactSpecV0::new(
            ContextFactIdV0::new("reachback").unwrap(),
            change_config(200),
            "series",
            MAX_CONTEXT_EVIDENCE_REACHBACK_NS_V0 + 1,
            None,
        ),
        Err(ContextCompileConfigErrorV0::AboveMaximum {
            field: ContextConfigFieldV0::EvidenceReachback,
            ..
        })
    ));

    let maximum_facts = (0..MAX_CONTEXT_FACT_REQUESTS_V0)
        .map(|index| fact(&format!("f{index:04}"), "error_rate_ppm", false))
        .collect::<Vec<_>>();
    assert!(
        ContextCompileRequestV0::new(0, EPOCH_END_NS, 1, 1, KEY, vec![], maximum_facts.clone(),)
            .is_ok()
    );
    let mut one_too_many = maximum_facts;
    one_too_many.push(fact("overflow", "error_rate_ppm", false));
    assert!(matches!(
        ContextCompileRequestV0::new(0, EPOCH_END_NS, 1, 1, KEY, vec![], one_too_many,),
        Err(ContextCompileConfigErrorV0::AboveMaximum {
            field: ContextConfigFieldV0::FactRequests,
            ..
        })
    ));

    let oversized_request = (0..2_100)
        .map(|index| {
            let discriminator = format!("{index:04}{}", "x".repeat(MAX_SCALAR_BYTES - 4));
            fact(&format!("large-{index:04}"), &discriminator, false)
        })
        .collect();
    assert!(matches!(
        ContextCompileRequestV0::new(0, EPOCH_END_NS, 1, 1, KEY, vec![], oversized_request,),
        Err(ContextCompileConfigErrorV0::AboveMaximum {
            field: ContextConfigFieldV0::RequestBytes,
            maximum: MAX_CONTEXT_REQUEST_BYTES_V0,
            ..
        })
    ));
}

#[test]
fn hostile_sizes_hit_candidate_and_metadata_caps_explicitly() {
    let mut state = admission();
    let mut facts = Vec::new();
    for index in 0..180 {
        let discriminator = format!("series-{index:03}");
        let mut previous = "\0".repeat(MAX_SCALAR_BYTES - 1);
        previous.push('a');
        let mut current = "\0".repeat(MAX_SCALAR_BYTES - 1);
        current.push('b');
        accept(
            &mut state,
            &metric_event(
                u64::try_from(index * 2 + 1).unwrap(),
                130 * SECOND_NS,
                &discriminator,
                ScalarValueV0::utf8(&previous).unwrap(),
            ),
        );
        accept(
            &mut state,
            &metric_event(
                u64::try_from(index * 2 + 2).unwrap(),
                160 * SECOND_NS,
                &discriminator,
                ScalarValueV0::utf8(&current).unwrap(),
            ),
        );
        facts.push(fact(&format!("fact-{index:03}"), &discriminator, false));
    }
    assert_eq!(
        state.finalize_source(&source("source-v0"), EPOCH_END_NS),
        FinalizeOutcomeV0::Finalized
    );
    assert!(matches!(
        compile_context_v0(
            &state,
            &request_with(EPOCH_END_NS, MAX_CONTEXT_OUTPUT_BYTES_V0, scope(), facts),
        ),
        Err(ContextCompileErrorV0::CandidateByteCapacity {
            maximum: MAX_CONTEXT_CANDIDATE_BYTES_V0,
            ..
        })
    ));

    let large_scope = (0..MAX_CONTEXT_SCOPE_PREDICATES_V0)
        .map(|index| {
            ContextScopePredicateV0::new(&format!("scope_{index}"), &"\0".repeat(MAX_SCALAR_BYTES))
                .unwrap()
        })
        .collect();
    assert!(matches!(
        compile_context_v0(
            &admission(),
            &request_with(
                EPOCH_END_NS,
                MAX_CONTEXT_OUTPUT_BYTES_V0,
                large_scope,
                vec![fact("empty", "missing", false)],
            ),
        ),
        Err(ContextCompileErrorV0::MetadataByteCapacity {
            maximum: MAX_CONTEXT_OUTPUT_BYTES_V0,
            ..
        })
    ));
}

#[test]
fn canonical_request_order_produces_frozen_exact_json() {
    let state = base_state(true);
    let first = compile_context_v0(&state, &two_fact_request(MAX_CONTEXT_OUTPUT_BYTES_V0)).unwrap();

    let mut reversed_scope = scope();
    reversed_scope.reverse();
    let reversed = request_with(
        EPOCH_END_NS,
        MAX_CONTEXT_OUTPUT_BYTES_V0,
        reversed_scope,
        vec![
            fact("error-rate", "error_rate_ppm", true),
            fact("p99-latency", "p99_latency_ms", true),
        ],
    );
    let second = compile_context_v0(&state, &reversed).unwrap();

    assert_eq!(first, second);
    assert_eq!(first.candidate_count(), 2);
    assert_eq!(first.included_count(), 2);
    assert_eq!(first.omitted_count(), 0);
    assert!(first.source_finalized());
    assert!(first.query_closed());
    assert_eq!(
        first.as_bytes(),
        include_str!("golden/projected-change-context-v0.json")
            .trim_end()
            .as_bytes()
    );

    let text = std::str::from_utf8(first.as_bytes()).unwrap();
    assert!(text.contains("\"relation\":\"strictly_preceding_not_causal\""));
    assert!(text.contains("\"value\":32000"));
    assert!(text.contains("\"value\":720"));
    assert!(text.contains("deploy-near"));
    assert!(!text.contains("decoy"));
    assert!(!text.contains("deploy-near-low-sequence"));
    assert!(text.find("error-rate").unwrap() < text.find("p99-latency").unwrap());
}

#[test]
fn exact_budget_is_atomic_and_reports_every_omission() {
    let state = base_state(true);
    let minimum = match compile_context_v0(&state, &two_fact_request(1)).unwrap_err() {
        ContextCompileErrorV0::BudgetTooSmall { minimum, actual: 1 } => minimum,
        other => panic!("unexpected error: {other:?}"),
    };
    let metadata_only = compile_context_v0(&state, &two_fact_request(minimum)).unwrap();
    assert_eq!(metadata_only.as_bytes().len(), minimum);
    assert_eq!(metadata_only.candidate_count(), 2);
    assert_eq!(metadata_only.included_count(), 0);
    assert_eq!(metadata_only.omitted_count(), 2);

    let mut first_fit = None;
    let mut second_fit = None;
    for budget in minimum..=MAX_CONTEXT_OUTPUT_BYTES_V0 {
        let response = compile_context_v0(&state, &two_fact_request(budget)).unwrap();
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
        compile_context_v0(&state, &two_fact_request(first_fit - 1))
            .unwrap()
            .included_count(),
        0
    );
    let one = compile_context_v0(&state, &two_fact_request(first_fit)).unwrap();
    assert_eq!(one.as_bytes().len(), first_fit);
    assert_eq!((one.included_count(), one.omitted_count()), (1, 1));
    assert_eq!(
        compile_context_v0(&state, &two_fact_request(second_fit - 1))
            .unwrap()
            .included_count(),
        1
    );
    let two = compile_context_v0(&state, &two_fact_request(second_fit)).unwrap();
    assert_eq!(two.as_bytes().len(), second_fit);
    assert_eq!((two.included_count(), two.omitted_count()), (2, 0));
}

#[test]
fn query_and_evidence_boundaries_fail_closed() {
    let state = base_state(true);
    assert_eq!(
        compile_context_v0(
            &state,
            &request_with(
                59 * SECOND_NS,
                MAX_CONTEXT_OUTPUT_BYTES_V0,
                scope(),
                vec![fact("one", "error_rate_ppm", false)],
            )
        ),
        Err(ContextCompileErrorV0::QueryOutsideEpoch)
    );

    let mut future = base_state(false);
    accept(
        &mut future,
        &metric_event(
            100,
            180 * SECOND_NS,
            "error_rate_ppm",
            ScalarValueV0::from_u64(99_000),
        ),
    );
    assert_eq!(
        compile_context_v0(
            &future,
            &request_with(
                170 * SECOND_NS,
                MAX_CONTEXT_OUTPUT_BYTES_V0,
                scope(),
                vec![fact("one", "error_rate_ppm", false)],
            )
        ),
        Err(ContextCompileErrorV0::HistoricalAsOfUnavailable)
    );

    let expired_fact = ProjectedChangeFactSpecV0::new(
        ContextFactIdV0::new("expired").unwrap(),
        change_config(80),
        "error_rate_ppm",
        20 * SECOND_NS as u64,
        None,
    )
    .unwrap();
    assert_eq!(
        compile_context_v0(
            &state,
            &request_with(
                190 * SECOND_NS,
                MAX_CONTEXT_OUTPUT_BYTES_V0,
                scope(),
                vec![expired_fact],
            )
        ),
        Err(ContextCompileErrorV0::RequestedEvidenceExpired)
    );
}

#[test]
fn scope_contract_is_checked_for_each_supporting_role() {
    let state = base_state(true);
    let wrong_scope = vec![ContextScopePredicateV0::new("region_id", "region-a").unwrap()];
    assert_eq!(
        compile_context_v0(
            &state,
            &request_with(
                EPOCH_END_NS,
                MAX_CONTEXT_OUTPUT_BYTES_V0,
                wrong_scope,
                vec![fact("one", "error_rate_ppm", false)],
            )
        ),
        Err(ContextCompileErrorV0::ScopeContract {
            role: ContextEvidenceRoleV0::Previous,
            issue: ContextScopeIssueV0::Unequal,
        })
    );

    let missing_scope = vec![ContextScopePredicateV0::new("missing", "value").unwrap()];
    assert_eq!(
        compile_context_v0(
            &state,
            &request_with(
                EPOCH_END_NS,
                MAX_CONTEXT_OUTPUT_BYTES_V0,
                missing_scope,
                vec![fact("one", "error_rate_ppm", false)],
            )
        ),
        Err(ContextCompileErrorV0::ScopeContract {
            role: ContextEvidenceRoleV0::Previous,
            issue: ContextScopeIssueV0::Missing,
        })
    );

    let mut current_mismatch = admission();
    accept(
        &mut current_mismatch,
        &metric_event(
            1,
            130 * SECOND_NS,
            "error_rate_ppm",
            ScalarValueV0::from_u64(4_000),
        ),
    );
    accept(
        &mut current_mismatch,
        &metric_event_with_region(
            2,
            160 * SECOND_NS,
            "error_rate_ppm",
            ScalarValueV0::from_u64(32_000),
            ScalarValueV0::utf8("region-a").unwrap(),
        ),
    );
    assert_eq!(
        compile_context_v0(
            &current_mismatch,
            &request_with(
                EPOCH_END_NS,
                MAX_CONTEXT_OUTPUT_BYTES_V0,
                scope(),
                vec![fact("one", "error_rate_ppm", false)],
            )
        ),
        Err(ContextCompileErrorV0::ScopeContract {
            role: ContextEvidenceRoleV0::Current,
            issue: ContextScopeIssueV0::Unequal,
        })
    );

    let mut non_utf8 = admission();
    accept(
        &mut non_utf8,
        &metric_event_with_region(
            1,
            130 * SECOND_NS,
            "error_rate_ppm",
            ScalarValueV0::from_u64(4_000),
            ScalarValueV0::from_u64(7),
        ),
    );
    accept(
        &mut non_utf8,
        &metric_event(
            2,
            160 * SECOND_NS,
            "error_rate_ppm",
            ScalarValueV0::from_u64(32_000),
        ),
    );
    assert!(matches!(
        compile_context_v0(
            &non_utf8,
            &request_with(
                EPOCH_END_NS,
                MAX_CONTEXT_OUTPUT_BYTES_V0,
                scope(),
                vec![fact("one", "error_rate_ppm", false)],
            )
        ),
        Err(ContextCompileErrorV0::ScopeContract {
            role: ContextEvidenceRoleV0::Previous,
            issue: ContextScopeIssueV0::ExpectedUtf8 { .. },
        })
    ));

    let mut preceding_mismatch = admission();
    accept(
        &mut preceding_mismatch,
        &deployment_event_with_region(
            1,
            159 * SECOND_NS,
            KEY,
            "wrong-scope",
            ScalarValueV0::utf8("region-a").unwrap(),
        ),
    );
    accept(
        &mut preceding_mismatch,
        &metric_event(
            2,
            130 * SECOND_NS,
            "error_rate_ppm",
            ScalarValueV0::from_u64(4_000),
        ),
    );
    accept(
        &mut preceding_mismatch,
        &metric_event(
            3,
            160 * SECOND_NS,
            "error_rate_ppm",
            ScalarValueV0::from_u64(32_000),
        ),
    );
    assert_eq!(
        compile_context_v0(
            &preceding_mismatch,
            &request_with(
                EPOCH_END_NS,
                MAX_CONTEXT_OUTPUT_BYTES_V0,
                scope(),
                vec![fact("one", "error_rate_ppm", true)],
            )
        ),
        Err(ContextCompileErrorV0::ScopeContract {
            role: ContextEvidenceRoleV0::Preceding,
            issue: ContextScopeIssueV0::Unequal,
        })
    );
}

#[test]
fn all_scalar_kinds_and_json_escaping_are_deterministic() {
    let mut state = admission();
    let series = [
        (
            "a-null",
            ScalarValueV0::from_bool(true),
            ScalarValueV0::null(),
        ),
        (
            "b-bool",
            ScalarValueV0::from_bool(false),
            ScalarValueV0::from_bool(true),
        ),
        (
            "c-i64",
            ScalarValueV0::from_i64(-1),
            ScalarValueV0::from_i64(i64::MIN),
        ),
        (
            "d-u64",
            ScalarValueV0::from_u64(1),
            ScalarValueV0::from_u64(u64::MAX),
        ),
        (
            "e-utf8",
            ScalarValueV0::utf8("plain").unwrap(),
            ScalarValueV0::utf8("quote\" slash\\ newline\n tab\t nul\0 snowman ☃").unwrap(),
        ),
        (
            "f-bytes",
            ScalarValueV0::bytes(&[0]).unwrap(),
            ScalarValueV0::bytes(&[0x00, 0x0f, 0x10, 0xff]).unwrap(),
        ),
    ];
    let mut sequence = 1;
    for (name, previous, current) in series {
        accept(
            &mut state,
            &metric_event(sequence, 130 * SECOND_NS, name, previous),
        );
        sequence += 1;
        accept(
            &mut state,
            &metric_event(sequence, 160 * SECOND_NS, name, current),
        );
        sequence += 1;
    }
    assert_eq!(
        state.finalize_source(&source("source-v0"), EPOCH_END_NS),
        FinalizeOutcomeV0::Finalized
    );
    let facts = ["a-null", "b-bool", "c-i64", "d-u64", "e-utf8", "f-bytes"]
        .into_iter()
        .map(|name| fact(name, name, false))
        .collect();
    let response = compile_context_v0(
        &state,
        &request_with(EPOCH_END_NS, MAX_CONTEXT_OUTPUT_BYTES_V0, scope(), facts),
    )
    .unwrap();
    assert_eq!(response.included_count(), 6);
    let text = std::str::from_utf8(response.as_bytes()).unwrap();
    for expected in [
        "{\"kind\":\"null\",\"value\":null}",
        "{\"kind\":\"bool\",\"value\":true}",
        "{\"kind\":\"i64\",\"value\":-9223372036854775808}",
        "{\"kind\":\"u64\",\"value\":18446744073709551615}",
        "quote\\\" slash\\\\ newline\\n tab\\t nul\\u0000 snowman ☃",
        "{\"kind\":\"bytes\",\"value\":\"000f10ff\"}",
    ] {
        assert!(text.contains(expected), "missing {expected}");
    }
}

#[test]
fn open_state_reports_only_local_query_status() {
    let state = base_state(false);
    let response =
        compile_context_v0(&state, &two_fact_request(MAX_CONTEXT_OUTPUT_BYTES_V0)).unwrap();
    assert!(!response.source_finalized());
    assert!(!response.query_closed());
    let text = std::str::from_utf8(response.as_bytes()).unwrap();
    assert!(text.contains("\"source_finalized\":false,\"query_closed\":false"));
}

#[test]
fn diagnostics_do_not_expose_configured_or_event_content() {
    let probe_scope = ContextScopePredicateV0::new("probe_field", "probe-value").unwrap();
    let probe_fact = fact("probe-fact", "probe-discriminator", false);
    let request = request_with(
        EPOCH_END_NS,
        MAX_CONTEXT_OUTPUT_BYTES_V0,
        vec![probe_scope],
        vec![probe_fact],
    );
    let response = compile_context_v0(&base_state(true), &request).unwrap();
    for diagnostic in [
        format!("{request:?}"),
        format!("{:?}", request.scope()[0]),
        format!("{:?}", request.facts()[0]),
        format!("{response:?}"),
        format!("{:?}", ContextCompileErrorV0::HistoricalAsOfUnavailable),
    ] {
        assert!(!diagnostic.contains("probe"), "leaked: {diagnostic}");
        assert!(
            !diagnostic.contains("decant://event"),
            "leaked: {diagnostic}"
        );
    }
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
            FixtureRecordV0::Event(event) => {
                state.apply_event(&event).unwrap();
            }
            FixtureRecordV0::EndOfInput(control) => assert_eq!(
                state.finalize_source(control.source_id(), control.finalized_through_ns()),
                FinalizeOutcomeV0::Finalized
            ),
        }
    }
    state
}

fn sre_fact(id: &str, discriminator: &str) -> ProjectedChangeFactSpecV0 {
    let retention =
        EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, 25 * MINUTE_NS as u64)
            .unwrap();
    let change = ProjectedFieldChangeConfigV0::new(
        PROJECTED_FIELD_CHANGE_VERSION_V0,
        retention,
        schema("metric-sample-v0"),
        "metric_name",
        "metric_value",
        ProjectedFieldChangeLimitsV0::maximum(),
    )
    .unwrap();
    let preceding = PrecedingEvidenceSpecV0::new(
        retention,
        schema("deployment-v0"),
        15 * MINUTE_NS as u64,
        "deployment_id",
    )
    .unwrap();
    ProjectedChangeFactSpecV0::new(
        ContextFactIdV0::new(id).unwrap(),
        change,
        discriminator,
        15 * MINUTE_NS as u64,
        Some(preceding),
    )
    .unwrap()
}

#[test]
fn primary_sre_fixture_compiles_metric_changes_and_exact_key_marker() {
    let state = scenario_state("sre-v0-primary");
    let request = ContextCompileRequestV0::new(
        0,
        PRIMARY_END_NS,
        10 * MINUTE_NS as u64,
        MAX_CONTEXT_OUTPUT_BYTES_V0,
        KEY,
        scope(),
        vec![
            sre_fact("error-rate", "error_rate_ppm"),
            sre_fact("p99-latency", "p99_latency_ms"),
        ],
    )
    .unwrap();
    let response = compile_context_v0(&state, &request).unwrap();
    assert_eq!(
        (response.candidate_count(), response.included_count()),
        (2, 2)
    );
    let text = std::str::from_utf8(response.as_bytes()).unwrap();
    assert!(text.contains("\"value\":4000"));
    assert!(text.contains("\"value\":32000"));
    assert!(text.contains("\"value\":180"));
    assert!(text.contains("\"value\":720"));
    assert!(text.contains("payments-v2-region-b-09"));
    assert!(!text.contains("payments-v2-region-a-09"));
    assert!(!text.contains("checkout-v1-region-b-09"));
    assert!(response.source_finalized());
    assert!(response.query_closed());
}

#[test]
fn no_regression_sre_fixture_emits_no_change_candidates() {
    let state = scenario_state("sre-v0-no-regression");
    let request = ContextCompileRequestV0::new(
        0,
        PRIMARY_END_NS,
        10 * MINUTE_NS as u64,
        MAX_CONTEXT_OUTPUT_BYTES_V0,
        KEY,
        scope(),
        vec![
            sre_fact("error-rate", "error_rate_ppm"),
            sre_fact("p99-latency", "p99_latency_ms"),
        ],
    )
    .unwrap();
    let response = compile_context_v0(&state, &request).unwrap();
    assert_eq!(
        (
            response.candidate_count(),
            response.included_count(),
            response.omitted_count()
        ),
        (0, 0, 0)
    );
}

#[test]
fn maximum_public_constants_remain_frozen() {
    assert_eq!(MAX_CONTEXT_LOOKBACK_NS_V0, 600_000_000_000);
    assert_eq!(MAX_CONTEXT_EVIDENCE_REACHBACK_NS_V0, 900_000_000_000);
    assert_eq!(MAX_CONTEXT_OUTPUT_BYTES_V0, 16_384);
    assert_eq!(MAX_CONTEXT_SCOPE_PREDICATES_V0, 8);
    assert_eq!(MAX_CONTEXT_FACT_REQUESTS_V0, 4_096);
}
