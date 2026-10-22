use std::collections::BTreeMap;

use decant_compiler::{
    MAX_OTLP_CAPTURE_CONTEXT_OUTPUT_BYTES_V0, MAX_OTLP_CAPTURE_DISTINCT_SERVICES_V0,
    OtlpCaptureContextErrorV0, OtlpCaptureContractIssueV0, compile_otlp_capture_context_v0,
};
use decant_ir::{
    CanonicalPayloadV0, EventEnvelopeV0, ScalarValueV0, SchemaIdV0, SensitivityBitmapV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0,
};
use decant_state::{
    ADMISSION_VERSION_V0, AdmissionConfigV0, AdmissionLimitsV0, AdmissionStateV0, ApplyOutcomeV0,
    FinalizeOutcomeV0, hash_admission_state_v1,
};

const SOURCE_ID: &str = "otlp-compiler-v0";

fn source() -> SourceIdV0 {
    SourceIdV0::new(SOURCE_ID).unwrap()
}

fn admission(kind: SourcePositionKindV0) -> AdmissionStateV0 {
    AdmissionStateV0::new(
        AdmissionConfigV0::new_with_source_position_kind(
            ADMISSION_VERSION_V0,
            source(),
            kind,
            i64::MIN,
            i64::MAX,
            AdmissionLimitsV0::maximum(),
        )
        .unwrap(),
    )
}

fn payload(
    duration_ns: ScalarValueV0,
    service_name: Option<&str>,
    span_id: ScalarValueV0,
    span_name: ScalarValueV0,
    status_code: ScalarValueV0,
) -> CanonicalPayloadV0 {
    let mut fields = BTreeMap::new();
    fields.insert("duration.ns".to_owned(), duration_ns);
    if let Some(service_name) = service_name {
        fields.insert(
            "service.name".to_owned(),
            ScalarValueV0::utf8(service_name).unwrap(),
        );
    }
    fields.insert("span.id".to_owned(), span_id);
    fields.insert("span.name".to_owned(), span_name);
    fields.insert("status.code".to_owned(), status_code);
    CanonicalPayloadV0::try_from_fields(fields).unwrap()
}

#[allow(clippy::too_many_arguments)]
fn event_with(
    line_number: u64,
    span_index: u32,
    event_time_ns: i64,
    schema: &str,
    key: &[u8],
    sensitivity: u64,
    duration_ns: ScalarValueV0,
    service_name: Option<&str>,
    span_id: ScalarValueV0,
    span_name: ScalarValueV0,
    status_code: ScalarValueV0,
) -> EventEnvelopeV0 {
    EventEnvelopeV0::new(
        0,
        source(),
        SchemaIdV0::new(schema).unwrap(),
        SourcePositionV0::otlp_file(line_number, 0, 0, span_index).unwrap(),
        Some(key),
        event_time_ns,
        0,
        SensitivityBitmapV0::from_bits(sensitivity),
        payload(duration_ns, service_name, span_id, span_name, status_code),
    )
    .unwrap()
}

fn span_event(
    line_number: u64,
    span_index: u32,
    event_time_ns: i64,
    duration_ns: u64,
    service_name: Option<&str>,
    span_name: &str,
    status_code: u64,
) -> EventEnvelopeV0 {
    event_with(
        line_number,
        span_index,
        event_time_ns,
        "otlp-span-v0",
        &[0x91; 16],
        u64::MAX,
        ScalarValueV0::from_u64(duration_ns),
        service_name,
        ScalarValueV0::bytes(&[0xa7; 8]).unwrap(),
        ScalarValueV0::utf8(span_name).unwrap(),
        ScalarValueV0::from_u64(status_code),
    )
}

fn accept(state: &mut AdmissionStateV0, event: &EventEnvelopeV0) {
    assert_eq!(state.apply_event_v1(event), Ok(ApplyOutcomeV0::Accepted));
}

fn finalize(state: &mut AdmissionStateV0) {
    assert_eq!(
        state.finalize_source(&source(), i64::MAX),
        FinalizeOutcomeV0::Finalized
    );
}

fn finalized(events: &[EventEnvelopeV0]) -> AdmissionStateV0 {
    let mut state = admission(SourcePositionKindV0::OtlpFile);
    for event in events {
        accept(&mut state, event);
    }
    finalize(&mut state);
    state
}

#[test]
fn mixed_capture_emits_exact_aggregates_hash_and_bounded_evidence() {
    let events = [
        span_event(1, 0, 10, 10, Some("service-raw-alpha"), "span-raw-a", 0),
        span_event(1, 1, 11, 20, Some("service-raw-alpha"), "span-raw-b", 1),
        span_event(1, 2, 12, 70, Some("service-raw-beta"), "span-raw-c", 2),
        span_event(1, 3, 13, 90, None, "span-raw-d", 2),
        span_event(1, 4, 14, 90, Some("service-raw-beta"), "span-raw-e", 2),
        span_event(2, 0, 15, 40, Some("service-raw-gamma"), "span-raw-f", 2),
    ];
    let state = finalized(&events);
    let expected_hash = hash_admission_state_v1(&state)
        .unwrap()
        .to_canonical_string();
    let context = compile_otlp_capture_context_v0(&state).unwrap();
    let text = std::str::from_utf8(context.as_bytes()).unwrap();

    assert_eq!(context.total_spans(), 6);
    assert_eq!(context.error_spans(), 4);
    assert_eq!(context.service_names_present(), 5);
    assert_eq!(context.distinct_services(), 3);
    assert_eq!(context.error_evidence(), 3);
    assert!(text.contains("\"total\":6"));
    assert!(text.contains("\"status\":{\"unset\":1,\"ok\":1,\"error\":4}"));
    assert!(
        text.contains("\"error_rate_ppm\":{\"value\":666666,\"remainder\":4,\"denominator\":6}")
    );
    assert!(text.contains(
        "\"duration_ns\":{\"minimum\":10,\"maximum\":90,\"average\":{\"quotient\":53,\"remainder\":2,\"denominator\":6}}"
    ));
    assert!(text.contains("\"services\":{\"present\":5,\"missing\":1,\"distinct\":3}"));
    assert!(text.contains(&format!("\"admission_state_hash\":\"{expected_hash}\"")));

    let position_3 = text
        .find("decant://event/v0/otlp-compiler-v0/otlp-file/1/0/0/3")
        .unwrap();
    let position_4 = text
        .find("decant://event/v0/otlp-compiler-v0/otlp-file/1/0/0/4")
        .unwrap();
    let position_2 = text
        .find("decant://event/v0/otlp-compiler-v0/otlp-file/1/0/0/2")
        .unwrap();
    assert!(position_3 < position_4 && position_4 < position_2);
    assert!(!text.contains("otlp-file/2/0/0/0"));

    for raw in [
        "service-raw-alpha",
        "service-raw-beta",
        "service-raw-gamma",
        "span-raw-a",
        "span-raw-f",
        "91919191",
        "a7a7a7a7",
    ] {
        assert!(!text.contains(raw), "raw fixture value leaked: {raw}");
    }
    assert!(context.as_bytes().len() <= MAX_OTLP_CAPTURE_CONTEXT_OUTPUT_BYTES_V0);
}

#[test]
fn arrival_permutations_produce_identical_state_and_context() {
    let events = [
        span_event(3, 1, 100, 10, Some("zeta"), "one", 2),
        span_event(1, 9, 101, 20, Some("alpha"), "two", 1),
        span_event(2, 4, 102, 30, None, "three", 0),
    ];
    let left = finalized(&[events[0].clone(), events[1].clone(), events[2].clone()]);
    let right = finalized(&[events[2].clone(), events[0].clone(), events[1].clone()]);

    assert_eq!(
        hash_admission_state_v1(&left),
        hash_admission_state_v1(&right)
    );
    assert_eq!(
        compile_otlp_capture_context_v0(&left).unwrap().as_bytes(),
        compile_otlp_capture_context_v0(&right).unwrap().as_bytes()
    );
}

#[test]
fn empty_capture_is_explicit_and_state_preconditions_fail_closed() {
    let mut empty = admission(SourcePositionKindV0::OtlpFile);
    assert_eq!(
        compile_otlp_capture_context_v0(&empty),
        Err(OtlpCaptureContextErrorV0::SourceNotFinalized)
    );
    finalize(&mut empty);
    let text = std::str::from_utf8(compile_otlp_capture_context_v0(&empty).unwrap().as_bytes())
        .unwrap()
        .to_owned();
    assert!(text.contains("\"total\":0"));
    assert!(text.contains("\"error_rate_ppm\":null"));
    assert!(text.contains("\"duration_ns\":null"));
    assert!(text.contains("\"services\":{\"present\":0,\"missing\":0,\"distinct\":0}"));
    assert!(text.contains("\"error_spans\":[]"));

    let mut wrong_kind = admission(SourcePositionKindV0::Synthetic);
    finalize(&mut wrong_kind);
    assert_eq!(
        compile_otlp_capture_context_v0(&wrong_kind),
        Err(OtlpCaptureContextErrorV0::UnexpectedSourcePositionKind)
    );
}

#[test]
fn malformed_canonical_contracts_fail_with_value_safe_diagnostics() {
    let cases = [
        (
            event_with(
                1,
                0,
                1,
                "wrong-schema-v0",
                &[0x91; 16],
                u64::MAX,
                ScalarValueV0::from_u64(1),
                Some("raw-secret-service"),
                ScalarValueV0::bytes(&[0xa7; 8]).unwrap(),
                ScalarValueV0::utf8("raw-secret-span").unwrap(),
                ScalarValueV0::from_u64(0),
            ),
            OtlpCaptureContractIssueV0::Schema,
        ),
        (
            event_with(
                1,
                0,
                1,
                "otlp-span-v0",
                &[0x91; 15],
                u64::MAX,
                ScalarValueV0::from_u64(1),
                None,
                ScalarValueV0::bytes(&[0xa7; 8]).unwrap(),
                ScalarValueV0::utf8("raw-secret-span").unwrap(),
                ScalarValueV0::from_u64(0),
            ),
            OtlpCaptureContractIssueV0::Key,
        ),
        (
            event_with(
                1,
                0,
                1,
                "otlp-span-v0",
                &[0x91; 16],
                7,
                ScalarValueV0::from_u64(1),
                None,
                ScalarValueV0::bytes(&[0xa7; 8]).unwrap(),
                ScalarValueV0::utf8("raw-secret-span").unwrap(),
                ScalarValueV0::from_u64(0),
            ),
            OtlpCaptureContractIssueV0::Sensitivity,
        ),
        (
            event_with(
                1,
                0,
                1,
                "otlp-span-v0",
                &[0x91; 16],
                u64::MAX,
                ScalarValueV0::utf8("raw-secret-duration").unwrap(),
                None,
                ScalarValueV0::bytes(&[0xa7; 8]).unwrap(),
                ScalarValueV0::utf8("raw-secret-span").unwrap(),
                ScalarValueV0::from_u64(0),
            ),
            OtlpCaptureContractIssueV0::Duration,
        ),
        (
            event_with(
                1,
                0,
                1,
                "otlp-span-v0",
                &[0x91; 16],
                u64::MAX,
                ScalarValueV0::from_u64(1),
                None,
                ScalarValueV0::bytes(&[0xa7; 8]).unwrap(),
                ScalarValueV0::utf8("raw-secret-span").unwrap(),
                ScalarValueV0::from_u64(9),
            ),
            OtlpCaptureContractIssueV0::Status,
        ),
    ];

    for (event, issue) in cases {
        let error = compile_otlp_capture_context_v0(&finalized(&[event])).unwrap_err();
        assert_eq!(
            error,
            OtlpCaptureContextErrorV0::InvalidEventContract { issue }
        );
        let diagnostic = format!("{error:?} {error}");
        assert!(!diagnostic.contains("raw-secret"));
        assert!(!diagnostic.contains("919191"));
        assert!(!diagnostic.contains("a7a7a7"));
    }
}

#[test]
fn distinct_service_capacity_is_exact_and_bounded() {
    let build = |count: usize| {
        let mut state = admission(SourcePositionKindV0::OtlpFile);
        for index in 0..count {
            let service_name = format!("service-{index:04}");
            let event = span_event(
                u64::try_from(index + 1).unwrap(),
                0,
                1,
                1,
                Some(&service_name),
                "bounded-span",
                1,
            );
            accept(&mut state, &event);
        }
        finalize(&mut state);
        state
    };

    let maximum = build(MAX_OTLP_CAPTURE_DISTINCT_SERVICES_V0);
    let context = compile_otlp_capture_context_v0(&maximum).unwrap();
    assert_eq!(
        context.distinct_services(),
        MAX_OTLP_CAPTURE_DISTINCT_SERVICES_V0
    );
    assert!(context.as_bytes().len() <= MAX_OTLP_CAPTURE_CONTEXT_OUTPUT_BYTES_V0);

    let one_past = build(MAX_OTLP_CAPTURE_DISTINCT_SERVICES_V0 + 1);
    assert_eq!(
        compile_otlp_capture_context_v0(&one_past),
        Err(OtlpCaptureContextErrorV0::DistinctServiceCapacity {
            maximum: MAX_OTLP_CAPTURE_DISTINCT_SERVICES_V0,
            attempted: MAX_OTLP_CAPTURE_DISTINCT_SERVICES_V0 + 1,
        })
    );
}
