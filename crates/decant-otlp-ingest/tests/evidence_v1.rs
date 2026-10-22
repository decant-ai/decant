use decant_compiler::evidence::{EvidenceRequestV1, compile_evidence_v1};
use decant_ir::{SourceIdV0, SourcePositionKindV0};
use decant_jsonl::JsonlLimitsV0;
use decant_otlp_ingest::{OtlpJsonlIngestConfigV0, ingest_otlp_jsonl_evidence_v1};
use std::io::Cursor;

const CAPTURE: &str = include_str!("../../../examples/evidence/trace-capture-v1.jsonl");

fn ingest(input: &[u8]) -> decant_otlp_ingest::OtlpJsonlIngestResultV0 {
    let config = OtlpJsonlIngestConfigV0::new(
        0,
        SourceIdV0::new("otel-evidence-test").unwrap(),
        JsonlLimitsV0::maximum(),
    )
    .unwrap();
    ingest_otlp_jsonl_evidence_v1(Cursor::new(input), &config).unwrap()
}

fn check(input: &[u8]) {
    let result = ingest(input);
    assert_eq!(result.usage().accepted_events(), 4);
    assert_eq!(result.usage().late_rejections(), 0);
    let request = EvidenceRequestV1::default();
    let report = compile_evidence_v1(result.admission(), &request).unwrap();
    assert_eq!(report.services.len(), 1);
    assert_eq!(report.services[0].baseline.observed_server_spans, 2);
    assert_eq!(report.services[0].current.observed_server_spans, 2);
    assert_eq!(report.services[0].baseline.error_spans, 1);
    assert_eq!(report.services[0].current.error_spans, 1);
    assert_eq!(
        report.services[0].baseline.p95_duration_ns,
        Some(20_000_000)
    );
    assert_eq!(
        report.services[0].current.p95_duration_ns,
        Some(200_000_000)
    );
    for event in result.admission().accepted_events() {
        assert_eq!(
            event.source_position().kind(),
            SourcePositionKindV0::OtlpFile
        );
        assert_eq!(event.schema_id().as_str(), "sre-span-v1");
        assert!(
            !event
                .payload()
                .fields()
                .any(|(name, _)| name == "span.name" || name == "private.detail")
        );
    }
    let text = String::from_utf8(report.to_bytes().unwrap()).unwrap();
    assert!(!text.contains("synthetic-operation-not-for-output"));
    assert!(!text.contains("synthetic-private-detail"));
    let mut allowed = request;
    allowed.policy.allow_service_versions = true;
    assert_eq!(
        compile_evidence_v1(result.admission(), &allowed)
            .unwrap()
            .services[0]
            .observed_version_differs,
        Some(true)
    );
}

#[test]
fn capture_uses_production_recipe_and_preserves_coordinates() {
    check(CAPTURE.as_bytes());
}

#[test]
fn repeated_capture_span_folds_and_changed_repeat_fails_closed() {
    let last = format!("{}\n", CAPTURE.lines().last().unwrap());
    let duplicate = ingest(format!("{CAPTURE}{last}").as_bytes());
    let report = compile_evidence_v1(duplicate.admission(), &EvidenceRequestV1::default()).unwrap();
    assert_eq!(report.gaps.logical_duplicates_folded, 1);
    assert_eq!(report.services[0].current.observed_server_spans, 2);
    let conflict =
        ingest(format!("{CAPTURE}{}", last.replace("600200000000", "600300000000")).as_bytes());
    assert!(compile_evidence_v1(conflict.admission(), &EvidenceRequestV1::default()).is_err());
    let unspecified = ingest(CAPTURE.replace(",\"kind\":2", "").as_bytes());
    assert!(
        compile_evidence_v1(unspecified.admission(), &EvidenceRequestV1::default())
            .unwrap()
            .services
            .is_empty()
    );
}

#[test]
#[ignore = "explicit local Collector harness supplies its independently serialized capture"]
fn collector_exported_capture_v1() {
    let path =
        std::env::var_os("DECANT_TEST_COLLECTOR_CAPTURE").expect("explicit capture required");
    let file = std::fs::File::open(path).expect("capture readable");
    use std::io::Read;
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1).read_to_end(&mut bytes).unwrap();
    assert!(bytes.len() <= 1024 * 1024);
    check(&bytes);
}
