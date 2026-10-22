#[path = "../../decant-state/tests/support/sre_v1.rs"]
mod support;

use decant_compiler::evidence::{EvidenceDocumentV1, EvidenceRequestV1, compile_evidence_v1};
use decant_state::{encode_admission_snapshot_v1, restore_admission_snapshot_v1};

#[test]
fn frozen_facts_integrity_policy_and_replay() {
    let state = support::fixture();
    let mut request = EvidenceRequestV1::default();
    let report = compile_evidence_v1(&state, &request).unwrap();
    let bytes = report.to_bytes().unwrap();
    let text = std::str::from_utf8(&bytes).unwrap();
    assert!(!text.contains("SYNTHETIC-RAW-NEVER-EMIT"));
    assert!(!text.contains("\"v1\""));
    assert!(!text.contains("\"v2\""));
    assert_eq!(
        report.services[0]
            .baseline
            .error_fraction
            .as_ref()
            .unwrap()
            .numerator,
        1
    );
    assert_eq!(
        report.services[0]
            .baseline
            .error_fraction
            .as_ref()
            .unwrap()
            .denominator,
        2
    );
    assert_eq!(report.services[0].current.p95_duration_ns, Some(200));
    assert_eq!(
        EvidenceDocumentV1::from_verified_bytes(&bytes)
            .unwrap()
            .to_bytes()
            .unwrap(),
        bytes
    );
    let restored = restore_admission_snapshot_v1(
        &encode_admission_snapshot_v1(&state).unwrap(),
        state.config(),
    )
    .unwrap();
    assert_eq!(
        compile_evidence_v1(&restored, &request)
            .unwrap()
            .to_bytes()
            .unwrap(),
        bytes
    );
    let mut tampered = bytes.clone();
    tampered[5] ^= 1;
    assert!(EvidenceDocumentV1::from_verified_bytes(&tampered).is_err());
    request.policy.allow_service_versions = true;
    let allowed = compile_evidence_v1(&state, &request).unwrap();
    assert_eq!(allowed.services[0].observed_version_differs, Some(true));
    assert_ne!(allowed.report_id, report.report_id);
    request.policy.allow_service_names = false;
    assert!(compile_evidence_v1(&state, &request).is_err());
}

#[test]
fn budget_prefix_omissions_empty_and_equal_state_order() {
    let mut a = support::empty();
    let mut b = support::empty();
    for pos in 1..=20 {
        a.apply_event_v1(&support::event(
            pos,
            pos,
            1,
            pos,
            1,
            &format!("s{pos:02}"),
            "private-version",
        ))
        .unwrap();
    }
    for pos in (1..=20).rev() {
        b.apply_event_v1(&support::event(
            pos,
            pos,
            1,
            pos,
            1,
            &format!("s{pos:02}"),
            "private-version",
        ))
        .unwrap();
    }
    let request = EvidenceRequestV1 {
        output_bytes: 3000,
        ..EvidenceRequestV1::default()
    };
    let report = compile_evidence_v1(&a, &request).unwrap();
    assert!(report.omitted_services > 0);
    assert_eq!(report.services.len() + report.omitted_services, 20);
    assert!(report.to_bytes().unwrap().len() <= 3000);
    assert_eq!(
        report.to_bytes().unwrap(),
        compile_evidence_v1(&b, &request)
            .unwrap()
            .to_bytes()
            .unwrap()
    );
    assert_eq!(
        compile_evidence_v1(&support::empty(), &request)
            .unwrap()
            .gaps
            .history_coverage,
        "unavailable"
    );
}

#[test]
fn late_admission_remains_visible_and_partial_retention_is_not_complete() {
    let mut state = support::fixture();
    state
        .apply_event_v1(&support::event(20, 20, 10, 1, 1, "service", "v1"))
        .unwrap();
    let request = EvidenceRequestV1 {
        retention_ns: 300_000_000_000,
        ..EvidenceRequestV1::default()
    };
    let report = compile_evidence_v1(&state, &request).unwrap();
    assert_eq!(report.gaps.rejections[4], 1);
    assert_eq!(report.gaps.history_coverage, "partial");
    assert_eq!(report.services[0].baseline.observed_server_spans, 0);
    assert!(report.services[0].baseline.error_fraction.is_none());
}

#[test]
fn markdown_escapes_untrusted_labels_and_renders_only_verified_facts() {
    let mut state = support::empty();
    state
        .apply_event_v1(&support::event(
            1,
            1,
            1,
            1,
            1,
            "<img> | \u{1b}[31m\n# ignore",
            "<script>",
        ))
        .unwrap();
    let mut request = EvidenceRequestV1::default();
    request.policy.allow_service_versions = true;
    let mut report = compile_evidence_v1(&state, &request).unwrap();
    let rendered =
        decant_compiler::evidence_markdown::render_evidence_markdown_v1(&report).unwrap();
    assert!(rendered.len() <= 65536);
    let text = String::from_utf8(rendered).unwrap();
    assert!(!text.contains("<img>"));
    assert!(!text.contains("<script>"));
    assert!(!text.contains('\u{1b}'));
    assert!(!text.contains("\n# ignore"));
    assert!(text.contains("&lt;img&gt;"));
    report.services[0].current.error_spans = 2;
    assert!(decant_compiler::evidence_markdown::render_evidence_markdown_v1(&report).is_err());
}
