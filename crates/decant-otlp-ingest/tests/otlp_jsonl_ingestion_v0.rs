use std::io::Cursor;

use decant_ir::SourceIdV0;
use decant_jsonl::JsonlLimitsV0;
use decant_otlp_ingest::{
    OTLP_JSONL_INGEST_VERSION_V0, OtlpJsonlIngestConfigV0, OtlpJsonlIngestErrorV0,
    ingest_otlp_jsonl_v0,
};
use decant_state::{RejectionReasonV0, encode_admission_snapshot_v1, hash_admission_state_v1};

fn config() -> OtlpJsonlIngestConfigV0 {
    OtlpJsonlIngestConfigV0::new(
        OTLP_JSONL_INGEST_VERSION_V0,
        SourceIdV0::new("external-capture-v0").unwrap(),
        JsonlLimitsV0::maximum(),
    )
    .unwrap()
}

fn span(trace_suffix: u8, start: i64, end: i64, status: u8, extra: &str) -> String {
    format!(
        "{{\"traceId\":\"000000000000000000000000000000{trace_suffix:02x}\",\"spanId\":\"00000000000000{trace_suffix:02x}\",\"name\":\"operation-{trace_suffix}\",\"startTimeUnixNano\":\"{start}\",\"endTimeUnixNano\":\"{end}\",\"status\":{{\"code\":{status},\"message\":\"{extra}\"}},\"attributes\":[{{}}],\"events\":[{{}}],\"links\":[{{}}],\"droppedAttributesCount\":1,\"droppedEventsCount\":2,\"droppedLinksCount\":3}}"
    )
}

fn line(service: &str, spans: &[String]) -> String {
    format!(
        "{{\"resourceSpans\":[{{\"resource\":{{\"attributes\":[{{\"key\":\"service.name\",\"value\":{{\"stringValue\":\"{service}\"}}}},{{\"key\":\"ignored\",\"value\":{{\"stringValue\":\"secret\"}}}}],\"droppedAttributesCount\":4}},\"scopeSpans\":[{{\"scope\":{{\"attributes\":[{{}}],\"droppedAttributesCount\":5}},\"spans\":[{}]}}]}}]}}\n",
        spans.join(",")
    )
}

#[test]
fn multi_line_path_reconciles_every_boundary_and_omission() {
    let input = format!(
        "{}{}",
        line(
            "frontend-distinctive",
            &[
                span(1, 100_000_000_000, 101_000_000_000, 0, ""),
                span(2, 101_000_000_000, 103_000_000_000, 2, "omitted"),
            ]
        ),
        line(
            "checkout-distinctive",
            &[span(3, 102_000_000_000, 106_000_000_000, 1, "")]
        )
    );
    let result = ingest_otlp_jsonl_v0(Cursor::new(input.as_bytes()), &config()).unwrap();
    let usage = result.usage();
    assert_eq!(usage.input_records(), 2);
    assert_eq!(usage.input_bytes(), input.len() as u64);
    assert_eq!(usage.canonical_events(), 3);
    assert_eq!(usage.enqueued_events(), 3);
    assert_eq!(usage.received_events(), 3);
    assert_eq!(usage.accepted_events(), 3);
    assert_eq!(usage.duplicate_noops(), 0);
    assert_eq!(usage.late_rejections(), 0);
    assert_eq!(usage.decode().decoded_spans(), 3);
    assert_eq!(usage.decode().resource_attributes_omitted(), 2);
    assert_eq!(usage.decode().scope_attributes_omitted(), 2);
    assert_eq!(usage.decode().span_attributes_omitted(), 3);
    assert_eq!(usage.decode().span_events_omitted(), 3);
    assert_eq!(usage.decode().span_links_omitted(), 3);
    assert_eq!(usage.decode().status_messages_omitted(), 1);
    assert_eq!(usage.decode().resource_dropped_attributes_reported(), 8);
    assert_eq!(usage.decode().scope_dropped_attributes_reported(), 10);
    assert_eq!(usage.decode().span_dropped_attributes_reported(), 3);
    assert_eq!(usage.decode().span_dropped_events_reported(), 6);
    assert_eq!(usage.decode().span_dropped_links_reported(), 9);
    let transport = usage.transport().unwrap();
    assert_eq!(transport.high_water_events(), 1);
    assert!(transport.high_water_accounted_bytes() > 0);
    assert!(result.admission().is_finalized());
}

#[test]
fn capture_order_lateness_is_explicit_and_replay_is_exact() {
    let input = format!(
        "{}{}",
        line("later", &[span(1, 100_000_000_000, 100_000_000_100, 0, "")]),
        line("earlier", &[span(2, 1, 2, 2, "")])
    );
    let first = ingest_otlp_jsonl_v0(Cursor::new(input.as_bytes()), &config()).unwrap();
    let second = ingest_otlp_jsonl_v0(Cursor::new(input.as_bytes()), &config()).unwrap();
    assert_eq!(first.usage(), second.usage());
    assert_eq!(first.usage().accepted_events(), 1);
    assert_eq!(first.usage().late_rejections(), 1);
    assert_eq!(
        first
            .admission()
            .rejection_summary()
            .range(RejectionReasonV0::Late)
            .count(),
        1
    );
    assert_eq!(
        encode_admission_snapshot_v1(first.admission()).unwrap(),
        encode_admission_snapshot_v1(second.admission()).unwrap()
    );
    assert_eq!(
        hash_admission_state_v1(first.admission()).unwrap(),
        hash_admission_state_v1(second.admission()).unwrap()
    );
}

#[test]
fn framing_decode_signal_and_epoch_failures_return_no_success() {
    let malformed = b"{not-json}\n";
    assert!(matches!(
        ingest_otlp_jsonl_v0(Cursor::new(malformed), &config()),
        Err(OtlpJsonlIngestErrorV0::Decode { line_number: 1, .. })
    ));
    assert!(matches!(
        ingest_otlp_jsonl_v0(Cursor::new(b"{\"resourceLogs\":[]}\n"), &config()),
        Err(OtlpJsonlIngestErrorV0::Decode { line_number: 1, .. })
    ));
    assert!(matches!(
        ingest_otlp_jsonl_v0(Cursor::new(b"{}"), &config()),
        Err(OtlpJsonlIngestErrorV0::Jsonl(_))
    ));

    let extreme = line("extreme", &[span(9, i64::MAX, i64::MAX, 0, "")]);
    assert!(matches!(
        ingest_otlp_jsonl_v0(Cursor::new(extreme.as_bytes()), &config()),
        Err(OtlpJsonlIngestErrorV0::UnexpectedAdmissionOutcome {
            reason: RejectionReasonV0::OutsideEpoch,
            ..
        })
    ));
}

#[test]
fn errors_and_result_debug_do_not_expose_source_values() {
    let secret = "distinctive-secret-operation";
    let input = format!(
        "{{\"resourceSpans\":[{{\"scopeSpans\":[{{\"spans\":[{{\"name\":\"{secret}\"}}]}}]}}]}}\n"
    );
    let error = ingest_otlp_jsonl_v0(Cursor::new(input.as_bytes()), &config()).unwrap_err();
    let diagnostics = format!("{error:?} {error}");
    assert!(!diagnostics.contains(secret));

    let empty = ingest_otlp_jsonl_v0(Cursor::new(b""), &config()).unwrap();
    assert!(!format!("{empty:?}").contains("external-capture-v0"));
    assert!(empty.admission().is_finalized());
    assert_eq!(empty.usage().canonical_events(), 0);
}
