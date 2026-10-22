use std::fmt::Write as _;

use decant_ir::{
    MAX_OTLP_FILE_LINE_NUMBER_V0, OtlpFilePositionFieldV0, OtlpFilePositionProblemV0, ScalarKindV0,
    SourceIdV0, SourcePositionKindV0, SourcePositionV0, ValidationErrorV0,
};
use decant_otlp_adapter::{
    OTLP_FILE_CANONICALIZER_VERSION_V0, OTLP_SPAN_SCHEMA_ID_V0, OtlpCanonicalizationErrorV0,
    OtlpFileCanonicalizerConfigV0, UNCLASSIFIED_OTLP_SENSITIVITY_BITS_V0,
    canonicalize_otlp_trace_line_v0,
};
use decant_otlp_json::decode_otlp_json_trace_record_v0;

const TRACE_ID: &str = "00112233445566778899aabbccddeeff";
const FIRST_SPAN_ID: &str = "0102030405060708";
const SECOND_SPAN_ID: &str = "1112131415161718";
const THIRD_SPAN_ID: &str = "2122232425262728";

fn config(source: &str) -> OtlpFileCanonicalizerConfigV0 {
    OtlpFileCanonicalizerConfigV0::new(
        OTLP_FILE_CANONICALIZER_VERSION_V0,
        SourceIdV0::new(source).unwrap(),
    )
    .unwrap()
}

fn representative_line(line_number: u64) -> decant_otlp_json::DecodedTraceLineV0 {
    let input = format!(
        concat!(
            "{{\"resourceSpans\":[",
            "{{\"resource\":{{\"attributes\":[",
            "{{\"key\":\"service.name\",\"value\":{{\"stringValue\":\"payment\"}}}},",
            "{{\"key\":\"unretained.resource\",\"value\":{{\"stringValue\":\"opaque-r-aaaa\"}}}}",
            "],\"droppedAttributesCount\":1}},",
            "\"scopeSpans\":[{{\"scope\":{{\"attributes\":[{{\"key\":\"scope.attr\",\"value\":{{\"stringValue\":\"opaque-s-bbbb\"}}}}],\"droppedAttributesCount\":2}},",
            "\"spans\":[",
            "{{\"traceId\":\"{TRACE_ID}\",\"spanId\":\"{FIRST_SPAN_ID}\",\"name\":\"charge\",",
            "\"startTimeUnixNano\":\"100\",\"endTimeUnixNano\":\"140\",",
            "\"attributes\":[{{\"key\":\"card\",\"value\":{{\"stringValue\":\"opaque-p-cccc\"}}}}],",
            "\"events\":[{{\"name\":\"opaque-e-dddd\"}}],\"links\":[{{\"traceId\":\"{TRACE_ID}\",\"spanId\":\"{FIRST_SPAN_ID}\"}}],",
            "\"droppedAttributesCount\":3,\"droppedEventsCount\":4,\"droppedLinksCount\":5,",
            "\"status\":{{\"message\":\"opaque-t-eeee\",\"code\":2}}}},",
            "{{\"traceId\":\"{TRACE_ID}\",\"spanId\":\"{SECOND_SPAN_ID}\",\"name\":\"authorize\",",
            "\"startTimeUnixNano\":\"10\",\"endTimeUnixNano\":\"20\",\"status\":{{\"code\":1}}}}",
            "]}}]}},",
            "{{\"resource\":{{}},\"scopeSpans\":[{{\"spans\":[",
            "{{\"traceId\":\"{TRACE_ID}\",\"spanId\":\"{THIRD_SPAN_ID}\",\"name\":\"settle\",",
            "\"startTimeUnixNano\":\"200\",\"endTimeUnixNano\":\"200\"}}",
            "]}}]}}",
            "]}}\n"
        ),
        TRACE_ID = TRACE_ID,
        FIRST_SPAN_ID = FIRST_SPAN_ID,
        SECOND_SPAN_ID = SECOND_SPAN_ID,
        THIRD_SPAN_ID = THIRD_SPAN_ID,
    );
    decode_otlp_json_trace_record_v0(line_number, input.as_bytes()).unwrap()
}

#[test]
fn representative_line_maps_exact_allowlist_identity_order_and_usage() {
    let decoded = representative_line(7);
    let canonical =
        canonicalize_otlp_trace_line_v0(&config("otel-demo-capture"), &decoded).unwrap();
    assert_eq!(canonical.line_number(), 7);
    assert_eq!(canonical.len(), 3);
    assert!(!canonical.is_empty());
    assert_eq!(canonical.decode_usage(), decoded.usage());
    assert_eq!(canonical.decode_usage().decoded_spans(), 3);
    assert_eq!(canonical.decode_usage().resource_attributes_omitted(), 1);
    assert_eq!(canonical.decode_usage().scope_attributes_omitted(), 1);
    assert_eq!(canonical.decode_usage().span_attributes_omitted(), 1);
    assert_eq!(canonical.decode_usage().span_events_omitted(), 1);
    assert_eq!(canonical.decode_usage().span_links_omitted(), 1);
    assert_eq!(canonical.decode_usage().status_messages_omitted(), 1);
    assert_eq!(
        canonical
            .decode_usage()
            .resource_dropped_attributes_reported(),
        1
    );
    assert_eq!(
        canonical.decode_usage().scope_dropped_attributes_reported(),
        2
    );
    assert_eq!(
        canonical.decode_usage().span_dropped_attributes_reported(),
        3
    );
    assert_eq!(canonical.decode_usage().span_dropped_events_reported(), 4);
    assert_eq!(canonical.decode_usage().span_dropped_links_reported(), 5);

    let events = canonical.events().collect::<Vec<_>>();
    let expected_positions = [(0, 0, 0), (0, 0, 1), (1, 0, 0)];
    let expected_times = [100, 10, 200];
    let expected_names = ["charge", "authorize", "settle"];
    let expected_span_ids = [FIRST_SPAN_ID, SECOND_SPAN_ID, THIRD_SPAN_ID];
    let expected_statuses = [2, 1, 0];
    for (index, event) in events.iter().enumerate() {
        assert_eq!(event.source_id().as_str(), "otel-demo-capture");
        assert_eq!(event.schema_id().as_str(), OTLP_SPAN_SCHEMA_ID_V0);
        assert_eq!(event.key(), Some(hex(TRACE_ID).as_slice()));
        assert_eq!(event.event_time_ns(), expected_times[index]);
        assert_eq!(event.ingest_time_ns(), 0);
        assert_eq!(
            event.sensitivity().bits(),
            UNCLASSIFIED_OTLP_SENSITIVITY_BITS_V0
        );
        assert_eq!(
            event.source_position().kind(),
            SourcePositionKindV0::OtlpFile
        );
        let position = event.source_position().otlp_file_position().unwrap();
        assert_eq!(position.line_number(), 7);
        assert_eq!(
            (
                position.resource_spans_index(),
                position.scope_spans_index(),
                position.span_index(),
            ),
            expected_positions[index]
        );
        assert_eq!(
            event.provenance().as_str(),
            format!(
                "decant://event/v0/otel-demo-capture/otlp-file/7/{}/{}/{}",
                expected_positions[index].0,
                expected_positions[index].1,
                expected_positions[index].2,
            )
        );

        let fields = event.payload().fields().collect::<Vec<_>>();
        let expected_field_count = if index < 2 { 5 } else { 4 };
        assert_eq!(fields.len(), expected_field_count);
        assert_eq!(
            field(event, "duration.ns").as_u64(),
            Some([40, 10, 0][index])
        );
        assert_eq!(
            field(event, "span.id").as_bytes(),
            Some(hex(expected_span_ids[index]).as_slice())
        );
        assert_eq!(
            field(event, "span.name").as_utf8(),
            Some(expected_names[index])
        );
        assert_eq!(
            field(event, "status.code").as_u64(),
            Some(expected_statuses[index])
        );
        if index < 2 {
            assert_eq!(field(event, "service.name").as_utf8(), Some("payment"));
        } else {
            assert!(
                event
                    .payload()
                    .fields()
                    .all(|(name, _)| name != "service.name")
            );
        }
        assert!(event.payload().fields().all(|(_, value)| matches!(
            value.kind(),
            ScalarKindV0::U64 | ScalarKindV0::Utf8 | ScalarKindV0::Bytes
        )));
    }

    assert_eq!(
        events[0].source_position().source_ordering_ordinal() + 1,
        events[1].source_position().source_ordering_ordinal()
    );
    assert!(
        events[1].source_position().source_ordering_ordinal()
            < events[2].source_position().source_ordering_ordinal()
    );
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_time_ns())
            .collect::<Vec<_>>(),
        [100, 10, 200]
    );
}

#[test]
fn raw_only_omitted_value_changes_are_intentionally_indistinguishable() {
    let raw_a = raw_line_with_omitted_attribute("raw-only-alpha");
    let raw_b = raw_line_with_omitted_attribute("raw-only-bravo");
    assert_ne!(raw_a, raw_b);

    let decoded_a = decode_otlp_json_trace_record_v0(19, raw_a.as_bytes()).unwrap();
    let decoded_b = decode_otlp_json_trace_record_v0(19, raw_b.as_bytes()).unwrap();
    assert_eq!(decoded_a, decoded_b);
    assert_eq!(decoded_a.usage(), decoded_b.usage());
    assert_eq!(decoded_a.usage().span_attributes_omitted(), 1);

    let canonical_a = canonicalize_otlp_trace_line_v0(&config("capture-v0"), &decoded_a).unwrap();
    let canonical_b = canonicalize_otlp_trace_line_v0(&config("capture-v0"), &decoded_b).unwrap();
    assert_eq!(canonical_a, canonical_b);
}

#[test]
fn canonicalization_is_repeatedly_equal_and_line_bound_is_atomic() {
    let decoded = representative_line(MAX_OTLP_FILE_LINE_NUMBER_V0);
    let first = canonicalize_otlp_trace_line_v0(&config("capture-v0"), &decoded).unwrap();
    let second = canonicalize_otlp_trace_line_v0(&config("capture-v0"), &decoded).unwrap();
    assert_eq!(first, second);
    assert_eq!(
        first
            .events()
            .last()
            .unwrap()
            .source_position()
            .source_ordering_ordinal(),
        ((MAX_OTLP_FILE_LINE_NUMBER_V0 - 1) << 26) | (1_u64 << 19)
    );

    let over = representative_line(MAX_OTLP_FILE_LINE_NUMBER_V0 + 1);
    assert_eq!(
        canonicalize_otlp_trace_line_v0(&config("capture-v0"), &over),
        Err(OtlpCanonicalizationErrorV0::InvalidSourcePosition(
            ValidationErrorV0::InvalidOtlpFilePosition {
                field: OtlpFilePositionFieldV0::LineNumber,
                problem: OtlpFilePositionProblemV0::AboveMaximum,
            }
        ))
    );
}

#[test]
fn configuration_and_diagnostics_are_value_safe() {
    assert_eq!(
        OtlpFileCanonicalizerConfigV0::new(1, SourceIdV0::new("secret-source").unwrap()),
        Err(OtlpCanonicalizationErrorV0::UnsupportedVersion { actual: 1 })
    );

    let decoded = representative_line(MAX_OTLP_FILE_LINE_NUMBER_V0 + 1);
    let error = canonicalize_otlp_trace_line_v0(&config("secret-source"), &decoded).unwrap_err();
    let canonical =
        canonicalize_otlp_trace_line_v0(&config("secret-source"), &representative_line(7)).unwrap();
    let numeric_raw = raw_line_with_times(9_081_726_354_001, 9_081_791_789_123);
    let numeric_decoded =
        decode_otlp_json_trace_record_v0(271_828_183, numeric_raw.as_bytes()).unwrap();
    let numeric_canonical =
        canonicalize_otlp_trace_line_v0(&config("numeric-secret-source"), &numeric_decoded)
            .unwrap();
    let numeric_position = SourcePositionV0::otlp_file(271_828_183, 109, 107, 3583).unwrap();
    let safe = format!(
        "{:?} {:?} {:?} {:?} {:?} {:?} {:?} {:?} {:?}",
        config("secret-source"),
        error,
        representative_line(7),
        canonical,
        canonical.events().next().unwrap(),
        numeric_decoded,
        numeric_canonical,
        numeric_canonical.events().next().unwrap(),
        numeric_position,
    );
    for secret in [
        "secret-source",
        "payment",
        "charge",
        TRACE_ID,
        FIRST_SPAN_ID,
        "opaque-r-aaaa",
        "opaque-s-bbbb",
        "opaque-p-cccc",
        "opaque-e-dddd",
        "opaque-t-eeee",
    ] {
        let escaped = secret
            .chars()
            .flat_map(char::escape_default)
            .collect::<String>();
        let decimal_comma = secret
            .as_bytes()
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let decimal_debug = format!("{:?}", secret.as_bytes());
        let hex_lower = hex_encode(secret.as_bytes(), false);
        let hex_upper = hex_encode(secret.as_bytes(), true);
        let base64 = base64(secret.as_bytes());
        let prefix = &secret[..secret.len().min(8)];
        for recoverable in [
            secret,
            escaped.as_str(),
            decimal_comma.as_str(),
            decimal_debug.as_str(),
            hex_lower.as_str(),
            hex_upper.as_str(),
            base64.as_str(),
            prefix,
        ] {
            assert!(
                !safe.contains(recoverable),
                "diagnostic contained fixture representation {recoverable:?}"
            );
        }
    }
    for secret in [
        271_828_183_u64,
        109,
        107,
        3583,
        9_081_726_354_001,
        9_081_791_789_123,
        65_435_122,
    ] {
        let decimal = secret.to_string();
        let decimal_debug = format!("{:?}", decimal.as_bytes());
        let hex_lower = format!("{secret:x}");
        let hex_upper = format!("{secret:X}");
        let encoded = base64(decimal.as_bytes());
        for recoverable in [
            decimal.as_str(),
            decimal_debug.as_str(),
            hex_lower.as_str(),
            hex_upper.as_str(),
            encoded.as_str(),
        ] {
            assert!(
                !safe.contains(recoverable),
                "diagnostic contained numeric representation {recoverable:?}"
            );
        }
    }
}

fn field<'a>(event: &'a decant_ir::EventEnvelopeV0, name: &str) -> &'a decant_ir::ScalarValueV0 {
    event
        .payload()
        .fields()
        .find_map(|(candidate, value)| (candidate == name).then_some(value))
        .unwrap()
}

fn raw_line_with_omitted_attribute(value: &str) -> String {
    format!(
        concat!(
            "{{\"resourceSpans\":[{{\"scopeSpans\":[{{\"spans\":[{{",
            "\"traceId\":\"{TRACE_ID}\",\"spanId\":\"{FIRST_SPAN_ID}\",",
            "\"name\":\"charge\",\"startTimeUnixNano\":\"100\",",
            "\"endTimeUnixNano\":\"140\",\"attributes\":[{{",
            "\"key\":\"raw.only\",\"value\":{{\"stringValue\":\"{value}\"}}",
            "}}]}}]}}]}}]}}\n"
        ),
        TRACE_ID = TRACE_ID,
        FIRST_SPAN_ID = FIRST_SPAN_ID,
        value = value,
    )
}

fn raw_line_with_times(start_time_ns: u64, end_time_ns: u64) -> String {
    format!(
        concat!(
            "{{\"resourceSpans\":[{{\"scopeSpans\":[{{\"spans\":[{{",
            "\"traceId\":\"{TRACE_ID}\",\"spanId\":\"{FIRST_SPAN_ID}\",",
            "\"name\":\"numeric-secret-span\",",
            "\"startTimeUnixNano\":\"{start_time_ns}\",",
            "\"endTimeUnixNano\":\"{end_time_ns}\"",
            "}}]}}]}}]}}\n"
        ),
        TRACE_ID = TRACE_ID,
        FIRST_SPAN_ID = FIRST_SPAN_ID,
        start_time_ns = start_time_ns,
        end_time_ns = end_time_ns,
    )
}

fn hex(input: &str) -> Vec<u8> {
    let (pairs, remainder) = input.as_bytes().as_chunks::<2>();
    assert!(remainder.is_empty());
    pairs
        .iter()
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).unwrap() as u8;
            let low = (pair[1] as char).to_digit(16).unwrap() as u8;
            (high << 4) | low
        })
        .collect()
}

fn hex_encode(bytes: &[u8], uppercase: bool) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        if uppercase {
            write!(&mut encoded, "{byte:02X}").unwrap();
        } else {
            write!(&mut encoded, "{byte:02x}").unwrap();
        }
    }
    encoded
}

fn base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);

    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        let combined = (u32::from(first) << 16) | (u32::from(second) << 8) | u32::from(third);

        encoded.push(char::from(ALPHABET[((combined >> 18) & 0x3f) as usize]));
        encoded.push(char::from(ALPHABET[((combined >> 12) & 0x3f) as usize]));
        if chunk.len() > 1 {
            encoded.push(char::from(ALPHABET[((combined >> 6) & 0x3f) as usize]));
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(char::from(ALPHABET[(combined & 0x3f) as usize]));
        } else {
            encoded.push('=');
        }
    }

    encoded
}
