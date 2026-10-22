use decant_otlp_json::{
    DecodeErrorV0, IdentifierFieldV0, IdentifierProblemV0, MAX_OTLP_JSON_LINE_BYTES_V0,
    MAX_RESOURCE_ATTRIBUTES_V0, MAX_RESOURCE_SPANS_PER_LINE_V0, MAX_SCOPE_ATTRIBUTES_V0,
    MAX_SCOPE_SPANS_PER_RESOURCE_V0, MAX_SERVICE_NAME_BYTES_V0, MAX_SPAN_ATTRIBUTES_V0,
    MAX_SPAN_EVENTS_V0, MAX_SPAN_LINKS_V0, MAX_SPAN_NAME_BYTES_V0, MAX_SPANS_PER_LINE_V0,
    MAX_SPANS_PER_SCOPE_V0, OtlpSignalV0, OtlpSpanStatusV0, ServiceNameProblemV0,
    StructuralBoundV0, TimestampFieldV0, decode_otlp_json_trace_record_v0,
};

const TRACE_ID_LOWER: &str = "0102030405060708090a0b0c0d0e0f10";
const TRACE_ID_UPPER: &str = "0102030405060708090A0B0C0D0E0F10";
const SPAN_ID_LOWER: &str = "1112131415161718";
const SPAN_ID_UPPER: &str = "1112131415161718";

fn line(json: &str) -> Vec<u8> {
    let mut bytes = json.as_bytes().to_vec();
    bytes.push(b'\n');
    bytes
}

fn minimal_span(name: &str) -> String {
    format!(
        "{{\"traceId\":\"{TRACE_ID_LOWER}\",\"spanId\":\"{SPAN_ID_LOWER}\",\"name\":\"{name}\",\"startTimeUnixNano\":\"100\",\"endTimeUnixNano\":\"110\"}}"
    )
}

fn one_span_line(span: &str) -> Vec<u8> {
    line(&format!(
        "{{\"resourceSpans\":[{{\"scopeSpans\":[{{\"spans\":[{span}]}}]}}]}}"
    ))
}

fn repeat_json(item: &str, count: usize) -> String {
    std::iter::repeat_n(item, count)
        .collect::<Vec<_>>()
        .join(",")
}

#[test]
fn representative_record_preserves_order_coordinates_and_accounts_for_omissions() {
    let json = concat!(
            "{\"unknownTop\":{\"nested\":[1,2,3]},\"resourceSpans\":[{",
            "\"unknownResourceSpans\":true,",
            "\"resource\":{\"attributes\":[",
            "{\"key\":\"service.name\",\"value\":{\"stringValue\":\"payment\",\"future\":1}},",
            "{\"key\":\"future.resource\",\"value\":{\"arrayValue\":{\"values\":[{\"stringValue\":\"ignored\"}]}}}",
            "],\"droppedAttributesCount\":7,\"future\":false},",
            "\"scopeSpans\":[{\"scope\":{\"attributes\":[{},{}],\"droppedAttributesCount\":8},",
            "\"unknownScopeSpans\":[],\"spans\":[",
            "{\"traceId\":\"$TRACE_UPPER\",\"spanId\":\"$SPAN_UPPER\",\"name\":\"later-in-time\",",
            "\"startTimeUnixNano\":\"200\",\"endTimeUnixNano\":\"220\",",
            "\"status\":{\"code\":2,\"message\":\"synthetic-status-secret\",\"future\":1},",
            "\"attributes\":[{},{}],\"events\":[{}],\"links\":[{},{}],",
            "\"droppedAttributesCount\":4,\"droppedEventsCount\":5,\"droppedLinksCount\":6,",
            "\"futureSpan\":{\"value\":\"ignored\"}},",
            "{\"traceId\":\"$TRACE_LOWER\",\"spanId\":\"$SPAN_LOWER\",\"name\":\"earlier-in-time\",",
            "\"startTimeUnixNano\":100,\"endTimeUnixNano\":110,\"status\":{\"code\":1}}",
            "]}]}]}"
        )
        .replace("$TRACE_UPPER", TRACE_ID_UPPER)
        .replace("$SPAN_UPPER", SPAN_ID_UPPER)
        .replace("$TRACE_LOWER", TRACE_ID_LOWER)
        .replace("$SPAN_LOWER", SPAN_ID_LOWER);
    let input = line(&json);

    let decoded = decode_otlp_json_trace_record_v0(9, &input).unwrap();
    let spans = decoded.spans().collect::<Vec<_>>();
    assert_eq!(decoded.line_number(), 9);
    assert_eq!(spans.len(), 2);
    assert_eq!(spans[0].name(), "later-in-time");
    assert_eq!(spans[1].name(), "earlier-in-time");
    assert_eq!(spans[0].service_name(), Some("payment"));
    assert_eq!(spans[0].position().line_number(), 9);
    assert_eq!(spans[0].position().resource_spans_index(), 0);
    assert_eq!(spans[0].position().scope_spans_index(), 0);
    assert_eq!(spans[0].position().span_index(), 0);
    assert_eq!(spans[1].position().span_index(), 1);
    assert_eq!(spans[0].trace_id()[0], 1);
    assert_eq!(spans[0].trace_id()[15], 16);
    assert_eq!(spans[0].span_id()[0], 17);
    assert_eq!(spans[0].span_id()[7], 24);
    assert_eq!(spans[0].start_time_unix_ns(), 200);
    assert_eq!(spans[0].end_time_unix_ns(), 220);
    assert_eq!(spans[0].duration_ns(), 20);
    assert_eq!(spans[0].status(), OtlpSpanStatusV0::Error);
    assert!(spans[0].status().is_error());
    assert_eq!(spans[1].status(), OtlpSpanStatusV0::Ok);

    let usage = decoded.usage();
    assert_eq!(usage.decoded_spans(), 2);
    assert_eq!(usage.resource_attributes_omitted(), 1);
    assert_eq!(usage.scope_attributes_omitted(), 2);
    assert_eq!(usage.span_attributes_omitted(), 2);
    assert_eq!(usage.span_events_omitted(), 1);
    assert_eq!(usage.span_links_omitted(), 2);
    assert_eq!(usage.status_messages_omitted(), 1);
    assert_eq!(usage.resource_dropped_attributes_reported(), 7);
    assert_eq!(usage.scope_dropped_attributes_reported(), 8);
    assert_eq!(usage.span_dropped_attributes_reported(), 4);
    assert_eq!(usage.span_dropped_events_reported(), 5);
    assert_eq!(usage.span_dropped_links_reported(), 6);

    assert_eq!(
        decoded,
        decode_otlp_json_trace_record_v0(9, &input).unwrap()
    );
}

#[test]
fn multiple_resource_and_scope_groups_preserve_flattened_input_order() {
    let span_at = |name: &str, start: u64| {
        format!(
            "{{\"traceId\":\"{TRACE_ID_LOWER}\",\"spanId\":\"{SPAN_ID_LOWER}\",\"name\":\"{name}\",\"startTimeUnixNano\":\"{start}\",\"endTimeUnixNano\":\"{}\"}}",
            start + 1
        )
    };
    let a = span_at("resource-0-scope-0-span-0", 400);
    let b = span_at("resource-0-scope-0-span-1", 100);
    let c = span_at("resource-0-scope-1-span-0", 300);
    let d = span_at("resource-1-scope-0-span-0", 200);
    let input = line(&format!(
        concat!(
            "{{\"resourceSpans\":[",
            "{{\"resource\":{{\"attributes\":[{{\"key\":\"service.name\",\"value\":{{\"stringValue\":\"service-z\"}}}}]}},",
            "\"scopeSpans\":[{{\"spans\":[{a},{b}]}},{{\"spans\":[{c}]}}]}},",
            "{{\"resource\":{{\"attributes\":[{{\"key\":\"service.name\",\"value\":{{\"stringValue\":\"service-a\"}}}}]}},",
            "\"scopeSpans\":[{{\"spans\":[{d}]}}]}}]}}"
        ),
        a = a,
        b = b,
        c = c,
        d = d
    ));

    let decoded = decode_otlp_json_trace_record_v0(5, &input).unwrap();
    let spans = decoded.spans().collect::<Vec<_>>();
    assert_eq!(
        spans.iter().map(|span| span.name()).collect::<Vec<_>>(),
        [
            "resource-0-scope-0-span-0",
            "resource-0-scope-0-span-1",
            "resource-0-scope-1-span-0",
            "resource-1-scope-0-span-0",
        ]
    );
    assert_eq!(
        spans
            .iter()
            .map(|span| span.service_name().unwrap())
            .collect::<Vec<_>>(),
        ["service-z", "service-z", "service-z", "service-a"]
    );
    assert_eq!(
        spans
            .iter()
            .map(|span| {
                let position = span.position();
                (
                    position.resource_spans_index(),
                    position.scope_spans_index(),
                    position.span_index(),
                )
            })
            .collect::<Vec<_>>(),
        [(0, 0, 0), (0, 0, 1), (0, 1, 0), (1, 0, 0)]
    );
    assert_eq!(
        spans
            .iter()
            .map(|span| span.start_time_unix_ns())
            .collect::<Vec<_>>(),
        [400, 100, 300, 200]
    );
}

#[test]
fn missing_service_name_is_explicit_and_status_defaults_to_unset() {
    let decoded =
        decode_otlp_json_trace_record_v0(1, &one_span_line(&minimal_span("charge"))).unwrap();
    let span = decoded.spans().next().unwrap();
    assert_eq!(span.service_name(), None);
    assert_eq!(span.status(), OtlpSpanStatusV0::Unset);
}

#[test]
fn recognized_non_trace_signal_never_looks_like_an_empty_trace_record() {
    for (field, signal) in [
        ("resourceMetrics", OtlpSignalV0::Metrics),
        ("resourceLogs", OtlpSignalV0::Logs),
        ("resourceProfiles", OtlpSignalV0::Profiles),
    ] {
        let input = line(&format!("{{\"{field}\":[]}}"));
        assert_eq!(
            decode_otlp_json_trace_record_v0(1, &input).unwrap_err(),
            DecodeErrorV0::UnsupportedSignal { signal }
        );
        let null_input = line(&format!("{{\"{field}\":null}}"));
        assert_eq!(
            decode_otlp_json_trace_record_v0(1, &null_input).unwrap_err(),
            DecodeErrorV0::UnsupportedSignal { signal }
        );
    }

    let mixed = line("{\"resourceSpans\":[],\"resourceMetrics\":null}");
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, &mixed).unwrap_err(),
        DecodeErrorV0::UnsupportedSignal {
            signal: OtlpSignalV0::Metrics
        }
    );
}

#[test]
fn identifier_validation_is_exact_width_case_insensitive_and_nonzero() {
    let upper = one_span_line(&format!(
        "{{\"traceId\":\"{TRACE_ID_UPPER}\",\"spanId\":\"{SPAN_ID_UPPER}\",\"name\":\"charge\",\"startTimeUnixNano\":\"1\",\"endTimeUnixNano\":\"2\"}}"
    ));
    let lower = one_span_line(&format!(
        "{{\"traceId\":\"{TRACE_ID_LOWER}\",\"spanId\":\"{SPAN_ID_LOWER}\",\"name\":\"charge\",\"startTimeUnixNano\":\"1\",\"endTimeUnixNano\":\"2\"}}"
    ));
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, &upper).unwrap(),
        decode_otlp_json_trace_record_v0(1, &lower).unwrap()
    );

    let cases = [
        (
            "{\"spanId\":\"1112131415161718\",\"name\":\"charge\",\"startTimeUnixNano\":\"1\",\"endTimeUnixNano\":\"2\"}",
            IdentifierFieldV0::TraceId,
            IdentifierProblemV0::Missing,
        ),
        (
            "{\"traceId\":\"01\",\"spanId\":\"1112131415161718\",\"name\":\"charge\",\"startTimeUnixNano\":\"1\",\"endTimeUnixNano\":\"2\"}",
            IdentifierFieldV0::TraceId,
            IdentifierProblemV0::InvalidLength,
        ),
        (
            "{\"traceId\":\"0102030405060708090a0b0c0d0e0f1g\",\"spanId\":\"1112131415161718\",\"name\":\"charge\",\"startTimeUnixNano\":\"1\",\"endTimeUnixNano\":\"2\"}",
            IdentifierFieldV0::TraceId,
            IdentifierProblemV0::InvalidHex,
        ),
        (
            "{\"traceId\":\"00000000000000000000000000000000\",\"spanId\":\"1112131415161718\",\"name\":\"charge\",\"startTimeUnixNano\":\"1\",\"endTimeUnixNano\":\"2\"}",
            IdentifierFieldV0::TraceId,
            IdentifierProblemV0::AllZero,
        ),
        (
            "{\"traceId\":\"0102030405060708090a0b0c0d0e0f10\",\"spanId\":\"0000000000000000\",\"name\":\"charge\",\"startTimeUnixNano\":\"1\",\"endTimeUnixNano\":\"2\"}",
            IdentifierFieldV0::SpanId,
            IdentifierProblemV0::AllZero,
        ),
    ];
    for (span, expected_field, expected_problem) in cases {
        let error = decode_otlp_json_trace_record_v0(1, &one_span_line(span)).unwrap_err();
        assert!(matches!(
            error,
            DecodeErrorV0::InvalidIdentifier {
                field,
                problem,
                ..
            } if field == expected_field && problem == expected_problem
        ));
    }
}

#[test]
fn timestamp_forms_are_equivalent_and_invalid_boundaries_fail_atomically() {
    let strings = one_span_line(&format!(
        "{{\"traceId\":\"{TRACE_ID_LOWER}\",\"spanId\":\"{SPAN_ID_LOWER}\",\"name\":\"charge\",\"startTimeUnixNano\":\"100\",\"endTimeUnixNano\":\"120\"}}"
    ));
    let numbers = one_span_line(&format!(
        "{{\"traceId\":\"{TRACE_ID_LOWER}\",\"spanId\":\"{SPAN_ID_LOWER}\",\"name\":\"charge\",\"startTimeUnixNano\":100,\"endTimeUnixNano\":120}}"
    ));
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, &strings).unwrap(),
        decode_otlp_json_trace_record_v0(1, &numbers).unwrap()
    );

    let missing_start = format!(
        "{{\"traceId\":\"{TRACE_ID_LOWER}\",\"spanId\":\"{SPAN_ID_LOWER}\",\"name\":\"charge\",\"endTimeUnixNano\":\"2\"}}"
    );
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, &one_span_line(&missing_start)).unwrap_err(),
        DecodeErrorV0::InvalidTimestamp {
            field: TimestampFieldV0::StartTimeUnixNano
        }
    );

    for invalid in ["-1", "1.5", "1.0", "1e2"] {
        let span = format!(
            "{{\"traceId\":\"{TRACE_ID_LOWER}\",\"spanId\":\"{SPAN_ID_LOWER}\",\"name\":\"charge\",\"startTimeUnixNano\":{invalid},\"endTimeUnixNano\":2}}"
        );
        assert_eq!(
            decode_otlp_json_trace_record_v0(1, &one_span_line(&span)).unwrap_err(),
            DecodeErrorV0::MalformedJson
        );
    }

    for invalid in ["+1", "1.0", "1e2"] {
        let span = format!(
            "{{\"traceId\":\"{TRACE_ID_LOWER}\",\"spanId\":\"{SPAN_ID_LOWER}\",\"name\":\"charge\",\"startTimeUnixNano\":\"{invalid}\",\"endTimeUnixNano\":\"2\"}}"
        );
        assert_eq!(
            decode_otlp_json_trace_record_v0(1, &one_span_line(&span)).unwrap_err(),
            DecodeErrorV0::InvalidTimestamp {
                field: TimestampFieldV0::StartTimeUnixNano
            }
        );
    }

    let too_large = format!(
        "{{\"traceId\":\"{TRACE_ID_LOWER}\",\"spanId\":\"{SPAN_ID_LOWER}\",\"name\":\"charge\",\"startTimeUnixNano\":\"1\",\"endTimeUnixNano\":\"9223372036854775808\"}}"
    );
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, &one_span_line(&too_large)).unwrap_err(),
        DecodeErrorV0::TimestampOutOfRange {
            field: TimestampFieldV0::EndTimeUnixNano
        }
    );

    let reversed = format!(
        "{{\"traceId\":\"{TRACE_ID_LOWER}\",\"spanId\":\"{SPAN_ID_LOWER}\",\"name\":\"charge\",\"startTimeUnixNano\":\"3\",\"endTimeUnixNano\":\"2\"}}"
    );
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, &one_span_line(&reversed)).unwrap_err(),
        DecodeErrorV0::EndBeforeStart
    );
}

#[test]
fn status_codes_have_only_the_three_otlp_v0_meanings() {
    for (fragment, expected) in [
        ("", OtlpSpanStatusV0::Unset),
        (",\"status\":{}", OtlpSpanStatusV0::Unset),
        (",\"status\":{\"code\":0}", OtlpSpanStatusV0::Unset),
        (",\"status\":{\"code\":1}", OtlpSpanStatusV0::Ok),
        (",\"status\":{\"code\":2}", OtlpSpanStatusV0::Error),
    ] {
        let span = format!(
            "{{\"traceId\":\"{TRACE_ID_LOWER}\",\"spanId\":\"{SPAN_ID_LOWER}\",\"name\":\"charge\",\"startTimeUnixNano\":\"1\",\"endTimeUnixNano\":\"2\"{fragment}}}"
        );
        let decoded = decode_otlp_json_trace_record_v0(1, &one_span_line(&span)).unwrap();
        assert_eq!(decoded.spans().next().unwrap().status(), expected);
    }

    let invalid = format!(
        "{{\"traceId\":\"{TRACE_ID_LOWER}\",\"spanId\":\"{SPAN_ID_LOWER}\",\"name\":\"charge\",\"startTimeUnixNano\":\"1\",\"endTimeUnixNano\":\"2\",\"status\":{{\"code\":3}}}}"
    );
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, &one_span_line(&invalid)).unwrap_err(),
        DecodeErrorV0::InvalidStatusCode { actual: 3 }
    );

    for noncanonical in [
        ",\"status\":{\"code\":null}",
        ",\"status\":{\"code\":\"2\"}",
    ] {
        let span = format!(
            "{{\"traceId\":\"{TRACE_ID_LOWER}\",\"spanId\":\"{SPAN_ID_LOWER}\",\"name\":\"charge\",\"startTimeUnixNano\":\"1\",\"endTimeUnixNano\":\"2\"{noncanonical}}}"
        );
        assert_eq!(
            decode_otlp_json_trace_record_v0(1, &one_span_line(&span)).unwrap_err(),
            DecodeErrorV0::MalformedJson
        );
    }

    let quoted_drop_count = one_span_line(&format!(
        "{{\"traceId\":\"{TRACE_ID_LOWER}\",\"spanId\":\"{SPAN_ID_LOWER}\",\"name\":\"charge\",\"startTimeUnixNano\":\"1\",\"endTimeUnixNano\":\"2\",\"droppedEventsCount\":\"1\"}}"
    ));
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, &quoted_drop_count).unwrap_err(),
        DecodeErrorV0::MalformedJson
    );
}

#[test]
fn service_and_span_names_are_strictly_bounded() {
    let span = minimal_span("charge");
    let service_cases = [
        (
            "{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"\"}}]}",
            ServiceNameProblemV0::Empty,
        ),
        (
            "{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"intValue\":\"1\"}}]}",
            ServiceNameProblemV0::WrongValueKind,
        ),
        (
            "{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"payment\",\"intValue\":\"1\"}}]}",
            ServiceNameProblemV0::WrongValueKind,
        ),
        (
            "{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"intValue\":\"1\",\"stringValue\":\"payment\"}}]}",
            ServiceNameProblemV0::WrongValueKind,
        ),
        (
            "{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":null}}]}",
            ServiceNameProblemV0::WrongValueKind,
        ),
        (
            "{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"a\"}},{\"key\":\"service.name\",\"value\":{\"stringValue\":\"b\"}}]}",
            ServiceNameProblemV0::Duplicate,
        ),
    ];
    for (resource, problem) in service_cases {
        let input = line(&format!(
            "{{\"resourceSpans\":[{{\"resource\":{resource},\"scopeSpans\":[{{\"spans\":[{span}]}}]}}]}}"
        ));
        assert!(matches!(
            decode_otlp_json_trace_record_v0(1, &input).unwrap_err(),
            DecodeErrorV0::InvalidServiceName { problem: actual, .. } if actual == problem
        ));
    }

    for alternate in [
        "\"stringValueStrindex\":7",
        "\"boolValue\":true",
        "\"intValue\":\"1\"",
        "\"doubleValue\":1.5",
        "\"arrayValue\":{\"values\":[]}",
        "\"kvlistValue\":{\"values\":[]}",
        "\"bytesValue\":\"AA==\"",
    ] {
        for any_value in [
            format!("{{\"stringValue\":\"payment\",{alternate}}}"),
            format!("{{{alternate},\"stringValue\":\"payment\"}}"),
        ] {
            let resource =
                format!("{{\"attributes\":[{{\"key\":\"service.name\",\"value\":{any_value}}}]}}");
            let input = line(&format!(
                "{{\"resourceSpans\":[{{\"resource\":{resource},\"scopeSpans\":[{{\"spans\":[{span}]}}]}}]}}"
            ));
            assert!(matches!(
                decode_otlp_json_trace_record_v0(1, &input).unwrap_err(),
                DecodeErrorV0::InvalidServiceName {
                    problem: ServiceNameProblemV0::WrongValueKind,
                    ..
                }
            ));
        }
    }

    let exact_service = "s".repeat(MAX_SERVICE_NAME_BYTES_V0);
    let exact_input = line(&format!(
        "{{\"resourceSpans\":[{{\"resource\":{{\"attributes\":[{{\"key\":\"service.name\",\"value\":{{\"stringValue\":\"{exact_service}\"}}}}]}},\"scopeSpans\":[{{\"spans\":[{span}]}}]}}]}}"
    ));
    assert!(decode_otlp_json_trace_record_v0(1, &exact_input).is_ok());

    let oversized_service = "s".repeat(MAX_SERVICE_NAME_BYTES_V0 + 1);
    let oversized_input = line(&format!(
        "{{\"resourceSpans\":[{{\"resource\":{{\"attributes\":[{{\"key\":\"service.name\",\"value\":{{\"stringValue\":\"{oversized_service}\"}}}}]}},\"scopeSpans\":[{{\"spans\":[{span}]}}]}}]}}"
    ));
    assert!(matches!(
        decode_otlp_json_trace_record_v0(1, &oversized_input).unwrap_err(),
        DecodeErrorV0::InvalidServiceName {
            problem: ServiceNameProblemV0::TooLong,
            byte_len
        } if byte_len == MAX_SERVICE_NAME_BYTES_V0 + 1
    ));

    let exact_name = "n".repeat(MAX_SPAN_NAME_BYTES_V0);
    assert!(
        decode_otlp_json_trace_record_v0(1, &one_span_line(&minimal_span(&exact_name))).is_ok()
    );
    for invalid_name in [String::new(), "n".repeat(MAX_SPAN_NAME_BYTES_V0 + 1)] {
        assert!(matches!(
            decode_otlp_json_trace_record_v0(1, &one_span_line(&minimal_span(&invalid_name)))
                .unwrap_err(),
            DecodeErrorV0::InvalidSpanNameLength { .. }
        ));
    }
}

#[test]
fn framing_and_absolute_byte_cap_are_checked_before_domain_parsing() {
    let mut exact = b"{\"resourceSpans\":[]}".to_vec();
    exact.resize(MAX_OTLP_JSON_LINE_BYTES_V0 - 1, b' ');
    exact.push(b'\n');
    let decoded = decode_otlp_json_trace_record_v0(1, &exact).unwrap();
    assert!(decoded.is_empty());

    let mut over = exact;
    over.insert(over.len() - 1, b' ');
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, &over).unwrap_err(),
        DecodeErrorV0::InputTooLarge {
            maximum: MAX_OTLP_JSON_LINE_BYTES_V0,
            actual: MAX_OTLP_JSON_LINE_BYTES_V0 + 1
        }
    );
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, b"{}").unwrap_err(),
        DecodeErrorV0::MissingTerminalLineFeed
    );
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, b"{}\r\n").unwrap_err(),
        DecodeErrorV0::CarriageReturn
    );
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, b"{\n}\n").unwrap_err(),
        DecodeErrorV0::EmbeddedLineFeed
    );
    for malformed in [b"\n".as_slice(), b"{} {}\n", b"not-json\n", &[0xff, b'\n']] {
        assert_eq!(
            decode_otlp_json_trace_record_v0(1, malformed).unwrap_err(),
            DecodeErrorV0::MalformedJson
        );
    }
    assert_eq!(
        decode_otlp_json_trace_record_v0(0, b"{}\n").unwrap_err(),
        DecodeErrorV0::InvalidLineNumber
    );
}

#[test]
fn malformed_byte_corpus_and_deep_unknown_nesting_never_panic() {
    let seed = one_span_line(&minimal_span("charge"));
    for index in 0..seed.len() {
        let truncated = &seed[..index];
        assert!(
            std::panic::catch_unwind(|| {
                let _ = decode_otlp_json_trace_record_v0(1, truncated);
            })
            .is_ok()
        );

        for replacement in [0, 0xff, b'{', b']', b'\n'] {
            let mut mutated = seed.clone();
            mutated[index] = replacement;
            assert!(
                std::panic::catch_unwind(|| {
                    let _ = decode_otlp_json_trace_record_v0(1, &mutated);
                })
                .is_ok()
            );
        }
    }

    let mut deep = String::from("{\"future\":");
    deep.push_str(&"[".repeat(2_048));
    deep.push('0');
    deep.push_str(&"]".repeat(2_048));
    deep.push_str("}\n");
    assert!(
        decode_otlp_json_trace_record_v0(1, deep.as_bytes())
            .unwrap()
            .is_empty()
    );
}

#[test]
fn resource_and_scope_caps_pass_exactly_and_fail_one_past() {
    let exact_resources = line(&format!(
        "{{\"resourceSpans\":[{}]}}",
        repeat_json("{}", MAX_RESOURCE_SPANS_PER_LINE_V0)
    ));
    assert!(decode_otlp_json_trace_record_v0(1, &exact_resources).is_ok());
    let over_resources = line(&format!(
        "{{\"resourceSpans\":[{}]}}",
        repeat_json("{}", MAX_RESOURCE_SPANS_PER_LINE_V0 + 1)
    ));
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, &over_resources).unwrap_err(),
        DecodeErrorV0::TooMany {
            bound: StructuralBoundV0::ResourceSpansPerLine,
            maximum: MAX_RESOURCE_SPANS_PER_LINE_V0,
            actual: MAX_RESOURCE_SPANS_PER_LINE_V0 + 1
        }
    );

    let exact_scopes = line(&format!(
        "{{\"resourceSpans\":[{{\"scopeSpans\":[{}]}}]}}",
        repeat_json("{}", MAX_SCOPE_SPANS_PER_RESOURCE_V0)
    ));
    assert!(decode_otlp_json_trace_record_v0(1, &exact_scopes).is_ok());
    let over_scopes = line(&format!(
        "{{\"resourceSpans\":[{{\"scopeSpans\":[{}]}}]}}",
        repeat_json("{}", MAX_SCOPE_SPANS_PER_RESOURCE_V0 + 1)
    ));
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, &over_scopes).unwrap_err(),
        DecodeErrorV0::TooMany {
            bound: StructuralBoundV0::ScopeSpansPerResource,
            maximum: MAX_SCOPE_SPANS_PER_RESOURCE_V0,
            actual: MAX_SCOPE_SPANS_PER_RESOURCE_V0 + 1
        }
    );
}

#[test]
fn span_caps_pass_exactly_and_fail_per_scope_or_total_atomically() {
    let span = minimal_span("charge");
    let exact = one_span_line(&repeat_json(&span, MAX_SPANS_PER_SCOPE_V0));
    let decoded = decode_otlp_json_trace_record_v0(1, &exact).unwrap();
    assert_eq!(decoded.len(), MAX_SPANS_PER_LINE_V0);

    let over_scope = one_span_line(&repeat_json(&span, MAX_SPANS_PER_SCOPE_V0 + 1));
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, &over_scope).unwrap_err(),
        DecodeErrorV0::TooMany {
            bound: StructuralBoundV0::SpansPerScope,
            maximum: MAX_SPANS_PER_SCOPE_V0,
            actual: MAX_SPANS_PER_SCOPE_V0 + 1
        }
    );

    let first = repeat_json(&span, MAX_SPANS_PER_LINE_V0 / 2);
    let second = repeat_json(&span, MAX_SPANS_PER_LINE_V0 / 2 + 1);
    let over_total = line(&format!(
        "{{\"resourceSpans\":[{{\"scopeSpans\":[{{\"spans\":[{first}]}},{{\"spans\":[{second}]}}]}}]}}"
    ));
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, &over_total).unwrap_err(),
        DecodeErrorV0::TooMany {
            bound: StructuralBoundV0::SpansPerLine,
            maximum: MAX_SPANS_PER_LINE_V0,
            actual: MAX_SPANS_PER_LINE_V0 + 1
        }
    );
}

#[test]
fn known_repeated_field_caps_pass_exactly_and_fail_one_past() {
    let ignored = "{}";
    let span = minimal_span("charge");

    for (bound, maximum, field_name) in [
        (
            StructuralBoundV0::SpanAttributes,
            MAX_SPAN_ATTRIBUTES_V0,
            "attributes",
        ),
        (StructuralBoundV0::SpanEvents, MAX_SPAN_EVENTS_V0, "events"),
        (StructuralBoundV0::SpanLinks, MAX_SPAN_LINKS_V0, "links"),
    ] {
        let base = span.strip_suffix('}').unwrap();
        let exact_span = format!(
            "{base},\"{field_name}\":[{}]}}",
            repeat_json(ignored, maximum)
        );
        assert!(decode_otlp_json_trace_record_v0(1, &one_span_line(&exact_span)).is_ok());
        let over_span = format!(
            "{base},\"{field_name}\":[{}]}}",
            repeat_json(ignored, maximum + 1)
        );
        assert_eq!(
            decode_otlp_json_trace_record_v0(1, &one_span_line(&over_span)).unwrap_err(),
            DecodeErrorV0::TooMany {
                bound,
                maximum,
                actual: maximum + 1
            }
        );
    }

    let exact_resource_attrs = line(&format!(
        "{{\"resourceSpans\":[{{\"resource\":{{\"attributes\":[{}]}},\"scopeSpans\":[{{\"spans\":[{span}]}}]}}]}}",
        repeat_json("{\"key\":\"ignored\"}", MAX_RESOURCE_ATTRIBUTES_V0)
    ));
    assert!(decode_otlp_json_trace_record_v0(1, &exact_resource_attrs).is_ok());
    let over_resource_attrs = line(&format!(
        "{{\"resourceSpans\":[{{\"resource\":{{\"attributes\":[{}]}},\"scopeSpans\":[{{\"spans\":[{span}]}}]}}]}}",
        repeat_json("{\"key\":\"ignored\"}", MAX_RESOURCE_ATTRIBUTES_V0 + 1)
    ));
    assert!(matches!(
        decode_otlp_json_trace_record_v0(1, &over_resource_attrs).unwrap_err(),
        DecodeErrorV0::TooMany {
            bound: StructuralBoundV0::ResourceAttributes,
            ..
        }
    ));

    let exact_scope_attrs = line(&format!(
        "{{\"resourceSpans\":[{{\"scopeSpans\":[{{\"scope\":{{\"attributes\":[{}]}},\"spans\":[{span}]}}]}}]}}",
        repeat_json(ignored, MAX_SCOPE_ATTRIBUTES_V0)
    ));
    assert!(decode_otlp_json_trace_record_v0(1, &exact_scope_attrs).is_ok());
    let over_scope_attrs = line(&format!(
        "{{\"resourceSpans\":[{{\"scopeSpans\":[{{\"scope\":{{\"attributes\":[{}]}},\"spans\":[{span}]}}]}}]}}",
        repeat_json(ignored, MAX_SCOPE_ATTRIBUTES_V0 + 1)
    ));
    assert!(matches!(
        decode_otlp_json_trace_record_v0(1, &over_scope_attrs).unwrap_err(),
        DecodeErrorV0::TooMany {
            bound: StructuralBoundV0::ScopeAttributes,
            ..
        }
    ));
}

#[test]
fn diagnostics_do_not_expose_source_values_or_identifier_encodings() {
    let service_secret = "synthetic-service-secret";
    let span_secret = "synthetic-span-secret";
    let status_secret = "synthetic-status-secret";
    let json = concat!(
        "{\"resourceSpans\":[{\"resource\":{\"attributes\":[",
        "{\"key\":\"service.name\",\"value\":{\"stringValue\":\"$SERVICE\"}}]},",
        "\"scopeSpans\":[{\"spans\":[{\"traceId\":\"$TRACE\",\"spanId\":\"$SPAN\",",
        "\"name\":\"$NAME\",\"startTimeUnixNano\":\"700\",\"endTimeUnixNano\":\"900\",",
        "\"status\":{\"code\":2,\"message\":\"$STATUS\"}}]}]}]}"
    )
    .replace("$SERVICE", service_secret)
    .replace("$TRACE", TRACE_ID_LOWER)
    .replace("$SPAN", SPAN_ID_LOWER)
    .replace("$NAME", span_secret)
    .replace("$STATUS", status_secret);
    let input = line(&json);
    let decoded = decode_otlp_json_trace_record_v0(77, &input).unwrap();
    let diagnostic = format!("{decoded:?}");
    for probe in [
        service_secret,
        span_secret,
        status_secret,
        TRACE_ID_LOWER,
        SPAN_ID_LOWER,
        "700",
        "900",
        "77",
        "73796e7468657469632d7370616e2d736563726574",
        "c3ludGhldGljLXNwYW4tc2VjcmV0",
    ] {
        assert!(!diagnostic.contains(probe), "diagnostic leaked probe");
    }

    let invalid = one_span_line(&minimal_span(&"x".repeat(MAX_SPAN_NAME_BYTES_V0 + 1)));
    let error = decode_otlp_json_trace_record_v0(1, &invalid).unwrap_err();
    let error_diagnostic = format!("{error:?} {error}");
    assert!(!error_diagnostic.contains(&"x".repeat(32)));
}
