use decant_otlp_json::{
    decode_otlp_json_trace_record_v0, evidence::decode_otlp_json_trace_record_v1,
};

const CAPTURE: &str = include_str!("../../../examples/evidence/trace-capture-v1.jsonl");

#[test]
fn kind_version_ids_and_omission_accounting() {
    let line = format!("{}\n", CAPTURE.lines().next().unwrap());
    let decoded = decode_otlp_json_trace_record_v1(1, line.as_bytes()).unwrap();
    let span = decoded.spans().next().unwrap();
    assert_eq!(span.kind(), 2);
    assert_eq!(span.service_version(), Some("v1"));
    assert_eq!(decoded.usage().resource_attributes_omitted(), 0);
    assert_eq!(decoded.usage().span_attributes_omitted(), 1);
    assert_eq!(
        decode_otlp_json_trace_record_v0(1, line.as_bytes())
            .unwrap()
            .usage()
            .resource_attributes_omitted(),
        1
    );
    let absent = line.replace(",\"kind\":2", "");
    assert_eq!(
        decode_otlp_json_trace_record_v1(1, absent.as_bytes())
            .unwrap()
            .spans()
            .next()
            .unwrap()
            .kind(),
        0
    );
}

#[test]
fn new_invalid_fields_are_rejected_only_by_v1() {
    let line = format!("{}\n", CAPTURE.lines().next().unwrap());
    for bad in [
        line.replace("\"kind\":2", "\"kind\":6"),
        line.replace("\"kind\":2", "\"kind\":\"SPAN_KIND_SERVER\""),
        line.replace("\"kind\":2", "\"kind\":2,\"kind\":2"),
        line.replace("\"stringValue\":\"v1\"", "\"intValue\":\"1\""),
        line.replace("\"stringValue\":\"v1\"", "\"stringValue\":\"\""),
        line.replace(
            "\"stringValue\":\"v1\"",
            &format!("\"stringValue\":\"{}\"", "x".repeat(257)),
        ),
    ] {
        assert!(decode_otlp_json_trace_record_v0(1, bad.as_bytes()).is_ok());
        assert!(decode_otlp_json_trace_record_v1(1, bad.as_bytes()).is_err());
    }
}

#[test]
fn malformed_byte_mutations_never_panic_or_echo_input() {
    let line = format!("{}\n", CAPTURE.lines().next().unwrap());
    for index in 0..line.len() {
        let mut changed = line.as_bytes().to_vec();
        changed[index] = 0xff;
        let result = std::panic::catch_unwind(|| decode_otlp_json_trace_record_v1(1, &changed));
        assert!(result.is_ok());
        if let Err(error) = result.unwrap() {
            assert!(!format!("{error:?}").contains("synthetic-private-detail"));
        }
    }
}
