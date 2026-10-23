use std::error::Error;

use decant_compiler::investigation_coverage::{
    InvestigationCoverageErrorV1 as CoverageError, MAX_INVESTIGATION_COVERAGE_BYTES_V1,
    MAX_INVESTIGATION_PROBES_V1, compile_investigation_coverage_v1,
};
use serde_json::{Value, json};

const GOLDEN: &str = concat!(
    r#"{"format":"decant-investigation-coverage","version":1,"complete":false,"probes":["#,
    r#"{"probe_id":"00000000000000000000000000000001","interval":{"start_ns":0,"end_ns":10},"outcome":{"status":"complete","observed_rows":7}},"#,
    r#"{"probe_id":"00000000000000000000000000000002","interval":{"start_ns":0,"end_ns":10},"outcome":{"status":"unavailable","reason":"permission_denied"}},"#,
    r#"{"probe_id":"00000000000000000000000000000003","interval":{"start_ns":0,"end_ns":10},"outcome":{"status":"unavailable","reason":"backend_unavailable"}}]}"#,
);
// Independently calculated with Python hashlib over the documented domain + NUL + GOLDEN.
const GOLDEN_HASH: &str =
    "sha256:v1:6d655a6de832486964fecb98d6a43f9340a1d5570e90a11163e4e183e5dd852c";

fn request(outcomes: &[Value]) -> Value {
    json!({
        "version": 1,
        "probes": outcomes.iter().enumerate().map(|(index, _)| json!({
            "probe_id": format!("{:032x}", index + 1),
            "interval": {"start_ns": 0, "end_ns": 10}
        })).collect::<Vec<_>>(),
        "outcomes": outcomes.iter().enumerate().map(|(index, outcome)| json!({
            "probe_id": format!("{:032x}", index + 1), "outcome": outcome
        })).collect::<Vec<_>>()
    })
}

fn complete(rows: u64) -> Value {
    json!({"status": "complete", "observed_rows": rows})
}

fn mixed() -> Value {
    // The caller maps these opaque IDs to metrics, Loki and Tempo, respectively.
    request(&[
        complete(7),
        json!({"status": "unavailable", "reason": "permission_denied"}),
        json!({"status": "unavailable", "reason": "backend_unavailable"}),
    ])
}

fn bytes(value: &Value) -> Vec<u8> {
    serde_json::to_vec(value).unwrap()
}

fn assert_error(value: &Value, expected: CoverageError) {
    let error = compile_investigation_coverage_v1(&bytes(value)).unwrap_err();
    assert_eq!(error, expected);
    assert!(error.source().is_none());
}

#[test]
fn mixed_backend_coverage_preserves_every_outcome_and_matches_frozen_golden() {
    let report = compile_investigation_coverage_v1(&bytes(&mixed())).unwrap();
    assert!(!report.is_complete());
    assert_eq!(report.canonical_bytes(), GOLDEN.as_bytes());
    assert_eq!(report.hash(), GOLDEN_HASH);
    assert!(!report.canonical_bytes().ends_with(b"\n"));
}

#[test]
fn empty_success_failed_request_partial_and_unattempted_are_distinct() {
    let outcomes = [
        complete(0),
        json!({"status": "unavailable", "reason": "request_failed"}),
        json!({"status": "unattempted", "reason": "not_scheduled"}),
        json!({"status": "partial", "observed_rows": 0, "reason": "backend_partial"}),
    ];
    let report = compile_investigation_coverage_v1(&bytes(&request(&outcomes))).unwrap();
    assert!(!report.is_complete());
    let value: Value = serde_json::from_slice(report.canonical_bytes()).unwrap();
    for (index, expected) in outcomes.iter().enumerate() {
        assert_eq!(&value["probes"][index]["outcome"], expected);
    }
    let empty = compile_investigation_coverage_v1(&bytes(&request(&[complete(0)]))).unwrap();
    // Only response coverage is complete; this API makes no health assertion.
    assert!(empty.is_complete());
    assert_eq!(
        serde_json::from_slice::<Value>(empty.canonical_bytes()).unwrap()["complete"],
        true
    );
}

#[test]
fn all_declaration_and_outcome_permutations_are_identical() {
    let original = mixed();
    let permutations = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    for probes in permutations {
        for outcomes in permutations {
            let input = json!({
                "version": 1,
                "probes": probes.map(|i| original["probes"][i].clone()),
                "outcomes": outcomes.map(|i| original["outcomes"][i].clone())
            });
            let report = compile_investigation_coverage_v1(&bytes(&input)).unwrap();
            assert_eq!(report.canonical_bytes(), GOLDEN.as_bytes());
            assert_eq!(report.hash(), GOLDEN_HASH);
        }
    }
    let pretty = serde_json::to_vec_pretty(&original).unwrap();
    assert_eq!(
        compile_investigation_coverage_v1(&pretty).unwrap().hash(),
        GOLDEN_HASH
    );
}

#[test]
fn identity_binds_interval_id_count_status_and_reason() {
    let original = mixed();
    let mut changed = original.clone();
    changed["probes"][0]["interval"]["end_ns"] = json!(11);
    assert_ne!(
        compile_investigation_coverage_v1(&bytes(&changed))
            .unwrap()
            .hash(),
        GOLDEN_HASH
    );
    changed = original.clone();
    changed["probes"][0]["probe_id"] = json!("00000000000000000000000000000009");
    changed["outcomes"][0]["probe_id"] = changed["probes"][0]["probe_id"].clone();
    assert_ne!(
        compile_investigation_coverage_v1(&bytes(&changed))
            .unwrap()
            .hash(),
        GOLDEN_HASH
    );
    changed = original.clone();
    changed["outcomes"][0]["outcome"]["observed_rows"] = json!(8);
    assert_ne!(
        compile_investigation_coverage_v1(&bytes(&changed))
            .unwrap()
            .hash(),
        GOLDEN_HASH
    );
    changed = original.clone();
    changed["outcomes"][1]["outcome"]["reason"] = json!("timeout");
    assert_ne!(
        compile_investigation_coverage_v1(&bytes(&changed))
            .unwrap()
            .hash(),
        GOLDEN_HASH
    );
    changed = original;
    changed["outcomes"][1]["outcome"] = json!({"status": "unattempted", "reason": "cancelled"});
    assert_ne!(
        compile_investigation_coverage_v1(&bytes(&changed))
            .unwrap()
            .hash(),
        GOLDEN_HASH
    );
}

#[test]
fn duplicates_missing_extra_and_unknown_outcomes_fail_closed() {
    for field in ["probes", "outcomes"] {
        let mut value = mixed();
        value[field][1]["probe_id"] = value[field][0]["probe_id"].clone();
        assert_error(&value, CoverageError::DuplicateProbeId);
    }
    let mut value = mixed();
    value["outcomes"].as_array_mut().unwrap().pop();
    assert_error(&value, CoverageError::OutcomeMismatch);
    value = mixed();
    value["probes"].as_array_mut().unwrap().pop();
    assert_error(&value, CoverageError::OutcomeMismatch);
    value = mixed();
    value["outcomes"][0]["probe_id"] = json!("ffffffffffffffffffffffffffffffff");
    assert_error(&value, CoverageError::OutcomeMismatch);
    assert_error(&request(&[]), CoverageError::EmptyProbeSet);
}

#[test]
fn intervals_require_positive_representable_duration_without_wall_clock() {
    for (start, end) in [(0, 0), (10, 9), (i64::MIN, i64::MAX)] {
        let mut value = request(&[complete(0)]);
        value["probes"][0]["interval"] = json!({"start_ns": start, "end_ns": end});
        assert_error(&value, CoverageError::InvalidInterval);
    }
    for (start, end) in [(-1, 1), (i64::MIN, i64::MIN + 1), (i64::MAX - 1, i64::MAX)] {
        let mut value = request(&[complete(0)]);
        value["probes"][0]["interval"] = json!({"start_ns": start, "end_ns": end});
        assert!(
            compile_investigation_coverage_v1(&bytes(&value))
                .unwrap()
                .is_complete()
        );
    }
}

#[test]
fn statuses_allow_only_their_own_metadata_and_reason_codes() {
    for reason in ["response_truncated", "backend_partial"] {
        let input =
            request(&[json!({"status": "partial", "reason": reason, "observed_rows": u64::MAX})]);
        assert!(
            !compile_investigation_coverage_v1(&bytes(&input))
                .unwrap()
                .is_complete()
        );
    }
    for reason in [
        "permission_denied",
        "timeout",
        "backend_unavailable",
        "invalid_response",
        "request_failed",
    ] {
        let input = request(&[json!({"status": "unavailable", "reason": reason})]);
        assert!(
            !compile_investigation_coverage_v1(&bytes(&input))
                .unwrap()
                .is_complete()
        );
    }
    for reason in ["not_scheduled", "budget_exhausted", "cancelled"] {
        let input = request(&[json!({"status": "unattempted", "reason": reason})]);
        assert!(
            !compile_investigation_coverage_v1(&bytes(&input))
                .unwrap()
                .is_complete()
        );
    }
    for outcome in [
        json!({"status": "complete"}),
        json!({"status": "complete", "observed_rows": -1}),
        json!({"status": "complete", "observed_rows": 1.5}),
        json!({"status": "complete", "observed_rows": 0, "reason": "backend_partial"}),
        json!({"status": "partial", "observed_rows": 0}),
        json!({"status": "partial", "observed_rows": 0, "reason": "permission_denied"}),
        json!({"status": "unavailable", "reason": "budget_exhausted"}),
        json!({"status": "unavailable", "reason": "timeout", "observed_rows": 0}),
        json!({"status": "unattempted", "reason": "timeout"}),
        json!({"status": "unattempted", "reason": "cancelled", "observed_rows": 0}),
        json!({"status": "healthy", "observed_rows": 0}),
        json!({"status": "unavailable", "reason": "new_reason"}),
        json!(null),
    ] {
        assert_error(&request(&[outcome]), CoverageError::InvalidInput);
    }
}

#[test]
fn unknown_and_duplicate_fields_versions_and_nonopaque_ids_are_rejected() {
    for pointer in [
        "",
        "/probes/0",
        "/probes/0/interval",
        "/outcomes/0",
        "/outcomes/0/outcome",
    ] {
        let mut value = request(&[complete(0)]);
        value.pointer_mut(pointer).unwrap()["unexpected"] = json!("SYNTHETIC-SENSITIVE");
        assert_error(&value, CoverageError::InvalidInput);
    }
    for version in [json!(0), json!(2), json!(null), json!("1")] {
        let mut value = request(&[complete(0)]);
        value["version"] = version;
        assert_error(&value, CoverageError::InvalidInput);
    }
    for id in [
        "",
        "1",
        "0000000000000000000000000000000G",
        "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        "SYNTHETIC-SECRET-IN-PROBE-ID",
    ] {
        let mut value = request(&[complete(0)]);
        value["probes"][0]["probe_id"] = json!(id);
        value["outcomes"][0]["probe_id"] = json!(id);
        assert_error(&value, CoverageError::InvalidInput);
    }
    let encoded = String::from_utf8(bytes(&request(&[complete(0)]))).unwrap();
    for (original, duplicate) in [
        ("\"version\":1", "\"version\":1,\"version\":1"),
        ("\"start_ns\":0", "\"start_ns\":0,\"start_ns\":0"),
        (
            "\"observed_rows\":0",
            "\"observed_rows\":0,\"observed_rows\":0",
        ),
        (
            "\"status\":\"complete\"",
            "\"status\":\"complete\",\"status\":\"complete\"",
        ),
        (
            "\"probe_id\":",
            "\"probe_id\":\"00000000000000000000000000000001\",\"probe_id\":",
        ),
    ] {
        let invalid = encoded.replace(original, duplicate);
        assert_ne!(invalid, encoded);
        assert_eq!(
            compile_investigation_coverage_v1(invalid.as_bytes()).unwrap_err(),
            CoverageError::InvalidInput
        );
    }
}

#[test]
fn object_records_cannot_be_replaced_with_positional_arrays() {
    let original = request(&[complete(0)]);
    let positional = json!([
        original["version"],
        original["probes"],
        original["outcomes"]
    ]);
    assert_error(&positional, CoverageError::InvalidInput);
    let mut value = original.clone();
    value["probes"][0] = json!([
        original["probes"][0]["probe_id"],
        original["probes"][0]["interval"]
    ]);
    assert_error(&value, CoverageError::InvalidInput);
    value = original.clone();
    value["probes"][0]["interval"] = json!([0, 10]);
    assert_error(&value, CoverageError::InvalidInput);
    value = original.clone();
    value["outcomes"][0] = json!([
        original["outcomes"][0]["probe_id"],
        original["outcomes"][0]["outcome"]
    ]);
    assert_error(&value, CoverageError::InvalidInput);
}

#[test]
fn record_and_byte_caps_are_inclusive_and_checked_before_deserialization() {
    assert_eq!(MAX_INVESTIGATION_PROBES_V1, 32);
    assert_eq!(MAX_INVESTIGATION_COVERAGE_BYTES_V1, 16 * 1024);
    let valid = request(&vec![complete(u64::MAX); 32]);
    let report = compile_investigation_coverage_v1(&bytes(&valid)).unwrap();
    assert!(report.is_complete());
    assert!(report.canonical_bytes().len() <= MAX_INVESTIGATION_COVERAGE_BYTES_V1);
    let too_many = request(&vec![complete(0); 33]);
    assert!(bytes(&too_many).len() < MAX_INVESTIGATION_COVERAGE_BYTES_V1);
    assert_error(&too_many, CoverageError::InvalidInput);
    for field in ["probes", "outcomes"] {
        let mut one_too_many = valid.clone();
        one_too_many[field] = too_many[field].clone();
        assert_error(&one_too_many, CoverageError::InvalidInput);
    }
    let mut padded = bytes(&valid);
    padded.resize(MAX_INVESTIGATION_COVERAGE_BYTES_V1, b' ');
    assert_eq!(
        compile_investigation_coverage_v1(&padded)
            .unwrap()
            .canonical_bytes(),
        report.canonical_bytes()
    );
    padded.push(b' ');
    assert_eq!(
        compile_investigation_coverage_v1(&padded).unwrap_err(),
        CoverageError::InputTooLarge
    );
    padded.fill(0xff);
    assert_eq!(
        compile_investigation_coverage_v1(&padded).unwrap_err(),
        CoverageError::InputTooLarge
    );
    padded.pop();
    assert_eq!(
        compile_investigation_coverage_v1(&padded).unwrap_err(),
        CoverageError::InvalidInput
    );
}

#[test]
fn malformed_nested_and_truncated_inputs_fail_without_panics_or_values() {
    let encoded = bytes(&mixed());
    for end in 0..encoded.len() {
        assert_eq!(
            compile_investigation_coverage_v1(&encoded[..end]).unwrap_err(),
            CoverageError::InvalidInput
        );
    }
    let nested = String::from_utf8(encoded.clone()).unwrap().replace(
        "\"observed_rows\":7",
        &format!("\"observed_rows\":{}0{}", "[".repeat(256), "]".repeat(256)),
    );
    for invalid in [
        b"null".to_vec(),
        b"[]".to_vec(),
        b"{}".to_vec(),
        [encoded.as_slice(), b" {}"].concat(),
        format!("{}0{}", "[".repeat(256), "]".repeat(256)).into_bytes(),
        nested.into_bytes(),
    ] {
        assert_eq!(
            compile_investigation_coverage_v1(&invalid).unwrap_err(),
            CoverageError::InvalidInput
        );
    }
}

#[test]
fn errors_are_fixed_and_never_echo_sensitive_fields_or_underlying_sources() {
    const SENTINEL: &str = "SYNTHETIC-SECRET-DO-NOT-EMIT";
    let mut inputs = vec![format!(r#"{{"{SENTINEL}":1}}"#).into_bytes()];
    for pointer in [
        "/probes/0/probe_id",
        "/probes/0/interval/start_ns",
        "/outcomes/0/outcome/status",
        "/outcomes/0/outcome/observed_rows",
    ] {
        let mut value = request(&[complete(0)]);
        *value.pointer_mut(pointer).unwrap() = json!(SENTINEL);
        inputs.push(bytes(&value));
    }
    inputs.push(bytes(&request(&[
        json!({"status": "unavailable", "reason": SENTINEL}),
    ])));
    for input in inputs {
        let error = compile_investigation_coverage_v1(&input).unwrap_err();
        assert_eq!(error, CoverageError::InvalidInput);
        assert_eq!(error.to_string(), "invalid investigation coverage input");
        assert_eq!(format!("{error:?}"), "InvalidInput");
        assert!(!format!("{error:?} {error}").contains(SENTINEL));
        assert!(error.source().is_none());
    }
    for (error, message) in [
        (
            CoverageError::InputTooLarge,
            "investigation coverage input exceeds byte limit",
        ),
        (
            CoverageError::EmptyProbeSet,
            "investigation coverage requires declared probes",
        ),
        (
            CoverageError::DuplicateProbeId,
            "duplicate investigation probe identifier",
        ),
        (
            CoverageError::InvalidInterval,
            "invalid investigation probe interval",
        ),
        (
            CoverageError::OutcomeMismatch,
            "investigation outcomes do not match declared probes",
        ),
        (
            CoverageError::Encoding,
            "investigation coverage encoding failed",
        ),
    ] {
        assert_eq!(error.to_string(), message);
        assert!(error.source().is_none());
    }
}
