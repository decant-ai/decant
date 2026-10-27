mod support;
use decant_backend::{
    grafana::{GrafanaV1, ProfileV1, ScopeV1},
    tempo::{ReadV1, TempoPlanV1, TempoRecipeV1, TracePolicyV1, WindowV1},
    transport::{BackendErrorV1 as E, MAX_RESPONSE_BYTES},
};
use serde_json::{Value, json};
use support::{Reply, Server};
const TRACE: &str = "01010101010101010101010101010101";
fn profile(url: &str) -> ProfileV1 {
    ProfileV1 {
        format: "decant-grafana-profile".into(),
        version: 1,
        url: url.into(),
        token_env: "SYNTHETIC_TOKEN".into(),
        ca_file: None,
        prometheus_uid: "metrics".into(),
        tempo_uid: Some("traces".into()),
        loki_uid: None,
        scope: ScopeV1 {
            service_label: "service".into(),
            service: "checkout".into(),
            environment_label: "environment".into(),
            environment: "prod".into(),
        },
    }
}
fn recipe() -> TempoRecipeV1 {
    TempoRecipeV1 {
        window: WindowV1 {
            end_seconds: 1000,
            window_seconds: 300,
        },
        traces_per_window: 4,
        policy: TracePolicyV1::default(),
    }
}
fn plan() -> TempoPlanV1 {
    TempoPlanV1::new(&profile("https://synthetic.invalid"), recipe()).unwrap()
}
fn search() -> Vec<u8> {
    serde_json::to_vec(
        &json!({"traces":[{"traceID":TRACE}],"metrics":{"completedJobs":2,"totalJobs":2}}),
    )
    .unwrap()
}
fn span(id: u64, parent: u64, kind: u8, start: u64, duration: u64, status: u8) -> Value {
    json!({"traceId":TRACE,"spanId":format!("{id:016x}"),"parentSpanId":if parent==0 {String::new()}else{format!("{parent:016x}")},
    "name":"GET /pay","kind":kind,"startTimeUnixNano":(start*1_000_000_000).to_string(),"endTimeUnixNano":(start*1_000_000_000+duration).to_string(),"status":{"code":status},
    "attributes":[{"key":"secret","value":{"stringValue":"SYNTHETIC_PROTECTED_VALUE"}}]})
}
fn resource(service: &str, environment: &str, spans: Vec<Value>) -> Value {
    json!({"resource":{"attributes":[{"key":"service.name","value":{"stringValue":service}},
    {"key":"deployment.environment.name","value":{"stringValue":environment}}]},"scopeSpans":[{"scope":{"name":"synthetic"},"spans":spans}]})
}
fn trace() -> Value {
    json!({"status":"COMPLETE","trace":{"resourceSpans":[
    resource("checkout","prod",vec![span(1,0,2,500,10,1),span(2,0,2,600,30,2),span(3,0,2,700,20,0),span(4,0,2,800,90,2),span(5,0,3,800,70,1),span(7,99,2,900,60,1)]),
    resource("payments","prod",vec![span(6,5,2,801,50,2)]),resource("checkout","staging",vec![span(8,0,2,800,900,2)])]}})
}
fn captures(plan: &TempoPlanV1, v: Value) -> Vec<ReadV1> {
    vec![
        plan.capture_search(0, Ok(search())).unwrap(),
        plan.capture_search(1, Ok(search())).unwrap(),
        plan.capture_trace(TRACE, Ok(serde_json::to_vec(&v).unwrap()))
            .unwrap(),
    ]
}
fn reduced(plan: &TempoPlanV1, c: &[ReadV1]) -> Value {
    serde_json::from_slice(plan.reduce(c).unwrap().bytes()).unwrap()
}
#[test]
fn independent_cohorts_boundaries_p95_and_missing_parent() {
    let p = plan();
    let r = reduced(&p, &captures(&p, trace()));
    let groups = r["groups"].as_array().unwrap();
    let op = groups.iter().find(|g| g["kind"] == "operation").unwrap();
    let dep = groups.iter().find(|g| g["kind"] == "dependency").unwrap();
    assert_eq!(op["baseline"]["observed_spans"], 3);
    assert_eq!(op["baseline"]["p95_duration_ns"], 30);
    assert_eq!(op["baseline"]["error_spans"], 1);
    assert_eq!(op["baseline"]["unknown_status_spans"], 1);
    assert_eq!(op["current"]["observed_spans"], 2);
    assert_eq!(op["current"]["p95_duration_ns"], 90);
    assert_eq!(dep["current"]["observed_spans"], 1);
    assert_eq!(dep["current"]["p95_duration_ns"], 50);
    assert_eq!(r["gaps"]["missing_parents"], 1);
    assert_eq!(r["gaps"]["excluded_spans"], 1);
    assert!(op["name"].is_null());
    assert!(dep["name"].is_null());
    assert!(!r.to_string().contains("SYNTHETIC_PROTECTED_VALUE"));
    assert!(!r.to_string().contains("GET /pay"));
    assert!(!r.to_string().contains("payments"));
    assert_eq!(r["qualifications"]["population_statistics"], false);
}
#[test]
fn repeated_spans_fold_conflicting_spans_fail_even_in_ignored_fields() {
    let p = plan();
    let mut v = trace();
    let s = v["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0].clone();
    v["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"]
        .as_array_mut()
        .unwrap()
        .push(s);
    let r = reduced(&p, &captures(&p, v.clone()));
    assert_eq!(r["duplicate_spans_folded"], 1);
    assert_eq!(r["unique_captured_spans"], 8);
    v["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][6]["attributes"][0]["value"]["stringValue"] =
        json!("CHANGED_SECRET");
    assert_eq!(p.reduce(&captures(&p, v)).unwrap_err(), E::InvalidResponse);
}
#[test]
fn canonical_capture_order_and_cross_cut_policy_source_rejection() {
    let p = plan();
    let c = captures(&p, trace());
    let expected = p.reduce(&c).unwrap();
    let mut permuted = c.clone();
    for _ in 0..3 {
        permuted.rotate_left(1);
        assert_eq!(p.reduce(&permuted).unwrap().bytes(), expected.bytes());
    }
    let mut r = recipe();
    r.window.end_seconds += 1;
    assert!(
        TempoPlanV1::new(&profile("https://synthetic.invalid"), r)
            .unwrap()
            .reduce(&c)
            .is_err()
    );
    let mut r = recipe();
    r.policy.operation_names.push("GET /pay".into());
    let allowed = TempoPlanV1::new(&profile("https://synthetic.invalid"), r).unwrap();
    assert!(allowed.reduce(&c).is_err());
    let v = reduced(&allowed, &captures(&allowed, trace()));
    assert!(v.to_string().contains("GET /pay"));
    assert!(
        TempoPlanV1::new(&profile("https://other.invalid"), recipe())
            .unwrap()
            .reduce(&c)
            .is_err()
    );
}
#[test]
fn partial_empty_denied_and_limit_reached_are_distinct() {
    let p = plan();
    let mut c = captures(&p, trace());
    c[0] = p.capture_search(0, Err(E::PermissionDenied)).unwrap();
    let mut partial = trace();
    partial["status"] = json!("PARTIAL");
    c[2] = p
        .capture_trace(TRACE, Ok(serde_json::to_vec(&partial).unwrap()))
        .unwrap();
    let r = reduced(&p, &c);
    assert_eq!(r["receipts"][0]["status"], "unavailable");
    assert_eq!(r["gaps"]["partial_traces"], 1);
    let empty = br#"{"traces":[],"metrics":{}}"#.to_vec();
    let c = vec![
        p.capture_search(0, Ok(empty.clone())).unwrap(),
        p.capture_search(1, Ok(empty)).unwrap(),
    ];
    assert_eq!(reduced(&p, &c)["searches"][0]["status"], "empty");
    let mut rr = recipe();
    rr.traces_per_window = 1;
    let p = TempoPlanV1::new(&profile("https://synthetic.invalid"), rr).unwrap();
    assert_eq!(
        reduced(&p, &captures(&p, trace()))["searches"][0]["limit_reached"],
        true
    );
}
#[test]
fn documented_base64_ids_enums_and_legacy_batches_are_supported() {
    let p = plan();
    let mut v =
        json!({"trace":{"batches":[resource("checkout","prod",vec![span(1,0,2,500,42,2)])]}});
    let s = &mut v["trace"]["batches"][0]["scopeSpans"][0]["spans"][0];
    s["traceId"] = json!("AQEBAQEBAQEBAQEBAQEBAQ==");
    s["spanId"] = json!("AAAAAAAAAAE=");
    s["kind"] = json!("SPAN_KIND_SERVER");
    s["status"]["code"] = json!("STATUS_CODE_ERROR");
    assert_eq!(
        reduced(&p, &captures(&p, v))["groups"][0]["baseline"]["p95_duration_ns"],
        42
    );
}
#[test]
fn malformed_trace_is_unavailable_never_successful_empty_or_leaking() {
    let p = plan();
    for field in ["traceId", "parentSpanId", "kind", "startTimeUnixNano"] {
        let mut v = trace();
        v["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0][field] =
            json!("SYNTHETIC_SECRET");
        let r = reduced(&p, &captures(&p, v));
        assert_eq!(r["receipts"][2]["status"], "unavailable");
        assert!(r["groups"].as_array().unwrap().is_empty());
        assert!(!r.to_string().contains("SYNTHETIC_SECRET"));
    }
    for bytes in [
        br#"{"trace":{},"trace":{}}"#.to_vec(),
        format!("{}0{}", "[".repeat(18), "]".repeat(18)).into_bytes(),
    ] {
        let mut c = captures(&p, trace());
        c[2] = p.capture_trace(TRACE, Ok(bytes)).unwrap();
        assert_eq!(reduced(&p, &c)["receipts"][2]["status"], "unavailable");
    }
}
#[test]
fn byte_span_and_group_bounds_are_not_silent_truncation() {
    let p = plan();
    let mut b = serde_json::to_vec(&trace()).unwrap();
    b.resize(MAX_RESPONSE_BYTES, b' ');
    let mut c = captures(&p, trace());
    c[2] = p.capture_trace(TRACE, Ok(b)).unwrap();
    assert_eq!(reduced(&p, &c)["unique_captured_spans"], 8);
    assert!(
        p.capture_trace(TRACE, Ok(vec![0; MAX_RESPONSE_BYTES + 1]))
            .is_err()
    );
    let s: Vec<_> = (1..=256)
        .map(|id| {
            let mut s = span(id, 0, 2, 500, id, 1);
            s.as_object_mut().unwrap().remove("attributes");
            s
        })
        .collect();
    let mut v = json!({"trace":{"resourceSpans":[resource("checkout","prod",s)]}});
    assert_eq!(
        reduced(&p, &captures(&p, v.clone()))["unique_captured_spans"],
        256
    );
    v["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"]
        .as_array_mut()
        .unwrap()
        .push(span(257, 0, 2, 500, 1, 1));
    assert_eq!(
        reduced(&p, &captures(&p, v))["receipts"][2]["status"],
        "unavailable"
    );
    for count in [32, 33] {
        let s: Vec<_> = (1..=count)
            .map(|id| {
                let mut s = span(id, 0, 2, 500, 1, 1);
                s["name"] = json!(format!("op{id}"));
                s
            })
            .collect();
        let v = json!({"trace":{"resourceSpans":[resource("checkout","prod",s)]}});
        assert_eq!(p.reduce(&captures(&p, v)).is_ok(), count == 32);
    }
}
#[tokio::test]
async fn tls_acquisition_queries_are_scoped_bounded_and_deduplicate_fetches() {
    let server = Server::new(vec![
        Reply::json(200, std::str::from_utf8(&search()).unwrap()),
        Reply::json(200, std::str::from_utf8(&search()).unwrap()),
        Reply::json(200, &trace().to_string()),
    ]);
    let profile = profile(&server.url);
    let p = TempoPlanV1::new(&profile, recipe()).unwrap();
    assert_eq!(
        p.search_query(),
        r#"{resource.service.name="checkout" && resource.deployment.environment.name="prod"}"#
    );
    let mut backend = GrafanaV1::new(profile, "synthetic", Some(&server.ca)).unwrap();
    let c = backend.acquire_tempo(&p).await.unwrap();
    assert_eq!(c.len(), 3);
    assert_eq!(reduced(&p, &c)["unique_captured_spans"], 8);
    let r = server.requests.lock().unwrap();
    assert_eq!(r.len(), 3);
    assert!(r[0].contains("start=400&end=700&limit=4&spss=1"));
    assert!(r[1].contains("start=700&end=1000"));
    assert!(r[2].contains(&format!(
        "/api/v2/traces/{TRACE}?start=400&end=1000&span_pruning=false"
    )));
}
#[tokio::test]
async fn wrong_source_is_rejected_before_io() {
    let s = Server::new(vec![]);
    let mut b = GrafanaV1::new(profile(&s.url), "synthetic", Some(&s.ca)).unwrap();
    assert_eq!(
        b.acquire_tempo(&plan()).await.unwrap_err(),
        E::InvalidConfiguration
    );
    assert!(s.requests.lock().unwrap().is_empty());
}

#[test]
fn cycle_and_ambiguous_envelope_do_not_produce_dependency_evidence() {
    let p = plan();
    let mut v = trace();
    v["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][0]["parentSpanId"] =
        json!("0000000000000002");
    v["trace"]["resourceSpans"][0]["scopeSpans"][0]["spans"][1]["parentSpanId"] =
        json!("0000000000000001");
    assert_eq!(
        reduced(&p, &captures(&p, v))["receipts"][2]["status"],
        "unavailable"
    );
    let mut v = trace();
    v["trace"]["batches"] = json!([]);
    assert_eq!(
        reduced(&p, &captures(&p, v))["receipts"][2]["status"],
        "unavailable"
    );
}

#[tokio::test]
async fn exhausted_session_is_unattempted_not_a_successful_empty_search() {
    let server = Server::new(vec![Reply::json(200, r#"{"traces":[],"metrics":{}}"#); 32]);
    let profile = profile(&server.url);
    let p = TempoPlanV1::new(&profile, recipe()).unwrap();
    let mut b = GrafanaV1::new(profile, "synthetic", Some(&server.ca)).unwrap();
    for _ in 0..16 {
        b.doctor(1000).await.unwrap();
    }
    let c = b.acquire_tempo(&p).await.unwrap();
    let r = reduced(&p, &c);
    assert_eq!(r["searches"][0]["status"], "unattempted");
    assert_eq!(r["receipts"][0]["attempted"], false);
    assert_eq!(server.requests.lock().unwrap().len(), 32);
}

#[test]
fn partial_search_and_duplicate_ids_preserve_qualification() {
    let p = plan();
    let mut c = captures(&p, trace());
    c[0] = p
        .capture_search(
            0,
            Ok(
                serde_json::to_vec(&json!({"traces":[{"traceID":TRACE},{"traceID":TRACE}],
        "metrics":{"completedJobs":1,"totalJobs":2}}))
                .unwrap(),
            ),
        )
        .unwrap();
    let r = reduced(&p, &c);
    assert_eq!(r["searches"][0]["status"], "partial");
    assert_eq!(r["searches"][0]["duplicate_results"], 1);
    assert_eq!(r["unique_captured_spans"], 8);
}
