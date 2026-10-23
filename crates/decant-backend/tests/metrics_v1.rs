mod support;
use decant_backend::{
    grafana::{GrafanaV1, ProfileV1, ScopeV1},
    metrics::{CaptureV1, MetricsPlanV1, MetricsRecipeV1},
    transport::{BackendErrorV1 as E, MAX_RESPONSE_BYTES},
};
use serde_json::{Value, json};
use support::{Reply, Server};

fn profile(url: &str) -> ProfileV1 {
    ProfileV1 {
        format: "decant-grafana-profile".into(),
        version: 1,
        url: url.into(),
        token_env: "SYNTHETIC_TOKEN".into(),
        ca_file: None,
        prometheus_uid: "metrics".into(),
        tempo_uid: None,
        loki_uid: None,
        scope: ScopeV1 {
            service_label: "service".into(),
            service: "checkout".into(),
            environment_label: "environment".into(),
            environment: "prod".into(),
        },
    }
}
fn recipe() -> MetricsRecipeV1 {
    MetricsRecipeV1 {
        request_counter: "http_requests_total".into(),
        error_label: "status".into(),
        error_values: vec!["503".into(), "500".into()],
        duration_bucket: "http_duration_seconds_bucket".into(),
        duration_unit: "seconds".into(),
        window_seconds: 300,
        end_seconds: 1000,
    }
}
fn plan() -> MetricsPlanV1 {
    MetricsPlanV1::new(&profile("https://synthetic.invalid"), recipe()).unwrap()
}
fn vector(end: u64, n: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"status":"success","data":{"resultType":"vector",
        "result":[{"metric":{},"value":[end,n]}]}}))
    .unwrap()
}
fn values() -> [&'static str; 10] {
    ["80", "10", "0.25", "1", "2", "160", "40", "1.25", "3", "1"]
}
fn captures(plan: &MetricsPlanV1) -> Vec<CaptureV1> {
    values()
        .iter()
        .enumerate()
        .map(|(i, n)| {
            plan.capture(i, Ok(vector(if i < 5 { 700 } else { 1000 }, n)))
                .unwrap()
        })
        .collect()
}
fn result(plan: &MetricsPlanV1, captures: &[CaptureV1]) -> Value {
    serde_json::from_slice(plan.reduce(captures).unwrap().bytes()).unwrap()
}

#[test]
fn exact_queries_freeze_reset_aware_rates_and_histogram_semantics() {
    let plan = plan();
    let expected = [
        r#"sum(rate(http_requests_total{service="checkout",environment="prod"}[300s]))"#,
        r#"sum(rate(http_requests_total{service="checkout",environment="prod",status=~"500|503"}[300s]))"#,
        r#"histogram_quantile(0.95, sum by (le) (rate(http_duration_seconds_bucket{service="checkout",environment="prod"}[300s])))"#,
        r#"sum(resets(http_requests_total{service="checkout",environment="prod"}[300s]))"#,
        r#"count(rate(http_requests_total{service="checkout",environment="prod"}[300s]))"#,
    ];
    assert_eq!(plan.queries().len(), 10);
    for (i, q) in plan.queries().iter().enumerate() {
        assert_eq!(q.promql, expected[i % 5]);
        assert_eq!(q.evaluation_seconds, if i < 5 { 700 } else { 1000 });
        assert_eq!(q.period, if i < 5 { "baseline" } else { "current" });
    }
}
#[tokio::test]
async fn production_acquisition_reduces_independently_specified_measurements() {
    // The response values are an independent oracle, not calculated by the recipe.
    let replies = values()
        .iter()
        .enumerate()
        .map(|(i, n)| {
            Reply::json(
                200,
                std::str::from_utf8(&vector(if i < 5 { 700 } else { 1000 }, n)).unwrap(),
            )
        })
        .collect();
    let server = Server::new(replies);
    let profile = profile(&server.url);
    let plan = MetricsPlanV1::new(&profile, recipe()).unwrap();
    let mut backend = GrafanaV1::new(profile, "synthetic", Some(&server.ca)).unwrap();
    let captured = backend.acquire_metrics(&plan).await.unwrap();
    let r = result(&plan, &captured);
    assert_eq!(r["complete_measurements"], true);
    assert_eq!(r["error_fraction"]["baseline"]["value"], 0.125);
    assert_eq!(r["error_fraction"]["current"]["value"], 0.25);
    assert_eq!(r["error_fraction"]["delta"], 0.125);
    assert_eq!(r["delta"]["request_rate"], 80.0);
    assert_eq!(r["delta"]["error_rate"], 30.0);
    assert_eq!(r["delta"]["duration_p95_seconds"], 1.0);
    assert_eq!(r["measurements"][3]["value"], 1.0);
    assert_eq!(r["measurements"][8]["value"], 3.0);
    assert_eq!(r["contributing_series_changed"], true);
    assert_eq!(r["baseline"]["start_seconds"], 400);
    assert_eq!(r["qualifications"]["atomic_snapshot"], false);
    assert_eq!(r["qualifications"]["population_coverage"], "unknown");
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 10);
    for request in requests.iter() {
        assert!(request.starts_with("GET /api/datasources/proxy/uid/metrics/api/v1/query?"));
        assert!(request.contains("environment%3D%22prod%22"));
        assert!(request.contains("timeout=10s&limit=1"));
    }
}
#[test]
fn replay_order_is_canonical_and_other_plan_references_fail_closed() {
    let plan = plan();
    let original = captures(&plan);
    let first = plan.reduce(&original).unwrap();
    let mut shuffled = original.clone();
    for _ in 0..10 {
        shuffled.rotate_left(1);
        assert_eq!(plan.reduce(&shuffled).unwrap().bytes(), first.bytes());
        shuffled.reverse();
        assert_eq!(plan.reduce(&shuffled).unwrap().id(), first.id());
        shuffled.reverse();
    }
    let mut other = profile("https://synthetic.invalid");
    other.scope.environment = "staging".into();
    let other_plan = MetricsPlanV1::new(&other, recipe()).unwrap();
    assert_eq!(
        other_plan.reduce(&original).unwrap_err(),
        E::InvalidResponse
    );
    let mut bad = original.clone();
    bad[1] = bad[0].clone();
    assert!(plan.reduce(&bad).is_err());
    assert!(plan.reduce(&original[..9]).is_err());
    let mut expanded = original.clone();
    expanded.push(original[0].clone());
    assert!(plan.reduce(&expanded).is_err());
    let mut changed = recipe();
    changed.end_seconds = 1001;
    assert!(
        MetricsPlanV1::new(&profile("https://synthetic.invalid"), changed)
            .unwrap()
            .reduce(&original)
            .is_err()
    );
}
#[test]
fn missing_denied_partial_nonfinite_and_zero_do_not_collapse() {
    let plan = plan();
    let empty = br#"{"status":"success","data":{"resultType":"vector","result":[]}}"#.to_vec();
    let mut warning: Value = serde_json::from_slice(&vector(700, "10")).unwrap();
    warning["warnings"] = json!(["SYNTHETIC_SECRET_INSTRUCTIONS"]);
    let mut info = warning.clone();
    info.as_object_mut().unwrap().remove("warnings");
    info["infos"] = json!(["SYNTHETIC_SECRET_INSTRUCTIONS"]);
    for (response, expected) in [
        (Ok(empty), "missing"),
        (Err(E::PermissionDenied), "unavailable"),
        (Ok(serde_json::to_vec(&warning).unwrap()), "partial"),
        (Ok(serde_json::to_vec(&info).unwrap()), "partial"),
        (Ok(vector(700, "NaN")), "non_finite"),
        (Ok(vector(700, "+Inf")), "non_finite"),
        (Ok(vector(700, "0")), "observed"),
    ] {
        let mut c = captures(&plan);
        c[1] = plan.capture(1, response).unwrap();
        let r = result(&plan, &c);
        assert_eq!(r["measurements"][1]["status"], expected);
        assert_eq!(r["complete_measurements"], expected == "observed");
        assert_eq!(
            r["error_fraction"]["baseline"]["value"].is_null(),
            expected != "observed"
        );
        assert!(!r.to_string().contains("SYNTHETIC_SECRET"));
    }
}
#[test]
fn zero_denominator_inconsistent_rates_missing_histograms_and_invalid_counts() {
    let plan = plan();
    for (total, errors, reason) in [
        ("0", "0", "zero_request_rate"),
        ("1", "2", "error_rate_exceeds_total"),
    ] {
        let mut c = captures(&plan);
        c[0] = plan.capture(0, Ok(vector(700, total))).unwrap();
        c[1] = plan.capture(1, Ok(vector(700, errors))).unwrap();
        let r = result(&plan, &c);
        assert_eq!(r["complete_measurements"], false);
        assert_eq!(r["error_fraction"]["baseline"]["reason"], reason);
        assert!(r["error_fraction"]["delta"].is_null());
    }
    let mut c = captures(&plan);
    c[2] = plan.capture(2, Ok(vector(700, "NaN"))).unwrap();
    c[3] = plan.capture(3, Ok(vector(700, "0.5"))).unwrap();
    let r = result(&plan, &c);
    assert_eq!(r["measurements"][2]["status"], "non_finite");
    assert_eq!(r["measurements"][3]["reason"], "invalid_count");
    assert!(r["delta"]["duration_p95_seconds"].is_null());
}
#[test]
fn hostile_inputs_have_fixed_safe_errors_and_bounded_captures() {
    let plan = plan();
    assert_eq!(
        plan.capture(10, Ok(vec![])).unwrap_err(),
        E::InvalidResponse
    );
    assert_eq!(
        plan.capture(0, Ok(vec![b' '; MAX_RESPONSE_BYTES + 1]))
            .unwrap_err(),
        E::InvalidResponse
    );
    let mut extra: Value = serde_json::from_slice(&vector(700, "1")).unwrap();
    extra["data"]["result"] =
        json!([{"metric":{},"value":[700,"1"]},{"metric":{},"value":[700,"2"]}]);
    for body in [
        b"{} SYNTHETIC_SECRET".to_vec(),
        b"{\"status\":\"success\",\"status\":\"error\"}".to_vec(),
        format!("{}0{}", "[".repeat(18), "]".repeat(18)).into_bytes(),
        serde_json::to_vec(&extra).unwrap(),
        vector(699, "1"),
        vector(700, "-1"),
        br#"{"status":"error","error":"SYNTHETIC_SECRET"}"#.to_vec(),
    ] {
        let mut c = captures(&plan);
        c[0] = plan.capture(0, Ok(body)).unwrap();
        let r = result(&plan, &c);
        assert_eq!(r["complete_measurements"], false);
        assert_eq!(r["measurements"][0]["status"], "unavailable");
        assert!(!r.to_string().contains("SYNTHETIC_SECRET"));
    }
}
#[test]
fn recipe_validation_prevents_cross_scope_queries_and_unsupported_units() {
    let profile = profile("https://synthetic.invalid");
    for key in [
        "counter",
        "label",
        "values",
        "bucket",
        "unit",
        "window",
        "end",
        "many_values",
    ] {
        let mut r = recipe();
        match key {
            "counter" => r.request_counter = "x} or up".into(),
            "label" => r.error_label = "environment".into(),
            "values" => r.error_values = vec![".*".into()],
            "bucket" => r.duration_bucket = "summary".into(),
            "unit" => r.duration_unit = "milliseconds".into(),
            "window" => r.window_seconds = 3601,
            "end" => r.end_seconds = 599,
            _ => r.error_values = vec!["500".into(); 17],
        }
        assert_eq!(
            MetricsPlanV1::new(&profile, r).unwrap_err(),
            E::InvalidConfiguration
        );
    }
    let mut same = recipe();
    same.error_values.reverse();
    assert_eq!(
        MetricsPlanV1::new(&profile, same).unwrap().id(),
        plan().id()
    );
    let mut duplicate = recipe();
    duplicate.error_values.push("500".into());
    assert!(MetricsPlanV1::new(&profile, duplicate).is_err());
}
#[test]
fn response_hash_has_an_independent_frozen_sha256_oracle() {
    let plan = plan();
    let mut c = captures(&plan);
    c[0] = plan.capture(0, Ok(vector(700, "100"))).unwrap();
    assert_eq!(
        result(&plan, &c)["measurements"][0]["response_hash"],
        "sha256:v1:ab3a7ca59962b6271bec26110622c88cb4c79a987e92558f9f1c8a4f0b5bf92d"
    );
    assert!(!format!("{:?} {:?}", c[0], plan).contains("checkout"));
}

#[tokio::test]
async fn different_source_plan_is_rejected_before_network() {
    let server = Server::new(vec![]);
    let mut backend = GrafanaV1::new(profile(&server.url), "synthetic", Some(&server.ca)).unwrap();
    assert_eq!(
        backend.acquire_metrics(&plan()).await.unwrap_err(),
        E::InvalidConfiguration
    );
    assert!(server.requests.lock().unwrap().is_empty());
}

#[test]
fn exact_byte_cap_and_extreme_values_keep_explicit_gaps() {
    let plan = plan();
    let mut c = captures(&plan);
    let mut body = vector(700, "1e308");
    body.resize(MAX_RESPONSE_BYTES, b' ');
    c[0] = plan.capture(0, Ok(body)).unwrap();
    c[1] = plan.capture(1, Ok(vector(700, "1e308"))).unwrap();
    c[4] = plan.capture(4, Ok(vector(700, "0"))).unwrap();
    let r = result(&plan, &c);
    assert_eq!(r["error_fraction"]["baseline"]["value"], 1.0);
    assert_eq!(r["complete_measurements"], false);
    assert_eq!(
        r["consistency_gaps"],
        json!(["baseline_positive_rate_without_series"])
    );
    assert!(r["delta"]["request_rate"].as_f64().unwrap().is_finite());
    c[0] = plan.capture(0, Ok(vector(700, "1e999"))).unwrap();
    assert_eq!(result(&plan, &c)["measurements"][0]["status"], "non_finite");
}

#[test]
fn positive_tiny_numbers_do_not_silently_become_zero_error_fractions() {
    let plan = plan();
    let mut c = captures(&plan);
    c[0] = plan.capture(0, Ok(vector(700, "1e308"))).unwrap();
    c[1] = plan.capture(1, Ok(vector(700, "1e-308"))).unwrap();
    let r = result(&plan, &c);
    assert_eq!(
        r["error_fraction"]["baseline"]["reason"],
        "arithmetic_underflow"
    );
    assert_eq!(r["complete_measurements"], false);
    c[1] = plan.capture(1, Ok(vector(700, "1e-999"))).unwrap();
    assert_eq!(
        result(&plan, &c)["measurements"][1]["reason"],
        "numeric_underflow"
    );
}

#[tokio::test]
async fn exhausted_session_preserves_unattempted_probes_without_new_requests() {
    let server = Server::new(vec![
        Reply::json(
            200,
            std::str::from_utf8(&vector(1000, "1")).unwrap()
        );
        32
    ]);
    let profile = profile(&server.url);
    let plan = MetricsPlanV1::new(&profile, recipe()).unwrap();
    let mut backend = GrafanaV1::new(profile, "synthetic", Some(&server.ca)).unwrap();
    for _ in 0..32 {
        assert!(backend.doctor(1000).await.unwrap().ready);
    }
    let c = backend.acquire_metrics(&plan).await.unwrap();
    let r = result(&plan, &c);
    assert_eq!(r["complete_measurements"], false);
    for m in r["measurements"].as_array().unwrap() {
        assert_eq!(m["status"], "unattempted");
        assert_eq!(m["attempted"], false);
        assert_eq!(m["reason"], "backend_budget_exhausted");
        assert!(m["response_hash"].is_null());
    }
    assert_eq!(server.requests.lock().unwrap().len(), 32);
}
