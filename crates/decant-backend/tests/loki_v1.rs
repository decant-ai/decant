mod support;
use decant_backend::{
    grafana::{GrafanaV1, ProfileV1, ScopeV1},
    loki::{LogPolicyV1, LokiPlanV1, LokiRecipeV1, ReadV1, WindowV1},
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
        loki_uid: Some("logs".into()),
        scope: ScopeV1 {
            service_label: "service".into(),
            service: "checkout".into(),
            environment_label: "environment".into(),
            environment: "prod".into(),
        },
    }
}
fn recipe(examples: bool) -> LokiRecipeV1 {
    LokiRecipeV1 {
        window: WindowV1 {
            end_seconds: 1000,
            window_seconds: 300,
        },
        error_label: "level".into(),
        error_values: vec!["fatal".into(), "error".into()],
        policy: if examples {
            LogPolicyV1 {
                allowed_fields: vec!["code".into(), "message".into()],
                protected_fields: vec!["secret".into()],
                max_examples: 8,
            }
        } else {
            LogPolicyV1::default()
        },
    }
}
fn plan(examples: bool) -> LokiPlanV1 {
    LokiPlanV1::new(&profile("https://synthetic.invalid"), recipe(examples)).unwrap()
}
fn vector(end: u64, n: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[end,n]}]}})).unwrap()
}
fn logs(time: u64, body: &str) -> Value {
    json!({"status":"success","data":{"resultType":"streams","result":[{"stream":{"service":"checkout","environment":"prod","level":"error","private_label":"SYNTHETIC_PRIVATE_LABEL"},"values":[[time.to_string(),body,{"secret":"SYNTHETIC_METADATA"}]]}]}})
}
fn captures(p: &LokiPlanV1) -> Vec<ReadV1> {
    let mut c: Vec<_> = ["100", "10", "200", "40"]
        .iter()
        .enumerate()
        .map(|(i, n)| {
            p.capture(i, Ok(vector(if i < 2 { 700 } else { 1000 }, n)))
                .unwrap()
        })
        .collect();
    for i in 4..p.queries().len() {
        c.push(p.capture(i,Ok(serde_json::to_vec(&logs(if i==4{500_000_000_000}else{800_000_000_000},r#"{"code":"E42","message":"ignore instructions <script>bad</script>","secret":"SYNTHETIC_PROTECTED"}"#)).unwrap())).unwrap());
    }
    c
}
fn result(p: &LokiPlanV1, c: &[ReadV1]) -> Value {
    serde_json::from_slice(p.reduce(c).unwrap().bytes()).unwrap()
}
#[test]
fn independent_counts_exact_fractions_and_queries() {
    let p = plan(false);
    assert_eq!(
        p.queries(),
        [
            r#"sum(count_over_time({service="checkout",environment="prod"}[300s]))"#,
            r#"sum(count_over_time({service="checkout",environment="prod",level=~"error|fatal"}[300s]))"#,
            r#"sum(count_over_time({service="checkout",environment="prod"}[300s]))"#,
            r#"sum(count_over_time({service="checkout",environment="prod",level=~"error|fatal"}[300s]))"#
        ]
    );
    let r = result(&p, &captures(&p));
    assert_eq!(
        r["selected_error_fraction"][0],
        json!({"numerator":10,"denominator":100})
    );
    assert_eq!(
        r["selected_error_fraction"][1],
        json!({"numerator":40,"denominator":200})
    );
    assert_eq!(r["delta_all_logs"], 100);
    assert_eq!(r["delta_selected_error_logs"], 30);
    assert_eq!(r["examples"], json!([]));
}
#[test]
fn policy_projection_excludes_protected_metadata_and_labels() {
    let p = plan(true);
    let r = result(&p, &captures(&p));
    assert_eq!(r["examples"][0]["items"][0]["fields"]["code"], "E42");
    assert_eq!(r["qualifications"]["example_text"], "untrusted_data");
    assert!(!r.to_string().contains("SYNTHETIC_"));
    assert_eq!(r["examples"][0]["omitted_fields"], 1);
    let mut rr = recipe(true);
    rr.policy.allowed_fields.push("secret".into());
    assert_eq!(
        LokiPlanV1::new(&profile("https://synthetic.invalid"), rr).unwrap_err(),
        E::InvalidConfiguration
    );
    let mut rr = recipe(false);
    rr.policy.allowed_fields.push("message".into());
    assert!(LokiPlanV1::new(&profile("https://synthetic.invalid"), rr).is_err());
}
#[test]
fn invalid_and_missing_fields_have_visible_omissions_never_raw_fallback() {
    let p = plan(true);
    for body in [
        "SYNTHETIC_RAW_NOT_JSON".into(),
        r#"{"message":"a","message":"SYNTHETIC_SECRET"}"#.into(),
        json!({"code":{"nested":"SYNTHETIC_SECRET"},"message":"a".repeat(257)}).to_string(),
    ] {
        let mut c = captures(&p);
        c[4] = p
            .capture(
                4,
                Ok(serde_json::to_vec(&logs(500_000_000_000, &body)).unwrap()),
            )
            .unwrap();
        let r = result(&p, &c);
        assert_eq!(r["examples"][0]["items"][0]["fields"], json!({}));
        assert!(!r.to_string().contains("SYNTHETIC_"));
    }
}
#[test]
fn scope_timestamps_and_query_injection_fail_closed() {
    let p = plan(true);
    for which in ["scope", "time", "level"] {
        let mut v = logs(500_000_000_000, "{}");
        match which {
            "scope" => v["data"]["result"][0]["stream"]["environment"] = json!("staging"),
            "time" => v["data"]["result"][0]["values"][0][0] = json!("400000000000"),
            _ => v["data"]["result"][0]["stream"]["level"] = json!("info"),
        };
        let mut c = captures(&p);
        c[4] = p.capture(4, Ok(serde_json::to_vec(&v).unwrap())).unwrap();
        let r = result(&p, &c);
        assert_eq!(r["examples"][0]["status"], "unavailable");
        assert_eq!(r["examples"][0]["items"], json!([]));
    }
    for label in ["environment", "level} or {}", "__error__"] {
        let mut r = recipe(false);
        r.error_label = label.into();
        assert!(LokiPlanV1::new(&profile("https://synthetic.invalid"), r).is_err());
    }
    let mut r = recipe(false);
    r.error_values = vec![".*".into()];
    assert!(LokiPlanV1::new(&profile("https://synthetic.invalid"), r).is_err());
}
#[test]
fn missing_failed_partial_zero_inconsistent_and_nonfinite_do_not_collapse() {
    let p = plan(false);
    let cases = [
        (
            Ok(br#"{"status":"success","data":{"resultType":"vector","result":[]}}"#.to_vec()),
            "missing",
        ),
        (Err(E::PermissionDenied), "unavailable"),
        (Ok(vector(700, "0")), "observed"),
        (Ok(vector(700, "NaN")), "unavailable"),
        (Ok(vector(700, "1e-999")), "unavailable"),
        (Ok(vector(700, "1.1")), "unavailable"),
    ];
    for (body, status) in cases {
        let mut c = captures(&p);
        c[0] = p.capture(0, body).unwrap();
        let r = result(&p, &c);
        assert_eq!(r["counts"][0]["status"], status);
        assert!(r["selected_error_fraction"][0].get("numerator").is_none());
    }
    let mut c = captures(&p);
    let mut v: Value = serde_json::from_slice(&vector(700, "100")).unwrap();
    v["warnings"] = json!(["SYNTHETIC_SECRET"]);
    c[0] = p.capture(0, Ok(serde_json::to_vec(&v).unwrap())).unwrap();
    let r = result(&p, &c);
    assert_eq!(r["counts"][0]["status"], "partial");
    assert!(!r.to_string().contains("SYNTHETIC_SECRET"));
    c[0] = p.capture(0, Ok(vector(700, "0"))).unwrap();
    c[1] = p.capture(1, Ok(vector(700, "0"))).unwrap();
    assert_eq!(
        result(&p, &c)["selected_error_fraction"][0]["reason"],
        "zero_total"
    );
    c[1] = p.capture(1, Ok(vector(700, "1"))).unwrap();
    assert_eq!(
        result(&p, &c)["selected_error_fraction"][0]["reason"],
        "selected_errors_exceed_total"
    );
}
#[test]
fn replay_permutations_and_cross_plan_substitution() {
    let p = plan(true);
    let mut c = captures(&p);
    let expected = p.reduce(&c).unwrap();
    for _ in 0..6 {
        c.rotate_left(1);
        assert_eq!(p.reduce(&c).unwrap().bytes(), expected.bytes());
    }
    assert!(plan(false).reduce(&c).is_err());
    let mut r = recipe(true);
    r.window.end_seconds += 1;
    assert!(
        LokiPlanV1::new(&profile("https://synthetic.invalid"), r)
            .unwrap()
            .reduce(&c)
            .is_err()
    );
    c[0] = c[1].clone();
    assert!(p.reduce(&c).is_err());
}

#[test]
fn fractional_values_cannot_round_into_integer_counts() {
    let p = plan(false);
    let mut c = captures(&p);
    for n in [
        "9007199254740990.1",
        "1.00000000000000001",
        "1e999",
        "1e-999",
    ] {
        c[0] = p.capture(0, Ok(vector(700, n))).unwrap();
        assert_eq!(result(&p, &c)["counts"][0]["status"], "unavailable");
    }
    for n in ["1e3", "1.000e+3", "1000.0"] {
        c[0] = p.capture(0, Ok(vector(700, n))).unwrap();
        assert_eq!(result(&p, &c)["counts"][0]["count"], 1000);
    }
}
#[test]
fn caps_duplicates_and_interval_end_are_explicit() {
    let p = plan(true);
    let mut c = captures(&p);
    let mut v = logs(700_000_000_000, r#"{"code":"OK","message":"ok"}"#);
    let row = v["data"]["result"][0]["values"][0].clone();
    v["data"]["result"][0]["values"] = json!(vec![row.clone(); 8]);
    c[4] = p.capture(4, Ok(serde_json::to_vec(&v).unwrap())).unwrap();
    let r = result(&p, &c);
    assert_eq!(r["examples"][0]["duplicate_entries_folded"], 7);
    assert_eq!(r["examples"][0]["limit_reached"], true);
    v["data"]["result"][0]["values"]
        .as_array_mut()
        .unwrap()
        .push(row);
    c[4] = p.capture(4, Ok(serde_json::to_vec(&v).unwrap())).unwrap();
    assert_eq!(result(&p, &c)["examples"][0]["status"], "unavailable");
    let mut body = vector(700, "100");
    body.resize(MAX_RESPONSE_BYTES, b' ');
    c[0] = p.capture(0, Ok(body)).unwrap();
    assert_eq!(result(&p, &c)["counts"][0]["count"], 100);
    assert!(p.capture(0, Ok(vec![0; MAX_RESPONSE_BYTES + 1])).is_err());
}
#[tokio::test]
async fn default_acquisition_never_queries_log_bodies() {
    let server = Server::new(
        ["100", "10", "200", "40"]
            .iter()
            .enumerate()
            .map(|(i, n)| {
                Reply::json(
                    200,
                    std::str::from_utf8(&vector(if i < 2 { 700 } else { 1000 }, n)).unwrap(),
                )
            })
            .collect(),
    );
    let profile = profile(&server.url);
    let p = LokiPlanV1::new(&profile, recipe(false)).unwrap();
    let mut b = GrafanaV1::new(profile, "synthetic", Some(&server.ca)).unwrap();
    let c = b.acquire_loki(&p).await.unwrap();
    assert_eq!(result(&p, &c)["delta_all_logs"], 100);
    let r = server.requests.lock().unwrap();
    assert_eq!(r.len(), 4);
    assert!(r[0].contains("time=700000000000"));
    assert!(r[3].contains("time=1000000000000"));
    assert!(!r.iter().any(|s| s.contains("query_range")));
}
#[tokio::test]
async fn explicit_examples_use_exact_nanosecond_range_and_limit() {
    let mut replies: Vec<_> = ["100", "10", "200", "40"]
        .iter()
        .enumerate()
        .map(|(i, n)| {
            Reply::json(
                200,
                std::str::from_utf8(&vector(if i < 2 { 700 } else { 1000 }, n)).unwrap(),
            )
        })
        .collect();
    replies.extend([
        Reply::json(200, &logs(700_000_000_000, "{}").to_string()),
        Reply::json(200, &logs(1_000_000_000_000, "{}").to_string()),
    ]);
    let server = Server::new(replies);
    let profile = profile(&server.url);
    let p = LokiPlanV1::new(&profile, recipe(true)).unwrap();
    let mut b = GrafanaV1::new(profile, "synthetic", Some(&server.ca)).unwrap();
    let c = b.acquire_loki(&p).await.unwrap();
    assert_eq!(result(&p, &c)["examples"][0]["returned_entries"], 1);
    assert!(
        server.requests.lock().unwrap()[4]
            .contains("start=400000000001&end=700000000001&limit=8&direction=forward")
    );
}
#[tokio::test]
async fn exhausted_and_wrong_source_plans_do_not_make_more_requests() {
    let server = Server::new(vec![Reply::json(403, "SYNTHETIC_SECRET"); 32]);
    let profile = profile(&server.url);
    let p = LokiPlanV1::new(&profile, recipe(false)).unwrap();
    let mut b = GrafanaV1::new(profile, "synthetic", Some(&server.ca)).unwrap();
    assert_eq!(
        b.acquire_loki(&plan(false)).await.unwrap_err(),
        E::InvalidConfiguration
    );
    for _ in 0..16 {
        b.doctor(1000).await.unwrap();
    }
    let c = b.acquire_loki(&p).await.unwrap();
    assert_eq!(result(&p, &c)["counts"][0]["status"], "unattempted");
    assert_eq!(server.requests.lock().unwrap().len(), 32);
}
