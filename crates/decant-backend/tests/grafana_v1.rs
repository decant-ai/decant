mod support;
use decant_backend::{
    grafana::*,
    transport::{BackendErrorV1 as E, HttpSessionV1, LimitsV1},
};
use serde_json::json;
use support::{Reply, Server};

fn profile(url: &str) -> ProfileV1 {
    ProfileV1 {
        format: "decant-grafana-profile".into(),
        version: 1,
        url: url.into(),
        token_env: "SYNTHETIC_GRAFANA_TOKEN".into(),
        ca_file: None,
        prometheus_uid: "metrics".into(),
        tempo_uid: Some("traces".into()),
        loki_uid: Some("logs".into()),
        scope: ScopeV1 {
            service_label: "service".into(),
            service: "checkout".into(),
            environment_label: "environment".into(),
            environment: "prod".into(),
        },
    }
}
fn vector(n: &str) -> String {
    json!({"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[1000,n]}]}}).to_string()
}
#[tokio::test]
async fn discovery_retains_only_confirmed_safe_uids_and_types() {
    let server = Server::new(vec![Reply::json(
        200,
        r#"[{"uid":"metrics","type":"prometheus","url":"https://SYNTHETIC_SECRET.invalid","basicAuthUser":"SYNTHETIC_SECRET"},
            {"uid":"traces","type":"tempo"},{"uid":"logs","type":"loki"},{"uid":"other","type":"unknown"}]"#,
    )]);
    let mut http = HttpSessionV1::new(
        &server.url,
        "synthetic",
        Some(&server.ca),
        LimitsV1::default(),
    )
    .unwrap();
    let sources = discover_v1(&mut http).await.unwrap();
    assert_eq!(sources.len(), 3);
    assert!(!format!("{sources:?}").contains("SYNTHETIC_SECRET"));
    confirm_sources_v1(&profile(&server.url), &sources).unwrap();
    let mut bad = profile(&server.url);
    bad.prometheus_uid = "traces".into();
    assert!(confirm_sources_v1(&bad, &sources).is_err());
}
#[tokio::test]
async fn doctor_preserves_success_denial_and_empty_and_exact_scope() {
    let server = Server::new(vec![
        Reply::json(200, &vector("3")),
        Reply::json(200, r#"{"traces":[]}"#),
        Reply::json(403, "SYNTHETIC_SECRET"),
    ]);
    let mut client = GrafanaV1::new(profile(&server.url), "synthetic", Some(&server.ca)).unwrap();
    let report = client.doctor(1000).await.unwrap();
    assert!(!report.ready);
    assert_eq!(
        report.sources.iter().map(|s| s.status).collect::<Vec<_>>(),
        ["ready", "empty", "unavailable"]
    );
    assert_eq!(
        report.sources[2].reason.as_deref(),
        Some("permission_denied")
    );
    let output = serde_json::to_string(&report).unwrap();
    assert!(!output.contains("SYNTHETIC_SECRET"));
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[0].starts_with("GET /api/datasources/proxy/uid/metrics/api/v1/query?"));
    assert!(requests[0].contains("service%3D%22checkout%22%2Cenvironment%3D%22prod%22"));
    assert!(requests[0].contains("time=1000"));
    assert!(requests[1].contains("start=700&end=1000&limit=1&spss=1"));
    assert!(requests[2].contains("/logs/loki/api/v1/query?"));
    assert!(
        !requests
            .iter()
            .any(|q| q.contains("POST") || q.contains("query_range"))
    );
}
#[tokio::test]
async fn failure_empty_partial_unconfigured_and_nonfinite_are_not_ready() {
    for (body, status) in [
        (
            r#"{"status":"success","data":{"resultType":"vector","result":[]}}"#.to_owned(),
            "empty",
        ),
        (vector("0"), "empty"),
        (
            r#"{"status":"error","error":"SYNTHETIC_SECRET"}"#.to_owned(),
            "unavailable",
        ),
        (vector("NaN"), "unavailable"),
        (vector("+Inf"), "unavailable"),
        (vector("-1"), "unavailable"),
        (
            vector("1").replacen("{", "{\"warnings\":[\"SYNTHETIC_SECRET\"],", 1),
            "partial",
        ),
        (vector("1").replace("1000", "1001"), "unavailable"),
        (
            r#"{"status":"success","status":"error"}"#.to_owned(),
            "unavailable",
        ),
    ] {
        let server = Server::new(vec![Reply::json(200, &body)]);
        let mut p = profile(&server.url);
        p.tempo_uid = None;
        p.loki_uid = None;
        let r = GrafanaV1::new(p, "synthetic", Some(&server.ca))
            .unwrap()
            .doctor(1000)
            .await
            .unwrap();
        assert!(!r.ready);
        assert_eq!(r.sources[0].status, status);
        assert_eq!(r.sources[1].status, "not_configured");
        assert!(
            !serde_json::to_string(&r)
                .unwrap()
                .contains("SYNTHETIC_SECRET")
        );
    }
}
#[test]
fn profile_is_strict_bounded_and_safe() {
    let p = profile("https://grafana.example.invalid");
    let bytes = p.encode().unwrap();
    assert_eq!(ProfileV1::decode(&bytes).unwrap().encode().unwrap(), bytes);
    for bad in [
        bytes
            .iter()
            .copied()
            .chain(b"garbage".iter().copied())
            .collect::<Vec<_>>(),
        b"[]".to_vec(),
        vec![b' '; 16 * 1024 + 1],
        br#"{"format":"a","format":"b"}"#.to_vec(),
        bytes[..5].to_vec(),
    ] {
        let e = ProfileV1::decode(&bad).unwrap_err();
        assert_eq!(e, E::InvalidConfiguration);
    }
    for field in ["scope", "unexpected"] {
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value[field] = json!(["SYNTHETIC_SECRET"]);
        assert!(ProfileV1::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    }
    let mut bad = p.clone();
    bad.scope.service_label = "service} or {}".into();
    assert!(bad.validate().is_err());
    assert!(!format!("{bad:?}").contains("service}"));
}
#[tokio::test]
async fn hostile_backend_json_and_discovery_bounds_fail_closed() {
    let row = r#"{"uid":"metrics","type":"prometheus"}"#;
    let cases = [
        format!("[{row},{row}]"),
        format!(
            "[{}]",
            (0..65)
                .map(|i| format!(r#"{{"uid":"u{i}","type":"prometheus"}}"#))
                .collect::<Vec<_>>()
                .join(",")
        ),
        format!("{}0{}", "[".repeat(18), "]".repeat(18)),
        format!(
            r#"[{{"uid":"metrics","type":"prometheus","secret":"{}"}}]"#,
            "x".repeat(8193)
        ),
    ];
    for body in cases {
        let server = Server::new(vec![Reply::json(200, &body)]);
        let mut http = HttpSessionV1::new(
            &server.url,
            "synthetic",
            Some(&server.ca),
            LimitsV1::default(),
        )
        .unwrap();
        assert_eq!(
            discover_v1(&mut http).await.unwrap_err(),
            E::InvalidResponse
        );
    }
}

#[tokio::test]
async fn tempo_partial_work_and_invalid_trace_rows_cannot_be_ready() {
    for (body, expected) in [
        (r#"{"traces":[{}]}"#, "unavailable"),
        (
            r#"{"traces":[],"metrics":{"totalJobs":2,"completedJobs":1}}"#,
            "partial",
        ),
        (r#"{"traces":[],"metrics":{"failedBlocks":1}}"#, "partial"),
        (
            r#"{"traces":[],"metrics":{"totalJobs":1,"completedJobs":2}}"#,
            "unavailable",
        ),
        (
            r#"{"traces":[{"traceID":"01010101010101010101010101010101"}]}"#,
            "ready",
        ),
    ] {
        let server = Server::new(vec![Reply::json(200, &vector("1")), Reply::json(200, body)]);
        let mut p = profile(&server.url);
        p.loki_uid = None;
        p.scope.service = "checkout\"},{}".into();
        let report = GrafanaV1::new(p, "synthetic", Some(&server.ca))
            .unwrap()
            .doctor(1000)
            .await
            .unwrap();
        assert_eq!(report.sources[1].status, expected);
        assert_eq!(report.ready, expected == "ready");
        assert!(server.requests.lock().unwrap()[0].contains("%5C%22"));
    }
}
