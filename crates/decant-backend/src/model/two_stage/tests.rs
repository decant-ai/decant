use super::super::tests::support::{Reply, Server};
use super::*;

fn message(tool: &str, input: Value) -> String {
    json!({"type":"message","role":"assistant","stop_reason":"tool_use","content":[{"type":"tool_use","name":tool,"input":input}],"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":3,"cache_creation_input_tokens":2}}).to_string()
}
fn model(s: &Server) -> TwoStageAnthropicV1 {
    TwoStageAnthropicV1::wrap(
        AnthropicV1::at(
            &s.url,
            "synthetic-model",
            "SYNTHETIC_MODEL_SECRET",
            Some(&s.ca),
            Duration::from_secs(10),
        )
        .unwrap(),
    )
    .unwrap()
}
fn reference() -> String {
    format!("sha256:v1:{}", "a".repeat(64))
}
fn input() -> ModelInputV1 {
    ModelInputV1::from_projected(br#"{"findings":{"latency":"increase"}}"#, vec![reference()])
        .unwrap()
}
fn explanation() -> Value {
    json!({"interpretation":"An observed latency increase warrants checking the dependency; causality is unverified.","references":[reference()],"hypotheses":[{"kind":"latency_regression","assessment":"supported","supporting":[reference()],"contradicting":[],"next_checks":["dependencies"]}]})
}
fn confirmed_scope() -> crate::grafana::ScopeV1 {
    crate::grafana::ScopeV1 {
        service_label: "SYNTHETIC_PRIVATE_SERVICE_LABEL".into(),
        service: "SYNTHETIC_APPROVED_SERVICE".into(),
        environment_label: "SYNTHETIC_PRIVATE_ENV_LABEL".into(),
        environment: "SYNTHETIC_APPROVED_ENV".into(),
    }
}
fn scope() -> PlanningScopeV1 {
    PlanningScopeV1::from_confirmed(&confirmed_scope()).unwrap()
}

#[tokio::test]
async fn exactly_two_calls_ordered_bounded_and_accounted() {
    let s = Server::new(vec![
        Reply::json(200, &message("decant_plan", json!({"recipe":"latency"}))),
        Reply::json(200, &message("decant_explain", explanation())),
    ]);
    let mut m = model(&s);
    assert!(matches!(
        m.explain(&input()).await,
        Err(ModelErrorV1::BudgetExhausted)
    ));
    assert_eq!(
        m.plan("Why is the configured service slower?", &scope())
            .await
            .unwrap(),
        RecipeV1::Latency
    );
    assert_eq!(
        m.plan("repeat", &scope()).await.unwrap_err(),
        ModelErrorV1::BudgetExhausted
    );
    m.explain(&input()).await.unwrap();
    assert!(matches!(
        m.explain(&input()).await,
        Err(ModelErrorV1::BudgetExhausted)
    ));
    let u = m.usage();
    assert_eq!(
        (
            u.calls,
            u.input_tokens,
            u.output_tokens,
            u.cache_read_input_tokens,
            u.cache_creation_input_tokens,
            u.unknown_usage_calls
        ),
        (2, 20, 10, 6, 4, 0)
    );
    let requests = s.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    for (i, request) in requests.iter().enumerate() {
        assert!(request.starts_with("POST /v1/messages HTTP/1.1\r\n"));
        assert!(request.contains("x-api-key: SYNTHETIC_MODEL_SECRET\r\n"));
        assert!(!request.to_ascii_lowercase().contains("authorization:"));
        let body = request.split("\r\n\r\n").nth(1).unwrap();
        assert!(body.len() <= if i == 0 { 8 * 1024 } else { 24 * 1024 });
        let b: Value = serde_json::from_str(body).unwrap();
        assert_eq!(b["max_tokens"], if i == 0 { 512 } else { 2048 });
        assert_eq!(
            b["tools"][0]["name"],
            if i == 0 {
                "decant_plan"
            } else {
                "decant_explain"
            }
        );
        assert!(!body.contains("SYNTHETIC_PRIVATE"));
        assert!(!body.contains("SYNTHETIC_MODEL_SECRET"));
        if i == 0 {
            let content: Value =
                serde_json::from_str(b["messages"][0]["content"].as_str().unwrap()).unwrap();
            assert_eq!(
                content,
                json!({
                    "question":"Why is the configured service slower?",
                    "scope":{"service":"SYNTHETIC_APPROVED_SERVICE","environment":"SYNTHETIC_APPROVED_ENV"},
                    "interval":"operator_confirmed_interval"
                })
            );
            assert!(
                b["system"]
                    .as_str()
                    .unwrap()
                    .contains("conflicts with the confirmed scope")
            );
        } else {
            assert!(!body.contains("Why is the configured service slower?"));
            assert!(!body.contains("SYNTHETIC_APPROVED"));
        }
    }
    assert!(!format!("{m:?}").contains("SYNTHETIC_MODEL_SECRET"));
}
#[tokio::test]
async fn invalid_or_declined_plan_cannot_execute_another_stage() {
    for value in [
        json!({"recipe":"unsupported"}),
        json!({"recipe":"clarification_required"}),
        json!({"recipe":"latency","url":"SYNTHETIC_PROTECTED"}),
        json!({"recipe":"shell"}),
    ] {
        let s = Server::new(vec![Reply::json(200, &message("decant_plan", value))]);
        let mut m = model(&s);
        let _ = m.plan("synthetic question", &scope()).await;
        assert!(matches!(
            m.explain(&input()).await,
            Err(ModelErrorV1::BudgetExhausted)
        ));
        assert_eq!(
            m.plan("retry", &scope()).await.unwrap_err(),
            ModelErrorV1::BudgetExhausted
        );
        assert_eq!(s.requests.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn failed_attempt_retains_usage_and_is_not_retried() {
    for reply in [
        Reply::json(403, "SYNTHETIC_PROTECTED"),
        Reply::json(200, "SYNTHETIC_PROTECTED"),
        Reply::json(200, &"x".repeat(64 * 1024 + 1)),
    ] {
        let s = Server::new(vec![reply]);
        let mut m = model(&s);
        let error = m.plan("synthetic", &scope()).await.unwrap_err();
        assert!(!format!("{error} {error:?}").contains("SYNTHETIC_PROTECTED"));
        assert_eq!(m.usage().calls, 1);
        assert_eq!(m.usage().unknown_usage_calls, 1);
        assert_eq!(
            m.plan("retry", &scope()).await.unwrap_err(),
            ModelErrorV1::BudgetExhausted
        );
        assert_eq!(s.requests.lock().unwrap().len(), 1);
    }
}
#[tokio::test]
async fn final_unknown_fields_references_and_control_text_fail() {
    for mutate in 0..5 {
        let mut e = explanation();
        match mutate {
            0 => e["references"] = json!([format!("sha256:v1:{}", "b".repeat(64))]),
            1 => e["interpretation"] = json!("unsafe\u{1b}[2J"),
            2 => e["interpretation"] = json!("a".repeat(2049)),
            3 => e["command"] = json!("SYNTHETIC_PROTECTED"),
            _ => e["hypotheses"] = json!([]),
        }
        let s = Server::new(vec![
            Reply::json(200, &message("decant_plan", json!({"recipe":"latency"}))),
            Reply::json(200, &message("decant_explain", e)),
        ]);
        let mut m = model(&s);
        m.plan("synthetic", &scope()).await.unwrap();
        assert!(matches!(
            m.explain(&input()).await,
            Err(ModelErrorV1::InvalidResponse)
        ));
        assert_eq!(m.usage().calls, 2);
        assert_eq!(m.usage().unknown_usage_calls, 0);
    }
}
#[test]
fn bounded_question_and_input_diagnostics() {
    assert!(validate_question(&"x".repeat(1024)).is_ok());
    for q in [
        "".into(),
        " ".into(),
        "x".repeat(1025),
        "é".repeat(513),
        "secret\n".into(),
    ] {
        assert_eq!(
            validate_question(&q).unwrap_err(),
            ModelErrorV1::InvalidInput
        );
    }
}

#[test]
fn scope_projection_bounds_and_redaction() {
    assert!(!format!("{:?}", scope()).contains("SYNTHETIC_APPROVED"));
    for field in ["service", "environment"] {
        for value in [
            String::new(),
            "x".repeat(129),
            "é".repeat(65),
            "SYNTHETIC_SECRET\n".into(),
        ] {
            let mut s = confirmed_scope();
            if field == "service" {
                s.service = value;
            } else {
                s.environment = value;
            }
            assert_eq!(
                PlanningScopeV1::from_confirmed(&s).unwrap_err(),
                ModelErrorV1::InvalidInput
            );
        }
    }
    let mut s = confirmed_scope();
    s.service = "x".repeat(128);
    s.environment = "é".repeat(64);
    assert!(PlanningScopeV1::from_confirmed(&s).is_ok());
}
