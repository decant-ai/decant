use super::*;
#[path = "../tests/support/mod.rs"]
mod support;
use support::{Reply, Server};

fn input() -> ModelInputV1 {
    ModelInputV1::from_projected(br#"{"report":"synthetic","measurements":[1,2]}"#, vec![]).unwrap()
}
fn message(decision: Value) -> Value {
    json!({"id":"synthetic-message","type":"message","role":"assistant","model":"synthetic-model",
        "stop_reason":"tool_use","content":[{"type":"tool_use","id":"synthetic-tool","name":"decant_decision","input":decision}],
        "usage":{"input_tokens":10,"output_tokens":20,"cache_read_input_tokens":30,"cache_creation_input_tokens":40}})
}
fn inspect() -> Value {
    json!({"action":"inspect","probe":"metrics"})
}
fn model(server: &Server, elapsed: Duration) -> AnthropicV1 {
    AnthropicV1::at(
        &server.url,
        "synthetic-model",
        "synthetic-api-secret",
        Some(&server.ca),
        elapsed,
    )
    .unwrap()
}
#[tokio::test]
async fn model_fixed_post_auth_version_and_usage() {
    let server = Server::new(vec![Reply::json(200, &message(inspect()).to_string())]);
    let mut m = model(&server, Duration::from_secs(5));
    assert!(matches!(
        m.decide(&input()).await.unwrap(),
        DecisionV1::Inspect {
            probe: ProbeV1::Metrics
        }
    ));
    let usage = m.usage();
    assert_eq!(
        (
            usage.calls,
            usage.unknown_usage_calls,
            usage.input_tokens,
            usage.output_tokens,
            usage.cache_read_input_tokens,
            usage.cache_creation_input_tokens
        ),
        (1, 0, 10, 20, 30, 40)
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert!(request.starts_with("POST /v1/messages HTTP/1.1\r\n"));
    assert!(request.contains("x-api-key: synthetic-api-secret\r\n"));
    assert!(request.contains("anthropic-version: 2023-06-01\r\n"));
    assert!(!request.to_ascii_lowercase().contains("authorization:"));
    let body: Value = serde_json::from_str(request.split("\r\n\r\n").nth(1).unwrap()).unwrap();
    assert_eq!(body["model"], "synthetic-model");
    assert_eq!(body["max_tokens"], 4096);
    assert_eq!(body["stream"], false);
    assert_eq!(body["tools"].as_array().unwrap().len(), 1);
    assert!(!format!("{m:?}").contains("synthetic-api-secret"));
}
#[tokio::test]
async fn model_six_calls_exact_and_seventh_no_request() {
    let server = Server::new(vec![Reply::json(200, &message(inspect()).to_string()); 6]);
    let mut m = model(&server, Duration::from_secs(10));
    for _ in 0..6 {
        m.decide(&input()).await.unwrap();
    }
    assert_eq!(
        m.decide(&input()).await.unwrap_err(),
        ModelErrorV1::BudgetExhausted
    );
    assert_eq!(server.requests.lock().unwrap().len(), 6);
    assert_eq!(m.usage().input_tokens, 60);
}
#[tokio::test]
async fn model_errors_preserve_known_or_unknown_usage() {
    let mut refusal = message(inspect());
    refusal["stop_reason"] = json!("refusal");
    let mut truncated = message(inspect());
    truncated["stop_reason"] = json!("max_tokens");
    let server = Server::new(vec![
        Reply::json(200, &refusal.to_string()),
        Reply::json(200, &truncated.to_string()),
        Reply::json(200, "sensitive-response"),
        Reply::json(403, "sensitive-response"),
    ]);
    let mut m = model(&server, Duration::from_secs(10));
    for expected in [
        ModelErrorV1::Refused,
        ModelErrorV1::Truncated,
        ModelErrorV1::InvalidResponse,
        ModelErrorV1::Transport(BackendErrorV1::PermissionDenied),
    ] {
        let err = m.decide(&input()).await.unwrap_err();
        assert_eq!(err, expected);
        assert!(!format!("{err:?} {err}").contains("sensitive-response"));
    }
    assert_eq!(m.usage().calls, 4);
    assert_eq!(m.usage().unknown_usage_calls, 2);
    assert_eq!(m.usage().input_tokens, 20);
}
#[tokio::test]
async fn model_redirect_has_no_followup_and_no_credential_forward() {
    let destination = Server::new(vec![]);
    let server=Server::new(vec![Reply::raw(format!("HTTP/1.1 302 Redirect\r\nLocation: {}/other\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",destination.url).into_bytes())]);
    let mut m = model(&server, Duration::from_secs(5));
    assert_eq!(
        m.decide(&input()).await.unwrap_err(),
        ModelErrorV1::Transport(BackendErrorV1::RedirectRefused)
    );
    assert!(destination.requests.lock().unwrap().is_empty());
}
#[tokio::test]
async fn model_tls_wrong_host_rejected() {
    let server = Server::with_identity(vec![], true);
    let mut m = model(&server, Duration::from_secs(5));
    assert!(matches!(
        m.decide(&input()).await,
        Err(ModelErrorV1::Transport(_))
    ));
    assert!(server.requests.lock().unwrap().is_empty());
}
#[tokio::test]
async fn model_cancelled_attempt_remains_charged() {
    let mut reply = Reply::json(200, &message(inspect()).to_string());
    reply.delay_headers = Duration::from_millis(200);
    let server = Server::new(vec![reply]);
    let mut m = model(&server, Duration::from_secs(5));
    let context = input();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), m.decide(&context))
            .await
            .is_err()
    );
    assert_eq!(m.usage().calls, 1);
    assert_eq!(m.usage().unknown_usage_calls, 1);
}
#[tokio::test]
async fn model_elapsed_and_response_limits() {
    let mut reply = Reply::json(200, &message(inspect()).to_string());
    reply.delay_body = Duration::from_millis(150);
    let server = Server::new(vec![reply]);
    let mut m = model(&server, Duration::from_millis(50));
    assert_eq!(
        m.decide(&input()).await.unwrap_err(),
        ModelErrorV1::Transport(BackendErrorV1::Timeout)
    );
    assert_eq!(
        m.decide(&input()).await.unwrap_err(),
        ModelErrorV1::BudgetExhausted
    );
    assert_eq!(m.usage().unknown_usage_calls, 1);
    let server = Server::new(vec![Reply::json(200, &"x".repeat(64 * 1024 + 1))]);
    let mut m = model(&server, Duration::from_secs(5));
    assert_eq!(
        m.decide(&input()).await.unwrap_err(),
        ModelErrorV1::Transport(BackendErrorV1::ResponseTooLarge)
    );
}
#[test]
fn model_closed_output_ref_and_assessment_validation() {
    for bad in [
        json!({"action":"shell","command":"sensitive"}),
        json!({"action":"inspect","probe":"arbitrary"}),
        json!({"action":"inspect","probe":"metrics","secret":"sensitive"}),
        json!({"action":"finish","hypotheses":[]}),
    ] {
        assert_eq!(
            decode(&message(bad), &[]).unwrap_err(),
            ModelErrorV1::InvalidResponse
        );
    }
    let id = format!("sha256:v1:{}", "a".repeat(64));
    let valid = json!({"action":"finish","hypotheses":[{"kind":"latency_regression","assessment":"supported","supporting":[id],"contradicting":[],"next_checks":["traffic_mix"]}]});
    assert!(decode(&message(valid.clone()), std::slice::from_ref(&id)).is_ok());
    assert_eq!(
        decode(&message(valid.clone()), &[]).unwrap_err(),
        ModelErrorV1::InvalidResponse
    );
    let mut bad = valid;
    bad["hypotheses"][0]["assessment"] = json!("contradicted");
    assert!(decode(&message(bad), &[id]).is_err());
    let mut text = message(inspect());
    text["content"] = json!([{"type":"text","text":"unverified arbitrary output"}]);
    assert!(decode(&text, &[]).is_err());
}
#[tokio::test]
async fn model_duplicate_fields_rejected_and_limits_precede_network() {
    let body = message(inspect()).to_string().replacen(
        "\"probe\":\"metrics\"",
        "\"probe\":\"metrics\",\"probe\":\"logs\"",
        1,
    );
    let server = Server::new(vec![Reply::json(200, &body)]);
    let mut m = model(&server, Duration::from_secs(5));
    assert_eq!(
        m.decide(&input()).await.unwrap_err(),
        ModelErrorV1::InvalidResponse
    );
    assert!(ModelInputV1::from_projected(&vec![b' '; 64 * 1024 + 1], vec![]).is_err());
    assert!(ModelInputV1::from_projected(br#"{"x":"synthetic-secret","x":1}"#, vec![]).is_err());
    assert!(AnthropicV1::new("", "synthetic", Duration::from_secs(1)).is_err());
    assert!(AnthropicV1::new("synthetic-model", "synthetic", Duration::from_secs(301)).is_err());
}
