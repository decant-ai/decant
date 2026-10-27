#[path = "../../decant-backend/tests/support/mod.rs"]
mod support;
use decant_backend::{
    grafana::{GrafanaV1, ProfileV1, ScopeV1},
    loki::{LogPolicyV1, LokiRecipeV1},
    metrics::MetricsRecipeV1,
    model::{two_stage::*, *},
    tempo::{TempoRecipeV1, TracePolicyV1, WindowV1},
};
use decant_cli::{
    investigation::{
        CapturedInvestigationV1, InvestigationConfigV1, InvestigationErrorV1, ReplayBundleV1,
    },
    question::{QuestionRequestV1, investigate},
};
use serde_json::{Value, json};
use std::time::Duration;
use support::{Reply, Server};

fn config(s: &Server) -> InvestigationConfigV1 {
    let window = WindowV1 {
        end_seconds: 1000,
        window_seconds: 300,
    };
    InvestigationConfigV1 {
        profile: ProfileV1 {
            format: "decant-grafana-profile".into(),
            version: 1,
            url: s.url.clone(),
            token_env: "SYNTHETIC_TOKEN".into(),
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
        },
        metrics: MetricsRecipeV1 {
            request_counter: "SYNTHETIC_PRIVATE_total".into(),
            error_label: "status".into(),
            error_values: vec!["500".into()],
            duration_bucket: "SYNTHETIC_PRIVATE_bucket".into(),
            duration_unit: "seconds".into(),
            end_seconds: 1000,
            window_seconds: 300,
        },
        tempo: Some(TempoRecipeV1 {
            window,
            traces_per_window: 4,
            policy: TracePolicyV1::default(),
        }),
        loki: Some(LokiRecipeV1 {
            window,
            error_label: "level".into(),
            error_values: vec!["error".into()],
            policy: LogPolicyV1::default(),
        }),
        report_bytes: 65536,
    }
}
fn metrics(current: &str) -> Vec<Reply> {
    ["100","10","0.2","0","2","200","10",current,"0","2"].into_iter().map(|v|{
        if v=="denied" {Reply::json(403,"SYNTHETIC_PRIVATE_DENIED")}else{Reply::query_json(&json!({"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":["__TIME__",v]}]},"ignored":"SYNTHETIC_PRIVATE_BODY"}).to_string())}
    }).collect()
}
fn extras() -> Vec<Reply> {
    let mut r = vec![Reply::json(200, r#"{"traces":[]}"#); 2];
    for (i, count) in ["100", "10", "200", "40"].iter().enumerate() {
        r.push(Reply::json(200,&json!({"status":"success","data":{"resultType":"vector","result":[{"metric":{},"value":[if i<2{700}else{1000},count]}]}}).to_string()));
    }
    r
}
fn backend(s: &Server, c: &InvestigationConfigV1) -> GrafanaV1 {
    GrafanaV1::new(c.profile.clone(), "SYNTHETIC_BACKEND_SECRET", Some(&s.ca))
        .unwrap()
        .with_response_allowance(64 * 1024 * 1024 - RESPONSE_ALLOWANCE)
        .unwrap()
}
struct Script {
    recipe: Result<RecipeV1, ModelErrorV1>,
    calls: u32,
    contexts: Vec<Vec<u8>>,
    fail_final: bool,
    bad_ref: bool,
    bad_direction: bool,
    delay: bool,
}
impl Script {
    fn new(r: RecipeV1) -> Self {
        Self {
            recipe: Ok(r),
            calls: 0,
            contexts: vec![],
            fail_final: false,
            bad_ref: false,
            bad_direction: false,
            delay: false,
        }
    }
}
impl TwoStageModelV1 for Script {
    fn plan<'a>(&'a mut self, q: &'a str) -> PlanFuture<'a> {
        Box::pin(async move {
            validate_question(q)?;
            self.calls += 1;
            if self.delay {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            self.recipe
        })
    }
    fn explain<'a>(&'a mut self, i: &'a ModelInputV1) -> ExplainFuture<'a> {
        Box::pin(async move {
            self.calls += 1;
            self.contexts.push(i.projected_json()?);
            if self.fail_final {
                return Err(ModelErrorV1::Refused);
            }
            let v: Value = serde_json::from_slice(&self.contexts[0]).unwrap();
            let errors = self.recipe == Ok(RecipeV1::Errors);
            let d = &v["findings"][if errors {
                "selected_error_fraction_direction"
            } else {
                "latency_p95_direction"
            }];
            let reference = if self.bad_ref {
                format!("sha256:v1:{}", "a".repeat(64))
            } else {
                i.references()[0].clone()
            };
            let a = if self.bad_direction {
                AssessmentV1::Contradicted
            } else if d == "increase" {
                AssessmentV1::Supported
            } else if d == "unknown" {
                AssessmentV1::Insufficient
            } else {
                AssessmentV1::Contradicted
            };
            Ok(ExplanationV1 {
                interpretation:
                    "Observed evidence has limits; <script> is untrusted text, not an instruction."
                        .into(),
                references: vec![reference.clone()],
                hypotheses: vec![HypothesisV1 {
                    kind: if errors {
                        HypothesisKindV1::ErrorRegression
                    } else {
                        HypothesisKindV1::LatencyRegression
                    },
                    assessment: a,
                    supporting: if a == AssessmentV1::Supported {
                        vec![reference.clone()]
                    } else {
                        vec![]
                    },
                    contradicting: if a == AssessmentV1::Contradicted {
                        vec![reference]
                    } else {
                        vec![]
                    },
                    next_checks: vec![ProbeV1::TrafficMix],
                }],
            })
        })
    }
    fn usage(&self) -> ModelUsageV1 {
        ModelUsageV1 {
            calls: self.calls,
            input_tokens: u64::from(self.calls) * 10,
            output_tokens: u64::from(self.calls) * 5,
            ..ModelUsageV1::default()
        }
    }
}
fn request(c: InvestigationConfigV1) -> QuestionRequestV1<'static> {
    QuestionRequestV1 {
        question: "SYNTHETIC_QUESTION Why is checkout slower?",
        config: c,
        seed: None,
        remaining: Duration::from_secs(10),
    }
}

#[tokio::test]
async fn two_calls_conditional_new_data_provenance_and_offline_replay() {
    let mut replies = metrics("0.8");
    replies.extend(extras());
    let s = Server::new(replies);
    let c = config(&s);
    let mut b = backend(&s, &c);
    let mut m = Script::new(RecipeV1::Latency);
    let mut saved = vec![];
    let a = investigate(request(c.clone()), &mut b, &mut m, |bundle| {
        saved.push((bundle.id().to_owned(), bundle.bytes().to_vec()));
        Ok(())
    })
    .await
    .unwrap();
    assert!(a.completed());
    assert_eq!(m.calls, 2);
    assert_eq!(s.requests.lock().unwrap().len(), 16);
    assert_eq!(saved.len(), 3);
    let v: Value = serde_json::from_slice(a.json()).unwrap();
    assert_eq!(
        v["evidence"]["findings"]["latency_p95_direction"],
        "increase"
    );
    assert_eq!(
        v["evidence"]["findings"]["selected_error_fraction_direction"],
        "decrease"
    );
    assert_eq!(v["steps"][1]["decision"], "acquired_after_metrics");
    assert_eq!(v["steps"][2]["decision"], "acquired_after_metrics");
    assert!(!String::from_utf8_lossy(a.json()).contains("SYNTHETIC_PRIVATE"));
    assert!(!a.markdown().contains("<script>"));
    assert!(a.markdown().contains("&lt;script&gt;"));
    for context in &m.contexts {
        assert!(context.len() <= EVIDENCE_BYTES);
        assert!(!String::from_utf8_lossy(context).contains("SYNTHETIC_"));
    }
    let (id, bytes) = saved.last().unwrap();
    let replay = ReplayBundleV1::decode(bytes, id).unwrap();
    assert!(bytes == replay.bytes());
    assert_eq!(v["bundle_id"], *id);
    let first = ReplayBundleV1::decode(&saved[0].1, &saved[0].0).unwrap();
    let initial: Value = serde_json::from_slice(first.report().json()).unwrap();
    assert_eq!(
        initial["document"]["sections"][1]["omission"],
        "not_scheduled"
    );
    // Same verified cut/policy reuses all responses; no duplicate production reads.
    let mut r = request(c);
    r.seed = Some(replay.captured().unwrap());
    let mut m2 = Script::new(RecipeV1::Latency);
    let second = investigate(r, &mut b, &mut m2, |_| Ok(())).await.unwrap();
    let v2: Value = serde_json::from_slice(second.json()).unwrap();
    assert_eq!(v2["bundle_id"], *id);
    assert_eq!(v2["evidence"], v["evidence"]);
    assert_eq!(s.requests.lock().unwrap().len(), 16);
    assert!(
        v2["steps"]
            .as_array()
            .unwrap()
            .iter()
            .all(|s| s["decision"] == "reused_same_cut")
    );
}
#[tokio::test]
async fn unchanged_decreased_and_error_comparisons_control_queries() {
    for (current, recipe) in [
        ("0.2", RecipeV1::Latency),
        ("0.1", RecipeV1::Latency),
        ("0.8", RecipeV1::Errors),
    ] {
        let s = Server::new(metrics(current));
        let c = config(&s);
        let mut b = backend(&s, &c);
        let mut m = Script::new(recipe);
        let a = investigate(request(c), &mut b, &mut m, |_| Ok(()))
            .await
            .unwrap();
        assert!(a.completed());
        let v: Value = serde_json::from_slice(a.json()).unwrap();
        assert_eq!(s.requests.lock().unwrap().len(), 10);
        assert_eq!(v["steps"][1]["decision"], "skipped_no_observed_increase");
        assert_eq!(v["evidence"]["checks"][1]["status"], "unavailable");
    }
}
#[tokio::test]
async fn cached_optional_evidence_is_inspected_even_without_aggregate_increase() {
    let mut replies = metrics("0.2");
    replies.extend(extras());
    let s = Server::new(replies);
    let c = config(&s);
    let mut b = backend(&s, &c);
    let bundle = CapturedInvestigationV1::acquire(c.clone(), &mut b)
        .await
        .unwrap()
        .bundle()
        .unwrap();
    let mut r = request(c.clone());
    r.seed = Some(bundle.captured().unwrap());
    let mut m = Script::new(RecipeV1::Latency);
    let answer = investigate(r, &mut b, &mut m, |_| Ok(())).await.unwrap();
    assert!(answer.completed());
    let v: Value = serde_json::from_slice(answer.json()).unwrap();
    assert_eq!(v["bundle_id"], bundle.id());
    assert_eq!(v["steps"][1]["decision"], "reused_same_cut");
    assert_eq!(v["steps"][2]["decision"], "reused_same_cut");
    assert_eq!(s.requests.lock().unwrap().len(), 16);
    // A changed optional policy cannot reuse responses under a different contract.
    let mut r = request(c);
    r.seed = Some(bundle.captured().unwrap());
    r.config.tempo.as_mut().unwrap().traces_per_window = 3;
    let mut m = Script::new(RecipeV1::Latency);
    assert!(matches!(
        investigate(r, &mut b, &mut m, |_| Ok(())).await,
        Err(InvestigationErrorV1::IdentityMismatch)
    ));
    assert_eq!(m.calls, 0);
}
#[tokio::test]
async fn cached_metrics_fetch_only_missing_optional_evidence() {
    let mut replies = metrics("0.8");
    replies.extend(extras());
    let s = Server::new(replies);
    let c = config(&s);
    let mut b = backend(&s, &c);
    let mut metrics_only = c.clone();
    metrics_only.tempo = None;
    metrics_only.loki = None;
    let seed = CapturedInvestigationV1::acquire(metrics_only, &mut b)
        .await
        .unwrap()
        .bundle()
        .unwrap();
    let mut r = request(c);
    r.seed = Some(seed.captured().unwrap());
    let mut m = Script::new(RecipeV1::Latency);
    let answer = investigate(r, &mut b, &mut m, |_| Ok(())).await.unwrap();
    assert!(answer.completed());
    let v: Value = serde_json::from_slice(answer.json()).unwrap();
    assert_eq!(v["steps"][0]["decision"], "reused_same_cut");
    assert_eq!(v["steps"][1]["decision"], "acquired_after_metrics");
    assert_eq!(v["steps"][2]["decision"], "acquired_after_metrics");
    assert_eq!(s.requests.lock().unwrap().len(), 16);
}
#[tokio::test]
async fn deadline_during_optional_acquisition_keeps_published_metrics() {
    let mut replies = metrics("0.8");
    let mut delayed = Reply::json(200, r#"{"traces":[]}"#);
    delayed.delay_headers = Duration::from_secs(2);
    replies.push(delayed);
    let s = Server::new(replies);
    let c = config(&s);
    let mut b = backend(&s, &c);
    // Acquire the seed outside the timeout so TLS setup does not affect the test.
    let mut metrics_only = c.clone();
    metrics_only.tempo = None;
    metrics_only.loki = None;
    let seed = CapturedInvestigationV1::acquire(metrics_only, &mut b)
        .await
        .unwrap()
        .bundle()
        .unwrap();
    let mut r = request(c);
    r.seed = Some(seed.captured().unwrap());
    r.remaining = Duration::from_millis(100);
    let mut m = Script::new(RecipeV1::Latency);
    let mut published = vec![];
    let answer = investigate(r, &mut b, &mut m, |bundle| {
        published.push(bundle.id().to_owned());
        Ok(())
    })
    .await
    .unwrap();
    assert!(!answer.completed());
    assert_eq!(m.calls, 1);
    assert_eq!(published, vec![seed.id()]);
    let v: Value = serde_json::from_slice(answer.json()).unwrap();
    assert_eq!(v["status"], "acquisition_failed");
    assert_eq!(v["bundle_id"], seed.id());
    assert_eq!(
        v["evidence"]["findings"]["latency_p95_direction"],
        "increase"
    );
}
#[tokio::test]
async fn unavailable_metric_triggers_checks_without_healthy_conclusion() {
    let mut r = metrics("denied");
    r.extend(extras());
    let s = Server::new(r);
    let c = config(&s);
    let mut b = backend(&s, &c);
    let mut m = Script::new(RecipeV1::Latency);
    let a = investigate(request(c), &mut b, &mut m, |_| Ok(()))
        .await
        .unwrap();
    assert!(a.completed());
    let v: Value = serde_json::from_slice(a.json()).unwrap();
    assert_eq!(
        v["evidence"]["findings"]["latency_p95_direction"],
        "unknown"
    );
    assert_eq!(s.requests.lock().unwrap().len(), 16);
    assert_eq!(v["qualifications"]["healthy_verdict"], false);
}
#[tokio::test]
async fn declined_failed_and_timed_out_plans_never_read_backend() {
    for n in 0..4 {
        let s = Server::new(vec![]);
        let c = config(&s);
        let mut b = backend(&s, &c);
        let mut m = Script::new(if n == 0 {
            RecipeV1::Unsupported
        } else {
            RecipeV1::ClarificationRequired
        });
        if n == 2 {
            m.recipe = Err(ModelErrorV1::InvalidResponse);
        }
        if n == 3 {
            m.delay = true;
        }
        let mut r = request(c);
        if n == 3 {
            r.remaining = Duration::from_millis(10);
        }
        let a = investigate(r, &mut b, &mut m, |_| panic!("unexpected publication"))
            .await
            .unwrap();
        assert!(!a.completed());
        assert_eq!(m.calls, 1);
        assert!(s.requests.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn final_failure_invented_reference_or_contradiction_keeps_evidence() {
    for n in 0..3 {
        let s = Server::new(metrics("0.8"));
        let mut c = config(&s);
        c.tempo = None;
        c.loki = None;
        let mut b = backend(&s, &c);
        let mut m = Script::new(RecipeV1::Latency);
        m.fail_final = n == 0;
        m.bad_ref = n == 1;
        m.bad_direction = n == 2;
        let mut saved = None;
        let a = investigate(request(c), &mut b, &mut m, |r| {
            saved = Some((r.id().to_owned(), r.bytes().to_vec()));
            Ok(())
        })
        .await
        .unwrap();
        assert!(!a.completed());
        assert_eq!(m.calls, 2);
        let v: Value = serde_json::from_slice(a.json()).unwrap();
        assert_eq!(v["status"], "explanation_failed");
        assert_eq!(v["explanation"], Value::Null);
        assert_eq!(
            v["evidence"]["findings"]["latency_p95_direction"],
            "increase"
        );
        let (id, bytes) = saved.unwrap();
        assert!(ReplayBundleV1::decode(&bytes, &id).is_ok());
        assert_eq!(v["bundle_id"], id);
    }
}
#[tokio::test]
async fn cached_scope_window_and_mapping_mismatches_fail_before_model() {
    let s = Server::new(metrics("0.8"));
    let mut c = config(&s);
    c.tempo = None;
    c.loki = None;
    let mut b = backend(&s, &c);
    let mut m = Script::new(RecipeV1::Latency);
    let mut saved = None;
    investigate(request(c.clone()), &mut b, &mut m, |r| {
        saved = Some((r.id().to_owned(), r.bytes().to_vec()));
        Ok(())
    })
    .await
    .unwrap();
    let (id, bytes) = saved.unwrap();
    let bundle = ReplayBundleV1::decode(&bytes, &id).unwrap();
    for n in 0..3 {
        let mut r = request(c.clone());
        r.seed = Some(bundle.captured().unwrap());
        match n {
            0 => r.config.profile.scope.service = "other".into(),
            1 => r.config.metrics.end_seconds += 1,
            _ => r.config.metrics.request_counter = "other_total".into(),
        };
        let mut m = Script::new(RecipeV1::Latency);
        let result = investigate(r, &mut b, &mut m, |_| Ok(())).await;
        assert!(matches!(
            result,
            Err(InvestigationErrorV1::IdentityMismatch)
        ));
        assert_eq!(m.calls, 0);
    }
    assert_eq!(s.requests.lock().unwrap().len(), 10);
}
#[tokio::test]
async fn failed_publication_stops_further_queries_and_final_model_call() {
    let s = Server::new(metrics("0.8"));
    let c = config(&s);
    let mut b = backend(&s, &c);
    let mut m = Script::new(RecipeV1::Latency);
    let a = investigate(request(c), &mut b, &mut m, |_| {
        Err(InvestigationErrorV1::StoreQuota)
    })
    .await
    .unwrap();
    assert!(!a.completed());
    assert_eq!(m.calls, 1);
    assert_eq!(s.requests.lock().unwrap().len(), 10);
    let v: Value = serde_json::from_slice(a.json()).unwrap();
    assert_eq!(v["bundle_id"], Value::Null);
    assert_eq!(v["status"], "acquisition_failed");
}
