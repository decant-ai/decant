//! A planning call, deterministic acquisition, and an explanation call.
use crate::{
    investigation::{
        CapturedInvestigationV1, InvestigationConfigV1, InvestigationErrorV1 as E, ReplayBundleV1,
        adaptive,
    },
    investigator::PreparedInvestigationV1,
};
use decant_backend::{
    grafana::GrafanaV1,
    model::{
        ModelErrorV1, ModelInputV1, ModelUsageV1,
        two_stage::{ExplanationV1, RecipeV1, TwoStageModelV1, validate_question},
    },
};
use serde_json::{Value, json};
use std::{fmt::Write, time::Duration};
use tokio::time::{Instant, timeout_at};

pub struct QuestionAnswerV1 {
    json: Vec<u8>,
    markdown: String,
    completed: bool,
}
impl QuestionAnswerV1 {
    pub fn json(&self) -> &[u8] {
        &self.json
    }
    pub fn markdown(&self) -> &str {
        &self.markdown
    }
    pub fn completed(&self) -> bool {
        self.completed
    }
}
struct Saved {
    bundle: String,
    prepared: PreparedInvestigationV1,
}
struct Outcome {
    scope: Value,
    status: &'static str,
    recipe: Option<RecipeV1>,
    saved: Option<Saved>,
    steps: Vec<adaptive::StepV1>,
    explanation: Option<ExplanationV1>,
    error: Option<ModelErrorV1>,
    input: Option<ModelInputV1>,
}
impl Outcome {
    fn render(self, usage: ModelUsageV1, backend: &GrafanaV1) -> Result<QuestionAnswerV1, E> {
        let evidence: Option<Value> = self
            .input
            .as_ref()
            .map(|i| {
                i.projected_json()
                    .map_err(|_| E::InvalidInput)
                    .and_then(|b| serde_json::from_slice(&b).map_err(|_| E::InvalidInput))
            })
            .transpose()?;
        let v = json!({"format":"decant-investigation-answer","version":1,"status":self.status,"recipe":self.recipe,"scope":self.scope,
            "bundle_id":self.saved.as_ref().map(|s|&s.bundle),"evidence":evidence,"steps":self.steps,
            "explanation":self.explanation,"model_error":self.error,"model_usage":usage,"backend_usage":{"requests":backend.usage().attempts,"received_body_bytes":backend.usage().received_bytes},
            "qualifications":{"model_prose":"unverified_interpretation_not_measurement","references_prove_entailment":false,"causal_proof":false,"healthy_verdict":false,"question_persisted":false,"model_calls_limit":2,"automatic_retries":false,"cache":"explicit_same_cut_not_current_data","recipe_coverage":"latency_errors_only_traffic_mix_and_versions_unavailable"}});
        let mut bytes = serde_json::to_vec(&v).map_err(|_| E::InvalidInput)?;
        bytes.push(b'\n');
        let mut md = String::from("# Decant investigation answer\n\n");
        md.push_str("Confirmed scope (the question cannot override it):\n\n<pre>");
        escape(&mut md, &self.scope.to_string());
        md.push_str("</pre>\n\n");
        writeln!(
            &mut md,
            "Status: {}. Recipe: {:?}.\n",
            self.status, self.recipe
        )
        .map_err(|_| E::InvalidInput)?;
        if let Some(e) = &self.explanation {
            md.push_str("## Model explanation — unverified interpretation\n\n<pre>");
            escape(&mut md, &e.interpretation);
            md.push_str("</pre>\n\nEvidence references:\n\n");
            for r in &e.references {
                writeln!(&mut md, "- `{r}`").map_err(|_| E::InvalidInput)?;
            }
        } else {
            md.push_str("No accepted model explanation. Any saved evidence remains available.\n\n");
        }
        if let Some(e) = &evidence {
            md.push_str("## Calculated observations\n\n");
            if let Some(findings) = e["findings"].as_object() {
                for (k, v) in findings {
                    write!(&mut md, "- {k}: ").map_err(|_| E::InvalidInput)?;
                    escape(&mut md, &v.to_string());
                    md.push('\n');
                }
            }
            let metrics = e["views"]
                .as_array()
                .and_then(|a| a.iter().find(|c| c["probe"] == "metrics"));
            if let Some(m) = metrics {
                md.push_str(
                    "\n| Backend measurement | Baseline | Current |\n| --- | ---: | ---: |\n",
                );
                for (i, label) in [
                    "Requests/second (estimate)",
                    "Selected errors/second (estimate)",
                    "p95 seconds (estimate)",
                    "Observed resets",
                    "Contributing series",
                ]
                .into_iter()
                .enumerate()
                {
                    writeln!(
                        &mut md,
                        "| {label} | {} | {} |",
                        m["facts"]["measurements"][i]["value"],
                        m["facts"]["measurements"][i + 5]["value"]
                    )
                    .map_err(|_| E::InvalidInput)?;
                }
            }
            md.push_str("\n## Checks and gaps\n\n");
            if let Some(checks) = e["checks"].as_array() {
                for c in checks {
                    writeln!(
                        &mut md,
                        "- {}: {} ({})",
                        c["probe"], c["status"], c["reason"]
                    )
                    .map_err(|_| E::InvalidInput)?;
                }
            }
        }
        if let Some(s) = self.saved {
            writeln!(&mut md,"\nSaved evidence: `{}`. Inspect offline with `decant investigate report --bundle {}`.\n",s.bundle,s.bundle).map_err(|_|E::InvalidInput)?;
        }
        writeln!(&mut md,"Model calls: {}. Reported input tokens: {}; output tokens: {}; cache-read tokens: {}; cache-write tokens: {}; unknown-usage calls: {}. No cost-saving claim.\n",usage.calls,usage.input_tokens,usage.output_tokens,usage.cache_read_input_tokens,usage.cache_creation_input_tokens,usage.unknown_usage_calls).map_err(|_|E::InvalidInput)?;
        md.push_str("## Exact evidence and execution record\n\n<pre>");
        escape(
            &mut md,
            std::str::from_utf8(&bytes).map_err(|_| E::InvalidInput)?,
        );
        md.push_str("</pre>\n");
        if bytes.len() > 64 * 1024 || md.len() > 64 * 1024 {
            return Err(E::OutputLimit);
        }
        Ok(QuestionAnswerV1 {
            json: bytes,
            markdown: md,
            completed: self.status == "completed",
        })
    }
}
fn escape(out: &mut String, text: &str) {
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            _ => out.push(c),
        }
    }
}

pub struct QuestionRequestV1<'a> {
    pub question: &'a str,
    pub config: InvestigationConfigV1,
    pub seed: Option<CapturedInvestigationV1>,
    pub remaining: Duration,
}
pub async fn investigate(
    request: QuestionRequestV1<'_>,
    backend: &mut GrafanaV1,
    provider: &mut impl TwoStageModelV1,
    mut publish: impl FnMut(&ReplayBundleV1) -> Result<(), E>,
) -> Result<QuestionAnswerV1, E> {
    validate_question(request.question).map_err(|_| E::InvalidInput)?;
    request.config.validate()?;
    if request.remaining.is_zero() || request.remaining > Duration::from_secs(300) {
        return Err(E::InvalidInput);
    }
    if let Some(s) = &request.seed {
        adaptive::validate_seed(&request.config, s)?;
    }
    let deadline = Instant::now() + request.remaining;
    let mut o = Outcome {
        scope: serde_json::to_value(&request.config.profile.scope).map_err(|_| E::InvalidInput)?,
        status: "planning_failed",
        recipe: None,
        saved: None,
        steps: vec![],
        explanation: None,
        error: None,
        input: None,
    };
    let plan = timeout_at(deadline, provider.plan(request.question))
        .await
        .unwrap_or(Err(ModelErrorV1::BudgetExhausted));
    let recipe = match plan {
        Ok(r) => r,
        Err(e) => {
            o.error = Some(e);
            return o.render(provider.usage(), backend);
        }
    };
    o.recipe = Some(recipe);
    if matches!(
        recipe,
        RecipeV1::Unsupported | RecipeV1::ClarificationRequired
    ) {
        o.status = if recipe == RecipeV1::Unsupported {
            "unsupported_question"
        } else {
            "clarification_required"
        };
        return o.render(provider.usage(), backend);
    }
    let focus = adaptive::focus(recipe)?;
    let acquired = timeout_at(
        deadline,
        adaptive::acquire(request.config, request.seed, recipe, backend, |b| {
            publish(b)?;
            o.saved = Some(Saved {
                bundle: b.id().into(),
                prepared: PreparedInvestigationV1::new(b.report())?,
            });
            Ok(())
        }),
    )
    .await;
    if let Some(saved) = &o.saved {
        o.input = Some(saved.prepared.explanation_input(focus)?);
    }
    match acquired {
        Ok(Ok(a)) => o.steps = a.steps,
        _ => {
            o.status = "acquisition_failed";
            return o.render(provider.usage(), backend);
        }
    }
    let input = o.input.as_ref().ok_or(E::InvalidInput)?;
    let explained = timeout_at(deadline, provider.explain(input))
        .await
        .unwrap_or(Err(ModelErrorV1::BudgetExhausted));
    o.status = "explanation_failed";
    match explained {
        Ok(e)
            if e.validate(input.references()).is_ok()
                && o.saved
                    .as_ref()
                    .is_some_and(|s| s.prepared.accepts_hypotheses(&e.hypotheses)) =>
        {
            o.explanation = Some(e);
            o.status = "completed";
        }
        Ok(_) => o.error = Some(ModelErrorV1::InvalidResponse),
        Err(e) => o.error = Some(e),
    }
    o.render(provider.usage(), backend)
}
