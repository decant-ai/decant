//! Inspect one immutable capture. No queries, credentials, clock-derived facts or fixes.
use crate::investigation::{InvestigationErrorV1 as E, InvestigationReportV1};
use decant_backend::{
    ArtifactDomainV1, artifact_hash_v1,
    model::{
        AssessmentV1, DecisionV1, HypothesisKindV1, HypothesisV1, ModelErrorV1, ModelInputV1,
        ModelUsageV1, ModelV1, ProbeV1,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeSet, fmt, time::Duration};
use tokio::time::Instant;

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FocusV1 {
    #[default]
    Overview,
    Latency,
    Errors,
    Dependencies,
    Gaps,
}
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StopV1 {
    EvidenceOnly,
    Completed,
    RepeatedProbe,
    InvalidDecision,
    ModelFailed,
    CallLimit,
    Deadline,
}
#[derive(Clone, Copy, Debug, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DirectionV1 {
    Increase,
    Decrease,
    Unchanged,
    Mixed,
    Unknown,
}
fn direction<T: PartialOrd>(a: Option<T>, b: Option<T>) -> DirectionV1 {
    match a.zip(b) {
        Some((a, b)) if b > a => DirectionV1::Increase,
        Some((a, b)) if b < a => DirectionV1::Decrease,
        Some(_) => DirectionV1::Unchanged,
        None => DirectionV1::Unknown,
    }
}
#[derive(Clone, Serialize)]
struct Check {
    probe: ProbeV1,
    reference: String,
    status: &'static str,
    reason: &'static str,
    facts: Value,
}
pub struct PreparedInvestigationV1 {
    report_id: String,
    window: Value,
    checks: Vec<Check>,
    findings: Value,
    expected: [DirectionV1; 6],
}
impl fmt::Debug for PreparedInvestigationV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("PreparedInvestigationV1 { redacted }")
    }
}
fn pick(value: &Value, keys: &[&str]) -> Value {
    let mut m = serde_json::Map::new();
    for k in keys {
        if let Some(v) = value.get(*k) {
            m.insert((*k).into(), v.clone());
        }
    }
    Value::Object(m)
}
fn numeric(v: &Value) -> Option<f64> {
    v.as_f64().filter(|n| n.is_finite())
}
fn metric_value(f: &Value, i: usize) -> Option<f64> {
    (f["measurements"][i]["status"] == "observed")
        .then(|| numeric(&f["measurements"][i]["value"]))
        .flatten()
}
impl PreparedInvestigationV1 {
    pub fn new(report: &InvestigationReportV1) -> Result<Self, E> {
        let envelope: Value = serde_json::from_slice(report.json()).map_err(|_| E::InvalidInput)?;
        let d = &envelope["document"];
        let sections = d["sections"].as_array().ok_or(E::InvalidInput)?;
        let section = |name: &str| -> &Value {
            sections
                .iter()
                .find(|s| s["signal"] == name)
                .unwrap_or(&Value::Null)
        };
        let m = &section("metrics")["facts"];
        let t = &section("tempo")["facts"];
        let l = &section("loki")["facts"];
        let latency = direction(metric_value(m, 2), metric_value(m, 7));
        let errors = if m["consistency_gaps"]
            .as_array()
            .is_some_and(|g| !g.is_empty())
        {
            DirectionV1::Unknown
        } else {
            direction(
                numeric(&m["error_fraction"]["baseline"]["value"]),
                numeric(&m["error_fraction"]["current"]["value"]),
            )
        };
        let logs = direction(
            l["counts"][1]["count"]
                .as_u64()
                .filter(|_| l["counts"][1]["status"] == "observed"),
            l["counts"][3]["count"]
                .as_u64()
                .filter(|_| l["counts"][3]["status"] == "observed"),
        );
        let mut checks = Vec::new();
        let mut downstream = DirectionV1::Unknown;
        for probe in ProbeV1::ALL {
            let reference = artifact_hash_v1(
                ArtifactDomainV1::Probe,
                &serde_json::to_vec(&json!([report.id(), probe])).map_err(|_| E::InvalidInput)?,
            )?;
            let (status, reason, facts) = match probe {
                ProbeV1::Metrics if !m.is_null() => {
                    let rows = m["measurements"].as_array().ok_or(E::InvalidInput)?;
                    let projected: Vec<_> = rows
                        .iter()
                        .map(|v| {
                            pick(
                                v,
                                &[
                                    "metric",
                                    "period",
                                    "unit",
                                    "status",
                                    "value",
                                    "reason",
                                    "warning_count",
                                    "info_count",
                                    "attempted",
                                ],
                            )
                        })
                        .collect();
                    let status = if m["complete_measurements"] == true {
                        "checked"
                    } else if rows.iter().any(|v| v["value"].is_number()) {
                        "partial"
                    } else {
                        "unavailable"
                    };
                    (
                        status,
                        "backend_estimates",
                        json!({"measurements":projected,"error_fraction":m["error_fraction"],"delta":m["delta"],"consistency_gaps":m["consistency_gaps"],"contributing_series_changed":m["contributing_series_changed"],"qualifications":m["qualifications"]}),
                    )
                }
                ProbeV1::Operations | ProbeV1::Dependencies if !t.is_null() => {
                    let kind = if probe == ProbeV1::Operations {
                        "operation"
                    } else {
                        "dependency"
                    };
                    let groups:Vec<_>=t["groups"].as_array().ok_or(E::InvalidInput)?.iter().filter(|g|g["kind"]==kind).map(|g|json!({"group_id":g["group_id"],"baseline":pick(&g["baseline"],&["observed_spans","error_spans","unknown_status_spans","p95_duration_ns","examples","examples_omitted"]),"current":pick(&g["current"],&["observed_spans","error_spans","unknown_status_spans","p95_duration_ns","examples","examples_omitted"])})).collect();
                    if probe == ProbeV1::Dependencies {
                        let mut up = false;
                        let mut down = false;
                        let mut any = false;
                        for g in &groups {
                            match direction(
                                g["baseline"]["p95_duration_ns"].as_u64(),
                                g["current"]["p95_duration_ns"].as_u64(),
                            ) {
                                DirectionV1::Increase => {
                                    up = true;
                                    any = true
                                }
                                DirectionV1::Decrease => {
                                    down = true;
                                    any = true
                                }
                                DirectionV1::Unchanged => any = true,
                                _ => (),
                            }
                        }
                        downstream = match (up, down, any) {
                            (true, true, _) => DirectionV1::Mixed,
                            (true, false, _) => DirectionV1::Increase,
                            (false, true, _) => DirectionV1::Decrease,
                            (false, false, true) => DirectionV1::Unchanged,
                            _ => DirectionV1::Unknown,
                        };
                    }
                    // First-N trace observations are always partial investigation
                    // evidence, even when each HTTP response was complete.
                    (
                        "partial",
                        "sampled_first_n_traces",
                        json!({"groups":groups,"gaps":t["gaps"],"searches":t["searches"],"response_outcomes":receipt_outcomes(t),"qualifications":t["qualifications"]}),
                    )
                }
                ProbeV1::Logs if !l.is_null() => {
                    let all = l["counts"]
                        .as_array()
                        .ok_or(E::InvalidInput)?
                        .iter()
                        .all(|c| c["status"] == "observed");
                    (
                        if all { "checked" } else { "partial" },
                        "log_counts_not_request_rates",
                        json!({"counts":l["counts"],"selected_error_fraction":l["selected_error_fraction"],"delta_all_logs":l["delta_all_logs"],"delta_selected_error_logs":l["delta_selected_error_logs"],"response_outcomes":receipt_outcomes(l),"qualifications":l["qualifications"],"example_text_omitted":true}),
                    )
                }
                ProbeV1::TrafficMix => (
                    "unavailable",
                    "operation_traffic_weights_not_acquired",
                    json!({}),
                ),
                ProbeV1::Versions => ("unavailable", "version_comparison_not_acquired", json!({})),
                _ => {
                    let source = match probe {
                        ProbeV1::Metrics => section("metrics"),
                        ProbeV1::Logs => section("loki"),
                        _ => section("tempo"),
                    };
                    let reason = match source["omission"].as_str() {
                        Some("report_budget") => "report_budget",
                        Some("not_configured") => "not_configured",
                        _ => "not_scheduled",
                    };
                    ("unavailable", reason, json!({}))
                }
            };
            checks.push(Check {
                probe,
                reference,
                status,
                reason,
                facts,
            });
        }
        let expected = [
            latency,
            errors,
            downstream,
            logs,
            DirectionV1::Unknown,
            DirectionV1::Unknown,
        ];
        let findings = json!({"latency_p95_direction":latency,"selected_error_fraction_direction":errors,"sampled_dependency_p95_direction":downstream,"selected_error_log_count_direction":logs,
            "interpretation":"observed_difference_not_statistical_significance_or_causality"});
        let prepared = Self {
            report_id: report.id().into(),
            window: d["window"].clone(),
            checks,
            findings,
            expected,
        };
        // Bound once before model access; no model may force larger allocations.
        prepared.finish(
            FocusV1::Overview,
            StopV1::EvidenceOnly,
            vec![],
            vec![],
            0,
            ModelUsageV1::default(),
            None,
        )?;
        Ok(prepared)
    }
    pub fn evidence(&self, focus: FocusV1) -> Result<AssessmentReportV1, E> {
        self.finish(
            focus,
            StopV1::EvidenceOnly,
            vec![],
            vec![],
            0,
            ModelUsageV1::default(),
            None,
        )
    }
    pub fn probe_reference(&self, probe: ProbeV1) -> &str {
        &self.checks[probe_index(probe)].reference
    }
    pub async fn reason(
        &self,
        focus: FocusV1,
        provider: &mut impl ModelV1,
        remaining: Duration,
    ) -> Result<AssessmentReportV1, E> {
        if remaining > Duration::from_secs(300) {
            return Err(E::InvalidInput);
        }
        let deadline = Instant::now() + remaining;
        let mut inspected = BTreeSet::new();
        inspected.insert(ProbeV1::Metrics);
        let mut history = vec![];
        for calls in 0..6 {
            if Instant::now() >= deadline {
                return self.finish(
                    focus,
                    StopV1::Deadline,
                    vec![],
                    history,
                    calls,
                    provider.usage(),
                    None,
                );
            }
            let visible: Vec<_> = self
                .checks
                .iter()
                .filter(|c| inspected.contains(&c.probe))
                .collect();
            let references: Vec<_> = visible.iter().map(|c| c.reference.clone()).collect();
            let catalog:Vec<_>=self.checks.iter().map(|c|json!({"probe":c.probe,"status":c.status,"reason":c.reason,"inspected":inspected.contains(&c.probe)})).collect();
            let context=serde_json::to_vec(&json!({"report_id":self.report_id,"window":self.window,"focus":focus,"mandatory_checks":catalog,"observed_findings":self.findings,"views":visible,"remaining_calls":6-calls,"qualifications":qualifications()})).map_err(|_|E::InvalidInput)?;
            let input = ModelInputV1::from_projected(&context, references.clone())
                .map_err(|_| E::OutputLimit)?;
            let reply = tokio::time::timeout_at(deadline, provider.decide(&input)).await;
            let decision = match reply {
                Err(_) => {
                    return self.finish(
                        focus,
                        StopV1::Deadline,
                        vec![],
                        history,
                        calls + 1,
                        provider.usage(),
                        None,
                    );
                }
                Ok(Err(error)) => {
                    return self.finish(
                        focus,
                        StopV1::ModelFailed,
                        vec![],
                        history,
                        calls + 1,
                        provider.usage(),
                        Some(error),
                    );
                }
                Ok(Ok(d)) => d,
            };
            if decision.validate(&references).is_err() {
                return self.finish(
                    focus,
                    StopV1::InvalidDecision,
                    vec![],
                    history,
                    calls + 1,
                    provider.usage(),
                    None,
                );
            }
            match decision {
                DecisionV1::Inspect { probe } => {
                    if !inspected.insert(probe) {
                        return self.finish(
                            focus,
                            StopV1::RepeatedProbe,
                            vec![],
                            history,
                            calls + 1,
                            provider.usage(),
                            None,
                        );
                    }
                    history.push(probe);
                }
                DecisionV1::Finish { hypotheses } => {
                    if !self.assessments_valid(&hypotheses) {
                        return self.finish(
                            focus,
                            StopV1::InvalidDecision,
                            vec![],
                            history,
                            calls + 1,
                            provider.usage(),
                            None,
                        );
                    }
                    return self.finish(
                        focus,
                        StopV1::Completed,
                        hypotheses,
                        history,
                        calls + 1,
                        provider.usage(),
                        None,
                    );
                }
            }
        }
        self.finish(
            focus,
            StopV1::CallLimit,
            vec![],
            history,
            6,
            provider.usage(),
            None,
        )
    }
    fn assessments_valid(&self, hs: &[HypothesisV1]) -> bool {
        hs.iter().all(|h| {
            let (index, probe) = match h.kind {
                HypothesisKindV1::LatencyRegression => (0, ProbeV1::Metrics),
                HypothesisKindV1::ErrorRegression => (1, ProbeV1::Metrics),
                HypothesisKindV1::DownstreamSlowdown => (2, ProbeV1::Dependencies),
                HypothesisKindV1::LogErrorIncrease => (3, ProbeV1::Logs),
                HypothesisKindV1::TrafficMixChange => (4, ProbeV1::TrafficMix),
                HypothesisKindV1::VersionChange => (5, ProbeV1::Versions),
            };
            let expected = self.expected[index];
            // Conservative insufficiency is always permitted; never force an
            // interpretation from numerically valid but context-poor evidence.
            if h.assessment == AssessmentV1::Insufficient {
                return true;
            }
            let check = &self.checks[probe_index(probe)];
            if check.status == "unavailable"
                || h.supporting
                    .iter()
                    .chain(&h.contradicting)
                    .any(|id| id != &check.reference)
            {
                return false;
            }
            matches!(
                (expected, h.assessment),
                (DirectionV1::Increase, AssessmentV1::Supported)
                    | (
                        DirectionV1::Decrease | DirectionV1::Unchanged,
                        AssessmentV1::Contradicted
                    )
            )
        })
    }
    #[allow(clippy::too_many_arguments)]
    fn finish(
        &self,
        focus: FocusV1,
        stop: StopV1,
        hypotheses: Vec<HypothesisV1>,
        inspections: Vec<ProbeV1>,
        calls: u32,
        usage: ModelUsageV1,
        error: Option<ModelErrorV1>,
    ) -> Result<AssessmentReportV1, E> {
        let missing: Vec<_> = self
            .checks
            .iter()
            .filter(|c| c.status != "checked")
            .map(|c| json!({"probe":c.probe,"status":c.status,"reason":c.reason}))
            .collect();
        let value = json!({"format":"decant-investigation-assessment","version":1,"evidence_report_id":self.report_id,"window":self.window,"focus":focus,"stop":stop,"model_calls":calls,
            "findings":self.findings,"checks":self.checks,"interpretations":hypotheses,"model_inspections":inspections,"model_usage":usage,"model_error":error,"missing_checks":missing,"qualifications":qualifications()});
        let json = serde_json::to_vec(&value).map_err(|_| E::InvalidInput)?;
        if json.len() > 60 * 1024 {
            return Err(E::OutputLimit);
        }
        let mut markdown = format!(
            "# Decant investigation\n\nEvidence report: `{}`\n\n",
            self.report_id
        );
        use std::fmt::Write;
        writeln!(&mut markdown,"Observed comparisons (not significance or causal proof):\n\n- Latency p95: {:?}\n- Selected-error fraction: {:?}\n- Sampled dependency p95: {:?}\n- Selected-error log count: {:?}\n",self.expected[0],self.expected[1],self.expected[2],self.expected[3]).map_err(|_|E::InvalidInput)?;
        let metrics = &self.checks[0].facts;
        markdown.push_str("| Backend measurement | Baseline | Current |\n| --- | ---: | ---: |\n");
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
                &mut markdown,
                "| {label} | {} | {} |",
                metrics["measurements"][i]["value"],
                metrics["measurements"][i + 5]["value"]
            )
            .map_err(|_| E::InvalidInput)?;
        }
        markdown.push_str("\n## Model interpretations\n\n");
        if hypotheses.is_empty() {
            markdown.push_str(
                "No accepted model interpretation. Deterministic evidence is retained.\n",
            );
        }
        for h in &hypotheses {
            writeln!(
                &mut markdown,
                "- {:?}: {:?} (interpretation, not causal proof).",
                h.kind, h.assessment
            )
            .map_err(|_| E::InvalidInput)?;
        }
        writeln!(&mut markdown,"\nStop: {stop:?}. Model calls: {calls}. Unknown-usage calls: {}.\n\n## Checks and gaps\n",usage.unknown_usage_calls).map_err(|_|E::InvalidInput)?;
        for c in &self.checks {
            writeln!(
                &mut markdown,
                "- {:?}: {} ({}) — `{}`",
                c.probe, c.status, c.reason, c.reference
            )
            .map_err(|_| E::InvalidInput)?;
        }
        markdown.push_str("\nSampling/population coverage remain unknown. No atomic cross-source snapshot, health verdict or automatic fix.\n\n<pre>\n");
        // Compact exact JSON rather than pretty-print expansion. No telemetry
        // strings survive projection, but escape independently at rendering.
        let text = std::str::from_utf8(&json).map_err(|_| E::InvalidInput)?;
        for ch in text.chars() {
            match ch {
                '&' => markdown.push_str("&amp;"),
                '<' => markdown.push_str("&lt;"),
                '>' => markdown.push_str("&gt;"),
                _ => markdown.push(ch),
            }
        }
        markdown.push_str("\n</pre>\n");
        if markdown.len() > 64 * 1024 {
            return Err(E::OutputLimit);
        }
        Ok(AssessmentReportV1 {
            json,
            markdown,
            stop,
        })
    }
}
fn receipt_outcomes(v: &Value) -> Vec<Value> {
    v["receipts"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .map(|r| {
                    pick(
                        r,
                        &[
                            "status",
                            "reason",
                            "attempted",
                            "warning_count",
                            "info_count",
                        ],
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}
fn probe_index(p: ProbeV1) -> usize {
    match p {
        ProbeV1::Metrics => 0,
        ProbeV1::Operations => 1,
        ProbeV1::Dependencies => 2,
        ProbeV1::Logs => 3,
        ProbeV1::TrafficMix => 4,
        ProbeV1::Versions => 5,
    }
}
fn qualifications() -> Value {
    json!({"mode":"captured_read_only_evidence","sampling":"unknown","population_coverage":"unknown","atomic_snapshot":false,"causal_proof":false,"health_verdict":false,"interpretations_are_model_output":true,"valid_references_prove_entailment":false,"model_egress":"numeric_facts_closed_statuses_opaque_ids_only","log_text_in_model_context":false,"traffic_mix_and_version_checks":"unavailable"})
}
pub struct AssessmentReportV1 {
    json: Vec<u8>,
    markdown: String,
    stop: StopV1,
}
impl fmt::Debug for AssessmentReportV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AssessmentReportV1")
            .field("stop", &self.stop)
            .finish_non_exhaustive()
    }
}
impl AssessmentReportV1 {
    pub fn json(&self) -> &[u8] {
        &self.json
    }
    pub fn markdown(&self) -> &str {
        &self.markdown
    }
    pub fn stop(&self) -> StopV1 {
        self.stop
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn integer_duration_comparison_does_not_round_above_f64_precision() {
        assert_eq!(
            direction(
                Some(9_007_199_254_740_992u64),
                Some(9_007_199_254_740_993u64)
            ),
            DirectionV1::Increase
        );
        assert_eq!(
            direction(Some(u64::MAX), Some(u64::MAX - 1)),
            DirectionV1::Decrease
        );
    }
}
