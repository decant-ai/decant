//! Evidence-only investigation artifacts. No model, ambient clock or credentials.
pub mod adaptive;
mod bundle;
mod store;
pub use bundle::ReplayBundleV1;
use decant_backend::{
    ArtifactDomainV1 as Domain, ReadV1, artifact_hash_v1,
    grafana::{GrafanaV1, ProfileV1},
    loki::{LokiPlanV1, LokiRecipeV1},
    metrics::{CaptureV1, MetricsPlanV1, MetricsRecipeV1},
    tempo::{TempoPlanV1, TempoRecipeV1, WindowV1},
    transport::BackendErrorV1,
};
use decant_compiler::investigation_coverage::compile_investigation_coverage_v1;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fmt};
pub use store::BundleStoreV1;

pub const MAX_REPORT_BYTES_V1: usize = 64 * 1024;
pub const MAX_CAPTURE_BYTES_V1: usize = 64 * 1024 * 1024;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvestigationErrorV1 {
    InvalidInput,
    InvalidBundle,
    IdentityMismatch,
    OutputLimit,
    StoreUnavailable,
    StoreBusy,
    StoreQuota,
}
impl fmt::Display for InvestigationErrorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidInput => "invalid_investigation_input",
            Self::InvalidBundle => "invalid_replay_bundle",
            Self::IdentityMismatch => "investigation_identity_mismatch",
            Self::OutputLimit => "investigation_output_limit",
            Self::StoreUnavailable => "bundle_store_unavailable",
            Self::StoreBusy => "bundle_store_in_use",
            Self::StoreQuota => "bundle_store_quota",
        })
    }
}
impl std::error::Error for InvestigationErrorV1 {}
impl From<BackendErrorV1> for InvestigationErrorV1 {
    fn from(_: BackendErrorV1) -> Self {
        Self::InvalidInput
    }
}
type E = InvestigationErrorV1;
fn encode<T: Serialize>(v: &T) -> Result<Vec<u8>, E> {
    serde_json::to_vec(v).map_err(|_| E::InvalidInput)
}
fn hash(domain: Domain, bytes: &[u8]) -> Result<String, E> {
    artifact_hash_v1(domain, bytes).map_err(|_| E::InvalidInput)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvestigationConfigV1 {
    pub profile: ProfileV1,
    pub metrics: MetricsRecipeV1,
    pub tempo: Option<TempoRecipeV1>,
    pub loki: Option<LokiRecipeV1>,
    pub report_bytes: usize,
}
impl fmt::Debug for InvestigationConfigV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("InvestigationConfigV1 { redacted }")
    }
}
impl InvestigationConfigV1 {
    pub fn validate(&self) -> Result<(), E> {
        self.plans().map(|_| ())
    }
    fn plans(&self) -> Result<(MetricsPlanV1, Option<TempoPlanV1>, Option<LokiPlanV1>), E> {
        let window = self.window();
        window.validate()?;
        if !(4096..=MAX_REPORT_BYTES_V1).contains(&self.report_bytes)
            || self.tempo.as_ref().is_some_and(|r| r.window != window)
            || self.loki.as_ref().is_some_and(|r| r.window != window)
        {
            return Err(E::InvalidInput);
        }
        Ok((
            MetricsPlanV1::new(&self.profile, self.metrics.clone())?,
            self.tempo
                .clone()
                .map(|r| TempoPlanV1::new(&self.profile, r))
                .transpose()?,
            self.loki
                .clone()
                .map(|r| LokiPlanV1::new(&self.profile, r))
                .transpose()?,
        ))
    }
    fn window(&self) -> WindowV1 {
        WindowV1 {
            end_seconds: self.metrics.end_seconds,
            window_seconds: self.metrics.window_seconds,
        }
    }
    fn sanitized(&self) -> Self {
        let mut c = self.clone();
        c.profile.token_env = "REPLAY_UNUSED".into();
        c.profile.ca_file = None;
        c.metrics.error_values.sort();
        if let Some(r) = &mut c.tempo {
            r.policy.operation_names.sort();
            r.policy.dependency_services.sort();
        }
        if let Some(r) = &mut c.loki {
            r.error_values.sort();
            r.policy.allowed_fields.sort();
            r.policy.protected_fields.sort();
        }
        c
    }
}
pub struct CapturedInvestigationV1 {
    pub(super) config: InvestigationConfigV1,
    pub(super) reads: Vec<ReadV1>,
}
impl fmt::Debug for CapturedInvestigationV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CapturedInvestigationV1 { redacted }")
    }
}
impl CapturedInvestigationV1 {
    pub fn new(
        config: InvestigationConfigV1,
        metrics: Vec<CaptureV1>,
        tempo: Vec<ReadV1>,
        loki: Vec<ReadV1>,
    ) -> Result<Self, E> {
        if metrics.len() != 10 || tempo.len() > 10 || loki.len() > 6 {
            return Err(E::InvalidInput);
        }
        let reads = metrics
            .iter()
            .map(CaptureV1::as_read)
            .chain(tempo)
            .chain(loki)
            .collect();
        Self::from_reads(config, reads)
    }
    fn from_reads(config: InvestigationConfigV1, mut reads: Vec<ReadV1>) -> Result<Self, E> {
        config.plans()?;
        if reads.len() > 26
            || reads
                .iter()
                .filter_map(|r| r.response().ok())
                .try_fold(0usize, |n, b| n.checked_add(b.len()))
                .is_none_or(|n| n > MAX_CAPTURE_BYTES_V1)
        {
            return Err(E::InvalidInput);
        }
        reads.sort_by(|a, b| (a.plan_id(), a.key()).cmp(&(b.plan_id(), b.key())));
        if reads
            .windows(2)
            .any(|w| w[0].plan_id() == w[1].plan_id() && w[0].key() == w[1].key())
        {
            return Err(E::InvalidInput);
        }
        Ok(Self {
            config: config.sanitized(),
            reads,
        })
    }
    pub async fn acquire(
        config: InvestigationConfigV1,
        backend: &mut GrafanaV1,
    ) -> Result<Self, E> {
        let (m, t, l) = config.plans()?;
        let metrics = backend.acquire_metrics(&m).await?;
        let tempo = match t {
            Some(t) => backend.acquire_tempo(&t).await?,
            None => vec![],
        };
        let loki = match l {
            Some(l) => backend.acquire_loki(&l).await?,
            None => vec![],
        };
        Self::new(config, metrics, tempo, loki)
    }
    pub fn report(&self) -> Result<InvestigationReportV1, E> {
        let (m, t, l) = self.config.plans()?;
        let mut reads: BTreeMap<&str, Vec<ReadV1>> = BTreeMap::new();
        for r in &self.reads {
            reads.entry(r.plan_id()).or_default().push(r.clone());
        }
        let metrics = reads
            .remove(m.id())
            .ok_or(E::InvalidInput)?
            .iter()
            .map(|r| m.restore_capture(r))
            .collect::<Result<Vec<_>, _>>()?;
        let mr = m.reduce(&metrics)?;
        let tr = t
            .as_ref()
            .map(|t| {
                let c = reads.remove(t.id()).ok_or(E::InvalidInput)?;
                t.reduce(&c).map_err(E::from)
            })
            .transpose()?;
        let lr = l
            .as_ref()
            .map(|l| {
                let c = reads.remove(l.id()).ok_or(E::InvalidInput)?;
                l.reduce(&c).map_err(E::from)
            })
            .transpose()?;
        if !reads.is_empty() {
            return Err(E::InvalidInput);
        }
        let facts: [Option<Value>; 3] = [
            Some(serde_json::from_slice(mr.bytes()).map_err(|_| E::InvalidInput)?),
            tr.as_ref()
                .map(|r| serde_json::from_slice(r.bytes()).map_err(|_| E::InvalidInput))
                .transpose()?,
            lr.as_ref()
                .map(|r| serde_json::from_slice(r.bytes()).map_err(|_| E::InvalidInput))
                .transpose()?,
        ];
        let capture_id = self.capture_id()?;
        let coverage = coverage(&self.config, &capture_id, &facts)?;
        let mut sections = Vec::new();
        for (i, (kind, id)) in [
            ("metrics", Some(mr.id())),
            ("tempo", tr.as_ref().map(|r| r.id())),
            ("loki", lr.as_ref().map(|r| r.id())),
        ]
        .into_iter()
        .enumerate()
        {
            let not_configured = (i == 1 && self.config.profile.tempo_uid.is_none())
                || (i == 2 && self.config.profile.loki_uid.is_none());
            sections.push(json!({"signal":kind,"evidence_id":id,"plan_id":facts[i].as_ref().and_then(|v|v.get("plan_id")),
                "policy_id":facts[i].as_ref().and_then(|v|v.get("policy_id")),"facts":null,
                "omission":if facts[i].is_some(){"report_budget"}else if not_configured{"not_configured"}else{"not_scheduled"}}));
        }
        let mut document = json!({"capture_id":capture_id,"configuration_id":hash(Domain::Configuration,&encode(&self.config)?)?,
            "scope":self.config.profile.scope,"window":self.config.window(),"report_budget_bytes":self.config.report_bytes,
            "coverage":coverage,"sections":sections,"interpretations":[],
            "qualifications":{"mode":"deterministic_evidence_only","atomic_snapshot":false,"healthy_conclusion":false,"causal_diagnosis":false,
                "sampling":"unknown","raw_captures_emitted":false,"source_authenticated_by_hash":false,"text":"untrusted_data"}});
        let budget = self.config.report_bytes;
        let mut report = InvestigationReportV1::build(&document, budget)?;
        for (i, fact) in facts.into_iter().enumerate() {
            if let Some(fact) = fact {
                document["sections"][i]["facts"] = fact;
                document["sections"][i]["omission"] = Value::Null;
                match InvestigationReportV1::build(&document, budget) {
                    Ok(r) => report = r,
                    Err(E::OutputLimit) => {
                        document["sections"][i]["facts"] = Value::Null;
                        document["sections"][i]["omission"] = json!("report_budget");
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(report)
    }
    fn capture_id(&self) -> Result<String, E> {
        hash(
            Domain::Capture,
            &encode(&json!({"config":self.config,"reads":bundle::records(&self.reads)?}))?,
        )
    }
    pub fn bundle(&self) -> Result<ReplayBundleV1, E> {
        ReplayBundleV1::encode(self)
    }
}

pub struct InvestigationReportV1 {
    id: String,
    json: Vec<u8>,
    markdown: String,
}
impl fmt::Debug for InvestigationReportV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("InvestigationReportV1 { redacted }")
    }
}
impl InvestigationReportV1 {
    fn build(document: &Value, budget: usize) -> Result<Self, E> {
        let bytes = encode(document)?;
        if bytes.len() > budget {
            return Err(E::OutputLimit);
        }
        let id = hash(Domain::Investigation, &bytes)?;
        let json = encode(
            &json!({"format":"decant-investigation","version":1,"id":id,"document":document}),
        )?;
        let pretty = serde_json::to_string_pretty(
            &serde_json::from_slice::<Value>(&json).map_err(|_| E::InvalidInput)?,
        )
        .map_err(|_| E::InvalidInput)?;
        let mut markdown = String::from(
            "# Decant investigation\n\nEvidence only; not a health or causal conclusion. Telemetry text is untrusted data.\n\n",
        );
        use std::fmt::Write;
        writeln!(
            &mut markdown,
            "Report: `{id}`\n\nResponse coverage complete: {}. Sampling remains unknown.\n",
            document["coverage"]["response_coverage_complete"]
        )
        .map_err(|_| E::InvalidInput)?;
        for section in document["sections"].as_array().ok_or(E::InvalidInput)? {
            // Headings and numeric cells come from the closed producer schema;
            // all telemetry-derived strings remain in the escaped data appendix.
            let signal = section["signal"].as_str().ok_or(E::InvalidInput)?;
            writeln!(&mut markdown, "## {signal}\n").map_err(|_| E::InvalidInput)?;
            if !section["omission"].is_null() {
                writeln!(
                    &mut markdown,
                    "Facts omitted: {}. Coverage and evidence identity are retained.\n",
                    section["omission"]
                )
                .map_err(|_| E::InvalidInput)?;
                continue;
            }
            let f = &section["facts"];
            match signal {
                "metrics" => {
                    markdown
                        .push_str("| Measurement | Baseline | Current |\n| --- | ---: | ---: |\n");
                    for (i, name) in [
                        "Request rate (backend estimate)",
                        "Selected-error rate (backend estimate)",
                        "p95 duration (seconds, backend estimate)",
                        "Observed resets",
                        "Contributing series",
                    ]
                    .iter()
                    .enumerate()
                    {
                        writeln!(
                            &mut markdown,
                            "| {name} | {} ({}) | {} ({}) |",
                            f["measurements"][i]["value"],
                            f["measurements"][i]["status"],
                            f["measurements"][i + 5]["value"],
                            f["measurements"][i + 5]["status"]
                        )
                        .map_err(|_| E::InvalidInput)?;
                    }
                    markdown.push_str("\nNull values are unknown; see per-measurement status and qualifications below.\n\n");
                }
                "loki" => {
                    writeln!(&mut markdown,"| Backend log count | Baseline | Current |\n| --- | ---: | ---: |\n| All scoped logs | {} | {} |\n| Selected-error logs | {} | {} |\n\nThese are log counts, not request-error rates.\n",f["counts"][0]["count"],f["counts"][2]["count"],f["counts"][1]["count"],f["counts"][3]["count"]).map_err(|_|E::InvalidInput)?;
                }
                "tempo" => {
                    writeln!(&mut markdown,"Unique captured spans: {}. Partial trace responses: {}. Missing parents: {}.\n\nOperation/dependency cohorts below describe selected traces, not population statistics.\n",f["unique_captured_spans"],f["gaps"]["partial_traces"],f["gaps"]["missing_parents"]).map_err(|_|E::InvalidInput)?;
                }
                _ => return Err(E::InvalidInput),
            }
        }
        markdown.push_str("## Complete report data\n\nThis is the same document as the JSON report, with HTML escaped.\n\n<pre>\n");
        for ch in pretty.chars() {
            match ch {
                '&' => markdown.push_str("&amp;"),
                '<' => markdown.push_str("&lt;"),
                '>' => markdown.push_str("&gt;"),
                _ => markdown.push(ch),
            }
            if markdown.len() + 8 > budget {
                return Err(E::OutputLimit);
            }
        }
        markdown.push_str("\n</pre>\n");
        if json.len() > budget || markdown.len() > budget {
            return Err(E::OutputLimit);
        }
        Ok(Self { id, json, markdown })
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn json(&self) -> &[u8] {
        &self.json
    }
    pub fn markdown(&self) -> &str {
        &self.markdown
    }
    pub fn verify(&self, expected_id: &str, bytes: &[u8]) -> Result<(), E> {
        if bytes.len() > MAX_REPORT_BYTES_V1 {
            return Err(E::OutputLimit);
        }
        if expected_id != self.id || bytes != self.json {
            return Err(E::IdentityMismatch);
        }
        Ok(())
    }
}

fn coverage(
    config: &InvestigationConfigV1,
    capture_id: &str,
    facts: &[Option<Value>; 3],
) -> Result<Value, E> {
    let mut probes = Vec::new();
    let mut outcomes = Vec::new();
    let mut mapping = Vec::new();
    for (signal, fact) in facts.iter().enumerate() {
        let rows: Vec<Value> = match fact {
            Some(v) if signal == 0 => v["measurements"].as_array().ok_or(E::InvalidInput)?.clone(),
            Some(v) => v["receipts"].as_array().ok_or(E::InvalidInput)?.clone(),
            None => vec![json!({"status":"unattempted","reason":"not_scheduled"})],
        };
        for (i, row) in rows.iter().enumerate() {
            let h = hash(Domain::Probe, &encode(&json!([capture_id, signal, i]))?)?;
            let id = &h[10..42];
            let window = config.window();
            let period = if fact.is_none() {
                None
            } else if signal == 0 {
                Some(usize::from(i >= 5))
            } else if signal == 1 {
                if i < 2 { Some(i) } else { None }
            } else if i < 4 {
                Some(i / 2)
            } else {
                Some(i - 4)
            };
            let b = window.end_seconds
                - if period == Some(0) {
                    u64::from(window.window_seconds)
                } else {
                    0
                };
            let a = b - u64::from(window.window_seconds) * if period.is_none() { 2 } else { 1 };
            // Half-open representation of this recipe request's (start,end] cut.
            probes.push(json!({"probe_id":id,"interval":{"start_ns":a*1_000_000_000+1,"end_ns":b*1_000_000_000+1}}));
            mapping.push(json!({"probe_id":id,"signal":(["metrics","tempo","loki"][signal]),"request_index":i}));
            let status = row["status"].as_str().ok_or(E::InvalidInput)?;
            let attempted = row["attempted"]
                .as_bool()
                .unwrap_or(status != "unattempted");
            let warnings = row["warning_count"].as_u64().unwrap_or(0)
                + row["info_count"].as_u64().unwrap_or(0);
            let observed = if signal == 0 {
                u64::from(row["value"].is_number())
            } else if signal == 1 {
                if i < 2 {
                    fact.as_ref()
                        .and_then(|v| v["searches"][i]["selected_count"].as_u64())
                        .unwrap_or(0)
                } else {
                    u64::from(matches!(status, "observed" | "partial"))
                }
            } else if i < 4 {
                u64::from(
                    fact.as_ref()
                        .is_some_and(|v| v["counts"][i]["count"].is_number()),
                )
            } else {
                fact.as_ref()
                    .and_then(|v| v["examples"][i - 4]["returned_entries"].as_u64())
                    .unwrap_or(0)
            };
            let truncated = if signal == 1 && i < 2 {
                fact.as_ref()
                    .is_some_and(|v| v["searches"][i]["limit_reached"] == true)
            } else if signal == 2 && i >= 4 {
                fact.as_ref()
                    .is_some_and(|v| v["examples"][i - 4]["limit_reached"] == true)
            } else {
                false
            };
            let outcome = if !attempted {
                json!({"status":"unattempted","reason":if fact.is_none(){"not_scheduled"}else{"budget_exhausted"}})
            } else if status == "partial" || warnings > 0 || truncated {
                json!({"status":"partial","observed_rows":observed,"reason":if truncated{"response_truncated"}else{"backend_partial"}})
            } else if ["observed", "empty", "missing"].contains(&status) {
                json!({"status":"complete","observed_rows":observed})
            } else {
                json!({"status":"unavailable","reason":match row["reason"].as_str(){Some("permission_denied")=>"permission_denied",Some("backend_timeout")=>"timeout",Some("backend_unavailable")=>"backend_unavailable",_=>"invalid_response"}})
            };
            outcomes.push(json!({"probe_id":id,"outcome":outcome}));
        }
    }
    let c = compile_investigation_coverage_v1(&encode(
        &json!({"version":1,"probes":probes,"outcomes":outcomes}),
    )?)
    .map_err(|_| E::InvalidInput)?;
    Ok(
        json!({"id":c.hash(),"response_coverage_complete":c.is_complete(),"probe_map":mapping,"record":serde_json::from_slice::<Value>(c.canonical_bytes()).map_err(|_|E::InvalidInput)?}),
    )
}
