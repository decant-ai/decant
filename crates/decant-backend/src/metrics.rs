//! Reset-aware, backend-qualified scalar comparisons. No diagnosis or model.
use crate::{
    bounded_json,
    grafana::{GrafanaV1, ProfileV1, label, quoted},
    transport::{BackendErrorV1 as E, MAX_RESPONSE_BYTES},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, fmt};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsRecipeV1 {
    pub request_counter: String,
    pub error_label: String,
    pub error_values: Vec<String>,
    pub duration_bucket: String,
    /// The operator declares seconds; metric names alone do not prove units.
    pub duration_unit: String,
    pub window_seconds: u32,
    pub end_seconds: u64,
}
impl fmt::Debug for MetricsRecipeV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MetricsRecipeV1 { redacted }")
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricV1 {
    RequestRate,
    ErrorRate,
    DurationP95,
    CounterResets,
    ContributingSeries,
}
impl MetricV1 {
    fn unit(self) -> &'static str {
        match self {
            Self::RequestRate | Self::ErrorRate => "requests_per_second",
            Self::DurationP95 => "seconds",
            Self::CounterResets => "observed_resets",
            Self::ContributingSeries => "observed_series",
        }
    }
}
#[derive(Clone, Serialize)]
pub struct QueryV1 {
    pub period: &'static str,
    pub metric: MetricV1,
    pub evaluation_seconds: u64,
    pub promql: String,
}
impl fmt::Debug for QueryV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryV1")
            .field("period", &self.period)
            .field("metric", &self.metric)
            .finish_non_exhaustive()
    }
}

pub struct MetricsPlanV1 {
    source_id: String,
    plan_id: String,
    uid: String,
    recipe: MetricsRecipeV1,
    queries: Vec<QueryV1>,
}
impl fmt::Debug for MetricsPlanV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("MetricsPlanV1 { redacted }")
    }
}
fn metric_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value.bytes().enumerate().all(|(i, b)| {
            b.is_ascii_alphabetic() || b == b'_' || b == b':' || (i > 0 && b.is_ascii_digit())
        })
}
fn hash(domain: &[u8], bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(domain);
    digest.update([0]);
    digest.update(bytes);
    let mut text = String::from("sha256:v1:");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in digest.finalize() {
        text.push(HEX[usize::from(byte >> 4)] as char);
        text.push(HEX[usize::from(byte & 15)] as char);
    }
    text
}
fn source_id(profile: &ProfileV1) -> Result<String, E> {
    profile.validate()?;
    let value = json!({"origin":profile.url,"uid":profile.prometheus_uid,"scope":profile.scope});
    Ok(hash(
        b"decant-metrics-source:v1",
        &serde_json::to_vec(&value).map_err(|_| E::InvalidConfiguration)?,
    ))
}
impl MetricsPlanV1 {
    pub fn new(profile: &ProfileV1, mut recipe: MetricsRecipeV1) -> Result<Self, E> {
        let source_id = source_id(profile)?;
        if !metric_name(&recipe.request_counter)
            || !metric_name(&recipe.duration_bucket)
            || !recipe.duration_bucket.ends_with("_bucket")
            || recipe.duration_unit != "seconds"
            || !label(&recipe.error_label)
            || recipe.error_label == profile.scope.service_label
            || recipe.error_label == profile.scope.environment_label
            || recipe.error_label == "__name__"
            || !(60..=3600).contains(&recipe.window_seconds)
            || recipe.end_seconds < u64::from(recipe.window_seconds) * 2
            || recipe.end_seconds > 253402300799
            || recipe.error_values.is_empty()
            || recipe.error_values.len() > 16
            || recipe.error_values.iter().any(|s| {
                s.is_empty()
                    || s.len() > 32
                    || !s
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
            })
        {
            return Err(E::InvalidConfiguration);
        }
        recipe.error_values.sort();
        if recipe.error_values.iter().collect::<BTreeSet<_>>().len() != recipe.error_values.len() {
            return Err(E::InvalidConfiguration);
        }
        let scope = profile.scope.selector();
        let window = recipe.window_seconds;
        let counter = format!("{}{{{scope}}}[{window}s]", recipe.request_counter);
        let errors = format!(
            "{}{{{scope},{}=~{}}}[{window}s]",
            recipe.request_counter,
            recipe.error_label,
            quoted(&recipe.error_values.join("|"))
        );
        let buckets = format!("{}{{{scope}}}[{window}s]", recipe.duration_bucket);
        let templates = [
            (MetricV1::RequestRate, format!("sum(rate({counter}))")),
            (MetricV1::ErrorRate, format!("sum(rate({errors}))")),
            (
                MetricV1::DurationP95,
                format!("histogram_quantile(0.95, sum by (le) (rate({buckets})))"),
            ),
            (MetricV1::CounterResets, format!("sum(resets({counter}))")),
            (
                MetricV1::ContributingSeries,
                format!("count(rate({counter}))"),
            ),
        ];
        let mut queries = Vec::with_capacity(10);
        for (period, end) in [
            ("baseline", recipe.end_seconds - u64::from(window)),
            ("current", recipe.end_seconds),
        ] {
            for (metric, promql) in &templates {
                queries.push(QueryV1 {
                    period,
                    metric: *metric,
                    evaluation_seconds: end,
                    promql: promql.clone(),
                });
            }
        }
        let plan_id = hash(
            b"decant-metrics-plan:v1",
            &serde_json::to_vec(&json!({
            "source_id":source_id,"recipe":recipe,"queries":queries}))
            .map_err(|_| E::InvalidConfiguration)?,
        );
        Ok(Self {
            source_id,
            plan_id,
            uid: profile.prometheus_uid.clone(),
            recipe,
            queries,
        })
    }
    pub fn queries(&self) -> &[QueryV1] {
        &self.queries
    }
    pub fn id(&self) -> &str {
        &self.plan_id
    }
    /// Bind an acquired response (or closed failure) to this exact request plan.
    /// The caller is trusted to supply the real response; hashes do not attest it.
    pub fn capture(&self, index: usize, response: Result<Vec<u8>, E>) -> Result<CaptureV1, E> {
        if index >= 10
            || response
                .as_ref()
                .is_ok_and(|b| b.len() > MAX_RESPONSE_BYTES)
        {
            return Err(E::InvalidResponse);
        }
        Ok(CaptureV1 {
            plan_id: self.plan_id.clone(),
            index,
            response,
            attempted: true,
        })
    }
    pub fn reduce(&self, captures: &[CaptureV1]) -> Result<MetricsComparisonV1, E> {
        if captures.len() != 10 {
            return Err(E::InvalidResponse);
        }
        let mut slots = [None; 10];
        for capture in captures {
            if capture.plan_id != self.plan_id
                || capture.index >= 10
                || slots[capture.index].is_some()
            {
                return Err(E::InvalidResponse);
            }
            slots[capture.index] = Some(capture);
        }
        let mut measurements = Vec::with_capacity(10);
        for (i, query) in self.queries.iter().enumerate() {
            measurements.push(observe(
                query,
                &slots[i].ok_or(E::InvalidResponse)?.response,
                slots[i].ok_or(E::InvalidResponse)?.attempted,
            ));
        }
        let baseline = fraction(&measurements[0], &measurements[1]);
        let current = fraction(&measurements[5], &measurements[6]);
        let delta = |index: usize| match (
            measurements[index].complete(),
            measurements[index + 5].complete(),
        ) {
            (Some(a), Some(b)) => Some(b - a),
            _ => None,
        };
        let mut consistency_gaps = Vec::new();
        for (start, period) in [(0, "baseline"), (5, "current")] {
            if let (Some(rate), Some(series)) = (
                measurements[start].complete(),
                measurements[start + 4].complete(),
            ) && rate > 0.0
                && series == 0.0
            {
                consistency_gaps.push(format!("{period}_positive_rate_without_series"));
            }
        }
        let complete = consistency_gaps.is_empty()
            && measurements.iter().all(|m| m.status == "observed")
            && baseline.value.is_some()
            && current.value.is_some();
        let series_changed = match (measurements[4].complete(), measurements[9].complete()) {
            (Some(a), Some(b)) => Some(a != b),
            _ => None,
        };
        let value = json!({
            "format":"decant-metrics-comparison","version":1,
            "plan_id":self.plan_id,"source_id":self.source_id,"datasource_uid":self.uid,
            "recipe":self.recipe,"queries":self.queries,
            "interval_semantics":"left_open_right_closed",
            "baseline":{"start_seconds":self.recipe.end_seconds-u64::from(self.recipe.window_seconds)*2,
                "end_seconds":self.recipe.end_seconds-u64::from(self.recipe.window_seconds)},
            "current":{"start_seconds":self.recipe.end_seconds-u64::from(self.recipe.window_seconds),
                "end_seconds":self.recipe.end_seconds},
            "measurements":measurements,
            "error_fraction":{"baseline":baseline,"current":current,
                "delta": match (baseline.value,current.value) {(Some(a),Some(b))=>Some(b-a),_=>None}},
            "delta":{"request_rate":delta(0),"error_rate":delta(1),"duration_p95_seconds":delta(2)},
            "contributing_series_changed":series_changed,"complete_measurements":complete,
            "consistency_gaps":consistency_gaps,
            "qualifications":{"measurement_kind":"backend_reported_estimate","population_coverage":"unknown",
                "sampling":"unknown","atomic_snapshot":false,"source_authenticated_by_hash":false,
                "causal_diagnosis":false,"raw_responses_emitted":false}
        });
        let bytes = serde_json::to_vec(&value).map_err(|_| E::InvalidResponse)?;
        if bytes.len() > 64 * 1024 {
            return Err(E::ResponseTooLarge);
        }
        let id = hash(b"decant-metrics-comparison:v1", &bytes);
        Ok(MetricsComparisonV1 { bytes, id })
    }
}
#[derive(Clone)]
pub struct CaptureV1 {
    plan_id: String,
    index: usize,
    response: Result<Vec<u8>, E>,
    attempted: bool,
}
impl fmt::Debug for CaptureV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CaptureV1 { redacted }")
    }
}
impl CaptureV1 {
    pub fn as_read(&self) -> crate::ReadV1 {
        crate::ReadV1 {
            plan: self.plan_id.clone(),
            key: format!("metrics-{}", self.index),
            response: self.response.clone(),
            attempted: self.attempted,
        }
    }
}
impl MetricsPlanV1 {
    pub fn restore_capture(&self, read: &crate::ReadV1) -> Result<CaptureV1, E> {
        let index = read
            .key
            .strip_prefix("metrics-")
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or(E::InvalidResponse)?;
        if read.plan != self.plan_id
            || read.key != format!("metrics-{index}")
            || (!read.attempted && read.response != Err(E::BudgetExhausted))
        {
            return Err(E::InvalidResponse);
        }
        let mut c = self.capture(index, read.response.clone())?;
        c.attempted = read.attempted;
        Ok(c)
    }
}
pub struct MetricsComparisonV1 {
    bytes: Vec<u8>,
    id: String,
}
impl MetricsComparisonV1 {
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn id(&self) -> &str {
        &self.id
    }
}
impl fmt::Debug for MetricsComparisonV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsComparisonV1")
            .field("bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}
impl GrafanaV1 {
    /// Captures remain private in-memory inputs for model-independent reduction.
    pub async fn acquire_metrics(&mut self, plan: &MetricsPlanV1) -> Result<Vec<CaptureV1>, E> {
        if source_id(&self.profile)? != plan.source_id {
            return Err(E::InvalidConfiguration);
        }
        let mut captures = Vec::with_capacity(10);
        for (i, query) in plan.queries.iter().enumerate() {
            let time = query.evaluation_seconds.to_string();
            let before = self.http.usage().attempts;
            let response = self
                .http
                .get(
                    &[
                        "api",
                        "datasources",
                        "proxy",
                        "uid",
                        &plan.uid,
                        "api",
                        "v1",
                        "query",
                    ],
                    &[
                        ("query", &query.promql),
                        ("time", &time),
                        ("timeout", "10s"),
                        ("limit", "1"),
                    ],
                )
                .await
                .map(|r| r.body().to_vec());
            let mut capture = plan.capture(i, response)?;
            capture.attempted = self.http.usage().attempts != before;
            captures.push(capture);
        }
        Ok(captures)
    }
}
#[derive(Serialize)]
struct Measurement {
    attempted: bool,
    metric: MetricV1,
    period: &'static str,
    unit: &'static str,
    status: &'static str,
    value: Option<f64>,
    reason: Option<String>,
    response_hash: Option<String>,
    warning_count: usize,
    info_count: usize,
}
impl Measurement {
    fn complete(&self) -> Option<f64> {
        if self.status == "observed" {
            self.value
        } else {
            None
        }
    }
}
fn observe(query: &QueryV1, response: &Result<Vec<u8>, E>, attempted: bool) -> Measurement {
    let mut m = Measurement {
        attempted,
        metric: query.metric,
        period: query.period,
        unit: query.metric.unit(),
        status: if attempted {
            "unavailable"
        } else {
            "unattempted"
        },
        value: None,
        reason: None,
        response_hash: None,
        warning_count: 0,
        info_count: 0,
    };
    let bytes = match response {
        Ok(bytes) => bytes,
        Err(e) => {
            m.reason = Some(e.to_string());
            return m;
        }
    };
    m.response_hash = Some(hash(b"decant-backend-response:v1", bytes));
    match parse_scalar(bytes, query) {
        Ok((state, value, warnings, infos)) => {
            m.status = state;
            m.value = value;
            m.warning_count = warnings;
            m.info_count = infos;
        }
        Err(reason) => m.reason = Some(reason.into()),
    }
    m
}
fn parse_scalar(
    bytes: &[u8],
    query: &QueryV1,
) -> Result<(&'static str, Option<f64>, usize, usize), &'static str> {
    let value = bounded_json::parse(bytes, MAX_RESPONSE_BYTES).map_err(|_| "invalid_response")?;
    if value.get("status").and_then(Value::as_str) == Some("error") {
        return Err("backend_error_response");
    }
    if value.get("status").and_then(Value::as_str) != Some("success")
        || value.pointer("/data/resultType").and_then(Value::as_str) != Some("vector")
    {
        return Err("invalid_response");
    }
    let annotations = |key: &str| -> Result<usize, &'static str> {
        match value.get(key) {
            None => Ok(0),
            Some(Value::Array(v)) if v.iter().all(Value::is_string) => Ok(v.len()),
            _ => Err("invalid_response"),
        }
    };
    let warnings = annotations("warnings")?;
    let infos = annotations("infos")?;
    let rows = value
        .pointer("/data/result")
        .and_then(Value::as_array)
        .ok_or("invalid_response")?;
    if rows.is_empty() {
        return Ok(("missing", None, warnings, infos));
    }
    if rows.len() != 1
        || rows[0].get("histogram").is_some()
        || !rows[0]
            .get("metric")
            .and_then(Value::as_object)
            .is_some_and(|v| v.is_empty())
    {
        return Err("invalid_response");
    }
    let pair = rows[0]
        .get("value")
        .and_then(Value::as_array)
        .filter(|v| v.len() == 2)
        .ok_or("invalid_response")?;
    if pair[0].as_f64() != Some(query.evaluation_seconds as f64) {
        return Err("timestamp_mismatch");
    }
    let text = pair[1]
        .as_str()
        .filter(|v| v.len() <= 64)
        .ok_or("invalid_response")?;
    let number = text.parse::<f64>().map_err(|_| "invalid_response")?;
    if !number.is_finite() {
        return Ok(("non_finite", None, warnings, infos));
    }
    if number < 0.0 {
        return Err("negative_value");
    }
    if crate::grafana::underflowed(text, number) {
        return Err("numeric_underflow");
    }
    if matches!(
        query.metric,
        MetricV1::CounterResets | MetricV1::ContributingSeries
    ) && (number.fract() != 0.0 || number >= 9_007_199_254_740_992.0)
    {
        return Err("invalid_count");
    }
    Ok((
        if warnings + infos > 0 {
            "partial"
        } else {
            "observed"
        },
        Some(if number == 0.0 { 0.0 } else { number }),
        warnings,
        infos,
    ))
}
#[derive(Serialize)]
struct Fraction {
    value: Option<f64>,
    reason: Option<&'static str>,
}
fn fraction(total: &Measurement, errors: &Measurement) -> Fraction {
    match (total.complete(), errors.complete()) {
        (Some(t), Some(e)) if e > t => Fraction {
            value: None,
            reason: Some("error_rate_exceeds_total"),
        },
        (Some(0.0), Some(_)) => Fraction {
            value: None,
            reason: Some("zero_request_rate"),
        },
        (Some(t), Some(e)) if e > 0.0 && e / t == 0.0 => Fraction {
            value: None,
            reason: Some("arithmetic_underflow"),
        },
        (Some(t), Some(e)) => Fraction {
            value: Some(e / t),
            reason: None,
        },
        _ => Fraction {
            value: None,
            reason: Some("incomplete_inputs"),
        },
    }
}
