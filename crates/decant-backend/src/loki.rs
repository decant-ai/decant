//! Scoped log comparisons with no body acquisition unless projection is enabled.
pub use crate::evidence::{EvidenceV1, ReadV1, WindowV1};
use crate::{
    bounded_json,
    evidence::{allowlist, identity, source, uint},
    grafana::{GrafanaV1, ProfileV1, annotations, label, quoted, scalar_vector},
    transport::{BackendErrorV1 as E, MAX_RESPONSE_BYTES},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::BTreeMap, fmt};

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LogPolicyV1 {
    pub allowed_fields: Vec<String>,
    pub protected_fields: Vec<String>,
    pub max_examples: u8,
}
impl fmt::Debug for LogPolicyV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LogPolicyV1 { redacted }")
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LokiRecipeV1 {
    pub window: WindowV1,
    pub error_label: String,
    pub error_values: Vec<String>,
    pub policy: LogPolicyV1,
}
impl fmt::Debug for LokiRecipeV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LokiRecipeV1 { redacted }")
    }
}
pub struct LokiPlanV1 {
    pub(crate) profile: ProfileV1,
    pub(crate) recipe: LokiRecipeV1,
    source: String,
    id: String,
    uid: String,
    queries: Vec<String>,
}
impl fmt::Debug for LokiPlanV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("LokiPlanV1 { redacted }")
    }
}
impl LokiPlanV1 {
    pub fn new(profile: &ProfileV1, mut recipe: LokiRecipeV1) -> Result<Self, E> {
        recipe.window.validate()?;
        recipe.error_values.sort();
        recipe.policy.allowed_fields.sort();
        recipe.policy.protected_fields.sort();
        allowlist(&recipe.error_values)?;
        allowlist(&recipe.policy.allowed_fields)?;
        allowlist(&recipe.policy.protected_fields)?;
        if !label(&recipe.error_label)
            || recipe.error_label.starts_with("__")
            || recipe.error_label == profile.scope.service_label
            || recipe.error_label == profile.scope.environment_label
            || recipe.error_values.is_empty()
            || recipe.error_values.iter().any(|s| {
                s.len() > 32
                    || !s
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
            })
            || recipe.policy.max_examples > 8
            || recipe.policy.allowed_fields.len() > 8
            || (recipe.policy.max_examples == 0) != recipe.policy.allowed_fields.is_empty()
            || recipe
                .policy
                .allowed_fields
                .iter()
                .chain(recipe.policy.protected_fields.iter())
                .any(|s| !label(s))
            || recipe
                .policy
                .allowed_fields
                .iter()
                .any(|s| recipe.policy.protected_fields.contains(s))
        {
            return Err(E::InvalidConfiguration);
        }
        let uid = profile.loki_uid.clone().ok_or(E::InvalidConfiguration)?;
        let source = source(profile, &uid)?;
        let selector = format!("{{{}}}", profile.scope.selector());
        let errors = format!(
            "{{{},{}=~{}}}",
            profile.scope.selector(),
            recipe.error_label,
            quoted(&recipe.error_values.join("|"))
        );
        let total = format!(
            "sum(count_over_time({selector}[{}s]))",
            recipe.window.window_seconds
        );
        let error = format!(
            "sum(count_over_time({errors}[{}s]))",
            recipe.window.window_seconds
        );
        let mut queries = vec![total.clone(), error.clone(), total, error];
        if recipe.policy.max_examples > 0 {
            queries.extend([errors.clone(), errors]);
        }
        let id = identity(
            "decant-loki-plan:v1",
            &json!({"source":source,"recipe":recipe,"queries":queries}),
        );
        Ok(Self {
            profile: profile.clone(),
            recipe,
            source,
            id,
            uid,
            queries,
        })
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn queries(&self) -> &[String] {
        &self.queries
    }
    pub fn capture(&self, index: usize, response: Result<Vec<u8>, E>) -> Result<ReadV1, E> {
        if index >= self.queries.len() {
            return Err(E::InvalidRequest);
        }
        ReadV1::new(&self.id, format!("loki-{index}"), response)
    }
    pub fn reduce(&self, captures: &[ReadV1]) -> Result<EvidenceV1, E> {
        if captures.len() != self.queries.len() {
            return Err(E::InvalidResponse);
        }
        let mut slots = BTreeMap::new();
        for c in captures {
            if c.plan != self.id || slots.insert(c.key.as_str(), c).is_some() {
                return Err(E::InvalidResponse);
            }
        }
        let mut receipts = Vec::new();
        let mut counts = Vec::new();
        let mut examples = Vec::new();
        for (i, q) in self.queries.iter().enumerate() {
            let key = format!("loki-{i}");
            let c = slots.remove(key.as_str()).ok_or(E::InvalidResponse)?;
            let period = if i < 4 { i / 2 } else { i - 4 };
            let (start, end) = self.recipe.window.bounds(period);
            let parsed = c
                .response
                .as_ref()
                .ok()
                .and_then(|b| bounded_json::parse(b, MAX_RESPONSE_BYTES).ok());
            let partial = parsed.as_ref().map(annotations).transpose().ok().flatten();
            let result = parsed.as_ref().and_then(|v| {
                if i < 4 {
                    count(v, end).ok().map(
                        |n| json!({"count":n,"status":if n.is_none(){"missing"}else{"observed"}}),
                    )
                } else {
                    self.examples(v, period).ok()
                }
            });
            let status = if !c.attempted {
                "unattempted"
            } else if result.is_none() || partial.is_none() {
                "unavailable"
            } else if partial == Some(true)
                || result
                    .as_ref()
                    .is_some_and(|v| v["unparseable_lines"].as_u64().is_some_and(|n| n > 0))
            {
                "partial"
            } else if i < 4 {
                if result.as_ref().is_some_and(|v| v["count"].is_null()) {
                    "missing"
                } else {
                    "observed"
                }
            } else if result.as_ref().is_some_and(|v| v["returned_entries"] == 0) {
                "empty"
            } else {
                "observed"
            };
            let mut receipt = c.receipt(status);
            receipt["query"] = json!(q);
            receipt["request"] = if i < 4 {
                json!({"route":"loki/api/v1/query","evaluation_seconds":end,"time_ns":end*1_000_000_000})
            } else {
                json!({"route":"loki/api/v1/query_range","start_ns":start*1_000_000_000+1,"end_exclusive_ns":end*1_000_000_000+1,"direction":"forward","limit":self.recipe.policy.max_examples})
            };
            if status == "unavailable" && c.response.is_ok() {
                receipt["reason"] = json!("invalid_response");
            }
            receipts.push(receipt);
            if i < 4 {
                counts.push(json!({"period":period,"kind":if i%2==0{"all_logs"}else{"selected_error_logs"},"status":status,
                "count":result.as_ref().and_then(|v|v["count"].as_u64())}));
            } else {
                let mut example = if status == "unavailable" || status == "unattempted" {
                    json!({"items":[]})
                } else {
                    result.unwrap_or_else(|| json!({"items":[]}))
                };
                example["period"] = json!(period);
                example["status"] = json!(status);
                examples.push(example);
            }
        }
        let valid = |i: usize| -> Option<u64> {
            if counts[i]["status"] == "observed" {
                counts[i]["count"].as_u64()
            } else {
                None
            }
        };
        let fractions: Vec<_> = (0..2)
            .map(|p| match (valid(p * 2), valid(p * 2 + 1)) {
                (Some(t), Some(e)) if e > t => json!({"reason":"selected_errors_exceed_total"}),
                (Some(0), Some(_)) => json!({"reason":"zero_total"}),
                (Some(t), Some(e)) => json!({"numerator":e,"denominator":t}),
                _ => json!({"reason":"incomplete_inputs"}),
            })
            .collect();
        EvidenceV1::new(
            "decant-loki-evidence:v1",
            json!({"format":"decant-loki-evidence","version":1,"plan_id":self.id,"source_id":self.source,
            "window":self.recipe.window,"interval_semantics":"left_open_right_closed","policy_id":identity("decant-log-policy:v1",&json!(self.recipe.policy)),
            "receipts":receipts,"counts":counts,"selected_error_fraction":fractions,"delta_all_logs":valid(0).zip(valid(2)).map(|(a,b)|b as i64-a as i64),
            "delta_selected_error_logs":valid(1).zip(valid(3)).map(|(a,b)|b as i64-a as i64),"examples":examples,
            "qualifications":{"measurement_kind":"backend_reported_log_count","request_error_rate":false,"sampling":"unknown","atomic_snapshot":false,
                "raw_body_emitted":false,"examples_enabled":self.recipe.policy.max_examples>0,"example_text":"untrusted_data","source_authenticated_by_hash":false,
                "missing_error_labels":"unknown","causal_diagnosis":false}}),
        )
    }
    fn examples(&self, v: &Value, period: usize) -> Result<Value, E> {
        if v["status"] != "success"
            || v.pointer("/data/resultType").and_then(Value::as_str) != Some("streams")
        {
            return Err(E::InvalidResponse);
        }
        let rows = v
            .pointer("/data/result")
            .and_then(Value::as_array)
            .filter(|a| a.len() <= 8)
            .ok_or(E::InvalidResponse)?;
        let mut items = BTreeMap::new();
        let mut returned = 0;
        let mut omitted = 0;
        let mut duplicates = 0;
        let mut invalid_lines = 0;
        for row in rows {
            let labels = row
                .get("stream")
                .and_then(Value::as_object)
                .ok_or(E::InvalidResponse)?;
            if labels
                .get(&self.profile.scope.service_label)
                .and_then(Value::as_str)
                != Some(&self.profile.scope.service)
                || labels
                    .get(&self.profile.scope.environment_label)
                    .and_then(Value::as_str)
                    != Some(&self.profile.scope.environment)
                || !labels
                    .get(&self.recipe.error_label)
                    .and_then(Value::as_str)
                    .is_some_and(|s| self.recipe.error_values.iter().any(|v| v == s))
                || labels.values().any(|v| !v.is_string())
            {
                return Err(E::InvalidResponse);
            }
            for entry in row
                .get("values")
                .and_then(Value::as_array)
                .ok_or(E::InvalidResponse)?
            {
                returned += 1;
                if returned > usize::from(self.recipe.policy.max_examples) {
                    return Err(E::ResponseTooLarge);
                }
                let pair = entry
                    .as_array()
                    .filter(|a| a.len() == 2 || (a.len() == 3 && a[2].is_object()))
                    .ok_or(E::InvalidResponse)?;
                let timestamp = uint(&pair[0])?;
                if self.recipe.window.period(timestamp) != Some(period) {
                    return Err(E::InvalidResponse);
                }
                let line = pair[1].as_str().ok_or(E::InvalidResponse)?;
                let reference = identity(
                    "decant-log-reference:v1",
                    &json!([self.id, period, labels, entry]),
                );
                let fingerprint = identity("decant-log-entry:v1", &json!([labels, entry]));
                let key = (timestamp, fingerprint);
                if items.contains_key(&key) {
                    duplicates += 1;
                    continue;
                }
                let body = bounded_json::parse(line.as_bytes(), 8192)
                    .ok()
                    .filter(Value::is_object);
                let mut fields = BTreeMap::new();
                if let Some(body) = body {
                    for key in &self.recipe.policy.allowed_fields {
                        match body.get(key).and_then(Value::as_str) {
                            Some(s) if s.len() <= 256 && !s.chars().any(char::is_control) => {
                                fields.insert(key.clone(), s.to_owned());
                            }
                            _ => omitted += 1,
                        }
                    }
                    omitted += body
                        .as_object()
                        .ok_or(E::InvalidResponse)?
                        .keys()
                        .filter(|k| !self.recipe.policy.allowed_fields.contains(k))
                        .count();
                } else {
                    invalid_lines += 1;
                }
                items.insert(
                    key,
                    json!({"reference":reference,"timestamp_ns":timestamp,"fields":fields}),
                );
            }
        }
        Ok(
            json!({"returned_entries":returned,"items":items.into_values().collect::<Vec<_>>(),"limit_reached":returned==usize::from(self.recipe.policy.max_examples),
            "duplicate_entries_folded":duplicates,"omitted_fields":omitted,"unparseable_lines":invalid_lines,"selection":"bounded_forward_query"}),
        )
    }
}
fn count(v: &Value, end: u64) -> Result<Option<u64>, E> {
    let n = scalar_vector(v, end)?;
    if let Some(n) = n
        && (n.fract() != 0.0 || n >= 9_007_199_254_740_992.0)
    {
        return Err(E::InvalidResponse);
    }
    if let Some(rows) = v.pointer("/data/result").and_then(Value::as_array)
        && rows.iter().any(|r| {
            !r.get("metric")
                .and_then(Value::as_object)
                .is_some_and(|m| m.is_empty())
        })
    {
        return Err(E::InvalidResponse);
    }
    n.map(|_| {
        exact_count(
            v.pointer("/data/result/0/value/1")
                .and_then(Value::as_str)
                .ok_or(E::InvalidResponse)?,
        )
    })
    .transpose()
}

// Validate decimal arithmetic before converting to an integer; f64 rounding must
// not turn a fractional or just-out-of-range backend value into an exact count.
fn exact_count(text: &str) -> Result<u64, E> {
    let mut parts = text.split(['e', 'E']);
    let mantissa = parts.next().ok_or(E::InvalidResponse)?;
    let exponent = parts
        .next()
        .map(|s| s.parse::<i32>().map_err(|_| E::InvalidResponse))
        .transpose()?
        .unwrap_or(0);
    if parts.next().is_some() || !(-38..=38).contains(&exponent) {
        return Err(E::InvalidResponse);
    }
    let mut coefficient = 0u128;
    let mut fraction = None;
    let mut digits = 0;
    for b in mantissa.bytes() {
        if b == b'.' && fraction.is_none() {
            fraction = Some(0);
            continue;
        }
        if !b.is_ascii_digit() {
            return Err(E::InvalidResponse);
        }
        digits += 1;
        coefficient = coefficient
            .checked_mul(10)
            .and_then(|n| n.checked_add(u128::from(b - b'0')))
            .ok_or(E::InvalidResponse)?;
        if let Some(n) = &mut fraction {
            *n += 1;
        }
    }
    let scale = exponent - fraction.unwrap_or(0);
    if digits == 0 || !(-38..=38).contains(&scale) {
        return Err(E::InvalidResponse);
    }
    let factor = 10u128.pow(scale.unsigned_abs());
    let n = if scale >= 0 {
        coefficient.checked_mul(factor).ok_or(E::InvalidResponse)?
    } else {
        if !coefficient.is_multiple_of(factor) {
            return Err(E::InvalidResponse);
        }
        coefficient / factor
    };
    if n >= 9_007_199_254_740_992 {
        return Err(E::InvalidResponse);
    }
    Ok(n as u64)
}
impl GrafanaV1 {
    pub async fn acquire_loki(&mut self, plan: &LokiPlanV1) -> Result<Vec<ReadV1>, E> {
        if self.profile.loki_uid.as_deref() != Some(&plan.uid)
            || source(&self.profile, &plan.uid)? != plan.source
        {
            return Err(E::InvalidConfiguration);
        }
        let mut reads = Vec::new();
        for (i, q) in plan.queries.iter().enumerate() {
            let p = if i < 4 { i / 2 } else { i - 4 };
            let (a, b) = plan.recipe.window.bounds(p);
            let before = self.http.usage().attempts;
            let mut path = vec![
                "api",
                "datasources",
                "proxy",
                "uid",
                &plan.uid,
                "loki",
                "api",
                "v1",
            ];
            let time = (b * 1_000_000_000).to_string();
            let start = (a * 1_000_000_000 + 1).to_string();
            let end = (b * 1_000_000_000 + 1).to_string();
            let limit = plan.recipe.policy.max_examples.to_string();
            let params = if i < 4 {
                path.push("query");
                vec![("query", q.as_str()), ("time", &time)]
            } else {
                path.push("query_range");
                vec![
                    ("query", q.as_str()),
                    ("start", &start),
                    ("end", &end),
                    ("limit", &limit),
                    ("direction", "forward"),
                ]
            };
            let response = self
                .http
                .get(&path, &params)
                .await
                .map(|r| r.body().to_vec());
            let mut c = plan.capture(i, response)?;
            c.attempted = self.http.usage().attempts != before;
            reads.push(c);
        }
        Ok(reads)
    }
}
