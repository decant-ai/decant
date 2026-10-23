//! Sampled Tempo evidence, never population statistics or a causal diagnosis.
pub use crate::evidence::{EvidenceV1, ReadV1, WindowV1};
use crate::{
    bounded_json,
    evidence::{allowlist, hash, identity, source, text, uint},
    grafana::{GrafanaV1, ProfileV1, annotations, quoted},
    transport::{BackendErrorV1 as E, MAX_RESPONSE_BYTES},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TracePolicyV1 {
    pub operation_names: Vec<String>,
    pub dependency_services: Vec<String>,
}
impl fmt::Debug for TracePolicyV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TracePolicyV1 { redacted }")
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TempoRecipeV1 {
    pub window: WindowV1,
    pub traces_per_window: u8,
    pub policy: TracePolicyV1,
}
impl fmt::Debug for TempoRecipeV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TempoRecipeV1 { redacted }")
    }
}
pub struct TempoPlanV1 {
    pub(crate) profile: ProfileV1,
    pub(crate) recipe: TempoRecipeV1,
    source: String,
    id: String,
    uid: String,
    query: String,
}
impl fmt::Debug for TempoPlanV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TempoPlanV1 { redacted }")
    }
}
impl TempoPlanV1 {
    pub fn new(profile: &ProfileV1, mut recipe: TempoRecipeV1) -> Result<Self, E> {
        recipe.window.validate()?;
        if !(1..=4).contains(&recipe.traces_per_window) {
            return Err(E::InvalidConfiguration);
        }
        recipe.policy.operation_names.sort();
        recipe.policy.dependency_services.sort();
        allowlist(&recipe.policy.operation_names)?;
        allowlist(&recipe.policy.dependency_services)?;
        let uid = profile.tempo_uid.clone().ok_or(E::InvalidConfiguration)?;
        let source = source(profile, &uid)?;
        let query = format!(
            "{{resource.service.name={} && resource.deployment.environment.name={}}}",
            quoted(&profile.scope.service),
            quoted(&profile.scope.environment)
        );
        let id = identity(
            "decant-tempo-plan:v1",
            &json!({"source":source,"recipe":recipe,"query":query}),
        );
        Ok(Self {
            profile: profile.clone(),
            recipe,
            source,
            id,
            uid,
            query,
        })
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn search_query(&self) -> &str {
        &self.query
    }
    pub fn capture_search(&self, period: usize, response: Result<Vec<u8>, E>) -> Result<ReadV1, E> {
        if period > 1 {
            return Err(E::InvalidRequest);
        }
        ReadV1::new(&self.id, format!("search-{period}"), response)
    }
    pub fn capture_trace(&self, trace_id: &str, response: Result<Vec<u8>, E>) -> Result<ReadV1, E> {
        let id = hex_id(trace_id, 16)?;
        ReadV1::new(&self.id, format!("trace-{id}"), response)
    }
    pub fn reduce(&self, captures: &[ReadV1]) -> Result<EvidenceV1, E> {
        if !(2..=10).contains(&captures.len()) {
            return Err(E::InvalidResponse);
        }
        let mut slots = BTreeMap::new();
        for c in captures {
            if c.plan != self.id || slots.insert(c.key.as_str(), c).is_some() {
                return Err(E::InvalidResponse);
            }
        }
        let mut expected = BTreeSet::new();
        let mut receipts = Vec::new();
        let mut searches = Vec::new();
        for p in 0..2 {
            let key = format!("search-{p}");
            let c = slots.remove(key.as_str()).ok_or(E::InvalidResponse)?;
            let search = c
                .response
                .as_ref()
                .ok()
                .and_then(|b| parse_search(b, self.recipe.traces_per_window).ok());
            let status = match &search {
                _ if !c.attempted => "unattempted",
                Some(s) if s.partial => "partial",
                Some(s) if s.ids.is_empty() => "empty",
                Some(_) => "observed",
                None => "unavailable",
            };
            let mut receipt = c.receipt(status);
            if search.is_none() && c.response.is_ok() {
                receipt["reason"] = json!("invalid_response");
            }
            receipt["request"] = json!({"route":"api/search","q":self.query,"start_seconds":self.recipe.window.bounds(p).0,
                "end_seconds":self.recipe.window.bounds(p).1,"limit":self.recipe.traces_per_window,"spss":1});
            receipts.push(receipt);
            if let Some(s) = &search {
                expected.extend(s.ids.iter().cloned());
            }
            searches.push(json!({"period":p,"status":status,"limit_reached":search.as_ref().is_some_and(|s|s.limit_reached),
                "selected_count":search.as_ref().map(|s|s.ids.len()),"duplicate_results":search.as_ref().map(|s|s.duplicates)}));
        }
        if slots.len() != expected.len() {
            return Err(E::InvalidResponse);
        }
        let mut spans: BTreeMap<(String, String), Span> = BTreeMap::new();
        let mut duplicates = 0;
        let mut partial_traces = 0;
        let mut dropped = 0u64;
        for id in expected {
            let key = format!("trace-{id}");
            let c = slots.remove(key.as_str()).ok_or(E::InvalidResponse)?;
            let parsed = c.response.as_ref().ok().map(|b| parse_trace(b, &id));
            // A conflicting logical identity is a hard failure, not an omitted successful trace.
            let parsed = match parsed {
                Some(Ok(v)) => Some(v),
                Some(Err(ParseError::Conflict)) => return Err(E::InvalidResponse),
                _ => None,
            };
            let status = match &parsed {
                Some(t) if t.partial => "partial",
                Some(t) if t.spans.is_empty() => "empty",
                Some(_) => "observed",
                None => "unavailable",
            };
            let mut receipt = c.receipt(status);
            if parsed.is_none() && c.response.is_ok() {
                receipt["reason"] = json!("invalid_response");
            }
            // Do not emit raw trace IDs; this key is scoped to the immutable response receipt.
            receipt["key"] = json!(identity("decant-selected-trace:v1", &json!([self.id, id])));
            receipt["request"] = json!({"route":"api/v2/traces/:selected_id","start_seconds":self.recipe.window.bounds(0).0,
                "end_seconds":self.recipe.window.end_seconds,"span_pruning":false});
            receipts.push(receipt);
            if let Some(t) = parsed {
                partial_traces += usize::from(t.partial);
                duplicates += t.duplicates;
                dropped = dropped.checked_add(t.dropped).ok_or(E::InvalidResponse)?;
                for s in t.spans {
                    let key = (s.trace.clone(), s.id.clone());
                    if let Some(old) = spans.get(&key) {
                        if old.fingerprint != s.fingerprint {
                            return Err(E::InvalidResponse);
                        }
                        duplicates += 1;
                    } else {
                        if spans.len() == 2048 {
                            return Err(E::ResponseTooLarge);
                        }
                        spans.insert(key, s);
                    }
                }
            }
        }
        let mut groups: BTreeMap<(String, String), Group> = BTreeMap::new();
        let mut excluded = 0;
        let mut missing_parents = 0;
        let mut omitted_names = 0;
        let mut unknown_kind = 0;
        for s in spans.values() {
            if s.environment.as_deref() != Some(&self.profile.scope.environment)
                || self.recipe.window.period(s.start).is_none()
            {
                excluded += 1;
                continue;
            }
            if !s.parent.is_empty() && !spans.contains_key(&(s.trace.clone(), s.parent.clone())) {
                missing_parents += 1;
            }
            if s.service.as_deref() == Some(&self.profile.scope.service) && s.kind == 0 {
                unknown_kind += 1;
            }
            if s.kind != 2 {
                continue;
            }
            let p = self
                .recipe
                .window
                .period(s.start)
                .ok_or(E::InvalidResponse)?;
            if s.service.as_deref() == Some(&self.profile.scope.service) {
                let key = identity("decant-operation:v1", &json!(s.name));
                let name = s
                    .name
                    .as_ref()
                    .filter(|n| self.recipe.policy.operation_names.contains(n))
                    .cloned();
                if name.is_none() {
                    omitted_names += 1;
                }
                add_group(&mut groups, ("operation".into(), key), name, p, s, &self.id)?;
            }
            if let Some(parent) = spans.get(&(s.trace.clone(), s.parent.clone()))
                && parent.kind == 3
                && parent.service.as_deref() == Some(&self.profile.scope.service)
                && parent.environment == s.environment
                && s.service.is_some()
                && s.service != parent.service
            {
                let key = identity("decant-dependency:v1", &json!(s.service));
                let name = s
                    .service
                    .as_ref()
                    .filter(|n| self.recipe.policy.dependency_services.contains(n))
                    .cloned();
                if name.is_none() {
                    omitted_names += 1;
                }
                add_group(
                    &mut groups,
                    ("dependency".into(), key),
                    name,
                    p,
                    s,
                    &self.id,
                )?;
            }
        }
        let groups: Vec<Value> = groups
            .into_iter()
            .map(|((kind, id), g)| {
                json!({"kind":kind,"group_id":id,"name":g.name,
            "baseline":g.stats[0].value(),"current":g.stats[1].value()})
            })
            .collect();
        EvidenceV1::new(
            "decant-tempo-evidence:v1",
            json!({"format":"decant-tempo-evidence","version":1,"plan_id":self.id,
            "source_id":self.source,"window":self.recipe.window,"interval_semantics":"span_start_left_open_right_closed",
            "policy_id":identity("decant-trace-policy:v1",&json!(self.recipe.policy)),"searches":searches,"receipts":receipts,"groups":groups,
            "gaps":{"partial_traces":partial_traces,"missing_parents":missing_parents,"dropped_items":dropped,"excluded_spans":excluded,
                "unknown_kind_scoped_spans":unknown_kind,"omitted_names":omitted_names},"duplicate_spans_folded":duplicates,"unique_captured_spans":spans.len(),
            "qualifications":{"sampling":"unknown","selection":"first_n_search_matches","population_statistics":false,"atomic_snapshot":false,
                "trace_completeness":"unknown_even_if_backend_complete","source_authenticated_by_hash":false,"causal_diagnosis":false,"raw_attributes_emitted":false}}),
        )
    }
}
impl GrafanaV1 {
    pub async fn acquire_tempo(&mut self, plan: &TempoPlanV1) -> Result<Vec<ReadV1>, E> {
        if source(&self.profile, &plan.uid)? != plan.source
            || self.profile.tempo_uid.as_deref() != Some(&plan.uid)
        {
            return Err(E::InvalidConfiguration);
        }
        let mut reads = Vec::new();
        let mut ids = BTreeSet::new();
        for p in 0..2 {
            let (a, b) = plan.recipe.window.bounds(p);
            let limit = plan.recipe.traces_per_window.to_string();
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
                        "search",
                    ],
                    &[
                        ("q", &plan.query),
                        ("start", &a.to_string()),
                        ("end", &b.to_string()),
                        ("limit", &limit),
                        ("spss", "1"),
                    ],
                )
                .await
                .map(|r| r.body().to_vec());
            if let Ok(b) = &response
                && let Ok(s) = parse_search(b, plan.recipe.traces_per_window)
            {
                ids.extend(s.ids);
            }
            let mut c = plan.capture_search(p, response)?;
            c.attempted = self.http.usage().attempts != before;
            reads.push(c);
        }
        for id in ids {
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
                        "v2",
                        "traces",
                        &id,
                    ],
                    &[
                        ("start", &plan.recipe.window.bounds(0).0.to_string()),
                        ("end", &plan.recipe.window.end_seconds.to_string()),
                        ("span_pruning", "false"),
                    ],
                )
                .await
                .map(|r| r.body().to_vec());
            let mut c = plan.capture_trace(&id, response)?;
            c.attempted = self.http.usage().attempts != before;
            reads.push(c);
        }
        Ok(reads)
    }
}
struct Search {
    ids: BTreeSet<String>,
    partial: bool,
    limit_reached: bool,
    duplicates: usize,
}
fn parse_search(bytes: &[u8], limit: u8) -> Result<Search, E> {
    let v = bounded_json::parse(bytes, MAX_RESPONSE_BYTES)?;
    if !v.is_object() || v.get("error").is_some() {
        return Err(E::InvalidResponse);
    }
    let mut partial = annotations(&v)?;
    if let Some(m) = v.get("metrics") {
        if !m.is_object() {
            return Err(E::InvalidResponse);
        }
        partial |= m
            .get("failedBlocks")
            .map(uint)
            .transpose()?
            .is_some_and(|n| n > 0);
        if let Some(total) = m.get("totalJobs") {
            let total = uint(total)?;
            let completed = m.get("completedJobs").map(uint).transpose()?.unwrap_or(0);
            if completed > total {
                return Err(E::InvalidResponse);
            }
            partial |= completed < total;
        }
    }
    let empty = Vec::new();
    let rows = match v.get("traces") {
        Some(v) => v.as_array().ok_or(E::InvalidResponse)?,
        None if v.get("metrics").is_some() => &empty,
        _ => return Err(E::InvalidResponse),
    };
    if rows.len() > usize::from(limit) {
        return Err(E::ResponseTooLarge);
    }
    let mut ids = BTreeSet::new();
    for row in rows {
        ids.insert(hex_id(
            row.get("traceID")
                .and_then(Value::as_str)
                .ok_or(E::InvalidResponse)?,
            16,
        )?);
    }
    Ok(Search {
        duplicates: rows.len() - ids.len(),
        ids,
        partial,
        limit_reached: rows.len() == usize::from(limit),
    })
}
fn hex_id(s: &str, n: usize) -> Result<String, E> {
    if s.len() != n * 2 || !s.bytes().all(|b| b.is_ascii_hexdigit()) || s.bytes().all(|b| b == b'0')
    {
        return Err(E::InvalidResponse);
    }
    Ok(s.to_ascii_lowercase())
}
fn identifier(v: &Value, n: usize) -> Result<String, E> {
    let s = v.as_str().ok_or(E::InvalidResponse)?;
    if s.len() == n * 2 {
        return hex_id(s, n);
    }
    if s.len() != n.div_ceil(3) * 4 {
        return Err(E::InvalidResponse);
    }
    let mut buffer = [0u8; 18];
    let size = STANDARD
        .decode_slice(s, &mut buffer)
        .map_err(|_| E::InvalidResponse)?;
    if size != n || buffer[..n].iter().all(|b| *b == 0) {
        return Err(E::InvalidResponse);
    }
    let mut out = String::new();
    for b in &buffer[..n] {
        use std::fmt::Write;
        write!(&mut out, "{b:02x}").expect("string");
    }
    Ok(out)
}
fn attrs(v: Option<&Value>) -> Result<BTreeMap<String, Value>, E> {
    let mut a = BTreeMap::new();
    if let Some(v) = v {
        for item in v
            .as_array()
            .filter(|a| a.len() <= 64)
            .ok_or(E::InvalidResponse)?
        {
            let key = text(item.get("key").ok_or(E::InvalidResponse)?)?;
            let value = item
                .get("value")
                .filter(|v| v.is_object())
                .ok_or(E::InvalidResponse)?
                .clone();
            if a.insert(key, value).is_some() {
                return Err(E::InvalidResponse);
            }
        }
    }
    Ok(a)
}
fn attr(a: &BTreeMap<String, Value>, key: &str) -> Result<Option<String>, E> {
    a.get(key)
        .map(|v| text(v.get("stringValue").ok_or(E::InvalidResponse)?))
        .transpose()
}
fn enumeration(v: Option<&Value>, names: &[&str]) -> Result<u64, E> {
    match v {
        None => Ok(0),
        Some(Value::String(s)) => names
            .iter()
            .position(|n| *n == s)
            .map(|n| n as u64)
            .ok_or(E::InvalidResponse),
        Some(v) => v
            .as_u64()
            .filter(|n| *n < (names.len() as u64))
            .ok_or(E::InvalidResponse),
    }
}
struct Span {
    trace: String,
    id: String,
    parent: String,
    service: Option<String>,
    environment: Option<String>,
    name: Option<String>,
    kind: u64,
    status: u64,
    start: u64,
    duration: u64,
    fingerprint: String,
}
enum ParseError {
    Invalid,
    Conflict,
}
impl From<E> for ParseError {
    fn from(_: E) -> Self {
        Self::Invalid
    }
}
struct Trace {
    spans: Vec<Span>,
    duplicates: usize,
    dropped: u64,
    partial: bool,
}
fn parse_trace(bytes: &[u8], expected: &str) -> Result<Trace, ParseError> {
    let v = bounded_json::parse(bytes, MAX_RESPONSE_BYTES)?;
    let partial = match v.get("status") {
        None => false,
        Some(Value::String(s)) if s == "COMPLETE" => false,
        Some(Value::String(s)) if s == "PARTIAL" => true,
        Some(n) if n.as_u64() == Some(0) => false,
        Some(n) if n.as_u64() == Some(1) => true,
        _ => return Err(ParseError::Invalid),
    };
    let trace = v
        .get("trace")
        .filter(|v| v.is_object())
        .ok_or(E::InvalidResponse)?;
    let resources = match (trace.get("resourceSpans"), trace.get("batches")) {
        (Some(v), None) | (None, Some(v)) => v.as_array().ok_or(E::InvalidResponse)?,
        _ => return Err(ParseError::Invalid),
    };
    let mut spans: BTreeMap<String, Span> = BTreeMap::new();
    let mut duplicates = 0;
    let mut count = 0;
    let mut dropped = 0u64;
    for r in resources {
        let resource = r
            .get("resource")
            .filter(|v| v.is_object())
            .ok_or(E::InvalidResponse)?;
        let attributes = attrs(resource.get("attributes"))?;
        let service = attr(&attributes, "service.name")?;
        let environment = attr(&attributes, "deployment.environment.name")?;
        dropped = dropped
            .checked_add(
                resource
                    .get("droppedAttributesCount")
                    .map(uint)
                    .transpose()?
                    .unwrap_or(0),
            )
            .ok_or(E::InvalidResponse)?;
        for scope in r
            .get("scopeSpans")
            .and_then(Value::as_array)
            .ok_or(E::InvalidResponse)?
        {
            for value in scope
                .get("spans")
                .and_then(Value::as_array)
                .ok_or(E::InvalidResponse)?
            {
                count += 1;
                if count > 256 {
                    return Err(ParseError::Invalid);
                }
                let mut normalized = value.clone();
                let object = normalized.as_object_mut().ok_or(E::InvalidResponse)?;
                let trace = identifier(object.get("traceId").ok_or(E::InvalidResponse)?, 16)?;
                if trace != expected {
                    return Err(ParseError::Invalid);
                }
                let id = identifier(object.get("spanId").ok_or(E::InvalidResponse)?, 8)?;
                let parent = match object.get("parentSpanId") {
                    None => String::new(),
                    Some(Value::String(s)) if s.is_empty() => String::new(),
                    Some(v) => identifier(v, 8)?,
                };
                if parent == id {
                    return Err(ParseError::Invalid);
                }
                let start = uint(object.get("startTimeUnixNano").ok_or(E::InvalidResponse)?)?;
                let end = uint(object.get("endTimeUnixNano").ok_or(E::InvalidResponse)?)?;
                let duration = end.checked_sub(start).ok_or(E::InvalidResponse)?;
                let name = object.get("name").map(text).transpose()?;
                let kind = enumeration(
                    object.get("kind"),
                    &[
                        "SPAN_KIND_UNSPECIFIED",
                        "SPAN_KIND_INTERNAL",
                        "SPAN_KIND_SERVER",
                        "SPAN_KIND_CLIENT",
                        "SPAN_KIND_PRODUCER",
                        "SPAN_KIND_CONSUMER",
                    ],
                )?;
                let status = match object.get("status") {
                    None => 0,
                    Some(v) if v.is_object() => enumeration(
                        v.get("code"),
                        &["STATUS_CODE_UNSET", "STATUS_CODE_OK", "STATUS_CODE_ERROR"],
                    )?,
                    _ => return Err(ParseError::Invalid),
                };
                attrs(object.get("attributes"))?;
                for k in [
                    "droppedAttributesCount",
                    "droppedEventsCount",
                    "droppedLinksCount",
                ] {
                    dropped = dropped
                        .checked_add(object.get(k).map(uint).transpose()?.unwrap_or(0))
                        .ok_or(E::InvalidResponse)?;
                }
                object.insert("traceId".into(), json!(trace));
                object.insert("spanId".into(), json!(id));
                object.insert("parentSpanId".into(), json!(parent));
                let fingerprint = identity(
                    "decant-tempo-span:v1",
                    &json!({"span":normalized,"resource":resource,"scope":scope.get("scope")}),
                );
                let s = Span {
                    trace,
                    id: id.clone(),
                    parent,
                    service: service.clone(),
                    environment: environment.clone(),
                    name,
                    kind,
                    status,
                    start,
                    duration,
                    fingerprint,
                };
                if let Some(old) = spans.get(&id) {
                    if old.fingerprint != s.fingerprint {
                        return Err(ParseError::Conflict);
                    }
                    duplicates += 1;
                } else {
                    spans.insert(id, s);
                }
            }
        }
    }
    // A cyclic parent graph is malformed, not an observed dependency chain.
    for span in spans.values() {
        let mut parent = span.parent.as_str();
        let mut visited = BTreeSet::new();
        visited.insert(span.id.as_str());
        while let Some(next) = spans.get(parent) {
            if !visited.insert(next.id.as_str()) {
                return Err(ParseError::Invalid);
            }
            parent = &next.parent;
        }
    }
    Ok(Trace {
        spans: spans.into_values().collect(),
        duplicates,
        dropped,
        partial: partial || annotations(&v)?,
    })
}
#[derive(Default)]
struct Stats {
    durations: Vec<u64>,
    errors: usize,
    unknown_status: usize,
    examples: Vec<String>,
}
impl Stats {
    fn value(&self) -> Value {
        let mut d = self.durations.clone();
        d.sort_unstable();
        json!({"observed_spans":d.len(),"error_spans":self.errors,
        "unknown_status_spans":self.unknown_status,"p95_duration_ns":if d.is_empty(){None}else{Some(d[(95*d.len()).div_ceil(100)-1])},
        "examples":self.examples,"examples_omitted":d.len().saturating_sub(self.examples.len())})
    }
}
struct Group {
    name: Option<String>,
    stats: [Stats; 2],
}
fn add_group(
    groups: &mut BTreeMap<(String, String), Group>,
    key: (String, String),
    name: Option<String>,
    p: usize,
    s: &Span,
    plan: &str,
) -> Result<(), E> {
    if !groups.contains_key(&key) && groups.len() == 32 {
        return Err(E::ResponseTooLarge);
    }
    let g = groups.entry(key).or_insert_with(|| Group {
        name,
        stats: Default::default(),
    });
    let stats = &mut g.stats[p];
    stats.durations.push(s.duration);
    stats.errors += usize::from(s.status == 2);
    stats.unknown_status += usize::from(s.status == 0);
    if stats.examples.len() < 2 {
        stats.examples.push(hash(
            "decant-span-reference:v1",
            &serde_json::to_vec(&json!([plan, s.trace, s.id, s.fingerprint])).expect("value"),
        ));
    }
    Ok(())
}
