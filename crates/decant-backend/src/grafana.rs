//! Confirmed scope and finite datasource probes through one Grafana origin.
use crate::{
    bounded_json,
    transport::{
        BackendErrorV1 as E, HttpSessionV1, LimitsV1, MAX_RESPONSE_BYTES, validate_base_url_v1,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeSet, fmt};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopeV1 {
    pub service_label: String,
    pub service: String,
    pub environment_label: String,
    pub environment: String,
}
impl ScopeV1 {
    pub fn validate(&self) -> Result<(), E> {
        if !label(&self.service_label)
            || !label(&self.environment_label)
            || self.service_label == self.environment_label
            || [&self.service, &self.environment]
                .iter()
                .any(|s| s.is_empty() || s.len() > 128 || s.chars().any(char::is_control))
        {
            return Err(E::InvalidConfiguration);
        }
        Ok(())
    }
    pub(crate) fn selector(&self) -> String {
        format!(
            "{}={},{}={}",
            self.service_label,
            quoted(&self.service),
            self.environment_label,
            quoted(&self.environment)
        )
    }
}
pub(crate) fn quoted(value: &str) -> String {
    // JSON escaping is also appropriate for the supported PromQL/LogQL strings.
    serde_json::to_string(value).expect("string serialization")
}
pub fn label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .enumerate()
            .all(|(i, b)| b.is_ascii_alphabetic() || b == b'_' || (i > 0 && b.is_ascii_digit()))
}
pub fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 40
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileV1 {
    pub format: String,
    pub version: u8,
    pub url: String,
    pub token_env: String,
    pub ca_file: Option<String>,
    pub prometheus_uid: String,
    pub tempo_uid: Option<String>,
    pub loki_uid: Option<String>,
    pub scope: ScopeV1,
}
impl fmt::Debug for ProfileV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProfileV1 { redacted }")
    }
}
impl ProfileV1 {
    pub fn validate(&self) -> Result<(), E> {
        if self.format != "decant-grafana-profile"
            || self.version != 1
            || !label(&self.token_env)
            || !identifier(&self.prometheus_uid)
            || self
                .tempo_uid
                .iter()
                .chain(self.loki_uid.iter())
                .any(|v| !identifier(v))
            || self
                .ca_file
                .as_ref()
                .is_some_and(|v| v.is_empty() || v.len() > 4096 || v.chars().any(char::is_control))
        {
            return Err(E::InvalidConfiguration);
        }
        let ids: Vec<&str> = std::iter::once(self.prometheus_uid.as_str())
            .chain(self.tempo_uid.as_deref())
            .chain(self.loki_uid.as_deref())
            .collect();
        if ids.iter().collect::<BTreeSet<_>>().len() != ids.len() {
            return Err(E::InvalidConfiguration);
        }
        validate_base_url_v1(&self.url)?;
        self.scope.validate()
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, E> {
        let v = bounded_json::parse(bytes, 16 * 1024).map_err(|_| E::InvalidConfiguration)?;
        if !v.is_object() || !v.get("scope").is_some_and(Value::is_object) {
            return Err(E::InvalidConfiguration);
        }
        let profile: Self = serde_json::from_value(v).map_err(|_| E::InvalidConfiguration)?;
        profile.validate()?;
        Ok(profile)
    }
    pub fn encode(&self) -> Result<Vec<u8>, E> {
        self.validate()?;
        let bytes = serde_json::to_vec_pretty(self).map_err(|_| E::InvalidConfiguration)?;
        if bytes.len() > 16 * 1024 {
            return Err(E::InvalidConfiguration);
        }
        Ok(bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatasourceV1 {
    pub uid: String,
    pub kind: String,
}
pub async fn discover_v1(http: &mut HttpSessionV1) -> Result<Vec<DatasourceV1>, E> {
    let response = http.get(&["api", "datasources"], &[]).await?;
    let value = bounded_json::parse(response.body(), MAX_RESPONSE_BYTES)?;
    let rows = value
        .as_array()
        .filter(|v| v.len() <= 64)
        .ok_or(E::InvalidResponse)?;
    let mut found = Vec::new();
    let mut ids = BTreeSet::new();
    for row in rows {
        let uid = row
            .get("uid")
            .and_then(Value::as_str)
            .filter(|v| identifier(v))
            .ok_or(E::InvalidResponse)?;
        let kind = row
            .get("type")
            .and_then(Value::as_str)
            .ok_or(E::InvalidResponse)?;
        if !ids.insert(uid) {
            return Err(E::InvalidResponse);
        }
        if ["prometheus", "tempo", "loki"].contains(&kind) {
            found.push(DatasourceV1 {
                uid: uid.into(),
                kind: kind.into(),
            });
        }
    }
    found.sort_by(|a, b| a.uid.cmp(&b.uid));
    Ok(found)
}
pub fn confirm_sources_v1(profile: &ProfileV1, sources: &[DatasourceV1]) -> Result<(), E> {
    profile.validate()?;
    for (id, kind) in [
        (Some(&profile.prometheus_uid), "prometheus"),
        (profile.tempo_uid.as_ref(), "tempo"),
        (profile.loki_uid.as_ref(), "loki"),
    ] {
        if let Some(id) = id
            && !sources.iter().any(|s| &s.uid == id && s.kind == kind)
        {
            return Err(E::InvalidConfiguration);
        }
    }
    Ok(())
}

pub struct GrafanaV1 {
    pub(crate) profile: ProfileV1,
    pub(crate) http: HttpSessionV1,
}
impl GrafanaV1 {
    /// Reserve response capacity for a separate model session before any reads.
    pub fn with_response_allowance(mut self, bytes: usize) -> Result<Self, E> {
        self.http.restrict_bytes(bytes)?;
        Ok(self)
    }
    pub fn usage(&self) -> crate::transport::UsageV1 {
        self.http.usage()
    }
    pub fn new(profile: ProfileV1, bearer: &str, ca: Option<&[u8]>) -> Result<Self, E> {
        profile.validate()?;
        let http = HttpSessionV1::new(&profile.url, bearer, ca, LimitsV1::default())?;
        Ok(Self { profile, http })
    }
    pub async fn doctor(&mut self, end_seconds: u64) -> Result<DoctorV1, E> {
        if !(300..=253402300799).contains(&end_seconds) {
            return Err(E::InvalidRequest);
        }
        let time = end_seconds.to_string();
        let start = (end_seconds - 300).to_string();
        let selector = self.profile.scope.selector();
        let mut sources = Vec::new();
        for (kind, uid) in [
            ("prometheus", Some(self.profile.prometheus_uid.clone())),
            ("tempo", self.profile.tempo_uid.clone()),
            ("loki", self.profile.loki_uid.clone()),
        ] {
            let Some(uid) = uid else {
                sources.push(ProbeV1 {
                    source: kind,
                    status: "not_configured",
                    reason: None,
                    limited_probe: true,
                });
                continue;
            };
            let result = match kind {
                "prometheus" => {
                    let q = format!("count({{{selector}}})");
                    self.http
                        .get(
                            &[
                                "api",
                                "datasources",
                                "proxy",
                                "uid",
                                &uid,
                                "api",
                                "v1",
                                "query",
                            ],
                            &[
                                ("query", &q),
                                ("time", &time),
                                ("timeout", "10s"),
                                ("limit", "1"),
                            ],
                        )
                        .await
                }
                "loki" => {
                    let q = format!("sum(count_over_time({{{selector}}}[5m]))");
                    self.http
                        .get(
                            &[
                                "api",
                                "datasources",
                                "proxy",
                                "uid",
                                &uid,
                                "loki",
                                "api",
                                "v1",
                                "query",
                            ],
                            &[("query", &q), ("time", &format!("{end_seconds}000000000"))],
                        )
                        .await
                }
                _ => {
                    let q = format!(
                        "{{resource.service.name={} && resource.deployment.environment.name={}}}",
                        quoted(&self.profile.scope.service),
                        quoted(&self.profile.scope.environment)
                    );
                    self.http
                        .get(
                            &["api", "datasources", "proxy", "uid", &uid, "api", "search"],
                            &[
                                ("q", &q),
                                ("start", &start),
                                ("end", &time),
                                ("limit", "1"),
                                ("spss", "1"),
                            ],
                        )
                        .await
                }
            };
            let outcome = result.and_then(|r| probe_status(kind, r.body(), end_seconds));
            let (status, reason) = match outcome {
                Ok(status) => (status, None),
                Err(error) => ("unavailable", Some(error.to_string())),
            };
            sources.push(ProbeV1 {
                source: kind,
                status,
                reason,
                limited_probe: true,
            });
        }
        Ok(DoctorV1 {
            format: "decant-grafana-doctor",
            version: 1,
            start_seconds: end_seconds - 300,
            end_seconds,
            ready: sources
                .iter()
                .all(|s| s.status == "ready" || s.status == "not_configured"),
            atomic_snapshot: false,
            service_health_assessed: false,
            sources,
        })
    }
}
#[derive(Serialize, Debug)]
pub struct DoctorV1 {
    pub format: &'static str,
    pub version: u8,
    pub start_seconds: u64,
    pub end_seconds: u64,
    pub ready: bool,
    pub atomic_snapshot: bool,
    pub service_health_assessed: bool,
    pub sources: Vec<ProbeV1>,
}
#[derive(Serialize, Debug)]
pub struct ProbeV1 {
    pub source: &'static str,
    pub status: &'static str,
    pub reason: Option<String>,
    pub limited_probe: bool,
}
fn probe_status(kind: &str, bytes: &[u8], end: u64) -> Result<&'static str, E> {
    let value = bounded_json::parse(bytes, MAX_RESPONSE_BYTES)?;
    if !value.is_object() {
        return Err(E::InvalidResponse);
    }
    let mut partial = annotations(&value)?;
    if kind == "tempo" {
        let count = match value.get("traces") {
            Some(Value::Array(rows))
                if rows.len() <= 1
                    && rows.iter().all(|row| {
                        row.get("traceID")
                            .and_then(Value::as_str)
                            .is_some_and(|id| {
                                id.len() == 32
                                    && id.bytes().all(|b| b.is_ascii_hexdigit())
                                    && id.bytes().any(|b| b != b'0')
                            })
                    }) =>
            {
                rows.len()
            }
            None if value.get("metrics").is_some_and(Value::is_object) => 0,
            _ => return Err(E::InvalidResponse),
        };
        if value.get("error").is_some() {
            return Err(E::InvalidResponse);
        }
        if let Some(metrics) = value.get("metrics") {
            if !metrics.is_object() {
                return Err(E::InvalidResponse);
            }
            let counter = |key: &str| -> Result<Option<u64>, E> {
                metrics
                    .get(key)
                    .map(|v| {
                        v.as_u64()
                            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                            .ok_or(E::InvalidResponse)
                    })
                    .transpose()
            };
            partial |= counter("failedBlocks")?.is_some_and(|n| n > 0);
            if let Some(total) = counter("totalJobs")? {
                let completed = counter("completedJobs")?.ok_or(E::InvalidResponse)?;
                if completed > total {
                    return Err(E::InvalidResponse);
                }
                partial |= completed < total;
            }
        }
        return Ok(if partial {
            "partial"
        } else if count == 0 {
            "empty"
        } else {
            "ready"
        });
    }
    let scalar = scalar_vector(&value, end)?;
    Ok(if partial {
        "partial"
    } else if scalar.is_none_or(|v| v == 0.0) {
        "empty"
    } else {
        "ready"
    })
}
pub(crate) fn annotations(value: &Value) -> Result<bool, E> {
    let mut present = false;
    for name in ["warnings", "infos"] {
        if let Some(v) = value.get(name) {
            let list = v.as_array().ok_or(E::InvalidResponse)?;
            if !list.iter().all(Value::is_string) {
                return Err(E::InvalidResponse);
            }
            present |= !list.is_empty();
        }
    }
    Ok(present)
}
pub(crate) fn scalar_vector(value: &Value, end: u64) -> Result<Option<f64>, E> {
    if value.get("status").and_then(Value::as_str) != Some("success")
        || value.pointer("/data/resultType").and_then(Value::as_str) != Some("vector")
    {
        return Err(E::InvalidResponse);
    }
    let rows = value
        .pointer("/data/result")
        .and_then(Value::as_array)
        .ok_or(E::InvalidResponse)?;
    if rows.is_empty() {
        return Ok(None);
    }
    if rows.len() != 1
        || !rows[0].get("metric").is_some_and(Value::is_object)
        || rows[0].get("histogram").is_some()
    {
        return Err(E::InvalidResponse);
    }
    let pair = rows[0]
        .get("value")
        .and_then(Value::as_array)
        .filter(|v| v.len() == 2)
        .ok_or(E::InvalidResponse)?;
    if pair[0].as_f64() != Some(end as f64) {
        return Err(E::InvalidResponse);
    }
    let text = pair[1]
        .as_str()
        .filter(|s| s.len() <= 64)
        .ok_or(E::InvalidResponse)?;
    let number = text.parse::<f64>().map_err(|_| E::InvalidResponse)?;
    if !number.is_finite() || number < 0.0 || underflowed(text, number) {
        return Err(E::InvalidResponse);
    }
    Ok(Some(number))
}

pub(crate) fn underflowed(text: &str, number: f64) -> bool {
    number == 0.0
        && text
            .split(['e', 'E'])
            .next()
            .is_some_and(|mantissa| mantissa.bytes().any(|b| (b'1'..=b'9').contains(&b)))
}
