//! Shared bounded capture primitives. Raw responses are private, not report text.
use crate::{
    grafana::ProfileV1,
    transport::{BackendErrorV1 as E, MAX_RESPONSE_BYTES},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::fmt;

pub(crate) fn hash(domain: &str, bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(domain.as_bytes());
    h.update([0]);
    h.update(bytes);
    let mut s = String::from("sha256:v1:");
    for b in h.finalize() {
        use std::fmt::Write;
        write!(&mut s, "{b:02x}").expect("string");
    }
    s
}
pub(crate) fn identity(domain: &str, value: &Value) -> String {
    hash(domain, &serde_json::to_vec(value).expect("JSON value"))
}
pub(crate) fn source(profile: &ProfileV1, uid: &str) -> Result<String, E> {
    profile.validate()?;
    Ok(identity(
        "decant-backend-source:v1",
        &json!({"origin":profile.url,"uid":uid,"scope":profile.scope}),
    ))
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WindowV1 {
    pub end_seconds: u64,
    pub window_seconds: u32,
}
impl WindowV1 {
    pub fn validate(self) -> Result<(), E> {
        if !(60..=3600).contains(&self.window_seconds)
            || self.end_seconds < u64::from(self.window_seconds) * 2
            || self.end_seconds > u64::from(u32::MAX)
        {
            return Err(E::InvalidConfiguration);
        }
        Ok(())
    }
    pub(crate) fn bounds(self, period: usize) -> (u64, u64) {
        let end = self.end_seconds
            - if period == 0 {
                u64::from(self.window_seconds)
            } else {
                0
            };
        (end - u64::from(self.window_seconds), end)
    }
    pub(crate) fn period(self, ns: u64) -> Option<usize> {
        (0..2).find(|&p| {
            let (a, b) = self.bounds(p);
            ns > a * 1_000_000_000 && ns <= b * 1_000_000_000
        })
    }
}
#[derive(Clone)]
pub struct ReadV1 {
    pub(crate) plan: String,
    pub(crate) key: String,
    pub(crate) response: Result<Vec<u8>, E>,
    pub(crate) attempted: bool,
}
impl fmt::Debug for ReadV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ReadV1 { redacted }")
    }
}
impl ReadV1 {
    pub(crate) fn new(plan: &str, key: String, response: Result<Vec<u8>, E>) -> Result<Self, E> {
        if response
            .as_ref()
            .is_ok_and(|b| b.len() > MAX_RESPONSE_BYTES)
        {
            return Err(E::ResponseTooLarge);
        }
        Ok(Self {
            plan: plan.into(),
            key,
            response,
            attempted: true,
        })
    }
    pub(crate) fn receipt(&self, status: &str) -> Value {
        json!({"key":self.key,"attempted":self.attempted,"status":if self.attempted {status} else {"unattempted"},
            "response_hash":self.response.as_ref().ok().map(|b|hash("decant-backend-response:v1",b)),
            "reason":self.response.as_ref().err().map(ToString::to_string)})
    }
}
pub struct EvidenceV1 {
    bytes: Vec<u8>,
    id: String,
}
impl fmt::Debug for EvidenceV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EvidenceV1")
            .field("bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}
impl EvidenceV1 {
    pub(crate) fn new(domain: &str, value: Value) -> Result<Self, E> {
        let bytes = serde_json::to_vec(&value).map_err(|_| E::InvalidResponse)?;
        if bytes.len() > 64 * 1024 {
            return Err(E::ResponseTooLarge);
        }
        let id = hash(domain, &bytes);
        Ok(Self { bytes, id })
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn id(&self) -> &str {
        &self.id
    }
}
pub(crate) fn text(value: &Value) -> Result<String, E> {
    let s = value
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= 128 && !s.chars().any(char::is_control))
        .ok_or(E::InvalidResponse)?;
    Ok(s.into())
}
pub(crate) fn uint(value: &Value) -> Result<u64, E> {
    value
        .as_u64()
        .or_else(|| {
            value
                .as_str()
                .filter(|s| !s.is_empty() && s.len() <= 20 && s.bytes().all(|b| b.is_ascii_digit()))
                .and_then(|s| s.parse().ok())
        })
        .ok_or(E::InvalidResponse)
}
pub(crate) fn allowlist(values: &[String]) -> Result<(), E> {
    if values.len() > 16
        || values.windows(2).any(|w| w[0] >= w[1])
        || values
            .iter()
            .any(|s| text(&Value::String(s.clone())).is_err())
    {
        return Err(E::InvalidConfiguration);
    }
    Ok(())
}
