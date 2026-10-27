//! One origin, one in-flight read, explicit finite budgets.

use reqwest::{Client, Url, header};
use std::{fmt, time::Duration};
use tokio::time::Instant;

pub const MAX_RESPONSE_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_TOTAL_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendErrorV1 {
    InvalidConfiguration,
    InvalidRequest,
    PermissionDenied,
    RateLimited,
    RedirectRefused,
    Timeout,
    Unavailable,
    InvalidResponse,
    ResponseTooLarge,
    BudgetExhausted,
}

impl fmt::Display for BackendErrorV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidConfiguration => "invalid_backend_configuration",
            Self::InvalidRequest => "invalid_backend_request",
            Self::PermissionDenied => "permission_denied",
            Self::RateLimited => "rate_limited",
            Self::RedirectRefused => "redirect_refused",
            Self::Timeout => "backend_timeout",
            Self::Unavailable => "backend_unavailable",
            Self::InvalidResponse => "invalid_backend_response",
            Self::ResponseTooLarge => "response_too_large",
            Self::BudgetExhausted => "backend_budget_exhausted",
        })
    }
}
impl std::error::Error for BackendErrorV1 {}

#[derive(Clone, Copy, Debug)]
pub struct LimitsV1 {
    pub attempts: u32,
    pub elapsed: Duration,
    pub request_timeout: Duration,
    pub response_bytes: usize,
    pub total_bytes: usize,
}
impl Default for LimitsV1 {
    fn default() -> Self {
        Self {
            attempts: 32,
            elapsed: Duration::from_secs(300),
            request_timeout: Duration::from_secs(30),
            response_bytes: MAX_RESPONSE_BYTES,
            total_bytes: MAX_TOTAL_BYTES,
        }
    }
}
impl LimitsV1 {
    fn validate(self) -> Result<Self, BackendErrorV1> {
        if self.attempts == 0
            || self.attempts > 32
            || self.elapsed.is_zero()
            || self.elapsed > Duration::from_secs(300)
            || self.request_timeout.is_zero()
            || self.request_timeout > self.elapsed
            || self.response_bytes == 0
            || self.response_bytes > MAX_RESPONSE_BYTES
            || self.total_bytes == 0
            || self.total_bytes > MAX_TOTAL_BYTES
        {
            return Err(BackendErrorV1::InvalidConfiguration);
        }
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UsageV1 {
    pub attempts: u32,
    /// Bytes delivered by the HTTP body reader, including a rejected chunk.
    /// Socket/TLS buffers and response headers are not body bytes.
    pub received_bytes: usize,
}

pub struct ResponseV1 {
    body: Vec<u8>,
}
impl ResponseV1 {
    pub fn body(&self) -> &[u8] {
        &self.body
    }
}
impl fmt::Debug for ResponseV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponseV1")
            .field("bytes", &self.body.len())
            .finish()
    }
}

pub struct HttpSessionV1 {
    client: Client,
    base: Url,
    authorization: header::HeaderValue,
    authorization_name: header::HeaderName,
    limits: LimitsV1,
    deadline: Instant,
    usage: UsageV1,
    exhausted: bool,
}
impl fmt::Debug for HttpSessionV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpSessionV1")
            .field("usage", &self.usage)
            .finish_non_exhaustive()
    }
}

/// Validate without making a connection. Reject normalizing traversal paths.
pub fn validate_base_url_v1(value: &str) -> Result<(), BackendErrorV1> {
    parse_base(value).map(|_| ())
}
fn parse_base(value: &str) -> Result<Url, BackendErrorV1> {
    let bad = BackendErrorV1::InvalidConfiguration;
    if value.len() > 2048
        || !value.starts_with("https://")
        || value
            .bytes()
            .any(|b| b <= 32 || b == 127 || b == b'\\' || b == b'%')
        || value.split('/').any(|s| s == "." || s == "..")
    {
        return Err(bad);
    }
    let mut url = Url::parse(value).map_err(|_| bad)?;
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(bad);
    }
    if !url.path().ends_with('/') {
        url.set_path(&format!("{}/", url.path()));
    }
    Ok(url)
}

impl HttpSessionV1 {
    pub fn new(
        base: &str,
        bearer: &str,
        ca_pem: Option<&[u8]>,
        limits: LimitsV1,
    ) -> Result<Self, BackendErrorV1> {
        let base = parse_base(base)?;
        let limits = limits.validate()?;
        let bad = BackendErrorV1::InvalidConfiguration;
        if bearer.is_empty() || bearer.len() > 4096 || !bearer.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(bad);
        }
        let mut authorization =
            header::HeaderValue::from_str(&format!("Bearer {bearer}")).map_err(|_| bad)?;
        authorization.set_sensitive(true);
        let mut builder = Client::builder()
            .https_only(true)
            .http1_only()
            .http1_max_headers(64)
            .tls_backend_native()
            .tls_version_min(reqwest::tls::Version::TLS_1_2)
            .redirect(reqwest::redirect::Policy::none())
            .referer(false)
            .retry(reqwest::retry::never())
            .no_proxy()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .pool_max_idle_per_host(0)
            .timeout(limits.request_timeout)
            .connect_timeout(limits.request_timeout);
        if let Some(pem) = ca_pem {
            if pem.len() > 64 * 1024 {
                return Err(bad);
            }
            builder =
                builder.tls_certs_merge([reqwest::Certificate::from_pem(pem).map_err(|_| bad)?]);
        }
        let client = builder.build().map_err(|_| bad)?;
        Ok(Self {
            client,
            base,
            authorization,
            authorization_name: header::AUTHORIZATION,
            limits,
            deadline: Instant::now() + limits.elapsed,
            usage: UsageV1::default(),
            exhausted: false,
        })
    }

    pub fn usage(&self) -> UsageV1 {
        self.usage
    }

    pub(crate) fn restrict_bytes(&mut self, bytes: usize) -> Result<(), BackendErrorV1> {
        if self.usage.attempts != 0 || bytes == 0 || bytes > self.limits.total_bytes {
            return Err(BackendErrorV1::InvalidConfiguration);
        }
        self.limits.total_bytes = bytes;
        self.limits.response_bytes = self.limits.response_bytes.min(bytes);
        Ok(())
    }

    pub(crate) fn model(
        base: &str,
        key: &str,
        ca: Option<&[u8]>,
        limits: LimitsV1,
    ) -> Result<Self, BackendErrorV1> {
        let mut session = Self::new(base, key, ca, limits)?;
        let mut value =
            header::HeaderValue::from_str(key).map_err(|_| BackendErrorV1::InvalidConfiguration)?;
        value.set_sensitive(true);
        session.authorization = value;
        session.authorization_name = header::HeaderName::from_static("x-api-key");
        Ok(session)
    }

    // Only the model adapter can submit this fixed operation. Not a generic
    // backend write API; origin selection is confined to its constructor.
    pub(crate) async fn messages(&mut self, body: Vec<u8>) -> Result<ResponseV1, BackendErrorV1> {
        if body.len() > 96 * 1024 {
            return Err(BackendErrorV1::InvalidRequest);
        }
        let url = self
            .base
            .join("v1/messages")
            .map_err(|_| BackendErrorV1::InvalidRequest)?;
        let request = self
            .client
            .post(url)
            .header("anthropic-version", "2023-06-01")
            .header(header::CONTENT_TYPE, "application/json")
            .body(body);
        self.execute(request).await
    }

    /// Paths are individual non-traversing segments, never an absolute URL.
    pub async fn get(
        &mut self,
        segments: &[&str],
        query: &[(&str, &str)],
    ) -> Result<ResponseV1, BackendErrorV1> {
        let bad = BackendErrorV1::InvalidRequest;
        if segments.is_empty()
            || segments.len() > 16
            || query.len() > 16
            || segments.iter().any(|s| {
                s.is_empty()
                    || s.len() > 128
                    || *s == "."
                    || *s == ".."
                    || !s
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            })
            || query.iter().any(|(k, v)| {
                k.is_empty()
                    || k.len() > 64
                    || v.len() > 4096
                    || !k.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                    || v.chars().any(char::is_control)
            })
        {
            return Err(bad);
        }
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|_| bad)?
            .pop_if_empty()
            .extend(segments);
        if !query.is_empty() {
            url.query_pairs_mut().extend_pairs(query.iter().copied());
        }
        if url.as_str().len() > 16 * 1024 {
            return Err(bad);
        }
        self.execute(self.client.get(url)).await
    }

    async fn execute(
        &mut self,
        request: reqwest::RequestBuilder,
    ) -> Result<ResponseV1, BackendErrorV1> {
        if self.exhausted
            || self.usage.attempts >= self.limits.attempts
            || self.usage.received_bytes >= self.limits.total_bytes
            || Instant::now() >= self.deadline
        {
            return Err(BackendErrorV1::BudgetExhausted);
        }
        // Charge before awaiting. Dropping this future cannot refund an attempt.
        self.usage.attempts += 1;
        let deadline = self
            .deadline
            .min(Instant::now() + self.limits.request_timeout);
        tokio::time::timeout_at(deadline, self.read(request))
            .await
            .unwrap_or(Err(BackendErrorV1::Timeout))
    }

    async fn read(
        &mut self,
        request: reqwest::RequestBuilder,
    ) -> Result<ResponseV1, BackendErrorV1> {
        let mut response = request
            .header(self.authorization_name.clone(), self.authorization.clone())
            .header(header::ACCEPT, "application/json")
            .header(header::ACCEPT_ENCODING, "identity")
            .send()
            .await
            .map_err(map_error)?;
        // The library bounds HTTP/1 header parsing; additionally reject >16 KiB
        // of decoded headers. No headers or response text enter diagnostics.
        if response
            .headers()
            .iter()
            .map(|(k, v)| k.as_str().len() + v.len())
            .sum::<usize>()
            > 16 * 1024
        {
            return Err(BackendErrorV1::InvalidResponse);
        }
        let status = response.status().as_u16();
        match status {
            200..=299 => (),
            300..=399 => return Err(BackendErrorV1::RedirectRefused),
            401 | 403 => return Err(BackendErrorV1::PermissionDenied),
            429 => return Err(BackendErrorV1::RateLimited),
            500..=599 => return Err(BackendErrorV1::Unavailable),
            _ => return Err(BackendErrorV1::InvalidResponse),
        }
        if response
            .headers()
            .get_all(header::CONTENT_ENCODING)
            .iter()
            .any(|value| value.as_bytes() != b"identity")
        {
            return Err(BackendErrorV1::InvalidResponse);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(map_error)? {
            // Account for all delivered bytes, even the chunk crossing a cap.
            // Never retain that chunk or poll another after a limit is crossed.
            self.usage.received_bytes = self.usage.received_bytes.saturating_add(chunk.len());
            if self.usage.received_bytes > self.limits.total_bytes {
                self.exhausted = true;
                return Err(BackendErrorV1::BudgetExhausted);
            }
            if chunk.len() > self.limits.response_bytes - body.len() {
                return Err(BackendErrorV1::ResponseTooLarge);
            }
            body.extend_from_slice(&chunk);
        }
        Ok(ResponseV1 { body })
    }
}
fn map_error(error: reqwest::Error) -> BackendErrorV1 {
    if error.is_timeout() {
        BackendErrorV1::Timeout
    } else if error.is_connect() {
        BackendErrorV1::Unavailable
    } else {
        BackendErrorV1::InvalidResponse
    }
}
