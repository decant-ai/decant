use super::*;
use decant_backend::transport::MAX_RESPONSE_BYTES;

const MAGIC: &[u8] = b"DECANT-INVESTIGATION-V1\n";
const MAX_MANIFEST: usize = 64 * 1024;
pub(super) const MAX_BUNDLE: usize = MAX_CAPTURE_BYTES_V1 + MAX_MANIFEST + 64;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    plan: String,
    key: String,
    attempted: bool,
    error: Option<BackendErrorV1>,
    length: usize,
    response_hash: Option<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: String,
    version: u8,
    config: InvestigationConfigV1,
    records: Vec<Record>,
    report_id: String,
}
pub(super) fn records(reads: &[ReadV1]) -> Result<Vec<Record>, E> {
    reads
        .iter()
        .map(|r| {
            Ok(Record {
                plan: r.plan_id().into(),
                key: r.key().into(),
                attempted: r.attempted(),
                error: r.response().err(),
                length: r.response().map_or(0, |b| b.len()),
                response_hash: r
                    .response()
                    .ok()
                    .map(|b| hash(Domain::Response, b))
                    .transpose()?,
            })
        })
        .collect()
}
/// Potentially sensitive raw captures. Not the shareable report or an archive.
pub struct ReplayBundleV1 {
    pub(super) bytes: Vec<u8>,
    id: String,
    report: InvestigationReportV1,
}
impl fmt::Debug for ReplayBundleV1 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReplayBundleV1")
            .field("bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}
impl ReplayBundleV1 {
    pub(super) fn encode(c: &CapturedInvestigationV1) -> Result<Self, E> {
        let report = c.report()?;
        let manifest = encode(&Manifest {
            format: "decant-investigation-bundle".into(),
            version: 1,
            config: c.config.clone(),
            records: records(&c.reads)?,
            report_id: report.id().into(),
        })?;
        if manifest.len() > MAX_MANIFEST {
            return Err(E::InvalidBundle);
        }
        let total = c
            .reads
            .iter()
            .filter_map(|r| r.response().ok())
            .try_fold(MAGIC.len() + 4 + manifest.len(), |n, b| {
                n.checked_add(b.len())
            })
            .ok_or(E::InvalidBundle)?;
        if total > MAX_BUNDLE {
            return Err(E::InvalidBundle);
        }
        let mut bytes = Vec::with_capacity(total);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&(manifest.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&manifest);
        for r in &c.reads {
            if let Ok(b) = r.response() {
                bytes.extend_from_slice(b);
            }
        }
        let id = hash(Domain::Bundle, &bytes)?;
        Ok(Self { bytes, id, report })
    }
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn report(&self) -> &InvestigationReportV1 {
        &self.report
    }
    /// The caller must supply the independently retained expected bundle ID.
    /// Verification does not authenticate the original backend or trust anchor.
    pub fn decode(bytes: &[u8], expected_id: &str) -> Result<Self, E> {
        if bytes.len() > MAX_BUNDLE || bytes.len() < MAGIC.len() + 4 || !bytes.starts_with(MAGIC) {
            return Err(E::InvalidBundle);
        }
        if hash(Domain::Bundle, bytes)? != expected_id {
            return Err(E::IdentityMismatch);
        }
        let length = u32::from_be_bytes(
            bytes[MAGIC.len()..MAGIC.len() + 4]
                .try_into()
                .map_err(|_| E::InvalidBundle)?,
        ) as usize;
        if length > MAX_MANIFEST {
            return Err(E::InvalidBundle);
        }
        let mut cursor = MAGIC.len() + 4;
        let header = bytes.get(cursor..cursor + length).ok_or(E::InvalidBundle)?;
        cursor += length;
        let manifest: Manifest = serde_json::from_slice(header).map_err(|_| E::InvalidBundle)?;
        if manifest.format != "decant-investigation-bundle"
            || manifest.version != 1
            || manifest.records.len() > 26
            || encode(&manifest)? != header
        {
            return Err(E::InvalidBundle);
        }
        let (m, t, l) = manifest.config.plans().map_err(|_| E::InvalidBundle)?;
        let mut reads = Vec::new();
        let mut total = 0usize;
        for r in manifest.records {
            total = total.checked_add(r.length).ok_or(E::InvalidBundle)?;
            if total > MAX_CAPTURE_BYTES_V1
                || r.length > MAX_RESPONSE_BYTES
                || r.key.len() > 64
                || r.plan.len() != 74
            {
                return Err(E::InvalidBundle);
            }
            let response = if let Some(error) = r.error {
                if r.length != 0 || r.response_hash.is_some() {
                    return Err(E::InvalidBundle);
                }
                Err(error)
            } else {
                let body = bytes
                    .get(cursor..cursor + r.length)
                    .ok_or(E::InvalidBundle)?;
                cursor += r.length;
                if Some(hash(Domain::Response, body)?) != r.response_hash {
                    return Err(E::IdentityMismatch);
                }
                Ok(body.to_vec())
            };
            let index = |prefix: &str| -> Result<usize, E> {
                r.key
                    .strip_prefix(prefix)
                    .and_then(|s| s.parse().ok())
                    .ok_or(E::InvalidBundle)
            };
            let read = if r.plan == m.id() {
                m.capture(index("metrics-")?, response)?.as_read()
            } else if let Some(t) = &t
                && r.plan == t.id()
            {
                if r.key.starts_with("search-") {
                    t.capture_search(index("search-")?, response)?
                } else {
                    t.capture_trace(
                        r.key.strip_prefix("trace-").ok_or(E::InvalidBundle)?,
                        response,
                    )?
                }
            } else if let Some(l) = &l
                && r.plan == l.id()
            {
                l.capture(index("loki-")?, response)?
            } else {
                return Err(E::IdentityMismatch);
            };
            if read.key() != r.key {
                return Err(E::InvalidBundle);
            }
            reads.push(
                read.with_attempted(r.attempted)
                    .map_err(|_| E::InvalidBundle)?,
            );
        }
        if cursor != bytes.len() {
            return Err(E::InvalidBundle);
        }
        let captured = CapturedInvestigationV1::from_reads(manifest.config, reads)
            .map_err(|_| E::InvalidBundle)?;
        let rebuilt = Self::encode(&captured)?;
        if rebuilt.report.id() != manifest.report_id || rebuilt.bytes != bytes {
            return Err(E::IdentityMismatch);
        }
        Ok(rebuilt)
    }
}
