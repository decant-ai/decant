//! Versioned, policy-filtered evidence. Markdown and MCP consume these same facts.

use std::io::{self, Write};

use decant_state::sre_evidence::{EvidenceErrorV1, IntervalV1, RecipeV1, reduce_sre_evidence_v1};
use decant_state::{AdmissionStateV0, RejectionReasonV0, hash_admission_state_v1};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const MAX_EVIDENCE_BYTES_V1: usize = 16 * 1024;

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EvidencePolicyV1 {
    pub allow_service_names: bool,
    pub allow_service_versions: bool,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EvidenceRequestV1 {
    pub version: u16,
    pub window_ns: u64,
    pub retention_ns: u64,
    pub max_services: usize,
    pub output_bytes: usize,
    pub policy: EvidencePolicyV1,
}

impl Default for EvidenceRequestV1 {
    fn default() -> Self {
        let recipe = RecipeV1::default();
        Self {
            version: 1,
            window_ns: recipe.window_ns,
            retention_ns: recipe.retention_ns,
            max_services: recipe.max_services,
            output_bytes: MAX_EVIDENCE_BYTES_V1,
            policy: EvidencePolicyV1 {
                allow_service_names: true,
                allow_service_versions: false,
            },
        }
    }
}

impl EvidenceRequestV1 {
    pub fn validate(&self) -> Result<(), EvidenceErrorV1> {
        self.recipe().validate()?;
        if self.version != 1 || !(2048..=MAX_EVIDENCE_BYTES_V1).contains(&self.output_bytes) {
            return Err(EvidenceErrorV1::Configuration);
        }
        if !self.policy.allow_service_names {
            return Err(EvidenceErrorV1::Policy);
        }
        Ok(())
    }
    fn recipe(&self) -> RecipeV1 {
        RecipeV1 {
            window_ns: self.window_ns,
            retention_ns: self.retention_ns,
            max_services: self.max_services,
        }
    }
}

// Never derive Debug on a type that contains source labels or provenance.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EvidenceDocumentV1 {
    pub format: String,
    pub version: u16,
    pub report_id: String,
    pub recipe: EvidenceRequestV1,
    pub source_cut: SourceCutV1,
    pub gaps: GapsV1,
    pub candidate_services: usize,
    pub omitted_services: usize,
    pub services: Vec<ServiceEvidenceV1>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct SourceCutV1 {
    pub input: EvidenceInputV1,
    pub state_hash: String,
    pub source_id: String,
    pub source_finalized: bool,
    pub anchor_ns: Option<i64>,
    pub baseline_start_ns: Option<i64>,
    pub current_start_ns: Option<i64>,
    pub retention_cutoff_ns: Option<i64>,
    pub accepted_retained_entries: usize,
}

#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct EvidenceInputV1 {
    pub capture: Option<CaptureAccountingV1>,
    pub source_progress_hash: Option<String>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CaptureAccountingV1 {
    pub records: u64,
    pub bytes: u64,
    pub decoded_spans: u64,
    // Resource/scope/span attributes, span events/links, status messages.
    pub omitted: [u64; 6],
    // Resource/scope/span attributes, span events/links, as reported upstream.
    pub upstream_dropped: [u64; 5],
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct GapsV1 {
    pub sampling: String,
    pub upstream_completeness: String,
    pub history_coverage: String,
    pub raw_fields_emitted: bool,
    pub version_labels_emitted: bool,
    pub expired_entries_excluded: usize,
    pub logical_duplicates_folded: usize,
    pub non_server_spans_excluded: usize,
    pub missing_service_spans: usize,
    pub outside_window_spans: usize,
    pub rejections: [u64; 8],
    pub rejection_counts_saturated: bool,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ServiceEvidenceV1 {
    pub service: String,
    pub baseline: IntervalEvidenceV1,
    pub current: IntervalEvidenceV1,
    pub observed_version_differs: Option<bool>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IntervalEvidenceV1 {
    pub observed_server_spans: u64,
    pub error_spans: u64,
    pub error_fraction: Option<FractionV1>,
    pub unset_status_spans: u64,
    pub p95_duration_ns: Option<u64>,
    pub duration_sum_ns: String,
    pub missing_version_spans: u64,
    pub first_observed_version: Option<String>,
    pub last_observed_version: Option<String>,
    pub observed_version_transitions: Option<u64>,
    pub selected_references: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FractionV1 {
    pub numerator: u64,
    pub denominator: u64,
}

fn interval(input: IntervalV1<'_>, versions: bool) -> IntervalEvidenceV1 {
    IntervalEvidenceV1 {
        observed_server_spans: input.count,
        error_spans: input.errors,
        error_fraction: (input.count > 0).then_some(FractionV1 {
            numerator: input.errors,
            denominator: input.count,
        }),
        unset_status_spans: input.unset_status,
        p95_duration_ns: input.p95_ns,
        duration_sum_ns: input.duration_sum_ns.to_string(),
        missing_version_spans: input.missing_versions,
        first_observed_version: versions
            .then_some(input.first_version)
            .flatten()
            .map(str::to_owned),
        last_observed_version: versions
            .then_some(input.last_version)
            .flatten()
            .map(str::to_owned),
        observed_version_transitions: versions.then_some(input.version_transitions),
        selected_references: input.evidence.into_iter().map(str::to_owned).collect(),
    }
}

pub fn compile_evidence_v1(
    state: &AdmissionStateV0,
    request: &EvidenceRequestV1,
) -> Result<EvidenceDocumentV1, EvidenceErrorV1> {
    compile_evidence_with_input_v1(state, request, EvidenceInputV1::default())
}

pub fn compile_evidence_with_input_v1(
    state: &AdmissionStateV0,
    request: &EvidenceRequestV1,
    input: EvidenceInputV1,
) -> Result<EvidenceDocumentV1, EvidenceErrorV1> {
    request.validate()?;
    if !valid_input(&input) {
        return Err(EvidenceErrorV1::Configuration);
    }
    let reduction = reduce_sre_evidence_v1(state, request.recipe())?;
    let middle = reduction
        .anchor_ns
        .map(|time| time.saturating_sub(request.window_ns as i64));
    let start = middle.map(|time| time.saturating_sub(request.window_ns as i64));
    let mut report = EvidenceDocumentV1 {
        format: "decant-evidence".into(),
        version: 1,
        report_id: String::new(),
        recipe: request.clone(),
        source_cut: SourceCutV1 {
            input,
            state_hash: hash_admission_state_v1(state)
                .map_err(|_| EvidenceErrorV1::Encoding)?
                .to_string(),
            // Opaque source identity, not a path/broker endpoint.
            source_id: digest(
                b"decant:evidence-source:v1\0",
                state.config().source_id().as_str().as_bytes(),
            ),
            source_finalized: state.is_finalized(),
            anchor_ns: reduction.anchor_ns,
            baseline_start_ns: start,
            current_start_ns: middle,
            retention_cutoff_ns: reduction.retention_cutoff_ns,
            accepted_retained_entries: state.usage().accepted_identities(),
        },
        gaps: GapsV1 {
            sampling: "unknown".into(),
            upstream_completeness: "unknown".into(),
            history_coverage: if reduction.anchor_ns.is_none() {
                "unavailable"
            } else if request.retention_ns < 2 * request.window_ns
                || start.is_some_and(|start| start < state.config().epoch_start_ns())
            {
                "partial"
            } else {
                "not_established"
            }
            .into(),
            raw_fields_emitted: false,
            version_labels_emitted: request.policy.allow_service_versions,
            expired_entries_excluded: reduction.expired,
            logical_duplicates_folded: reduction.duplicates,
            non_server_spans_excluded: reduction.non_server,
            missing_service_spans: reduction.missing_service,
            outside_window_spans: reduction.outside_windows,
            rejections: RejectionReasonV0::ALL
                .map(|reason| state.rejection_summary().range(reason).count()),
            rejection_counts_saturated: RejectionReasonV0::ALL
                .iter()
                .any(|reason| state.rejection_summary().range(*reason).count_saturated()),
        },
        candidate_services: reduction.services.len(),
        omitted_services: reduction.services.len(),
        services: Vec::new(),
    };
    // Reserve a fixed-size digest during packing. Candidate serialization is capped
    // before allocation; exclude a suffix only, never cherry-pick flattering rows.
    report.report_id = "0".repeat(64);
    for service in reduction.services {
        let versions = request.policy.allow_service_versions;
        let differs = if versions {
            service
                .baseline
                .last_version
                .zip(service.current.last_version)
                .map(|(a, b)| a != b)
        } else {
            None
        };
        report.services.push(ServiceEvidenceV1 {
            service: service.name.to_owned(),
            baseline: interval(service.baseline, versions),
            current: interval(service.current, versions),
            observed_version_differs: differs,
        });
        report.omitted_services -= 1;
        if bounded_json(&report, request.output_bytes).is_err() {
            report.services.pop();
            report.omitted_services += 1;
            break;
        }
    }
    report.report_id.clear();
    report.report_id = digest(
        b"decant:evidence:v1\0",
        &bounded_json(&report, request.output_bytes)?,
    );
    bounded_json(&report, request.output_bytes)?;
    Ok(report)
}

impl EvidenceDocumentV1 {
    pub fn to_bytes(&self) -> Result<Vec<u8>, EvidenceErrorV1> {
        bounded_json(self, self.recipe.output_bytes)
    }

    /// Check canonical typed bytes and integrity, not source authenticity or truth.
    pub fn from_verified_bytes(bytes: &[u8]) -> Result<Self, EvidenceErrorV1> {
        if bytes.len() > MAX_EVIDENCE_BYTES_V1 {
            return Err(EvidenceErrorV1::Capacity);
        }
        let mut report: Self =
            serde_json::from_slice(bytes).map_err(|_| EvidenceErrorV1::Verification)?;
        report.recipe.validate()?;
        if report.format != "decant-evidence"
            || report.version != 1
            || report.services.len() > report.recipe.max_services
            || report.candidate_services > report.recipe.max_services
            || report.services.len().checked_add(report.omitted_services)
                != Some(report.candidate_services)
            || report.gaps.raw_fields_emitted
            || report.gaps.sampling != "unknown"
            || report.gaps.upstream_completeness != "unknown"
            || !["unavailable", "partial", "not_established"]
                .contains(&report.gaps.history_coverage.as_str())
            || !valid_input(&report.source_cut.input)
            || !hex_digest(&report.source_cut.source_id)
            || !report
                .source_cut
                .state_hash
                .strip_prefix("sha256:v1:")
                .is_some_and(hex_digest)
            || report.source_cut.accepted_retained_entries
                > decant_state::MAX_ACCEPTED_IDENTITIES_V0
            || report.source_cut.current_start_ns
                != report
                    .source_cut
                    .anchor_ns
                    .map(|time| time.saturating_sub(report.recipe.window_ns as i64))
            || report.source_cut.baseline_start_ns
                != report
                    .source_cut
                    .current_start_ns
                    .map(|time| time.saturating_sub(report.recipe.window_ns as i64))
            || report.source_cut.retention_cutoff_ns
                != report
                    .source_cut
                    .anchor_ns
                    .map(|time| time.saturating_sub(report.recipe.retention_ns as i64))
            || report.gaps.version_labels_emitted != report.recipe.policy.allow_service_versions
            || report
                .services
                .windows(2)
                .any(|rows| rows[0].service >= rows[1].service)
            || report
                .services
                .iter()
                .any(|row| !valid_service(row, report.recipe.policy.allow_service_versions))
            || report.to_bytes()? != bytes
        {
            return Err(EvidenceErrorV1::Verification);
        }
        let id = std::mem::take(&mut report.report_id);
        let expected = digest(b"decant:evidence:v1\0", &report.to_bytes()?);
        if id != expected {
            return Err(EvidenceErrorV1::Verification);
        }
        report.report_id = id;
        Ok(report)
    }
}

fn hex_digest(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_input(input: &EvidenceInputV1) -> bool {
    !(input.capture.is_some() && input.source_progress_hash.is_some())
        && input.source_progress_hash.as_ref().is_none_or(|hash| {
            hash.strip_prefix("sha256:v0:")
                .or_else(|| hash.strip_prefix("sha256:v1:"))
                .is_some_and(hex_digest)
        })
        && input.capture.as_ref().is_none_or(|capture| {
            capture.records <= 100_000
                && capture.bytes <= 256 * 1024 * 1024
                && capture.decoded_spans <= capture.records * 4096
        })
}

fn valid_service(row: &ServiceEvidenceV1, versions: bool) -> bool {
    if row.service.is_empty()
        || row.service.len() > 256
        || row.observed_version_differs
            != (if versions {
                row.baseline
                    .last_observed_version
                    .as_ref()
                    .zip(row.current.last_observed_version.as_ref())
                    .map(|(a, b)| a != b)
            } else {
                None
            })
    {
        return false;
    }
    [&row.baseline, &row.current].into_iter().all(|interval| {
        let count = interval.observed_server_spans;
        count <= decant_state::MAX_ACCEPTED_IDENTITIES_V0 as u64
            && interval.error_spans <= count
            && interval.unset_status_spans <= count
            && interval.missing_version_spans <= count
            && interval.error_spans + interval.unset_status_spans <= count
            && interval
                .duration_sum_ns
                .parse::<u128>()
                .is_ok_and(|sum| sum <= u128::from(count) * u128::from(u64::MAX))
            && interval.p95_duration_ns.is_some() == (count > 0)
            && interval.error_fraction
                == (count > 0).then_some(FractionV1 {
                    numerator: interval.error_spans,
                    denominator: count,
                })
            && interval.selected_references.len() <= 3
            && interval.selected_references.iter().all(|reference| {
                reference.len() <= 512
                    && reference.starts_with("decant://event/v0/")
                    && reference.bytes().all(|byte| byte.is_ascii_graphic())
            })
            && [
                &interval.first_observed_version,
                &interval.last_observed_version,
            ]
            .into_iter()
            .all(|value| {
                value
                    .as_ref()
                    .is_none_or(|label| versions && !label.is_empty() && label.len() <= 256)
            })
            && (versions || interval.observed_version_transitions.is_none())
            && interval
                .observed_version_transitions
                .is_none_or(|counted| counted <= count.saturating_sub(1))
    })
}

pub fn digest(domain: &[u8], bytes: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(domain);
    hash.update(bytes);
    hash.finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub fn bounded_json(value: &impl Serialize, maximum: usize) -> Result<Vec<u8>, EvidenceErrorV1> {
    struct Writer {
        bytes: Vec<u8>,
        maximum: usize,
    }
    impl Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.maximum.saturating_sub(self.bytes.len()) {
                return Err(io::ErrorKind::OutOfMemory.into());
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        bytes: Vec::new(),
        maximum,
    };
    writer
        .bytes
        .try_reserve_exact(maximum)
        .map_err(|_| EvidenceErrorV1::Capacity)?;
    serde_json::to_writer(&mut writer, value).map_err(|_| EvidenceErrorV1::Capacity)?;
    Ok(writer.bytes)
}
