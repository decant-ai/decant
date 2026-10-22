use std::collections::BTreeSet;
use std::error::Error;
use std::fmt;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::MAX_FIXTURE_DOCUMENT_BYTES;

const MANIFEST_VERSION_V0: u16 = 0;
const MAX_SCENARIOS: usize = 8;
const MAX_QUERIES_PER_SCENARIO: usize = 16;
const MAX_FACTS_PER_QUERY: usize = 64;
const MAX_SUPPORT_RULES_PER_FACT: usize = 16;
const MAX_DEPLOYMENTS: usize = 64;
const MAX_IDENTIFIER_BYTES: usize = 64;
const MAX_MIXER_VECTORS: usize = 64;
const MAX_REFERENCE_VECTORS: usize = 32;
const MAX_PERCENTILE_VALUES: usize = 256;
const MAX_PROVENANCE_BYTES: usize = 256;
const REQUIRED_BASELINES: usize = 3;

const PRIMARY_SCENARIO_ID: &str = "sre-v0-primary";
const SRE_START_NS: i64 = 1_767_225_600_000_000_000;
const SRE_END_NS: i64 = 1_767_226_800_000_000_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractBoundV0 {
    Scenarios,
    Queries,
    Facts,
    SupportRules,
    Deployments,
    Baselines,
    MixerVectors,
    ReferenceVectors,
    PercentileValues,
    IdentifierBytes,
    ProvenanceBytes,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContractKindV0 {
    Scenario,
    Oracle,
    Baseline,
    Scoring,
    Bundle,
}

#[derive(Clone, Eq, PartialEq)]
pub enum ContractErrorV0 {
    DocumentTooLarge {
        maximum: usize,
        actual: usize,
    },
    MissingTerminalLineFeed,
    MalformedJson {
        kind: ContractKindV0,
    },
    UnsupportedManifestFormat {
        kind: ContractKindV0,
    },
    UnsupportedManifestVersion {
        kind: ContractKindV0,
        actual: u16,
    },
    NonCanonicalEncoding {
        kind: ContractKindV0,
    },
    BoundExceeded {
        bound: ContractBoundV0,
        maximum: usize,
        actual: usize,
    },
    InvalidIdentifier,
    InvalidContract {
        kind: ContractKindV0,
    },
    ArithmeticMismatch,
    ReferenceVectorMismatch,
    CrossManifestMismatch,
    EncodingFailure,
}

impl fmt::Debug for ContractErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DocumentTooLarge { maximum, actual } => formatter
                .debug_struct("DocumentTooLarge")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::MissingTerminalLineFeed => formatter.write_str("MissingTerminalLineFeed"),
            Self::MalformedJson { kind } => formatter
                .debug_struct("MalformedJson")
                .field("kind", kind)
                .finish(),
            Self::UnsupportedManifestFormat { kind } => formatter
                .debug_struct("UnsupportedManifestFormat")
                .field("kind", kind)
                .finish(),
            Self::UnsupportedManifestVersion { kind, actual } => formatter
                .debug_struct("UnsupportedManifestVersion")
                .field("kind", kind)
                .field("actual", actual)
                .finish(),
            Self::NonCanonicalEncoding { kind } => formatter
                .debug_struct("NonCanonicalEncoding")
                .field("kind", kind)
                .finish(),
            Self::BoundExceeded {
                bound,
                maximum,
                actual,
            } => formatter
                .debug_struct("BoundExceeded")
                .field("bound", bound)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::InvalidIdentifier => formatter.write_str("InvalidIdentifier"),
            Self::InvalidContract { kind } => formatter
                .debug_struct("InvalidContract")
                .field("kind", kind)
                .finish(),
            Self::ArithmeticMismatch => formatter.write_str("ArithmeticMismatch"),
            Self::ReferenceVectorMismatch => formatter.write_str("ReferenceVectorMismatch"),
            Self::CrossManifestMismatch => formatter.write_str("CrossManifestMismatch"),
            Self::EncodingFailure => formatter.write_str("EncodingFailure"),
        }
    }
}

impl fmt::Display for ContractErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for ContractErrorV0 {}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioManifestV0 {
    manifest_format: String,
    manifest_version: u16,
    scenarios: Vec<ScenarioV0>,
    selection_domains: SelectionDomainsV0,
    mixer_vectors: Vec<MixerVectorV0>,
    rate_vectors: Vec<RateVectorV0>,
    percentile_vectors: Vec<PercentileVectorV0>,
}

impl ScenarioManifestV0 {
    pub fn scenario_count(&self) -> usize {
        self.scenarios.len()
    }

    pub fn mixer_vectors(&self) -> &[MixerVectorV0] {
        &self.mixer_vectors
    }

    pub fn primary_counts(&self) -> Option<PrimaryCountsV0> {
        self.scenarios.iter().find_map(|scenario| match scenario {
            ScenarioV0::Primary { counts, .. } => Some(*counts),
            ScenarioV0::Holdout { .. } => None,
        })
    }

    pub fn holdout_count(&self) -> usize {
        self.scenarios
            .iter()
            .filter(|scenario| matches!(scenario, ScenarioV0::Holdout { .. }))
            .count()
    }

    pub fn primary_query_count(&self) -> usize {
        self.scenarios
            .iter()
            .find_map(|scenario| match scenario {
                ScenarioV0::Primary { queries, .. } => Some(queries.len()),
                ScenarioV0::Holdout { .. } => None,
            })
            .unwrap_or(0)
    }

    pub(crate) fn generator_spec(&self, scenario_id: &str) -> Option<GeneratorSpecV0> {
        let primary = self.scenarios.iter().find_map(|scenario| match scenario {
            ScenarioV0::Primary {
                seed,
                source_id,
                interval_start_ns,
                interval_end_ns,
                counts,
                timing,
                rates,
                latency,
                errors,
                deployments,
                ..
            } => Some((
                *seed,
                source_id,
                *interval_start_ns,
                *interval_end_ns,
                *counts,
                *timing,
                *rates,
                *latency,
                *errors,
                deployments,
            )),
            ScenarioV0::Holdout { .. } => None,
        })?;

        let (seed, transform) = self.scenarios.iter().find_map(|scenario| match scenario {
            ScenarioV0::Primary {
                scenario_id: candidate,
                seed,
                ..
            } if candidate == scenario_id => Some((*seed, GeneratorTransformV0::Primary)),
            ScenarioV0::Holdout {
                scenario_id: candidate,
                seed,
                transform,
                ..
            } if candidate == scenario_id => Some((
                *seed,
                match transform {
                    HoldoutTransformV0::AffectedRegionSwap => {
                        GeneratorTransformV0::AffectedRegionSwap
                    }
                    HoldoutTransformV0::AffectedServiceDeploymentSwap => {
                        GeneratorTransformV0::AffectedServiceDeploymentSwap
                    }
                    HoldoutTransformV0::NoRegression => GeneratorTransformV0::NoRegression,
                },
            )),
            _ => None,
        })?;

        let (
            _,
            source_id,
            interval_start_ns,
            interval_end_ns,
            counts,
            timing,
            rates,
            latency,
            errors,
            deployments,
        ) = primary;
        Some(GeneratorSpecV0 {
            seed,
            source_id: source_id.clone(),
            interval_start_ns,
            interval_end_ns,
            counts: GeneratorCountsV0 {
                payment_outcomes: counts.payment_outcomes,
                accepted_distractors: counts.accepted_distractors,
                late_distractors: counts.late_distractors,
                metric_events: counts.metric_events,
                deployment_events: counts.deployment_events,
                exact_duplicate_deliveries: counts.exact_duplicate_deliveries,
                data_deliveries: counts.data_deliveries,
                accepted_unique_events: counts.accepted_unique_events,
                delayed_accepted_deliveries: counts.delayed_accepted_deliveries,
                terminal_controls: counts.terminal_controls,
            },
            timing: GeneratorTimingV0 {
                late_event_time_before_ns: timing.late_event_time_before_ns,
                late_delivery_after_ns: timing.late_delivery_after_ns,
                delayed_min_ns: timing.delayed_min_ns,
                delayed_max_ns: timing.delayed_max_ns,
            },
            rates: GeneratorRatesV0 {
                requests_per_second_per_region: rates.requests_per_second_per_region,
                pre_region_a_ppm: rates.pre_region_a_ppm,
                pre_region_b_ppm: rates.pre_region_b_ppm,
                post_region_a_ppm: rates.post_region_a_ppm,
                post_region_b_ppm: rates.post_region_b_ppm,
                post_global_ppm: rates.post_global_ppm,
            },
            latency: GeneratorLatencyV0 {
                pre_region_a_p99_ms: latency.pre_region_a_p99_ms,
                pre_region_b_p99_ms: latency.pre_region_b_p99_ms,
                post_region_a_p99_ms: latency.post_region_a_p99_ms,
                post_region_b_p99_ms: latency.post_region_b_p99_ms,
                percentile_basis_points: latency.percentile_basis_points,
            },
            errors: GeneratorErrorPatternV0 {
                base_internal_error_numerator: errors.base_internal_error_numerator,
                base_denominator: errors.base_denominator,
                post_region_b_upstream_timeout_numerator: errors
                    .post_region_b_upstream_timeout_numerator,
                post_region_b_denominator: errors.post_region_b_denominator,
            },
            deployments: deployments
                .iter()
                .map(|deployment| GeneratorDeploymentV0 {
                    deployment_id: deployment.deployment_id.clone(),
                    service_id: deployment.service_id.clone(),
                    region_id: deployment.region_id.clone(),
                    event_time_ns: deployment.event_time_ns,
                    notable_temporal_evidence: deployment.notable_temporal_evidence,
                    causal_claim: deployment.causal_claim,
                })
                .collect(),
            duplicate_domain: self.selection_domains.duplicate,
            reorder_domain: self.selection_domains.reorder,
            transform,
        })
    }
}

#[derive(Clone)]
pub(crate) struct GeneratorSpecV0 {
    pub(crate) seed: u64,
    pub(crate) source_id: String,
    pub(crate) interval_start_ns: i64,
    pub(crate) interval_end_ns: i64,
    pub(crate) counts: GeneratorCountsV0,
    pub(crate) timing: GeneratorTimingV0,
    pub(crate) rates: GeneratorRatesV0,
    pub(crate) latency: GeneratorLatencyV0,
    pub(crate) errors: GeneratorErrorPatternV0,
    pub(crate) deployments: Vec<GeneratorDeploymentV0>,
    pub(crate) duplicate_domain: u64,
    pub(crate) reorder_domain: u64,
    pub(crate) transform: GeneratorTransformV0,
}

#[derive(Clone, Copy)]
pub(crate) struct GeneratorCountsV0 {
    pub(crate) payment_outcomes: u64,
    pub(crate) accepted_distractors: u64,
    pub(crate) late_distractors: u64,
    pub(crate) metric_events: u64,
    pub(crate) deployment_events: u64,
    pub(crate) exact_duplicate_deliveries: u64,
    pub(crate) data_deliveries: u64,
    pub(crate) accepted_unique_events: u64,
    pub(crate) delayed_accepted_deliveries: u64,
    pub(crate) terminal_controls: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct GeneratorTimingV0 {
    pub(crate) late_event_time_before_ns: i64,
    pub(crate) late_delivery_after_ns: i64,
    pub(crate) delayed_min_ns: i64,
    pub(crate) delayed_max_ns: i64,
}

#[derive(Clone, Copy)]
pub(crate) struct GeneratorRatesV0 {
    pub(crate) requests_per_second_per_region: u64,
    pub(crate) pre_region_a_ppm: u64,
    pub(crate) pre_region_b_ppm: u64,
    pub(crate) post_region_a_ppm: u64,
    pub(crate) post_region_b_ppm: u64,
    pub(crate) post_global_ppm: u64,
}

#[derive(Clone, Copy)]
pub(crate) struct GeneratorLatencyV0 {
    pub(crate) pre_region_a_p99_ms: u64,
    pub(crate) pre_region_b_p99_ms: u64,
    pub(crate) post_region_a_p99_ms: u64,
    pub(crate) post_region_b_p99_ms: u64,
    pub(crate) percentile_basis_points: u16,
}

#[derive(Clone, Copy)]
pub(crate) struct GeneratorErrorPatternV0 {
    pub(crate) base_internal_error_numerator: u64,
    pub(crate) base_denominator: u64,
    pub(crate) post_region_b_upstream_timeout_numerator: u64,
    pub(crate) post_region_b_denominator: u64,
}

#[derive(Clone)]
pub(crate) struct GeneratorDeploymentV0 {
    pub(crate) deployment_id: String,
    pub(crate) service_id: String,
    pub(crate) region_id: String,
    pub(crate) event_time_ns: i64,
    pub(crate) notable_temporal_evidence: bool,
    pub(crate) causal_claim: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GeneratorTransformV0 {
    Primary,
    AffectedRegionSwap,
    AffectedServiceDeploymentSwap,
    NoRegression,
}

impl fmt::Debug for ScenarioManifestV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScenarioManifestV0")
            .field("scenario_count", &self.scenarios.len())
            .field("mixer_vector_count", &self.mixer_vectors.len())
            .field("rate_vector_count", &self.rate_vectors.len())
            .field("percentile_vector_count", &self.percentile_vectors.len())
            .finish()
    }
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "scenario_kind")]
// At most eight scenarios are decoded. Keeping the variants inline avoids
// heap indirection for a private, strictly bounded manifest.
#[allow(clippy::large_enum_variant)]
enum ScenarioV0 {
    #[serde(rename = "primary")]
    Primary {
        scenario_id: String,
        seed: u64,
        source_id: String,
        interval_start_ns: i64,
        interval_end_ns: i64,
        counts: PrimaryCountsV0,
        timing: TimingV0,
        rates: RatesV0,
        latency: LatencyV0,
        errors: ErrorPatternV0,
        deployments: Vec<DeploymentV0>,
        queries: Vec<QueryV0>,
    },
    #[serde(rename = "holdout")]
    Holdout {
        scenario_id: String,
        base_scenario_id: String,
        seed: u64,
        transform: HoldoutTransformV0,
        queries: Vec<QueryV0>,
    },
}

impl ScenarioV0 {
    fn id(&self) -> &str {
        match self {
            Self::Primary { scenario_id, .. } | Self::Holdout { scenario_id, .. } => scenario_id,
        }
    }

    fn queries(&self) -> &[QueryV0] {
        match self {
            Self::Primary { queries, .. } | Self::Holdout { queries, .. } => queries,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrimaryCountsV0 {
    payment_outcomes: u64,
    accepted_distractors: u64,
    late_distractors: u64,
    metric_events: u64,
    deployment_events: u64,
    exact_duplicate_deliveries: u64,
    data_deliveries: u64,
    accepted_unique_events: u64,
    delayed_accepted_deliveries: u64,
    terminal_controls: u64,
}

impl PrimaryCountsV0 {
    pub fn unique_events(self) -> u64 {
        self.payment_outcomes
            + self.accepted_distractors
            + self.late_distractors
            + self.metric_events
            + self.deployment_events
    }

    pub fn data_deliveries(self) -> u64 {
        self.data_deliveries
    }

    pub fn exact_duplicate_deliveries(self) -> u64 {
        self.exact_duplicate_deliveries
    }

    pub fn accepted_unique_events(self) -> u64 {
        self.accepted_unique_events
    }

    pub fn terminal_controls(self) -> u64 {
        self.terminal_controls
    }
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TimingV0 {
    late_event_time_before_ns: i64,
    late_delivery_after_ns: i64,
    delayed_min_ns: i64,
    delayed_max_ns: i64,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RatesV0 {
    requests_per_second_per_region: u64,
    pre_region_a_ppm: u64,
    pre_region_b_ppm: u64,
    post_region_a_ppm: u64,
    post_region_b_ppm: u64,
    post_global_ppm: u64,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct LatencyV0 {
    pre_region_a_p99_ms: u64,
    pre_region_b_p99_ms: u64,
    post_region_a_p99_ms: u64,
    post_region_b_p99_ms: u64,
    percentile_basis_points: u16,
    method: PercentileMethodV0,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum PercentileMethodV0 {
    NearestRank,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ErrorPatternV0 {
    base_internal_error_numerator: u64,
    base_denominator: u64,
    post_region_b_upstream_timeout_numerator: u64,
    post_region_b_denominator: u64,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct DeploymentV0 {
    deployment_id: String,
    service_id: String,
    region_id: String,
    event_time_ns: i64,
    notable_temporal_evidence: bool,
    causal_claim: bool,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct QueryV0 {
    query_id: String,
    as_of_ns: i64,
    window_ns: i64,
    evidence_reachback_ns: i64,
    byte_budget: usize,
    max_raw_examples: usize,
    service_id: String,
    region_id: String,
    expected_regression: bool,
    allowed_event_kinds: Vec<EventKindV0>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum HoldoutTransformV0 {
    AffectedRegionSwap,
    AffectedServiceDeploymentSwap,
    NoRegression,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MixerVectorV0 {
    seed: u64,
    domain_constant: u64,
    source_sequence: u64,
    expected_score: u64,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SelectionDomainsV0 {
    duplicate: u64,
    reorder: u64,
    example: u64,
    holdout: u64,
    ordering: SelectionOrderV0,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum SelectionOrderV0 {
    ScoreThenSourceSequence,
}

impl MixerVectorV0 {
    pub fn seed(self) -> u64 {
        self.seed
    }

    pub fn domain_constant(self) -> u64 {
        self.domain_constant
    }

    pub fn source_sequence(self) -> u64 {
        self.source_sequence
    }

    pub fn expected_score(self) -> u64 {
        self.expected_score
    }
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct RateVectorV0 {
    numerator: u64,
    denominator: u64,
    expected_ppm: u64,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct PercentileVectorV0 {
    values: Vec<u64>,
    percentile_basis_points: u16,
    expected: u64,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OracleManifestV0 {
    manifest_format: String,
    manifest_version: u16,
    scenarios: Vec<ScenarioOracleV0>,
    support_vectors: Vec<SupportVectorV0>,
}

impl OracleManifestV0 {
    pub fn scenario_count(&self) -> usize {
        self.scenarios.len()
    }

    pub fn support_vectors(
        &self,
    ) -> impl Iterator<Item = (&SupportPredicateV0, &ProvenanceCandidateV0, bool)> {
        self.support_vectors.iter().map(|vector| {
            (
                &vector.predicate,
                &vector.candidate,
                vector.expected_allowed,
            )
        })
    }
}

impl fmt::Debug for OracleManifestV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let query_count: usize = self
            .scenarios
            .iter()
            .map(|scenario| scenario.queries.len())
            .sum();
        formatter
            .debug_struct("OracleManifestV0")
            .field("scenario_count", &self.scenarios.len())
            .field("query_count", &query_count)
            .field("support_vector_count", &self.support_vectors.len())
            .finish()
    }
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct ScenarioOracleV0 {
    scenario_id: String,
    queries: Vec<QueryOracleV0>,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct QueryOracleV0 {
    query_id: String,
    answer_facts: Vec<AnswerFactV0>,
    system_assertions: SystemAssertionsV0,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AnswerFactV0 {
    fact_id: String,
    metric: FactMetricV0,
    expected_integer: i64,
    support_rules: Vec<SupportPredicateV0>,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum FactMetricV0 {
    ErrorRatePpm,
    P99LatencyMs,
    DeploymentMarkerCount,
    OutcomeCount,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SystemAssertionsV0 {
    accepted_unique_events: u64,
    late_events: u64,
    exact_duplicate_deliveries: u64,
    terminal_controls: u64,
    complete_input_expected: bool,
    maximum_output_bytes: usize,
    replay_hashes_equal: bool,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SupportPredicateV0 {
    service_id: String,
    region_id: String,
    event_kind: EventKindV0,
    error_code: Option<ErrorCodeV0>,
    require_event_provenance: bool,
}

impl SupportPredicateV0 {
    pub fn allows(&self, candidate: &ProvenanceCandidateV0) -> bool {
        self.service_id == candidate.service_id
            && self.region_id == candidate.region_id
            && self.event_kind == candidate.event_kind
            && self.error_code == candidate.error_code
            && (!self.require_event_provenance
                || candidate.provenance.starts_with("decant://event/v0/"))
    }
}

impl fmt::Debug for SupportPredicateV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SupportPredicateV0")
            .field("event_kind", &self.event_kind)
            .field("has_error_code", &self.error_code.is_some())
            .field("require_event_provenance", &self.require_event_provenance)
            .finish()
    }
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProvenanceCandidateV0 {
    service_id: String,
    region_id: String,
    event_kind: EventKindV0,
    error_code: Option<ErrorCodeV0>,
    provenance: String,
}

impl fmt::Debug for ProvenanceCandidateV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProvenanceCandidateV0")
            .field("event_kind", &self.event_kind)
            .field("has_error_code", &self.error_code.is_some())
            .field("provenance_byte_len", &self.provenance.len())
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum EventKindV0 {
    PaymentOutcome,
    Metric,
    Deployment,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ErrorCodeV0 {
    InternalError,
    UpstreamTimeout,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct SupportVectorV0 {
    predicate: SupportPredicateV0,
    candidate: ProvenanceCandidateV0,
    expected_allowed: bool,
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineManifestV0 {
    manifest_format: String,
    manifest_version: u16,
    baselines: Vec<BaselineV0>,
    rag_enabled: bool,
}

impl BaselineManifestV0 {
    pub fn enabled_count(&self) -> usize {
        self.baselines
            .iter()
            .filter(|baseline| baseline.enabled)
            .count()
    }
}

impl fmt::Debug for BaselineManifestV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BaselineManifestV0")
            .field("baseline_count", &self.baselines.len())
            .field("enabled_count", &self.enabled_count())
            .field("rag_enabled", &self.rag_enabled)
            .finish()
    }
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct BaselineV0 {
    baseline_id: String,
    kind: BaselineKindV0,
    semantics: BaselineSemanticsV0,
    enabled: bool,
    request_input: bool,
    records_input: bool,
    budget_input: bool,
    config_input: bool,
    oracle_input: bool,
    scoring_input: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
enum BaselineKindV0 {
    LastNCanonical,
    ScopedEvidenceFilter,
    HandBuiltAggregation,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields, tag = "semantics_kind")]
enum BaselineSemanticsV0 {
    #[serde(rename = "last_n_canonical")]
    LastNCanonical {
        newest_first: bool,
        whole_record_packing: bool,
    },
    #[serde(rename = "scoped_evidence_filter")]
    ScopedEvidenceFilter {
        query_window: bool,
        evidence_reachback: bool,
        same_scope: bool,
        same_allowlist: bool,
    },
    #[serde(rename = "hand_built_aggregation")]
    HandBuiltAggregation {
        same_corpus: bool,
        same_interval: bool,
        integer_ppm: bool,
        nearest_rank: bool,
        deployment_temporal_only: bool,
    },
}

#[derive(Clone, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScoringManifestV0 {
    manifest_format: String,
    manifest_version: u16,
    answer_facts: AnswerScoringV0,
    hard_failures: Vec<HardFailureV0>,
    system_assertions: Vec<SystemAssertionKindV0>,
}

impl ScoringManifestV0 {
    pub fn causal_claim_is_hard_failure(&self) -> bool {
        self.hard_failures.contains(&HardFailureV0::CausalClaim)
    }
}

impl fmt::Debug for ScoringManifestV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScoringManifestV0")
            .field("hard_failure_count", &self.hard_failures.len())
            .field("system_assertion_count", &self.system_assertions.len())
            .finish()
    }
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct AnswerScoringV0 {
    rational_integer_counts: bool,
    exact_integer_match: bool,
    wrong_value_false_positive: bool,
    wrong_value_missing_expected: bool,
    unsupported_extra_reduces_precision: bool,
    provenance_required: bool,
    all_references_must_match_support: bool,
    deterministic_representative_examples: bool,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum HardFailureV0 {
    CausalClaim,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum SystemAssertionKindV0 {
    OutcomeCounts,
    Completeness,
    BudgetAdherence,
    ReplayAgreement,
}

pub fn decode_scenario_manifest_v0(input: &[u8]) -> Result<ScenarioManifestV0, ContractErrorV0> {
    let manifest = decode_canonical(input, ContractKindV0::Scenario)?;
    validate_scenario(&manifest)?;
    Ok(manifest)
}

pub fn decode_oracle_manifest_v0(input: &[u8]) -> Result<OracleManifestV0, ContractErrorV0> {
    let manifest = decode_canonical(input, ContractKindV0::Oracle)?;
    validate_oracle(&manifest)?;
    Ok(manifest)
}

pub fn decode_baseline_manifest_v0(input: &[u8]) -> Result<BaselineManifestV0, ContractErrorV0> {
    let manifest = decode_canonical(input, ContractKindV0::Baseline)?;
    validate_baselines(&manifest)?;
    Ok(manifest)
}

pub fn decode_scoring_manifest_v0(input: &[u8]) -> Result<ScoringManifestV0, ContractErrorV0> {
    let manifest = decode_canonical(input, ContractKindV0::Scoring)?;
    validate_scoring(&manifest)?;
    Ok(manifest)
}

pub fn validate_sre_contract_links_v0(
    scenarios: &ScenarioManifestV0,
    oracle: &OracleManifestV0,
) -> Result<(), ContractErrorV0> {
    validate_links(scenarios, oracle)
}

fn decode_canonical<T>(input: &[u8], kind: ContractKindV0) -> Result<T, ContractErrorV0>
where
    T: DeserializeOwned + Serialize,
{
    if input.len() > MAX_FIXTURE_DOCUMENT_BYTES {
        return Err(ContractErrorV0::DocumentTooLarge {
            maximum: MAX_FIXTURE_DOCUMENT_BYTES,
            actual: input.len(),
        });
    }
    let Some(json) = input.strip_suffix(b"\n") else {
        return Err(ContractErrorV0::MissingTerminalLineFeed);
    };
    let manifest: T =
        serde_json::from_slice(json).map_err(|_| ContractErrorV0::MalformedJson { kind })?;
    let mut canonical =
        serde_json::to_vec(&manifest).map_err(|_| ContractErrorV0::EncodingFailure)?;
    canonical.push(b'\n');
    if canonical != input {
        return Err(ContractErrorV0::NonCanonicalEncoding { kind });
    }
    Ok(manifest)
}

fn validate_header(
    actual_format: &str,
    expected_format: &str,
    actual_version: u16,
    kind: ContractKindV0,
) -> Result<(), ContractErrorV0> {
    if actual_format != expected_format {
        return Err(ContractErrorV0::UnsupportedManifestFormat { kind });
    }
    if actual_version != MANIFEST_VERSION_V0 {
        return Err(ContractErrorV0::UnsupportedManifestVersion {
            kind,
            actual: actual_version,
        });
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
fn validate_scenario(manifest: &ScenarioManifestV0) -> Result<(), ContractErrorV0> {
    validate_header(
        &manifest.manifest_format,
        "decant-sre-scenario",
        manifest.manifest_version,
        ContractKindV0::Scenario,
    )?;
    bounded(
        manifest.scenarios.len(),
        MAX_SCENARIOS,
        ContractBoundV0::Scenarios,
    )?;
    if manifest.scenarios.is_empty() {
        return invalid(ContractKindV0::Scenario);
    }
    bounded(
        manifest.mixer_vectors.len(),
        MAX_MIXER_VECTORS,
        ContractBoundV0::MixerVectors,
    )?;
    if manifest.mixer_vectors.len() < 10 {
        return invalid(ContractKindV0::Scenario);
    }
    bounded(
        manifest.rate_vectors.len(),
        MAX_REFERENCE_VECTORS,
        ContractBoundV0::ReferenceVectors,
    )?;
    bounded(
        manifest.percentile_vectors.len(),
        MAX_REFERENCE_VECTORS,
        ContractBoundV0::ReferenceVectors,
    )?;

    let mut primary_count = 0;
    let mut transforms = [false; 3];
    let mut scenario_ids = BTreeSet::new();
    for scenario in &manifest.scenarios {
        validate_id(scenario.id())?;
        if !scenario_ids.insert(scenario.id()) {
            return invalid(ContractKindV0::Scenario);
        }
        bounded(
            scenario.queries().len(),
            MAX_QUERIES_PER_SCENARIO,
            ContractBoundV0::Queries,
        )?;
        if scenario.queries().is_empty() {
            return invalid(ContractKindV0::Scenario);
        }
        let mut query_ids = BTreeSet::new();
        for query in scenario.queries() {
            validate_query(query)?;
            if !query_ids.insert(query.query_id.as_str()) {
                return invalid(ContractKindV0::Scenario);
            }
        }
        match scenario {
            ScenarioV0::Primary {
                scenario_id,
                seed,
                source_id,
                interval_start_ns,
                interval_end_ns,
                counts,
                timing,
                rates,
                latency,
                errors,
                deployments,
                queries,
            } => {
                primary_count += 1;
                validate_primary(
                    scenario_id,
                    *seed,
                    source_id,
                    *interval_start_ns,
                    *interval_end_ns,
                    *counts,
                    *timing,
                    *rates,
                    *latency,
                    *errors,
                    deployments,
                    queries,
                )?;
            }
            ScenarioV0::Holdout {
                base_scenario_id,
                transform,
                ..
            } => {
                validate_id(base_scenario_id)?;
                if base_scenario_id != PRIMARY_SCENARIO_ID {
                    return invalid(ContractKindV0::Scenario);
                }
                transforms[match transform {
                    HoldoutTransformV0::AffectedRegionSwap => 0,
                    HoldoutTransformV0::AffectedServiceDeploymentSwap => 1,
                    HoldoutTransformV0::NoRegression => 2,
                }] = true;
            }
        }
    }
    if primary_count != 1 || transforms != [true, true, true] {
        return invalid(ContractKindV0::Scenario);
    }

    let domains = manifest.selection_domains;
    if domains.duplicate != 4_923_930_056_176_386_388
        || domains.reorder != 5_928_231_699_254_432_339
        || domains.example != 4_996_815_586_883_028_307
        || domains.holdout != 5_210_467_150_163_432_531
        || domains.ordering != SelectionOrderV0::ScoreThenSourceSequence
        || BTreeSet::from([
            domains.duplicate,
            domains.reorder,
            domains.example,
            domains.holdout,
        ])
        .len()
            != 4
    {
        return invalid(ContractKindV0::Scenario);
    }

    for vector in &manifest.mixer_vectors {
        if mix_selection_v0(vector.seed ^ vector.domain_constant ^ vector.source_sequence)
            != vector.expected_score
        {
            return Err(ContractErrorV0::ReferenceVectorMismatch);
        }
    }
    for vector in &manifest.rate_vectors {
        let scaled = vector.numerator.checked_mul(1_000_000);
        if vector.denominator == 0
            || scaled.map(|value| value % vector.denominator) != Some(0)
            || scaled.map(|value| value / vector.denominator) != Some(vector.expected_ppm)
        {
            return Err(ContractErrorV0::ReferenceVectorMismatch);
        }
    }
    for vector in &manifest.percentile_vectors {
        bounded(
            vector.values.len(),
            MAX_PERCENTILE_VALUES,
            ContractBoundV0::PercentileValues,
        )?;
        if nearest_rank_v0(&vector.values, vector.percentile_basis_points) != Some(vector.expected)
        {
            return Err(ContractErrorV0::ReferenceVectorMismatch);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn validate_primary(
    scenario_id: &str,
    seed: u64,
    source_id: &str,
    interval_start_ns: i64,
    interval_end_ns: i64,
    counts: PrimaryCountsV0,
    timing: TimingV0,
    rates: RatesV0,
    latency: LatencyV0,
    errors: ErrorPatternV0,
    deployments: &[DeploymentV0],
    queries: &[QueryV0],
) -> Result<(), ContractErrorV0> {
    validate_id(source_id)?;
    bounded(
        deployments.len(),
        MAX_DEPLOYMENTS,
        ContractBoundV0::Deployments,
    )?;
    for deployment in deployments {
        validate_id(&deployment.deployment_id)?;
        validate_id(&deployment.service_id)?;
        validate_id(&deployment.region_id)?;
        if deployment.causal_claim {
            return invalid(ContractKindV0::Scenario);
        }
    }
    if deployments
        .iter()
        .map(|deployment| deployment.deployment_id.as_str())
        .collect::<BTreeSet<_>>()
        .len()
        != deployments.len()
    {
        return invalid(ContractKindV0::Scenario);
    }
    let exact_counts = counts.payment_outcomes == 60_000
        && counts.accepted_distractors == 38_250
        && counts.late_distractors == 250
        && counts.metric_events == 480
        && counts.deployment_events == 20
        && counts.exact_duplicate_deliveries == 1_000
        && counts.data_deliveries == 100_000
        && counts.accepted_unique_events == 98_750
        && counts.delayed_accepted_deliveries == 2_000
        && counts.terminal_controls == 1;
    let arithmetic = counts.unique_events() == 99_000
        && counts.unique_events() + counts.exact_duplicate_deliveries == counts.data_deliveries
        && counts.unique_events() - counts.late_distractors == counts.accepted_unique_events;
    if !exact_counts || !arithmetic || deployments.len() != 20 {
        return Err(ContractErrorV0::ArithmeticMismatch);
    }
    let notable = deployments.iter().filter(|deployment| {
        deployment.service_id == "payments-v2"
            && deployment.region_id == "region-b"
            && deployment.event_time_ns == 1_767_226_140_000_000_000
            && deployment.notable_temporal_evidence
    });
    if notable.count() != 1 {
        return invalid(ContractKindV0::Scenario);
    }
    let primary_fields_match = scenario_id == PRIMARY_SCENARIO_ID
        && seed == 42
        && interval_start_ns == SRE_START_NS
        && interval_end_ns == SRE_END_NS
        && timing.late_event_time_before_ns == SRE_START_NS + 300_000_000_000
        && timing.late_delivery_after_ns == SRE_START_NS + 900_000_000_000
        && timing.delayed_min_ns == 1_000_000_000
        && timing.delayed_max_ns == 25_000_000_000
        && rates.requests_per_second_per_region == 25
        && rates.pre_region_a_ppm == 4_000
        && rates.pre_region_b_ppm == 4_000
        && rates.post_region_a_ppm == 4_000
        && rates.post_region_b_ppm == 32_000
        && rates.post_global_ppm == 18_000
        && latency.pre_region_a_p99_ms == 180
        && latency.pre_region_b_p99_ms == 180
        && latency.post_region_a_p99_ms == 180
        && latency.post_region_b_p99_ms == 720
        && latency.percentile_basis_points == 9_900
        && latency.method == PercentileMethodV0::NearestRank
        && errors.base_internal_error_numerator == 1
        && errors.base_denominator == 250
        && errors.post_region_b_upstream_timeout_numerator == 7
        && errors.post_region_b_denominator == 250;
    let scoped_queries = queries.len() >= 2
        && queries.iter().any(|query| query.expected_regression)
        && queries.iter().any(|query| !query.expected_regression);
    if !primary_fields_match || !scoped_queries {
        return invalid(ContractKindV0::Scenario);
    }
    Ok(())
}

fn validate_query(query: &QueryV0) -> Result<(), ContractErrorV0> {
    validate_id(&query.query_id)?;
    validate_id(&query.service_id)?;
    validate_id(&query.region_id)?;
    if query.as_of_ns != SRE_END_NS
        || query.window_ns != 600_000_000_000
        || query.evidence_reachback_ns != 900_000_000_000
        || query.byte_budget != 16_384
        || query.max_raw_examples != 3
        || query.allowed_event_kinds
            != [
                EventKindV0::PaymentOutcome,
                EventKindV0::Metric,
                EventKindV0::Deployment,
            ]
    {
        return invalid(ContractKindV0::Scenario);
    }
    Ok(())
}

fn validate_oracle(manifest: &OracleManifestV0) -> Result<(), ContractErrorV0> {
    validate_header(
        &manifest.manifest_format,
        "decant-sre-oracle",
        manifest.manifest_version,
        ContractKindV0::Oracle,
    )?;
    bounded(
        manifest.scenarios.len(),
        MAX_SCENARIOS,
        ContractBoundV0::Scenarios,
    )?;
    if manifest.scenarios.is_empty() || manifest.support_vectors.is_empty() {
        return invalid(ContractKindV0::Oracle);
    }
    bounded(
        manifest.support_vectors.len(),
        MAX_REFERENCE_VECTORS,
        ContractBoundV0::ReferenceVectors,
    )?;
    let mut scenario_ids = BTreeSet::new();
    let mut has_decoy = false;
    for scenario in &manifest.scenarios {
        validate_id(&scenario.scenario_id)?;
        if !scenario_ids.insert(scenario.scenario_id.as_str()) {
            return invalid(ContractKindV0::Oracle);
        }
        bounded(
            scenario.queries.len(),
            MAX_QUERIES_PER_SCENARIO,
            ContractBoundV0::Queries,
        )?;
        let mut query_ids = BTreeSet::new();
        for query in &scenario.queries {
            validate_id(&query.query_id)?;
            if !query_ids.insert(query.query_id.as_str()) {
                return invalid(ContractKindV0::Oracle);
            }
            bounded(
                query.answer_facts.len(),
                MAX_FACTS_PER_QUERY,
                ContractBoundV0::Facts,
            )?;
            if query.system_assertions
                != (SystemAssertionsV0 {
                    accepted_unique_events: 98_750,
                    late_events: 250,
                    exact_duplicate_deliveries: 1_000,
                    terminal_controls: 1,
                    complete_input_expected: true,
                    maximum_output_bytes: 16_384,
                    replay_hashes_equal: true,
                })
            {
                return invalid(ContractKindV0::Oracle);
            }
            let mut fact_ids = BTreeSet::new();
            for fact in &query.answer_facts {
                validate_id(&fact.fact_id)?;
                if !fact_ids.insert(fact.fact_id.as_str()) {
                    return invalid(ContractKindV0::Oracle);
                }
                bounded(
                    fact.support_rules.len(),
                    MAX_SUPPORT_RULES_PER_FACT,
                    ContractBoundV0::SupportRules,
                )?;
                if fact.support_rules.is_empty() {
                    return invalid(ContractKindV0::Oracle);
                }
                for rule in &fact.support_rules {
                    validate_predicate(rule)?;
                }
            }
        }
    }
    for vector in &manifest.support_vectors {
        validate_predicate(&vector.predicate)?;
        validate_candidate(&vector.candidate)?;
        if vector.predicate.allows(&vector.candidate) != vector.expected_allowed {
            return Err(ContractErrorV0::ReferenceVectorMismatch);
        }
        has_decoy |= !vector.expected_allowed;
    }
    if !has_decoy {
        return invalid(ContractKindV0::Oracle);
    }
    Ok(())
}

fn validate_predicate(predicate: &SupportPredicateV0) -> Result<(), ContractErrorV0> {
    validate_id(&predicate.service_id)?;
    validate_id(&predicate.region_id)
}

fn validate_candidate(candidate: &ProvenanceCandidateV0) -> Result<(), ContractErrorV0> {
    validate_id(&candidate.service_id)?;
    validate_id(&candidate.region_id)?;
    bounded(
        candidate.provenance.len(),
        MAX_PROVENANCE_BYTES,
        ContractBoundV0::ProvenanceBytes,
    )?;
    if !candidate.provenance.is_ascii() {
        return invalid(ContractKindV0::Oracle);
    }
    Ok(())
}

fn validate_baselines(manifest: &BaselineManifestV0) -> Result<(), ContractErrorV0> {
    validate_header(
        &manifest.manifest_format,
        "decant-sre-baselines",
        manifest.manifest_version,
        ContractKindV0::Baseline,
    )?;
    if manifest.baselines.len() != REQUIRED_BASELINES {
        return Err(ContractErrorV0::BoundExceeded {
            bound: ContractBoundV0::Baselines,
            maximum: REQUIRED_BASELINES,
            actual: manifest.baselines.len(),
        });
    }
    let mut kinds = manifest
        .baselines
        .iter()
        .map(|baseline| baseline.kind)
        .collect::<Vec<_>>();
    kinds.sort_unstable();
    kinds.dedup();
    let mut baseline_ids = BTreeSet::new();
    for baseline in &manifest.baselines {
        validate_id(&baseline.baseline_id)?;
        if !baseline_ids.insert(baseline.baseline_id.as_str()) {
            return invalid(ContractKindV0::Baseline);
        }
        if !baseline.enabled
            || !baseline.request_input
            || !baseline.records_input
            || !baseline.budget_input
            || !baseline.config_input
            || baseline.oracle_input
            || baseline.scoring_input
        {
            return invalid(ContractKindV0::Baseline);
        }
        let semantics_match = matches!(
            (baseline.kind, baseline.semantics),
            (
                BaselineKindV0::LastNCanonical,
                BaselineSemanticsV0::LastNCanonical {
                    newest_first: true,
                    whole_record_packing: true,
                }
            ) | (
                BaselineKindV0::ScopedEvidenceFilter,
                BaselineSemanticsV0::ScopedEvidenceFilter {
                    query_window: true,
                    evidence_reachback: true,
                    same_scope: true,
                    same_allowlist: true,
                }
            ) | (
                BaselineKindV0::HandBuiltAggregation,
                BaselineSemanticsV0::HandBuiltAggregation {
                    same_corpus: true,
                    same_interval: true,
                    integer_ppm: true,
                    nearest_rank: true,
                    deployment_temporal_only: true,
                }
            )
        );
        if !semantics_match {
            return invalid(ContractKindV0::Baseline);
        }
    }
    if kinds.len() != REQUIRED_BASELINES || manifest.rag_enabled {
        return invalid(ContractKindV0::Baseline);
    }
    Ok(())
}

fn validate_scoring(manifest: &ScoringManifestV0) -> Result<(), ContractErrorV0> {
    validate_header(
        &manifest.manifest_format,
        "decant-sre-scoring",
        manifest.manifest_version,
        ContractKindV0::Scoring,
    )?;
    let answer = manifest.answer_facts;
    if !answer.rational_integer_counts
        || !answer.exact_integer_match
        || !answer.wrong_value_false_positive
        || !answer.wrong_value_missing_expected
        || !answer.unsupported_extra_reduces_precision
        || !answer.provenance_required
        || !answer.all_references_must_match_support
        || !answer.deterministic_representative_examples
        || manifest.hard_failures != [HardFailureV0::CausalClaim]
        || manifest.system_assertions
            != [
                SystemAssertionKindV0::OutcomeCounts,
                SystemAssertionKindV0::Completeness,
                SystemAssertionKindV0::BudgetAdherence,
                SystemAssertionKindV0::ReplayAgreement,
            ]
    {
        return invalid(ContractKindV0::Scoring);
    }
    Ok(())
}

fn validate_links(
    scenarios: &ScenarioManifestV0,
    oracle: &OracleManifestV0,
) -> Result<(), ContractErrorV0> {
    for scenario_oracle in &oracle.scenarios {
        let Some(scenario) = scenarios
            .scenarios
            .iter()
            .find(|scenario| scenario.id() == scenario_oracle.scenario_id)
        else {
            return Err(ContractErrorV0::CrossManifestMismatch);
        };
        for query_oracle in &scenario_oracle.queries {
            if !scenario
                .queries()
                .iter()
                .any(|query| query.query_id == query_oracle.query_id)
            {
                return Err(ContractErrorV0::CrossManifestMismatch);
            }
        }
    }
    Ok(())
}

fn validate_id(value: &str) -> Result<(), ContractErrorV0> {
    bounded(
        value.len(),
        MAX_IDENTIFIER_BYTES,
        ContractBoundV0::IdentifierBytes,
    )?;
    let bytes = value.as_bytes();
    let valid = !bytes.is_empty()
        && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        && bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        });
    if !valid {
        return Err(ContractErrorV0::InvalidIdentifier);
    }
    Ok(())
}

fn bounded(actual: usize, maximum: usize, bound: ContractBoundV0) -> Result<(), ContractErrorV0> {
    if actual > maximum {
        return Err(ContractErrorV0::BoundExceeded {
            bound,
            maximum,
            actual,
        });
    }
    Ok(())
}

fn invalid<T>(kind: ContractKindV0) -> Result<T, ContractErrorV0> {
    Err(ContractErrorV0::InvalidContract { kind })
}

fn mix_selection_v0(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    value ^ (value >> 31)
}

fn nearest_rank_v0(values: &[u64], percentile_basis_points: u16) -> Option<u64> {
    if values.is_empty() || percentile_basis_points == 0 || percentile_basis_points > 10_000 {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let numerator = usize::from(percentile_basis_points).checked_mul(sorted.len())?;
    let rank = numerator.div_ceil(10_000);
    sorted.get(rank - 1).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixer_has_frozen_unsigned_boundaries() {
        assert_eq!(mix_selection_v0(0), 16_294_208_416_658_607_535);
        assert_eq!(mix_selection_v0(u64::MAX), 16_490_336_266_968_443_936);
    }

    #[test]
    fn nearest_rank_is_one_based_and_uses_ceiling() {
        assert_eq!(nearest_rank_v0(&[30, 10, 20], 1), Some(10));
        assert_eq!(nearest_rank_v0(&[30, 10, 20], 5_000), Some(20));
        assert_eq!(nearest_rank_v0(&[30, 10, 20], 10_000), Some(30));
        assert_eq!(nearest_rank_v0(&[], 9_900), None);
    }

    #[test]
    fn every_structural_bound_accepts_exact_and_rejects_one_past() {
        for (maximum, bound) in [
            (MAX_SCENARIOS, ContractBoundV0::Scenarios),
            (MAX_QUERIES_PER_SCENARIO, ContractBoundV0::Queries),
            (MAX_FACTS_PER_QUERY, ContractBoundV0::Facts),
            (MAX_SUPPORT_RULES_PER_FACT, ContractBoundV0::SupportRules),
            (MAX_DEPLOYMENTS, ContractBoundV0::Deployments),
            (MAX_MIXER_VECTORS, ContractBoundV0::MixerVectors),
            (MAX_REFERENCE_VECTORS, ContractBoundV0::ReferenceVectors),
            (MAX_PERCENTILE_VALUES, ContractBoundV0::PercentileValues),
            (MAX_IDENTIFIER_BYTES, ContractBoundV0::IdentifierBytes),
            (MAX_PROVENANCE_BYTES, ContractBoundV0::ProvenanceBytes),
        ] {
            assert_eq!(bounded(maximum, maximum, bound), Ok(()));
            assert!(matches!(
                bounded(maximum + 1, maximum, bound),
                Err(ContractErrorV0::BoundExceeded { actual, .. }) if actual == maximum + 1
            ));
        }

        let exact_id = format!("a{}", "z".repeat(MAX_IDENTIFIER_BYTES - 1));
        assert_eq!(validate_id(&exact_id), Ok(()));
        assert!(matches!(
            validate_id(&format!("a{}", "z".repeat(MAX_IDENTIFIER_BYTES))),
            Err(ContractErrorV0::BoundExceeded {
                bound: ContractBoundV0::IdentifierBytes,
                actual,
                ..
            }) if actual == MAX_IDENTIFIER_BYTES + 1
        ));

        let exact_candidate = ProvenanceCandidateV0 {
            service_id: "service".to_owned(),
            region_id: "region-a".to_owned(),
            event_kind: EventKindV0::Metric,
            error_code: None,
            provenance: "p".repeat(MAX_PROVENANCE_BYTES),
        };
        assert_eq!(validate_candidate(&exact_candidate), Ok(()));
        let mut one_past_candidate = exact_candidate;
        one_past_candidate.provenance.push('p');
        assert!(matches!(
            validate_candidate(&one_past_candidate),
            Err(ContractErrorV0::BoundExceeded {
                bound: ContractBoundV0::ProvenanceBytes,
                actual,
                ..
            }) if actual == MAX_PROVENANCE_BYTES + 1
        ));
    }

    #[test]
    fn baseline_cardinality_accepts_exact_and_rejects_one_past() {
        let input = include_bytes!("../contracts/sre-v0/baselines.json");
        let mut manifest: BaselineManifestV0 =
            decode_canonical(input, ContractKindV0::Baseline).unwrap();
        assert_eq!(manifest.baselines.len(), REQUIRED_BASELINES);
        assert_eq!(validate_baselines(&manifest), Ok(()));
        manifest.baselines.push(manifest.baselines[0].clone());
        assert!(matches!(
            validate_baselines(&manifest),
            Err(ContractErrorV0::BoundExceeded {
                bound: ContractBoundV0::Baselines,
                actual,
                ..
            }) if actual == REQUIRED_BASELINES + 1
        ));
    }
}
