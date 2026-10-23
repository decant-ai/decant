//! Bounded deterministic context compilation over approved Decant state views.

#![forbid(unsafe_code)]

pub mod evidence;
pub mod evidence_markdown;
pub mod investigation_coverage;

mod kafka_activity;
mod kafka_grouped_ratio;
mod kafka_material_change;
mod otlp_capture;

pub use kafka_activity::{
    KAFKA_ACTIVITY_CONTEXT_VERSION_V0, KafkaActivityContextConfigErrorV0,
    KafkaActivityContextErrorV0, KafkaActivityContextRequestV0, KafkaActivityContextV0,
    KafkaActivityContractIssueV0, MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0,
    MAX_KAFKA_ACTIVITY_DISTINCT_SCHEMAS_V0, MIN_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0,
    compile_kafka_activity_context_v0,
};

pub use kafka_grouped_ratio::{
    KAFKA_FIELD_EGRESS_POLICY_VERSION_V0, KAFKA_GROUPED_RATIO_CONTEXT_VERSION_V0,
    KafkaFieldEgressActionV0, KafkaFieldEgressPolicyV0, KafkaFieldEgressRuleV0,
    KafkaGroupedRatioContextConfigErrorV0, KafkaGroupedRatioContextErrorV0,
    KafkaGroupedRatioContextRequestV0, KafkaGroupedRatioContextV0,
    KafkaGroupedRatioContractIssueV0, MAX_KAFKA_FIELD_EGRESS_RULES_V0,
    MAX_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0, MAX_KAFKA_GROUPED_RATIO_LOOKBACK_NS_V0,
    MIN_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0, compile_kafka_grouped_ratio_context_v0,
};

pub use kafka_material_change::{
    KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V0, KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1,
    KafkaEntityKeyEgressActionV0, KafkaMaterialChangeConfigErrorV0, KafkaMaterialChangeContextV0,
    KafkaMaterialChangeContractIssueV0, KafkaMaterialChangeErrorV0, KafkaMaterialChangeRequestV0,
    MAX_KAFKA_MATERIAL_CHANGE_ITEMS_V0, MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0,
    MIN_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0, MIN_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V1,
    compile_kafka_material_change_context_v0, compile_kafka_material_change_context_v1,
};

pub use otlp_capture::{
    MAX_OTLP_CAPTURE_CONTEXT_OUTPUT_BYTES_V0, MAX_OTLP_CAPTURE_DISTINCT_SERVICES_V0,
    MAX_OTLP_CAPTURE_ERROR_EVIDENCE_V0, OTLP_CAPTURE_CONTEXT_VERSION_V0,
    OtlpCaptureArithmeticFieldV0, OtlpCaptureContextErrorV0, OtlpCaptureContextV0,
    OtlpCaptureContractIssueV0, compile_otlp_capture_context_v0,
};

use std::error::Error;
use std::fmt;

use decant_ir::{
    EventEnvelopeV0, MAX_FIELD_NAME_BYTES, MAX_IDENTIFIER_BYTES, MAX_KEY_BYTES, MAX_SCALAR_BYTES,
    ScalarKindV0, ScalarValueV0, SchemaIdV0,
};
use decant_state::{
    AdmissionStateHashErrorV0, AdmissionStateHashV0, AdmissionStateV0, EventTimeRetentionConfigV0,
    INTERVAL_RATIO_PPM_VERSION_V0, IntervalRatioPpmConfigV0,
    IntervalRatioPpmMaterializationErrorV0, IntervalRatioPpmViewV0,
    PRECEDING_EVENT_SELECTOR_VERSION_V0, PrecedingEventSelectorConfigV0,
    PrecedingEventSelectorErrorV0, ProjectedFieldChangeConfigV0,
    ProjectedFieldChangeMaterializationErrorV0, hash_admission_state_v0,
};

pub const CONTEXT_COMPILER_VERSION_V0: u16 = 0;
pub const MAX_CONTEXT_LOOKBACK_NS_V0: u64 = 10 * 60 * 1_000_000_000;
pub const MAX_CONTEXT_EVIDENCE_REACHBACK_NS_V0: u64 = 15 * 60 * 1_000_000_000;
pub const MAX_CONTEXT_OUTPUT_BYTES_V0: usize = 16 * 1_024;
pub const MAX_CONTEXT_SCOPE_PREDICATES_V0: usize = 8;
pub const MAX_CONTEXT_FACT_REQUESTS_V0: usize = 4_096;
pub const MAX_CONTEXT_REQUEST_BYTES_V0: usize = 8 * 1_024 * 1_024;
pub const MAX_CONTEXT_CANDIDATE_BYTES_V0: usize = 8 * 1_024 * 1_024;
pub const MAX_CONTEXT_FACT_BYTES_V0: usize = 128 * 1_024;
pub const MAX_CONTEXT_RATIO_EVIDENCE_V0: usize = 3;

const LENGTH_PREFIX_WIDTH: usize = 4;
const COUNT_WIDTH: usize = 4;
const VERSION_WIDTH: usize = 2;
const TIMESTAMP_WIDTH: usize = 8;
const DURATION_WIDTH: usize = 8;
const BUDGET_WIDTH: usize = 8;
const OPTIONAL_TAG_WIDTH: usize = 1;
const FACT_TAG_WIDTH: usize = 1;
const LIMIT_WIDTH: usize = 8;
const SCALAR_TAG_WIDTH: usize = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextConfigFieldV0 {
    Lookback,
    OutputBytes,
    Key,
    ScopePredicates,
    FactRequests,
    RequestBytes,
    FactId,
    ScopeField,
    ScopeValue,
    Discriminator,
    EvidenceReachback,
    PrecedingLookback,
    SelectedField,
    ChangeRetention,
    PrecedingRetention,
    RatioEvidence,
    RatioPredicateField,
    RatioRetention,
}

#[derive(Clone, Eq, PartialEq)]
pub enum ContextCompileConfigErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    Zero {
        field: ContextConfigFieldV0,
    },
    AboveMaximum {
        field: ContextConfigFieldV0,
        maximum: usize,
        actual: usize,
    },
    InvalidSyntax {
        field: ContextConfigFieldV0,
    },
    DuplicateScopeField,
    DuplicateFactId,
    InsufficientRetention {
        field: ContextConfigFieldV0,
        minimum_ns: u64,
        actual_ns: u64,
    },
    ArithmeticOverflow {
        field: ContextConfigFieldV0,
    },
    Allocation,
}

impl fmt::Debug for ContextCompileConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::Zero { field } => formatter
                .debug_struct("Zero")
                .field("field", field)
                .finish(),
            Self::AboveMaximum {
                field,
                maximum,
                actual,
            } => formatter
                .debug_struct("AboveMaximum")
                .field("field", field)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::InvalidSyntax { field } => formatter
                .debug_struct("InvalidSyntax")
                .field("field", field)
                .finish(),
            Self::DuplicateScopeField => formatter.write_str("DuplicateScopeField"),
            Self::DuplicateFactId => formatter.write_str("DuplicateFactId"),
            Self::InsufficientRetention {
                field,
                minimum_ns,
                actual_ns,
            } => formatter
                .debug_struct("InsufficientRetention")
                .field("field", field)
                .field("minimum_ns", minimum_ns)
                .field("actual_ns", actual_ns)
                .finish(),
            Self::ArithmeticOverflow { field } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("field", field)
                .finish(),
            Self::Allocation => formatter.write_str("Allocation"),
        }
    }
}

impl fmt::Display for ContextCompileConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for ContextCompileConfigErrorV0 {}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub struct ContextFactIdV0(String);

impl ContextFactIdV0 {
    pub fn new(value: &str) -> Result<Self, ContextCompileConfigErrorV0> {
        validate_identifier(value, ContextConfigFieldV0::FactId)?;
        Ok(Self(try_owned_str(value)?))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ContextFactIdV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContextFactIdV0")
            .field("byte_len", &self.0.len())
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ContextScopePredicateV0 {
    field: String,
    value: String,
}

impl ContextScopePredicateV0 {
    pub fn new(field: &str, value: &str) -> Result<Self, ContextCompileConfigErrorV0> {
        validate_field_name(field, ContextConfigFieldV0::ScopeField)?;
        validate_bounded_text(value, ContextConfigFieldV0::ScopeValue, true)?;
        Ok(Self {
            field: try_owned_str(field)?,
            value: try_owned_str(value)?,
        })
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub fn value(&self) -> &str {
        &self.value
    }
}

impl fmt::Debug for ContextScopePredicateV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContextScopePredicateV0")
            .field("field_byte_len", &self.field.len())
            .field("value_byte_len", &self.value.len())
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct PrecedingEvidenceSpecV0 {
    retention: EventTimeRetentionConfigV0,
    schema_id: SchemaIdV0,
    lookback_ns: u64,
    selected_field: String,
}

impl PrecedingEvidenceSpecV0 {
    pub fn new(
        retention: EventTimeRetentionConfigV0,
        schema_id: SchemaIdV0,
        lookback_ns: u64,
        selected_field: &str,
    ) -> Result<Self, ContextCompileConfigErrorV0> {
        validate_positive_duration(
            lookback_ns,
            MAX_CONTEXT_EVIDENCE_REACHBACK_NS_V0,
            ContextConfigFieldV0::PrecedingLookback,
        )?;
        validate_field_name(selected_field, ContextConfigFieldV0::SelectedField)?;
        if retention.retention_horizon_ns() < lookback_ns {
            return Err(ContextCompileConfigErrorV0::InsufficientRetention {
                field: ContextConfigFieldV0::PrecedingRetention,
                minimum_ns: lookback_ns,
                actual_ns: retention.retention_horizon_ns(),
            });
        }
        Ok(Self {
            retention,
            schema_id,
            lookback_ns,
            selected_field: try_owned_str(selected_field)?,
        })
    }

    pub const fn retention(&self) -> EventTimeRetentionConfigV0 {
        self.retention
    }

    pub const fn schema_id(&self) -> &SchemaIdV0 {
        &self.schema_id
    }

    pub const fn lookback_ns(&self) -> u64 {
        self.lookback_ns
    }

    pub fn selected_field(&self) -> &str {
        &self.selected_field
    }
}

impl fmt::Debug for PrecedingEvidenceSpecV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrecedingEvidenceSpecV0")
            .field("retention", &self.retention)
            .field("schema_id", &"<redacted>")
            .field("lookback_ns", &self.lookback_ns)
            .field("selected_field_byte_len", &self.selected_field.len())
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ProjectedChangeFactSpecV0 {
    fact_id: ContextFactIdV0,
    change_config: ProjectedFieldChangeConfigV0,
    discriminator: String,
    evidence_reachback_ns: u64,
    preceding: Option<PrecedingEvidenceSpecV0>,
}

impl ProjectedChangeFactSpecV0 {
    pub fn new(
        fact_id: ContextFactIdV0,
        change_config: ProjectedFieldChangeConfigV0,
        discriminator: &str,
        evidence_reachback_ns: u64,
        preceding: Option<PrecedingEvidenceSpecV0>,
    ) -> Result<Self, ContextCompileConfigErrorV0> {
        validate_bounded_text(discriminator, ContextConfigFieldV0::Discriminator, true)?;
        if evidence_reachback_ns > MAX_CONTEXT_EVIDENCE_REACHBACK_NS_V0 {
            return Err(ContextCompileConfigErrorV0::AboveMaximum {
                field: ContextConfigFieldV0::EvidenceReachback,
                maximum: usize::try_from(MAX_CONTEXT_EVIDENCE_REACHBACK_NS_V0)
                    .unwrap_or(usize::MAX),
                actual: usize::try_from(evidence_reachback_ns).unwrap_or(usize::MAX),
            });
        }
        Ok(Self {
            fact_id,
            change_config,
            discriminator: try_owned_str(discriminator)?,
            evidence_reachback_ns,
            preceding,
        })
    }

    pub const fn fact_id(&self) -> &ContextFactIdV0 {
        &self.fact_id
    }

    pub const fn change_config(&self) -> &ProjectedFieldChangeConfigV0 {
        &self.change_config
    }

    pub fn discriminator(&self) -> &str {
        &self.discriminator
    }

    pub const fn evidence_reachback_ns(&self) -> u64 {
        self.evidence_reachback_ns
    }

    pub const fn preceding(&self) -> Option<&PrecedingEvidenceSpecV0> {
        self.preceding.as_ref()
    }
}

impl fmt::Debug for ProjectedChangeFactSpecV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectedChangeFactSpecV0")
            .field("fact_id", &self.fact_id)
            .field("change_config", &self.change_config)
            .field("discriminator_byte_len", &self.discriminator.len())
            .field("evidence_reachback_ns", &self.evidence_reachback_ns)
            .field("preceding", &self.preceding)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct IntervalRatioPpmFactSpecV0 {
    fact_id: ContextFactIdV0,
    retention: EventTimeRetentionConfigV0,
    schema_id: SchemaIdV0,
    predicate_field: String,
    predicate_value: ScalarValueV0,
    evidence_limit: usize,
}

impl IntervalRatioPpmFactSpecV0 {
    pub fn new(
        fact_id: ContextFactIdV0,
        retention: EventTimeRetentionConfigV0,
        schema_id: SchemaIdV0,
        predicate_field: &str,
        predicate_value: ScalarValueV0,
        evidence_limit: usize,
    ) -> Result<Self, ContextCompileConfigErrorV0> {
        validate_field_name(predicate_field, ContextConfigFieldV0::RatioPredicateField)?;
        if evidence_limit > MAX_CONTEXT_RATIO_EVIDENCE_V0 {
            return Err(ContextCompileConfigErrorV0::AboveMaximum {
                field: ContextConfigFieldV0::RatioEvidence,
                maximum: MAX_CONTEXT_RATIO_EVIDENCE_V0,
                actual: evidence_limit,
            });
        }
        Ok(Self {
            fact_id,
            retention,
            schema_id,
            predicate_field: try_owned_str(predicate_field)?,
            predicate_value,
            evidence_limit,
        })
    }

    pub const fn fact_id(&self) -> &ContextFactIdV0 {
        &self.fact_id
    }

    pub const fn retention(&self) -> EventTimeRetentionConfigV0 {
        self.retention
    }

    pub const fn schema_id(&self) -> &SchemaIdV0 {
        &self.schema_id
    }

    pub fn predicate_field(&self) -> &str {
        &self.predicate_field
    }

    pub const fn predicate_value(&self) -> &ScalarValueV0 {
        &self.predicate_value
    }

    pub const fn evidence_limit(&self) -> usize {
        self.evidence_limit
    }
}

impl fmt::Debug for IntervalRatioPpmFactSpecV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IntervalRatioPpmFactSpecV0")
            .field("fact_id", &self.fact_id)
            .field("retention", &self.retention)
            .field("schema_id", &"<redacted>")
            .field("predicate_field_byte_len", &self.predicate_field.len())
            .field("predicate_value", &"<redacted>")
            .field("evidence_limit", &self.evidence_limit)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct ContextCompileRequestV0 {
    version: u16,
    as_of_time_ns: i64,
    lookback_ns: u64,
    byte_budget: usize,
    key: Vec<u8>,
    scope: Vec<ContextScopePredicateV0>,
    facts: Vec<ProjectedChangeFactSpecV0>,
    interval_ratio_facts: Vec<IntervalRatioPpmFactSpecV0>,
    logical_bytes: usize,
}

impl ContextCompileRequestV0 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        version: u16,
        as_of_time_ns: i64,
        lookback_ns: u64,
        byte_budget: usize,
        key: &[u8],
        scope: Vec<ContextScopePredicateV0>,
        facts: Vec<ProjectedChangeFactSpecV0>,
    ) -> Result<Self, ContextCompileConfigErrorV0> {
        Self::new_with_interval_ratio_facts(
            version,
            as_of_time_ns,
            lookback_ns,
            byte_budget,
            key,
            scope,
            facts,
            Vec::new(),
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_interval_ratio_facts(
        version: u16,
        as_of_time_ns: i64,
        lookback_ns: u64,
        byte_budget: usize,
        key: &[u8],
        mut scope: Vec<ContextScopePredicateV0>,
        mut facts: Vec<ProjectedChangeFactSpecV0>,
        mut interval_ratio_facts: Vec<IntervalRatioPpmFactSpecV0>,
    ) -> Result<Self, ContextCompileConfigErrorV0> {
        if version != CONTEXT_COMPILER_VERSION_V0 {
            return Err(ContextCompileConfigErrorV0::UnsupportedVersion { actual: version });
        }
        validate_positive_duration(
            lookback_ns,
            MAX_CONTEXT_LOOKBACK_NS_V0,
            ContextConfigFieldV0::Lookback,
        )?;
        validate_positive_usize(
            byte_budget,
            MAX_CONTEXT_OUTPUT_BYTES_V0,
            ContextConfigFieldV0::OutputBytes,
        )?;
        if key.len() > MAX_KEY_BYTES {
            return Err(ContextCompileConfigErrorV0::AboveMaximum {
                field: ContextConfigFieldV0::Key,
                maximum: MAX_KEY_BYTES,
                actual: key.len(),
            });
        }
        if scope.len() > MAX_CONTEXT_SCOPE_PREDICATES_V0 {
            return Err(ContextCompileConfigErrorV0::AboveMaximum {
                field: ContextConfigFieldV0::ScopePredicates,
                maximum: MAX_CONTEXT_SCOPE_PREDICATES_V0,
                actual: scope.len(),
            });
        }
        let fact_count = facts.len().checked_add(interval_ratio_facts.len()).ok_or(
            ContextCompileConfigErrorV0::ArithmeticOverflow {
                field: ContextConfigFieldV0::FactRequests,
            },
        )?;
        validate_positive_usize(
            fact_count,
            MAX_CONTEXT_FACT_REQUESTS_V0,
            ContextConfigFieldV0::FactRequests,
        )?;

        scope.sort_by(|left, right| left.field.cmp(&right.field));
        if scope.windows(2).any(|pair| pair[0].field == pair[1].field) {
            return Err(ContextCompileConfigErrorV0::DuplicateScopeField);
        }
        facts.sort_by(|left, right| left.fact_id.cmp(&right.fact_id));
        if facts
            .windows(2)
            .any(|pair| pair[0].fact_id == pair[1].fact_id)
        {
            return Err(ContextCompileConfigErrorV0::DuplicateFactId);
        }
        interval_ratio_facts.sort_by(|left, right| left.fact_id.cmp(&right.fact_id));
        if interval_ratio_facts
            .windows(2)
            .any(|pair| pair[0].fact_id == pair[1].fact_id)
            || fact_id_families_overlap(&facts, &interval_ratio_facts)
        {
            return Err(ContextCompileConfigErrorV0::DuplicateFactId);
        }

        for fact in &facts {
            let required_change_retention = lookback_ns
                .checked_add(fact.evidence_reachback_ns)
                .ok_or(ContextCompileConfigErrorV0::ArithmeticOverflow {
                    field: ContextConfigFieldV0::ChangeRetention,
                })?;
            let actual_change_retention = fact.change_config.retention().retention_horizon_ns();
            if actual_change_retention < required_change_retention {
                return Err(ContextCompileConfigErrorV0::InsufficientRetention {
                    field: ContextConfigFieldV0::ChangeRetention,
                    minimum_ns: required_change_retention,
                    actual_ns: actual_change_retention,
                });
            }
            if let Some(preceding) = &fact.preceding {
                let required_preceding_retention =
                    lookback_ns.checked_add(preceding.lookback_ns).ok_or(
                        ContextCompileConfigErrorV0::ArithmeticOverflow {
                            field: ContextConfigFieldV0::PrecedingRetention,
                        },
                    )?;
                let actual_preceding_retention = preceding.retention.retention_horizon_ns();
                if actual_preceding_retention < required_preceding_retention {
                    return Err(ContextCompileConfigErrorV0::InsufficientRetention {
                        field: ContextConfigFieldV0::PrecedingRetention,
                        minimum_ns: required_preceding_retention,
                        actual_ns: actual_preceding_retention,
                    });
                }
            }
        }
        for fact in &interval_ratio_facts {
            let actual_retention = fact.retention.retention_horizon_ns();
            if actual_retention < lookback_ns {
                return Err(ContextCompileConfigErrorV0::InsufficientRetention {
                    field: ContextConfigFieldV0::RatioRetention,
                    minimum_ns: lookback_ns,
                    actual_ns: actual_retention,
                });
            }
        }

        let logical_bytes = request_logical_bytes(key, &scope, &facts, &interval_ratio_facts)?;
        if logical_bytes > MAX_CONTEXT_REQUEST_BYTES_V0 {
            return Err(ContextCompileConfigErrorV0::AboveMaximum {
                field: ContextConfigFieldV0::RequestBytes,
                maximum: MAX_CONTEXT_REQUEST_BYTES_V0,
                actual: logical_bytes,
            });
        }

        Ok(Self {
            version,
            as_of_time_ns,
            lookback_ns,
            byte_budget,
            key: try_owned_bytes(key)?,
            scope,
            facts,
            interval_ratio_facts,
            logical_bytes,
        })
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub const fn as_of_time_ns(&self) -> i64 {
        self.as_of_time_ns
    }

    pub const fn lookback_ns(&self) -> u64 {
        self.lookback_ns
    }

    pub const fn byte_budget(&self) -> usize {
        self.byte_budget
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn scope(&self) -> &[ContextScopePredicateV0] {
        &self.scope
    }

    pub fn facts(&self) -> &[ProjectedChangeFactSpecV0] {
        &self.facts
    }

    pub fn interval_ratio_facts(&self) -> &[IntervalRatioPpmFactSpecV0] {
        &self.interval_ratio_facts
    }

    pub const fn logical_bytes(&self) -> usize {
        self.logical_bytes
    }
}

impl fmt::Debug for ContextCompileRequestV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContextCompileRequestV0")
            .field("version", &self.version)
            .field("as_of_time_ns", &"<redacted>")
            .field("lookback_ns", &self.lookback_ns)
            .field("byte_budget", &self.byte_budget)
            .field("key_byte_len", &self.key.len())
            .field("scope_count", &self.scope.len())
            .field("projected_fact_count", &self.facts.len())
            .field("ratio_fact_count", &self.interval_ratio_facts.len())
            .field("logical_bytes", &self.logical_bytes)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextEvidenceRoleV0 {
    Previous,
    Current,
    Preceding,
    RatioDenominator,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextScopeIssueV0 {
    Missing,
    ExpectedUtf8 { actual: ScalarKindV0 },
    Unequal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContextSelectedFieldIssueV0 {
    Missing,
}

#[derive(Clone, Eq, PartialEq)]
pub enum ContextCompileErrorV0 {
    QueryOutsideEpoch,
    QueryStartArithmetic,
    HistoricalAsOfUnavailable,
    RequestedEvidenceExpired,
    EvidenceOutsideReachback,
    ScopeContract {
        role: ContextEvidenceRoleV0,
        issue: ContextScopeIssueV0,
    },
    SelectedFieldContract {
        issue: ContextSelectedFieldIssueV0,
    },
    ProjectedView(ProjectedFieldChangeMaterializationErrorV0),
    PrecedingView(PrecedingEventSelectorErrorV0),
    IntervalRatioView(IntervalRatioPpmMaterializationErrorV0),
    AdmissionStateHash(AdmissionStateHashErrorV0),
    PrecedingConfigInvariant,
    IntervalRatioConfigInvariant,
    CandidateByteCapacity {
        maximum: usize,
        attempted: usize,
    },
    FactByteCapacity {
        maximum: usize,
        attempted: usize,
    },
    MetadataByteCapacity {
        maximum: usize,
        attempted: usize,
    },
    BudgetTooSmall {
        minimum: usize,
        actual: usize,
    },
    ArithmeticOverflow,
    Allocation,
    EncodingInvariant,
}

impl fmt::Debug for ContextCompileErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueryOutsideEpoch => formatter.write_str("QueryOutsideEpoch"),
            Self::QueryStartArithmetic => formatter.write_str("QueryStartArithmetic"),
            Self::HistoricalAsOfUnavailable => formatter.write_str("HistoricalAsOfUnavailable"),
            Self::RequestedEvidenceExpired => formatter.write_str("RequestedEvidenceExpired"),
            Self::EvidenceOutsideReachback => formatter.write_str("EvidenceOutsideReachback"),
            Self::ScopeContract { role, issue } => formatter
                .debug_struct("ScopeContract")
                .field("role", role)
                .field("issue", issue)
                .finish(),
            Self::SelectedFieldContract { issue } => formatter
                .debug_struct("SelectedFieldContract")
                .field("issue", issue)
                .finish(),
            Self::ProjectedView(error) => {
                formatter.debug_tuple("ProjectedView").field(error).finish()
            }
            Self::PrecedingView(error) => {
                formatter.debug_tuple("PrecedingView").field(error).finish()
            }
            Self::IntervalRatioView(error) => formatter
                .debug_tuple("IntervalRatioView")
                .field(error)
                .finish(),
            Self::AdmissionStateHash(error) => formatter
                .debug_tuple("AdmissionStateHash")
                .field(error)
                .finish(),
            Self::PrecedingConfigInvariant => formatter.write_str("PrecedingConfigInvariant"),
            Self::IntervalRatioConfigInvariant => {
                formatter.write_str("IntervalRatioConfigInvariant")
            }
            Self::CandidateByteCapacity { maximum, attempted } => formatter
                .debug_struct("CandidateByteCapacity")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::FactByteCapacity { maximum, attempted } => formatter
                .debug_struct("FactByteCapacity")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::MetadataByteCapacity { maximum, attempted } => formatter
                .debug_struct("MetadataByteCapacity")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::BudgetTooSmall { minimum, actual } => formatter
                .debug_struct("BudgetTooSmall")
                .field("minimum", minimum)
                .field("actual", actual)
                .finish(),
            Self::ArithmeticOverflow => formatter.write_str("ArithmeticOverflow"),
            Self::Allocation => formatter.write_str("Allocation"),
            Self::EncodingInvariant => formatter.write_str("EncodingInvariant"),
        }
    }
}

impl fmt::Display for ContextCompileErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for ContextCompileErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub struct ContextCompileResponseV0 {
    bytes: Vec<u8>,
    candidate_count: usize,
    included_count: usize,
    omitted_count: usize,
    source_finalized: bool,
    query_closed: bool,
}

impl ContextCompileResponseV0 {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub const fn candidate_count(&self) -> usize {
        self.candidate_count
    }

    pub const fn included_count(&self) -> usize {
        self.included_count
    }

    pub const fn omitted_count(&self) -> usize {
        self.omitted_count
    }

    pub const fn source_finalized(&self) -> bool {
        self.source_finalized
    }

    pub const fn query_closed(&self) -> bool {
        self.query_closed
    }
}

impl fmt::Debug for ContextCompileResponseV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ContextCompileResponseV0")
            .field("byte_len", &self.bytes.len())
            .field("candidate_count", &self.candidate_count)
            .field("included_count", &self.included_count)
            .field("omitted_count", &self.omitted_count)
            .field("source_finalized", &self.source_finalized)
            .field("query_closed", &self.query_closed)
            .finish()
    }
}

pub fn compile_context_v0(
    admission: &AdmissionStateV0,
    request: &ContextCompileRequestV0,
) -> Result<ContextCompileResponseV0, ContextCompileErrorV0> {
    let epoch_start_ns = admission.config().epoch_start_ns();
    let epoch_end_ns = admission.config().epoch_end_ns();
    if request.as_of_time_ns <= epoch_start_ns || request.as_of_time_ns > epoch_end_ns {
        return Err(ContextCompileErrorV0::QueryOutsideEpoch);
    }
    let query_start_ns = subtract_duration(request.as_of_time_ns, request.lookback_ns)
        .ok_or(ContextCompileErrorV0::QueryStartArithmetic)?;
    if query_start_ns < epoch_start_ns {
        return Err(ContextCompileErrorV0::QueryOutsideEpoch);
    }

    let source_finalized = admission.is_finalized();
    let query_closed = source_finalized
        || admission
            .watermark_ns()
            .is_some_and(|watermark| watermark >= request.as_of_time_ns);

    let mut candidates = Vec::new();
    let requested_fact_count = request
        .facts
        .len()
        .checked_add(request.interval_ratio_facts.len())
        .ok_or(ContextCompileErrorV0::ArithmeticOverflow)?;
    candidates
        .try_reserve_exact(requested_fact_count)
        .map_err(|_| ContextCompileErrorV0::Allocation)?;
    let mut candidate_bytes = 0usize;
    for fact in &request.facts {
        let Some(candidate) =
            compile_projected_candidate(admission, request, fact, query_start_ns, epoch_start_ns)?
        else {
            continue;
        };
        push_candidate(&mut candidates, &mut candidate_bytes, candidate)?;
    }
    if !request.interval_ratio_facts.is_empty() {
        let admission_state_hash = hash_admission_state_v0(admission)
            .map_err(ContextCompileErrorV0::AdmissionStateHash)?;
        for fact in &request.interval_ratio_facts {
            let Some(candidate) = compile_interval_ratio_candidate(
                admission,
                request,
                fact,
                query_start_ns,
                &admission_state_hash,
            )?
            else {
                continue;
            };
            push_candidate(&mut candidates, &mut candidate_bytes, candidate)?;
        }
    }

    let empty = render_response(
        request,
        query_start_ns,
        source_finalized,
        query_closed,
        &candidates,
        0,
        MAX_CONTEXT_OUTPUT_BYTES_V0,
    )
    .map_err(map_metadata_write_error)?;
    let minimum = empty.len();
    if request.byte_budget < minimum {
        return Err(ContextCompileErrorV0::BudgetTooSmall {
            minimum,
            actual: request.byte_budget,
        });
    }

    let candidate_count = candidates.len();
    let mut included_count = 0usize;
    let mut included_bytes = 0usize;
    for candidate in &candidates {
        let next_count = included_count
            .checked_add(1)
            .ok_or(ContextCompileErrorV0::ArithmeticOverflow)?;
        let next_bytes = included_bytes
            .checked_add(candidate.len())
            .and_then(|value| value.checked_add(usize::from(included_count > 0)))
            .ok_or(ContextCompileErrorV0::ArithmeticOverflow)?;
        let next_size = adjusted_response_size(minimum, candidate_count, next_count, next_bytes)?;
        if next_size > request.byte_budget {
            break;
        }
        included_count = next_count;
        included_bytes = next_bytes;
    }

    let omitted_count = candidate_count - included_count;
    let bytes = render_response(
        request,
        query_start_ns,
        source_finalized,
        query_closed,
        &candidates,
        included_count,
        request.byte_budget,
    )
    .map_err(map_response_write_error)?;
    let expected_size =
        adjusted_response_size(minimum, candidate_count, included_count, included_bytes)?;
    if bytes.len() != expected_size || bytes.len() > request.byte_budget {
        return Err(ContextCompileErrorV0::EncodingInvariant);
    }

    Ok(ContextCompileResponseV0 {
        bytes,
        candidate_count,
        included_count,
        omitted_count,
        source_finalized,
        query_closed,
    })
}

fn push_candidate(
    candidates: &mut Vec<Vec<u8>>,
    candidate_bytes: &mut usize,
    candidate: Vec<u8>,
) -> Result<(), ContextCompileErrorV0> {
    let attempted = candidate_bytes
        .checked_add(candidate.len())
        .ok_or(ContextCompileErrorV0::ArithmeticOverflow)?;
    if attempted > MAX_CONTEXT_CANDIDATE_BYTES_V0 {
        return Err(ContextCompileErrorV0::CandidateByteCapacity {
            maximum: MAX_CONTEXT_CANDIDATE_BYTES_V0,
            attempted,
        });
    }
    *candidate_bytes = attempted;
    candidates.push(candidate);
    Ok(())
}

fn compile_projected_candidate(
    admission: &AdmissionStateV0,
    request: &ContextCompileRequestV0,
    fact: &ProjectedChangeFactSpecV0,
    query_start_ns: i64,
    epoch_start_ns: i64,
) -> Result<Option<Vec<u8>>, ContextCompileErrorV0> {
    let evidence_start_ns = subtract_duration(query_start_ns, fact.evidence_reachback_ns)
        .unwrap_or(i64::MIN)
        .max(epoch_start_ns);
    let view = admission
        .materialize_projected_field_changes(fact.change_config.clone())
        .map_err(ContextCompileErrorV0::ProjectedView)?;
    if view
        .retention_cutoff_event_time_ns()
        .is_some_and(|cutoff| cutoff > evidence_start_ns)
    {
        return Err(ContextCompileErrorV0::RequestedEvidenceExpired);
    }
    let Some(observation) = view.get(&request.key, &fact.discriminator) else {
        return Ok(None);
    };
    if observation.current_event().event_time_ns() >= request.as_of_time_ns {
        return Err(ContextCompileErrorV0::HistoricalAsOfUnavailable);
    }
    if observation.current_event().event_time_ns() < query_start_ns {
        return Ok(None);
    }
    if observation.previous_event().event_time_ns() < evidence_start_ns {
        return Err(ContextCompileErrorV0::EvidenceOutsideReachback);
    }

    verify_scope(
        observation.previous_event(),
        &request.scope,
        ContextEvidenceRoleV0::Previous,
    )?;
    verify_scope(
        observation.current_event(),
        &request.scope,
        ContextEvidenceRoleV0::Current,
    )?;

    let preceding = if let Some(spec) = &fact.preceding {
        let config = PrecedingEventSelectorConfigV0::new(
            PRECEDING_EVENT_SELECTOR_VERSION_V0,
            spec.retention,
            spec.schema_id.clone(),
            &request.key,
            observation.current_event().event_time_ns(),
            spec.lookback_ns,
        )
        .map_err(|_| ContextCompileErrorV0::PrecedingConfigInvariant)?;
        let preceding_view = admission
            .select_preceding_event(config)
            .map_err(ContextCompileErrorV0::PrecedingView)?;
        if let Some(candidate) = preceding_view.candidate() {
            verify_scope(candidate, &request.scope, ContextEvidenceRoleV0::Preceding)?;
            let selected = event_field(candidate, &spec.selected_field).ok_or(
                ContextCompileErrorV0::SelectedFieldContract {
                    issue: ContextSelectedFieldIssueV0::Missing,
                },
            )?;
            Some((candidate, selected))
        } else {
            None
        }
    } else {
        None
    };

    encode_candidate(fact, observation, preceding).map(Some)
}

fn encode_candidate(
    fact: &ProjectedChangeFactSpecV0,
    observation: decant_state::ProjectedFieldChangeObservationV0<'_>,
    preceding: Option<(&EventEnvelopeV0, &ScalarValueV0)>,
) -> Result<Vec<u8>, ContextCompileErrorV0> {
    let mut writer = BoundedJsonWriter::new(MAX_CONTEXT_FACT_BYTES_V0);
    writer.push_static(b"{\"kind\":\"projected_scalar_change\",\"fact_id\":")?;
    writer.push_json_string(fact.fact_id.as_str())?;
    writer.push_static(b",\"schema_id\":")?;
    writer.push_json_string(fact.change_config.schema_id().as_str())?;
    writer.push_static(b",\"discriminator\":")?;
    writer.push_json_string(&fact.discriminator)?;
    writer.push_static(b",\"previous\":{\"event_time_ns\":")?;
    writer.push_i64(observation.previous_event().event_time_ns())?;
    writer.push_static(b",\"value\":")?;
    writer.push_scalar(observation.previous_value())?;
    writer.push_static(b",\"provenance\":")?;
    writer.push_json_string(observation.previous_event().provenance().as_str())?;
    writer.push_static(b"},\"current\":{\"event_time_ns\":")?;
    writer.push_i64(observation.current_event().event_time_ns())?;
    writer.push_static(b",\"value\":")?;
    writer.push_scalar(observation.current_value())?;
    writer.push_static(b",\"provenance\":")?;
    writer.push_json_string(observation.current_event().provenance().as_str())?;
    writer.push_static(b"},\"preceding_evidence\":")?;
    if let Some((event, selected)) = preceding {
        writer.push_static(b"{\"relation\":\"strictly_preceding_not_causal\",\"schema_id\":")?;
        writer.push_json_string(event.schema_id().as_str())?;
        writer.push_static(b",\"event_time_ns\":")?;
        writer.push_i64(event.event_time_ns())?;
        writer.push_static(b",\"selected\":")?;
        writer.push_scalar(selected)?;
        writer.push_static(b",\"provenance\":")?;
        writer.push_json_string(event.provenance().as_str())?;
        writer.push_static(b"}")?;
    } else {
        writer.push_static(b"null")?;
    }
    writer.push_static(b"}")?;
    Ok(writer.finish())
}

fn compile_interval_ratio_candidate(
    admission: &AdmissionStateV0,
    request: &ContextCompileRequestV0,
    fact: &IntervalRatioPpmFactSpecV0,
    query_start_ns: i64,
    admission_state_hash: &AdmissionStateHashV0,
) -> Result<Option<Vec<u8>>, ContextCompileErrorV0> {
    let config = IntervalRatioPpmConfigV0::new(
        INTERVAL_RATIO_PPM_VERSION_V0,
        fact.retention,
        fact.schema_id.clone(),
        &request.key,
        &fact.predicate_field,
        fact.predicate_value.clone(),
        query_start_ns,
        request.as_of_time_ns,
        fact.evidence_limit,
    )
    .map_err(|_| ContextCompileErrorV0::IntervalRatioConfigInvariant)?;
    let view = admission
        .materialize_interval_ratio_ppm(config)
        .map_err(ContextCompileErrorV0::IntervalRatioView)?;
    for contributor in view.denominator_events() {
        verify_scope(
            contributor,
            &request.scope,
            ContextEvidenceRoleV0::RatioDenominator,
        )?;
    }
    if view.observation().is_none() {
        return Ok(None);
    }
    encode_interval_ratio_candidate(fact, &view, admission_state_hash).map(Some)
}

fn encode_interval_ratio_candidate(
    fact: &IntervalRatioPpmFactSpecV0,
    view: &IntervalRatioPpmViewV0<'_>,
    admission_state_hash: &AdmissionStateHashV0,
) -> Result<Vec<u8>, ContextCompileErrorV0> {
    let observation = view
        .observation()
        .ok_or(ContextCompileErrorV0::EncodingInvariant)?;
    let usage = view.usage();
    let mut writer = BoundedJsonWriter::new(MAX_CONTEXT_FACT_BYTES_V0);
    writer.push_static(b"{\"kind\":\"interval_ratio_ppm\",\"fact_id\":")?;
    writer.push_json_string(fact.fact_id.as_str())?;
    writer.push_static(b",\"schema_id\":")?;
    writer.push_json_string(fact.schema_id.as_str())?;
    writer.push_static(b",\"predicate\":{\"field\":")?;
    writer.push_json_string(&fact.predicate_field)?;
    writer.push_static(b",\"equals\":")?;
    writer.push_scalar(&fact.predicate_value)?;
    writer.push_static(b"},\"interval\":{\"start_event_time_ns\":")?;
    writer.push_i64(view.config().interval_start_ns())?;
    writer.push_static(b",\"end_exclusive_event_time_ns\":")?;
    writer.push_i64(view.config().interval_end_ns())?;
    writer.push_static(b"},\"ratio\":{\"numerator\":")?;
    writer.push_u64(observation.numerator())?;
    writer.push_static(b",\"denominator\":")?;
    writer.push_u64(observation.denominator())?;
    writer.push_static(b",\"rate_ppm\":")?;
    writer.push_u64(observation.rate_ppm())?;
    writer.push_static(b",\"remainder\":")?;
    writer.push_u64(observation.remainder())?;
    writer.push_static(
        b"},\"derivation\":{\"operator\":\"interval_ratio_ppm_v0\",\"admission_state_hash\":\"sha256:v0:",
    )?;
    writer.push_hex(admission_state_hash.as_bytes())?;
    writer.push_static(b"\",\"numerator_evidence_count\":")?;
    writer.push_usize(usage.numerator_examples_selected())?;
    writer.push_static(b",\"numerator_evidence_omitted\":")?;
    writer.push_usize(usage.numerator_examples_omitted())?;
    writer.push_static(b",\"numerator_evidence\":[")?;
    for (index, event) in view.numerator_examples().enumerate() {
        if index > 0 {
            writer.push_static(b",")?;
        }
        writer.push_static(b"{\"event_time_ns\":")?;
        writer.push_i64(event.event_time_ns())?;
        writer.push_static(b",\"provenance\":")?;
        writer.push_json_string(event.provenance().as_str())?;
        writer.push_static(b"}")?;
    }
    writer.push_static(b"]}}")?;
    Ok(writer.finish())
}

#[allow(clippy::too_many_arguments)]
fn render_response(
    request: &ContextCompileRequestV0,
    query_start_ns: i64,
    source_finalized: bool,
    query_closed: bool,
    candidates: &[Vec<u8>],
    included_count: usize,
    maximum: usize,
) -> Result<Vec<u8>, JsonWriteError> {
    let candidate_count = candidates.len();
    let omitted_count = candidate_count - included_count;
    let mut writer = BoundedJsonWriter::new(maximum);
    writer.push_static(
        b"{\"format\":\"decant-context\",\"version\":0,\"query\":{\"as_of_time_ns\":",
    )?;
    writer.push_i64(request.as_of_time_ns)?;
    writer.push_static(b",\"start_time_ns\":")?;
    writer.push_i64(query_start_ns)?;
    writer.push_static(b",\"end_exclusive_time_ns\":")?;
    writer.push_i64(request.as_of_time_ns)?;
    writer.push_static(b",\"entity_key_hex\":\"")?;
    writer.push_hex(&request.key)?;
    writer.push_static(b"\",\"scope\":[")?;
    for (index, predicate) in request.scope.iter().enumerate() {
        if index > 0 {
            writer.push_static(b",")?;
        }
        writer.push_static(b"{\"field\":")?;
        writer.push_json_string(&predicate.field)?;
        writer.push_static(b",\"value\":")?;
        writer.push_json_string(&predicate.value)?;
        writer.push_static(b"}")?;
    }
    writer.push_static(b"]},\"status\":{\"source_finalized\":")?;
    writer.push_bool(source_finalized)?;
    writer.push_static(b",\"query_closed\":")?;
    writer.push_bool(query_closed)?;
    writer.push_static(b"},\"candidate_count\":")?;
    writer.push_usize(candidate_count)?;
    writer.push_static(b",\"included_count\":")?;
    writer.push_usize(included_count)?;
    writer.push_static(b",\"omitted_count\":")?;
    writer.push_usize(omitted_count)?;
    writer.push_static(b",\"facts\":[")?;
    for (index, candidate) in candidates.iter().take(included_count).enumerate() {
        if index > 0 {
            writer.push_static(b",")?;
        }
        writer.push_bytes(candidate)?;
    }
    writer.push_static(b"]}")?;
    Ok(writer.finish())
}

fn adjusted_response_size(
    empty_size: usize,
    candidate_count: usize,
    included_count: usize,
    included_bytes: usize,
) -> Result<usize, ContextCompileErrorV0> {
    let mut size = empty_size
        .checked_add(included_bytes)
        .ok_or(ContextCompileErrorV0::ArithmeticOverflow)?;
    size = replace_decimal_width(size, 0, included_count)?;
    size = replace_decimal_width(size, candidate_count, candidate_count - included_count)?;
    Ok(size)
}

fn replace_decimal_width(
    size: usize,
    old: usize,
    new: usize,
) -> Result<usize, ContextCompileErrorV0> {
    let old_width = decimal_width(old);
    let new_width = decimal_width(new);
    if new_width >= old_width {
        size.checked_add(new_width - old_width)
            .ok_or(ContextCompileErrorV0::ArithmeticOverflow)
    } else {
        size.checked_sub(old_width - new_width)
            .ok_or(ContextCompileErrorV0::ArithmeticOverflow)
    }
}

fn decimal_width(mut value: usize) -> usize {
    let mut width = 1;
    while value >= 10 {
        value /= 10;
        width += 1;
    }
    width
}

fn verify_scope(
    event: &EventEnvelopeV0,
    scope: &[ContextScopePredicateV0],
    role: ContextEvidenceRoleV0,
) -> Result<(), ContextCompileErrorV0> {
    for predicate in scope {
        let Some(value) = event_field(event, &predicate.field) else {
            return Err(ContextCompileErrorV0::ScopeContract {
                role,
                issue: ContextScopeIssueV0::Missing,
            });
        };
        let Some(value) = value.as_utf8() else {
            return Err(ContextCompileErrorV0::ScopeContract {
                role,
                issue: ContextScopeIssueV0::ExpectedUtf8 {
                    actual: value.kind(),
                },
            });
        };
        if value != predicate.value {
            return Err(ContextCompileErrorV0::ScopeContract {
                role,
                issue: ContextScopeIssueV0::Unequal,
            });
        }
    }
    Ok(())
}

fn event_field<'a>(event: &'a EventEnvelopeV0, field: &str) -> Option<&'a ScalarValueV0> {
    event
        .payload()
        .fields()
        .find_map(|(candidate, value)| (candidate == field).then_some(value))
}

fn fact_id_families_overlap(
    projected: &[ProjectedChangeFactSpecV0],
    ratios: &[IntervalRatioPpmFactSpecV0],
) -> bool {
    let mut projected_index = 0;
    let mut ratio_index = 0;
    while projected_index < projected.len() && ratio_index < ratios.len() {
        match projected[projected_index]
            .fact_id
            .cmp(&ratios[ratio_index].fact_id)
        {
            std::cmp::Ordering::Less => projected_index += 1,
            std::cmp::Ordering::Greater => ratio_index += 1,
            std::cmp::Ordering::Equal => return true,
        }
    }
    false
}

fn request_logical_bytes(
    key: &[u8],
    scope: &[ContextScopePredicateV0],
    facts: &[ProjectedChangeFactSpecV0],
    interval_ratio_facts: &[IntervalRatioPpmFactSpecV0],
) -> Result<usize, ContextCompileConfigErrorV0> {
    let mut bytes = VERSION_WIDTH
        .checked_add(TIMESTAMP_WIDTH)
        .and_then(|value| value.checked_add(DURATION_WIDTH))
        .and_then(|value| value.checked_add(BUDGET_WIDTH))
        .and_then(|value| value.checked_add(LENGTH_PREFIX_WIDTH))
        .and_then(|value| value.checked_add(key.len()))
        .and_then(|value| value.checked_add(COUNT_WIDTH))
        .and_then(|value| value.checked_add(COUNT_WIDTH))
        .ok_or(ContextCompileConfigErrorV0::ArithmeticOverflow {
            field: ContextConfigFieldV0::RequestBytes,
        })?;
    for predicate in scope {
        add_owned_text(&mut bytes, &predicate.field)?;
        add_owned_text(&mut bytes, &predicate.value)?;
    }
    for fact in facts {
        bytes = bytes
            .checked_add(FACT_TAG_WIDTH)
            .and_then(|value| value.checked_add(VERSION_WIDTH))
            .and_then(|value| value.checked_add(VERSION_WIDTH))
            .and_then(|value| value.checked_add(DURATION_WIDTH))
            .and_then(|value| value.checked_add(LIMIT_WIDTH))
            .and_then(|value| value.checked_add(LIMIT_WIDTH))
            .ok_or(ContextCompileConfigErrorV0::ArithmeticOverflow {
                field: ContextConfigFieldV0::RequestBytes,
            })?;
        add_owned_text(&mut bytes, fact.fact_id.as_str())?;
        add_owned_text(&mut bytes, fact.change_config.schema_id().as_str())?;
        add_owned_text(&mut bytes, fact.change_config.discriminator_field())?;
        add_owned_text(&mut bytes, fact.change_config.value_field())?;
        add_owned_text(&mut bytes, &fact.discriminator)?;
        bytes = bytes
            .checked_add(DURATION_WIDTH)
            .and_then(|value| value.checked_add(OPTIONAL_TAG_WIDTH))
            .ok_or(ContextCompileConfigErrorV0::ArithmeticOverflow {
                field: ContextConfigFieldV0::RequestBytes,
            })?;
        if let Some(preceding) = &fact.preceding {
            bytes = bytes
                .checked_add(VERSION_WIDTH)
                .and_then(|value| value.checked_add(DURATION_WIDTH))
                .and_then(|value| value.checked_add(DURATION_WIDTH))
                .ok_or(ContextCompileConfigErrorV0::ArithmeticOverflow {
                    field: ContextConfigFieldV0::RequestBytes,
                })?;
            add_owned_text(&mut bytes, preceding.schema_id.as_str())?;
            add_owned_text(&mut bytes, &preceding.selected_field)?;
        }
    }
    for fact in interval_ratio_facts {
        bytes = bytes
            .checked_add(FACT_TAG_WIDTH)
            .and_then(|value| value.checked_add(VERSION_WIDTH))
            .and_then(|value| value.checked_add(VERSION_WIDTH))
            .and_then(|value| value.checked_add(DURATION_WIDTH))
            .and_then(|value| value.checked_add(LIMIT_WIDTH))
            .ok_or(ContextCompileConfigErrorV0::ArithmeticOverflow {
                field: ContextConfigFieldV0::RequestBytes,
            })?;
        add_owned_text(&mut bytes, fact.fact_id.as_str())?;
        add_owned_text(&mut bytes, fact.schema_id.as_str())?;
        add_owned_text(&mut bytes, &fact.predicate_field)?;
        add_owned_scalar(&mut bytes, &fact.predicate_value)?;
    }
    Ok(bytes)
}

fn add_owned_text(bytes: &mut usize, value: &str) -> Result<(), ContextCompileConfigErrorV0> {
    *bytes = bytes
        .checked_add(LENGTH_PREFIX_WIDTH)
        .and_then(|current| current.checked_add(value.len()))
        .ok_or(ContextCompileConfigErrorV0::ArithmeticOverflow {
            field: ContextConfigFieldV0::RequestBytes,
        })?;
    Ok(())
}

fn add_owned_scalar(
    bytes: &mut usize,
    value: &ScalarValueV0,
) -> Result<(), ContextCompileConfigErrorV0> {
    let content_bytes = match value.kind() {
        ScalarKindV0::Null => 0,
        ScalarKindV0::Bool => 1,
        ScalarKindV0::I64 | ScalarKindV0::U64 => 8,
        ScalarKindV0::Utf8 => LENGTH_PREFIX_WIDTH
            .checked_add(
                value
                    .as_utf8()
                    .ok_or(ContextCompileConfigErrorV0::ArithmeticOverflow {
                        field: ContextConfigFieldV0::RequestBytes,
                    })?
                    .len(),
            )
            .ok_or(ContextCompileConfigErrorV0::ArithmeticOverflow {
                field: ContextConfigFieldV0::RequestBytes,
            })?,
        ScalarKindV0::Bytes => LENGTH_PREFIX_WIDTH
            .checked_add(
                value
                    .as_bytes()
                    .ok_or(ContextCompileConfigErrorV0::ArithmeticOverflow {
                        field: ContextConfigFieldV0::RequestBytes,
                    })?
                    .len(),
            )
            .ok_or(ContextCompileConfigErrorV0::ArithmeticOverflow {
                field: ContextConfigFieldV0::RequestBytes,
            })?,
    };
    *bytes = bytes
        .checked_add(SCALAR_TAG_WIDTH)
        .and_then(|current| current.checked_add(content_bytes))
        .ok_or(ContextCompileConfigErrorV0::ArithmeticOverflow {
            field: ContextConfigFieldV0::RequestBytes,
        })?;
    Ok(())
}

fn validate_positive_duration(
    value: u64,
    maximum: u64,
    field: ContextConfigFieldV0,
) -> Result<(), ContextCompileConfigErrorV0> {
    if value == 0 {
        return Err(ContextCompileConfigErrorV0::Zero { field });
    }
    if value > maximum {
        return Err(ContextCompileConfigErrorV0::AboveMaximum {
            field,
            maximum: usize::try_from(maximum).unwrap_or(usize::MAX),
            actual: usize::try_from(value).unwrap_or(usize::MAX),
        });
    }
    Ok(())
}

fn validate_positive_usize(
    value: usize,
    maximum: usize,
    field: ContextConfigFieldV0,
) -> Result<(), ContextCompileConfigErrorV0> {
    if value == 0 {
        return Err(ContextCompileConfigErrorV0::Zero { field });
    }
    if value > maximum {
        return Err(ContextCompileConfigErrorV0::AboveMaximum {
            field,
            maximum,
            actual: value,
        });
    }
    Ok(())
}

fn validate_identifier(
    value: &str,
    field: ContextConfigFieldV0,
) -> Result<(), ContextCompileConfigErrorV0> {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return Err(ContextCompileConfigErrorV0::Zero { field });
    }
    if bytes.len() > MAX_IDENTIFIER_BYTES {
        return Err(ContextCompileConfigErrorV0::AboveMaximum {
            field,
            maximum: MAX_IDENTIFIER_BYTES,
            actual: bytes.len(),
        });
    }
    let first = bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit();
    let rest = bytes[1..].iter().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
    });
    if !first || !rest {
        return Err(ContextCompileConfigErrorV0::InvalidSyntax { field });
    }
    Ok(())
}

fn validate_field_name(
    value: &str,
    field: ContextConfigFieldV0,
) -> Result<(), ContextCompileConfigErrorV0> {
    let bytes = value.as_bytes();
    if bytes.is_empty() {
        return Err(ContextCompileConfigErrorV0::Zero { field });
    }
    if bytes.len() > MAX_FIELD_NAME_BYTES {
        return Err(ContextCompileConfigErrorV0::AboveMaximum {
            field,
            maximum: MAX_FIELD_NAME_BYTES,
            actual: bytes.len(),
        });
    }
    let first = bytes[0].is_ascii_lowercase() || bytes[0] == b'_';
    let rest = bytes[1..].iter().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
    });
    if !first || !rest {
        return Err(ContextCompileConfigErrorV0::InvalidSyntax { field });
    }
    Ok(())
}

fn validate_bounded_text(
    value: &str,
    field: ContextConfigFieldV0,
    allow_empty: bool,
) -> Result<(), ContextCompileConfigErrorV0> {
    if value.is_empty() && !allow_empty {
        return Err(ContextCompileConfigErrorV0::Zero { field });
    }
    if value.len() > MAX_SCALAR_BYTES {
        return Err(ContextCompileConfigErrorV0::AboveMaximum {
            field,
            maximum: MAX_SCALAR_BYTES,
            actual: value.len(),
        });
    }
    Ok(())
}

fn try_owned_str(value: &str) -> Result<String, ContextCompileConfigErrorV0> {
    let mut owned = String::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| ContextCompileConfigErrorV0::Allocation)?;
    owned.push_str(value);
    Ok(owned)
}

fn try_owned_bytes(value: &[u8]) -> Result<Vec<u8>, ContextCompileConfigErrorV0> {
    let mut owned = Vec::new();
    owned
        .try_reserve_exact(value.len())
        .map_err(|_| ContextCompileConfigErrorV0::Allocation)?;
    owned.extend_from_slice(value);
    Ok(owned)
}

fn subtract_duration(timestamp: i64, duration: u64) -> Option<i64> {
    let result = i128::from(timestamp) - i128::from(duration);
    i64::try_from(result).ok()
}

fn map_metadata_write_error(error: JsonWriteError) -> ContextCompileErrorV0 {
    match error {
        JsonWriteError::Capacity { maximum, attempted } => {
            ContextCompileErrorV0::MetadataByteCapacity { maximum, attempted }
        }
        JsonWriteError::Arithmetic => ContextCompileErrorV0::ArithmeticOverflow,
        JsonWriteError::Allocation => ContextCompileErrorV0::Allocation,
    }
}

fn map_response_write_error(error: JsonWriteError) -> ContextCompileErrorV0 {
    match error {
        JsonWriteError::Capacity { .. } => ContextCompileErrorV0::EncodingInvariant,
        JsonWriteError::Arithmetic => ContextCompileErrorV0::ArithmeticOverflow,
        JsonWriteError::Allocation => ContextCompileErrorV0::Allocation,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum JsonWriteError {
    Capacity { maximum: usize, attempted: usize },
    Arithmetic,
    Allocation,
}

impl From<JsonWriteError> for ContextCompileErrorV0 {
    fn from(error: JsonWriteError) -> Self {
        match error {
            JsonWriteError::Capacity { maximum, attempted } => {
                Self::FactByteCapacity { maximum, attempted }
            }
            JsonWriteError::Arithmetic => Self::ArithmeticOverflow,
            JsonWriteError::Allocation => Self::Allocation,
        }
    }
}

struct BoundedJsonWriter {
    bytes: Vec<u8>,
    maximum: usize,
}

impl BoundedJsonWriter {
    const fn new(maximum: usize) -> Self {
        Self {
            bytes: Vec::new(),
            maximum,
        }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    fn reserve(&mut self, additional: usize) -> Result<(), JsonWriteError> {
        let attempted = self
            .bytes
            .len()
            .checked_add(additional)
            .ok_or(JsonWriteError::Arithmetic)?;
        if attempted > self.maximum {
            return Err(JsonWriteError::Capacity {
                maximum: self.maximum,
                attempted,
            });
        }
        self.bytes
            .try_reserve_exact(additional)
            .map_err(|_| JsonWriteError::Allocation)
    }

    fn push_static(&mut self, value: &'static [u8]) -> Result<(), JsonWriteError> {
        self.push_bytes(value)
    }

    fn push_bytes(&mut self, value: &[u8]) -> Result<(), JsonWriteError> {
        self.reserve(value.len())?;
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    fn push_bool(&mut self, value: bool) -> Result<(), JsonWriteError> {
        self.push_static(if value { b"true" } else { b"false" })
    }

    fn push_usize(&mut self, value: usize) -> Result<(), JsonWriteError> {
        let value = u64::try_from(value).map_err(|_| JsonWriteError::Arithmetic)?;
        self.push_u64(value)
    }

    fn push_i64(&mut self, value: i64) -> Result<(), JsonWriteError> {
        if value < 0 {
            self.push_static(b"-")?;
        }
        self.push_u64(value.unsigned_abs())
    }

    fn push_u64(&mut self, mut value: u64) -> Result<(), JsonWriteError> {
        let mut digits = [0u8; 20];
        let mut cursor = digits.len();
        loop {
            cursor -= 1;
            digits[cursor] =
                b'0' + u8::try_from(value % 10).map_err(|_| JsonWriteError::Arithmetic)?;
            value /= 10;
            if value == 0 {
                break;
            }
        }
        self.push_bytes(&digits[cursor..])
    }

    fn push_json_string(&mut self, value: &str) -> Result<(), JsonWriteError> {
        let mut encoded_len = 2usize;
        for byte in value.as_bytes() {
            let width = match byte {
                b'"' | b'\\' | 0x08 | 0x09 | 0x0a | 0x0c | 0x0d => 2,
                0x00..=0x1f => 6,
                _ => 1,
            };
            encoded_len = encoded_len
                .checked_add(width)
                .ok_or(JsonWriteError::Arithmetic)?;
        }
        self.reserve(encoded_len)?;
        self.bytes.push(b'"');
        for byte in value.as_bytes() {
            match byte {
                b'"' => self.bytes.extend_from_slice(b"\\\""),
                b'\\' => self.bytes.extend_from_slice(b"\\\\"),
                0x08 => self.bytes.extend_from_slice(b"\\b"),
                0x09 => self.bytes.extend_from_slice(b"\\t"),
                0x0a => self.bytes.extend_from_slice(b"\\n"),
                0x0c => self.bytes.extend_from_slice(b"\\f"),
                0x0d => self.bytes.extend_from_slice(b"\\r"),
                0x00..=0x1f => {
                    self.bytes.extend_from_slice(b"\\u00");
                    self.bytes.push(hex_digit(byte >> 4));
                    self.bytes.push(hex_digit(byte & 0x0f));
                }
                _ => self.bytes.push(*byte),
            }
        }
        self.bytes.push(b'"');
        Ok(())
    }

    fn push_hex(&mut self, value: &[u8]) -> Result<(), JsonWriteError> {
        let encoded_len = value
            .len()
            .checked_mul(2)
            .ok_or(JsonWriteError::Arithmetic)?;
        self.reserve(encoded_len)?;
        for byte in value {
            self.bytes.push(hex_digit(byte >> 4));
            self.bytes.push(hex_digit(byte & 0x0f));
        }
        Ok(())
    }

    fn push_scalar(&mut self, value: &ScalarValueV0) -> Result<(), JsonWriteError> {
        match value.kind() {
            ScalarKindV0::Null => self.push_static(b"{\"kind\":\"null\",\"value\":null}"),
            ScalarKindV0::Bool => {
                self.push_static(b"{\"kind\":\"bool\",\"value\":")?;
                self.push_bool(value.as_bool().ok_or(JsonWriteError::Arithmetic)?)?;
                self.push_static(b"}")
            }
            ScalarKindV0::I64 => {
                self.push_static(b"{\"kind\":\"i64\",\"value\":")?;
                self.push_i64(value.as_i64().ok_or(JsonWriteError::Arithmetic)?)?;
                self.push_static(b"}")
            }
            ScalarKindV0::U64 => {
                self.push_static(b"{\"kind\":\"u64\",\"value\":")?;
                self.push_u64(value.as_u64().ok_or(JsonWriteError::Arithmetic)?)?;
                self.push_static(b"}")
            }
            ScalarKindV0::Utf8 => {
                self.push_static(b"{\"kind\":\"utf8\",\"value\":")?;
                self.push_json_string(value.as_utf8().ok_or(JsonWriteError::Arithmetic)?)?;
                self.push_static(b"}")
            }
            ScalarKindV0::Bytes => {
                self.push_static(b"{\"kind\":\"bytes\",\"value\":\"")?;
                self.push_hex(value.as_bytes().ok_or(JsonWriteError::Arithmetic)?)?;
                self.push_static(b"\"}")
            }
        }
    }
}

const fn hex_digit(value: u8) -> u8 {
    match value {
        0..=9 => b'0' + value,
        _ => b'a' + (value - 10),
    }
}
