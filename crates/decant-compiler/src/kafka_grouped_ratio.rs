//! Policy-gated payload-derived context for mapped Kafka JSON objects.

use std::error::Error;
use std::fmt;

use decant_ir::{MAX_FIELD_NAME_BYTES, MAX_IDENTIFIER_BYTES, SchemaIdV0, SourcePositionKindV0};
use decant_state::{
    ALLOWED_LATENESS_NS_V0, AdmissionStateV0, EVENT_TIME_RETENTION_VERSION_V0,
    EventTimeRetentionConfigV0, GROUPED_INTERVAL_RATIO_PPM_VERSION_V0,
    GroupedIntervalRatioLimitsV0, GroupedIntervalRatioMaterializationErrorV0,
    GroupedIntervalRatioPpmConfigV0, MAX_GROUPED_INTERVAL_RATIO_EVIDENCE_V0,
};

use super::{BoundedJsonWriter, JsonWriteError};

pub const KAFKA_FIELD_EGRESS_POLICY_VERSION_V0: u16 = 0;
pub const KAFKA_GROUPED_RATIO_CONTEXT_VERSION_V0: u16 = 0;
pub const MIN_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0: usize = 512;
pub const MAX_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0: usize = 16 * 1_024;
pub const MAX_KAFKA_FIELD_EGRESS_RULES_V0: usize = 64;
pub const MAX_KAFKA_GROUPED_RATIO_LOOKBACK_NS_V0: u64 = 10 * 60 * 1_000_000_000;

const MAX_CANDIDATE_BYTES: usize = 8 * 1_024 * 1_024;
const MAX_FACT_BYTES: usize = 16 * 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaFieldEgressActionV0 {
    Allow,
    Deny,
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaFieldEgressRuleV0 {
    field: String,
    action: KafkaFieldEgressActionV0,
}

impl KafkaFieldEgressRuleV0 {
    pub fn new(
        field: &str,
        action: KafkaFieldEgressActionV0,
    ) -> Result<Self, KafkaGroupedRatioContextConfigErrorV0> {
        validate_field(field)?;
        Ok(Self {
            field: field.to_owned(),
            action,
        })
    }

    pub fn field(&self) -> &str {
        &self.field
    }

    pub const fn action(&self) -> KafkaFieldEgressActionV0 {
        self.action
    }
}

impl fmt::Debug for KafkaFieldEgressRuleV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaFieldEgressRuleV0")
            .field("field_bytes", &self.field.len())
            .field("action", &self.action)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaFieldEgressPolicyV0 {
    rules: Vec<KafkaFieldEgressRuleV0>,
    allowed_fields: usize,
    denied_fields: usize,
}

impl KafkaFieldEgressPolicyV0 {
    pub fn new(
        version: u16,
        mut rules: Vec<KafkaFieldEgressRuleV0>,
    ) -> Result<Self, KafkaGroupedRatioContextConfigErrorV0> {
        if version != KAFKA_FIELD_EGRESS_POLICY_VERSION_V0 {
            return Err(
                KafkaGroupedRatioContextConfigErrorV0::UnsupportedPolicyVersion { actual: version },
            );
        }
        if rules.len() > MAX_KAFKA_FIELD_EGRESS_RULES_V0 {
            return Err(KafkaGroupedRatioContextConfigErrorV0::TooManyPolicyRules {
                maximum: MAX_KAFKA_FIELD_EGRESS_RULES_V0,
                actual: rules.len(),
            });
        }
        rules.sort_by(|left, right| left.field.cmp(&right.field));
        if rules.windows(2).any(|pair| pair[0].field == pair[1].field) {
            return Err(KafkaGroupedRatioContextConfigErrorV0::DuplicatePolicyField);
        }
        let allowed_fields = rules
            .iter()
            .filter(|rule| rule.action == KafkaFieldEgressActionV0::Allow)
            .count();
        let denied_fields = rules.len() - allowed_fields;
        Ok(Self {
            rules,
            allowed_fields,
            denied_fields,
        })
    }

    pub fn rules(&self) -> &[KafkaFieldEgressRuleV0] {
        &self.rules
    }

    pub const fn allowed_fields(&self) -> usize {
        self.allowed_fields
    }

    pub const fn denied_fields(&self) -> usize {
        self.denied_fields
    }

    pub fn action_for(&self, field: &str) -> Option<KafkaFieldEgressActionV0> {
        self.rules
            .binary_search_by(|rule| rule.field.as_str().cmp(field))
            .ok()
            .map(|index| self.rules[index].action)
    }
}

impl fmt::Debug for KafkaFieldEgressPolicyV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaFieldEgressPolicyV0")
            .field("version", &KAFKA_FIELD_EGRESS_POLICY_VERSION_V0)
            .field("classified_fields", &self.rules.len())
            .field("allowed_fields", &self.allowed_fields)
            .field("denied_fields", &self.denied_fields)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaGroupedRatioContextConfigErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    UnsupportedPolicyVersion {
        actual: u16,
    },
    InvalidFactId,
    InvalidField,
    DuplicateFieldRoles,
    ZeroLookback,
    LookbackAboveMaximum {
        maximum: u64,
        actual: u64,
    },
    OutputBudgetOutOfRange {
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    EvidenceLimitAboveMaximum {
        maximum: usize,
        actual: usize,
    },
    TooManyPolicyRules {
        maximum: usize,
        actual: usize,
    },
    DuplicatePolicyField,
    FieldNotAllowed,
}

impl fmt::Debug for KafkaGroupedRatioContextConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::UnsupportedPolicyVersion { actual } => formatter
                .debug_struct("UnsupportedPolicyVersion")
                .field("actual", actual)
                .finish(),
            Self::InvalidFactId => formatter.write_str("InvalidFactId"),
            Self::InvalidField => formatter.write_str("InvalidField"),
            Self::DuplicateFieldRoles => formatter.write_str("DuplicateFieldRoles"),
            Self::ZeroLookback => formatter.write_str("ZeroLookback"),
            Self::LookbackAboveMaximum { maximum, actual } => formatter
                .debug_struct("LookbackAboveMaximum")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::OutputBudgetOutOfRange {
                minimum,
                maximum,
                actual,
            } => formatter
                .debug_struct("OutputBudgetOutOfRange")
                .field("minimum", minimum)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::EvidenceLimitAboveMaximum { maximum, actual } => formatter
                .debug_struct("EvidenceLimitAboveMaximum")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::TooManyPolicyRules { maximum, actual } => formatter
                .debug_struct("TooManyPolicyRules")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::DuplicatePolicyField => formatter.write_str("DuplicatePolicyField"),
            Self::FieldNotAllowed => formatter.write_str("FieldNotAllowed"),
        }
    }
}

impl fmt::Display for KafkaGroupedRatioContextConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaGroupedRatioContextConfigErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaGroupedRatioContextRequestV0 {
    schema_id: SchemaIdV0,
    fact_id: String,
    group_field: String,
    predicate_field: String,
    predicate_value: bool,
    lookback_ns: u64,
    evidence_limit: usize,
    byte_budget: usize,
    policy: KafkaFieldEgressPolicyV0,
}

impl KafkaGroupedRatioContextRequestV0 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        version: u16,
        schema_id: SchemaIdV0,
        fact_id: &str,
        group_field: &str,
        predicate_field: &str,
        predicate_value: bool,
        lookback_ns: u64,
        evidence_limit: usize,
        byte_budget: usize,
        policy: KafkaFieldEgressPolicyV0,
    ) -> Result<Self, KafkaGroupedRatioContextConfigErrorV0> {
        if version != KAFKA_GROUPED_RATIO_CONTEXT_VERSION_V0 {
            return Err(KafkaGroupedRatioContextConfigErrorV0::UnsupportedVersion {
                actual: version,
            });
        }
        validate_fact_id(fact_id)?;
        validate_field(group_field)?;
        validate_field(predicate_field)?;
        if group_field == predicate_field {
            return Err(KafkaGroupedRatioContextConfigErrorV0::DuplicateFieldRoles);
        }
        if lookback_ns == 0 {
            return Err(KafkaGroupedRatioContextConfigErrorV0::ZeroLookback);
        }
        if lookback_ns > MAX_KAFKA_GROUPED_RATIO_LOOKBACK_NS_V0 {
            return Err(
                KafkaGroupedRatioContextConfigErrorV0::LookbackAboveMaximum {
                    maximum: MAX_KAFKA_GROUPED_RATIO_LOOKBACK_NS_V0,
                    actual: lookback_ns,
                },
            );
        }
        if evidence_limit > MAX_GROUPED_INTERVAL_RATIO_EVIDENCE_V0 {
            return Err(
                KafkaGroupedRatioContextConfigErrorV0::EvidenceLimitAboveMaximum {
                    maximum: MAX_GROUPED_INTERVAL_RATIO_EVIDENCE_V0,
                    actual: evidence_limit,
                },
            );
        }
        if !(MIN_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0
            ..=MAX_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0)
            .contains(&byte_budget)
        {
            return Err(
                KafkaGroupedRatioContextConfigErrorV0::OutputBudgetOutOfRange {
                    minimum: MIN_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0,
                    maximum: MAX_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0,
                    actual: byte_budget,
                },
            );
        }
        if policy.action_for(group_field) != Some(KafkaFieldEgressActionV0::Allow)
            || policy.action_for(predicate_field) != Some(KafkaFieldEgressActionV0::Allow)
        {
            return Err(KafkaGroupedRatioContextConfigErrorV0::FieldNotAllowed);
        }
        Ok(Self {
            schema_id,
            fact_id: fact_id.to_owned(),
            group_field: group_field.to_owned(),
            predicate_field: predicate_field.to_owned(),
            predicate_value,
            lookback_ns,
            evidence_limit,
            byte_budget,
            policy,
        })
    }

    pub const fn schema_id(&self) -> &SchemaIdV0 {
        &self.schema_id
    }

    pub fn fact_id(&self) -> &str {
        &self.fact_id
    }

    pub fn group_field(&self) -> &str {
        &self.group_field
    }

    pub fn predicate_field(&self) -> &str {
        &self.predicate_field
    }

    pub const fn predicate_value(&self) -> bool {
        self.predicate_value
    }

    pub const fn lookback_ns(&self) -> u64 {
        self.lookback_ns
    }

    pub const fn evidence_limit(&self) -> usize {
        self.evidence_limit
    }

    pub const fn byte_budget(&self) -> usize {
        self.byte_budget
    }

    pub const fn policy(&self) -> &KafkaFieldEgressPolicyV0 {
        &self.policy
    }
}

impl fmt::Debug for KafkaGroupedRatioContextRequestV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaGroupedRatioContextRequestV0")
            .field("version", &KAFKA_GROUPED_RATIO_CONTEXT_VERSION_V0)
            .field("schema_id", &"<redacted>")
            .field("fact_id_bytes", &self.fact_id.len())
            .field("group_field_bytes", &self.group_field.len())
            .field("predicate_field_bytes", &self.predicate_field.len())
            .field("predicate_value", &"<redacted>")
            .field("lookback_ns", &self.lookback_ns)
            .field("evidence_limit", &self.evidence_limit)
            .field("byte_budget", &self.byte_budget)
            .field("policy", &self.policy)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaGroupedRatioContractIssueV0 {
    SourcePositionKind,
    SourceIdentity,
    SchemaIdentity,
    IngestTime,
    Sensitivity,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaGroupedRatioContextErrorV0 {
    AdmissionContract {
        issue: KafkaGroupedRatioContractIssueV0,
    },
    QueryArithmetic,
    RetentionConfiguration,
    OperatorConfiguration,
    Operator(GroupedIntervalRatioMaterializationErrorV0),
    CandidateCapacity {
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

impl fmt::Debug for KafkaGroupedRatioContextErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AdmissionContract { issue } => formatter
                .debug_struct("AdmissionContract")
                .field("issue", issue)
                .finish(),
            Self::QueryArithmetic => formatter.write_str("QueryArithmetic"),
            Self::RetentionConfiguration => formatter.write_str("RetentionConfiguration"),
            Self::OperatorConfiguration => formatter.write_str("OperatorConfiguration"),
            Self::Operator(error) => formatter.debug_tuple("Operator").field(error).finish(),
            Self::CandidateCapacity { maximum, attempted } => formatter
                .debug_struct("CandidateCapacity")
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

impl fmt::Display for KafkaGroupedRatioContextErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaGroupedRatioContextErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaGroupedRatioContextV0 {
    bytes: Vec<u8>,
    candidate_count: usize,
    included_count: usize,
    omitted_count: usize,
    query_closed: bool,
}

impl KafkaGroupedRatioContextV0 {
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

    pub const fn query_closed(&self) -> bool {
        self.query_closed
    }
}

impl fmt::Debug for KafkaGroupedRatioContextV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaGroupedRatioContextV0")
            .field("byte_len", &self.bytes.len())
            .field("candidate_count", &self.candidate_count)
            .field("included_count", &self.included_count)
            .field("omitted_count", &self.omitted_count)
            .field("query_closed", &self.query_closed)
            .finish()
    }
}

pub fn compile_kafka_grouped_ratio_context_v0(
    admission: &AdmissionStateV0,
    request: &KafkaGroupedRatioContextRequestV0,
) -> Result<KafkaGroupedRatioContextV0, KafkaGroupedRatioContextErrorV0> {
    if admission.config().source_position_kind() != SourcePositionKindV0::Kafka {
        return Err(KafkaGroupedRatioContextErrorV0::AdmissionContract {
            issue: KafkaGroupedRatioContractIssueV0::SourcePositionKind,
        });
    }
    let mut maximum_time = None;
    for event in admission.accepted_events() {
        if event.source_id() != admission.config().source_id() {
            return Err(KafkaGroupedRatioContextErrorV0::AdmissionContract {
                issue: KafkaGroupedRatioContractIssueV0::SourceIdentity,
            });
        }
        if event.schema_id() != &request.schema_id {
            return Err(KafkaGroupedRatioContextErrorV0::AdmissionContract {
                issue: KafkaGroupedRatioContractIssueV0::SchemaIdentity,
            });
        }
        if event.ingest_time_ns() != 0 {
            return Err(KafkaGroupedRatioContextErrorV0::AdmissionContract {
                issue: KafkaGroupedRatioContractIssueV0::IngestTime,
            });
        }
        if event.sensitivity().bits() != u64::MAX {
            return Err(KafkaGroupedRatioContextErrorV0::AdmissionContract {
                issue: KafkaGroupedRatioContractIssueV0::Sensitivity,
            });
        }
        maximum_time = Some(maximum_time.map_or(event.event_time_ns(), |current: i64| {
            current.max(event.event_time_ns())
        }));
    }

    let interval = maximum_time
        .map(|maximum| {
            let end = maximum
                .checked_add(1)
                .ok_or(KafkaGroupedRatioContextErrorV0::QueryArithmetic)?;
            let unclamped = i128::from(end) - i128::from(request.lookback_ns);
            let start = i64::try_from(unclamped)
                .unwrap_or(i64::MIN)
                .max(admission.config().epoch_start_ns());
            Ok::<(i64, i64), KafkaGroupedRatioContextErrorV0>((start, end))
        })
        .transpose()?;

    let (view, query_closed) = if let Some((start, end)) = interval {
        let retention_horizon = request
            .lookback_ns
            .checked_add(ALLOWED_LATENESS_NS_V0.unsigned_abs())
            .and_then(|value| value.checked_add(1))
            .ok_or(KafkaGroupedRatioContextErrorV0::QueryArithmetic)?;
        let retention =
            EventTimeRetentionConfigV0::new(EVENT_TIME_RETENTION_VERSION_V0, retention_horizon)
                .map_err(|_| KafkaGroupedRatioContextErrorV0::RetentionConfiguration)?;
        let config = GroupedIntervalRatioPpmConfigV0::new(
            GROUPED_INTERVAL_RATIO_PPM_VERSION_V0,
            retention,
            request.schema_id.clone(),
            &request.group_field,
            &request.predicate_field,
            request.predicate_value,
            start,
            end,
            request.evidence_limit,
            GroupedIntervalRatioLimitsV0::maximum(),
        )
        .map_err(|_| KafkaGroupedRatioContextErrorV0::OperatorConfiguration)?;
        let view = admission
            .materialize_grouped_interval_ratio_ppm(config)
            .map_err(KafkaGroupedRatioContextErrorV0::Operator)?;
        let closed = admission.is_finalized()
            || admission
                .watermark_ns()
                .is_some_and(|watermark| watermark >= end);
        (Some(view), closed)
    } else {
        (None, admission.is_finalized())
    };

    let mut candidates = Vec::new();
    let candidate_count = view.as_ref().map_or(0, |value| value.groups().len());
    candidates
        .try_reserve_exact(candidate_count)
        .map_err(|_| KafkaGroupedRatioContextErrorV0::Allocation)?;
    let mut candidate_bytes = 0usize;
    if let Some(view) = &view {
        for (group, aggregate) in view.groups() {
            let candidate = encode_candidate(request, group, aggregate)?;
            candidate_bytes = candidate_bytes
                .checked_add(candidate.len())
                .ok_or(KafkaGroupedRatioContextErrorV0::ArithmeticOverflow)?;
            if candidate_bytes > MAX_CANDIDATE_BYTES {
                return Err(KafkaGroupedRatioContextErrorV0::CandidateCapacity {
                    maximum: MAX_CANDIDATE_BYTES,
                    attempted: candidate_bytes,
                });
            }
            candidates.push(candidate);
        }
    }

    let empty = render_response(
        admission,
        request,
        interval,
        query_closed,
        &candidates,
        0,
        MAX_KAFKA_GROUPED_RATIO_CONTEXT_OUTPUT_BYTES_V0,
    )
    .map_err(map_write_error)?;
    if empty.len() > request.byte_budget {
        return Err(KafkaGroupedRatioContextErrorV0::BudgetTooSmall {
            minimum: empty.len(),
            actual: request.byte_budget,
        });
    }
    let mut included_count = 0usize;
    let mut included_bytes = 0usize;
    for candidate in &candidates {
        let next_count = included_count
            .checked_add(1)
            .ok_or(KafkaGroupedRatioContextErrorV0::ArithmeticOverflow)?;
        let next_bytes = included_bytes
            .checked_add(candidate.len())
            .and_then(|value| value.checked_add(usize::from(included_count > 0)))
            .ok_or(KafkaGroupedRatioContextErrorV0::ArithmeticOverflow)?;
        let next_size = response_size(empty.len(), candidates.len(), next_count, next_bytes)?;
        if next_size > request.byte_budget {
            break;
        }
        included_count = next_count;
        included_bytes = next_bytes;
    }
    let bytes = render_response(
        admission,
        request,
        interval,
        query_closed,
        &candidates,
        included_count,
        request.byte_budget,
    )
    .map_err(map_response_write_error)?;
    let expected = response_size(
        empty.len(),
        candidates.len(),
        included_count,
        included_bytes,
    )?;
    if bytes.len() != expected || bytes.len() > request.byte_budget {
        return Err(KafkaGroupedRatioContextErrorV0::EncodingInvariant);
    }
    Ok(KafkaGroupedRatioContextV0 {
        bytes,
        candidate_count: candidates.len(),
        included_count,
        omitted_count: candidates.len() - included_count,
        query_closed,
    })
}

fn encode_candidate(
    request: &KafkaGroupedRatioContextRequestV0,
    group: &str,
    aggregate: &decant_state::GroupedIntervalRatioAggregateV0<'_>,
) -> Result<Vec<u8>, KafkaGroupedRatioContextErrorV0> {
    let mut writer = BoundedJsonWriter::new(MAX_FACT_BYTES);
    writer.push_static(b"{\"kind\":\"grouped_interval_ratio_ppm\",\"fact_id\":")?;
    writer.push_json_string(&request.fact_id)?;
    writer.push_static(b",\"group\":{\"field\":")?;
    writer.push_json_string(&request.group_field)?;
    writer.push_static(b",\"value\":")?;
    writer.push_json_string(group)?;
    writer.push_static(b"},\"predicate\":{\"field\":")?;
    writer.push_json_string(&request.predicate_field)?;
    writer.push_static(b",\"equals\":")?;
    writer.push_bool(request.predicate_value)?;
    writer.push_static(b"},\"ratio\":{\"numerator\":")?;
    writer.push_u64(aggregate.numerator())?;
    writer.push_static(b",\"denominator\":")?;
    writer.push_u64(aggregate.denominator())?;
    writer.push_static(b",\"rate_ppm\":")?;
    writer.push_u64(aggregate.rate_ppm())?;
    writer.push_static(b",\"remainder\":")?;
    writer.push_u64(aggregate.remainder())?;
    writer.push_static(b"},\"numerator_evidence_count\":")?;
    writer.push_usize(aggregate.numerator_evidence().len())?;
    writer.push_static(b",\"numerator_evidence_omitted\":")?;
    writer.push_usize(aggregate.numerator_evidence_omitted())?;
    writer.push_static(b",\"numerator_evidence\":[")?;
    for (index, event) in aggregate.numerator_evidence().enumerate() {
        if index > 0 {
            writer.push_static(b",")?;
        }
        writer.push_static(b"{\"event_time_ns\":")?;
        writer.push_i64(event.event_time_ns())?;
        writer.push_static(b",\"provenance\":")?;
        writer.push_json_string(event.provenance().as_str())?;
        writer.push_static(b"}")?;
    }
    writer.push_static(b"]}")?;
    Ok(writer.finish())
}

#[allow(clippy::too_many_arguments)]
fn render_response(
    admission: &AdmissionStateV0,
    request: &KafkaGroupedRatioContextRequestV0,
    interval: Option<(i64, i64)>,
    query_closed: bool,
    candidates: &[Vec<u8>],
    included_count: usize,
    maximum: usize,
) -> Result<Vec<u8>, JsonWriteError> {
    let mut writer = BoundedJsonWriter::new(maximum);
    writer.push_static(b"{\"format\":\"decant-kafka-grouped-ratio-context\",\"version\":0,\"query\":{\"available\":")?;
    writer.push_bool(interval.is_some())?;
    writer.push_static(b",\"start_event_time_ns\":")?;
    if let Some((start, _)) = interval {
        writer.push_i64(start)?;
    } else {
        writer.push_static(b"null")?;
    }
    writer.push_static(b",\"end_exclusive_event_time_ns\":")?;
    if let Some((_, end)) = interval {
        writer.push_i64(end)?;
    } else {
        writer.push_static(b"null")?;
    }
    writer.push_static(b"},\"status\":{\"source_finalized\":")?;
    writer.push_bool(admission.is_finalized())?;
    writer.push_static(b",\"query_closed\":")?;
    writer.push_bool(query_closed)?;
    writer.push_static(b"},\"groups\":{\"candidate_count\":")?;
    writer.push_usize(candidates.len())?;
    writer.push_static(b",\"included_count\":")?;
    writer.push_usize(included_count)?;
    writer.push_static(b",\"omitted_count\":")?;
    writer.push_usize(candidates.len() - included_count)?;
    writer.push_static(b",\"items\":[")?;
    for (index, candidate) in candidates.iter().take(included_count).enumerate() {
        if index > 0 {
            writer.push_static(b",")?;
        }
        writer.push_bytes(candidate)?;
    }
    writer.push_static(b"]},\"policy\":{\"version\":0,\"allowed_field_count\":")?;
    writer.push_usize(request.policy.allowed_fields)?;
    writer.push_static(b",\"denied_field_count\":")?;
    writer.push_usize(request.policy.denied_fields)?;
    writer.push_static(b"},\"omissions\":{\"keys_emitted\":false,\"denied_fields_emitted\":false,\"unselected_payload_fields_emitted\":false,\"transport_metadata_emitted\":false}}")?;
    Ok(writer.finish())
}

fn response_size(
    empty_size: usize,
    candidate_count: usize,
    included_count: usize,
    included_bytes: usize,
) -> Result<usize, KafkaGroupedRatioContextErrorV0> {
    let size = empty_size
        .checked_add(included_bytes)
        .ok_or(KafkaGroupedRatioContextErrorV0::ArithmeticOverflow)?;
    let size = replace_decimal_width(size, 0, included_count)?;
    replace_decimal_width(size, candidate_count, candidate_count - included_count)
}

fn replace_decimal_width(
    size: usize,
    old: usize,
    new: usize,
) -> Result<usize, KafkaGroupedRatioContextErrorV0> {
    let old_width = decimal_width(old);
    let new_width = decimal_width(new);
    if new_width >= old_width {
        size.checked_add(new_width - old_width)
            .ok_or(KafkaGroupedRatioContextErrorV0::ArithmeticOverflow)
    } else {
        size.checked_sub(old_width - new_width)
            .ok_or(KafkaGroupedRatioContextErrorV0::ArithmeticOverflow)
    }
}

fn decimal_width(mut value: usize) -> usize {
    let mut width = 1;
    while value >= 10 {
        width += 1;
        value /= 10;
    }
    width
}

fn validate_fact_id(value: &str) -> Result<(), KafkaGroupedRatioContextConfigErrorV0> {
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || bytes.len() > MAX_IDENTIFIER_BYTES
        || !(bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        || !bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(KafkaGroupedRatioContextConfigErrorV0::InvalidFactId);
    }
    Ok(())
}

fn validate_field(value: &str) -> Result<(), KafkaGroupedRatioContextConfigErrorV0> {
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || bytes.len() > MAX_FIELD_NAME_BYTES
        || !(bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        || !bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(KafkaGroupedRatioContextConfigErrorV0::InvalidField);
    }
    Ok(())
}

fn map_write_error(error: JsonWriteError) -> KafkaGroupedRatioContextErrorV0 {
    match error {
        JsonWriteError::Capacity { .. } => KafkaGroupedRatioContextErrorV0::EncodingInvariant,
        JsonWriteError::Arithmetic => KafkaGroupedRatioContextErrorV0::ArithmeticOverflow,
        JsonWriteError::Allocation => KafkaGroupedRatioContextErrorV0::Allocation,
    }
}

fn map_response_write_error(error: JsonWriteError) -> KafkaGroupedRatioContextErrorV0 {
    map_write_error(error)
}

impl From<JsonWriteError> for KafkaGroupedRatioContextErrorV0 {
    fn from(error: JsonWriteError) -> Self {
        match error {
            JsonWriteError::Capacity { maximum, attempted } => {
                Self::CandidateCapacity { maximum, attempted }
            }
            JsonWriteError::Arithmetic => Self::ArithmeticOverflow,
            JsonWriteError::Allocation => Self::Allocation,
        }
    }
}
