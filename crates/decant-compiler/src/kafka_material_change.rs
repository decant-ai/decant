//! Policy-gated exact scalar-transition context for accepted Kafka events.

use std::error::Error;
use std::fmt;

use decant_ir::{MAX_FIELD_NAME_BYTES, MAX_IDENTIFIER_BYTES, SchemaIdV0, SourcePositionKindV0};
use decant_state::{
    AdmissionStateHashErrorV1, AdmissionStateHashV1, AdmissionStateV0, EventTimeRetentionConfigV0,
    PROJECTED_FIELD_CHANGE_VERSION_V0, ProjectedFieldChangeConfigV0, ProjectedFieldChangeLimitsV0,
    ProjectedFieldChangeMaterializationErrorV0, hash_admission_state_v1,
};

use super::{BoundedJsonWriter, JsonWriteError};
use crate::{KafkaFieldEgressActionV0, KafkaFieldEgressPolicyV0};

pub const KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V0: u16 = 0;
pub const KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1: u16 = 1;
pub const MIN_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0: usize = 512;
pub const MIN_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V1: usize = 768;
pub const MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0: usize = 16 * 1_024;
pub const MAX_KAFKA_MATERIAL_CHANGE_ITEMS_V0: usize = 256;

const MAX_CANDIDATE_BYTES: usize = 8 * 1_024 * 1_024;
const MAX_FACT_BYTES: usize = 16 * 1_024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaEntityKeyEgressActionV0 {
    AllowHex,
    Deny,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaMaterialChangeConfigErrorV0 {
    UnsupportedVersion {
        actual: u16,
    },
    InvalidViewId,
    InvalidField,
    DuplicateFieldRoles,
    FieldNotAllowed,
    ItemLimitOutOfRange {
        maximum: usize,
        actual: usize,
    },
    OutputBudgetOutOfRange {
        minimum: usize,
        maximum: usize,
        actual: usize,
    },
    Allocation,
}

impl fmt::Debug for KafkaMaterialChangeConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { actual } => formatter
                .debug_struct("UnsupportedVersion")
                .field("actual", actual)
                .finish(),
            Self::InvalidViewId => formatter.write_str("InvalidViewId"),
            Self::InvalidField => formatter.write_str("InvalidField"),
            Self::DuplicateFieldRoles => formatter.write_str("DuplicateFieldRoles"),
            Self::FieldNotAllowed => formatter.write_str("FieldNotAllowed"),
            Self::ItemLimitOutOfRange { maximum, actual } => formatter
                .debug_struct("ItemLimitOutOfRange")
                .field("minimum", &1)
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
            Self::Allocation => formatter.write_str("Allocation"),
        }
    }
}

impl fmt::Display for KafkaMaterialChangeConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaMaterialChangeConfigErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaMaterialChangeRequestV0 {
    version: u16,
    view_id: String,
    schema_id: SchemaIdV0,
    retention: EventTimeRetentionConfigV0,
    discriminator_field: String,
    value_field: String,
    item_limit: usize,
    byte_budget: usize,
    field_policy: KafkaFieldEgressPolicyV0,
    entity_key_action: KafkaEntityKeyEgressActionV0,
}

impl KafkaMaterialChangeRequestV0 {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        version: u16,
        view_id: &str,
        schema_id: SchemaIdV0,
        retention: EventTimeRetentionConfigV0,
        discriminator_field: &str,
        value_field: &str,
        item_limit: usize,
        byte_budget: usize,
        field_policy: KafkaFieldEgressPolicyV0,
        entity_key_action: KafkaEntityKeyEgressActionV0,
    ) -> Result<Self, KafkaMaterialChangeConfigErrorV0> {
        if !matches!(
            version,
            KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V0 | KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1
        ) {
            return Err(KafkaMaterialChangeConfigErrorV0::UnsupportedVersion { actual: version });
        }
        validate_view_id(view_id)?;
        validate_field(discriminator_field)?;
        validate_field(value_field)?;
        if discriminator_field == value_field {
            return Err(KafkaMaterialChangeConfigErrorV0::DuplicateFieldRoles);
        }
        if field_policy.action_for(discriminator_field) != Some(KafkaFieldEgressActionV0::Allow)
            || field_policy.action_for(value_field) != Some(KafkaFieldEgressActionV0::Allow)
        {
            return Err(KafkaMaterialChangeConfigErrorV0::FieldNotAllowed);
        }
        if !(1..=MAX_KAFKA_MATERIAL_CHANGE_ITEMS_V0).contains(&item_limit) {
            return Err(KafkaMaterialChangeConfigErrorV0::ItemLimitOutOfRange {
                maximum: MAX_KAFKA_MATERIAL_CHANGE_ITEMS_V0,
                actual: item_limit,
            });
        }
        let minimum_output_bytes = match version {
            KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V0 => MIN_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0,
            KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1 => MIN_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V1,
            _ => unreachable!("version checked above"),
        };
        if !(minimum_output_bytes..=MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0)
            .contains(&byte_budget)
        {
            return Err(KafkaMaterialChangeConfigErrorV0::OutputBudgetOutOfRange {
                minimum: minimum_output_bytes,
                maximum: MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0,
                actual: byte_budget,
            });
        }
        Ok(Self {
            version,
            view_id: copy_string(view_id)?,
            schema_id,
            retention,
            discriminator_field: copy_string(discriminator_field)?,
            value_field: copy_string(value_field)?,
            item_limit,
            byte_budget,
            field_policy,
            entity_key_action,
        })
    }

    pub const fn version(&self) -> u16 {
        self.version
    }

    pub fn view_id(&self) -> &str {
        &self.view_id
    }

    pub const fn schema_id(&self) -> &SchemaIdV0 {
        &self.schema_id
    }

    pub const fn retention(&self) -> EventTimeRetentionConfigV0 {
        self.retention
    }

    pub fn discriminator_field(&self) -> &str {
        &self.discriminator_field
    }

    pub fn value_field(&self) -> &str {
        &self.value_field
    }

    pub const fn item_limit(&self) -> usize {
        self.item_limit
    }

    pub const fn byte_budget(&self) -> usize {
        self.byte_budget
    }

    pub const fn field_policy(&self) -> &KafkaFieldEgressPolicyV0 {
        &self.field_policy
    }

    pub const fn entity_key_action(&self) -> KafkaEntityKeyEgressActionV0 {
        self.entity_key_action
    }
}

impl fmt::Debug for KafkaMaterialChangeRequestV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaMaterialChangeRequestV0")
            .field("version", &self.version)
            .field("view_id_bytes", &self.view_id.len())
            .field("schema_id", &"<redacted>")
            .field("retention", &self.retention)
            .field("discriminator_field_bytes", &self.discriminator_field.len())
            .field("value_field_bytes", &self.value_field.len())
            .field("item_limit", &self.item_limit)
            .field("byte_budget", &self.byte_budget)
            .field("field_policy", &self.field_policy)
            .field("entity_key_action", &self.entity_key_action)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaMaterialChangeContractIssueV0 {
    SourcePositionKind,
    SourceIdentity,
    SchemaIdentity,
    IngestTime,
    Sensitivity,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaMaterialChangeErrorV0 {
    RequestVersion {
        expected: u16,
        actual: u16,
    },
    AdmissionContract {
        issue: KafkaMaterialChangeContractIssueV0,
    },
    OperatorConfiguration,
    Operator(ProjectedFieldChangeMaterializationErrorV0),
    AdmissionStateHash(AdmissionStateHashErrorV1),
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

impl fmt::Debug for KafkaMaterialChangeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RequestVersion { expected, actual } => formatter
                .debug_struct("RequestVersion")
                .field("expected", expected)
                .field("actual", actual)
                .finish(),
            Self::AdmissionContract { issue } => formatter
                .debug_struct("AdmissionContract")
                .field("issue", issue)
                .finish(),
            Self::OperatorConfiguration => formatter.write_str("OperatorConfiguration"),
            Self::Operator(error) => formatter.debug_tuple("Operator").field(error).finish(),
            Self::AdmissionStateHash(error) => formatter
                .debug_tuple("AdmissionStateHash")
                .field(error)
                .finish(),
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

impl fmt::Display for KafkaMaterialChangeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaMaterialChangeErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Operator(error) => Some(error),
            Self::AdmissionStateHash(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaMaterialChangeContextV0 {
    bytes: Vec<u8>,
    candidate_count: usize,
    included_count: usize,
    omitted_count: usize,
    source_finalized: bool,
    admission_state_hash: Option<AdmissionStateHashV1>,
}

impl KafkaMaterialChangeContextV0 {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
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

    pub const fn admission_state_hash(&self) -> Option<&AdmissionStateHashV1> {
        self.admission_state_hash.as_ref()
    }
}

impl fmt::Debug for KafkaMaterialChangeContextV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaMaterialChangeContextV0")
            .field("byte_len", &self.bytes.len())
            .field("candidate_count", &self.candidate_count)
            .field("included_count", &self.included_count)
            .field("omitted_count", &self.omitted_count)
            .field("source_finalized", &self.source_finalized)
            .field(
                "has_admission_state_hash",
                &self.admission_state_hash.is_some(),
            )
            .finish()
    }
}

pub fn compile_kafka_material_change_context_v0(
    admission: &AdmissionStateV0,
    request: &KafkaMaterialChangeRequestV0,
) -> Result<KafkaMaterialChangeContextV0, KafkaMaterialChangeErrorV0> {
    if request.version != KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V0 {
        return Err(KafkaMaterialChangeErrorV0::RequestVersion {
            expected: KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V0,
            actual: request.version,
        });
    }
    compile_kafka_material_change_context(
        admission,
        request,
        KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V0,
        None,
    )
}

pub fn compile_kafka_material_change_context_v1(
    admission: &AdmissionStateV0,
    request: &KafkaMaterialChangeRequestV0,
) -> Result<KafkaMaterialChangeContextV0, KafkaMaterialChangeErrorV0> {
    if request.version != KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1 {
        return Err(KafkaMaterialChangeErrorV0::RequestVersion {
            expected: KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1,
            actual: request.version,
        });
    }
    let receipt = hash_admission_state_v1(admission)
        .map_err(KafkaMaterialChangeErrorV0::AdmissionStateHash)?;
    compile_kafka_material_change_context(
        admission,
        request,
        KAFKA_MATERIAL_CHANGE_CONTEXT_VERSION_V1,
        Some(receipt),
    )
}

fn compile_kafka_material_change_context(
    admission: &AdmissionStateV0,
    request: &KafkaMaterialChangeRequestV0,
    response_version: u16,
    admission_state_hash: Option<AdmissionStateHashV1>,
) -> Result<KafkaMaterialChangeContextV0, KafkaMaterialChangeErrorV0> {
    validate_admission(admission, request)?;
    let config = ProjectedFieldChangeConfigV0::new(
        PROJECTED_FIELD_CHANGE_VERSION_V0,
        request.retention,
        request.schema_id.clone(),
        &request.discriminator_field,
        &request.value_field,
        ProjectedFieldChangeLimitsV0::maximum(),
    )
    .map_err(|_| KafkaMaterialChangeErrorV0::OperatorConfiguration)?;
    let view = admission
        .materialize_projected_field_changes(config)
        .map_err(KafkaMaterialChangeErrorV0::Operator)?;
    let candidate_count = view.usage().changed_series();

    let encodable = candidate_count.min(request.item_limit);
    let mut candidates = Vec::new();
    candidates
        .try_reserve_exact(encodable)
        .map_err(|_| KafkaMaterialChangeErrorV0::Allocation)?;
    let mut candidate_bytes = 0usize;
    for (key, discriminator, observation) in view.entries().take(encodable) {
        let candidate = encode_candidate(request, key, discriminator, observation)?;
        candidate_bytes = candidate_bytes
            .checked_add(candidate.len())
            .ok_or(KafkaMaterialChangeErrorV0::ArithmeticOverflow)?;
        if candidate_bytes > MAX_CANDIDATE_BYTES {
            return Err(KafkaMaterialChangeErrorV0::CandidateCapacity {
                maximum: MAX_CANDIDATE_BYTES,
                attempted: candidate_bytes,
            });
        }
        candidates.push(candidate);
    }

    let source_finalized = admission.is_finalized();
    let empty = render_response(
        request,
        response_version,
        admission_state_hash.as_ref(),
        source_finalized,
        candidate_count,
        &candidates,
        0,
        MAX_KAFKA_MATERIAL_CHANGE_OUTPUT_BYTES_V0,
    )
    .map_err(map_metadata_write_error)?;
    if empty.len() > request.byte_budget {
        return Err(KafkaMaterialChangeErrorV0::BudgetTooSmall {
            minimum: empty.len(),
            actual: request.byte_budget,
        });
    }

    let mut included_count = 0usize;
    let mut included_bytes = 0usize;
    for candidate in &candidates {
        let next_count = included_count
            .checked_add(1)
            .ok_or(KafkaMaterialChangeErrorV0::ArithmeticOverflow)?;
        let next_bytes = included_bytes
            .checked_add(candidate.len())
            .and_then(|value| value.checked_add(usize::from(included_count > 0)))
            .ok_or(KafkaMaterialChangeErrorV0::ArithmeticOverflow)?;
        let next_size = response_size(empty.len(), candidate_count, next_count, next_bytes)?;
        if next_size > request.byte_budget {
            break;
        }
        included_count = next_count;
        included_bytes = next_bytes;
    }
    let bytes = render_response(
        request,
        response_version,
        admission_state_hash.as_ref(),
        source_finalized,
        candidate_count,
        &candidates,
        included_count,
        request.byte_budget,
    )
    .map_err(map_response_write_error)?;
    let expected = response_size(empty.len(), candidate_count, included_count, included_bytes)?;
    if bytes.len() != expected || bytes.len() > request.byte_budget {
        return Err(KafkaMaterialChangeErrorV0::EncodingInvariant);
    }

    Ok(KafkaMaterialChangeContextV0 {
        bytes,
        candidate_count,
        included_count,
        omitted_count: candidate_count - included_count,
        source_finalized,
        admission_state_hash,
    })
}

fn validate_admission(
    admission: &AdmissionStateV0,
    request: &KafkaMaterialChangeRequestV0,
) -> Result<(), KafkaMaterialChangeErrorV0> {
    if admission.config().source_position_kind() != SourcePositionKindV0::Kafka {
        return Err(KafkaMaterialChangeErrorV0::AdmissionContract {
            issue: KafkaMaterialChangeContractIssueV0::SourcePositionKind,
        });
    }
    for event in admission.accepted_events() {
        let issue = if event.source_id() != admission.config().source_id() {
            Some(KafkaMaterialChangeContractIssueV0::SourceIdentity)
        } else if event.schema_id() != &request.schema_id {
            Some(KafkaMaterialChangeContractIssueV0::SchemaIdentity)
        } else if event.ingest_time_ns() != 0 {
            Some(KafkaMaterialChangeContractIssueV0::IngestTime)
        } else if event.sensitivity().bits() != u64::MAX {
            Some(KafkaMaterialChangeContractIssueV0::Sensitivity)
        } else {
            None
        };
        if let Some(issue) = issue {
            return Err(KafkaMaterialChangeErrorV0::AdmissionContract { issue });
        }
    }
    Ok(())
}

fn encode_candidate(
    request: &KafkaMaterialChangeRequestV0,
    key: &[u8],
    discriminator: &str,
    observation: decant_state::ProjectedFieldChangeObservationV0<'_>,
) -> Result<Vec<u8>, KafkaMaterialChangeErrorV0> {
    let mut writer = BoundedJsonWriter::new(MAX_FACT_BYTES);
    writer.push_static(b"{\"kind\":\"exact_scalar_transition\",\"view_id\":")?;
    writer.push_json_string(&request.view_id)?;
    writer.push_static(b",\"entity_key\":")?;
    match request.entity_key_action {
        KafkaEntityKeyEgressActionV0::AllowHex => {
            writer.push_static(b"{\"encoding\":\"lower_hex\",\"value\":\"")?;
            writer.push_hex(key)?;
            writer.push_static(b"\"}")?;
        }
        KafkaEntityKeyEgressActionV0::Deny => writer.push_static(b"null")?,
    }
    writer.push_static(b",\"series\":")?;
    writer.push_json_string(discriminator)?;
    writer.push_static(b",\"previous\":{\"value\":")?;
    writer.push_scalar(observation.previous_value())?;
    writer.push_static(b",\"event_time_ns\":")?;
    writer.push_i64(observation.previous_event().event_time_ns())?;
    writer.push_static(b",\"provenance\":")?;
    writer.push_json_string(observation.previous_event().provenance().as_str())?;
    writer.push_static(b"},\"current\":{\"value\":")?;
    writer.push_scalar(observation.current_value())?;
    writer.push_static(b",\"event_time_ns\":")?;
    writer.push_i64(observation.current_event().event_time_ns())?;
    writer.push_static(b",\"provenance\":")?;
    writer.push_json_string(observation.current_event().provenance().as_str())?;
    writer.push_static(b"}}")?;
    Ok(writer.finish())
}

#[allow(clippy::too_many_arguments)]
fn render_response(
    request: &KafkaMaterialChangeRequestV0,
    response_version: u16,
    admission_state_hash: Option<&AdmissionStateHashV1>,
    source_finalized: bool,
    candidate_count: usize,
    candidates: &[Vec<u8>],
    included_count: usize,
    maximum: usize,
) -> Result<Vec<u8>, JsonWriteError> {
    let mut writer = BoundedJsonWriter::new(maximum);
    writer.push_static(b"{\"format\":\"decant-kafka-material-change-context\",\"version\":")?;
    writer.push_u64(u64::from(response_version))?;
    writer.push_static(b",\"view_id\":")?;
    writer.push_json_string(&request.view_id)?;
    if let Some(admission_state_hash) = admission_state_hash {
        writer.push_static(b",\"receipt\":{\"admission_state_hash\":")?;
        writer.push_json_string(&admission_state_hash.to_canonical_string())?;
        writer.push_static(b"}")?;
    }
    writer.push_static(b",\"definition\":\"latest_distinct_retained_scalar_transition\",\"status\":{\"source_finalized\":")?;
    writer.push_bool(source_finalized)?;
    writer.push_static(b",\"provisional\":")?;
    writer.push_bool(!source_finalized)?;
    writer.push_static(b"},\"changes\":{\"candidate_count\":")?;
    writer.push_usize(candidate_count)?;
    writer.push_static(b",\"included_count\":")?;
    writer.push_usize(included_count)?;
    writer.push_static(b",\"omitted_count\":")?;
    writer.push_usize(candidate_count - included_count)?;
    writer.push_static(b",\"items\":[")?;
    for (index, candidate) in candidates.iter().take(included_count).enumerate() {
        if index > 0 {
            writer.push_static(b",")?;
        }
        writer.push_bytes(candidate)?;
    }
    writer.push_static(b"]},\"policy\":{\"version\":0,\"allowed_field_count\":")?;
    writer.push_usize(request.field_policy.allowed_fields())?;
    writer.push_static(b",\"denied_field_count\":")?;
    writer.push_usize(request.field_policy.denied_fields())?;
    writer.push_static(b",\"entity_key_action\":")?;
    writer.push_static(match request.entity_key_action {
        KafkaEntityKeyEgressActionV0::AllowHex => b"\"allow_lower_hex\"",
        KafkaEntityKeyEgressActionV0::Deny => b"\"deny\"",
    })?;
    writer.push_static(b"},\"omissions\":{\"denied_fields_emitted\":false,\"unselected_payload_fields_emitted\":false,\"transport_metadata_emitted\":false}}")?;
    Ok(writer.finish())
}

fn response_size(
    empty_size: usize,
    candidate_count: usize,
    included_count: usize,
    included_bytes: usize,
) -> Result<usize, KafkaMaterialChangeErrorV0> {
    let size = empty_size
        .checked_add(included_bytes)
        .ok_or(KafkaMaterialChangeErrorV0::ArithmeticOverflow)?;
    let size = replace_decimal_width(size, 0, included_count)?;
    replace_decimal_width(size, candidate_count, candidate_count - included_count)
}

fn replace_decimal_width(
    size: usize,
    old: usize,
    new: usize,
) -> Result<usize, KafkaMaterialChangeErrorV0> {
    let old_width = decimal_width(old);
    let new_width = decimal_width(new);
    if new_width >= old_width {
        size.checked_add(new_width - old_width)
            .ok_or(KafkaMaterialChangeErrorV0::ArithmeticOverflow)
    } else {
        size.checked_sub(old_width - new_width)
            .ok_or(KafkaMaterialChangeErrorV0::ArithmeticOverflow)
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

fn validate_view_id(value: &str) -> Result<(), KafkaMaterialChangeConfigErrorV0> {
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || bytes.len() > MAX_IDENTIFIER_BYTES
        || !(bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
        || !bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(KafkaMaterialChangeConfigErrorV0::InvalidViewId);
    }
    Ok(())
}

fn validate_field(value: &str) -> Result<(), KafkaMaterialChangeConfigErrorV0> {
    let bytes = value.as_bytes();
    if bytes.is_empty()
        || bytes.len() > MAX_FIELD_NAME_BYTES
        || !(bytes[0].is_ascii_lowercase() || bytes[0] == b'_')
        || !bytes[1..].iter().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'_' | b'-')
        })
    {
        return Err(KafkaMaterialChangeConfigErrorV0::InvalidField);
    }
    Ok(())
}

fn copy_string(value: &str) -> Result<String, KafkaMaterialChangeConfigErrorV0> {
    let mut output = String::new();
    output
        .try_reserve_exact(value.len())
        .map_err(|_| KafkaMaterialChangeConfigErrorV0::Allocation)?;
    output.push_str(value);
    Ok(output)
}

fn map_metadata_write_error(error: JsonWriteError) -> KafkaMaterialChangeErrorV0 {
    match error {
        JsonWriteError::Capacity { maximum, attempted } => {
            KafkaMaterialChangeErrorV0::CandidateCapacity { maximum, attempted }
        }
        JsonWriteError::Arithmetic => KafkaMaterialChangeErrorV0::ArithmeticOverflow,
        JsonWriteError::Allocation => KafkaMaterialChangeErrorV0::Allocation,
    }
}

fn map_response_write_error(error: JsonWriteError) -> KafkaMaterialChangeErrorV0 {
    match error {
        JsonWriteError::Capacity { .. } => KafkaMaterialChangeErrorV0::EncodingInvariant,
        JsonWriteError::Arithmetic => KafkaMaterialChangeErrorV0::ArithmeticOverflow,
        JsonWriteError::Allocation => KafkaMaterialChangeErrorV0::Allocation,
    }
}

impl From<JsonWriteError> for KafkaMaterialChangeErrorV0 {
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
