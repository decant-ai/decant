//! Bounded payload-free activity context for one Kafka admission state.

use std::cmp::Ordering;
use std::error::Error;
use std::fmt;

use decant_ir::{EventEnvelopeV0, SourcePositionKindV0, SourcePositionV0};
use decant_state::{AdmissionStateHashErrorV1, AdmissionStateV0, hash_admission_state_v1};

use super::{BoundedJsonWriter, JsonWriteError};

pub const KAFKA_ACTIVITY_CONTEXT_VERSION_V0: u16 = 0;
pub const MIN_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0: usize = 512;
pub const MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0: usize = 16 * 1_024;
pub const MAX_KAFKA_ACTIVITY_DISTINCT_SCHEMAS_V0: usize = 4_096;

const MAX_SCHEMA_ITEM_BYTES_V0: usize = 512;
const MAX_UNCLASSIFIED_SENSITIVITY_BITS_V0: u64 = u64::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaActivityContextConfigErrorV0 {
    UnsupportedVersion { actual: u16 },
    OutputBytesBelowMinimum { minimum: usize, actual: usize },
    OutputBytesAboveMaximum { maximum: usize, actual: usize },
}

impl fmt::Display for KafkaActivityContextConfigErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaActivityContextConfigErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KafkaActivityContextRequestV0 {
    version: u16,
    byte_budget: usize,
}

impl KafkaActivityContextRequestV0 {
    pub fn new(
        version: u16,
        byte_budget: usize,
    ) -> Result<Self, KafkaActivityContextConfigErrorV0> {
        if version != KAFKA_ACTIVITY_CONTEXT_VERSION_V0 {
            return Err(KafkaActivityContextConfigErrorV0::UnsupportedVersion { actual: version });
        }
        if byte_budget < MIN_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0 {
            return Err(KafkaActivityContextConfigErrorV0::OutputBytesBelowMinimum {
                minimum: MIN_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0,
                actual: byte_budget,
            });
        }
        if byte_budget > MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0 {
            return Err(KafkaActivityContextConfigErrorV0::OutputBytesAboveMaximum {
                maximum: MAX_KAFKA_ACTIVITY_CONTEXT_OUTPUT_BYTES_V0,
                actual: byte_budget,
            });
        }
        Ok(Self {
            version,
            byte_budget,
        })
    }

    pub const fn version(self) -> u16 {
        self.version
    }

    pub const fn byte_budget(self) -> usize {
        self.byte_budget
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaActivityContractIssueV0 {
    Source,
    SourcePosition,
    IngestTime,
    Sensitivity,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaActivityContextErrorV0 {
    UnexpectedSourcePositionKind,
    InvalidEventContract { issue: KafkaActivityContractIssueV0 },
    DistinctSchemaCapacity { maximum: usize, attempted: usize },
    ArithmeticOverflow,
    AdmissionStateHash(AdmissionStateHashErrorV1),
    OutputCapacity { maximum: usize, attempted: usize },
    OutputAllocation,
    EncodingInvariant,
}

impl fmt::Debug for KafkaActivityContextErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedSourcePositionKind => {
                formatter.write_str("UnexpectedSourcePositionKind")
            }
            Self::InvalidEventContract { issue } => formatter
                .debug_struct("InvalidEventContract")
                .field("issue", issue)
                .finish(),
            Self::DistinctSchemaCapacity { maximum, attempted } => formatter
                .debug_struct("DistinctSchemaCapacity")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::ArithmeticOverflow => formatter.write_str("ArithmeticOverflow"),
            Self::AdmissionStateHash(error) => formatter
                .debug_tuple("AdmissionStateHash")
                .field(error)
                .finish(),
            Self::OutputCapacity { maximum, attempted } => formatter
                .debug_struct("OutputCapacity")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::OutputAllocation => formatter.write_str("OutputAllocation"),
            Self::EncodingInvariant => formatter.write_str("EncodingInvariant"),
        }
    }
}

impl fmt::Display for KafkaActivityContextErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaActivityContextErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::AdmissionStateHash(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaActivityContextV0 {
    bytes: Vec<u8>,
    accepted_events: u64,
    candidate_schema_count: usize,
    included_schema_count: usize,
    omitted_schema_count: usize,
    source_finalized: bool,
}

impl KafkaActivityContextV0 {
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub const fn accepted_events(&self) -> u64 {
        self.accepted_events
    }

    pub const fn candidate_schema_count(&self) -> usize {
        self.candidate_schema_count
    }

    pub const fn included_schema_count(&self) -> usize {
        self.included_schema_count
    }

    pub const fn omitted_schema_count(&self) -> usize {
        self.omitted_schema_count
    }

    pub const fn source_finalized(&self) -> bool {
        self.source_finalized
    }
}

impl fmt::Debug for KafkaActivityContextV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaActivityContextV0")
            .field("byte_len", &self.bytes.len())
            .field("accepted_events", &self.accepted_events)
            .field("candidate_schema_count", &self.candidate_schema_count)
            .field("included_schema_count", &self.included_schema_count)
            .field("omitted_schema_count", &self.omitted_schema_count)
            .field("source_finalized", &self.source_finalized)
            .finish()
    }
}

#[derive(Clone, Copy)]
struct Evidence<'a> {
    event_time_ns: i64,
    source_position: SourcePositionV0,
    provenance: &'a str,
}

struct SchemaActivity<'a> {
    schema_id: &'a str,
    event_count: u64,
    event_time_minimum_ns: i64,
    event_time_maximum_ns: i64,
    earliest: Evidence<'a>,
    latest: Evidence<'a>,
}

pub fn compile_kafka_activity_context_v0(
    state: &AdmissionStateV0,
    request: KafkaActivityContextRequestV0,
) -> Result<KafkaActivityContextV0, KafkaActivityContextErrorV0> {
    if state.config().source_position_kind() != SourcePositionKindV0::Kafka {
        return Err(KafkaActivityContextErrorV0::UnexpectedSourcePositionKind);
    }

    let mut activities = Vec::new();
    activities
        .try_reserve_exact(MAX_KAFKA_ACTIVITY_DISTINCT_SCHEMAS_V0)
        .map_err(|_| KafkaActivityContextErrorV0::OutputAllocation)?;
    let mut accepted_events = 0u64;
    for event in state.accepted_events() {
        validate_event(state, event)?;
        accepted_events = accepted_events
            .checked_add(1)
            .ok_or(KafkaActivityContextErrorV0::ArithmeticOverflow)?;
        aggregate_event(&mut activities, event)?;
    }

    let state_hash =
        hash_admission_state_v1(state).map_err(KafkaActivityContextErrorV0::AdmissionStateHash)?;
    let items = encode_schema_items(&activities)?;
    let (bytes, included_schema_count) = select_largest_prefix(
        &items,
        accepted_events,
        state.is_finalized(),
        state_hash.as_bytes(),
        request.byte_budget(),
    )?;
    let candidate_schema_count = activities.len();
    let omitted_schema_count = candidate_schema_count
        .checked_sub(included_schema_count)
        .ok_or(KafkaActivityContextErrorV0::EncodingInvariant)?;

    Ok(KafkaActivityContextV0 {
        bytes,
        accepted_events,
        candidate_schema_count,
        included_schema_count,
        omitted_schema_count,
        source_finalized: state.is_finalized(),
    })
}

fn validate_event(
    state: &AdmissionStateV0,
    event: &EventEnvelopeV0,
) -> Result<(), KafkaActivityContextErrorV0> {
    if event.source_id() != state.config().source_id() {
        return invalid_contract(KafkaActivityContractIssueV0::Source);
    }
    if event.source_position().kind() != SourcePositionKindV0::Kafka {
        return invalid_contract(KafkaActivityContractIssueV0::SourcePosition);
    }
    if event.ingest_time_ns() != 0 {
        return invalid_contract(KafkaActivityContractIssueV0::IngestTime);
    }
    if event.sensitivity().bits() != MAX_UNCLASSIFIED_SENSITIVITY_BITS_V0 {
        return invalid_contract(KafkaActivityContractIssueV0::Sensitivity);
    }
    Ok(())
}

fn invalid_contract<T>(
    issue: KafkaActivityContractIssueV0,
) -> Result<T, KafkaActivityContextErrorV0> {
    Err(KafkaActivityContextErrorV0::InvalidEventContract { issue })
}

fn aggregate_event<'a>(
    activities: &mut Vec<SchemaActivity<'a>>,
    event: &'a EventEnvelopeV0,
) -> Result<(), KafkaActivityContextErrorV0> {
    let schema_id = event.schema_id().as_str();
    let evidence = Evidence {
        event_time_ns: event.event_time_ns(),
        source_position: event.source_position(),
        provenance: event.provenance().as_str(),
    };
    match activities.binary_search_by(|activity| activity.schema_id.cmp(schema_id)) {
        Ok(index) => {
            let activity = &mut activities[index];
            activity.event_count = activity
                .event_count
                .checked_add(1)
                .ok_or(KafkaActivityContextErrorV0::ArithmeticOverflow)?;
            activity.event_time_minimum_ns =
                activity.event_time_minimum_ns.min(evidence.event_time_ns);
            activity.event_time_maximum_ns =
                activity.event_time_maximum_ns.max(evidence.event_time_ns);
            if compare_evidence(&evidence, &activity.earliest) == Ordering::Less {
                activity.earliest = evidence;
            }
            if compare_evidence(&evidence, &activity.latest) == Ordering::Greater {
                activity.latest = evidence;
            }
        }
        Err(index) => {
            let attempted = activities
                .len()
                .checked_add(1)
                .ok_or(KafkaActivityContextErrorV0::ArithmeticOverflow)?;
            if attempted > MAX_KAFKA_ACTIVITY_DISTINCT_SCHEMAS_V0 {
                return Err(KafkaActivityContextErrorV0::DistinctSchemaCapacity {
                    maximum: MAX_KAFKA_ACTIVITY_DISTINCT_SCHEMAS_V0,
                    attempted,
                });
            }
            activities.insert(
                index,
                SchemaActivity {
                    schema_id,
                    event_count: 1,
                    event_time_minimum_ns: evidence.event_time_ns,
                    event_time_maximum_ns: evidence.event_time_ns,
                    earliest: evidence,
                    latest: evidence,
                },
            );
        }
    }
    Ok(())
}

fn compare_evidence(left: &Evidence<'_>, right: &Evidence<'_>) -> Ordering {
    left.event_time_ns
        .cmp(&right.event_time_ns)
        .then_with(|| left.source_position.cmp(&right.source_position))
}

fn encode_schema_items(
    activities: &[SchemaActivity<'_>],
) -> Result<Vec<Vec<u8>>, KafkaActivityContextErrorV0> {
    let mut items = Vec::new();
    items
        .try_reserve_exact(activities.len())
        .map_err(|_| KafkaActivityContextErrorV0::OutputAllocation)?;
    for activity in activities {
        let mut writer = BoundedJsonWriter::new(MAX_SCHEMA_ITEM_BYTES_V0);
        writer.push_static(b"{\"schema_id\":")?;
        writer.push_json_string(activity.schema_id)?;
        writer.push_static(b",\"event_count\":")?;
        writer.push_u64(activity.event_count)?;
        writer.push_static(b",\"event_time_ns\":{\"minimum\":")?;
        writer.push_i64(activity.event_time_minimum_ns)?;
        writer.push_static(b",\"maximum\":")?;
        writer.push_i64(activity.event_time_maximum_ns)?;
        writer.push_static(b"},\"evidence\":{\"earliest\":")?;
        writer.push_json_string(activity.earliest.provenance)?;
        writer.push_static(b",\"latest\":")?;
        writer.push_json_string(activity.latest.provenance)?;
        writer.push_static(b"}}")?;
        items.push(writer.finish());
    }
    Ok(items)
}

fn select_largest_prefix(
    items: &[Vec<u8>],
    accepted_events: u64,
    source_finalized: bool,
    state_hash: &[u8; 32],
    byte_budget: usize,
) -> Result<(Vec<u8>, usize), KafkaActivityContextErrorV0> {
    let mut best = None;
    for included in 0..=items.len() {
        match encode_context(
            items,
            included,
            accepted_events,
            source_finalized,
            state_hash,
            byte_budget,
        ) {
            Ok(bytes) => best = Some((bytes, included)),
            Err(KafkaActivityContextErrorV0::OutputCapacity { .. }) => {}
            Err(error) => return Err(error),
        }
    }
    best.ok_or(KafkaActivityContextErrorV0::OutputCapacity {
        maximum: byte_budget,
        attempted: byte_budget.saturating_add(1),
    })
}

fn encode_context(
    items: &[Vec<u8>],
    included: usize,
    accepted_events: u64,
    source_finalized: bool,
    state_hash: &[u8; 32],
    byte_budget: usize,
) -> Result<Vec<u8>, KafkaActivityContextErrorV0> {
    let omitted = items
        .len()
        .checked_sub(included)
        .ok_or(KafkaActivityContextErrorV0::EncodingInvariant)?;
    let mut writer = BoundedJsonWriter::new(byte_budget);
    writer.push_static(
        b"{\"format\":\"decant-kafka-activity-context\",\"version\":0,\"status\":{\"source_finalized\":",
    )?;
    writer.push_bool(source_finalized)?;
    writer.push_static(b"},\"activity\":{\"accepted_events\":")?;
    writer.push_u64(accepted_events)?;
    writer.push_static(b",\"distinct_schemas\":")?;
    writer.push_usize(items.len())?;
    writer.push_static(b"},\"schemas\":{\"candidate_count\":")?;
    writer.push_usize(items.len())?;
    writer.push_static(b",\"included_count\":")?;
    writer.push_usize(included)?;
    writer.push_static(b",\"omitted_count\":")?;
    writer.push_usize(omitted)?;
    writer.push_static(b",\"items\":[")?;
    for (index, item) in items.iter().take(included).enumerate() {
        if index != 0 {
            writer.push_static(b",")?;
        }
        writer.push_bytes(item)?;
    }
    writer.push_static(
        b"]},\"omissions\":{\"payload_fields_emitted\":false,\"keys_emitted\":false,\"transport_metadata_emitted\":false},\"receipt\":{\"admission_state_hash\":\"sha256:v1:",
    )?;
    writer.push_hex(state_hash)?;
    writer.push_static(b"\"}}")?;
    Ok(writer.finish())
}

impl From<JsonWriteError> for KafkaActivityContextErrorV0 {
    fn from(error: JsonWriteError) -> Self {
        match error {
            JsonWriteError::Capacity { maximum, attempted } => {
                Self::OutputCapacity { maximum, attempted }
            }
            JsonWriteError::Arithmetic => Self::ArithmeticOverflow,
            JsonWriteError::Allocation => Self::OutputAllocation,
        }
    }
}
