//! Canonical bounded bytes for one Kafka partition frontier.

use std::error::Error;
use std::fmt;
use std::str;

use decant_ir::{
    KafkaPositionV0, MAX_IDENTIFIER_BYTES, MAX_KAFKA_OFFSET_V0, SourceIdV0, ValidationErrorV0,
};

use crate::{
    KAFKA_PARTITION_FRONTIER_VERSION_V0, KAFKA_SOURCE_CONTRACT_VERSION_V0, KAFKA_TOPIC_ID_BYTES_V0,
    KafkaClusterIdV0, KafkaPartitionFrontierV0, KafkaSourceContractErrorV0, KafkaSourceContractV0,
    KafkaTopicIdV0, MAX_KAFKA_ASSIGNED_PARTITIONS_V0, MAX_KAFKA_CLUSTER_ID_BYTES_V0,
};

pub const KAFKA_FRONTIER_SNAPSHOT_VERSION_V0: u16 = 0;
pub const MAX_KAFKA_FRONTIER_SNAPSHOT_BYTES_V0: usize = 12_452;

const SNAPSHOT_MAGIC_V0: &[u8; 8] = b"DECKFRT0";
const VERSION_WIDTH: usize = 2;
const LENGTH_WIDTH: usize = 2;
const TOPIC_ID_WIDTH: usize = KAFKA_TOPIC_ID_BYTES_V0;
const ASSIGNMENT_COUNT_WIDTH: usize = 2;
const ASSIGNMENT_WIDTH: usize = 12;
const TERMINAL_NEXT_OFFSET_V0: i64 = MAX_KAFKA_OFFSET_V0 + 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaFrontierSnapshotFieldV0 {
    Magic,
    SnapshotVersion,
    FrontierVersion,
    SourceContractVersion,
    SourceId,
    ClusterId,
    TopicId,
    AssignmentCount,
    Partition,
    NextOffset,
    EndOfDocument,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaFrontierSnapshotExpectedContractFieldV0 {
    SourceId,
    ClusterId,
    TopicId,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaFrontierSnapshotEncodeErrorV0 {
    ArithmeticOverflow { field: KafkaFrontierSnapshotFieldV0 },
    SnapshotTooLarge { maximum: usize, attempted: usize },
    StateInvariant { field: KafkaFrontierSnapshotFieldV0 },
}

impl fmt::Debug for KafkaFrontierSnapshotEncodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArithmeticOverflow { field } => formatter
                .debug_struct("ArithmeticOverflow")
                .field("field", field)
                .finish(),
            Self::SnapshotTooLarge { maximum, attempted } => formatter
                .debug_struct("SnapshotTooLarge")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
            Self::StateInvariant { field } => formatter
                .debug_struct("StateInvariant")
                .field("field", field)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaFrontierSnapshotEncodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaFrontierSnapshotEncodeErrorV0 {}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaFrontierSnapshotDecodeErrorV0 {
    DocumentTooLarge {
        maximum: usize,
        actual: usize,
    },
    Truncated {
        field: KafkaFrontierSnapshotFieldV0,
    },
    InvalidMagic,
    UnsupportedSnapshotVersion {
        actual: u16,
    },
    UnsupportedFrontierVersion {
        actual: u16,
    },
    UnsupportedSourceContractVersion {
        actual: u16,
    },
    InvalidUtf8 {
        field: KafkaFrontierSnapshotFieldV0,
    },
    InvalidSourceId(ValidationErrorV0),
    InvalidSourceContract(KafkaSourceContractErrorV0),
    ExpectedSourceContractMismatch {
        field: KafkaFrontierSnapshotExpectedContractFieldV0,
    },
    EmptyAssignment,
    TooManyAssignments {
        maximum: usize,
        actual: usize,
    },
    InvalidCoordinate {
        field: KafkaFrontierSnapshotFieldV0,
    },
    NonCanonicalPartitionOrder,
    TrailingBytes,
}

impl fmt::Debug for KafkaFrontierSnapshotDecodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DocumentTooLarge { maximum, actual } => formatter
                .debug_struct("DocumentTooLarge")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::Truncated { field } => formatter
                .debug_struct("Truncated")
                .field("field", field)
                .finish(),
            Self::InvalidMagic => formatter.write_str("InvalidMagic"),
            Self::UnsupportedSnapshotVersion { actual } => formatter
                .debug_struct("UnsupportedSnapshotVersion")
                .field("actual", actual)
                .finish(),
            Self::UnsupportedFrontierVersion { actual } => formatter
                .debug_struct("UnsupportedFrontierVersion")
                .field("actual", actual)
                .finish(),
            Self::UnsupportedSourceContractVersion { actual } => formatter
                .debug_struct("UnsupportedSourceContractVersion")
                .field("actual", actual)
                .finish(),
            Self::InvalidUtf8 { field } => formatter
                .debug_struct("InvalidUtf8")
                .field("field", field)
                .finish(),
            Self::InvalidSourceId(error) => formatter
                .debug_tuple("InvalidSourceId")
                .field(error)
                .finish(),
            Self::InvalidSourceContract(error) => formatter
                .debug_tuple("InvalidSourceContract")
                .field(error)
                .finish(),
            Self::ExpectedSourceContractMismatch { field } => formatter
                .debug_struct("ExpectedSourceContractMismatch")
                .field("field", field)
                .finish(),
            Self::EmptyAssignment => formatter.write_str("EmptyAssignment"),
            Self::TooManyAssignments { maximum, actual } => formatter
                .debug_struct("TooManyAssignments")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::InvalidCoordinate { field } => formatter
                .debug_struct("InvalidCoordinate")
                .field("field", field)
                .finish(),
            Self::NonCanonicalPartitionOrder => formatter.write_str("NonCanonicalPartitionOrder"),
            Self::TrailingBytes => formatter.write_str("TrailingBytes"),
        }
    }
}

impl fmt::Display for KafkaFrontierSnapshotDecodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaFrontierSnapshotDecodeErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidSourceId(error) => Some(error),
            Self::InvalidSourceContract(error) => Some(error),
            _ => None,
        }
    }
}

pub fn encode_kafka_partition_frontier_snapshot_v0(
    frontier: &KafkaPartitionFrontierV0,
) -> Result<Vec<u8>, KafkaFrontierSnapshotEncodeErrorV0> {
    validate_frontier(frontier)?;
    let source_id = frontier.source_contract().source_id().as_str().as_bytes();
    let cluster_id = frontier.source_contract().cluster_id().as_str().as_bytes();
    let assignment_bytes = frontier
        .assignment_count()
        .checked_mul(ASSIGNMENT_WIDTH)
        .ok_or(KafkaFrontierSnapshotEncodeErrorV0::ArithmeticOverflow {
            field: KafkaFrontierSnapshotFieldV0::AssignmentCount,
        })?;
    let capacity = SNAPSHOT_MAGIC_V0
        .len()
        .checked_add(3 * VERSION_WIDTH)
        .and_then(|value| value.checked_add(2 * LENGTH_WIDTH))
        .and_then(|value| value.checked_add(source_id.len()))
        .and_then(|value| value.checked_add(cluster_id.len()))
        .and_then(|value| value.checked_add(TOPIC_ID_WIDTH + ASSIGNMENT_COUNT_WIDTH))
        .and_then(|value| value.checked_add(assignment_bytes))
        .ok_or(KafkaFrontierSnapshotEncodeErrorV0::ArithmeticOverflow {
            field: KafkaFrontierSnapshotFieldV0::EndOfDocument,
        })?;
    if capacity > MAX_KAFKA_FRONTIER_SNAPSHOT_BYTES_V0 {
        return Err(KafkaFrontierSnapshotEncodeErrorV0::SnapshotTooLarge {
            maximum: MAX_KAFKA_FRONTIER_SNAPSHOT_BYTES_V0,
            attempted: capacity,
        });
    }

    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(SNAPSHOT_MAGIC_V0);
    bytes.extend_from_slice(&KAFKA_FRONTIER_SNAPSHOT_VERSION_V0.to_be_bytes());
    bytes.extend_from_slice(&frontier.version().to_be_bytes());
    bytes.extend_from_slice(&frontier.source_contract().version().to_be_bytes());
    write_u16_length(
        &mut bytes,
        source_id,
        KafkaFrontierSnapshotFieldV0::SourceId,
    )?;
    write_u16_length(
        &mut bytes,
        cluster_id,
        KafkaFrontierSnapshotFieldV0::ClusterId,
    )?;
    bytes.extend_from_slice(frontier.source_contract().topic_id().as_bytes());
    let count = u16::try_from(frontier.assignment_count()).map_err(|_| {
        KafkaFrontierSnapshotEncodeErrorV0::ArithmeticOverflow {
            field: KafkaFrontierSnapshotFieldV0::AssignmentCount,
        }
    })?;
    bytes.extend_from_slice(&count.to_be_bytes());
    for (partition, next_offset) in frontier.iter() {
        bytes.extend_from_slice(&partition.to_be_bytes());
        bytes.extend_from_slice(&next_offset.to_be_bytes());
    }
    debug_assert_eq!(bytes.len(), capacity);
    Ok(bytes)
}

pub fn restore_kafka_partition_frontier_snapshot_v0(
    input: &[u8],
    expected_source_contract: &KafkaSourceContractV0,
) -> Result<KafkaPartitionFrontierV0, KafkaFrontierSnapshotDecodeErrorV0> {
    if input.len() > MAX_KAFKA_FRONTIER_SNAPSHOT_BYTES_V0 {
        return Err(KafkaFrontierSnapshotDecodeErrorV0::DocumentTooLarge {
            maximum: MAX_KAFKA_FRONTIER_SNAPSHOT_BYTES_V0,
            actual: input.len(),
        });
    }
    let mut cursor = SnapshotCursorV0::new(input);
    if cursor.read_exact(SNAPSHOT_MAGIC_V0.len(), KafkaFrontierSnapshotFieldV0::Magic)?
        != SNAPSHOT_MAGIC_V0
    {
        return Err(KafkaFrontierSnapshotDecodeErrorV0::InvalidMagic);
    }
    let snapshot_version = cursor.read_u16(KafkaFrontierSnapshotFieldV0::SnapshotVersion)?;
    if snapshot_version != KAFKA_FRONTIER_SNAPSHOT_VERSION_V0 {
        return Err(
            KafkaFrontierSnapshotDecodeErrorV0::UnsupportedSnapshotVersion {
                actual: snapshot_version,
            },
        );
    }
    let frontier_version = cursor.read_u16(KafkaFrontierSnapshotFieldV0::FrontierVersion)?;
    if frontier_version != KAFKA_PARTITION_FRONTIER_VERSION_V0 {
        return Err(
            KafkaFrontierSnapshotDecodeErrorV0::UnsupportedFrontierVersion {
                actual: frontier_version,
            },
        );
    }
    let contract_version = cursor.read_u16(KafkaFrontierSnapshotFieldV0::SourceContractVersion)?;
    if contract_version != KAFKA_SOURCE_CONTRACT_VERSION_V0 {
        return Err(
            KafkaFrontierSnapshotDecodeErrorV0::UnsupportedSourceContractVersion {
                actual: contract_version,
            },
        );
    }
    let source_id = SourceIdV0::new(
        cursor.read_bounded_str(KafkaFrontierSnapshotFieldV0::SourceId, MAX_IDENTIFIER_BYTES)?,
    )
    .map_err(KafkaFrontierSnapshotDecodeErrorV0::InvalidSourceId)?;
    let cluster_id = KafkaClusterIdV0::new(cursor.read_bounded_str(
        KafkaFrontierSnapshotFieldV0::ClusterId,
        MAX_KAFKA_CLUSTER_ID_BYTES_V0,
    )?)
    .map_err(KafkaFrontierSnapshotDecodeErrorV0::InvalidSourceContract)?;
    let mut topic_id_bytes = [0_u8; KAFKA_TOPIC_ID_BYTES_V0];
    topic_id_bytes.copy_from_slice(cursor.read_exact(
        KAFKA_TOPIC_ID_BYTES_V0,
        KafkaFrontierSnapshotFieldV0::TopicId,
    )?);
    let topic_id = KafkaTopicIdV0::from_bytes(topic_id_bytes)
        .map_err(KafkaFrontierSnapshotDecodeErrorV0::InvalidSourceContract)?;
    let restored_contract =
        KafkaSourceContractV0::new(contract_version, source_id, cluster_id, topic_id)
            .map_err(KafkaFrontierSnapshotDecodeErrorV0::InvalidSourceContract)?;
    validate_expected_contract(&restored_contract, expected_source_contract)?;

    let assignment_count =
        usize::from(cursor.read_u16(KafkaFrontierSnapshotFieldV0::AssignmentCount)?);
    if assignment_count == 0 {
        return Err(KafkaFrontierSnapshotDecodeErrorV0::EmptyAssignment);
    }
    if assignment_count > MAX_KAFKA_ASSIGNED_PARTITIONS_V0 {
        return Err(KafkaFrontierSnapshotDecodeErrorV0::TooManyAssignments {
            maximum: MAX_KAFKA_ASSIGNED_PARTITIONS_V0,
            actual: assignment_count,
        });
    }
    let mut entries = Vec::with_capacity(assignment_count);
    let mut previous_partition = None;
    for _ in 0..assignment_count {
        let partition = cursor.read_i32(KafkaFrontierSnapshotFieldV0::Partition)?;
        if KafkaPositionV0::new(partition, 0).is_err() {
            return Err(KafkaFrontierSnapshotDecodeErrorV0::InvalidCoordinate {
                field: KafkaFrontierSnapshotFieldV0::Partition,
            });
        }
        if previous_partition.is_some_and(|previous| partition <= previous) {
            return Err(KafkaFrontierSnapshotDecodeErrorV0::NonCanonicalPartitionOrder);
        }
        previous_partition = Some(partition);
        let next_offset = cursor.read_i64(KafkaFrontierSnapshotFieldV0::NextOffset)?;
        if !(0..=TERMINAL_NEXT_OFFSET_V0).contains(&next_offset) {
            return Err(KafkaFrontierSnapshotDecodeErrorV0::InvalidCoordinate {
                field: KafkaFrontierSnapshotFieldV0::NextOffset,
            });
        }
        entries.push((partition, next_offset));
    }
    if !cursor.is_finished() {
        return Err(KafkaFrontierSnapshotDecodeErrorV0::TrailingBytes);
    }
    Ok(KafkaPartitionFrontierV0::from_restored_next_offsets(
        frontier_version,
        expected_source_contract.clone(),
        entries,
    ))
}

fn validate_frontier(
    frontier: &KafkaPartitionFrontierV0,
) -> Result<(), KafkaFrontierSnapshotEncodeErrorV0> {
    if frontier.version() != KAFKA_PARTITION_FRONTIER_VERSION_V0 {
        return Err(KafkaFrontierSnapshotEncodeErrorV0::StateInvariant {
            field: KafkaFrontierSnapshotFieldV0::FrontierVersion,
        });
    }
    if frontier.source_contract().version() != KAFKA_SOURCE_CONTRACT_VERSION_V0 {
        return Err(KafkaFrontierSnapshotEncodeErrorV0::StateInvariant {
            field: KafkaFrontierSnapshotFieldV0::SourceContractVersion,
        });
    }
    if frontier.assignment_count() == 0
        || frontier.assignment_count() > MAX_KAFKA_ASSIGNED_PARTITIONS_V0
    {
        return Err(KafkaFrontierSnapshotEncodeErrorV0::StateInvariant {
            field: KafkaFrontierSnapshotFieldV0::AssignmentCount,
        });
    }
    for (partition, next_offset) in frontier.iter() {
        if KafkaPositionV0::new(partition, 0).is_err()
            || !(0..=TERMINAL_NEXT_OFFSET_V0).contains(&next_offset)
        {
            return Err(KafkaFrontierSnapshotEncodeErrorV0::StateInvariant {
                field: KafkaFrontierSnapshotFieldV0::NextOffset,
            });
        }
    }
    Ok(())
}

fn validate_expected_contract(
    restored: &KafkaSourceContractV0,
    expected: &KafkaSourceContractV0,
) -> Result<(), KafkaFrontierSnapshotDecodeErrorV0> {
    if restored.source_id() != expected.source_id() {
        return Err(
            KafkaFrontierSnapshotDecodeErrorV0::ExpectedSourceContractMismatch {
                field: KafkaFrontierSnapshotExpectedContractFieldV0::SourceId,
            },
        );
    }
    if restored.cluster_id() != expected.cluster_id() {
        return Err(
            KafkaFrontierSnapshotDecodeErrorV0::ExpectedSourceContractMismatch {
                field: KafkaFrontierSnapshotExpectedContractFieldV0::ClusterId,
            },
        );
    }
    if restored.topic_id() != expected.topic_id() {
        return Err(
            KafkaFrontierSnapshotDecodeErrorV0::ExpectedSourceContractMismatch {
                field: KafkaFrontierSnapshotExpectedContractFieldV0::TopicId,
            },
        );
    }
    Ok(())
}

fn write_u16_length(
    output: &mut Vec<u8>,
    value: &[u8],
    field: KafkaFrontierSnapshotFieldV0,
) -> Result<(), KafkaFrontierSnapshotEncodeErrorV0> {
    let length = u16::try_from(value.len())
        .map_err(|_| KafkaFrontierSnapshotEncodeErrorV0::ArithmeticOverflow { field })?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

struct SnapshotCursorV0<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> SnapshotCursorV0<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn read_exact(
        &mut self,
        length: usize,
        field: KafkaFrontierSnapshotFieldV0,
    ) -> Result<&'a [u8], KafkaFrontierSnapshotDecodeErrorV0> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(KafkaFrontierSnapshotDecodeErrorV0::Truncated { field })?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or(KafkaFrontierSnapshotDecodeErrorV0::Truncated { field })?;
        self.offset = end;
        Ok(value)
    }

    fn read_u16(
        &mut self,
        field: KafkaFrontierSnapshotFieldV0,
    ) -> Result<u16, KafkaFrontierSnapshotDecodeErrorV0> {
        let mut bytes = [0_u8; 2];
        bytes.copy_from_slice(self.read_exact(2, field)?);
        Ok(u16::from_be_bytes(bytes))
    }

    fn read_i32(
        &mut self,
        field: KafkaFrontierSnapshotFieldV0,
    ) -> Result<i32, KafkaFrontierSnapshotDecodeErrorV0> {
        let mut bytes = [0_u8; 4];
        bytes.copy_from_slice(self.read_exact(4, field)?);
        Ok(i32::from_be_bytes(bytes))
    }

    fn read_i64(
        &mut self,
        field: KafkaFrontierSnapshotFieldV0,
    ) -> Result<i64, KafkaFrontierSnapshotDecodeErrorV0> {
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(self.read_exact(8, field)?);
        Ok(i64::from_be_bytes(bytes))
    }

    fn read_bounded_str(
        &mut self,
        field: KafkaFrontierSnapshotFieldV0,
        maximum: usize,
    ) -> Result<&'a str, KafkaFrontierSnapshotDecodeErrorV0> {
        let length = usize::from(self.read_u16(field)?);
        if length > maximum {
            return Err(KafkaFrontierSnapshotDecodeErrorV0::InvalidCoordinate { field });
        }
        str::from_utf8(self.read_exact(length, field)?)
            .map_err(|_| KafkaFrontierSnapshotDecodeErrorV0::InvalidUtf8 { field })
    }

    const fn is_finished(&self) -> bool {
        self.offset == self.input.len()
    }
}
