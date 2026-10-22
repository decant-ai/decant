//! Canonical in-memory envelope for one semantic-state/source-progress cut.

use std::error::Error;
use std::fmt;

use decant_ir::SourcePositionKindV0;
use decant_kafka::{
    KAFKA_FRONTIER_SNAPSHOT_VERSION_V0, KafkaFrontierSnapshotDecodeErrorV0,
    KafkaFrontierSnapshotEncodeErrorV0, KafkaPartitionFrontierV0, KafkaSourceContractV0,
    MAX_KAFKA_FRONTIER_SNAPSHOT_BYTES_V0, encode_kafka_partition_frontier_snapshot_v0,
    restore_kafka_partition_frontier_snapshot_v0,
};
use decant_state::{
    ADMISSION_SNAPSHOT_VERSION_V1, AdmissionConfigV0, AdmissionSnapshotDecodeErrorV1,
    AdmissionSnapshotEncodeErrorV1, AdmissionStateV0, MAX_ADMISSION_SNAPSHOT_BYTES_V1,
    encode_admission_snapshot_v1, restore_admission_snapshot_v1,
};

pub const KAFKA_INGESTION_CHECKPOINT_VERSION_V0: u16 = 0;
pub const MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0: usize = CHECKPOINT_HEADER_BYTES_V0
    + MAX_ADMISSION_SNAPSHOT_BYTES_V1
    + MAX_KAFKA_FRONTIER_SNAPSHOT_BYTES_V0;

const CHECKPOINT_MAGIC_V0: &[u8; 8] = b"DECKPT00";
const CHECKPOINT_HEADER_BYTES_V0: usize = 26;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaIngestionCheckpointFieldV0 {
    Magic,
    CheckpointVersion,
    AdmissionSnapshotVersion,
    FrontierSnapshotVersion,
    AdmissionSnapshot,
    FrontierSnapshot,
    Checkpoint,
    EndOfDocument,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaIngestionCheckpointEncodeErrorV0 {
    UnsupportedAdmissionSourcePositionKind {
        actual: SourcePositionKindV0,
    },
    AdmissionSourceMismatch,
    AcceptedEventUnsupportedPosition {
        actual: SourcePositionKindV0,
    },
    AcceptedEventUnknownPartition,
    AcceptedEventNotBeforeFrontier,
    AdmissionSnapshot(AdmissionSnapshotEncodeErrorV1),
    FrontierSnapshot(KafkaFrontierSnapshotEncodeErrorV0),
    LengthOverflow {
        field: KafkaIngestionCheckpointFieldV0,
    },
    CheckpointTooLarge {
        maximum: usize,
        attempted: usize,
    },
}

impl fmt::Debug for KafkaIngestionCheckpointEncodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedAdmissionSourcePositionKind { actual } => formatter
                .debug_struct("UnsupportedAdmissionSourcePositionKind")
                .field("actual", actual)
                .finish(),
            Self::AdmissionSourceMismatch => formatter.write_str("AdmissionSourceMismatch"),
            Self::AcceptedEventUnsupportedPosition { actual } => formatter
                .debug_struct("AcceptedEventUnsupportedPosition")
                .field("actual", actual)
                .finish(),
            Self::AcceptedEventUnknownPartition => {
                formatter.write_str("AcceptedEventUnknownPartition")
            }
            Self::AcceptedEventNotBeforeFrontier => {
                formatter.write_str("AcceptedEventNotBeforeFrontier")
            }
            Self::AdmissionSnapshot(error) => formatter
                .debug_tuple("AdmissionSnapshot")
                .field(error)
                .finish(),
            Self::FrontierSnapshot(error) => formatter
                .debug_tuple("FrontierSnapshot")
                .field(error)
                .finish(),
            Self::LengthOverflow { field } => formatter
                .debug_struct("LengthOverflow")
                .field("field", field)
                .finish(),
            Self::CheckpointTooLarge { maximum, attempted } => formatter
                .debug_struct("CheckpointTooLarge")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaIngestionCheckpointEncodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaIngestionCheckpointEncodeErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::AdmissionSnapshot(error) => Some(error),
            Self::FrontierSnapshot(error) => Some(error),
            _ => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaIngestionCheckpointDecodeErrorV0 {
    ExpectedAdmissionSourcePositionKind {
        actual: SourcePositionKindV0,
    },
    ExpectedSourceMismatch,
    DocumentTooLarge {
        maximum: usize,
        actual: usize,
    },
    Truncated {
        field: KafkaIngestionCheckpointFieldV0,
    },
    InvalidMagic,
    UnsupportedCheckpointVersion {
        actual: u16,
    },
    UnsupportedAdmissionSnapshotVersion {
        actual: u16,
    },
    UnsupportedFrontierSnapshotVersion {
        actual: u16,
    },
    InvalidComponentLength {
        field: KafkaIngestionCheckpointFieldV0,
        maximum: usize,
        actual: usize,
    },
    LengthOverflow {
        field: KafkaIngestionCheckpointFieldV0,
    },
    TrailingBytes,
    FrontierSnapshot(KafkaFrontierSnapshotDecodeErrorV0),
    AdmissionSnapshot(AdmissionSnapshotDecodeErrorV1),
    RestoredAdmissionSourceMismatch,
    AcceptedEventUnsupportedPosition {
        actual: SourcePositionKindV0,
    },
    AcceptedEventUnknownPartition,
    AcceptedEventNotBeforeFrontier,
}

impl fmt::Debug for KafkaIngestionCheckpointDecodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExpectedAdmissionSourcePositionKind { actual } => formatter
                .debug_struct("ExpectedAdmissionSourcePositionKind")
                .field("actual", actual)
                .finish(),
            Self::ExpectedSourceMismatch => formatter.write_str("ExpectedSourceMismatch"),
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
            Self::UnsupportedCheckpointVersion { actual } => formatter
                .debug_struct("UnsupportedCheckpointVersion")
                .field("actual", actual)
                .finish(),
            Self::UnsupportedAdmissionSnapshotVersion { actual } => formatter
                .debug_struct("UnsupportedAdmissionSnapshotVersion")
                .field("actual", actual)
                .finish(),
            Self::UnsupportedFrontierSnapshotVersion { actual } => formatter
                .debug_struct("UnsupportedFrontierSnapshotVersion")
                .field("actual", actual)
                .finish(),
            Self::InvalidComponentLength {
                field,
                maximum,
                actual,
            } => formatter
                .debug_struct("InvalidComponentLength")
                .field("field", field)
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::LengthOverflow { field } => formatter
                .debug_struct("LengthOverflow")
                .field("field", field)
                .finish(),
            Self::TrailingBytes => formatter.write_str("TrailingBytes"),
            Self::FrontierSnapshot(error) => formatter
                .debug_tuple("FrontierSnapshot")
                .field(error)
                .finish(),
            Self::AdmissionSnapshot(error) => formatter
                .debug_tuple("AdmissionSnapshot")
                .field(error)
                .finish(),
            Self::RestoredAdmissionSourceMismatch => {
                formatter.write_str("RestoredAdmissionSourceMismatch")
            }
            Self::AcceptedEventUnsupportedPosition { actual } => formatter
                .debug_struct("AcceptedEventUnsupportedPosition")
                .field("actual", actual)
                .finish(),
            Self::AcceptedEventUnknownPartition => {
                formatter.write_str("AcceptedEventUnknownPartition")
            }
            Self::AcceptedEventNotBeforeFrontier => {
                formatter.write_str("AcceptedEventNotBeforeFrontier")
            }
        }
    }
}

impl fmt::Display for KafkaIngestionCheckpointDecodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaIngestionCheckpointDecodeErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::FrontierSnapshot(error) => Some(error),
            Self::AdmissionSnapshot(error) => Some(error),
            _ => None,
        }
    }
}

pub fn encode_kafka_ingestion_checkpoint_v0(
    admission: &AdmissionStateV0,
    frontier: &KafkaPartitionFrontierV0,
) -> Result<Vec<u8>, KafkaIngestionCheckpointEncodeErrorV0> {
    validate_consistent_cut(admission, frontier).map_err(encode_consistency_error)?;
    let admission_snapshot = encode_admission_snapshot_v1(admission)
        .map_err(KafkaIngestionCheckpointEncodeErrorV0::AdmissionSnapshot)?;
    let frontier_snapshot = encode_kafka_partition_frontier_snapshot_v0(frontier)
        .map_err(KafkaIngestionCheckpointEncodeErrorV0::FrontierSnapshot)?;
    let admission_length = u64::try_from(admission_snapshot.len()).map_err(|_| {
        KafkaIngestionCheckpointEncodeErrorV0::LengthOverflow {
            field: KafkaIngestionCheckpointFieldV0::AdmissionSnapshot,
        }
    })?;
    let frontier_length = u32::try_from(frontier_snapshot.len()).map_err(|_| {
        KafkaIngestionCheckpointEncodeErrorV0::LengthOverflow {
            field: KafkaIngestionCheckpointFieldV0::FrontierSnapshot,
        }
    })?;
    let capacity = CHECKPOINT_HEADER_BYTES_V0
        .checked_add(admission_snapshot.len())
        .and_then(|value| value.checked_add(frontier_snapshot.len()))
        .ok_or(KafkaIngestionCheckpointEncodeErrorV0::LengthOverflow {
            field: KafkaIngestionCheckpointFieldV0::Checkpoint,
        })?;
    if capacity > MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0 {
        return Err(KafkaIngestionCheckpointEncodeErrorV0::CheckpointTooLarge {
            maximum: MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0,
            attempted: capacity,
        });
    }

    let mut checkpoint = Vec::with_capacity(capacity);
    checkpoint.extend_from_slice(CHECKPOINT_MAGIC_V0);
    checkpoint.extend_from_slice(&KAFKA_INGESTION_CHECKPOINT_VERSION_V0.to_be_bytes());
    checkpoint.extend_from_slice(&ADMISSION_SNAPSHOT_VERSION_V1.to_be_bytes());
    checkpoint.extend_from_slice(&KAFKA_FRONTIER_SNAPSHOT_VERSION_V0.to_be_bytes());
    checkpoint.extend_from_slice(&admission_length.to_be_bytes());
    checkpoint.extend_from_slice(&frontier_length.to_be_bytes());
    checkpoint.extend_from_slice(&admission_snapshot);
    checkpoint.extend_from_slice(&frontier_snapshot);
    debug_assert_eq!(checkpoint.len(), capacity);
    Ok(checkpoint)
}

pub fn restore_kafka_ingestion_checkpoint_v0(
    input: &[u8],
    expected_admission_config: &AdmissionConfigV0,
    expected_source_contract: &KafkaSourceContractV0,
) -> Result<(AdmissionStateV0, KafkaPartitionFrontierV0), KafkaIngestionCheckpointDecodeErrorV0> {
    validate_kafka_ingestion_checkpoint_expected_contract_v0(
        expected_admission_config,
        expected_source_contract,
    )?;
    restore_validated_kafka_ingestion_checkpoint_v0(
        input,
        expected_admission_config,
        expected_source_contract,
    )
}

pub fn validate_kafka_ingestion_checkpoint_expected_contract_v0(
    expected_admission_config: &AdmissionConfigV0,
    expected_source_contract: &KafkaSourceContractV0,
) -> Result<(), KafkaIngestionCheckpointDecodeErrorV0> {
    if expected_admission_config.source_position_kind() != SourcePositionKindV0::Kafka {
        return Err(
            KafkaIngestionCheckpointDecodeErrorV0::ExpectedAdmissionSourcePositionKind {
                actual: expected_admission_config.source_position_kind(),
            },
        );
    }
    if expected_admission_config.source_id() != expected_source_contract.source_id() {
        return Err(KafkaIngestionCheckpointDecodeErrorV0::ExpectedSourceMismatch);
    }
    Ok(())
}

fn restore_validated_kafka_ingestion_checkpoint_v0(
    input: &[u8],
    expected_admission_config: &AdmissionConfigV0,
    expected_source_contract: &KafkaSourceContractV0,
) -> Result<(AdmissionStateV0, KafkaPartitionFrontierV0), KafkaIngestionCheckpointDecodeErrorV0> {
    if input.len() > MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0 {
        return Err(KafkaIngestionCheckpointDecodeErrorV0::DocumentTooLarge {
            maximum: MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0,
            actual: input.len(),
        });
    }

    let mut cursor = CheckpointCursorV0::new(input);
    if cursor.read_exact(
        CHECKPOINT_MAGIC_V0.len(),
        KafkaIngestionCheckpointFieldV0::Magic,
    )? != CHECKPOINT_MAGIC_V0
    {
        return Err(KafkaIngestionCheckpointDecodeErrorV0::InvalidMagic);
    }
    let checkpoint_version = cursor.read_u16(KafkaIngestionCheckpointFieldV0::CheckpointVersion)?;
    if checkpoint_version != KAFKA_INGESTION_CHECKPOINT_VERSION_V0 {
        return Err(
            KafkaIngestionCheckpointDecodeErrorV0::UnsupportedCheckpointVersion {
                actual: checkpoint_version,
            },
        );
    }
    let admission_version =
        cursor.read_u16(KafkaIngestionCheckpointFieldV0::AdmissionSnapshotVersion)?;
    if admission_version != ADMISSION_SNAPSHOT_VERSION_V1 {
        return Err(
            KafkaIngestionCheckpointDecodeErrorV0::UnsupportedAdmissionSnapshotVersion {
                actual: admission_version,
            },
        );
    }
    let frontier_version =
        cursor.read_u16(KafkaIngestionCheckpointFieldV0::FrontierSnapshotVersion)?;
    if frontier_version != KAFKA_FRONTIER_SNAPSHOT_VERSION_V0 {
        return Err(
            KafkaIngestionCheckpointDecodeErrorV0::UnsupportedFrontierSnapshotVersion {
                actual: frontier_version,
            },
        );
    }
    let admission_length_u64 =
        cursor.read_u64(KafkaIngestionCheckpointFieldV0::AdmissionSnapshot)?;
    let admission_length = usize::try_from(admission_length_u64).map_err(|_| {
        KafkaIngestionCheckpointDecodeErrorV0::LengthOverflow {
            field: KafkaIngestionCheckpointFieldV0::AdmissionSnapshot,
        }
    })?;
    if admission_length > MAX_ADMISSION_SNAPSHOT_BYTES_V1 {
        return Err(
            KafkaIngestionCheckpointDecodeErrorV0::InvalidComponentLength {
                field: KafkaIngestionCheckpointFieldV0::AdmissionSnapshot,
                maximum: MAX_ADMISSION_SNAPSHOT_BYTES_V1,
                actual: admission_length,
            },
        );
    }
    let frontier_length =
        usize::try_from(cursor.read_u32(KafkaIngestionCheckpointFieldV0::FrontierSnapshot)?)
            .map_err(|_| KafkaIngestionCheckpointDecodeErrorV0::LengthOverflow {
                field: KafkaIngestionCheckpointFieldV0::FrontierSnapshot,
            })?;
    if frontier_length > MAX_KAFKA_FRONTIER_SNAPSHOT_BYTES_V0 {
        return Err(
            KafkaIngestionCheckpointDecodeErrorV0::InvalidComponentLength {
                field: KafkaIngestionCheckpointFieldV0::FrontierSnapshot,
                maximum: MAX_KAFKA_FRONTIER_SNAPSHOT_BYTES_V0,
                actual: frontier_length,
            },
        );
    }
    let admission_snapshot = cursor.read_exact(
        admission_length,
        KafkaIngestionCheckpointFieldV0::AdmissionSnapshot,
    )?;
    let frontier_snapshot = cursor.read_exact(
        frontier_length,
        KafkaIngestionCheckpointFieldV0::FrontierSnapshot,
    )?;
    if !cursor.is_finished() {
        return Err(KafkaIngestionCheckpointDecodeErrorV0::TrailingBytes);
    }

    let frontier =
        restore_kafka_partition_frontier_snapshot_v0(frontier_snapshot, expected_source_contract)
            .map_err(KafkaIngestionCheckpointDecodeErrorV0::FrontierSnapshot)?;
    let admission = restore_admission_snapshot_v1(admission_snapshot, expected_admission_config)
        .map_err(KafkaIngestionCheckpointDecodeErrorV0::AdmissionSnapshot)?;
    validate_consistent_cut(&admission, &frontier).map_err(decode_consistency_error)?;
    Ok((admission, frontier))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConsistentCutErrorV0 {
    UnsupportedAdmissionSourcePositionKind { actual: SourcePositionKindV0 },
    AdmissionSourceMismatch,
    AcceptedEventUnsupportedPosition { actual: SourcePositionKindV0 },
    AcceptedEventUnknownPartition,
    AcceptedEventNotBeforeFrontier,
}

fn validate_consistent_cut(
    admission: &AdmissionStateV0,
    frontier: &KafkaPartitionFrontierV0,
) -> Result<(), ConsistentCutErrorV0> {
    if admission.config().source_position_kind() != SourcePositionKindV0::Kafka {
        return Err(
            ConsistentCutErrorV0::UnsupportedAdmissionSourcePositionKind {
                actual: admission.config().source_position_kind(),
            },
        );
    }
    if admission.config().source_id() != frontier.source_contract().source_id() {
        return Err(ConsistentCutErrorV0::AdmissionSourceMismatch);
    }
    for event in admission.accepted_events() {
        let Some(position) = event.source_position().kafka_position() else {
            return Err(ConsistentCutErrorV0::AcceptedEventUnsupportedPosition {
                actual: event.source_position().kind(),
            });
        };
        let Some(next_offset) = frontier.next_offset(position.partition()) else {
            return Err(ConsistentCutErrorV0::AcceptedEventUnknownPartition);
        };
        if position.offset() >= next_offset {
            return Err(ConsistentCutErrorV0::AcceptedEventNotBeforeFrontier);
        }
    }
    Ok(())
}

const fn encode_consistency_error(
    error: ConsistentCutErrorV0,
) -> KafkaIngestionCheckpointEncodeErrorV0 {
    match error {
        ConsistentCutErrorV0::UnsupportedAdmissionSourcePositionKind { actual } => {
            KafkaIngestionCheckpointEncodeErrorV0::UnsupportedAdmissionSourcePositionKind { actual }
        }
        ConsistentCutErrorV0::AdmissionSourceMismatch => {
            KafkaIngestionCheckpointEncodeErrorV0::AdmissionSourceMismatch
        }
        ConsistentCutErrorV0::AcceptedEventUnsupportedPosition { actual } => {
            KafkaIngestionCheckpointEncodeErrorV0::AcceptedEventUnsupportedPosition { actual }
        }
        ConsistentCutErrorV0::AcceptedEventUnknownPartition => {
            KafkaIngestionCheckpointEncodeErrorV0::AcceptedEventUnknownPartition
        }
        ConsistentCutErrorV0::AcceptedEventNotBeforeFrontier => {
            KafkaIngestionCheckpointEncodeErrorV0::AcceptedEventNotBeforeFrontier
        }
    }
}

const fn decode_consistency_error(
    error: ConsistentCutErrorV0,
) -> KafkaIngestionCheckpointDecodeErrorV0 {
    match error {
        ConsistentCutErrorV0::UnsupportedAdmissionSourcePositionKind { actual } => {
            KafkaIngestionCheckpointDecodeErrorV0::ExpectedAdmissionSourcePositionKind { actual }
        }
        ConsistentCutErrorV0::AdmissionSourceMismatch => {
            KafkaIngestionCheckpointDecodeErrorV0::RestoredAdmissionSourceMismatch
        }
        ConsistentCutErrorV0::AcceptedEventUnsupportedPosition { actual } => {
            KafkaIngestionCheckpointDecodeErrorV0::AcceptedEventUnsupportedPosition { actual }
        }
        ConsistentCutErrorV0::AcceptedEventUnknownPartition => {
            KafkaIngestionCheckpointDecodeErrorV0::AcceptedEventUnknownPartition
        }
        ConsistentCutErrorV0::AcceptedEventNotBeforeFrontier => {
            KafkaIngestionCheckpointDecodeErrorV0::AcceptedEventNotBeforeFrontier
        }
    }
}

struct CheckpointCursorV0<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> CheckpointCursorV0<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn read_exact(
        &mut self,
        length: usize,
        field: KafkaIngestionCheckpointFieldV0,
    ) -> Result<&'a [u8], KafkaIngestionCheckpointDecodeErrorV0> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(KafkaIngestionCheckpointDecodeErrorV0::LengthOverflow { field })?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or(KafkaIngestionCheckpointDecodeErrorV0::Truncated { field })?;
        self.offset = end;
        Ok(value)
    }

    fn read_u16(
        &mut self,
        field: KafkaIngestionCheckpointFieldV0,
    ) -> Result<u16, KafkaIngestionCheckpointDecodeErrorV0> {
        let mut bytes = [0_u8; 2];
        bytes.copy_from_slice(self.read_exact(2, field)?);
        Ok(u16::from_be_bytes(bytes))
    }

    fn read_u32(
        &mut self,
        field: KafkaIngestionCheckpointFieldV0,
    ) -> Result<u32, KafkaIngestionCheckpointDecodeErrorV0> {
        let mut bytes = [0_u8; 4];
        bytes.copy_from_slice(self.read_exact(4, field)?);
        Ok(u32::from_be_bytes(bytes))
    }

    fn read_u64(
        &mut self,
        field: KafkaIngestionCheckpointFieldV0,
    ) -> Result<u64, KafkaIngestionCheckpointDecodeErrorV0> {
        let mut bytes = [0_u8; 8];
        bytes.copy_from_slice(self.read_exact(8, field)?);
        Ok(u64::from_be_bytes(bytes))
    }

    const fn is_finished(&self) -> bool {
        self.offset == self.input.len()
    }
}
