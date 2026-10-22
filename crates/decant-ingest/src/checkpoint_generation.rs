//! Canonical bounded generation envelope for one combined Kafka checkpoint.

use std::error::Error;
use std::fmt;

use decant_kafka::{KafkaPartitionFrontierV0, KafkaSourceContractV0};
use decant_state::{AdmissionConfigV0, AdmissionStateV0};

use crate::checkpoint_hash::{
    KAFKA_INGESTION_CHECKPOINT_HASH_DIGEST_BYTES_V0, hash_kafka_ingestion_checkpoint_bytes_v0,
};
use crate::{
    KAFKA_INGESTION_CHECKPOINT_HASH_VERSION_V0, KAFKA_INGESTION_CHECKPOINT_VERSION_V0,
    KafkaIngestionCheckpointDecodeErrorV0, KafkaIngestionCheckpointEncodeErrorV0,
    KafkaIngestionCheckpointHashErrorV0, KafkaIngestionCheckpointHashV0,
    MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0, encode_kafka_ingestion_checkpoint_v0,
    restore_kafka_ingestion_checkpoint_v0,
};

pub const KAFKA_CHECKPOINT_GENERATION_ARTIFACT_VERSION_V0: u16 = 0;
pub const MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0: usize =
    KAFKA_CHECKPOINT_GENERATION_ARTIFACT_HEADER_BYTES_V0 + MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0;

const KAFKA_CHECKPOINT_GENERATION_ARTIFACT_MAGIC_V0: &[u8; 8] = b"DECKGEN0";
const KAFKA_CHECKPOINT_GENERATION_ARTIFACT_HEADER_BYTES_V0: usize = 62;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KafkaCheckpointGenerationV0(u64);

impl KafkaCheckpointGenerationV0 {
    pub fn new(value: u64) -> Result<Self, KafkaCheckpointGenerationErrorV0> {
        if value == 0 {
            return Err(KafkaCheckpointGenerationErrorV0::Zero);
        }
        Ok(Self(value))
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaCheckpointGenerationErrorV0 {
    Zero,
}

impl fmt::Display for KafkaCheckpointGenerationErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaCheckpointGenerationErrorV0 {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KafkaCheckpointGenerationArtifactFieldV0 {
    Magic,
    ArtifactVersion,
    Generation,
    CheckpointVersion,
    CheckpointHashVersion,
    CheckpointLength,
    CheckpointHash,
    Checkpoint,
    Artifact,
    EndOfDocument,
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaCheckpointGenerationArtifactEncodeErrorV0 {
    CheckpointEncoding(KafkaIngestionCheckpointEncodeErrorV0),
    CheckpointHash(KafkaIngestionCheckpointHashErrorV0),
    LengthOverflow {
        field: KafkaCheckpointGenerationArtifactFieldV0,
    },
    ArtifactTooLarge {
        maximum: usize,
        attempted: usize,
    },
}

impl fmt::Debug for KafkaCheckpointGenerationArtifactEncodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CheckpointEncoding(error) => formatter
                .debug_tuple("CheckpointEncoding")
                .field(error)
                .finish(),
            Self::CheckpointHash(error) => formatter
                .debug_tuple("CheckpointHash")
                .field(error)
                .finish(),
            Self::LengthOverflow { field } => formatter
                .debug_struct("LengthOverflow")
                .field("field", field)
                .finish(),
            Self::ArtifactTooLarge { maximum, attempted } => formatter
                .debug_struct("ArtifactTooLarge")
                .field("maximum", maximum)
                .field("attempted", attempted)
                .finish(),
        }
    }
}

impl fmt::Display for KafkaCheckpointGenerationArtifactEncodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaCheckpointGenerationArtifactEncodeErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CheckpointEncoding(error) => Some(error),
            Self::CheckpointHash(error) => Some(error),
            Self::LengthOverflow { .. } | Self::ArtifactTooLarge { .. } => None,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaCheckpointGenerationArtifactDecodeErrorV0 {
    ExpectedContract(KafkaIngestionCheckpointDecodeErrorV0),
    DocumentTooLarge {
        maximum: usize,
        actual: usize,
    },
    Truncated {
        field: KafkaCheckpointGenerationArtifactFieldV0,
    },
    InvalidMagic,
    UnsupportedArtifactVersion {
        actual: u16,
    },
    UnexpectedGeneration {
        expected: KafkaCheckpointGenerationV0,
        actual: u64,
    },
    UnsupportedCheckpointVersion {
        actual: u16,
    },
    UnsupportedCheckpointHashVersion {
        actual: u16,
    },
    InvalidCheckpointLength {
        maximum: usize,
        actual: usize,
    },
    LengthOverflow {
        field: KafkaCheckpointGenerationArtifactFieldV0,
    },
    CheckpointHash(KafkaIngestionCheckpointHashErrorV0),
    CheckpointHashMismatch,
    TrailingBytes,
    Checkpoint(KafkaIngestionCheckpointDecodeErrorV0),
}

impl fmt::Debug for KafkaCheckpointGenerationArtifactDecodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ExpectedContract(error) => formatter
                .debug_tuple("ExpectedContract")
                .field(error)
                .finish(),
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
            Self::UnsupportedArtifactVersion { actual } => formatter
                .debug_struct("UnsupportedArtifactVersion")
                .field("actual", actual)
                .finish(),
            Self::UnexpectedGeneration { expected, actual } => formatter
                .debug_struct("UnexpectedGeneration")
                .field("expected", expected)
                .field("actual", actual)
                .finish(),
            Self::UnsupportedCheckpointVersion { actual } => formatter
                .debug_struct("UnsupportedCheckpointVersion")
                .field("actual", actual)
                .finish(),
            Self::UnsupportedCheckpointHashVersion { actual } => formatter
                .debug_struct("UnsupportedCheckpointHashVersion")
                .field("actual", actual)
                .finish(),
            Self::InvalidCheckpointLength { maximum, actual } => formatter
                .debug_struct("InvalidCheckpointLength")
                .field("maximum", maximum)
                .field("actual", actual)
                .finish(),
            Self::LengthOverflow { field } => formatter
                .debug_struct("LengthOverflow")
                .field("field", field)
                .finish(),
            Self::CheckpointHash(error) => formatter
                .debug_tuple("CheckpointHash")
                .field(error)
                .finish(),
            Self::CheckpointHashMismatch => formatter.write_str("CheckpointHashMismatch"),
            Self::TrailingBytes => formatter.write_str("TrailingBytes"),
            Self::Checkpoint(error) => formatter.debug_tuple("Checkpoint").field(error).finish(),
        }
    }
}

impl fmt::Display for KafkaCheckpointGenerationArtifactDecodeErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaCheckpointGenerationArtifactDecodeErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ExpectedContract(error) | Self::Checkpoint(error) => Some(error),
            Self::CheckpointHash(error) => Some(error),
            _ => None,
        }
    }
}

pub struct KafkaCheckpointGenerationRestoreV0 {
    generation: KafkaCheckpointGenerationV0,
    checkpoint_hash: KafkaIngestionCheckpointHashV0,
    admission: AdmissionStateV0,
    frontier: KafkaPartitionFrontierV0,
}

impl KafkaCheckpointGenerationRestoreV0 {
    pub const fn generation(&self) -> KafkaCheckpointGenerationV0 {
        self.generation
    }
    pub const fn checkpoint_hash(&self) -> KafkaIngestionCheckpointHashV0 {
        self.checkpoint_hash
    }
    pub const fn admission(&self) -> &AdmissionStateV0 {
        &self.admission
    }
    pub const fn frontier(&self) -> &KafkaPartitionFrontierV0 {
        &self.frontier
    }
    pub fn into_parts(self) -> (AdmissionStateV0, KafkaPartitionFrontierV0) {
        (self.admission, self.frontier)
    }
}

impl fmt::Debug for KafkaCheckpointGenerationRestoreV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaCheckpointGenerationRestoreV0")
            .field("generation", &self.generation)
            .field("checkpoint_hash", &self.checkpoint_hash)
            .field("admission", &"<redacted>")
            .field("frontier", &"<redacted>")
            .finish()
    }
}

pub fn encode_kafka_checkpoint_generation_artifact_v0(
    generation: KafkaCheckpointGenerationV0,
    admission: &AdmissionStateV0,
    frontier: &KafkaPartitionFrontierV0,
) -> Result<Vec<u8>, KafkaCheckpointGenerationArtifactEncodeErrorV0> {
    let checkpoint = encode_kafka_ingestion_checkpoint_v0(admission, frontier)
        .map_err(KafkaCheckpointGenerationArtifactEncodeErrorV0::CheckpointEncoding)?;
    let checkpoint_hash = hash_kafka_ingestion_checkpoint_bytes_v0(&checkpoint)
        .map_err(KafkaCheckpointGenerationArtifactEncodeErrorV0::CheckpointHash)?;
    let checkpoint_length = u64::try_from(checkpoint.len()).map_err(|_| {
        KafkaCheckpointGenerationArtifactEncodeErrorV0::LengthOverflow {
            field: KafkaCheckpointGenerationArtifactFieldV0::Checkpoint,
        }
    })?;
    let capacity = KAFKA_CHECKPOINT_GENERATION_ARTIFACT_HEADER_BYTES_V0
        .checked_add(checkpoint.len())
        .ok_or(
            KafkaCheckpointGenerationArtifactEncodeErrorV0::LengthOverflow {
                field: KafkaCheckpointGenerationArtifactFieldV0::Artifact,
            },
        )?;
    if capacity > MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0 {
        return Err(
            KafkaCheckpointGenerationArtifactEncodeErrorV0::ArtifactTooLarge {
                maximum: MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0,
                attempted: capacity,
            },
        );
    }
    let mut artifact = Vec::with_capacity(capacity);
    artifact.extend_from_slice(KAFKA_CHECKPOINT_GENERATION_ARTIFACT_MAGIC_V0);
    artifact.extend_from_slice(&KAFKA_CHECKPOINT_GENERATION_ARTIFACT_VERSION_V0.to_be_bytes());
    artifact.extend_from_slice(&generation.get().to_be_bytes());
    artifact.extend_from_slice(&KAFKA_INGESTION_CHECKPOINT_VERSION_V0.to_be_bytes());
    artifact.extend_from_slice(&KAFKA_INGESTION_CHECKPOINT_HASH_VERSION_V0.to_be_bytes());
    artifact.extend_from_slice(&checkpoint_length.to_be_bytes());
    artifact.extend_from_slice(checkpoint_hash.as_bytes());
    artifact.extend_from_slice(&checkpoint);
    debug_assert_eq!(artifact.len(), capacity);
    Ok(artifact)
}

pub fn restore_kafka_checkpoint_generation_artifact_v0(
    input: &[u8],
    expected_generation: KafkaCheckpointGenerationV0,
    expected_admission_config: &AdmissionConfigV0,
    expected_source_contract: &KafkaSourceContractV0,
) -> Result<KafkaCheckpointGenerationRestoreV0, KafkaCheckpointGenerationArtifactDecodeErrorV0> {
    crate::checkpoint::validate_kafka_ingestion_checkpoint_expected_contract_v0(
        expected_admission_config,
        expected_source_contract,
    )
    .map_err(KafkaCheckpointGenerationArtifactDecodeErrorV0::ExpectedContract)?;
    if input.len() > MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0 {
        return Err(
            KafkaCheckpointGenerationArtifactDecodeErrorV0::DocumentTooLarge {
                maximum: MAX_KAFKA_CHECKPOINT_GENERATION_ARTIFACT_BYTES_V0,
                actual: input.len(),
            },
        );
    }
    let mut cursor = ArtifactCursorV0::new(input);
    if cursor.read_exact(8, KafkaCheckpointGenerationArtifactFieldV0::Magic)?
        != KAFKA_CHECKPOINT_GENERATION_ARTIFACT_MAGIC_V0
    {
        return Err(KafkaCheckpointGenerationArtifactDecodeErrorV0::InvalidMagic);
    }
    let artifact_version =
        cursor.read_u16(KafkaCheckpointGenerationArtifactFieldV0::ArtifactVersion)?;
    if artifact_version != KAFKA_CHECKPOINT_GENERATION_ARTIFACT_VERSION_V0 {
        return Err(
            KafkaCheckpointGenerationArtifactDecodeErrorV0::UnsupportedArtifactVersion {
                actual: artifact_version,
            },
        );
    }
    let generation = cursor.read_u64(KafkaCheckpointGenerationArtifactFieldV0::Generation)?;
    if generation != expected_generation.get() {
        return Err(
            KafkaCheckpointGenerationArtifactDecodeErrorV0::UnexpectedGeneration {
                expected: expected_generation,
                actual: generation,
            },
        );
    }
    let checkpoint_version =
        cursor.read_u16(KafkaCheckpointGenerationArtifactFieldV0::CheckpointVersion)?;
    if checkpoint_version != KAFKA_INGESTION_CHECKPOINT_VERSION_V0 {
        return Err(
            KafkaCheckpointGenerationArtifactDecodeErrorV0::UnsupportedCheckpointVersion {
                actual: checkpoint_version,
            },
        );
    }
    let hash_version =
        cursor.read_u16(KafkaCheckpointGenerationArtifactFieldV0::CheckpointHashVersion)?;
    if hash_version != KAFKA_INGESTION_CHECKPOINT_HASH_VERSION_V0 {
        return Err(
            KafkaCheckpointGenerationArtifactDecodeErrorV0::UnsupportedCheckpointHashVersion {
                actual: hash_version,
            },
        );
    }
    let checkpoint_length_u64 =
        cursor.read_u64(KafkaCheckpointGenerationArtifactFieldV0::CheckpointLength)?;
    let checkpoint_length = usize::try_from(checkpoint_length_u64).map_err(|_| {
        KafkaCheckpointGenerationArtifactDecodeErrorV0::LengthOverflow {
            field: KafkaCheckpointGenerationArtifactFieldV0::Checkpoint,
        }
    })?;
    if checkpoint_length > MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0 {
        return Err(
            KafkaCheckpointGenerationArtifactDecodeErrorV0::InvalidCheckpointLength {
                maximum: MAX_KAFKA_INGESTION_CHECKPOINT_BYTES_V0,
                actual: checkpoint_length,
            },
        );
    }
    let expected_hash = cursor.read_exact(
        KAFKA_INGESTION_CHECKPOINT_HASH_DIGEST_BYTES_V0,
        KafkaCheckpointGenerationArtifactFieldV0::CheckpointHash,
    )?;
    let checkpoint = cursor.read_exact(
        checkpoint_length,
        KafkaCheckpointGenerationArtifactFieldV0::Checkpoint,
    )?;
    if !cursor.is_finished() {
        return Err(KafkaCheckpointGenerationArtifactDecodeErrorV0::TrailingBytes);
    }
    let calculated_hash = hash_kafka_ingestion_checkpoint_bytes_v0(checkpoint)
        .map_err(KafkaCheckpointGenerationArtifactDecodeErrorV0::CheckpointHash)?;
    if calculated_hash.as_bytes().as_slice() != expected_hash {
        return Err(KafkaCheckpointGenerationArtifactDecodeErrorV0::CheckpointHashMismatch);
    }
    let (admission, frontier) = restore_kafka_ingestion_checkpoint_v0(
        checkpoint,
        expected_admission_config,
        expected_source_contract,
    )
    .map_err(KafkaCheckpointGenerationArtifactDecodeErrorV0::Checkpoint)?;
    Ok(KafkaCheckpointGenerationRestoreV0 {
        generation: expected_generation,
        checkpoint_hash: calculated_hash,
        admission,
        frontier,
    })
}

struct ArtifactCursorV0<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> ArtifactCursorV0<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn read_exact(
        &mut self,
        length: usize,
        field: KafkaCheckpointGenerationArtifactFieldV0,
    ) -> Result<&'a [u8], KafkaCheckpointGenerationArtifactDecodeErrorV0> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(KafkaCheckpointGenerationArtifactDecodeErrorV0::LengthOverflow { field })?;
        let value = self
            .input
            .get(self.offset..end)
            .ok_or(KafkaCheckpointGenerationArtifactDecodeErrorV0::Truncated { field })?;
        self.offset = end;
        Ok(value)
    }

    fn read_u16(
        &mut self,
        field: KafkaCheckpointGenerationArtifactFieldV0,
    ) -> Result<u16, KafkaCheckpointGenerationArtifactDecodeErrorV0> {
        Ok(u16::from_be_bytes(
            self.read_exact(2, field)?.try_into().expect("exact length"),
        ))
    }

    fn read_u64(
        &mut self,
        field: KafkaCheckpointGenerationArtifactFieldV0,
    ) -> Result<u64, KafkaCheckpointGenerationArtifactDecodeErrorV0> {
        Ok(u64::from_be_bytes(
            self.read_exact(8, field)?.try_into().expect("exact length"),
        ))
    }

    const fn is_finished(&self) -> bool {
        self.offset == self.input.len()
    }
}
