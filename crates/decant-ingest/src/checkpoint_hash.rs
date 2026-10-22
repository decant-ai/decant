//! Stable equality and accidental-corruption identity for a combined checkpoint.

use std::error::Error;
use std::fmt;

use decant_kafka::KafkaPartitionFrontierV0;
use decant_state::AdmissionStateV0;
use sha2::{Digest, Sha256};

use crate::{KafkaIngestionCheckpointEncodeErrorV0, encode_kafka_ingestion_checkpoint_v0};

pub const KAFKA_INGESTION_CHECKPOINT_HASH_VERSION_V0: u16 = 0;
pub const KAFKA_INGESTION_CHECKPOINT_HASH_ALGORITHM_V0: &str = "sha256";
pub const KAFKA_INGESTION_CHECKPOINT_HASH_DOMAIN_V0: &[u8; 41] =
    b"decant:kafka-ingestion-checkpoint-hash:v0";
pub const KAFKA_INGESTION_CHECKPOINT_HASH_TEXT_BYTES_V0: usize = 74;

const KAFKA_INGESTION_CHECKPOINT_HASH_DOMAIN_LENGTH_V0: u32 = 41;
pub(crate) const KAFKA_INGESTION_CHECKPOINT_HASH_DIGEST_BYTES_V0: usize = 32;

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KafkaIngestionCheckpointHashV0 {
    digest: [u8; KAFKA_INGESTION_CHECKPOINT_HASH_DIGEST_BYTES_V0],
}

impl KafkaIngestionCheckpointHashV0 {
    pub const fn as_bytes(&self) -> &[u8; KAFKA_INGESTION_CHECKPOINT_HASH_DIGEST_BYTES_V0] {
        &self.digest
    }

    pub fn to_canonical_string(self) -> String {
        self.to_string()
    }
}

impl fmt::Debug for KafkaIngestionCheckpointHashV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaIngestionCheckpointHashV0")
            .field("algorithm", &KAFKA_INGESTION_CHECKPOINT_HASH_ALGORITHM_V0)
            .field("version", &KAFKA_INGESTION_CHECKPOINT_HASH_VERSION_V0)
            .field("digest", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for KafkaIngestionCheckpointHashV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{KAFKA_INGESTION_CHECKPOINT_HASH_ALGORITHM_V0}:v{KAFKA_INGESTION_CHECKPOINT_HASH_VERSION_V0}:"
        )?;
        for byte in self.digest {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaIngestionCheckpointHashErrorV0 {
    CheckpointEncoding(KafkaIngestionCheckpointEncodeErrorV0),
    CheckpointLengthOverflow,
}

impl fmt::Debug for KafkaIngestionCheckpointHashErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CheckpointEncoding(error) => formatter
                .debug_tuple("CheckpointEncoding")
                .field(error)
                .finish(),
            Self::CheckpointLengthOverflow => formatter.write_str("CheckpointLengthOverflow"),
        }
    }
}

impl fmt::Display for KafkaIngestionCheckpointHashErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaIngestionCheckpointHashErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CheckpointEncoding(error) => Some(error),
            Self::CheckpointLengthOverflow => None,
        }
    }
}

pub fn hash_kafka_ingestion_checkpoint_v0(
    admission: &AdmissionStateV0,
    frontier: &KafkaPartitionFrontierV0,
) -> Result<KafkaIngestionCheckpointHashV0, KafkaIngestionCheckpointHashErrorV0> {
    let checkpoint = encode_kafka_ingestion_checkpoint_v0(admission, frontier)
        .map_err(KafkaIngestionCheckpointHashErrorV0::CheckpointEncoding)?;
    hash_kafka_ingestion_checkpoint_bytes_v0(&checkpoint)
}

pub(crate) fn hash_kafka_ingestion_checkpoint_bytes_v0(
    checkpoint: &[u8],
) -> Result<KafkaIngestionCheckpointHashV0, KafkaIngestionCheckpointHashErrorV0> {
    let checkpoint_length = u64::try_from(checkpoint.len())
        .map_err(|_| KafkaIngestionCheckpointHashErrorV0::CheckpointLengthOverflow)?;
    let mut hasher = Sha256::new();
    hasher.update(KAFKA_INGESTION_CHECKPOINT_HASH_DOMAIN_LENGTH_V0.to_be_bytes());
    hasher.update(KAFKA_INGESTION_CHECKPOINT_HASH_DOMAIN_V0);
    hasher.update(checkpoint_length.to_be_bytes());
    hasher.update(checkpoint);
    let output = hasher.finalize();
    let mut digest = [0_u8; KAFKA_INGESTION_CHECKPOINT_HASH_DIGEST_BYTES_V0];
    digest.copy_from_slice(&output);
    Ok(KafkaIngestionCheckpointHashV0 { digest })
}
