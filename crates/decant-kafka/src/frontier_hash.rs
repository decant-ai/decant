//! Stable equality/replay identity for one canonical Kafka frontier snapshot.

use std::error::Error;
use std::fmt;

use sha2::{Digest, Sha256};

use crate::{
    KafkaFrontierSnapshotEncodeErrorV0, KafkaPartitionFrontierV0,
    encode_kafka_partition_frontier_snapshot_v0,
};

pub const KAFKA_FRONTIER_HASH_VERSION_V0: u16 = 0;
pub const KAFKA_FRONTIER_HASH_ALGORITHM_V0: &str = "sha256";
pub const KAFKA_FRONTIER_HASH_DOMAIN_V0: &[u8; 29] = b"decant:kafka-frontier-hash:v0";
pub const KAFKA_FRONTIER_HASH_TEXT_BYTES_V0: usize = 74;

const KAFKA_FRONTIER_HASH_DOMAIN_LENGTH_V0: u32 = 29;
const KAFKA_FRONTIER_HASH_DIGEST_BYTES_V0: usize = 32;

#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KafkaFrontierHashV0 {
    digest: [u8; KAFKA_FRONTIER_HASH_DIGEST_BYTES_V0],
}

impl KafkaFrontierHashV0 {
    pub const fn as_bytes(&self) -> &[u8; KAFKA_FRONTIER_HASH_DIGEST_BYTES_V0] {
        &self.digest
    }

    pub fn to_canonical_string(self) -> String {
        self.to_string()
    }
}

impl fmt::Debug for KafkaFrontierHashV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaFrontierHashV0")
            .field("algorithm", &KAFKA_FRONTIER_HASH_ALGORITHM_V0)
            .field("version", &KAFKA_FRONTIER_HASH_VERSION_V0)
            .field("digest", &"<redacted>")
            .finish()
    }
}

impl fmt::Display for KafkaFrontierHashV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{KAFKA_FRONTIER_HASH_ALGORITHM_V0}:v{KAFKA_FRONTIER_HASH_VERSION_V0}:"
        )?;
        for byte in self.digest {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaFrontierHashErrorV0 {
    SnapshotEncoding(KafkaFrontierSnapshotEncodeErrorV0),
    SnapshotLengthOverflow,
}

impl fmt::Debug for KafkaFrontierHashErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SnapshotEncoding(error) => formatter
                .debug_tuple("SnapshotEncoding")
                .field(error)
                .finish(),
            Self::SnapshotLengthOverflow => formatter.write_str("SnapshotLengthOverflow"),
        }
    }
}

impl fmt::Display for KafkaFrontierHashErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaFrontierHashErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::SnapshotEncoding(error) => Some(error),
            Self::SnapshotLengthOverflow => None,
        }
    }
}

pub fn hash_kafka_partition_frontier_v0(
    frontier: &KafkaPartitionFrontierV0,
) -> Result<KafkaFrontierHashV0, KafkaFrontierHashErrorV0> {
    let snapshot = encode_kafka_partition_frontier_snapshot_v0(frontier)
        .map_err(KafkaFrontierHashErrorV0::SnapshotEncoding)?;
    let snapshot_length = u64::try_from(snapshot.len())
        .map_err(|_| KafkaFrontierHashErrorV0::SnapshotLengthOverflow)?;

    let mut hasher = Sha256::new();
    hasher.update(KAFKA_FRONTIER_HASH_DOMAIN_LENGTH_V0.to_be_bytes());
    hasher.update(KAFKA_FRONTIER_HASH_DOMAIN_V0);
    hasher.update(snapshot_length.to_be_bytes());
    hasher.update(&snapshot);

    let output = hasher.finalize();
    let mut digest = [0_u8; KAFKA_FRONTIER_HASH_DIGEST_BYTES_V0];
    digest.copy_from_slice(&output);
    Ok(KafkaFrontierHashV0 { digest })
}
