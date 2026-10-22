//! Immutable, bounded offset-commit coordinates for a later live Kafka adapter.
//!
//! A plan is inert data. Building one does not authorize or execute a broker
//! commit and does not establish any delivery guarantee.

use std::error::Error;
use std::fmt;

use crate::{
    KafkaFrontierHashErrorV0, KafkaFrontierHashV0, KafkaPartitionFrontierV0, KafkaSourceContractV0,
    hash_kafka_partition_frontier_v0,
};

pub const KAFKA_OFFSET_COMMIT_PLAN_VERSION_V0: u16 = 0;

#[derive(Clone, Eq, PartialEq)]
pub struct KafkaOffsetCommitPlanV0 {
    version: u16,
    frontier_hash: KafkaFrontierHashV0,
    frozen_frontier: KafkaPartitionFrontierV0,
}

impl KafkaOffsetCommitPlanV0 {
    pub const fn version(&self) -> u16 {
        self.version
    }

    pub const fn source_contract(&self) -> &KafkaSourceContractV0 {
        self.frozen_frontier.source_contract()
    }

    pub fn assignment_count(&self) -> usize {
        self.frozen_frontier.assignment_count()
    }

    pub fn next_offset(&self, partition: i32) -> Option<i64> {
        self.frozen_frontier.next_offset(partition)
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (i32, i64)> + ExactSizeIterator + '_ {
        self.frozen_frontier.iter()
    }

    pub const fn frontier_hash(&self) -> KafkaFrontierHashV0 {
        self.frontier_hash
    }
}

impl fmt::Debug for KafkaOffsetCommitPlanV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("KafkaOffsetCommitPlanV0")
            .field("version", &self.version)
            .field("assignment_count", &self.assignment_count())
            .field("frontier_hash", &self.frontier_hash)
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
pub enum KafkaOffsetCommitPlanErrorV0 {
    FrontierHash(KafkaFrontierHashErrorV0),
}

impl fmt::Debug for KafkaOffsetCommitPlanErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::FrontierHash(error) => {
                formatter.debug_tuple("FrontierHash").field(error).finish()
            }
        }
    }
}

impl fmt::Display for KafkaOffsetCommitPlanErrorV0 {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, formatter)
    }
}

impl Error for KafkaOffsetCommitPlanErrorV0 {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::FrontierHash(error) => Some(error),
        }
    }
}

pub fn build_kafka_offset_commit_plan_v0(
    frontier: &KafkaPartitionFrontierV0,
) -> Result<KafkaOffsetCommitPlanV0, KafkaOffsetCommitPlanErrorV0> {
    let frontier_hash = hash_kafka_partition_frontier_v0(frontier)
        .map_err(KafkaOffsetCommitPlanErrorV0::FrontierHash)?;
    Ok(KafkaOffsetCommitPlanV0 {
        version: KAFKA_OFFSET_COMMIT_PLAN_VERSION_V0,
        frontier_hash,
        frozen_frontier: frontier.clone(),
    })
}
